//! How an ack-policy change reaches the journal.
//!
//! The policy in force is part of the lineage: a change is an
//! `AckPolicyChange` entry, journaled in order with the events it governs,
//! and the response gate switches at the entry's own slot (see
//! `durability_gate`). This module is the producing side.
//!
//! The input ring has a single producer: the main thread while a primary
//! starts, then the io_uring reader or the DPDK poll thread. The admin
//! `ACK-POLICY` command runs on its own thread, so it cannot publish an
//! entry itself. It posts the request in [`AckPolicyControl`], a mailbox
//! shared with the ring's producer, and waits for the producer to publish
//! it:
//!
//! - **At boot and at promotion**, the main thread takes a
//!   [`PublisherClaim`] before the pipeline serves anyone, and journals the
//!   requested policy (the configured one, or one an operator staged) when
//!   the lineage holds a different one or none
//!   (`PublisherClaim::journal_boot`, which consumes the claim).
//! - **While the node serves**, the booted claim is attached to the
//!   producer (`BootedClaim::attach`), which checks the mailbox on its loop
//!   (`AckPolicyPublisher::poll`) and journals each request. The admin
//!   command replies once the entry is on the ring, or refuses if the ring
//!   stays full past a bounded wait (`AckPolicyControl::request`).
//! - **With no producer attached** (a replica, or a primary still
//!   starting), a request is staged: a replica journals it when promoted,
//!   a primary as its producer attaches, from the main thread that still
//!   owns the ring, so ahead of any runtime request.
//!
//! Every runtime request is journaled, duplicates included: the entries
//! are the audit trail of what operators asked for, and a re-sent request
//! is how an operator forces a swap a departed replica would otherwise
//! hold up.
//!
//! # Witnessed and forced changes
//!
//! A change normally takes effect once its entry is durable under the
//! policy before it, so the nodes that policy relied on hold it. A primary
//! halted under a policy that needs a replica, with none connected, can
//! never get there. The one way out is a *forced* change: an explicit
//! runtime `ACK-POLICY disk` while no replica is connected, journaled as
//! unwitnessed, which the gate applies once its entry is on this node's
//! disk. A node with replication disabled has no witness for any change,
//! so its entries are all unwitnessed too. The rule is `is_unwitnessed`.
//!
//! Journaling a forced change is also the operator's consent to serve on
//! this node's disk alone, so it lifts the halt on client writes until a
//! replica streaming live acknowledges an entry; journaling a policy that
//! needs a replica restores it (see `halt`). The producer records both as
//! it journals the entry.

use std::convert::Infallible;
use std::os::fd::OwnedFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use melin_app::{AppEvent, unix_epoch_nanos};
use melin_journal::{AckPolicyByte, AckPolicyChange, JournalEvent};
use melin_pipeline::ring;
use melin_transport_core::pipeline::InputSlot;
use tracing::{info, warn};

use crate::ack_policy::AckPolicy;
use crate::halt::{HaltGate, Journaled};

/// How long an `ACK-POLICY` command waits for its entry to be published
/// before it is refused. The producer checks the mailbox on every loop, so
/// only a full input ring (a pipeline stalled behind its gate) holds a
/// request this long; the admin connection's own timeout is longer, so the
/// operator reads the refusal.
const PUBLISH_WAIT: Duration = Duration::from_secs(2);

/// Who could witness a change right now: the replicas a primary streams
/// to, as the ring's producer sees them (`HaltGate::decide`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Witnesses {
    /// Replication is disabled on this node: no change can ever be
    /// witnessed.
    Disabled,
    /// Replication is on, and no replica is streaming.
    NoneConnected,
    /// At least one replica is streaming.
    Connected,
}

/// Where a request comes from, which decides whether it may be forced.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Origin {
    /// An `ACK-POLICY` command a running primary's producer publishes:
    /// the operator asked for this policy, now.
    Runtime,
    /// The policy a primary starts or is promoted under, or one staged
    /// before its producer ran. Never forced: nobody asked for it in the
    /// knowledge of who is connected when it is published.
    Staged,
}

/// Whether a change to `policy` is journaled unwitnessed: applied once its
/// entry is on this node's disk, rather than once the nodes the previous
/// policy relied on hold it.
///
/// With replication disabled, every change (nothing can witness one). With
/// no replica connected, an explicit runtime `disk` only: the operator's
/// consent to serve on this node's disk alone. Any other change waits.
pub(crate) fn is_unwitnessed(policy: AckPolicy, origin: Origin, witnesses: Witnesses) -> bool {
    match witnesses {
        Witnesses::Disabled => true,
        Witnesses::NoneConnected => origin == Origin::Runtime && policy == AckPolicy::Disk,
        Witnesses::Connected => false,
    }
}

/// The input-ring slot that journals `change`.
fn change_slot<E: AppEvent>(change: AckPolicyChange) -> InputSlot<E> {
    InputSlot::node_event(JournalEvent::AckPolicyChange(change), unix_epoch_nanos())
}

/// Journal `policy` as a staged change (`Origin::Staged`, never forced):
/// the policy a primary starts or is promoted under, or one an operator
/// staged before its producer attached. From the main thread, while it is
/// still the input ring's only producer, so the entry lands ahead of any
/// runtime request. Journaled through `halt`, as the producer does for a
/// runtime request: a staged policy that needs a replica drops any halt
/// override. Not waited on: the gate switches at the entry's slot, in
/// order, once the policy before it has cleared it. Returns what was
/// journaled, for the caller's log line.
fn publish_staged<E: AppEvent>(
    producer: &mut ring::Producer<InputSlot<E>>,
    halt: &HaltGate,
    policy: AckPolicy,
) -> Journaled {
    // A blocking publish: it waits for room rather than failing.
    let Ok(journaled) = halt.journal(policy, Origin::Staged, |change| {
        producer.publish(change_slot(change));
        Ok::<_, Infallible>(())
    });
    journaled
}

/// Log a change being journaled, at the level its risk calls for.
fn log_journaled(journaled: Journaled, origin: Origin) {
    let policy = journaled.policy();
    if journaled.is_forced() {
        warn!(
            policy = %policy,
            "ack policy change forced with no replica connected: client writes are taken again \
             until a replica streaming live acknowledges an entry, the change takes effect once on this node's disk, and a \
             replica that was offline does not hold it — if this node fails before that replica \
             rejoins, it may auto-promote over events acknowledged under the new policy"
        );
    } else {
        info!(
            policy = %policy,
            unwitnessed = journaled.change().unwitnessed,
            origin = ?origin,
            "ack policy change journaled"
        );
    }
}

/// What became of an `ACK-POLICY` request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RequestOutcome {
    /// Journaled: the entry is on the input ring.
    Published,
    /// No producer runs on this node (a replica, or a primary still
    /// starting): the policy is kept, and journaled when one does.
    Staged,
    /// Not journaled, for this reason: nothing changed.
    Refused(Refused),
}

/// Why an `ACK-POLICY` request was refused. Each calls for something
/// different from the operator, so the admin reply names it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Refused {
    /// The input ring stayed full past the bounded wait: the pipeline is
    /// stalled behind its gate. Retrying may succeed once it drains.
    RingFull,
    /// The producer stopped (the node is shutting down or stepping down)
    /// before it published the entry.
    ProducerStopped,
    /// Another request was already waiting for the producer: two are
    /// never merged.
    AnotherWaiting,
}

/// A runtime request waiting for the producer.
#[derive(Clone, Copy, Debug)]
struct Request {
    policy: AckPolicy,
    /// Its place among requests: published in order, so a requester waits
    /// for the published count to reach it.
    ticket: u64,
}

/// The input ring's producer, as the mailbox sees it.
enum Producer {
    /// None runs: requests are staged.
    Detached,
    /// One runs. `wake` brings it back round its loop when parked (the
    /// io_uring reader's eventfd); `None` for one that never parks (the
    /// DPDK poll thread).
    Attached { wake: Option<Arc<OwnedFd>> },
}

/// The mailbox's state. Behind one mutex: it is touched by the admin
/// thread per command, by the producer only when a request waits, never
/// on the producer's steady-state path (that reads [`AckPolicyControl::pending`]),
/// and by auto-promotion's fallback ([`AckPolicyControl::requested`]).
struct State {
    /// Whether a producer is attached, and how to wake it.
    producer: Producer,
    /// The request waiting for the producer, if any.
    request: Option<Request>,
    /// The last ticket handed out, and the last one published.
    issued: u64,
    published: u64,
    /// The requested policy: the one the next boot or promotion requests,
    /// i.e. the configured one, then each one staged or journaled.
    requested: AckPolicy,
}

/// The mailbox between the admin `ACK-POLICY` command and the input ring's
/// producer — see the module docs. One per process, shared by both roles:
/// a replica stages into it, and its promoted primary journals from it.
pub struct AckPolicyControl {
    state: Mutex<State>,
    /// Signalled when a request is published or withdrawn, or the
    /// producer detaches.
    changed: Condvar,
    /// Set while a request waits for the producer: the one load the
    /// producer's loop pays.
    pending: AtomicBool,
    publish_wait: Duration,
}

impl AckPolicyControl {
    /// A mailbox starting from the configured policy.
    pub(crate) fn new(configured: AckPolicy) -> Arc<Self> {
        Self::with_publish_wait(configured, PUBLISH_WAIT)
    }

    /// As [`Self::new`], with the bound a request waits for its entry.
    fn with_publish_wait(configured: AckPolicy, publish_wait: Duration) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                producer: Producer::Detached,
                request: None,
                issued: 0,
                published: 0,
                requested: configured,
            }),
            changed: Condvar::new(),
            pending: AtomicBool::new(false),
            publish_wait,
        })
    }

    /// The requested policy ([`State::requested`]), for a reader off the
    /// producer's path: auto-promotion's fallback, used only while a
    /// replica's lineage holds no change. Takes the state's lock, which
    /// only the admin thread and a producer publishing a request contend.
    pub(crate) fn requested(&self) -> AckPolicy {
        self.lock().requested
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        // Every critical section leaves the state consistent before it can
        // panic (none calls out of this module), so a poisoned lock holds
        // valid state.
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Journal `policy` (the admin `ACK-POLICY` command): post it to the
    /// attached producer and wait, at most the publish bound, until its
    /// entry is on the input ring; or stage it while no producer runs.
    ///
    /// Called by one thread, the admin listener's, which serves one
    /// command at a time, so no other request is waiting: a request is
    /// published or withdrawn before this returns.
    pub(crate) fn request(&self, policy: AckPolicy) -> RequestOutcome {
        let deadline = Instant::now() + self.publish_wait;
        let mut state = self.lock();
        let Producer::Attached { wake } = &state.producer else {
            state.requested = policy;
            return RequestOutcome::Staged;
        };
        if state.request.is_some() {
            // A second requester: two requests are never merged.
            return RequestOutcome::Refused(Refused::AnotherWaiting);
        }
        let wake = wake.clone();
        state.issued += 1;
        let ticket = state.issued;
        state.request = Some(Request { policy, ticket });
        self.pending.store(true, Ordering::Release);
        if let Some(fd) = wake {
            crate::reader::wake_reader(&fd);
        }
        loop {
            if state.published >= ticket {
                return RequestOutcome::Published;
            }
            if !state
                .request
                .is_some_and(|waiting| waiting.ticket == ticket)
            {
                // Gone unpublished: only the producer's drop withdraws a
                // request other than its own requester.
                return RequestOutcome::Refused(Refused::ProducerStopped);
            }
            let Some(left) = deadline.checked_duration_since(Instant::now()) else {
                // Withdrawn under the lock the producer publishes under,
                // so it is either on the ring (checked above) or never
                // will be.
                state.request = None;
                self.pending.store(false, Ordering::Release);
                self.changed.notify_all();
                return RequestOutcome::Refused(Refused::RingFull);
            };
            state = self.wait(state, left);
        }
    }

    fn wait<'a>(&self, state: MutexGuard<'a, State>, left: Duration) -> MutexGuard<'a, State> {
        self.changed
            .wait_timeout(state, left)
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .0
    }

    /// Claim the producer's side for a primary tenure: the policy to
    /// request as it starts, and the right to attach its producer. Called
    /// once, by the primary's main thread, before it journals that policy.
    pub(crate) fn claim(self: &Arc<Self>) -> PublisherClaim {
        PublisherClaim {
            control: Arc::clone(self),
            policy: self.requested(),
        }
    }
}

/// The policy a primary requests as it starts, and, once its boot entry
/// is decided (`PublisherClaim::journal_boot`), the token its input ring's
/// producer attaches with. Used by the main thread while it still owns
/// the ring's producer.
///
/// The tenure's two steps are a typestate: `journal_boot` consumes the
/// claim and returns the [`BootedClaim`] that alone can attach, so no
/// primary path, on either transport, can attach a producer without
/// having journaled its boot entry (or decided the lineage already holds
/// it), nor journal it twice or after attaching.
pub struct PublisherClaim {
    control: Arc<AckPolicyControl>,
    policy: AckPolicy,
}

impl PublisherClaim {
    /// The policy to journal as the primary starts.
    pub(crate) fn policy(&self) -> AckPolicy {
        self.policy
    }

    /// Journal the policy this tenure requests, if the lineage holds
    /// another or none (a new history, or a lineage written before policy
    /// changes were journaled) — see [`publish_staged`]. After a
    /// promotion's epoch bump, ahead of `on_primary` and of any client.
    /// Returns the claim its producer attaches with.
    #[must_use = "the booted claim is what attaches the producer"]
    pub(crate) fn journal_boot<E: AppEvent>(
        self,
        lineage: Option<AckPolicyByte>,
        producer: &mut ring::Producer<InputSlot<E>>,
        halt: &HaltGate,
    ) -> BootedClaim {
        if lineage != Some(self.policy.to_byte()) {
            let journaled = publish_staged(producer, halt, self.policy);
            info!(
                policy = %self.policy,
                unwitnessed = journaled.change().unwitnessed,
                lineage = ?lineage.map(|byte| byte.0),
                "journaling the requested ack policy"
            );
        }
        BootedClaim {
            control: self.control,
            policy: self.policy,
        }
    }
}

/// A [`PublisherClaim`] whose boot entry is decided: the token the input
/// ring's producer attaches with (`BootedClaim::attach`).
pub struct BootedClaim {
    control: Arc<AckPolicyControl>,
    /// The policy the boot entry journaled, or the lineage already held.
    policy: AckPolicy,
}

impl BootedClaim {
    /// Attach the input ring's producer. `wake` is the handle that brings
    /// a parked producer back round its loop (the io_uring reader's
    /// eventfd), `None` for one that never parks (the DPDK poll thread).
    ///
    /// Called by the main thread before it hands `producer` over to the
    /// producer's thread: a policy staged since the claim (while the
    /// primary started) is journaled here, on `producer`, so it lands
    /// ahead of every runtime request the attached producer publishes. One
    /// equal to the claim's own was journaled by the boot entry, or is the
    /// lineage's already.
    pub(crate) fn attach<E: AppEvent>(
        self,
        producer: &mut ring::Producer<InputSlot<E>>,
        halt: &HaltGate,
        wake: Option<Arc<OwnedFd>>,
    ) -> AckPolicyPublisher {
        let control = self.control;
        let staged = {
            let mut state = control.lock();
            state.producer = Producer::Attached { wake };
            state.requested
        };
        // Outside the lock: a full ring blocks this publish, and the admin
        // thread must not block on the lock behind it. Nothing else can
        // publish meanwhile: a runtime request waits for this thread's
        // first poll.
        if staged != self.policy {
            log_journaled(publish_staged(producer, halt, staged), Origin::Staged);
        }
        AckPolicyPublisher { control }
    }
}

/// The input ring producer's end of the mailbox: journals each request.
/// Detaches when dropped, so requests made after the producer stopped are
/// refused rather than left waiting.
pub struct AckPolicyPublisher {
    control: Arc<AckPolicyControl>,
}

impl AckPolicyPublisher {
    /// Journal the waiting request, if any. One acquire load when none
    /// waits. On a full ring the request stays, for the next call.
    ///
    /// `halt` is the producer's own halt gate, read only when a request
    /// waits: who is streaming decides whether the entry is unwitnessed
    /// ([`is_unwitnessed`]), and the journaled change then lifts or
    /// restores the halt ([`HaltGate::journal`]). Taking the gate
    /// rather than a reading of it is what keeps the two in step: the
    /// producer cannot journal a forced change without lifting the halt.
    #[inline]
    pub(crate) fn poll<E: AppEvent>(
        &mut self,
        producer: &mut ring::Producer<InputSlot<E>>,
        halt: &HaltGate,
    ) {
        if self.control.pending.load(Ordering::Acquire) {
            self.publish(producer, halt);
        }
    }

    #[cold]
    fn publish<E: AppEvent>(
        &mut self,
        producer: &mut ring::Producer<InputSlot<E>>,
        halt: &HaltGate,
    ) {
        let control = &*self.control;
        let mut state = control.lock();
        let Some(request) = state.request else {
            // Withdrawn since the flag was read.
            return;
        };
        // Recorded against the halt as it is published, before the
        // requester is released, so a write it sends once told `OK` meets
        // the halt the change leaves.
        let Ok(journaled) = halt.journal(request.policy, Origin::Runtime, |change| {
            producer.try_publish(change_slot(change)).map(drop)
        }) else {
            // Full: the requester's bounded wait decides when to give up.
            return;
        };
        state.request = None;
        state.published = request.ticket;
        state.requested = request.policy;
        control.pending.store(false, Ordering::Release);
        control.changed.notify_all();
        drop(state);
        log_journaled(journaled, Origin::Runtime);
    }
}

impl Drop for AckPolicyPublisher {
    fn drop(&mut self) {
        let mut state = self.control.lock();
        state.producer = Producer::Detached;
        // A request still waiting is refused by its requester, who sees it
        // gone unpublished.
        state.request = None;
        self.control.pending.store(false, Ordering::Release);
        self.control.changed.notify_all();
    }
}

/// Mailbox fixtures for the crate's tests.
#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use melin_pipeline::ring::{Consumer, DisruptorBuilder};
    use melin_pipeline::wait::WaitStrategy;

    /// A mailbox whose requests wait at most `publish_wait`.
    pub(crate) fn control_with_publish_wait(
        configured: AckPolicy,
        publish_wait: Duration,
    ) -> Arc<AckPolicyControl> {
        AckPolicyControl::with_publish_wait(configured, publish_wait)
    }

    /// An input ring of `capacity` slots with its one consumer.
    pub(crate) fn ring<E: AppEvent>(
        capacity: usize,
    ) -> (ring::Producer<InputSlot<E>>, Consumer<InputSlot<E>>) {
        let (producer, mut consumers) = DisruptorBuilder::<InputSlot<E>>::new(capacity)
            .add_consumer()
            .build(WaitStrategy::SpinThenYield);
        (producer, consumers.pop().expect("one consumer"))
    }

    /// A claim on `control` booted on a lineage that already holds its
    /// policy, so it journals nothing: for a test that only needs a
    /// producer to attach.
    pub(crate) fn booted(control: &Arc<AckPolicyControl>) -> BootedClaim {
        let claim = control.claim();
        let lineage = Some(claim.policy().to_byte());
        // The smallest ring there is: nothing is published on it.
        let (mut producer, _consumer) = ring::<counter_server::CounterEvent>(2);
        claim.journal_boot(
            lineage,
            &mut producer,
            &crate::halt::test_support::gate_over(None),
        )
    }

    /// Attach a claim on `control` to `producer`, under a standalone halt
    /// gate: for a test that only needs a producer attached.
    pub(crate) fn attach<E: AppEvent>(
        control: &Arc<AckPolicyControl>,
        producer: &mut ring::Producer<InputSlot<E>>,
    ) -> AckPolicyPublisher {
        booted(control).attach(producer, &crate::halt::test_support::gate_over(None), None)
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::{attach, booted, control_with_publish_wait, ring};
    use super::*;
    use crate::halt::Verdict;
    use crate::halt::test_support::{gate_over, presence};
    use counter_server::CounterEvent as TestEvent;
    use melin_pipeline::ring::Consumer;

    const ALL: [AckPolicy; 4] = [
        AckPolicy::Disk,
        AckPolicy::Ram,
        AckPolicy::DiskAndRam,
        AckPolicy::TwoDisks,
    ];

    type Slot = InputSlot<TestEvent>;

    /// Every change on the ring, in order.
    fn changes(consumer: &mut Consumer<Slot>) -> Vec<AckPolicyChange> {
        let mut out = Vec::new();
        while let Some((_, slot)) = consumer.try_consume() {
            match slot.event {
                JournalEvent::AckPolicyChange(change) => {
                    assert_eq!(
                        (slot.connection_id, slot.key_hash, slot.sequence),
                        (0, 0, 0),
                        "a node event"
                    );
                    out.push(change);
                }
                other => panic!("not a policy change: {other:?}"),
            }
        }
        out
    }

    fn change(policy: AckPolicy, unwitnessed: bool) -> AckPolicyChange {
        AckPolicyChange {
            policy: policy.to_byte(),
            unwitnessed,
        }
    }

    /// Only an explicit runtime `disk` with no replica connected is
    /// forced; with replication disabled every change is unwitnessed; with
    /// a replica connected none is.
    #[test]
    fn the_forced_rule() {
        for policy in ALL {
            for origin in [Origin::Runtime, Origin::Staged] {
                assert!(is_unwitnessed(policy, origin, Witnesses::Disabled));
                assert!(!is_unwitnessed(policy, origin, Witnesses::Connected));
                assert_eq!(
                    is_unwitnessed(policy, origin, Witnesses::NoneConnected),
                    origin == Origin::Runtime && policy == AckPolicy::Disk,
                    "{policy} from {origin:?}"
                );
            }
        }
    }

    /// The halt gate of a replicated primary with no replica connected:
    /// halted, and a runtime `disk` journaled through it is forced.
    fn halted_gate() -> HaltGate {
        gate_over(Some(presence(0)))
    }

    /// The boot entry: none when the lineage already holds the requested
    /// policy; otherwise the requested policy, as the node's own event,
    /// witnessed unless replication is disabled (a boot entry is never
    /// forced, so a `disk` lifts no halt).
    #[test]
    fn the_boot_entry_is_journaled_only_when_the_lineage_differs() {
        let disk = Some(AckPolicy::Disk.to_byte());
        let (mut producer, mut consumer) = ring::<TestEvent>(8);
        let halt = halted_gate();
        let boot = |requested, lineage, producer: &mut ring::Producer<Slot>, halt: &HaltGate| {
            drop(
                AckPolicyControl::new(requested)
                    .claim()
                    .journal_boot(lineage, producer, halt),
            );
        };

        boot(AckPolicy::Disk, disk, &mut producer, &halt);
        assert!(changes(&mut consumer).is_empty(), "nothing to change");
        boot(AckPolicy::DiskAndRam, disk, &mut producer, &halt);
        assert_eq!(
            changes(&mut consumer),
            [change(AckPolicy::DiskAndRam, false)]
        );
        boot(AckPolicy::Disk, None, &mut producer, &halt);
        assert_eq!(
            changes(&mut consumer),
            [change(AckPolicy::Disk, false)],
            "a lineage holding none gets one, never forced"
        );
        assert_ne!(halt.verdict(), Verdict::Take, "a boot `disk` is no consent");

        let standalone = gate_over(None);
        boot(
            AckPolicy::Disk,
            Some(AckPolicy::DiskAndRam.to_byte()),
            &mut producer,
            &standalone,
        );
        assert_eq!(
            changes(&mut consumer),
            [change(AckPolicy::Disk, true)],
            "standalone: unwitnessed"
        );
    }

    /// A boot entry for a policy that needs a replica restores a halt an
    /// earlier forced swap lifted, as a runtime one does.
    #[test]
    fn a_boot_entry_that_needs_a_replica_restores_the_halt() {
        let control = AckPolicyControl::new(AckPolicy::Disk);
        let (mut producer, mut consumer) = ring::<TestEvent>(8);
        let halt = halted_gate();
        let mut publisher = booted(&control).attach(&mut producer, &halt, None);
        let outcome = request_while_polling(
            &control,
            &mut publisher,
            &mut producer,
            AckPolicy::Disk,
            &halt,
        );
        assert_eq!(outcome, RequestOutcome::Published);
        assert_eq!(halt.verdict(), Verdict::Take, "forced: lifted");
        drop(publisher);

        drop(
            AckPolicyControl::new(AckPolicy::TwoDisks)
                .claim()
                .journal_boot(Some(AckPolicy::Disk.to_byte()), &mut producer, &halt),
        );
        assert_ne!(halt.verdict(), Verdict::Take, "restored");
        assert_eq!(
            changes(&mut consumer),
            [
                change(AckPolicy::Disk, true),
                change(AckPolicy::TwoDisks, false)
            ]
        );
    }

    /// With no producer attached a request is staged: the next claim
    /// requests it, and so does auto-promotion's fallback.
    #[test]
    fn a_request_with_no_producer_is_staged_for_the_next_claim() {
        let control = AckPolicyControl::new(AckPolicy::DiskAndRam);
        assert_eq!(control.claim().policy(), AckPolicy::DiskAndRam);
        assert_eq!(control.request(AckPolicy::Ram), RequestOutcome::Staged);
        assert_eq!(control.requested(), AckPolicy::Ram);
        assert_eq!(control.claim().policy(), AckPolicy::Ram);
    }

    /// Run `request` on a helper thread while this one plays the producer:
    /// polling `publisher` on `producer` until the request returns.
    fn request_while_polling(
        control: &Arc<AckPolicyControl>,
        publisher: &mut AckPolicyPublisher,
        producer: &mut ring::Producer<Slot>,
        policy: AckPolicy,
        halt: &HaltGate,
    ) -> RequestOutcome {
        std::thread::scope(|s| {
            let requesting = s.spawn(|| control.request(policy));
            while !requesting.is_finished() {
                publisher.poll(producer, halt);
                std::thread::yield_now();
            }
            requesting.join().expect("the request panicked")
        })
    }

    /// An attached producer journals each request, in order, duplicates
    /// included, and the requester returns once the entry is on the ring.
    /// The forced decision is the producer's, at publish time. (The halt a
    /// forced swap lifts is the gate's: `halt.rs`.)
    #[test]
    fn an_attached_producer_journals_every_request() {
        let control = AckPolicyControl::new(AckPolicy::DiskAndRam);
        let (mut producer, mut consumer) = ring(8);
        let replicas = presence(1);
        let halt = gate_over(Some(Arc::clone(&replicas)));
        let mut publisher = booted(&control).attach(&mut producer, &halt, None);
        let mut journal = |policy| {
            let outcome =
                request_while_polling(&control, &mut publisher, &mut producer, policy, &halt);
            assert_eq!(outcome, RequestOutcome::Published);
        };

        journal(AckPolicy::Disk);
        journal(AckPolicy::Disk);
        assert!(replicas.lower());
        journal(AckPolicy::Disk);
        journal(AckPolicy::TwoDisks);
        assert_eq!(
            changes(&mut consumer),
            [
                change(AckPolicy::Disk, false),
                change(AckPolicy::Disk, false),
                change(AckPolicy::Disk, true),
                change(AckPolicy::TwoDisks, false),
            ]
        );
        assert_eq!(control.claim().policy(), AckPolicy::TwoDisks);
    }

    /// Nothing waits: polling publishes nothing and leaves the halt alone.
    #[test]
    fn polling_with_nothing_requested_publishes_nothing() {
        let control = AckPolicyControl::new(AckPolicy::Disk);
        let (mut producer, mut consumer) = ring(8);
        let halt = halted_gate();
        let mut publisher = booted(&control).attach(&mut producer, &halt, None);
        publisher.poll(&mut producer, &halt);
        assert!(changes(&mut consumer).is_empty());
        assert_ne!(halt.verdict(), Verdict::Take);
    }

    /// A ring that stays full past the bound refuses the request, which
    /// is then withdrawn: it is not published once the ring drains, and
    /// the requested policy is unchanged.
    #[test]
    fn a_full_ring_refuses_the_request_and_withdraws_it() {
        let control = control_with_publish_wait(AckPolicy::DiskAndRam, Duration::from_millis(50));
        let (mut producer, mut consumer) = ring(2);
        for _ in 0..2 {
            producer
                .try_publish(change_slot(change(AckPolicy::Ram, false)))
                .expect("room");
        }
        // A `disk` that would be forced: refused, it lifts nothing.
        let halt = halted_gate();
        let mut publisher = booted(&control).attach(&mut producer, &halt, None);
        let outcome = request_while_polling(
            &control,
            &mut publisher,
            &mut producer,
            AckPolicy::Disk,
            &halt,
        );
        assert_eq!(outcome, RequestOutcome::Refused(Refused::RingFull));
        assert_eq!(changes(&mut consumer).len(), 2, "only what filled the ring");
        publisher.poll(&mut producer, &halt);
        assert!(changes(&mut consumer).is_empty(), "withdrawn");
        assert_eq!(control.requested(), AckPolicy::DiskAndRam);
        assert_ne!(halt.verdict(), Verdict::Take, "still halted");
    }

    /// A producer that stops (shutdown) refuses the request waiting on it,
    /// without the full bound, and later requests are staged.
    #[test]
    fn a_detached_producer_refuses_the_waiting_request() {
        let control = control_with_publish_wait(AckPolicy::Disk, Duration::from_secs(60));
        let (mut producer, _consumer) = ring::<TestEvent>(8);
        let publisher = attach(&control, &mut producer);
        let started = Instant::now();
        let outcome = std::thread::scope(|s| {
            let requesting = s.spawn(|| control.request(AckPolicy::Ram));
            while !control.pending.load(Ordering::Acquire) {
                std::thread::yield_now();
            }
            drop(publisher);
            requesting.join().expect("the request panicked")
        });
        assert_eq!(outcome, RequestOutcome::Refused(Refused::ProducerStopped));
        assert!(started.elapsed() < Duration::from_secs(30), "not the bound");
        assert!(!control.pending.load(Ordering::Acquire));
        assert_eq!(control.request(AckPolicy::Ram), RequestOutcome::Staged);
    }

    /// A producer replaced while a request waits (a tenure ending, the
    /// next one attaching) refuses it as stopped: the request went with
    /// the old producer, unpublished, and the new one never sees it.
    #[test]
    fn a_producer_replaced_while_a_request_waits_refuses_it() {
        let control = control_with_publish_wait(AckPolicy::Disk, Duration::from_secs(60));
        let (mut producer, mut consumer) = ring::<TestEvent>(8);
        let publisher = attach(&control, &mut producer);
        let started = Instant::now();
        let outcome = std::thread::scope(|s| {
            let requesting = s.spawn(|| control.request(AckPolicy::Ram));
            while !control.pending.load(Ordering::Acquire) {
                std::thread::yield_now();
            }
            drop(publisher);
            let mut next = attach(&control, &mut producer);
            let outcome = requesting.join().expect("the request panicked");
            next.poll(&mut producer, &gate_over(None));
            outcome
        });
        assert_eq!(outcome, RequestOutcome::Refused(Refused::ProducerStopped));
        assert!(started.elapsed() < Duration::from_secs(30), "not the bound");
        assert!(changes(&mut consumer).is_empty(), "never published");
    }

    /// A second request while one waits is refused, not merged into it,
    /// and the first is still published.
    #[test]
    fn a_second_waiting_request_is_refused() {
        let control = control_with_publish_wait(AckPolicy::DiskAndRam, Duration::from_secs(60));
        let (mut producer, mut consumer) = ring(8);
        let halt = halted_gate();
        let mut publisher = booted(&control).attach(&mut producer, &halt, None);
        std::thread::scope(|s| {
            let first = s.spawn(|| control.request(AckPolicy::TwoDisks));
            while !control.pending.load(Ordering::Acquire) {
                std::thread::yield_now();
            }
            assert_eq!(
                control.request(AckPolicy::Disk),
                RequestOutcome::Refused(Refused::AnotherWaiting)
            );
            publisher.poll(&mut producer, &halt);
            assert_eq!(first.join().expect("panicked"), RequestOutcome::Published);
        });
        assert_eq!(changes(&mut consumer), [change(AckPolicy::TwoDisks, false)]);
    }

    /// A request staged during bring-up (between the claim and the attach,
    /// a primary still starting) is journaled as the producer attaches,
    /// after the boot entry and ahead of every runtime request, and never
    /// forced, so it lifts no halt; a runtime `disk` after it is forced.
    #[test]
    fn a_request_staged_during_bring_up_is_journaled_at_attach_in_order() {
        let control = AckPolicyControl::new(AckPolicy::DiskAndRam);
        assert_eq!(control.request(AckPolicy::TwoDisks), RequestOutcome::Staged);
        let claim = control.claim();
        let (mut producer, mut consumer) = ring(8);
        let halt = halted_gate();
        let booted =
            claim.journal_boot(Some(AckPolicy::DiskAndRam.to_byte()), &mut producer, &halt);
        assert_eq!(control.request(AckPolicy::Disk), RequestOutcome::Staged);
        let mut publisher = booted.attach(&mut producer, &halt, None);
        assert_ne!(
            halt.verdict(),
            Verdict::Take,
            "a staged `disk` is no consent"
        );
        let outcome = request_while_polling(
            &control,
            &mut publisher,
            &mut producer,
            AckPolicy::Disk,
            &halt,
        );
        assert_eq!(outcome, RequestOutcome::Published);
        assert_eq!(
            changes(&mut consumer),
            [
                change(AckPolicy::TwoDisks, false),
                change(AckPolicy::Disk, false),
                change(AckPolicy::Disk, true),
            ]
        );
        assert_eq!(halt.verdict(), Verdict::Take, "the forced swap came last");
    }

    /// A policy staged during bring-up that ends up equal to the claim's
    /// own is not journaled again at attach: the boot entry (or the
    /// lineage) already holds it.
    #[test]
    fn a_staged_policy_equal_to_the_claims_is_not_journaled_at_attach() {
        let control = AckPolicyControl::new(AckPolicy::DiskAndRam);
        let claim = control.claim();
        assert_eq!(control.request(AckPolicy::TwoDisks), RequestOutcome::Staged);
        assert_eq!(
            control.request(AckPolicy::DiskAndRam),
            RequestOutcome::Staged
        );
        let (mut producer, mut consumer) = ring::<TestEvent>(8);
        let halt = halted_gate();
        let _publisher = claim
            .journal_boot(Some(AckPolicy::DiskAndRam.to_byte()), &mut producer, &halt)
            .attach(&mut producer, &halt, None);
        assert!(changes(&mut consumer).is_empty());
    }

    /// A parked producer is woken through its handle when a request is
    /// posted.
    #[test]
    fn a_request_wakes_a_parked_producer() {
        use std::os::fd::{AsRawFd, FromRawFd};
        // SAFETY: a fresh nonblocking eventfd, owned at once.
        let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK) };
        assert!(fd >= 0);
        // SAFETY: `fd` is fresh and owned by nothing else.
        let fd = Arc::new(unsafe { OwnedFd::from_raw_fd(fd) });
        let control = control_with_publish_wait(AckPolicy::DiskAndRam, Duration::from_millis(10));
        let (mut producer, _consumer) = ring::<TestEvent>(8);
        let _publisher =
            booted(&control).attach(&mut producer, &gate_over(None), Some(Arc::clone(&fd)));
        assert_eq!(
            control.request(AckPolicy::Disk),
            RequestOutcome::Refused(Refused::RingFull),
            "never polled, so the bound runs out"
        );
        let mut count = 0u64;
        // SAFETY: reads 8 bytes into a live `u64`.
        let read = unsafe {
            libc::read(
                fd.as_raw_fd(),
                (&mut count as *mut u64).cast(),
                std::mem::size_of::<u64>(),
            )
        };
        assert_eq!((read, count), (8, 1), "the eventfd was written once");
    }
}
