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
//!   the lineage holds a different one or none ([`boot_change`]).
//! - **While the node serves**, the claim is attached to the producer
//!   ([`PublisherClaim::attach`]), which checks the mailbox on its loop
//!   ([`AckPolicyPublisher::poll`]) and journals each request. The admin
//!   command replies once the entry is on the ring, or refuses if the ring
//!   stays full past a bounded wait ([`AckPolicyControl::request`]).
//! - **With no producer attached** (a replica, or a primary still
//!   starting), a request is staged: a replica journals it when promoted,
//!   a primary when its producer attaches.
//!
//! Every request is journaled, duplicates included: the entries are the
//! audit trail of what operators asked for, and a re-sent request is how
//! an operator forces a swap a departed replica would otherwise hold up.
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
//! so its entries are all unwitnessed too. The rule is [`is_unwitnessed`].

use std::os::fd::OwnedFd;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use melin_app::{AppEvent, unix_epoch_nanos};
use melin_journal::{AckPolicyByte, AckPolicyChange, JournalEvent};
use melin_pipeline::ring;
use melin_transport_core::pipeline::InputSlot;
use tracing::{info, warn};

use crate::ack_policy::AckPolicy;

/// How long an `ACK-POLICY` command waits for its entry to be published
/// before it is refused. The producer checks the mailbox on every loop, so
/// only a full input ring (a pipeline stalled behind its gate) holds a
/// request this long; the admin connection's own timeout is longer, so the
/// operator reads the refusal.
pub const PUBLISH_WAIT: Duration = Duration::from_secs(2);

/// Who could witness a change right now: the replicas a primary streams
/// to, as the ring's producer sees them (`HaltGate::witnesses`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Witnesses {
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
pub enum Origin {
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
pub fn is_unwitnessed(policy: AckPolicy, origin: Origin, witnesses: Witnesses) -> bool {
    match witnesses {
        Witnesses::Disabled => true,
        Witnesses::NoneConnected => origin == Origin::Runtime && policy == AckPolicy::Disk,
        Witnesses::Connected => false,
    }
}

/// The change a primary journals as it starts or is promoted, from the
/// main thread before any other producer runs: the requested policy, when
/// the lineage holds a different one, or none yet (a new history, or a
/// lineage written before policy changes were journaled).
pub fn boot_change(
    lineage: Option<AckPolicyByte>,
    requested: AckPolicy,
    witnesses: Witnesses,
) -> Option<AckPolicyChange> {
    let policy = requested.to_byte();
    (lineage != Some(policy)).then(|| AckPolicyChange {
        policy,
        unwitnessed: is_unwitnessed(requested, Origin::Staged, witnesses),
    })
}

/// The input-ring slot that journals `change`.
pub fn change_slot<E: AppEvent>(change: AckPolicyChange) -> InputSlot<E> {
    InputSlot::node_event(JournalEvent::AckPolicyChange(change), unix_epoch_nanos())
}

/// Log a change being journaled, at the level its risk calls for.
fn log_journaled(policy: AckPolicy, change: AckPolicyChange, origin: Origin, witnesses: Witnesses) {
    if change.unwitnessed && witnesses == Witnesses::NoneConnected {
        warn!(
            policy = %policy,
            "ack policy change forced with no replica connected: it takes effect once on this \
             node's disk, and a replica that was offline does not hold it — if this node fails \
             before that replica rejoins, it may auto-promote over events acknowledged under \
             the new policy"
        );
    } else {
        info!(
            policy = %policy,
            unwitnessed = change.unwitnessed,
            origin = ?origin,
            "ack policy change journaled"
        );
    }
}

/// What became of an `ACK-POLICY` request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestOutcome {
    /// Journaled: the entry is on the input ring.
    Published,
    /// No producer runs on this node (a replica, or a primary still
    /// starting): the policy is kept, and journaled when one does.
    Staged,
    /// The entry could not be published within the bounded wait (the
    /// input ring stayed full), or the producer stopped: nothing changed.
    Refused,
}

/// A request waiting for the producer.
#[derive(Clone, Copy, Debug)]
struct Request {
    policy: AckPolicy,
    origin: Origin,
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

impl Producer {
    fn is_attached(&self) -> bool {
        matches!(self, Self::Attached { .. })
    }
}

/// The mailbox's state. Behind one mutex: it is touched by the admin
/// thread per command and by the producer only when a request waits, never
/// on the producer's steady-state path (that reads [`AckPolicyControl::pending`]).
struct State {
    /// The policy the next boot or promotion requests: the configured
    /// one, then each one staged or journaled.
    staged: AckPolicy,
    /// Bumped by every request staged while no producer is attached, so an
    /// attaching producer can tell whether one landed after its claim.
    /// `u64`: a count of operator commands, never near wrapping.
    staged_generation: u64,
    /// Whether a producer is attached, and how to wake it.
    producer: Producer,
    /// The request waiting for the producer, if any.
    request: Option<Request>,
    /// The last ticket handed out, and the last one published.
    issued: u64,
    published: u64,
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
    /// The requested policy's byte, for a reader that does not take the
    /// lock: auto-promotion's fallback, used only while a replica's
    /// lineage holds no change. Updated when a request is staged or
    /// journaled.
    requested: Arc<AtomicU8>,
    publish_wait: Duration,
}

impl AckPolicyControl {
    /// A mailbox starting from the configured policy.
    pub fn new(configured: AckPolicy) -> Arc<Self> {
        Self::with_publish_wait(configured, PUBLISH_WAIT)
    }

    /// As [`Self::new`], with the bound a request waits for its entry.
    pub fn with_publish_wait(configured: AckPolicy, publish_wait: Duration) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                staged: configured,
                staged_generation: 0,
                producer: Producer::Detached,
                request: None,
                issued: 0,
                published: 0,
            }),
            changed: Condvar::new(),
            pending: AtomicBool::new(false),
            requested: Arc::new(AtomicU8::new(configured.as_u8())),
            publish_wait,
        })
    }

    /// The requested policy's byte, shared: see the field.
    pub fn requested_byte(&self) -> Arc<AtomicU8> {
        Arc::clone(&self.requested)
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
    pub fn request(&self, policy: AckPolicy) -> RequestOutcome {
        let deadline = Instant::now() + self.publish_wait;
        let mut state = self.lock();
        // A request already waiting (one staged before the producer
        // attached) is published first: two requests are never merged.
        while state.request.is_some() && state.producer.is_attached() {
            let Some(left) = deadline.checked_duration_since(Instant::now()) else {
                return RequestOutcome::Refused;
            };
            state = self.wait(state, left);
        }
        let Producer::Attached { wake } = &state.producer else {
            state.staged = policy;
            state.staged_generation += 1;
            self.requested.store(policy.as_u8(), Ordering::Relaxed);
            return RequestOutcome::Staged;
        };
        let wake = wake.clone();
        state.issued += 1;
        let ticket = state.issued;
        state.request = Some(Request {
            policy,
            origin: Origin::Runtime,
            ticket,
        });
        self.pending.store(true, Ordering::Release);
        if let Some(fd) = wake {
            crate::reader::wake_reader(&fd);
        }
        loop {
            if state.published >= ticket {
                return RequestOutcome::Published;
            }
            let left = deadline.checked_duration_since(Instant::now());
            match left {
                Some(left) if state.producer.is_attached() => state = self.wait(state, left),
                _ => {
                    // Withdrawn under the lock the producer publishes
                    // under, so it is either on the ring (checked above)
                    // or never will be.
                    if state
                        .request
                        .is_some_and(|waiting| waiting.ticket == ticket)
                    {
                        state.request = None;
                        self.pending.store(false, Ordering::Release);
                        self.changed.notify_all();
                    }
                    return RequestOutcome::Refused;
                }
            }
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
    pub fn claim(self: &Arc<Self>) -> PublisherClaim {
        let state = self.lock();
        PublisherClaim {
            control: Arc::clone(self),
            policy: state.staged,
            staged_generation: state.staged_generation,
        }
    }
}

/// The policy a primary requests as it starts, and the token its input
/// ring's producer attaches with ([`Self::attach`]).
pub struct PublisherClaim {
    control: Arc<AckPolicyControl>,
    policy: AckPolicy,
    staged_generation: u64,
}

impl PublisherClaim {
    /// The policy to journal as the primary starts (see [`boot_change`]).
    pub fn policy(&self) -> AckPolicy {
        self.policy
    }

    /// Attach the input ring's producer. `wake` is the handle that brings
    /// a parked producer back round its loop (the io_uring reader's
    /// eventfd), `None` for one that never parks (the DPDK poll thread).
    ///
    /// A request staged after the claim (between the main thread's boot
    /// entry and now) is queued for the producer, so it is not lost.
    pub fn attach(self, wake: Option<Arc<OwnedFd>>) -> AckPolicyPublisher {
        let control = self.control;
        {
            let mut state = control.lock();
            state.producer = Producer::Attached { wake };
            if state.staged_generation != self.staged_generation {
                state.issued += 1;
                state.request = Some(Request {
                    policy: state.staged,
                    origin: Origin::Staged,
                    ticket: state.issued,
                });
                control.pending.store(true, Ordering::Release);
            }
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
    /// `witnesses` is read only when a request waits: it decides whether
    /// the entry is unwitnessed ([`is_unwitnessed`]).
    #[inline]
    pub fn poll<E: AppEvent>(
        &mut self,
        producer: &mut ring::Producer<InputSlot<E>>,
        witnesses: impl FnOnce() -> Witnesses,
    ) {
        if self.control.pending.load(Ordering::Acquire) {
            self.publish(producer, witnesses());
        }
    }

    #[cold]
    fn publish<E: AppEvent>(
        &mut self,
        producer: &mut ring::Producer<InputSlot<E>>,
        witnesses: Witnesses,
    ) {
        let control = &*self.control;
        let mut state = control.lock();
        let Some(request) = state.request else {
            // Withdrawn since the flag was read.
            return;
        };
        let change = AckPolicyChange {
            policy: request.policy.to_byte(),
            unwitnessed: is_unwitnessed(request.policy, request.origin, witnesses),
        };
        if producer.try_publish(change_slot(change)).is_err() {
            // Full: the requester's bounded wait decides when to give up.
            return;
        }
        state.request = None;
        state.published = request.ticket;
        state.staged = request.policy;
        control
            .requested
            .store(request.policy.as_u8(), Ordering::Relaxed);
        control.pending.store(false, Ordering::Release);
        control.changed.notify_all();
        drop(state);
        log_journaled(request.policy, change, request.origin, witnesses);
    }
}

impl Drop for AckPolicyPublisher {
    fn drop(&mut self) {
        let mut state = self.control.lock();
        state.producer = Producer::Detached;
        // A runtime request still waiting is refused by its requester, who
        // sees it gone unpublished; a staged one is already the staged
        // policy.
        state.request = None;
        self.control.pending.store(false, Ordering::Release);
        self.control.changed.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use counter_server::CounterEvent as TestEvent;
    use melin_pipeline::ring::{Consumer, DisruptorBuilder};
    use melin_pipeline::wait::WaitStrategy;

    const ALL: [AckPolicy; 4] = [
        AckPolicy::Disk,
        AckPolicy::Ram,
        AckPolicy::DiskAndRam,
        AckPolicy::TwoDisks,
    ];

    type Slot = InputSlot<TestEvent>;

    fn ring(capacity: usize) -> (ring::Producer<Slot>, Consumer<Slot>) {
        let (producer, mut consumers) = DisruptorBuilder::<Slot>::new(capacity)
            .add_consumer()
            .build(WaitStrategy::SpinThenYield);
        (producer, consumers.pop().expect("one consumer"))
    }

    /// Every change on the ring, in order.
    fn changes(consumer: &mut Consumer<Slot>) -> Vec<AckPolicyChange> {
        let mut out = Vec::new();
        while let Some((_, slot)) = consumer.try_consume() {
            match slot.event {
                JournalEvent::AckPolicyChange(change) => {
                    assert_eq!((slot.connection_id, slot.key_hash), (0, 0), "a node event");
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

    /// The boot entry: none when the lineage already holds the requested
    /// policy; otherwise the requested policy, witnessed unless
    /// replication is disabled (a boot entry is never forced).
    #[test]
    fn the_boot_change() {
        let disk = Some(AckPolicy::Disk.to_byte());
        assert_eq!(
            boot_change(disk, AckPolicy::Disk, Witnesses::NoneConnected),
            None
        );
        assert_eq!(
            boot_change(disk, AckPolicy::Disk, Witnesses::Disabled),
            None
        );
        assert_eq!(
            boot_change(disk, AckPolicy::DiskAndRam, Witnesses::NoneConnected),
            Some(change(AckPolicy::DiskAndRam, false))
        );
        assert_eq!(
            boot_change(None, AckPolicy::Disk, Witnesses::NoneConnected),
            Some(change(AckPolicy::Disk, false)),
            "a lineage holding none gets one, never forced"
        );
        assert_eq!(
            boot_change(
                Some(AckPolicy::DiskAndRam.to_byte()),
                AckPolicy::Disk,
                Witnesses::Disabled
            ),
            Some(change(AckPolicy::Disk, true)),
            "standalone: unwitnessed"
        );
    }

    /// With no producer attached a request is staged: the next claim
    /// requests it, and the shared byte follows.
    #[test]
    fn a_request_with_no_producer_is_staged_for_the_next_claim() {
        let control = AckPolicyControl::new(AckPolicy::DiskAndRam);
        assert_eq!(control.claim().policy(), AckPolicy::DiskAndRam);
        assert_eq!(control.request(AckPolicy::Ram), RequestOutcome::Staged);
        assert_eq!(
            control.requested_byte().load(Ordering::Relaxed),
            AckPolicy::Ram.as_u8()
        );
        assert_eq!(control.claim().policy(), AckPolicy::Ram);
    }

    /// Run `request` on a helper thread while this one plays the producer:
    /// polling `publisher` on `producer` until the request returns.
    fn request_while_polling(
        control: &Arc<AckPolicyControl>,
        publisher: &mut AckPolicyPublisher,
        producer: &mut ring::Producer<Slot>,
        policy: AckPolicy,
        witnesses: Witnesses,
    ) -> RequestOutcome {
        std::thread::scope(|s| {
            let requesting = s.spawn(|| control.request(policy));
            while !requesting.is_finished() {
                publisher.poll(producer, || witnesses);
                std::thread::yield_now();
            }
            requesting.join().expect("the request panicked")
        })
    }

    /// An attached producer journals each request, in order, duplicates
    /// included, and the requester returns once the entry is on the ring.
    /// The forced decision is the producer's, at publish time.
    #[test]
    fn an_attached_producer_journals_every_request() {
        let control = AckPolicyControl::new(AckPolicy::DiskAndRam);
        let (mut producer, mut consumer) = ring(8);
        let mut publisher = control.claim().attach(None);

        for (policy, witnesses) in [
            (AckPolicy::Disk, Witnesses::Connected),
            (AckPolicy::Disk, Witnesses::Connected),
            (AckPolicy::Disk, Witnesses::NoneConnected),
            (AckPolicy::TwoDisks, Witnesses::NoneConnected),
        ] {
            let outcome =
                request_while_polling(&control, &mut publisher, &mut producer, policy, witnesses);
            assert_eq!(outcome, RequestOutcome::Published);
        }
        assert_eq!(
            changes(&mut consumer),
            [
                change(AckPolicy::Disk, false),
                change(AckPolicy::Disk, false),
                change(AckPolicy::Disk, true),
                change(AckPolicy::TwoDisks, false),
            ]
        );
        assert_eq!(
            control.requested_byte().load(Ordering::Relaxed),
            AckPolicy::TwoDisks.as_u8()
        );
        assert_eq!(control.claim().policy(), AckPolicy::TwoDisks);
    }

    /// Nothing waits: polling publishes nothing and never asks who is
    /// connected.
    #[test]
    fn polling_with_nothing_requested_publishes_nothing() {
        let control = AckPolicyControl::new(AckPolicy::Disk);
        let (mut producer, mut consumer) = ring(8);
        let mut publisher = control.claim().attach(None);
        publisher.poll(&mut producer, || {
            panic!("witnesses read with nothing to publish")
        });
        assert!(changes(&mut consumer).is_empty());
    }

    /// A ring that stays full past the bound refuses the request, which
    /// is then withdrawn: it is not published once the ring drains, and
    /// the requested policy is unchanged.
    #[test]
    fn a_full_ring_refuses_the_request_and_withdraws_it() {
        let control =
            AckPolicyControl::with_publish_wait(AckPolicy::DiskAndRam, Duration::from_millis(50));
        let (mut producer, mut consumer) = ring(2);
        for _ in 0..2 {
            producer
                .try_publish(change_slot(change(AckPolicy::Ram, false)))
                .expect("room");
        }
        let mut publisher = control.claim().attach(None);
        let outcome = request_while_polling(
            &control,
            &mut publisher,
            &mut producer,
            AckPolicy::Disk,
            Witnesses::Connected,
        );
        assert_eq!(outcome, RequestOutcome::Refused);
        assert_eq!(changes(&mut consumer).len(), 2, "only what filled the ring");
        publisher.poll(&mut producer, || Witnesses::Connected);
        assert!(changes(&mut consumer).is_empty(), "withdrawn");
        assert_eq!(
            control.requested_byte().load(Ordering::Relaxed),
            AckPolicy::DiskAndRam.as_u8()
        );
    }

    /// A producer that stops (shutdown) refuses the request waiting on it,
    /// without the full bound, and later requests are staged.
    #[test]
    fn a_detached_producer_refuses_the_waiting_request() {
        let control = AckPolicyControl::with_publish_wait(AckPolicy::Disk, Duration::from_secs(60));
        let publisher = control.claim().attach(None);
        let started = Instant::now();
        let outcome = std::thread::scope(|s| {
            let requesting = s.spawn(|| control.request(AckPolicy::Ram));
            while !control.pending.load(Ordering::Acquire) {
                std::thread::yield_now();
            }
            drop(publisher);
            requesting.join().expect("the request panicked")
        });
        assert_eq!(outcome, RequestOutcome::Refused);
        assert!(started.elapsed() < Duration::from_secs(30), "not the bound");
        assert!(!control.pending.load(Ordering::Acquire));
        assert_eq!(control.request(AckPolicy::Ram), RequestOutcome::Staged);
    }

    /// A request staged between the claim and the attach (a primary still
    /// starting) is journaled by the producer once it attaches, never
    /// forced; one staged before the claim is the claim's own policy and
    /// is not journaled twice.
    #[test]
    fn a_request_staged_after_the_claim_is_journaled_at_attach() {
        let control = AckPolicyControl::new(AckPolicy::DiskAndRam);
        assert_eq!(control.request(AckPolicy::TwoDisks), RequestOutcome::Staged);
        let claim = control.claim();
        assert_eq!(claim.policy(), AckPolicy::TwoDisks);
        assert_eq!(control.request(AckPolicy::Disk), RequestOutcome::Staged);
        let (mut producer, mut consumer) = ring(8);
        let mut publisher = claim.attach(None);
        publisher.poll(&mut producer, || Witnesses::NoneConnected);
        assert_eq!(changes(&mut consumer), [change(AckPolicy::Disk, false)]);

        let control = AckPolicyControl::new(AckPolicy::DiskAndRam);
        assert_eq!(control.request(AckPolicy::TwoDisks), RequestOutcome::Staged);
        let mut publisher = control.claim().attach(None);
        publisher.poll(&mut producer, || Witnesses::NoneConnected);
        assert!(changes(&mut consumer).is_empty());
    }

    /// A runtime request made while a staged one still waits for the
    /// producer is published after it, never merged into it.
    #[test]
    fn a_runtime_request_queues_behind_a_staged_one() {
        let control = AckPolicyControl::new(AckPolicy::DiskAndRam);
        let claim = control.claim();
        assert_eq!(control.request(AckPolicy::TwoDisks), RequestOutcome::Staged);
        let (mut producer, mut consumer) = ring(8);
        let mut publisher = claim.attach(None);
        let outcome = request_while_polling(
            &control,
            &mut publisher,
            &mut producer,
            AckPolicy::Disk,
            Witnesses::NoneConnected,
        );
        assert_eq!(outcome, RequestOutcome::Published);
        assert_eq!(
            changes(&mut consumer),
            [
                change(AckPolicy::TwoDisks, false),
                change(AckPolicy::Disk, true)
            ]
        );
    }

    /// A staged request the producer cannot publish in time holds the
    /// next request off until the bound, which refuses it.
    #[test]
    fn a_request_behind_one_that_never_publishes_is_refused() {
        let control =
            AckPolicyControl::with_publish_wait(AckPolicy::DiskAndRam, Duration::from_millis(50));
        let claim = control.claim();
        assert_eq!(control.request(AckPolicy::TwoDisks), RequestOutcome::Staged);
        let _publisher = claim.attach(None);
        assert_eq!(control.request(AckPolicy::Disk), RequestOutcome::Refused);
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
        let control =
            AckPolicyControl::with_publish_wait(AckPolicy::DiskAndRam, Duration::from_millis(10));
        let _publisher = control.claim().attach(Some(Arc::clone(&fd)));
        assert_eq!(control.request(AckPolicy::Disk), RequestOutcome::Refused);
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
