//! Whether a replicated primary has a replica to stream to, and so whether
//! it halts.
//!
//! A primary with replication configured halts (refuses client writes)
//! while no replica is streaming. The one exception is an operator's
//! explicit consent to serve on this node's disk alone: a runtime
//! `ACK-POLICY disk` sent while no replica is streaming latches an
//! *override* that lifts the halt. The override lasts until a streaming
//! replica acknowledges an entry, or a policy that needs a replica is
//! journaled; after that the next loss of the last replica halts the node
//! again. A swap made while a replica is still streaming sets none:
//! consent counts only once the operator knows the node has none.
//!
//! [`ReplicaPresence`] holds the count, the override and the one rule over
//! both ([`ReplicaPresence::halted`]), so the readers that refuse writes,
//! the rule that tells a forced swap from a witnessed one and the health
//! endpoint that reports `trading`/`halted` cannot disagree.
//!
//! # Counted once its handshake is accepted, override ended by its first ack
//!
//! A replica counts from the moment the primary accepts its handshake and
//! begins streaming to it: its catch-up (or snapshot), then the live
//! stream. Not from authentication: the primary can still refuse a
//! replica after it authenticates (its own epoch superseded, a handshake
//! it cannot validate), and one that retries would lift the halt for a
//! moment on every attempt. One count serves the halt, the forced-swap
//! rule ([`ReplicaPresence::observe`]) and the health endpoint's gauge,
//! so the three agree on what "a replica is connected" means.
//!
//! A replica ends the override only once it acknowledges an entry on that
//! connection ([`FirstAck`]). Being streamed to is not proof enough: a
//! replica that refuses the primary's history (a different genesis, a
//! stale primary by its own reckoning) does so on the stream's first frame
//! and closes, which the primary learns only from the closed connection.
//! Such a replica, retrying, would otherwise void the operator's consent
//! on its first attempt and flap the node back to halted. An ack is the
//! first sign that a replica took the stream, and every replica streaming
//! acknowledges what it receives.
//!
//! # Ended by a first ack, without a race
//!
//! The override is not a flag a replica's ack clears: the producer that
//! sets it decides on a replica count it read earlier, so a replica going
//! live and acking in between would clear a flag not yet set, and the
//! override would then outlive it. Instead every first ack bumps a
//! generation, and the override records the generation its decision
//! observed ([`NoneSince`]). It holds only while the generation is
//! unchanged, so any first ack after the observation voids it, however
//! the threads interleave. A replica counted after the observation that
//! leaves without acking leaves it standing: it never took the stream.
//!
//! # When the replicas went
//!
//! A halted primary's response stage releases the replies it holds on its
//! own fsync once the halt has lasted a grace period, which starts when
//! the count drops to zero. The senders stamp that moment here as they
//! lower the count ([`ReplicaPresence::lower`]), so the grace is measured
//! from the loss itself rather than from whenever the stage next looks
//! ([`ReplicaPresence::halted_for`]). A presence starts out with no
//! replica, so it is stamped when it is made, with the primary pipeline.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// The replica count and the halt override of a replicated primary.
/// Shared, behind an `Arc`, by the replication senders (which count),
/// the input ring's producer (which reads the verdict and sets or clears
/// the override) and the health endpoint.
///
/// Plain atomics, not cache-padded: the senders write only when a
/// replica's handshake is accepted, it first acks, or it leaves, and the
/// readers' steady-state cost is one relaxed load of the count.
#[derive(Debug)]
pub struct ReplicaPresence {
    /// Replicas currently streaming: handshake accepted, catching up or
    /// live, not merely authenticated. `u32`: at most a handful of
    /// replication slots.
    connected: AtomicU32,
    /// First acks ever received, one per live connection. `u64`, so it
    /// never wraps: the generation an override is bound to.
    acked: AtomicU64,
    /// `0` when no override is latched, otherwise the generation it was
    /// latched at plus one (so a latch at generation zero is not `0`).
    override_at: AtomicU64,
    /// When the count last dropped to zero, in nanoseconds since `origin`,
    /// plus one; `0` when unknown (cleared by a replica being counted).
    /// `u64` nanoseconds rather than an `Instant`, which no atomic holds:
    /// they cover centuries of uptime.
    last_lost: AtomicU64,
    /// The zero of `last_lost`.
    origin: Instant,
}

impl Default for ReplicaPresence {
    /// No replica connected, no override: halted, since now.
    fn default() -> Self {
        let presence = Self {
            connected: AtomicU32::new(0),
            acked: AtomicU64::new(0),
            override_at: AtomicU64::new(0),
            last_lost: AtomicU64::new(0),
            origin: Instant::now(),
        };
        presence.stamp_loss();
        presence
    }
}

/// What the input ring's producer saw when it looked: the evidence every
/// change to the override is made on. Latching it needs an observed
/// absence of replicas ([`NoneSince`]), so that only a decision made on
/// one can lift the halt; clearing it consumes the observation the
/// decision was made on ([`ReplicaPresence::clear_override`]), so the
/// override is dropped only by a producer that looked, never by a stray
/// call. Neither `Copy` nor `Clone`: one look settles at most one change.
/// Both variants carry a token with a private field ([`Seen`],
/// [`NoneSince`]), so only [`ReplicaPresence::observe`] makes either.
#[derive(Debug, PartialEq, Eq)]
pub enum Observed {
    /// At least one replica is streaming.
    Connected(Seen),
    /// None is, as of this generation.
    NoneConnected(NoneSince),
}

/// Proof that [`ReplicaPresence::observe`] saw a replica streaming. Only it
/// makes one (the private field), so an [`Observed::Connected`] handed to
/// [`ReplicaPresence::clear_override`] cannot be written out of thin air.
/// Neither `Copy` nor `Clone`, like the observation it comes from.
///
/// ```compile_fail,E0423
/// use melin_transport_core::replica_presence::{Observed, ReplicaPresence, Seen};
/// ReplicaPresence::new().clear_override(Observed::Connected(Seen(())));
/// ```
#[derive(Debug, PartialEq, Eq)]
pub struct Seen(());

/// The first-ack generation at which no replica was streaming. Only
/// [`ReplicaPresence::observe`] makes one; neither `Copy` nor `Clone`,
/// like the observation it comes from.
#[derive(Debug, PartialEq, Eq)]
pub struct NoneSince(u64);

/// What a replica's departure left behind ([`ReplicaPresence::lower`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lowered {
    /// Other replicas still stream.
    OthersRemain,
    /// That was the last one: the node halts.
    LastHalted,
    /// That was the last one, but the operator's forced `disk` override
    /// keeps the node taking writes.
    LastOverridden,
}

/// A counted connection's first ack, still to come. Only
/// [`ReplicaPresence::lift`] makes one, so a sender can report an ack only
/// for a replica it counted, and once per connection however many acks
/// follow: one branch per ack, no atomic after the first.
#[derive(Debug)]
#[must_use = "a live replica's first ack ends the halt override: report every ack with `FirstAck::acked`"]
pub struct FirstAck {
    /// Whether this connection has acked. `bool`: once, then nothing.
    acked: bool,
}

impl FirstAck {
    /// The connection's replica acknowledged an entry: on the first ack,
    /// void any override (bump the generation). `Release`, pairing with
    /// [`ReplicaPresence::observe`]: the count was raised before this, on
    /// the same thread, so an observation that sees the new generation sees
    /// the count too.
    #[inline]
    pub fn acked(&mut self, presence: &ReplicaPresence) {
        if !self.acked {
            self.acked = true;
            presence.acked.fetch_add(1, Ordering::Release);
        }
    }
}

impl ReplicaPresence {
    /// No replica connected, no override: halted, since now.
    pub fn new() -> Self {
        Self::default()
    }

    /// Replicas currently streaming, for the gauge. Relaxed: a reading.
    #[inline]
    pub(crate) fn connected(&self) -> u32 {
        self.connected.load(Ordering::Relaxed)
    }

    /// A replica's handshake was accepted, and the primary streams to it:
    /// lift the halt by one. The override stands until the replica
    /// acknowledges an entry, through the returned [`FirstAck`].
    pub fn lift(&self) -> FirstAck {
        self.connected.fetch_add(1, Ordering::Release);
        // Forget the last loss, so a later one is never measured from an
        // earlier one's stamp.
        self.last_lost.store(0, Ordering::Relaxed);
        FirstAck { acked: false }
    }

    /// A counted replica left: lower the halt by one. Returns whether it
    /// was the last one and, if so, whether the node now halts or an
    /// override keeps it taking writes, logging that here so every sender
    /// shares the wording. `fetch_sub` returns the *prior* count, so `== 1`
    /// means this call took it to zero; deriving "last one" from the
    /// returned value rather than a follow-up load avoids a TOCTOU race
    /// with a concurrent reconnect's `lift`. With the count at zero,
    /// [`halted`](Self::halted) is exactly "no override", so the override
    /// alone decides which. The override needs no clearing here: a
    /// departing replica that acked voided any override decided before its
    /// ack, and one that never acked never took the stream, so the
    /// operator's consent stands.
    ///
    /// The moment is stamped first, in case this is the last, and the count
    /// lowered after it (`Release`), so a reader that sees the count at
    /// zero (`Acquire`, [`halted_for`](Self::halted_for)) sees this stamp
    /// or a later one, never an earlier loss's. A stamp left by a replica
    /// that was not the last is harmless: nothing reads it while the count
    /// is above zero, and the next one counted clears it.
    pub fn lower(&self) -> Lowered {
        self.stamp_loss();
        if self.connected.fetch_sub(1, Ordering::Release) != 1 {
            return Lowered::OthersRemain;
        }
        if self.overridden() {
            tracing::warn!(
                "all replicas disconnected — still taking client writes under the forced `disk` override"
            );
            Lowered::LastOverridden
        } else {
            tracing::warn!("all replicas disconnected — halted, refusing client writes");
            Lowered::LastHalted
        }
    }

    /// Whether the node halts now: no replica streaming, and no override
    /// in force. One relaxed load while a replica is connected; the
    /// override is read only while none is.
    #[inline]
    pub fn halted(&self) -> bool {
        self.connected.load(Ordering::Relaxed) == 0 && !self.overridden()
    }

    /// How long no replica has been streaming, or `None` while one is. The
    /// halt as the count alone defines it: the operator's override, which
    /// only ever stands beside the `disk` policy, does not change it.
    ///
    /// When the moment of the loss is unknown (a replica counted after the
    /// leaving one wrote its stamp cleared it, and has left again before
    /// stamping), the halt reads as having just begun, which errs towards a
    /// longer wait. A caller that polls keeps the earliest start it has
    /// seen in that case, so the halt's age still grows from its first
    /// sighting.
    #[inline]
    pub fn halted_for(&self, now: Instant) -> Option<Duration> {
        if self.connected.load(Ordering::Acquire) != 0 {
            return None;
        }
        let stamp = self.last_lost.load(Ordering::Relaxed);
        if stamp == 0 {
            return Some(Duration::ZERO);
        }
        let lost_at = self.origin + Duration::from_nanos(stamp - 1);
        Some(now.saturating_duration_since(lost_at))
    }

    fn stamp_loss(&self) {
        // Saturated rather than an error: `u64` nanoseconds overflow only
        // after centuries of uptime, and the stamp then still reads as a
        // halt that began long ago.
        let since_origin = u64::try_from(self.origin.elapsed().as_nanos()).unwrap_or(u64::MAX - 1);
        self.last_lost.store(since_origin + 1, Ordering::Relaxed);
    }

    /// Whether an override latched since the last first ack is in force.
    fn overridden(&self) -> bool {
        let at = self.override_at.load(Ordering::Relaxed);
        at != 0 && at == self.acked.load(Ordering::Relaxed) + 1
    }

    /// Who is streaming, and as of which first-ack generation. The
    /// generation is loaded first (`Acquire`, pairing with
    /// [`FirstAck::acked`]), so a first ack it counts is reflected in the
    /// count read after it.
    #[inline]
    pub fn observe(&self) -> Observed {
        let generation = self.acked.load(Ordering::Acquire);
        if self.connected.load(Ordering::Relaxed) == 0 {
            Observed::NoneConnected(NoneSince(generation))
        } else {
            Observed::Connected(Seen(()))
        }
    }

    /// Lift the halt on the operator's consent, given while no replica was
    /// streaming as of `since`. Void if a replica has first acked since.
    pub fn latch_override(&self, since: NoneSince) {
        self.override_at.store(since.0 + 1, Ordering::Relaxed);
    }

    /// Drop any override: the next time no replica streams, the node halts.
    /// Takes the observation the decision to drop it was made on (a policy
    /// that needs a replica being journaled), whatever it saw: without one
    /// in hand, nothing can drop the operator's consent.
    pub fn clear_override(&self, _decided_on: Observed) {
        self.override_at.store(0, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn none_since(presence: &ReplicaPresence) -> NoneSince {
        match presence.observe() {
            Observed::NoneConnected(since) => since,
            Observed::Connected(_) => panic!("a replica is connected"),
        }
    }

    #[test]
    fn halted_follows_the_count() {
        let presence = ReplicaPresence::new();
        assert!(presence.halted());
        assert_eq!(presence.observe(), Observed::NoneConnected(NoneSince(0)));
        let mut first = presence.lift();
        let mut second = presence.lift();
        assert!(!presence.halted());
        assert_eq!(presence.connected(), 2);
        assert_eq!(presence.observe(), Observed::Connected(Seen(())));
        first.acked(&presence);
        first.acked(&presence);
        second.acked(&presence);
        assert_eq!(presence.lower(), Lowered::OthersRemain, "one left");
        assert_eq!(presence.lower(), Lowered::LastHalted, "the last one");
        assert!(presence.halted());
        assert_eq!(
            presence.observe(),
            Observed::NoneConnected(NoneSince(2)),
            "one generation per connection's first ack, not per ack"
        );
    }

    // Latching, clearing and a first ack voiding the override are pinned
    // through the halt gate, in `melin-server-runtime`'s `halt.rs` tests.

    /// The race the generation exists for: a replica is counted and acks
    /// between the observation and the latch. The latch lands after the
    /// ack and must still be void.
    #[test]
    fn an_override_decided_before_a_first_ack_is_void() {
        let presence = ReplicaPresence::new();
        let since = none_since(&presence);
        presence.lift().acked(&presence);
        presence.latch_override(since);
        assert_eq!(presence.lower(), Lowered::LastHalted);
        assert!(presence.halted());
    }

    /// The last replica leaving reports the truth the halt rule tells:
    /// halted without an override, still taking writes under one, and
    /// halted again once a first ack voided it.
    #[test]
    fn the_last_departure_reports_whether_an_override_holds() {
        let presence = ReplicaPresence::new();
        presence.latch_override(none_since(&presence));
        drop(presence.lift());
        assert_eq!(
            presence.lower(),
            Lowered::LastOverridden,
            "one that never acked leaves the override"
        );
        assert!(!presence.halted());

        let mut first = presence.lift();
        drop(presence.lift());
        first.acked(&presence);
        assert_eq!(presence.lower(), Lowered::OthersRemain);
        assert_eq!(presence.lower(), Lowered::LastHalted, "the ack voided it");
        assert!(presence.halted());
    }

    /// A primary starts with no replica: halted from the presence's making.
    #[test]
    fn the_halt_is_timed_from_the_making_on_a_node_that_starts_alone() {
        let before = Instant::now();
        let presence = ReplicaPresence::new();
        let halted_for = presence
            .halted_for(before + Duration::from_secs(5))
            .expect("no replica");
        assert!(halted_for <= Duration::from_secs(5), "{halted_for:?}");
        assert!(
            halted_for >= Duration::from_secs(4),
            "timed from the making, not from later: {halted_for:?}"
        );
    }

    #[test]
    fn the_halt_is_timed_from_the_last_replica_leaving() {
        let presence = ReplicaPresence::new();
        drop(presence.lift());
        assert_eq!(presence.halted_for(Instant::now()), None, "a replica is up");

        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(presence.lower(), Lowered::LastHalted);
        let left = Instant::now();
        let halted_for = presence
            .halted_for(left + Duration::from_secs(3))
            .expect("no replica");
        assert!(
            halted_for >= Duration::from_secs(3) && halted_for < Duration::from_secs(4),
            "{halted_for:?}"
        );
    }

    /// A replica counted clears the stamp, so a loss whose stamp is missing
    /// reads as a halt that just began, never as an earlier loss's.
    #[test]
    fn an_unknown_loss_reads_as_a_halt_that_just_began() {
        let presence = ReplicaPresence::new();
        drop(presence.lift());
        // The departure as seen before its stamp lands: the count only.
        presence.connected.fetch_sub(1, Ordering::Release);
        assert_eq!(
            presence.halted_for(Instant::now() + Duration::from_secs(60)),
            Some(Duration::ZERO)
        );
    }

    /// An override latched after earlier first acks holds at its own
    /// generation.
    #[test]
    fn an_override_after_earlier_first_acks_holds() {
        let presence = ReplicaPresence::new();
        presence.lift().acked(&presence);
        assert_eq!(presence.lower(), Lowered::LastHalted);
        presence.latch_override(none_since(&presence));
        assert!(!presence.halted());
    }
}
