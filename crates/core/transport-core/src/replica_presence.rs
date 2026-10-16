//! Whether a replicated primary has a replica to stream to, and so whether
//! it halts.
//!
//! A primary with replication configured halts (refuses client writes)
//! while no replica is streaming. The one exception is an operator's
//! explicit consent to serve on this node's disk alone: a runtime
//! `ACK-POLICY disk` sent while no replica is connected latches an
//! *override* that lifts the halt. The override lasts until a replica
//! connects, or a policy that needs a replica is journaled; after that the
//! next loss of the last replica halts the node again. A swap made while a
//! replica is still connected sets none: consent counts only once the
//! operator knows the node has none.
//!
//! [`ReplicaPresence`] holds the count, the override and the one rule over
//! both ([`ReplicaPresence::halted`]), so the readers that refuse writes and
//! the health endpoint that reports `trading`/`halted` cannot disagree.
//!
//! # Cleared by a connect, without a race
//!
//! The override is not a flag a connecting replica clears: the producer
//! that sets it decides on a replica count it read earlier, so a replica
//! connecting in between would clear a flag not yet set, and the override
//! would then outlive a connect. Instead every connect bumps a generation,
//! and the override records the generation its decision observed
//! ([`NoneSince`]). It holds only while the generation is unchanged, so any
//! connect after the observation voids it, however the two threads
//! interleave.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

/// The replica count and the halt override of a replicated primary.
/// Shared, behind an `Arc`, by the replication senders (which count),
/// the input ring's producer (which reads the verdict and sets or clears
/// the override) and the health endpoint.
///
/// Plain atomics, not cache-padded: the senders write only on a connect or
/// a departure, and the readers' steady-state cost is one relaxed load of
/// the count.
#[derive(Debug, Default)]
pub struct ReplicaPresence {
    /// Replicas currently streaming (authenticated, not merely
    /// connected). `u32`: at most a handful of replication slots.
    connected: AtomicU32,
    /// Connects ever made. `u64`, so it never wraps: the generation an
    /// override is bound to.
    connects: AtomicU64,
    /// `0` when no override is latched, otherwise the generation it was
    /// latched at plus one (so a latch at generation zero is not `0`).
    override_at: AtomicU64,
}

/// What the input ring's producer saw when it looked: needed to latch the
/// override, so that only a decision made on an observed absence of
/// replicas can lift the halt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Observed {
    /// At least one replica is streaming.
    Connected,
    /// None is, as of this generation.
    NoneConnected(NoneSince),
}

/// The connect generation at which no replica was streaming. Only
/// [`ReplicaPresence::observe`] makes one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NoneSince(u64);

impl ReplicaPresence {
    /// No replica connected, no override: halted.
    pub fn new() -> Self {
        Self::default()
    }

    /// Replicas currently streaming, for the gauge. Relaxed: a reading.
    #[inline]
    pub(crate) fn connected(&self) -> u32 {
        self.connected.load(Ordering::Relaxed)
    }

    /// A replica has authenticated: lift the halt by one, and void any
    /// override. The count goes up before the generation (both `Release`),
    /// so an observation that sees the new generation sees the count too.
    pub fn lift(&self) {
        self.connected.fetch_add(1, Ordering::Release);
        self.connects.fetch_add(1, Ordering::Release);
    }

    /// A replica left: lower the halt by one. Returns `true` if it was the
    /// last one (the node now halts), emitting the halt warning here so
    /// every sender shares the wording. `fetch_sub` returns the *prior*
    /// count, so `== 1` means this call took it to zero; deriving "last one"
    /// from the returned value rather than a follow-up load avoids a TOCTOU
    /// race with a concurrent reconnect's `lift`. The override needs no
    /// clearing here: the departing replica's own connect came after any
    /// observation an override could have been decided on, and voided it.
    pub fn lower(&self) -> bool {
        let was_last = self.connected.fetch_sub(1, Ordering::Release) == 1;
        if was_last {
            tracing::warn!("all replicas disconnected — halted, refusing client writes");
        }
        was_last
    }

    /// Whether the node halts now: no replica streaming, and no override
    /// in force. One relaxed load while a replica is connected; the
    /// override is read only while none is.
    #[inline]
    pub fn halted(&self) -> bool {
        self.connected.load(Ordering::Relaxed) == 0 && !self.overridden()
    }

    /// Whether an override latched since the last connect is in force.
    fn overridden(&self) -> bool {
        let at = self.override_at.load(Ordering::Relaxed);
        at != 0 && at == self.connects.load(Ordering::Relaxed) + 1
    }

    /// Who is streaming, and as of which connect generation. The
    /// generation is loaded first (`Acquire`, pairing with [`Self::lift`]),
    /// so a connect it counts is reflected in the count read after it.
    #[inline]
    pub fn observe(&self) -> Observed {
        let generation = self.connects.load(Ordering::Acquire);
        if self.connected.load(Ordering::Relaxed) == 0 {
            Observed::NoneConnected(NoneSince(generation))
        } else {
            Observed::Connected
        }
    }

    /// Lift the halt on the operator's consent, given while no replica was
    /// streaming as of `since`. Void if a replica has connected since.
    pub fn latch_override(&self, since: NoneSince) {
        self.override_at.store(since.0 + 1, Ordering::Relaxed);
    }

    /// Drop any override: the next time no replica streams, the node halts.
    pub fn clear_override(&self) {
        self.override_at.store(0, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn none_since(presence: &ReplicaPresence) -> NoneSince {
        match presence.observe() {
            Observed::NoneConnected(since) => since,
            Observed::Connected => panic!("a replica is connected"),
        }
    }

    #[test]
    fn halted_follows_the_count() {
        let presence = ReplicaPresence::new();
        assert!(presence.halted());
        assert_eq!(presence.observe(), Observed::NoneConnected(NoneSince(0)));
        presence.lift();
        presence.lift();
        assert!(!presence.halted());
        assert_eq!(presence.connected(), 2);
        assert_eq!(presence.observe(), Observed::Connected);
        assert!(!presence.lower(), "one left");
        assert!(presence.lower(), "the last one");
        assert!(presence.halted());
        assert_eq!(presence.observe(), Observed::NoneConnected(NoneSince(2)));
    }

    #[test]
    fn an_override_lifts_the_halt_until_cleared() {
        let presence = ReplicaPresence::new();
        presence.latch_override(none_since(&presence));
        assert!(!presence.halted());
        presence.clear_override();
        assert!(presence.halted());
    }

    /// A connect voids the override, so the next departure halts again.
    #[test]
    fn a_connect_voids_the_override() {
        let presence = ReplicaPresence::new();
        presence.latch_override(none_since(&presence));
        presence.lift();
        assert!(!presence.halted());
        presence.lower();
        assert!(
            presence.halted(),
            "the override did not outlive the connect"
        );
    }

    /// The race the generation exists for: a replica connects between the
    /// observation and the latch. The latch lands after the connect and
    /// must still be void.
    #[test]
    fn an_override_decided_before_a_connect_is_void() {
        let presence = ReplicaPresence::new();
        let since = none_since(&presence);
        presence.lift();
        presence.latch_override(since);
        presence.lower();
        assert!(presence.halted());
    }

    /// An override latched after earlier connects holds at its own
    /// generation.
    #[test]
    fn an_override_after_earlier_connects_holds() {
        let presence = ReplicaPresence::new();
        presence.lift();
        presence.lower();
        presence.latch_override(none_since(&presence));
        assert!(!presence.halted());
    }
}
