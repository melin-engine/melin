//! Replication fencing state: the node's observed epoch plus a one-way
//! "fenced" latch.
//!
//! An **epoch** is a monotonic `u64` bumped on every promotion (see
//! [`melin_journal::JournalEvent::EpochBump`]). It establishes which
//! primary tenure a journaled order belongs to. A node advances its epoch
//! by replaying `EpochBump` entries — on recovery, on the replication
//! stream, and at its own promotion — so the epoch is recovered state, not
//! a separately-maintained counter.
//!
//! The epoch is the **fencing** mechanism for failover: a node that
//! observes an epoch strictly higher than its own (on a replication
//! handshake, in either direction) has been superseded by a newer primary
//! and must stop acting as one. That observation latches [`FenceState`]
//! into the *fenced* state, which the matching stage (halt) and response
//! stage (ack gate) read to stop accepting and acknowledging client work.
//! The latch is one-way: once fenced, the node stays fenced until the
//! process is restarted as a replica by the operator (the manual-failover
//! "hard halt"; auto-rejoin is a separate roadmap item).

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Shared, lock-free fencing state. One instance per node, wrapped in an
/// `Arc` and handed to the matching stage, the response stage, and the
/// replication sender/receiver threads.
///
/// `AtomicU64` + `AtomicBool` (rather than a `Mutex`) because the epoch is
/// read on the replication handshake path and the fenced flag is folded
/// into the matching stage's per-batch halt check — both want a single
/// relaxed load with no contention. `align(64)` keeps the (read-mostly)
/// pair on its own cache line so an unrelated write-hot heap neighbour
/// can't turn those polls into coherence misses — same insurance the
/// pipeline's `CachePadded` cursors buy.
#[derive(Debug)]
#[repr(align(64))]
pub struct FenceState {
    /// Highest fencing epoch this node has observed. Monotonic:
    /// [`Self::observe_epoch`] only ever raises it.
    epoch: AtomicU64,
    /// One-way fenced latch — see the module docs.
    fenced: AtomicBool,
}

impl FenceState {
    /// Construct with a starting epoch (the value recovered from the
    /// journal/snapshot, or `0` for a genesis node).
    pub fn new(initial_epoch: u64) -> Self {
        Self {
            epoch: AtomicU64::new(initial_epoch),
            fenced: AtomicBool::new(false),
        }
    }

    /// Current observed epoch. `Relaxed` is sufficient — the epoch is
    /// advisory recency information advertised on handshakes; it is never
    /// used to synchronise access to other memory.
    #[inline]
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Relaxed)
    }

    /// Raise the observed epoch to `epoch` if it is higher (monotonic
    /// max). Called when an `EpochBump` is replayed/applied. Returns the
    /// epoch in force after the update.
    ///
    /// `fetch_max` keeps the counter monotonic even under the (benign)
    /// race where recovery and the live stream both touch it — the epoch
    /// only ever moves forward.
    #[inline]
    pub fn observe_epoch(&self, epoch: u64) -> u64 {
        let prev = self.epoch.fetch_max(epoch, Ordering::Relaxed);
        prev.max(epoch)
    }

    /// True once the node has been fenced. Folded into the matching
    /// stage's per-batch halt check, so it must be a single relaxed load.
    #[inline]
    pub fn is_fenced(&self) -> bool {
        self.fenced.load(Ordering::Relaxed)
    }

    /// Latch the node into the fenced state. Idempotent. Returns `true`
    /// the first time it transitions (so callers can log the fence once).
    pub fn fence(&self) -> bool {
        !self.fenced.swap(true, Ordering::Relaxed)
    }

    /// Fencing policy, sender side: a peer advertising an epoch strictly
    /// higher than ours means a promotion happened that this node missed —
    /// we are a superseded ex-primary and must self-demote.
    ///
    /// Returns `None` when the peer does not supersede us. Otherwise
    /// latches the fence, co-sets `shutdown` (the invariant "fenced ⇒
    /// shutting down" is owned here, not by each call site), and returns
    /// `Some(first_latch)` so the caller can log the demotion exactly once.
    /// Both replication transports route through this method so the
    /// kernel-TCP and DPDK paths cannot drift apart on when to fence.
    pub fn fence_if_superseded(&self, peer_epoch: u64, shutdown: &AtomicBool) -> Option<bool> {
        if peer_epoch <= self.epoch() {
            return None;
        }
        let first = self.fence();
        // Release: pairs with the Acquire/Relaxed shutdown polls in the
        // stage loops, same convention as the other shutdown publishers.
        shutdown.store(true, Ordering::Release);
        Some(first)
    }

    /// Fencing policy, receiver side: a primary advertising an epoch
    /// strictly lower than ours is a stale ex-primary — following its
    /// divergent lineage would overwrite more-current state, so the
    /// replica must refuse and retry. Not applicable right after a
    /// snapshot rebase (the local state was discarded, so the primary's
    /// epoch is adopted wholesale); callers skip the check there.
    #[inline]
    pub fn refuses_primary(&self, primary_epoch: u64) -> bool {
        primary_epoch < self.epoch()
    }
}

/// Monotonic-max merge for the replay paths that track an epoch *outside*
/// a [`FenceState`] (the recovery accumulator, the shadow stage's
/// snapshot-stamped epoch). One definition so the live dispatch
/// ([`FenceState::observe_epoch`]) and the replay dispatches cannot
/// diverge on `EpochBump` semantics.
#[inline]
pub fn observe_into(current: &mut u64, observed: u64) {
    *current = (*current).max(observed);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_is_monotonic() {
        let f = FenceState::new(3);
        assert_eq!(f.epoch(), 3);
        // Lower values never lower the epoch.
        assert_eq!(f.observe_epoch(1), 3);
        assert_eq!(f.epoch(), 3);
        // Higher values raise it.
        assert_eq!(f.observe_epoch(5), 5);
        assert_eq!(f.epoch(), 5);
    }

    #[test]
    fn fence_latches_once() {
        let f = FenceState::new(0);
        assert!(!f.is_fenced());
        assert!(f.fence()); // first transition
        assert!(f.is_fenced());
        assert!(!f.fence()); // already fenced
        assert!(f.is_fenced());
    }

    #[test]
    fn fence_if_superseded_policy() {
        let f = FenceState::new(3);
        let shutdown = AtomicBool::new(false);

        // Equal or lower peer epoch: not superseded, nothing latches.
        assert_eq!(f.fence_if_superseded(3, &shutdown), None);
        assert_eq!(f.fence_if_superseded(1, &shutdown), None);
        assert!(!f.is_fenced());
        assert!(!shutdown.load(Ordering::Relaxed));

        // Higher peer epoch: fence latches and shutdown co-sets; first
        // transition reports `true`, repeats report `false`.
        assert_eq!(f.fence_if_superseded(4, &shutdown), Some(true));
        assert!(f.is_fenced());
        assert!(shutdown.load(Ordering::Relaxed));
        assert_eq!(f.fence_if_superseded(5, &shutdown), Some(false));
    }

    #[test]
    fn refuses_primary_policy() {
        let f = FenceState::new(2);
        assert!(f.refuses_primary(1)); // stale primary
        assert!(!f.refuses_primary(2)); // same tenure
        assert!(!f.refuses_primary(3)); // newer primary — follow and adopt
    }

    #[test]
    fn observe_into_is_monotonic_max() {
        let mut e = 3u64;
        observe_into(&mut e, 1);
        assert_eq!(e, 3);
        observe_into(&mut e, 7);
        assert_eq!(e, 7);
    }
}
