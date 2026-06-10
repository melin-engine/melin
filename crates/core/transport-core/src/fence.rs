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
/// `Arc` and handed to the matching stage, the response stage, the
/// replication sender/receiver, and the client accept loop.
///
/// `AtomicU64` + `AtomicBool` (rather than a `Mutex`) because the epoch is
/// read on the replication handshake path and the fenced flag is folded
/// into the matching stage's per-event halt check — both want a single
/// relaxed load with no contention.
#[derive(Debug)]
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

    /// True once the node has been fenced. Read on the matching stage's
    /// per-event halt check, so it must be a single relaxed load.
    #[inline]
    pub fn is_fenced(&self) -> bool {
        self.fenced.load(Ordering::Relaxed)
    }

    /// Latch the node into the fenced state. Idempotent. Returns `true`
    /// the first time it transitions (so callers can log the fence once).
    pub fn fence(&self) -> bool {
        !self.fenced.swap(true, Ordering::Relaxed)
    }
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
}
