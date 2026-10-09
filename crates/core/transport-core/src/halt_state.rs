//! The replica-loss halt's shared state: the operator's override.
//!
//! A primary whose last replica has left halts: it refuses new client
//! writes, under every ack policy. Under `disk` the halt is not about the
//! ack contract (the primary's own fsync satisfies it) but about fencing:
//! a primary a partition has cut off from its replica hears of a promotion
//! on the other side only once the partition heals, so the halt is what
//! stops it from acking a history the cluster is about to abandon.
//!
//! # The operator's override
//!
//! An operator who knows the node is alone — a freshly promoted primary
//! whose replicas are gone, say — can consent to single-copy operation by
//! swapping the policy to `disk` with `ACK-POLICY disk`. That swap, made
//! while no replica is connected, latches the override here, and the halt
//! lifts. The latch clears when a replica starts streaming again, or when
//! the policy is swapped back to one that needs a replica; the next loss
//! of the last replica then halts the node again. A swap to `disk` made
//! while a replica is still connected latches nothing: consent counts
//! only once the operator knows the node has none.
//!
//! A policy set at boot never latches. The override is a statement about
//! one node at one moment, made knowingly; whether an unattended,
//! isolated primary should keep writing is a question for a leader lease,
//! not for the policy value.

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};

/// What a policy swap did to the override. Returned so the admin handler
/// can log, for the audit trail, whether the operator's swap lifted the
/// halt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SwapEffect {
    /// A swap to `disk` with no replica connected: the halt is lifted
    /// until a replica streams again or the policy goes back to one that
    /// needs a replica.
    Lifted,
    /// A swap to `disk` while a replica is connected: nothing latched, so
    /// the node still halts when that replica leaves.
    ReplicaConnected,
    /// A swap on a node that does not serve as a replicated primary (a
    /// replica before its promotion, or a standalone node): nothing
    /// latched. A replica's pre-staged policy carries over a promotion,
    /// but the consent to run alone has to be given once the node is
    /// alone.
    NotServing,
    /// A swap to a policy that needs a replica: the latch is cleared, if
    /// it was set (`was_lifted`).
    Cleared {
        /// The latch was set before this swap, so the swap restored the
        /// halt (on a node with no replica connected).
        was_lifted: bool,
    },
}

/// The halt's shared state: the override latch and the replica count it
/// is judged against. One per process, shared by the admin handler (sets
/// and clears the latch), the replication senders (clear the latch), and
/// the readers and the health endpoint (read the latch).
///
/// Atomics rather than anything heavier because the readers fold the
/// latch into their per-receive halt check, and only when the replica
/// count is already zero: one relaxed load on a path that is refusing
/// writes anyway.
#[derive(Debug, Default)]
pub struct HaltState {
    /// The latch. Written with `SeqCst` (see [`HaltState::on_policy_swap`]),
    /// read `Relaxed` by the readers.
    lifted: AtomicBool,
    /// The node's replica count, attached once the node serves as a
    /// replicated primary. A `OnceLock` because the count is created with
    /// the pipeline, after the admin endpoint (which must exist from boot,
    /// on a replica too) holds this state; a node becomes a primary at
    /// most once per process.
    replicas_connected: OnceLock<Arc<AtomicU32>>,
}

impl HaltState {
    /// An unlatched state, attached to no replica count yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Attach the replica count of the pipeline this node now serves.
    /// Called once, when the node starts serving as a replicated primary.
    /// `Err` if a count is already attached: a node serves as a primary
    /// once per process, so a second call is a bug in the caller.
    pub fn attach(&self, replicas_connected: Arc<AtomicU32>) -> Result<(), &'static str> {
        self.replicas_connected
            .set(replicas_connected)
            .map_err(|_| "a replica count is already attached to the halt state")
    }

    /// Whether the operator's override is latched. One relaxed load: a
    /// latch that changes between this load and the reader's publish is
    /// the same race as a halt that starts there, which the reader's halt
    /// check already accepts.
    #[inline]
    pub fn is_lifted(&self) -> bool {
        self.lifted.load(Ordering::Relaxed)
    }

    /// Record an operator's policy swap. `single_copy` is whether the new
    /// policy is satisfied by one node's disk alone (`disk`).
    ///
    /// A swap to `disk` latches the override only if no replica is
    /// connected, and the check has to hold against a replica that
    /// starts streaming at the same moment, whose
    /// [`on_replica_streaming`](Self::on_replica_streaming) must not be
    /// lost. Hence the order — load the count, latch, load it again and
    /// undo the latch if a replica showed up — all `SeqCst`, as is the
    /// replication senders' increment of the count and their clear: of
    /// the senders' increment and this handler's second load, one comes
    /// first in the single total order. If the increment does, the second
    /// load sees it and the latch is undone; if the load does, the
    /// sender's clear (which follows its increment) comes after the
    /// latch, and undoes it.
    pub fn on_policy_swap(&self, single_copy: bool) -> SwapEffect {
        if !single_copy {
            let was_lifted = self.lifted.swap(false, Ordering::SeqCst);
            return SwapEffect::Cleared { was_lifted };
        }
        let Some(count) = self.replicas_connected.get() else {
            return SwapEffect::NotServing;
        };
        if count.load(Ordering::SeqCst) > 0 {
            return SwapEffect::ReplicaConnected;
        }
        self.lifted.store(true, Ordering::SeqCst);
        if count.load(Ordering::SeqCst) > 0 {
            self.lifted.store(false, Ordering::SeqCst);
            return SwapEffect::ReplicaConnected;
        }
        SwapEffect::Lifted
    }

    /// A replica has started streaming: clear the latch, so that its
    /// departure halts the node again. Returns whether the latch was set.
    /// The caller increments the replica count, `SeqCst`, before this
    /// (see [`on_policy_swap`](Self::on_policy_swap)).
    pub fn on_replica_streaming(&self) -> bool {
        self.lifted.swap(false, Ordering::SeqCst)
    }

    /// Whether a replicated primary with `replicas_connected` replicas is
    /// halted: none connected and no override latched. The readers' halt
    /// gate's definition; the health endpoint folds the same two inputs
    /// into its `trading` flag. `replicas_connected` is `None` on a
    /// standalone node, which never halts for want of a replica.
    ///
    /// The count is passed in rather than read from the attached one so
    /// the per-write check stays two relaxed loads: the attached count
    /// sits behind a `OnceLock`, whose `get` is one more acquire load.
    /// In production the caller passes the same `Arc` it attached.
    #[inline]
    pub fn halted(&self, replicas_connected: Option<&AtomicU32>) -> bool {
        replicas_connected.is_some_and(|count| count.load(Ordering::Relaxed) == 0)
            && !self.is_lifted()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attached(count: u32) -> (HaltState, Arc<AtomicU32>) {
        let replicas = Arc::new(AtomicU32::new(count));
        let state = HaltState::new();
        state.attach(Arc::clone(&replicas)).expect("first attach");
        (state, replicas)
    }

    #[test]
    fn a_swap_to_disk_with_no_replica_lifts_the_halt() {
        let (state, replicas) = attached(0);
        assert!(state.halted(Some(&replicas)));
        assert_eq!(state.on_policy_swap(true), SwapEffect::Lifted);
        assert!(state.is_lifted());
        assert!(!state.halted(Some(&replicas)));
    }

    #[test]
    fn a_swap_to_disk_with_a_replica_connected_latches_nothing() {
        let (state, replicas) = attached(1);
        assert_eq!(state.on_policy_swap(true), SwapEffect::ReplicaConnected);
        assert!(!state.is_lifted());
        replicas.store(0, Ordering::Relaxed);
        assert!(
            state.halted(Some(&replicas)),
            "the replica's departure halts"
        );
    }

    #[test]
    fn a_streaming_replica_clears_the_latch() {
        let (state, replicas) = attached(0);
        assert_eq!(state.on_policy_swap(true), SwapEffect::Lifted);
        replicas.store(1, Ordering::SeqCst);
        assert!(state.on_replica_streaming(), "the latch was set");
        assert!(!state.on_replica_streaming(), "and is now clear");
        replicas.store(0, Ordering::Relaxed);
        assert!(state.halted(Some(&replicas)), "the next departure halts");
    }

    #[test]
    fn a_swap_back_to_a_replica_backed_policy_restores_the_halt() {
        let (state, replicas) = attached(0);
        assert_eq!(state.on_policy_swap(true), SwapEffect::Lifted);
        assert_eq!(
            state.on_policy_swap(false),
            SwapEffect::Cleared { was_lifted: true }
        );
        assert!(state.halted(Some(&replicas)));
        assert_eq!(
            state.on_policy_swap(false),
            SwapEffect::Cleared { was_lifted: false }
        );
    }

    #[test]
    fn a_node_with_no_count_attached_latches_nothing() {
        let state = HaltState::new();
        assert_eq!(state.on_policy_swap(true), SwapEffect::NotServing);
        assert!(!state.is_lifted());
    }

    #[test]
    fn standalone_never_halts() {
        let state = HaltState::new();
        assert!(!state.halted(None));
    }

    #[test]
    fn a_second_attach_is_refused() {
        let (state, _) = attached(0);
        assert!(state.attach(Arc::new(AtomicU32::new(0))).is_err());
    }
}
