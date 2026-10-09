//! The replica-loss halt's shared state: the operator's override, and
//! when the node last lost its last replica.
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
//!
//! # When the replicas went
//!
//! The response stage releases a halted node's held replies on the
//! primary's own fsync once the halt has lasted a grace period, which
//! starts when the replica count drops to zero. The replication senders
//! stamp that moment here ([`HaltState::on_replica_leaving`](crate::halt_state::HaltState::on_replica_leaving)), so the
//! grace is measured from the loss itself rather than from whenever the
//! response stage next looks.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

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

/// The halt's shared state: the override latch, the replica count it is
/// judged against, and when that count last dropped to zero. One per
/// process, shared by the admin handler (sets and clears the latch), the
/// replication senders (clear the latch, stamp a loss), the readers and
/// the health endpoint (read the latch) and the response stage (reads the
/// stamp).
///
/// Atomics rather than anything heavier because the readers fold the
/// latch into their per-receive halt check, and only when the replica
/// count is already zero: one relaxed load on a path that is refusing
/// writes anyway.
#[derive(Debug)]
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
    /// When the replica count last dropped to zero, in nanoseconds since
    /// `origin`, plus one; `0` when unknown (cleared by a replica joining).
    /// `u64` nanoseconds rather than an `Instant`, which no atomic holds:
    /// a `u64` of nanoseconds covers centuries of uptime.
    last_lost: AtomicU64,
    /// The zero of `last_lost`.
    origin: Instant,
}

impl Default for HaltState {
    fn default() -> Self {
        Self {
            lifted: AtomicBool::new(false),
            replicas_connected: OnceLock::new(),
            last_lost: AtomicU64::new(0),
            origin: Instant::now(),
        }
    }
}

impl HaltState {
    /// An unlatched state, attached to no replica count yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Attach the replica count of the pipeline this node now serves.
    /// Called once, when the node starts serving as a replicated primary,
    /// with no replica connected yet: the node starts out halted, and the
    /// grace period runs from here. `Err` if a count is already attached:
    /// a node serves as a primary once per process, so a second call is a
    /// bug in the caller.
    pub fn attach(&self, replicas_connected: Arc<AtomicU32>) -> Result<(), &'static str> {
        self.replicas_connected
            .set(replicas_connected)
            .map_err(|_| "a replica count is already attached to the halt state")?;
        self.stamp_loss();
        Ok(())
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

    /// A replica has joined the count: forget the last loss. Called by the
    /// replication senders right after they increment the count, so a
    /// later loss is never measured from an earlier one's stamp.
    pub fn on_replica_joined(&self) {
        self.last_lost.store(0, Ordering::Relaxed);
    }

    /// A replica is about to leave the count: stamp the moment, in case
    /// it is the last. Called by the replication senders right *before*
    /// they decrement the count (with `Release`), so a reader that sees
    /// the count at zero (with `Acquire`) sees this stamp or a later one,
    /// never an earlier loss's. A stamp left by a replica that was not
    /// the last is harmless: nothing reads it while the count is above
    /// zero, and the next join clears it.
    pub fn on_replica_leaving(&self) {
        self.stamp_loss();
    }

    fn stamp_loss(&self) {
        // Saturated rather than an error: `u64` nanoseconds overflow only
        // after centuries of uptime, and the stamp then still reads as a
        // halt that began long ago.
        let since_origin = u64::try_from(self.origin.elapsed().as_nanos()).unwrap_or(u64::MAX - 1);
        self.last_lost.store(since_origin + 1, Ordering::Relaxed);
    }

    /// Whether this node serves as a replicated primary with no replica
    /// connected. One `Acquire` load once attached: a reader that sees
    /// the count at zero sees the leaving replica's stamp too (see
    /// [`on_replica_leaving`](Self::on_replica_leaving)).
    #[inline]
    pub fn no_replica(&self) -> bool {
        self.replicas_connected
            .get()
            .is_some_and(|count| count.load(Ordering::Acquire) == 0)
    }

    /// How long this replicated primary has had no replica connected, or
    /// `None` while it has one (or is not a replicated primary). The
    /// halt as the count alone defines it: the operator's override, which
    /// only ever stands beside the `disk` policy, does not change it.
    ///
    /// When the moment of the loss is unknown (a join cleared the stamp
    /// after the leaving replica wrote it), the halt is reported as having
    /// just begun, which errs towards a longer wait. A caller that polls
    /// keeps the earliest start it has seen, so that the halt's age still
    /// grows from its first observation.
    #[inline]
    pub fn halted_for(&self, now: Instant) -> Option<Duration> {
        if !self.no_replica() {
            return None;
        }
        let stamp = self.last_lost.load(Ordering::Relaxed);
        if stamp == 0 {
            return Some(Duration::ZERO);
        }
        let lost_at = self.origin + Duration::from_nanos(stamp - 1);
        Some(now.saturating_duration_since(lost_at))
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
        assert_eq!(state.halted_for(Instant::now()), None);
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

    /// A primary that starts with no replica is halted from the attach.
    #[test]
    fn the_halt_is_timed_from_the_attach_on_a_node_that_starts_alone() {
        let before = Instant::now();
        let (state, _) = attached(0);
        let later = before + Duration::from_secs(5);
        let halted_for = state.halted_for(later).expect("no replica");
        assert!(halted_for <= Duration::from_secs(5), "{halted_for:?}");
        assert!(
            halted_for >= Duration::from_secs(4),
            "timed from the attach, not from later: {halted_for:?}"
        );
    }

    #[test]
    fn the_halt_is_timed_from_the_last_replica_leaving() {
        let (state, replicas) = attached(0);
        replicas.fetch_add(1, Ordering::SeqCst);
        state.on_replica_joined();
        assert_eq!(state.halted_for(Instant::now()), None, "a replica is up");

        std::thread::sleep(Duration::from_millis(20));
        state.on_replica_leaving();
        let left = Instant::now();
        replicas.fetch_sub(1, Ordering::Release);
        let halted_for = state
            .halted_for(left + Duration::from_secs(3))
            .expect("no replica");
        assert!(
            halted_for >= Duration::from_secs(3) && halted_for < Duration::from_secs(4),
            "{halted_for:?}"
        );
    }

    /// A join clears the stamp, so a loss whose stamp has not landed yet
    /// reads as a halt that just began, never as an earlier loss's.
    #[test]
    fn an_unknown_loss_reads_as_a_halt_that_just_began() {
        let (state, replicas) = attached(0);
        replicas.fetch_add(1, Ordering::SeqCst);
        state.on_replica_joined();
        replicas.fetch_sub(1, Ordering::Release);
        assert_eq!(
            state.halted_for(Instant::now() + Duration::from_secs(60)),
            Some(Duration::ZERO)
        );
    }
}
