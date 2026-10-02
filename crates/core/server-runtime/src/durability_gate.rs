//! The response stages' durability gate: the ack policy in force, the
//! cached durable position, and the wait that holds a reply until its
//! event is durable under that policy.
//!
//! Shared by the io_uring stage ([`crate::response`]) and the DPDK stage
//! (`crate::dpdk_response`). Both used to carry their own copy of this
//! state machine — startup resolve, runtime `ACK-POLICY` swaps, the gate
//! wait, degraded-time accrual — and a fix to one copy had to be
//! remembered in the other. What differs between the two stages is
//! egress (socket buffers vs. the poll thread's TX rings); what a reply
//! waits *for* does not, so it lives here once.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::time::{Duration, Instant};

use melin_app::amortized_timer::AmortizedTimer;
use melin_pipeline::wait::WaitStrategy;
use melin_transport_core::DurableWireSeqCursor;
use melin_transport_core::pipeline::{OutputSlot, StageUtilization};
#[cfg(feature = "tick-to-trade")]
use melin_transport_core::trace;

use crate::ack_policy::{AckPolicy, Blocker, Policy};
use crate::replication::ReplicationMetrics;
use crate::response::{DegradationLogger, evaluate_durability, evaluate_gate, slot_needs_gate};
#[cfg(feature = "tick-to-trade")]
use crate::response::{GateCrossTracker, policy_replica_cursor};

/// Re-emit interval for the "still degraded" reminder.
const DEGRADED_LOG_INTERVAL: Duration = Duration::from_secs(5);

/// Cadence at which the idle path re-evaluates the policy. Bounds the lag
/// between a connection-state change and the `/healthz` gauge / warn-log
/// reflecting it. Cheap (a handful of atomic loads + the policy
/// evaluator) at this rate.
const POLICY_CHECK_INTERVAL: Duration = Duration::from_secs(1);

/// Cadence at which the gate-wait spin folds elapsed time into the
/// degraded-duration counter while the durability gate is stalled.
/// Tighter than the idle cadence — it bounds the boundary error when a
/// degradation begins or flips mid-wedge, which matters most for short
/// stalls. The accrual tick is gated by this period, but the clock read
/// behind it is gated by the `AmortizedTimer` mask, so the effective
/// resolution is `max(this, the mask's clock-read cadence)`: one clock
/// read per `2^16` spin iterations (`AmortizedTimer::CHECK_MASK`), and
/// one per iteration once the waiter has fallen back to yielding.
const GATE_ACCRUAL_INTERVAL: Duration = Duration::from_millis(10);

/// How a durability wait ended.
#[must_use]
pub(crate) enum GateOutcome {
    /// The slot's event is durable under the policy in force; its reply
    /// may be sent.
    Open,
    /// Shutdown was requested while the gate was closed. The reply the
    /// policy never confirmed must not be sent; the caller goes straight
    /// back to its shutdown branch.
    Shutdown,
}

/// The cursors and shared state the gate reads. Grouped so the two
/// stages hand them over in one place instead of threading five
/// arguments through every call.
pub(crate) struct GateInputs {
    /// Highest wire seq durably persisted on this node's journal — see
    /// `response::Response::journal_persisted_wire_seq`.
    pub journal_persisted_wire_seq: DurableWireSeqCursor,
    /// The operator-selected policy, swapped at runtime by `ACK-POLICY`.
    pub ack_policy: Arc<AtomicU8>,
    /// The replicas' acked (persisted) and in-memory cursors, written by
    /// the replication handlers. `None` on a standalone node, where only
    /// the local journal can satisfy a policy.
    pub replication_metrics: Option<Arc<ReplicationMetrics>>,
    /// Per-replica-slot "connected and streaming" flags; an inactive
    /// slot's cursors are left out of the evaluation. A fixed `[_; 2]`
    /// rather than a `Vec` because a cluster has at most two replicas
    /// beside the primary and the array is indexed in step with
    /// `ReplicationMetrics`' per-slot cursors, with no allocation on the
    /// gate path. `None` on a standalone node, like the metrics.
    pub replica_active: Option<[Arc<AtomicBool>; 2]>,
    /// The stage's health gauges: the gate writes `policy_degraded`, the
    /// degraded-time counter and the journal/replication attribution
    /// counters read by `/healthz` and the metrics endpoint.
    pub utilization: Arc<StageUtilization>,
    /// How the gate waits on the journal-disk and replication threads.
    pub wait: WaitStrategy,
}

/// The durability gate and the ack-policy state it evaluates against.
///
/// Single-threaded: owned by the response stage's thread. The policy is
/// a thread-local copy of the shared atomic, rebuilt when an admin
/// `ACK-POLICY` command changes the byte.
pub(crate) struct DurabilityGate {
    inputs: GateInputs,
    /// Which stage owns this gate, for the swap / corruption logs.
    stage: &'static str,
    active_policy: AckPolicy,
    policy: Policy,
    /// Highest wire seq known durable under `policy`, cached so a slot
    /// whose event is already durable passes without touching an atomic.
    /// `u64` because it is compared against `OutputSlot::wire_seq`.
    cached_durable_pos: u64,
    degraded_logger: DegradationLogger,
    /// Last idle-path policy evaluation; bumped by [`Self::after_batch`]
    /// so the logger is not double-ticked when traffic stops.
    last_policy_check: Instant,
    /// Paces accrual ticks inside the gate-wait spin so the degraded-
    /// duration counter keeps advancing during a hard stall. Held across
    /// gate entries (not per entry) so the normal gated path — entered
    /// briefly whenever durability lags by a few µs — pays no extra
    /// `Instant::now()`; the amortized mask only reads the clock once per
    /// ~65 k cumulative spin iterations.
    accrual_timer: AmortizedTimer,
}

impl DurabilityGate {
    /// Resolve the starting policy from the shared atomic and evaluate it
    /// once, so the cached durable position and the `/healthz` gauge
    /// reflect the cluster's startup shape before the first batch — an
    /// unsatisfiable policy (e.g. a primary that just lost both replicas
    /// while running `disk+ram`) is visible immediately.
    ///
    /// A corrupted byte at startup falls back to `DiskAndRam`, the
    /// default operators see at boot: better than panicking on a
    /// degraded process.
    pub(crate) fn new(inputs: GateInputs, stage: &'static str) -> Self {
        let active_policy = AckPolicy::from_u8(inputs.ack_policy.load(Ordering::Relaxed))
            .unwrap_or_else(|| {
                tracing::error!(
                    stage,
                    "ack_policy atomic held a corrupted byte at startup; defaulting to disk+ram"
                );
                AckPolicy::DiskAndRam
            });
        let policy = active_policy.to_policy();
        let now = Instant::now();
        let status = evaluate_durability(
            &policy,
            inputs.journal_persisted_wire_seq.load(),
            inputs.replication_metrics.as_deref(),
            inputs.replica_active.as_ref(),
        );
        inputs
            .utilization
            .policy_degraded
            .store(status.degraded, Ordering::Relaxed);
        let degraded_logger = if status.degraded {
            DegradationLogger::new_starting_degraded(now, &policy)
        } else {
            DegradationLogger::new(now)
        };
        Self {
            inputs,
            stage,
            active_policy,
            policy,
            cached_durable_pos: status.durable_pos,
            degraded_logger,
            last_policy_check: now,
            accrual_timer: AmortizedTimer::new(),
        }
    }

    /// Observe a runtime policy swap from the admin `ACK-POLICY` command.
    /// Called once per outer-loop iteration. Relaxed load (single writer
    /// is the admin handler, single reader is this thread).
    ///
    /// On a change, rebuild the local policy and reset the cached durable
    /// position so the next gate evaluation starts from a clean slate —
    /// the fresh policy may evaluate degraded/undegraded differently
    /// against the same cluster shape. The logger is re-seeded so a
    /// transition under the new policy surfaces immediately rather than
    /// waiting out the sustained-state hold; accrual is flushed first so
    /// pre-swap degraded time is not dropped. An unknown byte is treated
    /// as memory corruption: logged, and the prior policy kept rather
    /// than silently downgraded.
    #[inline]
    pub(crate) fn observe_policy_swap(&mut self) {
        let observed_byte = self.inputs.ack_policy.load(Ordering::Relaxed);
        if observed_byte == self.active_policy.as_u8() {
            return;
        }
        match AckPolicy::from_u8(observed_byte) {
            Some(next) => {
                tracing::info!(
                    stage = self.stage,
                    prev = self.active_policy.as_str(),
                    next = next.as_str(),
                    "ack policy swapped at runtime"
                );
                self.active_policy = next;
                self.policy = next.to_policy();
                self.cached_durable_pos = 0;
                self.degraded_logger
                    .reseed(&self.inputs.utilization, Instant::now());
            }
            None => {
                tracing::error!(
                    stage = self.stage,
                    byte = observed_byte,
                    "ack_policy atomic held a corrupted byte; retaining prior policy"
                );
            }
        }
    }

    /// Whether `slot` must wait before its reply is sent: its own event is
    /// not yet known durable. See [`slot_needs_gate`].
    #[inline]
    pub(crate) fn needs_wait<R: Copy, Q: Copy>(&self, slot: &OutputSlot<R, Q>) -> bool {
        slot_needs_gate(slot, self.cached_durable_pos)
    }

    /// Wait until wire seq `needed` is durable under the policy in force,
    /// or shutdown is requested.
    ///
    /// The gate waits on the journal-disk thread and the replication
    /// handlers — the exact threads a small box co-schedules with the
    /// response stage, so it waits the way every other wait does. A fresh
    /// waiter per gate entry: the spin budget is meant to cover one
    /// durability lag, not to carry over from the idle loop.
    ///
    /// A policy swap is observed inside the wait too. Without it, a slot
    /// whose gate became structurally unsatisfiable (every replica gone
    /// under `disk+ram`) would wedge the stage forever, even after an
    /// operator sent the remediating `ACK-POLICY disk` — the outer loop's
    /// observation would never run. Unlike the outer observation it keeps
    /// the cached position (the evaluation below overwrites it anyway)
    /// and ignores a corrupted byte silently, retrying it next spin.
    ///
    /// Shutdown is observed for the same reason: a gate that cannot open
    /// would otherwise hold this thread until a replica returned, and the
    /// shutdown sequence joins it without a timeout — an operator
    /// restarting the degraded node, or a fence (which co-sets
    /// `shutdown`), would hang the process.
    #[inline]
    pub(crate) fn wait_durable(
        &mut self,
        needed: u64,
        shutdown: &AtomicBool,
        #[cfg(feature = "tick-to-trade")] tracker: &mut GateCrossTracker,
    ) -> GateOutcome {
        let mut gate_waiter = self.inputs.wait.waiter();
        loop {
            // ~1 cycle on x86; cheaper than the wait below.
            let observed_byte = self.inputs.ack_policy.load(Ordering::Relaxed);
            if observed_byte != self.active_policy.as_u8()
                && let Some(next) = AckPolicy::from_u8(observed_byte)
            {
                tracing::info!(
                    stage = self.stage,
                    prev = self.active_policy.as_str(),
                    next = next.as_str(),
                    "ack policy swapped during gate wait"
                );
                self.active_policy = next;
                self.policy = next.to_policy();
                // Flush accrual before re-seeding so the wedged-degraded
                // interval up to the swap isn't dropped.
                self.degraded_logger
                    .reseed(&self.inputs.utilization, Instant::now());
            }

            if shutdown.load(Ordering::Relaxed) {
                return GateOutcome::Shutdown;
            }

            let journal_pos = self.inputs.journal_persisted_wire_seq.load();
            let metrics_ref = self.inputs.replication_metrics.as_deref();
            let active_ref = self.inputs.replica_active.as_ref();

            // The cross-tracker (traced builds only) samples the replica
            // cursor itself rather than sharing the evaluation's read
            // below: computing a standalone replica cursor
            // unconditionally spent four Acquire loads per spin iteration
            // on `ReplicationMetrics`, the same cache line the
            // replication sender writes on every ack and every completed
            // SEND. Gate attribution does not re-read at all — it comes
            // out of `evaluate_gate`, from the same snapshot that opens
            // the gate.
            #[cfg(feature = "tick-to-trade")]
            tracker.observe(
                journal_pos.get(),
                // The level the *active policy* gates replicas on —
                // in-memory under `disk+ram`, persisted under
                // `two-disks`. `None` when no clause is replica-supplied
                // (`disk`) and, transiently, when the binding replica
                // drops out of the cursor view mid-wait. Passed through
                // as-is so the tracker can tell "no replica wait to
                // measure" from "the replica caught up".
                policy_replica_cursor(&self.policy, journal_pos, metrics_ref, active_ref),
                trace::mono_trace_ns(),
            );

            let (status, blocker) =
                evaluate_gate(&self.policy, needed, journal_pos, metrics_ref, active_ref);
            self.cached_durable_pos = status.durable_pos;
            self.inputs
                .utilization
                .policy_degraded
                .store(status.degraded, Ordering::Relaxed);

            // Accrue degraded time while wedged. The post-batch tick
            // attributes the whole wait to a single state, so without
            // this a healthy→degraded flip during the wedge would be
            // mis-charged. While the gate waiter spins the clock read
            // behind the tick is mask-gated, landing only every ~65 k
            // iterations (`CHECK_MASK = 2^16`) regardless of the period;
            // once it yields, each iteration already pays a syscall and
            // the read is unmasked.
            if self
                .accrual_timer
                .tick(GATE_ACCRUAL_INTERVAL, gate_waiter.spinning())
                .is_some()
            {
                self.degraded_logger.tick(
                    &self.policy,
                    &self.inputs.utilization,
                    status.degraded,
                    Instant::now(),
                    DEGRADED_LOG_INTERVAL,
                );
            }

            if self.cached_durable_pos >= needed {
                // Attribution: which subsystem supplied the binding
                // cursor, from the same snapshot that opened the gate and
                // against the policy actually in force. Relaxed is fine —
                // health reads are infrequent.
                //
                // `None` is unreachable here: `needed >= 1` inside this
                // loop, and a degraded evaluation pins `durable_pos` to
                // 0, so an open gate implies the policy was satisfiable
                // and attribution has a verdict. The no-op arm keeps a
                // metrics-only path from ever panicking regardless.
                match blocker {
                    Some(Blocker::Journal) => {
                        self.inputs
                            .utilization
                            .gate_journal
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    Some(Blocker::Replication) => {
                        self.inputs
                            .utilization
                            .gate_replication
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    None => {}
                }
                return GateOutcome::Open;
            }
            gate_waiter.idle();
        }
    }

    /// Re-evaluate the policy on a slow timer while no batches flow, so
    /// the `policy_degraded` flag and the periodic warn track the
    /// cluster's real state even on a quiet node, and the next batch's
    /// gate starts from a fresh cached position rather than spinning from
    /// a stale one. A no-op until [`POLICY_CHECK_INTERVAL`] has passed
    /// since the last check (or the last batch).
    #[inline]
    pub(crate) fn idle_recheck(&mut self, now: Instant) {
        if now.duration_since(self.last_policy_check) < POLICY_CHECK_INTERVAL {
            return;
        }
        self.last_policy_check = now;
        let status = evaluate_durability(
            &self.policy,
            self.inputs.journal_persisted_wire_seq.load(),
            self.inputs.replication_metrics.as_deref(),
            self.inputs.replica_active.as_ref(),
        );
        self.degraded_logger.tick(
            &self.policy,
            &self.inputs.utilization,
            status.degraded,
            now,
            DEGRADED_LOG_INTERVAL,
        );
        self.cached_durable_pos = status.durable_pos;
    }

    /// Log degradation transitions / re-emit the reminder after a batch.
    /// Transitions are gated on a sustained-state hold so sub-second flap
    /// doesn't spam.
    ///
    /// Off a fresh clock read: with the gate evaluated per slot a batch
    /// can span several waits, and a timestamp taken at the start of the
    /// batch predates all of them. The accrual inside each wait already
    /// charges degraded time as it elapses; this tick decides
    /// transitions, so it wants the state and the timestamp as of the end
    /// of the batch. Bumps the idle path's check timestamp so the logger
    /// is not double-ticked when traffic stops.
    #[inline]
    pub(crate) fn after_batch(&mut self) {
        let ticked_at = Instant::now();
        let degraded_now = self
            .inputs
            .utilization
            .policy_degraded
            .load(Ordering::Relaxed);
        self.degraded_logger.tick(
            &self.policy,
            &self.inputs.utilization,
            degraded_now,
            ticked_at,
            DEGRADED_LOG_INTERVAL,
        );
        self.last_policy_check = ticked_at;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;

    use melin_transport_core::WireSeq;

    use super::*;

    /// A byte no `AckPolicy` maps to, standing in for a corrupted atomic.
    const CORRUPT: u8 = 0xEE;

    /// A standalone node's gate (no replicas), so `disk` is satisfied by
    /// the local journal alone and `disk+ram` is structurally
    /// unsatisfiable — degraded from the start.
    fn gate(policy: AckPolicy, journal_pos: u64) -> DurabilityGate {
        let inputs = GateInputs {
            journal_persisted_wire_seq: DurableWireSeqCursor::detached(WireSeq::new(journal_pos)),
            ack_policy: Arc::new(AtomicU8::new(policy.as_u8())),
            replication_metrics: None,
            replica_active: None,
            utilization: Arc::new(StageUtilization::new()),
            wait: WaitStrategy::SpinThenYield,
        };
        DurabilityGate::new(inputs, "test")
    }

    /// A primary's gate with both replicas connected and caught up to the
    /// journal, so every policy opens at `pos`.
    fn gate_with_replicas(policy: AckPolicy, pos: u64) -> DurabilityGate {
        let metrics = ReplicationMetrics::default();
        for slot in 0..2 {
            metrics.acked_sequence[slot].store(pos, Ordering::Relaxed);
            metrics.in_memory_sequence[slot].store(pos, Ordering::Relaxed);
        }
        let inputs = GateInputs {
            journal_persisted_wire_seq: DurableWireSeqCursor::detached(WireSeq::new(pos)),
            ack_policy: Arc::new(AtomicU8::new(policy.as_u8())),
            replication_metrics: Some(Arc::new(metrics)),
            replica_active: Some([
                Arc::new(AtomicBool::new(true)),
                Arc::new(AtomicBool::new(true)),
            ]),
            utilization: Arc::new(StageUtilization::new()),
            wait: WaitStrategy::SpinThenYield,
        };
        DurabilityGate::new(inputs, "test")
    }

    fn wait(gate: &mut DurabilityGate, needed: u64, shutdown: &AtomicBool) -> GateOutcome {
        #[cfg(feature = "tick-to-trade")]
        let mut tracker = GateCrossTracker::new(needed);
        gate.wait_durable(
            needed,
            shutdown,
            #[cfg(feature = "tick-to-trade")]
            &mut tracker,
        )
    }

    fn degraded_nanos(gate: &DurabilityGate) -> u64 {
        gate.inputs
            .utilization
            .policy_degraded_nanos
            .load(Ordering::Relaxed)
    }

    fn attributions(gate: &DurabilityGate) -> (u64, u64) {
        let u = &gate.inputs.utilization;
        (
            u.gate_journal.load(Ordering::Relaxed),
            u.gate_replication.load(Ordering::Relaxed),
        )
    }

    /// Let measurable time pass so a degraded-time flush is non-zero.
    fn let_time_pass() {
        std::thread::sleep(Duration::from_millis(2));
    }

    #[test]
    fn startup_resolves_the_policy_and_its_durable_position() {
        let g = gate(AckPolicy::Disk, 5);
        assert_eq!(g.active_policy, AckPolicy::Disk);
        assert_eq!(g.cached_durable_pos, 5);
        assert!(!g.inputs.utilization.policy_degraded.load(Ordering::Relaxed));

        let g = gate(AckPolicy::DiskAndRam, 5);
        assert_eq!(g.cached_durable_pos, 0, "an unsatisfiable policy pins 0");
        assert!(g.inputs.utilization.policy_degraded.load(Ordering::Relaxed));
    }

    /// A swap rebuilds the policy, drops the cached position, and
    /// re-seeds the logger — flushing the degraded time accrued under
    /// the old policy, which is how the re-seed shows from outside.
    #[test]
    fn a_policy_swap_resets_the_cached_position_and_reseeds_the_logger() {
        let mut g = gate(AckPolicy::Disk, 5);
        g.inputs
            .ack_policy
            .store(AckPolicy::TwoDisks.as_u8(), Ordering::Relaxed);
        g.observe_policy_swap();
        assert_eq!(g.active_policy, AckPolicy::TwoDisks);
        assert_eq!(g.policy, AckPolicy::TwoDisks.to_policy());
        assert_eq!(g.cached_durable_pos, 0);

        let mut g = gate(AckPolicy::DiskAndRam, 5);
        let_time_pass();
        g.inputs
            .ack_policy
            .store(AckPolicy::Disk.as_u8(), Ordering::Relaxed);
        g.observe_policy_swap();
        assert_eq!(g.active_policy, AckPolicy::Disk);
        assert!(
            degraded_nanos(&g) > 0,
            "the re-seed flushes the degraded interval before the swap"
        );
    }

    #[test]
    fn a_corrupted_byte_keeps_the_prior_policy() {
        let mut g = gate(AckPolicy::DiskAndRam, 5);
        let_time_pass();
        g.inputs.ack_policy.store(CORRUPT, Ordering::Relaxed);
        g.observe_policy_swap();
        assert_eq!(g.active_policy, AckPolicy::DiskAndRam);
        assert_eq!(g.policy, AckPolicy::DiskAndRam.to_policy());
        assert_eq!(degraded_nanos(&g), 0, "no re-seed on a corrupted byte");

        let mut g = gate(AckPolicy::Disk, 5);
        g.inputs.ack_policy.store(CORRUPT, Ordering::Relaxed);
        g.observe_policy_swap();
        assert_eq!(g.active_policy, AckPolicy::Disk);
        assert_eq!(g.cached_durable_pos, 5, "cached position kept");
    }

    #[test]
    fn an_open_gate_attributes_exactly_once() {
        let mut g = gate(AckPolicy::Disk, 5);
        let shutdown = AtomicBool::new(false);
        assert!(matches!(wait(&mut g, 3, &shutdown), GateOutcome::Open));
        assert_eq!(attributions(&g), (1, 0), "disk opens on the journal");
        assert_eq!(g.cached_durable_pos, 5);
    }

    /// A wedged gate (every replica gone under `disk+ram`) still honours
    /// shutdown, and the reply it never confirmed is not attributed.
    /// Run on a helper thread so a regression fails the test instead of
    /// hanging it.
    #[test]
    fn shutdown_releases_a_wedged_gate_without_attribution() {
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let mut g = gate(AckPolicy::DiskAndRam, 5);
            let shutdown = AtomicBool::new(true);
            let outcome = wait(&mut g, 3, &shutdown);
            // The receiver only disappears once the test has failed on
            // its timeout; nothing is left to report to.
            let _ = tx.send((matches!(outcome, GateOutcome::Shutdown), attributions(&g)));
        });
        let (was_shutdown, attributed) = rx
            .recv_timeout(Duration::from_secs(10))
            .expect("the wedged gate ignored shutdown");
        assert!(was_shutdown);
        assert_eq!(attributed, (0, 0));
    }

    /// Shutdown is checked before the gate is evaluated: a gate that
    /// would open on this pass still reports `Shutdown`, and attributes
    /// nothing, so a reply is never released during teardown.
    #[test]
    fn shutdown_wins_over_a_gate_ready_to_open() {
        let mut g = gate_with_replicas(AckPolicy::Disk, 5);
        let shutdown = AtomicBool::new(true);
        assert!(matches!(wait(&mut g, 3, &shutdown), GateOutcome::Shutdown));
        assert_eq!(attributions(&g), (0, 0));
    }

    /// The remediating `ACK-POLICY` reaches a gate already wedged: the
    /// wait swaps to the new policy, flushes the degraded time, and opens.
    #[test]
    fn a_swap_during_the_wait_unwedges_the_gate() {
        let mut g = gate(AckPolicy::DiskAndRam, 5);
        let_time_pass();
        g.inputs
            .ack_policy
            .store(AckPolicy::Disk.as_u8(), Ordering::Relaxed);
        let shutdown = AtomicBool::new(false);
        assert!(matches!(wait(&mut g, 3, &shutdown), GateOutcome::Open));
        assert_eq!(g.active_policy, AckPolicy::Disk);
        // Not a mutation guard for "the mid-wait swap keeps the cached
        // position": the evaluation on the same pass overwrites it, so a
        // reset there is unobservable from outside the wait.
        assert_eq!(g.cached_durable_pos, 5);
        assert_eq!(attributions(&g), (1, 0));
        assert!(
            degraded_nanos(&g) > 0,
            "the re-seed flushes the wedged interval"
        );
    }

    /// Replicas are caught up so any policy would open the gate: what is
    /// pinned is that the corrupted byte changes none of it.
    #[test]
    fn a_corrupted_byte_during_the_wait_is_ignored() {
        let mut g = gate_with_replicas(AckPolicy::Disk, 5);
        g.inputs.ack_policy.store(CORRUPT, Ordering::Relaxed);
        let shutdown = AtomicBool::new(false);
        assert!(matches!(wait(&mut g, 3, &shutdown), GateOutcome::Open));
        assert_eq!(g.active_policy, AckPolicy::Disk);
        assert_eq!(g.policy, AckPolicy::Disk.to_policy());
    }

    #[test]
    fn idle_recheck_waits_out_the_check_interval() {
        let mut g = gate(AckPolicy::Disk, 5);
        let checked = g.last_policy_check;
        g.inputs.journal_persisted_wire_seq.store(WireSeq::new(9));

        g.idle_recheck(checked + POLICY_CHECK_INTERVAL - Duration::from_millis(1));
        assert_eq!(g.cached_durable_pos, 5, "too early: no re-evaluation");
        assert_eq!(g.last_policy_check, checked);

        let due = checked + POLICY_CHECK_INTERVAL;
        g.idle_recheck(due);
        assert_eq!(g.cached_durable_pos, 9);
        assert_eq!(g.last_policy_check, due);
    }

    #[test]
    fn a_batch_defers_the_next_idle_recheck() {
        let mut g = gate(AckPolicy::Disk, 5);
        // An idle check long overdue, as after a quiet spell.
        g.last_policy_check = Instant::now()
            .checked_sub(2 * POLICY_CHECK_INTERVAL)
            .expect("the monotonic clock is past two check intervals");
        g.inputs.journal_persisted_wire_seq.store(WireSeq::new(9));

        g.after_batch();
        g.idle_recheck(Instant::now());
        assert_eq!(
            g.cached_durable_pos, 5,
            "the batch counted as the check, so the idle path waits"
        );
    }
}
