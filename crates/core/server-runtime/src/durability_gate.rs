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
//!
//! # Policy changes
//!
//! The policy is part of the lineage: an `AckPolicyChange` entry is
//! journaled in order with the events it governs, and reaches the gate as
//! an output slot at the entry's position. One rule covers every change,
//! with no ranking of policies: a change at wire sequence `w` takes effect
//! for every slot after it once `w` itself is durable under the policy in
//! force before it. Changes are applied one by one, never merged — two
//! pending changes each wait for the policy before them.
//!
//! The one exception is a forced (unwitnessed) change: a gate halted
//! under a policy no connected node can satisfy is stuck on a slot ahead
//! of the change, so the in-order switch is never reached. The matching
//! stage publishes such a change as it dispatches it, and the wait
//! applies it as soon as its own entry is durable on this node's disk,
//! then ignores every in-order change at or below it.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::time::{Duration, Instant};

use melin_app::amortized_timer::AmortizedTimer;
use melin_journal::AckPolicyByte;
use melin_pipeline::wait::WaitStrategy;
use melin_transport_core::DurableWireSeqCursor;
use melin_transport_core::lineage::ForcedPolicyReader;
use melin_transport_core::pipeline::{OutputPayload, OutputSlot, StageUtilization};
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

/// Where the gate's policy comes from: the one it starts under, and the
/// two ways it changes while running.
pub struct GatePolicy {
    /// The policy in force when the gate starts — the lineage's, or the
    /// requested one while the lineage holds no change (a primary resolves
    /// it with the crate's `initial_gate_policy`).
    pub initial: AckPolicy,
    /// The admin `ACK-POLICY` command's atomic, observed for changes.
    /// Transitional: the gate follows a change of the byte, not the byte
    /// itself, so a gate started from the lineage keeps that policy until
    /// an operator sends one. Retired once the command journals its
    /// change instead.
    pub admin: Arc<AtomicU8>,
    /// The admin byte `initial` was resolved against. The gate applies
    /// any later change of the atomic, including one made between the
    /// resolution and the gate's start, which the atomic's value at
    /// startup would hide.
    pub admin_seen: u8,
    /// Forced changes the matching stage has dispatched — see
    /// [`ForcedPolicyReader`] and the module docs.
    pub forced: ForcedPolicyReader,
}

/// The policy a primary's gate starts under: the one the lineage has in
/// force, or `requested` (the configured or staged one) while the lineage
/// holds no change.
///
/// A lineage byte this build does not know is refused rather than
/// replaced: the events it governs were acknowledged under a policy this
/// node cannot evaluate, so it must neither start nor promote on it.
pub(crate) fn initial_gate_policy(
    lineage: Option<AckPolicyByte>,
    requested: AckPolicy,
) -> Result<AckPolicy, String> {
    match lineage {
        None => Ok(requested),
        Some(byte) => AckPolicy::from_byte(byte).ok_or_else(|| {
            format!(
                "the journal's ack policy (byte {}) is unknown to this build; \
                 refusing to serve under it",
                byte.0
            )
        }),
    }
}

/// The cursors and shared state the gate reads. Grouped so the two
/// stages hand them over in one place instead of threading five
/// arguments through every call.
pub(crate) struct GateInputs {
    /// Highest wire seq durably persisted on this node's journal — see
    /// `response::Response::journal_persisted_wire_seq`.
    pub journal_persisted_wire_seq: DurableWireSeqCursor,
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
/// Single-threaded: owned by the response stage's thread. Every change of
/// policy goes through [`Self::switch_to`].
pub(crate) struct DurabilityGate {
    inputs: GateInputs,
    /// Which stage owns this gate, for the swap / corruption logs.
    stage: &'static str,
    /// See [`GatePolicy::admin`].
    admin: Arc<AtomicU8>,
    /// The admin byte last observed: the gate reacts to a change of it.
    admin_seen: u8,
    /// See [`GatePolicy::forced`].
    forced: ForcedPolicyReader,
    /// Wire seq of the newest journaled change the gate has applied, in
    /// order or forced; 0 before any (no entry has wire seq 0). A change
    /// at or below it is stale: a forced change already superseded it.
    /// `u64` because it is compared against `OutputSlot::wire_seq`.
    applied_through: u64,
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
    /// Start under `policy.initial` and evaluate it once, so the cached
    /// durable position and the `/healthz` gauge reflect the cluster's
    /// startup shape before the first batch — an unsatisfiable policy
    /// (e.g. a primary that just lost both replicas while running
    /// `disk+ram`) is visible immediately.
    pub(crate) fn new(inputs: GateInputs, policy: GatePolicy, stage: &'static str) -> Self {
        let GatePolicy {
            initial: active_policy,
            admin,
            admin_seen,
            forced,
        } = policy;
        inputs
            .utilization
            .ack_policy_in_force
            .store(Some(active_policy.to_byte()));
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
            admin,
            admin_seen,
            forced,
            applied_through: 0,
            active_policy,
            policy,
            cached_durable_pos: status.durable_pos,
            degraded_logger,
            last_policy_check: now,
            accrual_timer: AmortizedTimer::new(),
        }
    }

    /// The one place the policy in force changes.
    ///
    /// Rebuilds the local policy and resets the cached durable position so
    /// the next gate evaluation starts from a clean slate — the fresh
    /// policy may evaluate degraded/undegraded differently against the
    /// same cluster shape. The logger is re-seeded so a transition under
    /// the new policy surfaces immediately rather than waiting out the
    /// sustained-state hold; accrual is flushed first so degraded time
    /// before the switch is not dropped.
    fn switch_to(&mut self, next: AckPolicy, cause: &'static str) {
        tracing::info!(
            stage = self.stage,
            prev = self.active_policy.as_str(),
            next = next.as_str(),
            cause,
            "ack policy switched"
        );
        self.active_policy = next;
        self.policy = next.to_policy();
        self.cached_durable_pos = 0;
        self.degraded_logger
            .reseed(&self.inputs.utilization, Instant::now());
        self.inputs
            .utilization
            .ack_policy_in_force
            .store(Some(next.to_byte()));
    }

    /// Observe a runtime policy swap from the admin `ACK-POLICY` command
    /// (transitional, see [`GatePolicy::admin`]). Called once per
    /// outer-loop iteration and on every spin of the wait. Relaxed load
    /// (single writer is the admin handler, single reader is this
    /// thread).
    ///
    /// Reacts to a change of the byte, once. An unknown byte is treated as
    /// memory corruption: logged, and the prior policy kept rather than
    /// silently downgraded.
    #[inline]
    pub(crate) fn observe_policy_swap(&mut self) {
        let observed_byte = self.admin.load(Ordering::Relaxed);
        if observed_byte == self.admin_seen {
            return;
        }
        self.admin_seen = observed_byte;
        match AckPolicy::from_u8(observed_byte) {
            Some(next) => self.switch_to(next, "admin swap"),
            None => {
                tracing::error!(
                    stage = self.stage,
                    byte = observed_byte,
                    "ack_policy atomic held a corrupted byte; retaining prior policy"
                );
            }
        }
    }

    /// Apply the journaled change at `wire_seq`, unless a change at or
    /// after it has been applied already (a forced one overtook it).
    ///
    /// A byte this build does not know keeps the prior policy, loudly:
    /// this node journaled the entry itself, from a policy it named, so
    /// it is a bug, and refusing every later reply would turn it into an
    /// outage.
    fn apply_change(&mut self, wire_seq: u64, policy: AckPolicyByte, cause: &'static str) {
        if wire_seq <= self.applied_through {
            return;
        }
        self.applied_through = wire_seq;
        match AckPolicy::from_byte(policy) {
            Some(next) => self.switch_to(next, cause),
            None => {
                tracing::error!(
                    stage = self.stage,
                    byte = policy.0,
                    wire_seq,
                    "journaled ack-policy change names an unknown policy; retaining prior policy"
                );
            }
        }
    }

    /// Apply the newest forced change once its own entry is durable on
    /// this node's disk (`journal_pos`), so the record of the swap exists
    /// before any reply relies on it.
    #[inline]
    fn observe_forced(&mut self, journal_pos: u64) {
        if let Some(forced) = self.forced.load()
            && forced.wire_seq > self.applied_through
            && journal_pos >= forced.wire_seq
        {
            self.apply_change(forced.wire_seq, forced.policy, "forced change");
        }
    }

    /// Admit one output slot: hold it until its event is durable under
    /// the policy in force, then, for an ack-policy change, switch to the
    /// policy it names for every slot after it. Both response stages call
    /// this for every slot, in ring order.
    ///
    /// `before_wait` runs once, only when the slot has to wait — the
    /// kernel stage flushes its buffered replies there.
    ///
    /// A change a forced one has overtaken is stale: it is neither waited
    /// on (it carries no reply) nor applied.
    #[inline]
    pub(crate) fn admit<R: Copy, Q: Copy>(
        &mut self,
        slot: &OutputSlot<R, Q>,
        shutdown: &AtomicBool,
        #[cfg(feature = "tick-to-trade")] tracker: &mut GateCrossTracker,
        before_wait: impl FnOnce(),
    ) -> GateOutcome {
        let change = match slot.payload {
            OutputPayload::AckPolicyChange(change) => {
                if slot.wire_seq <= self.applied_through {
                    return GateOutcome::Open;
                }
                Some(change)
            }
            OutputPayload::Report(_)
            | OutputPayload::QueryResponse(_)
            | OutputPayload::BatchEnd
            | OutputPayload::EngineError => None,
        };
        if slot_needs_gate(slot, self.cached_durable_pos) {
            before_wait();
            if let GateOutcome::Shutdown = self.wait_durable(
                slot.wire_seq,
                shutdown,
                #[cfg(feature = "tick-to-trade")]
                tracker,
            ) {
                return GateOutcome::Shutdown;
            }
        }
        if let Some(change) = change {
            let cause = if change.unwitnessed {
                "unwitnessed change, in order"
            } else {
                "journaled change"
            };
            self.apply_change(slot.wire_seq, change.policy, cause);
        }
        GateOutcome::Open
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
    /// A forced change (and, transitionally, an admin swap) is observed
    /// inside the wait too. Without it, a slot whose gate became
    /// structurally unsatisfiable (every replica gone under `disk+ram`)
    /// would wedge the stage forever, even after an operator sent the
    /// remediating `ACK-POLICY disk` — its slot sits behind the stuck one.
    ///
    /// Shutdown is observed for the same reason: a gate that cannot open
    /// would otherwise hold this thread until a replica returned, and the
    /// shutdown sequence joins it without a timeout — an operator
    /// restarting the degraded node, or a fence (which co-sets
    /// `shutdown`), would hang the process.
    #[inline]
    fn wait_durable(
        &mut self,
        needed: u64,
        shutdown: &AtomicBool,
        #[cfg(feature = "tick-to-trade")] tracker: &mut GateCrossTracker,
    ) -> GateOutcome {
        let mut gate_waiter = self.inputs.wait.waiter();
        loop {
            // ~1 cycle on x86; cheaper than the wait below.
            self.observe_policy_swap();

            if shutdown.load(Ordering::Relaxed) {
                return GateOutcome::Shutdown;
            }

            let journal_pos = self.inputs.journal_persisted_wire_seq.load();
            self.observe_forced(journal_pos.get());
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
    use melin_transport_core::lineage::{ForcedPolicy, ForcedPolicyWriter, forced_policy_cell};

    use super::*;

    /// A byte no `AckPolicy` maps to, standing in for a corrupted atomic.
    const CORRUPT: u8 = 0xEE;

    /// A gate starting under `initial`, its admin atomic holding the same
    /// policy, and the writer of its forced cell.
    fn build(
        initial: AckPolicy,
        journal_pos: u64,
        replication_metrics: Option<Arc<ReplicationMetrics>>,
        replica_active: Option<[Arc<AtomicBool>; 2]>,
    ) -> (DurabilityGate, ForcedPolicyWriter) {
        let (forced_writer, forced) = forced_policy_cell();
        let inputs = GateInputs {
            journal_persisted_wire_seq: DurableWireSeqCursor::detached(WireSeq::new(journal_pos)),
            replication_metrics,
            replica_active,
            utilization: Arc::new(StageUtilization::new()),
            wait: WaitStrategy::SpinThenYield,
        };
        let policy = GatePolicy {
            initial,
            admin: Arc::new(AtomicU8::new(initial.as_u8())),
            admin_seen: initial.as_u8(),
            forced,
        };
        (DurabilityGate::new(inputs, policy, "test"), forced_writer)
    }

    /// A standalone node's gate (no replicas), so `disk` is satisfied by
    /// the local journal alone and `disk+ram` is structurally
    /// unsatisfiable — degraded from the start.
    fn gate(policy: AckPolicy, journal_pos: u64) -> DurabilityGate {
        build(policy, journal_pos, None, None).0
    }

    /// A primary's gate with both replicas connected and caught up to the
    /// journal, so every policy opens at `pos`.
    fn gate_with_replicas(policy: AckPolicy, pos: u64) -> DurabilityGate {
        let metrics = ReplicationMetrics::default();
        for slot in 0..2 {
            metrics.acked_sequence[slot].store(pos, Ordering::Relaxed);
            metrics.in_memory_sequence[slot].store(pos, Ordering::Relaxed);
        }
        build(
            policy,
            pos,
            Some(Arc::new(metrics)),
            Some([
                Arc::new(AtomicBool::new(true)),
                Arc::new(AtomicBool::new(true)),
            ]),
        )
        .0
    }

    /// A primary's gate with one replica connected, holding nothing yet
    /// (both its cursors at 0): `disk` opens on the journal alone, and
    /// `disk+ram` waits for the replica's in-memory cursor, which the
    /// test moves through the returned metrics.
    fn gate_with_one_replica(
        initial: AckPolicy,
        journal_pos: u64,
    ) -> (DurabilityGate, Arc<ReplicationMetrics>, ForcedPolicyWriter) {
        let metrics = Arc::new(ReplicationMetrics::default());
        let (gate, forced) = build(
            initial,
            journal_pos,
            Some(Arc::clone(&metrics)),
            Some([
                Arc::new(AtomicBool::new(true)),
                Arc::new(AtomicBool::new(false)),
            ]),
        );
        (gate, metrics, forced)
    }

    /// The replica of [`gate_with_one_replica`] now holds every event up
    /// to `pos` in memory.
    fn replica_holds(metrics: &ReplicationMetrics, pos: u64) {
        metrics.in_memory_sequence[0].store(pos, Ordering::Release);
    }

    /// An application slot (no policy change) for the event at `wire_seq`.
    fn reply(wire_seq: u64) -> OutputSlot<u8, u8> {
        OutputSlot {
            wire_seq,
            payload: OutputPayload::Report(0),
            is_last_in_request: true,
            ..OutputSlot::default()
        }
    }

    /// The output slot of a policy change journaled at `wire_seq`.
    fn change(wire_seq: u64, policy: AckPolicy, unwitnessed: bool) -> OutputSlot<u8, u8> {
        change_byte(wire_seq, policy.to_byte(), unwitnessed)
    }

    fn change_byte(wire_seq: u64, policy: AckPolicyByte, unwitnessed: bool) -> OutputSlot<u8, u8> {
        OutputSlot {
            wire_seq,
            payload: OutputPayload::AckPolicyChange(melin_journal::AckPolicyChange {
                policy,
                unwitnessed,
            }),
            ..OutputSlot::default()
        }
    }

    fn admit(
        gate: &mut DurabilityGate,
        slot: &OutputSlot<u8, u8>,
        shutdown: &AtomicBool,
    ) -> GateOutcome {
        #[cfg(feature = "tick-to-trade")]
        let mut tracker = GateCrossTracker::new(slot.wire_seq);
        gate.admit(
            slot,
            shutdown,
            #[cfg(feature = "tick-to-trade")]
            &mut tracker,
            || {},
        )
    }

    /// How long a held slot is watched before it is released: long enough
    /// that a gate wrongly open would have returned.
    const HOLD: Duration = Duration::from_millis(50);

    /// How long a slot may take to open once nothing holds it.
    const OPEN_WITHIN: Duration = Duration::from_secs(10);

    /// Admit `slot` on a helper thread; after `hold` (if any) record
    /// whether it is still waiting, run `release`, and give it
    /// [`OPEN_WITHIN`] to open. Shutdown stops a gate that never opens, so
    /// a regression fails the test instead of hanging it. Returns whether
    /// it was held, and whether it then opened.
    fn admit_watched(
        gate: &mut DurabilityGate,
        slot: &OutputSlot<u8, u8>,
        hold: Option<Duration>,
        release: impl FnOnce(),
    ) -> (bool, bool) {
        let shutdown = AtomicBool::new(false);
        std::thread::scope(|s| {
            let admitting = s.spawn(|| admit(gate, slot, &shutdown));
            let held = hold.is_some_and(|hold| {
                std::thread::sleep(hold);
                !admitting.is_finished()
            });
            release();
            let deadline = Instant::now() + OPEN_WITHIN;
            while !admitting.is_finished() && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(1));
            }
            shutdown.store(true, Ordering::Relaxed);
            let outcome = admitting.join().expect("the admitting thread panicked");
            (held, matches!(outcome, GateOutcome::Open))
        })
    }

    /// Admit `slot`: it must open on the state as it is.
    #[track_caller]
    fn admit_now(gate: &mut DurabilityGate, slot: &OutputSlot<u8, u8>) {
        let (_, opened) = admit_watched(gate, slot, None, || {});
        assert!(opened, "slot {} did not open", slot.wire_seq);
    }

    /// Admit `slot`: it must still be held after [`HOLD`], and open once
    /// `release` has run.
    #[track_caller]
    fn assert_held_until(
        gate: &mut DurabilityGate,
        slot: &OutputSlot<u8, u8>,
        release: impl FnOnce(),
    ) {
        let (held, opened) = admit_watched(gate, slot, Some(HOLD), release);
        assert!(
            held,
            "slot {} was admitted before its release",
            slot.wire_seq
        );
        assert!(
            opened,
            "slot {} stayed held after its release",
            slot.wire_seq
        );
    }

    /// The policy the health gauge reports in force.
    fn in_force(gate: &DurabilityGate) -> Option<AckPolicy> {
        gate.inputs
            .utilization
            .ack_policy_in_force
            .load()
            .map(|byte| AckPolicy::from_byte(byte).expect("a known policy"))
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
        g.admin
            .store(AckPolicy::TwoDisks.as_u8(), Ordering::Relaxed);
        g.observe_policy_swap();
        assert_eq!(g.active_policy, AckPolicy::TwoDisks);
        assert_eq!(g.policy, AckPolicy::TwoDisks.to_policy());
        assert_eq!(g.cached_durable_pos, 0);
        assert_eq!(in_force(&g), Some(AckPolicy::TwoDisks));

        let mut g = gate(AckPolicy::DiskAndRam, 5);
        let_time_pass();
        g.admin.store(AckPolicy::Disk.as_u8(), Ordering::Relaxed);
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
        g.admin.store(CORRUPT, Ordering::Relaxed);
        g.observe_policy_swap();
        assert_eq!(g.active_policy, AckPolicy::DiskAndRam);
        assert_eq!(g.policy, AckPolicy::DiskAndRam.to_policy());
        assert_eq!(degraded_nanos(&g), 0, "no re-seed on a corrupted byte");

        let mut g = gate(AckPolicy::Disk, 5);
        g.admin.store(CORRUPT, Ordering::Relaxed);
        g.observe_policy_swap();
        assert_eq!(g.active_policy, AckPolicy::Disk);
        assert_eq!(g.cached_durable_pos, 5, "cached position kept");
    }

    /// A gate started under `initial`, its policy resolved against an
    /// admin byte of `admin_seen`, the atomic now holding `admin_now`.
    fn gate_resolved_against(
        initial: AckPolicy,
        admin_seen: AckPolicy,
        admin_now: AckPolicy,
    ) -> (DurabilityGate, Arc<AtomicU8>) {
        let admin = Arc::new(AtomicU8::new(admin_now.as_u8()));
        let g = DurabilityGate::new(
            GateInputs {
                journal_persisted_wire_seq: DurableWireSeqCursor::detached(WireSeq::new(5)),
                replication_metrics: None,
                replica_active: None,
                utilization: Arc::new(StageUtilization::new()),
                wait: WaitStrategy::SpinThenYield,
            },
            GatePolicy {
                initial,
                admin: Arc::clone(&admin),
                admin_seen: admin_seen.as_u8(),
                forced: forced_policy_cell().1,
            },
            "test",
        );
        (g, admin)
    }

    /// The admin atomic is followed by its changes, not its value: a gate
    /// started from the lineage under a policy other than the requested
    /// one keeps it until an operator sends a policy.
    #[test]
    fn the_gate_follows_admin_changes_not_the_admin_byte() {
        let (mut g, admin) =
            gate_resolved_against(AckPolicy::TwoDisks, AckPolicy::Disk, AckPolicy::Disk);
        assert_eq!(in_force(&g), Some(AckPolicy::TwoDisks));
        g.observe_policy_swap();
        assert_eq!(g.active_policy, AckPolicy::TwoDisks, "no admin change yet");
        admin.store(AckPolicy::Ram.as_u8(), Ordering::Relaxed);
        g.observe_policy_swap();
        assert_eq!(g.active_policy, AckPolicy::Ram);
    }

    /// An admin swap landing after the initial policy was resolved but
    /// before the gate started is still applied.
    #[test]
    fn an_admin_swap_before_the_gate_starts_is_not_lost() {
        let (mut g, _admin) = gate_resolved_against(
            AckPolicy::DiskAndRam,
            AckPolicy::DiskAndRam,
            AckPolicy::Disk,
        );
        g.observe_policy_swap();
        assert_eq!(g.active_policy, AckPolicy::Disk);
    }

    /// The lineage's policy wins over the requested one; a lineage with
    /// none takes the requested one; an unknown lineage byte is refused.
    #[test]
    fn the_initial_policy_is_the_lineages_when_it_has_one() {
        assert_eq!(
            initial_gate_policy(None, AckPolicy::Disk),
            Ok(AckPolicy::Disk)
        );
        assert_eq!(
            initial_gate_policy(Some(AckPolicy::TwoDisks.to_byte()), AckPolicy::Disk),
            Ok(AckPolicy::TwoDisks)
        );
        let refused = initial_gate_policy(Some(AckPolicyByte(CORRUPT)), AckPolicy::Disk)
            .expect_err("an unknown byte is refused");
        assert!(refused.contains("238"), "names the byte: {refused}");
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
        g.admin.store(AckPolicy::Disk.as_u8(), Ordering::Relaxed);
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
        g.admin.store(CORRUPT, Ordering::Relaxed);
        let shutdown = AtomicBool::new(false);
        assert!(matches!(wait(&mut g, 3, &shutdown), GateOutcome::Open));
        assert_eq!(g.active_policy, AckPolicy::Disk);
        assert_eq!(g.policy, AckPolicy::Disk.to_policy());
    }

    /// A change takes effect at its slot: the slot before it is gated
    /// under the old policy, the slot after it under the new one, and the
    /// health gauge follows.
    #[test]
    fn a_change_applies_from_its_own_slot_on() {
        let (mut g, metrics, _forced) = gate_with_one_replica(AckPolicy::Disk, 10);
        assert_eq!(in_force(&g), Some(AckPolicy::Disk));
        admit_now(&mut g, &reply(4));
        admit_now(&mut g, &change(5, AckPolicy::DiskAndRam, false));
        assert_eq!(g.active_policy, AckPolicy::DiskAndRam);
        assert_eq!(in_force(&g), Some(AckPolicy::DiskAndRam));
        // On disk already, but `disk+ram` also wants the replica.
        assert_held_until(&mut g, &reply(6), || replica_holds(&metrics, 6));
    }

    /// A change waits until the policy before it has cleared its entry:
    /// under `disk+ram`, a switch to `disk` holds until the replica has
    /// the entry, and only the slots after it then open on the journal
    /// alone.
    #[test]
    fn a_change_is_held_until_the_old_policy_clears_its_entry() {
        let (mut g, metrics, _forced) = gate_with_one_replica(AckPolicy::DiskAndRam, 10);
        assert_held_until(&mut g, &change(5, AckPolicy::Disk, false), || {
            replica_holds(&metrics, 5)
        });
        assert_eq!(g.active_policy, AckPolicy::Disk);
        admit_now(&mut g, &reply(8));
    }

    /// Two pending changes are applied one by one, never merged: between
    /// `disk` → `disk+ram` at 3 and back to `disk` at 6, the slots are
    /// gated under `disk+ram`, and the second change itself waits for it.
    #[test]
    fn two_pending_changes_are_never_merged() {
        let (mut g, metrics, _forced) = gate_with_one_replica(AckPolicy::Disk, 10);
        admit_now(&mut g, &change(3, AckPolicy::DiskAndRam, false));
        assert_held_until(&mut g, &reply(4), || replica_holds(&metrics, 4));
        assert_held_until(&mut g, &change(6, AckPolicy::Disk, false), || {
            replica_holds(&metrics, 6)
        });
        assert_eq!(g.active_policy, AckPolicy::Disk);
        admit_now(&mut g, &reply(7));
    }

    /// A forced swap frees a slot stuck ahead of it, but only once its own
    /// entry is on this node's disk; later in-order changes at or below
    /// it — its own slot included — are stale and ignored, and a change
    /// after it applies again.
    #[test]
    fn a_forced_swap_frees_an_earlier_stuck_slot_once_its_entry_is_on_disk() {
        // No replica connected: `disk+ram` cannot open at all.
        let (mut g, mut forced) = build(AckPolicy::DiskAndRam, 4, None, None);
        let journal = g.inputs.journal_persisted_wire_seq.clone();
        forced.store(ForcedPolicy {
            wire_seq: 6,
            policy: AckPolicy::Disk.to_byte(),
        });
        // The stuck slot is on disk already; the forced entry is not.
        assert_held_until(&mut g, &reply(3), || journal.store(WireSeq::new(6)));
        assert_eq!(g.active_policy, AckPolicy::Disk);
        assert_eq!(in_force(&g), Some(AckPolicy::Disk));

        let cached = g.cached_durable_pos;
        admit_now(&mut g, &change(5, AckPolicy::TwoDisks, false));
        admit_now(&mut g, &change(6, AckPolicy::Disk, true));
        assert_eq!(g.active_policy, AckPolicy::Disk, "stale changes ignored");
        assert_eq!(g.cached_durable_pos, cached, "no switch, no reset");

        journal.store(WireSeq::new(7));
        admit_now(&mut g, &change(7, AckPolicy::Ram, false));
        assert_eq!(g.active_policy, AckPolicy::Ram);
    }

    /// A forced change the gate reached in order is not applied a second
    /// time by a later wait: the change after it stays in force, and the
    /// slot it gates stays held.
    #[test]
    fn a_forced_change_applied_in_order_is_not_reapplied() {
        let (mut g, metrics, mut forced) = gate_with_one_replica(AckPolicy::DiskAndRam, 10);
        replica_holds(&metrics, 3);
        forced.store(ForcedPolicy {
            wire_seq: 2,
            policy: AckPolicy::Disk.to_byte(),
        });
        admit_now(&mut g, &change(2, AckPolicy::Disk, true));
        admit_now(&mut g, &change(3, AckPolicy::DiskAndRam, false));
        assert_held_until(&mut g, &reply(4), || replica_holds(&metrics, 4));
        assert_eq!(g.active_policy, AckPolicy::DiskAndRam);
    }

    /// A change naming a policy this build does not know keeps the prior
    /// policy, in order and forced alike.
    #[test]
    fn an_unknown_policy_in_a_change_keeps_the_prior_policy() {
        let (mut g, mut forced) = build(AckPolicy::Disk, 10, None, None);
        admit_now(&mut g, &change_byte(4, AckPolicyByte(CORRUPT), false));
        assert_eq!(g.active_policy, AckPolicy::Disk);
        forced.store(ForcedPolicy {
            wire_seq: 6,
            policy: AckPolicyByte(CORRUPT),
        });
        // The journal is past the forced entry: the wait reads it and
        // keeps the prior policy, then opens.
        g.cached_durable_pos = 0;
        let shutdown = AtomicBool::new(false);
        assert!(matches!(wait(&mut g, 5, &shutdown), GateOutcome::Open));
        assert_eq!(g.active_policy, AckPolicy::Disk);
        assert_eq!(g.applied_through, 6, "the forced entry is consumed once");
    }

    /// The flush hook runs only for a slot that has to wait.
    #[test]
    fn before_wait_runs_only_when_the_slot_waits() {
        let mut g = gate(AckPolicy::Disk, 5);
        let shutdown = AtomicBool::new(false);
        for (wire_seq, waits) in [(5, false), (6, true)] {
            let mut ran = false;
            if waits {
                // Durable while the hook runs, so the wait opens at once.
                let journal = g.inputs.journal_persisted_wire_seq.clone();
                journal.store(WireSeq::new(wire_seq));
            }
            #[cfg(feature = "tick-to-trade")]
            let mut tracker = GateCrossTracker::new(wire_seq);
            let outcome = g.admit(
                &reply(wire_seq),
                &shutdown,
                #[cfg(feature = "tick-to-trade")]
                &mut tracker,
                || ran = true,
            );
            assert!(matches!(outcome, GateOutcome::Open));
            assert_eq!(ran, waits, "slot {wire_seq}");
        }
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
