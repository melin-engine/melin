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
//! # Degraded release
//!
//! A primary halted for want of a replica cannot meet a policy that needs
//! one, so the replies it holds — writes it had sequenced before the
//! halt, and queries, whose answers may reflect them — would wait for a
//! replica to come back. Once the halt has lasted a grace period, the
//! gate releases a held slot on a second, weaker condition: the slot's
//! event is fsynced on the primary's own journal. Such a reply goes out
//! with `BatchEndDegraded` ([`Backing::PrimaryOnly`]) instead of
//! `BatchEnd`, so the client knows only the primary's disk backs it.
//!
//! The policy is always evaluated first, on every slot, and a slot it
//! confirms is a full reply. The degraded release keeps a position of its
//! own and never writes the cached durable position: were it to, every
//! later slot would pass the cached check without waiting and go out with
//! a full `BatchEnd`, an ack the policy never gave. The release is driven
//! by the halt (no replica connected), never by gate lag: a connected
//! replica that is merely slow is backpressure, and its replies wait.
//! It is an operator switch, on by default ([`DegradedRelease`]).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::time::{Duration, Instant};

use melin_app::amortized_timer::AmortizedTimer;
use melin_pipeline::wait::WaitStrategy;
use melin_transport_core::DurableWireSeqCursor;
use melin_transport_core::pipeline::{OutputSlot, StageUtilization};
#[cfg(feature = "tick-to-trade")]
use melin_transport_core::trace;
use melin_wire_protocol::control::TransportResponse;
use melin_wire_protocol::control_codec;

use crate::ack_policy::{AckPolicy, Blocker, Policy};
use crate::halt::DegradedRelease;
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

/// How long a node must have been halted for want of a replica before
/// the gate releases held replies on the primary's own fsync, unless the
/// operator sets `--degraded-ack-grace-ms`.
///
/// Long enough that a replica dropped by a short blip, which reconnects
/// and catches up within it, confirms the held replies in full rather
/// than turning them into degraded ones: a replica's first reconnect
/// attempt comes a second after it loses its primary. Short enough that
/// clients of a primary that has really lost its replicas are answered
/// well inside a typical client timeout. A constant of its own, not the
/// DPDK replication liveness deadline: kernel TCP has no such deadline,
/// and the halt itself, which starts the grace, already comes after it.
pub(crate) const DEFAULT_DEGRADED_ACK_GRACE_MS: u64 = 2_000;

/// What backs a reply the gate released, and so which terminator its
/// request gets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Backing {
    /// The ack policy in force confirmed the slot's event: `BatchEnd`.
    Policy,
    /// Released while halted past the grace period, on the primary's own
    /// fsync alone: `BatchEndDegraded`.
    PrimaryOnly,
}

/// How a durability wait ended.
#[must_use]
pub(crate) enum GateOutcome {
    /// The slot's reply may be sent, backed as given.
    Open(Backing),
    /// Shutdown was requested while the gate was closed. The reply the
    /// policy never confirmed must not be sent; the caller goes straight
    /// back to its shutdown branch.
    Shutdown,
}

/// The two pre-encoded request terminators, full wire frames (length
/// prefix and tag), shared by both stages. Encoded once at startup:
/// every request ends with one, and picking one of two buffers in hand
/// costs nothing on the normal path.
pub(crate) struct BatchEnds {
    /// `BatchEnd`: backed as the policy requires.
    pub policy: Vec<u8>,
    /// `BatchEndDegraded`: backed by the primary's disk alone.
    pub primary_only: Vec<u8>,
}

impl BatchEnds {
    pub(crate) fn new() -> Self {
        let encode = |response| {
            let mut buf = [0u8; 8];
            let written = control_codec::encode_transport_response(&response, &mut buf)
                .expect("a tag-only control frame encodes into eight bytes");
            buf[..written].to_vec()
        };
        Self {
            policy: encode(TransportResponse::BatchEnd),
            primary_only: encode(TransportResponse::BatchEndDegraded),
        }
    }
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
    /// Degraded release while halted; `None` when off (or standalone).
    pub degraded_release: Option<DegradedRelease>,
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
    /// Highest wire seq released on the primary's own fsync while
    /// halted — the degraded release's own position. Never folded into
    /// `cached_durable_pos`: see the module docs. `u64`, like that cache,
    /// because it is compared against `OutputSlot::wire_seq`.
    degraded_pos: u64,
    /// When the current halt began: the latest loss's stamp, or the
    /// earliest sighting when the stamp is missing (see
    /// `HaltState::halted_for`); `None` while a replica is connected.
    halt_started: Option<Instant>,
    /// The degraded release is under way: the halt has outlasted the
    /// grace period. Kept for the `warn!` on entering and leaving it.
    releasing_degraded: bool,
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
            degraded_pos: 0,
            halt_started: None,
            releasing_degraded: false,
        }
    }

    /// The terminator for the request `backing` closes, from `ends`.
    /// Counts each degraded one for `melin_degraded_acks_total`: called
    /// once per request (on its last slot), by both stages.
    #[inline]
    pub(crate) fn batch_end<'e>(&self, backing: Backing, ends: &'e BatchEnds) -> &'e [u8] {
        match backing {
            Backing::Policy => &ends.policy,
            Backing::PrimaryOnly => {
                self.inputs
                    .utilization
                    .degraded_acks
                    .fetch_add(1, Ordering::Relaxed);
                &ends.primary_only
            }
        }
    }

    /// Whether the degraded release is open now: degraded release on, no
    /// replica connected, a policy that needs one, and the halt older than
    /// the grace period. Also tracks the halt's start and logs entering
    /// and leaving the release.
    ///
    /// One acquire load while a replica is connected (the normal path,
    /// where this runs only once the policy has already failed a slot); a
    /// clock read on top while halted.
    fn degraded_release_open(&mut self) -> bool {
        let Some(release) = self.inputs.degraded_release.as_ref() else {
            return false;
        };
        let halted = release.halt_state.no_replica();
        let started = if halted {
            let now = Instant::now();
            // `None` only if a replica joined since the load above: the
            // halt is ending, and reads as one that has just begun.
            let halted_for = release.halt_state.halted_for(now).unwrap_or(Duration::ZERO);
            // An `Instant` before the process's clock origin is not
            // representable; such a halt began at the origin at the
            // latest, and `now` is no later than that by much.
            let stamped = now.checked_sub(halted_for).unwrap_or(now);
            // A non-zero age comes from the loss's own stamp, which is
            // authoritative: it is the *latest* loss, so a replica that
            // joined and left again between two of the gate's looks
            // (unseen while idle, checked once a second) restarts the
            // grace period, as documented. Only a zero age — the stamp
            // went missing (see `HaltState::halted_for`), or the halt has
            // just begun — falls back to the earliest start seen, so a
            // halt whose stamp was cleared still ages from its first
            // sighting.
            let started = if halted_for.is_zero() {
                self.halt_started.map_or(stamped, |seen| seen.min(stamped))
            } else {
                stamped
            };
            self.halt_started = Some(started);
            Some((now, started))
        } else {
            self.halt_started = None;
            None
        };
        // Under a policy the primary's own disk satisfies (`disk`), the
        // policy itself confirms on the same cursor: nothing is weaker
        // than it, and there is nothing to release.
        let open = match started {
            Some((now, started)) if self.active_policy.needs_replica() => {
                now.saturating_duration_since(started) >= release.grace
            }
            _ => false,
        };
        if !open && self.releasing_degraded {
            self.releasing_degraded = false;
            tracing::warn!(
                stage = self.stage,
                policy = self.active_policy.as_str(),
                replica_connected = !halted,
                "degraded release stopped: held replies wait for the ack policy again"
            );
        }
        if open && !self.releasing_degraded {
            self.releasing_degraded = true;
            tracing::warn!(
                stage = self.stage,
                policy = self.active_policy.as_str(),
                grace_ms = release.grace.as_millis() as u64,
                "no replica for the grace period: replies held for the ack policy are now \
                 released once this node's own journal holds them, marked as backed by the \
                 primary alone (BatchEndDegraded)"
            );
        }
        open
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
                return GateOutcome::Open(Backing::Policy);
            }

            // The policy has not confirmed the slot. While halted past
            // the grace period, the primary's own fsync releases it,
            // marked. Its position is the release's own: `cached_durable_pos`
            // keeps what the policy confirmed, so the next slot is
            // evaluated against the policy again rather than waved
            // through on this weaker condition.
            if self.degraded_release_open() {
                self.degraded_pos = self.degraded_pos.max(journal_pos.get());
                if self.degraded_pos >= needed {
                    return GateOutcome::Open(Backing::PrimaryOnly);
                }
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
        // Track the halt on a quiet node too, so entering and leaving the
        // degraded release is logged when it happens rather than at the
        // next held reply. Whether it is open matters only to a slot
        // being held, and none is.
        self.degraded_release_open();
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
    use std::sync::atomic::AtomicU32;
    use std::sync::mpsc;

    use melin_transport_core::WireSeq;
    use melin_transport_core::halt_state::HaltState;

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
            degraded_release: None,
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
            degraded_release: None,
        };
        DurabilityGate::new(inputs, "test")
    }

    /// A replicated primary's gate and the levers a test pulls on it: the
    /// journal cursor, one replica slot's cursors and active flag, and the
    /// replica count the halt is judged on.
    struct Primary {
        gate: DurabilityGate,
        journal: DurableWireSeqCursor,
        metrics: Arc<ReplicationMetrics>,
        active: Arc<AtomicBool>,
        replicas: Arc<AtomicU32>,
        halt_state: Arc<HaltState>,
    }

    impl Primary {
        /// One replica slot, connected and caught up to `pos` when
        /// `connected`, else gone (the node halted since the attach).
        /// `grace` is the degraded release's, `None` for the switch off.
        fn new(policy: AckPolicy, pos: u64, connected: bool, grace: Option<Duration>) -> Self {
            let metrics = Arc::new(ReplicationMetrics::default());
            let active = Arc::new(AtomicBool::new(connected));
            let replicas = Arc::new(AtomicU32::new(u32::from(connected)));
            if connected {
                metrics.acked_sequence[0].store(pos, Ordering::Relaxed);
                metrics.in_memory_sequence[0].store(pos, Ordering::Relaxed);
            }
            let halt_state = Arc::new(HaltState::new());
            halt_state
                .attach(Arc::clone(&replicas))
                .expect("first attach");
            let journal = DurableWireSeqCursor::detached(WireSeq::new(pos));
            let inputs = GateInputs {
                journal_persisted_wire_seq: journal.clone(),
                ack_policy: Arc::new(AtomicU8::new(policy.as_u8())),
                replication_metrics: Some(Arc::clone(&metrics)),
                replica_active: Some([Arc::clone(&active), Arc::new(AtomicBool::new(false))]),
                utilization: Arc::new(StageUtilization::new()),
                wait: WaitStrategy::SpinThenYield,
                degraded_release: grace.map(|grace| DegradedRelease {
                    halt_state: Arc::clone(&halt_state),
                    grace,
                }),
            };
            Self {
                gate: DurabilityGate::new(inputs, "test"),
                journal,
                metrics,
                active,
                replicas,
                halt_state,
            }
        }

        /// The replica leaves, as a sender records it.
        fn replica_leaves(&self) {
            self.halt_state.on_replica_leaving();
            self.active.store(false, Ordering::Release);
            self.replicas.fetch_sub(1, Ordering::Release);
        }

        /// A replica joins and streams, acked up to `pos`.
        fn replica_streams(&self, pos: u64) {
            self.replicas.fetch_add(1, Ordering::SeqCst);
            self.halt_state.on_replica_joined();
            self.metrics.acked_sequence[0].store(pos, Ordering::Relaxed);
            self.metrics.in_memory_sequence[0].store(pos, Ordering::Relaxed);
            self.active.store(true, Ordering::Release);
        }

        fn journal_at(&self, pos: u64) {
            self.journal.store(WireSeq::new(pos));
        }

        fn degraded_acks(&self) -> u64 {
            self.gate
                .inputs
                .utilization
                .degraded_acks
                .load(Ordering::Relaxed)
        }

        /// Whether a slot at `wire_seq` is released within `patience`,
        /// and how. A timer thread raises the shutdown flag once patience
        /// runs out, since a gate that holds the slot never returns on its
        /// own: `None` is "still held when patience ran out".
        fn release(self, wire_seq: u64, patience: Duration) -> (Self, Option<Backing>) {
            let stop = Arc::new(AtomicBool::new(false));
            let stop_after = Arc::clone(&stop);
            let timer = std::thread::spawn(move || {
                std::thread::sleep(patience);
                stop_after.store(true, Ordering::Relaxed);
            });
            let mut this = self;
            let outcome = wait(&mut this.gate, wire_seq, &stop);
            stop.store(true, Ordering::Relaxed);
            timer.join().expect("timer thread");
            let backing = match outcome {
                GateOutcome::Open(backing) => Some(backing),
                GateOutcome::Shutdown => None,
            };
            (this, backing)
        }
    }

    /// Long enough for a gate that releases to have done so; short
    /// enough that a held slot costs the suite little.
    const PATIENCE: Duration = Duration::from_millis(300);

    /// A grace period that has long passed by the first check: the node
    /// halted at the attach.
    const NO_GRACE: Option<Duration> = Some(Duration::ZERO);

    #[test]
    fn no_degraded_release_before_the_grace_period() {
        let p = Primary::new(
            AckPolicy::DiskAndRam,
            5,
            false,
            Some(Duration::from_secs(60)),
        );
        let (p, released) = p.release(5, PATIENCE);
        assert_eq!(released, None, "held for the grace period");
        assert_eq!(p.degraded_acks(), 0);
    }

    #[test]
    fn a_halted_node_past_the_grace_releases_on_its_own_fsync_marked() {
        for policy in [AckPolicy::Ram, AckPolicy::DiskAndRam, AckPolicy::TwoDisks] {
            let p = Primary::new(policy, 5, false, NO_GRACE);
            let (p, released) = p.release(5, PATIENCE);
            assert_eq!(released, Some(Backing::PrimaryOnly), "{policy}");
            assert_eq!(p.gate.cached_durable_pos, 0, "the policy confirmed nothing");
            assert_eq!(p.gate.degraded_pos, 5);
        }
    }

    #[test]
    fn the_grace_period_runs_from_the_loss() {
        let p = Primary::new(
            AckPolicy::DiskAndRam,
            5,
            true,
            Some(Duration::from_millis(150)),
        );
        p.replica_leaves();
        let started = Instant::now();
        let (_, released) = p.release(5, Duration::from_secs(5));
        assert_eq!(released, Some(Backing::PrimaryOnly));
        assert!(
            started.elapsed() >= Duration::from_millis(150),
            "released before the grace ran out: {:?}",
            started.elapsed()
        );
    }

    /// A replica that joins and leaves again between two of the gate's
    /// looks, so the gate never sees the halt end, still restarts the
    /// grace period: it runs from the latest loss, not the first.
    #[test]
    fn an_unseen_rejoin_restarts_the_grace_period() {
        // Wide enough that a stalled runner between the second loss and
        // the first check still lands well inside the grace from it.
        let grace = Duration::from_millis(1_000);
        let mut p = Primary::new(AckPolicy::DiskAndRam, 5, true, Some(grace));
        p.replica_leaves();
        assert!(!p.gate.degraded_release_open(), "the halt has just begun");
        std::thread::sleep(Duration::from_millis(700));
        // The blip, between two looks.
        p.replica_streams(5);
        p.replica_leaves();
        let second_loss = Instant::now();
        std::thread::sleep(Duration::from_millis(400));
        // Past the grace from the first loss, not from the second.
        assert!(
            !p.gate.degraded_release_open(),
            "aged from the first loss: {:?} since the second",
            second_loss.elapsed()
        );
        std::thread::sleep(grace.saturating_sub(second_loss.elapsed()));
        assert!(
            p.gate.degraded_release_open(),
            "past the grace from the second loss"
        );
    }

    /// `disk` is met by the same cursor: every reply is a full one, halted
    /// or not, and nothing is counted as degraded.
    #[test]
    fn no_degraded_release_under_disk() {
        let p = Primary::new(AckPolicy::Disk, 5, false, NO_GRACE);
        let (p, released) = p.release(5, PATIENCE);
        assert_eq!(released, Some(Backing::Policy));
        let (p, released) = p.release(6, PATIENCE);
        assert_eq!(released, None, "not yet on the journal");
        assert!(!p.gate.releasing_degraded);
    }

    #[test]
    fn no_degraded_release_with_the_switch_off() {
        let p = Primary::new(AckPolicy::DiskAndRam, 5, false, None);
        let (p, released) = p.release(5, PATIENCE);
        assert_eq!(released, None, "today's stall, unchanged");
        assert_eq!(p.degraded_acks(), 0);
    }

    /// A replica that is connected but behind is backpressure, not a halt:
    /// its replies wait however long it lags.
    #[test]
    fn no_degraded_release_while_a_replica_is_connected_but_behind() {
        let p = Primary::new(AckPolicy::TwoDisks, 5, true, NO_GRACE);
        p.journal_at(9);
        let (_, released) = p.release(9, PATIENCE);
        assert_eq!(released, None, "held for the replica");
    }

    /// Under `ram` an event may be only in the primary's memory: the
    /// degraded release waits for the primary's fsync like any other.
    #[test]
    fn an_unfsynced_slot_under_ram_is_held_until_the_primary_fsyncs_it() {
        let p = Primary::new(AckPolicy::Ram, 5, false, NO_GRACE);
        let (p, released) = p.release(6, PATIENCE);
        assert_eq!(released, None, "the primary's disk does not hold it yet");
        p.journal_at(6);
        let (_, released) = p.release(6, PATIENCE);
        assert_eq!(released, Some(Backing::PrimaryOnly));
    }

    #[test]
    fn a_full_ack_resumes_once_a_replica_streams_again() {
        let p = Primary::new(AckPolicy::DiskAndRam, 5, false, NO_GRACE);
        let (p, released) = p.release(5, PATIENCE);
        assert_eq!(released, Some(Backing::PrimaryOnly));
        assert!(p.gate.releasing_degraded);

        p.journal_at(7);
        p.replica_streams(7);
        let (p, released) = p.release(6, PATIENCE);
        assert_eq!(released, Some(Backing::Policy), "the policy is met again");
        let (p, released) = p.release(7, PATIENCE);
        assert_eq!(released, Some(Backing::Policy));
        assert_eq!(p.gate.cached_durable_pos, 7);

        // A connected replica that falls behind holds replies again: the
        // degraded release stopped with the halt.
        p.journal_at(9);
        let (p, released) = p.release(9, PATIENCE);
        assert_eq!(released, None);
        assert!(!p.gate.releasing_degraded);
    }

    /// The cache trap: a degraded release must not advance the position
    /// the policy confirmed, or the next slot would pass the cached check
    /// and go out with a full `BatchEnd`.
    #[test]
    fn after_a_degraded_release_the_next_slot_is_still_evaluated_and_still_degraded() {
        let p = Primary::new(AckPolicy::DiskAndRam, 9, false, NO_GRACE);
        let (p, released) = p.release(5, PATIENCE);
        assert_eq!(released, Some(Backing::PrimaryOnly));

        let next = OutputSlot::<u64, u64> {
            wire_seq: 6,
            ..OutputSlot::default()
        };
        assert!(
            p.gate.needs_wait(&next),
            "the next slot is not waved through on the degraded position"
        );
        let (p, released) = p.release(6, PATIENCE);
        assert_eq!(
            released,
            Some(Backing::PrimaryOnly),
            "never a full BatchEnd"
        );
        assert_eq!(p.gate.cached_durable_pos, 0);
    }

    /// The terminator follows the backing, and only degraded ones count.
    #[test]
    fn the_terminator_follows_the_backing_and_counts_degraded_acks() {
        let p = Primary::new(AckPolicy::DiskAndRam, 5, false, NO_GRACE);
        let ends = BatchEnds::new();
        assert_eq!(p.gate.batch_end(Backing::Policy, &ends), ends.policy);
        assert_eq!(p.degraded_acks(), 0);
        assert_eq!(
            p.gate.batch_end(Backing::PrimaryOnly, &ends),
            ends.primary_only
        );
        assert_eq!(p.degraded_acks(), 1);
        assert_eq!(
            ends.policy[4],
            melin_wire_protocol::control_codec::TAG_BATCH_END
        );
        assert_eq!(
            ends.primary_only[4],
            melin_wire_protocol::control_codec::TAG_BATCH_END_DEGRADED
        );
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
        assert!(matches!(
            wait(&mut g, 3, &shutdown),
            GateOutcome::Open(Backing::Policy)
        ));
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
        assert!(matches!(
            wait(&mut g, 3, &shutdown),
            GateOutcome::Open(Backing::Policy)
        ));
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
        assert!(matches!(
            wait(&mut g, 3, &shutdown),
            GateOutcome::Open(Backing::Policy)
        ));
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
