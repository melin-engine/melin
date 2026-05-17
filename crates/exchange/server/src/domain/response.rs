//! io_uring-based response stage — routes matching output to connections via
//! `IORING_OP_SEND`.
//!
//! Replaces the blocking `write(2)` + `BufWriter` flush path with batched
//! io_uring sends. Instead of N `write(2)` syscalls (one per dirty connection
//! on flush), we submit N SEND SQEs in a single `io_uring_enter` call.
//!
//! Same SPSC consumption and journal cursor gating as `response.rs`.
//! Runs on a dedicated OS thread.

use std::collections::{HashMap, HashSet};
use std::os::unix::io::RawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use io_uring::{IoUring, opcode, types};
use tracing::{debug, error};

use melin_disruptor::ring;

use crate::runtime::durability_policy::{CursorView, DurabilityMode, EvalStatus, Policy};
use crate::runtime::replication::ReplicationMetrics;
use crate::{OutputPayload, OutputSlot};
use melin_transport_core::pipeline::StageUtilization;
#[cfg(feature = "latency-trace")]
use melin_transport_core::trace;
use melin_types::types::QueryResponse;

use melin_protocol::codec;
use melin_protocol::message::ResponseKind;

/// Maximum number of output slots consumed per batch.
const MAX_BATCH: usize = 1024;

/// Maximum encoded response size. PositionSnapshot is the largest variant
/// at up to 330 bytes (length(4) + tag(1) + account(4) + count(1) +
/// 16*(currency(4)+free(8)+reserved(8))). 512 bytes covers all variants.
const MAX_RESPONSE_BUF: usize = 512;

/// io_uring submission queue depth for sends. Must be ≥ max concurrent
/// connections to avoid SQ overflow when all connections are dirty.
/// Power of 2 for io_uring alignment. 4096 supports 1024+ client
/// benchmarks where all connections flush simultaneously.
const RING_SIZE: u32 = 4096;

/// Maximum accumulated send buffer per connection (64 KiB). If a client
/// falls behind and the buffer exceeds this, the connection is dropped.
/// 64 KiB holds ~500 response frames — well beyond any reasonable lag.
const MAX_SEND_BUF: usize = 64 * 1024;

pub use crate::ControlEvent;

/// Configuration and shared state for the response stage.
pub struct Response {
    /// Highest wire seq durably persisted on the primary's journal.
    /// In the same sequence space as `OutputSlot.wire_seq` and the
    /// replica metrics (`metrics.in_memory_sequence` /
    /// `metrics.acked_sequence`), so the durability gate can compare
    /// these values numerically and the comparison is meaningful
    /// regardless of `starting_sequence` (fresh vs recovered primary).
    /// Updated by the journal stage after every fsync batch via
    /// `set_last_seq_publisher`.
    pub journal_persisted_wire_seq: Arc<AtomicU64>,
    /// Operator-selected durability mode, published through a shared
    /// [`AtomicU8`] so the admin `DURABILITY` command can swap it at
    /// runtime without restarting the node. The response stage reads
    /// this once per gate iteration with a relaxed load (cheaper than a
    /// `Mutex` or refcounted `Arc<Policy>` snapshot) and rebuilds its
    /// local [`Policy`] when the byte changes. See
    /// [`crate::runtime::durability_policy::DurabilityMode::as_u8`] for the
    /// encoding.
    pub durability_mode: Arc<std::sync::atomic::AtomicU8>,
    /// Per-slot replica cursors. `None` for standalone deployments
    /// (no replication wiring) — the policy then evaluates against the
    /// primary alone.
    pub replication_metrics: Option<Arc<ReplicationMetrics>>,
    /// Per-slot replica active flags. Only "true" slots are included in
    /// the cursor view fed to `Policy::evaluate`, so disconnected slots
    /// don't pollute the view with stale zero cursors. When the
    /// resulting view is too small to satisfy a clause, the policy
    /// reports degraded and the gate stalls.
    /// Mirrors `replication_metrics` — `None` in standalone.
    pub replica_active: Option<[Arc<AtomicBool>; 2]>,
    pub heartbeat_interval: Option<Duration>,
    pub busy_spin: bool,
    pub utilization: Arc<StageUtilization>,
}

/// Per-connection state for batched io_uring sends.
struct ConnectionEntry {
    fd: RawFd,
    /// Owns the write half of the socket to keep the fd alive.
    _owner: Box<dyn Send>,
    /// Accumulates encoded response frames between flushes.
    /// The full wire frame (length prefix + payload) is appended here.
    /// Vec's internal data pointer is heap-stable, so io_uring SEND SQEs
    /// referencing `as_ptr()` remain valid even if the HashMap relocates
    /// this struct — as long as we don't reallocate the Vec during in-flight sends.
    send_buf: Vec<u8>,
    /// Last time data was sent to this connection. Used for heartbeat scheduling.
    last_send: Instant,
}

/// Run the io_uring response stage loop. Blocks the calling thread until shutdown.
///
/// Consumes from the output SPSC, waits for durability confirmation, and
/// sends responses via io_uring SEND.
///
/// Durability gating: every gate iteration reads the journal cursor
/// (primary persisted) plus per-slot replica cursors (in-memory and
/// persisted) from `replication_metrics` and feeds them through the
/// configured [`Policy`]. See [`evaluate_durability`].
pub fn run(
    mut consumer: ring::Consumer<OutputSlot>,
    control_rx: mpsc::Receiver<ControlEvent>,
    config: Response,
    shutdown: &AtomicBool,
) {
    let Response {
        journal_persisted_wire_seq,
        durability_mode,
        replication_metrics,
        replica_active,
        heartbeat_interval,
        busy_spin,
        utilization,
    } = config;
    // Resolve the starting mode from the shared atomic and derive the
    // local Policy. The atomic is the single source of truth across the
    // process lifetime; the response thread keeps a thread-local copy
    // for cheap per-iteration use and rebuilds it when an admin
    // `DURABILITY` command swaps the atomic. Initialise as Hybrid (the
    // default mode) if the atomic ever holds a corrupted byte — better
    // than panicking on a degraded process and matches the default
    // operators see at boot.
    let mut active_mode =
        DurabilityMode::from_u8(durability_mode.load(std::sync::atomic::Ordering::Relaxed))
            .unwrap_or_else(|| {
                tracing::error!(
                    "durability_mode atomic held a corrupted byte at startup; defaulting to hybrid"
                );
                DurabilityMode::Hybrid
            });
    let mut policy = active_mode.to_policy();
    let mut ring =
        IoUring::new(RING_SIZE).expect("failed to create io_uring instance for response stage");

    // Connection table: maps connection IDs to their state.
    // HashMap for O(1) lookup. Pre-sized for a reasonable number of concurrent clients.
    let mut connections: HashMap<u64, ConnectionEntry> = HashMap::with_capacity(256);

    let mut batch = [OutputSlot::default(); MAX_BATCH];
    let mut encode_buf = [0u8; MAX_RESPONSE_BUF];

    // Cached durability position to avoid atomic reads on every slot.
    // Initialised below from the policy's startup evaluation; updated
    // via `evaluate_durability` on every gate iteration.
    let mut cached_durable_pos: u64;

    // Degradation logger. Tracks transitions, suppresses sub-second
    // flap noise, and drives the `/healthz` `policy_degraded` gauge.
    // See `DegradationLogger` for the full state machine. Initialised
    // below from the policy's startup evaluation so an unsatisfiable
    // policy (e.g. a primary that just lost both replicas while
    // running `hybrid` or `durably-replicated`) is visible immediately
    // on `/healthz` and in the journal.
    let startup_now = Instant::now();
    let mut last_policy_check = startup_now;
    /// Re-emit interval for the "still degraded" reminder.
    const DEGRADED_LOG_INTERVAL: Duration = Duration::from_secs(5);
    /// Cadence at which the idle path re-evaluates the policy. Bounds
    /// the lag between a connection-state change and the `/healthz`
    /// gauge / warn-log reflecting it. Cheap (a handful of atomic
    /// loads + the policy evaluator) at this rate.
    const POLICY_CHECK_INTERVAL: Duration = Duration::from_secs(1);

    // Initial evaluation so the cached durable position and the
    // `/healthz` gauge reflect the cluster's startup shape before
    // the first batch arrives.
    let mut degraded_logger;
    {
        let journal_pos = journal_persisted_wire_seq.load(Ordering::Acquire);
        let metrics_ref = replication_metrics.as_deref();
        let active_ref = replica_active.as_ref();
        let status = evaluate_durability(&policy, journal_pos, metrics_ref, active_ref);
        cached_durable_pos = status.durable_pos;
        utilization
            .policy_degraded
            .store(status.degraded, Ordering::Relaxed);
        degraded_logger = if status.degraded {
            DegradationLogger::new_starting_degraded(startup_now, &policy)
        } else {
            DegradationLogger::new(startup_now)
        };
    }

    // Stage histograms registered with the global registry — see
    // `melin_transport_core::trace`. The four breakdown stages
    // (journal-wait, replica-wait, encode, egress) feed the bench's
    // tick-to-trade decomposition; spsc/dispatch/server-e2e are kept
    // alongside as overall sanity checks.
    #[cfg(feature = "latency-trace")]
    let mut spsc_rec =
        trace::register_stage("response: SPSC wakeup (matching publish → response consume)");
    #[cfg(feature = "latency-trace")]
    let mut dispatch_rec = trace::register_stage("response: dispatch (consume → socket write)");
    #[cfg(feature = "latency-trace")]
    let mut server_e2e_rec = trace::register_stage("server e2e (reader recv → response flush)");
    // Tick-to-trade breakdown: per-slot wait observed for each
    // durability path (recorded only when the gate actually held us
    // up — cache-hit paths skip to avoid inflating the metric with
    // crossings that happened before we noticed). Encode is wall-time
    // around `codec::encode_response`. Egress wraps a `flush_sends`
    // call (one sample per io_uring flush, batching many slots).
    // Gated on `tick-to-trade`, not `latency-trace`, because these
    // stages roughly double the hot-path mutex traffic vs the lighter
    // 4-stage mode.
    #[cfg(feature = "tick-to-trade")]
    let mut journal_wait_rec =
        trace::register_stage("response: journal-wait (match_complete → journal cursor crossed)");
    #[cfg(feature = "tick-to-trade")]
    let mut replica_wait_rec = trace::register_stage(
        "response: replica-wait (match_complete → replication cursor crossed)",
    );
    #[cfg(feature = "tick-to-trade")]
    let mut encode_rec = trace::register_stage("response: encode (per-kind wire encoding)");
    #[cfg(feature = "tick-to-trade")]
    let mut egress_rec = trace::register_stage("response: egress (flush_sends elapsed)");

    // Track connections with buffered (unflushed) writes across batches.
    let mut dirty_connections: HashSet<u64> = HashSet::new();

    // Connections to remove after flush (send errors).
    let mut to_remove: Vec<u64> = Vec::new();

    // Pre-allocated CQE collection buffer. Must collect CQEs before
    // processing because the CQ borrow must end before mutating connections.
    // Pre-sized to RING_SIZE to avoid per-iteration heap allocation.
    let mut cqes: Vec<(u64, i32)> = Vec::with_capacity(RING_SIZE as usize);

    // Pre-encode the heartbeat response frame once. Full wire frame
    // (length prefix + tag) for direct append to send_buf.
    let heartbeat_wire_frame = {
        let mut buf = [0u8; 8];
        let written =
            codec::encode_response(&ResponseKind::Heartbeat, &mut buf).expect("heartbeat encodes");
        buf[..written].to_vec()
    };

    // Coarse timestamp for heartbeat scan — avoids Instant::now() on every spin.
    let mut last_heartbeat_scan = Instant::now();

    // Adaptive spin: spin first (fast wakeup), yield after threshold.
    let mut idle_spins: u32 = 0;

    let mut busy_count: u64 = 0;
    let mut idle_count: u64 = 0;

    loop {
        // Observe runtime mode swaps from the admin `DURABILITY`
        // command. Relaxed load (single writer is the admin handler,
        // single reader is this thread). When the byte changes,
        // rebuild the local Policy and reset the cached durable
        // position so the next gate evaluation starts from a clean
        // slate under the new shape; log the transition for the audit
        // trail. An unknown byte is treated as memory corruption: we
        // log and keep the prior mode rather than silently downgrading.
        let observed_byte = durability_mode.load(Ordering::Relaxed);
        if observed_byte != active_mode.as_u8() {
            match DurabilityMode::from_u8(observed_byte) {
                Some(next) => {
                    tracing::info!(
                        prev = active_mode.as_str(),
                        next = next.as_str(),
                        "durability mode swapped at runtime"
                    );
                    active_mode = next;
                    policy = active_mode.to_policy();
                    // The fresh policy may evaluate degraded/undegraded
                    // differently against the same cluster shape; let
                    // the next gate evaluation re-derive.
                    cached_durable_pos = 0;
                    // Re-seed the degradation logger so a transition
                    // out of (or into) degraded under the new policy
                    // surfaces immediately rather than waiting for the
                    // sustained-state hold to roll over.
                    degraded_logger = DegradationLogger::new(Instant::now());
                }
                None => {
                    tracing::error!(
                        byte = observed_byte,
                        "durability_mode atomic held a corrupted byte; retaining prior mode"
                    );
                }
            }
        }

        if shutdown.load(Ordering::Relaxed) {
            // Best-effort flush before shutdown.
            if !dirty_connections.is_empty() {
                flush_sends(
                    &mut ring,
                    &mut connections,
                    &dirty_connections,
                    &mut to_remove,
                    &mut cqes,
                );
                dirty_connections.clear();
            }
            utilization.busy.store(busy_count, Ordering::Relaxed);
            utilization.idle.store(idle_count, Ordering::Relaxed);
            #[cfg(feature = "pipeline-stats")]
            print_utilization("response", busy_count, idle_count);
            return;
        }

        // Poll control channel (non-blocking) for connect/disconnect.
        while let Ok(event) = control_rx.try_recv() {
            match event {
                ControlEvent::Connected {
                    connection_id,
                    fd,
                    writer,
                } => {
                    // The writer keeps the fd alive — store it as the owner.
                    let owner: Box<dyn Send> = Box::new(writer);
                    connections.insert(
                        connection_id,
                        ConnectionEntry {
                            fd,
                            _owner: owner,
                            send_buf: Vec::with_capacity(4096),
                            last_send: Instant::now(),
                        },
                    );
                }
                ControlEvent::Disconnected { connection_id } => {
                    connections.remove(&connection_id);
                    dirty_connections.remove(&connection_id);
                }
            }
        }

        // Consume output slots from matching stage.
        let count = consumer.consume_batch(&mut batch, MAX_BATCH);
        if count == 0 {
            // SPSC is empty — flush all dirty connections via io_uring.
            // This is the response-data egress path; heartbeat flushes
            // below aren't sampled because they're admin traffic, not
            // on the client RTT path.
            if !dirty_connections.is_empty() {
                #[cfg(feature = "tick-to-trade")]
                let egress_start = trace::mono_trace_ns();
                flush_sends(
                    &mut ring,
                    &mut connections,
                    &dirty_connections,
                    &mut to_remove,
                    &mut cqes,
                );
                #[cfg(feature = "tick-to-trade")]
                egress_rec.record_elapsed(egress_start, trace::mono_trace_ns());
                for conn_id in to_remove.drain(..) {
                    connections.remove(&conn_id);
                }
                dirty_connections.clear();
            }

            // Send heartbeats to idle connections. Only checked during
            // idle periods (SPSC empty) to avoid overhead on the hot path.
            if let Some(interval) = heartbeat_interval {
                let now = Instant::now();
                // Coarse gate: only scan at most once per second.
                if now.duration_since(last_heartbeat_scan) >= Duration::from_secs(1) {
                    last_heartbeat_scan = now;
                    for (&conn_id, entry) in connections.iter_mut() {
                        if now.duration_since(entry.last_send) >= interval {
                            entry.send_buf.extend_from_slice(&heartbeat_wire_frame);
                            dirty_connections.insert(conn_id);
                            entry.last_send = now;
                        }
                    }
                    // Flush the heartbeat sends immediately.
                    if !dirty_connections.is_empty() {
                        flush_sends(
                            &mut ring,
                            &mut connections,
                            &dirty_connections,
                            &mut to_remove,
                            &mut cqes,
                        );
                        for conn_id in to_remove.drain(..) {
                            connections.remove(&conn_id);
                        }
                        dirty_connections.clear();
                    }
                }
            }

            // Re-evaluate the durability policy on a slow timer so the
            // `policy_degraded` flag and the periodic warn track the
            // cluster's real state even on idle / quiet venues. The
            // gate-open block also calls `update_degraded_state` after
            // each consumed batch; this is the equivalent for the
            // no-batch path.
            {
                let now_ts = Instant::now();
                if now_ts.duration_since(last_policy_check) >= POLICY_CHECK_INTERVAL {
                    last_policy_check = now_ts;
                    let journal_pos = journal_persisted_wire_seq.load(Ordering::Acquire);
                    let metrics_ref = replication_metrics.as_deref();
                    let active_ref = replica_active.as_ref();
                    let status = evaluate_durability(&policy, journal_pos, metrics_ref, active_ref);
                    degraded_logger.tick(
                        &policy,
                        &utilization,
                        status.degraded,
                        now_ts,
                        DEGRADED_LOG_INTERVAL,
                    );
                    // Cache the position so the next batch's gate sees a
                    // fresh value rather than spinning from a stale cache.
                    cached_durable_pos = status.durable_pos;
                }
            }

            idle_count += 1;
            if idle_count.is_multiple_of(1024) {
                utilization.busy.store(busy_count, Ordering::Relaxed);
                utilization.idle.store(idle_count, Ordering::Relaxed);
            }
            if busy_spin || idle_spins < 1000 {
                idle_spins = idle_spins.wrapping_add(1);
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
            }
            continue;
        }
        idle_spins = 0;
        busy_count += 1;

        #[cfg(feature = "latency-trace")]
        let consume_ts = trace::mono_trace_ns();

        // Wait for durability confirmation before sending responses.
        //
        // Each iteration: read the primary journal cursor + per-slot
        // replica cursors (both in-memory and persisted), build a
        // `CursorView`, and evaluate the configured policy. Spin until
        // the durable position catches up to the batch's max input_seq.
        //
        // Per-slot journal-wait / replica-wait tracker. See
        // `GateCrossTracker` for the rationale (only records cursors
        // that were actually on the critical path). Attribution uses
        // `repl_min` = min of connected-replica persisted cursors so
        // operators see "which subsystem to optimize" the same way as
        // before the policy refactor.
        #[cfg(feature = "tick-to-trade")]
        let mut gate_tracker;
        {
            // Gate on `wire_seq`, not `input_seq`. `input_seq` is in
            // local-consumer space (the matching cursor on the input
            // ring, starts at 0 in this process) while replica metrics
            // and the primary's `journal_persisted_wire_seq` live in
            // wire-seq space (allocated by the journal stage starting
            // at `starting_sequence`). A `needed` derived from
            // `input_seq` and compared against wire-seq cursors only
            // works when `starting_sequence == 1`; a recovered primary
            // (or any process whose journal already has prior content
            // pushing `starting_sequence` above 1) would silently open
            // the gate ahead of the replica's actual replicated state.
            //
            // Every cursor in the policy view (`journal_persisted_wire_seq`,
            // `metrics.in_memory_sequence`, `metrics.acked_sequence`)
            // carries "highest wire seq known to be in that state on
            // node X". A batch's `needed` is therefore the *exact*
            // wire seq the gate must see — not `+1` — for the latest
            // event in the batch to be considered durable. The legacy
            // `+1` was load-bearing only because `input_seq` was off
            // by `starting_sequence - 1` from wire seq; with the
            // wire-seq stamp it would over-shoot by one event and
            // make the gate stall an extra round-trip per response.
            let needed = batch[..count]
                .iter()
                .map(|s| s.wire_seq)
                .max()
                .expect("non-empty batch");
            #[cfg(feature = "tick-to-trade")]
            {
                gate_tracker = GateCrossTracker::new(needed);
            }
            // Durability-gate carve-out for halt-state output. Slots
            // tagged `durability_bypass = true` at emission carry no
            // engine state worth replicating before delivery — see
            // `OutputSlot::durability_bypass` for the correctness
            // argument. When every slot in the batch carries the flag
            // the gate is skipped entirely, so clients receive the halt
            // reason immediately rather than blocking on a structurally
            // unsatisfiable policy (e.g. `Hybrid` with all replicas
            // disconnected, which would otherwise stall the gate until
            // peers return). If even one normal slot is present, gate
            // the whole batch as usual — the bypass slots ride along
            // behind the gated one, which is safe (no ordering
            // inversion vs. a strict-gate world).
            let needs_gate = batch[..count].iter().any(|s| !s.durability_bypass);
            if needs_gate && cached_durable_pos < needed {
                loop {
                    // Inside the gate-wait spin loop, also observe a
                    // mode swap. Without this, a batch whose gate
                    // becomes structurally unsatisfiable (e.g. all
                    // replicas die while a non-bypass slot is in
                    // flight under `Hybrid`) would wedge the response
                    // stage forever, even if an operator sends the
                    // remediating `DURABILITY local` — the outer loop
                    // observation never gets a chance to run. The
                    // relaxed load is ~1 cycle on x86; cheaper than
                    // the `spin_loop` hint below.
                    let observed_byte = durability_mode.load(Ordering::Relaxed);
                    if observed_byte != active_mode.as_u8()
                        && let Some(next) = DurabilityMode::from_u8(observed_byte)
                    {
                        tracing::info!(
                            prev = active_mode.as_str(),
                            next = next.as_str(),
                            "durability mode swapped during gate wait"
                        );
                        active_mode = next;
                        policy = active_mode.to_policy();
                        degraded_logger = DegradationLogger::new(Instant::now());
                    }

                    let journal_pos = journal_persisted_wire_seq.load(Ordering::Acquire);
                    let metrics_ref = replication_metrics.as_deref();
                    let active_ref = replica_active.as_ref();
                    let repl_min = connected_persisted_min(metrics_ref, active_ref);

                    #[cfg(feature = "tick-to-trade")]
                    gate_tracker.observe(journal_pos, repl_min, trace::mono_trace_ns());

                    let status = evaluate_durability(&policy, journal_pos, metrics_ref, active_ref);
                    cached_durable_pos = status.durable_pos;
                    utilization
                        .policy_degraded
                        .store(status.degraded, Ordering::Relaxed);

                    if cached_durable_pos >= needed {
                        // Attribution: which subsystem was slowest at
                        // the moment the gate opened. Relaxed is fine —
                        // health reads are infrequent.
                        if journal_pos <= repl_min {
                            utilization.gate_journal.fetch_add(1, Ordering::Relaxed);
                        } else {
                            utilization.gate_replication.fetch_add(1, Ordering::Relaxed);
                        }
                        break;
                    }
                    std::hint::spin_loop();
                }
            }
        }

        let batch_now = Instant::now();

        // Log degradation transitions / re-emit the reminder. Same
        // logger the idle path uses; transitions are gated on a
        // sustained-state hold so sub-second flap doesn't spam.
        let degraded_now = utilization.policy_degraded.load(Ordering::Relaxed);
        degraded_logger.tick(
            &policy,
            &utilization,
            degraded_now,
            batch_now,
            DEGRADED_LOG_INTERVAL,
        );
        // Bump the idle-path's check timestamp so we don't double-
        // tick the logger when traffic stops.
        last_policy_check = batch_now;

        for slot in &batch[..count] {
            #[cfg(feature = "latency-trace")]
            spsc_rec.record_elapsed(slot.match_complete_ts, consume_ts);

            // Per-slot durability-gate breakdown. Recorded only when
            // the gate actually held us up (the tracker captured a
            // cross). Note: the cross timestamp is for the *batch's*
            // `needed` — for slots earlier in the batch, the cursor
            // may have crossed their individual `input_seq+1` earlier,
            // so this systematically overestimates wait for non-last
            // slots by up to the batch's matching span. Acceptable
            // noise for the operator-facing breakdown; documented in
            // `docs/benchmarking.md`.
            #[cfg(feature = "tick-to-trade")]
            if let Some(ts) = gate_tracker.journal_crossed() {
                journal_wait_rec.record_elapsed(slot.match_complete_ts, ts);
            }
            #[cfg(feature = "tick-to-trade")]
            if let Some(ts) = gate_tracker.replica_crossed() {
                replica_wait_rec.record_elapsed(slot.match_complete_ts, ts);
            }

            // Each slot expands to at most two wire frames: the payload
            // (Report / QueryResponse / EngineError) and an optional
            // trailing `BatchEnd` when `is_last_in_request` is set.
            // `OutputPayload::BatchEnd` carries no payload of its own —
            // the wire BatchEnd is emitted purely from the flag.
            let mut kinds: [ResponseKind; 2] = [ResponseKind::BatchEnd; 2];
            let mut kinds_len: usize = 0;
            match slot.payload {
                OutputPayload::QueryResponse(QueryResponse::Stats {
                    active_connections,
                    events_processed,
                    journal_sequence,
                }) => {
                    kinds[kinds_len] = ResponseKind::StatsHeader {
                        active_connections,
                        events_processed,
                        journal_sequence,
                    };
                    kinds_len += 1;
                }
                OutputPayload::QueryResponse(QueryResponse::Position {
                    account,
                    balances,
                    count,
                }) => {
                    kinds[kinds_len] = ResponseKind::PositionSnapshot {
                        account,
                        balances,
                        count,
                    };
                    kinds_len += 1;
                }
                OutputPayload::QueryResponse(QueryResponse::RequestSeqHwm { hwm }) => {
                    kinds[kinds_len] = ResponseKind::RequestSeqHwm { hwm };
                    kinds_len += 1;
                }
                OutputPayload::Report(report) => {
                    kinds[kinds_len] = ResponseKind::Report(report);
                    kinds_len += 1;
                }
                OutputPayload::BatchEnd => {
                    // No payload — terminator only. is_last_in_request
                    // is always set on BatchEnd-payload slots.
                }
                OutputPayload::EngineError => {
                    kinds[kinds_len] = ResponseKind::EngineError;
                    kinds_len += 1;
                }
            }
            if slot.is_last_in_request {
                kinds[kinds_len] = ResponseKind::BatchEnd;
                kinds_len += 1;
            }

            if let Some(entry) = connections.get_mut(&slot.connection_id) {
                for kind in &kinds[..kinds_len] {
                    // Encode the response (includes 4-byte length prefix).
                    #[cfg(feature = "tick-to-trade")]
                    let encode_start = trace::mono_trace_ns();
                    let written = match codec::encode_response(kind, &mut encode_buf) {
                        Ok(n) => n,
                        Err(e) => {
                            tracing::error!(
                                connection_id = slot.connection_id,
                                error = %e,
                                "encode error"
                            );
                            continue;
                        }
                    };
                    #[cfg(feature = "tick-to-trade")]
                    encode_rec.record_elapsed(encode_start, trace::mono_trace_ns());

                    // Drop slow clients whose send buffer has grown too large.
                    // This prevents unbounded memory growth from a single laggy
                    // connection causing allocator pressure and tail latency spikes.
                    if entry.send_buf.len() + written > MAX_SEND_BUF {
                        debug!(
                            connection_id = slot.connection_id,
                            send_buf_len = entry.send_buf.len(),
                            "send buffer exceeded limit, dropping connection"
                        );
                        to_remove.push(slot.connection_id);
                        break;
                    }

                    // Append the full wire frame to the connection's send buffer.
                    // encode_response writes [length(4) | payload], which is the
                    // complete wire format — no extra framing needed.
                    entry.send_buf.extend_from_slice(&encode_buf[..written]);
                    entry.last_send = batch_now;
                    dirty_connections.insert(slot.connection_id);

                    // Record server-side end-to-end: reader recv → response flush.
                    #[cfg(feature = "latency-trace")]
                    if matches!(kind, ResponseKind::BatchEnd) {
                        server_e2e_rec.record_elapsed(slot.recv_ts, trace::mono_trace_ns());
                    }
                }
            }
        }

        // Remove connections that exceeded the send buffer limit.
        for conn_id in to_remove.drain(..) {
            connections.remove(&conn_id);
            dirty_connections.remove(&conn_id);
        }

        #[cfg(feature = "latency-trace")]
        dispatch_rec.record_elapsed(consume_ts, trace::mono_trace_ns());
    }
}

/// Submit io_uring SEND SQEs for all dirty connections and wait for completions.
///
/// Each dirty connection's accumulated send buffer is sent in a single SEND
/// operation. Partial sends are retried until all bytes are delivered.
/// Failed connections are collected in `to_remove` for the caller to clean up.
fn flush_sends(
    ring: &mut IoUring,
    connections: &mut HashMap<u64, ConnectionEntry>,
    dirty: &HashSet<u64>,
    to_remove: &mut Vec<u64>,
    cqes: &mut Vec<(u64, i32)>,
) {
    // Submit SEND SQEs for all dirty connections.
    let mut pending: usize = 0;
    for &conn_id in dirty {
        if let Some(entry) = connections.get(&conn_id) {
            if entry.send_buf.is_empty() {
                continue;
            }
            let sqe = opcode::Send::new(
                types::Fd(entry.fd),
                entry.send_buf.as_ptr(),
                entry.send_buf.len() as u32,
            )
            .build()
            .user_data(conn_id);

            unsafe {
                ring.submission()
                    .push(&sqe)
                    .expect("io_uring SQ full — increase RING_SIZE");
            }
            pending += 1;
        }
    }

    if pending == 0 {
        return;
    }

    // Submit and wait for all completions.
    if let Err(e) = ring.submit_and_wait(pending) {
        error!(error = %e, "io_uring submit_and_wait failed in response stage");
        return;
    }

    // Drain completions into pre-allocated buffer. Must collect to
    // release CQ borrow before mutating connections.
    cqes.clear();
    cqes.extend(ring.completion().map(|cqe| (cqe.user_data(), cqe.result())));

    for &(conn_id, result) in cqes.iter() {
        if result < 0 {
            debug!(
                connection_id = conn_id,
                error = result,
                "send error, dropping connection"
            );
            to_remove.push(conn_id);
            continue;
        }

        let sent = result as usize;
        if let Some(entry) = connections.get_mut(&conn_id) {
            if sent >= entry.send_buf.len() {
                entry.send_buf.clear();
            } else {
                // Partial send — drain sent bytes, retry remainder.
                // Rare for small response frames over TCP/UDS but must
                // be handled for correctness (e.g., send buffer pressure).
                entry.send_buf.drain(..sent);
                retry_send(ring, entry, conn_id, to_remove);
            }
        }
    }
}

/// Retry sending remaining bytes after a partial send. Loops until the
/// entire buffer is delivered or an error occurs.
fn retry_send(
    ring: &mut IoUring,
    entry: &mut ConnectionEntry,
    conn_id: u64,
    to_remove: &mut Vec<u64>,
) {
    while !entry.send_buf.is_empty() {
        let sqe = opcode::Send::new(
            types::Fd(entry.fd),
            entry.send_buf.as_ptr(),
            entry.send_buf.len() as u32,
        )
        .build()
        .user_data(conn_id);

        unsafe {
            ring.submission()
                .push(&sqe)
                .expect("io_uring SQ full during send retry");
        }

        if let Err(e) = ring.submit_and_wait(1) {
            debug!(connection_id = conn_id, error = %e, "send retry failed");
            to_remove.push(conn_id);
            return;
        }

        if let Some(cqe) = ring.completion().next() {
            let result = cqe.result();
            if result <= 0 {
                debug!(
                    connection_id = conn_id,
                    error = result,
                    "send retry error, dropping connection"
                );
                to_remove.push(conn_id);
                return;
            }
            let sent = result as usize;
            if sent >= entry.send_buf.len() {
                entry.send_buf.clear();
            } else {
                entry.send_buf.drain(..sent);
            }
        }
    }
}

/// Evaluate the durability policy against the live cursor state.
///
/// Builds a `CursorView` containing the primary plus every *currently
/// connected* replica slot and returns the highest sequence at which
/// the policy is satisfied. The primary's in-memory cursor is modeled
/// as `u64::MAX` because the response stage only gates events that have
/// already been processed by the matching engine — those are trivially
/// in-memory on the primary by construction.
///
/// Disconnected slots are *omitted from the view* rather than included
/// with zero cursors. The view's `len()` reflects how many nodes are
/// actually available; if it's too small to satisfy a clause, the
/// policy reports degraded and the gate stalls.
#[inline]
pub(crate) fn evaluate_durability(
    policy: &Policy,
    journal_pos: u64,
    metrics: Option<&ReplicationMetrics>,
    replica_active: Option<&[Arc<AtomicBool>; 2]>,
) -> EvalStatus {
    // Primary + up to 2 replica slots = 3 nodes max.
    let mut nodes: [[u64; 2]; 3] = [[0, 0]; 3];
    nodes[0] = [u64::MAX, journal_pos];
    let mut len = 1;
    if let (Some(m), Some(active)) = (metrics, replica_active) {
        for (i, slot_active) in active.iter().enumerate() {
            // Skip inactive slots up-front.
            if !slot_active.load(Ordering::Acquire) {
                continue;
            }
            let in_mem = m.in_memory_sequence[i].load(Ordering::Acquire);
            let persisted = m.acked_sequence[i].load(Ordering::Acquire);
            nodes[len] = [in_mem, persisted];
            len += 1;
        }
    }
    policy.evaluate_with_status(&CursorView::new(&nodes[..len]))
}

/// Hold-time before a state transition is committed to the log.
/// Suppresses log spam when a replica flaps faster than this — only
/// transitions that hold for at least this long emit warn/info
/// entries. The `/healthz` gauge updates immediately regardless,
/// so dashboards and alerts still see real-time state.
const DEGRADED_FLAP_HOLD: Duration = Duration::from_secs(1);

/// Tracks degradation state across calls and emits warn/info logs
/// with sustained-state gating + a periodic heartbeat re-emit.
///
/// The hot path calls [`Self::tick`] every gate iteration / idle
/// poll with the current `degraded` value and the wall clock. The
/// logger handles:
///
/// - Updating the `policy_degraded` health gauge immediately.
/// - Suppressing log lines for transitions that don't hold for at
///   least [`DEGRADED_FLAP_HOLD`] — a replica flapping at sub-second
///   cadence produces no log noise, only a quietly-updating gauge.
/// - Emitting a warn at the moment a sustained degraded state
///   crosses the hold threshold, plus a periodic re-emit every
///   `heartbeat_interval` while it persists.
/// - Emitting an info when a sustained healthy state crosses the
///   hold threshold (the cluster is back to its target shape and
///   stayed there long enough that we trust the recovery).
pub(crate) struct DegradationLogger {
    /// Last value passed to `tick`; what we'd log about if it stayed
    /// at this value past the hold threshold.
    pending_state: bool,
    /// When `pending_state` first appeared. Reset on every flip.
    pending_since: Instant,
    /// Whether the current pending state has been logged yet. Only
    /// the *first* log per sustained streak crosses; subsequent
    /// re-emits while degraded are heartbeat warns.
    pending_logged: bool,
    /// When the last warn fired. Drives the periodic re-emit while
    /// degraded.
    last_log: Option<Instant>,
}

impl DegradationLogger {
    pub(crate) fn new(now: Instant) -> Self {
        Self {
            pending_state: false,
            pending_since: now,
            pending_logged: true, // healthy is the assumed initial state; nothing to log
            last_log: None,
        }
    }

    /// Use when the policy is known to start in a degraded state
    /// (e.g. a primary in `hybrid` mode with no replica yet
    /// connected). Logs a startup warn immediately and treats the
    /// state as already-logged so the next tick doesn't re-emit.
    pub(crate) fn new_starting_degraded(now: Instant, policy: &Policy) -> Self {
        tracing::warn!(
            policy = %policy,
            "durability policy starts in degraded mode — fewer connected nodes than the target count"
        );
        Self {
            pending_state: true,
            pending_since: now,
            pending_logged: true,
            last_log: Some(now),
        }
    }

    /// Update the gauge + emit transition/heartbeat logs as needed.
    /// Cheap on the hot path: one atomic store, a few branches, one
    /// `Instant::duration_since`.
    pub(crate) fn tick(
        &mut self,
        policy: &Policy,
        utilization: &StageUtilization,
        degraded_now: bool,
        now: Instant,
        heartbeat_interval: Duration,
    ) {
        utilization
            .policy_degraded
            .store(degraded_now, Ordering::Relaxed);

        if degraded_now != self.pending_state {
            // State changed — start a new hold window. Don't log
            // until / unless this new state stays long enough.
            self.pending_state = degraded_now;
            self.pending_since = now;
            self.pending_logged = false;
            return;
        }

        // State held. If we haven't yet logged this streak's onset,
        // and it's been pending for at least the flap-hold time,
        // emit the transition message and mark logged.
        if !self.pending_logged && now.duration_since(self.pending_since) >= DEGRADED_FLAP_HOLD {
            if degraded_now {
                tracing::warn!(
                    policy = %policy,
                    "durability policy operating in degraded mode — fewer connected nodes than the target count, gate clamped to surviving cluster"
                );
            } else {
                tracing::info!(
                    policy = %policy,
                    "durability policy returned to target shape"
                );
            }
            self.pending_logged = true;
            self.last_log = Some(now);
            return;
        }

        // Heartbeat re-emit while a degraded state persists.
        if degraded_now
            && self.pending_logged
            && self
                .last_log
                .is_none_or(|t| now.duration_since(t) >= heartbeat_interval)
        {
            tracing::warn!(
                policy = %policy,
                "durability policy still degraded — fewer connected nodes than the target count"
            );
            self.last_log = Some(now);
        }
    }
}

/// Minimum persisted cursor across currently-connected replica slots.
/// Used for gate-bottleneck attribution and the journal-wait /
/// replica-wait histograms — *not* for durability decisions, which go
/// through [`evaluate_durability`].
///
/// Returns `u64::MAX` when no replica is connected, which makes
/// attribution always credit the journal — correct, because in
/// standalone mode the journal is the only path.
#[inline]
pub(crate) fn connected_persisted_min(
    metrics: Option<&ReplicationMetrics>,
    replica_active: Option<&[Arc<AtomicBool>; 2]>,
) -> u64 {
    let (Some(m), Some(active)) = (metrics, replica_active) else {
        return u64::MAX;
    };
    let mut min = u64::MAX;
    for (i, slot_active) in active.iter().enumerate() {
        if !slot_active.load(Ordering::Acquire) {
            continue;
        }
        let v = m.acked_sequence[i].load(Ordering::Acquire);
        if v < min {
            min = v;
        }
    }
    min
}

/// Tracks per-cursor "first observed transition from below to >= needed"
/// inside the durability gate loop, to drive the journal-wait /
/// replica-wait histograms in the bench's tick-to-trade decomposition.
///
/// A sample is recorded only for cursors that were strictly below
/// `needed` at the loop's first observation. Cursors already past at
/// entry were not on the critical path for this batch, so attributing
/// "wait time" to them would inflate the metric with cursor-poll
/// observation timestamps that have nothing to do with how long the
/// stage actually held us up.
///
/// `now_ns` is taken as a parameter rather than read internally so
/// tests can supply deterministic timestamps. The caller's hot path
/// reads `trace::mono_trace_ns()` once per gate iteration and feeds it in.
#[cfg(feature = "tick-to-trade")]
pub(crate) struct GateCrossTracker {
    needed: u64,
    journal_crossed_ts: Option<trace::MonoTraceInstant>,
    replica_crossed_ts: Option<trace::MonoTraceInstant>,
    journal_was_below: bool,
    replica_was_below: bool,
    first: bool,
}

#[cfg(feature = "tick-to-trade")]
impl GateCrossTracker {
    pub(crate) fn new(needed: u64) -> Self {
        Self {
            needed,
            journal_crossed_ts: None,
            replica_crossed_ts: None,
            journal_was_below: false,
            replica_was_below: false,
            first: true,
        }
    }

    pub(crate) fn observe(
        &mut self,
        journal_pos: u64,
        repl_min: u64,
        now_ns: trace::MonoTraceInstant,
    ) {
        if self.first {
            self.journal_was_below = journal_pos < self.needed;
            self.replica_was_below = repl_min < self.needed;
            self.first = false;
        }
        if self.journal_was_below && self.journal_crossed_ts.is_none() && journal_pos >= self.needed
        {
            self.journal_crossed_ts = Some(now_ns);
        }
        if self.replica_was_below && self.replica_crossed_ts.is_none() && repl_min >= self.needed {
            self.replica_crossed_ts = Some(now_ns);
        }
    }

    pub(crate) fn journal_crossed(&self) -> Option<trace::MonoTraceInstant> {
        self.journal_crossed_ts
    }

    pub(crate) fn replica_crossed(&self) -> Option<trace::MonoTraceInstant> {
        self.replica_crossed_ts
    }
}

/// Print busy/idle utilization for a pipeline stage on shutdown.
#[cfg(feature = "pipeline-stats")]
fn print_utilization(stage: &str, busy: u64, idle: u64) {
    let total = busy + idle;
    if total == 0 {
        tracing::info!(stage, "no iterations recorded");
        return;
    }
    let pct = (busy as f64 / total as f64) * 100.0;
    tracing::info!(
        stage,
        pct_busy = format_args!("{pct:.2}%"),
        busy,
        idle,
        total,
        "pipeline utilization",
    );
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "tick-to-trade")]
    use super::GateCrossTracker;
    use super::{DegradationLogger, connected_persisted_min, evaluate_durability};
    use crate::runtime::durability_policy::{Clause, Level, Policy};
    use crate::runtime::replication::ReplicationMetrics;

    /// Build a [`Policy`] from a mini DSL: one or more
    /// `"<level>>=<count>"` clauses joined with `&&`. Test-only
    /// ergonomics — production builds policies via
    /// [`DurabilityMode::to_policy`].
    fn parse(s: &str) -> Result<Policy, String> {
        let mut clauses = Vec::new();
        for raw in s.split("&&") {
            let token = raw.trim();
            let (lvl, rhs) = token
                .split_once(">=")
                .ok_or_else(|| format!("clause `{token}` missing `>=`"))?;
            let level = match lvl.trim() {
                "persisted" => Level::Persisted,
                "in_memory" => Level::InMemory,
                other => return Err(format!("unknown level `{other}`")),
            };
            let count: u8 = rhs.trim().parse().map_err(|e| format!("bad count: {e}"))?;
            clauses.push(Clause { count, level });
        }
        Policy::new(clauses).map_err(|e| e.to_string())
    }
    use melin_transport_core::pipeline::StageUtilization;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    /// Build a `ReplicationMetrics` with both slots populated. Tests
    /// that need to simulate a disconnected slot use [`flags`] to mark
    /// it inactive — its cursors are then ignored regardless of value.
    fn metrics(slot0: (u64, u64), slot1: (u64, u64)) -> Arc<ReplicationMetrics> {
        let m = Arc::new(ReplicationMetrics::default());
        m.in_memory_sequence[0].store(slot0.0, Ordering::Relaxed);
        m.acked_sequence[0].store(slot0.1, Ordering::Relaxed);
        m.in_memory_sequence[1].store(slot1.0, Ordering::Relaxed);
        m.acked_sequence[1].store(slot1.1, Ordering::Relaxed);
        m
    }

    /// Build a `[active; 2]` flags array.
    fn flags(slot0_active: bool, slot1_active: bool) -> [Arc<AtomicBool>; 2] {
        [
            Arc::new(AtomicBool::new(slot0_active)),
            Arc::new(AtomicBool::new(slot1_active)),
        ]
    }

    /// Both replicas active — the common healthy-cluster case.
    fn both_active() -> [Arc<AtomicBool>; 2] {
        flags(true, true)
    }

    // --- Standalone (no replicas wired) ---

    #[test]
    fn standalone_persisted_one_gates_on_journal() {
        // No metrics → only the primary is in the view. `persisted>=1`
        // is satisfied by the primary alone at journal_pos.
        let p = parse("persisted>=1").unwrap();
        assert_eq!(evaluate_durability(&p, 500, None, None).durable_pos, 500);
    }

    #[test]
    fn standalone_strict_persisted_two_never_opens() {
        // `persisted>=2` on a standalone primary stays at 0: the
        // operator asked for two copies and there is only one. The
        // policy surfaces as degraded so the operator sees the gate
        // is stalled because the cluster can't meet the policy.
        let p = parse("persisted>=2").unwrap();
        let r = evaluate_durability(&p, 500, None, None);
        assert_eq!(r.durable_pos, 0);
        assert!(
            r.degraded,
            "policy structurally unsatisfiable on this shape → degraded",
        );
    }

    // --- 2 replicas connected ---

    #[test]
    fn quorum_both_replicas_ahead_of_journal() {
        // Both replicas persisted past journal. `persisted>=2` returns
        // the 2nd-largest persisted across {primary, slot0, slot1}.
        let p = parse("persisted>=2").unwrap();
        let m = metrics((100, 100), (120, 120));
        let a = both_active();
        assert_eq!(
            evaluate_durability(&p, 50, Some(&m), Some(&a)).durable_pos,
            100
        );
    }

    #[test]
    fn quorum_journal_ahead_of_both_replicas() {
        // Journal at 500, replicas at 100/120. 2nd-largest persisted = 120.
        let p = parse("persisted>=2").unwrap();
        let m = metrics((100, 100), (120, 120));
        let a = both_active();
        assert_eq!(
            evaluate_durability(&p, 500, Some(&m), Some(&a)).durable_pos,
            120
        );
    }

    #[test]
    fn quorum_journal_between_slow_and_fast_replica() {
        // {primary=150, slot0_persisted=50, slot1_persisted=200}.
        // 2nd-largest = 150 (primary itself).
        let p = parse("persisted>=2").unwrap();
        let m = metrics((50, 50), (200, 200));
        let a = both_active();
        assert_eq!(
            evaluate_durability(&p, 150, Some(&m), Some(&a)).durable_pos,
            150
        );
    }

    // --- Single replica connected ---

    #[test]
    fn single_replica_strict_persisted_two_requires_both_survivors() {
        // Slot 0 connected, slot 1 disconnected. View = {primary, slot0}.
        // Strict `persisted>=2`: 2nd-largest of the 2-row view =
        // min(primary, slot0). Strictly stronger than legacy auto-
        // degrade-to-1-node in the same shape.
        let p = parse("persisted>=2").unwrap();
        let m = metrics((100, 100), (999, 999)); // slot 1 cursors ignored
        let a = flags(true, false);
        assert_eq!(
            evaluate_durability(&p, 50, Some(&m), Some(&a)).durable_pos,
            50
        );
        assert_eq!(
            evaluate_durability(&p, 200, Some(&m), Some(&a)).durable_pos,
            100
        );
    }

    #[test]
    fn single_replica_persisted_two_requires_both_survivors() {
        // 2-node view (primary + surviving replica). `persisted>=2` is
        // satisfiable; the gate opens at the slower of the two and the
        // policy is not degraded.
        let p = parse("persisted>=2").unwrap();
        let m = metrics((100, 100), (999, 999));
        let a = flags(true, false);
        let r = evaluate_durability(&p, 50, Some(&m), Some(&a));
        assert_eq!(r.durable_pos, 50);
        assert!(!r.degraded);
    }

    #[test]
    fn both_replicas_disconnected_strict_stalls() {
        // View has only the primary. Strict `persisted>=2` cannot be
        // satisfied — operator opted out of degrade.
        let p = parse("persisted>=2").unwrap();
        let m = metrics((999, 999), (999, 999));
        let a = flags(false, false);
        assert_eq!(
            evaluate_durability(&p, 500, Some(&m), Some(&a)).durable_pos,
            0
        );
    }

    #[test]
    fn both_replicas_disconnected_strict_stalls_and_flags_degraded() {
        // With `persisted>=2` and both replicas down, the cursor view
        // collapses to {primary}: the clause's count (=2) exceeds the
        // view size, so the gate stays at 0 and the policy flags
        // degraded. Note the matching stage's separate halt at
        // `replicas_connected==0` rejects new orders before they reach
        // the gate; this verifies the gate semantics in isolation.
        let p = parse("persisted>=2").unwrap();
        let m = metrics((999, 999), (999, 999));
        let a = flags(false, false);
        let r = evaluate_durability(&p, 500, Some(&m), Some(&a));
        assert_eq!(r.durable_pos, 0);
        assert!(r.degraded);
    }

    // --- Mixed-level policies ---

    #[test]
    fn persisted_one_and_in_memory_two() {
        // "Leader persists, plus one other node has it in memory" —
        // the cheap-but-non-zero durability target. Slot 0 has it in
        // memory, slot 1 disconnected.
        let p = parse("persisted>=1 && in_memory>=2").unwrap();
        // primary persisted=50, slot0 in_mem=80 / persisted=20.
        // persisted>=1: max(50, 20, 0) = 50.
        // in_memory>=2: primary in_mem=u64::MAX (always), slot0_eff=max(80, 20)=80,
        //               slot1=0. 2nd-largest = 80.
        // min(50, 80) = 50.
        let m = metrics((80, 20), (999, 999));
        let a = flags(true, false);
        assert_eq!(
            evaluate_durability(&p, 50, Some(&m), Some(&a)).durable_pos,
            50
        );
    }

    // --- Edge: journal at 0 ---

    #[test]
    fn journal_at_zero_with_replicas_persisted_one() {
        // Journal hasn't fsynced anything; both replicas have. With
        // `persisted>=1` the gate opens at the fastest replica.
        let p = parse("persisted>=1").unwrap();
        let m = metrics((100, 100), (200, 200));
        let a = both_active();
        assert_eq!(
            evaluate_durability(&p, 0, Some(&m), Some(&a)).durable_pos,
            200
        );
    }

    // --- connected_persisted_min — used for gate-bottleneck attribution ---

    #[test]
    fn attribution_min_skips_disconnected_slots() {
        // Slot 1 disconnected via active flag.
        let m = metrics((150, 100), (999, 999));
        let a = flags(true, false);
        assert_eq!(connected_persisted_min(Some(&m), Some(&a)), 100);
    }

    #[test]
    fn attribution_min_returns_max_when_standalone() {
        // No metrics wired → u64::MAX, which makes attribution always
        // credit the journal. Correct for a standalone deployment.
        assert_eq!(connected_persisted_min(None, None), u64::MAX);
    }

    /// Fresh-cluster catch-up: a replica that handshakes at sequence
    /// 0 (the legitimate genesis case, not a stale-flag race) must be
    /// included in the cursor view with its zero cursors so the policy
    /// behaves the same way it would for a 1-replica deployment that
    /// has just produced its first batch. The disconnect-race
    /// mitigations (B1 seed-on-connect + B2 reorder) keep this from
    /// being conflated with the stale-flag-paired-with-zero-cursor
    /// case under normal cluster lifecycles.
    #[test]
    fn fresh_cluster_zero_cursors_included_in_view() {
        let p = parse("persisted>=2").unwrap();
        // Both replicas just handshook at seq 0, primary also at 0
        // (fresh cluster, no events yet). View = 3 nodes; the clause's
        // count (=2) is met by the view size, so the policy is not
        // degraded and the gate sits at the 2nd-largest persisted = 0.
        let m = metrics((0, 0), (0, 0));
        let a = both_active();
        let r = evaluate_durability(&p, 0, Some(&m), Some(&a));
        assert_eq!(r.durable_pos, 0);
        assert!(
            !r.degraded,
            "all 3 nodes present, view meets clause target — should not flag degraded"
        );
    }

    #[test]
    fn attribution_min_takes_smaller_when_both_connected() {
        let m = metrics((150, 100), (180, 80));
        let a = both_active();
        assert_eq!(connected_persisted_min(Some(&m), Some(&a)), 80);
    }

    // -- Race-window regression tests --
    //
    // The replication senders fix two memory-ordering issues at the
    // active-flag transition points:
    //
    //   B1 (`a84540a`): seed `metrics.{acked,in_memory}_sequence[i]`
    //   to `handshake.last_sequence` BEFORE setting active_flag=true
    //   on reconnect. Without this, the gate would observe (active=
    //   true, cursor=0) for ~1 RTT after a replica catch-up completed,
    //   freezing the gate on a degrade-friendly clause.
    //
    //   B2 (`8888732`): zero `metrics.{acked,in_memory}_sequence[i]`
    //   BEFORE setting active_flag=false on disconnect. Without this,
    //   a weak-memory reader could observe (active=true, cursor=0)
    //   for one iteration during the disconnect window.
    //
    // Both fixes are in the senders, but the gate's *behaviour* under
    // the race-window inputs is tested here. The intent is to lock in
    // the invariant: even under a hypothetical (active=true,cursor=0)
    // observation, the gate must not produce a spuriously-open answer
    // that would cause a client to be told "your event is durable"
    // when it isn't. Stalling-briefly is safe; opening-spuriously is
    // not.

    #[test]
    fn race_b1_post_seed_gate_doesnt_freeze_on_reconnect() {
        // Post-B1-fix state: replica reconnected, cursors seeded to
        // `handshake.last_sequence` (480) before active flipped to
        // true. Primary kept moving and is at 500. The gate's view
        // is now [primary=500, slot=480]; the durable position dips
        // from 500 (primary alone, degraded) to 480 (both nodes).
        //
        // The dip is correct, not a bug: once a 2nd node is
        // connected, durability is bounded by the slower of the two.
        // Events 481-500 were already served as durable on primary
        // alone — they aren't unsent. New responses for seq>500 wait
        // until slot acks; we just don't freeze at 0.
        let p = parse("persisted>=2").unwrap();
        let m = metrics((480, 480), (999, 999));
        let a = flags(true, false);
        let r = evaluate_durability(&p, 500, Some(&m), Some(&a));
        assert_eq!(
            r.durable_pos, 480,
            "post-seed reconnect should produce a coherent gate position equal to the slower node, not freeze at 0"
        );
    }

    #[test]
    fn race_b1_pre_seed_freeze_is_what_the_fix_avoids() {
        // Pre-B1-fix state: cursors at 0, active=true. The gate sees
        // [primary=500, slot=[0,0]] and 2nd-largest persisted = 0.
        // The gate WOULD freeze at 0. This test documents the bug
        // the seeding fix is designed to avoid; the senders ensure
        // this state is never observed in production.
        let p = parse("persisted>=2").unwrap();
        let m = metrics((0, 0), (999, 999));
        let a = flags(true, false);
        let r = evaluate_durability(&p, 500, Some(&m), Some(&a));
        assert_eq!(
            r.durable_pos, 0,
            "the gate behaviour under (active=true, cursor=0) — if the senders ever fail to seed before flipping active, this is the freeze the operator would see"
        );
    }

    #[test]
    fn race_b2_disconnect_window_doesnt_open_gate_spuriously() {
        // Simulates the B2 race window: a weak-memory reader observes
        // (active=true, cursor=0) for one iteration during the
        // disconnect transition. The slot legitimately has cursor=0
        // because the disconnect handler just zeroed the metrics.
        //
        // Critical invariant: the gate must NOT produce a higher
        // durable_pos than it would with the slot correctly excluded.
        // Specifically: with primary at 500, slot stale-zero-included,
        // the gate must not "see" the primary alone and open at 500
        // — that would let a client be told a seq is durable when
        // only the primary has it under a `persisted>=2` policy that
        // demands 2 nodes.
        let p = parse("persisted>=2").unwrap();
        let m = metrics((0, 0), (999, 999));
        let a = flags(true, false);
        let r = evaluate_durability(&p, 500, Some(&m), Some(&a));
        // 2nd-largest persisted across {primary=500, slot=0} = 0.
        // Gate stalls. ✓
        assert_eq!(r.durable_pos, 0);

        // Post-disconnect (active=false): view shrinks to {primary}.
        // `persisted>=2` is structurally unsatisfiable on a 1-node
        // view, so the gate stays at 0 AND surfaces degraded. The
        // matching stage's `replicas_connected==0` halt is what stops
        // accepting new orders; the gate side's job is just to keep
        // the existing in-flight orders stalled and the alert lit.
        let a_disconnected = flags(false, false);
        let r_after = evaluate_durability(&p, 500, Some(&m), Some(&a_disconnected));
        assert_eq!(r_after.durable_pos, 0);
        assert!(
            r_after.degraded,
            "post-disconnect view of size 1 cannot meet persisted>=2 → degraded"
        );
    }

    #[test]
    fn race_invariant_zero_cursor_never_opens_gate_above_slower_node() {
        // Property under both B1 and B2 race windows: for any slot
        // observed at cursor=0 with active=true, the gate cannot
        // produce a durable_pos that exceeds what an honest 2-node
        // evaluation would give. Spot-check a handful of primary
        // positions to lock the invariant.
        let p = parse("persisted>=2").unwrap();
        let m = metrics((0, 0), (999, 999));
        let a = flags(true, false);
        for primary_pos in [0, 1, 100, 500, 1_000_000_000_u64] {
            let r = evaluate_durability(&p, primary_pos, Some(&m), Some(&a));
            // 2nd-largest of {primary_pos, 0} = 0 for any primary > 0.
            // For primary_pos = 0, also 0. So always 0.
            assert_eq!(
                r.durable_pos, 0,
                "race-window observation must not open the gate above 0 for any primary position (got {} for primary_pos={primary_pos})",
                r.durable_pos
            );
        }
    }

    // ------------------------------------------------------------------
    // GateCrossTracker — per-cursor "first transition from below to
    // crossed" inside the gate loop, used by the journal-wait /
    // replica-wait histograms.
    // ------------------------------------------------------------------

    #[cfg(feature = "tick-to-trade")]
    #[test]
    fn gate_cross_tracker_records_journal_when_strictly_below() {
        // Journal starts at 5 (< 10), repl_min already at 100.
        // Journal crosses on the second observation. Replica was already
        // past at entry, so no replica sample.
        let mut t = GateCrossTracker::new(10);
        t.observe(5, 100, 1_000);
        t.observe(15, 100, 2_000);
        assert_eq!(t.journal_crossed(), Some(2_000));
        assert_eq!(t.replica_crossed(), None);
    }

    #[cfg(feature = "tick-to-trade")]
    #[test]
    fn gate_cross_tracker_records_replica_when_strictly_below() {
        // Mirror image: journal already past, replica below at entry.
        let mut t = GateCrossTracker::new(10);
        t.observe(50, 5, 1_000);
        t.observe(50, 12, 2_000);
        assert_eq!(t.journal_crossed(), None);
        assert_eq!(t.replica_crossed(), Some(2_000));
    }

    #[cfg(feature = "tick-to-trade")]
    #[test]
    fn gate_cross_tracker_records_both_when_both_below() {
        // Both below at entry, both cross independently.
        let mut t = GateCrossTracker::new(100);
        t.observe(50, 60, 1_000); // both below
        t.observe(105, 60, 2_000); // journal crosses
        t.observe(105, 110, 3_000); // replica crosses
        assert_eq!(t.journal_crossed(), Some(2_000));
        assert_eq!(t.replica_crossed(), Some(3_000));
    }

    #[cfg(feature = "tick-to-trade")]
    #[test]
    fn gate_cross_tracker_skips_cursor_already_past_at_entry() {
        // Both cursors already >= needed at first observation —
        // neither was on the critical path. No samples.
        let mut t = GateCrossTracker::new(10);
        t.observe(50, 100, 1_000);
        // Even later observations don't backfill: was_below is sticky.
        t.observe(60, 110, 2_000);
        assert_eq!(t.journal_crossed(), None);
        assert_eq!(t.replica_crossed(), None);
    }

    #[cfg(feature = "tick-to-trade")]
    #[test]
    fn gate_cross_tracker_first_observation_only_for_cross_decision() {
        // A cursor that goes back below `needed` after first iteration
        // (impossible in practice — cursors are monotonic — but we
        // verify the first-iteration snapshot is what gates the
        // sample). Journal: 50 < 10 false → was_below=false → no sample.
        let mut t = GateCrossTracker::new(10);
        t.observe(50, 5, 1_000); // journal already past, replica below
        t.observe(20, 12, 2_000); // both >= needed now
        // Journal: was_below=false at entry → still no sample.
        assert_eq!(t.journal_crossed(), None);
        // Replica: was_below=true at entry, crosses on iter 2 → sample.
        assert_eq!(t.replica_crossed(), Some(2_000));
    }

    #[cfg(feature = "tick-to-trade")]
    #[test]
    fn gate_cross_tracker_holds_first_cross_only() {
        // Once a cross is recorded, later observations don't
        // overwrite — the metric is "when did it first cross", not
        // "when was it last below".
        let mut t = GateCrossTracker::new(10);
        t.observe(5, 100, 1_000);
        t.observe(15, 100, 2_000); // first cross
        t.observe(25, 100, 3_000); // would otherwise re-record
        assert_eq!(t.journal_crossed(), Some(2_000));
    }

    // -- DegradationLogger flap-suppression --
    //
    // The logger gates transition logs on a sustained-state hold so a
    // replica flapping at sub-second cadence doesn't spam the journal.
    // These tests don't observe the logs themselves (tracing is
    // process-global and brittle to capture in unit tests); they
    // assert the underlying state machine via the `policy_degraded`
    // gauge, which the logger updates on every tick regardless of
    // log emission.

    fn logger_test_policy() -> crate::runtime::durability_policy::Policy {
        parse("persisted>=2").unwrap()
    }

    /// Tick the logger N times at `step` intervals, alternating
    /// `degraded` per call. Returns the gauge value after the last
    /// tick — useful for asserting that flap cycles don't leak the
    /// AtomicBool into a wrong terminal state.
    fn drive_logger(
        logger: &mut DegradationLogger,
        utilization: &StageUtilization,
        policy: &crate::runtime::durability_policy::Policy,
        start: Instant,
        states: &[bool],
        step: Duration,
    ) -> bool {
        for (i, &state) in states.iter().enumerate() {
            let now = start + step.checked_mul(i as u32).unwrap_or(Duration::ZERO);
            logger.tick(policy, utilization, state, now, Duration::from_secs(5));
        }
        utilization.policy_degraded.load(Ordering::Relaxed)
    }

    #[test]
    fn logger_gauge_tracks_state_immediately() {
        // The /healthz gauge reflects the *latest* state on every
        // tick — this is what dashboards / alerts read. Sustained-
        // state gating only affects the warn/info log emission.
        let p = logger_test_policy();
        let utilization = StageUtilization::new();
        let now = Instant::now();
        let mut logger = DegradationLogger::new(now);

        logger.tick(&p, &utilization, true, now, Duration::from_secs(5));
        assert!(utilization.policy_degraded.load(Ordering::Relaxed));

        logger.tick(
            &p,
            &utilization,
            false,
            now + Duration::from_millis(50),
            Duration::from_secs(5),
        );
        assert!(!utilization.policy_degraded.load(Ordering::Relaxed));
    }

    #[test]
    fn logger_starting_degraded_marks_initial_state_logged() {
        // `new_starting_degraded` emits the startup warn and treats
        // the state as already-logged so the next tick at the same
        // cluster shape doesn't re-emit instantly. The gauge starts
        // at 1.
        let p = logger_test_policy();
        let utilization = StageUtilization::new();
        let now = Instant::now();
        let mut logger = DegradationLogger::new_starting_degraded(now, &p);
        // The logger doesn't write the gauge from the constructor —
        // first tick does. Tick at the same state to settle the
        // gauge. No new log line should fire (state hasn't changed).
        logger.tick(
            &p,
            &utilization,
            true,
            now + Duration::from_millis(10),
            Duration::from_secs(5),
        );
        assert!(utilization.policy_degraded.load(Ordering::Relaxed));
    }

    #[test]
    fn logger_handles_rapid_flap_without_panic() {
        // Drive the logger through 100 alternating flips at 100ms
        // each (faster than the 1s flap-hold). The state machine
        // must remain coherent — no panics, gauge tracks final
        // state, and `pending_logged` doesn't get stuck.
        let p = logger_test_policy();
        let utilization = StageUtilization::new();
        let now = Instant::now();
        let mut logger = DegradationLogger::new(now);
        let states: Vec<bool> = (0..100u32).map(|i| i.is_multiple_of(2)).collect();
        let final_state = drive_logger(
            &mut logger,
            &utilization,
            &p,
            now,
            &states,
            Duration::from_millis(100),
        );
        // 100 states starting at i=0 → final state is i=99 → odd → false.
        assert!(!final_state);
    }

    #[test]
    fn logger_sustained_degraded_eventually_settles() {
        // After a sustained-true state, the logger should be in the
        // "logged the onset" mode. Drive 5 ticks of degraded=true at
        // 500ms intervals — total 2s, well past the 1s flap-hold.
        // Last tick should leave gauge=1 and the heartbeat re-emit
        // window primed (last_log set).
        let p = logger_test_policy();
        let utilization = StageUtilization::new();
        let now = Instant::now();
        let mut logger = DegradationLogger::new(now);
        let final_state = drive_logger(
            &mut logger,
            &utilization,
            &p,
            now,
            &[true; 5],
            Duration::from_millis(500),
        );
        assert!(final_state);
    }

    #[test]
    fn logger_recovery_to_healthy_settles_gauge() {
        // Sustained degraded → sustained healthy. Gauge should end at 0.
        let p = logger_test_policy();
        let utilization = StageUtilization::new();
        let now = Instant::now();
        let mut logger = DegradationLogger::new_starting_degraded(now, &p);
        let mut states = vec![true; 5]; // 2.5s degraded
        states.extend(vec![false; 5]); // 2.5s healthy
        let final_state = drive_logger(
            &mut logger,
            &utilization,
            &p,
            now,
            &states,
            Duration::from_millis(500),
        );
        assert!(!final_state);
    }
}
