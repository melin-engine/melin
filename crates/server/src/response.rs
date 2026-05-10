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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use io_uring::{IoUring, opcode, types};
use tracing::{debug, error};

use melin_disruptor::padding::Sequence;
use melin_disruptor::ring;

use crate::durability_policy::{CursorView, EvalStatus, Policy};
use crate::replication::ReplicationMetrics;
use crate::{OutputPayload, OutputSlot};
#[cfg(feature = "latency-trace")]
use melin_journal::trace;
use melin_trading::types::QueryResponse;
use melin_transport_core::pipeline::StageUtilization;

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
    pub journal_cursor: Arc<Sequence>,
    /// Durability policy evaluated per gate iteration. See
    /// [`crate::durability_policy`] for the policy model.
    pub policy: Policy,
    /// Per-slot replica cursors. `None` for standalone deployments
    /// (no replication wiring) — the policy then evaluates against the
    /// primary alone.
    pub replication_metrics: Option<Arc<ReplicationMetrics>>,
    /// Per-slot replica active flags. Only "true" slots are included in
    /// the cursor view fed to `Policy::evaluate`, so degrade-friendly
    /// clauses (`persisted>=2 best_effort`) clamp against the *connected* count
    /// rather than counting disconnected slots as zero-cursor nodes.
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
        journal_cursor,
        policy,
        replication_metrics,
        replica_active,
        heartbeat_interval,
        busy_spin,
        utilization,
    } = config;
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
    // below from the policy's startup evaluation so a degraded
    // standalone deployment (default `persisted>=2 best_effort` →
    // view.len()=1 → clamps to 1, marks `degraded=true`) is visible
    // immediately on `/healthz` and in the journal.
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
        let journal_pos = journal_cursor.get().load(Ordering::Acquire);
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
    // `melin_journal::trace`. The four breakdown stages
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
                let egress_start = trace::trace_ts();
                flush_sends(
                    &mut ring,
                    &mut connections,
                    &dirty_connections,
                    &mut to_remove,
                    &mut cqes,
                );
                #[cfg(feature = "tick-to-trade")]
                egress_rec.record_elapsed(egress_start, trace::trace_ts());
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
                    let journal_pos = journal_cursor.get().load(Ordering::Acquire);
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
        let consume_ts = trace::trace_ts();

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
            let max_seq = batch[..count]
                .iter()
                .map(|s| s.input_seq)
                .max()
                .expect("non-empty batch");
            // `saturating_add` is free on this cold path. `u64::MAX`
            // is astronomically out of reach (~10²² events), but if
            // it ever happens — bug, test fixture, far-future replay
            // — we'd rather saturate at MAX than wrap to 0 and open
            // the gate spuriously.
            let needed = max_seq.saturating_add(1);
            #[cfg(feature = "tick-to-trade")]
            {
                gate_tracker = GateCrossTracker::new(needed);
            }
            if cached_durable_pos < needed {
                loop {
                    let journal_pos = journal_cursor.get().load(Ordering::Acquire);
                    let metrics_ref = replication_metrics.as_deref();
                    let active_ref = replica_active.as_ref();
                    let repl_min = connected_persisted_min(metrics_ref, active_ref);

                    #[cfg(feature = "tick-to-trade")]
                    gate_tracker.observe(journal_pos, repl_min, trace::trace_ts());

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
                    let encode_start = trace::trace_ts();
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
                    encode_rec.record_elapsed(encode_start, trace::trace_ts());

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
                        server_e2e_rec.record_elapsed(slot.recv_ts, trace::trace_ts());
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
        dispatch_rec.record_elapsed(consume_ts, trace::trace_ts());
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
/// with zero cursors. This is what gives degrade-friendly clauses
/// (`persisted>=2 best_effort`) the correct "clamp to connected count" semantics
/// — the view's `len()` reflects how many nodes are actually available.
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
    /// (e.g. standalone deployments running `persisted>=2 best_effort`
    /// from t=0). Logs a startup warn immediately and treats the
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
/// reads `trace::trace_ts()` once per gate iteration and feeds it in.
#[cfg(feature = "tick-to-trade")]
pub(crate) struct GateCrossTracker {
    needed: u64,
    journal_crossed_ts: Option<trace::TraceTimestamp>,
    replica_crossed_ts: Option<trace::TraceTimestamp>,
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
        now_ns: trace::TraceTimestamp,
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

    pub(crate) fn journal_crossed(&self) -> Option<trace::TraceTimestamp> {
        self.journal_crossed_ts
    }

    pub(crate) fn replica_crossed(&self) -> Option<trace::TraceTimestamp> {
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
    use super::{connected_persisted_min, evaluate_durability};
    use crate::durability_policy::parse;
    use crate::replication::ReplicationMetrics;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

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
        // Strict `persisted>=2` on a standalone primary stays at 0:
        // the operator asked for two copies and there is only one.
        let p = parse("persisted>=2").unwrap();
        let r = evaluate_durability(&p, 500, None, None);
        assert_eq!(r.durable_pos, 0);
        // Strict clauses don't surface as "degraded" — the operator
        // chose fail-closed and got it. Degraded is reserved for
        // clauses with `!` that actively clamped.
        assert!(!r.degraded);
    }

    #[test]
    fn standalone_degrade_persisted_two_opens_at_primary() {
        // Same shape with `!`: clamp to the connected count (=1) and
        // gate opens at journal_pos. The clamp from 2 → 1 is exactly
        // what `degraded` reports.
        let p = parse("persisted>=2 best_effort").unwrap();
        let r = evaluate_durability(&p, 500, None, None);
        assert_eq!(r.durable_pos, 500);
        assert!(r.degraded);
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
    fn single_replica_degrade_persisted_two_still_requires_both_survivors() {
        // Same shape with `!`. `effective_count = min(2, view.len()=2)
        // = 2`, so the clamp is a no-op when 2 nodes are connected.
        let p = parse("persisted>=2 best_effort").unwrap();
        let m = metrics((100, 100), (999, 999));
        let a = flags(true, false);
        assert_eq!(
            evaluate_durability(&p, 50, Some(&m), Some(&a)).durable_pos,
            50
        );
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
    fn both_replicas_disconnected_degrade_opens_at_primary() {
        // Same shape with `!` clamps to view.len()=1 and gate opens
        // at the primary alone. Note the matching stage's separate
        // halt at `replicas_connected==0` rejects new orders before
        // they reach the gate; this verifies the gate semantics in
        // isolation.
        let p = parse("persisted>=2 best_effort").unwrap();
        let m = metrics((999, 999), (999, 999));
        let a = flags(false, false);
        assert_eq!(
            evaluate_durability(&p, 500, Some(&m), Some(&a)).durable_pos,
            500
        );
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
        let p = parse("persisted>=2 best_effort").unwrap();
        // Both replicas just handshook at seq 0, primary also at 0
        // (fresh cluster, no events yet). View = 3 nodes; clamp from
        // target=2 to 2 is a no-op; 2nd-largest persisted = 0.
        let m = metrics((0, 0), (0, 0));
        let a = both_active();
        let r = evaluate_durability(&p, 0, Some(&m), Some(&a));
        assert_eq!(r.durable_pos, 0);
        assert!(
            !r.degraded,
            "all 3 nodes present, no clamp — should not flag degraded"
        );
    }

    #[test]
    fn attribution_min_takes_smaller_when_both_connected() {
        let m = metrics((150, 100), (180, 80));
        let a = both_active();
        assert_eq!(connected_persisted_min(Some(&m), Some(&a)), 80);
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
}
