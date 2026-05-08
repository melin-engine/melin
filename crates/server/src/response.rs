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
    pub replication_cursor: Arc<std::sync::atomic::AtomicU64>,
    pub fastest_replica_cursor: Arc<std::sync::atomic::AtomicU64>,
    pub quorum_durability: bool,
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
/// Durability gating (quorum mode, default):
///   `durable = max(repl_min, min(journal, repl_max))`
/// An event is durable when it exists on 2+ nodes: either both replicas
/// acked, or the journal fsynced and the fastest replica acked.
/// With `--no-quorum-durability`: `durable = min(journal, repl_min)`.
pub fn run(
    mut consumer: ring::Consumer<OutputSlot>,
    control_rx: mpsc::Receiver<ControlEvent>,
    config: Response,
    shutdown: &AtomicBool,
) {
    let Response {
        journal_cursor,
        replication_cursor,
        fastest_replica_cursor,
        quorum_durability,
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
    // This is the minimum confirmed-durable sequence across all durability
    // sources (journal + replication, or replication-only in quorum mode).
    let mut cached_durable_pos: u64 = 0;

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
        // An event is durable when it exists on at least two nodes:
        //
        //   durable = max(both_replicas_acked, min(journal_synced, fastest_replica_acked))
        //
        // - `replication_cursor` = min(slot0, slot1): both replicas acked.
        // - `fastest_replica_cursor` = max(slot0, slot1): fastest replica acked.
        // - `journal_cursor`: local fsync confirmed.
        //
        // This gives the best of both paths: if both replicas ack before
        // fsync, NVMe latency is off the critical path. If one replica is
        // slow but fsync is fast, we respond as soon as fsync + fast replica
        // confirms (two durable copies via different routes).
        //
        // Without quorum (--no-quorum-durability): gate on
        // min(journal_cursor, replication_cursor) as before.
        // Per-slot journal-wait / replica-wait tracker. See
        // `GateCrossTracker` for the rationale (only records cursors
        // that were actually on the critical path).
        #[cfg(feature = "tick-to-trade")]
        let mut gate_tracker;
        {
            let max_seq = batch[..count]
                .iter()
                .map(|s| s.input_seq)
                .max()
                .expect("non-empty batch");
            let needed = max_seq + 1;
            #[cfg(feature = "tick-to-trade")]
            {
                gate_tracker = GateCrossTracker::new(needed);
            }
            if cached_durable_pos < needed {
                loop {
                    let journal_pos = journal_cursor.get().load(Ordering::Acquire);
                    let repl_min = replication_cursor.load(Ordering::Acquire);

                    #[cfg(feature = "tick-to-trade")]
                    gate_tracker.observe(journal_pos, repl_min, trace::trace_ts());

                    cached_durable_pos = durable_pos(
                        journal_pos,
                        repl_min,
                        fastest_replica_cursor.load(Ordering::Acquire),
                        quorum_durability,
                    );

                    if cached_durable_pos >= needed {
                        // Record which cursor was slower at the moment the
                        // gate opened. This answers "which subsystem should
                        // I optimize?" — not "which path provided durability"
                        // (in quorum mode, durability can come from replicas
                        // alone even when the journal is slower).
                        // Relaxed is fine — health reads are infrequent.
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

/// Compute the durable position from journal and replication cursors.
///
/// Quorum mode (2 replicas connected — both cursors finite):
///   `durable = max(repl_min, min(journal_pos, repl_max))`
/// An event is durable when it exists on 2+ nodes.
///
/// Standalone / degraded (0-1 replicas — either cursor is `u64::MAX`):
///   `durable = min(journal_pos, repl_min)`
/// Falls back to journal + replication gating.
#[inline(always)]
pub(crate) fn durable_pos(
    journal_pos: u64,
    repl_min: u64,
    repl_max: u64,
    quorum_durability: bool,
) -> u64 {
    // Quorum requires both replica slots active (neither cursor is
    // u64::MAX). With only 1 replica, repl_max = u64::MAX (the idle
    // slot), and the formula would degrade to max(repl_min, journal)
    // which can skip the replica ack — only 1-node durability.
    if quorum_durability && repl_min != u64::MAX && repl_max != u64::MAX {
        // Both replicas connected: two durable copies via whichever
        // path completes first.
        repl_min.max(journal_pos.min(repl_max))
    } else {
        // Standalone or degraded: gate on journal fsync + replication.
        journal_pos.min(repl_min)
    }
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
    use super::durable_pos;

    // --- Standalone (no replicas) ---

    #[test]
    fn standalone_gates_on_journal() {
        // repl_min = repl_max = u64::MAX → must return journal_pos,
        // NOT u64::MAX. This was the bug: the quorum formula produced
        // u64::MAX in standalone mode, bypassing all durability gating.
        let journal = 500;
        let pos = durable_pos(journal, u64::MAX, u64::MAX, true);
        assert_eq!(pos, journal);
    }

    #[test]
    fn standalone_non_quorum_gates_on_journal() {
        let journal = 500;
        let pos = durable_pos(journal, u64::MAX, u64::MAX, false);
        assert_eq!(pos, journal);
    }

    // --- Quorum mode, 2 replicas connected ---

    #[test]
    fn quorum_both_replicas_ahead_of_journal() {
        // Both replicas acked past journal → durable = repl_min.
        // Journal hasn't fsynced yet but both replicas have the data.
        let pos = durable_pos(50, 100, 120, true);
        assert_eq!(pos, 100);
    }

    #[test]
    fn quorum_journal_ahead_of_both_replicas() {
        // Journal fsynced past both replicas → durable = repl_min.
        // min(journal=500, repl_max=120) = 120, max(repl_min=100, 120) = 120.
        let pos = durable_pos(500, 100, 120, true);
        assert_eq!(pos, 120);
    }

    #[test]
    fn quorum_journal_between_slow_and_fast_replica() {
        // Fast replica at 200, slow at 50, journal at 150.
        // min(journal=150, repl_max=200) = 150, max(repl_min=50, 150) = 150.
        // Durable = 150: journal + fast replica both have it.
        let pos = durable_pos(150, 50, 200, true);
        assert_eq!(pos, 150);
    }

    #[test]
    fn quorum_both_replicas_equal() {
        // Both replicas at same position, journal ahead.
        // min(500, 100) = 100, max(100, 100) = 100.
        let pos = durable_pos(500, 100, 100, true);
        assert_eq!(pos, 100);
    }

    // --- Non-quorum mode, replicas connected ---

    #[test]
    fn non_quorum_takes_min_of_journal_and_replication() {
        // Non-quorum always returns min(journal, repl_min).
        let pos = durable_pos(500, 100, 200, false);
        assert_eq!(pos, 100);

        let pos = durable_pos(50, 100, 200, false);
        assert_eq!(pos, 50);
    }

    // --- Single replica (repl_min == repl_max, but not u64::MAX) ---

    #[test]
    fn single_replica_falls_back_to_non_quorum() {
        // One replica at 100, other idle (u64::MAX). Quorum requires
        // both slots active — with repl_max = u64::MAX, falls back to
        // min(journal, repl_min) to gate on both journal and replica.
        let pos = durable_pos(50, 100, u64::MAX, true);
        assert_eq!(pos, 50);

        // Replica ahead of journal: gates on journal.
        let pos = durable_pos(50, 200, u64::MAX, true);
        assert_eq!(pos, 50);

        // Journal ahead of replica: gates on replica.
        let pos = durable_pos(200, 100, u64::MAX, true);
        assert_eq!(pos, 100);
    }

    // --- Edge: journal at 0 ---

    #[test]
    fn quorum_journal_at_zero() {
        // Journal hasn't fsynced anything yet, replicas acked.
        // min(0, 200) = 0, max(100, 0) = 100.
        let pos = durable_pos(0, 100, 200, true);
        assert_eq!(pos, 100);
    }

    #[test]
    fn standalone_journal_at_zero() {
        let pos = durable_pos(0, u64::MAX, u64::MAX, true);
        assert_eq!(pos, 0);
    }

    // --- Gate bottleneck attribution ---
    //
    // The response stage increments gate_journal when journal_pos <= repl_min
    // at the moment the gate opens, and gate_replication otherwise. These
    // tests verify the attribution logic matches the durable_pos formula.

    use melin_transport_core::pipeline::StageUtilization;
    use std::sync::Arc;

    /// Simulate the attribution logic at the moment the gate opens.
    /// Passes cursor values that make durable_pos >= needed, then
    /// checks which counter (journal or replication) was incremented.
    fn simulate_gate(
        journal_pos: u64,
        repl_min: u64,
        repl_max: u64,
        quorum: bool,
        needed: u64,
    ) -> (&'static str, Arc<StageUtilization>) {
        let util = Arc::new(StageUtilization::new());
        let durable = durable_pos(journal_pos, repl_min, repl_max, quorum);
        assert!(
            durable >= needed,
            "test setup error: durable_pos ({durable}) < needed ({needed})"
        );
        // Same attribution logic as the response stage spin loop.
        {
            if journal_pos <= repl_min {
                util.gate_journal
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            } else {
                util.gate_replication
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
        let j = util.gate_journal.load(std::sync::atomic::Ordering::Relaxed);
        let r = util
            .gate_replication
            .load(std::sync::atomic::Ordering::Relaxed);
        let label = if j > 0 && r == 0 {
            "journal"
        } else if r > 0 && j == 0 {
            "replication"
        } else {
            "none"
        };
        (label, util)
    }

    #[test]
    fn gate_bottleneck_standalone_journal() {
        // Standalone mode: repl_min = u64::MAX, journal is the only path.
        // journal_pos (50) <= repl_min (MAX) → attributed to journal.
        let (label, _) = simulate_gate(50, u64::MAX, u64::MAX, false, 50);
        assert_eq!(label, "journal");
    }

    #[test]
    fn gate_bottleneck_journal_slower_than_replication() {
        // Both connected, journal behind replication.
        // journal_pos (50) <= repl_min (100) → journal was the bottleneck.
        let (label, _) = simulate_gate(50, 100, 120, false, 50);
        assert_eq!(label, "journal");
    }

    #[test]
    fn gate_bottleneck_replication_slower_than_journal() {
        // Journal ahead, replication behind.
        // journal_pos (200) > repl_min (50) → replication was the bottleneck.
        let (label, _) = simulate_gate(200, 50, 80, false, 50);
        assert_eq!(label, "replication");
    }

    #[test]
    fn gate_bottleneck_both_equal() {
        // Both cursors at the same position. journal_pos (100) <= repl_min
        // (100), so attributed to journal (tie-break favors journal).
        let (label, _) = simulate_gate(100, 100, 100, false, 100);
        assert_eq!(label, "journal");
    }

    #[test]
    fn gate_bottleneck_quorum_journal_slower() {
        // Quorum mode: both replicas at 100+, journal at 50.
        // durable = max(100, min(50, 120)) = 100. Journal is slower.
        let (label, _) = simulate_gate(50, 100, 120, true, 100);
        assert_eq!(label, "journal");
    }

    #[test]
    fn gate_bottleneck_quorum_replication_slower() {
        // Quorum mode: journal at 200, slow replica at 50, fast at 80.
        // durable = max(50, min(200, 80)) = max(50, 80) = 80.
        // journal_pos (200) > repl_min (50) → replication.
        let (label, _) = simulate_gate(200, 50, 80, true, 80);
        assert_eq!(label, "replication");
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
