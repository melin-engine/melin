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
use tracing::debug;

use trading_disruptor::padding::Sequence;
use trading_disruptor::spsc;

use trading_engine::journal::pipeline::{OutputPayload, OutputSlot};
#[cfg(feature = "latency-trace")]
use trading_engine::journal::trace;

use trading_protocol::codec;
use trading_protocol::message::ResponseKind;

/// Maximum number of output slots consumed per batch.
const MAX_BATCH: usize = 1024;

/// Maximum encoded response size. Responses are small (execution reports),
/// so 128 bytes is generous.
const MAX_RESPONSE_BUF: usize = 128;

/// io_uring submission queue depth for sends. Must be ≥ max concurrent
/// connections to avoid SQ overflow when all connections are dirty.
/// Power of 2 for io_uring alignment.
const RING_SIZE: u32 = 1024;

/// Maximum accumulated send buffer per connection (64 KiB). If a client
/// falls behind and the buffer exceeds this, the connection is dropped.
/// 64 KiB holds ~500 response frames — well beyond any reasonable lag.
const MAX_SEND_BUF: usize = 64 * 1024;

/// Control plane events for connection registration.
///
/// Carries the raw fd for io_uring SEND and a boxed owner to keep the
/// fd alive. The owner is the write half of the socket (TCP/UDS),
/// type-erased since we never call `Write` methods on it.
pub enum ControlEvent {
    /// Register a new connection for io_uring sends.
    Connected {
        connection_id: u64,
        fd: RawFd,
        /// Owns the write half — keeps the fd valid. Dropped on disconnect.
        _owner: Box<dyn Send>,
    },
    /// Remove a disconnected connection.
    Disconnected { connection_id: u64 },
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
/// Same semantics as `response::run` — consumes from the output SPSC, waits
/// for journal durability, and sends responses — but uses io_uring SEND
/// instead of blocking `write(2)` syscalls.
pub fn run(
    mut consumer: spsc::Consumer<OutputSlot>,
    control_rx: mpsc::Receiver<ControlEvent>,
    journal_cursor: Arc<Sequence>,
    shutdown: &AtomicBool,
    heartbeat_interval: Option<Duration>,
) {
    let mut ring =
        IoUring::new(RING_SIZE).expect("failed to create io_uring instance for response stage");

    // Connection table: maps connection IDs to their state.
    // HashMap for O(1) lookup. Pre-sized for a reasonable number of concurrent clients.
    let mut connections: HashMap<u64, ConnectionEntry> = HashMap::with_capacity(256);

    let mut batch = [OutputSlot::default(); MAX_BATCH];
    let mut encode_buf = [0u8; MAX_RESPONSE_BUF];

    // Cached journal cursor value to avoid atomic reads on every slot.
    #[cfg(not(feature = "no-fsync"))]
    let mut cached_journal_pos: u64 = 0;
    // Suppress unused warnings when journal gating is disabled.
    #[cfg(feature = "no-fsync")]
    let _ = &journal_cursor;

    #[cfg(feature = "latency-trace")]
    let mut spsc_hist =
        trace::StageHistogram::new("response: SPSC wakeup (matching publish → response consume)");
    #[cfg(feature = "latency-trace")]
    let mut dispatch_hist =
        trace::StageHistogram::new("response: dispatch (consume → socket write)");
    #[cfg(feature = "latency-trace")]
    let mut server_e2e_hist =
        trace::StageHistogram::new("server e2e (reader recv → response flush)");

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

    #[cfg(feature = "pipeline-stats")]
    let mut busy_count: u64 = 0;
    #[cfg(feature = "pipeline-stats")]
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
            #[cfg(feature = "latency-trace")]
            {
                spsc_hist.print_report();
                dispatch_hist.print_report();
                server_e2e_hist.print_report();
            }
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
                    _owner,
                } => {
                    connections.insert(
                        connection_id,
                        ConnectionEntry {
                            fd,
                            _owner,
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

            #[cfg(feature = "pipeline-stats")]
            {
                idle_count += 1;
            }
            if idle_spins < 1000 {
                idle_spins += 1;
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
            }
            continue;
        }
        idle_spins = 0;
        #[cfg(feature = "pipeline-stats")]
        {
            busy_count += 1;
        }

        #[cfg(feature = "latency-trace")]
        let consume_ts = trace::trace_ts();

        // Wait for the journal to confirm the entire batch is durable.
        #[cfg(not(feature = "no-fsync"))]
        {
            let max_seq = batch[..count]
                .iter()
                .map(|s| s.input_seq)
                .max()
                .expect("non-empty batch");
            let needed = max_seq + 1;
            if cached_journal_pos < needed {
                loop {
                    cached_journal_pos = journal_cursor.get().load(Ordering::Acquire);
                    if cached_journal_pos >= needed {
                        break;
                    }
                    std::hint::spin_loop();
                }
            }
        }

        for slot in &batch[..count] {
            #[cfg(feature = "latency-trace")]
            spsc_hist.record_ns(trace::trace_elapsed_ns(slot.match_complete_ts, consume_ts));

            let kind = match slot.payload {
                OutputPayload::Report(report) => ResponseKind::Report(report),
                OutputPayload::BatchEnd => ResponseKind::BatchEnd,
                OutputPayload::EngineError => ResponseKind::EngineError,
                OutputPayload::StatsHeader {
                    active_connections,
                    events_processed,
                    journal_sequence,
                } => ResponseKind::StatsHeader {
                    active_connections,
                    events_processed,
                    journal_sequence,
                },
            };

            if let Some(entry) = connections.get_mut(&slot.connection_id) {
                // Encode the response (includes 4-byte length prefix).
                let written = match codec::encode_response(&kind, &mut encode_buf) {
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
                    continue;
                }

                // Append the full wire frame to the connection's send buffer.
                // encode_response writes [length(4) | payload], which is the
                // complete wire format — no extra framing needed.
                entry.send_buf.extend_from_slice(&encode_buf[..written]);
                entry.last_send = Instant::now();
                dirty_connections.insert(slot.connection_id);

                // Record server-side end-to-end: reader recv → response flush.
                #[cfg(feature = "latency-trace")]
                if matches!(kind, ResponseKind::BatchEnd) {
                    server_e2e_hist
                        .record_ns(trace::trace_elapsed_ns(slot.recv_ts, trace::trace_ts()));
                }
            }
        }

        // Remove connections that exceeded the send buffer limit.
        for conn_id in to_remove.drain(..) {
            connections.remove(&conn_id);
            dirty_connections.remove(&conn_id);
        }

        #[cfg(feature = "latency-trace")]
        dispatch_hist.record_ns(trace::trace_elapsed_ns(consume_ts, trace::trace_ts()));
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
        debug!(error = %e, "io_uring submit_and_wait failed in response stage");
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
