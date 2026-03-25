//! Response stage — routes matching output directly to connection sockets.
//!
//! Consumes from the output SPSC queue (matching → response) and writes
//! encoded responses directly to each connection's blocking socket writer.
//! Before sending, waits for the journal cursor to confirm durability —
//! this is the persist-before-ack boundary.
//!
//! Runs on a dedicated OS thread. No tokio involvement — eliminates
//! async scheduling jitter from the response path.

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use melin_disruptor::padding::Sequence;
use melin_disruptor::spsc;

use melin_engine::journal::pipeline::{OutputPayload, OutputSlot};
#[cfg(feature = "latency-trace")]
use melin_engine::journal::trace;

use melin_protocol::blocking::BlockingFrameWriter;
use melin_protocol::codec;
use melin_protocol::message::ResponseKind;

/// Maximum number of output slots consumed per batch.
const MAX_BATCH: usize = 1024;

/// Maximum encoded response size. Responses are small (execution reports),
/// so 128 bytes is generous.
const MAX_RESPONSE_BUF: usize = 128;

/// Control plane events for connection registration.
///
/// Sent on a `std::sync::mpsc` channel (not the disruptor) because
/// connect/disconnect is rare and not on the hot path.
///
/// Uses `Box<dyn Write + Send>` to erase the concrete stream type
/// (TCP or UDS). The vtable dispatch cost is negligible compared to
/// the syscall cost of write_all.
pub enum ControlEvent {
    /// Register a new connection's blocking writer.
    Connected {
        connection_id: u64,
        writer: BlockingFrameWriter<Box<dyn Write + Send>>,
    },
    /// Remove a disconnected connection's writer.
    Disconnected { connection_id: u64 },
}

/// Per-connection state for the response stage.
struct ConnectionState {
    writer: BlockingFrameWriter<Box<dyn Write + Send>>,
    /// Last time data was sent to this connection. Used for heartbeat scheduling.
    last_send: Instant,
}

/// Run the response stage loop. Blocks the calling thread until shutdown.
///
/// Consumes from the output SPSC and writes encoded responses directly
/// to each connection's socket. For each output slot, waits until both
/// the journal cursor and replication cursor have advanced past `input_seq`
/// before writing — ensuring the client never receives a response for an
/// event that isn't yet durable locally AND replicated.
///
/// When replication is disabled (standalone mode), `replication_cursor` is
/// initialized to `u64::MAX` so `min(journal, MAX) = journal`.
pub fn run(
    mut consumer: spsc::Consumer<OutputSlot>,
    control_rx: mpsc::Receiver<ControlEvent>,
    journal_cursor: Arc<Sequence>,
    replication_cursor: Arc<AtomicU64>,
    shutdown: &AtomicBool,
    heartbeat_interval: Option<Duration>,
    active_connections: Arc<AtomicU64>,
    busy_spin: bool,
) {
    // Connection table: maps connection IDs to their state (writer + last_send).
    // HashMap for O(1) lookup. Pre-sized for a reasonable number of concurrent clients.
    let mut connections: HashMap<u64, ConnectionState> = HashMap::with_capacity(256);

    let mut batch = [OutputSlot::default(); MAX_BATCH];
    let mut encode_buf = [0u8; MAX_RESPONSE_BUF];

    // Cached journal cursor value to avoid atomic reads on every slot.
    #[cfg(not(feature = "no-fsync"))]
    let mut cached_journal_pos: u64 = 0;
    // Suppress unused warnings when journal gating is disabled.
    #[cfg(feature = "no-fsync")]
    let _ = (&journal_cursor, &replication_cursor);

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
    // Under high load, we process many SPSC batches before flushing,
    // amortizing the cost of N flush syscalls (one per connection) across
    // many batches instead of paying it every batch.
    let mut dirty_connections: HashSet<u64> = HashSet::new();

    // Pre-encode the heartbeat response frame once. Tag-only (1 byte payload).
    let heartbeat_frame = {
        let mut buf = [0u8; 8];
        let written =
            codec::encode_response(&ResponseKind::Heartbeat, &mut buf).expect("heartbeat encodes");
        // write_frame expects payload without length prefix.
        buf[4..written].to_vec()
    };

    // Coarse timestamp for heartbeat scan — avoids Instant::now() on every spin.
    let mut last_heartbeat_scan = Instant::now();

    // Adaptive spin: spin first (fast wakeup), yield after threshold
    // to avoid aggressive OS preemption of this pipeline thread.
    let mut idle_spins: u32 = 0;

    #[cfg(feature = "pipeline-stats")]
    let mut busy_count: u64 = 0;
    #[cfg(feature = "pipeline-stats")]
    let mut idle_count: u64 = 0;

    loop {
        if shutdown.load(Ordering::Relaxed) {
            // Flush any remaining buffered writes before shutdown.
            // Best-effort: clients may have disconnected already.
            for conn_id in &dirty_connections {
                if let Some(state) = connections.get_mut(conn_id)
                    && let Err(e) = state.writer.flush()
                {
                    tracing::debug!(conn = conn_id, "flush on shutdown: {e}");
                }
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
                    writer,
                } => {
                    connections.insert(
                        connection_id,
                        ConnectionState {
                            writer,
                            last_send: Instant::now(),
                        },
                    );
                }
                ControlEvent::Disconnected { connection_id } => {
                    if connections.remove(&connection_id).is_some() {
                        active_connections.fetch_sub(1, Ordering::Relaxed);
                    }
                }
            }
        }

        // Consume output slots from matching stage.
        let count = consumer.consume_batch(&mut batch, MAX_BATCH);
        if count == 0 {
            // SPSC is empty — flush all dirty connections before spinning.
            // This is the adaptive flushing strategy: under high load, we
            // process many batches before reaching this point, amortizing
            // flush syscall overhead across thousands of entries. Under low
            // load, we reach this quickly and flush promptly.
            if !dirty_connections.is_empty() {
                for conn_id in dirty_connections.drain() {
                    if let Some(state) = connections.get_mut(&conn_id)
                        && let Err(e) = state.writer.flush()
                    {
                        tracing::debug!(
                            connection_id = conn_id,
                            error = %e,
                            "flush error, dropping connection"
                        );
                        connections.remove(&conn_id);
                        active_connections.fetch_sub(1, Ordering::Relaxed);
                    }
                }
            }

            // Send heartbeats to idle connections. Only checked during
            // idle periods (SPSC empty) to avoid overhead on the hot path.
            if let Some(interval) = heartbeat_interval {
                let now = Instant::now();
                // Coarse gate: only scan at most once per second.
                if now.duration_since(last_heartbeat_scan) >= Duration::from_secs(1) {
                    last_heartbeat_scan = now;
                    let mut failed: Vec<u64> = Vec::new();
                    for (&conn_id, state) in connections.iter_mut() {
                        if now.duration_since(state.last_send) >= interval {
                            if let Err(e) = state.writer.write_frame(&heartbeat_frame) {
                                tracing::debug!(
                                    connection_id = conn_id,
                                    error = %e,
                                    "heartbeat write error, dropping connection"
                                );
                                failed.push(conn_id);
                                continue;
                            }
                            if let Err(e) = state.writer.flush() {
                                tracing::debug!(
                                    connection_id = conn_id,
                                    error = %e,
                                    "heartbeat flush error, dropping connection"
                                );
                                failed.push(conn_id);
                                continue;
                            }
                            state.last_send = now;
                        }
                    }
                    for conn_id in failed {
                        connections.remove(&conn_id);
                        active_connections.fetch_sub(1, Ordering::Relaxed);
                    }
                }
            }
            #[cfg(feature = "pipeline-stats")]
            {
                idle_count += 1;
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
        #[cfg(feature = "pipeline-stats")]
        {
            busy_count += 1;
        }

        #[cfg(feature = "latency-trace")]
        let consume_ts = trace::trace_ts();

        // Wait for both journal AND replication to confirm the entire batch.
        // Find the highest input_seq in the batch and wait once, rather
        // than spin-waiting per event. This eliminates redundant atomic
        // loads when the batch contains many events from different clients.
        //
        // The effective cursor is min(journal_cursor, replication_cursor).
        // When replication is disabled, replication_cursor = u64::MAX so
        // min(journal, MAX) = journal — no change in behavior.
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
                    let journal_pos = journal_cursor.get().load(Ordering::Acquire);
                    let repl_pos = replication_cursor.load(Ordering::Acquire);
                    cached_journal_pos = journal_pos.min(repl_pos);
                    if cached_journal_pos >= needed {
                        break;
                    }
                    std::hint::spin_loop();
                }
            }
        }

        // One Instant::now() per batch for heartbeat tracking instead of
        // per response — heartbeat interval is 10s, sub-ms precision is plenty.
        let batch_now = Instant::now();

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

            if let Some(state) = connections.get_mut(&slot.connection_id) {
                // Encode the response directly to wire format.
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

                // write_frame expects the payload (tag + fields), not the
                // length prefix. encode_response writes [length(4) | tag+payload].
                if let Err(e) = state.writer.write_frame(&encode_buf[4..written]) {
                    tracing::debug!(
                        connection_id = slot.connection_id,
                        error = %e,
                        "write error, dropping connection"
                    );
                    connections.remove(&slot.connection_id);
                    active_connections.fetch_sub(1, Ordering::Relaxed);
                    continue;
                }

                state.last_send = batch_now;
                dirty_connections.insert(slot.connection_id);

                // Record server-side end-to-end: reader recv → response flush.
                #[cfg(feature = "latency-trace")]
                if matches!(kind, ResponseKind::BatchEnd) {
                    server_e2e_hist
                        .record_ns(trace::trace_elapsed_ns(slot.recv_ts, trace::trace_ts()));
                }
            }
        }

        #[cfg(feature = "latency-trace")]
        dispatch_hist.record_ns(trace::trace_elapsed_ns(consume_ts, trace::trace_ts()));
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
