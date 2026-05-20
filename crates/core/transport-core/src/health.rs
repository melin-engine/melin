//! Health/liveness endpoint — plain TCP listener on a dedicated port.
//!
//! Supports three response modes based on the incoming request:
//!
//! 1. **Plain TCP** (no data sent): writes a one-line status and closes.
//!    Backward-compatible with Kubernetes TCP probes and `nc`.
//! 2. **HTTP `GET /`**: wraps the one-line status in an HTTP 200 response.
//! 3. **HTTP `GET /metrics`**: returns Prometheus text exposition format with
//!    all pipeline and replication counters.
//!
//! ## Plain-text response format
//!
//! ```text
//! OK <active_connections> <journal_seq> <replication_lag> trading|halted\n
//! ```
//!
//! Returns `ERR` instead of `OK` when the pipeline is unhealthy (a thread
//! panicked or the server is shutting down).
//!
//! - `active_connections`: currently authenticated client connections
//! - `journal_seq`: latest durable journal sequence number
//! - `replication_lag`: `journal_seq - replication_cursor` (0 in standalone)

use std::io::{Cursor, Read as _, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

use melin_disruptor::padding::Sequence;
use melin_disruptor::ring::QueueCursor;
use tracing::{debug, error, info};

use crate::pipeline::{INPUT_RING_CAPACITY, StageUtilization};

/// Shared monitoring state passed to the health loop.
/// Bundles all the atomics/cursors into one struct to avoid parameter explosion.
pub struct HealthState {
    pub active_connections: Arc<AtomicU64>,
    pub events_processed: Arc<AtomicU64>,
    pub journal_cursor: Arc<Sequence>,
    pub matching_cursor: Arc<Sequence>,
    pub input_cursor: Box<dyn QueueCursor>,
    pub replication_cursor: Arc<AtomicU64>,
    pub pipeline_healthy: Arc<AtomicBool>,
    pub replicas_connected: Option<Arc<AtomicU32>>,
    /// Per-replica replication metrics. None in standalone mode.
    pub replication_metrics: Option<Arc<crate::replication::ReplicationMetrics>>,
    /// Per-slot replication-ring producer cursors. Paired index-wise with
    /// `replication_ring_consumer_cursors` to compute per-slot ring depth
    /// (producer - consumer). `None` in standalone mode.
    pub replication_ring_producer_cursors: Option<[Arc<dyn QueueCursor>; 2]>,
    /// Per-slot replication-ring consumer progress counters. See above.
    pub replication_ring_consumer_cursors: Option<[Arc<Sequence>; 2]>,
    /// The "fastest replica" cursor — `max(slot_acked[0], slot_acked[1])`,
    /// maintained by the replication sender. Stored as `u64::MAX` when no
    /// replica has engaged yet. `None` in standalone mode.
    pub fastest_replica_cursor: Option<Arc<AtomicU64>>,
    /// Per-stage busy/idle utilization counters.
    pub journal_utilization: Arc<StageUtilization>,
    pub matching_utilization: Arc<StageUtilization>,
    pub response_utilization: Arc<StageUtilization>,
}

/// Spawn the health endpoint thread. Returns the join handle.
///
/// Binds a TCP listener on `bind_addr` and accepts connections in a loop.
/// Each connection receives a one-line status response and is closed.
/// The thread exits when `shutdown` is set to true.
///
/// `pipeline_healthy` should be set to `true` at startup and flipped to
/// `false` by the accept loop when a pipeline thread dies or on shutdown.
pub fn spawn(
    bind_addr: SocketAddr,
    state: HealthState,
    shutdown: Arc<AtomicBool>,
) -> Result<std::thread::JoinHandle<()>, std::io::Error> {
    let listener = TcpListener::bind(bind_addr)?;
    // Non-blocking so we can check the shutdown flag periodically.
    listener.set_nonblocking(true)?;

    info!(addr = %bind_addr, "health endpoint listening");

    let handle = std::thread::Builder::new()
        .name("health".into())
        .spawn(move || {
            health_loop(&listener, &state, &shutdown);
        })
        .expect("failed to spawn health thread");

    Ok(handle)
}

/// Snapshot of all health metrics — collected once per connection to avoid
/// duplicate atomic reads between the plain-text and Prometheus formatters.
struct HealthSnapshot {
    healthy: bool,
    active_connections: u64,
    events_processed: u64,
    journal_seq: u64,
    replication_lag: u64,
    input_queue_depth: u64,
    trading: bool,
    /// Number of replicas currently connected. 0 in standalone mode.
    replicas_connected: u32,
    /// Per-replica lag: journal_seq - acked_sequence (0 if no ack yet).
    /// Fixed-size array for up to 2 replica slots.
    per_replica_lag: [u64; 2],
    /// Per-replica cumulative bytes sent.
    per_replica_bytes_sent: [u64; 2],
    /// Per-replica ack round-trip latency in microseconds.
    per_replica_ack_latency_us: [u64; 2],
    /// Per-replica catch-up state.
    per_replica_catching_up: [bool; 2],
    /// Per-replica last acked sequence number.
    per_replica_acked_sequence: [u64; 2],
    /// Per-replica last in-memory sequence number (highest seq the
    /// replica has accepted into its input ring, pre-journal). Always
    /// `>= per_replica_acked_sequence` under correct operation —
    /// inversion or equality under sustained traffic indicates a
    /// namespace-translation bug between local-ring positions and
    /// primary sequences.
    per_replica_in_memory_sequence: [u64; 2],
    /// Per-slot replication-ring depth: producer_cursor - consumer.processed.
    /// 0 in standalone mode or when ring cursors aren't available.
    per_replica_ring_depth: [u64; 2],
    /// Fastest-replica cursor (max of slot_acked values). 0 when no replica
    /// has engaged — the sentinel `u64::MAX` is mapped to 0 for plotting.
    fastest_replica_cursor: u64,
    /// Total replica eviction count.
    evictions_total: u64,
    /// Per-stage busy/idle iteration counters for utilization monitoring.
    /// Monotonic counters — Prometheus `rate()` gives utilization over any window.
    journal_busy: u64,
    journal_idle: u64,
    matching_busy: u64,
    matching_idle: u64,
    response_busy: u64,
    response_idle: u64,
    /// Response gate-wait events where the journal cursor was the bottleneck.
    response_gate_journal: u64,
    /// Response gate-wait events where the replication cursor was the bottleneck.
    response_gate_replication: u64,
    /// Whether the durability policy was last evaluated as degraded —
    /// at least one degrade-friendly clause was clamped below its
    /// target node count. Trips when a replica disconnects from a
    /// 2-of-3 cluster running `persisted>=2 best_effort`, etc. Operator alerting
    /// should fire on this transitioning to `true`.
    response_policy_degraded: bool,
}

impl HealthSnapshot {
    /// Collect a snapshot from the shared atomics.
    fn collect(state: &HealthState) -> Self {
        let healthy = state.pipeline_healthy.load(Ordering::Relaxed);
        let conns = state.active_connections.load(Ordering::Relaxed);
        let evts = state.events_processed.load(Ordering::Relaxed);
        let journal_seq = state.journal_cursor.get().load(Ordering::Relaxed);
        let repl_cursor = state.replication_cursor.load(Ordering::Relaxed);

        // Input queue depth: producer_cursor - matching_cursor.
        // Matching is the terminal consumer (gated on journal), so this
        // is the total pending items in the input disruptor.
        let producer_seq = state.input_cursor.load();
        let matching_seq = state.matching_cursor.get().load(Ordering::Relaxed);
        let input_queue_depth = producer_seq.saturating_sub(matching_seq);

        // Replication lag: 0 in standalone mode (cursor is u64::MAX).
        let replication_lag = if repl_cursor == u64::MAX {
            0
        } else {
            journal_seq.saturating_sub(repl_cursor)
        };

        // Trading state: "trading" when standalone or at least one replica
        // connected, "halted" when replication is enabled but all replicas
        // are disconnected.
        let trading = state
            .replicas_connected
            .as_ref()
            .is_none_or(|count| count.load(Ordering::Relaxed) > 0);

        // Per-replica metrics from the replication sender (if enabled).
        let replicas_connected_val = state
            .replicas_connected
            .as_ref()
            .map_or(0, |c| c.load(Ordering::Relaxed));

        type ReplMetricsTuple = (
            [u64; 2],
            [u64; 2],
            [u64; 2],
            [u64; 2],
            [u64; 2],
            [bool; 2],
            u64,
        );
        let (
            per_replica_acked_sequence,
            per_replica_in_memory_sequence,
            per_replica_lag,
            per_replica_bytes_sent,
            per_replica_ack_latency_us,
            per_replica_catching_up,
            evictions_total,
        ): ReplMetricsTuple = if let Some(ref rm) = state.replication_metrics {
            let acked = [
                rm.acked_sequence[0].load(Ordering::Relaxed),
                rm.acked_sequence[1].load(Ordering::Relaxed),
            ];
            let in_memory = [
                rm.in_memory_sequence[0].load(Ordering::Relaxed),
                rm.in_memory_sequence[1].load(Ordering::Relaxed),
            ];
            let lag = [
                if acked[0] == 0 {
                    0
                } else {
                    journal_seq.saturating_sub(acked[0])
                },
                if acked[1] == 0 {
                    0
                } else {
                    journal_seq.saturating_sub(acked[1])
                },
            ];
            let bytes = [
                rm.bytes_sent[0].load(Ordering::Relaxed),
                rm.bytes_sent[1].load(Ordering::Relaxed),
            ];
            let latency = [
                rm.ack_latency_us[0].load(Ordering::Relaxed),
                rm.ack_latency_us[1].load(Ordering::Relaxed),
            ];
            let catching = [
                rm.catching_up[0].load(Ordering::Relaxed),
                rm.catching_up[1].load(Ordering::Relaxed),
            ];
            let evictions = rm.evictions_total.load(Ordering::Relaxed);
            (acked, in_memory, lag, bytes, latency, catching, evictions)
        } else {
            ([0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [false, false], 0)
        };

        // Per-slot replication ring depth: producer_cursor - consumer.processed.
        // Zero when cursors aren't wired (standalone mode). `saturating_sub`
        // tolerates the benign race where the consumer side is read a hair
        // after the producer — never produces underflow.
        let per_replica_ring_depth = match (
            state.replication_ring_producer_cursors.as_ref(),
            state.replication_ring_consumer_cursors.as_ref(),
        ) {
            (Some(prods), Some(cons)) => [
                prods[0]
                    .load()
                    .saturating_sub(cons[0].get().load(Ordering::Relaxed)),
                prods[1]
                    .load()
                    .saturating_sub(cons[1].get().load(Ordering::Relaxed)),
            ],
            _ => [0, 0],
        };

        // Fastest-replica cursor. Mapped from the `u64::MAX` sentinel
        // (no replica engaged yet / all disconnected) to 0 so the
        // plotted series stays on-scale.
        let fastest_replica_cursor = state
            .fastest_replica_cursor
            .as_ref()
            .map(|c| c.load(Ordering::Relaxed))
            .map(|v| if v == u64::MAX { 0 } else { v })
            .unwrap_or(0);

        Self {
            healthy,
            active_connections: conns,
            events_processed: evts,
            journal_seq,
            replication_lag,
            input_queue_depth,
            trading,
            replicas_connected: replicas_connected_val,
            per_replica_lag,
            per_replica_bytes_sent,
            per_replica_ack_latency_us,
            per_replica_catching_up,
            per_replica_acked_sequence,
            per_replica_in_memory_sequence,
            per_replica_ring_depth,
            fastest_replica_cursor,
            evictions_total,
            journal_busy: state.journal_utilization.busy.load(Ordering::Relaxed),
            journal_idle: state.journal_utilization.idle.load(Ordering::Relaxed),
            matching_busy: state.matching_utilization.busy.load(Ordering::Relaxed),
            matching_idle: state.matching_utilization.idle.load(Ordering::Relaxed),
            response_busy: state.response_utilization.busy.load(Ordering::Relaxed),
            response_idle: state.response_utilization.idle.load(Ordering::Relaxed),
            response_gate_journal: state
                .response_utilization
                .gate_journal
                .load(Ordering::Relaxed),
            response_gate_replication: state
                .response_utilization
                .gate_replication
                .load(Ordering::Relaxed),
            response_policy_degraded: state
                .response_utilization
                .policy_degraded
                .load(Ordering::Relaxed),
        }
    }

    /// Write the one-line status into `buf`. Returns bytes written.
    fn write_status_line(&self, buf: &mut [u8]) -> usize {
        let status = if self.healthy { "OK" } else { "ERR" };
        let trading = if self.trading { "trading" } else { "halted" };
        let mut c = Cursor::new(buf);
        let _ = writeln!(
            c,
            "{status} {} {} {} {trading}",
            self.active_connections, self.journal_seq, self.replication_lag
        );
        c.position() as usize
    }

    /// Write the Prometheus text exposition body into `buf`. Returns bytes written.
    fn write_prometheus(&self, buf: &mut [u8]) -> usize {
        let healthy_val: u8 = if self.healthy { 1 } else { 0 };
        let trading_val: u8 = if self.trading { 1 } else { 0 };
        let catching_0: u8 = if self.per_replica_catching_up[0] {
            1
        } else {
            0
        };
        let catching_1: u8 = if self.per_replica_catching_up[1] {
            1
        } else {
            0
        };
        let mut c = Cursor::new(buf);
        let _ = write!(
            c,
            "# HELP melin_active_connections Current authenticated client connections.\n\
             # TYPE melin_active_connections gauge\n\
             melin_active_connections {}\n\
             # HELP melin_events_processed Total events processed by the matching engine.\n\
             # TYPE melin_events_processed counter\n\
             melin_events_processed {}\n\
             # HELP melin_journal_sequence Latest durable journal sequence number.\n\
             # TYPE melin_journal_sequence counter\n\
             melin_journal_sequence {}\n\
             # HELP melin_replication_lag Journal sequence minus replication cursor.\n\
             # TYPE melin_replication_lag gauge\n\
             melin_replication_lag {}\n\
             # HELP melin_pipeline_healthy Whether the pipeline is healthy (1) or degraded (0).\n\
             # TYPE melin_pipeline_healthy gauge\n\
             melin_pipeline_healthy {}\n\
             # HELP melin_input_queue_depth Items pending in the input disruptor.\n\
             # TYPE melin_input_queue_depth gauge\n\
             melin_input_queue_depth {}\n\
             # HELP melin_input_queue_capacity Total input ring buffer capacity.\n\
             # TYPE melin_input_queue_capacity gauge\n\
             melin_input_queue_capacity {}\n\
             # HELP melin_trading_active Whether the engine is accepting orders (1) or halted (0).\n\
             # TYPE melin_trading_active gauge\n\
             melin_trading_active {}\n\
             # HELP melin_replicas_connected Number of replicas currently connected.\n\
             # TYPE melin_replicas_connected gauge\n\
             melin_replicas_connected {}\n\
             # HELP melin_replica_acked_sequence Last sequence acked by each replica slot (persisted to journal).\n\
             # TYPE melin_replica_acked_sequence gauge\n\
             melin_replica_acked_sequence{{slot=\"0\"}} {}\n\
             melin_replica_acked_sequence{{slot=\"1\"}} {}\n\
             # HELP melin_replica_in_memory_sequence Last sequence the replica has accepted into its input ring (pre-journal).\n\
             # TYPE melin_replica_in_memory_sequence gauge\n\
             melin_replica_in_memory_sequence{{slot=\"0\"}} {}\n\
             melin_replica_in_memory_sequence{{slot=\"1\"}} {}\n\
             # HELP melin_replica_lag Per-replica replication lag (journal_seq - acked_sequence).\n\
             # TYPE melin_replica_lag gauge\n\
             melin_replica_lag{{slot=\"0\"}} {}\n\
             melin_replica_lag{{slot=\"1\"}} {}\n\
             # HELP melin_replica_bytes_sent_total Cumulative bytes sent to each replica.\n\
             # TYPE melin_replica_bytes_sent_total counter\n\
             melin_replica_bytes_sent_total{{slot=\"0\"}} {}\n\
             melin_replica_bytes_sent_total{{slot=\"1\"}} {}\n\
             # HELP melin_replica_ack_latency_us Ack round-trip latency per replica in microseconds.\n\
             # TYPE melin_replica_ack_latency_us gauge\n\
             melin_replica_ack_latency_us{{slot=\"0\"}} {}\n\
             melin_replica_ack_latency_us{{slot=\"1\"}} {}\n\
             # HELP melin_replica_catching_up Whether each replica is catching up from journal (1) or live (0).\n\
             # TYPE melin_replica_catching_up gauge\n\
             melin_replica_catching_up{{slot=\"0\"}} {}\n\
             melin_replica_catching_up{{slot=\"1\"}} {}\n\
             # HELP melin_replica_evictions_total Total replica evictions due to ring backpressure.\n\
             # TYPE melin_replica_evictions_total counter\n\
             melin_replica_evictions_total {}\n\
             # HELP melin_replication_ring_depth Per-slot replication-ring depth (producer_cursor - consumer.processed).\n\
             # TYPE melin_replication_ring_depth gauge\n\
             melin_replication_ring_depth{{slot=\"0\"}} {}\n\
             melin_replication_ring_depth{{slot=\"1\"}} {}\n\
             # HELP melin_fastest_replica_cursor Highest acked sequence across replica slots (0 when none engaged).\n\
             # TYPE melin_fastest_replica_cursor gauge\n\
             melin_fastest_replica_cursor {}\n\
             # HELP melin_stage_busy_total Cumulative busy iterations per pipeline stage (journal/response: batches, matching: events).\n\
             # TYPE melin_stage_busy_total counter\n\
             melin_stage_busy_total{{stage=\"journal\"}} {}\n\
             melin_stage_busy_total{{stage=\"matching\"}} {}\n\
             melin_stage_busy_total{{stage=\"response\"}} {}\n\
             # HELP melin_stage_idle_total Cumulative idle iterations per pipeline stage.\n\
             # TYPE melin_stage_idle_total counter\n\
             melin_stage_idle_total{{stage=\"journal\"}} {}\n\
             melin_stage_idle_total{{stage=\"matching\"}} {}\n\
             melin_stage_idle_total{{stage=\"response\"}} {}\n\
             # HELP melin_response_gate_total Gate-wait events by bottleneck (journal fsync vs replica ack).\n\
             # TYPE melin_response_gate_total counter\n\
             melin_response_gate_total{{blocker=\"journal\"}} {}\n\
             melin_response_gate_total{{blocker=\"replication\"}} {}\n\
             # HELP melin_durability_policy_degraded Durability policy currently clamped below its target node count (1 = degraded, 0 = healthy).\n\
             # TYPE melin_durability_policy_degraded gauge\n\
             melin_durability_policy_degraded {}\n",
            self.active_connections,
            self.events_processed,
            self.journal_seq,
            self.replication_lag,
            healthy_val,
            self.input_queue_depth,
            INPUT_RING_CAPACITY,
            trading_val,
            self.replicas_connected,
            self.per_replica_acked_sequence[0],
            self.per_replica_acked_sequence[1],
            self.per_replica_in_memory_sequence[0],
            self.per_replica_in_memory_sequence[1],
            self.per_replica_lag[0],
            self.per_replica_lag[1],
            self.per_replica_bytes_sent[0],
            self.per_replica_bytes_sent[1],
            self.per_replica_ack_latency_us[0],
            self.per_replica_ack_latency_us[1],
            catching_0,
            catching_1,
            self.evictions_total,
            self.per_replica_ring_depth[0],
            self.per_replica_ring_depth[1],
            self.fastest_replica_cursor,
            self.journal_busy,
            self.matching_busy,
            self.response_busy,
            self.journal_idle,
            self.matching_idle,
            self.response_idle,
            self.response_gate_journal,
            self.response_gate_replication,
            if self.response_policy_degraded { 1 } else { 0 },
        );
        c.position() as usize
    }
}

/// What kind of request the client sent.
enum RequestKind {
    /// No data within timeout — plain TCP probe (e.g., `nc`, Kubernetes TCP check).
    PlainTcp,
    /// HTTP GET / — serve the one-line status wrapped in HTTP.
    HttpHealth,
    /// HTTP GET /metrics — serve Prometheus text exposition format.
    Metrics,
    /// HTTP GET /stats-dump — serve the bench's tick-to-trade per-stage
    /// histogram dump from the latency-trace registry. Empty body when
    /// the server was built without `--features latency-trace`.
    StatsDump,
}

/// Peek at the first bytes to detect HTTP vs plain TCP.
///
/// Strategy: try a non-blocking read first. If data is already buffered
/// (HTTP client sent request before we accepted), we classify immediately
/// with zero delay. Only if the non-blocking read returns WouldBlock do
/// we fall back to a short blocking read — 5ms is enough for loopback
/// HTTP headers to arrive, and keeps plain TCP probes fast (~5ms worst
/// case instead of the old 50ms).
fn detect_request(stream: &mut TcpStream) -> RequestKind {
    // 16 bytes is enough to distinguish "GET /m" from "GET /" from nothing.
    let mut buf = [0u8; 16];

    // First try: non-blocking. Data is usually already in the kernel
    // buffer by the time we accept() the connection.
    let _ = stream.set_nonblocking(true);
    let n = match stream.read(&mut buf) {
        Ok(n) => n,
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
            // No data yet — fall back to a short blocking wait.
            // 5ms is generous for loopback; plain TCP probes (nc, k8s)
            // never send data, so this is their worst-case delay.
            let _ = stream.set_nonblocking(false);
            let _ = stream.set_read_timeout(Some(Duration::from_millis(5)));
            match stream.read(&mut buf) {
                Ok(n) => n,
                Err(_) => return RequestKind::PlainTcp,
            }
        }
        Err(_) => return RequestKind::PlainTcp,
    };

    let data = &buf[..n];
    // Prefix matches use 6 bytes (`GET /` + 1 path byte) so that a
    // short non-blocking read still classifies correctly. `/m` and
    // `/s` are the only documented two paths beyond `/`; an
    // undocumented path beginning with `m` or `s` would be
    // misclassified, but no other paths are exposed.
    let kind = if data.starts_with(b"GET /m") {
        RequestKind::Metrics
    } else if data.starts_with(b"GET /s") {
        RequestKind::StatsDump
    } else if data.starts_with(b"GET /") {
        RequestKind::HttpHealth
    } else {
        return RequestKind::PlainTcp;
    };

    // Drain remaining HTTP request data so close() doesn't RST the connection.
    // HTTP clients send headers beyond our 16-byte peek; leaving unread data
    // in the recv buffer causes the kernel to send RST instead of FIN.
    // Cap at 4 KiB to prevent a malicious client from holding the health thread.
    let mut discard = [0u8; 512];
    let mut drained = 0usize;
    while drained < 4096 {
        match stream.read(&mut discard) {
            Ok(0) | Err(_) => break,
            Ok(n) => drained += n,
        }
    }

    kind
}

/// Write the latency-trace stage histograms into `buf` as one
/// tab-separated record per non-empty stage. Returns bytes written.
///
/// Format (one line per stage, '\n'-terminated):
///
///   stage\t<name>\t<samples>\t<min_ns>\t<p50_ns>\t<p90_ns>\t<p99_ns>\t<p99_9_ns>\t<max_ns>
///
/// Tab as the field delimiter so stage names containing spaces / colons
/// / parens parse unambiguously. The bench (phase 3) parses this and
/// merges with its own RTT histograms for the tick-to-trade table.
///
/// When `latency-trace` is disabled the body is a single comment line
/// so the bench can detect the unsupported state without a different
/// HTTP status code.
fn write_stats_dump(buf: &mut [u8]) -> usize {
    let mut c = Cursor::new(buf);

    #[cfg(feature = "latency-trace")]
    {
        let snaps = crate::trace::global_registry().snapshot_all();
        if snaps.is_empty() {
            // Feature on but no samples yet — explicit empty marker so
            // the bench doesn't confuse it with a feature-off server.
            let _ = writeln!(c, "# no samples");
        } else {
            for s in snaps {
                let _ = writeln!(
                    c,
                    "stage\t{name}\t{samples}\t{min}\t{p50}\t{p90}\t{p99}\t{p99_9}\t{max}",
                    name = s.name,
                    samples = s.samples,
                    min = s.min_ns,
                    p50 = s.p50_ns,
                    p90 = s.p90_ns,
                    p99 = s.p99_ns,
                    p99_9 = s.p99_9_ns,
                    max = s.max_ns,
                );
            }
        }
    }
    #[cfg(not(feature = "latency-trace"))]
    {
        let _ = writeln!(c, "# latency-trace disabled");
    }

    c.position() as usize
}

/// Write HTTP header + body into `buf`. Returns total bytes written.
fn write_http(buf: &mut [u8], content_type: &str, body: &[u8]) -> usize {
    let mut c = Cursor::new(buf);
    let _ = write!(
        c,
        "HTTP/1.1 200 OK\r\n\
         Content-Type: {content_type}\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n",
        body.len()
    );
    let _ = c.write_all(body);
    c.position() as usize
}

/// Main health endpoint loop. Accepts connections and writes status.
fn health_loop(listener: &TcpListener, state: &HealthState, shutdown: &AtomicBool) {
    while !shutdown.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((stream, addr)) => {
                debug!(addr = %addr, "health check");
                handle_health_connection(stream, state);
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                // No pending connection — sleep briefly then retry.
                // 100ms is fine for health checks (they're infrequent).
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(e) => {
                error!(error = %e, "health accept error");
            }
        }
    }
}

/// Collect snapshot, detect request kind, write the appropriate response.
/// Best-effort — errors are debug-logged but don't affect the server.
///
/// Zero heap allocations — all formatting uses stack buffers.
fn handle_health_connection(mut stream: TcpStream, state: &HealthState) {
    // Short write timeout — health probes should not block the thread.
    let _ = stream.set_write_timeout(Some(Duration::from_secs(1)));

    let snapshot = HealthSnapshot::collect(state);

    let kind = detect_request(&mut stream);

    // Stack buffers — sized for the largest body we serve.
    // - Prometheus body is ~3.5 KiB with max-length u64 values
    //   (includes per-replica replication metrics, ring depth, and
    //   the fastest-replica cursor).
    // - StatsDump body is ~260 bytes per registered stage; current
    //   set is 9–13 stages (transport-dependent) for ~3.5 KiB tops.
    //   8 KiB gives headroom for future stages without resizing.
    // Response = body + HTTP headers (~200 bytes).
    let mut body_buf = [0u8; 8192];
    let mut resp_buf = [0u8; 8448];

    let resp_len = match kind {
        RequestKind::Metrics => {
            let body_len = snapshot.write_prometheus(&mut body_buf);
            write_http(
                &mut resp_buf,
                "text/plain; version=0.0.4; charset=utf-8",
                &body_buf[..body_len],
            )
        }
        RequestKind::HttpHealth => {
            let body_len = snapshot.write_status_line(&mut body_buf);
            write_http(
                &mut resp_buf,
                "text/plain; charset=utf-8",
                &body_buf[..body_len],
            )
        }
        RequestKind::StatsDump => {
            let body_len = write_stats_dump(&mut body_buf);
            write_http(
                &mut resp_buf,
                "text/tab-separated-values; charset=utf-8",
                &body_buf[..body_len],
            )
        }
        RequestKind::PlainTcp => snapshot.write_status_line(&mut resp_buf),
    };

    if let Err(e) = stream.write_all(&resp_buf[..resp_len]) {
        debug!(error = %e, "health write failed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::ReplicationMetrics;
    use std::io::Read;

    /// Test-only QueueCursor backed by an AtomicU64.
    struct MockCursor(AtomicU64);
    impl QueueCursor for MockCursor {
        fn load(&self) -> u64 {
            self.0.load(Ordering::Relaxed)
        }
    }

    /// Helper: create a non-blocking listener and spawn the health loop.
    /// Returns (addr, events_processed, pipeline_healthy, shutdown_flag, join_handle).
    /// `replicas_connected` is None (standalone mode) unless overridden.
    fn start_health(
        active: u64,
        journal_seq: u64,
        repl_cursor: u64,
    ) -> (
        SocketAddr,
        Arc<AtomicU64>,
        Arc<AtomicBool>,
        Arc<AtomicBool>,
        std::thread::JoinHandle<()>,
    ) {
        start_health_with_replica(active, journal_seq, repl_cursor, None)
    }

    /// Like `start_health` but with an explicit `replicas_connected` flag.
    fn start_health_with_replica(
        active: u64,
        journal_seq: u64,
        repl_cursor: u64,
        replicas_connected: Option<Arc<AtomicU32>>,
    ) -> (
        SocketAddr,
        Arc<AtomicU64>,
        Arc<AtomicBool>,
        Arc<AtomicBool>,
        std::thread::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();

        let active = Arc::new(AtomicU64::new(active));
        let events = Arc::new(AtomicU64::new(0));
        let journal = Arc::new(Sequence::new(AtomicU64::new(journal_seq)));
        // Matching cursor = journal_seq (fully caught up) for most tests.
        let matching = Arc::new(Sequence::new(AtomicU64::new(journal_seq)));
        let repl = Arc::new(AtomicU64::new(repl_cursor));
        let healthy = Arc::new(AtomicBool::new(true));
        let shutdown = Arc::new(AtomicBool::new(false));

        let s = Arc::clone(&shutdown);
        let state = HealthState {
            active_connections: active,
            events_processed: Arc::clone(&events),
            journal_cursor: journal,
            matching_cursor: matching,
            // Input cursor = journal_seq (empty queue) for most tests.
            input_cursor: Box::new(MockCursor(AtomicU64::new(journal_seq))),
            replication_cursor: repl,
            pipeline_healthy: Arc::clone(&healthy),
            replicas_connected,
            replication_metrics: None,
            replication_ring_producer_cursors: None,
            replication_ring_consumer_cursors: None,
            fastest_replica_cursor: None,
            journal_utilization: Arc::new(StageUtilization::new()),
            matching_utilization: Arc::new(StageUtilization::new()),
            response_utilization: Arc::new(StageUtilization::new()),
        };

        let handle = std::thread::spawn(move || {
            health_loop(&listener, &state, &s);
        });

        (addr, events, healthy, shutdown, handle)
    }

    /// Read the full response from a health connection (plain TCP, no request sent).
    fn read_health(addr: SocketAddr) -> String {
        let mut client = TcpStream::connect(addr).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut buf = String::new();
        client.read_to_string(&mut buf).unwrap();
        buf
    }

    /// Send an HTTP request and read the full response.
    fn http_request(addr: SocketAddr, request: &str) -> String {
        let mut client = TcpStream::connect(addr).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        client.write_all(request.as_bytes()).unwrap();
        // Shut down write side so the server's drain sees EOF immediately
        // instead of blocking until the 50ms read timeout expires.
        client.shutdown(std::net::Shutdown::Write).unwrap();
        let mut buf = String::new();
        client.read_to_string(&mut buf).unwrap();
        buf
    }

    #[test]
    fn plain_tcp_backward_compatible() {
        // Connect without sending any data → raw one-line status (no HTTP headers).
        let (addr, _events, _healthy, shutdown, handle) = start_health(5, 42, 40);

        let buf = read_health(addr);
        assert_eq!(buf, "OK 5 42 2 trading\n");

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn health_standalone_replication_lag_is_zero() {
        // Standalone mode: replication cursor is u64::MAX → lag = 0.
        let (addr, _events, _healthy, shutdown, handle) = start_health(0, 100, u64::MAX);

        let buf = read_health(addr);
        assert_eq!(buf, "OK 0 100 0 trading\n");

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn health_multiple_connections() {
        let (addr, _events, _healthy, shutdown, handle) = start_health(10, 0, u64::MAX);

        // Multiple sequential health checks should all succeed.
        for _ in 0..3 {
            let buf = read_health(addr);
            assert!(buf.starts_with("OK "), "unexpected response: {buf}");
        }

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn health_returns_err_when_pipeline_unhealthy() {
        let (addr, _events, healthy, shutdown, handle) = start_health(3, 50, u64::MAX);

        // Healthy pipeline returns OK.
        let buf = read_health(addr);
        assert!(buf.starts_with("OK "), "expected OK, got: {buf}");

        // Mark pipeline unhealthy (simulates thread panic detection).
        healthy.store(false, Ordering::Relaxed);

        let buf = read_health(addr);
        assert_eq!(buf, "ERR 3 50 0 trading\n");

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn health_shutdown_stops_loop() {
        let (_addr, _events, _healthy, shutdown, handle) = start_health(0, 0, u64::MAX);

        // Signal shutdown — thread should exit within ~200ms.
        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn spawn_end_to_end() {
        // Test the public `spawn` API (bind + thread + accept + respond).
        let active = Arc::new(AtomicU64::new(7));
        let events = Arc::new(AtomicU64::new(0));
        let journal = Arc::new(Sequence::new(AtomicU64::new(99)));
        let matching = Arc::new(Sequence::new(AtomicU64::new(99)));
        let repl = Arc::new(AtomicU64::new(u64::MAX));
        let healthy = Arc::new(AtomicBool::new(true));
        let shutdown = Arc::new(AtomicBool::new(false));

        let handle = spawn(
            "127.0.0.1:0".parse().unwrap(),
            HealthState {
                active_connections: Arc::clone(&active),
                events_processed: Arc::clone(&events),
                journal_cursor: Arc::clone(&journal),
                matching_cursor: Arc::clone(&matching),
                input_cursor: Box::new(MockCursor(AtomicU64::new(99))),
                replication_cursor: Arc::clone(&repl),
                pipeline_healthy: Arc::clone(&healthy),
                replicas_connected: None,
                replication_metrics: None,
                replication_ring_producer_cursors: None,
                replication_ring_consumer_cursors: None,
                fastest_replica_cursor: None,
                journal_utilization: Arc::new(StageUtilization::new()),
                matching_utilization: Arc::new(StageUtilization::new()),
                response_utilization: Arc::new(StageUtilization::new()),
            },
            Arc::clone(&shutdown),
        );
        // spawn binds to port 0 which is auto-assigned — we can't know the
        // port, so this test just verifies it doesn't panic or error.
        // For a full round-trip, use start_health (which gives us the addr).
        assert!(handle.is_ok());
        shutdown.store(true, Ordering::Relaxed);
        handle.unwrap().join().unwrap();
    }

    #[test]
    fn spawn_bind_failure_returns_error() {
        // Bind to the same port twice — second should fail.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let result = spawn(
            addr,
            HealthState {
                active_connections: Arc::new(AtomicU64::new(0)),
                events_processed: Arc::new(AtomicU64::new(0)),
                journal_cursor: Arc::new(Sequence::new(AtomicU64::new(0))),
                matching_cursor: Arc::new(Sequence::new(AtomicU64::new(0))),
                input_cursor: Box::new(MockCursor(AtomicU64::new(0))),
                replication_cursor: Arc::new(AtomicU64::new(u64::MAX)),
                pipeline_healthy: Arc::new(AtomicBool::new(true)),
                replicas_connected: None,
                replication_metrics: None,
                replication_ring_producer_cursors: None,
                replication_ring_consumer_cursors: None,
                fastest_replica_cursor: None,
                journal_utilization: Arc::new(StageUtilization::new()),
                matching_utilization: Arc::new(StageUtilization::new()),
                response_utilization: Arc::new(StageUtilization::new()),
            },
            Arc::new(AtomicBool::new(false)),
        );
        assert!(result.is_err(), "expected bind failure on occupied port");
        drop(listener);
    }

    #[test]
    fn client_disconnect_before_reading() {
        // TCP connect-only probe: connect and immediately drop (no read).
        // The health loop should handle the broken pipe gracefully.
        let (addr, _events, _healthy, shutdown, handle) = start_health(0, 0, u64::MAX);

        for _ in 0..3 {
            let client = TcpStream::connect(addr).unwrap();
            drop(client); // immediate disconnect
        }

        // Health loop should still be alive and serving.
        let buf = read_health(addr);
        assert!(
            buf.starts_with("OK "),
            "expected OK after disconnects, got: {buf}"
        );

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn concurrent_health_checks() {
        let (addr, _events, _healthy, shutdown, handle) = start_health(2, 77, u64::MAX);

        // Spawn 5 concurrent clients.
        let threads: Vec<_> = (0..5)
            .map(|_| {
                let a = addr;
                std::thread::spawn(move || read_health(a))
            })
            .collect();

        for t in threads {
            let buf = t.join().unwrap();
            assert!(buf.starts_with("OK "), "unexpected: {buf}");
        }

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn health_shows_halted_when_replica_disconnected() {
        let replica_count = Arc::new(AtomicU32::new(0)); // no replicas connected
        let (addr, _events, _healthy, shutdown, handle) =
            start_health_with_replica(5, 100, u64::MAX, Some(Arc::clone(&replica_count)));

        let buf = read_health(addr);
        assert_eq!(buf, "OK 5 100 0 halted\n");

        // Connect a replica — should switch to trading.
        replica_count.store(1, Ordering::Relaxed);
        let buf = read_health(addr);
        assert_eq!(buf, "OK 5 100 0 trading\n");

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn metrics_response_format() {
        let (addr, events, _healthy, shutdown, handle) = start_health(5, 42, 40);
        events.store(1000, Ordering::Relaxed);

        let response = http_request(addr, "GET /metrics HTTP/1.1\r\n\r\n");

        // Verify HTTP response structure.
        assert!(
            response.starts_with("HTTP/1.1 200 OK\r\n"),
            "expected HTTP 200, got: {response}"
        );
        assert!(
            response.contains("Content-Type: text/plain; version=0.0.4; charset=utf-8"),
            "missing prometheus content type"
        );

        // Verify all 8 metric lines.
        assert!(response.contains("melin_active_connections 5\n"));
        assert!(response.contains("melin_events_processed 1000\n"));
        assert!(response.contains("melin_journal_sequence 42\n"));
        assert!(response.contains("melin_replication_lag 2\n"));
        assert!(response.contains("melin_pipeline_healthy 1\n"));
        assert!(response.contains("melin_input_queue_depth 0\n"));
        assert!(response.contains("melin_input_queue_capacity 1048576\n"));
        assert!(response.contains("melin_trading_active 1\n"));

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn metrics_boolean_encoding() {
        // Verify that unhealthy + halted → 0 values.
        let replica_count = Arc::new(AtomicU32::new(0)); // disconnected → halted
        let (addr, _events, healthy, shutdown, handle) =
            start_health_with_replica(0, 0, u64::MAX, Some(Arc::clone(&replica_count)));

        healthy.store(false, Ordering::Relaxed);

        let response = http_request(addr, "GET /metrics HTTP/1.1\r\n\r\n");
        assert!(response.contains("melin_pipeline_healthy 0\n"));
        assert!(response.contains("melin_trading_active 0\n"));

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn http_health_response() {
        let (addr, _events, _healthy, shutdown, handle) = start_health(5, 42, 40);

        let response = http_request(addr, "GET / HTTP/1.1\r\n\r\n");

        assert!(
            response.starts_with("HTTP/1.1 200 OK\r\n"),
            "expected HTTP 200, got: {response}"
        );
        assert!(
            response.contains("Content-Type: text/plain; charset=utf-8"),
            "missing content type"
        );
        assert!(
            response.contains("OK 5 42 2 trading\n"),
            "missing status line in body: {response}"
        );

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn events_processed_in_metrics() {
        let (addr, events, _healthy, shutdown, handle) = start_health(0, 0, u64::MAX);
        events.store(999_999, Ordering::Relaxed);

        let response = http_request(addr, "GET /metrics HTTP/1.1\r\n\r\n");
        assert!(
            response.contains("melin_events_processed 999999\n"),
            "events_processed not found in: {response}"
        );

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn input_queue_depth_in_metrics() {
        // Set up with producer at 1000, matching at 900 → depth = 100.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();

        let shutdown = Arc::new(AtomicBool::new(false));
        let s = Arc::clone(&shutdown);
        let state = HealthState {
            active_connections: Arc::new(AtomicU64::new(0)),
            events_processed: Arc::new(AtomicU64::new(0)),
            journal_cursor: Arc::new(Sequence::new(AtomicU64::new(1000))),
            matching_cursor: Arc::new(Sequence::new(AtomicU64::new(900))),
            input_cursor: Box::new(MockCursor(AtomicU64::new(1000))),
            replication_cursor: Arc::new(AtomicU64::new(u64::MAX)),
            pipeline_healthy: Arc::new(AtomicBool::new(true)),
            replicas_connected: None,
            replication_metrics: None,
            replication_ring_producer_cursors: None,
            replication_ring_consumer_cursors: None,
            fastest_replica_cursor: None,
            journal_utilization: Arc::new(StageUtilization::new()),
            matching_utilization: Arc::new(StageUtilization::new()),
            response_utilization: Arc::new(StageUtilization::new()),
        };

        let handle = std::thread::spawn(move || {
            health_loop(&listener, &state, &s);
        });

        let response = http_request(addr, "GET /metrics HTTP/1.1\r\n\r\n");
        assert!(
            response.contains("melin_input_queue_depth 100\n"),
            "expected depth 100, response: {response}"
        );
        assert!(
            response.contains("melin_input_queue_capacity 1048576\n"),
            "expected capacity metric, response: {response}"
        );

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn stage_utilization_in_metrics() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();

        let shutdown = Arc::new(AtomicBool::new(false));
        let s = Arc::clone(&shutdown);

        let journal_util = Arc::new(StageUtilization::new());
        journal_util.busy.store(500, Ordering::Relaxed);
        journal_util.idle.store(9500, Ordering::Relaxed);

        let matching_util = Arc::new(StageUtilization::new());
        matching_util.busy.store(2000, Ordering::Relaxed);
        matching_util.idle.store(8000, Ordering::Relaxed);

        let response_util = Arc::new(StageUtilization::new());
        // Response left at 0/0 — verifies zero counters render correctly.

        let state = HealthState {
            active_connections: Arc::new(AtomicU64::new(0)),
            events_processed: Arc::new(AtomicU64::new(0)),
            journal_cursor: Arc::new(Sequence::new(AtomicU64::new(0))),
            matching_cursor: Arc::new(Sequence::new(AtomicU64::new(0))),
            input_cursor: Box::new(MockCursor(AtomicU64::new(0))),
            replication_cursor: Arc::new(AtomicU64::new(u64::MAX)),
            pipeline_healthy: Arc::new(AtomicBool::new(true)),
            replicas_connected: None,
            replication_metrics: None,
            replication_ring_producer_cursors: None,
            replication_ring_consumer_cursors: None,
            fastest_replica_cursor: None,
            journal_utilization: journal_util,
            matching_utilization: matching_util,
            response_utilization: response_util,
        };

        let handle = std::thread::spawn(move || {
            health_loop(&listener, &state, &s);
        });

        let response = http_request(addr, "GET /metrics HTTP/1.1\r\n\r\n");
        assert!(
            response.contains("melin_stage_busy_total{stage=\"journal\"} 500\n"),
            "journal busy not found in: {response}"
        );
        assert!(
            response.contains("melin_stage_idle_total{stage=\"journal\"} 9500\n"),
            "journal idle not found in: {response}"
        );
        assert!(
            response.contains("melin_stage_busy_total{stage=\"matching\"} 2000\n"),
            "matching busy not found in: {response}"
        );
        assert!(
            response.contains("melin_stage_busy_total{stage=\"response\"} 0\n"),
            "response busy not found in: {response}"
        );

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn replication_ring_depth_and_fastest_cursor_in_metrics() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();

        let shutdown = Arc::new(AtomicBool::new(false));
        let s = Arc::clone(&shutdown);

        // Slot 0: producer at 5000, consumer at 4950 → depth 50 (backpressured).
        // Slot 1: producer = consumer at 5000 → depth 0 (caught up).
        let prod_0: Arc<dyn QueueCursor> = Arc::new(MockCursor(AtomicU64::new(5000)));
        let prod_1: Arc<dyn QueueCursor> = Arc::new(MockCursor(AtomicU64::new(5000)));
        let cons_0 = Arc::new(Sequence::new(AtomicU64::new(4950)));
        let cons_1 = Arc::new(Sequence::new(AtomicU64::new(5000)));
        let fastest = Arc::new(AtomicU64::new(4990));

        let state = HealthState {
            active_connections: Arc::new(AtomicU64::new(0)),
            events_processed: Arc::new(AtomicU64::new(0)),
            journal_cursor: Arc::new(Sequence::new(AtomicU64::new(5000))),
            matching_cursor: Arc::new(Sequence::new(AtomicU64::new(5000))),
            input_cursor: Box::new(MockCursor(AtomicU64::new(5000))),
            replication_cursor: Arc::new(AtomicU64::new(4990)),
            pipeline_healthy: Arc::new(AtomicBool::new(true)),
            replicas_connected: None,
            replication_metrics: None,
            replication_ring_producer_cursors: Some([prod_0, prod_1]),
            replication_ring_consumer_cursors: Some([cons_0, cons_1]),
            fastest_replica_cursor: Some(fastest),
            journal_utilization: Arc::new(StageUtilization::new()),
            matching_utilization: Arc::new(StageUtilization::new()),
            response_utilization: Arc::new(StageUtilization::new()),
        };

        let handle = std::thread::spawn(move || {
            health_loop(&listener, &state, &s);
        });

        let response = http_request(addr, "GET /metrics HTTP/1.1\r\n\r\n");
        assert!(
            response.contains("melin_replication_ring_depth{slot=\"0\"} 50\n"),
            "slot 0 depth not found in: {response}"
        );
        assert!(
            response.contains("melin_replication_ring_depth{slot=\"1\"} 0\n"),
            "slot 1 depth not found in: {response}"
        );
        assert!(
            response.contains("melin_fastest_replica_cursor 4990\n"),
            "fastest cursor not found in: {response}"
        );

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn fastest_replica_cursor_sentinel_mapped_to_zero() {
        // u64::MAX is the "no replica engaged" sentinel — it must render as 0
        // so it doesn't dominate the plotted y-axis or skew aggregates.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();

        let shutdown = Arc::new(AtomicBool::new(false));
        let s = Arc::clone(&shutdown);

        let state = HealthState {
            active_connections: Arc::new(AtomicU64::new(0)),
            events_processed: Arc::new(AtomicU64::new(0)),
            journal_cursor: Arc::new(Sequence::new(AtomicU64::new(0))),
            matching_cursor: Arc::new(Sequence::new(AtomicU64::new(0))),
            input_cursor: Box::new(MockCursor(AtomicU64::new(0))),
            replication_cursor: Arc::new(AtomicU64::new(u64::MAX)),
            pipeline_healthy: Arc::new(AtomicBool::new(true)),
            replicas_connected: None,
            replication_metrics: None,
            replication_ring_producer_cursors: None,
            replication_ring_consumer_cursors: None,
            fastest_replica_cursor: Some(Arc::new(AtomicU64::new(u64::MAX))),
            journal_utilization: Arc::new(StageUtilization::new()),
            matching_utilization: Arc::new(StageUtilization::new()),
            response_utilization: Arc::new(StageUtilization::new()),
        };

        let handle = std::thread::spawn(move || {
            health_loop(&listener, &state, &s);
        });

        let response = http_request(addr, "GET /metrics HTTP/1.1\r\n\r\n");
        assert!(
            response.contains("melin_fastest_replica_cursor 0\n"),
            "expected sentinel mapped to 0, got: {response}"
        );

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    // ------------------------------------------------------------------
    // STATS-DUMP — bench tick-to-trade per-stage histogram dump.
    // ------------------------------------------------------------------

    #[test]
    fn stats_dump_returns_http_with_tsv_content_type() {
        let (addr, _events, _healthy, shutdown, handle) = start_health(0, 0, u64::MAX);

        let response = http_request(addr, "GET /stats-dump HTTP/1.1\r\n\r\n");

        assert!(
            response.starts_with("HTTP/1.1 200 OK\r\n"),
            "expected HTTP 200, got: {response}"
        );
        assert!(
            response.contains("Content-Type: text/tab-separated-values"),
            "expected tab-separated-values content type, got: {response}"
        );

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[cfg(not(feature = "latency-trace"))]
    #[test]
    fn stats_dump_body_when_latency_trace_disabled() {
        // Without the feature, the body is a single comment line so
        // the bench can detect the unsupported state.
        let (addr, _events, _healthy, shutdown, handle) = start_health(0, 0, u64::MAX);

        let response = http_request(addr, "GET /stats-dump HTTP/1.1\r\n\r\n");

        assert!(
            response.contains("# latency-trace disabled"),
            "expected feature-disabled marker, got: {response}"
        );

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[cfg(feature = "latency-trace")]
    #[test]
    fn stats_dump_body_emits_registered_stages() {
        // Register a stage with deterministic samples and verify the
        // dump contains a tab-separated record for it.
        // The global registry is shared across tests; we use a unique
        // stage name to avoid collisions with concurrent test runs.
        // Recorder dropped before the snapshot fetch — see the
        // SyncHistogram caveat in `crates/core/journal/src/trace.rs` tests.
        {
            let mut rec =
                melin_transport_core::trace::register_stage("test::stats_dump_emit_marker");
            rec.record_ns(1_500);
            rec.record_ns(2_500);
            rec.record_ns(3_500);
        }

        let (addr, _events, _healthy, shutdown, handle) = start_health(0, 0, u64::MAX);
        let response = http_request(addr, "GET /stats-dump HTTP/1.1\r\n\r\n");

        // Body lines look like:
        //   stage\t<name>\t<samples>\t<min>\t<p50>\t<p90>\t<p99>\t<p99_9>\t<max>
        assert!(
            response.contains("stage\ttest::stats_dump_emit_marker\t3\t"),
            "expected stage record with 3 samples, got: {response}"
        );

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[cfg(feature = "latency-trace")]
    #[test]
    fn stats_dump_body_line_format() {
        // Pin the wire contract that phase 3's bench parser will rely
        // on: every non-comment body line is exactly 9 tab-separated
        // fields — `stage`, name, then 7 numeric percentile fields.
        // Recorder dropped before the snapshot fetch — see the
        // SyncHistogram caveat in `crates/core/journal/src/trace.rs` tests.
        {
            let mut rec =
                melin_transport_core::trace::register_stage("test::stats_dump_line_format_marker");
            rec.record_ns(1_000);
            rec.record_ns(2_000);
            rec.record_ns(3_000);
        }

        let (addr, _events, _healthy, shutdown, handle) = start_health(0, 0, u64::MAX);
        let response = http_request(addr, "GET /stats-dump HTTP/1.1\r\n\r\n");

        // Strip HTTP head, find our marker line.
        let body = response
            .split("\r\n\r\n")
            .nth(1)
            .expect("body separated by blank line");
        let line = body
            .lines()
            .find(|l| l.contains("test::stats_dump_line_format_marker"))
            .unwrap_or_else(|| panic!("marker line missing in body: {body}"));

        let fields: Vec<&str> = line.split('\t').collect();
        assert_eq!(
            fields.len(),
            9,
            "expected 9 tab-separated fields, got {}: {fields:?}",
            fields.len(),
        );
        assert_eq!(fields[0], "stage");
        assert_eq!(fields[1], "test::stats_dump_line_format_marker");
        assert_eq!(fields[2], "3");
        // Fields 3..9 are min/p50/p90/p99/p99_9/max — must parse as u64.
        for (i, f) in fields.iter().enumerate().skip(2) {
            f.parse::<u64>()
                .unwrap_or_else(|_| panic!("field {i} not a u64: {f:?}"));
        }

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[cfg(feature = "latency-trace")]
    #[test]
    fn stats_dump_body_skips_empty_stages() {
        // A stage with no samples must not appear in the dump.
        let _empty =
            melin_transport_core::trace::register_stage("test::stats_dump_empty_stage_marker");
        // No record_ns calls.

        let (addr, _events, _healthy, shutdown, handle) = start_health(0, 0, u64::MAX);
        let response = http_request(addr, "GET /stats-dump HTTP/1.1\r\n\r\n");

        assert!(
            !response.contains("test::stats_dump_empty_stage_marker"),
            "empty stage leaked into dump: {response}"
        );

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    // ------------------------------------------------------------------
    // PER-REPLICA METRICS — Prometheus output for the per-slot replication
    // counters wasn't asserted by any test before, so a rename or label
    // typo in `write_prometheus` could ship silently. One test per family.
    // ------------------------------------------------------------------

    /// Spin up the health loop with a fully-populated `ReplicationMetrics`
    /// and gauge-style auxiliary cursors, then return the Prometheus body
    /// for the caller to make assertions on. Keeps each per-family test
    /// short.
    fn prometheus_with_full_replication_state()
    -> (String, Arc<AtomicBool>, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();

        let shutdown = Arc::new(AtomicBool::new(false));
        let s = Arc::clone(&shutdown);

        // Populate every per-slot counter with a distinct value so a
        // label/index swap (e.g. printing slot 1's value under slot 0)
        // shows up as a failed assertion below.
        let metrics = Arc::new(ReplicationMetrics::default());
        metrics.acked_sequence[0].store(900, Ordering::Relaxed);
        metrics.acked_sequence[1].store(800, Ordering::Relaxed);
        metrics.in_memory_sequence[0].store(950, Ordering::Relaxed);
        metrics.in_memory_sequence[1].store(850, Ordering::Relaxed);
        metrics.bytes_sent[0].store(11_111, Ordering::Relaxed);
        metrics.bytes_sent[1].store(22_222, Ordering::Relaxed);
        metrics.ack_latency_us[0].store(33, Ordering::Relaxed);
        metrics.ack_latency_us[1].store(44, Ordering::Relaxed);
        metrics.catching_up[0].store(true, Ordering::Relaxed);
        metrics.catching_up[1].store(false, Ordering::Relaxed);
        metrics.evictions_total.store(7, Ordering::Relaxed);

        // journal_seq=1000 so per_replica_lag = 1000 - acked.
        let state = HealthState {
            active_connections: Arc::new(AtomicU64::new(0)),
            events_processed: Arc::new(AtomicU64::new(0)),
            journal_cursor: Arc::new(Sequence::new(AtomicU64::new(1000))),
            matching_cursor: Arc::new(Sequence::new(AtomicU64::new(1000))),
            input_cursor: Box::new(MockCursor(AtomicU64::new(1000))),
            replication_cursor: Arc::new(AtomicU64::new(900)),
            pipeline_healthy: Arc::new(AtomicBool::new(true)),
            replicas_connected: Some(Arc::new(AtomicU32::new(2))),
            replication_metrics: Some(metrics),
            replication_ring_producer_cursors: None,
            replication_ring_consumer_cursors: None,
            fastest_replica_cursor: None,
            journal_utilization: Arc::new(StageUtilization::new()),
            matching_utilization: Arc::new(StageUtilization::new()),
            response_utilization: Arc::new(StageUtilization::new()),
        };

        let handle = std::thread::spawn(move || {
            health_loop(&listener, &state, &s);
        });

        let body = http_request(addr, "GET /metrics HTTP/1.1\r\n\r\n");
        (body, shutdown, handle)
    }

    #[test]
    fn metrics_emits_per_replica_acked_and_in_memory_sequence() {
        let (body, shutdown, handle) = prometheus_with_full_replication_state();
        assert!(
            body.contains("melin_replica_acked_sequence{slot=\"0\"} 900\n"),
            "slot 0 acked: {body}"
        );
        assert!(
            body.contains("melin_replica_acked_sequence{slot=\"1\"} 800\n"),
            "slot 1 acked: {body}"
        );
        assert!(
            body.contains("melin_replica_in_memory_sequence{slot=\"0\"} 950\n"),
            "slot 0 in_memory: {body}"
        );
        assert!(
            body.contains("melin_replica_in_memory_sequence{slot=\"1\"} 850\n"),
            "slot 1 in_memory: {body}"
        );
        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn metrics_emits_per_replica_lag_relative_to_journal_seq() {
        let (body, shutdown, handle) = prometheus_with_full_replication_state();
        // journal_seq=1000, acked=[900, 800] → lag=[100, 200].
        assert!(
            body.contains("melin_replica_lag{slot=\"0\"} 100\n"),
            "slot 0 lag: {body}"
        );
        assert!(
            body.contains("melin_replica_lag{slot=\"1\"} 200\n"),
            "slot 1 lag: {body}"
        );
        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn metrics_emits_per_replica_bytes_and_ack_latency() {
        let (body, shutdown, handle) = prometheus_with_full_replication_state();
        assert!(
            body.contains("melin_replica_bytes_sent_total{slot=\"0\"} 11111\n"),
            "slot 0 bytes: {body}"
        );
        assert!(
            body.contains("melin_replica_bytes_sent_total{slot=\"1\"} 22222\n"),
            "slot 1 bytes: {body}"
        );
        assert!(
            body.contains("melin_replica_ack_latency_us{slot=\"0\"} 33\n"),
            "slot 0 latency: {body}"
        );
        assert!(
            body.contains("melin_replica_ack_latency_us{slot=\"1\"} 44\n"),
            "slot 1 latency: {body}"
        );
        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn metrics_emits_per_replica_catching_up_and_evictions() {
        let (body, shutdown, handle) = prometheus_with_full_replication_state();
        assert!(
            body.contains("melin_replica_catching_up{slot=\"0\"} 1\n"),
            "slot 0 catching_up: {body}"
        );
        assert!(
            body.contains("melin_replica_catching_up{slot=\"1\"} 0\n"),
            "slot 1 catching_up: {body}"
        );
        assert!(
            body.contains("melin_replica_evictions_total 7\n"),
            "evictions: {body}"
        );
        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn metrics_emits_response_gate_counters_and_policy_degraded() {
        // The response-stage StageUtilization carries three signals not
        // exercised by `stage_utilization_in_metrics`: gate_journal,
        // gate_replication, and policy_degraded. All three are read
        // from the response stage's utilization counter on every
        // health snapshot.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();

        let shutdown = Arc::new(AtomicBool::new(false));
        let s = Arc::clone(&shutdown);

        let response_util = Arc::new(StageUtilization::new());
        response_util.gate_journal.store(13, Ordering::Relaxed);
        response_util.gate_replication.store(17, Ordering::Relaxed);
        response_util.policy_degraded.store(true, Ordering::Relaxed);

        let state = HealthState {
            active_connections: Arc::new(AtomicU64::new(0)),
            events_processed: Arc::new(AtomicU64::new(0)),
            journal_cursor: Arc::new(Sequence::new(AtomicU64::new(0))),
            matching_cursor: Arc::new(Sequence::new(AtomicU64::new(0))),
            input_cursor: Box::new(MockCursor(AtomicU64::new(0))),
            replication_cursor: Arc::new(AtomicU64::new(u64::MAX)),
            pipeline_healthy: Arc::new(AtomicBool::new(true)),
            replicas_connected: None,
            replication_metrics: None,
            replication_ring_producer_cursors: None,
            replication_ring_consumer_cursors: None,
            fastest_replica_cursor: None,
            journal_utilization: Arc::new(StageUtilization::new()),
            matching_utilization: Arc::new(StageUtilization::new()),
            response_utilization: response_util,
        };

        let handle = std::thread::spawn(move || {
            health_loop(&listener, &state, &s);
        });

        let response = http_request(addr, "GET /metrics HTTP/1.1\r\n\r\n");
        assert!(
            response.contains("melin_response_gate_total{blocker=\"journal\"} 13\n"),
            "gate_journal: {response}"
        );
        assert!(
            response.contains("melin_response_gate_total{blocker=\"replication\"} 17\n"),
            "gate_replication: {response}"
        );
        assert!(
            response.contains("melin_durability_policy_degraded 1\n"),
            "policy_degraded: {response}"
        );

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    // ------------------------------------------------------------------
    // REQUEST CLASSIFICATION — guard against `detect_request` returning
    // `PlainTcp` for an HTTP request that doesn't start with `GET `.
    // A non-GET method would otherwise get a raw status-line response
    // (no HTTP framing), which most HTTP clients would treat as garbage.
    // ------------------------------------------------------------------

    #[test]
    fn non_get_http_method_is_classified_as_plain_tcp() {
        // POST is not a documented health-endpoint method — it falls
        // through the GET prefix guards and is treated as a plain TCP
        // probe. The server writes a raw status line and closes; the
        // unread request bytes still in the kernel buffer mean the close
        // may RST the connection, so the client can legitimately observe
        // either the status line + EOF or a truncated read + RST. The
        // load-bearing assertion is just "no HTTP framing comes back" —
        // a future regression that started serving an HTTP response to
        // POST would be a deliberate, reviewed change.
        let (addr, _events, _healthy, shutdown, handle) = start_health(0, 42, u64::MAX);

        let mut client = TcpStream::connect(addr).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        client
            .write_all(b"POST /metrics HTTP/1.1\r\nContent-Length: 0\r\n\r\n")
            .unwrap();
        client.shutdown(std::net::Shutdown::Write).unwrap();
        let mut buf = String::new();
        // RST from the server's close (unread bytes in recv buffer) is
        // expected on Linux — tolerate the read error and inspect what
        // bytes did arrive before the reset.
        let _ = client.read_to_string(&mut buf);

        assert!(
            !buf.starts_with("HTTP/"),
            "POST must not get an HTTP response (got: {buf:?})"
        );

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    // ------------------------------------------------------------------
    // BUFFER CAPACITY — `write_prometheus` writes into a fixed-size stack
    // buffer (8 KiB). A future addition of more per-slot metrics could
    // silently truncate output. Pin a lower bound on the current full
    // body length and assert the buffer still holds it with headroom.
    // ------------------------------------------------------------------

    #[test]
    fn prometheus_body_fits_with_headroom_under_full_replication_state() {
        let (body, shutdown, handle) = prometheus_with_full_replication_state();

        // Strip HTTP headers — keep only the metrics body.
        let metrics_body = body
            .split("\r\n\r\n")
            .nth(1)
            .expect("HTTP head separator present");

        // The body buffer in handle_health_connection is 8192 bytes.
        // Today's body is around 3 KiB; allocate 25 % headroom and fail
        // loudly if we ever drift past it. The point of this test is
        // to fire before silent truncation, not to track the exact size.
        const BODY_BUF: usize = 8192;
        const HEADROOM_LIMIT: usize = BODY_BUF * 3 / 4; // 6144

        assert!(
            metrics_body.len() < HEADROOM_LIMIT,
            "prometheus body ({} bytes) is past 75 % of the {BODY_BUF}-byte stack \
             buffer — adding more metrics will silently truncate the output. \
             Either trim the body or grow body_buf in handle_health_connection.",
            metrics_body.len()
        );

        // Sanity: confirm we're well past zero. If the body shrank
        // dramatically, something would have stopped rendering.
        assert!(
            metrics_body.len() > 1500,
            "prometheus body unexpectedly short ({} bytes) — a write! failure \
             would silently drop content. Body: {metrics_body}",
            metrics_body.len()
        );

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }
}
