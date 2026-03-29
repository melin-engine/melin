//! Health/liveness endpoint — plain TCP listener on a dedicated port.
//!
//! Supports three response modes based on the incoming request:
//!
//! 1. **Plain TCP** (no data sent): writes a one-line status and closes.
//!    Backward-compatible with Kubernetes TCP probes and `nc`.
//! 2. **HTTP `GET /`**: wraps the one-line status in an HTTP 200 response.
//! 3. **HTTP `GET /metrics`**: returns Prometheus text exposition format with
//!    all engine counters.
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

/// Input disruptor capacity. Duplicated here to avoid depending on the engine
/// crate — the value is stable (1 << 20 = 1,048,576 slots).
const INPUT_QUEUE_CAPACITY: u64 = 1 << 20;

/// Shared monitoring state passed to the health loop.
/// Bundles all the atomics/cursors into one struct to avoid parameter explosion.
struct HealthState {
    active_connections: Arc<AtomicU64>,
    events_processed: Arc<AtomicU64>,
    journal_cursor: Arc<Sequence>,
    matching_cursor: Arc<Sequence>,
    input_cursor: Box<dyn QueueCursor>,
    replication_cursor: Arc<AtomicU64>,
    pipeline_healthy: Arc<AtomicBool>,
    replicas_connected: Option<Arc<AtomicU32>>,
}

/// Spawn the health endpoint thread. Returns the join handle.
///
/// Binds a TCP listener on `bind_addr` and accepts connections in a loop.
/// Each connection receives a one-line status response and is closed.
/// The thread exits when `shutdown` is set to true.
///
/// `pipeline_healthy` should be set to `true` at startup and flipped to
/// `false` by the accept loop when a pipeline thread dies or on shutdown.
#[allow(clippy::too_many_arguments)] // Bundled into HealthState internally.
pub fn spawn(
    bind_addr: SocketAddr,
    active_connections: Arc<AtomicU64>,
    events_processed: Arc<AtomicU64>,
    journal_cursor: Arc<Sequence>,
    matching_cursor: Arc<Sequence>,
    input_cursor: Box<dyn QueueCursor>,
    replication_cursor: Arc<AtomicU64>,
    pipeline_healthy: Arc<AtomicBool>,
    replicas_connected: Option<Arc<AtomicU32>>,
    shutdown: Arc<AtomicBool>,
) -> Result<std::thread::JoinHandle<()>, std::io::Error> {
    let listener = TcpListener::bind(bind_addr)?;
    // Non-blocking so we can check the shutdown flag periodically.
    listener.set_nonblocking(true)?;

    info!(addr = %bind_addr, "health endpoint listening");

    let state = HealthState {
        active_connections,
        events_processed,
        journal_cursor,
        matching_cursor,
        input_cursor,
        replication_cursor,
        pipeline_healthy,
        replicas_connected,
    };

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

        Self {
            healthy,
            active_connections: conns,
            events_processed: evts,
            journal_seq,
            replication_lag,
            input_queue_depth,
            trading,
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
             melin_trading_active {}\n",
            self.active_connections,
            self.events_processed,
            self.journal_seq,
            self.replication_lag,
            healthy_val,
            self.input_queue_depth,
            INPUT_QUEUE_CAPACITY,
            trading_val,
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
    let kind = if data.starts_with(b"GET /m") {
        RequestKind::Metrics
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

    // Stack buffers — sized for worst case (all u64::MAX values).
    // Body: Prometheus body is ~1 KiB with max-length u64 values.
    // Response: body + HTTP headers (~200 bytes).
    let mut body_buf = [0u8; 1536];
    let mut resp_buf = [0u8; 1792];

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
        RequestKind::PlainTcp => snapshot.write_status_line(&mut resp_buf),
    };

    if let Err(e) = stream.write_all(&resp_buf[..resp_len]) {
        debug!(error = %e, "health write failed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
            Arc::clone(&active),
            Arc::clone(&events),
            Arc::clone(&journal),
            Arc::clone(&matching),
            Box::new(MockCursor(AtomicU64::new(99))),
            Arc::clone(&repl),
            Arc::clone(&healthy),
            None,
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
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(Sequence::new(AtomicU64::new(0))),
            Arc::new(Sequence::new(AtomicU64::new(0))),
            Box::new(MockCursor(AtomicU64::new(0))),
            Arc::new(AtomicU64::new(u64::MAX)),
            Arc::new(AtomicBool::new(true)),
            None,
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
}
