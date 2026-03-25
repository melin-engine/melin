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

use std::io::{Read as _, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use melin_disruptor::padding::Sequence;
use tracing::{debug, error, info};

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
    active_connections: Arc<AtomicU64>,
    events_processed: Arc<AtomicU64>,
    journal_cursor: Arc<Sequence>,
    replication_cursor: Arc<AtomicU64>,
    pipeline_healthy: Arc<AtomicBool>,
    replica_connected: Option<Arc<AtomicBool>>,
    shutdown: Arc<AtomicBool>,
) -> Result<std::thread::JoinHandle<()>, std::io::Error> {
    let listener = TcpListener::bind(bind_addr)?;
    // Non-blocking so we can check the shutdown flag periodically.
    listener.set_nonblocking(true)?;

    info!(addr = %bind_addr, "health endpoint listening");

    let handle = std::thread::Builder::new()
        .name("health".into())
        .spawn(move || {
            health_loop(
                &listener,
                &active_connections,
                &events_processed,
                &journal_cursor,
                &replication_cursor,
                &pipeline_healthy,
                &replica_connected,
                &shutdown,
            );
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
    trading: bool,
}

impl HealthSnapshot {
    /// Collect a snapshot from the shared atomics.
    fn collect(
        active_connections: &AtomicU64,
        events_processed: &AtomicU64,
        journal_cursor: &Arc<Sequence>,
        replication_cursor: &AtomicU64,
        pipeline_healthy: &AtomicBool,
        replica_connected: &Option<Arc<AtomicBool>>,
    ) -> Self {
        let healthy = pipeline_healthy.load(Ordering::Relaxed);
        let conns = active_connections.load(Ordering::Relaxed);
        let evts = events_processed.load(Ordering::Relaxed);
        let journal_seq = journal_cursor.get().load(Ordering::Relaxed);
        let repl_cursor = replication_cursor.load(Ordering::Relaxed);

        // Replication lag: 0 in standalone mode (cursor is u64::MAX).
        let replication_lag = if repl_cursor == u64::MAX {
            0
        } else {
            journal_seq.saturating_sub(repl_cursor)
        };

        // Trading state: "trading" when standalone or replica connected,
        // "halted" when replication is enabled but replica is disconnected.
        let trading = replica_connected
            .as_ref()
            .is_none_or(|flag| flag.load(Ordering::Relaxed));

        Self {
            healthy,
            active_connections: conns,
            events_processed: evts,
            journal_seq,
            replication_lag,
            trading,
        }
    }

    /// One-line status string (no trailing newline — caller adds it).
    fn status_line(&self) -> String {
        let status = if self.healthy { "OK" } else { "ERR" };
        let trading_str = if self.trading { "trading" } else { "halted" };
        format!(
            "{status} {} {} {} {trading_str}",
            self.active_connections, self.journal_seq, self.replication_lag
        )
    }

    /// Prometheus text exposition format body.
    fn prometheus_body(&self) -> Vec<u8> {
        let healthy_val: u8 = if self.healthy { 1 } else { 0 };
        let trading_val: u8 = if self.trading { 1 } else { 0 };

        // Pre-format into a Vec<u8> so we can compute Content-Length.
        format!(
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
             # HELP melin_trading_active Whether the engine is accepting orders (1) or halted (0).\n\
             # TYPE melin_trading_active gauge\n\
             melin_trading_active {}\n",
            self.active_connections,
            self.events_processed,
            self.journal_seq,
            self.replication_lag,
            healthy_val,
            trading_val,
        )
        .into_bytes()
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

/// Read up to 16 bytes with a 50ms timeout to detect HTTP vs plain TCP.
/// The 50ms timeout only affects the health thread (not the hot path).
fn detect_request(stream: &mut TcpStream) -> RequestKind {
    // 16 bytes is enough to distinguish "GET /m" from "GET /" from nothing.
    let mut buf = [0u8; 16];

    // Ensure the socket is in blocking mode — the listener is non-blocking
    // but we need a timed-blocking read here.
    let _ = stream.set_nonblocking(false);
    let _ = stream.set_read_timeout(Some(Duration::from_millis(50)));

    let n = match stream.read(&mut buf) {
        Ok(n) => n,
        Err(e)
            if e.kind() == std::io::ErrorKind::WouldBlock
                || e.kind() == std::io::ErrorKind::TimedOut =>
        {
            return RequestKind::PlainTcp;
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

/// Write a minimal HTTP/1.1 200 response with the given body.
fn write_http_response(stream: &mut TcpStream, content_type: &str, body: &[u8]) {
    // Build headers + body in one allocation, write in one syscall.
    let header = format!(
        "HTTP/1.1 200 OK\r\n\
         Content-Type: {content_type}\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n",
        body.len()
    );
    let mut response = header.into_bytes();
    response.extend_from_slice(body);

    if let Err(e) = stream.write_all(&response) {
        debug!(error = %e, "health write failed");
    }
}

/// Main health endpoint loop. Accepts connections and writes status.
fn health_loop(
    listener: &TcpListener,
    active_connections: &AtomicU64,
    events_processed: &AtomicU64,
    journal_cursor: &Arc<Sequence>,
    replication_cursor: &AtomicU64,
    pipeline_healthy: &AtomicBool,
    replica_connected: &Option<Arc<AtomicBool>>,
    shutdown: &AtomicBool,
) {
    while !shutdown.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((stream, addr)) => {
                debug!(addr = %addr, "health check");
                handle_health_connection(
                    stream,
                    active_connections,
                    events_processed,
                    journal_cursor,
                    replication_cursor,
                    pipeline_healthy,
                    replica_connected,
                );
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
fn handle_health_connection(
    mut stream: TcpStream,
    active_connections: &AtomicU64,
    events_processed: &AtomicU64,
    journal_cursor: &Arc<Sequence>,
    replication_cursor: &AtomicU64,
    pipeline_healthy: &AtomicBool,
    replica_connected: &Option<Arc<AtomicBool>>,
) {
    // Short write timeout — health probes should not block the thread.
    let _ = stream.set_write_timeout(Some(Duration::from_secs(1)));

    let snapshot = HealthSnapshot::collect(
        active_connections,
        events_processed,
        journal_cursor,
        replication_cursor,
        pipeline_healthy,
        replica_connected,
    );

    let kind = detect_request(&mut stream);

    match kind {
        RequestKind::Metrics => {
            let body = snapshot.prometheus_body();
            write_http_response(
                &mut stream,
                "text/plain; version=0.0.4; charset=utf-8",
                &body,
            );
        }
        RequestKind::HttpHealth => {
            let line = format!("{}\n", snapshot.status_line());
            write_http_response(&mut stream, "text/plain; charset=utf-8", line.as_bytes());
        }
        RequestKind::PlainTcp => {
            let line = format!("{}\n", snapshot.status_line());
            if let Err(e) = stream.write_all(line.as_bytes()) {
                debug!(error = %e, "health write failed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    /// Helper: create a non-blocking listener and spawn the health loop.
    /// Returns (addr, events_processed, pipeline_healthy, shutdown_flag, join_handle).
    /// `replica_connected` is None (standalone mode) unless overridden.
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

    /// Like `start_health` but with an explicit `replica_connected` flag.
    fn start_health_with_replica(
        active: u64,
        journal_seq: u64,
        repl_cursor: u64,
        replica_connected: Option<Arc<AtomicBool>>,
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
        let repl = Arc::new(AtomicU64::new(repl_cursor));
        let healthy = Arc::new(AtomicBool::new(true));
        let shutdown = Arc::new(AtomicBool::new(false));

        let a = Arc::clone(&active);
        let ev = Arc::clone(&events);
        let j = Arc::clone(&journal);
        let r = Arc::clone(&repl);
        let h = Arc::clone(&healthy);
        let rc = replica_connected;
        let s = Arc::clone(&shutdown);

        let handle = std::thread::spawn(move || {
            health_loop(&listener, &a, &ev, &j, &r, &h, &rc, &s);
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
        let repl = Arc::new(AtomicU64::new(u64::MAX));
        let healthy = Arc::new(AtomicBool::new(true));
        let shutdown = Arc::new(AtomicBool::new(false));

        let handle = spawn(
            "127.0.0.1:0".parse().unwrap(),
            Arc::clone(&active),
            Arc::clone(&events),
            Arc::clone(&journal),
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
        let replica_flag = Arc::new(AtomicBool::new(false)); // disconnected
        let (addr, _events, _healthy, shutdown, handle) =
            start_health_with_replica(5, 100, u64::MAX, Some(Arc::clone(&replica_flag)));

        let buf = read_health(addr);
        assert_eq!(buf, "OK 5 100 0 halted\n");

        // Reconnect replica — should switch to trading.
        replica_flag.store(true, Ordering::Relaxed);
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

        // Verify all 6 metric lines.
        assert!(response.contains("melin_active_connections 5\n"));
        assert!(response.contains("melin_events_processed 1000\n"));
        assert!(response.contains("melin_journal_sequence 42\n"));
        assert!(response.contains("melin_replication_lag 2\n"));
        assert!(response.contains("melin_pipeline_healthy 1\n"));
        assert!(response.contains("melin_trading_active 1\n"));

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn metrics_boolean_encoding() {
        // Verify that unhealthy + halted → 0 values.
        let replica_flag = Arc::new(AtomicBool::new(false)); // disconnected → halted
        let (addr, _events, healthy, shutdown, handle) =
            start_health_with_replica(0, 0, u64::MAX, Some(Arc::clone(&replica_flag)));

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
}
