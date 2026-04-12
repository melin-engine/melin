//! FIX gateway metrics surface.
//!
//! Atomic counters incremented on the io_uring hot path; the
//! `/metrics` endpoint thread reads them with relaxed ordering. Mirrors
//! the hand-rolled, allocation-free pattern in
//! `crates/server/src/replication/mod.rs::ReplicationMetrics` so the
//! gateway can expose itself in the same Prometheus text format
//! without pulling in an external metrics crate.

use std::io::{Cursor, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use tracing::{debug, error, info};

/// Per-gateway counters. A single instance lives for the lifetime of
/// the process and is shared between the event loop (incrementing on
/// the hot path) and the metrics endpoint thread (reading on demand).
#[derive(Default)]
pub struct GatewayMetrics {
    /// Cumulative count of accepted FIX client connections.
    pub sessions_accepted_total: AtomicU64,
    /// Currently active sessions (gauge).
    pub sessions_active: AtomicU64,
    /// Complete FIX frames handed to the session for dispatch
    /// (includes those that subsequently fail to parse — see
    /// `parse_errors_total` for the failed subset).
    pub messages_received_total: AtomicU64,
    /// FIX messages queued for transmission to clients.
    pub messages_sent_total: AtomicU64,
    /// Inbound messages that failed to parse (trust-boundary rejects).
    pub parse_errors_total: AtomicU64,
    /// ResendRequest messages we sent in response to detected gaps.
    pub resend_requests_sent_total: AtomicU64,
    /// ResendRequest messages we received from peers.
    pub resend_requests_received_total: AtomicU64,
    /// Outbound store evictions (oldest message dropped because the
    /// store hit `MAX_OUTBOUND_STORE_MSGS`).
    pub store_evictions_total: AtomicU64,
    /// Inbound messages dropped because the per-session rate limit
    /// was exceeded in the current window.
    pub rate_limit_hits_total: AtomicU64,
}

impl GatewayMetrics {
    /// Allocate a fresh metrics instance and leak it as `'static`.
    /// The gateway process holds one of these for its lifetime, so a
    /// one-time leak is the simplest way to share it across threads
    /// without ref counting on the hot path.
    pub fn leak_default() -> &'static Self {
        Box::leak(Box::new(Self::default()))
    }

    /// Write Prometheus text exposition into `buf`. Returns bytes
    /// written. Zero allocations — all formatting goes through a
    /// stack-backed Cursor. Truncates if `buf` is too small (which
    /// would only happen if the caller passed a buffer smaller than
    /// the fixed-size payload).
    fn write_prometheus(&self, buf: &mut [u8]) -> usize {
        let mut c = Cursor::new(buf);
        // Discard write! result: Cursor::write only fails on buffer
        // overflow. Callers pass a 4 KiB stack buffer for a fixed
        // ~1 KiB payload, so overflow is impossible in practice; if
        // it ever happened the body would simply be truncated and
        // the scrape would surface as malformed Prometheus.
        let _ = write!(
            c,
            "# HELP fix_gateway_sessions_accepted_total Cumulative FIX client sessions accepted.\n\
             # TYPE fix_gateway_sessions_accepted_total counter\n\
             fix_gateway_sessions_accepted_total {}\n\
             # HELP fix_gateway_sessions_active Currently active FIX sessions.\n\
             # TYPE fix_gateway_sessions_active gauge\n\
             fix_gateway_sessions_active {}\n\
             # HELP fix_gateway_messages_received_total Complete FIX frames received from clients (includes parse failures).\n\
             # TYPE fix_gateway_messages_received_total counter\n\
             fix_gateway_messages_received_total {}\n\
             # HELP fix_gateway_messages_sent_total FIX frames written to client sockets (includes resend replays).\n\
             # TYPE fix_gateway_messages_sent_total counter\n\
             fix_gateway_messages_sent_total {}\n\
             # HELP fix_gateway_parse_errors_total Inbound FIX messages that failed to parse.\n\
             # TYPE fix_gateway_parse_errors_total counter\n\
             fix_gateway_parse_errors_total {}\n\
             # HELP fix_gateway_resend_requests_sent_total ResendRequest messages sent in response to inbound gaps.\n\
             # TYPE fix_gateway_resend_requests_sent_total counter\n\
             fix_gateway_resend_requests_sent_total {}\n\
             # HELP fix_gateway_resend_requests_received_total ResendRequest messages received from peers.\n\
             # TYPE fix_gateway_resend_requests_received_total counter\n\
             fix_gateway_resend_requests_received_total {}\n\
             # HELP fix_gateway_store_evictions_total Outbound store entries evicted because the per-session cap was reached.\n\
             # TYPE fix_gateway_store_evictions_total counter\n\
             fix_gateway_store_evictions_total {}\n\
             # HELP fix_gateway_rate_limit_hits_total Inbound messages dropped due to per-session rate limit.\n\
             # TYPE fix_gateway_rate_limit_hits_total counter\n\
             fix_gateway_rate_limit_hits_total {}\n",
            self.sessions_accepted_total.load(Ordering::Relaxed),
            self.sessions_active.load(Ordering::Relaxed),
            self.messages_received_total.load(Ordering::Relaxed),
            self.messages_sent_total.load(Ordering::Relaxed),
            self.parse_errors_total.load(Ordering::Relaxed),
            self.resend_requests_sent_total.load(Ordering::Relaxed),
            self.resend_requests_received_total.load(Ordering::Relaxed),
            self.store_evictions_total.load(Ordering::Relaxed),
            self.rate_limit_hits_total.load(Ordering::Relaxed),
        );
        c.position() as usize
    }
}

/// Spawn the `/metrics` endpoint thread. Returns the join handle.
///
/// Mirrors the simpler subset of `crates/server/src/health.rs::spawn`:
/// non-blocking accept loop, short read timeout to detect the GET
/// request, single Prometheus body response, drop the connection.
/// `shutdown` allows graceful exit.
pub fn spawn_metrics_endpoint(
    bind_addr: SocketAddr,
    metrics: &'static GatewayMetrics,
    shutdown: Arc<AtomicBool>,
) -> std::io::Result<JoinHandle<()>> {
    let listener = TcpListener::bind(bind_addr)?;
    listener.set_nonblocking(true)?;
    info!(addr = %bind_addr, "FIX gateway metrics endpoint listening");

    let handle = std::thread::Builder::new()
        .name("fix-metrics".into())
        .spawn(move || metrics_loop(&listener, metrics, &shutdown))?;
    Ok(handle)
}

fn metrics_loop(listener: &TcpListener, metrics: &GatewayMetrics, shutdown: &AtomicBool) {
    while !shutdown.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((stream, addr)) => {
                debug!(peer = %addr, "metrics scrape");
                handle_metrics_connection(stream, metrics);
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                // No pending connection — sleep briefly. 100 ms is fine
                // for Prometheus scrapes (typical interval is 15s+).
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(e) => {
                error!(error = %e, "metrics endpoint accept error");
            }
        }
    }
}

fn handle_metrics_connection(mut stream: TcpStream, metrics: &GatewayMetrics) {
    // Discarded results: setting timeouts on a brand-new TCP socket
    // only fails if the fd is invalid, which it isn't here. We still
    // proceed even if the kernel rejects the option — worst case the
    // scrape blocks under the parent thread's default timeout.
    let _ = stream.set_read_timeout(Some(Duration::from_millis(50)));
    let _ = stream.set_write_timeout(Some(Duration::from_millis(50)));

    // Best-effort: read enough of the request line to confirm it's a
    // GET. We don't actually route on the path — there's only one
    // resource exposed by this endpoint, so any GET returns the
    // metrics body. Anything else is treated as a malformed scrape and
    // gets a 400.
    let mut req_buf = [0u8; 256];
    // Treat any read error (timeout, EOF, RST) as zero bytes: the
    // empty buffer fails the `GET ` check below and the connection
    // gets a 400, which is the right behavior for any peer that
    // can't or won't send a valid request.
    let n = stream.read(&mut req_buf).unwrap_or(0);
    let request = &req_buf[..n];
    if !request.starts_with(b"GET ") {
        // Best-effort error write: a peer that sent garbage may also
        // disconnect before reading the response. We don't care if
        // the 400 actually lands.
        let _ = stream.write_all(
            b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        );
        return;
    }

    // 4 KiB stack buffer easily fits the fixed-size body (~1 KiB).
    let mut body_buf = [0u8; 4096];
    let body_len = metrics.write_prometheus(&mut body_buf);
    let body = &body_buf[..body_len];

    let mut header_buf = [0u8; 256];
    let mut hc = Cursor::new(header_buf.as_mut_slice());
    // Discard write! result: the header is fixed-size (~120 bytes)
    // and the buffer is 256 bytes — overflow is impossible.
    let _ = write!(
        hc,
        "HTTP/1.1 200 OK\r\n\
         Content-Type: text/plain; version=0.0.4\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n",
        body.len()
    );
    let header_len = hc.position() as usize;

    // Best-effort writes: a Prometheus scraper that disconnects mid
    // response is normal during scrape timeouts. The endpoint thread
    // moves on to the next accept regardless.
    let _ = stream.write_all(&header_buf[..header_len]);
    let _ = stream.write_all(body);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leak_default_returns_zeroed_static() {
        let m = GatewayMetrics::leak_default();
        assert_eq!(m.sessions_active.load(Ordering::Relaxed), 0);
        assert_eq!(m.parse_errors_total.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn write_prometheus_emits_all_counters_with_help_and_type() {
        let m = GatewayMetrics::default();
        m.sessions_accepted_total.store(7, Ordering::Relaxed);
        m.sessions_active.store(3, Ordering::Relaxed);
        m.messages_received_total.store(42, Ordering::Relaxed);
        m.parse_errors_total.store(1, Ordering::Relaxed);

        let mut buf = [0u8; 4096];
        let n = m.write_prometheus(&mut buf);
        let s = std::str::from_utf8(&buf[..n]).unwrap();

        assert!(s.contains("# TYPE fix_gateway_sessions_accepted_total counter"));
        assert!(s.contains("fix_gateway_sessions_accepted_total 7"));
        assert!(s.contains("# TYPE fix_gateway_sessions_active gauge"));
        assert!(s.contains("fix_gateway_sessions_active 3"));
        assert!(s.contains("fix_gateway_messages_received_total 42"));
        assert!(s.contains("fix_gateway_parse_errors_total 1"));
        // Every counter must be present even when zero so the time
        // series doesn't drop out of Prometheus during gaps.
        for name in [
            "fix_gateway_messages_sent_total",
            "fix_gateway_resend_requests_sent_total",
            "fix_gateway_resend_requests_received_total",
            "fix_gateway_store_evictions_total",
            "fix_gateway_rate_limit_hits_total",
        ] {
            assert!(s.contains(name), "{name} missing from output");
        }
    }

    /// Spawn the endpoint, scrape it once with a hand-rolled HTTP
    /// request, verify the response body matches what
    /// `write_prometheus` produces and that the headers are sane.
    #[test]
    fn metrics_endpoint_serves_prometheus_body() {
        use std::io::{Read as _, Write as _};
        use std::net::TcpStream;

        let metrics = GatewayMetrics::leak_default();
        metrics.sessions_accepted_total.store(99, Ordering::Relaxed);
        metrics.parse_errors_total.store(5, Ordering::Relaxed);

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = shutdown.clone();
        // Reuse the loop directly so the test owns the listener.
        listener.set_nonblocking(true).unwrap();
        let join = std::thread::spawn(move || {
            metrics_loop(&listener, metrics, &shutdown_clone);
        });

        // Scrape.
        let mut conn = TcpStream::connect(("127.0.0.1", port)).unwrap();
        conn.write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .unwrap();
        conn.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        let mut response = Vec::new();
        conn.read_to_end(&mut response).unwrap();

        let text = std::str::from_utf8(&response).unwrap();
        assert!(text.starts_with("HTTP/1.1 200 OK"), "got:\n{text}");
        assert!(text.contains("Content-Type: text/plain; version=0.0.4"));
        assert!(text.contains("\r\n\r\n"), "missing header/body separator");
        let body = text.split("\r\n\r\n").nth(1).unwrap();
        assert!(body.contains("fix_gateway_sessions_accepted_total 99"));
        assert!(body.contains("fix_gateway_parse_errors_total 5"));

        shutdown.store(true, Ordering::Relaxed);
        join.join().unwrap();
    }

    #[test]
    fn metrics_endpoint_rejects_non_get() {
        use std::io::{Read as _, Write as _};
        use std::net::TcpStream;

        let metrics = GatewayMetrics::leak_default();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = shutdown.clone();
        listener.set_nonblocking(true).unwrap();
        let join = std::thread::spawn(move || {
            metrics_loop(&listener, metrics, &shutdown_clone);
        });

        let mut conn = TcpStream::connect(("127.0.0.1", port)).unwrap();
        conn.write_all(b"POST /metrics HTTP/1.1\r\n\r\n").unwrap();
        conn.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        let mut resp = Vec::new();
        conn.read_to_end(&mut resp).unwrap();
        let text = std::str::from_utf8(&resp).unwrap();
        assert!(text.starts_with("HTTP/1.1 400"), "got:\n{text}");

        shutdown.store(true, Ordering::Relaxed);
        join.join().unwrap();
    }
}
