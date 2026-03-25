//! Health/liveness endpoint — plain TCP listener on a dedicated port.
//!
//! On connect: writes a one-line status and closes. No auth, no framing.
//! Kubernetes/load balancers check TCP connect success for liveness;
//! ops scripts can parse the status line for richer diagnostics.
//!
//! ## Response format
//!
//! ```text
//! OK <active_connections> <journal_seq> <replication_lag>\n
//! ```
//!
//! Returns `ERR` instead of `OK` when the pipeline is unhealthy (a thread
//! panicked or the server is shutting down). Kubernetes probes that parse
//! the first token can distinguish healthy from degraded.
//!
//! - `active_connections`: currently authenticated client connections
//! - `journal_seq`: latest durable journal sequence number
//! - `replication_lag`: `journal_seq - replication_cursor` (0 in standalone)

use std::io::Write;
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

/// Main health endpoint loop. Accepts connections and writes status.
fn health_loop(
    listener: &TcpListener,
    active_connections: &AtomicU64,
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

/// Write the status line and close. Best-effort — errors are debug-logged
/// but don't affect the server (health probes are fire-and-forget).
fn handle_health_connection(
    mut stream: TcpStream,
    active_connections: &AtomicU64,
    journal_cursor: &Arc<Sequence>,
    replication_cursor: &AtomicU64,
    pipeline_healthy: &AtomicBool,
    replica_connected: &Option<Arc<AtomicBool>>,
) {
    // Short write timeout — health probes should not block the thread.
    let _ = stream.set_write_timeout(Some(Duration::from_secs(1)));

    let healthy = pipeline_healthy.load(Ordering::Relaxed);
    let conns = active_connections.load(Ordering::Relaxed);
    let journal_seq = journal_cursor.get().load(Ordering::Relaxed);
    let repl_cursor = replication_cursor.load(Ordering::Relaxed);

    // Replication lag: 0 in standalone mode (cursor is u64::MAX).
    let repl_lag = if repl_cursor == u64::MAX {
        0
    } else {
        journal_seq.saturating_sub(repl_cursor)
    };

    // Trading state: "trading" when standalone or replica connected,
    // "halted" when replication is enabled but replica is disconnected.
    let trading = replica_connected
        .as_ref()
        .is_none_or(|flag| flag.load(Ordering::Relaxed));
    let trading_str = if trading { "trading" } else { "halted" };

    let status = if healthy { "OK" } else { "ERR" };
    let response = format!("{status} {conns} {journal_seq} {repl_lag} {trading_str}\n");
    if let Err(e) = stream.write_all(response.as_bytes()) {
        debug!(error = %e, "health write failed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    /// Helper: create a non-blocking listener and spawn the health loop.
    /// Returns (addr, pipeline_healthy, replica_connected, shutdown_flag, join_handle).
    /// `replica_connected` is None (standalone mode) unless overridden.
    fn start_health(
        active: u64,
        journal_seq: u64,
        repl_cursor: u64,
    ) -> (
        SocketAddr,
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
        Arc<AtomicBool>,
        Arc<AtomicBool>,
        std::thread::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();

        let active = Arc::new(AtomicU64::new(active));
        let journal = Arc::new(Sequence::new(AtomicU64::new(journal_seq)));
        let repl = Arc::new(AtomicU64::new(repl_cursor));
        let healthy = Arc::new(AtomicBool::new(true));
        let shutdown = Arc::new(AtomicBool::new(false));

        let a = Arc::clone(&active);
        let j = Arc::clone(&journal);
        let r = Arc::clone(&repl);
        let h = Arc::clone(&healthy);
        let rc = replica_connected;
        let s = Arc::clone(&shutdown);

        let handle = std::thread::spawn(move || {
            health_loop(&listener, &a, &j, &r, &h, &rc, &s);
        });

        (addr, healthy, shutdown, handle)
    }

    /// Read the full response from a health connection.
    fn read_health(addr: SocketAddr) -> String {
        let mut client = TcpStream::connect(addr).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut buf = String::new();
        client.read_to_string(&mut buf).unwrap();
        buf
    }

    #[test]
    fn health_response_format() {
        let (addr, _healthy, shutdown, handle) = start_health(5, 42, 40);

        let buf = read_health(addr);
        assert_eq!(buf, "OK 5 42 2 trading\n");

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn health_standalone_replication_lag_is_zero() {
        // Standalone mode: replication cursor is u64::MAX → lag = 0.
        let (addr, _healthy, shutdown, handle) = start_health(0, 100, u64::MAX);

        let buf = read_health(addr);
        assert_eq!(buf, "OK 0 100 0 trading\n");

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn health_multiple_connections() {
        let (addr, _healthy, shutdown, handle) = start_health(10, 0, u64::MAX);

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
        let (addr, healthy, shutdown, handle) = start_health(3, 50, u64::MAX);

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
        let (_addr, _healthy, shutdown, handle) = start_health(0, 0, u64::MAX);

        // Signal shutdown — thread should exit within ~200ms.
        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn spawn_end_to_end() {
        // Test the public `spawn` API (bind + thread + accept + respond).
        let active = Arc::new(AtomicU64::new(7));
        let journal = Arc::new(Sequence::new(AtomicU64::new(99)));
        let repl = Arc::new(AtomicU64::new(u64::MAX));
        let healthy = Arc::new(AtomicBool::new(true));
        let shutdown = Arc::new(AtomicBool::new(false));

        let handle = spawn(
            "127.0.0.1:0".parse().unwrap(),
            Arc::clone(&active),
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
        let (addr, _healthy, shutdown, handle) = start_health(0, 0, u64::MAX);

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
        let (addr, _healthy, shutdown, handle) = start_health(2, 77, u64::MAX);

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
        let (addr, _healthy, shutdown, handle) =
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
}
