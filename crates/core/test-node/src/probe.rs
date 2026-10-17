//! Reading a running node from the outside, as an operator does: its
//! health endpoint's gauges and its admin endpoint's replies.
//!
//! Both endpoints are kernel TCP on either transport (see the crate docs),
//! so the same calls read a node on DPDK.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use melin_client::{Connection, SigningKey};

/// How long a gauge may take to reach the value a test waits for, or an
/// admin endpoint to answer as a test needs: generous, for a slow and
/// shared CI runner, and bounded, so a hang fails with a message.
pub const PROBE_LIMIT: Duration = Duration::from_secs(60);

/// The whole `/metrics` response of a node's health endpoint, or `None`
/// while the endpoint is not up.
pub fn http_metrics(health: SocketAddr) -> Option<String> {
    let mut stream = TcpStream::connect_timeout(&health, Duration::from_millis(200)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(2))).ok()?;
    stream.write_all(b"GET /metrics HTTP/1.1\r\n\r\n").ok()?;
    let mut body = String::new();
    stream.read_to_string(&mut body).ok()?;
    Some(body)
}

/// The value of one Prometheus gauge on a node's health endpoint, or `None`
/// while the endpoint is not up, or the gauge is omitted.
pub fn gauge(health: SocketAddr, name: &str) -> Option<u64> {
    http_metrics(health)?
        .lines()
        .find_map(|line| line.strip_prefix(name)?.trim().parse().ok())
}

/// Wait, at most [`PROBE_LIMIT`], until `name` reads `value`.
pub fn wait_for_gauge(health: SocketAddr, name: &str, value: u64) {
    wait_for_gauge_within(health, name, value, PROBE_LIMIT);
}

/// Wait, at most `limit`, until `name` reads `value`.
pub fn wait_for_gauge_within(health: SocketAddr, name: &str, value: u64, limit: Duration) {
    let deadline = Instant::now() + limit;
    while gauge(health, name) != Some(value) {
        assert!(
            Instant::now() < deadline,
            "{name} did not reach {value} within {limit:?} (last: {:?})",
            gauge(health, name)
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Send one admin command over a fresh authenticated connection and
/// return the reply line, or `None` if the connection or handshake failed
/// (the node is not up yet — callers retry).
pub fn admin_command(addr: SocketAddr, key: &SigningKey, command: &str) -> Option<String> {
    // The admin listener authenticates like the client one, then speaks
    // text lines: the client crate does the handshake and hands over the
    // socket.
    let mut stream = Connection::connect_timeout(addr, key, Duration::from_millis(300))
        .ok()?
        .into_stream();
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
    stream.write_all(command.as_bytes()).ok()?;
    stream.write_all(b"\n").ok()?;
    stream.flush().ok()?;
    let mut line = String::new();
    BufReader::new(stream).read_line(&mut line).ok()?;
    Some(line.trim_end().to_owned())
}

/// The reply to `command`, retried for at most [`PROBE_LIMIT`] until the
/// admin endpoint is up and answers (a booting node's listener may not be
/// yet).
pub fn admin_reply_when_up(addr: SocketAddr, key: &SigningKey, command: &str) -> String {
    let deadline = Instant::now() + PROBE_LIMIT;
    loop {
        if let Some(reply) = admin_command(addr, key, command) {
            return reply;
        }
        assert!(
            Instant::now() < deadline,
            "the admin endpoint never answered `{command}`"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Send `command`, resent for at most [`PROBE_LIMIT`] until the node
/// answers `OK`: while it is not up, or still settling into a role (a
/// just-promoted node may stage the command, or refuse it, before it
/// serves), as an operator would resend it.
pub fn admin_until_ok(addr: SocketAddr, key: &SigningKey, command: &str) {
    let deadline = Instant::now() + PROBE_LIMIT;
    loop {
        let reply = admin_command(addr, key, command);
        if reply.as_deref() == Some("OK") {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "`{command}` never answered OK (last reply: {reply:?})"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}
