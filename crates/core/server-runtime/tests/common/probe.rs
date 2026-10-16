//! Reading a running node from the outside, as an operator does: its
//! health endpoint's gauges and its admin endpoint's replies.
//!
//! Each test binary that uses it includes this file as a module (`#[path]`),
//! and compiles its own copy.

// Each test binary compiles its own copy of this module and uses a subset
// of it; what one binary leaves unused is another's.
#![allow(dead_code)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use melin_client::{Connection, SigningKey};

/// How long a gauge may take to reach the value a test waits for:
/// generous, for a slow and shared CI runner, and bounded, so a hang fails
/// with a message.
pub const GAUGE_LIMIT: Duration = Duration::from_secs(60);

/// The value of one Prometheus gauge on a node's health endpoint, or `None`
/// while the endpoint is not up, or the gauge is omitted.
pub fn gauge(health: SocketAddr, name: &str) -> Option<u64> {
    let mut stream = TcpStream::connect_timeout(&health, Duration::from_millis(200)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(2))).ok()?;
    stream.write_all(b"GET /metrics HTTP/1.1\r\n\r\n").ok()?;
    let mut body = String::new();
    stream.read_to_string(&mut body).ok()?;
    body.lines()
        .find_map(|line| line.strip_prefix(name)?.trim().parse().ok())
}

/// Wait, at most [`GAUGE_LIMIT`], until `name` reads `value`.
pub fn wait_for_gauge(health: SocketAddr, name: &str, value: u64) {
    let deadline = Instant::now() + GAUGE_LIMIT;
    while gauge(health, name) != Some(value) {
        assert!(
            Instant::now() < deadline,
            "{name} never reached {value} (last: {:?})",
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
