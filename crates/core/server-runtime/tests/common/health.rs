//! A node's health endpoint, read as the integration tests need it: one
//! Prometheus gauge at a time.
//!
//! Each test binary that uses it includes this file as a module (`#[path]`),
//! and compiles its own copy.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

/// How long [`wait_for_gauge`] waits: generous, for a slow and shared CI
/// runner, and bounded, so a hang fails with a message.
const LIMIT: Duration = Duration::from_secs(60);

/// The value of one Prometheus gauge on a node's health endpoint, or `None`
/// while the endpoint is not up.
pub fn gauge(health: SocketAddr, name: &str) -> Option<u64> {
    let mut stream = TcpStream::connect_timeout(&health, Duration::from_millis(200)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(2))).ok()?;
    stream.write_all(b"GET /metrics HTTP/1.1\r\n\r\n").ok()?;
    let mut body = String::new();
    stream.read_to_string(&mut body).ok()?;
    body.lines()
        .find_map(|line| line.strip_prefix(name)?.trim().parse().ok())
}

/// Wait until gauge `name` reads `value`; panics after [`LIMIT`].
pub fn wait_for_gauge(health: SocketAddr, name: &str, value: u64) {
    wait_for_gauge_where(health, name, &format!("reached {value}"), |v| v == value);
}

/// Wait until gauge `name` reads a value `accept` takes, and return it;
/// panics after [`LIMIT`], saying the gauge never `what`.
pub fn wait_for_gauge_where(
    health: SocketAddr,
    name: &str,
    what: &str,
    accept: impl Fn(u64) -> bool,
) -> u64 {
    let deadline = Instant::now() + LIMIT;
    loop {
        let last = gauge(health, name);
        if let Some(value) = last
            && accept(value)
        {
            return value;
        }
        assert!(
            Instant::now() < deadline,
            "{name} never {what} (last: {last:?})"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}
