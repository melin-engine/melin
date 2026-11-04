//! Reading a running node from the outside, as an operator does: its
//! health endpoint's gauges, its admin endpoint's replies, the snapshot
//! it writes, and whether its endpoints' ports are let go once it stops.
//!
//! Both endpoints are kernel TCP on either transport (see the crate docs),
//! so the same calls read a node on DPDK.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::time::{Duration, Instant};

use melin_app::Application;
use melin_client::{Connection, SigningKey};
use melin_transport_core::cursors::ReplicaSlotCursors;
use melin_transport_core::snapshot::{self, SnapshotHeader};
use melin_wire_protocol::control_codec::TAG_CHALLENGE;

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
    wait_for_gauge_where_within(
        health,
        name,
        &format!("reached {value}"),
        |v| v == value,
        limit,
    );
}

/// Wait, at most [`PROBE_LIMIT`], until `name` reads a value `accept`
/// takes, and return it; the failure says the gauge never `what`.
pub fn wait_for_gauge_where(
    health: SocketAddr,
    name: &str,
    what: &str,
    accept: impl Fn(u64) -> bool,
) -> u64 {
    wait_for_gauge_where_within(health, name, what, accept, PROBE_LIMIT)
}

/// Wait, at most `limit`, until `name` reads a value `accept` takes, and
/// return it; the failure says the gauge never `what`.
pub fn wait_for_gauge_where_within(
    health: SocketAddr,
    name: &str,
    what: &str,
    accept: impl Fn(u64) -> bool,
    limit: Duration,
) -> u64 {
    let deadline = Instant::now() + limit;
    loop {
        let last = gauge(health, name);
        if let Some(value) = last
            && accept(value)
        {
            return value;
        }
        assert!(
            Instant::now() < deadline,
            "{name} never {what} within {limit:?} (last: {last:?})"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Whether a client connect to `addr` is answered, within `patience`, with
/// the auth challenge: the definitive "this node serves as primary"
/// signal, since a replica's client listener never accepts. Only the
/// frame's tag is read: any challenge counts, whatever protocol it speaks.
pub fn serves_clients(addr: SocketAddr, patience: Duration) -> bool {
    let Ok(mut stream) = TcpStream::connect_timeout(&addr, Duration::from_millis(300)) else {
        return false;
    };
    if stream.set_read_timeout(Some(patience)).is_err() {
        return false;
    }
    // The frame's length prefix, then its tag.
    let mut head = [0u8; 5];
    stream.read_exact(&mut head).is_ok() && head[4] == TAG_CHALLENGE
}

/// The last sequence a running node has journaled durably
/// (`melin_journal_sequence`). Panics if its health endpoint does not
/// report it: a test reads it from a node it knows is up.
pub fn journal_head(health: SocketAddr) -> u64 {
    gauge(health, "melin_journal_sequence")
        .unwrap_or_else(|| panic!("{health} reports no melin_journal_sequence"))
}

/// The sequence the primary has recorded as acked by the replica in each
/// slot, `None` for a slot whose gauge it does not report.
pub fn replica_acked_sequences(health: SocketAddr) -> [Option<u64>; ReplicaSlotCursors::SLOTS] {
    std::array::from_fn(|slot| {
        gauge(
            health,
            &format!("melin_replica_acked_sequence{{slot=\"{slot}\"}}"),
        )
    })
}

/// Wait, at most [`PROBE_LIMIT`], until the primary at `health` has
/// recorded the ack of `sequence` from at least `replicas` of its
/// replicas, on whichever slots they took: 1 for "a replica holds it",
/// 2 for "both do".
pub fn wait_for_replica_acks(health: SocketAddr, sequence: u64, replicas: usize) {
    assert!(
        (1..=ReplicaSlotCursors::SLOTS).contains(&replicas),
        "a primary has {} replica slots, not {replicas}",
        ReplicaSlotCursors::SLOTS
    );
    let deadline = Instant::now() + PROBE_LIMIT;
    loop {
        let acked = replica_acked_sequences(health);
        let holding = acked
            .iter()
            .filter(|slot| slot.is_some_and(|seq| seq >= sequence))
            .count();
        if holding >= replicas {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{replicas} replica(s) never acked {sequence} within {PROBE_LIMIT:?} \
             (acked per slot: {acked:?})"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Wait, at most [`PROBE_LIMIT`], until nothing listens on any of
/// `endpoints`, health or admin endpoints of a node that has stopped,
/// before a node is started on them again. Both are reused on every boot,
/// but stopping a node does not join their threads: the admin listener
/// sees the shutdown only on its next poll, and a node started while one
/// is still bound fails to bind it and never comes up. (Waiting here does
/// not take the ports: each probe listener is dropped at once.)
pub fn wait_released(endpoints: &[SocketAddr]) {
    let deadline = Instant::now() + PROBE_LIMIT;
    for &addr in endpoints {
        while TcpListener::bind(addr).is_err() {
            assert!(Instant::now() < deadline, "{addr} was never let go");
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

/// Wait, at most [`PROBE_LIMIT`], until the snapshot at `path` loads as
/// one of `A` and its header is one `accept` takes, and return that
/// header; the failure says the snapshot never `what`. A snapshot is
/// written aside and renamed into place, so a load that fails is one not
/// in place yet (none taken, or the last one being replaced), retried
/// like any other not-yet.
pub fn wait_for_snapshot_where<A: Application>(
    path: &Path,
    what: &str,
    accept: impl Fn(&SnapshotHeader) -> bool,
) -> SnapshotHeader {
    let deadline = Instant::now() + PROBE_LIMIT;
    loop {
        let last = snapshot::load::<A>(path).map(|(_, header)| header);
        if let Ok(header) = &last
            && accept(header)
        {
            return *header;
        }
        assert!(
            Instant::now() < deadline,
            "the snapshot at {} never {what} within {PROBE_LIMIT:?} (last: {:?})",
            path.display(),
            last.map(|header| header.sequence)
        );
        std::thread::sleep(Duration::from_millis(20));
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
