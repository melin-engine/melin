//! End-to-end contract for a halted primary: a write it refuses is gone.
//!
//! A primary whose last replica has left refuses client writes. The client
//! is told so, and the refusal has to be the whole truth: the write must
//! not be applied, and it must not be journaled either, or the next replay
//! applies what the live engine refused. That second half used to fail —
//! the refusal was decided after the journal had already recorded the
//! write — and only a restart shows it.
//!
//! A primary and one replica (counter app, `disk` ack policy) over real TCP.
//! One increment is acked while the replica is attached; the replica is
//! then stopped, and a second increment must be refused while a query
//! still answers, and counted on the health endpoint. A replica then
//! attaches again, and the client resends the refused increment, which is
//! taken. The primary is restarted standalone on its own journal, and
//! replay must reach the value the client was told.
//!
//! Nodes are started through `melin-test-node`: on kernel TCP by default,
//! on DPDK with this crate's `dpdk` feature, under
//! `scripts/dpdk/netns-runner.sh` (see
//! `docs/internal/dpdk-testing.md`).

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use base64::Engine;
use counter_server::{
    Counter, GET_VALUE_REQUEST, KIND_RESP_ACK, KIND_RESP_REJECTED, KIND_RESP_VALUE, RequestDecoder,
    ResponseEncoder, increment_request,
};
use ed25519_dalek::SigningKey;
use melin_client::Connection;
use melin_server_runtime::StartupEvents;
use melin_server_runtime::ack_policy::AckPolicy;
use melin_server_runtime::layout::PipelineCores;
use melin_server_runtime::server::ServerConfig;
use melin_test_node::{Addrs, Node};
use melin_transport_core::test_ports::free_addr;

/// Port range for `free_addr`, shared with `replicated_failover.rs`: every
/// other range below the ephemeral floor is taken. Sharing is safe only
/// because both binaries are in nextest's `cluster-serial` group, so they
/// never run at the same time.
const PORT_BASE: u16 = 10_000;

/// Node `slot`'s addresses (`melin_test_node::addrs`), from this file's
/// port range on kernel TCP.
fn addrs(slot: usize) -> Addrs {
    melin_test_node::addrs(slot, || free_addr(PORT_BASE))
}

fn spawn_node(addrs: &Addrs, config: ServerConfig) -> Node {
    melin_test_node::start_at::<Counter>(
        addrs,
        config,
        StartupEvents::none(),
        (),
        RequestDecoder,
        ResponseEncoder,
    )
}

/// The value of one Prometheus gauge on a node's health endpoint, or `None`
/// while the endpoint is not up.
fn gauge(health: SocketAddr, name: &str) -> Option<u64> {
    let mut stream = TcpStream::connect_timeout(&health, Duration::from_millis(200)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(2))).ok()?;
    stream.write_all(b"GET /metrics HTTP/1.1\r\n\r\n").ok()?;
    let mut body = String::new();
    stream.read_to_string(&mut body).ok()?;
    body.lines()
        .find_map(|line| line.strip_prefix(name)?.trim().parse().ok())
}

fn wait_for_gauge(health: SocketAddr, name: &str, value: u64) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while gauge(health, name) != Some(value) {
        assert!(
            Instant::now() < deadline,
            "{name} never reached {value} (last: {:?})",
            gauge(health, name)
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Send one request and return the single frame of its reply.
fn one_reply(conn: &mut Connection, body: &[u8]) -> Vec<u8> {
    conn.request_one(body)
        .unwrap_or_else(|e| panic!("request {body:02x?} failed: {e}"))
}

fn value_of(conn: &mut Connection) -> u64 {
    let reply = one_reply(conn, &GET_VALUE_REQUEST);
    assert_eq!(reply[0], KIND_RESP_VALUE);
    u64::from_le_bytes(reply[1..9].try_into().expect("8 bytes"))
}

#[test]
fn a_write_refused_while_halted_is_not_replayed() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let primary_key = SigningKey::from_bytes(&[0x71; 32]);
    let replica_key = SigningKey::from_bytes(&[0x72; 32]);
    let client_key = SigningKey::from_bytes(&[0x73; 32]);

    let b64 = |k: &SigningKey| {
        base64::engine::general_purpose::STANDARD.encode(k.verifying_key().to_bytes())
    };
    let auth_path = tmp.path().join("authorized_keys");
    std::fs::write(
        &auth_path,
        format!(
            "replication {} primary\nreplication {} replica\noperator {} client\n",
            b64(&primary_key),
            b64(&replica_key),
            b64(&client_key)
        ),
    )
    .expect("write authorized_keys");

    let node_config = |name: &str, key: &SigningKey| -> ServerConfig {
        let key_path = tmp.path().join(format!("{name}.key"));
        std::fs::write(&key_path, key.to_bytes()).expect("write node key");
        ServerConfig {
            journal: tmp.path().join(format!("{name}.journal")),
            authorized_keys: auth_path.clone(),
            ack_policy: AckPolicy::Disk,
            // Unpinned, and therefore yielding: both nodes share this
            // process with the test's own client.
            cores: PipelineCores::unpinned(),
            tick_interval_ms: 0,
            snapshot_interval_ms: 0,
            health_bind: Some(free_addr(PORT_BASE)),
            replication_key: Some(key_path),
            ..melin_test_node::config()
        }
    };

    let primary_addrs = addrs(0);
    let mut primary_config = node_config("primary", &primary_key);
    primary_config.replication_bind = Some(primary_addrs.replication());
    let primary_health = primary_config.health_bind.expect("set above");
    let primary_journal = primary_config.journal.clone();
    let mut replica_config = node_config("replica", &replica_key);
    replica_config.replica_of = Some(primary_addrs.replication());

    let primary = spawn_node(&primary_addrs, primary_config);
    let replica = spawn_node(&addrs(1), replica_config);

    // --- Taking writes: the replica is attached, the write is taken. ---
    wait_for_gauge(primary_health, "melin_replicas_connected", 1);
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut conn = Connection::connect_by(primary.addr(), &client_key, deadline)
        .expect("client connects to the primary");
    let ack = one_reply(&mut conn, &increment_request(1));
    assert_eq!(ack[0], KIND_RESP_ACK, "the first increment is acked");

    // --- Halted: the replica leaves, the next write is refused. ---
    replica.stop();
    wait_for_gauge(primary_health, "melin_replicas_connected", 0);
    let refused = one_reply(&mut conn, &increment_request(5));
    assert_eq!(
        refused[0], KIND_RESP_REJECTED,
        "a halted primary must refuse the write"
    );
    assert_eq!(
        value_of(&mut conn),
        1,
        "queries still answer, and the refused write is not applied"
    );
    // The reader counts the refusal after committing the receive it came
    // in; the reply can beat it, hence the wait.
    wait_for_gauge(primary_health, "melin_writes_refused_total", 1);

    // --- Taking writes again: a replica attaches, the refused write is resent. ---
    let mut replica_config = node_config("replica2", &replica_key);
    replica_config.replica_of = Some(primary_addrs.replication());
    let replica = spawn_node(&addrs(2), replica_config);
    wait_for_gauge(primary_health, "melin_replicas_connected", 1);
    let ack = one_reply(&mut conn, &increment_request(5));
    assert_eq!(ack[0], KIND_RESP_ACK, "the resend is taken");
    assert_eq!(value_of(&mut conn), 6);
    drop(conn);
    replica.stop();
    primary.stop();

    // --- Replay: the journal holds only what the client was told. ---
    let restart_config = ServerConfig {
        journal: primary_journal,
        authorized_keys: auth_path.clone(),
        standalone: true,
        ack_policy: AckPolicy::Disk,
        cores: PipelineCores::unpinned(),
        tick_interval_ms: 0,
        snapshot_interval_ms: 0,
        health_bind: None,
        ..melin_test_node::config()
    };
    let restarted = spawn_node(&addrs(0), restart_config);
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut conn = Connection::connect_by(restarted.addr(), &client_key, deadline)
        .expect("client connects to the restarted primary");
    let replayed = value_of(&mut conn);
    drop(conn);
    restarted.stop();

    assert_eq!(
        replayed, 6,
        "replay applied a write the halted primary refused"
    );
}
