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
//! attaches again, and the client resends the refused increment under the
//! same request sequence: a refusal consumed nothing, so the resend is
//! taken. The primary is restarted standalone on its own journal, and
//! replay must reach the value the client was told.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use base64::Engine;
use counter_server::{
    Counter, RequestDecoder, ResponseEncoder, TAG_GET_VALUE, TAG_INCREMENT, TAG_RESP_ACK,
    TAG_RESP_REJECTED, TAG_RESP_VALUE,
};
use ed25519_dalek::SigningKey;
use melin_client::Connection;
use melin_server_runtime::StartupEvents;
use melin_server_runtime::ack_policy::AckPolicy;
use melin_server_runtime::layout::PipelineCores;
use melin_server_runtime::server::{self, ServerConfig};
use melin_transport_core::test_ports::free_addr;
use melin_wire_protocol::tcp::BlockingTcpListener;

/// Port range for `free_addr`, shared with `replicated_failover.rs`: every
/// other range below the ephemeral floor is taken. Sharing is safe only
/// because both binaries are in nextest's `cluster-serial` group, so they
/// never run at the same time.
const PORT_BASE: u16 = 10_000;

type Node = JoinHandle<Result<(), String>>;

fn spawn_node(config: ServerConfig, shutdown: &Arc<AtomicBool>) -> Node {
    let listener = BlockingTcpListener::bind(config.bind).expect("bind client port");
    let shutdown = Arc::clone(shutdown);
    std::thread::spawn(move || {
        server::run_with_listener::<Counter>(
            listener,
            config,
            StartupEvents::none(),
            (),
            RequestDecoder,
            ResponseEncoder,
            None,
            shutdown,
        )
        .map_err(|e| e.to_string())
    })
}

fn stop_node(node: Node, shutdown: &AtomicBool, client_addr: SocketAddr) {
    shutdown.store(true, Ordering::Relaxed);
    // Wake the accept loop so it sees the flag. Dropped deliberately: a
    // node that already stopped listening refuses, which is fine.
    let _ = TcpStream::connect_timeout(&client_addr, Duration::from_millis(100));
    node.join()
        .expect("node thread panicked")
        .expect("node returned an error");
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
fn one_reply(conn: &mut Connection, seq: u64, tag: u8, body: &[u8]) -> Vec<u8> {
    conn.request_one(seq, tag, body)
        .unwrap_or_else(|e| panic!("request {seq} (tag {tag:#04x}) failed: {e}"))
}

fn value_of(conn: &mut Connection, seq: u64) -> u64 {
    let reply = one_reply(conn, seq, TAG_GET_VALUE, &[]);
    assert_eq!(reply[0], TAG_RESP_VALUE);
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
            bind: free_addr(PORT_BASE),
            journal: tmp.path().join(format!("{name}.journal")),
            authorized_keys: auth_path.clone(),
            ack_policy: AckPolicy::Disk,
            no_mlock: true,
            // Unpinned, and therefore yielding: both nodes share this
            // process with the test's own client.
            cores: PipelineCores::unpinned(),
            tick_interval_ms: 0,
            snapshot_interval_ms: 0,
            health_bind: Some(free_addr(PORT_BASE)),
            replication_key: Some(key_path),
            ..ServerConfig::default()
        }
    };

    let replication_addr = free_addr(PORT_BASE);
    let mut primary_config = node_config("primary", &primary_key);
    primary_config.replication_bind = Some(replication_addr);
    let primary_client = primary_config.bind;
    let primary_health = primary_config.health_bind.expect("set above");
    let primary_journal = primary_config.journal.clone();
    let mut replica_config = node_config("replica", &replica_key);
    replica_config.replica_of = Some(replication_addr);
    let replica_client = replica_config.bind;

    let primary_shutdown = Arc::new(AtomicBool::new(false));
    let primary = spawn_node(primary_config, &primary_shutdown);
    let replica_shutdown = Arc::new(AtomicBool::new(false));
    let replica = spawn_node(replica_config, &replica_shutdown);

    // --- Taking writes: the replica is attached, the write is taken. ---
    wait_for_gauge(primary_health, "melin_replicas_connected", 1);
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut conn = Connection::connect_by(primary_client, &client_key, deadline)
        .expect("client connects to the primary");
    let ack = one_reply(&mut conn, 1, TAG_INCREMENT, &1u64.to_le_bytes());
    assert_eq!(ack[0], TAG_RESP_ACK, "the first increment is acked");

    // --- Halted: the replica leaves, the next write is refused. ---
    stop_node(replica, &replica_shutdown, replica_client);
    wait_for_gauge(primary_health, "melin_replicas_connected", 0);
    let refused = one_reply(&mut conn, 2, TAG_INCREMENT, &5u64.to_le_bytes());
    assert_eq!(
        refused[0], TAG_RESP_REJECTED,
        "a halted primary must refuse the write"
    );
    assert_eq!(
        value_of(&mut conn, 3),
        1,
        "queries still answer, and the refused write is not applied"
    );
    // The reader counts the refusal after committing the receive it came
    // in; the reply can beat it, hence the wait.
    wait_for_gauge(primary_health, "melin_writes_refused_total", 1);

    // --- Taking writes again: a replica attaches, the refused write is resent. ---
    let mut replica_config = node_config("replica2", &replica_key);
    replica_config.replica_of = Some(replication_addr);
    let replica_client = replica_config.bind;
    let replica_shutdown = Arc::new(AtomicBool::new(false));
    let replica = spawn_node(replica_config, &replica_shutdown);
    wait_for_gauge(primary_health, "melin_replicas_connected", 1);
    let ack = one_reply(&mut conn, 2, TAG_INCREMENT, &5u64.to_le_bytes());
    assert_eq!(
        ack[0], TAG_RESP_ACK,
        "a refusal consumed no request sequence: the resend is taken"
    );
    assert_eq!(value_of(&mut conn, 4), 6);
    drop(conn);
    stop_node(replica, &replica_shutdown, replica_client);
    stop_node(primary, &primary_shutdown, primary_client);

    // --- Replay: the journal holds only what the client was told. ---
    let restart_config = ServerConfig {
        bind: free_addr(PORT_BASE),
        journal: primary_journal,
        authorized_keys: auth_path.clone(),
        standalone: true,
        ack_policy: AckPolicy::Disk,
        no_mlock: true,
        cores: PipelineCores::unpinned(),
        tick_interval_ms: 0,
        snapshot_interval_ms: 0,
        health_bind: None,
        ..ServerConfig::default()
    };
    let restart_client = restart_config.bind;
    let restart_shutdown = Arc::new(AtomicBool::new(false));
    let restarted = spawn_node(restart_config, &restart_shutdown);
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut conn = Connection::connect_by(restart_client, &client_key, deadline)
        .expect("client connects to the restarted primary");
    let replayed = value_of(&mut conn, 5);
    drop(conn);
    stop_node(restarted, &restart_shutdown, restart_client);

    assert_eq!(
        replayed, 6,
        "replay applied a write the halted primary refused"
    );
}
