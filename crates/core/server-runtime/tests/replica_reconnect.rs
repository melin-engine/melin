//! End-to-end contract for a replica's reconnect: it resumes from exactly
//! what it holds, never from less.
//!
//! A replica rebuilds its pipeline on every boot. Its reconnect handshake
//! used to read a position the pipeline only filled in after its first
//! durable batch, so a replica that lost its session before that batch
//! told the primary it held nothing. The primary streamed its history
//! again from sequence 1, the replica applied it on top of the state it
//! already had, and in a release build appended it to its journal, which
//! then refused to recover (`SequenceDuplicate`).
//!
//! A primary and one replica (counter app, `two-disks` ack policy, so
//! every ack means the replica holds the entry on disk) over real TCP. Three
//! increments are acked; the replica restarts and reconnects while the
//! primary is idle, so its new pipeline journals nothing; then the primary
//! restarts, which ends the replica's session before that pipeline's first
//! durable batch. The replica is then promoted and read: it must hold what
//! the primary holds, once.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use base64::Engine;
use counter_server::{
    Counter, CounterEvent, GET_VALUE_REQUEST, KIND_RESP_ACK, KIND_RESP_VALUE, RequestDecoder,
    ResponseEncoder, increment_request,
};
use ed25519_dalek::{Signer, SigningKey};
use melin_client::Connection;
use melin_server_runtime::StartupEvents;
use melin_server_runtime::ack_policy::AckPolicy;
use melin_server_runtime::layout::PipelineCores;
use melin_server_runtime::server::{self, ServerConfig};
use melin_transport_core::test_ports::free_addr;
use melin_wire_protocol::control_codec::{TAG_CHALLENGE, TAG_CHALLENGE_RESPONSE, TAG_SERVER_READY};
use melin_wire_protocol::tcp::BlockingTcpListener;

/// Port range for `free_addr`, shared with `replicated_failover.rs` and
/// `halt_refusal.rs`: every other range below the ephemeral floor is
/// taken. Sharing is safe only because these binaries are in nextest's
/// `cluster-serial` group, so they never run at the same time.
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

fn connect(addr: SocketAddr, key: &SigningKey) -> Connection {
    let deadline = Instant::now() + Duration::from_secs(30);
    Connection::connect_by(addr, key, deadline).expect("client connects")
}

fn value_of(conn: &mut Connection) -> u64 {
    let reply = conn.request_one(&GET_VALUE_REQUEST).expect("value query");
    assert_eq!(reply[0], KIND_RESP_VALUE);
    u64::from_le_bytes(reply[1..9].try_into().expect("8 bytes"))
}

// --- Admin endpoint: challenge-response, then one command line. ---

fn read_frame(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let mut len = [0u8; 4];
    stream.read_exact(&mut len)?;
    let mut payload = vec![0u8; u32::from_le_bytes(len) as usize];
    stream.read_exact(&mut payload)?;
    Ok(payload)
}

/// One admin command over a fresh authenticated connection: the reply
/// line, or `None` if the node is not answering yet.
fn admin_command(addr: SocketAddr, key: &SigningKey, command: &str) -> Option<String> {
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_millis(300)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
    let challenge = read_frame(&mut stream).ok()?;
    if challenge.first() != Some(&TAG_CHALLENGE) {
        return None;
    }
    let mut frame = vec![TAG_CHALLENGE_RESPONSE];
    frame.extend_from_slice(&key.sign(&challenge[1..33]).to_bytes());
    frame.extend_from_slice(&key.verifying_key().to_bytes());
    stream.write_all(&(frame.len() as u32).to_le_bytes()).ok()?;
    stream.write_all(&frame).ok()?;
    let ready = read_frame(&mut stream).ok()?;
    if ready.first() != Some(&TAG_SERVER_READY) {
        return None;
    }
    stream.write_all(command.as_bytes()).ok()?;
    stream.write_all(b"\n").ok()?;
    let mut line = String::new();
    BufReader::new(stream).read_line(&mut line).ok()?;
    Some(line.trim_end().to_owned())
}

/// Retry `command` until the node answers `OK`: the admin listener is up
/// from boot, but a promotion may still be settling when it first lands.
fn admin_until_ok(addr: SocketAddr, key: &SigningKey, command: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let reply = admin_command(addr, key, command);
        if reply.as_deref() == Some("OK") {
            return;
        }
        assert!(Instant::now() < deadline, "{command}: last reply {reply:?}");
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// The sequences in a journal's live segment, or where reading it stopped.
fn journal_sequences(path: &std::path::Path) -> String {
    let mut reader = match melin_journal::JournalReader::<CounterEvent>::open(path) {
        Ok(r) => r,
        Err(e) => return format!("open failed: {e}"),
    };
    let mut seqs = Vec::new();
    loop {
        match reader.next_entry() {
            Ok(Some(entry)) => seqs.push(entry.sequence),
            Ok(None) => return format!("{seqs:?}"),
            Err(e) => return format!("{seqs:?} then error: {e}"),
        }
    }
}

#[test]
fn a_replica_reconnecting_before_its_first_durable_batch_does_not_reapply_history() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let primary_key = SigningKey::from_bytes(&[0x81; 32]);
    let replica_key = SigningKey::from_bytes(&[0x82; 32]);
    let client_key = SigningKey::from_bytes(&[0x83; 32]);

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

    let write_key = |name: &str, key: &SigningKey| {
        let path = tmp.path().join(format!("{name}.key"));
        std::fs::write(&path, key.to_bytes()).expect("write node key");
        path
    };
    let primary_key_path = write_key("primary", &primary_key);
    let replica_key_path = write_key("replica", &replica_key);

    // Every address is fixed up front: both nodes restart on the same
    // ones, and the replica must find the restarted primary where it
    // left it.
    let replication_addr = free_addr(PORT_BASE);
    let primary_client = free_addr(PORT_BASE);
    let primary_health = free_addr(PORT_BASE);
    let replica_client = free_addr(PORT_BASE);
    let replica_admin = free_addr(PORT_BASE);
    let replica_journal = tmp.path().join("replica.journal");

    let primary_config = || ServerConfig {
        bind: primary_client,
        journal: tmp.path().join("primary.journal"),
        authorized_keys: auth_path.clone(),
        // Every ack waits for the replica's disk, so the replica holds
        // each acked increment durably before the restarts below.
        ack_policy: AckPolicy::TwoDisks,
        no_mlock: true,
        // Unpinned, and therefore yielding: both nodes share this
        // process with the test's own client.
        cores: PipelineCores::unpinned(),
        // No ticks: an idle primary sends the restarted replica nothing
        // to journal, which is what holds its first durable batch off.
        tick_interval_ms: 0,
        snapshot_interval_ms: 0,
        health_bind: Some(primary_health),
        replication_bind: Some(replication_addr),
        replication_key: Some(primary_key_path.clone()),
        ..ServerConfig::default()
    };
    let replica_config = || ServerConfig {
        bind: replica_client,
        journal: replica_journal.clone(),
        authorized_keys: auth_path.clone(),
        ack_policy: AckPolicy::TwoDisks,
        no_mlock: true,
        cores: PipelineCores::unpinned(),
        tick_interval_ms: 0,
        snapshot_interval_ms: 0,
        health_bind: None,
        admin_bind: Some(replica_admin),
        replica_of: Some(replication_addr),
        replication_key: Some(replica_key_path.clone()),
        ..ServerConfig::default()
    };

    // --- Three acked increments: the replica holds sequences 1 to 3. ---
    let primary_shutdown = Arc::new(AtomicBool::new(false));
    let primary = spawn_node(primary_config(), &primary_shutdown);
    let replica_shutdown = Arc::new(AtomicBool::new(false));
    let replica = spawn_node(replica_config(), &replica_shutdown);
    wait_for_gauge(primary_health, "melin_replicas_connected", 1);
    {
        let mut conn = connect(primary_client, &client_key);
        for amount in [1u64, 2, 4] {
            let ack = conn
                .request_one(&increment_request(amount))
                .expect("increment");
            assert_eq!(ack[0], KIND_RESP_ACK, "increment {amount} is acked");
        }
        assert_eq!(value_of(&mut conn), 7);
    }

    // --- The replica restarts: it recovers sequence 3, reconnects, and
    // is caught up, so its new pipeline has nothing to journal. ---
    stop_node(replica, &replica_shutdown, replica_client);
    wait_for_gauge(primary_health, "melin_replicas_connected", 0);
    let replica_shutdown = Arc::new(AtomicBool::new(false));
    let replica = spawn_node(replica_config(), &replica_shutdown);
    wait_for_gauge(primary_health, "melin_replicas_connected", 1);

    // --- The primary restarts: the replica's session ends before its
    // new pipeline's first durable batch, and it reconnects. ---
    stop_node(primary, &primary_shutdown, primary_client);
    let primary_shutdown = Arc::new(AtomicBool::new(false));
    let primary = spawn_node(primary_config(), &primary_shutdown);
    wait_for_gauge(primary_health, "melin_replicas_connected", 1);
    let primary_value = value_of(&mut connect(primary_client, &client_key));
    assert_eq!(primary_value, 7, "the primary recovered its own journal");
    // A correct replica receives nothing on this session, so there is no
    // signal to wait for. Give a wrong handshake's catch-up time to land
    // before promotion cuts the session.
    std::thread::sleep(Duration::from_secs(2));

    // --- Read the replica the only way a replica exposes its state:
    // promote it, swap it to an ack policy it can meet alone, and ask. ---
    admin_until_ok(replica_admin, &client_key, "PROMOTE");
    admin_until_ok(replica_admin, &client_key, "ACK-POLICY disk");
    let replica_value = value_of(&mut connect(replica_client, &client_key));
    stop_node(replica, &replica_shutdown, replica_client);
    stop_node(primary, &primary_shutdown, primary_client);

    assert_eq!(
        replica_value,
        primary_value,
        "the promoted replica serves state the primary never had (replica journal: {})",
        journal_sequences(&replica_journal)
    );
}
