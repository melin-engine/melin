//! Failing end-to-end reproductions for findings 1, 3 and 4 of
//! `docs/internal/determinism-audit-2026-09.md`. Gated behind the
//! `determinism-repro` feature. Every test asserts the documented
//! behaviour; a failure is the bug reproducing, and its message says what
//! the node actually did. Finding 1 shows its release-build symptom
//! (`--release`); a debug build trips an assertion instead.
//!
//! Shares `replicated_failover.rs`'s port range: run this file on its own.
#![cfg(feature = "determinism-repro")]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use counter_server::{
    Counter, CounterEvent, GET_VALUE_REQUEST, KIND_RESP_VALUE, RequestDecoder, ResponseEncoder,
    increment_request,
};
use ed25519_dalek::Signer;
use melin_client::{Connection, SigningKey, key};
use melin_server_runtime::StartupEvents;
use melin_server_runtime::ack_policy::AckPolicy;
use melin_server_runtime::layout::PipelineCores;
use melin_server_runtime::server::{self, ServerConfig};
use melin_transport_core::test_ports::free_addr;
use melin_wire_protocol::control_codec::{TAG_CHALLENGE, TAG_CHALLENGE_RESPONSE, TAG_SERVER_READY};
use melin_wire_protocol::tcp::BlockingTcpListener;
use serial_test::serial;

const PORT_BASE: u16 = 10_000;
const CLIENT_KEY: [u8; 32] = [0x11; 32];
const GENESIS: u64 = 1_000_000;

struct Node {
    addr: SocketAddr,
    shutdown: Arc<AtomicBool>,
    handle: JoinHandle<Result<(), String>>,
}

fn spawn(config: ServerConfig, startup: StartupEvents<CounterEvent>) -> Node {
    let listener = BlockingTcpListener::bind(config.bind).expect("bind client port");
    let addr = config.bind;
    let shutdown = Arc::new(AtomicBool::new(false));
    let sd = Arc::clone(&shutdown);
    let handle = std::thread::spawn(move || -> Result<(), String> {
        server::run_with_listener::<Counter>(
            listener,
            config,
            startup,
            (),
            RequestDecoder,
            ResponseEncoder,
            None,
            sd,
        )
        .map_err(|e| e.to_string())
    });
    Node {
        addr,
        shutdown,
        handle,
    }
}

impl Node {
    fn connect(&self) -> Connection {
        let mut c = Connection::connect_by(
            self.addr,
            &SigningKey::from_bytes(&CLIENT_KEY),
            Instant::now() + Duration::from_secs(30),
        )
        .expect("a serving node");
        c.set_read_timeout(Duration::from_secs(30))
            .expect("timeout");
        c
    }

    fn value(&self) -> u64 {
        let frame = self
            .connect()
            .request_one(&GET_VALUE_REQUEST)
            .expect("query");
        assert_eq!(frame[0], KIND_RESP_VALUE);
        u64::from_le_bytes(frame[1..9].try_into().expect("8-byte value"))
    }

    fn increment(&self, amount: u64) {
        self.connect()
            .request_one(&increment_request(amount))
            .expect("increment acked");
    }

    fn stop(self) -> Result<(), String> {
        self.shutdown.store(true, Ordering::Relaxed);
        // Poke the accept loop so it notices the flag; failure is fine.
        let _ = TcpStream::connect_timeout(&self.addr, Duration::from_millis(100));
        self.handle.join().expect("server thread panicked")
    }
}

fn write_auth(dir: &Path, replication_keys: &[&SigningKey]) -> std::path::PathBuf {
    let path = dir.join("authorized_keys");
    let client = SigningKey::from_bytes(&CLIENT_KEY);
    let mut table = key::authorized_keys_line("operator", &client.verifying_key(), "client");
    table.push('\n');
    for (i, k) in replication_keys.iter().enumerate() {
        table += &key::authorized_keys_line("replication", &k.verifying_key(), &format!("n{i}"));
        table.push('\n');
    }
    std::fs::write(&path, table).expect("write auth");
    path
}

fn standalone(dir: &Path, snapshot_interval_ms: u64) -> ServerConfig {
    ServerConfig {
        bind: free_addr(PORT_BASE),
        journal: dir.join("counter.journal"),
        authorized_keys: write_auth(dir, &[]),
        standalone: true,
        ack_policy: AckPolicy::Disk,
        no_mlock: true,
        cores: PipelineCores::unpinned(),
        tick_interval_ms: 0,
        snapshot_interval_ms,
        health_bind: None,
        ..ServerConfig::default()
    }
}

fn genesis() -> StartupEvents<CounterEvent> {
    StartupEvents {
        genesis: vec![CounterEvent::Increment { amount: GENESIS }],
        on_primary: Vec::new(),
    }
}

// --- raw admin client (challenge-response, then one command line) ---

fn read_frame(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let mut len = [0u8; 4];
    stream.read_exact(&mut len)?;
    let mut payload = vec![0u8; u32::from_le_bytes(len) as usize];
    stream.read_exact(&mut payload)?;
    Ok(payload)
}

fn admin_command(addr: SocketAddr, command: &str) -> Option<String> {
    let key = ed25519_dalek::SigningKey::from_bytes(&CLIENT_KEY);
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

fn admin_until_ok(addr: SocketAddr, command: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let reply = admin_command(addr, command);
        if reply.as_deref() == Some("OK") {
            return;
        }
        assert!(Instant::now() < deadline, "{command}: last reply {reply:?}");
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn metrics(addr: SocketAddr) -> String {
    let Ok(mut s) = TcpStream::connect_timeout(&addr, Duration::from_millis(200)) else {
        return String::new();
    };
    if s.set_read_timeout(Some(Duration::from_secs(2))).is_err()
        || s.write_all(b"GET /metrics HTTP/1.1\r\n\r\n").is_err()
    {
        return String::new();
    }
    let mut body = String::new();
    // Best effort: an unreachable endpoint reads as "no gauge yet".
    let _ = s.read_to_string(&mut body);
    body
}

fn wait_for_gauge(health: SocketAddr, line: &str) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while !metrics(health).contains(line) {
        assert!(Instant::now() < deadline, "never saw `{line}` on {health}");
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn journal_sequences(path: &Path) -> String {
    let mut r = match melin_journal::JournalReader::<CounterEvent>::open(path) {
        Ok(r) => r,
        Err(e) => return format!("open failed: {e}"),
    };
    let mut seqs = Vec::new();
    loop {
        match r.next_entry() {
            Ok(Some(e)) => seqs.push(e.sequence),
            Ok(None) => return format!("{seqs:?}"),
            Err(e) => return format!("{seqs:?} then error: {e}"),
        }
    }
}

/// Finding 3. startup.rs: genesis is "Journaled once, by the node that
/// creates the journal". docs/journal.md's Standard Upgrade (snapshot,
/// deploy, start on a fresh journal) boots from the snapshot alone, which
/// already holds genesis, and genesis is journaled and applied again.
#[test]
#[serial]
fn snapshot_only_boot_does_not_journal_genesis_again() {
    let dir = tempfile::tempdir().unwrap();
    let node = spawn(standalone(dir.path(), 100), genesis());
    assert_eq!(node.value(), GENESIS);
    node.increment(5);
    let snap = dir.path().join("counter.snapshot");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok((_, seq, _, _)) = melin_transport_core::snapshot::load::<Counter>(&snap)
            && seq >= 2
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "no snapshot covering the increment"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    node.stop().expect("clean stop");

    // The upgrade: keep the snapshot, move the old journal aside.
    let old = dir.path().join("old-format");
    std::fs::create_dir(&old).unwrap();
    std::fs::rename(
        dir.path().join("counter.journal"),
        old.join("counter.journal"),
    )
    .unwrap();

    let node = spawn(standalone(dir.path(), 100), genesis());
    let value = node.value();
    node.stop().expect("clean stop");
    assert_eq!(
        value,
        GENESIS + 5,
        "the snapshot already held genesis; the node journaled it a second time"
    );
}

/// Finding 4. A first boot refused after `init_engine` has created the
/// journal file (here `--standalone` with the default ack policy) leaves
/// an empty journal. The corrected boot then recovers it, decides genesis
/// is already in the history, and serves without it.
#[test]
#[serial]
fn refused_first_boot_does_not_lose_genesis() {
    let dir = tempfile::tempdir().unwrap();
    let mut misconfigured = standalone(dir.path(), 0);
    misconfigured.ack_policy = AckPolicy::DiskAndRam;
    let refusal = spawn(misconfigured, genesis())
        .handle
        .join()
        .expect("server thread panicked");
    assert!(refusal.is_err(), "the misconfiguration must be refused");
    let journal_left_behind = dir.path().join("counter.journal").exists();

    let node = spawn(standalone(dir.path(), 0), genesis());
    let value = node.value();
    node.stop().expect("clean stop");
    assert_eq!(
        value, GENESIS,
        "genesis was never journaled (refusal: {refusal:?}; journal file left behind by the \
         refused boot: {journal_left_behind})"
    );
}

/// Finding 1, end to end. A replica that restarts, reconnects while caught
/// up, and loses the session before its new pipeline's first durable
/// batch handshakes as a fresh replica. The primary streams its history
/// from sequence 1 and the replica applies it on top of the state it
/// already holds.
#[test]
#[serial]
fn replica_reconnect_before_first_fsync_does_not_reapply_history() {
    let dir = tempfile::tempdir().unwrap();
    let replica_key = SigningKey::from_bytes(&[0x62; 32]);
    let auth = write_auth(dir.path(), &[&replica_key]);
    let replica_key_path = dir.path().join("replica.key");
    std::fs::write(&replica_key_path, replica_key.to_bytes()).unwrap();

    let repl_addr = free_addr(PORT_BASE);
    let primary_health = free_addr(PORT_BASE);
    let primary_client = free_addr(PORT_BASE);
    let primary_config = || ServerConfig {
        bind: primary_client,
        journal: dir.path().join("primary.journal"),
        authorized_keys: auth.clone(),
        ack_policy: AckPolicy::TwoDisks,
        replication_bind: Some(repl_addr),
        no_mlock: true,
        cores: PipelineCores::unpinned(),
        tick_interval_ms: 0,
        snapshot_interval_ms: 0,
        health_bind: Some(primary_health),
        ..ServerConfig::default()
    };
    let replica_admin = free_addr(PORT_BASE);
    let replica_client = free_addr(PORT_BASE);
    let replica_config = || ServerConfig {
        bind: replica_client,
        journal: dir.path().join("replica.journal"),
        authorized_keys: auth.clone(),
        ack_policy: AckPolicy::TwoDisks,
        replica_of: Some(repl_addr),
        replication_key: Some(replica_key_path.clone()),
        admin_bind: Some(replica_admin),
        no_mlock: true,
        cores: PipelineCores::unpinned(),
        tick_interval_ms: 0,
        snapshot_interval_ms: 0,
        health_bind: None,
        ..ServerConfig::default()
    };

    // 1. Primary and replica; three increments, each acked under
    //    two-disks, so the replica holds all three durably.
    let primary = spawn(primary_config(), StartupEvents::none());
    let replica = spawn(replica_config(), StartupEvents::none());
    wait_for_gauge(primary_health, "melin_replicas_connected 1\n");
    for amount in [1, 2, 4] {
        primary.increment(amount);
    }
    assert_eq!(primary.value(), 7);

    // 2. Restart the replica: it recovers its journal (sequence 3),
    //    reconnects, is caught up, and builds a pipeline that journals
    //    nothing because the primary is idle.
    replica.stop().expect("replica stop");
    wait_for_gauge(primary_health, "melin_replicas_connected 0\n");
    let replica = spawn(replica_config(), StartupEvents::none());
    wait_for_gauge(primary_health, "melin_replicas_connected 1\n");
    std::thread::sleep(Duration::from_secs(1));

    // 3. Bounce the primary. The replica's session ends before its
    //    pipeline wrote anything.
    primary.stop().expect("primary stop");
    let primary = spawn(primary_config(), StartupEvents::none());
    wait_for_gauge(primary_health, "melin_replicas_connected 1\n");
    std::thread::sleep(Duration::from_secs(2));
    let primary_value = primary.value();

    // 4. Read the replica's state the only way a replica exposes it:
    //    promote it and ask.
    admin_until_ok(replica_admin, "PROMOTE");
    admin_until_ok(replica_admin, "ACK-POLICY disk");
    let promoted = Node {
        addr: replica_client,
        shutdown: replica.shutdown,
        handle: replica.handle,
    };
    let replica_value = promoted.value();
    let stopped = promoted.stop();
    primary.stop().expect("primary stop");
    let replica_journal = journal_sequences(&dir.path().join("replica.journal"));

    assert_eq!(
        replica_value, primary_value,
        "the promoted replica serves state the primary never had (replica stop: {stopped:?}; \
         replica journal: {replica_journal})"
    );
}
