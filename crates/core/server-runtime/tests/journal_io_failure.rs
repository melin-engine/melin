//! A journal write failure stops the node with its own exit status,
//! end to end: a real node, a real client, and an `fdatasync` of the live
//! segment that the kernel refuses (injected where the kernel's error
//! would enter, so everything above it runs unchanged). The disk thread
//! latches the failure, the node stops, `run` returns the journal's
//! error, and `exit::exit_code` maps it to 74.
//!
//! And the converse: a node that stops for any other reason, a missing
//! keys file or a journal recovery refuses, exits with status 1, so a
//! supervisor that holds back on 74 still restarts it.
//!
//! Not built under `no-persist`, which never syncs the journal and so has
//! no sync to fail.

#![cfg(not(feature = "no-persist"))]

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use base64::Engine;
use counter_server::{
    Counter, GET_VALUE_REQUEST, KIND_RESP_VALUE, RequestDecoder, ResponseEncoder, increment_request,
};
use melin_client::{Connection, SigningKey, key};
use melin_server_runtime::StartupEvents;
use melin_server_runtime::ack_policy::AckPolicy;
use melin_server_runtime::exit::{self, EXIT_JOURNAL_IO_ERROR};
use melin_server_runtime::layout::PipelineCores;
use melin_server_runtime::server::{self, ServerConfig};
use melin_transport_core::test_ports::free_addr;
use melin_wire_protocol::tcp::BlockingTcpListener;

/// Port range this file owns for `free_addr`, shared with the other
/// files of this crate: each test binary is its own process, and the
/// allocator splits the range by process. See `test_ports::free_addr`.
const PORT_BASE: u16 = 10_000;

const CLIENT_KEY: [u8; 32] = [0xAA; 32];

/// What a node's `main` would see: the exit status `exit_code` maps the
/// result of `run` to, and the error's message. Collected on the node's
/// thread because the error itself is not `Send`.
struct Stopped {
    status: ExitCode,
    write_failure: bool,
    message: String,
}

type Node = JoinHandle<Stopped>;

fn spawn_node(
    listener: BlockingTcpListener,
    config: ServerConfig,
    shutdown: &Arc<AtomicBool>,
) -> Node {
    let shutdown = Arc::clone(shutdown);
    std::thread::spawn(move || {
        let result = server::run_with_listener::<Counter>(
            listener,
            config,
            StartupEvents::none(),
            (),
            RequestDecoder,
            ResponseEncoder,
            None,
            shutdown,
        );
        let write_failure = result
            .as_ref()
            .is_err_and(|e| exit::is_journal_write_failure(&**e));
        let message = match &result {
            Ok(()) => String::new(),
            Err(e) => e.to_string(),
        };
        Stopped {
            status: exit::exit_code(result),
            write_failure,
            message,
        }
    })
}

/// Wait for the node to stop on its own (a failure, not a shutdown
/// request), with a deadline so a node that keeps running fails the test
/// instead of hanging it.
fn stopped(node: Node) -> Stopped {
    let deadline = Instant::now() + Duration::from_secs(60);
    while !node.is_finished() {
        assert!(
            Instant::now() < deadline,
            "the node is still running: a journal failure must stop it"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    node.join().expect("node thread panicked")
}

fn write_keys(dir: &Path, extra: &str) -> PathBuf {
    let path = dir.join("authorized_keys");
    let client = SigningKey::from_bytes(&CLIENT_KEY);
    std::fs::write(
        &path,
        format!(
            "{}\n{extra}",
            key::authorized_keys_line("operator", &client.verifying_key(), "test")
        ),
    )
    .expect("write authorized_keys");
    path
}

fn base_config(dir: &Path, name: &str, authorized_keys: PathBuf) -> ServerConfig {
    ServerConfig {
        bind: free_addr(PORT_BASE),
        journal: dir.join(format!("{name}.journal")),
        authorized_keys,
        ack_policy: AckPolicy::Disk,
        no_mlock: true,
        // Unpinned, and therefore yielding: the suite runs many nodes at
        // once, and a spinner would starve whatever shares its core.
        cores: PipelineCores::unpinned(),
        tick_interval_ms: 0,
        snapshot_interval_ms: 0,
        health_bind: None,
        ..ServerConfig::default()
    }
}

fn connect(addr: SocketAddr) -> Connection {
    let mut conn = Connection::connect_by(
        addr,
        &SigningKey::from_bytes(&CLIENT_KEY),
        Instant::now() + Duration::from_secs(30),
    )
    .expect("a serving node");
    conn.set_read_timeout(Duration::from_secs(30))
        .expect("set timeout");
    conn
}

fn value_of(conn: &mut Connection) -> u64 {
    let frame = conn.request_one(&GET_VALUE_REQUEST).expect("query");
    assert_eq!(frame[0], KIND_RESP_VALUE);
    u64::from_le_bytes(frame[1..9].try_into().expect("8-byte value"))
}

fn assert_write_failure(stopped: &Stopped) {
    assert!(
        stopped.write_failure,
        "run must return the journal's write failure, got: {:?}",
        stopped.message
    );
    assert_eq!(
        stopped.status,
        ExitCode::from(EXIT_JOURNAL_IO_ERROR),
        "a journal write failure exits with status 74: {}",
        stopped.message
    );
    assert!(
        stopped.message.contains("journal write failed"),
        "the error names the failure: {}",
        stopped.message
    );
}

/// A standalone primary whose journal sync fails: the write is never
/// acknowledged, the node stops, and its status is 74. Before this was
/// distinguished, `run` returned the bare message "pipeline failure",
/// the same as for any panicked pipeline thread, and the process exited
/// with status 1.
#[test]
fn a_failed_journal_sync_stops_a_primary_with_status_74() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = ServerConfig {
        standalone: true,
        ..base_config(dir.path(), "counter", write_keys(dir.path(), ""))
    };
    let journal = config.journal.clone();
    let listener = BlockingTcpListener::bind(config.bind).expect("bind");
    let addr = config.bind;
    let shutdown = Arc::new(AtomicBool::new(false));
    let node = spawn_node(listener, config, &shutdown);

    let mut conn = connect(addr);
    assert_eq!(value_of(&mut conn), 0, "serving, nothing journaled yet");

    melin_journal::test_utils::fail_next_sync(&journal);
    assert!(
        conn.request_one(&increment_request(1)).is_err(),
        "a write whose sync failed must never be acknowledged"
    );

    let stopped = stopped(node);
    assert_write_failure(&stopped);
}

/// The same on a replica, whose journal failure travels a different path
/// (the replication receiver tears its pipeline down and returns the
/// journal's error): the replica stops with status 74, and the primary
/// keeps serving.
#[test]
fn a_failed_journal_sync_stops_a_replica_with_status_74() {
    let dir = tempfile::tempdir().expect("tempdir");
    let primary_key = SigningKey::from_bytes(&[0x81; 32]);
    let replica_key = SigningKey::from_bytes(&[0x82; 32]);
    let b64 = |k: &SigningKey| {
        base64::engine::general_purpose::STANDARD.encode(k.verifying_key().to_bytes())
    };
    let keys = write_keys(
        dir.path(),
        &format!(
            "replication {} primary\nreplication {} replica\n",
            b64(&primary_key),
            b64(&replica_key)
        ),
    );
    let node_config = |name: &str, signing: &SigningKey| {
        let key_path = dir.path().join(format!("{name}.key"));
        std::fs::write(&key_path, signing.to_bytes()).expect("write node key");
        ServerConfig {
            replication_key: Some(key_path),
            ..base_config(dir.path(), name, keys.clone())
        }
    };

    let replication_addr = free_addr(PORT_BASE);
    let health_addr = free_addr(PORT_BASE);
    let primary_config = ServerConfig {
        replication_bind: Some(replication_addr),
        health_bind: Some(health_addr),
        ..node_config("primary", &primary_key)
    };
    let replica_config = ServerConfig {
        replica_of: Some(replication_addr),
        ..node_config("replica", &replica_key)
    };
    let replica_journal = replica_config.journal.clone();
    let primary_addr = primary_config.bind;

    let primary_shutdown = Arc::new(AtomicBool::new(false));
    let primary = spawn_node(
        BlockingTcpListener::bind(primary_config.bind).expect("bind"),
        primary_config,
        &primary_shutdown,
    );
    let replica_shutdown = Arc::new(AtomicBool::new(false));
    let replica = spawn_node(
        BlockingTcpListener::bind(replica_config.bind).expect("bind"),
        replica_config,
        &replica_shutdown,
    );

    // The replica streams and journals: it has acknowledged the first
    // increment from its own journal before its sync is made to fail.
    wait_for_gauge(health_addr, "melin_replicas_connected", 1);
    let mut conn = connect(primary_addr);
    conn.request_one(&increment_request(1)).expect("increment");
    wait_for_gauge(health_addr, "melin_replica_acked_sequence{slot=\"0\"}", 1);

    melin_journal::test_utils::fail_next_sync(&replica_journal);
    // Acknowledged by the primary's own sync under `disk`; the replica's
    // copy is the one that fails.
    conn.request_one(&increment_request(2)).expect("increment");

    let stopped_replica = stopped(replica);
    assert_write_failure(&stopped_replica);
    assert_eq!(value_of(&mut conn), 3, "the primary keeps serving");

    drop(conn);
    primary_shutdown.store(true, Ordering::Relaxed);
    // Wake the accept loop so it sees the flag. Dropped deliberately: a
    // node that already stopped listening refuses, which is fine.
    let _ = TcpStream::connect_timeout(&primary_addr, Duration::from_millis(100));
    let primary = primary.join().expect("primary thread panicked");
    assert_eq!(primary.status, ExitCode::SUCCESS, "{}", primary.message);
}

/// A node stopped by anything but a journal write failure exits with
/// status 1: a missing keys file, and a recovery that refuses the
/// journal it finds. Neither can leave unwritten data in the page cache,
/// so restarting in place stays the supervisor's call.
#[test]
fn other_failures_exit_with_status_1() {
    let dir = tempfile::tempdir().expect("tempdir");

    // Configuration: the keys file does not exist.
    let config = ServerConfig {
        standalone: true,
        ..base_config(dir.path(), "missing-keys", dir.path().join("no-such-file"))
    };
    let node = spawn_node(
        BlockingTcpListener::bind(config.bind).expect("bind"),
        config,
        &Arc::new(AtomicBool::new(false)),
    );
    let stopped_node = stopped(node);
    assert!(!stopped_node.write_failure, "{}", stopped_node.message);
    assert_eq!(
        stopped_node.status,
        ExitCode::FAILURE,
        "{}",
        stopped_node.message
    );

    // Recovery refuses: the file at the journal path is not a journal.
    let config = ServerConfig {
        standalone: true,
        ..base_config(dir.path(), "not-a-journal", write_keys(dir.path(), ""))
    };
    std::fs::write(&config.journal, vec![0x5A; 8192]).expect("write a foreign file");
    let node = spawn_node(
        BlockingTcpListener::bind(config.bind).expect("bind"),
        config,
        &Arc::new(AtomicBool::new(false)),
    );
    let stopped_node = stopped(node);
    assert!(
        !stopped_node.message.is_empty(),
        "recovery must refuse a foreign file"
    );
    assert!(!stopped_node.write_failure, "{}", stopped_node.message);
    assert_eq!(
        stopped_node.status,
        ExitCode::FAILURE,
        "{}",
        stopped_node.message
    );
}

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
