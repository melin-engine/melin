//! Full round-trip integration test: start counter-server, connect with
//! `melin-client`, send Increment + GetValue, verify responses, shut down
//! cleanly.

use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use melin_client::{Connection, SigningKey, key};
use melin_server_runtime::server::{self, ServerConfig};
use melin_wire_protocol::tcp::BlockingTcpListener;

use counter_server::{
    CounterFactory, RequestDecoder, ResponseEncoder, TAG_GET_VALUE, TAG_INCREMENT, TAG_RESP_ACK,
    TAG_RESP_VALUE,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Connect and authenticate, retrying until the server is serving: the
/// kernel accepts the connection before the accept loop runs, so the
/// client has to get through the handshake to know.
fn connect_authenticated(addr: SocketAddr, key: &SigningKey) -> Connection {
    let mut node = Connection::connect_by(addr, key, Instant::now() + Duration::from_secs(10))
        .expect("a serving node");
    // Generous: the suite shares the machine, and how fast a node answers
    // under full-suite load is not what these tests check.
    node.set_read_timeout(Duration::from_secs(30))
        .expect("set timeout");
    node
}

/// The `u64` a value frame carries after its tag.
fn value_of(frame: &[u8]) -> u64 {
    u64::from_le_bytes(frame[1..9].try_into().expect("8-byte value"))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

fn start_server() -> (
    Arc<AtomicBool>,
    SocketAddr,
    std::thread::JoinHandle<Result<(), String>>,
) {
    let key = SigningKey::from_bytes(&[0xAA; 32]);

    let tmp = tempfile::tempdir().expect("tempdir");
    let auth_path = tmp.path().join("authorized_keys");
    std::fs::write(
        &auth_path,
        key::authorized_keys_line("operator", &key.verifying_key(), "test") + "\n",
    )
    .expect("write auth keys");

    let journal_path = tmp.path().join("counter.journal");

    let listener =
        BlockingTcpListener::bind("127.0.0.1:0".parse::<SocketAddr>().expect("parse addr"))
            .expect("bind");
    let server_addr = listener.local_addr().expect("local_addr");

    let config = ServerConfig {
        bind: server_addr,
        journal: journal_path,
        authorized_keys: auth_path,
        standalone: true,
        ack_policy: melin_server_runtime::ack_policy::AckPolicy::Disk,
        no_mlock: true,
        tick_interval_ms: 0,
        snapshot_interval_ms: 0,
        health_bind: None,
        accounts: 0,
        instruments: 0,
        ..ServerConfig::default()
    };

    let shutdown = Arc::new(AtomicBool::new(false));
    let sd = shutdown.clone();

    // tempdir must outlive the server thread (journal lives inside it).
    let handle = std::thread::spawn(move || -> Result<(), String> {
        let _tmp = tmp;
        server::run_with_listener(
            listener,
            config,
            CounterFactory,
            RequestDecoder,
            ResponseEncoder,
            None,
            sd,
        )
        .map_err(|e| e.to_string())
    });

    (shutdown, server_addr, handle)
}

fn stop_server(
    shutdown: Arc<AtomicBool>,
    addr: SocketAddr,
    handle: std::thread::JoinHandle<Result<(), String>>,
) {
    shutdown.store(true, Ordering::Relaxed);
    // Poke the accept loop so it notices the shutdown flag.
    let _ = TcpStream::connect_timeout(&addr, Duration::from_millis(100));
    handle
        .join()
        .expect("server thread panicked")
        .expect("server returned error");
}

#[test]
fn full_round_trip() {
    let (shutdown, addr, handle) = start_server();
    let key = SigningKey::from_bytes(&[0xAA; 32]);
    let mut node = connect_authenticated(addr, &key);

    // --- Increment by 10 ---
    let ack = node
        .request_one(1, TAG_INCREMENT, &10u64.to_le_bytes())
        .expect("increment");
    assert_eq!(ack[0], TAG_RESP_ACK);
    assert_eq!(value_of(&ack), 10);

    // --- Increment by 32 ---
    let ack = node
        .request_one(2, TAG_INCREMENT, &32u64.to_le_bytes())
        .expect("increment");
    assert_eq!(ack[0], TAG_RESP_ACK);
    assert_eq!(value_of(&ack), 42);

    // --- GetValue query ---
    let value = node.request_one(3, TAG_GET_VALUE, &[]).expect("query");
    assert_eq!(value[0], TAG_RESP_VALUE);
    assert_eq!(value_of(&value), 42);

    drop(node);
    stop_server(shutdown, addr, handle);
}

#[test]
fn second_connection_sees_persisted_state() {
    let (shutdown, addr, handle) = start_server();
    let key = SigningKey::from_bytes(&[0xAA; 32]);

    // First connection: increment to 100.
    {
        let mut node = connect_authenticated(addr, &key);
        let ack = node
            .request_one(1, TAG_INCREMENT, &100u64.to_le_bytes())
            .expect("increment");
        assert_eq!(value_of(&ack), 100);
    }

    // Second connection: query — should see 100 (state survives connections).
    {
        let mut node = connect_authenticated(addr, &key);
        let value = node.request_one(1, TAG_GET_VALUE, &[]).expect("query");
        assert_eq!(value[0], TAG_RESP_VALUE);
        assert_eq!(value_of(&value), 100);
    }

    stop_server(shutdown, addr, handle);
}
