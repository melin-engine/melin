//! Full round-trip integration test: start counter-server, connect with
//! `melin-client`, send Increment + GetValue, verify responses, shut down
//! cleanly.
//!
//! The node is started through `melin-test-node`: on kernel TCP by
//! default, on DPDK with this crate's `dpdk` feature, under
//! `scripts/dpdk/netns-runner.sh` (see
//! `docs/internal/dpdk-testing.md`).

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use melin_client::{Connection, SigningKey, key};
use melin_server_runtime::layout::PipelineCores;
use melin_server_runtime::server::ServerConfig;
use melin_test_node::Node;

use counter_server::{
    Counter, GET_VALUE_REQUEST, KIND_RESP_ACK, KIND_RESP_OVERFLOW, KIND_RESP_VALUE, RequestDecoder,
    ResponseEncoder, increment_request,
};
use melin_server_runtime::StartupEvents;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Connect and authenticate, retrying until the server is serving: the
/// kernel accepts the connection before the accept loop runs, so the
/// client has to get through the handshake to know.
fn connect_authenticated(addr: SocketAddr, key: &SigningKey) -> Connection {
    let mut node =
        Connection::connect_by(addr, key, Instant::now() + melin_test_node::STARTUP_LIMIT)
            .expect("a serving node");
    // Generous: the suite shares the machine, and how fast a node answers
    // under full-suite load is not what these tests check.
    node.set_read_timeout(Duration::from_secs(30))
        .expect("set timeout");
    node
}

/// The `u64` a value frame's body carries after its kind.
fn value_of(frame: &[u8]) -> u64 {
    u64::from_le_bytes(frame[1..9].try_into().expect("8-byte value"))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Start a node in a fresh temporary directory. The directory is
/// returned so the caller keeps it alive for as long as the node runs:
/// the journal lives inside it.
fn start_server() -> (tempfile::TempDir, Node) {
    start_server_capped(melin_test_node::MAX_CONNECTIONS)
}

/// [`start_server`], accepting at most `max_connections` clients.
fn start_server_capped(max_connections: u64) -> (tempfile::TempDir, Node) {
    // Server logs go to stderr, which the harness only shows for a
    // failing test: what the node did is then in the report.
    // Deliberately ignored: only the first test in the process installs
    // the subscriber, the rest reuse it.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("debug")),
        )
        .with_test_writer()
        .try_init();

    let key = SigningKey::from_bytes(&[0xAA; 32]);

    let tmp = tempfile::tempdir().expect("tempdir");
    let auth_path = tmp.path().join("authorized_keys");
    std::fs::write(
        &auth_path,
        key::authorized_keys_line("operator", &key.verifying_key(), "test") + "\n",
    )
    .expect("write auth keys");

    let journal_path = tmp.path().join("counter.journal");

    let config = ServerConfig {
        journal: journal_path,
        authorized_keys: auth_path,
        standalone: true,
        ack_policy: melin_server_runtime::ack_policy::AckPolicy::Disk,
        // Unpinned, and therefore yielding: the suite runs many nodes at
        // once, and the default layout would stack every node's same-role
        // thread on one core while a spinner would starve whatever shares
        // its core, the test's own client included.
        cores: PipelineCores::unpinned(),
        tick_interval_ms: 0,
        snapshot_interval_ms: 0,
        health_bind: None,
        max_connections,
        ..melin_test_node::config()
    };

    let server = melin_test_node::start::<Counter>(
        config,
        StartupEvents::none(),
        (),
        RequestDecoder,
        ResponseEncoder,
    );
    (tmp, server)
}

#[test]
fn full_round_trip() {
    let (_tmp, server) = start_server();
    let key = SigningKey::from_bytes(&[0xAA; 32]);
    let mut node = connect_authenticated(server.addr(), &key);

    // --- Increment by 10 ---
    let ack = node.request_one(&increment_request(10)).expect("increment");
    assert_eq!(ack[0], KIND_RESP_ACK);
    assert_eq!(value_of(&ack), 10);

    // --- Increment by 32 ---
    let ack = node.request_one(&increment_request(32)).expect("increment");
    assert_eq!(ack[0], KIND_RESP_ACK);
    assert_eq!(value_of(&ack), 42);

    // --- GetValue query ---
    let value = node.request_one(&GET_VALUE_REQUEST).expect("query");
    assert_eq!(value[0], KIND_RESP_VALUE);
    assert_eq!(value_of(&value), 42);

    drop(node);
    server.stop();
}

/// An increment that would pass `u64::MAX` is answered with an overflow
/// report carrying the unchanged value, and leaves the counter as it was.
#[test]
fn overflowing_increment_is_refused() {
    let (_tmp, server) = start_server();
    let key = SigningKey::from_bytes(&[0xAA; 32]);
    let mut node = connect_authenticated(server.addr(), &key);

    let ack = node
        .request_one(&increment_request(u64::MAX))
        .expect("increment");
    assert_eq!(ack[0], KIND_RESP_ACK);
    assert_eq!(value_of(&ack), u64::MAX);

    let refused = node.request_one(&increment_request(1)).expect("increment");
    assert_eq!(refused[0], KIND_RESP_OVERFLOW);
    assert_eq!(value_of(&refused), u64::MAX);

    let value = node.request_one(&GET_VALUE_REQUEST).expect("query");
    assert_eq!(
        value_of(&value),
        u64::MAX,
        "a refused increment adds nothing"
    );

    drop(node);
    server.stop();
}

/// A node at the production connection cap, whose rings are the largest
/// a default deployment runs, starts and serves. The other tests run
/// small test-sized nodes; this one keeps the production sizing covered.
#[test]
fn a_node_at_the_production_cap_serves() {
    let (_tmp, server) = start_server_capped(ServerConfig::default().max_connections);
    let key = SigningKey::from_bytes(&[0xAA; 32]);
    let mut node = connect_authenticated(server.addr(), &key);

    let ack = node.request_one(&increment_request(7)).expect("increment");
    assert_eq!(ack[0], KIND_RESP_ACK);
    assert_eq!(value_of(&ack), 7);

    drop(node);
    server.stop();
}

/// `0`, which once meant "unlimited", leaves the rings nothing to be
/// sized from: the node refuses to start, and says so.
#[test]
fn a_node_without_a_connection_cap_refuses_to_start() {
    let (_tmp, server) = start_server_capped(0);
    let err = server.join().expect_err("a node with no connection cap");
    assert!(err.contains("unlimited"), "{err}");
}

#[test]
fn second_connection_sees_persisted_state() {
    let (_tmp, server) = start_server();
    let key = SigningKey::from_bytes(&[0xAA; 32]);

    // First connection: increment to 100.
    {
        let mut node = connect_authenticated(server.addr(), &key);
        let ack = node
            .request_one(&increment_request(100))
            .expect("increment");
        assert_eq!(value_of(&ack), 100);
    }

    // Second connection: query — should see 100 (state survives connections).
    {
        let mut node = connect_authenticated(server.addr(), &key);
        let value = node.request_one(&GET_VALUE_REQUEST).expect("query");
        assert_eq!(value[0], KIND_RESP_VALUE);
        assert_eq!(value_of(&value), 100);
    }

    server.stop();
}
