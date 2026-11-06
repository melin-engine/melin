//! The divergence stop, end to end, through a running node.
//!
//! Nothing in this build detects a divergence yet; the tests trip a running
//! node's stop through the test seam (`divergence::test_seam`), as a
//! detection will. A stopped node stops answering, returns `NodeDiverged`
//! once its threads are down, leaves its marker, and refuses every later
//! start.
//!
//! Nodes are started through `melin-test-node`: on kernel TCP by default,
//! on DPDK with this crate's `dpdk` feature, under
//! `scripts/dpdk/netns-runner.sh` (see `docs/internal/dpdk-testing.md`).

use std::time::{Duration, Instant};

use counter_server::{Counter, RequestDecoder, ResponseEncoder, increment_request};
use melin_client::SigningKey;
use melin_server_runtime::StartupEvents;
use melin_server_runtime::ack_policy::AckPolicy;
use melin_server_runtime::server::ServerConfig;
use melin_test_node::probe::wait_for_gauge;
use melin_transport_core::WireSeq;
use melin_transport_core::divergence::{HaltCause, test_seam};
use melin_transport_core::output_digest::{ChainValue, Divergence};
use melin_transport_core::replication::protocol::MSG_HALT;

#[path = "common/standalone.rs"]
mod standalone;
use standalone::standalone_config;

#[path = "common/halt_cluster.rs"]
mod halt_cluster;
use halt_cluster::{Fixture, addrs, spawn_node};

#[path = "common/proxy.rs"]
mod proxy;
use melin_transport_core::divergence::HALT_NOTICE_BOUND;
use proxy::Proxy;

const CLIENT_KEY: [u8; 32] = [0xD1; 32];

/// How long a stopped node has to return.
const STOP_WITHIN: Duration = Duration::from_secs(20);

fn start(config: ServerConfig) -> melin_test_node::Node {
    melin_test_node::start::<Counter>(
        config,
        StartupEvents::none(),
        (),
        RequestDecoder,
        ResponseEncoder,
    )
}

/// A divergence as a comparison would report it.
fn detected() -> HaltCause {
    HaltCause::Detected(Divergence {
        end: WireSeq::new(128),
        run_from: WireSeq::new(1),
        last_match: Some(WireSeq::new(64)),
        expected: ChainValue::from_bytes([0x11; 16]),
        actual: ChainValue::from_bytes([0x22; 16]),
    })
}

/// A serving node, tripped: it answers nothing more, returns
/// `NodeDiverged` with its marker written, and refuses to start again.
#[test]
fn a_tripped_node_stops_and_refuses_to_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let key = SigningKey::from_bytes(&CLIENT_KEY);
    let config = || standalone_config(dir.path(), &key);
    let marker = config().store().expect("store").divergence_marker();
    let node = start(config());

    let mut conn = melin_test_node::connect(node.addr(), &key);
    conn.set_read_timeout(STOP_WITHIN).expect("set timeout");
    conn.request_one(&increment_request(1))
        .expect("served before the stop");

    let halted = test_seam::trip(&marker, detected());
    assert!(halted.is_some(), "the running node's latch was found");

    // A request after the stop is never answered: the stopped node closes
    // its clients (kernel TCP) or answers nothing until its read times out
    // (a DPDK node keeps the connection open).
    conn.set_read_timeout(Duration::from_secs(2))
        .expect("set timeout");
    let after = conn.request_one(&increment_request(1));
    assert!(after.is_err(), "answered after the stop: {after:?}");

    let deadline = Instant::now() + STOP_WITHIN;
    while !node.is_finished() {
        assert!(Instant::now() < deadline, "the node did not stop");
        std::thread::sleep(Duration::from_millis(10));
    }
    let error = node.join().expect_err("a stopped node returns an error");
    assert!(
        error.contains("its output diverged from its primary's between positions 65 and 128"),
        "{error}"
    );
    assert!(
        error.contains("its marker refuses every restart"),
        "{error}"
    );
    assert!(marker.exists(), "the marker is beside the journal");

    // The store still resolves, for tools; a start is refused.
    config()
        .store()
        .expect("resolving writes and refuses nothing");
    let restarted = start(config()).join().expect_err("a restart is refused");
    for part in [
        "between positions 65 and 128",
        "do not delete this marker",
        "Start a new cluster",
    ] {
        assert!(
            restarted.contains(part),
            "{part:?} missing from {restarted}"
        );
    }
}

/// Wait for `node` to return, and return its error.
fn stopped(node: melin_test_node::Node) -> String {
    let deadline = Instant::now() + STOP_WITHIN;
    while !node.is_finished() {
        assert!(Instant::now() < deadline, "the node did not stop");
        std::thread::sleep(Duration::from_millis(10));
    }
    node.join().expect_err("a stopped node returns an error")
}

/// A stopped primary tells every replica streaming from it, with its
/// `Halt`: each stops, says who told it and how, leaves its marker, and
/// refuses to restart.
#[test]
fn a_stopped_primary_halts_its_replicas() {
    let fx = Fixture::new(0xD3);
    let primary_addrs = addrs(0);
    let primary_config = fx.primary_config(&primary_addrs, AckPolicy::DiskAndRam);
    let health = primary_config.health_bind.expect("set by the fixture");
    let primary_marker = primary_config.store().expect("store").divergence_marker();
    let primary = spawn_node::<Counter>(&primary_addrs, primary_config);
    wait_for_gauge(health, "melin_replicas_connected", 0);

    let replica_config =
        |name: &str| fx.replica_config(name, primary_addrs.replication(), AckPolicy::DiskAndRam);
    let replicas: Vec<_> = ["replica-a", "replica-b"]
        .into_iter()
        .enumerate()
        .map(|(i, name)| {
            let config = replica_config(name);
            let marker = config.store().expect("store").divergence_marker();
            let slot = i + 1;
            (
                name,
                slot,
                marker,
                spawn_node::<Counter>(&addrs(slot), config),
            )
        })
        .collect();
    wait_for_gauge(health, "melin_replicas_connected", 2);
    let mut client = fx.client(&primary);
    client
        .request_one(&increment_request(1))
        .expect("served by the whole cluster");

    let _halted = test_seam::trip(&primary_marker, detected()).expect("the primary runs");

    stopped(primary);
    for (name, slot, marker, node) in replicas {
        let error = stopped(node);
        assert!(
            error.contains("told by the primary via the halt frame"),
            "{name}: {error}"
        );
        assert!(marker.exists(), "{name}'s marker");
        // On the replica's own slot, free again now that it has returned.
        let restarted = spawn_node::<Counter>(&addrs(slot), replica_config(name))
            .join()
            .expect_err("a restart is refused");
        assert!(
            restarted.contains("do not delete this marker"),
            "{name}: {restarted}"
        );
    }
}

/// A replica that detects the divergence stops the whole cluster: its
/// stopped ack stops the primary, whose `Halt` answers it and stops the
/// other replica. Every node says why, leaves its marker, and nothing
/// serves after.
#[test]
fn a_detecting_replica_stops_the_whole_cluster() {
    let fx = Fixture::new(0xDB);
    let primary_addrs = addrs(0);
    let primary_config = fx.primary_config(&primary_addrs, AckPolicy::DiskAndRam);
    let health = primary_config.health_bind.expect("set by the fixture");
    let primary_marker = primary_config.store().expect("store").divergence_marker();
    let primary = spawn_node::<Counter>(&primary_addrs, primary_config);
    wait_for_gauge(health, "melin_replicas_connected", 0);

    let replica = |slot: usize, name: &str| {
        let config = fx.replica_config(name, primary_addrs.replication(), AckPolicy::DiskAndRam);
        let marker = config.store().expect("store").divergence_marker();
        (marker, spawn_node::<Counter>(&addrs(slot), config))
    };
    let (detector_marker, detector) = replica(1, "detector");
    let (other_marker, other) = replica(2, "other");
    wait_for_gauge(health, "melin_replicas_connected", 2);
    let mut client = fx.client(&primary);
    client
        .request_one(&increment_request(1))
        .expect("served by the whole cluster");

    let _halted = test_seam::trip(&detector_marker, detected()).expect("the replica runs");

    let error = stopped(detector);
    assert!(
        error.contains("its output diverged from its primary's between positions 65 and 128"),
        "detector: {error}"
    );
    let error = stopped(primary);
    assert!(
        error.contains("told by the replica on slot") && error.contains("via its acknowledgement"),
        "primary: {error}"
    );
    let error = stopped(other);
    assert!(
        error.contains("told by the primary via the halt frame"),
        "other: {error}"
    );
    for marker in [&detector_marker, &primary_marker, &other_marker] {
        assert!(marker.exists(), "{}", marker.display());
    }
}

/// A detector whose stopped ack never reaches its primary (its link frozen
/// at the trip) redials it once the in-session wait runs out, and its
/// stopped handshake, once the link is back, stops the primary.
#[test]
fn a_lost_stopped_ack_is_made_good_by_the_redial() {
    let fx = Fixture::new(0xDF);
    let primary_addrs = addrs(0);
    let primary_config = fx.primary_config(&primary_addrs, AckPolicy::DiskAndRam);
    let health = primary_config.health_bind.expect("set by the fixture");
    let primary = spawn_node::<Counter>(&primary_addrs, primary_config);
    wait_for_gauge(health, "melin_replicas_connected", 0);

    let proxy = Proxy::start(primary_addrs.replication());
    let config = fx.replica_config("detector", proxy.addr(), AckPolicy::DiskAndRam);
    let marker = config.store().expect("store").divergence_marker();
    let detector = spawn_node::<Counter>(&addrs(1), config);
    wait_for_gauge(health, "melin_replicas_connected", 1);

    proxy.freeze();
    let _halted = test_seam::trip(&marker, detected()).expect("the replica runs");
    // Past the replica's in-session wait for the `Halt`: the primary has
    // heard nothing.
    std::thread::sleep(HALT_NOTICE_BOUND + Duration::from_secs(1));
    assert!(!primary.is_finished(), "the frozen link carried the stop");
    // The held redial fails; the next one, through the restored link,
    // reaches the primary.
    proxy.cut();
    proxy.restore();

    let error = stopped(primary);
    assert!(
        error.contains("told by the replica on slot") && error.contains("via its handshake"),
        "primary: {error}"
    );
    let error = stopped(detector);
    assert!(
        error.contains("its output diverged from its primary's"),
        "detector: {error}"
    );
}

/// A replica that redials its primary as stopped, with a flagged
/// handshake, stops the primary, which answers it with `Halt` and sends
/// it nothing else: no stream, no snapshot.
#[test]
fn a_stopped_handshake_stops_the_primary_and_is_answered_with_halt() {
    let fx = Fixture::new(0xD7);
    let primary_addrs = addrs(0);
    let primary_config = fx.primary_config(&primary_addrs, AckPolicy::DiskAndRam);
    let health = primary_config.health_bind.expect("set by the fixture");
    let primary = spawn_node::<Counter>(&primary_addrs, primary_config);
    wait_for_gauge(health, "melin_replicas_connected", 0);

    let answer = stopped_handshake(primary_addrs.replication(), &fx.replica_key);
    assert_eq!(answer, [MSG_HALT], "the answer is the halt frame alone");

    let error = stopped(primary);
    assert!(
        error.contains("told by the replica on slot 0 via its handshake"),
        "{error}"
    );
}

/// A replica that tells its streaming primary it stopped, with a flagged
/// ack, stops the primary, which answers it with `Halt` and closes once
/// the replica does.
#[test]
fn a_stopped_ack_stops_the_primary_and_is_answered_with_halt() {
    use std::io::Write;

    use melin_transport_core::replication::protocol::{
        Ack, MSG_STREAM_START, encode_handshake, encode_stopped_ack,
    };

    let fx = Fixture::new(0xD9);
    let primary_addrs = addrs(0);
    let primary_config = fx.primary_config(&primary_addrs, AckPolicy::DiskAndRam);
    let health = primary_config.health_bind.expect("set by the fixture");
    let primary = spawn_node::<Counter>(&primary_addrs, primary_config);
    wait_for_gauge(health, "melin_replicas_connected", 0);

    let mut stream = authenticate(primary_addrs.replication(), &fx.replica_key);
    let mut out = Vec::new();
    encode_handshake(&fresh_handshake(), &mut out);
    stream.write_all(&out).expect("send the handshake");
    while read_frame(&mut stream).first() != Some(&MSG_STREAM_START) {}

    out.clear();
    encode_stopped_ack(
        &Ack {
            acked_sequence: 0,
            in_memory_sequence: 0,
        },
        &mut out,
    );
    stream.write_all(&out).expect("send the stopped ack");
    // Heartbeats may come first; the stream ends with the `Halt`.
    while read_frame(&mut stream) != [MSG_HALT] {}
    drop(stream);

    let error = stopped(primary);
    assert!(
        error.contains("told by the replica on slot 0 via its acknowledgement"),
        "{error}"
    );
}

/// The handshake of a replica with an empty journal.
fn fresh_handshake() -> melin_transport_core::replication::protocol::Handshake {
    melin_transport_core::replication::protocol::Handshake {
        last_sequence: 0,
        chain_hash: [0; 32],
        epoch: 0,
    }
}

/// One frame's payload from `stream`.
fn read_frame(stream: &mut std::net::TcpStream) -> Vec<u8> {
    use std::io::Read;
    let mut prefix = [0u8; 4];
    stream.read_exact(&mut prefix).expect("a frame's length");
    let mut payload = vec![0u8; u32::from_le_bytes(prefix) as usize];
    stream.read_exact(&mut payload).expect("a frame's payload");
    payload
}

/// Authenticate to `primary` as a replica keyed by `key`, handshake as
/// stopped, and return the first frame the primary answers with.
fn stopped_handshake(primary: std::net::SocketAddr, key: &SigningKey) -> Vec<u8> {
    use std::io::Write;

    use melin_transport_core::replication::protocol::encode_stopped_handshake;

    let mut stream = authenticate(primary, key);
    let mut out = Vec::new();
    encode_stopped_handshake(&fresh_handshake(), &mut out);
    stream.write_all(&out).expect("send the stopped handshake");
    read_frame(&mut stream)
}

/// Authenticate to `primary` as a replica keyed by `key`, stopping before
/// the handshake.
///
/// Repeats `common/raw_replica.rs`'s authentication rather than sharing
/// it: that module is included whole by each binary, which must use every
/// item it holds, and this one needs none of its others.
fn authenticate(primary: std::net::SocketAddr, key: &SigningKey) -> std::net::TcpStream {
    use std::io::Write;

    use ed25519_dalek::Signer;
    use melin_transport_core::replication::protocol::{
        AuthResult, REPL_PROTOCOL_VERSION, decode_auth_result, decode_challenge,
        encode_challenge_response,
    };

    let mut stream = std::net::TcpStream::connect(primary).expect("connect to the primary");
    stream
        .set_read_timeout(Some(STOP_WITHIN))
        .expect("set read timeout");
    let nonce = decode_challenge(&read_frame(&mut stream)).expect("a challenge");
    let mut out = Vec::new();
    encode_challenge_response(
        &key.sign(&nonce).to_bytes(),
        key.verifying_key().as_bytes(),
        &mut out,
    );
    stream.write_all(&out).expect("send the challenge response");
    assert_eq!(
        decode_auth_result(&read_frame(&mut stream)).expect("an auth result"),
        AuthResult::Ok {
            protocol_version: Some(REPL_PROTOCOL_VERSION)
        }
    );
    stream
}
