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
use melin_server_runtime::server::ServerConfig;
use melin_transport_core::WireSeq;
use melin_transport_core::divergence::{HaltCause, test_seam};
use melin_transport_core::output_digest::{ChainValue, Divergence};

#[path = "common/standalone.rs"]
mod standalone;
use standalone::standalone_config;

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
