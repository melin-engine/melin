//! Smoke test: a raft-enabled server (single-voter control plane) boots,
//! elects itself, and serves the `melin_raft_*` gauges on `--health-bind`.
//!
//! The node is started through `melin-test-node`, so it runs on DPDK with
//! this crate's `dpdk` feature; raft and the health endpoint are kernel
//! TCP on every node, on the namespace's loopback under the runner.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use base64::Engine;
use counter_server::{Counter, RequestDecoder, ResponseEncoder};
use melin_server_runtime::StartupEvents;
use melin_server_runtime::layout::PipelineCores;
use melin_server_runtime::server::ServerConfig;
use melin_transport_core::test_ports::free_addr;
use serial_test::serial;

/// Port range this file owns for `free_addr` (25000..30000);
/// `raft_failover.rs` owns 20000..25000.
const PORT_BASE: u16 = 25_000;

fn http_metrics(addr: SocketAddr) -> Option<String> {
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_millis(200)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(2))).ok()?;
    stream.write_all(b"GET /metrics HTTP/1.1\r\n\r\n").ok()?;
    let mut body = String::new();
    stream.read_to_string(&mut body).ok()?;
    Some(body)
}

#[test]
#[serial]
fn raft_enabled_server_elects_itself_and_serves_gauges() {
    let tmp = tempfile::tempdir().expect("tempdir");

    // Node identity: one replication key, listed in authorized_keys and in
    // the (single-entry) peer list.
    let signing_key = ed25519_dalek::SigningKey::from_bytes(&[0x42; 32]);
    let key_path = tmp.path().join("replication_key");
    std::fs::write(&key_path, signing_key.to_bytes()).unwrap();
    let pub_b64 =
        base64::engine::general_purpose::STANDARD.encode(signing_key.verifying_key().to_bytes());
    let auth_path = tmp.path().join("authorized_keys");
    std::fs::write(&auth_path, format!("replication {pub_b64} node-1\n")).unwrap();

    let raft_addr = free_addr(PORT_BASE);
    let health_addr = free_addr(PORT_BASE);

    let config = ServerConfig {
        journal: tmp.path().join("smoke.journal"),
        authorized_keys: auth_path,
        standalone: true,
        ack_policy: melin_server_runtime::ack_policy::AckPolicy::Disk,
        no_mlock: true,
        // Unpinned, and therefore yielding: the suite runs many nodes at
        // once, and the default layout would stack every node's same-role
        // thread on one core while a spinner would starve whatever shares
        // its core, the test's own client included.
        cores: PipelineCores::unpinned(),
        tick_interval_ms: 0,
        snapshot_interval_ms: 0,
        health_bind: Some(health_addr),
        replication_key: Some(key_path),
        raft_bind: Some(raft_addr),
        raft_node_id: Some(1),
        raft_peer: vec![format!("1@{raft_addr}#{pub_b64}")],
        raft_dir: Some(tmp.path().join("smoke.raft")),
        ..ServerConfig::default()
    };

    let node = melin_test_node::start::<Counter>(
        config,
        StartupEvents::none(),
        (),
        RequestDecoder,
        ResponseEncoder,
    );

    // A single voter elects itself within the 1–2 s election timeout;
    // poll the real health endpoint for the gauges.
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut led = false;
    while Instant::now() < deadline {
        if node.is_finished() {
            panic!("server exited early: {:?}", node.join());
        }
        if let Some(body) = http_metrics(health_addr)
            && body.contains("melin_raft_is_leader 1\n")
        {
            assert!(body.contains("melin_raft_node_id 1\n"), "{body}");
            assert!(body.contains("melin_raft_driver_running 1\n"), "{body}");
            assert!(body.contains("melin_raft_leader_id 1\n"), "{body}");
            led = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    node.stop();
    assert!(led, "raft gauges never reported leadership");
}
