//! Smoke test: a raft-enabled server (single-voter control plane) boots,
//! elects itself, and serves the `melin_raft_*` gauges on `--health-bind`.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use base64::Engine;
use counter_server::{CounterFactory, RequestDecoder, ResponseEncoder};
use melin_server_runtime::server::{self, ServerConfig};
use melin_wire_protocol::tcp::BlockingTcpListener;
use serial_test::serial;

fn free_addr() -> SocketAddr {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap()
}

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

    let raft_addr = free_addr();
    let health_addr = free_addr();

    let listener = BlockingTcpListener::bind("127.0.0.1:0".parse::<SocketAddr>().unwrap())
        .expect("bind client listener");
    let addr = listener.local_addr().expect("local_addr");

    let config = ServerConfig {
        bind: addr,
        journal: tmp.path().join("smoke.journal"),
        authorized_keys: auth_path,
        standalone: true,
        durability_mode: melin_server_runtime::durability_policy::DurabilityMode::Local,
        no_mlock: true,
        tick_interval_ms: 0,
        snapshot_interval_ms: 0,
        health_bind: Some(health_addr),
        accounts: 0,
        instruments: 0,
        replication_key: Some(key_path),
        raft_bind: Some(raft_addr),
        raft_node_id: Some(1),
        raft_peer: vec![format!("1@{raft_addr}#{pub_b64}")],
        raft_dir: Some(tmp.path().join("smoke.raft")),
        ..ServerConfig::default()
    };

    let shutdown = Arc::new(AtomicBool::new(false));
    let sd = Arc::clone(&shutdown);
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

    // A single voter elects itself within the 1–2 s election timeout;
    // poll the real health endpoint for the gauges.
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut led = false;
    while Instant::now() < deadline {
        if handle.is_finished() {
            panic!("server exited early: {:?}", handle.join().unwrap());
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

    shutdown.store(true, Ordering::Relaxed);
    // Nudge the accept loop past its poll.
    let _ = TcpStream::connect_timeout(&addr, Duration::from_millis(100));
    let result = handle.join().expect("server thread panicked");
    assert!(led, "raft gauges never reported leadership");
    result.expect("server returned error");
}
