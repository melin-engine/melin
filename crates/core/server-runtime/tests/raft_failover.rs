//! End-to-end auto-promotion: a primary and two replicas (counter app,
//! hybrid durability, raft + `--raft-auto-promote` on all three), real
//! TCP and real Ed25519 auth throughout. Kill the primary; exactly one
//! replica must win the election and promote itself into a serving
//! primary, and the other must keep following. Then the killed primary
//! comes back on its stale epoch-0 journal — the raft peer mesh must
//! fence it (fence-on-supersession) so it self-demotes rather than
//! serving clients alongside the new primary.

use std::io::Read;
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use base64::Engine;
use counter_server::{CounterFactory, RequestDecoder, ResponseEncoder};
use melin_server_runtime::server::{self, ServerConfig};
use melin_transport_core::test_ports::free_addr;
use melin_wire_protocol::control_codec::TAG_CHALLENGE;
use melin_wire_protocol::tcp::BlockingTcpListener;
use serial_test::serial;

/// Port range this file owns for `free_addr` (20000..25000);
/// `raft_smoke.rs` owns 25000..30000, `melin-raft`'s election tests
/// 15000..20000. A stolen replication port once left this cluster
/// unable to ever form — see `test_ports::free_addr` for the scheme.
const PORT_BASE: u16 = 20_000;

fn http_metrics(addr: SocketAddr) -> Option<String> {
    use std::io::Write;
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_millis(200)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(2))).ok()?;
    stream.write_all(b"GET /metrics HTTP/1.1\r\n\r\n").ok()?;
    let mut body = String::new();
    stream.read_to_string(&mut body).ok()?;
    Some(body)
}

/// Whether a client connect to `addr` is answered with the auth
/// Challenge — the definitive "this node serves as primary" signal (a
/// replica's client listener never accepts).
fn serves_clients(addr: SocketAddr, patience: Duration) -> bool {
    let Ok(mut stream) = TcpStream::connect_timeout(&addr, Duration::from_millis(300)) else {
        return false;
    };
    if stream.set_read_timeout(Some(patience)).is_err() {
        return false;
    }
    let mut len_buf = [0u8; 4];
    if stream.read_exact(&mut len_buf).is_err() {
        return false;
    }
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).is_ok() && payload.first() == Some(&TAG_CHALLENGE)
}

struct NodeSetup {
    key: ed25519_dalek::SigningKey,
    client_addr: SocketAddr,
    raft_addr: SocketAddr,
    health_addr: SocketAddr,
}

/// Per-node control-plane state for deadline diagnostics: the raft and
/// replication gauges of every node, so a timed-out wait reports *which*
/// formation condition was stuck (replication attach vs. election) and
/// what each node believed at the time — the servers install no tracing
/// subscriber under test, so the panic message is the only record.
fn cluster_summary(nodes: &[NodeSetup]) -> String {
    nodes
        .iter()
        .enumerate()
        .map(|(i, n)| {
            let gauges = match http_metrics(n.health_addr) {
                None => "health endpoint unreachable".to_owned(),
                Some(body) => body
                    .lines()
                    .filter(|l| {
                        l.starts_with("melin_raft_") || l.starts_with("melin_replicas_connected")
                    })
                    .collect::<Vec<_>>()
                    .join(" "),
            };
            format!("node {}: {gauges}", i + 1)
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
#[serial]
fn killed_primary_triggers_exactly_one_auto_promotion() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let replication_addr = free_addr(PORT_BASE);

    let nodes: Vec<NodeSetup> = (0..3)
        .map(|i| NodeSetup {
            key: ed25519_dalek::SigningKey::from_bytes(&[0x51 + i as u8; 32]),
            client_addr: free_addr(PORT_BASE),
            raft_addr: free_addr(PORT_BASE),
            health_addr: free_addr(PORT_BASE),
        })
        .collect();

    let b64 = |k: &ed25519_dalek::SigningKey| {
        base64::engine::general_purpose::STANDARD.encode(k.verifying_key().to_bytes())
    };
    let auth_path = tmp.path().join("authorized_keys");
    let table: String = nodes
        .iter()
        .enumerate()
        .map(|(i, n)| format!("replication {} node-{}\n", b64(&n.key), i + 1))
        .collect();
    std::fs::write(&auth_path, table).unwrap();

    // Identical peer list on every node, self included.
    let peers: Vec<String> = nodes
        .iter()
        .enumerate()
        .map(|(i, n)| format!("{}@{}#{}", i + 1, n.raft_addr, b64(&n.key)))
        .collect();

    let make_config = |i: usize| -> ServerConfig {
        let key_path = tmp.path().join(format!("key-{i}"));
        std::fs::write(&key_path, nodes[i].key.to_bytes()).unwrap();
        ServerConfig {
            bind: nodes[i].client_addr,
            journal: tmp.path().join(format!("node-{i}.journal")),
            authorized_keys: auth_path.clone(),
            no_mlock: true,
            tick_interval_ms: 0,
            snapshot_interval_ms: 0,
            health_bind: Some(nodes[i].health_addr),
            accounts: 0,
            instruments: 0,
            replication_key: Some(key_path),
            raft_bind: Some(nodes[i].raft_addr),
            raft_node_id: Some(i as u64 + 1),
            raft_peer: peers.clone(),
            raft_dir: Some(tmp.path().join(format!("node-{i}.raft"))),
            raft_auto_promote: true,
            ..ServerConfig::default()
        }
    };

    // --- Primary (node 0): hybrid durability, replication bind. ---
    let primary_shutdown = Arc::new(AtomicBool::new(false));
    let primary_handle = {
        let mut config = make_config(0);
        config.replication_bind = Some(replication_addr);
        let listener = BlockingTcpListener::bind(config.bind).expect("bind primary client port");
        let sd = Arc::clone(&primary_shutdown);
        std::thread::spawn(move || -> Result<(), String> {
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
        })
    };

    // --- Replicas (nodes 1, 2). ---
    let replica_shutdown = Arc::new(AtomicBool::new(false));
    let replica_handles: Vec<_> = (1..3)
        .map(|i| {
            let mut config = make_config(i);
            config.replica_of = Some(replication_addr);
            let listener =
                BlockingTcpListener::bind(config.bind).expect("bind replica client port");
            let sd = Arc::clone(&replica_shutdown);
            std::thread::spawn(move || -> Result<(), String> {
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
            })
        })
        .collect();

    // --- Phase 1: cluster forms. Both replicas connected, a raft leader
    // elected, and no replica has promoted (the primary link is up, and
    // if the primary itself leads there is nothing to act on).
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if primary_handle.is_finished() {
            panic!("primary exited early: {:?}", primary_handle.join().unwrap());
        }
        let connected = http_metrics(nodes[0].health_addr)
            .is_some_and(|m| m.contains("melin_replicas_connected 2\n"));
        let any_leader = nodes.iter().any(|n| {
            http_metrics(n.health_addr).is_some_and(|m| m.contains("melin_raft_is_leader 1\n"))
        });
        if connected && any_leader {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "cluster never formed: replicas_connected_2={connected} any_leader={any_leader}\n{}",
            cluster_summary(&nodes)
        );
        std::thread::sleep(Duration::from_millis(200));
    }

    // --- Phase 2: kill the primary. ---
    primary_shutdown.store(true, Ordering::Relaxed);
    // Nudge its accept loop past the poll.
    let _ = TcpStream::connect_timeout(&nodes[0].client_addr, Duration::from_millis(100));
    primary_handle
        .join()
        .expect("primary thread panicked")
        .expect("primary returned error");

    // --- Phase 3: exactly one replica promotes and serves clients. ---
    let deadline = Instant::now() + Duration::from_secs(60);
    let winner = loop {
        assert!(
            Instant::now() < deadline,
            "no replica promoted within the deadline\n{}",
            cluster_summary(&nodes)
        );
        let serving: Vec<usize> = (1..3)
            .filter(|&i| serves_clients(nodes[i].client_addr, Duration::from_millis(500)))
            .collect();
        match serving.len() {
            0 => std::thread::sleep(Duration::from_millis(200)),
            1 => break serving[0],
            _ => panic!("both replicas promoted — split brain: {serving:?}"),
        }
    };

    // The winner's health endpoint (rebound by run_as_primary) reports
    // leadership and a term that minted its fencing epoch (>= 1; the
    // genesis primary's epoch was 0, and auto-promotion requires the
    // term strictly above the epoch in force).
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(m) = http_metrics(nodes[winner].health_addr)
            && m.contains("melin_raft_is_leader 1\n")
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "winner's health endpoint never reported leadership\n{}",
            cluster_summary(&nodes)
        );
        std::thread::sleep(Duration::from_millis(200));
    }

    // The loser must still be following: not serving clients, not leading.
    let loser = if winner == 1 { 2 } else { 1 };
    assert!(
        !serves_clients(nodes[loser].client_addr, Duration::from_secs(2)),
        "the losing replica must not serve clients"
    );
    if let Some(m) = http_metrics(nodes[loser].health_addr) {
        assert!(
            !m.contains("melin_raft_is_leader 1\n"),
            "the losing replica must not report leadership: {m}"
        );
    }

    // --- Phase 4: the killed primary comes back and must be fenced by the
    // raft mesh. It recovers its stale epoch-0 journal and so boots as a
    // primary that *claims to be serving* — exactly the split-brain risk
    // fencing exists to close. It cannot disrupt the new leader: the
    // recency filter drops its epoch-0 vote requests, and its raft log is
    // missing the winner's post-election entry so no caught-up peer will
    // grant it either. The leader therefore stays put, and its next append
    // — inbound to this node — trips `fence_if_superseded`, which co-sets
    // the process shutdown flag. That flag is the very Arc we hand to
    // `run_with_listener` (the `SupersessionPolicy` holds a clone), so the
    // node tears its own server down and the flag we never write to
    // ourselves ends up set — the fingerprint of fence-on-supersession,
    // the only thing that stops a serving node from the inside (a driver
    // fatal leaves trading up). Requires `--raft-auto-promote` (already
    // set), which is what arms the `SupersessionPolicy`.
    let revived_shutdown = Arc::new(AtomicBool::new(false));
    let revived_handle = {
        // Reuse node 0's identity, raft address, journal, and raft dir; a
        // fresh client/health port avoids the old ones lingering in
        // TIME_WAIT. No replication bind — this exercises the raft-mesh
        // fencing channel in isolation, not a data-plane handshake.
        let mut config = make_config(0);
        config.bind = free_addr(PORT_BASE);
        config.health_bind = Some(free_addr(PORT_BASE));
        let listener =
            BlockingTcpListener::bind(config.bind).expect("bind revived primary client port");
        let sd = Arc::clone(&revived_shutdown);
        std::thread::spawn(move || -> Result<(), String> {
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
        })
    };

    let deadline = Instant::now() + Duration::from_secs(60);
    while !revived_handle.is_finished() {
        assert!(
            Instant::now() < deadline,
            "the revived ex-primary was never fenced by the raft mesh"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    // We never write this flag ourselves, so its being set proves the
    // node fenced itself (fence-on-supersession co-sets the process
    // shutdown flag it shares with the runtime).
    assert!(
        revived_shutdown.load(Ordering::Relaxed),
        "the revived node stopped without fencing — supersession did not fire"
    );
    revived_handle
        .join()
        .expect("revived primary thread panicked")
        .expect("revived primary returned an error instead of fencing cleanly");

    // Teardown.
    replica_shutdown.store(true, Ordering::Relaxed);
    for (i, h) in replica_handles.into_iter().enumerate() {
        // Nudge the promoted node's accept loop.
        let _ = TcpStream::connect_timeout(&nodes[i + 1].client_addr, Duration::from_millis(100));
        h.join()
            .expect("replica thread panicked")
            .expect("replica returned error");
    }
}
