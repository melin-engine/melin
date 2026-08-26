//! End-to-end contract test for the `ram` ack policy: acks gate on a
//! second node's *in-memory* receipt only — no disk anywhere on the ack
//! path — so every acked event must survive the death of the primary
//! via failover.
//!
//! A primary and two replicas (counter app, `--ack-policy ram`, raft +
//! `--raft-auto-promote` on all three) over real TCP and real Ed25519
//! auth. A client sends increments and collects acks — proof the
//! RAM-quorum gate opens on live replica cursors. The primary is then
//! killed; exactly one replica must auto-promote (exercising the
//! ack-policy byte end-to-end: the primary advertised `ram` on the
//! replication stream, and the promotion policy must accept it on the
//! same grounds as `disk+ram`). The operator then swaps the promoted
//! node to `disk` over the admin endpoint — the documented
//! post-failover workflow, since its own gate is unsatisfiable with no
//! replicas attached — and a fresh client connection must read back the
//! full acked total: nothing the client was told about died with the
//! primary.
//!
//! The final assertion is the reproducer that surfaced the promotion
//! peer-tip veto: without it, the election could land on the replica
//! that *lacks* the last acked event (the ack quorum is the primary
//! plus the faster replica, and the journal-tip vote filter is
//! best-effort by design), and this test failed ~40% of runs with an
//! acked increment missing after failover. It now passes because a
//! behind leader refuses to promote while a live peer advertises a
//! higher tip, and the caught-up peer campaigns to take over — see
//! `raft_promotion::auto_promotion_decision` and
//! `challenger_should_campaign`.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use base64::Engine;
use counter_server::{CounterFactory, RequestDecoder, ResponseEncoder};
use ed25519_dalek::{Signer, SigningKey};
use melin_server_runtime::ack_policy::AckPolicy;
use melin_server_runtime::server::{self, ServerConfig};
use melin_transport_core::test_ports::free_addr;
use melin_wire_protocol::control_codec::{
    TAG_BATCH_END, TAG_CHALLENGE, TAG_CHALLENGE_RESPONSE, TAG_SERVER_READY,
};
use melin_wire_protocol::tcp::BlockingTcpListener;
use serial_test::serial;

/// Port range this file owns for `free_addr` (10000..15000);
/// `melin-raft`'s election tests own 15000..20000, `raft_failover.rs`
/// 20000..25000, `raft_smoke.rs` 25000..30000, and the notary example's
/// `round_trip.rs` 5000..10000. See `test_ports::free_addr` for the
/// scheme.
const PORT_BASE: u16 = 10_000;

const TAG_INCREMENT: u8 = 0x10;
const TAG_GET_VALUE: u8 = 0x11;
const TAG_RESP_ACK: u8 = 0x30;
const TAG_RESP_VALUE: u8 = 0x31;

// ---------------------------------------------------------------------------
// Wire helpers (client and admin share the challenge-response handshake)
// ---------------------------------------------------------------------------

fn read_frame(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload)?;
    Ok(payload)
}

fn write_frame(stream: &mut TcpStream, payload: &[u8]) {
    let len = (payload.len() as u32).to_le_bytes();
    stream.write_all(&len).expect("write frame length");
    stream.write_all(payload).expect("write frame payload");
    stream.flush().expect("flush");
}

/// Answer the server's Challenge on an already-connected stream and
/// consume the ServerReady. Returns `None` if the handshake stalls or
/// the frames are not the expected tags (e.g. the node is not serving
/// yet) — callers retry.
fn answer_challenge(stream: &mut TcpStream, key: &SigningKey) -> Option<()> {
    let challenge = read_frame(stream).ok()?;
    if challenge.first() != Some(&TAG_CHALLENGE) {
        return None;
    }
    let nonce = &challenge[1..33];
    let signature = key.sign(nonce);
    let mut frame = Vec::with_capacity(105);
    frame.extend_from_slice(&0u64.to_le_bytes());
    frame.push(TAG_CHALLENGE_RESPONSE);
    frame.extend_from_slice(&signature.to_bytes());
    frame.extend_from_slice(&key.verifying_key().to_bytes());
    write_frame(stream, &frame);
    let ready = read_frame(stream).ok()?;
    (ready.first() == Some(&TAG_SERVER_READY)).then_some(())
}

/// Connect and authenticate a trading client, retrying until the node
/// serves. The kernel backlog accepts the TCP SYN before the accept
/// loop runs, so a successful `connect` alone proves nothing.
fn connect_authenticated(addr: SocketAddr, key: &SigningKey) -> TcpStream {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(mut stream) = TcpStream::connect_timeout(&addr, Duration::from_millis(300)) {
            stream
                .set_read_timeout(Some(Duration::from_millis(500)))
                .expect("set timeout");
            if answer_challenge(&mut stream, key).is_some() {
                // Generous post-auth timeout: acks under `ram`
                // wait on a live replica round-trip, and CI machines
                // schedule these three servers on shared cores.
                stream
                    .set_read_timeout(Some(Duration::from_secs(30)))
                    .expect("set timeout");
                return stream;
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for a serving node at {addr}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn send_request(stream: &mut TcpStream, seq: u64, tag: u8, payload: &[u8]) {
    let mut frame = Vec::with_capacity(9 + payload.len());
    frame.extend_from_slice(&seq.to_le_bytes());
    frame.push(tag);
    frame.extend_from_slice(payload);
    write_frame(stream, &frame);
}

fn read_until_batch_end(stream: &mut TcpStream) -> Vec<Vec<u8>> {
    let mut responses = Vec::new();
    loop {
        let frame = read_frame(stream).expect("read response frame");
        if frame[0] == TAG_BATCH_END {
            break;
        }
        responses.push(frame);
    }
    responses
}

/// Send one admin command over a fresh authenticated connection and
/// return the server's reply line, or `None` if the connection or
/// handshake failed (node not up yet — caller retries).
fn admin_command(addr: SocketAddr, key: &SigningKey, command: &str) -> Option<String> {
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_millis(300)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
    answer_challenge(&mut stream, key)?;
    stream.write_all(command.as_bytes()).ok()?;
    stream.write_all(b"\n").ok()?;
    stream.flush().ok()?;
    let mut line = String::new();
    BufReader::new(stream).read_line(&mut line).ok()?;
    Some(line.trim_end().to_owned())
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
    matches!(read_frame(&mut stream), Ok(f) if f.first() == Some(&TAG_CHALLENGE))
}

fn http_metrics(addr: SocketAddr) -> Option<String> {
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_millis(200)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(2))).ok()?;
    stream.write_all(b"GET /metrics HTTP/1.1\r\n\r\n").ok()?;
    let mut body = String::new();
    stream.read_to_string(&mut body).ok()?;
    Some(body)
}

// ---------------------------------------------------------------------------
// Cluster scaffolding
// ---------------------------------------------------------------------------

struct NodeSetup {
    key: SigningKey,
    client_addr: SocketAddr,
    raft_addr: SocketAddr,
    health_addr: SocketAddr,
    admin_addr: SocketAddr,
}

/// Deadline diagnostics: the servers install no tracing subscriber
/// under test, so the panic message is the only record of what each
/// node believed when a wait timed out.
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
fn acked_events_survive_primary_death_under_ram_policy() {
    // Opt-in diagnostics: with RUST_LOG set, capture the nodes' tracing
    // output (all three run in this process) so a failing run records
    // the control-plane election dialogue, not just the panic-time
    // metrics snapshot. No-op when RUST_LOG is unset.
    if std::env::var_os("RUST_LOG").is_some() {
        // Error dropped deliberately: try_init fails only when a
        // subscriber is already installed (an earlier #[serial] test),
        // which is exactly the state we want.
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_test_writer()
            .try_init();
    }
    let tmp = tempfile::tempdir().expect("tempdir");
    let replication_addr = free_addr(PORT_BASE);

    let nodes: Vec<NodeSetup> = (0..3)
        .map(|i| NodeSetup {
            key: SigningKey::from_bytes(&[0x61 + i as u8; 32]),
            client_addr: free_addr(PORT_BASE),
            raft_addr: free_addr(PORT_BASE),
            health_addr: free_addr(PORT_BASE),
            admin_addr: free_addr(PORT_BASE),
        })
        .collect();
    // One key for both trading and admin: the operator permission
    // covers each.
    let client_key = SigningKey::from_bytes(&[0x11; 32]);

    let b64 = |k: &SigningKey| {
        base64::engine::general_purpose::STANDARD.encode(k.verifying_key().to_bytes())
    };
    let auth_path = tmp.path().join("authorized_keys");
    let mut table: String = nodes
        .iter()
        .enumerate()
        .map(|(i, n)| format!("replication {} node-{}\n", b64(&n.key), i + 1))
        .collect();
    table.push_str(&format!("operator {} client\n", b64(&client_key)));
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
            ack_policy: AckPolicy::Ram,
            no_mlock: true,
            tick_interval_ms: 0,
            snapshot_interval_ms: 0,
            health_bind: Some(nodes[i].health_addr),
            admin_bind: Some(nodes[i].admin_addr),
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

    // --- Primary (node 0): `ram` ack policy, replication bind. ---
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

    // --- Phase 1: cluster forms (both replicas attached, a raft leader
    // elected). Client traffic waits for this so the RAM-quorum gate is
    // satisfiable when the first increment arrives.
    let deadline = Instant::now() + Duration::from_secs(60);
    let leader_at_formation = loop {
        if primary_handle.is_finished() {
            panic!("primary exited early: {:?}", primary_handle.join().unwrap());
        }
        let connected = http_metrics(nodes[0].health_addr)
            .is_some_and(|m| m.contains("melin_replicas_connected 2\n"));
        let leader = nodes.iter().position(|n| {
            http_metrics(n.health_addr).is_some_and(|m| m.contains("melin_raft_is_leader 1\n"))
        });
        if connected && leader.is_some() {
            break leader;
        }
        assert!(
            Instant::now() < deadline,
            "cluster never formed: replicas_connected_2={connected} leader={leader:?}\n{}",
            cluster_summary(&nodes)
        );
        std::thread::sleep(Duration::from_millis(200));
    };

    // --- Phase 2: acked client traffic. Each response returned only
    // after a replica confirmed in-memory receipt — the `ram` gate
    // live, on real cursors. Total after 1 + 2 + 4 = 7.
    {
        let mut stream = connect_authenticated(nodes[0].client_addr, &client_key);
        let mut expected_total = 0u64;
        for (seq, amount) in [(1u64, 1u64), (2, 2), (3, 4)] {
            expected_total += amount;
            send_request(&mut stream, seq, TAG_INCREMENT, &amount.to_le_bytes());
            let responses = read_until_batch_end(&mut stream);
            assert_eq!(responses.len(), 1);
            assert_eq!(responses[0][0], TAG_RESP_ACK, "increment must be acked");
            let value = u64::from_le_bytes(responses[0][1..9].try_into().unwrap());
            assert_eq!(value, expected_total, "ack carries the running total");
        }
    }

    // --- Phase 3: kill the primary. Every acked event now exists only
    // on the surviving nodes (their RAM, and their journals as their
    // own disk syncs trail through). ---
    primary_shutdown.store(true, Ordering::Relaxed);
    let _ = TcpStream::connect_timeout(&nodes[0].client_addr, Duration::from_millis(100));
    primary_handle
        .join()
        .expect("primary thread panicked")
        .expect("primary returned error");

    // --- Phase 4: exactly one replica auto-promotes. The promotion
    // policy sees the ack policy the dead primary advertised on the
    // replication stream — `ram` — and must accept it (an ack
    // always waited for a second node, so the election recency filter
    // proves the winner holds every acked event). ---
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
    let loser = if winner == 1 { 2 } else { 1 };
    assert!(
        !serves_clients(nodes[loser].client_addr, Duration::from_secs(2)),
        "the losing replica must not serve clients"
    );

    // --- Phase 5: the documented post-failover workflow. The promoted
    // node still runs `ram`, whose gate is structurally
    // unsatisfiable with no replicas attached (fail-closed), so the
    // operator swaps it to `disk` over the admin endpoint. Retried:
    // the admin listener is up from boot, but promotion may still be
    // settling when the first command lands.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match admin_command(nodes[winner].admin_addr, &client_key, "ACK-POLICY disk") {
            Some(reply) if reply == "OK" => break,
            reply => {
                assert!(
                    Instant::now() < deadline,
                    "ACK-POLICY disk never accepted by the promoted node; last reply: {reply:?}"
                );
                std::thread::sleep(Duration::from_millis(200));
            }
        }
    }

    // --- Phase 6: the acked total survived. A fresh client reads the
    // counter from the new primary; every event the old primary acked
    // under the `ram` ack policy must be in it. ---
    {
        let mut stream = connect_authenticated(nodes[winner].client_addr, &client_key);
        send_request(&mut stream, 1, TAG_GET_VALUE, &[]);
        let responses = read_until_batch_end(&mut stream);
        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0][0], TAG_RESP_VALUE);
        let value = u64::from_le_bytes(responses[0][1..9].try_into().unwrap());
        let seqs: Vec<String> = (1..3)
            .map(|i| {
                let m = http_metrics(nodes[i].health_addr).unwrap_or_default();
                let line = m
                    .lines()
                    .filter(|l| {
                        l.starts_with("melin_journal_sequence")
                            || l.starts_with("melin_events_processed")
                    })
                    .collect::<Vec<_>>()
                    .join(" ");
                format!("node {}: {line}", i + 1)
            })
            .collect();
        assert_eq!(
            value,
            7,
            "the promoted node must hold every event acked under `ram` \
             (winner=node {}, leader at formation=node {:?}, {})",
            winner + 1,
            leader_at_formation.map(|i| i + 1),
            seqs.join("; ")
        );
    }

    // Teardown.
    replica_shutdown.store(true, Ordering::Relaxed);
    for (i, h) in replica_handles.into_iter().enumerate() {
        let _ = TcpStream::connect_timeout(&nodes[i + 1].client_addr, Duration::from_millis(100));
        h.join()
            .expect("replica thread panicked")
            .expect("replica returned error");
    }
}
