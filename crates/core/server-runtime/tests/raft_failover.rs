//! End-to-end auto-promotion: a primary and two replicas (counter app,
//! raft + `--raft-auto-promote` on all three), real TCP and real Ed25519
//! auth throughout.
//!
//! - Kill the primary (`disk+ram`); exactly one replica must win the
//!   election and promote itself into a serving primary, and the other
//!   must keep following. Then the killed primary comes back on its stale
//!   epoch-0 journal — the raft peer mesh must fence it
//!   (fence-on-supersession) so it self-demotes rather than serving
//!   clients alongside the new primary. The replicas judge the policy by
//!   the change the primary journaled as its lineage began, which they
//!   hold.
//! - Finding 10 of the determinism audit, both ways, with clock ticks
//!   flowing, so the primary never sends an idle heartbeat: a runtime
//!   `ACK-POLICY disk` reaches the replicas in the stream, and none
//!   auto-promotes once the primary dies; a runtime `ACK-POLICY disk+ram`
//!   on a primary that began under `disk` does too, and the failover
//!   proceeds, the promoted node's gate starting from that lineage policy.
//!
//! Nodes are started through `melin-test-node`: on kernel TCP by default,
//! on DPDK with this crate's `dpdk` feature, under
//! `scripts/dpdk/netns-runner.sh` (see
//! `docs/internal/dpdk-testing.md`). Raft is kernel TCP on every
//! node. On DPDK the primary's orderly stop resets its replicas' links,
//! which they act on at once, and the winner serves as a DPDK primary.

use std::io::Read;
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use base64::Engine;
use counter_server::{Counter, KIND_RESP_ACK, RequestDecoder, ResponseEncoder, increment_request};
use melin_server_runtime::StartupEvents;
use melin_server_runtime::ack_policy::AckPolicy;
use melin_server_runtime::layout::PipelineCores;
use melin_server_runtime::server::ServerConfig;
use melin_test_node::{Addrs, Node};
use melin_transport_core::test_ports::free_addr;
use melin_wire_protocol::control_codec::TAG_CHALLENGE;
use serial_test::serial;

#[path = "common/probe.rs"]
mod probe;
use probe::{admin_command, gauge, wait_for_gauge};

/// Port range this file owns for `free_addr` (20000..25000);
/// `raft_smoke.rs` owns 25000..30000, `melin-raft`'s election tests
/// 15000..20000. A stolen replication port once left this cluster
/// unable to ever form — see `test_ports::free_addr` for the scheme.
const PORT_BASE: u16 = 20_000;

/// The policy gauges, as the policy's wire byte (`AckPolicy::as_u8`).
const IN_FORCE: &str = "melin_ack_policy_in_force";
const JOURNALED: &str = "melin_ack_policy_journaled";

/// The primary's clock-tick cadence in the finding-10 tests: fast, so a
/// tick is always on the link and the sender never goes idle long enough
/// to heartbeat.
const TICK_MS: u64 = 5;

/// How long the finding-10 refusal is watched once the primary is gone:
/// past the link-down grace, an election and a promotion, which a replica
/// judging by a stale `disk+ram` would have completed.
const NO_PROMOTION_WATCH: Duration = Duration::from_secs(12);

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
    /// Client and replication addresses.
    addrs: Addrs,
    raft_addr: SocketAddr,
    health_addr: SocketAddr,
    admin_addr: SocketAddr,
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

/// Three nodes' identities and addresses, the shared `authorized_keys`
/// (the three replication keys and an operator's) and raft peer list,
/// and their journals' directory.
struct Cluster {
    tmp: tempfile::TempDir,
    nodes: Vec<NodeSetup>,
    auth_path: PathBuf,
    peers: Vec<String>,
    operator: ed25519_dalek::SigningKey,
}

impl Cluster {
    fn new() -> Self {
        let tmp = tempfile::tempdir().expect("tempdir");
        let nodes: Vec<NodeSetup> = (0..3)
            .map(|i| NodeSetup {
                key: ed25519_dalek::SigningKey::from_bytes(&[0x51 + i as u8; 32]),
                addrs: melin_test_node::addrs(i, || free_addr(PORT_BASE)),
                raft_addr: free_addr(PORT_BASE),
                health_addr: free_addr(PORT_BASE),
                admin_addr: free_addr(PORT_BASE),
            })
            .collect();
        let operator = ed25519_dalek::SigningKey::from_bytes(&[0x5F; 32]);

        let b64 = |k: &ed25519_dalek::SigningKey| {
            base64::engine::general_purpose::STANDARD.encode(k.verifying_key().to_bytes())
        };
        let auth_path = tmp.path().join("authorized_keys");
        let mut table: String = nodes
            .iter()
            .enumerate()
            .map(|(i, n)| format!("replication {} node-{}\n", b64(&n.key), i + 1))
            .collect();
        table.push_str(&format!("operator {} operator\n", b64(&operator)));
        std::fs::write(&auth_path, table).unwrap();

        // Identical peer list on every node, self included.
        let peers: Vec<String> = nodes
            .iter()
            .enumerate()
            .map(|(i, n)| format!("{}@{}#{}", i + 1, n.raft_addr, b64(&n.key)))
            .collect();
        Cluster {
            tmp,
            nodes,
            auth_path,
            peers,
            operator,
        }
    }

    /// Node `i`'s configuration: the same journal, keys and raft identity
    /// on every boot, with health and admin endpoints.
    fn config(&self, i: usize, ack_policy: AckPolicy, tick_interval_ms: u64) -> ServerConfig {
        let key_path = self.tmp.path().join(format!("key-{i}"));
        std::fs::write(&key_path, self.nodes[i].key.to_bytes()).unwrap();
        ServerConfig {
            journal: self.tmp.path().join(format!("node-{i}.journal")),
            authorized_keys: self.auth_path.clone(),
            ack_policy,
            // Unpinned, and therefore yielding: three nodes run in this
            // process alongside the rest of the suite, and the default
            // layout would stack every node's same-role thread on one core
            // while a spinner would starve whatever shares its core, the
            // test's own client included.
            cores: PipelineCores::unpinned(),
            tick_interval_ms,
            snapshot_interval_ms: 0,
            health_bind: Some(self.nodes[i].health_addr),
            admin_bind: Some(self.nodes[i].admin_addr),
            replication_key: Some(key_path),
            raft_bind: Some(self.nodes[i].raft_addr),
            raft_node_id: Some(i as u64 + 1),
            raft_peer: self.peers.clone(),
            raft_dir: Some(self.tmp.path().join(format!("node-{i}.raft"))),
            raft_auto_promote: true,
            ..melin_test_node::config()
        }
    }

    fn start(&self, addrs: &Addrs, config: ServerConfig) -> Node {
        melin_test_node::start_at::<Counter>(
            addrs,
            config,
            StartupEvents::none(),
            (),
            RequestDecoder,
            ResponseEncoder,
        )
    }

    /// Node 0, the primary, under `ack_policy`, ticking every
    /// `tick_interval_ms` (0: no ticks).
    fn start_primary(&self, ack_policy: AckPolicy, tick_interval_ms: u64) -> Node {
        let mut config = self.config(0, ack_policy, tick_interval_ms);
        config.replication_bind = Some(self.nodes[0].addrs.replication());
        self.start(&self.nodes[0].addrs, config)
    }

    /// Nodes 1 and 2, replicas of node 0, each requesting `ack_policy`
    /// should it be promoted. With `replication_bind`, each binds its own
    /// replication address, so a promoted one replicates (and its gate
    /// waits for replicas); without, a promoted one runs with replication
    /// disabled. Either way the other replica keeps following node 0.
    fn start_replicas(&self, ack_policy: AckPolicy, replication_bind: bool) -> Vec<Node> {
        (1..3)
            .map(|i| {
                let mut config = self.config(i, ack_policy, 0);
                config.replica_of = Some(self.nodes[0].addrs.replication());
                config.replication_bind =
                    replication_bind.then(|| self.nodes[i].addrs.replication());
                self.start(&self.nodes[i].addrs, config)
            })
            .collect()
    }

    /// Both replicas connected, a raft leader elected, and no replica
    /// promoted (the primary link is up, and if the primary itself leads
    /// there is nothing to act on).
    fn wait_formed(&self, primary: &Node) {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            assert!(!primary.is_finished(), "primary exited early");
            let connected = http_metrics(self.nodes[0].health_addr)
                .is_some_and(|m| m.contains("melin_replicas_connected 2\n"));
            let any_leader = self.nodes.iter().any(|n| {
                http_metrics(n.health_addr).is_some_and(|m| m.contains("melin_raft_is_leader 1\n"))
            });
            if connected && any_leader {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "cluster never formed: replicas_connected_2={connected} any_leader={any_leader}\n{}",
                self.cluster_summary()
            );
            std::thread::sleep(Duration::from_millis(200));
        }
    }

    /// The replicas serving clients, 1 and 2 by index.
    fn serving_replicas(&self, patience: Duration) -> Vec<usize> {
        (1..3)
            .filter(|&i| serves_clients(self.nodes[i].addrs.client(), patience))
            .collect()
    }

    /// Wait until exactly one replica serves clients, and return it.
    fn wait_one_promoted(&self) -> usize {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            assert!(
                Instant::now() < deadline,
                "no replica promoted within the deadline\n{}",
                self.cluster_summary()
            );
            match self.serving_replicas(Duration::from_millis(500))[..] {
                [] => std::thread::sleep(Duration::from_millis(200)),
                [winner] => return winner,
                ref both => panic!("both replicas promoted — split brain: {both:?}"),
            }
        }
    }

    /// `ACK-POLICY <policy>` on the primary's admin endpoint, which must
    /// journal it.
    fn change_policy(&self, policy: AckPolicy) {
        assert_eq!(
            admin_command(
                self.nodes[0].admin_addr,
                &self.operator,
                &format!("ACK-POLICY {policy}")
            )
            .as_deref(),
            Some("OK")
        );
    }

    fn cluster_summary(&self) -> String {
        cluster_summary(&self.nodes)
    }
}

#[test]
#[serial]
fn killed_primary_triggers_exactly_one_auto_promotion() {
    let cluster = Cluster::new();
    let nodes = &cluster.nodes;

    // --- Primary (node 0) under `disk+ram`, and replicas (nodes 1, 2).
    // No ticks, so the primary journals nothing but its lineage's first
    // policy change before it is killed. ---
    let primary = cluster.start_primary(AckPolicy::DiskAndRam, 0);
    let replicas = cluster.start_replicas(AckPolicy::DiskAndRam, false);

    // --- Phase 1: cluster forms. ---
    cluster.wait_formed(&primary);

    // --- Phase 2: kill the primary. ---
    primary.stop();

    // --- Phase 3: exactly one replica promotes and serves clients. ---
    let winner = cluster.wait_one_promoted();

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
            cluster.cluster_summary()
        );
        std::thread::sleep(Duration::from_millis(200));
    }

    // The loser must still be following: not serving clients, not leading.
    let loser = if winner == 1 { 2 } else { 1 };
    assert!(
        !serves_clients(nodes[loser].addrs.client(), Duration::from_secs(2)),
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
    // the process shutdown flag. That flag is the very one the launcher
    // hands the node (the `SupersessionPolicy` holds a clone), so the
    // node tears its own server down and the flag we never write to
    // ourselves ends up set — the fingerprint of fence-on-supersession,
    // the only thing that stops a serving node from the inside (a driver
    // fatal leaves the node serving). Requires `--raft-auto-promote` (already
    // set), which is what arms the `SupersessionPolicy`.
    let revived = {
        // Reuse node 0's identity, raft address, journal, and raft dir; a
        // fresh client/health/admin port avoids the old ones lingering in
        // TIME_WAIT (on DPDK, node 0's slot again: nothing lingers on a
        // userspace stack that is gone). No replication bind — this
        // exercises the raft-mesh fencing channel in isolation, not a
        // data-plane handshake.
        let mut config = cluster.config(0, AckPolicy::DiskAndRam, 0);
        config.health_bind = Some(free_addr(PORT_BASE));
        config.admin_bind = Some(free_addr(PORT_BASE));
        cluster.start(&melin_test_node::addrs(0, || free_addr(PORT_BASE)), config)
    };

    let deadline = Instant::now() + Duration::from_secs(60);
    while !revived.is_finished() {
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
        revived.shutdown_requested(),
        "the revived node stopped without fencing — supersession did not fire"
    );
    revived
        .join()
        .expect("revived primary returned an error instead of fencing cleanly");

    // Teardown.
    for replica in replicas {
        replica.stop();
    }
}

/// Finding 10 of the determinism audit, the weakening: under `disk+ram`
/// with ticks flowing, `ACK-POLICY disk` reaches the replicas as an entry
/// in the stream, no heartbeat needed, so once the primary dies no replica
/// auto-promotes over events it may have acked on its own disk.
#[test]
#[serial]
fn a_weakening_streamed_between_ticks_stops_auto_promotion() {
    let cluster = Cluster::new();
    let health = cluster.nodes[0].health_addr;
    let primary = cluster.start_primary(AckPolicy::DiskAndRam, TICK_MS);
    // The replicas' own policy is `disk+ram`: only the stream can tell
    // them otherwise.
    let replicas = cluster.start_replicas(AckPolicy::DiskAndRam, false);
    cluster.wait_formed(&primary);

    cluster.change_policy(AckPolicy::Disk);
    // In force once a replica holds the change (the old policy's
    // witness); the other is a tick or two behind at most.
    wait_for_gauge(health, IN_FORCE, AckPolicy::Disk.as_u8().into());
    std::thread::sleep(Duration::from_secs(1));

    primary.stop();
    let deadline = Instant::now() + NO_PROMOTION_WATCH;
    while Instant::now() < deadline {
        assert_eq!(
            cluster.serving_replicas(Duration::from_millis(300)),
            Vec::<usize>::new(),
            "a replica auto-promoted after the primary acked under `disk`\n{}",
            cluster.cluster_summary()
        );
        std::thread::sleep(Duration::from_millis(300));
    }

    for replica in replicas {
        replica.stop();
    }
}

/// Finding 10 of the determinism audit, the reverse: a primary that began
/// under `disk` and is moved to `disk+ram` at runtime, with ticks flowing,
/// streams that change too, so once it dies the failover proceeds, though
/// the replicas themselves request `disk`.
///
/// The promoted node, which replicates, is promoted right after the
/// streamed change and starts from it: its gate under the lineage's
/// `disk+ram`, not its own requested `disk`, which it journals as its
/// promotion entry. No replica ever witnesses that entry (the other
/// replica still follows the dead primary), so `disk+ram` stays in force.
#[test]
#[serial]
fn a_strengthening_streamed_between_ticks_lets_the_failover_proceed() {
    let cluster = Cluster::new();
    let health = cluster.nodes[0].health_addr;
    let primary = cluster.start_primary(AckPolicy::Disk, TICK_MS);
    let replicas = cluster.start_replicas(AckPolicy::Disk, true);
    cluster.wait_formed(&primary);

    cluster.change_policy(AckPolicy::DiskAndRam);
    wait_for_gauge(health, IN_FORCE, AckPolicy::DiskAndRam.as_u8().into());
    // A write acked under `disk+ram`: a replica holds it, and so the
    // change before it.
    let mut conn = melin_client::Connection::connect_by(
        primary.addr(),
        &cluster.operator,
        Instant::now() + Duration::from_secs(30),
    )
    .expect("client connects");
    conn.set_read_timeout(Duration::from_secs(30))
        .expect("set the client's read timeout");
    let reply = conn
        .request_one(&increment_request(1))
        .expect("a reply to the write");
    assert_eq!(reply[0], KIND_RESP_ACK);
    drop(conn);
    std::thread::sleep(Duration::from_secs(1));

    primary.stop();
    let winner = cluster.wait_one_promoted();
    let winner_health = cluster.nodes[winner].health_addr;
    wait_for_gauge(winner_health, JOURNALED, AckPolicy::Disk.as_u8().into());
    assert_eq!(
        gauge(winner_health, IN_FORCE),
        Some(AckPolicy::DiskAndRam.as_u8().into()),
        "the promoted gate starts from the lineage's policy, not the node's own"
    );

    for replica in replicas {
        replica.stop();
    }
}
