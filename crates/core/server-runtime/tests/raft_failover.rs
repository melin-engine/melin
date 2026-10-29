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
//! - Raft leadership held by a replica (the replicas start first and
//!   elect one before the primary joins): it promotes when the primary
//!   dies, but not when the other replica died before it (a lone survivor
//!   never auto-promotes); isolated from both peers, the other two elect a
//!   new leader, the isolated one never promotes, and once healed the
//!   primary is still unfenced and the only node serving; cut from the
//!   primary alone while writes flow, it does not promote, and once healed
//!   rejoins as a replica.
//!
//! Nodes are started through `melin-test-node`: on kernel TCP by default,
//! on DPDK with this crate's `dpdk` feature, under
//! `scripts/dpdk/netns-runner.sh` (see
//! `docs/internal/dpdk-testing.md`). Raft is kernel TCP on every
//! node. On DPDK the primary's orderly stop resets its replicas' links,
//! which they act on at once, and the winner serves as a DPDK primary.
//!
//! The partition tests ([`Cluster::partitionable`]) give each node a
//! loopback IP of its own and assume Linux's routing of all of
//! 127.0.0.0/8 to `lo`; the crate builds on Linux only (io_uring), so
//! there is no fallback.

#[path = "common/port_pool.rs"]
mod port_pool;
#[path = "common/proxy.rs"]
mod proxy;

use std::io::Read;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use base64::Engine;
use counter_server::{Counter, KIND_RESP_ACK, RequestDecoder, ResponseEncoder, increment_request};
use melin_server_runtime::StartupEvents;
use melin_server_runtime::ack_policy::AckPolicy;
use melin_server_runtime::layout::PipelineCores;
use melin_server_runtime::server::ServerConfig;
use melin_test_node::probe::{admin_command, gauge, http_metrics, wait_for_gauge};
use melin_test_node::{Addrs, Node};
use melin_wire_protocol::control_codec::TAG_CHALLENGE;
use proxy::Proxy;
use serial_test::serial;

/// Port range this file owns for `free_addr` (20000..25000);
/// `raft_smoke.rs` owns 25000..30000, `melin-raft`'s election tests
/// 15000..20000. A stolen replication port once left this cluster
/// unable to ever form — see `test_ports::free_addr` for the scheme.
const PORT_BASE: u16 = 20_000;

/// A loopback port no running test holds, from this file's port range.
/// Each test hands its ports back as its [`Cluster`] drops, since
/// together the tests take more than `free_addr` gives one process
/// (`port_pool`).
fn port() -> SocketAddr {
    port_pool::port(PORT_BASE)
}

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

/// How long a healed cluster is watched for the fallout of a partition:
/// several election timeouts, and a replica's reconnect and catch-up.
const HEAL_WATCH: Duration = Duration::from_secs(15);

/// The reasons a replica archives its lineage, as they appear in the
/// archive's directory name (`<journal file name>.<reason>.<n>`). A copy
/// of what `ArchiveReason::tag` in `melin-transport-core` owns, which is
/// private: the names are documented for operators
/// (`docs/replication.md`), so a rename is already a visible change, but
/// a reason added there must be added here too.
const ARCHIVE_REASONS: [&str; 2] = ["divergent", "resync"];

/// The archives beside `journal`, by file name: its siblings named
/// `<journal file name>.<reason>.<n>`. Not a bare prefix match: rotated
/// segments (`<journal file name>.NNNNNN`) and snapshot files share the
/// journal's stem.
fn archives_beside(journal: &Path) -> Vec<String> {
    let name = journal
        .file_name()
        .and_then(|n| n.to_str())
        .expect("a journal file name in UTF-8");
    let dir = journal.parent().expect("a journal in a directory");
    std::fs::read_dir(dir)
        .expect("list the journal's directory")
        .map(|entry| entry.expect("a directory entry").file_name())
        .filter_map(|sibling| sibling.into_string().ok())
        .filter(|sibling| {
            sibling
                .strip_prefix(name)
                .and_then(|rest| rest.strip_prefix('.'))
                .and_then(|rest| rest.split_once('.'))
                .is_some_and(|(reason, n)| {
                    ARCHIVE_REASONS.contains(&reason) && n.parse::<u32>().is_ok()
                })
        })
        .collect()
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

/// One increment on `conn`, which the node must ack.
fn acked_write(conn: &mut melin_client::Connection) {
    if let Err(e) = try_acked_write(conn) {
        panic!("{e}");
    }
}

/// One increment on `conn`: `Err` saying why if the node did not ack it,
/// for a test that records the failure rather than stopping at it.
fn try_acked_write(conn: &mut melin_client::Connection) -> Result<(), String> {
    let reply = conn
        .request_one(&increment_request(1))
        .map_err(|e| format!("no reply to the write: {e}"))?;
    match reply.first() {
        Some(&KIND_RESP_ACK) => Ok(()),
        kind => Err(format!("the write was not acked: reply kind {kind:?}")),
    }
}

/// Node `i`'s loopback IP in a partitionable cluster: its raft listener
/// binds it, so its outgoing raft connections come from it too, and a
/// proxy can tell them apart from another node's. `127.0.0.(i+2)`: no
/// node takes 127.0.0.1, the source an unbound loopback socket gets, so
/// should nodes stop dialling from their listener's IP, no block matches
/// rather than one matching by accident.
fn raft_ip(i: usize) -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(127, 0, 0, i as u8 + 2))
}

/// The sources a node's raft proxy stalls, the predicate it is started
/// with ([`Proxy::start_on`]). A `Vec` under a `Mutex`: a handful of
/// entries, read on every relay pass, never on a hot path.
#[derive(Clone, Default)]
struct BlockList(Arc<Mutex<Vec<IpAddr>>>);

impl BlockList {
    fn block(&self, ip: IpAddr) {
        let mut blocked = self.0.lock().unwrap();
        if !blocked.contains(&ip) {
            blocked.push(ip);
        }
    }

    fn clear(&self) {
        self.0.lock().unwrap().clear();
    }

    fn blocks(&self, ip: IpAddr) -> bool {
        self.0.lock().unwrap().contains(&ip)
    }
}

/// The links of a partitionable cluster, each through a proxy the test
/// controls.
struct Links {
    /// In front of each node's raft listener, by node index, with the
    /// sources it stalls. The peer list names these, so every raft
    /// connection to a node goes through its proxy.
    raft: Vec<(Proxy, BlockList)>,
    /// Replica `i`'s link to the primary, at `i - 1`: its `replica_of`.
    replication: Vec<Proxy>,
}

impl Links {
    fn replication(&self, replica: usize) -> &Proxy {
        assert!((1..3).contains(&replica), "node {replica} is not a replica");
        &self.replication[replica - 1]
    }
}

/// A cluster whose control plane a replica leads
/// ([`Cluster::start_replicas_first`]).
struct ReplicaLed {
    primary: Node,
    /// Nodes 1 and 2, in that order.
    replicas: Vec<Node>,
    /// The leading replica's index.
    leader: usize,
    /// The term it was elected at, and still leads in.
    term: u64,
}

impl ReplicaLed {
    /// The replica that does not lead.
    fn other(&self) -> usize {
        3 - self.leader
    }

    fn stop(self) {
        for replica in self.replicas {
            replica.stop();
        }
        self.primary.stop();
    }
}

struct NodeSetup {
    key: ed25519_dalek::SigningKey,
    /// Client and replication addresses.
    addrs: Addrs,
    /// Where the node's raft listener binds.
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
    /// The proxies a partitionable cluster runs its links through.
    links: Option<Links>,
}

impl Cluster {
    /// Nodes linked directly to each other.
    fn new() -> Self {
        Self::build(false)
    }

    /// Nodes whose raft and replication links a test can break pair by
    /// pair ([`Cluster::partition`], [`Cluster::isolate`],
    /// [`Cluster::isolate_from_primary`], [`Cluster::heal`]). Node `i`'s
    /// raft listener binds [`raft_ip`]`(i)`, behind a proxy on 127.0.0.1
    /// (kernel TCP on every transport, so raft never touches the DPDK
    /// runner's client-facing address), and each replica follows the
    /// primary through a replication proxy of its own.
    fn partitionable() -> Self {
        Self::build(true)
    }

    fn build(partitionable: bool) -> Self {
        let tmp = tempfile::tempdir().expect("tempdir");
        let nodes: Vec<NodeSetup> = (0..3)
            .map(|i| NodeSetup {
                key: ed25519_dalek::SigningKey::from_bytes(&[0x51 + i as u8; 32]),
                addrs: melin_test_node::addrs(i, port),
                raft_addr: if partitionable {
                    // Free on the node's own IP too: a fresh port was
                    // never issued here before, and a handed-back one was
                    // probed on the IP its last listener bound (`Drop`).
                    SocketAddr::new(raft_ip(i), port().port())
                } else {
                    port()
                },
                health_addr: port(),
                admin_addr: port(),
            })
            .collect();
        let links = partitionable.then(|| Links {
            raft: nodes
                .iter()
                .map(|n| {
                    let blocked = BlockList::default();
                    let stalls = blocked.clone();
                    let proxy = Proxy::start_on(
                        IpAddr::V4(Ipv4Addr::LOCALHOST),
                        n.raft_addr,
                        move |source| stalls.blocks(source),
                    );
                    (proxy, blocked)
                })
                .collect(),
            replication: (1..3)
                .map(|_| Proxy::start(nodes[0].addrs.replication()))
                .collect(),
        });
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

        // Identical peer list on every node, self included: where the
        // others dial each node, its proxy if it has one.
        let peers: Vec<String> = nodes
            .iter()
            .enumerate()
            .map(|(i, n)| {
                let dial = links
                    .as_ref()
                    .map_or(n.raft_addr, |links| links.raft[i].0.addr());
                format!("{}@{dial}#{}", i + 1, b64(&n.key))
            })
            .collect();
        Cluster {
            tmp,
            nodes,
            auth_path,
            peers,
            operator,
            links,
        }
    }

    fn links(&self) -> &Links {
        self.links
            .as_ref()
            .expect("only a partitionable cluster has links to break")
    }

    /// Where replica `i` follows the primary: through its replication
    /// proxy if it has one.
    fn replica_of(&self, i: usize) -> SocketAddr {
        self.links.as_ref().map_or_else(
            || self.nodes[0].addrs.replication(),
            |links| links.replication(i).addr(),
        )
    }

    /// Sever the raft links between nodes `a` and `b`, both directions,
    /// and nothing else: each one's connections stall at the other's
    /// proxy, as on a link gone silent.
    fn partition(&self, a: usize, b: usize) {
        let raft = &self.links().raft;
        raft[a].1.block(raft_ip(b));
        raft[b].1.block(raft_ip(a));
    }

    /// Cut replica `i` off from both other nodes: nothing reaches it over
    /// raft (its proxy frozen), it reaches nobody (its source stalled at
    /// both other proxies), and its replication link is cut. The primary
    /// and the other replica still reach each other, so the primary keeps
    /// serving and the two can elect a new raft leader without `i`.
    fn isolate(&self, i: usize) {
        let links = self.links();
        links.raft[i].0.freeze();
        for (j, (_, blocked)) in links.raft.iter().enumerate() {
            if j != i {
                blocked.block(raft_ip(i));
            }
        }
        links.replication(i).cut();
    }

    /// Cut replica `i` off from the primary alone: their raft link and
    /// `i`'s replication link. `i` and the other replica still reach each
    /// other, and the other replica the primary.
    fn isolate_from_primary(&self, i: usize) {
        self.partition(i, 0);
        self.links().replication(i).cut();
    }

    /// Undo every partition. The raft proxies are cut first, so that a
    /// relay stalled for a blocked source closes on the generation change
    /// instead of resuming when the block list empties and flushing what
    /// it buffered during the partition (stale frames the dialler gave up
    /// on at its RPC timeout); a connection held since then closes too.
    /// The healthy pairs' relays close with them, and reconnect on the
    /// next heartbeat. Then the block lists clear and the raft proxies
    /// relay again. Last, the replication proxies relay again: a replica
    /// whose link a partition cut gets back in on its next reconnect, and
    /// for one whose link no partition touched it changes nothing (it is
    /// not cut: that would drop the primary's in-memory replica).
    fn heal(&self) {
        let links = self.links();
        for (proxy, _) in &links.raft {
            proxy.cut();
        }
        for (_, blocked) in &links.raft {
            blocked.clear();
        }
        for (proxy, _) in &links.raft {
            proxy.restore();
        }
        for proxy in &links.replication {
            proxy.restore();
        }
    }

    /// Node `i`'s journal, the same on every boot.
    fn journal(&self, i: usize) -> PathBuf {
        self.tmp.path().join(format!("node-{i}.journal"))
    }

    /// Node `i`'s configuration: the same journal, keys and raft identity
    /// on every boot, with health and admin endpoints.
    fn config(&self, i: usize, ack_policy: AckPolicy, tick_interval_ms: u64) -> ServerConfig {
        let key_path = self.tmp.path().join(format!("key-{i}"));
        std::fs::write(&key_path, self.nodes[i].key.to_bytes()).unwrap();
        ServerConfig {
            journal: self.journal(i),
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
                config.replica_of = Some(self.replica_of(i));
                config.replication_bind =
                    replication_bind.then(|| self.nodes[i].addrs.replication());
                self.start(&self.nodes[i].addrs, config)
            })
            .collect()
    }

    /// The replicas (under `ack_policy`, not replicating if promoted)
    /// first, until one of them leads the control plane, then the primary
    /// under `ack_policy` with no ticks. Neither replica can promote
    /// meanwhile: one that never observed a primary refuses. This puts
    /// raft leadership on a replica deterministically, which a normal
    /// bring-up does not (the primary, raft id 1, ticks fastest and
    /// usually wins the first election).
    ///
    /// The primary's own first campaign must not disturb that leader: its
    /// log is shorter, and the other replica's vote is held by the
    /// leader's lease. Checked here on every bring-up: the cluster forms
    /// with every node following the same leader at the same term, and no
    /// node ever reports a higher term.
    fn start_replicas_first(&self, ack_policy: AckPolicy) -> ReplicaLed {
        let replicas = self.start_replicas(ack_policy, false);
        // Every term is above 0.
        let (leader, term) = self.wait_leader_above(0, &[1, 2]);

        let primary = self.start_primary(ack_policy, 0);
        self.wait_formed(&primary);
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let views: Vec<(Option<u64>, Option<u64>)> = self
                .nodes
                .iter()
                .map(|n| {
                    (
                        gauge(n.health_addr, "melin_raft_term"),
                        gauge(n.health_addr, "melin_raft_leader_id"),
                    )
                })
                .collect();
            assert!(
                views.iter().all(|&(t, _)| t.is_none_or(|t| t <= term)),
                "the late primary forced a re-election: node {} led at term {term}, \
                 now (term, leader id) per node {views:?}\n{}",
                leader + 1,
                self.cluster_summary()
            );
            if views
                .iter()
                .all(|&view| view == (Some(term), Some(leader as u64 + 1)))
            {
                return ReplicaLed {
                    primary,
                    replicas,
                    leader,
                    term,
                };
            }
            assert!(
                Instant::now() < deadline,
                "the primary never followed node {} at term {term}: (term, leader id) per \
                 node {views:?}\n{}",
                leader + 1,
                self.cluster_summary()
            );
            std::thread::sleep(Duration::from_millis(200));
        }
    }

    /// The term node `i` leads the control plane in, if it does.
    fn leads_at(&self, i: usize) -> Option<u64> {
        let health = self.nodes[i].health_addr;
        if gauge(health, "melin_raft_is_leader") != Some(1) {
            return None;
        }
        gauge(health, "melin_raft_term")
    }

    /// One of the nodes `among` (by index) that leads the control plane at
    /// a term above `term`, with its term.
    fn leader_above(&self, term: u64, among: &[usize]) -> Option<(usize, u64)> {
        among
            .iter()
            .find_map(|&i| self.leads_at(i).filter(|&t| t > term).map(|t| (i, t)))
    }

    /// Wait until one of the nodes `among` (by index) leads the control
    /// plane at a term above `term`, and return it with its term.
    fn wait_leader_above(&self, term: u64, among: &[usize]) -> (usize, u64) {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if let Some(leading) = self.leader_above(term, among) {
                return leading;
            }
            assert!(
                Instant::now() < deadline,
                "none of nodes {among:?} (by index) came to lead above term {term}\n{}",
                self.cluster_summary()
            );
            std::thread::sleep(Duration::from_millis(200));
        }
    }

    /// A client of the operator's on `primary`, patient enough for any
    /// write these tests make.
    fn client(&self, primary: &Node) -> melin_client::Connection {
        let mut conn = melin_client::Connection::connect_by(
            primary.addr(),
            &self.operator,
            Instant::now() + Duration::from_secs(30),
        )
        .expect("client connects");
        conn.set_read_timeout(Duration::from_secs(30))
            .expect("set the client's read timeout");
        conn
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

impl Drop for Cluster {
    /// Hand the nodes' ports back, once the test has stopped them. A raft
    /// port goes back on the IP its node bound, which a partitionable
    /// cluster's node took as its own: the pool probes it there, so a
    /// raft listener outliving its node keeps the port out of reuse.
    fn drop(&mut self) {
        for n in &self.nodes {
            port_pool::give_back(&n.addrs, [n.raft_addr, n.health_addr, n.admin_addr]);
        }
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
    // Reuse node 0's identity, raft address, journal, and raft dir; a
    // fresh client/health/admin port avoids the old ones lingering in
    // TIME_WAIT (on DPDK, node 0's slot again: nothing lingers on a
    // userspace stack that is gone). No replication bind — this
    // exercises the raft-mesh fencing channel in isolation, not a
    // data-plane handshake.
    let (revived_addrs, revived_endpoints) = (melin_test_node::addrs(0, port), [port(), port()]);
    let revived = {
        let mut config = cluster.config(0, AckPolicy::DiskAndRam, 0);
        config.health_bind = Some(revived_endpoints[0]);
        config.admin_bind = Some(revived_endpoints[1]);
        cluster.start(&revived_addrs, config)
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
    port_pool::give_back(&revived_addrs, revived_endpoints);

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
    acked_write(&mut cluster.client(&primary));
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

/// A replica that leads the control plane when the primary dies promotes
/// itself: leadership held *through* the failure, where
/// `killed_primary_triggers_exactly_one_auto_promotion` usually has the
/// primary leading and an election after it.
#[test]
#[serial]
fn replica_leader_promotes_when_the_primary_dies() {
    let cluster = Cluster::new();
    let ReplicaLed {
        primary,
        replicas,
        leader,
        ..
    } = cluster.start_replicas_first(AckPolicy::DiskAndRam);

    primary.stop();
    assert_eq!(
        cluster.wait_one_promoted(),
        leader,
        "the replica that led is the one to promote\n{}",
        cluster.cluster_summary()
    );

    for replica in replicas {
        replica.stop();
    }
}

/// The lone survivor (`docs/internal/raft-promotion-quorum-lease-plan.md`,
/// §8): a replica that leads the control plane while the primary and the
/// other replica both die never auto-promotes, since from where it stands
/// that cannot be told from being partitioned; recovery is a manual
/// `PROMOTE`. It keeps leading at its term throughout (openraft has no
/// check-quorum step-down), so it is the leader-lease filter, not a lost
/// leadership, that holds it back. Both peers are voters, so the lease
/// lapses `QUORUM_ACK_MAX_AGE` after the second of them stops. The other
/// replica is stopped first, and `stop` returns only once it is down, so
/// the primary's acks end when the primary-down grace starts and the
/// lease lapses well inside it. The reverse order would not be safe: a
/// slow clean stop of the primary could let the other replica's acks
/// keep the lease fresh past the grace.
#[test]
#[serial]
fn a_replica_leader_alone_never_auto_promotes() {
    let cluster = Cluster::new();
    let ReplicaLed {
        primary,
        mut replicas,
        leader,
        term,
    } = cluster.start_replicas_first(AckPolicy::DiskAndRam);

    // `replicas` holds nodes 1 and 2 at 0 and 1.
    let survivor = replicas.remove(leader - 1);
    let other = replicas.pop().expect("the other replica");
    other.stop();
    primary.stop();

    let deadline = Instant::now() + NO_PROMOTION_WATCH;
    while Instant::now() < deadline {
        assert!(
            !serves_clients(
                cluster.nodes[leader].addrs.client(),
                Duration::from_millis(300)
            ),
            "node {} promoted itself with both other nodes gone\n{}",
            leader + 1,
            cluster.cluster_summary()
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    assert_eq!(
        cluster.leads_at(leader),
        Some(term),
        "node {} should still lead at term {term}, alone: otherwise the refusal proves \
         nothing about the lease\n{}",
        leader + 1,
        cluster.cluster_summary()
    );

    survivor.stop();
}

/// A replica leader isolated from both peers is replaced: the primary and
/// the other replica, which still reach each other, elect a new leader at
/// a higher term, and the primary keeps serving. Proves that
/// [`Cluster::isolate`] takes effect: with the leader's links up, it
/// would keep its majority and nobody would campaign.
#[test]
#[serial]
fn an_isolated_replica_leader_is_replaced_by_the_primary_and_the_other_replica() {
    let cluster = Cluster::partitionable();
    let led = cluster.start_replicas_first(AckPolicy::DiskAndRam);

    cluster.isolate(led.leader);
    cluster.wait_leader_above(led.term, &[0, led.other()]);
    acked_write(&mut cluster.client(&led.primary));

    led.stop();
}

/// The one-sided partition (`docs/internal/raft-promotion-quorum-lease-plan.md`,
/// §8): the replica leader loses only its links to the primary, while the
/// other replica still reaches both. With writes flowing, the other
/// replica's tip moves ahead of the leader's, so the peer-tip veto keeps
/// the leader from promoting, and the other replica, whose own primary
/// link is up, does not campaign: nobody promotes and the primary keeps
/// serving. Once healed, the leader rejoins as a replica. That the
/// partition took effect is checked too: the primary, hearing from no
/// leader, campaigns above the leader's term, in vain.
///
/// A characterisation of what the code does, not a guarantee: with an
/// idle primary nothing vetoes, and the leader promotes (a known gap).
#[test]
#[serial]
fn a_replica_leader_cut_from_the_primary_alone_does_not_promote_while_writes_flow() {
    let cluster = Cluster::partitionable();
    let led = cluster.start_replicas_first(AckPolicy::DiskAndRam);
    let primary_health = cluster.nodes[0].health_addr;
    let mut conn = cluster.client(&led.primary);

    cluster.isolate_from_primary(led.leader);
    wait_for_gauge(primary_health, "melin_replicas_connected", 1);
    let deadline = Instant::now() + NO_PROMOTION_WATCH;
    while Instant::now() < deadline {
        acked_write(&mut conn);
        assert_eq!(
            cluster.serving_replicas(Duration::from_millis(300)),
            Vec::<usize>::new(),
            "a replica promoted while the leader was cut from the primary alone, with writes \
             flowing\n{}",
            cluster.cluster_summary()
        );
    }

    // The partition took effect, and the leader kept its majority through
    // the other replica: the primary, hearing from no leader, campaigns
    // in vain at ever higher terms (the other replica's vote is held by
    // the leader's lease), while the leader still leads at its term.
    let primary_term = gauge(primary_health, "melin_raft_term");
    assert!(
        primary_term.is_some_and(|t| t > led.term)
            && cluster.leads_at(led.leader) == Some(led.term),
        "expected node {} still leading at term {} and the primary campaigning above it \
         (term {primary_term:?})\n{}",
        led.leader + 1,
        led.term,
        cluster.cluster_summary()
    );

    cluster.heal();
    wait_for_gauge(primary_health, "melin_replicas_connected", 2);
    acked_write(&mut conn);
    assert_eq!(
        cluster.serving_replicas(Duration::from_millis(300)),
        Vec::<usize>::new(),
        "a replica serves after the heal\n{}",
        cluster.cluster_summary()
    );

    drop(conn);
    led.stop();
}

/// What [`an_isolated_replica_leader_never_deposes_a_healthy_primary`]
/// saw, recorded through both phases and asserted at the end, so that one
/// failing run shows every consequence rather than the first. Nodes are
/// numbered from 1, as in [`Cluster::cluster_summary`], so the record
/// reads beside it without translation.
#[derive(Debug)]
struct IsolationRecord {
    /// The isolated replica leader's node number, and the term it led at.
    leader_node: usize,
    term: u64,
    /// While isolated: how long after the isolation a write to the primary
    /// first went unacked, and why, if one did (no write is attempted
    /// after it).
    write_failed: Option<(Duration, String)>,
    /// While isolated: the first leader the primary or the other replica
    /// elected above `term`, by node number, with its term.
    re_elected: Option<(usize, u64)>,
    /// While isolated: how long after the isolation the isolated leader
    /// was first seen serving clients, if it was.
    leader_served_after: Option<Duration>,
    /// Over the heal watch: how long after the heal the primary was first
    /// seen stopped by itself (finished, or its shutdown flag set, which
    /// only a fence does before the test stops it), if it was.
    primary_fenced_after: Option<Duration>,
    /// At the end of the heal watch: whether the primary serves clients,
    /// and which replicas do, by node number.
    primary_serves: bool,
    replica_nodes_serving: Vec<usize>,
    /// The primary's `melin_replicas_connected` and
    /// `melin_replica_divergence_total` as last read over the heal watch
    /// (`None`: its health endpoint never answered).
    replicas_connected: Option<u64>,
    divergence_total: Option<u64>,
    /// Archives beside the isolated leader's journal at the end.
    archives: Vec<String>,
}

/// The isolated-leader bug (`docs/internal/raft-promotion-quorum-lease-plan.md`,
/// §1 and step 1c): a replica that leads the control plane and is cut off
/// from both peers must not promote itself, and once healed the primary,
/// which the other replica kept reaching, must still serve, and be the only
/// node that does, with both replicas back and nothing diverged or
/// archived. That the partition took effect is part of it: the primary
/// and the other replica elect a new leader above the isolated one's term.
#[test]
#[serial]
fn an_isolated_replica_leader_never_deposes_a_healthy_primary() {
    let cluster = Cluster::partitionable();
    let led = cluster.start_replicas_first(AckPolicy::DiskAndRam);
    let (leader, other, term) = (led.leader, led.other(), led.term);
    let primary_health = cluster.nodes[0].health_addr;
    let mut conn = cluster.client(&led.primary);

    // Isolated, with writes flowing, so the primary's journal moves past
    // the isolated leader's.
    cluster.isolate(leader);
    let isolated = Instant::now();
    let mut write_failed = None;
    let mut re_elected = None;
    let mut leader_served_after = None;
    while isolated.elapsed() < NO_PROMOTION_WATCH {
        if write_failed.is_none()
            && let Err(why) = try_acked_write(&mut conn)
        {
            write_failed = Some((isolated.elapsed(), why));
        }
        re_elected = re_elected.or_else(|| cluster.leader_above(term, &[0, other]));
        if leader_served_after.is_none()
            && cluster
                .serving_replicas(Duration::from_millis(300))
                .contains(&leader)
        {
            leader_served_after = Some(isolated.elapsed());
        }
    }
    drop(conn);

    cluster.heal();
    let healed = Instant::now();
    let mut primary_fenced_after = None;
    let mut replicas_connected = None;
    let mut divergence_total = None;
    while healed.elapsed() < HEAL_WATCH {
        if primary_fenced_after.is_none()
            && (led.primary.is_finished() || led.primary.shutdown_requested())
        {
            primary_fenced_after = Some(healed.elapsed());
        }
        replicas_connected =
            gauge(primary_health, "melin_replicas_connected").or(replicas_connected);
        divergence_total =
            gauge(primary_health, "melin_replica_divergence_total").or(divergence_total);
        std::thread::sleep(Duration::from_millis(300));
    }
    let node = |i: usize| i + 1;
    let record = IsolationRecord {
        leader_node: node(leader),
        term,
        write_failed,
        re_elected: re_elected.map(|(i, t)| (node(i), t)),
        leader_served_after,
        primary_fenced_after,
        primary_serves: serves_clients(cluster.nodes[0].addrs.client(), Duration::from_secs(2)),
        replica_nodes_serving: cluster
            .serving_replicas(Duration::from_secs(2))
            .into_iter()
            .map(node)
            .collect(),
        replicas_connected,
        divergence_total,
        archives: archives_beside(&cluster.journal(leader)),
    };

    assert!(
        record.write_failed.is_none()
            && record.re_elected.is_some()
            && record.leader_served_after.is_none()
            && record.primary_fenced_after.is_none()
            && record.primary_serves
            && record.replica_nodes_serving.is_empty()
            && record.replicas_connected == Some(2)
            && record.divergence_total == Some(0)
            && record.archives.is_empty(),
        "expected the primary to ack every write while node {node} was isolated, the primary \
         and the other replica to elect a leader above node {node}'s term {term}, node {node} never \
         to serve, and the healed primary unfenced and the only node serving, with both \
         replicas back, no divergence and no archive: {record:#?}\n{}",
        cluster.cluster_summary(),
        node = record.leader_node,
        term = record.term,
    );
    led.stop();
}

/// The archive scan matches both archive forms at any index, and nothing
/// else that shares the journal's stem: rotated segments, snapshot files,
/// another node's archives, or a name that only looks like one.
#[test]
fn archives_beside_matches_only_archive_forms() {
    let dir = tempfile::tempdir().unwrap();
    let journal = dir.path().join("node-1.journal");
    for name in [
        "node-1.journal",
        "node-1.journal.000001",
        "node-1.journal.snap.tmp",
        "node-1.journal.snapshot.prev",
        "node-1.journal.divergent.0",
        "node-1.journal.resync.12",
        "node-1.journal.divergent.x",
        "node-1.journal.resync.",
        "node-1.journal.other.0",
        "node-1.journaldivergent.0",
        "node-2.journal.divergent.0",
    ] {
        std::fs::create_dir(dir.path().join(name)).unwrap();
    }
    let mut archives = archives_beside(&journal);
    archives.sort();
    assert_eq!(
        archives,
        ["node-1.journal.divergent.0", "node-1.journal.resync.12"]
    );
}
