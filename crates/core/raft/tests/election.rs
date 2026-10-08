//! In-process election tests: one real driver thread per node (each with
//! its own current-thread runtime, storage dir, and TCP listener on
//! localhost) authenticated with real Ed25519 keys — the production shape,
//! one runtime per node, minus only the process boundaries.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use base64::Engine;
use ed25519_dalek::SigningKey;
use melin_app::auth::{AuthorizedKeys, NoRoles};
use melin_raft::driver::{RaftConfig, RaftHandles, RaftPeer, spawn};
use melin_raft::recency::TipSource;
use melin_transport_core::cursors::AdvertisedJournalTip;
use melin_transport_core::fence::FenceState;
use melin_transport_core::health::RaftStatus;
use melin_transport_core::test_ports::free_addr;
use tempfile::TempDir;

struct Cluster {
    nodes: Vec<Node>,
    _dirs: Vec<TempDir>,
}

struct Node {
    id: u64,
    handles: RaftHandles,
    shutdown: Arc<AtomicBool>,
}

/// Overall bound for a cluster to settle on a leader. Elections are tuned
/// to 1–2 s; several rounds of collisions would still fit well inside this.
const ELECTION_DEADLINE: Duration = Duration::from_secs(20);

/// Port range this file owns for `free_addr` (15000..20000); the
/// `melin-server-runtime` raft tests own 20000..30000.
const PORT_BASE: u16 = 15_000;

fn start_cluster(n: u64) -> Cluster {
    // All nodes at the same (zero) journal tip: recency never filters.
    start_cluster_with_tips(&vec![0u64; n as usize])
}

/// Boot a cluster with a fixed advertised journal tip per node (index i =
/// node id i+1), for recency-steering tests. Every tip is `ready`.
fn start_cluster_with_tips(tips: &[u64]) -> Cluster {
    let n = tips.len() as u64;
    let keys: Vec<SigningKey> = (0..n)
        .map(|i| SigningKey::from_bytes(&[i as u8 + 1; 32]))
        .collect();

    // Reserve n distinct localhost ports — see `free_addr`.
    let addrs: Vec<String> = (0..n).map(|_| free_addr(PORT_BASE).to_string()).collect();

    let table: String = keys
        .iter()
        .enumerate()
        .map(|(i, k)| {
            format!(
                "replication {} node-{}\n",
                base64::engine::general_purpose::STANDARD.encode(k.verifying_key().to_bytes()),
                i + 1
            )
        })
        .collect();
    let authorized_keys = Arc::new(AuthorizedKeys::parse::<NoRoles>(&table).unwrap());

    // Identical peer list on every node, self included — the production
    // configuration shape.
    let peers: Vec<RaftPeer> = (0..n as usize)
        .map(|i| RaftPeer {
            id: i as u64 + 1,
            addr: addrs[i].clone(),
            pubkey: keys[i].verifying_key().to_bytes(),
        })
        .collect();

    let mut nodes = Vec::new();
    let mut dirs = Vec::new();
    for i in 0..n as usize {
        let dir = TempDir::new().unwrap();
        let shutdown = Arc::new(AtomicBool::new(false));
        let config = RaftConfig {
            node_id: i as u64 + 1,
            bind: addrs[i].parse().unwrap(),
            dir: dir.path().to_path_buf(),
            peers: peers.clone(),
        };
        let tip = Arc::new(TipSource {
            fence: Arc::new(FenceState::new(0)),
            seq: AdvertisedJournalTip::new(melin_transport_core::WireSeq::new(tips[i])),
            ready: Arc::new(AtomicBool::new(true)),
        });
        let handles = spawn(
            config,
            Arc::new(keys[i].clone()),
            Arc::clone(&authorized_keys),
            tip,
            None, // no supersession policy — election-only test nodes
            Arc::clone(&shutdown),
        )
        .expect("driver spawn");
        nodes.push(Node {
            id: i as u64 + 1,
            handles,
            shutdown,
        });
        dirs.push(dir);
    }
    Cluster { nodes, _dirs: dirs }
}

/// Poll the live nodes' gauges until exactly one reports leadership and the
/// others agree on it. Returns the leader's node id.
fn await_single_leader(nodes: &[&Node]) -> u64 {
    await_single_leader_within(nodes, ELECTION_DEADLINE)
}

/// [`await_single_leader`] with an explicit bound, for elections that are
/// slow by design.
fn await_single_leader_within(nodes: &[&Node], within: Duration) -> u64 {
    let deadline = Instant::now() + within;
    loop {
        let leaders: Vec<u64> = nodes
            .iter()
            .filter(|n| {
                n.handles.status.running.load(Ordering::Relaxed)
                    && n.handles.status.role.load(Ordering::Relaxed) == RaftStatus::ROLE_LEADER
            })
            .map(|n| n.id)
            .collect();
        if leaders.len() == 1 {
            let leader = leaders[0];
            // Every live node must believe in that leader.
            let agreed = nodes
                .iter()
                .all(|n| n.handles.status.leader_id.load(Ordering::Relaxed) == leader);
            if agreed {
                return leader;
            }
        }
        assert!(
            Instant::now() < deadline,
            "no single agreed leader within {within:?}; leaders now: {leaders:?}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

impl Cluster {
    fn stop_node(&mut self, id: u64) {
        let node = self.nodes.iter_mut().find(|n| n.id == id).unwrap();
        node.shutdown.store(true, Ordering::Relaxed);
    }

    fn stop_all(&mut self) {
        for node in &self.nodes {
            node.shutdown.store(true, Ordering::Relaxed);
        }
        for node in self.nodes.drain(..) {
            node.handles.join.join().expect("driver thread panicked");
            assert!(
                !node.handles.status.running.load(Ordering::Relaxed),
                "driver must mark itself stopped"
            );
        }
    }
}

#[test]
fn three_nodes_elect_exactly_one_leader_and_reelect_on_leader_loss() {
    let mut cluster = start_cluster(3);

    let refs: Vec<&Node> = cluster.nodes.iter().collect();
    let first_leader = await_single_leader(&refs);
    let first_term = cluster
        .nodes
        .iter()
        .find(|n| n.id == first_leader)
        .unwrap()
        .handles
        .status
        .term
        .load(Ordering::Relaxed);
    assert!(first_term >= 1, "an elected leader must carry a term >= 1");

    // Kill the leader; the survivors must elect a new one at a higher term.
    cluster.stop_node(first_leader);
    let survivors: Vec<&Node> = cluster
        .nodes
        .iter()
        .filter(|n| n.id != first_leader)
        .collect();
    let second_leader = await_single_leader(&survivors);
    assert_ne!(second_leader, first_leader);
    let second_term = survivors
        .iter()
        .find(|n| n.id == second_leader)
        .unwrap()
        .handles
        .status
        .term
        .load(Ordering::Relaxed);
    assert!(
        second_term > first_term,
        "re-election must advance the term ({second_term} vs {first_term}) — \
         term-mints-epoch depends on this"
    );

    cluster.stop_all();
}

#[test]
fn single_voter_elects_itself() {
    // Degenerate but useful: a 1-voter control plane (tests, dev) elects
    // itself without peers to talk to.
    let mut cluster = start_cluster(1);
    let refs: Vec<&Node> = cluster.nodes.iter().collect();
    assert_eq!(await_single_leader(&refs), 1);
    cluster.stop_all();
}

/// Each node's vote-filter escape count, by node id.
///
/// Call it right after watching a leader emerge (or before any
/// election, when every count is zero). The fence pairs with the
/// driver's Release store of `leader_id`, which the caller's relaxed
/// reads in `await_single_leader` saw: a voter seen following a leader
/// has published any escape that let it vote for that leader (see the
/// driver's poll loop).
fn escape_counts(nodes: &[&Node]) -> Vec<(u64, u64)> {
    std::sync::atomic::fence(Ordering::Acquire);
    nodes
        .iter()
        .map(|n| {
            (
                n.id,
                n.handles.status.vote_filter_escapes.load(Ordering::Relaxed),
            )
        })
        .collect()
}

/// The behind node may only have won `election` through the filter's
/// liveness escape: some caught-up voter must have stopped filtering
/// since `before` was taken. A win with every filter still steering is
/// the bug this test exists to catch.
fn assert_behind_node_lost_or_escaped(
    winner: u64,
    behind: u64,
    voters: &[&Node],
    before: &[(u64, u64)],
    election: &str,
) {
    if winner != behind {
        return;
    }
    let caught_up: Vec<&Node> = voters.iter().copied().filter(|n| n.id != behind).collect();
    let after = escape_counts(&caught_up);
    let escaped = after.iter().any(|&(id, count)| {
        let was = before
            .iter()
            .find(|&&(b, _)| b == id)
            .map_or(0, |&(_, c)| c);
        count > was
    });
    assert!(
        escaped,
        "the behind node won the {election} with every caught-up voter still filtering \
         (escape counts before {before:?}, after {after:?})"
    );
    // Accepted, but worth seeing when this test is slow or a CI log is
    // read after the fact: steering gave way to liveness.
    eprintln!(
        "the behind node won the {election} through the liveness escape \
         (escape counts before {before:?}, after {after:?})"
    );
}

/// Recency steering over real sockets: while a quorum of caught-up nodes
/// exists, a node whose advertised journal tip is behind cannot assemble
/// a quorum, so leadership lands on a most-caught-up node — including
/// across a re-election after the leader dies. This is the property
/// auto-promotion relies on to prefer a most-caught-up replica.
///
/// Five nodes (not three) so a caught-up quorum survives the leader kill
/// without the behind node's cooperation.
///
/// The filter is best-effort steering, with promotion-time checks staying
/// authoritative (see `melin_raft::recency`): after enough dropped vote
/// requests with no leader in sight, a voter's *liveness escape* opens
/// and the behind node may win. On a loaded machine that can happen here:
/// these nodes run without the server runtime's election stand-down, so
/// the behind node keeps campaigning, its filtered campaigns still
/// inflate its term (openraft has no pre-vote), and the churn can hold
/// the caught-up survivors leaderless long enough. So the test asserts
/// what the filter guarantees: the behind node never wins *while every
/// caught-up voter is still filtering*, read from the escape counter
/// the health endpoint serves.
#[test]
fn behind_node_never_wins_an_election() {
    // Nodes 1–4 hold seq 100; node 5 is behind at seq 10. Node 5 can only
    // win with a caught-up grant, and every caught-up node drops its vote
    // requests (candidate tip 10 < local tip 100) until its escape opens.
    const BEHIND: u64 = 5;
    let mut cluster = start_cluster_with_tips(&[100, 100, 100, 100, 10]);
    let refs: Vec<&Node> = cluster.nodes.iter().collect();
    let before_first = escape_counts(&refs);
    let first = await_single_leader(&refs);
    assert_behind_node_lost_or_escaped(first, BEHIND, &refs, &before_first, "first election");

    // Kill the leader: three caught-up nodes remain, a quorum (3 of 5)
    // that can elect among itself without the behind node's grant.
    cluster.stop_node(first);
    let survivors: Vec<&Node> = cluster.nodes.iter().filter(|n| n.id != first).collect();
    let before_second = escape_counts(&survivors);
    let second = await_single_leader(&survivors);
    assert_behind_node_lost_or_escaped(second, BEHIND, &survivors, &before_second, "re-election");

    cluster.stop_all();
}

/// The liveness escape over real sockets, and the counter that reports
/// it: a caught-up voter that never campaigns faces a behind candidate
/// it must refuse, in a two-node cluster where nothing else can win.
/// It drops every vote request until its escape opens, the behind node
/// is elected, and the voter's published count says it gave way — the
/// signal `behind_node_never_wins_an_election` relies on.
#[test]
fn a_voter_that_blocks_every_election_escapes_and_counts_it() {
    // Node 1 is behind; node 2 is caught up and stood down, as the
    // promotion policy stands down a node, so only node 1 campaigns.
    // In time: the driver applies the flag within its 100 ms poll, and
    // no node campaigns before its first 1–2 s election timeout.
    let mut cluster = start_cluster_with_tips(&[10, 100]);
    cluster.nodes[1]
        .handles
        .elect_enabled
        .store(false, Ordering::Release);

    // Slow by design: the escape opens only after LIVENESS_ESCAPE_DROPS
    // dropped campaigns, one per randomized election timeout.
    let refs: Vec<&Node> = cluster.nodes.iter().collect();
    let leader = await_single_leader_within(&refs, Duration::from_secs(60));
    assert_eq!(leader, 1, "only the behind node campaigns");

    let counts = escape_counts(&refs);
    assert_eq!(
        counts,
        [(1, 0), (2, 1)],
        "the caught-up voter escaped exactly once; the candidate dropped nothing"
    );

    cluster.stop_all();
}
