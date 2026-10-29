//! In-process 3-node election tests: three real driver threads (each with
//! its own current-thread runtime, storage dir, and TCP listener on
//! localhost) authenticated with real Ed25519 keys — the production shape,
//! one runtime per node, minus only the process boundaries.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use crate::driver::{
    LEADER_CONFIRM_TIMEOUT, LeaderConfirmError, RaftConfig, RaftHandles, RaftPeer, spawn,
};
use crate::tests::tip_at;
use base64::Engine;
use ed25519_dalek::SigningKey;
use melin_app::auth::{AuthorizedKeys, NoRoles};
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
        let handles = spawn(
            config,
            Arc::new(keys[i].clone()),
            Arc::clone(&authorized_keys),
            tip_at(0, tips[i]),
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

/// `spawn` for node 1, listening on `bind`, with node 2 at `peer_addr`
/// and node 1's own entry at `self_addr`; started already shut down, so
/// a configuration it accepts stops at once. `dir` holds its raft state.
fn spawn_node_1(
    dir: &TempDir,
    bind: &str,
    self_addr: &str,
    peer_addr: &str,
) -> std::io::Result<RaftHandles> {
    let key = SigningKey::from_bytes(&[1; 32]);
    let peers = vec![
        RaftPeer {
            id: 1,
            addr: self_addr.to_owned(),
            pubkey: key.verifying_key().to_bytes(),
        },
        RaftPeer {
            id: 2,
            addr: peer_addr.to_owned(),
            pubkey: SigningKey::from_bytes(&[2; 32]).verifying_key().to_bytes(),
        },
    ];
    let config = RaftConfig {
        node_id: 1,
        bind: bind.parse().unwrap(),
        dir: dir.path().to_path_buf(),
        peers,
    };
    spawn(
        config,
        Arc::new(key),
        Arc::new(AuthorizedKeys::parse::<NoRoles>("").unwrap()),
        tip_at(0, 0),
        None,
        Arc::new(AtomicBool::new(true)),
    )
}

#[test]
fn spawn_refuses_a_peer_a_specific_bind_cannot_dial() {
    for (bind, peer) in [
        ("127.0.0.2:0", "[::1]:7002"),
        ("[::1]:0", "127.0.0.3:7002"),
        ("127.0.0.2:0", "localhost:7002"),
    ] {
        let dir = TempDir::new().unwrap();
        let Err(e) = spawn_node_1(&dir, bind, "127.0.0.2:7001", peer) else {
            panic!("{bind} with peer {peer} must be refused");
        };
        assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput);
        let message = e.to_string();
        let bind_ip = bind
            .parse::<std::net::SocketAddr>()
            .unwrap()
            .ip()
            .to_string();
        assert!(
            message.contains("id 2") && message.contains(&bind_ip) && message.contains(peer),
            "{message}"
        );
    }
}

/// Only the peers this node dials are checked: its own entry is what the
/// others dial, and a wildcard bind lets the kernel route and resolve.
#[test]
fn spawn_accepts_what_the_bind_can_dial() {
    for (bind, self_addr, peer) in [
        ("127.0.0.2:0", "node-1.internal:7001", "127.0.0.3:7002"),
        ("0.0.0.0:0", "127.0.0.2:7001", "[::1]:7002"),
        ("0.0.0.0:0", "127.0.0.2:7001", "localhost:7002"),
    ] {
        let dir = TempDir::new().unwrap();
        let handles = spawn_node_1(&dir, bind, self_addr, peer)
            .unwrap_or_else(|e| panic!("{bind} with peer {peer} must start: {e}"));
        handles.join.join().unwrap();
    }
}

/// Poll the live nodes' gauges until exactly one reports leadership and the
/// others agree on it. Returns the leader's node id.
fn await_single_leader(nodes: &[&Node]) -> u64 {
    let deadline = Instant::now() + ELECTION_DEADLINE;
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
            "no single agreed leader within {ELECTION_DEADLINE:?}; leaders now: {leaders:?}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

impl Cluster {
    fn handles(&self, id: u64) -> &RaftHandles {
        &self.nodes.iter().find(|n| n.id == id).unwrap().handles
    }

    fn status(&self, id: u64) -> &RaftStatus {
        &self.handles(id).status
    }

    /// The node ids other than `id`.
    fn others(&self, id: u64) -> Vec<u64> {
        self.nodes
            .iter()
            .map(|n| n.id)
            .filter(|&other| other != id)
            .collect()
    }

    fn stop_node(&mut self, id: u64) {
        let node = self.nodes.iter_mut().find(|n| n.id == id).unwrap();
        node.shutdown.store(true, Ordering::Relaxed);
    }

    /// Stop node `id` and wait for its driver to exit, so its listener is
    /// closed and connecting to it is refused.
    fn stop_node_and_join(&mut self, id: u64) {
        self.stop_node(id);
        let at = self.nodes.iter().position(|n| n.id == id).unwrap();
        let node = self.nodes.remove(at);
        node.handles.join.join().expect("driver thread panicked");
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

/// Wait until `status` reports a quorum-ack age: a new leader's first
/// majority acknowledgement lands one round trip after its election.
fn await_quorum_ack(status: &RaftStatus) -> u64 {
    let deadline = Instant::now() + ELECTION_DEADLINE;
    loop {
        if let Some(age) = status.millis_since_quorum_ack() {
            return age;
        }
        assert!(
            Instant::now() < deadline,
            "the leader reported no quorum ack within {ELECTION_DEADLINE:?}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Pins the driver's liveness assertion empirically: a leader whose
/// peers all answer never reports an age the promotion filter refuses.
#[test]
fn a_healthy_leaders_quorum_ack_age_stays_under_the_gate() {
    const SAMPLE_FOR: Duration = Duration::from_secs(3);
    // The age peaks (about one tick) only between a heartbeat's send and
    // its reply, a round trip, and reads near 0 the rest of the time: a
    // coarse sampler never sees the peak this test is about.
    const SAMPLE_EVERY: Duration = Duration::from_millis(1);

    let mut cluster = start_cluster(3);
    let refs: Vec<&Node> = cluster.nodes.iter().collect();
    let leader = await_single_leader(&refs);
    let status = cluster.status(leader);
    let mut max_age = await_quorum_ack(status);

    let end = Instant::now() + SAMPLE_FOR;
    while Instant::now() < end {
        assert_eq!(
            status.role.load(Ordering::Relaxed),
            RaftStatus::ROLE_LEADER,
            "node {leader} lost leadership in a healthy cluster"
        );
        let age = status
            .millis_since_quorum_ack()
            .expect("an acknowledged leader keeps reporting an age");
        max_age = max_age.max(age);
        std::thread::sleep(SAMPLE_EVERY);
    }
    // Reported so the margin can be re-measured (plan §7 Q1).
    eprintln!("healthy leader {leader}: max quorum-ack age {max_age} ms");
    assert!(
        Duration::from_millis(max_age) <= crate::QUORUM_ACK_MAX_AGE,
        "a healthy leader reported a quorum-ack age of {max_age} ms, past the \
         {:?} promotion gate",
        crate::QUORUM_ACK_MAX_AGE
    );

    cluster.stop_all();
}

/// Pins the premise of the promotion gate: openraft 0.9 has no
/// check-quorum, so a leader cut off from every peer keeps its role, and
/// only its ageing quorum ack tells it apart from a healthy one. If this
/// fails, openraft changed and the gate's design must be revisited.
#[test]
fn a_leader_that_loses_both_peers_keeps_leading_and_its_ack_ages() {
    let mut cluster = start_cluster(3);
    let refs: Vec<&Node> = cluster.nodes.iter().collect();
    let leader = await_single_leader(&refs);
    await_quorum_ack(cluster.status(leader));

    for id in cluster.others(leader) {
        cluster.stop_node(id);
    }
    std::thread::sleep(crate::QUORUM_ACK_MAX_AGE + Duration::from_secs(1));

    let status = cluster.status(leader);
    assert!(status.running.load(Ordering::Relaxed));
    assert_eq!(
        status.role.load(Ordering::Relaxed),
        RaftStatus::ROLE_LEADER,
        "a leader with no reachable peer stepped down: openraft gained check-quorum"
    );
    let age = status
        .millis_since_quorum_ack()
        .expect("a leader acknowledged once keeps reporting an age");
    assert!(
        Duration::from_millis(age) > crate::QUORUM_ACK_MAX_AGE,
        "the quorum-ack age ({age} ms) did not grow past the {:?} gate with both peers gone",
        crate::QUORUM_ACK_MAX_AGE
    );

    cluster.stop_all();
}

/// The leader's quorum round trip confirms its own term and no other; a
/// follower's is refused at once; a stopped driver's is refused too.
#[test]
fn leadership_is_confirmed_for_the_leaders_own_term_only() {
    let mut cluster = start_cluster(3);
    let refs: Vec<&Node> = cluster.nodes.iter().collect();
    let leader = await_single_leader(&refs);
    await_quorum_ack(cluster.status(leader));
    let term = cluster.status(leader).term.load(Ordering::Relaxed);
    let leader_confirm = cluster.handles(leader).leader_confirm.clone();

    assert_eq!(leader_confirm.confirm(term), Ok(()));
    assert_eq!(
        leader_confirm.confirm(term - 1),
        Err(LeaderConfirmError::TermMoved { now: term })
    );

    let follower = cluster.others(leader)[0];
    let follower_term = cluster.status(follower).term.load(Ordering::Relaxed);
    assert_eq!(
        cluster
            .handles(follower)
            .leader_confirm
            .confirm(follower_term),
        Err(LeaderConfirmError::NotLeader)
    );

    cluster.stop_all();
    assert_eq!(
        leader_confirm.confirm(term),
        Err(LeaderConfirmError::DriverStopped)
    );
}

/// With both followers stopped, their connects are refused at once, so the
/// leader's confirmation fails with exactly `NoQuorum` and well inside its
/// deadline rather than timing out (a frozen link would allow `TimedOut`
/// as well).
#[test]
fn a_leader_without_a_live_peer_is_refused_confirmation_for_lack_of_quorum() {
    let mut cluster = start_cluster(3);
    let refs: Vec<&Node> = cluster.nodes.iter().collect();
    let leader = await_single_leader(&refs);
    await_quorum_ack(cluster.status(leader));
    let term = cluster.status(leader).term.load(Ordering::Relaxed);

    for id in cluster.others(leader) {
        cluster.stop_node_and_join(id);
    }
    let started = Instant::now();
    assert_eq!(
        cluster.handles(leader).leader_confirm.confirm(term),
        Err(LeaderConfirmError::NoQuorum)
    );
    let took = started.elapsed();
    assert!(
        took < LEADER_CONFIRM_TIMEOUT,
        "the refusal took {took:?}, past the {LEADER_CONFIRM_TIMEOUT:?} deadline"
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

/// Recency steering over real sockets: while a quorum of caught-up nodes
/// exists, a node whose advertised journal tip is behind cannot assemble
/// a quorum, so leadership lands on a most-caught-up node — including
/// across a re-election after the leader dies. This is the property
/// auto-promotion relies on to prefer a most-caught-up replica.
///
/// Five nodes (not three) so a caught-up quorum survives the leader kill
/// without the behind node's cooperation. With only two survivors the
/// filter's *liveness escape* is load-dependent by design: quorum would
/// need the behind node's grant, the behind node's filtered campaigns
/// still inflate its term (openraft has no pre-vote), and sustained
/// leadership churn legitimately opens the escape — best-effort steering,
/// with promotion-time checks staying authoritative (see
/// `crate::recency`).
#[test]
fn behind_node_never_wins_an_election() {
    // Nodes 1–4 hold seq 100; node 5 is behind at seq 10. Node 5 can only
    // win with a caught-up grant, and every caught-up node drops its vote
    // requests (candidate tip 10 < local tip 100).
    let mut cluster = start_cluster_with_tips(&[100, 100, 100, 100, 10]);
    let refs: Vec<&Node> = cluster.nodes.iter().collect();
    let first = await_single_leader(&refs);
    assert_ne!(first, 5, "the behind node must not win the first election");

    // Kill the leader: three caught-up nodes remain — a quorum (3 of 5)
    // that elects among itself long before the behind node's dropped
    // campaigns could open the liveness escape.
    cluster.stop_node(first);
    let survivors: Vec<&Node> = cluster.nodes.iter().filter(|n| n.id != first).collect();
    let second = await_single_leader(&survivors);
    assert_ne!(second, 5, "the behind node must not win the re-election");

    cluster.stop_all();
}
