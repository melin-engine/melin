//! In-process 3-node election tests: three real driver threads (each with
//! its own current-thread runtime, storage dir, and TCP listener on
//! localhost) authenticated with real Ed25519 keys — the production shape,
//! one runtime per node, minus only the process boundaries.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use crate::driver::{
    ELECTION_TIMEOUT_MAX_MS, LEADER_CONFIRM_TIMEOUT, LeaderConfirmError, RaftConfig, RaftHandles,
    RaftPeer, spawn,
};
use crate::tests::authorized_keys_table;
use crate::tests::tip_at;
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
    /// Its raft listener, where the others dial it.
    addr: std::net::SocketAddr,
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

    let names: Vec<String> = (1..=n).map(|id| format!("node-{id}")).collect();
    let entries: Vec<(&SigningKey, &str, &str)> = keys
        .iter()
        .zip(&names)
        .map(|(k, name)| (k, "replication", name.as_str()))
        .collect();
    let authorized_keys = Arc::new(authorized_keys_table(&entries));

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
        let addr = addrs[i].parse().unwrap();
        let config = RaftConfig {
            node_id: i as u64 + 1,
            bind: addr,
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
            addr,
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
    let peer_key = SigningKey::from_bytes(&[2; 32]).verifying_key().to_bytes();
    spawn_node_1_with_peer_key(dir, bind, self_addr, peer_addr, peer_key)
}

/// [`spawn_node_1`], with node 2's entry naming `peer_key`.
fn spawn_node_1_with_peer_key(
    dir: &TempDir,
    bind: &str,
    self_addr: &str,
    peer_addr: &str,
    peer_key: [u8; 32],
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
            pubkey: peer_key,
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

/// A peer list naming this node's key for node 2 too is refused at
/// startup, naming both entries, before the listener is bound.
#[test]
fn spawn_refuses_a_key_named_by_two_peer_entries() {
    let dir = TempDir::new().unwrap();
    let own_key = SigningKey::from_bytes(&[1; 32]).verifying_key().to_bytes();
    let Err(e) = spawn_node_1_with_peer_key(
        &dir,
        "127.0.0.1:0",
        "127.0.0.1:7001",
        "127.0.0.1:7002",
        own_key,
    ) else {
        panic!("a key shared by two --raft-peer entries must be refused");
    };
    assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput);
    assert_eq!(
        e.to_string(),
        "--raft-peer ids 1 and 2 name the same public key: each member needs its own \
         replication key"
    );
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
    /// closed and connecting to it is refused. Returns that listener's
    /// address.
    fn stop_node_and_join(&mut self, id: u64) -> std::net::SocketAddr {
        self.stop_node(id);
        let at = self.nodes.iter().position(|n| n.id == id).unwrap();
        let node = self.nodes.remove(at);
        node.handles.join.join().expect("driver thread panicked");
        node.addr
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

/// The `melin_raft_quorum_ack_age_ms` level `docs/replication.md` tells
/// operators to alert on: a healthy leader stays below it and a leader
/// cut off from its peers passes it.
const QUORUM_ACK_ALERT_AGE: Duration = Duration::from_secs(1);

// The same document tells operators that a leader past the alert level no
// longer auto-promotes, which holds only while the level is the
// promotion filter's threshold. That threshold is derived from the raft
// timing constants, so a retuning that moves it breaks the build here
// rather than leaving the documentation quietly wrong.
const _: () = assert!(
    QUORUM_ACK_ALERT_AGE.as_millis() == crate::QUORUM_ACK_MAX_AGE.as_millis(),
    "QUORUM_ACK_MAX_AGE moved off the documented one-second alert level: update docs/replication.md"
);

/// Pins the operator guidance and the promotion filter's liveness
/// empirically: a leader whose peers all answer never reports an age at
/// the alert level, nor one the filter refuses.
#[test]
fn a_healthy_leaders_quorum_ack_age_stays_under_the_alert_level() {
    const SAMPLE_FOR: Duration = Duration::from_secs(3);
    // The age peaks (about one tick) only between a heartbeat's send and
    // its reply, a round trip, and reads near 0 the rest of the time: a
    // coarse sampler never sees the peak this test is about. Even this one
    // often misses it, idle machine or not: the driver publishes only what
    // openraft's metrics reports carry, and those are mostly taken just
    // after an ack, so the peak reaches the status only some of the time
    // (and a loaded machine's overshooting sleep adds to the misses). That
    // only weakens the bound for the run (the reported max shows it, under
    // `--nocapture`: a maximum of 0 means the run proved nothing); it
    // cannot fail a healthy cluster.
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
    // Reported so the margin can be re-measured on other hardware; the
    // harness captures it on a passing run, so read it with `--nocapture`.
    eprintln!("healthy leader {leader}: max quorum-ack age {max_age} ms");
    assert!(
        Duration::from_millis(max_age) < QUORUM_ACK_ALERT_AGE,
        "a healthy leader reported a quorum-ack age of {max_age} ms, at or past \
         the {QUORUM_ACK_ALERT_AGE:?} alert level"
    );
    // Implied by the alert bound while the two levels are equal (the
    // assertion above them), and asserted anyway so the filter's liveness
    // stays pinned should they ever part.
    assert!(
        Duration::from_millis(max_age) <= crate::QUORUM_ACK_MAX_AGE,
        "a healthy leader reported a quorum-ack age of {max_age} ms, past the {:?} \
         auto-promotion filter",
        crate::QUORUM_ACK_MAX_AGE
    );

    cluster.stop_all();
}

/// Pins the premise of the gauge and of the auto-promotion leader-lease
/// filter: openraft 0.9 has no check-quorum, so a leader cut off from
/// every peer keeps its role, and only its ageing quorum ack tells it
/// apart from a healthy one. If this fails, openraft changed and the
/// gauge's operator guidance and the filter's design must be revisited.
#[test]
fn a_leader_that_loses_both_peers_keeps_leading_and_its_ack_ages() {
    let mut cluster = start_cluster(3);
    let refs: Vec<&Node> = cluster.nodes.iter().collect();
    let leader = await_single_leader(&refs);
    await_quorum_ack(cluster.status(leader));

    for id in cluster.others(leader) {
        cluster.stop_node(id);
    }

    // Watch the leader until its age passes the alert level, the
    // promotion filter's threshold, and the longest election timeout, the
    // point by which a check-quorum leader would have stepped down. Polled
    // rather than slept: the age is published on the leader's ticks, so a
    // fixed sleep would race a late tick on a loaded machine; the
    // deadline bounds the wait.
    let stale_for = QUORUM_ACK_ALERT_AGE
        .max(crate::QUORUM_ACK_MAX_AGE)
        .max(Duration::from_millis(ELECTION_TIMEOUT_MAX_MS));
    let status = cluster.status(leader);
    let deadline = Instant::now() + ELECTION_DEADLINE;
    loop {
        assert!(status.running.load(Ordering::Relaxed));
        assert_eq!(
            status.role.load(Ordering::Relaxed),
            RaftStatus::ROLE_LEADER,
            "a leader with no reachable peer stepped down: openraft gained check-quorum"
        );
        let age = status
            .millis_since_quorum_ack()
            .expect("a leader acknowledged once keeps reporting an age");
        if Duration::from_millis(age) > stale_for {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the quorum-ack age ({age} ms) did not grow past {stale_for:?} within \
             {ELECTION_DEADLINE:?} with both peers gone"
        );
        std::thread::sleep(Duration::from_millis(10));
    }

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
    // A term it no longer leads in, and one it never reached.
    for other in [term - 1, term + 1] {
        assert_eq!(
            leader_confirm.confirm(other),
            Err(LeaderConfirmError::TermMoved { now: term })
        );
    }

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
/// deadline rather than timing out.
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

/// A leader cut off from its majority by links gone silent (each follower's
/// address accepts the connection, then never answers) is refused, and
/// within the confirmation's deadline: the one case where nothing answers
/// at all, so only openraft's per-voter wait or the deadline ends it.
#[test]
fn a_leader_whose_peers_went_silent_is_refused_within_the_deadline() {
    /// What the waiting thread may overrun the deadline by on a loaded
    /// host, between the deadline passing and it being scheduled again.
    const WAKE_UP_ALLOWANCE: Duration = Duration::from_millis(250);

    let mut cluster = start_cluster(3);
    let refs: Vec<&Node> = cluster.nodes.iter().collect();
    let leader = await_single_leader(&refs);
    await_quorum_ack(cluster.status(leader));
    let term = cluster.status(leader).term.load(Ordering::Relaxed);

    // Listening on each follower's address and never accepting: the
    // kernel completes the connect, and the leader's handshake then waits
    // on a peer that never speaks.
    let _silent: Vec<std::net::TcpListener> = cluster
        .others(leader)
        .into_iter()
        .map(|id| {
            let addr = cluster.stop_node_and_join(id);
            std::net::TcpListener::bind(addr).expect("rebind the stopped follower's address")
        })
        .collect();

    let started = Instant::now();
    let refused = cluster.handles(leader).leader_confirm.confirm(term);
    let took = started.elapsed();
    // Reported so which bound ended the wait shows under `--nocapture`.
    eprintln!("silent peers: {refused:?} after {took:?}");
    // `NoQuorum` when openraft's per-voter wait ends first, as the
    // liveness assertion on the deadline intends; `TimedOut` if a loaded
    // host delays its answer past the deadline. Either refuses.
    assert!(
        matches!(
            refused,
            Err(LeaderConfirmError::NoQuorum | LeaderConfirmError::TimedOut)
        ),
        "a leader with no answering peer got {refused:?}"
    );
    assert!(
        took < LEADER_CONFIRM_TIMEOUT + WAKE_UP_ALLOWANCE,
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
    // It is its own majority, so its leadership is confirmed with no
    // round trip at all.
    let term = cluster.status(1).term.load(Ordering::Relaxed);
    assert_eq!(cluster.handles(1).leader_confirm.confirm(term), Ok(()));
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
