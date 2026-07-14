//! In-process 3-node election tests: three real driver threads (each with
//! its own current-thread runtime, storage dir, and TCP listener on
//! localhost) authenticated with real Ed25519 keys — the production shape,
//! one runtime per node, minus only the process boundaries.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use base64::Engine;
use ed25519_dalek::SigningKey;
use melin_app::auth::AuthorizedKeys;
use melin_raft::driver::{RaftConfig, RaftHandles, RaftPeer, spawn};
use melin_transport_core::health::RaftStatus;
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

fn start_cluster(n: u64) -> Cluster {
    let keys: Vec<SigningKey> = (0..n)
        .map(|i| SigningKey::from_bytes(&[i as u8 + 1; 32]))
        .collect();

    // Reserve n distinct localhost ports by binding and dropping.
    let addrs: Vec<String> = (0..n)
        .map(|_| {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().to_string()
        })
        .collect();

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
    let authorized_keys = Arc::new(AuthorizedKeys::parse(&table).unwrap());

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
