//! `ControlNode` — the deterministic core of a control-plane Raft peer.
//!
//! Wraps a `RawNode<FileStorage>` and owns the one piece of logic that
//! is easy to get subtly wrong: the **ready-loop persistence ordering**
//! (snapshot → entries → hard state → only then the persisted
//! messages; commit index persisted before applying). The server's
//! control-plane thread supplies clocks and sockets; everything in
//! here is synchronous and I/O-free apart from the storage fsyncs, so
//! multi-node behaviour is testable in-process with a simulated
//! network (see the `sim` tests).

use std::io;
use std::path::Path;

use raft::eraftpb::{ConfChange, ConfChangeV2, Entry, EntryType, Message};
use raft::{Config, RawNode, StateRole};
use tracing::{info, warn};

use crate::storage::FileStorage;

/// Raft timing, in ticks. The driver thread owns the tick length; with
/// the recommended 100 ms tick these defaults give a 200 ms heartbeat
/// and a 1–2 s randomized election timeout — deliberately slow for a
/// control plane (failover latency is dominated by promotion, not
/// detection) and far above LAN RTT + two fsyncs, so healthy clusters
/// never elect spuriously.
pub const HEARTBEAT_TICKS: usize = 2;
pub const ELECTION_TICKS: usize = 10;

/// One control-plane Raft peer.
pub struct ControlNode {
    raw: RawNode<FileStorage>,
}

/// What a drained ready handed to the caller: messages to put on the
/// wire, plus committed application entries (none in step 1 — config
/// payloads arrive with the config-propagation step).
#[derive(Debug, Default)]
pub struct Drained {
    /// Peer messages, in send order. Every message in here is already
    /// safe to send: `drain_ready` only surfaces them after the state
    /// they depend on has been fsynced.
    pub messages: Vec<Message>,
    /// Committed `EntryNormal` payloads (non-empty data only), in
    /// apply order.
    pub committed: Vec<Vec<u8>>,
}

impl ControlNode {
    /// Open (bootstrapping if fresh) a node with identity `id` and the
    /// cluster's initial `voters`. Every node of a new cluster must be
    /// given the same voter set; on later boots the persisted
    /// membership wins and `voters` is ignored.
    pub fn open(id: u64, dir: &Path, voters: &[u64]) -> io::Result<Self> {
        let mut storage = FileStorage::open(dir)?;
        if !storage.initialized() {
            storage.initialize_with_conf_state(voters.to_vec())?;
            info!(id, ?voters, "bootstrapped control-plane raft membership");
        }

        let config = Config {
            id,
            election_tick: ELECTION_TICKS,
            heartbeat_tick: HEARTBEAT_TICKS,
            // Pre-vote (raft thesis §9.6): a partitioned node that
            // rejoins cannot force an election (and thus a spurious
            // failover) by having inflated its term while isolated.
            pre_vote: true,
            // Leader steps down when it hasn't heard from a quorum for
            // an election timeout — the property auto-promotion (step
            // 3) relies on so an isolated ex-leader stops acting.
            check_quorum: true,
            ..Default::default()
        };
        config
            .validate()
            .map_err(|e| io::Error::other(format!("raft config invalid: {e}")))?;

        let raw = RawNode::new(&config, storage, &crate::tracing_logger())
            .map_err(|e| io::Error::other(format!("raft node init failed: {e}")))?;
        Ok(Self { raw })
    }

    /// Advance the logical clock by one tick (the driver calls this at
    /// a fixed cadence). Returns `true` if raft wants a ready drained.
    pub fn tick(&mut self) -> bool {
        self.raw.tick()
    }

    /// Feed one inbound peer message. The caller applies the
    /// journal-tip recency filter ([`crate::recency`]) *before* this —
    /// by the time a message reaches the state machine it is
    /// unconditional.
    pub fn step(&mut self, msg: Message) {
        // A step error means raft refused the message (e.g. unknown
        // peer after a membership change, stale term chatter). That is
        // peer-input trouble, not a local invariant violation — log
        // and drop, mirroring how the replication receiver treats
        // malformed frames.
        if let Err(e) = self.raw.step(msg) {
            warn!(error = %e, "control-plane raft rejected a peer message");
        }
    }

    /// Ask raft to start an election now (test/ops hook; normal
    /// elections come from tick timeouts).
    pub fn campaign(&mut self) -> io::Result<()> {
        self.raw
            .campaign()
            .map_err(|e| io::Error::other(format!("campaign failed: {e}")))
    }

    /// True when raft has state to persist, messages to send, or
    /// entries to apply.
    pub fn has_ready(&self) -> bool {
        self.raw.has_ready()
    }

    /// Drain one ready, honouring the persistence contract:
    ///
    /// 1. immediately-sendable messages are collected;
    /// 2. snapshot, then entries, then `HardState` are **fsynced**;
    /// 3. only then are the persisted-dependent messages collected
    ///    (vote responses above all — see `storage.rs` on double-vote);
    /// 4. the commit index is fsynced **before** committed entries are
    ///    surfaced for application;
    /// 5. committed conf changes are applied to raft + storage here;
    ///    normal entries are handed back for the caller to apply.
    ///
    /// An `Err` from storage leaves raft inoperable by contract — the
    /// caller must stop driving this node (and keep the exchange
    /// running; the control plane is not the data plane).
    pub fn drain_ready(&mut self) -> io::Result<Drained> {
        let mut out = Drained::default();
        if !self.raw.has_ready() {
            return Ok(out);
        }

        let mut ready = self.raw.ready();

        // Messages that don't depend on this ready's persistence.
        out.messages.extend(ready.take_messages());

        if !ready.snapshot().data.is_empty() || ready.snapshot().metadata.is_some() {
            let snapshot = ready.snapshot().clone();
            self.raw.mut_store().apply_snapshot(snapshot)?;
        }
        if !ready.entries().is_empty() {
            let entries = ready.entries().clone();
            self.raw.mut_store().append(&entries)?;
        }
        if let Some(hs) = ready.hs() {
            let hs = hs.clone();
            self.raw.mut_store().set_hard_state(&hs)?;
        }
        out.messages.extend(ready.take_persisted_messages());

        let committed = ready.take_committed_entries();
        self.apply_committed(committed, &mut out)?;

        let mut light = self.raw.advance(ready);
        if let Some(commit) = light.commit_index() {
            self.raw.mut_store().set_commit(commit)?;
        }
        out.messages.extend(light.take_messages());
        let committed = light.take_committed_entries();
        self.apply_committed(committed, &mut out)?;
        self.raw.advance_apply();

        Ok(out)
    }

    /// Apply a batch of committed entries: conf changes mutate raft +
    /// durable membership here; normal payloads are handed to the
    /// caller. Empty `EntryNormal` data (the no-op a fresh leader
    /// commits) is skipped.
    fn apply_committed(&mut self, entries: Vec<Entry>, out: &mut Drained) -> io::Result<()> {
        for entry in entries {
            match entry.entry_type() {
                EntryType::EntryNormal => {
                    if !entry.data.is_empty() {
                        out.committed.push(entry.data);
                    }
                }
                EntryType::EntryConfChange => {
                    let cc: ConfChange = prost_decode(&entry.data)?;
                    let cs = self
                        .raw
                        .apply_conf_change(&cc)
                        .map_err(|e| io::Error::other(format!("conf change failed: {e}")))?;
                    self.raw.mut_store().set_conf_state(cs)?;
                }
                EntryType::EntryConfChangeV2 => {
                    let cc: ConfChangeV2 = prost_decode(&entry.data)?;
                    let cs = self
                        .raw
                        .apply_conf_change(&cc)
                        .map_err(|e| io::Error::other(format!("conf change failed: {e}")))?;
                    self.raw.mut_store().set_conf_state(cs)?;
                }
            }
        }
        Ok(())
    }

    /// This node's id.
    pub fn id(&self) -> u64 {
        self.raw.raft.id
    }

    /// Current raft term. Doubles as the fencing epoch a promotion
    /// journals (`EpochBump { epoch: term }`) — terms are unique per
    /// leader tenure, which closes the manual-failover dual-promotion
    /// collision documented in `docs/replication.md`.
    pub fn term(&self) -> u64 {
        self.raw.raft.term
    }

    /// Current role (leader / follower / candidate / pre-candidate).
    pub fn role(&self) -> StateRole {
        self.raw.raft.state
    }

    /// The leader this node currently believes in; `None` when unknown
    /// (mid-election).
    pub fn leader_id(&self) -> Option<u64> {
        match self.raw.raft.leader_id {
            raft::INVALID_ID => None,
            id => Some(id),
        }
    }
}

fn prost_decode<M: prost::Message + Default>(data: &[u8]) -> io::Result<M> {
    M::decode(data).map_err(|e| io::Error::other(format!("undecodable conf-change entry: {e}")))
}

#[cfg(test)]
mod sim {
    //! In-process multi-node simulation: real `ControlNode`s (real
    //! fsyncs into tempdirs) exchanging messages over a scriptable
    //! in-memory network. No sockets, no sleeps — ticks are the only
    //! clock, so every scenario is deterministic apart from raft's own
    //! randomized election timeout (bounded, so `for` limits stay
    //! small).

    use super::*;
    use crate::recency::{JournalTip, candidate_is_current, is_vote_request};
    use std::collections::HashMap;

    struct Cluster {
        nodes: HashMap<u64, ControlNode>,
        dirs: HashMap<u64, tempfile::TempDir>,
        /// Node ids currently partitioned away (messages to/from are
        /// dropped).
        down: Vec<u64>,
        /// Journal tips per node for the recency filter; `None`
        /// disables filtering (default).
        tips: Option<HashMap<u64, JournalTip>>,
        /// In-flight messages.
        inbox: Vec<Message>,
    }

    impl Cluster {
        fn new(ids: &[u64]) -> Self {
            let mut nodes = HashMap::new();
            let mut dirs = HashMap::new();
            for &id in ids {
                let dir = tempfile::tempdir().unwrap();
                nodes.insert(id, ControlNode::open(id, dir.path(), ids).unwrap());
                dirs.insert(id, dir);
            }
            Self {
                nodes,
                dirs,
                down: Vec::new(),
                tips: None,
                inbox: Vec::new(),
            }
        }

        /// One cluster step: tick every live node, drain readies,
        /// deliver messages (applying partitions and the recency
        /// filter).
        fn step_all(&mut self) {
            let ids: Vec<u64> = self.nodes.keys().copied().collect();
            for id in &ids {
                if self.down.contains(id) {
                    continue;
                }
                let node = self.nodes.get_mut(id).unwrap();
                node.tick();
                while node.has_ready() {
                    let drained = node.drain_ready().unwrap();
                    self.inbox.extend(drained.messages);
                }
            }
            // Deliver everything currently in flight.
            let inbox = std::mem::take(&mut self.inbox);
            for msg in inbox {
                if self.down.contains(&msg.to) || self.down.contains(&msg.from) {
                    continue;
                }
                if let Some(tips) = &self.tips
                    && is_vote_request(msg.msg_type())
                    && !candidate_is_current(tips[&msg.from], tips[&msg.to])
                {
                    continue; // voter drops the stale candidate's request
                }
                if let Some(node) = self.nodes.get_mut(&msg.to) {
                    node.step(msg);
                }
            }
        }

        fn leader(&self) -> Option<u64> {
            let leaders: Vec<u64> = self
                .nodes
                .iter()
                .filter(|(id, n)| !self.down.contains(id) && n.role() == StateRole::Leader)
                .map(|(id, _)| *id)
                .collect();
            match leaders.as_slice() {
                [single] => Some(*single),
                [] => None,
                // More than one *visible* leader is only legal across
                // terms; assert they differ so a real split brain fails
                // loudly in every test that polls for a leader.
                multiple => {
                    let terms: Vec<u64> = multiple.iter().map(|id| self.nodes[id].term()).collect();
                    assert!(
                        terms.windows(2).all(|w| w[0] != w[1]),
                        "two leaders in the same term: {multiple:?} terms {terms:?}"
                    );
                    None // unsettled — keep stepping
                }
            }
        }

        /// Step until exactly one live leader exists, up to `max`
        /// rounds.
        fn settle(&mut self, max: usize) -> u64 {
            for _ in 0..max {
                self.step_all();
                if let Some(leader) = self.leader() {
                    // One extra round so followers observe the leader.
                    self.step_all();
                    return leader;
                }
            }
            panic!("no leader after {max} rounds");
        }
    }

    #[test]
    fn three_nodes_elect_exactly_one_leader() {
        let mut c = Cluster::new(&[1, 2, 3]);
        let leader = c.settle(200);
        // Followers agree on who leads.
        for (id, node) in &c.nodes {
            if *id != leader {
                assert_eq!(node.role(), StateRole::Follower);
                assert_eq!(node.leader_id(), Some(leader));
            }
        }
    }

    #[test]
    fn killing_the_leader_elects_a_new_one() {
        let mut c = Cluster::new(&[1, 2, 3]);
        let first = c.settle(200);
        let first_term = c.nodes[&first].term();

        c.down.push(first);
        let second = c.settle(400);
        assert_ne!(second, first);
        // The new tenure has a strictly higher term — the fencing-epoch
        // guarantee auto-promotion will rely on.
        assert!(c.nodes[&second].term() > first_term);
    }

    #[test]
    fn minority_cannot_elect() {
        let mut c = Cluster::new(&[1, 2, 3]);
        let leader = c.settle(200);
        // Partition away two nodes — the remaining one (whichever it
        // is) can never win alone.
        let lone: u64 = *c.nodes.keys().find(|id| **id != leader).unwrap();
        for id in c.nodes.keys().copied().collect::<Vec<_>>() {
            if id != lone {
                c.down.push(id);
            }
        }
        for _ in 0..400 {
            c.step_all();
        }
        assert_ne!(c.nodes[&lone].role(), StateRole::Leader);
    }

    #[test]
    fn recency_filter_steers_election_to_the_caught_up_node() {
        // Node 3 is behind on the journal; nodes 1 and 2 are at the
        // tip. With the filter active, node 3 must never win — run the
        // scenario several times to cover raft's randomized timeouts.
        for _ in 0..5 {
            let mut c = Cluster::new(&[1, 2, 3]);
            let mut tips = HashMap::new();
            tips.insert(
                1,
                JournalTip {
                    epoch: 5,
                    last_sequence: 1_000,
                },
            );
            tips.insert(
                2,
                JournalTip {
                    epoch: 5,
                    last_sequence: 1_000,
                },
            );
            tips.insert(
                3,
                JournalTip {
                    epoch: 5,
                    last_sequence: 400,
                },
            );
            c.tips = Some(tips);

            let leader = c.settle(400);
            assert_ne!(leader, 3, "stale node must not win the election");
        }
    }

    #[test]
    fn restarted_node_rejoins_and_keeps_its_term() {
        let mut c = Cluster::new(&[1, 2, 3]);
        let leader = c.settle(200);
        let follower = *c.nodes.keys().find(|id| **id != leader).unwrap();

        // "Crash" the follower and reopen it from its own directory —
        // the persisted vote/term must carry over (double-vote guard).
        let term_before = c.nodes[&follower].term();
        let dir = c.dirs[&follower].path().to_path_buf();
        c.nodes.remove(&follower);
        let reopened = ControlNode::open(follower, &dir, &[]).unwrap();
        assert!(reopened.term() >= term_before);
        c.nodes.insert(follower, reopened);

        // It follows again without disturbing the leader.
        for _ in 0..50 {
            c.step_all();
        }
        assert_eq!(c.leader(), Some(leader));
        assert_eq!(c.nodes[&follower].leader_id(), Some(leader));
    }
}
