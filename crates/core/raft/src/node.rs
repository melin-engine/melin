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
use tracing::{debug, info};

use crate::storage::FileStorage;

/// Raft timing, in ticks. The driver thread owns the tick length; with
/// the recommended 100 ms tick these defaults give a 200 ms heartbeat
/// and a 1–2 s randomized election timeout — deliberately slow for a
/// control plane (failover latency is dominated by promotion, not
/// detection) and far above LAN RTT + two fsyncs, so healthy clusters
/// never elect spuriously.
pub const HEARTBEAT_TICKS: usize = 2;
pub const ELECTION_TICKS: usize = 10;

/// Applied log entries older than this many indexes behind the applied
/// index are compacted away. Bounds the state file (every mutation
/// rewrites the whole log) while leaving a buffer so a briefly-lagging
/// follower catches up via appends rather than a snapshot. A few hundred
/// entries is negligible on disk and covers realistic control-plane
/// lag at election/config cadence.
const LOG_RETENTION: u64 = 512;

/// One control-plane Raft peer.
pub struct ControlNode {
    raw: RawNode<FileStorage>,
    /// Compaction retention window, [`LOG_RETENTION`] in production;
    /// `u64` to match raft's index arithmetic. A field (not the const
    /// directly) so tests can shrink it to reach the snapshot path
    /// without generating hundreds of entries.
    log_retention: u64,
}

/// What a drained ready handed to the caller: messages to put on the
/// wire, plus committed application entries (none in step 1 — config
/// payloads arrive with the config-propagation step).
#[derive(Debug, Default)]
pub struct Drained {
    /// Peer messages, in send order. Every message in here is already
    /// safe to send: `drain_ready` only surfaces them after the state
    /// they depend on has been fsynced.
    ///
    /// **Delivery-report contract**: a `MsgSnapshot` in here puts the
    /// recipient's leader-side progress into a paused snapshot state
    /// that raft never times out on its own. If the transport fails to
    /// deliver one, the caller **must** call
    /// [`ControlNode::report_snapshot`] with
    /// [`SnapshotStatus::Failure`](raft::SnapshotStatus::Failure) —
    /// otherwise the leader stops replicating to that peer until the
    /// next leadership change. (A *delivered* snapshot needs no report:
    /// the follower's ack un-pauses the progress by itself.)
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

        // Seed the applied index from the persisted commit index.
        // `drain_ready` applies every committed entry synchronously
        // before it returns (no async apply), so on a clean process
        // everything committed is also applied. Without this, a restart
        // leaves `applied` at 0 and raft re-delivers every committed
        // entry from the truncation point — harmless for the empty
        // election no-ops, but re-running a committed conf-change
        // against the already-updated membership makes raft-rs error
        // (e.g. "config is already joint"), which would permanently
        // stop the driver on every boot.
        let applied = storage.hard_state().commit;

        let config = Config {
            id,
            applied,
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
            // Cap the bytes of log entries a single MsgAppend carries so
            // no message overruns the peer wire frame cap
            // (`melin_raft::wire::MAX_FRAME`, 4 MiB). 1 MiB leaves ample
            // room for the frame envelope; a catching-up follower just
            // receives more, smaller appends.
            max_size_per_msg: 1 << 20,
            ..Default::default()
        };
        config
            .validate()
            .map_err(|e| io::Error::other(format!("raft config invalid: {e}")))?;

        let raw = RawNode::new(&config, storage, &crate::tracing_logger())
            .map_err(|e| io::Error::other(format!("raft node init failed: {e}")))?;
        Ok(Self {
            raw,
            log_retention: LOG_RETENTION,
        })
    }

    /// Shrink the compaction retention window so tests can reach the
    /// snapshot catch-up path with a handful of entries.
    #[cfg(test)]
    fn set_log_retention(&mut self, retention: u64) {
        self.log_retention = retention;
    }

    /// Report the delivery outcome of a `MsgSnapshot` previously handed
    /// out via [`Drained::messages`] — see the contract there. Safe to
    /// call with a stale peer id (raft ignores unknown ids).
    pub fn report_snapshot(&mut self, id: u64, status: raft::SnapshotStatus) {
        self.raw.report_snapshot(id, status);
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
        // peer-input trouble, not a local invariant violation, and a
        // misconfigured or removed peer can trigger it repeatedly — so
        // it is a client-caused event at `debug`, mirroring how the
        // replication receiver treats malformed frames.
        if let Err(e) = self.raw.step(msg) {
            debug!(error = %e, "control-plane raft rejected a peer message");
        }
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

        self.maybe_compact()?;

        Ok(out)
    }

    /// Discard applied log entries older than the retention window so
    /// the log — and the whole-file state rewrite on every mutation —
    /// stays bounded rather than growing for the life of the
    /// deployment. Keeps [`LOG_RETENTION`] entries behind the applied
    /// index so a briefly-lagging follower still catches up via appends;
    /// one further behind gets a snapshot (metadata-only here, which is
    /// sufficient — the control-plane "state" is just membership + term,
    /// both carried in the snapshot). `compact` is a no-op (no fsync)
    /// when there is nothing old enough to drop, so this is cheap on the
    /// common path.
    fn maybe_compact(&mut self) -> io::Result<()> {
        let applied = self.raw.raft.raft_log.applied();
        let Some(target) = applied.checked_sub(self.log_retention) else {
            return Ok(()); // fewer than a window's worth applied yet
        };
        self.raw.mut_store().compact(target)
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

    /// Last raft log index this node holds (test observability).
    #[cfg(test)]
    fn last_log_index(&self) -> u64 {
        self.raw.raft.raft_log.last_index()
    }

    /// Current raft term. Terms are unique per leader tenure, so a
    /// later step will use the term to allocate the replication fencing
    /// epoch a promotion journals (`EpochBump { epoch: term }`) —
    /// closing the manual-failover dual-promotion collision documented
    /// in `docs/replication.md`. Today the term is election bookkeeping
    /// only; nothing journals it yet.
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
    use crate::recency::{JournalTip, VoteFilter, is_vote_request};
    use raft::eraftpb::MessageType;
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
        /// Per-voter stateful recency gate (one per local node, like
        /// the driver holds), lazily created while `tips` is active.
        filters: HashMap<u64, VoteFilter>,
        /// While `true`, `MsgSnapshot` frames are silently dropped in
        /// delivery (simulating a transport that loses them without
        /// reporting) and counted in `snapshots_dropped`.
        drop_snapshots: bool,
        snapshots_dropped: u32,
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
                filters: HashMap::new(),
                drop_snapshots: false,
                snapshots_dropped: 0,
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
                if self.drop_snapshots && msg.msg_type() == MessageType::MsgSnapshot {
                    self.snapshots_dropped += 1;
                    continue; // lost in transit, nobody reports it
                }
                if let Some(tips) = &self.tips
                    && is_vote_request(msg.msg_type())
                    && !self
                        .filters
                        .entry(msg.to)
                        .or_default()
                        .should_deliver(tips[&msg.from], tips[&msg.to])
                {
                    continue; // voter drops the stale candidate's request
                }
                if let Some(node) = self.nodes.get_mut(&msg.to) {
                    node.step(msg);
                }
            }
            // Mirror the driver: a node that currently sees a leader
            // re-arms its vote filter.
            for (id, node) in &self.nodes {
                if !self.down.contains(id) && node.leader_id().is_some() {
                    self.filters.entry(*id).or_default().leader_observed();
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
    fn tip_log_conflict_escapes_deadlock_and_elects_the_log_ahead_node() {
        // The verified liveness hazard: a control-plane-only partition
        // deposes primary A; the pair elects a new leader whose no-op A
        // never sees. That leader dies and A heals — the live pair is a
        // quorum, but A holds the highest journal tip with the OLDER
        // raft log, and survivor T the newer log with a frozen lower
        // tip. A's filter drops T's requests; raft's log check rejects
        // A's. Without the VoteFilter escape this sits leaderless
        // forever (settle panics); with it, T must win.
        let mut c = Cluster::new(&[1, 2, 3]);
        let a = c.settle(200);

        // Control-plane-only partition of A: the others elect and
        // commit a term the deposed primary never observes.
        c.down.push(a);
        let l2 = c.settle(400);

        // The new leader dies; A heals. Live set {A, T} is a quorum.
        let t = *c.nodes.keys().find(|id| **id != a && **id != l2).unwrap();
        c.down.push(l2);
        c.down.retain(|id| *id != a);

        // Journal tips frozen with the deposed primary ahead (the data
        // plane stalled when it halted).
        let tips = c
            .nodes
            .keys()
            .map(|id| {
                let seq = if *id == a { 1_000 } else { 900 };
                (
                    *id,
                    JournalTip {
                        epoch: 5,
                        last_sequence: seq,
                    },
                )
            })
            .collect();
        c.tips = Some(tips);

        // A froze mid-tenure while partitioned, so it resumes as a
        // stale term-1 leader; let it hear T's newer term and step
        // down so the deadlock actually forms before settling.
        for _ in 0..25 {
            c.step_all();
        }
        assert_ne!(
            c.nodes[&a].role(),
            StateRole::Leader,
            "deposed primary must step down on contact"
        );

        let leader = c.settle(600);
        assert_eq!(leader, t, "log-ahead survivor must win via the escape");
    }

    #[test]
    fn lost_snapshot_wedges_until_report_failure() {
        let mut c = Cluster::new(&[1, 2, 3]);
        for node in c.nodes.values_mut() {
            node.set_log_retention(0);
        }
        let first = c.settle(200);

        // Depose the first leader so a second no-op raises the applied
        // index past the (shrunk) retention window and the survivors
        // compact their logs.
        c.down.push(first);
        let leader = c.settle(400);

        // Wipe the deposed node's state: it can now only catch up via
        // a leader snapshot (the log entries it needs are compacted).
        let victim = first;
        c.nodes.remove(&victim);
        let dir = tempfile::tempdir().unwrap();
        c.nodes
            .insert(victim, ControlNode::open(victim, dir.path(), &[]).unwrap());
        c.dirs.insert(victim, dir);
        c.down.retain(|id| *id != victim);

        // Lose the MsgSnapshot in transit: the leader sends exactly one
        // and then wedges — raft never retries a pending snapshot on
        // its own, even after the transport heals.
        c.drop_snapshots = true;
        for _ in 0..80 {
            c.step_all();
        }
        assert_eq!(c.snapshots_dropped, 1, "no self-retry of a lost snapshot");
        c.drop_snapshots = false;
        for _ in 0..80 {
            c.step_all();
        }
        assert_eq!(
            c.nodes[&victim].last_log_index(),
            0,
            "victim must still be empty: this is the wedge"
        );

        // The transport reports the loss — the leader re-probes, sends
        // a fresh snapshot, and the victim finally catches up.
        c.nodes
            .get_mut(&leader)
            .unwrap()
            .report_snapshot(victim, raft::SnapshotStatus::Failure);
        for _ in 0..80 {
            c.step_all();
        }
        assert!(
            c.nodes[&victim].last_log_index() > 0,
            "victim must catch up once the failure is reported"
        );
        assert_eq!(c.nodes[&victim].leader_id(), Some(leader));
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
