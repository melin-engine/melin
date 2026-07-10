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

use raft::eraftpb::{
    ConfChange, ConfChangeType, ConfChangeV2, ConfState, Entry, EntryType, Message,
};
use raft::{Config, RawNode, StateRole};
use tracing::{debug, error, info};

use crate::registry::{MemberRecord, Registry};
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
    /// The applied membership registry — decoded once from the
    /// persisted state at open, kept current by `drain_ready`'s apply
    /// path. The storage owns its serialized durability.
    registry: Registry,
}

/// What a drained ready handed to the caller: messages to put on the
/// wire, plus whether the applied membership registry changed (new or
/// moved records, or a snapshot install) — the driver re-derives its
/// dial targets from [`ControlNode::registry`] when it did.
#[derive(Debug, Default)]
pub struct Drained {
    /// Peer messages, in send order. Every message in here is already
    /// safe to send: `drain_ready` only surfaces them after the state
    /// they depend on has been fsynced.
    pub messages: Vec<Message>,
    /// The registry changed while draining.
    pub registry_changed: bool,
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

        // Seed the applied index from the storage's own record of how
        // far applies reached — NOT from the commit index. The two
        // differ only across a crash between a commit's persist and
        // its applies' persist; seeding from `applied_index` makes
        // raft re-deliver exactly that `(applied, commit]` tail, which
        // re-applies safely (registry applies are idempotent upserts,
        // and a conf change whose ConfState persisted also persisted
        // its applied index — atomically, same file rewrite — so it is
        // never re-delivered).
        let applied = storage.applied_index();
        let registry = Registry::decode(storage.app_state()).map_err(|e| {
            io::Error::other(format!(
                "persisted registry state is undecodable ({e}) — refusing to start raft"
            ))
        })?;

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
        Ok(Self { raw, registry })
    }

    /// Propose `record` into the raft log. Works from any node that
    /// knows a leader: raft forwards a follower's proposal to the
    /// leader over the existing peer links. Returns `false` when the
    /// proposal was dropped (no leader known yet) — and `true` is only
    /// "accepted for forwarding/append", never "committed": proposals
    /// can still be lost to leader churn, so callers re-propose until
    /// the record shows up in [`Self::registry`].
    pub fn propose_member(&mut self, record: &MemberRecord) -> bool {
        match self.raw.propose(Vec::new(), record.encode()) {
            Ok(()) => true,
            Err(e) => {
                // Expected while leaderless (ProposalDropped) — the
                // caller's announce loop retries.
                debug!(error = %e, node_id = record.node_id, "member record proposal dropped");
                false
            }
        }
    }

    /// The applied membership registry.
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// The current voter set, ascending. Read from raft's applied
    /// configuration (kept in lockstep with the persisted `ConfState`
    /// by [`Self::absorb_conf_change`]). Callers poll this to observe
    /// whether a proposed voter change has committed and applied —
    /// proposals are ack-less, exactly like `propose_member`.
    pub fn voters(&self) -> Vec<u64> {
        // `to_conf_state().voters` is the incoming voter set for a
        // simple (non-joint) config, which is all we ever run — we only
        // ever propose single-node changes. Sorted for a stable,
        // comparable result.
        let mut voters = self.raw.raft.prs().conf().to_conf_state().voters;
        voters.sort_unstable();
        voters
    }

    /// Propose adding `node_id` to the voter set. Same contract as
    /// [`Self::propose_member`]: `true` means "accepted for
    /// append/forwarding", never "committed" — the caller re-proposes
    /// and polls [`Self::voters`] until it reflects the change. The
    /// caller is responsible for the safety rails (a seed `MemberRecord`
    /// must already be proposed so peers can dial the joiner, the key
    /// must be authorized, etc.); this is the raw mechanism.
    pub fn propose_add_voter(&mut self, node_id: u64) -> bool {
        self.propose_voter_change(node_id, ConfChangeType::AddNode)
    }

    /// Propose removing `node_id` from the voter set. Same ack-less
    /// contract as [`Self::propose_add_voter`].
    pub fn propose_remove_voter(&mut self, node_id: u64) -> bool {
        self.propose_voter_change(node_id, ConfChangeType::RemoveNode)
    }

    fn propose_voter_change(&mut self, node_id: u64, change: ConfChangeType) -> bool {
        let mut cc = ConfChange::default();
        cc.set_change_type(change);
        cc.node_id = node_id;
        match self.raw.propose_conf_change(Vec::new(), cc) {
            Ok(()) => true,
            Err(e) => {
                // Expected while leaderless or when a conf change is
                // already in flight (raft admits only one) — the driver
                // retries. Same level/rationale as `propose_member`.
                debug!(error = %e, node_id, ?change, "voter conf-change proposal dropped");
                false
            }
        }
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
            // The snapshot's data replaced the persisted application
            // state wholesale; rebuild the in-memory registry to match
            // before any post-snapshot entries apply on top of it.
            self.registry = Registry::decode(self.raw.store().app_state())
                .map_err(|e| io::Error::other(format!("snapshot registry undecodable: {e}")))?;
            out.registry_changed = true;
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

        // Applies whose commit was persisted by an *earlier* ready may
        // have staged progress without any persist in this drain —
        // make them durable before returning (no-op when clean).
        self.raw.mut_store().flush_if_dirty()?;

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
        let Some(target) = applied.checked_sub(LOG_RETENTION) else {
            return Ok(()); // fewer than a window's worth applied yet
        };
        self.raw.mut_store().compact(target)
    }

    /// Apply a batch of committed entries: registry payloads mutate
    /// the in-memory registry and stage the serialized state; conf
    /// changes mutate raft + durable membership. Every entry —
    /// including a fresh leader's empty no-op — stages its index as
    /// applied, so boot-time re-delivery resumes exactly where applies
    /// stopped (see the module docs on `applied_index`).
    fn apply_committed(&mut self, entries: Vec<Entry>, out: &mut Drained) -> io::Result<()> {
        for entry in entries {
            match entry.entry_type() {
                EntryType::EntryNormal => {
                    let state = if !entry.data.is_empty() && self.registry.apply(&entry.data) {
                        out.registry_changed = true;
                        Some(self.registry.encode())
                    } else {
                        None
                    };
                    self.raw.mut_store().stage_applied(entry.index, state);
                }
                EntryType::EntryConfChange => {
                    let cc: ConfChange = prost_decode(&entry.data)?;
                    let removed: Vec<u64> = (cc.change_type() == ConfChangeType::RemoveNode)
                        .then_some(cc.node_id)
                        .into_iter()
                        .collect();
                    let before = self.voters().len();
                    let applied = self.raw.apply_conf_change(&cc);
                    self.absorb_conf_change(entry.index, applied, before, &removed, out)?;
                }
                EntryType::EntryConfChangeV2 => {
                    let cc: ConfChangeV2 = prost_decode(&entry.data)?;
                    let removed: Vec<u64> = cc
                        .changes
                        .iter()
                        .filter(|c| c.change_type() == ConfChangeType::RemoveNode)
                        .map(|c| c.node_id)
                        .collect();
                    let before = self.voters().len();
                    let applied = self.raw.apply_conf_change(&cc);
                    self.absorb_conf_change(entry.index, applied, before, &removed, out)?;
                }
            }
        }
        Ok(())
    }

    /// Land the outcome of applying a committed conf change entry.
    ///
    /// On success the entry is staged applied *before* the `ConfState`
    /// persists, so both land in the same whole-file rewrite — a
    /// re-delivered conf change then never double-applies.
    ///
    /// On an `apply_conf_change` **rejection** we skip rather than halt.
    /// A committed entry was already durably agreed by a quorum, so the
    /// rejection means our pre-proposal validation (the driver's safety
    /// rails) missed a case — a bug on our side, hence `error!`. Every
    /// node applies the same committed entry and would reject it
    /// identically, so returning the error here would brick the control
    /// plane on *every* node, deterministically and permanently. Staging
    /// the entry applied (without touching `ConfState`) lets all nodes
    /// converge past it; the driver observes `voters()` unchanged and
    /// reports the change as failed. A storage error from the persist
    /// itself is a genuine local I/O failure and still propagates.
    fn absorb_conf_change(
        &mut self,
        index: u64,
        applied: raft::Result<ConfState>,
        voters_before: usize,
        removed: &[u64],
        out: &mut Drained,
    ) -> io::Result<()> {
        match applied {
            Ok(cs) => {
                // A committed `RemoveNode` also prunes the departed node's
                // directory record, deterministically on every node. This
                // reclaims an orphaned seed too: a `RAFT-ADD-VOTER` whose
                // record committed but whose `AddNode` never did leaves a
                // record with no voter — re-issuing `RAFT-REMOVE-VOTER`
                // proposes a `RemoveNode` that no-ops on `ConfState` yet
                // still lands here to drop the record. Staged into the same
                // rewrite as the `ConfState` below, so the prune is atomic.
                let mut pruned = false;
                for &id in removed {
                    pruned |= self.registry.remove(id);
                }
                let state = if pruned {
                    out.registry_changed = true;
                    Some(self.registry.encode())
                } else {
                    None
                };
                self.raw.mut_store().stage_applied(index, state);
                if cs.voters.len() > voters_before {
                    // A grown voter set means a fresh voter whose empty
                    // bootstrap `ConfState` carries no genesis membership;
                    // compact to this entry's index — atomically with the
                    // `ConfState` persist — so its match position (0)
                    // falls below the log start and the leader ships it a
                    // snapshot instead of a log replay that would leave it
                    // a lone voter. Tying the trim to this persist keeps
                    // the guarantee crash-safe (see the storage method).
                    self.raw.mut_store().set_conf_state_compacting(cs, index)?;
                } else {
                    self.raw.mut_store().set_conf_state(cs)?;
                }
            }
            Err(e) => {
                error!(index, error = %e, "committed conf change rejected on apply — skipping to keep the control plane live");
                self.raw.mut_store().stage_applied(index, None);
            }
        }
        Ok(())
    }

    /// This node's id.
    pub fn id(&self) -> u64 {
        self.raw.raft.id
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

        /// Open a fresh node with an **empty** voter bootstrap (the
        /// `--raft-join` pattern) and insert it. It stays a passive
        /// follower — raft accepts an empty `ConfState` and waits for
        /// the leader's `AddNode` + appends to bring it in.
        fn add_joiner(&mut self, id: u64) {
            let dir = tempfile::tempdir().unwrap();
            let node = ControlNode::open(id, dir.path(), &[]).unwrap();
            self.nodes.insert(id, node);
            self.dirs.insert(id, dir);
        }

        /// Step until every **live** node whose id is in `expected`
        /// reports exactly `expected` as its voter set. Nodes outside
        /// `expected` (e.g. a just-removed voter that hasn't learned it
        /// yet) and partitioned nodes (a dead voter still in the config)
        /// are ignored — their view is allowed to lag or diverge.
        fn settle_voters(&mut self, expected: &[u64], max: usize) {
            for _ in 0..max {
                self.step_all();
                if expected.iter().all(|id| {
                    self.down.contains(id)
                        || self.nodes.get(id).is_some_and(|n| n.voters() == expected)
                }) {
                    return;
                }
            }
            let got: Vec<(u64, Vec<u64>)> =
                self.nodes.iter().map(|(id, n)| (*id, n.voters())).collect();
            panic!("voters did not converge to {expected:?} within {max} rounds; got {got:?}");
        }

        /// Step until every node in `ids` has `rec` in its registry.
        /// Like `settle_record` but scoped to a subset — used while a
        /// joiner is not yet a voter and so not yet receiving the log.
        fn settle_record_among(&mut self, rec: &MemberRecord, ids: &[u64], max: usize) {
            for _ in 0..max {
                self.step_all();
                if ids
                    .iter()
                    .all(|id| self.nodes[id].registry().get(rec.node_id) == Some(rec))
                {
                    return;
                }
            }
            panic!(
                "record {} did not replicate to {ids:?} within {max} rounds",
                rec.node_id
            );
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

    fn record(id: u64) -> MemberRecord {
        MemberRecord {
            node_id: id,
            raft_addr: format!("127.0.0.1:{}", 7000 + id).parse().expect("addr"),
            replication_addr: Some(format!("10.0.0.{}:9877", id % 250).parse().expect("addr")),
            order_entry_addr: None,
            public_key: [id as u8; 32],
        }
    }

    impl Cluster {
        /// Step until every live node's registry contains `rec`, up to
        /// `max` rounds.
        fn settle_record(&mut self, rec: &MemberRecord, max: usize) {
            for _ in 0..max {
                self.step_all();
                if self
                    .nodes
                    .iter()
                    .filter(|(id, _)| !self.down.contains(id))
                    .all(|(_, n)| n.registry().get(rec.node_id) == Some(rec))
                {
                    return;
                }
            }
            panic!(
                "record {} did not replicate within {max} rounds",
                rec.node_id
            );
        }
    }

    #[test]
    fn committed_conf_change_that_fails_to_apply_does_not_halt() {
        use raft::Storage;
        use raft::eraftpb::ConfChangeType;

        // Single node: it elects itself and commits instantly, so a
        // proposed conf change reaches apply within a couple of steps.
        let mut c = Cluster::new(&[1]);
        c.settle(200);

        // Propose removing the sole voter. raft happily appends and
        // commits it — the "removed all voters" rejection only fires at
        // apply time, inside our apply_committed. Before the hardening
        // that Err propagated out of drain_ready and bricked the node
        // (and, cluster-wide, every node deterministically).
        {
            let node = c.nodes.get_mut(&1).unwrap();
            let mut cc = ConfChange::default();
            cc.set_change_type(ConfChangeType::RemoveNode);
            cc.node_id = 1;
            node.raw
                .propose_conf_change(Vec::new(), cc)
                .expect("leader accepts the conf-change proposal");
        }

        // With the old code, step_all's `drain_ready().unwrap()` would
        // panic here. The hardening absorbs the rejection, so this runs
        // clean.
        for _ in 0..20 {
            c.step_all();
        }

        // The rejected change left ConfState untouched...
        let node = c.nodes.get(&1).unwrap();
        let voters = node.raw.store().initial_state().unwrap().conf_state.voters;
        assert_eq!(
            voters,
            vec![1],
            "a rejected conf change must not alter ConfState"
        );
        assert_eq!(node.role(), StateRole::Leader, "node must still lead");

        // ...and the node still commits and applies normal entries,
        // proving the control plane is live rather than wedged.
        let rec = record(7);
        assert!(c.nodes.get_mut(&1).unwrap().propose_member(&rec));
        c.settle_record(&rec, 20);
    }

    #[test]
    fn joiner_catches_up_via_snapshot() {
        let mut c = Cluster::new(&[1, 2, 3]);
        let leader = c.settle(200);

        // Push enough records to compact the early log away before node
        // 4 is wired in, so it can only join via snapshot (which must
        // carry both ConfState and the registry).
        let count = LOG_RETENTION + 40;
        for i in 0..count {
            let rec = record(100 + i);
            assert!(c.nodes.get_mut(&leader).unwrap().propose_member(&rec));
            c.step_all();
        }
        let last = record(100 + count - 1);
        c.settle_record_among(&last, &[1, 2, 3], 200);

        // Now admit node 4. Its log is empty and the entries it would
        // need are compacted, so the leader must ship a snapshot.
        c.add_joiner(4);
        let rec4 = record(4);
        assert!(c.nodes.get_mut(&leader).unwrap().propose_member(&rec4));
        c.settle_record_among(&rec4, &[1, 2, 3], 100);
        assert!(c.nodes.get_mut(&leader).unwrap().propose_add_voter(4));
        c.settle_voters(&[1, 2, 3, 4], 400);
        // `voters()` reflects raft's in-memory config, which a snapshot
        // restore updates during `step()` — before the snapshot has
        // been drained and persisted. A few more rounds let node 4
        // actually drain it and rebuild its registry from the snapshot
        // `app_state`.
        for _ in 0..10 {
            c.step_all();
        }

        // The snapshot carried the whole registry, not just post-join
        // entries.
        assert_eq!(
            c.nodes[&4].registry(),
            c.nodes[&leader].registry(),
            "joiner must converge on the full registry via snapshot"
        );
        assert_eq!(c.nodes[&4].registry().get(last.node_id), Some(&last));
    }

    #[test]
    fn add_voter_grows_the_cluster_and_the_new_voter_votes() {
        let mut c = Cluster::new(&[1, 2, 3]);
        let leader = c.settle(200);

        // Admit node 4 through the two-stage flow (seed record → AddNode).
        c.add_joiner(4);
        let rec4 = record(4);
        assert!(c.nodes.get_mut(&leader).unwrap().propose_member(&rec4));
        c.settle_record_among(&rec4, &[1, 2, 3], 100);
        assert!(c.nodes.get_mut(&leader).unwrap().propose_add_voter(4));
        c.settle_voters(&[1, 2, 3, 4], 400);
        // Let node 4 fully drain the log it now receives as a voter.
        for _ in 0..10 {
            c.step_all();
        }

        // Kill the old leader. Three voters remain (two originals + the
        // joiner), and a 4-voter quorum is 3 — so the survivors can only
        // elect if node 4 grants its vote. A successful election proves
        // the freshly-added voter actually participates in consensus.
        c.down.push(leader);
        let second = c.settle(600);
        assert_ne!(second, leader, "the dead leader must not lead");
        assert!(
            [1, 2, 3, 4].contains(&second) && second != leader,
            "a live voter from the grown set must win, got {second}"
        );
    }

    #[test]
    fn add_voter_forwarded_from_a_follower_commits_cluster_wide() {
        // The operator may target *any* node's admin endpoint, not just
        // the leader (docs/replication.md, "Changing the cluster
        // membership at runtime"). That guarantee rests on raft forwarding
        // a follower's conf-change proposal to the leader, exactly as it
        // forwards a normal MsgPropose. Drive the whole two-stage add from
        // a follower and assert the grown set commits — including on the
        // leader, the only node that can actually append the change.
        let mut c = Cluster::new(&[1, 2, 3]);
        let leader = c.settle(200);
        let follower = *c.nodes.keys().find(|id| **id != leader).unwrap();

        c.add_joiner(4);
        let rec4 = record(4);
        // Stage 1: seed record proposed on the follower, forwarded up.
        assert!(
            c.nodes.get_mut(&follower).unwrap().propose_member(&rec4),
            "a follower must accept a record proposal for forwarding"
        );
        c.settle_record_among(&rec4, &[1, 2, 3], 100);
        // Stage 2: AddNode conf-change proposed on the same follower.
        assert!(
            c.nodes.get_mut(&follower).unwrap().propose_add_voter(4),
            "a follower must accept a conf-change proposal for forwarding"
        );
        c.settle_voters(&[1, 2, 3, 4], 400);

        // The grow could only have been appended by the leader, yet it was
        // initiated on a follower — forwarding worked end to end. Adding a
        // voter must not have disturbed leadership.
        assert_eq!(
            c.leader(),
            Some(leader),
            "the follower-initiated grow must not move leadership"
        );
    }

    #[test]
    fn added_voter_and_config_survive_restart() {
        let mut c = Cluster::new(&[1, 2, 3]);
        let leader = c.settle(200);

        // Grow to four voters (seed record → AddNode; the joiner catches
        // up via the forced snapshot + compaction on the grow path).
        c.add_joiner(4);
        let rec4 = record(4);
        assert!(c.nodes.get_mut(&leader).unwrap().propose_member(&rec4));
        c.settle_record_among(&rec4, &[1, 2, 3], 100);
        assert!(c.nodes.get_mut(&leader).unwrap().propose_add_voter(4));
        c.settle_voters(&[1, 2, 3, 4], 400);
        for _ in 0..10 {
            c.step_all();
        }

        // Crash-reopen an existing voter *and* the freshly-added node 4
        // from their own directories. The grown ConfState and the
        // registry must return from persisted state — pinning that the
        // grow path lands the applied index, the ConfState, and the
        // compaction in one atomic rewrite (a split would let the
        // committed AddNode re-deliver or lose node 4's membership).
        for id in [leader, 4] {
            let dir = c.dirs[&id].path().to_path_buf();
            c.nodes.remove(&id);
            let reopened = ControlNode::open(id, &dir, &[]).unwrap();
            assert_eq!(
                reopened.voters(),
                vec![1, 2, 3, 4],
                "node {id} must reopen with the grown voter set"
            );
            assert!(
                reopened.registry().get(4).is_some(),
                "node {id} must retain node 4's record across restart"
            );
            c.nodes.insert(id, reopened);
        }
    }

    #[test]
    fn remove_voter_shrinks_the_cluster() {
        let mut c = Cluster::new(&[1, 2, 3]);
        let leader = c.settle(200);
        // Remove a follower so leadership is undisturbed by the change.
        let victim = *c.nodes.keys().find(|id| **id != leader).unwrap();

        assert!(
            c.nodes
                .get_mut(&leader)
                .unwrap()
                .propose_remove_voter(victim)
        );
        let remaining: Vec<u64> = [1, 2, 3].into_iter().filter(|id| *id != victim).collect();
        c.settle_voters(&remaining, 200);

        // The removed node is no longer part of consensus; the remaining
        // pair keeps a single stable leader (a 2-voter quorum is 2, so
        // this is the strongest liveness assertion available).
        assert!(
            c.leader().is_some(),
            "the shrunk 2-voter set must keep a leader"
        );
    }

    #[test]
    fn duplicate_add_is_harmless() {
        let mut c = Cluster::new(&[1, 2, 3]);
        let leader = c.settle(200);

        // Re-adding an existing voter is a no-op in raft-rs (make_voter
        // is idempotent), so the apply neither errors nor changes the
        // set — verifies pre-implementation assumption 4.
        assert!(c.nodes.get_mut(&leader).unwrap().propose_add_voter(2));
        for _ in 0..50 {
            c.step_all();
        }
        c.settle_voters(&[1, 2, 3], 50);
        assert_eq!(c.nodes[&leader].voters(), vec![1, 2, 3]);
    }

    #[test]
    fn remove_absent_is_harmless() {
        let mut c = Cluster::new(&[1, 2, 3]);
        let leader = c.settle(200);

        // Removing an id that was never a voter is a no-op (remove of an
        // absent id changes nothing), not an error.
        assert!(c.nodes.get_mut(&leader).unwrap().propose_remove_voter(99));
        for _ in 0..50 {
            c.step_all();
        }
        c.settle_voters(&[1, 2, 3], 50);
        assert_eq!(c.nodes[&leader].voters(), vec![1, 2, 3]);
    }

    #[test]
    fn conf_change_survives_restart() {
        let mut c = Cluster::new(&[1, 2, 3]);
        let leader = c.settle(200);
        let victim = *c.nodes.keys().find(|id| **id != leader).unwrap();

        assert!(
            c.nodes
                .get_mut(&leader)
                .unwrap()
                .propose_remove_voter(victim)
        );
        let remaining: Vec<u64> = [1, 2, 3].into_iter().filter(|id| *id != victim).collect();
        c.settle_voters(&remaining, 200);

        // Crash-reopen a surviving voter from its own directory: the
        // applied ConfState must come back from persisted state, which
        // pins the stage_applied-before-set_conf_state atomicity — the
        // conf change and its applied index land in one file rewrite.
        let survivor = *remaining.iter().find(|id| **id != leader).unwrap();
        let dir = c.dirs[&survivor].path().to_path_buf();
        c.nodes.remove(&survivor);
        let reopened = ControlNode::open(survivor, &dir, &[]).unwrap();
        assert_eq!(
            reopened.voters(),
            remaining,
            "a removed voter must stay removed across a crash-reopen"
        );
    }

    #[test]
    fn remove_prunes_the_departed_member_record() {
        let mut c = Cluster::new(&[1, 2, 3]);
        let leader = c.settle(200);
        let victim = *c.nodes.keys().find(|id| **id != leader).unwrap();

        // Seed the victim's directory record and let it replicate.
        let rec = record(victim);
        assert!(c.nodes.get_mut(&leader).unwrap().propose_member(&rec));
        c.settle_record(&rec, 200);

        // Removing the voter must also prune its record cluster-wide, so
        // survivors stop dialing a decommissioned node.
        assert!(
            c.nodes
                .get_mut(&leader)
                .unwrap()
                .propose_remove_voter(victim)
        );
        let remaining: Vec<u64> = [1, 2, 3].into_iter().filter(|id| *id != victim).collect();
        c.settle_voters(&remaining, 200);
        for _ in 0..50 {
            c.step_all();
        }
        for n in c.nodes.values() {
            assert!(
                n.registry().get(victim).is_none(),
                "the removed voter's record must be pruned on {}",
                n.id()
            );
        }
    }

    #[test]
    fn orphaned_record_is_reclaimed_by_remove() {
        let mut c = Cluster::new(&[1, 2, 3]);
        let leader = c.settle(200);

        // A record for node 4 with no matching voter — the orphan an
        // interrupted `RAFT-ADD-VOTER` (seed committed, `AddNode` did not)
        // would leave behind.
        let orphan = record(4);
        assert!(c.nodes.get_mut(&leader).unwrap().propose_member(&orphan));
        c.settle_record(&orphan, 200);
        assert!(c.nodes[&leader].registry().get(4).is_some());
        assert!(!c.nodes[&leader].voters().contains(&4));

        // `RAFT-REMOVE-VOTER 4`: a `RemoveNode` that no-ops on the voter
        // set but still prunes the orphaned record so it is recoverable.
        assert!(c.nodes.get_mut(&leader).unwrap().propose_remove_voter(4));
        for _ in 0..80 {
            c.step_all();
        }
        for n in c.nodes.values() {
            assert!(
                n.registry().get(4).is_none(),
                "the orphaned record must be reclaimed on {}",
                n.id()
            );
            assert_eq!(n.voters(), vec![1, 2, 3], "the voter set must be untouched");
        }
    }

    #[test]
    fn member_records_replicate_from_leader_and_followers() {
        let mut c = Cluster::new(&[1, 2, 3]);
        let leader = c.settle(200);

        // Leader proposes its own record.
        let leader_rec = record(leader);
        assert!(
            c.nodes
                .get_mut(&leader)
                .unwrap()
                .propose_member(&leader_rec)
        );
        c.settle_record(&leader_rec, 100);

        // A follower's proposal is forwarded to the leader by raft
        // itself — no application-level forwarding machinery.
        let follower = *c.nodes.keys().find(|id| **id != leader).unwrap();
        let follower_rec = record(follower);
        assert!(
            c.nodes
                .get_mut(&follower)
                .unwrap()
                .propose_member(&follower_rec)
        );
        c.settle_record(&follower_rec, 100);

        // A moved address (changed record, same id) replaces the old one.
        let moved = MemberRecord {
            raft_addr: "127.0.0.1:9999".parse().expect("addr"),
            ..follower_rec
        };
        assert!(c.nodes.get_mut(&follower).unwrap().propose_member(&moved));
        c.settle_record(&moved, 100);
    }

    #[test]
    fn registry_survives_restart() {
        let mut c = Cluster::new(&[1, 2, 3]);
        let leader = c.settle(200);
        let rec = record(leader);
        assert!(c.nodes.get_mut(&leader).unwrap().propose_member(&rec));
        c.settle_record(&rec, 100);

        // Crash-reopen a follower from its own directory: the registry
        // must come back from the persisted state, before any peer
        // traffic.
        let follower = *c.nodes.keys().find(|id| **id != leader).unwrap();
        let dir = c.dirs[&follower].path().to_path_buf();
        c.nodes.remove(&follower);
        let reopened = ControlNode::open(follower, &dir, &[]).unwrap();
        assert_eq!(reopened.registry().get(rec.node_id), Some(&rec));
    }

    /// A node that falls behind past the log-retention window catches
    /// up via a raft snapshot — whose data must carry the registry, or
    /// the compacted-away records would be silently lost on that node.
    #[test]
    fn snapshot_carries_registry_to_lagging_node() {
        let mut c = Cluster::new(&[1, 2, 3]);
        let leader = c.settle(200);

        let lagger = *c.nodes.keys().find(|id| **id != leader).unwrap();
        c.down.push(lagger);

        // Push enough records through to compact the early log away
        // (LOG_RETENTION entries behind applied are discarded).
        let count = LOG_RETENTION + 40;
        for i in 0..count {
            let rec = record(100 + i);
            assert!(c.nodes.get_mut(&leader).unwrap().propose_member(&rec));
            // Step a couple of rounds per proposal so the log advances
            // steadily rather than in one giant append.
            c.step_all();
        }
        let last = record(100 + count - 1);
        c.settle_record(&last, 200);

        // The lagging node rejoins and must converge — necessarily via
        // snapshot: the entries it missed are compacted on the leader.
        c.down.retain(|id| *id != lagger);
        for _ in 0..400 {
            c.step_all();
            if c.nodes[&lagger].registry().len() == count as usize {
                break;
            }
        }
        let lagger_reg = c.nodes[&lagger].registry();
        let leader_reg = c.nodes[&leader].registry();
        assert_eq!(
            lagger_reg, leader_reg,
            "lagging node must converge on the full registry"
        );
        assert_eq!(lagger_reg.get(last.node_id), Some(&last));
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
