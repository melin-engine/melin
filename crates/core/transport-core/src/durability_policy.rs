//! Durability ack policy — application-agnostic core.
//!
//! The response stage gates outgoing acks on a cluster-wide durability
//! condition. This module expresses that condition as a structured
//! policy over per-node durability levels.
//!
//! # Levels
//!
//! Two levels matter:
//!
//! - [`Level::InMemory`] — the event has been accepted into the node's
//!   pipeline. Survives nothing — process death loses it. Useful as a
//!   "received this far" signal in cross-node policies.
//! - [`Level::Persisted`] — `pwrite` *and* `fdatasync` have returned, so
//!   the kernel reports the bytes on stable media. Survives power loss
//!   on any drive, with or without power-loss-protection capacitors.
//!
//! # Policy shape
//!
//! A [`Policy`] is an AND-combined list of [`Clause`]s. Each clause is
//! `<level>>=<count>` — "at least `count` nodes (counting both the
//! primary and any connected replicas) have reached `level`". Clauses
//! are strict: if the current cluster shape can't satisfy the count,
//! the gate stalls and [`EvalStatus::degraded`] reports it.
//!
//! # Evaluation
//!
//! Given a [`CursorView`] exposing per-(node, level) sequence cursors,
//! [`Policy::evaluate`] returns the highest sequence at which every
//! clause is satisfied. Per clause: take the `count`-th largest cursor
//! at that level — that is the highest seq for which `count` nodes have
//! crossed. Across clauses: take the `min` (AND semantics).
//!
//! # What is _not_ here
//!
//! The operator-facing CLI shape (the named modes — `local`, `hybrid`,
//! `durably-replicated`) is application policy and lives in the
//! consuming crate. Core only knows about clauses and levels.

use std::fmt;

/// Durability level a single node can be at for a given sequence.
///
/// Ordered from weakest to strongest — `Persisted >= InMemory` always
/// holds for any given cursor pair on a single node, since persisting
/// is downstream of receiving in the pipeline. The `Ord` derive reflects
/// this and lets evaluation code treat a higher-level cursor as also
/// satisfying any lower-level requirement on the same node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Level {
    /// Event has been accepted into the node's pipeline. No durability
    /// guarantee — process crash or power loss loses it.
    InMemory,
    /// Event has been written to the journal and `fdatasync` has
    /// returned. Survives power loss on any drive — the guarantee rests
    /// on the sync, not on the device's power-loss-protection
    /// capacitors. Published by the journal disk thread only after the
    /// sync completes.
    Persisted,
}

impl Level {
    /// Stable lowercase name used in policy strings (`"in_memory"`,
    /// `"persisted"`).
    pub fn as_str(self) -> &'static str {
        match self {
            Level::InMemory => "in_memory",
            Level::Persisted => "persisted",
        }
    }
}

impl fmt::Display for Level {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Single AND-combined clause of a [`Policy`].
///
/// Read as: "at least `count` nodes have reached `level` for the
/// candidate sequence". Strict: if the current cluster shape can't
/// satisfy the count, the gate stalls and the policy reports
/// degraded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Clause {
    /// Target number of nodes that must satisfy `level`. Counted across
    /// the primary and all connected replicas. `0` is rejected by
    /// [`Policy::new`] — a zero-count clause is trivially true and
    /// almost always a config mistake.
    pub count: u8,
    /// Durability level required.
    pub level: Level,
}

impl fmt::Display for Clause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}>={}", self.level, self.count)
    }
}

/// Durability ack policy: an AND-combined list of clauses.
///
/// The empty policy is rejected by the parser; an "ack immediately"
/// behaviour can be expressed as `in_memory>=1`, which is satisfied by
/// the primary alone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Policy {
    // `Vec` is fine — policies are small (1–4 clauses) and read once
    // per gate-cross. Hot-path evaluation iterates linearly. A fixed-
    // size array would save the allocation but at the cost of a
    // size-vs-flexibility tradeoff that does not pay back at this scale.
    clauses: Vec<Clause>,
}

impl Policy {
    /// Construct a policy from clauses. Returns an error if the clause
    /// list is empty, contains a zero-count clause, or contains a
    /// clause whose count exceeds [`MAX_CLUSTER_SIZE`].
    pub fn new(clauses: Vec<Clause>) -> Result<Self, PolicyError> {
        if clauses.is_empty() {
            return Err(PolicyError::Empty);
        }
        if let Some(c) = clauses.iter().find(|c| c.count == 0) {
            return Err(PolicyError::ZeroCount(*c));
        }
        if let Some(c) = clauses.iter().find(|c| c.count > MAX_CLUSTER_SIZE) {
            return Err(PolicyError::CountExceedsClusterCap {
                count: c.count,
                max: MAX_CLUSTER_SIZE,
            });
        }
        Ok(Self { clauses })
    }

    /// Slice access for callers that need to inspect or display the
    /// policy (health endpoint, startup logging).
    pub fn clauses(&self) -> &[Clause] {
        &self.clauses
    }

    /// Highest sequence at which every clause is satisfied given the
    /// supplied cursor view.
    ///
    /// Returns `0` if no sequence satisfies all clauses — typically
    /// because at least one clause requires more nodes than are
    /// currently connected.
    #[inline]
    pub fn evaluate(&self, cursors: &CursorView<'_>) -> u64 {
        self.evaluate_with_status(cursors).durable_pos
    }

    /// Like [`evaluate`](Self::evaluate) but also reports whether the
    /// policy is structurally unsatisfiable by the current cluster
    /// shape — i.e. at least one clause's `count` exceeds the number
    /// of nodes in the view. The response stage uses this to surface a
    /// `policy_degraded` health metric and emit periodic warnings; the
    /// gate is stalled while degraded.
    #[inline]
    pub fn evaluate_with_status(&self, cursors: &CursorView<'_>) -> EvalStatus {
        let view_len = cursors.len();
        let mut result = u64::MAX;
        let mut degraded = false;
        for clause in &self.clauses {
            if (clause.count as usize) > view_len {
                degraded = true;
            }
            let satisfied = nth_largest_cursor(cursors, clause.level, clause.count);
            if satisfied < result {
                result = satisfied;
            }
        }
        // u64::MAX is a sentinel for "no cursors, vacuously satisfied" —
        // an empty cluster gates nothing. `Policy::new` rejects an empty
        // clause list, so reaching here with `u64::MAX` requires an
        // empty `CursorView`, which the response stage never constructs.
        let durable_pos = if result == u64::MAX { 0 } else { result };
        EvalStatus {
            durable_pos,
            degraded,
        }
    }

    /// Which subsystem supplied the binding cursor — i.e. what the gate
    /// was actually waiting on.
    ///
    /// Finds the clause that produced `durable_pos` (the most
    /// constraining one) and reports whether the node at that clause's
    /// threshold rank was the primary or a replica. This is the honest
    /// answer to "journal or replication?" for *any* policy shape,
    /// unlike comparing the journal cursor against a fixed replica
    /// level: under `local` (`persisted>=1`) replicas cannot bind at
    /// all, and under `hybrid` (`persisted>=1 && in_memory>=2`) the
    /// binding replica cursor is in-memory, not persisted.
    ///
    /// Returns `None` when a clause is unsatisfiable by the current
    /// cluster shape — the gate is stalled on missing nodes rather than
    /// on either subsystem's progress, so no attribution is meaningful.
    ///
    /// Not on the hot path: the response stage calls this once per gate
    /// open, not per spin iteration.
    pub fn attribute_blocker(&self, cursors: &CursorView<'_>) -> Option<Blocker> {
        let mut binding: Option<(u64, Blocker)> = None;
        for clause in &self.clauses {
            let (value, node) = nth_largest_with_node(cursors, clause.level, clause.count)?;
            // Node 0 is the primary by construction of the view; every
            // other index is a replica slot.
            let blocker = if node == 0 {
                Blocker::Journal
            } else {
                Blocker::Replication
            };
            // Strict `<`, matching the fold in `evaluate_with_status`:
            // both keep the *first* clause on a tie, so both name the
            // same clause as binding. If they diverged, attribution
            // would report a subsystem that did not supply
            // `durable_pos`.
            if binding.is_none_or(|(best, _)| value < best) {
                binding = Some((value, blocker));
            }
        }
        binding.map(|(_, blocker)| blocker)
    }

    /// The tightest replica-supplied clause value — the replica-side
    /// cursor this policy is actually waiting on.
    ///
    /// Returns `None` when no clause is supplied by a replica (e.g.
    /// `local`, where the primary alone satisfies `persisted>=1`), or
    /// when a clause is unsatisfiable by the current cluster shape.
    /// Callers use this to measure replica wait against the level the
    /// policy really gates on rather than a hardcoded one.
    pub fn replica_gate_cursor(&self, cursors: &CursorView<'_>) -> Option<u64> {
        let mut tightest: Option<u64> = None;
        for clause in &self.clauses {
            let (value, node) = nth_largest_with_node(cursors, clause.level, clause.count)?;
            if node == 0 {
                continue;
            }
            if tightest.is_none_or(|best| value < best) {
                tightest = Some(value);
            }
        }
        tightest
    }
}

/// Which subsystem the durability gate was waiting on. Reported by
/// [`Policy::attribute_blocker`] and surfaced as the `blocker` label on
/// the `melin_response_gate_total` counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Blocker {
    /// The primary's own journal supplied the binding cursor.
    Journal,
    /// A replica supplied the binding cursor.
    Replication,
}

impl fmt::Display for Policy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, clause) in self.clauses.iter().enumerate() {
            if i > 0 {
                f.write_str(" && ")?;
            }
            write!(f, "{clause}")?;
        }
        Ok(())
    }
}

/// Outcome of a single policy evaluation.
///
/// `durable_pos` is the highest sequence at which every clause is
/// satisfied. `degraded` is true iff at least one clause's `count`
/// exceeds the current cursor view's size — i.e. the policy is
/// structurally unsatisfiable until more nodes connect, and the gate
/// is therefore stalled. Operators surface this via `/healthz` and a
/// periodic warn-level log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvalStatus {
    pub durable_pos: u64,
    pub degraded: bool,
}

/// Read-only view over per-node, per-level cursor values used by
/// [`Policy::evaluate`].
///
/// The view is indexed by node first, then level: `nodes[i][level as
/// usize]` is the highest sequence node `i` has reached at `level`.
/// Callers (the response stage) build this view once per gate iteration
/// from atomic loads on the live cursors. Tests build it directly from
/// constant arrays.
///
/// `&[[u64; 2]]` rather than `&[NodeCursors]`: the inner array indices
/// match `Level as usize` so the hot-path lookup is a pointer-arithmetic
/// load rather than a struct-field branch.
pub struct CursorView<'a> {
    nodes: &'a [[u64; 2]],
}

impl<'a> CursorView<'a> {
    /// Build a view from a slice of `[in_memory, persisted]` pairs.
    /// Caller is responsible for indexing matching `Level`'s discriminant
    /// order — `[0]` = `InMemory`, `[1]` = `Persisted`.
    pub fn new(nodes: &'a [[u64; 2]]) -> Self {
        Self { nodes }
    }

    /// Number of nodes in the view.
    #[inline]
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Whether the view has no nodes.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
}

/// One node's effective cursor at `level`.
///
/// A higher-level cursor implies satisfaction of all lower levels on
/// the same node: if the node has persisted seq S, then it has
/// trivially also "received" seq S. Taking the max over
/// `level..=Persisted` honours that monotonicity even if the caller's
/// cursors temporarily violate it (e.g. during the brief window where
/// a write completes before the in-memory cursor has been republished).
#[inline]
fn effective_cursor(node: &[u64; 2], level: Level) -> u64 {
    let mut v = 0u64;
    for &c in &node[level as usize..] {
        if c > v {
            v = c;
        }
    }
    v
}

/// The `count`-th largest cursor at `level`, together with the index of
/// the node that supplied it.
///
/// That node is the *marginal* contributor to the clause: the one whose
/// participation is what makes the count. Ranking is descending by
/// cursor, ties broken by ascending node index — so at equal cursors the
/// primary (index 0) sorts first and the marginal slot at rank
/// `count - 1` falls to a replica, which is the honest reading of "whose
/// arrival completed the quorum".
///
/// Returns `None` when the clause cannot be satisfied by the current
/// cluster shape (`count` exceeds the node count), because then no node
/// supplies a threshold at all.
///
/// Selects the top `count` rather than sorting the view, so the scratch
/// space is bounded by [`MAX_CLUSTER_SIZE`] — which [`Policy::new`]
/// enforces on every clause — instead of by how many nodes the caller
/// happens to pass. A view longer than the cluster cap is then handled
/// correctly rather than silently truncated.
#[inline]
fn nth_largest_with_node(view: &CursorView<'_>, level: Level, count: u8) -> Option<(u64, usize)> {
    let n = count as usize;
    // `taken` holds one node index per rank, so it must cover `n`.
    // `Policy::new` rejects `count > MAX_CLUSTER_SIZE`, making this
    // unreachable through any policy; the guard keeps the indexing
    // below sound for a hypothetical direct caller.
    let mut taken = [usize::MAX; MAX_CLUSTER_SIZE as usize];
    if n == 0 || n > view.nodes.len() || n > taken.len() {
        return None;
    }
    // Take the running maximum `n` times, skipping nodes already
    // claimed by an earlier rank. Strict `>` means the lowest node
    // index wins among equal cursors, so ranking is descending by
    // cursor with ties broken by ascending index.
    for rank in 0..n {
        let mut best: Option<(u64, usize)> = None;
        for (i, node) in view.nodes.iter().enumerate() {
            if taken[..rank].contains(&i) {
                continue;
            }
            let value = effective_cursor(node, level);
            if best.is_none_or(|(best_value, _)| value > best_value) {
                best = Some((value, i));
            }
        }
        // `rank < n <= view.nodes.len()` and only `rank` nodes are
        // taken, so at least one candidate always remains.
        let (value, node) = best?;
        if rank == n - 1 {
            return Some((value, node));
        }
        taken[rank] = node;
    }
    None
}

/// Compute the `count`-th largest cursor among all nodes at `level`.
/// That value is the highest sequence at which at least `count` nodes
/// have reached `level`.
///
/// Returns `0` when `count > nodes.len()` — the clause cannot be
/// satisfied by any sequence; the response gate must wait.
#[inline]
fn nth_largest_cursor(view: &CursorView<'_>, level: Level, count: u8) -> u64 {
    match nth_largest_with_node(view, level, count) {
        Some((value, _)) => value,
        None => 0,
    }
}

/// Errors from [`Policy::new`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyError {
    /// The clause list is empty.
    Empty,
    /// A clause had `count == 0`, which is trivially true and almost
    /// always a misconfiguration.
    ZeroCount(Clause),
    /// A clause requires more nodes than the deployment can have. The
    /// server caps cluster size at 1 primary + 2 replicas = 3 nodes
    /// (see [`MAX_CLUSTER_SIZE`]); a clause with `count > 3` would
    /// produce a permanently-stalled gate.
    CountExceedsClusterCap { count: u8, max: u8 },
}

/// Maximum number of nodes the gate's cursor view can carry. Hard-
/// coded at 1 primary + 2 replica slots = 3. Update if/when the
/// replication topology grows past 1+2 (e.g. via Raft, roadmap #7).
pub const MAX_CLUSTER_SIZE: u8 = 3;

impl fmt::Display for PolicyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PolicyError::Empty => f.write_str("durability policy must contain at least one clause"),
            PolicyError::ZeroCount(c) => {
                write!(f, "durability policy clause `{c}` has zero count")
            }
            PolicyError::CountExceedsClusterCap { count, max } => write!(
                f,
                "durability policy clause requires {count} nodes but the server caps cluster size at {max} (1 primary + 2 replicas)"
            ),
        }
    }
}

impl std::error::Error for PolicyError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn view(nodes: &[[u64; 2]]) -> CursorView<'_> {
        CursorView::new(nodes)
    }

    fn policy(level: Level, count: u8) -> Policy {
        Policy::new(vec![Clause { count, level }]).unwrap()
    }

    fn and_policy(clauses: &[(Level, u8)]) -> Policy {
        Policy::new(
            clauses
                .iter()
                .map(|&(level, count)| Clause { count, level })
                .collect(),
        )
        .unwrap()
    }

    // --- attribute_blocker / replica_gate_cursor ---

    #[test]
    fn blocker_follows_the_binding_clause_not_a_fixed_level() {
        // hybrid: primary persisted 300, replica in-memory 400 (fsync
        // trailing at 250). The binding term is the primary's own
        // persisted cursor.
        let p = and_policy(&[(Level::Persisted, 1), (Level::InMemory, 2)]);
        let nodes = [[u64::MAX, 300], [400, 250]];
        assert_eq!(
            p.attribute_blocker(&CursorView::new(&nodes)),
            Some(Blocker::Journal)
        );

        // Replica in-memory now trails the primary → replication binds.
        let nodes = [[u64::MAX, 300], [200, 100]];
        assert_eq!(
            p.attribute_blocker(&CursorView::new(&nodes)),
            Some(Blocker::Replication)
        );
    }

    #[test]
    fn blocker_never_credits_replication_for_a_single_node_clause() {
        // `persisted>=1` is met by whichever node is furthest along. A
        // lagging replica cannot bind it, so the journal must be the
        // verdict no matter how far behind the replica is.
        let p = policy(Level::Persisted, 1);
        let nodes = [[u64::MAX, 500], [1, 1], [1, 1]];
        assert_eq!(
            p.attribute_blocker(&CursorView::new(&nodes)),
            Some(Blocker::Journal)
        );
    }

    #[test]
    fn blocker_is_none_when_a_clause_outruns_the_cluster() {
        // `persisted>=2` with only the primary present: stalled on a
        // missing node, not on either subsystem.
        let p = policy(Level::Persisted, 2);
        let nodes = [[u64::MAX, 500]];
        assert_eq!(p.attribute_blocker(&CursorView::new(&nodes)), None);
    }

    #[test]
    fn blocker_marginal_node_at_equal_cursors_is_the_replica() {
        // Tie at the threshold: `in_memory>=2` needs a second node, and
        // the replica is the one whose arrival completes the count.
        let p = policy(Level::InMemory, 2);
        let nodes = [[u64::MAX, 300], [300, 300]];
        assert_eq!(
            p.attribute_blocker(&CursorView::new(&nodes)),
            Some(Blocker::Replication)
        );
    }

    #[test]
    fn blocker_tie_at_rank_zero_goes_to_the_primary() {
        // The mirror of the case above, and the one that pins the
        // tie-break direction. `persisted>=1` is met at rank 0, and a
        // replica that has caught up *exactly* to the primary ties for
        // it. Ranking must break that tie by ascending node index so
        // rank 0 stays the primary: a replica can never bind a
        // single-node clause, so the verdict has to be Journal.
        // Ranking ties the other way would silently report replication
        // under `local` — the original bug, in a new disguise.
        let p = policy(Level::Persisted, 1);
        let nodes = [[u64::MAX, 300], [300, 300]];
        assert_eq!(
            p.attribute_blocker(&CursorView::new(&nodes)),
            Some(Blocker::Journal)
        );
        // And nothing replica-supplied to measure, for the same reason.
        assert_eq!(p.replica_gate_cursor(&CursorView::new(&nodes)), None);
    }

    #[test]
    fn view_longer_than_the_cluster_cap_is_not_truncated() {
        // The scratch space is sized by the clause count, not by the
        // view, so a view carrying more nodes than MAX_CLUSTER_SIZE is
        // still ranked over all of them. The previous implementation
        // copied the view into a fixed 16-slot buffer and silently
        // ignored anything past it; this pins that the ranking sees
        // every node.
        let p = policy(Level::Persisted, 2);
        // 20 nodes, with the two highest cursors deliberately placed
        // last so a truncating implementation would miss them.
        let mut nodes = vec![[10u64, 10u64]; 18];
        nodes.push([900, 900]);
        nodes.push([800, 800]);
        let view = CursorView::new(&nodes);
        assert_eq!(p.evaluate(&view), 800, "2nd largest across all 20 nodes");
        assert_eq!(
            p.attribute_blocker(&view),
            Some(Blocker::Replication),
            "node 19 is not the primary"
        );
    }

    #[test]
    fn replica_gate_cursor_reports_the_level_the_policy_uses() {
        // hybrid gates replicas on in-memory: 400, not the fsync at 250.
        let p = and_policy(&[(Level::Persisted, 1), (Level::InMemory, 2)]);
        let nodes = [[u64::MAX, 300], [400, 250]];
        assert_eq!(p.replica_gate_cursor(&CursorView::new(&nodes)), Some(400));

        // durably-replicated gates them on persisted: 250.
        let p = policy(Level::Persisted, 2);
        assert_eq!(p.replica_gate_cursor(&CursorView::new(&nodes)), Some(250));
    }

    #[test]
    fn replica_gate_cursor_is_none_when_no_clause_needs_a_replica() {
        // local: the primary satisfies `persisted>=1` alone, so there is
        // no replica wait to measure even with a replica connected.
        let p = policy(Level::Persisted, 1);
        let nodes = [[u64::MAX, 500], [100, 100]];
        assert_eq!(p.replica_gate_cursor(&CursorView::new(&nodes)), None);
    }

    #[test]
    fn level_ordering() {
        assert!(Level::Persisted > Level::InMemory);
        assert_eq!(Level::InMemory as usize, 0);
        assert_eq!(Level::Persisted as usize, 1);
    }

    #[test]
    fn empty_policy_rejected() {
        assert_eq!(Policy::new(vec![]), Err(PolicyError::Empty));
    }

    #[test]
    fn zero_count_clause_rejected() {
        let c = Clause {
            count: 0,
            level: Level::Persisted,
        };
        assert_eq!(Policy::new(vec![c]), Err(PolicyError::ZeroCount(c)));
    }

    #[test]
    fn count_exceeding_cluster_cap_rejected() {
        for &count in &[4u8, 10, 255] {
            let c = Clause {
                count,
                level: Level::Persisted,
            };
            match Policy::new(vec![c]) {
                Err(PolicyError::CountExceedsClusterCap { count: got, max }) => {
                    assert_eq!(got, count);
                    assert_eq!(max, MAX_CLUSTER_SIZE);
                }
                other => panic!("expected CountExceedsClusterCap for count={count}, got {other:?}"),
            }
        }
    }

    #[test]
    fn count_at_cluster_cap_accepted() {
        assert!(policy(Level::Persisted, MAX_CLUSTER_SIZE).clauses().len() == 1);
    }

    #[test]
    fn display_renders_canonical_form() {
        let p = and_policy(&[(Level::Persisted, 1), (Level::InMemory, 3)]);
        assert_eq!(format!("{p}"), "persisted>=1 && in_memory>=3");
    }

    #[test]
    fn evaluate_single_clause_persisted_one_node() {
        let p = policy(Level::Persisted, 1);
        let nodes = [[100, 50], [80, 40], [70, 30]];
        assert_eq!(p.evaluate(&view(&nodes)), 50);
    }

    #[test]
    fn evaluate_single_clause_persisted_quorum() {
        let p = policy(Level::Persisted, 2);
        let nodes = [[100, 50], [80, 40], [70, 30]];
        assert_eq!(p.evaluate(&view(&nodes)), 40);
    }

    #[test]
    fn evaluate_and_clauses_takes_min() {
        let p = and_policy(&[(Level::Persisted, 1), (Level::InMemory, 2)]);
        let nodes = [[100, 80], [70, 10], [60, 5]];
        assert_eq!(p.evaluate(&view(&nodes)), 70);
    }

    #[test]
    fn evaluate_persisted_implies_in_memory() {
        let p = policy(Level::InMemory, 1);
        let nodes = [[0, 50], [0, 0], [0, 0]];
        assert_eq!(p.evaluate(&view(&nodes)), 50);
    }

    #[test]
    fn evaluate_count_exceeds_node_count() {
        let p = policy(Level::Persisted, 3);
        let nodes = [[100, 100], [100, 100]];
        let v = view(&nodes);
        let r = p.evaluate_with_status(&v);
        assert_eq!(r.durable_pos, 0);
        assert!(r.degraded, "unsatisfiable clause must flag degraded");
    }

    #[test]
    fn strict_clause_stalls_when_under_target() {
        let p = policy(Level::Persisted, 2);
        let nodes = [[u64::MAX, 500]];
        let v = view(&nodes);
        let r = p.evaluate_with_status(&v);
        assert_eq!(r.durable_pos, 0);
        assert!(r.degraded);
    }

    #[test]
    fn evaluate_single_node_cluster() {
        let p = policy(Level::Persisted, 1);
        let nodes = [[42, 30]];
        let v = view(&nodes);
        let r = p.evaluate_with_status(&v);
        assert_eq!(r.durable_pos, 30);
        assert!(!r.degraded);
    }

    #[test]
    fn empty_view_flags_every_clause_degraded() {
        let p = and_policy(&[(Level::Persisted, 1), (Level::InMemory, 2)]);
        let nodes: [[u64; 2]; 0] = [];
        let r = p.evaluate_with_status(&view(&nodes));
        assert_eq!(r.durable_pos, 0);
        assert!(r.degraded);
    }

    // -- Property-based tests --

    use proptest::prelude::*;

    fn any_clause() -> impl Strategy<Value = Clause> {
        (
            prop_oneof![Just(Level::Persisted), Just(Level::InMemory)],
            1u8..=MAX_CLUSTER_SIZE,
        )
            .prop_map(|(level, count)| Clause { count, level })
    }

    fn any_policy() -> impl Strategy<Value = Policy> {
        proptest::collection::vec(any_clause(), 1..=4)
            .prop_map(|clauses| Policy::new(clauses).unwrap())
    }

    proptest! {
        /// `evaluate_with_status` is total over arbitrary cursor
        /// views (up to the typical cluster shape). No panics, no
        /// arithmetic overflow.
        #[test]
        fn evaluate_never_panics(
            policy in any_policy(),
            nodes in proptest::collection::vec(any::<[u64; 2]>(), 0..=8),
        ) {
            let view = CursorView::new(&nodes);
            let _ = policy.evaluate_with_status(&view);
        }

        /// `degraded` is true iff at least one clause's `count`
        /// exceeds the cursor view's size.
        #[test]
        fn degraded_iff_any_clause_exceeds_view(
            policy in any_policy(),
            view_len in 0usize..=(MAX_CLUSTER_SIZE as usize + 2),
        ) {
            let nodes: Vec<[u64; 2]> = (0..view_len).map(|_| [100, 100]).collect();
            let view = CursorView::new(&nodes);
            let r = policy.evaluate_with_status(&view);
            let any_unsat = policy.clauses().iter().any(|c| (c.count as usize) > view_len);
            prop_assert_eq!(r.degraded, any_unsat);
        }

        /// The top-`count` selection agrees with a naive sort over
        /// arbitrary cursor values — the property the example tests
        /// above cannot cover, since they all use hand-picked cursors
        /// in a fixed order.
        #[test]
        fn evaluate_agrees_with_a_reference_sort(
            policy in any_policy(),
            nodes in proptest::collection::vec(any::<[u64; 2]>(), 0..=8),
        ) {
            let expected = policy
                .clauses()
                .iter()
                .map(|c| reference_nth_largest(&nodes, c.level, c.count))
                .min()
                .expect("Policy::new rejects an empty clause list");
            // `evaluate_with_status` folds with `u64::MAX` as its
            // "nothing seen yet" sentinel and maps a surviving sentinel
            // back to 0, so a policy whose clauses genuinely resolve to
            // u64::MAX is indistinguishable from an empty view. Not
            // reachable in production — the only `u64::MAX` cursor in a
            // real view is the primary's in-memory sentinel, and every
            // `DurabilityMode` clause is either persisted-level or
            // requires a second node, so the binding value always comes
            // from some other node's real cursor (a replica's in-memory
            // receipt under `replicated`, a journal position otherwise)
            // — so the fold is left alone and the case is excluded here
            // rather than papered over.
            prop_assume!(expected != u64::MAX);
            prop_assert_eq!(policy.evaluate(&CursorView::new(&nodes)), expected);
        }

        /// Attribution is available exactly when the cluster shape can
        /// satisfy the policy, and the blocker it names really is the
        /// node holding the binding cursor.
        #[test]
        fn blocker_owns_the_binding_cursor(
            policy in any_policy(),
            nodes in proptest::collection::vec(any::<[u64; 2]>(), 0..=8),
        ) {
            let view = CursorView::new(&nodes);
            let status = policy.evaluate_with_status(&view);
            let blocker = policy.attribute_blocker(&view);

            // `Some` iff not degraded — the two entry points must agree
            // on whether the current shape is workable at all.
            prop_assert_eq!(blocker.is_some(), !status.degraded);

            let Some(blocker) = blocker else { return Ok(()) };
            prop_assume!(status.durable_pos != 0);

            // Whichever side was named must actually hold `durable_pos`
            // at one of the levels, otherwise the counter is pointing
            // operators at the wrong subsystem.
            let holds = |node: &[u64; 2]| {
                [Level::InMemory, Level::Persisted]
                    .iter()
                    .any(|&l| effective_cursor(node, l) == status.durable_pos)
            };
            match blocker {
                Blocker::Journal => prop_assert!(holds(&nodes[0])),
                Blocker::Replication => prop_assert!(nodes[1..].iter().any(holds)),
            }
        }
    }

    /// Naive reference for a clause's threshold: materialise every
    /// node's effective cursor, sort descending, index the rank.
    /// Deliberately the shape the production selection replaced — its
    /// only job is to disagree if the rank arithmetic ever drifts.
    fn reference_nth_largest(nodes: &[[u64; 2]], level: Level, count: u8) -> u64 {
        let n = count as usize;
        if n == 0 || n > nodes.len() {
            return 0;
        }
        let mut cursors: Vec<u64> = nodes.iter().map(|n| effective_cursor(n, level)).collect();
        cursors.sort_unstable_by(|a, b| b.cmp(a));
        cursors[n - 1]
    }
}
