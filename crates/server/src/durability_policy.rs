//! Configurable durability ack policy.
//!
//! The response stage gates outgoing acks on a cluster-wide durability
//! condition. Today's behaviour is a hardcoded `min`/`max` over two
//! cursors (journal + replication); this module generalises it into a
//! structured policy expressed against per-node durability levels.
//!
//! # Levels
//!
//! Two levels matter on this hardware (`O_DIRECT` + PLP-backed NVMe):
//!
//! - [`Level::InMemory`] — the event has been accepted into the node's
//!   pipeline. Survives nothing — process death loses it. Useful as a
//!   "received this far" signal in cross-node policies.
//! - [`Level::Persisted`] — `pwrite` returned, the bytes are in NVMe
//!   DRAM behind power-loss-protection capacitors. Survives power loss.
//!   No `RWF_DSYNC` round-trip is needed; PLP makes write-and-durable a
//!   single event.
//!
//! # Policy shape
//!
//! A [`Policy`] is an AND-combined list of [`Clause`]s. Each clause is
//! `AtLeast { count, level }` — "at least `count` nodes (counting both
//! the primary and any connected replicas) have reached `level`".
//!
//! # Evaluation
//!
//! Given a [`CursorView`] exposing per-(node, level) sequence cursors,
//! [`Policy::evaluate`] returns the highest sequence at which every
//! clause is satisfied. Per clause: take the `count`-th largest cursor
//! at that level — that is the highest seq for which `count` nodes have
//! crossed. Across clauses: take the `min` (AND semantics).

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
    /// Event has been written to NVMe via `O_DIRECT` `pwrite`. With
    /// PLP-backed devices this survives power loss without an explicit
    /// fsync.
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
/// candidate sequence".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Clause {
    /// Minimum number of nodes that must satisfy `level`. Counted across
    /// the primary and all connected replicas. `0` is rejected by the
    /// parser — a zero-count clause is trivially true and almost always
    /// a config mistake.
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
    /// list is empty or contains a zero-count clause.
    pub fn new(clauses: Vec<Clause>) -> Result<Self, PolicyError> {
        if clauses.is_empty() {
            return Err(PolicyError::Empty);
        }
        if let Some(c) = clauses.iter().find(|c| c.count == 0) {
            return Err(PolicyError::ZeroCount(*c));
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
    /// Returns `0` if no sequence satisfies all clauses (e.g. a clause
    /// requires more nodes than exist at the requested level).
    #[inline]
    pub fn evaluate(&self, cursors: &CursorView<'_>) -> u64 {
        let mut result = u64::MAX;
        for clause in &self.clauses {
            let satisfied = nth_largest_cursor(cursors, clause.level, clause.count);
            if satisfied < result {
                result = satisfied;
            }
        }
        // u64::MAX is a sentinel for "no cursors, vacuously satisfied" —
        // an empty cluster gates nothing. `Policy::new` rejects an empty
        // clause list, so reaching here with `u64::MAX` requires an
        // empty `CursorView`, which the response stage never constructs.
        if result == u64::MAX { 0 } else { result }
    }
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

/// Compute the `count`-th largest cursor among all nodes at `level`.
/// That value is the highest sequence at which at least `count` nodes
/// have reached `level`.
///
/// Returns `0` when `count > nodes.len()` — the clause cannot be
/// satisfied by any sequence; the response gate must wait.
#[inline]
fn nth_largest_cursor(view: &CursorView<'_>, level: Level, count: u8) -> u64 {
    let n = count as usize;
    if n == 0 || n > view.nodes.len() {
        return 0;
    }
    // Stack buffer sized for the maximum cluster shape we ever expect
    // (1 primary + up to 8 replicas = 9). Replication today caps at
    // 1+2; even with Raft (#7) this stays in single digits. Avoiding
    // the heap allocation here matters because `evaluate` runs on the
    // response-stage hot path.
    const MAX_NODES: usize = 16;
    debug_assert!(
        view.nodes.len() <= MAX_NODES,
        "cluster larger than expected"
    );
    let mut buf = [0u64; MAX_NODES];
    let len = view.nodes.len().min(MAX_NODES);
    let level_idx = level as usize;
    for (i, node) in view.nodes.iter().take(len).enumerate() {
        // A higher-level cursor implies satisfaction of all lower
        // levels on the same node: if the node has persisted seq S,
        // then it has trivially also "received" seq S. Take the max
        // over `level_idx..=Persisted` to honour that monotonicity
        // even if the caller's cursors temporarily violate it (e.g.
        // during the brief window where a write completes before the
        // in-memory cursor has been republished).
        let mut v = 0u64;
        for &c in &node[level_idx..] {
            if c > v {
                v = c;
            }
        }
        buf[i] = v;
    }
    // Sort descending. `len` is at most 16, so an unstable sort is cheap.
    buf[..len].sort_unstable_by(|a, b| b.cmp(a));
    buf[n - 1]
}

/// Errors from [`Policy::new`] or [`parse`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyError {
    /// The clause list is empty.
    Empty,
    /// A clause had `count == 0`, which is trivially true and almost
    /// always a misconfiguration.
    ZeroCount(Clause),
    /// The policy string failed to tokenise / parse. The `String`
    /// holds an operator-facing diagnostic.
    Parse(String),
}

impl fmt::Display for PolicyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PolicyError::Empty => f.write_str("durability policy must contain at least one clause"),
            PolicyError::ZeroCount(c) => {
                write!(f, "durability policy clause `{c}` has zero count")
            }
            PolicyError::Parse(msg) => write!(f, "durability policy parse error: {msg}"),
        }
    }
}

impl std::error::Error for PolicyError {}

/// Parse a policy string of the form `"<level>>=<n> [&& <level>>=<n>]*"`.
///
/// Whitespace around tokens is ignored. Level names match
/// [`Level::as_str`] (`"in_memory"`, `"persisted"`). Examples:
///
/// - `"persisted>=1"` — at least one node has persisted (single-node
///   durability).
/// - `"persisted>=2"` — replaces the old `--quorum-durability` default.
/// - `"persisted>=1 && in_memory>=2"` — one node persisted, plus a
///   second node with the event in memory.
pub fn parse(input: &str) -> Result<Policy, PolicyError> {
    let mut clauses = Vec::new();
    for raw in input.split("&&") {
        let token = raw.trim();
        if token.is_empty() {
            return Err(PolicyError::Parse(format!(
                "empty clause in policy `{input}`"
            )));
        }
        clauses.push(parse_clause(token)?);
    }
    Policy::new(clauses)
}

fn parse_clause(token: &str) -> Result<Clause, PolicyError> {
    let (lvl_str, count_str) = token.split_once(">=").ok_or_else(|| {
        PolicyError::Parse(format!(
            "clause `{token}` is not of the form `<level>>=<n>`"
        ))
    })?;
    let level = match lvl_str.trim() {
        "in_memory" => Level::InMemory,
        "persisted" => Level::Persisted,
        other => {
            return Err(PolicyError::Parse(format!(
                "unknown level `{other}` (expected `in_memory` or `persisted`)"
            )));
        }
    };
    let count: u8 = count_str.trim().parse().map_err(|_| {
        PolicyError::Parse(format!(
            "clause `{token}` count must be a non-negative integer ≤ 255"
        ))
    })?;
    Ok(Clause { count, level })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn view(nodes: &[[u64; 2]]) -> CursorView<'_> {
        CursorView::new(nodes)
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
    fn parse_single_clause() {
        let p = parse("persisted>=2").unwrap();
        assert_eq!(p.clauses().len(), 1);
        assert_eq!(p.clauses()[0].count, 2);
        assert_eq!(p.clauses()[0].level, Level::Persisted);
    }

    #[test]
    fn parse_multi_clause_with_whitespace() {
        let p = parse("  persisted>=1   &&   in_memory>=2  ").unwrap();
        assert_eq!(p.clauses().len(), 2);
        assert_eq!(p.clauses()[0].level, Level::Persisted);
        assert_eq!(p.clauses()[1].level, Level::InMemory);
    }

    #[test]
    fn parse_rejects_unknown_level() {
        let err = parse("fsynced>=1").unwrap_err();
        assert!(matches!(err, PolicyError::Parse(_)));
    }

    #[test]
    fn parse_rejects_missing_op() {
        assert!(matches!(parse("persisted=1"), Err(PolicyError::Parse(_))));
    }

    #[test]
    fn parse_rejects_empty_clause() {
        assert!(matches!(
            parse("persisted>=1 &&"),
            Err(PolicyError::Parse(_))
        ));
    }

    #[test]
    fn display_round_trips_through_parse() {
        for s in [
            "persisted>=1",
            "persisted>=2",
            "persisted>=1 && in_memory>=3",
        ] {
            let p = parse(s).unwrap();
            assert_eq!(format!("{p}"), s);
        }
    }

    #[test]
    fn evaluate_single_clause_persisted_one_node() {
        // 3-node cluster, single-replica durability requirement.
        let p = parse("persisted>=1").unwrap();
        let nodes = [[100, 50], [80, 40], [70, 30]];
        // 1st-largest persisted = 50.
        assert_eq!(p.evaluate(&view(&nodes)), 50);
    }

    #[test]
    fn evaluate_single_clause_persisted_quorum() {
        // Replaces the old --quorum-durability default.
        let p = parse("persisted>=2").unwrap();
        let nodes = [[100, 50], [80, 40], [70, 30]];
        // 2nd-largest persisted = 40.
        assert_eq!(p.evaluate(&view(&nodes)), 40);
    }

    #[test]
    fn evaluate_and_clauses_takes_min() {
        // Mixed-level: persist on the leader, plus second node has
        // the event in memory.
        let p = parse("persisted>=1 && in_memory>=2").unwrap();
        // Node 0: in_mem=100 persisted=80
        // Node 1: in_mem=70  persisted=10
        // Node 2: in_mem=60  persisted=5
        // persisted>=1 → 80 (largest persisted)
        // in_memory>=2 → 70 (2nd-largest in_memory, after the implicit
        //                    promotion from persisted=80 which yields 80)
        // Wait — node 0 has persisted=80, which implies in_memory>=80.
        // So effective in_memory cursors are [100, 70, 60]; 2nd-largest = 70.
        // min(80, 70) = 70.
        let nodes = [[100, 80], [70, 10], [60, 5]];
        assert_eq!(p.evaluate(&view(&nodes)), 70);
    }

    #[test]
    fn evaluate_persisted_implies_in_memory() {
        // A node that has persisted seq S also satisfies in_memory at S
        // even if its raw in_memory cursor lags (e.g. wasn't republished).
        let p = parse("in_memory>=1").unwrap();
        // Raw in_memory cursors are all 0 but persisted is 50 on node 0.
        let nodes = [[0, 50], [0, 0], [0, 0]];
        assert_eq!(p.evaluate(&view(&nodes)), 50);
    }

    #[test]
    fn evaluate_count_exceeds_node_count() {
        // 2-node cluster, policy requires 3 persisted — gate stays at 0.
        let p = parse("persisted>=3").unwrap();
        let nodes = [[100, 100], [100, 100]];
        assert_eq!(p.evaluate(&view(&nodes)), 0);
    }

    #[test]
    fn evaluate_single_node_cluster() {
        // Standalone primary: one node, persisted>=1 satisfied by its
        // own journal cursor.
        let p = parse("persisted>=1").unwrap();
        let nodes = [[42, 30]];
        assert_eq!(p.evaluate(&view(&nodes)), 30);
    }
}
