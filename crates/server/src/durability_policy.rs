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
//! `<level>>=<count>[ best_effort]` — "at least `count` nodes (counting
//! both the primary and any connected replicas) have reached `level`",
//! with an optional `best_effort` modifier that clamps the count to
//! the connected cluster shape rather than failing closed.
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
/// candidate sequence", with optional best-effort degrade behaviour
/// when fewer than `count` nodes are connected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Clause {
    /// Target number of nodes that must satisfy `level`. Counted across
    /// the primary and all connected replicas. `0` is rejected by the
    /// parser — a zero-count clause is trivially true and almost always
    /// a config mistake.
    pub count: u8,
    /// Durability level required.
    pub level: Level,
    /// When `true`, evaluate against `min(count, connected_node_count)`
    /// rather than `count` itself. Spelled `best_effort` in the policy
    /// string — e.g. `persisted>=2 best_effort` reads as "two
    /// persisted when two are available, otherwise as many as we
    /// have". Strict-by-default (`persisted>=2`) leaves the gate
    /// closed if fewer than two nodes are connected.
    ///
    /// Importantly, "best_effort" preserves the *count* semantic against
    /// the reduced cluster: a 1-replica-remaining cluster with
    /// `persisted>=2 best_effort` still requires both surviving nodes
    /// (primary + survivor) to persist, not just any one.
    ///
    /// Comparison to the legacy auto-degrade behaviour depends on cluster
    /// shape:
    ///
    /// - 1+2 deployments (primary + 2 replicas): when one replica dies,
    ///   the new code requires the primary AND the surviving replica to
    ///   persist (2-of-2). The legacy formula dropped to 1-node
    ///   durability in the same shape — the new code is **strictly
    ///   stronger** here.
    /// - 1+1 deployments (primary + 1 replica): when the replica dies,
    ///   the new code clamps to 1-of-1 (primary alone). The legacy
    ///   formula did the same. **Equivalent**, not stronger.
    pub best_effort: bool,
}

impl fmt::Display for Clause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}>={}", self.level, self.count)?;
        if self.best_effort {
            f.write_str(" best_effort")?;
        }
        Ok(())
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
    /// Returns `0` if no sequence satisfies all clauses (e.g. a strict
    /// clause requires more nodes than are connected). Clauses with
    /// `best_effort = true` clamp their count against `cursors.len()`
    /// so the gate keeps moving in degraded cluster shapes.
    #[inline]
    pub fn evaluate(&self, cursors: &CursorView<'_>) -> u64 {
        self.evaluate_with_status(cursors).durable_pos
    }

    /// Like [`evaluate`](Self::evaluate) but also reports whether any
    /// clause was actively clamped — i.e. the policy is currently
    /// running degraded. The response stage uses this to surface a
    /// `policy_degraded` health metric and emit periodic warnings while
    /// the cluster is operating below the policy's target shape.
    #[inline]
    pub fn evaluate_with_status(&self, cursors: &CursorView<'_>) -> EvalStatus {
        let mut result = u64::MAX;
        let mut degraded = false;
        for clause in &self.clauses {
            let effective_count = if clause.best_effort {
                let view_len = cursors.len() as u8;
                let clamped = clause.count.min(view_len.max(1));
                if clamped < clause.count {
                    degraded = true;
                }
                clamped
            } else {
                clause.count
            };
            let satisfied = nth_largest_cursor(cursors, clause.level, effective_count);
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
/// satisfied. `degraded` is true iff at least one degrade-friendly
/// clause was actively clamped against a smaller-than-target cluster
/// shape — i.e. the policy is honouring availability over the strict
/// target count for this evaluation. Operators surface this via
/// `/healthz` and a periodic warn-level log.
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
    /// A clause requires more nodes than the deployment can have. The
    /// server caps cluster size at 1 primary + 2 replicas = 3 nodes
    /// (see [`MAX_CLUSTER_SIZE`]); a clause with `count > 3` is
    /// either a typo or a copy-paste from a hypothetical future
    /// topology and would silently produce a permanently-stalled
    /// gate (strict) or always clamp (best-effort). Either way the
    /// operator's intent is violated, so we reject at parse time.
    CountExceedsClusterCap { count: u8, max: u8 },
    /// The policy string failed to tokenise / parse. The `String`
    /// holds an operator-facing diagnostic.
    Parse(String),
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
            PolicyError::Parse(msg) => write!(f, "durability policy parse error: {msg}"),
        }
    }
}

impl std::error::Error for PolicyError {}

/// Parse a policy string of the form
/// `"<level>>=<n>[ best_effort] [&& <level>>=<n>[ best_effort]]*"`.
///
/// Whitespace around tokens is ignored. Level names match
/// [`Level::as_str`] (`"in_memory"`, `"persisted"`). The optional
/// `best_effort` keyword (separated from the count by whitespace)
/// marks the clause as degrade-friendly: it clamps to the connected
/// cluster shape rather than failing closed when fewer nodes than
/// the target count are connected.
///
/// Examples:
///
/// - `"persisted>=1"` — at least one node has persisted (single-node
///   durability).
/// - `"persisted>=2"` — strict two-node quorum; gate stalls if a
///   replica is lost.
/// - `"persisted>=2 best_effort"` — two-node quorum when two nodes
///   are connected, clamps to as many as remain (still requires
///   *all* survivors to persist).
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
    let (lvl_str, rhs) = token.split_once(">=").ok_or_else(|| {
        PolicyError::Parse(format!(
            "clause `{token}` is not of the form `<level>>=<n>[ best_effort]`"
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
    // Split the right-hand side into the integer count and an
    // optional `best_effort` keyword separated by whitespace.
    // Any other trailing token is rejected so a typo (e.g.
    // `besteffort`, `best-effort`, `BestEffort`) surfaces as a
    // parse error rather than silently giving strict semantics.
    let mut tokens = rhs.split_whitespace();
    let count_str = tokens
        .next()
        .ok_or_else(|| PolicyError::Parse(format!("clause `{token}` is missing a count")))?;
    let best_effort = match tokens.next() {
        None => false,
        Some("best_effort") => true,
        Some(other) => {
            return Err(PolicyError::Parse(format!(
                "unknown clause modifier `{other}` in `{token}` (expected `best_effort`)"
            )));
        }
    };
    if let Some(extra) = tokens.next() {
        return Err(PolicyError::Parse(format!(
            "unexpected trailing token `{extra}` in clause `{token}`"
        )));
    }
    let count: u8 = count_str.parse().map_err(|_| {
        PolicyError::Parse(format!(
            "clause `{token}` count must be a non-negative integer ≤ 255"
        ))
    })?;
    Ok(Clause {
        count,
        level,
        best_effort,
    })
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
            best_effort: false,
        };
        assert_eq!(Policy::new(vec![c]), Err(PolicyError::ZeroCount(c)));
    }

    #[test]
    fn parse_single_clause() {
        let p = parse("persisted>=2").unwrap();
        assert_eq!(p.clauses().len(), 1);
        assert_eq!(p.clauses()[0].count, 2);
        assert_eq!(p.clauses()[0].level, Level::Persisted);
        assert!(!p.clauses()[0].best_effort);
    }

    #[test]
    fn parse_best_effort_keyword() {
        let p = parse("persisted>=2 best_effort").unwrap();
        assert_eq!(p.clauses()[0].count, 2);
        assert!(p.clauses()[0].best_effort);
    }

    #[test]
    fn parse_best_effort_with_extra_whitespace() {
        // Multiple inner spaces are fine — `split_whitespace` handles
        // them. Leading / trailing whitespace also OK via outer trim.
        let p = parse("  persisted>=2   best_effort  ").unwrap();
        assert!(p.clauses()[0].best_effort);
    }

    #[test]
    fn parse_rejects_misspelled_modifier() {
        // A typo like `besteffort` or `best-effort` must surface as a
        // parse error, not silently fall through to strict semantics.
        for bad in [
            "persisted>=2 besteffort",
            "persisted>=2 best-effort",
            "persisted>=2 BestEffort",
            "persisted>=2 weak",
        ] {
            assert!(
                matches!(parse(bad), Err(PolicyError::Parse(_))),
                "expected parse error for `{bad}`"
            );
        }
    }

    #[test]
    fn parse_rejects_trailing_garbage() {
        assert!(matches!(
            parse("persisted>=2 best_effort extra"),
            Err(PolicyError::Parse(_))
        ));
    }

    #[test]
    fn count_exceeding_cluster_cap_rejected() {
        // The deployment caps cluster size at 1 primary + 2 replicas
        // = 3. A clause asking for more is either a typo or wishful
        // thinking; either way the operator's intent isn't met. Fail
        // at parse time instead of producing a permanently-stalled
        // gate (strict) or always-clamped-to-3 (best-effort).
        for bad in [
            "persisted>=4",
            "persisted>=4 best_effort",
            "in_memory>=10",
            "persisted>=1 && in_memory>=255",
        ] {
            match parse(bad) {
                Err(PolicyError::CountExceedsClusterCap { count: _, max }) => {
                    assert_eq!(max, MAX_CLUSTER_SIZE);
                }
                other => panic!("expected CountExceedsClusterCap for `{bad}`, got {other:?}"),
            }
        }
    }

    #[test]
    fn count_at_cluster_cap_accepted() {
        // The full-cluster count is valid; only > MAX_CLUSTER_SIZE
        // is rejected. `persisted>=3` is the strict-quorum-on-full-
        // cluster policy a paranoid venue would write.
        assert!(parse("persisted>=3").is_ok());
        assert!(parse("persisted>=3 best_effort").is_ok());
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
            "persisted>=2 best_effort",
            "persisted>=1 && in_memory>=3",
            "persisted>=2 best_effort && in_memory>=1",
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

    // --- Degrade-friendly clauses ---

    #[test]
    fn degrade_clamps_to_view_len_when_under_target() {
        // `persisted>=2 best_effort` on a 1-node view (just the primary, all
        // replicas disconnected): clamp to 1, gate opens at the primary.
        let p = parse("persisted>=2 best_effort").unwrap();
        let nodes = [[u64::MAX, 50]];
        assert_eq!(p.evaluate(&view(&nodes)), 50);
    }

    #[test]
    fn degrade_no_op_when_view_meets_target() {
        // `persisted>=2 best_effort` on a 3-node view behaves identically to the
        // strict form when the cluster is healthy.
        let strict = parse("persisted>=2").unwrap();
        let degrade = parse("persisted>=2 best_effort").unwrap();
        let nodes = [[u64::MAX, 100], [120, 80], [110, 70]];
        let v = view(&nodes);
        assert_eq!(strict.evaluate(&v), degrade.evaluate(&v));
    }

    #[test]
    fn degrade_one_replica_down_requires_both_survivors() {
        // `persisted>=2 best_effort` on 2 connected nodes (primary + 1 surviving
        // replica): clamp to 2, gate opens only when both have persisted.
        // This is the key win over legacy auto-degrade, which would have
        // dropped to 1-node durability here.
        let p = parse("persisted>=2 best_effort").unwrap();
        // Primary persisted=100, survivor persisted=50.
        // 2nd-largest = 50 (both must reach this).
        let nodes = [[u64::MAX, 100], [70, 50]];
        assert_eq!(p.evaluate(&view(&nodes)), 50);

        // If the survivor lags, the gate waits for it.
        let nodes = [[u64::MAX, 200], [10, 10]];
        assert_eq!(p.evaluate(&view(&nodes)), 10);
    }

    #[test]
    fn strict_clause_stalls_when_under_target() {
        // Without `!`, `persisted>=2` returns 0 on a 1-node view.
        let p = parse("persisted>=2").unwrap();
        let nodes = [[u64::MAX, 500]];
        assert_eq!(p.evaluate(&view(&nodes)), 0);
    }

    #[test]
    fn degrade_floors_at_one_node() {
        // Defensive: even if `cursors.len()` were somehow 0 the clamp
        // shouldn't allow `effective_count = 0` (that would trivially
        // satisfy the clause — exactly what `Policy::new` rejects). The
        // `view_len.max(1)` floor in evaluate guards against this.
        // In practice the response stage always includes the primary,
        // so view.len() >= 1.
        let p = parse("persisted>=2 best_effort").unwrap();
        let nodes: [[u64; 2]; 0] = [];
        // Empty view: nth_largest_cursor returns 0 (no nodes to satisfy).
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
