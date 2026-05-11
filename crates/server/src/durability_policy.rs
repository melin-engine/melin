//! Durability ack policy.
//!
//! The response stage gates outgoing acks on a cluster-wide durability
//! condition. This module expresses that condition as a structured
//! policy over per-node durability levels.
//!
//! # Operator surface
//!
//! Operators pick a [`DurabilityMode`] — `local`, `hybrid`, or
//! `durably-replicated` — via `--durability-mode`. Each mode constructs
//! its [`Policy`] (list of [`Clause`]s) directly in code; there is no
//! DSL between the flag and the gate.
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
    /// produce a permanently-stalled gate. Since [`Policy`] objects
    /// are constructed by [`DurabilityMode::to_policy`] from hand-
    /// written clause lists, hitting this error indicates a bug in
    /// that mapping.
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

/// Operator-facing durability mode. Each variant maps to one of three
/// named policies that compose the underlying [`Clause`] list directly
/// in code, replacing the legacy `--durability-policy <STRING>` DSL.
/// See `docs/replication.md` for the three-tier menu in operational
/// terms.
///
/// `clap::ValueEnum` derives `--durability-mode <local|hybrid|durably-replicated>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum DurabilityMode {
    /// `persisted>=1`. Single-node durability — the primary's
    /// PLP-backed NVMe write is the only confirmation needed.
    /// Required when running with `--standalone`; appropriate for
    /// dev/staging deployments without a replica.
    Local,

    /// `persisted>=1 && in_memory>=2`. One durable copy on the
    /// primary's disk plus an in-memory ack from a second node.
    /// Single-failure-safe with a brief RAM-only window (~80 µs on
    /// PLP-backed NVMe) for the secondary copy. The default — typical
    /// live trading deployments. Saves ~50–80 µs per fill vs
    /// [`DurablyReplicated`](Self::DurablyReplicated). Fails closed
    /// when no replica is connected.
    Hybrid,

    /// `persisted>=2`. Two durable copies before client ack. Zero
    /// RAM-only window; the gate stalls if no replica is currently
    /// connected. Compliance-driven venues.
    DurablyReplicated,
}

impl DurabilityMode {
    /// Build the underlying [`Policy`] for this mode. Every variant's
    /// clause list is hand-constructed from in-range counts, so
    /// [`Policy::new`] cannot fail — any regression would surface in
    /// the unit tests below.
    pub fn to_policy(self) -> Policy {
        let clauses = match self {
            DurabilityMode::Local => vec![Clause {
                count: 1,
                level: Level::Persisted,
            }],
            DurabilityMode::Hybrid => vec![
                Clause {
                    count: 1,
                    level: Level::Persisted,
                },
                Clause {
                    count: 2,
                    level: Level::InMemory,
                },
            ],
            DurabilityMode::DurablyReplicated => vec![Clause {
                count: 2,
                level: Level::Persisted,
            }],
        };
        Policy::new(clauses)
            .expect("DurabilityMode::to_policy: hand-constructed clauses must validate")
    }

    /// CLI / log-friendly name. Matches the `clap::ValueEnum`
    /// kebab-cased spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            DurabilityMode::Local => "local",
            DurabilityMode::Hybrid => "hybrid",
            DurabilityMode::DurablyReplicated => "durably-replicated",
        }
    }

    /// Parse the admin-channel / CLI wire spelling. Accepts the same
    /// kebab-cased strings [`as_str`](Self::as_str) emits so operators
    /// only have to learn one vocabulary across `--durability-mode`
    /// and the admin `DURABILITY` command.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "local" => Some(DurabilityMode::Local),
            "hybrid" => Some(DurabilityMode::Hybrid),
            "durably-replicated" => Some(DurabilityMode::DurablyReplicated),
            _ => None,
        }
    }

    /// Stable u8 discriminant. The response stage publishes the
    /// operator-selected mode through an [`AtomicU8`] so it can detect
    /// a runtime swap (via the admin `DURABILITY` command) with a
    /// relaxed load on every gate iteration — cheaper than crossing a
    /// `Mutex` or carrying a refcounted `Arc<Policy>` snapshot.
    /// Values are part of the in-process ABI between admin and
    /// response, not a wire format; they must remain stable so the
    /// round-trip `from_u8(as_u8(x)) == Some(x)` always holds.
    pub fn as_u8(self) -> u8 {
        match self {
            DurabilityMode::Local => 0,
            DurabilityMode::Hybrid => 1,
            DurabilityMode::DurablyReplicated => 2,
        }
    }

    /// Inverse of [`as_u8`]. Returns `None` for an unknown byte —
    /// callers initialise the atomic from a valid mode and the admin
    /// path only writes `as_u8(parse(s)?)`, so an unknown byte
    /// indicates memory corruption or a programmer bug. The response
    /// stage logs and retains the prior mode in that case rather than
    /// silently falling back.
    pub fn from_u8(b: u8) -> Option<Self> {
        match b {
            0 => Some(DurabilityMode::Local),
            1 => Some(DurabilityMode::Hybrid),
            2 => Some(DurabilityMode::DurablyReplicated),
            _ => None,
        }
    }
}

impl fmt::Display for DurabilityMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn view(nodes: &[[u64; 2]]) -> CursorView<'_> {
        CursorView::new(nodes)
    }

    /// Build a single-clause policy directly. Replaces the retired
    /// `--durability-policy` DSL parser as a test ergonomics helper.
    fn policy(level: Level, count: u8) -> Policy {
        Policy::new(vec![Clause { count, level }]).unwrap()
    }

    /// Build an AND-policy from `(level, count)` clauses.
    fn and_policy(clauses: &[(Level, u8)]) -> Policy {
        Policy::new(
            clauses
                .iter()
                .map(|&(level, count)| Clause { count, level })
                .collect(),
        )
        .unwrap()
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
        // The deployment caps cluster size at 1 primary + 2 replicas
        // = 3. `Policy::new` rejects any clause with a higher count;
        // since policies are only built by `DurabilityMode::to_policy`,
        // hitting this error in production would mean a bug in that
        // mapping.
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
        // `persisted>=3` is the strict-quorum-on-full-cluster policy a
        // paranoid venue would compose by hand.
        assert!(policy(Level::Persisted, MAX_CLUSTER_SIZE).clauses().len() == 1);
    }

    #[test]
    fn display_renders_canonical_form() {
        let p = and_policy(&[(Level::Persisted, 1), (Level::InMemory, 3)]);
        assert_eq!(format!("{p}"), "persisted>=1 && in_memory>=3");
    }

    #[test]
    fn durability_mode_u8_round_trip() {
        for m in [
            DurabilityMode::Local,
            DurabilityMode::Hybrid,
            DurabilityMode::DurablyReplicated,
        ] {
            assert_eq!(DurabilityMode::from_u8(m.as_u8()), Some(m));
        }
        // Unknown bytes surface as None — the response stage relies on
        // this to detect a corrupted atomic and retain the prior mode.
        for b in [3, 4, 255] {
            assert_eq!(DurabilityMode::from_u8(b), None);
        }
    }

    #[test]
    fn durability_mode_parse_matches_as_str() {
        for m in [
            DurabilityMode::Local,
            DurabilityMode::Hybrid,
            DurabilityMode::DurablyReplicated,
        ] {
            assert_eq!(DurabilityMode::parse(m.as_str()), Some(m));
        }
        for bad in ["", "LOCAL", "hyb", "fast", "durably_replicated"] {
            assert_eq!(DurabilityMode::parse(bad), None, "{bad:?} should not parse");
        }
    }

    #[test]
    fn evaluate_single_clause_persisted_one_node() {
        // 3-node cluster, persisted>=1 returns the largest persisted.
        let p = policy(Level::Persisted, 1);
        let nodes = [[100, 50], [80, 40], [70, 30]];
        // 1st-largest persisted = 50.
        assert_eq!(p.evaluate(&view(&nodes)), 50);
    }

    #[test]
    fn evaluate_single_clause_persisted_quorum() {
        let p = policy(Level::Persisted, 2);
        let nodes = [[100, 50], [80, 40], [70, 30]];
        // 2nd-largest persisted = 40.
        assert_eq!(p.evaluate(&view(&nodes)), 40);
    }

    #[test]
    fn evaluate_and_clauses_takes_min() {
        // Mixed-level: persist on the leader, plus second node has
        // the event in memory.
        let p = and_policy(&[(Level::Persisted, 1), (Level::InMemory, 2)]);
        // Node 0: in_mem=100 persisted=80
        // Node 1: in_mem=70  persisted=10
        // Node 2: in_mem=60  persisted=5
        // persisted>=1 → 80 (largest persisted)
        // in_memory>=2 → 70 (2nd-largest in_memory, after the implicit
        //                    promotion from persisted=80 which yields 80)
        // Effective in_memory cursors are [100, 70, 60]; 2nd-largest = 70.
        // min(80, 70) = 70.
        let nodes = [[100, 80], [70, 10], [60, 5]];
        assert_eq!(p.evaluate(&view(&nodes)), 70);
    }

    #[test]
    fn evaluate_persisted_implies_in_memory() {
        // A node that has persisted seq S also satisfies in_memory at S
        // even if its raw in_memory cursor lags (e.g. wasn't republished).
        let p = policy(Level::InMemory, 1);
        // Raw in_memory cursors are all 0 but persisted is 50 on node 0.
        let nodes = [[0, 50], [0, 0], [0, 0]];
        assert_eq!(p.evaluate(&view(&nodes)), 50);
    }

    #[test]
    fn evaluate_count_exceeds_node_count() {
        // 2-node cluster, policy requires 3 persisted — gate stays at 0
        // and the policy reports degraded.
        let p = policy(Level::Persisted, 3);
        let nodes = [[100, 100], [100, 100]];
        let v = view(&nodes);
        let r = p.evaluate_with_status(&v);
        assert_eq!(r.durable_pos, 0);
        assert!(r.degraded, "unsatisfiable clause must flag degraded");
    }

    #[test]
    fn strict_clause_stalls_when_under_target() {
        // 1-node view, persisted>=2 cannot be met — gate stays at 0.
        let p = policy(Level::Persisted, 2);
        let nodes = [[u64::MAX, 500]];
        let v = view(&nodes);
        let r = p.evaluate_with_status(&v);
        assert_eq!(r.durable_pos, 0);
        assert!(r.degraded);
    }

    #[test]
    fn evaluate_single_node_cluster() {
        // Standalone primary: one node, persisted>=1 satisfied by its
        // own journal cursor. Not degraded.
        let p = policy(Level::Persisted, 1);
        let nodes = [[42, 30]];
        let v = view(&nodes);
        let r = p.evaluate_with_status(&v);
        assert_eq!(r.durable_pos, 30);
        assert!(!r.degraded);
    }

    #[test]
    fn empty_view_flags_every_clause_degraded() {
        // Defensive: an empty view (no primary, no replicas) cannot
        // satisfy any non-zero clause. In practice the response stage
        // always includes the primary so the view is never empty;
        // this pins the safe behaviour for that invariant.
        let p = and_policy(&[(Level::Persisted, 1), (Level::InMemory, 2)]);
        let nodes: [[u64; 2]; 0] = [];
        let r = p.evaluate_with_status(&view(&nodes));
        assert_eq!(r.durable_pos, 0);
        assert!(r.degraded);
    }

    // -- Property-based tests --
    //
    // The structured policy surface (operator-facing input is now a
    // 3-variant enum) is type-safe; the property test surface that
    // remains is `evaluate_with_status` totality across arbitrary
    // cursor views.

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
    }

    // --- DurabilityMode tests ---

    #[test]
    fn mode_local_builds_persisted_ge_1() {
        let p = DurabilityMode::Local.to_policy();
        assert_eq!(p.clauses().len(), 1);
        let c = p.clauses()[0];
        assert_eq!(c.level, Level::Persisted);
        assert_eq!(c.count, 1);
    }

    #[test]
    fn mode_hybrid_builds_persisted_ge_1_and_in_memory_ge_2() {
        let p = DurabilityMode::Hybrid.to_policy();
        assert_eq!(p.clauses().len(), 2);
        let persisted = p
            .clauses()
            .iter()
            .find(|c| c.level == Level::Persisted)
            .expect("persisted clause");
        assert_eq!(persisted.count, 1);
        let in_mem = p
            .clauses()
            .iter()
            .find(|c| c.level == Level::InMemory)
            .expect("in_memory clause");
        assert_eq!(in_mem.count, 2);
    }

    #[test]
    fn mode_durably_replicated_builds_persisted_ge_2() {
        let p = DurabilityMode::DurablyReplicated.to_policy();
        assert_eq!(p.clauses().len(), 1);
        let c = p.clauses()[0];
        assert_eq!(c.level, Level::Persisted);
        assert_eq!(c.count, 2);
    }

    #[test]
    fn mode_hybrid_fails_closed_on_single_node() {
        // The gate must NOT advance when only the primary is present
        // — in_memory>=2 can't be satisfied. This is the fail-closed
        // semantic the design call rests on; the dev-evaluator
        // footgun is caught upstream by the `--standalone` validation
        // in `server.rs`.
        let p = DurabilityMode::Hybrid.to_policy();
        let nodes = [[100u64, 100u64]];
        let v = CursorView::new(&nodes);
        assert_eq!(p.evaluate(&v), 0, "hybrid stalls on single-node view");
    }

    #[test]
    fn mode_hybrid_advances_with_two_nodes_acking_in_memory() {
        // Primary at persisted=100, in_memory=100; replica at
        // in_memory=80, persisted=0. Both clauses' nth-largest must
        // cross to advance.
        // persisted>=1: 1st largest persisted = 100.
        // in_memory>=2: 2nd largest in_memory = 80.
        // Gate = min(100, 80) = 80.
        let p = DurabilityMode::Hybrid.to_policy();
        let nodes = [[100u64, 100u64], [80u64, 0u64]];
        let v = CursorView::new(&nodes);
        assert_eq!(p.evaluate(&v), 80);
    }
}
