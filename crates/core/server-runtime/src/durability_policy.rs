//! Operator-facing durability mode.
//!
//! The generic policy types — [`Level`], [`Clause`], [`Policy`],
//! [`CursorView`], [`EvalStatus`], [`PolicyError`], [`MAX_CLUSTER_SIZE`]
//! — live in `melin_transport_core::durability_policy`. They're
//! re-exported here so existing call sites (`crate::durability_policy::*`)
//! keep working, and the response stage's ack gate is built on them.
//!
//! What this module owns is the *operator surface*: a small enum that
//! exposes four named modes (`local`, `replicated`, `hybrid`,
//! `durably-replicated`) via `--durability-mode`, plus the mapping from
//! each mode to the underlying clause list. The set of modes is
//! exchange-server policy, not a transport-core concern, so it lives
//! here.

use std::fmt;

pub use melin_transport_core::durability_policy::{
    Blocker, Clause, CursorView, EvalStatus, Level, MAX_CLUSTER_SIZE, Policy, PolicyError,
};

/// Operator-facing durability mode. Each variant maps to one of four
/// named policies that compose the underlying [`Clause`] list directly
/// in code, replacing the legacy `--durability-policy <STRING>` DSL.
/// See `docs/replication.md` for the mode menu in operational terms.
///
/// Sentinel for "no primary acking mode observed yet" in the
/// replica-side gauge the replication stream feeds (see
/// `ReplicaControlPlane::primary_acking_mode`). Deliberately outside
/// the `as_u8` range so `from_u8` maps it to `None`; `u8::MAX` leaves
/// the low values free for future modes.
pub const ACKING_MODE_UNKNOWN: u8 = u8::MAX;

/// `clap::ValueEnum` derives `--durability-mode <local|replicated|hybrid|durably-replicated>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum DurabilityMode {
    /// `persisted>=1`. Single-node durability — the primary's
    /// PLP-backed NVMe write is the only confirmation needed.
    /// Required when running with `--standalone`; appropriate for
    /// dev/staging deployments without a replica.
    Local,

    /// `in_memory>=2`. RAM-quorum durability — a second node holds the
    /// event in memory before the client ack; every disk write trails
    /// asynchronously off the ack path (the journal still fsyncs every
    /// batch, it just no longer gates responses). Survives any single
    /// node failure via failover; loses only the un-fsynced tail on a
    /// simultaneous whole-cluster power loss. For deployments where
    /// fsync is slow (cloud block storage) or where that bounded RPO
    /// is an acceptable trade for the lowest ack latency. Fails closed
    /// when no replica is connected.
    Replicated,

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
            DurabilityMode::Replicated => vec![Clause {
                count: 2,
                level: Level::InMemory,
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
            DurabilityMode::Replicated => "replicated",
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
            "replicated" => Some(DurabilityMode::Replicated),
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
    ///
    /// **These values are a wire format**, not just an in-process ABI:
    /// the primary stamps this byte onto every `StreamStart` and
    /// `Heartbeat`, and the replica decodes it with [`from_u8`] to
    /// judge auto-promotion (see `raft_promotion`). So they must stay
    /// stable across releases in two directions — the round-trip
    /// `from_u8(as_u8(x)) == Some(x)` has to hold, *and* a value once
    /// released can never be reassigned to a different mode, or a peer
    /// on an older build would silently read the new mode as the old
    /// one. Add new modes on the next free byte; never renumber.
    ///
    /// A peer that does not know a byte gets `None` and refuses to
    /// auto-promote on it — fail-closed, and the reason replicas must
    /// be upgraded before a primary starts advertising a new mode.
    pub fn as_u8(self) -> u8 {
        match self {
            DurabilityMode::Local => 0,
            DurabilityMode::Hybrid => 1,
            DurabilityMode::DurablyReplicated => 2,
            // 3, not a re-sort: these bytes are on the wire (see doc
            // comment), so a later variant takes the next free one
            // regardless of where it sits in the operator-facing menu.
            DurabilityMode::Replicated => 3,
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
            3 => Some(DurabilityMode::Replicated),
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

    #[test]
    fn durability_mode_u8_round_trip() {
        for m in [
            DurabilityMode::Local,
            DurabilityMode::Hybrid,
            DurabilityMode::DurablyReplicated,
            DurabilityMode::Replicated,
        ] {
            assert_eq!(DurabilityMode::from_u8(m.as_u8()), Some(m));
        }
        // Unknown bytes surface as None — the response stage relies on
        // this to detect a corrupted atomic and retain the prior mode.
        for b in [4, 5, 255] {
            assert_eq!(DurabilityMode::from_u8(b), None);
        }
    }

    #[test]
    fn durability_mode_parse_matches_as_str() {
        for m in [
            DurabilityMode::Local,
            DurabilityMode::Hybrid,
            DurabilityMode::DurablyReplicated,
            DurabilityMode::Replicated,
        ] {
            assert_eq!(DurabilityMode::parse(m.as_str()), Some(m));
        }
        for bad in ["", "LOCAL", "hyb", "fast", "durably_replicated", "repl"] {
            assert_eq!(DurabilityMode::parse(bad), None, "{bad:?} should not parse");
        }
    }

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
    fn mode_replicated_builds_in_memory_ge_2() {
        let p = DurabilityMode::Replicated.to_policy();
        assert_eq!(p.clauses().len(), 1);
        let c = p.clauses()[0];
        assert_eq!(c.level, Level::InMemory);
        assert_eq!(c.count, 2);
    }

    #[test]
    fn mode_replicated_fails_closed_on_single_node() {
        // Same fail-closed semantic as hybrid: `in_memory>=2` cannot be
        // satisfied by the primary alone, so the gate must stall rather
        // than silently weaken to local durability.
        let p = DurabilityMode::Replicated.to_policy();
        let nodes = [[100u64, 100u64]];
        let v = CursorView::new(&nodes);
        assert_eq!(p.evaluate(&v), 0, "replicated stalls on single-node view");
    }

    #[test]
    fn mode_replicated_ignores_every_persisted_cursor() {
        // The mode's whole point: no disk gates the ack. Primary at
        // in_memory=100 with fsync trailing at 10; replica at
        // in_memory=80 with nothing persisted at all.
        // in_memory>=2: 2nd largest in_memory = 80. Gate = 80 — the
        // trailing fsyncs on both nodes must not bind.
        let p = DurabilityMode::Replicated.to_policy();
        let nodes = [[100u64, 10u64], [80u64, 0u64]];
        let v = CursorView::new(&nodes);
        assert_eq!(p.evaluate(&v), 80);
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
    fn no_mode_gates_on_the_primary_in_memory_sentinel() {
        // The response stage models the primary's in-memory cursor as
        // `u64::MAX` (events it gates are trivially in-memory on the
        // primary). A mode whose policy contained `in_memory>=1` would
        // therefore ack instantly and unconditionally — and would also
        // break the transport-core evaluation's u64::MAX-sentinel
        // reasoning. Pin the invariant: every clause of every mode is
        // either persisted-level or requires a second node.
        for m in [
            DurabilityMode::Local,
            DurabilityMode::Replicated,
            DurabilityMode::Hybrid,
            DurabilityMode::DurablyReplicated,
        ] {
            for c in m.to_policy().clauses() {
                assert!(
                    c.level == Level::Persisted || c.count >= 2,
                    "mode {m}: clause `{c}` would be satisfied by the primary's \
                     in-memory sentinel alone"
                );
            }
        }
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
