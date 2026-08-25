//! Operator-facing ack policy.
//!
//! The generic policy types — [`Level`], [`Clause`], [`Policy`],
//! [`CursorView`], [`EvalStatus`], [`PolicyError`], [`MAX_CLUSTER_SIZE`]
//! — live in `melin_transport_core::ack_policy`. They're re-exported
//! here so call sites (`crate::ack_policy::*`) reach both the enum and
//! the machinery it compiles to, and the response stage's ack gate is
//! built on them.
//!
//! What this module owns is the *operator surface*: a small enum that
//! exposes four named policies (`disk`, `ram`, `disk+ram`, `two-disks`)
//! via `--ack-policy`, plus the mapping from each policy to the
//! underlying clause list. The set of policies is server policy, not a
//! transport-core concern, so it lives here.
//!
//! Every policy counts *copies*, never *nodes*: `disk` is satisfied by
//! whichever node fsyncs first, which is usually the primary but not
//! always (a disk spike on the primary lets the replica's fsync win).
//! The names deliberately say what must hold the event at ack time and
//! nothing about which node does it.

use std::fmt;

pub use melin_transport_core::ack_policy::{
    Blocker, Clause, CursorView, EvalStatus, Level, MAX_CLUSTER_SIZE, Policy, PolicyError,
};

/// Sentinel for "no primary ack policy observed yet" in the replica-side
/// gauge the replication stream feeds (see
/// `ReplicaControlPlane::primary_ack_policy`). Deliberately outside the
/// `as_u8` range so `from_u8` maps it to `None`; `u8::MAX` leaves the
/// low values free for future policies.
pub const ACK_POLICY_UNKNOWN: u8 = u8::MAX;

/// Operator-facing ack policy: which copies of an event must exist
/// before its response is released. Each variant maps to a named
/// [`Clause`] list composed directly in code. See `docs/replication.md`
/// for the menu in operational terms.
///
/// `clap::ValueEnum` derives `--ack-policy <disk|ram|disk+ram|two-disks>`;
/// the `disk+ram` spelling is set explicitly because the derive would
/// kebab-case the variant to `disk-and-ram`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum AckPolicy {
    /// `persisted>=1`. One fsynced copy, on whichever node's disk
    /// confirms first. Required when running with `--standalone`;
    /// appropriate for dev/staging deployments without a replica.
    Disk,

    /// `in_memory>=2`. Two nodes hold the event in memory before the
    /// client ack; every disk write trails asynchronously off the ack
    /// path (the journal still fsyncs every batch, it just no longer
    /// gates responses). Survives any single node failure via
    /// failover; loses only the un-fsynced tail on a simultaneous
    /// whole-cluster power loss. For deployments where fsync is slow
    /// (cloud block storage) or where that bounded RPO is an acceptable
    /// trade for the lowest ack latency. Fails closed when no replica
    /// is connected.
    Ram,

    /// `persisted>=1 && in_memory>=2`. One fsynced copy plus a second
    /// copy in another node's memory. Single-failure-safe with a brief
    /// RAM-only window (~80 µs on PLP-backed NVMe) for the second copy.
    /// The default — typical live trading deployments. Saves ~50–80 µs
    /// per fill vs [`TwoDisks`](Self::TwoDisks). Fails closed when no
    /// replica is connected.
    #[value(name = "disk+ram")]
    DiskAndRam,

    /// `persisted>=2`. Two fsynced copies before the client ack. Zero
    /// RAM-only window; the gate stalls if no replica is currently
    /// connected. Compliance-driven venues.
    TwoDisks,
}

impl AckPolicy {
    /// Build the underlying [`Policy`]. Every variant's clause list is
    /// hand-constructed from in-range counts, so [`Policy::new`] cannot
    /// fail — any regression would surface in the unit tests below.
    pub fn to_policy(self) -> Policy {
        let clauses = match self {
            AckPolicy::Disk => vec![Clause {
                count: 1,
                level: Level::Persisted,
            }],
            AckPolicy::Ram => vec![Clause {
                count: 2,
                level: Level::InMemory,
            }],
            AckPolicy::DiskAndRam => vec![
                Clause {
                    count: 1,
                    level: Level::Persisted,
                },
                Clause {
                    count: 2,
                    level: Level::InMemory,
                },
            ],
            AckPolicy::TwoDisks => vec![Clause {
                count: 2,
                level: Level::Persisted,
            }],
        };
        Policy::new(clauses).expect("AckPolicy::to_policy: hand-constructed clauses must validate")
    }

    /// CLI / log-friendly name. Matches the `clap::ValueEnum` spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            AckPolicy::Disk => "disk",
            AckPolicy::Ram => "ram",
            AckPolicy::DiskAndRam => "disk+ram",
            AckPolicy::TwoDisks => "two-disks",
        }
    }

    /// Parse the admin-channel / CLI wire spelling. Accepts exactly the
    /// strings [`as_str`](Self::as_str) emits so operators only have to
    /// learn one vocabulary across `--ack-policy` and the admin
    /// `ACK-POLICY` command.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "disk" => Some(AckPolicy::Disk),
            "ram" => Some(AckPolicy::Ram),
            "disk+ram" => Some(AckPolicy::DiskAndRam),
            "two-disks" => Some(AckPolicy::TwoDisks),
            _ => None,
        }
    }

    /// Parse the pre-0.15 "durability mode" names, for the deprecated
    /// `DURABILITY` admin alias. Accepts the current spellings too so
    /// the alias is a strict superset of [`parse`](Self::parse).
    /// Removed with the alias in the next minor release.
    pub fn parse_legacy(s: &str) -> Option<Self> {
        match s {
            "local" => Some(AckPolicy::Disk),
            "replicated" => Some(AckPolicy::Ram),
            "hybrid" => Some(AckPolicy::DiskAndRam),
            "durably-replicated" => Some(AckPolicy::TwoDisks),
            _ => Self::parse(s),
        }
    }

    /// Stable u8 discriminant. The response stage publishes the
    /// operator-selected policy through an
    /// [`AtomicU8`](std::sync::atomic::AtomicU8) so it can detect
    /// a runtime swap (via the admin `ACK-POLICY` command) with a
    /// relaxed load on every gate iteration — cheaper than crossing a
    /// `Mutex` or carrying a refcounted `Arc<Policy>` snapshot.
    ///
    /// **These values are a wire format**, not just an in-process ABI:
    /// the primary stamps this byte onto every `StreamStart` and
    /// `Heartbeat`, and the replica decodes it with [`from_u8`](Self::from_u8) to
    /// judge auto-promotion (see `raft_promotion`). So they must stay
    /// stable across releases in two directions — the round-trip
    /// `from_u8(as_u8(x)) == Some(x)` has to hold, *and* a value once
    /// released can never be reassigned to a different policy, or a
    /// peer on an older build would silently read the new policy as
    /// the old one. Add new policies on the next free byte; never
    /// renumber. (The bytes predate the current names: they were
    /// assigned to `local`/`hybrid`/`durably-replicated`/`replicated`
    /// and kept through the rename.)
    ///
    /// A peer that does not know a byte gets `None` and refuses to
    /// auto-promote on it — fail-closed, and the reason replicas must
    /// be upgraded before a primary starts advertising a new policy.
    pub fn as_u8(self) -> u8 {
        match self {
            AckPolicy::Disk => 0,
            AckPolicy::DiskAndRam => 1,
            AckPolicy::TwoDisks => 2,
            // 3, not a re-sort: these bytes are on the wire (see doc
            // comment), so a later variant takes the next free one
            // regardless of where it sits in the operator-facing menu.
            AckPolicy::Ram => 3,
        }
    }

    /// Inverse of [`as_u8`](Self::as_u8). Returns `None` for an unknown byte —
    /// callers initialise the atomic from a valid policy and the admin
    /// path only writes `as_u8(parse(s)?)`, so an unknown byte
    /// indicates memory corruption or a programmer bug. The response
    /// stage logs and retains the prior policy in that case rather than
    /// silently falling back.
    pub fn from_u8(b: u8) -> Option<Self> {
        match b {
            0 => Some(AckPolicy::Disk),
            1 => Some(AckPolicy::DiskAndRam),
            2 => Some(AckPolicy::TwoDisks),
            3 => Some(AckPolicy::Ram),
            _ => None,
        }
    }
}

impl fmt::Display for AckPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::ValueEnum;

    const ALL: [AckPolicy; 4] = [
        AckPolicy::Disk,
        AckPolicy::DiskAndRam,
        AckPolicy::TwoDisks,
        AckPolicy::Ram,
    ];

    #[test]
    fn u8_round_trip() {
        for p in ALL {
            assert_eq!(AckPolicy::from_u8(p.as_u8()), Some(p));
        }
        // Unknown bytes surface as None — the response stage relies on
        // this to detect a corrupted atomic and retain the prior policy.
        for b in [4, 5, 255] {
            assert_eq!(AckPolicy::from_u8(b), None);
        }
    }

    #[test]
    fn wire_bytes_are_pinned() {
        // On the wire since before the rename; see `as_u8`.
        assert_eq!(AckPolicy::Disk.as_u8(), 0);
        assert_eq!(AckPolicy::DiskAndRam.as_u8(), 1);
        assert_eq!(AckPolicy::TwoDisks.as_u8(), 2);
        assert_eq!(AckPolicy::Ram.as_u8(), 3);
    }

    #[test]
    fn parse_matches_as_str() {
        for p in ALL {
            assert_eq!(AckPolicy::parse(p.as_str()), Some(p));
        }
        for bad in [
            "",
            "DISK",
            "dis",
            "disk-ram",
            "disk-and-ram",
            "two-disk",
            "disks",
            "local",
            "hybrid",
            "replicated",
            "durably-replicated",
        ] {
            assert_eq!(AckPolicy::parse(bad), None, "{bad:?} should not parse");
        }
    }

    #[test]
    fn parse_legacy_maps_old_names_and_accepts_new_ones() {
        assert_eq!(AckPolicy::parse_legacy("local"), Some(AckPolicy::Disk));
        assert_eq!(AckPolicy::parse_legacy("replicated"), Some(AckPolicy::Ram));
        assert_eq!(
            AckPolicy::parse_legacy("hybrid"),
            Some(AckPolicy::DiskAndRam)
        );
        assert_eq!(
            AckPolicy::parse_legacy("durably-replicated"),
            Some(AckPolicy::TwoDisks)
        );
        for p in ALL {
            assert_eq!(AckPolicy::parse_legacy(p.as_str()), Some(p));
        }
        assert_eq!(AckPolicy::parse_legacy("fast"), None);
    }

    #[test]
    fn clap_spelling_matches_as_str() {
        // The admin channel parses `as_str` and the CLI parses the clap
        // value name; an operator must be able to use the same word in
        // both places.
        for p in ALL {
            let clap_name = p.to_possible_value().expect("every variant is a CLI value");
            assert_eq!(clap_name.get_name(), p.as_str());
        }
    }

    #[test]
    fn disk_builds_persisted_ge_1() {
        let p = AckPolicy::Disk.to_policy();
        assert_eq!(p.clauses().len(), 1);
        let c = p.clauses()[0];
        assert_eq!(c.level, Level::Persisted);
        assert_eq!(c.count, 1);
    }

    #[test]
    fn disk_and_ram_builds_persisted_ge_1_and_in_memory_ge_2() {
        let p = AckPolicy::DiskAndRam.to_policy();
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
    fn ram_builds_in_memory_ge_2() {
        let p = AckPolicy::Ram.to_policy();
        assert_eq!(p.clauses().len(), 1);
        let c = p.clauses()[0];
        assert_eq!(c.level, Level::InMemory);
        assert_eq!(c.count, 2);
    }

    #[test]
    fn ram_fails_closed_on_single_node() {
        // Same fail-closed semantic as disk+ram: `in_memory>=2` cannot
        // be satisfied by one node, so the gate must stall rather than
        // silently weaken to a single copy.
        let p = AckPolicy::Ram.to_policy();
        let nodes = [[100u64, 100u64]];
        let v = CursorView::new(&nodes);
        assert_eq!(p.evaluate(&v), 0, "ram stalls on single-node view");
    }

    #[test]
    fn ram_ignores_every_persisted_cursor() {
        // The policy's whole point: no disk gates the ack. Primary at
        // in_memory=100 with fsync trailing at 10; replica at
        // in_memory=80 with nothing persisted at all.
        // in_memory>=2: 2nd largest in_memory = 80. Gate = 80 — the
        // trailing fsyncs on both nodes must not bind.
        let p = AckPolicy::Ram.to_policy();
        let nodes = [[100u64, 10u64], [80u64, 0u64]];
        let v = CursorView::new(&nodes);
        assert_eq!(p.evaluate(&v), 80);
    }

    #[test]
    fn two_disks_builds_persisted_ge_2() {
        let p = AckPolicy::TwoDisks.to_policy();
        assert_eq!(p.clauses().len(), 1);
        let c = p.clauses()[0];
        assert_eq!(c.level, Level::Persisted);
        assert_eq!(c.count, 2);
    }

    #[test]
    fn no_policy_gates_on_the_primary_in_memory_sentinel() {
        // The response stage models the primary's in-memory cursor as
        // `u64::MAX` (events it gates are trivially in-memory on the
        // primary). A policy containing `in_memory>=1` would therefore
        // ack instantly and unconditionally — and would also break the
        // transport-core evaluation's u64::MAX-sentinel reasoning. Pin
        // the invariant: every clause of every policy is either
        // persisted-level or requires a second node.
        for p in ALL {
            for c in p.to_policy().clauses() {
                assert!(
                    c.level == Level::Persisted || c.count >= 2,
                    "policy {p}: clause `{c}` would be satisfied by the primary's \
                     in-memory sentinel alone"
                );
            }
        }
    }

    #[test]
    fn disk_and_ram_fails_closed_on_single_node() {
        // The gate must NOT advance when only the primary is present
        // — in_memory>=2 can't be satisfied. This is the fail-closed
        // semantic the design call rests on; the dev-evaluator
        // footgun is caught upstream by the `--standalone` validation
        // in `server.rs`.
        let p = AckPolicy::DiskAndRam.to_policy();
        let nodes = [[100u64, 100u64]];
        let v = CursorView::new(&nodes);
        assert_eq!(p.evaluate(&v), 0, "disk+ram stalls on single-node view");
    }

    #[test]
    fn disk_and_ram_advances_with_two_nodes_acking_in_memory() {
        // Primary at persisted=100, in_memory=100; replica at
        // in_memory=80, persisted=0. Both clauses' nth-largest must
        // cross to advance.
        // persisted>=1: 1st largest persisted = 100.
        // in_memory>=2: 2nd largest in_memory = 80.
        // Gate = min(100, 80) = 80.
        let p = AckPolicy::DiskAndRam.to_policy();
        let nodes = [[100u64, 100u64], [80u64, 0u64]];
        let v = CursorView::new(&nodes);
        assert_eq!(p.evaluate(&v), 80);
    }

    #[test]
    fn disk_is_satisfied_by_whichever_node_fsyncs_first() {
        // `disk` counts copies, not nodes: the primary's fsync is
        // stalled at 10 (disk spike) while the replica has persisted
        // through 90. persisted>=1 takes the largest persisted cursor,
        // so the replica's disk opens the gate.
        let p = AckPolicy::Disk.to_policy();
        let nodes = [[100u64, 10u64], [90u64, 90u64]];
        let v = CursorView::new(&nodes);
        assert_eq!(p.evaluate(&v), 90);
    }
}
