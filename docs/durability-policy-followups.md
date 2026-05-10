# Durability policy — open follow-ups

Remaining production / commercial gaps on the configurable durability
ack policy (roadmap item #4). The original review punch list (P0
correctness bugs, P1 operator-surface gaps, the bulk of P2 polish, and
the `!`→`best_effort` rename from P3) all landed on this branch; this
file tracks only what's still open.

Two pieces of follow-up work are planned, in this order:

1. **Ack-on-receive plumbing** (roadmap item #5) — re-implement the
   legacy `--async-replica-ack` semantic correctly so a policy with
   an `in_memory` clause actually saves the ~50–80 µs of fsync
   round-trip. Until this lands, `in_memory>=N` parses correctly but
   produces the same end-to-end latency as `persisted>=N`. Includes a
   revisit of ack coalescing — the new design doubles the trigger
   rate (receive + journal-advance), and while replication-wire
   bandwidth is comfortably under-utilised (~1.6 MB/s of acks vs
   ~1 GB/s of data the other way), the coalescing window and ack-
   piggyback rule should be tuned to keep the send-syscall rate
   honest and avoid packet flood under bursty journal advances.
2. **Replace the policy DSL with a 3-variant enum** (see below) —
   collapse the operator-facing surface to a single
   `--durability-mode` flag with three named modes, each a real
   product tier. Depends on (1) so `Hybrid` actually delivers its
   latency advantage.

---

## Planned interface simplification: 3-variant enum

The current `--durability-policy <STRING>` exposes a custom DSL
(`<level>>=<n>[ best_effort]` clauses joined with `&&`). The DSL is
flexible but the flexibility doesn't pay off — all the meaningful
operator intent for a 1+2 deployment fits into three named modes.

Industry comparables all use named modes or simple flags rather than
grammars: Cassandra (`ONE`/`QUORUM`/`LOCAL_QUORUM`/`EACH_QUORUM`),
Postgres (`synchronous_commit = local | remote_write | on`), MongoDB
(`w=1|majority|N`), Kafka (`acks=0|1|all` + `min.insync.replicas`).
None ship a grammar.

### Target enum

```rust
pub enum DurabilityMode {
    /// `persisted>=1`. Single-node durability — the primary's
    /// fsync is the only confirmation needed before client ack.
    /// For standalone deployments and dev/test.
    Local,

    /// `persisted>=1 && in_memory>=2`. One durable copy on disk
    /// plus an in-memory ack from another node. Single-failure-
    /// safe (any one node can fail without data loss); the only
    /// loss window is a simultaneous primary-disk-failure-AND-
    /// replica-process-crash within ~80 µs of ack — outside any
    /// practical durability guarantee. The default — the right
    /// choice for typical exchange deployments running PLP-backed
    /// enterprise NVMe. Saves ~50–80 µs per fill vs
    /// `DurablyReplicated` on the gate's critical path. The name
    /// reads as "hybrid: durable on one, in-memory on another".
    Hybrid,

    /// `persisted>=2`. Two durable copies on separate nodes before
    /// client ack. Zero RAM-only window; gate stalls if a replica
    /// is unreachable. For compliance-driven venues that prefer
    /// halt-on-degrade over the `Hybrid` mode's ~80 µs RAM-only
    /// window. The name spells out the property: durably replicated
    /// — the data is on more than one disk before any client hears
    /// about it.
    DurablyReplicated,
}
```

The default flips from the current `persisted>=2 best_effort` to
`Hybrid`. `DurablyReplicated` covers the strict-fsync use case;
`persisted>=2 best_effort` (try-2-then-degrade) becomes redundant
since `Hybrid` is strictly faster with identical single-failure
survivability.

### CLI surface

```
--durability-mode local | hybrid | durably-replicated
```

One flag, three values. Operators reading the deploy config parse
the intent on first contact.

### What gets dropped

- `--durability-policy <STRING>` flag.
- The DSL parser (~150 LOC + ~30 unit tests).
- The `best_effort` modifier syntax.
- Compound clauses (`persisted>=1 && in_memory>=2` etc. — expressible
  as `Replicated`).
- The floor pattern (`persisted>=3 best_effort && persisted>=2` —
  largely redundant with the matching-stage halt anyway).

### What stays

- `Policy`/`Clause`/`Level` types as internal construction helpers
  (each mode builds its clause list in code).
- Wire protocol unchanged: `Ack { acked_sequence, in_memory_sequence }`
  carries forward identically.
- All cursor plumbing, observability (`policy_degraded` gauge,
  periodic warn, `DegradationLogger`), bug fixes (B1/B2), and tests.
- The `MAX_CLUSTER_SIZE` cap and parser fuzz tests become internal
  invariants on the construction helpers.

### Sequencing

1. Roadmap #5 lands `Hybrid`'s latency win — without ack-on-receive
   plumbing, `Hybrid` and `DurablyReplicated` have identical end-to-
   end latency and shipping the enum would be advertising a
   difference that doesn't materialise.
2. Then a follow-up branch swaps the CLI surface to `--durability-mode`,
   removes the DSL parser, and updates `operations.md` /
   `replication.md`. Net code reduction probably ~200 LOC.

---

## P3 — Commercial polish

### Degraded-duration counter on `/healthz`

`melin_durability_policy_degraded` is a 0/1 gauge — operators want
"how long has this been degraded" for SLO reporting. Add
`melin_durability_policy_degraded_seconds_total` (counter) so
dashboards can compute time-in-degraded over arbitrary windows.

### Multi-region awareness

The current model treats all replicas as fungible. Real exchange
deployments split replicas across availability zones / data centres
and want to express "≥1 ack from each zone" — the equivalent of
Cassandra's `EACH_QUORUM`. Out of scope for this branch, but worth
noting as a buyer-driven enhancement that would justify a 4th
`DurabilityMode` variant or a richer interface.

### Per-request policy override

Cassandra and MongoDB let the application pick a stronger consistency
level per write for high-stakes operations. Melin's policy is
currently global. Worth considering once a buyer asks; the wire
protocol already carries a per-request envelope that could be
extended. Composes cleanly with the enum simplification — the
operator's `--durability-mode` becomes a default, with per-request
overrides scoped to the named-mode set.
