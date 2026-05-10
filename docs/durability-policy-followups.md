# Durability policy — open follow-ups

Remaining production / commercial gaps on the configurable durability
ack policy (roadmap item #4). The original review punch list (P0
correctness bugs, P1 operator-surface gaps, the bulk of P2 polish, and
the `!`→`best_effort` rename from P3) all landed on this branch; this
file tracks only what's still open.

The ack-on-receive plumbing — re-implementing the legacy
`--async-replica-ack` semantic correctly so `in_memory>=N` policies
actually save the ~50–80 µs of fsync round-trip — graduated to
roadmap item #5 because it's a meaningful product-polish in its own
right, paired with flipping the default to `persisted>=1 &&
in_memory>=2` and re-framing the docs around a three-tier menu.

---

## P2 — Polish before public release

### Degrade floor

`persisted>=3 best_effort` on a 1-node cluster silently clamps to 1.
An operator who configured `>=3` for a regulatory or commercial reason
almost certainly does not want the gate to open at the primary alone
in a 2-node-down scenario.

Fix: add a floor syntax — e.g. `persisted>=3 best_effort_floor=2`
("degrade no further than 2 nodes"), or a separate `--min-durability`
CLI flag with a default that prevents single-node fallback. Pair with
parse-time validation that `floor <= count`.

---

## P3 — Commercial polish

### Compound named levels

Expressing "primary persisted plus any replica in-memory" is
`persisted>=1 && in_memory>=2`, which is correct but non-obvious
because the `in_memory>=2` count includes the primary's implicit
`u64::MAX` in-memory cursor. An exchange architect comparing this to
Aurora/Spanner-style "one local commit + one regional acknowledge"
needs to read the source to verify.

Recommendation: add named compound levels (`local_quorum`,
`cross_region_ack`, etc.) or worked examples in `docs/` mapping common
exchange-architecture patterns to policy strings.

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
noting as a buyer-driven enhancement.

### Per-request policy override

Cassandra and MongoDB let the application pick a stronger consistency
level per write for high-stakes operations. Melin's policy is
currently global. Worth considering once a buyer asks; the wire
protocol already carries a per-request envelope that could be
extended.
