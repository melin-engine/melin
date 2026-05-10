# Durability policy — review follow-ups

Punch list from the post-implementation review of the `feat/durability-policy`
branch. The branch implements the configurable durability ack policy
(roadmap item #4) and is functionally correct for the failover suite,
but the review surfaced production / commercial gaps that should be
addressed before the work is held up as a finished feature for an
exchange-operator demo.

Items are tagged P1–P3:

- **P1** — fix before any operator demo or perf re-publish.
- **P2** — fix before public release.
- **P3** — commercial polish; can land on a follow-up branch.

The original P0 entries (just-connected gate freeze, disconnect-side
memory-model race, missing wire byte-pattern test) landed in commits
`a84540a`, `8888732`, and `40f9c76`.

---

## P1 — Production gaps

All original P1 items landed on this branch:

- Idle-stage observability for `policy_degraded` — `8f28bf1`
- Stale operator docs (`operations.md`, `replication.md`,
  `roadmap.md`), broken bench script, and the misleading
  "strictly stronger" claim — `6fb806a`

The remaining ack-on-receive plumbing graduated to a standalone
roadmap item (#5 in `docs/roadmap.md`) because it's a meaningful
product polish in its own right — paired with flipping the default
to `persisted>=1 && in_memory>=2` and re-framing the docs around a
three-tier durability menu (paranoid quorum / fast cross-node /
single-node).

---

## P2 — Polish before public release

### Degrade floor

`persisted>=3 best_effort` on a 1-node cluster silently clamps to 1.
An operator who configured `>=3` for a regulatory or commercial
reason almost certainly does not want the gate to open at the
primary alone in a 2-node-down scenario.

Fix: add a floor syntax — e.g. `persisted>=3 best_effort_floor=2`
("degrade no further than 2 nodes"), or a separate
`--min-durability` CLI flag with a default that prevents single-
node fallback. Pair with parse-time validation that `floor <=
count`.

### Test coverage gaps (remaining)

- No regression test for the **just-connected race** (P0 above —
  fixed in `a84540a`; the existing failover suite covers the broad
  scenario but a focused unit test in `response.rs` that simulates
  active=true with cursors=0 would harden against future
  regressions).
- No regression test for the **disconnect race** (P0 above — fixed
  in `8888732`; same comment).
- Consider adding a **fuzz / proptest** target on the policy parser
  since this is operator-facing input.

The other original P2 items (flap-log spam, parser cap rejection,
stale prose comment, overflow check, degrade-transition integration
test) landed in commit `821b6e6`.

---

## P3 — Commercial polish

The original "replace `!` syntax with named modes" entry landed in
commit `821b6e6` — the modifier is now spelled `best_effort` and the
parser rejects misspellings explicitly.

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

---

## Sequencing

1. ~~P0~~ — landed in `a84540a`, `8888732`, `40f9c76`.
2. ~~P1~~ (idle staleness, doc updates, bench script, doc claim) —
   landed in `6fb806a`, `8f28bf1`. Ack-on-receive + three-tier
   default graduated to roadmap item #5 in `docs/roadmap.md`.
3. ~~P2~~ (flap-log, parser cap, stale comment, overflow,
   degrade-transition test) and the `!`→`best_effort` rename from
   P3 — landed in `821b6e6`. Remaining P2 items (degrade floor,
   focused regression tests for the P0 races, parser fuzz target)
   on a follow-up branch.
4. P3 driven by buyer feedback / commercial roadmap.
