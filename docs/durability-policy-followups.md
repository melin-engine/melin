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

### Bench-number regression vs published latency figures

The legacy `--async-replica-ack` removed ~50–80 µs of fsync round-trip
from the replication path. With the flag gone (`async_ack` hardcoded
to `false` at the three receiver call sites), the receiver gates ack
send on the local journal cursor — same behaviour as the pre-async-ack
baseline.

The new `in_memory>=N` policy clauses parse and evaluate correctly,
but they don't yet save fsync latency in practice because the receiver
still waits for the journal cursor before sending the ack carrying the
in-memory cursor.

Fix: re-implement the ack-on-receive optimisation in the receiver such
that an ack fires on every received batch (carrying the current
in-memory cursor + the journal-gated acked cursor). Done correctly,
this lets `in_memory>=2` match or beat the legacy async-ack
end-to-end latency without any operator-visible knob. Until this lands,
**bench numbers under the new code will be slightly worse** than
previously published figures using `--async-replica-ack`. Either ship
the optimisation before re-publishing, or flag the regression in
release notes.

The other original P1 items (idle-stage staleness, stale operator
docs, broken bench script, misleading "strictly stronger" claim)
landed in commits `8f28bf1` and `6fb806a`.

---

## P2 — Polish before public release

### Flap-log spam under rapid disconnect/reconnect

`crates/server/src/response.rs:389-407` re-emits the warn on every
transition into `degraded=true` with no rate limit. A flapping replica
(connect/disconnect every few seconds) will produce paired
warn/info entries on every flap.

Fix: gate transition logs on a sustained-state requirement (e.g. the
state must hold for ≥1 s before logging), or add a flap-counter and
escalate the message wording rather than spam.

### Reject impossible policies at parse time

The current parser accepts `persisted>=10` even though
`ReplicationMetrics` and `replica_active` are hard-coded to 2 slots
(supporting at most a 1+2 cluster, view length ≤ 3). Strict policies
above the cluster cap silently produce a permanently-stalled gate;
degrade-friendly policies above the cap silently clamp.

Fix: parse-time check `count <= MAX_CLUSTER_SIZE` (currently 3) and
return a `PolicyError::CountExceedsClusterCap`. Document the cap
prominently in `--help` and `docs/replication.md`.

### Degrade floor

`persisted>=3!` on a 1-node cluster silently clamps to 1. An operator
who configured `>=3` for a regulatory or commercial reason almost
certainly does not want the gate to open at the primary alone in a
2-node-down scenario.

Fix: add a floor syntax — e.g. `persisted>=3!2` ("degrade no further
than 2 nodes"), or a separate `--min-durability` CLI flag with a
default that prevents single-node fallback. Pair with parse-time
validation that `floor <= count`.

### Stale prose comment

`crates/server/src/replication/protocol.rs:27` still describes the old
9-byte Ack frame layout. Cosmetic but misleading for protocol-level
debugging.

### Overflow risk in `needed`

`crates/server/src/response.rs:436` computes `let needed = max_seq + 1`.
Astronomically improbable for `max_seq` to reach `u64::MAX` in
practice, but `checked_add` is free on the cold path and saves a
post-mortem the day someone breaks the assumption.

### Test coverage gaps

- No integration test for the **degrade transition itself**: fail a
  replica mid-test, assert `/healthz`'s `melin_durability_policy_degraded`
  flips to 1, restore, assert it flips back to 0.
- No regression test for the **just-connected race** (P0 above).
- No regression test for the **disconnect race** (P0 above).
- No test for a **standalone server with `persisted>=2!`** asserting
  `policy_degraded=1` from startup (P1 idle-staleness above).
- No **flapping** test for the warn-rate-limit fix above.
- Consider adding a **fuzz / proptest** target on the policy parser
  since this is operator-facing input.

---

## P3 — Commercial polish

### Replace `!` syntax with named modes

Industry comparables (Postgres `synchronous_standby_names`, Cassandra
consistency levels, Kafka `min.insync.replicas`) all use named modes
or word-form qualifiers. The `persisted>=2!` syntax is unique to Melin
and an exchange operator reading the deploy config will not parse it
on first contact.

Recommendation: spell it out. Options:

- `persisted>=2 best_effort` / `persisted>=2 strict`
- Named modes: `quorum_or_survivor` / `quorum_strict` / `journal_only`
- Both, with the named modes desugaring to clauses.

8 bytes of CLI saved by the punctuation suffix is not worth the
operator-comprehension friction at the deployment-review stage.

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

1. ~~P0 (B1, B2, wire byte-pattern test)~~ — landed in `a84540a`,
   `8888732`, `40f9c76`.
2. ~~P1 idle staleness, doc updates, bench script, doc claim~~ —
   landed in `6fb806a`, `8f28bf1`. The remaining P1 item
   (ack-on-receive plumbing) is still open; bench numbers under the
   new code carry the documented ~50–80 µs regression until it lands.
3. P2 on a follow-up branch.
4. P3 driven by buyer feedback / commercial roadmap.
