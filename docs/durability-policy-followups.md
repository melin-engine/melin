# Durability policy — review follow-ups

Punch list from the post-implementation review of the `feat/durability-policy`
branch (commits `329bbdf`..`bf36083`). The branch itself implements the
configurable durability ack policy (roadmap item #4) and is functionally
correct for the failover suite, but the review surfaced two real bugs and
a handful of production / commercial gaps that should be addressed before
the work is held up as a finished feature for an exchange-operator demo.

Items are tagged P0–P3:

- **P0** — block ship. Real bugs.
- **P1** — fix before any operator demo or perf re-publish.
- **P2** — fix before public release.
- **P3** — commercial polish; can land on a follow-up branch.

---

## P0 — Bugs

### Just-connected replica freezes the gate

Symptom: a 1+1 cluster running degraded (replica disconnected, gate
clamped to primary alone) momentarily freezes when the replica
reconnects. `cached_durable_pos` collapses to `0` until the first live
ack from the new slot arrives.

Root cause: the sender flips `active_flag` to `true` *after*
catch-up completes, but `ReplicationMetrics::{acked,in_memory}_sequence[i]`
were reset to `0` on the prior disconnect and stay at `0` until the
first live ack lands. The gate at `crates/server/src/response.rs:702`
reads `active=true`, then loads `cursor=0`, includes a `[0, 0]` row in
the view. With the default `persisted>=2!` and primary at e.g. seq 500,
the 2nd-largest persisted across `{primary=500, slot=0}` is `0` and the
gate stalls.

Affects:

- `crates/server/src/replication/tcp_sender.rs:580` — `active_flag.store(true, Release)`
- `crates/server/src/replication/dpdk.rs:426` — same shape
- `crates/server/src/replication/rumcast_sender.rs:969` — same shape

Fix: seed `metrics.{acked,in_memory}_sequence[i]` to a sane value (e.g.
the handshake's `last_sequence`, or a sentinel handled in
`evaluate_durability`) **before** the `active_flag` Release. Add a
regression test that exercises the connect-after-degrade transition and
asserts the gate does not regress below the previous `cached_durable_pos`.

### Disconnect-side ordering race

Symptom: on weak-memory architectures (ARM/AArch64) the gate can
observe `active=true` (stale) paired with `cursor=0` (fresh) during a
disconnect. x86 TSO accidentally hides this — the bug is latent on
current production hardware but ships waiting for the first ARM
deployment.

Root cause: the disconnect cleanup writes `active_flag=false`
(`Release`) **before** the cursor resets, which use `Relaxed`. The
cursor stores can be reordered around the Release from the reader's
POV under a relaxed memory model.

Affects:

- `crates/server/src/replication/tcp_sender.rs:185-188`
- `crates/server/src/replication/dpdk.rs:227-229, 524-527, 594-597, 626-628`

Fix: zero the cursor metrics **before** the `active_flag` Release, or
promote the cursor resets to `Release`. Pair with a regression test
covering the disconnect window.

### Wire-format byte-pattern test missing

Only `size_of::<AckFrame>() == 17` is asserted. A future `repr(C)`
field reorder or `little_endian::U64` wrapper change would silently
break wire compatibility without the const assert noticing.

Fix: add a byte-pattern test in `crates/server/src/replication/mod.rs`'s
`tests` module that encodes `Ack { acked_sequence: 0xDEAD_BEEF_CAFE_F00D,
in_memory_sequence: 0x1122_3344_5566_7788 }` and asserts the exact 21
bytes (`[len:u32 = 17][tag:u8 = MSG_ACK][acked_sequence:u64 LE][in_memory_sequence:u64 LE]`).

---

## P1 — Production gaps

### `policy_degraded` is stale during idle periods

`StageUtilization::policy_degraded` and the periodic warn-level log are
only updated inside the response stage's gate-spin loop, which only
runs when a batch is consumed. Two consequences:

1. A standalone deployment with the default `persisted>=2!` is in fact
   running clamped (`view.len()=1`, target=2 → degrade clamps to 1).
   `evaluate_durability` correctly reports `degraded=true` — but the
   flag is not written until the first batch flows through the gate.
   `/healthz` polled before any traffic shows `policy_degraded=0`.
2. A quiet exchange overnight that loses a replica won't see the warn
   re-emit or the `/healthz` gauge flip until traffic resumes. Operator
   alerting can lag by minutes/hours.

Fix:

- Initialize the flag at server startup by evaluating the policy
  against the initial cluster shape (primary alone if standalone,
  primary + 0 connected replicas otherwise).
- Move the periodic warn to a wall-clock timer that runs regardless
  of gate activity. Either piggyback on the existing heartbeat scan
  in the response stage's idle path, or have the health-endpoint
  thread emit it.

### Operator-facing docs reference deleted flags

`docs/operations.md:74-76`, `docs/replication.md:55, 61, 65, 69-89,
150, 189`, and `docs/roadmap.md:15` all describe `--no-quorum-durability`
and `--async-replica-ack` as live, working flags with detailed
durability-tradeoff prose. Both flags are gone.

Fix:

- Replace the flag rows in `operations.md` with a `--durability-policy`
  row.
- Rewrite the `docs/replication.md` durability section around the
  policy framework. Drop the async-replica-ack section or replace it
  with a forward-reference to the planned ack-on-receive work.
- Update `docs/roadmap.md` item #4: either mark complete or rewrite the
  description to reflect what landed (the description still references
  three durability levels including the obsolete `journaled` /
  `fsynced` framing).

### Bench script breaks immediately

`scripts/lan-bench-suite.sh:171` defaults
`REPLICA_EXTRA_ARGS=--async-replica-ack`. The new server rejects this
as an unknown CLI argument, so any LAN bench run fails before the
first server starts.

Fix: drop the default flag from the bench script. Note in the comment
above that the previous `--async-replica-ack` optimisation is not yet
reachable through the policy framework (see the next item).

### Bench-number regression vs published latency figures

The legacy `--async-replica-ack` removed ~50–80 µs of fsync round-trip
from the replication path (`docs/replication.md:65`). With the flag
gone (`async_ack` hardcoded to `false` at the three receiver call
sites), the receiver gates ack send on the local journal cursor — same
behaviour as the pre-async-ack baseline.

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

### Misleading "strictly stronger" claim

`crates/server/src/durability_policy.rs:94-96` claims `persisted>=2!`
is strictly stronger than legacy auto-degrade in degraded mode. This
is true only for **1+2** clusters (where degrade clamps to 2-of-2,
beating legacy's drop to 1). In **1+1** clusters the new and legacy
behaviours are equivalent in both healthy and degraded states.

Fix: rewrite the claim to spell out the cluster-shape dependency. The
strictly-stronger property holds for "deployments with ≥2 connected
replicas at policy authoring time"; in 1+1 the new code matches legacy
exactly.

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

1. P0 (B1, B2, wire byte-pattern test) on this branch before merge.
2. P1 (idle staleness, doc updates, bench script, doc claim, async-ack
   regression note) on this branch before merge — operator-facing
   surface needs to match what shipped.
3. P2 on a follow-up branch.
4. P3 driven by buyer feedback / commercial roadmap.
