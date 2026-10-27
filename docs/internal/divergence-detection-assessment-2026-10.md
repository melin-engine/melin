# Divergence detection: a critical assessment of the plan

Status: **review**. Assesses [divergence-detection-2026-10.md](divergence-detection-2026-10.md)
as it stands on branch `plan/state-divergence-detection` at 25f75166.
Every claim below was
checked against the plan's text and the code at that commit, and is cited
as `file:line`. The plan's own line numbers are cited as `plan:N`, against
the working tree (25f75166 plus the uncommitted removal of one line from
the step-7 roadmap text at plan:1846); against 25f75166 itself, citations
past plan:1846 are one line later. Bare file names are in one of two
crates: `pipeline.rs`, `dispatch.rs`, `journaled_app.rs`, `shadow.rs`,
`node_store.rs` and `replication/archive.rs` and `replication/catchup.rs`
are in `crates/core/transport-core/src`; `halt.rs`, `response_loop.rs`,
`response.rs`, `dpdk_response.rs`, `server.rs`, `replication/mod.rs`,
`replication/tcp_sender.rs` and `replication/join_worker.rs` are in
`crates/core/server-runtime/src`. Where a
point depends on the Melin Exchange Core, which is maintained separately and
is not in this repo, it says "unknown".

The plan is careful and mostly sound. Its chain, its anchor and pipeline-id
rules and its stop ordering hold up under the cases checked here. Its
weaknesses are elsewhere: one cost it does not see (replication ring slots),
two statements about what happens after a divergence that do not hold
(the halt under `disk`, which the code contradicts, and who is left
running when the primary is the faulty node, which the plan's own
decision 10 contradicts), and gaps (the encode buffer's stale bytes,
the tick trap) that will make the first rollout stop healthy applications
for reasons other than a broken `apply`.

## Summary, ranked by harm to an operator

| # | Item | Group | Severity | Before implementation? |
| --- | --- | --- | --- | --- |
| 1 | Digest frames take a replication-ring slot each, up to one per fsync batch. That can halve the headroom before a replica is evicted | Performance | High | **Yes** |
| 2 | "Under `disk` the primary keeps serving alone" is wrong: the halt applies under every policy | Correctness of the guarantee text | High | **Yes** (text) |
| 3 | When the primary is at fault, the plan stops the healthy copies, archives their good snapshots and tells the operator to reseed them from the faulty primary | Correctness / operations | High | **Yes** |
| 4 | The encode buffer is shared by reports and queries, so an encoder that leaves bytes unwritten folds stale bytes, which differ by copy | Correctness | High | **Yes** |
| 5 | First rollout: the only fallback from enforcement is `--no-output-digests`, which gives up detection entirely | Operations | Medium-High | Decide now |
| 6 | The tick trap is wider than finding 7 says: replica restarts and promotions trigger it too, with no clock step back needed | Correctness caveat | Medium-High | Confirms open question 8 (b); adds a test |
| 7 | The snapshot/restore check is narrower than claimed: one round trip per pipeline start, none at all if `clone_via_snapshot` is overridden | Coverage | Medium | Text and runbook |
| 8 | Replica-side stop plumbing: the replica's shadow holds only the pipeline's shutdown flag | Implementation risk | Medium | **Yes** (design) |
| 9 | Replication protocol 9 is another full-cluster-stop upgrade | Operations | Medium | Decide now |
| 10 | Build-id gating silently stops comparing on mixed-arch and per-host builds, and nothing alertable shows it | Coverage / operations | Medium | Decide now |
| 11 | Applications that already break the hash-map-order rule (report order following capacity) will now be stopped: the shadow is restored and never prefaulted, and `Sizing` varies by node | Coverage caveat (rollout) | Medium | Document; shadow prefault and test in step 3 |
| 12 | Latent state divergence can be promoted at failover | Coverage | Medium | Defer (step 7) |
| 13 | Uncovered windows around restarts and promotions; the failover tail | Coverage | Medium | Defer (step 7) |
| 14 | Fold placement: after the send it still delays the next reply in the batch | Performance | Medium | Measure (step 6) |
| 15 | Encode on the replica drain and on the shadow: both are back-pressure points | Performance | Medium | Measure (step 6) |
| 16 | Windows close only on a `Report` slot or an empty ring, so they close late under report-less load | Coverage / detection delay | Low-Medium | **Yes** (one-line design change) |
| 17 | Queries are not covered, and `QueryCtx` makes a query check hard | Coverage | Low-Medium | Document |
| 18 | Idle detection delay is bounded in ticks; unbounded with ticks off | Coverage | Low-Medium | Document |
| 19 | Chain break under report-less traffic with a stalled drain (open question 11) | Coverage | Low | Defer |
| 20 | Downgrade ignores the marker | Operations | Low-Medium | Document, plus a convention |
| 21 | API breaks, the API review's "additive" label, and a name clash in `ArchiveReason` | Operations | Low | **Yes** (the rename); the rest is text |
| 22 | Comparator complexity, and "cannot compile" claims that are only lints | Complexity | Medium | **Yes** (the comparator calls `trip`); the rest in design review |
| 23 | Smaller implementation traps (allocating sibling, encode-error logs at `error!`, notice aggregation, the board's shape) | Complexity | Low-Medium | **Yes** (stack buffer, encode-error logging); the rest in the steps |
| 24 | Room for improvement: majority verdict from notices, staleness metrics, offline qualification, a debug encoder check | Improvement | — | Some now, some later |

Items 1 to 4, 8 and 16, and the parts of 21, 22 and 23 marked above, should
change the plan before step 1 starts. Items 5, 9 and 10 are decisions to
take now, because they shape the flag, the protocol and the metrics of
steps 2 to 5. The rest can be documented or deferred.

## 1. Correctness and guarantee gaps

### 1.1 The halt applies under every ack policy, `disk` included (item 2)

**Shortfall.** The plan says a nondeterministic primary stops every
replica and "under a replica-backed ack policy, the primary then halts
writes for want of a replica; under `disk` it keeps serving alone"
(plan:1796-1800). It says the same in step 5's changelog line
(plan:1754-1756) and in open question 3 (plan:1929-1930). The code says
otherwise. `halt.rs:14-16`: "The halt applies under every ack policy,
`disk` included". `docs/replication.md:174-179`: "Independent of the ack
gate, the node halts when every configured replica disconnects."
`HaltGate::verdict` (`halt.rs:332-344`) refuses on `presence.halted()`
whatever the policy. Only a standalone node is exempt (`halt.rs:80-82`,
`docs/replication.md:218-219`).

**Why it matters.** The runbook is the document an operator reads during
the incident. Under `disk` it would tell them their primary is still
taking writes when it is refusing them. Clients see `ReplicaDisconnected`
(`halt.rs:7`), a reason that names the wrong cause. The single-replica
case is also wider than a replica-backed ack policy: under *any* policy, a node
with replication configured and one replica halts writes on that
replica's divergence. This includes a replica-only fault such as a memory
error on the replica's host.

**Options.** (a) Correct the text: every node with replication
configured halts once its last replica stops, whatever the policy. Under
every policy it refuses new writes and answers queries. Under a
replica-backed policy its held replies go out degraded after the grace
period (`halt.rs:72-76`); under `disk` its own fsync confirms them, so the
policy itself releases them and degraded release does not apply
(`docs/internal/roadmap.md`, "Degraded acks on a halted node"). (b) Also add
a halt cause, so `/healthz` and the reject reason can say "replicas
stopped for divergence". A new `RejectReason` variant is a breaking
change to an exhaustive public enum, so the `/healthz` cause alone is the
cheaper half.

**Recommendation.** (a) now, in the plan's guarantee, consequences, step
5 changelog and open question 3. (b)'s `/healthz` half as part of step 5.

### 1.2 When the primary is the faulty node (item 3)

**Shortfall.** The plan's picture of a faulty primary is "it stops every
replica and is never stopped automatically" (open question 3,
plan:1927-1932). That picture is incomplete in both directions, and the
recovery path it gives is the wrong one for this case.

- **The primary is often stopped, by its own shadow.** Most real sources
  of nondeterminism differ between two copies in one process: a randomly
  seeded `HashMap` (each instance draws its own keys), an allocation
  address, a wall-clock read (the shadow applies later than live). With a
  shadow configured (the default, `snapshot_interval_ms: 3_000_000`,
  `server.rs:588`), the primary's shadow check (decision 10) trips on
  these too. So does each replica's own shadow. A nondeterministic `apply`
  therefore tends to stop **every node**, each with a marker, and raft
  loses quorum. That is the plan working as designed, but the plan never
  says the outcome is a full-cluster, manual, every-node recovery. What
  leaves the primary running is nondeterminism that is *deterministic
  within one host*: the restart tick trap (item 6), a per-node input (node
  identity, local config, `Sizing`), a library difference between hosts,
  a replica-side memory fault.
- **The healthy copies' good snapshots are archived.** When the replicas
  are right and the primary is wrong, each replica archives every snapshot
  above its last verified position (decision 11 step 5, plan:1033-1048).
  Those are exactly the snapshots the recovery wants.
- **The refusal message points at the wrong copy.** It tells the operator
  to "rebuild this node's state (reseed it from the current primary, or
  replay its journal)" (plan:1129-1131). When several replicas reported the
  same window, reseeding them from the current primary copies the faulty
  state into the healthy nodes. The message mentions "several replicas
  reporting the same window point at the primary", but its instruction
  does not change with that.
- **Replay may be impossible.** A replica bootstrapped from a primary
  snapshot has no journal before that snapshot. `docs/journal.md:204`
  covers a node that "starts from a snapshot alone, with no journal
  segment left". With every snapshot archived ("none" threshold), "replay
  its journal" is not available to it, and the message does not say so.

**Why it matters.** The operator scenario is the worst one the feature
creates. Two replicas stop, refuse to restart and lose their snapshots.
The one node still up is the one whose output is wrong, and it is
answering queries and, under a replica-backed policy, releasing held
replies degraded after the grace period (`halt.rs:72-82`).
If the operator follows the printed advice, the fault spreads to the
healthy nodes.

**Options.**
- (a) Accept the behaviour, fix the text. The refusal message and the
  runbook branch on "who is at fault". On a primary-fault verdict: do not
  reseed from it, restore the archived snapshots (they are good), clear the
  markers, promote. State whether the local journal covers genesis, so
  "replay" is offered only where it can work.
- (b) Majority verdict from the notices the plan already sends. Each
  `Diverged` frame carries `window_end`, `expected_window` and
  `actual_window` (decision 8). Two replicas that report the same
  `window_end` with equal `actual_window` and a different `expected_window`
  agree with each other against the primary. That is a two-against-one
  verdict, needing no new wire field and no replica-to-replica link. The
  primary can then trip its own latch (it is the odd one out) and log it.
  It is a change of scope: the plan excludes "majority verdicts" by name
  (plan:2031). It is also partial. The primary forwards only the newest
  closed window per batch and skips the rest (decision 7, plan:866-871),
  so under load a one-off divergence in a skipped window is caught through
  the chain alone, and the window that is reported matches in its own
  digest. The fallback, comparing `actual_chain`, works only when both
  replicas anchored their runs at the same point: a replica that anchored
  after the divergence inherited the primary's chain value and agrees with
  it. So the verdict decides persistent divergences, and one-off ones
  between replicas that anchored together. Anything else stays "likely".
  The verdict is also only as reliable as notice delivery: the plan sends
  the notice best effort, right before the session ends (decision 11 step
  4, plan:1031-1032), so a replica that stops before the frame leaves, or
  a torn-down DPDK session, turns two-against-one into "one replica
  reported". The primary should log when a replica disconnects without a
  notice shortly after a peer reported one.
- (c) On top of (b), replicas defer their exit: trip the latch (no
  promotion, no client, as today), send the notice, and wait a bounded
  time for a primary verdict before writing the marker. If the primary
  declares itself at fault, they stay up and one is promoted. This brings
  back the automatic failover the plan rules out, at the cost of a new
  primary-to-replica verdict frame and a timed state on the replica.
- (d) Open question 3's current recommendation: no automatic stop, page
  the operator.

**Recommendation.** (a) before implementation: it is text, and the wrong
advice is a correctness hazard. (b) in step 5: it costs a comparison of
two 16-byte values on a cold path, and it turns the plan's `error!` from
"likely" into a verdict in the cases above, with two or more replicas.
Record it as a deliberate widening of the plan's scope. Leave (c)
as a follow-up. It is the right end state for a product sold on automatic
failover, but it deserves its own plan. With one replica, (b) cannot
decide. Say so, and keep (d) for that case.

### 1.3 Report order that follows capacity will now be caught (item 11)

**Shortfall.** The digest folds reports "in output order" (decision 1,
plan:317), so a different report order is a divergence. The contract
already forbids an order that follows capacity.
`docs/building-an-application.md:374`: "even with a fixed hasher the order
depends on capacity, which a node's memory sizing changes. If what you do
depends on the order you visit entries — which one fills first, which
report comes first — use an ordered structure". The production checklist
repeats it (`docs/building-an-application.md:665`), and `prefault`'s
rustdoc requires that "every decision `apply` would make afterwards is the
same" (`app/src/lib.rs:510-511`). Its sentence "a rebuild may change
iteration order" is about snapshot bytes, which the plan does not compare.
So this is not a contract gap. It is a rollout caveat: an application that
already breaks this rule has diverged silently until now, and will be
stopped from upgrade day. Three code facts make capacity differ between
copies, so the rule is easy to break without noticing:

- the shadow is a `clone_via_snapshot` of the live app (`server.rs:1777-1782`,
  `replication/mod.rs:589-590`), restored at whatever capacity `restore`
  chooses and never prefaulted, while live is prefaulted (`replication/mod.rs:589`);
- `Sizing` is per node (`replication/mod.rs:575-577`). The plan itself
  calls it "the one input a test may vary per node" (plan:1509-1510);
- a snapshot-restored instance is "sized only after" restore
  (`app/src/lib.rs:506-507`), unlike a replayed one.

An application that emits several reports by iterating a hash map with a
fixed hasher emits them in an order that depends on bucket count. It
breaks a written rule, but nothing has caught it before. On upgrade day
its shadow check trips.

**Why it matters.** The stop is correct: clients connected to different
nodes after a failover would see a different report order. To the operator
it still looks like an upgrade-day outage, because the application "worked
yesterday". Whether the Exchange Core emits reports in iteration order is
unknown.

**Options.** (a) Accept and document: name it in the rollout notes, and
make the rule more visible by repeating it in `apply`'s rustdoc, which
needs no **Changed** entry because the contract does not change. (b) Sort each event's reports
before folding. This is wrong: clients receive them in emission order, so
the digest would no longer cover what clients see. (c) Fold each event's
reports as a multiset (an order-independent combination). It has the same
flaw as (b), and order is client-visible. (d) Also prefault the shadow
after the clone, which brings it closer to live but does nothing for
per-node `Sizing`.

**Recommendation.** (a), plus (d) as cheap hygiene (it is not required for
correctness, because a compliant application is unaffected). Add a fault-injecting
test application whose reports follow capacity-dependent iteration, to pin
that the check catches it (step 3). The rollout notes should name this as
the second suspect, after the tick rule.

### 1.4 The shared encode buffer folds stale bytes (item 4)

**Shortfall.** On the primary, reports and query responses are encoded
into one reused stack buffer (`response_loop.rs:493`, `encode_slot_payload`
at `response_frame.rs:76-91`). The encoder returns a length
(`encoder.rs:41-46`), and nothing says it must write every byte below
that length. An encoder that skips a reserved or padding field leaves
whatever the previous encode wrote there. On the primary that may be a
query response. On a replica (no queries) and in the shadow it is a
different history. The plan's clause, "a pure function of the report"
(plan:335-339), arguably covers this, but no implementer will read it that
way. Clients that ignore reserved bytes have never noticed.

**Why it matters.** It is a false divergence with no bug in `apply`, it
depends on traffic mix, and it is hard to diagnose from a marker.

**Options.** (a) State the rule: every byte in `buf[..n]` is written by
the encoder. Then enforce it in debug builds: the folder fills the body
region with a poison pattern, encodes twice with two patterns, and asserts
equal output. The cost is debug-only, and tests then catch it. (b) Zero
the whole body region (`MAX_RESPONSE_BODY`, 512 bytes,
`response_frame.rs:25`) before each encode in release, or the high-water
mark of the longest body written so far. Zeroing only the previous
encode's length is not enough: the buffer is reused across every encode,
so a stale byte can come from any earlier one (encode A writes 100 bytes,
B writes 10, C returns 50 but skips bytes 10..20, and A's bytes survive
there). It is one `memset` of at most 512 bytes per report on every copy:
cheap, removes the class at runtime, and leaves the contract as written. (c) Give each copy separate buffers for reports and
queries. That fixes the query mix but not reuse across reports.

**Recommendation.** (b) in `encode_report_body` (decision 4), so the class
cannot occur. Add (a)'s debug double-encode as a check on the encoder's
purity in general: it also catches an encoder with internal state.

### 1.5 The tick trap is wider than finding 7 states (item 6)

**Shortfall.** Finding 7 frames the trap as "a primary restarted after a
clock step back" and says step 5 is "the first to compare a restarted
primary with a replica that kept running" (plan:270-276, 292-293). The
code makes it wider:

- **Every live stage starts its watermark at 0** (`pipeline.rs:2839`), on
  a replica too, and after replay (`journaled_app.rs:293` is replay's own).
  The first live event after *any* restart calls `tick(t)` when `t > 0`
  (`dispatch.rs:72-75`). The copy that kept running calls it only when
  `t` is above its own watermark. Under load, consecutive events share a
  timestamp, so a restarted **replica** whose first live event repeats the
  previous timestamp makes a call the primary did not. No clock step is
  needed. The rule in `Application::tick` covers equal times ("the same
  value may arrive more than once", `app/src/lib.rs:450-459`), so a
  rule-following application is safe. One that fires work on `>=` is not.
- **Promotion** builds a new matching stage with watermark 0, stamping its
  first events from its own clock. If that clock is behind the old
  primary's last timestamp, the new primary calls `tick` for a time the
  following replicas skip. This happens at a failover, which is exactly
  when an operator least wants a second stop.
- Every journaled `Tick` calls `tick` twice (`dispatch.rs:72-85`). The two
  calls are the same on every copy, so they are not a divergence in
  themselves.

The plan's no-false-positive tests use a rule-following test application
(plan:1733-1741). They show the check does not misfire on good
applications. They cannot show the trap, and the plan lists the trap as
untested because the tick generator has no clock seam (plan:1769-1774).
The replica-restart variant needs no clock seam.

**Why it matters.** Until monotonic time lands
([monotonic-sequencer-time-2026-09.md](monotonic-sequencer-time-2026-09.md),
"proposed, not started"), routine operations can trip the cross-node check
for applications whose `tick` is not idempotent on equal or earlier times.
The roadmap says the Exchange Core's `tick` "stamps the clock its rate
limiter reads" (`docs/internal/roadmap.md:29`). Whether that is
idempotent on an equal time is unknown.

**Options.** (a) Open question 8's (b), as the plan recommends: release
step 5 only with or after monotonic time. (b) Ship step 5 earlier and rely
on the contract and the runbook. (c) A narrow interim fix: seed the live
stage's watermark from the last journaled timestamp at boot and promotion.
That closes the restart and equal-timestamp cases without the journal
format change monotonic time needs, but leaves backwards ticks.

**Recommendation.** Keep (a), and widen finding 7's text to replica
restarts, promotions and equal timestamps. Add one step-5 test that does
not need a clock seam: a replica restarted under load, running a test
application whose `tick` counts calls into its reports. It should be
expected to diverge while monotonic time is open and to stop diverging
once it lands, so the dependency is pinned by a test rather than by prose.
Consider (c) only if monotonic time slips by more than a release.

### 1.6 The snapshot/restore check is narrower than claimed (item 7)

**Shortfall.** Decision 10 says the shadow catches "a `snapshot`/`restore`
pair that drops a field, which is exactly the copy a restart or a resync
would come back from" (plan:987-991). In practice:

- The round trip happens once per pipeline start (`clone_via_snapshot`,
  `app/src/lib.rs:530-546`). A field that is empty at that moment and
  filled later is never round-tripped in-process. Every later snapshot the
  shadow saves can drop it unseen.
- `clone_via_snapshot` may be overridden "if a cheaper same-process clone
  is possible" (`app/src/lib.rs:525-527`). With a plain clone, the
  in-process check does not exercise `snapshot`/`restore` at all.
- After a restart from a lossy snapshot, live and the new shadow both lack
  the field, so they agree. Only the cross-node check sees it, and it
  stops the *other* node when that node is the one holding the full state.
  This is the same "healthy node stops" shape as item 1.2. A replica
  re-seeded from such a snapshot (snapshot transfer) inherits the loss.
- The way-back procedure boots from "the snapshot it kept at or below the
  verified position" (plan:1497-1503). When the fault is in `snapshot()`,
  that kept snapshot is also lossy. Its step-3 test switches the fault off
  on restart (plan:1502-1503), so it does not exercise this case.

**Options.** (a) State the limit: snapshot fidelity is checked for state
present at pipeline start, and only through the default
`clone_via_snapshot`. (b) Have the shadow round-trip its own state
periodically, off the hot path, and compare its own output before and
after for a window. This costs a snapshot-sized allocation and a pause on
the shadow, which already pauses to save. (c) The step-7 state digest,
which compares state rather than output and so catches the drop before
any report shows it.

**Recommendation.** (a) now, in the guarantee and the runbook: "if the
divergence is in snapshot handling, discard all snapshots and reseed or
replay from genesis". (c) is the real fix. (b) is a reasonable middle step
if the state digest is far off.

### 1.7 Latent divergence and failover (item 12)

**Confirmed.** Stated by the plan itself (plan:47-53, 1834-1848). Raft
already elects the most caught-up node. Nothing prefers the node whose
output was verified most recently, and nothing could see a latent
difference anyway.

**Options.** (a) Accept and document (the plan's position). (b) At
promotion, prefer the candidate with the newest `verified_through` among
equally caught-up nodes. That is cheap, but it reaches raft's election,
which the plan keeps untouched. (c) The state digest (step 7).

**Recommendation.** (a) with (c) on the roadmap, as planned. (b) is not
worth reaching into raft for: it narrows only the tail, not latent state.

## 2. Coverage gaps and caveats

### 2.1 Windows around restarts and promotions, and the tail (item 13)

**Confirmed and correctly stated** (plan:54-67, 76-77, decision 13). The
in-process check covers those windows on each node (decision 10), except
for differences that are deterministic within one host (item 1.2). The
roadmap's "chain stored in snapshots" (plan:1850-1866) closes the
cross-node part. One refinement: the failover tail is not just "windows
not yet compared". The primary forwards a window only once its response
stage has folded it, after the ack gate, at the next fsync batch
(decision 7). So the tail always includes at least the replica-ack and
fsync latency's worth of windows, on top of `K`.

**Recommendation.** Accept and document. Defer the stored chain.

### 2.2 Build-id gating (item 10)

**Shortfall.** Open question 2 (b) compares only when both ends run the
same build, identified by a BLAKE3 hash of the executable (plan:1907-1926).
So:

- a mixed x86/ARM cluster built from the same source never compares;
- binaries built per host without reproducible-build flags (paths and
  build metadata end up in the binary) never compare, although the source
  is identical;
- the state shows only as `verified_through` standing still and a `warn!`
  once per session (plan:1211, 1230). There is no gauge an operator can
  alert on, and `unverified{reason="build"}` grows only *after* the builds
  converge (plan:1230).
- Conversely, the same binary on hosts with different shared libraries
  (the system math library, for an application using floating-point
  transcendental functions) compares, and can diverge for real. That is a
  correct stop, but a surprising one. Whether the Exchange Core uses such
  functions is unknown.

**Options.** (a) Keep the executable hash (it is the only id that covers
the application's code without its cooperation, since a source id taken
in `server-runtime`'s build script cannot see the application's commit).
Add an operator override, `--output-build-id <string>`, for operators who
know two binaries are equivalent (mixed architecture, per-host builds).
(b) An id from source: crate versions, lockfile hash, compiler, features.
It needs the application to contribute its own id, which is the trait
surface the plan avoids. (c) Compare across builds anyway, and on mismatch
log instead of stopping. That is an observe mode scoped to mixed builds
(see 3.2).

**Recommendation.** (a), plus a gauge `melin_output_comparing{against}`
(1 when a compared session is live, 0 otherwise) with the reason in
`/healthz`, so "not comparing" can be alerted on directly. A mixed-arch
cluster that diverges is a real failover hazard, and comparing it is
desirable: the override lets the operator ask for exactly that.

### 2.3 Queries (item 17)

**Confirmed.** Queries are answered on the primary only and not journaled
(`app/src/lib.rs:417-437`, `shadow.rs:128-130`). One refinement: a query
check is harder than "nobody else answers it". The shadow does see every
query in the input ring, in order, and could answer it from identical
state. But `QueryCtx` hands the handler node-local and timing-dependent
values (`journal_sequence` from the durable cursor, `active_connections`,
`events_processed`; `pipeline.rs:2944-2949`), so query answers are allowed
to differ between copies today.

**Options.** (a) Accept and document (the plan). (b) An opt-in in-process
query digest: a new kind, folded by the shadow and the response stage, for
applications that declare their queries free of `QueryCtx` counters. It
costs the shadow one `query` call per query. (c) Fold queries with the
`QueryCtx` fields masked. This does not work: the runtime cannot know
which bytes came from them.

**Recommendation.** (a) now. Note (b) on the roadmap: the kind byte and
the comparator already fit it.

### 2.4 Idle detection delay (item 18)

**Confirmed.** `K` tick intervals plus one, about 16 s at the default 250 ms
(`server.rs:590`), and unbounded with `--tick-interval-ms 0` (plan:68-75,
445-453). One refinement: the bound also requires the response stage to
reach its idle path, so it holds only while the output ring is empty
(next item).

**Options.** (a) Document, as planned. (b) Close a window early when idle:
pad an idle window by declaring it closed at a tick, which needs the grid
to be per-window variable. That breaks "positions alone" and is not worth
it. (c) A smaller `K` (open question 9).

**Recommendation.** (a).

### 2.5 Windows close only on a `Report` slot or an empty ring (item 16)

**Shortfall.** Decision 4's table closes a window when "a `Report` slot
past the window is read, or the output ring is empty" (plan:591). On a
busy primary whose requests produce no reports, the ring holds `BatchEnd`
slots (`pipeline.rs:3094-3118`), so it is rarely empty, and windows close
late. Any slot carries a `wire_seq` that every earlier report precedes in
ring order: a `BatchEnd` at `w`, and a query response stamped with the
prior position (`pipeline.rs:2980-2981`).

**Recommendation.** Close every window ending below any slot's `wire_seq`,
and at or below a query slot's. It is one comparison per slot and needs
no change to the matching stage.

### 2.6 Chain breaks (item 19)

**Numbering.** Chain breaks are the plan's open question 11
(plan:1992-2018), the last of its eleven; there is no open question 12.

**Confirmed.** A lap needs a drain stall of about `R·K` entries of
report-less traffic, which with both rings at `1 << 20`
(`pipeline.rs:247, 252`) is about two million entries. It is counted, never
a false claim. One refinement: the drain's wait strategy can be
`SpinThenYield` on a shared core (`wait.rs:41-48`), so a small replica host
that places the drain beside a busy thread makes the stall likelier than
"descheduled" suggests.

**Recommendation.** Keep (a), as the plan does. Step 6 should run its
report-less catch-up measurement with the drain on a shared core too.

## 3. Operational and upgrade concerns

### 3.1 Replication protocol 9 is a flag-day upgrade (item 9)

**Confirmed.** The handshake requires the exact version
(`protocol.rs:603-609`, `REPL_PROTOCOL_VERSION = 8` at `protocol.rs:60`).
The upgrade notes say to stop the whole cluster (`docs/replication.md:1129-1154`).
Protocols 6, 7 and 8 were the same, so protocol 9 is the norm, not a
regression. Raft frames carry no version byte (`raft/src/wire.rs`: serde
enums, with only the error string chosen to survive versions), but this
plan does not touch raft, so raft matters only to a general rolling-upgrade
path.

**Options.** (a) Accept and document: one more cluster stop. (b) Add
negotiation in protocol 9, so 9 is the last flag day: the replica
advertises the highest version it speaks, the primary accepts N and N−1
and sends only what both understand. A digest-less session is already a
supported state (`SessionKind::Uncompared`), so a v8 replica is simply
uncompared. (c) The full zero-downtime path: (b), a raft frame version, a
planned-switchover command, and cluster-version activation for journaled
features such as monotonic time and the encoding version. That is a
roadmap item of its own.

**Recommendation.** (b), because this bump already rewrites the
`StreamStart` layout and adds a capability byte (`digest_kinds`). Paying
for negotiation once here costs less than the next bump. (c) belongs on
the roadmap, not in this plan.

### 3.2 First rollout: enforcement with only an off switch (item 5)

**Shortfall.** The flag is binary (`--no-output-digests`, plan:454-461).
Items 1.4 and 1.5 are ways an application that follows today's contract
stops on upgrade day. Item 1.3 is a way an application that already breaks
a written rule, unnoticed until now, stops on upgrade day. An operator who
hits any of them has two choices: stay down, or turn digests off, after
which nothing is detected at all.

**Weighing the stance.** The user's position is that nondeterminism breaks
the contract and an outage is acceptable. That holds for a broken `apply`,
and it holds for item 1.3, whose rule is already written down. It holds
less well where this plan introduces the contract (the encoder determinism
clause and the buffer bytes it covers, item 1.4) or where the runtime
causes the stop (the tick trap, item 1.5). There the application was
correct yesterday. An observe mode would not weaken the default. It would
replace "off" as the fallback.

**Options.** (a) As planned: enforce, or off. (b) A three-state
`--output-digests enforce|observe|off`, default `enforce`. Observe
compares and counts, logs `error!` and sets a `/healthz` flag, but does
not trip the latch or write a marker. It is not a silent mode: the
`warn!` at startup and the gauge make it alertable. (c) An offline
qualification tool, run before the upgrade: replay a production journal
through two pipelines (or a pipeline and its shadow) with digests and
report the first divergent window. It runs off production, catches most
in-process nondeterminism, and gives the operator a go/no-go before
upgrade day. (d) Release step 3 (the in-process check) one release ahead
of step 5, so applications meet the stricter contract on a single node
before cross-node stops exist.

**Recommendation.** (b) and (c). (b) keeps the stance (the default
stops) while making the fallback strictly better than off. Add a rule
that observe mode is refused at startup on a node that already has a
marker, so it cannot be used to skip the refusal. (c) is the
operator-facing answer to "will my application survive the upgrade".
(d) happens anyway under open question 8 (b).

### 3.3 The refusal and the off switch

**Shortfall (wording, not design).** The plan says `--no-output-digests`
turns "every folder and every check on the node off" (plan:455). The
design already makes the refusal unconditional: every node entry calls
`refuse_if_diverged` right after `resolve` and before it opens its journal
(plan:1104-1106), in `ServerConfig::startup_store()`, "the one place the
refusal is made" (plan:1117), and nothing there consults the flag. But
plan:455 read alone suggests the off switch disables every divergence
behaviour, and an implementer who gates the new startup step on the flag
would turn the off switch into a way around the marker.

**Recommendation.** Say explicitly beside plan:455 that the flag does not
reach the refusal (decision 12), and pin it with a step-3 test: a node
with a marker and `--no-output-digests` still refuses to start.

### 3.4 Downgrade ignores the marker (item 20)

**Confirmed.** A pre-plan binary does not know `<journal>.diverged`, so
rolling back the release brings a diverged node back silently. There is no
safe way to make an old binary refuse: renaming the journal so it cannot
be found would make an old primary start a new history, which is far
worse.

**Options.** (a) Document: a rollback past this release must first
handle each marker by hand. (b) Introduce a generic convention now, one
refusal file name that every future release checks
(`<journal>.refuse`, with a reason line). The divergence marker is its
first user, so later stop-causes need no new startup check and stay
visible to releases from this one on.

**Recommendation.** Both. (b) is cheap and only pays off if done in the
first release that has a marker.

### 3.5 API breaks (item 21)

**Correction.** `Egress::send_slot` is not a public break: `Egress` is
`pub(crate)` (`response_loop.rs:92`). The public breaks are:

- `ServerConfig` gains fields. It implements `Default` (`server.rs:537`),
  so only exhaustive struct literals break;
- `NodeStoreError::Diverged` (`node_store.rs:43`, re-exported at
  `transport-core/src/lib.rs:83`);
- `ArchiveReason::Diverged` (`replication/archive.rs:24`, public through
  `replication::archive`). The enum already has `ArchiveReason::Divergent`,
  for a journal that forked from the primary's after a `HashMismatch`
  (`replication/archive.rs:24-27`). The plan adds `Diverged` beside it
  (plan:1045). That gives two near-identical names for different causes,
  and both end up in the archive directory names an operator triages
  (`replication/archive.rs:20-21`). Name the new one for its cause, for
  example `OutputDiverged`, with a tag to match;
- `build_pipeline_with_replication` takes a `PipelineId` (`pipeline.rs:3521`);
- `hex_prefix` widens from `&[u8; 32]` to `&[u8]` (`journal/src/error.rs:9`).
  Calls coerce, but a use as a function pointer of the old type does not;
- the encoder's determinism clause, a behaviour change with no compile-time
  check.

The API review lists S8 as "No (additive)" for breaking
(`application-api-review-2026-09.md:48`). The plan is not additive: it
bumps the protocol and changes public enums and contracts.

**Recommendation.** Update S8's row when step 6 points it at the plan.
For the encoder clause, item 1.4's debug double-encode is the closest
thing to a compile-time check available.

## 4. Performance risks

### 4.1 Digest frames take replication-ring slots (item 1)

**Shortfall.** The replication ring is counted in slots, one publish per
slot, whatever its size: `REPLICATION_RING_CAPACITY = 1 << 8`
(`journal/src/replication.rs:37-42`). `try_publish` claims a whole slot
for any byte count (`replication.rs:159-176`), and a full ring evicts the
replica (`pipeline.rs:1836-1849`). The chain check publishes one frame
every `CHAIN_CHECK_INTERVAL_BATCHES = 64` batches (`pipeline.rs:920`).
Decision 7 publishes a digest frame "at most one per batch", whenever a
window newer than the last forwarded one has closed (plan:866-871). Under
load with batches wider than `K`, that is nearly every batch. Ring slots
consumed per batch then go from about one to about two, which roughly
halves the time a replica can stall before it is evicted.

**Why it matters.** The roadmap's open item "A replica that falls behind
at full rate cannot rejoin" (`roadmap.md:15`) already shows evictions at
full rate on this exact ring, with only tens of milliseconds of headroom
at the measured batch sizes, and an eviction that then loops. Halving that headroom turns
a measured, intermittent problem into a likely one, on the deployments
that need replication most. Neither decision 15's cost table nor step 6's
measurement looks at ring occupancy.

**Options.** (a) Rate-limit: forward a digest at most every `N` batches,
or only when at least `M` windows have closed since the last. The chain
makes sampling free in coverage and costs only delay (decision 3). (b)
Piggyback: write the digest frame at the front of the next data batch's
chunk instead of in a slot of its own. It still follows the entries it
covers, its chunk's `end_seq` classifies it with that batch during
catch-up, and it costs no slot. It does need the batch encoder to reserve
42 bytes in the chunk. (c) Fix the ring to count bytes or entries (the
roadmap item), which makes small frames cheap for everyone.

**Recommendation.** (b) if the journal stage's chunk assembly allows it
cleanly, otherwise (a) with `N` chosen so the added delay stays within one
`K`-window at the bench's batch rate. Either way, add "replication ring
slots per batch" and "evictions" to step 6's measurement. Do not make this
plan wait on (c).

### 4.2 The fold on the primary's response thread (item 14)

**Shortfall.** Decision 4 folds "after the send so that the hash is not on
the reply's own latency, only on the stage's throughput" (plan:609-611).
It is still on the latency of the *next* slot in the same batch: the loop
sends slot by slot (`response_loop.rs:700-855`) and gives the egress a
turn between slots (`after_slot`, `response_loop.rs:851-854`). The DPDK
egress flushes the slot's lane there every time
(`dpdk_response.rs:378-382`), so each fold delays the next slot's send.
The kernel egress only flushes there when a flush is due
(`response.rs:686-688`). Its steady-state flush waits for the output ring
to empty (`response.rs:692-694`), so a fold on kernel TCP delays the
batch's flush itself, which is worse. Either way, in a burst the last reply
waits for every earlier fold. Encoding the reports
of node events and gone clients (new on the primary) adds to the same
queue.

**Options.** (a) As planned, measured by step 6's `latency-trace`
histograms. (b) Defer folds to the batch's end. The bodies are gone by
then (one reused buffer), so it needs a re-encode or a per-batch copy.
(c) Open question 5's dedicated consumer: no fold on the response thread,
at the cost of a core and a second encode.

**Recommendation.** (a), with step 6 reporting the tail per slot position
in the batch, not just per reply. Fix open question 5's text, which
implies the fold is off the reply path.

### 4.3 Encoding on the replica drain and on the shadow (item 15)

**Confirmed, with one refinement.** The replica's drain today only copies
slots out (`replication/mod.rs:675-690`). After the plan it encodes and
hashes every report, and if it falls behind it back-pressures the ack path
(finding 4). The refinement is on the primary. The shadow is a consumer of
the **input** ring, so a slower shadow eventually stops the reader, which
is client latency. The shadow already pauses for snapshot saves, which the
input ring absorbs. Adding a per-report encode makes it the input ring's
likeliest slowest consumer at saturation. Whether the Exchange Core's
reports per event (fills fan out) make this matter is unknown.

**Recommendation.** Step 6 should report each input-ring consumer's lag
under saturation, digests on and off, not only "no growth in the matching
stage's ring waits".

### 4.4 Per-window cost, board shape, hash features

- **Per-window cost.** Plausible as estimated, but on the replica receiver
  it lands on the in-memory-ack path of `ram` and `disk+ram`: a board read
  of a line the drain wrote on another core, plus a chain hash, per
  window. That is small, and step 6 should measure it there, not only on
  the response thread.
- **Board shape.** `melin_pipeline::seqlock` is one value per `Arc` cell
  (`seqlock.rs:106, 121-122`). Building a board of `R` slots from it gives
  `R` separate heap cells, scattered, each needing its own reader handle
  per reader. That works against the cache-line-per-slot layout decision 5
  assumes. The board will want its own array type over the seqlock
  protocol, which is new `unsafe` code. The plan's Miri test (plan:1298-1301)
  should then cover that type, not the existing one.
- **Memory.** `R = 32770` with both rings at `1 << 20` and `K = 64`. That
  is 2 MiB per board, plus two 40-byte arrays of `R` per comparator. A
  replica with a shadow holds two comparators, so with its board or boards
  it is in the high single-digit MiB. Small against the rings. This is not
  a concern.
- **`twox-hash` features.** The crate is MIT (on the allow list,
  `deny.toml:50`) and its `xxhash3_128` module has `RawHasher` and
  `SecretBuffer` (docs.rs). This assessment could not check the claim that
  dropping `std` loses SIMD dispatch: there is no source in the local
  registry. Step 6 measures both feature sets, which settles it.

## 5. Complexity and implementation risk

### 5.1 The replica's stop needs the node's shutdown flag (item 8)

**Shortfall.** On a replica, the shadow runs on the pipeline's own flag,
`pipeline_shutdown` (`replication/mod.rs:617, 696-711`). The receiver
watches the node's flag (`receiver_transport.rs:975`). Decision 11 says
`trip` "co-sets the shutdown flag" (plan:1001-1003) and relies on the
receiver leaving through `SessionExit::Shutdown` after its shadow tripped
(plan:1083-1087). If the latch is built from what the shadow holds today,
it sets the pipeline's flag. The pipeline's stages then exit, while the
receiver keeps running. It can wait on a ring nobody drains, or treat the
dead pipeline as a reason to rebuild it.

**Options.** (a) The `DivergenceLatch` holds the node-level flag and is
built once per node, passed into every pipeline builder (the plan's "one
`Arc` per node", plan:1073-1075). Say so explicitly, and make the
replica's shadow take the latch, not a flag. (b) Also make the receiver's
ring-wait loop check the latch.

**Recommendation.** (a) and (b) in the design, and have step 3's
replica-shadow integration test (plan:1504-1507) assert that the receiver
exits promptly with the input ring full.

### 5.2 Claims of compile-time enforcement that are lints (item 22)

Decision 11 says "a new detection site cannot compile without going
through `trip`" and "no thread can 'log and carry on'" (plan:1059-1062).
The token chain does enforce marker-after-latch and archive-after-marker.
It does not force a detection site to call `trip`. Rust has no linear
types, so a `Mismatch` verdict that is matched and ignored compiles.
`#[must_use]` on the verdict is a lint, not a guarantee.

**Recommendation.** Have the comparator call `trip` itself (it holds the
latch), so no caller ever holds a `Mismatch` it could drop. Reword the
claim to match what the types prove.

### 5.3 The comparator's state machine (item 22)

Decision 6 is the plan's highest-risk code. It has two modes, a run, a
pending anchor held apart from the array, sessions with three kinds and
four causes, eviction rules, and an accounting invariant
(plan:723-849). The rules checked here hold: the anchor cannot sit below
`F − K`, the run cannot span a local gap, and a held id with no run keeps
waiting entries. The property tests listed (plan:1302-1370) are the right
ones. The risk is that what they generate misses an interleaving the
prose does not name, and a bug there is either a false stop (a healthy
replica down) or, worse, a `Match` across a gap.

**Options.** (a) As planned, property tests. (b) Also write a small
exhaustive model (a bounded-model-check of the comparator over short
sequences, or a reference implementation that keeps every window and
compares by brute force) and check the real comparator against it in the
property tests. (c) Simplify. Dropping the pending anchor costs one
unverified window per anchor. That is a counted loss, and removes the one
state held outside the array.

**Recommendation.** (b): a brute-force reference that stores every window
is a few dozen lines and turns every property test into a differential
test. Keep the pending anchor unless the reference finds it hard to get
right.

### 5.4 Smaller traps (item 23)

- **The sibling it mirrors allocates.** `maybe_publish_chain_check` builds
  a `Vec` per frame (`pipeline.rs:1873-1874`). A copy of that pattern for
  the digest frame would allocate up to once per batch on the journal
  stage, breaking decision 15's "nothing allocates". Use a stack buffer,
  and consider fixing the chain check while there.
- **Encode failures log at `error!`.** Both egresses log an encode error
  at `error!` (`response.rs:1084`, `dpdk_response.rs:505`). Today that
  fires only for delivered replies. After the plan, every copy encodes
  every report, including node events' reports, which are never encoded
  today. An application whose encoder deliberately refuses internal report
  variants that only ever go to connection 0 would turn that into `error!`
  noise on every node. That breaks the log-level rule (`error!` only for
  server malfunctions). Decide in `encode_report_body`: fold `u32::MAX`
  silently, count it, and log once per variant at `warn!`. Keep `error!`
  for a reply that was due to a live client, since that drops a reply.
  The encoder's public rustdoc promises "the runtime logs at error level
  and drops the response" (`crates/core/app/src/encoder.rs:43-45`), so the
  plan's step 2 must update that contract along with the new sites.
- **"Every replica streaming reported the same window"** (plan:1209). Each
  replica disconnects right after its notice, so the set of streaming
  replicas shrinks as notices arrive. The rule needs a definition: the
  configured replicas whose notices name the same window end within one
  primary pipeline. With item 1.2 (b), compare `actual_window` too.
- **Archival during a snapshot transfer.** A primary whose shadow trips
  may be serving its snapshot to a resyncing replica. On kernel TCP the
  transfer already stops: `run_sender` takes the node's shutdown flag
  (`tcp_sender.rs:92`) and hands it to `snapshot_transfer_with`
  (`tcp_sender.rs:624`, `catchup.rs:511`), whose chunk loop returns
  between chunks once the flag is set (`catchup.rs:393-394`), and the
  trip co-sets that flag (plan:1001). Two residuals remain. The snapshot
  is read whole before publishing starts (`catchup.rs:525`), so archiving
  the file does not affect a transfer in progress, and the chunk in flight
  when the flag is set still goes out (the transfer cannot complete
  without its end frame, so this matters only if the trip lands after the
  last chunk). On DPDK the join worker's per-job `cancel` is set when its
  `JoinStream` is dropped (`join_worker.rs:159, 317`), not by the node
  flag, so the transfer runs until the DPDK sender notices shutdown and
  drops the stream. The plan should say the trip stops transfers on both
  transports, and test the DPDK case: either pass the node flag into the
  join job, or have the sender drop its streams on the trip.

## 6. Room for improvement and later extensions

In order of value for the cost:

1. **Majority verdict from the notices** (item 1.2 (b)). It needs no new
   wire field and, with two or more replicas, turns a guess into a verdict
   for persistent divergences and replicas that anchored together. It
   widens the plan's scope, which excludes majority verdicts (plan:2031).
   Step 5.
2. **Alertable staleness.** Add `melin_output_comparing{against}` and a
   lag gauge (`applied position − verified_through`, in entries), so "not
   comparing" and "falling behind" are alerts rather than "worth a look"
   (plan:1228). Step 5.
3. **Observe mode and offline qualification** (item 3.2). Before step 5
   is released.
4. **A debug-build encoder purity check** (item 1.4 (a)). Step 2.
5. **Protocol negotiation in protocol 9** (item 3.1 (b)). Step 4.
6. **A generic refusal file** (item 3.4 (b)). Step 3.
7. **An in-process query digest kind, opt-in** (item 2.3 (b)). Roadmap.
8. **State digest; chain stored in snapshots.** Already step 7. The state
   digest also closes items 1.6 and 1.7.
9. **Deferred replica exit with automatic failover on a primary-fault
   verdict** (item 1.2 (c)). Its own plan.

## What should change before implementation, and what can wait

**Change the plan before step 1:**

- 4.1: emit digests without a ring slot each (piggyback or rate-limit),
  and measure ring occupancy in step 6.
- 1.1: correct the halt-under-`disk` statements (plan:1754-1756,
  1796-1800, 1929-1930).
- 1.2: refusal message and runbook branch on who is at fault; restore
  archived snapshots on a primary-fault verdict; say whether replay is
  possible; add the majority verdict to step 5.
- 1.4: zero the body region in `encode_report_body`; add the debug
  double-encode.
- 5.1: the latch holds the node-level flag; the replica's shadow takes the
  latch.
- 2.5: close windows on any slot past them.
- 5.2, 5.4: the comparator calls `trip` itself; stack buffer for the frame;
  encode-error logging.
- 3.5: name the new archive reason so it cannot be confused with
  `Divergent`.

**Decide now, because they shape steps 2 to 5:** observe mode (3.2), the
build-id override and the comparing gauge (2.2), protocol negotiation
(3.1), the generic refusal file (3.4).

**Fold into the steps:** the hash-map-order rollout caveat (1.3), named in
the rollout notes, with the shadow prefault and the capacity-order fault
test in step 3.

**Can be deferred, documented as limits:** latent divergence and the
failover tail (1.7, 2.1), queries (2.3), the idle delay (2.4), chain breaks
(2.6), downgrades (3.4 (a)), the dedicated fold consumer (4.2 (c)), the
state digest and stored chain (step 7).

**Release gating, unchanged:** step 5 waits for monotonic time (open
question 8 (b)), with finding 7 widened (1.5) and the replica-restart tick
test added.

## Decisions for the user

1. **Ring slots (4.1):** piggyback digest frames on data batches, or
   rate-limit them?
2. **Faulty primary (1.2):** widen the plan's scope (it excludes majority
   verdicts) to add the partial two-against-one verdict from notices in
   step 5, and should a primary found at fault by it trip its own latch?
   Plan a deferred-exit failover separately, or rule it out?
3. **First rollout (3.2):** add an `observe` mode beside `enforce` and
   `off`, and an offline qualification tool? Or keep enforce-or-off as the
   stance?
4. **Upgrade (3.1):** pay for N/N−1 negotiation in protocol 9, or accept
   another cluster stop and leave negotiation for later?
5. **Build id (2.2):** add the operator override and the comparing gauge?
6. **Monotonic time (1.5):** keep the release gate, or ship the narrow
   watermark-seeding fix if monotonic time slips?
7. **Refusal convention (3.4):** introduce `<journal>.refuse` now as the
   general mechanism?
