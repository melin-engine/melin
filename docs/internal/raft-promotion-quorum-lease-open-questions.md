# Open questions of the quorum-lease plan: options and recommendations

Companion to `raft-promotion-quorum-lease-plan.md` (branch
`fix/raft-promotion-quorum-lease`). Contributor document. It takes each
open question of the plan's §7 (Q1–Q14) in turn: what hangs on it, what
reading the code settles, the realistic options with their pros, cons,
risk, value and cost, and a recommendation. Function names are the
anchors; line numbers are left out because they go stale.

openraft paths are relative to
`/usr/local/cargo/registry/src/index.crates.io-1949cf8c6b5b557f/openraft-0.9.25/src`.
That tree was checked against the `.crate` archive whose checksum
`Cargo.lock` records, so what is read below is upstream 0.9.25, not a
patched copy.

Cost scale: **small** is a few lines plus a test, **medium** is a new
helper or a test fixture of its own, **large** is a redesign or new
infrastructure.

Status per question:

- **resolved by reading**: the code answers it; the answer is cited.
- **needs experiment**: reading narrows it, but a run decides. The
  experiment and the result that selects each option are given.
- **decided**: the user has decided; the decision is recorded in the
  plan, and the analysis below is kept for its reasoning only.

Every recommendation below has been adopted in the plan, in the step
named under each question, except where a question says otherwise and
why. The plan's §7 lists where each one landed.

---

## Q1. The gate threshold (decided: 1000 ms)

**Question.** Is `QUORUM_ACK_MAX_AGE` = `LEADER_LEASE_MS / 2` = 1000 ms
right? The liveness assertion and the transient-refusal behaviour of the
plan's §5 hang on it. Since the mint-time quorum confirmation carries the
safety claim, the threshold is an early filter only (plan §4), and the
plan no longer carries a safety assertion over it.

**Decision recorded.** 1000 ms, agreed with the user (plan §5). Not
reopened here.

**Residual risks, and how the plan checks each.**

- *A healthy leader's reported age gets near the gate under load*, so a
  genuine failover is refused transiently. What can go wrong: a slower
  failover, never a stuck one (the 100 ms poll retries) and never an
  unsafe one. Likelihood: low on a quiet host, higher under
  `cargo nextest` load, where the current-thread runtime that carries the
  raft core also runs the RPC server, the bridge, the state-machine worker
  and inline `fsync`s (see Q2). Check:
  `a_healthy_leaders_quorum_ack_age_stays_under_the_gate` (step 2), re-run
  under full-suite load as plan §7 Q1 asks; the liveness assertion is the
  compile-time form.
- *The reported age lags when the driver thread is blocked.*
  `FileLogStore::save_vote` and `FileLogStore::append`
  (`crates/core/raft/src/storage.rs`) call `sync_data` inline on the raft
  runtime thread, so a slow disk delays the flush and the bridge store.
  The filter then reads an age younger than the truth by up to the stall.
  Likelihood: low (those writes happen on elections and membership
  entries, not on heartbeats), impact: the filter passes when it should
  not, which costs an abandoned promotion, not safety. Check: none at
  decision time, by design; the mint-time quorum round trip (step 4) does
  not read the metric, and its deadline is timed on the server thread, so
  a stall there refuses rather than mints late (Q2).
- *A future openraft changes the lease.* `leader_lease` is
  `election_timeout_max` (`EngineConfig::new`) and is applied in
  `Engine::handle_vote_req` only to a committed vote. A version that
  changes either invalidates the mint-time safety assertion. Check: the locked 0.9.25, the
  re-derive comment beside the `"0.9"` requirement, and
  `a_leader_that_loses_both_peers_keeps_leading_and_its_ack_ages`, which
  fails if check-quorum appears or the age stops growing. A downstream
  build resolving a later 0.9.x is not covered (plan §10).
- *Clock-rate drift between nodes.* The lease is timed on the follower's
  clock, the confirmation's deadline on the leader's. Covered by
  `SCHEDULING_SLACK_MS` in the mint-time safety assertion; not testable
  at run time (plan §10).

---

## Q2. Bridge latency and the election nudge

**Question.** Can `raft.trigger().elect().await` in the `driver_main`
bridge loop stall the metric stores for anywhere near
`SCHEDULING_SLACK_MS`? If so, the stores move ahead of the nudge. An
earlier draft of the plan had a safety assertion over the bridge's
staleness that depended on it; the plan has since dropped that assertion
(the decision-time check is an early filter, plan §4), so the answer now
only affects how promptly the filter sees a fresh age.

**Resolved by reading: the nudge cannot stall.** `Trigger::elect`
(`raft/trigger.rs`) calls `RaftInner::send_external_command`
(`raft/raft_inner.rs`), which is a `tokio::sync::mpsc` **unbounded**
`send` of `RaftMsg::ExternalCommand` and returns. There is no await on
the success path, so the future completes on its first poll without
yielding. On failure it awaits `get_core_stopped_error`, which happens
only when the core has already stopped, and the bridge then exits on
`running_state` anyway. The core handles the command later
(`RaftCore`, `ExternalCommand::Elect` arm), where a leader ignores it.

**What reading finds instead.** The bridge, the raft core, the RPC server
and openraft's state-machine worker share one current-thread runtime, and
the raft storage `fsync`s inline (`FileLogStore::save_vote` through
`write_file_atomic`, `FileLogStore::append` with `sync_data`). A stall
there delays every task on the thread, the bridge included. That matters
little at decision time (Q1). It does not threaten the mint-time bound,
because step 4 times that bound on the server thread: the driver cannot
start `get_read_log_id` (C) before it has received the request, so a
reply that arrives at all arrives within the server thread's deadline,
and a stalled runtime turns into a `TimedOut` refusal, never a late mint.
The draft this question was written against started that deadline at the
wait rather than before the `send`, leaving a preemption gap between the
two; option 3 closes it.

**Options.**

1. **Leave the loop as it is** (stores after the nudge).
   - Pros: no change; the reading shows the order is irrelevant.
   - Cons: none for the nudge itself.
   - Risk: none from the nudge.
   - Value: nothing to buy.
   - Cost: none.
2. **Move the stores ahead of the nudge anyway.**
   - Pros: the stores come first by construction.
   - Cons: churn for no measured benefit; the plan's text implies a
     hazard that does not exist.
   - Risk: none, but a misleading comment would stay behind.
   - Value: cosmetic.
   - Cost: small.
3. **Option 1, plus anchoring the existing backstop's deadline before the
   send.** Inside `LeaderConfirm::confirm`, take
   `deadline = Instant::now() + LEADER_CONFIRM_TIMEOUT + SCHEDULING_SLACK`
   before `send`, then wait with
   `recv_timeout(deadline.saturating_duration_since(Instant::now()))`.
   That instant precedes C, so the backstop then bounds the whole window
   from a conservative lower bound on C, preemption between `send` and
   the wait included. (As analysed; the adopted form below drops the
   driver-side timeout and moves the slack out of the deadline into the
   mint-time assertion.)
   - Pros: closes the one gap above. It is an implementation detail of
     `confirm`: no new refusal, no new error variant, no change to agreed
     behaviour (the same `TimedOut`, the same total bound).
   - Cons: none worth naming; the wait is a few nanoseconds of arithmetic
     shorter.
   - Risk: very low. The gap it closes needs the server thread preempted
     for a large part of the slack at exactly that point; a wrong
     `saturating_duration_since` (a negative duration) is ruled out by
     the saturating form.
   - Value: the mint-time safety bound is then measured from before C on
     the thread that publishes, with no window outside it.
   - Cost: small (two lines in `confirm`; a test with a driver stub that
     replies just past the deadline and expects `TimedOut`).

A separate `started.elapsed()` check before the publish would repeat the
same bound the backstop already enforces, so it is not listed.

**Recommendation.** Option 3. The nudge question resolves to "no change";
anchoring the backstop's deadline before the send closes the one
preemption gap without changing step 4's behaviour, so the implementer
can settle it.

**Adopted (plan step 4), simplified.** Once the deadline is taken before
the send, it alone bounds the window from before C, so the driver-side
timeout adds nothing and the plan drops it: `confirm` takes
`deadline = Instant::now() + LEADER_CONFIRM_TIMEOUT` before `send` and
waits on what is left of it. The mint-time safety assertion becomes
`LEADER_CONFIRM_TIMEOUT_MS + SCHEDULING_SLACK_MS < LEADER_LEASE_MS`, with
the slack covering reply-to-publish and clock drift. The step 4 test with
a late-replying stub pins only that a late reply is refused; it cannot
pin the anchoring, since the send is a non-blocking enqueue and the two
orders differ by nanoseconds. The anchoring is held by `deadline` being
the first statement of `confirm`, with a comment, and is listed as
untested in plan §10.

---

## Q3. Torn reads of term and age

**Question.** The bridge stores term, leader, role and (new) age as
separate `Relaxed` atomics, and the poller reads them separately. Can a
fresh age from term *t* pair with term *t′ > t*? An earlier draft relied
on a timing argument plus a term double-read; the alternatives are a
single read or a Release/Acquire seqlock.

**Resolved by reading: the timing argument holds, and the mint-time check
makes any residual tear harmless.**

- Within openraft the pair is consistent: `RaftCore::report_metrics`
  builds `current_term` and `millis_since_quorum_ack` into one
  `RaftMetrics`, and the bridge clones one value per pass
  (`metrics_rx.borrow().clone()`). A tear can only come from the
  bridge-to-atomics-to-poller hop.
- A leader cannot campaign. `handle_tick_election` returns early for a
  leader, and the `ExternalCommand::Elect` arm ignores the trigger while
  `engine.leader` is set. So leader(*t*) → leader(*t′*) always goes
  through a vote update that demotes it, then a campaign of its own
  (`Engine::elect` always takes `term + 1`).
- That campaign is either timer-driven, which waits at least
  `election_timeout` from `vote_utime` (`leader_lease + election_timeout`
  if the vote is committed; `handle_tick_election`), so at least
  `ELECTION_TIMEOUT_MIN_MS`, or nudged. A nudge needs the poller to have
  seen a published non-leader role with another node as leader
  (`consider_challenger_nudge`), and then `CHALLENGER_HOLD_DOWN`. Either
  way the interval is many bridge stores long, and a non-leader state is
  published in between.
- One corner: a nudge filed while the node was a follower, swapped by the
  bridge after the node has meanwhile won and lost a leadership, could
  start a campaign at once. The published history is then follower →
  leader(*t′*), not leader(*t*) → leader(*t′*), so no age from an older
  leadership is in the atomics to tear against. openraft keeps the
  quorum clock in the `Leader` value (`Leader::clock_progress`, created
  empty by `Leader::new`), so a new leadership reports `None` until its
  first ack and never inherits an age.
- `single-term-leader` does not change this. Under it `LeaderId` carries
  `voted_for`, and a committed vote outranks an uncommitted one in the
  same term (the tie-break is `Vote::partial_cmp` in `vote/vote.rs`;
  `LeaderId`'s own ordering is `LeaderId::partial_cmp` in
  `vote/leader_id/leader_id_std.rs`).
  That restricts who may lead a term. It adds no transition from leader
  to leader.
- The residual tear (a store landing between two loads that are
  nanoseconds apart) can pair a term with the age of the neighbouring
  store. Step 4 confirms the term itself with a quorum round trip (Q11:
  exactly), so a promotion filed on a torn read cannot mint unless that
  term's leadership is confirmed.

**Options.**

0. **`Relaxed`, each value read once.**
   - Pros: no change to the bridge's memory model, no extra code or test.
     The term filed with the request is the one the mint confirms
     exactly (Q11).
   - Cons: a tear goes undetected at decision time.
   - Risk: low. A tear can let the filter pass for one poll, or file a
     term the node no longer leads in; either way the mint-time
     confirmation refuses (at worst an abandoned promotion and an exit,
     which needs a tear across a re-election, shown above to be many
     bridge stores apart).
   - Value: the guarantee the other options aim at already comes from
     step 4.
   - Cost: none.
1. **`Relaxed` plus the term double-read** (an earlier draft of the plan).
   - Pros: no change to the bridge's memory model; the double-read
     catches a tear across a term change in the common case.
   - Cons: not a guarantee on weakly ordered hardware (aarch64). It only
     catches a store landing between two loads nanoseconds apart, and
     not a stale-but-coherent term, which is what a cross-location
     `Relaxed` tear actually is.
   - Risk: low. A missed tear lets an early filter pass for one poll, and
     the mint-time confirmation still refuses.
   - Value: little beyond option 0: what it catches, the mint already
     refuses.
   - Cost: small (a second load, a refusal path and its test).
2. **Seqlock-style Release/Acquire** (bridge stores age then term with
   `Release`; reader loads term, age, term with `Acquire`).
   - Pros: an actual guarantee that age and term are from one store.
   - Cons: ordering subtleties to document and review in two crates; the
     guarantee buys nothing step 4 does not already give.
   - Risk: low, but a wrongly placed ordering would look correct and be
     wrong.
   - Value: low, given step 4.
   - Cost: small to medium (the reasoning, not the code).
3. **Publish the pair under a lock**: a `Mutex<(u64, Option<u64>)>` (term,
   age) in `RaftStatus` behind the two accessor methods, written once per
   bridge pass and read by the 100 ms poller and the health thread.
   - Pros: trivially consistent, no ordering arguments. A mutex suits this
     data: two readers at human timescales, one writer, never on the hot
     path, and it is uncontended almost always.
   - Cons: `term` is already a public atomic that other readers use, so
     the term would live in two places, or the public field would have to
     go (a breaking change in `melin-transport-core`).
   - Risk: a second copy of the term could drift from the first.
   - Value: moderate, if a guarantee were needed.
   - Cost: medium (API churn in a published crate).

**Recommendation.** Option 0, adopted in plan step 3. The timing argument
holds by reading, openraft never carries an age across leaderships, and
the exact term check at mint time (Q11) makes acting on a torn read
harmless. The double-read (option 1, recommended in an earlier version of
this analysis) and any stronger ordering would duplicate, weakly or
fully, what step 4 already proves.

---

## Q4. Mint refusal policy (decided: quorum confirmation, exit on failure)

**Question.** What happens when, at mint time, the node cannot confirm it
still leads? Step 4 depends on it.

**Decision recorded.** A real quorum confirmation
(`Raft::get_read_log_id`, bounded by `LEADER_CONFIRM_TIMEOUT`) right
before the `EpochBump` publish; on failure the node publishes nothing and
exits with an error (plan step 4). Not reopened here.

**Residual risks, and how the plan checks each.**

- *A false refusal on a slow but healthy control plane* (confirmation past
  `LEADER_CONFIRM_TIMEOUT`). The node exits, restarts as a replica, and a
  genuine failover is delayed by a restart, recovery and prefault cycle.
  Likelihood: low on a LAN. Each confirmation opens a fresh connection per
  peer and authenticates it (Q12), all inside one heartbeat interval.
  Check: the `election.rs` confirmation tests (pass on a healthy cluster).
  Not checked: behaviour under a deliberately slow peer.
- *Liveness depends on a supervisor.* Exit-on-failure assumes something
  restarts the process; without one, an abandoned promotion leaves the
  node down. Check: none in code; the plan's `docs/replication.md` text
  says that the node exits and must be restarted by its supervisor
  (adopted, step 4).
- *A stalled raft runtime during the confirmation* (Q2). Not a safety
  risk: the confirmation's deadline is taken on the server thread before
  the request is sent, so before the driver can start C; a stall becomes
  a `TimedOut` refusal, and the publish follows an in-time reply with
  nothing blocking. Check: a `confirm` test with a stub that replies past
  the deadline (step 4) pins the refusal of a late reply; the
  deadline-before-send order is held by the code's shape, not a test
  (plan §10).
- *"Journal untouched" is asserted by the unit test only.* The
  `journal_promotion_epoch_bump` stub test shows nothing is published,
  but no test restarts an abandoned node and checks that it rejoins
  without a divergence. Likelihood of a problem: low (the bump is the
  tenure's first entry). A restart-after-abandon integration test was
  considered and **not adopted**: no integration test can produce an
  abandoned promotion on demand (the quorum loss would have to be timed
  inside the drain/prefault window, plan §10), and the rejoin itself is
  the ordinary replica restart that existing tests cover. Recorded in
  plan §10.
- *Manual `PROMOTE` skips the confirmation.* Intended: it keeps manual
  failover available during a control-plane outage. Check: the
  `Manual`-skips-the-stub unit test.

---

## Q5. Can `journal_promotion_epoch_bump` be unit-tested?

**Question.** Can a ring `Producer`/consumer pair and a `FenceState` be
built in `server.rs` tests without a pipeline? If not, the refusal path
falls back to integration coverage and a recorded gap.

**Resolved by reading: yes.** `server.rs`'s own
`primary_gate_policy_tests::matching_stage` already builds
`DisruptorBuilder::<InputSlot<CounterEvent>>::new(8).add_consumer().build(..)`
and `FenceState::new(0)` with no pipeline. `journal_promotion_epoch_bump`
publishes once, then waits on
`ORCHESTRATOR_WAIT.wait_until(|| fence_state.epoch() >= new_epoch || shutdown…)`.
`WaitStrategy::wait_until` tests the predicate before it idles, so a test
can pass a raised `shutdown` flag to return at once, or run a thread that
`try_consume`s the slot and calls `FenceState::observe_epoch`, which
stands in for the matching stage.

**Options.**

1. **Unit tests in `server.rs`** with the existing ring builder and a
   stub confirmation (a closure in the plan, which dropped the
   `LeadershipConfirm` trait an earlier draft named: one production
   closure, built by one helper both call sites share, and one test
   stub do not need a trait).
   - Pros: covers every origin and outcome deterministically; asserts
     both "no slot published" (`try_consume` is `None`) and "fence epoch
     unchanged".
   - Cons: none material.
   - Risk: none.
   - Value: the mint-time refusal, the step that guards the epoch, gets
     direct coverage.
   - Cost: small. The fixture exists; one helper builds the ring for
     these tests (hoist `matching_stage`'s ring construction rather than
     copy it).
2. **Integration-only coverage.**
   - Pros: real pipeline.
   - Cons: cannot time a quorum loss inside the drain window (plan §10);
     the refusal path would go untested.
   - Risk: an untested refusal path on the epoch-minting code.
   - Value: low.
   - Cost: large.

**Recommendation.** Option 1, adopted in plan step 4. The fixture already
exists in the same file, so the integration fallback is not needed.

---

## Q6. Post-heal evidence: divergence counter and archive naming

**Question.** Is `melin_replica_divergence_total` exported on the
primary, and is the archive directory name stable enough to assert on?
Step 1c's heal-phase assertions depend on both.

**Resolved by reading.**

- The counter is emitted unconditionally by `HealthSnapshot::write_prometheus`
  (`crates/core/transport-core/src/health.rs`), read from
  `replication_metrics` and 0 where there is none. It is incremented by
  the replication *sender* on a divergent handshake (`tcp_sender.rs` and
  `replication/dpdk.rs`, `divergence_total.fetch_add`), so it counts on
  the primary. `snapshot_location.rs` already waits on it on a primary
  (`wait_for_gauge_within`).
- The archive name is `<file name>.<reason>.<n>` (`archive_dir` in
  `crates/core/transport-core/src/replication/archive.rs`, tags
  `divergent` / `resync` from `ArchiveReason::tag`). Both are private, but
  the format is documented for operators in `docs/replication.md`
  ("sibling directory named `<journal>.divergent.<n>`"), so renaming it
  is an operator-visible change. `dpdk_veth/main.rs` already hard-codes
  `divergent.journal.divergent.0`.
- A caveat: an *absence* assertion on one exact name passes vacuously if
  the format ever changes. Divergence and archiving are also post-fix
  checks: in the red run the primary is fenced, and nobody points `L` back
  at the old lineage within the window, so neither fires there.

**Options.**

1. **Assert absence of the exact `<journal>.divergent.0` path.**
   - Pros: simplest; matches the existing test.
   - Cons: vacuous if the format changes; misses `.resync.<n>`.
   - Risk: a silent pass. Low likelihood, medium impact (a missed
     regression).
   - Value: low.
   - Cost: small.
2. **Assert that no sibling of `L`'s journal matches an archive form,
   `<journal file name>.divergent.<n>` or `<journal file name>.resync.<n>`**
   (directory scan in the test, `<n>` any integer). A bare
   `<journal file name>.` prefix would not do: rotated journal segments
   are `<live>.NNNNNN` (`archive_path` and `list_archives` in
   `crates/core/journal/src/segment.rs`), and snapshot files share the
   journal stem too (`parse_archive_index` there rejects `.snap.tmp` and
   `.snapshot.prev` for that reason). So a prefix scan would trip on any
   rotation of `L`'s journal during the test.
   - Pros: covers every index of both reasons; immune to rotation and
     snapshot siblings.
   - Cons: the test spells the two tags itself, a copy of what
     `ArchiveReason::tag` owns (private); a third reason added later
     would not be covered until the test learns it. A comment beside the
     tag list should point at `ArchiveReason::tag` and say why it is
     copied.
   - Risk: low. The tags are documented for operators, so a rename is
     already a visible change that would be caught in review.
   - Value: a real "nothing was archived" check.
   - Cost: small.
3. **Add a public listing helper to `archive.rs`** (for example a
   function that returns a lineage's archives) and use it here and in
   `dpdk_veth`.
   - Pros: one owner of the format and of the tag strings, which removes
     the hard-coded copy in `dpdk_veth` and the tag copy option 2 needs
     (the no-duplication rule), and covers a future reason automatically.
   - Cons: new public API in a published crate, for tests only.
   - Risk: low.
   - Value: moderate (de-duplication).
   - Cost: small to medium.

**Recommendation.** Option 2 for step 1c, plus the divergence counter on
the primary as planned; both adopted in plan step 1c. Option 3 is worth
doing only if another test needs the format, because it adds public API
for tests alone.

---

## Q7. Is `PromotionRequest` public API?

**Question.** Step 0 changes `PromotionRequest`'s contract. It skips a
CHANGELOG entry only if nothing re-exports it.

**Resolved by reading: not public.** `mod promotion` is private in
`crates/core/server-runtime/src/lib.rs`, and `rg PromotionRequest` finds
no `pub use`. The one public type that holds it,
`replication::ReplicaControlPlane`, keeps it in a `pub(crate) promote`
field, and the functions in `replication/mod.rs` that take it are
`pub(super)` or `pub(in crate::replication)`. No semver tooling runs in
CI, so nothing would flag a leak either: the reading is the check.

**What the plan's step 0 misses.** Besides the callers it lists, test
code calls the `u64` API: `admin.rs` tests,
`replication/tcp_receiver.rs` and `replication/receiver_transport.rs`
tests (`request(PromotionRequest::MANUAL)`), and `replication/mod.rs`
tests. They port with the type; the compiler will list them.

**Options.** 1. No CHANGELOG entry (pros: accurate; risk: none; cost:
none). 2. An entry anyway (cons: describes a non-API change to operators;
value: none).

**Recommendation.** Option 1, as planned; the test callers are listed in
plan step 0.

---

## Q8. Partition tests under the DPDK netns runner

**Question.** `raft_failover.rs` runs in CI's "Runtime suites on DPDK"
step (`pre-merge.yml`, `kind(test)` over `melin-server-runtime` with
`--features dpdk`, under `scripts/dpdk/netns-runner.sh`). Do per-node
127.0.0.*i* raft addresses and the proxies work there?

**What reading settles.**

- `lo` is up in the runner's namespace: the header of `netns-runner.sh`
  says so, and `veth-setup.py` brings up `lo` with the veths. The
  existing raft tests already bind 127.0.0.1 there.
- The replication proxy as `replica_of` on DPDK is already proven:
  `degraded_acks.rs` gives a replica `Proxy::addr` as its `replica_of`
  and runs in that CI step.
- **A gap the plan does not mention:** `Proxy::start` binds
  `melin_test_node::local_ip()`, which on DPDK is the runner's kernel-side
  client IP (`10.99.0.1`), not loopback. Raft proxies built with it would
  have raft traffic, bound to 127.0.0.*i*, dial a non-loopback local
  address.
- **A second gap:** an earlier draft's `raft_ip(i) = 127.0.0.(i+1)` gives node 0
  `127.0.0.1`, the source address the kernel picks for any unbound
  loopback socket. Checked here: an unbound connect to 127.0.0.3 arrives
  from 127.0.0.1. If 1a's source binding regressed, node 0's connections
  would look unchanged and a partition involving it could pass for the
  wrong reason.

**Experiment run.** In a fresh unprivileged network namespace, with `lo`
up and `10.99.0.1` as an alias standing in for the bridge address, a
socket bound to 127.0.0.2 connected both to 127.0.0.3 and to 10.99.0.1,
and each listener saw source 127.0.0.2. So both designs work in
principle. The runner itself was not used: this host is aarch64, the
runner is wired for the x86_64 target, and the real bridge address sits
on `br0`, not on `lo`.

**Options.**

1. **Use `Proxy::start` unchanged for the raft proxies** (bound on
   `local_ip()`).
   - Pros: no change to `proxy.rs`.
   - Cons: on DPDK, raft goes through the client-side address for no
     reason; correctness rests on routing a 127/8 source to a bridge
     address, which was shown only with an alias on `lo`.
   - Risk: low-to-medium. A runner-only failure would surface in CI's
     DPDK job, not locally.
   - Value: none beyond saving a few lines.
   - Cost: none.
2. **Add `Proxy::start_on(ip, upstream, stalled)`; `Proxy::start`
   becomes `start_on(local_ip(), upstream, |_| false)`; the raft proxies
   bind on 127.0.0.1; `raft_ip(i) = 127.0.0.(i+2)`.**
   - Pros: raft (kernel TCP on every transport) never touches the
     DPDK-facing address, so the partition tests behave the same on both
     transports. No node shares the default loopback source, so under a
     1a regression no per-source block matches; the isolated leader's
     outbound links stay up and step 1c's assertion that the other two
     elect a new leader fails visibly (the 1a unit tests catch it first).
   - Cons: one more constructor in `proxy.rs`. The file has no dead-code
     allowance, so every item must be used by both includers: `start`
     delegates to `start_on`, which keeps `start_on` used by
     `degraded_acks.rs` too. (The same rule is why the plan passes the
     per-source block list to `start_on` as a predicate, defined in
     `raft_failover.rs`, rather than adding `block_from` methods that
     `degraded_acks.rs` would leave unused.)
   - Risk: low.
   - Value: transport-independent partition tests, and, with step 1c
     asserting the re-election, a harness whose isolation is checked
     rather than assumed.
   - Cost: small.
3. **Gate the partition tests to kernel TCP** (the plan's fallback).
   - Pros: no DPDK question at all.
   - Cons: loses DPDK coverage of the failover path the partition
     exercises (the promoted node serves as a DPDK primary).
   - Risk: a DPDK-only regression in the post-heal rejoin goes unseen.
   - Value: negative.
   - Cost: small.

**Status: needs experiment**, a single CI run. Implement option 2 and
run the "Runtime suites on DPDK" step. If the partition tests pass, done.
If only they fail, compare against option 1 before falling back to
option 3, and record why in the file header.

**Recommendation.** Option 2, adopted in plan step 1b; the CI run is
still to do (plan §7).

---

## Q9. Frozen raft links on heal

**Question.** A restored frozen relay flushes stale raft frames. Does the
heal converge, or should `heal()` cut then restore the raft proxies?

**What reading settles.**

- openraft tolerates delayed and duplicated frames: a stale append or
  vote from a lower vote is refused (`Engine::handle_vote_req`; appends
  answer `HigherVote`). Safety does not depend on the proxy.
- The client side has already given up on those connections:
  `RaftClient::request` (`crates/core/raft/src/network.rs`) drops its
  stream on the RPC timeout (one heartbeat interval), so a frozen relay
  holds at most one unanswered request per connection, and its reply goes
  to a closed socket.
- **Melin-specific effect:** the RPC server records the sender's journal
  tip from every inbound envelope, and `PeerTips::record_at`
  (`crates/core/raft/src/recency.rs`) overwrites with the *arrival* time.
  A flushed stale frame therefore registers an old tip as fresh until the
  next real frame from that peer, about one heartbeat. In this scenario
  that is harmless: after the heal `L` is demoted by `HigherVote` on its
  first append, and with the fix its lease is not fresh in the old term.
  The same thing can happen in production with TCP retransmission after
  a real partition; see "Findings beyond the questions".
- **Resource growth during isolation.** Every dial attempt from a
  blocked source that the proxy accepts and holds costs a socket and,
  with today's relay design, two threads. Attempts recur on openraft's
  backoff for as long as the isolation lasts, in every blocked direction.

**Options.**

1. **Unblock only; frozen relays flush** (the plan's default).
   - Pros: most realistic (delayed delivery happens on real networks).
   - Cons: heal timing depends on what was buffered; stale tips are
     briefly recorded as fresh; held relays and their threads pile up.
   - Risk: a flaky heal phase (low likelihood, but a flaky test in the
     `cluster-serial` group costs everyone).
   - Value: exercises delayed delivery, though no assertion targets it.
   - Cost: none.
2. **`heal()` cuts the raft proxies, then clears the block list, then
   restores them** (the generation bump closes every held relay; new
   connections are relayed). The cut must precede the clear: a relay
   stalled for a blocked source would otherwise resume and flush before
   the cut. Only the raft proxies, plus the isolated node's replication
   proxy: cutting the other replica's replication link would drop the
   primary's in-memory replica under `disk+ram` during the heal phase.
   - Pros: deterministic heal; matches what the clients already did
     (they abandoned those connections); frees the held threads.
   - Cons: delayed-frame delivery is not exercised.
   - Risk: low.
   - Value: a stable test whose subject is the gate, not the network.
   - Cost: small (`cut` and `restore` exist).
3. **Option 2, and blocked accepts held without relay threads** (keep the
   accepted socket in a list and drop it on heal, still silent: no read,
   no RST until heal).
   - Pros: as option 2, and the isolation phase no longer grows threads.
   - Cons: a little more proxy code. Comment the collection: a `Vec` under
     a `Mutex`, pushed once per accepted attempt and drained once on heal,
     never iterated on a relay path.
   - Risk: low.
   - Value: a bounded harness however long the watch window grows.
   - Cost: small.

**Status: resolved; adopted (plan step 1b).** The experiment that would
have decided whether option 1 is viable is not needed, since option 1 is
not used.

**Recommendation.** Option 3, adopted in plan step 1b. It is cheap, it
makes the heal deterministic, and it keeps the harness bounded. The plan
also has the relay re-check the generation between read and write, so a
held relay that passed its generation check just before the heal's cut,
and then saw the block list cleared, drops the bytes buffered during the
isolation instead of flushing them into the healed link. The plan's `cut` also closes the healthy raft
pair's relays once at heal; they reconnect on the next
heartbeat, well inside an election timeout. Stale-tip delivery is left to
the stale-peer-tip follow-up (plan §8), which pins it with tests of its
own.

---

## Q10. Late primary bring-up converges without a re-election (decided: settled by a step 1b test)

**Question.** `start_replicas_first()` starts node 1 (the primary) after
nodes 2 and 3 have elected a leader. Nodes are named by raft id here;
`Cluster` indexes them from 0, so node 1 is index 0. Does node 1's own `initialize` and
first campaign disturb that leader? Every later integration case relies
on leadership staying on the replica.

**What reading settles.**

- A fresh node 1 initializes: `Engine::initialize` passes
  `check_initialize` (empty log, default vote), appends the membership
  entry at `LogId::default()`, and **calls `elect()` at once**, at
  term 0 + 1 = 1.
- Its vote request cannot win. The other replica holds a committed vote
  inside its lease (`Engine::handle_vote_req` refuses while
  `now <= vote_utime + leader_lease`). The leader refuses on the log: the
  candidate's last log id is index 0 of term 0, and the leader holds its
  own term's blank entry after it. If the replicas' term is also 1, the
  leader's committed `(1, leader)` outranks node 1's uncommitted
  `(1, node 1)` under `single-term-leader` (`Vote::partial_cmp`). If it is
  higher, node 1's term is simply lower.
- When the replicas' term is also 1, node 1 does not join through the
  refusal. `Engine::handle_vote_resp` adopts only the *uncommitted* form
  of a rejecting voter's vote, and with equal terms `(1, leader)` and
  `(1, node 1)` are incomparable, so nothing changes. When their term
  *t* is higher, the refusal does move node 1: it adopts the uncommitted
  `(t, leader)` vote and resets its vote timer, but it is still not a
  follower of a committed leader. Either way it joins on the leader's
  first append, whose committed vote
  outranks its own. Index 0 matches on both sides (the same
  `LogId::default()` membership entry from identical peer lists), so that
  append extends its log without conflict.
- The disruptive case is node 1 campaigning again, at term 2, before
  that append arrives. Its appends would then answer `HigherVote` and
  demote the leader (openraft 0.9 has no pre-vote). Reading makes this
  unlikely. The refusal reports a longer log than node 1's, so
  `handle_vote_resp` calls `set_greater_log`, and `handle_tick_election`
  then adds `smaller_log_timeout` (`EngineConfig::new`: twice
  `election_timeout_max`) to node 1's next timeout. The leader's
  replication to node 1 resumes after at most one openraft backoff
  (`RaftNetwork::backoff`, 500 ms by default) and one tick, well before
  that.
- A harmless race: the RPC server is spawned before `initialize`, so an
  append could in principle land first. `initialize` then answers
  `NotAllowed`, and `driver_main` logs "already initialized — using
  stored membership". Convergence is unaffected, and the race is unlikely
  (the leader's replication to node 1 is in backoff after refused
  connects).
- Melin's vote filter and stand-down rules act only on journal tips, and
  every node here has a blank journal, so they do not change this.
- Still a timing argument over openraft internals, hence the experiment.

**Options.**

1. **Keep `initialize`; assert the leader's id and term are unchanged
   after node 1 joins** (the plan).
   - Pros: no production change; the assertion turns the reading into a
     tested fact every run.
   - Cons: depends on openraft internals (lease, vote order, the
     greater-log delay), which are pinned by the lockfile.
   - Risk: low. A failure is loud and immediate in the bring-up.
   - Value: deterministic replica leadership for every later case.
   - Cost: small.
2. **A delayed or skipped `initialize` for a late joiner.**
   - Pros: node 1 never campaigns.
   - Cons: no such knob exists. It would be a production config option
     added for a test, or a test-only seam in `melin-raft`'s public
     `RaftConfig` (a published crate).
   - Risk: a test-only path in production code.
   - Value: only if option 1 fails.
   - Cost: medium.
3. **Re-roll**: start all three normally and restart the primary until a
   replica leads.
   - Pros: no production change.
   - Cons: nondeterministic, slow.
   - Risk: flaky.
   - Value: low.
   - Cost: small.

**Status: decided (plan step 1b and §7).** The deciding test is step
1b's `replica_leader_promotes_when_the_primary_dies` with the id-and-term
assertion, run repeatedly. If it holds, option 1 stands. If node 1 ever forces
a re-election, capture the raft logs (`RUST_LOG=openraft=info`) before
choosing option 2, which would then need the user (a new config surface).

**Recommendation.** Option 1.

**Decision recorded** (plan step 1b and §7): option 1, with the
id-and-term assertion in `start_replicas_first()` and repeated runs
before step 1b is committed; options 2 and 3 are deferred until that test
says they are needed.

---

## Q11. Does `read_log_id` carry the confirming term?

**Question.** Can step 4 check the term from `get_read_log_id`'s result
instead of re-reading metrics afterwards?

**Resolved by reading: yes, exactly.**

- `RaftCore::handle_check_is_leader_request` computes
  `LeaderHandler::get_read_log_id` =
  `max(leader.noop_log_id, committed)` and captures `my_vote` in the same
  synchronous section, before any heartbeat is sent.
- `Leader::new` always sets `noop_log_id = Some(..)` with the leader's
  committed leader id: either the first log it already proposed, if that
  log's leader id equals its vote's, or a new `LogId` built from
  `vote.committed_leader_id()`.
- `committed` cannot hold a later leader id than the leader's own (a
  leader's log never runs ahead of its vote). Log ids order by leader id
  first, and under `single-term-leader` `CommittedLeaderId` is `{ term }`
  (`vote/leader_id/leader_id_std.rs`). So the max always carries the
  confirming leader's term.
- So on `Ok`, `read_log_id` is `Some` and `read_log_id.leader_id.term` is
  the term whose leadership the quorum just acknowledged. A node that
  lost term *t* and won *t′* in between would return *t′*, and the
  comparison refuses.

**Options.**

1. **Compare `read_log_id.map(|l| l.leader_id.term)` with the requested
   term**; `None` maps to `NotLeader`, with a comment that it is
   unreachable by the reading above.
   - Pros: exact; no second read; no window after the confirmation.
   - Cons: relies on an openraft invariant (pinned by the lockfile, and
     re-checked on upgrade with the rest of §3).
   - Risk: low.
   - Value: the term check becomes part of the quorum proof itself.
   - Cost: small.
2. **Re-read metrics after `Ok`** (the plan's fallback).
   - Pros: independent of the read-log-id invariant.
   - Cons: a separate read after the fact. It can see an unchanged term
     while the confirmation in fact covered another one only if the node
     regained the same term, which is impossible, so it is weaker without
     being safer.
   - Risk: low.
   - Value: lower than option 1.
   - Cost: small.

**Recommendation.** Option 1, adopted in plan step 4. An earlier draft
also pre-checked term and role from current metrics before the round
trip; the plan drops that pre-check. The result check already covers both
cases exactly (a follower's `get_read_log_id` answers `ForwardToLeader`
at once, without a round trip, and a stale term fails the comparison), so
the pre-check only saved a round trip on a path that is about to exit,
at the cost of two more branches to cover.

---

## Q12. How fast does confirmation fail without a quorum?

**Question.** When no follower answers, does `get_read_log_id` fail fast
or wait on long timeouts? It decides which `LeaderConfirmError` the
isolation test expects.

**Resolved by reading.** `handle_check_is_leader_request` sends one
`append_entries` per voter, each under
`timeout(ttl, ..)` with `ttl = config.heartbeat_interval`, which is this
node's `HEARTBEAT_INTERVAL_MS` plus its jitter (at most
`HEARTBEAT_INTERVAL_MS + HEARTBEAT_JITTER_MS - 1`). It
answers `QuorumNotEnough` once every pending request has resolved without
a majority. It also creates a **new** client per voter through
`RaftNetworkFactory::new_client`, so each confirmation opens a fresh TCP
connection and runs the Ed25519 handshake (`RaftClient::ensure_connected`)
inside that ttl.

- Peers stopped (connection refused): fails almost at once, with
  `NoQuorum`.
- Peers behind a frozen proxy: fails after one ttl, still under
  `LEADER_CONFIRM_TIMEOUT` (500 ms), with `NoQuorum`.
- A follower with a higher vote answers `HigherVote`, which yields
  `ForwardToLeader`, mapped to `NotLeader`.
- `TimedOut` appears only if the driver's runtime or the server thread is
  stalled long enough to push the answer past the server thread's
  500 ms deadline (Q2; the plan applies no driver-side timeout).

**Options.**

1. **Assert exactly `NoQuorum` in the stopped-followers test**, and the
   set `{NoQuorum, TimedOut}` plus the time bound in any frozen-proxy
   variant.
   - Pros: pins the fast path openraft actually takes; a change of
     variant flags an openraft behaviour change.
   - Cons: a heavily loaded host could, rarely, produce `TimedOut`.
   - Risk: a rare flake (low).
   - Value: a sharper test.
   - Cost: small.
2. **Assert the set everywhere** (the plan).
   - Pros: no flake.
   - Cons: would not notice openraft switching to a slow path.
   - Risk: none.
   - Value: lower.
   - Cost: small.

**Recommendation.** Option 1 for the stopped-followers case, whose
refused connects involve no timer, and the set for anything that involves
a frozen link. Adopted in plan step 4, which adds only the
stopped-followers test; the set applies to a frozen-link variant if one
is ever added (plan §7, Q12).

---

## Q13. Non-Linux hosts

**Question.** 127.0.0.*i* for *i* > 1 is routed to `lo` only on Linux.
Does any CI job or supported host run the server-runtime integration
tests elsewhere?

**Resolved by reading: no.** Every job in `.github/workflows/`
(`pre-merge.yml`, `post-merge.yml`, `nightly.yml`) runs on
`ubuntu-latest`. `melin-server-runtime` and `melin-transport-core` depend
on `io-uring` unconditionally, and server-runtime's manifest says
io_uring "is now always used", so the crate does not build off Linux in
the first place. The bench suite's hosts and `test-containers.sh` are
Linux as well.

**Options.** 1. No fallback; one sentence in the harness noting the Linux
assumption (cost: small; risk: none). 2. A `cfg(target_os)` skip path
(cons: dead code on a platform that cannot build the crate; value: none).

**Recommendation.** Option 1, adopted in plan step 1b.

---

## Q14. Bind-before-connect edge cases (decided: startup refusal)

**Question.** With 1a, a raft `bind` on a specific IP whose family differs
from a peer's, or one that is no longer assigned, must fail the connect
with a clear `io::Error` and not panic. Does openraft's retry handle the
repeated failure quietly?

**What reading and probing settle.**

- Error kinds (std's errno mapping, `sys/io/error/unix.rs`, plus a probe
  on this host): an unassigned bind IP gives `EADDRNOTAVAIL` →
  `ErrorKind::AddrNotAvailable`. A family mismatch gives an errno std
  leaves `Uncategorized` (`EAFNOSUPPORT`) or `InvalidInput` (`EINVAL`).
  None of these panic.
- `to_rpc_error` (`crates/core/raft/src/network.rs`) maps only
  `TimedOut`, `ConnectionRefused` and `NotFound` to `RPCError::Unreachable`.
  Every bind failure becomes `RPCError::Network`.
- openraft's replication loop (`replication/mod.rs`) starts its backoff
  (`RaftNetwork::backoff`, 500 ms by default) **only on `Unreachable`**.
  `Network` is retried on the next heartbeat, with no backoff.
- "Quietly" is not attainable from Melin's side: that loop logs
  `warn!("error replication to target")` and `error!("RPCError")` on
  *every* failed RPC, for a dead peer today as much as for a bind failure.
- `RaftPeer::addr` is a `String`. The CLI guarantees an IP literal
  (`parse_raft_peer` in `crates/core/server-runtime/src/raft.rs` rejects
  anything that is not a `SocketAddr`), but `TcpSocket::connect` needs a
  `SocketAddr`, so 1a must parse the stored address. A library user who
  passes a hostname, which `TcpStream::connect(&str)` resolves today,
  would fail on the bound path.

**Options.**

1. **Rely on the OS errors as they come.**
   - Pros: no extra code.
   - Cons: a family mismatch surfaces as a bare errno string; bind
     failures retry on every heartbeat, without backoff.
   - Risk: low (log noise, no correctness impact).
   - Value: none.
   - Cost: none.
2. **In `ensure_connected`, check the family up front and return
   `InvalidInput` naming the bind IP and the peer; map `AddrNotAvailable`
   and that `InvalidInput` to `Unreachable` in `to_rpc_error`; return
   `InvalidInput` for an address that does not parse as a `SocketAddr` on
   the bound path.**
   - Pros: clear messages; openraft backs off like for a dead peer; no
     panic path.
   - Cons: a hostname peer on the bound path is refused outright rather
     than resolved (document it on `RaftConfig::bind`).
   - Risk: low.
   - Value: an operator reads "bind 10.0.0.5 cannot reach [fd00::2]"
     instead of an errno, and the retry rate matches every other
     unreachable peer.
   - Cost: small, with unit tests for each mapping.
3. **Option 2, plus validation at startup:** `build_raft_config` refuses
   a specific `--raft-bind` whose family differs from any `--raft-peer`
   address.
   - Pros: the misconfiguration fails the boot, with the flag named,
     instead of retrying forever in the background.
   - Cons: the stored membership, not the CLI, is what is dialled after
     first boot (`driver_main`'s `NotAllowed` branch), so this checks the
     CLI copy only. The runtime check of option 2 stays necessary.
   - Risk: low; a new refusal at startup, which is operator-visible.
   - Value: fail-fast on a configuration error.
   - Cost: small.

**Recommendation.** Option 3. Correctness is unaffected either way, but a
node whose control plane can never dial is a misconfiguration the
operator should hear about at boot. The startup refusal is a new
operator-facing behaviour, so it is listed for the user; option 2 alone
is the implementer's call.

**Decision recorded** (plan step 1a): option 3. Two refinements in the
plan: the startup check sits in `melin_raft::spawn`'s config validation
rather than `build_raft_config`, so library users get it too, and it
shares one helper with the connect-path check, so the rule exists once.
A specific bind with a peer that is not an IP literal is refused at
startup as well, since the bound path cannot dial it.

---

## Findings beyond the questions

Found while answering the above. Each has a line in the plan or a
follow-up, as noted.

1. **Raft storage `fsync`s on the driver thread** (Q2). They do not
   endanger the mint-time bound, which the server thread's deadline
   enforces; the deadline is taken before the `send` (adopted, plan
   step 4).
2. **Each confirmation opens fresh connections**
   (`handle_check_is_leader_request` → `new_client`), with the Ed25519
   handshake, inside one heartbeat interval (Q12). Fine on a LAN; stated
   in plan §3, §5 and §8, so that nobody reads the 500 ms bound as one
   RTT.
3. **`Proxy::start` binds `local_ip()`**, and **`raft_ip(0)` was
   127.0.0.1** (Q8). Both fixed in plan step 1b.
4. **Step 0's caller list omitted test callers** (Q7). Listed in plan
   step 0.
5. **A delayed raft frame can regress a peer's recorded tip.** *Decided:
   a follow-up branch after this plan, recorded in plan §8* (a raft
   protocol version first, then envelopes stamped with a kernel boot
   stamp and a counter taken with the tip read). The options sketched
   below are superseded by that decision and kept for context.
   `PeerTips::record_at` overwrites with the arrival time, so a frame
   delayed by a partition (proxy flush in tests, TCP retransmission in
   production) makes an old tip look fresh until the peer's next frame.
   The peer-tip veto could then pass for about a heartbeat on a tip lower
   than the peer's real one. It is out of this plan's scope, narrow (it
   needs a stale frame from a peer that has since moved ahead, arriving
   while this node is deciding), and the vote filter and election remain
   the primary steering. Options for a follow-up: ignore a sample whose
   tip is behind the stored one within the same epoch (a peer's tip only
   regresses on a re-seed, which changes its lineage), or stamp tips with
   the sender's term and drop samples from older terms.
6. **Bind failures retry without backoff** (Q14). Mapped to
   `Unreachable` in plan step 1a.

---

## Summary

| Question | Status | Recommended option | Risk if wrong | Cost |
|---|---|---|---|---|
| Q1 threshold | decided (1000 ms) | keep; re-measure under nextest load in step 2 | transient refusals delay failover; never unsafe (the mint-time confirmation) | small |
| Q2 bridge latency | resolved by reading; adopted (step 4) | no loop change; `confirm`'s single deadline taken before the send, no driver-side timeout | a server thread preempted between send and wait stretches the mint window by that preemption | small |
| Q3 torn reads | resolved by reading; adopted (step 3) | `Relaxed`, read once; no double-read | an early filter passes for one poll; mint-time term check refuses | none |
| Q4 mint refusal | decided (confirm, exit; step 4) | keep; docs name the supervisor restart; no restart-after-abandon test (cannot be produced on demand) | an abandoned node that does not rejoin cleanly goes unnoticed | small |
| Q5 unit-testing the mint | resolved by reading; adopted (step 4) | unit tests with the existing ring fixture | the refusal path stays untested | small |
| Q6 post-heal evidence | resolved by reading; adopted (step 1c) | divergence counter on primary; scan for `.divergent.<n>` / `.resync.<n>` siblings (not a bare prefix, which rotated segments match) | an absence check passes vacuously | small |
| Q7 public API | resolved by reading; adopted (step 0) | no CHANGELOG entry; test callers port too | none | none |
| Q8 DPDK runner | adopted (step 1b); one CI run to confirm | `Proxy::start_on`, raft proxies on 127.0.0.1, `raft_ip(i) = 127.0.0.(i+2)` | partition tests pass vacuously or fail only on DPDK | small |
| Q9 frozen links on heal | resolved; adopted (step 1b) | cut raft proxies, clear blocks, restore; hold blocked accepts without threads | flaky heal phase; thread growth | small |
| Q10 late primary bring-up | decided (step 1b test settles it) | keep `initialize`; assert leader id and term unchanged; fallbacks deferred | every later case runs with leadership off the replica | small |
| Q11 term of `read_log_id` | resolved by reading; adopted (step 4) | compare `read_log_id.leader_id.term` to the requested term; no metric pre-check | a confirmation of another term accepted | small |
| Q12 confirm latency | resolved by reading; adopted (step 4) | exact `NoQuorum` for stopped peers; set for frozen links | a rare flake, or an unnoticed openraft change | small |
| Q13 non-Linux | resolved by reading; adopted (step 1b) | no fallback; note the Linux assumption | none (the crate does not build off Linux) | small |
| Q14 bind edge cases | decided (startup refusal; step 1a) | clear errors, map to `Unreachable`, startup check in `melin_raft::spawn` sharing one helper with the connect path | log noise and an unclear error, no correctness impact | small |

## Decisions recorded

No question is still waiting on the user. These are the user's
decisions, each recorded in the plan:

1. **Q1:** the threshold is 1000 ms (plan §5).
2. **Q4:** a mint-time quorum confirmation, and exit on failure (plan
   step 4).
3. **Per-node loopback IPs with source-bound raft dialing** (plan
   step 1).
4. **Q14:** refusing to start on a raft bind/peer address-family
   mismatch (plan step 1a).
5. **Q10:** settled by the step 1b test; the fallbacks, one of which
   would add a late-joiner knob to `RaftConfig` (published) and need the
   user, are deferred until that test fails (plan §7).
6. **Finding 5:** the stale-peer-tip fix gets its own branch after this
   plan: a raft protocol version first, then envelopes stamped with a
   kernel boot stamp and a counter taken with the tip read (plan §8).
