# One response loop for both transports (plan, 2026-10)

Status: plan, revised after four reviews, names, test retirement and the
shutdown check decided.

The seam table below describes the code as it is today, under today's
names; the kernel stage's `Egress` struct becomes `UringEgress` in step 2.

Paths are relative to `crates/core/server-runtime/src/`.

## Why

`response::run` (kernel TCP, io_uring) and `dpdk_response::run` are the
same algorithm written twice. Each step is a call into shared code
(`DurabilityGate`, `BatchBoundary`, `EarlyReplyQueue`, `encode_slot_payload`,
`encode_early_reply`, `BatchEnds`, `GateCrossTracker`, `AmortizedTimer`).
What is duplicated is the loop that calls them, and that loop carries the
ordering rules the stage's correctness depends on:

- the control channel is drained after the output ring is read, never
  before, so every slot's and early reply's `Connected` is seen;
- early replies go out only between two events' replies;
- per slot: `gate.admit` first, then `boundary.handled`, then the send; a
  slot's reply is sent only once the admit has released it;
- `GateWait::before_wait` runs at most once per admit, before its first
  wait, and `while_waiting` sends early replies only for a slot that is the
  first of its event;
- the batch's early-reply snapshot is released at each event's first slot
  *before* that slot's `gate.admit`, which is why a refusal never waits
  behind a held write;
- the batch's clock is read once per batch (`batch_now`), while early
  replies read it lazily, only when there is one to send;
- the admit's `Shutdown` arm drops the slot's reply and the rest of the
  batch, and goes back to the top of the loop, which exits; it is safe only
  because `wait_durable` read the flag itself (`durability_gate.rs`), so the
  top of the loop exits before reading the ring again (step 2 makes it a
  `break`, see there);
- the consumer commits only after every slot of the batch cleared the gate,
  which is what the event publisher, chained behind it, relies on;
- after the commit, the kernel's `drop_overflowed`, then `gate.after_batch`;
  `gate.idle_recheck` on the idle path.

The DPDK copy keeps these by comments that say "see `response::run`". The
copies have drifted: `transport-divergences-2026-10.md` lists three
response-stage divergences, and the heartbeat bug fixed in finding 27 of the
determinism audit was one of them. The goal is one loop that holds those
rules, and a narrow trait for what really differs: where the bytes go.

## Non-goals

- Changing what either transport puts on the wire, or when, except in the
  steps marked as behaviour changes below, each its own commit.
- Changing any log or stats text. Where the two stages name things
  differently (trace stages, the gate's label), the egress supplies the
  name, so the shared loop logs what each stage logs today.
- Touching client ingress, replication or primary startup. The divergences
  listed there live in other code (see the last section).
- A runtime choice of transport (`dyn`). The loop stays monomorphized per
  transport.

## Safety rules for the whole change

1. **Align before merging.** Where the two loops differ in a way that is
   not "where the bytes go", the DPDK loop is first brought in line with
   the kernel one *in place*, as its own small commit with its own test.
   The commits that move code into the shared loop are then purely
   structural.
2. **Structural commits change no test assertion.** In the commits that
   introduce and adopt the shared loop, no test's assertions change. What
   may change is a `use` path where an item moved, and the two test helpers
   that build a `GateWaitHook` (below), whose callers do not change. A test
   that has to change in substance means the commit changed behaviour, and
   is split out.
3. **Entry points stay put.** `response::run` and `dpdk_response::run` keep
   their signatures and become thin wrappers: build the transport's egress,
   run the shared loop. Every loop-level test, the `StageHarness` suites
   included, keeps driving the same function it drives today.
4. **Transport internals stay where they are.** `Egress`, `append_frames`,
   `flush_sends`, `FlushCadence`, `teardown_dropped` stay in `response.rs`.
   `push_frame`, `process_control_events`, `HeartbeatStalls`, `tx_lane`
   stay in `dpdk_response.rs`. Their unit tests move with nothing.
5. **Both test runs per step.** Every step passes the default suite and the
   `dpdk` nextest profile under `scripts/dpdk/netns-runner.sh`. On a host
   where the runner cannot start (AppArmor's
   `kernel.apparmor_restrict_unprivileged_userns`), CI's `dpdk` job is the
   gate, and the step is not merged before it is green.
6. **One reviewed commit per step**, in the order below, so a bisect lands
   on one idea.

## The seam

What differs between the stages, and so belongs to the egress:

| Concern | Kernel TCP (`response.rs`) | DPDK (`dpdk_response.rs`) |
|---|---|---|
| Control event type, and draining it | `crate::ControlEvent`, with the socket writer; `Egress::drain_control` | `dpdk_response::ControlEvent`, ID only; `process_control_events` |
| Deliver one slot's reply (payload + trailer) | `append_frames` into `send_buf`; overflow queued for removal | `push_frame` into the connection's lane |
| Stamp `last_send` | inside `append_frames`, only when bytes were appended without overflow | after every slot to a known connection, even when `push_frame` pushed nothing |
| After every slot, known connection or not | `Egress::flush_if_due` | flush the lane the slot pushed to, if it reached a known connection |
| Server end-to-end sample | queued after a clean append of a request's last slot, closed at the next flush | recorded right after the lane flush, for a request's last slot |
| Encode the payload, build the trailer (`gate.batch_end`, which counts degraded acks) | after the connection lookup, before the encode sample | after the connection lookup, before the encode sample |
| Traced encode sample (`encode_rec`, `tick-to-trade`) | wraps `append_frames`, not the encode | wraps `push_frame`, not the encode or the lane flush |
| Deliver early replies | `Egress::release_early_replies` (appends; can overflow) | `release_early_replies` (one TX frame each) |
| Before a gate wait | `Egress::flush` | nothing |
| After a release during a gate wait, only if it sent any | `drop_overflowed`, then `flush` | nothing |
| Idle, after a release at a boundary, whether it sent any or not | `drop_overflowed` | nothing |
| Idle, every iteration | `flush`: the steady-state flush, which ships whatever the last batch appended | nothing |
| Heartbeat to a due connection | skip if blocked or over `MAX_SEND_BUF`; append, flush, tear down failures | `try_publish`; on a full ring, keep it due and record the stall |
| After the commit | `drop_overflowed` | nothing |
| Idle stats flush, of the recorders the egress owns | `server_e2e_rec`; under `tick-to-trade`, `encode_rec` and `egress_rec` | `server_e2e_rec`; under `tick-to-trade`, `encode_rec` |
| Shutdown | unless fenced (it holds the `FenceState`), `send_dirty` and the close of the queued end-to-end samples, not `flush` (which would also tear down flush failures); `pipeline-stats` utilization print | nothing; no utilization print |
| Names in logs and stats | gate label `"response"`; trace stages "socket write", "response flush" | gate label `"dpdk response"`; trace stages "SPSC publish", "response SPSC publish" |
| Startup | io_uring setup, then the ready signal; returns whether it ran | none; returns `()` |

Shared, and so in the loop: reading the ring, the control drain and the
`pause_after_control_drain` test seam, the early-reply snapshot and boundary,
the admit and its outcome, the miss's debug line and its connection-0
exception, the spsc-wakeup, dispatch, journal-wait and replica-wait
samples, the commit, `after_batch`, the idle housekeeping timer and
`IDLE_HOUSEKEEPING_INTERVAL`, `MAX_BATCH` (defined in both files today),
`idle_recheck`, the utilization counters. The payload's encode, the
trailer and the encode sample stay in each egress, after its own lookup,
where they are today.

A sketch of the trait:

```rust
/// Where a response stage's frames go: the one part of the stage that
/// differs by transport. Generic, never `dyn`: the loop is monomorphized
/// per transport, so every hook inlines as the code it replaces.
pub(crate) trait Egress {
    type Control;

    /// The stage's name in the gate's log lines and the trace stages.
    fn names() -> StageNames;
    fn drain_control(&mut self, rx: &mpsc::Receiver<Self::Control>);
    /// One slot's reply to its connection, if this egress holds it: look it
    /// up, encode (`SlotReply::encode`), build the trailer
    /// (`SlotReply::trailer`), append or push under the encode sample,
    /// stamp its own `last_send`. `Unknown` makes the loop log the miss;
    /// nothing is encoded for it and no trailer built.
    fn send_slot<R: Copy, Q: Copy>(&mut self, connection_id: u64,
                                   reply: SlotReply<'_, R, Q>, now: Instant,
                                   /* step 4: */ shutdown: &AtomicBool) -> Sent;
    /// After every slot, sent or not.
    fn after_slot(&mut self);
    fn release_early_replies<R: Copy, Q: Copy>(&mut self, /* …, */
                                               /* step 4: */ shutdown: &AtomicBool)
                                               -> usize;
    fn before_wait(&mut self);
    /// After a release during a gate wait, called only if it sent any.
    fn after_wait_release(&mut self);
    /// After a release on the idle path, at a boundary, whether it sent any
    /// or not.
    fn after_idle_release(&mut self);
    /// Every idle iteration, after any release: the kernel's steady-state
    /// flush.
    fn on_idle(&mut self);
    fn send_heartbeats(&mut self, frame: &[u8], now: Instant, interval: Duration);
    fn after_commit(&mut self);
    /// The recorders this egress owns, `encode_rec` among them; the loop
    /// flushes its own.
    fn flush_idle_stats(&mut self);
    /// The kernel egress decides from its own fence state.
    fn on_shutdown(&mut self);
}

/// What `send_slot` hands over: what encoding the payload needs (the
/// payload, the encoder, the encode buffer), what building the trailer
/// needs (`&DurabilityGate`, `&mut BatchEnds`, the slot's backing and
/// request ID, `is_last_in_request`), and what the e2e sample needs
/// (`recv_ts`). The egress calls `encode()` and `trailer()` after its
/// lookup, never before.
struct SlotReply<'a, R: Copy, Q: Copy> { /* … */ }

enum Sent { Delivered, Unknown, /* step 4: */ Stopped }
```

Names, decided:

- **`Egress`** for the trait, implemented by **`UringEgress`** (today's
  kernel `Egress` struct, renamed) and **`DpdkEgress`** (new: the DPDK
  stage's connection map, TX producers, heartbeat stall tracker and
  connection counter, today loose locals in `dpdk_response::run`). The
  symmetric pair says the two are interchangeable halves of one stage;
  `ResponseEgress` beside a struct called `Egress` would have left the
  kernel side looking like the default.
- **`response_loop.rs`** for the loop, the trait and the generic
  `GateWaitHook`. Putting the loop in `response.rs` would make it look
  kernel-owned; turning `response/` into a directory with a file per
  transport is tidier but moves both stage files, breaking their blame and
  every test's `use` paths in one commit. That can follow later, once the
  loop has settled.
- **`SlotReply`** for `send_slot`'s argument, not the whole `OutputSlot`,
  which would hand the egress the gate's inputs (`wire_seq`, the payload
  kind) it has no business reading. It carries what encoding needs rather
  than an encode result, so each egress encodes after its own lookup, as
  both stages do today: a reply to an unknown connection, connection 0's
  server-originated events among them (every tick, on a node with a tick
  interval), is never encoded. The same goes for the trailer:
  `gate.batch_end` counts each degraded terminator for
  `melin_degraded_acks_total` ("called once per request"), so building it
  for a miss would count, under a degraded release, every tick and every
  reply to a connection gone mid-batch, with no frame sent, against a
  metric `tests/degraded_acks.rs` pins. `trailer()` on `SlotReply` is the
  only way to `batch_end`, and only an egress that found the connection
  calls it. (`batch_end` takes `&self`, so `SlotReply` can borrow the gate
  once `admit` has returned.) Each stage also handles an encode error
  itself today (log it, still send the terminator), with tests on it;
  resolving it once in the loop is the follow-up under "After this plan".
  `recv_ts` is not feature-gated on the slot, so `SlotReply` needs no
  `cfg` of its own.

The traced encode sample (`encode_rec`) moves into each egress, which
records it around its own append or push, as each loop does today: not the
encode, which comes before it, and for DPDK not the lane flush either.
`DpdkEgress` does not flush in `send_slot`: it keeps the lane it pushed to,
with the slot's `recv_ts` and `is_last_in_request`, in an `Option`, and
`after_slot` takes it, flushes that lane and records the end-to-end sample,
which is today's order. Taken, not read: `after_slot` also runs after a
later miss or a change slot, which must neither flush again nor record the
sample a second time with a stale `recv_ts`.

The two `GateWaitHook`s become one, generic over the egress: it borrows
`&mut E`, the `Registered` snapshot and `&Receiver<E::Control>` exactly as
today's hooks do, and `admit` already takes `impl GateWait`. The latency
recorders only one side has stay inside that side's egress, under the same
`cfg`s as today.

The shared loop goes in a new module, `response_loop.rs`, which owns the
ordering comments now spread across both files. Each comment is written
once there, and the "see `response::run`" pointers go.

## Steps

### Step 0: pin both stages before touching them (tests only)

Loop-level coverage today, by suite:

- **On both stages through one body:** `response::tests::early_release`,
  written against `StageHarness` and run on DPDK from
  `dpdk_response::tests::early_release` (the proptest included).
- **On both stages, as two separate bodies:** the degraded release
  (`degraded_release` in each file).
- **Partly on both:** the change slot. Both stages pin that a change slot
  sends nothing to its connection
  (`a_change_slot_sends_nothing_and_a_forced_one_waits_for_its_entry`,
  `a_change_slot_queues_no_frame_for_its_connection`); only the kernel test
  also pins that a forced change waits for its own entry.
- **On kernel TCP only:** `registration_race` (a reply that lands with its
  connection's `Connected`), `refusal_order` (a refusal is not released
  ahead of its connection) and `stop_while_gated` (a stopped or fenced stage
  leaves the gate and drops the held reply).
- **The wait hook, per stage:** `response::tests::gate_wait_hook` and the
  `gate_wait_hook_*` tests in `dpdk_response::tests::early_release`. Each
  builds its hook in one helper (`one_turn`, `hook_turn`).
- **io_uring-specific, stays kernel-only:** `startup_report` (a ring that
  cannot be created).

Extend the harness first:

- **The drain pause.** `StageInputs` gains the `pause_after_control_drain`
  seam, which both `run` functions already accept and both `start_*`
  functions pass as `None`. `rig()` builds `StageInputs` itself and
  connects every connection right after the stage starts, so it gains the
  pause as a parameter and lets a test start with no connection.
  `registration_race` needs both: it parks the stage, then connects
  connection 1 itself. `refusal_order` needs only the pause (connection 1
  is registered by the second iteration either way).
- **The forced-policy cell.** `start_uring` and `start_dpdk` build
  `forced_policy_cell()` internally and keep neither half. `StageInputs`
  carries the reader half, and the test keeps the writer, so the
  forced-change port can force a policy.
- **A stop probe.** `StageHarness` gains `stopped_within(Duration) -> bool`.
  `stop_while_gated` catches a hang, and both harnesses' `Drop` joins the
  stage thread, so a probe that timed out and then failed its assertion
  would hang in `Drop` on exactly the wedge it exists to catch. The probe
  sets the harness's `shutdown` flag, takes the thread handle and polls
  `is_finished()` until the deadline. If the thread finished, it joins it
  and propagates a panic, so a stage that panicked out of the gate fails
  the test instead of passing as stopped. Only on a timeout does it drop
  the handle (detaching the thread) and return `false`; `Drop` then has
  nothing to join.
- **A full TX ring** needs no new knob for step 4's test. The output ring
  commits per batch, so publishing one more single-frame slot than the DPDK
  harness's TX ring holds, with nothing reading the ring, blocks the stage
  on its last push; the early-reply variant likewise needs one more early
  reply than the ring holds, since exactly the capacity fills it without
  blocking. The test takes the count from a harness constant for the TX
  ring's size, which `start_dpdk` uses too, not a repeated literal.

The DPDK side of every port runs only in the `dpdk` feature build
(`dpdk_response` is compiled under that feature), so never in the default
suite or on a host without libdpdk. That is where the DPDK stage's tests
already run; the transport-independent pin is step 2's recording-egress
test, which is ungated.

Then port to `StageHarness`, so they run on both stages before either loop
changes:

- `registration_race`, `refusal_order` and `stop_while_gated`, as new
  `StageHarness` bodies beside the kernel originals, which stay until step
  5. `registration_race` asserts the reply's value through its socket
  today (`new_value == 7`); the harness sees an application frame, not its
  value, which still pins what the race is about (the reply arrives).
  `stop_while_gated`'s fenced case has no DPDK meaning (the DPDK stage has
  no flush to suppress) and keeps only its stopped case there.
- the forced change waiting for its own entry, so the DPDK stage pins it
  too.

This step changes no production code. A ported test that fails on DPDK has
found a divergence: it is recorded in `transport-divergences-2026-10.md`,
left out of the `dpdk` profile under the entry's name (the doc's existing
rule), and fixed before step 3.

### Step 1: bring the DPDK idle path in line (behaviour change, DPDK only)

Three changes to `dpdk_response::run`, in place, one commit each:

1. **Housekeeping cadence.** Today the policy re-check reads the clock on
   every idle iteration, and the heartbeat timer, `tick(1 s,
   waiter.spinning())`, masks its read whenever the waiter is spinning, so
   on a nearly saturated stage, which spins only briefly between batches,
   the heartbeat scan can stretch to many seconds. Take the kernel's
   shape: one `IDLE_HOUSEKEEPING_INTERVAL` timer, masked only when spinning
   and past the spin budget, feeding a one-second heartbeat scan,
   `idle_recheck` and the stats flush. `idle_recheck` is rate-limited
   inside the gate, so its cadence does not change; only the clock reads
   it costs do. The constant moves to a place both stages import.
2. **`pipeline-stats` utilization print at shutdown**, which the kernel
   stage has and DPDK lacks. A one-line commit (plus `print_utilization`
   becoming `pub(crate)`), so step 3 adds nothing.
3. **A shared `heartbeat_due(last_send, now, interval)`** free function,
   which DPDK's scan calls. The kernel's `heartbeat_due` becomes that rule
   plus its blocked-peer and buffer-cap guards, and its doc comment goes
   back on it (it currently sits above `mark_dirty`).

Tests: the existing heartbeat tests and the degraded-release suite stay
green; a unit test pins the shared due rule and one pins the scan's
due-ness from the housekeeping timer. The cadence change itself is not
observable from outside except through heartbeat timing.

Closes the divergence "DPDK idle path reads the clock every iteration".

### Step 2: the shared loop, kernel TCP on it

Two commits.

1. **Rename** the kernel stage's `Egress` struct to `UringEgress`, and
   nothing else, so the next commit's diff shows only the loop moving. The
   tests that name it (`early_drain`, `gate_wait_hook`) change only the
   identifier.
2. **The loop.** Add `response_loop.rs` with the `Egress` trait and the
   loop, a straight lift of `response::run`'s body. Implement the trait for
   `UringEgress`, which takes the `FenceState`. `response::run` keeps its
   startup (io_uring, ready signal), its signature and its return value,
   and calls the loop. DPDK is untouched.

**One exit.** Today the admit's `Shutdown` arm goes back to the top of the
loop (`continue 'run`), which exits only because `wait_durable` read the
flag before returning: if it ever returned `Shutdown` with the flag clear,
the loop would read the same uncommitted batch again and resend every reply
already sent. The shared loop gives every exit one path instead: the top's
shutdown check, the admit's `Shutdown` and, from step 4, `Sent::Stopped`
all `break 'run` to one epilogue after the loop (the kernel's fenced flush,
the utilization counters, the `pipeline-stats` print). Today the flag is
set in every case that reaches the admit's arm, so this changes nothing
observable; it removes the dependency on who read the flag.

Structural. Besides the rename, the test changes are:

- `gate_wait_hook::one_turn`, which builds the hook: it builds the generic
  one, and the tests that call it do not change;
- `early_drain::drain_burst` calls `release_early_replies`, which becomes a
  trait method, so the module gains a `use` of the trait.

Also add a test-only recording egress in `response_loop.rs`'s tests that
logs every hook call. It pins the ordering rules above as a sequence: the
drain after the read; `release_early_replies` only between events, and at
an event's first slot before its admit; `before_wait` once per held admit;
`send_slot` only after the admit; `after_slot` for every slot;
`after_wait_release` only after a release during a wait that sent any;
the commit after the last slot; `after_commit` before `after_batch`; and no
commit, and no further hook but the epilogue's, after any exit. They are
then stated once, independent of either transport, and the test runs in
every build. (`boundary.handled` is the loop's own state, not a hook, so
the recording egress cannot see it; "only between events" is what it pins
in its place.)

### Step 3: DPDK on the shared loop

Gather the DPDK stage's egress state into `DpdkEgress` and implement
`Egress` for it. `send_slot` encodes, pushes into the slot's lane under the
encode sample, stamps `last_send` and keeps the lane in an `Option`;
`after_slot` takes it, flushes the lane and records the end-to-end sample,
which is today's order (see `SlotReply` above).
`dpdk_response::run` keeps its signature and calls the loop. Its own loop
and `GateWaitHook` go.

Structural. The only test change is `early_release::hook_turn`, as in step
2. Its callers assert on the connection map it returns, so it keeps
returning that map (now the egress's), and they do not change. The DPDK
`StageHarness` suites, step 0's ports, the degraded-release
suite, the change-slot test and the heartbeat tests all drive
`dpdk_response::run` and must pass unchanged under the netns runner.

### Step 4: DPDK pushes observe shutdown (behaviour change, DPDK only)

Both DPDK pushes block in `push_with` until the TX ring has room and never
check `shutdown`: the reply push in `send_slot`, and the early-reply push,
which runs on the idle path and inside the gate's `while_waiting`. A wedged
poll thread holds the stage past a shutdown request.

- **When the flag is read: only while waiting for room.** A push with
  room goes through as today, whatever the flag says, so DPDK still
  finishes the batch in hand, as kernel TCP does (its appends never read
  the flag; it notices a shutdown at a gate wait or at the top of the
  loop). This step removes the hang and nothing else: no batch is cut
  where it was not, and the hot path, a push with room, reads no new
  atomic. Reading the flag before every push was weighed and rejected: it
  would cut DPDK batches the kernel finishes, a new divergence inside a
  change meant to remove them, for one more atomic load per reply. Whether
  a stage should stop mid-batch at all is a fencing question, recorded
  under "After this plan".
- **The wait lives in the SPSC producer.** `push_with` already waits for
  room, flushing its pending writes as it spins so a consumer that has
  drained everything published can see them and free a slot. A second wait
  loop in `DpdkEgress` with a shutdown check added would be a copy of that
  logic. Instead `spsc.rs` gains `Producer::push_with_unless(f, stop:
  &AtomicBool) -> Result<u64, Stopped>`: it tries the push first, and only
  while the ring is full spins, flushing as `push_with` does and reading
  `stop` on each turn, at the top of the turn, before the waiter idles (as
  `wait_durable` does). Read after the idle, every push that finds the
  ring full once the flag is set would pay a fresh waiter's spin budget
  before returning, and a stopped early-reply drain makes one such push
  per reply left in its snapshot. `push_with` becomes a wrapper over the
  same wait (a stop that never fires), so the wait is written once. Unit
  tests: it
  pushes when there is room, even with the flag set; it waits while the
  ring is full; it returns `Stopped`, having written nothing, when the flag
  is set while it waits; `push_with`'s own tests stay as they are. Both
  DPDK pushes already go through `push_frame`, which becomes the one
  caller.
- **The `shutdown` parameter arrives here**, on `send_slot` and
  `release_early_replies` together, as `Sent::Stopped` does, so steps 2 and
  3 stay structural. Three test helpers that call them directly gain the
  argument, a signature-only edit with no assertion changed:
  `gate_wait_hook::one_turn`, `early_release::hook_turn` (through the
  generic hook) and `early_drain::drain_burst`.
- **The reply push.** `send_slot` returns `Sent::Stopped` when the push was
  stopped. Step 2's single exit handles
  it: the loop breaks to the epilogue, without committing and without
  reading the ring again. The `Sent` variant gives an egress a way to report
  a stop, not an obligation to check; `DpdkEgress` is the one that must.
- **The early-reply push.** `release_early_replies` takes `shutdown` too.
  `Registered::drain` cannot be cut short by the closure it calls, so a
  stop does not end the drain: once the flag is set, each remaining reply
  of the snapshot is still pushed if the ring has room, and dropped if the
  push would have to wait, as a shutdown drops every unsent reply. Either
  way the drain finishes without waiting. On the idle path the loop
  reaches the top's shutdown check next and exits. Inside `while_waiting`,
  the gate's own wait reads the flag on its next turn and returns
  `Shutdown`, which exits the same way.

Test: a DPDK stage whose TX ring is full and never drained returns from
`run` after `shutdown` is set, once with a reply pending and once with an
early reply, through the `StageHarness` shape and its `stopped_within`
probe. Closes "DPDK reply push blocks without observing shutdown".

### Step 5: retire the kernel-only originals (tests only)

Through steps 1 to 4, the kernel-only originals are the strongest evidence
that the structural steps are structural: they did not change. Once the
refactor has landed, keeping them beside their step-0 ports is duplicated
test logic, which drifts. Each original goes only if its port asserts at
least everything it does on that transport:

| Original | Decision |
|---|---|
| `refusal_order` | Retire: the port reads frames as `Seen`, as the original does. |
| `stop_while_gated`, stopped case | Retire: the port covers it through `stopped_within`. |
| `stop_while_gated`, fenced case | Keep, as a kernel-only test slimmed to that case: it has no DPDK meaning. |
| `registration_race` | Retire, accepting the loss of its `new_value == 7` check: the race is about whether the reply arrives, which the port pins. |
| The two change-slot tests | Fold into one `StageHarness` body, step 0's port of the forced half included, and retire both. |

A separate commit from the refactor, so the history keeps "the loop moved"
and "the tests were consolidated" apart.

### Step 6: docs

Update `transport-divergences-2026-10.md` for the entries closed, and move
the "Deliberate differences" bullets on the response stage into the
egress impls' docs, where they are now code.

## Performance acceptance

The loop is on the hot path of every reply. The trait is generic, so each
hook should compile to the code it replaces. That is checked, not assumed:

- before and after steps 2 and 3, the LAN bench suite's echo closed-loop
  round trip on kernel TCP and on DPDK (p50, p99, p99.9) and the
  tick-to-trade breakdown, on the same hosts and build flags;
- a regression beyond run-to-run noise blocks the step, and is profiled
  before anything is micro-optimized (`#[inline]` on hooks is the first
  suspect).

## What this does to the divergence list

Response stage, which this plan touches:

- **DPDK heartbeat SPSC-full drops the connection:** already fixed (finding
  27). The fix stays in the DPDK egress's heartbeat delivery.
- **DPDK reply push blocks without observing shutdown:** not fixed by the
  merge alone. Step 4 fixes it, for the reply push and the early-reply
  push both.
- **DPDK idle path reads the clock every iteration:** fixed by step 1, and
  kept fixed after step 3, since there is one idle path.
- **Deliberate differences** (no flush cadence or pre-gate flush on DPDK,
  no fenced-shutdown flush, no buffer checks on DPDK heartbeats) stay, as
  the DPDK egress's no-op or simpler hooks instead of comments.

Not touched, because they are in other code:

- client ingress (`dpdk_transport.rs`, `crates/core/dpdk`): the silent
  close, the pipelined first request after auth, admission counts, the auth
  timeout defined twice;
- replication (`replication/`): the DPDK `Handshaking` deadline, the TCP
  receiver's pipeline leak and its fatal quiet primary, DPDK's ignored
  `queue_send` failures;
- primary startup (`server.rs`): the DPDK primary never spawning the event
  publisher. That one stops a node that has a publisher configured, and is
  worth fixing on its own, ahead of this plan.

## After this plan

- **A byte-level egress.** The loop encodes early replies itself, with the
  shared `encode_early_reply`, and resolves an encode error once, handing
  the egress only bytes. That would fold the duplicated "early reply
  dropped" and "encode error" lines into the loop. It changes the
  `append_early_reply` and `push_frame` tests, so it is a follow-up after
  step 3, not part of it.
- **Decide whether a fenced stage must stop mid-batch.** The gate reads
  the shutdown flag only when a slot has to wait (`wait_durable`): a slot
  already durable is admitted without reading it. So once `shutdown` is set,
  replies the gate already cleared can still go out, on both transports.
  On kernel TCP, three paths ship them:
  - the pre-wait flush, the largest: the first slot that has to wait runs
    `before_wait` (`hook.starting()` in `admit_slot`) *before*
    `wait_durable` reads the flag, and so sends everything buffered, this
    batch's cleared slots and, under load, earlier ones not yet flushed;
  - the byte-threshold flush between slots;
  - the slot-count flush between slots (`FLUSH_SLOT_INTERVAL`).

  On DPDK, every cleared slot is pushed as it is handled. A fence co-sets
  `shutdown`, so a superseded ex-primary can do the same. The question is
  whether that can acknowledge
  an event the new primary does not hold, settled against the failover
  guarantee. If it can, it is a correctness bug on both transports today,
  to fix for its own sake and ahead of other work. The candidate fixes,
  each a both-transport change in the shared loop, with its own test and
  the hot-path bench, and either one placed at the top of each slot,
  *before* `gate.admit`, since a check after it comes too late for the
  pre-wait flush:
  - check `shutdown` once per slot: a prompt, uniform stop, at the price of
    dropping replies the gate already cleared on every graceful shutdown;
  - check the fence once per slot: a fenced node stops mid-batch, a
    graceful shutdown still finishes its cleared batch. The kernel egress
    holds the fence state already; the DPDK stage would need it passed in.

  A per-slot check stops only what the stage has yet to send: on kernel
  TCP, replies an earlier slot's flush already handed to the kernel are
  gone, so the answer may also bear on the flush triggers themselves.

  If it cannot, nothing changes: the batch in hand is legitimately
  acknowledged.

## Open questions for review

None left: the names are under "The seam", and the originals' retirement
is step 5.
