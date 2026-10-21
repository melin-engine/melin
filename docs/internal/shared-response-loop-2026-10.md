# One response loop for both transports (plan, 2026-10)

Status: plan, revised after one review, for review before any code.

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
- the admit's `Shutdown` arm drops the slot's reply and the rest of the
  batch, and goes back to the top of the loop, which exits; it is safe only
  because `wait_durable` read the flag itself (`durability_gate.rs`), so the
  top of the loop exits before reading the ring again;
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
| Deliver one slot's reply (payload + trailer) | `append_frames` into `send_buf`; overflow queued for removal | `push_frame` into the connection's lane, then that lane's `flush` |
| Stamp `last_send` | inside `append_frames`, only when bytes were appended without overflow | after every slot to a known connection, even when `push_frame` pushed nothing |
| After every slot, known connection or not | `Egress::flush_if_due` | nothing (the lane was flushed in the send) |
| Server end-to-end sample | queued after a clean append of a request's last slot, closed at the next flush | recorded right after the lane flush, for a request's last slot |
| Deliver early replies | `Egress::release_early_replies` (appends; can overflow) | `release_early_replies` (one TX frame each) |
| Before a gate wait | `Egress::flush` | nothing |
| After early replies sent during a wait | `drop_overflowed`, then `flush` | nothing |
| Idle, after early replies | `drop_overflowed`, then `flush` | nothing |
| Heartbeat to a due connection | skip if blocked or over `MAX_SEND_BUF`; append, flush, tear down failures | `try_publish`; on a full ring, keep it due and record the stall |
| After the commit | `drop_overflowed` | nothing |
| Idle stats flush | `server_e2e_rec`, and `egress_rec` under `tick-to-trade` | `server_e2e_rec` |
| Shutdown | best-effort flush unless fenced (it holds the `FenceState`); `pipeline-stats` utilization print | nothing; no utilization print |
| Names in logs and stats | gate label `"response"`; trace stages "socket write", "response flush" | gate label `"dpdk response"`; trace stages "SPSC publish", "response SPSC publish" |
| Startup | io_uring setup, then the ready signal; returns whether it ran | none; returns `()` |

Shared, and so in the loop: reading the ring, the control drain and the
`pause_after_control_drain` test seam, the early-reply snapshot and boundary,
the admit and its outcome, the miss's debug line and its connection-0
exception, payload encoding and the trailer, the spsc-wakeup, dispatch,
journal-wait, replica-wait and encode samples, the commit, `after_batch`,
the idle housekeeping timer, `idle_recheck`, the utilization counters.

A sketch of the trait (names open to review):

```rust
/// Where a response stage's frames go: the one part of the stage that
/// differs by transport. Generic, never `dyn`: the loop is monomorphized
/// per transport, so every hook inlines as the code it replaces.
pub(crate) trait ResponseEgress {
    type Control;

    /// The stage's name in the gate's log lines and the trace stages.
    fn names() -> StageNames;
    fn drain_control(&mut self, rx: &mpsc::Receiver<Self::Control>);
    /// One slot's frames to `slot`'s connection, if this egress holds it,
    /// with its own `last_send` stamp and e2e sample. `Unknown` makes the
    /// loop log the miss.
    fn send_slot(&mut self, connection_id: u64, slot: SlotFrames<'_>,
                 now: Instant, shutdown: &AtomicBool) -> Sent;
    /// After every slot, sent or not.
    fn after_slot(&mut self);
    fn release_early_replies<R: Copy, Q: Copy>(&mut self, /* … */) -> usize;
    fn before_wait(&mut self);
    fn after_early_replies(&mut self);
    fn on_idle(&mut self);
    fn send_heartbeats(&mut self, frame: &[u8], now: Instant, interval: Duration);
    fn after_commit(&mut self);
    fn flush_idle_stats(&mut self);
    /// The kernel egress decides from its own fence state.
    fn on_shutdown(&mut self);
}

/// What `send_slot` hands over: the encode result, the encode buffer, the
/// trailer, and what the e2e sample needs (`recv_ts`,
/// `is_last_in_request`).
struct SlotFrames<'a> { /* … */ }

enum Sent { Delivered, Unknown, /* step 5: */ Stopped }
```

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

- `StageInputs` gains the `pause_after_control_drain` seam, which both `run`
  functions already accept and both `start_*` functions pass as `None`.
  `registration_race` and `refusal_order` need it.
- `StageHarness` gains a `stopped_within(Duration)` probe. `stop_while_gated`
  catches a hang, so it polls the stage thread rather than joining it; the
  harness's join on drop would hang on exactly the failure it looks for.

Then port to `StageHarness`, so they run on both stages before either loop
changes:

- `registration_race`, `refusal_order` and `stop_while_gated`, as new
  `StageHarness` bodies beside the kernel originals, which stay for this
  change. `registration_race` asserts the reply's value through its socket
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
   stage has and DPDK lacks. A one-line commit, so step 3 adds nothing.
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

Add `response_loop.rs` with `ResponseEgress` and the loop, a straight lift
of `response::run`'s body. Implement the trait for the kernel `Egress`,
which takes the `FenceState`. `response::run` keeps its startup (io_uring,
ready signal), its signature and its return value, and calls the loop. DPDK
is untouched.

Structural. The only test change is `gate_wait_hook::one_turn`, which builds
the hook: it builds the generic one, and the tests that call it do not
change.

Also add a test-only recording egress in `response_loop.rs`'s tests that
logs every hook call. It pins the ordering rules above as a sequence (the
drain after the read, `before_wait` once per held admit, `boundary.handled`
before the send, `send_slot` only after the admit, `after_slot` for every
slot, the commit after the last slot, `after_commit` before `after_batch`),
so they are stated in one place, independent of either transport.

### Step 3: DPDK on the shared loop

Implement `ResponseEgress` for the DPDK side. `send_slot` pushes and flushes
the slot's lane, stamps `last_send` and records the e2e sample, as the loop
does today; `after_slot` does nothing. `dpdk_response::run` keeps its
signature and calls the loop. Its own loop and `GateWaitHook` go.

Structural. The only test change is `early_release::hook_turn`, as in step
2. The DPDK `StageHarness` suites, step 0's ports, the degraded-release
suite, the change-slot test and the heartbeat tests all drive
`dpdk_response::run` and must pass unchanged under the netns runner.

### Step 4: DPDK pushes observe shutdown (behaviour change, DPDK only)

Both DPDK pushes block in `push_with` until the TX ring has room and never
check `shutdown`: the reply push in `send_slot`, and the early-reply push,
which runs on the idle path and inside the gate's `while_waiting`. A wedged
poll thread holds the stage past a shutdown request.

- `send_slot` already takes `shutdown`. The DPDK egress pushes with a wait
  that checks it, and returns `Sent::Stopped` if it is set while the ring is
  full. The loop treats `Stopped` as the exit: it **breaks to the shutdown
  epilogue**, without committing and without going back to the top of the
  loop. Going back would be wrong if an egress ever returned `Stopped`
  with the flag clear: the loop would read the same uncommitted batch again
  and resend every slot already delivered. The `Sent` variant gives an
  egress a way to report a stop, not an obligation to check; the DPDK
  egress is the one that must.
- The early-reply release takes `shutdown` the same way and stops releasing
  when it is set; the loop checks the flag after it and exits. Inside
  `while_waiting`, the gate's own wait reads the flag on its next turn.

Test: a DPDK stage whose TX ring is full and never drained returns from
`run` after `shutdown` is set, once with a reply pending and once with an
early reply, through the `StageHarness` shape and its `stopped_within`
probe. Closes "DPDK reply push blocks without observing shutdown".

### Step 5: docs

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

A byte-level egress: the loop encodes early replies itself, with the shared
`encode_early_reply`, and resolves an encode error once, handing the egress
only bytes. That would fold the duplicated "early reply dropped" and
"encode error" lines into the loop. It changes the `append_early_reply` and
`push_frame` tests, so it is a follow-up after step 3, not part of it.

## Open questions for review

- Trait name and module (`ResponseEgress` in `response_loop.rs`), and the
  shape of `SlotFrames`.
- Step 0's ported tests sit beside the kernel-only originals. Whether to
  retire the originals afterwards.
