# One response loop for both transports (plan, 2026-10)

Status: plan, for review before any code.

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
- a slot's reply is sent only once `gate.admit` has released it, and the
  shutdown arm of the admit drops the slot's reply and the rest of the batch;
- the consumer commits only after every slot of the batch cleared the gate,
  which is what the event publisher, chained behind it, relies on;
- `gate.after_batch` runs after dispatch, and `gate.idle_recheck` on the
  idle path.

The DPDK copy keeps these by comments that say "see `response::run`". The
copies have drifted: `transport-divergences-2026-10.md` lists three
response-stage divergences, and the heartbeat bug fixed in finding 27 of the
determinism audit was one of them. The goal is one loop that holds those
rules, and a narrow trait for what really differs: where the bytes go.

## Non-goals

- Changing what either transport puts on the wire, or when, except in the
  steps marked as behaviour changes below, each its own commit.
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
2. **Structural commits change no test.** In the commits that introduce and
   adopt the shared loop, no test body changes. Only a `use` path changes,
   and only where an item moved. A test that has to change in substance
   means the commit changed behaviour, and is split out.
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

What differs between the stages, and so becomes a trait method:

| Hook | Kernel TCP (`response.rs`) | DPDK (`dpdk_response.rs`) |
|---|---|---|
| Control event type, and draining it | `crate::ControlEvent`, with the socket writer; `Egress::drain_control` | `dpdk_response::ControlEvent`, ID only; `process_control_events` |
| Is a connection known; stamp its last send | `ConnectionEntry` in `Egress::connections` | `ConnectionHeartbeat` in the map |
| Deliver one slot's reply (payload + trailer) | `append_frames` into `send_buf`; overflow queued for removal | `push_frame` into the lane's TX ring |
| After each slot | `Egress::flush_if_due` | flush the lane's ring |
| Deliver an early reply | `Egress::release_early_replies` | `release_early_replies` |
| Before a gate wait (`GateWait::before_wait`) | `Egress::flush` | nothing |
| After early replies sent during a wait | `drop_overflowed`, then `flush` | nothing |
| Idle, after early replies | `drop_overflowed`, then `flush` | nothing |
| Heartbeat to a due connection | skip if blocked or over `MAX_SEND_BUF`; append, flush, tear down failures | `try_publish`; on a full ring, keep it due and record the stall |
| End of a batch, after commit | `drop_overflowed` | nothing |
| Shutdown | best-effort flush unless fenced | nothing |
| Server end-to-end latency sample | queued at append, closed at flush | recorded at push |
| Idle stats flush | its egress recorder as well | none of its own |
| Startup | io_uring setup, then the ready signal | none |

A sketch of the trait (names open to review):

```rust
/// Where a response stage's frames go: the one part of the stage that
/// differs by transport. Generic, never `dyn`: the loop is monomorphized
/// per transport, so every hook inlines as the code it replaces.
pub(crate) trait ResponseEgress {
    type Control;

    fn drain_control(&mut self, rx: &mpsc::Receiver<Self::Control>);
    fn send_slot(&mut self, connection_id: u64, payload: Option<Result<usize, &'static str>>,
                 encode_buf: &[u8], trailer: &[u8], now: Instant) -> Sent;
    fn after_slot(&mut self);
    fn release_early_replies<R: Copy, Q: Copy>(&mut self, /* … */) -> usize;
    fn before_wait(&mut self);
    fn after_early_replies(&mut self);
    fn on_idle(&mut self);
    fn send_heartbeats(&mut self, frame: &[u8], now: Instant, interval: Duration);
    fn after_batch(&mut self);
    fn on_shutdown(&mut self, fenced: bool);
}
```

`Sent` says whether the connection was known, so the shared loop keeps the
one "reply dropped: connection not registered" debug line and its
connection-0 exception. The two `GateWaitHook`s become one, generic over the
egress. Latency-trace and tick-to-trade hooks that only one side has stay
inside that side's egress, under the same `cfg`s as today.

The shared loop goes in a new module, `response_loop.rs`, which owns the
ordering comments now spread across both files. Each comment is written
once there, and the "see `response::run`" pointers go.

## Steps

### Step 0: pin both stages before touching them

Loop-level coverage today, by suite:

- **On both stages through one body:** `response::tests::early_release`,
  written against `StageHarness` and run on DPDK from
  `dpdk_response::tests::early_release` (the proptest included).
- **On both stages, as two separate bodies:** the degraded release
  (`degraded_release` in each file) and the change slot
  (`a_change_slot_sends_nothing_and_a_forced_one_waits_for_its_entry` and
  `a_change_slot_queues_no_frame_for_its_connection`).
- **On kernel TCP only:** `registration_race` (a reply that lands with its
  connection's `Connected`), `refusal_order` (a refusal is not released
  ahead of its connection) and `stop_while_gated` (a stopped or fenced stage
  leaves the gate and drops the held reply).
- **io_uring-specific, stays kernel-only:** `startup_report` (a ring that
  cannot be created).

Port the three kernel-only suites to `StageHarness`, so they run on both
stages before either loop changes. `stop_while_gated`'s fenced case has no
DPDK meaning (the DPDK stage has no flush to suppress) and keeps only its
stopped case on DPDK. Fold the two-body suites into one `StageHarness` body
each only if that is mechanical; otherwise leave them, since both already
run.

This step adds tests and changes no production code. A ported test that
fails on DPDK has found a divergence: it is recorded in
`transport-divergences-2026-10.md`, left out of the `dpdk` profile under the
entry's name (the doc's existing rule), and fixed before step 3.

### Step 1: bring the DPDK idle path in line (behaviour change, DPDK only)

The DPDK idle path reads the clock on every iteration for the policy
re-check. Its heartbeat timer also omits `past_spin_budget()`, which the
kernel stage needs so the heartbeat scan is not stretched to minutes on a
nearly saturated stage. Rework it, in place, into the kernel's shape:
`IDLE_HOUSEKEEPING_INTERVAL` gates one clock read, which feeds the
heartbeat scan's one-second cadence, `idle_recheck` and the stats flush.
The constant moves to a place both stages import.

Test: the idle-path cadence is not observable from outside except through
heartbeats and `/healthz`. The existing heartbeat tests and the degraded
release suite must stay green. A unit test pins the shared "is the scan
due" rule both stages now use.

Closes the divergence "DPDK idle path reads the clock every iteration".

### Step 2: the shared loop, kernel TCP on it

Add `response_loop.rs` with `ResponseEgress` and the loop, a straight lift
of `response::run`'s body. Implement the trait for the kernel `Egress`.
`response::run` keeps its startup (io_uring, ready signal) and its
signature, and calls the loop. DPDK is untouched.

Structural: no test changes. Also add a test-only recording egress in
`response_loop.rs`'s tests that logs every hook call. It pins the ordering
rules above as a sequence (the drain after the read, `before_wait` before a
held slot, `send_slot` only after admit, commit after the last slot,
`after_batch` after commit), so they are stated in one place, independent
of either transport.

### Step 3: DPDK on the shared loop

Implement `ResponseEgress` for the DPDK side. `dpdk_response::run` keeps its
signature and calls the loop. Its own loop and `GateWaitHook` go.

Structural: no test changes. The DPDK `StageHarness` suites, the
degraded-release suite, the change-slot test and the heartbeat tests all
drive `dpdk_response::run` and must pass unchanged under the netns runner.

Two small things the DPDK loop does differently today, decided here and not
silently: it prints no utilization under `pipeline-stats` at shutdown (the
kernel does; the shared loop will, for both), and it records the
end-to-end sample at push rather than at flush (kept, through its egress
hook).

### Step 4: heartbeat scheduling in the shared loop

The due rule (`now - last_send >= interval`) and the scan cadence move into
the loop. The egress decides only how to deliver a due heartbeat and what a
failure means: the kernel keeps its blocked-peer and buffer-cap guards and
teardown, DPDK keeps its retry and stall reporting. Puts `heartbeat_due`'s
doc comment back on `heartbeat_due` (it currently sits above `mark_dirty`).

Tests: the kernel `heartbeat` unit tests and the DPDK heartbeat and stall
tests stay, against their egress. One shared test pins the due rule.

### Step 5: DPDK reply push observes shutdown (behaviour change, DPDK only)

`push_frame` blocks in `push_with` until the TX ring has room and never
checks `shutdown`, so a wedged poll thread holds the stage past a shutdown
request. With the shared loop, `send_slot` can return a shutdown outcome
the loop must handle: the loop treats it as the gate's `Shutdown` arm (the
slot's reply and the rest of the batch are dropped, the batch is not
committed). Making it a variant of `Sent` means no egress can block without
the loop handling it.

Test: a DPDK stage whose TX ring is full and never drained returns from
`run` after `shutdown` is set, through the `StageHarness` shape. Closes
"DPDK reply push blocks without observing shutdown".

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
  27). Step 4 keeps the fix, now as the DPDK egress's heartbeat delivery.
- **DPDK reply push blocks without observing shutdown:** not fixed by the
  merge alone. Step 5 fixes it, and the shared loop makes the fix a type:
  a send that can give up on shutdown has to be handled by the loop.
- **DPDK idle path reads the clock every iteration:** fixed by step 1, and
  kept fixed structurally after step 3, since there is one idle path.
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

## Open questions for review

- Trait name and module (`ResponseEgress` in `response_loop.rs`), and
  whether `send_slot` takes the encoded payload or the encode result.
- Whether step 0's ported tests replace the kernel-only originals or sit
  beside them. Beside them is safer for this change; the originals can go
  later.
- Whether step 1 is wanted at all: it changes the DPDK idle path's clock
  cadence, which the latency floor is measured through.
