# io_uring vs DPDK: behavioural divergences (2026-10)

Found while deduplicating the kernel-TCP (io_uring) and DPDK paths. The
refactor itself is behaviour-preserving; everything below was left as it is
and is listed here for a decision. Each entry says which side looks wrong and
why. Some differences are deliberate, and those are marked.

Paths are relative to `crates/core/server-runtime/src/`.

The DPDK side of an entry can be exercised without hardware: the
integration suites run on DPDK over veth (`dpdk-testing.md`). A test that
fails on an entry is left out of the `dpdk` nextest profile
(`.config/nextest.toml`), under the entry's name, until the entry is
fixed. An entry says which tests cover it or are left out on it; the
others have no DPDK test yet.

## Client ingress

### DPDK closes a client connection without telling the peer

`DpdkTransport::close` (`crates/core/dpdk`) aborts and removes the socket
before anything is dispatched: no FIN, no RST. Every server-side close of a
client connection is affected, a failed auth included: the client gets
`AuthFailed` and then silence. `melin-client` returns on the error frame and is
unaffected, but a client that waits for EOF hangs until it next sends. io_uring
closes the socket, so the peer sees EOF.

Pinned by `a_server_side_close_is_silent` in the DPDK veth tests
(`crates/core/server-runtime/tests/dpdk_veth/`): no FIN or RST within a
read timeout after the close, and an RST answering the client's next send.
The fix flips that test to require the EOF.

Replication links no longer close this way: they go through
`DpdkTransport::reset`, which sends the RST before removing the socket
(see "Peer liveness on replication links" in `dpdk-replication.md`). A
client close could use the same call, or a graceful FIN, which needs the
socket kept until the FIN is acknowledged.

### DPDK `PipelineFull` drops the client instead of sending ServerBusy

`dpdk_transport.rs`, the `FrameAction::PipelineFull` arm. On a full input ring
DPDK closes the connection. io_uring keeps it and sends `ServerBusy` through
the response stage (`ControlEvent::PipelineBusy`). The `client_frames.rs`
contract ("signal backpressure, e.g. ServerBusy") matches io_uring. The DPDK
`ControlEvent` has no `PipelineBusy` variant.

**Fixed** by request IDs ([request-ids-2026-10.md](request-ids-2026-10.md)):
`FrameAction::PipelineFull` and `ControlEvent::PipelineBusy` are gone, and a
full ring is handled inside the shared `process_client_frames`, which sheds
the request (and the complete frames buffered behind it) through the
early-reply queue as `ServerBusy { id }` and returns `Continue`, so both
transports keep the connection. A full early-reply queue parks the connection
on both (`FrameAction::Park`). `tests/request_ids.rs`'s
`a_full_input_ring_answers_busy_and_keeps_the_connection` runs on DPDK through
the `dpdk` profile, and the poll loop's parking has its own netns cases in
`dpdk_transport.rs`.

### Pipelined first request after auth can stall on DPDK

`dpdk_transport.rs`. If the ChallengeResponse and the first request arrive in
the same segment, only the auth frame is processed that iteration. The request
bytes wait in `parse_buf` until another recv returns data, which may never
happen if the client is waiting for the reply. io_uring reads the auth frame
with `read_exact` and has no such stall.

### Admission counts differ

- io_uring gates `max_connections` on the shared `active_connections`, which
  counts authenticated connections only.
- DPDK gates on a local `connection_count` that includes connections still
  authenticating.

So the same `max_connections` admits different populations.

### Auth timeout defined twice (cosmetic)

The 5 s client auth timeout is a literal in `server.rs` (per read, so up to
~10 s across the two reads) and `AUTH_TIMEOUT` in `dpdk_transport.rs` (total
since accept).

## Response stage

Since the [shared response loop plan](shared-response-loop-2026-10.md), both
transports run one response loop (`response_loop.rs`), and what still
differs is only where the bytes go: each transport's egress
(`UringEgress` in `response.rs`, `DpdkEgress` in `dpdk_response.rs`).

### DPDK heartbeat SPSC-full drops the connection without closing it

`dpdk_response.rs`, the heartbeat scan. When `try_publish` fails, the entry is
removed and `active_connections` decremented, but the poll thread is never
told to close the socket. As a result:

- The client stays connected and every later reply is dropped as
  "connection not registered".
- Its `max_connections` permit is released while the socket is still open.
- The ring is per poll thread, so the victim is whichever connection the scan
  reached when the ring filled, not necessarily a slow one.

io_uring calls `teardown_dropped`, which `shutdown(2)`s the socket.

**Fixed** (finding 27 of the
[determinism audit](determinism-audit-2026-09.md)): a heartbeat that finds
the ring full is skipped and retried on the next scan, and the connection
stays registered until the poll thread's `Disconnected`. Heartbeats are
best effort, so there is nothing to close. Pinned by
`a_heartbeat_on_a_full_tx_ring_is_retried_and_keeps_the_connections` in
`dpdk_response.rs`. A lane whose heartbeats start finding the ring full
logs a `warn!` when they start, once per heartbeat interval while they
keep finding it full, and when they get through again or the lane has no
connections left
(`a_heartbeat_stall_is_reported_at_its_edges_and_while_it_lasts`,
`a_stalled_lane_left_without_connections_closes_its_stall`), since a poll
thread that falls behind otherwise leaves no trace.

### DPDK reply push blocks without observing shutdown

`dpdk_response.rs`, `push_frame` uses `push_with`, which blocks until the TX
ring has space and never checks `shutdown`. A wedged or dead poll thread can
hang the response stage past a shutdown request. The gate wait was
specifically fixed to avoid exactly this.

**Fixed** by step 4 of the
[shared response loop plan](shared-response-loop-2026-10.md), for the reply
push and the early-reply push both: `push_frame` now waits through
`spsc::Producer::push_with_unless`, which reads the shutdown flag only
while the ring is full, so a push with room goes through as before and
DPDK still finishes the batch in hand as kernel TCP does. A stopped reply
push returns `SlotOutcome::Stopped`, which leaves the shared loop through
its one epilogue, uncommitted; a stopped early-reply push drops that reply,
and the drain finishes without waiting. Pinned by
`a_reply_blocked_on_a_full_tx_ring_does_not_hold_the_stage_past_shutdown`
and
`an_early_reply_blocked_on_a_full_tx_ring_does_not_hold_the_stage_past_shutdown`
in `dpdk_response.rs` (a TX ring never drained), the egress's
`a_slot_on_a_full_lane_is_stopped_by_shutdown` and
`a_stopped_release_drops_what_would_wait_and_sends_what_has_room`, the
`push_with_unless_*` tests in `melin-pipeline`'s `spsc.rs`, and, for the
loop's exit, `a_stopped_send_commits_nothing_and_runs_only_the_epilogue`
in `response_loop.rs`.

### DPDK idle path reads the clock every iteration

`dpdk_response.rs` calls `Instant::now()` on every idle iteration for the
policy re-check. io_uring gates that read behind `IDLE_HOUSEKEEPING_INTERVAL`.
The DPDK heartbeat timer also omits the `past_spin_budget()` condition that
`response.rs` explains is needed to keep the heartbeat scan from stretching to
minutes on a nearly saturated stage.

**Fixed** by step 1 of the
[shared response loop plan](shared-response-loop-2026-10.md): both stages
now pace their idle housekeeping (heartbeat scan, policy re-check, stats
flush) through the shared `IdleHousekeeping` cadence in
`idle_housekeeping.rs`, one `IDLE_HOUSEKEEPING_INTERVAL` timer whose clock
read is masked only while spinning past the spin budget; since step 3
both stages run that idle path in the one loop they share
(`response_loop.rs`). Pinned by
`a_brief_spin_finds_the_heartbeat_scan_due_on_the_first_tick`,
`a_yielding_waiter_finds_the_heartbeat_scan_due_on_the_first_tick`,
`a_sustained_busy_spin_masks_the_clock_read` and
`the_heartbeat_scan_is_due_once_per_scan_interval` there.

## Replication

### DPDK sender `Handshaking` state has no deadline

`replication/dpdk.rs`. `AUTH_TIMEOUT` covers `Authenticating` only. An
authenticated replica that is still running but sends no Handshake holds
a slot until it disconnects (one that has stopped is reset by the link's
liveness deadline). TCP's 10 s read timeout covers both.

### TCP receiver leaks the replica pipeline on handshake errors (likely bug)

`replication/tcp_receiver.rs`. Every `?` from the handshake write through the
StreamStart/resync negotiation returns from `run_receiver` without
`teardown_replica_pipeline`. That includes `read_frame`, decode, the genesis
checks, `handle_resync_verdict`, and "unexpected response". DPDK wraps the
same cases in `fatal_err_dpdk!`.

### TCP receiver treats a quiet primary after auth as fatal

`replication/tcp_receiver.rs`. A primary that disconnects or times out (5 s)
after auth fails `run_receiver` fatally through `read_frame(...)?`. DPDK backs
off and reconnects.

### DPDK ignores `queue_send` failures for the handshake and heartbeats

`replication/dpdk.rs`. A dropped Handshake frame leaves both sides waiting.
Combined with the sender's missing `Handshaking` deadline, that hangs the slot.

## Primary startup

### DPDK never runs the event publisher (likely bug)

`server.rs`, `run_as_primary_dpdk`. With `--event-bind` set and a binary
that supplies an event publisher, the DPDK primary gives the publisher
its consumer on the output ring, as kernel TCP does, but never spawns the
publisher thread. The output ring's producer is gated on its slowest
consumer, so once the matching stage has produced a ring's worth of
output past that idle consumer it waits for good: the node stops
answering. Kernel TCP spawns the publisher (`spawn_event_publisher`),
whose endpoint is kernel TCP by design and would serve a DPDK node
unchanged. No example supplies a publisher, so no test reaches it. Found
while sharing the DPDK primary path with promotion, which kept the
startup as it was.

### DPDK primary does not notice a dead pipeline stage (likely bug)

`server.rs`. The kernel TCP primary's accept loop checks each pass
whether a stage thread that only exits on shutdown or a panic has exited
(`PipelineHandles::stage_died`). If one has, it logs `error!`, clears
`pipeline_healthy` and stops the node, which returns `pipeline failure`.
`run_as_primary_dpdk` runs `run_dpdk_poll`, which leaves only on the
shutdown flag and never makes that check. After the response stage
panics, for example, a DPDK primary:

- keeps accepting and authenticating clients, and answers none of their
  requests;
- keeps reporting healthy on its health endpoint, since `pipeline_healthy`
  is only cleared when the node is stopped (`stop_and_join`);
- reports the failure only when something else stops it.

An operator watching health sees a live node, and its clients see a node
that has gone silent. The fix is the same check in the DPDK poll loop,
rate-limited (the kernel loop's is once per accept pass) so it stays off
the hot path. Pinned by `an_encoder_that_skips_a_body_byte_stops_the_node`
in `tests/encoder_purity.rs` (a debug-build test, run on DPDK under the
netns runner), whose `#[cfg(feature = "dpdk")]` block asserts today's
DPDK behaviour: served on until stopped, the stop then reporting the
error. The fix replaces that block with the kernel TCP assertion.

## Deliberate differences (no action)

- Response stage: the DPDK egress's flush, overflow, heartbeat and
  shutdown hooks are simpler than the kernel's, on purpose. The reasons
  are in the doc comment on `impl Egress for DpdkEgress`
  (`dpdk_response.rs`), next to the hooks they describe.
