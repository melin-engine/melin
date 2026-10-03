# io_uring vs DPDK: behavioural divergences (2026-10)

Found while deduplicating the kernel-TCP (io_uring) and DPDK paths. The
refactor itself is behaviour-preserving; everything below was left as it is
and is listed here for a decision. Each entry says which side looks wrong and
why. Some differences are deliberate, and those are marked.

Paths are relative to `crates/core/server-runtime/src/`.

## Client ingress

### DPDK auth failure leaves the connection open (likely bug)

`dpdk_transport.rs`, `process_auth_frame` / `send_auth_failed`. A bad
signature, an undecodable ChallengeResponse, or an oversized auth frame sends
`AuthFailed` but leaves the connection in `WaitingForResponse`.

- **Bad signature.** The client can retry signatures against the same nonce
  until `AUTH_TIMEOUT`, getting an `AuthFailed` per attempt.
- **Oversized frame.** The bytes are never consumed, so every later recv
  re-sends `AuthFailed` until `MAX_PARSE_BUF` or the timeout ends it.

io_uring drops the connection after the first failure.

### DPDK `PipelineFull` drops the client instead of sending ServerBusy

`dpdk_transport.rs`, the `FrameAction::PipelineFull` arm. On a full input ring
DPDK closes the connection. io_uring keeps it and sends `ServerBusy` through
the response stage (`ControlEvent::PipelineBusy`). The `client_frames.rs`
contract ("signal backpressure, e.g. ServerBusy") matches io_uring. The DPDK
`ControlEvent` has no `PipelineBusy` variant.

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

### DPDK client framing tests test a copy

`dpdk_transport.rs` tests exercise a test-local `try_extract_frame`, not
`process_client_frames`. They give no coverage of the production path.

## Response stage

### DPDK heartbeat SPSC-full drops the connection without closing it (likely bug)

`dpdk_response.rs`, the heartbeat scan. When `try_publish` fails, the entry is
removed and `active_connections` decremented, but the poll thread is never
told to close the socket. As a result:

- The client stays connected and every later reply is dropped as
  "connection not registered".
- Its `max_connections` permit is released while the socket is still open.
- The ring is per poll thread, so the victim is whichever connection the scan
  reached when the ring filled, not necessarily a slow one.

io_uring calls `teardown_dropped`, which `shutdown(2)`s the socket.

### DPDK reply push blocks without observing shutdown

`dpdk_response.rs`, `push_frame` uses `push_with`, which blocks until the TX
ring has space and never checks `shutdown`. A wedged or dead poll thread would
hang the response stage past a shutdown request. The gate wait was
specifically fixed to avoid exactly this.

### DPDK idle path reads the clock every iteration

`dpdk_response.rs` calls `Instant::now()` on every idle iteration for the
policy re-check. io_uring gates that read behind `IDLE_HOUSEKEEPING_INTERVAL`.
The DPDK heartbeat timer also omits the `past_spin_budget()` condition that
`response.rs` explains is needed to keep the heartbeat scan from stretching to
minutes on a nearly saturated stage.

## Replication

### DPDK sender `Handshaking` state has no deadline

`replication/dpdk.rs`. `AUTH_TIMEOUT` covers `Authenticating` only. An
authenticated replica that then stays silent holds a slot until it
disconnects. TCP's 10 s read timeout covers it.

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

## Deliberate differences (no action)

- DPDK response has no flush cadence or pre-gate flush: it flushes per slot.
- DPDK response has no shutdown flush to suppress when fenced: exiting stops
  releasing frames, which is the whole ack gate.
- DPDK heartbeats skip the buffer-cap / blocked-peer checks: there is no
  per-connection send buffer on that side.
