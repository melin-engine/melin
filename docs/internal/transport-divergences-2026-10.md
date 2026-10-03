# io_uring vs DPDK: behavioural divergences (2026-10)

Found while deduplicating the kernel-TCP (io_uring) and DPDK paths. The
refactor itself is behaviour-preserving; everything below was left as it is
and is listed here for a decision. Each entry says which side looks wrong and
why. Some differences are deliberate, and those are marked.

Paths are relative to `crates/core/server-runtime/src/`.

The DPDK side of an entry can be exercised without hardware on the veth
harness (`dpdk-veth-testing.md`), and by the kernel-TCP integration tests
run on DPDK (`dpdk-transparent-tests.md`), where a test that fails on an
entry is compiled out under the `dpdk` feature, naming it, until the
entry is fixed. An entry says which tests cover it or are gated on it;
the others have no DPDK test yet.

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

### DPDK does not see a client's close (likely bug)

`dpdk_transport.rs`, the read path. A connection is released when a
zero-byte read finds the socket no longer active. A client's FIN moves the
smoltcp socket to CloseWait, which still counts as active, so the node keeps
the connection, its slot and its socket. They are released only when the
next heartbeat is answered with an RST, at the idle timeout with heartbeats
off, or never with both off. Until then a node at `max_connections` turns new clients
away. io_uring releases the connection on EOF. The fix is to treat a
zero-byte read on a socket that can no longer receive as a close.

Pinned by `a_client_close_is_seen_only_at_the_next_heartbeat` in the DPDK
veth tests: no slot within a few seconds of the close, one after the
heartbeat. The fix flips that test to require the slot back promptly.

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

### DPDK does not notice a replication peer that has gone (likely bug)

`replication/dpdk.rs`, both ends of the link. A DPDK node that stops
tells its replication peer nothing (no FIN, no RST: the close described
in "DPDK closes a client connection without telling the peer"), and
neither end of a DPDK link has a deadline on a silent peer. The sender's
streaming slot is released only once its socket is no longer active, and
a socket retransmitting heartbeats to a peer that never answers stays
active, since no timeout is set on it. The receiver waits on its primary
the same way. io_uring sees the EOF, and its receiver gives a quiet
primary 5 s.

The effect, found by the cluster tests on DPDK
(`dpdk-transparent-tests.md`, step 2):

- A primary whose replica has stopped keeps counting it
  (`melin_replicas_connected`) and never halts, so it does not refuse
  writes as a primary whose last replica has left must. Under a policy
  that needs a replica, acknowledgements stall instead.
- A replica whose primary has stopped keeps its link "up": auto-promotion
  refuses to depose a live primary, and the cluster never fails over.

Tests gated on it: `halt_refusal`, `replicated_failover` and
`raft_failover` (`crates/core/server-runtime/tests/`).

### A promoted DPDK replica serves on kernel TCP

`server.rs`, the promotion arm of the DPDK replica path, marked TODO
there. A promoted DPDK replica runs the kernel-TCP primary: it binds a
kernel listener on its client address (and on `--replication-bind`).
That address is normally the DPDK port's, which no kernel interface
holds, so the bind fails and the node exits rather than serving. Where
the kernel does hold the address, the node serves, on kernel TCP: a
failover silently gives up kernel bypass. A promoted io_uring replica
serves on the transport it ran on. The fix is a DPDK primary path for a
promoted replica.

Found by the cluster tests on DPDK (`dpdk-transparent-tests.md`, step
2). Tests gated on it: `genesis_promotion`'s
`a_replica_configured_with_a_larger_genesis_is_promoted`, the notary
example's `a_promoted_replica_reports_the_head_the_primary_receipted`,
and, behind the entry above, `replicated_failover` and `raft_failover`.

## Deliberate differences (no action)

- DPDK response has no flush cadence or pre-gate flush: it flushes per slot.
- DPDK response has no shutdown flush to suppress when fenced: exiting stops
  releasing frames, which is the whole ack gate.
- DPDK heartbeats skip the buffer-cap / blocked-peer checks: there is no
  per-connection send buffer on that side.
