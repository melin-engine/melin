# Lost signals at phase boundaries (2026-10)

Found by a read-only audit after the fix for a DPDK primary that dropped a
joining replica's acks during its catch-up (`dpdk_veth`'s
`an_ack_sent_during_the_catch_up_counts_once_the_joiner_is_live`). That bug
had a particular shape: a signal sent once and never repeated (there, the
replica's last ack of its join) was lost at a phase boundary, on the
assumption that a later event would cover it. On a quiet primary, with ticks
off, no later event comes, and a write held for the signal is never answered.

The audit looked for the same shape on the primary's replication sender, the
replica's receiver, client ingress and the reply path, on both transports, and
noted the other divergences it met on the way. Nothing below is in
`transport-divergences-2026-10.md` or `halt-degraded-acks-2026-10.md`. Nothing
here has a test yet, and nothing has been reproduced: every entry comes from
reading the code.

Each entry says which transports it affects, how sure we are, and a direction
for the fix. **Verified** means the code path was traced end to end by a second
reader; **reported** means only the audit traced it.

Paths are relative to `crates/core/server-runtime/src/`, except those
starting `transport-core/`, which are under `crates/core/`.

## Same shape as the fixed bug

### An entry journaled during a handoff can be missed (both transports)

Verified. `drain_into_contiguity` (`transport-core/src/replication/catchup.rs`)
on kernel TCP and `LiveHandoff::step`'s drain phase
(`transport-core/src/replication/handoff.rs`) on DPDK go live as soon as the
replica's ring is empty. The journal stage publishes a batch to the replication
rings in `submit_batch` (`transport-core/src/pipeline.rs`) before the disk
thread writes it. An entry whose publish saw the replica's ring still inactive,
and whose write lands only after the handoff's last journal pass read the
journal, is in neither the ring nor what was streamed.

The comment in `drain_into_contiguity` names this corner and leaves it to the
replica's contiguity check. That check fires only when a later chunk arrives,
past the gap. Heartbeats carry the primary's sent high-water, which excludes
the entry, and the replica ignores a heartbeat's sequence anyway.

Scenario: one replica under `disk+ram`, ticks off, and one client write as the
replica's handoff begins (more likely while the disk thread is in an earlier
batch's fsync). The write is held until something else is journaled.

Direction: heartbeats advertise the primary's durable journal tip, and a
replica that sees a tip past what it holds treats it as a contiguity break and
reconnects. Alternatively, the sender runs a journal pass when its ring is
empty and the journal tip is past its sent high-water.

### A refused persisted ack is lost on a DPDK replica

Verified. `try_flush_dual_track` (`transport-core/src/replication/ack_queue.rs`)
pops the durable entries off the pending-ack queue before the ack is sent. The
DPDK receiver's `send_ack` (`replication/dpdk.rs`) gives up after
`ACK_RETRY_CAP` refused `queue_send` calls and returns `Ok(false)`, which
`streaming_loop` (`replication/receiver_transport.rs`) treats as "try next
iteration". The popped value is gone: the next call falls back to the last
value sent, so a retry carries a stale `acked_sequence`, or no ack is built at
all. The tracker contract on `try_flush_dual_track` names this case and
assumes a later pop covers it, which on a quiet primary never happens.

Kernel TCP does not lose it: its `send_ack` never refuses, keeping the newest
ack while a send is in flight and sending it when that completes.

Scenario: the replica's ack TX fills (the primary's poll thread stalls, or the
replica-to-primary direction loses segments), the replica fsyncs the last batch
in that window, and the primary then goes quiet. A write held for that
replica's persisted ack (`disk` or `disk+ram`) is never answered. The only
signal is the "DPDK ack send failed" warning.

The mock transport refuses only while an ack is in flight, which the loop never
calls it with, so the refusal path is untested.

Direction: in `streaming_loop`, keep the highest value ever popped and use it
as the fallback in place of the last value sent, so a refused ack is rebuilt
next iteration on any transport. A test: a mock that refuses one ack, then one
batch, the journal cursor advanced, and no more data; a later ack must carry
the persisted sequence.

### Pipelined writes after a full input ring are never parsed (io_uring)

Verified with the next entry. `process_client_frames` (`client_frames.rs`)
stops at the frame that met a full input ring (or a full refusal queue): the
frames before it are committed, it is answered with ServerBusy, and the frames
after it stay in the connection's parse buffer. That buffer is parsed again
only when more bytes arrive (`reader.rs`). A client that pipelined N writes and
waits for their replies sends nothing more, so those writes end in its
no-reply deadline. A unit test in `reader.rs` pins the leftover tail as
intended; nothing covers what happens to it.

DPDK closes the connection instead (listed in `transport-divergences-2026-10.md`
as "DPDK `PipelineFull` drops the client instead of sending ServerBusy").

Direction: reparse the leftover once the ring has room, or decide that a
ServerBusy ends the connection's pipeline and say so in the protocol.

### The io_uring idle timeout is checked only when the reader wakes

Reported; the scan's placement is verified. The `connection_timeout` scan in
`reader.rs` runs only after `submit_and_wait` returns, and the only timeout the
reader submits is the tick. Server heartbeats do not count as activity, and the
accept loop's `max_connections` rejection does not wake the reader. DPDK's poll
loop busy-spins and runs the check on its slow-check interval.

Scenario: ticks off, `max_connections` taken by idle or vanished clients, no
traffic. Those connections are never reaped, and every new client is refused
for good. On DPDK they go after the timeout.

Direction: submit a timeout SQE for the next scan whenever `connection_timeout`
is set, independent of ticks.

## Other divergences found

### A runtime ack-policy change reaches replicas only after a quiet spell (both)

Verified. Only StreamStart and Heartbeat carry the policy, and a heartbeat goes
out only after the heartbeat interval without data: kernel TCP restarts its
heartbeat timer on every data send (`replication/tcp_sender.rs`), and DPDK
sends one only on a tick that sent nothing (`replication/dpdk.rs`). The replica
stores the policy only from those two frames, and auto-promotion refuses only
under `disk` (`raft_promotion.rs`).

Scenario: a primary on `disk+ram` is swapped to `disk` under steady traffic,
and dies before a quiet spell. Its replicas still believe `disk+ram`, a lagging
one auto-promotes, and writes acknowledged under `disk` that it lacks drop out
of the new history.

Direction: send a heartbeat as soon as the policy differs from what the slot
last advertised, under load or not.

### ServerBusy overtakes earlier replies (io_uring)

Verified. On `PipelineFull` the reader sends `ControlEvent::PipelineBusy`, and
the response stage (`response.rs`) appends ServerBusy to the connection's send
buffer while draining control events, before it handles any output slot. The
replies to the connection's earlier writes, still in the ring behind the
backlog, follow it. Refusals have an ordering mechanism (`RefusalQueue`);
ServerBusy has none. The same happens during a halt once the refusal queue is
full.

`melin-client`'s polled connection settles the oldest in-flight request on a
ServerBusy, and keeps the connection. It therefore takes ServerBusy as the
answer to a write that was applied: a retry applies it twice, and every later
reply is matched one request off.

Direction: order ServerBusy behind the connection's pending replies, as
refusals are, or end the connection on it as DPDK does.

### A leftover eviction flag evicts the next replica on a DPDK slot

Reported. When kernel TCP collects a finished replica handler it skips the
ring to the producer and clears the slot's eviction flag, however the handler
exited (`replication/tcp_sender.rs`). DPDK's `go_idle` (`replication/dpdk.rs`)
does neither, and the eviction check skips idle slots. The flag is left set
when the journal stage raises it after the top-of-tick check and the same tick
then goes idle for another reason (TX overflow, disconnect, join stall), or
when the journal stage read the slot as active just before `go_idle` and then
found the ring full.

The next replica accepted on that slot is evicted on its first tick, while
still authenticating: `evictions_total` counts it, the log says "evicting slow
replica", and the replica pays a reconnect backoff. If that accept comes on the
very next poll, the idle drain never runs either, and a ring left full evicts
the new joiner in its handoff.

Direction: in `go_idle`, after marking the slot inactive, skip its ring to the
producer and clear the flag, as kernel TCP does.

### Client sockets are left open when a DPDK node shuts down or is fenced

Verified. On `shutdown`, `run_dpdk_poll` (`dpdk_transport.rs`) closes the
replication links (`close_all_links`) and leaves the client sockets as they
are. The io_uring reader's teardown `shutdown(2)`s every client socket, so the
peer sees EOF. A client waiting on a held write sends nothing, so the halt's
"closed if it sends anything" rule never reaches it either. On a fence, io_uring
clients reconnect to the new primary at once, and DPDK clients only after their
own read timeout.

This is a different path from the listed "DPDK closes a client connection
without telling the peer": here nothing is closed at all.

Direction: close every client socket on shutdown, as the replication links are.

### A partly sent reply waits out another connection's gate (io_uring)

Reported, plausible. The io_uring response stage flushes once before it waits
in the durability gate, and a send cut short by `EAGAIN` keeps its remainder
for a paced retry. `wait_durable` sends nothing, so that remainder waits for
the whole gate wait on another connection's reply: a halt under `ram`,
`disk+ram` or `two-disks`, or a lagging replica. DPDK's poll thread transmits
independently of the stage.

Direction: retry pending sends from the gate's idle path.

## Minor

- **Join acks under-counted on DPDK.** `keep_last_ack` hands the streaming arm
  only the newest ack of a join, so `melin_replica_acks_received_total`
  ("cumulative valid ack frames") and ratios derived from it read lower than
  on kernel TCP, which reads the whole backlog. Not a correctness issue.
- **The persist-stall warning needs an ack to fire** (both). `PersistWatch`
  is observed only on ack receipt, so a replica whose journal wedges on a quiet
  primary acks its in-memory position once and the warning never comes, which
  under `two-disks` leaves a held write unexplained.
- **The DPDK primary has no bring-up wait on `replica_ready`.** Explained in
  `server.rs`, but not listed as a deliberate difference.
- **Stale comment** on the DPDK slot's `sent` field: it says it is re-seeded on
  every handshake; it is set when the replica goes live.
- **The DPDK replica's handshake wait ignores promotion.** An extension of the
  listed "DPDK ignores `queue_send` failures for the handshake". The replica's
  handshake wait and `DpdkFrameSource::next_frame` check `shutdown` but not
  `promote`, and the link stays up meanwhile, which vetoes raft
  auto-promotion. With a lost Handshake the replica hangs and ignores even an
  operator's promote. Kernel TCP leaves the same wait through its read timeout.

## Checked and consistent

No lost wakeup in the reply release: both response stages spin in
`DurabilityGate::wait_durable`, reloading the cursors, the policy and
`shutdown` each iteration, so nothing depends on an edge trigger. Refusals on a
quiet halted node are released without a later event. Receive buffers carry a
trailing partial frame through every replication phase on both sides. A
replica's persisted ack goes out once fsync completes, with nothing new
arriving, on both transports (except as above). The halt is raised and lifted
identically.
