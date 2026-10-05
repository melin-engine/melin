# DPDK replication: design notes

How the DPDK replication path does what the kernel-TCP one gets from the
kernel, and why it is built the way it is. Each section was written with
the fix it describes, found by running the integration suites on DPDK
(`dpdk-testing.md`).

## Peer liveness on replication links

The problem: a kernel-TCP node's peer learns that it stopped from the
node's kernel, which closes its sockets (EOF, or a reset) even when the
process crashed. A DPDK node is its own TCP stack, so once it stops
nothing speaks for it, and neither end of a DPDK replication link had a
deadline on a silent peer. A primary kept counting a stopped replica and
never halted; a replica kept a stopped primary's link up, so it never
failed over. The kernel-TCP path has no application-level deadline while
streaming either: its sender and receiver both end the session on EOF or
a reset (its receiver's 5 s read timeout covers only the auth and
handshake reads).

What was built, in the replication layer and the transport calls it uses
(`melin_dpdk::peer_liveness`, `replication/dpdk.rs`):

- **A deadline on a silent peer, in the TCP stack.** Every replication
  socket, at both ends, is armed once established with smoltcp's
  keep-alive (a probe a second on an idle link) and its timeout (the
  connection is reset once the peer has sent nothing for 5 s, the
  kernel-TCP receiver's quiet-primary figure). A peer whose stack answers
  is alive however quiet its application is; one that answers nothing is
  gone, and every link check then ends the session: the sender's slot
  goes `Idle` (the halt gate lowered, the replica uncounted), the receiver
  drops its primary link (auto-promotion may proceed). A link sending
  into a dead peer can take up to twice the timeout (smoltcp restarts the
  count when data is queued on an idle socket), never more.
- **A peer that has stopped reading is alive.** Its receive buffer fills
  and it advertises a zero window. With data queued for it, the node
  sends zero-window probes instead of keep-alives, and the peer's stack
  answers each one. fastcp before 0.13.2 doubled the probe delay without
  bound, so once it passed the timeout the deadline fired between two
  answers and reset a live peer about 12 s in: a replica installing a
  large snapshot could never join. 0.13.2 caps the probe delay at the
  keep-alive interval, and the workspace requires it.
- **Telling the peer.** Every replication close goes through
  `DpdkTransport::reset`, which sends the socket's RST before removing
  it: a slot dropped by the primary, a session the replica ends, and both
  ends' links when a node stops (the primary's poll loop resets its
  replicas' links as it exits; every receiver session exit, a stop or a
  promotion included, resets the link). Before the reset each waits up to
  100 ms for what it has queued to be acknowledged (the replica's final
  ack, the primary's last stream frames), the part of a kernel's flush
  before its FIN that matters here. An orderly stop is seen at once, as
  on kernel TCP; the deadline covers a crash, a cut link, and a lost RST.
- **Answering while busy.** The stack answers only when its thread polls
  it, and a replica's receiver thread also waits on local work: a push
  into a full input ring behind a slow journal, and the resync's teardown,
  snapshot load and seed install. Unanswered, the primary's deadline would
  drop a replica that is only slow, and halt if it was the last, where a
  kernel-TCP replica stays connected behind a zero window. So the ring
  wait polls the stack on every pass
  (`ReceiverTransport::keep_link_serviced`), and the resync steps run on a
  helper thread while the receiver polls (`ControlFrameSource::serviced`,
  `run_serviced`). Neither reads, so the window closes as the socket
  buffer fills, as a kernel's would. Kernel TCP runs both inline. A
  replica whose disk stalls long enough still fills the primary's
  replication ring and is evicted, on either transport.
- **Seeing the peer's FIN.** The link checks on both ends ask whether both
  halves are open (`is_connected`), not whether the socket is active, so
  a peer's FIN (a kernel-TCP primary's close, for a DPDK replica) ends the
  session once the bytes before it are read, as EOF does on kernel TCP.
- **A monotonic stack clock.** The transport fed smoltcp the wall clock;
  a step forward would reset every replication link at once, and a step
  back would hold a dead peer for as long as the step. The stack's clock
  is the wall clock's reading at start plus a monotonic count.
- **Promotion during a dial.** The receiver's connect loop also stops on a
  promotion request: a dial to a stopped DPDK primary is answered by
  nothing, and waiting out the connect timeout delayed the failover by as
  much.

Not serviced on the replica: once the stream starts, opening the
continuing journal writer and building the replica pipeline (ring
allocation, memory locking, thread spawn) run inline on the receiver
thread while the primary streams, so only a build that stalled past the
deadline would cost the link. That build is ordinarily far quicker than
the deadline, so it is left as is.

Nothing on the hot path: the probes and the timeout are timers the stack
already runs on its egress passes, the link check replaces the one each
tick already made, and the clock is read where it was.

Alternatives considered:

- **An application-level deadline on silence.** The primary heartbeats an
  idle link, so a replica could time its primary out on the heartbeat
  interval. The replica does not speak when idle (it acks data only), so
  the primary could not do the same without a new replica heartbeat in
  the wire protocol, and the replica's deadline would hang on an operator
  setting (`--replication-heartbeat-secs`) that was never meant as one.
  The TCP-level deadline works at both ends, needs no protocol change,
  and keeps the kernel-TCP path untouched.
- **Announcing the close only (FIN or RST on stop).** Covers an orderly
  stop, not a crash, which is the case failover exists for.
- **A graceful FIN instead of the RST.** Delivers what is queued and is
  what kernel TCP mostly sends, but the socket must stay in the set until
  the FIN is acknowledged, where every close here removes it at once. The
  RST is one segment; the final ack a replica owes on its way out is
  waited for (bounded) before it.
- **Changing `DpdkTransport::close` itself.** It is shared with client
  connections, whose silent close is a separate divergence ("DPDK closes a
  client connection without telling the peer"), left as it is: the new
  call is used by replication alone.

Tests. The decision is unit-tested without libdpdk, on two smoltcp stacks
over an in-memory wire and a virtual clock (`peer_liveness` tests): a live
idle peer kept, a live peer that has stopped reading kept for six
timeouts, a stopped one declared gone within the bound idle and while
sending, a cut link seen at both ends, the announced reset seen at once
where a plain removal is not, and a peer's FIN. The busy-replica servicing
is unit-tested as well: a full-ring push that completes only if the wait
services the link, and `run_serviced` servicing until its work is done.
End to end, `halt_refusal` covers an orderly stop on DPDK, and
`dpdk_veth`'s `a_replica_cut_off_is_dropped_and_its_primary_halts` the
deadline: the replica's link is taken down under it
(`melin_test_node::Node::cut_off`), and its primary must drop it within
the bound and refuse writes. Only the primary's end is checked there; the
replica's has no gauge to read. The failover tests cover it:
`replicated_failover` cuts its primary off before stopping it, so that no
RST arrives, and auto-promotion refuses while a replica's primary link is
up, so the failover completes only once a replica has dropped the link on
its own deadline.

## A replica's join off the primary's poll thread

The deadline made a stall on the DPDK primary's poll thread cost links:
the thread is the sender, the client loop and the only thing running the
stack, and a replica's join did its file I/O on it (the catch-up probe,
the snapshot pre-flight, the snapshot's and the segment seed's whole-file
reads, and every journal read of the catch-up). A stall past the 5 s
deadline, a large snapshot on a cold or slow disk say, made every other
DPDK replica reset its link to a live primary, and the primary halt if
that left it none. Short of that, it froze client traffic and the other
replica's stream for as long as the disk took. Kernel TCP has a thread
per replica and a kernel answering for the process. So the two land
together: the deadline alone would ship this stall.

What was built (`replication/join_worker.rs`, the `Joining` slot state in
`replication/dpdk.rs`): a parked join worker per slot, spawned with the
driver (as the validation worker is, for the same reason: a thread
created from the pinned, real-time poll thread never runs), does the
join's disk side, the same steps in the same order, with the pre-flight
still ahead of the resync verdict. It encodes its frames into a bounded
channel (a few frames deep, buffers recycled). Once the handshake is
validated the slot goes `Joining`, and each tick moves what the worker
has ready into its socket, as TX space allows and at most a tick's byte
budget, then returns: the poll thread goes on serving clients, the other
slot's stream and every link's probes and ACKs while the disk takes what
it takes. A frame the socket refuses is held and sent first next tick;
one larger than the socket's queue can ever take fails the join rather
than wedging it. When the worker reports the end, every frame is already
queued in order, and the slot runs the bridge into the live ring as
before. A slot that drops mid-join cancels it (the worker stops at its
next frame); the slot drops its stream before its worker, so a worker
blocked on a full channel is released before it is joined.

Alternatives considered:

- **`run_serviced` around the inline steps**, as the receiver's resync
  does. It keeps the stack answered, but holds up client traffic and the
  other replica's stream for the whole step (that replica is then evicted
  when its ring fills, and clients see the stall), and it spawns its
  helper from the calling thread, which on the primary is the pinned,
  real-time poll thread whose children never run.
- **Reading the snapshot in chunks between polls.** Bounds the join's
  memory to a chunk, but a single read on a cold or slow disk still
  blocks the poll thread, and the probe, the pre-flight, the seed and the
  catch-up reads stay inline. The worker covers every step at once.

Open follow-up: the snapshot's and the segment seed's whole-file reads
remain, now on the worker. Reading them in chunks there is still worth
doing on its merits: it would bound the sender's peak memory to a chunk
instead of a whole body, on both transports (the transfer code is
shared), and shorten the one wait on the disk a stop still has (a slot's
worker joined mid-read waits out that read). It is a change to the shared
transfer code (`snapshot_transfer_with` and the seed prefix), not to the
poll thread, and is on the roadmap.

What else the poll thread does that could block for long:

- The chain validation: on its worker already.
- The bridge into the live ring (`bridge_catchup_to_live`) runs inline,
  because it owns the slot's ring consumer and active flag. It re-reads
  only what was journaled since the worker reached the end of the
  journal, polls the stack after every frame, and its wait on the disk is
  bounded (30 ms). The links stay answered throughout, but the client
  loop sharing the thread is held while the bridge runs, and the bridge
  waits for room in the joiner's send queue. A joiner that dies
  mid-bridge holds client ingress until its liveness deadline resets the
  link, up to about twice the liveness timeout since data is being sent
  into it. A joiner that is alive but stops reading keeps acknowledging
  with a zero window, so its deadline never fires: ingress is held until
  it drains, with no bound but the joiner. This predates the deadline,
  and is DPDK only (the kernel-TCP sender bridges on the replica's own
  thread, off the client path).
- Streaming, acks, heartbeats, auth (one signature check), accepting (one
  nonce), dropping a link: no I/O, no waits.
- Closing every link on stop waits at most 100 ms, once, on the way out.
- The client loop sharing the thread: every hand-off (the input ring, the
  refusal queue, the tick, the response queue, the control channel) is a
  non-blocking push or an unbounded send; nothing reads a file.
- Logging: a log line on this thread is written by the application's
  subscriber, which the embedding binary chooses; lines here are
  lifecycle and per-connection events, not per-message.

The hot path is unchanged: the streaming arm is untouched, and a slot
pays for the join's channel only while it is `Joining`.

Tests. The join worker's mechanics are unit-tested without libdpdk
(`join_worker` tests: frames in order, a pump that never waits on a
stalled job, a refused frame resent first, the tick budget, an abandoned
join freeing the worker, a slot dropped mid-join not hanging, the join's
steps against a real journal). End to end, `dpdk_veth`'s
`a_stalled_join_costs_the_other_replica_nothing` stalls a divergent
replica's join on the primary's snapshot (a FIFO the test holds open and
never writes into) for twice the liveness timeout and more, while a
request is answered promptly every few hundred milliseconds and both
replicas stay counted; with the join's wait put back on the poll thread
it fails on the first request. `large_joins_complete_a_tick_at_a_time`
brings up a fresh replica (journal catch-up) and a divergent one
(snapshot, a seed of several MiB, catch-up), each many times a socket's
queue, and checks both ack the primary's whole history.

## A promoted replica on DPDK

The problem: a promoted DPDK replica ran the kernel-TCP primary, which
binds kernel listeners on the client address and `--replication-bind`.
Only the DPDK port holds those, so the bind failed and the node exited
instead of serving; where the kernel did hold the address, the node
served on kernel TCP, giving up kernel bypass without a word. A promoted
kernel-TCP replica serves on the transport it ran on.

What was built (`server.rs`):

- **One DPDK primary function for both entries.** The part of
  `run_dpdk_impl` that serves as a primary is `run_as_primary_dpdk`, the
  twin of the kernel-TCP `run_as_primary`, with the same parameters bar
  the bring-up gate's: the pipeline, the response stage and its ack gate,
  the halt gate, the replication driver (its liveness deadline and join
  worker included), shadow snapshots, health, ticks and the poll loop.
  The normal startup calls it after `init_engine` and the raft driver,
  with no promotion; the promotion arm calls it with the receiver's
  application and journal writer and the promotion request's epoch
  floor. The epoch bump, which was inline in `run_as_primary`, is a
  function both call (`journal_promotion_epoch_bump`), so the two
  transports mint and order it the same way: after the pipeline is up,
  before `on_primary`, before the first client or replica is served.
- **The replica's transport, reused.** The receiver borrows the queue-0
  transport rather than consuming it, and resets every link it opened
  before it returns. The replica builds that transport with no listener
  (`DpdkTransport::from_shared_unlistening`), where it used to listen on a
  stray port nobody read; at promotion it gains the client listener
  `from_shared` gives a booting primary, and `run_as_primary_dpdk` adds
  the replication listener as it does at boot. EAL, ports and pool are
  never touched again (EAL cannot be initialised twice), and the
  receiver's stack, with its neighbour cache, carries on as the poll
  thread's. A gratuitous ARP goes out again, as at boot.
- **Everything else as on kernel TCP.** The promotion arm runs the same
  steps in the same order as the kernel-TCP one: the genesis check
  (`check_promotable`), the replica health endpoint released for the
  primary's, sizing, the latched `ROTATE` cleared; the fence state, the
  advertised tip, the raft driver and the admin endpoint (with its
  `PROMOTE` flag) carry over. The receiver's pipeline is torn down before
  the primary's is built, by the shared `take_pipeline_for_promotion`, so
  the history continues in the same journal from the writer's next
  sequence: nothing lost, repeated or reordered. One DPDK-only step: the
  main thread ran the receiver, pinned to the reader's core once it
  streamed, and is unpinned before it spawns the primary's threads (a
  child of a pinned real-time thread never runs to pin itself), then
  pinned again as the poll thread.
- **Clients before the promotion.** A replica serves no client on either
  transport. On kernel TCP its listener is bound from boot, so a connect
  completes in the kernel's backlog and is served if the node is promoted
  while it waits; on DPDK there is no listener until the promotion, so a
  connect is refused. Clients retry either way.

The normal DPDK primary startup behaves as before, with two changes of
order, neither visible to a client or replica: the admin endpoint is
spawned before the pipeline is built rather than just after (the order
the kernel-TCP primary has always used; a failed admin bind now refuses
the boot before any pipeline thread exists), and the one-queue check
returns an error before anything is spawned instead of asserting after.
The `on_primary` drain, the poll loop and the hot path are untouched;
promotion adds nothing to them.

Alternatives considered:

- **A second, promotion-only DPDK primary function.** Simplest to write,
  but two copies of the primary's assembly drift apart, and a promoted
  node would be a different primary from a booted one.
- **A fresh transport at promotion** (`from_shared` on queue 0 after
  dropping the replica's). It would also reuse EAL, ports and pool, but it
  rebuilds the stack under a queue another stack had been polling, and
  throws away its neighbour cache and clock for nothing; reusing the one
  transport keeps a single owner of the queue throughout.
- **Listening on the client port from boot**, as the kernel-TCP replica
  holds its listener. On DPDK nothing else can take the port, so there is
  nothing to hold, and established connections nobody accepts would pile
  up in the replica's socket set without the bound a kernel backlog has.

Tests. `genesis_promotion`'s
`a_replica_configured_with_a_larger_genesis_is_promoted` and the notary
example's `a_promoted_replica_reports_the_head_the_primary_receipted`
(operator `PROMOTE`, then clients served by the promoted DPDK node, its
state and chain continuing the primary's), `raft_failover` (an
auto-promotion after an orderly stop, the loser still following, the
revived ex-primary fenced), and `replicated_failover` (an auto-promotion
after the primary is cut off, every event acknowledged under `ram`
present on the new primary). `dpdk_veth`'s
`a_replica_refuses_clients_until_promoted` pins the refused connect.
