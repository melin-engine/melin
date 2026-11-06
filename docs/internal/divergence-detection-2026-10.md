# Divergence detection (plan)

Status: **proposal**. Implements item S8 of the
[application API review](application-api-review-2026-09.md), "No divergence
detection for a nondeterministic `apply`".

Code is cited as `file:line` against main at 5301f789, the commit this
branch is based on (the
[cluster-halt evaluation](divergence-cluster-halt-evaluation-2026-10.md)
cites an older tree); a later edit to a cited file moves its lines, so the
function or type named beside each citation is the anchor to search for.

The one-line goal: **every copy of the application digests the reports it
produced, in windows of a fixed number of entries, chains the window digests,
and when two copies disagree the whole cluster stops serving, for good: the
way back is a new cluster.** The hash chain proves that every node
holds the same *input*. Nothing proves that the same input produced the same
*output*. A nondeterministic `apply` (a randomly seeded `HashMap` iterated,
the wall clock read, an allocation address hashed), a `snapshot`/`restore`
pair that drops a field, or a memory error leaves every journal bitwise
identical while the copies answer differently, silently, until a failover
serves the drifted copy.

What is digested is the **output**: the bytes of every report the
application's encoder produces for a journaled event. It is computed once,
in the runtime, for every application. There is no trait to implement and
no opt-in, and it is on unless the operator turns it off. Copies are
compared whatever build each runs (open question 2). A divergence means
either the application broke its determinism contract, or two builds the
cluster runs side by side answer the same input differently; for the
cluster the two are one fault, since a failover would show clients another
history. Which copy produced the odd output does not matter, and nothing
is repaired. The node that sees it stops and tells the rest of the cluster,
which stops too (decision 11), and every stopped node refuses to start
again. Operations resume only on a new cluster, started from the journal
with a fixed application or with output digests off (step 6). The tick
rule, once a trap the runtime set for the cross-node check, no longer is:
finding 7 says why.

The journal stays input-only and the snapshot format is untouched. Nothing
new is journaled and nothing is stored: each copy's chain starts when its
pipeline starts. The digest rides the replication stream as a control frame,
as the chain check does, and the stop travels the same stream: a flag on the
replica's acks and a halt frame from the primary. Raft carries nothing new.

## The guarantee, and its limits

Stated here as the operator docs will state it (step 6):

- **Once two copies' outputs are seen to disagree, the cluster stops
  serving.** A replica whose reports differ from its primary's, or a node
  whose snapshot-making copy (the shadow) answers differently from its live
  copy, stops at once: it sends no further reply, takes no further client
  and cannot be promoted. It then tells the nodes it can reach, directly or
  through the primary, and each of them stops the same way. Every stopped
  node refuses to restart. With at most three raft voters, two stopped
  nodes leave no quorum, so no election can promote the third (decision
  14). The only way back is a new cluster (step 6).
- **Every copy still running produced the same client-visible reports, byte
  for byte, as the copy clients were answered from, for every position of
  its verified run** `[from, through]` (`/healthz`, `/metrics`). A run is
  contiguous by construction: no window inside it went uncompared.
- **Copies on different builds are compared like any others.** Another
  release of the application, another architecture (a floating-point
  transcendental function can round differently on x86 and on ARM), or a
  binary built per host: if the output differs for the same input, the
  copies have diverged, and the cluster stops. The marker, the `error!`
  line and the startup refusal name both builds when they differ, so a
  mixed-build cluster is plain as the cause (open question 2).
- **What this means for upgrades.** An application upgrade that changes
  its output for the same input cannot run mixed with the old build in a
  live cluster with output digests on: the first window whose reports
  differ stops the cluster, and rightly, since a failover across the two
  builds would show clients another history. Two paths are supported:
  - **Stop everything, upgrade, start together.** Every node then replays
    the same journal on one build.
  - **A journaled activation point.** The new build keeps the old
    behaviour until an entry every node applies at the same position (an
    operator command the application journals, say) switches it, so old
    and new builds produce identical output until every node runs the
    new one, and the switch happens at one position everywhere. A
    pattern for the application, described in
    `docs/building-an-application.md` (step 6).

  An upgrade that keeps output identical for the same input (a fix
  elsewhere, a faster path) can still roll node by node. A Melin runtime
  upgrade is already a stop-everything flag day for replication protocol
  9 (decision 8); protocol 9, on `main` already, moved the version check
  into authentication, where a primary accepts a newer replica, so that a
  later release can roll without one (raft protocol 1 needs the same,
  not yet scheduled).
- **The limits.**
  - **A detector nobody can reach stops alone.** The stop travels on the
    replication links. A stopped node keeps its replication endpoint up for
    a bounded linger (decision 11) to reach nodes that were between
    sessions. A detector that stays cut off from both others past the
    linger, while they still reach each other, stops alone, and they may
    fail over and serve. That needs a partition isolating exactly the
    detecting node, starting around the moment of detection and lasting
    longer than the failover grace and the linger. The others' own checks
    are a likely backstop: nondeterminism tends to recur on every copy. It
    is not a guarantee.
  - **A manual promotion overrides the stop.** `PROMOTE` of a node that
    never heard the halt is the operator's act, and the docs list every
    path around the stop in one place (step 6).
  - **Divergence that has not yet shown in any report is not caught.** Two
    copies can hold different state and still answer every request
    identically, for example a balance that differs only on an account
    nobody has touched since. That latent divergence surfaces the first time
    a report depends on it, which may be after a failover has promoted the
    drifted copy. Closing it needs a digest of application state, a roadmap
    item (step 7).
  - **Windows a replica applies before its first comparison with a new
    primary are not verified against the primary.** A primary restart or a
    promotion starts a new output chain, and so does a replica's own
    restart or a resync of its state: each starts a new run, and the
    windows between the end of the last run and the start of the next are
    counted as unverified, never claimed. A reconnect to a primary that has
    not restarted (a network blip, a damaged frame) keeps the run: the first
    comparison after it verifies every window applied since the last one,
    the catch-up's included, unless the replica's own record of its windows
    was overrun meanwhile, which ends the run and is counted as a chain
    break. The node's own in-process check (shadow against live) has no
    such hole: it covers every window from its pipeline's start. Verifying
    the windows around a primary restart or a promotion against the primary
    too is a roadmap item (step 7).
  - **The detection delay is bounded in entries, not in seconds.** A
    divergence is seen within one window of `K` entries of the report that
    shows it, plus the one fsync batch that carries the primary's digest
    (and, under a replica-backed ack policy, the replica's ack before the
    primary folds); step 6 measures it. An idle primary journals
    one tick per tick interval, so idle it is at most about `K` tick
    intervals. With ticks disabled (`--tick-interval-ms 0`), an idle node's
    last window stays open until traffic fills it.
  - **The tail at failover.** Positions after a run's end (the windows not
    yet compared when a primary stops) are not verified. Nor are the
    windows a promoted replica's own in-process check had not reached when
    the promotion took its copies down: the promoted node's check starts
    afresh from its live state.
  - **Queries are not covered**: they are not journaled, so no other copy
    answers them.
  - **Snapshot handling is checked in-process only narrowly.** The shadow
    is made by one snapshot-and-restore of the live copy at the
    pipeline's start, so the in-process check catches a `snapshot` or
    `restore` that loses state only for state present at that moment: a
    field first filled later is never round-tripped in-process, since the
    shadow writes later snapshots but never reads them back. An
    application that overrides `clone_via_snapshot` with a direct clone
    gets no round trip at all. Either way, a lossy snapshot shows once a
    node restores one (a restart, a resync, a new replica), and then only
    the check across nodes sees it. The way back sets every snapshot aside
    (step 6), so a lossy one is not carried into the new cluster.

## What the code shows

### The chain check this plan mirrors

- **Emission.** The journal stage is the sole producer of the two
  replication rings (`SequencerCore::publish_to_replication_rings`,
  `transport-core/src/pipeline.rs`). Right after a batch is handed to the
  disk thread, `maybe_publish_chain_check` counts published batches and,
  every `CHAIN_CHECK_INTERVAL_BATCHES`, encodes a `ChainCheck` frame (the
  chain value at `next_sequence - 1`) and publishes it into the rings with
  that sequence as the chunk's `end_seq`. Rotations are announced the same
  way (`publish_rotate_to_rings`). Because the frame enters the ring after
  the batch that holds its position, it reaches the wire after the entries
  it covers, on kernel TCP and DPDK alike: both senders drain the same
  rings.
- **Wire.** `MSG_CHAIN_CHECK = 0x17` and `MSG_ROTATE = 0x16` share one
  41-byte layout, `SeqHashFrame` (`transport-core/src/replication/protocol.rs`).
  Any frame-layout change bumps `REPL_PROTOCOL_VERSION` (9 on `main`,
  unreleased), which the primary checks at authentication, where it
  admits a replica at or above its own (`judge_challenge_response`), and
  again in the replica's handshake (`decode_replica_message`), so a mixed
  pair fails with the version error. An unknown tag mid-stream decodes
  as an error and ends the session.
- **Catch-up.** Control frames in a ring chunk are classified against the
  stream position by `RingChunk::classify` (`replication/catchup.rs`),
  shared by the kernel sender's inline drain and the DPDK sender's
  `LiveHandoff`: behind is dropped, at is forwarded, ahead is forwarded once
  the disk back-fill reaches it.
- **Receipt.** `process_streaming_frames`
  (`server-runtime/src/replication/receiver_transport.rs`, shared by both
  receivers through `streaming_loop`) hands `Rotate` and `ChainCheck` to
  `queue_stream_mark`: a mark at the stream position is queued, one behind is
  dropped as re-delivery, one ahead is a `FrameError::SequenceGap`. A push
  that waits for input-ring space runs a callback on every pass
  (`keep_link_serviced`), so the receiver thread keeps working while the
  ring is full.
- **Repair is not reused.** A chain divergence is repaired by snapshot
  resync (`handle_session_exit`, `MAX_INPROCESS_DIVERGENCE_RESYNCS`,
  `archive_local_lineage`). An output divergence is not a damaged journal
  but a broken contract, so this plan takes none of that path, and moves
  no file: the snapshots of a stopped cluster are set aside by the operator
  as part of starting a new one (step 6).

### Where reports are produced and where they go, on each copy

- **One producer of reports.** Live matching, journal replay and the shadow
  all hand events to the application through `dispatch::dispatch`, which
  fills a `Vec<A::Report>`.
- **The matching stage publishes every report, whatever its connection**
  (`MatchingStage::run`): one `OutputPayload::Report` slot per report,
  stamped with the entry's `wire_seq` (`OutputSlot::wire_seq`), including
  those of ticks and other node events (`connection_id == 0`). An entry
  with no report publishes a dataless `OutputPayload::BatchEnd` slot **only
  if its connection is not 0**, so on a primary a client request with no
  report gets one, and on a replica, where every slot's connection is 0,
  nothing does. The shutdown drain (`MatchingStage::drain_remaining`) is
  the exception: it publishes a `BatchEnd` for every report-less entry,
  whatever its connection. Neither matters to the digest, which folds no
  `BatchEnd`, and windows still open at shutdown are dropped (decision 13).
  Ack-policy changes get their own slot on every copy.
  Queries take no sequence and publish an `OutputPayload::QueryResponse`, on
  the primary only (they are not journaled).
- **Positions are the same on every copy.** `wire_seq` is the primary's
  allocation, carried to replicas in the stream; the shadow re-derives it
  with the allocator's own rule (`JournalEvent::takes_wire_seq`) from the
  same `starting_wire_seq` as its pipeline (`shadow.rs`). A grid on
  `wire_seq` therefore lands on the same entries everywhere.
- **The primary's response stage does not encode every report.**
  `response_loop::run` hands each slot to the transport's `Egress` as a
  `SlotReply`, and the egress encodes it (`SlotReply::into_frames`) only
  after it has looked the connection up. A slot whose connection it does not
  hold, a node event's (connection 0) or a client's that has gone, returns
  `SlotOutcome::Unknown` and is never encoded ("a reply to a connection it
  does not hold is never encoded", in the loop's own comment). A slot after
  a shutdown seen during the gate's wait is not reached at all.
- **Answers that never enter the journal go another way.** Busy, a
  decoder's refusal, a filtered request and a halt's rejection are early
  replies (`early_reply.rs`, encoded by `encode_early_reply`), decided by
  the reader without publishing the request. A degraded reply is an
  ordinary slot whose trailer is `BatchEndDegraded`: its body is the same.
- **A replica encodes nothing.** Its pipeline's output ring has one consumer,
  a drain thread on the response core (`replication/mod.rs`, the `"drain"`
  thread), which copies slots out and discards them. It is not the matching
  thread.
- **The shadow encodes nothing.** `shadow::run` dispatches into a scratch
  `Vec` and clears it ("shadow discards all reports",
  `transport-core/src/shadow.rs`).
- **The encoding is the application's, the framing the runtime's.**
  `ResponseEncoder::encode_report` (`melin-app`) writes a body.
  `frame_app_response` (`server-runtime/src/response_frame.rs`) zeroes the
  body region, calls the encoder, bounds the result by `MAX_RESPONSE_BODY`
  and writes the header. In debug builds it first checks that the encoder
  is pure (`check_encoder_purity`): two more encodes over two fill
  patterns must agree below the returned length. The encoder's contract
  already says so (`ResponseEncoder`'s "Purity" section in `melin-app`,
  and "The determinism rules" in `docs/building-an-application.md`): the
  same value encodes to the same result whatever came before, and every
  byte below the returned length is written. Every application response
  is framed there today; the replica and the shadow encode nothing.
- **Ticks.** The primary's reader journals a `Tick` every
  `--tick-interval-ms` (default 250, `ServerConfig::tick_interval_ms`; 0
  disables tick generation, `ServerConfig::tick_interval`). Each is an
  entry with its own `wire_seq`, so an idle primary still advances the
  position, one entry per interval.

### What can stop a node at once

- **The fence latch** (`FenceState`, `transport-core/src/fence.rs`, one per
  node) is the existing stop. Once it is latched: the readers' `HaltGate::verdict`
  answers `Verdict::Close` to every client frame; `fence_if_superseded`
  co-sets the shutdown flag, so every stage leaves at its next check; the
  kernel egress's `on_shutdown` flushes nothing; and raft's
  `auto_promotion_decision`, `election_should_stand_down` and
  `challenger_should_campaign` all read `fenced`. The halt proper (refusing
  writes for want of a replica) keeps answering queries and releasing held
  replies, which is the opposite of what a diverged node must do, so the
  fence is the halt machinery this plan uses.
- **Raft leaves with the node.** The raft driver is handed the node's
  shutdown flag (`server.rs`, `spawn_raft_driver`; `raft.rs`) and leaves its
  loop within one 100 ms poll (`melin-raft`'s `driver.rs`). So a node whose
  shutdown flag is set drops out of the control plane, and a node that
  refuses to start never rejoins it: both entries resolve the store before
  any raft spawn, and `melin_raft::spawn` has no other caller. openraft
  counts a voter that is down: its majority is over the configured set.
- **Raft voters are capped from below only.** `build_raft_config` checks a
  minimum under auto-promotion (`raft.rs:91`), and nothing refuses more
  voters than data-plane nodes. A primary streams to at most two replicas
  (`ReplicaSlotCursors::SLOTS`, the two producer slots), so a fourth or
  fifth voter is a standby that can never be a connected replica. After
  the raft directory's first boot the stored membership is in force, and
  the driver compares it with the flags only inside its own thread, after
  `initialize`, where a failure can only `mark_stopped` and leave the node
  running without raft (`driver.rs:688`, `:801`); the store itself is
  opened synchronously in `melin_raft::spawn` (`driver.rs:524`), so the
  node's start can still fail there.
- **Replicas never re-point.** After a failover, surviving replicas keep
  dialling their `--replica-of` until an operator re-points them
  (`docs/replication.md`, "Deployment rules"), with reconnect backoff from
  1 s up to `MAX_BACKOFF` (30 s, `replication/mod.rs`).
- **Acks are read in a few places.** Kernel TCP reads acks in the live
  loop only, not during catch-up, snapshot transfer or the bridge
  (`tcp_sender.rs`). The DPDK sender keeps only the newest ack while a slot
  joins (`keep_last_ack` in `dpdk.rs`), and the streaming arm records it
  only once the join is over. Every ack that moves
  a cursor passes `ReplicaCursors::record_ack`
  (`transport-core/src/replication/cursors.rs:149`), which checks the ack
  against the sent high-water first, evicting on a violation, and then
  stores rather than takes a maximum. Its two callers are the kernel live
  loop (`tcp_sender.rs:1040`) and the DPDK streaming arm. Every read site
  skips frames it does not decode (`drain_ack_frames` in
  `receiver_transport.rs`, shared by both senders' streaming loops, and
  `keep_last_ack`).
- **The primary's handshake is read in one place per transport**, after
  authentication and before the epoch check: `handle_replica_connection`
  (`tcp_sender.rs:480`, the fence check at `:502`) and the DPDK
  `Handshaking` arm (`dpdk.rs:1033`, the fence check at `:1048`).
- **Nothing replication-facing outlives the pipeline today.** On kernel
  TCP the replication listener is bound once in the entry
  (`server.rs:1050`) but moved into the sender thread, whose supervisor
  joins its handlers and returns on the shutdown flag
  (`tcp_sender.rs:163`), dropping the listener. On DPDK the EAL and port
  (`DpdkShared`, `server.rs:2565`) live in the entry, but the transport,
  which holds the replication listener, is moved into
  `run_as_primary_dpdk` (`server.rs:3006`), whose poll loop ends with
  `close_all_links`: a 100 ms drain, then a reset of every link
  (`linger_then_reset`, `dpdk.rs:202`, `1526`). The primary's health
  endpoint runs on the node's shutdown flag (`spawn_health_endpoint`,
  `server.rs:4163`) and is joined with the pipeline (`stop_and_join`,
  `server.rs:1658`); only the
  replica's has a stop flag of its own, held by the entry
  (`spawn_replica_health`, `raft.rs:318`).
- **A replica reads primary messages in five places**, each ending in a
  wildcard arm that returns an error out of the receiver: the handshake's
  answer (`tcp_receiver.rs:783`, the `_` arm at `:854`; `dpdk.rs:1928`),
  the snapshot's begin and the segment seed's (`replication/mod.rs:1209`,
  `:1246`), the chunked body (`receiver_transport.rs:242`), the
  `StreamStart` that follows a snapshot transfer (`replication/mod.rs:1423`,
  the "expected StreamStart after snapshot" arm at `:1470`), and the
  stream (`process_streaming_frames`).
- **The replica's shutdown exit drains its acks.** `streaming_loop`'s
  shutdown arm (`receiver_transport.rs:975`) runs `drain_pending_acks`,
  which waits on the journal cursor and sends an ordinary cumulative ack
  before the session ends.
- **The response loop checks shutdown once per batch** (top of `'run`), and
  "a send with room goes through whatever the flag says": a batch already
  read is sent to its end. Stopping replies *at* detection needs a per-slot
  check.
- **Every role resolves its paths through `NodeStore::resolve`**
  (`transport-core/src/node_store.rs`), "a store in hand has passed its
  start-up checks", in both transports' entries before the journal is
  opened and before `spawn_raft_driver`. Its contract is "reads the
  filesystem, writes nothing", and tests call it to work out a node's paths
  after the node has run (`tests/ack_policy_changes.rs`), so a startup
  refusal belongs right after it, not inside it.
- **Snapshots** live at the store's snapshot path, with the previous one at
  `<snapshot>.prev`, kept for a manual rollback. Boot reads only the
  former. Each header carries its `journal_sequence`.

## Findings that shape the design

**1. Folding at the primary's response stage needs the encode lifted out of
the egress.** As it stands, the bytes the primary produces are exactly the
bytes it *delivers*, and delivery depends on which connections are open.
Folding there would make the digest depend on client connectivity, and a
replica, which delivers nothing, would disagree with every primary that has
ever seen a client disconnect. The loop must encode every `Report` slot
itself, fold it, and hand the egress the frames, so the egress can no
longer encode, skip or re-encode anything (decision 4).

**2. Only `Report` payloads are the same on every copy.** `BatchEnd`
slots exist on the primary only (connection 0 on replicas), query responses
on the primary only, request IDs and connection IDs per node, trailers per
backing. The digest therefore covers report bodies and their positions, and
nothing else of the slot (decision 1).

**3. Downstream of the output ring a window cannot be closed by position
alone.** An entry with no report puts nothing in the output ring, so a
consumer there cannot tell that the window's last entry has been applied:
under a stream of report-less ticks, a window would never close. The
matching stage must publish how far it has applied, in wire-sequence space
(decision 4). The input ring's progress counter will not do: it counts ring
slots, queries included.

**4. On a replica the cost lands on the drain thread, not the matching
thread.** The drain consumer is a thread of its own, so encoding and folding
there does not lengthen `apply`. It does take part in backpressure: a drain
that falls behind fills the output ring, which stops the matching stage,
which fills the input ring, which stops the receiver, whose in-memory ack is
what a `ram` or `disk+ram` primary waits on. So the drain must keep up with
the matching stage at full rate, which the measurement step checks
(decision 15). The shadow is a terminal consumer of the input ring too, so
the same holds for it, on the reader's side.

**5. The replica must buffer in both directions.** On the primary the digest
for a window exists only once the response stage has folded the window, and
the response stage releases a slot only once the ack policy holds it, so
under a replica-backed policy only after the replica has acknowledged it.
The replica's own digest is usually ready first, and has to wait. But the
replica's matching stage can also trail its receiver, so the primary's
digest can arrive first too. The comparator holds both sides (decision 6).
The primary still orders its frame behind the entries it covers, from the
journal stage: a digest ahead of the stream becomes a contiguity violation,
catch-up stays quiet, the wire stays ordered (decision 7).

**6. Raft carries nothing new: the stop travels the data plane, and quorum
loss does the rest.** A stopped node latches the fence before anything else
(decision 11), so until its raft driver leaves on the shutdown flag the
fenced inputs make it refuse auto-promotion, and the readers close every
client. Every place a replica honours a promotion request (the in-session
poll in `streaming_loop`, each receiver's reconnect-loop top) checks the
shutdown flag first, which the stop sets with the latch, and both read the
latch again once the replica pipeline is joined, so a trip by a shadow
still applying the entries the receiver fed it is caught before the
application is handed over (decision 11). If a request still
slipped through those checks (the reconnect-loop top's check-then-act
window), the primary pipeline it
builds shares the node's `FenceState`, so its readers close every client
and its stages stop on the shutdown flag. After the exit, the marker keeps
the node from restarting (decision 12), so it never rejoins raft.

What makes the stop cluster-wide is arithmetic on voters, not a raft
message. A cluster has at most three voters once decision 14 caps them,
and openraft's majority is over the configured set. So once two nodes have
stopped, no election can succeed. The stop reaches a second node in every
connected case: a replica that detects tells its primary through its acks;
a primary that detects, or is told, tells its connected replicas with a
halt frame (decision 11). A raft log entry would also reach a node that
missed both, but it costs the first application data in raft, a
forwarding RPC, a persisted halted state and a raft storage version. The
only case it would add is a partition around the detector (the limits).

Two conditions hold this argument up. First, a raft leader cut off from
its quorum keeps its role (openraft 0.9 has no check-quorum step-down),
so a replica already leading needs no election to promote. `main` now
closes that: before a promotion journals its epoch, a fresh majority round
trip must confirm the leadership, and with two nodes stopped nobody
answers, so the promotion is abandoned and the node exits (decision 14).
Second, a cluster with more than three voters keeps a majority after two
stop, which decision 14's voter cap refuses.

**7. Every copy applying the same journal makes the same tick calls.**
Until recently it did not: the dispatch watermark was per-process state,
restarting at 0 whenever a stage was built (at boot after replay, on
snapshot restore, on a resync, on promotion), so a copy restarted,
restored or promoted inside a run of repeated or regressed timestamps
called `Application::tick` where an uninterrupted copy did not. An
application whose `tick` is not idempotent against a time already passed
(the roadmap names the Exchange Core's, which stamps its rate limiter's
clock) then held different state on the two copies, and with this plan
the first report to show it would have stopped the cluster for a cause
the runtime set. `main` closed that half of determinism audit finding 17
(merge 87e36302): replay hands its watermark to the stages it seeds, a
snapshot records the watermark of its position (snapshot framing version
5), a resynced replica takes the one its primary's snapshot records, and
a promotion carries the replica stage's into the primary pipeline, in the
matching and the shadow stages alike, pinned end to end by
`server-runtime/tests/tick_watermark.rs`. The plan's position:

- **What remains of the tick contract cannot split copies.** The rest of
  the roadmap item "Monotonic sequencer time"
  ([monotonic-sequencer-time-2026-09.md](monotonic-sequencer-time-2026-09.md))
  is open: time going backwards (a clock step on the primary, a failover
  to a node whose clock is behind), the producer race that journals
  timestamps out of order, and the double call per journaled `Tick`
  (`dispatch.rs`), whose repeat is at the watermark, a time already
  passed. Each is a
  function of the journal, so every copy sees it alike: it can make an
  application's own clock odd, never make two copies differ. That item is
  independent of this plan, neither a build prerequisite nor a release
  one (open question 8).
- **One residue, once.** A snapshot written before framing version 5
  records no watermark. Restoring one starts from the timestamp of the
  journal entry at its sequence (exact unless timestamps went backwards
  before it), or from zero when the journal no longer holds that entry,
  and logs a warning naming the snapshot; the node's next snapshot records
  the watermark. Such a snapshot exists only on a node that has not yet
  written one since upgrading to the release with format 5. Step 6's
  runbook names that warning, beside a divergence that follows it, as
  the one tick-related suspect left.
- **The stop is right whenever it fires.** If copies do differ around a
  restart, they differ, which is exactly what the product promises cannot
  happen silently; suppressing the check around restarts would hide a real
  divergence. The rule stays in the contract (`Application::tick`'s
  rustdoc: "a call for a time already passed must change nothing", and
  "The determinism rules" in `docs/building-an-application.md`), for the
  application's own correctness rather than for agreement between copies.
- **The tests break the rule on purpose.** `main` now has `TickCountingApp`
  (`transport-core`'s `test_support`, behind `test-utils`, with
  `TickCountingCodec` to serve it through a node): its `tick` counts every
  call, an equal or earlier time included, and every report carries the
  count. Step 5's no-false-positive tests across a restart, a recovery
  and a promotion run it, rather than a rule-following application, so a
  copy that made one tick call more or fewer reaches the digest and would
  stop the cluster: they pin that the runtime keeps the copies' tick calls
  equal, not that a well-behaved application hides a difference.
  `TestApp` stays as it is: the dispatch tests count its calls on purpose.

## Design decisions

### 1. What is digested

For each window `W` (decision 2), on every copy:

```text
d(W) = XXH3-128 (seed 0, default secret) over
    "melin output digest v1" || kind (u8 = 1, output)
        || K (u64 LE) || window_end (u64 LE)
    then, for every Report slot whose wire_seq is in W, in output order:
        wire_seq (u64 LE) || body_len (u32 LE) || body
```

- **`body` is exactly what the application's encoder wrote**, after the
  runtime's bound check, so what is folded is what a client receives,
  without the runtime's header or trailer. An encode that fails (the
  encoder's `Err`, or a body over `MAX_RESPONSE_BODY`) folds
  `body_len = u32::MAX` and no body. Failing the same way on every copy is
  agreement; failing on one copy is divergence.
- **`wire_seq` per report** makes moving a report to another entry a
  difference, and `body_len` makes the concatenation unambiguous.
- **Not folded:** `BatchEnd` and `QueryResponse` slots, ack-policy change
  slots, request and connection IDs, trailers (degraded or not), and every
  early reply. Each of these differs by node or never reaches a replica
  (finding 2). Reports of node events (connection 0) and reports to clients
  that have gone **are** folded: they are the application's output whether
  or not anyone receives them.
- **The encoder's contract is already the one the digest needs.**
  `ResponseEncoder`'s "Purity" section and "The determinism rules" in
  `docs/building-an-application.md` require the same result for the same
  value whatever was encoded before, and every byte below the returned
  length written. The zeroing makes a skipped byte zero on every copy, so
  only an encoder whose output depends on something other than its input
  (its own state, the clock, the node) can make copies disagree. The
  debug-build purity check catches that in an application's tests. What
  changes is the consequence: an encoder that breaks the contract now
  stops replicas, which the contract text and `CHANGELOG.md` **Changed**
  say, and the contract adds "no node identity" to its examples.
- **Why XXH3-128, not BLAKE3.** The digest compares copies the operator
  runs, fed by their own `apply`, so the only collisions that matter are
  accidental, and at 128 bits those are not a practical concern. What
  BLAKE3 adds, resistance to an adversary constructing a matching input,
  buys nothing here, and its speed on large inputs comes from hashing
  whole chunks side by side, which a stream fed one report of tens of
  bytes at a time never offers: every 64 bytes folded pays a full serial
  compression, on the primary's response thread among others. XXH3's
  per-byte cost on the same stream is a fraction of that. The fold runs
  per report, so the choice is confirmed by measurement, not by this
  estimate (decision 15, open question 1).
- **The crate and its API.** `twox-hash` 2.x (MIT, on `deny.toml`'s allow
  list). The window digest is streamed, since a window is fed one report at
  a time: `xxhash3_128::RawHasher` over `SecretBuffer::default()`, `write`
  per piece, `finish_128` at the window's end. Neither hasher has a reset,
  so a window starts with a fresh one. A fresh
  `RawHasher::new(SecretBuffer::default())`, assigned over the old one, is
  a plain value with its stripe buffer inline and the default secret by
  `'static` reference: no heap, built once per window. Not the crate's
  `Hasher` instance, whose constructor boxes a copy of the secret, so
  starting each window with one would allocate, and nothing on the
  response thread may. The chain step (decision 3) is one-shot over 32
  bytes, `xxhash3_128::Hasher::oneshot`, which needs no instance and no
  `alloc`. Seed 0 with the default secret is the reference XXH3-128, so the
  pinned vectors of step 1 can be cross-checked against the reference
  implementation (`xxh128sum`). The digest's bytes are
  `finish_128().to_be_bytes()` (and the same for `oneshot`), the
  reference's canonical form (high half first, big-endian), so the
  marker's hex reads the same as that tool's output.
- **Two newtypes, not one.** `WindowDigest` (a window's `d`) and
  `ChainValue` (decision 3's `C`) are each a `[u8; 16]` newtype, so a chain
  value cannot be compared against, or chained as, a window digest: the
  type system makes the swap impossible rather than a test catching it.
  Byte arrays rather than `u128`: the frames, the board payload and the
  marker carry bytes, a byte array has no byte-order question left once
  built, and its alignment of 1 keeps the board's payload free of padding
  (a `u128` beside the `u64` window end would align it to 16 and pad it,
  failing `NoPadding`).
- **The domain string and the kind** make the definition explicit. `K` is
  part of it (decision 2): changing the algorithm, the string, the layout
  or `K` later is a new definition, sent under a new kind value (decision
  8), so a replica that does not know it skips it and counts it rather than
  reporting a divergence: a wire-visible change, never a silent one.
  `transport-core`, where the fold lives, takes `twox-hash` as an ordinary
  dependency.
- **The journal chain keeps BLAKE3.** The chain is the journal's audit
  trail: its worth is that no entry can be altered while keeping the chain
  intact, which needs exactly the adversarial resistance XXH3 does not
  claim. It hashes per entry on the journal stage, batched with writes it
  makes anyway, so it is not where the per-report cost sits; and changing
  it would break the segment format, `ChainCheck` on the wire and every
  existing lineage. Two hashes for two jobs, not a migration.
- **Why the encoded body, not `std::hash::Hash` on the typed report.**
  Hashing the typed `Report` would spare the replica and the shadow the
  encode, and was considered. It was rejected: `Hash`'s contract is
  consistency with `Eq` for map keys, so an implementation may skip fields,
  and every skipped field is a divergence unseen; the std docs say what
  `Hash` feeds a `Hasher` is neither portable across platforms (byte
  order, `usize`, derived enum discriminants) nor stable across compiler
  releases; and a `Hash` impl is a second description of each report
  beside the encoder, free to drift from what clients receive. The
  encoder's body is the one canonical, portable form of a report. If the
  measurement shows the replicas cannot afford the encode, the fallback
  is a purpose-built digest trait in `melin-app` with fixed-width
  little-endian rules, not `Hash`.

### 2. Windows by position, and a fixed K

- **Windows are absolute positions.** Window `m` holds positions
  `(m-1)·K + 1 ..= m·K` of `wire_seq` and is named by its end `m·K`. Every
  report slot already carries its `wire_seq`, and every copy has the same
  positions (what the code shows), so the primary's live folder, the
  replica's drain folder and the shadow all cut the same grid with no
  coordination. The grid is positions alone, so output slots carry nothing
  new.
- **A copy digests only windows it saw whole.** A folder starting at
  position `s` (the first position its pipeline applies live) digests first
  the window ending at `F = (ceil((s-1)/K) + 1)·K`, the first that begins at
  or after `s`. Replay is not folded. So a restarted, restored, caught-up or
  promoted copy starts at its next full window, and a partial window is
  never digested.
- **K is a constant, `OUTPUT_DIGEST_WINDOW = 64`, part of kind 1's
  definition.** Every build of a kind cuts the same grid, so nothing about
  it is configured or exchanged. With windows chained (decision 3),
  the primary forwarding only some windows and a board lapping a window
  both cost no coverage, so `K` is not bounded below by how many frames
  the primary can send. It is bounded below by two costs only:
  - **The per-window work**: a fresh `RawHasher`, its finalize, one
    one-shot chain hash over 32 bytes, and one 40-byte seqlock store into
    a board slot that is usually not in cache, all paid once per `K`
    entries, on the primary's response thread and the replica's drain
    (and on the replica's receiver, a board read and one chain hash per
    window, decision 9). The per-report encode and update are paid per
    report and should dominate it. The bound to hold is that the
    per-window work, divided by `K`, is a small share of the ~100 ns
    per-event budget; step 6's measurement checks it, and if it does not
    hold, open question 9's next value is the fallback.
  - **The board and comparator sizing** `R = ceil((input ring slots +
    output ring slots) / K) + 2` (decision 5), from the pipeline's
    configured `--ring-capacity` (both rings take the same `RingCapacity`).
    At the default capacity (`1 << 20` slots per ring) and `K = 64` that is
    32770 slots, one cache line each, so a board of 2 MiB, and comparator
    arrays of the same count at 40 bytes a slot. Both scale with the ring
    capacity, so they stay small beside the rings themselves, allocated
    once per pipeline; a smaller `K` grows them in proportion. `R` is a headroom in entries
    (decision 5 says what it bounds and what it does not), not a
    guarantee that no window is ever lapped.
- **What `K` costs in delay.** A divergence is seen once its window closes
  and the primary's next frame reaches the replica: within `K` entries
  plus one fsync batch, and under a replica-backed ack policy the replica's
  ack, since the primary folds a report only once the gate releases it;
  step 6's dated measurement gives the figure. An idle primary
  advances one entry per tick, so idle it is at most `K` tick intervals
  plus one: at the default `--tick-interval-ms 250`, about sixteen
  seconds. With ticks disabled an idle window waits for traffic to fill it,
  and is dropped if the node shuts down first. Both are stated as limits.
- **The off switch.** `--no-output-digests` (`ServerConfig::no_output_digests`,
  default `false`) turns every folder and every check on the node off, and
  a primary with it sends no digests. Each node's own flag governs what it
  computes; the cross-node check runs where both ends have it on. A
  mismatch of the flag across nodes only loses coverage, never reports a
  divergence, and is visible: `/healthz` and `/metrics` show whether
  digests are on, the node logs it at startup (`warn!` when off), and a
  replica whose primary sends none logs it once per session (decision 16).

### 3. A chain per comparison

- **Every folder chains its windows.** Beside each window digest `d_i`, a
  folder keeps `C_i = XXH3-128(C_{i-1} || d_i)` (one-shot, 32 bytes in,
  canonical big-endian bytes out), from `C = CHAIN_SEED` (16 zero bytes)
  before its first window `F`. The chain starts when the folder's pipeline
  starts and lives as long as it: nothing of it is stored, journaled or
  negotiated.
- **Why a chain.** The primary forwards at most one window per fsync batch
  (decision 7), and a board can lap a window before a reader takes it, so
  most windows are never compared on their own. One equal chain value at
  window `e` vouches for
  every window from where both chains agreed to `e`, so sampling costs
  delay, never coverage.
- **Two comparisons, two starting points.**
  - *Shadow against live, in one process* (decision 10): both folders
    start at the same pipeline start, on the same grid, from the same seed,
    so their chain values compare directly at any window both have closed.
    The run covers every window from `F`.
  - *Replica against primary* (decision 9): the two chains started in
    different processes at different positions, so the replica cannot
    compare its own `C` with the primary's. It takes the **first** primary
    frame it accepts after its own start, a local gap, an uncompared
    session or a change of primary pipeline (below), at window `e₀`, as
    its starting value,
    without checking its chain value, and extends that value with its own
    local `d_i` for every later window: `C'_{e₀+K} = H(C_{e₀} || d_{e₀+K})`
    and so on. Each later frame at `e` is compared against `C'_e`. A match
    at `e` verifies every window from `e₀ + 1` to `e`, and a mismatch says
    some window in that range differs. The frame also carries `d_e`
    (open question 10, decided): comparing it with the local `d_e` checks the
    anchor's own window too, and names window `e` as one that differs when
    it does.
- **A local gap breaks the run, never the claim.** If the replica's own
  board laps a window before its comparator copied it, the extension cannot
  continue: the comparator counts a chain break, ends the run, counts the
  windows from the run's end to the next anchor as unverified, and takes
  the next frame as a fresh starting value. `verified_through` never
  advances over the gap, because the run it belongs to ends before it.
  The shadow side cannot break: the shadow feeds its comparator its own
  windows directly, in order.
- **A new primary pipeline is a new starting value; a reconnect is not.**
  The primary's chain lives as long as its pipeline, so what a replica must
  know at each session start is whether the primary's chain continued or
  restarted from the seed. Nothing it already sees says so: a primary
  restarted at the same address keeps its node identity, its journal and
  even its epoch, and a reconnect to a running primary looks the same
  until a frame arrives. Keeping the run across a restart would turn the
  restarted chain's first frame into a false mismatch, which stops a
  healthy replica; dropping it at every reconnect loses coverage on a
  routine event (the windows between the last match and the new session's
  first frame, and every window applied during the catch-up, which carries
  no digests). So the primary names its chain:
  - **The pipeline id.** Each primary pipeline build draws a random 128-bit
    `PipelineId` and announces it on every `StreamStart` (decision 8). It is
    drawn exactly where the primary's live folder is made: `run_as_primary`
    and `run_as_primary_dpdk` (`server.rs`) each draw one with
    `PipelineId::draw()` and hand it, by value, to
    `build_pipeline_with_replication`, the one constructor of that folder,
    which keeps it in the `Pipeline` for the senders. A boot (after recovery
    from snapshot and journal) and a promotion both go through those
    functions, so each gets a new id; a primary never rebuilds its pipeline
    in place (resync and snapshot rebuild are replica paths), so the rule
    "the id changes whenever the folder's chain restarts from the seed"
    holds with no exception to remember. `PipelineId` is neither `Clone`
    nor `Copy` and `draw` is its only constructor, so one id cannot be
    handed to two builds, and a test harness cannot build a primary with an
    id equal to an earlier one: it is drawn per pipeline start, not per
    process or per node identity. The wire takes the raw bytes:
    `encode_stream_start` (`replication/protocol.rs`) gains a `pipeline_id: [u8; 16]`
    argument, and every sender passes it from the pipeline's id through
    the by-reference accessor `PipelineId::as_bytes(&self) -> &[u8; 16]`,
    so the guard holds where ids are made (only `draw` makes a
    `PipelineId`) while a scripted primary in a test of another crate can
    still encode a `StreamStart` naming any id it needs.
  - **Why random, and why 128 bits.** A counter or a start time would have
    to be stored or trusted (a counter restarts from the same value after a
    crash unless it is persisted, and the plan stores nothing; a wall clock
    can repeat across a clock step), and everything else a node has is the
    same across its restart. A random draw needs neither. 128 bits, the
    width of the digest itself, so the id is never the weak link: an
    accidental equal draw would keep a run against a restarted chain and
    stop a healthy replica (never hide a divergence), and at 128 bits that
    is out of reach, for 16 bytes once per session. It is read once per
    build with `getrandom::fill`, the call the node already uses for its
    nonces and the journal's `fresh_anchor`: `transport-core` takes
    `getrandom` 0.4, already a direct dependency of `server-runtime`,
    `journal` and `raft` at that version, so `Cargo.lock` gains an edge and
    no package, and `deny.toml` is unchanged. Not a read of `/dev/urandom`
    through `std`: the same source, but a file descriptor to open, a
    failure in a chroot or a container without `/dev`, and a second way of
    reading randomness beside the codebase's one. A failed draw fails the
    primary's start, as a failed nonce draw does.
  - **At each session start** the replica compares the id with the
    previous compared session's (decision 6). Equal: the run, its anchor
    and the expected side are kept, and the session's first frame is
    compared as an ordinary later frame. It then verifies every window
    applied since the last match, the reconnect's catch-up included, as
    long as the replica's own board did not lap meanwhile (decision 5).
    Different, or no previous session: the run ends and the next frame
    anchors a new one, as at the replica's start. The previous id is held
    by the comparator itself, which lives exactly as long as the replica's
    pipeline (decision 9), so every path that rebuilds that pipeline (a
    snapshot resync at session start, the in-process repair of a chain
    divergence) starts with no previous id and re-anchors: there is no
    separate variable to forget to clear.

  Nothing is stored, the id is never journaled, and the snapshot format,
  replay and the journal are untouched.

### 4. Where each copy folds: one folder, three call sites

`transport-core`'s new `output_digest` module holds `WindowFolder`: it
encodes a report's body through the application's `ResponseEncoder` into
the caller's buffer, folds `(wire_seq, body)` into the open window,
closes windows as positions pass, extends its chain, and publishes each
closed window to a `DigestBoard` (decision 5). The body encode itself
moves from `response_frame.rs` into one free function there,
`encode_report_body`: the zeroing of the body region, the debug-build
purity check (`check_encoder_purity`), the encoder's call and the
`MAX_RESPONSE_BODY` check, all of what `frame_app_response` and
`frame_zeroed` do today except the header. The folder calls it, and
`frame_app_response` keeps only the header and takes it as its body
closure. So the bound a client's frame obeys and the bound the fold
folds are one statement. And so are the zeroing and the purity check,
which then cover the replica's drain and the shadow too: those encodes
never pass through `frame_app_response`, and a copy of the zeroing there
could drift from the original. Early replies call it too, without a
folder, so a halt's rejection is framed as today and folded nowhere.
`server-runtime` keeps re-exporting `MAX_RESPONSE_BODY` from where it is
today.

**Encode failures are logged by what they cost.** Today an encode error
is logged at `error!` by both egresses (`response.rs`, `dpdk_response.rs`),
and only for a reply due to a client. Once every copy encodes every
report, the same failure would also fire for:
- reports nobody receives: node events' reports to connection 0, replies
  to gone clients;
- the replica's and the shadow's encodes.

An application whose encoder refuses an internal report variant on
purpose would then fill every node's log with `error!` lines for no fault
of the server, which breaks the log-level rule. So the split is:
- **The fold** records the failure silently (`u32::MAX`, as above) and
  counts it in `melin_output_encode_errors_total{copy="live"|"shadow"}`.
  The label keeps a primary's two copies from summing to twice the
  failures.
- **The folder** logs at `warn!` once per copy at the first failure, then
  at each doubling of the count, the same rate limit as chain breaks,
  with the encoder's static reason.
- **The egress** keeps `error!` for a reply due to a live client, since
  that is a dropped reply.

`ResponseEncoder::encode_report`'s rustdoc ("the runtime logs at error
level and drops the response") is updated in step 2 to say both.

Three call sites, each with one folder:

| Copy | Thread | Closes a window when |
| --- | --- | --- |
| Primary, live | response stage (`response_loop::run`, so both transports) | any slot is read whose position the window cannot reach (a `Report` or `BatchEnd` slot at a `wire_seq` past the window's end, a query slot at or past it), or the output ring is empty and the matching stage has applied past the window's end |
| Replica, live | the drain thread, hoisted out of `replication/mod.rs` into a function with tests | same rule as the primary |
| Shadow, primary and replica | shadow stage (`shadow::run`) | it has dispatched the window's last position: it sees every position |

- **Any slot closes what lies behind it.** The matching stage writes the
  output ring in `wire_seq` order. A `BatchEnd` at `w` follows every
  report below `w`, and a query slot carries the position before it
  (`pipeline.rs`, the `is_query_event` stamp), so every report at or below
  that position precedes it. Without this rule, a busy primary whose
  requests produce no reports would close windows late: its ring holds
  `BatchEnd` slots and is rarely empty. The rule costs one comparison per
  slot and changes no digest, because which window a report belongs to is
  fixed by its position. Only the moment a window closes moves.
- **The matching stage publishes its applied position.** One `Release` store
  of `next_wire_seq - 1` per batch, after `out_batch.commit()`, into a
  `WireSeq` cursor in `PipelineCursors` (`matching_applied_wire_seq`). An
  output-ring consumer loads it (`Acquire`) *before* reading the ring and,
  if the ring is then empty, every report at or below it has been read, so
  every window ending at or below it can close (finding 3). Windows with no
  report close with their header alone, on every copy alike. This is the
  only change to the matching stage.
- **The primary folds every report, whatever happens to its delivery.**
  `Egress::send_slot` takes the encoded `SlotFrames` instead of a
  `SlotReply`, and the egress no longer sees the encoder: it cannot skip the
  encode or do it differently. The loop, for each `Report` slot: encodes
  through the folder, offers the frames to the egress (if the gate gave a
  reply and the stop latch is clear, decision 11), then folds the encoded
  body, whatever `SlotOutcome` came back. The fold comes after the send so
  that the hash is not on the reply's own latency, only on the stage's
  throughput. `SlotOutcome::Unknown` comes to mean "not sent" rather than
  "not encoded". The cost is encoding reports nobody receives (node events',
  gone clients'), which the primary skips today.
- **Replica and shadow encode with the same encoder.** The node already
  holds it as an `Arc<dyn ResponseEncoder>`. It goes to the replica builder
  and to `shadow::run`, which feed it to their folders.
- **Off is no folder.** With `--no-output-digests`, the pipeline builders
  create no folder and no board, and the response stage hands the egress
  frames encoded as today, without the fold.

### 5. The digest board: one writer, many readers, no backpressure

Each folder publishes to a **`DigestBoard`**: `R` slots of
`(window_end, d, C)`, the slot for window end `E` being `(E / K) % R`, each a
`melin_pipeline::seqlock` (a `repr(C)` 40-byte payload, a `u64` window end
then the 16-byte `WindowDigest` and the 16-byte `ChainValue`, without
padding, so it satisfies `NoPadding`), plus an `AtomicU64` holding the
latest window end for cheap change detection. A seqlock rather than a
queue: a folder must never wait on a reader, a reader that falls behind
should lose old windows rather than stall anyone, and there are several
readers (the journal stage's injector, the shadow's comparator, the
replica's receiver, the health endpoint). The seqlock module already
enforces single-writer and no-padding in the type system.

**Sizing R.** `R = ceil((input ring slots + output ring slots) / K) + 2`,
computed when the pipeline is built from the `RingCapacity` it builds both
rings with (`ring_capacity.slots()`, `pipeline.rs:3665`, set by
`--ring-capacity`), and allocated then (decision 2 gives its size at the
default). It is not a constant: the rings are configurable, and a board
sized for the default would lap inside the ring-bounded backlog below on a
node run with larger rings. It matters for one reader only: the
replica's comparator, which must copy *every* window of its local board
(decision 3). The receiver reads its board at each loop iteration and on
every pass of a push waiting for ring space (beside `keep_link_serviced`),
so a full ring does not stop it reading. The board laps a window when the
drain closes more than `R` windows between two of those reads, and what
the drain can close at once is its own backlog: the entries the receiver
has published that the drain has not yet closed a window over. That
backlog is bounded by the rings only in part:

- entries still in the input ring, at most its capacity;
- applied entries whose reports wait in the output ring, at most its
  capacity;
- applied entries that published **no** output slot, bounded by no ring.
  The matching stage publishes nothing for an entry with no report and
  connection id 0 (`MatchingStage::run` pushes its `BatchEnd` slot only
  when `connection_id != 0`), and on a replica every entry has connection
  id 0, so every report-less entry (a tick whose application reports
  nothing, a request whose application answers nobody) keeps the matching
  stage moving while the output ring stays empty. A drain descheduled
  while such traffic flows comes back to a backlog of everything applied
  meanwhile, and the idle-close rule (decision 4) closes it in one burst of
  header-only windows.

So the lap comes from a stalled **drain** under report-less traffic, or
from a stall longer than the rings' worth of reports, not from a slow
receiver: a receiver that stops reading also stops publishing, and the
drain cannot get more than the backlog above ahead of it. `R` covers the
ring-bounded part, so a lap needs a drain stall worth about `R·K` entries
of report-less traffic. When it happens it is a chain break, counted and
logged (decision 16), never a divergence and never a false claim: the
comparator sees the gap and ends the run (decision 6). Open question 11
weighs removing the class instead. For every other reader a lapped window
is a skipped sample and costs nothing: the journal stage forwards a later
window, the shadow compares at a later one, the health endpoint shows the
latest.

**Between sessions and through a catch-up.** A run kept across a reconnect
(decision 3) needs the local chain unbroken from the last match, through
the time with no session and the journal catch-up that follows, to the new
session's first frame. The bound above covers both, and the catch-up's
length is not part of it:

- **No session.** The receiver reads no board between sessions, but it
  publishes nothing either, and the next session does not start until the
  journal covers what the last one published (`settled_resume_point`). A
  disconnect adds nothing to the drain's backlog, since nothing is
  published, so whatever its length it can lap only what could lap live:
  the backlog the session left, which the bullet below bounds as it does a
  live one.
- **Catch-up.** The primary then streams from disk as fast as the link
  carries it, and the receiver is busy publishing. `process_streaming_frames`
  checks its `MAX_SLOTS_PER_CYCLE` (512) cap only at a frame boundary, so
  one call publishes up to the cap plus one whole frame's slots before the
  loop returns to its top, where the board is read. A catch-up frame is
  large (the journal catch-up targets about 64 KiB, hundreds of entries;
  small ticks give more) and any frame is at most `MAX_DATA_FRAME`
  (768 KiB), so between two reads the receiver can publish far more than
  512 entries. It cannot publish more than the input ring's free space
  either: a push that waits for ring space reads the board on each pass.
  So between two reads it publishes at most the smaller of the cap plus
  one frame and the input ring's capacity, both within the input ring's
  term of `R`, however long the catch-up runs, and
  the drain cannot close more than its own backlog plus that between two
  reads, as live.
- **What can still lap** is what can lap live: a drain stalled while about
  `R·K` entries of report-less traffic are applied. A catch-up is where
  that is likeliest, since it drives the matching stage at full rate, and
  a catch-up after a long idle spell is mostly ticks. Such a lap is a chain
  break like any other: counted, logged, `verified_through` never crosses
  it, and the next frame anchors anew.

The comparator need not hold the catch-up's windows either: inside a run a
local window is extended as it is pushed, and the first frame after the
catch-up is compared at its own window, which, arriving in stream order
just after its entries, lies within the rings' span of the replica's
position, so its `C'` is still held or still to come.

Lookups are `get(end) -> Option<BoardEntry>` (present, not yet written, or
lapped) and `latest() -> Option<BoardEntry>`. Positions are `WireSeq`, the
existing cursor-space newtype. The board and the comparator are built
through one constructor taking the capacity: production builds them only
through `DigestBoard::for_pipeline(ring_capacity)` (and the comparator's
counterpart), which computes `R` from the pipeline's `RingCapacity` with
one shared function, and tests build small ones to force laps. They share one indexing helper, so the two cannot
index differently.

### 6. The comparator: one pure type for both checks

A `DigestComparator<T: Trip>` (`transport-core`, no I/O, no threads) holds
the node's stop, `T`, and two fixed-capacity arrays of `R` slots, indexed as
the board is:

- **local**: `(end, d, C')` for each local window, fed strictly in order
  (`push_local`), `C'` filled once a run is anchored;
- **expected**: `(end, d, C)` of the reference side's windows not yet
  matched against a local one (`push_expected`).

Nothing allocates after construction. It is built in one of two modes,
which differ only in how a run starts:

- `DigestComparator::shared_start(F)` (shadow against live): the run is
  anchored from construction at `(F - K, CHAIN_SEED)`, the starting point
  both chains share, so the run covers every window from `F`.
- `DigestComparator::anchored(F)` (replica against primary): no run until
  the first usable expected entry (decision 3). It takes `F`, the local
  folder's first window, as `shared_start` does: the replica knows its
  start position, and without `F` an expected entry that arrives before
  the first local window could anchor below it, and the first local push
  would then read as a gap, a spurious `Break` at every replica start.

The rules, for both modes:

- **Local windows.** A local window whose end is not the previous one plus
  `K` is a gap: the run (if any) ends, `Break { after, lapped }` is
  returned, and the comparator waits for a new anchor. Otherwise it is
  stored, and, inside a run, extended: `C' = H(C'_prev || d)`. Any expected
  entry waiting for that end is then compared.
- **Expected entries.** With no run, an entry at `e` anchors one if every
  local window after `e` is still to come or still held, unbroken: `e` is
  at or above `F - K` and at or above the last gap, and window `e + K` has
  not been evicted. Otherwise it is dropped and the next entry is tried;
  its windows are not lost from the accounting, since the run that does
  anchor counts every window before it as unverified. If the local window
  `e` is held, anchoring checks `d_e` against it at once and the run starts
  at `e - K + 1`. If it is still to come (the replica's matching commonly
  trails its receiver), the entry is held as the pending anchor until the
  local push at `e` lands, then checked the same way, so a clean anchor
  leaves its own window verified rather than unverified; expected entries
  arriving meanwhile wait as they would inside a run. Only when window `e`
  will never be compared (it was evicted, a local gap passed over it, or
  `e = F - K`, the position before the first local window) does the run
  start at `e + 1`; a pending anchor that a gap passes over is re-tried
  under the rule above, and so dropped. Anchoring computes `C'` for every
  held window after `e`. Inside a run, an entry at or below `verified_through` is
  re-delivery and dropped; one the local side has extended to is compared
  at once; one ahead of it waits, and when the array is full the oldest
  waiting entry gives way (a pending anchor is held apart from the array
  and never gives way: only a gap passing over it drops it, as above, and
  decision 5's sizing keeps the drain from trailing the receiver by `R`
  windows, so the array does not fill behind it in practice); one whose
  local `C'` has been evicted is a
  skipped sample. A dropped or skipped entry costs delay, not coverage: the
  next comparison covers its windows.
- **Session start** (decision 9): `begin_session(SessionKind)`, where
  `SessionKind` is `Compared(PrimaryPipelineId)` or `Uncompared`
  (a `StreamStart` with no output bit: the primary sends no digests), an
  enum so a caller cannot forget to say which, and so an uncompared
  session carries no id to compare. A primary on another build is
  `Compared` like any other (open question 2), and the comparator is
  never told the build: with no build in its input, no rule here can
  read one, so nothing can come to skip on it. The session's build goes
  to the latch instead, which names it in the stop (decision 11). The comparator
  holds the id of the last compared session, `Option<PrimaryPipelineId>`,
  `None` at construction.
  - `Compared(id)` with the held id: nothing changes. The run, its anchor,
    a pending anchor and the expected array are kept, and the session's
    first entry is compared as any later one (at or below
    `verified_through` dropped as re-delivery, otherwise compared or
    waiting). Local windows pushed between the sessions extend the run as
    they arrive. The held id can come with no run (a `Break` since the last
    match, or a previous session that never anchored); then too nothing
    changes: expected entries already held stay, and the first usable one
    (at or above the last local gap) anchors once its local window lands,
    as in any run-less state. Returns `SessionStart::Continued`.
  - `Compared(id)` with another id, or with none held: the run ends, the
    expected array and any pending anchor are cleared, the id is held, and
    the next usable entry anchors. A stretch begun here is caused
    `new_primary` when an id was held, and continues whatever stretch was
    open when none was (a replica's first session continues its `start`
    stretch, a session after an uncompared one its `uncompared` stretch).
    Returns `SessionStart::Reanchor { cause }`: `NewPrimary` when an id
    was held; with none held, `AfterUncompared` when the open stretch
    contains an `Uncompared` session (the comparator already records this
    for the accounting below, and no run can anchor in between, so no new
    state is needed), else `FirstSession`.
  - `Uncompared`: as a new id, and the held id is forgotten, so the next
    compared session re-anchors whatever id it names; the receiver feeds
    no expected entries, so no run can anchor in it, and the comparator
    remembers that the current stretch of unverified windows contains one.
    Returns `SessionStart::Reanchor { cause: Uncompared }`.

  The local array is kept in every case, so a window closed before a
  reconnect can extend the kept run, or a run anchored by the new session's
  first frame.
- **Accounting has one owner: the comparator, at `Anchored`.** Each
  unverified window is counted once, by the `Anchored` that ends the
  stretch it lies in, under the stretch's cause: `uncompared` if any
  `Uncompared` session fell inside the stretch, else `gap` if the stretch
  began at a local break, else `new_primary` if it began at a session with
  a new pipeline id, else `start`. A session with the held id begins no
  stretch and counts nothing. A `Break` reports the lapped windows for its
  log and `chain_breaks_total` but adds nothing to the unverified counter,
  and the receiver counts nothing itself, so a stretch spanning an
  uncompared session and a new primary is never counted under two
  reasons. Windows of a stretch no run has yet ended are not counted until
  one does; until then they show as `verified_through` standing still.

Every call returns the verdicts it produced:

- `Match { end }`: advances the run's `verified_through` to `end`, counted;
- `Mismatch { divergence, halted }`: the divergence verdict, returned
  only after the comparator has already called `T::trip(divergence)`.
  `divergence` is `{ end, run_from, verified_through, expected, actual }`,
  where each side is a `(WindowDigest, ChainValue)` pair. The
  divergence
  lies in `(verified_through or run_from - 1, end]`, and when the two
  `d_e` differ, window `end` itself is one that differs. When the anchor's
  own `d_e` differs, no run exists yet: `run_from` is the window's first
  position `end - K + 1`, `verified_through` is `None` (`Option<WireSeq>`,
  so no caller can read an absent run as position 0), and the range is
  window `end` alone. `halted` is the trip's `Option<T::Token>`: `None`
  when another site tripped first, and then the caller only stops. A
  caller cannot hold a mismatch the node has not acted on: the replies are
  halted before the verdict exists, so dropping it changes nothing. After
  a `Mismatch` the comparator is spent: every later call returns no
  verdict;
- `Anchored { end, unverified, reason }`: a run starts, and the windows
  between the previous run's end (or `F`) and the new run's start are
  counted unverified under `reason` (`start`, `new_primary`, `gap` or
  `uncompared`, by the rule above);
- `Break { after, lapped }`: the run ended at a local gap of `lapped`
  windows.

**Why the comparator trips, and why through a trait.** If the comparator
only returned the verdict and left the trip to its caller, a call site
that matched `Mismatch` and ignored it would compile: Rust has no linear
types, and `#[must_use]` is only a lint. Holding the stop closes that
gap, since no caller is left to forget the trip. `Trip` is a trait
(`fn trip(&self, Divergence) -> Option<Self::Token>`), so the comparator
stays the step-1 building block it is, with no fence, no flag and no
file in it. Its property tests use a recording implementation, and step 3
implements `Trip` for `DivergenceLatch` with `Token = Halted`. Generic
rather than `dyn`, because there is one implementation per build and the
call is monomorphized.

`verified_from` and `verified_through` (the current run) are exposed for
`/healthz` and `/metrics`, and the marker records the detecting
comparison's run (decision 11).

### 7. The primary emits from the journal stage, in the batch's ring slot

The digest frame rides in the ring slot of the batch it follows; it takes
no slot of its own. A replication ring slot is a `CHUNK_SIZE` byte buffer,
not a frame (`journal/src/replication.rs`): `try_publish` copies any byte
count up to it and records only its length and `end_sequence`. Frames are
length-prefixed. Both senders forward a slot's bytes verbatim
(`tcp_sender.rs`, `dpdk.rs`, `extend_from_slice(batch.data())`, with
byte-based budgets only). The receiver extracts frame by frame
(`process_streaming_frames`), so an `InputBatch` frame followed by a
digest frame in one slot reads exactly as two slots would.

A slot of its own, as `ChainCheck` takes, would cost a ring slot for
nearly every batch under load (one window closes per `K` entries, and
batches are wider than that). That roughly halves the time a replica can
stall before the full ring evicts it, on a ring that already evicts at
full rate (`roadmap.md`, "A replica that falls behind at full rate
cannot rejoin"). Rate-limiting separate frames would cut the cost without
removing it. Sharing the slot costs `DIGEST_FRAME_LEN` bytes per batch
instead: 46, the 42-byte frame of decision 8 and its length prefix.

`output_digest_trailer`, called inside `publish_input_batch_to_rings`
just before the publish, returns `None` without replication producers or
with digests off. Otherwise, with `end` the last sequence of the batch
being published, it picks the newest closed window whose end is at or
below `end`:

- if the board's `latest()` is at or below `end`, that entry (the response
  stage trails the journal stage, which under any ack policy is the usual
  case);
- otherwise (the window closed through the applied cursor while the journal
  stage was behind it), `get((end / K) * K)`, one division per fsync batch,
  cold; lapped means no frame this batch.

If the pick is newer than the last it forwarded, it encodes the frame of
decision 8 on the stack, as a `[u8; DIGEST_FRAME_LEN]`.
`publish_input_batch_to_rings` then appends it to the batch buffer
**after** `finalize_input_batch`, which back-fills the `InputBatch`
length from the buffer's length, so a trailer appended before it would be
swallowed into the batch. The buffer is published as one slot with the
batch's `end_seq`. The rest follows from the layout:

- **One change point.** Every batch publish goes through
  `publish_input_batch_to_rings`: the sync point in `submit_batch` (which
  `drain_remaining` also reaches) and the flush before a rotation in
  `maybe_rotate`. It is a free function over `&mut ReplicationState`, to
  avoid borrowing `self`, so the board's read handle and the last-forwarded
  end live in `ReplicationState`. The trailer is typed:
  `publish_input_batch_to_rings(repl, end_seq, trailer: Option<&[u8;
  DIGEST_FRAME_LEN]>)`.
- **Order by construction.** The digest's window ends at or below the
  slot's last entry, and the slot's bytes put it after every one of those
  entries. The receiver's rule that a digest ahead of the stream position
  is a `SequenceGap` (decision 9) therefore cannot fire on a healthy
  stream. That holds even on a handoff's partly duplicated chunk, since
  the receiver's position after the batch is at least its end.
- **The board is read slightly earlier than a post-publish call would
  read it.** `submit_batch` publishes the batch at its top, before the
  disk slot is claimed, and returns on submission, not durability. The
  read therefore moves only from after that claim to before it, with the
  same `end`. The pick rule is unchanged; the board is at worst a moment
  staler, and the next batch picks up any window that closed meanwhile.
- **Catch-up.** `RingChunk::classify` peeks only the first frame's tag, so
  a slot with a trailer is a data batch: `Covered` drops it whole, the
  digest included (a dropped digest costs delay, never coverage: the next
  one's chain value covers its windows), and `Next` and `Ahead` forward
  it whole. Journal-file catch-up carries no digests, as before (decision
  9 anchors on the first frame).
- **A batch with no entries carries nothing.** Query-only batches publish
  no slot (`publish_input_batch_to_rings` returns early). The next batch
  with entries carries the newest window. An idle primary still journals
  ticks, which take positions, so batches keep coming.
- **Eviction is unchanged.** A ring too full for the slot evicts the
  replica exactly as for the batch alone. The trailer adds no publish.

`ChainCheck` keeps its own slot: one every 64 batches is noise beside the
batch slots, and moving it is not this plan's change.

**The slot's bytes are reserved, not found.** `max_journal_batch` divides
the chunk by the full `entry_size`, while a wire slot is `entry_size - 2`
(the magic stripped). Today that leaves at least several hundred bytes
free in a full chunk, but only as a by-product of a bound its comment
calls deliberately conservative. The plan does not rely on it. The
replication chunk's share of `max_journal_batch` becomes
`(replication::CHUNK_SIZE - FRAME_HEADER_LEN - DIGEST_FRAME_LEN) /
entry_size`; the journal write ring's share is unchanged. A `const`
assertion beside `Sequencer::MAX_BATCH` checks `FRAME_HEADER_LEN +
n·(entry_size - 2) + DIGEST_FRAME_LEN <= replication::CHUNK_SIZE` for
the batch bound `n`. `MAX_DATA_FRAME` bounds one frame, not a slot, so it
is unaffected. The ring's module doc gains the slot's layout: an
`InputBatch` frame, optionally followed by one digest frame.

**Independent of the journal chain.** Not code inside
`maybe_publish_chain_check`, which returns early when
`encoder.chain_hash()` is `None` (the chain-less build) and runs on its
own batch cadence. Nothing in the digest path is behind
`cfg(feature = "hash-chain")`.

### 8. Wire: replication protocol 9

**Already on `main`: the version at authentication** (`f4971f95`,
merged 2026-11-05, ahead of this plan). `REPL_PROTOCOL_VERSION` is 9
there, and the replication auth frames are the ones this decision
specified: the replica's `ChallengeResponse` states its version (99
bytes, from 97), `AuthOk` names the session's (3 bytes, from 1), and
`MSG_VERSION_REFUSED = 0x07` answers an older replica. Both senders
share one verdict (`judge_challenge_response`, `auth.rs`: the
signature first, then `admits_replica_version`), and the replica's
`session_version_check` runs a session only at its own version.
`decode_challenge_response` returns a `ChallengeResponse` and
`decode_auth_result` an `AuthResult`. One thing this decision did not
foresee: raft's control-plane auth (`raft/src/auth.rs`) uses the same
codecs. It keeps the unversioned frames
(`encode_unversioned_challenge_response`, `encode_unversioned_auth_ok`),
since it exchanges its own version after authentication; its wire is
unchanged, and it ignores a stated replication version.

What remains of protocol 9 is the table below. Version 9 is unreleased,
so these frames ride it if they ship in the same release. If a release
cuts 9 first, they are protocol 10, and that upgrade is again a stop of
every node unless the v10 replica is written to speak 9 to a v9 primary,
uncompared and without `Halt` (see the flag-day bullet below).

| Change | Layout (after the length prefix) |
| --- | --- |
| New frame `MSG_DIGEST = 0x19`, primary to replica | `DigestFrame { tag, kind: u8, window_end: U64, window: [u8; 16], chain: [u8; 16] }`, 42 bytes |
| `Ack` gains the stop flag, replica to primary | `AckFrame { tag, acked_sequence: U64, in_memory_sequence: U64, flags: u8, window_end: U64 }`, 26 bytes, from 17. `flags` bit 0 is `STOPPED`: this replica has stopped (decision 11), and its positions are frozen at the last ones it sent. `window_end` is the position its stop names (the diverged window, or the one it was told of), 0 when `STOPPED` is clear |
| `Handshake` gains the same two fields and the replica's build, replica to primary | `HandshakeFrame { tag, last_sequence: U64, chain_hash: [u8; 32], epoch: U64, protocol_version: U16, flags: u8, window_end: U64, build_id: [u8; 32] }`, 92 bytes, from 51: a stopped replica redialling tells the primary before anything else is exchanged (decision 11), and the primary learns each replica's build, so a primary told to stop by a replica on another build can name both (open question 2). Appended after `protocol_version`, so an older primary still parses the version and refuses it |
| New frame `MSG_HALT = 0x1A`, primary to replica | `HaltFrame { tag, cause: u8, window_end: U64 }`, 10 bytes: the cluster is stopping. `cause` 1 is "this primary detected", 2 "a replica told this primary"; `window_end` as on the ack |
| `StreamStart` gains the digest kinds, the build and the pipeline id | `digest_kinds: u8` (bit `k - 1` set: this primary sends digests of kind `k`; 0: none), then `build_id: [u8; 32]` (for diagnosis only, open question 2), then `pipeline_id: [u8; 16]` (decision 3), appended after `genesis_entries`: 115 bytes, from 66 |

- **Unknown flag bits and causes are refused**, as `genesis_known` is: a
  bit this build does not define is a peer's protocol bug, never read as
  a stop or as no stop. A `window_end` that is not 0 with `STOPPED` clear
  is refused the same way. On the primary's ack read sites, which today
  skip any frame that does not decode (`drain_ack_frames`, which both
  senders' streaming loops call, and the DPDK join's `keep_last_ack`), an ack
  refused for its flags is not skipped: it takes the ack-violation path
  (evict, `error!`), since it is an authenticated member's bug exactly as
  an impossible cursor is, and a skipped one could be a stop the primary
  never sees. `decode_replica_message` returns that refusal as an error
  of its own, distinct from a frame of another kind, so a read site can
  tell the two apart; `keep_last_ack` returns it as a `JoinFrames`
  violation arm, which the `Joining` arm evicts on. The DPDK streaming
  arm already calls `drain_ack_frames` (landed: hoisted, ungated, into
  `receiver_transport.rs`, with the record, latency and persist-watch
  work in its closure and its error ending the slot's session as kernel
  TCP's does), so one helper reads every streaming ack on both
  transports, and its ungated tests cover both.
- **`Halt` cannot fall into a wildcard arm.** Every place a replica reads
  a primary message ends in a `_` arm that returns an error out of the
  receiver (what the code shows), so a `Halt` added as one more
  `PrimaryMessage` variant would make a replica that meets it in a
  handshake answer or a snapshot transfer exit with "unexpected response",
  with no marker and free to restart. `decode_primary_message` therefore
  returns `PrimaryFrame::Halt(Halt)` or `PrimaryFrame::Message(PrimaryMessage)`,
  and `Halt` is not a `PrimaryMessage`: each of the five read sites must
  match the outer enum, which has no room for a wildcard to swallow it,
  and trips the latch with `ClusterHalted { via: Halt }` before it looks
  at the message.
- **The full divergence record stays in the marker.** The wire carries
  only what lets a node stop and say who told it, plus the position for
  its log. Digests and chain values are the detecting node's evidence, in
  its marker and its `error!` line.
- **Fixed-size frames encode into arrays.** `encode_digest` returns
  `[u8; DIGEST_FRAME_LEN]`, and `encode_halt` returns
  `[u8; HALT_FRAME_LEN]`, with the length prefix included. The length is
  then part of the type: a test cannot pass with a frame one byte short,
  and the journal stage's emit cannot allocate. `encode_chain_check` and
  `encode_rotate` already have this shape (landed: they return
  `[u8; SEQ_HASH_FRAME_LEN]`, built by `zerocopy::transmute!` from a
  prefixed frame struct, their wire bytes pinned by
  `chain_check_wire_byte_pattern` and `rotate_wire_byte_pattern`), so the
  new frames follow the one pattern for fixed-size control frames. The
  other fixed-size encoders (heartbeat, ack, segment seed, snapshot
  begin and end, hash mismatch, the handshake frames) still append to a
  `Vec`; that is legacy, not the pattern to imitate. Moving them is a
  follow-up outside this plan, each a public API break with its own
  `CHANGELOG.md` entry and a wire-byte pin written first.
- **The kind byte from the start.** Only `1 = output` is defined; 0 is
  refused as malformed. Each kind fixes its own window length and
  definition, so frames carry no length. A replica skips a frame of a kind
  it does not know (counted `melin_output_frames_skipped_total{reason="kind"}`,
  `warn!` once per session), and ignores an unknown bit in `digest_kinds`
  with the same warning, so a later state digest (step 7) is a new kind
  value on the same frames, sent to replicas that already speak protocol 9,
  not another version bump. The digest fields stay 16 bytes for every kind,
  so a later kind is defined to produce 16 bytes; one that needs more (a
  tamper-evident digest) is a new frame and a version bump (open question
  1). `digest_kinds` is one byte because it names up to eight kinds, more
  than any plan foresees; a ninth would change the layout, which is a
  version bump anyway.
- **The pipeline id is sent whatever the digest kinds**, since every
  primary pipeline draws one; a replica reads it only in a compared
  session. The decoder hands it up as a `PrimaryPipelineId`, a
  `[u8; 16]` newtype that is `Copy` and `Eq` and has no other public
  constructor, distinct from the primary's own `PipelineId`: the replica
  can compare what it was sent, and nothing can turn a received id into
  one a pipeline builder takes. Any 16 bytes are valid, zero included (a
  draw can produce it).
- **The build id is for diagnosis, sent both ways.** Each node reads its
  own once at startup (open question 2) and sends it in `StreamStart` (a
  primary) and in its `Handshake` (a replica), so both ends of a session
  know both builds. The decoder hands it up as a `BuildId`, a `[u8; 32]`
  newtype, so it cannot be mistaken for the chain hash beside it in the
  handshake. Nothing compares, skips or refuses on it: it is printed in
  the session's `info!` line when the two differ (decision 16), and in
  the stop's `error!` line, marker and refusal message (decisions 11 and
  12). All zeros means the sender could not read its executable and is
  printed as an unknown build. Two builds **differ**, everywhere this
  plan says so, only when both ids are known (neither all zeros) and
  unequal: an unknown build is printed as `unknown` beside this node's,
  never with the mixed-build sentence, since the runtime does not know
  the cluster ran mixed.
- **A window end that is not a positive multiple of its kind's `K`** is the
  primary's protocol bug and ends the session as a `FrameError`, never as a
  divergence.
- **A mixed pair is still refused in both directions**, at
  authentication, as on `main` already. A v9 primary reads a v8
  replica's 97-byte `ChallengeResponse`, verifies its signature, and
  answers `VersionRefused` for a replica that names no version; the v8
  replica fails on the unknown tag ("expected AuthOk/AuthFailed, got
  0x07"), and the primary's log names both versions. A v8 primary
  ignores the appended version and answers its one-byte `AuthOk`, which
  a v9 replica reads as an older primary's (it names no version) and
  refuses with an error naming its own, sending no handshake. Should it
  get that far, the handshake's `protocol_version` keeps its offset, so
  a v8 primary still refuses a v9 handshake (the handshake's new fields
  come after it). If version 9 is still unreleased when this lands, the
  constant stays 9 and its history line on `main` gains this plan's
  frames; otherwise it becomes 10 with a line of its own. Size
  assertions for the two new frames and the grown `Ack`, `Handshake`
  and `StreamStart` (`StreamStartFrame`, 66 to 115 bytes);
  `docs/replication.md`'s frame tables gain them, and its
  `StreamStart`, `Ack` and `Handshake` rows and the frame list in
  `server-runtime/src/replication/mod.rs`'s module docs gain the new
  fields.
- **Protocol 9 is a flag day, and the version check moves into
  authentication so that it is the last one the replication protocol
  forces** (assessment 3.1, decided 2026-11-04; the check is on `main`,
  see the head of this decision). The release this lands
  in already stops every node for raft protocol 1, so protocol 9 adds no
  stop if it ships with it. Talking to an older peer would not help this
  upgrade: a v8 node accepts only v8, and raft links refuse a peer on
  another release anyway. The rule, with no ranges, each side stating
  one version:
  - **The primary accepts a replica at or above its own version** and
    refuses an older one. The replica sends its version in its
    `ChallengeResponse`; the primary, once the signature verifies,
    answers `AuthOk` carrying the session's version, which is its own,
    or `VersionRefused` carrying the same for an older replica. A newer
    replica is trusted to speak down; if it gets that wrong, the worst
    case is a frame the primary cannot decode, which ends the session.
  - **The replica decides whether it can speak the primary's version.**
    Its authentication (`authenticate_with_primary`, which the DPDK
    receiver runs too, over its polled adapter) reads the version in
    `AuthOk`: its own, and nothing
    changes; an older one, and it either speaks it (a later release's
    code) or ends the session with an error naming both versions,
    without sending a handshake. A v9 replica requires 9. On
    `VersionRefused` it errors the same way and retries as after any
    refused session.

  The primary's check on the handshake's `protocol_version` stays,
  against the session's version. A later release can then roll replicas
  first without a refused session: a v9 primary accepts a v10 replica,
  which speaks 9 to it, and after a switchover a v10 primary may accept
  its v9 replicas. Both are that release's code; the v9 side needs none.
  Rolling upgrades are not supported today; this only keeps protocol 9
  from ruling them out.
- **A rolling upgrade also needs raft links across versions; not done
  in this plan.** Raft protocol 1, unreleased on `main`, refuses a peer
  on any other version (`exchange_version_as`, `raft/src/wire.rs`), so
  a mixed cluster would have no failover and no switchover onto an
  upgraded node. The same rule fits there, both ends exchanging
  versions: accept a peer at or above one's own, refuse an older one,
  the newer side deciding whether it can speak down. Changing it before
  raft protocol 1 ships is a small edit and a test; after, it costs
  another stop of every node. It is a separate change on `main`, not a
  step of this plan, and is not scheduled yet. It would carry raft's
  version in its own exchange, as now: the replication auth frames raft
  borrows stay unversioned on raft links.

### 9. The replica compares in its receiver

- `process_streaming_frames` decodes `Digest { kind, end, window, chain }`.
  Since the primary sends a digest only once its stream has passed `end`,
  `end` above the stream position means entries are missing: a
  `FrameError::SequenceGap`, the contract of a mark ahead. A digest at or
  behind the position goes to the comparator's expected side.
- The comparator (`anchored` mode) runs on the receiver thread inside
  `streaming_loop` (one copy for kernel TCP and DPDK), but `streaming_loop`
  does not own it: `streaming_loop` runs once per session, and anything it
  built would be rebuilt at every reconnect, losing the local windows
  closed before it. The comparator is a field of `ReplicaPipelineHandles`,
  beside the live board handle and `stream_marks`, and both receivers lend
  it to `streaming_loop` by `&mut`, as they already lend `input_producer`.
  It therefore lives exactly as long as the pipeline. Both receivers build
  the pipeline only when none is held (`if pipeline.is_none()` before the
  streaming session, in `tcp_receiver.rs` and `dpdk.rs`), so the replica's
  pipeline, and with it its drain folder, live board, comparator and
  positions, survives every exit that reconnects
  (`SessionExit::Disconnected`, `StreamGap`, `Corrupted`, all
  `AfterSession::Reconnect` in `handle_session_exit`): the local chain the
  comparator extends is the same chain on both sides of the reconnect. The
  paths that rebuild the pipeline drop the comparator with it, and so its
  held id and its run: `handle_resync_verdict` (a `NeedSnapshot` or
  `HashMismatch` at session start) takes and tears the pipeline down
  before the transfer, and the `Fatal` arm's in-process repair of a chain
  divergence (found in a session, or by the journal while disconnected)
  tears it down before `AfterSession::Resync`; every other `Fatal` ends
  the receiver (decision 13's table). Its local side
  is the replica's live board: one `Acquire` load of the latest window end
  at each loop iteration and on each pass of a push waiting for ring
  space, copying every new window in order only when it moved (decision
  5). At every session start, once `StreamStart` is accepted,
  `streaming_loop` calls `begin_session` with the session's kind and
  pipeline id (decision 6), records the primary's build in the latch
  (`follow`, decision 11), and logs the verdict at `debug!`:
  the run kept (same primary pipeline) or re-anchoring (a new primary
  pipeline, the pipeline's first session, the first compared session
  after an uncompared one, an uncompared session).
- A session whose `StreamStart` has no output bit begins as
  `SessionKind::Uncompared` and feeds the comparator no expected entries;
  the session logs why once. The receiver counts nothing: the comparator
  attributes the session's windows to `uncompared` at the next `Anchored`
  (decision 6), so they are counted once. A primary that names another
  build is compared as any other (open question 2); the session logs both
  builds once at `info!` (decision 16). A replica with digests off decodes
  and drops frames.
- A `Mismatch` (the comparator has already tripped the latch, decision
  6), a `Halt` frame from the primary, or the latch found tripped by the
  replica's own shadow, ends streaming. The receiver publishes nothing
  more and, on the same session, runs decision 11's notice: it sends one
  flagged ack with the last positions it sent, then waits for the
  primary's `Halt` or its close, within the notice bound. The session then
  ends with a new `SessionExit::Stopped(HaltCause)`. Both receivers hand
  every exit to the one shared `handle_session_exit`
  (`replication/mod.rs:958`), whose `match exit` is exhaustive, so the
  new variant cannot compile without its arm there. That arm tears the
  pipeline down and returns `AfterSession::Return` with the stop's error,
  which `AfterSession::adopt` passes through to `run_receiver*`'s caller
  on both transports, so it never reaches a reconnect-loop top, where a
  promotion request could be honoured: it returns to the exit helper,
  which runs the linger (decision 11). `process_streaming_frames`
  decodes `Halt` beside `Digest`, and a `Halt` trips the latch with
  cause `ClusterHalted`.

### 10. The in-process check: shadow against live, on every node with a shadow

The shadow folds with the same `WindowFolder` on the same grid from the same
pipeline start, and runs a `DigestComparator` in `shared_start` mode whose
local side is its own windows, pushed as it closes them, and whose expected
side is the node's live board (the response stage's on a primary, the
drain's on a replica), polled with `latest()` between batches. The shadow
runs the comparator itself: it is off the ack path and already does cold
work between batches. Either may lead (the shadow trails the journal; the
live folder on a primary trails the gate), so an entry can wait in the
comparator for the other; a live window lapped before the shadow polled it
is a skipped sample, and the next poll's covers it. A `Mismatch` stops the
node and, through it, the cluster (decision 11), whether it is a primary
or a replica.

This is the only check on a node without replicas, and it catches what the
replica check cannot see on a primary: a shadow built by
`clone_via_snapshot` that answers differently, that is, a `snapshot`/`restore`
pair that drops a field, which is exactly the copy a restart or a resync
would come back from. It does so only for state present when the shadow
was cloned, once per pipeline start, and only through the default
`clone_via_snapshot`: an application that overrides it with a direct clone
(its rustdoc allows "a cheaper same-process clone") gives the shadow no
round trip, and the check then sees no snapshot fault at all. Both are
stated as a limit ("The guarantee, and its limits"; assessment 1.6). A node without a shadow (`--snapshot-interval-ms 0`)
has no in-process check, and a standalone node without one has no check at
all; both are logged at startup and documented as limits.

### 11. On divergence: the cluster stops, in one order

A node stops for one of two causes, `HaltCause` (exhaustive):
`Detected { divergence, peer_build }`, when one of its own comparisons
finds it (a replica's receiver, or any node's shadow), and `ClusterHalted
{ by, via, window_end, peer_build }`, when another node tells it (`by`
the primary or a replica's slot and key fingerprint, `via` the ack, the
handshake or the halt frame, `peer_build` the teller's `Option<BuildId>`,
from the session's `StreamStart` or the replica's handshake, `None` when
no session told it, as for a `Halt` answering a handshake). That `None`
is deliberate: the `Halt` comes before any `StreamStart`, so the teller
has not named its build, and the build the latch followed last may be
another node's (a primary since replaced). Taking it would name a build
the teller may not run, and a wrong mixed-build claim misleads more than
a missing one. The marker of such a stop names this node's build alone.

A `Detected` stop's `peer_build` is the build of the primary the node
follows, whichever of its checks detected. On a replica, the receiver
and the shadow can both catch one fault, and which trips first is a race
(the shadow trails live by its lag only; the receiver waits for the
primary's frame for that window or a later one). If the build came from
the detecting check, a mixed-build replica would name the primary's build
or not depending on that race. So the build is not the check's: each
receiver records its session's primary build in the latch at
`StreamStart` (`DivergenceLatch::follow(BuildId)`, compared and
uncompared sessions alike), and the latch stamps it on every `Detected`
stop it builds. `Trip::trip` takes only the `Divergence`, so no trip site
supplies the build or can forget it. The record is a `Mutex<Option<BuildId>>`
in the latch: 32 bytes are no single atomic, and it is touched once per
session and once at the trip, both cold. It is `None` on a primary, on a
replica before its first session, and after a promotion: the promotion
teardown that reads the latch (item 1 below) clears it before handing the
application over, so a promoted node's own shadow names no stale build.
Between two sessions it keeps the last one's, the primary whose entries
the shadow is still applying.
The stop is honoured whatever the node's own `--no-output-digests`: that
flag governs what a node computes, not whether it obeys a stop. Either
way, the node:

1. **Halts replies at once.** `DivergenceLatch::trip(cause)` latches the
   node's `FenceState` with a new stop cause and co-sets the shutdown flag,
   the way `fence_if_superseded` owns "fenced ⇒ shutting down". It is the
   **node's** shutdown flag, the one the replica's receiver and the
   primary's readers watch, never a pipeline's own. On a replica the
   shadow runs on `pipeline_shutdown` (`replication/mod.rs`, the shadow
   spawn), which only the receiver's teardown sets. A latch built from
   that flag would stop the pipeline's stages while the receiver ran on
   against a ring nobody drains. So the latch is built once per node,
   with the node's `FenceState` and shutdown flag, and handed to every
   pipeline builder. The replica's shadow takes the latch, not a flag, so
   it cannot pick the wrong one. The receiver's waiting push, the
   `accept_slot` abort closure that today checks `journal_failed`, also
   checks the latch, at both of its call sites: the streaming path
   (`receiver_transport.rs:545`) and the promotion drain,
   `process_drain_frames` (`receiver_transport.rs:800`). A shadow that
   trips stops consuming the input ring, so without that check the
   receiver would wedge in a push on a full ring and never reach the loop
   top that reads the shutdown flag, and a drain that a promotion started
   at the moment of the trip would wait forever, its node never reaching
   the exit helper or the notice. On the streaming path an abort leaves
   the frame's slots pushed before it as today's `journal_failed` abort
   does: the `if frame_published` block after the abort's `break`
   (`receiver_transport.rs:572-584`) commits them and queues their ack
   target, and nothing after them is pushed. Harmless after a stop: the
   stopped loop top skips `drain_pending_acks`, so that target is never
   acknowledged, and the flagged ack carries the positions last sent, not
   it; and the marker refuses the restart that would replay them. Kept
   rather than special-cased, so the abort has one shape whatever its
   cause. The drain's `Err` arm already stops the
   drain at the last contiguous slot; its `warn!` names the cause it read
   (the journal stage's failure, or the stop) rather than always the
   journal. What follows the drain changes with it. Today the promotion
   branch runs `drain_pending_acks` after the drain
   (`receiver_transport.rs:1025`) and then `break SessionExit::Promote`
   (`:1034`), whose arm in `handle_session_exit`
   (`replication/mod.rs:994`) hands the application to the entry to run
   as primary. A drain that ended with the latch set, whether its abort
   read the latch or the shadow tripped while it ran, does neither: it
   skips `drain_pending_acks`, as the stopped loop top does, so no
   ordinary ack goes out after the stop, and leaves as the stopped loop
   top does, through the notice (the flagged ack, the wait for `Halt`)
   and `SessionExit::Stopped`, never `Promote`. That reading, once after
   the drain loop and before `drain_pending_acks`, is only the early exit:
   it sees a trip by the receiver's own abort, or by the shadow on entries
   it applied before the reading. It cannot see the rest. The shadow trips
   on its own thread as it applies entries, and it trails the receiver, so
   the entries the drain itself pushed are usually applied after the
   reading: while `drain_pending_acks` waits on the journal cursor
   (`pop_all_blocking`, `receiver_transport.rs:1170`), or during the
   teardown, which publishes the sentinel and joins the shadow only after
   the session has broken with `Promote` (`teardown_replica_pipeline`,
   `replication/mod.rs:742-772`, reached from the `Promote` arm at `:994`). A divergent window closing
   on one of the drained frames is the common case, not a corner. So the
   guarantee is a second reading, in `handle_session_exit`'s `Promote` arm,
   after `teardown_replica_pipeline` has returned: every thread of the
   replica pipeline is joined then (`shutdown_pipeline`,
   `replication/mod.rs:351` on), so no thread that could trip for the old
   pipeline is left, and the receiver is the caller. Set, the arm does not
   hand the application over (`Ok(Some(..))`, `:996`): it returns as the
   `Stopped` arm does, and the exit helper finds the stop in the latch.
   The other promotion, at each receiver's reconnect-loop top
   (`tcp_receiver.rs:691`, `dpdk.rs:1713`), has the same gap: it checks the
   shutdown flag, then `take_pipeline_for_promotion` tears down a pipeline
   whose shadow may still be applying the last session's entries
   (`replication/mod.rs:1141-1142`). So the post-join reading is one
   function, a promotion teardown that joins the pipeline, reads the latch
   and returns the application only when it is clear (clearing the
   followed build as it does, above), and both the
   `Promote` arm and `take_pipeline_for_promotion` go through it, so
   neither can hand over a stopped node's application.
   The session's link is already closed (`close()`, `:975`, runs before the
   `match`), so no flagged ack can go out on it; the marker's `notice`
   stays `pending`, and the exit helper's replica linger redials
   `--replica-of` with a flagged handshake (below), as for a replica whose
   in-session notice went unanswered. An ordinary ack that
   `drain_pending_acks` sent before the shadow's trip is an ack sent before
   the stop, like any ack the receiver sends between a trip and its next
   reading: it carries journal positions, not a verdict, and the flagged
   handshake follows it. The shadow leaves on the pipeline flag the
   teardown sets (`shadow.rs:107`), not at the sentinel, so the entries it
   had not applied by then are never compared by the old pipeline's shadow,
   and the promoted node's shadow is a new clone of the live application
   (`server.rs:1779`) starting a new run: that tail is unverified (the
   limits, "The tail at failover"), not a stop missed. With both readings,
   a stopped replica never becomes a primary that told nobody. The latch
   keeps the first cause only; a second trip returns `None`. From the
   latch on: the readers close every client (`Verdict::Close`), the
   response loop hands no slot and no early reply to the egress (a per-slot
   `Relaxed` load of the latch, before the send, decision 4), the shadow
   saves no further snapshot (checked before every save), auto-promotion
   refuses on `fenced`, and the raft driver leaves on the shutdown flag, so
   the node drops out of the control plane (finding 6).
2. **Logs `error!`.** For `Detected`: the check (`replica-vs-primary` or
   `shadow-vs-live`), the range known to hold the divergence, whether the
   window at its end differs itself, both chain values' and window digests'
   prefixes (`hex_prefix`, the journal crate's formatter, widened from
   `&[u8; 32]` to `&[u8]` so 16-byte values and the 32-byte chain hash
   print alike), and the run. For `ClusterHalted`: who told it, by which
   path, and the position named. Both name this node's build and, when the
   other node's (`peer_build`) is known and differs (decision 8, the
   build id), that one too, with "the cluster runs mixed builds"; an
   unknown one is printed as `unknown`, without that sentence. Both end "the
   cluster is stopping: see docs/replication.md, Output verification".
3. **Writes the divergence marker** beside the journal
   (`NodeStore::divergence_marker()`, `<journal>.diverged`), written to a
   temporary file, fsynced, renamed and the directory fsynced, so it
   survives a crash. Plain text, for an operator: the format version, the
   cause (`detected` with the check, or `cluster-halted` with `by` and
   `via`), the window end, and for `detected` the range known to hold the
   divergence, whether that window differs itself, both window digests and
   both chain values in full hex, and the run (`verified_from`,
   `verified_through`); then `notice` (`pending`, `delivered` or `none`,
   rewritten as the notice of item 4 progresses), the node's build id and
   the other node's when known (`unknown` for an all-zero id), and
   the wall-clock time (for people; nothing reads it).
4. **Tells the rest of the cluster, then lingers.** What it does depends
   on its role (below, "The notice and the linger"). In short: a replica
   flags its acks and, if no session is open, redials with a flagged
   handshake; a primary sends `Halt` to every connected replica and keeps
   its replication endpoint answering `Halt`. Every stopped node keeps
   doing so for `HALT_LINGER` after its pipeline is down, unless it knows
   the rest are told, then exits.
5. **Moves no file.** Every snapshot the broken build wrote, on every node
   and below any verified position too, is suspect: state can diverge
   before a report shows it, and a fixed build may not even restore it. So
   nothing is archived here; the new-cluster procedure sets every node's
   snapshots aside (step 6). The journal is left in place: it is
   input-only and the chain vouches for it.
6. **Exits with an error.** The node's run returns
   `Err(NodeDiverged { .. })`, through one helper every node entry
   (`run_tcp`, `run_dpdk`, so `run`, `run_with_shutdown` and
   `run_with_listener`) returns through, which the binaries turn into a
   non-zero exit. A return rather than `process::exit`, because a test or a
   host may run several nodes in one process.

**The order is enforced by types, not by a comment.** `trip` returns a
`Halted` token (private constructor, neither `Clone` nor `Copy`), the
marker writer consumes it and returns a `MarkerOutcome` (`Written`, the
I/O error, or `Unconfirmed` when the write did not settle in time), and
the notice and the linger take that. Nothing can write a marker, or tell
another node, before the latch is set. Nothing tells another node before
the marker write has settled or `HALT_NOTICE_BOUND` has passed. In every
case but a hung disk, a crash just after telling the others then leaves
a node that refuses to restart, rather than one that rejoins the cluster
it has just stopped. A failed or unconfirmed write still lets the notice
run (below). The halt itself does not rest
on the caller: the comparator trips
before it returns a `Mismatch` (decision 6), so a detection site that
ignored the verdict would still have halted the node's replies and set
its shutdown flag. Writing the marker does rest on the detecting thread,
and the types alone cannot force it. A dropped `Halted` token compiles,
and `#[must_use]` on it is only a lint. So the token's `Drop` records
that it was abandoned, and whichever of the notice or the exit helper
reaches the latch first writes the marker (below).

**Which thread does what.** The steps span threads: detection is on the
shadow thread (primary or replica) or the replica's receiver thread, a
`ClusterHalted` trip on a replica's receiver or on any of a primary's
senders, while the linger must wait until every pipeline thread has been
joined. So:

- On the **tripping thread** (a comparator's for `Detected`, the receiver
  or a sender for `ClusterHalted`), the trip latches. The thread logs and
  hands the `Halted` token to a short-lived `divergence-marker` thread
  (one spawn per node lifetime, on the cold path), which consumes the
  token, writes the marker and deposits the `MarkerOutcome`. No
  link-owning thread ever enters the write. At three of the four trip
  sites the tripping thread *is* a link owner: the replica's receiver for
  replica-vs-primary and for `ClusterHalted`, a primary sender for
  `ClusterHalted`, and on DPDK the poll thread for every `ClusterHalted`
  (a flagged ack through `record_ack`, `dpdk.rs:1341`, or a flagged
  handshake, `dpdk.rs:1033`). A link owner stuck inside `fsync(2)` could
  not be released by any bound on a wait. Writing beside the links also
  means the DPDK poll thread never stalls on the write, and the pipeline
  joins never wait on a marker fsync, so the exit helper and the linger
  always run. Writing the marker before any join means a hang in the
  shutdown leaves the node refusing to restart all the same. If the
  thread cannot be spawned, the tripping thread writes the marker
  itself, as the one fallback, and logs `error!`. The outcome goes to the
  `DivergenceLatch`, which is one `Arc` per
  node shared by every trip site, the notice and the exit helper. It
  lives in `transport-core` beside `FenceState`, so `ReplicaCursors` (the
  same crate) can hold it. Its marker state is a `Mutex` around an
  exhaustive enum, touched only on this cold path: `Clear`, `Pending`
  (tripped, the token alive), `Abandoned` (the token dropped unconsumed),
  `Claimed` (a waiter writing in the token's place), `Written` or
  `Failed(io::Error)`. `Halted`'s `Drop` moves `Pending` to `Abandoned`
  when the token goes unconsumed, and does no I/O itself. A `Condvar`
  beside the `Mutex` wakes the kernel transport's blocked waiters on every
  change; the DPDK poll thread never blocks on it, and reads the state
  once per poll iteration instead.
- The **notice in the session** runs on the replica's receiver and on the
  primary's senders, the threads that own the links. Each waits up to
  `HALT_NOTICE_BOUND` for the latch to leave `Pending` and `Claimed`
  before sending its flagged ack or its `Halt`. A small fsync lands in
  milliseconds, so in every ordinary stop the marker is down first. Past
  the bound, the waiter logs `error!` ("marker not confirmed within the
  notice bound; telling the cluster without it") and sends its notice
  anyway. A marker fsync that outlasts the bound is a hung disk, and a
  notice held for it would turn the eventual kill into a failover onto
  the broken build, with no marker anywhere and no `Halt` sent (open
  question 7). A late write is harmless: the state moves to `Written`
  whenever it lands, and the exit helper reads it once more after the
  linger. The DPDK poll thread keeps polling its transport while it
  waits. A waiter that finds `Abandoned` claims the write (`Abandoned` to
  `Claimed` under the lock, so exactly one claimer writes) and hands it to
  a `divergence-marker` thread as the trip does, from the cause the latch
  kept. A token dropped by a bug then cannot leave the link-owning threads
  waiting, and the claimer itself never enters the write. Claims are taken
  only on `Abandoned`, never on `Pending`, so there is never a second
  writer. The claim is a guard: depositing the outcome consumes it, and
  its `Drop` moves `Claimed` back to `Abandoned`, so a writer that unwinds
  mid-write leaves the write to the next claimer.
- The **exit helper** is the single return path of every node entry, after
  every pipeline thread (and, on a replica, the receiver) has been joined,
  whichever way they left. It claims the latch's contents: nothing there is
  an ordinary exit; a stop whose marker is written runs the linger and returns
  `NodeDiverged`; a stop left `Abandoned` (a detecting thread that
  dropped its token, or unwound before writing, in a build that unwinds)
  has its marker handed to a writer here, by the same claim, and then goes
  on as either case below. `Pending` or `Claimed` found here means a
  writer still in its fsync (a hung disk), or a leaked token or claim
  guard (a `mem::forget`, since a dropped one returns the state to
  `Abandoned`). The helper cannot tell them apart and does not need to: it
  waits one `HALT_NOTICE_BOUND`, then runs the linger, reads the state
  once more, and returns `NodeDiverged` with `MarkerOutcome::Unconfirmed`
  if it still has not settled, logged at `error!`. It never blocks on the
  write. The helper's `match` names all six states, so a seventh must
  decide too. A stop whose marker write failed, or stayed unconfirmed,
  still runs the linger, then returns `NodeDiverged` carrying the I/O
  error or `Unconfirmed`, logged at `error!`, so the operator learns the
  refusal on restart is not guaranteed. The linger
  runs without a marker because only the running process holds the stop
  then: telling the others is what keeps the cluster down when this node
  might come back. The decision is the latch's, not the exit path's: a
  replica receiver that sees only the shutdown flag outside a session (in
  its backoff, or mid-handshake) after its shadow tripped, and leaves as
  on an ordinary shutdown, still returns through the helper, which finds
  the stop and lingers. No exit path can skip the
  claim, because the helper is the only way to return from the entry.

**The notice and the linger.** `HALT_LINGER` is `2 × MAX_BACKOFF`, 60 s: a
replica in reconnect backoff redials within `MAX_BACKOFF`, so a primary that
lingers twice that long meets every replica that was between sessions when
it stopped, with a full backoff's margin. A constant, not a flag: the value
follows from the backoff, and an operator has nothing to tune it against.

The notice bound, `HALT_NOTICE_BOUND`, is a second constant, 5 s, and
not derived from `replication_heartbeat_secs` (`server.rs:318`, a flag,
default 5). It bounds a wait on a peer that is connected and was just
sent a frame (the replica waiting for `Halt` after its flagged ack, a
sender's write and drain after its `Halt`), which a live peer answers
within a round trip and a poll, not on its heartbeat cadence. It also
bounds the link owners' wait for the marker (above), which a healthy disk
settles in milliseconds: the journal sets no fsync deadline of its own to
derive one from. Tied to
the heartbeat, an operator's large heartbeat would let these waits eat
most or all of the linger. The kernel slots drain on their own handler
threads and DPDK's `close_all_links` polls every slot at once, so the
primary's teardown spends about one bound, not one per slot, before its
endpoint lingers; a replica spends one before it redials. The relation
is pinned at compile time, beside both constants:

```rust
const _: () = assert!(5 * HALT_NOTICE_BOUND.as_secs() <= HALT_LINGER.as_secs());
```

(`Duration::as_secs` is `const`, so no second pair of constants), so a
later change to either cannot leave the notice waits a large share of
the linger unnoticed.

By role:

- **A replica that stopped.** `streaming_loop`'s loop top reads the latch
  before its shutdown arm: a stopped replica must not run
  `drain_pending_acks` (`receiver_transport.rs:977`), which would wait on
  its journal cursor and then send an ordinary ack after the stop. On its
  current session it sends one ack with `STOPPED` set, its positions
  frozen at the last ones it sent (`last_sent_acked_seq` and
  `last_sent_in_memory_seq`), and waits up to the notice bound
  (`HALT_NOTICE_BOUND`, above) for the primary's `Halt`
  or its close, reading and discarding every other frame meanwhile and
  publishing nothing. A `Halt` received means the primary knows: the
  marker's `notice` becomes `delivered`, and the replica exits without
  lingering. Otherwise, after the pipeline is down, it redials its
  `--replica-of` with `STOPPED` in its handshake until an answer arrives (a
  `Halt`, or the primary closing after reading it) or `HALT_LINGER` ends.
  The redial is the exit helper's, over the receiver's own dial,
  authentication and handshake code (factored out of each receiver's
  reconnect loop, not copied), with the transport the entry still holds: a
  kernel socket, or on DPDK the transport `run_receiver_dpdk` borrows
  (`dpdk.rs:1553`). The factored dial takes the linger's absolute
  deadline. Today's kernel dial is a bare `TcpStream::connect`
  (`tcp_receiver.rs:698`), which against a primary whose SYNs are dropped
  rather than reset (a host down behind a switch, a firewall) blocks for
  the kernel's SYN retries, longer than `HALT_LINGER` itself; the factored
  one uses `TcpStream::connect_timeout` with the time left before the
  deadline. A per-call timeout does not bound what follows: `read_frame`'s
  `read_exact` loops over reads (`protocol.rs:532-543`), so a listener at
  `--replica-of` that drips its challenge a byte per read timeout (a
  mis-pointed address, a stale host: the replica authenticates to it, not
  the reverse) would hold the redial past the linger. So the exchange runs
  through the same deadline `Read` adapter as the primary's kernel linger
  (below), one type in `server-runtime` used by both sides, not two. Writes
  need a bound added: today's kernel receiver sets a 5 s read timeout on
  the stream (`tcp_receiver.rs:730`) and no write timeout at all, so the
  factored dial sets the write timeout to the time left, and no redial
  outlasts the linger. The dial checks
  the deadline before each of those calls and returns its timeout
  outcome when nothing is left: `connect_timeout` returns `InvalidInput`
  for a zero `Duration`, and `set_read_timeout` and `set_write_timeout`
  refuse `Some(Duration::ZERO)`, so a remainder that has run out (or
  rounds to zero) must never reach them. On DPDK the dial and the
  handshake are polled, and the linger loop checks the deadline on every
  pass. The reconnect loop calls the same dial with no deadline beyond
  its own, so its behaviour is unchanged. A replica stopped by a `Halt`
  tells nobody: the primary already knows, and its `notice` is `none`. A
  `Halt` is
  recognised wherever a replica reads a primary message, the handshake's
  answer and a snapshot transfer included (decision 8), so a replica that
  was between sessions, or mid-transfer, stops with its marker rather than
  exiting on an "unexpected response".
- **A primary that stopped.** Every replication sender, on both transports,
  sends `Halt` on its slot and closes it at a frame boundary on every exit
  path once the latch is set, and closes it so that a reset cannot destroy
  the frame before the replica reads it: the replica's acks, left unread,
  make a close send a reset, and whether a peer stack still delivers
  bytes it received before a reset is the stack's choice, not one the stop
  should rest on. So:
  - *Kernel TCP, live.* `live_stream_uring` sends the `Halt` through its
    ring once the send in flight completes (a whole ring chunk, so at a
    frame boundary), then shuts its write side only and keeps its `RECV`
    reaping, and discarding, acks until the replica's end of stream or the
    notice bound, and only then lets `InflightUring`'s two-way shutdown and
    drain run (`uring_teardown.rs:49`).
  - *Kernel TCP, every other path* (authenticated but before streaming,
    catch-up, snapshot transfer, bridge): these return to
    `run_replica_slot` (`tcp_sender.rs:428`), the one exit of every
    handler, which writes the `Halt` on a clone of the stream with a write
    timeout of the notice bound (a replica that has stopped reading must
    not hold the handler, and with it the join), half-closes, and reads
    with the same bound until end of stream. Today `run_replica_slot`
    moves its stream into `handle_replica_connection` (`:433`) and keeps
    no handle, so it takes the clone before the call. Since every handler,
    the live loop's included, returns through it, the handler says how it
    ended rather than leaving `run_replica_slot` to guess:
    `handle_replica_connection` returns a `SlotEnd` (`Unauthenticated`,
    `HaltSent`, `Open`, or `Broken` for a path that failed mid-frame)
    alongside its result, and `run_replica_slot` writes only on `Open`.
    The live loop, which sent its own and shut its write side, returns
    `HaltSent`, so no second write meets a shut socket and is logged as a
    connection error; a peer that never authenticated is never told; and
    a `Broken` path has no frame boundary to write at, so its replica
    meets the halt at its next handshake. An exhaustive `match` on the
    enum makes a new exit path decide which it is.
  - *DPDK.* `close_all_links` (`dpdk.rs:1526`), the poll loop's last act,
    queues `Halt` on every authenticated slot and closes each (a FIN after
    the frame) instead of `linger_then_reset`'s reset, polling until each
    replica has closed or the notice bound passes; only then are the
    remaining links reset.

  Then, after the pipeline is down, the primary keeps its replication
  endpoint answering every authenticated handshake with `Halt`, through
  each transport's existing authentication and handshake decode, hoisted
  rather than copied. On kernel TCP the exit helper accepts on the
  returned listener. The listener is non-blocking by construction
  (`ReplicationListener::new`, `server.rs:1428`), so the loop waits for a
  connection with `poll(2)` on its descriptor, with the linger's remaining
  time as the timeout, rather than spinning on `WouldBlock` or switching
  the listener to blocking, and leaves when the poll times out at the
  deadline. On each accepted stream it runs the sequence
  `handle_replica_connection` opens with (`authenticate_replica`,
  `auth.rs:130`, then the handshake's `read_frame` and
  `decode_replica_message`, `tcp_sender.rs:476-484`), hoisted into one
  function both call. A per-read timeout is not enough here: the
  handler's 10 s (`set_read_timeout`, `:470`) bounds each `read`, and
  `read_frame`'s `read_exact` (`protocol.rs:532-543`) loops over reads,
  so a peer dripping one byte per timeout holds a thread for most of
  `HALT_LINGER`, and two such peers would hold every linger thread
  while a real replica goes untold. So the linger reads through a small
  `Read` adapter over the stream that carries an absolute deadline, one
  notice bound for the whole authenticate-and-handshake exchange, sets
  the socket's read timeout to the time remaining before each read, and
  returns `TimedOut` once it has passed. The DPDK linger bounds a
  connection the same way, with one absolute deadline of its own (below),
  not the sender's `AuthChallenge` deadline, which is `AUTH_TIMEOUT`,
  10 s (`dpdk.rs:319`), covers only the challenge, and is cleared once
  the peer authenticates (`dpdk.rs:889`). Each stream runs on a
  short-lived thread of its own, at most `SLOTS` at once as today's
  acceptor spawns one handler per slot (`:278-362`), so a silent or
  dripping peer holds one of them for one bound at most and never the
  accept loop. On DPDK there is no blocking path to
  reuse: the primary authenticates only inside the sender's poll loop
  (`accept_connection` issuing the challenge, `dpdk.rs:778-805`, and the
  `Authenticating` arm's `step_authentication`, `:865-899`, `auth.rs:277`),
  and `close_all_links` has just ended that loop. So the linger drives a
  poll loop of its own over the returned transport: the challenge issue,
  hoisted out of `accept_connection` into a function both call, then
  `step_authentication` per connection, then the handshake frame's
  decode and the queued `Halt`. Its per-connection state is a small
  linger-only struct (handle, buffers, challenge, phase), not
  `SlotSender`, which carries rings and cursors a stopped primary no
  longer has. The struct carries one absolute deadline, set at accept to
  now plus `HALT_NOTICE_BOUND`, which it hands to `step_authentication`
  as the challenge's deadline and which the loop checks on every poll in
  each phase, the handshaking one included, resetting the connection
  once it has passed. The sender's bounds do not carry over: its
  `AuthChallenge` deadline ends at authentication, and its handshaking
  phase after it is bounded only by `PeerLiveness::REPLICATION`, whose
  probes a peer that is alive but silent answers
  (`peer_liveness.rs:360-363`), so without the linger's own deadline an
  authenticated peer that never sends its handshake would hold a linger
  connection for all of `HALT_LINGER`. The loop holds at most `SLOTS`
  linger connections at once, in a fixed array as the sender's
  `accept_connection` holds its slots (`dpdk.rs:757-765`), and resets a
  connection that arrives with every one in use, as the sender does, so
  a silent or dripping peer holds one entry for one bound at most, the
  same limit as the kernel side's threads. Nothing outlives
  the pipeline today (what the code shows), so the endpoint is handed back
  to the exit helper: on kernel TCP the sender thread returns the
  `ReplicationListener` it was given, so connections that arrive during
  the teardown wait in its backlog rather than being refused, and a
  replica's backoff is not escalated by a refusal; on DPDK
  `run_as_primary_dpdk` returns its transport, whose replication listener
  is still open, and the EAL and port stay with the entry's `DpdkShared`.
  The primary's health endpoint gets a stop flag of its own, as the
  replica's has, raised by the exit helper after the linger rather than
  by the pipeline's shutdown, so `/healthz` and `/metrics` stay readable
  throughout. Its handle therefore leaves `PipelineHandles`: today
  `stop_and_join` joins it with the pipeline threads (`server.rs:1658`),
  and with a flag only the exit helper raises, which runs after that
  join, the join would never return. The entry holds it instead, in the
  replica's `ReplicaHealthGuard` (`raft.rs:285`), reused rather than
  copied (and renamed `HealthGuard`, now that both roles hold one),
  whose `stop` raises the flag and joins and whose `Drop` does the same.
  On an ordinary shutdown, with no stop latched, the entry stops the
  guard as soon as `stop_and_join` returns, so a clean exit is as prompt
  as today; an error return out of the entry drops it. The endpoint
  answers until `HALT_LINGER` ends or the replicas told, counted by
  distinct key fingerprint, in session or by handshake, reach
  `ReplicaSlotCursors::SLOTS`, whichever is first.
  Counting distinct keys rather than sessions keeps a replica told twice
  (its session cut after the first `Halt` was sent but before it was read)
  from ending the linger while the other is still between sessions; two
  replicas sharing a key make the linger run its full length, which costs
  time, never coverage. It never streams, never answers clients and never
  starts raft in that time.
- **A primary told by a replica.** The flag trips the latch with cause
  `ClusterHalted`, at the first place each transport sees it:
  - `ReplicaCursors::record_ack`, which holds the node's latch, checks
    `STOPPED` before its sent high-water check and before it stores, so a
    flagged ack can neither be lost to an eviction nor move a cursor. It
    returns `Result<AckRecorded, AckViolation>`, with
    `AckRecorded::Stored` or `AckRecorded::Stopped(Option<Halted>)`, so
    neither caller can treat a stop as an ordinary ack, and the one that
    tripped first holds the token for the marker.
  - `keep_last_ack`, which today only keeps the newest ack for the
    streaming arm to record after the join, returns
    `JoinFrames::Stopped { window_end }` when any ack it reads carries the
    flag, and the `Joining` arm trips at once: a stopped replica stops
    reading its join, so a flag held for the streaming arm might never be
    recorded.
  - The handshake, after authentication and before the epoch and chain
    checks (`tcp_sender.rs:480`; `dpdk.rs:1033`), so a stopped replica is
    never sent a snapshot.

  The primary then does all of the above, so the other replica is told.

**Why this stops the cluster.** Finding 6 gives the argument: with at most
three voters (decision 14), two stopped nodes leave no quorum. In every
connected case, the stop reaches a second node: a replica that detects
reaches the primary; a primary that detects, or is told, reaches its
connected replicas; and the linger extends "connected" from the moment of
detection to the minute after it. A node that missed both is alone against
two stopped nodes, so no election promotes it. What remains is the
partition of "The guarantee, and its limits".

What "at once" means, precisely: no slot is handed to a transport after the
response stage has observed the latch. Bytes handed on before it (a kernel
send buffer, a DPDK TX ring) may still leave. Detection itself trails the
diverged window (it must close and reach the other copy), so the replies of
that window have gone out by the time anyone knows; what the stop
guarantees is that nothing further does.

No automatic repair, no retry and no fault attribution: a divergence means
the application's determinism contract is broken, or that the builds the
cluster runs side by side do not agree; either way the build or builds in
place produced it, a restart from the same journal on them has no reason
to behave differently, and an outage of the whole cluster is the intended
outcome. On mixed builds, which copy is "right" is not a question the
runtime can answer: the markers name both builds, and the operator
decides which build the new cluster runs (step 6).

### 12. Startup refuses while the marker exists

A new `NodeStore::refuse_if_diverged(&self) -> Result<(), NodeStoreError>`,
which every node entry calls right after `resolve` and before it opens its
journal, starts raft or serves anything, returns
`NodeStoreError::Diverged { marker, cause, window_end, builds, stopped_at }`
if the marker exists, every field parsed from the marker (`cause` the
marker's cause, `detected` with its check and range or `cluster-halted`
with who told it, `builds` this node's build and the other node's when
the marker names it, `stopped_at` its wall-clock time), so the message below
quotes the marker and nothing else. `resolve` stays as it
is, writing nothing, so a test or tool that resolves a diverged node's
paths is neither refused nor moves files.
Both entries resolve through `ServerConfig::store()` today (`run_tcp` and
`run_dpdk`, `server.rs`), which tests also call. They move to a new
`ServerConfig::startup_store()`, resolve then `refuse_if_diverged`, the one
place the refusal is made, so a third entry takes it by calling the same
function; `store()` stays write-free for tests. The error says what
happened and what to do:

> this node stopped on `<time>` because the cluster's output diverged
> (`<detected here: its output differs from the primary's | its shadow's,
> between positions <a> and <b>>` or `<told by <who> via <path>, at
> position <n>>`; details in `<marker>`). [This node ran build `<a>`,
> the other node build `<b>`: the cluster ran mixed builds, whose output
> differs for the same input.] The application broke its determinism contract,
> or its builds do not agree, so this cluster cannot be restarted: do not
> delete this marker to bring this node back. Start a new cluster from the
> journal, on one build, with a fixed application or with output digests
> off. See docs/replication.md, "Output verification".

The bracketed sentence appears only when the marker names two builds
that differ, both known (decision 8): a marker naming an `unknown` other
build carries it as such, without the sentence. A marker that cannot be parsed still refuses, naming the
file.

So a stopped node cannot come back on its own (systemd's `Restart=always`
just logs the refusal at each attempt), never rejoins raft, and cannot be
elected. Neither can the cluster: once two nodes refuse, there is no
quorum (finding 6). The marker is moved aside, not deleted, as part of
the new-cluster procedure, where it is the incident's record (step 6).

### 13. Snapshots, recovery, failover, rejoin

| Situation | Behaviour |
| --- | --- |
| Restart, recovery from snapshot + journal | Replay is not folded. Live and shadow start new chains at the first full window after the recovered position; the shadow check covers everything from there. The restarted primary's pipeline draws a new id, even at the same address with the same identity, journal and epoch, so its replicas' next session re-anchors on its first frame rather than comparing it against the old chain, and never reports a mismatch for the restart: the windows from their last run's end to that anchor are unverified (`new_primary`). From the anchor on, every primary restart is a check of the output its *replayed* state produces against the replicas' *live* history. |
| Replica booting from its own snapshot | A new replica pipeline, so a new comparator with no run and no held id: it anchors on its first accepted frame. A `snapshot`/`restore` pair that drops a field shows at the first report it changes, in the shadow check from the first window and in the primary check from the first anchor. |
| Replica catching up from the primary's journal | The disk walk carries no digests; ring chunks holding digests are classified like any control frame. After a reconnect to the same primary pipeline the run is kept, so the first frame after the catch-up verifies every window the catch-up applied, unless the replica's board lapped during it (decision 5: a chain break, counted). After the replica's own start its windows before its first accepted frame are unverified (`start`); its shadow check covers them. |
| Snapshot resync (chain divergence, pruned history) | Pipeline rebuilt: new folders, new boards, new comparators, starting at the next full window. The new comparator holds no id, so the run ends even when the primary's pipeline is the same, and the next frame anchors (`start`). |
| Promotion | The warm application moves into a new primary pipeline with new folders, a new chain from the seed and a new pipeline id; its replicas re-anchor on its first frame (`new_primary`). |
| Ex-primary rejoining | A forked journal is caught by the chain check first and resynced. Otherwise it is a replica like any other, with a new replica pipeline, anchoring on the new primary's first frame. |
| Reconnect to the same primary | The same pipeline id: the run, its anchor and the expected side are kept, the local windows closed meanwhile extend the run, and the new session's first frame is compared as any later frame. It verifies every window since the last match, catch-up included, and nothing is counted unverified. A stream gap or a damaged entry reconnects the same way. |
| A stopped node | Lingering for `HALT_LINGER` to tell the others, then stopped and refusing to start (decisions 11, 12). |
| A failover around a stop | None should follow: the stop reaches the others, and two stopped nodes leave no quorum (finding 6). |
| Shutdown drain | Windows still open are dropped: nothing would act on them. |

### 14. Raft and failover

Raft carries no message for the stop (finding 6). Two things change, and
one is a prerequisite:

- **The voter cap (on `main`).** A cluster refuses more raft voters than
  one plus the primary's replica slots, three today:
  `melin_raft::MAX_VOTERS`, derived from `ReplicaSlotCursors::SLOTS` so
  the bound moves with it. It is checked on the `--raft-peer` flags
  (`check_voter_count`, in `build_raft_config` at config validation and
  in `spawn`'s peer-list validation), and on the *stored* membership,
  which overrides edited flags after the raft directory's first boot: in
  `spawn`, right after `storage::open`, over the newer by log id of the
  state machine's membership and the log's latest membership entry, read
  synchronously through the stores' mutexes (`storage::stored_membership`),
  so it fails the node's start before the driver thread exists. Not in
  `driver_main` beside the stored-membership comparison: a refusal there
  can only `mark_stopped`, leaving a node that serves on without raft.
  It applies whatever the digest setting: a voter that can never be a
  connected replica adds nothing to failover. With
  one configured replica there are two voters, and auto-promotion is
  already refused (`raft.rs`). Without
  raft, or with raft observational, nothing auto-promotes: only the
  operator paths of step 6 remain.
- **The leader lease and its confirmation (on `main`, the prerequisite
  met).** A raft leader cut off from its quorum keeps reporting `Leader`
  (openraft 0.9 has no check-quorum step-down). A replica can hold raft
  leadership while the primary is healthy, and would promote without an
  election once its link had been down past `PRIMARY_DOWN_GRACE`. So
  quorum loss alone would not stop a raft-leading replica that missed
  the stop. `main` closes this in two layers (`fix/raft-promotion-lease`,
  `docs/internal/raft-control-plane.md`):
  - **When deciding to promote**, `leader_lease_check`
    (`raft_promotion.rs`) refuses unless a majority acknowledged this
    leadership within `QUORUM_ACK_MAX_AGE`, half the leader lease
    (`melin-raft`'s `driver.rs`). It is an early filter, not the safety
    argument: a recent ack can predate the halt.
  - **When the promotion takes effect**, `journal_promotion_epoch_bump`
    (`server.rs`) asks the raft driver to confirm with a fresh majority
    round trip (`LeaderConfirm::confirm`), at the term the election was
    won at, before it journals the new epoch. Without the confirmation,
    the promotion is abandoned, nothing is journaled, and the node exits
    saying why.

  The confirmation is what this plan rests on. It starts after the
  decision, so after the grace ran out and after any halt that came
  before. With both other nodes stopped, nobody answers it, and the
  unaware leader's promotion is abandoned; that node then exits, so it
  stops too. With only one other node stopped (both replicas between
  sessions when the primary stopped), the other replica is still in raft
  and answers, and the case is the documented limit. A manual `PROMOTE`
  is deliberately not confirmed, so it stays usable during a raft outage:
  the plan's "manual promotion overrides the stop".

The stop cause added to `FenceState` lets raft's stand-down and refusal
lines say "stopped: output divergence" rather than "fenced". Step 4
carries the test that pins the whole: a three-node cluster whose primary,
the raft leader, detects; both replicas stop, nothing promotes, and every
restart is refused.

### 15. Hot-path cost

| Where | Per report | Per batch / iteration | Per window |
| --- | --- | --- | --- |
| Matching stage, every copy | nothing | one `Release` store (applied position) | nothing |
| Primary response stage | encode (already paid for delivered replies; new for node events' and gone clients'), one XXH3 update, after the send | one `Relaxed` latch load per slot; on the idle path one `Acquire` load | finalize, one chain hash, one seqlock store |
| Replica drain thread | encode, one XXH3 update (both new) | one `Acquire` load when the ring is empty | finalize, one chain hash, one seqlock store |
| Shadow | encode, one XXH3 update (both new) | one board `latest()` read | finalize, one chain hash, comparator update |
| Journal stage (primary with replicas) | nothing | one latest-window load, at most one division and one board read | at most one 42-byte frame encode per batch, appended to the batch's slot: 46 more bytes copied, no extra publish |
| Replica receiver | nothing | one `Acquire` load per iteration (and per pass of a waiting push) | one board read, one chain hash, comparator update; per frame, decode and comparator update |
| Primary senders | nothing | per ack: 9 more bytes read and one well-predicted flag test in `record_ack` | nothing |

Nothing allocates after construction. The measurement step (step 6) first
folds windows of `K` representative report bodies (the examples', and a
few hundred bytes) through XXH3-128, with the chain step, with and without
`twox-hash`'s `std` feature (step 1), and through BLAKE3, in a
micro-benchmark, so the hash's share, and the per-window share decision 2
estimates, are known apart from the encode's. It then runs `melin-bench`
closed-loop against echo, counter and notary, digests on and off, over
kernel TCP and DPDK, primary with two replicas under `disk+ram`, and
compares p50, p99 and p99.9. It reads the `latency-trace` response-stage
histograms for the fold's share of the reply path, and checks that the
replica drain, the replica receiver and the shadow keep up at full rate
(no growth in the matching stage's ring waits). It reports the replicas'
chain-break count, under the examples' traffic and under a report-less
load (ticks only, at a short tick interval, beside echo), and across a
replica cut off long enough that its reconnect catches up over many
windows, as the evidence open question 11 is decided on: a nonzero count
is a finding to report, not a correctness failure (decision 5). Results go
in a dated measurement document naming its hardware.

### 16. Observability

Logs (levels per the project convention):

| Event | Level | Where |
| --- | --- | --- |
| Output diverged: the node stops (check, range, digests, run, this node's build and, when they differ, the primary's) | `error!` | the detecting node |
| Told to stop (who, which path, the position, this node's build and, when known and different, the teller's) | `error!` | every node told, by an ack, a handshake or a halt frame |
| Marker written (with its path) | `error!` (part of the stop) | every stopped node |
| Still telling: the notice not yet answered (who it is waiting for) | `warn!`, at intervals during the linger | a stopped node |
| Notice answered, or the linger ended unanswered | `info!` / `warn!` | a stopped node, before it exits |
| Startup refused: the marker exists | `error!` (the run's returned error) | a stopped node, at each start |
| Output digests off on this node (`--no-output-digests`), or no in-process check (no shadow) | `warn!` at startup | every node |
| The primary sends no output digests for this session | `warn!`, once per session | replica |
| The other end of a replication session runs another build (both builds' prefixes; compared all the same, open question 2) | `info!`, once per session | replica, and the primary per replica |
| This node cannot read its own executable, so it sends an unknown build | `warn!` at startup | every node |
| A digest kind unknown to this build (a frame or a `digest_kinds` bit) | `warn!`, once per session | replica |
| A chain broke: a local window lapped before the comparator copied it (with the number of windows lapped) | `warn!`, first then at each doubling of the count | the comparing thread |
| A report the encoder refused, folded as a failure (with the encoder's reason and the count) | `warn!`, first then at each doubling of the count, per copy | every folding thread |
| A reply due to a live client the encoder refused (unchanged) | `error!` | the egress |
| A session started: the run kept (same primary pipeline id) or re-anchoring (a new primary pipeline, this replica pipeline's first session, the first compared session after an uncompared one, an uncompared session), with both ids' prefixes | `debug!` | replica receiver |
| A run anchored (with the windows left unverified before it, and their reason) | `debug!` | the comparing thread |
| Output digests on, window `K` entries, this node's build | `info!` at pipeline start | every node |
| A match | none (metrics only) | |

The mixed-build line is `info!`, not `warn!`: nothing is degraded, since
the pair is compared as any other, and running two builds is a state an
operator chooses (an upgrade that keeps output identical, a cluster
across architectures). It is there so that, should the stop fire later,
the log already shows the cluster ran mixed, beside the `error!` line
that names both builds. An unreadable executable is `warn!`: nothing is
lost to detection, but every stop and every peer's log would then name
an unknown build, a diagnosis the operator should restore.

Metrics (`/metrics`, rendered in `health.rs`, on replicas through the
replica health endpoint; `/healthz` carries whether digests are on, each
comparison's run, and, on a stopped node during its linger, the stop's
cause and the notice's state):

| Metric | Type | Meaning |
| --- | --- | --- |
| `melin_output_digests_enabled` | gauge | 1 when this node folds and checks. Alert on 0 where it is not intended |
| `melin_output_digest_window` | gauge | last window end this node's live copy digested |
| `melin_output_verified_from{against="primary"\|"shadow"}` | gauge | first position of the current run |
| `melin_output_verified_through{against}` | gauge | last window end of the current run found equal. Every position from `verified_from` to it was compared. A gauge that stops moving under traffic is worth a look |
| `melin_output_checks_total{against}` | counter | comparisons found equal |
| `melin_output_unverified_windows_total{reason="start"\|"new_primary"\|"gap"\|"uncompared"}` | counter | replica against primary only, so it carries no `against` label: the shadow's run starts at `F` and cannot break, so it never has an unverified window. Windows left out of every run, each counted once, when the next run anchors, under the stretch's cause (`uncompared` over `gap` over `new_primary` over `start`). `start` grows at a replica's start and at a resync of its state, `new_primary` at a primary restart or a promotion; neither is an alert, and a reconnect to the same primary adds to neither. Alert on `gap`. Nothing is counted while a stretch is still open: while the primary sends no digests, `uncompared` stays flat and grows only once a primary sends them again and a run anchors; meanwhile the stall shows as `verified_through` standing still and the once-per-session "sends no output digests" `warn!`. A primary on another build is compared, so it adds nothing here |
| `melin_output_chain_breaks_total` | counter | replica against primary only, as above. Runs ended by a lapped local window, live or during a catch-up; a session start never counts here, whatever its id. Alert on any growth |
| `melin_output_frames_skipped_total{reason="kind"}` | counter | digest frames of a kind this build does not know |
| `melin_output_encode_errors_total{copy="live"\|"shadow"}` | counter | reports the encoder refused, folded as failures (decision 4). Every copy counts the same failures when they agree, so growth alone is the application's choice or bug, not a divergence |
| `melin_output_diverged{cause="detected"\|"cluster_halted"}` | gauge | 1 once the node has stopped (seen during the linger, before it exits) |
| `melin_cluster_halt_notice{state="pending"\|"delivered"}` | gauge | a stopped node's notice, during the linger. A linger that ends `pending` is the documented limit's case: page on it |

`melin_replica_divergence_total` keeps its meaning (journal chain) so that
existing alerts do not change. A stopped node is fenced, so the health
endpoint's `trading` flag already reads false on it; `/healthz` adds the
stop cause. The health endpoint stays up through the linger, so these are
readable until the node exits. After that, the exit, the refused restarts
and the marker are the record.

## Steps

Each step builds, passes the full suite (kernel TCP, and the DPDK suite
under the netns runner), and is committable on its own. Docs and
`CHANGELOG.md` move with the behaviour they describe.

**Steps 3 to 5 ship in one release.** Their raft prerequisite, the
promotion's leadership confirmation, is already on `main` (decision 14).
Step 3 alone would stop a node whose shadow trips without
telling the others, so a primary would stop and its replicas fail over:
the opposite of the decision. Step 4 brings the notice, and step 5 the
cross-node check. Each stays its own commit, and the tree passes at each.

### Step 1. The pure building blocks

- `transport-core::output_digest`: `DigestKind` (exhaustive, `Output = 1`),
  `WindowDigest` and `ChainValue`, `OUTPUT_DIGEST_WINDOW`, `CHAIN_SEED`,
  `WindowFolder` (decision 1's definition, decision 2's first-full-window
  rule, decision 3's chain), `DigestBoard` (decision 5, with
  `for_pipeline(RingCapacity)` deriving `R` from both rings' capacity and
  `K` through one function the comparator shares),
  `DigestComparator` (decision 6, both modes, with `begin_session`,
  `SessionKind` and `SessionStart`), the `Trip` trait and `Divergence`
  (decision 6), `PrimaryPipelineId` (decision 8,
  with a crate-private constructor for step 4's decoder and these
  tests), and `BuildId` (decision 8, the newtype step 2's build-id
  function returns and step 3's latch records; the comparator takes
  none). Each type and constant
  carries the comment justifying its choice (decision 1's reasons for the
  hash and for `RawHasher` rather than an allocating `Hasher`; decision 2's
  for `K`; decision 5's for the seqlock board and `R`).
  `twox-hash` (2.x) becomes a workspace dependency, taken by
  `transport-core` with `default-features = false, features =
  ["xxhash3_128"]` (MIT, already on `deny.toml`'s allow list, so `cargo
  deny check` passes unchanged; new to `Cargo.lock`, which is staged with
  it). Leaving the defaults off drops the other algorithms and the
  `random` feature, and with it `rand`. It also drops `std`, which the
  crate needs for its runtime choice of SSE2, AVX2 or NEON: without it,
  the stripe loop that every window longer than XXH3's 240-byte
  short-input cutoff runs is the scalar one. Step 6's micro-benchmark runs
  both feature sets, and `std` is added if it wins (it turns on `alloc`
  only to make the allocating `Hasher` instance available, which the
  folder does not use).
- `encode_report_body` (decision 4): the zeroing, the debug-build purity
  check, `MAX_RESPONSE_BODY` and the body-bound check move into it from
  `response_frame.rs` (`frame_zeroed`, `check_encoder_purity`), with their
  comments; `server-runtime` re-exports the constant from where it is
  today, and `frame_app_response` and `encode_early_reply` call it, so the
  framing is unchanged. The purity check's tests (`response_frame.rs`'s
  unit tests and `server-runtime/tests/encoder_purity.rs`) keep passing,
  and the unit tests of the zeroing and the check move with the code; a
  test in `transport-core` pins that a direct call to `encode_report_body`
  (the path the drain and the shadow take) zeroes and, in debug builds,
  checks.
- `ResponseEncoder`'s "Purity" rustdoc and `docs/building-an-application.md`
  ("The determinism rules") already state the contract (decision 1). They
  gain its new consequence and "no node identity" among the examples.
  The rustdoc's "every response a node encodes" and the guide's "each
  response a node sends" become "every report a node encodes, sent or
  folded", since replicas and the shadow now encode reports that nobody
  is sent. The guide also names, under the existing tick rule, the causes
  of a repeated or backwards tick that remain (the double call per
  journaled tick, a clock step back on the primary, a failover to a node
  whose clock is behind), and says they reach every copy alike, so they
  matter to the application's own clock, not to the agreement between
  copies (finding 7).
- Tests, all ungated:
  - folder: the digest of a known window and the chain value after two
    known windows pinned to byte patterns (so the definition cannot drift
    silently between builds, including through a `twox-hash` upgrade or a
    change of its feature set), the pinned values taken from `xxh128sum`
    over the same bytes, not from the code under test; the streaming
    hasher fed report by report equal to `Hasher::oneshot` of the
    concatenated bytes, over windows on both sides of the 240-byte cutoff
    and longer than the hasher's internal buffer; a report moved to
    another position, a byte changed, two reports swapped, an encode error
    in place of a body, all change `d` and every later `C`; the same reports
    folded in two folders started at different positions agree on `d` for
    every window both saw whole and digest no partial window; two folders
    started at the same position agree on `C` at every window; a start at
    a window boundary and one just past it pick the right first window;
    empty windows digest their header and still extend the chain;
  - board: exact lookup, lap detection, `latest`, the `(end, d, C)`
    payload read back whole; a two-thread stress test asserting no torn
    value is ever returned, run under Miri by the existing Miri job as the
    seqlock is;
  - comparator, property tests over random interleavings of local windows
    and expected entries from two folders fed the same reports, a
    reference folder started earlier than the local one, the reference
    side sampled at random (most windows never sent), and laps forced by a
    small capacity, with a recording `Trip`:
    - every `Mismatch` follows exactly one `trip` call with the same
      `Divergence`, made before the verdict was returned. No trip happens
      without a `Mismatch`. After one, the comparator returns nothing more
      and trips no more, and a `Trip` that returns `None` (tripped
      elsewhere first) yields a `Mismatch` with `halted: None`;
    - never a `Mismatch` when the reports agree;
    - a divergence confined to one window inside a run (from the window
      after an anchor, or the anchor's own window when its local `d` is
      held or still to come, through a later expected entry with no `Break` between),
      including a window the reference never sends, is a `Mismatch` at the
      first expected entry at or after it, never later, and never missed;
    - the converse: a divergence confined to a window outside every run
      (before the first usable anchor, in a lapped local gap, or an anchor
      window whose local `d` is no longer held) produces no `Mismatch` and
      is counted unverified, under the right reason;
    - the invariant the chain exists for: every window inside every run
      (`verified_from` to `verified_through`) was pushed locally and
      extended, so no run spans a gap; after a `Break`, no `Match` is
      returned until a new `Anchored`;
    - `Anchored` accounts for every unverified window exactly, under the
      right reason, so `verified` plus `unverified` windows cover every
      local window up to the newest run's end, each exactly once, with the
      session starts in the generated sequence a random mix of held-id,
      new-id and `Uncompared` ones and breaks among them: a stretch holding
      an `Uncompared` session is counted under `uncompared` alone, never also
      under `new_primary` or `gap`, a held-id session start adds nothing,
      and a `Break` adds nothing to the count;
    - the anchor's own window checked by `d` when held, and, when still to
      come, held as the pending anchor and checked once it lands, so an
      anchor on a clean stream whose local side trails its expected side
      counts no unverified window of its own; a differing `d` there a
      `Mismatch` with `verified_through` `None` and the range that window
      alone; a pending anchor passed over by a local gap dropped, never
      anchored; a pending anchor kept while enough later expected entries
      arrive to fill the array, the oldest of those giving way instead;
    - `anchored(F)` given an expected entry before its first local window
      anchors no run below `F - K` and returns no `Break` at the first
      local push;
    - an expected entry below `F - K`, below the last gap, or whose next
      window has left the local array anchors nothing, and the next usable
      entry anchors, counting the windows before it;
    - sessions, with the generated session starts a random mix of the held
      id, a new id and `Uncompared`, and the reference folder restarted
      from the seed exactly when the id changes (as a restarted primary's
      is): never a `Mismatch` when the reports agree, across any of them;
      a session with the held id keeps the run, its anchor, a pending
      anchor and the expected side, returns `Continued`, counts nothing,
      and its first expected entry is compared rather than anchored, so a
      divergent one is a `Mismatch` and a matching one advances
      `verified_through` over every window pushed locally since the last
      match, the windows pushed between the two sessions included; a new
      id ends the run, clears the expected side and the pending anchor,
      re-anchors on the next usable entry and counts the stretch
      `new_primary`; the pipeline's first session continues the `start`
      stretch and returns `FirstSession`; an `Uncompared` session forgets
      the held id, so a later compared session re-anchors even under the
      earlier id and returns `AfterUncompared`, never `FirstSession`; a
      session with the held id and no run (after a `Break`) changes
      nothing either: an expected entry held from before it, at or above
      the gap, is kept and anchors once its local window lands; in every case
      a local window closed before the session start extends the run the
      session keeps, or the one anchored after it (no build enters the
      comparator, so "no build is ever skipped" needs no test here: its
      input has none to skip on);
    - `shared_start` mode: verified from `F` with no anchor, chain values
      compared directly;
    - built through `for_pipeline` at `RingCapacity::DEFAULT`, at
      `RingCapacity::MIN` and at a capacity above the default, with the
      local side closing up to both rings' worth of entries (at that
      capacity) between two reads: no break and no evicted sample; closing
      more than `R` windows between two reads is a `Break`, never a
      `Match` across it; one indexing helper for board and comparator, and
      one sizing function for both.
- `framing` tests in `response_frame.rs` keep passing unchanged: the
  framing is the same bytes, the zeroing and the purity check included.
- **Leaves untested:** nothing calls these types yet, so whether each copy
  feeds them the same reports is step 2's to show.

### Step 2. Every copy folds

- The matching stage's applied cursor (`PipelineCursors::matching_applied_wire_seq`).
- `response_loop::run`: encode through the folder, send, fold, as in
  decision 4; `Egress::send_slot` takes `SlotFrames`; `SlotReply` and its
  encoder go; both egresses (`response.rs`, `dpdk_response.rs`) adapt. The
  per-slot latch check is not added yet (no latch exists until step 3).
- The replica's drain thread hoisted into a function that borrows slots in
  place (as the response stage does) and folds; `shadow::run` takes the
  encoder and folds. Each pipeline builder creates the boards.
- `ServerConfig::no_output_digests` and `--no-output-digests`, on every
  node: no folder and no board when set (decision 2).
- The node's build id (open question 2): one function hashes the running
  executable once at startup, and a `ServerConfig::build_id` field
  defaults to its result. A field rather than a direct read so that a
  test can give two nodes of one process different builds (step 5's
  mixed-build case): the one seam that stands in for a second binary.
  The startup `info!` line names it; step 3's marker and step 4's frames
  carry it.
- Encode failures handled as decision 4 says:
  - the folder counts them and rate-limits a `warn!`;
  - both egresses keep `error!`, but only for a reply they were due to
    send;
  - `encode_report`'s rustdoc says both;
  - metric `melin_output_encode_errors_total`.
- Metrics `melin_output_digests_enabled`, `melin_output_digest_window`; the
  startup logs of decision 16.
- Tests:
  - `melin-server-runtime`'s in-crate `src/tests/` (a new
    `output_digest_agreement.rs` beside `response_flush_before_gate.rs`:
    the response stage and the replica's drain are in that crate, not in
    `melin-transport-core`, which depends on neither): one input sequence
    driven through a primary pipeline (response stage over a recording
    egress), a replica pipeline (its drain) and a shadow publishes identical
    window digests for every
    window all three saw whole, and identical chain values where they
    started at the same position. The sequence carries what differs between
    copies on purpose: a client request with no report (a `BatchEnd` slot
    on the primary only), a query (primary only), reports of a tick
    (connection 0), a report to a connection the egress does not hold, a
    reply released degraded, an ack-policy change, and a report the
    encoder fails on;
  - `response_loop.rs` tests: a reply to an unknown connection is folded
    and not sent; a window whose last entries have no report closes on the
    idle path once the applied cursor passes it, and not before; a
    window closes on a `BatchEnd` slot past its end and on a query slot
    at its end, with the ring never empty, so the test isolates the
    any-slot rule (decision 4); the
    applied cursor is loaded before the ring is read (a recording seam
    holding the stage between the two); with digests off, nothing is
    folded and the frames sent are the same bytes;
  - encode failures, over a recording egress and `logs::capture`: a
    refused report to connection 0, and one to a gone client, are folded,
    counted and logged at `warn!` (once, then at the second and fourth),
    never at `error!`; a refused reply to a live client is also logged at
    `error!` by the egress; the replica's drain and the shadow count under
    their `copy` label;
  - the chain-less build: the same agreement test with `hash-chain` off.
    The pre-commit hook and CI only `cargo check` that build, so the step
    adds a test run of these cases (`cargo nextest run -p
    melin-server-runtime --no-default-features`, filtered to them; that
    crate's default `hash-chain` forwards to the journal and transport-core
    features, so the whole pipeline is chain-less) to the pre-merge job's
    chain-off loop;
  - the DPDK egress's adaptation runs under the netns runner through the
    existing response-stage tests in `dpdk_response.rs`;
  - the build id: the hashing function, run on the test binary, equals a
    BLAKE3 of that file read independently; given a path it cannot
    read, it returns the all-zero id and logs the startup `warn!`
    (`logs::capture`).
- Docs: `docs/pipeline-architecture.md` (the response stage folds every
  report; the replica's drain and the shadow encode), the new flag in
  `docs/replication.md`'s CLI table. `CHANGELOG.md` **Changed**:
  `encode_report` must be deterministic; `ServerConfig` gains the public
  fields `no_output_digests` and `build_id` (breaking for a struct literal); a report the
  encoder refuses that no client was due is now logged at `warn!` and
  counted, rather than never encoded. **Added**: the
  `--no-output-digests` flag, `melin_output_encode_errors_total`.
- **Leaves untested:** the cost, which is step 6's measurement, not a test;
  any comparison, which steps 3 and 5 add.

### Step 3. The stop, and the in-process check

- `FenceState`: a stop cause (`Superseded | Diverged`, exhaustive), first
  cause wins; `is_fenced` unchanged for its readers; the cause in
  `/healthz` and in the raft stand-down's log line.
- `DivergenceLatch` (in `transport-core`, beside `FenceState`), `trip`,
  `follow` and the followed build it stamps on a `Detected` stop (decision
  11; step 4's receivers call it),
  `HaltCause` (`Detected` only; step 4 adds `ClusterHalted`), the `Halted`
  token with its `Drop` recording `Abandoned`, the latch's marker state
  and its claim, the `divergence-marker` writer thread, `MarkerOutcome`
  (`Unconfirmed` included), the marker
  (`NodeStore::divergence_marker`), the `NodeDiverged` error through the
  one exit helper (decision 11), which in this step returns once every
  thread is joined: the linger comes with the notice, in step 4.
  `melin_journal::error::hex_prefix` widened to `&[u8]`
  for the divergence log line, rather than a second formatter beside it;
  every existing call passes a reference to a 32-byte array, which
  coerces, so no caller changes.
- The response loop's per-slot latch check, before the send and before an
  early reply's release; the shadow's check before each save.
- The latch built once per node from the node's `FenceState` and shutdown
  flag, and passed to every pipeline builder. The replica's shadow takes
  the latch in place of `pipeline_shutdown` for its trip, and keeps the
  pipeline flag only to exit at teardown. The receiver's `accept_slot`
  abort closure checks the latch beside `journal_failed` at both call
  sites: the streaming path, which leaves through `SessionExit::Shutdown`,
  and the promotion drain (`process_drain_frames`), which stops at the
  last contiguous slot and logs the cause it read. The promotion branch
  reads the latch after its drain loop: set, it skips
  `drain_pending_acks` and leaves through `SessionExit::Shutdown` (the
  interim exit, as on the streaming path; step 4 makes it the notice and
  `Stopped`), never `SessionExit::Promote`. That reading is the early
  exit; the guarantee is the promotion teardown (decision 11), which joins
  the replica pipeline, reads the latch, and hands the application over
  only when it is clear, called by `handle_session_exit`'s `Promote` arm
  and by `take_pipeline_for_promotion` alike. Set, both return as on an
  ordinary shutdown (`Ok(None)`, the interim, as above), and the exit
  helper finds the stop and returns `NodeDiverged`.
- The shadow's comparator (`shared_start`) against the live board on every
  node with a shadow (decision 10).
- The shadow prefaulted (assessment 1.3 (d)): today the live instance is
  prefaulted with the node's `Sizing` and the shadow, a
  `clone_via_snapshot` of it, never is, so it runs at whatever capacity
  `restore` chose. One `server-runtime` helper, `shadow_clone(&app,
  sizing)`, clones and then prefaults, and replaces the bare
  `clone_via_snapshot` at all three sites: the kernel primary
  (`server.rs:1838`), the DPDK primary (`:3047`) and the replica
  (`replication/mod.rs:593`). `run_impl` and `run_dpdk_impl` prefault the
  live instance before calling `run_as_primary` and `run_as_primary_dpdk`,
  at boot and after a promotion (`server.rs:1353` and `:1268`, `:2910`
  and `:2796`); those two take no `Sizing` today, so both gain the
  parameter. This
  is hygiene, not correctness: a compliant application's decisions do not
  depend on capacity (`prefault`'s contract), but one that breaks the rule
  by emitting reports in a hash map's iteration order would otherwise trip
  its own shadow on upgrade day for a capacity difference the node itself
  made.
- `NodeStore::refuse_if_diverged` and `NodeStoreError::Diverged`, called
  from `ServerConfig::startup_store()` by both entries; `resolve` and
  `store()` unchanged (decision 12).
- Metrics `verified_from{against="shadow"}`,
  `verified_through{against="shadow"}`, `checks_total{against="shadow"}`,
  `melin_output_diverged`. On a replica they must reach the endpoint
  `spawn_replica_health` builds through `HealthState::for_replica`
  (`raft.rs:338`), which takes only the fence state, the raft status and
  `pipeline_healthy` today and outlives every pipeline a resync rebuilds:
  so the output metrics live in one node-lifetime handle the entry
  creates, passes to `HealthState::for_replica` (and to the primary's
  `HealthState`), and hands to each pipeline it builds.
- Tests:
  - `response_loop.rs`: a latch set while a slot waits at the gate: the
    slot is never offered to the egress after its release, nor any later
    slot or early reply; a latch set between two slots of one batch: the
    first is sent, the second is not. This pins "no reply after the latch
    is observed";
  - `DivergenceLatch`: the first trip wins; it fences and sets shutdown
    before returning the token; a `Detected` stop carries the build last
    passed to `follow` (`None` with no `follow`, or after it was cleared),
    whatever the trip site; the exit helper's claim, for each of its
    cases (nothing, marker written, marker failed, a stop left
    `Abandoned` by a dropped token: the helper hands the write to a
    writer), and `Pending` or `Claimed` left by a `mem::forget` token or
    guard, or by a writer still in its fsync: the helper waits one bound,
    never blocks, and returns `Unconfirmed`, including a stop deposited
    by another thread while the caller left through ordinary shutdown; a
    waiter blocked on `Pending` released by the token's drop, finding
    `Abandoned`, claiming once, and a second waiter racing it waiting
    through `Claimed`, finding `Written` and claiming nothing (two
    threads, so the claim's exclusivity is what is tested); a claim
    guard dropped before it deposits returns the state to `Abandoned`,
    and the next waiter claims; a waiter whose bound passes with the
    write unsettled returns unsettled, and no tripping thread ever runs
    the fsync itself (it hands the token over, the spawn-failure fallback
    aside);
  - marker: written durably (temporary file, rename), parsed back, every
    field present, each value in full canonical hex, the range and the
    "window differs itself" flag; `notice` reads `none` in this step; the
    node's build always, and a `Detected` stop whose latch followed
    another build (`follow` called directly, since only step 4's receivers
    call it) writes both, parsed back into `builds`, while one following
    the same build or none writes one, and an all-zero id reads back as
    unknown (a latch that followed an all-zero build writes both, the
    other as `unknown`, and its refusal carries no mixed-build
    sentence); an
    unparseable marker still refuses;
  - `hex_prefix`: the same prefix for a 16-byte value as for a 32-byte
    hash sharing its first bytes, so the chain's log lines are unchanged;
  - no file moves: after a stop, every snapshot and the journal are where
    they were;
  - the shutdown drain (decision 13): a primary and a replica, each with a
    shadow, shut down cleanly under load with a window open on every
    folder (the load stopped off the grid, at a position not a multiple of
    `K`): neither trips, no marker is written, the run returns `Ok`, and
    both restart unrefused;
  - `node_store.rs`: `refuse_if_diverged` refuses with the marker present,
    and the message names the marker, the cause, the position and the doc
    section, each as the marker holds it, and says a new cluster is the way
    back; it carries the mixed-build sentence, naming both builds, exactly
    when the marker names two that differ; `resolve` on the same store
    succeeds and moves nothing;
    `server.rs`: `startup_store()` refuses where `store()` does not, and
    a node refused at startup never spawns its raft driver;
  - shadow: a fault-injecting test app whose `restore` leaves a
    non-serialized flag that changes a later report (a lossy
    `snapshot`/`restore`): the shadow, built by `clone_via_snapshot`,
    diverges at the first live window it compares at or after the window
    holding the changed report, the marker's range contains that window,
    and no snapshot is saved after the trip; the shadow's run covers every
    window from its first, with no unverified window;
  - integration, `server-runtime/tests/output_divergence.rs`, started
    through `melin-test-node` so it also runs on DPDK under the netns
    runner: one node with that app under load stops, its run returns
    `NodeDiverged`, the `error!` appears (`logs::capture`), the marker
    exists, every client connection is closed and a request sent after the
    trip gets no reply (the per-slot ordering itself is pinned by the unit
    test above, which an end-to-end test cannot time), and a restart is
    refused with the error;
  - integration, same file, a replica's shadow trips (the lossy-restore app
    on the replica only): its receiver leaves through ordinary shutdown,
    and its run still returns `NodeDiverged` with the marker written
    (decision 11, "Which thread does what"). The primary
    keeps streaming under load, so the trip lands with the replica's input
    ring full behind the stopped shadow, and the run must return within a
    bound, which pins the latch check in the waiting push;
  - `receiver_transport.rs`, the waiting push: a scripted transport, an
    input ring with no consumer so it fills, and the latch tripped from
    another thread while a push waits. The session ends in
    `SessionExit::Shutdown`; the frame's slots pushed before the abort
    are committed (the ring's producer cursor covers them) and nothing
    after them is pushed. The same for the
    promotion drain: `process_drain_frames` over scripted drain frames
    into a full ring, the latch tripped while a push waits; the drain
    returns, stopped at the last slot it published. And through
    `streaming_loop`, a promotion requested on the scripted transport
    with the latch tripped during the drain (the full-ring case) and,
    separately, tripped by the scripted transport's `poll_recv` during
    the drain with ring space to spare, so the drain publishes every
    frame and only the branch's latch read sees the trip: each session ends in `SessionExit::Shutdown`, not
    `Promote`, and the scripted transport records no ack after the trip;
  - `replication/mod.rs`, beside the `handle_session_exit` exit table
    (`:4031`), the trip that both of those miss: the shadow tripping on
    the last drained entry after the receiver's reading. A pipeline whose
    shadow stub waits for `pipeline_shutdown` (which only the teardown
    sets), then trips the latch and exits, so the trip lands strictly
    after the session ended in `SessionExit::Promote` and before the join.
    `handle_session_exit` with `Promote` returns `Ok(None)`, never the
    application, and the exit helper returns `NodeDiverged`; the same stub
    through `take_pipeline_for_promotion` (the reconnect-loop top's
    promotion) returns no application either; and with the stub exiting
    without a trip, both hand the application over as today, with the
    latch's followed build cleared, so a trip after it names none;
  - shadow prefault: a test app that records each `prefault` call and its
    `Sizing`; on the kernel primary, the DPDK primary (under the runner)
    and the replica, the shadow instance has been prefaulted once with
    the node's `Sizing`, after the clone. And a capacity-order app (its
    reports for an event follow a fixed-hasher map's iteration order, its
    `restore` rebuilds the map at the entry count, its `prefault` reserves
    `Sizing`'s count): a unit test on `shadow_clone` applies the same
    events to a prefaulted live instance, to its bare `clone_via_snapshot`
    and to its `shadow_clone`, and the reports of the bare clone differ in
    order while the `shadow_clone`'s match, which pins that the prefault
    is what aligns the two; and one node under load with that app and a
    shadow does not stop;
  - `DivergenceLatch`: a trip sets the flag the latch was built with,
    and the replica builder passes the node's flag, not
    `pipeline_shutdown`. The builder test asserts that the shadow's latch
    holds the same `Arc` as the receiver's shutdown flag (`Arc::ptr_eq`);
  - a stopped node's raft driver leaves within its poll: a three-node
    in-process raft test whose stopped node's peers see it `Silent`.
- Docs: `docs/replication.md`, a first "Output verification" section with
  the in-process check, the stop and the startup refusal; the cluster-wide
  stop lands with step 4, the runbook in step 6. `CHANGELOG.md` (with
  steps 4 and 5, one release) **Changed**: a node whose shadow disagrees
  with its live copy now stops and refuses to restart (new, always-on
  behaviour, named first); `NodeStoreError::Diverged` (re-exported from
  `melin-transport-core`) is a new variant of a public exhaustive enum, so
  a consumer's `match` stops compiling until it handles it; the node
  entries resolve through `startup_store()`. **Added**: the `NodeDiverged`
  error a node's run returns, `NodeStore::refuse_if_diverged`.
- **Leaves untested:** a crash between the trip and the marker (the
  exit helper's backstop is unit-tested, not a killed process); a
  hardware fault is modelled only by the fault-injecting app (the
  detection path is the same); the cluster-wide stop, which is step 4's.

### Step 4. Replication protocol 9, and the cluster-wide stop

The version check at authentication, first planned in this step, is on
`main` already (decision 8, `f4971f95`), with its tests: the frames'
round trips and truncations, the primary's verdict on both transports,
the replica's check, and a v9 replica refusing a scripted v8 primary's
one-byte `AuthOk` without a handshake and redialling. What remains:

- `replication/protocol.rs`: `MSG_DIGEST` and `MSG_HALT` with
  encoders returning fixed-size arrays (decision 8), decoders and size
  assertions; `AckFrame` and `HandshakeFrame` gain `flags` and
  `window_end`, with unknown bits refused, and `HandshakeFrame` the
  replica's `build_id`; `StreamStart`'s
  `digest_kinds`, build id and pipeline id (`StreamStartFrame` to 115
  bytes); the version's history line gains these frames (9 if still
  unreleased, else a bump to 10, decision 8).
- The pipeline id (decision 3): `output_digest::PipelineId`, its
  `draw()` and its `as_bytes()` accessor (`encode_stream_start` takes the
  raw `[u8; 16]`), with `getrandom` 0.4 added to `transport-core` (already in
  `Cargo.lock` at that version, staged with the manifest);
  `build_pipeline_with_replication` takes a `PipelineId` by value and keeps
  it in `Pipeline`; `run_as_primary` and `run_as_primary_dpdk` draw one per
  call, a failed draw failing the start; every test caller of the builder
  draws its own.
- Every sender fills `StreamStart` (kernel `tcp_sender.rs` and
  `catchup.rs`'s snapshot path, the DPDK sender and its `join_worker.rs`):
  the output bit unless the node runs with `--no-output-digests`; the
  node's build id (step 2's `ServerConfig::build_id`); the pipeline's id, the
  same on every session of that pipeline. Every receiver fills its
  handshake's `build_id` from the same field.
- Every receiver records the session's kinds, build and pipeline id from
  `StreamStart` (kinds and pipeline id for its comparator, the build
  through the latch's `follow`, decision 11), and warns once when the
  primary sends no output digests or names an unknown kind. Every sender records each
  replica's build from its handshake, for the `peer_build` of a stop that
  replica tells it of. Both ends log the other's build once at `info!`
  when it differs from their own (decision 16), and nothing else reads it.
  Digest frames are decoded and, for now, dropped: no primary sends one
  until step 5.
- The cluster-wide stop (decision 11, "The notice and the linger"):
  - `HaltCause::ClusterHalted` and `SessionExit::Stopped(HaltCause)`, with
    its arm in the shared `handle_session_exit`'s exhaustive `match exit`
    returning `AfterSession::Return` with the stop's error (decision 9),
    so neither receiver's reconnect loop goes round again;
  - `PrimaryFrame` (decision 8), so all five replica read sites handle
    `Halt`, the post-snapshot `StreamStart` (`replication/mod.rs:1423`)
    included;
  - `streaming_loop`'s loop top reading the latch before its shutdown arm,
    so a stop skips `drain_pending_acks`; the replica's flagged ack, its
    wait for `Halt`, and the exit helper's redial with a flagged handshake
    over the receivers' factored dial-and-handshake code. The waiting
    push's abort on the latch (step 3's; on today's code an abort becomes
    `FrameError::Fatal("ring full mid-publish")`,
    `receiver_transport.rs:554`, and `SessionExit::Fatal` at `:1120`, and
    which step 3 routes to `SessionExit::Shutdown` as an interim) now
    pushes nothing after the slots already in the ring (those it commits,
    as on step 3, decision 11) and returns to `streaming_loop`'s loop top,
    whose latch read runs the notice and ends in `Stopped`; a `journal_failed`
    abort stays `Fatal`. The promotion branch's latch read after its drain
    (step 3's, which left through `Shutdown`) now runs the same notice
    and ends in `Stopped`, and step 3's promotion teardown, finding the
    latch set, now returns the stop's error as the `Stopped` arm does, so
    the exit helper's replica linger redials `--replica-of` with a flagged
    handshake (the link is closed by then, so no in-session notice); step
    3's post-`Promote` stub case now also asserts that redial, with
    `STOPPED` and the window end in its handshake, against a scripted
    primary that answers `Halt`, and the marker's `notice` going from
    `pending` to `delivered`. Step 3's replica-shadow integration case, whose
    receiver left through `SessionExit::Shutdown`, now asserts `Stopped`
    and the primary's stop, and step 3's `receiver_transport.rs`
    waiting-push case now asserts one flagged ack on the scripted
    transport, carrying the positions last sent rather than the committed
    partial frame's target, no ordinary ack, and `Stopped`; step 3's two
    promotion-drain cases now assert the same (one flagged ack, no
    ordinary ack, the wait for `Halt`, and `Stopped`, never `Promote`);
  - `record_ack` (`AckRecorded`, the `STOPPED` check first), `keep_last_ack`
    (`JoinFrames::Stopped`, and a violation arm for an ack refused for its
    flags) and the handshake tripping the latch;
    `ReplicaCursors::new` takes the latch;
  - every sender's `Halt` on every exit path once the latch is set: the
    kernel live loop through its ring before its guard's teardown, every
    other kernel path in `run_replica_slot` with a bounded write, keyed on
    the handler's `SlotEnd`, DPDK in
    `close_all_links` with a FIN and a bounded wait instead of the reset;
  - the endpoint handed back to the exit helper (the sender thread
    returning its `ReplicationListener`, `run_as_primary_dpdk` its
    transport), and the primary's health endpoint on a stop flag of its
    own, its handle moved out of `PipelineHandles` into a
    `ReplicaHealthGuard` the entry holds (renamed `HealthGuard`, now that
    both roles use it), stopped after the linger, or at once after
    `stop_and_join` on an ordinary shutdown;
  - the handshake-only endpoint during `HALT_LINGER`, its end counted by
    distinct key: on kernel TCP over the hoisted authenticate-and-read-
    handshake function `handle_replica_connection` now calls, on DPDK a
    linger poll loop over the returned transport, reusing the hoisted
    challenge issue and `step_authentication`, its fixed array of `SLOTS`
    linger connections each with one absolute deadline checked in every
    phase (decision 11); the exit helper running
    the linger; the marker's
    `notice` states; `HALT_NOTICE_BOUND`, a constant independent of the
    heartbeat, with its compile-time bound against `HALT_LINGER`.

  The notice is driven by the shadow's trip in this step. Step 5 adds the
  replica's comparison as a second detector, through the same notice.
- Raft: the stop cause in the stand-down and refusal lines (the voter
  cap of decision 14 is on `main`).
- Tests: round trips; wire byte patterns beside `ack_wire_byte_pattern`,
  `rotate_wire_byte_pattern` and `chain_check_wire_byte_pattern`
  (`server-runtime/src/replication/mod.rs`); the grown handshake still refused at another version (the
  existing `a_handshake_from_another_protocol_version_is_refused`, with
  the new fields after the version; the authentication cases are on
  `main`); kind 0 refused, an unknown kind skipped and counted, not
  fatal, and an unknown `digest_kinds` bit warned about and ignored; a
  window end of 0 or off the kind's grid refused as malformed;
  `RingChunk::classify` sorts a `Digest` chunk exactly as a `ChainCheck`
  one (ungated, so the DPDK sender's `LiveHandoff`, which shares the rule,
  is covered on every host); a sender started with `--no-output-digests`
  advertises no kind (`receiver_transport.rs` with the scripted
  transport, which records what the receiver logs and counts); the
  pipeline id: two `draw()`s differ; two `build_pipeline_with_replication`
  calls in one process, over the same application, journal and
  configuration, announce different ids (`pipeline_tests.rs`), while every
  `StreamStart` one pipeline sends, across reconnects and on the catch-up
  and snapshot paths, carries the same one (the `catchup.rs` test that
  checks the post-snapshot `StreamStart`'s fields gains it).
- Tests of the stop, unit and ungated:
  - `protocol.rs`: the ack, the handshake and `Halt` round-trip with
    every flag and cause, the handshake with its `build_id`, all-zero
    included; unknown bits and causes, and a `window_end`
    without `STOPPED`, are refused; a v8 handshake gets the too-short
    error, and a v9-sized one carrying version 8 the version error;
  - `cursors.rs`: a flagged ack trips the latch, returns
    `AckRecorded::Stopped` with the token, and moves no cursor; a flagged
    ack whose positions would fail the sent high-water check still trips
    rather than evicting; a second flagged ack returns `Stopped(None)`;
    an ack refused for its flags evicts as a violation, and is not
    skipped, in `drain_ack_frames`, which reads the streaming acks of
    both transports, and in `keep_last_ack` below;
  - `keep_last_ack` returns `JoinFrames::Stopped` when any ack it reads
    carries the flag, a newer unflagged one included, and its violation
    arm for an ack refused for its flags. Today its tests
    compile only under the `dpdk` feature, so the function and its
    `JoinFrames`, which need no hardware, move beside `try_extract_frame`
    in `receiver_transport.rs`, ungated, so the test runs on every host;
  - `receiver_transport.rs`, with the scripted transport: after the
    latch, one flagged ack carrying the last positions sent and no
    ordinary ack before it (the shutdown arm's drain skipped), data frames
    read and discarded and nothing published during the wait for `Halt`,
    and a flagged handshake on the next dial when none came; a `Halt`
    received trips `ClusterHalted` and ends the notice at once; a `Halt`
    as the handshake's answer, and in place of a `SnapshotBegin`, a
    `SnapshotChunk`, a `SegmentSeedBegin` and the `StreamStart` that
    follows `SnapshotEnd` and the seed, trips the latch and ends in
    `Stopped`, never in an "unexpected response" error (the scripted
    transport serves both receivers' shared code; the transport-specific
    handshake arms are the integration cases' below);
  - `tcp_sender.rs`, over a socket pair: the live loop's halt sends the
    `Halt` after an in-flight send completes, shuts the write side only,
    keeps reading acks the peer sends after it, and the peer reads the
    whole `Halt` before end of stream, and exactly one `Halt` (the live
    loop's `HaltSent` suppresses `run_replica_slot`'s); `run_replica_slot`
    writes a `Halt` on `Open` and none on `Unauthenticated` or `Broken`;
    its halt on a peer that never reads returns within the write bound;
  - the latch's marker state: a link-owning waiter finds `Abandoned`,
    claims it and hands the write to a `divergence-marker` thread, and the
    marker lands before it sends (step 3's claim, here exercised from
    the notice);
  - the linger's rules as pure functions: when the primary's endpoint
    stops answering (`HALT_LINGER`, or `SLOTS` distinct keys told; one key
    told twice, in session and by handshake, counts once), when a replica
    stops redialling, and the marker's `notice` transitions;
  - the factored kernel dial under a deadline, ungated over loopback: a
    listener whose accept queue is full (its backlog shrunk with
    `libc::listen(fd, 0)` and filled by one unaccepted connection, so the
    kernel drops further SYNs, as a host behind a dead link does) makes
    the dial return a timeout at the deadline rather than after the SYN
    retries, and a peer that accepts but never answers the handshake
    returns at the deadline too, not after the 5 s read timeout when less
    is left; a peer that drips its challenge one byte inside each read
    timeout still makes the dial return at the deadline; and a deadline
    already passed returns the timeout outcome
    without a call, not an `InvalidInput` from a zero `Duration`; the
    DPDK dial's deadline is the linger loop's, covered by the
    pure-function case above and the netns linger case below.
- Tests of the stop, integration (`output_divergence.rs`, on both
  transports, the DPDK cases under the netns runner; raft on kernel TCP),
  faults through `Sizing`:
  - the primary, the raft leader, detects through its shadow: both
    replicas stop on `Halt`, nothing promotes, and every restart is
    refused;
  - a replica's shadow detects: the primary stops on the flag, and the
    other replica on the primary's `Halt`. The detecting replica runs
    under another build id (step 2's `ServerConfig::build_id`), so the
    session's `info!` line names both builds on both ends, and the
    primary's marker, its `error!` line and its refusal name the
    replica's build beside its own (`peer_build`, learnt from the
    handshake), the detecting replica's name the primary's beside its own
    (the build its latch followed, though its shadow, not its receiver,
    detected), while the other replica's, on the primary's build, name
    one;
  - the unaware raft leader. A replica is made raft leader first. Only its
    replication link is then cut by the test proxy, held past
    `HALT_LINGER`, timed so its grace runs out just after the primary's
    shadow detects; its raft links stay up. The primary and the other
    replica stop without reaching it, and leave raft. The replica's last
    majority ack is from just before the stop, so whether the lease
    filter or the confirmation refuses it depends on timing the test does
    not control. The test asserts the outcome both give: no `EpochBump`
    is journaled, no node serves, and the refusal logged is one of the
    two. `main`'s own tests pin each layer; this one pins that the stop
    leaves a raft-leading survivor with nothing that confirms it;
  - a replica between sessions when the primary stops (held in reconnect
    backoff by the test proxy, `tests/common/proxy.rs`, then released
    inside `HALT_LINGER`): it meets the lingering primary's handshake
    answer and stops with its marker (`cluster-halted`, by the primary,
    via the handshake answer), and nothing promotes; the same with the
    replica dialling while the primary's pipeline is still tearing down,
    which is answered from the listener's backlog rather than refused. The
    DPDK case, under the netns runner, is the only test of the linger's own
    poll loop (challenge, `step_authentication`, handshake decode, `Halt`
    over the transport `run_as_primary_dpdk` returned), which no stub
    stands in for; a peer that connects to the lingering endpoint and
    never answers the challenge, or drips its response a byte at a time
    inside each read timeout, or authenticates and then never sends its
    handshake, is dropped at the exchange's deadline (`HALT_NOTICE_BOUND`
    from its accept), on each transport, and a replica dialling after it
    is still answered; on DPDK, `SLOTS` such peers connected at once
    leave a further connection reset, and a replica dialling once their
    deadline has passed is answered (the kernel adapter's deadline
    arithmetic is also a unit test over a loopback pair, ungated; the
    DPDK linger's per-connection deadline and full-array decision are
    pure functions of the phase, the deadline and the clock, unit-tested
    ungated beside the rules below). The
    primary's `/healthz` and `/metrics` answer throughout the linger, and
    on an ordinary shutdown with no stop the primary's health port is
    released when its run returns (the guard stopped after
    `stop_and_join`, not left to a linger);
  - a replica that detects while disconnected: its redial reaches the
    primary inside the linger, the primary stops and halts the other
    replica;
  - a replica stops during a kernel-TCP catch-up and during a DPDK join,
    a snapshot transfer included: the primary stops at the flagged
    handshake or through `keep_last_ack`, and never sends a snapshot to a
    stopped replica;
  - the converse: the primary stops while a replica is mid catch-up and
    mid snapshot transfer, on each transport, the transfer cut both inside
    the body and after the seed but before the post-snapshot `StreamStart`:
    the replica stops on the `Halt` in its transfer with its marker
    written, and on DPDK, where the
    sender's ordinary teardown resets, the `Halt` is read before the link
    goes;
  - a failed marker write (a seam): the node still lingers and tells the
    others, and its run returns the I/O error;
  - a hung marker write. The marker writer takes its fsync through a small
    trait the node is built with, and `melin-test-node` passes one that
    blocks on an `Arc<Barrier>`. On kernel TCP, the primary's shadow
    detects with its fsync blocked: both replicas receive `Halt` within
    `HALT_NOTICE_BOUND` and refuse to restart, and the primary's run
    returns `NodeDiverged` with `Unconfirmed`. The same with a replica
    detecting: the primary stops within the bound. Ungated latch unit
    tests sit beside them: a writer blocked on the barrier, a waiter with
    a short bound returns unsettled, and releasing the barrier yields
    `Written`, read by the exit helper's second look;
  - the documented limit: the detector cut off by the test proxy for
    longer than `HALT_LINGER` stops alone; the others are a quorum and
    may fail over. The test asserts the counted, logged outcome
    (`notice` left `pending`, the linger's `warn!`), not a stop the design
    does not promise.
- Docs: `docs/replication.md` frame tables, the
  `StreamStart`, `Ack` and `Handshake` rows' descriptions (the id names the running
  primary, so a replica knows a reconnect from a restart), "Output
  verification" gaining the cluster-wide stop, the linger and its limit;
  its authentication table and the protocol 9 upgrade note are on `main`
  already (a protocol 10 would need a note of its own). `CHANGELOG.md`
  **Changed**: the protocol 9 entry on `main` (or a protocol 10 one)
  gains the digest, halt, ack and handshake frames;
  `build_pipeline_with_replication` takes a `PipelineId`;
  `decode_primary_message` returns a `PrimaryFrame`, and
  `ReplicaCursors::new` and `record_ack` change signature (all public in
  `melin-transport-core`); a divergence on any node stops the whole
  cluster; more than three raft voters are refused, from the flags or
  from a raft directory's stored membership.
- **Leaves untested:** a primary that sends digests, which is step 5; a
  routing split with asymmetric reachability (the proxy cuts whole
  links); a host reboot during the linger; a reset racing the `Halt` on a
  real network, where only the half-close ordering is pinned (by the
  socket-pair test and the integration cases), not the peer stack's
  behaviour on a reset, which the design no longer relies on.

### Step 5. The primary streams digests; replicas compare and stop

- Journal stage (decision 7):
  - `output_digest_trailer`;
  - `publish_input_batch_to_rings` taking the typed trailer and
    appending it after `finalize_input_batch`;
  - the board handle and the last-forwarded end in `ReplicationState`;
  - `max_journal_batch` reserving `DIGEST_FRAME_LEN`, with its `const`
    assertion;
  - the ring's module doc stating the slot's layout.
- `replication_wire.rs`: `chunk_frames(&[u8])`, an iterator over the
  length-prefixed frames of one ring slot. The test helpers that decode a
  slot as a single `InputBatch` move onto it:
  - `handoff.rs`'s `decode`;
  - `catchup.rs`'s and `join_worker.rs`'s slot decoders;
  - `pipeline_tests.rs`'s ring readers.

  `try_decode_input_batch_into` reads its declared count of entries and
  never looks at trailing bytes. A helper that decodes one batch per slot
  would therefore pass while silently dropping the digest, rather than
  fail.
- Receiver: decode, contiguity, the comparator (`anchored`) against the
  live board read at each iteration and in the waiting push,
  `begin_session` at each session start with its `debug!` line, and the
  `Mismatch` handing over to step 4's notice through
  `SessionExit::Stopped` (decision 9).
- Metrics `verified_from{against="primary"}`,
  `verified_through{against="primary"}`, `checks_total{against="primary"}`,
  `unverified_windows_total`, `chain_breaks_total`,
  `frames_skipped_total`. A replica's endpoint is not the primary's: it is
  built by `spawn_replica_health` through `HealthState::for_replica`
  (`raft.rs:338`), which today takes only the fence state, the raft status
  and `pipeline_healthy`, and it outlives every replica pipeline a resync
  rebuilds. So these metrics go into the node-lifetime handle step 3
  creates for the shadow's series, never into the pipeline's own state,
  which a rebuild would replace behind the endpoint's back.
- Tests:
  - the replica's endpoint (`/metrics` on a replica under load) shows the
    `against="primary"` series growing, and after a snapshot resync
    rebuilds its pipeline the same series keep counting rather than
    vanishing or resetting the counters;
  - `receiver_transport.rs`, with the scripted transport: a replica
    started with `--no-output-digests` behind a primary that advertises
    the output kind and sends `Digest` frames decodes and drops every
    frame, feeds no comparator, never trips, and acks as without them;
  - `pipeline_tests.rs`: `primary_emits_output_digest_after_its_position`,
    modelled on `primary_emits_chain_check_every_interval`, reading each
    slot through `chunk_frames`:
    - a digest frame only ever follows the `InputBatch` frame in that
      batch's slot, never alone and never first;
    - it names a window ending at or below the slot's `end_seq`;
    - frames are in increasing window order and carry the live board's
      `d` and `C` for their window;
    - the digests take no slot: the ring holds exactly one slot per batch
      published, plus the chain checks;
    - the same holds through the rotation flush in `maybe_rotate`;
    - a window that closes during a query-only batch (which publishes
      nothing) goes out in the next batch's slot.

    These run with the response stage lagging the journal stage, and with
    a window closed by the applied cursor ahead of it (the `get` arm, and
    a lapped `get` that sends nothing).
  - `max_journal_batch`: a batch of the bound's full width, for an
    application at `MAX_ENTRY_SIZE` and for one at the smallest entry,
    plus its trailer, fits `replication::CHUNK_SIZE`. The `const`
    assertion pins it at compile time; the test pins the arithmetic the
    assertion relies on;
  - `catchup.rs` and `handoff.rs`: a slot with a trailer is classified by
    its batch. `Covered` drops it whole, and `Next` (with an overlap the
    receiver trims) and `Ahead` forward the trailer with it. A scripted
    receiver after a `Next` overlap takes the digest with no
    `SequenceGap`;
  - `primary_emits_output_digest_after_its_position` again with
    `hash-chain` off, where digests are emitted and no `ChainCheck` is (a
    `cargo nextest run -p melin-transport-core --no-default-features` run,
    filtered to it, added to the chain-off loop beside step 2's: this test
    needs only the journal stage, which is in that crate);
  - `receiver_transport.rs`, with the scripted transport and a scripted
    local board: a digest ahead of the stream position is a `SequenceGap`;
    the first is taken as the anchor and later ones compared; a fault in a
    window the scripted primary never sends a frame for is a `Mismatch` at
    the next frame, which trips the latch, sends the flagged ack and ends
    the session with `SessionExit::Stopped`; a promotion request pending
    when the mismatch arrives is not honoured (the session ends in the
    divergence, and the receiver returns the error); a local window lapped (a small-capacity
    board) breaks the run, counts the gap and re-anchors on the next
    frame, and `verified_through` never moves past the gap; the board read
    in a push waiting for ring space keeps the run unbroken with the input
    ring full; a session whose `StreamStart` carries another build id is
    compared as any other: matching digests advance `verified_through`,
    and a mismatching one is a `Mismatch` that trips the latch and ends in
    `SessionExit::Stopped`, its `Detected` stop carrying the primary's
    build as `peer_build` (the receiver passed it to `follow` at
    `StreamStart`); the same with an all-zero build id; a session
    whose `StreamStart` has no output bit compares nothing, and when a
    digest-sending session follows and anchors, the windows since the
    last run are counted once, all under `unverified{reason="uncompared"}`,
    none under `new_primary`;
  - `receiver_transport.rs`, sessions, each case successive sessions of
    `streaming_loop` over the same `ReplicaPipelineHandles` comparator,
    the scripted primary's `StreamStart` naming the pipeline id the case
    needs (the wire is the one place a test may repeat an id, and it
    stands for a primary that did not restart):
    - the same id, with a catch-up: the first session matches at least
      once; the second resumes from the replica's durable position and
      streams a catch-up of many windows with no digest frame, then one
      frame. That frame is compared, not taken as an anchor:
      `verified_through` advances over every window applied during the
      catch-up and before it, `verified_from` is unchanged, nothing is
      counted unverified and no `Anchored` is returned; a fault in a
      catch-up window makes that same frame a `Mismatch` and
      `SessionExit::Stopped`; a local window closing in the first
      session after its primary's last frame extends the kept run; the
      `debug!` line says the run was kept;
    - the same id, a frame of the first session re-delivered in the
      second (at or below `verified_through`) is dropped, not compared
      and not counted;
    - a new id (a primary restarted at the same address, with the same
      identity, journal and epoch: everything in the `StreamStart` equal
      but the id), its scripted folder restarted from the seed: the
      second session's first frame anchors, no `Mismatch` is ever
      returned, the windows between the runs are counted `new_primary`,
      and the `debug!` line says why; the same with a frame that would
      mismatch the old chain, so a restart can never pass as a
      divergence;
    - a promotion: the second session's primary is another node with its
      own id, which re-anchors exactly as a new id;
    - the same id, with the board lapped during the catch-up (a
      small-capacity board, the scripted drain closing more than its
      capacity between two receiver reads): a `Break`, counted under
      `chain_breaks_total`, `verified_through` never crossing the gap,
      the next frame anchoring anew and the gap counted `gap`, never
      `new_primary`;
    - an uncompared session between two of the same id: the third session
      re-anchors (`uncompared`);
    - a catch-up many times longer than the board, through `streaming_loop`
      over the scripted transport like the cases above, so the board is
      read only at the receiver's own read points rather than at a cadence
      the harness picks between calls to `process_streaming_frames`; the
      input ring never fills here (the cap plus one frame is far below its
      capacity), so this case pins the loop-top read, and the waiting
      push's read is pinned by the full-ring case above; its frames built
      by the journal catch-up's own frame builder (so catch-up-sized, the
      test asserting that none carries more than `S` slots), the board's
      capacity just above `(MAX_SLOTS_PER_CYCLE + S) / K` windows and the
      scripted drain closing every window as soon as its entries are
      published: no `Break`, since between two reads the receiver
      publishes at most the cap plus one frame (decision 5), so a long
      catch-up alone never laps. The run also records, from the input
      ring's cursor at each loop-top read, the largest number of entries
      `P` published between two reads, and asserts `P` exceeds the cap by
      at least two windows' entries (with frames of `S` slots the
      overshoot is `ceil(cap / S)·S - cap`, so the catch-up's entry sizes
      are chosen to give `S > 2K` and `cap mod S` between 1 and
      `S - 2K`: a frame size just under the cap is the simplest that
      qualifies; the overshoot is then real and wide enough to fit a board
      between the two); the same catch-up
      is then replayed against a board sized from `P`, at least
      `MAX_SLOTS_PER_CYCLE / K` windows (so it would hold had the receiver
      stopped at the cap) and one window below `P / K`, which does break,
      so the test pins the frame overshoot rather than passing on a cap
      the receiver does not enforce mid-frame, and does not depend on a
      nominal frame width the builder only bounds;
  - `tcp_receiver.rs` and `dpdk.rs` (the latter under the runner): the
    `Stopped` exit tears the pipeline down, runs the notice and returns the
    error without reaching the reconnect-loop top; a `Disconnected`, a
    `StreamGap` and a `Corrupted` reconnect each keep the pipeline's
    comparator, with its run and held id, against a scripted primary
    reconnected under the same id (the reconnect path is
    transport-specific, so `dpdk.rs`'s case is the runner's); a session
    answered `NeedSnapshot` or `HashMismatch`, and the in-process repair of
    a chain divergence, rebuild the pipeline with a comparator holding no
    run and no id, so a primary with the same id is anchored on, not
    compared against (`unverified{reason="start"}`);
  - integration (`output_divergence.rs`, both transports): primary and two
    replicas healthy under load, checks grow on both replicas, nothing
    diverges, no chain breaks (the test's load is far below decision 5's
    headroom and no drain is stalled); a replica given a fault through its
    `Sizing` (its test app's `prefault` stores "change the report of the
    event that brings my count to X", violating `prefault`'s contract on
    purpose) stops with a marker whose range contains the position of X;
    its flagged ack stops the primary, whose `Halt` stops the other
    replica; every node's marker names its cause (`detected` on the
    replica, `cluster-halted` by that replica on the primary, by the
    primary on the other replica), nothing promotes, and every restart is
    refused. The same with both replicas faulted, the second stop arriving
    while the primary is already stopping. The same with step 3's
    capacity-order app and one replica given a larger `Sizing` than the
    primary, an application that breaks the determinism rule without
    violating `prefault`'s contract: the first event whose reports come
    out in another order stops the cluster. And the mixed-build case, the
    primary started with another build id than its replicas (step 2's
    `ServerConfig::build_id`, which `melin-test-node` can set, the one seam
    that stands in for a second binary): healthy under load, both
    replicas compare it as any other, their checks grow, nothing is
    counted unverified beyond the replicas' start, and each end logs the
    other's build once at `info!` per session; then the same fault on a
    replica stops the whole cluster as above, and every node's marker,
    `error!` line and refusal name both builds (the detecting replica the
    primary's, followed from its `StreamStart`, whether its receiver or
    its shadow tripped first, the primary the replica's as `peer_build`
    from its handshake, the other replica the primary's from its
    `StreamStart`), so a mixed-build cluster reads as the likely cause
    from any one node;
  - integration, reconnect (`output_divergence.rs`, both transports, so
    the DPDK case runs under the runner): a replica behind the test proxy
    (`tests/common/proxy.rs`, bound at `melin_test_node::local_ip` so it
    serves both transports), checks growing; the link is cut while the
    load runs on, long enough that the replica's next session catches up
    from the primary's journal over many windows, then restored. The
    replica reconnects to the same pipeline: its `verified_from` is
    unchanged, its first check after the catch-up moves
    `verified_through` past every catch-up window,
    `unverified_windows_total` and `chain_breaks_total` stay at zero, and
    nothing diverges;
  - integration, no false positives across the transitions of decision 13:
    a replica that joins mid-run (journal catch-up, then live), one
    restarted from its own snapshot, a primary restarted (recovery replay)
    under traffic, and a promotion, each followed by a sustained run of
    checks with nothing diverging, ticks on at a short interval, driven
    by `TickCountingApp` (finding 7), whose reports carry its count of
    every tick call, so a copy that made one call more or fewer across the
    transition would stop the cluster;
    each re-anchors once and counts the windows before its anchor as
    unverified, under the reason the transition implies (`start` for the
    replica's join and its own restart, `new_primary` for the primary's
    restart and the promotion). The primary's restart goes through
    `melin-test-node`'s restart, at the same address with the same node
    identity, journal and epoch, in the same test process: only its newly
    drawn pipeline id tells the replica its chain restarted, so this case
    is the one that shows an id the harness cannot make equal hides no
    restart;
  - integration, idle: a primary with ticks on and no client traffic, the
    replica's checks keep growing, one window per `K` ticks; with ticks
    off, none grows until traffic resumes;
  - the replica-detected stop over DPDK, under the runner.
- Docs: `docs/replication.md`, "Output verification" extended to the
  replica check. `CHANGELOG.md` **Changed**: a replica whose output
  disagrees with its primary's now stops the whole cluster, as step 4's
  shadow check does, whatever build each runs; so an application upgrade
  that changes its output for the same input can no longer roll node by
  node with output digests on (stop every node and start them together on
  the new build, or switch behaviour at a journaled activation point:
  docs/building-an-application.md).
- **Leaves untested:** a chain break end to end (it needs a drain thread
  stalled under report-less traffic past decision 5's headroom, which a
  test cannot arrange without a seam in the drain; the receiver tests with
  a small board, live and during a catch-up, and the comparator's property
  test cover the verdict and the accounting); two primary pipelines
  drawing the same id, which a test cannot cause without a seam in
  `draw`, and `draw` takes none (its consequence, a kept run compared
  against a restarted chain, is a `Mismatch` on the first frame, the path
  the same-id fault case exercises: a needless stop of the cluster, never
  a missed divergence, and at 128 random bits not a case to plan for); a
  primary restarted across a clock step back: the tick generator reads
  the wall clock and has no seam a test can step back, and that copies
  make the same tick calls there is `tick_watermark.rs`'s to pin on
  `main` (finding 7), which the no-false-positive cases above extend to
  the digest under repeated timestamps; a genuine output difference
  between two binaries (the mixed-build case injects the fault through
  `Sizing` under two build ids: the detection path is the same).

### Step 6. Measurement and the operator runbook

- The measurement of decision 15, the hash micro-benchmark and the
  per-window cost included, in a dated document naming its hardware. If
  the response stage's tail moves, open questions 1 and 5 decide; if the
  per-window cost breaks decision 2's bound, open question 9 does.
  The run also reports replication ring slots per published batch (one,
  digests on or off, decision 7) and replica evictions, both with digests
  on and off.
- `docs/replication.md`, "Output verification", completed for operators,
  with no implementation detail:
  - **the guarantee** in the words of "The guarantee, and its limits"
    above, and **the limits**: a detector nobody can reach within the
    linger; manual promotion; latent divergence; the windows before a
    replica's first comparison with a restarted or newly promoted primary,
    or after its own restart or resync, while a reconnect to a running
    primary keeps coverage, catch-up included, unless a chain break is
    counted; the detection delay bounded in entries and so in ticks when
    idle; the unverified tail at failover; queries; nodes without
    snapshots; snapshot handling checked in-process only for the state
    present at the pipeline's start, and not at all under an overridden
    `clone_via_snapshot`;
  - **what an operator sees**: the `error!` lines on every node, naming
    who detected and who told whom, and both builds when they differ;
    the gauges to alert on (`melin_output_diverged`,
    `melin_cluster_halt_notice`, chain breaks, digests off), the run
    gauges, each node's marker, the refused restarts;
  - **consequences**: any divergence, on any node, stops the whole
    cluster: no node answers clients, no node is promoted, and no node
    restarts. Which copy produced the odd output does not matter. When
    the markers name one build, that build broke its determinism
    contract, or the hosts differ beneath it (a system math library
    behind a floating-point function the application calls). When they
    name two, the cluster ran mixed builds that
    answer the same input differently (an output-changing upgrade rolled
    node by node, two architectures, per-host builds), and the operator
    picks the one build the new cluster runs. A divergence that follows
    a node's restore of a snapshot written before snapshot framing
    version 5, with its watermark warning in the log, points first at
    how the application handles a tick for a time already passed
    (finding 7). The second suspect, especially on the first rollout of
    this release, is report order that follows a hash map's capacity (the
    determinism rule in `docs/building-an-application.md` already forbids
    it): nodes sized differently iterate differently, and an application
    that broke the rule unseen until now is stopped from upgrade day
    (assessment 1.3). A hardware fault is possible, and the new cluster
    shows it if the fixed build diverges again;
  - **upgrades** (also in the "Upgrade and rollback notes"): an
    application upgrade that changes its output for the same input cannot
    run mixed with the old build while output digests are on, since the
    first differing report stops the cluster, rightly. Upgrade such a
    build by stopping every node and starting them together on it (they
    replay the same journal on one build), or ship it with a journaled
    activation point (below). An upgrade that keeps the output identical
    can still roll node by node. A Melin runtime upgrade is a
    stop-everything upgrade for replication protocol 9 already;
  - **rolling back past this release** (in the "Upgrade and rollback
    notes", beside "Rolling back across a promotion…"): a release before
    this one does not know the marker, and would start a stopped node
    whose snapshot and journal it can read, or one whose v5 snapshot the
    rollback procedure set aside. Before rolling a node back, check for
    `<journal>.diverged`: a node with one must not be started on any
    release, and the cluster comes back only as a new one (below). No
    refusal file older releases would honour is possible (assessment
    3.4);
  - **what overrides the stop**, in one place: a manual `PROMOTE` of a
    node that never heard the halt; observational raft steering an
    operator to `PROMOTE` its leader, which may be the unaware node;
    voter replacement, which looks like the fix for "two nodes refuse to
    start" and is not; and a forced `ACK-POLICY disk` on a node whose
    marker never landed (a hung disk at the stop: its log says "marker not
    confirmed", and its peers refuse to start). The refusal text points
    here;
  - **starting a new cluster**, the only way back, tested (below):
    1. Stop every node, lingering ones included.
    2. Move each node's marker aside, kept as the incident's record, and
       set every node's snapshots aside: the broken build wrote them all.
       Rolling the stopped nodes back to an older release is not a way
       around this procedure: it ignores the marker (rolling back,
       above).
    3. Pick the node whose journal reaches the highest position *and*
       goes back to sequence 1. With snapshots set aside, recovery needs
       the whole history and refuses with `MissingHistoryPrefix`
       otherwise. A replica seeded by snapshot transfer holds only the
       segments from its snapshot on. If no node reaches sequence 1, copy
       the missing early segments from any node or archive that has them:
       a healthy replica's segments are the primary's byte for byte, and
       the segment chain check verifies the splice. Only if no copy
       survives anywhere is a snapshot the broken build wrote the starting
       point, knowingly.
    4. Replay it once with the fixed build (after a mixed-build stop, the
       one build every node will now run; or, on the same build, with
       `--no-output-digests`, which is not a recovery: the replay is one
       more draw of the nondeterminism, and the docs say so), standalone,
       so its shadow writes a snapshot from the fixed build.
    5. Give every node a fresh `--raft-dir` and the other nodes clean data
       directories, and start the cluster as on first deployment; the
       replicas reseed from the new primary.

    The history clients were served and the fixed build's replay may
    differ: the journal is the audit trail of what was asked, and the
    replay is the state from now on. A cluster that ever failed over has
    journaled epochs above the fresh raft terms, and auto-promotion
    refuses while the term is at or below the epoch: the new cluster has
    manual failover only until enough elections have passed (a roadmap
    item, step 7, which also fixes voter replacement);
  - the `--no-output-digests` switch, what turning it off gives up, and
    why the window is not configurable (it is part of the digest's
    definition, the same on every node).
- `docs/building-an-application.md`, "Snapshots and upgrades", gains the
  upgrade rule above for application authors, and the **journaled
  activation point** as a pattern: the new build ships both behaviours,
  keeps the old one until it applies an entry that switches it (an
  operator command the application journals, either switching at once
  or naming a later position at which to switch, that position then
  being part of the journaled state), and keeps the switch in its state
  and its snapshot. Old and new builds then produce identical output until
  every node runs the new one, the switch lands at one position on every
  copy, and replay from any point takes the same branch. The pattern's
  rules: the activation must be an applied entry, never the wall clock or
  a node's configuration (both differ by node); old builds must never
  see it (activate only once every node runs the new build, since an old
  build would not switch, and diverge, or fail to decode the entry); once
  activated, there is no rolling back to the old build, which would
  replay the activating entry without switching and diverge (going back
  means a new release that keeps the new behaviour, or a new cluster);
  and the old path stays
  until a later release, once no journal still replayed predates the
  activation. The production checklist gains a line for it.
- `docs/internal/application-api-review-2026-09.md` S8 points here; the
  pipeline architecture doc's responsibilities table names the folders.
- Tests: the new-cluster procedure as an integration test
  (`output_divergence.rs`), so the runbook's steps are tested rather than
  prose. A three-node cluster is stopped by a fault through `Sizing` and
  then rebuilt, with the fault switched off as an operator restarts on a
  fixed build:
  - every marker and snapshot is moved aside, and each raft dir is fresh;
  - the new primary replays from genesis, and its state equals a fresh
    replay of the same journal;
  - the replicas reseed and the cluster serves;
  - for a history that once failed over, auto-promotion refuses with the
    term-below-epoch reason.

  A second case starts from a node whose journal does not reach sequence
  1: recovery refuses with `MissingHistoryPrefix` until the early segments
  are copied in.
- **Leaves untested:** the runbook's prose itself; a hardware fault; a
  rollback to an older release, whose binary the suite does not build.

### Step 7. Roadmap: five follow-ups

Add five rows to `docs/internal/roadmap.md` ("Next"), roughly, the first
listed first:

> **Offline output qualification** (Medium, next after output digests).
> A stopped cluster's marker names the first window whose outputs
> differed. Investigating means reproducing it, and qualifying a build
> means knowing whether it will diverge before it reaches production. A
> tool replays a journal, which is input-only and checked by its chain, on
> two copies of the application. Each copy is folded as a node folds, one
> copy can be taken through `snapshot`/`restore` as the shadow is, and the
> tool reports *every* window whose outputs differ, not just the first.
> No client is answered, so it can run past a divergence. This is the
> debugging path an observe mode would otherwise have been, without
> serving on a broken contract. It reuses the folder and the comparator
> as they are. It also replays from genesis with the fixed build for the
> new-cluster procedure, so it can provision that step too. Run once per
> build over the same journal, with their window digests compared, it
> tells an operator whether an application upgrade keeps output identical,
> and so may roll node by node, or must go through a full stop or a
> journaled activation point. Plan:
> [divergence-detection-2026-10.md](divergence-detection-2026-10.md).

> **Application state digest** (Medium). Output digests catch a divergence
> only once a report shows it. Two copies whose state differs where no
> report has yet looked (latent divergence) are both verified, and the
> drifted one may be promoted at a failover, after which its reports
> differ from what the old primary's would have been, and the cluster
> stops on the first one. A digest of application state, chained at window
> boundaries as output digests are, closes that. It plugs into what output
> digests built: a new kind value on the same digest frame and
> `StreamStart` kind bits (no replication protocol bump), the same 16-byte
> values, the same board and comparator, the same stop. The new surface is
> the application's: an opt-in digest function whose contract is the
> logical state, independent of capacity and iteration order. An
> application that already keeps an incremental state hash exposes it
> directly, reduced to 16 bytes by the kind's definition if it is wider. Plan:
> [divergence-detection-2026-10.md](divergence-detection-2026-10.md).

> **Output chain stored in snapshots** (Medium). Each copy's output chain
> starts when its pipeline starts. A replica keeps its comparison across a
> reconnect to the same running primary, catch-up included, but a primary
> restart or a promotion starts a new chain, and so does a replica's own
> restart or resync: the windows from the last comparison to the first
> one after such a transition are verified by the replica's own shadow
> but not against the primary. Storing the chain value in each snapshot
> lets a copy restored from one continue a single whole-history chain, so
> every copy's chain is the same value at the same window and those
> windows are verified too, the primary's restarts and promotions
> included. It needs snapshots taken at window ends,
> replay folding the windows between the snapshot and the live start, a
> snapshot header change, and an agreed chain origin (genesis). It builds
> on the window grid, frames, board and comparator of output digests
> without changing them: a replica whose chain is known to share the
> primary's origin compares chain values directly instead of anchoring.
> Plan: [divergence-detection-2026-10.md](divergence-detection-2026-10.md).

> **A stopped node that holds its notice until delivered** (Low). A node
> stopped for divergence tells the others on the replication links and
> lingers for a minute to reach one that was between sessions. A detector
> cut off for longer stops alone, and the others may fail over. Keeping
> the stopped node in a handshake-only mode until its notice is
> delivered, across restarts (the marker records it pending), closes all
> but a host reboot. Worth it only if the linger's limit is seen in
> practice. Plan: [divergence-detection-2026-10.md](divergence-detection-2026-10.md).

> **A raft term floor for a fresh raft cluster** (Medium). Auto-promotion
> refuses while the raft term is at or below the journal's fencing epoch.
> A raft cluster started fresh on an existing journal (a new cluster after
> a divergence, or a voter replaced with a wiped raft dir) restarts terms
> from zero, so a history that ever failed over gets manual failover only
> until enough elections have passed. Raise a fresh raft cluster's terms
> above the highest journal epoch its peers advertise.

- **Leaves untested:** five roadmap rows.

## What this leaves untested, and why

Beyond each step's own list:

- **Hardware faults** (a bit flip in live memory) are modelled only by the
  fault-injecting apps; the detection path is the same.
- **The partition limit** is tested only as the documented outcome (step
  4): the cut-off detector stops alone and says so. Nothing asserts what
  the others do, since the design does not promise it.
- **Mixed builds** are covered by step 5's receiver tests and its
  mixed-build integration case (compared like any pair, and named on
  every node when the stop fires), which set the build id through
  configuration, not by running two binaries: the hash of the executable
  itself is read by one function, tested on the test binary (step 2). A
  real output difference between two binaries, or between two
  architectures, is not produced; the injected fault stands in for it,
  and nothing on the path from the mismatch on depends on its cause.
- **The fold's cost** is measured, not tested: a wall-clock threshold in a
  test flakes.
- **A non-default `--ring-capacity` end to end.** The board's and the
  comparator's sizing is one function of the `RingCapacity`, unit-tested
  at the minimum, the default and above it (step 1), and the builders
  call only that function. No cluster runs at a capacity above the
  default: its rings alone would take several times the default's memory
  per node, and the sizing is what such a run would test.
- **The idle delay in seconds** is not asserted: step 5's idle test counts
  checks per ticks, since a wall-clock bound flakes; the bound in ticks is
  what the docs state.

## Open questions and trade-offs

1. **The hash.** XXH3-128 through `twox-hash` (recommended, decision 1)
   or BLAKE3 (already in the tree, but a cryptographic hash paying a serial
   block compression for every 64 bytes folded, for a guarantee this check
   does not need). Collisions are accidental, not adversarial, so either
   is adequate; the difference is cost on the response thread.
   Recommendation: XXH3-128, for both the window digest and the chain step,
   confirmed by step 6's micro-benchmark. If a crate outside `deny.toml`'s
   allow list were ever needed instead (`xxhash-rust` is BSL-1.0, the
   Boost Software License), its allow entry must say in a comment that it
   is Boost and not BUSL-1.1, the Business Source License the file
   reserves for our own crates. The frames fix the values at 16 bytes for
   every kind (decision 8), which suits any later digest compared between
   copies, a state digest included: its collisions are just as accidental.
   An application hash wider than that (a state hash of its own) is
   reduced to 16 bytes by its kind's definition, and a cryptographic hash
   truncated to 128 bits still leaves accidental collisions out of reach.
   A digest that must be tamper-evident to a third party is a different
   job: it needs a wider field, so a new frame and a protocol bump, and is
   not what these frames are for.
2. **Comparing across builds. Decided (2026-11-04): always compare,
   whatever the builds.** A rolling upgrade that changes a report's
   layout, or the encoder, makes a mixed pair disagree by design, and under
   this plan the cluster stops. The options were: (a) document that such
   upgrades run with digests off on the primary; (b) compare only when
   both ends run the same build, identified by a build id in
   `StreamStart`, and otherwise compare nothing; (c) an
   application-declared output version, rejected because it is the trait
   surface this plan avoids; (d) compare always. (d) is the decision. A
   replica on another build whose output differs for the same input has
   diverged: its state no longer matches the primary's, and a failover
   would show clients another history. Skipping the comparison hides
   exactly that, and (b) would have hidden it silently in two cases an
   operator does not think of as an upgrade: a cluster across
   architectures (a floating-point transcendental function can round
   differently on x86 and on ARM), and binaries built per host without
   reproducible-build flags, whose hashes differ although the source is
   the same, so such a cluster would never have compared at all. The
   consequence is an upgrade constraint, stated with the guarantee and in
   the operator docs (step 6): an upgrade that changes output cannot run
   mixed, and is done by stopping every node, or behind a journaled
   activation point.

   The build id stays, for diagnosis only: a BLAKE3 hash of the running
   executable, read once at startup, in `server-runtime`, which already
   depends on `blake3` unconditionally (it stays BLAKE3 and 32 bytes,
   since it is computed once, off every hot path, so decision 1's cost
   argument does not reach it). A primary sends it in `StreamStart` and a
   replica in its `Handshake` (decision 8), and the stop's `error!` line,
   its marker and the refusal message name both builds when they differ
   (decisions 11 and 12), so a mixed-build cluster is plain as the cause.
   Nothing compares, skips or refuses on it, so it needs no override for
   builds an operator knows to be equivalent: equivalent builds compare
   equal by their output. A node that cannot read its own executable
   sends an all-zero id, printed as an unknown build, and logs `warn!` at
   startup (decision 16); it is compared like any other.
3. **Does the promotion gate need tightening for the stop?** Settled by
   `main`, no change needed. The concern was a raft-leading replica whose
   primary link had been down about 2.5 s when the primary stopped: its
   last majority ack, from before the stop, is still recent when its
   grace ends, so an age-only check passes it. `main`'s lease check is
   age-only, but it is only the early filter. The promotion is confirmed
   by a fresh majority round trip that starts after the decision, so
   after the stop, before its epoch is journaled (decision 14). Stopped
   peers never answer it, so the promotion is abandoned. The case that
   remains, both replicas between sessions when the primary stops, is
   the documented limit, as before.
4. **Clearing the marker. Decided: the file is moved aside** (decision
   12, and step 2 of the new-cluster procedure in step 6), not an admin
   command, and kept as the incident's record. It is an explicit act, it
   works when the node will not start (which is exactly when an admin
   socket is not there), and the refusal says not to do it to bring the
   node back.
5. **Where the primary folds.** In the response stage after the send
   (recommended: no new thread, the bytes are already encoded there), or in
   a dedicated output-ring consumer, the same code as the replica's drain,
   which takes the fold off the response thread entirely at the cost of a
   core and a second encode. Recommendation: the response stage, and the
   dedicated consumer only if step 6 shows the response thread's tail move.
6. **Closing windows downstream of the output ring. Decided: the applied
   cursor** (decision 4): one store per matching batch, no new payload.
   The alternative, a marker slot the matching stage publishes at each
   window end, costs a per-event compare on the matching thread and a new
   `OutputPayload` variant, breaking for publishers, which the gate must
   also pass without waiting.
7. **A stop that does not finish.** The stop exits through the node's
   ordinary shutdown, which fenced nodes already take. If a thread never
   joined, the exit helper never runs the linger, so a stopped primary
   would not answer handshakes with `Halt`; its raft driver still leaves
   on the shutdown flag on its own. Recommendation: no watchdog in this
   plan; the fenced-shutdown path is the same one superseded primaries
   rely on, and a hang there is a bug for both. The stop's own waits are
   bounded by the notice bound: every `Halt` write and drain, and the
   link-owning threads' wait on the marker, which a `divergence-marker`
   thread writes beside them (decision 11). A hung marker fsync is bounded
   like every other wait before the notice: the notice goes out without
   the marker, and the exit returns `Unconfirmed`. Unlike a hung journal,
   which stalls the cluster and, once killed, fails over onto a good build,
   a hung stop that is killed would otherwise fail the cluster over onto
   a build already seen to diverge, with no marker anywhere and no `Halt`
   sent. That is why this wait is bounded and the journal's is not. The
   residue: a node whose marker never landed can restart once its disk
   recovers. Its peers have stopped, so it has no quorum to rejoin and
   cannot be promoted. A primary restarted that way has no replica
   streaming, so it halts client writes under every ack policy (`halt.rs`,
   "Lifted by the operator's consent"). It still answers queries from the
   broken build's state, and an operator's forced `ACK-POLICY disk` would
   let it take writes. Its two replicas' refusals and the `error!` lines
   are the operator's signal (step 6).
8. **Ordering against monotonic sequencer time. Settled by `main`
   (merge 87e36302): no ordering.** The options were (a) make the
   monotonic-time plan a build prerequisite of step 3; (b) land steps 1
   to 5 in any order against it, but release step 5 only with or after
   it; (c) release independently and rely on the contract and the
   runbook. (b) was recommended because step 5 is the first comparison
   across a restart, and with the tick watermark restarting at 0 at every
   stage build, an application's repeated-tick handling would have been
   the likeliest cause of a cluster-wide stop. The watermark is now
   carried across restart, promotion, snapshot restore and resync
   (finding 7), so copies applying the same journal make the same tick
   calls, and the trap step 5 would have sprung is gone. What remains of
   monotonic time (time going backwards, the double call per journaled
   `Tick`, the producer race) reaches every copy alike and cannot split
   them, so it is independent of this plan: neither gates the other's
   build or release. The one residue, a snapshot written before framing
   version 5, is named in the runbook (step 6).
9. **The window, `K`.** 64 (recommended, decision 2), 16, or 256 and up.
   Below 64 the per-window work becomes a visible share of the event
   budget on the response thread, the drain and the receiver, and the
   board and comparator grow in proportion (`R` quadruples at 16), for a
   delay under load already dominated by the fsync batch rather than by
   the window. Above it, the cost
   shrinks further but the idle detection delay grows in step (256 entries
   is about a minute at the default tick interval), and idle is exactly
   when an operator restarts or fails over and wants the check current.
   Recommendation: 64, confirmed by step 6. Because `K` is part of kind 1's
   definition, changing it after release is a new kind value, which mixed
   builds handle as an unknown kind, not as a divergence; before release
   it is a constant to edit.
10. **Does the digest frame carry `d_e` beside `C_e`? Decided: yes**, as
    decision 8's frame layout does: (a) 16 bytes more per frame, rather
    than (b) the chain value alone. With (a), the
    anchor's own window is checked rather than taken on trust, and a
    mismatch can say whether the window it was found at differs itself,
    which narrows the marker's range for the operator; the cost is 16
    bytes on a frame sent at most once per fsync batch. With (b), a
    mismatch only says the divergence lies somewhere since the last match.
11. **Chain breaks on a replica's board.** Decision 5 leaves one lap a
    replica's comparator cannot absorb: a drain stalled while about `R·K`
    entries of report-less traffic were applied, which the rings do not
    bound, most likely during a catch-up. With runs kept across a reconnect
    (decision 3), it is also the one way a reconnect to a running primary
    still loses coverage. (a) Keep it a counted chain break (recommended);
    (b) gate the
    drain on a read cursor the receiver publishes, so it never closes a
    window more than `R` ahead of the last one copied; (c) extend the
    replica's chain on the drain thread: the receiver hands each run's
    anchor and its expected entries to the drain over a queue, the drain
    writes `C'` to the board beside `C` as it closes each window, and a
    lapped window then costs a sample, not the run. (b) makes laps
    impossible but couples the drain to the receiver, which can sit in a
    socket read with the primary idle, and reads nothing between sessions;
    the drain would then hold the output ring, and through it the matching
    stage, on the receiver. (c) removes
    the class without backpressure, but moves the comparator off the
    receiver, adds a queue and makes a session start cross threads, and the
    drain must still hold its recent windows for an anchor that arrives
    after it closed the next one (the primary's frame trails the entries it
    covers). Recommendation: (a) for this plan. A break never produces a
    false claim, only an unverified gap that is counted, logged and
    alerted on, and its trigger is a stall the replica's ack latency would
    show first. Step 6 reports the break count under report-less load and
    across a reconnect with a long catch-up; any break seen outside a
    forced stall makes (c) the follow-up.
12. **Upgrading to protocol 9. Decided (2026-11-04): a flag day, with
    the version check moved into authentication.** (a) Stop every node,
    as for protocols 6 to 8; (b) negotiate N/N−1 in protocol 9; (c) the
    whole zero-downtime path. (a) costs no extra stop when it ships with
    raft protocol 1, and (b) would not spare this upgrade one: a v8 node
    accepts only v8, and raft links refuse a peer on another release.
    The one part only this release can add is a v9 primary accepting a
    newer replica: the replica's version in `ChallengeResponse`, the
    session's (the primary's) in `AuthOk`, one version each way, no
    ranges; the primary accepts a replica at or above its own version,
    and the replica decides whether it can speak the primary's (decision
    8). Raft protocol 1 needs the same rule before it ships, noted in
    decision 8 and not scheduled. Rolling upgrades are not supported, so
    (c) is not planned. The version check at authentication landed on
    `main` on 2026-11-05, ahead of this plan (decision 8).

## Not part of this plan

- An application state digest, and a chain stored in snapshots (step 7
  records both on the roadmap).
- Any automatic repair, retry or resync of a diverged node.
- Journaling digests, or carrying anything new in snapshot headers (both
  formats stay untouched).
- Digests on the event publisher's feed: a feed subscriber runs no
  application and produces no reports, so it has nothing to compare an
  output digest against. A state digest (step 7) is the kind a subscriber
  could check, and would be the time to revisit this.
- Replica-to-replica comparison, majority verdicts, and any attribution
  of a divergence to one copy.
- An observe mode that detects and logs without stopping. It would mean
  serving on a contract known to be broken, and once turned on to get
  past one stop it is easily left on. The switch is on or off
  (`--no-output-digests`), and debugging goes through the offline tool
  (step 7).
- Raft carrying the stop (a log entry, a forwarding RPC, a persisted
  halted state). It would reach a node that missed the data-plane notice,
  which only the documented partition case needs, at the cost of the first
  application data in raft and a raft storage version. Rejected for this
  plan; revisit only if that limit is seen in practice.
- Making `apply` deterministic: this plan detects nondeterminism; the
  determinism work removes sources of it. "Monotonic sequencer time" no
  longer gates this plan: the part that split copies, the tick watermark,
  is fixed on `main`, and the rest is independent (finding 7, open
  question 8).
- Skipping the comparison for a peer on another build, or an override
  declaring two builds equivalent: copies are compared whatever their
  builds, and the build id only names them (open question 2).
- A `melin_output_comparing` gauge (1 while a compared session is live).
  With every build compared, "not comparing" now means only a primary
  with digests off or no replica connected, both already visible
  (`melin_output_digests_enabled`, the replica gauges, the "sends no
  output digests" `warn!`, `verified_through` standing still). An
  optional follow-up, not part of these steps.
