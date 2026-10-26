# Divergence detection (plan)

Status: **proposal**. Implements item S8 of the
[application API review](application-api-review-2026-09.md), "No divergence
detection for a nondeterministic `apply`".

The one-line goal: **every copy of the application digests the reports it
produced, window by window, and a node whose digest disagrees with the
reference stops serving, for good, until an operator brings it back.** The
hash chain proves that every node holds the same *input*. Nothing proves
that the same input produced the same *output*. A nondeterministic `apply`
(a randomly seeded `HashMap` iterated, the wall clock read, an allocation
address hashed), a `snapshot`/`restore` pair that drops a field, or a
memory error leaves every journal bitwise identical while the copies
answer differently, silently, until a failover serves the drifted copy.

What is digested is the **output**: the bytes of every report the
application's encoder produces for a journaled event. It is computed once,
in the runtime, for every application. There is no trait to implement and
no opt-in, and it is on unless the operator turns it off. A divergence means
the application broke its determinism contract, so there is no repair: the
node that sees it stops and refuses to start again until an operator has
looked. One clause of that contract is a trap the runtime still sets, the
tick rule, and finding 7 takes a position on it.

The journal stays input-only and the snapshot format is untouched. The
digest rides the replication stream as a control frame, as the chain check
does.

## The guarantee, and its limit

Stated here as the operator docs will state it (step 6):

- **A node never serves once its output has been seen to disagree with the
  output clients were served.** A replica whose reports differ from its
  primary's, or a node whose snapshot-making copy (the shadow) answers
  differently from its live copy, stops: it sends no further reply, takes no
  further client, cannot be promoted, and refuses to restart until an
  operator clears it.
- **Every copy still running produced the same client-visible reports, byte
  for byte, as the copy clients were answered from, for every compared
  window up to its verified position `N`** (`/healthz`, `/metrics`).
- **The limit: divergence that has not yet shown in any report is not
  caught.** Two copies can hold different state and still answer every
  request identically, for example a balance that differs only on an
  account nobody has touched since. That latent divergence surfaces the
  first time a report depends on it, which may be after a failover has
  promoted the drifted copy. Positions after `N` (the last window not yet
  compared when a primary stops) are not verified either. Query responses
  are not covered: queries are not journaled, so no other copy answers them.
  Closing the latent case needs a digest of application state, which is a
  roadmap item (step 7).

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
  Any frame-layout change bumps `REPL_PROTOCOL_VERSION` (8 today), which the
  primary checks in the replica's handshake (`decode_replica_message`), so a
  mixed pair fails with the version error. An unknown tag mid-stream decodes
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
  dropped as re-delivery, one ahead is a `FrameError::SequenceGap`.
- **Repair is not reused.** A chain divergence is repaired by snapshot
  resync (`handle_session_exit`, `MAX_INPROCESS_DIVERGENCE_RESYNCS`,
  `archive_local_lineage`). An output divergence is not a damaged journal
  but a broken contract, so this plan takes none of that path. What it
  reuses is the archive naming (`replication/archive.rs`: `archive_dir`,
  `lowest_free_index`, the exhaustive `ArchiveReason`).

### Where reports are produced and where they go, on each copy

- **One producer of reports.** Live matching, journal replay and the shadow
  all hand events to the application through `dispatch::dispatch`, which
  fills a `Vec<A::Report>`.
- **The matching stage publishes every report, whatever its connection**
  (`MatchingStage::run`): one `OutputPayload::Report` slot per report,
  stamped with the entry's `wire_seq`, including those of ticks and other
  node events (`connection_id == 0`). An entry with no report publishes a
  dataless `OutputPayload::BatchEnd` slot **only if its connection is not
  0**, so on a primary a client request with no report gets one, and on a
  replica, where every slot's connection is 0, nothing does. Ack-policy
  changes get their own slot on every copy. Queries take no sequence and
  publish an `OutputPayload::QueryResponse`, on the primary only (they are
  not journaled).
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
  `frame_app_response` (`server-runtime/src/response_frame.rs`) bounds it by
  `MAX_RESPONSE_BODY` and writes the header. Nothing in the encoder's
  contract says today that its bytes depend only on the report.

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
longer encode, skip or re-encode anything (decision 3).

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
(decision 3). The input ring's progress counter will not do: it counts ring
slots, queries included.

**4. On a replica the cost lands on the drain thread, not the matching
thread.** The drain consumer is a thread of its own, so encoding and folding
there does not lengthen `apply`. It does take part in backpressure: a drain
that falls behind fills the output ring, which stops the matching stage,
which fills the input ring, which stops the receiver, whose in-memory ack is
what a `ram` or `disk+ram` primary waits on. So the drain must keep up with
the matching stage at full rate, which the measurement step checks
(decision 14). The shadow is a terminal consumer of the input ring too, so
the same holds for it, on the reader's side.

**5. The replica must buffer in both directions.** On the primary the digest
for a window exists only once the response stage has folded the window, and
the response stage releases a slot only once the ack policy holds it, so
under a replica-backed policy only after the replica has acknowledged it.
The replica's own digest is usually ready first, and has to wait. But the
replica's matching stage can also trail its receiver, so the primary's
digest can arrive first too. The comparator holds both sides (decision 5).
The primary still orders its frame behind the entries it covers, from the
journal stage, for the reasons it did before: a digest ahead of the stream
becomes a contiguity violation, catch-up stays quiet, the wire stays
ordered (decision 6).

**6. Raft needs nothing new.** A diverged node latches the fence before
anything else (decision 10). From then until it exits, the fenced inputs
already make it refuse auto-promotion, stand down from elections and never
challenge, and the readers close every client. Every place a replica honours
a promotion request (the in-session poll in `streaming_loop`, each
receiver's reconnect-loop top) checks the shutdown flag first, which the
divergence sets with the latch. If a request still slipped through that
check-then-check window, the primary pipeline it builds shares the node's
`FenceState`, so its readers close every client and its stages stop on the
shutdown flag. After the exit, the marker keeps the node from restarting
(decision 11), so it can neither rejoin raft nor be elected. A raft frame
version, a divergent bit or a challenger arm would add nothing. The one
thing to pin is that a diverged raft *leader* exits, so peers elect: an
integration test in step 3.

**7. The runtime still hands applications repeated and backwards ticks.**
Determinism audit finding 17 and the roadmap item "Monotonic sequencer time"
([monotonic-sequencer-time-2026-09.md](monotonic-sequencer-time-2026-09.md),
not started) are open: the dispatch watermark is per-process state, starting
at 0 in the live matching stage at boot (after replay), in replay and on
restore, and every journaled `Tick` calls `Application::tick` twice
(`dispatch.rs`). So a primary restarted after a clock step back calls `tick`
for event timestamps a replica that kept running skips. An application whose
`tick` is not idempotent against a time already passed (the roadmap names
the Exchange Core's, which stamps its rate limiter's clock) then holds
different state on the two copies, and once a report shows it, every
replica stops. The plan's position:

- **The stop is right when it fires.** The copies really do differ, which is
  exactly what the product promises cannot happen silently; suppressing the
  check around restarts would hide a real divergence. The rule is already in
  the contract: `Application::tick`'s rustdoc says "a call for a time
  already passed must change nothing", and "The determinism rules" in
  `docs/building-an-application.md` covers `tick`. Step 1 makes the
  guide name the routine causes (the double call, a restart or restore
  across a clock step back) beside the encoder clause, and step 6's runbook
  names this as the first suspect when a divergence follows a restart, a
  failover or a clock step.
- **Monotonic time is not a build prerequisite, but it is a release one for
  step 5.** Steps 1 to 4 cannot trip on it: the shadow and the live stage of
  one pipeline start their watermarks together and see the same events
  (`shadow.rs`, `pipeline.rs`), so the in-process check of step 3 compares
  like with like. Step 5 is the first to compare a restarted primary with a
  replica that kept running. It can be committed and tested before
  monotonic time lands, but should not reach a release ahead of it: until
  then, the first divergence an operator sees is likelier to be this trap
  than their own bug (open question 8).
- **The test applications follow the rule.** `TestApp::tick` counts every
  call (`test_support.rs`), breaking it, but `TestReport` does not carry
  `ticks`, so no report shows the difference today. Step 5's no-false-
  positive tests across a restart and a recovery must exercise the rule, not
  dodge it: they run a test application whose `tick` is idempotent against
  a clock it keeps in its snapshot (the audit's fix direction for finding
  17) and whose reports carry that clock, so time-driven state reaches the
  digest. It sits beside the fault-injecting apps, in the test crate.
  `TestApp` stays as it is: the dispatch tests count its calls on purpose.

## Design decisions

### 1. What is digested

For each window `W` (decision 2), on every copy:

```text
digest(W) = XXH3-128 (seed 0, default secret) over
    "melin output digest v1" || kind (u8 = 1, output)
        || window_len K (u64 LE) || window_end (u64 LE)
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
- **The encoder's contract gains a clause.** `ResponseEncoder::encode_report`'s
  rustdoc and `docs/building-an-application.md` ("The determinism rules")
  say that the body must be a pure function of the report: no clock, no
  node identity, no state of the encoder's own. An encoder that breaks it
  now stops replicas, so it goes under `CHANGELOG.md` **Changed**.
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
  estimate (decision 14, open question 1).
- **The crate and its API.** `twox-hash` 2.x (MIT, on `deny.toml`'s allow
  list), through its streaming `xxhash3_128::RawHasher` over
  `SecretBuffer::default()`, since a window is fed one report at a time:
  `write` per piece, `finish_128` at the window's end. Neither hasher has a
  reset, so a window starts with a fresh one. A fresh
  `RawHasher::new(SecretBuffer::default())`, assigned over the old one, is
  a plain value with its stripe buffer inline and the default secret by
  `'static` reference: no heap, built once per window. Not the crate's
  `Hasher`, whose constructor boxes a copy of the secret, so starting each
  window with one would allocate, and nothing on the response thread may.
  Seed 0 with the default secret is the reference XXH3-128, so the pinned
  vector of step 1 can be cross-checked against the reference
  implementation (`xxh128sum`). The digest's bytes are
  `finish_128().to_be_bytes()`, the reference's canonical form (high half
  first, big-endian), so the marker's hex reads the same as that tool's
  output. `Digest` is a `[u8; 16]` newtype rather than a `u128`: the
  frames, the board payload and the marker carry bytes, a byte array has
  no byte-order question left once built, and its alignment of 1 keeps the
  board's payload free of padding (a `u128` beside the `u64` window end
  would align it to 16 and pad it, failing `NoPadding`).
- **The domain string and the kind** make the definition explicit. Changing
  the algorithm, the string or the layout later is a new definition, sent
  under a new kind value (decision 7), so a replica that does not know it
  skips it and counts it rather than reporting a divergence: a
  wire-visible change, never a silent one. `transport-core`, where the
  fold lives, takes `twox-hash` as an ordinary dependency.
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

### 2. Windows, the interval K, and keeping K identical

- **Windows are absolute.** Window `m` holds positions `(m-1)·K + 1 ..= m·K`
  and is named by its end `m·K`. A window is digested on its own: there is
  no running chain across windows, so nothing has to be carried across a
  restart, a snapshot or a promotion, and the snapshot format does not
  change.
- **A copy digests only windows it saw whole.** A folder starting at
  position `s` (the first position its pipeline applies live) digests first
  the window ending at `(ceil((s-1)/K) + 1)·K`, the first that begins at or
  after `s`. Replay is not folded. So a restarted, restored, caught-up or
  promoted copy simply starts comparing at its next full window, and a
  partial window is never compared.
- **Where K comes from.** `--output-digest-interval` (0 disables), default
  `DEFAULT_OUTPUT_DIGEST_INTERVAL = 65536`, at least
  `MIN_OUTPUT_DIGEST_INTERVAL = 4096` (a memory bound on the buffers below,
  not a cost bound: the per-window work is one finalize and one small frame).
  A value under the minimum, other than 0, is refused at startup with an
  error naming it. The primary advertises its K on `StreamStart`, and **a
  replica adopts it for the session**, for its drain folder and its shadow
  alike. An operator who sets different values on different nodes has only
  set what each uses *when primary*. This follows the roadmap's "Replicas
  pull configuration from primary": a flag kept identical by hand is a
  known source of silent divergence.
- **Realigning on a change of K.** The folders read the adopted K from an
  atomic once per batch. On a change, a folder drops its open window and
  starts at the first new-K window beginning after the last position it has
  seen, and publishes the schedule `(K, first_end)` to its board before its
  first digest under it, so a comparator never guesses where a schedule
  starts. Positions are contiguous across a replica's sessions (the
  reconnect resumes after what it holds), so a window is never folded from
  two histories. A replica that has not yet had a `StreamStart` folds
  nothing.
- **Disabled is visible.** With K = 0 on a primary its replicas fold nothing
  either. `/healthz` and `/metrics` show the interval in force (0 included)
  on every node, the node logs it at startup, and a replica whose primary
  advertises 0 logs it once per session (decision 15).

### 3. Where each copy folds: one folder, three call sites

`transport-core`'s new `output_digest` module holds `WindowFolder`: it
encodes a report's body through the application's `ResponseEncoder` into
the caller's buffer, folds `(wire_seq, body)` into the open window,
closes windows as positions pass, and publishes each closed window to a
`DigestBoard` (decision 4). The body encode itself (the encoder's call and
the `MAX_RESPONSE_BODY` check) moves from `response_frame.rs` into one free
function there, `encode_report_body`, which the folder calls and
`frame_app_response` takes as its body closure, so the bound a client's
frame obeys and the bound the fold folds are one statement. Early replies
call it too, without a folder, so a halt's rejection is framed as today and
folded nowhere. `server-runtime` keeps re-exporting `MAX_RESPONSE_BODY`
from where it is today. Three call sites, each with one folder:

| Copy | Thread | Closes a window when |
| --- | --- | --- |
| Primary, live | response stage (`response_loop::run`, so both transports) | a `Report` slot past the window is read, or the output ring is empty and the matching stage has applied past the window's end |
| Replica, live | the drain thread, hoisted out of `replication/mod.rs` into a function with tests | same rule as the primary |
| Shadow, primary and replica | shadow stage (`shadow::run`) | it has dispatched the window's last position: it sees every position |

- **The matching stage publishes its applied position.** One `Release` store
  of `next_wire_seq - 1` per batch, after `out_batch.commit()`, into a
  `WireSeq` cursor in `PipelineCursors` (`matching_applied_wire_seq`). An
  output-ring consumer loads it (`Acquire`) *before* reading the ring and,
  if the ring is then empty, every report at or below it has been read, so
  every window ending at or below it can close (finding 3). Windows with no
  report close with their header alone, on every copy alike.
- **The primary folds every report, whatever happens to its delivery.**
  `Egress::send_slot` takes the encoded `SlotFrames` instead of a
  `SlotReply`, and the egress no longer sees the encoder: it cannot skip the
  encode or do it differently. The loop, for each `Report` slot: encodes
  through the folder, offers the frames to the egress (if the gate gave a
  reply and the stop latch is clear, decision 10), then folds the encoded
  body, whatever `SlotOutcome` came back. The fold comes after the send so
  that the hash is not on the reply's own latency, only on the stage's
  throughput. `SlotOutcome::Unknown` comes to mean "not sent" rather than
  "not encoded". The cost is encoding reports nobody receives (node events',
  gone clients'), which the primary skips today.
- **Replica and shadow encode with the same encoder.** The node already
  holds it as an `Arc<dyn ResponseEncoder>`. It goes to the replica builder
  and to `shadow::run`, which feed it to their folders.

### 4. The digest board: one writer, many readers, no backpressure

Each folder publishes to a **`DigestBoard`**: `R` slots of `(window_end,
digest)`, the slot for window end `E` being `(E / K) % R`, each a
`melin_pipeline::seqlock` (a `repr(C)` 24-byte payload, a `u64` window end
then the 16 digest bytes, without padding, so it satisfies `NoPadding`),
plus an `AtomicU64` holding the latest window end for cheap change
detection, plus the schedule record of decision 2 (a seqlock of its own,
written before the first digest under it). A seqlock rather than a queue:
a folder must never wait on a reader, a reader that falls behind should
lose old digests rather than stall anyone, and there are several readers
(the journal stage's injector, the shadow's comparator, the replica's
receiver, the health endpoint). The seqlock module already enforces
single-writer and no-padding in the type system.

**Sizing R.** `R = ceil((INPUT_RING_CAPACITY + OUTPUT_RING_CAPACITY) /
MIN_OUTPUT_DIGEST_INTERVAL) + 2`, a constant, allocated once when the
pipeline is built: with both rings at `1 << 20` slots, 514 slots of one
cache line each (each slot is its own seqlock cell, aligned to 64 bytes,
so the digest's width does not change the footprint). It is sized for
the worst K, not the K in force, because K is not fixed for a pipeline's
life: a replica's pipeline outlives its sessions (`tcp_receiver.rs` and
`dpdk.rs` build it only if absent), and each session adopts the K of
whichever node is primary (decision 2). A board sized for a first
primary's K = 65536 would hold 34 slots, lap constantly after a failover
to a primary at K = 4096, and turn nearly every comparison into
`Unverified { Lapped }`. The comparator's arrays take the same constant,
so its sizing cannot drift from the board's either. Reallocating at each
session start instead would put an allocation on the session path for a
saving of tens of kilobytes. At any K, R covers the lag between any two
folders while every ring is moving, with room to spare as K grows. A
stage held for a long time (a gate held by a halt, with ticks journaled
behind it) can exceed it, and a reader that finds a window overwritten
counts it as `Unverified { Lapped }`, never as divergence.

Lookups are `get(end) -> Option<Digest>` (present, not yet written, or
lapped) and `latest() -> Option<(WireSeq, Digest)>`. Positions are
`WireSeq`, the existing cursor-space newtype.

### 5. The comparator: one pure type for both checks

A `DigestComparator` (`transport-core`, no I/O, no threads) holds two
fixed-capacity arrays of `R` slots (decision 4's constant, whatever K it is
built with), *expected* (the primary's digests, or
the live board's for the in-process check) and *local*, indexed exactly as
the board is (one helper, so the two cannot index differently), each slot
storing its window end beside the digest. A lookup is one division and one
compare; nothing allocates after construction. It is built with K, and
at a change of K it clears both arrays and re-keys them under the new K
before taking any digest under it: an entry kept from the old grid,
indexed under the new one, could sit in the slot of a new-grid window end
and be compared against it. `verified_through` survives the change; only
the held, not-yet-compared digests are dropped.

**Its start is one rule:** the later `first_end` among the sides that have
a schedule record, re-read whenever a record changes (a change of K) and at
nothing else. A local board always has one. The primary's digest stream on
a replica has none (`StreamStart` carries `{kind, interval}`, not a first
window), so on the replica-vs-primary check the start is the replica's own
folder's `first_end`; on the shadow-vs-live check, where both sides are
local boards, it is the later of their two. A reset (decision 8) clears the
*expected* side only and leaves the start and the local side untouched, so
a local window closed before a reconnect is still compared when the new
session's primary sends its digest.

Feeding either side returns a verdict for every window both sides now hold:

- `Match { end }`, which advances `verified_through` to `end` and counts;
- `Mismatch { end, expected, actual }`, the divergence verdict;
- `Unverified { end, reason }`, counted, never a divergence:
  - `Lapped`: one side's digest was evicted before the other's arrived.
    Possible only past decision 4's bound, so worth a `warn!`;
  - `BeforeStart`: a window below the comparator's start. Normal on a
    replica's first session (the primary's digests of windows the fresh
    folder did not see whole) and at a change of K;
    counted under its own label so it never adds noise to `lapped`, and
    logged at `debug!` only.

A digest with no counterpart *yet* is not an event: the primary does not
send every window (it skips windows it lapped, and sends none during
catch-up), so silence is sampling. Off-grid digests (a straggler of the old
schedule at a change of K) are dropped, uncounted.

### 6. The primary emits from the journal stage, beside `ChainCheck`

`maybe_publish_output_digest`, called right after `maybe_publish_chain_check`
at the sync point, does nothing without replication producers or with K = 0.
Otherwise, with `end` the last sequence the batch just published, it picks
the newest digest whose window ends at or below `end`:

- if the board's `latest()` is at or below `end`, that digest (the response
  stage trails the journal stage, which under any ack policy is the usual
  case);
- otherwise (the window closed through the applied cursor while the journal
  stage was behind it), `get((end / K) * K)`, one division per fsync batch,
  cold.

If the pick is newer than the last it forwarded, it encodes the frame of
decision 7 and publishes it through `publish_to_replication_rings` with
`end_seq = end`, the `ChainCheck` convention, so `RingChunk::classify`
sorts it like any control frame during catch-up. Intermediate windows are
skipped when several closed between two batches (fewer checks, nothing
wrong), and an idle primary still journals ticks, so batches keep coming. A
ring too full to take the frame evicts the replica exactly as for a
`ChainCheck`.

**Independent of the chain.** A sibling of `maybe_publish_chain_check`, not
code inside it: the chain check returns early when `encoder.chain_hash()`
is `None` (the chain-less build) and runs on its own batch cadence. Nothing
in the digest path is behind `cfg(feature = "hash-chain")`.

### 7. Wire: replication protocol 9

| Change | Layout (after the length prefix) |
| --- | --- |
| New frame `MSG_DIGEST = 0x19`, primary to replica | `DigestFrame { tag, kind: u8, window_end: U64, window_len: U64, digest: [u8; 16] }`, 34 bytes |
| New frame `MSG_DIVERGED = 0x07`, replica to primary | `DivergedFrame { tag, kind: u8, window_end: U64, window_len: U64, expected: [u8; 16], actual: [u8; 16] }`, 50 bytes: the replica's notice before it stops (decision 10) |
| `StreamStart` gains the digest schedules and the build | `schedules: [ScheduleEntry; 4]` with `ScheduleEntry { kind: u8, interval: U64 }` (kind 0: empty slot; interval 0: this primary sends none of that kind), then `build_id: [u8; 32]` (open question 2), appended after `genesis_entries` |

- **The kind byte from the start.** Only `1 = output` is defined; 0 is
  refused as malformed. A replica skips a frame or schedule entry of a kind
  it does not know (counted `unverified{reason="kind"}`, `warn!` once per
  session), so a later state digest (step 7) is a new kind value on the
  same frames, sent to replicas that already speak protocol 9, not another
  version bump. That is also why each frame carries its window length,
  and why `StreamStart` carries a table rather than one interval: a new
  kind brings its own schedule without changing a layout. The digest
  field stays 16 bytes for every kind, so a later kind is defined to
  produce 16 bytes; one that needs more (a tamper-evident digest) is a
  new frame and a version bump (open question 1).
- **The handshake does not change**, so the existing version check
  (`HandshakeFrame.protocol_version`, parsed by `decode_replica_message`)
  refuses a mixed pair in both directions with the version error, as at
  every earlier bump. `REPL_PROTOCOL_VERSION` becomes 9, with the history
  comment extended. Size assertions for the two new frames and the grown
  `StreamStart`; `docs/replication.md`'s frame tables gain them.

### 8. The replica compares in its receiver

- `process_streaming_frames` decodes `Digest { kind, end, len, digest }`.
  Since the primary sends a digest only once its stream has passed `end`,
  `end` above the stream position means entries are missing: a
  `FrameError::SequenceGap`, the contract of a mark ahead. A window length
  other than the adopted schedule's is the primary's protocol bug and ends
  the session as a `FrameError`, never as a divergence. A digest behind the
  position is normal and goes to the comparator.
- The comparator runs on the receiver thread inside `streaming_loop` (one
  copy for kernel TCP and DPDK), but `streaming_loop` does not own it:
  `streaming_loop` runs once per session, and anything it built would be
  rebuilt at every reconnect, losing the local windows closed before it.
  The comparator is a field of `ReplicaPipelineHandles`, beside the live
  board handle and `stream_marks`, and both receivers lend it to
  `streaming_loop` by `&mut`, as they already lend `input_producer`. It
  therefore lives exactly as long as the pipeline: kept across
  `Disconnected` reconnects, rebuilt with it on a resync (decision 12's
  table). Its local side is the replica's live board: one `Acquire` load of
  the latest window per loop iteration, draining only when it moved. At
  every session start `streaming_loop` resets it, clearing the expected
  side only (the previous primary's digests are not carried into a session
  with another); its start and local side are kept, by decision 5's one
  rule.
- A `Mismatch` ends the session with a new
  `SessionExit::Diverged(Divergence)`. The match in `handle_session_exit` is
  exhaustive, so every receiver must decide what it means, and its arm runs
  decision 10's stop and returns `Err`: it never reaches a reconnect-loop
  top, where a promotion request could be honoured.

### 9. The in-process check: shadow against live, on every node with a shadow

The shadow folds with the same `WindowFolder` and the same schedule (its own
flag on a primary, the adopted K on a replica), and runs a
`DigestComparator` whose expected side is the node's live board (the
response stage's on a primary, the drain's on a replica) and whose local
side is its own. The shadow runs the comparator itself: it is off the ack
path and already does cold work between batches. Either may lead (the
shadow trails the journal; the live folder on a primary trails the gate),
so a digest can wait in the comparator for the other. A `Mismatch` stops the
node (decision 10), whether it is a primary or a replica.

This is the only check on a node without replicas, and it catches what the
replica check cannot see on a primary: a shadow built by
`clone_via_snapshot` that answers differently, that is, a `snapshot`/`restore`
pair that drops a field, which is exactly the copy a restart or a resync
would come back from. A node without a shadow (`--snapshot-interval-ms 0`)
has no in-process check, and a standalone node without one has no check at
all; both are logged at startup and documented as limits.

### 10. On divergence: stop, in one order

Whichever comparison finds it (a replica's receiver, or any node's shadow),
the node:

1. **Halts replies at once.** `DivergenceLatch::trip(divergence)` latches the
   node's `FenceState` with a new stop cause and co-sets the shutdown flag,
   the way `fence_if_superseded` owns "fenced ⇒ shutting down". The latch
   keeps the first `Divergence` only; a second trip returns `None`. From the
   latch on: the readers close every client (`Verdict::Close`), the
   response loop hands no slot and no early reply to the egress (a per-slot
   `Relaxed` load of the latch, before the send, decision 3), the shadow
   saves no further snapshot (checked before every save), and raft refuses
   and stands down on `fenced` (finding 6).
2. **Logs `error!`** naming the check (`replica-vs-primary` or
   `shadow-vs-live`), the window, both digest prefixes (`hex_prefix`, the
   journal crate's formatter, widened from `&[u8; 32]` to `&[u8]` so the
   16-byte digest and the 32-byte chain hash print alike), and
   `verified_through`. On a replica it also says: "if other replicas
   report the same window, the primary's output is the one at fault".
3. **Writes the divergence marker** beside the journal
   (`NodeStore::divergence_marker()`, `<journal>.diverged`), written to a
   temporary file, fsynced, renamed and the directory fsynced, so it
   survives a crash. Plain text, for an operator: the format version, the
   check, the kind, the window (start, end, length), both digests in full
   hex, `verified_through`, the archive directory (once known), the node's
   build id, and the wall-clock time (for people; nothing reads it).
4. **On a replica, sends the `Diverged` notice** to its primary, best
   effort, before the session ends.
5. **Archives, never deletes, the snapshots above the last verified
   position**, once every pipeline thread has been joined, so no shadow save
   can race it. `verified_through` is the least of the node's comparisons'
   (on a replica, its check against the primary and its shadow's against
   live): a snapshot is only as good as the least-checked copy that led to
   it. Each of `<snapshot>` and `<snapshot>.prev` whose header's
   `journal_sequence` is above it moves into
   `<snapshot dir>/<snapshot name>.diverged.<n>/`, through the archive
   module's helpers and a new `ArchiveReason::Diverged`. The journal is left
   in place: it is input-only and the chain vouches for it. The marker is
   rewritten with the archive's path.
6. **Exits with an error.** The node's run returns
   `Err(NodeDiverged { .. })`, through one helper every node entry
   (`run_tcp`, `run_dpdk`, so `run`, `run_with_shutdown` and
   `run_with_listener`) returns through, which the binaries turn into a
   non-zero exit. A return rather than `process::exit`, because a test or a
   host may run several nodes in one process.

**The order is enforced by types, not by a comment.** `trip` returns a
`Halted` token (private constructor, neither `Clone` nor `Copy`), the
marker writer consumes it and returns `MarkerWritten`, and the archiver
consumes that. Nothing can write a marker, or archive, before the latch is
set, and a new detection site cannot compile without going through `trip`.
The comparators hand their verdict to `trip` directly, so no thread can
"log and carry on".

**Which thread does what.** The steps span threads: detection is on the
shadow thread (primary or replica) or the replica's receiver thread, while
the archive must wait until every pipeline thread has been joined. So:

- The **detecting thread** trips the latch, logs, and writes the marker
  itself, at once, holding the `Halted` token: the marker is cold work of
  the kind the shadow already does (it writes snapshots), and writing it
  before any join means a hang in the shutdown leaves the node refusing to
  restart all the same. It then deposits the result, `MarkerWritten` or the
  I/O error, back into the `DivergenceLatch`, which is one `Arc` per node
  shared by every detection site and the exit helper (a `Mutex` around an
  `Option`, touched only on this cold path).
- The **exit helper** is the single return path of every node entry, after
  every pipeline thread (and, on a replica, the receiver) has been joined,
  whichever way they left. It claims the latch's contents: nothing there is
  an ordinary exit; a divergence with `MarkerWritten` archives and returns
  `NodeDiverged`; a divergence whose marker write failed archives nothing
  (the token is absent, so it cannot) and returns `NodeDiverged` carrying
  the I/O error, logged at `error!`, so the operator learns the refusal on
  restart is not guaranteed. The decision is the latch's, not the exit
  path's: a replica receiver that sees only the shutdown flag and leaves
  through `SessionExit::Shutdown`, after its shadow tripped, still returns
  through the helper, which finds the divergence and archives. No exit
  path can skip the claim, because the helper is the only way to return
  from the entry.

What "at once" means, precisely: no slot is handed to a transport after the
response stage has observed the latch. Bytes handed on before it (a kernel
send buffer, a DPDK TX ring) may still leave. Detection itself trails the
diverged window (it must close and reach the other copy), so the replies of
that window have gone out by the time anyone knows; what the stop
guarantees is that nothing further does.

No automatic repair and no retry: a divergence means the application's
determinism contract is broken, a restart from the same journal and the same
build has no reason to behave differently, and an outage is the intended
outcome. The old plan's resync request, repair budget and "poison the shadow
and keep serving" mode are gone.

### 11. Startup refuses while the marker exists

A new `NodeStore::refuse_if_diverged(&self) -> Result<(), NodeStoreError>`,
which every node entry calls right after `resolve` and before it opens its
journal, starts raft or serves anything, returns
`NodeStoreError::Diverged { marker, check, window_end, archive }` if the
marker exists. `resolve` stays as it is, writing nothing, so a test or tool
that resolves a diverged node's paths is neither refused nor moves files.
Both entries resolve through `ServerConfig::store()` today (`run_tcp` and
`run_dpdk`, `server.rs`), which tests also call. They move to a new
`ServerConfig::startup_store()`, resolve then `refuse_if_diverged`, the one
place the refusal is made, so a third entry takes it by calling the same
function; `store()` stays write-free for tests. The error says what
happened and what to do:

> this node stopped on `<time>` because its output diverged from the
> `<primary's | shadow's>` at window `<a..=b>` (details in `<marker>`).
> Its snapshots after position `<N>` are in `<archive>`; its journal is
> intact. Find which copy is at fault (several replicas reporting the same
> window point at the primary), fix the application, rebuild this node's
> state (reseed it from the current primary, or replay its journal), then
> delete `<marker>`. See docs/replication.md, "Output verification".

Before refusing, `refuse_if_diverged` re-runs the archival of decision 10
(idempotent: it
moves only snapshots above the marker's `verified_through` that are still in
place), so a crash between the marker and the archive cannot leave a
suspect snapshot for an operator to boot from by mistake. A marker that
cannot be parsed still refuses, naming the file.

So a diverged node cannot come back on its own (systemd's `Restart=always`
just logs the refusal at each attempt), and cannot be elected. Normal
failover then promotes a replica whose digests matched the primary's live
output up to when the primary stopped.

### 12. Snapshots, recovery, failover, rejoin

| Situation | Behaviour |
| --- | --- |
| Restart, recovery from snapshot + journal | Replay is not folded. Live and shadow start folding at the first full window after the recovered position. Replicas compare as usual, so every primary restart becomes a check of its *replay* against the replicas' *live* history. |
| Replica booting from its own snapshot | Same. A `snapshot`/`restore` pair that drops a field shows at the first report it changes. |
| Replica catching up from the primary's journal | The disk walk carries no digests. Ring chunks holding digests are classified like any control frame. Local windows during catch-up go unmatched, which is sampling. |
| Snapshot resync (chain divergence, pruned history) | Pipeline rebuilt: new folders, new boards, new comparators, starting at the next full window. |
| Promotion | The warm application moves into a new primary pipeline with new folders; K reverts to the new primary's own flag and is advertised on its replicas' `StreamStart`. |
| Ex-primary rejoining | A forked journal is caught by the chain check first and resynced. Otherwise its output is compared against the new primary's from its first full window. |
| A diverged node | Stopped and refusing to start (decisions 10, 11). |
| Shutdown drain | Windows still open are dropped: nothing would act on them. |

### 13. Raft and failover

Nothing changes in `raft_promotion.rs` or `melin-raft` (finding 6). The step
that adds the stop (step 3) carries the test that pins it: a diverged
primary that is the raft leader exits, a healthy replica is elected and
promotes, and the diverged node's restart is refused. The stop cause added
to `FenceState` lets the stand-down's log line say "diverged" rather than
"fenced" for an operator reading why a node stopped campaigning; that is
the only raft-side line that changes.

### 14. Hot-path cost

| Where | Per report | Per batch / iteration | Per window |
| --- | --- | --- | --- |
| Matching stage, every copy | nothing | one `Release` store (applied position) | nothing |
| Primary response stage | encode (already paid for delivered replies; new for node events' and gone clients'), one XXH3 update, after the send | one `Relaxed` latch load per slot; on the idle path one `Acquire` load | finalize, one seqlock store |
| Replica drain thread | encode, one XXH3 update (both new) | one `Acquire` load when the ring is empty | finalize, one seqlock store |
| Shadow | encode, one XXH3 update (both new) | none | finalize, comparator update |
| Journal stage (primary with replicas) | nothing | one latest-window load, at most one division and one board read | 34-byte frame encode and ring publish |
| Replica receiver | nothing | one `Acquire` load | decode, comparator update |

Nothing allocates after construction. The measurement step (step 6) first
folds windows of representative report bodies (the examples', and a few
hundred bytes) through XXH3-128, with and without `twox-hash`'s `std`
feature (step 1), and through BLAKE3, in a micro-benchmark, so the hash's
share is known apart from the encode's. It then runs
`melin-bench` closed-loop against echo, counter and notary, digests at the
default interval and disabled, over kernel TCP and DPDK, primary with two
replicas under `disk+ram`, and compares p50, p99 and p99.9. It reads the
`latency-trace` response-stage histograms for the fold's share of the
reply path, and checks that the replica drain and the shadow keep up at
full rate (no growth in the matching stage's ring waits, no `lapped`).
Results go in a dated measurement document naming its hardware.

### 15. Observability

Logs (levels per the project convention):

| Event | Level | Where |
| --- | --- | --- |
| Output diverged: the node stops (check, window, digests, verified position) | `error!` | the diverging node |
| Marker written; snapshots archived (with paths) | `error!` (part of the stop) | the diverging node |
| Startup refused: the marker exists | `error!` (the run's returned error) | the diverged node, at each start |
| A replica reported divergence (`Diverged` notice) | `error!` | primary |
| Every replica streaming reported the same window | `error!`, once per window: "every replica disagrees with this primary at the same window: this primary's output is the likely fault" | primary |
| Output digests disabled on this node (`--output-digest-interval 0`), or no in-process check (no shadow) | `warn!` at startup | every node |
| The primary sends no digests (interval 0), or a different build (open question 2), for this session | `warn!`, once per session | replica |
| A digest of an unknown kind skipped | `warn!`, once per session | replica |
| A window lapped before it could be compared | `warn!`, first then at each doubling of the count | the comparing thread |
| A window below the comparator's start | `debug!` | the comparing thread |
| Output digests on at interval K | `info!` at pipeline start | every node |
| A match | none (metrics only) | |

Metrics (`/metrics`, rendered in `health.rs`, on replicas through the
replica health endpoint; `/healthz` carries the interval, `verified_through`
and whether the node stopped diverged):

| Metric | Type | Meaning |
| --- | --- | --- |
| `melin_output_digest_interval` | gauge | K in force (0: off). Alert on 0 where it is not intended |
| `melin_output_digest_window` | gauge | last window end this node's live copy digested |
| `melin_output_verified_through{against="primary"\|"shadow"}` | gauge | last window end found equal. A gauge that stops moving under traffic is worth a look |
| `melin_output_checks_total{against}` | counter | windows compared and found equal |
| `melin_output_diverged` | gauge | 1 once the node has stopped for divergence (seen in the short window before it exits) |
| `melin_output_unverified_total{reason="lapped"\|"before_start"\|"kind"\|"build"}` | counter | windows that could not be compared. `before_start` grows at every session start and is not an alert; alert on `lapped` |
| `melin_replica_output_divergence_total` | counter | primary: `Diverged` notices received. Alert on any growth |

`melin_replica_divergence_total` keeps its meaning (journal chain) so that
existing alerts do not change. A diverged node is fenced, so the health
endpoint's `trading` flag already reads false on it; `/healthz` adds the
stop cause.

## Steps

Each step builds, passes the full suite (kernel TCP, and the DPDK suite
under the netns runner), and is committable on its own. Docs and
`CHANGELOG.md` move with the behaviour they describe.

### Step 1. The pure building blocks

- `transport-core::output_digest`: `DigestKind` (exhaustive, `Output = 1`),
  `WindowFolder` (decision 1's definition, decision 2's first-full-window
  rule and realignment), `DigestBoard` (decision 4, sized from both ring
  capacities and the minimum interval, never from the K in force),
  `DigestComparator` (decision 5, the same constant),
  `DEFAULT_OUTPUT_DIGEST_INTERVAL`, `MIN_OUTPUT_DIGEST_INTERVAL`.
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
  only to make the allocating `Hasher` available, which the folder does
  not use). The folder holds a `xxhash3_128::RawHasher` over
  `SecretBuffer::default()` (no allocation), replaced by a fresh one at
  each window (decision 1), and writes the domain string and header into
  it first. The field carries the comment justifying the choice (decision
  1's reasons, and why `RawHasher` rather than `Hasher`), as do `Digest`
  and the board's payload.
- `encode_report_body` (decision 3): `MAX_RESPONSE_BODY` and the body-bound
  check move into it; `server-runtime` re-exports the constant from where it
  is today and `frame_app_response` and `encode_early_reply` call it, so the
  framing is unchanged.
- `ResponseEncoder::encode_report` rustdoc gains the determinism clause
  (decision 1); `docs/building-an-application.md`, "The determinism rules",
  says the same, and names the routine causes of a repeated or backwards
  tick (the double call, a restart or restore across a clock step back)
  under the existing tick rule (finding 7).
- Tests, all ungated:
  - folder: the digest of a known window pinned to a byte pattern (so the
    definition cannot drift silently between builds, including through a
    `twox-hash` upgrade or a change of its feature set), the pinned value
    taken from `xxh128sum` over the same bytes, not from the code under
    test; the streaming hasher fed report by report equal to the one-shot
    `xxhash3_128::Hasher::oneshot` of the concatenated bytes, over windows
    on both sides of the 240-byte cutoff and longer than the hasher's
    internal buffer (`oneshot` needs no `alloc` either); a second window
    after a restart digesting as a first one would (nothing carried over);
    a report moved to another position, a byte changed, two reports
    swapped, an encode error in place of a body, all change it; the same
    reports folded in two folders started at different positions agree on
    every window both saw whole and digest no partial window; empty
    windows digest their header; realignment on a change of K lands on the
    new grid and publishes the schedule before the first digest under it;
  - board: exact lookup, lap detection, `latest`, the schedule record
    written before the first digest under it; a two-thread stress test
    asserting no torn value is ever returned, run under Miri by the existing
    Miri job as the seqlock is;
  - comparator, property tests over random interleavings of the two sides
    within decision 4's bound: never a `Mismatch` when the sequences agree,
    a `Mismatch` at exactly the first differing window both hold, never
    `Lapped` within the bound, `BeforeStart` exactly below the start, and
    `Lapped` (not `Mismatch`) past the bound; the start taken from the one
    side with a schedule record, or the later of two; a reset that clears
    the expected side and still compares a local window closed before it
    against an expected digest fed after it; across a change of K, both
    arrays cleared and re-keyed (no old-grid entry ever compared against a
    new-grid window end it aliases), `verified_through` kept, the
    old-grid stragglers dropped uncounted and the skipped windows
    `BeforeStart`; one indexing helper for board and comparator; the
    bound holds at every K the flag accepts, generated from the minimum
    up, with the one constant R, and across a change from a large K to
    the minimum (the failover case of decision 4).
- `framing` tests in `response_frame.rs` keep passing unchanged: the
  framing is the same bytes.
- **Leaves untested:** nothing calls these types yet, so whether each copy
  feeds them the same reports is step 2's to show.

### Step 2. Every copy folds

- The matching stage's applied cursor (`PipelineCursors::matching_applied_wire_seq`).
- `response_loop::run`: encode through the folder, send, fold, as in
  decision 3; `Egress::send_slot` takes `SlotFrames`; `SlotReply` and its
  encoder go; both egresses (`response.rs`, `dpdk_response.rs`) adapt. The
  per-slot latch check is not added yet (no latch exists until step 3).
- The replica's drain thread hoisted into a function that borrows slots in
  place (as the response stage does) and folds; `shadow::run` takes the
  encoder and folds. Each pipeline builder creates the boards.
- `ServerConfig::output_digest_interval` and `--output-digest-interval`
  (validated, 0 disables), on every node. In this step a replica folds at
  its own configured K: adoption needs the protocol of step 4.
- Metrics `melin_output_digest_interval`, `melin_output_digest_window`; the
  startup logs of decision 15.
- Tests:
  - `pipeline_tests.rs`: one input sequence driven through a primary
    pipeline (response stage over a recording egress), a replica pipeline
    (its drain) and a shadow publishes identical digests for every window
    all three saw whole. The sequence carries what differs between copies
    on purpose: a client request with no report (a `BatchEnd` slot on the
    primary only), a query (primary only), reports of a tick (connection
    0), a report to a connection the egress does not hold, a reply released
    degraded, an ack-policy change, and a report the encoder fails on;
  - `response_loop.rs` tests: a reply to an unknown connection is folded
    and not sent; a window whose last entries have no report closes on the
    idle path once the applied cursor passes it, and not before; the
    applied cursor is loaded before the ring is read (a recording seam
    holding the stage between the two);
  - the chain-less build: the same agreement test with `hash-chain` off.
    The pre-commit hook and CI only `cargo check` that build, so the step
    adds a test run of these cases (`cargo nextest run -p
    melin-transport-core --no-default-features`, filtered to them) to the
    pre-merge job's chain-off loop;
  - the DPDK egress's adaptation runs under the netns runner through the
    existing response-stage tests in `dpdk_response.rs`.
- Docs: `docs/pipeline-architecture.md` (the response stage folds every
  report; the replica's drain and the shadow encode), the new flag in
  `docs/replication.md`'s CLI table. `CHANGELOG.md` **Changed**:
  `encode_report` must be deterministic; `ServerConfig` gains the public
  field `output_digest_interval` (breaking for a struct literal).
  **Added**: the `--output-digest-interval` flag.
- **Leaves untested:** the cost, which is step 6's measurement, not a test;
  a replica at another K than its primary, which cannot be compared yet and
  is made impossible by step 4.

### Step 3. The stop, and the in-process check

- `FenceState`: a stop cause (`Superseded | Diverged`, exhaustive), first
  cause wins; `is_fenced` unchanged for its readers; the cause in
  `/healthz` and in the raft stand-down's log line.
- `DivergenceLatch`, `trip`, the `Halted` / `MarkerWritten` tokens, the
  marker (`NodeStore::divergence_marker`), `ArchiveReason::Diverged` and the
  snapshot archival, the `NodeDiverged` error through the one exit helper
  (decision 10). `melin_journal::error::hex_prefix` widened to `&[u8]`
  for the divergence log line, rather than a second formatter beside it;
  every existing call passes a reference to a 32-byte array, which
  coerces, so no caller changes.
- The response loop's per-slot latch check, before the send and before an
  early reply's release; the shadow's check before each save.
- The shadow's comparator against the live board on every node with a
  shadow (decision 9).
- `NodeStore::refuse_if_diverged`, `NodeStoreError::Diverged` and the
  archival re-run at startup, called from `ServerConfig::startup_store()`
  by both entries; `resolve` and `store()` unchanged (decision 11).
- Metrics `verified_through{against="shadow"}`,
  `checks_total{against="shadow"}`, `melin_output_diverged`,
  `unverified_total`.
- Tests:
  - `response_loop.rs`: a latch set while a slot waits at the gate: the
    slot is never offered to the egress after its release, nor any later
    slot or early reply; a latch set between two slots of one batch: the
    first is sent, the second is not. This pins "no reply after the latch
    is observed";
  - `DivergenceLatch`: the first trip wins; it fences and sets shutdown
    before returning the token; the exit helper's claim, for each of its
    three cases (nothing, marker written, marker failed), including a
    divergence deposited by another thread while the caller left through
    ordinary shutdown;
  - marker: written durably (temporary file, rename), parsed back, every
    field present, each digest in full canonical hex; an unparseable
    marker still refuses;
  - `hex_prefix`: the same prefix for a 16-byte digest as for a 32-byte
    hash sharing its first bytes, so the chain's log lines are unchanged;
  - archival: snapshots above `verified_through` move, those at or below
    stay, `.prev` handled the same way, a second run moves nothing, and the
    journal is untouched; with no verified window, every snapshot moves;
  - `node_store.rs`: `refuse_if_diverged` refuses with the marker present,
    and the message names the marker, the window, the archive and the doc
    section; `resolve` on the same store succeeds and moves nothing;
    `server.rs`: `startup_store()` refuses where `store()` does not;
  - shadow: a fault-injecting test app whose `restore` leaves a
    non-serialized flag that changes a later report (a lossy
    `snapshot`/`restore`): the shadow, built by `clone_via_snapshot`,
    diverges at the first window holding the changed report, and no
    snapshot is saved after the trip;
  - integration, `server-runtime/tests/output_divergence.rs`, started
    through `melin-test-node` so it also runs on DPDK under the netns
    runner: one node with that app under load stops, its run returns
    `NodeDiverged`, the `error!` appears (`logs::capture`), the marker
    exists, the post-divergence snapshot is archived, every client
    connection is closed and a request sent after the trip gets no reply
    (the per-slot ordering itself is pinned by the unit test above, which
    an end-to-end test cannot time), and a restart is refused with the
    error;
  - integration, same file, the way back (the runbook's procedure, so the
    runbook's steps are tested rather than prose): after the refusal,
    delete the marker and restart with the post-divergence snapshots left
    archived; the node boots from its journal and the snapshot it kept at
    or below the verified position (or from the journal alone when none
    was kept), its state equals a fresh replay of the same journal, and it
    serves again. Run with the lossy-restore fault switched off on the
    restart (it is the `Sizing`), as an operator restarts on a fixed build;
  - integration, same file, a replica's shadow trips (the lossy-restore app
    on the replica only): its receiver leaves through ordinary shutdown,
    and its run still returns `NodeDiverged` with the marker written and
    the archive done (decision 10, "Which thread does what");
  - integration beside `raft_failover.rs`: three nodes, the primary (raft
    leader) carrying the lossy-restore app via its `Sizing` (the one input
    a test may vary per node), the primary stops, a replica is elected and
    promotes, and the ex-primary's restart is refused. The data plane runs
    on DPDK under the runner; raft is kernel TCP by design.
- Docs: `docs/replication.md`, a first "Output verification" section with
  the in-process check, the stop and the startup refusal; the runbook is
  completed in step 6. `CHANGELOG.md` **Changed**: a node whose shadow
  disagrees with its live copy now stops and refuses to restart until its
  marker is cleared (new, always-on behaviour, named first);
  `NodeStoreError::Diverged` (re-exported from `melin-transport-core`) and
  `ArchiveReason::Diverged` are new variants of public exhaustive enums, so
  a consumer's `match` stops compiling until it handles them; the node
  entries resolve through `startup_store()`. **Added**: the `NodeDiverged`
  error a node's run returns, `NodeStore::refuse_if_diverged`.
- **Leaves untested:** a crash between the marker and the archive is
  covered by the startup re-run's unit test, not by killing a process
  mid-stop; a hardware fault is modelled only by the fault-injecting app
  (the detection path is the same).

### Step 4. Replication protocol 9

- `protocol.rs`: `MSG_DIGEST` and `MSG_DIVERGED` with encoders, decoders and
  size assertions; `StreamStart`'s schedules and build id; the version bump
  with its history line.
- Every sender fills `StreamStart` from its configuration (kernel
  `tcp_sender.rs`, the DPDK sender and its `join_worker.rs`), with K from
  its own flag; the build id per open question 2, held in a `ServerConfig`
  field that defaults to the executable's hash (so step 5's mixed-build
  test can set it; listed with step 2's field under `CHANGELOG.md`
  **Changed**).
- Every receiver adopts the output schedule on `StreamStart`, published to
  the drain's and the shadow's folders through the atomic of decision 2,
  before publishing any entry of the session; it warns once on interval 0,
  an unknown kind or another build. Digest frames are decoded and, for now,
  dropped: no primary sends one until step 5. The primary decodes
  `Diverged` and logs it (decision 15), though no replica sends one yet.
- Tests: round trips; wire byte patterns beside `ack_wire_byte_pattern` and
  `rotate_wire_byte_pattern`; a v8 handshake refused with the version error
  in both directions (the existing
  `a_handshake_from_another_protocol_version_is_refused` covers it once the
  constant moves; one case added for a v9 replica at a scripted v8
  primary); kind 0 refused, an unknown kind skipped and counted, not
  fatal; `RingChunk::classify` sorts a `Digest` chunk exactly as a
  `ChainCheck` one (ungated, so the DPDK sender's `LiveHandoff`, which
  shares the rule, is covered on every host); a replica's folders adopt a
  new K at a session start and realign (`receiver_transport.rs` with the
  scripted transport); a replica pipeline kept across two sessions, the
  first at the default K and the second at the minimum, keeps one board
  whose capacity is the constant R, so it does not lap under the second.
- Docs: `docs/replication.md` frame tables. `CHANGELOG.md` **Changed**:
  replication protocol 9, mixed pairs refused.
- **Leaves untested:** a primary that sends digests, which is step 5.

### Step 5. The primary streams digests; replicas compare and stop

- Journal stage: `maybe_publish_output_digest` (decision 6).
- Receiver: decode, contiguity, the comparator against the live board, the
  `Diverged` notice, `SessionExit::Diverged` and its arm in
  `handle_session_exit` running decision 10's stop (decision 8).
- Primary: the "every replica, same window" aggregation of decision 15.
- Metrics `verified_through{against="primary"}`,
  `checks_total{against="primary"}`,
  `melin_replica_output_divergence_total`.
- Tests:
  - `pipeline_tests.rs`: `primary_emits_output_digest_after_its_position`,
    modelled on `primary_emits_chain_check_every_interval`: every digest
    frame in the ring names a window ending at or below its chunk's
    `end_seq`, none precedes the batch holding its end, with the response
    stage lagging the journal stage and with a window closed by the applied
    cursor ahead of it (the `get` arm); and the same with `hash-chain` off,
    where digests are emitted and no `ChainCheck` is (added to step 2's
    chain-off run);
  - `receiver_transport.rs`: a digest ahead of the stream position is a
    `SequenceGap`; one behind is compared; a wrong window length ends the
    session as a frame error; a mismatching one sends the notice and ends
    the session with `SessionExit::Diverged`; a promotion request pending
    when the mismatch arrives is not honoured (the session ends in the
    divergence, and the receiver returns the error); a session whose
    `StreamStart` carries another build id compares nothing, counts every
    digest `unverified{reason="build"}`, and a mismatching digest in it
    does not stop the replica; an all-zero id compares as any build; two
    sessions of `streaming_loop` over the same `ReplicaPipelineHandles`
    comparator, a local window closing in the first after its primary's
    digest was not yet sent, the second session's primary then sending a
    matching (and, separately, a mismatching) digest for it: it is
    compared (`Match`, and `SessionExit::Diverged`), not `BeforeStart`, so
    the reset-keeps-local property is pinned where the comparator is
    actually owned;
  - `tcp_receiver.rs` and `dpdk.rs` (the latter under the runner): the
    divergence exit tears the pipeline down, archives and returns the error
    without reaching the reconnect-loop top; a `Disconnected` reconnect
    keeps the pipeline's comparator (its `verified_through` survives the
    reconnect);
  - integration (`output_divergence.rs`, both transports): primary and two
    replicas healthy under load, checks grow on both replicas, nothing
    diverges; a replica given a fault through its `Sizing` (its test app's
    `prefault` stores "change the report of the event that brings my count
    to X", violating `prefault`'s contract on purpose) stops at the window
    holding X, its marker and archive exist, its restart is refused, the
    primary logs the notice and keeps serving on the other replica; the
    same fault on both replicas makes the primary log the "same window"
    error, and under `disk+ram` the primary then halts writes (its last
    replica gone), as the docs will state; and the mixed-build case: the
    same fault on a replica whose primary is started with another build id
    (a `ServerConfig` field that defaults to the executable's hash and that
    `melin-test-node` can set, the one seam that stands in for a second
    binary) leaves the replica serving, its checks counted as
    `unverified{reason="build"}`;
  - integration, no false positives across the transitions of decision 12:
    a replica that joins mid-run (journal catch-up, then live), one
    restarted from its own snapshot, a primary restarted (recovery replay)
    under traffic, and a promotion, each followed by a sustained run of
    checks with nothing diverging, with ticks on, driven by finding 7's
    rule-following test app, whose reports carry its tick-driven clock;
  - the primary's notice handling over DPDK, under the runner.
- Docs: `docs/replication.md`, "Output verification" extended to the
  replica check and the notices. `CHANGELOG.md` **Changed**: a replica
  whose output disagrees with its primary's now stops, and a primary whose
  output disagrees stops every replica (under a replica-backed ack policy,
  writes then halt); applications whose `tick` is not idempotent against a
  time already passed are named as at risk until monotonic sequencer time
  lands (finding 7).
- **Leaves untested:** a lapped window end to end (it needs a stall past
  decision 4's bound; the comparator's property test covers the verdict); a
  primary restarted across a clock step back with an application whose
  `tick` breaks the rule (finding 7): the tick generator reads the wall
  clock and has no seam a test can step back, and the monotonic-time plan's
  acceptance tests add exactly that case, which with this plan in place
  also shows as a divergence until monotonic time lands.

### Step 6. Measurement and the operator runbook

- The measurement of decision 14, the hash micro-benchmark included, in a
  dated document naming its hardware. If the response stage's tail moves,
  open questions 1 and 5 decide.
- `docs/replication.md`, "Output verification", completed for operators,
  with no implementation detail:
  - **the guarantee** in the words of "The guarantee, and its limit" above,
    and **the limit** (latent divergence, the unverified tail at failover,
    queries, nodes without snapshots);
  - **what an operator sees**: the `error!` lines, the gauges and counters
    to alert on (`melin_output_diverged`, `melin_replica_output_divergence_total`,
    `lapped`, an interval of 0), the marker's contents, the refused
    restarts;
  - **consequences**: a nondeterministic primary stops every replica (each
    sees the primary's output disagree) and, under a replica-backed ack
    policy, the primary then halts writes for want of a replica; under
    `disk` it keeps serving alone. The primary's log says when every replica
    disagreed at the same window, which points at the primary. A
    nondeterministic replica stops alone and the cluster runs on with one
    copy fewer. A diverged primary stops and a replica is promoted. A
    diverged node is never promoted and does not come back by itself;
  - **which copy is at fault**: several replicas at the same window point
    at the primary; one replica alone points at that replica (or its
    host); a divergence soon after a restart, a failover or a clock step
    points first at how the application handles a tick for a time already
    passed, a rule its contract states and the runtime does not yet make
    impossible (finding 7);
  - **bringing a node back**: read the marker; decide which copy is at
    fault; fix the application (a determinism bug) or the host (memory);
    rebuild the node's state, either by reseeding (move its journal and
    snapshots aside and start it as a replica, so it takes the primary's
    snapshot) or by replay from before the divergence (keep the journal,
    keep or restore a snapshot at or below the verified position, the
    archived ones being the suspect ones, kept for investigation); delete
    the marker; start it;
  - the `--output-digest-interval` flag and its trade-off (detection delay
    against frames and work per window).
- `docs/internal/application-api-review-2026-09.md` S8 points here; the
  pipeline architecture doc's responsibilities table names the folders.
- **Leaves untested:** the runbook is prose; its procedure is exercised by
  step 3's integration tests (the refusal, then the way back: marker
  deleted, journal replay from a kept snapshot, serving again) and step 5's
  (a stopped replica's refusal). Reseeding a cleared replica from its
  primary is not a new path: it is the existing bootstrap of a replica with
  no local state, which its own tests cover.

### Step 7. Roadmap: the application state digest

Add a row to `docs/internal/roadmap.md` ("Next"), roughly:

> **Application state digest** (Medium). Output digests catch a divergence
> only once a report shows it. Two copies whose state differs where no
> report has yet looked (latent divergence) are both verified, and the
> drifted one may be promoted at a failover, after which its reports
> differ from what the old primary's would have been, and its own replicas
> stop on the first one. A digest of application state, at the same window
> boundaries, closes that. It plugs into what output digests built: a new
> kind value on the same digest frame and `StreamStart` schedule table (no
> replication protocol bump), the same 16-byte digest, the same board and
> comparator, the same stop. The new surface is the application's: an
> opt-in digest function whose contract is the logical state, independent
> of capacity and iteration order. An application that already keeps an
> incremental state hash, as the Exchange Core does (Theia), exposes it
> directly, reduced to 16 bytes by the kind's definition if it is wider.
> Plan:
> [divergence-detection-2026-10.md](divergence-detection-2026-10.md).

- **Leaves untested:** a roadmap row.

## What this leaves untested, and why

Beyond each step's own list:

- **Hardware faults** (a bit flip in live memory) are modelled only by the
  fault-injecting apps; the detection path is the same.
- **Mixed builds during a rolling upgrade** are covered by step 5's
  receiver tests and its mixed-build integration case, which sets the build
  id through configuration, not by running two binaries: the hash of the
  executable itself is read by one function, tested on the test binary.
- **The fold's cost** is measured, not tested: a wall-clock threshold in a
  test flakes.

## Open questions and trade-offs

1. **The hash.** XXH3-128 through `twox-hash` (recommended, decision 1)
   or BLAKE3 (already in the tree, but a cryptographic hash paying a serial
   block compression for every 64 bytes folded, for a guarantee this check
   does not need). Collisions are accidental, not adversarial, so either
   is adequate; the difference is cost on the response thread.
   Recommendation: XXH3-128, confirmed by step 6's micro-benchmark. If a
   crate outside `deny.toml`'s allow list were ever needed instead
   (`xxhash-rust` is BSL-1.0, the Boost Software License), its allow
   entry must say in a comment that it is Boost and not BUSL-1.1, the
   Business Source License the file reserves for our own crates. The
   frames fix the digest at 16 bytes for every kind (decision 7), which
   suits any later digest compared between copies, a state digest
   included: its collisions are just as accidental. An application
   hash wider than that (a state hash of its own) is reduced to 16 bytes
   by its kind's definition, and a cryptographic hash truncated to 128
   bits still leaves accidental collisions out of reach. A digest that
   must be tamper-evident to a third party is a different job: it needs a
   wider digest field, so a new frame and a protocol bump, and is not
   what these frames are for.
2. **Comparing across builds.** A rolling upgrade that changes a report's
   layout, or the encoder, makes a mixed pair disagree by design, and under
   this plan the replica stops. Options: (a) document that such upgrades run
   with digests off on the primary; (b) `StreamStart` carries a build id (a
   BLAKE3 hash of the running executable, read once at startup, in
   `server-runtime`, which already depends on `blake3` unconditionally; it
   stays BLAKE3 and 32 bytes, since it is computed once, off every hot
   path, so decision 1's cost argument does not reach it), and a replica
   compares only against the same build, counting
   `unverified{reason="build"}` otherwise; (c) an application-declared
   output version, rejected because it is the trait surface this plan
   avoids. Recommendation: (b). It needs nothing from the application, it
   only loses coverage while a pair runs two builds, and an operator cannot
   forget it. A node that cannot read its own executable sends an all-zero
   id, which a replica treats as matching any build, and logs `warn!` at
   startup: skipping the check on such hosts would lose it silently, while
   comparing at worst stops a replica loudly.
3. **Should a primary stop when every replica reports the same window?**
   Its output is then the likely fault, but with one replica the question is
   undecidable, and under a replica-backed policy the primary halts writes
   anyway once its replicas have stopped. Recommendation: no automatic stop;
   the `error!` and the counter page the operator, who can stop it. Revisit
   with replica-to-replica comparison, if ever.
4. **Clearing the marker.** Deleting the file, or an admin command.
   Recommendation: deleting the file. It is an explicit act, works when the
   node will not start (which is exactly when an admin socket is not
   there), and is what the refusal's message says.
5. **Where the primary folds.** In the response stage after the send
   (recommended: no new thread, the bytes are already encoded there), or in
   a dedicated output-ring consumer, the same code as the replica's drain,
   which takes the fold off the response thread entirely at the cost of a
   core and a second encode. Recommendation: the response stage, and the
   dedicated consumer only if step 6 shows the response thread's tail move.
6. **Closing windows downstream of the output ring.** The applied cursor
   (recommended: one store per matching batch, no new payload) or a marker
   slot the matching stage publishes at each window end (a per-event compare
   on the matching thread, and a new `OutputPayload` variant, breaking for
   publishers, which the gate must also pass without waiting).
7. **A stop that does not finish.** The stop exits through the node's
   ordinary shutdown, which fenced nodes already take. If a thread never
   joined, a fenced, diverged raft leader could hold its leadership until it
   is killed. Recommendation: no watchdog in this plan; the fenced-shutdown
   path is the same one superseded primaries rely on, and a hang there is a
   bug for both. The marker is written by the detecting thread before any
   join (decision 10), so even a stop that hangs and is killed leaves a
   node that refuses to restart.
8. **Ordering against monotonic sequencer time.** (a) Make the
   monotonic-time plan a build prerequisite of step 3; (b) land steps 1 to
   5 in any order against it, but release step 5 only with or after it;
   (c) release independently and rely on the contract and the runbook.
   Recommendation: (b). Step 3's in-process check compares two copies of
   one pipeline, which share their watermark's start, so the trap cannot
   trip it, and holding it back would delay the snapshot/restore check for
   nothing (finding 7). Step 5 is the first comparison across a restart,
   and shipping it ahead of monotonic time would make an application's
   repeated-tick handling, today a silent and rare divergence, the likeliest
   cause of a cluster-wide stop. The stop would still be correct, which is
   why (c) is defensible, but an operator's first encounter with the
   feature should be their own bug, not the runtime's known one. If
   monotonic time slips, (c) with the Exchange Core's `tick` made
   idempotent first is the fallback.

## Not part of this plan

- An application state digest (step 7 records it on the roadmap).
- Any automatic repair, retry or resync of a diverged node.
- Journaling digests, or carrying them in snapshot headers (both formats
  stay untouched).
- Digests on the event publisher's feed: a feed subscriber runs no
  application and produces no reports, so it has nothing to compare an
  output digest against. A state digest (step 7) is the kind a subscriber
  could check, and would be the time to revisit this.
- Replica-to-replica comparison and majority verdicts.
- Making `apply` deterministic: this plan detects nondeterminism; the
  determinism work removes sources of it. "Monotonic sequencer time" is the
  one this plan depends on for its release (finding 7, open question 8),
  not for its build.
