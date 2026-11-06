# Divergence detection (plan)

Status: **proposal, trimmed to the essentials** (2026-11-06). Implements
item S8 of the [application API review](application-api-review-2026-09.md),
"No divergence detection for a nondeterministic `apply`". The full plan,
with the in-process check, the lingering endpoint, the build ids, runs
kept across reconnects and the rest, is in git history (this file at
b403d38b); what it adds beyond this scope is summarised under "Deferred",
which is where a follow-up plan starts. Its
[assessment](divergence-detection-assessment-2026-10.md) and the
[cluster-halt evaluation](divergence-cluster-halt-evaluation-2026-10.md)
cite that full version's step and decision numbers, not this one's.

Code is cited against main plus the raft term floor (b403d38b), which this
branch stacks on. Names, not line numbers, are the anchors: search for the
function or type named.

The goal: **every copy of the application digests the reports it produces
in windows of a fixed number `K` of entries and chains the window digests.
The primary sends its digests down the replication stream as a control
frame, and each replica compares them with its own. A mismatch stops the
whole cluster, raft included, for good: the way back is a new cluster.**

The hash chain proves that every node holds the same *input*. Nothing
proves that the same input produced the same *output*. A nondeterministic
`apply` (a randomly seeded `HashMap` iterated, the wall clock read, an
allocation address hashed), a `snapshot`/`restore` pair that drops a
field, or a memory error leaves every journal bitwise identical while the
copies answer differently, silently, until a failover serves the drifted
copy.

What is digested is the **output**: the bytes of every report the
application's encoder produces for a journaled event, computed in the
runtime for every application, with no trait to implement. It is on unless
the operator turns it off. Copies are compared whatever build each runs: a
copy whose output differs for the same input has diverged, whichever
release or architecture produced it. Which copy is wrong does not matter,
and nothing is repaired. The replica that sees the mismatch stops and tells
its primary, which stops and tells its other replica. Every stopped node
refuses to start again.

The journal stays input-only and the snapshot format is untouched. Nothing
new is journaled or stored: each copy's chain starts when its pipeline
starts. Raft carries nothing new.

## The guarantee, and its limits

- **Once a replica's reports are seen to differ from its primary's, the
  cluster stops serving.** The replica stops at once: it cannot be
  promoted and refuses to restart. It tells its primary on its session, or
  by redialling it, and the primary stops the same way: it sends no further
  reply and tells the replicas streaming from it. With
  at most three raft voters (on `main`), two stopped nodes leave no
  quorum, and a promotion is confirmed by a fresh majority round trip
  before it takes effect (on `main`), so no automatic failover follows.
  The only way back is a new cluster.
- **Copies on different builds are compared like any others.** An
  application release, an architecture (a floating-point function can
  round differently on x86 and ARM), or a binary built per host: if the
  output differs for the same input, the cluster stops. So an application
  upgrade that changes its output cannot roll node by node with output
  digests on: stop every node and start them together on the new build. An
  upgrade that keeps output identical can still roll. Nothing names the
  builds involved: the operator checks them.
- **The limits.**
  - **Only replicas check.** There is no in-process check: a standalone
    node, or a primary with no replica connected, detects nothing. A
    `snapshot`/`restore` pair that loses state shows only once a node that
    restored such a snapshot answers differently from its primary.
  - **A primary the detector cannot reach is not told.** A replica that
    cannot deliver its notice, in session or by redialling within a bound,
    stops alone, and the primary and the other replica may go on serving.
    That needs the primary to be unreachable from the detector, and only
    from it, at the moment of detection: a partition, or a primary down
    and restarted before the redial ends.
  - **A replica not streaming live when its primary stops, or whose
    `Halt` is lost to the close, is not told** (between sessions, still
    catching up, or a kernel TCP close that resets the connection before
    the `Halt` is read). It
    stays up, not serving, and cannot be promoted automatically, since the
    two stopped nodes leave no quorum. It is stopped by hand.
  - **A manual promotion overrides the stop**, as does a cluster without
    raft or with raft observational, where only the operator promotes.
  - **Divergence that has not yet shown in any report is not caught.**
    Two copies can hold different state and answer every request
    identically until a report depends on the difference, which may be
    after a failover has promoted the drifted copy.
  - **Each session verifies from its first digest frame on.** Every
    replication session, a reconnect to the same primary included,
    re-anchors on the primary's first frame: the windows applied before
    that frame, the anchor's own window and the session's catch-up
    included, are not compared. So are the windows a replica's own board
    overwrote before its receiver read them.
  - **The detection delay is bounded in entries, not seconds.** A
    divergence is seen once its window of `K` entries closes on both
    copies and the primary's next frame arrives: within `K` entries plus
    one fsync batch, plus the replica's ack under a replica-backed ack
    policy. An idle primary journals one tick per tick interval, so idle
    it is about `K` tick intervals. With ticks off, an idle window waits
    for traffic.
  - **The tail at failover is not verified**: windows not yet compared
    when a primary stops.
  - **Queries are not covered**: they are not journaled, so no other copy
    answers them.
  - **Coverage is shown only as a count.** Each replica counts the
    comparisons it found equal; no position range is reported as
    verified.

## What the code shows

### The chain check the digest mirrors

- **Emission.** The journal stage is the sole producer of the two
  replication rings (`transport-core/src/pipeline.rs`). Every batch goes
  through `publish_input_batch_to_rings`, which calls
  `finalize_input_batch` (back-filling the `InputBatch` length from the
  buffer) and then `publish_to_replication_rings`, from `submit_batch`
  (which `drain_remaining` also reaches) and from the flush in
  `maybe_rotate`. `maybe_publish_chain_check` publishes a `ChainCheck`
  every `CHAIN_CHECK_INTERVAL_BATCHES` batches as a slot of its own,
  after the batch, so it reaches the wire after the entries it covers on
  both transports.
- **The ring slot is bytes, not a frame.** A replication ring slot is a
  `CHUNK_SIZE` buffer (`journal/src/replication.rs`); `try_publish` copies
  any byte count up to it with an `end_sequence`, both senders forward a
  slot's bytes verbatim, and the receiver extracts frame by frame. So two
  frames in one slot read as two slots would. `max_journal_batch` bounds
  a batch by `chunk / entry_size`, which leaves free bytes in a full chunk
  only as a by-product.
- **Wire.** Control frames carry a one-byte tag (`MSG_*`,
  `replication/protocol.rs`, `0x01` to `0x07`, `0x10` to `0x18`, `0x30`;
  `MSG_INPUT_BATCH = 0x21` in `replication_wire.rs`) behind a 4-byte
  length prefix. `encode_chain_check` returns a fixed-size array
  (`SEQ_HASH_FRAME_LEN`), the pattern for fixed-size control frames.
  `REPL_PROTOCOL_VERSION` is 9, unreleased; the version is checked at
  authentication (`judge_challenge_response`, `auth.rs`), where a primary
  admits a replica at or above its own, and again on the handshake.
  `HandshakeFrame` and `AckFrame` are fixed `zerocopy` layouts with the
  version appended last in the handshake.
- **Catch-up.** `RingChunk::classify` (`replication/catchup.rs`), shared
  by the kernel sender's drain and the DPDK sender's `LiveHandoff`, peeks
  only a slot's first frame tag: an `InputBatch` slot is `Covered`,
  `Next` or `Ahead` by its sequences, whatever follows it in the slot.
  Journal-file catch-up builds its own frames and carries no control
  frames from the ring.
- **Receipt.** `process_streaming_frames`
  (`server-runtime/src/replication/receiver_transport.rs`, run by
  `streaming_loop` for both receivers) hands `Rotate` and `ChainCheck` to
  `queue_stream_mark`: at the stream position queued, behind dropped, ahead
  a `FrameError::SequenceGap`. The promotion drain,
  `process_drain_frames`, skips every frame that is not an `InputBatch`.

### Where reports are produced, on each copy

- **One producer.** Live matching and journal replay hand events to the
  application through `dispatch::dispatch`, which fills a `Vec` of
  reports.
- **The matching stage publishes every report** (`MatchingStage::run`):
  one `OutputPayload::Report` slot per report, stamped with the entry's
  `wire_seq`, ticks' and other node events' (`connection_id == 0`)
  included. An entry with no report publishes a `BatchEnd` slot only if
  its connection is not 0, so a replica, whose slots all carry connection
  0, publishes nothing for it. Queries publish a `QueryResponse`, on the
  primary only. `wire_seq` is the primary's allocation carried in the
  stream, so a grid on it lands on the same entries everywhere.
- **The primary encodes only what it delivers.** `response_loop::run`
  hands each slot to the transport's `Egress::send_slot` as a `SlotReply`,
  and the egress encodes it (`SlotReply::into_frames`) only once it has
  found the connection. A slot for a connection it does not hold, a node
  event's or a gone client's, is `SlotOutcome::Unknown` and never encoded.
  An encode error travels in `SlotFrames::payload`, and the egress logs it.
- **A replica encodes nothing.** Its output ring's one consumer is the
  `"drain"` thread (`replication/mod.rs`), which discards slots.
- **Encoding is the application's, framing the runtime's.**
  `frame_app_response` (`server-runtime/src/response_frame.rs`) zeroes the
  body region, calls `ResponseEncoder::encode_report`, bounds the result by
  `MAX_RESPONSE_BODY` and writes the header; debug builds first run
  `check_encoder_purity`. The encoder's contract (`ResponseEncoder`'s
  "Purity", and "The determinism rules" in
  `docs/building-an-application.md`) already says the same value encodes
  to the same bytes.
- **Ticks.** The primary journals a `Tick` every `--tick-interval-ms`
  (default 250; 0 disables), each an entry with its own `wire_seq`.

### What can stop a node

- **The fence latch.** `FenceState` (`transport-core/src/fence.rs`):
  `fence()` latches once. A fenced node's readers close every client
  (`HaltGate::verdict`), and raft's auto-promotion and election paths
  refuse on `fenced` (`raft_promotion.rs`). `fence_if_superseded` co-sets
  the node's shutdown flag beside the latch, so every stage leaves.
- **Raft leaves with the node.** The raft driver runs on the node's
  shutdown flag and leaves within its `SHUTDOWN_POLL`
  (`raft/src/driver.rs`); a node that refuses to start never spawns it
  (`spawn_raft_driver` runs after the store is resolved).
- **Two stopped nodes leave no quorum (on `main`).** Raft voters are capped
  at `melin_raft::MAX_VOTERS`, one plus the primary's replica slots
  (`ReplicaSlotCursors::SLOTS`), checked on the flags and on the stored
  membership. A raft leader cut off from its quorum keeps its role, but a
  promotion is confirmed by a fresh majority round trip before its epoch is
  journaled (`journal_promotion_epoch_bump`, `LeaderConfirm`), and
  abandoned otherwise. Fresh raft directories start their term at the
  journal's epoch (b403d38b), so a new cluster can fail over at once.
- **Acks are read in a few places.** Kernel TCP reads acks in the live
  loop only (`tcp_sender.rs`); both transports' streaming loops go through
  `drain_ack_frames` (`receiver_transport.rs`), and the DPDK join keeps
  only the newest ack (`keep_last_ack`, `dpdk.rs`, compiled under the
  `dpdk` feature). Every ack that moves a cursor passes
  `ReplicaCursors::record_ack` (`transport-core/src/replication/cursors.rs`).
  Read sites skip frames they do not decode.
- **The primary reads the handshake in one place per transport**, after
  authentication and before the fence and chain checks:
  `handle_replica_connection` (`tcp_sender.rs`) and the DPDK
  `Handshaking` arm.
- **The replica's session.** `streaming_loop`'s shutdown arm runs
  `drain_pending_acks`, which sends an ordinary cumulative ack. Promotion
  requests are honoured in `streaming_loop` and at each receiver's
  reconnect-loop top, both after a shutdown check. Every session exit goes
  through the shared `handle_session_exit` (`replication/mod.rs`), whose
  `match exit` over `SessionExit` is exhaustive. Reconnect backoff tops
  out at `MAX_BACKOFF` (30 s).
- **The response loop checks shutdown once per batch**, and a send with
  room goes through whatever the flag says.
- **Every entry resolves its paths through `NodeStore::resolve`**
  (`transport-core/src/node_store.rs`, through `ServerConfig::store()` in
  `run_tcp` and `run_dpdk`), "reads the filesystem, writes nothing", before
  the journal is opened and before raft starts.

## Findings that shape the design

1. **The primary's encode must leave the egress.** Folding what the egress
   encodes would make the digest depend on which clients are connected,
   and a replica, which delivers nothing, would disagree with every primary
   that ever saw a client go. The loop encodes every `Report` slot itself,
   folds it, and hands the egress frames (decision 4).
2. **Only `Report` payloads are the same on every copy.** `BatchEnd`
   slots and query responses exist on the primary only; request and
   connection IDs and trailers differ by node. The digest covers report
   bodies and their positions, nothing else (decision 1).
3. **Downstream of the output ring a window cannot close by position
   alone.** An entry with no report publishes nothing on a replica, so a
   window of report-less ticks would never close. The matching stage
   publishes how far it has applied (decision 4).
4. **On a replica the cost lands on the drain thread.** Encoding and
   folding there does not lengthen `apply`, but a drain that falls behind
   fills the output ring and, through the matching stage and the input
   ring, delays the replica's acks.
5. **Either side of a comparison can arrive first.** The primary's digest
   exists only once its response stage has folded the window, after the
   gate released it, so under a replica-backed policy after the replica's
   ack. A replica's matching stage can also trail its receiver. The
   comparator holds both sides (decision 6). The primary still sends each
   frame behind the entries it covers (decision 7), so a digest ahead of
   the stream is a protocol error.
6. **Raft carries nothing: the stop travels the data plane, and quorum loss
   does the rest.** A stopped node latches the fence first, so raft refuses
   to promote it, and its driver leaves on the shutdown flag. Once two of
   at most three voters have stopped, no election succeeds and no
   promotion is confirmed. The stop reaches a second node whenever the
   detector can reach its primary, since only replicas detect.
7. **Copies applying the same journal make the same tick calls.** The tick
   watermark is carried across replay, snapshot restore, resync and
   promotion on `main` (merge 87e36302, pinned by
   `server-runtime/tests/tick_watermark.rs`). What remains of "Monotonic
   sequencer time" (time going backwards, the double call per journaled
   `Tick`, the producer race) is a function of the journal, so it reaches
   every copy alike. The one residue: a snapshot written before snapshot
   framing version 5 records no watermark, and restoring one logs a
   warning; the runbook names it as a suspect. The no-false-positive tests
   use `TickCountingApp` (`transport-core`'s `test_support`), whose
   reports count every tick call, so a copy making one call more or fewer
   would stop the cluster.

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

- **`body` is exactly what the encoder wrote**, after the runtime's bound
  check, without the runtime's header or trailer. A failed encode (the
  encoder's `Err`, or a body over `MAX_RESPONSE_BODY`) folds
  `body_len = u32::MAX` and no body: failing alike is agreement.
- **Folded:** reports of node events (connection 0) and reports to gone
  clients. **Not folded:** `BatchEnd`, `QueryResponse` and ack-policy
  change slots, IDs, trailers, early replies (finding 2).
- **The encoder's contract is the one the digest needs**: the same value
  encodes to the same bytes, every byte below the returned length written.
  The runtime's zeroing makes a skipped byte zero on every copy. What
  changes is the consequence of breaking it, which the contract text and
  `CHANGELOG.md` **Changed** state, adding "no node identity" to its
  examples.
- **XXH3-128, not BLAKE3.** Only accidental collisions matter, and at 128
  bits they are out of reach. BLAKE3's adversarial resistance buys
  nothing here, and a stream fed one short report at a time pays a full
  serial compression per 64 bytes. Step 1's micro-benchmark confirms the
  choice. The journal chain keeps BLAKE3: it is the audit trail, where
  adversarial resistance is the point.
- **`twox-hash` 2.x** (MIT, already allowed by `deny.toml`), with
  `default-features = false, features = ["xxhash3_128", "std"]`, which
  drops `rand`. `std` turns on the SIMD stripe loop for inputs past 240
  bytes (without it the loop is scalar), and costs nothing in a crate that
  links `std`. The window digest streams through
  `xxhash3_128::RawHasher::new(SecretBuffer::default())`, a plain value with
  no heap, made fresh per window since it has no reset (the crate's
  `Hasher` instance boxes the secret, and the response thread must not
  allocate). The chain step uses `Hasher::oneshot`. Bytes are
  `finish_128().to_be_bytes()`, the reference's canonical form. The crate
  is checked against xxHash's own published test vectors, and the digest's
  definition by hashing bytes built by hand in one shot.
- **Two newtypes**, `WindowDigest` and `ChainValue`, each `[u8; 16]`, so
  the type system rules out comparing or chaining one as the other. Byte
  arrays rather than `u128`: no byte-order question, and alignment 1 keeps
  the board's payload free of padding.
- **The domain string, the kind and `K` are the definition.** Changing any
  of them later is a new kind value on the same frame (decision 8).
- **The encoded body, not `std::hash::Hash`** on the typed report: `Hash`
  may skip fields, is not portable across platforms or compiler releases,
  and is a second description of a report beside the encoder.

### 2. Windows by position, a fixed K, and the off switch

- **Windows are absolute positions.** Window `m` holds `wire_seq`
  `(m-1)·K + 1 ..= m·K` and is named by its end `m·K`, so every copy cuts
  the same grid with no coordination.
- **A copy digests only windows it saw whole.** A folder whose pipeline
  applies its first live entry at `s` digests first the window ending at
  `F = (ceil((s-1)/K) + 1)·K`. Replay is not folded.
- **`K = OUTPUT_DIGEST_WINDOW = 64`, a constant of kind 1.** Bounded below
  by the per-window work (a fresh hasher, a finalize, a one-shot chain
  hash, a 40-byte seqlock store, and on the replica's receiver a board
  read and a chain hash), which divided by `K` must stay a small share of
  the per-event budget, and by the board's size (decision 5); bounded above
  by the idle delay (`K` tick intervals, about sixteen seconds at the
  default). The micro-benchmark confirms it (open question 1).
- **`--no-output-digests`** (`ServerConfig::no_output_digests`, default
  `false`): the node builds no folder and no board, a primary sends no
  digests, and a replica decodes and drops them. A mismatch of the flag
  across nodes only loses coverage. A node logs `warn!` at startup when it
  is set. A node honours a stop whatever its own flag.

### 3. The chain, anchored per session

- **Every folder chains its windows**: `C_i = XXH3-128(C_{i-1} || d_i)`
  (one-shot over 32 bytes) from `C = CHAIN_SEED` (16 zero bytes) before its
  first window `F`. The chain lives as long as the folder's pipeline:
  nothing is stored or negotiated.
- **Why a chain.** The primary forwards at most one window per fsync batch
  (decision 7), and a board can overwrite a window before a reader takes it,
  so most windows are never sent. One equal chain value vouches for every
  window since the run's start: sampling costs delay, never coverage.
- **Anchoring.** The two chains started at different positions, so the
  replica cannot compare its own `C` with the primary's. It takes the
  first primary frame it can use in a session, at window `e₀`, as its
  starting value, without checking it, and extends it with its own window
  digests: `C'_{e₀+K} = H(C_{e₀} || d_{e₀+K})`, and so on. A later frame at
  `e` is compared against `C'_e`. A match verifies every window from
  `e₀ + 1` to `e`; a mismatch says a window in that range differs. The
  frame carries `C_e` only, so the anchor's own window is taken on trust.
- **Every session re-anchors.** The replica cannot tell whether a primary
  it reconnects to is the same pipeline or a restarted one (a restart keeps
  its address, identity, journal and epoch), and keeping a run against a
  restarted chain would be a false mismatch. So each session start ends the
  run, and the session's first usable frame anchors anew.
- **A local gap breaks the run, never the verdict.** If the replica's board
  overwrote a window before the comparator copied it, the extension cannot
  continue: the run ends and the next usable frame anchors anew.

### 4. Where each copy folds

`transport-core`'s new `output_digest` module holds `WindowFolder`: it
folds `(wire_seq, body)` into the open window, closes windows as positions
pass, extends its chain, and publishes each closed window to the digest
board (decision 5). The caller encodes the body and hands the folder the
bytes, or `ReportBody::Failed`.

The body encode moves out of `response_frame.rs` into one free function in
that module, `encode_app_body`: the zeroing, the debug-build purity check,
the encoder's call and the `MAX_RESPONSE_BODY` check, everything
`frame_app_response` did except the header (named for every application
body, since query responses go through it too). `frame_app_response` keeps
the header and calls it; early replies call it without a folder. So the
replica's drain gets the same zeroing and purity check as a client's
frame, with no second copy. `MAX_RESPONSE_BODY` moves with it, and
`server-runtime` keeps re-exporting it at its root.

| Copy | Thread | Closes a window when |
| --- | --- | --- |
| Primary | response stage (`response_loop::run`, both transports) | a slot is read whose position the window cannot reach (a `Report` or `BatchEnd` past its end, a query slot at or past it), or the output ring is empty and the matching stage has applied past its end |
| Replica | the drain thread, hoisted out of `replication/mod.rs` into a function with tests | the same rule |

- **The matching stage publishes its applied position**: one `Release`
  store of `next_wire_seq - 1` per batch, after `out_batch.commit()`, into a
  `WireSeq` cursor in `PipelineCursors`. A consumer loads it (`Acquire`)
  *before* reading the ring; if the ring is then empty, every window ending
  at or below it can close. Windows with no report close with their header
  alone. This is the matching stage's only change.
- **Any slot closes what lies behind it**: the output ring is in `wire_seq`
  order, so a busy primary whose requests produce no reports still closes
  windows. Which window a report belongs to is fixed by its position; only
  the moment of closing moves.
- **The primary folds every report.** `Egress::send_slot` takes `SlotFrames`
  instead of a `SlotReply`, so the egress no longer sees the encoder. For
  each `Report` slot the loop encodes through the folder, offers the frames
  to the egress (when the gate gave a reply and the stop latch is clear,
  decision 10), then folds the body whatever `SlotOutcome` came back: the
  hash is off the reply's own latency. `SlotOutcome::Unknown` comes to mean
  "not sent". The egress logs an encode error as today, for a connection
  it holds; a failure for a report nobody receives is folded and not
  logged, so an encoder that refuses an internal report fills no log.
- **The replica encodes with the node's encoder** (an
  `Arc<dyn ResponseEncoder>`), handed to the replica builder.
- **Off is no folder**: the loop hands the egress frames encoded as today,
  without the fold.

### 5. The digest board

Each folder publishes to a digest board (`digest_board`, a
`DigestBoardWriter` for the folder and a cloneable `DigestBoardReader`):
`R` slots of `(window_end, d, C)`,
window end `E` at slot `(E / K) % R`, each a `melin_pipeline::seqlock` over
a 40-byte `repr(C)` payload (`u64`, then two `[u8; 16]`, no padding, so it
satisfies `NoPadding`), plus an `AtomicU64` holding the latest window end.
A seqlock rather than a queue: a folder must never wait on a reader, and a
reader that falls behind should lose old windows, not stall anyone. Its
readers are the primary's journal stage and the replica's receiver.
Lookups are `get(end)` (present, not yet written, or overwritten) and
`latest()`.

`R = ceil((input ring slots + output ring slots) / K) + 2`, computed from
the pipeline's `RingCapacity` (both rings use it) by one function that the
comparator's constructor shares: `BoardSize::for_pipeline`, a newtype
that also owns the slot index, so the two cannot size or index
differently; tests build small ones (`BoardSize::new`) to force
overwrites. Only the replica's comparator needs every window. The drain can
close at most its backlog between two receiver reads: the entries in the
input ring, those whose reports wait in the output ring, both covered by
`R`, and applied entries that published no slot, which no ring bounds. So
a lap needs a drain stalled while about `R·K` report-less entries are
applied. It is a break in the run (decision 3), never a wrong verdict.

### 6. The comparator

A `DigestComparator<T: Trip>` (`transport-core`, no I/O, no threads) holds
the node's stop `T` and two fixed arrays of `R` slots, indexed as the board
is: **local** `(end, d, C')`, pushed strictly in order, `C'` filled inside a
run; **expected** `(end, C)` from the primary, not yet compared. Nothing
allocates after construction. It is built with `F`, the local folder's first
window, so an expected entry arriving before any local window cannot anchor
below it.

- **Local push.** An end that is not the previous plus `K` is a gap: the run
  ends and `Break` is returned. Otherwise the window is stored and, inside a
  run, extended; any expected entry waiting for that end is compared.
- **Expected entry, no run.** An entry at `e` anchors if every local window
  after `e` is held or still to come, unbroken: `e ≥ F - K`, `e` at or above
  the last gap, and window `e + K` not evicted. Anchoring computes `C'` for
  every held window after `e`, and returns `Anchored`. Otherwise it is
  dropped and a later entry is tried.
- **Expected entry, inside a run.** At or below the last match: dropped as
  re-delivery. At a window extended to: compared at once. Ahead: it waits;
  when the array is full, the oldest waiting entry gives way. One whose
  local `C'` was evicted is a skipped sample. A dropped entry costs delay:
  the next comparison covers its windows.
- **`begin_session()`** ends the run and clears the expected side; the local
  side is kept.

Verdicts: `Match { end }`; `Mismatch { divergence, halted }`, where
`divergence` is `{ end, run_from, last_match: Option<WireSeq>, expected:
ChainValue, actual: ChainValue }` and the differing window lies in
`(last_match or run_from - 1, end]`; `Anchored { end }`; `Break { after }`.
After a `Mismatch` the comparator is spent.

**The comparator trips before it returns a `Mismatch`.** `Trip` is a trait
(`fn trip(&self, Divergence) -> Option<Self::Token>`), and `halted` is what
it returned, `None` when another site tripped first. If the comparator only
returned the verdict, a call site that ignored it would compile: Rust has no
linear types, and `#[must_use]` is a lint. Holding the stop means no caller
can forget it. Generic rather than `dyn`: one implementation per build,
monomorphized. Step 2 implements it for `DivergenceLatch`; step 3's property
tests use a recording one.

### 7. The primary emits from the journal stage, in the batch's ring slot

The digest frame rides in the ring slot of the batch it follows. A slot of
its own, as `ChainCheck` takes, would cost a ring slot for nearly every
batch under load and roughly halve how long a replica can stall before the
full ring evicts it. Sharing the slot costs `DIGEST_FRAME_LEN` bytes per
batch: 30, the 26-byte frame of decision 8 and its length prefix.

`output_digest_trailer`, called inside `publish_input_batch_to_rings` before
the publish, returns `None` without replication producers or with digests
off. Otherwise, with `end` the batch's last sequence, it picks the newest
closed window ending at or below `end`: the board's `latest()` if it is at
or below `end` (the usual case: the response stage trails the journal
stage), else `get((end / K) * K)`, where an overwritten slot means no frame
this batch. If the pick is newer than the last forwarded, it encodes the
frame on the stack, and `publish_input_batch_to_rings` appends it to the
batch buffer **after** `finalize_input_batch` (which back-fills the batch's
length from the buffer, so a trailer appended before it would be swallowed
into the batch) and publishes the buffer as one slot with the batch's
`end_seq`.

- **One change point.** `publish_input_batch_to_rings(repl, end_seq,
  trailer: Option<&[u8; DIGEST_FRAME_LEN]>)`, a free function over
  `&mut ReplicationState`, which holds the board's read handle and the last
  forwarded end.
- **Order by construction.** The window ends at or below the slot's last
  entry, and the slot's bytes put the frame after every one of them, so the
  receiver's "digest ahead of the stream is a gap" rule cannot fire on a
  healthy stream, a handoff's partly duplicated chunk included.
- **Catch-up.** `RingChunk::classify` peeks the first tag only, so a slot
  with a trailer is classified by its batch: `Covered` drops it whole,
  digest included (a dropped digest costs delay), `Next` and `Ahead`
  forward it whole. Journal-file catch-up carries no digests.
- **Query-only batches publish no slot**; the next batch carries the newest
  window. Idle primaries journal ticks, so batches keep coming.
- **The bytes are reserved, not found.** The replication chunk's share of
  `max_journal_batch` becomes `(replication::CHUNK_SIZE - FRAME_HEADER_LEN -
  DIGEST_FRAME_LEN) / entry_size`, with a `const` assertion beside
  `Sequencer::MAX_BATCH` that a full batch plus its trailer fits
  `CHUNK_SIZE`. The ring's module doc states the slot's layout.
- **Independent of the journal chain.** Not inside
  `maybe_publish_chain_check`, and nothing behind `cfg(feature =
  "hash-chain")`.

### 8. Wire: protocol 9 additions

Version 9 is unreleased; these ride it if they ship in the same release,
and its history line gains them. If a release cuts 9 first, they are
protocol 10, a stop of every node.

| Change | Layout (after the length prefix) |
| --- | --- |
| New `MSG_DIGEST = 0x19`, primary to replica | `DigestFrame { tag, kind: u8, window_end: U64, chain: [u8; 16] }`, 26 bytes |
| `Ack` gains a flag byte | `AckFrame { tag, acked_sequence: U64, in_memory_sequence: U64, flags: u8 }`, 18 bytes, from 17. Bit 0, `STOPPED`: this replica has stopped, and its positions are the last ones it sent |
| `Handshake` gains the same flag byte | `HandshakeFrame { …, protocol_version: U16, flags: u8 }`, 52 bytes, from 51, appended after the version so an older primary still reads the version and refuses it |
| New `MSG_HALT = 0x1A`, primary to replica | `HaltFrame { tag }`, 1 byte: this primary has stopped |

- **Unknown flag bits are refused**: never read as a stop or as no stop.
  An ack refused for its flags is not skipped as an undecodable frame: it
  takes the ack-violation path (evict, `error!`), since a skipped one could
  be a stop the primary never sees. The decoder returns that refusal as an
  error of its own, so `drain_ack_frames` and `keep_last_ack` can tell it
  from a frame of another kind.
- **Fixed-size frames encode into arrays**, as `encode_chain_check` does:
  `encode_digest` returns `[u8; DIGEST_FRAME_LEN]`, `encode_halt`
  `[u8; HALT_FRAME_LEN]`, length prefix included, so a frame one byte short
  cannot pass a test and the journal stage's emit cannot allocate.
- **The kind byte from the start.** Only `1 = output` is defined; 0 is
  refused as malformed. A frame of a kind the replica does not know is
  skipped with a `warn!` once per session, so a later digest kind needs no
  version bump.
- **A window end that is not a positive multiple of `K`** is the primary's
  protocol bug: a `FrameError`, never a divergence.
- **Where a `Halt` is sent, and read.** A primary sends `Halt` only on a
  live streaming session and as its answer to a `STOPPED` handshake. A
  replica reads it in `process_streaming_frames` and in its stopped
  redial (decision 10); the promotion drain skips it as it skips every
  control frame. At the other read sites (handshake answer, snapshot
  transfer) it falls into today's "unexpected response" error, which a
  correct primary never causes and which, should it happen, leaves a
  replica that can join no quorum with two nodes stopped.

### 9. The replica compares in its receiver

- `process_streaming_frames` decodes `Digest`. `end` above the stream
  position is a `FrameError::SequenceGap` (the primary sends a digest only
  after its entries); otherwise the entry goes to the comparator.
- The comparator lives in `ReplicaPipelineHandles`, beside the live board
  handle, and both receivers lend it to `streaming_loop` by `&mut`, so it
  lives as long as the replica pipeline and its local windows survive a
  reconnect. A rebuild (snapshot resync, in-process chain repair) builds a
  new one. `streaming_loop` calls `begin_session()` once `StreamStart` is
  accepted, and at each loop iteration loads the board's latest end
  (`Acquire`), copying every new window in order only when it moved.
- A `Mismatch` (the latch already tripped, decision 6) ends streaming: the
  receiver publishes nothing more and runs the notice (decision 10).
  `Anchored` and `Break` are logged at `debug!`. Each `Match` adds one to
  `melin_output_checks_total`.
- A replica with digests off decodes and drops `Digest` frames.

### 10. On divergence: the cluster stops

A node stops for one of two causes, `HaltCause` (exhaustive):
`Detected(Divergence)`, by its own comparator (a replica's receiver), and
`ClusterHalted { by, via }`, when another node tells it (`by` the primary,
or a replica's slot and key fingerprint; `via` the ack, the handshake or
the halt frame).

**The latch.** `DivergenceLatch`, one `Arc` per node in `transport-core`
beside `FenceState`, built by the entry from the node's `FenceState` and
shutdown flag and handed to every pipeline builder, `ReplicaCursors` and
the receivers. `trip(cause)` keeps the first cause only (a second trip
returns `None`) and, in order:

1. **Latches the fence and sets the node's shutdown flag**, as
   `fence_if_superseded` owns "fenced ⇒ shutting down". From then: the
   readers close every client, auto-promotion refuses on `fenced`, the raft
   driver leaves, every stage leaves at its next check, and the response
   loop hands no slot and no early reply to the egress (a per-slot
   `Relaxed` load of the latch before each send, since the loop checks
   shutdown only per batch).
2. **Logs `error!`**: for `Detected`, the range known to hold the
   divergence, both chain values (`hex_prefix`, widened from `&[u8; 32]` to
   `&[u8]` rather than a second formatter) and the run; for
   `ClusterHalted`, who told it and how. Both end "the cluster is stopping:
   see docs/replication.md, Output verification".
3. **Starts the marker write** on a short-lived `divergence-marker` thread
   (the tripping thread writes it itself only if the spawn fails, logged
   at `error!`). The marker, `NodeStore::divergence_marker()`
   (`<journal>.diverged`), is written to a temporary file, fsynced, renamed
   and the directory fsynced. Plain text: a format version, the cause, for
   `Detected` the range, both chain values in full hex and the run, and the
   wall-clock time. The writer deposits `Written` or `Failed(io::Error)`
   in the latch's marker state, a `Mutex` with a `Condvar`, touched only
   on this cold path.
4. **Returns the `Halted` token** (private constructor, neither `Clone` nor
   `Copy`) to the first tripper.

**The notice waits for the marker, within a bound.** Telling another node
before the marker is down would let a crash leave a node that restarts
into the cluster it has just stopped, restoring a quorum. So every notice
waits for the marker state to leave `Pending`, up to
`HALT_NOTICE_BOUND` (5 s), then sends anyway, logging `error!` if the
marker is unconfirmed: a hung disk must not hold the notice, since a
killed node that told nobody leaves the cluster to fail over onto the
diverged build. The DPDK poll thread reads the state each iteration rather
than block. Writing on its own thread means no link-owning thread ever sits
in `fsync(2)`.

**A replica that stopped** (its comparator, or a `Halt`):

- `streaming_loop`'s loop top reads the latch before its shutdown arm, so a
  stopped replica never runs `drain_pending_acks` and sends no ordinary ack
  after the stop.
- Stopped by `Detected`, it sends one ack with `STOPPED` set and its
  positions frozen at the last ones sent, then waits up to the bound for
  the primary's `Halt`, reading and discarding every other frame and
  publishing nothing. Stopped by a `Halt`, it tells nobody: the primary
  knows.
- The session ends with `SessionExit::Stopped(HaltCause)`. Its arm in
  `handle_session_exit` (the exhaustive `match` makes it compile only with
  one) tears the pipeline down and returns, never reaching a
  reconnect-loop top where a promotion could be honoured.
- If no `Halt` arrived, the receiver redials `--replica-of` with `STOPPED`
  in its handshake until a `Halt` answers or `HALT_REDIAL` (60 s,
  `2 × MAX_BACKOFF`) runs out, with the reconnect loop's backoff. The
  redial uses each receiver's own dial, authentication and handshake code,
  factored out of its reconnect loop, not copied. The kernel dial uses
  `TcpStream::connect_timeout` with the time left (a bare `connect` waits
  out the kernel's SYN retries against a host that drops them), sets read
  and write timeouts no longer than what is left, and never passes them a
  zero `Duration`, which they refuse. A redial that ends unanswered logs
  `warn!`: the primary may not have been told.
- A `const` assertion keeps `5 × HALT_NOTICE_BOUND <= HALT_REDIAL`.

Only the receiver trips on a replica, so a trip and a promotion request
are handled on one thread in order: nothing promotes a stopped replica.

**A primary told by a replica** trips with `ClusterHalted` at the first
place each transport sees the flag:

- `ReplicaCursors::record_ack`, which holds the latch, checks `STOPPED`
  before its sent high-water check and before it stores, so a flagged ack
  is neither lost to an eviction nor moves a cursor. It returns
  `Result<AckRecorded, AckViolation>`, `AckRecorded::Stored` or
  `AckRecorded::Stopped(Option<Halted>)`, so no caller treats a stop as an
  ordinary ack.
- `keep_last_ack` returns `JoinFrames::Stopped` when any ack it reads
  carries the flag, and the DPDK `Joining` arm trips at once: a stopped
  replica stops acking, so a flag held for after the join might never be
  recorded. The function and `JoinFrames` move, ungated, beside
  `try_extract_frame` in `receiver_transport.rs`, so their tests run on
  every host.
- The handshake read, before the fence and chain checks, so a stopped
  replica is never sent a snapshot. The primary answers it with `Halt`.

The tripping sender sends `Halt` on its session once its in-flight send
completes, then closes. Every other live streaming session, leaving on the
shutdown flag with the latch set, sends `Halt` at a frame boundary before
closing. Sessions not yet live just close. A `Halt` lost to the close
costs the detector its redial, never the stop: the primary has stopped.

**The exit.** Every node entry (`run_tcp`, `run_dpdk`, and so `run`,
`run_with_shutdown`, `run_with_listener`) returns through one exit helper,
after every pipeline thread and the receiver are joined. A clear latch is
an ordinary exit. A stopped one waits up to one `HALT_NOTICE_BOUND` for the
marker and returns `Err(NodeDiverged { cause, marker })`, `marker` being
`Written`, the I/O error, or `Unconfirmed`, each but `Written` logged at
`error!`, since a restart may then not be refused. A return rather than
`process::exit`: a test or host may run several nodes in one process.

**Nothing is moved.** Every snapshot the broken build wrote is suspect, and
the journal is input-only and checked by its chain; the new-cluster
procedure sets snapshots aside (step 3).

**What "at once" means.** No slot is handed to a transport after the
response stage has observed the latch; bytes handed on before may still
leave. Detection trails the diverged window, so that window's replies have
gone out; the stop guarantees nothing further does.

### 11. Startup refuses while the marker exists

`NodeStore::refuse_if_diverged(&self) -> Result<(), NodeStoreError>`
returns `NodeStoreError::Diverged { marker, cause, window_end, stopped_at }`
when the marker exists, every field parsed from it; a marker that does not
parse still refuses, naming the file. Both entries move from
`ServerConfig::store()` to a new `ServerConfig::startup_store()` (resolve,
then refuse), the one place the refusal is made, before the journal is
opened and raft started; `resolve` and `store()` stay write-free, so tests
and tools can still resolve a stopped node's paths. The message:

> this node stopped on `<time>` because the cluster's output diverged
> (`<detected here, between positions <a> and <b>>` or `<told by <who> via
> <path>>`; details in `<marker>`). The application broke its determinism
> contract, or the builds the cluster ran do not agree, so this cluster
> cannot be restarted: do not delete this marker to bring this node back.
> Start a new cluster from the journal, on one build, with a fixed
> application or with output digests off. See docs/replication.md,
> "Output verification".

A stopped node cannot come back on its own and never rejoins raft. The
marker is moved aside, not deleted, by the new-cluster procedure, as the
incident's record.

### 12. Restarts, recovery, failover

| Situation | Behaviour |
| --- | --- |
| Primary restart (recovery from snapshot and journal) | Replay is not folded; the folder starts a new chain at the first full window after recovery. Its replicas' next session re-anchors. From the anchor on, the restarted primary's replayed state is checked against the replicas' live history |
| Replica booting from its own snapshot, or resynced | A new replica pipeline and comparator: anchors on its first usable frame. A lossy `snapshot`/`restore` shows at the first report it changes |
| Replica catching up from the primary's journal | No digests in the disk walk; the session anchors on its first ring frame after it |
| Reconnect to the same primary | Re-anchors: the windows between the last match and the new anchor are not compared |
| Promotion | The application moves into a new primary pipeline with a new folder and a new chain; its replicas re-anchor |
| Ex-primary rejoining | A forked journal is caught by the chain check and resynced; otherwise a replica like any other |
| A stopped node | Exits, refusing to start (decisions 10, 11) |
| Shutdown drain | Windows still open are dropped |

### 13. Hot-path cost

| Where | Per report | Per batch | Per window |
| --- | --- | --- | --- |
| Matching stage | nothing | one `Release` store | nothing |
| Primary response stage | encode (new only for node events' and gone clients' reports), one XXH3 update after the send | one `Relaxed` latch load per slot; one `Acquire` load per iteration, before the ring read | finalize, chain hash, seqlock store |
| Replica drain | encode and XXH3 update (both new) | one `Acquire` load per iteration, before the ring read | finalize, chain hash, seqlock store |
| Journal stage (primary with replicas) | nothing | one latest-window load, at most one board read | at most one frame per batch, appended: 30 bytes copied, no extra publish |
| Replica receiver | nothing | one `Acquire` load per iteration | board read, chain hash, comparator update |
| Primary senders | nothing | one more byte per ack and one flag test | nothing |

Nothing allocates after construction.

## Steps

Each step builds, passes the suite (kernel TCP, and the DPDK suite under the
netns runner where it touches the transport), and is committable on its
own; docs and `CHANGELOG.md` move with the behaviour. Step 1 changes
nothing a client sees, and step 2 has no detector, so each can ship alone. Step
3's detector needs step 2's notice: a replica that stopped alone would
leave its primary serving.

### Step 1. Every copy folds

- `transport-core::output_digest`: `DigestKind` (exhaustive, `Output = 1`),
  `WindowDigest`, `ChainValue`, `OUTPUT_DIGEST_WINDOW`, `CHAIN_SEED`,
  `WindowFolder` (decisions 1 to 3), the digest board with `BoardSize`
  (decision 5), and `encode_app_body` (decision 4), moved with its
  comments and unit tests from `response_frame.rs`; `frame_app_response`
  and `encode_early_reply` call it. Each type and constant carries the
  comment justifying its choice. `twox-hash` becomes a workspace
  dependency (`Cargo.lock` staged with it).
- A micro-benchmark, an example in `transport-core/examples/` (as
  `melin-journal`'s `scan_bench`), folding windows of `K` representative
  bodies (the examples' and a few hundred bytes) with the chain step,
  through XXH3-128 and through BLAKE3, for `K` of 16, 64 and 256. It
  fixes the hash and `K` (open question 1); its results go in a dated
  document naming its hardware.
- The matching stage's applied cursor; `response_loop::run` encoding,
  sending and folding (decision 4); `Egress::send_slot` taking
  `SlotFrames`, `SlotReply` gone, both egresses adapted. The replica's drain
  hoisted into `replication/drain.rs`, which folds; the replica builder
  takes the encoder (`None` with digests off). The slot rules both
  consumers follow (load the applied cursor before the read, what each
  payload closes, a report's fold) live once, in `OutputDigester`, whose
  `AppliedBeforeRead` and `ReportAt` tokens keep the order and the pairing
  in the types; `server::output_digester` builds one for every pipeline,
  and the pipeline builders expose `first_live`.
- `ServerConfig::no_output_digests` and `--no-output-digests`, with its
  startup `warn!`.
- `ResponseEncoder`'s "Purity" rustdoc and "The determinism rules" in
  `docs/building-an-application.md` gain the consequence and "no node
  identity"; "every response a node encodes" becomes "every report a node
  encodes, sent or folded". `encode_report`'s rustdoc says an error is
  logged only for a reply due to a held connection.
- Tests, ungated:
  - folder: `twox-hash` against xxHash's published XXH3-128 test
    vectors either side of each internal cutoff; a known window's digest
    and the chain value after two known windows equal to one-shot hashes
    of bytes the test builds by hand from the definition, not from the
    code under test; the streaming hasher, fed report by report, equal to the
    one-shot hash of the same bytes on both sides of XXH3's 240-byte
    cutoff and past its internal buffer; a report moved, a byte changed,
    two reports swapped or an encode error each change `d` and every later
    `C`; two folders started at different positions agree on `d` for every
    window both saw whole and digest no partial window; the first-window
    rule at and just past a boundary; empty windows digest their header and
    extend the chain;
  - board: exact lookup, overwrite detection, `latest`, the payload read
    back whole; a two-thread stress test with no torn read, run under Miri
    by the existing Miri job; `for_pipeline` at `RingCapacity::MIN`, the
    default and above it;
  - `encode_app_body` called directly zeroes and, in debug builds,
    checks purity; `response_frame.rs`'s framing tests and
    `server-runtime/tests/encoder_purity.rs` pass unchanged;
  - the agreement test (`response_loop.rs`,
    `a_primary_and_a_replica_publish_the_same_window_digests`): one input
    sequence through two real matching stages, a primary's and a
    replica's, whose output goes through the response stage over a
    recording egress and through the drain; both publish identical window
    digests and chains. The primary's stream carries what differs between
    copies on purpose: queries, ticks that publish nothing, reports to a
    connection the egress does not hold, an ack-policy change, and reports
    the encoder refuses. (A degraded reply changes only its trailer, which
    is not digested; the counter publishes no `BatchEnd`, which the
    digester's own tests cover.)
  - `response_loop.rs`: a reply to an unknown connection is digested; a
    window of report-less entries closes on the idle path once the applied
    cursor passes it, and not before; a window closes on a query slot at
    its end with the ring never empty. `output_digest.rs`: what each slot
    kind closes, and an empty ring closing through what was loaded before
    the read (the load-before-read order itself is held by the
    `AppliedBeforeRead` token); `pipeline_tests.rs`: the applied cursor
    reaches the last allocated position; `drain.rs`: the drain's closing
    rules;
  - the chain-less build: the agreement test with `hash-chain` off,
    added to the pre-merge job's chain-off loop (`cargo nextest run -p
    melin-server-runtime --no-default-features`, filtered to it), since
    CI only `cargo check`s that build otherwise;
  - the DPDK egress's adaptation, under the netns runner, through the
    existing tests in `dpdk_response.rs`.
- Docs: `docs/pipeline-architecture.md` (the response stage folds every
  report; the replica's drain encodes), the flag in `docs/replication.md`'s
  CLI table. `CHANGELOG.md` **Changed**: `encode_report` must be
  deterministic, and is now called for every report, those no client
  receives included; `ServerConfig` gains `no_output_digests` (breaking for
  a struct literal). **Added**: `--no-output-digests`.
- **Leaves untested:** any comparison (step 3); the fold's cost end to end,
  which the micro-benchmark does not measure (see "Deferred"); digests in
  a running node, end to end: nothing reads a node's board until step 3
  (the builder drops its reader), so a node-level test would observe
  nothing. The agreement test runs both consumers over real matching
  stages, and every node test in the suite now runs with digests on.

### Step 2. The stop

Built in parts, each reviewed and committed on its own. The first, the
stop on one node (the latch, the marker and the refusal, the exit, the
response loop's check), departs from the text below in three places:

- The chain values are logged and recorded in full hex (`ChainValue`'s
  `Debug`), so `hex_prefix` is not widened.
- The node's fence is made once, with the latch, as soon as the store is
  resolved (`server::NodeStop`): a primary raises it to its recovered
  epoch rather than creating its own, so the latch and every reader share
  one fence.
- The seam (`divergence::test_seam`) is a registry of running nodes'
  latches by marker path, reached from the runtime's integration tests
  directly; `melin-test-node` gains a wrapper when the replica's half
  needs it.

The second part, the wire and the receiving end of each notice (a
primary told by a stopped ack or handshake, a replica told by `Halt`),
departs from the text below in these places:

- `ReplicaCursors` is unchanged: the senders hold the latch, and the
  decoded `ReplicaMessage::StoppedAck` (a variant of its own, as are
  `StoppedHandshake` and `UnknownFlags`) forces each read site to handle
  a stop explicitly, so `record_ack` keeps its signature and only ever
  sees ordinary acks.
- `HaltedBy::Replica` carries the slot only: naming the replica's key is
  diagnostics, deferred with the build ids.
- A primary that has sent its `Halt` keeps the connection open, reading
  and dropping what arrives, until the replica closes it (bounded by
  `HALT_NOTICE_BOUND`), on both transports: closing first had the kernel
  answer the replica's unread acks with a reset that discarded the `Halt`
  every time. So the limit "a `Halt` lost to the close" holds only for a
  replica that does not close within the bound.
- On DPDK every live session's `Halt` is sent by the poll loop's close
  (`close_all_links`); a slot answered for a stopped handshake waits there
  in a `Halting` state.
- The replica's latch reaches its receivers through `ReplicaControlPlane`,
  whose constructor now requires it.

The third part, the replica's own notice, departs from the text below in
these places:

- The streaming loop owes the notice by the latch's cause, not by holding
  the `Halted` token: a `Detected` cause can only be the replica's own
  (its comparator in step 3, the seam until then), so the receiver that
  sees it at its loop top sends the stopped ack. The seam trips the latch
  directly, from the test's thread.
- The stopped ack carries the last positions the session sent, waits for
  the marker as every notice does (`HaltNotice`), and is never coalesced
  with an ordinary ack (`ReceiverTransport::send_stopped_ack` is taken
  only with nothing in flight).
- `SessionExit::Stopped` says whether a notice was owed and answered; an
  unanswered one makes `handle_session_exit` return
  `AfterSession::RedialStopped`, and `AfterSession::adopt` takes each
  receiver's redial as a parameter, so neither can fold it without one.
- Each receiver's dial is factored for the redial: kernel TCP's
  `dial_until` (`dial_checking_flags` over the node's flags), DPDK's
  `dial_primary_dpdk`. The redial runs with the node's shutdown flag
  already set, so its dial is never interrupted, and every wait is bounded
  by the time left of `HALT_REDIAL`.
- The test proxy now holds a chunk read across a freeze, as it holds one
  read while frozen: a read already waiting when the link froze forwarded
  its bytes, which let the lost-ack test's stopped ack through.

The last part is the operator section, "Output verification" in
`docs/replication.md`: the stop, the marker and the refusal, the
new-cluster procedure and the rollback warning (written here rather than
with step 3, since they concern the stop), and the limits; and the frame
tables. As built, step 2's integration cases are those of
`tests/output_divergence.rs` (a tripped node; a primary halting both
replicas; a stopped handshake and a stopped ack each stopping the primary;
a detecting replica stopping the whole cluster; a lost stopped ack made
good by the redial), on both transports. It leaves untested at the node
level, each covered as noted:

- **The primary's join-time reads** (a stopped ack read in the DPDK
  `Joining` arm; kernel TCP reads nothing from a replica between its
  handshake and the live loop, so a stopped ack sent mid-catch-up is
  first read by the live loop's drain, the path the stopped-ack case
  covers): `keep_last_ack`'s unit tests, and the handshake read site
  through the stopped-handshake case. A replica trips only on streaming
  frames, never mid-join, so no production path reaches them before
  step 3.
- **The `Halt` owed to a joining replica**: on DPDK `close_all_links`
  halts `Joining` slots with `Streaming` ones; on kernel TCP the journal
  catch-up returns at the stop and the live loop's first iteration sends
  the `Halt`, which the replica's stream loop reads mid-catch-up. One
  window misses it on kernel TCP: `bridge_catchup_to_live` sits between
  the catch-up and the live loop, and a batch the journal stage was
  already publishing when the latch tripped can land in the just-activated
  ring ahead of the stalled catch-up position; the refill returns at the
  shutdown flag without advancing, so `drain_into_contiguity` spins to
  `HANDOFF_BRIDGE_TIMEOUT` and returns its stall error, and the handler
  exits without the `Halt`. Step 3 makes the bridge go straight to the
  live loop once shutdown is set (with a unit test of the drain under a
  set flag); until then the operator limits hedge it. No case
  stops a primary while a replica is joining; one needs a catch-up held
  open across the stop (a large journal or a frozen link), left for
  step 3's integration suite. A replica receiving a snapshot is not told
  (the `Halt` arrives where a `SnapshotChunk` is expected, so the
  transfer fails and it retries into a stopped primary), as the operator
  limits say.
- **The unaware raft leader** (a replica made raft leader, its
  replication link cut before the stop): the promotion's leadership
  confirmation is tested on `main`, and two stopped voters of three cannot
  confirm it. A node-level case needs a raft cluster with a cut link,
  left for step 3's integration suite.
- **A failed or hung marker write in a running node**: the latch's and the
  exit's unit tests (a failing and a blocked `MarkerSync`); a node is
  built with the real disk, and no seam reaches its `MarkerSync`.
- **A detector cut off past `HALT_REDIAL`**: the kernel redial's unit test
  pins that an attempt ends at the time left; a node-level case would run
  past a minute.
- **A crash between the trip and the marker**: the exit's bound is
  unit-tested, not a killed process.
- **The new-cluster procedure**: step 3's integration suite runs it.

- `output_digest::Trip` and `Divergence` (decision 6); `DivergenceLatch`,
  `HaltCause`, `Halted`, the marker writer and its state, the marker
  (`NodeStore::divergence_marker`), the exit helper and `NodeDiverged`
  (decision 10); `NodeStore::refuse_if_diverged`,
  `NodeStoreError::Diverged` and `ServerConfig::startup_store()` (decision
  11); `hex_prefix` widened.
- The response loop's per-slot latch check, before a send and before an
  early reply's release.
- Wire (decision 8): `AckFrame` and `HandshakeFrame` gain `flags`,
  `MSG_HALT` and `encode_halt`, unknown bits refused, size assertions, the
  version's history line.
- The primary told: `record_ack` with `AckRecorded` (`ReplicaCursors::new`
  takes the latch), `keep_last_ack` with `JoinFrames::Stopped` and its
  violation arm, moved ungated, and both handshake read sites; the `Halt`
  on the tripping session, on every live session leaving with the latch
  set, and as the answer to a flagged handshake.
- The replica: `streaming_loop`'s latch read before its shutdown arm, the
  flagged ack and the wait for `Halt`, `Halt` decoded in
  `process_streaming_frames`, `SessionExit::Stopped` and its arm, the
  stopped redial over each receiver's factored dial, `HALT_NOTICE_BOUND`,
  `HALT_REDIAL` and their assertion.
- The marker writer takes its fsync through a small trait the node is
  built with, so a test can fail it or block it.
- A test seam, behind `test-utils`, through which `melin-test-node` trips a
  running node's latch with a synthetic `Detected` cause: until step 3
  nothing in production detects. On a replica the seam hands the trip to
  the receiver, which takes it on its own thread at its next frame or
  poll, as it would a mismatch, so the trip and promotion ordering stays
  single-threaded.
- Tests, unit and ungated:
  - `protocol.rs`: the ack, the handshake and `Halt` round-trip with each
    flag; wire byte patterns beside `ack_wire_byte_pattern` and
    `chain_check_wire_byte_pattern` (`server-runtime/src/replication/mod.rs`);
    unknown bits refused with their own error; a v8 handshake is too short
    and a v9 one carrying version 8 gets the version error
    (`a_handshake_from_another_protocol_version_is_refused`, with the flag
    after the version);
  - `cursors.rs`: a flagged ack trips, returns `Stopped` with the token and
    moves no cursor, even with positions that would fail the high-water
    check; a second returns `Stopped(None)`; an ack refused for its flags
    evicts as a violation in `drain_ack_frames` and in `keep_last_ack`,
    never skipped; `keep_last_ack` returns `Stopped` when any ack it reads
    is flagged;
  - `DivergenceLatch`: the first trip wins; it fences and sets the node's
    shutdown flag before returning the token; the marker state goes
    `Pending` to `Written` or `Failed`; a waiter on a writer blocked through
    an fsync seam returns at its bound with the state unsettled, and the
    tripping thread never runs the fsync itself; the exit helper returns
    `Unconfirmed` without blocking;
  - marker: written durably, parsed back with every field;
    `refuse_if_diverged` refuses with the marker present, its message
    quoting the marker and naming a new cluster as the way back; an
    unparseable marker still refuses; `resolve` on the same store succeeds
    and writes nothing; `startup_store()` refuses where `store()` does not,
    and a refused node never spawns its raft driver;
  - `hex_prefix`: a 16-byte value prints as a 32-byte one sharing its
    first bytes, so existing log lines are unchanged;
  - `response_loop.rs`: a latch set while a slot waits at the gate: that
    slot, every later slot and every early reply are never offered to the
    egress; a latch set between two slots of one batch: the first is sent,
    the second is not;
  - `receiver_transport.rs`, with the scripted transport: after a trip, one
    flagged ack carrying the last positions sent and no ordinary ack before
    or after it; frames read and discarded and nothing published while
    waiting; a `Halt` ends the wait; a `Halt` in the stream of an
    unstopped replica trips `ClusterHalted` and ends in `Stopped`; the
    `Stopped` exit returns without reaching a reconnect-loop top, a
    promotion request pending meanwhile not honoured;
  - the kernel redial under a deadline, over loopback: a listener whose
    accept queue is full (backlog shrunk with `libc::listen(fd, 0)` and
    filled) makes the dial time out at the deadline, not after the SYN
    retries; a peer that accepts and never answers returns at the deadline;
    a deadline already passed returns without a call.
- Tests, integration (`server-runtime/tests/output_divergence.rs`, through
  `melin-test-node`, both transports, the DPDK cases under the netns
  runner; raft on kernel TCP), three nodes under load:
  - a replica tripped through the seam: its flagged ack stops the primary,
    whose `Halt` stops the other replica; every node's run returns
    `NodeDiverged`, every marker names its cause, every restart is refused,
    nothing promotes, the client connections are closed, and a request
    sent after the stop gets no reply;
  - the flagged ack lost: the test proxy (`tests/common/proxy.rs`) cuts the
    detector's link at the trip and restores it inside `HALT_REDIAL`; the
    redial's flagged handshake stops the primary;
  - the primary's join-time paths: a replica tripped while the primary is
    still in its kernel-TCP catch-up or its DPDK `Joining` arm; the
    primary stops at the flagged handshake of the replica's redial or
    through `keep_last_ack`, and sends no snapshot to the stopped replica.
    This pins the primary's handling, not a production trip site: a
    replica trips only on streaming frames, never during a snapshot
    transfer;
  - the unaware raft leader: the other replica is made raft leader, and its
    replication link is cut before the trip and kept cut, so it never hears
    the `Halt`; once its grace runs out, no `EpochBump` is journaled and no
    node serves, refused by the lease filter or the leader confirmation;
  - a failed marker write (a seam): the node still tells the others, and
    its run returns the I/O error; a hung one (the fsync seam blocked on a
    barrier): the others are told within `HALT_NOTICE_BOUND`, and the run
    returns `Unconfirmed`;
  - the documented limit: the detector cut off past `HALT_REDIAL` stops
    alone and logs the unanswered redial; nothing is asserted of the
    others.
- Docs: `docs/replication.md`, a first "Output verification" section (the
  stop, the refusal, the limits), the frame tables and the `Ack` and
  `Handshake` rows. `CHANGELOG.md` **Changed**: the protocol 9 entry gains
  the ack and handshake flag and the halt frame; `record_ack` and
  `ReplicaCursors::new` change signature; `NodeStoreError::Diverged` is a
  new variant of a public exhaustive enum; the node entries resolve
  through `startup_store()`. **Added**: `NodeDiverged`,
  `NodeStore::refuse_if_diverged`.
- **Leaves untested:** a crash between the trip and the marker (the exit
  helper's bound is unit-tested, not a killed process); a routing split
  with asymmetric reachability (the proxy cuts whole links); a `Halt`
  destroyed by the close's reset on a real network, which costs only the
  redial.

### Step 3. The primary streams digests; replicas compare and stop

- `DigestComparator` (decision 6), built through its `for_pipeline`
  counterpart.
- Journal stage (decision 7): `output_digest_trailer`, the typed trailer on
  `publish_input_batch_to_rings`, the board handle and last-forwarded end
  in `ReplicationState`, `max_journal_batch`'s reservation and its
  assertion, the ring's module doc. `MSG_DIGEST` and `encode_digest`.
- `replication_wire.rs`: `chunk_frames(&[u8])`, an iterator over one ring
  slot's frames. The test helpers that decode a slot as a single
  `InputBatch` (`handoff.rs`'s `decode`, `catchup.rs`'s and
  `join_worker.rs`'s slot decoders, `pipeline_tests.rs`'s ring readers)
  move onto it: `try_decode_input_batch_into` ignores trailing bytes, so
  they would otherwise pass while silently dropping the digest.
- Receiver (decision 9): decode, contiguity, the comparator in
  `ReplicaPipelineHandles`, `begin_session`, the board read, a `Mismatch`
  handing over to step 2's notice. `melin_output_checks_total` in one
  node-lifetime handle the entry creates and passes to
  `HealthState::for_replica` (via `spawn_replica_health`) and to each
  replica pipeline it builds, since that endpoint outlives every pipeline a
  resync rebuilds.
- Kernel TCP's join bridge at the stop (step 2's untested list): once
  shutdown is set, `bridge_catchup_to_live` goes straight to the live loop
  rather than waiting out `HANDOFF_BRIDGE_TIMEOUT` on a ring chunk the
  stopped catch-up can no longer reach, so a joining replica still gets
  its `Halt`; a unit test drives `drain_into_contiguity` with an ahead
  chunk and the flag set.
- Tests, ungated:
  - comparator, property tests over random interleavings of local windows
    and expected entries from two folders fed the same reports, the
    reference started earlier, sampled at random (most windows never
    sent), with random session starts and overwrites forced by a small
    capacity, and a recording `Trip`:
    - every `Mismatch` follows exactly one `trip` with the same
      `Divergence`, made before the verdict returns; no trip without one;
      after one, nothing more; a `Trip` returning `None` yields
      `halted: None`;
    - never a `Mismatch` when the reports agree, across any session
      starts, the reference restarted from the seed at any of them;
    - a divergence in one window inside a run, sent or not, is a
      `Mismatch` at the first expected entry at or after it, never later,
      and its range contains that window;
    - no run spans a local gap: after a `Break`, no `Match` until a new
      `Anchored`;
    - an expected entry before `F - K`, below the last gap, or whose next
      window has left the local array anchors nothing, and a later usable
      one does; an entry arriving before the first local window anchors no
      run below `F - K` and causes no `Break`;
    - `begin_session` ends the run and clears the expected side, and a
      local window closed before it extends the run anchored after it;
    - built through `for_pipeline` at `RingCapacity::MIN`, the default and
      above: closing up to both rings' worth of entries between two reads
      breaks nothing; closing more than `R` windows is a `Break`;
  - `pipeline_tests.rs`, `primary_emits_output_digest_after_its_position`
    (beside `primary_emits_chain_check_every_interval`, reading slots
    through `chunk_frames`): a digest only follows the `InputBatch` in its
    batch's slot; it names a window at or below the slot's `end_seq`;
    frames rise in window order and carry the board's `C`; the ring holds
    one slot per batch plus the chain checks; the same through the
    rotation flush; a window closing during a query-only batch goes out in
    the next batch's slot; with the response stage lagging, and with a
    window closed by the applied cursor ahead of it (the `get` arm, and an
    overwritten `get` sending nothing). Again with `hash-chain` off (`-p
    melin-transport-core --no-default-features`, added to the chain-off
    loop);
  - `max_journal_batch`: a full-width batch plus its trailer fits
    `CHUNK_SIZE` at `MAX_ENTRY_SIZE` and at the smallest entry;
  - `catchup.rs` and `handoff.rs`: a slot with a trailer is classified by
    its batch, `Covered` dropping it whole, `Next` (with an overlap the
    receiver trims) and `Ahead` forwarding the trailer; the receiver takes
    the digest after a `Next` overlap with no `SequenceGap`;
  - `receiver_transport.rs`, scripted transport and board: a digest ahead
    of the stream is a `SequenceGap`; the first usable frame anchors and
    later ones are compared; a fault in a window the primary never sends is
    a `Mismatch` at the next frame, which trips, sends the flagged ack and
    ends in `Stopped`; an overwritten local window breaks the run and the
    next frame anchors anew; a second session whose primary restarted from
    the seed anchors and never mismatches; a replica with digests off
    drops every frame, never trips and acks as without them; each `Match`
    counts once in `melin_output_checks_total`;
  - `tcp_receiver.rs` and `dpdk.rs` (the latter under the runner): a
    reconnect keeps the pipeline's comparator and its local windows; a
    resync builds a new one.
- Tests, integration (`output_divergence.rs`, both transports, the DPDK
  cases under the netns runner):
  - three nodes healthy under load: the replicas' checks grow, nothing
    diverges;
  - a replica faulted through its `Sizing` (its test app's `prefault`
    stores "change the report of the event that brings my count to X",
    breaking `prefault`'s contract on purpose): it stops with a marker
    whose range holds X's position, its flagged ack stops the primary,
    whose `Halt` stops the other replica; nothing promotes; every restart
    is refused. The same with both replicas faulted. The same with an
    application whose report order follows a hash map's capacity and a
    replica sized differently from its primary, which breaks the
    determinism rule without breaking `prefault`'s contract (assessment
    1.3);
  - no false positives, driven by `TickCountingApp` with ticks at a short
    interval, across a replica joining mid-run, a replica restarted from
    its own snapshot, a reconnect behind the test proxy with a long
    catch-up, a primary restarted under traffic through `melin-test-node`
    (same address, identity, journal and epoch), and a promotion: each
    followed by a run of checks, nothing diverging;
  - idle: with ticks on and no traffic, checks keep growing; with ticks
    off, none grows until traffic resumes;
  - the new-cluster procedure (below): a cluster stopped by a `Sizing`
    fault is rebuilt by its steps with the fault off; the new primary's
    state equals a fresh replay of the same journal, the replicas reseed,
    and the cluster serves; a node whose journal does not reach sequence 1
    is refused with `MissingHistoryPrefix` until the early segments are
    copied in.
- Docs: `docs/replication.md`, "Output verification" completed for
  operators, with no implementation detail: the guarantee and its limits
  as stated above; what an operator sees (the `error!` lines, the markers,
  the refused restarts, `melin_output_checks_total`, a flat count under
  traffic being worth a look); the consequences (any divergence stops the
  whole cluster; the likely causes: the application's determinism, builds
  that differ, a pre-format-5 snapshot's watermark warning (finding 7),
  report order following capacity, a hardware fault); the upgrade rule;
  rolling back past this release (an older release ignores the marker:
  check for `<journal>.diverged` first, a node with one is never started);
  what overrides the stop (a manual `PROMOTE`, observational raft steering
  an operator to promote, voter replacement, a forced `ACK-POLICY disk` on
  a node whose marker never landed); and **starting a new cluster**:
  1. Stop every node.
  2. Move each node's marker aside, kept as the record, and set every
     node's snapshots aside: the broken build wrote them.
  3. Pick the node whose journal reaches the highest position and goes
     back to sequence 1 (recovery without snapshots needs the whole
     history); if none does, copy the missing early segments from any node
     or archive (the segment chain check verifies the splice). Only if no
     copy survives is a suspect snapshot the starting point, knowingly.
  4. Replay it once, standalone, on the one fixed build (or with
     `--no-output-digests`, which is not a fix: the replay is another draw
     of the nondeterminism).
  5. Give every node a fresh `--raft-dir` and the others clean data
     directories, and start as on first deployment; the replicas reseed.
     The fresh raft directories start at the journal's epoch, so the new
     cluster can fail over at once.

  `docs/building-an-application.md`: an upgrade that changes output for the
  same input means stopping every node. The API review's S8 points here.
  `CHANGELOG.md` **Changed**: the protocol 9 entry gains the digest frame; a
  replica whose output disagrees with its primary's now stops the whole
  cluster, whatever build each runs, so an output-changing application
  upgrade can no longer roll node by node with digests on.
- **Leaves untested:** a run broken by an overwritten board end to end (it
  needs a drain stalled under report-less traffic, which no test can
  arrange without a seam in the drain; the receiver and property tests
  cover the verdict); a primary restarted across a clock step back (the
  tick generator has no clock seam; finding 7's argument and
  `tick_watermark.rs` stand for it); a genuine output difference between
  two binaries or architectures (the `Sizing` fault stands in, and nothing
  after the mismatch depends on its cause).

## What this leaves untested, and why

Beyond each step's list:

- **Hardware faults** are modelled only by the fault-injecting apps; the
  detection path is the same.
- **The partition limit** is tested only as its documented outcome (step
  2).
- **The fold's cost on the reply path** is not measured end to end, and
  no test asserts a cost: a wall-clock threshold flakes.
- **A non-default `--ring-capacity` end to end**: the sizing is one
  function, unit-tested at the minimum, the default and above.
- **The idle delay in seconds**: the idle test counts checks, not time.

## Open questions

1. **The hash and `K`. Decided by step 1's micro-benchmark
   ([results](output-digest-fold-bench-2026-11.md)): XXH3-128 at
   `K = 64`.** BLAKE3 costs several times more per report; `K` of 16 costs
   more per report and grows the board, and 256 saves under half a
   nanosecond while the idle delay grows fourfold (about a minute at the
   default tick interval), and idle is when an operator restarts or fails
   over. `K` is part of kind 1: changing it after release is a new kind.
2. **Where the primary folds.** In the response stage after the send
   (recommended: no new thread, the body is already encoded there), or in a
   dedicated output-ring consumer like the replica's drain, which takes the
   fold off the response thread at the cost of a core and a second encode.
   Only an end-to-end measurement (deferred) can show the response
   thread's tail moving.
3. **A stop that does not finish.** The stop exits through the ordinary
   fenced shutdown; if a thread never joins, the exit helper never runs.
   Recommendation: no watchdog; a hang there is a bug for superseded
   primaries too. The stop's own waits are bounded. The residue of a hung
   marker disk: such a node can restart once its disk recovers, without
   quorum to rejoin or be promoted; a primary restarted that way halts
   client writes for want of a replica but answers queries, and a forced
   `ACK-POLICY disk` would let it take writes. Its peers' refusals and
   `error!` lines are the operator's signal.

## Deferred

Each item below was weighed against one test: would leaving it out make
detection wrong, or let the cluster serve, or fail over onto, a copy
already seen to diverge? None does, so each waits for a follow-up plan.
"Former" numbers are the full plan's (this file at b403d38b), where each
item is worked out in detail.

**The in-process check, shadow against live** (former decision 10). The
shadow would fold with the same folder and compare its chain with the live
copy's from a shared start, catching a lossy `snapshot`/`restore` of the
state present at the pipeline's start and giving a standalone node a check.
Safe to defer: without it nothing is seen in-process, so nothing seen is
served; the gap is coverage, stated as a limit. It brings what this plan
avoids: a second detector on another thread, which makes the replica's
promotion drain and waiting push race the trip (both must then read the
latch, and promotion must re-read it after the pipeline is joined), a trip
on the primary with no replica told, and the `Halted` token's abandonment
states. Builds on the folder, the board and a `shared_start` mode of the
comparator; also prefaulting the shadow (`shadow_clone`, on `main`), so a
capacity-ordered application does not trip on upgrade day.

**Build ids and mixed-build naming** (former open question 2's
diagnostics). Comparing every copy whatever its build stays. A BLAKE3 of
the running executable, sent in `StreamStart` and `Handshake`, would let the
markers, the `error!` lines and the refusal name both builds, and a session
log a mixed pair at `info!`. Safe to defer: nothing compares or skips on
it. Builds on the stop's marker and log line, and a `ServerConfig::build_id`
seam for tests.

**The stopped primary's linger, and `Halt` everywhere** (former decision
11's endpoint). A stopped primary would keep its replication endpoint up
for `2 × MAX_BACKOFF`, answering every handshake with `Halt`, so a replica
between sessions stops too; with its own health endpoint kept up, a
deadline `Read` adapter against dripping peers, a half-close so a reset
cannot destroy the `Halt`, a DPDK poll loop of its own, and
`PrimaryFrame` making every replica read site handle `Halt`. Safe to defer:
a primary stops only when a replica has already stopped, so two of three
voters are down and nothing can promote the third; it only stays up
unstopped. Also deferred: holding the notice until delivered across
restarts, which closes the partition limit but for a host reboot. Builds on
step 2's frames, the latch and the factored dial.

**Keeping a run across a reconnect, and the pipeline id** (former decision
3). A random 128-bit id per primary pipeline, in `StreamStart`, would tell a
replica whether its primary's chain continued, so a reconnect to the same
pipeline keeps the run and verifies the catch-up's windows. Safe to defer:
re-anchoring every session only leaves windows uncompared, never claims
them. Builds on the comparator (a `begin_session` taking the session's
kind and id) and `StreamStart`.

**`d_e` beside `C_e` in the frame** (former open question 10). Sixteen bytes
more per frame would check the anchor's own window and say whether the
window a mismatch is found at differs itself, narrowing the marker's range.
Safe to defer: the anchor window is then unverified, not misreported.
Builds on the frame (a new layout, so with the next version bump or a new
kind) and a pending-anchor state in the comparator.

**Chain-break accounting and lap hardening** (former open question 11 and
decision 5's catch-up analysis). Counting unverified windows by cause
(`start`, `new_primary`, `gap`, `uncompared`) and chain breaks, reading the
board in a push waiting for ring space, and removing the overwrite class (a
drain gated on the receiver, or the chain extended on the drain). Safe to
defer: a break ends the run and the next frame re-anchors; nothing is
claimed across it. Builds on the comparator's `Break` verdict.

**Observability beyond the minimum.** Kept: the `error!` lines,
`melin_output_checks_total`, the startup `warn!` when digests are off.
Deferred: the stop cause in `FenceState` (raft's lines say "fenced"), the
other gauges and counters (digests enabled, last window, encode errors,
diverged, notice state), `digest_kinds` in `StreamStart` with a "primary
sends no digests" warning, a rate-limited `warn!` for encode failures, and
the notice's state in the marker. Safe to defer: none changes a verdict or
a stop. Builds on the latch and the replica's node-lifetime metrics handle.

**Measurement beyond picking the hash and `K`.** `melin-bench` closed-loop
against the examples with digests on and off, over both transports, under
`disk+ram`, at p50, p99 and p99.9, with the response stage's `latency-trace`
histograms and a check that the replica drain keeps up at full rate. Safe
to defer for correctness, not for release: it decides open question 2 and
whether the fold's tail cost is acceptable. Builds on step 1's
micro-benchmark and the LAN bench suite.

**The verified run in `/healthz` and `/metrics`.** `verified_from` and
`verified_through` per comparison. Safe to defer: reporting no range claims
none. Builds on the comparator's run.

**Upgrade patterns.** A journaled activation point lets an output-changing
upgrade roll: the new build keeps the old behaviour until an applied entry
switches it at one position everywhere. Safe to defer: the stop-everything
upgrade is always available and stated. Builds on
`docs/building-an-application.md`.

**Roadmap rows.** Offline output qualification (replaying a journal on two
copies to report every differing window, and to qualify an upgrade), an
application state digest (latent divergence, as a new kind on the same
frame), and the output chain stored in snapshots (comparing across restarts
and promotions without anchoring). Safe to defer: each widens coverage of a
stated limit. Each builds on the folder, the board and the comparator; the
rows go in `docs/internal/roadmap.md` with the follow-up plan.

**Not planned at all:** automatic repair or resync of a diverged node;
journaling digests or changing the snapshot format; digests on the event
publisher's feed (no application runs there); replica-to-replica comparison
or majority verdicts; an observe mode that detects without stopping; raft
carrying the stop.
