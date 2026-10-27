# Divergence detection (plan)

Status: **proposal**. Implements item S8 of the
[application API review](application-api-review-2026-09.md), "No divergence
detection for a nondeterministic `apply`".

The one-line goal: **every copy of the application digests the reports it
produced, in windows of a fixed number of entries, chains the window digests,
and a node whose chain disagrees with the reference stops serving, for good,
until an operator brings it back.** The hash chain proves that every node
holds the same *input*. Nothing proves that the same input produced the same
*output*. A nondeterministic `apply` (a randomly seeded `HashMap` iterated,
the wall clock read, an allocation address hashed), a `snapshot`/`restore`
pair that drops a field, or a memory error leaves every journal bitwise
identical while the copies answer differently, silently, until a failover
serves the drifted copy.

What is digested is the **output**: the bytes of every report the
application's encoder produces for a journaled event. It is computed once,
in the runtime, for every application. There is no trait to implement and
no opt-in, and it is on unless the operator turns it off. A divergence means
the application broke its determinism contract, so there is no repair: the
node that sees it stops and refuses to start again until an operator has
looked. One clause of that contract is a trap the runtime still sets, the
tick rule, and finding 7 takes a position on it.

The journal stays input-only and the snapshot format is untouched. Nothing
new is journaled and nothing is stored: each copy's chain starts when its
pipeline starts. The digest rides the replication stream as a control frame,
as the chain check does.

## The guarantee, and its limits

Stated here as the operator docs will state it (step 6):

- **A node never serves once its output has been seen to disagree with the
  output clients were served.** A replica whose reports differ from its
  primary's, or a node whose snapshot-making copy (the shadow) answers
  differently from its live copy, stops: it sends no further reply, takes no
  further client, cannot be promoted, and refuses to restart until an
  operator clears it.
- **Every copy still running produced the same client-visible reports, byte
  for byte, as the copy clients were answered from, for every position of
  its verified run** `[from, through]` (`/healthz`, `/metrics`). A run is
  contiguous by construction: no window inside it went uncompared.
- **The limits.**
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
    yet compared when a primary stops) are not verified.
  - **Queries are not covered**: they are not journaled, so no other copy
    answers them.

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
  dropped as re-delivery, one ahead is a `FrameError::SequenceGap`. A push
  that waits for input-ring space runs a callback on every pass
  (`keep_link_serviced`), so the receiver thread keeps working while the
  ring is full.
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

**6. Raft needs nothing new.** A diverged node latches the fence before
anything else (decision 11). From then until it exits, the fenced inputs
already make it refuse auto-promotion, stand down from elections and never
challenge, and the readers close every client. Every place a replica honours
a promotion request (the in-session poll in `streaming_loop`, each
receiver's reconnect-loop top) checks the shutdown flag first, which the
divergence sets with the latch. If a request still slipped through that
check-then-check window, the primary pipeline it builds shares the node's
`FenceState`, so its readers close every client and its stages stop on the
shutdown flag. After the exit, the marker keeps the node from restarting
(decision 12), so it can neither rejoin raft nor be elected. A raft frame
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
    a board slot that is usually not in cache. Estimated on the order of a
    hundred nanoseconds per window, so at `K = 64` under two nanoseconds
    per entry, a small share of the ~100 ns per-event budget, on the
    primary's response thread and the replica's drain (and on the
    replica's receiver, a board read and one chain hash per window,
    decision 9). The per-report encode and update dominate it. Step 6's
    measurement confirms the estimate; if it does not hold, open
    question 9's next value is the fallback.
  - **The board and comparator sizing** `R = ceil((INPUT_RING_CAPACITY +
    OUTPUT_RING_CAPACITY) / K) + 2` (decision 5): with both rings at
    `1 << 20` slots and `K = 64`, 32770 slots, one cache line each, so a
    board of 2 MiB, and comparator arrays of the same count at 40 bytes a
    slot. Small beside the rings themselves, allocated once per pipeline;
    a smaller `K` grows them in proportion. `R` is a headroom in entries
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
    (open question 10): comparing it with the local `d_e` checks the
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
today. Three call sites, each with one folder:

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

**Sizing R.** `R = ceil((INPUT_RING_CAPACITY + OUTPUT_RING_CAPACITY) / K) +
2`, a constant (`DIGEST_BOARD_SLOTS`), allocated once when the pipeline is
built (decision 2 gives its size). It matters for one reader only: the
replica's comparator, which must copy *every* window of its local board
(decision 3). The receiver reads its board at each loop iteration and on
every pass of a push waiting for ring space (beside `keep_link_serviced`),
so a full ring does not stop it reading. The board laps a window when the
drain closes more than `R` windows between two of those reads, and what
the drain can close at once is its own backlog: the entries the receiver
has published that the drain has not yet closed a window over. That
backlog is bounded by the rings only in part:

- entries still in the input ring, at most `INPUT_RING_CAPACITY`;
- applied entries whose reports wait in the output ring, at most
  `OUTPUT_RING_CAPACITY`;
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
  one frame and the input ring's capacity, both within the
  `INPUT_RING_CAPACITY` term of `R`, however long the catch-up runs, and
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
with `DIGEST_BOARD_SLOTS` (`DigestBoard::for_pipeline`), and tests build
small ones to force laps. They share one indexing helper, so the two cannot
index differently.

### 6. The comparator: one pure type for both checks

A `DigestComparator` (`transport-core`, no I/O, no threads) holds two
fixed-capacity arrays of `R` slots, indexed as the board is:

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
  `SessionKind` is `Compared(PrimaryPipelineId)` or `Uncompared` (a
  `StreamStart` with no output bit or another build), an enum so a caller
  cannot forget to say which, and so an uncompared session carries no id
  to compare. The comparator holds the id of the last compared session,
  `Option<PrimaryPipelineId>`, `None` at construction.
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
    stretch, a session after an uncompared one its `build` stretch).
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
  stretch it lies in, under the stretch's cause: `build` if any
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
- `Mismatch { end, run_from, verified_through, expected, actual }`, each
  side a `(WindowDigest, ChainValue)` pair: the divergence verdict. The
  divergence lies in `(verified_through or run_from - 1, end]`, and when
  the two `d_e` differ, window `end` itself is one that differs. When the
  anchor's own `d_e` differs, no run exists yet: `run_from` is the window's
  first position `end - K + 1`, `verified_through` is `None`
  (`Option<WireSeq>`, so no caller can read an absent run as position 0),
  and the range is window `end` alone;
- `Anchored { end, unverified, reason }`: a run starts, and the windows
  between the previous run's end (or `F`) and the new run's start are
  counted unverified under `reason` (`start`, `new_primary`, `gap` or
  `build`, by the rule above);
- `Break { after, lapped }`: the run ended at a local gap of `lapped`
  windows.

`verified_from` and `verified_through` (the current run) are exposed for
`/healthz` and `/metrics`; the newest verified window end of any run is
published beside them, so the marker writer on whichever thread detects
reads every comparison's value for the archive threshold (decision 11).

### 7. The primary emits from the journal stage, beside `ChainCheck`

`maybe_publish_output_digest`, called right after `maybe_publish_chain_check`
at the sync point, does nothing without replication producers or with
digests off. Otherwise, with `end` the last sequence the batch just
published, it picks the newest closed window whose end is at or below
`end`:

- if the board's `latest()` is at or below `end`, that entry (the response
  stage trails the journal stage, which under any ack policy is the usual
  case);
- otherwise (the window closed through the applied cursor while the journal
  stage was behind it), `get((end / K) * K)`, one division per fsync batch,
  cold; lapped means no frame this batch.

If the pick is newer than the last it forwarded, it encodes the frame of
decision 8 and publishes it through `publish_to_replication_rings` with
`end_seq = end`, the `ChainCheck` convention, so `RingChunk::classify`
sorts it like any control frame during catch-up. Windows that closed
between two batches are skipped, which the chain makes safe: the next frame
carries a chain value covering them. An idle primary still journals ticks,
so batches keep coming. A ring too full to take the frame evicts the
replica exactly as for a `ChainCheck`.

**Independent of the journal chain.** A sibling of
`maybe_publish_chain_check`, not code inside it: the chain check returns
early when `encoder.chain_hash()` is `None` (the chain-less build) and runs
on its own batch cadence. Nothing in the digest path is behind
`cfg(feature = "hash-chain")`.

### 8. Wire: replication protocol 9

| Change | Layout (after the length prefix) |
| --- | --- |
| New frame `MSG_DIGEST = 0x19`, primary to replica | `DigestFrame { tag, kind: u8, window_end: U64, window: [u8; 16], chain: [u8; 16] }`, 42 bytes |
| New frame `MSG_DIVERGED = 0x07`, replica to primary | `DivergedFrame { tag, kind: u8, window_end: U64, verified_through: U64, expected_window: [u8; 16], actual_window: [u8; 16], expected_chain: [u8; 16], actual_chain: [u8; 16] }`, 82 bytes: the replica's notice before it stops (decision 11). `verified_through` 0 means no run yet (the anchor's own window differs): positions start at 1, so 0 is never a window end, and the decoder maps it to `None` |
| `StreamStart` gains the digest kinds, the build and the pipeline id | `digest_kinds: u8` (bit `k - 1` set: this primary sends digests of kind `k`; 0: none), then `build_id: [u8; 32]` (open question 2), then `pipeline_id: [u8; 16]` (decision 3), appended after `genesis_entries`: 115 bytes, from 66 |

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
- **A window end that is not a positive multiple of its kind's `K`** is the
  primary's protocol bug and ends the session as a `FrameError`, never as a
  divergence.
- **The handshake does not change**, so the existing version check
  (`HandshakeFrame.protocol_version`, parsed by `decode_replica_message`)
  refuses a mixed pair in both directions with the version error, as at
  every earlier bump. `REPL_PROTOCOL_VERSION` becomes 9, with the history
  comment extended. Size assertions for the two new frames and the grown
  `StreamStart` (`StreamStartFrame`, 66 to 115 bytes); `docs/replication.md`'s
  frame tables gain them, and its `StreamStart` row and the frame list in
  `server-runtime/src/replication/mod.rs`'s module docs gain the three
  fields.

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
  pipeline id (decision 6) and logs the verdict at `debug!`: the run kept
  (same primary pipeline) or re-anchoring (a new primary pipeline, the
  pipeline's first session, the first compared session after an
  uncompared one, an uncompared session).
- A session whose `StreamStart` has no output bit, or names another build
  (open question 2), begins as `SessionKind::Uncompared` and feeds the
  comparator no expected entries; the session logs why once. The receiver
  counts nothing: the comparator attributes the session's windows to
  `build` at the next `Anchored` (decision 6), so they are counted once.
  A replica with digests off decodes and drops frames.
- A `Mismatch` ends the session with a new
  `SessionExit::Diverged(Divergence)`. The match in `handle_session_exit` is
  exhaustive, so every receiver must decide what it means, and its arm runs
  decision 11's stop and returns `Err`: it never reaches a reconnect-loop
  top, where a promotion request could be honoured.

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
node (decision 11), whether it is a primary or a replica.

This is the only check on a node without replicas, and it catches what the
replica check cannot see on a primary: a shadow built by
`clone_via_snapshot` that answers differently, that is, a `snapshot`/`restore`
pair that drops a field, which is exactly the copy a restart or a resync
would come back from. A node without a shadow (`--snapshot-interval-ms 0`)
has no in-process check, and a standalone node without one has no check at
all; both are logged at startup and documented as limits.

### 11. On divergence: stop, in one order

Whichever comparison finds it (a replica's receiver, or any node's shadow),
the node:

1. **Halts replies at once.** `DivergenceLatch::trip(divergence)` latches the
   node's `FenceState` with a new stop cause and co-sets the shutdown flag,
   the way `fence_if_superseded` owns "fenced ⇒ shutting down". The latch
   keeps the first `Divergence` only; a second trip returns `None`. From the
   latch on: the readers close every client (`Verdict::Close`), the
   response loop hands no slot and no early reply to the egress (a per-slot
   `Relaxed` load of the latch, before the send, decision 4), the shadow
   saves no further snapshot (checked before every save), and raft refuses
   and stands down on `fenced` (finding 6).
2. **Logs `error!`** naming the check (`replica-vs-primary` or
   `shadow-vs-live`), the range known to hold the divergence, whether the
   window at its end differs itself, both chain values' and window digests'
   prefixes (`hex_prefix`, the journal crate's formatter, widened from
   `&[u8; 32]` to `&[u8]` so 16-byte values and the 32-byte chain hash
   print alike), and the run. On a replica it also says: "if other replicas
   report the same window, the primary's output is the one at fault".
3. **Writes the divergence marker** beside the journal
   (`NodeStore::divergence_marker()`, `<journal>.diverged`), written to a
   temporary file, fsynced, renamed and the directory fsynced, so it
   survives a crash. Plain text, for an operator: the format version, the
   check, the kind, the window end, the range known to hold the divergence
   and whether that window differs itself, both window digests and both
   chain values in full hex, the run (`verified_from`, `verified_through`),
   the archive threshold (step 5's last verified position, or "none" with
   no verified window), the archive directory (once known), the node's
   build id, and the wall-clock time (for people; nothing reads it). The
   threshold is fixed when the marker is written, from each comparison's
   newest verified window end as it stands then; a comparison that
   advances afterwards is ignored, which can only archive more, never
   less. The exit helper's archival and startup's re-run both read it from
   the marker, so the two cannot disagree.
4. **On a replica, sends the `Diverged` notice** to its primary, best
   effort, before the session ends.
5. **Archives, never deletes, the snapshots above the last verified
   position**, once every pipeline thread has been joined, so no shadow save
   can race it. The last verified position (the marker's archive
   threshold) is the newest window end a comparison has verified, in any
   run, and the least of it across the node's comparisons (on a replica,
   its check against the primary and its shadow's against live): a
   snapshot is only as good as the least-checked copy that led to it. If
   any comparison has verified no window, the threshold is "none" and
   every snapshot is archived. A snapshot at or below it was followed by matching
   output; one above it was not. Each of `<snapshot>` and `<snapshot>.prev`
   whose header's `journal_sequence` is above it moves into
   `<snapshot dir>/<snapshot name>.diverged.<n>/`, through the archive
   module's helpers and a new `ArchiveReason::Diverged`; with no verified
   window, every snapshot moves. The journal is left in place: it is
   input-only and the chain vouches for it. The marker is rewritten with
   the archive's path.
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
outcome.

### 12. Startup refuses while the marker exists

A new `NodeStore::refuse_if_diverged(&self) -> Result<(), NodeStoreError>`,
which every node entry calls right after `resolve` and before it opens its
journal, starts raft or serves anything, returns
`NodeStoreError::Diverged { marker, check, range, threshold, archive, stopped_at }`
if the marker exists, every field parsed from the marker (`range` the
positions known to hold the divergence, `threshold` the archive threshold,
`None` with no verified window, `archive` an `Option`, `None` when no
snapshot lay above the threshold, `stopped_at` its wall-clock time), so the
message below quotes the marker and nothing else. `resolve` stays as it
is, writing nothing, so a test or tool that resolves a diverged node's
paths is neither refused nor moves files.
Both entries resolve through `ServerConfig::store()` today (`run_tcp` and
`run_dpdk`, `server.rs`), which tests also call. They move to a new
`ServerConfig::startup_store()`, resolve then `refuse_if_diverged`, the one
place the refusal is made, so a third entry takes it by calling the same
function; `store()` stays write-free for tests. The error says what
happened and what to do:

> this node stopped on `<time>` because its output diverged from the
> `<primary's | shadow's>` between positions `<a>` and `<b>` (details in
> `<marker>`). Its snapshots after position `<N>` (the archive threshold;
> "all of its snapshots" with none) are in `<archive>`; its
> journal is intact. (With nothing archived, the sentence reads "None of
> its snapshots lies after position `<N>`, so none was moved; its journal
> is intact.") Find which copy is at fault (several replicas
> reporting the same window point at the primary), fix the application,
> rebuild this node's state (reseed it from the current primary, or replay
> its journal), then delete `<marker>`. See docs/replication.md, "Output
> verification".

Before refusing, `refuse_if_diverged` re-runs the archival of decision 11
(idempotent: it moves only snapshots above the marker's archive threshold
that are still in place), so a crash between the marker and the
archive cannot leave a suspect snapshot for an operator to boot from by
mistake. A marker that cannot be parsed still refuses, naming the file.

So a diverged node cannot come back on its own (systemd's `Restart=always`
just logs the refusal at each attempt), and cannot be elected. Normal
failover then promotes a replica whose output matched the primary's up to
its last run's end.

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
| A diverged node | Stopped and refusing to start (decisions 11, 12). |
| Shutdown drain | Windows still open are dropped: nothing would act on them. |

### 14. Raft and failover

Nothing changes in `raft_promotion.rs` or `melin-raft` (finding 6). The step
that adds the stop (step 3) carries the test that pins it: a diverged
primary that is the raft leader exits, a healthy replica is elected and
promotes, and the diverged node's restart is refused. The stop cause added
to `FenceState` lets the stand-down's log line say "diverged" rather than
"fenced" for an operator reading why a node stopped campaigning; that is
the only raft-side line that changes.

### 15. Hot-path cost

| Where | Per report | Per batch / iteration | Per window |
| --- | --- | --- | --- |
| Matching stage, every copy | nothing | one `Release` store (applied position) | nothing |
| Primary response stage | encode (already paid for delivered replies; new for node events' and gone clients'), one XXH3 update, after the send | one `Relaxed` latch load per slot; on the idle path one `Acquire` load | finalize, one chain hash, one seqlock store |
| Replica drain thread | encode, one XXH3 update (both new) | one `Acquire` load when the ring is empty | finalize, one chain hash, one seqlock store |
| Shadow | encode, one XXH3 update (both new) | one board `latest()` read | finalize, one chain hash, comparator update |
| Journal stage (primary with replicas) | nothing | one latest-window load, at most one division and one board read | at most one 42-byte frame encode and ring publish per batch |
| Replica receiver | nothing | one `Acquire` load per iteration (and per pass of a waiting push) | one board read, one chain hash, comparator update; per frame, decode and comparator update |

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
| Output diverged: the node stops (check, range, digests, run) | `error!` | the diverging node |
| Marker written; snapshots archived (with paths) | `error!` (part of the stop) | the diverging node |
| Startup refused: the marker exists | `error!` (the run's returned error) | the diverged node, at each start |
| A replica reported divergence (`Diverged` notice) | `error!` | primary |
| Every replica streaming reported the same window | `error!`, once per window: "every replica disagrees with this primary at the same window: this primary's output is the likely fault" | primary |
| Output digests off on this node (`--no-output-digests`), or no in-process check (no shadow) | `warn!` at startup | every node |
| The primary sends no output digests, or runs another build (open question 2), for this session | `warn!`, once per session | replica |
| A digest kind unknown to this build (a frame or a `digest_kinds` bit) | `warn!`, once per session | replica |
| A chain broke: a local window lapped before the comparator copied it (with the number of windows lapped) | `warn!`, first then at each doubling of the count | the comparing thread |
| A session started: the run kept (same primary pipeline id) or re-anchoring (a new primary pipeline, this replica pipeline's first session, the first compared session after an uncompared one, an uncompared session), with both ids' prefixes | `debug!` | replica receiver |
| A run anchored (with the windows left unverified before it, and their reason) | `debug!` | the comparing thread |
| Output digests on, window `K` entries | `info!` at pipeline start | every node |
| A match | none (metrics only) | |

Metrics (`/metrics`, rendered in `health.rs`, on replicas through the
replica health endpoint; `/healthz` carries whether digests are on, each
comparison's run and whether the node stopped diverged):

| Metric | Type | Meaning |
| --- | --- | --- |
| `melin_output_digests_enabled` | gauge | 1 when this node folds and checks. Alert on 0 where it is not intended |
| `melin_output_digest_window` | gauge | last window end this node's live copy digested |
| `melin_output_verified_from{against="primary"\|"shadow"}` | gauge | first position of the current run |
| `melin_output_verified_through{against}` | gauge | last window end of the current run found equal. Every position from `verified_from` to it was compared. A gauge that stops moving under traffic is worth a look |
| `melin_output_checks_total{against}` | counter | comparisons found equal |
| `melin_output_unverified_windows_total{reason="start"\|"new_primary"\|"gap"\|"build"}` | counter | replica against primary only, so it carries no `against` label: the shadow's run starts at `F` and cannot break, so it never has an unverified window. Windows left out of every run, each counted once, when the next run anchors, under the stretch's cause (`build` over `gap` over `new_primary` over `start`). `start` grows at a replica's start and at a resync of its state, `new_primary` at a primary restart or a promotion; neither is an alert, and a reconnect to the same primary adds to neither. Alert on `gap`. Nothing is counted while a stretch is still open: during a rolling upgrade `build` stays flat while the builds differ and grows only when the pair converges and a run anchors; meanwhile the stall shows as `verified_through` standing still and the once-per-session "another build" `warn!` |
| `melin_output_chain_breaks_total` | counter | replica against primary only, as above. Runs ended by a lapped local window, live or during a catch-up; a session start never counts here, whatever its id. Alert on any growth |
| `melin_output_frames_skipped_total{reason="kind"}` | counter | digest frames of a kind this build does not know |
| `melin_output_diverged` | gauge | 1 once the node has stopped for divergence (seen in the short window before it exits) |
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
  `WindowDigest` and `ChainValue`, `OUTPUT_DIGEST_WINDOW`, `CHAIN_SEED`,
  `WindowFolder` (decision 1's definition, decision 2's first-full-window
  rule, decision 3's chain), `DigestBoard` (decision 5,
  `DIGEST_BOARD_SLOTS` derived from both ring capacities and `K`),
  `DigestComparator` (decision 6, both modes, with `begin_session`,
  `SessionKind` and `SessionStart`), and `PrimaryPipelineId` (decision 8,
  with a crate-private constructor for step 4's decoder and these
  tests). Each type and constant
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
  is sent. The guide also names the routine causes of a repeated or
  backwards tick (the double call, a restart or restore across a clock
  step back) under the existing tick rule (finding 7).
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
    small capacity:
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
      an `Uncompared` session is counted under `build` alone, never also
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
      session keeps, or the one anchored after it;
    - `shared_start` mode: verified from `F` with no anchor, chain values
      compared directly;
    - with capacity `DIGEST_BOARD_SLOTS` and the local side closing up to
      both rings' worth of entries between two reads, no break and no
      evicted sample; closing more than `R` windows between two reads is a
      `Break`, never a `Match` across it; one indexing helper for board and
      comparator.
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
    idle path once the applied cursor passes it, and not before; the
    applied cursor is loaded before the ring is read (a recording seam
    holding the stage between the two); with digests off, nothing is
    folded and the frames sent are the same bytes;
  - the chain-less build: the same agreement test with `hash-chain` off.
    The pre-commit hook and CI only `cargo check` that build, so the step
    adds a test run of these cases (`cargo nextest run -p
    melin-server-runtime --no-default-features`, filtered to them; that
    crate's default `hash-chain` forwards to the journal and transport-core
    features, so the whole pipeline is chain-less) to the pre-merge job's
    chain-off loop;
  - the DPDK egress's adaptation runs under the netns runner through the
    existing response-stage tests in `dpdk_response.rs`.
- Docs: `docs/pipeline-architecture.md` (the response stage folds every
  report; the replica's drain and the shadow encode), the new flag in
  `docs/replication.md`'s CLI table. `CHANGELOG.md` **Changed**:
  `encode_report` must be deterministic; `ServerConfig` gains the public
  field `no_output_digests` (breaking for a struct literal). **Added**: the
  `--no-output-digests` flag.
- **Leaves untested:** the cost, which is step 6's measurement, not a test;
  any comparison, which steps 3 and 5 add.

### Step 3. The stop, and the in-process check

- `FenceState`: a stop cause (`Superseded | Diverged`, exhaustive), first
  cause wins; `is_fenced` unchanged for its readers; the cause in
  `/healthz` and in the raft stand-down's log line.
- `DivergenceLatch`, `trip`, the `Halted` / `MarkerWritten` tokens, the
  marker (`NodeStore::divergence_marker`), `ArchiveReason::Diverged` and the
  snapshot archival, the `NodeDiverged` error through the one exit helper
  (decision 11). `melin_journal::error::hex_prefix` widened to `&[u8]`
  for the divergence log line, rather than a second formatter beside it;
  every existing call passes a reference to a 32-byte array, which
  coerces, so no caller changes.
- The response loop's per-slot latch check, before the send and before an
  early reply's release; the shadow's check before each save.
- The shadow's comparator (`shared_start`) against the live board on every
  node with a shadow (decision 10).
- `NodeStore::refuse_if_diverged`, `NodeStoreError::Diverged` and the
  archival re-run at startup, called from `ServerConfig::startup_store()`
  by both entries; `resolve` and `store()` unchanged (decision 12).
- Metrics `verified_from{against="shadow"}`,
  `verified_through{against="shadow"}`, `checks_total{against="shadow"}`,
  `melin_output_diverged`.
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
    field present, each value in full canonical hex, the range, the
    "window differs itself" flag and the archive threshold (on a replica,
    the lesser of its two comparisons' newest verified ends, from an earlier
    run when the current one has verified nothing; "none" with no verified
    window); an unparseable marker still refuses;
  - `hex_prefix`: the same prefix for a 16-byte value as for a 32-byte
    hash sharing its first bytes, so the chain's log lines are unchanged;
  - archival: snapshots above the marker's archive threshold move, those at
    or below stay, `.prev` handled the same way, a second run moves
    nothing, and the journal is untouched; with no verified window, every
    snapshot moves;
  - `node_store.rs`: `refuse_if_diverged` refuses with the marker present,
    and the message names the marker, the range, the archive threshold, the
    archive and the doc section, each as the marker holds it, and, for a
    marker with no archive (no snapshot above the threshold), says that
    none was moved rather than naming a directory; `resolve`
    on the same store succeeds and moves nothing; `server.rs`: `startup_store()` refuses where `store()` does not;
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
    the archive done (decision 11, "Which thread does what");
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

- `replication/protocol.rs`: `MSG_DIGEST` and `MSG_DIVERGED` with
  encoders, decoders and size assertions; `StreamStart`'s
  `digest_kinds`, build id and pipeline id (`StreamStartFrame` to 115
  bytes); the version bump with its history line.
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
  build id per open question 2, held in a `ServerConfig` field that
  defaults to the executable's hash (so step 5's mixed-build test can set
  it; listed with step 2's field under `CHANGELOG.md` **Changed**); the
  pipeline's id, the same on every session of that pipeline.
- Every receiver records the session's kinds, build and pipeline id from
  `StreamStart` for its comparator, and warns once when the primary sends
  no output digests, names an unknown kind or runs another build. Digest
  frames are decoded and, for now, dropped: no primary sends one until
  step 5. The primary decodes `Diverged` and logs it (decision 16),
  though no replica sends one yet.
- Tests: round trips; wire byte patterns beside `ack_wire_byte_pattern` and
  `rotate_wire_byte_pattern`; a v8 handshake refused with the version error
  in both directions (the existing
  `a_handshake_from_another_protocol_version_is_refused` covers it once the
  constant moves; one case added for a v9 replica at a scripted v8
  primary); kind 0 refused, an unknown kind skipped and counted, not
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
- Docs: `docs/replication.md` frame tables, and the `StreamStart` row's
  description (the id names the running primary, so a replica knows a
  reconnect from a restart). `CHANGELOG.md` **Changed**: replication
  protocol 9, mixed pairs refused; `build_pipeline_with_replication` takes
  a `PipelineId`.
- **Leaves untested:** a primary that sends digests, which is step 5.

### Step 5. The primary streams digests; replicas compare and stop

- Journal stage: `maybe_publish_output_digest` (decision 7).
- Receiver: decode, contiguity, the comparator (`anchored`) against the
  live board read at each iteration and in the waiting push,
  `begin_session` at each session start with its `debug!` line, the
  `Diverged` notice, `SessionExit::Diverged` and its arm
  in `handle_session_exit` running decision 11's stop (decision 9).
- Primary: the "every replica, same window" aggregation of decision 16.
- Metrics `verified_from{against="primary"}`,
  `verified_through{against="primary"}`, `checks_total{against="primary"}`,
  `unverified_windows_total`, `chain_breaks_total`,
  `frames_skipped_total`, `melin_replica_output_divergence_total`.
- Tests:
  - `pipeline_tests.rs`: `primary_emits_output_digest_after_its_position`,
    modelled on `primary_emits_chain_check_every_interval`: every digest
    frame in the ring names a window ending at or below its chunk's
    `end_seq`, none precedes the batch holding its end, frames are in
    increasing window order and carry the live board's `d` and `C` for
    their window, with the response stage lagging the journal stage and
    with a window closed by the applied cursor ahead of it (the `get` arm,
    and a lapped `get` that sends nothing); and the same with `hash-chain`
    off, where digests are emitted and no `ChainCheck` is (a
    `cargo nextest run -p melin-transport-core --no-default-features` run,
    filtered to it, added to the chain-off loop beside step 2's: this test
    needs only the journal stage, which is in that crate);
  - `receiver_transport.rs`, with the scripted transport and a scripted
    local board: a digest ahead of the stream position is a `SequenceGap`;
    the first is taken as the anchor and later ones compared; a fault in a
    window the scripted primary never sends a frame for is a `Mismatch` at
    the next frame, which sends the notice and ends the session with
    `SessionExit::Diverged`; a promotion request pending when the mismatch
    arrives is not honoured (the session ends in the divergence, and the
    receiver returns the error); a local window lapped (a small-capacity
    board) breaks the run, counts the gap and re-anchors on the next
    frame, and `verified_through` never moves past the gap; the board read
    in a push waiting for ring space keeps the run unbroken with the input
    ring full; a session whose `StreamStart` carries another build id
    compares nothing and a mismatching digest in it does not stop the
    replica, and when a same-build session follows and anchors, the
    windows since the last run are counted once, all under
    `unverified{reason="build"}`, none under `new_primary`; an all-zero
    build id compares as any build;
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
      `SessionExit::Diverged`; a local window closing in the first
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
      re-anchors (`build`);
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
    divergence exit tears the pipeline down, archives and returns the error
    without reaching the reconnect-loop top; a `Disconnected`, a
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
    purpose) stops with a marker whose range contains the position of X,
    its archive exists, its restart is refused, the primary logs the
    notice and keeps serving on the other replica; the same fault on both
    replicas makes the primary log the "same window" error, and under
    `disk+ram` the primary then halts writes (its last replica gone), as
    the docs will state; and the mixed-build case: the same fault on a
    replica whose primary is started with another build id (a
    `ServerConfig` field that defaults to the executable's hash and that
    `melin-test-node` can set, the one seam that stands in for a second
    binary) leaves the replica serving, with `verified_through` standing
    still, `unverified{reason="build"}` still at zero (no run has ended
    the stretch) and the "another build" `warn!` logged once for the
    session; the primary is then restarted with the replica's build id,
    the replica anchors on its first frame, and the windows of the
    mixed-build stretch are counted under `unverified{reason="build"}`,
    none under `new_primary`;
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
    checks with nothing diverging, ticks on, driven by finding 7's
    rule-following test app, whose reports carry its tick-driven clock;
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
  - the primary's notice handling over DPDK, under the runner.
- Docs: `docs/replication.md`, "Output verification" extended to the
  replica check and the notices. `CHANGELOG.md` **Changed**: a replica
  whose output disagrees with its primary's now stops, and a primary whose
  output disagrees stops every replica (under a replica-backed ack policy,
  writes then halt); applications whose `tick` is not idempotent against a
  time already passed are named as at risk until monotonic sequencer time
  lands (finding 7).
- **Leaves untested:** a chain break end to end (it needs a drain thread
  stalled under report-less traffic past decision 5's headroom, which a
  test cannot arrange without a seam in the drain; the receiver tests with
  a small board, live and during a catch-up, and the comparator's property
  test cover the verdict and the accounting); two primary pipelines
  drawing the same id, which a test cannot cause without a seam in
  `draw`, and `draw` takes none (its consequence, a kept run compared
  against a restarted chain, is a `Mismatch` on the first frame, the path
  the same-id fault case exercises: a stop of a healthy replica, never a
  missed divergence, and at 128 random bits not a case to plan for); a
  primary restarted across a clock step back with an
  application whose `tick` breaks the rule (finding 7): the tick generator
  reads the wall clock and has no seam a test can step back, and the
  monotonic-time plan's acceptance tests add exactly that case, which with
  this plan in place also shows as a divergence until monotonic time
  lands.

### Step 6. Measurement and the operator runbook

- The measurement of decision 15, the hash micro-benchmark and the
  per-window cost included, in a dated document naming its hardware. If
  the response stage's tail moves, open questions 1 and 5 decide; if the
  per-window cost is above decision 2's estimate, open question 9 does.
- `docs/replication.md`, "Output verification", completed for operators,
  with no implementation detail:
  - **the guarantee** in the words of "The guarantee, and its limits"
    above, and **the limits**: latent divergence; the windows before a
    replica's first comparison with a restarted or newly promoted primary,
    or after its own restart or resync, while a reconnect to a running
    primary keeps coverage, catch-up included, unless a chain break is
    counted; the detection delay bounded in entries and so in ticks when
    idle; the unverified tail at failover; queries; nodes without
    snapshots;
  - **what an operator sees**: the `error!` lines, the gauges and counters
    to alert on (`melin_output_diverged`, `melin_replica_output_divergence_total`,
    chain breaks, digests off), the run gauges, the marker's contents, the
    refused restarts;
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
  - the `--no-output-digests` switch, what turning it off gives up, and
    why the window is not configurable (it is part of the digest's
    definition, the same on every node).
- `docs/internal/application-api-review-2026-09.md` S8 points here; the
  pipeline architecture doc's responsibilities table names the folders.
- **Leaves untested:** the runbook is prose; its procedure is exercised by
  step 3's integration tests (the refusal, then the way back: marker
  deleted, journal replay from a kept snapshot, serving again) and step 5's
  (a stopped replica's refusal). Reseeding a cleared replica from its
  primary is not a new path: it is the existing bootstrap of a replica with
  no local state, which its own tests cover.

### Step 7. Roadmap: two follow-ups

Add two rows to `docs/internal/roadmap.md` ("Next"), roughly:

> **Application state digest** (Medium). Output digests catch a divergence
> only once a report shows it. Two copies whose state differs where no
> report has yet looked (latent divergence) are both verified, and the
> drifted one may be promoted at a failover, after which its reports
> differ from what the old primary's would have been, and its own replicas
> stop on the first one. A digest of application state, chained at window
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

- **Leaves untested:** two roadmap rows.

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
2. **Comparing across builds.** A rolling upgrade that changes a report's
   layout, or the encoder, makes a mixed pair disagree by design, and under
   this plan the replica stops. Options: (a) document that such upgrades run
   with digests off on the primary; (b) `StreamStart` carries a build id (a
   BLAKE3 hash of the running executable, read once at startup, in
   `server-runtime`, which already depends on `blake3` unconditionally; it
   stays BLAKE3 and 32 bytes, since it is computed once, off every hot
   path, so decision 1's cost argument does not reach it), and a replica
   compares only against the same build, and otherwise compares nothing;
   while the builds differ that shows as `verified_through` standing still
   and a once-per-session `warn!`, and the stretch's windows are counted
   `unverified{reason="build"}` only once the pair runs one build again and
   a run anchors; (c) an application-declared
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
   join (decision 11), so even a stop that hangs and is killed leaves a
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
10. **Does the digest frame carry `d_e` beside `C_e`?** (a) Yes, 16 bytes
    more per frame (recommended); (b) the chain value alone. With (a), the
    anchor's own window is checked rather than taken on trust, and a
    mismatch can say whether the window it was found at differs itself,
    which narrows the marker's range for the operator; the cost is 16
    bytes on a frame sent at most once per fsync batch. With (b), a
    mismatch only says the divergence lies somewhere since the last match.
    Recommendation: (a).
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
- Replica-to-replica comparison and majority verdicts.
- Making `apply` deterministic: this plan detects nondeterminism; the
  determinism work removes sources of it. "Monotonic sequencer time" is the
  one this plan depends on for its release (finding 7, open question 8),
  not for its build.
