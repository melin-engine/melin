# State divergence detection (plan)

Status: **proposal**. Implements item S8 of the
[application API review](application-api-review-2026-09.md), "No divergence
detection for a nondeterministic `apply`".

The one-line goal: **every copy of the application digests its state at the
same journal positions, and a copy whose digest disagrees with its
primary's (or, in-process, with its own shadow's) is caught and repaired,
the same way a replica whose journal chain disagrees is today.** The hash
chain proves that every node holds the same *input*; nothing proves that
they hold the same *state*. A nondeterministic `apply` (a randomly seeded
`HashMap` iterated, the wall clock read, an allocation address hashed), a
`snapshot`/`restore` pair that drops a field, or a memory error leaves every
journal bitwise identical while the states drift apart, silently, until a
failover serves the drifted copy.

The journal stays input-only: nothing in this plan is journaled and the
journal format is untouched. The digest rides the replication stream as a
control frame, as the chain does.

## What the code shows

### The chain check this plan mirrors

- **Emission.** The journal stage is the sole producer of the two
  replication rings (`SequencerCore::publish_to_replication_rings`,
  `transport-core/src/pipeline.rs`). Right after a batch is handed to the
  disk thread (`submit_batch`), `maybe_publish_chain_check` counts published
  batches and, every `CHAIN_CHECK_INTERVAL_BATCHES`, encodes a `ChainCheck`
  frame (the chain value at `next_sequence - 1`) and publishes it into the
  rings with that sequence as the chunk's `end_seq`. Rotations are announced
  the same way (`publish_rotate_to_rings`). Because the frame enters the
  ring after the batch that holds its position, it reaches the wire after
  the entries it covers, on kernel TCP and DPDK alike: both senders drain
  the same rings.
- **Wire.** `MSG_CHAIN_CHECK = 0x17` and `MSG_ROTATE = 0x16` share one
  41-byte layout, `SeqHashFrame { tag, sequence: U64, hash: [u8; 32] }`
  (`transport-core/src/replication/protocol.rs`). Any frame-layout change
  bumps `REPL_PROTOCOL_VERSION` (8 today), which the primary checks in the
  replica's handshake so that a mixed pair fails with a diagnosable error.
  An unknown tag mid-stream decodes as an error and ends the session
  (`decode_primary_message`'s `other =>` arm), so a new tag needs the bump.
- **Catch-up.** Control frames in a ring chunk are classified against the
  stream position by `RingChunk::classify` (`replication/catchup.rs`),
  shared by the kernel sender's inline drain and the DPDK sender's
  `LiveHandoff`: behind is dropped, at is forwarded, ahead is forwarded once
  the disk back-fill reaches it.
- **Receipt.** `process_streaming_frames`
  (`server-runtime/src/replication/receiver_transport.rs`, shared by both
  receivers through `streaming_loop`) hands `Rotate` and `ChainCheck` to
  `queue_stream_mark`: a mark exactly at the stream position is pushed onto
  the `StreamMarks` queue before any later slot is published, one behind is
  dropped as re-delivery, one ahead is a `FrameError::SequenceGap`.
- **Comparison.** The replica's journal stage pops marks and resolves them
  at the exact position (`resolve_stream_marks`, `pipeline.rs`). A chain
  that disagrees returns `JournalError::ReplicaChainDivergence`, which kills
  the journal stage.
- **Repair.** The receiver's session ends `Fatal`; `handle_session_exit`
  (`replication/mod.rs`, shared by both receivers) sees
  `TeardownOutcome::JournalFailed(ReplicaChainDivergence)`, spends the
  once-per-process budget (`MAX_INPROCESS_DIVERGENCE_RESYNCS`, a second
  occurrence exits the process), re-derives the local position from disk
  with `recover_replica_state`, and returns `AfterSession::Resync`. The
  replica reconnects; **the primary**, validating the handshake
  (`validate_replica_handshake_settled`, `replication/validate.rs`), finds
  the chain at the replica's position wrong, counts
  `melin_replica_divergence_total`, runs `preflight_snapshot_transfer`
  (refusing, with the replica's journal untouched, when it cannot serve a
  snapshot), and sends `HashMismatch`. The replica's `handle_resync_verdict`
  archives its lineage (`archive_local_lineage`, `ArchiveReason::Divergent`,
  `<journal>.divergent.<n>`), and installs the primary's snapshot and
  segment seed.

### What the application side offers

- **One dispatch path.** The live matching stage, journal replay and the
  shadow stage all hand events to the application through
  `dispatch::dispatch`, so they apply the same sequence.
- **Every copy knows the journal position of each event.** The matching
  stage stamps `wire_seq` per event (`MatchingStage::run`, the allocating
  arm; queries take no sequence). The shadow counts it the same way
  (`shadow::run`, `next_wire_seq`, with a debug check against a replica
  slot's own sequence).
- **No digest exists.** `Application` has `snapshot`/`restore` and
  `clone_via_snapshot`, and its rustdoc says outright that snapshot bytes
  are *not* comparable across nodes: `prefault` may rebuild a collection and
  change its iteration order. A digest therefore has to be defined over the
  logical state, by the application.
- **The output ring already carries a positional, reply-less payload**
  (`OutputPayload::AckPolicyChange`), and the event publisher is documented
  to see it in order, after the ack gate.
- **Raft promotion is decided by pure functions** in
  `server-runtime/src/raft_promotion.rs`: `auto_promotion_decision`,
  `election_should_stand_down`, `challenger_should_campaign`.
- **The shadow is also where resync snapshots come from**: the snapshot a
  primary serves to a divergent replica is the file its shadow wrote.

## Where the rough plan holds and where it needs adjusting

The shape holds: digest at deterministic positions, keep the last few, send
`Digest { at, hash }` down the replication stream as a control frame, never
journal it, compare at the replica, compare the shadow in-process. Four
findings change the details.

**1. The chain repair path is driven by the primary's handshake check, and
that check passes for a state-divergent replica.** A chain divergence
repairs because, on reconnect, the primary recomputes its chain at the
replica's position and finds it wrong. A replica whose *state* diverged has
a journal identical to the primary's, so the same reconnect would pass
validation and simply resume streaming onto the bad state. The repair can
reuse everything from the handshake onward (preflight, verdict, archive,
snapshot, seed, budget), but the replica has to *ask* for it: a flag on its
handshake (decision 6 and step 7).

**2. The replica must buffer early digests whichever ordering the primary
uses, and the primary can order its frame for free.** Two lags exist, not
one. On the primary, the matching stage and the journal stage are parallel
consumers of the input ring, so either may lead, by up to the ring's
capacity. On the replica, the receiver publishes entries into the input
ring and the replica's matching stage applies them later, so a digest for
`N` normally arrives *before* the replica has computed its own digest at
`N`, even when the frame is strictly behind entry `N` on the wire. Holding
the frame on the primary therefore does not spare the replica its buffer.
What holding does buy, and cheaply, is that a digest can never name a
position the replica has not received: the journal stage, which publishes
`ChainCheck` exactly this way, reads the newest digest whose position it
has already published, once per fsync batch, without waiting for the
matching stage or making it wait. No stage blocks on another, so this is
not the coupling the rough plan wanted to avoid, and it buys three
things. A digest ahead of the stream becomes a contiguity violation the
replica can reject, as it does for stream marks. Catch-up stays quiet: the
disk walk carries no digests, and ring chunks holding them are classified
like any control frame. And the wire stays ordered, which the replica's
buffer bound below relies on. **Recommendation: both.** The journal stage
injects digests in stream order, and the replica buffers whatever its own
matching stage has not reached yet.

**3. A digest is not a stream mark.** Marks exist because the replica's
*journal stage* has to act at an exact position (rotate, or check the chain
of the bytes it encoded). A state digest needs nothing from the replica's
journal stage and no exact position: it compares two values keyed by
sequence. It therefore bypasses `StreamMarks` and goes to a comparator
(decision 4). Forcing it through the mark queue would stall the journal
stage's encode loop at every digest position for no reason.

**4. The shadow check matters more than a self-test: it is what makes a
resync snapshot trustworthy.** A diverged replica is repaired with the
primary's snapshot, which the primary's shadow wrote. If the shadow and the
live state disagree on the primary, that snapshot is suspect and must not be
served. So the in-process comparison does two jobs: it detects
nondeterminism on a node with no replica, and it gates snapshot serving
(decision 8).

One consequence of the shape is worth stating: **the primary is
authoritative.** When a replica disagrees with its primary, the plan never
asks which state is "right". The primary's live state is what clients were
answered from, so the replica takes the primary's state. When a primary's
live state disagrees with its own shadow, no copy is known to be right, and
the runtime stops making snapshots and pages the operator rather than guess
(open question 2).

## Design decisions

### 1. The application surface: an opt-in associated const

```rust
/// A digest of the application's logical state. 32 bytes: wide enough for
/// a cryptographic hash (BLAKE3, SHA-256), so accidental collisions are not
/// a practical concern, and the width of the chain hash, so the wire frame
/// reuses `SeqHashFrame`.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct StateDigest(pub [u8; 32]);          // Debug: hex prefix

pub struct StateDigestSpec<A> {
    /// Identifies the digest's definition. Bumped whenever what the digest
    /// covers or how it is computed changes, so that two builds that would
    /// disagree for that reason alone never compare (decision 2).
    pub scheme: NonZeroU16,
    /// Positions between digests when the operator sets none.
    pub default_interval: NonZeroU64,
    pub digest: fn(&A) -> StateDigest,
}

pub trait Application: Sized + Default {
    // ...
    const STATE_DIGEST: Option<StateDigestSpec<Self>> = None;
}
```

Why an associated const holding a function pointer, rather than the review's
`fn state_digest(&self) -> Option<[u8; 32]>`:

- **Opting in cannot be done by halves.** A method returning `Option` allows
  an implementation that returns `Some` at one position and `None` at the
  next, which the runtime would have to treat as a bug at run time. With the
  const, an application either supplies scheme, interval and function
  together, or supplies nothing.
- **Zero cost for applications that do not opt in, by construction.** The
  runtime reads `A::STATE_DIGEST` in generic code, so for `None` the
  digest branch is removed when the code is monomorphized. Echo, the latency
  floor, compiles to exactly what it compiles to today.
- **Additive.** Defaulted, so no existing application changes. (Associated
  *type* defaults, the other way to get a compile-time opt-in, are not
  stable Rust.)

The contract, in the rustdoc and in `docs/building-an-application.md`:

- A pure function of the **logical** state, the same on every node and every
  platform running the same build. It must not depend on capacity,
  iteration order, allocation addresses or anything `prefault` may change
  (the same reasons snapshot bytes are not comparable). A per-entity hash
  folded with an order-independent combine (XOR, or addition modulo 2^256),
  or hashing in sorted order, are the usual shapes. `TestApp`'s `HashMap`
  is a ready example.
- Covers everything `apply` decides from, and so everything `snapshot`
  writes. A field the digest leaves out is a field whose divergence goes
  unseen.
- **Cheap.** It runs on the matching thread at every digest position
  (decision 13). The intended shape is an incrementally maintained hash that
  `apply` updates as it changes state, so that reading it is O(1). An
  application that already keeps such a hash (the Exchange Core's Theia
  state hash is one) exposes it directly. An O(state) digest is allowed but
  puts its whole cost on one event's latency every interval. The docs say so,
  and the runtime reports slow digests (decision 14).

The examples: `counter` and `notary` opt in, and `echo` stays out. Echo has
no state, and as the latency floor it is the example that proves the
opt-out free.

### 2. Positions, the interval K, and keeping K identical

- **Positions are absolute journal sequences**: a copy digests after
  applying entry `N` whenever `N % K == 0`, whatever kind of entry `N` is
  (an application event, a tick, an epoch bump). Absolute positions make the
  schedule independent of where a copy started: after a restart, a snapshot
  restore, a resync or a promotion, the next digest falls at the next
  multiple of `K` on every node. Queries take no sequence and are never
  digest points.
- **The per-event test is not a modulo.** Each copy holds `next_digest_at`
  and compares `wire_seq == next_digest_at`. Only when it matches does it
  compute the next position (`N + K`, or, after a change of `K`, the next
  multiple). That division runs once an interval. On a change of `K` the
  copy publishes the new schedule's `(K, first_at)` to its board before
  its first digest under it (decision 4), so a comparator never has to
  infer where the new schedule begins.
- **Where K comes from.** The primary's interval is `--state-digest-interval`
  (0 disables digests), defaulting to the application's `default_interval`.
  The primary advertises it on `StreamStart` together with its digest scheme
  (decision 6), and **a replica adopts the primary's interval for the
  session**. So within a session the two copies digest at the same
  positions by construction, and an operator who sets different values on
  different nodes has set only what each node uses *when it is primary*.
  This follows the direction of the roadmap's "Replicas pull configuration
  from primary": an operator keeping a flag identical by hand is a known
  source of silent divergence. The replica's matching stage and shadow read
  the interval from an atomic once per batch (one `Relaxed` load beside the
  ones the matching stage already does per batch), and realign when it
  changes.
- **What happens if they differ anyway.** They can differ only when a
  replica's digest *definition* differs from its primary's: different
  `scheme`, or one side without a digest (a rolling upgrade across a change
  to the digest). The replica then compares nothing for that session, logs
  it once at `warn!`, and counts `melin_state_digest_unverified_total
  {reason="scheme"}`. **It is never a divergence.** A rolling upgrade must
  not turn into a resync storm. A mismatched `K` within one scheme cannot
  happen, because the replica adopts. If a future change ever lets it
  happen, the comparator still compares only positions both sides computed
  (`N` multiple of both), and a digest with no local counterpart is
  counted unverified, never divergent.
- **A lower bound on K.** `K` is validated at startup against
  `MIN_STATE_DIGEST_INTERVAL = 4096`, a named constant beside
  `INPUT_RING_CAPACITY` (`1 << 20`), because every buffer below holds
  `R = ceil(INPUT_RING_CAPACITY / K) + 2` digests. At the minimum `R` is
  258, so a board is about 10 KiB and a comparator about twice that,
  allocated once. A value under it (other than 0, which disables) is
  refused with an error that names the minimum. The minimum is a memory
  bound, not a cost bound: the per-digest cost (decision 13) is a few
  stores and one 41-byte frame, so even at the minimum it stays far below
  the per-event budget when amortized over `K` events.
- **The default.** The runtime has none of its own: the application's
  `default_interval` is it. The examples use `65536` (`R` = 18), which at
  the design rate of 10M events/s is a check every few milliseconds, so
  detection latency is far below any operator's reaction time, while the
  app's digest cost, even an O(state) one, is spread over tens of
  thousands of events. The application docs recommend that order of
  magnitude, and a lower value only for an O(1) digest.

### 3. The digest board: one writer, many readers, no backpressure

The matching stage publishes each digest to a **`DigestBoard`**
(`transport-core`): `R` slots of `(at: u64, hash: [u8; 32])`, the slot for
position `N` being `(N / K) % R`. Each slot is a
`melin_pipeline::seqlock` (the payload is a `repr(C)` 40-byte struct with
no padding, so it satisfies `NoPadding`), plus an `AtomicU64` holding the
latest position, for cheap change detection. A seqlock rather than a queue:
the matching stage must never wait on a reader, a reader that falls behind
should lose old digests rather than stall anyone, and there are several
readers (the journal stage's injector, the shadow comparator, the replica
comparator, the health endpoint, the feed). The seqlock module already
enforces single-writer and no-padding in the type system, so the board
inherits both.

**Sizing R.** On the primary the matching stage can lead the journal stage,
and the shadow, by at most the input ring's capacity. On a replica its
matching stage can trail the receiver by at most that too. Every reader
therefore needs at most `ceil(INPUT_RING_CAPACITY / K) + 2` digests of
history. The board is allocated once at that size when the pipeline is
built. A reader that finds its position overwritten (a "lap", impossible
while the bound holds) counts it as unverified, never as divergence.

Lookups are `get(at) -> Option<StateDigest>` (exact position: present, not
yet written, or lapped) and `latest() -> Option<(WireSeq, StateDigest)>`.
Positions are `WireSeq`, the existing cursor-space newtype, so a ring index
cannot be passed where a journal position is meant.

### 4. The comparator: one pure type for both checks

A `DigestComparator` (`transport-core`, no I/O, no threads) holds two
bounded buffers: *expected* (the primary's digests, or the live stage's for
the in-process check) and *local* (this copy's own). Both are
fixed-capacity arrays of `R` slots indexed exactly as the board is, the
slot for position `N` being `(N / K) % R`, each slot storing its `at`
beside the hash. Within one comparator every stored position is a multiple
of the one `K` it was built with, so a lookup is one division and one
compare of the stored `at`: O(1), no scan, whatever `R` the minimum
interval allows, and no allocation after construction. Writing a position
into a slot that still holds an older, uncompared one evicts it.

**The local schedule's start.** A comparator is built with `K` and with
`first_at`, the first position the local side digests under that `K`. The
local side does not adopt a new `K` at the instant the comparator is reset
(the matching stage and the shadow realign at their next batch boundary,
decision 2), so the comparator must not guess where the new schedule
starts. Instead, whoever realigns publishes it: the `DigestBoard` carries a
schedule record `(K, first_at)` (a seqlock of its own, written before the
first digest under it), and a comparator whose local side is fed from a
board takes `first_at` from that record, holding expected digests (up to
`R`, as always) until the record for its `K` appears. Two rules then keep
the grid exact and the counters honest:

- a digest on either side off the comparator's grid (`at % K != 0`, a
  straggler of the old schedule from the batch in flight at the change),
  and a local digest below `first_at`, is dropped, uncounted: nothing asked
  about it (the primary's digests are on its advertised grid by
  construction, so off-grid expected digests arise only in the in-process
  check, from the live stage's own batch in flight);
- an expected digest below `first_at` is `BeforeStart`, never `Lapped`.
  This covers exactly the new-`K` multiples the local side skipped in the
  batch in flight when it realigned, so the window of an interval change
  never reaches the `lapped` series.

Feeding either side returns a verdict for every position both sides now
hold:

- `Match { at }`, counted;
- `Mismatch { at, expected, actual }`, the divergence verdict;
- `Unverified { at, reason }`, counted, never a divergence, with one of two
  reasons:
  - `Lapped`: an expected digest whose local counterpart was evicted
    before it arrived, or the reverse. Impossible while decision 3's lag
    bound holds, so any occurrence is worth a look;
  - `BeforeStart`: an expected digest below the local schedule's
    `first_at`. This is normal right after a comparator reset (every
    session start, decision 7), at an interval change, and during
    catch-up, when the primary's first digests name positions this copy
    passed before the reset. It is
    counted under its own label so that it never adds noise to the
    `lapped` series an operator alerts on (decision 14), and it is
    logged at `debug!` only.

A local digest with no expected counterpart is not an event: the primary
does not send every digest (it skips any it lapped, and sends none during
catch-up), so silence is normal sampling. Both checks below use this one
type, so the in-process and cross-node rules cannot drift apart.

### 5. The primary emits from the journal stage, beside `ChainCheck`

`maybe_publish_state_digest`, called right after `maybe_publish_chain_check`
at the sync point, does nothing without replication producers. Otherwise,
with `end` the last sequence the batch just published, it picks the newest
digest at or below `end`:

- if the board's `latest()` position is at or below `end`, that digest
  (the matching stage trails the journal stage, by any distance);
- otherwise (the matching stage leads), `candidate = (end / K) * K`, looked
  up exactly with `get(candidate)` (one division per fsync batch, cold),
  which the board still holds because the matching stage leads by less
  than the ring's capacity.

If the pick is above the last digest it forwarded, it encodes
`StateDigest { at, hash }` and publishes it through
`publish_to_replication_rings` with `end_seq = end`, the same `end_seq`
convention `ChainCheck` uses. The ring's `end_seq` stays monotonic, and
`RingChunk::classify` sorts the chunk like any control frame during
catch-up. Looking only at `candidate` would not do: under a sustained lag
of the matching stage by more than `K`, every batch's `candidate` is a
position the matching stage has not reached yet, so the primary would send
no digest at all and its replicas would silently verify nothing. With
`latest()` first, a primary emits a digest at least every time the
matching stage completes one, whichever stage leads. Intermediate
multiples are skipped when several fall between two batches (fewer checks,
nothing wrong), and an idle primary still journals ticks, so batches keep
coming. A ring too full to take the frame evicts the replica exactly as
for a `ChainCheck`.

**Independent of the chain.** `maybe_publish_state_digest` is a sibling
of `maybe_publish_chain_check`, not code inside it: the chain check
returns early when `encoder.chain_hash()` is `None`, which is every batch
of the chain-less build (`--no-default-features`, `hash-chain` off), and
it runs on its own batch cadence (`CHAIN_CHECK_INTERVAL_BATCHES`). The
state digest shares neither gate: it needs only replication producers and
a board, so a deployment that turns the audit chain off still gets state
verification. Nothing in the digest path is behind `cfg(feature =
"hash-chain")`.

### 6. Wire: replication protocol 9

One bump for all three changes, landed in one step so that mixed pairs are
refused once:

| Change | Layout |
| --- | --- |
| New frame `MSG_STATE_DIGEST = 0x19` | `SeqHashFrame` reused: `[len:u32][0x19][at:u64][hash:[u8;32]]`, 41 bytes after the length |
| `StreamStart` gains the digest schedule | `digest_interval: U64` (0: the primary sends no digests) and `digest_scheme: U16` (0: none), appended after `genesis_entries` |
| `Handshake` gains a flags byte | `flags: u8` appended *after* `protocol_version`, so an older peer still reads the version and refuses with its explicit error (and the new decoder reads the version before the flags, below). Bit 0: the replica requests a state resync (decision 9). Other bits must be zero, and a frame with any set is refused. A flag byte rather than a bool, so the next request needs no layout change. |

`REPL_PROTOCOL_VERSION` becomes 9, with the history comment extended.

**The handshake must refuse a mixed pair in both directions with the
version error.** Today `decode_replica_message` parses the whole
`HandshakeFrame` with `ref_from_prefix` *before* reading
`protocol_version`. Growing the frame from 51 to 52 bytes would therefore
leave a v8 replica at a v9 primary with "handshake frame too short", not
the version error (only a v9 replica at a v8 primary would get the version
error, through the v8 decoder's prefix parse). The step splits the frame:
`HandshakeHead`, the existing 51-byte layout ending in `protocol_version`
and frozen from here on, is parsed first and the version checked; only
then is the extension (`flags`) parsed, with a too-short error of its own
that can no longer be a version skew. The head's layout gets a `const`
size assertion and a comment saying any later field goes in the
extension, so the next bump keeps the property.
`docs/replication.md`'s frame table gains the new frame and the changed
layouts.

### 7. The replica compares in its receiver

- `process_streaming_frames` decodes `StateDigest { at, hash }`. Since the
  primary sends a digest only once its stream has passed `at`, `at` above
  the stream position (`pending_accum`) means entries are missing: a
  `FrameError::SequenceGap`, the same contract as a mark ahead. A digest
  behind the position is normal, unlike a stale mark, and is fed to the
  comparator.
- The comparator lives on the receiver thread, owned by `streaming_loop`
  (one copy for kernel TCP and DPDK). Its local side is fed from the
  replica's own `DigestBoard`: one `Acquire` load of the board's latest
  position per loop iteration, draining only when it moved. The board
  handle rides `ReplicaPipelineHandles` beside `stream_marks`.
- The comparator is reset at every session start, and whenever the adopted
  interval changes: the expected side from an old session refers to the
  same lineage, but resetting is cheaper than reasoning about it. The reset
  fixes `K`; `first_at` comes from the board's schedule record once the
  matching stage has realigned (decision 4), so a session start that
  changes `K` mid-batch produces `BeforeStart` for the skipped positions
  and nothing else. When the session keeps the previous `K`, the board's
  current record already applies and is taken at once. In both cases the
  comparator's start is the later of that `first_at` and the first on-grid
  local digest it drains after the reset (it drains from the board's latest
  position at reset, not from its history), so positions this copy passed
  before the reset are `BeforeStart` too, as decision 4 says.
- **A `Mismatch` on a replica that is not latched ends the session** with
  a new `SessionExit::StateDivergence { at, expected, actual }`, to request
  the repair of decision 9. The match in `handle_session_exit` is
  exhaustive, so the compiler forces every receiver to decide what it
  means. A dedicated exit, rather than reusing `Fatal` with a journal error
  as the chain path does, because the journal is healthy and the repair
  differs.
- **A `Mismatch` on a latched replica neither ends the session nor spends
  anything.** It is counted (`melin_state_divergence_total`) and logged
  rate-limited (decision 14), and the replica keeps following. The latch
  already holds everything a further mismatch could tell the runtime: the
  copy is unpromotable, and a resync is requested at every reconnect
  (decision 9). Without this rule, the reset at each session start would
  turn a systematic divergence into a reconnect loop, one session per `K`
  entries. The latch lives in `ReplicaControlPlane`, which outlives the
  session, so the receiver reads it when a verdict arrives, and the rule
  is one branch in `streaming_loop`, not a convention each receiver
  follows.

### 8. The in-process check: shadow against live

On every node with a shadow (primary and replica alike) the shadow digests
at the same positions after applying `N` (same `DigestSchedule` helper as
the matching stage, so the position rule exists once). It feeds a
`DigestComparator` whose expected side is the live matching stage's board.
The shadow stage runs the comparator itself: it is not on the ack path, and
it already does cold work (snapshot scheduling) between batches. The live
stage usually leads the shadow (the shadow trails the journal), but need
not, so a shadow digest can wait in the comparator until the live one
appears. Here the shadow is the local side, so on an interval change (on a
replica, at a session start) it resets the comparator itself with its own
new schedule's `first_at`, with no board record to wait for, and the live
stage's off-grid stragglers and skipped positions fall under decision 4's
two rules.

On `Mismatch`:

- `error!` naming the position and both hash prefixes (`hex_prefix`, the
  journal crate's existing formatter), and `melin_state_divergence_total
  {against="shadow"}`.
- **The shadow is poisoned**: it stops saving snapshots, and records the
  last position verified before the mismatch (`verified_through`).
- **The primary stops serving suspect snapshots**: `servable_snapshot` /
  `preflight_snapshot_transfer` refuse a snapshot above `verified_through`,
  through the existing refusal path (the replica's journal is left
  untouched, and the error names the reason).
- The node keeps serving (open question 2 discusses halting).

### 9. Verdicts and repair: the chain path from the handshake onward

On a replica, `handle_session_exit` treats `SessionExit::StateDivergence`
as follows:

1. Logs `error!` (the copies disagree: a bug in the application or a
   memory fault, never a client's doing) and counts
   `melin_state_divergence_total{against="primary"}`.
2. Checks the **same** once-per-process budget as a chain divergence. If
   it is already spent (a resync, of either kind, already ran in this
   process), it exits with an error naming both divergences, as a second
   chain divergence does today. It does not spend the budget here.
3. Latches `ReplicaControlPlane::state_divergent` (process-lifetime, read
   by raft, decision 10, and exported as `melin_state_divergent`).
4. Tears the pipeline down (cleanly: the journal is fine), re-derives the
   position with `recover_replica_state`, and returns
   `AfterSession::Resync`. Every handshake of a latched replica carries
   flag bit 0, so the request is the latch itself rather than a field the
   reconnect path must remember to set.
5. Reconnects with the flag set.

**The budget is spent when a requested resync starts**, that is when the
primary answers the flag with `NeedSnapshot` and `handle_resync_verdict`
begins to archive. Spending it at the mismatch instead, as the chain path
does, would exhaust it before the replica learns whether a resync is
available, and the next mismatch on the rebuilt state would exit the
process: exactly the outcome the fallback below exists to avoid. The
chain path keeps spending at detection, because its handshake always
leads to a verdict (or to a refusal that leaves the replica disconnected
either way).

On the primary, a handshake with the bit set and a valid chain is
**treated as divergent**: it counts the new
`melin_replica_state_divergence_total`, logs `warn!` (degraded but
handled), runs the existing `preflight_snapshot_transfer` and sends the
verdict and the snapshot exactly as for `HashMismatch`. It sends
`NeedSnapshot`, not `HashMismatch`, because the primary has not judged the
journal divergent. The replica chooses the archive reason from what it
asked for. `handle_resync_verdict`'s `divergent: bool` becomes an exhaustive
`ResyncReason { Behind, ChainDivergent, StateDivergent }`, and
`ArchiveReason` gains `StateDivergent` (`<journal>.state-divergent.<n>`,
snapshot archived beside it as today). An operator can then tell a forked
journal from a forked state on disk. The archived journal is a byte-copy of
the primary's, kept anyway: the rule is "archived, never deleted", and it
costs one rename.

**When the primary cannot serve a snapshot** (none configured, boundary
pruned, or its own shadow poisoned), refusing the connection, as a chain
divergence's preflight failure does, would leave the replica not following
at all. Under a replica-backed ack policy the primary would then halt, so a
state problem on one copy would cost the cluster its writes. Instead the
primary logs `error!` and answers with a plain `StreamStart`. The replica,
having asked, reads that as "resync unavailable": it rebuilds its state
locally from its own snapshot and journal (`recover_replica_state` already
did), follows the stream so its journal copy keeps counting toward
durability, **stays latched divergent** (unpromotable), and has spent no
budget. Further mismatches on the latched replica are counted and logged,
rate-limited, and never end the session (decision 7), so a systematic
fault, which a local rebuild cannot fix, leaves the replica following
rather than exiting or looping. Any later reconnect (a network drop, a
primary restart, a failover) carries the flag again, so the resync
happens as soon as some primary can serve a snapshot. A local rebuild is a
real repair for a transient fault (a bit flip in live memory) and a no-op
for systematic nondeterminism. The digests that follow tell which, and a
later `Match` does **not** clear the latch (open question 4).

The latch clears only when a requested snapshot resync completes, because
the state is then the primary's.

### 10. Raft: a divergent copy must not lead

- `AutoPromotionInputs` gains `state_divergent`, and
  `auto_promotion_decision` refuses with an operator-facing reason ("this
  node's application state diverged from its primary's; it must not serve;
  promote another node, or restart this one, which rebuilds its state from
  snapshot and journal, and promote it then").
- `election_should_stand_down` stands down on the same latch. The node
  still votes (its journal tip is valid, and a quorum may need its vote),
  but never campaigns.
- **The gap: a divergent node that is already the raft leader.** Standing
  down stops timeouts, not an established leadership, and
  `challenger_should_campaign` only fires against a leader whose *tip* is
  behind. With equal tips the cluster would sit primary-less. Closing it
  needs the peers to know: one bit in the raft `RpcFrame` envelope beside
  `tip_epoch`/`tip_seq`, on which a healthy follower campaigns (the
  challenger rule's new arm) and the vote filter drops a divergent
  candidate's requests (the dual of standing down). `RpcFrame` is
  postcard-encoded with no version field, so this is a raft wire change: the
  step adds a version byte first (open question 5).
- **Manual `PROMOTE` of a latched node is refused.** Promotion hands the
  replica's warm in-memory application to the primary pipeline
  (`SessionExit::Promote` → `TeardownOutcome::Clean(ex, wr)` in
  `handle_session_exit`), that is the very copy the latch says is not
  verified: either the live state that disagreed, or a local rebuild that
  has disagreed again or has not been cleared by a resync (open question
  4). Its journal, by contrast, is faithful (the chain says so). This is
  the inverse of the `disk`-policy case, where the state is faithful and
  the journal may be short, so the eyes-on override there does not carry
  over. The admin command answers `ERR PROMOTE refused: this node's
  application state diverged from its primary's; restart it to rebuild
  its state from snapshot and journal, then promote it` and logs at
  `debug!` (an operator's request, refused and answered, like the
  command's other refusals). The latch is process-lifetime, so the restart
  is the override: it rebuilds the state by replay from the faithful
  journal, which repairs a transient fault outright, and a systematic one
  re-latches at the first digest once the node follows a primary again.
  Rebuilding inside the promotion path instead (`recover_replica_state`,
  then clear the latch) was considered and rejected: it is the same replay
  as the restart, but it makes a known-diverged node one command away from
  leading, and the restart keeps the decision an explicit act.
- **The refusal is enforced at every site that honours a request**, not
  only at the admin socket. A request filed before the latch (manual, or
  the raft driver's between `auto_promotion_decision` and the mismatch)
  is otherwise honoured by whichever site sees it first, and there are
  several: the in-session poll (`receiver_transport.rs`, ending in
  `SessionExit::Promote` and its arm in `handle_session_exit`), and each
  receiver's reconnect loop, which promotes while disconnected through
  `take_pipeline_for_promotion` (`tcp_receiver.rs` and `dpdk.rs`, at the
  loop top, which every backoff path `continue`s back to, so one site per
  loop). The waits that merely abort on a request
  (`settled_resume_point`, the DPDK connect window, the backoff sleep)
  honour nothing and lead back to a loop top, so they need no gate. A
  `SessionExit::StateDivergence` (step 7) followed by `AfterSession::Resync`
  reaches the loop top with the request still pending, which is the case
  a gate on the in-session poll alone would miss.
  - **One gate, enforced by the type system.** Honouring a request takes a
    `PromotionGrant`, a token with a private constructor that only
    `ReplicaControlPlane::grant_promotion(&self) -> Option<Result<PromotionGrant, StateDivergent>>`
    produces (`None`: nothing requested; `Err`: requested, but the latch is
    set). `take_pipeline_for_promotion` takes the grant by value and
    `SessionExit::Promote` carries it, so a new honour site cannot compile
    without passing through the latch check, and the compiler lists every
    existing one when the signatures change.
  - **A pending request on a latched node exits the process.** The request
    stays one-way: `promotion.rs` documents that it cannot be un-asked once
    the receive loop may have observed it, and two consumers depend on
    that. The raft-promotion thread leaves its loop for good once a request
    is pending and re-enables elections (`raft_promotion.rs`), and the
    raft driver's `SupersessionPolicy` treats a pending request as
    "serving". Withdrawing it would leave the node with no promotion
    monitor, elections enabled and no stand-down, so it could campaign, and
    nothing would make the "next evaluation" a withdrawal implies. On
    `Err(StateDivergent)` the receiver therefore returns
    `Err("promotion requested on a node whose application state diverged
    from its primary's; restart it to rebuild its state from snapshot and
    journal")`, which tears the pipeline down and ends the process as any
    receiver error does, logged at `error!` (the node stops). That exit is
    the restart that decision 10 already names as the override, and it
    clears both the request and the latch, since both live only in the
    process. It is not free: under the resync-unavailable fallback of
    decision 9 a latched replica keeps following so its journal copy counts
    toward durability, and exiting removes that ack. It is still the right
    call, because the request already meant the node was leaving replica
    duty, a raft-filed one implies the primary is down, and a latched node
    is already not a standby (`melin_state_divergent`). The raft-promotion
    thread's stand-down state needs no re-arming. The one change there is
    that its post-loop `elect_enabled.store(true)` becomes
    `store(!state_divergent)`, so a latched node cannot campaign in the
    short window before the exit. `SupersessionPolicy` keeps reporting
    serving in that window, which is harmless: its only effect is to fence
    and shut the node down on a newer epoch, which is the outcome anyway.
  - Rejected: withdraw the request and keep the node following. That needs
    a re-armable raft-promotion thread, a restored stand-down and a
    supersession claim that can be taken back, all to keep a replica alive
    that cannot be promoted or counted as a standby until a resync clears
    its latch.
- No raft log entry, no RPC and no change to the raft thread are involved:
  digests never touch the control plane's tokio thread.

### 11. The feed: digests reach subscribers as an output payload

So that gateways can compare without a protocol of their own, the matching
stage emits an `OutputPayload::StateDigest(StateDigest)` slot at each
digest position (`wire_seq = N`, no connection, no reply). The response
stage passes it through like `AckPolicyChange`, and the event publisher
yields it in order and only after the ack gate, so a subscriber sees the
digest at `N` only once `N` holds the copies the policy demands. Two costs
to settle in that step:

- **Breaking for consumers that match `OutputPayload`** (no
  `#[non_exhaustive]`, deliberately): `CHANGELOG.md` **Changed**.
- **Slot width.** A 32-byte variant widens `OutputSlot` for an application
  whose reports are narrower, on every slot of the hot path. The step adds
  `const` size assertions for the examples' slots. If the variant widens
  any, the hash travels instead in a small SPSC lane from the matching stage
  to the publisher, paired with a dataless marker slot, dropped (and
  counted) when full: the feed is a sampling check, and a full lane must not
  backpressure the matching stage, since ticks keep flowing even while the
  gate holds the feed. A lookup in the board by position is *not* an
  option: entries that produce no output slot (ticks) let the matching
  stage run arbitrarily far ahead of a held feed, so the board would lap.

### 12. Snapshots, recovery, failover, rejoin

| Situation | Behaviour |
| --- | --- |
| Restart, recovery from snapshot + journal | The schedule realigns at the next multiple of `K`. Nothing is compared during replay (nothing to compare with). Afterwards live and shadow start from the same recovered state, and replicas compare as usual, which turns every primary restart into a cross-node check of *replay* against the replicas' *live* history. |
| Replica booting from its own snapshot | Same. A `snapshot`/`restore` pair that drops a field is caught at the first digest after boot. |
| Replica catching up from the primary's journal | The disk walk carries no digests. Ring chunks holding digests are classified like any control frame. Local digests during catch-up go unmatched, which is normal sampling. |
| Snapshot resync (any reason) | Pipeline rebuilt, so a new board and a new comparator. The latch clears if the resync was a requested state resync. |
| Promotion | The warm application moves into a new primary pipeline with a new board. The interval reverts to the new primary's own configuration and is advertised to its replicas on their `StreamStart`. |
| Ex-primary rejoining | A forked journal is caught first by the chain check and repaired by snapshot resync. Otherwise its state, built by its own live apply, is compared against the new primary from the first digest. |
| Shutdown drain | No digests: the drain has no comparator to feed, and nothing it would find is acted on before exit. |

### 13. Hot-path cost

| Where | Per event | Per digest (every K) | Per batch / iteration |
| --- | --- | --- | --- |
| Matching stage, app without a digest | nothing (branch removed at monomorphization) | nothing | nothing |
| Matching stage, app with a digest | one compare against `next_digest_at`, in the allocating arm, predicted not-taken | the app's digest (contract: O(1)), one seqlock store, one latest-position store | replica only: one `Relaxed` interval load |
| Shadow | same compare | digest, comparator update | one board load while a shadow digest waits |
| Journal stage (primary with replicas) | nothing | 41-byte frame encode and ring publish | one latest-position load, at most one division, one board read |
| Replica receiver | nothing | decode, comparator update | one `Acquire` load |

Nothing allocates after construction (board and comparator are fixed
arrays). The measurement step runs `melin-bench` closed-loop against echo
(must be unchanged within noise: the opt-out is free), counter and notary
with digests at the default interval and disabled, comparing p50, p99 and
p99.9, and reads the `latency-trace` "matching: execute" histogram for the
digest-position outliers. Results go in a dated measurement document naming
its hardware, not here.

### 14. Observability

Logs (levels per the project convention):

| Event | Level | Where |
| --- | --- | --- |
| Replica digest disagrees with the primary's | `error!` | replica |
| A further disagreement while latched | `error!`, first then at each doubling of the count | replica |
| An expected digest below the local schedule's start (after a reset or an interval change) | `debug!` | reader |
| Shadow digest disagrees with live | `error!` | any node with a shadow |
| Requested state resync unavailable, following on a rebuilt state | `error!` | both sides |
| A replica requested a state resync | `warn!` | primary |
| A pending promotion on a latched node, ending the process | `error!` | replica |
| Admin `PROMOTE` refused on a latched node (answered `ERR`) | `debug!` | replica |
| Archiving the local lineage for a state resync | `warn!` | replica (as for a chain divergence) |
| Digests off for this session (scheme or opt-in differs) | `warn!`, once per session | replica |
| A digest lapped before it could be compared (should be impossible) | `warn!`, first then at each doubling of the count | reader |
| A digest call took longer than a fixed threshold | `warn!`, rate-limited | matching / shadow |
| Digests enabled at interval K, scheme S | `info!` at pipeline start | every node |
| A match | none (metric only) | |

Metrics (`/metrics`, rendered in `health.rs`, on replicas through the
replica health endpoint):

| Metric | Type | Meaning |
| --- | --- | --- |
| `melin_state_digest_interval` | gauge | interval in force (0: off) |
| `melin_state_digest_sequence` | gauge | last position this node's live stage digested |
| `melin_state_digest_checks_total{against="primary"\|"shadow"}` | counter | positions compared and found equal |
| `melin_state_divergence_total{against="primary"\|"shadow"}` | counter | mismatches. Alert on any growth |
| `melin_state_divergent` | gauge | 1 while the latch is set. Alert, and do not count this replica as a standby |
| `melin_state_digest_unverified_total{reason="lapped"\|"scheme"\|"before_start"}` | counter | digests that could not be compared. `before_start` grows at every session start, at an interval change and during catch-up and is not an alert; alert on `lapped` |
| `melin_replica_state_divergence_total` | counter | primary: state resyncs requested by replicas |

`melin_replica_divergence_total` keeps its meaning (journal chain) so that
existing alerts do not change. Optionally, an admin `DIGEST` command
answering the latest `(position, hash)` lets an operator compare nodes by
hand. It is cheap (one board read) and left to the observability step.

## Steps

Each step builds, passes the full suite (kernel TCP, and the DPDK suite
under the netns runner), and is committable on its own. Docs and
`CHANGELOG.md` move with the behaviour they describe.

### Step 1. The application surface

- `melin-app`: `StateDigest`, `StateDigestSpec`, `Application::STATE_DIGEST`
  (default `None`), rustdoc carrying the contract of decision 1.
- `counter`: digest over its value. `notary`: digest over head and entry
  count. `echo`: none. `transport-core`'s `TestApp`: an order-independent
  digest over its `HashMap`, the shape the docs recommend.
- One contract-check helper, shared rather than copied per example: apply a
  sequence to two instances and compare. Snapshot, restore and compare.
  `prefault` and compare. Change state and see the digest change. It lives in
  `melin-app` behind a `test-utils` feature (the name `melin-journal`
  already uses for the same purpose), and the examples and `transport-core`
  dev-depend on it.
- Tests: the helper run over each digesting app, including `TestApp` with
  keys inserted in different orders (iteration-order independence).
- Docs: `docs/building-an-application.md`, "State digest (optional)":
  contract, the incremental shape, cost. `CHANGELOG.md` **Added**.

### Step 2. Board, schedule and comparator (pure building blocks)

- `transport-core`: `DigestSchedule` (`next_digest_at`, realign on interval
  change; the one statement of the position rule), `DigestBoard` (decision
  3, sized from `INPUT_RING_CAPACITY` and `K`), `DigestComparator`
  (decision 4). `MIN_STATE_DIGEST_INTERVAL` (decision 2).
- Tests, all ungated so they run on every host:
  - schedule: positions are exactly the multiples of `K` from any starting
    sequence, and realignment after an interval change lands on multiples
    of the new one;
  - board: exact-position lookup, lap detection, `latest`; a two-thread
    stress test (writer and reader) asserting no torn value is ever
    returned; run under Miri with `scripts/`' existing Miri job, as the
    seqlock is;
  - comparator, property tests over random interleavings of the two sides
    with lags bounded as decision 3 states: never a `Mismatch` when the two
    sequences agree, a `Mismatch` at exactly the first differing position
    both hold, never `Unverified { Lapped }` while the lag bound holds,
    `Unverified { BeforeStart }` exactly for expected positions below the
    local schedule's start (the post-reset case), and
    `Unverified { Lapped }` (not `Mismatch`) when the bound is exceeded;
  - comparator across an interval change: reset with a new `K` while the
    local side still emits old-`K` positions for one batch and then
    publishes its schedule record; the new-`K` multiples skipped in that
    batch come out `BeforeStart` and never `Lapped`, the old-`K`
    stragglers are dropped uncounted, and every position stored is on the
    new grid; the same with the record already present (unchanged `K`);
  - board: the schedule record is written before the first digest under it
    (a reader that sees a digest at `first_at` sees the record);
    and, with `K` at `MIN_STATE_DIGEST_INTERVAL`, that each slot index is
    `(at / K) % R` for both the board and the comparator (one helper, so
    the two cannot index differently).

### Step 3. Every copy digests; the shadow checks in-process

- `MatchingStage` and `shadow::run` digest through `DigestSchedule`, in the
  allocating arm only. The matching stage publishes to the board, and the
  shadow feeds its comparator from it (decision 8). Poisoning: snapshots
  stop, `verified_through` recorded, and `servable_snapshot` refuses above
  it.
- `ServerConfig::state_digest_interval` and `--state-digest-interval`
  (validated against the minimum, 0 disables), threaded to both pipeline
  builders (primary in `server.rs`, replica in `replication/mod.rs`).
- Metrics: `interval`, `sequence`, `checks_total{against="shadow"}`,
  `divergence_total{against="shadow"}`. Logs per decision 14.
- Tests:
  - `pipeline_tests.rs`: matching and shadow over the same slots publish
    identical digests at exactly the multiples of `K`; queries and
    non-allocating slots are never digest points (pins the `wire_seq`
    agreement a false divergence would come from);
  - a fault-injecting test app whose `restore` leaves a non-serialized flag
    that perturbs `apply`, which models a lossy `snapshot`/`restore`. The
    shadow (built by `clone_via_snapshot`) diverges, the comparator reports
    the first digest position after the fault, snapshots stop, and
    `preflight_snapshot_transfer` refuses;
  - integration, `server-runtime/tests/state_divergence.rs`, started
    through `melin-test-node` so it also runs on DPDK under the netns
    runner: one node with that app, the metric grows, the `error!` appears
    (`logs::capture`), and no snapshot is written after the mismatch.
- Docs: `docs/pipeline-architecture.md` (matching and shadow stage duties,
  the responsibilities table).

### Step 4. Replication protocol 9

- `protocol.rs`: tag, encoder and decoder for `StateDigest` (on
  `SeqHashFrame`, its doc comment widened to the three frames that share
  it), the appended `StreamStart` and `Handshake` fields, the size
  assertions updated, the version bump with its history line.
- Every sender fills `StreamStart` from its configuration (kernel
  `tcp_sender.rs`, the DPDK sender and its `join_worker.rs`). Every receiver
  decodes and, for now, ignores the new fields. The handshake flag is always
  0.
- Tests: round trips; wire byte patterns beside `ack_wire_byte_pattern` and
  `rotate_wire_byte_pattern`; a v8-layout handshake (51 bytes, version 8)
  refused with the *version* error, not "too short", which pins the
  head-first parse of decision 6 (the existing
  `a_handshake_from_another_protocol_version_is_refused` varies only the
  number in a current-length frame, so it is kept and this case added);
  a v9-length handshake carrying another version refused with the version
  error; a handshake shorter than the head refused as too short; a v8
  `StreamStart` refused with the upgrade advice (extending the existing
  v6/v7 tests); a handshake with an unknown flag bit refused; `RingChunk::classify` sorts a
  `StateDigest` chunk exactly as a `ChainCheck` one (ungated, so the DPDK
  sender's `LiveHandoff`, which shares the rule, is covered on every host).
- Docs: `docs/replication.md` frame table. `CHANGELOG.md` **Changed**:
  replication protocol 9, mixed pairs refused.

### Step 5. The primary streams digests; replicas detect divergence

- Journal stage: `maybe_publish_state_digest` (decision 5).
- Replica: adopt interval and scheme from `StreamStart` (decision 2, warn
  once on a scheme difference), decode digests in
  `process_streaming_frames`, compare on the receiver (decision 7). In
  this step a `Mismatch` **never ends the session**: the first latches
  `state_divergent`, logs `error!` and counts, and later ones are
  counted and logged rate-limited, as decision 7 specifies for a latched
  replica. The replica keeps following (no repair yet).
  `SessionExit::StateDivergence`, the one thing ending the session would
  be for, arrives with the repair in step 7, so this step cannot leave a
  diverged replica reconnecting once per `K` entries.
- Metrics: `checks_total{against="primary"}`,
  `divergence_total{against="primary"}`, `melin_state_divergent`,
  `unverified_total`.
- Tests:
  - `pipeline_tests.rs`: `primary_emits_state_digest_after_its_position`,
    modelled on `primary_emits_chain_check_every_interval`: classify the
    ring and assert every digest frame names a multiple of `K` at or below
    its chunk's `end_seq`, and none precedes the batch holding its
    position, with the matching stage made to lag and to lead the journal
    stage; and, with the matching stage held more than `K` behind the
    journal stage for many batches, that digests **are** emitted (one per
    digest the matching stage completes), which pins the `latest()` arm of
    decision 5 against the starvation a `candidate`-only lookup has;
  - the same emission test with `hash-chain` off: digests are emitted and
    no `ChainCheck` is (so these tests, unlike their model, which carries
    `cfg(all(feature = "hash-chain", not(feature = "no-persist")))`, are
    gated on `no-persist` only). The pre-commit hook and CI only `cargo check` the
    chain-less build, so the step adds a test run of this case
    (`cargo nextest run -p melin-transport-core --no-default-features`
    filtered to the state-digest tests) to the pre-merge job's chain-off
    loop; a check alone would not notice digests silently gated off;
  - `receiver_transport.rs`: a digest ahead of the stream position is a
    `SequenceGap`; one behind is compared; a mismatching one latches and
    does not end the session, and a run of further mismatching ones is
    counted without a session exit (beside
    `streaming_chain_check_at_exact_position_is_queued`);
  - integration (`state_divergence.rs`, both transports): primary and
    replica healthy under load, checks grow on the replica and nothing
    diverges; a replica given a fault through its `Sizing` (the one input a
    test may vary per node: the test app's `prefault` stores "perturb the
    event that brings my count to X", violating `prefault`'s contract on
    purpose) is detected at the first digest position after X and latched,
    and several digest positions later it is still following, with no
    session exit, one `error!` for the first mismatch, and
    `divergence_total` counting the rest;
    a replica whose app has another scheme compares nothing and counts
    `unverified{reason="scheme"}`;
  - integration, no false positives across the transitions of decision 12:
    a replica that joins mid-run (journal catch-up, then live), one
    restarted from its own snapshot, and a primary restarted (recovery
    replay) under traffic, each followed by a sustained clean stream of
    checks.

### Step 6. Raft: refuse, stand down, challenge

- `raft_promotion.rs`: `state_divergent` in `AutoPromotionInputs`, a new
  refusal, a new stand-down ground.
- Manual `PROMOTE` refused on a latched node (`admin.rs`, decision 10).
  `PromotionGrant` and `ReplicaControlPlane::grant_promotion`, taken by
  `take_pipeline_for_promotion` and carried by `SessionExit::Promote`, so
  every honour site (the in-session poll in `receiver_transport.rs`, the
  reconnect loops of `tcp_receiver.rs` and `dpdk.rs`) passes the latch
  check. A refused grant ends the process with the error of decision 10.
  `raft_promotion.rs`: the post-loop `elect_enabled` store respects the
  latch.
- `melin-raft`: a version byte in `RpcFrame`, then the `state_divergent`
  bit. `challenger_should_campaign` gains the divergent-leader arm, and the
  recency filter drops a divergent candidate's vote requests.
- Tests: the pure functions, exhaustively, as their existing tests do
  (refuses when latched, otherwise unchanged; stands down when latched; a
  follower campaigns against a latched leader with an equal tip); the frame
  version is refused when it differs; `admin.rs`: `PROMOTE` on a latched
  node answers the refusal and files no request; `grant_promotion`: none
  when unrequested, a grant when requested and unlatched, a refusal when
  requested and latched; `receiver_transport.rs`: a request filed before
  the latch is not honoured once the latch is set and the session ends in
  the error (in this step a mismatch ends no session, so within the same
  session), and a request on an unlatched node promotes as today;
  `tcp_receiver.rs` and `dpdk.rs` (the latter under the runner): a request
  pending on a latched node while disconnected (filed before the first
  connect, and filed during a backoff, both reaching the one loop-top
  site) ends the receiver in the error rather than promoting;
  `raft_promotion.rs`: after a request on a latched node the thread leaves
  `elect_enabled` false; integration beside
  `raft_failover.rs`: three nodes, one replica latched through its
  `Sizing`, the primary killed, and the healthy replica promotes, including
  the run where the latched node held raft leadership. The data plane runs
  on DPDK under the runner, and raft is kernel TCP by design.
- Docs: `docs/replication.md` (failover section), and
  `docs/internal/raft-control-plane.md` (the new rule and the frame
  version).

### Step 7. Repair: a requested state resync

- Replica: `SessionExit::StateDivergence`, ended by the first mismatch of
  an unlatched replica (decision 7; step 5's latched behaviour is
  unchanged), and `handle_session_exit` per decision 9 (budget checked,
  recovery, `AfterSession::Resync`, the handshake flag derived from the
  latch), the budget spent when `NeedSnapshot` starts the resync,
  `ResyncReason`, `ArchiveReason::StateDivergent`, the "resync
  unavailable" fallback, the latch cleared on a completed resync.
- Primary: a flagged handshake routes through the snapshot path, as in
  decision 9, on both senders. Today the kernel sender (`tcp_sender.rs`)
  and the DPDK join worker (`join_worker.rs`) each turn the validation
  verdict into "stream, or snapshot with which verdict frame". The step
  hoists that decision into one function both call, with the flag as one
  more input, rather than adding the check twice. An unavailable snapshot
  answers `StreamStart`.
- Tests:
  - `tcp_receiver.rs`' `divergence_resync` module, against its scripted
    primary: a state divergence reconnects with the flag set, archives to
    `.state-divergent.0` on `NeedSnapshot`, installs the snapshot, clears
    the latch; a second divergence in the process exits with the error
    naming both; a `StreamStart` in answer to the flag keeps following,
    stays latched and leaves the budget unspent, and a run of further
    mismatching digests then ends no session; a later reconnect of the
    latched replica carries the flag again, and a `NeedSnapshot` to it
    resyncs (the budget, unspent, is spent then); a promotion request
    filed just before the mismatch does not promote after the
    `SessionExit::StateDivergence` and the rebuild: the reconnect loop's
    disconnected-promotion path refuses the grant and the receiver ends in
    the error of decision 10 (decision 10's gate, now across a session
    boundary). The same case runs on DPDK in `dpdk.rs`' tests under the
    runner;
  - primary side: a flagged handshake with a valid chain gets `NeedSnapshot`
    and a transfer, and counts the metric; with no servable snapshot (none
    configured, or the shadow poisoned) it gets `StreamStart` and logs;
  - integration (`state_divergence.rs`, both transports): the step 5 fault
    replica repairs in-process, its archive exists, digests match after the
    resync, and the latch clears. The fault does not recur because the
    replica's count now comes from the primary's snapshot, past X. A
    primary whose shadow is poisoned refuses the snapshot, and the replica
    follows, latched; and the case the fallback exists for: a deterministic
    fault that replays into the rebuilt state (the `Sizing` fault with no
    servable snapshot, on a primary started without snapshots), where the
    replica is still following and its acks still advance the primary
    under a replica-backed policy after several more digest positions, the
    process alive and the budget unspent.
- Docs: `docs/replication.md`, "Divergence repair is automatic", extended to
  state divergence, with the archive name and what an operator does with
  it.

### Step 8. Digests on the feed

- `OutputPayload::StateDigest` (decision 11), the response stage's pass,
  the publisher contract documented on `EventPublisherFn`, the slot-size
  assertions, and the side lane if the assertions demand it.
- Tests: beside `event_publisher_gate.rs`, the publisher sees the digest at
  `N` in order with the reports around it, and only once `N` satisfies the
  ack policy (held while a replica the policy needs is gone).
- `CHANGELOG.md` **Changed** (new variant).

### Step 9. Measurement, operator docs, roadmap

- The measurement of decision 13, in a dated document with its hardware.
- `docs/replication.md`: a section "Application state verification" for
  operators: what is checked and where, what a divergence means (a bug in
  the application, or failing hardware, never a client's doing), what
  happens automatically, which metrics to alert on, how to read a
  `.state-divergent.<n>` archive, the interaction with failover (a
  divergent node neither auto-promotes nor accepts `PROMOTE`; restarting it
  is the override), and the
  `--state-digest-interval` flag with its trade-off.
- `docs/internal/application-api-review-2026-09.md` S8 and the roadmap point
  here. The admin `DIGEST` command, if taken.

## What this leaves untested, and why

- **Hardware faults** (a bit flip in live memory) are modelled only by the
  fault-injecting apps. The detection path is the same, so the model covers
  it.
- **The slow-digest warning** is tested with an application whose digest
  sleeps, in a unit test of the timing check rather than end to end, since
  wall-clock thresholds in integration tests flake.
- **Lapped digests** cannot happen while decision 3's bound holds. The
  property test exceeds the bound in the comparator directly, and no
  integration test tries to produce one.
- **A rolling upgrade across a scheme change** is covered by the two-app
  integration test (step 5), not by running two builds.

## Open questions and trade-offs

1. **Ordering on the stream: primary-held, replica-buffered, or both?**
   Discussed in finding 2. Recommendation: both. The journal-stage
   injection is the existing `ChainCheck` mechanism, costs one board read
   per fsync batch and makes "digest ahead of the stream" a detectable
   violation. The replica's buffer is needed regardless.
2. **What a primary does when its shadow disagrees with its live state.**
   Options: (a) alert, poison the shadow, refuse to serve suspect
   snapshots, keep serving (the plan); (b) also halt writes through the
   existing halt machinery, since the node's journal no longer replays to
   what its clients saw; (c) a flag choosing between them. Recommendation:
   (c), defaulting to (a). A false halt on a trading venue is itself an
   incident, and the operator, paged by `error!` and the counter, is better
   placed than the runtime to decide. A further question is what a
   *restart* of such a primary recovers from, since the poison is in
   memory. Recommendation: on poisoning, move snapshots above
   `verified_through` aside (archived, never deleted), so a restart recovers
   from the `.prev` copy or the journal, and refuses to start, naming the
   archive, if neither reaches.
3. **Where K comes from.** The alternative to adopt-from-`StreamStart` is a
   fixed per-application constant (no flag): identical by construction for
   one build, with nothing to configure, but not tunable to a deployment's
   event rate. Recommendation: the flag with the application's default,
   adopted by replicas. A journaled interval, the strongest option, is ruled
   out by the input-only journal.
4. **Should a later match clear a replica's latch after a local rebuild?**
   A transient fault would then heal without operator action, but
   nondeterminism that happens not to show at the next few digest positions
   would clear it too. Recommendation: no. Only a resync from the primary
   clears it, and the operator can force one by restarting the replica with
   its lineage moved aside.
5. **The raft frame version.** Adding the divergent bit means a raft wire
   change, and `RpcFrame` has no version today, so a mixed raft mesh would
   fail to decode rather than refuse clearly. Recommendation: add the
   version byte in step 6 before the bit, and document that raft peers
   upgrade together, as replication pairs already must.
6. **One repair budget or two?** Sharing the once-per-process budget between
   chain and state divergence is conservative: two divergences of any kind
   in one process mean something systematic. Separate budgets would let a
   replica that rejoined through a chain repair also repair its state once.
   Recommendation: shared, revisited if it ever bites.
7. **Primary-side nondeterminism.** If the *primary's* `apply` is the
   nondeterministic one, every replica diverges, resyncs once to the
   primary's state, and exits at the second divergence. Under a
   replica-backed policy the primary then halts. That is loud and safe, but
   it costs availability, so the budget-exhausted error says that a
   divergence on several replicas at once points at the primary. Comparing
   replicas with each other (a majority) is out of scope.

## Not part of this plan

- Journaling digests, or carrying them in snapshot headers (both formats
  stay untouched; a snapshot-header digest would let a restore check
  itself, and can follow later on its own merits).
- Replica-to-replica comparison and majority verdicts.
- Merkle proofs of state (see [merkle-hash-chain.md](merkle-hash-chain.md)
  for the journal's).
- Making `apply` deterministic: this plan detects nondeterminism; the
  determinism work (for example "Monotonic sequencer time") removes
  sources of it.
