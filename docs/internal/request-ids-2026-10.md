# Request IDs on the client protocol (plan)

Status: **built in melin** (steps 1 to 6: the slots' request ID, client
protocol 1, early replies released at batch boundaries, the shed tail and
parking on both transports, the metrics and the docs); **step 7, the
Exchange Core's migration, is open**, as is the Aeron benchmark harness's
Java rig, which frames requests itself (decision 9). The design below is
the one reviewed before the work began, with the questions the first
draft left open settled (see "Settled in review" at the end); "What the
code shows" describes the code as it was then.

Closes finding 18 of the [determinism audit](determinism-audit-2026-09.md)
("A ServerBusy reply can overtake replies to earlier requests"), S1 of the
[application API review](application-api-review-2026-09.md) ("A request the
decoder refuses gets no reply"), the two client-facing entries of
[lost signals](lost-signals-2026-10.md) ("Pipelined writes after a full input
ring are never parsed", "ServerBusy overtakes earlier replies"), and the
[transport divergence](transport-divergences-2026-10.md) "DPDK `PipelineFull`
drops the client instead of sending ServerBusy".

The one-line goal: **every request carries an identifier the client chose,
and the node answers every request exactly once, naming it.** Correlation
stops depending on order, so a request the node does not accept can be
answered immediately, from wherever the decision is made, without the
ordering machinery each such path needs today.

"Exactly once" holds for as long as the connection lives. A node that
stops or is fenced closes its connections and answers nothing more (see
"A reply still waiting when the node stops" in
[pipeline architecture](../pipeline-architecture.md)); the client sees
the close, as today, and reconciles.

Not a goal: idempotency. Whether a retry may be applied twice is the
application's decision and its design varies by application
([building an application](../building-an-application.md), "Retries and
duplicates"). The request ID is a transport correlation token: never
journaled, never replicated, never handed to the application's decoder,
`apply` or `query`, and meaningless after the connection ends. An
application's own identifiers (an order ID, the Exchange Core's
`request_seq`) live in the request body, which is journaled, and remain
what an audit trail correlates on.

## What the code shows

- **Pairing is positional.** Replies carry no identifier (`halt.rs`
  module docs, "Delivered in order"). `melin-client`'s blocking
  `Connection` leaves pairing to the caller; `PolledConnection` keeps one
  `in_flight: u64` counter and decrements it on `BatchEnd` and on
  `ServerBusy`, so a busy settles "the oldest" request; `melin-bench`
  (`load.rs`, `sent_ns: VecDeque<u64>`) and `melin-shm-proxy`'s trace
  (`in_flight: VecDeque<u64>`) keep FIFOs. The Exchange Core gateway and
  bench are reported to keep FIFOs too (not verifiable from this
  repository).
- **Four paths break the invariant "one terminal frame per request, in
  request order".**
  - `ServerBusy` goes out of band. On `FrameAction::PipelineFull` the
    io_uring reader (`reader.rs`, `process_frames`) sends
    `ControlEvent::PipelineBusy` on the unbounded control channel, and the
    response stage appends the busy frame during its control drain, ahead
    of replies still in the output ring. The stage reads the ring with
    `read_contiguous(MAX_BATCH)`, so one event's slots can straddle two
    reads (the read cap, the ring's wrap), and the busy frame can then land
    between an event's `Response` frames and its `BatchEnd`.
  - `Decoded::Filter`, `Decoded::PermissionDenied` and
    `Decoded::DecodeError` are dropped with no reply
    (`client_frames.rs`, `process_client_frames`), so every later reply on
    a pipelining connection is paired with the request before it. So are
    an empty frame and a frame under any tag but `TAG_APP`.
  - A full refusal queue during a halt is `PipelineFull` too, and falls
    back to the same out-of-band `ServerBusy`.
  - After `PipelineFull` the frames behind the shed one stay in
    `parse_buf` until more bytes arrive, which a client waiting on its
    replies never sends. DPDK (`dpdk_transport.rs`) closes the connection
    instead, throwing away the replies to writes already published.
- **Halt refusals hold the invariant at a cost.** `RefusalQueue` stamps
  each refusal with the input sequence the request would have taken and
  releases it once every earlier event is answered (`idle_bound`, `sync`,
  `release`). That only exists because pairing is positional, and it is
  why a refusal waits behind a write the ack policy holds.
- **Replies to published requests are in request order.** All replies,
  from every connection, writes and queries alike, flow through one
  output ring in input order. The durability gate (`DurabilityGate::admit`)
  blocks the response stage inline before a slot is handled, and a query's
  output slot carries the previous `wire_seq`, so it waits too. Nothing in
  this plan changes that.
- **The gate can block inside an event.** An event's slots share one
  `wire_seq`, so the gate usually blocks only at an event's first slot. Not
  always: `idle_recheck` overwrites `cached_durable_pos` with a fresh
  evaluation, and a degraded evaluation pins it to 0, so an event whose
  slots straddled two reads can block on a later slot. `admit`'s
  `before_wait` is an `FnOnce`, called once before the wait; nothing runs
  during it.
- **`EngineError` is never produced.** `OutputPayload::EngineError` is
  encoded when present (`response_frame.rs`), but nothing constructs it.
  When encoded it is a payload frame followed by a `BatchEnd`, not a
  terminal, and the two clients disagree: `collect_batch` treats it as
  terminal, `PolledConnection::poll_found` does not settle a request on it.
- **There is no client protocol version.** Past breaks were signalled by
  changing tags, so a mismatched client fails the handshake and sees its
  key refused (0.18.0, which removed the journaled request sequence and
  changed the challenge response). The replication protocol has
  `REPL_PROTOCOL_VERSION`; the client protocol has nothing equivalent.
- **The challenge is encoded in one place and parsed in several.** The
  client listener (`server.rs`), the DPDK poll thread (`dpdk_transport.rs`)
  and the admin listener (`admin.rs`) all go through
  `encode_transport_response`. `Handshake::feed` parses it for every
  `melin-client` surface; test helpers parse it by hand (`server.rs`'s
  `read_challenge`, `admin.rs`'s tests, `replicated_failover.rs`'s
  `answer_challenge`). `raft_failover.rs` only checks the tag, and
  `dpdk_veth` goes through `Handshake`.
- **The reader writes an input slot field by field.**
  `process_client_frames` fills the ring slot inside `try_push_with` one
  field at a time, so a field added to `InputSlot` compiles there unset
  and the slot keeps its previous occupant's value.
- **The io_uring reader cannot simply stop reading a connection.** It
  arms one multishot `RecvMulti` per connection, which keeps delivering
  until the kernel ends it (`push_recv_multi`, `multishot_active`), and it
  sleeps in `submit_and_wait` until a CQE, the tick timeout or its eventfd
  wakes it. Its `parse_buf` has no cap (the DPDK one has `MAX_PARSE_BUF`).
  The idle-connection scan reaps any connection with no received bytes
  for `connection_timeout`.
- **`OutputSlot` is visible to application code.** An event publisher
  (`EventPublisherFn`) consumes the output ring, so a field added there is
  visible to it and is a breaking change for code that builds slots.
- **The precedent to distinguish from.** 0.18.0 removed a `u64` request
  sequence from the wire, the journal and replication: the runtime no
  longer read it, yet every entry and slot carried it. The ID planned here
  is the other half. The runtime needs it, but only for one connection's
  lifetime, so it stays out of the journal and replication.

## Relationship to other plans

- **[Degraded acks on a halted node](halt-degraded-acks-2026-10.md)**
  (steps 1 to 5 proposed) adds a second batch terminator,
  `BatchEndDegraded`, and `melin-client`'s `Ack` on `BatchEnd`. Neither
  exists on `main` yet. This plan reserves tag `0x0A` for it and fixes its
  payload as `[id: u64]`, like `BatchEnd`'s. Whichever lands second adopts
  the other's shape: if degraded acks land first, step 2 here adds the ID
  to both terminators; if this plan lands first, that plan's step 2 adds
  `BatchEndDegraded { id }` with `melin-client`'s
  `BatchEnd { id, ack }`. Both are client-breaking, so they should ship in
  the same release, under one protocol version (decision 1).
  *What "reserves" means, if this plan lands first:* step 2 defines
  `TAG_BATCH_END_DEGRADED = 0x0A` and puts it in `CONTROL_TAGS`, so the
  compile-time tag checks (decision 3) already cover it, and nothing else:
  no `TransportResponse` variant, no `melin-client` `Frame` arm,
  `terminal_request_id` returns `None` for it, and the shm-proxy trace does
  not count it. A `0x0A` frame from a node is therefore `Error::Protocol`
  in the client until the degraded-acks plan's step 2 adds the variant, the
  `Frame` arm, the `terminal_request_id` case, the proxy's count, and their
  tests, all in its own commit. If degraded acks land first, those pieces
  exist and step 2 here gives them the ID like the other terminals. The
  mentions of `BatchEndDegraded` in decisions 4 and 9 below describe the
  end state, once both plans have landed.
  That plan's decision 4 ("Refusals keep their place in each connection's
  order") is superseded by decision 6 here; update its text when step 3
  lands. Its rule that a refusal ends in a plain `BatchEnd` stands.
- **API review F1** (make the halt refusal a protocol frame and drop
  `build_reject`) becomes possible once `Refused` exists, but changes the
  application API. Not part of this plan; halt refusals stay application
  reports (decision 3).
- **Lost signals, "A partly sent reply waits out another connection's
  gate"** is related: decision 6 gives the gate wait a hook that flushes.
  Whether that hook also retries a send cut short by `EAGAIN` is that
  entry's fix, not claimed here.

## Design decisions

### 1. A client protocol version, carried by the challenge

`CLIENT_PROTOCOL_VERSION: u16 = 1` in `melin-wire-protocol`
(`control_codec.rs`, beside the tags). Protocol 0 is every earlier
release. The node's challenge becomes
`[TAG_CHALLENGE][version: u16 LE][nonce: 32]`, a 35-byte payload, on the
client listener, the DPDK poll thread and the admin listener, which all
use `encode_transport_response`.

- **New client, old node.** `Handshake::feed` takes a 33-byte challenge as
  protocol 0 and fails with a new
  `Error::ProtocolVersion { node: u16, client: u16 }` naming both, before
  signing anything. A 35-byte challenge with any other version is the same
  error. Any other length stays `Error::Protocol`. This is the case that
  matters most: a client upgraded ahead of a node.
- **Old client, new node.** `Handshake::feed` already refuses a challenge
  that is not exactly 33 bytes, so the client fails with
  `Error::Protocol("expected an auth challenge, got a 35-byte frame …")`
  before sending anything. A hand-rolled client that does not check the
  length signs the wrong bytes and sees `AuthFailed`. The `CHANGELOG`
  says to upgrade clients with the nodes.
- **Future versions.** A client refuses any version other than its own.
  Negotiating a range is deferred until there are two versions worth
  supporting at once.
- **The version names a released protocol.** It changes once per release
  that changes the client wire, not once per commit: everything in this
  plan and the degraded-ack terminator is protocol 1.
- The signature still covers the nonce alone. The version is not
  authenticated, as no frame the node sends in the handshake is: it
  selects a parser, it grants nothing.

### 2. The request frame: a new tag, then the ID

`[len: u32 LE][TAG_REQUEST = 0x0B][request_id: u64 LE][body]`.

- **A new tag rather than reusing `TAG_APP`.** A request from protocol 0
  that somehow got past the handshake would otherwise have its first eight
  body bytes read as an ID. With a new tag it is a protocol violation, and
  the node closes the connection, logged at `debug!`. `TAG_APP` stays as
  the tag of application *response* frames, which carry no ID
  (decision 4).
- **`RequestId(pub u64)`**, a newtype in `melin-wire-protocol`'s
  `control.rs` beside `ConnectionId`, re-exported by `melin-client`. The
  slots carry it too (decision 5), so it cannot be swapped with the
  adjacent `connection_id: u64` without a compile error. `u64`: no
  wraparound question on a connection that lives for months at millions of
  requests a second.
- **The node treats it as opaque.** It is echoed, never checked for
  uniqueness or monotonicity: the node keeps no per-request state. `0` is
  a valid ID on the wire. Uniqueness is the client's business, and
  `melin-client` allocates IDs itself (decision 8).
- **Sizes.** `REQUEST_HEADER_LEN` goes from 5 to 13. The ID comes out of
  the body: `MAX_FRAME_SIZE` stays 1024 and `MAX_REQUEST_BODY` goes from
  1023 to 1015 (settled, see "Settled in review"). `request_payload_len`
  stays the one place the limit is decided.
- **One encoder and one parser.** In `framing.rs`: `seal_request` and
  `frame_request` take the ID; the bytes after the length prefix
  (`[TAG_REQUEST][id]`) come from one function that both they and
  `Connection::send` use (`Connection::send` keeps writing through
  `write_frame_parts`, which spares it a copy of the body, as its comment
  says). `split_request(payload) -> Result<(RequestId, &[u8]), NotARequest>`
  is the one parser, used by `process_client_frames`, the shm-proxy trace
  and the Exchange Core codec.
- **Anything after the handshake that is not `TAG_REQUEST`, or is too short
  for the ID, closes the connection.** Today such frames, and empty ones,
  are dropped silently. With every request answered, silence would be the
  one exception left, and a client speaking another protocol is better
  told at once.

### 3. Terminal frames carry the ID; every request gets exactly one

| Frame | Tag | Payload after the tag | Meaning |
|---|---|---|---|
| `BatchEnd` | `0x02` | `[id: u64]` | The batch for `id` is complete. If `id` was applied, it is durable under the ack policy in force; a halt rejection or a filtered request also ends in `BatchEnd`, with nothing applied (decision 6). |
| `BatchEndDegraded` | `0x0A` (reserved, see "Relationship to other plans") | `[id: u64]` | As above, released primary-only. |
| `ServerBusy` | `0x04` | `[id: u64]` | `id` was shed at ingress: not applied, safe to resend. The connection stays open. |
| `Refused` (new) | `0x0C` | `[id: u64][kind: u8][len: u8][reason: len bytes, UTF-8]` | The decoder refused `id`: not applied. |

- **`RefusalKind`**, a `#[repr(u8)]` enum in `melin-wire-protocol`:
  `PermissionDenied = 1`, `Malformed = 2` (`Decoded::DecodeError`). `0` is
  no kind, so a zeroed byte is an error, not a refusal. Decoded through an
  exhaustive `match`; any other byte is `Error::Protocol` in the client.
- **The reason** is the decoder's `&'static str`, truncated to 255 bytes on
  a character boundary by the encoder, the one place that truncates. The
  widest `Refused` payload is therefore 266 bytes counting the tag (265
  after it, as the table counts; 270 with the length prefix). The docs say the text
  is for people, may change with any application release, and is shown to
  the authenticated client whose request it was (see "Settled in review",
  2).
- **`Decoded::Filter` is answered with an empty `BatchEnd { id }`**
  (settled, 1). An empty batch already means "nothing to say", so no new
  frame, and the `Decoded` API is unchanged; its docs change from "drop
  silently" to "answered with an empty batch".
- **`EngineError` is removed.** It is never produced, the two clients
  already disagree on whether it is terminal, and it would need an ID to
  be meaningful. The protocol breaks now anyway; keeping a frame nothing
  sends would just leave another arm in every client.
  `TransportResponse::EngineError`, `TAG_ENGINE_ERROR`,
  `OutputPayload::EngineError`, its encoding in `response_frame.rs`, and
  the client's `Frame`, `Reply` and `Error` variants go with it. Tag `0x03`
  is retired, not reused: it goes into a `RETIRED_TAGS` list that the
  existing `const` block in `control_codec.rs` checks no live tag matches.
- **The compile-time tag checks are extended to cover every tag.** Today
  the `const` block in `control_codec.rs` checks only that `TAG_APP` is
  neither `0x00` nor in `0x10..`, and that no `CONTROL_TAGS` entry equals
  `TAG_APP`; two control tags set to the same value would compile.
  `TAG_REQUEST`, `TAG_REFUSED` and `TAG_BATCH_END_DEGRADED` join
  `CONTROL_TAGS` (`TAG_ENGINE_ERROR` leaves it for `RETIRED_TAGS`), and the
  block gains, over `CONTROL_TAGS` and `RETIRED_TAGS` together: every
  entry `!= 0x00` and `< 0x10`, every pair distinct, and `TAG_APP` in
  neither list (one `const fn` with nested `while` loops, as `const`
  allows, so step 2 can unit-test the rule on bad inputs). A duplicated tag is then a build failure, not
  a mispaired frame. The existing runtime test that every encoded variant's
  tag is in `CONTROL_TAGS` stays.
- **Halt refusals stay application reports**, built by `build_reject` and
  ended by `BatchEnd { id }`, never `BatchEndDegraded`, as today.
- **Encoding cost.** The response stages pre-encode `BatchEnd` today as a
  constant (`batch_end_wire_frame`); it becomes a 13-byte template whose ID
  the stage writes per request: one more 8-byte store on the hot path.
- **The terminator's width is one constant.** Both stages size their
  pre-encoding buffer by hand today, at 8 bytes: `response.rs` with a
  literal `[0u8; 8]`, `dpdk_response.rs` with `MAX_BATCH_END_FRAME = 8`,
  which also sizes every DPDK transmit slot
  (`MAX_TX_FRAME = MAX_APP_FRAME + MAX_BATCH_END_FRAME`, a response and
  the terminator that closes it in one `TxFrame`). A 13-byte `BatchEnd`
  outgrows both: the stage-start `expect("BatchEnd encodes")` panics, and
  on DPDK a maximum response plus its terminator overruns the slot, which
  `push_frame` guards only with a `debug_assert!`. So
  `melin-wire-protocol` gains `BATCH_END_FRAME_LEN`
  (`PREFIX_LEN + TAG_LEN + 8`, the frame of either batch terminator) beside
  the encoder, and one function there that writes a terminal's ID into such
  a template, so the ID's offset is not restated in each stage. Both
  stages' templates are `[u8; BATCH_END_FRAME_LEN]`;
  `MAX_BATCH_END_FRAME` is replaced by it, so `MAX_TX_FRAME`, and with it
  the transmit SPSC's footprint, grows by five bytes per slot. The
  `debug_assert!` in `push_frame` becomes an `assert!`: a slot overrun is a
  bug that must not reach a release build as a slice-index panic with no
  context, and the check is one compare per frame. The other early replies
  (`ServerBusy`, `Refused`, at most the widest `Refused` payload plus the
  length prefix) carry no response
  ahead of them, so they fit a slot with room to spare.

### 4. Response frames stay as they are; a reply batch is contiguous

`Response` frames (`TAG_APP`, application bytes) carry no ID. A client
attributes them to the next terminal frame on the connection. That holds
because of an invariant the node enforces structurally: **no frame other
than a heartbeat ever falls between a request's first `Response` frame and
its terminal frame.** Replies to published requests are already contiguous
except for the busy frame noted above, and decision 6 makes early replies
respect the boundary by type.

Rejected alternative: put the ID on every frame. That costs eight bytes per
response on the hot path and every client parse, just to cover an invariant
the response stage can enforce with a type.

The client checks the invariant: only `BatchEnd` and `BatchEndDegraded`
(once it exists, see "Relationship to other plans") close a batch that
holds `Response` frames. A `ServerBusy` or `Refused`
arriving while `Response` frames have come since the last terminal is
`Error::Protocol`, loudly, not a silent mispairing. The cost of the choice
is that a client learns which request a `Response` belongs to only at the
terminal; `melin-bench` timestamps the first `Response` and attributes it
there (decision 9).

### 5. The ID travels in the slots, outside the journal

`InputSlot` and `OutputSlot` gain `request_id: RequestId`, beside
`connection_id`. `melin-transport-core` takes a dependency on
`melin-wire-protocol`, which has none of its own.

- **Excluded from the journal and replication by construction.** The
  journal encoder (`melin_journal::codec::encode`, called through the
  journal stage's `encode_event`) takes `sequence`, `timestamp_ns`,
  `key_hash` and the event as arguments, and replication ships the
  journal's own bytes (`last_user_entry_replication_slice`), so neither
  can see the ID. A test pins it: two slots differing only in
  `request_id` journal to identical bytes.
- **Replicas and node events** set it to `RequestId(0)`, as they do
  `connection_id` (`InputSlot::node_event`, the replication receiver).
- **One constructor for client slots.** Both readers' slot write moves to
  an `InputSlot` constructor that builds the whole slot with a struct
  literal, so a field added later is a compile error at the one place a
  client slot is made rather than a stale value from the slot's previous
  occupant. Check in the step's PR that the write still compiles to an
  in-place store (no extra copy of the event); if it does not, keep the
  in-place closure but have it destructure the slot exhaustively.
- **The matching stage copies it** into every output slot of the event,
  `drain_remaining`'s included. Every `OutputSlot` there is a struct
  literal already, so the compiler finds each site.
- **Footprint.** `InputSlot` is `repr(align(64))`. Its fixed header grows
  from 32 to 40 bytes, which adds a cache line for an application whose
  event lands within eight bytes of a line boundary. The PR reports
  `size_of::<InputSlot<_>>()` and `size_of::<OutputSlot<_, _>>()` before
  and after for echo, notary and counter (and the Exchange Core in its own
  migration). The echo and notary `footprint.rs` budget tests fail if a
  ring outgrows its budget; if one does, the fix is in that application's
  event layout, not here.
- Rejected: overlaying the ID on `InputSlot::sequence`, which is zero on a
  primary's client input exactly when the ID is meaningful. It would save
  the eight bytes, but it reworks how the journal stage adopts or
  allocates sequences, the core of replication's correctness, inside a
  protocol change. Revisit only if the footprint numbers demand it.
- `OutputSlot` is visible to event publishers, which see the ID as they
  see `connection_id`. The new field is a breaking change for code that
  builds one, noted in the `CHANGELOG`.

### 6. Early replies: one queue, released at batch boundaries, unordered

`RefusalQueue` becomes the **early-reply queue**: an SPSC of
`EarlyReply<R> { connection_id: u64, request_id: RequestId, kind:
EarlyKind<R> }` with
`EarlyKind<R> = Rejected(R) | Filtered | Refused { kind: RefusalKind,
reason: &'static str } | Busy`, from the reader (or the DPDK poll thread)
to the response stage. Every answer the reader decides on goes through it:
halt refusals, filtered messages, decoder refusals, and shed requests.
`ControlEvent::PipelineBusy` is deleted (step 2), so no busy notice rides
the unbounded control channel any more. `Rejected` encodes as the
application's report then `BatchEnd { id }`, `Filtered` as `BatchEnd { id }`
alone, `Refused` and `Busy` as their one frame.

- **No input-sequence ordering** (step 3). With IDs an early reply no
  longer has to wait for earlier replies, so `Refusal::input_seq`,
  `idle_bound`, the bounded `release` and `refusal_channel`'s
  `matching_progress` argument go, and with them the reader's rule that
  refusals are flushed before each commit. A refusal no longer waits
  behind a write the ack policy holds.
- **The reader's flush point.** The queue's sender only stages an entry
  (`try_send`); `flush` publishes it. Today the only flushes are the two
  `refusals.flush(); batch.commit();` pairs in `client_frames.rs`. Those
  pairs go, and `process_client_frames` instead flushes the early-reply
  sender once on every return path: `Continue`, `Disconnect`, `Park`, and
  a return in the middle of a shed tail, so no staged reply is left
  unpublished whatever the outcome. The position of that flush relative to
  the input batch's commit no longer matters: an early reply's only
  ordering constraint is that its connection's `Connected` reaches the
  stage first, which the `Registered` order below secures at the drain,
  not at the flush. Step 3's tests pin it: after each outcome of
  `process_client_frames`, the queue's receiver sees every reply the call
  staged without a further flush.
- **The batch boundary is a type.** Each response stage keeps a
  `BatchBoundary` tracker, told about every slot it handles (whether or
  not the slot's frames went anywhere: a reply to a closed connection
  still ends its event), which records whether the last one was
  `is_last_in_request`. Draining the queue takes an `AtBoundary<'_>` token
  that only the tracker hands out, and only between events, so a drain
  mid-event does not compile. The token borrows the tracker, so no slot
  can be handled while a drain holds it.
- **The registration order is a type too.** An early reply must never
  reach the stage before its connection's `Connected`: the stage syncs the
  queue's cursor, then drains the control channel, then releases, as
  today. One function does the first two in that order and returns a
  `Registered<'_>` token that the drain also requires, so the order is
  kept in one place for both stages and both call sites below.
- **Drain points:** the idle path; before each event's first slot (one
  queue sync per iteration decides for the batch, as today); and during a
  gate wait. `admit`'s `before_wait: impl FnOnce()` becomes a small trait
  with a `before_wait` and a `while_waiting` hook, the second called on
  each idle turn of the gate's waiter. The stage enters `admit` holding
  the tracker's state for that slot: if it is at a boundary, the hook
  syncs and drains the control channel (the `Registered` function), drains
  the queue, and flushes (io_uring) or pushes to the transmit SPSC
  (DPDK); if the gate is blocking inside an event (see "What the code
  shows"), the hook drains nothing. The DPDK response stage gets the same
  tracker, tokens and hook, which live in shared code
  (`durability_gate.rs` and a new module beside `halt.rs`), not in each
  stage.
- **Capacity.** `EARLY_REPLY_QUEUE_CAPACITY` keeps today's
  `REFUSAL_QUEUE_CAPACITY` value. It now bounds early replies in flight
  rather than refusals held behind the gate, so it fills only if the
  response stage stops draining, not on a held write. A full queue parks
  the connection (decision 7); it no longer sheds.
- **Send-buffer overflow, and flushing inside a drain.** A drain can
  release many replies to one connection: decision 7 sheds every complete
  frame buffered behind a shed request, up to a full parse buffer of
  minimal requests (thousands of 13-byte `ServerBusy` frames), and a
  burst of malformed requests draws back `Refused` frames with up to 266
  bytes of payload each. Today's io_uring drain appends every released reply to the
  connection's `send_buf` (`release` into `append_refusal` into
  `append_frames`) and `FlushCadence::on_append` only latches a flush as
  due for after the drain, so one such drain would cross `MAX_SEND_BUF`
  and drop a well-behaved pipelining client that decision 7 means to keep.
  The io_uring drain therefore applies the consumed path's cadence between
  released replies: after each one it calls `on_append`, and when
  `is_due()` it runs `flush_sends` and `on_flush` before the next, exactly
  as the event loop does between slots. Flushing there is allowed: the
  drain only runs holding `AtBoundary`, between events. The cadence check
  and flush are one helper shared by the slot loop and the drain, not a
  second copy. With that, `MAX_SEND_BUF` keeps today's meaning, a cap on
  a peer that is not reading: an early reply that would take a connection
  past it drops the connection, as `append_refusal` does now, and only a
  client that does not read its replies reaches it. The DPDK stage pushes
  each reply to the transmit SPSC as its own frame and has no
  `send_buf`, so it needs no change here.

### 7. Shedding and parking: no request is left unanswered

`process_client_frames` returns `Continue`, `Disconnect` or the new
`Park` (`PipelineFull` is gone since step 2), so both readers' exhaustive
matches must decide what `Park` means. Its per-connection input becomes a small
`ClientIngress { parse_buf, shed_remaining }` struct that both readers
hold, so the bookkeeping below is written once.

- **Input ring full at request `k`: shed the buffered tail** (settled, 4).
  `k` and every complete frame already in `parse_buf` behind it are
  answered `Busy`, read only as far as their ID (`split_request`), never
  decoded. A frame in the tail that is not a request still closes the
  connection. A trailing partial frame stays for the next receive. This
  fixes the lost-signals "never parsed" entry: no tail waits for bytes that
  never come.
- **The shed tail survives a park.** `shed_remaining` counts the bytes of
  the tail not yet answered, adjusted when `parse_buf` is compacted. A
  park in the middle of the tail resumes shedding there, so "nothing
  buffered behind `k` is applied" holds even when the early-reply queue
  fills halfway through.
- **Early-reply queue full.** Processing stops *before* the frame that has
  nowhere to go, leaving it and everything after it in `parse_buf`, and
  returns `Park`. Nothing is dropped and nothing is answered out of turn.
  Parking is the safety net under shedding, not the normal response to
  load: it happens only while the response stage is not draining.
- **Parking on io_uring** (`reader.rs`). A connection's receive state
  becomes one enum (armed, idle between a multishot's end and its re-arm,
  parked with or without a cancel in flight, dying), replacing the
  `multishot_active` and `dying` flags, so a parked connection cannot be
  re-armed or freed by the paths written for the others. On `Park` the
  reader cancels the connection's multishot receive (`AsyncCancel` on its
  slab index, as `begin_teardown` does); data CQEs that complete before
  the cancel are appended to `parse_buf` as usual, and the terminal CQE of
  a parked connection does not re-arm. With nothing reading the socket,
  its receive buffer fills and TCP's window closes on the client. While
  any connection is parked, the reader's wait is bounded by a
  `PARK_RETRY` timeout SQE, independent of ticks, since nothing wakes it
  when the queue frees up. The reader's eventfd (`wake_reader`, which a
  registration, shutdown and the ack-policy publisher already use) could
  serve instead: the response stage would write it after a drain while the
  parks gauge is non-zero. That is not done, for three reasons. Avoiding a
  lost wake needs a fenced park/drain handshake (the reader bumps the
  gauge and then re-checks the queue, the stage drains and then loads the
  gauge, both `SeqCst`), which the timer makes unnecessary; the wake would
  add a second, more delicate path to the same retry. It puts an
  eventfd `write` on the response stage, inside the drain that runs
  between events, to shorten a path that is degraded by definition: a
  park happens only while the stage is not draining. And the DPDK side
  needs nothing of the kind, since its poll loop retries a parked
  connection itself, every `PARK_RETRY_INTERVAL` iterations, so the
  wake would be io_uring's alone. The
  cost of the timer is a resume delay of at most `PARK_RETRY` after the
  stage resumes, on a connection the client is already being pushed back
  on. Each `PARK_RETRY` expiry, or any other wake of the reader, retries
  every parked connection's `parse_buf` and re-arms the receive of one that comes back
  `Continue`. The idle-connection scan skips parked connections: the client
  is not idle, the node is not reading it.
- **Parking on DPDK** (`dpdk_transport.rs`). A parked connection is not
  read from: the poll loop skips
  its `recv_into_vec` and retries its `parse_buf` on one iteration in
  `PARK_RETRY_INTERVAL`.
  smoltcp keeps unread bytes in the socket's receive buffer and advertises
  only its free space, so the client is pushed back the same way; a netns
  test pins it (step 5). The parked branch comes **before** the
  `MAX_PARSE_BUF` check at the top of the connection's turn, not merely
  before `recv_into_vec`: one receive can take `parse_buf` past 64 KiB and
  the connection can then park on a full queue, and that buffer is the
  node's backlog, not client abuse, so it must not be dropped for it. The
  idle check skips parked connections, as on io_uring.
- **Counted and logged.** Shed requests, decoder refusals by kind and
  filtered requests are counted once per call, as the halt's refusals are
  (`HaltGate::record_refused`); parked connections are a gauge with a
  counter of parks beside it (step 6). Each shed, refusal and park is a
  `debug!` with the connection ID (client-caused or load-caused, per the
  log-level convention). A connection parked for longer than a second is
  degraded operation, not load: the response stage has stopped draining.
  The reader logs it at `warn!`, at most once per interval while any
  connection stays parked, naming how many are.

### 8. `melin-client`

- **IDs are the connection's.** Every connection allocates `RequestId`s
  from 1 upward (so a zeroed buffer is never mistaken for its first
  request). `send`/`send_with` return the ID on every surface: the
  blocking `Connection`, `PolledConnection`, `StreamConnection` and the
  `Session` trait. The I/O-free `seal_request` and `frame_request` take
  one from the caller.
- **`Frame` and `Reply`:** `Response(&[u8])`, `BatchEnd { id }` (with
  `ack` once degraded acks land), `ServerBusy { id }`,
  `Refused { id, kind, reason: &str }`, with `EngineError` gone; `Reply`
  keeps `Heartbeat`. `classify` checks each terminal's payload length
  exactly, and a `Refused` reason that is not UTF-8 or overruns the frame
  is `Error::Protocol`.
- **One outstanding-request tracker, shared by both connections.** An
  `Outstanding` type in the crate replaces `PolledConnection::in_flight`
  and is added to the blocking `Connection`, so both apply the same rules
  from one implementation: a `VecDeque<RequestId>` in send order, sorted
  because IDs are allocated in increasing order. **Every terminal is
  looked up by its ID**, whatever its frame: since step 3 a `BatchEnd`
  can itself be early (a `Filtered` request's empty batch, or a halt
  `Rejected` report and its `BatchEnd`, both overtaking earlier requests
  held by the gate), and the client cannot tell an early `BatchEnd` from
  an in-order one. The front is checked first, the fast path for an
  in-order reply (O(1)); otherwise the ID is found by binary search and
  removed. A terminal is never assumed to settle the front. A terminal for
  an ID not outstanding is `Error::Protocol`, and so is the batch rule of
  decision 4. The
  `PolledConnection` deadline runs while the tracker is non-empty, as it
  runs on `in_flight > 0` today.
- **Convenience calls** (`request`, `request_one`) assume nothing else is
  in flight on the connection, and check that the terminal names the ID
  they sent: a terminal for another ID is `Error::Protocol` rather than a
  silently stolen reply. `Connection::request`'s loop is folded into the
  existing `collect_batch`, so the batch is collected in one place.
- **Errors.** `Error::Refused { kind, reason: String }` is new.
  `Error::ServerBusy` now means "resend" and leaves the connection usable:
  its text drops "on a new connection", and `Frame::ServerBusy`'s doc drops
  "the connection should be dropped". `Error::NoReply`'s text drops its
  explanation of silent drops and goes back to meaning the node stopped
  answering. The `Connection` docs' "after any error, drop it" names
  `ServerBusy` and `Refused` as the two answers that leave it usable.
  `Error::ProtocolVersion` is decision 1's.
- **Docs.** The crate's "Silence" section is rewritten: a node answers
  every request, and silence means the node or the network.
  `DEFAULT_TIMEOUT`'s and `Connection`'s docs drop the "silently dropped
  request" rationale.

### 9. Consumers

Every in-repo consumer moves in the same commit as the wire change
(step 2), so the build stays green.

- **`melin-bench`** (`load.rs`): key each request's send timestamp and the
  application sequence it checks (`app.check`) by ID instead of deriving
  them from the FIFO. Latency is still measured at the first `Response`,
  as now, and attributed at the terminal. Count `ServerBusy` as shed,
  reported in the summary, instead of failing the run: load shedding is
  behaviour the benchmark should measure. A `Refused` still fails the
  run: the bench sends only well-formed requests.
- **`melin-shm-proxy` trace:** pair requests and terminals by ID, and
  count `BatchEndDegraded`, `ServerBusy` and `Refused` as answers; today it
  recognises `BatchEnd` only. (`BatchEndDegraded` is counted from whichever
  plan adds the frame, see "Relationship to other plans".)
  - *The scanner.* `FrameScanner` today hands its callback only the tag
    (`on_frame(tag: Option<u8>)`), and frames arrive split across the
    ring's `first`/`second` slices and across feeds. It becomes a scanner
    that keeps each frame's head, the first nine payload bytes (the tag
    and an ID: `[u8; 9]` plus a count, the widest head either direction
    needs, so no allocation), across piece and feed boundaries, and hands
    the callback the head it got (shorter for a frame shorter than nine
    bytes). Requests are read through `split_request` on that head;
    terminals through `terminal_request_id(payload) -> Option<RequestId>`,
    a new function in `melin-wire-protocol`'s `control_codec.rs` beside
    the encoder, which `melin-client`'s `classify` also uses for the ID,
    so the "ID follows the tag" layout of a terminal is decoded in one
    place (it reads only the tag and the ID, so a head is enough).
  - *The stamps.* The IDs come from the Java rig, so the proxy cannot
    assume they increase and `Outstanding`'s sorted deque does not apply.
    They become a `HashMap<RequestId, u64>` with `IN_FLIGHT_CAP` reserved
    up front (O(1) lookup by an arbitrary ID, no allocation in steady
    state; a diagnostic path, not the node's). At the cap the new
    request's stamp is not recorded rather than an old one evicted
    (eviction would need a second, ordered structure), and the trace's
    report counts requests left untracked and terminals for an ID it holds
    no stamp for, beside today's "never answered" line. A repeated ID
    overwrites its stamp and is counted the same way.
  - Its unit tests' frame builders and `tests/dpdk_bridge.rs` follow. The
    proxy's client keeps its own framing, so the **Aeron benchmark
    harness's Java rig** (out of repository) must frame `TAG_REQUEST` with
    an ID and read the new terminals; the proxy's contract with that
    harness changes, and the `CHANGELOG` says so.
- **Examples.**
  - The tests that pin silent drops flip to asserting a `Refused` of the
    right kind: `a_reader_key_cannot_echo` (echo),
    `an_auditor_key_can_audit_but_not_notarize`,
    `a_malformed_leaf_is_refused_without_dropping_the_connection` (notary),
    and `the_client_explains_a_silently_dropped_request` (notary), which
    becomes a test that the client reports the refusal's reason.
  - `a_frame_that_is_not_an_application_frame_is_dropped` (echo) flips to
    asserting the close, one non-request frame per connection.
  - `a_payload_that_looks_like_a_protocol_frame_is_echoed` (echo) names
    `TAG_ENGINE_ERROR` as a body byte; it uses the retired value from
    `RETIRED_TAGS`, and gains `TAG_REQUEST` and `TAG_REFUSED`.
  - The echo client (`client.rs`) and its test `Session` stub follow the
    new `Frame` and `send`.
- **Test harnesses.** `tests/replicated_failover.rs` has its own framing
  and handshake (`answer_challenge`, `send_request`,
  `read_until_batch_end`); it moves onto `melin-client` rather than
  gaining a copy of the new header. The hand-written challenge parsers in
  `server.rs`'s and `admin.rs`'s unit tests read the version and the
  nonce at its new offset, through a test helper beside the encoder
  rather than a third copy of the layout. `raft_failover.rs` (tag check)
  and `dpdk_veth` (through `Handshake`) need no change.
- **Operator tools on the admin listener** authenticate with the same
  challenge, so any that parse it themselves break with the client
  protocol; tools built on `melin-client` (`melin-test-node`'s
  `admin_command`) follow automatically.
- **Exchange Core** (separate repository, after melin lands; the items
  below come from a survey of it and are to be confirmed there):
  - `codec::decode_request` and `app_frame_body` stop parsing the tag
    themselves and use `split_request`.
  - The bench's `next_wire_frame` uses `frame_request`; its existing
    equality test keeps it honest.
  - The OE gateway's `pending_positions` FIFO becomes a map keyed by
    request ID, and it consumes the empty `BatchEnd` now sent for each of
    its filtered heartbeats and subscription messages.
  - Its codec test that asserts `4 + 1 + MAX_REQUEST_BODY` as the widest
    frame uses `REQUEST_HEADER_LEN + MAX_REQUEST_BODY`.
  - `docs/wire-protocol.md` gets the new frame table.
  - Its application-level `request_seq` stays: that is its idempotency
    key, and it is unrelated to this ID.
  - The survey suggests the Exchange Core is still on the pre-`Ack` API
    and may not build against melin main. Confirm that first and catch it
    up separately, so the protocol change is not mixed with an unrelated
    migration.

### 10. The ordering contract, as documented

> Replies to requests the node accepted arrive in the order the requests
> were sent. A request the node did not accept (shed as busy, refused by
> the application's decoder, filtered, refused while the node is halted)
> may be answered before requests sent ahead of it, but never in the
> middle of another request's reply. Every request gets exactly one final
> frame naming it, for as long as the connection is open.
>
> A busy request was not applied, but requests sent after it may have
> been. A client whose requests depend on each other's order (a cancel
> after the order it cancels) stops on a busy, waits for the replies it
> is owed, and resends from the busy request on, in order.

This replaces "A refused request gets no answer" and the silent-refusal
error in `building-an-application.md`; the halt refusal's "goes out after
the replies to the client's earlier requests" in `replication.md` ("Writes
halt when all replicas disconnect"); and "keeping replies in request
order" in `pipeline-architecture.md`. `building-an-application.md` also
tells application authors that a decoder's refusal reason now reaches the
client.

## Steps

Each step builds, passes the suite, adds its own `CHANGELOG.md` lines, and
is committed and reviewed on its own. Steps 1 to 6 are committed on one
feature branch, merged to `main` as a unit once step 6 is reviewed, as
recent work has been (`fix/journaled-ack-policy`): `main`, and so any
release cut from it, never carries the interim behaviours noted in steps
2 and 4. Each step
lists what it leaves untested and why; coverage of the changed crates is
checked with `cargo llvm-cov` before the step is proposed.

### Step 1. Slots carry `request_id`; wire unchanged

`RequestId` in `melin-wire-protocol`; `melin-transport-core` depends on
it. `InputSlot` and `OutputSlot` gain the field, always `RequestId(0)` for
now. The client-slot constructor replaces both readers' field-by-field
write. The matching stage copies the ID into every output slot, in the run
loop and in `drain_remaining`.

Tests:
- `melin-journal`/`transport-core`: two input slots differing only in
  `request_id` journal to identical bytes (and so replicate identically).
- `pipeline_tests.rs`: every output slot of an event carries its input
  slot's ID: no report, several reports, a query, an ack-policy change,
  and the shutdown drain.
- The readers' existing `process_frames` tests run against the
  constructor unchanged.
- PR: the slot sizes before and after (decision 5).

### Step 2. The wire break

Decisions 1 to 5, 8 and 9:
- `CLIENT_PROTOCOL_VERSION` and the versioned challenge;
- `TAG_REQUEST`, `RequestId` in the framing functions, `split_request`,
  the new sizes;
- the ID-carrying terminals, `Refused` and `RefusalKind`,
  `terminal_request_id`, `RETIRED_TAGS` and the extended tag checks, with
  `EngineError` removed end to end;
- `TAG_BATCH_END_DEGRADED` reserved as "Relationship to other plans"
  describes: the constant in `CONTROL_TAGS` and nothing more if this plan
  lands first, or the ID added to the existing frame if degraded acks
  landed first;
- `BATCH_END_FRAME_LEN` and the template writer in `melin-wire-protocol`,
  replacing `response.rs`'s literal buffer and `dpdk_response.rs`'s
  `MAX_BATCH_END_FRAME` (so `MAX_TX_FRAME` grows), and `push_frame`'s
  bound check made an `assert!` (decision 3);
- `process_client_frames` reads the ID and closes on non-request, empty
  and too-short frames;
- the response stages echo the ID;
- `melin-client`, the bench, shm-proxy, the examples and the test
  harnesses move over.

Ordering is unchanged in this step: the refusal queue becomes the
early-reply queue with all four kinds, **still released by input
sequence**, so replies stay in request order and positional pairing would
still work. That includes `Busy`: `ControlEvent::PipelineBusy` is deleted
here rather than in step 3, because a busy frame on the control channel can
land inside another request's batch, which the new client reports as
`Error::Protocol`. `response.rs`'s pre-encoded `server_busy_wire_frame`
(a literal `[0u8; 8]` buffer, which a 13-byte `ServerBusy { id }` would
outgrow, panicking at stage start) goes with `PipelineBusy`, its only user:
a `Busy` is encoded per reply, with its request's ID, through the
early-reply path like the other kinds, so no busy template is kept or
resized. `FrameAction::PipelineFull` goes: a full ring now queues
a `Busy`, stops parsing, and returns `Continue` (keeping the
connection), so DPDK, which shares `process_client_frames`, stops
closing the connection on a full ring in this step too. Interim,
until step 4: a full early-reply queue is `Disconnect` on both transports
(loud and safe; today it sends an out-of-band busy), and the frames behind
a request shed on a full ring stay in `parse_buf`. On io_uring that is
today's behaviour. On DPDK it is new (today DPDK closes the connection on
a full ring): the poll loop parses only after a receive returns bytes, so
the frames behind wait for the client's next bytes, as on io_uring.
Released by input sequence, a `Busy` for a request shed on a full ring
leaves only once every event then in the ring has been answered, so the
busy signal is late, not prompt; step 2's tests must not expect prompt
release, and step 3's release at batch boundaries makes it prompt.
Acceptable for an interim that never ships; step 4 sheds the tail.

Tests:
- `wire-protocol`: the header constants and the `MAX_REQUEST_BODY`
  arithmetic; `frame_request`/`seal_request` write tag and ID; the
  existing round-trip proptest carries random IDs; `split_request` on a
  request, a short frame, an empty frame and every other tag; each
  terminal encodes with its ID and decodes back; a `Refused` reason at,
  past and well past 255 bytes, ending in a multi-byte character, is
  truncated on a character boundary; the versioned challenge. The
  extended `const` block (decision 3) is written as a `const fn` over a
  slice of tags returning which rule fails, if any, which the block
  asserts on; a unit test calls the same function on a duplicated tag, a
  `0x00`, a `0x10` and a valid set, so the check itself is tested, not
  only today's tags.
- `melin-client`:
  - the handshake against a fake node on protocol 0 (a 33-byte challenge)
    and on an unknown version is `Error::ProtocolVersion` with both
    numbers, before anything is signed; a 35-byte challenge on version 1
    succeeds;
  - `classify` on each terminal at its exact length, one byte short and
    one long, a `Refused` with an unknown kind, a zero kind, invalid UTF-8,
    and a length past the frame;
  - `Outstanding`, unit and proptest: random interleavings of sends,
    in-order `BatchEnd`s and out-of-order early replies, `BatchEnd`s
    among them, settle each ID once and never the front in another's
    place; an unknown or repeated ID is `Error::Protocol`;
  - a `ServerBusy` or `Refused` after a `Response` with no terminal
    between is `Error::Protocol`; a heartbeat there is not;
  - the convenience calls on every surface refuse a terminal for another
    ID, and return `Error::Refused` and `Error::ServerBusy` with the
    connection still usable for the next request;
  - IDs start at 1 and increase on every surface.
- `process_client_frames` (unit, in `reader.rs`'s test module, where the
  current `process_frames` tests live): each decoder outcome yields its
  early reply with the frame's ID; a non-request tag, an empty frame and a
  request too short for its ID are `Disconnect`, with the frames before
  them committed; a full ring queues `Busy` for the frame that met it;
  a full early-reply queue is `Disconnect`.
  `a_full_refusal_queue_is_pipeline_busy` and
  `process_frames_partial_commit_then_server_busy_when_pipeline_full`
  are rewritten for the new outcomes; the "PipelineBusy is emitted on
  Full, not on oversize" assertions go with `PipelineBusy`.
- `melin-shm-proxy` trace (unit, ungated): the scanner reports the same
  heads whatever the frames' split across pieces and feeds (a new proptest
  over random splits, beside today's hand-picked split tests), including
  a head cut inside the ID and a frame shorter than nine bytes;
  out-of-order terminals pair with their own request's stamp; every
  terminal kind counts as an answer; the cap leaves the new stamp
  unrecorded and counts it; an unknown or repeated ID is counted, not
  paired.
- `terminal_request_id`: each terminal's ID, `None` for a non-terminal
  tag and a head too short for the ID; `classify` reads its IDs through
  it. If this plan lands first, `None` for `TAG_BATCH_END_DEGRADED` too,
  and `classify` on a `0x0A` frame is `Error::Protocol` (the degraded-acks
  plan flips both tests when it adds the frame).
- The terminator template: `BatchEnd { id }` encoded through
  `encode_transport_response` is exactly `BATCH_END_FRAME_LEN` bytes, and
  the template writer's output equals it for random IDs (proptest), so the
  pre-encoded path and the encoder cannot drift. In `dpdk_response.rs`,
  `the_encoded_terminator_fits_its_reserved_room` encodes `BatchEnd { id }`
  into a `[u8; BATCH_END_FRAME_LEN]`, and a new `push_frame` test pushes a
  `MAX_APP_FRAME` response with a full terminator trailer and checks the
  frame's bytes, the overrun this change would otherwise cause. Both
  compile only under the `dpdk` feature, so they run through the netns
  runner like the module's other cases; the wire-protocol tests above are
  the ungated half.
- Response stage (unit): `refusal_order`'s tests keep passing with the
  generalised queue, plus one per new kind (a `Busy`, a `Refused`, a
  `Filtered` each released in order with the right frames and ID);
  `registration_race` unchanged.
- Integration (`server-runtime/tests`, through `melin-test-node`): S1, a
  pipelined client with a denied and a malformed request among
  well-formed ones gets every reply paired; a filtered request gets an
  empty batch; a halted node's refusal carries its ID
  (`halt_refusal.rs`).
- DPDK, under the netns runner: the integration tests above run on DPDK
  through `melin-test-node` unchanged; `dpdk_veth`'s handshake tests now
  exercise the versioned challenge from the DPDK poll thread; and the
  echo and counter `round_trip.rs` DPDK-client cases and the shm-proxy's
  `dpdk_bridge.rs` round-trip requests with their IDs over the client
  NIC.

Left untested: the interim close on a full queue gets a unit test only;
it is replaced in step 4.

### Step 3. Early replies, unordered

Decision 6: input-sequence ordering removed, the `BatchBoundary` tracker
and `AtBoundary` token, the `Registered` token, the gate-wait hook, both
stages; the reader's flush on every return path in place of the
flush-before-commit pairs; the io_uring drain's flush cadence between
released replies. Update the degraded-acks plan's decision 4 text.

Tests:
- The shared module (unit, ungated): the tracker hands out `AtBoundary`
  only after a slot marked last, across every mix of slots; the
  `Registered` function syncs before it drains the control channel.
- Response stage (unit, both stages; `dpdk_response.rs` compiles only
  under the `dpdk` feature, so its cases run through the netns runner):
  - an early reply for a connection whose event straddles two reads lands
    after that event's terminal, never between its frames;
  - an early reply is released while the gate holds an earlier write of
    the same connection (finding 18's shape), and of another connection;
  - the gate blocking on a continuation slot (cached position dropped by
    `idle_recheck`) drains nothing until the event ends;
  - the registration race stays covered, now including a `Connected`
    first seen inside a gate wait;
  - a proptest interleaving events (several slots each, random straddling
    reads), early replies and gate holds across connections checks that,
    per connection, every ID gets exactly one terminal and `Response`
    frames are contiguous before their terminal.
- Response stage, io_uring (unit, over a socket pair with a reading
  peer): one drain releasing more early replies to one connection than
  `MAX_SEND_BUF` holds (`Busy` and widest-reason `Refused` alike) flushes
  as it goes and keeps the connection, every reply reaching the peer in
  queue order; the same drain to a peer that does not read drops the
  connection at the cap, as today.
- `process_client_frames` (unit): after each return (`Continue`,
  `Disconnect`, and from step 4 `Park` and a return mid shed tail), the
  queue's receiver sees every reply that call staged without a further
  flush.
- `durability_gate.rs`: the hook's `before_wait` still runs once and only
  when the slot waits (`before_wait_runs_only_when_the_slot_waits`), and
  `while_waiting` runs during a wait and never on a slot that does not
  wait.
- `refusal_order`'s `a_refusal_waits_for_earlier_replies_and_precedes_later_ones`
  is replaced by its opposite: the refusal goes out while the earlier
  reply is held.
- Finding 18 at the client (`melin-client` against its fake node, on the
  blocking and the polled connection): each kind of early terminal for W2
  arriving ahead of W1's reply is paired with W2 and leaves W1
  outstanding, then W1's own batch settles W1: a `ServerBusy`, a
  `Refused`, an empty `BatchEnd { W2 }` (filtered) and a `Response` frame
  followed by `BatchEnd { W2 }` (a halt rejection). The node side is the
  response-stage test above, and the end-to-end shed is step 4's.
- Integration (`halt_refusal.rs`): a primary under a replica-backed
  policy with the hand-written replica of `tests/common/raw_replica.rs`,
  which is told not to ack; a write W1 is held by the gate; the replica
  disconnects, halting the node; a write W2 on the same connection is
  refused, and its refusal arrives while W1 is still held.

### Step 4. Shed the tail, park on overflow (shared, then io_uring)

Decision 7 in the shared `process_client_frames` (`ClientIngress`, the
shed tail, `FrameAction::Park`) and in the kernel reader (the
receive-state enum, cancel on park, the `PARK_RETRY` timeout, the idle
scan skipping parked connections). Interim, until step 5: the DPDK poll
loop, whose exhaustive match must now handle `Park`, closes the connection
on it, as it does on a full queue since step 2.

Tests:
- `process_client_frames` (unit): a full ring sheds the frame and every
  complete frame behind it, each with its ID, and leaves a trailing
  partial frame in place; a full early-reply queue parks before the frame
  and leaves the tail intact; a park inside a shed tail resumes shedding
  and publishes nothing from the tail; a `Park` and a return mid shed
  tail leave no staged early reply unpublished (step 3's flush test,
  extended to the new outcomes); a proptest over random frame
  streams and random ring and queue capacities checks that every complete
  request frame is published, answered early, or still buffered, exactly
  once, and that nothing behind a shed request in the same tail is
  published.
- Reader (unit, on a real io_uring over a socket pair, with the existing
  fixture's small input ring and an early-reply queue built with a small
  capacity, which the channel constructor takes for this): a
  parked connection resumes without new bytes and its pipelined writes all
  complete (the lost-signals scenario, which fails today); a parked
  connection's multishot is cancelled and re-armed on resume; bytes that
  arrive between the park and the cancel are kept; a parked connection
  is not reaped by the idle scan; a parked connection torn down by the
  response stage is freed once; a parked connection whose retry returns
  `Disconnect` (the halt gate's `Verdict::Close` reached while parked, and
  separately a non-request frame in the shed tail) is torn down once and
  freed once, both with the cancel still in flight and after its terminal
  CQE, and teardown issues no second `AsyncCancel` for a receive already
  cancelled (the receive-state enum makes `begin_teardown` take the parked
  states explicitly); a retry that comes back `Continue` before the
  cancel's terminal CQE does not arm a second multishot: the state moves
  to "resume once cancelled" and the re-arm happens on that CQE.
- A new `server-runtime/tests` integration test, through
  `melin-test-node`, so it runs on kernel TCP in every CI job and on DPDK
  under the netns runner (the shed tail is shared code, and DPDK has kept
  the connection on a full ring since step 2): a full ring answers
  `ServerBusy { id }` and keeps the connection, and the requests behind it
  in the same burst are shed. The ring is filled from one client by a
  test application whose `apply` waits on a latch the test holds, then
  releases; every request is answered once, applied or busy, and the
  connection serves the next request. The client reads its replies while
  it sends (`melin-client`'s polled connection, driving both in one loop),
  so an unread socket buffer never trips `MAX_SEND_BUF` or the
  blocked-send timeout and the test measures shedding, not a slow reader.
  The connection is built with `PolledConnection::with_timeout` and a read
  timeout long enough to cover filling the ring: its no-reply deadline
  runs while any request is owed a reply, heartbeats do not push it back,
  and nothing is answered until the ring is full and the first
  `ServerBusy` arrives, so the default timeout could fail the test on a
  slow CI host for a reason unrelated to shedding;
  the shed burst then also exercises the drain's in-drain flush end to
  end. The cost is accepted: the input
  ring is the fixed `INPUT_RING_CAPACITY` (`1 << 20`, no knob), so the
  test sends more than that many minimal requests, all journaled, before
  the first shed, on kernel TCP and again on DPDK. That is tens of
  megabytes on loopback or the veth bridge, seconds rather than minutes.
  A test-only ring-size seam is rejected: the constant is read by the
  `/metrics` capacity gauge and the examples' footprint budgets, so a seam
  would be a new knob on a published API carried for one test, while the
  shed logic's edges (small rings, random capacities) are already covered
  by the unit tests and proptest above on the small fixture ring.

Left untested: the DPDK interim close on `Park`, a one-line arm replaced
in step 5.

### Step 5. DPDK parking

Parking in the poll loop (decision 7), replacing step 4's interim close.

Tests:
- Parking cannot be driven through a running node on demand: the response
  stage drains the queue at every boundary. It is tested inside
  `server-runtime` under the `dpdk` feature (run by the netns runner), on
  the poll loop with a `cfg(test)` seam that holds the DPDK response
  stage's drain, as `pause_after_control_drain` holds the kernel stage's:
  the parked connection is not read, its client's sends stall once the
  node's receive buffer is full, and every write completes once the seam
  lets go; a parked connection is not dropped by the idle check; a
  connection that parks holding at least `MAX_PARSE_BUF` (64 KiB) of
  unparsed requests is not dropped by that check and completes every
  write once the seam lets go.

The shared `ClientIngress` logic is already unit-tested from step 4 on
every host.

### Step 6. Metrics and docs

- Metrics on `/metrics`, beside `melin_writes_refused_total`:
  `melin_requests_shed_total`, `melin_requests_refused_total` labelled by
  `kind`, `melin_requests_filtered_total`, `melin_connections_parked`
  (gauge) and `melin_connection_parks_total`. `melin_writes_refused_total`'s
  HELP text drops "or ServerBusy when the refusal queue is full". Tests in
  `health.rs`, as for the existing counters.
- Operator docs: the replacements listed in decision 10, the metrics in
  `replication.md`'s metrics list, and `building-an-application.md`'s
  client-errors list and its "Clients with their own I/O loop" paragraph.
  `pipeline-architecture.md` also has two field tables that go stale: the
  `InputSlot` table (beside `connection_id: u64`) and the `OutputSlot`
  table gain a `request_id` row ("the client's ID for the request, echoed
  on its terminal frame; never journaled", step 1), and the `OutputSlot`
  `payload` row drops `EngineError` (step 2). Steps 1 and 2 update their
  rows in their own commits; step 6 rereads the file as a whole.
- Code docs: `melin-app`'s `Decoded`; the `halt.rs` module docs ("Delivered
  in order" goes, "Counted" changes); `client_frames.rs`'s `FrameAction`;
  `TransportResponse::ServerBusy`'s stale "accept queue is full" wording;
  `melin-client`'s crate docs (decision 8).
- Internal: close the findings listed at the top, and add or update the
  roadmap entry to point here.
- `CHANGELOG.md`: the entries the steps added, read together and made one
  account. Under **Changed**, "client protocol 1", wire-breaking (upgrade
  clients with the nodes, admin tools and the shm-proxy's client
  included), with the API breaks listed: `send` returns `RequestId`; the
  framing functions take an ID; the `Frame`/`Reply` variants;
  `REQUEST_HEADER_LEN`, and `MAX_REQUEST_BODY` (a body an application does
  not check at build time fails at run time between 1016 and 1023 bytes);
  the `InputSlot`/`OutputSlot` fields; refused and filtered requests are
  answered, and a decoder's reason reaches the client. Also under
  **Changed**, the other breaks to published crates' public API:
  - `melin-wire-protocol`: `TransportResponse::BatchEnd` and
    `ServerBusy` carry a `RequestId`, and `TransportResponse` gains a
    `Refused` variant (exhaustive, so a new arm in every match) (step 2).
  - `melin-transport-core`: `OutputPayload` loses `EngineError` (step 2).
  - `melin-client`: `Error::Refused` and `Error::ProtocolVersion` are new
    variants of an exhaustive `Error`, so a consumer's match breaks; they
    are listed here, not under **Added** (step 2).
  - `melin-server-runtime`: `ControlEvent::PipelineBusy` is removed from
    the crate-root `ControlEvent` (step 2); in the `halt` module,
    `Refusal`, `RefusalSender` and `RefusalQueue` become the early-reply
    types (`EarlyReply`, `EarlyKind` and the queue's sender and receiver),
    and `refusal_channel(matching_progress)` becomes the early-reply
    channel constructor (renamed in step 2), which loses
    `matching_progress` in step 3, as `RefusalQueue::idle_bound` and the
    bounded release go, and takes a capacity in step 4; `response::Response`'s pub `refusals`
    field is renamed with its type. Out-of-crate users such as
    `tests/response_flush_before_gate.rs` and
    `tests/response_flush_within_batch.rs` are migrated in those steps.
    The entry names each renamed or removed item (`Refusal`,
    `RefusalSender`, `RefusalQueue`, `refusal_channel`, `idle_bound`,
    `release`'s bound, `Response::refusals`) and its replacement; the 0.17.0 entry that
    introduced the halt gate and refusal queue named only the functions
    that took them, so there is no earlier entry to point back to.
  **Removed** for `EngineError` (the frame, `TAG_ENGINE_ERROR`, and the
  client's variants); **Added** for `CLIENT_PROTOCOL_VERSION`,
  `RequestId`, the `Refused` frame, `RefusalKind`, `split_request`,
  `terminal_request_id`, `RETIRED_TAGS` (public, since the echo test
  reads it), `TAG_BATCH_END_DEGRADED` if this plan lands first (reserved,
  no frame yet) and the metrics; **Fixed** for the findings
  closed.

### Step 7. Exchange Core migration

In its own repository, per decision 9.

## Not part of this plan

- Idempotency or retry by `melin-client`: the client reports a busy or a
  refusal, and the caller decides.
- Node-enforced resend order after a busy (sticky shedding until the
  client says it has caught up): see "Settled in review", 4.
- The halt refusal as a protocol frame (API review F1).
- Negotiating a range of protocol versions.

## Settled in review

The first draft left four questions open. Each is settled here with the
option the review recommended; the alternatives are kept so the choice can
be revisited with its reasons in view.

### 1. What answers a `Decoded::Filter`: an empty `BatchEnd { id }`

Every request frame gets exactly one terminal, with no exception, so the
client's outstanding set and deadline need no special case, the `Decoded`
API is unchanged, and the "every request answered exactly once" proptests
hold. The cost is one 13-byte frame and one queue entry per filtered
message, on a path that costs nothing today, and every client must consume
those replies (the OE gateway's heartbeats).

Rejected: a separate one-way frame tag that is never answered. It saves the
reply traffic but makes the runtime police every combination of frame kind
and decode outcome (a one-way frame that decodes to `Permitted`, or to a
refusal, has no ID to answer with), which needs either a `RequestDecoder`
API change or a close on any one-way frame that is not filtered, plus a
second send path in every client. Rejected: keeping `Filter` silent, which
brings back the silence this plan removes and leaks an outstanding ID until
`NoReply` fires on a healthy connection.

### 2. What a `Refused` carries: the kind and the reason text

The client's error names the cause ("auditor role may not notarize"), which
replaces today's `NoReply` text guessing at a role or version problem, and
the operator no longer needs node debug logs to find out why. It costs
nothing on the success path; the strings already exist. Accepted costs:
wording becomes observable, so the docs say it is for people and may
change; a malformed request of a few bytes can draw back a frame of up to
266 bytes, bounded per connection by `MAX_SEND_BUF` like any slow reader;
and the application's validation wording reaches the authenticated client
whose request it was.

Rejected: the kind only, which leaves the rule in the node's logs.
Rejected for now: an application-defined `u16` code, machine-readable but
a breaking change to `Decoded` for every application; it can be added
beside the text later.

### 3. Where the ID's eight bytes come from: out of the body

`MAX_FRAME_SIZE` stays 1024 and `MAX_REQUEST_BODY` goes to 1015. The node
enforces one limit, and every buffer sized by `MAX_FRAME_SIZE` (the
readers' parse buffers, `melin-client`'s, hand-rolled clients' such as the
OE gateway's) is unchanged. Applications lose eight bytes of body; those
that assert against `MAX_REQUEST_BODY` at compile time find out at build
time (echo does, and the Exchange Core reportedly does), and neither is
near the limit. An application that does not assert sees
`RequestTooLarge` at run time for a body between 1016 and 1023 bytes,
which the `CHANGELOG` calls out.

Rejected: growing `MAX_FRAME_SIZE` to 1032, which grows every frame buffer
in the node and every client, and breaks a hand-rolled client with a
hardcoded 1024 only on maximum-size requests, where it is hardest to
notice.

### 4. The frames behind a request shed on a full ring: shed the buffered tail

Once request `k` is shed, later requests on the same connection may still
be applied, under any option short of closing the connection. A client
that resends `k` has it applied after them, so a client whose requests
depend on each other's order must stop, wait for its outstanding replies
and resend from `k` in order (decision 10). `melin-client` cannot do that
for it, because it does not know which requests depend on each other.

Shedding every complete frame already buffered behind `k` answers every
buffered request at once, the immediate signal a latency-critical client
wants, reads only IDs, and guarantees that nothing in that burst behind `k`
is applied (decision 7 keeps that across a park). A request arriving on a
later receive can still be applied ahead of the resend, and the node can
over-shed when the ring has room again a moment later.

Rejected: shedding only `k` and parking the rest, which makes the ordering
hazard the common case (`k+1…` are applied ahead of the resend of `k`
almost every time) and leaves the parked requests with no signal and
unbounded latency. Rejected: closing the connection after the busy frames,
which rules out the hazard but costs a reconnect and an Ed25519 handshake
per shed on an overloaded node, and gives up keeping the connection
through backpressure. Deferred: sticky shedding until the client sends a
`Resume` frame, which rules the hazard out with the connection kept, at the
cost of a new frame, a per-connection flag, and a client that must send
`Resume` or be shed forever. It is the follow-up if an application needs
the node to enforce resend order, and would be a protocol version of its
own.
