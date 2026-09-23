# Application API review, September 2026

Code-level review of the application seam: the `melin-app` traits and
types (`Application`, `AppEvent`, `RequestDecoder`, `ResponseEncoder`,
`ApplyCtx`, `QueryCtx`, `Permission`, `AuthorizedKeys`), the three
examples (`echo`, `counter`, `notary`), and the runtime code that drives
them (`transport-core`'s `dispatch` and matching loop, snapshot load, the
journal and replication encoders, the reader's handling of decoder
outcomes, `melin-client`'s reply correlation). Read-only: nothing was run
or changed. Three lenses: safety, performance, and the friction an
application developer meets.

**Status.**

- Resolved: S4, S5, S9.
- Partly resolved: S6 (replication keys refused on the client listener,
  duplicate keys refused, doc example fixed; application-defined roles
  remain open); F1 (defaults for `tick` and `query`; removing
  `build_reject` remains open, with S1).
- All other findings open.

The findings that matter most are in safety. Four of them concern state
that outlives the process (journal bytes, per-key identity, snapshot
layout, codec version) and are cheaper to settle before more
deployments depend on today's behaviour.

---

## Summary

Effort: **Low** is a local change with a test, no design question;
**Medium** needs a design decision or touches several crates; **High** is
an API or on-disk format redesign. *Breaking* means an application or
client has to change.

| # | Finding | Effort | Breaking |
|---|---------|--------|----------|
| S1 | A request the decoder refuses gets no reply; pipelined clients misattribute replies | Medium | Protocol |
| S2 | `key_hash` is persistent identity derived from an unstable hash | Low (code) / Medium (migration) | Persisted values |
| S3 | No application codec version on the journal or the replication handshake | High | On-disk format |
| S4 | `encode` / `encoded_size` disagreement only caught in debug builds | Low | No |
| S5 | `restore` is not checked for unread trailing bytes | Low | No |
| S6 | Role model: fixed role list, replication keys reach the decoder, keys-file laxity | Low (parts) / Medium (app-defined roles) | Roles: yes |
| S7 | Query routing is a convention (`is_query`), not a type | High | Trait |
| S8 | No divergence detection for a nondeterministic `apply` | Medium | No (additive) |
| S9 | Stale and misleading docs (mostly fixed on main since) | Low | No |
| P1 | A large `QueryResponse` widens every output ring slot | Low (doc) / Medium (fix) | No |
| P2 | `apply` / `query` take the event by value | Low | Trait |
| P3 | `out: &mut Vec<Report>` lets `apply` allocate | Medium | Trait |
| F1 | Required methods most applications no-op (`tick`, `query`, `build_reject`) | Low | No |
| F2 | "No queries" expressed as `()` plus a runtime error | Low | No (additive) |
| F3 | Codec written twice; hand-computed byte offsets | Medium | No (additive) |
| F4 | Public structs and enums not `#[non_exhaustive]` | Low | Yes, once |
| F5 | No conformance test kit | Medium | No (additive) |
| F6 | No idempotency helper | Medium | No (additive) |
| F7 | `server::run` takes six positional arguments | Medium | No (additive) |
| F8 | The reference example teaches weak habits | Low | No |

**Tracked elsewhere.** Work that landed on main while this review was
under way overlaps three findings. The roadmap now carries S3 ("Record
the application's event-encoding version in the journal") and S6's
application-defined roles ("Application-defined client roles"); the
`ApplyCtx::now_ns` item left in S9 belongs with "Monotonic sequencer
time, derived from the journal". The runtime refactor that stripped out
the exchange fixed the rest of S9. The new application guide
(`docs/building-an-application.md`) takes a position on S1; see there.

---

## Safety and correctness

### S1. A request the decoder refuses gets no reply

**Problem.** `client_frames.rs` handles `Decoded::Filter`,
`Decoded::PermissionDenied` and `Decoded::DecodeError` by logging at
debug level and dropping the request: no frame goes back. The
protocol has no request id: `melin-client` pairs a reply with a request
by counting `BatchEnd` frames (`Client::request` reads frames until the
next `BatchEnd`). So:

- a client waiting on one request sits out its whole read timeout and
  gets `NoReply`, indistinguishable from a dead network;
- a client with several requests in flight attributes every later
  reply to the request before it, a correctness failure on the client
  side, caused by the server.

**Fix.** A protocol-level refusal frame, sent the way `ServerBusy`
already is, carrying the decoder's static reason, so every request gets
exactly one batch. `Filter` either goes (every current use is better
served by a refusal) or also answers. Needs a protocol version bump and
handling in `melin-client`. Related roadmap items: "Halt refusals
behind a stalled ack gate" and "Answer queries on a halted node" are
the same class of problem: a client learning a verdict from its own
timeout.

**Counterpoint.** The application guide calls the silence deliberate: a
node spends nothing answering a client that sends it garbage, and a
client that needs to know why a well-formed request was refused should
have the decoder permit it and `apply` reject it. That answers the cost
of a refusal, not the correlation failure: a pipelining client is
harmed by one malformed or unauthorized request among well-formed ones,
and the guide's workaround does not cover a permission denial, which
the decoder is the place to make. A refusal frame costs one small write
per refused request (on the reader thread, off the sequenced path),
and a client flooding garbage can still be disconnected. Worth settling
explicitly either way; if silence stays, the guide should warn
pipelining clients.

### S2. `key_hash` is persistent identity derived from an unstable hash

**Problem.** `ApplyCtx::key_hash` / `QueryCtx::key_hash` are
`FxHasher` over `<[u8; 32] as Hash>::hash`, computed in two places
(`server.rs` for TCP, `dpdk_transport.rs` for DPDK). The value is
journaled with every event and applications are told to key per-client
state on it (idempotency marks, rate limits). Neither link is stable:
rustc-hash makes no cross-version output promise (2.0 already changed
its algorithm), and std's `Hash` for arrays is not promised stable
across Rust releases. Replay is unaffected (the old values are in the
journal), but after a dependency or toolchain bump the same key
reconnects under a new `key_hash`, and any state keyed on the old one
is silently orphaned.

**Fix.** One function in `melin-app` with a specified derivation (the
first eight bytes of BLAKE3 over the public key; BLAKE3 is already a
workspace dependency), pinned by a test vector, called from both
transports. Changing the derivation is itself the same orphaning event
for existing deployments, so it wants a release note, and ideally a
one-time mapping or an operator-visible warning. The consolidation into
one function is independent and costs nothing.

### S3. No application codec version on the journal or the replication stream

**Problem.** `Application::APP_VERSION` is checked only when a snapshot
is loaded (`snapshot.rs`). Journal segments and the replication
handshake carry no application version. So a build whose event layout
or `apply` semantics changed replays an existing journal under the new
meaning, or fails to decode in the middle of recovery, and a replica
on a different build accepts the primary's stream without complaint.
For an audit trail it also means nothing records which application
version produced a given segment.

**Fix.** Record an application codec version (distinct from the
snapshot layout version, or `APP_VERSION` reused with its doc widened)
in the segment header and exchange it in the replication handshake.
Check a compatibility range rather than equality, so a rolling upgrade
(new code that still reads the old codec) stays possible. On-disk format
change: bump the journal format version. Now on the roadmap as "Record
the application's event-encoding version in the journal", which weighs
the options; until it lands, the operator procedure in `docs/journal.md`
("Changing the Application's Encoding") is the only guard.

### S4. `encode` / `encoded_size` disagreement is only caught in debug builds

**Problem.** The journal codec (`melin_journal::codec::encode`) checks
`written == encoded_size()` with `debug_assert_eq!` and frames the entry
from `written`. In release, an `encode` that reports fewer bytes than
the event needs journals a truncated event, acknowledges it, and ships
it. The live replication stream carries the journal entry's own bytes
(`last_user_entry_replication_slice`), so the replica holds the same
truncated entry. Nothing notices until a recovery or a replica fails to
decode it, or decodes it into something else.

(The first draft of this finding claimed the primary and replica would
hold different bytes. They cannot: `replication_wire::append_input_slot`,
which re-encodes events, is only reached from tests.)

**Fix.** Quick: make the check hard, `CorruptEntry` in the codec,
alongside the neighbouring bound checks; the journal stage then fails
before the event is persisted or acknowledged. The alternative
considered, dropping `encoded_size` so the returned length is the only
figure, removes the disagreement but also the means of detecting a
short count, so the two figures and the check between them stay.

### S5. `restore` is not checked for unread trailing bytes

**Problem.** `snapshot::load` hands `A::restore` the payload slice and
never checks that it was consumed. A snapshot layout change made without
bumping `APP_VERSION` (the mistake the version exists to catch) can
restore "successfully" into wrong state, because the old reader stops
short of the new fields.

**Fix.** In `snapshot::load`, after `restore`, fail with a new `SnapshotError` variant if the
slice is not empty. Update the `Application::restore` doc to say so.

### S6. Role model

**Problems.**

- `Permission` is a fixed list of five roles described in
  domain-specific terms (`Trader`, `Custodian`, `can_trade`,
  `can_manage_funds`, "exchange configuration"), living in the
  application-agnostic crate. An application cannot define its own
  roles; it maps its operations onto whichever names fit least badly.
- Replication keys reach the client decoder. The runtime checks
  `is_replication` only on the replication port, so each application
  must remember to refuse `Permission::Replication` on its writes. The
  counter does not check permission at all: a read-only or replication
  key can increment it.
- `AuthorizedKeys::load`'s doc example uses `admin`, which the parser
  rejects.
- A key listed twice silently takes its last role
  (`duplicate_key_last_permission_wins` pins this). For an access-control
  file that is an operator error that should fail the load.

**Fix.** Low: refuse replication-role connections on the client
listener in the runtime; fix the doc example; make a duplicate key a
parse error (tightening: no valid file changes meaning). Medium: make
the role set application-defined (an associated type parsed from the
keys file, with the runtime keeping only its own infrastructure roles:
replication and admin). The medium part is now on the roadmap as
"Application-defined client roles".

### S7. Query routing is a convention, not a type

**Problem.** One `Event` type goes through two doors chosen by
`AppEvent::is_query()`. Consequences:

- every application writes dead arms: a query arm in `apply`, a
  command arm in `query` (all three examples do);
- every application writes journal encode/decode for its query
  variants, which are never journaled;
- a write misclassified as a query goes to `query(&self)`, changes
  nothing, is not journaled, and the client gets an empty batch: a
  silently lost write.

**Fix.** Split the event: `type Command: AppEvent` (journaled, the only
type `apply` sees) and `type Query: Copy` (never encoded, the only type
`query` sees), with `Decoded::Command(C) | Decoded::Query(Q)`. The input
ring slot becomes an enum of the two; queries still travel the ring so
they are ordered with writes. `is_query` disappears. All three mistakes
become unrepresentable and every application loses code. Breaking for
every application and for `JournalEvent`'s generic parameter; the
largest item here and the one with the best long-term return.

### S8. No divergence detection for a nondeterministic `apply`

**Problem.** Determinism is a documented obligation with no check.
`apply` iterating a `HashMap` with a random seed, reading the wall
clock, or depending on allocation addresses diverges replicas silently
until something downstream disagrees.

**Fix.** An optional `fn state_digest(&self) -> Option<[u8; 32]>`
(default `None`). The shadow stage already replays the same stream as
the matching stage: compare digests at snapshot points, and between
primary and replicas at a journaled marker, and raise an operator-facing
divergence alarm. The notary's head is exactly such a digest. Also the
natural hook for S5-style and `prefault`-contract checks in F5.

### S9. Stale and misleading docs

As found, `Application::build_reject`'s doc described duplicates being
rejected on the matching thread, which the runtime no longer does; the
unused `EncodeReport` trait still carried a "Phase 3" note; and
`EventPublisherFn`'s doc named a trading binary. The refactor that
stripped the exchange out of the runtime fixed all three on main. What
remains:

- `ApplyCtx::now_ns` is not monotonic from one event to the next
  (`dispatch` documents the producer race that publishes a slot with an
  earlier timestamp than its predecessor), but the `ApplyCtx` doc does
  not warn, and the notary folds `now_ns` into receipts that a reader
  will assume are ordered in time. `Application::tick`'s doc now carries
  the warning; the fix itself belongs with the roadmap's "Monotonic
  sequencer time, derived from the journal".
- The crate-level doc of `melin-app` still lists "trading engines,
  bespoke matchers" as the applications it serves.

---

## Performance

### P1. A large `QueryResponse` widens every output ring slot

`Application::QueryResponse`'s doc says it is separate from `Report` so
a large query payload does not inflate the per-slot size. That holds
for the `reports` scratch vec only: `OutputSlot` stores
`OutputPayload<R, Q>` inline, so every output ring slot is as wide as
the larger of the two. An application that adds a wide query answer
pays for it on every write's report. Low: correct the doc and give
applications a footprint assertion helper (the examples' `footprint.rs`
tests do this by hand). Medium: route query responses through their own
ring or a side buffer.

### P2. `apply` and `query` take the event by value

Moving the event out of the ring slot copies `MAX_ENCODED_SIZE`-worth of
bytes per event for wide events (the echo payload). `&Self::Event` costs
nothing. Expected gain is small (removing the journal stage's per-event
copy measured flat), so this is worth doing only as part of the S7
breaking revision.

### P3. `out: &mut Vec<Report>` lets `apply` allocate

"Keep `apply` free of allocation" is a comment; `Vec::push` can grow the
buffer on first use and without bound under fan-out. A `ReportSink` with
a fixed capacity and a fallible push makes the rule enforceable and
bounds a single event's fan-out. Breaking; bundle with S7.

---

## Application-development friction

### F1. Required methods most applications no-op

`tick` and `query` have no default; all three examples implement `tick`
as a no-op and echo implements `query` as `None`. Give both a default.
With S1's refusal frame, the transport's own rejection
(`ReplicaDisconnected`) can become a protocol frame like `ServerBusy`,
and `build_reject`, plus the `Rejected` report variant every example
carries only for it, can go.

### F2. "No queries" is `()` plus a runtime error

Echo sets `QueryResponse = ()` and implements `encode_query` as
`Err("this application has no queries")`. An uninhabited
`pub enum NoQuery {}` in `melin-app` is `Copy`, makes `query` return
`Option<NoQuery>`, and turns `encode_query` into `match *q {}`: the
compiler proves the arm unreachable.

### F3. Codec written twice; hand-computed offsets

The counter and notary encode the same layout twice, once as
`AppEvent::decode` and once as `RequestDecoder::decode` (the counter's
two differ in strictness; see F8). A helper that decodes a request body
with the journal codec plus a permission hook removes the duplicate for
applications that choose the same layout. Separately, encoders index by
hand (`body[17..49]` in the notary); a small cursor reader/writer in
`melin-app`, or a zerocopy path for fixed-layout events, removes the
arithmetic.

### F4. Public types are not `#[non_exhaustive]`

`ApplyCtx`, `QueryCtx`, `RejectReason`, and `CodecError` are
exhaustive, and the examples' tests build the contexts as struct
literals. (`Decoded` should stay exhaustive: the runtime matches it and
must be forced to handle a new outcome.) The next field added to a context breaks every application's
tests. Mark them `#[non_exhaustive]` and add constructors (`ApplyCtx::new`,
a `QueryCtx` builder or test constructor). Breaking once, then never
again for this reason. Downstream applications with struct literals need
a one-line change each.

### F5. No conformance test kit

Each example hand-writes the same checks. A `melin_app::testing` module
(behind a feature) could run, for any application, as property tests:
codec round trip; `encoded_size` equals the bytes written and stays
within `MAX_ENCODED_SIZE`; apply a stream, snapshot midway, restore,
continue, and compare the result against an uninterrupted run (via S8's
digest, or snapshot bytes); `prefault` leaves every later decision
unchanged; restore consumes the whole snapshot. This turns the trait's
prose contracts into checks every application gets for free.

### F6. No idempotency helper

Both stateful examples defer duplicate handling to a comment
("a production app carries a per-client sequence … keyed on
`ctx.key_hash`"). Every real application needs it and will write the
same per-key high-water mark. A helper type in `melin-app` fits with the
roadmap item on request-sequence sync at connect, which needs the same
mark on the transport side.

### F7. `server::run` takes six positional arguments

Every example's `main.rs` repeats
`server::run::<A>(config, StartupEvents::none(), (), RequestDecoder, ResponseEncoder, None)`
plus identical tracing setup. An additive builder with defaults
(`Server::<A>::new(config).decoder(..).encoder(..).run()`) cuts each
binary to a few lines and gives new options a place to go without
growing the positional list.

### F8. The reference example teaches weak habits

The counter is the documented reference application, and:

- `Increment` wraps on overflow instead of refusing;
- it never exercises an application-level rejection: its `Rejected`
  report exists only for `build_reject`;
- its `AppEvent::decode` and `RequestDecoder` accept trailing bytes,
  where the notary and echo are strict, and the journal codec contract says
  the buffer is exactly one event;
- its decoder ignores permission (S6).

Use `checked_add` with an application rejection, make both decoders
exact, and gate writes on role.

---

## Suggested order

1. **Local fixes (Low):** S5, the S4 hard check, S9's crate doc, the Low parts of S6,
   F1's defaults, F2, F8, the P1 doc correction, and consolidating S2's
   derivation into one function.
2. **Persisted-state decisions (Medium–High):** S2's derivation change
   and S3's codec version (on the roadmap): cheaper before more
   journals depend on the current behaviour.
3. **S1**: decide between the refusal frame and the guide's deliberate
   silence; if the frame, coordinate with the halt roadmap items.
4. **One breaking API revision:** S7 with P2, P3, F4, and F1's removal of
   `build_reject`, so applications migrate once.
5. **Additive tooling:** S8, F5, F6, F7, F3.
