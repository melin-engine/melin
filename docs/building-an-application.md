# Building an Application on Melin

Melin is a replicated sequencer. It puts every request into one total order, journals it, replicates it, applies it to your application, and answers the client only once the copies your acknowledgement policy asks for exist. What it does not know is what a request *means*. That part is yours: a state machine, the events that drive it, and the bytes clients use to talk to it.

This guide is for the engineer writing that part. It walks through a complete application — the counter in [`crates/examples/counter`](../crates/examples/counter) — then sets out the rules an application has to keep, most of which the compiler cannot check for you. Every Rust block in this guide is compiled and run as a test of the counter example, so what you read here builds against the current API.

Contents:

1. [Quickstart](#quickstart)
2. [What you write, what Melin does](#what-you-write-what-melin-does)
3. [Walkthrough: the counter](#walkthrough-the-counter)
4. [The determinism rules](#the-determinism-rules)
5. [Designing events](#designing-events)
6. [Requests and responses](#requests-and-responses)
7. [Retries and duplicates](#retries-and-duplicates)
8. [Snapshots and upgrades](#snapshots-and-upgrades)
9. [The client side](#the-client-side)
10. [Testing your application](#testing-your-application)
11. [Before production](#before-production)

## Quickstart

Before writing anything, watch a node work. The echo example is the smallest application there is: it hands every request back unchanged, after the sequencer has ordered and journaled it. Its client sends requests one at a time and reports how long each round trip took.

```sh
# 1. A client key, and a node that accepts it:
openssl genpkey -algorithm ed25519 -out /tmp/melin-key.pem
PUB=$(openssl pkey -in /tmp/melin-key.pem -pubout -outform DER | tail -c 32 | base64)
echo "trader $PUB me" > /tmp/authorized_keys

# 2. A standalone node, set up for a development machine:
RUST_LOG=info cargo run --release --bin echo-server -- --standalone --ack-policy disk --authorized-keys /tmp/authorized_keys --journal /tmp/echo.journal --cores none --no-mlock

# 3. In another terminal, a thousand requests:
cargo run --release --bin echo-client -- --key /tmp/melin-key.pem --count 1000
```

`--cores none` leaves the node's threads unpinned and yielding when idle, and `--no-mlock` skips locking its memory, which needs a privilege a development machine rarely grants; the node warns that nothing is pinned, which is expected here. A production node is laid out differently — see [Pipeline Architecture](pipeline-architecture.md#waiting).

Every one of those requests was given a sequence number, written to `/tmp/echo.journal` and synced to disk before its reply left the node. Stop the node and start it again with the same command, and it replays that journal on the way up.

## What you write, what Melin does

A request's life, with your code in the middle of it:

```text
client ──> reader ──> your decoder ──> sequenced ──┬──> journaled (and replicated)
                                                   └──> your application ──> your encoder ──> reply,
                                                                                once the journal has it
```

- The **reader** accepts the connection, authenticates the client's key and reads the request's framing.
- **Your decoder** turns the request's bytes into one of your events, or refuses it.
- The sequencer gives the event its place in the total order. From here on it is **journaled**, replicated to any replica, and **applied** — to the node's copy of your application now, and to every other copy of it, on every node, forever after: on replicas, on restart, when a snapshot is restored.
- **Your application** applies the event and produces reports.
- **Your encoder** turns each report into response bytes, and the node sends them once the event holds the copies the acknowledgement policy demands.

What you implement:

| Piece | Trait | What it is |
|-------|-------|------------|
| Event | `AppEvent` | The events your state machine takes, and how each is encoded in the journal |
| State machine | `Application` | Your state, how an event changes it, what it reports, and how it snapshots and restores itself |
| Decoder | `RequestDecoder` | A client's request bytes to an event |
| Encoder | `ResponseEncoder` | A report to response bytes |
| Binary | — | A `main` that hands those four to the runtime |

Everything else — transport, framing, authentication, ordering, journaling, replication, failover, snapshots, recovery, CPU pinning — is the runtime's.

## Walkthrough: the counter

The counter keeps one number. Clients add to it and read it back. It is small, but it has everything a real application has: an event that changes state, a query that does not, a reply, and state that must survive a restart.

The blocks below are the counter's own code, in the order you would write it. Each imports what the previous step defined from the example crate.

### The event

An event is a plain `Copy` value. The runtime keeps events inline in its rings, so there is no heap allocation per event, and it journals them through the encoding you give it.

```rust
use melin_app::{AppEvent, CodecError};

const KIND_INCREMENT: u8 = 0x10;
const KIND_GET_VALUE: u8 = 0x11;

#[derive(Debug, Clone, Copy)]
enum CounterEvent {
    /// Add `amount` to the counter. Changes state, so it is journaled.
    Increment { amount: u64 },
    /// Read the counter. A query: never journaled.
    GetValue,
}

impl AppEvent for CounterEvent {
    // The widest event, `Increment`: its kind (1 byte) and amount (8).
    const MAX_ENCODED_SIZE: usize = 9;

    fn encoded_size(&self) -> usize {
        match self {
            CounterEvent::Increment { .. } => 9,
            CounterEvent::GetValue => 1,
        }
    }

    fn encode(&self, buf: &mut [u8]) -> usize {
        match *self {
            CounterEvent::Increment { amount } => {
                buf[0] = KIND_INCREMENT;
                buf[1..9].copy_from_slice(&amount.to_le_bytes());
                9
            }
            CounterEvent::GetValue => {
                buf[0] = KIND_GET_VALUE;
                1
            }
        }
    }

    fn decode(buf: &[u8]) -> Result<Self, CodecError> {
        match buf.split_first() {
            Some((&KIND_INCREMENT, amount)) => {
                let amount = amount.first_chunk::<8>().ok_or(CodecError::Truncated)?;
                Ok(CounterEvent::Increment { amount: u64::from_le_bytes(*amount) })
            }
            Some((&KIND_GET_VALUE, _)) => Ok(CounterEvent::GetValue),
            Some((&kind, _)) => Err(CodecError::UnknownTag(kind)),
            None => Err(CodecError::Truncated),
        }
    }

    fn is_query(&self) -> bool {
        matches!(self, CounterEvent::GetValue)
    }
}

let mut buf = [0u8; CounterEvent::MAX_ENCODED_SIZE];
let len = CounterEvent::Increment { amount: 42 }.encode(&mut buf);
assert!(matches!(CounterEvent::decode(&buf[..len]), Ok(CounterEvent::Increment { amount: 42 })));
```

Three things to get right here:

- **`MAX_ENCODED_SIZE` is a bound, `encoded_size` is exact.** The journal reserves the bound for every entry and sizes its batches from it; each entry then takes only its exact size on disk. A bound too small is refused when an event exceeds it; a bound past what the journal can carry fails the build (on `cargo build` and `cargo test` — not on `cargo check`). `encode` must return exactly `encoded_size`: an event whose two figures disagree is refused before it is journaled, since the entry would otherwise hold a truncated event.
- **The encoding is permanent.** Every event you journal is decoded again by every future version of your application that replays it. See [Snapshots and upgrades](#snapshots-and-upgrades).
- **A query is an event too.** `is_query` sends it to `Application::query` instead of `apply`, and keeps it out of the journal.

### The state machine

```rust
use std::io::{self, Read, Write};

use counter_server::{CounterEvent, CounterQuery, CounterReport};
use melin_app::{Application, ApplyCtx, QueryCtx, RejectReason};

/// A single number, zero before the first event.
#[derive(Default)]
struct Counter {
    value: u64,
}

impl Application for Counter {
    type Event = CounterEvent;
    type Report = CounterReport;
    type QueryResponse = CounterQuery;
    // What the node's operator tells the application to reserve memory for.
    // A single number needs nothing.
    type Sizing = ();

    fn apply(&mut self, event: CounterEvent, _ctx: &ApplyCtx, out: &mut Vec<CounterReport>) {
        match event {
            CounterEvent::Increment { amount } => {
                self.value = self.value.wrapping_add(amount);
                out.push(CounterReport::Ack { new_value: self.value });
            }
            // A query never reaches `apply`.
            CounterEvent::GetValue => {}
        }
    }

    fn query(&self, event: CounterEvent, _ctx: &QueryCtx) -> Option<CounterQuery> {
        match event {
            CounterEvent::GetValue => Some(CounterQuery { value: self.value }),
            CounterEvent::Increment { .. } => None,
        }
    }

    // Nothing in the counter depends on time.
    fn tick(&mut self, _now_ns: u64, _out: &mut Vec<CounterReport>) {}

    fn build_reject(_event: &CounterEvent, _reason: RejectReason) -> CounterReport {
        CounterReport::Rejected
    }

    fn snapshot<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_all(&self.value.to_le_bytes())
    }

    fn restore<R: Read>(r: &mut R) -> io::Result<Self> {
        let mut value = [0u8; 8];
        r.read_exact(&mut value)?;
        Ok(Counter { value: u64::from_le_bytes(value) })
    }

    const APP_VERSION: u16 = 1;
}

let mut counter = Counter::default();
let mut reports = Vec::new();
let ctx = ApplyCtx { now_ns: 1, key_hash: 7 };
counter.apply(CounterEvent::Increment { amount: 5 }, &ctx, &mut reports);
assert!(matches!(reports[..], [CounterReport::Ack { new_value: 5 }]));
```

- **`Default` is the state before the first event**, on every node. A fresh node, a replica catching up from the start and a restart with no snapshot all begin there.
- **`apply` is the only way state changes.** What it pushes into `out` is the reply the client receives, in order.
- **`query` reads state and cannot change it.** It takes `&self`: a query is never journaled, so a change it made would happen on one node and nowhere else.
- **`tick` is time passing.** The runtime calls it as time advances, so an application can expire, time out or schedule things. The counter ignores it.
- **`build_reject` answers a request the runtime refused on its own.** Today that happens in one case: a node that has lost its last replica refuses writes rather than acknowledge them without the copies the policy demands. The rejection is built from the event alone, because the event never reached `apply`.
- **`snapshot` and `restore` must round-trip exactly**, and `APP_VERSION` names the layout `snapshot` writes. `restore` must read every byte `snapshot` wrote: a node refuses a snapshot whose `restore` leaves bytes unread.

### Decoding requests

A client's request is a frame whose body belongs entirely to you. The runtime reads the framing and hands your decoder the body; your first byte is yours, and so is every byte after it. The counter uses the first byte as a message kind:

```rust
use counter_server::{CounterEvent, KIND_GET_VALUE, KIND_INCREMENT};
use melin_app::auth::Permission;
use melin_app::decoder::{Decoded, RequestDecoder};

struct Decoder;

impl RequestDecoder for Decoder {
    type Event = CounterEvent;

    fn decode(&self, body: &[u8], _permission: Permission) -> Decoded<CounterEvent> {
        let Some((&kind, fields)) = body.split_first() else {
            return Decoded::DecodeError("empty request");
        };
        match kind {
            KIND_INCREMENT => match fields.first_chunk::<8>() {
                Some(amount) => Decoded::Permitted(CounterEvent::Increment {
                    amount: u64::from_le_bytes(*amount),
                }),
                None => Decoded::DecodeError("increment too short"),
            },
            KIND_GET_VALUE => Decoded::Permitted(CounterEvent::GetValue),
            _ => Decoded::DecodeError("unknown kind"),
        }
    }
}

assert!(matches!(
    Decoder.decode(&[KIND_GET_VALUE], Permission::Trader),
    Decoded::Permitted(CounterEvent::GetValue)
));
```

The decoder runs before the event is sequenced. It is the last point at which a request can be turned away without leaving a trace in the journal — see [Designing events](#designing-events).

### Encoding responses

```rust
use counter_server::{CounterQuery, CounterReport, KIND_RESP_ACK, KIND_RESP_REJECTED, KIND_RESP_VALUE};
use melin_app::encoder::ResponseEncoder;

struct Encoder;

/// A response body: its kind, then the value.
fn value_body(buf: &mut [u8], kind: u8, value: u64) -> Result<usize, &'static str> {
    let body = buf.first_chunk_mut::<9>().ok_or("buffer too small")?;
    body[0] = kind;
    body[1..].copy_from_slice(&value.to_le_bytes());
    Ok(9)
}

impl ResponseEncoder for Encoder {
    type Report = CounterReport;
    type Query = CounterQuery;

    fn encode_report(&self, report: &CounterReport, buf: &mut [u8]) -> Result<usize, &'static str> {
        match *report {
            CounterReport::Ack { new_value } => value_body(buf, KIND_RESP_ACK, new_value),
            CounterReport::Rejected => {
                *buf.first_mut().ok_or("buffer too small")? = KIND_RESP_REJECTED;
                Ok(1)
            }
        }
    }

    fn encode_query(&self, query: &CounterQuery, buf: &mut [u8]) -> Result<usize, &'static str> {
        value_body(buf, KIND_RESP_VALUE, query.value)
    }
}

let mut buf = [0u8; 16];
let len = Encoder.encode_query(&CounterQuery { value: 3 }, &mut buf).expect("fits");
assert_eq!(buf[..len], [KIND_RESP_VALUE, 3, 0, 0, 0, 0, 0, 0, 0]);
```

The encoder writes the body into the buffer it is given and returns its length; the runtime frames it. An encoder error is logged and that response is dropped — it is a bug in your application, not something a client caused.

### The binary

Your binary hands the four pieces to the runtime. Configuration comes from the command line the runtime defines — listening address, journal path, acknowledgement policy, replication, CPU layout — so `--help` on your binary documents all of it.

```rust,no_run
use clap::Parser;
use counter_server::{Counter, RequestDecoder, ResponseEncoder};
use melin_server_runtime::StartupEvents;
use melin_server_runtime::server::{self, ServerConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config = ServerConfig::parse();
    server::run::<Counter>(
        config,
        StartupEvents::none(), // events journaled as the node becomes primary: none
        (),                    // the counter's `Sizing`
        RequestDecoder,
        ResponseEncoder,
        None, // no event publisher
    )
}
```

Run it the way the quickstart ran echo:

```sh
RUST_LOG=info cargo run --release --bin counter-server -- --standalone --ack-policy disk --authorized-keys /tmp/authorized_keys --journal /tmp/counter.journal --cores none --no-mlock
```

[The client side](#the-client-side) shows a program that talks to it.

## The determinism rules

Your application runs many times over the same events: on the primary, on each replica, on every restart, and in the background copy that takes snapshots. Replicas and recovery are only worth anything if every one of those runs reaches the same state. So `apply`, `tick` and `restore` must be a pure function of their inputs. None of this is checked by the compiler, and breaking it does not fail loudly — the nodes simply disagree.

- **No I/O, no clocks, no randomness, no threads.** Nothing an application reads may differ between nodes or between runs. Time comes from `ApplyCtx::now_ns` and `tick`, which carry the time the primary recorded in the journal; randomness, if you need it, comes from a seed carried in an event.
- **Never let a hash map's iteration order decide anything.** The standard `HashMap` is seeded randomly per process, and even with a fixed hasher the order depends on capacity, which a node's memory sizing changes. If what you do depends on the order you visit entries — which one fills first, which report comes first — use an ordered structure (`BTreeMap`, a `Vec` kept sorted) for that decision.
- **Prefer integers to floating point** for anything that decides. The same binary on the same architecture gives the same float results, but a replica on a different CPU or build may not; fixed-point integers never differ.
- **`apply` must never panic.** Every event is journaled whether or not your application can handle it. An event that makes `apply` panic stops the primary — and then every replica that applies it, and every restart that replays it. Validate in the decoder, use checked or saturating arithmetic, and handle every case in `apply` by producing a report.
- **Your starting state is `Default`, and it depends on nothing local.** Not a flag, not the environment, not a file. Whatever the operator configures reaches the application as events, below.
- **Operator configuration is journaled.** Reference data a deployment starts with, and limits an operator sets, go in `StartupEvents`: `genesis` is journaled once, when a node creates the journal; `on_primary` is journaled every time a node becomes primary. Replicas then apply the primary's values, not their own, and replay reproduces every decision made under them. Applying a value already in force must change nothing.
- **Memory sizing is not state.** `Sizing`, passed to `Application::prefault` on every node, may reserve and pre-fault memory, and nothing else: every decision `apply` makes must come out the same whatever the sizing.
- **Time can repeat and can go backwards.** `now_ns` is wall-clock time recorded by the primary. It may arrive more than once, and it may be earlier than a time already seen — after the primary's clock steps back, or after a failover to a node whose clock is behind. Firing what is due at a given time must be idempotent, a call for a time already passed must change nothing, and elapsed-time arithmetic must saturate.

## Designing events

**Validate in the decoder.** Everything the decoder permits is sequenced and journaled, permanently, and applied on every node. A request that is malformed, too large, or from a key not allowed to make it should stop at the decoder, where refusing it costs nothing. Keep for `apply` only what depends on state — a balance, a limit, a record that must exist — and make it answer with a rejection report rather than fail.

**A rejection in `apply` is part of the history.** When `apply` refuses an event, the event is still journaled, with the rejection it produced. That is usually what an audit trail wants: the attempt and the refusal are both on record, and both replay.

**Keep events narrow.** The runtime holds each event inline in its rings, sized for your widest event, so width costs memory in every slot; and the journal sizes its batches from `MAX_ENCODED_SIZE`, so a wide bound means fewer events per disk write. A reference to data — an identifier, a digest — is cheaper to sequence than the data itself. The echo and notary examples show both ends: echo carries full payloads and pays for it, and its tests print what; notary carries a 32-byte digest.

**Check the limits at compile time.** A request body has a maximum size (`melin_server_runtime::MAX_REQUEST_BODY`), and so does a response body (`MAX_RESPONSE_BODY`). A request over the limit costs the client its connection, and a response the encoder cannot fit is dropped. Assert your widest message against both, next to its definition, so a change that breaks them does not compile:

```rust
/// The counter's widest request and response: a kind and a `u64`.
const WIDEST_MESSAGE: usize = 1 + 8;

const _: () = assert!(WIDEST_MESSAGE <= melin_server_runtime::MAX_REQUEST_BODY);
const _: () = assert!(WIDEST_MESSAGE <= melin_server_runtime::MAX_RESPONSE_BODY);
```

**Queries are cheap reads.** A query is answered from the node's state in sequence with the events around it, and never journaled. Its reply still waits until every event before it holds the copies the policy demands, so a client never reads state a failover could take back. Use `QueryCtx` for what only a query may see: the caller's key, the node's journal position and connection count.

## Requests and responses

**The request is yours from its first byte.** The runtime frames each request and reads the frame's own header; your decoder sees the body alone, and no value of it is reserved. A kind byte first, as the counter does, is one convention; an application with a single kind of request needs none.

**Permissions come from the client's key.** Each key in the node's `authorized_keys` file carries a role, and the decoder receives it with every request:

```text
# <role> <base64 public key> <comment>
trader   AAAA...  desk-1
readonly BBBB...  monitoring
```

The set of roles is fixed by the runtime today — `operator`, `trader`, `custodian`, `readonly`, `replication` — and it is your decoder that decides what each may do. `replication` authenticates replicas and `operator` the admin endpoint; an application usually refuses writes from `readonly` and `replication` keys:

```rust
use counter_server::CounterEvent;
use melin_app::AppEvent;
use melin_app::auth::Permission;
use melin_app::decoder::Decoded;

/// Permit `event` unless it changes state and the key may only read.
fn permit(permission: Permission, event: CounterEvent) -> Decoded<CounterEvent> {
    let read_only = matches!(permission, Permission::ReadOnly | Permission::Replication);
    if read_only && !event.is_query() {
        return Decoded::PermissionDenied("this key may not write");
    }
    Decoded::Permitted(event)
}

let increment = CounterEvent::Increment { amount: 1 };
assert!(matches!(permit(Permission::ReadOnly, increment), Decoded::PermissionDenied(_)));
assert!(matches!(permit(Permission::ReadOnly, CounterEvent::GetValue), Decoded::Permitted(_)));
```

**A refused request gets no answer.** When your decoder returns anything but `Permitted` — a decode error, a permission denied, a filtered message — the runtime logs it and drops the request, and keeps the connection. The client learns nothing until its read times out. That is deliberate: a node spends nothing on a client sending it garbage. When a client needs to know why a well-formed request was refused, let the decoder permit it and have `apply` answer with a rejection report.

**A reply is a batch.** Each request gets the reports `apply` pushed for it, in order, then an end-of-batch marker. A batch may hold none — the request changed state but had nothing to say — or several: an acknowledgement and the effects it caused. A query's reply is its one response, or an empty batch if `query` returned `None`.

**Beyond the client that asked**, an application can also stream its reports to subscribers through an event publisher, a consumer of its own it passes to `server::run`. Most applications need none.

## Retries and duplicates

A client that sends a request and loses its connection before the reply cannot tell whether the request was applied. If it retries, the runtime sequences the retry as a new event: whether a repeat is refused is your application's decision, because only your application knows which requests are safe to apply twice.

The usual design is a sequence number per client. The client numbers its requests; `apply` remembers, per client key, the highest it has applied, and refuses anything at or below it. `ApplyCtx::key_hash` identifies the key the request came in on, the same on every node and every replay:

```rust
use std::collections::BTreeMap;

/// The highest request sequence applied for each client key. A `BTreeMap`,
/// not a `HashMap`: it is part of the state a snapshot writes, and its order
/// must not depend on the process.
#[derive(Default)]
struct AppliedSequences {
    by_key: BTreeMap<u64, u64>,
}

impl AppliedSequences {
    /// Whether `sequence` from `key_hash` is new, recording it if so.
    fn accept(&mut self, key_hash: u64, sequence: u64) -> bool {
        let applied = self.by_key.entry(key_hash).or_insert(0);
        if sequence <= *applied {
            return false;
        }
        *applied = sequence;
        true
    }
}

let mut applied = AppliedSequences::default();
assert!(applied.accept(7, 1));
assert!(!applied.accept(7, 1), "a retry of request 1 is refused");
assert!(applied.accept(7, 2));
assert!(applied.accept(8, 1), "every key has its own sequence");
```

Three details make it work:

- **It is state.** It goes in the snapshot, like everything else `apply` depends on.
- **The refusal is a report.** Answer a repeat with the original outcome, or with a rejection the client recognizes as "already done" — not with silence.
- **The client needs its sequence back after a restart.** Either it keeps its own counter durably, or your application answers a query with the highest sequence applied for the caller's key, which `QueryCtx::key_hash` identifies.

Events the node journals on its own behalf, such as startup events, carry a `key_hash` of 0.

## Snapshots and upgrades

**Snapshots bound recovery time.** A node periodically writes a snapshot of your application from a background copy that applies the same events, so taking one never pauses the node. On restart it loads the newest snapshot and replays only the journal after it. `snapshot` writes your state and `restore` reads it back; the runtime adds the framing, the position in the journal and a checksum around it. The round trip must be exact: a restored application must make every future decision the original would have.

**`APP_VERSION` names your snapshot layout.** Bump it whenever the bytes `snapshot` writes change. A node refuses to load a snapshot written under a different `APP_VERSION`, before your `restore` sees it. It also refuses one whose `restore` returns without reading every byte of it, the usual sign that the layout changed and the version did not.

**Changing your event encoding needs care, because the journal does not record it.** Old entries are decoded by whichever version of your application replays them. Adding a new kind of event is safe for replay, since no existing entry carries it — but upgrade every node before one is written, or a node on the old version cannot decode it. Changing an existing event's layout changes what entries already in the journal mean: do it only across a snapshot boundary, so the new version never replays an entry the old one wrote. The procedures are in [Journal & Event Sourcing](journal.md#migration-procedure).

**Clone cheaply if you can.** The runtime takes its background copy through a snapshot round trip. An application with a faster way to copy itself can override `Application::clone_via_snapshot`.

## The client side

[`melin-client`](../crates/core/client) speaks the node's protocol: framing, the key handshake, the reply batch. A client of your application is that library plus your own request and response bytes. It is Apache-2.0, so it can be linked into your client binaries as it is.

```rust,no_run
use counter_server::{GET_VALUE_REQUEST, KIND_RESP_ACK, increment_request};
use melin_client::{Connection, key};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let key = key::load_signing_key("/tmp/melin-key.pem".as_ref())?;
    let mut node = Connection::connect("127.0.0.1:9876".parse()?, &key)?;

    // A write: one report in its reply, the counter's new value.
    let ack = node.request_one(&increment_request(5))?;
    assert_eq!(ack[0], KIND_RESP_ACK);
    println!("counter: {}", u64::from_le_bytes(ack[1..9].try_into()?));

    // A query, answered from the node's state.
    let value = node.request_one(&GET_VALUE_REQUEST)?;
    println!("counter: {}", u64::from_le_bytes(value[1..9].try_into()?));
    Ok(())
}
```

`request_one` expects exactly one frame in the reply; `request` returns the whole batch; `send` and `next_frame` keep several requests in flight. The errors a client should expect:

- **No reply within the read timeout** — the node refused the request silently: a role not allowed to make it, or bytes the decoder rejected.
- **Server busy** — the node is shedding load; retry later, on a new connection.
- **Disconnected** — the node closed the connection. After a failover, reconnect to the new primary. A request in flight may or may not have been applied — see [Retries and duplicates](#retries-and-duplicates).

## Testing your application

**Unit-test `apply` directly.** An application is a value with methods: build one from `Default`, apply events with a hand-made `ApplyCtx`, and check the reports and the state. No node is needed, and every rule above is testable this way.

**Test determinism.** The one property the runtime depends on is that every way of reaching a state reaches the same one. Test it the way the runtime exercises it: a live run, a snapshot taken partway and restored, and a full replay from `Default`, all of which must end in the same state:

```rust
use counter_server::{Counter, CounterEvent};
use melin_app::{Application, ApplyCtx};

/// Apply `events` in order, advancing the clock the way the runtime does:
/// before an event whose time is past the latest one handed out.
fn apply_all<A: Application>(app: &mut A, events: &[(A::Event, ApplyCtx)]) {
    let mut reports = Vec::new();
    let mut clock = 0;
    for (event, ctx) in events {
        if ctx.now_ns > clock {
            clock = ctx.now_ns;
            app.tick(clock, &mut reports);
        }
        app.apply(*event, ctx, &mut reports);
        reports.clear();
    }
}

/// An application's state, as its snapshot writes it.
fn state<A: Application>(app: &A) -> Vec<u8> {
    let mut bytes = Vec::new();
    app.snapshot(&mut bytes).expect("snapshot");
    bytes
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Mixed clients, times that repeat and go backwards: the inputs a
    // deployment produces, not the tidy ones.
    let events: Vec<(CounterEvent, ApplyCtx)> = (1..=1_000u64)
        .map(|i| {
            let ctx = ApplyCtx { now_ns: 1_000 * (i % 97), key_hash: i % 5 };
            (CounterEvent::Increment { amount: i }, ctx)
        })
        .collect();
    let (before, after) = events.split_at(400);

    let mut live = Counter::default();
    apply_all(&mut live, &events);

    let mut snapshotted = Counter::default();
    apply_all(&mut snapshotted, before);
    let mut restored = Counter::restore(&mut state(&snapshotted).as_slice())?;
    apply_all(&mut restored, after);

    let mut replayed = Counter::default();
    apply_all(&mut replayed, &events);

    assert_eq!(state(&restored), state(&live), "snapshot and restore changed the outcome");
    assert_eq!(state(&replayed), state(&live), "replay changed the outcome");
    Ok(())
}
```

When the three disagree, which pair differs says where to look: the restored run points at `snapshot` and `restore`, the replay at something in `apply` or `tick` that is not a function of its inputs. Comparing snapshot bytes works when your snapshot is written in a deterministic order; if it iterates a hash map, compare what `query` answers instead. Feed it the inputs a real deployment produces — many clients, timestamps that repeat or step backwards — and consider generating them with a property-testing library.

**Test end to end.** Start a node in the test on a free port, talk to it through `melin-client`, and check the replies. The counter's [round-trip test](../crates/examples/counter/tests/round_trip.rs) does exactly that, and is a template to copy; [echo's](../crates/examples/echo/tests/round_trip.rs) also reads the journal back from disk, and [notary's](../crates/examples/notary/tests/round_trip.rs) fails a primary over to a replica.

## Before production

- [ ] `apply`, `tick` and `restore` read nothing but their arguments and your state: no I/O, clocks, randomness or threads.
- [ ] No decision depends on a hash map's iteration order.
- [ ] `apply` cannot panic on any event the decoder permits; overflow is checked or saturating.
- [ ] Time-driven work is idempotent at a given time, does nothing for a time already passed, and saturates elapsed-time arithmetic.
- [ ] Operator configuration reaches the application as `StartupEvents`, never through `Default` or `Sizing`.
- [ ] The decoder refuses everything malformed and everything a role may not do.
- [ ] Your widest request, response and event are checked against the runtime's limits at compile time.
- [ ] Repeated requests are refused, if your requests are not safe to apply twice, and clients can recover their sequence.
- [ ] `snapshot` and `restore` round-trip exactly, and `APP_VERSION` changes whenever the snapshot layout does.
- [ ] A determinism test runs live, snapshot-and-restore and full replay over realistic inputs, and agrees.
- [ ] An upgrade plan exists for changing an event's encoding: see [Snapshots and upgrades](#snapshots-and-upgrades).

Where to go next: [Journal & Event Sourcing](journal.md) for what is on disk and how recovery works, [Replication](replication.md) for acknowledgement policies and failover, and [Pipeline Architecture](pipeline-architecture.md) for the threads and rings your application runs inside.
