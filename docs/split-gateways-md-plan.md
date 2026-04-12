# Split Gateways + Market-Data Subsystem + TUI

## Context

Melin needs a TUI trading client that shows an order book, price candles, active orders, and balances, and lets a trader place/cancel orders. Order entry is already solved — `crates/tui/` connects via `crates/client/` and places orders today, and `crates/fix-gateway/` translates FIX 4.2 `NewOrderSingle`/`OrderCancelRequest`/`OrderCancelReplace` to melin requests. Everything else is missing:

- No protocol message for "give me the current order book"
- No protocol message for "give me active orders for this account"
- No protocol message for "give me balances for this account"
- No persistent trade history or candles
- No market-data subscription in the FIX gateway (`MarketDataRequest (V)` rejected by the catch-all at `crates/fix-gateway/src/session.rs:640`)
- No cross-session shared state in the gateway — each FIX session has its own melin TCP connection, no book mirror, no firehose subscription

The architecturally correct place for book mirrors, position queries, and market-data fan-out is **not** the matching engine (whose hot path must stay at ~100 ns/order for the 10M orders/sec target) but a gateway layer that consumes the existing `event_publisher` firehose (`crates/server/src/event_publisher.rs`) and serves queries from cache. FIX 4.4 covers every feature we need as standard application messages (`V`/`W`/`X`/`Y`/`H`/`AF`/`AN`/`AP`/`x`/`y`), so no custom protocol work is needed on the client-facing side.

**Scope decisions already made with the user**:
- **Full design, executed in slices with review gates between phases.**
- **Upgrade the FIX gateway to FIX 4.4** (not FIX 4.2 + custom messages).
- **Split the gateway into two binaries from day one**: an `oe-gateway` (order entry, renamed from the current `fix-gateway`) and a new `md-gateway` (market data), with a shared `gateway-core` library for common session/protocol plumbing. Matches standard exchange operational topology (failure isolation, independent scaling, different auth boundaries) and produces two saleable gateway products rather than one.

## Crate layout

```
crates/
├── gateway-core/     (NEW — shared library)
│   ├── src/
│   │   ├── fix/parse.rs, fix/serialize.rs, fix/tags.rs   (moved from fix-gateway)
│   │   ├── session/core.rs   (Logon, Heartbeat, TestRequest, ResendRequest,
│   │   │                      Logout, SequenceReset, seq numbers, replay buffer)
│   │   ├── auth.rs           (Ed25519 challenge-response helpers)
│   │   └── dispatch.rs       (MessageDispatcher trait; role-specific handlers
│   │                          are injected by each gateway binary)
│
├── market-data/      (NEW — shared library)
│   ├── src/
│   │   ├── mirror.rs         (BookMirror, Level, apply_report)
│   │   ├── index.rs          (OrderIndex: HashMap<OrderId, RestingOrder>)
│   │   ├── trade_ring.rs     (TradeRing per symbol, ArrayDeque<Trade, 4096>)
│   │   ├── core.rs           (MarketDataCore consumer loop + fan-out)
│   │   ├── subscriber.rs     (SessionSlotId, MdCommand, MdOutput)
│   │   └── cold_start.rs     (client-side SubscribeWithSnapshot parsing)
│
├── oe-gateway/       (RENAMED from fix-gateway — binary)
│   ├── src/
│   │   ├── main.rs
│   │   ├── event_loop.rs     (io_uring — OE-shaped: per-session melin TCP)
│   │   ├── session.rs        (OeSession { core: SessionCore, id_map,
│   │   │                       order_symbols, pending_cancels, order_ledger,
│   │   │                       pending_positions, dispatch() })
│   │   ├── translate.rs      (OE-specific Melin ↔ FIX helpers: D→SubmitOrder,
│   │   │                      8-out, H-out, AF-out, AN↔QueryPosition↔AP)
│   │   └── config.rs
│
├── md-gateway/       (NEW — binary)
│   ├── src/
│   │   ├── main.rs
│   │   ├── event_loop.rs     (io_uring — MD-shaped: single upstream firehose,
│   │   │                      many downstream FIX sessions with fan-out)
│   │   ├── session.rs        (MdSession { core: SessionCore, md_subs,
│   │   │                       md_output_queue, dispatch() })
│   │   ├── translate.rs      (MD-specific helpers: V→MdCommand, W-out, X-out,
│   │   │                      Y-out, x→y)
│   │   └── config.rs
│
├── engine/           (existing — Phase 0 touches types.rs + journal/pipeline.rs)
├── protocol/         (existing — Phase 0 touches message.rs + codec.rs;
│                      Phase 7 adds Request::QueryPosition)
├── server/           (existing — Phase 3 extends event_publisher)
├── client/           (existing — Phase 0 consumer update)
├── tui/              (existing — kept for now; Phase 9 replaces with tui-fix-client)
├── tui-fix-client/   (NEW — binary, Phase 9)
└── ...               (bench, admin, dpdk, disruptor, etc. — consumer updates in Phase 0)
```

## Architecture

```
┌─ melin-server (existing) ──────────────────────────────────┐
│  engine pipeline                                            │
│  ├─ Phase 0: ExecutionReport gains symbol + account on      │
│  │   every per-order variant (clean break, no wrapper).     │
│  └─ event_publisher (crates/server/src/event_publisher.rs)  │
│      Phase 3 adds a post-ServerReady SubscribeWithSnapshot  │
│      handshake: server streams book snapshot + trade rings, │
│      then resumes firehose. Server seeds its own authoritative│
│      BookMirror at boot from Exchange::snapshot_state().    │
└────────────┬───────────────────────────────────────────────┘
             │ 1× long-lived TCP firehose connection (md-gateway)
             │ N× per-session TCP connections (oe-gateway)
             │
       ┌─────┴───────┬──────────────────────────────────────┐
       ▼             ▼                                       │
┌─ md-gateway (NEW) ──────────────┐   ┌─ oe-gateway (renamed fix-gateway) ─┐
│  MarketDataCore thread          │   │  io_uring event loop               │
│  ├─ owns firehose TCP, ReadOnly │   │  ├─ accepts FIX 4.4 sessions       │
│  ├─ BookMirror per symbol       │   │  ├─ per-session melin connection   │
│  ├─ TradeRing per symbol        │   │  │   (existing pattern)            │
│  ├─ fan-out queues per session  │   │  ├─ OeSession state                │
│  └─ crossbeam + eventfd wakeup  │   │  │   (id_map, order_symbols,       │
│                                 │   │  │    order_ledger,                │
│  io_uring event loop            │   │  │    pending_positions)          │
│  ├─ accepts FIX 4.4 sessions    │   │  └─ handles D/F/G/8/9/H/AF/AN/AP  │
│  ├─ MdSession state (md_subs,   │   │                                    │
│  │   md_output_queue)           │   │  Uses: gateway-core                │
│  ├─ handles V/W/X/Y/x/y         │   │  New protocol: Request::QueryPosition│
│  └─ eventfd fan-out drain       │   │                                    │
│                                 │   └────────────────────────────────────┘
│  Uses: gateway-core, market-data│
└─────────────────────────────────┘
                         ▲           ▲
                         │           │
                         │           │ FIX client, one session per gateway
                         │           │ (trader opens 2 sessions: 1 MD, 1 OE)
                         │           │
                  ┌──────┴───────────┴──────┐
                  │  tui-fix-client (NEW)   │
                  │  Phase 9                │
                  │  ratatui + crossterm,   │
                  │  two FIX sessions,      │
                  │  order book / candles / │
                  │  active orders /        │
                  │  balances / order entry │
                  └─────────────────────────┘
```

## Process topology

Two binaries, each a small entry point over shared libraries. Deployment scenarios:

- **Dev / bench / small operator**: one host runs both `oe-gateway` and `md-gateway` as separate processes on different ports. Single machine, two services.
- **Production exchange**: hosts run one role each. Multiple `md-gateway` instances fan out to many clients behind a load balancer (MD is read-only and can be horizontally scaled). `oe-gateway` stays per-instance, per-tenant.
- **Failure isolation**: a panic or crash in one gateway does not affect the other. `md-gateway` going down does not block order submission; `oe-gateway` going down does not interrupt market-data feeds.

Both binaries share a FIX version (4.4), a Cargo workspace, the Ed25519 authorized-keys file format, the metrics exporter shape, and config loading conventions — all lifted into `gateway-core`.

FIX clients needing both roles open **two sessions** (one per gateway), with distinct `SenderCompID` / `TargetCompID` pairs per operator convention. This is the standard pattern at CME, BATS, ICE, etc.

## Phase 0 — ExecutionReport normalization (symbol + account)

`ExecutionReport` variants (`crates/engine/src/types.rs:264-318`) are missing context inconsistently: `Placed`/`Triggered`/`Replaced` carry neither symbol nor account; `Fill` has accounts but no symbol; `Cancelled`/`Rejected` have account but no symbol. An out-of-process book mirror can't apply events without the symbol, and per-account query features (Phase 7) need the account on every event that could belong to a trader's ledger.

**Clean break, no retrocompatibility.** Every per-order variant gains `symbol: Symbol`; `Placed`/`Triggered`/`Replaced` gain `account: AccountId`. `Fill` already has `maker_account`/`taker_account`, just add symbol. `InstrumentStatusChanged` already has symbol, no account applicable. The matching stage has both values in scope at every emit site (~10 call sites in `crates/engine/src/journal/pipeline.rs` around lines 1765-1834).

Final shape:

```rust
Placed            { order_id, symbol, account, side, price, quantity }
Fill              { maker_order_id, taker_order_id, symbol, maker_account, taker_account,
                    price, quantity, maker_fee, taker_fee }
Cancelled         { order_id, symbol, account, remaining_quantity }
Triggered         { order_id, symbol, account, trigger_price }
Rejected          { order_id, symbol, account, reason }
Replaced          { order_id, symbol, account, side, old_price, new_price,
                    old_remaining, new_remaining }
InstrumentStatusChanged { symbol, status }
```

**Files**: `crates/engine/src/types.rs`, `crates/engine/src/journal/pipeline.rs`, `crates/engine/src/exchange.rs` + `crates/engine/src/orderbook.rs` (any helpers building reports), `crates/protocol/src/message.rs`, `crates/protocol/src/codec.rs`, `crates/server/src/event_publisher.rs:46` (`payload_to_response`), `crates/fix-gateway/src/session.rs:757+` (soon-to-be-renamed — still a consumer here), `crates/client/src/lib.rs`, `crates/bench/`, `crates/tui/`, `crates/admin/`.

**Verification**: `cargo check` across the whole workspace (all `ExecutionReport` consumers must compile). `cargo test -p melin-engine -p melin-protocol -p melin-fix-gateway`. A fixture test that records a sequence of engine events and asserts each emitted report carries the symbol and account the matcher was working on at emission.

## Phase 1 — `crates/gateway-core/` + rename fix-gateway → oe-gateway

Structural refactor with no behavior change. Two mechanical steps, one commit each:

### 1a. Rename `crates/fix-gateway/` → `crates/oe-gateway/`

- `git mv crates/fix-gateway crates/oe-gateway`
- Update `Cargo.toml` package name `melin-fix-gateway` → `melin-oe-gateway`
- Update workspace `Cargo.toml` member list
- Update any `-p melin-fix-gateway` in scripts / CI / docs
- Update binary name in `[[bin]]` or `main.rs` header

**Verification**: `cargo check -p melin-oe-gateway`, `cargo test -p melin-oe-gateway`, `cargo check --workspace`.

### 1b. Extract `crates/gateway-core/`

Move to `gateway-core`:
- `crates/oe-gateway/src/fix/parse.rs` → `crates/gateway-core/src/fix/parse.rs`
- `crates/oe-gateway/src/fix/serialize.rs` → `crates/gateway-core/src/fix/serialize.rs`
- `crates/oe-gateway/src/fix/tags.rs` → `crates/gateway-core/src/fix/tags.rs`
- Session-layer message handling from `crates/oe-gateway/src/session.rs` (Logon at 258-358, Heartbeat at 587, TestRequest at 589-595, SequenceReset at 539-548, Logout at 597-600, ResendRequest at 602-617, sequence numbers at 550-580, replay buffer at 124-143) → a new `SessionCore` struct in `crates/gateway-core/src/session/core.rs`
- Ed25519 auth helpers → `crates/gateway-core/src/auth.rs`

Define in `gateway-core`:
```rust
pub trait MessageDispatcher {
    /// Role-specific dispatch. Called after SessionCore handles
    /// session-layer messages (Heartbeat, TestRequest, ResendRequest, etc).
    /// Implementations handle application messages for their role.
    fn dispatch(&mut self, msg_type: &[u8], msg: &FixMessage<'_>) -> SessionAction;
}

pub struct SessionCore {
    pub sender_comp_id: String,
    pub target_comp_id: String,
    pub seq_in: u64,
    pub seq_out: u64,
    pub replay_buf: VecDeque<StoredMessage>,
    pub last_activity: Instant,
    // ...
}

impl SessionCore {
    pub fn handle_session_level(&mut self, msg_type: &[u8], msg: &FixMessage<'_>) -> Option<SessionAction> {
        // Handle A/0/1/2/4/5; return None if not a session-level message.
    }
}
```

`oe-gateway` `OeSession` holds `core: SessionCore` plus the OE-specific fields (`id_map`, `order_symbols`, `pending_cancels`, eventually `order_ledger`, `pending_positions`), and implements `MessageDispatcher` for `D`/`F`/`G` (and in Phase 7, `H`/`AF`/`AN`).

No io_uring primitives in `gateway-core` — the two gateways have different I/O shapes and will each own their own `event_loop.rs`.

**Files**: new crate `crates/gateway-core/`, oe-gateway imports updated, workspace `Cargo.toml` updated.

**Verification**: `cargo check -p melin-gateway-core -p melin-oe-gateway`, all existing oe-gateway tests still green. No runtime behavior change — this is a pure refactor.

## Phase 2 — FIX 4.4 upgrade (before adding handlers)

Do the version bump **now**, before any new FIX message handlers are written. This way all new code (Phases 3-8) is authored against FIX 4.4 from the start — no porting, no version-conditional logic.

Touches `crates/gateway-core/src/fix/`:
- `BeginString` changes from `FIX.4.2` to `FIX.4.4` (`serialize.rs`, `parse.rs` header validation)
- Tag set audit — FIX 4.4 makes a few tags optional that were required in 4.2 (`HandlInst (21)`) and vice versa. Mostly compatible at the code we care about.
- MsgType constants remain the same bytes (`D`, `F`, `G`, `8`, `0`, `A`, etc.)
- Session-layer messages unchanged at the fields we use
- Update client-side fixture tests to FIX 4.4

Single commit. Both `oe-gateway` and any future `md-gateway` inherit 4.4 via `gateway-core`.

**Verification**: `cargo test -p melin-gateway-core -p melin-oe-gateway`. Optional interop test with quickfix-python using a standard FIX 4.4 DataDictionary against the oe-gateway for existing order-entry messages.

## Phase 3 — `crates/market-data/` crate (library only)

Pure library crate, no I/O, no server, no gateway. Defines the data types and unit-tested update logic that both the server-side mirror (Phase 3) and the client-side `MarketDataCore` (Phase 4) will reuse.

```
crates/market-data/
├── Cargo.toml                 (depends on melin-engine for types, no tokio, no nix)
└── src/
    ├── lib.rs
    ├── mirror.rs              (BookMirror, Level, apply_report)
    ├── trade_ring.rs          (TradeRing per symbol, ArrayDeque<Trade, 4096>)
    └── index.rs               (OrderIndex: HashMap<OrderId, RestingOrder>)
```

`BookMirror` per symbol:
- `bids: BTreeMap<Price, Level>` — best bid via `.iter().next_back()`
- `asks: BTreeMap<Price, Level>` — best ask via `.iter().next()`
- `last_trade_price: Option<Price>`, `last_trade_ts_ns: u64`
- `dirty_bids / dirty_asks: SmallVec<[Price; 8]>` — drained on fan-out

`Level { total_qty: u64, order_count: u32 }`. No L3 per-order granularity in v1.

`OrderIndex: HashMap<OrderId, RestingOrder { symbol, price, side, remaining }>` for resolving `Fill`/`Cancelled`/`Replaced` back to the right symbol + level. Phase 0's symbol-on-event lets `apply_report` trust the symbol from the event rather than look it up.

**Update rules**, one per `ExecutionReport` variant:
- `Placed` → insert into `OrderIndex`, credit level `(+qty, +1 count)`, mark dirty
- `Fill` → look up `maker_order_id` in `OrderIndex`, decrement level `(-qty, -1 count only if maker residual hits zero)`, update `last_trade_*`, push to trade ring
- `Cancelled` → look up, decrement level by `remaining_quantity`, `-1 count`, remove from index
- `Replaced` → decrement old level, credit new level (`-1 / +1` count)
- `Triggered` → no-op on the book (wait for the subsequent `Placed` or `Fill`)
- `InstrumentStatusChanged` → on `Disabled`/`Removed`, clear the symbol's books (assert-empty: engine emits `Cancelled` for every resting order before this event)
- `Rejected` → ignored (never on the book)

**Sizing**: ~22 KiB per symbol at 500 active levels, plus `OrderIndex` at ~32 bytes per resting order. Configurable ceiling exposed later via gateway/server config.

**No threading, no channels, no I/O** at this phase — just data + pure functions. `MarketDataCore` (the stateful consumer + fan-out loop) is introduced in Phase 4.

**Verification**: `crates/market-data/tests/` unit tests — replay a recorded fixture of `ExecutionReport` sequences against the mirror, assert book state matches an expected reference. Property test via `proptest` — generate random event sequences, apply to both a naive reference book and `BookMirror`, assert equivalence after each event. This is the single most important test in the plan; mirror divergence bugs would otherwise only surface at integration time.

## Phase 4 — Server-side event_publisher uses BookMirror + SubscribeWithSnapshot

With `market-data::BookMirror` in place, the server-side event publisher seeds an authoritative mirror at boot from `Exchange::snapshot_state()` (`crates/engine/src/journal/snapshot.rs:1130`, read-only, boot-only — never touches the matching thread post-boot), then applies every `OutputPayload::Report` (now carrying symbol per Phase 0) on its way to subscribers.

**New subscription handshake** in `crates/server/src/event_publisher.rs`: after `ServerReady`, the subscriber sends a `SubscribeRequest { symbols: Vec<Symbol>, include_trades: bool }` frame. No legacy mode — every subscriber goes through this handshake. Existing subscribers (tests, analytics) are updated to send it. The publisher serializes the matching book mirror state as:

```
0x82 BookSnapshotBegin { symbol, last_applied_seq }
0x83 BookSnapshotLevel { symbol, side, price, qty, order_count } [...]
0x84 BookSnapshotEnd   { symbol, level_count }
0x85 TradeRingSnapshot { symbol, count, [trade...] }
0x86 SnapshotComplete  { last_applied_seq }
```

Then resume the firehose, frame sequence = `last_applied_seq + 1`, no gaps (guaranteed by consuming continuously before serving the snapshot).

**Reconnect**: on md-gateway disconnect + reconnect, the gateway drops local state, re-handshakes, re-snapshots. Active FIX sessions with MarketData subscriptions get re-sent `MarketDataSnapshotFullRefresh` after the gateway finishes its sync — no FIX-level disconnect needed.

**Background reconciliation**: every 60s the publisher diffs its live mirror against a fresh `Exchange::snapshot_state()` capture; any divergence increments `book_mirror_divergence_total` and logs the offending symbol. Early-warning signal for mirror bugs.

**Files**: `crates/server/src/event_publisher.rs` (+300 LOC), `crates/engine/src/exchange.rs` (expose a lighter `snapshot_books_only()` if the full snapshot is too heavy at boot).

**Verification**: new integration test `crates/server/tests/subscribe_with_snapshot.rs` — boots engine + publisher with seeded resting orders, connects in subscribe mode, asserts returned snapshot frames match an expected reference. Feeds one new fill via a test client, asserts the follow-up `Report { symbol, report }` frame arrives and the mirror's state reflects the fill.

## Phase 5 — MarketDataCore consumer loop + fan-out

Extends `crates/market-data/` with the stateful client-side consumer: the component `md-gateway` instantiates to connect to the server's event publisher, drive the subscribe handshake, maintain its own `BookMirror` from snapshot frames + firehose events, and fan out to downstream subscribers.

New files in `crates/market-data/src/`:

```
├── core.rs                (MarketDataCore::run — consumer loop + fan-out)
├── subscriber.rs          (SessionSlotId, MdCommand, MdOutput)
└── cold_start.rs          (client-side SubscribeWithSnapshot parsing: 0x82-0x86)
```

`MarketDataCore::run(cmd_rx, server_addr, creds)` main loop:
1. Connect to the server's event publisher endpoint
2. Complete Ed25519 challenge-response auth with ReadOnly+
3. Send `SubscribeRequest` with the configured symbol set
4. Parse inbound snapshot frames (Phase 3's 0x82-0x86 wire format), seed the local mirror
5. Enter steady state: `crossbeam_channel::select!` on `(cmd_rx: bounded(256), tcp_read_ready)`
6. Apply every inbound firehose `Report { symbol, report }` frame to the mirror, collect dirty levels per symbol, fan out to subscribing sessions
7. Drain command channel for `MdCommand::{Subscribe, Unsubscribe, Reject}`

**Threading**: `MarketDataCore` runs on its **own dedicated OS thread**. Justifications: the firehose is bursty and independent of consumer I/O; fan-out shouldn't share a critical path with it; a second thread with a wakeup eventfd is strictly simpler than multiplexing two upstream protocols on one ring. This thread lives inside `md-gateway`, but the code is reusable from any binary that links `crates/market-data/`.

**Fan-out**: per-subscriber `Arc<ArrayQueue<MdOutput, 1024>>`. On push failure the subscription is dropped — the core sends `MdCommand::Reject` and removes it from its fan-out table. The consumer of the queue (md-gateway session in Phase 5) translates the reject to `MarketDataRequestReject` with `MDReqRejReason=0`. The core itself never blocks on fan-out.

**Reconnect**: on firehose disconnect the core drops its mirror, reconnects, re-handshakes, re-snapshots. All active subscriptions get re-emitted `MdOutput::Snapshot` after sync completes; downstream consumers translate to fresh `MarketDataSnapshotFullRefresh` without needing a FIX-level session disconnect.

**Verification**: `crates/market-data/tests/core_test.rs` — stub the event-publisher endpoint with a scripted fixture stream, subscribe via `MarketDataCore`, replay N events, assert the emitted `MdOutput` sequence matches expected. Integration test against a real Phase 3 server+publisher: boot both, subscribe, drive order flow, verify mirror consistency.

## Phase 6 — `crates/md-gateway/` binary: V / W / Y

New binary. Starts as a minimal FIX 4.4 listener that delegates session-layer messages to `gateway-core::SessionCore`, spawns a `MarketDataCore` thread on startup, and implements `MessageDispatcher` for `MarketDataRequest`, `MarketDataSnapshotFullRefresh` output, and `MarketDataRequestReject`. No incremental refresh yet (Phase 6), no query messages yet (oe-gateway Phase 7).

**Files (new)**: `crates/md-gateway/Cargo.toml`, `src/main.rs`, `src/event_loop.rs`, `src/session.rs` (`MdSession`), `src/translate.rs`, `src/config.rs`.

**Event loop**: registers a TCP listener for FIX 4.4 connections. On accept, runs Logon via `SessionCore`, creates an `MdSession`. Registers the `MarketDataCore` wakeup eventfd with io_uring using multishot `OP_READ`. On eventfd wakeup, iterates active MdSessions, drains each session's `ArrayQueue<MdOutput>`, dispatches to `MdSession::handle_md_output`.

**MdSession state**:
- `core: SessionCore` (from gateway-core)
- `md_subs: HashMap<MdReqId, MdSubscription { symbols, depth, incremental, include_trades }>`
- `md_output_queue: Arc<ArrayQueue<MdOutput, 1024>>`
- `symbol_map: &SymbolMap` (shared)

**Dispatch** (`MessageDispatcher::dispatch` in `session.rs`):
- `tags::MSG_MARKET_DATA_REQUEST (b"V")` → `handle_market_data_request`
- everything else → reject (md-gateway doesn't handle OE messages)

**`handle_market_data_request`** (~60 LOC): parses `MDReqID (262)`, `SubscriptionRequestType (263)` ∈ {0=snapshot, 1=subscribe, 2=unsubscribe}, `MarketDepth (264)`, `MDUpdateType (265)` ∈ {0,1}, iterates the `NoRelatedSym (146)` group collecting `Symbol (55)`. Validates symbols, unknown → `MarketDataRequestReject` with `MDReqRejReason=1`. On success, pushes `MdCommand::Subscribe { session_slot, mdreq_id, symbols, depth, incremental, include_trades }` via the crossbeam channel.

**New tags** in `crates/gateway-core/src/fix/tags.rs`: `MDReqID (262)`, `SubscriptionRequestType (263)`, `MarketDepth (264)`, `MDUpdateType (265)`, `MDEntryType (269)`, `MDEntryPx (270)`, `MDEntrySize (271)`, `NoMDEntries (268)`, `NoRelatedSym (146)`, `MDUpdateAction (279)`, `NumberOfOrders (346)`. MsgType constants `MSG_MARKET_DATA_REQUEST = b"V"`, `MSG_MD_SNAPSHOT = b"W"`, `MSG_MD_INCREMENTAL = b"X"`, `MSG_MD_REQUEST_REJECT = b"Y"`. Shared with oe-gateway even though oe-gateway doesn't emit them — keeps tag table single-source.

**New helpers** in `crates/md-gateway/src/translate.rs`: `md_snapshot_to_fix` (~80 LOC), `md_request_reject` (~20 LOC).

**Verification**: end-to-end manual test. A scripted FIX client sends `Logon` + `MarketDataRequest` for BTCUSD, asserts it receives a `W` message with the expected top-10 levels within 100 ms of the gateway's firehose receiving the snapshot frames. Also a first integration test covering Phase 3+4+5 end-to-end: bench running, md-gateway serving, test client asserts monotonic book state as fills arrive.

## Phase 7 — md-gateway: X (incremental refresh), unsubscribe, coalescing, SecurityList

`MarketDataCore` after each batch of applied events collects dirty levels per subscribed symbol, builds one `MdOutput::Incremental { mdreq_id, updates: Vec<LevelUpdate> }` per subscribing session, pushes to the per-session queue.

**Coalescing**: up to 32 level updates per `MarketDataIncrementalRefresh` message, flushed when the budget fills or 1 ms elapses since the first queued update. Coalescing state lives on the session, not the core — md-gateway builds the outbound FIX message from the queued updates.

**Session handler** `MdSession::emit_md_incremental(update)` (`md-gateway/src/session.rs`, ~50 LOC) builds `MSG_MD_INCREMENTAL` with the `NoMDEntries (268)` group, each entry carrying `MDUpdateAction (279)` ∈ {0=New, 1=Change, 2=Delete}, `MDEntryType (269)` ∈ {0=Bid, 1=Offer, 2=Trade}, `MDEntryPx (270)`, `MDEntrySize (271)`, `NumberOfOrders (346)`.

**Unsubscribe**: `SubscriptionRequestType=2` dispatches `MdCommand::Unsubscribe { session_slot, mdreq_id }` to the core; the core removes the subscription and stops queuing fan-out for it.

**Backpressure**: if `ArrayQueue::push` fails, core increments `md_queue_overflow_total`, sends `MdCommand::Reject` for that mdreq_id, and drops the subscription. Session translates to `MarketDataRequestReject` with reason=0.

**SecurityListRequest (x) → SecurityList (y)**: new dispatch case (~20 LOC) in `MdSession::dispatch`. Iterates md-gateway's `symbol_map` (with new `base_ccy: String`, `quote_ccy: String` fields added in `md-gateway/src/config.rs`), builds one `SecurityList` with `NoRelatedSym` group carrying `Symbol (55)`, `MinPriceIncrement (969)`, `RoundLot (561)`, `Currency (15)`, `SettlCurrency (120)`. Helper `md-gateway/src/translate.rs::security_list_to_fix` (~50 LOC). New tags in gateway-core: `SecurityReqID (320)`, `SecurityListRequestType (559)`, `MinPriceIncrement (969)`, `RoundLot (561)`, `Currency (15)`, `SettlCurrency (120)`, plus MsgType `MSG_SECURITY_LIST_REQUEST (b"x")`, `MSG_SECURITY_LIST (b"y")`.

**Verification**: property-style test — generate a random sequence of `ExecutionReport`s, apply to both a reference book and `MarketDataCore`+md-gateway, assert the emitted `W` + stream of `X` messages reconstruct the reference exactly (bid/ask levels, quantities, counts). Plus a simple integration test for `x`/`y`.

## Phase 8 — oe-gateway: H, AF, AN, AP + Request::QueryPosition

### New protocol message in `crates/protocol/`

- `Request::QueryPosition { account: AccountId }`
- `Response::PositionSnapshot { account, balances: SmallVec<[(Currency, u64 /*free*/, u64 /*reserved*/); 8]> }`
- Codec additions in `crates/protocol/src/codec.rs`
- Server handler in `crates/server/src/request.rs` routing to the engine via the existing input disruptor path, mirroring how `Request::QueryStats` is handled today
- Matching-stage handler reads `Exchange::accounts[account].balances` (read-only, one-shot, O(currencies)); serializes into `OutputPayload::PositionSnapshot` and returns. Cap at 64 currencies per response to keep the hot-path cost bounded.

### OrderStatusRequest (H) and OrderMassStatusRequest (AF)

Per-session, no `MarketDataCore` involvement (oe-gateway doesn't have one). Requires new per-session state `order_ledger: HashMap<OrderId, OrderLiveState { ord_status, leaves_qty, cum_qty, avg_px, symbol, side, price }>` updated in `handle_active_melin` as each inbound report is processed.

Dispatch cases in `oe-gateway/src/session.rs` (~25 LOC each): parse tags, look up in `order_ledger`, emit `ExecutionReport (8)` with `ExecType (150) = I` (OrderStatus). Helpers `oe-gateway/src/translate.rs::order_status_report` and `order_mass_status` (~120 LOC total).

`OrderMassStatusReqType (585)`: 1 = all active orders, 7 = by instrument. Empty result = one terminating ER with `TotNumReports=0`.

### RequestForPositions (AN) → PositionReport (AP)

Dispatch case in `oe-gateway/src/session.rs` (~30 LOC): parse `PosReqID (710)`, verify `Account (1)` matches the session's CompID (or default to self), send `Request::QueryPosition` on the existing per-session melin connection, track in a `pending_positions: HashMap<MelinReqSeq, PosReqId>` map, translate the `PositionSnapshot` response to `PositionReport (AP)` when it arrives in `handle_active_melin`. Helper `oe-gateway/src/translate.rs::position_report_to_fix` (~60 LOC).

**New tags** in gateway-core: `ExecType (150)`, `MassStatusReqID (584)`, `MassStatusReqType (585)`, `TotNumReports (911)`, `PosReqID (710)`, `PosReqType (724)`, `NoPositions (702)`, `LongQty (704)`, `ShortQty (705)`, `Account (1)`. MsgType constants `MSG_ORDER_STATUS_REQUEST (b"H")`, `MSG_ORDER_MASS_STATUS_REQUEST (b"AF")`, `MSG_REQUEST_FOR_POSITIONS (b"AN")`, `MSG_POSITION_REPORT (b"AP")`.

**Verification**: one integration test per handler against a live server with seeded account state.

## Phase 9 — TUI as FIX 4.4 client (two sessions)

**New crate** `crates/tui-fix-client/` — distinct from the existing `crates/tui/` (which talks to melin directly via `melin-client`). The old `crates/tui/` can be retired once feature parity is reached, or kept as a developer smoke-test client.

Stack: `crossterm` + `ratatui`. Uses `crates/gateway-core/src/fix/{parse,serialize}.rs` for Tag=Value codec. Wraps them in a simple blocking TCP client.

**Two FIX sessions**:
- **MD session** to `md-gateway` — sends `MarketDataRequest`, `SecurityListRequest`; consumes `SecurityList`, `MarketDataSnapshotFullRefresh`, `MarketDataIncrementalRefresh`
- **OE session** to `oe-gateway` — sends `NewOrderSingle`, `OrderCancelRequest`, `OrderStatusRequest`, `OrderMassStatusRequest`, `RequestForPositions`; consumes `ExecutionReport`, `OrderCancelReject`, `PositionReport`

**Panels**:
- Order book (top 10 levels, driven by MD session `MarketDataRequest`, rendered from `W`/`X` messages)
- Trade tape / candle chart (from `MDEntryType=2` trade entries, aggregated locally into 1m/5m/1h OHLCV, rendered with ratatui braille chart widget)
- Active orders (from `OrderMassStatusRequest` on the OE session, refreshed every ~1s, or passively updated from inbound `ExecutionReport`s)
- Balances (from `RequestForPositions` on the OE session, refreshed every ~2s)
- Order entry form (sends `NewOrderSingle` on the OE session; confirmation dialog shows symbol/side/qty/price + available balance before submission)

**Dynamic instrument discovery**: `SecurityListRequest` on MD session startup populates a symbol dropdown.

**Session-only candles**: fills accumulated from the moment the TUI connects to md-gateway. Pre-session candles require server-side trade persistence (explicitly out of scope for v1).

**Verification**: manual end-to-end — run `melin-server` + `oe-gateway` + `md-gateway` + `tui-fix-client` + a bench generating order flow. Verify each panel updates as trades flow through, orders appear and disappear, balances change, candles accumulate.

## Critical files

| Path | Role | Phase |
|---|---|---|
| `crates/engine/src/types.rs:264` | `ExecutionReport` enum — gains `symbol` + `account` | 0 |
| `crates/engine/src/journal/pipeline.rs` | ~10 emit sites updated | 0 |
| `crates/protocol/src/message.rs` | Updated destructure shape; later `QueryPosition`/`PositionSnapshot` | 0, 8 |
| `crates/protocol/src/codec.rs` | Wire encoding updates | 0, 8 |
| `crates/server/src/event_publisher.rs` | Phase 0: `payload_to_response`; Phase 4: SubscribeWithSnapshot, BookMirror, reconciliation | 0, 4 |
| `crates/fix-gateway/` → `crates/oe-gateway/` | Rename + imports | 1 |
| `crates/gateway-core/` (new) | FIX parse/serialize/tags, `SessionCore`, `MessageDispatcher`, auth | 1 |
| `crates/gateway-core/src/fix/{serialize,parse}.rs` | FIX 4.4 BeginString | 2 |
| `crates/market-data/` (new) | `BookMirror`, `OrderIndex`, `TradeRing` (Phase 3); `MarketDataCore`, `cold_start` (Phase 5) | 3, 5 |
| `crates/engine/src/journal/snapshot.rs:1130` | `Exchange::snapshot_state()` — mirror boot seeding | 4 |
| `crates/md-gateway/` (new) | MD-role binary, `MdSession`, V/W/Y handlers (Phase 6), X/x/y handlers (Phase 7) | 6, 7 |
| `crates/oe-gateway/src/session.rs` | `OeSession`: `order_ledger`, `pending_positions`, H/AF/AN handlers | 8 |
| `crates/oe-gateway/src/translate.rs` | `order_status_report`, `order_mass_status`, `position_report_to_fix` | 8 |
| `crates/server/src/request.rs` | `QueryPosition` server-side handler | 8 |
| `crates/gateway-core/src/fix/tags.rs` | New FIX 4.4 tag constants (shared) | 6, 7, 8 |
| `crates/tui-fix-client/` (new) | TUI speaking FIX 4.4 to both gateways | 9 |

## Verification strategy

- **Per-phase unit tests and integration tests** as called out under each phase. Gate on `cargo test` green before moving on.
- **Phase 3 property test against `BookMirror`** — the single most important test in the plan. Generate 10k-event sequences via `proptest`, compare `BookMirror` state against a naive reference after each event.
- **Phase 4 reconciliation job** running in the publisher emits a metric any time the live book mirror diverges from `Exchange::snapshot_state()`. During development, turn this into a hard fail in test builds.
- **Phase 9 end-to-end smoke test**: launch `melin-server` + `oe-gateway` + `md-gateway` + `tui-fix-client` + a bench generating order flow. Visually confirm the book updates, active orders move, balances change, candles accumulate.
- **`cargo check --workspace`** must pass before each commit (per `CLAUDE.md`).
- **No `.unwrap()` in production code** (per `CLAUDE.md`) — enforce via clippy lint and code review.
- **Correctness and performance discipline** throughout: profile hot paths, measure p99, avoid allocations in mirror update loops, test edge cases (cancel-replace across price levels, stop-trigger cascades, empty-book scenarios).

## Risks and decisions baked in

- **Book mirror divergence is the top correctness risk.** Mitigations: server-side reference mirror that always runs (Phase 4); periodic reconciliation against `Exchange::snapshot_state()` (60s cadence, loud metric); Phase 3 property test; md-gateway periodically re-snapshots and diffs against its local mirror as a self-check.
- **Phase 0 is a clean break.** Every `ExecutionReport` consumer must compile against the new variant shape in the same commit. Lockstep update across all workspace crates.
- **Phase 1 is a pure refactor, zero runtime change.** Target byte-identical wire output on the existing test corpus.
- **Phase 2 (FIX 4.4 upgrade) is early** — all subsequent handler code is written against 4.4 from the start. No porting, no version-conditional logic.
- **No legacy firehose mode.** SubscribeWithSnapshot is the only event_publisher subscription protocol. No mode byte, no branching. All existing subscribers (tests, analytics) updated in Phase 4.
- **`QueryPosition` touches the matching hot path.** Precedent: `QueryStats` already does this. Capped at 64 currencies per response. Likely <1 µs.
- **`MarketDataCore` disconnection.** Core drops local state, reconnects, re-snapshots. Active FIX sessions get re-issued `MarketDataSnapshotFullRefresh` after the sync completes. No FIX-level session disruption.
- **Slow FIX clients.** `ArrayQueue` overflow drops the individual subscription; core keeps consuming; other subscribers unaffected.
- **Separate binary failure isolation.** md-gateway panic does not affect oe-gateway or vice versa. Watchdog / systemd restart policies handle recovery.
- **DPDK transport interaction.** `event_publisher` is TCP-only; DPDK transport is unaffected.
- **Open question**: does the engine emit `Placed` before immediate fills for marketable orders? Needs verification before starting Phase 3.
- **Open question**: does `Fill` fire once per trade or twice? Sanity-check before Phase 3.

## Branch strategy

**All work on a dedicated feature branch** (e.g., `feat/split-gateways-md`), branched from main. Each phase is one or more commits. Review gates between phases — don't merge to main until a well-defined milestone is reached (e.g., Phases 0-6 where md-gateway serves a live book).

Correctness and performance are first-class concerns throughout — not something to bolt on at the end. Every mirror update function gets property tests, every FIX message handler gets a fixture test, every allocation-sensitive path gets profiled before moving on.

## Execution cadence

Execute in slices with review gates between phases.

Estimated effort:
- Phase 0: ~0.5 day (ExecutionReport normalization)
- Phase 1: ~1.5 days (rename + gateway-core extraction)
- Phase 2: ~0.5 day (FIX 4.4 upgrade, mostly mechanical)
- Phase 3: ~1.5 days (market-data library + property tests)
- Phase 4: ~1.5 days (server-side publisher + SubscribeWithSnapshot + reconciliation)
- Phase 5: ~1 day (MarketDataCore consumer loop + fan-out)
- Phase 6: ~1 day (md-gateway minimal binary + V/W/Y)
- Phase 7: ~1 day (md-gateway incremental + SecurityList)
- Phase 8: ~2 days (oe-gateway query handlers + QueryPosition protocol)
- Phase 9: ~5-7 days (TUI as FIX client)

**Gateway critical path (Phases 0-8): ~10 days.** TUI: another ~1 week.
