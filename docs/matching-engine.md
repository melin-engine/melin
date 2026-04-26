# Matching Engine

This document describes the matching engine's order types, matching algorithm, and execution semantics. All types referenced below are defined in `crates/engine/src/types.rs`, with matching logic in `crates/engine/src/orderbook.rs` and multi-instrument dispatch in `crates/engine/src/exchange.rs`.

---

## Order Types

Every order carries an `OrderType` variant that determines how it enters the book or executes immediately.

### Market

```rust
OrderType::Market
```

Executes immediately at the best available prices on the opposite side. A market order never rests on the book -- any unfilled remainder after sweeping available liquidity is cancelled (a `Cancelled` report is emitted with `remaining_quantity`).

**Buy-side quote budget:** Because market buys have no limit price, the fill cost is unknown at submission time. The `Exchange` reserves the account's entire available quote balance and passes it as a `quote_budget` to the matching engine. The matcher stops filling once the budget is exhausted (see `match_against`). If the budget cannot afford even 1 lot at the current price (`affordable == 0`), matching halts. Any leftover reservation is released after execution.

**Empty book rejection:** If the opposite side is empty when a market order arrives, it is rejected with `RejectReason::NoLiquidity` before any matching attempt.

### Limit

```rust
OrderType::Limit { price: Price, post_only: bool }
```

Executes at the specified `price` or better. The order first matches against the opposite side at prices that would satisfy it (asks <= price for buys, bids >= price for sells). Any unfilled remainder is handled according to the order's `TimeInForce`:

- **GTC**: remainder is placed on the book as a `RestingOrder`.
- **IOC**: remainder is cancelled.
- **FOK**: see the FOK section below -- the order is rejected upfront if it cannot fill entirely.
- **Day**: remainder rests like GTC, but is automatically cancelled when an `EndOfDay` event is processed.
- **GTD**: remainder rests like GTC, but is automatically cancelled when an `ExpireOrders` event with `timestamp_ns >= order.expiry_ns` is processed.

**Post-Only mode**: When `post_only` is `true`, the order is rejected with `PostOnlyWouldCross` if it would immediately match against resting liquidity (cross the spread). This guarantees maker-only execution. Post-only is only meaningful for GTC/Day/GTD limit orders -- IOC and FOK are inherently taker-side.

### Stop

```rust
OrderType::Stop { trigger_price: Price }
```

A dormant order that becomes a **market order** when the `last_trade_price` reaches the trigger:

- **Stop buy**: triggers when `last_trade_price >= trigger_price`.
- **Stop sell**: triggers when `last_trade_price <= trigger_price`.

Stop orders are stored in the `PendingStop` struct and indexed in `stop_buys` / `stop_sells` BTreeMaps keyed by `trigger_price`. They do not appear on the visible bid/ask book.

The `quote_budget` from the original reservation is preserved in the `PendingStop` so that the triggered market order respects the same cost cap.

### StopLimit

```rust
OrderType::StopLimit { trigger_price: Price, limit_price: Price }
```

A dormant order that becomes a **limit order** at `limit_price` when the trigger condition is met (same trigger rules as Stop). The `PendingStop` stores `limit_price` as `Some(limit_price)`. No `quote_budget` is needed because the limit price bounds the cost.

---

## Time in Force

The `TimeInForce` enum controls what happens to unfilled quantity after the immediate matching phase.

### GTC (Good-Til-Cancelled)

The unfilled remainder is placed on the book as a resting order. It stays until explicitly cancelled or filled by a subsequent incoming order.

Only meaningful for `Limit` orders (and triggered `StopLimit` orders that become limit orders). Market orders never rest, and Stop/StopLimit orders use TIF only after they trigger.

### IOC (Immediate-Or-Cancel)

Fill as much as possible immediately. Any unfilled remainder is cancelled. Produces one or more `Fill` reports followed by a `Cancelled` report if there is a remainder.

### FOK (Fill-Or-Kill)

The order must fill **entirely** or not at all. Before any matching occurs, the engine performs a pre-check by calling `BookSide::available_quantity()` on the opposite side, summing all resting quantity at matchable prices. If the available quantity is less than the order's `quantity`, the order is rejected with `RejectReason::FOKCannotFill`.

When self-trade prevention is active (any mode other than `Allow`), the FOK pre-check excludes orders from the same account via the `exclude_account` parameter, since those orders would not produce fills.

FOK applies to both market and limit orders. A FOK limit buy checks available ask quantity at prices <= the limit price. A FOK market order checks total opposite-side quantity with no price bound.

### Day

The unfilled remainder is placed on the book like GTC. However, when the operator sends an `EndOfDay` event, all Day orders across all instruments are cancelled. This supports session-based markets where resting orders do not carry over to the next trading day.

### GTD (Good-Til-Date)

The unfilled remainder is placed on the book like GTC, but carries an `expiry_ns` timestamp (nanoseconds since Unix epoch). When the operator sends an `ExpireOrders { timestamp_ns }` event, all GTD orders with `expiry_ns <= timestamp_ns` are cancelled. The operator is responsible for sending `ExpireOrders` at the appropriate time (e.g., via a periodic timer).

GTD orders must have a non-zero `expiry_ns`; non-GTD orders must have `expiry_ns == 0`. Violations are rejected with `InvalidExpiry`.

---

## Matching Algorithm

### Price-time priority

The engine implements strict **price-time priority** (also called FIFO matching):

1. **Price priority**: the best-priced resting order is matched first. For a buy, this is the lowest ask; for a sell, the highest bid.
2. **Time priority**: within the same price level, orders are matched in insertion order (first in, first out).

### Book data structure

Each side of the book (`BookSide`) uses a **sorted `Vec<(Price, VecDeque<RestingOrder>)>`** with binary search:

- **Sorted `Vec`** keeps price levels in sorted order. Typical books have 10-100 active price levels; at ~16 bytes per level entry, the entire Vec fits in L1/L2 cache. Binary search for a price level is O(log n). Insert and remove shift entries but are fast for small n -- the shift is a single `memcpy` in L1. `BTreeMap`'s node-per-entry layout scatters across heap pages and incurs cache misses on traversal.
- **`VecDeque`** at each price level maintains FIFO ordering. `push_back` for new orders and `pop_front` for fills are both O(1).

An auxiliary **`HashMap<OrderId, (Side, Price)>`** (`order_index`) provides O(1) amortized lookup for cancel operations, avoiding a linear scan of the book.

Stop orders use **`BTreeMap<Price, Vec<PendingStop>>`** for `stop_buys` and `stop_sells`, keyed by trigger price. BTreeMap is appropriate here because stop books can have many more price levels (no consolidation at inside prices) and are not on the matching hot path.

### Matching walk

The `match_against` method collects all matchable price levels into a `Vec<Price>`, then iterates through them:

- For a **buy** taker: asks are visited in ascending price order, stopping at the first price above the taker's limit (if any).
- For a **sell** taker: bids are visited in descending price order, stopping at the first price below the taker's limit (if any).

At each price level, the front of the `VecDeque` (oldest order) is matched first. The fill quantity is `min(taker_remaining, maker_remaining)`, further constrained by the `quote_budget` for market buys. A `Fill` report is emitted for each match. The `last_trade_price` is updated after every fill.

---

## Stop Order Lifecycle

### 1. Submission

When a `Stop` or `StopLimit` order arrives at `OrderBook::execute()`, it is **not** matched. Instead, `add_stop()` creates a `PendingStop` and inserts it into:

- `stop_buys` (BTreeMap keyed by `trigger_price`) for buy stops.
- `stop_sells` (BTreeMap keyed by `trigger_price`) for sell stops.

The `stop_index` HashMap tracks `OrderId -> (Side, trigger_price)` for O(1) cancel lookup.

The original order's `time_in_force`, `stp` (self-trade prevention mode), and `quote_budget` are all preserved in the `PendingStop`.

### 2. Trigger check

After every `execute()` call (including fills from other orders), `check_triggers()` runs:

- **Stop buys**: iterates `stop_buys` keys in ascending order, collecting all trigger prices <= `last_trade_price`.
- **Stop sells**: iterates `stop_sells` keys in descending order, collecting all trigger prices >= `last_trade_price`.

All matching stops are removed from the BTreeMaps and `stop_index`.

### 3. Conversion and execution

Each triggered stop emits a `Triggered` report, then is converted:

- `PendingStop` with `limit_price: None` becomes `OrderType::Market` and calls `execute_market()`.
- `PendingStop` with `limit_price: Some(p)` becomes `OrderType::Limit { price: p }` and calls `execute_limit()`.

Triggered orders re-enter the matching pipeline (including FOK pre-checks and TIF handling) but **skip** `check_triggers()` to avoid recursion, since the converted order is always a market or limit type and will never re-add a stop.

### 4. Cancellation

Pending stops can be cancelled via `OrderBook::cancel()`. The method first checks `order_index` (resting orders), then falls back to `stop_index`. A cancelled stop emits `Cancelled` with the full original `quantity` as `remaining_quantity`.

---

## Cancel-Replace (Order Amendment)

The `Exchange::cancel_replace()` method atomically amends a resting limit order's price and/or quantity. It operates only on resting limit orders -- stops, market orders, and unknown order IDs are rejected.

### Validation sequence

All checks run before any mutation. If any check fails, the original order is untouched:

1. **Instrument exists** -- rejects with `UnknownSymbol`.
2. **Order exists on the book** -- `get_resting_order(order_id)` must return a resting limit order. Rejects with `UnknownOrder`.
3. **Circuit breaker** -- the new price is checked against the instrument's `CircuitBreakerConfig` (halt status and price bands). Rejects with `TradingHalted` or `OutsidePriceBand`.
4. **Risk limits** -- the new quantity and notional (`new_price * new_quantity`) are checked against `RiskLimits`. Rejects with `ExceedsMaxOrderQty` or `ExceedsMaxNotional`.
5. **Price-would-cross** -- if the new price would cross the opposite best price (buy price >= best ask, or sell price <= best bid), the amendment is rejected with `PriceWouldCross`. The rationale: replacements must not become aggressors. To cross the spread, cancel and submit a new order.
6. **Reservation adjustment** -- `try_adjust_reservation()` computes the new required amount (including fee cushion for buys) and checks the account has sufficient balance. Rejects with `InsufficientBalance`.

### Time priority rules

Implemented in `OrderBook::replace_order()`:

| Scenario | Priority |
|---|---|
| Same price, quantity decrease (or unchanged) | **Preserved** -- in-place update at current queue position |
| Same price, quantity increase | **Lost** -- removed from current position and pushed to back of the price level's VecDeque |
| Price change (any direction) | **Lost** -- removed from old price level, added to back of new price level |

### Output

On success, emits `ExecutionReport::Replaced` with `old_price`, `new_price`, `old_remaining`, and `new_remaining`.

---

## Self-Trade Prevention

Each order carries an `stp: SelfTradeProtection` field (default: `CancelNewest`). When an incoming taker order would match against a resting maker order from the **same `AccountId`**, the STP mode determines what happens.

STP is checked inside `match_against()` before each fill. The check is `stp != Allow && maker.account == taker_account`.

### Modes

#### `Allow`

No prevention. Self-trades execute normally, producing `Fill` reports.

#### `CancelNewest` (default)

The incoming **taker** order is cancelled. The resting maker stays on the book. Matching halts immediately (`break 'outer`), and `stp_cancelled` is set to `true`. The caller then emits a `Cancelled` report for the taker's remaining quantity.

This is the safest default: it prevents accidental wash trades without disturbing existing resting orders.

#### `CancelOldest`

The resting **maker** order is cancelled (removed from the book, `Cancelled` report emitted). The taker **continues matching** against the next order at the same or subsequent price levels. Multiple maker orders from the same account may be cancelled in sequence as the taker sweeps through.

#### `CancelBoth`

Both the resting maker and the incoming taker are cancelled. The maker is removed and a `Cancelled` report is emitted. The method returns immediately with `stp_cancelled = true`, and the caller cancels the taker's remainder.

### Interaction with FOK pre-check

When STP is active (any mode except `Allow`), the FOK pre-check passes `exclude_account: Some(order.account)` to `available_quantity()`. This excludes same-account orders from the quantity tally, since they would not produce fills. Without this adjustment, a FOK order could pass the pre-check but then fail to fill due to STP cancellations.

### Interaction with partial fills

STP is checked per-maker-order, not per-price-level. With `CancelOldest`, a taker can partially fill against other accounts' orders, cancel a same-account maker, then continue filling. With `CancelNewest` or `CancelBoth`, any prior fills from earlier price levels are kept -- only the taker's **remaining** quantity is cancelled.

---

## Execution Reports

The `ExecutionReport` enum represents all events emitted by the matching engine. Each variant and when it is produced:

### `Fill`

Emitted for every individual trade between a maker and taker. Fields:

- `maker_order_id`, `taker_order_id` -- the two matched orders.
- `maker_account`, `taker_account` -- account identifiers for balance updates.
- `price` -- the maker's resting price (the price at which the trade executes).
- `quantity` -- the fill quantity (lots traded).
- `maker_fee`, `taker_fee` -- fees in quote currency, set to 0 by the order book and populated by `apply_fees()` in the Exchange layer based on the instrument's `FeeSchedule`.

A single incoming order can produce multiple `Fill` reports if it sweeps across price levels.

### `Placed`

Emitted when an unfilled GTC limit order (or its remainder after partial fills) is added to the book. Fields: `order_id`, `side`, `price`, `quantity` (the resting quantity, which may be less than the original if partially filled).

### `Triggered`

Emitted when a pending stop order's trigger condition is met. Fields: `order_id`, `trigger_price`. Always followed by the execution reports from the converted market/limit order (fills, placed, cancelled, or rejected).

### `Cancelled`

Emitted when:
- An IOC/FOK order has unfilled remainder.
- A market order has unfilled remainder (liquidity exhausted or budget exhausted).
- STP cancels a maker (`CancelOldest`, `CancelBoth`) or terminates a taker (`CancelNewest`, `CancelBoth`).
- An explicit `cancel()` call removes a resting or stop order.
- A `cancel_all_for_account()` kill switch removes all of an account's orders.

Fields: `order_id`, `account`, `remaining_quantity`.

### `Rejected`

Emitted when an order fails pre-execution validation. The order produces no other reports. Fields: `order_id`, `account`, `reason: RejectReason`.

Rejection reasons:
- `NoLiquidity` -- market order on an empty opposite side.
- `FOKCannotFill` -- FOK pre-check failed (insufficient available quantity).
- `InsufficientBalance` -- account cannot cover the reservation.
- `UnknownAccount` -- account not registered.
- `UnknownSymbol` -- instrument not registered.
- `SelfTradePrevented` -- (reserved; STP currently uses `Cancelled` rather than `Rejected`).
- `DuplicateOrderId` -- `(account, order.id)` already names a currently-live order. Reuse after the original closes is permitted.
- `ExceedsMaxOrderQty` -- quantity exceeds `RiskLimits::max_order_qty`.
- `ExceedsMaxNotional` -- price * quantity exceeds `RiskLimits::max_order_notional`.
- `TradingHalted` -- instrument's circuit breaker `halted` flag is set.
- `OutsidePriceBand` -- limit/stop-limit price outside `[price_band_lower, price_band_upper]`.
- `UnknownOrder` -- cancel-replace target not found on the book.
- `PriceWouldCross` -- cancel-replace new price crosses the opposite best price.
- `PostOnlyWouldCross` -- post-only limit order would immediately match.
- `HasRestingOrders` -- withdrawal rejected because the account has resting orders.
- `DuplicateRequest` -- per-key request sequence already processed.
- `ReplicaDisconnected` -- replication enabled but replica disconnected; state-mutating operations blocked.
- `InvalidExpiry` -- GTD order missing expiry, or non-GTD order with unexpected expiry.
- `InstrumentDisabled` -- instrument is disabled; no new orders accepted.

### `Replaced`

Emitted on successful cancel-replace. Fields: `order_id`, `side`, `old_price`, `new_price`, `old_remaining`, `new_remaining`.

### `InstrumentStatusChanged`

Emitted when an instrument's lifecycle status changes. Fields: `symbol`, `status: InstrumentStatus` (Enabled, Disabled, or Removed).

---

## Instrument Lifecycle

Instruments go through three states: **Enabled** (default, accepts orders), **Disabled** (rejects new orders), and **Removed** (permanent, memory reclaimed).

### DisableInstrument

Disables an instrument. All resting orders and pending stops are cancelled (each emitting a `Cancelled` report), followed by an `InstrumentStatusChanged { status: Disabled }` report. New order submissions for the instrument are rejected with `InstrumentDisabled`.

### EnableInstrument

Re-enables a previously disabled instrument. Emits `InstrumentStatusChanged { status: Enabled }`. New orders are accepted again.

### RemoveInstrument

Permanently removes a disabled instrument and reclaims its memory. Only succeeds if the instrument is currently disabled and has no resting orders. Emits `InstrumentStatusChanged { status: Removed }`. After removal, the symbol is unknown -- submissions are rejected with `UnknownSymbol`.

### EndOfDay

Cancels all resting orders and pending stops with `TimeInForce::Day` across all instruments. Each cancelled order emits a `Cancelled` report.

### ExpireOrders

Cancels all resting orders and pending stops with `TimeInForce::GTD` whose `expiry_ns <= timestamp_ns`. Each cancelled order emits a `Cancelled` report.

---

## Edge Cases

### Empty book

A market order submitted when the opposite side has no resting orders is rejected with `NoLiquidity`. The check `opposite.is_empty()` runs before `match_against()`. This also applies to triggered stop-market orders if the book has been drained by the time they execute.

### FOK pre-check

The FOK pre-check sums `available_quantity()` across all matchable price levels on the opposite side. For limit FOK, only prices within the limit are counted. For market FOK, all prices are counted. If the sum is less than the order's `quantity`, the order is rejected with `FOKCannotFill` and no fills or cancellations occur.

The pre-check accounts for STP by excluding same-account orders when STP is anything other than `Allow`.

### Market order budget exhaustion

A buy-side market order may exhaust its `quote_budget` mid-sweep. At each fill, the matcher checks whether the budget can afford the fill quantity at the current price level. If `affordable == 0` (cannot buy even 1 lot), matching stops. The unfilled remainder is cancelled. The Exchange layer releases any leftover reservation after execution.

### Price bands on market and stop orders

Circuit breaker price bands (`price_band_lower`, `price_band_upper`) apply only to orders with a known submission-time price: `Limit` and `StopLimit`. Market and Stop orders bypass price band checks because they have no price at submission. A market order can therefore fill at prices outside the bands. The mitigation is to use the `halted` flag for full trading halts.

### Duplicate order ID detection

The Exchange tracks the set of currently-live `(account, order_id)` pairs and rejects a submission with `DuplicateOrderId` whenever the pair is already in the set. Entries are added when an order is accepted by the matching engine and removed when it closes (full fill, cancel, expiry, instrument disable, end-of-session). Reuse of an `OrderId` after the original closes is permitted -- the dedup defends the cancel/replace lookup invariant ("no two simultaneously-live orders share `(account, order_id)`") rather than burning IDs forever. Replay-side idempotency is handled separately by `(key_hash, request_seq)` at the transport layer.

### Triggered stop cascade

When a fill updates `last_trade_price`, `check_triggers()` may fire multiple stops. Each triggered stop re-enters the matching pipeline and may itself produce fills that update `last_trade_price`. However, `check_triggers()` is called only once per top-level `execute()` call -- triggered orders call `execute_limit` / `execute_market` directly without re-invoking `check_triggers()`. This means a cascade of stop triggers within a single `execute()` call is limited to one level deep. Stops whose trigger conditions are met by fills from other triggered stops will fire on the next incoming order.
