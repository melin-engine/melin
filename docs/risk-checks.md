# Risk Management Controls

This document describes the risk checks and safety mechanisms enforced by the trading engine before and during order processing. All checks run on the single-threaded matching engine hot path (no locks, no I/O).

## Validation Chain

Every order submitted via `Exchange::execute()` passes through a strict validation chain. Checks run in order; the first failure rejects the order immediately and no subsequent checks execute.

1. **Instrument lookup** -- reject with `UnknownSymbol` if the symbol is not registered.
2. **Client deduplication** -- reject with `DuplicateOrderId` if the order ID is not strictly greater than the account's high-water mark.
3. **Circuit breaker** -- reject with `TradingHalted` if the instrument is halted; reject with `OutsidePriceBand` if the limit price falls outside configured bands.
4. **Fat finger checks** -- reject with `ExceedsMaxOrderQty` or `ExceedsMaxNotional` if the order exceeds per-instrument risk limits.
5. **Balance reservation** -- reject with `InsufficientBalance` if the account cannot cover the required reserve amount.

Only after all five checks pass does the order reach the matching engine.

## Client Deduplication

The exchange tracks a per-account **high-water mark** (HWM) of the highest `OrderId` seen. An incoming order is rejected with `DuplicateOrderId` if `order.id <= hwm[account]`.

The HWM advances unconditionally on every submission, regardless of whether the order is later rejected by a downstream check (e.g., insufficient balance). This is intentional: the journal records every `SubmitOrder` event, so on crash-recovery replay, a rejected order must not be re-executed. Clients must use a new (higher) `OrderId` for every submission, even if the previous one was rejected.

`OrderId` is a client-assigned `u64`. The monotonic-increase requirement is the only constraint -- clients may use any scheme (sequential counter, timestamp-based, etc.) as long as each new order for the same account has a strictly higher ID than all previous submissions.

## Circuit Breakers

Circuit breaker configuration is per-instrument via `CircuitBreakerConfig`:

| Field | Type | Description |
|---|---|---|
| `halted` | `bool` | When `true`, all new orders for this instrument are rejected with `TradingHalted`. |
| `price_band_lower` | `Option<Price>` | Inclusive lower bound for limit order prices. `None` = no lower bound. |
| `price_band_upper` | `Option<Price>` | Inclusive upper bound for limit order prices. `None` = no upper bound. |

### Which order types are checked

- **Limit** and **StopLimit** orders: the limit price (for StopLimit, the `limit_price` field, not `trigger_price`) is checked against `[price_band_lower, price_band_upper]`. Rejected with `OutsidePriceBand` if the price is strictly below the lower bound or strictly above the upper bound.
- **Market** and **Stop** orders: bypass price band checks entirely because they have no submission-time price. A large market order can fill far outside the intended bands. To prevent this, use the `halted` flag or implement automatic volatility halts.

### Trading halt

When `halted = true`, all order types are rejected -- Market, Limit, Stop, and StopLimit alike. This is the kill switch for an instrument. Existing resting orders remain on the book but no new orders can enter.

## Fat Finger Checks

Fat finger limits are per-instrument via `RiskLimits`:

| Field | Type | Description |
|---|---|---|
| `max_order_qty` | `Option<Quantity>` | Maximum order quantity in lots. Rejects if `quantity > max_order_qty`. |
| `max_order_notional` | `Option<u64>` | Maximum order notional value (price * quantity) in ticks. |

Both fields are optional. `None` means "no limit" -- unconfigured instruments pass all fat finger checks.

### Quantity check

Applies to all order types. If `order.quantity > max_order_qty`, the order is rejected with `ExceedsMaxOrderQty`.

### Notional check

Notional is computed as `price * quantity` using `u128` arithmetic to avoid overflow on the `u64 * u64` multiplication. The result is compared against the configured `max_order_notional` ceiling (stored as `u64`).

The notional check applies only to order types with a known price at submission time:

- **Limit**: uses the limit price.
- **StopLimit**: uses the `limit_price` (worst-case resting price).
- **Market** and **Stop**: skip the notional check because they have no submission-time price.

Rejected with `ExceedsMaxNotional` if `price * quantity > max_order_notional`.

## Balance Reservation

After all risk checks pass, the engine attempts to reserve funds via `AccountManager::try_reserve()`. If the account has insufficient available balance, the order is rejected with `InsufficientBalance`.

### Reserve semantics by order type and side

| Side | Order Type | Reserved Currency | Reserved Amount |
|---|---|---|---|
| Buy | Limit | Quote | `price * quantity` (plus fee cushion) |
| Buy | StopLimit | Quote | `limit_price * quantity` (plus fee cushion) |
| Buy | Market | Quote | Entire available quote balance |
| Buy | Stop | Quote | Entire available quote balance |
| Sell | Limit | Base | `quantity` |
| Sell | StopLimit | Base | `quantity` |
| Sell | Market | Base | `quantity` |
| Sell | Stop | Base | `quantity` |

**Buy limit/stop-limit** reservations include a fee cushion: `cost * max_fee_bps / 10_000`, where `max_fee_bps` is the higher of the instrument's maker and taker fee rates. This guarantees fees can always be charged from the reservation even when filling at the exact limit price.

**Buy market/stop** orders reserve the entire available quote balance because the final fill price is unknown at submission time. The unused portion is released after execution.

**Sell** orders always reserve the order quantity in base currency, regardless of order type.

If `price * quantity` overflows `u64` (after fee cushion), the order is rejected with `InsufficientBalance`.

## Kill Switch (CancelAll)

The `CancelAll` command cancels all resting orders **and** pending stop orders for a given account across **all instruments**. For each cancelled order, the associated balance reservation is released and a `Cancelled` execution report is emitted.

This is the account-level emergency kill switch. It iterates over every order book in the exchange, calling `cancel_all_for_account()` on each.

`CancelAll` is journaled as a single event. On crash-recovery replay, it re-executes identically.

## Cancel-Replace Validation

`Exchange::cancel_replace()` atomically amends a resting limit order's price and/or quantity. It re-validates the new values through a subset of the risk checks before mutating anything:

1. **Instrument exists** -- reject with `UnknownSymbol` if not found.
2. **Order exists on the book** -- reject with `UnknownOrder` if the order is not a resting limit order (stops and market orders cannot be amended).
3. **Circuit breaker** -- the new price is checked against `halted` and price bands, same rules as new order submission.
4. **Risk limits** -- the new quantity and new notional (`new_price * new_quantity`) are checked against `max_order_qty` and `max_order_notional`.
5. **Cross-price check** -- reject with `PriceWouldCross` if the new price would cross the opposite best price (buy price >= best ask, or sell price <= best bid). To aggress, the client must cancel and submit a new order.
6. **Reservation adjustment** -- the new required reserve amount is computed (including fee cushion for buys). If the new amount exceeds the old reservation and the account has insufficient available balance for the delta, reject with `InsufficientBalance`. If the new amount is less than or equal to the old amount, the excess is released back to available balance.

If any check fails, the original order remains untouched on the book with its original price, quantity, and queue priority.

### Time priority rules

- Same price, quantity decrease: keep queue priority.
- Same price, quantity increase: lose queue priority (moved to back of price level).
- Price change: lose queue priority.

## RejectReason Variants

| Variant | Meaning |
|---|---|
| `DuplicateOrderId` | Order ID is not strictly greater than the account's high-water mark. Prevents double-execution on crash-recovery retry. |
| `TradingHalted` | Circuit breaker: the instrument is halted. |
| `OutsidePriceBand` | Circuit breaker: the limit price is outside the configured `[lower, upper]` bounds. |
| `ExceedsMaxOrderQty` | Fat finger: order quantity exceeds the instrument's `max_order_qty`. |
| `ExceedsMaxNotional` | Fat finger: order notional (price * quantity) exceeds the instrument's `max_order_notional`. |
| `InsufficientBalance` | Account does not have sufficient available balance to cover the reservation. |
| `UnknownSymbol` | The instrument is not registered. |
| `UnknownAccount` | The account is not registered (returned by withdraw; orders get `InsufficientBalance`). |
| `UnknownOrder` | Cancel or cancel-replace target order not found on the book. |
| `PriceWouldCross` | Cancel-replace: new price would cross the opposite best price. |
| `NoLiquidity` | Market order with no liquidity on the opposite side. |
| `FOKCannotFill` | Fill-or-Kill order cannot be fully filled. |
| `SelfTradePrevented` | Self-trade prevention triggered: order would match against the same account. |

## Configuration

Risk controls are configured per-instrument through admin commands, journaled for crash recovery:

- **`SetRiskLimits { symbol, limits }`** -- sets or updates `RiskLimits` (max_order_qty, max_order_notional) for an instrument. `None` fields clear the corresponding limit.
- **`SetCircuitBreaker { symbol, config }`** -- sets or updates `CircuitBreakerConfig` (price_band_lower, price_band_upper, halted) for an instrument.

Both commands are available through the admin CLI and the wire protocol. They take effect immediately on the next order submission -- no restart required. Changes are persisted in the journal and restored on recovery.

Unconfigured instruments (no `SetRiskLimits` or `SetCircuitBreaker` issued) pass all risk checks by default. There are no global/exchange-wide risk limits; all controls are per-instrument.
