# Maker/Taker Fee Model

This document describes the trading engine's fee model: how fees are configured, computed, reserved, and charged.

## Fee Schedule

Fees are configured per instrument via the `FeeSchedule` struct:

```rust
pub struct FeeSchedule {
    pub maker_fee_bps: u16,  // 0-10000
    pub taker_fee_bps: u16,  // 0-10000
}
```

Both fields are in **basis points** (bps). One basis point equals 0.01%, so 10,000 bps = 100%. A value of `10` means 0.10%; a value of `0` means no fee.

If no fee schedule is set for an instrument, both rates default to zero.

## Maker vs Taker Identification

- **Maker**: the resting order already on the book when a match occurs. Makers add liquidity.
- **Taker**: the incoming (aggressive) order that crosses the spread and triggers the match. Takers remove liquidity.

In an `ExecutionReport::Fill`, the `maker_order_id`/`maker_account` fields identify the maker and `taker_order_id`/`taker_account` identify the taker.

## Fee Computation

Fees are computed by the `apply_fees` function after the matching engine produces fill reports and before balance updates are applied. The formula for each fill is:

```
cost       = price * quantity           (u128 intermediate to avoid overflow)
maker_fee  = cost * maker_fee_bps / 10_000   (truncating integer division)
taker_fee  = cost * taker_fee_bps / 10_000   (truncating integer division)
```

The multiplication uses `u128` to prevent overflow (two `u64` values multiplied together). The final fee is truncated back to `u64`. Truncation means fees always round down in the trader's favor.

If both `maker_fee_bps` and `taker_fee_bps` are zero, the function returns immediately without iterating over reports (fast path).

### Fee Charging Currency

Both maker and taker fees are charged in the **quote currency** (cost-based), not the received currency. This matches the standard model used by most centralized exchanges.

- **Buyer's fee**: deducted from the buyer's quote currency reservation alongside the trade cost.
- **Seller's fee**: deducted from the seller's quote currency proceeds.

Concretely, for a BTC/USD instrument where the buyer pays USD and receives BTC:
- The buyer pays `cost + buyer_fee` in USD.
- The seller receives `cost - seller_fee` in USD.

## Fee Cushion (Buy-Side Reservation)

### The Problem

When a buy-side limit order is placed, the engine reserves `price * quantity` in quote currency. But when the order eventually fills, a fee is also charged in quote currency. If the fill happens at the exact limit price, there would be no room in the reservation to cover the fee.

### The Solution

For buy-side limit and stop-limit orders, the reservation includes a **fee cushion**:

```
max_fee_bps = max(maker_fee_bps, taker_fee_bps)
cost        = price * quantity
fee_cushion = cost * max_fee_bps / 10_000
reservation = cost + fee_cushion
```

The cushion uses `max(maker_fee_bps, taker_fee_bps)` because at reservation time, the engine does not know whether the order will be a maker (resting) or taker (immediately matching). Using the maximum guarantees the reservation covers the fee regardless of which role the order takes.

The fee cushion formula uses the same rounding direction (truncating division) as `apply_fees`, which guarantees `fee_cushion >= actual_fee` for any fill at or below the limit price.

### When the Cushion Is Released

After a fill, the reservation is reduced by `cost + actual_fee`. If the actual fee is less than the cushion (because the order acted as maker with a lower fee rate, or because of rounding), the excess remains in the reservation. It is released back to available balance when:

- The order is fully filled (reservation reaches zero and is removed).
- The order is cancelled (remaining reservation released via `release()`).

### Market and Stop-Market Buys

Market and stop-market buy orders do **not** need a fee cushion because they reserve the **entire available quote balance**. Since the full balance is already locked, fees are always covered. The unused portion is refunded after execution.

### Sell-Side Orders

Sell orders reserve `quantity` in the **base** currency. Their fee is deducted from their quote currency proceeds (not from a reservation), so no cushion is needed.

## Balance Flow with Fees

### Step-by-step for a buy-side limit order fill

1. **Reserve** (at order placement): lock `cost + fee_cushion` in quote currency.
2. **Fill** (at match time):
   - Buyer: deduct `cost + buyer_fee` from quote reservation; credit `quantity` to base available.
   - Seller: deduct `quantity` from base reservation; credit `cost - seller_fee` to quote available.
3. **Release** (on cancel or full fill): return any remaining reservation to available balance.

### Step-by-step for a sell-side limit order fill

1. **Reserve** (at order placement): lock `quantity` in base currency.
2. **Fill** (at match time):
   - Seller: deduct `quantity` from base reservation; credit `cost - seller_fee` to quote available.
   - Buyer: deduct `cost + buyer_fee` from quote reservation; credit `quantity` to base available.
3. **Release** (on cancel or full fill): return any remaining base reservation to available balance.

## Cancel-Replace and Fees

When a cancel-replace changes the price or quantity of a resting buy order, the engine recomputes the required reservation including the fee cushion at the new price and quantity. The reservation is adjusted atomically: if the new amount is higher and the account lacks sufficient available balance, the replace is rejected and the original order remains intact.

## Configuration

Fee schedules are set via the `SetFeeSchedule` admin command, which calls `Exchange::set_fee_schedule(symbol, schedule)`. Key properties:

- **Per-instrument**: each instrument (symbol) has its own independent fee schedule.
- **Dynamic**: changes take effect on subsequent fills. Orders already resting on the book will use the new fee rates when they fill, not the rates that were active when they were placed.
- **Persistent**: fee schedules are included in journal snapshots and restored on recovery.

Note: because the fee cushion is computed at order placement time using the fee schedule active at that moment, a fee schedule increase could theoretically cause a resting buy order's reservation to be insufficient for the new fee. In practice this is handled by the `saturating_sub`/`saturating_add` arithmetic in the balance manager -- the reservation covers whatever it can, and any shortfall is absorbed.

## Worked Examples

### Example 1: Standard maker/taker fill

**Setup**: BTC/USD instrument, `maker_fee_bps = 10` (0.10%), `taker_fee_bps = 20` (0.20%).

1. Account A deposits 50,000 USD.
2. Account B deposits 100 BTC.
3. Account A places a limit buy: price = 1,000, quantity = 10 (GTC).
   - `cost = 1,000 * 10 = 10,000`
   - `max_fee_bps = max(10, 20) = 20`
   - `fee_cushion = 10,000 * 20 / 10,000 = 20`
   - **Reservation = 10,020 USD** (moved from available to reserved)
   - A's USD: available = 39,980, reserved = 10,020
4. Account B places a limit sell: price = 1,000, quantity = 10 (GTC).
   - Reservation = 10 BTC (base currency, no cushion needed)
   - B's BTC: available = 90, reserved = 10
5. **Match**: A's buy (maker, resting) fills against B's sell (taker, incoming) at price 1,000 for 10.
   - `cost = 10,000`
   - `maker_fee = 10,000 * 10 / 10,000 = 10 USD`
   - `taker_fee = 10,000 * 20 / 10,000 = 20 USD`
6. **Balance updates**:
   - A (buyer/maker): quote reservation decreases by `10,000 + 10 = 10,010`. Leftover cushion = `10,020 - 10,010 = 10` released to available. Base available increases by 10 BTC.
     - A's USD: available = 39,990, reserved = 0
     - A's BTC: available = 10
   - B (seller/taker): base reservation decreases by 10 BTC. Quote available increases by `10,000 - 20 = 9,980` USD.
     - B's USD: available = 9,980
     - B's BTC: available = 90, reserved = 0

**Net fees collected**: 10 (maker) + 20 (taker) = 30 USD.

### Example 2: Fee cushion at a rounding boundary

**Setup**: BTC/USD instrument, `maker_fee_bps = 3` (0.03%), `taker_fee_bps = 3` (0.03%).

1. Account A deposits exactly 15,004 USD.
2. Account A places a limit buy: price = 150, quantity = 100.
   - `cost = 150 * 100 = 15,000`
   - `fee_cushion = 15,000 * 3 / 10,000 = 4` (note: `15,000 * 3 = 45,000; 45,000 / 10,000 = 4` with truncation)
   - **Reservation = 15,004 USD** (exactly equal to available balance)
3. On fill:
   - `maker_fee = 15,000 * 3 / 10,000 = 4 USD`
   - Deducted from reservation: `15,000 + 4 = 15,004`
   - Leftover: 0. A's USD available = 0.

The cushion formula guarantees exact coverage because it uses the same `cost * bps / 10,000` rounding as `apply_fees`.

### Example 3: Dynamic fee schedule change

1. First trade executes with no fee schedule set (0/0). No fees charged.
2. Admin sets `maker_fee_bps = 50, taker_fee_bps = 100`.
3. Second trade at price = 1,000, quantity = 10:
   - `maker_fee = 10,000 * 50 / 10,000 = 50 USD`
   - `taker_fee = 10,000 * 100 / 10,000 = 100 USD`

The new fee schedule applies immediately to all subsequent fills, including fills on orders that were placed before the fee change.

### Example 4: Market buy with fees

1. Account A has 10,000 USD. Places a market buy for 10 units.
   - Reservation = **10,000 USD** (entire available balance; no cushion computation).
2. Fills at price 100 for 10 units: cost = 1,000. Taker fee at 20 bps = 2.
   - Reservation reduced by `1,000 + 2 = 1,002`.
3. Market order completes. Remaining reservation `10,000 - 1,002 = 8,998` released.
   - A's USD: available = 8,998, reserved = 0.
   - A's BTC: available = 10.
