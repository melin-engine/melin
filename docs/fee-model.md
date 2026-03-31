# Maker/Taker Fee Model

Fees are configured per instrument in basis points (bps, where 100 bps = 1%). Both maker and taker fees are signed (`i16`, range -10000 to 10000): positive values are fees charged to the trader, negative values are rebates paid by the exchange. If no fee schedule is set, both rates default to zero. Fee schedules can be changed at runtime via `SetFeeSchedule`; changes apply to all subsequent fills, including fills on orders placed before the change.

Collected fees are credited to a dedicated **fee collection account** (`AccountId(0)`). This account is never evicted and always exists. Operators can withdraw accumulated fees via the Withdraw command.

## Maker vs Taker

- **Maker**: the resting order already on the book. Adds liquidity.
- **Taker**: the incoming order that crosses the spread. Removes liquidity.

## Fee Currency

Both maker and taker fees are charged in the **quote currency**, matching the standard centralized exchange model:

- **Buyer** pays `cost + fee` in quote currency.
- **Seller** receives `cost - fee` in quote currency.

## Fee Cushion (Buy-Side Reservation)

When a buy limit order is placed, the engine reserves `price * quantity` in quote currency. But the fee is also charged in quote currency at fill time. If the order fills at the exact limit price, there would be no room for the fee.

The reservation therefore includes a fee cushion computed at `max(maker_fee_bps, taker_fee_bps)` — the maximum is used because the engine doesn't know at placement time whether the order will rest (maker) or match immediately (taker). This guarantees the reservation covers the fee regardless of role.

The cushion uses the same truncating integer division as fee computation, so `cushion >= actual_fee` is guaranteed for any fill at or below the limit price.

**Market buys** don't need a cushion — they reserve the entire available quote balance. **Sell orders** don't need one either — their fee is deducted from quote proceeds, not from a base currency reservation.

## Dynamic Fee Schedule Changes

Because the fee cushion is computed at order placement using the fee schedule active at that moment, a fee schedule increase could cause a resting buy order's cushion to be insufficient for the new fee. The exchange absorbs the shortfall via saturating arithmetic — the buyer pays `min(actual_fee, cushion_at_placement_time)` rather than the full configured fee rate. There is no log or audit trail entry for the shortfall.

**Recommendation**: To avoid silent fee shortfalls, increase fee schedules only when no resting buy orders exist for the instrument (e.g., after a trading halt and CancelAll).
