# Maker/Taker Fee Model

Fees are configured per instrument in basis points (bps, where 100 bps = 1%). If no fee schedule is set, both rates default to zero. Fee schedules can be changed at runtime via `SetFeeSchedule`; changes apply to all subsequent fills, including fills on orders placed before the change.

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

Because the fee cushion is computed at order placement using the fee schedule active at that moment, a fee increase could theoretically cause a resting order's cushion to be insufficient. This is handled by saturating arithmetic in the balance manager — the reservation covers what it can, and any shortfall is absorbed.
