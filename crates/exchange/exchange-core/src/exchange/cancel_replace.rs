//! Atomic price/quantity amendment for a resting limit order. Pulled
//! into its own submodule because the validation pipeline (instrument,
//! circuit-breaker, risk, would-cross, reservation) is ~175 lines on
//! its own and parallels the structure of `execute.rs`.

use super::Exchange;
use super::instrument::{inst_mut, inst_ref};
use crate::types::{
    AccountId, ExecutionReport, OrderId, Price, Quantity, RejectReason, Side, Symbol,
};

/// Compute the required reservation for a buy-side order at a known
/// price: `price * qty` in quote currency. Pure notional — fees are
/// settled from the fill's received asset (industry-standard model),
/// not from the reservation. Uses u128 to avoid overflow.
#[inline]
fn required_notional(price: u64, qty: u64) -> u64 {
    let cost = price as u128 * qty as u128;
    // Saturate to u64::MAX — identical to try_reserve behavior.
    cost.min(u64::MAX as u128) as u64
}

impl Exchange {
    /// Atomically amend a resting limit order's price and/or quantity.
    ///
    /// Validation order (all checks before any mutation):
    /// 1. Instrument exists
    /// 2. Order exists on the book (resting limit only — not stops, not market)
    /// 3. Circuit breaker: halted or price band violation
    /// 4. Risk limits: max qty, max notional
    /// 5. Price-would-cross check: reject if new price crosses the spread
    /// 6. Reservation adjustment: compute new required amount, check balance
    ///
    /// If any check fails, the original order remains untouched.
    ///
    /// Time priority rules:
    /// - Same price, qty decrease → keep priority
    /// - Same price, qty increase → lose priority
    /// - Price change → lose priority
    pub fn cancel_replace(
        &mut self,
        symbol: Symbol,
        account: AccountId,
        order_id: OrderId,
        new_price: Price,
        new_quantity: Quantity,
        reports: &mut Vec<ExecutionReport>,
    ) {
        // Single lookup for all instrument state — O(1) Vec index, no hashing.
        let Some(inst) = inst_ref(&self.instruments, symbol) else {
            reports.push(ExecutionReport::Rejected {
                order_id,
                symbol,
                account,
                reason: RejectReason::UnknownSymbol,
            });
            return;
        };

        // Disabled instruments reject cancel-replace — all orders were
        // already cancelled during disable.
        if inst.disabled {
            reports.push(ExecutionReport::Rejected {
                order_id,
                symbol,
                account,
                reason: RejectReason::InstrumentDisabled,
            });
            return;
        }

        // 1. Order must exist as a resting limit order.
        // Use peek_order_location (O(1) index lookup) for validation —
        // the VecDeque scan for old_remaining is deferred to replace_order.
        let Some((side, _old_price, slot)) = inst.book.peek_order_location(account, order_id)
        else {
            reports.push(ExecutionReport::Rejected {
                order_id,
                symbol,
                account,
                reason: RejectReason::UnknownOrder,
            });
            return;
        };

        // 2. Circuit breaker checks on the new price.
        let cb = &inst.circuit_breaker;
        if cb.halted {
            reports.push(ExecutionReport::Rejected {
                order_id,
                symbol,
                account,
                reason: RejectReason::TradingHalted,
            });
            return;
        }
        if let Some(lower) = cb.price_band_lower
            && new_price < lower
        {
            reports.push(ExecutionReport::Rejected {
                order_id,
                symbol,
                account,
                reason: RejectReason::OutsidePriceBand,
            });
            return;
        }
        if let Some(upper) = cb.price_band_upper
            && new_price > upper
        {
            reports.push(ExecutionReport::Rejected {
                order_id,
                symbol,
                account,
                reason: RejectReason::OutsidePriceBand,
            });
            return;
        }

        // 3. Risk limit checks on the new quantity/notional.
        let limits = &inst.risk_limits;
        if let Some(max_qty) = limits.max_order_qty
            && new_quantity.get() > max_qty.get()
        {
            reports.push(ExecutionReport::Rejected {
                order_id,
                symbol,
                account,
                reason: RejectReason::ExceedsMaxOrderQty,
            });
            return;
        }
        if let Some(max_notional) = limits.max_order_notional {
            let notional = new_price.get() as u128 * new_quantity.get() as u128;
            if notional > max_notional as u128 {
                reports.push(ExecutionReport::Rejected {
                    order_id,
                    symbol,
                    account,
                    reason: RejectReason::ExceedsMaxNotional,
                });
                return;
            }
        }

        // 4. Reject if the new price would cross the opposite best price.
        // This prevents the replacement from becoming an aggressor. If the
        // user wants to cross the spread, they should cancel and submit a
        // new order.
        let would_cross = match side {
            Side::Buy => inst
                .book
                .best_ask()
                .is_some_and(|best_ask| new_price >= best_ask),
            Side::Sell => inst
                .book
                .best_bid()
                .is_some_and(|best_bid| new_price <= best_bid),
        };
        if would_cross {
            reports.push(ExecutionReport::Rejected {
                order_id,
                symbol,
                account,
                reason: RejectReason::PriceWouldCross,
            });
            return;
        }

        // 5. Adjust reservation atomically. Compute the new required
        // amount as pure notional. If insufficient balance, the original
        // reservation stays intact.
        let new_required = match side {
            Side::Buy => required_notional(new_price.get(), new_quantity.get()),
            Side::Sell => new_quantity.get(),
        };

        // The reservation slot was already retrieved from peek_order_location above.
        if let Err(reason) = self.accounts.try_adjust_reservation(slot, new_required) {
            reports.push(ExecutionReport::Rejected {
                order_id,
                symbol,
                account,
                reason,
            });
            return;
        }

        // 6. All checks passed — perform the book replacement (single VecDeque
        // scan). This returns (old_price, old_remaining).
        // Cannot fail since we verified the order exists above and matching is
        // single-threaded (no concurrent removal possible).
        // Note: `live_order_ids` is intentionally not touched. The order keeps
        // the same `(account, order_id)` identity through the replacement, so
        // its entry stays valid; it'll be removed by the same cancel/fill
        // path as any other resting order when the order eventually closes.
        let inst =
            inst_mut(&mut self.instruments, symbol).expect("instrument verified to exist above");
        let (old_price, old_remaining) = inst
            .book
            .replace_order(account, order_id, new_price, new_quantity)
            .expect("order verified to exist");

        reports.push(ExecutionReport::Replaced {
            order_id,
            symbol,
            account,
            side,
            old_price,
            new_price,
            old_remaining,
            new_remaining: new_quantity,
        });
    }
}
