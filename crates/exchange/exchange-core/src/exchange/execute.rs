//! The order-submission hot path. Pulled into its own submodule because
//! it dominates `exchange.rs` by line count and is the most heavily
//! exercised path in the engine — keeping it isolated makes targeted
//! review and perf work easier.

use super::Exchange;
use super::instrument::{inst_mut, inst_ref};
use super::token_bucket::TokenBucket;
use crate::types::{ExecutionReport, Order, OrderType, RejectReason, Side, Symbol, TimeInForce};

impl Exchange {
    /// Submit an order to the matching engine for the given instrument.
    ///
    /// Validates the instrument exists, reserves funds, then executes.
    /// On fill, balances are updated. On reject/cancel, reserves are released.
    ///
    /// Under `feature = "skip-order-exec"` the body is short-circuited
    /// to a single `Rejected{NoLiquidity}` push, used by the server's
    /// transport-only benchmark build to isolate transport throughput
    /// from matching cost. Same wire shape — bench clients still see
    /// one response per `SubmitOrder` — but no order book / account
    /// state touched.
    #[inline]
    pub fn execute(&mut self, symbol: Symbol, order: Order, reports: &mut Vec<ExecutionReport>) {
        #[cfg(feature = "skip-order-exec")]
        {
            reports.push(ExecutionReport::Rejected {
                order_id: order.id,
                symbol,
                account: order.account,
                reason: RejectReason::NoLiquidity,
            });
            return;
        }
        #[cfg_attr(feature = "skip-order-exec", allow(unreachable_code))]
        let Some(inst) = inst_ref(&self.instruments, symbol) else {
            reports.push(ExecutionReport::Rejected {
                order_id: order.id,
                symbol,
                account: order.account,
                reason: RejectReason::UnknownSymbol,
            });
            return;
        };
        // Disabled instruments reject before HWM advance — the order is
        // never "processed", same as UnknownSymbol.
        if inst.disabled {
            reports.push(ExecutionReport::Rejected {
                order_id: order.id,
                symbol,
                account: order.account,
                reason: RejectReason::InstrumentDisabled,
            });
            return;
        }
        // Copy spec before taking mutable borrow on instruments below.
        // InstrumentSpec is Copy (3 × u32 = 12 bytes).
        let spec = inst.spec;

        // Dedup: reject if `(account, order_id)` already names a live
        // order. Cancel/replace look up by the same key, so two live
        // orders sharing it would make those operations ambiguous.
        // Replay-safety is provided one layer up by `check_request_seq`
        // (transport-level idempotency on `(key_hash, request_seq)`),
        // not here — duplicate journaled SubmitOrder events never reach
        // this point. Reuse of an `OrderId` after the original closes
        // is permitted by design.
        if self.live_order_ids.contains(&(order.account, order.id)) {
            reports.push(ExecutionReport::Rejected {
                order_id: order.id,
                symbol,
                account: order.account,
                reason: RejectReason::DuplicateOrderId,
            });
            return;
        }

        // Existence already established by the `let Some(inst) = inst_ref(...)
        // else { ... return; }` guard at the top of `execute`. The matcher
        // is single-threaded and no instrument deregistration runs between
        // events, so the slot is still populated here.
        let inst = inst_ref(&self.instruments, symbol).expect("instrument verified to exist above");

        // Circuit breaker checks: trading halt rejects all orders; price
        // bands reject limit/stop-limit orders outside [lower, upper].
        // No HashMap lookup — circuit breaker is in the same struct.
        let cb = &inst.circuit_breaker;
        if cb.halted {
            reports.push(ExecutionReport::Rejected {
                order_id: order.id,
                symbol,
                account: order.account,
                reason: RejectReason::TradingHalted,
            });
            return;
        }
        // Price band check applies only to orders with a known price.
        // Market and Stop orders have no submission-time price and
        // bypass bands by design (SEC-12). A large market order can
        // fill far outside the intended bands. Mitigation: use the
        // trading halt flag, or implement automatic volatility halts
        // (Phase 3 of the circuit breaker plan).
        let limit_price = match order.order_type {
            OrderType::Limit { price, .. } => Some(price),
            OrderType::StopLimit { limit_price, .. } => Some(limit_price),
            OrderType::Market | OrderType::Stop { .. } => None,
        };
        if let Some(price) = limit_price {
            if let Some(lower) = cb.price_band_lower
                && price < lower
            {
                reports.push(ExecutionReport::Rejected {
                    order_id: order.id,
                    symbol,
                    account: order.account,
                    reason: RejectReason::OutsidePriceBand,
                });
                return;
            }
            if let Some(upper) = cb.price_band_upper
                && price > upper
            {
                reports.push(ExecutionReport::Rejected {
                    order_id: order.id,
                    symbol,
                    account: order.account,
                    reason: RejectReason::OutsidePriceBand,
                });
                return;
            }
        }

        // Fat finger checks: reject orders exceeding per-instrument limits.
        let limits = &inst.risk_limits;
        if let Some(max_qty) = limits.max_order_qty
            && order.quantity.get() > max_qty.get()
        {
            reports.push(ExecutionReport::Rejected {
                order_id: order.id,
                symbol,
                account: order.account,
                reason: RejectReason::ExceedsMaxOrderQty,
            });
            return;
        }
        if let Some(max_notional) = limits.max_order_notional {
            // Notional check applies only to orders with a known price.
            // Market and Stop orders have no submission-time price.
            // StopLimit uses limit_price (worst-case resting price).
            let limit_price = match order.order_type {
                OrderType::Limit { price, .. } => Some(price),
                OrderType::StopLimit { limit_price, .. } => Some(limit_price),
                OrderType::Market | OrderType::Stop { .. } => None,
            };
            if let Some(price) = limit_price {
                let notional = price.get() as u128 * order.quantity.get() as u128;
                if notional > max_notional as u128 {
                    reports.push(ExecutionReport::Rejected {
                        order_id: order.id,
                        symbol,
                        account: order.account,
                        reason: RejectReason::ExceedsMaxNotional,
                    });
                    return;
                }
            }
        }

        // GTD validation: GTD orders must have a non-zero expiry, and
        // non-GTD orders must not carry an expiry timestamp.
        if order.time_in_force == TimeInForce::GTD && order.expiry_ns == 0 {
            reports.push(ExecutionReport::Rejected {
                order_id: order.id,
                symbol,
                account: order.account,
                reason: RejectReason::InvalidExpiry,
            });
            return;
        }
        if order.time_in_force != TimeInForce::GTD && order.expiry_ns != 0 {
            reports.push(ExecutionReport::Rejected {
                order_id: order.id,
                symbol,
                account: order.account,
                reason: RejectReason::InvalidExpiry,
            });
            return;
        }

        // Per-account open-order cap (SEC-03). Runs after every other
        // reject reason (UnknownSymbol, InstrumentDisabled, DuplicateOrderId,
        // TradingHalted, OutsidePriceBand, ExceedsMaxOrderQty,
        // ExceedsMaxNotional, InvalidExpiry) so an order that would have
        // been rejected for a venue-side or order-shape reason still
        // reports that reason — the cap is account-state, akin to
        // InsufficientBalance, and belongs adjacent to reservation.
        // Order: cap before reservation so a capped account doesn't churn
        // the slab. `order_counts` tracks (resting + pending stops +
        // in-flight) per account; `>=` rejects when accepting this order
        // would push the count past the limit. `0` = unlimited (opt-out).
        if self.max_open_orders_per_account > 0
            && self.order_counts.get(&order.account).copied().unwrap_or(0)
                >= self.max_open_orders_per_account
        {
            reports.push(ExecutionReport::Rejected {
                order_id: order.id,
                symbol,
                account: order.account,
                reason: RejectReason::ExceedsMaxOpenOrders,
            });
            return;
        }

        // Per-account order-submission rate limit (SEC-04). Token bucket
        // refilled at `max_orders_per_second`, capped at `max_orders_burst`,
        // metered against the journaled event timestamp
        // (`current_event_ts_ns`) so primary and replicas see identical
        // accept/reject decisions. Sits next to the open-orders cap above
        // because both are per-account policy gates that take effect
        // *before* any reservation work — a throttled order should not
        // perturb the slab or `order_counts`. Disabled when either knob
        // is `0`.
        if self.max_orders_per_second > 0 && self.max_orders_burst > 0 {
            let now_ns = self.current_event_ts_ns;
            let rate = self.max_orders_per_second;
            let burst = self.max_orders_burst;
            let bucket = self
                .order_buckets
                .entry(order.account)
                .or_insert_with(|| TokenBucket::new(burst, now_ns));
            if !bucket.refill_and_consume(now_ns, rate, burst) {
                reports.push(ExecutionReport::Rejected {
                    order_id: order.id,
                    symbol,
                    account: order.account,
                    reason: RejectReason::ExceedsOrderRate,
                });
                return;
            }
        }

        // Reserve pure notional (no fee cushion). Fees are settled from
        // the fill's received asset, not from this reservation, so a
        // schedule change after placement can never make the reservation
        // insufficient — by construction.
        let (reserved, slot) = match self.accounts.try_reserve(&order, &spec) {
            Ok(result) => result,
            Err(reason) => {
                reports.push(ExecutionReport::Rejected {
                    order_id: order.id,
                    symbol,
                    account: order.account,
                    reason,
                });
                return;
            }
        };

        // For buy-side market/stop-market orders, pass a cost budget so
        // the matching engine stops before exceeding the reservation. The
        // budget is exactly the reservation amount — no fee carve-out
        // needed since fees come out of the buyer's base credit, not the
        // quote reservation.
        let quote_budget = match (order.side, order.order_type) {
            (Side::Buy, OrderType::Market) | (Side::Buy, OrderType::Stop { .. }) => Some(reserved),
            _ => None,
        };

        *self.order_counts.entry(order.account).or_default() += 1;
        // Tentatively claim the (account, order_id) slot for the live
        // dedup check. If the order closes within this `execute` call
        // (IOC/FOK fill, FOK kill, etc.) the entry is freed in the
        // `freed` loop below; if it rests, the entry stays put.
        self.live_order_ids.insert((order.account, order.id));

        let taker_account = order.account;
        let taker_id = order.id;
        let report_start = reports.len();

        // Take scratch buffers out of `self` BEFORE the `inst_mut` borrow
        // below. `inst` mutably borrows `self.instruments` for the rest
        // of the function, so we can't touch `self.scratch_*` once it's
        // live. `mem::take` swaps with an empty Vec (no allocation —
        // `Vec::new()` is const) and the populated buffer is restored
        // at the end. Net effect: the inner loop has the same shape as
        // before but no per-event Vec allocation.
        //
        // The leading `clear()` calls are belt-and-braces: the put-back
        // at function end leaves the field empty, so under normal
        // control flow the take yields an already-empty Vec. The clear
        // only does work if a previous `execute` panicked between take
        // and put-back, leaving stale entries in the scratch.
        let mut consumed = std::mem::take(&mut self.scratch_consumed);
        consumed.clear();
        let mut freed = std::mem::take(&mut self.scratch_freed);
        freed.clear();

        // Single mutable lookup: book, fees all from the same struct.
        // Existence was established by the `inst_ref` guard at the top of
        // `execute`; same single-threaded invariant as the earlier
        // re-lookup applies.
        let inst =
            inst_mut(&mut self.instruments, symbol).expect("instrument verified to exist above");
        let taker_rested = inst.book.execute(order, quote_budget, slot, reports);

        // Capture the fee schedule for use inside the loop (we need
        // `maker_side` to attribute maker_fee/taker_fee to base vs quote
        // legs, so fees must be computed alongside the maker/taker slot
        // lookup rather than in a separate pre-pass).
        let fee_schedule = inst.fee_schedule;

        // Process reports to update balances. Mirrors the old process_reports
        // logic but resolves slots from the book instead of a separate HashMap.
        //
        // consumed_slots: fully-filled or STP-cancelled makers, with their
        // reservation slots. Typically 0-5 entries per aggressive order.
        consumed.extend(inst.book.drain_consumed_slots());

        for report in &mut reports[report_start..] {
            match report {
                ExecutionReport::Fill {
                    maker_order_id,
                    taker_order_id,
                    symbol: _,
                    maker_account,
                    taker_account: fill_taker_account,
                    price,
                    quantity,
                    maker_fee,
                    taker_fee,
                } => {
                    // Dereference for clarity; the `&mut` references are
                    // used only to write maker_fee/taker_fee below.
                    let maker_order_id = *maker_order_id;
                    let taker_order_id = *taker_order_id;
                    let maker_account = *maker_account;
                    let fill_taker_account = *fill_taker_account;
                    let price = *price;
                    let quantity = *quantity;
                    // Resolve maker slot: consumed list (fully filled) or
                    // order_index (partially filled, still on book).
                    let maker_info = consumed
                        .iter()
                        .find(|(a, id, _, _)| *a == maker_account && *id == maker_order_id)
                        .map(|(_, _, side, slot)| (*side, *slot))
                        .or_else(|| {
                            inst.book
                                .peek_order_location(maker_account, maker_order_id)
                                .map(|(side, _, slot)| (side, slot))
                        });

                    let Some((maker_side, maker_slot)) = maker_info else {
                        continue;
                    };

                    // Resolve taker slot. The fill's taker may be the original
                    // order (use `slot`) or a triggered stop (consumed_slots
                    // if fully filled/cancelled, or order_index if it rested).
                    let taker_slot = if fill_taker_account == taker_account
                        && taker_order_id == taker_id
                    {
                        slot
                    } else {
                        // Triggered stop's slot — check consumed first,
                        // then order_index (stop-limit that partially
                        // filled and rested).
                        match consumed
                            .iter()
                            .find(|(a, id, _, _)| *a == fill_taker_account && *id == taker_order_id)
                            .map(|(_, _, _, s)| *s)
                            .or_else(|| {
                                inst.book
                                    .peek_order_location(fill_taker_account, taker_order_id)
                                    .map(|(_, _, s)| s)
                            }) {
                            Some(s) => s,
                            None => continue,
                        }
                    };

                    // Compute fees from the schedule. The wire-format
                    // report carries fees in **quote currency** (cost-based)
                    // for both legs — that's the economic value of the
                    // fee, stable across A's received-asset settlement.
                    // Internally, fill() takes the buyer fee in base
                    // units and the seller fee in quote units (each
                    // deducted from that side's received asset).
                    let cost_i128 = price.get() as i128 * quantity.get() as i128;
                    let qty_i128 = quantity.get() as i128;
                    let (buyer_slot, seller_slot, buyer_fee_bps, seller_fee_bps) = match maker_side
                    {
                        Side::Buy => (
                            maker_slot,
                            taker_slot,
                            fee_schedule.maker_fee_bps,
                            fee_schedule.taker_fee_bps,
                        ),
                        Side::Sell => (
                            taker_slot,
                            maker_slot,
                            fee_schedule.taker_fee_bps,
                            fee_schedule.maker_fee_bps,
                        ),
                    };
                    let buyer_quote_fee_report =
                        (cost_i128 * buyer_fee_bps as i128 / 10_000) as i64;
                    let seller_quote_fee = (cost_i128 * seller_fee_bps as i128 / 10_000) as i64;
                    let buyer_base_fee = (qty_i128 * buyer_fee_bps as i128 / 10_000) as i64;
                    // Update the report fields (quote-denominated).
                    match maker_side {
                        Side::Buy => {
                            *maker_fee = buyer_quote_fee_report;
                            *taker_fee = seller_quote_fee;
                        }
                        Side::Sell => {
                            *maker_fee = seller_quote_fee;
                            *taker_fee = buyer_quote_fee_report;
                        }
                    }
                    self.accounts.fill(
                        buyer_slot,
                        seller_slot,
                        price,
                        quantity,
                        buyer_base_fee,
                        seller_quote_fee,
                        &spec,
                    );

                    // Free fully consumed reservation slots (remaining == 0).
                    if self.accounts.reservation_remaining(maker_slot) == 0 {
                        self.accounts.free_slot(maker_slot);
                        freed.push((maker_account, maker_order_id));
                    }
                    if self.accounts.reservation_remaining(taker_slot) == 0 {
                        self.accounts.free_slot(taker_slot);
                        freed.push((fill_taker_account, taker_order_id));
                    }
                }
                ExecutionReport::Cancelled {
                    order_id, account, ..
                } => {
                    let order_id = *order_id;
                    let account = *account;
                    let key = (account, order_id);
                    if freed.contains(&key) {
                        continue;
                    }
                    // Cancelled: taker or STP-cancelled maker.
                    if account == taker_account && order_id == taker_id {
                        self.accounts.release(slot);
                    } else if let Some((_, _, _, maker_slot)) = consumed
                        .iter()
                        .find(|(a, id, _, _)| *a == account && *id == order_id)
                    {
                        self.accounts.release(*maker_slot);
                    }
                    freed.push(key);
                }
                ExecutionReport::Rejected {
                    order_id, account, ..
                } => {
                    let order_id = *order_id;
                    let account = *account;
                    let key = (account, order_id);
                    if freed.contains(&key) {
                        continue;
                    }
                    if account == taker_account && order_id == taker_id {
                        self.accounts.release(slot);
                    } else if let Some((_, _, _, triggered_slot)) = consumed
                        .iter()
                        .find(|(a, id, _, _)| *a == account && *id == order_id)
                    {
                        self.accounts.release(*triggered_slot);
                    }
                    freed.push(key);
                }
                _ => {}
            }
        }

        // Release leftover reservations for orders no longer on the book
        // (price improvement, market buy budget surplus, etc.).
        // Determined from report analysis — no HashMap lookup needed.
        if !taker_rested && !freed.contains(&(taker_account, taker_id)) {
            self.accounts.release(slot);
            freed.push((taker_account, taker_id));
        }
        for &(account, order_id, _, maker_slot) in &consumed {
            if !freed.contains(&(account, order_id)) {
                self.accounts.release(maker_slot);
                freed.push((account, order_id));
            }
        }

        // Decrement order_counts and free the live_order_ids entry
        // for every order that closed this turn (consumed maker slots
        // plus the taker if it didn't rest). Both maps are kept in
        // lockstep — they have to agree on "which orders are live."
        for &(account, order_id) in &freed {
            self.live_order_ids.remove(&(account, order_id));
            self.release_open_order(account);
        }

        // Schedule GTD expiry if the order rested (limit) or is now pending
        // (stop). Stop orders that triggered and fully filled in this same
        // execute call won't appear in the book any more — find_gtd_expiry
        // will return None and we won't schedule. Triggered stops that
        // re-rest as limits keep the same OrderId/expiry_ns, so the single
        // task scheduled here covers both lifecycle stages.
        if order.time_in_force == TimeInForce::GTD
            && order.expiry_ns > 0
            && inst_ref(&self.instruments, symbol)
                .and_then(|inst| inst.book.find_gtd_expiry(taker_account, taker_id))
                .is_some()
        {
            self.schedule_gtd_expiry(symbol, taker_account, taker_id, order.expiry_ns);
        }

        // Clear before restoring so the next call starts from an empty
        // Vec; capacity is retained. (`consumed` is iterated by reference
        // in the loop above and may still hold entries; `freed` is also
        // by-reference in its loop. Neither is drained as a side effect.)
        consumed.clear();
        freed.clear();
        self.scratch_consumed = consumed;
        self.scratch_freed = freed;
    }
}
