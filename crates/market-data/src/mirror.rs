//! L2 order book mirror reconstructed from the `ExecutionReport` stream.
//!
//! One `BookMirror` per symbol. Maintains bid/ask levels (price →
//! aggregate quantity + order count), an `OrderIndex` for resolving
//! fills/cancels to the correct level, and a `TradeRing` for recent
//! trade history.

use std::collections::BTreeMap;
use std::num::NonZeroU64;

use melin_trading::types::{
    ExecutionReport, InstrumentStatus, OrderId, Price, Quantity, Side, Symbol,
};

use crate::index::{OrderIndex, RestingOrder};
use crate::trade_ring::{Trade, TradeRing};

/// Aggregate state for a single price level (L2).
///
/// 12 bytes — fits two levels per cache line alongside the BTreeMap node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Level {
    /// Sum of remaining quantities of all orders at this price.
    pub total_qty: u64,
    /// Number of resting orders at this price.
    pub order_count: u32,
}

/// Per-symbol order book mirror.
///
/// BTreeMap for each side: sorted by price, O(log n) insert/remove,
/// O(1) best bid/ask via first/last iterators. This is not the hot
/// path (the matching engine is) — the mirror runs on a separate
/// thread in the gateway, so BTreeMap's branch-heavy traversal is
/// acceptable for the ~1 µs budget per update.
pub struct BookMirror {
    symbol: Symbol,
    /// Bid levels sorted ascending by price. Best bid = last entry.
    bids: BTreeMap<Price, Level>,
    /// Ask levels sorted ascending by price. Best ask = first entry.
    asks: BTreeMap<Price, Level>,
    /// Resting order index for resolving fills/cancels.
    index: OrderIndex,
    /// Recent trade history.
    trades: TradeRing,
    /// Last trade price for this symbol.
    last_trade_price: Option<Price>,
}

impl BookMirror {
    pub fn new(symbol: Symbol) -> Self {
        Self {
            symbol,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            index: OrderIndex::new(),
            trades: TradeRing::new(),
            last_trade_price: None,
        }
    }

    // -- Accessors --

    pub fn symbol(&self) -> Symbol {
        self.symbol
    }

    pub fn bids(&self) -> &BTreeMap<Price, Level> {
        &self.bids
    }

    pub fn asks(&self) -> &BTreeMap<Price, Level> {
        &self.asks
    }

    /// Best bid price (highest), or `None` if empty.
    pub fn best_bid(&self) -> Option<Price> {
        self.bids.keys().next_back().copied()
    }

    /// Best ask price (lowest), or `None` if empty.
    pub fn best_ask(&self) -> Option<Price> {
        self.asks.keys().next().copied()
    }

    pub fn last_trade_price(&self) -> Option<Price> {
        self.last_trade_price
    }

    pub fn trades(&self) -> &TradeRing {
        &self.trades
    }

    pub fn order_index(&self) -> &OrderIndex {
        &self.index
    }

    // -- Update logic --

    /// Apply a single `ExecutionReport` to the mirror.
    ///
    /// Returns `true` if the book state changed (levels modified),
    /// `false` if the event was a no-op (Triggered, Rejected, or
    /// symbol mismatch).
    pub fn apply(&mut self, report: &ExecutionReport) -> bool {
        match *report {
            ExecutionReport::Placed {
                order_id,
                symbol,
                account,
                side,
                price,
                quantity,
            } => {
                if symbol != self.symbol {
                    return false;
                }
                self.index.insert(
                    order_id,
                    RestingOrder {
                        symbol,
                        side,
                        price,
                        remaining: quantity,
                        account,
                    },
                );
                self.credit_level(side, price, quantity.get(), 1);
                true
            }

            ExecutionReport::Fill {
                maker_order_id,
                taker_order_id,
                symbol,
                price,
                quantity,
                ..
            } => {
                if symbol != self.symbol {
                    return false;
                }

                // Only the maker is on the book. The taker was never
                // Placed (marketable orders skip Placed entirely).
                //
                // Copy fields before mutating — avoids holding &mut index
                // while calling debit_level/debit_count on self.
                if let Some(maker) = self.index.get(&maker_order_id).copied() {
                    let fill_qty = quantity.get();
                    self.debit_level(maker.side, maker.price, fill_qty);

                    let new_remaining = maker.remaining.get().saturating_sub(fill_qty);
                    if new_remaining == 0 {
                        self.debit_count(maker.side, maker.price);
                        self.index.remove(&maker_order_id);
                    } else if let Some(entry) = self.index.get_mut(&maker_order_id) {
                        entry.remaining =
                            Quantity(NonZeroU64::new(new_remaining).expect("checked > 0"));
                    }

                    self.last_trade_price = Some(price);
                    self.trades.push(Trade {
                        maker_order_id,
                        taker_order_id,
                        price,
                        quantity,
                    });
                    true
                } else {
                    // Unknown maker — cold-start gap or stale snapshot.
                    // Don't record the trade: the book wasn't debited, so
                    // adding a trade entry would create an inconsistency
                    // between the trade ring and the order book.
                    tracing::warn!(
                        order_id = maker_order_id.0,
                        "fill for unknown maker — cold-start gap or bug"
                    );
                    false
                }
            }

            ExecutionReport::Cancelled {
                order_id,
                symbol,
                remaining_quantity,
                ..
            } => {
                if symbol != self.symbol {
                    return false;
                }
                if let Some(order) = self.index.remove(&order_id) {
                    self.debit_level(order.side, order.price, remaining_quantity.get());
                    self.debit_count(order.side, order.price);
                    true
                } else {
                    tracing::debug!(
                        order_id = order_id.0,
                        "cancel for unknown order — cold-start gap or bug"
                    );
                    false
                }
            }

            ExecutionReport::Replaced {
                order_id,
                symbol,
                side,
                old_price,
                new_price,
                old_remaining,
                new_remaining,
                ..
            } => {
                if symbol != self.symbol {
                    return false;
                }
                // Remove from old level, add to new level.
                self.debit_level(side, old_price, old_remaining.get());
                self.debit_count(side, old_price);
                self.credit_level(side, new_price, new_remaining.get(), 1);

                // Update the index entry.
                if let Some(order) = self.index.get_mut(&order_id) {
                    order.price = new_price;
                    order.remaining = new_remaining;
                }
                true
            }

            ExecutionReport::Triggered { .. } | ExecutionReport::Rejected { .. } => {
                // Triggered: no-op — wait for subsequent Placed or Fill.
                // Rejected: never on the book.
                false
            }

            ExecutionReport::InstrumentStatusChanged { symbol, status } => {
                if symbol != self.symbol {
                    return false;
                }
                match status {
                    InstrumentStatus::Disabled | InstrumentStatus::Removed => {
                        // Engine cancels all resting orders before this
                        // event, so the book should already be empty.
                        self.bids.clear();
                        self.asks.clear();
                        self.index.clear();
                        true
                    }
                    InstrumentStatus::Enabled => false,
                }
            }
        }
    }

    // -- Cold-start seeding (used by snapshot parser) --

    /// Seed a level directly from a snapshot. Inserts the level into the
    /// BTreeMap and adds a synthetic index entry. This bypasses the normal
    /// `apply(Placed)` path which always sets `order_count=1`.
    pub fn seed_level(&mut self, synthetic_order_id: OrderId, order: RestingOrder, level: Level) {
        let book = self.book_side_mut(order.side);
        let entry = book.entry(order.price).or_insert(Level {
            total_qty: 0,
            order_count: 0,
        });
        entry.total_qty = level.total_qty;
        entry.order_count = level.order_count;
        self.index.insert(synthetic_order_id, order);
    }

    // -- Internal helpers --

    fn book_side_mut(&mut self, side: Side) -> &mut BTreeMap<Price, Level> {
        match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        }
    }

    /// Add quantity and order count to a price level.
    fn credit_level(&mut self, side: Side, price: Price, qty: u64, count: u32) {
        let level = self.book_side_mut(side).entry(price).or_insert(Level {
            total_qty: 0,
            order_count: 0,
        });
        level.total_qty += qty;
        level.order_count += count;
    }

    /// Subtract quantity from a price level. Removes the level if
    /// total_qty reaches zero.
    fn debit_level(&mut self, side: Side, price: Price, qty: u64) {
        let book = self.book_side_mut(side);
        if let Some(level) = book.get_mut(&price) {
            level.total_qty = level.total_qty.saturating_sub(qty);
            if level.total_qty == 0 {
                book.remove(&price);
            }
        }
    }

    /// Decrement order count at a price level.
    fn debit_count(&mut self, side: Side, price: Price) {
        let book = self.book_side_mut(side);
        if let Some(level) = book.get_mut(&price) {
            level.order_count = level.order_count.saturating_sub(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use melin_trading::types::{AccountId, OrderId, RejectReason};

    const SYM: Symbol = Symbol(1);
    const ACCT: AccountId = AccountId(1);

    fn price(n: u64) -> Price {
        Price(NonZeroU64::new(n).unwrap())
    }

    fn qty(n: u64) -> Quantity {
        Quantity(NonZeroU64::new(n).unwrap())
    }

    fn placed(order_id: u64, side: Side, p: u64, q: u64) -> ExecutionReport {
        ExecutionReport::Placed {
            order_id: OrderId(order_id),
            symbol: SYM,
            account: ACCT,
            side,
            price: price(p),
            quantity: qty(q),
        }
    }

    fn fill(maker_id: u64, taker_id: u64, p: u64, q: u64) -> ExecutionReport {
        ExecutionReport::Fill {
            maker_order_id: OrderId(maker_id),
            taker_order_id: OrderId(taker_id),
            symbol: SYM,
            maker_account: ACCT,
            taker_account: AccountId(2),
            price: price(p),
            quantity: qty(q),
            maker_fee: 0,
            taker_fee: 0,
        }
    }

    fn cancelled(order_id: u64, remaining: u64) -> ExecutionReport {
        ExecutionReport::Cancelled {
            order_id: OrderId(order_id),
            symbol: SYM,
            account: ACCT,
            remaining_quantity: qty(remaining),
        }
    }

    fn replaced(
        order_id: u64,
        side: Side,
        old_p: u64,
        new_p: u64,
        old_rem: u64,
        new_rem: u64,
    ) -> ExecutionReport {
        ExecutionReport::Replaced {
            order_id: OrderId(order_id),
            symbol: SYM,
            account: ACCT,
            side,
            old_price: price(old_p),
            new_price: price(new_p),
            old_remaining: qty(old_rem),
            new_remaining: qty(new_rem),
        }
    }

    // -- Basic placement tests --

    #[test]
    fn place_bid_creates_level() {
        let mut m = BookMirror::new(SYM);
        assert!(m.apply(&placed(1, Side::Buy, 100, 10)));
        assert_eq!(m.best_bid(), Some(price(100)));
        assert_eq!(
            m.bids().get(&price(100)),
            Some(&Level {
                total_qty: 10,
                order_count: 1
            })
        );
        assert!(m.best_ask().is_none());
    }

    #[test]
    fn place_ask_creates_level() {
        let mut m = BookMirror::new(SYM);
        m.apply(&placed(1, Side::Sell, 200, 5));
        assert_eq!(m.best_ask(), Some(price(200)));
        assert_eq!(
            m.asks().get(&price(200)),
            Some(&Level {
                total_qty: 5,
                order_count: 1
            })
        );
    }

    #[test]
    fn multiple_orders_same_level() {
        let mut m = BookMirror::new(SYM);
        m.apply(&placed(1, Side::Buy, 100, 10));
        m.apply(&placed(2, Side::Buy, 100, 20));
        assert_eq!(
            m.bids().get(&price(100)),
            Some(&Level {
                total_qty: 30,
                order_count: 2
            })
        );
    }

    // -- Fill tests --

    #[test]
    fn fill_decrements_maker_level() {
        let mut m = BookMirror::new(SYM);
        m.apply(&placed(1, Side::Sell, 200, 10));
        m.apply(&fill(1, 100, 200, 3));

        // 10 - 3 = 7 remaining
        assert_eq!(
            m.asks().get(&price(200)),
            Some(&Level {
                total_qty: 7,
                order_count: 1
            })
        );
        assert_eq!(m.last_trade_price(), Some(price(200)));
        assert_eq!(m.trades().len(), 1);
    }

    #[test]
    fn fill_removes_fully_filled_order() {
        let mut m = BookMirror::new(SYM);
        m.apply(&placed(1, Side::Buy, 100, 10));
        m.apply(&fill(1, 100, 100, 10));

        // Level should be removed entirely.
        assert!(m.bids().is_empty());
        assert!(m.order_index().is_empty());
    }

    #[test]
    fn fill_unknown_maker_is_graceful() {
        let mut m = BookMirror::new(SYM);
        // No panic — warn log, no trade recorded (book wasn't debited).
        assert!(!m.apply(&fill(999, 100, 200, 5)));
        assert_eq!(m.trades().len(), 0);
    }

    // -- Cancel tests --

    #[test]
    fn cancel_removes_order_and_level() {
        let mut m = BookMirror::new(SYM);
        m.apply(&placed(1, Side::Buy, 100, 10));
        assert!(m.apply(&cancelled(1, 10)));
        assert!(m.bids().is_empty());
        assert!(m.order_index().is_empty());
    }

    #[test]
    fn cancel_one_of_two_at_same_level() {
        let mut m = BookMirror::new(SYM);
        m.apply(&placed(1, Side::Sell, 200, 10));
        m.apply(&placed(2, Side::Sell, 200, 20));
        m.apply(&cancelled(1, 10));
        assert_eq!(
            m.asks().get(&price(200)),
            Some(&Level {
                total_qty: 20,
                order_count: 1
            })
        );
    }

    // -- Replace tests --

    #[test]
    fn replace_moves_order_between_levels() {
        let mut m = BookMirror::new(SYM);
        m.apply(&placed(1, Side::Buy, 100, 10));
        m.apply(&replaced(1, Side::Buy, 100, 110, 10, 10));

        assert!(m.bids().get(&price(100)).is_none());
        assert_eq!(
            m.bids().get(&price(110)),
            Some(&Level {
                total_qty: 10,
                order_count: 1
            })
        );
    }

    #[test]
    fn replace_changes_quantity() {
        let mut m = BookMirror::new(SYM);
        m.apply(&placed(1, Side::Sell, 200, 10));
        m.apply(&replaced(1, Side::Sell, 200, 200, 10, 5));

        assert_eq!(
            m.asks().get(&price(200)),
            Some(&Level {
                total_qty: 5,
                order_count: 1
            })
        );
    }

    // -- Triggered / Rejected are no-ops --

    #[test]
    fn triggered_is_noop() {
        let mut m = BookMirror::new(SYM);
        let report = ExecutionReport::Triggered {
            order_id: OrderId(1),
            symbol: SYM,
            account: ACCT,
            trigger_price: price(100),
        };
        assert!(!m.apply(&report));
    }

    #[test]
    fn rejected_is_noop() {
        let mut m = BookMirror::new(SYM);
        let report = ExecutionReport::Rejected {
            order_id: OrderId(1),
            symbol: SYM,
            account: ACCT,
            reason: RejectReason::InsufficientBalance,
        };
        assert!(!m.apply(&report));
    }

    // -- Symbol filtering --

    #[test]
    fn ignores_wrong_symbol() {
        let mut m = BookMirror::new(SYM);
        let report = ExecutionReport::Placed {
            order_id: OrderId(1),
            symbol: Symbol(99),
            account: ACCT,
            side: Side::Buy,
            price: price(100),
            quantity: qty(10),
        };
        assert!(!m.apply(&report));
        assert!(m.bids().is_empty());
    }

    // -- InstrumentStatusChanged --

    #[test]
    fn disabled_clears_book() {
        let mut m = BookMirror::new(SYM);
        m.apply(&placed(1, Side::Buy, 100, 10));
        m.apply(&placed(2, Side::Sell, 200, 5));
        m.apply(&ExecutionReport::InstrumentStatusChanged {
            symbol: SYM,
            status: InstrumentStatus::Disabled,
        });
        assert!(m.bids().is_empty());
        assert!(m.asks().is_empty());
        assert!(m.order_index().is_empty());
    }

    #[test]
    fn enabled_is_noop() {
        let mut m = BookMirror::new(SYM);
        m.apply(&placed(1, Side::Buy, 100, 10));
        assert!(!m.apply(&ExecutionReport::InstrumentStatusChanged {
            symbol: SYM,
            status: InstrumentStatus::Enabled,
        }));
        // Book unchanged.
        assert_eq!(m.bids().len(), 1);
    }

    #[test]
    fn removed_clears_book() {
        let mut m = BookMirror::new(SYM);
        m.apply(&placed(1, Side::Buy, 100, 10));
        assert!(m.apply(&ExecutionReport::InstrumentStatusChanged {
            symbol: SYM,
            status: InstrumentStatus::Removed,
        }));
        assert!(m.bids().is_empty());
    }

    #[test]
    fn cancel_unknown_order_is_graceful() {
        let mut m = BookMirror::new(SYM);
        // No panic — returns false.
        assert!(!m.apply(&cancelled(999, 10)));
    }

    // -- Sequence: place, partial fill, cancel remainder --

    #[test]
    fn place_partial_fill_cancel() {
        let mut m = BookMirror::new(SYM);
        m.apply(&placed(1, Side::Sell, 200, 10));
        m.apply(&fill(1, 100, 200, 3)); // 7 remaining
        m.apply(&cancelled(1, 7)); // cancel rest

        assert!(m.asks().is_empty());
        assert!(m.order_index().is_empty());
        assert_eq!(m.trades().len(), 1);
    }

    // -- Multiple fills sweeping a level --

    #[test]
    fn multiple_fills_sweep_level() {
        let mut m = BookMirror::new(SYM);
        m.apply(&placed(1, Side::Sell, 200, 10));
        m.apply(&placed(2, Side::Sell, 200, 20));

        // Taker sweeps both.
        m.apply(&fill(1, 100, 200, 10)); // order 1 fully filled
        m.apply(&fill(2, 100, 200, 20)); // order 2 fully filled

        assert!(m.asks().is_empty());
        assert!(m.order_index().is_empty());
        assert_eq!(m.trades().len(), 2);
    }

    // -- Edge cases --

    #[test]
    fn fill_qty_exceeds_remaining_saturates() {
        let mut m = BookMirror::new(SYM);
        m.apply(&placed(1, Side::Buy, 100, 10));
        // Fill with more than remaining — saturating_sub prevents panic,
        // order is removed (remainder = 0).
        m.apply(&fill(1, 99, 100, 15));
        assert!(m.bids().is_empty());
        assert!(m.order_index().is_empty());
        assert_eq!(m.trades().len(), 1);
    }

    #[test]
    fn replace_unknown_order_adds_level_without_panic() {
        let mut m = BookMirror::new(SYM);
        // Replace on a non-existent order — debits a missing level (no-op),
        // credits the new level. Index update is a no-op (get_mut returns None).
        m.apply(&replaced(999, Side::Buy, 100, 110, 10, 10));
        // New level was credited even though old level was absent.
        assert_eq!(
            m.bids().get(&price(110)),
            Some(&Level {
                total_qty: 10,
                order_count: 1
            })
        );
        // Index has no entry for 999 — Replace doesn't insert, only mutates.
        assert!(m.order_index().is_empty());
    }

    #[test]
    fn sequential_fills_reduce_to_zero() {
        let mut m = BookMirror::new(SYM);
        m.apply(&placed(1, Side::Sell, 200, 30));
        m.apply(&fill(1, 10, 200, 10)); // 20 remaining
        m.apply(&fill(1, 11, 200, 10)); // 10 remaining
        m.apply(&fill(1, 12, 200, 10)); // 0 remaining — removed

        assert!(m.asks().is_empty());
        assert!(m.order_index().is_empty());
        assert_eq!(m.trades().len(), 3);
    }

    #[test]
    fn cancel_unknown_leaves_book_unchanged() {
        let mut m = BookMirror::new(SYM);
        m.apply(&placed(1, Side::Buy, 100, 10));
        // Cancel a different order — should not affect order 1.
        assert!(!m.apply(&cancelled(999, 5)));
        assert_eq!(
            m.bids().get(&price(100)),
            Some(&Level {
                total_qty: 10,
                order_count: 1
            })
        );
    }
}
