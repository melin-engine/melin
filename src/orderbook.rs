//! Order book with price-time priority matching.
//!
//! Bids are stored in descending price order, asks in ascending.
//! Within a price level, orders are matched FIFO.

use std::collections::{BTreeMap, HashMap, VecDeque};

use crate::types::{
    ExecutionReport, Order, OrderId, OrderType, Price, Quantity, RejectReason, Side, TimeInForce,
};

/// A resting order on the book (the unfilled portion of a limit order).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RestingOrder {
    id: OrderId,
    remaining: Quantity,
}

/// One side of the order book (either all bids or all asks).
#[derive(Debug, Default)]
struct BookSide {
    /// BTreeMap: keeps price levels sorted so we can efficiently iterate from
    /// best price (lowest ask / highest bid) without re-sorting. O(log n)
    /// insert/remove per level.
    ///
    /// VecDeque: FIFO queue within each price level for time priority. O(1)
    /// push_back (new orders) and pop_front (fills).
    levels: BTreeMap<Price, VecDeque<RestingOrder>>,
}

impl BookSide {
    fn add(&mut self, price: Price, order: RestingOrder) {
        self.levels.entry(price).or_default().push_back(order);
    }

    fn remove(&mut self, price: Price, order_id: OrderId) -> Option<Quantity> {
        let level = self.levels.get_mut(&price)?;
        let pos = level.iter().position(|o| o.id == order_id)?;
        let order = level.remove(pos).expect("position was valid");
        if level.is_empty() {
            self.levels.remove(&price);
        }
        Some(order.remaining)
    }

    fn is_empty(&self) -> bool {
        self.levels.is_empty()
    }

    /// Total available quantity at prices that would match the given limit price.
    fn available_quantity(&self, side: Side, limit: Option<Price>) -> u64 {
        let mut total: u64 = 0;
        match side {
            Side::Buy => {
                // Bids: iterate from highest price downward
                for (&price, level) in self.levels.iter().rev() {
                    if let Some(limit) = limit
                        && price < limit
                    {
                        break;
                    }
                    for order in level {
                        total = total.saturating_add(order.remaining.get());
                    }
                }
            }
            Side::Sell => {
                // Asks: iterate from lowest price upward
                for (&price, level) in &self.levels {
                    if let Some(limit) = limit
                        && price > limit
                    {
                        break;
                    }
                    for order in level {
                        total = total.saturating_add(order.remaining.get());
                    }
                }
            }
        }
        total
    }
}

/// Central limit order book for a single instrument.
#[derive(Debug)]
pub struct OrderBook {
    bids: BookSide,
    asks: BookSide,
    /// HashMap: O(1) amortized lookup for cancel operations. Maps order_id to
    /// its location (side, price) so we don't need to scan the book.
    order_index: HashMap<OrderId, (Side, Price)>,
}

impl Default for OrderBook {
    fn default() -> Self {
        Self::new()
    }
}

impl OrderBook {
    pub fn new() -> Self {
        Self {
            bids: BookSide::default(),
            asks: BookSide::default(),
            order_index: HashMap::new(),
        }
    }

    /// Process an incoming order, appending execution reports to `reports`.
    pub fn execute(&mut self, order: Order, reports: &mut Vec<ExecutionReport>) {
        match order.order_type {
            OrderType::Limit { price } => self.execute_limit(order, price, reports),
            OrderType::Market => self.execute_market(order, reports),
        }
    }

    /// Cancel a resting order by ID.
    pub fn cancel(&mut self, order_id: OrderId, reports: &mut Vec<ExecutionReport>) {
        let Some((side, price)) = self.order_index.remove(&order_id) else {
            return;
        };
        let book_side = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        if let Some(remaining) = book_side.remove(price, order_id) {
            reports.push(ExecutionReport::Cancelled {
                order_id,
                remaining_quantity: remaining,
            });
        }
    }

    fn execute_limit(&mut self, order: Order, price: Price, reports: &mut Vec<ExecutionReport>) {
        let opposite = self.opposite_side(order.side);

        // FOK: check if we can fill entirely before doing anything.
        if order.time_in_force == TimeInForce::FOK {
            let available = opposite.available_quantity(
                Self::opposite(order.side),
                Some(price),
            );
            if available < order.quantity.get() {
                reports.push(ExecutionReport::Rejected {
                    order_id: order.id,
                    reason: RejectReason::FOKCannotFill,
                });
                return;
            }
        }

        let remaining = self.match_against(order.id, order.side, order.quantity, Some(price), reports);

        match remaining {
            Some(rem) => match order.time_in_force {
                TimeInForce::GTC => {
                    self.place_on_book(order.id, order.side, price, rem, reports);
                }
                TimeInForce::IOC | TimeInForce::FOK => {
                    reports.push(ExecutionReport::Cancelled {
                        order_id: order.id,
                        remaining_quantity: rem,
                    });
                }
            },
            None => {
                // Fully filled, nothing to do.
            }
        }
    }

    fn execute_market(&mut self, order: Order, reports: &mut Vec<ExecutionReport>) {
        let opposite = self.opposite_side(order.side);

        // FOK: check if we can fill entirely.
        if order.time_in_force == TimeInForce::FOK {
            let available = opposite.available_quantity(Self::opposite(order.side), None);
            if available < order.quantity.get() {
                reports.push(ExecutionReport::Rejected {
                    order_id: order.id,
                    reason: RejectReason::FOKCannotFill,
                });
                return;
            }
        }

        // Reject market order on empty book.
        if opposite.is_empty() {
            reports.push(ExecutionReport::Rejected {
                order_id: order.id,
                reason: RejectReason::NoLiquidity,
            });
            return;
        }

        let remaining = self.match_against(order.id, order.side, order.quantity, None, reports);

        if let Some(rem) = remaining {
            // Market order couldn't fully fill — cancel remainder.
            reports.push(ExecutionReport::Cancelled {
                order_id: order.id,
                remaining_quantity: rem,
            });
        }
    }

    /// Match an incoming order against the opposite side of the book.
    ///
    /// Returns the remaining quantity if not fully filled, or `None` if fully filled.
    fn match_against(
        &mut self,
        taker_id: OrderId,
        taker_side: Side,
        mut quantity: Quantity,
        price_limit: Option<Price>,
        reports: &mut Vec<ExecutionReport>,
    ) -> Option<Quantity> {
        let opposite = match taker_side {
            Side::Buy => &mut self.asks,
            Side::Sell => &mut self.bids,
        };

        // Collect the prices we need to visit. We can't iterate and mutate simultaneously,
        // so we gather matching price levels first.
        let prices: Vec<Price> = match taker_side {
            Side::Buy => {
                // Buy matches against asks (lowest first)
                opposite
                    .levels
                    .keys()
                    .take_while(|&&p| price_limit.is_none_or(|limit| p <= limit))
                    .copied()
                    .collect()
            }
            Side::Sell => {
                // Sell matches against bids (highest first)
                opposite
                    .levels
                    .keys()
                    .rev()
                    .take_while(|&&p| price_limit.is_none_or(|limit| p >= limit))
                    .copied()
                    .collect()
            }
        };

        for price in prices {
            let Some(level) = opposite.levels.get_mut(&price) else {
                continue;
            };

            while let Some(maker) = level.front_mut() {
                let fill_qty = quantity.min(maker.remaining);

                reports.push(ExecutionReport::Fill {
                    maker_order_id: maker.id,
                    taker_order_id: taker_id,
                    price,
                    quantity: fill_qty,
                });

                match maker.remaining.checked_sub(fill_qty) {
                    Some(new_remaining) => {
                        maker.remaining = new_remaining;
                    }
                    None => {
                        // Maker fully filled — remove from book.
                        let filled_maker = level.pop_front().expect("front existed");
                        self.order_index.remove(&filled_maker.id);
                    }
                }

                match quantity.checked_sub(fill_qty) {
                    Some(new_qty) => {
                        quantity = new_qty;
                    }
                    None => {
                        // Taker fully filled.
                        if level.is_empty() {
                            opposite.levels.remove(&price);
                        }
                        return None;
                    }
                }
            }

            // Level fully consumed.
            opposite.levels.remove(&price);
        }

        Some(quantity)
    }

    fn place_on_book(
        &mut self,
        id: OrderId,
        side: Side,
        price: Price,
        quantity: Quantity,
        reports: &mut Vec<ExecutionReport>,
    ) {
        let book_side = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        book_side.add(price, RestingOrder { id, remaining: quantity });
        self.order_index.insert(id, (side, price));
        reports.push(ExecutionReport::Placed {
            order_id: id,
            side,
            price,
            quantity,
        });
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.bids.is_empty() && self.asks.is_empty()
    }

    fn opposite_side(&self, side: Side) -> &BookSide {
        match side {
            Side::Buy => &self.asks,
            Side::Sell => &self.bids,
        }
    }

    fn opposite(side: Side) -> Side {
        match side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::*;

    fn qty(n: u64) -> Quantity {
        Quantity(NonZeroU64::new(n).unwrap())
    }

    fn price(n: u64) -> Price {
        Price(NonZeroU64::new(n).unwrap())
    }

    fn limit_order(id: u64, side: Side, p: u64, q: u64, tif: TimeInForce) -> Order {
        Order {
            id: OrderId(id),
            side,
            order_type: OrderType::Limit { price: price(p) },
            time_in_force: tif,
            quantity: qty(q),
        }
    }

    fn market_order(id: u64, side: Side, q: u64, tif: TimeInForce) -> Order {
        Order {
            id: OrderId(id),
            side,
            order_type: OrderType::Market,
            time_in_force: tif,
            quantity: qty(q),
        }
    }

    // -- Limit order placement --

    #[test]
    fn limit_order_rests_on_empty_book() {
        let mut book = OrderBook::new();
        let mut reports = Vec::new();
        book.execute(limit_order(1, Side::Buy, 100, 10, TimeInForce::GTC), &mut reports);

        assert_eq!(reports.len(), 1);
        assert!(matches!(
            reports[0],
            ExecutionReport::Placed {
                order_id: OrderId(1),
                side: Side::Buy,
                ..
            }
        ));
        // Verify the order is resting: a matching sell should fill.
        reports.clear();
        book.execute(limit_order(2, Side::Sell, 100, 10, TimeInForce::GTC), &mut reports);
        assert!(matches!(reports[0], ExecutionReport::Fill { .. }));
        assert!(book.is_empty());
    }

    #[test]
    fn non_crossing_limit_orders_both_rest() {
        let mut book = OrderBook::new();
        let mut reports = Vec::new();

        // Bid at 100, ask at 200 — no cross.
        book.execute(limit_order(1, Side::Buy, 100, 10, TimeInForce::GTC), &mut reports);
        book.execute(limit_order(2, Side::Sell, 200, 10, TimeInForce::GTC), &mut reports);

        assert_eq!(reports.len(), 2);
        assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
        assert!(matches!(reports[1], ExecutionReport::Placed { .. }));

        // Verify both sides have liquidity.
        reports.clear();
        book.execute(market_order(3, Side::Sell, 10, TimeInForce::IOC), &mut reports);
        assert!(matches!(reports[0], ExecutionReport::Fill { .. }));
        reports.clear();
        book.execute(market_order(4, Side::Buy, 10, TimeInForce::IOC), &mut reports);
        assert!(matches!(reports[0], ExecutionReport::Fill { .. }));
        assert!(book.is_empty());
    }

    // -- Limit order matching --

    #[test]
    fn limit_buy_matches_resting_ask() {
        let mut book = OrderBook::new();
        let mut reports = Vec::new();

        book.execute(limit_order(1, Side::Sell, 100, 10, TimeInForce::GTC), &mut reports);
        reports.clear();

        // Buy at 100 should match the resting sell.
        book.execute(limit_order(2, Side::Buy, 100, 10, TimeInForce::GTC), &mut reports);

        assert_eq!(reports.len(), 1);
        assert_eq!(
            reports[0],
            ExecutionReport::Fill {
                maker_order_id: OrderId(1),
                taker_order_id: OrderId(2),
                price: price(100),
                quantity: qty(10),
            }
        );

        assert!(book.is_empty());
    }

    #[test]
    fn limit_buy_matches_at_better_price() {
        let mut book = OrderBook::new();
        let mut reports = Vec::new();

        // Resting ask at 90.
        book.execute(limit_order(1, Side::Sell, 90, 10, TimeInForce::GTC), &mut reports);
        reports.clear();

        // Buy limit at 100 should match at the maker's price (90).
        book.execute(limit_order(2, Side::Buy, 100, 10, TimeInForce::GTC), &mut reports);

        assert_eq!(reports.len(), 1);
        assert_eq!(
            reports[0],
            ExecutionReport::Fill {
                maker_order_id: OrderId(1),
                taker_order_id: OrderId(2),
                price: price(90),
                quantity: qty(10),
            }
        );

        assert!(book.is_empty());
    }

    #[test]
    fn partial_fill_remainder_rests() {
        let mut book = OrderBook::new();
        let mut reports = Vec::new();

        book.execute(limit_order(1, Side::Sell, 100, 5, TimeInForce::GTC), &mut reports);
        reports.clear();

        // Buy 10, only 5 available — partial fill, rest goes on book.
        book.execute(limit_order(2, Side::Buy, 100, 10, TimeInForce::GTC), &mut reports);

        assert_eq!(reports.len(), 2);
        assert_eq!(
            reports[0],
            ExecutionReport::Fill {
                maker_order_id: OrderId(1),
                taker_order_id: OrderId(2),
                price: price(100),
                quantity: qty(5),
            }
        );
        assert_eq!(
            reports[1],
            ExecutionReport::Placed {
                order_id: OrderId(2),
                side: Side::Buy,
                price: price(100),
                quantity: qty(5),
            }
        );

        // Consume the resting remainder by selling 5 into it.
        reports.clear();
        book.execute(limit_order(3, Side::Sell, 100, 5, TimeInForce::GTC), &mut reports);
        assert_eq!(reports.len(), 1);
        assert!(matches!(reports[0], ExecutionReport::Fill { quantity, .. } if quantity == qty(5)));
        assert!(book.is_empty());
    }

    #[test]
    fn price_time_priority() {
        let mut book = OrderBook::new();
        let mut reports = Vec::new();

        // Two asks at price 100, first one should fill first.
        book.execute(limit_order(1, Side::Sell, 100, 5, TimeInForce::GTC), &mut reports);
        book.execute(limit_order(2, Side::Sell, 100, 5, TimeInForce::GTC), &mut reports);
        reports.clear();

        // Buy 7: should fill 5 from order 1 (first in queue), then 2 from order 2.
        book.execute(limit_order(3, Side::Buy, 100, 7, TimeInForce::GTC), &mut reports);

        assert_eq!(reports.len(), 2);
        assert_eq!(
            reports[0],
            ExecutionReport::Fill {
                maker_order_id: OrderId(1),
                taker_order_id: OrderId(3),
                price: price(100),
                quantity: qty(5),
            }
        );
        assert_eq!(
            reports[1],
            ExecutionReport::Fill {
                maker_order_id: OrderId(2),
                taker_order_id: OrderId(3),
                price: price(100),
                quantity: qty(2),
            }
        );

        // Order 2 should still have 3 remaining on the book.
        reports.clear();
        book.execute(market_order(4, Side::Buy, 3, TimeInForce::IOC), &mut reports);
        assert!(matches!(reports[0], ExecutionReport::Fill { quantity, .. } if quantity == qty(3)));
        assert!(book.is_empty());
    }

    #[test]
    fn price_priority_best_price_first() {
        let mut book = OrderBook::new();
        let mut reports = Vec::new();

        // Asks at 110, then 100. Buy should hit 100 first.
        book.execute(limit_order(1, Side::Sell, 110, 5, TimeInForce::GTC), &mut reports);
        book.execute(limit_order(2, Side::Sell, 100, 5, TimeInForce::GTC), &mut reports);
        reports.clear();

        book.execute(limit_order(3, Side::Buy, 110, 3, TimeInForce::GTC), &mut reports);

        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0], ExecutionReport::Fill {
            maker_order_id: OrderId(2),
            taker_order_id: OrderId(3),
            price: price(100),
            quantity: qty(3),
        });

        // Ask at 110 (5 remaining) and bid at 100 (2 remaining from partial) should still be on book.
        reports.clear();
        book.execute(market_order(4, Side::Buy, 7, TimeInForce::IOC), &mut reports);
        assert!(matches!(reports[0], ExecutionReport::Fill { quantity, .. } if quantity == qty(2)));
        assert!(matches!(reports[1], ExecutionReport::Fill { quantity, .. } if quantity == qty(5)));
        assert!(book.is_empty());
    }

    // -- Market orders --

    #[test]
    fn market_buy_fills_against_asks() {
        let mut book = OrderBook::new();
        let mut reports = Vec::new();

        book.execute(limit_order(1, Side::Sell, 100, 10, TimeInForce::GTC), &mut reports);
        reports.clear();

        book.execute(market_order(2, Side::Buy, 10, TimeInForce::IOC), &mut reports);

        assert_eq!(reports.len(), 1);
        assert!(matches!(reports[0], ExecutionReport::Fill { .. }));
        assert!(book.is_empty());
    }

    #[test]
    fn market_order_rejected_on_empty_book() {
        let mut book = OrderBook::new();
        let mut reports = Vec::new();

        book.execute(market_order(1, Side::Buy, 10, TimeInForce::IOC), &mut reports);

        assert_eq!(reports.len(), 1);
        assert_eq!(
            reports[0],
            ExecutionReport::Rejected {
                order_id: OrderId(1),
                reason: RejectReason::NoLiquidity,
            }
        );
    }

    #[test]
    fn market_order_partial_fill_cancels_remainder() {
        let mut book = OrderBook::new();
        let mut reports = Vec::new();

        book.execute(limit_order(1, Side::Sell, 100, 5, TimeInForce::GTC), &mut reports);
        reports.clear();

        // Market buy for 10, only 5 available.
        book.execute(market_order(2, Side::Buy, 10, TimeInForce::IOC), &mut reports);

        assert_eq!(reports.len(), 2);
        assert!(matches!(reports[0], ExecutionReport::Fill { quantity, .. } if quantity == qty(5)));
        assert_eq!(
            reports[1],
            ExecutionReport::Cancelled {
                order_id: OrderId(2),
                remaining_quantity: qty(5),
            }
        );
        assert!(book.is_empty());
    }

    // -- IOC --

    #[test]
    fn ioc_limit_cancels_unfilled_remainder() {
        let mut book = OrderBook::new();
        let mut reports = Vec::new();

        book.execute(limit_order(1, Side::Sell, 100, 5, TimeInForce::GTC), &mut reports);
        reports.clear();

        book.execute(limit_order(2, Side::Buy, 100, 10, TimeInForce::IOC), &mut reports);

        assert_eq!(reports.len(), 2);
        assert!(matches!(reports[0], ExecutionReport::Fill { .. }));
        assert_eq!(
            reports[1],
            ExecutionReport::Cancelled {
                order_id: OrderId(2),
                remaining_quantity: qty(5),
            }
        );
        assert!(book.is_empty());
    }

    // -- FOK --

    #[test]
    fn fok_rejected_when_insufficient_quantity() {
        let mut book = OrderBook::new();
        let mut reports = Vec::new();

        book.execute(limit_order(1, Side::Sell, 100, 5, TimeInForce::GTC), &mut reports);
        reports.clear();

        // FOK buy for 10, only 5 available — should reject without any fills.
        book.execute(limit_order(2, Side::Buy, 100, 10, TimeInForce::FOK), &mut reports);

        assert_eq!(reports.len(), 1);
        assert_eq!(
            reports[0],
            ExecutionReport::Rejected {
                order_id: OrderId(2),
                reason: RejectReason::FOKCannotFill,
            }
        );

        // The resting ask should be untouched.
        reports.clear();
        book.execute(market_order(3, Side::Buy, 5, TimeInForce::IOC), &mut reports);
        assert!(matches!(reports[0], ExecutionReport::Fill { quantity, .. } if quantity == qty(5)));
        assert!(book.is_empty());
    }

    #[test]
    fn fok_fills_entirely_when_sufficient() {
        let mut book = OrderBook::new();
        let mut reports = Vec::new();

        book.execute(limit_order(1, Side::Sell, 100, 10, TimeInForce::GTC), &mut reports);
        reports.clear();

        book.execute(limit_order(2, Side::Buy, 100, 10, TimeInForce::FOK), &mut reports);

        assert_eq!(reports.len(), 1);
        assert!(matches!(reports[0], ExecutionReport::Fill { .. }));
        assert!(book.is_empty());
    }

    // -- Cancel --

    #[test]
    fn cancel_resting_order() {
        let mut book = OrderBook::new();
        let mut reports = Vec::new();

        book.execute(limit_order(1, Side::Buy, 100, 10, TimeInForce::GTC), &mut reports);
        reports.clear();

        book.cancel(OrderId(1), &mut reports);

        assert_eq!(reports.len(), 1);
        assert_eq!(
            reports[0],
            ExecutionReport::Cancelled {
                order_id: OrderId(1),
                remaining_quantity: qty(10),
            }
        );
        assert!(book.is_empty());
    }

    #[test]
    fn cancel_unknown_order_is_noop() {
        let mut book = OrderBook::new();
        let mut reports = Vec::new();

        book.cancel(OrderId(999), &mut reports);

        assert!(reports.is_empty());
    }

    #[test]
    fn cancelled_order_does_not_match() {
        let mut book = OrderBook::new();
        let mut reports = Vec::new();

        book.execute(limit_order(1, Side::Sell, 100, 10, TimeInForce::GTC), &mut reports);
        book.cancel(OrderId(1), &mut reports);
        reports.clear();

        // Market buy should find no liquidity.
        book.execute(market_order(2, Side::Buy, 10, TimeInForce::IOC), &mut reports);

        assert_eq!(reports.len(), 1);
        assert!(matches!(reports[0], ExecutionReport::Rejected { reason: RejectReason::NoLiquidity, .. }));
    }

    // -- Multi-level matching --

    #[test]
    fn market_order_sweeps_multiple_price_levels() {
        let mut book = OrderBook::new();
        let mut reports = Vec::new();

        book.execute(limit_order(1, Side::Sell, 100, 5, TimeInForce::GTC), &mut reports);
        book.execute(limit_order(2, Side::Sell, 101, 5, TimeInForce::GTC), &mut reports);
        book.execute(limit_order(3, Side::Sell, 102, 5, TimeInForce::GTC), &mut reports);
        reports.clear();

        book.execute(market_order(4, Side::Buy, 12, TimeInForce::IOC), &mut reports);

        // Should fill 5@100, 5@101, 2@102.
        assert_eq!(reports.len(), 3);
        assert_eq!(reports[0], ExecutionReport::Fill {
            maker_order_id: OrderId(1),
            taker_order_id: OrderId(4),
            price: price(100),
            quantity: qty(5),
        });
        assert_eq!(reports[1], ExecutionReport::Fill {
            maker_order_id: OrderId(2),
            taker_order_id: OrderId(4),
            price: price(101),
            quantity: qty(5),
        });
        assert_eq!(reports[2], ExecutionReport::Fill {
            maker_order_id: OrderId(3),
            taker_order_id: OrderId(4),
            price: price(102),
            quantity: qty(2),
        });

        // Order 3 still has 3 remaining on the book.
        reports.clear();
        book.execute(market_order(5, Side::Buy, 3, TimeInForce::IOC), &mut reports);
        assert!(matches!(reports[0], ExecutionReport::Fill { quantity, .. } if quantity == qty(3)));
        assert!(book.is_empty());
    }

    // -- Sell-side matching --

    #[test]
    fn limit_sell_matches_resting_bid() {
        let mut book = OrderBook::new();
        let mut reports = Vec::new();

        book.execute(limit_order(1, Side::Buy, 100, 10, TimeInForce::GTC), &mut reports);
        reports.clear();

        book.execute(limit_order(2, Side::Sell, 100, 10, TimeInForce::GTC), &mut reports);

        assert_eq!(reports.len(), 1);
        assert_eq!(
            reports[0],
            ExecutionReport::Fill {
                maker_order_id: OrderId(1),
                taker_order_id: OrderId(2),
                price: price(100),
                quantity: qty(10),
            }
        );
        assert!(book.is_empty());
    }

    #[test]
    fn sell_matches_best_bid_first() {
        let mut book = OrderBook::new();
        let mut reports = Vec::new();

        // Bids at 90 and 100. Sell should hit 100 first.
        book.execute(limit_order(1, Side::Buy, 90, 5, TimeInForce::GTC), &mut reports);
        book.execute(limit_order(2, Side::Buy, 100, 5, TimeInForce::GTC), &mut reports);
        reports.clear();

        book.execute(limit_order(3, Side::Sell, 90, 3, TimeInForce::GTC), &mut reports);

        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0], ExecutionReport::Fill {
            maker_order_id: OrderId(2),
            taker_order_id: OrderId(3),
            price: price(100),
            quantity: qty(3),
        });

        // Bid at 90 (5) and bid at 100 (2 remaining) should still be on book.
        reports.clear();
        book.execute(market_order(4, Side::Sell, 7, TimeInForce::IOC), &mut reports);
        assert!(matches!(reports[0], ExecutionReport::Fill { quantity, .. } if quantity == qty(2)));
        assert!(matches!(reports[1], ExecutionReport::Fill { quantity, .. } if quantity == qty(5)));
        assert!(book.is_empty());
    }
}
