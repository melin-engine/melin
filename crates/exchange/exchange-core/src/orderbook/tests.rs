#![cfg(test)]

use std::num::NonZeroU64;

use super::*;
use crate::types::Symbol;

const TEST_SYMBOL: Symbol = Symbol(1);

fn qty(n: u64) -> Quantity {
    Quantity(NonZeroU64::new(n).unwrap())
}

fn price(n: u64) -> Price {
    Price(NonZeroU64::new(n).unwrap())
}

/// Default test account — most orderbook tests don't care about account identity.
const TEST_ACCOUNT: AccountId = AccountId(1);

fn limit_order(id: u64, side: Side, p: u64, q: u64, tif: TimeInForce) -> Order {
    Order {
        id: OrderId(id),
        account: TEST_ACCOUNT,
        side,
        order_type: OrderType::Limit {
            price: price(p),
            post_only: false,
        },
        time_in_force: tif,
        quantity: qty(q),
        stp: SelfTradeProtection::Allow,
        expiry_ns: 0,
    }
}

fn market_order(id: u64, side: Side, q: u64, tif: TimeInForce) -> Order {
    Order {
        id: OrderId(id),
        account: TEST_ACCOUNT,
        side,
        order_type: OrderType::Market,
        time_in_force: tif,
        quantity: qty(q),
        stp: SelfTradeProtection::Allow,
        expiry_ns: 0,
    }
}

// -- Limit order placement --

#[test]
fn limit_order_rests_on_empty_book() {
    let mut book = OrderBook::new(TEST_SYMBOL);
    let mut reports = Vec::new();
    book.execute(
        limit_order(1, Side::Buy, 100, 10, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );

    assert_eq!(reports.len(), 1);
    assert!(matches!(
        reports[0],
        ExecutionReport::Placed {
            order_id: OrderId(1),
            symbol: TEST_SYMBOL,
            account: TEST_ACCOUNT,
            side: Side::Buy,
            ..
        }
    ));
    // Verify the order is resting: a matching sell should fill.
    reports.clear();
    book.execute(
        limit_order(2, Side::Sell, 100, 10, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Fill { .. }));
    assert!(book.is_empty());
}

#[test]
fn non_crossing_limit_orders_both_rest() {
    let mut book = OrderBook::new(TEST_SYMBOL);
    let mut reports = Vec::new();

    // Bid at 100, ask at 200 — no cross.
    book.execute(
        limit_order(1, Side::Buy, 100, 10, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    book.execute(
        limit_order(2, Side::Sell, 200, 10, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );

    assert_eq!(reports.len(), 2);
    assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
    assert!(matches!(reports[1], ExecutionReport::Placed { .. }));

    // Verify both sides have liquidity.
    reports.clear();
    book.execute(
        market_order(3, Side::Sell, 10, TimeInForce::IOC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Fill { .. }));
    reports.clear();
    book.execute(
        market_order(4, Side::Buy, 10, TimeInForce::IOC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Fill { .. }));
    assert!(book.is_empty());
}

// -- Limit order matching --

#[test]
fn limit_buy_matches_resting_ask() {
    let mut book = OrderBook::new(TEST_SYMBOL);
    let mut reports = Vec::new();

    book.execute(
        limit_order(1, Side::Sell, 100, 10, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    reports.clear();

    // Buy at 100 should match the resting sell.
    book.execute(
        limit_order(2, Side::Buy, 100, 10, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );

    assert_eq!(reports.len(), 1);
    assert_eq!(
        reports[0],
        ExecutionReport::Fill {
            maker_order_id: OrderId(1),
            taker_order_id: OrderId(2),
            symbol: TEST_SYMBOL,
            maker_account: TEST_ACCOUNT,
            taker_account: TEST_ACCOUNT,
            price: price(100),
            quantity: qty(10),
            maker_fee: 0,
            taker_fee: 0,
        }
    );

    assert!(book.is_empty());
}

#[test]
fn limit_buy_matches_at_better_price() {
    let mut book = OrderBook::new(TEST_SYMBOL);
    let mut reports = Vec::new();

    // Resting ask at 90.
    book.execute(
        limit_order(1, Side::Sell, 90, 10, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    reports.clear();

    // Buy limit at 100 should match at the maker's price (90).
    book.execute(
        limit_order(2, Side::Buy, 100, 10, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );

    assert_eq!(reports.len(), 1);
    assert_eq!(
        reports[0],
        ExecutionReport::Fill {
            maker_order_id: OrderId(1),
            taker_order_id: OrderId(2),
            symbol: TEST_SYMBOL,
            maker_account: TEST_ACCOUNT,
            taker_account: TEST_ACCOUNT,
            price: price(90),
            quantity: qty(10),
            maker_fee: 0,
            taker_fee: 0,
        }
    );

    assert!(book.is_empty());
}

#[test]
fn partial_fill_remainder_rests() {
    let mut book = OrderBook::new(TEST_SYMBOL);
    let mut reports = Vec::new();

    book.execute(
        limit_order(1, Side::Sell, 100, 5, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    reports.clear();

    // Buy 10, only 5 available — partial fill, rest goes on book.
    book.execute(
        limit_order(2, Side::Buy, 100, 10, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );

    assert_eq!(reports.len(), 2);
    assert_eq!(
        reports[0],
        ExecutionReport::Fill {
            maker_order_id: OrderId(1),
            taker_order_id: OrderId(2),
            symbol: TEST_SYMBOL,
            maker_account: TEST_ACCOUNT,
            taker_account: TEST_ACCOUNT,
            price: price(100),
            quantity: qty(5),
            maker_fee: 0,
            taker_fee: 0,
        }
    );
    assert_eq!(
        reports[1],
        ExecutionReport::Placed {
            order_id: OrderId(2),
            symbol: TEST_SYMBOL,
            account: TEST_ACCOUNT,
            side: Side::Buy,
            price: price(100),
            quantity: qty(5),
        }
    );

    // Consume the resting remainder by selling 5 into it.
    reports.clear();
    book.execute(
        limit_order(3, Side::Sell, 100, 5, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    assert_eq!(reports.len(), 1);
    assert!(matches!(reports[0], ExecutionReport::Fill { quantity, .. } if quantity == qty(5)));
    assert!(book.is_empty());
}

#[test]
fn price_time_priority() {
    let mut book = OrderBook::new(TEST_SYMBOL);
    let mut reports = Vec::new();

    // Two asks at price 100, first one should fill first.
    book.execute(
        limit_order(1, Side::Sell, 100, 5, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    book.execute(
        limit_order(2, Side::Sell, 100, 5, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    reports.clear();

    // Buy 7: should fill 5 from order 1 (first in queue), then 2 from order 2.
    book.execute(
        limit_order(3, Side::Buy, 100, 7, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );

    assert_eq!(reports.len(), 2);
    assert_eq!(
        reports[0],
        ExecutionReport::Fill {
            maker_order_id: OrderId(1),
            taker_order_id: OrderId(3),
            symbol: TEST_SYMBOL,
            maker_account: TEST_ACCOUNT,
            taker_account: TEST_ACCOUNT,
            price: price(100),
            quantity: qty(5),
            maker_fee: 0,
            taker_fee: 0,
        }
    );
    assert_eq!(
        reports[1],
        ExecutionReport::Fill {
            maker_order_id: OrderId(2),
            taker_order_id: OrderId(3),
            symbol: TEST_SYMBOL,
            maker_account: TEST_ACCOUNT,
            taker_account: TEST_ACCOUNT,
            price: price(100),
            quantity: qty(2),
            maker_fee: 0,
            taker_fee: 0,
        }
    );

    // Order 2 should still have 3 remaining on the book.
    reports.clear();
    book.execute(
        market_order(4, Side::Buy, 3, TimeInForce::IOC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Fill { quantity, .. } if quantity == qty(3)));
    assert!(book.is_empty());
}

#[test]
fn price_priority_best_price_first() {
    let mut book = OrderBook::new(TEST_SYMBOL);
    let mut reports = Vec::new();

    // Asks at 110, then 100. Buy should hit 100 first.
    book.execute(
        limit_order(1, Side::Sell, 110, 5, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    book.execute(
        limit_order(2, Side::Sell, 100, 5, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    reports.clear();

    book.execute(
        limit_order(3, Side::Buy, 110, 3, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );

    assert_eq!(reports.len(), 1);
    assert_eq!(
        reports[0],
        ExecutionReport::Fill {
            maker_order_id: OrderId(2),
            taker_order_id: OrderId(3),
            symbol: TEST_SYMBOL,
            maker_account: TEST_ACCOUNT,
            taker_account: TEST_ACCOUNT,
            price: price(100),
            quantity: qty(3),
            maker_fee: 0,
            taker_fee: 0,
        }
    );

    // Ask at 110 (5 remaining) and bid at 100 (2 remaining from partial) should still be on book.
    reports.clear();
    book.execute(
        market_order(4, Side::Buy, 7, TimeInForce::IOC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Fill { quantity, .. } if quantity == qty(2)));
    assert!(matches!(reports[1], ExecutionReport::Fill { quantity, .. } if quantity == qty(5)));
    assert!(book.is_empty());
}

// -- Market orders --

#[test]
fn market_buy_fills_against_asks() {
    let mut book = OrderBook::new(TEST_SYMBOL);
    let mut reports = Vec::new();

    book.execute(
        limit_order(1, Side::Sell, 100, 10, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    reports.clear();

    book.execute(
        market_order(2, Side::Buy, 10, TimeInForce::IOC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );

    assert_eq!(reports.len(), 1);
    assert!(matches!(reports[0], ExecutionReport::Fill { .. }));
    assert!(book.is_empty());
}

#[test]
fn market_order_rejected_on_empty_book() {
    let mut book = OrderBook::new(TEST_SYMBOL);
    let mut reports = Vec::new();

    book.execute(
        market_order(1, Side::Buy, 10, TimeInForce::IOC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );

    assert_eq!(reports.len(), 1);
    assert_eq!(
        reports[0],
        ExecutionReport::Rejected {
            order_id: OrderId(1),
            symbol: TEST_SYMBOL,
            account: TEST_ACCOUNT,
            reason: RejectReason::NoLiquidity,
        }
    );
}

#[test]
fn market_order_partial_fill_cancels_remainder() {
    let mut book = OrderBook::new(TEST_SYMBOL);
    let mut reports = Vec::new();

    book.execute(
        limit_order(1, Side::Sell, 100, 5, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    reports.clear();

    // Market buy for 10, only 5 available.
    book.execute(
        market_order(2, Side::Buy, 10, TimeInForce::IOC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );

    assert_eq!(reports.len(), 2);
    assert!(matches!(reports[0], ExecutionReport::Fill { quantity, .. } if quantity == qty(5)));
    assert_eq!(
        reports[1],
        ExecutionReport::Cancelled {
            order_id: OrderId(2),
            symbol: TEST_SYMBOL,
            account: TEST_ACCOUNT,
            remaining_quantity: qty(5),
        }
    );
    assert!(book.is_empty());
}

// -- IOC --

#[test]
fn ioc_limit_cancels_unfilled_remainder() {
    let mut book = OrderBook::new(TEST_SYMBOL);
    let mut reports = Vec::new();

    book.execute(
        limit_order(1, Side::Sell, 100, 5, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    reports.clear();

    book.execute(
        limit_order(2, Side::Buy, 100, 10, TimeInForce::IOC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );

    assert_eq!(reports.len(), 2);
    assert!(matches!(reports[0], ExecutionReport::Fill { .. }));
    assert_eq!(
        reports[1],
        ExecutionReport::Cancelled {
            order_id: OrderId(2),
            symbol: TEST_SYMBOL,
            account: TEST_ACCOUNT,
            remaining_quantity: qty(5),
        }
    );
    assert!(book.is_empty());
}

// -- FOK --

#[test]
fn fok_rejected_when_insufficient_quantity() {
    let mut book = OrderBook::new(TEST_SYMBOL);
    let mut reports = Vec::new();

    book.execute(
        limit_order(1, Side::Sell, 100, 5, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    reports.clear();

    // FOK buy for 10, only 5 available — should reject without any fills.
    book.execute(
        limit_order(2, Side::Buy, 100, 10, TimeInForce::FOK),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );

    assert_eq!(reports.len(), 1);
    assert_eq!(
        reports[0],
        ExecutionReport::Rejected {
            order_id: OrderId(2),
            symbol: TEST_SYMBOL,
            account: TEST_ACCOUNT,
            reason: RejectReason::FOKCannotFill,
        }
    );

    // The resting ask should be untouched.
    reports.clear();
    book.execute(
        market_order(3, Side::Buy, 5, TimeInForce::IOC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Fill { quantity, .. } if quantity == qty(5)));
    assert!(book.is_empty());
}

#[test]
fn fok_fills_entirely_when_sufficient() {
    let mut book = OrderBook::new(TEST_SYMBOL);
    let mut reports = Vec::new();

    book.execute(
        limit_order(1, Side::Sell, 100, 10, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    reports.clear();

    book.execute(
        limit_order(2, Side::Buy, 100, 10, TimeInForce::FOK),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );

    assert_eq!(reports.len(), 1);
    assert!(matches!(reports[0], ExecutionReport::Fill { .. }));
    assert!(book.is_empty());
}

// -- Cancel --

#[test]
fn cancel_resting_order() {
    let mut book = OrderBook::new(TEST_SYMBOL);
    let mut reports = Vec::new();

    book.execute(
        limit_order(1, Side::Buy, 100, 10, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    reports.clear();

    book.cancel(TEST_ACCOUNT, OrderId(1), &mut reports);

    assert_eq!(reports.len(), 1);
    assert_eq!(
        reports[0],
        ExecutionReport::Cancelled {
            order_id: OrderId(1),
            symbol: TEST_SYMBOL,
            account: TEST_ACCOUNT,
            remaining_quantity: qty(10),
        }
    );
    assert!(book.is_empty());
}

#[test]
fn cancel_unknown_order_is_noop() {
    let mut book = OrderBook::new(TEST_SYMBOL);
    let mut reports = Vec::new();

    book.cancel(TEST_ACCOUNT, OrderId(999), &mut reports);

    assert!(reports.is_empty());
}

#[test]
fn cancelled_order_does_not_match() {
    let mut book = OrderBook::new(TEST_SYMBOL);
    let mut reports = Vec::new();

    book.execute(
        limit_order(1, Side::Sell, 100, 10, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    book.cancel(TEST_ACCOUNT, OrderId(1), &mut reports);
    reports.clear();

    // Market buy should find no liquidity.
    book.execute(
        market_order(2, Side::Buy, 10, TimeInForce::IOC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );

    assert_eq!(reports.len(), 1);
    assert!(matches!(
        reports[0],
        ExecutionReport::Rejected {
            reason: RejectReason::NoLiquidity,
            ..
        }
    ));
}

// -- Intrusive-list edge cases --
//
// These tests cover the slab + doubly-linked list under operations that
// exercise the prev/next splicing logic directly — middle-of-FIFO
// cancellation, slab reuse cycles, and `replace_order` paths. The
// higher-level matching tests above all hit head/tail patterns; without
// these, a bug that left dangling `prev`/`next` links on cancel (or
// failed to refresh `LevelHead.head`/`tail`/`len`) could go undetected.

/// Cancelling a node with both prev and next neighbors must splice it
/// cleanly so the remaining FIFO order is preserved. A bug that forgot
/// to update either `prev.next` or `next.prev` would surface here as
/// a wrong fill order or a panic.
#[test]
fn cancel_middle_of_level_preserves_fifo() {
    let mut book = OrderBook::new(TEST_SYMBOL);
    let mut reports = Vec::new();

    // Three asks at the same price — head=1, middle=2, tail=3.
    for id in 1..=3 {
        book.execute(
            limit_order(id, Side::Sell, 100, 5, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
    }
    reports.clear();

    // Cancel the middle order. List must become 1 <-> 3 with no
    // dangling links to the freed slot.
    book.cancel(TEST_ACCOUNT, OrderId(2), &mut reports);
    assert_eq!(reports.len(), 1);
    assert!(matches!(
        reports[0],
        ExecutionReport::Cancelled {
            order_id: OrderId(2),
            ..
        }
    ));
    reports.clear();

    // A buy that exhausts both remaining makers must fill 1 first, 3 second.
    book.execute(
        limit_order(4, Side::Buy, 100, 10, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    let fill_makers: Vec<OrderId> = reports
        .iter()
        .filter_map(|r| match r {
            ExecutionReport::Fill { maker_order_id, .. } => Some(*maker_order_id),
            _ => None,
        })
        .collect();
    assert_eq!(fill_makers, vec![OrderId(1), OrderId(3)]);
    assert!(
        book.is_empty(),
        "book should be empty after exhausting both makers"
    );
}

/// Cancelling the head of a multi-order level promotes the next order
/// to head and leaves prev=INVALID — verified by feeding a market buy
/// and checking the new head fills first.
#[test]
fn cancel_head_promotes_next_to_head() {
    let mut book = OrderBook::new(TEST_SYMBOL);
    let mut reports = Vec::new();

    for id in 1..=3 {
        book.execute(
            limit_order(id, Side::Sell, 100, 5, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
    }
    reports.clear();

    book.cancel(TEST_ACCOUNT, OrderId(1), &mut reports);
    reports.clear();

    // 2 should now be the head.
    book.execute(
        limit_order(4, Side::Buy, 100, 5, TimeInForce::IOC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    assert!(matches!(
        reports[0],
        ExecutionReport::Fill {
            maker_order_id: OrderId(2),
            ..
        }
    ));
}

/// Cancelling the tail of a multi-order level reduces tail to its
/// predecessor; a subsequent newly-placed order at the same price
/// must splice onto the (new) tail correctly.
#[test]
fn cancel_tail_then_add_keeps_fifo() {
    let mut book = OrderBook::new(TEST_SYMBOL);
    let mut reports = Vec::new();

    for id in 1..=3 {
        book.execute(
            limit_order(id, Side::Sell, 100, 5, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
    }
    reports.clear();

    // Cancel tail (3). List is now 1 <-> 2.
    book.cancel(TEST_ACCOUNT, OrderId(3), &mut reports);
    reports.clear();

    // Place a fresh order; it should reuse the freed slab slot but
    // splice onto the new tail (2) — list becomes 1 <-> 2 <-> 4.
    book.execute(
        limit_order(4, Side::Sell, 100, 5, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    reports.clear();

    // Sweep all three: order must be 1, 2, 4.
    book.execute(
        limit_order(5, Side::Buy, 100, 15, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    let makers: Vec<OrderId> = reports
        .iter()
        .filter_map(|r| match r {
            ExecutionReport::Fill { maker_order_id, .. } => Some(*maker_order_id),
            _ => None,
        })
        .collect();
    assert_eq!(makers, vec![OrderId(1), OrderId(2), OrderId(4)]);
}

/// Cancelling the only order at a price must remove the whole level,
/// not leave a zero-length entry. Verified by re-placing at that
/// price and checking it's the new top of book.
#[test]
fn cancel_only_order_removes_level() {
    let mut book = OrderBook::new(TEST_SYMBOL);
    let mut reports = Vec::new();

    book.execute(
        limit_order(1, Side::Sell, 100, 5, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    book.cancel(TEST_ACCOUNT, OrderId(1), &mut reports);
    assert!(
        book.is_empty(),
        "single-order level must be removed on cancel"
    );
    assert_eq!(book.best_ask(), None);

    // Re-place at the same price; level is recreated.
    reports.clear();
    book.execute(
        limit_order(2, Side::Sell, 100, 5, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    assert_eq!(book.best_ask(), Some(price(100)));
}

/// Stress slab reuse: many cycles of place+cancel followed by a final
/// batch that must still match in correct FIFO order. Catches free-list
/// cycles, stale `prev`/`next` on reused slots, and `len` drift.
#[test]
fn repeated_alloc_free_preserves_book_invariants() {
    let mut book = OrderBook::new(TEST_SYMBOL);
    let mut reports = Vec::new();

    // 200 cycles of place-then-cancel at the same price churn the
    // slab through many alloc/free transitions.
    for id in 1..=200 {
        book.execute(
            limit_order(id, Side::Sell, 100, 1, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        book.cancel(TEST_ACCOUNT, OrderId(id), &mut reports);
    }
    assert!(
        book.is_empty(),
        "book should be empty after symmetric place/cancel"
    );
    reports.clear();

    // Final batch: 5 orders at the same price. After all the slab
    // churn, FIFO must still be respected.
    for id in 1000..=1004 {
        book.execute(
            limit_order(id, Side::Sell, 100, 1, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
    }
    reports.clear();

    book.execute(
        limit_order(2000, Side::Buy, 100, 5, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    let makers: Vec<OrderId> = reports
        .iter()
        .filter_map(|r| match r {
            ExecutionReport::Fill { maker_order_id, .. } => Some(*maker_order_id),
            _ => None,
        })
        .collect();
    assert_eq!(
        makers,
        vec![
            OrderId(1000),
            OrderId(1001),
            OrderId(1002),
            OrderId(1003),
            OrderId(1004),
        ]
    );
    assert!(book.is_empty());
}

/// `replace_order` qty-increase at the same price must move the order
/// to the back of the FIFO. Direct `OrderBook` test (the Exchange-level
/// `cancel_replace_qty_increase_loses_priority` exercises the same
/// behavior through more layers).
#[test]
fn replace_order_same_price_qty_increase_loses_priority() {
    let mut book = OrderBook::new(TEST_SYMBOL);
    let mut reports = Vec::new();

    for id in 1..=3 {
        book.execute(
            limit_order(id, Side::Sell, 100, 5, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
    }
    reports.clear();

    // Increase order 1's qty — must drop to back of queue.
    let result = book.replace_order(TEST_ACCOUNT, OrderId(1), price(100), qty(7));
    assert!(result.is_some());

    // A buy of 5 should hit order 2 first (the new head), not order 1.
    book.execute(
        limit_order(99, Side::Buy, 100, 5, TimeInForce::IOC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    assert!(matches!(
        reports[0],
        ExecutionReport::Fill {
            maker_order_id: OrderId(2),
            ..
        }
    ));
}

/// `replace_order` qty-decrease keeps priority — front-of-queue order
/// stays at the front after shrinking.
#[test]
fn replace_order_same_price_qty_decrease_keeps_priority() {
    let mut book = OrderBook::new(TEST_SYMBOL);
    let mut reports = Vec::new();

    for id in 1..=3 {
        book.execute(
            limit_order(id, Side::Sell, 100, 5, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
    }
    reports.clear();

    // Decrease order 1's qty — keeps head position.
    book.replace_order(TEST_ACCOUNT, OrderId(1), price(100), qty(3));

    book.execute(
        limit_order(99, Side::Buy, 100, 3, TimeInForce::IOC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    assert!(matches!(
        reports[0],
        ExecutionReport::Fill { maker_order_id: OrderId(1), quantity, .. } if quantity == qty(3)
    ));
}

/// `replace_order` to a different price unlinks from the old level
/// (removing it if empty) and splices onto the new level's tail.
#[test]
fn replace_order_to_different_price_relocates() {
    let mut book = OrderBook::new(TEST_SYMBOL);
    let mut reports = Vec::new();

    // One order at 100 (sole occupant), one at 110 (sole occupant).
    book.execute(
        limit_order(1, Side::Sell, 100, 5, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    book.execute(
        limit_order(2, Side::Sell, 110, 5, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    reports.clear();

    // Move order 1 from 100 → 110. The 100 level must be removed
    // (was its sole occupant); 110 must now have [2, 1] in FIFO.
    book.replace_order(TEST_ACCOUNT, OrderId(1), price(110), qty(5));
    assert_eq!(
        book.best_ask(),
        Some(price(110)),
        "after relocate, best ask must be the new price (no orphan empty level at 100)"
    );

    // Sweep both: maker order must be 2 (older at 110), then 1.
    book.execute(
        limit_order(3, Side::Buy, 110, 10, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    let makers: Vec<OrderId> = reports
        .iter()
        .filter_map(|r| match r {
            ExecutionReport::Fill { maker_order_id, .. } => Some(*maker_order_id),
            _ => None,
        })
        .collect();
    assert_eq!(makers, vec![OrderId(2), OrderId(1)]);
}

/// `prefault` must be a no-op on a non-empty book. `Exchange::prefault`
/// can be called after snapshot restore has placed orders, so wiping
/// the slab would leave `LevelHead` indices pointing at empty memory
/// and crash the next operation that touches them.
#[test]
fn prefault_on_populated_book_is_noop() {
    let mut book = OrderBook::with_capacity(TEST_SYMBOL);
    let mut reports = Vec::new();
    book.execute(
        limit_order(1, Side::Sell, 100, 5, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );

    // Calling prefault now must NOT clear the slab.
    book.prefault();

    // The resting order must still match.
    reports.clear();
    book.execute(
        limit_order(2, Side::Buy, 100, 5, TimeInForce::IOC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    assert!(matches!(
        reports[0],
        ExecutionReport::Fill {
            maker_order_id: OrderId(1),
            ..
        }
    ));
}

/// `prefault` must be idempotent and must not corrupt subsequent
/// book operations. Run it twice on a pre-sized book, then exercise
/// matching to confirm the slab is in a usable state.
#[test]
fn prefault_is_idempotent_and_safe() {
    let mut book = OrderBook::with_capacity(TEST_SYMBOL);
    book.prefault();
    book.prefault();

    let mut reports = Vec::new();
    book.execute(
        limit_order(1, Side::Sell, 100, 5, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    book.execute(
        limit_order(2, Side::Buy, 100, 5, TimeInForce::IOC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    // Should produce one Placed and one Fill — proves the slab pages
    // are usable and the linked-list invariants survived the prefault.
    assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
    assert!(matches!(
        reports[1],
        ExecutionReport::Fill {
            maker_order_id: OrderId(1),
            taker_order_id: OrderId(2),
            ..
        }
    ));
    assert!(book.is_empty());
}

// -- Multi-level matching --

#[test]
fn market_order_sweeps_multiple_price_levels() {
    let mut book = OrderBook::new(TEST_SYMBOL);
    let mut reports = Vec::new();

    book.execute(
        limit_order(1, Side::Sell, 100, 5, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    book.execute(
        limit_order(2, Side::Sell, 101, 5, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    book.execute(
        limit_order(3, Side::Sell, 102, 5, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    reports.clear();

    book.execute(
        market_order(4, Side::Buy, 12, TimeInForce::IOC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );

    // Should fill 5@100, 5@101, 2@102.
    assert_eq!(reports.len(), 3);
    assert_eq!(
        reports[0],
        ExecutionReport::Fill {
            maker_order_id: OrderId(1),
            taker_order_id: OrderId(4),
            symbol: TEST_SYMBOL,
            maker_account: TEST_ACCOUNT,
            taker_account: TEST_ACCOUNT,
            price: price(100),
            quantity: qty(5),
            maker_fee: 0,
            taker_fee: 0,
        }
    );
    assert_eq!(
        reports[1],
        ExecutionReport::Fill {
            maker_order_id: OrderId(2),
            taker_order_id: OrderId(4),
            symbol: TEST_SYMBOL,
            maker_account: TEST_ACCOUNT,
            taker_account: TEST_ACCOUNT,
            price: price(101),
            quantity: qty(5),
            maker_fee: 0,
            taker_fee: 0,
        }
    );
    assert_eq!(
        reports[2],
        ExecutionReport::Fill {
            maker_order_id: OrderId(3),
            taker_order_id: OrderId(4),
            symbol: TEST_SYMBOL,
            maker_account: TEST_ACCOUNT,
            taker_account: TEST_ACCOUNT,
            price: price(102),
            quantity: qty(2),
            maker_fee: 0,
            taker_fee: 0,
        }
    );

    // Order 3 still has 3 remaining on the book.
    reports.clear();
    book.execute(
        market_order(5, Side::Buy, 3, TimeInForce::IOC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Fill { quantity, .. } if quantity == qty(3)));
    assert!(book.is_empty());
}

// -- Sell-side matching --

#[test]
fn limit_sell_matches_resting_bid() {
    let mut book = OrderBook::new(TEST_SYMBOL);
    let mut reports = Vec::new();

    book.execute(
        limit_order(1, Side::Buy, 100, 10, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    reports.clear();

    book.execute(
        limit_order(2, Side::Sell, 100, 10, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );

    assert_eq!(reports.len(), 1);
    assert_eq!(
        reports[0],
        ExecutionReport::Fill {
            maker_order_id: OrderId(1),
            taker_order_id: OrderId(2),
            symbol: TEST_SYMBOL,
            maker_account: TEST_ACCOUNT,
            taker_account: TEST_ACCOUNT,
            price: price(100),
            quantity: qty(10),
            maker_fee: 0,
            taker_fee: 0,
        }
    );
    assert!(book.is_empty());
}

#[test]
fn sell_matches_best_bid_first() {
    let mut book = OrderBook::new(TEST_SYMBOL);
    let mut reports = Vec::new();

    // Bids at 90 and 100. Sell should hit 100 first.
    book.execute(
        limit_order(1, Side::Buy, 90, 5, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    book.execute(
        limit_order(2, Side::Buy, 100, 5, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    reports.clear();

    book.execute(
        limit_order(3, Side::Sell, 90, 3, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );

    assert_eq!(reports.len(), 1);
    assert_eq!(
        reports[0],
        ExecutionReport::Fill {
            maker_order_id: OrderId(2),
            taker_order_id: OrderId(3),
            symbol: TEST_SYMBOL,
            maker_account: TEST_ACCOUNT,
            taker_account: TEST_ACCOUNT,
            price: price(100),
            quantity: qty(3),
            maker_fee: 0,
            taker_fee: 0,
        }
    );

    // Bid at 90 (5) and bid at 100 (2 remaining) should still be on book.
    reports.clear();
    book.execute(
        market_order(4, Side::Sell, 7, TimeInForce::IOC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Fill { quantity, .. } if quantity == qty(2)));
    assert!(matches!(reports[1], ExecutionReport::Fill { quantity, .. } if quantity == qty(5)));
    assert!(book.is_empty());
}

// -- Stop orders --

fn stop_order(id: u64, side: Side, trigger: u64, q: u64, tif: TimeInForce) -> Order {
    Order {
        id: OrderId(id),
        account: TEST_ACCOUNT,
        side,
        order_type: OrderType::Stop {
            trigger_price: price(trigger),
        },
        time_in_force: tif,
        quantity: qty(q),
        stp: SelfTradeProtection::Allow,
        expiry_ns: 0,
    }
}

fn stop_limit_order(
    id: u64,
    side: Side,
    trigger: u64,
    limit: u64,
    q: u64,
    tif: TimeInForce,
) -> Order {
    Order {
        id: OrderId(id),
        account: TEST_ACCOUNT,
        side,
        order_type: OrderType::StopLimit {
            trigger_price: price(trigger),
            limit_price: price(limit),
        },
        time_in_force: tif,
        quantity: qty(q),
        stp: SelfTradeProtection::Allow,
        expiry_ns: 0,
    }
}

#[test]
fn stop_buy_triggers_on_trade_at_trigger_price() {
    let mut book = OrderBook::new(TEST_SYMBOL);
    let mut reports = Vec::new();

    // Place a resting ask at 100 and a stop buy that triggers at 100.
    book.execute(
        limit_order(1, Side::Sell, 100, 10, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    book.execute(
        stop_order(2, Side::Buy, 100, 5, TimeInForce::IOC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    reports.clear();

    // A trade at 100 should trigger the stop buy.
    book.execute(
        limit_order(3, Side::Buy, 100, 5, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );

    // Order 3 fills against order 1 (5@100), then stop triggers and fills (5@100).
    assert!(matches!(
        reports[0],
        ExecutionReport::Fill {
            taker_order_id: OrderId(3),
            ..
        }
    ));
    assert_eq!(
        reports[1],
        ExecutionReport::Triggered {
            order_id: OrderId(2),
            symbol: TEST_SYMBOL,
            account: TEST_ACCOUNT,
            trigger_price: price(100),
        }
    );
    assert!(matches!(
        reports[2],
        ExecutionReport::Fill {
            taker_order_id: OrderId(2),
            ..
        }
    ));
    assert!(book.is_empty());
}

#[test]
fn stop_sell_triggers_on_trade_at_trigger_price() {
    let mut book = OrderBook::new(TEST_SYMBOL);
    let mut reports = Vec::new();

    // Place a resting bid at 100 and a stop sell that triggers at 100.
    book.execute(
        limit_order(1, Side::Buy, 100, 10, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    book.execute(
        stop_order(2, Side::Sell, 100, 5, TimeInForce::IOC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    reports.clear();

    // A trade at 100 should trigger the stop sell.
    book.execute(
        limit_order(3, Side::Sell, 100, 5, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );

    assert!(matches!(
        reports[0],
        ExecutionReport::Fill {
            taker_order_id: OrderId(3),
            ..
        }
    ));
    assert_eq!(
        reports[1],
        ExecutionReport::Triggered {
            order_id: OrderId(2),
            symbol: TEST_SYMBOL,
            account: TEST_ACCOUNT,
            trigger_price: price(100),
        }
    );
    assert!(matches!(
        reports[2],
        ExecutionReport::Fill {
            taker_order_id: OrderId(2),
            ..
        }
    ));
    assert!(book.is_empty());
}

#[test]
fn stop_buy_does_not_trigger_below_price() {
    let mut book = OrderBook::new(TEST_SYMBOL);
    let mut reports = Vec::new();

    // Stop buy at 110, but trade happens at 100.
    book.execute(
        limit_order(1, Side::Sell, 100, 10, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    book.execute(
        stop_order(2, Side::Buy, 110, 5, TimeInForce::IOC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    reports.clear();

    book.execute(
        limit_order(3, Side::Buy, 100, 5, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );

    // Only the limit order fills, stop is NOT triggered.
    assert_eq!(reports.len(), 1);
    assert!(matches!(
        reports[0],
        ExecutionReport::Fill {
            taker_order_id: OrderId(3),
            ..
        }
    ));
    // Stop and remaining ask still on book.
    assert!(!book.is_empty());
}

/// Stop buy triggers when trade price is ABOVE the trigger price
/// (not just at it). Trigger condition: trade_price >= trigger_price.
#[test]
fn stop_buy_triggers_above_trigger_price() {
    let mut book = OrderBook::new(TEST_SYMBOL);
    let mut reports = Vec::new();

    // Resting asks at 100 and 110.
    book.execute(
        limit_order(1, Side::Sell, 100, 5, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    book.execute(
        limit_order(2, Side::Sell, 110, 10, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    // Stop buy triggers at 95 — should fire when trade happens at 100.
    book.execute(
        stop_order(3, Side::Buy, 95, 5, TimeInForce::IOC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    reports.clear();

    // Trade at 100 (above trigger 95) → stop should trigger.
    book.execute(
        limit_order(4, Side::Buy, 100, 5, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );

    assert!(
        reports.iter().any(|r| matches!(
            r,
            ExecutionReport::Triggered {
                order_id: OrderId(3),
                ..
            }
        )),
        "stop buy should trigger when trade price (100) > trigger price (95)"
    );
}

/// Stop sell triggers when trade price is BELOW the trigger price
/// (not just at it). Trigger condition: trade_price <= trigger_price.
#[test]
fn stop_sell_triggers_below_trigger_price() {
    let mut book = OrderBook::new(TEST_SYMBOL);
    let mut reports = Vec::new();

    // Resting bids at 100 and 90.
    book.execute(
        limit_order(1, Side::Buy, 100, 5, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    book.execute(
        limit_order(2, Side::Buy, 90, 10, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    // Stop sell triggers at 105 — should fire when trade happens at 100.
    book.execute(
        stop_order(3, Side::Sell, 105, 5, TimeInForce::IOC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    reports.clear();

    // Trade at 100 (below trigger 105) → stop should trigger.
    book.execute(
        limit_order(4, Side::Sell, 100, 5, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );

    assert!(
        reports.iter().any(|r| matches!(
            r,
            ExecutionReport::Triggered {
                order_id: OrderId(3),
                ..
            }
        )),
        "stop sell should trigger when trade price (100) < trigger price (105)"
    );
}

/// Stop sell does NOT trigger when trade price is above trigger price.
#[test]
fn stop_sell_does_not_trigger_above_price() {
    let mut book = OrderBook::new(TEST_SYMBOL);
    let mut reports = Vec::new();

    // Resting bid at 100 and stop sell at trigger 90.
    book.execute(
        limit_order(1, Side::Buy, 100, 10, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    book.execute(
        stop_order(2, Side::Sell, 90, 5, TimeInForce::IOC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    reports.clear();

    // Trade at 100 (above trigger 90) → stop sell should NOT trigger.
    book.execute(
        limit_order(3, Side::Sell, 100, 5, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );

    assert_eq!(reports.len(), 1, "only the limit fill, no trigger");
    assert!(matches!(
        reports[0],
        ExecutionReport::Fill {
            taker_order_id: OrderId(3),
            ..
        }
    ));
}

#[test]
fn stop_limit_triggers_and_rests() {
    let mut book = OrderBook::new(TEST_SYMBOL);
    let mut reports = Vec::new();

    // Resting ask at 100, stop-limit buy: trigger at 100, limit at 95.
    book.execute(
        limit_order(1, Side::Sell, 100, 10, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    book.execute(
        stop_limit_order(2, Side::Buy, 100, 95, 5, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    reports.clear();

    // Trade at 100 triggers the stop, but limit price 95 < ask 100, so it rests.
    book.execute(
        limit_order(3, Side::Buy, 100, 5, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );

    assert!(matches!(
        reports[0],
        ExecutionReport::Fill {
            taker_order_id: OrderId(3),
            ..
        }
    ));
    assert_eq!(
        reports[1],
        ExecutionReport::Triggered {
            order_id: OrderId(2),
            symbol: TEST_SYMBOL,
            account: TEST_ACCOUNT,
            trigger_price: price(100),
        }
    );
    // The stop-limit becomes a limit buy at 95, which rests (no asks at 95).
    assert!(matches!(
        reports[2],
        ExecutionReport::Placed {
            order_id: OrderId(2),
            symbol: TEST_SYMBOL,
            account: TEST_ACCOUNT,
            side: Side::Buy,
            ..
        }
    ));
}

#[test]
/// Cancelling a stop in the middle of a multi-stop trigger level
/// must splice cleanly. Mirrors `cancel_middle_of_level_preserves_fifo`
/// for the stop side.
fn stop_cancel_middle_of_level_preserves_fifo() {
    let mut book = OrderBook::new(TEST_SYMBOL);
    let mut reports = Vec::new();

    // Three buy stops at the same trigger price — head=1, mid=2, tail=3.
    for id in 1..=3 {
        book.execute(
            stop_order(id, Side::Buy, 100, 5, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
    }
    reports.clear();

    // Cancel the middle stop. Subsequent trigger must fire 1 then 3.
    book.cancel(TEST_ACCOUNT, OrderId(2), &mut reports);
    assert!(matches!(
        reports[0],
        ExecutionReport::Cancelled {
            order_id: OrderId(2),
            ..
        }
    ));
    reports.clear();

    // Place a sell at 100 then a buy that crosses it to drive a trade
    // at 100, which triggers both buy stops. They convert to market
    // orders but the book is empty post-trade, so they reject.
    // What matters here is the *Triggered* report order: 1 then 3.
    book.execute(
        limit_order(10, Side::Sell, 100, 1, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    book.execute(
        limit_order(11, Side::Buy, 100, 1, TimeInForce::IOC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    let triggered: Vec<OrderId> = reports
        .iter()
        .filter_map(|r| match r {
            ExecutionReport::Triggered { order_id, .. } => Some(*order_id),
            _ => None,
        })
        .collect();
    assert_eq!(triggered, vec![OrderId(1), OrderId(3)]);
}

/// Stops at the same trigger price must fire FIFO. After 5 stops
/// are placed and a trade hits the trigger, the Triggered reports
/// must come out in placement order.
#[test]
fn stops_fire_in_fifo_order_at_same_trigger() {
    let mut book = OrderBook::new(TEST_SYMBOL);
    let mut reports = Vec::new();

    for id in 1..=5 {
        book.execute(
            stop_order(id, Side::Buy, 100, 1, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
    }
    reports.clear();

    // Drive a trade at 100 to fire all five.
    book.execute(
        limit_order(10, Side::Sell, 100, 1, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    book.execute(
        limit_order(11, Side::Buy, 100, 1, TimeInForce::IOC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    let triggered: Vec<OrderId> = reports
        .iter()
        .filter_map(|r| match r {
            ExecutionReport::Triggered { order_id, .. } => Some(*order_id),
            _ => None,
        })
        .collect();
    assert_eq!(
        triggered,
        vec![OrderId(1), OrderId(2), OrderId(3), OrderId(4), OrderId(5)]
    );
}

/// Stress slab reuse on the stop side. 200 cycles of place+cancel
/// followed by a final batch that must trigger in correct FIFO order.
/// Catches stale `prev`/`next` on reused stop slots.
#[test]
fn stop_repeated_alloc_free_preserves_fifo() {
    let mut book = OrderBook::new(TEST_SYMBOL);
    let mut reports = Vec::new();

    for id in 1..=200 {
        book.execute(
            stop_order(id, Side::Buy, 100, 1, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        book.cancel(TEST_ACCOUNT, OrderId(id), &mut reports);
    }
    reports.clear();

    // Final batch.
    for id in 1000..=1003 {
        book.execute(
            stop_order(id, Side::Buy, 100, 1, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
    }
    reports.clear();

    // Drive a trade to fire them.
    book.execute(
        limit_order(10, Side::Sell, 100, 1, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    book.execute(
        limit_order(11, Side::Buy, 100, 1, TimeInForce::IOC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    let triggered: Vec<OrderId> = reports
        .iter()
        .filter_map(|r| match r {
            ExecutionReport::Triggered { order_id, .. } => Some(*order_id),
            _ => None,
        })
        .collect();
    assert_eq!(
        triggered,
        vec![OrderId(1000), OrderId(1001), OrderId(1002), OrderId(1003)]
    );
}

#[test]
fn cancel_pending_stop_order() {
    let mut book = OrderBook::new(TEST_SYMBOL);
    let mut reports = Vec::new();

    book.execute(
        stop_order(1, Side::Buy, 100, 10, TimeInForce::IOC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    reports.clear();

    book.cancel(TEST_ACCOUNT, OrderId(1), &mut reports);

    assert_eq!(reports.len(), 1);
    assert_eq!(
        reports[0],
        ExecutionReport::Cancelled {
            order_id: OrderId(1),
            symbol: TEST_SYMBOL,
            account: TEST_ACCOUNT,
            remaining_quantity: qty(10),
        }
    );
    assert!(book.is_empty());
}

#[test]
fn cancelled_stop_does_not_trigger() {
    let mut book = OrderBook::new(TEST_SYMBOL);
    let mut reports = Vec::new();

    book.execute(
        limit_order(1, Side::Sell, 100, 10, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    book.execute(
        stop_order(2, Side::Buy, 100, 5, TimeInForce::IOC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );
    book.cancel(TEST_ACCOUNT, OrderId(2), &mut reports);
    reports.clear();

    // Trade at 100 — cancelled stop should not trigger.
    book.execute(
        limit_order(3, Side::Buy, 100, 10, TimeInForce::GTC),
        None,
        ReservationSlot::DUMMY,
        &mut reports,
    );

    assert_eq!(reports.len(), 1);
    assert!(matches!(reports[0], ExecutionReport::Fill { .. }));
    assert!(book.is_empty());
}
