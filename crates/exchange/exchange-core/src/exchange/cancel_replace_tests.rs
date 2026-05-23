//! Tests for the cancel/replace validation pipeline in
//! `exchange/cancel_replace.rs`. Cover instrument-existence,
//! disable, circuit-breaker / price-band, risk-limit, would-cross,
//! reservation-adjustment, and time-priority outcomes.

use std::num::NonZeroU64;

use super::Exchange;
use super::test_helpers::*;
use crate::types::{
    CircuitBreakerConfig, ExecutionReport, OrderId, Price, RejectReason, RiskLimits, Side, Symbol,
    TimeInForce,
};

// -- Cancel-replace tests --

#[test]
fn cancel_replace_basic_price_change() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 50_000);

    let mut reports = Vec::new();

    // Place a limit buy at 100 for 10.
    exchange.execute(
        btc,
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
    reports.clear();

    // Cancel-replace to price 120 (same qty).
    exchange.cancel_replace(btc, ACCT_A, OrderId(1), price(120), qty(10), &mut reports);

    assert_eq!(reports.len(), 1);
    assert_eq!(
        reports[0],
        ExecutionReport::Replaced {
            order_id: OrderId(1),
            symbol: btc,
            account: ACCT_A,
            side: Side::Buy,
            old_price: price(100),
            new_price: price(120),
            old_remaining: qty(10),
            new_remaining: qty(10),
        }
    );

    // Old reservation was 100*10=1000, new is 120*10=1200.
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 1_200);
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).available, 48_800);
}

#[test]
fn cancel_replace_qty_decrease_keeps_priority() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 50_000);
    exchange.deposit(ACCT_B, BTC, 100);

    let mut reports = Vec::new();

    // Place two buys at same price. Order 1 is first in queue.
    exchange.execute(
        btc,
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    exchange.execute(
        btc,
        limit_order(2, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    // Cancel-replace order 1 to lower qty (5). Should keep priority.
    exchange.cancel_replace(btc, ACCT_A, OrderId(1), price(100), qty(5), &mut reports);
    assert!(matches!(reports[0], ExecutionReport::Replaced { .. }));
    reports.clear();

    // Sell 5 into the book — should match order 1 first (kept priority).
    exchange.execute(
        btc,
        limit_order(3, ACCT_B, Side::Sell, 100, 5, TimeInForce::GTC),
        &mut reports,
    );

    // Expect a fill against order 1 (maker), not order 2.
    let fill = reports
        .iter()
        .find(|r| matches!(r, ExecutionReport::Fill { .. }));
    assert!(fill.is_some());
    assert!(matches!(
        fill.unwrap(),
        ExecutionReport::Fill {
            maker_order_id: OrderId(1),
            taker_order_id: OrderId(3),
            ..
        }
    ));
}

#[test]
fn cancel_replace_qty_increase_loses_priority() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 50_000);
    exchange.deposit(ACCT_B, BTC, 100);

    let mut reports = Vec::new();

    // Place two buys at same price. Order 1 is first in queue.
    exchange.execute(
        btc,
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    exchange.execute(
        btc,
        limit_order(2, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    // Cancel-replace order 1 to higher qty (15). Should lose priority.
    exchange.cancel_replace(btc, ACCT_A, OrderId(1), price(100), qty(15), &mut reports);
    assert!(matches!(reports[0], ExecutionReport::Replaced { .. }));
    reports.clear();

    // Sell 5 into the book — should match order 2 first (order 1 lost priority).
    exchange.execute(
        btc,
        limit_order(3, ACCT_B, Side::Sell, 100, 5, TimeInForce::GTC),
        &mut reports,
    );

    let fill = reports
        .iter()
        .find(|r| matches!(r, ExecutionReport::Fill { .. }));
    assert!(fill.is_some());
    assert!(matches!(
        fill.unwrap(),
        ExecutionReport::Fill {
            maker_order_id: OrderId(2),
            taker_order_id: OrderId(3),
            ..
        }
    ));
}

#[test]
fn cancel_replace_insufficient_balance() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    // Only deposit enough for the initial order.
    exchange.deposit(ACCT_A, USD, 1_100);

    let mut reports = Vec::new();

    // Place buy at 100 for 10 (reserves 1000).
    exchange.execute(
        btc,
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
    reports.clear();

    // Cancel-replace to price 500 for 10 (would need 5000, only have 1100 total).
    exchange.cancel_replace(btc, ACCT_A, OrderId(1), price(500), qty(10), &mut reports);

    assert_eq!(reports.len(), 1);
    assert_eq!(
        reports[0],
        ExecutionReport::Rejected {
            order_id: OrderId(1),
            symbol: btc,
            account: ACCT_A,
            reason: RejectReason::InsufficientBalance,
        }
    );

    // Original order must still be on the book with original reservation.
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 1_000);
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).available, 100);
}

#[test]
fn cancel_replace_unknown_order() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());

    let mut reports = Vec::new();

    // Cancel-replace on an order ID that was never placed.
    exchange.cancel_replace(btc, ACCT_A, OrderId(999), price(100), qty(10), &mut reports);

    assert_eq!(reports.len(), 1);
    assert_eq!(
        reports[0],
        ExecutionReport::Rejected {
            order_id: OrderId(999),
            symbol: btc,
            account: ACCT_A,
            reason: RejectReason::UnknownOrder,
        }
    );
}

#[test]
fn cancel_replace_unknown_symbol() {
    let mut exchange = Exchange::new();
    // Don't add any instruments.

    let mut reports = Vec::new();

    exchange.cancel_replace(
        Symbol(42),
        ACCT_A,
        OrderId(1),
        price(100),
        qty(10),
        &mut reports,
    );

    assert_eq!(reports.len(), 1);
    assert_eq!(
        reports[0],
        ExecutionReport::Rejected {
            order_id: OrderId(1),
            symbol: Symbol(42),
            account: ACCT_A,
            reason: RejectReason::UnknownSymbol,
        }
    );
}

#[test]
fn cancel_replace_price_would_cross() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 50_000);
    exchange.deposit(ACCT_B, BTC, 100);

    let mut reports = Vec::new();

    // Place a buy at 100.
    exchange.execute(
        btc,
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    // Place an ask at 110.
    exchange.execute(
        btc,
        limit_order(2, ACCT_B, Side::Sell, 110, 10, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    // Cancel-replace the buy to price 110 — would cross the ask.
    exchange.cancel_replace(btc, ACCT_A, OrderId(1), price(110), qty(10), &mut reports);

    assert_eq!(reports.len(), 1);
    assert_eq!(
        reports[0],
        ExecutionReport::Rejected {
            order_id: OrderId(1),
            symbol: btc,
            account: ACCT_A,
            reason: RejectReason::PriceWouldCross,
        }
    );

    // Original order must remain intact.
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 1_000);
}

#[test]
fn cancel_replace_trading_halted() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 50_000);

    let mut reports = Vec::new();

    // Place a buy at 100.
    exchange.execute(
        btc,
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
    reports.clear();

    // Halt trading.
    exchange.set_circuit_breaker(
        btc,
        CircuitBreakerConfig {
            halted: true,
            ..Default::default()
        },
    );

    // Cancel-replace should be rejected.
    exchange.cancel_replace(btc, ACCT_A, OrderId(1), price(120), qty(10), &mut reports);

    assert_eq!(reports.len(), 1);
    assert_eq!(
        reports[0],
        ExecutionReport::Rejected {
            order_id: OrderId(1),
            symbol: btc,
            account: ACCT_A,
            reason: RejectReason::TradingHalted,
        }
    );

    // Original order remains.
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 1_000);
}

#[test]
fn cancel_replace_outside_price_band() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 500_000);

    let mut reports = Vec::new();

    // Place a buy at 100.
    exchange.execute(
        btc,
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
    reports.clear();

    // Set price bands [90, 110].
    exchange.set_circuit_breaker(
        btc,
        CircuitBreakerConfig {
            price_band_lower: Some(price(90)),
            price_band_upper: Some(price(110)),
            halted: false,
        },
    );

    // Cancel-replace to price 120 — outside upper band.
    exchange.cancel_replace(btc, ACCT_A, OrderId(1), price(120), qty(10), &mut reports);

    assert_eq!(reports.len(), 1);
    assert_eq!(
        reports[0],
        ExecutionReport::Rejected {
            order_id: OrderId(1),
            symbol: btc,
            account: ACCT_A,
            reason: RejectReason::OutsidePriceBand,
        }
    );

    // Original order remains.
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 1_000);
}

#[test]
fn cancel_replace_exceeds_max_order_qty() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 500_000);

    let mut reports = Vec::new();

    // Place a buy at 100 for 10.
    exchange.execute(
        btc,
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
    reports.clear();

    // Set max order qty to 20.
    exchange.set_risk_limits(
        btc,
        RiskLimits {
            max_order_qty: Some(qty(20)),
            max_order_notional: None,
        },
    );

    // Cancel-replace to qty 25 — exceeds limit.
    exchange.cancel_replace(btc, ACCT_A, OrderId(1), price(100), qty(25), &mut reports);

    assert_eq!(reports.len(), 1);
    assert_eq!(
        reports[0],
        ExecutionReport::Rejected {
            order_id: OrderId(1),
            symbol: btc,
            account: ACCT_A,
            reason: RejectReason::ExceedsMaxOrderQty,
        }
    );

    // Original order remains with original qty.
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 1_000);
}

#[test]
fn cancel_replace_partially_filled_order() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 50_000);
    exchange.deposit(ACCT_B, BTC, 100);

    let mut reports = Vec::new();

    // Place a limit buy for 100 at price 100 (reserves 10_000).
    exchange.execute(
        btc,
        limit_order(1, ACCT_A, Side::Buy, 100, 100, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
    reports.clear();

    // Sell 30 into it — partial fill, remaining = 70.
    exchange.execute(
        btc,
        limit_order(2, ACCT_B, Side::Sell, 100, 30, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Fill { .. }));
    reports.clear();

    // Cancel-replace remaining to qty 50 at price 90.
    exchange.cancel_replace(btc, ACCT_A, OrderId(1), price(90), qty(50), &mut reports);

    assert_eq!(reports.len(), 1);
    assert_eq!(
        reports[0],
        ExecutionReport::Replaced {
            order_id: OrderId(1),
            symbol: btc,
            account: ACCT_A,
            side: Side::Buy,
            old_price: price(100),
            new_price: price(90),
            old_remaining: qty(70),
            new_remaining: qty(50),
        }
    );

    // New reservation should be 90*50=4500.
    // Buyer started with 50_000, spent 30*100=3000 on fills.
    // Remaining USD = 50_000 - 3000 = 47_000 total.
    // Reserved = 4500, available = 42_500.
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 4_500);
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).available, 42_500);
    // Buyer received 30 BTC from the fill.
    assert_eq!(exchange.accounts().balance(ACCT_A, BTC).available, 30);
}

#[test]
fn cancel_replace_sell_order() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, BTC, 100);

    let mut reports = Vec::new();

    // Place a limit sell at 200 for 10 (reserves 10 BTC).
    exchange.execute(
        btc,
        limit_order(1, ACCT_A, Side::Sell, 200, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
    reports.clear();

    // Cancel-replace to price 180, qty 8.
    exchange.cancel_replace(btc, ACCT_A, OrderId(1), price(180), qty(8), &mut reports);

    assert_eq!(reports.len(), 1);
    assert_eq!(
        reports[0],
        ExecutionReport::Replaced {
            order_id: OrderId(1),
            symbol: btc,
            account: ACCT_A,
            side: Side::Sell,
            old_price: price(200),
            new_price: price(180),
            old_remaining: qty(10),
            new_remaining: qty(8),
        }
    );

    // Sell reservation is qty-based: was 10, now 8. Released 2 back.
    assert_eq!(exchange.accounts().balance(ACCT_A, BTC).reserved, 8);
    assert_eq!(exchange.accounts().balance(ACCT_A, BTC).available, 92);
}

#[test]
fn cancel_replace_noop_same_price_same_qty() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 50_000);

    let mut reports = Vec::new();

    // Place a limit buy at 100 for 10.
    exchange.execute(
        btc,
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    // Cancel-replace with same price and qty — should succeed as a no-op.
    exchange.cancel_replace(btc, ACCT_A, OrderId(1), price(100), qty(10), &mut reports);

    assert_eq!(reports.len(), 1);
    assert_eq!(
        reports[0],
        ExecutionReport::Replaced {
            order_id: OrderId(1),
            symbol: btc,
            account: ACCT_A,
            side: Side::Buy,
            old_price: price(100),
            new_price: price(100),
            old_remaining: qty(10),
            new_remaining: qty(10),
        }
    );

    // Balances unchanged.
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 1_000);
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).available, 49_000);
}

#[test]
fn cancel_replace_above_upper_price_band() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 50_000);
    exchange.set_circuit_breaker(
        btc,
        CircuitBreakerConfig {
            price_band_lower: Some(price(80)),
            price_band_upper: Some(price(120)),
            halted: false,
        },
    );

    let mut reports = Vec::new();

    exchange.execute(
        btc,
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    // Replace to price 130 — above upper band.
    exchange.cancel_replace(btc, ACCT_A, OrderId(1), price(130), qty(10), &mut reports);
    assert!(matches!(
        reports[0],
        ExecutionReport::Rejected {
            reason: RejectReason::OutsidePriceBand,
            ..
        }
    ));
    // Original order intact.
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 1_000);
}

#[test]
fn cancel_replace_exceeds_max_notional() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 500_000);
    exchange.set_risk_limits(
        btc,
        RiskLimits {
            max_order_qty: None,
            max_order_notional: Some(10_000),
        },
    );

    let mut reports = Vec::new();

    exchange.execute(
        btc,
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    // Replace to 200*100 = 20_000 notional — exceeds 10_000 limit.
    exchange.cancel_replace(btc, ACCT_A, OrderId(1), price(200), qty(100), &mut reports);
    assert!(matches!(
        reports[0],
        ExecutionReport::Rejected {
            reason: RejectReason::ExceedsMaxNotional,
            ..
        }
    ));
    // Original order intact.
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 1_000);
}

#[test]
fn cancel_replace_sell_price_would_cross_bid() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, BTC, 100);
    exchange.deposit(ACCT_B, USD, 50_000);

    let mut reports = Vec::new();

    // Resting bid at 100.
    exchange.execute(
        btc,
        limit_order(1, ACCT_B, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    // Resting ask at 200.
    exchange.execute(
        btc,
        limit_order(2, ACCT_A, Side::Sell, 200, 10, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    // Replace ask to price 100 — would cross the bid. Rejected.
    exchange.cancel_replace(btc, ACCT_A, OrderId(2), price(100), qty(10), &mut reports);
    assert!(matches!(
        reports[0],
        ExecutionReport::Rejected {
            reason: RejectReason::PriceWouldCross,
            ..
        }
    ));
    // Original order intact.
    assert_eq!(exchange.accounts().balance(ACCT_A, BTC).reserved, 10);
}

#[test]
fn cancel_replace_price_overflow_rejected() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, u64::MAX / 2);

    let mut reports = Vec::new();

    exchange.execute(
        btc,
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    // Replace to a price/qty combination that overflows u64.
    // Price close to u64::MAX, qty > 1 → overflow.
    let huge_price = Price(NonZeroU64::new(u64::MAX / 2).unwrap());
    exchange.cancel_replace(btc, ACCT_A, OrderId(1), huge_price, qty(3), &mut reports);
    assert!(matches!(
        reports[0],
        ExecutionReport::Rejected {
            reason: RejectReason::InsufficientBalance,
            ..
        }
    ));
    // Original order intact.
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 1_000);
}
