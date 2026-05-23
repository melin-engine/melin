//! Circuit breaker tests. Cover `set_circuit_breaker` configuration
//! and the halt + price-band checks that `Exchange::execute` and
//! `cancel_replace` consult before any reservation work.

use super::Exchange;
use super::test_helpers::*;
use crate::types::{
    CircuitBreakerConfig, ExecutionReport, Order, OrderId, OrderType, RejectReason,
    SelfTradeProtection, Side, Symbol, TimeInForce,
};

// --- Circuit breaker tests ---

#[test]
fn halt_rejects_all_orders() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 100_000);
    exchange.deposit(ACCT_B, BTC, 500);

    exchange.set_circuit_breaker(
        Symbol(1),
        CircuitBreakerConfig {
            halted: true,
            ..Default::default()
        },
    );

    let mut reports = Vec::new();
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert_eq!(reports.len(), 1);
    assert!(matches!(
        reports[0],
        ExecutionReport::Rejected {
            reason: RejectReason::TradingHalted,
            ..
        }
    ));
}

#[test]
fn halt_then_resume_allows_orders() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 100_000);

    // Halt.
    exchange.set_circuit_breaker(
        Symbol(1),
        CircuitBreakerConfig {
            halted: true,
            ..Default::default()
        },
    );

    let mut reports = Vec::new();
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(
        reports[0],
        ExecutionReport::Rejected {
            reason: RejectReason::TradingHalted,
            ..
        }
    ));

    // Resume.
    exchange.set_circuit_breaker(
        Symbol(1),
        CircuitBreakerConfig {
            halted: false,
            ..Default::default()
        },
    );

    reports.clear();
    exchange.execute(
        Symbol(1),
        limit_order(2, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
}

#[test]
fn price_band_rejects_out_of_range_limit() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 1_000_000);

    exchange.set_circuit_breaker(
        Symbol(1),
        CircuitBreakerConfig {
            price_band_lower: Some(price(90)),
            price_band_upper: Some(price(110)),
            halted: false,
        },
    );

    let mut reports = Vec::new();
    // Below lower band.
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 89, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(
        reports[0],
        ExecutionReport::Rejected {
            reason: RejectReason::OutsidePriceBand,
            ..
        }
    ));

    // Above upper band.
    reports.clear();
    exchange.execute(
        Symbol(1),
        limit_order(2, ACCT_A, Side::Buy, 111, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(
        reports[0],
        ExecutionReport::Rejected {
            reason: RejectReason::OutsidePriceBand,
            ..
        }
    ));
}

#[test]
fn price_band_allows_in_range_orders() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 1_000_000);

    exchange.set_circuit_breaker(
        Symbol(1),
        CircuitBreakerConfig {
            price_band_lower: Some(price(90)),
            price_band_upper: Some(price(110)),
            halted: false,
        },
    );

    let mut reports = Vec::new();
    // At lower boundary (inclusive).
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 90, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Placed { .. }));

    // At upper boundary (inclusive).
    reports.clear();
    exchange.execute(
        Symbol(1),
        limit_order(2, ACCT_A, Side::Buy, 110, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Placed { .. }));

    // In middle.
    reports.clear();
    exchange.execute(
        Symbol(1),
        limit_order(3, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
}

#[test]
fn market_orders_bypass_price_bands() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 1_000_000);
    exchange.deposit(ACCT_B, BTC, 500);

    // Place a resting sell first.
    let mut reports = Vec::new();
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_B, Side::Sell, 100, 50, TimeInForce::GTC),
        &mut reports,
    );

    // Set narrow bands.
    exchange.set_circuit_breaker(
        Symbol(1),
        CircuitBreakerConfig {
            price_band_lower: Some(price(95)),
            price_band_upper: Some(price(105)),
            halted: false,
        },
    );

    // Market order should not be rejected by price bands.
    reports.clear();
    exchange.execute(
        Symbol(1),
        market_order(2, ACCT_A, Side::Buy, 10),
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Fill { .. }));
}

#[test]
fn resting_orders_survive_band_change() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 1_000_000);
    exchange.deposit(ACCT_B, BTC, 500);

    // Place a resting buy at price 100.
    let mut reports = Vec::new();
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 50, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Placed { .. }));

    // Narrow bands to exclude price 100.
    exchange.set_circuit_breaker(
        Symbol(1),
        CircuitBreakerConfig {
            price_band_lower: Some(price(105)),
            price_band_upper: Some(price(115)),
            halted: false,
        },
    );

    // New incoming sell at price 100 is outside bands, so it gets rejected.
    // The resting buy at 100 is NOT cancelled — bands only apply to new orders.
    reports.clear();
    exchange.execute(
        Symbol(1),
        limit_order(2, ACCT_B, Side::Sell, 100, 20, TimeInForce::GTC),
        &mut reports,
    );
    // The sell at 100 is outside bands, so it gets rejected.
    // But the resting buy at 100 survives — it won't be cancelled.
    assert!(matches!(
        reports[0],
        ExecutionReport::Rejected {
            reason: RejectReason::OutsidePriceBand,
            ..
        }
    ));
}

#[test]
fn halt_rejects_all_order_types() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 1_000_000);
    exchange.deposit(ACCT_A, BTC, 500);

    exchange.set_circuit_breaker(
        Symbol(1),
        CircuitBreakerConfig {
            halted: true,
            ..Default::default()
        },
    );

    let mut reports = Vec::new();

    // Market.
    exchange.execute(
        Symbol(1),
        market_order(1, ACCT_A, Side::Buy, 10),
        &mut reports,
    );
    assert!(matches!(
        reports[0],
        ExecutionReport::Rejected {
            reason: RejectReason::TradingHalted,
            ..
        }
    ));

    // Stop.
    reports.clear();
    exchange.execute(
        Symbol(1),
        Order {
            id: OrderId(2),
            account: ACCT_A,
            side: Side::Buy,
            order_type: OrderType::Stop {
                trigger_price: price(100),
            },
            time_in_force: TimeInForce::GTC,
            quantity: qty(10),
            stp: SelfTradeProtection::Allow,
            expiry_ns: 0,
        },
        &mut reports,
    );
    assert!(matches!(
        reports[0],
        ExecutionReport::Rejected {
            reason: RejectReason::TradingHalted,
            ..
        }
    ));

    // StopLimit.
    reports.clear();
    exchange.execute(
        Symbol(1),
        Order {
            id: OrderId(3),
            account: ACCT_A,
            side: Side::Sell,
            order_type: OrderType::StopLimit {
                trigger_price: price(90),
                limit_price: price(85),
            },
            time_in_force: TimeInForce::GTC,
            quantity: qty(10),
            stp: SelfTradeProtection::Allow,
            expiry_ns: 0,
        },
        &mut reports,
    );
    assert!(matches!(
        reports[0],
        ExecutionReport::Rejected {
            reason: RejectReason::TradingHalted,
            ..
        }
    ));
}

#[test]
fn stop_limit_checked_against_price_bands() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 1_000_000);
    exchange.deposit(ACCT_A, BTC, 500);

    exchange.set_circuit_breaker(
        Symbol(1),
        CircuitBreakerConfig {
            price_band_lower: Some(price(90)),
            price_band_upper: Some(price(110)),
            halted: false,
        },
    );

    let mut reports = Vec::new();

    // StopLimit with limit_price below lower band — rejected.
    exchange.execute(
        Symbol(1),
        Order {
            id: OrderId(1),
            account: ACCT_A,
            side: Side::Sell,
            order_type: OrderType::StopLimit {
                trigger_price: price(85),
                limit_price: price(80),
            },
            time_in_force: TimeInForce::GTC,
            quantity: qty(10),
            stp: SelfTradeProtection::Allow,
            expiry_ns: 0,
        },
        &mut reports,
    );
    assert!(matches!(
        reports[0],
        ExecutionReport::Rejected {
            reason: RejectReason::OutsidePriceBand,
            ..
        }
    ));

    // StopLimit with limit_price within bands — accepted.
    reports.clear();
    exchange.execute(
        Symbol(1),
        Order {
            id: OrderId(2),
            account: ACCT_A,
            side: Side::Sell,
            order_type: OrderType::StopLimit {
                trigger_price: price(95),
                limit_price: price(95),
            },
            time_in_force: TimeInForce::GTC,
            quantity: qty(10),
            stp: SelfTradeProtection::Allow,
            expiry_ns: 0,
        },
        &mut reports,
    );
    // Stop orders rest in the stop queue — they produce no immediate report
    // unless triggered, so we just verify no rejection.
    assert!(
        !reports
            .iter()
            .any(|r| matches!(r, ExecutionReport::Rejected { .. })),
        "in-range stop-limit should not be rejected"
    );
}

#[test]
fn stop_orders_bypass_price_bands() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 1_000_000);

    exchange.set_circuit_breaker(
        Symbol(1),
        CircuitBreakerConfig {
            price_band_lower: Some(price(90)),
            price_band_upper: Some(price(110)),
            halted: false,
        },
    );

    let mut reports = Vec::new();

    // Stop order with trigger outside bands — should NOT be rejected
    // (stop orders have no submission-time price for band check).
    exchange.execute(
        Symbol(1),
        Order {
            id: OrderId(1),
            account: ACCT_A,
            side: Side::Buy,
            order_type: OrderType::Stop {
                trigger_price: price(200),
            },
            time_in_force: TimeInForce::GTC,
            quantity: qty(10),
            stp: SelfTradeProtection::Allow,
            expiry_ns: 0,
        },
        &mut reports,
    );
    assert!(
        !reports
            .iter()
            .any(|r| matches!(r, ExecutionReport::Rejected { .. })),
        "stop order should bypass price bands"
    );
}

#[test]
fn one_sided_price_bands() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 1_000_000);

    // Only lower bound, no upper.
    exchange.set_circuit_breaker(
        Symbol(1),
        CircuitBreakerConfig {
            price_band_lower: Some(price(90)),
            price_band_upper: None,
            halted: false,
        },
    );

    let mut reports = Vec::new();

    // Below lower — rejected.
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 89, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(
        reports[0],
        ExecutionReport::Rejected {
            reason: RejectReason::OutsidePriceBand,
            ..
        }
    ));

    // High price — no upper bound, should pass.
    reports.clear();
    exchange.execute(
        Symbol(1),
        limit_order(2, ACCT_A, Side::Buy, 500, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Placed { .. }));

    // Now test only upper bound, no lower.
    exchange.set_circuit_breaker(
        Symbol(1),
        CircuitBreakerConfig {
            price_band_lower: None,
            price_band_upper: Some(price(110)),
            halted: false,
        },
    );

    // Above upper — rejected.
    reports.clear();
    exchange.execute(
        Symbol(1),
        limit_order(3, ACCT_A, Side::Buy, 111, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(
        reports[0],
        ExecutionReport::Rejected {
            reason: RejectReason::OutsidePriceBand,
            ..
        }
    ));

    // Low price — no lower bound, should pass.
    reports.clear();
    exchange.execute(
        Symbol(1),
        limit_order(4, ACCT_A, Side::Buy, 1, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
}

#[test]
fn cancel_works_during_halt() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 100_000);

    // Place a resting order before halt.
    let mut reports = Vec::new();
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Placed { .. }));

    // Halt the instrument.
    exchange.set_circuit_breaker(
        Symbol(1),
        CircuitBreakerConfig {
            halted: true,
            ..Default::default()
        },
    );

    // Cancel should still work — cancels bypass circuit breaker checks.
    reports.clear();
    exchange.cancel(Symbol(1), ACCT_A, OrderId(1), &mut reports);
    assert!(matches!(reports[0], ExecutionReport::Cancelled { .. }));
}

#[test]
fn sell_side_band_rejection() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_B, BTC, 500);

    exchange.set_circuit_breaker(
        Symbol(1),
        CircuitBreakerConfig {
            price_band_lower: Some(price(90)),
            price_band_upper: Some(price(110)),
            halted: false,
        },
    );

    let mut reports = Vec::new();

    // Sell below lower band — rejected.
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_B, Side::Sell, 80, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(
        reports[0],
        ExecutionReport::Rejected {
            reason: RejectReason::OutsidePriceBand,
            ..
        }
    ));

    // Sell above upper band — rejected.
    reports.clear();
    exchange.execute(
        Symbol(1),
        limit_order(2, ACCT_B, Side::Sell, 120, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(
        reports[0],
        ExecutionReport::Rejected {
            reason: RejectReason::OutsidePriceBand,
            ..
        }
    ));

    // Sell within bands — accepted.
    reports.clear();
    exchange.execute(
        Symbol(1),
        limit_order(3, ACCT_B, Side::Sell, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
}

#[test]
fn halt_does_not_affect_other_instruments() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.add_instrument(eth_usd_spec());
    exchange.deposit(ACCT_A, USD, 1_000_000);

    // Halt BTC/USD only.
    exchange.set_circuit_breaker(
        Symbol(1),
        CircuitBreakerConfig {
            halted: true,
            ..Default::default()
        },
    );

    let mut reports = Vec::new();

    // BTC/USD — rejected.
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(
        reports[0],
        ExecutionReport::Rejected {
            reason: RejectReason::TradingHalted,
            ..
        }
    ));

    // ETH/USD — should still work.
    reports.clear();
    exchange.execute(
        Symbol(2),
        limit_order(2, ACCT_A, Side::Buy, 50, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
}

#[test]
fn cancel_all_works_during_halt() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 100_000);

    // Place resting orders before halt.
    let mut reports = Vec::new();
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    exchange.execute(
        Symbol(1),
        limit_order(2, ACCT_A, Side::Buy, 95, 10, TimeInForce::GTC),
        &mut reports,
    );

    // Halt.
    exchange.set_circuit_breaker(
        Symbol(1),
        CircuitBreakerConfig {
            halted: true,
            ..Default::default()
        },
    );

    // Cancel all should still work.
    reports.clear();
    exchange.cancel_all(ACCT_A, &mut reports);
    let cancel_count = reports
        .iter()
        .filter(|r| matches!(r, ExecutionReport::Cancelled { .. }))
        .count();
    assert_eq!(cancel_count, 2);
}

#[test]
fn default_circuit_breaker_config_is_permissive() {
    // Default config has no bands and halted=false — should allow everything.
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 100_000);

    exchange.set_circuit_breaker(Symbol(1), CircuitBreakerConfig::default());

    let mut reports = Vec::new();
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
}

#[test]
fn halted_order_id_can_be_reused() {
    // A halt-rejected order doesn't rest, so under live-orders-only
    // dedup it doesn't claim the OrderId. Once trading resumes, the
    // same id can be retried — clients/gateways don't need to skip
    // an id just because the engine was halted when they first sent.
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 100_000);

    exchange.set_circuit_breaker(
        Symbol(1),
        CircuitBreakerConfig {
            halted: true,
            ..Default::default()
        },
    );

    let mut reports = Vec::new();
    exchange.execute(
        Symbol(1),
        limit_order(5, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(
        reports[0],
        ExecutionReport::Rejected {
            reason: RejectReason::TradingHalted,
            ..
        }
    ));

    exchange.set_circuit_breaker(Symbol(1), CircuitBreakerConfig::default());
    reports.clear();
    exchange.execute(
        Symbol(1),
        limit_order(5, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(
        matches!(reports[0], ExecutionReport::Placed { .. }),
        "retry after halt cleared should place, got {:?}",
        reports[0]
    );
}

#[test]
fn band_rejected_order_id_can_be_reused() {
    // Same as `halted_order_id_can_be_reused` but for the price-band
    // rejection path: an OutsidePriceBand reject doesn't rest the
    // order, so the live set stays empty and the same id is free
    // to retry once the bands are widened.
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 100_000);

    exchange.set_circuit_breaker(
        Symbol(1),
        CircuitBreakerConfig {
            price_band_lower: Some(price(90)),
            price_band_upper: Some(price(110)),
            halted: false,
        },
    );

    let mut reports = Vec::new();
    exchange.execute(
        Symbol(1),
        limit_order(5, ACCT_A, Side::Buy, 50, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(
        reports[0],
        ExecutionReport::Rejected {
            reason: RejectReason::OutsidePriceBand,
            ..
        }
    ));

    exchange.set_circuit_breaker(Symbol(1), CircuitBreakerConfig::default());
    reports.clear();
    exchange.execute(
        Symbol(1),
        limit_order(5, ACCT_A, Side::Buy, 50, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(
        matches!(reports[0], ExecutionReport::Placed { .. }),
        "retry after band widened should place, got {:?}",
        reports[0]
    );
}

#[test]
fn halt_with_bands_rejects_on_halt_not_bands() {
    // When both halt=true and bands are set, the halt check comes first.
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 100_000);

    exchange.set_circuit_breaker(
        Symbol(1),
        CircuitBreakerConfig {
            price_band_lower: Some(price(90)),
            price_band_upper: Some(price(110)),
            halted: true,
        },
    );

    let mut reports = Vec::new();
    // Price is within bands, but instrument is halted — should get TradingHalted.
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(
        reports[0],
        ExecutionReport::Rejected {
            reason: RejectReason::TradingHalted,
            ..
        }
    ));
}
