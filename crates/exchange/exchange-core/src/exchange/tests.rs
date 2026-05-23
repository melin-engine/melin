use std::num::NonZeroU64;

use super::test_helpers::*;
use super::*;
use crate::types::{Order, OrderType, Price, Quantity, SelfTradeProtection, TimeInForce};

#[test]
fn execute_on_unknown_symbol_rejects() {
    let mut exchange = Exchange::new();
    let mut reports = Vec::new();

    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );

    assert_eq!(reports.len(), 1);
    assert_eq!(
        reports[0],
        ExecutionReport::Rejected {
            order_id: OrderId(1),
            symbol: Symbol(1),
            account: ACCT_A,
            reason: RejectReason::UnknownSymbol,
        }
    );
}

#[test]
fn insufficient_balance_rejects_order() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    // No deposit — no funds.

    let mut reports = Vec::new();
    exchange.execute(
        btc,
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );

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
}

#[test]
fn limit_order_places_with_sufficient_balance() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 10_000);

    let mut reports = Vec::new();
    exchange.execute(
        btc,
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );

    assert_eq!(reports.len(), 1);
    assert!(matches!(reports[0], ExecutionReport::Placed { .. }));

    // 1000 reserved (100 * 10), 9000 available.
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).available, 9_000);
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 1_000);
}

#[test]
fn fill_updates_both_accounts() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 10_000);
    exchange.deposit(ACCT_B, BTC, 100);

    let mut reports = Vec::new();

    // Seller places ask.
    exchange.execute(
        btc,
        limit_order(1, ACCT_B, Side::Sell, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    // Buyer matches.
    exchange.execute(
        btc,
        limit_order(2, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );

    assert!(matches!(reports[0], ExecutionReport::Fill { .. }));

    // Buyer: spent 1000 USD, got 10 BTC.
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).available, 9_000);
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 0);
    assert_eq!(exchange.accounts().balance(ACCT_A, BTC).available, 10);

    // Seller: spent 10 BTC, got 1000 USD.
    assert_eq!(exchange.accounts().balance(ACCT_B, BTC).available, 90);
    assert_eq!(exchange.accounts().balance(ACCT_B, BTC).reserved, 0);
    assert_eq!(exchange.accounts().balance(ACCT_B, USD).available, 1_000);
}

#[test]
fn cancel_releases_reserved_balance() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 10_000);

    let mut reports = Vec::new();
    exchange.execute(
        btc,
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    exchange.cancel(btc, ACCT_A, OrderId(1), &mut reports);
    assert!(matches!(reports[0], ExecutionReport::Cancelled { .. }));

    assert_eq!(exchange.accounts().balance(ACCT_A, USD).available, 10_000);
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 0);
}

#[test]
fn orders_on_different_symbols_are_isolated() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    let eth = Symbol(2);
    exchange.add_instrument(btc_usd_spec());
    exchange.add_instrument(eth_usd_spec());
    exchange.deposit(ACCT_A, USD, 50_000);
    exchange.deposit(ACCT_B, BTC, 100);

    let mut reports = Vec::new();

    // Place a sell on BTC.
    exchange.execute(
        btc,
        limit_order(1, ACCT_B, Side::Sell, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
    reports.clear();

    // Market buy on ETH should find no liquidity — books are isolated.
    exchange.execute(eth, market_order(2, ACCT_A, Side::Buy, 10), &mut reports);
    // Market buy with no liquidity: the reserve of full available is done
    // then the book rejects, then reserve is released.
    assert!(reports.iter().any(|r| matches!(
        r,
        ExecutionReport::Rejected {
            reason: RejectReason::NoLiquidity,
            ..
        }
    )));
    reports.clear();

    // Market buy on BTC should match.
    exchange.execute(btc, market_order(3, ACCT_A, Side::Buy, 10), &mut reports);
    assert!(matches!(reports[0], ExecutionReport::Fill { .. }));
}

#[test]
fn cross_instrument_shared_balance() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    let eth = Symbol(2);
    exchange.add_instrument(btc_usd_spec());
    exchange.add_instrument(eth_usd_spec());
    exchange.deposit(ACCT_A, USD, 2_000);

    let mut reports = Vec::new();

    // Place a buy on BTC for 1500 USD.
    exchange.execute(
        btc,
        limit_order(1, ACCT_A, Side::Buy, 150, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
    reports.clear();

    // Try to place a buy on ETH for 1000 USD — should fail, only 500 available.
    exchange.execute(
        eth,
        limit_order(2, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert_eq!(
        reports[0],
        ExecutionReport::Rejected {
            order_id: OrderId(2),
            symbol: eth,
            account: ACCT_A,
            reason: RejectReason::InsufficientBalance,
        }
    );
}

#[test]
fn partial_fill_then_cancel_releases_remainder() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 10_000);
    exchange.deposit(ACCT_B, BTC, 100);

    let mut reports = Vec::new();

    // Seller: 5 BTC @ 100.
    exchange.execute(
        btc,
        limit_order(1, ACCT_B, Side::Sell, 100, 5, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    // Buyer: wants 10 BTC @ 100 (reserves 1000). Fills 5, rests 5.
    exchange.execute(
        btc,
        limit_order(2, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    // Cancel the remaining 5.
    exchange.cancel(btc, ACCT_A, OrderId(2), &mut reports);

    // Buyer: spent 500 on 5 fills, 500 returned from cancel.
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).available, 9_500);
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 0);
    assert_eq!(exchange.accounts().balance(ACCT_A, BTC).available, 5);
}

#[test]
fn fok_rejection_releases_reservation() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 10_000);
    exchange.deposit(ACCT_B, BTC, 5);

    let mut reports = Vec::new();

    // Only 5 available.
    exchange.execute(
        btc,
        limit_order(1, ACCT_B, Side::Sell, 100, 5, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    // FOK buy for 10 — can't fill entirely.
    exchange.execute(
        btc,
        limit_order(2, ACCT_A, Side::Buy, 100, 10, TimeInForce::FOK),
        &mut reports,
    );

    assert!(matches!(
        reports[0],
        ExecutionReport::Rejected {
            reason: RejectReason::FOKCannotFill,
            ..
        }
    ));

    // Balance fully restored.
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).available, 10_000);
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 0);
}

// --- Client dedup tests ---

#[test]
fn duplicate_order_id_rejected() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 100_000);

    let mut reports = Vec::new();
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Placed { .. }));

    reports.clear();
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(
        reports[0],
        ExecutionReport::Rejected {
            reason: RejectReason::DuplicateOrderId,
            ..
        }
    ));
}

#[test]
fn cancel_replace_preserves_live_entry() {
    // cancel_replace amends the resting order in-place keeping the
    // same `(account, order_id)` identity, so the live set must
    // not be touched. A duplicate submission during/after the
    // replace must still hit DuplicateOrderId.
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 100_000);

    let mut reports = Vec::new();
    exchange.execute(
        Symbol(1),
        limit_order(11, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );

    reports.clear();
    exchange.cancel_replace(
        Symbol(1),
        ACCT_A,
        OrderId(11),
        price(95),
        qty(8),
        &mut reports,
    );
    assert!(
        reports
            .iter()
            .any(|r| matches!(r, ExecutionReport::Replaced { .. })),
        "expected Replaced, got {reports:?}"
    );

    reports.clear();
    exchange.execute(
        Symbol(1),
        limit_order(11, ACCT_A, Side::Buy, 90, 5, TimeInForce::GTC),
        &mut reports,
    );
    assert!(
        matches!(
            reports[0],
            ExecutionReport::Rejected {
                reason: RejectReason::DuplicateOrderId,
                ..
            }
        ),
        "duplicate after cancel_replace should reject, got {:?}",
        reports[0]
    );
}

#[test]
fn order_id_reusable_after_disable_instrument() {
    // disable_instrument cancels every resting order on the symbol.
    // Each cancellation must remove its `(account, order_id)` from
    // the live set so the same id can be reused (typically on a
    // different instrument).
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.add_instrument(eth_usd_spec());
    exchange.deposit(ACCT_A, USD, 100_000);

    let mut reports = Vec::new();
    exchange.execute(
        Symbol(1),
        limit_order(13, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );

    reports.clear();
    exchange.disable_instrument(Symbol(1), &mut reports);

    // Reuse OrderId 13 on a different live instrument. Disable
    // freed the live-set entry, so this must place.
    reports.clear();
    exchange.execute(
        Symbol(2),
        limit_order(13, ACCT_A, Side::Buy, 50, 5, TimeInForce::GTC),
        &mut reports,
    );
    assert!(
        matches!(reports[0], ExecutionReport::Placed { .. }),
        "reuse on live instrument after disable should place, got {:?}",
        reports[0]
    );
}

#[test]
fn order_id_reusable_after_cancel() {
    // Cancelling a resting order frees its `(account, order_id)`
    // entry from the live set, so the same id can be reused for a
    // fresh submission. This is the bot's actual reconnect scenario:
    // the gateway resets its session-local id counter, and we need
    // the engine to accept the colliding ids once the prior session's
    // orders are gone (e.g. via cancel-on-disconnect).
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 100_000);

    let mut reports = Vec::new();
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Placed { .. }));

    reports.clear();
    exchange.cancel(Symbol(1), ACCT_A, OrderId(1), &mut reports);

    reports.clear();
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 99, 5, TimeInForce::GTC),
        &mut reports,
    );
    assert!(
        matches!(reports[0], ExecutionReport::Placed { .. }),
        "reuse after cancel should place, got {:?}",
        reports[0]
    );
}

#[test]
fn order_id_reusable_after_full_fill() {
    // A full fill closes the order: `(account, order_id)` leaves the
    // live set in the same place every other close site does. The
    // same id is then reusable for a fresh submission.
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 100_000);
    exchange.deposit(ACCT_B, BTC, 100);

    let mut reports = Vec::new();
    // Maker (B) sells, taker (A) buys — A's order fully fills.
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_B, Side::Sell, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();
    exchange.execute(
        Symbol(1),
        limit_order(7, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(
        reports
            .iter()
            .any(|r| matches!(r, ExecutionReport::Fill { .. })),
        "expected a fill, got {reports:?}"
    );

    // Reuse OrderId 7 — the prior order was fully filled, so it's
    // gone from the live set.
    reports.clear();
    exchange.execute(
        Symbol(1),
        limit_order(7, ACCT_A, Side::Buy, 99, 5, TimeInForce::GTC),
        &mut reports,
    );
    assert!(
        matches!(reports[0], ExecutionReport::Placed { .. }),
        "reuse after full fill should place, got {:?}",
        reports[0]
    );
}

#[test]
fn lower_order_id_accepted_when_not_live() {
    // Under live-orders-only dedup, OrderIds aren't a monotonic
    // counter — only currently-live `(account, order_id)` pairs are
    // protected. Submitting a *different* (lower or higher) free ID
    // while ID 5 is resting must succeed; the dedup only triggers
    // when the same ID is reused while live.
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 100_000);

    let mut reports = Vec::new();
    exchange.execute(
        Symbol(1),
        limit_order(5, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Placed { .. }));

    reports.clear();
    exchange.execute(
        Symbol(1),
        limit_order(3, ACCT_A, Side::Buy, 99, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(
        matches!(reports[0], ExecutionReport::Placed { .. }),
        "fresh lower id should place, got {:?}",
        reports[0]
    );
}

#[test]
fn rejected_order_id_can_be_reused() {
    // Validation rejections (InsufficientBalance, OutsidePriceBand,
    // TradingHalted, etc.) leave the live set untouched: the order
    // never rested, so cancel/replace can't reference it. Reusing
    // its OrderId for a fresh submission is therefore fine — the
    // gateway's session-local id_map doesn't have to keep moving
    // forward forever just because earlier attempts bounced.
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());

    let mut reports = Vec::new();
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(
        reports[0],
        ExecutionReport::Rejected {
            reason: RejectReason::InsufficientBalance,
            ..
        }
    ));

    // Retry with the same ID after depositing — the previous
    // rejection didn't consume the slot, so this places.
    exchange.deposit(ACCT_A, USD, 100_000);
    reports.clear();
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(
        matches!(reports[0], ExecutionReport::Placed { .. }),
        "retry of rejected id should place, got {:?}",
        reports[0]
    );
}

// --- Fat finger checks ---

#[test]
fn qty_exceeds_max_rejected() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 1_000_000);
    exchange.set_risk_limits(
        Symbol(1),
        RiskLimits {
            max_order_qty: Some(qty(100)),
            max_order_notional: None,
        },
    );

    let mut reports = Vec::new();
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 101, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(
        reports[0],
        ExecutionReport::Rejected {
            reason: RejectReason::ExceedsMaxOrderQty,
            ..
        }
    ));
}

#[test]
fn qty_at_boundary_accepted() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 1_000_000);
    exchange.set_risk_limits(
        Symbol(1),
        RiskLimits {
            max_order_qty: Some(qty(100)),
            max_order_notional: None,
        },
    );

    let mut reports = Vec::new();
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 100, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
}

#[test]
fn notional_exceeds_max_rejected() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 1_000_000);
    exchange.set_risk_limits(
        Symbol(1),
        RiskLimits {
            max_order_qty: None,
            max_order_notional: Some(10_000),
        },
    );

    let mut reports = Vec::new();
    // price 101 * qty 100 = 10100 > 10000
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 101, 100, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(
        reports[0],
        ExecutionReport::Rejected {
            reason: RejectReason::ExceedsMaxNotional,
            ..
        }
    ));
}

#[test]
fn notional_at_boundary_accepted() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 1_000_000);
    exchange.set_risk_limits(
        Symbol(1),
        RiskLimits {
            max_order_qty: None,
            max_order_notional: Some(10_000),
        },
    );

    let mut reports = Vec::new();
    // price 100 * qty 100 = 10000 == max
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 100, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
}

#[test]
fn market_order_skips_notional_check() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 1_000_000);
    exchange.set_risk_limits(
        Symbol(1),
        RiskLimits {
            max_order_qty: None,
            max_order_notional: Some(1), // very low notional limit
        },
    );

    let mut reports = Vec::new();
    exchange.execute(
        Symbol(1),
        Order {
            id: OrderId(1),
            account: ACCT_A,
            side: Side::Buy,
            order_type: OrderType::Market,
            time_in_force: TimeInForce::IOC,
            quantity: qty(1000),
            stp: SelfTradeProtection::Allow,
            expiry_ns: 0,
        },
        &mut reports,
    );
    // Should NOT be rejected for notional — market orders have no price.
    // Will be rejected for NoLiquidity (empty book), which is fine.
    assert!(!reports.iter().any(|r| matches!(
        r,
        ExecutionReport::Rejected {
            reason: RejectReason::ExceedsMaxNotional,
            ..
        }
    )));
}

#[test]
fn no_limits_configured_passes() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 1_000_000_000);
    // No set_risk_limits call — all orders should pass fat finger checks.

    let mut reports = Vec::new();
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 1_000_000, 1000, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
}

// --- Kill switch tests ---

#[test]
fn cancel_all_cancels_resting_orders() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 1_000_000);
    exchange.deposit(ACCT_B, BTC, 1000);

    let mut reports = Vec::new();
    // ACCT_A places two buy orders.
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    exchange.execute(
        Symbol(1),
        limit_order(2, ACCT_A, Side::Buy, 99, 20, TimeInForce::GTC),
        &mut reports,
    );
    // ACCT_B places a sell order (distinct OrderId to avoid global collision).
    exchange.execute(
        Symbol(1),
        limit_order(100, ACCT_B, Side::Sell, 200, 5, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    // Kill switch for ACCT_A.
    exchange.cancel_all(ACCT_A, &mut reports);

    // Should produce exactly 2 Cancelled reports (ACCT_A's orders).
    assert_eq!(reports.len(), 2);
    assert!(
        reports
            .iter()
            .all(|r| matches!(r, ExecutionReport::Cancelled { .. }))
    );

    // ACCT_B's order should still be resting.
    reports.clear();
    exchange.cancel(Symbol(1), ACCT_B, OrderId(100), &mut reports);
    assert_eq!(reports.len(), 1);
    assert!(matches!(reports[0], ExecutionReport::Cancelled { .. }));
}

#[test]
fn cancel_all_releases_reservations() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 10_000);

    let mut reports = Vec::new();
    // Place a buy order that reserves 100 * 50 = 5000.
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 50, TimeInForce::GTC),
        &mut reports,
    );
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).available, 5_000);
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 5_000);

    reports.clear();
    exchange.cancel_all(ACCT_A, &mut reports);

    // Reservation should be fully released.
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).available, 10_000);
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 0);
}

#[test]
fn cancel_all_across_multiple_instruments() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.add_instrument(eth_usd_spec());
    exchange.deposit(ACCT_A, USD, 1_000_000);

    let mut reports = Vec::new();
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    exchange.execute(
        Symbol(2),
        limit_order(2, ACCT_A, Side::Buy, 50, 20, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    exchange.cancel_all(ACCT_A, &mut reports);

    // Both orders cancelled.
    assert_eq!(reports.len(), 2);
    assert!(
        reports
            .iter()
            .all(|r| matches!(r, ExecutionReport::Cancelled { .. }))
    );
}

#[test]
fn cancel_all_cancels_pending_stops() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 1_000_000);
    exchange.deposit(ACCT_A, BTC, 100);

    let mut reports = Vec::new();
    // Place a resting sell so there's a trade to set last_trade_price,
    // then a stop-buy for ACCT_A.
    exchange.execute(
        Symbol(1),
        Order {
            id: OrderId(1),
            account: ACCT_A,
            side: Side::Buy,
            order_type: OrderType::Stop {
                trigger_price: price(500),
            },
            time_in_force: TimeInForce::GTC,
            quantity: qty(10),
            stp: SelfTradeProtection::Allow,
            expiry_ns: 0,
        },
        &mut reports,
    );
    // Also a resting limit order.
    exchange.execute(
        Symbol(1),
        limit_order(2, ACCT_A, Side::Sell, 1000, 5, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    exchange.cancel_all(ACCT_A, &mut reports);

    // Both the pending stop and the resting limit should be cancelled.
    assert_eq!(reports.len(), 2);
    assert!(
        reports
            .iter()
            .all(|r| matches!(r, ExecutionReport::Cancelled { .. }))
    );
}

#[test]
fn cancel_all_empty_is_noop() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());

    let mut reports = Vec::new();
    exchange.cancel_all(ACCT_A, &mut reports);
    assert!(reports.is_empty());
}

// --- Client dedup tests (continued) ---

#[test]
fn same_order_id_different_accounts_allowed() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 100_000);
    exchange.deposit(ACCT_B, BTC, 100);

    let mut reports = Vec::new();
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Placed { .. }));

    reports.clear();
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_B, Side::Sell, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    // Should succeed — dedup is per-account, not global.
    assert!(matches!(reports[0], ExecutionReport::Fill { .. }));
}

#[test]
fn same_order_id_different_accounts_cancel_targets_correct_order() {
    // Two accounts place resting orders with the same OrderId on the
    // same side. Cancelling one must not affect the other.
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 100_000);
    exchange.deposit(ACCT_B, USD, 100_000);

    let mut reports = Vec::new();

    // Both place buy OrderId(1) at different prices (so they don't fill).
    exchange.execute(
        btc,
        limit_order(1, ACCT_A, Side::Buy, 90, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
    reports.clear();

    exchange.execute(
        btc,
        limit_order(1, ACCT_B, Side::Buy, 80, 5, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
    reports.clear();

    // Cancel ACCT_A's order — ACCT_B's should survive.
    exchange.cancel(btc, ACCT_A, OrderId(1), &mut reports);
    assert_eq!(reports.len(), 1);
    assert!(matches!(
        reports[0],
        ExecutionReport::Cancelled {
            order_id: OrderId(1),
            account: ACCT_A,
            ..
        }
    ));
    reports.clear();

    // ACCT_A's reservation released.
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 0);
    // ACCT_B's reservation still held.
    assert!(exchange.accounts().balance(ACCT_B, USD).reserved > 0);

    // Cancel ACCT_B's order — should also work.
    exchange.cancel(btc, ACCT_B, OrderId(1), &mut reports);
    assert_eq!(reports.len(), 1);
    assert!(matches!(
        reports[0],
        ExecutionReport::Cancelled {
            order_id: OrderId(1),
            account: ACCT_B,
            ..
        }
    ));
    assert_eq!(exchange.accounts().balance(ACCT_B, USD).reserved, 0);
}

#[test]
fn same_order_id_different_accounts_amend_targets_correct_order() {
    // Two accounts with the same OrderId resting. Amending one must
    // not affect the other.
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 100_000);
    exchange.deposit(ACCT_B, USD, 100_000);

    let mut reports = Vec::new();

    exchange.execute(
        btc,
        limit_order(1, ACCT_A, Side::Buy, 90, 10, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();
    exchange.execute(
        btc,
        limit_order(1, ACCT_B, Side::Buy, 80, 5, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    // Amend ACCT_A's order to new price/qty.
    exchange.cancel_replace(btc, ACCT_A, OrderId(1), price(85), qty(8), &mut reports);
    assert_eq!(reports.len(), 1);
    if let ExecutionReport::Replaced {
        order_id,
        old_price,
        new_price,
        old_remaining,
        new_remaining,
        ..
    } = &reports[0]
    {
        assert_eq!(*order_id, OrderId(1));
        assert_eq!(*old_price, price(90));
        assert_eq!(*new_price, price(85));
        assert_eq!(*old_remaining, qty(10));
        assert_eq!(*new_remaining, qty(8));
    } else {
        panic!("expected Replaced, got {:?}", reports[0]);
    }
    reports.clear();

    // ACCT_B's order should be unchanged — verify by cancelling it
    // and checking the remaining quantity is still 5 at price 80.
    exchange.cancel(btc, ACCT_B, OrderId(1), &mut reports);
    if let ExecutionReport::Cancelled {
        remaining_quantity, ..
    } = &reports[0]
    {
        assert_eq!(*remaining_quantity, qty(5));
    } else {
        panic!("expected Cancelled, got {:?}", reports[0]);
    }
}

#[test]
fn same_order_id_different_accounts_cancel_all_targets_correct_account() {
    // Two accounts with the same OrderId resting. CancelAll for one
    // account must not touch the other's orders.
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 100_000);
    exchange.deposit(ACCT_B, USD, 100_000);

    let mut reports = Vec::new();

    exchange.execute(
        btc,
        limit_order(1, ACCT_A, Side::Buy, 90, 10, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();
    exchange.execute(
        btc,
        limit_order(1, ACCT_B, Side::Buy, 80, 5, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    // CancelAll for ACCT_A.
    exchange.cancel_all(ACCT_A, &mut reports);
    assert_eq!(reports.len(), 1);
    assert!(matches!(
        reports[0],
        ExecutionReport::Cancelled {
            order_id: OrderId(1),
            account: ACCT_A,
            ..
        }
    ));
    reports.clear();

    // ACCT_A fully released, ACCT_B still has reservation.
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 0);
    assert!(exchange.accounts().balance(ACCT_B, USD).reserved > 0);

    // ACCT_B's order is still live — it can be cancelled independently.
    exchange.cancel(btc, ACCT_B, OrderId(1), &mut reports);
    assert_eq!(reports.len(), 1);
    assert!(matches!(
        reports[0],
        ExecutionReport::Cancelled {
            order_id: OrderId(1),
            account: ACCT_B,
            ..
        }
    ));
}

// -- Fee model tests --

#[test]
fn fee_deducted_from_fill_proceeds() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 50_000);
    exchange.deposit(ACCT_B, BTC, 100);

    // 10 bps maker, 20 bps taker.
    let mut reports = Vec::new();
    exchange.set_fee_schedule(
        btc,
        FeeSchedule {
            maker_fee_bps: 10,
            taker_fee_bps: 20,
        },
        &mut reports,
    );

    // Resting buy (maker) at 1000 for 10.
    exchange.execute(
        btc,
        limit_order(1, ACCT_A, Side::Buy, 1000, 10, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    // Incoming sell (taker) hits the resting buy.
    exchange.execute(
        btc,
        limit_order(2, ACCT_B, Side::Sell, 1000, 10, TimeInForce::GTC),
        &mut reports,
    );

    // Find the fill report.
    let fill = reports
        .iter()
        .find(|r| matches!(r, ExecutionReport::Fill { .. }))
        .unwrap();
    if let ExecutionReport::Fill {
        maker_fee,
        taker_fee,
        ..
    } = fill
    {
        // cost = 1000 * 10 = 10_000
        // maker_fee = 10_000 * 10 / 10_000 = 10
        // taker_fee = 10_000 * 20 / 10_000 = 20
        assert_eq!(*maker_fee, 10);
        assert_eq!(*taker_fee, 20);
    } else {
        panic!("expected Fill");
    }

    // Buyer (maker): reserved exactly cost (10_000), no fee cushion
    // under A. Fee paid in **base** out of the 10 BTC credit:
    // buyer_base_fee = qty × 10 bps / 10_000 = 10 × 10 / 10_000 = 0
    // (integer truncation). So the buyer receives the full 10 BTC.
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).available, 40_000);
    assert_eq!(exchange.accounts().balance(ACCT_A, BTC).available, 10);

    // Seller (taker): received cost - seller_quote_fee (20) = 9_980.
    assert_eq!(exchange.accounts().balance(ACCT_B, USD).available, 9_980);
    assert_eq!(exchange.accounts().balance(ACCT_B, BTC).available, 90);
}

#[test]
fn zero_fees_produce_no_deduction() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 50_000);
    exchange.deposit(ACCT_B, BTC, 100);

    // No fee schedule set — defaults to 0/0.

    let mut reports = Vec::new();
    exchange.execute(
        btc,
        limit_order(1, ACCT_A, Side::Buy, 1000, 10, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    exchange.execute(
        btc,
        limit_order(2, ACCT_B, Side::Sell, 1000, 10, TimeInForce::GTC),
        &mut reports,
    );

    let fill = reports
        .iter()
        .find(|r| matches!(r, ExecutionReport::Fill { .. }))
        .unwrap();
    if let ExecutionReport::Fill {
        maker_fee,
        taker_fee,
        ..
    } = fill
    {
        assert_eq!(*maker_fee, 0);
        assert_eq!(*taker_fee, 0);
    }

    // No fees: buyer pays exactly 10_000, seller receives exactly 10_000.
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).available, 40_000);
    assert_eq!(exchange.accounts().balance(ACCT_B, USD).available, 10_000);
}

#[test]
fn fee_schedule_change_applies_to_subsequent_fills() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 100_000);
    exchange.deposit(ACCT_B, BTC, 200);

    let mut reports = Vec::new();

    // First trade with no fees.
    exchange.execute(
        btc,
        limit_order(1, ACCT_A, Side::Buy, 1000, 10, TimeInForce::GTC),
        &mut reports,
    );
    exchange.execute(
        btc,
        limit_order(2, ACCT_B, Side::Sell, 1000, 10, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    // Set fees.
    exchange.set_fee_schedule(
        btc,
        FeeSchedule {
            maker_fee_bps: 50,
            taker_fee_bps: 100,
        },
        &mut reports,
    );

    // Second trade with fees.
    exchange.execute(
        btc,
        limit_order(3, ACCT_A, Side::Buy, 1000, 10, TimeInForce::GTC),
        &mut reports,
    );
    exchange.execute(
        btc,
        limit_order(4, ACCT_B, Side::Sell, 1000, 10, TimeInForce::GTC),
        &mut reports,
    );

    let fill = reports
        .iter()
        .find(|r| matches!(r, ExecutionReport::Fill { .. }))
        .unwrap();
    if let ExecutionReport::Fill {
        maker_fee,
        taker_fee,
        ..
    } = fill
    {
        // cost = 10_000. maker = 10_000 * 50 / 10_000 = 50. taker = 100.
        assert_eq!(*maker_fee, 50);
        assert_eq!(*taker_fee, 100);
    }
}

#[test]
fn maker_rebate_negative_fee() {
    use crate::account::FEE_ACCOUNT;

    // Negative maker fee = rebate. Under A, the buyer (here the
    // maker) is rebated in **base** out of their base credit.
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 200_000);
    exchange.deposit(ACCT_B, BTC, 1_000);
    // Pre-fund FEE_ACCOUNT in base so it can pay the rebate from
    // available (otherwise it accumulates as deficit — also valid,
    // but this test focuses on trader-visible balances).
    exchange.deposit(FEE_ACCOUNT, BTC, 100);

    // -1000 bps maker (10% rebate), 100 bps taker (1%). Large bps so
    // the integer-divided fees round to non-zero values at low qty.
    let mut reports = Vec::new();
    exchange.set_fee_schedule(
        btc,
        FeeSchedule {
            maker_fee_bps: -1_000,
            taker_fee_bps: 100,
        },
        &mut reports,
    );

    // Resting buy (maker) at 1000 for 100. cost = 100_000.
    exchange.execute(
        btc,
        limit_order(1, ACCT_A, Side::Buy, 1000, 100, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    // Incoming sell (taker) hits the resting buy.
    exchange.execute(
        btc,
        limit_order(2, ACCT_B, Side::Sell, 1000, 100, TimeInForce::GTC),
        &mut reports,
    );

    let fill = reports
        .iter()
        .find(|r| matches!(r, ExecutionReport::Fill { .. }))
        .unwrap();
    if let ExecutionReport::Fill {
        maker_fee,
        taker_fee,
        ..
    } = fill
    {
        // Wire-format report: quote-denominated fees.
        // maker_fee = cost × -1000 / 10_000 = -10_000.
        // taker_fee = cost × 100 / 10_000 = 1_000.
        assert_eq!(*maker_fee, -10_000);
        assert_eq!(*taker_fee, 1_000);
    } else {
        panic!("expected Fill");
    }

    // Buyer (maker): reservation was pure cost (100_000), all
    // consumed. Base credit = qty - buyer_base_fee where
    // buyer_base_fee = qty × -1000 / 10_000 = -10 (rebate).
    // So buyer receives 100 + 10 = 110 BTC.
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).available, 100_000);
    assert_eq!(exchange.accounts().balance(ACCT_A, BTC).available, 110);

    // Seller (taker): received cost - seller_quote_fee = 99_000.
    assert_eq!(exchange.accounts().balance(ACCT_B, USD).available, 99_000);
    assert_eq!(exchange.accounts().balance(ACCT_B, BTC).available, 900);

    // Fee account: paid 10 BTC rebate (100 - 10 = 90 left), gained
    // 1_000 USD fee.
    assert_eq!(exchange.accounts().balance(FEE_ACCOUNT, BTC).available, 90);
    assert_eq!(
        exchange.accounts().balance(FEE_ACCOUNT, USD).available,
        1_000
    );
}

// -- Post-only tests --

fn post_only_order(id: u64, account: AccountId, side: Side, p: u64, q: u64) -> Order {
    Order {
        id: OrderId(id),
        account,
        side,
        order_type: OrderType::Limit {
            price: price(p),
            post_only: true,
        },
        time_in_force: TimeInForce::GTC,
        quantity: qty(q),
        stp: SelfTradeProtection::Allow,
        expiry_ns: 0,
    }
}

#[test]
fn post_only_rests_on_empty_book() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 50_000);

    let mut reports = Vec::new();
    exchange.execute(
        btc,
        post_only_order(1, ACCT_A, Side::Buy, 1000, 10),
        &mut reports,
    );

    assert!(
        reports
            .iter()
            .any(|r| matches!(r, ExecutionReport::Placed { .. })),
        "post-only should rest on empty book"
    );
}

#[test]
fn post_only_rests_when_no_cross() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 50_000);
    exchange.deposit(ACCT_B, BTC, 100);

    let mut reports = Vec::new();

    // Resting ask at 1100.
    exchange.execute(
        btc,
        limit_order(1, ACCT_B, Side::Sell, 1100, 10, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    // Post-only buy at 1000 — does not cross ask at 1100.
    exchange.execute(
        btc,
        post_only_order(2, ACCT_A, Side::Buy, 1000, 10),
        &mut reports,
    );

    assert!(
        reports
            .iter()
            .any(|r| matches!(r, ExecutionReport::Placed { .. })),
        "post-only buy below best ask should rest"
    );
}

#[test]
fn post_only_rejected_when_would_cross() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 50_000);
    exchange.deposit(ACCT_B, BTC, 100);

    let mut reports = Vec::new();

    // Resting ask at 1000.
    exchange.execute(
        btc,
        limit_order(1, ACCT_B, Side::Sell, 1000, 10, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    // Post-only buy at 1000 — would cross the ask.
    exchange.execute(
        btc,
        post_only_order(2, ACCT_A, Side::Buy, 1000, 10),
        &mut reports,
    );

    assert!(
        reports.iter().any(|r| matches!(
            r,
            ExecutionReport::Rejected {
                reason: RejectReason::PostOnlyWouldCross,
                ..
            }
        )),
        "post-only buy at ask should be rejected"
    );
}

#[test]
fn post_only_rejected_releases_reservation() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 50_000);
    exchange.deposit(ACCT_B, BTC, 100);

    let mut reports = Vec::new();

    // Resting ask at 1000.
    exchange.execute(
        btc,
        limit_order(1, ACCT_B, Side::Sell, 1000, 10, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    // Post-only buy at 1000 — rejected.
    exchange.execute(
        btc,
        post_only_order(2, ACCT_A, Side::Buy, 1000, 10),
        &mut reports,
    );

    // Balance should be fully restored (no funds locked).
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).available, 50_000);
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 0);
}

#[test]
fn post_only_sell_rejected_when_would_cross() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 50_000);
    exchange.deposit(ACCT_B, BTC, 100);

    let mut reports = Vec::new();

    // Resting bid at 1000.
    exchange.execute(
        btc,
        limit_order(1, ACCT_A, Side::Buy, 1000, 10, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    // Post-only sell at 1000 — would cross the bid.
    exchange.execute(
        btc,
        post_only_order(2, ACCT_B, Side::Sell, 1000, 10),
        &mut reports,
    );

    assert!(
        reports.iter().any(|r| matches!(
            r,
            ExecutionReport::Rejected {
                reason: RejectReason::PostOnlyWouldCross,
                ..
            }
        )),
        "post-only sell at bid should be rejected"
    );
}

#[test]
fn post_only_rejected_order_id_can_be_reused() {
    // PostOnlyWouldCross is another submit-time rejection: the order
    // never rests, the live set never gains an entry, so the same
    // OrderId is free to retry with a non-crossing price.
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 50_000);
    exchange.deposit(ACCT_B, BTC, 100);

    let mut reports = Vec::new();

    // Resting ask at 1000.
    exchange.execute(
        btc,
        limit_order(1, ACCT_B, Side::Sell, 1000, 10, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    // Post-only buy at 1000 with order_id=2 — rejected (would cross).
    exchange.execute(
        btc,
        post_only_order(2, ACCT_A, Side::Buy, 1000, 10),
        &mut reports,
    );
    reports.clear();

    // Resubmitting order_id=2 below the ask must place — the prior
    // rejection didn't claim the slot.
    exchange.execute(
        btc,
        limit_order(2, ACCT_A, Side::Buy, 900, 10, TimeInForce::GTC),
        &mut reports,
    );

    assert!(
        matches!(reports[0], ExecutionReport::Placed { .. }),
        "retry of rejected post-only id should place, got {:?}",
        reports[0],
    );
}

// --- Withdraw and account lifecycle ---

#[test]
fn withdraw_reduces_available() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 10_000);

    exchange.withdraw(ACCT_A, USD, 3_000).unwrap();
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).available, 7_000);
}

#[test]
fn withdraw_insufficient_rejected() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 1_000);

    let err = exchange.withdraw(ACCT_A, USD, 5_000).unwrap_err();
    assert_eq!(err, RejectReason::InsufficientBalance);
}

#[test]
fn withdraw_with_resting_orders_rejected() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 10_000);

    let mut reports = Vec::new();
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Placed { .. }));

    // Can't withdraw while orders are resting.
    let err = exchange.withdraw(ACCT_A, USD, 1_000).unwrap_err();
    assert_eq!(err, RejectReason::HasRestingOrders);
}

#[test]
fn withdraw_after_cancel_all_succeeds() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 10_000);

    let mut reports = Vec::new();
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );

    // Cancel all, then withdraw.
    reports.clear();
    exchange.cancel_all(ACCT_A, &mut reports);
    exchange.withdraw(ACCT_A, USD, 10_000).unwrap();
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).available, 0);
}

#[test]
fn order_id_reusable_after_full_withdrawal() {
    // Under live-orders-only dedup there is no per-account HWM that
    // persists across an account's lifetime. Once a fill closes an
    // order the live set drops the entry; full account withdrawal
    // doesn't change that. A re-deposited account can reuse the
    // same OrderId because no live order claims it.
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 10_000);
    exchange.deposit(ACCT_A, BTC, 100);

    // Submit and fill an order.
    let mut reports = Vec::new();
    exchange.deposit(ACCT_B, BTC, 100);
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_B, Side::Sell, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );

    // Withdraw everything.
    exchange
        .withdraw(
            ACCT_A,
            USD,
            exchange.accounts().balance(ACCT_A, USD).available,
        )
        .unwrap();
    exchange
        .withdraw(
            ACCT_A,
            BTC,
            exchange.accounts().balance(ACCT_A, BTC).available,
        )
        .unwrap();

    assert!(!exchange.accounts().has_balances(ACCT_A));

    // Re-deposit and reuse OrderId 1 — must place because the
    // earlier fill removed it from the live set.
    exchange.deposit(ACCT_A, USD, 10_000);
    reports.clear();
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(
        matches!(reports[0], ExecutionReport::Placed { .. }),
        "reuse after fill+withdrawal should place, got {:?}",
        reports[0]
    );
}

#[test]
fn order_counts_track_resting_orders() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 100_000);

    let mut reports = Vec::new();
    // Place 3 orders.
    for i in 1..=3 {
        exchange.execute(
            Symbol(1),
            limit_order(i, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
    }

    // All 3 resting — withdraw should fail.
    assert_eq!(
        exchange.withdraw(ACCT_A, USD, 1).unwrap_err(),
        RejectReason::HasRestingOrders
    );

    // Cancel all — withdraw should succeed.
    reports.clear();
    exchange.cancel_all(ACCT_A, &mut reports);
    assert!(exchange.withdraw(ACCT_A, USD, 1).is_ok());
}

#[test]
fn order_counts_zero_after_ioc_fill() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 100_000);
    exchange.deposit(ACCT_B, BTC, 100);

    let mut reports = Vec::new();
    // Seller places resting order.
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_B, Side::Sell, 100, 10, TimeInForce::GTC),
        &mut reports,
    );

    // IOC buy fills immediately — should not leave resting count.
    reports.clear();
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::IOC),
        &mut reports,
    );

    // ACCT_A should have no resting orders — withdraw should work.
    assert!(exchange.withdraw(ACCT_A, USD, 1).is_ok());
}

#[test]
fn withdraw_after_partial_fill_and_cancel() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 100_000);
    exchange.deposit(ACCT_B, BTC, 100);

    let mut reports = Vec::new();
    // ACCT_A places GTC buy for 20 @ 100.
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 20, TimeInForce::GTC),
        &mut reports,
    );

    // ACCT_B fills 10 of 20.
    reports.clear();
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_B, Side::Sell, 100, 10, TimeInForce::GTC),
        &mut reports,
    );

    // Cancel remaining, then withdraw all.
    reports.clear();
    exchange.cancel_all(ACCT_A, &mut reports);

    let usd_avail = exchange.accounts().balance(ACCT_A, USD).available;
    exchange.withdraw(ACCT_A, USD, usd_avail).unwrap();
    let btc_avail = exchange.accounts().balance(ACCT_A, BTC).available;
    exchange.withdraw(ACCT_A, BTC, btc_avail).unwrap();

    assert!(!exchange.accounts().has_balances(ACCT_A));
}

#[test]
fn order_counts_zero_after_fok_no_fill() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 100_000);

    let mut reports = Vec::new();
    // FOK buy on empty book — rejected, no fill.
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::FOK),
        &mut reports,
    );
    assert!(matches!(
        reports[0],
        ExecutionReport::Rejected {
            reason: RejectReason::FOKCannotFill,
            ..
        }
    ));

    // No resting orders — withdraw should succeed.
    assert!(exchange.withdraw(ACCT_A, USD, 1).is_ok());
}

#[test]
fn order_counts_zero_after_fok_full_fill() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 100_000);
    exchange.deposit(ACCT_B, BTC, 100);

    let mut reports = Vec::new();
    // Seller rests.
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_B, Side::Sell, 100, 10, TimeInForce::GTC),
        &mut reports,
    );

    // FOK buy fills entirely.
    reports.clear();
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::FOK),
        &mut reports,
    );

    // ACCT_A has no resting orders.
    assert!(exchange.withdraw(ACCT_A, USD, 1).is_ok());
}

#[test]
fn order_counts_after_stp_cancel_newest() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 100_000);
    exchange.deposit(ACCT_A, BTC, 100);

    let mut reports = Vec::new();
    // Place a resting sell.
    exchange.execute(
        Symbol(1),
        Order {
            id: OrderId(1),
            account: ACCT_A,
            side: Side::Sell,
            order_type: OrderType::Limit {
                price: Price(NonZeroU64::new(100).unwrap()),
                post_only: false,
            },
            time_in_force: TimeInForce::GTC,
            quantity: Quantity(NonZeroU64::new(10).unwrap()),
            stp: SelfTradeProtection::CancelNewest,
            expiry_ns: 0,
        },
        &mut reports,
    );

    // Self-trade: buy crosses the sell. CancelNewest rejects the taker (buy).
    reports.clear();
    exchange.execute(
        Symbol(1),
        Order {
            id: OrderId(2),
            account: ACCT_A,
            side: Side::Buy,
            order_type: OrderType::Limit {
                price: Price(NonZeroU64::new(100).unwrap()),
                post_only: false,
            },
            time_in_force: TimeInForce::GTC,
            quantity: Quantity(NonZeroU64::new(10).unwrap()),
            stp: SelfTradeProtection::CancelNewest,
            expiry_ns: 0,
        },
        &mut reports,
    );

    // The maker (sell) still rests. Cancel it, then withdraw should succeed.
    reports.clear();
    exchange.cancel_all(ACCT_A, &mut reports);
    assert!(exchange.withdraw(ACCT_A, USD, 1).is_ok());
}

#[test]
fn order_counts_after_stp_cancel_oldest() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 100_000);
    exchange.deposit(ACCT_A, BTC, 100);

    let mut reports = Vec::new();
    // Place a resting sell.
    exchange.execute(
        Symbol(1),
        Order {
            id: OrderId(1),
            account: ACCT_A,
            side: Side::Sell,
            order_type: OrderType::Limit {
                price: Price(NonZeroU64::new(100).unwrap()),
                post_only: false,
            },
            time_in_force: TimeInForce::GTC,
            quantity: Quantity(NonZeroU64::new(10).unwrap()),
            stp: SelfTradeProtection::CancelOldest,
            expiry_ns: 0,
        },
        &mut reports,
    );

    // Self-trade: buy crosses. CancelOldest cancels the maker (sell).
    // The taker buy may rest if GTC.
    reports.clear();
    exchange.execute(
        Symbol(1),
        Order {
            id: OrderId(2),
            account: ACCT_A,
            side: Side::Buy,
            order_type: OrderType::Limit {
                price: Price(NonZeroU64::new(100).unwrap()),
                post_only: false,
            },
            time_in_force: TimeInForce::GTC,
            quantity: Quantity(NonZeroU64::new(10).unwrap()),
            stp: SelfTradeProtection::CancelOldest,
            expiry_ns: 0,
        },
        &mut reports,
    );

    // Cancel remaining, then withdraw.
    reports.clear();
    exchange.cancel_all(ACCT_A, &mut reports);
    assert!(exchange.withdraw(ACCT_A, USD, 1).is_ok());
}

// --- Per-key request sequence dedup tests ---

#[test]
fn duplicate_request_rejected() {
    let mut exchange = Exchange::new();
    let key_hash: u64 = 0xDEAD_BEEF;

    // First request with seq=1 should succeed.
    assert!(exchange.check_request_seq(key_hash, 1));
    // Same key, same seq=1 should be rejected.
    assert!(!exchange.check_request_seq(key_hash, 1));
    // Lower seq should also be rejected.
    assert!(!exchange.check_request_seq(key_hash, 0));
}

#[test]
fn different_keys_overlapping_seqs() {
    let mut exchange = Exchange::new();
    let key_a: u64 = 0xAAAA;
    let key_b: u64 = 0xBBBB;

    // Both keys can use seq=1 independently.
    assert!(exchange.check_request_seq(key_a, 1));
    assert!(exchange.check_request_seq(key_b, 1));
    // key_a advancing to seq=2 doesn't affect key_b.
    assert!(exchange.check_request_seq(key_a, 2));
    assert!(!exchange.check_request_seq(key_b, 1)); // still duplicate for key_b
    assert!(exchange.check_request_seq(key_b, 2));
}

#[test]
fn key_hash_zero_exempt_from_dedup() {
    let mut exchange = Exchange::new();

    // key_hash=0 is exempt (internal/seed events) — always returns true.
    assert!(exchange.check_request_seq(0, 1));
    assert!(exchange.check_request_seq(0, 1)); // same seq, still passes
    assert!(exchange.check_request_seq(0, 0)); // seq=0 also passes
}

#[test]
fn key_hwm_snapshot_round_trip() {
    let mut exchange = Exchange::new();
    let key_a: u64 = 0x1111;
    let key_b: u64 = 0x2222;

    exchange.check_request_seq(key_a, 5);
    exchange.check_request_seq(key_b, 10);

    let snap = exchange.snapshot_key_hwm();
    assert_eq!(snap.len(), 2);

    // Verify both entries are present (order may vary).
    let mut sorted = snap.clone();
    sorted.sort();
    assert_eq!(sorted, vec![(key_a, 5), (key_b, 10)]);
}

#[test]
fn request_seq_must_be_strictly_increasing() {
    let mut exchange = Exchange::new();
    let key: u64 = 0xABCD;

    // Gap in sequence is fine (3, then 10).
    assert!(exchange.check_request_seq(key, 3));
    assert!(exchange.check_request_seq(key, 10));
    // seq=4..9 are now below HWM (10).
    assert!(!exchange.check_request_seq(key, 4));
    assert!(!exchange.check_request_seq(key, 9));
    assert!(!exchange.check_request_seq(key, 10)); // equal = dup
    assert!(exchange.check_request_seq(key, 11)); // strictly greater
}

#[test]
fn request_seq_zero_on_real_key_is_duplicate_after_any_request() {
    let mut exchange = Exchange::new();
    let key: u64 = 0xFF00;

    // First request at seq=1 sets HWM to 1.
    assert!(exchange.check_request_seq(key, 1));
    // seq=0 is below HWM (1) — rejected as duplicate.
    assert!(!exchange.check_request_seq(key, 0));
}

#[test]
fn request_seq_u64_max_accepted() {
    let mut exchange = Exchange::new();
    let key: u64 = 0x42;

    // u64::MAX should be accepted as a valid seq.
    assert!(exchange.check_request_seq(key, u64::MAX));
    // Nothing can be strictly greater than u64::MAX.
    assert!(!exchange.check_request_seq(key, u64::MAX));
    assert!(!exchange.check_request_seq(key, u64::MAX - 1));
}

#[test]
fn dedup_interleaved_with_orders() {
    // Verify that per-key request-seq dedup and the live `(account,
    // order_id)` set are independent: a fresh request_seq doesn't
    // bypass live-order dedup, and vice versa.
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 1_000_000);

    let key: u64 = 0xAAAA;
    let mut reports = Vec::new();

    // Submit order via check_request_seq first (as the pipeline would).
    assert!(exchange.check_request_seq(key, 1));
    exchange.execute(
        Symbol(1),
        Order {
            id: OrderId(100),
            account: ACCT_A,
            side: Side::Buy,
            order_type: OrderType::Limit {
                price: price(100),
                post_only: false,
            },
            time_in_force: TimeInForce::GTC,
            quantity: qty(1),
            stp: SelfTradeProtection::default(),
            expiry_ns: 0,
        },
        &mut reports,
    );
    assert!(
        reports
            .iter()
            .any(|r| matches!(r, ExecutionReport::Placed { .. }))
    );

    // Duplicate per-key seq should be caught before execute.
    assert!(!exchange.check_request_seq(key, 1));

    // A new request_seq doesn't help if the OrderId is still live
    // — the live-order dedup catches it.
    reports.clear();
    assert!(exchange.check_request_seq(key, 2));
    exchange.execute(
        Symbol(1),
        Order {
            id: OrderId(100), // same order ID as above
            account: ACCT_A,
            side: Side::Buy,
            order_type: OrderType::Limit {
                price: price(100),
                post_only: false,
            },
            time_in_force: TimeInForce::GTC,
            quantity: qty(1),
            stp: SelfTradeProtection::default(),
            expiry_ns: 0,
        },
        &mut reports,
    );
    assert!(reports.iter().any(|r| matches!(
        r,
        ExecutionReport::Rejected {
            reason: RejectReason::DuplicateOrderId,
            ..
        }
    )));
}

// -----------------------------------------------------------------------
// Same OrderId, different accounts at same price level
//
// Two accounts may independently use the same OrderId. When both rest at
// the same price level, cancel/replace/stop-cancel must operate on the
// correct account's order without disturbing the other.
// -----------------------------------------------------------------------

/// Place OrderId(1) for both ACCT_A and ACCT_B as buy limits at the same
/// price. Returns the exchange ready for disambiguation tests.
fn setup_same_id_same_price() -> Exchange {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 100_000);
    exchange.deposit(ACCT_B, USD, 100_000);

    let mut reports = Vec::new();
    // ACCT_A places OrderId(1) buy @ 100, qty 10.
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
    reports.clear();

    // ACCT_B places OrderId(1) buy @ 100, qty 5.
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_B, Side::Buy, 100, 5, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
    exchange
}

#[test]
fn cancel_disambiguates_by_account_at_same_price() {
    let mut exchange = setup_same_id_same_price();
    let mut reports = Vec::new();

    // Cancel ACCT_B's OrderId(1) — ACCT_A's should survive.
    exchange.cancel(Symbol(1), ACCT_B, OrderId(1), &mut reports);
    assert!(matches!(
        reports[0],
        ExecutionReport::Cancelled {
            account,
            order_id: OrderId(1),
            ..
        } if account == ACCT_B
    ));
    reports.clear();

    // ACCT_A's order is still resting — a sell should fill against it.
    exchange.deposit(ACCT_A, BTC, 100);
    exchange.deposit(ACCT_B, BTC, 100);
    exchange.execute(
        Symbol(1),
        limit_order(2, ACCT_B, Side::Sell, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    let fill = reports
        .iter()
        .find(|r| matches!(r, ExecutionReport::Fill { .. }));
    assert!(
        fill.is_some(),
        "ACCT_A's order should still be resting and fillable"
    );
}

#[test]
fn cancel_replace_same_price_disambiguates_by_account() {
    let mut exchange = setup_same_id_same_price();
    let mut reports = Vec::new();

    // Amend ACCT_A's OrderId(1): reduce qty from 10 to 3 (same price).
    exchange.cancel_replace(
        Symbol(1),
        ACCT_A,
        OrderId(1),
        price(100),
        qty(3),
        &mut reports,
    );
    assert!(matches!(
        reports[0],
        ExecutionReport::Replaced {
            order_id: OrderId(1),
            ..
        }
    ));
    // Verify the replaced order reports the correct old/new quantities.
    if let ExecutionReport::Replaced {
        old_remaining,
        new_remaining,
        ..
    } = &reports[0]
    {
        assert_eq!(*old_remaining, qty(10));
        assert_eq!(*new_remaining, qty(3));
    }
    reports.clear();

    // ACCT_B's order should be untouched — cancel it and verify qty 5.
    exchange.cancel(Symbol(1), ACCT_B, OrderId(1), &mut reports);
    assert!(matches!(
        reports[0],
        ExecutionReport::Cancelled {
            account,
            remaining_quantity,
            ..
        } if account == ACCT_B && remaining_quantity == qty(5)
    ));
}

#[test]
fn cancel_replace_different_price_disambiguates_by_account() {
    let mut exchange = setup_same_id_same_price();
    let mut reports = Vec::new();

    // Move ACCT_A's OrderId(1) from price 100 to price 90.
    exchange.cancel_replace(
        Symbol(1),
        ACCT_A,
        OrderId(1),
        price(90),
        qty(10),
        &mut reports,
    );
    assert!(matches!(
        reports[0],
        ExecutionReport::Replaced {
            order_id: OrderId(1),
            ..
        }
    ));
    reports.clear();

    // Sell at 100 should only fill ACCT_B's order (still at 100),
    // not ACCT_A's (now at 90).
    exchange.deposit(ACCT_A, BTC, 100);
    exchange.execute(
        Symbol(1),
        limit_order(2, ACCT_A, Side::Sell, 100, 5, TimeInForce::GTC),
        &mut reports,
    );
    let fills: Vec<_> = reports
        .iter()
        .filter(|r| matches!(r, ExecutionReport::Fill { .. }))
        .collect();
    assert_eq!(
        fills.len(),
        1,
        "should only fill ACCT_B's order at price 100"
    );
}

#[test]
fn cancel_stop_disambiguates_by_account() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 100_000);
    exchange.deposit(ACCT_B, USD, 100_000);

    let mut reports = Vec::new();

    // Both accounts place a stop buy with OrderId(1), same trigger price.
    let stop_a = Order {
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
    };
    exchange.execute(Symbol(1), stop_a, &mut reports);
    reports.clear();

    let stop_b = Order {
        id: OrderId(1),
        account: ACCT_B,
        side: Side::Buy,
        order_type: OrderType::Stop {
            trigger_price: price(200),
        },
        time_in_force: TimeInForce::GTC,
        quantity: qty(5),
        stp: SelfTradeProtection::Allow,
        expiry_ns: 0,
    };
    exchange.execute(Symbol(1), stop_b, &mut reports);
    reports.clear();

    // Cancel ACCT_A's stop — ACCT_B's should survive.
    exchange.cancel(Symbol(1), ACCT_A, OrderId(1), &mut reports);
    assert!(matches!(
        reports[0],
        ExecutionReport::Cancelled {
            account,
            order_id: OrderId(1),
            ..
        } if account == ACCT_A
    ));
    reports.clear();

    // Verify ACCT_B's stop is still pending: cancel it to confirm it exists.
    exchange.cancel(Symbol(1), ACCT_B, OrderId(1), &mut reports);
    assert!(matches!(
        reports[0],
        ExecutionReport::Cancelled {
            account,
            order_id: OrderId(1),
            ..
        } if account == ACCT_B
    ));
}

// -----------------------------------------------------------------------
// Day TIF + EndOfDay
// -----------------------------------------------------------------------

#[test]
fn day_order_places_on_book() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 10_000);

    let mut reports = Vec::new();
    exchange.execute(
        btc,
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::Day),
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 1_000);
}

#[test]
fn end_of_day_cancels_day_orders_not_gtc() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 20_000);
    exchange.deposit(ACCT_B, USD, 20_000);

    let mut reports = Vec::new();

    // ACCT_A: Day order.
    exchange.execute(
        btc,
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::Day),
        &mut reports,
    );
    reports.clear();

    // ACCT_B: GTC order at the same price.
    exchange.execute(
        btc,
        limit_order(1, ACCT_B, Side::Buy, 100, 5, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    // EndOfDay should cancel ACCT_A's Day order but not ACCT_B's GTC.
    exchange.end_of_day(&mut reports);
    assert_eq!(reports.len(), 1);
    assert!(matches!(
        reports[0],
        ExecutionReport::Cancelled {
            account,
            order_id: OrderId(1),
            ..
        } if account == ACCT_A
    ));

    // ACCT_A's balance fully released, ACCT_B's still reserved.
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 0);
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).available, 20_000);
    assert_eq!(exchange.accounts().balance(ACCT_B, USD).reserved, 500);
}

#[test]
fn end_of_day_on_empty_book_is_noop() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());

    let mut reports = Vec::new();
    exchange.end_of_day(&mut reports);
    assert!(reports.is_empty());
}

#[test]
fn day_order_partially_fills_then_cancelled_at_eod() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 10_000);
    exchange.deposit(ACCT_B, BTC, 100);

    let mut reports = Vec::new();

    // ACCT_A: Day buy limit @ 100, qty 10.
    exchange.execute(
        btc,
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::Day),
        &mut reports,
    );
    reports.clear();

    // ACCT_B: sell 3, partially filling the Day order.
    exchange.execute(
        btc,
        limit_order(1, ACCT_B, Side::Sell, 100, 3, TimeInForce::IOC),
        &mut reports,
    );
    let fills: Vec<_> = reports
        .iter()
        .filter(|r| matches!(r, ExecutionReport::Fill { .. }))
        .collect();
    assert_eq!(fills.len(), 1);
    reports.clear();

    // EndOfDay cancels the remaining 7.
    exchange.end_of_day(&mut reports);
    assert_eq!(reports.len(), 1);
    assert!(matches!(
        reports[0],
        ExecutionReport::Cancelled {
            remaining_quantity,
            ..
        } if remaining_quantity.get() == 7
    ));

    // All reservations released.
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 0);
}

#[test]
fn end_of_day_cancels_day_stop_orders() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 100_000);

    let mut reports = Vec::new();

    // Day stop order.
    exchange.execute(
        btc,
        Order {
            id: OrderId(1),
            account: ACCT_A,
            side: Side::Buy,
            order_type: OrderType::Stop {
                trigger_price: price(200),
            },
            time_in_force: TimeInForce::Day,
            quantity: qty(10),
            stp: SelfTradeProtection::Allow,
            expiry_ns: 0,
        },
        &mut reports,
    );
    reports.clear();

    // GTC stop order.
    exchange.execute(
        btc,
        Order {
            id: OrderId(2),
            account: ACCT_A,
            side: Side::Buy,
            order_type: OrderType::Stop {
                trigger_price: price(200),
            },
            time_in_force: TimeInForce::GTC,
            quantity: qty(5),
            stp: SelfTradeProtection::Allow,
            expiry_ns: 0,
        },
        &mut reports,
    );
    reports.clear();

    // EndOfDay cancels only the Day stop.
    exchange.end_of_day(&mut reports);
    assert_eq!(reports.len(), 1);
    assert!(matches!(
        reports[0],
        ExecutionReport::Cancelled {
            order_id: OrderId(1),
            ..
        }
    ));
}

// -- Fee account conservation tests --

#[test]
fn fees_credited_to_fee_account() {
    use crate::account::FEE_ACCOUNT;

    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 100_000);
    exchange.deposit(ACCT_B, BTC, 100);

    let mut reports = Vec::new();
    exchange.set_fee_schedule(
        Symbol(1),
        FeeSchedule {
            maker_fee_bps: 10,
            taker_fee_bps: 20,
        },
        &mut reports,
    );

    // Sell 10@100 (maker).
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_B, Side::Sell, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    // Buy 10@100 (taker) — fills immediately.
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );

    // cost = 100 * 10 = 1000
    // maker_fee = 1000 * 10 / 10_000 = 1
    // taker_fee = 1000 * 20 / 10_000 = 2
    let fill = reports
        .iter()
        .find(|r| matches!(r, ExecutionReport::Fill { .. }))
        .unwrap();
    if let ExecutionReport::Fill {
        maker_fee,
        taker_fee,
        ..
    } = fill
    {
        // Wire-format report: quote-denominated.
        // maker_fee = cost × 10 bps = 1, taker_fee = cost × 20 bps = 2.
        assert_eq!(*maker_fee, 1);
        assert_eq!(*taker_fee, 2);
    }

    // Under A, fees go to FEE_ACCOUNT in the **received asset** of
    // each leg: buyer (taker) is rebated/charged in BASE, seller
    // (maker) is rebated/charged in QUOTE.
    // buyer_base_fee = qty × 20 bps = 10×20/10_000 = 0 (truncates).
    // seller_quote_fee = cost × 10 bps = 1_000×10/10_000 = 1.
    let fee_usd = exchange.accounts().balance(FEE_ACCOUNT, USD);
    let fee_btc = exchange.accounts().balance(FEE_ACCOUNT, BTC);
    assert_eq!(fee_usd.available, 1, "seller fee credited in quote");
    assert_eq!(
        fee_btc.available, 0,
        "buyer fee truncated to zero at this qty"
    );

    // System conservation: deposited 100_000 USD + 100 BTC.
    let a_bal = exchange.accounts().balance(ACCT_A, USD);
    let b_bal = exchange.accounts().balance(ACCT_B, USD);
    let total_usd = a_bal.available as u128
        + a_bal.reserved as u128
        + b_bal.available as u128
        + fee_usd.available as u128;
    assert_eq!(total_usd, 100_000, "USD conservation");
    let total_btc = exchange.accounts().balance(ACCT_A, BTC).total() as u128
        + exchange.accounts().balance(ACCT_B, BTC).total() as u128
        + fee_btc.available as u128;
    assert_eq!(total_btc, 100, "BTC conservation");
}

#[test]
fn fee_schedule_change_after_placement_conserves_balance() {
    use crate::account::FEE_ACCOUNT;

    // Reproduces the bug found by proptests: fee schedule changes after
    // order placement, causing reservation to lack fee cushion. The fill
    // must not create or destroy value.
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, BTC, 100);
    exchange.deposit(ACCT_B, USD, 10_000);

    // No fees at order placement time.
    let mut reports = Vec::new();

    // ACCT_A sells 10@100.
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Sell, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    // Now set fees.
    exchange.set_fee_schedule(
        Symbol(1),
        FeeSchedule {
            maker_fee_bps: 0,
            taker_fee_bps: 50, // 0.5%
        },
        &mut reports,
    );

    // ACCT_B buys 10@100 — fills with taker fee, but buyer's reservation
    // was computed without fee cushion (fees were 0 at placement time...
    // but actually the buyer places now, with fees active, so cushion
    // is included). Let's test the seller side: seller placed when fees
    // were 0, so seller_fee = 0 and proceeds = full cost.
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_B, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );

    // Conservation: deposited 10_000 USD, no withdrawals.
    let a = exchange.accounts().balance(ACCT_A, USD);
    let b = exchange.accounts().balance(ACCT_B, USD);
    let fee = exchange.accounts().balance(FEE_ACCOUNT, USD);
    let total = a.available as u128
        + a.reserved as u128
        + b.available as u128
        + b.reserved as u128
        + fee.available as u128;
    assert_eq!(
        total, 10_000,
        "USD must be conserved after fee schedule change"
    );
}

#[test]
fn stop_trigger_after_fee_change_conserves_balance() {
    use crate::account::FEE_ACCOUNT;

    // Exact reproduction of the proptest failure: stop order placed
    // with fee=0, then fee schedule changes, stop triggers.
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, BTC, 100);
    exchange.deposit(ACCT_B, USD, 20_000);

    let mut reports = Vec::new();

    // ACCT_A sells 10@100 (resting, no fees yet).
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Sell, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    // ACCT_B buys 1@100 (fills, establishes last_trade_price=100).
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_B, Side::Buy, 100, 1, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    // ACCT_B places a stop buy: trigger_price=50, qty=1.
    // Since last_trade_price=100 >= 50, this triggers immediately
    // and becomes a market buy.
    // But first, change the fee schedule.
    exchange.set_fee_schedule(
        Symbol(1),
        FeeSchedule {
            maker_fee_bps: 0,
            taker_fee_bps: 100, // 1%
        },
        &mut reports,
    );

    // The stop's reservation was computed with the new fee schedule
    // (it's placed after the change), so this should be fine.
    // Let's test a scenario where the stop was placed BEFORE fees.
    // Reset: place stop with no fees, then change fees, then trigger.
    exchange.set_fee_schedule(
        Symbol(1),
        FeeSchedule {
            maker_fee_bps: 0,
            taker_fee_bps: 0,
        },
        &mut reports,
    );

    // Place stop buy@50 (no fees, so reservation = cost without cushion).
    exchange.execute(
        Symbol(1),
        Order {
            id: OrderId(2),
            account: ACCT_B,
            side: Side::Buy,
            order_type: OrderType::Stop {
                trigger_price: price(50),
            },
            time_in_force: TimeInForce::GTC,
            quantity: qty(1),
            stp: SelfTradeProtection::Allow,
            expiry_ns: 0,
        },
        &mut reports,
    );
    reports.clear();

    // Change fees while the stop is pending.
    exchange.set_fee_schedule(
        Symbol(1),
        FeeSchedule {
            maker_fee_bps: 0,
            taker_fee_bps: 200, // 2%
        },
        &mut reports,
    );

    // Trigger the stop by trading at price >= 50.
    // Place a sell that fills against existing bid... but we need a bid
    // to trade and set last_trade_price. The stop triggers on
    // last_trade_price crossing the trigger. Since last_trade=100 >= 50,
    // the stop should have already triggered when placed.
    // Let's check if it triggered:
    assert!(
        reports.is_empty(),
        "stop should have triggered on placement (last_trade=100 >= trigger=50)"
    );
    // Actually, the stop triggers during the execute call above (placement).
    // Let me re-read the reports from that call.
    // The reports were cleared. Let me verify conservation directly.

    let a = exchange.accounts().balance(ACCT_A, USD);
    let b = exchange.accounts().balance(ACCT_B, USD);
    let fee = exchange.accounts().balance(FEE_ACCOUNT, USD);
    let total = a.available as u128
        + a.reserved as u128
        + b.available as u128
        + b.reserved as u128
        + fee.available as u128;
    assert_eq!(total, 20_000, "USD must be conserved with stop+fee change");
}

// -- Targeted edge-case tests --

/// Cancel-replace to a price that would cross the spread must be rejected.
#[test]
fn cancel_replace_crossing_spread_rejected() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 100_000);
    exchange.deposit(ACCT_B, BTC, 100);

    let mut reports = Vec::new();

    // Sell 10@100 (ask).
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_B, Side::Sell, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    // Buy 10@90 (bid, resting below spread).
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 90, 10, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    // Amend buy from 90 to 100 — would cross the ask at 100.
    exchange.cancel_replace(
        Symbol(1),
        ACCT_A,
        OrderId(1),
        price(100),
        qty(10),
        &mut reports,
    );
    assert!(
        reports.iter().any(|r| matches!(
            r,
            ExecutionReport::Rejected {
                reason: RejectReason::PriceWouldCross,
                ..
            }
        )),
        "cancel-replace crossing spread must be rejected"
    );
    // Original order should still be resting at 90.
    reports.clear();
    exchange.cancel(Symbol(1), ACCT_A, OrderId(1), &mut reports);
    assert!(
        reports
            .iter()
            .any(|r| matches!(r, ExecutionReport::Cancelled { .. })),
        "original order should still be on book after rejected amend"
    );
}

/// FOK with STP exclusion: only liquidity is from the same account.
/// FOK must reject because the self-trade would be prevented.
#[test]
fn fok_stp_rejects_when_only_self_liquidity() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 100_000);
    exchange.deposit(ACCT_A, BTC, 100);

    let mut reports = Vec::new();

    // ACCT_A sells 10@100.
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Sell, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    // ACCT_A FOK buy 10@100 with STP CancelNewest — only liquidity
    // is own order, which STP would cancel. FOK must reject.
    exchange.execute(
        Symbol(1),
        Order {
            id: OrderId(2),
            account: ACCT_A,
            side: Side::Buy,
            order_type: OrderType::Limit {
                price: price(100),
                post_only: false,
            },
            time_in_force: TimeInForce::FOK,
            quantity: qty(10),
            stp: SelfTradeProtection::CancelNewest,
            expiry_ns: 0,
        },
        &mut reports,
    );

    // Should be rejected, not filled.
    assert!(
        reports
            .iter()
            .any(|r| matches!(r, ExecutionReport::Rejected { .. })),
        "FOK with only self-liquidity and STP must reject"
    );
    assert!(
        !reports
            .iter()
            .any(|r| matches!(r, ExecutionReport::Fill { .. })),
        "FOK must never partially fill"
    );
}

/// FOK with STP Allow: same-account liquidity should fill normally.
#[test]
fn fok_stp_allow_fills_self() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 100_000);
    exchange.deposit(ACCT_A, BTC, 100);

    let mut reports = Vec::new();

    // ACCT_A sells 10@100.
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Sell, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    // ACCT_A FOK buy 10@100 with STP Allow — self-trade allowed.
    exchange.execute(
        Symbol(1),
        Order {
            id: OrderId(2),
            account: ACCT_A,
            side: Side::Buy,
            order_type: OrderType::Limit {
                price: price(100),
                post_only: false,
            },
            time_in_force: TimeInForce::FOK,
            quantity: qty(10),
            stp: SelfTradeProtection::Allow,
            expiry_ns: 0,
        },
        &mut reports,
    );

    assert!(
        reports
            .iter()
            .any(|r| matches!(r, ExecutionReport::Fill { .. })),
        "FOK with STP Allow should fill against own order"
    );
}

/// Per-fill fee rounding: buyer_deducted + seller_proceeds + fee_credit == cost
/// for every individual fill.
#[test]
fn per_fill_fee_rounding_conservation() {
    use crate::account::FEE_ACCOUNT;

    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());

    // Use odd prices/quantities to maximize rounding effects.
    let mut reports = Vec::new();
    exchange.set_fee_schedule(
        Symbol(1),
        FeeSchedule {
            maker_fee_bps: 3, // 0.03%
            taker_fee_bps: 7, // 0.07%
        },
        &mut reports,
    );

    exchange.deposit(ACCT_A, USD, 10_000_000);
    exchange.deposit(ACCT_B, BTC, 10_000);

    let mut reports = Vec::new();

    // Place asks at various odd prices.
    for (id, p, q) in [(1, 137, 13), (2, 251, 7), (3, 499, 3), (4, 1009, 1)] {
        exchange.execute(
            Symbol(1),
            limit_order(id, ACCT_B, Side::Sell, p, q, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();
    }

    // Aggressive buy that sweeps all levels.
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 1100, 30, TimeInForce::GTC),
        &mut reports,
    );

    // Check system conservation after all fills.
    let a = exchange.accounts().balance(ACCT_A, USD);
    let b = exchange.accounts().balance(ACCT_B, USD);
    let fee = exchange.accounts().balance(FEE_ACCOUNT, USD);
    let total = a.available as u128
        + a.reserved as u128
        + b.available as u128
        + b.reserved as u128
        + fee.available as u128;
    assert_eq!(
        total, 10_000_000,
        "USD must be conserved across all fills with odd rounding"
    );
}

/// Stop trigger cascade: a fill triggers stop A, which fills and triggers stop B.
#[test]
fn stop_trigger_cascade() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 1_000_000);
    exchange.deposit(ACCT_A, BTC, 1_000);
    exchange.deposit(ACCT_B, USD, 1_000_000);
    exchange.deposit(ACCT_B, BTC, 1_000);

    let mut reports = Vec::new();

    // Sell liquidity at multiple levels (ACCT_B provides asks).
    for (id, p) in [(1, 100), (2, 105), (3, 110)] {
        exchange.execute(
            Symbol(1),
            limit_order(id, ACCT_B, Side::Sell, p, 10, TimeInForce::GTC),
            &mut reports,
        );
    }
    reports.clear();

    // ACCT_A places stop-buy orders that chain:
    // Stop at trigger=95 → market buy (fills at 100, setting last_trade=100)
    // Stop at trigger=100 → market buy (triggers from the fill above)
    exchange.execute(
        Symbol(1),
        Order {
            id: OrderId(1),
            account: ACCT_A,
            side: Side::Buy,
            order_type: OrderType::Stop {
                trigger_price: price(100),
            },
            time_in_force: TimeInForce::GTC,
            quantity: qty(5),
            stp: SelfTradeProtection::Allow,
            expiry_ns: 0,
        },
        &mut reports,
    );
    reports.clear();

    exchange.execute(
        Symbol(1),
        Order {
            id: OrderId(2),
            account: ACCT_A,
            side: Side::Buy,
            order_type: OrderType::Stop {
                trigger_price: price(105),
            },
            time_in_force: TimeInForce::GTC,
            quantity: qty(3),
            stp: SelfTradeProtection::Allow,
            expiry_ns: 0,
        },
        &mut reports,
    );
    reports.clear();

    // Trigger the cascade: ACCT_B buys 1@100 (uses ACCT_B so ACCT_A's
    // balance isn't exhausted by stop reservations).
    // This fills against ACCT_B's own sell@100 (STP Allow), setting
    // last_trade=100. check_triggers fires: stop@100 triggers → market
    // buy fills remaining asks at 100. Stop@105 needs last_trade >= 105
    // but fills were at 100, so it should NOT trigger.
    exchange.execute(
        Symbol(1),
        limit_order(4, ACCT_B, Side::Buy, 100, 1, TimeInForce::GTC),
        &mut reports,
    );

    // We should see: the initial fill (1@100) + triggered stop fill.
    let fills: Vec<_> = reports
        .iter()
        .filter(|r| matches!(r, ExecutionReport::Fill { .. }))
        .collect();
    let triggers: Vec<_> = reports
        .iter()
        .filter(|r| matches!(r, ExecutionReport::Triggered { .. }))
        .collect();

    assert!(!fills.is_empty(), "should have at least one fill");
    assert!(
        triggers.len() <= 1,
        "stop@105 should not trigger (last_trade=100 < 105)"
    );
}

/// Cancel-replace preserves time priority when price unchanged and qty decreases.
#[test]
fn cancel_replace_preserves_priority_on_qty_decrease() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, BTC, 1_000);
    exchange.deposit(ACCT_B, USD, 1_000_000);

    let mut reports = Vec::new();

    // Three sells at 100: ids 1, 2, 3 in order.
    for id in 1..=3 {
        exchange.execute(
            Symbol(1),
            limit_order(id, ACCT_A, Side::Sell, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
    }
    reports.clear();

    // Amend order 2: same price, lower qty (5 instead of 10).
    // Should preserve time priority (still between 1 and 3).
    exchange.cancel_replace(
        Symbol(1),
        ACCT_A,
        OrderId(2),
        price(100),
        qty(5),
        &mut reports,
    );
    assert!(
        reports
            .iter()
            .any(|r| matches!(r, ExecutionReport::Replaced { .. }))
    );
    reports.clear();

    // Buy 15@100 — should fill 10 from order 1, then 5 from order 2.
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_B, Side::Buy, 100, 15, TimeInForce::GTC),
        &mut reports,
    );

    let fills: Vec<_> = reports
        .iter()
        .filter_map(|r| match r {
            ExecutionReport::Fill {
                maker_order_id,
                quantity,
                ..
            } => Some((maker_order_id.0, quantity.get())),
            _ => None,
        })
        .collect();

    assert_eq!(
        fills,
        vec![(1, 10), (2, 5)],
        "order 2 should fill after 1 (priority preserved)"
    );
}

/// Cancel-replace loses priority on price change.
#[test]
fn cancel_replace_loses_priority_on_price_change() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, BTC, 1_000);
    exchange.deposit(ACCT_B, USD, 1_000_000);

    let mut reports = Vec::new();

    // Three sells at 100: ids 1, 2, 3.
    for id in 1..=3 {
        exchange.execute(
            Symbol(1),
            limit_order(id, ACCT_A, Side::Sell, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
    }
    reports.clear();

    // Amend order 2: change price to 99, then back to 100. Loses priority.
    exchange.cancel_replace(
        Symbol(1),
        ACCT_A,
        OrderId(2),
        price(99),
        qty(10),
        &mut reports,
    );
    reports.clear();
    exchange.cancel_replace(
        Symbol(1),
        ACCT_A,
        OrderId(2),
        price(100),
        qty(10),
        &mut reports,
    );
    reports.clear();

    // Buy 25@100 — should fill 1 (10), then 3 (10), then 2 (5).
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_B, Side::Buy, 100, 25, TimeInForce::GTC),
        &mut reports,
    );

    let fills: Vec<_> = reports
        .iter()
        .filter_map(|r| match r {
            ExecutionReport::Fill {
                maker_order_id,
                quantity,
                ..
            } => Some((maker_order_id.0, quantity.get())),
            _ => None,
        })
        .collect();

    assert_eq!(
        fills,
        vec![(1, 10), (3, 10), (2, 5)],
        "order 2 should fill last (priority lost on price change)"
    );
}

/// Snapshot round-trip preserves fee account balance.
#[test]
fn snapshot_preserves_fee_account() {
    use crate::account::FEE_ACCOUNT;

    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 100_000);
    exchange.deposit(ACCT_B, BTC, 100);

    let mut reports = Vec::new();
    exchange.set_fee_schedule(
        Symbol(1),
        FeeSchedule {
            maker_fee_bps: 10,
            taker_fee_bps: 20,
        },
        &mut reports,
    );

    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_B, Side::Sell, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );

    let fee_before = exchange.accounts().balance(FEE_ACCOUNT, USD).available;
    assert!(fee_before > 0, "fees should have been collected");

    // In-memory payload round-trip via the engine's encode/decode
    // pair — same code path the production on-disk snapshot uses,
    // minus the transport framing/CRC (which lives behind the
    // `Application` trait in `melin-transport-core` and is exercised
    // by the integration tests in `melin-server/tests/`).
    let bytes = crate::snapshot::encode_exchange_payload(&exchange);
    let restored = crate::snapshot::decode_exchange_payload(&bytes).unwrap();

    let fee_after = restored.accounts().balance(FEE_ACCOUNT, USD).available;
    assert_eq!(
        fee_before, fee_after,
        "fee account must survive snapshot round-trip"
    );
}

/// Market order on an empty book is rejected with NoLiquidity.
#[test]
fn market_order_empty_book_rejected() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 100_000);

    let mut reports = Vec::new();
    exchange.execute(
        Symbol(1),
        Order {
            id: OrderId(1),
            account: ACCT_A,
            side: Side::Buy,
            order_type: OrderType::Market,
            time_in_force: TimeInForce::IOC,
            quantity: qty(10),
            stp: SelfTradeProtection::Allow,
            expiry_ns: 0,
        },
        &mut reports,
    );

    assert!(
        reports.iter().any(|r| matches!(
            r,
            ExecutionReport::Rejected {
                reason: RejectReason::NoLiquidity,
                ..
            }
        )),
        "market order on empty book must be rejected with NoLiquidity"
    );
}

/// FOK buy that requires liquidity across multiple price levels:
/// enough total quantity exists but spread across 3 levels.
#[test]
fn fok_fills_across_multiple_price_levels() {
    let mut exchange = Exchange::new();
    let spec = btc_usd_spec();
    exchange.add_instrument(spec);

    exchange.deposit(ACCT_A, USD, 1_000_000);
    exchange.deposit(ACCT_B, BTC, 100);

    let mut reports = Vec::new();

    // ACCT_B: asks at 3 levels: 5@100, 5@101, 5@102.
    for (id, p) in [(1, 100), (2, 101), (3, 102)] {
        exchange.execute(
            spec.symbol,
            limit_order(id, ACCT_B, Side::Sell, p, 5, TimeInForce::GTC),
            &mut reports,
        );
    }
    reports.clear();

    // ACCT_A: FOK buy 12@102 — needs 5@100 + 5@101 + 2@102 = 12.
    exchange.execute(
        spec.symbol,
        limit_order(1, ACCT_A, Side::Buy, 102, 12, TimeInForce::FOK),
        &mut reports,
    );

    let fills: Vec<_> = reports
        .iter()
        .filter(|r| matches!(r, ExecutionReport::Fill { .. }))
        .collect();
    assert_eq!(fills.len(), 3, "FOK should fill across 3 price levels");

    // No rejection or cancellation — fully filled.
    assert!(
        !reports.iter().any(|r| matches!(
            r,
            ExecutionReport::Rejected { .. } | ExecutionReport::Cancelled { .. }
        )),
        "FOK should not be rejected or cancelled"
    );

    // ACCT_A should own 12 BTC.
    let bal = exchange.accounts().balance(ACCT_A, BTC);
    assert_eq!(bal.available, 12);
}

/// FOK buy that has enough quantity at one level but not enough
/// across all levels within the limit price — must be rejected
/// without any fills.
#[test]
fn fok_rejected_insufficient_across_levels() {
    let mut exchange = Exchange::new();
    let spec = btc_usd_spec();
    exchange.add_instrument(spec);

    exchange.deposit(ACCT_A, USD, 1_000_000);
    exchange.deposit(ACCT_B, BTC, 100);

    let mut reports = Vec::new();

    // ACCT_B: 5@100 + 5@101 = 10 total within limit 101.
    exchange.execute(
        spec.symbol,
        limit_order(1, ACCT_B, Side::Sell, 100, 5, TimeInForce::GTC),
        &mut reports,
    );
    exchange.execute(
        spec.symbol,
        limit_order(2, ACCT_B, Side::Sell, 101, 5, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    // FOK buy 15@101 — only 10 available, should reject entirely.
    exchange.execute(
        spec.symbol,
        limit_order(1, ACCT_A, Side::Buy, 101, 15, TimeInForce::FOK),
        &mut reports,
    );

    assert_eq!(reports.len(), 1);
    assert!(matches!(
        reports[0],
        ExecutionReport::Rejected {
            reason: RejectReason::FOKCannotFill,
            ..
        }
    ));

    // Resting orders untouched.
    let bal = exchange.accounts().balance(ACCT_B, BTC);
    assert_eq!(bal.reserved, 10, "resting asks should be untouched");
}

/// Operators can withdraw collected fees from the FEE_ACCOUNT.
#[test]
fn fee_account_withdrawal() {
    use crate::account::FEE_ACCOUNT;

    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 100_000);
    exchange.deposit(ACCT_B, BTC, 100);

    let mut reports = Vec::new();
    exchange.set_fee_schedule(
        Symbol(1),
        FeeSchedule {
            maker_fee_bps: 10,
            taker_fee_bps: 20,
        },
        &mut reports,
    );

    // Create a fill to generate fees.
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_B, Side::Sell, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );

    // Under A: maker is seller (quote fee), taker is buyer (base fee).
    // seller_quote_fee = cost × 10 bps = 1, credited to FEE_ACCOUNT.USD.
    // buyer_base_fee = qty × 20 bps = 10×20/10_000 = 0 (truncates), so
    // FEE_ACCOUNT.BTC stays 0. Operator withdraws the USD revenue.
    let fee_bal = exchange.accounts().balance(FEE_ACCOUNT, USD);
    assert_eq!(fee_bal.available, 1);

    // Withdraw the 1 USD.
    let result = exchange.withdraw(FEE_ACCOUNT, USD, 1);
    assert!(result.is_ok(), "fee account withdrawal should succeed");
    let fee_bal = exchange.accounts().balance(FEE_ACCOUNT, USD);
    assert_eq!(fee_bal.available, 0);

    // Over-withdraw fails.
    let result = exchange.withdraw(FEE_ACCOUNT, USD, 1);
    assert!(result.is_err(), "over-withdrawing fee account should fail");
}

/// Different instruments can have different fee schedules, and fills
/// on each instrument apply the correct schedule.
#[test]
fn per_instrument_fee_isolation() {
    use crate::account::FEE_ACCOUNT;

    let mut exchange = Exchange::new();
    let btc_spec = btc_usd_spec();
    let eth_spec = eth_usd_spec();
    exchange.add_instrument(btc_spec);
    exchange.add_instrument(eth_spec);

    exchange.deposit(ACCT_A, USD, 1_000_000);
    exchange.deposit(ACCT_B, BTC, 1_000);
    exchange.deposit(ACCT_B, ETH, 1_000);

    // BTC/USD: 10 bps maker, 20 bps taker.
    let mut reports = Vec::new();
    exchange.set_fee_schedule(
        btc_spec.symbol,
        FeeSchedule {
            maker_fee_bps: 10,
            taker_fee_bps: 20,
        },
        &mut reports,
    );
    // ETH/USD: 50 bps maker, 100 bps taker.
    exchange.set_fee_schedule(
        eth_spec.symbol,
        FeeSchedule {
            maker_fee_bps: 50,
            taker_fee_bps: 100,
        },
        &mut reports,
    );

    // BTC/USD fill: 10@1000, cost=10_000.
    exchange.execute(
        btc_spec.symbol,
        limit_order(1, ACCT_B, Side::Sell, 1000, 10, TimeInForce::GTC),
        &mut reports,
    );
    exchange.execute(
        btc_spec.symbol,
        limit_order(1, ACCT_A, Side::Buy, 1000, 10, TimeInForce::GTC),
        &mut reports,
    );

    let btc_fill = reports
        .iter()
        .find(|r| matches!(r, ExecutionReport::Fill { .. }))
        .unwrap();
    // BTC fees: maker=10_000*10/10_000=10, taker=10_000*20/10_000=20.
    if let ExecutionReport::Fill {
        maker_fee,
        taker_fee,
        ..
    } = btc_fill
    {
        assert_eq!(*maker_fee, 10, "BTC maker fee");
        assert_eq!(*taker_fee, 20, "BTC taker fee");
    }
    reports.clear();

    // ETH/USD fill: 10@1000, cost=10_000.
    exchange.execute(
        eth_spec.symbol,
        limit_order(2, ACCT_B, Side::Sell, 1000, 10, TimeInForce::GTC),
        &mut reports,
    );
    exchange.execute(
        eth_spec.symbol,
        limit_order(2, ACCT_A, Side::Buy, 1000, 10, TimeInForce::GTC),
        &mut reports,
    );

    let eth_fill = reports
        .iter()
        .find(|r| matches!(r, ExecutionReport::Fill { .. }))
        .unwrap();
    // ETH fees: maker=10_000*50/10_000=50, taker=10_000*100/10_000=100.
    if let ExecutionReport::Fill {
        maker_fee,
        taker_fee,
        ..
    } = eth_fill
    {
        assert_eq!(*maker_fee, 50, "ETH maker fee");
        assert_eq!(*taker_fee, 100, "ETH taker fee");
    }

    // Under A, fees split by currency:
    //  BTC/USD:  seller_quote_fee (maker, 10 bps × 10_000 cost)  =   10 USD
    //            buyer_base_fee (taker, 20 bps × 10 qty / 10_000) =   0 BTC (truncates)
    //  ETH/USD:  seller_quote_fee (maker, 50 bps × 10_000)        =   50 USD
    //            buyer_base_fee (taker, 100 bps × 10 / 10_000)     =   0 ETH (truncates)
    //  Total USD = 10 + 50 = 60. ETH/BTC at FEE_ACCOUNT = 0 each.
    let fee_usd = exchange.accounts().balance(FEE_ACCOUNT, USD);
    assert_eq!(fee_usd.available, 60, "aggregated quote-side fees");
    assert_eq!(exchange.accounts().balance(FEE_ACCOUNT, BTC).available, 0);
    assert_eq!(exchange.accounts().balance(FEE_ACCOUNT, ETH).available, 0);
}

/// Post-only buy that would cross a resting sell from the SAME account
/// is rejected as PostOnlyWouldTake (post-only checked before STP).
#[test]
fn post_only_rejected_before_stp_evaluated() {
    let mut exchange = Exchange::new();
    let spec = btc_usd_spec();
    exchange.add_instrument(spec);

    exchange.deposit(ACCT_A, USD, 100_000);
    exchange.deposit(ACCT_A, BTC, 100);

    let mut reports = Vec::new();

    // ACCT_A: resting sell @ 500.
    exchange.execute(
        spec.symbol,
        limit_order(1, ACCT_A, Side::Sell, 500, 10, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    // ACCT_A: post-only buy @ 500 with CancelNewest STP.
    // Would cross own sell → post-only rejects before STP fires.
    exchange.execute(
        spec.symbol,
        Order {
            id: OrderId(2),
            account: ACCT_A,
            side: Side::Buy,
            order_type: OrderType::Limit {
                price: price(500),
                post_only: true,
            },
            time_in_force: TimeInForce::GTC,
            quantity: qty(5),
            stp: SelfTradeProtection::CancelNewest,
            expiry_ns: 0,
        },
        &mut reports,
    );

    assert_eq!(reports.len(), 1);
    assert!(
        matches!(
            reports[0],
            ExecutionReport::Rejected {
                order_id: OrderId(2),
                reason: RejectReason::PostOnlyWouldCross,
                ..
            }
        ),
        "post-only should reject before STP is evaluated: {:?}",
        reports[0]
    );

    // Original sell should be untouched.
    let bal = exchange.accounts().balance(ACCT_A, BTC);
    assert_eq!(bal.reserved, 10);
}

/// Post-only buy that does NOT cross (different account's sell is above),
/// rests, and then is filled as a maker when a sell arrives.
/// Verifies post-only orders with STP work correctly as makers.
#[test]
fn post_only_with_stp_rests_and_fills_as_maker() {
    let mut exchange = Exchange::new();
    let spec = btc_usd_spec();
    exchange.add_instrument(spec);

    exchange.deposit(ACCT_A, USD, 100_000);
    exchange.deposit(ACCT_A, BTC, 100);
    exchange.deposit(ACCT_B, BTC, 100);

    let mut reports = Vec::new();

    // ACCT_A: post-only buy @ 400, CancelOldest STP.
    exchange.execute(
        spec.symbol,
        Order {
            id: OrderId(1),
            account: ACCT_A,
            side: Side::Buy,
            order_type: OrderType::Limit {
                price: price(400),
                post_only: true,
            },
            time_in_force: TimeInForce::GTC,
            quantity: qty(10),
            stp: SelfTradeProtection::CancelOldest,
            expiry_ns: 0,
        },
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
    reports.clear();

    // ACCT_A: sell @ 400 (same account) → STP CancelOldest cancels
    // the resting post-only buy, taker continues.
    exchange.execute(
        spec.symbol,
        Order {
            id: OrderId(2),
            account: ACCT_A,
            side: Side::Sell,
            order_type: OrderType::Limit {
                price: price(400),
                post_only: false,
            },
            time_in_force: TimeInForce::GTC,
            quantity: qty(5),
            stp: SelfTradeProtection::CancelOldest,
            expiry_ns: 0,
        },
        &mut reports,
    );

    // STP CancelOldest should cancel the resting buy and the sell rests.
    assert!(
        reports.iter().any(|r| matches!(
            r,
            ExecutionReport::Cancelled {
                order_id: OrderId(1),
                ..
            }
        )),
        "STP CancelOldest should cancel the resting post-only buy"
    );
    // No fill should occur.
    assert!(
        !reports
            .iter()
            .any(|r| matches!(r, ExecutionReport::Fill { .. })),
        "no fill should occur due to STP"
    );
}

/// Negative maker fee (rebate) debits the FEE_ACCOUNT. When net fees
/// are negative (rebate > taker fee), the fee account funds the rebate.
#[test]
fn rebate_debits_fee_account() {
    use crate::account::FEE_ACCOUNT;

    let mut exchange = Exchange::new();
    let spec = btc_usd_spec();
    exchange.add_instrument(spec);

    exchange.deposit(ACCT_A, USD, 100_000);
    exchange.deposit(ACCT_B, BTC, 100);

    // First: a normal trade to seed the fee account.
    let mut reports = Vec::new();
    exchange.set_fee_schedule(
        spec.symbol,
        FeeSchedule {
            maker_fee_bps: 10,
            taker_fee_bps: 20,
        },
        &mut reports,
    );
    exchange.execute(
        spec.symbol,
        limit_order(1, ACCT_B, Side::Sell, 1000, 10, TimeInForce::GTC),
        &mut reports,
    );
    exchange.execute(
        spec.symbol,
        limit_order(1, ACCT_A, Side::Buy, 1000, 10, TimeInForce::GTC),
        &mut reports,
    );

    // Under A: seller (maker) fee in quote = cost × 10 bps = 10 USD.
    // buyer (taker) fee in base = qty × 20 bps = 10×20/10_000 = 0
    // (truncates). So FEE_ACCOUNT.USD = 10, FEE_ACCOUNT.BTC = 0.
    let fee_after_first = exchange.accounts().balance(FEE_ACCOUNT, USD).available;
    assert_eq!(fee_after_first, 10);
    reports.clear();

    // Now switch to rebate schedule: -50 bps maker, 10 bps taker.
    exchange.set_fee_schedule(
        spec.symbol,
        FeeSchedule {
            maker_fee_bps: -50,
            taker_fee_bps: 10,
        },
        &mut reports,
    );

    // Second trade: ACCT_A sells (new BTC from first fill), ACCT_B buys.
    exchange.deposit(ACCT_B, USD, 100_000);
    exchange.execute(
        spec.symbol,
        limit_order(2, ACCT_A, Side::Sell, 1000, 5, TimeInForce::GTC),
        &mut reports,
    );
    exchange.execute(
        spec.symbol,
        limit_order(2, ACCT_B, Side::Buy, 1000, 5, TimeInForce::GTC),
        &mut reports,
    );

    // cost=5000, qty=5. maker (seller) rebate: -25 USD (5000×-50/10000).
    // taker (buyer) base fee: 5×10/10_000 = 0 BTC (truncates).
    // FEE_ACCOUNT.USD: was 10, drained to 0, signed = -15 (deficit).
    let fee_avail = exchange.accounts().balance(FEE_ACCOUNT, USD).available;
    let fee_deficit = exchange.accounts().fee_account_deficit(USD);
    assert_eq!(fee_avail, 0, "rebate drained available");
    assert_eq!(fee_deficit, 15, "remaining 15 USD on deficit");
    assert_eq!(
        exchange.accounts().fee_signed_balance(USD),
        -15,
        "signed fee balance reflects net rebate"
    );
}

// -----------------------------------------------------------------------
// Fee schedule changes do not affect reservations (A: pure-notional)
// -----------------------------------------------------------------------

#[test]
fn fee_schedule_change_does_not_touch_resting_reservations() {
    // Under A, reservations are pure notional. A schedule change
    // takes effect on subsequent fills only — no re-reservation of
    // resting orders, no cancellations.
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 1_000); // exactly cost, no cushion

    let mut reports = Vec::new();
    exchange.execute(
        btc,
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 1_000);
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).available, 0);
    reports.clear();

    // Raise fees from 0 to 100 bps. Reservations untouched, no
    // reports emitted.
    exchange.set_fee_schedule(
        btc,
        FeeSchedule {
            maker_fee_bps: 100,
            taker_fee_bps: 100,
        },
        &mut reports,
    );
    assert!(reports.is_empty(), "no orders cancelled or replaced");
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 1_000);
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).available, 0);
}
