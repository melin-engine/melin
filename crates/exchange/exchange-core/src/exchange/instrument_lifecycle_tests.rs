//! Instrument lifecycle tests. Cover `add_instrument`,
//! `disable_instrument`, `enable_instrument`, and
//! `remove_instrument` — the admin operations that remain on
//! `Exchange` (in `exchange.rs`) rather than in a per-method
//! submodule.

use super::Exchange;
use super::test_helpers::*;
use crate::types::{
    ExecutionReport, InstrumentStatus, Order, OrderId, OrderType, RejectReason,
    SelfTradeProtection, Side, Symbol, TimeInForce,
};

// --- Instrument lifecycle tests ---

#[test]
fn disable_instrument_cancels_all_orders() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 1_000_000);
    exchange.deposit(ACCT_B, BTC, 1000);

    let mut reports = Vec::new();
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
    exchange.execute(
        Symbol(1),
        limit_order(100, ACCT_B, Side::Sell, 200, 5, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    exchange.disable_instrument(Symbol(1), &mut reports);

    let cancelled_count = reports
        .iter()
        .filter(|r| matches!(r, ExecutionReport::Cancelled { .. }))
        .count();
    assert_eq!(cancelled_count, 3);
    assert!(matches!(
        reports.last().unwrap(),
        ExecutionReport::InstrumentStatusChanged {
            status: InstrumentStatus::Disabled,
            ..
        }
    ));
}

#[test]
fn disable_instrument_releases_reservations() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 10_000);

    let mut reports = Vec::new();
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 50, TimeInForce::GTC),
        &mut reports,
    );
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).available, 5_000);
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 5_000);

    reports.clear();
    exchange.disable_instrument(Symbol(1), &mut reports);

    assert_eq!(exchange.accounts().balance(ACCT_A, USD).available, 10_000);
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 0);
}

#[test]
fn disabled_instrument_rejects_new_orders() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 10_000);

    let mut reports = Vec::new();
    exchange.disable_instrument(Symbol(1), &mut reports);
    reports.clear();

    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert_eq!(reports.len(), 1);
    assert!(matches!(
        reports[0],
        ExecutionReport::Rejected {
            reason: RejectReason::InstrumentDisabled,
            ..
        }
    ));
}

#[test]
fn enable_instrument_allows_trading() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 10_000);

    let mut reports = Vec::new();
    exchange.disable_instrument(Symbol(1), &mut reports);
    reports.clear();

    exchange.enable_instrument(Symbol(1), &mut reports);
    assert!(matches!(
        reports.last().unwrap(),
        ExecutionReport::InstrumentStatusChanged {
            status: InstrumentStatus::Enabled,
            ..
        }
    ));
    reports.clear();

    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
}

#[test]
fn remove_only_when_disabled() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());

    let mut reports = Vec::new();
    exchange.remove_instrument(Symbol(1), &mut reports);
    assert!(reports.is_empty());

    exchange.deposit(ACCT_A, USD, 10_000);
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
}

#[test]
fn remove_frees_slot() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());

    let mut reports = Vec::new();
    exchange.disable_instrument(Symbol(1), &mut reports);
    reports.clear();

    exchange.remove_instrument(Symbol(1), &mut reports);
    assert!(matches!(
        reports.last().unwrap(),
        ExecutionReport::InstrumentStatusChanged {
            status: InstrumentStatus::Removed,
            ..
        }
    ));
    reports.clear();

    exchange.deposit(ACCT_A, USD, 10_000);
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(
        reports[0],
        ExecutionReport::Rejected {
            reason: RejectReason::UnknownSymbol,
            ..
        }
    ));
}

#[test]
fn disable_is_idempotent() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());

    let mut reports = Vec::new();
    exchange.disable_instrument(Symbol(1), &mut reports);
    reports.clear();

    exchange.disable_instrument(Symbol(1), &mut reports);
    assert!(reports.is_empty());
}

#[test]
fn add_after_remove() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());

    let mut reports = Vec::new();
    exchange.disable_instrument(Symbol(1), &mut reports);
    exchange.remove_instrument(Symbol(1), &mut reports);
    reports.clear();

    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 10_000);
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
}

#[test]
fn disable_cancels_pending_stops() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 1_000_000);

    let mut reports = Vec::new();
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
    reports.clear();

    exchange.disable_instrument(Symbol(1), &mut reports);

    let cancelled_count = reports
        .iter()
        .filter(|r| matches!(r, ExecutionReport::Cancelled { .. }))
        .count();
    assert_eq!(cancelled_count, 1);
}

#[test]
fn cancel_replace_on_disabled_instrument() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 10_000);

    let mut reports = Vec::new();
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    exchange.disable_instrument(Symbol(1), &mut reports);
    reports.clear();

    exchange.cancel_replace(
        Symbol(1),
        ACCT_A,
        OrderId(1),
        price(110),
        qty(10),
        &mut reports,
    );
    assert_eq!(reports.len(), 1);
    assert!(matches!(
        reports[0],
        ExecutionReport::Rejected {
            reason: RejectReason::InstrumentDisabled,
            ..
        }
    ));
}

#[test]
fn cancel_on_disabled_instrument_is_noop() {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 10_000);

    let mut reports = Vec::new();
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    exchange.disable_instrument(Symbol(1), &mut reports);
    assert_eq!(
        reports
            .iter()
            .filter(|r| matches!(r, ExecutionReport::Cancelled { .. }))
            .count(),
        1
    );
    reports.clear();

    exchange.cancel(Symbol(1), ACCT_A, OrderId(1), &mut reports);
    assert!(reports.is_empty());
}
