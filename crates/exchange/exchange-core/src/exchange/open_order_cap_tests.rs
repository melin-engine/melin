//! SEC-03: per-account open-order cap tests. Exercise
//! `max_open_orders_per_account` and the `order_counts` bookkeeping
//! around it (the cap field + `release_open_order` + `cancel_all` all
//! live in `exchange.rs`).

use super::test_helpers::*;
use super::{DEFAULT_MAX_OPEN_ORDERS_PER_ACCOUNT, Exchange};
use crate::types::{
    CircuitBreakerConfig, ExecutionReport, Order, OrderId, OrderType, RejectReason,
    SelfTradeProtection, Side, Symbol, TimeInForce,
};

// --- SEC-03: per-account open-order cap -------------------------------

#[test]
fn max_open_orders_default_is_ten_thousand() {
    // Constant is the documented operator-friendly default. Asserted to
    // catch silent drift if someone bumps it without updating the
    // CLI flag default in `melin-server`.
    assert_eq!(DEFAULT_MAX_OPEN_ORDERS_PER_ACCOUNT, 10_000);
    let exchange = Exchange::new();
    assert_eq!(
        exchange.max_open_orders_per_account(),
        DEFAULT_MAX_OPEN_ORDERS_PER_ACCOUNT,
    );
}

#[test]
fn max_open_orders_zero_disables_cap() {
    // The `0` sentinel must continue to mean "unlimited" — operators
    // turn the cap off by passing `--max-orders-per-account=0`.
    let mut exchange = Exchange::new();
    exchange.set_max_open_orders_per_account(0);
    exchange.add_instrument(btc_usd_spec());
    // Plenty of cash for many small orders.
    exchange.deposit(ACCT_A, USD, 1_000_000);

    let mut reports = Vec::new();
    // 50 distinct prices keeps the test small but well past any
    // accidentally-small cap. Every order should rest, not reject.
    for i in 0..50u64 {
        exchange.execute(
            Symbol(1),
            limit_order(i + 1, ACCT_A, Side::Buy, 100 + i, 1, TimeInForce::GTC),
            &mut reports,
        );
    }
    assert!(
        !reports
            .iter()
            .any(|r| matches!(r, ExecutionReport::Rejected { .. })),
        "no rejections with cap disabled, got {reports:?}"
    );
}

#[test]
fn max_open_orders_rejects_at_cap() {
    let mut exchange = Exchange::new();
    exchange.set_max_open_orders_per_account(3);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 1_000_000);

    let mut reports = Vec::new();
    for i in 0..3u64 {
        exchange.execute(
            Symbol(1),
            limit_order(i + 1, ACCT_A, Side::Buy, 100 + i, 1, TimeInForce::GTC),
            &mut reports,
        );
    }
    // The first three rest cleanly.
    assert!(
        reports
            .iter()
            .all(|r| !matches!(r, ExecutionReport::Rejected { .. })),
        "first three should rest, got {reports:?}"
    );
    reports.clear();

    // Fourth submission must be rejected with the new reason.
    exchange.execute(
        Symbol(1),
        limit_order(4, ACCT_A, Side::Buy, 200, 1, TimeInForce::GTC),
        &mut reports,
    );
    assert_eq!(reports.len(), 1);
    assert!(
        matches!(
            reports[0],
            ExecutionReport::Rejected {
                reason: RejectReason::ExceedsMaxOpenOrders,
                ..
            }
        ),
        "expected ExceedsMaxOpenOrders, got {:?}",
        reports[0],
    );
    // Reservation must NOT be charged when the cap rejects — the cap
    // runs before `try_reserve`, which is the whole point of this
    // ordering (cheap fail, no reservation churn).
    let bal = exchange.accounts().balance(ACCT_A, USD);
    // 3 reservations × (price × qty) = 100+101+102 = 303.
    assert_eq!(bal.reserved, 303);
}

#[test]
fn max_open_orders_room_after_cancel() {
    // Cap is a soft limit: cancelling a resting order frees a slot.
    let mut exchange = Exchange::new();
    exchange.set_max_open_orders_per_account(2);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 1_000_000);

    let mut reports = Vec::new();
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 1, TimeInForce::GTC),
        &mut reports,
    );
    exchange.execute(
        Symbol(1),
        limit_order(2, ACCT_A, Side::Buy, 101, 1, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    // At cap. Submitting a third → reject.
    exchange.execute(
        Symbol(1),
        limit_order(3, ACCT_A, Side::Buy, 102, 1, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(
        reports[0],
        ExecutionReport::Rejected {
            reason: RejectReason::ExceedsMaxOpenOrders,
            ..
        }
    ));
    reports.clear();

    // Cancel one → a slot opens up → the next submission is accepted.
    exchange.cancel(Symbol(1), ACCT_A, OrderId(1), &mut reports);
    reports.clear();
    exchange.execute(
        Symbol(1),
        limit_order(3, ACCT_A, Side::Buy, 102, 1, TimeInForce::GTC),
        &mut reports,
    );
    assert!(
        !reports
            .iter()
            .any(|r| matches!(r, ExecutionReport::Rejected { .. })),
        "post-cancel submission should be accepted, got {reports:?}"
    );
}

#[test]
fn max_open_orders_spans_instruments() {
    // The cap is global per-account. Resting orders on instrument A
    // count against the same budget as resting orders on instrument B.
    let mut exchange = Exchange::new();
    exchange.set_max_open_orders_per_account(2);
    exchange.add_instrument(btc_usd_spec());
    exchange.add_instrument(eth_usd_spec());
    exchange.deposit(ACCT_A, USD, 1_000_000);

    let mut reports = Vec::new();
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 1, TimeInForce::GTC),
        &mut reports,
    );
    exchange.execute(
        Symbol(2),
        limit_order(2, ACCT_A, Side::Buy, 100, 1, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    // Third order on yet another instrument-side: capped.
    exchange.execute(
        Symbol(1),
        limit_order(3, ACCT_A, Side::Buy, 99, 1, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(
        reports[0],
        ExecutionReport::Rejected {
            reason: RejectReason::ExceedsMaxOpenOrders,
            ..
        }
    ));
}

#[test]
fn max_open_orders_counts_pending_stops() {
    // Pending stop orders consume a slot in `order_counts` exactly
    // like resting limits — both contribute to global memory growth
    // and must therefore both be capped.
    let mut exchange = Exchange::new();
    exchange.set_max_open_orders_per_account(2);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, BTC, 1_000);

    let mut reports = Vec::new();
    // Two pending stop sells (no taker exists, so they sit pending).
    for i in 0..2u64 {
        exchange.execute(
            Symbol(1),
            Order {
                id: OrderId(i + 1),
                account: ACCT_A,
                side: Side::Sell,
                order_type: OrderType::Stop {
                    trigger_price: price(50 + i),
                },
                time_in_force: TimeInForce::IOC,
                quantity: qty(1),
                stp: SelfTradeProtection::Allow,
                expiry_ns: 0,
            },
            &mut reports,
        );
    }
    reports.clear();

    // A third stop must hit the cap.
    exchange.execute(
        Symbol(1),
        Order {
            id: OrderId(3),
            account: ACCT_A,
            side: Side::Sell,
            order_type: OrderType::Stop {
                trigger_price: price(60),
            },
            time_in_force: TimeInForce::IOC,
            quantity: qty(1),
            stp: SelfTradeProtection::Allow,
            expiry_ns: 0,
        },
        &mut reports,
    );
    assert!(matches!(
        reports[0],
        ExecutionReport::Rejected {
            reason: RejectReason::ExceedsMaxOpenOrders,
            ..
        }
    ));
}

#[test]
fn max_open_orders_does_not_shadow_other_rejects() {
    // Reject ordering must still surface the venue/order-shape reason
    // first when both apply. A halted instrument or an oversized order
    // should *not* be reported as ExceedsMaxOpenOrders even when the
    // submitter is at cap, because the operator/customer needs the
    // real reason for triage.
    let mut exchange = Exchange::new();
    exchange.set_max_open_orders_per_account(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 1_000_000);

    // Fill the cap.
    let mut reports = Vec::new();
    exchange.execute(
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 1, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    // Halt the instrument — TradingHalted must win over the cap.
    exchange.set_circuit_breaker(
        Symbol(1),
        CircuitBreakerConfig {
            halted: true,
            ..Default::default()
        },
    );
    exchange.execute(
        Symbol(1),
        limit_order(2, ACCT_A, Side::Buy, 99, 1, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(
        reports[0],
        ExecutionReport::Rejected {
            reason: RejectReason::TradingHalted,
            ..
        }
    ));
    reports.clear();

    // Un-halt and try an unknown symbol — UnknownSymbol must also win.
    exchange.set_circuit_breaker(Symbol(1), CircuitBreakerConfig::default());
    exchange.execute(
        Symbol(99),
        limit_order(3, ACCT_A, Side::Buy, 99, 1, TimeInForce::GTC),
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
fn max_open_orders_survives_clone_via_snapshot() {
    // The cap is operator config, not journaled state, so it must be
    // copied across the in-process `clone_via_snapshot` path used by
    // the shadow-snapshot stage. Without this carry-over, the shadow
    // would diverge whenever the primary's cap is non-default.
    let mut exchange = Exchange::new();
    exchange.set_max_open_orders_per_account(7);
    let cloned = exchange.clone_via_snapshot();
    assert_eq!(cloned.max_open_orders_per_account(), 7);

    // And `0` (unlimited) must also round-trip.
    exchange.set_max_open_orders_per_account(0);
    let cloned_unlimited = exchange.clone_via_snapshot();
    assert_eq!(cloned_unlimited.max_open_orders_per_account(), 0);
}

#[test]
fn max_open_orders_does_not_charge_filled_orders() {
    // A taker order that fully fills doesn't permanently consume a
    // slot — `order_counts` decrements when the order completes. The
    // cap should let an account keep trading after fully-filled
    // submissions even when the headcount would suggest otherwise.
    let mut exchange = Exchange::new();
    exchange.set_max_open_orders_per_account(2);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 1_000_000);
    exchange.deposit(ACCT_B, BTC, 100);

    // Maker rests on the book (counts against B's cap, not A's).
    let mut reports = Vec::new();
    exchange.execute(
        Symbol(1),
        limit_order(10, ACCT_B, Side::Sell, 100, 1, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    // Taker fully fills against the maker — doesn't rest.
    exchange.execute(
        Symbol(1),
        market_order(1, ACCT_A, Side::Buy, 1),
        &mut reports,
    );
    // The taker fully filled, so A's count must be back to 0 and the
    // next two submissions (filling the remaining cap) should succeed.
    reports.clear();
    // Need fresh maker liquidity for the next fill.
    exchange.execute(
        Symbol(1),
        limit_order(11, ACCT_B, Side::Sell, 100, 1, TimeInForce::GTC),
        &mut reports,
    );
    // ACCT_B is now at the cap (maker 10 was fully consumed; only 11
    // is resting). One more from B should still go through.
    reports.clear();
    exchange.execute(
        Symbol(1),
        limit_order(12, ACCT_B, Side::Sell, 101, 1, TimeInForce::GTC),
        &mut reports,
    );
    assert!(
        !reports
            .iter()
            .any(|r| matches!(r, ExecutionReport::Rejected { .. })),
        "B should still have a slot, got {reports:?}"
    );
}
