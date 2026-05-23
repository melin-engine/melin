//! Good-Til-Date (GTD) expiry tests. Cover validation
//! (`InvalidExpiry`), scheduling of `ExpireOrder` tasks, tombstone
//! handling for cancelled/filled GTDs, and the interaction with
//! `drain_due_scheduled_tasks` on the matching thread.

use super::Exchange;
use super::test_helpers::*;
use crate::types::{
    AccountId, ExecutionReport, Order, OrderId, OrderType, RejectReason, SelfTradeProtection, Side,
    Symbol, TimeInForce,
};

// -- GTD expiration tests --

#[test]
fn gtd_order_rejected_with_zero_expiry() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 10_000);

    let mut reports = Vec::new();
    exchange.execute(
        btc,
        Order {
            id: OrderId(1),
            account: ACCT_A,
            side: Side::Buy,
            order_type: OrderType::Limit {
                price: price(100),
                post_only: false,
            },
            time_in_force: TimeInForce::GTD,
            quantity: qty(10),
            stp: SelfTradeProtection::Allow,
            expiry_ns: 0,
        },
        &mut reports,
    );
    assert_eq!(reports.len(), 1);
    assert!(matches!(
        reports[0],
        ExecutionReport::Rejected {
            reason: RejectReason::InvalidExpiry,
            ..
        }
    ));
}

#[test]
fn non_gtd_order_rejected_with_nonzero_expiry() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 10_000);

    let mut reports = Vec::new();
    exchange.execute(
        btc,
        Order {
            id: OrderId(1),
            account: ACCT_A,
            side: Side::Buy,
            order_type: OrderType::Limit {
                price: price(100),
                post_only: false,
            },
            time_in_force: TimeInForce::GTC,
            quantity: qty(10),
            stp: SelfTradeProtection::Allow,
            expiry_ns: 5000,
        },
        &mut reports,
    );
    assert_eq!(reports.len(), 1);
    assert!(matches!(
        reports[0],
        ExecutionReport::Rejected {
            reason: RejectReason::InvalidExpiry,
            ..
        }
    ));
}

// -- Scheduler-driven GTD expiry --

/// Submitting a GTD limit that rests on the book schedules exactly one
/// `ExpireOrder` task on the heap. Draining before the deadline is a
/// no-op; draining at or after the deadline cancels the order and
/// releases its reservation.
#[test]
fn gtd_limit_schedules_and_expires_on_drain() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 10_000);

    let mut reports = Vec::new();
    exchange.execute(
        btc,
        Order {
            id: OrderId(1),
            account: ACCT_A,
            side: Side::Buy,
            order_type: OrderType::Limit {
                price: price(100),
                post_only: false,
            },
            time_in_force: TimeInForce::GTD,
            quantity: qty(5),
            stp: SelfTradeProtection::Allow,
            expiry_ns: 1_000,
        },
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
    reports.clear();
    assert_eq!(exchange.scheduled_task_count(), 1);
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 500);

    // Pre-deadline drain: nothing fires.
    exchange.drain_due_scheduled_tasks(999, &mut reports);
    assert!(reports.is_empty());
    assert_eq!(exchange.scheduled_task_count(), 1);

    // At-deadline drain: cancel + release.
    exchange.drain_due_scheduled_tasks(1_000, &mut reports);
    assert_eq!(reports.len(), 1);
    assert!(matches!(
        reports[0],
        ExecutionReport::Cancelled {
            order_id: OrderId(1),
            ..
        }
    ));
    assert_eq!(exchange.scheduled_task_count(), 0);
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 0);
}

/// A GTD pending stop also schedules an expiry task; firing the task
/// cancels the pending stop before it ever triggers.
#[test]
fn gtd_pending_stop_schedules_and_expires() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 100_000);

    let mut reports = Vec::new();
    exchange.execute(
        btc,
        Order {
            id: OrderId(1),
            account: ACCT_A,
            side: Side::Buy,
            order_type: OrderType::Stop {
                trigger_price: price(120),
            },
            time_in_force: TimeInForce::GTD,
            quantity: qty(10),
            stp: SelfTradeProtection::Allow,
            expiry_ns: 5_000,
        },
        &mut reports,
    );
    // Stops emit no Placed report at submit time.
    assert!(reports.is_empty());
    assert_eq!(exchange.scheduled_task_count(), 1);

    exchange.drain_due_scheduled_tasks(5_000, &mut reports);
    assert_eq!(reports.len(), 1);
    assert!(matches!(
        reports[0],
        ExecutionReport::Cancelled {
            order_id: OrderId(1),
            ..
        }
    ));
    assert_eq!(exchange.scheduled_task_count(), 0);
}

/// Cancelling a GTD order before its deadline leaves a tombstone task
/// in the heap. When the tombstone fires, `find_gtd_expiry` returns
/// None, the task drops silently, and no double-cancel report is emitted.
#[test]
fn cancelled_gtd_creates_tombstone_no_double_cancel() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 10_000);

    let mut reports = Vec::new();
    exchange.execute(
        btc,
        Order {
            id: OrderId(1),
            account: ACCT_A,
            side: Side::Buy,
            order_type: OrderType::Limit {
                price: price(100),
                post_only: false,
            },
            time_in_force: TimeInForce::GTD,
            quantity: qty(1),
            stp: SelfTradeProtection::Allow,
            expiry_ns: 2_000,
        },
        &mut reports,
    );
    reports.clear();
    assert_eq!(exchange.scheduled_task_count(), 1);

    // Cancel before deadline.
    exchange.cancel(btc, ACCT_A, OrderId(1), &mut reports);
    assert!(matches!(reports[0], ExecutionReport::Cancelled { .. }));
    reports.clear();
    // Tombstone still in heap.
    assert_eq!(exchange.scheduled_task_count(), 1);

    // Drain past deadline: tombstone drops without emitting anything.
    exchange.drain_due_scheduled_tasks(2_000, &mut reports);
    assert!(reports.is_empty(), "tombstone must not emit Cancelled");
    assert_eq!(exchange.scheduled_task_count(), 0);
}

/// Cancel-replace preserves the order's `expiry_ns`, so the originally
/// scheduled task remains valid and still fires at the original deadline.
#[test]
fn cancel_replace_preserves_gtd_expiry_schedule() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 10_000);

    let mut reports = Vec::new();
    exchange.execute(
        btc,
        Order {
            id: OrderId(1),
            account: ACCT_A,
            side: Side::Buy,
            order_type: OrderType::Limit {
                price: price(50),
                post_only: false,
            },
            time_in_force: TimeInForce::GTD,
            quantity: qty(2),
            stp: SelfTradeProtection::Allow,
            expiry_ns: 3_000,
        },
        &mut reports,
    );
    reports.clear();
    assert_eq!(exchange.scheduled_task_count(), 1);

    // Cancel-replace to a new price + size; expiry stays unchanged.
    exchange.cancel_replace(btc, ACCT_A, OrderId(1), price(60), qty(3), &mut reports);
    assert!(matches!(reports[0], ExecutionReport::Replaced { .. }));
    reports.clear();
    // Heap unchanged: no new schedule, no removal.
    assert_eq!(exchange.scheduled_task_count(), 1);

    // Original deadline still fires.
    exchange.drain_due_scheduled_tasks(3_000, &mut reports);
    assert_eq!(reports.len(), 1);
    assert!(matches!(
        reports[0],
        ExecutionReport::Cancelled {
            order_id: OrderId(1),
            ..
        }
    ));
}

/// A GTD limit that partially fills leaves the remainder on the book
/// — the scheduled task still cancels that remainder at expiry.
#[test]
fn gtd_partial_fill_remainder_still_expires() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 100_000);
    exchange.deposit(ACCT_B, BTC, 10);

    let mut reports = Vec::new();
    // ACCT_B places a small ask: 1 unit at price 100.
    exchange.execute(
        btc,
        Order {
            id: OrderId(1),
            account: ACCT_B,
            side: Side::Sell,
            order_type: OrderType::Limit {
                price: price(100),
                post_only: false,
            },
            time_in_force: TimeInForce::GTC,
            quantity: qty(1),
            stp: SelfTradeProtection::Allow,
            expiry_ns: 0,
        },
        &mut reports,
    );
    reports.clear();

    // ACCT_A submits a GTD buy for 5 units at 100 — fills 1, rests 4.
    exchange.execute(
        btc,
        Order {
            id: OrderId(2),
            account: ACCT_A,
            side: Side::Buy,
            order_type: OrderType::Limit {
                price: price(100),
                post_only: false,
            },
            time_in_force: TimeInForce::GTD,
            quantity: qty(5),
            stp: SelfTradeProtection::Allow,
            expiry_ns: 4_000,
        },
        &mut reports,
    );
    reports.clear();
    assert_eq!(exchange.scheduled_task_count(), 1);

    // Drain at the deadline — remainder cancelled.
    exchange.drain_due_scheduled_tasks(4_000, &mut reports);
    assert_eq!(reports.len(), 1);
    assert!(matches!(
        reports[0],
        ExecutionReport::Cancelled {
            order_id: OrderId(2),
            remaining_quantity,
            ..
        } if remaining_quantity.get() == 4
    ));
}

/// Triggered GTD stop becomes a resting limit (same OrderId, same
/// expiry_ns); the original scheduled task still finds and cancels it.
#[test]
fn gtd_stop_triggered_into_resting_still_expires() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 100_000);
    exchange.deposit(ACCT_B, BTC, 100);

    let mut reports = Vec::new();
    // ACCT_A: GTD stop-limit buy that triggers at 110, limit 110, exp 8000.
    exchange.execute(
        btc,
        Order {
            id: OrderId(1),
            account: ACCT_A,
            side: Side::Buy,
            order_type: OrderType::StopLimit {
                trigger_price: price(110),
                limit_price: price(110),
            },
            time_in_force: TimeInForce::GTD,
            quantity: qty(2),
            stp: SelfTradeProtection::Allow,
            expiry_ns: 8_000,
        },
        &mut reports,
    );
    reports.clear();
    assert_eq!(exchange.scheduled_task_count(), 1);

    // ACCT_B sells low to set last_trade and trigger the stop.
    // First make a buy to populate the bid side (so the sell can fill).
    exchange.execute(
        btc,
        Order {
            id: OrderId(10),
            account: ACCT_A,
            side: Side::Buy,
            order_type: OrderType::Limit {
                price: price(115),
                post_only: false,
            },
            time_in_force: TimeInForce::GTC,
            quantity: qty(1),
            stp: SelfTradeProtection::Allow,
            expiry_ns: 0,
        },
        &mut reports,
    );
    reports.clear();
    exchange.execute(
        btc,
        Order {
            id: OrderId(11),
            account: ACCT_B,
            side: Side::Sell,
            order_type: OrderType::Limit {
                price: price(115),
                post_only: false,
            },
            time_in_force: TimeInForce::IOC,
            quantity: qty(1),
            stp: SelfTradeProtection::Allow,
            expiry_ns: 0,
        },
        &mut reports,
    );
    // Should have triggered the stop — order 1 is now a resting limit.
    let triggered = reports.iter().any(|r| {
        matches!(
            r,
            ExecutionReport::Triggered {
                order_id: OrderId(1),
                ..
            }
        )
    });
    assert!(triggered, "stop should have triggered");
    reports.clear();

    // Drain past expiry — the originally scheduled task cancels the
    // now-resting limit form of the order.
    exchange.drain_due_scheduled_tasks(8_000, &mut reports);
    assert_eq!(reports.len(), 1);
    assert!(matches!(
        reports[0],
        ExecutionReport::Cancelled {
            order_id: OrderId(1),
            ..
        }
    ));
}

/// Triggered stop: stop sell triggers via a trade, triggered market
/// sell fills. Verifies that the stop's embedded reservation slot is
/// carried through check_triggers → execute_market → fill and that
/// balances are conserved.
#[test]
fn triggered_stop_fill_balance_conservation() {
    let mut exchange = Exchange::new();
    let spec = btc_usd_spec();
    exchange.add_instrument(spec);

    // Acct A: buyer with USD. Acct B: seller with BTC.
    exchange.deposit(ACCT_A, USD, 100_000);
    exchange.deposit(ACCT_B, BTC, 100);

    let mut reports = Vec::new();

    // Acct B places a stop sell at trigger=500, qty=10.
    exchange.execute(
        spec.symbol,
        Order {
            id: OrderId(1),
            account: ACCT_B,
            side: Side::Sell,
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

    // Acct B places a limit sell at price=500, qty=5.
    exchange.execute(
        spec.symbol,
        limit_order(2, ACCT_B, Side::Sell, 500, 5, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    // Acct A buys with market order qty=15. This should:
    // 1. Fill 5 against the limit sell at 500
    // 2. Trade at 500 triggers the stop sell
    // 3. Triggered stop becomes market sell — no bids left, so cancelled
    exchange.execute(
        spec.symbol,
        Order {
            id: OrderId(1),
            account: ACCT_A,
            side: Side::Buy,
            order_type: OrderType::Market,
            time_in_force: TimeInForce::GTC,
            quantity: qty(15),
            stp: SelfTradeProtection::Allow,
            expiry_ns: 0,
        },
        &mut reports,
    );

    // Verify balance conservation.
    let bal_a_usd = exchange.accounts().balance(ACCT_A, USD);
    let bal_b_usd = exchange.accounts().balance(ACCT_B, USD);
    let total_usd =
        bal_a_usd.available + bal_a_usd.reserved + bal_b_usd.available + bal_b_usd.reserved;
    assert_eq!(total_usd, 100_000, "USD conservation violated");

    let bal_a_btc = exchange.accounts().balance(ACCT_A, BTC);
    let bal_b_btc = exchange.accounts().balance(ACCT_B, BTC);
    let total_btc =
        bal_a_btc.available + bal_a_btc.reserved + bal_b_btc.available + bal_b_btc.reserved;
    assert_eq!(total_btc, 100, "BTC conservation violated");

    // No reservations should remain (all orders consumed or cancelled).
    assert_eq!(bal_a_usd.reserved, 0);
    assert_eq!(bal_b_btc.reserved, 0);
}

/// Triggered stop-limit that partially fills and rests: verifies
/// the triggered order's slot is resolvable from order_index for
/// fill accounting.
#[test]
fn triggered_stop_limit_partial_fill_rests() {
    let mut exchange = Exchange::new();
    let spec = btc_usd_spec();
    exchange.add_instrument(spec);

    exchange.deposit(ACCT_A, USD, 100_000);
    exchange.deposit(ACCT_B, BTC, 100);

    let mut reports = Vec::new();

    // Acct B places a stop-limit: trigger=500, limit=400, sell qty=10.
    exchange.execute(
        spec.symbol,
        Order {
            id: OrderId(1),
            account: ACCT_B,
            side: Side::Sell,
            order_type: OrderType::StopLimit {
                trigger_price: price(500),
                limit_price: price(400),
            },
            time_in_force: TimeInForce::GTC,
            quantity: qty(10),
            stp: SelfTradeProtection::Allow,
            expiry_ns: 0,
        },
        &mut reports,
    );
    reports.clear();

    // Acct A places a limit buy at 500, qty=5.
    exchange.execute(
        spec.symbol,
        limit_order(1, ACCT_A, Side::Buy, 500, 5, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    // Acct B places a limit sell at 500, qty=1 to create a trade.
    exchange.execute(
        spec.symbol,
        limit_order(2, ACCT_B, Side::Sell, 500, 1, TimeInForce::GTC),
        &mut reports,
    );
    // This trade at 500 triggers the stop-limit. The triggered limit
    // sell at 400 can match Acct A's buy at 500 (price 400 <= 500).
    // Acct A's buy has qty=5, triggered sell has qty=10 → partial fill.
    // Remaining 6 rests on the ask side at 400 (5 were consumed by
    // the initial sell + the fill).

    // Verify balance conservation.
    let bal_a_usd = exchange.accounts().balance(ACCT_A, USD);
    let bal_b_usd = exchange.accounts().balance(ACCT_B, USD);
    let total_usd =
        bal_a_usd.available + bal_a_usd.reserved + bal_b_usd.available + bal_b_usd.reserved;
    assert_eq!(total_usd, 100_000, "USD conservation violated");

    let bal_a_btc = exchange.accounts().balance(ACCT_A, BTC);
    let bal_b_btc = exchange.accounts().balance(ACCT_B, BTC);
    let total_btc =
        bal_a_btc.available + bal_a_btc.reserved + bal_b_btc.available + bal_b_btc.reserved;
    assert_eq!(total_btc, 100, "BTC conservation violated");
}

/// Stop-limit buy with IOC TIF: triggers, fills what's available,
/// cancels remainder (IOC semantics apply post-trigger).
#[test]
fn stop_limit_ioc_cancels_unfilled_remainder() {
    let mut exchange = Exchange::new();
    let spec = btc_usd_spec();
    exchange.add_instrument(spec);

    exchange.deposit(ACCT_A, USD, 100_000);
    exchange.deposit(ACCT_B, BTC, 100);
    exchange.deposit(ACCT_B, USD, 100_000);

    let mut reports = Vec::new();

    // ACCT_B: resting sell 3@500.
    exchange.execute(
        spec.symbol,
        limit_order(1, ACCT_B, Side::Sell, 500, 3, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    // ACCT_A: stop-limit buy, trigger=500, limit=500, qty=10, IOC.
    exchange.execute(
        spec.symbol,
        Order {
            id: OrderId(1),
            account: ACCT_A,
            side: Side::Buy,
            order_type: OrderType::StopLimit {
                trigger_price: price(500),
                limit_price: price(500),
            },
            time_in_force: TimeInForce::IOC,
            quantity: qty(10),
            stp: SelfTradeProtection::Allow,
            expiry_ns: 0,
        },
        &mut reports,
    );
    reports.clear();

    // Trade at 500 to trigger the stop.
    exchange.execute(
        spec.symbol,
        limit_order(2, ACCT_B, Side::Sell, 500, 1, TimeInForce::GTC),
        &mut reports,
    );
    exchange.execute(
        spec.symbol,
        limit_order(2, ACCT_A, Side::Buy, 500, 1, TimeInForce::GTC),
        &mut reports,
    );

    // Triggered limit buy fills 3@500 (resting ask), IOC cancels remaining 7.
    let fills: Vec<_> = reports
        .iter()
        .filter(|r| matches!(r, ExecutionReport::Fill { .. }))
        .collect();
    let cancels: Vec<_> = reports
        .iter()
        .filter(|r| {
            matches!(
                r,
                ExecutionReport::Cancelled {
                    order_id: OrderId(1),
                    ..
                }
            )
        })
        .collect();
    assert!(
        reports.iter().any(|r| matches!(
            r,
            ExecutionReport::Triggered {
                order_id: OrderId(1),
                ..
            }
        )),
        "stop should trigger"
    );
    // The trigger fill (1@500) + the stop-limit fills (3@500).
    assert!(
        fills.len() >= 2,
        "expected at least trigger fill + stop-limit fill, got {}",
        fills.len()
    );
    assert_eq!(cancels.len(), 1, "IOC remainder should be cancelled");
}

/// Stop-limit with STP CancelNewest: the triggered limit order should
/// respect self-trade prevention just like a regular limit order.
#[test]
fn stop_limit_stp_cancel_newest() {
    let mut exchange = Exchange::new();
    let spec = btc_usd_spec();
    exchange.add_instrument(spec);

    exchange.deposit(ACCT_A, USD, 100_000);
    exchange.deposit(ACCT_A, BTC, 100);
    exchange.deposit(ACCT_B, USD, 100_000);
    exchange.deposit(ACCT_B, BTC, 100);

    let mut reports = Vec::new();

    // ACCT_A: resting sell @ 500.
    exchange.execute(
        spec.symbol,
        limit_order(1, ACCT_A, Side::Sell, 500, 5, TimeInForce::GTC),
        &mut reports,
    );
    // ACCT_B: resting sell @ 500 (behind A in queue).
    exchange.execute(
        spec.symbol,
        limit_order(1, ACCT_B, Side::Sell, 500, 5, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    // ACCT_A: stop-limit buy, trigger=500, limit=500, qty=5, CancelNewest.
    exchange.execute(
        spec.symbol,
        Order {
            id: OrderId(2),
            account: ACCT_A,
            side: Side::Buy,
            order_type: OrderType::StopLimit {
                trigger_price: price(500),
                limit_price: price(500),
            },
            time_in_force: TimeInForce::GTC,
            quantity: qty(5),
            stp: SelfTradeProtection::CancelNewest,
            expiry_ns: 0,
        },
        &mut reports,
    );
    reports.clear();

    // Trigger via ACCT_B buy → trade at 500.
    exchange.execute(
        spec.symbol,
        limit_order(3, ACCT_B, Side::Buy, 500, 1, TimeInForce::GTC),
        &mut reports,
    );

    // Triggered stop-limit buy hits ACCT_A's own sell → CancelNewest
    // cancels the taker (the stop-limit). It should NOT self-trade.
    let self_fills: Vec<_> = reports
        .iter()
        .filter(|r| {
            matches!(
                r,
                ExecutionReport::Fill {
                    taker_order_id: OrderId(2),
                    maker_account: a,
                    ..
                } if *a == ACCT_A
            )
        })
        .collect();
    assert!(
        self_fills.is_empty(),
        "STP should prevent self-trade on triggered stop-limit"
    );
}

/// Stop-limit sell with wide gap between trigger and limit: the triggered
/// limit sell rests because its limit price is above the best bid.
#[test]
fn stop_limit_wide_trigger_limit_gap_rests() {
    let mut exchange = Exchange::new();
    let spec = btc_usd_spec();
    exchange.add_instrument(spec);

    exchange.deposit(ACCT_A, USD, 100_000);
    exchange.deposit(ACCT_A, BTC, 100);
    exchange.deposit(ACCT_B, USD, 100_000);
    exchange.deposit(ACCT_B, BTC, 100);

    let mut reports = Vec::new();

    // ACCT_A: resting buy @ 400.
    exchange.execute(
        spec.symbol,
        limit_order(1, ACCT_A, Side::Buy, 400, 10, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    // ACCT_B: stop-limit sell, trigger=500, limit=450, qty=5.
    // Wide gap: trigger at 500, but limit sell at 450 (above best bid 400).
    exchange.execute(
        spec.symbol,
        Order {
            id: OrderId(1),
            account: ACCT_B,
            side: Side::Sell,
            order_type: OrderType::StopLimit {
                trigger_price: price(500),
                limit_price: price(450),
            },
            time_in_force: TimeInForce::GTC,
            quantity: qty(5),
            stp: SelfTradeProtection::Allow,
            expiry_ns: 0,
        },
        &mut reports,
    );
    reports.clear();

    // Trade at 500 to trigger: ACCT_A sells 1@500 to ACCT_B.
    exchange.execute(
        spec.symbol,
        limit_order(2, ACCT_A, Side::Sell, 500, 1, TimeInForce::GTC),
        &mut reports,
    );
    exchange.execute(
        spec.symbol,
        limit_order(2, ACCT_B, Side::Buy, 500, 1, TimeInForce::GTC),
        &mut reports,
    );

    // Triggered limit sell at 450 can't match bid@400 (450 > 400) → rests.
    assert!(
        reports.iter().any(|r| matches!(
            r,
            ExecutionReport::Triggered {
                order_id: OrderId(1),
                ..
            }
        )),
        "stop should trigger"
    );
    assert!(
        reports.iter().any(|r| matches!(
            r,
            ExecutionReport::Placed {
                order_id: OrderId(1),
                side: Side::Sell,
                ..
            }
        )),
        "triggered limit sell should rest (limit 450 > best bid 400)"
    );

    // Verify balance conservation.
    let total_btc = exchange.accounts().balance(ACCT_A, BTC).available
        + exchange.accounts().balance(ACCT_A, BTC).reserved
        + exchange.accounts().balance(ACCT_B, BTC).available
        + exchange.accounts().balance(ACCT_B, BTC).reserved;
    assert_eq!(total_btc, 200, "BTC conservation violated");
}

/// Stop-limit buy fills across multiple price levels after triggering.
#[test]
fn stop_limit_fills_multiple_levels() {
    let mut exchange = Exchange::new();
    let spec = btc_usd_spec();
    exchange.add_instrument(spec);

    exchange.deposit(ACCT_A, USD, 1_000_000);
    exchange.deposit(ACCT_B, BTC, 1_000);
    exchange.deposit(ACCT_B, USD, 1_000_000);

    let mut reports = Vec::new();

    // ACCT_B: asks at 500, 510, 520.
    for (id, p) in [(1, 500), (2, 510), (3, 520)] {
        exchange.execute(
            spec.symbol,
            limit_order(id, ACCT_B, Side::Sell, p, 5, TimeInForce::GTC),
            &mut reports,
        );
    }
    reports.clear();

    // ACCT_A: stop-limit buy, trigger=500, limit=520, qty=12.
    // Should fill 5@500 + 5@510 + 2@520 after triggering.
    exchange.execute(
        spec.symbol,
        Order {
            id: OrderId(1),
            account: ACCT_A,
            side: Side::Buy,
            order_type: OrderType::StopLimit {
                trigger_price: price(500),
                limit_price: price(520),
            },
            time_in_force: TimeInForce::GTC,
            quantity: qty(12),
            stp: SelfTradeProtection::Allow,
            expiry_ns: 0,
        },
        &mut reports,
    );
    reports.clear();

    // Trigger: ACCT_B buys own ask at 500 (STP Allow).
    exchange.execute(
        spec.symbol,
        limit_order(4, ACCT_B, Side::Buy, 500, 1, TimeInForce::GTC),
        &mut reports,
    );

    // After trigger, the stop-limit becomes a limit buy at 520 with qty=12.
    // It sweeps: remaining 4@500 + 5@510 + 3 left, but only 5@520 available
    // at that level → fills 4@500 + 5@510 + partially 3@520.
    let trigger_count = reports
        .iter()
        .filter(|r| {
            matches!(
                r,
                ExecutionReport::Triggered {
                    order_id: OrderId(1),
                    ..
                }
            )
        })
        .count();
    assert_eq!(trigger_count, 1, "stop should trigger exactly once");

    let stop_limit_fills: Vec<_> = reports
        .iter()
        .filter(|r| {
            matches!(
                r,
                ExecutionReport::Fill {
                    taker_order_id: OrderId(1),
                    ..
                }
            )
        })
        .collect();
    assert!(
        stop_limit_fills.len() >= 2,
        "stop-limit should fill across multiple levels, got {} fills",
        stop_limit_fills.len()
    );

    // Verify balance conservation.
    let total_usd = exchange.accounts().balance(ACCT_A, USD).available
        + exchange.accounts().balance(ACCT_A, USD).reserved
        + exchange.accounts().balance(ACCT_B, USD).available
        + exchange.accounts().balance(ACCT_B, USD).reserved;
    assert_eq!(total_usd, 2_000_000, "USD conservation violated");
}

/// Snapshot round-trip: verifies that reservation slots survive
/// save/restore and that post-restore operations work correctly.
#[test]
fn snapshot_roundtrip_reservation_integrity() {
    let mut exchange = Exchange::new();
    let spec = btc_usd_spec();
    exchange.add_instrument(spec);

    exchange.deposit(ACCT_A, USD, 100_000);
    exchange.deposit(ACCT_B, BTC, 100);

    let mut reports = Vec::new();

    // Place a resting sell order.
    exchange.execute(
        spec.symbol,
        limit_order(1, ACCT_B, Side::Sell, 500, 10, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    // Snapshot and restore.
    let restored = exchange.clone_via_snapshot();

    // Verify the restored exchange has the correct reserved balance.
    let bal = restored.accounts().balance(ACCT_B, BTC);
    assert_eq!(bal.reserved, 10, "post-restore: seller reservation lost");

    // Execute a fill against the restored resting order.
    let mut restored = restored;
    exchange.execute(
        spec.symbol,
        limit_order(1, ACCT_A, Side::Buy, 500, 5, TimeInForce::GTC),
        &mut reports,
    );
    restored.execute(
        spec.symbol,
        limit_order(1, ACCT_A, Side::Buy, 500, 5, TimeInForce::GTC),
        &mut reports,
    );

    // Both should have the same balances after the fill.
    let orig_a_usd = exchange.accounts().balance(ACCT_A, USD);
    let rest_a_usd = restored.accounts().balance(ACCT_A, USD);
    assert_eq!(orig_a_usd.available, rest_a_usd.available);
    assert_eq!(orig_a_usd.reserved, rest_a_usd.reserved);

    let orig_b_btc = exchange.accounts().balance(ACCT_B, BTC);
    let rest_b_btc = restored.accounts().balance(ACCT_B, BTC);
    assert_eq!(orig_b_btc.available, rest_b_btc.available);
    assert_eq!(orig_b_btc.reserved, rest_b_btc.reserved);

    // Cancel the remaining resting order on the restored exchange.
    reports.clear();
    restored.cancel(spec.symbol, ACCT_B, OrderId(1), &mut reports);
    assert_eq!(
        restored.accounts().balance(ACCT_B, BTC).reserved,
        0,
        "post-restore cancel: reservation not released"
    );
}

#[test]
fn snapshot_roundtrip_multi_instrument_fill() {
    // Reproduce the shadow-stage panic at runtime: many resting orders
    // across multiple instruments, then a clone, then a taker that
    // matches against a recovered maker. If any maker's reservation
    // slot wasn't injected by `inject_reservation_slots`, the fill
    // path indexes the slab with `ReservationSlot::DUMMY` (u32::MAX)
    // and the engine panics. The bot's demo hits this scenario the
    // moment a bot order matches against a journal-recovered maker.
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    exchange.add_instrument(eth_usd_spec());

    // Seed lots of accounts with both currencies so we can place
    // many makers across both instruments — same shape the bot
    // produces (31 accounts touching 2 symbols). Plus two extra
    // accounts (50, 51) used by the post-restore takers below.
    for acct in 2..=32u32 {
        exchange.deposit(AccountId(acct), USD, 1_000_000);
        exchange.deposit(AccountId(acct), BTC, 1_000);
        exchange.deposit(AccountId(acct), ETH, 1_000);
    }
    for acct in [50u32, 51] {
        exchange.deposit(AccountId(acct), USD, 1_000_000);
        exchange.deposit(AccountId(acct), BTC, 1_000);
        exchange.deposit(AccountId(acct), ETH, 1_000);
    }

    // Place 200 resting orders, many sharing price levels (the bot's
    // shape — narrow spread, many orders clustered around the mid).
    let mut reports = Vec::new();
    for i in 0..200u64 {
        let acct = AccountId(2 + (i as u32 % 31));
        let symbol = if i % 2 == 0 { Symbol(1) } else { Symbol(2) };
        let side = if i % 3 == 0 { Side::Buy } else { Side::Sell };
        let id = 1000 + i;
        // Cluster prices: only ~10 distinct price points → many
        // orders per level → exercises the level-queue + index pair.
        let p = match side {
            Side::Buy => 95 - (i % 5),
            Side::Sell => 105 + (i % 5),
        };
        exchange.execute(
            symbol,
            limit_order(id, acct, side, p, 5, TimeInForce::GTC),
            &mut reports,
        );
    }
    reports.clear();

    let mut restored = exchange.clone_via_snapshot();

    // Drive a taker into the restored exchange that should match an
    // existing resting bid (any of the Side::Buy orders above).
    // If any maker slot is DUMMY, fill() panics here.
    restored.execute(
        Symbol(1),
        limit_order(9999, AccountId(50), Side::Sell, 50, 1, TimeInForce::GTC),
        &mut reports,
    );

    // Symmetric: a buy that crosses an existing ask.
    restored.execute(
        Symbol(2),
        limit_order(9998, AccountId(51), Side::Buy, 200, 1, TimeInForce::GTC),
        &mut reports,
    );

    // Survival is the assertion. If we got here without panicking,
    // every recovered maker had a real reservation slot.
}

#[test]
fn snapshot_roundtrip_preserves_live_dedup() {
    // The v15 snapshot format drops the explicit OrderId map and
    // rebuilds the live `(account, order_id)` set from `order_index`
    // + `stop_index` on restore. Verify the rebuild: a duplicate of
    // a still-resting order must reject post-restore, while a
    // duplicate of an order that *closed* before the snapshot must
    // succeed (the entry should not have made it into the live set).
    let mut exchange = Exchange::new();
    let spec = btc_usd_spec();
    exchange.add_instrument(spec);
    exchange.deposit(ACCT_A, USD, 100_000);
    exchange.deposit(ACCT_B, BTC, 100);

    let mut reports = Vec::new();

    // OrderId 7 fills (closes), OrderId 9 rests.
    exchange.execute(
        spec.symbol,
        limit_order(7, ACCT_B, Side::Sell, 100, 5, TimeInForce::GTC),
        &mut reports,
    );
    exchange.execute(
        spec.symbol,
        limit_order(7, ACCT_A, Side::Buy, 100, 5, TimeInForce::GTC),
        &mut reports,
    );
    exchange.execute(
        spec.symbol,
        limit_order(9, ACCT_A, Side::Buy, 90, 5, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();

    let mut restored = exchange.clone_via_snapshot();

    // Reusing OrderId 7 (closed before snapshot) must succeed.
    restored.execute(
        spec.symbol,
        limit_order(7, ACCT_A, Side::Buy, 89, 5, TimeInForce::GTC),
        &mut reports,
    );
    assert!(
        matches!(reports[0], ExecutionReport::Placed { .. }),
        "reuse of closed-before-snapshot id should place, got {:?}",
        reports[0]
    );

    // Duplicating OrderId 9 (resting at snapshot time) must reject.
    reports.clear();
    restored.execute(
        spec.symbol,
        limit_order(9, ACCT_A, Side::Buy, 88, 5, TimeInForce::GTC),
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
        "duplicate of live-at-snapshot id should reject, got {:?}",
        reports[0]
    );
}
