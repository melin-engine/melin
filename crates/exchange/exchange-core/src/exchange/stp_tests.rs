//! Self-trade prevention (STP) tests. Cover all four STP modes
//! (Allow, CancelNewest, CancelOldest, CancelBoth) across limit,
//! market, IOC, FOK, and triggered-stop flows. Logic lives inside
//! the matching engine (`OrderBook::execute` and `Exchange::execute`).

use super::Exchange;
use super::test_helpers::*;
use crate::types::{
    AccountId, ExecutionReport, Order, OrderId, OrderType, RejectReason, SelfTradeProtection, Side,
    Symbol, TimeInForce,
};

// -- Self-trade prevention --

/// Helper that creates a limit order with a specific STP mode.
fn limit_order_stp(
    id: u64,
    account: AccountId,
    side: Side,
    p: u64,
    q: u64,
    tif: TimeInForce,
    stp: SelfTradeProtection,
) -> Order {
    Order {
        id: OrderId(id),
        account,
        side,
        order_type: OrderType::Limit {
            price: price(p),
            post_only: false,
        },
        time_in_force: tif,
        quantity: qty(q),
        stp,
        expiry_ns: 0,
    }
}

fn market_order_stp(
    id: u64,
    account: AccountId,
    side: Side,
    q: u64,
    stp: SelfTradeProtection,
) -> Order {
    Order {
        id: OrderId(id),
        account,
        side,
        order_type: OrderType::Market,
        time_in_force: TimeInForce::IOC,
        quantity: qty(q),
        stp,
        expiry_ns: 0,
    }
}

#[test]
fn stp_allow_permits_self_trade() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 10_000);
    exchange.deposit(ACCT_A, BTC, 100);

    let mut reports = Vec::new();

    // Place sell at 100.
    exchange.execute(
        btc,
        limit_order_stp(
            1,
            ACCT_A,
            Side::Sell,
            100,
            10,
            TimeInForce::GTC,
            SelfTradeProtection::Allow,
        ),
        &mut reports,
    );
    reports.clear();

    // Same account buy — STP Allow, should fill.
    exchange.execute(
        btc,
        limit_order_stp(
            2,
            ACCT_A,
            Side::Buy,
            100,
            10,
            TimeInForce::GTC,
            SelfTradeProtection::Allow,
        ),
        &mut reports,
    );

    assert!(matches!(reports[0], ExecutionReport::Fill { .. }));
}

#[test]
fn stp_cancel_newest_rejects_taker() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 10_000);
    exchange.deposit(ACCT_A, BTC, 100);

    let mut reports = Vec::new();

    // Place sell at 100 (resting maker, STP doesn't matter on resting side).
    exchange.execute(
        btc,
        limit_order_stp(
            1,
            ACCT_A,
            Side::Sell,
            100,
            10,
            TimeInForce::GTC,
            SelfTradeProtection::Allow,
        ),
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
    reports.clear();

    // Same account buy with CancelNewest — taker should be cancelled, maker stays.
    exchange.execute(
        btc,
        limit_order_stp(
            2,
            ACCT_A,
            Side::Buy,
            100,
            10,
            TimeInForce::GTC,
            SelfTradeProtection::CancelNewest,
        ),
        &mut reports,
    );

    // Taker rejected due to STP.
    assert!(reports.iter().any(|r| matches!(
        r,
        ExecutionReport::Cancelled {
            order_id: OrderId(2),
            ..
        }
    )));
    // No fill occurred.
    assert!(
        !reports
            .iter()
            .any(|r| matches!(r, ExecutionReport::Fill { .. }))
    );

    // Maker still resting — verify by matching with a different account.
    reports.clear();
    exchange.deposit(ACCT_B, USD, 10_000);
    exchange.execute(
        btc,
        limit_order_stp(
            3,
            ACCT_B,
            Side::Buy,
            100,
            10,
            TimeInForce::GTC,
            SelfTradeProtection::CancelNewest,
        ),
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Fill { .. }));

    // Taker's balance should be fully restored.
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 0);
}

#[test]
fn stp_cancel_oldest_cancels_maker_continues_matching() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 10_000);
    exchange.deposit(ACCT_A, BTC, 50);
    exchange.deposit(ACCT_B, BTC, 50);

    let mut reports = Vec::new();

    // ACCT_A sells 5 @ 100 (will be cancelled by STP).
    exchange.execute(
        btc,
        limit_order_stp(
            1,
            ACCT_A,
            Side::Sell,
            100,
            5,
            TimeInForce::GTC,
            SelfTradeProtection::Allow,
        ),
        &mut reports,
    );
    // ACCT_B sells 5 @ 100 (should be matched after ACCT_A's is cancelled).
    exchange.execute(
        btc,
        limit_order_stp(
            2,
            ACCT_B,
            Side::Sell,
            100,
            5,
            TimeInForce::GTC,
            SelfTradeProtection::Allow,
        ),
        &mut reports,
    );
    reports.clear();

    // ACCT_A buys 5 @ 100 with CancelOldest — should skip own order, match with ACCT_B.
    exchange.execute(
        btc,
        limit_order_stp(
            3,
            ACCT_A,
            Side::Buy,
            100,
            5,
            TimeInForce::GTC,
            SelfTradeProtection::CancelOldest,
        ),
        &mut reports,
    );

    // Maker (order 1) cancelled.
    assert!(reports.iter().any(|r| matches!(
        r,
        ExecutionReport::Cancelled {
            order_id: OrderId(1),
            ..
        }
    )));
    // Fill against ACCT_B's order.
    assert!(reports.iter().any(|r| matches!(
        r,
        ExecutionReport::Fill {
            maker_order_id: OrderId(2),
            taker_order_id: OrderId(3),
            ..
        }
    )));

    // ACCT_A's sell reservation should be fully released.
    assert_eq!(exchange.accounts().balance(ACCT_A, BTC).reserved, 0);
}

#[test]
fn stp_cancel_both_cancels_maker_and_taker() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 10_000);
    exchange.deposit(ACCT_A, BTC, 100);

    let mut reports = Vec::new();

    // Place sell at 100.
    exchange.execute(
        btc,
        limit_order_stp(
            1,
            ACCT_A,
            Side::Sell,
            100,
            10,
            TimeInForce::GTC,
            SelfTradeProtection::Allow,
        ),
        &mut reports,
    );
    reports.clear();

    // Same account buy with CancelBoth.
    exchange.execute(
        btc,
        limit_order_stp(
            2,
            ACCT_A,
            Side::Buy,
            100,
            10,
            TimeInForce::GTC,
            SelfTradeProtection::CancelBoth,
        ),
        &mut reports,
    );

    // Maker cancelled.
    assert!(reports.iter().any(|r| matches!(
        r,
        ExecutionReport::Cancelled {
            order_id: OrderId(1),
            ..
        }
    )));
    // Taker cancelled.
    assert!(reports.iter().any(|r| matches!(
        r,
        ExecutionReport::Cancelled {
            order_id: OrderId(2),
            ..
        }
    )));
    // No fill.
    assert!(
        !reports
            .iter()
            .any(|r| matches!(r, ExecutionReport::Fill { .. }))
    );

    // Both reservations released.
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 0);
    assert_eq!(exchange.accounts().balance(ACCT_A, BTC).reserved, 0);
}

#[test]
fn stp_cancel_newest_after_partial_fill_with_other_account() {
    // Taker fills against a different account first, then hits own order.
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 10_000);
    exchange.deposit(ACCT_A, BTC, 50);
    exchange.deposit(ACCT_B, BTC, 50);

    let mut reports = Vec::new();

    // ACCT_B sells 5 @ 100 (at better time priority — placed first).
    exchange.execute(
        btc,
        limit_order_stp(
            1,
            ACCT_B,
            Side::Sell,
            100,
            5,
            TimeInForce::GTC,
            SelfTradeProtection::Allow,
        ),
        &mut reports,
    );
    // ACCT_A sells 5 @ 100.
    exchange.execute(
        btc,
        limit_order_stp(
            2,
            ACCT_A,
            Side::Sell,
            100,
            5,
            TimeInForce::GTC,
            SelfTradeProtection::Allow,
        ),
        &mut reports,
    );
    reports.clear();

    // ACCT_A buys 10 @ 100 with CancelNewest.
    // Should fill 5 against ACCT_B, then cancel remaining 5 when hitting own order.
    exchange.execute(
        btc,
        limit_order_stp(
            3,
            ACCT_A,
            Side::Buy,
            100,
            10,
            TimeInForce::GTC,
            SelfTradeProtection::CancelNewest,
        ),
        &mut reports,
    );

    // Fill against ACCT_B's order.
    assert!(reports.iter().any(|r| matches!(
            r,
            ExecutionReport::Fill { maker_order_id: OrderId(1), taker_order_id: OrderId(3), quantity, .. }
            if *quantity == qty(5)
        )));
    // Taker remainder cancelled.
    assert!(reports.iter().any(|r| matches!(
        r,
        ExecutionReport::Cancelled { order_id: OrderId(3), remaining_quantity, .. }
        if *remaining_quantity == qty(5)
    )));
    // ACCT_A's resting sell (order 2) is untouched.
    // No fill with order 2.
    assert!(!reports.iter().any(|r| matches!(
        r,
        ExecutionReport::Fill {
            maker_order_id: OrderId(2),
            ..
        }
    )));
}

#[test]
fn stp_different_accounts_always_match() {
    // STP should never prevent matches between different accounts.
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, BTC, 100);
    exchange.deposit(ACCT_B, USD, 10_000);

    let mut reports = Vec::new();

    exchange.execute(
        btc,
        limit_order_stp(
            1,
            ACCT_A,
            Side::Sell,
            100,
            10,
            TimeInForce::GTC,
            SelfTradeProtection::CancelNewest,
        ),
        &mut reports,
    );
    reports.clear();

    exchange.execute(
        btc,
        limit_order_stp(
            2,
            ACCT_B,
            Side::Buy,
            100,
            10,
            TimeInForce::GTC,
            SelfTradeProtection::CancelNewest,
        ),
        &mut reports,
    );

    assert!(matches!(reports[0], ExecutionReport::Fill { .. }));
}

#[test]
fn stp_cancel_newest_with_market_order() {
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 10_000);
    exchange.deposit(ACCT_A, BTC, 100);

    let mut reports = Vec::new();

    // Place sell at 100.
    exchange.execute(
        btc,
        limit_order_stp(
            1,
            ACCT_A,
            Side::Sell,
            100,
            10,
            TimeInForce::GTC,
            SelfTradeProtection::Allow,
        ),
        &mut reports,
    );
    reports.clear();

    // Market buy from same account with CancelNewest.
    exchange.execute(
        btc,
        market_order_stp(2, ACCT_A, Side::Buy, 10, SelfTradeProtection::CancelNewest),
        &mut reports,
    );

    // No fill, taker cancelled.
    assert!(
        !reports
            .iter()
            .any(|r| matches!(r, ExecutionReport::Fill { .. }))
    );
    assert!(reports.iter().any(|r| matches!(
        r,
        ExecutionReport::Cancelled {
            order_id: OrderId(2),
            ..
        }
    )));
}

#[test]
fn stp_cancel_oldest_cancels_multiple_resting_orders() {
    // Multiple resting orders from same account at different prices.
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 10_000);
    exchange.deposit(ACCT_A, BTC, 100);
    exchange.deposit(ACCT_B, BTC, 100);

    let mut reports = Vec::new();

    // ACCT_A sells 5 @ 100.
    exchange.execute(
        btc,
        limit_order_stp(
            1,
            ACCT_A,
            Side::Sell,
            100,
            5,
            TimeInForce::GTC,
            SelfTradeProtection::Allow,
        ),
        &mut reports,
    );
    // ACCT_A sells 5 @ 101.
    exchange.execute(
        btc,
        limit_order_stp(
            2,
            ACCT_A,
            Side::Sell,
            101,
            5,
            TimeInForce::GTC,
            SelfTradeProtection::Allow,
        ),
        &mut reports,
    );
    // ACCT_B sells 5 @ 102.
    exchange.execute(
        btc,
        limit_order_stp(
            3,
            ACCT_B,
            Side::Sell,
            102,
            5,
            TimeInForce::GTC,
            SelfTradeProtection::Allow,
        ),
        &mut reports,
    );
    reports.clear();

    // ACCT_A buys 5 @ 102 with CancelOldest — should skip both own orders,
    // cancel them, and match with ACCT_B @ 102.
    exchange.execute(
        btc,
        limit_order_stp(
            4,
            ACCT_A,
            Side::Buy,
            102,
            5,
            TimeInForce::GTC,
            SelfTradeProtection::CancelOldest,
        ),
        &mut reports,
    );

    // Both same-account makers cancelled.
    assert!(reports.iter().any(|r| matches!(
        r,
        ExecutionReport::Cancelled {
            order_id: OrderId(1),
            ..
        }
    )));
    assert!(reports.iter().any(|r| matches!(
        r,
        ExecutionReport::Cancelled {
            order_id: OrderId(2),
            ..
        }
    )));
    // Fill against ACCT_B.
    assert!(reports.iter().any(|r| matches!(
        r,
        ExecutionReport::Fill {
            maker_order_id: OrderId(3),
            taker_order_id: OrderId(4),
            ..
        }
    )));
}

#[test]
fn stp_cancel_newest_with_fok_rejects_entirely() {
    // FOK + CancelNewest: if STP would prevent full fill, FOK must reject.
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 10_000);
    exchange.deposit(ACCT_A, BTC, 100);

    let mut reports = Vec::new();

    // Place sell at 100.
    exchange.execute(
        btc,
        limit_order_stp(
            1,
            ACCT_A,
            Side::Sell,
            100,
            10,
            TimeInForce::GTC,
            SelfTradeProtection::Allow,
        ),
        &mut reports,
    );
    reports.clear();

    // FOK buy for 10 from same account — can't fill due to STP.
    exchange.execute(
        btc,
        limit_order_stp(
            2,
            ACCT_A,
            Side::Buy,
            100,
            10,
            TimeInForce::FOK,
            SelfTradeProtection::CancelNewest,
        ),
        &mut reports,
    );

    // FOK rejection (STP prevented the fill, so FOK can't be satisfied).
    assert!(reports.iter().any(|r| matches!(
        r,
        ExecutionReport::Rejected {
            reason: RejectReason::FOKCannotFill,
            ..
        } | ExecutionReport::Cancelled { .. }
    )));
    assert!(
        !reports
            .iter()
            .any(|r| matches!(r, ExecutionReport::Fill { .. }))
    );

    // Balances restored.
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 0);
}

#[test]
fn stp_cancel_newest_fok_mixed_book_no_partial_fill() {
    // FOK must not partially fill when STP prevents the rest.
    // Book: ACCT_B sells 5 @ 100, ACCT_A sells 5 @ 100.
    // ACCT_A FOK buy 10 @ 100 CancelNewest: would fill 5 from B then hit own
    // order. FOK must reject entirely — no partial fill allowed.
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 10_000);
    exchange.deposit(ACCT_A, BTC, 50);
    exchange.deposit(ACCT_B, BTC, 50);

    let mut reports = Vec::new();

    exchange.execute(
        btc,
        limit_order_stp(
            1,
            ACCT_B,
            Side::Sell,
            100,
            5,
            TimeInForce::GTC,
            SelfTradeProtection::Allow,
        ),
        &mut reports,
    );
    exchange.execute(
        btc,
        limit_order_stp(
            2,
            ACCT_A,
            Side::Sell,
            100,
            5,
            TimeInForce::GTC,
            SelfTradeProtection::Allow,
        ),
        &mut reports,
    );
    reports.clear();

    exchange.execute(
        btc,
        limit_order_stp(
            3,
            ACCT_A,
            Side::Buy,
            100,
            10,
            TimeInForce::FOK,
            SelfTradeProtection::CancelNewest,
        ),
        &mut reports,
    );

    // No fills should have occurred — FOK is all-or-nothing.
    assert!(
        !reports
            .iter()
            .any(|r| matches!(r, ExecutionReport::Fill { .. }))
    );
    // Order should be rejected or cancelled.
    assert!(reports.iter().any(|r| matches!(
        r,
        ExecutionReport::Rejected { .. } | ExecutionReport::Cancelled { .. }
    )));
    // ACCT_B's resting order must still be on the book.
    assert_eq!(exchange.accounts().balance(ACCT_B, BTC).reserved, 5);
    // ACCT_A's buy reservation must be fully released.
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 0);
}

#[test]
fn stp_cancel_oldest_fok_mixed_book_no_partial_fill() {
    // FOK + CancelOldest: same-account orders get cancelled during matching,
    // so FOK pre-check must exclude them. Without enough non-self liquidity,
    // FOK must reject.
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 10_000);
    exchange.deposit(ACCT_A, BTC, 50);
    exchange.deposit(ACCT_B, BTC, 50);

    let mut reports = Vec::new();

    // ACCT_A sells 5 @ 100 (will be cancelled by CancelOldest).
    exchange.execute(
        btc,
        limit_order_stp(
            1,
            ACCT_A,
            Side::Sell,
            100,
            5,
            TimeInForce::GTC,
            SelfTradeProtection::Allow,
        ),
        &mut reports,
    );
    // ACCT_B sells 5 @ 100 (only 5 non-self liquidity).
    exchange.execute(
        btc,
        limit_order_stp(
            2,
            ACCT_B,
            Side::Sell,
            100,
            5,
            TimeInForce::GTC,
            SelfTradeProtection::Allow,
        ),
        &mut reports,
    );
    reports.clear();

    // ACCT_A FOK buy 10 @ 100 CancelOldest — only 5 fillable, should reject.
    exchange.execute(
        btc,
        limit_order_stp(
            3,
            ACCT_A,
            Side::Buy,
            100,
            10,
            TimeInForce::FOK,
            SelfTradeProtection::CancelOldest,
        ),
        &mut reports,
    );

    // No fills.
    assert!(
        !reports
            .iter()
            .any(|r| matches!(r, ExecutionReport::Fill { .. }))
    );
    // Rejected because not enough non-self liquidity.
    assert!(reports.iter().any(|r| matches!(
        r,
        ExecutionReport::Rejected {
            reason: RejectReason::FOKCannotFill,
            ..
        }
    )));
    // Both resting orders still on book.
    assert_eq!(exchange.accounts().balance(ACCT_A, BTC).reserved, 5);
    assert_eq!(exchange.accounts().balance(ACCT_B, BTC).reserved, 5);
}

#[test]
fn stp_cancel_oldest_gtc_taker_rests_after_clearing() {
    // CancelOldest cancels same-account makers, fills what it can from
    // other accounts, and the GTC taker rests with remaining quantity.
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 10_000);
    exchange.deposit(ACCT_A, BTC, 50);
    exchange.deposit(ACCT_B, BTC, 50);

    let mut reports = Vec::new();

    // ACCT_A sells 5 @ 100 (will be cancelled).
    exchange.execute(
        btc,
        limit_order_stp(
            1,
            ACCT_A,
            Side::Sell,
            100,
            5,
            TimeInForce::GTC,
            SelfTradeProtection::Allow,
        ),
        &mut reports,
    );
    // ACCT_B sells 3 @ 100 (will fill).
    exchange.execute(
        btc,
        limit_order_stp(
            2,
            ACCT_B,
            Side::Sell,
            100,
            3,
            TimeInForce::GTC,
            SelfTradeProtection::Allow,
        ),
        &mut reports,
    );
    reports.clear();

    // ACCT_A buys 10 @ 100 GTC CancelOldest.
    // Should cancel own sell (5), fill 3 from B, rest 7 on book.
    exchange.execute(
        btc,
        limit_order_stp(
            3,
            ACCT_A,
            Side::Buy,
            100,
            10,
            TimeInForce::GTC,
            SelfTradeProtection::CancelOldest,
        ),
        &mut reports,
    );

    // Maker cancelled.
    assert!(reports.iter().any(|r| matches!(
        r,
        ExecutionReport::Cancelled {
            order_id: OrderId(1),
            ..
        }
    )));
    // Fill against ACCT_B.
    assert!(reports.iter().any(|r| matches!(
        r,
        ExecutionReport::Fill {
            maker_order_id: OrderId(2),
            taker_order_id: OrderId(3),
            ..
        }
    )));
    // Taker rests with remaining 7.
    assert!(reports.iter().any(|r| matches!(
        r,
        ExecutionReport::Placed { order_id: OrderId(3), quantity, .. }
        if *quantity == qty(7)
    )));

    // Verify the resting order matches with a new sell.
    reports.clear();
    exchange.deposit(ACCT_B, BTC, 50);
    exchange.execute(
        btc,
        limit_order_stp(
            4,
            ACCT_B,
            Side::Sell,
            100,
            7,
            TimeInForce::GTC,
            SelfTradeProtection::Allow,
        ),
        &mut reports,
    );
    assert!(matches!(reports[0], ExecutionReport::Fill { .. }));
}

#[test]
fn stp_cancel_both_mixed_book_partial_then_cancel() {
    // CancelBoth with a mixed book: fill other accounts first, then hit
    // own order → cancel both the maker and taker remainder.
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 10_000);
    exchange.deposit(ACCT_A, BTC, 50);
    exchange.deposit(ACCT_B, BTC, 50);

    let mut reports = Vec::new();

    // ACCT_B sells 5 @ 100.
    exchange.execute(
        btc,
        limit_order_stp(
            1,
            ACCT_B,
            Side::Sell,
            100,
            5,
            TimeInForce::GTC,
            SelfTradeProtection::Allow,
        ),
        &mut reports,
    );
    // ACCT_A sells 5 @ 100.
    exchange.execute(
        btc,
        limit_order_stp(
            2,
            ACCT_A,
            Side::Sell,
            100,
            5,
            TimeInForce::GTC,
            SelfTradeProtection::Allow,
        ),
        &mut reports,
    );
    reports.clear();

    // ACCT_A buys 10 @ 100 CancelBoth.
    exchange.execute(
        btc,
        limit_order_stp(
            3,
            ACCT_A,
            Side::Buy,
            100,
            10,
            TimeInForce::GTC,
            SelfTradeProtection::CancelBoth,
        ),
        &mut reports,
    );

    // Fill 5 against ACCT_B.
    assert!(reports.iter().any(|r| matches!(
        r,
        ExecutionReport::Fill {
            maker_order_id: OrderId(1),
            taker_order_id: OrderId(3),
            ..
        }
    )));
    // Own maker cancelled.
    assert!(reports.iter().any(|r| matches!(
        r,
        ExecutionReport::Cancelled {
            order_id: OrderId(2),
            ..
        }
    )));
    // Taker remainder cancelled.
    assert!(reports.iter().any(|r| matches!(
        r,
        ExecutionReport::Cancelled { order_id: OrderId(3), remaining_quantity, .. }
        if *remaining_quantity == qty(5)
    )));
    // No second fill.
    let fill_count = reports
        .iter()
        .filter(|r| matches!(r, ExecutionReport::Fill { .. }))
        .count();
    assert_eq!(fill_count, 1);
}

#[test]
fn stp_cancel_oldest_interleaved_same_price() {
    // At the same price level: [own, other, own, other].
    // CancelOldest should cancel own orders and fill others in order.
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 10_000);
    exchange.deposit(ACCT_A, BTC, 50);
    exchange.deposit(ACCT_B, BTC, 50);

    let mut reports = Vec::new();

    // Interleaved at price 100: A(3), B(2), A(4), B(1).
    exchange.execute(
        btc,
        limit_order_stp(
            1,
            ACCT_A,
            Side::Sell,
            100,
            3,
            TimeInForce::GTC,
            SelfTradeProtection::Allow,
        ),
        &mut reports,
    );
    exchange.execute(
        btc,
        limit_order_stp(
            2,
            ACCT_B,
            Side::Sell,
            100,
            2,
            TimeInForce::GTC,
            SelfTradeProtection::Allow,
        ),
        &mut reports,
    );
    exchange.execute(
        btc,
        limit_order_stp(
            3,
            ACCT_A,
            Side::Sell,
            100,
            4,
            TimeInForce::GTC,
            SelfTradeProtection::Allow,
        ),
        &mut reports,
    );
    exchange.execute(
        btc,
        limit_order_stp(
            4,
            ACCT_B,
            Side::Sell,
            100,
            1,
            TimeInForce::GTC,
            SelfTradeProtection::Allow,
        ),
        &mut reports,
    );
    reports.clear();

    // ACCT_A buys 3 @ 100 CancelOldest.
    // Should: cancel A(3), fill B(2), cancel A(4), fill B(1) → fully filled.
    exchange.execute(
        btc,
        limit_order_stp(
            5,
            ACCT_A,
            Side::Buy,
            100,
            3,
            TimeInForce::GTC,
            SelfTradeProtection::CancelOldest,
        ),
        &mut reports,
    );

    // Both own orders cancelled.
    assert!(reports.iter().any(|r| matches!(
        r,
        ExecutionReport::Cancelled {
            order_id: OrderId(1),
            ..
        }
    )));
    assert!(reports.iter().any(|r| matches!(
        r,
        ExecutionReport::Cancelled {
            order_id: OrderId(3),
            ..
        }
    )));
    // Both other-account orders filled.
    assert!(reports.iter().any(|r| matches!(
        r,
        ExecutionReport::Fill {
            maker_order_id: OrderId(2),
            ..
        }
    )));
    assert!(reports.iter().any(|r| matches!(
        r,
        ExecutionReport::Fill {
            maker_order_id: OrderId(4),
            ..
        }
    )));
    // Taker fully filled (no Placed or Cancelled for order 5).
    assert!(!reports.iter().any(|r| matches!(
        r,
        ExecutionReport::Placed {
            order_id: OrderId(5),
            ..
        } | ExecutionReport::Cancelled {
            order_id: OrderId(5),
            ..
        }
    )));

    // ACCT_A sell reservations released for cancelled orders.
    // Originally reserved 3+4=7, both cancelled → 0 reserved.
    assert_eq!(exchange.accounts().balance(ACCT_A, BTC).reserved, 0);
}

#[test]
fn stp_cancel_newest_ioc() {
    // IOC + CancelNewest: STP cancels taker, same as IOC natural cancel.
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 10_000);
    exchange.deposit(ACCT_A, BTC, 50);
    exchange.deposit(ACCT_B, BTC, 50);

    let mut reports = Vec::new();

    // ACCT_B sells 3 @ 100.
    exchange.execute(
        btc,
        limit_order_stp(
            1,
            ACCT_B,
            Side::Sell,
            100,
            3,
            TimeInForce::GTC,
            SelfTradeProtection::Allow,
        ),
        &mut reports,
    );
    // ACCT_A sells 5 @ 100.
    exchange.execute(
        btc,
        limit_order_stp(
            2,
            ACCT_A,
            Side::Sell,
            100,
            5,
            TimeInForce::GTC,
            SelfTradeProtection::Allow,
        ),
        &mut reports,
    );
    reports.clear();

    // ACCT_A IOC buy 10 @ 100 CancelNewest.
    // Fills 3 from B, hits own order → cancel remainder (7).
    exchange.execute(
        btc,
        limit_order_stp(
            3,
            ACCT_A,
            Side::Buy,
            100,
            10,
            TimeInForce::IOC,
            SelfTradeProtection::CancelNewest,
        ),
        &mut reports,
    );

    // Fill against B.
    assert!(reports.iter().any(|r| matches!(
        r,
        ExecutionReport::Fill { maker_order_id: OrderId(1), quantity, .. }
        if *quantity == qty(3)
    )));
    // Taker cancelled with remaining 7.
    assert!(reports.iter().any(|r| matches!(
        r,
        ExecutionReport::Cancelled { order_id: OrderId(3), remaining_quantity, .. }
        if *remaining_quantity == qty(7)
    )));
    // ACCT_A's resting sell (order 2) untouched.
    assert_eq!(exchange.accounts().balance(ACCT_A, BTC).reserved, 5);
    // Taker buy reservation released.
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 0);
}

#[test]
fn stp_cancel_oldest_market_order() {
    // Market + CancelOldest: cancels own resting orders, fills others.
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 10_000);
    exchange.deposit(ACCT_A, BTC, 50);
    exchange.deposit(ACCT_B, BTC, 50);

    let mut reports = Vec::new();

    // ACCT_A sells 5 @ 100.
    exchange.execute(
        btc,
        limit_order_stp(
            1,
            ACCT_A,
            Side::Sell,
            100,
            5,
            TimeInForce::GTC,
            SelfTradeProtection::Allow,
        ),
        &mut reports,
    );
    // ACCT_B sells 5 @ 100.
    exchange.execute(
        btc,
        limit_order_stp(
            2,
            ACCT_B,
            Side::Sell,
            100,
            5,
            TimeInForce::GTC,
            SelfTradeProtection::Allow,
        ),
        &mut reports,
    );
    reports.clear();

    // ACCT_A market buy 5 CancelOldest.
    exchange.execute(
        btc,
        market_order_stp(3, ACCT_A, Side::Buy, 5, SelfTradeProtection::CancelOldest),
        &mut reports,
    );

    // Own maker cancelled.
    assert!(reports.iter().any(|r| matches!(
        r,
        ExecutionReport::Cancelled {
            order_id: OrderId(1),
            ..
        }
    )));
    // Fill against B.
    assert!(reports.iter().any(|r| matches!(
        r,
        ExecutionReport::Fill {
            maker_order_id: OrderId(2),
            taker_order_id: OrderId(3),
            ..
        }
    )));
    // Taker fully filled — no cancel for order 3.
    assert!(!reports.iter().any(|r| matches!(
        r,
        ExecutionReport::Cancelled {
            order_id: OrderId(3),
            ..
        }
    )));
}

#[test]
fn stp_cancel_both_market_order() {
    // Market + CancelBoth: both orders cancelled, no fill.
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 10_000);
    exchange.deposit(ACCT_A, BTC, 50);

    let mut reports = Vec::new();

    exchange.execute(
        btc,
        limit_order_stp(
            1,
            ACCT_A,
            Side::Sell,
            100,
            5,
            TimeInForce::GTC,
            SelfTradeProtection::Allow,
        ),
        &mut reports,
    );
    reports.clear();

    exchange.execute(
        btc,
        market_order_stp(2, ACCT_A, Side::Buy, 5, SelfTradeProtection::CancelBoth),
        &mut reports,
    );

    // No fill.
    assert!(
        !reports
            .iter()
            .any(|r| matches!(r, ExecutionReport::Fill { .. }))
    );
    // Both cancelled.
    assert!(reports.iter().any(|r| matches!(
        r,
        ExecutionReport::Cancelled {
            order_id: OrderId(1),
            ..
        }
    )));
    assert!(reports.iter().any(|r| matches!(
        r,
        ExecutionReport::Cancelled {
            order_id: OrderId(2),
            ..
        }
    )));
    // All reservations released.
    assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 0);
    assert_eq!(exchange.accounts().balance(ACCT_A, BTC).reserved, 0);
}

#[test]
fn stp_cancel_both_fok_mixed_book_rejects() {
    // FOK + CancelBoth: same-account orders excluded from FOK check.
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 10_000);
    exchange.deposit(ACCT_A, BTC, 50);
    exchange.deposit(ACCT_B, BTC, 50);

    let mut reports = Vec::new();

    exchange.execute(
        btc,
        limit_order_stp(
            1,
            ACCT_B,
            Side::Sell,
            100,
            5,
            TimeInForce::GTC,
            SelfTradeProtection::Allow,
        ),
        &mut reports,
    );
    exchange.execute(
        btc,
        limit_order_stp(
            2,
            ACCT_A,
            Side::Sell,
            100,
            5,
            TimeInForce::GTC,
            SelfTradeProtection::Allow,
        ),
        &mut reports,
    );
    reports.clear();

    // FOK buy 10, but only 5 non-self → reject.
    exchange.execute(
        btc,
        limit_order_stp(
            3,
            ACCT_A,
            Side::Buy,
            100,
            10,
            TimeInForce::FOK,
            SelfTradeProtection::CancelBoth,
        ),
        &mut reports,
    );

    assert!(
        !reports
            .iter()
            .any(|r| matches!(r, ExecutionReport::Fill { .. }))
    );
    assert!(reports.iter().any(|r| matches!(
        r,
        ExecutionReport::Rejected {
            reason: RejectReason::FOKCannotFill,
            ..
        }
    )));
}

#[test]
fn stp_triggered_stop_with_cancel_newest() {
    // A stop order with CancelNewest triggers and would match against
    // the same account's resting order. STP should prevent the fill.
    let mut exchange = Exchange::new();
    let btc = Symbol(1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 20_000);
    exchange.deposit(ACCT_A, BTC, 100);
    exchange.deposit(ACCT_B, USD, 20_000);
    exchange.deposit(ACCT_B, BTC, 100);

    let mut reports = Vec::new();

    // ACCT_A resting sell @ 100.
    exchange.execute(
        btc,
        limit_order_stp(
            1,
            ACCT_A,
            Side::Sell,
            100,
            5,
            TimeInForce::GTC,
            SelfTradeProtection::Allow,
        ),
        &mut reports,
    );
    // ACCT_B resting sell @ 100 (behind A in queue).
    exchange.execute(
        btc,
        limit_order_stp(
            2,
            ACCT_B,
            Side::Sell,
            100,
            5,
            TimeInForce::GTC,
            SelfTradeProtection::Allow,
        ),
        &mut reports,
    );
    // ACCT_A places a stop-buy that triggers at price 100, with CancelNewest.
    exchange.execute(
        btc,
        Order {
            id: OrderId(3),
            account: ACCT_A,
            side: Side::Buy,
            order_type: OrderType::Stop {
                trigger_price: price(100),
            },
            time_in_force: TimeInForce::IOC,
            quantity: qty(5),
            stp: SelfTradeProtection::CancelNewest,
            expiry_ns: 0,
        },
        &mut reports,
    );
    reports.clear();

    // A trade at price 100 triggers the stop.
    // ACCT_B buys 1 @ 100 from ACCT_A's resting sell → trade at 100.
    exchange.execute(
        btc,
        limit_order_stp(
            4,
            ACCT_B,
            Side::Buy,
            100,
            1,
            TimeInForce::GTC,
            SelfTradeProtection::Allow,
        ),
        &mut reports,
    );

    // The trade triggers ACCT_A's stop buy. The triggered stop becomes a
    // market buy with CancelNewest. The first ask is ACCT_A's remaining
    // sell (4 lots) → STP prevents the fill, taker cancelled.
    // Then it should match ACCT_B's sell (5 lots) — but CancelNewest
    // stops matching entirely when it hits own order.
    let triggered = reports.iter().any(|r| {
        matches!(
            r,
            ExecutionReport::Triggered {
                order_id: OrderId(3),
                ..
            }
        )
    });
    assert!(triggered, "stop should have triggered");

    // The triggered order should NOT have filled against ACCT_A's own resting sell.
    assert!(!reports.iter().any(|r| matches!(
        r,
        ExecutionReport::Fill {
            maker_order_id: OrderId(1),
            taker_order_id: OrderId(3),
            ..
        }
    )));
}
