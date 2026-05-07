//! Property-based tests for the trading engine.
//!
//! These tests verify invariants that must hold for *any* sequence of operations,
//! not just hand-crafted scenarios. They complement the unit tests in each module.

use std::collections::HashMap;
use std::num::NonZeroU64;

use proptest::prelude::*;

use crate::account::AccountManager;
use crate::exchange::Exchange;
use crate::orderbook::OrderBook;
use crate::types::*;

/// Default test account for orderbook proptests (same as orderbook::tests::TEST_ACCOUNT).
const TEST_ACCOUNT: AccountId = AccountId(1);

// ---------------------------------------------------------------------------
// Constants and helpers
// ---------------------------------------------------------------------------

const ACCT_A: AccountId = AccountId(1);
const ACCT_B: AccountId = AccountId(2);
const BTC: CurrencyId = CurrencyId(0);
const USD: CurrencyId = CurrencyId(1);

fn btc_usd_spec() -> InstrumentSpec {
    InstrumentSpec {
        symbol: Symbol(1),
        base: BTC,
        quote: USD,
    }
}

// ---------------------------------------------------------------------------
// Strategies
// ---------------------------------------------------------------------------

/// Generate a valid Price (1..=10_000 to keep price × quantity in u64 range).
fn arb_price() -> impl Strategy<Value = Price> {
    (1u64..=10_000).prop_map(|n| Price(NonZeroU64::new(n).unwrap()))
}

/// Generate a valid Quantity (1..=1_000).
fn arb_quantity() -> impl Strategy<Value = Quantity> {
    (1u64..=1_000).prop_map(|n| Quantity(NonZeroU64::new(n).unwrap()))
}

fn arb_side() -> impl Strategy<Value = Side> {
    prop_oneof![Just(Side::Buy), Just(Side::Sell)]
}

/// Generate a TIF value paired with an optional expiry_ns.
/// GTD requires a non-zero expiry; other TIFs use 0.
fn arb_tif() -> impl Strategy<Value = (TimeInForce, u64)> {
    prop_oneof![
        Just((TimeInForce::GTC, 0)),
        Just((TimeInForce::IOC, 0)),
        Just((TimeInForce::FOK, 0)),
        Just((TimeInForce::Day, 0)),
        // GTD with random expiry in a reasonable range (1..=10_000 ns).
        // Small values so Tick actions with similar range can trigger cancellation.
        (1u64..=10_000).prop_map(|exp| (TimeInForce::GTD, exp)),
    ]
}

fn arb_account() -> impl Strategy<Value = AccountId> {
    prop_oneof![Just(ACCT_A), Just(ACCT_B)]
}

fn arb_stp() -> impl Strategy<Value = SelfTradeProtection> {
    prop_oneof![
        Just(SelfTradeProtection::Allow),
        Just(SelfTradeProtection::CancelNewest),
        Just(SelfTradeProtection::CancelOldest),
        Just(SelfTradeProtection::CancelBoth),
    ]
}

// ---------------------------------------------------------------------------
// Order book actions (no balances — tests pure matching logic)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum BookAction {
    Limit {
        side: Side,
        price: Price,
        quantity: Quantity,
        tif: TimeInForce,
        stp: SelfTradeProtection,
    },
    Market {
        side: Side,
        quantity: Quantity,
        stp: SelfTradeProtection,
    },
    Stop {
        side: Side,
        trigger_price: Price,
        quantity: Quantity,
        stp: SelfTradeProtection,
    },
    StopLimit {
        side: Side,
        trigger_price: Price,
        limit_price: Price,
        quantity: Quantity,
        tif: TimeInForce,
        stp: SelfTradeProtection,
    },
    Cancel {
        /// Index into the action list to pick which order to cancel.
        target_idx: usize,
    },
}

fn arb_book_action() -> impl Strategy<Value = BookAction> {
    prop_oneof![
        4 => (arb_side(), arb_price(), arb_quantity(), arb_tif(), arb_stp()).prop_map(
            |(side, price, quantity, (tif, _expiry), stp)| BookAction::Limit { side, price, quantity, tif, stp }
        ),
        2 => (arb_side(), arb_quantity(), arb_stp()).prop_map(
            |(side, quantity, stp)| BookAction::Market { side, quantity, stp }
        ),
        1 => (arb_side(), arb_price(), arb_quantity(), arb_stp()).prop_map(
            |(side, trigger_price, quantity, stp)| BookAction::Stop { side, trigger_price, quantity, stp }
        ),
        1 => (arb_side(), arb_price(), arb_price(), arb_quantity(), arb_tif(), arb_stp()).prop_map(
            |(side, trigger_price, limit_price, quantity, (tif, _expiry), stp)| BookAction::StopLimit {
                side, trigger_price, limit_price, quantity, tif, stp,
            }
        ),
        1 => (0usize..200).prop_map(|target_idx| BookAction::Cancel { target_idx }),
    ]
}

fn arb_book_actions() -> impl Strategy<Value = Vec<BookAction>> {
    proptest::collection::vec(arb_book_action(), 1..=200)
}

// ---------------------------------------------------------------------------
// Exchange actions (includes deposits for balance-aware tests)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum ExchangeAction {
    Deposit {
        account: AccountId,
        currency: CurrencyId,
        amount: u64,
    },
    Limit {
        account: AccountId,
        side: Side,
        price: Price,
        quantity: Quantity,
        tif: TimeInForce,
        stp: SelfTradeProtection,
        post_only: bool,
        /// Non-zero for GTD orders (expiry timestamp in ns).
        expiry_ns: u64,
    },
    Market {
        account: AccountId,
        side: Side,
        quantity: Quantity,
        stp: SelfTradeProtection,
    },
    Stop {
        account: AccountId,
        side: Side,
        trigger_price: Price,
        quantity: Quantity,
        stp: SelfTradeProtection,
    },
    StopLimit {
        account: AccountId,
        side: Side,
        trigger_price: Price,
        limit_price: Price,
        quantity: Quantity,
        tif: TimeInForce,
        stp: SelfTradeProtection,
        /// Non-zero for GTD orders (expiry timestamp in ns).
        expiry_ns: u64,
    },
    Cancel {
        target_idx: usize,
    },
    CancelAll {
        account: AccountId,
    },
    /// Withdraw all balances for an account (CancelAll first, then drain).
    WithdrawAll {
        account: AccountId,
    },
    SetCircuitBreaker {
        halted: bool,
        lower: Option<Price>,
        upper: Option<Price>,
    },
    /// Cancel-replace a previous resting limit order.
    CancelReplace {
        target_idx: usize,
        new_price: Price,
        new_quantity: Quantity,
    },
    /// Set maker/taker fee schedule on the instrument.
    SetFeeSchedule {
        maker_fee_bps: i16,
        taker_fee_bps: i16,
    },
    /// Advance the engine clock; drains any due scheduled tasks (GTD expiry).
    Tick {
        now_ns: u64,
    },
}

fn arb_exchange_action() -> impl Strategy<Value = ExchangeAction> {
    prop_oneof![
        1 => (arb_account(), prop_oneof![Just(BTC), Just(USD)], 1u64..=100_000)
            .prop_map(|(account, currency, amount)| ExchangeAction::Deposit {
                account, currency, amount,
            }),
        4 => (arb_account(), arb_side(), arb_price(), arb_quantity(), arb_tif(), arb_stp(), proptest::bool::ANY)
            .prop_map(|(account, side, price, quantity, (tif, expiry_ns), stp, post_only)| ExchangeAction::Limit {
                account, side, price, quantity, tif, stp, post_only, expiry_ns,
            }),
        2 => (arb_account(), arb_side(), arb_quantity(), arb_stp())
            .prop_map(|(account, side, quantity, stp)| ExchangeAction::Market {
                account, side, quantity, stp,
            }),
        1 => (arb_account(), arb_side(), arb_price(), arb_quantity(), arb_stp())
            .prop_map(|(account, side, trigger_price, quantity, stp)| ExchangeAction::Stop {
                account, side, trigger_price, quantity, stp,
            }),
        1 => (arb_account(), arb_side(), arb_price(), arb_price(), arb_quantity(), arb_tif(), arb_stp())
            .prop_map(|(account, side, trigger_price, limit_price, quantity, (tif, expiry_ns), stp)| ExchangeAction::StopLimit {
                account, side, trigger_price, limit_price, quantity, tif, stp, expiry_ns,
            }),
        1 => (0usize..200).prop_map(|target_idx| ExchangeAction::Cancel { target_idx }),
        1 => arb_account().prop_map(|account| ExchangeAction::CancelAll { account }),
        1 => arb_account().prop_map(|account| ExchangeAction::WithdrawAll { account }),
        1 => (proptest::bool::ANY, proptest::option::of(arb_price()), proptest::option::of(arb_price()))
            .prop_map(|(halted, lower, upper)| ExchangeAction::SetCircuitBreaker {
                halted, lower, upper,
            }),
        // Cancel-replace a previous order with new price/quantity.
        2 => (0usize..200, arb_price(), arb_quantity()).prop_map(|(target_idx, new_price, new_quantity)| {
            ExchangeAction::CancelReplace { target_idx, new_price, new_quantity }
        }),
        // Set fee schedule (basis points, 0..=100). Rebates (negative fees)
        // are excluded: they create value funded by the exchange operator's
        // pre-deposited fee account balance, which the proptest doesn't set up.
        1 => (0i16..=100, 0i16..=100).prop_map(|(maker_fee_bps, taker_fee_bps)| {
            ExchangeAction::SetFeeSchedule { maker_fee_bps, taker_fee_bps }
        }),
        // Occasional Tick to exercise scheduler-driven GTD cancellation.
        // Timestamp range matches GTD expiry range (1..=10_000).
        1 => (1u64..=10_000).prop_map(|now_ns| ExchangeAction::Tick { now_ns }),
    ]
}

fn arb_exchange_actions() -> impl Strategy<Value = Vec<ExchangeAction>> {
    proptest::collection::vec(arb_exchange_action(), 1..=200)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Execute a sequence of BookActions, returning the reports and order ID mapping.
fn run_book_actions(
    book: &mut OrderBook,
    actions: &[BookAction],
) -> (Vec<ExecutionReport>, Vec<Option<OrderId>>) {
    let mut reports = Vec::new();
    let mut next_id = 1u64;
    let mut action_order_ids: Vec<Option<OrderId>> = Vec::new();

    for action in actions {
        let action_idx = action_order_ids.len();
        match action {
            BookAction::Limit {
                side,
                price,
                quantity,
                tif,
                stp,
            } => {
                let id = OrderId(next_id);
                next_id += 1;
                action_order_ids.push(Some(id));
                let order = Order {
                    id,
                    account: ACCT_A,
                    side: *side,
                    order_type: OrderType::Limit {
                        price: *price,
                        post_only: false,
                    },
                    time_in_force: *tif,
                    quantity: *quantity,
                    stp: *stp,
                    expiry_ns: 0,
                };
                book.execute(order, None, ReservationSlot::DUMMY, &mut reports);
            }
            BookAction::Market {
                side,
                quantity,
                stp,
            } => {
                let id = OrderId(next_id);
                next_id += 1;
                action_order_ids.push(Some(id));
                let order = Order {
                    id,
                    account: ACCT_A,
                    side: *side,
                    order_type: OrderType::Market,
                    time_in_force: TimeInForce::IOC,
                    quantity: *quantity,
                    stp: *stp,
                    expiry_ns: 0,
                };
                book.execute(order, None, ReservationSlot::DUMMY, &mut reports);
            }
            BookAction::Stop {
                side,
                trigger_price,
                quantity,
                stp,
            } => {
                let id = OrderId(next_id);
                next_id += 1;
                action_order_ids.push(Some(id));
                let order = Order {
                    id,
                    account: ACCT_A,
                    side: *side,
                    order_type: OrderType::Stop {
                        trigger_price: *trigger_price,
                    },
                    time_in_force: TimeInForce::GTC,
                    quantity: *quantity,
                    stp: *stp,
                    expiry_ns: 0,
                };
                book.execute(order, None, ReservationSlot::DUMMY, &mut reports);
            }
            BookAction::StopLimit {
                side,
                trigger_price,
                limit_price,
                quantity,
                tif,
                stp,
            } => {
                let id = OrderId(next_id);
                next_id += 1;
                action_order_ids.push(Some(id));
                let order = Order {
                    id,
                    account: ACCT_A,
                    side: *side,
                    order_type: OrderType::StopLimit {
                        trigger_price: *trigger_price,
                        limit_price: *limit_price,
                    },
                    time_in_force: *tif,
                    quantity: *quantity,
                    stp: *stp,
                    expiry_ns: 0,
                };
                book.execute(order, None, ReservationSlot::DUMMY, &mut reports);
            }
            BookAction::Cancel { target_idx } => {
                action_order_ids.push(None);
                if *target_idx < action_idx
                    && let Some(id) = action_order_ids[*target_idx]
                {
                    book.cancel(TEST_ACCOUNT, id, &mut reports);
                }
            }
        }
    }
    (reports, action_order_ids)
}

/// Build a map from OrderId → submitted quantity from the action list.
fn build_submitted_quantities(
    actions: &[BookAction],
    order_ids: &[Option<OrderId>],
) -> HashMap<OrderId, u64> {
    let mut map = HashMap::new();
    let mut id_idx = 0usize;
    for action in actions {
        match action {
            BookAction::Limit { quantity, .. }
            | BookAction::Market { quantity, .. }
            | BookAction::Stop { quantity, .. }
            | BookAction::StopLimit { quantity, .. } => {
                if let Some(id) = order_ids[id_idx] {
                    map.insert(id, quantity.get());
                }
                id_idx += 1;
            }
            BookAction::Cancel { .. } => {
                id_idx += 1;
            }
        }
    }
    map
}

/// Sum all resting and pending-stop quantities on the book.
fn book_total_quantity(book: &OrderBook) -> u64 {
    let mut total = 0u64;
    for (_price, level) in book.bids().levels_snapshot() {
        for order in &level {
            total += order.remaining().get();
        }
    }
    for (_price, level) in book.asks().levels_snapshot() {
        for order in &level {
            total += order.remaining().get();
        }
    }
    book.stop_buys().for_each_stop(|stop| {
        total += stop.quantity().get();
    });
    book.stop_sells().for_each_stop(|stop| {
        total += stop.quantity().get();
    });
    total
}

/// Check that order_sides, reservations, and book contents are consistent.
/// Panics with a descriptive message if any invariant is violated.
#[cfg(test)]
fn assert_exchange_consistent(exchange: &Exchange, action_idx: usize, action_desc: &str) {
    let order_sides = exchange.snapshot_order_sides();
    // Key by (AccountId, OrderId) — two accounts may share the same OrderId.
    let sides_ids: std::collections::HashSet<(AccountId, OrderId)> = order_sides
        .iter()
        .map(|((acct, id), _)| (*acct, *id))
        .collect();

    let reservations = exchange.snapshot_reservations();
    let reserved_ids: std::collections::HashSet<(AccountId, OrderId)> = reservations
        .iter()
        .map(|(id, acct, _, _)| (*acct, *id))
        .collect();

    let mut book_ids: std::collections::HashSet<(AccountId, OrderId)> =
        std::collections::HashSet::new();
    for (_sym, book) in exchange.books() {
        for (_price, level) in book.bids().levels_snapshot() {
            for order in &level {
                book_ids.insert((order.account(), order.id()));
            }
        }
        for (_price, level) in book.asks().levels_snapshot() {
            for order in &level {
                book_ids.insert((order.account(), order.id()));
            }
        }
        book.stop_buys().for_each_stop(|stop| {
            book_ids.insert((stop.account(), stop.id()));
        });
        book.stop_sells().for_each_stop(|stop| {
            book_ids.insert((stop.account(), stop.id()));
        });
    }

    // Stale order_sides entries (in order_sides but not on book).
    let stale_sides: Vec<_> = sides_ids.difference(&book_ids).collect();
    assert!(
        stale_sides.is_empty(),
        "After action #{action_idx} ({action_desc}): stale order_sides entries: {stale_sides:?}"
    );

    // Missing order_sides entries (on book but not in order_sides).
    let missing_sides: Vec<_> = book_ids.difference(&sides_ids).collect();
    assert!(
        missing_sides.is_empty(),
        "After action #{action_idx} ({action_desc}): missing order_sides entries: {missing_sides:?}"
    );

    // Orphan reservations (reserved but not on book).
    let orphan_res: Vec<_> = reserved_ids.difference(&book_ids).collect();
    assert!(
        orphan_res.is_empty(),
        "After action #{action_idx} ({action_desc}): orphan reservations: {orphan_res:?}"
    );
}

/// Return type for `run_exchange_actions`:
/// (exchange, action_order_ids, all_reports, withdrawn_amounts).
type ExchangeActionResult = (
    Exchange,
    Vec<Option<(OrderId, AccountId)>>,
    Vec<ExecutionReport>,
    HashMap<CurrencyId, u128>,
);

/// Run a sequence of ExchangeActions and return final exchange state plus all reports.
/// Also returns total withdrawn amounts per currency (from WithdrawAll actions).
fn run_exchange_actions(actions: &[ExchangeAction]) -> ExchangeActionResult {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    let mut reports = Vec::new();
    let mut all_reports = Vec::new();
    // Per-account order ID counters so two accounts can independently use the
    // same OrderId value — this is realistic (e.g., LOBSTER replay) and tests
    // that the engine correctly disambiguates by (account, order_id).
    let mut next_id_per_account: HashMap<AccountId, u64> = HashMap::new();
    let mut action_order_ids: Vec<Option<(OrderId, AccountId)>> = Vec::new();
    let mut withdrawn: HashMap<CurrencyId, u128> = HashMap::new();
    let sym = Symbol(1);

    for action in actions {
        let action_idx = action_order_ids.len();
        match action {
            ExchangeAction::Deposit {
                account,
                currency,
                amount,
            } => {
                action_order_ids.push(None);
                exchange.deposit(*account, *currency, *amount);
            }
            ExchangeAction::Limit {
                account,
                side,
                price,
                quantity,
                tif,
                stp,
                post_only,
                expiry_ns,
            } => {
                let counter = next_id_per_account.entry(*account).or_insert(0);
                *counter += 1;
                let id = OrderId(*counter);
                action_order_ids.push(Some((id, *account)));
                let order = Order {
                    id,
                    account: *account,
                    side: *side,
                    order_type: OrderType::Limit {
                        price: *price,
                        post_only: *post_only,
                    },
                    time_in_force: *tif,
                    quantity: *quantity,
                    stp: *stp,
                    expiry_ns: *expiry_ns,
                };
                exchange.execute(sym, order, &mut reports);
                all_reports.extend_from_slice(&reports);
                reports.clear();
            }
            ExchangeAction::Market {
                account,
                side,
                quantity,
                stp,
            } => {
                let counter = next_id_per_account.entry(*account).or_insert(0);
                *counter += 1;
                let id = OrderId(*counter);
                action_order_ids.push(Some((id, *account)));
                let order = Order {
                    id,
                    account: *account,
                    side: *side,
                    order_type: OrderType::Market,
                    time_in_force: TimeInForce::IOC,
                    quantity: *quantity,
                    stp: *stp,
                    expiry_ns: 0,
                };
                exchange.execute(sym, order, &mut reports);
                all_reports.extend_from_slice(&reports);
                reports.clear();
            }
            ExchangeAction::Stop {
                account,
                side,
                trigger_price,
                quantity,
                stp,
            } => {
                let counter = next_id_per_account.entry(*account).or_insert(0);
                *counter += 1;
                let id = OrderId(*counter);
                action_order_ids.push(Some((id, *account)));
                let order = Order {
                    id,
                    account: *account,
                    side: *side,
                    order_type: OrderType::Stop {
                        trigger_price: *trigger_price,
                    },
                    time_in_force: TimeInForce::GTC,
                    quantity: *quantity,
                    stp: *stp,
                    expiry_ns: 0,
                };
                exchange.execute(sym, order, &mut reports);
                all_reports.extend_from_slice(&reports);
                reports.clear();
            }
            ExchangeAction::StopLimit {
                account,
                side,
                trigger_price,
                limit_price,
                quantity,
                tif,
                stp,
                expiry_ns,
            } => {
                let counter = next_id_per_account.entry(*account).or_insert(0);
                *counter += 1;
                let id = OrderId(*counter);
                action_order_ids.push(Some((id, *account)));
                let order = Order {
                    id,
                    account: *account,
                    side: *side,
                    order_type: OrderType::StopLimit {
                        trigger_price: *trigger_price,
                        limit_price: *limit_price,
                    },
                    time_in_force: *tif,
                    quantity: *quantity,
                    stp: *stp,
                    expiry_ns: *expiry_ns,
                };
                exchange.execute(sym, order, &mut reports);
                all_reports.extend_from_slice(&reports);
                reports.clear();
            }
            ExchangeAction::Cancel { target_idx } => {
                action_order_ids.push(None);
                if *target_idx < action_idx
                    && let Some((id, account)) = action_order_ids[*target_idx]
                {
                    exchange.cancel(sym, account, id, &mut reports);
                    all_reports.extend_from_slice(&reports);
                    reports.clear();
                }
            }
            ExchangeAction::CancelAll { account } => {
                action_order_ids.push(None);
                exchange.cancel_all(*account, &mut reports);
                all_reports.extend_from_slice(&reports);
                reports.clear();
            }
            ExchangeAction::WithdrawAll { account } => {
                action_order_ids.push(None);
                // CancelAll first to clear resting orders.
                exchange.cancel_all(*account, &mut reports);
                all_reports.extend_from_slice(&reports);
                reports.clear();
                // Drain all balances for this account, tracking amounts.
                for &currency in &[BTC, USD] {
                    let avail = exchange.accounts().balance(*account, currency).available;
                    if avail > 0 {
                        exchange.withdraw(*account, currency, avail).unwrap();
                        *withdrawn.entry(currency).or_default() += avail as u128;
                    }
                }
            }
            ExchangeAction::SetCircuitBreaker {
                halted,
                lower,
                upper,
            } => {
                action_order_ids.push(None);
                exchange.set_circuit_breaker(
                    sym,
                    CircuitBreakerConfig {
                        price_band_lower: *lower,
                        price_band_upper: *upper,
                        halted: *halted,
                    },
                );
            }
            ExchangeAction::CancelReplace {
                target_idx,
                new_price,
                new_quantity,
            } => {
                action_order_ids.push(None);
                if *target_idx < action_idx
                    && let Some((id, account)) = action_order_ids[*target_idx]
                {
                    exchange.cancel_replace(
                        sym,
                        account,
                        id,
                        *new_price,
                        *new_quantity,
                        &mut reports,
                    );
                    all_reports.extend_from_slice(&reports);
                    reports.clear();
                }
            }
            ExchangeAction::SetFeeSchedule {
                maker_fee_bps,
                taker_fee_bps,
            } => {
                action_order_ids.push(None);
                exchange.set_fee_schedule(
                    sym,
                    FeeSchedule {
                        maker_fee_bps: *maker_fee_bps,
                        taker_fee_bps: *taker_fee_bps,
                    },
                    &mut reports,
                );
                reports.clear();
            }
            ExchangeAction::Tick { now_ns } => {
                action_order_ids.push(None);
                exchange.drain_due_scheduled_tasks(*now_ns, &mut reports);
                all_reports.extend_from_slice(&reports);
                reports.clear();
            }
        }
        assert_exchange_consistent(&exchange, action_idx, &format!("{:?}", action));
    }
    (exchange, action_order_ids, all_reports, withdrawn)
}

// ===========================================================================
// 1. Volume Conservation
// ===========================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    /// For any sequence of orders and cancels on a single order book:
    ///
    ///   total_submitted == 2 × total_filled + remaining_on_book + cancelled + rejected
    ///
    /// Each Fill event represents Q shares transferred — consuming Q from the
    /// taker's submitted quantity AND Q from the maker's resting quantity. Both
    /// were counted in total_submitted, so fills are counted with a factor of 2.
    #[test]
    fn volume_conservation(actions in arb_book_actions()) {
        let mut book = OrderBook::new(Symbol(0));
        let (reports, order_ids) = run_book_actions(&mut book, &actions);
        let submitted_map = build_submitted_quantities(&actions, &order_ids);

        let total_submitted: u64 = submitted_map.values().sum();

        let mut total_filled: u64 = 0;
        let mut total_cancelled: u64 = 0;
        let mut total_rejected: u64 = 0;

        for report in &reports {
            match report {
                ExecutionReport::Fill { quantity, .. } => {
                    total_filled += quantity.get();
                }
                ExecutionReport::Cancelled {
                    remaining_quantity, ..
                } => {
                    total_cancelled += remaining_quantity.get();
                }
                ExecutionReport::Rejected { order_id, .. } => {
                    if let Some(&q) = submitted_map.get(order_id) {
                        total_rejected += q;
                    }
                }
                ExecutionReport::Placed { .. }
                | ExecutionReport::Triggered { .. }
                | ExecutionReport::Replaced { .. }
                | ExecutionReport::InstrumentStatusChanged { .. } => {}
            }
        }

        let on_book = book_total_quantity(&book);

        prop_assert_eq!(
            total_submitted,
            2 * total_filled + on_book + total_cancelled + total_rejected,
            "volume conservation violated: submitted={} != 2*filled({}) + on_book({}) + cancelled({}) + rejected({})",
            total_submitted, total_filled, on_book, total_cancelled, total_rejected
        );
    }
}

// ===========================================================================
// 2. Order Book Consistency
// ===========================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    /// After any sequence of operations, the order_index HashMap and the
    /// BookSide BTreeMap levels must agree on which orders are resting.
    /// Same for stop_index vs stop_buys/stop_sells.
    #[test]
    fn book_index_consistency(actions in arb_book_actions()) {
        let mut book = OrderBook::new(Symbol(0));
        let _ = run_book_actions(&mut book, &actions);

        // --- Resting orders ---
        let index = book.snapshot_order_index();
        let mut ids_from_book = std::collections::HashSet::new();

        for (_price, level) in book.bids().levels_snapshot() {
            for order in &level {
                ids_from_book.insert((order.account(), order.id()));
            }
        }
        for (_price, level) in book.asks().levels_snapshot() {
            for order in &level {
                ids_from_book.insert((order.account(), order.id()));
            }
        }

        // Key by (AccountId, OrderId) — two accounts may share the same OrderId.
        let ids_from_index: std::collections::HashSet<(AccountId, OrderId)> =
            index.iter().map(|&(id, acct, _, _)| (acct, id)).collect();

        prop_assert_eq!(
            ids_from_book.len(),
            ids_from_index.len(),
            "order count mismatch: book has {}, index has {}",
            ids_from_book.len(),
            ids_from_index.len()
        );
        prop_assert_eq!(
            ids_from_book,
            ids_from_index,
            "order_index and book levels disagree on resting order IDs"
        );

        // --- No empty levels ---
        for (_price, level) in book.bids().levels_snapshot() {
            prop_assert!(!level.is_empty(), "empty bid level should have been removed");
        }
        for (_price, level) in book.asks().levels_snapshot() {
            prop_assert!(!level.is_empty(), "empty ask level should have been removed");
        }

        // --- Stop orders ---
        let stop_idx = book.snapshot_stop_index();
        let mut stop_ids_from_book = std::collections::HashSet::new();

        book.stop_buys().for_each_stop(|stop| {
                stop_ids_from_book.insert((stop.account(), stop.id()));
        });
        book.stop_sells().for_each_stop(|stop| {
                stop_ids_from_book.insert((stop.account(), stop.id()));
        });

        let stop_ids_from_index: std::collections::HashSet<(AccountId, OrderId)> =
            stop_idx.iter().map(|&(id, acct, _, _)| (acct, id)).collect();

        prop_assert_eq!(
            stop_ids_from_book,
            stop_ids_from_index,
            "stop_index and stop books disagree on pending stop IDs"
        );
    }
}

// ===========================================================================
// 3. Balance Conservation
// ===========================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    /// System-wide balance conservation: the sum of (available + reserved) across
    /// all accounts and currencies only changes via explicit deposits and
    /// withdrawals. Orders, fills, and cancels transfer value between
    /// available/reserved or between accounts, but never create or destroy value.
    ///
    ///   system_total == total_deposited - total_withdrawn - total_fees
    ///
    /// Since we don't track fees here, we use the weaker invariant:
    ///   system_total <= total_deposited - total_withdrawn
    #[test]
    fn balance_conservation(actions in arb_exchange_actions()) {
        let (exchange, _, _, withdrawn) = run_exchange_actions(&actions);

        // Track total deposited per currency.
        let mut total_deposited_btc: u128 = 0;
        let mut total_deposited_usd: u128 = 0;

        for action in &actions {
            if let ExchangeAction::Deposit { currency, amount, .. } = action {
                if *currency == BTC {
                    total_deposited_btc += *amount as u128;
                } else {
                    total_deposited_usd += *amount as u128;
                }
            }
        }

        let total_withdrawn_btc = *withdrawn.get(&BTC).unwrap_or(&0);
        let total_withdrawn_usd = *withdrawn.get(&USD).unwrap_or(&0);

        use crate::account::FEE_ACCOUNT;

        let accounts = exchange.accounts();
        // Include the fee collection account in the system total.
        let all_accounts = [ACCT_A, ACCT_B, FEE_ACCOUNT];
        let system_btc: u128 = all_accounts
            .iter()
            .map(|a| {
                let b = accounts.balance(*a, BTC);
                b.available as u128 + b.reserved as u128
            })
            .sum();
        let system_usd: u128 = all_accounts
            .iter()
            .map(|a| {
                let b = accounts.balance(*a, USD);
                b.available as u128 + b.reserved as u128
            })
            .sum();

        let net_btc = total_deposited_btc.saturating_sub(total_withdrawn_btc);
        let net_usd = total_deposited_usd.saturating_sub(total_withdrawn_usd);

        // Strict equality: fees are credited to FEE_ACCOUNT, so they stay
        // in the system. No value is created or destroyed.
        if total_deposited_btc <= u64::MAX as u128 && total_deposited_usd <= u64::MAX as u128 {
            prop_assert_eq!(
                system_btc, net_btc,
                "BTC conservation violated: system={} != net(deposited-withdrawn)={}",
                system_btc, net_btc
            );
            prop_assert_eq!(
                system_usd, net_usd,
                "USD conservation violated: system={} != net(deposited-withdrawn)={}",
                system_usd, net_usd
            );
        }
    }
}

/// Strategy biased toward fee-schedule-change-then-fill scenarios.
///
/// The general `arb_exchange_actions` strategy only has a 1-in-16 chance
/// of generating a SetFeeSchedule, and the bug requires a specific sequence
/// (order placement → fee change → fill). This strategy guarantees at least
/// one fee change sandwiched between orders, making conservation violations
/// from stale fee cushions much more likely to surface.
fn arb_fee_change_then_fill_actions() -> impl Strategy<Value = Vec<ExchangeAction>> {
    (
        // Phase 1: deposits + initial orders (placed under old fee schedule).
        proptest::collection::vec(arb_exchange_action(), 3..=30),
        // Fee schedule change.
        (0i16..=100, 0i16..=100),
        // Phase 2: more actions (fills happen under new fee schedule).
        proptest::collection::vec(arb_exchange_action(), 3..=30),
    )
        .prop_map(|(mut phase1, (maker_bps, taker_bps), phase2)| {
            phase1.push(ExchangeAction::SetFeeSchedule {
                maker_fee_bps: maker_bps,
                taker_fee_bps: taker_bps,
            });
            phase1.extend(phase2);
            phase1
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    /// Targeted variant of `balance_conservation` that biases toward the
    /// fee-schedule-change-then-fill pattern. This is the scenario where
    /// stale fee cushions on existing orders can cause silent value loss.
    #[test]
    fn balance_conservation_fee_change(actions in arb_fee_change_then_fill_actions()) {
        let (exchange, _, _, withdrawn) = run_exchange_actions(&actions);

        let mut total_deposited_btc: u128 = 0;
        let mut total_deposited_usd: u128 = 0;

        for action in &actions {
            if let ExchangeAction::Deposit { currency, amount, .. } = action {
                if *currency == BTC {
                    total_deposited_btc += *amount as u128;
                } else {
                    total_deposited_usd += *amount as u128;
                }
            }
        }

        let total_withdrawn_btc = *withdrawn.get(&BTC).unwrap_or(&0);
        let total_withdrawn_usd = *withdrawn.get(&USD).unwrap_or(&0);

        use crate::account::FEE_ACCOUNT;

        let accounts = exchange.accounts();
        let all_accounts = [ACCT_A, ACCT_B, FEE_ACCOUNT];
        let system_btc: u128 = all_accounts
            .iter()
            .map(|a| {
                let b = accounts.balance(*a, BTC);
                b.available as u128 + b.reserved as u128
            })
            .sum();
        let system_usd: u128 = all_accounts
            .iter()
            .map(|a| {
                let b = accounts.balance(*a, USD);
                b.available as u128 + b.reserved as u128
            })
            .sum();

        let net_btc = total_deposited_btc.saturating_sub(total_withdrawn_btc);
        let net_usd = total_deposited_usd.saturating_sub(total_withdrawn_usd);

        if total_deposited_btc <= u64::MAX as u128 && total_deposited_usd <= u64::MAX as u128 {
            prop_assert_eq!(
                system_btc, net_btc,
                "BTC conservation violated after fee change: system={} != net={}",
                system_btc, net_btc
            );
            prop_assert_eq!(
                system_usd, net_usd,
                "USD conservation violated after fee change: system={} != net={}",
                system_usd, net_usd
            );
        }
    }
}

// ===========================================================================
// 4. Price-Time Priority
// ===========================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    /// Place N limit sell orders at random prices, then send a large market buy.
    /// Fills must arrive in strict price order (lowest first) and FIFO within
    /// each price level.
    #[test]
    fn price_time_priority_asks(
        orders in proptest::collection::vec((arb_price(), arb_quantity()), 2..=50),
        market_qty in 1u64..=50_000,
    ) {
        let mut book = OrderBook::new(Symbol(0));
        let mut reports = Vec::new();

        for (i, (p, q)) in orders.iter().enumerate() {
            let order = Order {
                id: OrderId(i as u64 + 1),
                account: ACCT_A,
                side: Side::Sell,
                order_type: OrderType::Limit { price: *p, post_only: false },
                time_in_force: TimeInForce::GTC,
                quantity: *q,
                stp: SelfTradeProtection::Allow,
                expiry_ns: 0,
            };
            book.execute(order, None, ReservationSlot::DUMMY, &mut reports);
        }
        reports.clear();

        let market = Order {
            id: OrderId(orders.len() as u64 + 1),
            account: ACCT_B,
            side: Side::Buy,
            order_type: OrderType::Market,
            time_in_force: TimeInForce::IOC,
            quantity: Quantity(NonZeroU64::new(market_qty).unwrap()),
            stp: SelfTradeProtection::Allow,
            expiry_ns: 0,
        };
        book.execute(market, None, ReservationSlot::DUMMY, &mut reports);

        let fills: Vec<(Price, OrderId)> = reports
            .iter()
            .filter_map(|r| match r {
                ExecutionReport::Fill {
                    price,
                    maker_order_id,
                    ..
                } => Some((*price, *maker_order_id)),
                _ => None,
            })
            .collect();

        for window in fills.windows(2) {
            let (pa, ida) = window[0];
            let (pb, idb) = window[1];
            prop_assert!(
                pa <= pb,
                "ask fills not in price order: {}@{} before {}@{}",
                ida.0, pa.get(), idb.0, pb.get()
            );
            if pa == pb {
                prop_assert!(
                    ida.0 < idb.0,
                    "ask fills not in time order at price {}: order {} before {}",
                    pa.get(), ida.0, idb.0
                );
            }
        }
    }

    /// Same test for bids: fills must be in descending price order (best bid
    /// first), FIFO within each level.
    #[test]
    fn price_time_priority_bids(
        orders in proptest::collection::vec((arb_price(), arb_quantity()), 2..=50),
        market_qty in 1u64..=50_000,
    ) {
        let mut book = OrderBook::new(Symbol(0));
        let mut reports = Vec::new();

        for (i, (p, q)) in orders.iter().enumerate() {
            let order = Order {
                id: OrderId(i as u64 + 1),
                account: ACCT_A,
                side: Side::Buy,
                order_type: OrderType::Limit { price: *p, post_only: false },
                time_in_force: TimeInForce::GTC,
                quantity: *q,
                stp: SelfTradeProtection::Allow,
                expiry_ns: 0,
            };
            book.execute(order, None, ReservationSlot::DUMMY, &mut reports);
        }
        reports.clear();

        let market = Order {
            id: OrderId(orders.len() as u64 + 1),
            account: ACCT_B,
            side: Side::Sell,
            order_type: OrderType::Market,
            time_in_force: TimeInForce::IOC,
            quantity: Quantity(NonZeroU64::new(market_qty).unwrap()),
            stp: SelfTradeProtection::Allow,
            expiry_ns: 0,
        };
        book.execute(market, None, ReservationSlot::DUMMY, &mut reports);

        let fills: Vec<(Price, OrderId)> = reports
            .iter()
            .filter_map(|r| match r {
                ExecutionReport::Fill {
                    price,
                    maker_order_id,
                    ..
                } => Some((*price, *maker_order_id)),
                _ => None,
            })
            .collect();

        for window in fills.windows(2) {
            let (pa, ida) = window[0];
            let (pb, idb) = window[1];
            prop_assert!(
                pa >= pb,
                "bid fills not in price order: {}@{} before {}@{}",
                ida.0, pa.get(), idb.0, pb.get()
            );
            if pa == pb {
                prop_assert!(
                    ida.0 < idb.0,
                    "bid fills not in time order at price {}: order {} before {}",
                    pa.get(), ida.0, idb.0
                );
            }
        }
    }
}

// ===========================================================================
// 5. Deterministic Replay
// ===========================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    /// Running the same action sequence twice must produce identical final state.
    /// This is the foundation of event sourcing: deterministic replay guarantees
    /// that journal replay reconstructs the exact same exchange.
    #[test]
    fn deterministic_replay(actions in arb_exchange_actions()) {
        let balances_of = |exchange: &Exchange| -> Vec<(AccountId, CurrencyId, u64, u64)> {
            let mut result = Vec::new();
            for &acct in &[ACCT_A, ACCT_B] {
                for &cur in &[BTC, USD] {
                    let b = exchange.accounts().balance(acct, cur);
                    result.push((acct, cur, b.available, b.reserved));
                }
            }
            result
        };

        let (exchange1, _, _, _) = run_exchange_actions(&actions);
        let (exchange2, _, _, _) = run_exchange_actions(&actions);

        prop_assert_eq!(
            balances_of(&exchange1),
            balances_of(&exchange2),
            "two runs of the same actions produced different final state"
        );
    }
}

// ===========================================================================
// 6. Overflow Safety
// ===========================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1000))]

    /// No panic when price × quantity approaches or exceeds u64::MAX.
    /// The engine must handle large values gracefully (reject or saturate).
    #[test]
    fn overflow_reserve_safety(
        price_val in 1u64..=u64::MAX,
        qty_val in 1u64..=u64::MAX,
    ) {
        let mut mgr = AccountManager::new();
        let spec = btc_usd_spec();

        mgr.deposit(ACCT_A, USD, u64::MAX);
        mgr.deposit(ACCT_A, BTC, u64::MAX);

        let p = Price(NonZeroU64::new(price_val).unwrap());
        let q = Quantity(NonZeroU64::new(qty_val).unwrap());

        // Buy limit: may overflow price × qty. Must not panic.
        let buy = Order {
            id: OrderId(1),
            account: ACCT_A,
            side: Side::Buy,
            order_type: OrderType::Limit { price: p, post_only: false },
            time_in_force: TimeInForce::GTC,
            quantity: q,
            stp: SelfTradeProtection::Allow,
            expiry_ns: 0,
        };
        let _ = mgr.try_reserve(&buy, &spec, 0);

        // Sell limit: reserves quantity in base, no multiplication.
        let sell = Order {
            id: OrderId(2),
            account: ACCT_A,
            side: Side::Sell,
            order_type: OrderType::Limit { price: p, post_only: false },
            time_in_force: TimeInForce::GTC,
            quantity: q,
            stp: SelfTradeProtection::Allow,
            expiry_ns: 0,
        };
        let _ = mgr.try_reserve(&sell, &spec, 0);
    }

    /// Fill operations with large values must not panic.
    #[test]
    fn overflow_fill_safety(
        price_val in 1u64..=u64::MAX,
        qty_val in 1u64..=u64::MAX,
    ) {
        let mut mgr = AccountManager::new();
        let spec = btc_usd_spec();

        mgr.deposit(ACCT_A, USD, u64::MAX);
        mgr.deposit(ACCT_B, BTC, u64::MAX);

        let p = Price(NonZeroU64::new(price_val).unwrap());
        let q = Quantity(NonZeroU64::new(qty_val).unwrap());

        let buy = Order {
            id: OrderId(1),
            account: ACCT_A,
            side: Side::Buy,
            order_type: OrderType::Limit { price: p, post_only: false },
            time_in_force: TimeInForce::GTC,
            quantity: q,
            stp: SelfTradeProtection::Allow,
            expiry_ns: 0,
        };
        let sell = Order {
            id: OrderId(2),
            account: ACCT_B,
            side: Side::Sell,
            order_type: OrderType::Limit { price: p, post_only: false },
            time_in_force: TimeInForce::GTC,
            quantity: q,
            stp: SelfTradeProtection::Allow,
            expiry_ns: 0,
        };

        let buy_res = mgr.try_reserve(&buy, &spec, 0);
        let sell_res = mgr.try_reserve(&sell, &spec, 0);

        if let (Ok((_amt, buy_slot)), Ok((_amt2, sell_slot))) = (buy_res, sell_res) {
            // Fill must not panic regardless of price × quantity magnitude.
            // Buyer = ACCT_A (buy_slot), Seller = ACCT_B (sell_slot).
            mgr.fill(buy_slot, sell_slot, p, q, 0, 0, &spec);
        }
    }
}

// ===========================================================================
// 7. Reservation ↔ Book Consistency
// ===========================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    /// Every resting order and pending stop on the book must have a
    /// corresponding reservation in the account manager. Conversely,
    /// no reservation should exist for an order that is not on the book.
    /// Violations mean leaked capital (orphan reservation) or double-spend
    /// risk (missing reservation).
    #[test]
    fn reservation_matches_book(actions in arb_exchange_actions()) {
        let (exchange, _, _, _) = run_exchange_actions(&actions);

        let reservations = exchange.snapshot_reservations();
        // Key by (AccountId, OrderId) — two accounts may share the same OrderId.
        let reserved_order_ids: std::collections::HashSet<(AccountId, OrderId)> =
            reservations.iter().map(|(id, acct, _, _)| (*acct, *id)).collect();

        // Collect all (account, order_id) pairs on the book (resting + pending stops).
        let mut book_order_ids = std::collections::HashSet::new();
        for (_sym, book) in exchange.books() {
            for (_price, level) in book.bids().levels_snapshot() {
                for order in &level {
                    book_order_ids.insert((order.account(), order.id()));
                }
            }
            for (_price, level) in book.asks().levels_snapshot() {
                for order in &level {
                    book_order_ids.insert((order.account(), order.id()));
                }
            }
            book.stop_buys().for_each_stop(|stop| {
                    book_order_ids.insert((stop.account(), stop.id()));
        });
            book.stop_sells().for_each_stop(|stop| {
                    book_order_ids.insert((stop.account(), stop.id()));
        });
        }

        // Every book order must have a reservation.
        for &(acct, id) in &book_order_ids {
            prop_assert!(
                reserved_order_ids.contains(&(acct, id)),
                "order ({:?}, {}) is on the book but has no reservation",
                acct, id.0
            );
        }

        // Every reservation must correspond to a book order.
        for &(acct, id) in &reserved_order_ids {
            prop_assert!(
                book_order_ids.contains(&(acct, id)),
                "reservation exists for ({:?}, {}) but it is not on the book",
                acct, id.0
            );
        }
    }
}

// ===========================================================================
// 8. Order-Side Map Consistency
// ===========================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    /// The `order_sides` map must have exactly one entry per resting order
    /// and pending stop. Stale entries cause unbounded HashMap growth
    /// (memory leak on the hot path); missing entries cause fill processing
    /// to skip balance updates.
    #[test]
    fn order_sides_matches_book(actions in arb_exchange_actions()) {
        let (exchange, _, _, _) = run_exchange_actions(&actions);

        let order_sides = exchange.snapshot_order_sides();
        // Key by (AccountId, OrderId) — two accounts may share the same OrderId.
        let sides_ids: std::collections::HashSet<(AccountId, OrderId)> =
            order_sides.iter().map(|((acct, id), _)| (*acct, *id)).collect();

        let mut book_order_ids = std::collections::HashSet::new();
        for (_sym, book) in exchange.books() {
            for (_price, level) in book.bids().levels_snapshot() {
                for order in &level {
                    book_order_ids.insert((order.account(), order.id()));
                }
            }
            for (_price, level) in book.asks().levels_snapshot() {
                for order in &level {
                    book_order_ids.insert((order.account(), order.id()));
                }
            }
            book.stop_buys().for_each_stop(|stop| {
                    book_order_ids.insert((stop.account(), stop.id()));
        });
            book.stop_sells().for_each_stop(|stop| {
                    book_order_ids.insert((stop.account(), stop.id()));
        });
        }

        prop_assert_eq!(
            sides_ids, book_order_ids,
            "order_sides map does not match book contents"
        );
    }
}

// (No Balance Underflow test — removed; identical to balance_conservation above.)

// ===========================================================================
// 9. No Self-Trades Under STP
// ===========================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    /// When self-trade prevention is active (any mode except Allow) on the
    /// taker, no Fill should have maker_account == taker_account. We track
    /// the STP mode for each order ID and verify this across all reports.
    #[test]
    fn no_self_trades_under_stp(actions in arb_exchange_actions()) {
        let (_, order_ids, all_reports, _) = run_exchange_actions(&actions);

        // Build a map from (AccountId, OrderId) → STP mode.
        let mut stp_map: HashMap<(AccountId, OrderId), SelfTradeProtection> = HashMap::new();
        let mut id_idx = 0usize;
        for action in &actions {
            match action {
                ExchangeAction::Limit { stp, .. }
                | ExchangeAction::Market { stp, .. }
                | ExchangeAction::Stop { stp, .. }
                | ExchangeAction::StopLimit { stp, .. } => {
                    if let Some((id, account)) = order_ids[id_idx] {
                        stp_map.insert((account, id), *stp);
                    }
                    id_idx += 1;
                }
                _ => {
                    id_idx += 1;
                }
            }
        }

        for report in &all_reports {
            if let ExecutionReport::Fill {
                maker_account,
                taker_account,
                maker_order_id,
                taker_order_id,
                ..
            } = report
            {
                // Two different accounts may share the same OrderId, so only
                // assert inequality when they're the same account.
                if maker_account == taker_account {
                    prop_assert_ne!(
                        maker_order_id, taker_order_id,
                        "self-fill has maker_order_id == taker_order_id on {:?}: {:?}",
                        maker_account, report
                    );

                    // The taker's STP must have been Allow for a self-trade to occur.
                    let taker_stp = stp_map
                        .get(&(*taker_account, *taker_order_id))
                        .copied()
                        .unwrap_or(SelfTradeProtection::Allow);
                    prop_assert_eq!(
                        taker_stp,
                        SelfTradeProtection::Allow,
                        "self-trade fill between same account {:?} but taker {:?} had STP={:?}",
                        maker_account, taker_order_id, taker_stp
                    );
                }
            }
        }
    }
}

// ===========================================================================
// 10. Withdrawal invariants
// ===========================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    /// After any sequence of deposits, orders, cancels, and withdrawals:
    ///
    /// 1. No funds are created from thin air: total in system <= total deposited.
    /// 2. No account has negative available or reserved funds (enforced by u64).
    /// 3. WithdrawAll followed by no further deposits for that account
    ///    leaves zero balances.
    #[test]
    fn withdrawal_preserves_invariants(actions in arb_exchange_actions()) {
        let (exchange, _ids, _reports, _withdrawn) = run_exchange_actions(&actions);

        // If the last action for an account is WithdrawAll (no subsequent
        // deposits), that account must have zero balances.
        for &account in &[ACCT_A, ACCT_B] {
            let last_withdraw_idx = actions.iter().rposition(|a| {
                matches!(a, ExchangeAction::WithdrawAll { account: a } if *a == account)
            });
            let last_deposit_idx = actions.iter().rposition(|a| {
                matches!(a, ExchangeAction::Deposit { account: a, .. } if *a == account)
            });
            // If there's a WithdrawAll and no deposit after it:
            if let Some(w) = last_withdraw_idx {
                let has_later_deposit = last_deposit_idx.is_some_and(|d| d > w);
                if !has_later_deposit {
                    for &currency in &[BTC, USD] {
                        let bal = exchange.accounts().balance(account, currency);
                        prop_assert_eq!(
                            bal.available, 0,
                            "account {:?} has available {} in {:?} after final WithdrawAll",
                            account, bal.available, currency
                        );
                        prop_assert_eq!(
                            bal.reserved, 0,
                            "account {:?} has reserved {} in {:?} after final WithdrawAll",
                            account, bal.reserved, currency
                        );
                    }
                }
            }
        }
    }
}
