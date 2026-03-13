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

fn arb_tif() -> impl Strategy<Value = TimeInForce> {
    prop_oneof![
        Just(TimeInForce::GTC),
        Just(TimeInForce::IOC),
        Just(TimeInForce::FOK),
    ]
}

fn arb_account() -> impl Strategy<Value = AccountId> {
    prop_oneof![Just(ACCT_A), Just(ACCT_B)]
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
    },
    Market {
        side: Side,
        quantity: Quantity,
    },
    Cancel {
        /// Index into the action list to pick which order to cancel.
        target_idx: usize,
    },
}

fn arb_book_action() -> impl Strategy<Value = BookAction> {
    prop_oneof![
        4 => (arb_side(), arb_price(), arb_quantity(), arb_tif()).prop_map(
            |(side, price, quantity, tif)| BookAction::Limit { side, price, quantity, tif }
        ),
        2 => (arb_side(), arb_quantity()).prop_map(
            |(side, quantity)| BookAction::Market { side, quantity }
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
    },
    Market {
        account: AccountId,
        side: Side,
        quantity: Quantity,
    },
    Cancel {
        target_idx: usize,
    },
}

fn arb_exchange_action() -> impl Strategy<Value = ExchangeAction> {
    prop_oneof![
        1 => (arb_account(), prop_oneof![Just(BTC), Just(USD)], 1u64..=100_000)
            .prop_map(|(account, currency, amount)| ExchangeAction::Deposit {
                account, currency, amount,
            }),
        4 => (arb_account(), arb_side(), arb_price(), arb_quantity(), arb_tif())
            .prop_map(|(account, side, price, quantity, tif)| ExchangeAction::Limit {
                account, side, price, quantity, tif,
            }),
        2 => (arb_account(), arb_side(), arb_quantity())
            .prop_map(|(account, side, quantity)| ExchangeAction::Market {
                account, side, quantity,
            }),
        1 => (0usize..200).prop_map(|target_idx| ExchangeAction::Cancel { target_idx }),
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
            } => {
                let id = OrderId(next_id);
                next_id += 1;
                action_order_ids.push(Some(id));
                let order = Order {
                    id,
                    account: ACCT_A,
                    side: *side,
                    order_type: OrderType::Limit { price: *price },
                    time_in_force: *tif,
                    quantity: *quantity,
                    stp: SelfTradeProtection::Allow,
                };
                book.execute(order, None, &mut reports);
            }
            BookAction::Market { side, quantity } => {
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
                    stp: SelfTradeProtection::Allow,
                };
                book.execute(order, None, &mut reports);
            }
            BookAction::Cancel { target_idx } => {
                action_order_ids.push(None);
                if *target_idx < action_idx {
                    if let Some(id) = action_order_ids[*target_idx] {
                        book.cancel(id, &mut reports);
                    }
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
            BookAction::Limit { quantity, .. } | BookAction::Market { quantity, .. } => {
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
    for (_price, level) in book.bids().levels_iter() {
        for order in level {
            total += order.remaining().get();
        }
    }
    for (_price, level) in book.asks().levels_iter() {
        for order in level {
            total += order.remaining().get();
        }
    }
    for (_price, stops) in book.stop_buys() {
        for stop in stops {
            total += stop.quantity().get();
        }
    }
    for (_price, stops) in book.stop_sells() {
        for stop in stops {
            total += stop.quantity().get();
        }
    }
    total
}

/// Run a sequence of ExchangeActions and return final exchange state.
fn run_exchange_actions(actions: &[ExchangeAction]) -> (Exchange, Vec<Option<OrderId>>) {
    let mut exchange = Exchange::new();
    exchange.add_instrument(btc_usd_spec());
    let mut reports = Vec::new();
    let mut next_id = 1u64;
    let mut action_order_ids: Vec<Option<OrderId>> = Vec::new();
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
            } => {
                let id = OrderId(next_id);
                next_id += 1;
                action_order_ids.push(Some(id));
                let order = Order {
                    id,
                    account: *account,
                    side: *side,
                    order_type: OrderType::Limit { price: *price },
                    time_in_force: *tif,
                    quantity: *quantity,
                    stp: SelfTradeProtection::Allow,
                };
                exchange.execute(sym, order, &mut reports);
                reports.clear();
            }
            ExchangeAction::Market {
                account,
                side,
                quantity,
            } => {
                let id = OrderId(next_id);
                next_id += 1;
                action_order_ids.push(Some(id));
                let order = Order {
                    id,
                    account: *account,
                    side: *side,
                    order_type: OrderType::Market,
                    time_in_force: TimeInForce::IOC,
                    quantity: *quantity,
                    stp: SelfTradeProtection::Allow,
                };
                exchange.execute(sym, order, &mut reports);
                reports.clear();
            }
            ExchangeAction::Cancel { target_idx } => {
                action_order_ids.push(None);
                if *target_idx < action_idx {
                    if let Some(id) = action_order_ids[*target_idx] {
                        exchange.cancel(sym, id, &mut reports);
                        reports.clear();
                    }
                }
            }
        }
    }
    (exchange, action_order_ids)
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
        let mut book = OrderBook::new();
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
                ExecutionReport::Placed { .. } | ExecutionReport::Triggered { .. } => {}
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
        let mut book = OrderBook::new();
        let _ = run_book_actions(&mut book, &actions);

        // --- Resting orders ---
        let index = book.snapshot_order_index();
        let mut ids_from_book = std::collections::HashSet::new();

        for (_price, level) in book.bids().levels_iter() {
            for order in level {
                ids_from_book.insert(order.id());
            }
        }
        for (_price, level) in book.asks().levels_iter() {
            for order in level {
                ids_from_book.insert(order.id());
            }
        }

        let ids_from_index: std::collections::HashSet<OrderId> =
            index.iter().map(|&(id, _, _)| id).collect();

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
        for (_price, level) in book.bids().levels_iter() {
            prop_assert!(!level.is_empty(), "empty bid level should have been removed");
        }
        for (_price, level) in book.asks().levels_iter() {
            prop_assert!(!level.is_empty(), "empty ask level should have been removed");
        }

        // --- Stop orders ---
        let stop_idx = book.snapshot_stop_index();
        let mut stop_ids_from_book = std::collections::HashSet::new();

        for (_price, stops) in book.stop_buys() {
            for stop in stops {
                stop_ids_from_book.insert(stop.id());
            }
        }
        for (_price, stops) in book.stop_sells() {
            for stop in stops {
                stop_ids_from_book.insert(stop.id());
            }
        }

        let stop_ids_from_index: std::collections::HashSet<OrderId> =
            stop_idx.iter().map(|&(id, _, _)| id).collect();

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
    /// all accounts and currencies only changes via explicit deposits. Orders,
    /// fills, and cancels transfer value between available/reserved or between
    /// accounts, but never create or destroy value.
    #[test]
    fn balance_conservation(actions in arb_exchange_actions()) {
        let (exchange, _) = run_exchange_actions(&actions);

        // Track total deposited per currency.
        let mut total_deposited_btc: u128 = 0;
        let mut total_deposited_usd: u128 = 0;

        for action in &actions {
            if let ExchangeAction::Deposit {
                currency, amount, ..
            } = action
            {
                if *currency == BTC {
                    total_deposited_btc += *amount as u128;
                } else {
                    total_deposited_usd += *amount as u128;
                }
            }
        }

        let accounts = exchange.accounts();
        let system_btc: u128 = [ACCT_A, ACCT_B]
            .iter()
            .map(|a| {
                let b = accounts.balance(*a, BTC);
                b.available as u128 + b.reserved as u128
            })
            .sum();
        let system_usd: u128 = [ACCT_A, ACCT_B]
            .iter()
            .map(|a| {
                let b = accounts.balance(*a, USD);
                b.available as u128 + b.reserved as u128
            })
            .sum();

        // Skip check when deposits would saturate u64 (saturating_add clips
        // individual balances, so the system total diverges from deposited).
        if total_deposited_btc <= u64::MAX as u128 && total_deposited_usd <= u64::MAX as u128 {
            prop_assert_eq!(
                system_btc, total_deposited_btc,
                "BTC conservation violated: system={} != deposited={}",
                system_btc, total_deposited_btc
            );
            prop_assert_eq!(
                system_usd, total_deposited_usd,
                "USD conservation violated: system={} != deposited={}",
                system_usd, total_deposited_usd
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
        let mut book = OrderBook::new();
        let mut reports = Vec::new();

        for (i, (p, q)) in orders.iter().enumerate() {
            let order = Order {
                id: OrderId(i as u64 + 1),
                account: ACCT_A,
                side: Side::Sell,
                order_type: OrderType::Limit { price: *p },
                time_in_force: TimeInForce::GTC,
                quantity: *q,
                stp: SelfTradeProtection::Allow,
            };
            book.execute(order, None, &mut reports);
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
        };
        book.execute(market, None, &mut reports);

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
        let mut book = OrderBook::new();
        let mut reports = Vec::new();

        for (i, (p, q)) in orders.iter().enumerate() {
            let order = Order {
                id: OrderId(i as u64 + 1),
                account: ACCT_A,
                side: Side::Buy,
                order_type: OrderType::Limit { price: *p },
                time_in_force: TimeInForce::GTC,
                quantity: *q,
                stp: SelfTradeProtection::Allow,
            };
            book.execute(order, None, &mut reports);
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
        };
        book.execute(market, None, &mut reports);

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

        let (exchange1, _) = run_exchange_actions(&actions);
        let (exchange2, _) = run_exchange_actions(&actions);

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
            order_type: OrderType::Limit { price: p },
            time_in_force: TimeInForce::GTC,
            quantity: q,
            stp: SelfTradeProtection::Allow,
        };
        let _ = mgr.try_reserve(&buy, &spec);

        // Sell limit: reserves quantity in base, no multiplication.
        let sell = Order {
            id: OrderId(2),
            account: ACCT_A,
            side: Side::Sell,
            order_type: OrderType::Limit { price: p },
            time_in_force: TimeInForce::GTC,
            quantity: q,
            stp: SelfTradeProtection::Allow,
        };
        let _ = mgr.try_reserve(&sell, &spec);
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
            order_type: OrderType::Limit { price: p },
            time_in_force: TimeInForce::GTC,
            quantity: q,
            stp: SelfTradeProtection::Allow,
        };
        let sell = Order {
            id: OrderId(2),
            account: ACCT_B,
            side: Side::Sell,
            order_type: OrderType::Limit { price: p },
            time_in_force: TimeInForce::GTC,
            quantity: q,
            stp: SelfTradeProtection::Allow,
        };

        let buy_ok = mgr.try_reserve(&buy, &spec).is_ok();
        let sell_ok = mgr.try_reserve(&sell, &spec).is_ok();

        if buy_ok && sell_ok {
            // Fill must not panic regardless of price × quantity magnitude.
            mgr.fill(OrderId(2), OrderId(1), p, q, Side::Sell, &spec);
        }
    }
}
