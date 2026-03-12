//! Exchange: dispatches orders to per-instrument order books.
//!
//! All order books run on a single thread (LMAX-style). This keeps event
//! ordering deterministic and allows portfolio-wide risk checks (margin,
//! exposure limits) without cross-thread coordination.
//!
//! If throughput exceeds a single core, shard by instrument — each shard
//! stays single-threaded. Note: portfolio risk checks then require
//! cross-shard message passing, adding latency and complexity.

use std::collections::HashMap;

use crate::account::AccountManager;
use crate::orderbook::OrderBook;
use crate::types::{
    AccountId, CurrencyId, ExecutionReport, InstrumentSpec, Order, OrderId, RejectReason, Side,
    Symbol,
};

/// Top-level exchange managing multiple instruments.
pub struct Exchange {
    /// HashMap for symbol → order book dispatch. O(1) amortized lookup.
    // TODO: If profiling shows hashing overhead on the hot path, consider
    // replacing with a pre-allocated `OrderBook` array indexed by
    // Symbol(u32), giving true O(1) dispatch with no hashing.
    books: HashMap<Symbol, OrderBook>,
    /// Instrument specifications mapping symbols to their currency pairs.
    instruments: HashMap<Symbol, InstrumentSpec>,
    /// Shared account balance manager across all instruments.
    accounts: AccountManager,
    /// Tracks order side by ID so fills can determine buyer/seller.
    /// Populated on order submission, cleaned up on full fill or cancel.
    order_sides: HashMap<OrderId, Side>,
}

impl Exchange {
    pub fn new() -> Self {
        Self {
            books: HashMap::new(),
            instruments: HashMap::new(),
            accounts: AccountManager::new(),
            order_sides: HashMap::new(),
        }
    }

    /// Reconstruct from pre-built parts (used by snapshot restore).
    pub(crate) fn from_parts(
        books: HashMap<Symbol, OrderBook>,
        instruments: HashMap<Symbol, InstrumentSpec>,
        accounts: AccountManager,
        order_sides: HashMap<OrderId, Side>,
    ) -> Self {
        Self {
            books,
            instruments,
            accounts,
            order_sides,
        }
    }

    /// Access instrument specifications (for snapshot serialization).
    pub(crate) fn instruments(&self) -> &HashMap<Symbol, InstrumentSpec> {
        &self.instruments
    }

    /// Access order books (for snapshot serialization).
    pub(crate) fn books(&self) -> &HashMap<Symbol, OrderBook> {
        &self.books
    }

    /// Snapshot the order-side map as a Vec for serialization.
    pub(crate) fn snapshot_order_sides(&self) -> Vec<(OrderId, Side)> {
        self.order_sides
            .iter()
            .map(|(&id, &side)| (id, side))
            .collect()
    }

    /// Register a new instrument with its currency pair specification.
    pub fn add_instrument(&mut self, spec: InstrumentSpec) {
        self.books.entry(spec.symbol).or_default();
        self.instruments.insert(spec.symbol, spec);
    }

    /// Deposit funds into an account.
    pub fn deposit(&mut self, account: AccountId, currency: CurrencyId, amount: u64) {
        self.accounts.deposit(account, currency, amount);
    }

    /// Get the account manager (for balance queries).
    pub fn accounts(&self) -> &AccountManager {
        &self.accounts
    }

    /// Submit an order to the matching engine for the given instrument.
    ///
    /// Validates the instrument exists, reserves funds, then executes.
    /// On fill, balances are updated. On reject/cancel, reserves are released.
    pub fn execute(&mut self, symbol: Symbol, order: Order, reports: &mut Vec<ExecutionReport>) {
        let Some(spec) = self.instruments.get(&symbol).copied() else {
            reports.push(ExecutionReport::Rejected {
                order_id: order.id,
                reason: RejectReason::UnknownSymbol,
            });
            return;
        };

        // Reserve funds before submitting to the matching engine.
        if let Err(reason) = self.accounts.try_reserve(&order, &spec) {
            reports.push(ExecutionReport::Rejected {
                order_id: order.id,
                reason,
            });
            return;
        }

        // Track the order's side for fill processing.
        self.order_sides.insert(order.id, order.side);

        let report_start = reports.len();

        let book = self
            .books
            .get_mut(&symbol)
            .expect("book exists because instrument was added");
        book.execute(order, reports);

        // Process reports to update balances.
        let new_reports = &reports[report_start..];
        self.accounts
            .process_reports(new_reports, &self.order_sides, &spec);

        // Clean up order_sides for fully consumed orders.
        for report in &reports[report_start..] {
            match report {
                ExecutionReport::Cancelled { order_id, .. }
                | ExecutionReport::Rejected { order_id, .. } => {
                    self.order_sides.remove(order_id);
                }
                _ => {}
            }
        }
    }

    /// Cancel a resting order on the given instrument.
    pub fn cancel(
        &mut self,
        symbol: Symbol,
        order_id: OrderId,
        reports: &mut Vec<ExecutionReport>,
    ) {
        let Some(spec) = self.instruments.get(&symbol).copied() else {
            return;
        };

        let report_start = reports.len();

        let Some(book) = self.books.get_mut(&symbol) else {
            return;
        };
        book.cancel(order_id, reports);

        // Release reserved funds if cancellation succeeded.
        let new_reports = &reports[report_start..];
        self.accounts
            .process_reports(new_reports, &self.order_sides, &spec);

        // Clean up order_sides.
        for report in &reports[report_start..] {
            if let ExecutionReport::Cancelled { order_id, .. } = report {
                self.order_sides.remove(order_id);
            }
        }
    }
}

impl Default for Exchange {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::*;
    use crate::types::{OrderType, Price, Quantity, TimeInForce};

    const ACCT_A: AccountId = AccountId(1);
    const ACCT_B: AccountId = AccountId(2);
    const BTC: CurrencyId = CurrencyId(0);
    const USD: CurrencyId = CurrencyId(1);
    const ETH: CurrencyId = CurrencyId(2);

    fn btc_usd_spec() -> InstrumentSpec {
        InstrumentSpec {
            symbol: Symbol(1),
            base: BTC,
            quote: USD,
        }
    }

    fn eth_usd_spec() -> InstrumentSpec {
        InstrumentSpec {
            symbol: Symbol(2),
            base: ETH,
            quote: USD,
        }
    }

    fn qty(n: u64) -> Quantity {
        Quantity(NonZeroU64::new(n).unwrap())
    }

    fn price(n: u64) -> Price {
        Price(NonZeroU64::new(n).unwrap())
    }

    fn limit_order(
        id: u64,
        account: AccountId,
        side: Side,
        p: u64,
        q: u64,
        tif: TimeInForce,
    ) -> Order {
        Order {
            id: OrderId(id),
            account,
            side,
            order_type: OrderType::Limit { price: price(p) },
            time_in_force: tif,
            quantity: qty(q),
        }
    }

    fn market_order(id: u64, account: AccountId, side: Side, q: u64) -> Order {
        Order {
            id: OrderId(id),
            account,
            side,
            order_type: OrderType::Market,
            time_in_force: TimeInForce::IOC,
            quantity: qty(q),
        }
    }

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

        exchange.cancel(btc, OrderId(1), &mut reports);
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
        exchange.cancel(btc, OrderId(2), &mut reports);

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
}
