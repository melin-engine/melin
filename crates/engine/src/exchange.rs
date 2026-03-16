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
    AccountId, CircuitBreakerConfig, CurrencyId, ExecutionReport, InstrumentSpec, Order, OrderId,
    OrderType, RejectReason, RiskLimits, Side, Symbol,
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
    /// Per-instrument fat finger limits. Checked in `execute()` before
    /// balance reservation. HashMap for O(1) lookup by Symbol(u32).
    risk_limits: HashMap<Symbol, RiskLimits>,
    /// Per-instrument circuit breaker configuration. Checked in `execute()`
    /// after dedup and before fat finger checks. HashMap for O(1) lookup.
    circuit_breakers: HashMap<Symbol, CircuitBreakerConfig>,
    /// Per-account high-water mark for order IDs. Rejects submissions
    /// with `order_id <= max_seen[account]` to prevent duplicate execution
    /// on crash-recovery retry. HashMap for O(1) lookup keyed on
    /// AccountId(u32) — cheap single-word hash.
    max_order_id: HashMap<AccountId, u64>,
    /// Reusable buffer for consumed order IDs from `process_reports()`.
    /// Avoids per-order Vec allocation on the hot path.
    consumed_buf: Vec<OrderId>,
    /// When true, new order books are created with generous pre-allocation
    /// to avoid HashMap resize spikes on the hot path.
    presized: bool,
}

impl Exchange {
    pub fn new() -> Self {
        Self {
            books: HashMap::new(),
            instruments: HashMap::new(),
            accounts: AccountManager::new(),
            order_sides: HashMap::new(),
            risk_limits: HashMap::new(),
            circuit_breakers: HashMap::new(),
            max_order_id: HashMap::new(),
            consumed_buf: Vec::new(),
            presized: false,
        }
    }

    /// Create an Exchange pre-sized for production workloads. Avoids
    /// HashMap resize spikes on the hot path by allocating upfront.
    /// RAM is cheap; tail latency is not.
    pub fn with_capacity() -> Self {
        Self {
            books: HashMap::with_capacity(64),
            instruments: HashMap::with_capacity(64),
            accounts: AccountManager::with_capacity(),
            order_sides: HashMap::with_capacity(2_000_000),
            risk_limits: HashMap::with_capacity(64),
            circuit_breakers: HashMap::with_capacity(64),
            max_order_id: HashMap::with_capacity(10_000),
            consumed_buf: Vec::with_capacity(256),
            presized: true,
        }
    }

    /// Reconstruct from pre-built parts (used by snapshot restore).
    pub(crate) fn from_parts(
        books: HashMap<Symbol, OrderBook>,
        instruments: HashMap<Symbol, InstrumentSpec>,
        accounts: AccountManager,
        order_sides: HashMap<OrderId, Side>,
        risk_limits: HashMap<Symbol, RiskLimits>,
        circuit_breakers: HashMap<Symbol, CircuitBreakerConfig>,
        max_order_id: HashMap<AccountId, u64>,
    ) -> Self {
        Self {
            books,
            instruments,
            accounts,
            order_sides,
            risk_limits,
            circuit_breakers,
            max_order_id,
            consumed_buf: Vec::new(),
            presized: false,
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

    /// Snapshot the per-account order ID high-water marks for serialization.
    pub(crate) fn snapshot_max_order_id(&self) -> Vec<(AccountId, u64)> {
        self.max_order_id
            .iter()
            .map(|(&account, &hwm)| (account, hwm))
            .collect()
    }

    /// Set fat finger risk limits for an instrument.
    pub fn set_risk_limits(&mut self, symbol: Symbol, limits: RiskLimits) {
        self.risk_limits.insert(symbol, limits);
    }

    /// Snapshot the per-instrument risk limits for serialization.
    pub(crate) fn snapshot_risk_limits(&self) -> Vec<(Symbol, RiskLimits)> {
        self.risk_limits
            .iter()
            .map(|(&symbol, &limits)| (symbol, limits))
            .collect()
    }

    /// Set circuit breaker configuration for an instrument.
    pub fn set_circuit_breaker(&mut self, symbol: Symbol, config: CircuitBreakerConfig) {
        self.circuit_breakers.insert(symbol, config);
    }

    /// Snapshot the per-instrument circuit breaker configs for serialization.
    pub(crate) fn snapshot_circuit_breakers(&self) -> Vec<(Symbol, CircuitBreakerConfig)> {
        self.circuit_breakers
            .iter()
            .map(|(&symbol, &config)| (symbol, config))
            .collect()
    }

    /// Touch all pre-allocated HashMap pages so page faults happen at startup,
    /// not on the hot path. Call once after adding instruments, before accepting
    /// orders. Skips maps that already contain data — their pages are already
    /// faulted from the insertions that populated them.
    pub fn prefault(&mut self) {
        if self.order_sides.is_empty() {
            let cap = self.order_sides.capacity();
            for i in 0..cap {
                self.order_sides.insert(OrderId(i as u64), Side::Buy);
            }
            self.order_sides.clear();
        }

        if self.max_order_id.is_empty() {
            let max_oid_cap = self.max_order_id.capacity();
            for i in 0..max_oid_cap {
                self.max_order_id.insert(AccountId(i as u32), 0);
            }
            self.max_order_id.clear();
        }

        self.accounts.prefault();

        for book in self.books.values_mut() {
            book.prefault();
        }
    }

    /// Register a new instrument with its currency pair specification.
    pub fn add_instrument(&mut self, spec: InstrumentSpec) {
        let presized = self.presized;
        self.books.entry(spec.symbol).or_insert_with(|| {
            if presized {
                OrderBook::with_capacity()
            } else {
                OrderBook::new()
            }
        });
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

        // Dedup: reject if this account already submitted an order with
        // the same or higher ID. Prevents duplicate execution on
        // crash-recovery replay. The HWM advances unconditionally because
        // the journal records every SubmitOrder regardless of matching
        // outcome — a replayed InsufficientBalance rejection is harmless,
        // but a replayed fill is not. Clients must use a new OrderId for
        // genuinely new orders, even if the previous one was rejected.
        let hwm = self.max_order_id.entry(order.account).or_insert(0);
        if order.id.0 <= *hwm {
            reports.push(ExecutionReport::Rejected {
                order_id: order.id,
                reason: RejectReason::DuplicateOrderId,
            });
            return;
        }
        *hwm = order.id.0;

        // Circuit breaker checks: trading halt rejects all orders; price
        // bands reject limit/stop-limit orders outside [lower, upper].
        // Single bool check (~1ns) + HashMap lookup + 2 comparisons (~3-5ns).
        if let Some(cb) = self.circuit_breakers.get(&symbol) {
            if cb.halted {
                reports.push(ExecutionReport::Rejected {
                    order_id: order.id,
                    reason: RejectReason::TradingHalted,
                });
                return;
            }
            // Price band check applies only to orders with a known price.
            // Market and Stop orders have no submission-time price.
            let limit_price = match order.order_type {
                OrderType::Limit { price } => Some(price),
                OrderType::StopLimit { limit_price, .. } => Some(limit_price),
                OrderType::Market | OrderType::Stop { .. } => None,
            };
            if let Some(price) = limit_price {
                if let Some(lower) = cb.price_band_lower {
                    if price < lower {
                        reports.push(ExecutionReport::Rejected {
                            order_id: order.id,
                            reason: RejectReason::OutsidePriceBand,
                        });
                        return;
                    }
                }
                if let Some(upper) = cb.price_band_upper {
                    if price > upper {
                        reports.push(ExecutionReport::Rejected {
                            order_id: order.id,
                            reason: RejectReason::OutsidePriceBand,
                        });
                        return;
                    }
                }
            }
        }

        // Fat finger checks: reject orders exceeding per-instrument limits.
        if let Some(limits) = self.risk_limits.get(&symbol) {
            if let Some(max_qty) = limits.max_order_qty
                && order.quantity.get() > max_qty.get()
            {
                reports.push(ExecutionReport::Rejected {
                    order_id: order.id,
                    reason: RejectReason::ExceedsMaxOrderQty,
                });
                return;
            }
            if let Some(max_notional) = limits.max_order_notional {
                // Notional check applies only to orders with a known price.
                // Market and Stop orders have no submission-time price.
                // StopLimit uses limit_price (worst-case resting price).
                let limit_price = match order.order_type {
                    OrderType::Limit { price } => Some(price),
                    OrderType::StopLimit { limit_price, .. } => Some(limit_price),
                    OrderType::Market | OrderType::Stop { .. } => None,
                };
                if let Some(price) = limit_price {
                    let notional = price.get() as u128 * order.quantity.get() as u128;
                    if notional > max_notional as u128 {
                        reports.push(ExecutionReport::Rejected {
                            order_id: order.id,
                            reason: RejectReason::ExceedsMaxNotional,
                        });
                        return;
                    }
                }
            }
        }

        // Reserve funds before submitting to the matching engine.
        let reserved = match self.accounts.try_reserve(&order, &spec) {
            Ok(amount) => amount,
            Err(reason) => {
                reports.push(ExecutionReport::Rejected {
                    order_id: order.id,
                    reason,
                });
                return;
            }
        };

        // For buy-side market/stop-market orders, pass the reserved amount as
        // a cost budget so the matching engine stops before exceeding it.
        // Limit and stop-limit buys don't need this — their cost is bounded
        // by price × quantity which matches the reservation exactly.
        let quote_budget = match (order.side, order.order_type) {
            (Side::Buy, OrderType::Market) | (Side::Buy, OrderType::Stop { .. }) => Some(reserved),
            _ => None,
        };

        // Track the order's side for fill processing.
        self.order_sides.insert(order.id, order.side);

        let report_start = reports.len();

        let book = self
            .books
            .get_mut(&symbol)
            .expect("book exists because instrument was added");
        book.execute(order, quote_budget, reports);

        // Process reports to update balances.
        let new_reports = &reports[report_start..];
        self.consumed_buf.clear();
        self.accounts.process_reports(
            new_reports,
            &self.order_sides,
            &spec,
            &mut self.consumed_buf,
        );

        // Buy-side orders may have leftover reservation after fills due to
        // price improvement: a limit buy at price 100 filling at price 80
        // reserved 100×qty but only spent 80×qty, leaving 20×qty in the
        // reservation. Market/stop buys reserve the entire available quote
        // balance, making this even more likely. The Fill handler in
        // process_reports only cleans up reservations when remaining == 0,
        // which won't happen with price improvement.
        //
        // Scan all order IDs from fill reports. If any has a leftover
        // reservation but is no longer on the book (fully filled), release
        // the unused portion. This also handles triggered stops whose fill
        // didn't exhaust their budget-based reservation.
        let book = self
            .books
            .get(&symbol)
            .expect("book exists because instrument was added");
        for report in &reports[report_start..] {
            if let ExecutionReport::Fill {
                maker_order_id,
                taker_order_id,
                ..
            } = report
            {
                for &id in &[*maker_order_id, *taker_order_id] {
                    if !self.consumed_buf.contains(&id)
                        && self.accounts.has_reservation(id)
                        && !book.has_order(id)
                        && !book.has_stop(id)
                    {
                        self.accounts.release(id);
                        self.consumed_buf.push(id);
                    }
                }
            }
        }

        // Clean up order_sides for fully consumed orders (filled, cancelled,
        // or rejected). Without this, order_sides leaks entries and triggers
        // increasingly expensive HashMap resizes on the hot path.
        for &order_id in &self.consumed_buf {
            self.order_sides.remove(&order_id);
        }
    }

    /// Cancel all resting orders and pending stops for an account across
    /// all instruments (kill switch). Releases all associated reservations.
    pub fn cancel_all(&mut self, account: AccountId, reports: &mut Vec<ExecutionReport>) {
        // Collect symbols first to avoid borrowing self.books while also
        // needing self.accounts and self.order_sides.
        let symbols: Vec<Symbol> = self.books.keys().copied().collect();

        for symbol in symbols {
            let Some(spec) = self.instruments.get(&symbol).copied() else {
                continue;
            };

            let report_start = reports.len();

            let Some(book) = self.books.get_mut(&symbol) else {
                continue;
            };
            book.cancel_all_for_account(account, reports);

            let new_reports = &reports[report_start..];
            self.consumed_buf.clear();
            self.accounts.process_reports(
                new_reports,
                &self.order_sides,
                &spec,
                &mut self.consumed_buf,
            );

            for &order_id in &self.consumed_buf {
                self.order_sides.remove(&order_id);
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
        self.consumed_buf.clear();
        self.accounts.process_reports(
            new_reports,
            &self.order_sides,
            &spec,
            &mut self.consumed_buf,
        );

        // Clean up order_sides for cancelled orders.
        for &order_id in &self.consumed_buf {
            self.order_sides.remove(&order_id);
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
    use crate::types::{OrderType, Price, Quantity, SelfTradeProtection, TimeInForce};

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
            stp: SelfTradeProtection::Allow,
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
            stp: SelfTradeProtection::Allow,
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
            order_type: OrderType::Limit { price: price(p) },
            time_in_force: tif,
            quantity: qty(q),
            stp,
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
            ExecutionReport::Cancelled { order_id: OrderId(3), remaining_quantity }
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
            ExecutionReport::Cancelled { order_id: OrderId(3), remaining_quantity }
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
            ExecutionReport::Cancelled { order_id: OrderId(3), remaining_quantity }
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
    fn lower_order_id_rejected() {
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
            limit_order(3, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
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
    fn rejected_order_consumes_id() {
        // Even if an order is rejected (e.g., InsufficientBalance), the
        // HWM advances because the journal already recorded the event.
        // A retry with the same ID is a duplicate. The client must use
        // a new OrderId for genuinely new orders.
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

        // Retry with the same ID — blocked by dedup even after depositing.
        exchange.deposit(ACCT_A, USD, 100_000);
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

        // A new, higher ID succeeds.
        reports.clear();
        exchange.execute(
            Symbol(1),
            limit_order(2, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
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
        exchange.cancel(Symbol(1), OrderId(100), &mut reports);
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
        exchange.cancel(Symbol(1), OrderId(1), &mut reports);
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
    fn halted_order_still_consumes_order_id() {
        // A halt-rejected order must consume its OrderId (HWM advances).
        // Otherwise replaying the journal after recovery could double-execute.
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

        // Unhalt and retry with the same OrderId — should be rejected as duplicate.
        exchange.set_circuit_breaker(Symbol(1), CircuitBreakerConfig::default());
        reports.clear();
        exchange.execute(
            Symbol(1),
            limit_order(5, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
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
    fn band_rejected_order_still_consumes_order_id() {
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

        // Widen bands and retry same OrderId — should be duplicate.
        exchange.set_circuit_breaker(Symbol(1), CircuitBreakerConfig::default());
        reports.clear();
        exchange.execute(
            Symbol(1),
            limit_order(5, ACCT_A, Side::Buy, 50, 10, TimeInForce::GTC),
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
}
