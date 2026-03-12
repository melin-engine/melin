//! Account balance management for the trading engine.
//!
//! Tracks per-account, per-currency balances. Reserves funds on order
//! placement, updates balances on fills, and releases reserves on
//! cancellation. Runs on the same single thread as the matching engine
//! (no locks needed).

use std::collections::HashMap;

use crate::types::{
    AccountId, CurrencyId, ExecutionReport, InstrumentSpec, Order, OrderId, OrderType, Price,
    Quantity, RejectReason, Side,
};

/// Per-currency balance for an account.
///
/// Split into `available` (free to use) and `reserved` (locked by open orders).
/// Uses `u64` to match the scale of `Price`/`Quantity`. Overflow-prone
/// calculations (price × quantity) use `u128` intermediates.
#[derive(Debug, Clone, Copy, Default)]
pub struct Balance {
    /// Funds available for new orders.
    pub available: u64,
    /// Funds locked by open orders (not yet filled or cancelled).
    pub reserved: u64,
}

impl Balance {
    /// Total balance (available + reserved).
    pub fn total(&self) -> u64 {
        self.available.saturating_add(self.reserved)
    }
}

/// Tracks what was reserved for a specific order so we can release on
/// cancel/fill without recomputing.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Reservation {
    account: AccountId,
    /// The currency that was reserved (quote for buys, base for sells).
    currency: CurrencyId,
    /// Remaining reserved amount. Decremented on each partial fill,
    /// fully released on cancel.
    remaining: u64,
}

impl Reservation {
    pub(crate) fn new(account: AccountId, currency: CurrencyId, remaining: u64) -> Self {
        Self {
            account,
            currency,
            remaining,
        }
    }

    pub(crate) fn account(&self) -> AccountId {
        self.account
    }

    pub(crate) fn currency(&self) -> CurrencyId {
        self.currency
    }

    pub(crate) fn remaining(&self) -> u64 {
        self.remaining
    }
}

/// Manages account balances across all currencies.
///
/// Uses `HashMap<(AccountId, CurrencyId), Balance>` — a flat composite key
/// avoids nested map lookups, keeping balance checks to a single hash
/// lookup on the hot path.
pub struct AccountManager {
    balances: HashMap<(AccountId, CurrencyId), Balance>,
    /// Maps each open order to its reservation details. O(1) lookup on
    /// fill/cancel so we don't need to recompute costs.
    reservations: HashMap<OrderId, Reservation>,
}

impl AccountManager {
    pub fn new() -> Self {
        Self {
            balances: HashMap::new(),
            reservations: HashMap::new(),
        }
    }

    /// Reconstruct from pre-built parts (used by snapshot restore).
    pub(crate) fn from_parts(
        balances: HashMap<(AccountId, CurrencyId), Balance>,
        reservations: HashMap<OrderId, Reservation>,
    ) -> Self {
        Self {
            balances,
            reservations,
        }
    }

    /// Iterate over all balances (for snapshot serialization).
    pub(crate) fn balances_iter(
        &self,
    ) -> impl Iterator<Item = (&(AccountId, CurrencyId), &Balance)> {
        self.balances.iter()
    }

    /// Iterate over all reservations (for snapshot serialization).
    pub(crate) fn reservations_iter(&self) -> impl Iterator<Item = (&OrderId, &Reservation)> {
        self.reservations.iter()
    }

    /// Credit funds to an account. Creates the account/currency entry if needed.
    pub fn deposit(&mut self, account: AccountId, currency: CurrencyId, amount: u64) {
        let balance = self.balances.entry((account, currency)).or_default();
        balance.available = balance.available.saturating_add(amount);
    }

    /// Debit available funds from an account.
    /// Returns `Err` if the account doesn't exist or has insufficient available balance.
    pub fn withdraw(
        &mut self,
        account: AccountId,
        currency: CurrencyId,
        amount: u64,
    ) -> Result<(), RejectReason> {
        let balance = self
            .balances
            .get_mut(&(account, currency))
            .ok_or(RejectReason::UnknownAccount)?;
        if balance.available < amount {
            return Err(RejectReason::InsufficientBalance);
        }
        balance.available -= amount;
        Ok(())
    }

    /// Get the balance for an account/currency pair.
    pub fn balance(&self, account: AccountId, currency: CurrencyId) -> Balance {
        self.balances
            .get(&(account, currency))
            .copied()
            .unwrap_or_default()
    }

    /// Attempt to reserve funds for an incoming order.
    ///
    /// - **Buy limit/stop-limit**: reserves `price × quantity` in quote currency.
    /// - **Sell limit/stop-limit**: reserves `quantity` in base currency.
    /// - **Buy market/stop-market**: reserves entire available quote balance
    ///   (refunded after execution, since final price is unknown).
    /// - **Sell market/stop-market**: reserves `quantity` in base currency.
    ///
    /// Returns the reserved amount on success, or a `RejectReason` on failure.
    pub fn try_reserve(
        &mut self,
        order: &Order,
        spec: &InstrumentSpec,
    ) -> Result<u64, RejectReason> {
        let (currency, amount) = self.required_reserve(order, spec)?;

        let balance = self
            .balances
            .get_mut(&(order.account, currency))
            .ok_or(RejectReason::InsufficientBalance)?;

        if balance.available < amount {
            return Err(RejectReason::InsufficientBalance);
        }

        balance.available -= amount;
        balance.reserved += amount;

        self.reservations.insert(
            order.id,
            Reservation {
                account: order.account,
                currency,
                remaining: amount,
            },
        );

        Ok(amount)
    }

    /// Update balances after a fill. Called once per `ExecutionReport::Fill`.
    ///
    /// The buyer's reserved quote decreases by `cost`, available base increases
    /// by `quantity`. The seller's reserved base decreases by `quantity`,
    /// available quote increases by `cost`.
    pub fn fill(
        &mut self,
        maker_order_id: OrderId,
        taker_order_id: OrderId,
        price: Price,
        quantity: Quantity,
        maker_side: Side,
        spec: &InstrumentSpec,
    ) {
        // cost = price × quantity, using u128 to avoid overflow.
        // This cannot overflow u64 here because we already validated at
        // reservation time. If it somehow does, saturate rather than panic.
        let cost = (price.get() as u128) * (quantity.get() as u128);
        let cost_u64 = u64::try_from(cost).unwrap_or(u64::MAX);
        let qty = quantity.get();

        let (buyer_order, seller_order) = match maker_side {
            Side::Buy => (maker_order_id, taker_order_id),
            Side::Sell => (taker_order_id, maker_order_id),
        };

        // Buyer: reserved quote decreases, available base increases.
        if let Some(res) = self.reservations.get_mut(&buyer_order) {
            res.remaining = res.remaining.saturating_sub(cost_u64);
            if let Some(bal) = self.balances.get_mut(&(res.account, spec.quote)) {
                bal.reserved = bal.reserved.saturating_sub(cost_u64);
            }
            if let Some(bal) = self.balances.get_mut(&(res.account, spec.base)) {
                bal.available = bal.available.saturating_add(qty);
            } else {
                // Account may not have a base currency entry yet.
                self.balances.insert(
                    (res.account, spec.base),
                    Balance {
                        available: qty,
                        reserved: 0,
                    },
                );
            }
        }

        // Seller: reserved base decreases, available quote increases.
        if let Some(res) = self.reservations.get_mut(&seller_order) {
            res.remaining = res.remaining.saturating_sub(qty);
            if let Some(bal) = self.balances.get_mut(&(res.account, spec.base)) {
                bal.reserved = bal.reserved.saturating_sub(qty);
            }
            if let Some(bal) = self.balances.get_mut(&(res.account, spec.quote)) {
                bal.available = bal.available.saturating_add(cost_u64);
            } else {
                self.balances.insert(
                    (res.account, spec.quote),
                    Balance {
                        available: cost_u64,
                        reserved: 0,
                    },
                );
            }
        }

        // Clean up fully consumed reservations.
        self.cleanup_reservation(buyer_order);
        self.cleanup_reservation(seller_order);
    }

    /// Release all remaining reserved funds for an order (on cancel or reject).
    pub fn release(&mut self, order_id: OrderId) {
        if let Some(res) = self.reservations.remove(&order_id)
            && let Some(bal) = self.balances.get_mut(&(res.account, res.currency))
        {
            bal.reserved = bal.reserved.saturating_sub(res.remaining);
            bal.available = bal.available.saturating_add(res.remaining);
        }
    }

    /// Process execution reports to update balances.
    /// Call this after the order book processes an order.
    pub fn process_reports(
        &mut self,
        reports: &[ExecutionReport],
        maker_sides: &HashMap<OrderId, Side>,
        spec: &InstrumentSpec,
    ) {
        for report in reports {
            match *report {
                ExecutionReport::Fill {
                    maker_order_id,
                    taker_order_id,
                    price,
                    quantity,
                    ..
                } => {
                    // Look up the maker's side to determine buyer/seller.
                    if let Some(&maker_side) = maker_sides.get(&maker_order_id) {
                        self.fill(
                            maker_order_id,
                            taker_order_id,
                            price,
                            quantity,
                            maker_side,
                            spec,
                        );
                    }
                }
                ExecutionReport::Cancelled { order_id, .. } => {
                    self.release(order_id);
                }
                ExecutionReport::Rejected { order_id, .. } => {
                    self.release(order_id);
                }
                ExecutionReport::Placed { .. } | ExecutionReport::Triggered { .. } => {}
            }
        }
    }

    /// Compute the required reserve currency and amount for an order.
    fn required_reserve(
        &self,
        order: &Order,
        spec: &InstrumentSpec,
    ) -> Result<(CurrencyId, u64), RejectReason> {
        match order.side {
            Side::Buy => {
                let currency = spec.quote;
                let amount = match order.order_type {
                    OrderType::Limit { price }
                    | OrderType::StopLimit {
                        limit_price: price, ..
                    } => {
                        // price × quantity in quote currency. Use u128 intermediate.
                        let cost = (price.get() as u128) * (order.quantity.get() as u128);
                        u64::try_from(cost).map_err(|_| RejectReason::InsufficientBalance)?
                    }
                    OrderType::Market | OrderType::Stop { .. } => {
                        // Reserve entire available quote balance since final
                        // price is unknown. Refunded after execution.
                        self.balances
                            .get(&(order.account, currency))
                            .map(|b| b.available)
                            .unwrap_or(0)
                    }
                };
                if amount == 0 {
                    return Err(RejectReason::InsufficientBalance);
                }
                Ok((currency, amount))
            }
            Side::Sell => {
                // Reserve quantity in base currency.
                Ok((spec.base, order.quantity.get()))
            }
        }
    }

    /// Remove reservation entry if fully consumed.
    fn cleanup_reservation(&mut self, order_id: OrderId) {
        if let Some(res) = self.reservations.get(&order_id)
            && res.remaining == 0
        {
            self.reservations.remove(&order_id);
        }
    }
}

impl Default for AccountManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::*;
    use crate::types::{OrderType, Symbol, TimeInForce};

    const ACCT_A: AccountId = AccountId(1);
    const ACCT_B: AccountId = AccountId(2);
    const BTC: CurrencyId = CurrencyId(0);
    const USD: CurrencyId = CurrencyId(1);

    fn spec() -> InstrumentSpec {
        InstrumentSpec {
            symbol: Symbol(1),
            base: BTC,
            quote: USD,
        }
    }

    fn qty(n: u64) -> Quantity {
        Quantity(NonZeroU64::new(n).unwrap())
    }

    fn price(n: u64) -> Price {
        Price(NonZeroU64::new(n).unwrap())
    }

    fn limit_buy(id: u64, account: AccountId, p: u64, q: u64) -> Order {
        Order {
            id: OrderId(id),
            account,
            side: Side::Buy,
            order_type: OrderType::Limit { price: price(p) },
            time_in_force: TimeInForce::GTC,
            quantity: qty(q),
        }
    }

    fn limit_sell(id: u64, account: AccountId, p: u64, q: u64) -> Order {
        Order {
            id: OrderId(id),
            account,
            side: Side::Sell,
            order_type: OrderType::Limit { price: price(p) },
            time_in_force: TimeInForce::GTC,
            quantity: qty(q),
        }
    }

    fn market_buy(id: u64, account: AccountId, q: u64) -> Order {
        Order {
            id: OrderId(id),
            account,
            side: Side::Buy,
            order_type: OrderType::Market,
            time_in_force: TimeInForce::IOC,
            quantity: qty(q),
        }
    }

    fn market_sell(id: u64, account: AccountId, q: u64) -> Order {
        Order {
            id: OrderId(id),
            account,
            side: Side::Sell,
            order_type: OrderType::Market,
            time_in_force: TimeInForce::IOC,
            quantity: qty(q),
        }
    }

    // -- Deposit / Withdraw --

    #[test]
    fn deposit_credits_available() {
        let mut mgr = AccountManager::new();
        mgr.deposit(ACCT_A, USD, 10_000);

        let bal = mgr.balance(ACCT_A, USD);
        assert_eq!(bal.available, 10_000);
        assert_eq!(bal.reserved, 0);
    }

    #[test]
    fn withdraw_debits_available() {
        let mut mgr = AccountManager::new();
        mgr.deposit(ACCT_A, USD, 10_000);
        mgr.withdraw(ACCT_A, USD, 3_000).unwrap();

        assert_eq!(mgr.balance(ACCT_A, USD).available, 7_000);
    }

    #[test]
    fn withdraw_insufficient_fails() {
        let mut mgr = AccountManager::new();
        mgr.deposit(ACCT_A, USD, 1_000);

        let err = mgr.withdraw(ACCT_A, USD, 5_000).unwrap_err();
        assert_eq!(err, RejectReason::InsufficientBalance);
        // Balance unchanged.
        assert_eq!(mgr.balance(ACCT_A, USD).available, 1_000);
    }

    #[test]
    fn balance_of_unknown_account_is_zero() {
        let mgr = AccountManager::new();
        let bal = mgr.balance(AccountId(999), USD);
        assert_eq!(bal.available, 0);
        assert_eq!(bal.reserved, 0);
    }

    // -- Reservation --

    #[test]
    fn reserve_limit_buy_locks_quote() {
        let mut mgr = AccountManager::new();
        mgr.deposit(ACCT_A, USD, 10_000);

        let order = limit_buy(1, ACCT_A, 100, 50); // cost = 100 * 50 = 5000
        let reserved = mgr.try_reserve(&order, &spec()).unwrap();

        assert_eq!(reserved, 5_000);
        let bal = mgr.balance(ACCT_A, USD);
        assert_eq!(bal.available, 5_000);
        assert_eq!(bal.reserved, 5_000);
    }

    #[test]
    fn reserve_limit_sell_locks_base() {
        let mut mgr = AccountManager::new();
        mgr.deposit(ACCT_A, BTC, 100);

        let order = limit_sell(1, ACCT_A, 50_000, 30);
        let reserved = mgr.try_reserve(&order, &spec()).unwrap();

        assert_eq!(reserved, 30);
        let bal = mgr.balance(ACCT_A, BTC);
        assert_eq!(bal.available, 70);
        assert_eq!(bal.reserved, 30);
    }

    #[test]
    fn reserve_insufficient_balance_rejected() {
        let mut mgr = AccountManager::new();
        mgr.deposit(ACCT_A, USD, 1_000);

        let order = limit_buy(1, ACCT_A, 100, 50); // cost = 5000 > 1000
        let err = mgr.try_reserve(&order, &spec()).unwrap_err();
        assert_eq!(err, RejectReason::InsufficientBalance);

        // Balance unchanged.
        assert_eq!(mgr.balance(ACCT_A, USD).available, 1_000);
        assert_eq!(mgr.balance(ACCT_A, USD).reserved, 0);
    }

    #[test]
    fn reserve_market_buy_locks_all_available_quote() {
        let mut mgr = AccountManager::new();
        mgr.deposit(ACCT_A, USD, 10_000);

        let order = market_buy(1, ACCT_A, 50);
        let reserved = mgr.try_reserve(&order, &spec()).unwrap();

        assert_eq!(reserved, 10_000);
        assert_eq!(mgr.balance(ACCT_A, USD).available, 0);
        assert_eq!(mgr.balance(ACCT_A, USD).reserved, 10_000);
    }

    #[test]
    fn reserve_market_sell_locks_base_quantity() {
        let mut mgr = AccountManager::new();
        mgr.deposit(ACCT_A, BTC, 100);

        let order = market_sell(1, ACCT_A, 30);
        let reserved = mgr.try_reserve(&order, &spec()).unwrap();

        assert_eq!(reserved, 30);
        assert_eq!(mgr.balance(ACCT_A, BTC).available, 70);
        assert_eq!(mgr.balance(ACCT_A, BTC).reserved, 30);
    }

    // -- Release --

    #[test]
    fn release_returns_reserved_to_available() {
        let mut mgr = AccountManager::new();
        mgr.deposit(ACCT_A, USD, 10_000);

        let order = limit_buy(1, ACCT_A, 100, 50);
        mgr.try_reserve(&order, &spec()).unwrap();
        mgr.release(OrderId(1));

        assert_eq!(mgr.balance(ACCT_A, USD).available, 10_000);
        assert_eq!(mgr.balance(ACCT_A, USD).reserved, 0);
    }

    #[test]
    fn release_unknown_order_is_noop() {
        let mut mgr = AccountManager::new();
        mgr.deposit(ACCT_A, USD, 10_000);
        mgr.release(OrderId(999));
        assert_eq!(mgr.balance(ACCT_A, USD).available, 10_000);
    }

    // -- Fill --

    #[test]
    fn fill_transfers_between_buyer_and_seller() {
        let mut mgr = AccountManager::new();
        mgr.deposit(ACCT_A, USD, 10_000); // buyer
        mgr.deposit(ACCT_B, BTC, 100); // seller

        let buy = limit_buy(1, ACCT_A, 100, 10); // cost = 1000
        let sell = limit_sell(2, ACCT_B, 100, 10);
        mgr.try_reserve(&buy, &spec()).unwrap();
        mgr.try_reserve(&sell, &spec()).unwrap();

        // Maker is seller (order 2), taker is buyer (order 1).
        mgr.fill(
            OrderId(2),
            OrderId(1),
            price(100),
            qty(10),
            Side::Sell,
            &spec(),
        );

        // Buyer: spent 1000 USD, got 10 BTC.
        assert_eq!(mgr.balance(ACCT_A, USD).available, 9_000);
        assert_eq!(mgr.balance(ACCT_A, USD).reserved, 0);
        assert_eq!(mgr.balance(ACCT_A, BTC).available, 10);

        // Seller: got 1000 USD, spent 10 BTC.
        assert_eq!(mgr.balance(ACCT_B, BTC).available, 90);
        assert_eq!(mgr.balance(ACCT_B, BTC).reserved, 0);
        assert_eq!(mgr.balance(ACCT_B, USD).available, 1_000);
    }

    #[test]
    fn partial_fill_keeps_remaining_reserved() {
        let mut mgr = AccountManager::new();
        mgr.deposit(ACCT_A, USD, 10_000);
        mgr.deposit(ACCT_B, BTC, 100);

        let buy = limit_buy(1, ACCT_A, 100, 20); // reserve 2000
        let sell = limit_sell(2, ACCT_B, 100, 10);
        mgr.try_reserve(&buy, &spec()).unwrap();
        mgr.try_reserve(&sell, &spec()).unwrap();

        // Partial fill: only 10 of 20 filled.
        mgr.fill(
            OrderId(2),
            OrderId(1),
            price(100),
            qty(10),
            Side::Sell,
            &spec(),
        );

        // Buyer: 1000 spent, 1000 still reserved for remaining 10 qty.
        assert_eq!(mgr.balance(ACCT_A, USD).available, 8_000);
        assert_eq!(mgr.balance(ACCT_A, USD).reserved, 1_000);
        assert_eq!(mgr.balance(ACCT_A, BTC).available, 10);
    }

    #[test]
    fn cancel_after_partial_fill_releases_remainder() {
        let mut mgr = AccountManager::new();
        mgr.deposit(ACCT_A, USD, 10_000);
        mgr.deposit(ACCT_B, BTC, 100);

        let buy = limit_buy(1, ACCT_A, 100, 20); // reserve 2000
        let sell = limit_sell(2, ACCT_B, 100, 10);
        mgr.try_reserve(&buy, &spec()).unwrap();
        mgr.try_reserve(&sell, &spec()).unwrap();

        // Fill 10 of 20.
        mgr.fill(
            OrderId(2),
            OrderId(1),
            price(100),
            qty(10),
            Side::Sell,
            &spec(),
        );
        // Cancel remaining 10.
        mgr.release(OrderId(1));

        // Buyer: 1000 spent on fills, 1000 returned from cancel.
        assert_eq!(mgr.balance(ACCT_A, USD).available, 9_000);
        assert_eq!(mgr.balance(ACCT_A, USD).reserved, 0);
        assert_eq!(mgr.balance(ACCT_A, BTC).available, 10);
    }

    #[test]
    fn overflow_price_times_quantity_rejected() {
        let mut mgr = AccountManager::new();
        mgr.deposit(ACCT_A, USD, u64::MAX);

        // price * quantity overflows u64.
        let order = limit_buy(1, ACCT_A, u64::MAX, 2);
        let err = mgr.try_reserve(&order, &spec()).unwrap_err();
        assert_eq!(err, RejectReason::InsufficientBalance);
    }

    #[test]
    fn self_trade_updates_same_account() {
        let mut mgr = AccountManager::new();
        mgr.deposit(ACCT_A, USD, 10_000);
        mgr.deposit(ACCT_A, BTC, 100);

        let buy = limit_buy(1, ACCT_A, 100, 10); // reserve 1000 USD
        let sell = limit_sell(2, ACCT_A, 100, 10); // reserve 10 BTC
        mgr.try_reserve(&buy, &spec()).unwrap();
        mgr.try_reserve(&sell, &spec()).unwrap();

        mgr.fill(
            OrderId(1),
            OrderId(2),
            price(100),
            qty(10),
            Side::Buy,
            &spec(),
        );

        // Self-trade: USD moves from reserved to available, BTC same.
        // Net effect: same balances as before (minus/plus cancel out).
        assert_eq!(mgr.balance(ACCT_A, USD).available, 9_000 + 1_000);
        assert_eq!(mgr.balance(ACCT_A, USD).reserved, 0);
        assert_eq!(mgr.balance(ACCT_A, BTC).available, 90 + 10);
        assert_eq!(mgr.balance(ACCT_A, BTC).reserved, 0);
    }

    #[test]
    fn market_buy_refund_after_fill() {
        let mut mgr = AccountManager::new();
        mgr.deposit(ACCT_A, USD, 10_000);
        mgr.deposit(ACCT_B, BTC, 100);

        let buy = market_buy(1, ACCT_A, 10);
        let sell = limit_sell(2, ACCT_B, 100, 10);
        mgr.try_reserve(&buy, &spec()).unwrap(); // reserves all 10_000
        mgr.try_reserve(&sell, &spec()).unwrap();

        // Fill at price 100, qty 10 → cost = 1000.
        mgr.fill(
            OrderId(2),
            OrderId(1),
            price(100),
            qty(10),
            Side::Sell,
            &spec(),
        );

        // Market order is fully filled, release unused reservation.
        mgr.release(OrderId(1));

        // Buyer: spent 1000, got back 9000 from unused reserve.
        assert_eq!(mgr.balance(ACCT_A, USD).available, 9_000);
        assert_eq!(mgr.balance(ACCT_A, USD).reserved, 0);
        assert_eq!(mgr.balance(ACCT_A, BTC).available, 10);
    }

    #[test]
    fn multiple_partial_fills_at_different_prices() {
        let mut mgr = AccountManager::new();
        mgr.deposit(ACCT_A, USD, 50_000);
        mgr.deposit(ACCT_B, BTC, 100);

        let buy = limit_buy(1, ACCT_A, 200, 20); // reserve 200*20 = 4000
        mgr.try_reserve(&buy, &spec()).unwrap();

        // Sell 1: 10 @ 100.
        let sell1 = limit_sell(2, ACCT_B, 100, 10);
        mgr.try_reserve(&sell1, &spec()).unwrap();
        mgr.fill(
            OrderId(2),
            OrderId(1),
            price(100),
            qty(10),
            Side::Sell,
            &spec(),
        );

        // Sell 2: 5 @ 150.
        let sell2 = limit_sell(3, ACCT_B, 150, 5);
        mgr.try_reserve(&sell2, &spec()).unwrap();
        mgr.fill(
            OrderId(3),
            OrderId(1),
            price(150),
            qty(5),
            Side::Sell,
            &spec(),
        );

        // Buyer spent 1000 + 750 = 1750, reserved 4000 - 1750 = 2250 remaining.
        assert_eq!(mgr.balance(ACCT_A, USD).available, 46_000);
        assert_eq!(mgr.balance(ACCT_A, USD).reserved, 2_250);
        assert_eq!(mgr.balance(ACCT_A, BTC).available, 15);
    }
}
