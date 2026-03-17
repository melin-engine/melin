//! Account balance management for the trading engine.
//!
//! Tracks per-account, per-currency balances. Reserves funds on order
//! placement, updates balances on fills, and releases reserves on
//! cancellation. Runs on the same single thread as the matching engine
//! (no locks needed).
//!
//! Balances are stored in a flat `Vec<Balance>` indexed by
//! `account_id * currency_stride + currency_id`. This gives O(1) lookups
//! with no hashing, no prefault needed (sequential allocation), and
//! near-instant bulk provisioning (single allocation + sequential writes).
//!
//! The Vec is sized at startup to cover all seeded accounts/currencies.
//! Hot-path operations (`try_reserve`, `fill`, `release`, `balance`) use
//! direct indexing with no allocation. Only `deposit` can grow the Vec
//! when a new account or currency appears — this is an admin operation
//! that happens outside the order-matching critical path.

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
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
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

    fn is_zero(&self) -> bool {
        self.available == 0 && self.reserved == 0
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
/// Balances are stored in a flat `Vec<Balance>` indexed by
/// `account_id * currency_stride + currency_id` for O(1) direct lookups.
/// No hashing, no prefault, and bulk provisioning is a single allocation.
///
/// The Vec is pre-sized at startup to cover all seeded accounts. Runtime
/// deposits for new accounts may grow the Vec, but deposits are admin
/// operations — not on the order-matching hot path.
pub struct AccountManager {
    /// Flat balance array. Index = account.0 * currency_stride + currency.0.
    balances: Vec<Balance>,
    /// Number of currency slots per account row (max_currency_id + 1).
    currency_stride: usize,
    /// Maps each open order to its reservation details. Keyed by
    /// (AccountId, OrderId) because different accounts can independently
    /// use the same OrderId. HashMap because keys are sparse.
    reservations: HashMap<(AccountId, OrderId), Reservation>,
}

impl AccountManager {
    pub fn new() -> Self {
        Self {
            balances: Vec::new(),
            currency_stride: 0,
            reservations: HashMap::new(),
        }
    }

    /// Create an AccountManager pre-sized for production workloads.
    pub fn with_capacity() -> Self {
        Self {
            balances: Vec::new(),
            currency_stride: 0,
            // One reservation per resting order across all instruments.
            reservations: HashMap::with_capacity(2_000_000),
        }
    }

    /// Touch all pre-allocated pages so page faults happen at startup,
    /// not on the hot path. Only needed for the reservations HashMap;
    /// the flat balance Vec is already contiguous and sequentially faulted.
    pub fn prefault(&mut self) {
        if self.reservations.is_empty() {
            let cap = self.reservations.capacity();
            for i in 0..cap {
                self.reservations.insert(
                    (AccountId(0), OrderId(i as u64)),
                    Reservation::new(AccountId(0), CurrencyId(0), 0),
                );
            }
            self.reservations.clear();
        }
    }

    /// Reconstruct from snapshot data.
    pub(crate) fn from_parts(
        balance_entries: Vec<((AccountId, CurrencyId), Balance)>,
        reservations: Vec<(OrderId, AccountId, CurrencyId, u64)>,
    ) -> Self {
        // Find dimensions from the balance entries.
        let mut max_account: u32 = 0;
        let mut max_currency: u32 = 0;
        for &((account, currency), _) in &balance_entries {
            max_account = max_account.max(account.0);
            max_currency = max_currency.max(currency.0);
        }
        let currency_stride = max_currency as usize + 1;
        let num_accounts = max_account as usize + 1;
        let mut balances = vec![Balance::default(); num_accounts * currency_stride];
        for ((account, currency), balance) in balance_entries {
            let idx = account.0 as usize * currency_stride + currency.0 as usize;
            balances[idx] = balance;
        }

        let reservation_map: HashMap<(AccountId, OrderId), Reservation> = reservations
            .into_iter()
            .map(|(order_id, account, currency, remaining)| {
                (
                    (account, order_id),
                    Reservation::new(account, currency, remaining),
                )
            })
            .collect();

        Self {
            balances,
            currency_stride,
            reservations: reservation_map,
        }
    }

    /// Snapshot all non-zero balances for serialization.
    pub(crate) fn snapshot_balances(&self) -> Vec<((AccountId, CurrencyId), Balance)> {
        if self.currency_stride == 0 {
            return Vec::new();
        }
        let mut out = Vec::new();
        for (i, bal) in self.balances.iter().enumerate() {
            if !bal.is_zero() {
                let account = AccountId((i / self.currency_stride) as u32);
                let currency = CurrencyId((i % self.currency_stride) as u32);
                out.push(((account, currency), *bal));
            }
        }
        out
    }

    /// Snapshot all reservations for serialization.
    pub(crate) fn snapshot_reservations(&self) -> Vec<(OrderId, AccountId, CurrencyId, u64)> {
        self.reservations
            .iter()
            .map(|(&(_account, order_id), res)| {
                (order_id, res.account(), res.currency(), res.remaining())
            })
            .collect()
    }

    /// Credit funds to an account. Grows the balance array if needed.
    /// This is an admin operation — not on the order-matching hot path.
    /// After startup seeding, the Vec already covers all known accounts
    /// so runtime deposits for existing accounts never allocate.
    pub fn deposit(&mut self, account: AccountId, currency: CurrencyId, amount: u64) {
        self.ensure_capacity(account, currency);
        let idx = account.0 as usize * self.currency_stride + currency.0 as usize;
        self.balances[idx].available = self.balances[idx].available.saturating_add(amount);
    }

    /// Debit available funds from an account.
    /// Returns `Err` if the account doesn't exist or has insufficient available balance.
    pub fn withdraw(
        &mut self,
        account: AccountId,
        currency: CurrencyId,
        amount: u64,
    ) -> Result<(), RejectReason> {
        let bal = self
            .get_mut(account, currency)
            .ok_or(RejectReason::UnknownAccount)?;
        if bal.available < amount {
            return Err(RejectReason::InsufficientBalance);
        }
        bal.available -= amount;
        Ok(())
    }

    /// Get the balance for an account/currency pair.
    pub fn balance(&self, account: AccountId, currency: CurrencyId) -> Balance {
        self.get(account, currency).copied().unwrap_or_default()
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
    /// `max_fee_bps` is the highest applicable fee rate (max of maker, taker).
    /// For buy limit orders, the reservation includes a fee cushion so that
    /// fees can always be charged from the reservation, even at the limit price.
    pub fn try_reserve(
        &mut self,
        order: &Order,
        spec: &InstrumentSpec,
        max_fee_bps: u16,
    ) -> Result<u64, RejectReason> {
        let (currency, amount) = self.required_reserve(order, spec, max_fee_bps)?;

        let bal = self
            .get_mut(order.account, currency)
            .ok_or(RejectReason::InsufficientBalance)?;

        if bal.available < amount {
            return Err(RejectReason::InsufficientBalance);
        }

        bal.available -= amount;
        bal.reserved += amount;

        self.reservations.insert(
            (order.account, order.id),
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
    /// The buyer's reserved quote decreases by `cost + buyer_fee`, available
    /// base increases by `quantity`. The seller's reserved base decreases by
    /// `quantity`, available quote increases by `cost - seller_fee`.
    #[allow(clippy::too_many_arguments)]
    pub fn fill(
        &mut self,
        maker_account: AccountId,
        maker_order_id: OrderId,
        taker_account: AccountId,
        taker_order_id: OrderId,
        price: Price,
        quantity: Quantity,
        maker_side: Side,
        maker_fee: u64,
        taker_fee: u64,
        spec: &InstrumentSpec,
    ) {
        // cost = price × quantity, using u128 to avoid overflow.
        // This fits in u64 because reservation validated price × quantity
        // at order placement (limit buys) or the quote budget caps total
        // cost (market buys). Assert in debug builds; saturate in release
        // as a defensive fallback.
        let cost = (price.get() as u128) * (quantity.get() as u128);
        debug_assert!(
            cost <= u64::MAX as u128,
            "fill cost overflows u64: {cost} = {} × {}",
            price.get(),
            quantity.get()
        );
        let cost_u64 = u64::try_from(cost).unwrap_or(u64::MAX);
        let qty = quantity.get();

        // Determine which fee applies to the buyer and seller.
        // Fees are in quote currency for both sides.
        let (buyer_account, buyer_order, seller_account, seller_order, buyer_fee, seller_fee) =
            match maker_side {
                Side::Buy => (
                    maker_account,
                    maker_order_id,
                    taker_account,
                    taker_order_id,
                    maker_fee,
                    taker_fee,
                ),
                Side::Sell => (
                    taker_account,
                    taker_order_id,
                    maker_account,
                    maker_order_id,
                    taker_fee,
                    maker_fee,
                ),
            };

        // Buyer: reserved quote decreases by cost + fee, available base increases.
        // The reservation includes a fee cushion (reserved at placement time
        // with max_fee_bps), so cost + fee fits within the reservation.
        // ensure_capacity is a no-op after startup seeding (all currencies
        // already in range) — just two comparisons, no allocation.
        if let Some(res) = self.reservations.get_mut(&(buyer_account, buyer_order)) {
            let total_deduct = cost_u64.saturating_add(buyer_fee);
            res.remaining = res.remaining.saturating_sub(total_deduct);
            let buyer_account = res.account;
            self.ensure_capacity(buyer_account, spec.base);
            let stride = self.currency_stride;
            let quote_idx = buyer_account.0 as usize * stride + spec.quote.0 as usize;
            self.balances[quote_idx].reserved = self.balances[quote_idx]
                .reserved
                .saturating_sub(total_deduct);
            let base_idx = buyer_account.0 as usize * stride + spec.base.0 as usize;
            self.balances[base_idx].available =
                self.balances[base_idx].available.saturating_add(qty);
        }

        // Seller: reserved base decreases, available quote increases by cost - fee.
        if let Some(res) = self.reservations.get_mut(&(seller_account, seller_order)) {
            res.remaining = res.remaining.saturating_sub(qty);
            let seller_account = res.account;
            self.ensure_capacity(seller_account, spec.quote);
            let stride = self.currency_stride;
            let base_idx = seller_account.0 as usize * stride + spec.base.0 as usize;
            self.balances[base_idx].reserved = self.balances[base_idx].reserved.saturating_sub(qty);
            let quote_idx = seller_account.0 as usize * stride + spec.quote.0 as usize;
            self.balances[quote_idx].available = self.balances[quote_idx]
                .available
                .saturating_add(cost_u64.saturating_sub(seller_fee));
        }

        // Note: reservation cleanup is handled by process_reports(), which
        // checks remaining == 0 after each fill and returns the consumed IDs.
        // Do NOT clean up here — process_reports needs the entry to exist
        // so it can report consumed IDs back to Exchange for order_sides cleanup.
    }

    /// Check if a reservation exists for the given (account, order) pair.
    pub fn has_reservation(&self, account: AccountId, order_id: OrderId) -> bool {
        self.reservations.contains_key(&(account, order_id))
    }

    /// Adjust an existing reservation in-place for cancel-replace.
    ///
    /// If the new amount is higher, checks that the account has sufficient
    /// available balance for the delta. If insufficient, returns
    /// `Err(InsufficientBalance)` and leaves the reservation unchanged.
    ///
    /// If the new amount is lower or equal, always succeeds.
    pub fn try_adjust_reservation(
        &mut self,
        account: AccountId,
        order_id: OrderId,
        new_amount: u64,
    ) -> Result<(), RejectReason> {
        let res = self
            .reservations
            .get(&(account, order_id))
            .ok_or(RejectReason::UnknownOrder)?;
        let old_amount = res.remaining;
        let account = res.account;
        let currency = res.currency;

        if new_amount == old_amount {
            return Ok(());
        }

        let bal = self
            .get_mut(account, currency)
            .ok_or(RejectReason::InsufficientBalance)?;

        if new_amount > old_amount {
            let delta = new_amount - old_amount;
            if bal.available < delta {
                return Err(RejectReason::InsufficientBalance);
            }
            bal.available -= delta;
            bal.reserved += delta;
        } else {
            let delta = old_amount - new_amount;
            bal.reserved = bal.reserved.saturating_sub(delta);
            bal.available = bal.available.saturating_add(delta);
        }

        // Update the reservation.
        let res = self
            .reservations
            .get_mut(&(account, order_id))
            .expect("checked above");
        res.remaining = new_amount;

        Ok(())
    }

    /// Release all remaining reserved funds for an order (on cancel or reject).
    pub fn release(&mut self, account: AccountId, order_id: OrderId) {
        if let Some(res) = self.reservations.remove(&(account, order_id))
            && let Some(bal) = self.get_mut(res.account, res.currency)
        {
            bal.reserved = bal.reserved.saturating_sub(res.remaining);
            bal.available = bal.available.saturating_add(res.remaining);
        }
    }

    /// Process execution reports to update balances.
    /// Call this after the order book processes an order.
    ///
    /// Returns order IDs whose reservations are fully consumed (remaining
    /// reached zero on fill, or released on cancel/reject). The caller
    /// should use this to clean up any per-order tracking maps (e.g.
    /// `order_sides` in Exchange).
    pub fn process_reports(
        &mut self,
        reports: &[ExecutionReport],
        maker_sides: &HashMap<(AccountId, OrderId), Side>,
        spec: &InstrumentSpec,
        consumed: &mut Vec<(AccountId, OrderId)>,
    ) {
        for report in reports {
            match *report {
                ExecutionReport::Fill {
                    maker_order_id,
                    taker_order_id,
                    maker_account,
                    taker_account,
                    price,
                    quantity,
                    maker_fee,
                    taker_fee,
                } => {
                    // Look up the maker's side to determine buyer/seller.
                    let maker_key = (maker_account, maker_order_id);
                    if let Some(&maker_side) = maker_sides.get(&maker_key) {
                        self.fill(
                            maker_account,
                            maker_order_id,
                            taker_account,
                            taker_order_id,
                            price,
                            quantity,
                            maker_side,
                            maker_fee,
                            taker_fee,
                            spec,
                        );
                    }
                    // Remove fully consumed reservations (remaining == 0).
                    let taker_key = (taker_account, taker_order_id);
                    if self
                        .reservations
                        .get(&maker_key)
                        .is_some_and(|r| r.remaining == 0)
                    {
                        self.reservations.remove(&maker_key);
                        consumed.push(maker_key);
                    }
                    if self
                        .reservations
                        .get(&taker_key)
                        .is_some_and(|r| r.remaining == 0)
                    {
                        self.reservations.remove(&taker_key);
                        consumed.push(taker_key);
                    }
                }
                ExecutionReport::Cancelled {
                    order_id, account, ..
                } => {
                    self.release(account, order_id);
                    consumed.push((account, order_id));
                }
                ExecutionReport::Rejected {
                    order_id, account, ..
                } => {
                    self.release(account, order_id);
                    consumed.push((account, order_id));
                }
                ExecutionReport::Placed { .. }
                | ExecutionReport::Triggered { .. }
                | ExecutionReport::Replaced { .. } => {}
            }
        }
    }

    /// Compute the required reserve currency and amount for an order.
    fn required_reserve(
        &self,
        order: &Order,
        spec: &InstrumentSpec,
        max_fee_bps: u16,
    ) -> Result<(CurrencyId, u64), RejectReason> {
        match order.side {
            Side::Buy => {
                let currency = spec.quote;
                let amount = match order.order_type {
                    OrderType::Limit { price }
                    | OrderType::StopLimit {
                        limit_price: price, ..
                    } => {
                        // price × quantity in quote currency, plus fee cushion.
                        // The fee cushion ensures fees can be charged from the
                        // reservation even when filling at the exact limit price.
                        let cost = (price.get() as u128) * (order.quantity.get() as u128);
                        // Fee cushion must use the same rounding direction as
                        // apply_fees (cost * bps / 10_000) to guarantee the
                        // reservation always covers the actual fee. No overflow
                        // risk: u128 handles cost * 10_000 for any u64 inputs.
                        let fee_cushion = cost * max_fee_bps as u128 / 10_000;
                        let with_fee = cost.saturating_add(fee_cushion);
                        u64::try_from(with_fee).map_err(|_| RejectReason::InsufficientBalance)?
                    }
                    OrderType::Market | OrderType::Stop { .. } => {
                        // Reserve entire available quote balance since final
                        // price is unknown. Refunded after execution.
                        self.get(order.account, currency)
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

    /// Get a reference to a balance slot, or None if out of bounds.
    /// Used on the hot path — two comparisons + one array index, no allocation.
    #[inline]
    fn get(&self, account: AccountId, currency: CurrencyId) -> Option<&Balance> {
        let c = currency.0 as usize;
        if c >= self.currency_stride {
            return None;
        }
        let idx = account.0 as usize * self.currency_stride + c;
        self.balances.get(idx)
    }

    /// Get a mutable reference to a balance slot, or None if out of bounds.
    #[inline]
    fn get_mut(&mut self, account: AccountId, currency: CurrencyId) -> Option<&mut Balance> {
        let c = currency.0 as usize;
        if c >= self.currency_stride {
            return None;
        }
        let idx = account.0 as usize * self.currency_stride + c;
        self.balances.get_mut(idx)
    }

    /// Grow the balance array if `(account, currency)` is out of bounds.
    /// After startup seeding this is a no-op (early return on two comparisons).
    /// Only allocates when a runtime deposit introduces a previously unseen
    /// account or currency — an admin operation, not on the matching hot path.
    ///
    /// Two growth cases: (1) currency_stride needs to increase — requires
    /// reshuffling all rows; (2) just need more account rows — simple extend.
    fn ensure_capacity(&mut self, account: AccountId, currency: CurrencyId) {
        let needed_stride = currency.0 as usize + 1;
        let needed_rows = account.0 as usize + 1;

        if needed_stride > self.currency_stride {
            // Stride increase: reshuffle existing rows to widen each row.
            let old_stride = self.currency_stride;
            let old_rows = if old_stride > 0 {
                self.balances.len() / old_stride
            } else {
                0
            };
            let new_rows = old_rows.max(needed_rows);
            let mut new_balances = vec![Balance::default(); new_rows * needed_stride];
            // Copy each old row into the wider layout.
            for row in 0..old_rows {
                let old_start = row * old_stride;
                let new_start = row * needed_stride;
                new_balances[new_start..new_start + old_stride]
                    .copy_from_slice(&self.balances[old_start..old_start + old_stride]);
            }
            self.balances = new_balances;
            self.currency_stride = needed_stride;
        } else if needed_rows * self.currency_stride > self.balances.len() {
            // Just need more rows — extend with zeros.
            self.balances
                .resize(needed_rows * self.currency_stride, Balance::default());
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
    use crate::types::{OrderType, SelfTradeProtection, Symbol, TimeInForce};

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
            stp: SelfTradeProtection::Allow,
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
            stp: SelfTradeProtection::Allow,
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
            stp: SelfTradeProtection::Allow,
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
            stp: SelfTradeProtection::Allow,
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
        let reserved = mgr.try_reserve(&order, &spec(), 0).unwrap();

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
        let reserved = mgr.try_reserve(&order, &spec(), 0).unwrap();

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
        let err = mgr.try_reserve(&order, &spec(), 0).unwrap_err();
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
        let reserved = mgr.try_reserve(&order, &spec(), 0).unwrap();

        assert_eq!(reserved, 10_000);
        assert_eq!(mgr.balance(ACCT_A, USD).available, 0);
        assert_eq!(mgr.balance(ACCT_A, USD).reserved, 10_000);
    }

    #[test]
    fn reserve_market_sell_locks_base_quantity() {
        let mut mgr = AccountManager::new();
        mgr.deposit(ACCT_A, BTC, 100);

        let order = market_sell(1, ACCT_A, 30);
        let reserved = mgr.try_reserve(&order, &spec(), 0).unwrap();

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
        mgr.try_reserve(&order, &spec(), 0).unwrap();
        mgr.release(ACCT_A, OrderId(1));

        assert_eq!(mgr.balance(ACCT_A, USD).available, 10_000);
        assert_eq!(mgr.balance(ACCT_A, USD).reserved, 0);
    }

    #[test]
    fn release_unknown_order_is_noop() {
        let mut mgr = AccountManager::new();
        mgr.deposit(ACCT_A, USD, 10_000);
        mgr.release(ACCT_A, OrderId(999));
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
        mgr.try_reserve(&buy, &spec(), 0).unwrap();
        mgr.try_reserve(&sell, &spec(), 0).unwrap();

        // Maker is seller (order 2, ACCT_B), taker is buyer (order 1, ACCT_A).
        mgr.fill(
            ACCT_B,
            OrderId(2),
            ACCT_A,
            OrderId(1),
            price(100),
            qty(10),
            Side::Sell,
            0,
            0,
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
        mgr.try_reserve(&buy, &spec(), 0).unwrap();
        mgr.try_reserve(&sell, &spec(), 0).unwrap();

        // Partial fill: only 10 of 20 filled.
        mgr.fill(
            ACCT_B,
            OrderId(2),
            ACCT_A,
            OrderId(1),
            price(100),
            qty(10),
            Side::Sell,
            0,
            0,
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
        mgr.try_reserve(&buy, &spec(), 0).unwrap();
        mgr.try_reserve(&sell, &spec(), 0).unwrap();

        // Fill 10 of 20.
        mgr.fill(
            ACCT_B,
            OrderId(2),
            ACCT_A,
            OrderId(1),
            price(100),
            qty(10),
            Side::Sell,
            0,
            0,
            &spec(),
        );
        // Cancel remaining 10.
        mgr.release(ACCT_A, OrderId(1));

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
        let err = mgr.try_reserve(&order, &spec(), 0).unwrap_err();
        assert_eq!(err, RejectReason::InsufficientBalance);
    }

    #[test]
    fn self_trade_updates_same_account() {
        let mut mgr = AccountManager::new();
        mgr.deposit(ACCT_A, USD, 10_000);
        mgr.deposit(ACCT_A, BTC, 100);

        let buy = limit_buy(1, ACCT_A, 100, 10); // reserve 1000 USD
        let sell = limit_sell(2, ACCT_A, 100, 10); // reserve 10 BTC
        mgr.try_reserve(&buy, &spec(), 0).unwrap();
        mgr.try_reserve(&sell, &spec(), 0).unwrap();

        mgr.fill(
            ACCT_A,
            OrderId(1),
            ACCT_A,
            OrderId(2),
            price(100),
            qty(10),
            Side::Buy,
            0,
            0,
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
        mgr.try_reserve(&buy, &spec(), 0).unwrap(); // reserves all 10_000
        mgr.try_reserve(&sell, &spec(), 0).unwrap();

        // Fill at price 100, qty 10 → cost = 1000.
        mgr.fill(
            ACCT_B,
            OrderId(2),
            ACCT_A,
            OrderId(1),
            price(100),
            qty(10),
            Side::Sell,
            0,
            0,
            &spec(),
        );

        // Market order is fully filled, release unused reservation.
        mgr.release(ACCT_A, OrderId(1));

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
        mgr.try_reserve(&buy, &spec(), 0).unwrap();

        // Sell 1: 10 @ 100.
        let sell1 = limit_sell(2, ACCT_B, 100, 10);
        mgr.try_reserve(&sell1, &spec(), 0).unwrap();
        mgr.fill(
            ACCT_B,
            OrderId(2),
            ACCT_A,
            OrderId(1),
            price(100),
            qty(10),
            Side::Sell,
            0,
            0,
            &spec(),
        );

        // Sell 2: 5 @ 150.
        let sell2 = limit_sell(3, ACCT_B, 150, 5);
        mgr.try_reserve(&sell2, &spec(), 0).unwrap();
        mgr.fill(
            ACCT_B,
            OrderId(3),
            ACCT_A,
            OrderId(1),
            price(150),
            qty(5),
            Side::Sell,
            0,
            0,
            &spec(),
        );

        // Buyer spent 1000 + 750 = 1750, reserved 4000 - 1750 = 2250 remaining.
        assert_eq!(mgr.balance(ACCT_A, USD).available, 46_000);
        assert_eq!(mgr.balance(ACCT_A, USD).reserved, 2_250);
        assert_eq!(mgr.balance(ACCT_A, BTC).available, 15);
    }
}
