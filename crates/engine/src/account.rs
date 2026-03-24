//! Account balance management for the trading engine.
//!
//! Tracks per-account, per-currency balances. Reserves funds on order
//! placement, updates balances on fills, and releases reserves on
//! cancellation. Runs on the same single thread as the matching engine
//! (no locks needed).
//!
//! Balances are stored in a sparse `HashMap<(AccountId, CurrencyId), Balance>`.
//! Only accounts with non-zero balances consume memory, scaling with active
//! accounts rather than `max(account_id) × max(currency_id)`. This enables
//! the gateway deposit/withdraw lifecycle pattern for extreme scale (see
//! `docs/account-lifecycle.md`).
//!
//! HashMap lookups (~20-50ns) are slower than flat Vec indexing (~1-3ns),
//! but the engine remains sub-microsecond per order. The self-contained
//! design (no gateway cooperation needed for correctness) is the right
//! commercial tradeoff.

use crate::types::{
    AccountId, CurrencyId, ExecutionReport, HashMap, InstrumentSpec, Order, OrderId, OrderType,
    Price, Quantity, RejectReason, Side,
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

/// Opaque handle to a reservation in the slab. O(1) Vec-indexed access,
/// no hashing. Valid from `try_reserve` until `release` or fill completion.
///
/// u32 index: supports up to ~4 billion concurrent reservations. At 2M
/// pre-allocated slots this is more than sufficient.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReservationSlot(u32);

impl ReservationSlot {
    /// Sentinel value for prefault dummy entries. Never used in production.
    pub const DUMMY: Self = Self(u32::MAX);
}

/// Tracks order side and reservation slot for a resting or in-flight order.
/// Stored in Exchange's order tracking map, providing O(1) reservation
/// access via the slot index (instead of a second HashMap lookup).
#[derive(Debug, Clone, Copy)]
pub struct OrderInfo {
    pub side: Side,
    pub reservation: ReservationSlot,
}

/// Manages account balances across all currencies.
///
/// Balances are stored in a sparse `HashMap<(AccountId, CurrencyId), Balance>`.
/// Only accounts with non-zero balances consume memory. Zero-balance entries
/// are removed on withdraw/release, so memory scales with active accounts.
///
/// HashMap: fast non-cryptographic hashing (~20-50ns per lookup). Chosen
/// over BTreeMap (log-n), std HashMap (SipHash overhead), and flat Vec
/// (can't handle sparse account ID space without wasting memory).
pub struct AccountManager {
    /// Sparse balance map. Only (account, currency) pairs with non-zero
    /// balances are present. Entries are removed when both available and
    /// reserved reach zero.
    balances: HashMap<(AccountId, CurrencyId), Balance>,
    /// Slab of active reservations. Indexed by `ReservationSlot(u32)` for
    /// O(1) access with no hashing. Freed slots are recycled via `free_slots`.
    /// Vec: contiguous, cache-friendly, zero per-access overhead vs HashMap's
    /// hash + probe + comparison per lookup.
    reservation_slab: Vec<Reservation>,
    /// Stack of recycled slot indices. Pop to allocate, push to free.
    /// LIFO reuse keeps recently-freed (cache-hot) slots in rotation.
    free_slots: Vec<u32>,
}

impl AccountManager {
    pub fn new() -> Self {
        Self {
            balances: HashMap::default(),
            reservation_slab: Vec::new(),
            free_slots: Vec::new(),
        }
    }

    /// Create an AccountManager pre-sized for production workloads.
    pub fn with_capacity() -> Self {
        // Pre-allocate 2M reservation slots. At 16 bytes each this is 32 MB.
        // Pages are faulted during prefault().
        //
        // Balance HashMap starts empty — deposits insert entries on demand.
        // No pre-allocation needed since deposit is an admin operation.
        Self {
            balances: HashMap::default(),
            reservation_slab: Vec::with_capacity(2_000_000),
            free_slots: Vec::with_capacity(2_000_000),
        }
    }

    /// Touch all pre-allocated pages so page faults happen at startup,
    /// not on the hot path. Pre-fills the slab with dummy reservations and
    /// builds the free list in reverse order (so slot 0 is allocated first).
    pub fn prefault(&mut self) {
        if self.reservation_slab.is_empty() {
            let cap = self.reservation_slab.capacity().max(2_000_000);
            let dummy = Reservation::new(AccountId(0), CurrencyId(0), 0);
            self.reservation_slab.resize(cap, dummy);
            self.free_slots.clear();
            self.free_slots.reserve(cap);
            // Reverse order: slot 0 at top of stack, allocated first.
            for i in (0..cap).rev() {
                self.free_slots.push(i as u32);
            }
        }
        // Balances HashMap: pages are faulted on deposit (admin path).
        // No prefault needed — the HashMap grows organically and never
        // causes a hot-path resize spike (only deposits/withdrawals
        // insert/remove entries).
    }

    /// Reconstruct from snapshot data. Returns `(manager, slot_assignments)`
    /// where `slot_assignments` maps each `(AccountId, OrderId)` to its
    /// `ReservationSlot` so the caller can build `OrderInfo` entries.
    #[allow(clippy::type_complexity)]
    pub(crate) fn from_parts(
        balance_entries: Vec<((AccountId, CurrencyId), Balance)>,
        reservations: Vec<(OrderId, AccountId, CurrencyId, u64)>,
    ) -> (Self, Vec<((AccountId, OrderId), ReservationSlot)>) {
        // Build balance HashMap directly from sparse entries.
        let mut balances =
            HashMap::with_capacity_and_hasher(balance_entries.len(), Default::default());
        for (key, balance) in balance_entries {
            if !balance.is_zero() {
                balances.insert(key, balance);
            }
        }

        // Build slab sequentially — slots 0..n for n reservations.
        let mut slab = Vec::with_capacity(reservations.len());
        let mut slot_assignments = Vec::with_capacity(reservations.len());
        for (order_id, account, currency, remaining) in reservations {
            let slot = ReservationSlot(slab.len() as u32);
            slab.push(Reservation::new(account, currency, remaining));
            slot_assignments.push(((account, order_id), slot));
        }

        let mgr = Self {
            balances,
            reservation_slab: slab,
            free_slots: Vec::new(),
        };
        (mgr, slot_assignments)
    }

    /// Snapshot all non-zero balances for serialization.
    pub(crate) fn snapshot_balances(&self) -> Vec<((AccountId, CurrencyId), Balance)> {
        // fill() can create zero-balance entries via entry().or_default(),
        // so filter them out to keep snapshots compact.
        self.balances
            .iter()
            .filter(|(_, v)| !v.is_zero())
            .map(|(&k, &v)| (k, v))
            .collect()
    }

    /// Snapshot all active reservations for serialization. The caller
    /// provides `(AccountId, OrderId, ReservationSlot)` tuples from its
    /// order tracking map since the slab doesn't track which slots are live.
    pub(crate) fn snapshot_reservations(
        &self,
        active: &[((AccountId, OrderId), ReservationSlot)],
    ) -> Vec<(OrderId, AccountId, CurrencyId, u64)> {
        active
            .iter()
            .map(|&((_, order_id), slot)| {
                let res = &self.reservation_slab[slot.0 as usize];
                (order_id, res.account(), res.currency(), res.remaining())
            })
            .collect()
    }

    /// Credit funds to an account. Inserts a new balance entry if the
    /// (account, currency) pair doesn't exist yet.
    /// This is an admin operation — not on the order-matching hot path.
    pub fn deposit(&mut self, account: AccountId, currency: CurrencyId, amount: u64) {
        let bal = self.balances.entry((account, currency)).or_default();
        bal.available = bal.available.saturating_add(amount);
    }

    /// Debit available funds from an account.
    /// Returns `Err` if the account doesn't exist or has insufficient available balance.
    /// Removes the entry if both available and reserved reach zero (memory cleanup).
    pub fn withdraw(
        &mut self,
        account: AccountId,
        currency: CurrencyId,
        amount: u64,
    ) -> Result<(), RejectReason> {
        let bal = self
            .balances
            .get_mut(&(account, currency))
            .ok_or(RejectReason::UnknownAccount)?;
        if bal.available < amount {
            return Err(RejectReason::InsufficientBalance);
        }
        bal.available -= amount;
        // Clean up zero-balance entries so memory scales with active accounts.
        if bal.is_zero() {
            self.balances.remove(&(account, currency));
        }
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
    /// Returns `(reserved_amount, slot)` on success, or a `RejectReason` on
    /// failure. The `ReservationSlot` is an opaque handle for O(1) access to
    /// the reservation in subsequent fill/release/adjust calls.
    /// `max_fee_bps` is the highest applicable fee rate (max of maker, taker).
    /// For buy limit orders, the reservation includes a fee cushion so that
    /// fees can always be charged from the reservation, even at the limit price.
    pub fn try_reserve(
        &mut self,
        order: &Order,
        spec: &InstrumentSpec,
        max_fee_bps: u16,
    ) -> Result<(u64, ReservationSlot), RejectReason> {
        let (currency, amount) = self.required_reserve(order, spec, max_fee_bps)?;

        let bal = self
            .balances
            .get_mut(&(order.account, currency))
            .ok_or(RejectReason::InsufficientBalance)?;

        if bal.available < amount {
            return Err(RejectReason::InsufficientBalance);
        }

        bal.available -= amount;
        bal.reserved += amount;

        let slot = self.alloc_slot(Reservation {
            account: order.account,
            currency,
            remaining: amount,
        });

        Ok((amount, slot))
    }

    /// Update balances after a fill. Called once per `ExecutionReport::Fill`.
    ///
    /// The buyer's reserved quote decreases by `cost + buyer_fee`, available
    /// base increases by `quantity`. The seller's reserved base decreases by
    /// `quantity`, available quote increases by `cost - seller_fee`.
    ///
    /// Takes `ReservationSlot` handles for O(1) slab access (no hashing).
    pub fn fill(
        &mut self,
        buyer_slot: ReservationSlot,
        seller_slot: ReservationSlot,
        price: Price,
        quantity: Quantity,
        buyer_fee: i64,
        seller_fee: i64,
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

        // Buyer: reserved quote decreases by cost + fee, available base increases.
        // The reservation includes a fee cushion (reserved at placement time
        // with max_fee_bps), so cost + fee fits within the reservation.
        //
        // Signed fees: positive = fee deducted, negative = rebate credited.
        // cost_i128 + buyer_fee_i128 is clamped to [0, u64::MAX] to handle
        // rebates that exceed cost (defensive; shouldn't happen in practice).
        {
            let res = &mut self.reservation_slab[buyer_slot.0 as usize];
            let total_deduct_i128 = cost_u64 as i128 + buyer_fee as i128;
            let total_deduct =
                u64::try_from(total_deduct_i128.clamp(0, u64::MAX as i128)).unwrap_or(0);
            res.remaining = res.remaining.saturating_sub(total_deduct);
            let buyer_account = res.account;

            let quote_bal = self
                .balances
                .entry((buyer_account, spec.quote))
                .or_default();
            quote_bal.reserved = quote_bal.reserved.saturating_sub(total_deduct);

            let base_bal = self.balances.entry((buyer_account, spec.base)).or_default();
            base_bal.available = base_bal.available.saturating_add(qty);
        }

        // Seller: reserved base decreases, available quote increases by cost - fee.
        // Signed: cost - positive_fee = less proceeds; cost - negative_fee = more proceeds (rebate).
        {
            let res = &mut self.reservation_slab[seller_slot.0 as usize];
            res.remaining = res.remaining.saturating_sub(qty);
            let seller_account = res.account;

            let base_bal = self
                .balances
                .entry((seller_account, spec.base))
                .or_default();
            base_bal.reserved = base_bal.reserved.saturating_sub(qty);

            let quote_bal = self
                .balances
                .entry((seller_account, spec.quote))
                .or_default();
            let proceeds_i128 = cost_u64 as i128 - seller_fee as i128;
            let proceeds = u64::try_from(proceeds_i128.clamp(0, u64::MAX as i128)).unwrap_or(0);
            quote_bal.available = quote_bal.available.saturating_add(proceeds);
        }

        // Note: reservation cleanup is handled by process_reports(), which
        // checks remaining == 0 after each fill and returns the consumed IDs.
        // Do NOT clean up here — process_reports needs the entry to exist
        // so it can report consumed IDs back to Exchange for order_info cleanup.
    }

    /// Check if a reservation's remaining amount is zero.
    pub fn is_reservation_empty(&self, slot: ReservationSlot) -> bool {
        self.reservation_slab[slot.0 as usize].remaining == 0
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
        slot: ReservationSlot,
        new_amount: u64,
    ) -> Result<(), RejectReason> {
        let res = &self.reservation_slab[slot.0 as usize];
        let old_amount = res.remaining;
        let account = res.account;
        let currency = res.currency;

        if new_amount == old_amount {
            return Ok(());
        }

        let bal = self
            .balances
            .get_mut(&(account, currency))
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

        self.reservation_slab[slot.0 as usize].remaining = new_amount;
        Ok(())
    }

    /// Release all remaining reserved funds and free the slot.
    pub fn release(&mut self, slot: ReservationSlot) {
        let res = self.reservation_slab[slot.0 as usize];
        if let Some(bal) = self.balances.get_mut(&(res.account, res.currency)) {
            bal.reserved = bal.reserved.saturating_sub(res.remaining);
            bal.available = bal.available.saturating_add(res.remaining);
        }
        self.free_slots.push(slot.0);
    }

    /// Process execution reports to update balances.
    /// Call this after the order book processes an order.
    ///
    /// Uses the `order_info` map to resolve `(AccountId, OrderId)` pairs from
    /// execution reports into `ReservationSlot` handles for O(1) slab access.
    ///
    /// Returns order IDs whose reservations are fully consumed (remaining
    /// reached zero on fill, or released on cancel/reject). The caller
    /// should use this to clean up any per-order tracking maps.
    pub fn process_reports(
        &mut self,
        reports: &[ExecutionReport],
        order_info: &HashMap<(AccountId, OrderId), OrderInfo>,
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
                    let maker_key = (maker_account, maker_order_id);
                    let taker_key = (taker_account, taker_order_id);

                    // Look up both sides' OrderInfo for slot + side resolution.
                    // Cache the reservation slots to avoid re-querying order_info
                    // for the remaining==0 check below (saves 2 HashMap lookups
                    // per fill — was 30% of this function's cost).
                    if let (Some(maker_info), Some(taker_info)) =
                        (order_info.get(&maker_key), order_info.get(&taker_key))
                    {
                        let maker_slot = maker_info.reservation;
                        let taker_slot = taker_info.reservation;

                        // Determine buyer/seller slots and fees from maker side.
                        let (buyer_slot, seller_slot, buyer_fee, seller_fee) = match maker_info.side
                        {
                            Side::Buy => (maker_slot, taker_slot, maker_fee, taker_fee),
                            Side::Sell => (taker_slot, maker_slot, taker_fee, maker_fee),
                        };
                        self.fill(
                            buyer_slot,
                            seller_slot,
                            price,
                            quantity,
                            buyer_fee,
                            seller_fee,
                            spec,
                        );

                        // Free fully consumed reservations (remaining == 0).
                        // Uses cached slots — no re-lookup needed.
                        if self.reservation_slab[maker_slot.0 as usize].remaining == 0 {
                            self.free_slots.push(maker_slot.0);
                            consumed.push(maker_key);
                        }
                        if self.reservation_slab[taker_slot.0 as usize].remaining == 0 {
                            self.free_slots.push(taker_slot.0);
                            consumed.push(taker_key);
                        }
                    }
                }
                ExecutionReport::Cancelled {
                    order_id, account, ..
                } => {
                    let key = (account, order_id);
                    // Skip if already consumed (e.g., fill set remaining to 0
                    // earlier in this batch, then IOC cancelled the unfilled
                    // remainder). Without this guard, we'd double-free the slab
                    // slot and corrupt a future reservation.
                    if !consumed.contains(&key) {
                        if let Some(info) = order_info.get(&key) {
                            self.release(info.reservation);
                        }
                        consumed.push(key);
                    }
                }
                ExecutionReport::Rejected {
                    order_id, account, ..
                } => {
                    let key = (account, order_id);
                    if !consumed.contains(&key) {
                        if let Some(info) = order_info.get(&key) {
                            self.release(info.reservation);
                        }
                        consumed.push(key);
                    }
                }
                ExecutionReport::Placed { .. }
                | ExecutionReport::Triggered { .. }
                | ExecutionReport::Replaced { .. } => {}
            }
        }
    }

    /// Allocate a slab slot for a new reservation. O(1): pops from the free
    /// list, or extends the Vec if no recycled slots are available.
    fn alloc_slot(&mut self, reservation: Reservation) -> ReservationSlot {
        if let Some(idx) = self.free_slots.pop() {
            self.reservation_slab[idx as usize] = reservation;
            ReservationSlot(idx)
        } else {
            let idx = self.reservation_slab.len();
            self.reservation_slab.push(reservation);
            ReservationSlot(idx as u32)
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
                    OrderType::Limit { price, .. }
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

    /// Check if an account has any non-zero balances.
    ///
    /// O(n) scan of all entries — test-only. If needed in production,
    /// replace with a per-account non-zero currency counter.
    #[cfg(test)]
    pub fn has_balances(&self, account: AccountId) -> bool {
        self.balances
            .iter()
            .any(|(&(a, _), v)| a == account && !v.is_zero())
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
            order_type: OrderType::Limit {
                price: price(p),
                post_only: false,
            },
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
            order_type: OrderType::Limit {
                price: price(p),
                post_only: false,
            },
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
        let (reserved, _slot) = mgr.try_reserve(&order, &spec(), 0).unwrap();

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
        let (reserved, _slot) = mgr.try_reserve(&order, &spec(), 0).unwrap();

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
        let (reserved, _slot) = mgr.try_reserve(&order, &spec(), 0).unwrap();

        assert_eq!(reserved, 10_000);
        assert_eq!(mgr.balance(ACCT_A, USD).available, 0);
        assert_eq!(mgr.balance(ACCT_A, USD).reserved, 10_000);
    }

    #[test]
    fn reserve_market_sell_locks_base_quantity() {
        let mut mgr = AccountManager::new();
        mgr.deposit(ACCT_A, BTC, 100);

        let order = market_sell(1, ACCT_A, 30);
        let (reserved, _slot) = mgr.try_reserve(&order, &spec(), 0).unwrap();

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
        let (_amount, slot) = mgr.try_reserve(&order, &spec(), 0).unwrap();
        mgr.release(slot);

        assert_eq!(mgr.balance(ACCT_A, USD).available, 10_000);
        assert_eq!(mgr.balance(ACCT_A, USD).reserved, 0);
    }

    // -- Fill --

    #[test]
    fn fill_transfers_between_buyer_and_seller() {
        let mut mgr = AccountManager::new();
        mgr.deposit(ACCT_A, USD, 10_000); // buyer
        mgr.deposit(ACCT_B, BTC, 100); // seller

        let buy = limit_buy(1, ACCT_A, 100, 10); // cost = 1000
        let sell = limit_sell(2, ACCT_B, 100, 10);
        let (_amt, buy_slot) = mgr.try_reserve(&buy, &spec(), 0).unwrap();
        let (_amt, sell_slot) = mgr.try_reserve(&sell, &spec(), 0).unwrap();

        // Buyer = ACCT_A (buy_slot), Seller = ACCT_B (sell_slot).
        mgr.fill(buy_slot, sell_slot, price(100), qty(10), 0, 0, &spec());

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
        let (_amt, buy_slot) = mgr.try_reserve(&buy, &spec(), 0).unwrap();
        let (_amt, sell_slot) = mgr.try_reserve(&sell, &spec(), 0).unwrap();

        // Partial fill: only 10 of 20 filled.
        mgr.fill(buy_slot, sell_slot, price(100), qty(10), 0, 0, &spec());

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
        let (_amt, buy_slot) = mgr.try_reserve(&buy, &spec(), 0).unwrap();
        let (_amt, sell_slot) = mgr.try_reserve(&sell, &spec(), 0).unwrap();

        // Fill 10 of 20.
        mgr.fill(buy_slot, sell_slot, price(100), qty(10), 0, 0, &spec());
        // Cancel remaining 10.
        mgr.release(buy_slot);

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
        let (_amt, buy_slot) = mgr.try_reserve(&buy, &spec(), 0).unwrap();
        let (_amt, sell_slot) = mgr.try_reserve(&sell, &spec(), 0).unwrap();

        // Buyer = buy_slot (ACCT_A buy), Seller = sell_slot (ACCT_A sell).
        mgr.fill(buy_slot, sell_slot, price(100), qty(10), 0, 0, &spec());

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
        let (_amt, buy_slot) = mgr.try_reserve(&buy, &spec(), 0).unwrap(); // reserves all 10_000
        let (_amt, sell_slot) = mgr.try_reserve(&sell, &spec(), 0).unwrap();

        // Fill at price 100, qty 10 → cost = 1000.
        mgr.fill(buy_slot, sell_slot, price(100), qty(10), 0, 0, &spec());

        // Market order is fully filled, release unused reservation.
        mgr.release(buy_slot);

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
        let (_amt, buy_slot) = mgr.try_reserve(&buy, &spec(), 0).unwrap();

        // Sell 1: 10 @ 100.
        let sell1 = limit_sell(2, ACCT_B, 100, 10);
        let (_amt, sell1_slot) = mgr.try_reserve(&sell1, &spec(), 0).unwrap();
        mgr.fill(buy_slot, sell1_slot, price(100), qty(10), 0, 0, &spec());

        // Sell 2: 5 @ 150.
        let sell2 = limit_sell(3, ACCT_B, 150, 5);
        let (_amt, sell2_slot) = mgr.try_reserve(&sell2, &spec(), 0).unwrap();
        mgr.fill(buy_slot, sell2_slot, price(150), qty(5), 0, 0, &spec());

        // Buyer spent 1000 + 750 = 1750, reserved 4000 - 1750 = 2250 remaining.
        assert_eq!(mgr.balance(ACCT_A, USD).available, 46_000);
        assert_eq!(mgr.balance(ACCT_A, USD).reserved, 2_250);
        assert_eq!(mgr.balance(ACCT_A, BTC).available, 15);
    }

    // -- Sparse storage / withdrawal cleanup --

    #[test]
    fn withdraw_to_zero_removes_entry() {
        let mut mgr = AccountManager::new();
        mgr.deposit(ACCT_A, USD, 1_000);
        assert!(mgr.has_balances(ACCT_A));

        mgr.withdraw(ACCT_A, USD, 1_000).unwrap();
        // Entry should be removed from the HashMap.
        assert!(!mgr.has_balances(ACCT_A));
        assert_eq!(mgr.balance(ACCT_A, USD), Balance::default());
    }

    #[test]
    fn partial_withdraw_keeps_entry() {
        let mut mgr = AccountManager::new();
        mgr.deposit(ACCT_A, USD, 1_000);

        mgr.withdraw(ACCT_A, USD, 500).unwrap();
        assert!(mgr.has_balances(ACCT_A));
        assert_eq!(mgr.balance(ACCT_A, USD).available, 500);
    }

    #[test]
    fn snapshot_balances_sparse() {
        let mut mgr = AccountManager::new();
        mgr.deposit(ACCT_A, USD, 1_000);
        mgr.deposit(ACCT_B, BTC, 500);

        let snap = mgr.snapshot_balances();
        assert_eq!(snap.len(), 2);
    }

    #[test]
    fn withdraw_unknown_account_fails() {
        let mgr = AccountManager::new();
        let err = mgr.balance(AccountId(999), USD);
        assert_eq!(err, Balance::default());
        // Withdraw from non-existent account.
        let mut mgr = AccountManager::new();
        let err = mgr.withdraw(AccountId(999), USD, 100).unwrap_err();
        assert_eq!(err, RejectReason::UnknownAccount);
    }

    #[test]
    fn zero_amount_deposit_creates_entry() {
        let mut mgr = AccountManager::new();
        mgr.deposit(ACCT_A, USD, 0);
        // Zero deposit creates an entry (available=0, reserved=0).
        // has_balances filters zero entries, so this should be false.
        assert!(!mgr.has_balances(ACCT_A));
    }

    #[test]
    fn withdraw_multiple_currencies_partial() {
        let mut mgr = AccountManager::new();
        mgr.deposit(ACCT_A, USD, 10_000);
        mgr.deposit(ACCT_A, BTC, 100);

        // Withdraw all USD, keep BTC.
        mgr.withdraw(ACCT_A, USD, 10_000).unwrap();
        assert!(mgr.has_balances(ACCT_A)); // Still has BTC.
        assert_eq!(mgr.balance(ACCT_A, USD), Balance::default());
        assert_eq!(mgr.balance(ACCT_A, BTC).available, 100);
    }

    #[test]
    fn fill_zero_entries_filtered_in_snapshot() {
        let mut mgr = AccountManager::new();
        // Buyer has quote only, seller has base only.
        mgr.deposit(ACCT_A, USD, 10_000);
        mgr.deposit(ACCT_B, BTC, 100);

        let buy = limit_buy(1, ACCT_A, 100, 10);
        let sell = limit_sell(2, ACCT_B, 100, 10);
        let (_, buy_slot) = mgr.try_reserve(&buy, &spec(), 0).unwrap();
        let (_, sell_slot) = mgr.try_reserve(&sell, &spec(), 0).unwrap();

        // Fill creates base entry for buyer, quote entry for seller.
        mgr.fill(buy_slot, sell_slot, price(100), qty(10), 0, 0, &spec());

        // Snapshot should only include non-zero entries.
        let snap = mgr.snapshot_balances();
        for ((_, _), bal) in &snap {
            assert!(!bal.is_zero(), "snapshot contains zero-balance entry");
        }
    }

    #[test]
    fn from_parts_round_trip_sparse() {
        let mut mgr = AccountManager::new();
        mgr.deposit(ACCT_A, USD, 1_000);
        mgr.deposit(ACCT_B, BTC, 500);

        let snap = mgr.snapshot_balances();
        let (restored, _) = AccountManager::from_parts(snap, Vec::new());

        assert_eq!(restored.balance(ACCT_A, USD).available, 1_000);
        assert_eq!(restored.balance(ACCT_B, BTC).available, 500);
        assert_eq!(restored.balance(ACCT_A, BTC), Balance::default());
    }
}
