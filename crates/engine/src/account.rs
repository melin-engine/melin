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
    AccountId, CurrencyId, HashMap4, InstrumentSpec, Order, OrderId, OrderType, Price, Quantity,
    RejectReason, Side,
};

/// Saturating fallback for a checked subtraction in a balance path.
///
/// Used when a u64 balance/reservation field would underflow. Logs a
/// structured `error!` (which the audit requires for SEC-07 — silent
/// `saturating_sub` previously masked balance corruption) and returns 0.
/// `#[cold]` keeps the failure branch off the hot path.
#[cold]
#[inline(never)]
fn log_underflow(
    op: &'static str,
    account: AccountId,
    currency: CurrencyId,
    current: u64,
    requested: u64,
) -> u64 {
    tracing::error!(
        op,
        account = account.0,
        currency = currency.0,
        current,
        requested,
        "balance arithmetic underflow in fill path — clamping to zero; possible state corruption"
    );
    0
}

/// Saturating fallback for a checked addition in a balance path. Returns
/// `u64::MAX` and logs on overflow. u64 overflow is not reachable at
/// realistic exchange scales, but the audit asks us to surface it rather
/// than silently saturate.
#[cold]
#[inline(never)]
fn log_overflow(
    op: &'static str,
    account: AccountId,
    currency: CurrencyId,
    current: u64,
    addend: u64,
) -> u64 {
    tracing::error!(
        op,
        account = account.0,
        currency = currency.0,
        current,
        addend,
        "balance arithmetic overflow in fill path — clamping to u64::MAX; possible state corruption"
    );
    u64::MAX
}

/// Reserved account for collected trading fees. Fees deducted from traders
/// are credited here so they remain in the system (balance conservation).
/// The exchange operator can withdraw from this account via the admin API.
///
/// **Signed ledger.** Unlike trader accounts, the fee account is allowed to
/// go negative. Its logical balance is `available - deficit` (per currency).
/// Maker rebates that exceed `available` accumulate on the per-currency
/// `fee_account_deficits` map; subsequent fee revenue pays the deficit
/// down before crediting `available`. This matches industry accounting
/// (fees are operator P&L, not a user-facing balance) and prevents the
/// previous failure mode where `saturating_sub` silently shortchanged a
/// trader when the rebate exceeded the fee account balance. Operators
/// monitor `fee_signed_balance(currency)` to know whether the account is
/// in the red and needs funding.
pub const FEE_ACCOUNT: AccountId = AccountId(0);

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

// Re-export for backward compatibility with existing imports.
pub use crate::types::ReservationSlot;

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
    balances: HashMap4<(AccountId, CurrencyId), Balance>,
    /// Slab of active reservations. Indexed by `ReservationSlot(u32)` for
    /// O(1) access with no hashing. Freed slots are recycled via `free_slots`.
    /// Vec: contiguous, cache-friendly, zero per-access overhead vs HashMap's
    /// hash + probe + comparison per lookup.
    reservation_slab: Vec<Reservation>,
    /// Stack of recycled slot indices. Pop to allocate, push to free.
    /// LIFO reuse keeps recently-freed (cache-hot) slots in rotation.
    free_slots: Vec<u32>,
    /// Per-currency deficit on the fee account, tracking how far it has
    /// gone "into the red" funding rebates. Logical fee-account balance
    /// for a currency is `balances[FEE_ACCOUNT, ccy].available - deficit`
    /// (signed, i128). Fee revenue pays down deficit before crediting
    /// `available`; rebates that exceed `available` drain it to zero and
    /// push the overage onto deficit.
    ///
    /// HashMap4 (4-entry buckets, fast non-cryptographic hash): consistent
    /// with `balances`. Entries are removed when deficit returns to zero
    /// to keep the map small. Sparse: only currencies that have ever been
    /// in deficit have entries.
    fee_account_deficits: HashMap4<CurrencyId, u64>,
}

impl AccountManager {
    pub fn new() -> Self {
        Self {
            balances: HashMap4::default(),
            reservation_slab: Vec::new(),
            free_slots: Vec::new(),
            fee_account_deficits: HashMap4::default(),
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
            balances: HashMap4::default(),
            reservation_slab: Vec::with_capacity(2_000_000),
            free_slots: Vec::with_capacity(2_000_000),
            fee_account_deficits: HashMap4::default(),
        }
    }

    /// Create an AccountManager with the balances HashMap pre-sized for a
    /// known bulk-seed workload. `balance_capacity` should be the expected
    /// total number of `(account, currency)` pairs after seeding completes
    /// — typically `num_accounts × num_instruments × 2` for the bench
    /// `ProvisionAccount` flow which deposits both base and quote per
    /// instrument. Pre-sizing eliminates the multi-hundred-ms rehash
    /// stalls that otherwise show up in seed-phase outliers.
    pub fn with_balance_capacity(balance_capacity: usize) -> Self {
        Self {
            balances: HashMap4::with_capacity_and_hasher(balance_capacity, Default::default()),
            reservation_slab: Vec::with_capacity(2_000_000),
            free_slots: Vec::with_capacity(2_000_000),
            fee_account_deficits: HashMap4::default(),
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
        // Balances HashMap: pre-sizing via `with_balance_capacity`
        // eliminates the directory-doubling rehash spikes during bulk
        // seed (T1's >1 s outliers near the end of a 100K-account seed).
        // Page faults still happen lazily on first-touch — that cost is
        // spread per-insert rather than concentrated in rehash bursts,
        // so it no longer shows up as a discrete tail event. A future
        // change could prefault the balance pages too if we want to
        // shave seed wall time further.
    }

    /// Reconstruct from snapshot data. Returns `(manager, slot_assignments)`
    /// where `slot_assignments` maps each `(AccountId, OrderId)` to its
    /// `ReservationSlot` so the caller can inject them into order books.
    #[allow(clippy::type_complexity)]
    pub(crate) fn from_parts(
        balance_entries: Vec<((AccountId, CurrencyId), Balance)>,
        reservations: Vec<(OrderId, AccountId, CurrencyId, u64)>,
        fee_deficits: Vec<(CurrencyId, u64)>,
    ) -> (Self, Vec<((AccountId, OrderId), ReservationSlot)>) {
        // Build balance HashMap4 directly from sparse entries (4-entry buckets for hot path).
        let mut balances =
            HashMap4::with_capacity_and_hasher(balance_entries.len(), Default::default());
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

        let mut fee_account_deficits =
            HashMap4::with_capacity_and_hasher(fee_deficits.len(), Default::default());
        for (ccy, amt) in fee_deficits {
            if amt != 0 {
                fee_account_deficits.insert(ccy, amt);
            }
        }

        let mgr = Self {
            balances,
            reservation_slab: slab,
            free_slots: Vec::new(),
            fee_account_deficits,
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

    /// Snapshot non-zero fee-account deficits for serialization.
    pub(crate) fn snapshot_fee_deficits(&self) -> Vec<(CurrencyId, u64)> {
        self.fee_account_deficits
            .iter()
            .filter(|&(_, &v)| v != 0)
            .map(|(&k, &v)| (k, v))
            .collect()
    }

    /// Signed fee-account balance for a currency: `available - deficit`.
    /// Operators monitor this to know whether the fee account is in the
    /// red (negative) and needs funding. Returns 0 when no entry exists.
    pub fn fee_signed_balance(&self, currency: CurrencyId) -> i128 {
        let avail = self
            .balances
            .get(&(FEE_ACCOUNT, currency))
            .map(|b| b.available)
            .unwrap_or(0) as i128;
        let deficit = self
            .fee_account_deficits
            .get(&currency)
            .copied()
            .unwrap_or(0) as i128;
        avail - deficit
    }

    /// Current fee-account deficit for a currency (0 if not in deficit).
    /// Exposed for snapshot validation and operator monitoring.
    pub fn fee_account_deficit(&self, currency: CurrencyId) -> u64 {
        self.fee_account_deficits
            .get(&currency)
            .copied()
            .unwrap_or(0)
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
        // Never evict FEE_ACCOUNT — it must always exist for fee crediting.
        if bal.is_zero() && account != FEE_ACCOUNT {
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

    /// Read the total (`available + reserved`) for an `(account, currency)`
    /// pair, treating a missing entry as zero. Used by the fill conservation
    /// check; returns `u64` (Copy) so the caller doesn't hold a borrow.
    fn balance_total(&self, account: AccountId, currency: CurrencyId) -> u64 {
        self.balances
            .get(&(account, currency))
            .map(|b| b.total())
            .unwrap_or(0)
    }

    /// Signed contribution of an `(account, currency)` pair to system
    /// totals. For trader accounts this is just `available + reserved`
    /// (always ≥ 0). For `FEE_ACCOUNT` it is `total - deficit` and may go
    /// negative. The fill conservation check sums these across the three
    /// accounts touched per fill.
    fn account_signed_total(&self, account: AccountId, currency: CurrencyId) -> i128 {
        let unsigned = self.balance_total(account, currency) as i128;
        if account == FEE_ACCOUNT {
            unsigned - self.fee_account_deficit(currency) as i128
        } else {
            unsigned
        }
    }

    /// Update balances after a fill. Called once per `ExecutionReport::Fill`.
    ///
    /// The buyer's reserved quote decreases by `cost + buyer_fee`, available
    /// base increases by `quantity`. The seller's reserved base decreases by
    /// `quantity`, available quote increases by `cost - seller_fee`.
    ///
    /// Takes `ReservationSlot` handles for O(1) slab access (no hashing).
    ///
    /// Arithmetic on the balance fields uses `checked_*` operations: an
    /// overflow or underflow logs a structured `error!` (with op, account,
    /// currency, and operand context) and falls back to 0 / `u64::MAX`. A
    /// post-fill conservation check verifies that total quote across
    /// {buyer, seller, fee account} and total base across {buyer, seller}
    /// are unchanged; a violation is logged for forensic analysis.
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
        // cost (market buys). Assert in debug builds; clamp + log in
        // release as a defensive fallback.
        let cost = (price.get() as u128) * (quantity.get() as u128);
        debug_assert!(
            cost <= u64::MAX as u128,
            "fill cost overflows u64: {cost} = {} × {}",
            price.get(),
            quantity.get()
        );
        let cost_u64 = match u64::try_from(cost) {
            Ok(v) => v,
            Err(_) => {
                tracing::error!(
                    cost = %cost,
                    price = price.get(),
                    quantity = quantity.get(),
                    "fill cost overflows u64 — clamping; balance conservation will be violated"
                );
                u64::MAX
            }
        };
        let qty = quantity.get();

        // Resolve accounts up front so we can snapshot pre-fill totals for
        // the conservation check. Both reservations are read-only here.
        let buyer_account = self.reservation_slab[buyer_slot.0 as usize].account;
        let seller_account = self.reservation_slab[seller_slot.0 as usize].account;

        // Account aliasing: STP normally prevents buyer == seller, but the
        // AccountManager layer accepts it (unit-tested directly). FEE_ACCOUNT
        // is reserved and traders should not collide with it. Dedup before
        // summing so the conservation check below doesn't double-count a
        // shared balance.
        let seller_distinct = seller_account != buyer_account;
        let fee_distinct = FEE_ACCOUNT != buyer_account && FEE_ACCOUNT != seller_account;

        // Pre-fill conservation snapshot. Signed (i128) because the fee
        // account is a signed ledger (`available - deficit`); a rebate
        // that drains it pushes the contribution negative.
        let pre_total_quote = {
            let mut t = self.account_signed_total(buyer_account, spec.quote);
            if seller_distinct {
                t += self.account_signed_total(seller_account, spec.quote);
            }
            if fee_distinct {
                t += self.account_signed_total(FEE_ACCOUNT, spec.quote);
            }
            t
        };
        let pre_total_base = {
            // Base never touches the fee account, so unsigned totals are
            // sufficient — but we use i128 for consistency with quote.
            let mut t = self.balance_total(buyer_account, spec.base) as i128;
            if seller_distinct {
                t += self.balance_total(seller_account, spec.base) as i128;
            }
            t
        };

        // Buyer: reserved quote decreases by cost + fee, available base increases.
        // The reservation includes a fee cushion (reserved at placement time
        // with max_fee_bps), so cost + fee normally fits within the reservation.
        // If the fee schedule changed after order placement, the cushion may
        // be insufficient — checked_sub clamps to zero and logs, and the
        // actual deducted amount is tracked for fee account crediting below.
        //
        // Signed fees: positive = fee deducted, negative = rebate credited.
        let buyer_actual_deducted;
        {
            let res = &mut self.reservation_slab[buyer_slot.0 as usize];
            let total_deduct_i128 = cost_u64 as i128 + buyer_fee as i128;
            let total_deduct =
                u64::try_from(total_deduct_i128.clamp(0, u64::MAX as i128)).unwrap_or(0);
            debug_assert!(
                total_deduct <= res.remaining,
                "fill deduction {total_deduct} exceeds reservation {remaining} \
                 (cost={cost_u64}, buyer_fee={buyer_fee}): fee cushion insufficient — \
                 likely a fee schedule change after order placement or a \
                 market buy budget that didn't account for fees",
                remaining = res.remaining,
            );
            // Track actual deduction (may be less than requested if checked_sub clamps).
            let old_remaining = res.remaining;
            res.remaining = res.remaining.checked_sub(total_deduct).unwrap_or_else(|| {
                log_underflow(
                    "buyer.reservation.remaining",
                    res.account,
                    res.currency,
                    res.remaining,
                    total_deduct,
                )
            });
            buyer_actual_deducted = old_remaining - res.remaining;

            let quote_bal = self
                .balances
                .entry((buyer_account, spec.quote))
                .or_default();
            // Use actual deducted amount (not total_deduct) so the aggregate
            // reserved balance stays consistent with individual slot totals.
            // When the fee schedule changed after placement, total_deduct may
            // exceed this slot's remaining, and the excess would eat into
            // other slots' share of the aggregate.
            quote_bal.reserved = quote_bal
                .reserved
                .checked_sub(buyer_actual_deducted)
                .unwrap_or_else(|| {
                    log_underflow(
                        "buyer.quote.reserved",
                        buyer_account,
                        spec.quote,
                        quote_bal.reserved,
                        buyer_actual_deducted,
                    )
                });

            let base_bal = self.balances.entry((buyer_account, spec.base)).or_default();
            base_bal.available = base_bal.available.checked_add(qty).unwrap_or_else(|| {
                log_overflow(
                    "buyer.base.available",
                    buyer_account,
                    spec.base,
                    base_bal.available,
                    qty,
                )
            });
        }

        // Seller: reserved base decreases, available quote increases by cost - fee.
        // Signed: cost - positive_fee = less proceeds; cost - negative_fee = more proceeds (rebate).
        let seller_actual_proceeds;
        {
            let res = &mut self.reservation_slab[seller_slot.0 as usize];
            res.remaining = res.remaining.checked_sub(qty).unwrap_or_else(|| {
                log_underflow(
                    "seller.reservation.remaining",
                    res.account,
                    res.currency,
                    res.remaining,
                    qty,
                )
            });

            let base_bal = self
                .balances
                .entry((seller_account, spec.base))
                .or_default();
            base_bal.reserved = base_bal.reserved.checked_sub(qty).unwrap_or_else(|| {
                log_underflow(
                    "seller.base.reserved",
                    seller_account,
                    spec.base,
                    base_bal.reserved,
                    qty,
                )
            });

            let quote_bal = self
                .balances
                .entry((seller_account, spec.quote))
                .or_default();
            let proceeds_i128 = cost_u64 as i128 - seller_fee as i128;
            let proceeds = u64::try_from(proceeds_i128.clamp(0, u64::MAX as i128)).unwrap_or(0);
            quote_bal.available = quote_bal
                .available
                .checked_add(proceeds)
                .unwrap_or_else(|| {
                    log_overflow(
                        "seller.quote.available",
                        seller_account,
                        spec.quote,
                        quote_bal.available,
                        proceeds,
                    )
                });
            seller_actual_proceeds = proceeds;
        }

        // Credit fees to the fee collection account. The fee credited is
        // computed from actual balance movements to maintain conservation:
        //   fee_credit = buyer_deducted - seller_proceeds
        // This equals buyer_fee + seller_fee when reservations have
        // sufficient cushion, but is naturally capped when checked_* clamps
        // (e.g., fee schedule changed after order placement).
        //
        // The fee account is a signed ledger: its logical balance is
        // `available - deficit`. Fee revenue pays down any outstanding
        // deficit first; rebates that exceed `available` drain it to zero
        // and push the overage onto deficit (so the rebate is paid in
        // full and the operator's debt is recorded). This matches how
        // every other venue treats fee accounting (operator P&L, not a
        // user-facing balance) and prevents the previous failure mode
        // where saturating_sub silently shortchanged the trader.
        let fee_credit = buyer_actual_deducted as i128 - seller_actual_proceeds as i128;
        if fee_credit > 0 {
            let mut amount = u64::try_from(fee_credit).unwrap_or(u64::MAX);
            // Pay down deficit first.
            if let Some(deficit) = self.fee_account_deficits.get_mut(&spec.quote) {
                let pay = (*deficit).min(amount);
                *deficit -= pay;
                amount -= pay;
                if *deficit == 0 {
                    self.fee_account_deficits.remove(&spec.quote);
                }
            }
            // Remainder credits available.
            if amount > 0 {
                let fee_bal = self.balances.entry((FEE_ACCOUNT, spec.quote)).or_default();
                fee_bal.available = fee_bal.available.checked_add(amount).unwrap_or_else(|| {
                    log_overflow(
                        "fee.quote.available",
                        FEE_ACCOUNT,
                        spec.quote,
                        fee_bal.available,
                        amount,
                    )
                });
            }
        } else if fee_credit < 0 {
            // Net rebate: drain available, push overage onto deficit.
            let mut rebate = u64::try_from(-fee_credit).unwrap_or(u64::MAX);
            let fee_bal = self.balances.entry((FEE_ACCOUNT, spec.quote)).or_default();
            let from_avail = fee_bal.available.min(rebate);
            fee_bal.available -= from_avail;
            rebate -= from_avail;
            if rebate > 0 {
                let deficit = self.fee_account_deficits.entry(spec.quote).or_insert(0);
                *deficit = deficit.checked_add(rebate).unwrap_or_else(|| {
                    log_overflow(
                        "fee.quote.deficit",
                        FEE_ACCOUNT,
                        spec.quote,
                        *deficit,
                        rebate,
                    )
                });
            }
        }

        // Post-fill conservation check. Signed total quote across
        // {buyer, seller, fee} and total base across {buyer, seller} must
        // be unchanged. A violation means a saturating fallback in one of
        // the checked_* calls above fired (e.g., reservation underflow
        // from a fee schedule change), or the cost_u64 clamp lost
        // precision, or a future bug introduced a balance leak. The
        // rebate path no longer violates conservation — it pushes the
        // overage onto the fee deficit (signed ledger, accounted for in
        // `account_signed_total`).
        //
        // Cost: up to 5 HashMap reads (fewer when accounts collide), all
        // cache-warm because `entry()` just mutated the same buckets. At
        // ~20-50 ns per `HashMap4` lookup this adds ~100-250 ns to fills,
        // which is meaningful next to the ~100 ns/order target. If a future
        // profiling pass shows fills dominating the budget, gate this
        // block behind `cfg(debug_assertions)` or a feature flag — the
        // checked_* calls above already log on individual saturations.
        let post_total_quote = {
            let mut t = self.account_signed_total(buyer_account, spec.quote);
            if seller_distinct {
                t += self.account_signed_total(seller_account, spec.quote);
            }
            if fee_distinct {
                t += self.account_signed_total(FEE_ACCOUNT, spec.quote);
            }
            t
        };
        let post_total_base = {
            let mut t = self.balance_total(buyer_account, spec.base) as i128;
            if seller_distinct {
                t += self.balance_total(seller_account, spec.base) as i128;
            }
            t
        };

        if post_total_quote != pre_total_quote || post_total_base != pre_total_base {
            tracing::error!(
                buyer_account = buyer_account.0,
                seller_account = seller_account.0,
                base = spec.base.0,
                quote = spec.quote.0,
                price = price.get(),
                quantity = qty,
                buyer_fee,
                seller_fee,
                pre_total_quote = %pre_total_quote,
                post_total_quote = %post_total_quote,
                pre_total_base = %pre_total_base,
                post_total_base = %post_total_base,
                "balance conservation violated in fill — total quote or base changed across (buyer, seller, fee)"
            );
            debug_assert_eq!(
                post_total_quote, pre_total_quote,
                "balance conservation: quote total changed in fill"
            );
            debug_assert_eq!(
                post_total_base, pre_total_base,
                "balance conservation: base total changed in fill"
            );
        }

        // Note: reservation cleanup (free_slot when remaining == 0) is
        // handled by Exchange after fill, not here.
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

    /// Read the remaining reserved amount for a slot. Used after `fill()`
    /// to check if the reservation is fully consumed (remaining == 0).
    pub fn reservation_remaining(&self, slot: ReservationSlot) -> u64 {
        self.reservation_slab[slot.0 as usize].remaining
    }

    /// Free a reservation slot without releasing funds. Used when the
    /// reservation was already fully consumed by `fill()` (remaining == 0).
    pub fn free_slot(&mut self, slot: ReservationSlot) {
        self.free_slots.push(slot.0);
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
    /// Collect all non-zero balances for an account into a fixed-size array.
    ///
    /// Returns `(balances, count)` where `count` is the number of valid entries
    /// (capped at 16). O(n) scan of the sparse balance map — acceptable for
    /// query operations that flow through the pipeline (not on the order hot path).
    pub fn balances_for(&self, account: AccountId) -> ([(CurrencyId, u64, u64); 16], u8) {
        let mut result = [(CurrencyId(0), 0u64, 0u64); 16];
        let mut count: u8 = 0;
        for (&(acct, currency), balance) in self.balances.iter() {
            if acct == account && !balance.is_zero() && (count as usize) < 16 {
                result[count as usize] = (currency, balance.available, balance.reserved);
                count += 1;
            }
        }
        (result, count)
    }

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
            expiry_ns: 0,
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
            expiry_ns: 0,
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
            expiry_ns: 0,
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
            expiry_ns: 0,
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
        let (restored, _) = AccountManager::from_parts(snap, Vec::new(), Vec::new());

        assert_eq!(restored.balance(ACCT_A, USD).available, 1_000);
        assert_eq!(restored.balance(ACCT_B, BTC).available, 500);
        assert_eq!(restored.balance(ACCT_A, BTC), Balance::default());
    }

    /// FEE_ACCOUNT entry persists in the balances map even when its balance
    /// reaches zero. Regular accounts are evicted (removed from map) at zero,
    /// but FEE_ACCOUNT keeps its entry so fee credits can use `entry()`.
    #[test]
    fn fee_account_not_evicted_at_zero_balance() {
        let mut mgr = AccountManager::new();
        mgr.deposit(FEE_ACCOUNT, USD, 100);

        // Withdraw to zero — entry should remain in map.
        mgr.withdraw(FEE_ACCOUNT, USD, 100).unwrap();
        assert_eq!(mgr.balance(FEE_ACCOUNT, USD).available, 0);

        // FEE_ACCOUNT's entry still exists: withdrawing 0 more should NOT
        // return UnknownAccount (it would if the entry were evicted).
        // A regular account at zero IS evicted and returns UnknownAccount.
        mgr.deposit(ACCT_A, USD, 100);
        mgr.withdraw(ACCT_A, USD, 100).unwrap();
        let regular_result = mgr.withdraw(ACCT_A, USD, 1);
        assert_eq!(
            regular_result,
            Err(RejectReason::UnknownAccount),
            "regular account should be evicted at zero"
        );

        let fee_result = mgr.withdraw(FEE_ACCOUNT, USD, 1);
        assert_eq!(
            fee_result,
            Err(RejectReason::InsufficientBalance),
            "FEE_ACCOUNT should still exist (InsufficientBalance, not UnknownAccount)"
        );

        // FEE_ACCOUNT can receive new deposits/credits after hitting zero.
        mgr.deposit(FEE_ACCOUNT, USD, 50);
        assert_eq!(mgr.balance(FEE_ACCOUNT, USD).available, 50);
    }

    // -- Conservation invariant (SEC-07 follow-up) --
    //
    // The fill path runs a post-mutation check that total quote across
    // {buyer, seller, fee} and total base across {buyer, seller} are
    // unchanged. The proptests cover this indirectly; the tests below
    // exercise it explicitly, including the dedup branches for
    // buyer == seller and trader == FEE_ACCOUNT collisions. If the check
    // fails, `debug_assert_eq!` in `fill()` will panic in test builds.

    /// Helper: returns (signed total quote across {buyer, seller, fee},
    /// total base across {buyer, seller}) — matching what `fill()` snapshots.
    /// Quote is signed because the fee account contribution is
    /// `available - deficit` and may go negative.
    fn conserved_totals(mgr: &AccountManager, buyer: AccountId, seller: AccountId) -> (i128, i128) {
        let mut q = mgr.account_signed_total(buyer, USD);
        if seller != buyer {
            q += mgr.account_signed_total(seller, USD);
        }
        if FEE_ACCOUNT != buyer && FEE_ACCOUNT != seller {
            q += mgr.account_signed_total(FEE_ACCOUNT, USD);
        }
        let mut b = mgr.balance(buyer, BTC).total() as i128;
        if seller != buyer {
            b += mgr.balance(seller, BTC).total() as i128;
        }
        (q, b)
    }

    #[test]
    fn fill_conserves_totals_with_fees() {
        let mut mgr = AccountManager::new();
        mgr.deposit(ACCT_A, USD, 10_000);
        mgr.deposit(ACCT_B, BTC, 100);
        mgr.deposit(FEE_ACCOUNT, USD, 0); // present so dedup branches see it

        let buy = limit_buy(1, ACCT_A, 100, 10);
        let sell = limit_sell(2, ACCT_B, 100, 10);
        let (_, bs) = mgr.try_reserve(&buy, &spec(), 50).unwrap();
        let (_, ss) = mgr.try_reserve(&sell, &spec(), 50).unwrap();

        let (pre_q, pre_b) = conserved_totals(&mgr, ACCT_A, ACCT_B);
        // 5 bps each side at price 100 × qty 10 = 1000 → fee 5/side.
        mgr.fill(bs, ss, price(100), qty(10), 5, 5, &spec());
        let (post_q, post_b) = conserved_totals(&mgr, ACCT_A, ACCT_B);

        assert_eq!(post_q, pre_q, "quote conservation across buyer+seller+fee");
        assert_eq!(post_b, pre_b, "base conservation across buyer+seller");
        // Fee account credited the net.
        assert_eq!(mgr.balance(FEE_ACCOUNT, USD).available, 10);
    }

    #[test]
    fn fill_conserves_totals_with_rebate() {
        // Negative fees: rebates funded from FEE_ACCOUNT; total quote across
        // {buyer, seller, fee} must still hold (fee account loses what
        // traders gain).
        let mut mgr = AccountManager::new();
        mgr.deposit(ACCT_A, USD, 10_000);
        mgr.deposit(ACCT_B, BTC, 100);
        mgr.deposit(FEE_ACCOUNT, USD, 1_000); // rebate funding

        let buy = limit_buy(1, ACCT_A, 100, 10);
        let sell = limit_sell(2, ACCT_B, 100, 10);
        let (_, bs) = mgr.try_reserve(&buy, &spec(), 0).unwrap();
        let (_, ss) = mgr.try_reserve(&sell, &spec(), 0).unwrap();

        let (pre_q, pre_b) = conserved_totals(&mgr, ACCT_A, ACCT_B);
        mgr.fill(bs, ss, price(100), qty(10), -3, -2, &spec());
        let (post_q, post_b) = conserved_totals(&mgr, ACCT_A, ACCT_B);

        assert_eq!(post_q, pre_q, "quote conservation under net rebate");
        assert_eq!(post_b, pre_b, "base conservation under net rebate");
        // Fee account paid out the net rebate (5).
        assert_eq!(mgr.balance(FEE_ACCOUNT, USD).available, 995);
    }

    #[test]
    fn fill_conserves_totals_self_trade() {
        // buyer == seller: the dedup branch must not double-count, and
        // conservation must still hold (zero-sum on the trader, with the
        // fee account absorbing the net fee).
        let mut mgr = AccountManager::new();
        mgr.deposit(ACCT_A, USD, 10_000);
        mgr.deposit(ACCT_A, BTC, 100);

        let buy = limit_buy(1, ACCT_A, 100, 10);
        let sell = limit_sell(2, ACCT_A, 100, 10);
        let (_, bs) = mgr.try_reserve(&buy, &spec(), 50).unwrap();
        let (_, ss) = mgr.try_reserve(&sell, &spec(), 50).unwrap();

        let (pre_q, pre_b) = conserved_totals(&mgr, ACCT_A, ACCT_A);
        mgr.fill(bs, ss, price(100), qty(10), 5, 5, &spec());
        let (post_q, post_b) = conserved_totals(&mgr, ACCT_A, ACCT_A);

        assert_eq!(post_q, pre_q, "quote conservation under self-trade");
        assert_eq!(post_b, pre_b, "base conservation under self-trade");
        assert_eq!(mgr.balance(FEE_ACCOUNT, USD).available, 10);
    }

    #[test]
    fn fill_conserves_totals_when_trader_is_fee_account() {
        // Pathological: a trader collides with FEE_ACCOUNT. Nothing in
        // `fill()` itself prevents this, and the dedup branch
        // `fee_distinct = false` must keep the conservation check correct
        // by counting the shared balance only once.
        let mut mgr = AccountManager::new();
        mgr.deposit(FEE_ACCOUNT, USD, 10_000); // FEE_ACCOUNT acting as buyer
        mgr.deposit(ACCT_B, BTC, 100);

        let buy = limit_buy(1, FEE_ACCOUNT, 100, 10);
        let sell = limit_sell(2, ACCT_B, 100, 10);
        let (_, bs) = mgr.try_reserve(&buy, &spec(), 50).unwrap();
        let (_, ss) = mgr.try_reserve(&sell, &spec(), 50).unwrap();

        let (pre_q, pre_b) = conserved_totals(&mgr, FEE_ACCOUNT, ACCT_B);
        mgr.fill(bs, ss, price(100), qty(10), 5, 5, &spec());
        let (post_q, post_b) = conserved_totals(&mgr, FEE_ACCOUNT, ACCT_B);

        assert_eq!(
            post_q, pre_q,
            "quote conservation when buyer collides with FEE_ACCOUNT"
        );
        assert_eq!(
            post_b, pre_b,
            "base conservation under FEE_ACCOUNT collision"
        );
    }

    // -- Fee-account signed ledger (D) --
    //
    // Rebates that exceed the fee account's `available` push the overage
    // onto a per-currency deficit. Subsequent fee revenue (and direct
    // pay-down) reduces the deficit before crediting `available`. The
    // logical balance reported to operators is the signed `available -
    // deficit`. Conservation must hold across the signed ledger.

    #[test]
    fn rebate_exceeding_available_accumulates_deficit() {
        // Fee account starts at 5; a 10-USD net rebate drains it to 0
        // and pushes 5 onto deficit. Trader receives the full rebate.
        let mut mgr = AccountManager::new();
        mgr.deposit(ACCT_A, USD, 10_000);
        mgr.deposit(ACCT_B, BTC, 100);
        mgr.deposit(FEE_ACCOUNT, USD, 5);

        let buy = limit_buy(1, ACCT_A, 100, 10);
        let sell = limit_sell(2, ACCT_B, 100, 10);
        let (_, bs) = mgr.try_reserve(&buy, &spec(), 0).unwrap();
        let (_, ss) = mgr.try_reserve(&sell, &spec(), 0).unwrap();

        let (pre_q, pre_b) = conserved_totals(&mgr, ACCT_A, ACCT_B);
        // -7 / -3 → buyer rebated 7, seller rebated 3, net rebate 10.
        mgr.fill(bs, ss, price(100), qty(10), -7, -3, &spec());
        let (post_q, post_b) = conserved_totals(&mgr, ACCT_A, ACCT_B);

        assert_eq!(
            mgr.balance(FEE_ACCOUNT, USD).available,
            0,
            "fee available drained"
        );
        assert_eq!(mgr.fee_account_deficit(USD), 5, "deficit absorbs overage");
        assert_eq!(
            mgr.fee_signed_balance(USD),
            -5,
            "signed ledger reads negative"
        );
        // Trader-side (reservation cleanup is the Exchange's job, so the
        // buyer's reservation slot retains the 7-USD rebate amount that
        // wasn't deducted): ACCT_A USD = 9_000 available + 7 reserved =
        // 10_007 total. ACCT_B received 1000 cost + 3 rebate = 1003.
        assert_eq!(mgr.balance(ACCT_A, USD).available, 9_000);
        assert_eq!(mgr.balance(ACCT_A, USD).reserved, 7);
        assert_eq!(mgr.balance(ACCT_B, USD).available, 1_003);
        // Signed conservation: pre 10_000 + 0 + 5 = 10_005;
        //                     post (9_000+7) + 1_003 + (0 − 5) = 10_005.
        assert_eq!(post_q, pre_q, "signed conservation under rebate underflow");
        assert_eq!(post_b, pre_b);
    }

    #[test]
    fn fee_credit_pays_down_deficit_first() {
        // After a rebate underflow leaves a 5-USD deficit, a subsequent
        // fee-paying fill must reduce the deficit before crediting
        // `available`.
        let mut mgr = AccountManager::new();
        mgr.deposit(ACCT_A, USD, 100_000);
        mgr.deposit(ACCT_B, BTC, 100);
        mgr.deposit(FEE_ACCOUNT, USD, 5);

        // Rebate fill: leaves deficit=5, available=0.
        let buy1 = limit_buy(1, ACCT_A, 100, 10);
        let sell1 = limit_sell(2, ACCT_B, 100, 10);
        let (_, bs1) = mgr.try_reserve(&buy1, &spec(), 0).unwrap();
        let (_, ss1) = mgr.try_reserve(&sell1, &spec(), 0).unwrap();
        mgr.fill(bs1, ss1, price(100), qty(10), -7, -3, &spec());
        assert_eq!(mgr.fee_account_deficit(USD), 5);

        // Fee-paying fill: net fee of 12 should pay down the 5 deficit
        // first, then credit 7 to available. max_fee_bps=100 (1%) gives
        // a 10-USD cushion on the 1000-USD notional, comfortably above
        // the 7-USD buyer fee at fill time.
        let buy2 = limit_buy(3, ACCT_A, 100, 10);
        let sell2 = limit_sell(4, ACCT_B, 100, 10);
        let (_, bs2) = mgr.try_reserve(&buy2, &spec(), 100).unwrap();
        let (_, ss2) = mgr.try_reserve(&sell2, &spec(), 100).unwrap();

        let (pre_q, pre_b) = conserved_totals(&mgr, ACCT_A, ACCT_B);
        mgr.fill(bs2, ss2, price(100), qty(10), 7, 5, &spec());
        let (post_q, post_b) = conserved_totals(&mgr, ACCT_A, ACCT_B);

        assert_eq!(mgr.fee_account_deficit(USD), 0, "deficit cleared");
        assert_eq!(
            mgr.balance(FEE_ACCOUNT, USD).available,
            7,
            "remainder credits available"
        );
        assert_eq!(mgr.fee_signed_balance(USD), 7);
        assert_eq!(post_q, pre_q);
        assert_eq!(post_b, pre_b);
    }

    #[test]
    fn rebate_with_empty_fee_account_records_full_deficit() {
        // Edge case: fee account is completely empty (no entry) when a
        // rebate fill arrives. The full rebate accumulates as deficit.
        let mut mgr = AccountManager::new();
        mgr.deposit(ACCT_A, USD, 10_000);
        mgr.deposit(ACCT_B, BTC, 100);

        let buy = limit_buy(1, ACCT_A, 100, 10);
        let sell = limit_sell(2, ACCT_B, 100, 10);
        let (_, bs) = mgr.try_reserve(&buy, &spec(), 0).unwrap();
        let (_, ss) = mgr.try_reserve(&sell, &spec(), 0).unwrap();

        let (pre_q, pre_b) = conserved_totals(&mgr, ACCT_A, ACCT_B);
        mgr.fill(bs, ss, price(100), qty(10), -3, -2, &spec());
        let (post_q, post_b) = conserved_totals(&mgr, ACCT_A, ACCT_B);

        assert_eq!(mgr.fee_account_deficit(USD), 5);
        assert_eq!(mgr.fee_signed_balance(USD), -5);
        assert_eq!(post_q, pre_q);
        assert_eq!(post_b, pre_b);
    }

    #[test]
    fn fee_signed_balance_zero_when_no_activity() {
        let mgr = AccountManager::new();
        assert_eq!(mgr.fee_signed_balance(USD), 0);
        assert_eq!(mgr.fee_account_deficit(USD), 0);
    }

    #[test]
    fn fee_deficits_round_trip_via_snapshot() {
        // Build a manager with a non-zero deficit, snapshot it, restore,
        // and confirm the deficit survived.
        let mut mgr = AccountManager::new();
        mgr.deposit(ACCT_A, USD, 10_000);
        mgr.deposit(ACCT_B, BTC, 100);

        let buy = limit_buy(1, ACCT_A, 100, 10);
        let sell = limit_sell(2, ACCT_B, 100, 10);
        let (_, bs) = mgr.try_reserve(&buy, &spec(), 0).unwrap();
        let (_, ss) = mgr.try_reserve(&sell, &spec(), 0).unwrap();
        mgr.fill(bs, ss, price(100), qty(10), -3, -2, &spec());
        assert_eq!(mgr.fee_account_deficit(USD), 5);

        let balances = mgr.snapshot_balances();
        let deficits = mgr.snapshot_fee_deficits();
        assert_eq!(deficits, vec![(USD, 5)]);
        let (restored, _) = AccountManager::from_parts(balances, Vec::new(), deficits);
        assert_eq!(restored.fee_account_deficit(USD), 5);
        assert_eq!(restored.fee_signed_balance(USD), -5);
    }

    // Property-based test: across any sequence of fills with random fees
    // (positive, zero, or negative), conservation holds and the fee
    // ledger's signed balance equals the cumulative `fee_credit` per fill.
    use proptest::prelude::*;

    #[derive(Debug, Clone, Copy)]
    struct FillEvent {
        price: u64,
        quantity: u64,
        buyer_fee: i32,
        seller_fee: i32,
    }

    fn arb_fill_event() -> impl Strategy<Value = FillEvent> {
        // Cost ∈ [price × qty] ≥ 100 × 10 = 1_000. With max_fee_bps=500
        // (5%) the cushion is `cost × 500 / 10_000 ≥ 50`, which absorbs
        // every fee in [-50, 50]. This keeps the proptest focused on
        // fee-ledger semantics, not the SEC-07 saturation paths
        // (covered by unit tests with deliberate underflow).
        (100u64..=1_000, 10u64..=100, -50i32..=50, -50i32..=50).prop_map(
            |(price, quantity, buyer_fee, seller_fee)| FillEvent {
                price,
                quantity,
                buyer_fee,
                seller_fee,
            },
        )
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(200))]

        /// For any sequence of fills with random fees:
        /// 1. Conservation: `Σ ACCT_A.total + Σ ACCT_B.total + signed_fee` is
        ///    unchanged from initial deposits (debug_assert in `fill()` also
        ///    catches per-fill violations).
        /// 2. The fee account's signed balance equals the cumulative
        ///    `buyer_fee + seller_fee` across all fills (since the fee
        ///    cushion is generous enough that no saturation occurs).
        /// 3. Deficit and available are both u64 (never negative); only the
        ///    *signed* balance can be negative.
        #[test]
        fn fee_ledger_conserves_under_random_fees(events in proptest::collection::vec(arb_fill_event(), 1..=30)) {
            let mut mgr = AccountManager::new();
            // Pre-fund both traders generously so reservations always succeed.
            // ACCT_A as recurring buyer needs quote (USD); ACCT_B as recurring
            // seller needs base (BTC).
            mgr.deposit(ACCT_A, USD, 10_000_000);
            mgr.deposit(ACCT_B, BTC, 100_000);
            // Pre-deposit some quote on ACCT_B so it can accumulate proceeds
            // (already happens via fill, but seed the entry for consistency).
            mgr.deposit(ACCT_B, USD, 0);

            let initial_signed_quote = mgr.account_signed_total(ACCT_A, USD)
                + mgr.account_signed_total(ACCT_B, USD)
                + mgr.account_signed_total(FEE_ACCOUNT, USD);
            let initial_total_base = mgr.balance(ACCT_A, BTC).total() as i128
                + mgr.balance(ACCT_B, BTC).total() as i128;

            let mut order_id = 1u64;
            let mut expected_fee_credit: i128 = 0;

            for ev in &events {
                let buy = limit_buy(order_id, ACCT_A, ev.price, ev.quantity);
                order_id += 1;
                let sell = limit_sell(order_id, ACCT_B, ev.price, ev.quantity);
                order_id += 1;

                // max_fee_bps=500 (5%) gives a generous cushion: at qty=100
                // price=1_000 cost=100_000, cushion = 5_000, larger than any
                // |fee| in [-50, 50]. So no checked_* saturation fires and
                // the cumulative fee_credit equals Σ(buyer_fee + seller_fee).
                let (_, bs) = match mgr.try_reserve(&buy, &spec(), 500) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let (_, ss) = match mgr.try_reserve(&sell, &spec(), 500) {
                    Ok(v) => v,
                    Err(_) => { mgr.release(bs); continue; }
                };

                mgr.fill(
                    bs,
                    ss,
                    price(ev.price),
                    qty(ev.quantity),
                    ev.buyer_fee as i64,
                    ev.seller_fee as i64,
                    &spec(),
                );
                // Free leftover reservations (rebate leaves residue; fees
                // ≥0 may also leave residue from the cushion).
                mgr.release(bs);
                mgr.release(ss);

                expected_fee_credit += ev.buyer_fee as i128 + ev.seller_fee as i128;
            }

            // (1) Signed quote conservation across the system.
            let final_signed_quote = mgr.account_signed_total(ACCT_A, USD)
                + mgr.account_signed_total(ACCT_B, USD)
                + mgr.account_signed_total(FEE_ACCOUNT, USD);
            prop_assert_eq!(
                final_signed_quote, initial_signed_quote,
                "signed quote conservation violated"
            );

            // Base conservation.
            let final_total_base = mgr.balance(ACCT_A, BTC).total() as i128
                + mgr.balance(ACCT_B, BTC).total() as i128;
            prop_assert_eq!(
                final_total_base, initial_total_base,
                "base conservation violated"
            );

            // (2) Fee signed balance matches cumulative fee_credit. The
            // fee account had no initial deposit, so its starting signed
            // balance was 0.
            prop_assert_eq!(
                mgr.fee_signed_balance(USD), expected_fee_credit,
                "fee signed balance != Σ(fee_credit)"
            );

            // (3) Component invariants: available and deficit are u64,
            // and not both nonzero simultaneously *for this proptest*
            // because we never deposit to FEE_ACCOUNT mid-sequence (a
            // fill either drains available toward deficit or pays
            // deficit toward available, never both nonzero at exit).
            let avail = mgr.balance(FEE_ACCOUNT, USD).available;
            let deficit = mgr.fee_account_deficit(USD);
            prop_assert!(
                !(avail > 0 && deficit > 0),
                "fee account has both available={} and deficit={} simultaneously",
                avail, deficit
            );
        }
    }

    #[test]
    fn zero_deficits_omitted_from_snapshot() {
        // A deficit that gets fully paid down should not appear in the
        // snapshot — the entry is removed when it hits zero.
        let mut mgr = AccountManager::new();
        mgr.deposit(ACCT_A, USD, 100_000);
        mgr.deposit(ACCT_B, BTC, 100);

        // Accumulate then fully pay down.
        let buy1 = limit_buy(1, ACCT_A, 100, 10);
        let sell1 = limit_sell(2, ACCT_B, 100, 10);
        let (_, bs1) = mgr.try_reserve(&buy1, &spec(), 0).unwrap();
        let (_, ss1) = mgr.try_reserve(&sell1, &spec(), 0).unwrap();
        mgr.fill(bs1, ss1, price(100), qty(10), -3, -2, &spec());
        assert_eq!(mgr.fee_account_deficit(USD), 5);

        let buy2 = limit_buy(3, ACCT_A, 100, 10);
        let sell2 = limit_sell(4, ACCT_B, 100, 10);
        let (_, bs2) = mgr.try_reserve(&buy2, &spec(), 50).unwrap();
        let (_, ss2) = mgr.try_reserve(&sell2, &spec(), 50).unwrap();
        mgr.fill(bs2, ss2, price(100), qty(10), 3, 2, &spec());

        assert_eq!(mgr.fee_account_deficit(USD), 0);
        assert!(
            mgr.snapshot_fee_deficits().is_empty(),
            "zero entries removed"
        );
    }
}
