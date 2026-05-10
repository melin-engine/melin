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
    /// Reservations are **pure notional** — no fee cushion. Fees are deducted
    /// from the fill's received asset at trade time (buyer pays in base out
    /// of their base credit; seller pays in quote out of their proceeds).
    /// This matches industry practice (CME, Coinbase, Binance) and removes
    /// the SEC-07 fee-schedule-change failure mode entirely: a reservation
    /// can never be insufficient to cover its fill, by construction.
    pub fn try_reserve(
        &mut self,
        order: &Order,
        spec: &InstrumentSpec,
    ) -> Result<(u64, ReservationSlot), RejectReason> {
        let (currency, amount) = self.required_reserve(order, spec)?;

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

    /// Settle one side of a fill: the trader receives `gross` units of
    /// `currency` minus a signed `fee`. Conservation holds within the
    /// (trader, fee account) pair: trader gain + fee account gain ==
    /// `gross`.
    ///
    /// Positive fee → trader pays. The fee is **capped** at `gross` (a
    /// trader cannot lose more than they received in this leg) — the
    /// fee account credits the actual capped amount, not the requested
    /// fee. This is the agreed fee-cap policy.
    ///
    /// Negative fee → trader rebated. The trader receives `gross +
    /// |fee|`; the fee account funds the rebate from `available` first,
    /// accumulating per-currency deficit if drained. The signed fee
    /// ledger absorbs arbitrary rebate magnitudes.
    fn settle_fee_leg(
        &mut self,
        trader: AccountId,
        currency: CurrencyId,
        gross: u64,
        fee: i64,
        op_prefix: &'static str,
    ) {
        if fee >= 0 {
            // Cap the fee at `gross` so the trader's credit never goes
            // negative. The fee account collects the actual (capped)
            // amount, not the requested fee.
            let fee_u64 = fee as u64;
            let actual_fee = fee_u64.min(gross);
            if actual_fee < fee_u64 {
                // Fee exceeded the trader's receipt and was clamped. This
                // is the agreed policy, but if it fires it's almost
                // always a fee-schedule misconfiguration (e.g. bps set
                // to 10000 = 100%) — surface it so operators notice
                // before the revenue gap is large.
                tracing::warn!(
                    op = op_prefix,
                    account = trader.0,
                    currency = currency.0,
                    requested_fee = fee_u64,
                    gross,
                    capped_to = actual_fee,
                    "fee exceeded trader's receipt and was capped — likely a fee schedule misconfiguration"
                );
            }
            let credit = gross - actual_fee;
            if credit > 0 {
                let bal = self.balances.entry((trader, currency)).or_default();
                bal.available = bal.available.checked_add(credit).unwrap_or_else(|| {
                    log_overflow(op_prefix, trader, currency, bal.available, credit)
                });
            }
            self.credit_fee_account(currency, actual_fee);
        } else {
            // Negative fee = rebate. Trader receives gross + |fee|.
            let rebate = (-(fee as i128)) as u64; // |fee|, fits because fee ≥ i64::MIN+1 in practice
            let credit_i128 = gross as i128 + rebate as i128;
            let credit = u64::try_from(credit_i128)
                .unwrap_or_else(|_| log_overflow(op_prefix, trader, currency, gross, rebate));
            let bal = self.balances.entry((trader, currency)).or_default();
            bal.available = bal.available.checked_add(credit).unwrap_or_else(|| {
                log_overflow(op_prefix, trader, currency, bal.available, credit)
            });
            self.debit_fee_account(currency, rebate);
        }
    }

    /// Add `amount` to the fee account's signed balance for `currency`.
    /// Pays down deficit first; remainder credits `available`.
    fn credit_fee_account(&mut self, currency: CurrencyId, amount: u64) {
        if amount == 0 {
            return;
        }
        let mut remaining = amount;
        if let Some(deficit) = self.fee_account_deficits.get_mut(&currency) {
            let pay = (*deficit).min(remaining);
            *deficit -= pay;
            remaining -= pay;
            if *deficit == 0 {
                self.fee_account_deficits.remove(&currency);
            }
        }
        if remaining > 0 {
            let fee_bal = self.balances.entry((FEE_ACCOUNT, currency)).or_default();
            fee_bal.available = fee_bal.available.checked_add(remaining).unwrap_or_else(|| {
                log_overflow(
                    "fee.available",
                    FEE_ACCOUNT,
                    currency,
                    fee_bal.available,
                    remaining,
                )
            });
        }
    }

    /// Subtract `amount` from the fee account's signed balance for
    /// `currency`. Drains `available` first; overage accumulates as
    /// deficit (the fee account is a signed ledger).
    fn debit_fee_account(&mut self, currency: CurrencyId, amount: u64) {
        if amount == 0 {
            return;
        }
        let mut rebate = amount;
        let fee_bal = self.balances.entry((FEE_ACCOUNT, currency)).or_default();
        let from_avail = fee_bal.available.min(rebate);
        fee_bal.available -= from_avail;
        rebate -= from_avail;
        if rebate > 0 {
            let deficit = self.fee_account_deficits.entry(currency).or_insert(0);
            *deficit = deficit.checked_add(rebate).unwrap_or_else(|| {
                log_overflow("fee.deficit", FEE_ACCOUNT, currency, *deficit, rebate)
            });
        }
    }

    /// Update balances after a fill. Called once per `ExecutionReport::Fill`.
    ///
    /// **Fee model (industry-standard).** Reservations lock pure notional:
    /// buyer's quote reservation decreases by `cost = price × quantity`,
    /// seller's base reservation decreases by `quantity`. Fees are charged
    /// in the **received asset**:
    /// - Buyer pays `buyer_base_fee` units of base out of their `quantity`
    ///   credit (capped at `quantity` — fee never exceeds receipt).
    /// - Seller pays `seller_quote_fee` units of quote out of their
    ///   `cost` credit (capped at `cost`).
    ///
    /// Negative fees are rebates: the trader receives extra (the rebate
    /// is added to their credit) and the fee account funds it from
    /// `available`, accumulating deficit if drained. The signed fee
    /// ledger is per-currency, so both quote (seller fees) and base
    /// (buyer fees) are tracked.
    ///
    /// Takes `ReservationSlot` handles for O(1) slab access (no hashing).
    ///
    /// Arithmetic on the balance fields uses `checked_*` operations: an
    /// overflow logs a structured `error!` (with op, account, currency,
    /// and operand context) and falls back to `u64::MAX`. Underflow on
    /// the reservation legs is unreachable by construction now that
    /// reservations carry pure notional (no fee cushion to exhaust). A
    /// post-fill conservation check verifies that signed totals of quote
    /// and base across {buyer, seller, fee account} are unchanged.
    pub fn fill(
        &mut self,
        buyer_slot: ReservationSlot,
        seller_slot: ReservationSlot,
        price: Price,
        quantity: Quantity,
        buyer_base_fee: i64,
        seller_quote_fee: i64,
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
        // account is a signed ledger (`available - deficit`) — and now
        // holds balances in **both** quote (seller fees) and base
        // (buyer fees), so both totals are signed.
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
            let mut t = self.account_signed_total(buyer_account, spec.base);
            if seller_distinct {
                t += self.account_signed_total(seller_account, spec.base);
            }
            if fee_distinct {
                t += self.account_signed_total(FEE_ACCOUNT, spec.base);
            }
            t
        };

        // Buyer reservation: deduct pure cost (no fee). Underflow is
        // unreachable by construction since the reservation was sized at
        // exactly `cost` for limits and at the available quote balance
        // for markets (the matching engine bounds market fills against
        // the reservation budget); checked_sub still logs in case of a
        // bug elsewhere.
        {
            let res = &mut self.reservation_slab[buyer_slot.0 as usize];
            res.remaining = res.remaining.checked_sub(cost_u64).unwrap_or_else(|| {
                log_underflow(
                    "buyer.reservation.remaining",
                    res.account,
                    res.currency,
                    res.remaining,
                    cost_u64,
                )
            });
            let quote_bal = self
                .balances
                .entry((buyer_account, spec.quote))
                .or_default();
            quote_bal.reserved = quote_bal.reserved.checked_sub(cost_u64).unwrap_or_else(|| {
                log_underflow(
                    "buyer.quote.reserved",
                    buyer_account,
                    spec.quote,
                    quote_bal.reserved,
                    cost_u64,
                )
            });
        }

        // Seller reservation: deduct quantity (no fee). Same construction
        // guarantee as above.
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
        }

        // Settle the buyer's leg: credit `qty` base, deduct
        // `buyer_base_fee` from the credit (or pay rebate).
        self.settle_fee_leg(buyer_account, spec.base, qty, buyer_base_fee, "buyer.base");

        // Settle the seller's leg: credit `cost` quote, deduct
        // `seller_quote_fee` from the credit (or pay rebate).
        self.settle_fee_leg(
            seller_account,
            spec.quote,
            cost_u64,
            seller_quote_fee,
            "seller.quote",
        );

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
            let mut t = self.account_signed_total(buyer_account, spec.base);
            if seller_distinct {
                t += self.account_signed_total(seller_account, spec.base);
            }
            if fee_distinct {
                t += self.account_signed_total(FEE_ACCOUNT, spec.base);
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
                buyer_base_fee,
                seller_quote_fee,
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
    ) -> Result<(CurrencyId, u64), RejectReason> {
        match order.side {
            Side::Buy => {
                let currency = spec.quote;
                let amount = match order.order_type {
                    OrderType::Limit { price, .. }
                    | OrderType::StopLimit {
                        limit_price: price, ..
                    } => {
                        // Pure notional: price × quantity in quote currency.
                        // Fees are deducted from the buyer's base credit at
                        // fill time, never from this reservation.
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
        let (reserved, _slot) = mgr.try_reserve(&order, &spec()).unwrap();

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
        let (reserved, _slot) = mgr.try_reserve(&order, &spec()).unwrap();

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
        let (reserved, _slot) = mgr.try_reserve(&order, &spec()).unwrap();

        assert_eq!(reserved, 10_000);
        assert_eq!(mgr.balance(ACCT_A, USD).available, 0);
        assert_eq!(mgr.balance(ACCT_A, USD).reserved, 10_000);
    }

    #[test]
    fn reserve_market_sell_locks_base_quantity() {
        let mut mgr = AccountManager::new();
        mgr.deposit(ACCT_A, BTC, 100);

        let order = market_sell(1, ACCT_A, 30);
        let (reserved, _slot) = mgr.try_reserve(&order, &spec()).unwrap();

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
        let (_amount, slot) = mgr.try_reserve(&order, &spec()).unwrap();
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
        let (_amt, buy_slot) = mgr.try_reserve(&buy, &spec()).unwrap();
        let (_amt, sell_slot) = mgr.try_reserve(&sell, &spec()).unwrap();

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
        let (_amt, buy_slot) = mgr.try_reserve(&buy, &spec()).unwrap();
        let (_amt, sell_slot) = mgr.try_reserve(&sell, &spec()).unwrap();

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
        let (_amt, buy_slot) = mgr.try_reserve(&buy, &spec()).unwrap();
        let (_amt, sell_slot) = mgr.try_reserve(&sell, &spec()).unwrap();

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
        let (_amt, buy_slot) = mgr.try_reserve(&buy, &spec()).unwrap();
        let (_amt, sell_slot) = mgr.try_reserve(&sell, &spec()).unwrap();

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
        let (_amt, buy_slot) = mgr.try_reserve(&buy, &spec()).unwrap(); // reserves all 10_000
        let (_amt, sell_slot) = mgr.try_reserve(&sell, &spec()).unwrap();

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
        let (_amt, buy_slot) = mgr.try_reserve(&buy, &spec()).unwrap();

        // Sell 1: 10 @ 100.
        let sell1 = limit_sell(2, ACCT_B, 100, 10);
        let (_amt, sell1_slot) = mgr.try_reserve(&sell1, &spec()).unwrap();
        mgr.fill(buy_slot, sell1_slot, price(100), qty(10), 0, 0, &spec());

        // Sell 2: 5 @ 150.
        let sell2 = limit_sell(3, ACCT_B, 150, 5);
        let (_amt, sell2_slot) = mgr.try_reserve(&sell2, &spec()).unwrap();
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
        let (_, buy_slot) = mgr.try_reserve(&buy, &spec()).unwrap();
        let (_, sell_slot) = mgr.try_reserve(&sell, &spec()).unwrap();

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

    /// Helper: returns (signed total quote, signed total base) across
    /// {buyer, seller, fee}, matching what `fill()` snapshots. Both are
    /// signed because the fee account is a per-currency signed ledger,
    /// and under A it holds balances in both quote (seller fees) and
    /// base (buyer fees).
    fn conserved_totals(mgr: &AccountManager, buyer: AccountId, seller: AccountId) -> (i128, i128) {
        let mut q = mgr.account_signed_total(buyer, USD);
        let mut b = mgr.account_signed_total(buyer, BTC);
        if seller != buyer {
            q += mgr.account_signed_total(seller, USD);
            b += mgr.account_signed_total(seller, BTC);
        }
        if FEE_ACCOUNT != buyer && FEE_ACCOUNT != seller {
            q += mgr.account_signed_total(FEE_ACCOUNT, USD);
            b += mgr.account_signed_total(FEE_ACCOUNT, BTC);
        }
        (q, b)
    }

    #[test]
    fn fill_conserves_totals_with_fees() {
        // Under A, buyer fee is in base (deducted from BTC credit) and
        // seller fee is in quote (deducted from USD proceeds).
        let mut mgr = AccountManager::new();
        mgr.deposit(ACCT_A, USD, 10_000);
        mgr.deposit(ACCT_B, BTC, 100);

        let buy = limit_buy(1, ACCT_A, 100, 10);
        let sell = limit_sell(2, ACCT_B, 100, 10);
        let (_, bs) = mgr.try_reserve(&buy, &spec()).unwrap();
        let (_, ss) = mgr.try_reserve(&sell, &spec()).unwrap();

        let (pre_q, pre_b) = conserved_totals(&mgr, ACCT_A, ACCT_B);
        // buyer_base_fee=1 BTC, seller_quote_fee=5 USD.
        mgr.fill(bs, ss, price(100), qty(10), 1, 5, &spec());
        let (post_q, post_b) = conserved_totals(&mgr, ACCT_A, ACCT_B);

        assert_eq!(post_q, pre_q, "signed quote conservation");
        assert_eq!(post_b, pre_b, "signed base conservation");
        // Buyer received 9 BTC (10 - 1 fee); seller received 995 USD (1000 - 5).
        assert_eq!(mgr.balance(ACCT_A, BTC).available, 9);
        assert_eq!(mgr.balance(ACCT_B, USD).available, 995);
        // Fee account collected fees in both currencies.
        assert_eq!(mgr.balance(FEE_ACCOUNT, BTC).available, 1);
        assert_eq!(mgr.balance(FEE_ACCOUNT, USD).available, 5);
    }

    #[test]
    fn fill_conserves_totals_with_rebate() {
        // Negative fees: rebates funded from FEE_ACCOUNT (per currency).
        // Under A, the buyer rebate is in base (extra BTC) and the seller
        // rebate is in quote (extra USD).
        let mut mgr = AccountManager::new();
        mgr.deposit(ACCT_A, USD, 10_000);
        mgr.deposit(ACCT_B, BTC, 100);
        mgr.deposit(FEE_ACCOUNT, USD, 1_000);
        mgr.deposit(FEE_ACCOUNT, BTC, 100);

        let buy = limit_buy(1, ACCT_A, 100, 10);
        let sell = limit_sell(2, ACCT_B, 100, 10);
        let (_, bs) = mgr.try_reserve(&buy, &spec()).unwrap();
        let (_, ss) = mgr.try_reserve(&sell, &spec()).unwrap();

        let (pre_q, pre_b) = conserved_totals(&mgr, ACCT_A, ACCT_B);
        // buyer_base_fee = -1 BTC (rebate); seller_quote_fee = -2 USD.
        mgr.fill(bs, ss, price(100), qty(10), -1, -2, &spec());
        let (post_q, post_b) = conserved_totals(&mgr, ACCT_A, ACCT_B);

        assert_eq!(post_q, pre_q, "signed quote conservation under rebate");
        assert_eq!(post_b, pre_b, "signed base conservation under rebate");
        // Buyer received 11 BTC (10 + 1 rebate); seller received 1002 USD.
        assert_eq!(mgr.balance(ACCT_A, BTC).available, 11);
        assert_eq!(mgr.balance(ACCT_B, USD).available, 1_002);
        // Fee account paid out from both ledgers.
        assert_eq!(mgr.balance(FEE_ACCOUNT, BTC).available, 99);
        assert_eq!(mgr.balance(FEE_ACCOUNT, USD).available, 998);
    }

    #[test]
    fn fill_conserves_totals_self_trade() {
        // buyer == seller: the dedup branch must not double-count, and
        // conservation must still hold (zero-sum on the trader, with the
        // fee account absorbing the net fee in each currency).
        let mut mgr = AccountManager::new();
        mgr.deposit(ACCT_A, USD, 10_000);
        mgr.deposit(ACCT_A, BTC, 100);

        let buy = limit_buy(1, ACCT_A, 100, 10);
        let sell = limit_sell(2, ACCT_A, 100, 10);
        let (_, bs) = mgr.try_reserve(&buy, &spec()).unwrap();
        let (_, ss) = mgr.try_reserve(&sell, &spec()).unwrap();

        let (pre_q, pre_b) = conserved_totals(&mgr, ACCT_A, ACCT_A);
        mgr.fill(bs, ss, price(100), qty(10), 1, 5, &spec());
        let (post_q, post_b) = conserved_totals(&mgr, ACCT_A, ACCT_A);

        assert_eq!(post_q, pre_q, "quote conservation under self-trade");
        assert_eq!(post_b, pre_b, "base conservation under self-trade");
        assert_eq!(mgr.balance(FEE_ACCOUNT, BTC).available, 1);
        assert_eq!(mgr.balance(FEE_ACCOUNT, USD).available, 5);
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
        let (_, bs) = mgr.try_reserve(&buy, &spec()).unwrap();
        let (_, ss) = mgr.try_reserve(&sell, &spec()).unwrap();

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
        // FEE_ACCOUNT.USD starts at 2; a -5 USD seller rebate drains
        // available to 0 and pushes 3 onto the USD deficit.
        let mut mgr = AccountManager::new();
        mgr.deposit(ACCT_A, USD, 10_000);
        mgr.deposit(ACCT_B, BTC, 100);
        mgr.deposit(FEE_ACCOUNT, USD, 2);

        let buy = limit_buy(1, ACCT_A, 100, 10);
        let sell = limit_sell(2, ACCT_B, 100, 10);
        let (_, bs) = mgr.try_reserve(&buy, &spec()).unwrap();
        let (_, ss) = mgr.try_reserve(&sell, &spec()).unwrap();

        let (pre_q, pre_b) = conserved_totals(&mgr, ACCT_A, ACCT_B);
        // buyer_base_fee=0, seller_quote_fee=-5 → 5-USD rebate.
        mgr.fill(bs, ss, price(100), qty(10), 0, -5, &spec());
        let (post_q, post_b) = conserved_totals(&mgr, ACCT_A, ACCT_B);

        assert_eq!(mgr.balance(FEE_ACCOUNT, USD).available, 0);
        assert_eq!(
            mgr.fee_account_deficit(USD),
            3,
            "USD deficit absorbs overage"
        );
        assert_eq!(mgr.fee_signed_balance(USD), -3);
        // Buyer received full qty (no base fee); seller received cost + rebate.
        assert_eq!(mgr.balance(ACCT_A, BTC).available, 10);
        assert_eq!(mgr.balance(ACCT_B, USD).available, 1_005);
        assert_eq!(post_q, pre_q, "signed conservation under rebate underflow");
        assert_eq!(post_b, pre_b);
    }

    #[test]
    fn fee_credit_pays_down_deficit_first() {
        // After a rebate underflow leaves a USD deficit, a subsequent
        // fee-paying seller fee must reduce that deficit before crediting
        // `available`.
        let mut mgr = AccountManager::new();
        mgr.deposit(ACCT_A, USD, 100_000);
        mgr.deposit(ACCT_B, BTC, 100);
        mgr.deposit(FEE_ACCOUNT, USD, 2);

        // Rebate fill: drains FEE_ACCOUNT.USD and pushes 3 onto deficit.
        let buy1 = limit_buy(1, ACCT_A, 100, 10);
        let sell1 = limit_sell(2, ACCT_B, 100, 10);
        let (_, bs1) = mgr.try_reserve(&buy1, &spec()).unwrap();
        let (_, ss1) = mgr.try_reserve(&sell1, &spec()).unwrap();
        mgr.fill(bs1, ss1, price(100), qty(10), 0, -5, &spec());
        assert_eq!(mgr.fee_account_deficit(USD), 3);

        // Fee-paying fill: seller_quote_fee=10 should pay down the
        // 3-USD deficit first, then credit 7 to available.
        let buy2 = limit_buy(3, ACCT_A, 100, 10);
        let sell2 = limit_sell(4, ACCT_B, 100, 10);
        let (_, bs2) = mgr.try_reserve(&buy2, &spec()).unwrap();
        let (_, ss2) = mgr.try_reserve(&sell2, &spec()).unwrap();

        let (pre_q, pre_b) = conserved_totals(&mgr, ACCT_A, ACCT_B);
        mgr.fill(bs2, ss2, price(100), qty(10), 0, 10, &spec());
        let (post_q, post_b) = conserved_totals(&mgr, ACCT_A, ACCT_B);

        assert_eq!(mgr.fee_account_deficit(USD), 0, "deficit cleared");
        assert_eq!(mgr.balance(FEE_ACCOUNT, USD).available, 7);
        assert_eq!(mgr.fee_signed_balance(USD), 7);
        assert_eq!(post_q, pre_q);
        assert_eq!(post_b, pre_b);
    }

    #[test]
    fn rebate_with_empty_fee_account_records_full_deficit() {
        // Edge case: fee account is completely empty when a rebate fill
        // arrives. The full rebate accumulates as deficit in **both**
        // currencies under A's split semantics.
        let mut mgr = AccountManager::new();
        mgr.deposit(ACCT_A, USD, 10_000);
        mgr.deposit(ACCT_B, BTC, 100);

        let buy = limit_buy(1, ACCT_A, 100, 10);
        let sell = limit_sell(2, ACCT_B, 100, 10);
        let (_, bs) = mgr.try_reserve(&buy, &spec()).unwrap();
        let (_, ss) = mgr.try_reserve(&sell, &spec()).unwrap();

        let (pre_q, pre_b) = conserved_totals(&mgr, ACCT_A, ACCT_B);
        // buyer_base_fee=-3 BTC (rebate), seller_quote_fee=-2 USD (rebate).
        mgr.fill(bs, ss, price(100), qty(10), -3, -2, &spec());
        let (post_q, post_b) = conserved_totals(&mgr, ACCT_A, ACCT_B);

        assert_eq!(mgr.fee_account_deficit(BTC), 3, "BTC deficit");
        assert_eq!(mgr.fee_account_deficit(USD), 2, "USD deficit");
        assert_eq!(mgr.fee_signed_balance(BTC), -3);
        assert_eq!(mgr.fee_signed_balance(USD), -2);
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
        // Build a manager with non-zero deficits in both base and quote,
        // snapshot it, restore, and confirm both deficits survived.
        let mut mgr = AccountManager::new();
        mgr.deposit(ACCT_A, USD, 10_000);
        mgr.deposit(ACCT_B, BTC, 100);

        let buy = limit_buy(1, ACCT_A, 100, 10);
        let sell = limit_sell(2, ACCT_B, 100, 10);
        let (_, bs) = mgr.try_reserve(&buy, &spec()).unwrap();
        let (_, ss) = mgr.try_reserve(&sell, &spec()).unwrap();
        mgr.fill(bs, ss, price(100), qty(10), -3, -2, &spec());
        assert_eq!(mgr.fee_account_deficit(BTC), 3);
        assert_eq!(mgr.fee_account_deficit(USD), 2);

        let balances = mgr.snapshot_balances();
        let mut deficits = mgr.snapshot_fee_deficits();
        deficits.sort_by_key(|(c, _)| c.0); // deterministic for assertion
        assert_eq!(deficits, vec![(BTC, 3), (USD, 2)]);
        let (restored, _) = AccountManager::from_parts(balances, Vec::new(), deficits);
        assert_eq!(restored.fee_account_deficit(BTC), 3);
        assert_eq!(restored.fee_account_deficit(USD), 2);
        assert_eq!(restored.fee_signed_balance(BTC), -3);
        assert_eq!(restored.fee_signed_balance(USD), -2);
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
        // Bounds chosen so fees never trip the fee-cap path (covered by
        // separate unit tests). Buyer fee in [-10, 10] base units ≤
        // min(qty)=10. Seller fee in [-50, 50] quote units ≤ min(cost)=
        // 100×10=1_000. This keeps the proptest focused on conservation
        // and signed-ledger invariants under the uncapped regime.
        (100u64..=1_000, 10u64..=100, -10i32..=10, -50i32..=50).prop_map(
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

            let signed_quote = |m: &AccountManager| {
                m.account_signed_total(ACCT_A, USD)
                    + m.account_signed_total(ACCT_B, USD)
                    + m.account_signed_total(FEE_ACCOUNT, USD)
            };
            let signed_base = |m: &AccountManager| {
                m.account_signed_total(ACCT_A, BTC)
                    + m.account_signed_total(ACCT_B, BTC)
                    + m.account_signed_total(FEE_ACCOUNT, BTC)
            };

            let initial_signed_quote = signed_quote(&mgr);
            let initial_signed_base = signed_base(&mgr);

            let mut order_id = 1u64;
            let mut expected_quote_fee: i128 = 0;
            let mut expected_base_fee: i128 = 0;

            for ev in &events {
                let buy = limit_buy(order_id, ACCT_A, ev.price, ev.quantity);
                order_id += 1;
                let sell = limit_sell(order_id, ACCT_B, ev.price, ev.quantity);
                order_id += 1;

                let (_, bs) = match mgr.try_reserve(&buy, &spec()) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let (_, ss) = match mgr.try_reserve(&sell, &spec()) {
                    Ok(v) => v,
                    Err(_) => { mgr.release(bs); continue; }
                };

                mgr.fill(
                    bs,
                    ss,
                    price(ev.price),
                    qty(ev.quantity),
                    ev.buyer_fee as i64,  // base-denominated under A
                    ev.seller_fee as i64, // quote-denominated under A
                    &spec(),
                );
                // Buyer's reservation is fully consumed by `cost`; sell
                // similarly. release() is a no-op when remaining==0 and
                // safely returns leftover otherwise.
                mgr.release(bs);
                mgr.release(ss);

                // Under A, buyer fee accumulates on FEE_ACCOUNT.base and
                // seller fee on FEE_ACCOUNT.quote (per-currency split).
                expected_base_fee += ev.buyer_fee as i128;
                expected_quote_fee += ev.seller_fee as i128;
            }

            // (1) Signed conservation across both currencies.
            prop_assert_eq!(
                signed_quote(&mgr), initial_signed_quote,
                "signed quote conservation violated"
            );
            prop_assert_eq!(
                signed_base(&mgr), initial_signed_base,
                "signed base conservation violated"
            );

            // (2) Per-currency fee signed balance matches cumulative
            // fee charged in that currency.
            prop_assert_eq!(
                mgr.fee_signed_balance(USD), expected_quote_fee,
                "fee USD signed balance != Σ seller_fee"
            );
            prop_assert_eq!(
                mgr.fee_signed_balance(BTC), expected_base_fee,
                "fee BTC signed balance != Σ buyer_fee"
            );

            // (3) Per-currency invariant: available and deficit are
            // never both nonzero simultaneously without an external
            // deposit (the proptest doesn't deposit mid-sequence).
            for ccy in [USD, BTC] {
                let avail = mgr.balance(FEE_ACCOUNT, ccy).available;
                let deficit = mgr.fee_account_deficit(ccy);
                prop_assert!(
                    !(avail > 0 && deficit > 0),
                    "fee account {:?} has both available={} and deficit={}",
                    ccy, avail, deficit
                );
            }
        }
    }

    // -- Fee-cap behavior: positive fee cannot exceed what the trader
    //    received in this leg (industry-standard policy). --

    #[test]
    fn buyer_fee_capped_at_received_quantity() {
        // buyer_base_fee = 15 > qty = 10 → fee is capped at 10. Buyer
        // receives 0 BTC, fee account collects 10 BTC.
        let mut mgr = AccountManager::new();
        mgr.deposit(ACCT_A, USD, 10_000);
        mgr.deposit(ACCT_B, BTC, 100);

        let buy = limit_buy(1, ACCT_A, 100, 10);
        let sell = limit_sell(2, ACCT_B, 100, 10);
        let (_, bs) = mgr.try_reserve(&buy, &spec()).unwrap();
        let (_, ss) = mgr.try_reserve(&sell, &spec()).unwrap();

        let (pre_q, pre_b) = conserved_totals(&mgr, ACCT_A, ACCT_B);
        mgr.fill(bs, ss, price(100), qty(10), 15, 0, &spec());
        let (post_q, post_b) = conserved_totals(&mgr, ACCT_A, ACCT_B);

        // Buyer's base credit clamped to zero (not negative).
        assert_eq!(mgr.balance(ACCT_A, BTC).available, 0);
        // Fee account collected the capped 10 (not the requested 15).
        assert_eq!(mgr.balance(FEE_ACCOUNT, BTC).available, 10);
        assert_eq!(post_q, pre_q);
        assert_eq!(post_b, pre_b);
    }

    #[test]
    fn seller_fee_capped_at_received_proceeds() {
        // seller_quote_fee = 1500 > cost = 1000 → fee capped at 1000.
        // Seller receives 0 USD, fee account collects 1000 USD.
        let mut mgr = AccountManager::new();
        mgr.deposit(ACCT_A, USD, 10_000);
        mgr.deposit(ACCT_B, BTC, 100);

        let buy = limit_buy(1, ACCT_A, 100, 10);
        let sell = limit_sell(2, ACCT_B, 100, 10);
        let (_, bs) = mgr.try_reserve(&buy, &spec()).unwrap();
        let (_, ss) = mgr.try_reserve(&sell, &spec()).unwrap();

        let (pre_q, pre_b) = conserved_totals(&mgr, ACCT_A, ACCT_B);
        mgr.fill(bs, ss, price(100), qty(10), 0, 1500, &spec());
        let (post_q, post_b) = conserved_totals(&mgr, ACCT_A, ACCT_B);

        assert_eq!(mgr.balance(ACCT_B, USD).available, 0);
        assert_eq!(mgr.balance(FEE_ACCOUNT, USD).available, 1_000);
        assert_eq!(post_q, pre_q);
        assert_eq!(post_b, pre_b);
    }

    #[test]
    fn fee_equal_to_received_zeroes_credit() {
        // Boundary: fee == gross. Trader gets exactly 0; fee account
        // collects the full gross. Conservation still holds.
        let mut mgr = AccountManager::new();
        mgr.deposit(ACCT_A, USD, 10_000);
        mgr.deposit(ACCT_B, BTC, 100);

        let buy = limit_buy(1, ACCT_A, 100, 10);
        let sell = limit_sell(2, ACCT_B, 100, 10);
        let (_, bs) = mgr.try_reserve(&buy, &spec()).unwrap();
        let (_, ss) = mgr.try_reserve(&sell, &spec()).unwrap();

        let (pre_q, pre_b) = conserved_totals(&mgr, ACCT_A, ACCT_B);
        mgr.fill(bs, ss, price(100), qty(10), 10, 1000, &spec());
        let (post_q, post_b) = conserved_totals(&mgr, ACCT_A, ACCT_B);

        assert_eq!(mgr.balance(ACCT_A, BTC).available, 0);
        assert_eq!(mgr.balance(ACCT_B, USD).available, 0);
        assert_eq!(mgr.balance(FEE_ACCOUNT, BTC).available, 10);
        assert_eq!(mgr.balance(FEE_ACCOUNT, USD).available, 1_000);
        assert_eq!(post_q, pre_q);
        assert_eq!(post_b, pre_b);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(200))]

        /// Fee-cap regime: fees can exceed gross. Verifies that
        /// conservation holds even when the cap fires (trader credit
        /// clamps to zero, fee account collects the capped amount).
        #[test]
        fn fee_cap_preserves_conservation(
            price_v in 1u64..=1_000,
            qty_v in 1u64..=100,
            buyer_fee in 0i64..=200,    // can exceed qty=1 minimum
            seller_fee in 0i64..=20_000, // can exceed cost=1 × 1 minimum
        ) {
            let mut mgr = AccountManager::new();
            mgr.deposit(ACCT_A, USD, 1_000_000);
            mgr.deposit(ACCT_B, BTC, 10_000);

            let buy = limit_buy(1, ACCT_A, price_v, qty_v);
            let sell = limit_sell(2, ACCT_B, price_v, qty_v);
            let (_, bs) = mgr.try_reserve(&buy, &spec()).unwrap();
            let (_, ss) = mgr.try_reserve(&sell, &spec()).unwrap();

            let (pre_q, pre_b) = conserved_totals(&mgr, ACCT_A, ACCT_B);
            mgr.fill(bs, ss, price(price_v), qty(qty_v), buyer_fee, seller_fee, &spec());
            let (post_q, post_b) = conserved_totals(&mgr, ACCT_A, ACCT_B);

            prop_assert_eq!(post_q, pre_q, "quote conservation under fee cap");
            prop_assert_eq!(post_b, pre_b, "base conservation under fee cap");

            // Trader credits never go negative (Balance is u64).
            // Fee account collects at most the gross (the cap); when fee
            // ≤ gross it collects exactly the fee.
            let actual_buyer_fee = (buyer_fee as u64).min(qty_v);
            let actual_seller_fee = (seller_fee as u64).min(price_v * qty_v);
            prop_assert_eq!(
                mgr.balance(FEE_ACCOUNT, BTC).available, actual_buyer_fee,
                "fee account base = min(buyer_fee, qty)"
            );
            prop_assert_eq!(
                mgr.balance(FEE_ACCOUNT, USD).available, actual_seller_fee,
                "fee account quote = min(seller_fee, cost)"
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
        let (_, bs1) = mgr.try_reserve(&buy1, &spec()).unwrap();
        let (_, ss1) = mgr.try_reserve(&sell1, &spec()).unwrap();
        // -3 buyer base fee, -2 seller quote fee → both currencies
        // accumulate deficit (BTC=3, USD=2).
        mgr.fill(bs1, ss1, price(100), qty(10), -3, -2, &spec());
        assert_eq!(mgr.fee_account_deficit(BTC), 3);
        assert_eq!(mgr.fee_account_deficit(USD), 2);

        let buy2 = limit_buy(3, ACCT_A, 100, 10);
        let sell2 = limit_sell(4, ACCT_B, 100, 10);
        let (_, bs2) = mgr.try_reserve(&buy2, &spec()).unwrap();
        let (_, ss2) = mgr.try_reserve(&sell2, &spec()).unwrap();
        // +3 / +2: pays both deficits down to zero exactly.
        mgr.fill(bs2, ss2, price(100), qty(10), 3, 2, &spec());

        assert_eq!(mgr.fee_account_deficit(BTC), 0);
        assert_eq!(mgr.fee_account_deficit(USD), 0);
        assert!(
            mgr.snapshot_fee_deficits().is_empty(),
            "zero entries removed in both currencies"
        );
    }
}
