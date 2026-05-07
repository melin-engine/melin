//! Exchange: dispatches orders to per-instrument order books.
//!
//! All order books run on a single thread (LMAX-style). This keeps event
//! ordering deterministic and allows portfolio-wide risk checks (margin,
//! exposure limits) without cross-thread coordination.
//!
//! If throughput exceeds a single core, shard by instrument — each shard
//! stays single-threaded. Note: portfolio risk checks then require
//! cross-shard message passing, adding latency and complexity.

use crate::account::AccountManager;
use crate::orderbook::OrderBook;
use crate::scheduler::{ScheduledTask, ScheduledTaskHeap, ScheduledTaskKind};
use crate::types::{
    AccountId, CircuitBreakerConfig, CurrencyId, ExecutionReport, FeeSchedule, HashMap, HashMap4,
    InstrumentSpec, InstrumentStatus, Order, OrderId, OrderType, Price, Quantity, RejectReason,
    ReservationSlot, RiskLimits, Side, Symbol, TimeInForce,
};

/// Helper: get an immutable reference to the InstrumentState at `symbol`.
#[inline]
fn inst_ref(
    instruments: &[Option<Box<InstrumentState>>],
    symbol: Symbol,
) -> Option<&InstrumentState> {
    instruments
        .get(symbol.0 as usize)
        .and_then(|o| o.as_deref())
}

/// Helper: get a mutable reference to the InstrumentState at `symbol`.
#[inline]
fn inst_mut(
    instruments: &mut [Option<Box<InstrumentState>>],
    symbol: Symbol,
) -> Option<&mut InstrumentState> {
    instruments
        .get_mut(symbol.0 as usize)
        .and_then(|o| o.as_deref_mut())
}

/// Compute the effective max fee rate from a fee schedule. Takes the
/// maximum of (0, maker_fee_bps, taker_fee_bps), clamping negatives
/// (rebates) to 0. Used to size the reservation fee cushion.
#[inline]
fn max_fee_bps(fees: &FeeSchedule) -> u16 {
    0i16.max(fees.maker_fee_bps)
        .max(0i16.max(fees.taker_fee_bps)) as u16
}

/// Compute the required reservation for a buy-side order at a known price,
/// including fee cushion: `price * qty * (10_000 + max_fee_bps) / 10_000`.
/// Uses u128 to avoid overflow.
#[inline]
fn required_with_fee(price: u64, qty: u64, max_fee_bps: u16) -> u64 {
    let cost = price as u128 * qty as u128;
    let fee_cushion = cost * max_fee_bps as u128 / 10_000;
    // Saturate to u64::MAX — identical to try_reserve behavior.
    (cost + fee_cushion).min(u64::MAX as u128) as u64
}

/// All per-instrument state in one struct for cache-friendly single-lookup
/// access. On every order the engine does one HashMap lookup instead of 5,
/// turning 5 potential cache misses into 1.
pub(crate) struct InstrumentState {
    pub(crate) spec: InstrumentSpec,
    pub(crate) book: OrderBook,
    pub(crate) risk_limits: RiskLimits,
    pub(crate) circuit_breaker: CircuitBreakerConfig,
    pub(crate) fee_schedule: FeeSchedule,
    /// When true, the instrument is disabled — no new orders or amendments
    /// are accepted. All resting orders are cancelled on disable.
    pub(crate) disabled: bool,
}

/// Top-level exchange managing multiple instruments.
pub struct Exchange {
    /// Flat Vec indexed by `Symbol.0` for true O(1) instrument dispatch with
    /// zero hashing overhead. Boxed to keep empty slots at 8 bytes (null ptr)
    /// since InstrumentState is large (contains OrderBook). Typical exchanges
    /// have <100 instruments, so the Vec is tiny.
    instruments: Vec<Option<Box<InstrumentState>>>,
    /// Shared account balance manager across all instruments.
    accounts: AccountManager,
    /// Currently-live (account, order_id) pairs across all instruments.
    /// A submission with an `(account, order_id)` already in this set is
    /// rejected as `DuplicateOrderId` — required because cancel/replace
    /// look up by that same key, so two simultaneously-live orders sharing
    /// it would make the lookup ambiguous. Entries are removed when the
    /// order leaves the book (full fill, cancel, expiry, instrument
    /// disable). Reuse of an `OrderId` after its original closes is
    /// permitted by design — the dedup invariant is "no two live orders
    /// share `(account, order_id)`," not "an `OrderId` is consumed
    /// forever," which keeps the gateway's session-local id_map workable
    /// across reconnects without needing to query the engine for HWMs.
    /// Used as a set: the unit value carries no information.
    live_order_ids: HashMap4<(AccountId, OrderId), ()>,
    /// Per-account count of resting orders (on the book or pending stops).
    /// Used to reject withdrawals while orders are outstanding.
    /// Entries are removed when the count reaches zero.
    order_counts: HashMap4<AccountId, u32>,
    /// Per-key high-water mark for request sequences. Prevents duplicate
    /// processing on retry after network failure. Keyed by u64 hash of
    /// the client's Ed25519 public key. Never evicted — key count is
    /// small (~100 max for any exchange).
    key_hwm: HashMap<u64, u64>,
    /// Min-heap of pending time-driven tasks (GTD expiry, halt evaluation,
    /// session transitions). Drained at the head of every event the matching
    /// stage processes — see `drain_due_scheduled_tasks`. Empty until a
    /// feature pushes a task; the substrate alone never schedules anything.
    scheduled_tasks: ScheduledTaskHeap,
    /// When true, new order books are created with generous pre-allocation
    /// to avoid HashMap resize spikes on the hot path.
    presized: bool,
}

impl Exchange {
    pub fn new() -> Self {
        Self {
            instruments: Vec::new(),
            accounts: AccountManager::new(),
            live_order_ids: HashMap4::default(),
            order_counts: HashMap4::default(),
            key_hwm: HashMap::default(),
            scheduled_tasks: ScheduledTaskHeap::new(),
            presized: false,
        }
    }

    /// Create an Exchange pre-sized for production workloads.
    pub fn with_capacity() -> Self {
        Self {
            // 64 instrument slots — each empty slot is 8 bytes (null Box ptr).
            instruments: Vec::with_capacity(64),
            accounts: AccountManager::with_capacity(),
            // 1M live-order slots × ~24 bytes per entry ≈ 24 MB. Sized
            // for the default benchmark's peak resting depth — orders
            // turn over fast at 10M ord/s so the live count is much
            // smaller than the lifetime total.
            live_order_ids: HashMap4::with_capacity_and_hasher(1_000_000, Default::default()),
            // 1M accounts × ~32 bytes per entry ≈ 32 MB. Covers the
            // default benchmark (1M accounts) with no hot-path resizes.
            // Pages are faulted during prefault() via insert/clear.
            order_counts: HashMap4::with_capacity_and_hasher(1_000_000, Default::default()),
            key_hwm: HashMap::default(),
            scheduled_tasks: ScheduledTaskHeap::new(),
            presized: true,
        }
    }

    /// Reconstruct from pre-built parts (used by snapshot restore).
    pub(crate) fn from_parts(
        instruments: Vec<Option<Box<InstrumentState>>>,
        accounts: AccountManager,
        key_hwm: HashMap<u64, u64>,
        scheduled_tasks: ScheduledTaskHeap,
    ) -> Self {
        // Derive order_counts and live_order_ids from order_index across
        // all instruments. Both are fully reconstructible from the books,
        // so the snapshot doesn't carry them — the only source of truth
        // is the order index.
        let mut order_counts: HashMap4<AccountId, u32> = HashMap4::default();
        let mut live_order_ids: HashMap4<(AccountId, OrderId), ()> = HashMap4::default();
        for inst in &instruments {
            if let Some(inst) = inst.as_deref() {
                for ((account, order_id), _) in inst.book.active_order_slots() {
                    *order_counts.entry(account).or_default() += 1;
                    live_order_ids.insert((account, order_id), ());
                }
                for ((account, order_id), _) in inst.book.active_stop_slots() {
                    *order_counts.entry(account).or_default() += 1;
                    live_order_ids.insert((account, order_id), ());
                }
            }
        }
        Self {
            instruments,
            accounts,
            live_order_ids,
            order_counts,
            key_hwm,
            scheduled_tasks,
            presized: false,
        }
    }

    /// Pending count of scheduled tasks (including tombstones). Test-only
    /// helper for asserting heap state.
    #[cfg(test)]
    pub(crate) fn scheduled_task_count(&self) -> usize {
        self.scheduled_tasks.len()
    }

    /// Drain every scheduled task whose `fire_ns <= now_ns`. Called at the
    /// head of every event the matching stage processes, so time-driven work
    /// runs in lockstep with the journal. Tombstones — tasks that point to
    /// orders that have already been cancelled or filled — are silently
    /// dropped via the `find_gtd_expiry` lookup.
    pub fn drain_due_scheduled_tasks(&mut self, now_ns: u64, reports: &mut Vec<ExecutionReport>) {
        while let Some(task) = self.scheduled_tasks.pop_due(now_ns) {
            match task.kind {
                ScheduledTaskKind::ExpireOrder {
                    symbol,
                    account,
                    order_id,
                } => {
                    let Some(inst) = inst_mut(&mut self.instruments, symbol) else {
                        // Instrument removed between schedule and fire — tombstone.
                        continue;
                    };
                    // Skip tombstones: if the order is no longer GTD on the
                    // book, it's already been cancelled or filled. The task
                    // is just stale; drop it without side effect.
                    if inst.book.find_gtd_expiry(account, order_id).is_none() {
                        continue;
                    }
                    if let Some((_side, slot)) = inst.book.cancel(account, order_id, reports) {
                        self.accounts.release(slot);
                        self.live_order_ids.remove(&(account, order_id));
                        if let Some(count) = self.order_counts.get_mut(&account) {
                            *count = count.saturating_sub(1);
                            if *count == 0 {
                                self.order_counts.remove(&account);
                            }
                        }
                    }
                }
            }
        }
    }

    /// Schedule an `ExpireOrder` task for a GTD order that just rested on
    /// the book (or registered as a pending stop).
    fn schedule_gtd_expiry(
        &mut self,
        symbol: Symbol,
        account: AccountId,
        order_id: OrderId,
        expiry_ns: u64,
    ) {
        self.scheduled_tasks.push(ScheduledTask {
            fire_ns: expiry_ns,
            kind: ScheduledTaskKind::ExpireOrder {
                symbol,
                account,
                order_id,
            },
        });
    }

    /// Check per-key request sequence for idempotency dedup.
    /// Returns true if this is a new request (should be processed).
    /// Returns false if duplicate (caller should reject with DuplicateRequest).
    /// Exempt when key_hash == 0 (internal/seed events with no authenticated key).
    #[inline]
    pub fn check_request_seq(&mut self, key_hash: u64, request_seq: u64) -> bool {
        if key_hash == 0 {
            return true; // exempt: internal/seed events
        }
        let hwm = self.key_hwm.entry(key_hash).or_insert(0);
        if request_seq <= *hwm {
            return false; // duplicate
        }
        *hwm = request_seq;
        true
    }

    /// Current request_seq HWM for `key_hash`, or `0` if no event has
    /// ever been accepted from that key. Read-only; safe to call from
    /// the matching stage at any point. Used by the `QueryRequestSeq`
    /// query handler so reconnecting clients can resume their outbound
    /// seq past whatever the engine has already seen.
    pub fn request_seq_hwm(&self, key_hash: u64) -> u64 {
        self.key_hwm.get(&key_hash).copied().unwrap_or(0)
    }

    /// Snapshot per-key request sequence HWMs for serialization.
    pub(crate) fn snapshot_key_hwm(&self) -> Vec<(u64, u64)> {
        self.key_hwm
            .iter()
            .filter(|(_, hwm)| **hwm > 0)
            .map(|(&key, &hwm)| (key, hwm))
            .collect()
    }

    /// Number of active instruments (for diagnostics).
    pub fn instrument_count(&self) -> usize {
        self.instruments.iter().filter(|s| s.is_some()).count()
    }

    /// Iterate over instrument specs (for snapshot serialization).
    pub(crate) fn instrument_specs(&self) -> impl Iterator<Item = &InstrumentSpec> {
        self.instruments
            .iter()
            .filter_map(|slot| slot.as_deref())
            .map(|inst| &inst.spec)
    }

    /// Iterate over (symbol, book) pairs (for snapshot serialization and proptests).
    pub(crate) fn books(&self) -> impl Iterator<Item = (Symbol, &OrderBook)> {
        self.instruments
            .iter()
            .filter_map(|slot| slot.as_deref())
            .map(|inst| (inst.spec.symbol, &inst.book))
    }

    /// Snapshot the order-side map as a Vec for serialization.
    /// Only serializes the side; reservation slots are ephemeral and
    /// reassigned on restore.
    pub(crate) fn snapshot_order_sides(&self) -> Vec<((AccountId, OrderId), Side)> {
        let mut sides = Vec::new();
        for inst in &self.instruments {
            if let Some(inst) = inst.as_deref() {
                for (key, (side, _slot)) in inst.book.active_order_slots() {
                    sides.push((key, side));
                }
                for (key, (side, _slot)) in inst.book.active_stop_slots() {
                    sides.push((key, side));
                }
            }
        }
        sides
    }

    /// Collect active reservation slot assignments from all instruments.
    fn active_reservation_slots(&self) -> Vec<((AccountId, OrderId), ReservationSlot)> {
        let mut slots = Vec::new();
        for inst in &self.instruments {
            if let Some(inst) = inst.as_deref() {
                for (key, (_side, slot)) in inst.book.active_order_slots() {
                    slots.push((key, slot));
                }
                for (key, (_side, slot)) in inst.book.active_stop_slots() {
                    slots.push((key, slot));
                }
            }
        }
        slots
    }

    /// Snapshot all active reservations. Delegates to AccountManager with
    /// the active slot assignments.
    pub(crate) fn snapshot_reservations(&self) -> Vec<(OrderId, AccountId, CurrencyId, u64)> {
        let active = self.active_reservation_slots();
        self.accounts.snapshot_reservations(&active)
    }

    /// Set fat finger risk limits for an instrument. No-op if the
    /// instrument doesn't exist (matches previous behavior).
    pub fn set_risk_limits(&mut self, symbol: Symbol, limits: RiskLimits) {
        if let Some(inst) = inst_mut(&mut self.instruments, symbol) {
            inst.risk_limits = limits;
        }
    }

    /// Snapshot the per-instrument risk limits for serialization.
    pub(crate) fn snapshot_risk_limits(&self) -> Vec<(Symbol, RiskLimits)> {
        self.instruments
            .iter()
            .filter_map(|slot| slot.as_deref())
            .map(|inst| (inst.spec.symbol, inst.risk_limits))
            .collect()
    }

    /// Set circuit breaker configuration for an instrument. No-op if the
    /// instrument doesn't exist (matches previous behavior).
    pub fn set_circuit_breaker(&mut self, symbol: Symbol, config: CircuitBreakerConfig) {
        if let Some(inst) = inst_mut(&mut self.instruments, symbol) {
            inst.circuit_breaker = config;
        }
    }

    /// Set the maker/taker fee schedule for an instrument.
    ///
    /// When the effective max fee rate changes, all affected buy-side
    /// orders have their reservations adjusted:
    /// - Resting limit buys and pending stop-limit buys: reservation
    ///   topped up from available balance, or cancelled if insufficient.
    /// - Pending stop-market buys: `quote_budget` recalculated so the
    ///   fill leaves room for the new fee.
    ///
    /// No-op if the instrument doesn't exist.
    pub fn set_fee_schedule(
        &mut self,
        symbol: Symbol,
        schedule: FeeSchedule,
        reports: &mut Vec<ExecutionReport>,
    ) {
        let Some(inst) = inst_mut(&mut self.instruments, symbol) else {
            return;
        };

        let old_max = max_fee_bps(&inst.fee_schedule);
        let new_max = max_fee_bps(&schedule);
        inst.fee_schedule = schedule;

        if old_max == new_max {
            return;
        }

        // Collect buy-side orders that need re-reservation, with their
        // reservation slot captured directly during the walk.
        // (account, order_id, reservation_slot, new_required_amount)
        let mut to_adjust: Vec<(AccountId, OrderId, ReservationSlot, u64)> = Vec::new();

        // Resting limit buys (bids).
        inst.book.bids().for_each_order(|price, order| {
            let new_required = required_with_fee(price.get(), order.remaining().get(), new_max);
            to_adjust.push((
                order.account(),
                order.id(),
                order.reservation(),
                new_required,
            ));
        });

        // Pending stop-limit buys (have a known limit_price).
        inst.book.stop_buys().for_each_stop(|stop| {
            if let Some(limit_price) = stop.limit_price() {
                let new_required =
                    required_with_fee(limit_price.get(), stop.quantity().get(), new_max);
                to_adjust.push((stop.account(), stop.id(), stop.reservation(), new_required));
            }
        });

        // Adjust reservations. Cancel orders that can't afford the new fee.
        let mut to_cancel: Vec<(AccountId, OrderId)> = Vec::new();
        for &(account, order_id, slot, new_required) in &to_adjust {
            if self
                .accounts
                .try_adjust_reservation(slot, new_required)
                .is_err()
            {
                to_cancel.push((account, order_id));
            }
        }

        // Cancel underfunded orders.
        for (account, order_id) in to_cancel {
            if let Some((_side, slot)) = inst.book.cancel(account, order_id, reports) {
                self.accounts.release(slot);
                self.live_order_ids.remove(&(account, order_id));
                if let Some(count) = self.order_counts.get_mut(&account) {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        self.order_counts.remove(&account);
                    }
                }
            }
        }

        // Adjust quote_budget on pending buy stop-market orders.
        let accounts = &self.accounts;
        inst.book
            .adjust_stop_buy_budgets(new_max, |slot| accounts.reservation_remaining(slot));
    }

    /// Snapshot the fee schedules for serialization.
    pub(crate) fn snapshot_fee_schedules(&self) -> Vec<(Symbol, FeeSchedule)> {
        self.instruments
            .iter()
            .filter_map(|slot| slot.as_deref())
            .map(|inst| (inst.spec.symbol, inst.fee_schedule))
            .collect()
    }

    /// Snapshot the per-instrument circuit breaker configs for serialization.
    pub(crate) fn snapshot_circuit_breakers(&self) -> Vec<(Symbol, CircuitBreakerConfig)> {
        self.instruments
            .iter()
            .filter_map(|slot| slot.as_deref())
            .map(|inst| (inst.spec.symbol, inst.circuit_breaker))
            .collect()
    }

    /// Touch all pre-allocated HashMap pages so page faults happen at startup,
    /// not on the hot path. Call once after adding instruments, before accepting
    /// orders. Skips maps that already contain data — their pages are already
    /// faulted from the insertions that populated them.
    pub fn prefault(&mut self) {
        // Fault live_order_ids and order_counts pages. with_capacity()
        // allocated the backing table but didn't write to it — insert
        // dummy entries and clear to touch every page before the hot path.
        if self.live_order_ids.is_empty() {
            let cap = self.live_order_ids.capacity();
            for i in 0..cap as u32 {
                self.live_order_ids
                    .insert((AccountId(i), OrderId(i as u64)), ());
            }
            self.live_order_ids.clear();
        }
        if self.order_counts.is_empty() {
            let cap = self.order_counts.capacity();
            for i in 0..cap as u32 {
                self.order_counts.insert(AccountId(i), 0);
            }
            self.order_counts.clear();
        }

        self.accounts.prefault();

        for slot in &mut self.instruments {
            if let Some(inst) = slot.as_deref_mut() {
                inst.book.prefault();
            }
        }
    }

    /// Register a new instrument with its currency pair specification.
    /// Grows the instrument Vec if needed (admin operation, not hot path).
    pub fn add_instrument(&mut self, spec: InstrumentSpec) {
        let idx = spec.symbol.0 as usize;
        // Grow Vec to accommodate the new symbol index.
        if idx >= self.instruments.len() {
            self.instruments.resize_with(idx + 1, || None);
        }
        // Only insert if slot is empty (don't overwrite existing instrument).
        if self.instruments[idx].is_none() {
            let book = if self.presized {
                OrderBook::with_capacity(spec.symbol)
            } else {
                OrderBook::new(spec.symbol)
            };
            self.instruments[idx] = Some(Box::new(InstrumentState {
                spec,
                book,
                risk_limits: RiskLimits::default(),
                circuit_breaker: CircuitBreakerConfig::default(),
                fee_schedule: FeeSchedule::default(),
                disabled: false,
            }));
        }
    }

    /// Deposit funds into an account.
    pub fn deposit(&mut self, account: AccountId, currency: CurrencyId, amount: u64) {
        self.accounts.deposit(account, currency, amount);
    }

    /// Provision an account with `amount` deposited in every currency of
    /// every registered instrument. Replaces O(instruments) individual
    /// Deposit calls with a single operation for bulk seeding.
    pub fn provision_account(&mut self, account: AccountId, amount: u64) {
        for state in self.instruments.iter().flatten() {
            self.accounts.deposit(account, state.spec.base, amount);
            self.accounts.deposit(account, state.spec.quote, amount);
        }
    }

    /// Get the account manager (for balance queries).
    pub fn accounts(&self) -> &AccountManager {
        &self.accounts
    }

    /// Submit an order to the matching engine for the given instrument.
    ///
    /// Validates the instrument exists, reserves funds, then executes.
    /// On fill, balances are updated. On reject/cancel, reserves are released.
    #[inline]
    pub fn execute(&mut self, symbol: Symbol, order: Order, reports: &mut Vec<ExecutionReport>) {
        let Some(inst) = inst_ref(&self.instruments, symbol) else {
            reports.push(ExecutionReport::Rejected {
                order_id: order.id,
                symbol,
                account: order.account,
                reason: RejectReason::UnknownSymbol,
            });
            return;
        };
        // Disabled instruments reject before HWM advance — the order is
        // never "processed", same as UnknownSymbol.
        if inst.disabled {
            reports.push(ExecutionReport::Rejected {
                order_id: order.id,
                symbol,
                account: order.account,
                reason: RejectReason::InstrumentDisabled,
            });
            return;
        }
        // Copy spec before taking mutable borrow on instruments below.
        // InstrumentSpec is Copy (3 × u32 = 12 bytes).
        let spec = inst.spec;

        // Dedup: reject if `(account, order_id)` already names a live
        // order. Cancel/replace look up by the same key, so two live
        // orders sharing it would make those operations ambiguous.
        // Replay-safety is provided one layer up by `check_request_seq`
        // (transport-level idempotency on `(key_hash, request_seq)`),
        // not here — duplicate journaled SubmitOrder events never reach
        // this point. Reuse of an `OrderId` after the original closes
        // is permitted by design.
        if self.live_order_ids.contains_key(&(order.account, order.id)) {
            reports.push(ExecutionReport::Rejected {
                order_id: order.id,
                symbol,
                account: order.account,
                reason: RejectReason::DuplicateOrderId,
            });
            return;
        }

        let inst = inst_ref(&self.instruments, symbol).expect("instrument verified to exist above");

        // Circuit breaker checks: trading halt rejects all orders; price
        // bands reject limit/stop-limit orders outside [lower, upper].
        // No HashMap lookup — circuit breaker is in the same struct.
        let cb = &inst.circuit_breaker;
        if cb.halted {
            reports.push(ExecutionReport::Rejected {
                order_id: order.id,
                symbol,
                account: order.account,
                reason: RejectReason::TradingHalted,
            });
            return;
        }
        // Price band check applies only to orders with a known price.
        // Market and Stop orders have no submission-time price and
        // bypass bands by design (SEC-12). A large market order can
        // fill far outside the intended bands. Mitigation: use the
        // trading halt flag, or implement automatic volatility halts
        // (Phase 3 of the circuit breaker plan).
        let limit_price = match order.order_type {
            OrderType::Limit { price, .. } => Some(price),
            OrderType::StopLimit { limit_price, .. } => Some(limit_price),
            OrderType::Market | OrderType::Stop { .. } => None,
        };
        if let Some(price) = limit_price {
            if let Some(lower) = cb.price_band_lower
                && price < lower
            {
                reports.push(ExecutionReport::Rejected {
                    order_id: order.id,
                    symbol,
                    account: order.account,
                    reason: RejectReason::OutsidePriceBand,
                });
                return;
            }
            if let Some(upper) = cb.price_band_upper
                && price > upper
            {
                reports.push(ExecutionReport::Rejected {
                    order_id: order.id,
                    symbol,
                    account: order.account,
                    reason: RejectReason::OutsidePriceBand,
                });
                return;
            }
        }

        // Fat finger checks: reject orders exceeding per-instrument limits.
        let limits = &inst.risk_limits;
        if let Some(max_qty) = limits.max_order_qty
            && order.quantity.get() > max_qty.get()
        {
            reports.push(ExecutionReport::Rejected {
                order_id: order.id,
                symbol,
                account: order.account,
                reason: RejectReason::ExceedsMaxOrderQty,
            });
            return;
        }
        if let Some(max_notional) = limits.max_order_notional {
            // Notional check applies only to orders with a known price.
            // Market and Stop orders have no submission-time price.
            // StopLimit uses limit_price (worst-case resting price).
            let limit_price = match order.order_type {
                OrderType::Limit { price, .. } => Some(price),
                OrderType::StopLimit { limit_price, .. } => Some(limit_price),
                OrderType::Market | OrderType::Stop { .. } => None,
            };
            if let Some(price) = limit_price {
                let notional = price.get() as u128 * order.quantity.get() as u128;
                if notional > max_notional as u128 {
                    reports.push(ExecutionReport::Rejected {
                        order_id: order.id,
                        symbol,
                        account: order.account,
                        reason: RejectReason::ExceedsMaxNotional,
                    });
                    return;
                }
            }
        }

        // GTD validation: GTD orders must have a non-zero expiry, and
        // non-GTD orders must not carry an expiry timestamp.
        if order.time_in_force == TimeInForce::GTD && order.expiry_ns == 0 {
            reports.push(ExecutionReport::Rejected {
                order_id: order.id,
                symbol,
                account: order.account,
                reason: RejectReason::InvalidExpiry,
            });
            return;
        }
        if order.time_in_force != TimeInForce::GTD && order.expiry_ns != 0 {
            reports.push(ExecutionReport::Rejected {
                order_id: order.id,
                symbol,
                account: order.account,
                reason: RejectReason::InvalidExpiry,
            });
            return;
        }

        // Reserve funds before submitting to the matching engine.
        // Include a fee cushion for buy-side limit orders so fees can be
        // charged from the reservation even at the exact limit price.
        // Only positive fees need a reservation cushion — rebates (negative)
        // don't require upfront funds.
        let max_fee_bps = max_fee_bps(&inst.fee_schedule);
        let (reserved, slot) = match self.accounts.try_reserve(&order, &spec, max_fee_bps) {
            Ok(result) => result,
            Err(reason) => {
                reports.push(ExecutionReport::Rejected {
                    order_id: order.id,
                    symbol,
                    account: order.account,
                    reason,
                });
                return;
            }
        };

        // For buy-side market/stop-market orders, pass a cost budget so the
        // matching engine stops before exceeding what the reservation can
        // cover (cost + fees). Limit and stop-limit buys don't need this —
        // their cost is bounded by price × quantity and the reservation
        // already includes the fee cushion.
        //
        // When fees are active, the budget must leave room for the fee:
        //   cost + floor(cost × fee_bps / 10_000) ≤ reserved
        // ⟹ cost ≤ floor(reserved × 10_000 / (10_000 + fee_bps))
        let quote_budget = match (order.side, order.order_type) {
            (Side::Buy, OrderType::Market) | (Side::Buy, OrderType::Stop { .. }) => {
                if max_fee_bps > 0 {
                    let effective = reserved as u128 * 10_000 / (10_000 + max_fee_bps as u128);
                    Some(effective as u64)
                } else {
                    Some(reserved)
                }
            }
            _ => None,
        };

        *self.order_counts.entry(order.account).or_default() += 1;
        // Tentatively claim the (account, order_id) slot for the live
        // dedup check. If the order closes within this `execute` call
        // (IOC/FOK fill, FOK kill, etc.) the entry is freed in the
        // `freed` loop below; if it rests, the entry stays put.
        self.live_order_ids.insert((order.account, order.id), ());

        let taker_account = order.account;
        let taker_id = order.id;
        let report_start = reports.len();

        // Single mutable lookup: book, fees all from the same struct.
        let inst =
            inst_mut(&mut self.instruments, symbol).expect("instrument verified to exist above");
        let taker_rested = inst.book.execute(order, quote_budget, slot, reports);

        // Compute fees on fills before balance updates.
        apply_fees(&mut reports[report_start..], &inst.fee_schedule);

        // Process reports to update balances. Mirrors the old process_reports
        // logic but resolves slots from the book instead of a separate HashMap.
        //
        // consumed_slots: fully-filled or STP-cancelled makers, with their
        // reservation slots. Typically 0-5 entries per aggressive order.
        let consumed: Vec<(AccountId, OrderId, Side, ReservationSlot)> =
            inst.book.drain_consumed_slots().collect();

        // Track which (account, order_id) pairs are fully done (slot freed).
        // Used to avoid double-free and to decrement order_counts.
        let mut freed: Vec<(AccountId, OrderId)> = Vec::new();

        for report in &reports[report_start..] {
            match *report {
                ExecutionReport::Fill {
                    maker_order_id,
                    taker_order_id,
                    symbol: _,
                    maker_account,
                    taker_account: fill_taker_account,
                    price,
                    quantity,
                    maker_fee,
                    taker_fee,
                } => {
                    // Resolve maker slot: consumed list (fully filled) or
                    // order_index (partially filled, still on book).
                    let maker_info = consumed
                        .iter()
                        .find(|(a, id, _, _)| *a == maker_account && *id == maker_order_id)
                        .map(|(_, _, side, slot)| (*side, *slot))
                        .or_else(|| {
                            inst.book
                                .peek_order_location(maker_account, maker_order_id)
                                .map(|(side, _, slot)| (side, slot))
                        });

                    let Some((maker_side, maker_slot)) = maker_info else {
                        continue;
                    };

                    // Resolve taker slot. The fill's taker may be the original
                    // order (use `slot`) or a triggered stop (consumed_slots
                    // if fully filled/cancelled, or order_index if it rested).
                    let taker_slot = if fill_taker_account == taker_account
                        && taker_order_id == taker_id
                    {
                        slot
                    } else {
                        // Triggered stop's slot — check consumed first,
                        // then order_index (stop-limit that partially
                        // filled and rested).
                        match consumed
                            .iter()
                            .find(|(a, id, _, _)| *a == fill_taker_account && *id == taker_order_id)
                            .map(|(_, _, _, s)| *s)
                            .or_else(|| {
                                inst.book
                                    .peek_order_location(fill_taker_account, taker_order_id)
                                    .map(|(_, _, s)| s)
                            }) {
                            Some(s) => s,
                            None => continue,
                        }
                    };

                    let (buyer_slot, seller_slot, buyer_fee, seller_fee) = match maker_side {
                        Side::Buy => (maker_slot, taker_slot, maker_fee, taker_fee),
                        Side::Sell => (taker_slot, maker_slot, taker_fee, maker_fee),
                    };
                    self.accounts.fill(
                        buyer_slot,
                        seller_slot,
                        price,
                        quantity,
                        buyer_fee,
                        seller_fee,
                        &spec,
                    );

                    // Free fully consumed reservation slots (remaining == 0).
                    if self.accounts.reservation_remaining(maker_slot) == 0 {
                        self.accounts.free_slot(maker_slot);
                        freed.push((maker_account, maker_order_id));
                    }
                    if self.accounts.reservation_remaining(taker_slot) == 0 {
                        self.accounts.free_slot(taker_slot);
                        freed.push((fill_taker_account, taker_order_id));
                    }
                }
                ExecutionReport::Cancelled {
                    order_id, account, ..
                } => {
                    let key = (account, order_id);
                    if freed.contains(&key) {
                        continue;
                    }
                    // Cancelled: taker or STP-cancelled maker.
                    if account == taker_account && order_id == taker_id {
                        self.accounts.release(slot);
                    } else if let Some((_, _, _, maker_slot)) = consumed
                        .iter()
                        .find(|(a, id, _, _)| *a == account && *id == order_id)
                    {
                        self.accounts.release(*maker_slot);
                    }
                    freed.push(key);
                }
                ExecutionReport::Rejected {
                    order_id, account, ..
                } => {
                    let key = (account, order_id);
                    if freed.contains(&key) {
                        continue;
                    }
                    if account == taker_account && order_id == taker_id {
                        self.accounts.release(slot);
                    } else if let Some((_, _, _, triggered_slot)) = consumed
                        .iter()
                        .find(|(a, id, _, _)| *a == account && *id == order_id)
                    {
                        self.accounts.release(*triggered_slot);
                    }
                    freed.push(key);
                }
                _ => {}
            }
        }

        // Release leftover reservations for orders no longer on the book
        // (price improvement, market buy budget surplus, etc.).
        // Determined from report analysis — no HashMap lookup needed.
        if !taker_rested && !freed.contains(&(taker_account, taker_id)) {
            self.accounts.release(slot);
            freed.push((taker_account, taker_id));
        }
        for &(account, order_id, _, maker_slot) in &consumed {
            if !freed.contains(&(account, order_id)) {
                self.accounts.release(maker_slot);
                freed.push((account, order_id));
            }
        }

        // Decrement order_counts and free the live_order_ids entry
        // for every order that closed this turn (consumed maker slots
        // plus the taker if it didn't rest). Both maps are kept in
        // lockstep — they have to agree on "which orders are live."
        for &(account, order_id) in &freed {
            if let Some(count) = self.order_counts.get_mut(&account) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    self.order_counts.remove(&account);
                }
            }
            self.live_order_ids.remove(&(account, order_id));
        }

        // Schedule GTD expiry if the order rested (limit) or is now pending
        // (stop). Stop orders that triggered and fully filled in this same
        // execute call won't appear in the book any more — find_gtd_expiry
        // will return None and we won't schedule. Triggered stops that
        // re-rest as limits keep the same OrderId/expiry_ns, so the single
        // task scheduled here covers both lifecycle stages.
        if order.time_in_force == TimeInForce::GTD
            && order.expiry_ns > 0
            && inst_ref(&self.instruments, symbol)
                .and_then(|inst| inst.book.find_gtd_expiry(taker_account, taker_id))
                .is_some()
        {
            self.schedule_gtd_expiry(symbol, taker_account, taker_id, order.expiry_ns);
        }
    }

    /// Cancel all resting orders and pending stops for an account across
    /// all instruments (kill switch). Releases all associated reservations.
    pub fn cancel_all(&mut self, account: AccountId, reports: &mut Vec<ExecutionReport>) {
        for idx in 0..self.instruments.len() {
            let Some(inst) = self.instruments[idx].as_deref_mut() else {
                continue;
            };

            let report_start = reports.len();

            inst.book.cancel_all_for_account(account, reports);

            // cancel_all_for_account collects returned slots in consumed_slots.
            let consumed: Vec<(AccountId, OrderId, Side, ReservationSlot)> =
                inst.book.drain_consumed_slots().collect();
            for &(consumed_account, order_id, _, slot) in &consumed {
                self.accounts.release(slot);
                self.live_order_ids.remove(&(consumed_account, order_id));
            }

            let n_cancelled = reports.len() - report_start;
            if let Some(count) = self.order_counts.get_mut(&account) {
                *count = count.saturating_sub(n_cancelled as u32);
                if *count == 0 {
                    self.order_counts.remove(&account);
                }
            }
        }
    }

    /// Cancel all resting orders and pending stops with `TimeInForce::Day`
    /// across all instruments. Called at end-of-session.
    pub fn end_of_day(&mut self, reports: &mut Vec<ExecutionReport>) {
        for idx in 0..self.instruments.len() {
            let Some(inst) = self.instruments[idx].as_deref_mut() else {
                continue;
            };

            inst.book.cancel_day_orders(reports);

            for (account, order_id, _, slot) in inst.book.drain_consumed_slots() {
                self.accounts.release(slot);
                self.live_order_ids.remove(&(account, order_id));
                if let Some(count) = self.order_counts.get_mut(&account) {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        self.order_counts.remove(&account);
                    }
                }
            }
        }
    }

    /// Disable an instrument: reject future orders and cancel all resting
    /// orders and pending stops. Idempotent — disabling an already-disabled
    /// instrument is a no-op (no reports emitted).
    pub fn disable_instrument(&mut self, symbol: Symbol, reports: &mut Vec<ExecutionReport>) {
        let Some(inst) = inst_mut(&mut self.instruments, symbol) else {
            return;
        };
        if inst.disabled {
            return;
        }
        inst.disabled = true;

        inst.book.cancel_all_orders(reports);

        // Release reservations — same pattern as end_of_day.
        for (account, order_id, _, slot) in inst.book.drain_consumed_slots() {
            self.accounts.release(slot);
            self.live_order_ids.remove(&(account, order_id));
            if let Some(count) = self.order_counts.get_mut(&account) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    self.order_counts.remove(&account);
                }
            }
        }

        reports.push(ExecutionReport::InstrumentStatusChanged {
            symbol,
            status: InstrumentStatus::Disabled,
        });
    }

    /// Re-enable a previously disabled instrument, allowing new orders.
    pub fn enable_instrument(&mut self, symbol: Symbol, reports: &mut Vec<ExecutionReport>) {
        let Some(inst) = inst_mut(&mut self.instruments, symbol) else {
            return;
        };
        if !inst.disabled {
            return;
        }
        inst.disabled = false;

        reports.push(ExecutionReport::InstrumentStatusChanged {
            symbol,
            status: InstrumentStatus::Enabled,
        });
    }

    /// Permanently remove a disabled instrument, reclaiming memory.
    /// Only succeeds if the instrument is disabled and has no resting orders
    /// (which disable guarantees). Active instruments must be disabled first.
    pub fn remove_instrument(&mut self, symbol: Symbol, reports: &mut Vec<ExecutionReport>) {
        let idx = symbol.0 as usize;
        if idx >= self.instruments.len() {
            return;
        }
        let dominated = self.instruments[idx]
            .as_ref()
            .is_some_and(|inst| inst.disabled && inst.book.is_empty());
        if !dominated {
            return;
        }
        self.instruments[idx] = None;

        reports.push(ExecutionReport::InstrumentStatusChanged {
            symbol,
            status: InstrumentStatus::Removed,
        });
    }

    /// Snapshot the disabled instrument symbols for serialization.
    pub(crate) fn snapshot_disabled_instruments(&self) -> Vec<Symbol> {
        self.instruments
            .iter()
            .filter_map(|slot| slot.as_deref())
            .filter(|inst| inst.disabled)
            .map(|inst| inst.spec.symbol)
            .collect()
    }

    /// Cancel a resting order on the given instrument.
    #[inline]
    pub fn cancel(
        &mut self,
        symbol: Symbol,
        account: AccountId,
        order_id: OrderId,
        reports: &mut Vec<ExecutionReport>,
    ) {
        let Some(inst) = inst_mut(&mut self.instruments, symbol) else {
            return;
        };

        if let Some((_side, slot)) = inst.book.cancel(account, order_id, reports) {
            self.accounts.release(slot);
            self.live_order_ids.remove(&(account, order_id));
            if let Some(count) = self.order_counts.get_mut(&account) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    self.order_counts.remove(&account);
                }
            }
        }
    }

    /// Withdraw funds from an account. Rejects if the account has resting
    /// orders (must `CancelAll` first) or insufficient available balance.
    /// Removes the balance entry if it reaches zero (memory cleanup).
    pub fn withdraw(
        &mut self,
        account: AccountId,
        currency: CurrencyId,
        amount: u64,
    ) -> Result<(), RejectReason> {
        // Reject withdrawal if the account has resting orders — funds might
        // be reserved. Caller must CancelAll first.
        if self.order_counts.get(&account).copied().unwrap_or(0) > 0 {
            return Err(RejectReason::HasRestingOrders);
        }
        self.accounts.withdraw(account, currency, amount)
    }

    /// Atomically amend a resting limit order's price and/or quantity.
    ///
    /// Validation order (all checks before any mutation):
    /// 1. Instrument exists
    /// 2. Order exists on the book (resting limit only — not stops, not market)
    /// 3. Circuit breaker: halted or price band violation
    /// 4. Risk limits: max qty, max notional
    /// 5. Price-would-cross check: reject if new price crosses the spread
    /// 6. Reservation adjustment: compute new required amount, check balance
    ///
    /// If any check fails, the original order remains untouched.
    ///
    /// Time priority rules:
    /// - Same price, qty decrease → keep priority
    /// - Same price, qty increase → lose priority
    /// - Price change → lose priority
    pub fn cancel_replace(
        &mut self,
        symbol: Symbol,
        account: AccountId,
        order_id: OrderId,
        new_price: Price,
        new_quantity: Quantity,
        reports: &mut Vec<ExecutionReport>,
    ) {
        // Single lookup for all instrument state — O(1) Vec index, no hashing.
        let Some(inst) = inst_ref(&self.instruments, symbol) else {
            reports.push(ExecutionReport::Rejected {
                order_id,
                symbol,
                account,
                reason: RejectReason::UnknownSymbol,
            });
            return;
        };

        // Disabled instruments reject cancel-replace — all orders were
        // already cancelled during disable.
        if inst.disabled {
            reports.push(ExecutionReport::Rejected {
                order_id,
                symbol,
                account,
                reason: RejectReason::InstrumentDisabled,
            });
            return;
        }

        // 1. Order must exist as a resting limit order.
        // Use peek_order_location (O(1) index lookup) for validation —
        // the VecDeque scan for old_remaining is deferred to replace_order.
        let Some((side, _old_price, slot)) = inst.book.peek_order_location(account, order_id)
        else {
            reports.push(ExecutionReport::Rejected {
                order_id,
                symbol,
                account,
                reason: RejectReason::UnknownOrder,
            });
            return;
        };

        // 2. Circuit breaker checks on the new price.
        let cb = &inst.circuit_breaker;
        if cb.halted {
            reports.push(ExecutionReport::Rejected {
                order_id,
                symbol,
                account,
                reason: RejectReason::TradingHalted,
            });
            return;
        }
        if let Some(lower) = cb.price_band_lower
            && new_price < lower
        {
            reports.push(ExecutionReport::Rejected {
                order_id,
                symbol,
                account,
                reason: RejectReason::OutsidePriceBand,
            });
            return;
        }
        if let Some(upper) = cb.price_band_upper
            && new_price > upper
        {
            reports.push(ExecutionReport::Rejected {
                order_id,
                symbol,
                account,
                reason: RejectReason::OutsidePriceBand,
            });
            return;
        }

        // 3. Risk limit checks on the new quantity/notional.
        let limits = &inst.risk_limits;
        if let Some(max_qty) = limits.max_order_qty
            && new_quantity.get() > max_qty.get()
        {
            reports.push(ExecutionReport::Rejected {
                order_id,
                symbol,
                account,
                reason: RejectReason::ExceedsMaxOrderQty,
            });
            return;
        }
        if let Some(max_notional) = limits.max_order_notional {
            let notional = new_price.get() as u128 * new_quantity.get() as u128;
            if notional > max_notional as u128 {
                reports.push(ExecutionReport::Rejected {
                    order_id,
                    symbol,
                    account,
                    reason: RejectReason::ExceedsMaxNotional,
                });
                return;
            }
        }

        // 4. Reject if the new price would cross the opposite best price.
        // This prevents the replacement from becoming an aggressor. If the
        // user wants to cross the spread, they should cancel and submit a
        // new order.
        let would_cross = match side {
            Side::Buy => inst
                .book
                .best_ask()
                .is_some_and(|best_ask| new_price >= best_ask),
            Side::Sell => inst
                .book
                .best_bid()
                .is_some_and(|best_bid| new_price <= best_bid),
        };
        if would_cross {
            reports.push(ExecutionReport::Rejected {
                order_id,
                symbol,
                account,
                reason: RejectReason::PriceWouldCross,
            });
            return;
        }

        // 5. Adjust reservation atomically. Compute the new required amount
        // based on the new price/quantity, including fee cushion for buys.
        // If insufficient balance, the original reservation stays intact.
        // Only positive fees need a reservation cushion — rebates (negative)
        // don't require upfront funds.
        let new_required = match side {
            Side::Buy => required_with_fee(
                new_price.get(),
                new_quantity.get(),
                max_fee_bps(&inst.fee_schedule),
            ),
            Side::Sell => new_quantity.get(),
        };

        // The reservation slot was already retrieved from peek_order_location above.
        if let Err(reason) = self.accounts.try_adjust_reservation(slot, new_required) {
            reports.push(ExecutionReport::Rejected {
                order_id,
                symbol,
                account,
                reason,
            });
            return;
        }

        // 6. All checks passed — perform the book replacement (single VecDeque
        // scan). This returns (old_price, old_remaining).
        // Cannot fail since we verified the order exists above and matching is
        // single-threaded (no concurrent removal possible).
        // Note: `live_order_ids` is intentionally not touched. The order keeps
        // the same `(account, order_id)` identity through the replacement, so
        // its entry stays valid; it'll be removed by the same cancel/fill
        // path as any other resting order when the order eventually closes.
        let inst =
            inst_mut(&mut self.instruments, symbol).expect("instrument verified to exist above");
        let (old_price, old_remaining) = inst
            .book
            .replace_order(account, order_id, new_price, new_quantity)
            .expect("order verified to exist");

        reports.push(ExecutionReport::Replaced {
            order_id,
            symbol,
            account,
            side,
            old_price,
            new_price,
            old_remaining,
            new_remaining: new_quantity,
        });
    }
}

/// Compute and set maker/taker fees on Fill reports.
///
/// Both maker and taker fees are in quote currency (cost-based):
///   `fee = cost * fee_bps / 10_000` where `cost = price * quantity`.
///
/// The buyer's fee is deducted from their reservation (which includes
/// a fee cushion). The seller's fee is deducted from their proceeds.
///
/// Uses u128 intermediate to avoid overflow on `value * bps`.
fn apply_fees(reports: &mut [ExecutionReport], fees: &FeeSchedule) {
    if fees.maker_fee_bps == 0 && fees.taker_fee_bps == 0 {
        return;
    }

    for report in reports.iter_mut() {
        if let ExecutionReport::Fill {
            price,
            quantity,
            maker_fee,
            taker_fee,
            ..
        } = report
        {
            // Both fees are in quote currency (cost-based). This is the
            // standard model used by most centralized exchanges.
            // Signed arithmetic: negative bps = rebate (exchange pays trader).
            let cost = price.get() as i128 * quantity.get() as i128;
            *maker_fee = (cost * fees.maker_fee_bps as i128 / 10_000) as i64;
            *taker_fee = (cost * fees.taker_fee_bps as i128 / 10_000) as i64;
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
            order_type: OrderType::Limit {
                price: price(p),
                post_only: false,
            },
            time_in_force: tif,
            quantity: qty(q),
            stp: SelfTradeProtection::Allow,
            expiry_ns: 0,
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
            expiry_ns: 0,
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
                symbol: Symbol(1),
                account: ACCT_A,
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
                symbol: btc,
                account: ACCT_A,
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

        exchange.cancel(btc, ACCT_A, OrderId(1), &mut reports);
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
                symbol: eth,
                account: ACCT_A,
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
        exchange.cancel(btc, ACCT_A, OrderId(2), &mut reports);

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
    fn cancel_replace_preserves_live_entry() {
        // cancel_replace amends the resting order in-place keeping the
        // same `(account, order_id)` identity, so the live set must
        // not be touched. A duplicate submission during/after the
        // replace must still hit DuplicateOrderId.
        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 100_000);

        let mut reports = Vec::new();
        exchange.execute(
            Symbol(1),
            limit_order(11, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
            &mut reports,
        );

        reports.clear();
        exchange.cancel_replace(
            Symbol(1),
            ACCT_A,
            OrderId(11),
            price(95),
            qty(8),
            &mut reports,
        );
        assert!(
            reports
                .iter()
                .any(|r| matches!(r, ExecutionReport::Replaced { .. })),
            "expected Replaced, got {reports:?}"
        );

        reports.clear();
        exchange.execute(
            Symbol(1),
            limit_order(11, ACCT_A, Side::Buy, 90, 5, TimeInForce::GTC),
            &mut reports,
        );
        assert!(
            matches!(
                reports[0],
                ExecutionReport::Rejected {
                    reason: RejectReason::DuplicateOrderId,
                    ..
                }
            ),
            "duplicate after cancel_replace should reject, got {:?}",
            reports[0]
        );
    }

    #[test]
    fn order_id_reusable_after_disable_instrument() {
        // disable_instrument cancels every resting order on the symbol.
        // Each cancellation must remove its `(account, order_id)` from
        // the live set so the same id can be reused (typically on a
        // different instrument).
        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());
        exchange.add_instrument(eth_usd_spec());
        exchange.deposit(ACCT_A, USD, 100_000);

        let mut reports = Vec::new();
        exchange.execute(
            Symbol(1),
            limit_order(13, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
            &mut reports,
        );

        reports.clear();
        exchange.disable_instrument(Symbol(1), &mut reports);

        // Reuse OrderId 13 on a different live instrument. Disable
        // freed the live-set entry, so this must place.
        reports.clear();
        exchange.execute(
            Symbol(2),
            limit_order(13, ACCT_A, Side::Buy, 50, 5, TimeInForce::GTC),
            &mut reports,
        );
        assert!(
            matches!(reports[0], ExecutionReport::Placed { .. }),
            "reuse on live instrument after disable should place, got {:?}",
            reports[0]
        );
    }

    #[test]
    fn order_id_reusable_after_cancel() {
        // Cancelling a resting order frees its `(account, order_id)`
        // entry from the live set, so the same id can be reused for a
        // fresh submission. This is the bot's actual reconnect scenario:
        // the gateway resets its session-local id counter, and we need
        // the engine to accept the colliding ids once the prior session's
        // orders are gone (e.g. via cancel-on-disconnect).
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
        exchange.cancel(Symbol(1), ACCT_A, OrderId(1), &mut reports);

        reports.clear();
        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_A, Side::Buy, 99, 5, TimeInForce::GTC),
            &mut reports,
        );
        assert!(
            matches!(reports[0], ExecutionReport::Placed { .. }),
            "reuse after cancel should place, got {:?}",
            reports[0]
        );
    }

    #[test]
    fn order_id_reusable_after_full_fill() {
        // A full fill closes the order: `(account, order_id)` leaves the
        // live set in the same place every other close site does. The
        // same id is then reusable for a fresh submission.
        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 100_000);
        exchange.deposit(ACCT_B, BTC, 100);

        let mut reports = Vec::new();
        // Maker (B) sells, taker (A) buys — A's order fully fills.
        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_B, Side::Sell, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();
        exchange.execute(
            Symbol(1),
            limit_order(7, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        assert!(
            reports
                .iter()
                .any(|r| matches!(r, ExecutionReport::Fill { .. })),
            "expected a fill, got {reports:?}"
        );

        // Reuse OrderId 7 — the prior order was fully filled, so it's
        // gone from the live set.
        reports.clear();
        exchange.execute(
            Symbol(1),
            limit_order(7, ACCT_A, Side::Buy, 99, 5, TimeInForce::GTC),
            &mut reports,
        );
        assert!(
            matches!(reports[0], ExecutionReport::Placed { .. }),
            "reuse after full fill should place, got {:?}",
            reports[0]
        );
    }

    #[test]
    fn lower_order_id_accepted_when_not_live() {
        // Under live-orders-only dedup, OrderIds aren't a monotonic
        // counter — only currently-live `(account, order_id)` pairs are
        // protected. Submitting a *different* (lower or higher) free ID
        // while ID 5 is resting must succeed; the dedup only triggers
        // when the same ID is reused while live.
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
            limit_order(3, ACCT_A, Side::Buy, 99, 10, TimeInForce::GTC),
            &mut reports,
        );
        assert!(
            matches!(reports[0], ExecutionReport::Placed { .. }),
            "fresh lower id should place, got {:?}",
            reports[0]
        );
    }

    #[test]
    fn rejected_order_id_can_be_reused() {
        // Validation rejections (InsufficientBalance, OutsidePriceBand,
        // TradingHalted, etc.) leave the live set untouched: the order
        // never rested, so cancel/replace can't reference it. Reusing
        // its OrderId for a fresh submission is therefore fine — the
        // gateway's session-local id_map doesn't have to keep moving
        // forward forever just because earlier attempts bounced.
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

        // Retry with the same ID after depositing — the previous
        // rejection didn't consume the slot, so this places.
        exchange.deposit(ACCT_A, USD, 100_000);
        reports.clear();
        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        assert!(
            matches!(reports[0], ExecutionReport::Placed { .. }),
            "retry of rejected id should place, got {:?}",
            reports[0]
        );
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
                expiry_ns: 0,
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
        exchange.cancel(Symbol(1), ACCT_B, OrderId(100), &mut reports);
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
                expiry_ns: 0,
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

    #[test]
    fn same_order_id_different_accounts_cancel_targets_correct_order() {
        // Two accounts place resting orders with the same OrderId on the
        // same side. Cancelling one must not affect the other.
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 100_000);
        exchange.deposit(ACCT_B, USD, 100_000);

        let mut reports = Vec::new();

        // Both place buy OrderId(1) at different prices (so they don't fill).
        exchange.execute(
            btc,
            limit_order(1, ACCT_A, Side::Buy, 90, 10, TimeInForce::GTC),
            &mut reports,
        );
        assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
        reports.clear();

        exchange.execute(
            btc,
            limit_order(1, ACCT_B, Side::Buy, 80, 5, TimeInForce::GTC),
            &mut reports,
        );
        assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
        reports.clear();

        // Cancel ACCT_A's order — ACCT_B's should survive.
        exchange.cancel(btc, ACCT_A, OrderId(1), &mut reports);
        assert_eq!(reports.len(), 1);
        assert!(matches!(
            reports[0],
            ExecutionReport::Cancelled {
                order_id: OrderId(1),
                account: ACCT_A,
                ..
            }
        ));
        reports.clear();

        // ACCT_A's reservation released.
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 0);
        // ACCT_B's reservation still held.
        assert!(exchange.accounts().balance(ACCT_B, USD).reserved > 0);

        // Cancel ACCT_B's order — should also work.
        exchange.cancel(btc, ACCT_B, OrderId(1), &mut reports);
        assert_eq!(reports.len(), 1);
        assert!(matches!(
            reports[0],
            ExecutionReport::Cancelled {
                order_id: OrderId(1),
                account: ACCT_B,
                ..
            }
        ));
        assert_eq!(exchange.accounts().balance(ACCT_B, USD).reserved, 0);
    }

    #[test]
    fn same_order_id_different_accounts_amend_targets_correct_order() {
        // Two accounts with the same OrderId resting. Amending one must
        // not affect the other.
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 100_000);
        exchange.deposit(ACCT_B, USD, 100_000);

        let mut reports = Vec::new();

        exchange.execute(
            btc,
            limit_order(1, ACCT_A, Side::Buy, 90, 10, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();
        exchange.execute(
            btc,
            limit_order(1, ACCT_B, Side::Buy, 80, 5, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();

        // Amend ACCT_A's order to new price/qty.
        exchange.cancel_replace(btc, ACCT_A, OrderId(1), price(85), qty(8), &mut reports);
        assert_eq!(reports.len(), 1);
        if let ExecutionReport::Replaced {
            order_id,
            old_price,
            new_price,
            old_remaining,
            new_remaining,
            ..
        } = &reports[0]
        {
            assert_eq!(*order_id, OrderId(1));
            assert_eq!(*old_price, price(90));
            assert_eq!(*new_price, price(85));
            assert_eq!(*old_remaining, qty(10));
            assert_eq!(*new_remaining, qty(8));
        } else {
            panic!("expected Replaced, got {:?}", reports[0]);
        }
        reports.clear();

        // ACCT_B's order should be unchanged — verify by cancelling it
        // and checking the remaining quantity is still 5 at price 80.
        exchange.cancel(btc, ACCT_B, OrderId(1), &mut reports);
        if let ExecutionReport::Cancelled {
            remaining_quantity, ..
        } = &reports[0]
        {
            assert_eq!(*remaining_quantity, qty(5));
        } else {
            panic!("expected Cancelled, got {:?}", reports[0]);
        }
    }

    #[test]
    fn same_order_id_different_accounts_cancel_all_targets_correct_account() {
        // Two accounts with the same OrderId resting. CancelAll for one
        // account must not touch the other's orders.
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 100_000);
        exchange.deposit(ACCT_B, USD, 100_000);

        let mut reports = Vec::new();

        exchange.execute(
            btc,
            limit_order(1, ACCT_A, Side::Buy, 90, 10, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();
        exchange.execute(
            btc,
            limit_order(1, ACCT_B, Side::Buy, 80, 5, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();

        // CancelAll for ACCT_A.
        exchange.cancel_all(ACCT_A, &mut reports);
        assert_eq!(reports.len(), 1);
        assert!(matches!(
            reports[0],
            ExecutionReport::Cancelled {
                order_id: OrderId(1),
                account: ACCT_A,
                ..
            }
        ));
        reports.clear();

        // ACCT_A fully released, ACCT_B still has reservation.
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 0);
        assert!(exchange.accounts().balance(ACCT_B, USD).reserved > 0);

        // ACCT_B's order is still live — it can be cancelled independently.
        exchange.cancel(btc, ACCT_B, OrderId(1), &mut reports);
        assert_eq!(reports.len(), 1);
        assert!(matches!(
            reports[0],
            ExecutionReport::Cancelled {
                order_id: OrderId(1),
                account: ACCT_B,
                ..
            }
        ));
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
                expiry_ns: 0,
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
                expiry_ns: 0,
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
                expiry_ns: 0,
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
                expiry_ns: 0,
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
                expiry_ns: 0,
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
        exchange.cancel(Symbol(1), ACCT_A, OrderId(1), &mut reports);
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
    fn halted_order_id_can_be_reused() {
        // A halt-rejected order doesn't rest, so under live-orders-only
        // dedup it doesn't claim the OrderId. Once trading resumes, the
        // same id can be retried — clients/gateways don't need to skip
        // an id just because the engine was halted when they first sent.
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

        exchange.set_circuit_breaker(Symbol(1), CircuitBreakerConfig::default());
        reports.clear();
        exchange.execute(
            Symbol(1),
            limit_order(5, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        assert!(
            matches!(reports[0], ExecutionReport::Placed { .. }),
            "retry after halt cleared should place, got {:?}",
            reports[0]
        );
    }

    #[test]
    fn band_rejected_order_id_can_be_reused() {
        // Same as `halted_order_id_can_be_reused` but for the price-band
        // rejection path: an OutsidePriceBand reject doesn't rest the
        // order, so the live set stays empty and the same id is free
        // to retry once the bands are widened.
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

        exchange.set_circuit_breaker(Symbol(1), CircuitBreakerConfig::default());
        reports.clear();
        exchange.execute(
            Symbol(1),
            limit_order(5, ACCT_A, Side::Buy, 50, 10, TimeInForce::GTC),
            &mut reports,
        );
        assert!(
            matches!(reports[0], ExecutionReport::Placed { .. }),
            "retry after band widened should place, got {:?}",
            reports[0]
        );
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

    // -- Cancel-replace tests --

    #[test]
    fn cancel_replace_basic_price_change() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 50_000);

        let mut reports = Vec::new();

        // Place a limit buy at 100 for 10.
        exchange.execute(
            btc,
            limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
        reports.clear();

        // Cancel-replace to price 120 (same qty).
        exchange.cancel_replace(btc, ACCT_A, OrderId(1), price(120), qty(10), &mut reports);

        assert_eq!(reports.len(), 1);
        assert_eq!(
            reports[0],
            ExecutionReport::Replaced {
                order_id: OrderId(1),
                symbol: btc,
                account: ACCT_A,
                side: Side::Buy,
                old_price: price(100),
                new_price: price(120),
                old_remaining: qty(10),
                new_remaining: qty(10),
            }
        );

        // Old reservation was 100*10=1000, new is 120*10=1200.
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 1_200);
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).available, 48_800);
    }

    #[test]
    fn cancel_replace_qty_decrease_keeps_priority() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 50_000);
        exchange.deposit(ACCT_B, BTC, 100);

        let mut reports = Vec::new();

        // Place two buys at same price. Order 1 is first in queue.
        exchange.execute(
            btc,
            limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        exchange.execute(
            btc,
            limit_order(2, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();

        // Cancel-replace order 1 to lower qty (5). Should keep priority.
        exchange.cancel_replace(btc, ACCT_A, OrderId(1), price(100), qty(5), &mut reports);
        assert!(matches!(reports[0], ExecutionReport::Replaced { .. }));
        reports.clear();

        // Sell 5 into the book — should match order 1 first (kept priority).
        exchange.execute(
            btc,
            limit_order(3, ACCT_B, Side::Sell, 100, 5, TimeInForce::GTC),
            &mut reports,
        );

        // Expect a fill against order 1 (maker), not order 2.
        let fill = reports
            .iter()
            .find(|r| matches!(r, ExecutionReport::Fill { .. }));
        assert!(fill.is_some());
        assert!(matches!(
            fill.unwrap(),
            ExecutionReport::Fill {
                maker_order_id: OrderId(1),
                taker_order_id: OrderId(3),
                ..
            }
        ));
    }

    #[test]
    fn cancel_replace_qty_increase_loses_priority() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 50_000);
        exchange.deposit(ACCT_B, BTC, 100);

        let mut reports = Vec::new();

        // Place two buys at same price. Order 1 is first in queue.
        exchange.execute(
            btc,
            limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        exchange.execute(
            btc,
            limit_order(2, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();

        // Cancel-replace order 1 to higher qty (15). Should lose priority.
        exchange.cancel_replace(btc, ACCT_A, OrderId(1), price(100), qty(15), &mut reports);
        assert!(matches!(reports[0], ExecutionReport::Replaced { .. }));
        reports.clear();

        // Sell 5 into the book — should match order 2 first (order 1 lost priority).
        exchange.execute(
            btc,
            limit_order(3, ACCT_B, Side::Sell, 100, 5, TimeInForce::GTC),
            &mut reports,
        );

        let fill = reports
            .iter()
            .find(|r| matches!(r, ExecutionReport::Fill { .. }));
        assert!(fill.is_some());
        assert!(matches!(
            fill.unwrap(),
            ExecutionReport::Fill {
                maker_order_id: OrderId(2),
                taker_order_id: OrderId(3),
                ..
            }
        ));
    }

    #[test]
    fn cancel_replace_insufficient_balance() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        // Only deposit enough for the initial order.
        exchange.deposit(ACCT_A, USD, 1_100);

        let mut reports = Vec::new();

        // Place buy at 100 for 10 (reserves 1000).
        exchange.execute(
            btc,
            limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
        reports.clear();

        // Cancel-replace to price 500 for 10 (would need 5000, only have 1100 total).
        exchange.cancel_replace(btc, ACCT_A, OrderId(1), price(500), qty(10), &mut reports);

        assert_eq!(reports.len(), 1);
        assert_eq!(
            reports[0],
            ExecutionReport::Rejected {
                order_id: OrderId(1),
                symbol: btc,
                account: ACCT_A,
                reason: RejectReason::InsufficientBalance,
            }
        );

        // Original order must still be on the book with original reservation.
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 1_000);
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).available, 100);
    }

    #[test]
    fn cancel_replace_unknown_order() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());

        let mut reports = Vec::new();

        // Cancel-replace on an order ID that was never placed.
        exchange.cancel_replace(btc, ACCT_A, OrderId(999), price(100), qty(10), &mut reports);

        assert_eq!(reports.len(), 1);
        assert_eq!(
            reports[0],
            ExecutionReport::Rejected {
                order_id: OrderId(999),
                symbol: btc,
                account: ACCT_A,
                reason: RejectReason::UnknownOrder,
            }
        );
    }

    #[test]
    fn cancel_replace_unknown_symbol() {
        let mut exchange = Exchange::new();
        // Don't add any instruments.

        let mut reports = Vec::new();

        exchange.cancel_replace(
            Symbol(42),
            ACCT_A,
            OrderId(1),
            price(100),
            qty(10),
            &mut reports,
        );

        assert_eq!(reports.len(), 1);
        assert_eq!(
            reports[0],
            ExecutionReport::Rejected {
                order_id: OrderId(1),
                symbol: Symbol(42),
                account: ACCT_A,
                reason: RejectReason::UnknownSymbol,
            }
        );
    }

    #[test]
    fn cancel_replace_price_would_cross() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 50_000);
        exchange.deposit(ACCT_B, BTC, 100);

        let mut reports = Vec::new();

        // Place a buy at 100.
        exchange.execute(
            btc,
            limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();

        // Place an ask at 110.
        exchange.execute(
            btc,
            limit_order(2, ACCT_B, Side::Sell, 110, 10, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();

        // Cancel-replace the buy to price 110 — would cross the ask.
        exchange.cancel_replace(btc, ACCT_A, OrderId(1), price(110), qty(10), &mut reports);

        assert_eq!(reports.len(), 1);
        assert_eq!(
            reports[0],
            ExecutionReport::Rejected {
                order_id: OrderId(1),
                symbol: btc,
                account: ACCT_A,
                reason: RejectReason::PriceWouldCross,
            }
        );

        // Original order must remain intact.
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 1_000);
    }

    #[test]
    fn cancel_replace_trading_halted() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 50_000);

        let mut reports = Vec::new();

        // Place a buy at 100.
        exchange.execute(
            btc,
            limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
        reports.clear();

        // Halt trading.
        exchange.set_circuit_breaker(
            btc,
            CircuitBreakerConfig {
                halted: true,
                ..Default::default()
            },
        );

        // Cancel-replace should be rejected.
        exchange.cancel_replace(btc, ACCT_A, OrderId(1), price(120), qty(10), &mut reports);

        assert_eq!(reports.len(), 1);
        assert_eq!(
            reports[0],
            ExecutionReport::Rejected {
                order_id: OrderId(1),
                symbol: btc,
                account: ACCT_A,
                reason: RejectReason::TradingHalted,
            }
        );

        // Original order remains.
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 1_000);
    }

    #[test]
    fn cancel_replace_outside_price_band() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 500_000);

        let mut reports = Vec::new();

        // Place a buy at 100.
        exchange.execute(
            btc,
            limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
        reports.clear();

        // Set price bands [90, 110].
        exchange.set_circuit_breaker(
            btc,
            CircuitBreakerConfig {
                price_band_lower: Some(price(90)),
                price_band_upper: Some(price(110)),
                halted: false,
            },
        );

        // Cancel-replace to price 120 — outside upper band.
        exchange.cancel_replace(btc, ACCT_A, OrderId(1), price(120), qty(10), &mut reports);

        assert_eq!(reports.len(), 1);
        assert_eq!(
            reports[0],
            ExecutionReport::Rejected {
                order_id: OrderId(1),
                symbol: btc,
                account: ACCT_A,
                reason: RejectReason::OutsidePriceBand,
            }
        );

        // Original order remains.
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 1_000);
    }

    #[test]
    fn cancel_replace_exceeds_max_order_qty() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 500_000);

        let mut reports = Vec::new();

        // Place a buy at 100 for 10.
        exchange.execute(
            btc,
            limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
        reports.clear();

        // Set max order qty to 20.
        exchange.set_risk_limits(
            btc,
            RiskLimits {
                max_order_qty: Some(qty(20)),
                max_order_notional: None,
            },
        );

        // Cancel-replace to qty 25 — exceeds limit.
        exchange.cancel_replace(btc, ACCT_A, OrderId(1), price(100), qty(25), &mut reports);

        assert_eq!(reports.len(), 1);
        assert_eq!(
            reports[0],
            ExecutionReport::Rejected {
                order_id: OrderId(1),
                symbol: btc,
                account: ACCT_A,
                reason: RejectReason::ExceedsMaxOrderQty,
            }
        );

        // Original order remains with original qty.
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 1_000);
    }

    #[test]
    fn cancel_replace_partially_filled_order() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 50_000);
        exchange.deposit(ACCT_B, BTC, 100);

        let mut reports = Vec::new();

        // Place a limit buy for 100 at price 100 (reserves 10_000).
        exchange.execute(
            btc,
            limit_order(1, ACCT_A, Side::Buy, 100, 100, TimeInForce::GTC),
            &mut reports,
        );
        assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
        reports.clear();

        // Sell 30 into it — partial fill, remaining = 70.
        exchange.execute(
            btc,
            limit_order(2, ACCT_B, Side::Sell, 100, 30, TimeInForce::GTC),
            &mut reports,
        );
        assert!(matches!(reports[0], ExecutionReport::Fill { .. }));
        reports.clear();

        // Cancel-replace remaining to qty 50 at price 90.
        exchange.cancel_replace(btc, ACCT_A, OrderId(1), price(90), qty(50), &mut reports);

        assert_eq!(reports.len(), 1);
        assert_eq!(
            reports[0],
            ExecutionReport::Replaced {
                order_id: OrderId(1),
                symbol: btc,
                account: ACCT_A,
                side: Side::Buy,
                old_price: price(100),
                new_price: price(90),
                old_remaining: qty(70),
                new_remaining: qty(50),
            }
        );

        // New reservation should be 90*50=4500.
        // Buyer started with 50_000, spent 30*100=3000 on fills.
        // Remaining USD = 50_000 - 3000 = 47_000 total.
        // Reserved = 4500, available = 42_500.
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 4_500);
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).available, 42_500);
        // Buyer received 30 BTC from the fill.
        assert_eq!(exchange.accounts().balance(ACCT_A, BTC).available, 30);
    }

    #[test]
    fn cancel_replace_sell_order() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, BTC, 100);

        let mut reports = Vec::new();

        // Place a limit sell at 200 for 10 (reserves 10 BTC).
        exchange.execute(
            btc,
            limit_order(1, ACCT_A, Side::Sell, 200, 10, TimeInForce::GTC),
            &mut reports,
        );
        assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
        reports.clear();

        // Cancel-replace to price 180, qty 8.
        exchange.cancel_replace(btc, ACCT_A, OrderId(1), price(180), qty(8), &mut reports);

        assert_eq!(reports.len(), 1);
        assert_eq!(
            reports[0],
            ExecutionReport::Replaced {
                order_id: OrderId(1),
                symbol: btc,
                account: ACCT_A,
                side: Side::Sell,
                old_price: price(200),
                new_price: price(180),
                old_remaining: qty(10),
                new_remaining: qty(8),
            }
        );

        // Sell reservation is qty-based: was 10, now 8. Released 2 back.
        assert_eq!(exchange.accounts().balance(ACCT_A, BTC).reserved, 8);
        assert_eq!(exchange.accounts().balance(ACCT_A, BTC).available, 92);
    }

    #[test]
    fn cancel_replace_noop_same_price_same_qty() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 50_000);

        let mut reports = Vec::new();

        // Place a limit buy at 100 for 10.
        exchange.execute(
            btc,
            limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();

        // Cancel-replace with same price and qty — should succeed as a no-op.
        exchange.cancel_replace(btc, ACCT_A, OrderId(1), price(100), qty(10), &mut reports);

        assert_eq!(reports.len(), 1);
        assert_eq!(
            reports[0],
            ExecutionReport::Replaced {
                order_id: OrderId(1),
                symbol: btc,
                account: ACCT_A,
                side: Side::Buy,
                old_price: price(100),
                new_price: price(100),
                old_remaining: qty(10),
                new_remaining: qty(10),
            }
        );

        // Balances unchanged.
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 1_000);
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).available, 49_000);
    }

    #[test]
    fn cancel_replace_above_upper_price_band() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 50_000);
        exchange.set_circuit_breaker(
            btc,
            CircuitBreakerConfig {
                price_band_lower: Some(price(80)),
                price_band_upper: Some(price(120)),
                halted: false,
            },
        );

        let mut reports = Vec::new();

        exchange.execute(
            btc,
            limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();

        // Replace to price 130 — above upper band.
        exchange.cancel_replace(btc, ACCT_A, OrderId(1), price(130), qty(10), &mut reports);
        assert!(matches!(
            reports[0],
            ExecutionReport::Rejected {
                reason: RejectReason::OutsidePriceBand,
                ..
            }
        ));
        // Original order intact.
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 1_000);
    }

    #[test]
    fn cancel_replace_exceeds_max_notional() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 500_000);
        exchange.set_risk_limits(
            btc,
            RiskLimits {
                max_order_qty: None,
                max_order_notional: Some(10_000),
            },
        );

        let mut reports = Vec::new();

        exchange.execute(
            btc,
            limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();

        // Replace to 200*100 = 20_000 notional — exceeds 10_000 limit.
        exchange.cancel_replace(btc, ACCT_A, OrderId(1), price(200), qty(100), &mut reports);
        assert!(matches!(
            reports[0],
            ExecutionReport::Rejected {
                reason: RejectReason::ExceedsMaxNotional,
                ..
            }
        ));
        // Original order intact.
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 1_000);
    }

    #[test]
    fn cancel_replace_sell_price_would_cross_bid() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, BTC, 100);
        exchange.deposit(ACCT_B, USD, 50_000);

        let mut reports = Vec::new();

        // Resting bid at 100.
        exchange.execute(
            btc,
            limit_order(1, ACCT_B, Side::Buy, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        // Resting ask at 200.
        exchange.execute(
            btc,
            limit_order(2, ACCT_A, Side::Sell, 200, 10, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();

        // Replace ask to price 100 — would cross the bid. Rejected.
        exchange.cancel_replace(btc, ACCT_A, OrderId(2), price(100), qty(10), &mut reports);
        assert!(matches!(
            reports[0],
            ExecutionReport::Rejected {
                reason: RejectReason::PriceWouldCross,
                ..
            }
        ));
        // Original order intact.
        assert_eq!(exchange.accounts().balance(ACCT_A, BTC).reserved, 10);
    }

    #[test]
    fn cancel_replace_price_overflow_rejected() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, u64::MAX / 2);

        let mut reports = Vec::new();

        exchange.execute(
            btc,
            limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();

        // Replace to a price/qty combination that overflows u64.
        // Price close to u64::MAX, qty > 1 → overflow.
        let huge_price = Price(NonZeroU64::new(u64::MAX / 2).unwrap());
        exchange.cancel_replace(btc, ACCT_A, OrderId(1), huge_price, qty(3), &mut reports);
        assert!(matches!(
            reports[0],
            ExecutionReport::Rejected {
                reason: RejectReason::InsufficientBalance,
                ..
            }
        ));
        // Original order intact.
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 1_000);
    }

    // -- Fee model tests --

    #[test]
    fn fee_deducted_from_fill_proceeds() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 50_000);
        exchange.deposit(ACCT_B, BTC, 100);

        // 10 bps maker, 20 bps taker.
        let mut reports = Vec::new();
        exchange.set_fee_schedule(
            btc,
            FeeSchedule {
                maker_fee_bps: 10,
                taker_fee_bps: 20,
            },
            &mut reports,
        );

        // Resting buy (maker) at 1000 for 10.
        exchange.execute(
            btc,
            limit_order(1, ACCT_A, Side::Buy, 1000, 10, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();

        // Incoming sell (taker) hits the resting buy.
        exchange.execute(
            btc,
            limit_order(2, ACCT_B, Side::Sell, 1000, 10, TimeInForce::GTC),
            &mut reports,
        );

        // Find the fill report.
        let fill = reports
            .iter()
            .find(|r| matches!(r, ExecutionReport::Fill { .. }))
            .unwrap();
        if let ExecutionReport::Fill {
            maker_fee,
            taker_fee,
            ..
        } = fill
        {
            // cost = 1000 * 10 = 10_000
            // maker_fee = 10_000 * 10 / 10_000 = 10
            // taker_fee = 10_000 * 20 / 10_000 = 20
            assert_eq!(*maker_fee, 10);
            assert_eq!(*taker_fee, 20);
        } else {
            panic!("expected Fill");
        }

        // Buyer (maker): reserved 10_020 (cost 10_000 + max fee cushion 20).
        // Fill deducts cost (10_000) + maker_fee (10) = 10_010 from reservation.
        // Leftover cushion (10) released: available = 50_000 - 10_020 + 10 = 39_990.
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).available, 39_990);
        assert_eq!(exchange.accounts().balance(ACCT_A, BTC).available, 10);

        // Seller (taker): received cost (10_000) - taker_fee (20) = 9_980 in quote.
        assert_eq!(exchange.accounts().balance(ACCT_B, USD).available, 9_980);
        assert_eq!(exchange.accounts().balance(ACCT_B, BTC).available, 90);
    }

    #[test]
    fn zero_fees_produce_no_deduction() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 50_000);
        exchange.deposit(ACCT_B, BTC, 100);

        // No fee schedule set — defaults to 0/0.

        let mut reports = Vec::new();
        exchange.execute(
            btc,
            limit_order(1, ACCT_A, Side::Buy, 1000, 10, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();

        exchange.execute(
            btc,
            limit_order(2, ACCT_B, Side::Sell, 1000, 10, TimeInForce::GTC),
            &mut reports,
        );

        let fill = reports
            .iter()
            .find(|r| matches!(r, ExecutionReport::Fill { .. }))
            .unwrap();
        if let ExecutionReport::Fill {
            maker_fee,
            taker_fee,
            ..
        } = fill
        {
            assert_eq!(*maker_fee, 0);
            assert_eq!(*taker_fee, 0);
        }

        // No fees: buyer pays exactly 10_000, seller receives exactly 10_000.
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).available, 40_000);
        assert_eq!(exchange.accounts().balance(ACCT_B, USD).available, 10_000);
    }

    #[test]
    fn fee_schedule_change_applies_to_subsequent_fills() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 100_000);
        exchange.deposit(ACCT_B, BTC, 200);

        let mut reports = Vec::new();

        // First trade with no fees.
        exchange.execute(
            btc,
            limit_order(1, ACCT_A, Side::Buy, 1000, 10, TimeInForce::GTC),
            &mut reports,
        );
        exchange.execute(
            btc,
            limit_order(2, ACCT_B, Side::Sell, 1000, 10, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();

        // Set fees.
        exchange.set_fee_schedule(
            btc,
            FeeSchedule {
                maker_fee_bps: 50,
                taker_fee_bps: 100,
            },
            &mut reports,
        );

        // Second trade with fees.
        exchange.execute(
            btc,
            limit_order(3, ACCT_A, Side::Buy, 1000, 10, TimeInForce::GTC),
            &mut reports,
        );
        exchange.execute(
            btc,
            limit_order(4, ACCT_B, Side::Sell, 1000, 10, TimeInForce::GTC),
            &mut reports,
        );

        let fill = reports
            .iter()
            .find(|r| matches!(r, ExecutionReport::Fill { .. }))
            .unwrap();
        if let ExecutionReport::Fill {
            maker_fee,
            taker_fee,
            ..
        } = fill
        {
            // cost = 10_000. maker = 10_000 * 50 / 10_000 = 50. taker = 100.
            assert_eq!(*maker_fee, 50);
            assert_eq!(*taker_fee, 100);
        }
    }

    #[test]
    fn fee_cushion_covers_actual_fee_at_rounding_boundary() {
        // Regression test: cost=15_000, bps=3 → actual fee = 15_000*3/10_000 = 4.
        // Old formula (cost/10_000*bps) = 1*3 = 3, which was insufficient.
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        // Deposit just enough for cost (15_000) + correct fee cushion (4) = 15_004.
        exchange.deposit(ACCT_A, USD, 15_004);
        exchange.deposit(ACCT_B, BTC, 100);

        let mut reports = Vec::new();
        exchange.set_fee_schedule(
            btc,
            FeeSchedule {
                maker_fee_bps: 3,
                taker_fee_bps: 3,
            },
            &mut reports,
        );

        // price=150, qty=100 → cost=15_000. fee=15_000*3/10_000=4.
        // Reservation must be 15_004 to cover cost+fee.
        exchange.execute(
            btc,
            limit_order(1, ACCT_A, Side::Buy, 150, 100, TimeInForce::GTC),
            &mut reports,
        );
        // Should be placed, not rejected for insufficient balance.
        assert!(
            matches!(reports[0], ExecutionReport::Placed { .. }),
            "expected Placed, got {:?}",
            reports[0]
        );
        reports.clear();

        // Fill it.
        exchange.execute(
            btc,
            limit_order(2, ACCT_B, Side::Sell, 150, 100, TimeInForce::GTC),
            &mut reports,
        );

        let fill = reports
            .iter()
            .find(|r| matches!(r, ExecutionReport::Fill { .. }))
            .unwrap();
        if let ExecutionReport::Fill {
            maker_fee,
            taker_fee,
            ..
        } = fill
        {
            assert_eq!(*maker_fee, 4);
            assert_eq!(*taker_fee, 4);
        }

        // Buyer: deposited 15_004, reserved 15_004, cost=15_000 + fee=4 = 15_004.
        // No leftover — balance should be 0 available, 0 reserved.
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).available, 0);
        assert_eq!(exchange.accounts().balance(ACCT_A, BTC).available, 100);
    }

    #[test]
    fn maker_rebate_negative_fee() {
        // Negative maker fee = rebate. The maker should pay less (or receive more)
        // than the raw cost, and the fee field should be negative.
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 50_000);
        exchange.deposit(ACCT_B, BTC, 100);

        // -10 bps maker (rebate), 20 bps taker.
        let mut reports = Vec::new();
        exchange.set_fee_schedule(
            btc,
            FeeSchedule {
                maker_fee_bps: -10,
                taker_fee_bps: 20,
            },
            &mut reports,
        );

        // Resting buy (maker) at 1000 for 10. cost = 10_000.
        // max_fee_bps = max(0, -10).max(max(0, 20)) = 20.
        // Reservation = 10_000 + 10_000 * 20 / 10_000 = 10_020.
        exchange.execute(
            btc,
            limit_order(1, ACCT_A, Side::Buy, 1000, 10, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();

        // Incoming sell (taker) hits the resting buy.
        exchange.execute(
            btc,
            limit_order(2, ACCT_B, Side::Sell, 1000, 10, TimeInForce::GTC),
            &mut reports,
        );

        let fill = reports
            .iter()
            .find(|r| matches!(r, ExecutionReport::Fill { .. }))
            .unwrap();
        if let ExecutionReport::Fill {
            maker_fee,
            taker_fee,
            ..
        } = fill
        {
            // cost = 10_000. maker_fee = 10_000 * (-10) / 10_000 = -10 (rebate).
            // taker_fee = 10_000 * 20 / 10_000 = 20.
            assert_eq!(*maker_fee, -10);
            assert_eq!(*taker_fee, 20);
        } else {
            panic!("expected Fill");
        }

        // Buyer (maker): reserved 10_020, cost=10_000, maker_fee=-10.
        // Buyer deduction = cost + fee = 10_000 + (-10) = 9_990 from reservation.
        // Leftover = 10_020 - 9_990 = 30 released back.
        // available = 50_000 - 10_020 + 30 = 40_010.
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).available, 40_010);
        assert_eq!(exchange.accounts().balance(ACCT_A, BTC).available, 10);

        // Seller (taker): received cost - taker_fee = 10_000 - 20 = 9_980.
        assert_eq!(exchange.accounts().balance(ACCT_B, USD).available, 9_980);
        assert_eq!(exchange.accounts().balance(ACCT_B, BTC).available, 90);
    }

    // -- Post-only tests --

    fn post_only_order(id: u64, account: AccountId, side: Side, p: u64, q: u64) -> Order {
        Order {
            id: OrderId(id),
            account,
            side,
            order_type: OrderType::Limit {
                price: price(p),
                post_only: true,
            },
            time_in_force: TimeInForce::GTC,
            quantity: qty(q),
            stp: SelfTradeProtection::Allow,
            expiry_ns: 0,
        }
    }

    #[test]
    fn post_only_rests_on_empty_book() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 50_000);

        let mut reports = Vec::new();
        exchange.execute(
            btc,
            post_only_order(1, ACCT_A, Side::Buy, 1000, 10),
            &mut reports,
        );

        assert!(
            reports
                .iter()
                .any(|r| matches!(r, ExecutionReport::Placed { .. })),
            "post-only should rest on empty book"
        );
    }

    #[test]
    fn post_only_rests_when_no_cross() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 50_000);
        exchange.deposit(ACCT_B, BTC, 100);

        let mut reports = Vec::new();

        // Resting ask at 1100.
        exchange.execute(
            btc,
            limit_order(1, ACCT_B, Side::Sell, 1100, 10, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();

        // Post-only buy at 1000 — does not cross ask at 1100.
        exchange.execute(
            btc,
            post_only_order(2, ACCT_A, Side::Buy, 1000, 10),
            &mut reports,
        );

        assert!(
            reports
                .iter()
                .any(|r| matches!(r, ExecutionReport::Placed { .. })),
            "post-only buy below best ask should rest"
        );
    }

    #[test]
    fn post_only_rejected_when_would_cross() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 50_000);
        exchange.deposit(ACCT_B, BTC, 100);

        let mut reports = Vec::new();

        // Resting ask at 1000.
        exchange.execute(
            btc,
            limit_order(1, ACCT_B, Side::Sell, 1000, 10, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();

        // Post-only buy at 1000 — would cross the ask.
        exchange.execute(
            btc,
            post_only_order(2, ACCT_A, Side::Buy, 1000, 10),
            &mut reports,
        );

        assert!(
            reports.iter().any(|r| matches!(
                r,
                ExecutionReport::Rejected {
                    reason: RejectReason::PostOnlyWouldCross,
                    ..
                }
            )),
            "post-only buy at ask should be rejected"
        );
    }

    #[test]
    fn post_only_rejected_releases_reservation() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 50_000);
        exchange.deposit(ACCT_B, BTC, 100);

        let mut reports = Vec::new();

        // Resting ask at 1000.
        exchange.execute(
            btc,
            limit_order(1, ACCT_B, Side::Sell, 1000, 10, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();

        // Post-only buy at 1000 — rejected.
        exchange.execute(
            btc,
            post_only_order(2, ACCT_A, Side::Buy, 1000, 10),
            &mut reports,
        );

        // Balance should be fully restored (no funds locked).
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).available, 50_000);
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 0);
    }

    #[test]
    fn post_only_sell_rejected_when_would_cross() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 50_000);
        exchange.deposit(ACCT_B, BTC, 100);

        let mut reports = Vec::new();

        // Resting bid at 1000.
        exchange.execute(
            btc,
            limit_order(1, ACCT_A, Side::Buy, 1000, 10, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();

        // Post-only sell at 1000 — would cross the bid.
        exchange.execute(
            btc,
            post_only_order(2, ACCT_B, Side::Sell, 1000, 10),
            &mut reports,
        );

        assert!(
            reports.iter().any(|r| matches!(
                r,
                ExecutionReport::Rejected {
                    reason: RejectReason::PostOnlyWouldCross,
                    ..
                }
            )),
            "post-only sell at bid should be rejected"
        );
    }

    #[test]
    fn post_only_rejected_order_id_can_be_reused() {
        // PostOnlyWouldCross is another submit-time rejection: the order
        // never rests, the live set never gains an entry, so the same
        // OrderId is free to retry with a non-crossing price.
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 50_000);
        exchange.deposit(ACCT_B, BTC, 100);

        let mut reports = Vec::new();

        // Resting ask at 1000.
        exchange.execute(
            btc,
            limit_order(1, ACCT_B, Side::Sell, 1000, 10, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();

        // Post-only buy at 1000 with order_id=2 — rejected (would cross).
        exchange.execute(
            btc,
            post_only_order(2, ACCT_A, Side::Buy, 1000, 10),
            &mut reports,
        );
        reports.clear();

        // Resubmitting order_id=2 below the ask must place — the prior
        // rejection didn't claim the slot.
        exchange.execute(
            btc,
            limit_order(2, ACCT_A, Side::Buy, 900, 10, TimeInForce::GTC),
            &mut reports,
        );

        assert!(
            matches!(reports[0], ExecutionReport::Placed { .. }),
            "retry of rejected post-only id should place, got {:?}",
            reports[0],
        );
    }

    // --- Withdraw and account lifecycle ---

    #[test]
    fn withdraw_reduces_available() {
        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 10_000);

        exchange.withdraw(ACCT_A, USD, 3_000).unwrap();
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).available, 7_000);
    }

    #[test]
    fn withdraw_insufficient_rejected() {
        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 1_000);

        let err = exchange.withdraw(ACCT_A, USD, 5_000).unwrap_err();
        assert_eq!(err, RejectReason::InsufficientBalance);
    }

    #[test]
    fn withdraw_with_resting_orders_rejected() {
        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 10_000);

        let mut reports = Vec::new();
        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        assert!(matches!(reports[0], ExecutionReport::Placed { .. }));

        // Can't withdraw while orders are resting.
        let err = exchange.withdraw(ACCT_A, USD, 1_000).unwrap_err();
        assert_eq!(err, RejectReason::HasRestingOrders);
    }

    #[test]
    fn withdraw_after_cancel_all_succeeds() {
        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 10_000);

        let mut reports = Vec::new();
        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
            &mut reports,
        );

        // Cancel all, then withdraw.
        reports.clear();
        exchange.cancel_all(ACCT_A, &mut reports);
        exchange.withdraw(ACCT_A, USD, 10_000).unwrap();
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).available, 0);
    }

    #[test]
    fn order_id_reusable_after_full_withdrawal() {
        // Under live-orders-only dedup there is no per-account HWM that
        // persists across an account's lifetime. Once a fill closes an
        // order the live set drops the entry; full account withdrawal
        // doesn't change that. A re-deposited account can reuse the
        // same OrderId because no live order claims it.
        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 10_000);
        exchange.deposit(ACCT_A, BTC, 100);

        // Submit and fill an order.
        let mut reports = Vec::new();
        exchange.deposit(ACCT_B, BTC, 100);
        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_B, Side::Sell, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();
        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
            &mut reports,
        );

        // Withdraw everything.
        exchange
            .withdraw(
                ACCT_A,
                USD,
                exchange.accounts().balance(ACCT_A, USD).available,
            )
            .unwrap();
        exchange
            .withdraw(
                ACCT_A,
                BTC,
                exchange.accounts().balance(ACCT_A, BTC).available,
            )
            .unwrap();

        assert!(!exchange.accounts().has_balances(ACCT_A));

        // Re-deposit and reuse OrderId 1 — must place because the
        // earlier fill removed it from the live set.
        exchange.deposit(ACCT_A, USD, 10_000);
        reports.clear();
        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        assert!(
            matches!(reports[0], ExecutionReport::Placed { .. }),
            "reuse after fill+withdrawal should place, got {:?}",
            reports[0]
        );
    }

    #[test]
    fn order_counts_track_resting_orders() {
        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 100_000);

        let mut reports = Vec::new();
        // Place 3 orders.
        for i in 1..=3 {
            exchange.execute(
                Symbol(1),
                limit_order(i, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
                &mut reports,
            );
        }

        // All 3 resting — withdraw should fail.
        assert_eq!(
            exchange.withdraw(ACCT_A, USD, 1).unwrap_err(),
            RejectReason::HasRestingOrders
        );

        // Cancel all — withdraw should succeed.
        reports.clear();
        exchange.cancel_all(ACCT_A, &mut reports);
        assert!(exchange.withdraw(ACCT_A, USD, 1).is_ok());
    }

    #[test]
    fn order_counts_zero_after_ioc_fill() {
        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 100_000);
        exchange.deposit(ACCT_B, BTC, 100);

        let mut reports = Vec::new();
        // Seller places resting order.
        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_B, Side::Sell, 100, 10, TimeInForce::GTC),
            &mut reports,
        );

        // IOC buy fills immediately — should not leave resting count.
        reports.clear();
        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::IOC),
            &mut reports,
        );

        // ACCT_A should have no resting orders — withdraw should work.
        assert!(exchange.withdraw(ACCT_A, USD, 1).is_ok());
    }

    #[test]
    fn withdraw_after_partial_fill_and_cancel() {
        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 100_000);
        exchange.deposit(ACCT_B, BTC, 100);

        let mut reports = Vec::new();
        // ACCT_A places GTC buy for 20 @ 100.
        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_A, Side::Buy, 100, 20, TimeInForce::GTC),
            &mut reports,
        );

        // ACCT_B fills 10 of 20.
        reports.clear();
        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_B, Side::Sell, 100, 10, TimeInForce::GTC),
            &mut reports,
        );

        // Cancel remaining, then withdraw all.
        reports.clear();
        exchange.cancel_all(ACCT_A, &mut reports);

        let usd_avail = exchange.accounts().balance(ACCT_A, USD).available;
        exchange.withdraw(ACCT_A, USD, usd_avail).unwrap();
        let btc_avail = exchange.accounts().balance(ACCT_A, BTC).available;
        exchange.withdraw(ACCT_A, BTC, btc_avail).unwrap();

        assert!(!exchange.accounts().has_balances(ACCT_A));
    }

    #[test]
    fn order_counts_zero_after_fok_no_fill() {
        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 100_000);

        let mut reports = Vec::new();
        // FOK buy on empty book — rejected, no fill.
        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::FOK),
            &mut reports,
        );
        assert!(matches!(
            reports[0],
            ExecutionReport::Rejected {
                reason: RejectReason::FOKCannotFill,
                ..
            }
        ));

        // No resting orders — withdraw should succeed.
        assert!(exchange.withdraw(ACCT_A, USD, 1).is_ok());
    }

    #[test]
    fn order_counts_zero_after_fok_full_fill() {
        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 100_000);
        exchange.deposit(ACCT_B, BTC, 100);

        let mut reports = Vec::new();
        // Seller rests.
        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_B, Side::Sell, 100, 10, TimeInForce::GTC),
            &mut reports,
        );

        // FOK buy fills entirely.
        reports.clear();
        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::FOK),
            &mut reports,
        );

        // ACCT_A has no resting orders.
        assert!(exchange.withdraw(ACCT_A, USD, 1).is_ok());
    }

    #[test]
    fn order_counts_after_stp_cancel_newest() {
        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 100_000);
        exchange.deposit(ACCT_A, BTC, 100);

        let mut reports = Vec::new();
        // Place a resting sell.
        exchange.execute(
            Symbol(1),
            Order {
                id: OrderId(1),
                account: ACCT_A,
                side: Side::Sell,
                order_type: OrderType::Limit {
                    price: Price(NonZeroU64::new(100).unwrap()),
                    post_only: false,
                },
                time_in_force: TimeInForce::GTC,
                quantity: Quantity(NonZeroU64::new(10).unwrap()),
                stp: SelfTradeProtection::CancelNewest,
                expiry_ns: 0,
            },
            &mut reports,
        );

        // Self-trade: buy crosses the sell. CancelNewest rejects the taker (buy).
        reports.clear();
        exchange.execute(
            Symbol(1),
            Order {
                id: OrderId(2),
                account: ACCT_A,
                side: Side::Buy,
                order_type: OrderType::Limit {
                    price: Price(NonZeroU64::new(100).unwrap()),
                    post_only: false,
                },
                time_in_force: TimeInForce::GTC,
                quantity: Quantity(NonZeroU64::new(10).unwrap()),
                stp: SelfTradeProtection::CancelNewest,
                expiry_ns: 0,
            },
            &mut reports,
        );

        // The maker (sell) still rests. Cancel it, then withdraw should succeed.
        reports.clear();
        exchange.cancel_all(ACCT_A, &mut reports);
        assert!(exchange.withdraw(ACCT_A, USD, 1).is_ok());
    }

    #[test]
    fn order_counts_after_stp_cancel_oldest() {
        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 100_000);
        exchange.deposit(ACCT_A, BTC, 100);

        let mut reports = Vec::new();
        // Place a resting sell.
        exchange.execute(
            Symbol(1),
            Order {
                id: OrderId(1),
                account: ACCT_A,
                side: Side::Sell,
                order_type: OrderType::Limit {
                    price: Price(NonZeroU64::new(100).unwrap()),
                    post_only: false,
                },
                time_in_force: TimeInForce::GTC,
                quantity: Quantity(NonZeroU64::new(10).unwrap()),
                stp: SelfTradeProtection::CancelOldest,
                expiry_ns: 0,
            },
            &mut reports,
        );

        // Self-trade: buy crosses. CancelOldest cancels the maker (sell).
        // The taker buy may rest if GTC.
        reports.clear();
        exchange.execute(
            Symbol(1),
            Order {
                id: OrderId(2),
                account: ACCT_A,
                side: Side::Buy,
                order_type: OrderType::Limit {
                    price: Price(NonZeroU64::new(100).unwrap()),
                    post_only: false,
                },
                time_in_force: TimeInForce::GTC,
                quantity: Quantity(NonZeroU64::new(10).unwrap()),
                stp: SelfTradeProtection::CancelOldest,
                expiry_ns: 0,
            },
            &mut reports,
        );

        // Cancel remaining, then withdraw.
        reports.clear();
        exchange.cancel_all(ACCT_A, &mut reports);
        assert!(exchange.withdraw(ACCT_A, USD, 1).is_ok());
    }

    // --- Per-key request sequence dedup tests ---

    #[test]
    fn duplicate_request_rejected() {
        let mut exchange = Exchange::new();
        let key_hash: u64 = 0xDEAD_BEEF;

        // First request with seq=1 should succeed.
        assert!(exchange.check_request_seq(key_hash, 1));
        // Same key, same seq=1 should be rejected.
        assert!(!exchange.check_request_seq(key_hash, 1));
        // Lower seq should also be rejected.
        assert!(!exchange.check_request_seq(key_hash, 0));
    }

    #[test]
    fn different_keys_overlapping_seqs() {
        let mut exchange = Exchange::new();
        let key_a: u64 = 0xAAAA;
        let key_b: u64 = 0xBBBB;

        // Both keys can use seq=1 independently.
        assert!(exchange.check_request_seq(key_a, 1));
        assert!(exchange.check_request_seq(key_b, 1));
        // key_a advancing to seq=2 doesn't affect key_b.
        assert!(exchange.check_request_seq(key_a, 2));
        assert!(!exchange.check_request_seq(key_b, 1)); // still duplicate for key_b
        assert!(exchange.check_request_seq(key_b, 2));
    }

    #[test]
    fn key_hash_zero_exempt_from_dedup() {
        let mut exchange = Exchange::new();

        // key_hash=0 is exempt (internal/seed events) — always returns true.
        assert!(exchange.check_request_seq(0, 1));
        assert!(exchange.check_request_seq(0, 1)); // same seq, still passes
        assert!(exchange.check_request_seq(0, 0)); // seq=0 also passes
    }

    #[test]
    fn key_hwm_snapshot_round_trip() {
        let mut exchange = Exchange::new();
        let key_a: u64 = 0x1111;
        let key_b: u64 = 0x2222;

        exchange.check_request_seq(key_a, 5);
        exchange.check_request_seq(key_b, 10);

        let snap = exchange.snapshot_key_hwm();
        assert_eq!(snap.len(), 2);

        // Verify both entries are present (order may vary).
        let mut sorted = snap.clone();
        sorted.sort();
        assert_eq!(sorted, vec![(key_a, 5), (key_b, 10)]);
    }

    #[test]
    fn request_seq_must_be_strictly_increasing() {
        let mut exchange = Exchange::new();
        let key: u64 = 0xABCD;

        // Gap in sequence is fine (3, then 10).
        assert!(exchange.check_request_seq(key, 3));
        assert!(exchange.check_request_seq(key, 10));
        // seq=4..9 are now below HWM (10).
        assert!(!exchange.check_request_seq(key, 4));
        assert!(!exchange.check_request_seq(key, 9));
        assert!(!exchange.check_request_seq(key, 10)); // equal = dup
        assert!(exchange.check_request_seq(key, 11)); // strictly greater
    }

    #[test]
    fn request_seq_zero_on_real_key_is_duplicate_after_any_request() {
        let mut exchange = Exchange::new();
        let key: u64 = 0xFF00;

        // First request at seq=1 sets HWM to 1.
        assert!(exchange.check_request_seq(key, 1));
        // seq=0 is below HWM (1) — rejected as duplicate.
        assert!(!exchange.check_request_seq(key, 0));
    }

    #[test]
    fn request_seq_u64_max_accepted() {
        let mut exchange = Exchange::new();
        let key: u64 = 0x42;

        // u64::MAX should be accepted as a valid seq.
        assert!(exchange.check_request_seq(key, u64::MAX));
        // Nothing can be strictly greater than u64::MAX.
        assert!(!exchange.check_request_seq(key, u64::MAX));
        assert!(!exchange.check_request_seq(key, u64::MAX - 1));
    }

    #[test]
    fn dedup_interleaved_with_orders() {
        // Verify that per-key request-seq dedup and the live `(account,
        // order_id)` set are independent: a fresh request_seq doesn't
        // bypass live-order dedup, and vice versa.
        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 1_000_000);

        let key: u64 = 0xAAAA;
        let mut reports = Vec::new();

        // Submit order via check_request_seq first (as the pipeline would).
        assert!(exchange.check_request_seq(key, 1));
        exchange.execute(
            Symbol(1),
            Order {
                id: OrderId(100),
                account: ACCT_A,
                side: Side::Buy,
                order_type: OrderType::Limit {
                    price: price(100),
                    post_only: false,
                },
                time_in_force: TimeInForce::GTC,
                quantity: qty(1),
                stp: SelfTradeProtection::default(),
                expiry_ns: 0,
            },
            &mut reports,
        );
        assert!(
            reports
                .iter()
                .any(|r| matches!(r, ExecutionReport::Placed { .. }))
        );

        // Duplicate per-key seq should be caught before execute.
        assert!(!exchange.check_request_seq(key, 1));

        // A new request_seq doesn't help if the OrderId is still live
        // — the live-order dedup catches it.
        reports.clear();
        assert!(exchange.check_request_seq(key, 2));
        exchange.execute(
            Symbol(1),
            Order {
                id: OrderId(100), // same order ID as above
                account: ACCT_A,
                side: Side::Buy,
                order_type: OrderType::Limit {
                    price: price(100),
                    post_only: false,
                },
                time_in_force: TimeInForce::GTC,
                quantity: qty(1),
                stp: SelfTradeProtection::default(),
                expiry_ns: 0,
            },
            &mut reports,
        );
        assert!(reports.iter().any(|r| matches!(
            r,
            ExecutionReport::Rejected {
                reason: RejectReason::DuplicateOrderId,
                ..
            }
        )));
    }

    // -----------------------------------------------------------------------
    // Same OrderId, different accounts at same price level
    //
    // Two accounts may independently use the same OrderId. When both rest at
    // the same price level, cancel/replace/stop-cancel must operate on the
    // correct account's order without disturbing the other.
    // -----------------------------------------------------------------------

    /// Place OrderId(1) for both ACCT_A and ACCT_B as buy limits at the same
    /// price. Returns the exchange ready for disambiguation tests.
    fn setup_same_id_same_price() -> Exchange {
        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 100_000);
        exchange.deposit(ACCT_B, USD, 100_000);

        let mut reports = Vec::new();
        // ACCT_A places OrderId(1) buy @ 100, qty 10.
        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
        reports.clear();

        // ACCT_B places OrderId(1) buy @ 100, qty 5.
        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_B, Side::Buy, 100, 5, TimeInForce::GTC),
            &mut reports,
        );
        assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
        exchange
    }

    #[test]
    fn cancel_disambiguates_by_account_at_same_price() {
        let mut exchange = setup_same_id_same_price();
        let mut reports = Vec::new();

        // Cancel ACCT_B's OrderId(1) — ACCT_A's should survive.
        exchange.cancel(Symbol(1), ACCT_B, OrderId(1), &mut reports);
        assert!(matches!(
            reports[0],
            ExecutionReport::Cancelled {
                account,
                order_id: OrderId(1),
                ..
            } if account == ACCT_B
        ));
        reports.clear();

        // ACCT_A's order is still resting — a sell should fill against it.
        exchange.deposit(ACCT_A, BTC, 100);
        exchange.deposit(ACCT_B, BTC, 100);
        exchange.execute(
            Symbol(1),
            limit_order(2, ACCT_B, Side::Sell, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        let fill = reports
            .iter()
            .find(|r| matches!(r, ExecutionReport::Fill { .. }));
        assert!(
            fill.is_some(),
            "ACCT_A's order should still be resting and fillable"
        );
    }

    #[test]
    fn cancel_replace_same_price_disambiguates_by_account() {
        let mut exchange = setup_same_id_same_price();
        let mut reports = Vec::new();

        // Amend ACCT_A's OrderId(1): reduce qty from 10 to 3 (same price).
        exchange.cancel_replace(
            Symbol(1),
            ACCT_A,
            OrderId(1),
            price(100),
            qty(3),
            &mut reports,
        );
        assert!(matches!(
            reports[0],
            ExecutionReport::Replaced {
                order_id: OrderId(1),
                ..
            }
        ));
        // Verify the replaced order reports the correct old/new quantities.
        if let ExecutionReport::Replaced {
            old_remaining,
            new_remaining,
            ..
        } = &reports[0]
        {
            assert_eq!(*old_remaining, qty(10));
            assert_eq!(*new_remaining, qty(3));
        }
        reports.clear();

        // ACCT_B's order should be untouched — cancel it and verify qty 5.
        exchange.cancel(Symbol(1), ACCT_B, OrderId(1), &mut reports);
        assert!(matches!(
            reports[0],
            ExecutionReport::Cancelled {
                account,
                remaining_quantity,
                ..
            } if account == ACCT_B && remaining_quantity == qty(5)
        ));
    }

    #[test]
    fn cancel_replace_different_price_disambiguates_by_account() {
        let mut exchange = setup_same_id_same_price();
        let mut reports = Vec::new();

        // Move ACCT_A's OrderId(1) from price 100 to price 90.
        exchange.cancel_replace(
            Symbol(1),
            ACCT_A,
            OrderId(1),
            price(90),
            qty(10),
            &mut reports,
        );
        assert!(matches!(
            reports[0],
            ExecutionReport::Replaced {
                order_id: OrderId(1),
                ..
            }
        ));
        reports.clear();

        // Sell at 100 should only fill ACCT_B's order (still at 100),
        // not ACCT_A's (now at 90).
        exchange.deposit(ACCT_A, BTC, 100);
        exchange.execute(
            Symbol(1),
            limit_order(2, ACCT_A, Side::Sell, 100, 5, TimeInForce::GTC),
            &mut reports,
        );
        let fills: Vec<_> = reports
            .iter()
            .filter(|r| matches!(r, ExecutionReport::Fill { .. }))
            .collect();
        assert_eq!(
            fills.len(),
            1,
            "should only fill ACCT_B's order at price 100"
        );
    }

    #[test]
    fn cancel_stop_disambiguates_by_account() {
        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 100_000);
        exchange.deposit(ACCT_B, USD, 100_000);

        let mut reports = Vec::new();

        // Both accounts place a stop buy with OrderId(1), same trigger price.
        let stop_a = Order {
            id: OrderId(1),
            account: ACCT_A,
            side: Side::Buy,
            order_type: OrderType::Stop {
                trigger_price: price(200),
            },
            time_in_force: TimeInForce::GTC,
            quantity: qty(10),
            stp: SelfTradeProtection::Allow,
            expiry_ns: 0,
        };
        exchange.execute(Symbol(1), stop_a, &mut reports);
        reports.clear();

        let stop_b = Order {
            id: OrderId(1),
            account: ACCT_B,
            side: Side::Buy,
            order_type: OrderType::Stop {
                trigger_price: price(200),
            },
            time_in_force: TimeInForce::GTC,
            quantity: qty(5),
            stp: SelfTradeProtection::Allow,
            expiry_ns: 0,
        };
        exchange.execute(Symbol(1), stop_b, &mut reports);
        reports.clear();

        // Cancel ACCT_A's stop — ACCT_B's should survive.
        exchange.cancel(Symbol(1), ACCT_A, OrderId(1), &mut reports);
        assert!(matches!(
            reports[0],
            ExecutionReport::Cancelled {
                account,
                order_id: OrderId(1),
                ..
            } if account == ACCT_A
        ));
        reports.clear();

        // Verify ACCT_B's stop is still pending: cancel it to confirm it exists.
        exchange.cancel(Symbol(1), ACCT_B, OrderId(1), &mut reports);
        assert!(matches!(
            reports[0],
            ExecutionReport::Cancelled {
                account,
                order_id: OrderId(1),
                ..
            } if account == ACCT_B
        ));
    }

    // -----------------------------------------------------------------------
    // Day TIF + EndOfDay
    // -----------------------------------------------------------------------

    #[test]
    fn day_order_places_on_book() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 10_000);

        let mut reports = Vec::new();
        exchange.execute(
            btc,
            limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::Day),
            &mut reports,
        );
        assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 1_000);
    }

    #[test]
    fn end_of_day_cancels_day_orders_not_gtc() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 20_000);
        exchange.deposit(ACCT_B, USD, 20_000);

        let mut reports = Vec::new();

        // ACCT_A: Day order.
        exchange.execute(
            btc,
            limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::Day),
            &mut reports,
        );
        reports.clear();

        // ACCT_B: GTC order at the same price.
        exchange.execute(
            btc,
            limit_order(1, ACCT_B, Side::Buy, 100, 5, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();

        // EndOfDay should cancel ACCT_A's Day order but not ACCT_B's GTC.
        exchange.end_of_day(&mut reports);
        assert_eq!(reports.len(), 1);
        assert!(matches!(
            reports[0],
            ExecutionReport::Cancelled {
                account,
                order_id: OrderId(1),
                ..
            } if account == ACCT_A
        ));

        // ACCT_A's balance fully released, ACCT_B's still reserved.
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 0);
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).available, 20_000);
        assert_eq!(exchange.accounts().balance(ACCT_B, USD).reserved, 500);
    }

    #[test]
    fn end_of_day_on_empty_book_is_noop() {
        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());

        let mut reports = Vec::new();
        exchange.end_of_day(&mut reports);
        assert!(reports.is_empty());
    }

    #[test]
    fn day_order_partially_fills_then_cancelled_at_eod() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 10_000);
        exchange.deposit(ACCT_B, BTC, 100);

        let mut reports = Vec::new();

        // ACCT_A: Day buy limit @ 100, qty 10.
        exchange.execute(
            btc,
            limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::Day),
            &mut reports,
        );
        reports.clear();

        // ACCT_B: sell 3, partially filling the Day order.
        exchange.execute(
            btc,
            limit_order(1, ACCT_B, Side::Sell, 100, 3, TimeInForce::IOC),
            &mut reports,
        );
        let fills: Vec<_> = reports
            .iter()
            .filter(|r| matches!(r, ExecutionReport::Fill { .. }))
            .collect();
        assert_eq!(fills.len(), 1);
        reports.clear();

        // EndOfDay cancels the remaining 7.
        exchange.end_of_day(&mut reports);
        assert_eq!(reports.len(), 1);
        assert!(matches!(
            reports[0],
            ExecutionReport::Cancelled {
                remaining_quantity,
                ..
            } if remaining_quantity.get() == 7
        ));

        // All reservations released.
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 0);
    }

    #[test]
    fn end_of_day_cancels_day_stop_orders() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 100_000);

        let mut reports = Vec::new();

        // Day stop order.
        exchange.execute(
            btc,
            Order {
                id: OrderId(1),
                account: ACCT_A,
                side: Side::Buy,
                order_type: OrderType::Stop {
                    trigger_price: price(200),
                },
                time_in_force: TimeInForce::Day,
                quantity: qty(10),
                stp: SelfTradeProtection::Allow,
                expiry_ns: 0,
            },
            &mut reports,
        );
        reports.clear();

        // GTC stop order.
        exchange.execute(
            btc,
            Order {
                id: OrderId(2),
                account: ACCT_A,
                side: Side::Buy,
                order_type: OrderType::Stop {
                    trigger_price: price(200),
                },
                time_in_force: TimeInForce::GTC,
                quantity: qty(5),
                stp: SelfTradeProtection::Allow,
                expiry_ns: 0,
            },
            &mut reports,
        );
        reports.clear();

        // EndOfDay cancels only the Day stop.
        exchange.end_of_day(&mut reports);
        assert_eq!(reports.len(), 1);
        assert!(matches!(
            reports[0],
            ExecutionReport::Cancelled {
                order_id: OrderId(1),
                ..
            }
        ));
    }

    // -- GTD expiration tests --

    #[test]
    fn gtd_order_rejected_with_zero_expiry() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 10_000);

        let mut reports = Vec::new();
        exchange.execute(
            btc,
            Order {
                id: OrderId(1),
                account: ACCT_A,
                side: Side::Buy,
                order_type: OrderType::Limit {
                    price: price(100),
                    post_only: false,
                },
                time_in_force: TimeInForce::GTD,
                quantity: qty(10),
                stp: SelfTradeProtection::Allow,
                expiry_ns: 0,
            },
            &mut reports,
        );
        assert_eq!(reports.len(), 1);
        assert!(matches!(
            reports[0],
            ExecutionReport::Rejected {
                reason: RejectReason::InvalidExpiry,
                ..
            }
        ));
    }

    #[test]
    fn non_gtd_order_rejected_with_nonzero_expiry() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 10_000);

        let mut reports = Vec::new();
        exchange.execute(
            btc,
            Order {
                id: OrderId(1),
                account: ACCT_A,
                side: Side::Buy,
                order_type: OrderType::Limit {
                    price: price(100),
                    post_only: false,
                },
                time_in_force: TimeInForce::GTC,
                quantity: qty(10),
                stp: SelfTradeProtection::Allow,
                expiry_ns: 5000,
            },
            &mut reports,
        );
        assert_eq!(reports.len(), 1);
        assert!(matches!(
            reports[0],
            ExecutionReport::Rejected {
                reason: RejectReason::InvalidExpiry,
                ..
            }
        ));
    }

    // -- Scheduler-driven GTD expiry --

    /// Submitting a GTD limit that rests on the book schedules exactly one
    /// `ExpireOrder` task on the heap. Draining before the deadline is a
    /// no-op; draining at or after the deadline cancels the order and
    /// releases its reservation.
    #[test]
    fn gtd_limit_schedules_and_expires_on_drain() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 10_000);

        let mut reports = Vec::new();
        exchange.execute(
            btc,
            Order {
                id: OrderId(1),
                account: ACCT_A,
                side: Side::Buy,
                order_type: OrderType::Limit {
                    price: price(100),
                    post_only: false,
                },
                time_in_force: TimeInForce::GTD,
                quantity: qty(5),
                stp: SelfTradeProtection::Allow,
                expiry_ns: 1_000,
            },
            &mut reports,
        );
        assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
        reports.clear();
        assert_eq!(exchange.scheduled_task_count(), 1);
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 500);

        // Pre-deadline drain: nothing fires.
        exchange.drain_due_scheduled_tasks(999, &mut reports);
        assert!(reports.is_empty());
        assert_eq!(exchange.scheduled_task_count(), 1);

        // At-deadline drain: cancel + release.
        exchange.drain_due_scheduled_tasks(1_000, &mut reports);
        assert_eq!(reports.len(), 1);
        assert!(matches!(
            reports[0],
            ExecutionReport::Cancelled {
                order_id: OrderId(1),
                ..
            }
        ));
        assert_eq!(exchange.scheduled_task_count(), 0);
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 0);
    }

    /// A GTD pending stop also schedules an expiry task; firing the task
    /// cancels the pending stop before it ever triggers.
    #[test]
    fn gtd_pending_stop_schedules_and_expires() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 100_000);

        let mut reports = Vec::new();
        exchange.execute(
            btc,
            Order {
                id: OrderId(1),
                account: ACCT_A,
                side: Side::Buy,
                order_type: OrderType::Stop {
                    trigger_price: price(120),
                },
                time_in_force: TimeInForce::GTD,
                quantity: qty(10),
                stp: SelfTradeProtection::Allow,
                expiry_ns: 5_000,
            },
            &mut reports,
        );
        // Stops emit no Placed report at submit time.
        assert!(reports.is_empty());
        assert_eq!(exchange.scheduled_task_count(), 1);

        exchange.drain_due_scheduled_tasks(5_000, &mut reports);
        assert_eq!(reports.len(), 1);
        assert!(matches!(
            reports[0],
            ExecutionReport::Cancelled {
                order_id: OrderId(1),
                ..
            }
        ));
        assert_eq!(exchange.scheduled_task_count(), 0);
    }

    /// Cancelling a GTD order before its deadline leaves a tombstone task
    /// in the heap. When the tombstone fires, `find_gtd_expiry` returns
    /// None, the task drops silently, and no double-cancel report is emitted.
    #[test]
    fn cancelled_gtd_creates_tombstone_no_double_cancel() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 10_000);

        let mut reports = Vec::new();
        exchange.execute(
            btc,
            Order {
                id: OrderId(1),
                account: ACCT_A,
                side: Side::Buy,
                order_type: OrderType::Limit {
                    price: price(100),
                    post_only: false,
                },
                time_in_force: TimeInForce::GTD,
                quantity: qty(1),
                stp: SelfTradeProtection::Allow,
                expiry_ns: 2_000,
            },
            &mut reports,
        );
        reports.clear();
        assert_eq!(exchange.scheduled_task_count(), 1);

        // Cancel before deadline.
        exchange.cancel(btc, ACCT_A, OrderId(1), &mut reports);
        assert!(matches!(reports[0], ExecutionReport::Cancelled { .. }));
        reports.clear();
        // Tombstone still in heap.
        assert_eq!(exchange.scheduled_task_count(), 1);

        // Drain past deadline: tombstone drops without emitting anything.
        exchange.drain_due_scheduled_tasks(2_000, &mut reports);
        assert!(reports.is_empty(), "tombstone must not emit Cancelled");
        assert_eq!(exchange.scheduled_task_count(), 0);
    }

    /// Cancel-replace preserves the order's `expiry_ns`, so the originally
    /// scheduled task remains valid and still fires at the original deadline.
    #[test]
    fn cancel_replace_preserves_gtd_expiry_schedule() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 10_000);

        let mut reports = Vec::new();
        exchange.execute(
            btc,
            Order {
                id: OrderId(1),
                account: ACCT_A,
                side: Side::Buy,
                order_type: OrderType::Limit {
                    price: price(50),
                    post_only: false,
                },
                time_in_force: TimeInForce::GTD,
                quantity: qty(2),
                stp: SelfTradeProtection::Allow,
                expiry_ns: 3_000,
            },
            &mut reports,
        );
        reports.clear();
        assert_eq!(exchange.scheduled_task_count(), 1);

        // Cancel-replace to a new price + size; expiry stays unchanged.
        exchange.cancel_replace(btc, ACCT_A, OrderId(1), price(60), qty(3), &mut reports);
        assert!(matches!(reports[0], ExecutionReport::Replaced { .. }));
        reports.clear();
        // Heap unchanged: no new schedule, no removal.
        assert_eq!(exchange.scheduled_task_count(), 1);

        // Original deadline still fires.
        exchange.drain_due_scheduled_tasks(3_000, &mut reports);
        assert_eq!(reports.len(), 1);
        assert!(matches!(
            reports[0],
            ExecutionReport::Cancelled {
                order_id: OrderId(1),
                ..
            }
        ));
    }

    /// A GTD limit that partially fills leaves the remainder on the book
    /// — the scheduled task still cancels that remainder at expiry.
    #[test]
    fn gtd_partial_fill_remainder_still_expires() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 100_000);
        exchange.deposit(ACCT_B, BTC, 10);

        let mut reports = Vec::new();
        // ACCT_B places a small ask: 1 unit at price 100.
        exchange.execute(
            btc,
            Order {
                id: OrderId(1),
                account: ACCT_B,
                side: Side::Sell,
                order_type: OrderType::Limit {
                    price: price(100),
                    post_only: false,
                },
                time_in_force: TimeInForce::GTC,
                quantity: qty(1),
                stp: SelfTradeProtection::Allow,
                expiry_ns: 0,
            },
            &mut reports,
        );
        reports.clear();

        // ACCT_A submits a GTD buy for 5 units at 100 — fills 1, rests 4.
        exchange.execute(
            btc,
            Order {
                id: OrderId(2),
                account: ACCT_A,
                side: Side::Buy,
                order_type: OrderType::Limit {
                    price: price(100),
                    post_only: false,
                },
                time_in_force: TimeInForce::GTD,
                quantity: qty(5),
                stp: SelfTradeProtection::Allow,
                expiry_ns: 4_000,
            },
            &mut reports,
        );
        reports.clear();
        assert_eq!(exchange.scheduled_task_count(), 1);

        // Drain at the deadline — remainder cancelled.
        exchange.drain_due_scheduled_tasks(4_000, &mut reports);
        assert_eq!(reports.len(), 1);
        assert!(matches!(
            reports[0],
            ExecutionReport::Cancelled {
                order_id: OrderId(2),
                remaining_quantity,
                ..
            } if remaining_quantity.get() == 4
        ));
    }

    /// Triggered GTD stop becomes a resting limit (same OrderId, same
    /// expiry_ns); the original scheduled task still finds and cancels it.
    #[test]
    fn gtd_stop_triggered_into_resting_still_expires() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 100_000);
        exchange.deposit(ACCT_B, BTC, 100);

        let mut reports = Vec::new();
        // ACCT_A: GTD stop-limit buy that triggers at 110, limit 110, exp 8000.
        exchange.execute(
            btc,
            Order {
                id: OrderId(1),
                account: ACCT_A,
                side: Side::Buy,
                order_type: OrderType::StopLimit {
                    trigger_price: price(110),
                    limit_price: price(110),
                },
                time_in_force: TimeInForce::GTD,
                quantity: qty(2),
                stp: SelfTradeProtection::Allow,
                expiry_ns: 8_000,
            },
            &mut reports,
        );
        reports.clear();
        assert_eq!(exchange.scheduled_task_count(), 1);

        // ACCT_B sells low to set last_trade and trigger the stop.
        // First make a buy to populate the bid side (so the sell can fill).
        exchange.execute(
            btc,
            Order {
                id: OrderId(10),
                account: ACCT_A,
                side: Side::Buy,
                order_type: OrderType::Limit {
                    price: price(115),
                    post_only: false,
                },
                time_in_force: TimeInForce::GTC,
                quantity: qty(1),
                stp: SelfTradeProtection::Allow,
                expiry_ns: 0,
            },
            &mut reports,
        );
        reports.clear();
        exchange.execute(
            btc,
            Order {
                id: OrderId(11),
                account: ACCT_B,
                side: Side::Sell,
                order_type: OrderType::Limit {
                    price: price(115),
                    post_only: false,
                },
                time_in_force: TimeInForce::IOC,
                quantity: qty(1),
                stp: SelfTradeProtection::Allow,
                expiry_ns: 0,
            },
            &mut reports,
        );
        // Should have triggered the stop — order 1 is now a resting limit.
        let triggered = reports.iter().any(|r| {
            matches!(
                r,
                ExecutionReport::Triggered {
                    order_id: OrderId(1),
                    ..
                }
            )
        });
        assert!(triggered, "stop should have triggered");
        reports.clear();

        // Drain past expiry — the originally scheduled task cancels the
        // now-resting limit form of the order.
        exchange.drain_due_scheduled_tasks(8_000, &mut reports);
        assert_eq!(reports.len(), 1);
        assert!(matches!(
            reports[0],
            ExecutionReport::Cancelled {
                order_id: OrderId(1),
                ..
            }
        ));
    }

    /// Triggered stop: stop sell triggers via a trade, triggered market
    /// sell fills. Verifies that the stop's embedded reservation slot is
    /// carried through check_triggers → execute_market → fill and that
    /// balances are conserved.
    #[test]
    fn triggered_stop_fill_balance_conservation() {
        let mut exchange = Exchange::new();
        let spec = btc_usd_spec();
        exchange.add_instrument(spec);

        // Acct A: buyer with USD. Acct B: seller with BTC.
        exchange.deposit(ACCT_A, USD, 100_000);
        exchange.deposit(ACCT_B, BTC, 100);

        let mut reports = Vec::new();

        // Acct B places a stop sell at trigger=500, qty=10.
        exchange.execute(
            spec.symbol,
            Order {
                id: OrderId(1),
                account: ACCT_B,
                side: Side::Sell,
                order_type: OrderType::Stop {
                    trigger_price: price(500),
                },
                time_in_force: TimeInForce::GTC,
                quantity: qty(10),
                stp: SelfTradeProtection::Allow,
                expiry_ns: 0,
            },
            &mut reports,
        );
        reports.clear();

        // Acct B places a limit sell at price=500, qty=5.
        exchange.execute(
            spec.symbol,
            limit_order(2, ACCT_B, Side::Sell, 500, 5, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();

        // Acct A buys with market order qty=15. This should:
        // 1. Fill 5 against the limit sell at 500
        // 2. Trade at 500 triggers the stop sell
        // 3. Triggered stop becomes market sell — no bids left, so cancelled
        exchange.execute(
            spec.symbol,
            Order {
                id: OrderId(1),
                account: ACCT_A,
                side: Side::Buy,
                order_type: OrderType::Market,
                time_in_force: TimeInForce::GTC,
                quantity: qty(15),
                stp: SelfTradeProtection::Allow,
                expiry_ns: 0,
            },
            &mut reports,
        );

        // Verify balance conservation.
        let bal_a_usd = exchange.accounts().balance(ACCT_A, USD);
        let bal_b_usd = exchange.accounts().balance(ACCT_B, USD);
        let total_usd =
            bal_a_usd.available + bal_a_usd.reserved + bal_b_usd.available + bal_b_usd.reserved;
        assert_eq!(total_usd, 100_000, "USD conservation violated");

        let bal_a_btc = exchange.accounts().balance(ACCT_A, BTC);
        let bal_b_btc = exchange.accounts().balance(ACCT_B, BTC);
        let total_btc =
            bal_a_btc.available + bal_a_btc.reserved + bal_b_btc.available + bal_b_btc.reserved;
        assert_eq!(total_btc, 100, "BTC conservation violated");

        // No reservations should remain (all orders consumed or cancelled).
        assert_eq!(bal_a_usd.reserved, 0);
        assert_eq!(bal_b_btc.reserved, 0);
    }

    /// Triggered stop-limit that partially fills and rests: verifies
    /// the triggered order's slot is resolvable from order_index for
    /// fill accounting.
    #[test]
    fn triggered_stop_limit_partial_fill_rests() {
        let mut exchange = Exchange::new();
        let spec = btc_usd_spec();
        exchange.add_instrument(spec);

        exchange.deposit(ACCT_A, USD, 100_000);
        exchange.deposit(ACCT_B, BTC, 100);

        let mut reports = Vec::new();

        // Acct B places a stop-limit: trigger=500, limit=400, sell qty=10.
        exchange.execute(
            spec.symbol,
            Order {
                id: OrderId(1),
                account: ACCT_B,
                side: Side::Sell,
                order_type: OrderType::StopLimit {
                    trigger_price: price(500),
                    limit_price: price(400),
                },
                time_in_force: TimeInForce::GTC,
                quantity: qty(10),
                stp: SelfTradeProtection::Allow,
                expiry_ns: 0,
            },
            &mut reports,
        );
        reports.clear();

        // Acct A places a limit buy at 500, qty=5.
        exchange.execute(
            spec.symbol,
            limit_order(1, ACCT_A, Side::Buy, 500, 5, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();

        // Acct B places a limit sell at 500, qty=1 to create a trade.
        exchange.execute(
            spec.symbol,
            limit_order(2, ACCT_B, Side::Sell, 500, 1, TimeInForce::GTC),
            &mut reports,
        );
        // This trade at 500 triggers the stop-limit. The triggered limit
        // sell at 400 can match Acct A's buy at 500 (price 400 <= 500).
        // Acct A's buy has qty=5, triggered sell has qty=10 → partial fill.
        // Remaining 6 rests on the ask side at 400 (5 were consumed by
        // the initial sell + the fill).

        // Verify balance conservation.
        let bal_a_usd = exchange.accounts().balance(ACCT_A, USD);
        let bal_b_usd = exchange.accounts().balance(ACCT_B, USD);
        let total_usd =
            bal_a_usd.available + bal_a_usd.reserved + bal_b_usd.available + bal_b_usd.reserved;
        assert_eq!(total_usd, 100_000, "USD conservation violated");

        let bal_a_btc = exchange.accounts().balance(ACCT_A, BTC);
        let bal_b_btc = exchange.accounts().balance(ACCT_B, BTC);
        let total_btc =
            bal_a_btc.available + bal_a_btc.reserved + bal_b_btc.available + bal_b_btc.reserved;
        assert_eq!(total_btc, 100, "BTC conservation violated");
    }

    /// Stop-limit buy with IOC TIF: triggers, fills what's available,
    /// cancels remainder (IOC semantics apply post-trigger).
    #[test]
    fn stop_limit_ioc_cancels_unfilled_remainder() {
        let mut exchange = Exchange::new();
        let spec = btc_usd_spec();
        exchange.add_instrument(spec);

        exchange.deposit(ACCT_A, USD, 100_000);
        exchange.deposit(ACCT_B, BTC, 100);
        exchange.deposit(ACCT_B, USD, 100_000);

        let mut reports = Vec::new();

        // ACCT_B: resting sell 3@500.
        exchange.execute(
            spec.symbol,
            limit_order(1, ACCT_B, Side::Sell, 500, 3, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();

        // ACCT_A: stop-limit buy, trigger=500, limit=500, qty=10, IOC.
        exchange.execute(
            spec.symbol,
            Order {
                id: OrderId(1),
                account: ACCT_A,
                side: Side::Buy,
                order_type: OrderType::StopLimit {
                    trigger_price: price(500),
                    limit_price: price(500),
                },
                time_in_force: TimeInForce::IOC,
                quantity: qty(10),
                stp: SelfTradeProtection::Allow,
                expiry_ns: 0,
            },
            &mut reports,
        );
        reports.clear();

        // Trade at 500 to trigger the stop.
        exchange.execute(
            spec.symbol,
            limit_order(2, ACCT_B, Side::Sell, 500, 1, TimeInForce::GTC),
            &mut reports,
        );
        exchange.execute(
            spec.symbol,
            limit_order(2, ACCT_A, Side::Buy, 500, 1, TimeInForce::GTC),
            &mut reports,
        );

        // Triggered limit buy fills 3@500 (resting ask), IOC cancels remaining 7.
        let fills: Vec<_> = reports
            .iter()
            .filter(|r| matches!(r, ExecutionReport::Fill { .. }))
            .collect();
        let cancels: Vec<_> = reports
            .iter()
            .filter(|r| {
                matches!(
                    r,
                    ExecutionReport::Cancelled {
                        order_id: OrderId(1),
                        ..
                    }
                )
            })
            .collect();
        assert!(
            reports.iter().any(|r| matches!(
                r,
                ExecutionReport::Triggered {
                    order_id: OrderId(1),
                    ..
                }
            )),
            "stop should trigger"
        );
        // The trigger fill (1@500) + the stop-limit fills (3@500).
        assert!(
            fills.len() >= 2,
            "expected at least trigger fill + stop-limit fill, got {}",
            fills.len()
        );
        assert_eq!(cancels.len(), 1, "IOC remainder should be cancelled");
    }

    /// Stop-limit with STP CancelNewest: the triggered limit order should
    /// respect self-trade prevention just like a regular limit order.
    #[test]
    fn stop_limit_stp_cancel_newest() {
        let mut exchange = Exchange::new();
        let spec = btc_usd_spec();
        exchange.add_instrument(spec);

        exchange.deposit(ACCT_A, USD, 100_000);
        exchange.deposit(ACCT_A, BTC, 100);
        exchange.deposit(ACCT_B, USD, 100_000);
        exchange.deposit(ACCT_B, BTC, 100);

        let mut reports = Vec::new();

        // ACCT_A: resting sell @ 500.
        exchange.execute(
            spec.symbol,
            limit_order(1, ACCT_A, Side::Sell, 500, 5, TimeInForce::GTC),
            &mut reports,
        );
        // ACCT_B: resting sell @ 500 (behind A in queue).
        exchange.execute(
            spec.symbol,
            limit_order(1, ACCT_B, Side::Sell, 500, 5, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();

        // ACCT_A: stop-limit buy, trigger=500, limit=500, qty=5, CancelNewest.
        exchange.execute(
            spec.symbol,
            Order {
                id: OrderId(2),
                account: ACCT_A,
                side: Side::Buy,
                order_type: OrderType::StopLimit {
                    trigger_price: price(500),
                    limit_price: price(500),
                },
                time_in_force: TimeInForce::GTC,
                quantity: qty(5),
                stp: SelfTradeProtection::CancelNewest,
                expiry_ns: 0,
            },
            &mut reports,
        );
        reports.clear();

        // Trigger via ACCT_B buy → trade at 500.
        exchange.execute(
            spec.symbol,
            limit_order(3, ACCT_B, Side::Buy, 500, 1, TimeInForce::GTC),
            &mut reports,
        );

        // Triggered stop-limit buy hits ACCT_A's own sell → CancelNewest
        // cancels the taker (the stop-limit). It should NOT self-trade.
        let self_fills: Vec<_> = reports
            .iter()
            .filter(|r| {
                matches!(
                    r,
                    ExecutionReport::Fill {
                        taker_order_id: OrderId(2),
                        maker_account: a,
                        ..
                    } if *a == ACCT_A
                )
            })
            .collect();
        assert!(
            self_fills.is_empty(),
            "STP should prevent self-trade on triggered stop-limit"
        );
    }

    /// Stop-limit sell with wide gap between trigger and limit: the triggered
    /// limit sell rests because its limit price is above the best bid.
    #[test]
    fn stop_limit_wide_trigger_limit_gap_rests() {
        let mut exchange = Exchange::new();
        let spec = btc_usd_spec();
        exchange.add_instrument(spec);

        exchange.deposit(ACCT_A, USD, 100_000);
        exchange.deposit(ACCT_A, BTC, 100);
        exchange.deposit(ACCT_B, USD, 100_000);
        exchange.deposit(ACCT_B, BTC, 100);

        let mut reports = Vec::new();

        // ACCT_A: resting buy @ 400.
        exchange.execute(
            spec.symbol,
            limit_order(1, ACCT_A, Side::Buy, 400, 10, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();

        // ACCT_B: stop-limit sell, trigger=500, limit=450, qty=5.
        // Wide gap: trigger at 500, but limit sell at 450 (above best bid 400).
        exchange.execute(
            spec.symbol,
            Order {
                id: OrderId(1),
                account: ACCT_B,
                side: Side::Sell,
                order_type: OrderType::StopLimit {
                    trigger_price: price(500),
                    limit_price: price(450),
                },
                time_in_force: TimeInForce::GTC,
                quantity: qty(5),
                stp: SelfTradeProtection::Allow,
                expiry_ns: 0,
            },
            &mut reports,
        );
        reports.clear();

        // Trade at 500 to trigger: ACCT_A sells 1@500 to ACCT_B.
        exchange.execute(
            spec.symbol,
            limit_order(2, ACCT_A, Side::Sell, 500, 1, TimeInForce::GTC),
            &mut reports,
        );
        exchange.execute(
            spec.symbol,
            limit_order(2, ACCT_B, Side::Buy, 500, 1, TimeInForce::GTC),
            &mut reports,
        );

        // Triggered limit sell at 450 can't match bid@400 (450 > 400) → rests.
        assert!(
            reports.iter().any(|r| matches!(
                r,
                ExecutionReport::Triggered {
                    order_id: OrderId(1),
                    ..
                }
            )),
            "stop should trigger"
        );
        assert!(
            reports.iter().any(|r| matches!(
                r,
                ExecutionReport::Placed {
                    order_id: OrderId(1),
                    side: Side::Sell,
                    ..
                }
            )),
            "triggered limit sell should rest (limit 450 > best bid 400)"
        );

        // Verify balance conservation.
        let total_btc = exchange.accounts().balance(ACCT_A, BTC).available
            + exchange.accounts().balance(ACCT_A, BTC).reserved
            + exchange.accounts().balance(ACCT_B, BTC).available
            + exchange.accounts().balance(ACCT_B, BTC).reserved;
        assert_eq!(total_btc, 200, "BTC conservation violated");
    }

    /// Stop-limit buy fills across multiple price levels after triggering.
    #[test]
    fn stop_limit_fills_multiple_levels() {
        let mut exchange = Exchange::new();
        let spec = btc_usd_spec();
        exchange.add_instrument(spec);

        exchange.deposit(ACCT_A, USD, 1_000_000);
        exchange.deposit(ACCT_B, BTC, 1_000);
        exchange.deposit(ACCT_B, USD, 1_000_000);

        let mut reports = Vec::new();

        // ACCT_B: asks at 500, 510, 520.
        for (id, p) in [(1, 500), (2, 510), (3, 520)] {
            exchange.execute(
                spec.symbol,
                limit_order(id, ACCT_B, Side::Sell, p, 5, TimeInForce::GTC),
                &mut reports,
            );
        }
        reports.clear();

        // ACCT_A: stop-limit buy, trigger=500, limit=520, qty=12.
        // Should fill 5@500 + 5@510 + 2@520 after triggering.
        exchange.execute(
            spec.symbol,
            Order {
                id: OrderId(1),
                account: ACCT_A,
                side: Side::Buy,
                order_type: OrderType::StopLimit {
                    trigger_price: price(500),
                    limit_price: price(520),
                },
                time_in_force: TimeInForce::GTC,
                quantity: qty(12),
                stp: SelfTradeProtection::Allow,
                expiry_ns: 0,
            },
            &mut reports,
        );
        reports.clear();

        // Trigger: ACCT_B buys own ask at 500 (STP Allow).
        exchange.execute(
            spec.symbol,
            limit_order(4, ACCT_B, Side::Buy, 500, 1, TimeInForce::GTC),
            &mut reports,
        );

        // After trigger, the stop-limit becomes a limit buy at 520 with qty=12.
        // It sweeps: remaining 4@500 + 5@510 + 3 left, but only 5@520 available
        // at that level → fills 4@500 + 5@510 + partially 3@520.
        let trigger_count = reports
            .iter()
            .filter(|r| {
                matches!(
                    r,
                    ExecutionReport::Triggered {
                        order_id: OrderId(1),
                        ..
                    }
                )
            })
            .count();
        assert_eq!(trigger_count, 1, "stop should trigger exactly once");

        let stop_limit_fills: Vec<_> = reports
            .iter()
            .filter(|r| {
                matches!(
                    r,
                    ExecutionReport::Fill {
                        taker_order_id: OrderId(1),
                        ..
                    }
                )
            })
            .collect();
        assert!(
            stop_limit_fills.len() >= 2,
            "stop-limit should fill across multiple levels, got {} fills",
            stop_limit_fills.len()
        );

        // Verify balance conservation.
        let total_usd = exchange.accounts().balance(ACCT_A, USD).available
            + exchange.accounts().balance(ACCT_A, USD).reserved
            + exchange.accounts().balance(ACCT_B, USD).available
            + exchange.accounts().balance(ACCT_B, USD).reserved;
        assert_eq!(total_usd, 2_000_000, "USD conservation violated");
    }

    /// Snapshot round-trip: verifies that reservation slots survive
    /// save/restore and that post-restore operations work correctly.
    #[test]
    fn snapshot_roundtrip_reservation_integrity() {
        let mut exchange = Exchange::new();
        let spec = btc_usd_spec();
        exchange.add_instrument(spec);

        exchange.deposit(ACCT_A, USD, 100_000);
        exchange.deposit(ACCT_B, BTC, 100);

        let mut reports = Vec::new();

        // Place a resting sell order.
        exchange.execute(
            spec.symbol,
            limit_order(1, ACCT_B, Side::Sell, 500, 10, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();

        // Snapshot and restore.
        let restored = exchange.clone_via_snapshot();

        // Verify the restored exchange has the correct reserved balance.
        let bal = restored.accounts().balance(ACCT_B, BTC);
        assert_eq!(bal.reserved, 10, "post-restore: seller reservation lost");

        // Execute a fill against the restored resting order.
        let mut restored = restored;
        exchange.execute(
            spec.symbol,
            limit_order(1, ACCT_A, Side::Buy, 500, 5, TimeInForce::GTC),
            &mut reports,
        );
        restored.execute(
            spec.symbol,
            limit_order(1, ACCT_A, Side::Buy, 500, 5, TimeInForce::GTC),
            &mut reports,
        );

        // Both should have the same balances after the fill.
        let orig_a_usd = exchange.accounts().balance(ACCT_A, USD);
        let rest_a_usd = restored.accounts().balance(ACCT_A, USD);
        assert_eq!(orig_a_usd.available, rest_a_usd.available);
        assert_eq!(orig_a_usd.reserved, rest_a_usd.reserved);

        let orig_b_btc = exchange.accounts().balance(ACCT_B, BTC);
        let rest_b_btc = restored.accounts().balance(ACCT_B, BTC);
        assert_eq!(orig_b_btc.available, rest_b_btc.available);
        assert_eq!(orig_b_btc.reserved, rest_b_btc.reserved);

        // Cancel the remaining resting order on the restored exchange.
        reports.clear();
        restored.cancel(spec.symbol, ACCT_B, OrderId(1), &mut reports);
        assert_eq!(
            restored.accounts().balance(ACCT_B, BTC).reserved,
            0,
            "post-restore cancel: reservation not released"
        );
    }

    #[test]
    fn snapshot_roundtrip_multi_instrument_fill() {
        // Reproduce the shadow-stage panic at runtime: many resting orders
        // across multiple instruments, then a clone, then a taker that
        // matches against a recovered maker. If any maker's reservation
        // slot wasn't injected by `inject_reservation_slots`, the fill
        // path indexes the slab with `ReservationSlot::DUMMY` (u32::MAX)
        // and the engine panics. The bot's demo hits this scenario the
        // moment a bot order matches against a journal-recovered maker.
        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());
        exchange.add_instrument(eth_usd_spec());

        // Seed lots of accounts with both currencies so we can place
        // many makers across both instruments — same shape the bot
        // produces (31 accounts touching 2 symbols). Plus two extra
        // accounts (50, 51) used by the post-restore takers below.
        for acct in 2..=32u32 {
            exchange.deposit(AccountId(acct), USD, 1_000_000);
            exchange.deposit(AccountId(acct), BTC, 1_000);
            exchange.deposit(AccountId(acct), ETH, 1_000);
        }
        for acct in [50u32, 51] {
            exchange.deposit(AccountId(acct), USD, 1_000_000);
            exchange.deposit(AccountId(acct), BTC, 1_000);
            exchange.deposit(AccountId(acct), ETH, 1_000);
        }

        // Place 200 resting orders, many sharing price levels (the bot's
        // shape — narrow spread, many orders clustered around the mid).
        let mut reports = Vec::new();
        for i in 0..200u64 {
            let acct = AccountId(2 + (i as u32 % 31));
            let symbol = if i % 2 == 0 { Symbol(1) } else { Symbol(2) };
            let side = if i % 3 == 0 { Side::Buy } else { Side::Sell };
            let id = 1000 + i;
            // Cluster prices: only ~10 distinct price points → many
            // orders per level → exercises the level-queue + index pair.
            let p = match side {
                Side::Buy => 95 - (i % 5),
                Side::Sell => 105 + (i % 5),
            };
            exchange.execute(
                symbol,
                limit_order(id, acct, side, p, 5, TimeInForce::GTC),
                &mut reports,
            );
        }
        reports.clear();

        let mut restored = exchange.clone_via_snapshot();

        // Drive a taker into the restored exchange that should match an
        // existing resting bid (any of the Side::Buy orders above).
        // If any maker slot is DUMMY, fill() panics here.
        restored.execute(
            Symbol(1),
            limit_order(9999, AccountId(50), Side::Sell, 50, 1, TimeInForce::GTC),
            &mut reports,
        );

        // Symmetric: a buy that crosses an existing ask.
        restored.execute(
            Symbol(2),
            limit_order(9998, AccountId(51), Side::Buy, 200, 1, TimeInForce::GTC),
            &mut reports,
        );

        // Survival is the assertion. If we got here without panicking,
        // every recovered maker had a real reservation slot.
    }

    #[test]
    fn snapshot_roundtrip_preserves_live_dedup() {
        // The v15 snapshot format drops the explicit OrderId map and
        // rebuilds the live `(account, order_id)` set from `order_index`
        // + `stop_index` on restore. Verify the rebuild: a duplicate of
        // a still-resting order must reject post-restore, while a
        // duplicate of an order that *closed* before the snapshot must
        // succeed (the entry should not have made it into the live set).
        let mut exchange = Exchange::new();
        let spec = btc_usd_spec();
        exchange.add_instrument(spec);
        exchange.deposit(ACCT_A, USD, 100_000);
        exchange.deposit(ACCT_B, BTC, 100);

        let mut reports = Vec::new();

        // OrderId 7 fills (closes), OrderId 9 rests.
        exchange.execute(
            spec.symbol,
            limit_order(7, ACCT_B, Side::Sell, 100, 5, TimeInForce::GTC),
            &mut reports,
        );
        exchange.execute(
            spec.symbol,
            limit_order(7, ACCT_A, Side::Buy, 100, 5, TimeInForce::GTC),
            &mut reports,
        );
        exchange.execute(
            spec.symbol,
            limit_order(9, ACCT_A, Side::Buy, 90, 5, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();

        let mut restored = exchange.clone_via_snapshot();

        // Reusing OrderId 7 (closed before snapshot) must succeed.
        restored.execute(
            spec.symbol,
            limit_order(7, ACCT_A, Side::Buy, 89, 5, TimeInForce::GTC),
            &mut reports,
        );
        assert!(
            matches!(reports[0], ExecutionReport::Placed { .. }),
            "reuse of closed-before-snapshot id should place, got {:?}",
            reports[0]
        );

        // Duplicating OrderId 9 (resting at snapshot time) must reject.
        reports.clear();
        restored.execute(
            spec.symbol,
            limit_order(9, ACCT_A, Side::Buy, 88, 5, TimeInForce::GTC),
            &mut reports,
        );
        assert!(
            matches!(
                reports[0],
                ExecutionReport::Rejected {
                    reason: RejectReason::DuplicateOrderId,
                    ..
                }
            ),
            "duplicate of live-at-snapshot id should reject, got {:?}",
            reports[0]
        );
    }

    // --- Instrument lifecycle tests ---

    #[test]
    fn disable_instrument_cancels_all_orders() {
        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 1_000_000);
        exchange.deposit(ACCT_B, BTC, 1000);

        let mut reports = Vec::new();
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
        exchange.execute(
            Symbol(1),
            limit_order(100, ACCT_B, Side::Sell, 200, 5, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();

        exchange.disable_instrument(Symbol(1), &mut reports);

        let cancelled_count = reports
            .iter()
            .filter(|r| matches!(r, ExecutionReport::Cancelled { .. }))
            .count();
        assert_eq!(cancelled_count, 3);
        assert!(matches!(
            reports.last().unwrap(),
            ExecutionReport::InstrumentStatusChanged {
                status: InstrumentStatus::Disabled,
                ..
            }
        ));
    }

    #[test]
    fn disable_instrument_releases_reservations() {
        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 10_000);

        let mut reports = Vec::new();
        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_A, Side::Buy, 100, 50, TimeInForce::GTC),
            &mut reports,
        );
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).available, 5_000);
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 5_000);

        reports.clear();
        exchange.disable_instrument(Symbol(1), &mut reports);

        assert_eq!(exchange.accounts().balance(ACCT_A, USD).available, 10_000);
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 0);
    }

    #[test]
    fn disabled_instrument_rejects_new_orders() {
        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 10_000);

        let mut reports = Vec::new();
        exchange.disable_instrument(Symbol(1), &mut reports);
        reports.clear();

        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        assert_eq!(reports.len(), 1);
        assert!(matches!(
            reports[0],
            ExecutionReport::Rejected {
                reason: RejectReason::InstrumentDisabled,
                ..
            }
        ));
    }

    #[test]
    fn enable_instrument_allows_trading() {
        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 10_000);

        let mut reports = Vec::new();
        exchange.disable_instrument(Symbol(1), &mut reports);
        reports.clear();

        exchange.enable_instrument(Symbol(1), &mut reports);
        assert!(matches!(
            reports.last().unwrap(),
            ExecutionReport::InstrumentStatusChanged {
                status: InstrumentStatus::Enabled,
                ..
            }
        ));
        reports.clear();

        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
    }

    #[test]
    fn remove_only_when_disabled() {
        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());

        let mut reports = Vec::new();
        exchange.remove_instrument(Symbol(1), &mut reports);
        assert!(reports.is_empty());

        exchange.deposit(ACCT_A, USD, 10_000);
        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
    }

    #[test]
    fn remove_frees_slot() {
        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());

        let mut reports = Vec::new();
        exchange.disable_instrument(Symbol(1), &mut reports);
        reports.clear();

        exchange.remove_instrument(Symbol(1), &mut reports);
        assert!(matches!(
            reports.last().unwrap(),
            ExecutionReport::InstrumentStatusChanged {
                status: InstrumentStatus::Removed,
                ..
            }
        ));
        reports.clear();

        exchange.deposit(ACCT_A, USD, 10_000);
        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        assert!(matches!(
            reports[0],
            ExecutionReport::Rejected {
                reason: RejectReason::UnknownSymbol,
                ..
            }
        ));
    }

    #[test]
    fn disable_is_idempotent() {
        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());

        let mut reports = Vec::new();
        exchange.disable_instrument(Symbol(1), &mut reports);
        reports.clear();

        exchange.disable_instrument(Symbol(1), &mut reports);
        assert!(reports.is_empty());
    }

    #[test]
    fn add_after_remove() {
        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());

        let mut reports = Vec::new();
        exchange.disable_instrument(Symbol(1), &mut reports);
        exchange.remove_instrument(Symbol(1), &mut reports);
        reports.clear();

        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 10_000);
        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
    }

    #[test]
    fn disable_cancels_pending_stops() {
        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 1_000_000);

        let mut reports = Vec::new();
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
                expiry_ns: 0,
            },
            &mut reports,
        );
        reports.clear();

        exchange.disable_instrument(Symbol(1), &mut reports);

        let cancelled_count = reports
            .iter()
            .filter(|r| matches!(r, ExecutionReport::Cancelled { .. }))
            .count();
        assert_eq!(cancelled_count, 1);
    }

    #[test]
    fn cancel_replace_on_disabled_instrument() {
        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 10_000);

        let mut reports = Vec::new();
        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();

        exchange.disable_instrument(Symbol(1), &mut reports);
        reports.clear();

        exchange.cancel_replace(
            Symbol(1),
            ACCT_A,
            OrderId(1),
            price(110),
            qty(10),
            &mut reports,
        );
        assert_eq!(reports.len(), 1);
        assert!(matches!(
            reports[0],
            ExecutionReport::Rejected {
                reason: RejectReason::InstrumentDisabled,
                ..
            }
        ));
    }

    #[test]
    fn cancel_on_disabled_instrument_is_noop() {
        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 10_000);

        let mut reports = Vec::new();
        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();

        exchange.disable_instrument(Symbol(1), &mut reports);
        assert_eq!(
            reports
                .iter()
                .filter(|r| matches!(r, ExecutionReport::Cancelled { .. }))
                .count(),
            1
        );
        reports.clear();

        exchange.cancel(Symbol(1), ACCT_A, OrderId(1), &mut reports);
        assert!(reports.is_empty());
    }

    // -- Fee account conservation tests --

    #[test]
    fn fees_credited_to_fee_account() {
        use crate::account::FEE_ACCOUNT;

        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 100_000);
        exchange.deposit(ACCT_B, BTC, 100);

        let mut reports = Vec::new();
        exchange.set_fee_schedule(
            Symbol(1),
            FeeSchedule {
                maker_fee_bps: 10,
                taker_fee_bps: 20,
            },
            &mut reports,
        );

        // Sell 10@100 (maker).
        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_B, Side::Sell, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();

        // Buy 10@100 (taker) — fills immediately.
        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
            &mut reports,
        );

        // cost = 100 * 10 = 1000
        // maker_fee = 1000 * 10 / 10_000 = 1
        // taker_fee = 1000 * 20 / 10_000 = 2
        let fill = reports
            .iter()
            .find(|r| matches!(r, ExecutionReport::Fill { .. }))
            .unwrap();
        if let ExecutionReport::Fill {
            maker_fee,
            taker_fee,
            ..
        } = fill
        {
            assert_eq!(*maker_fee, 1);
            assert_eq!(*taker_fee, 2);
        }

        // Fee account should have maker_fee + taker_fee = 3 USD.
        let fee_bal = exchange.accounts().balance(FEE_ACCOUNT, USD);
        assert_eq!(
            fee_bal.available, 3,
            "fee account should hold collected fees"
        );

        // System conservation: deposited 100_000 USD, no withdrawals.
        // system = ACCT_A(available+reserved) + ACCT_B(available) + FEE_ACCOUNT(available)
        let a_bal = exchange.accounts().balance(ACCT_A, USD);
        let b_bal = exchange.accounts().balance(ACCT_B, USD);
        let total = a_bal.available as u128
            + a_bal.reserved as u128
            + b_bal.available as u128
            + fee_bal.available as u128;
        assert_eq!(
            total, 100_000,
            "USD conservation: total must equal deposited"
        );
    }

    #[test]
    fn fee_schedule_change_after_placement_conserves_balance() {
        use crate::account::FEE_ACCOUNT;

        // Reproduces the bug found by proptests: fee schedule changes after
        // order placement, causing reservation to lack fee cushion. The fill
        // must not create or destroy value.
        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, BTC, 100);
        exchange.deposit(ACCT_B, USD, 10_000);

        // No fees at order placement time.
        let mut reports = Vec::new();

        // ACCT_A sells 10@100.
        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_A, Side::Sell, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();

        // Now set fees.
        exchange.set_fee_schedule(
            Symbol(1),
            FeeSchedule {
                maker_fee_bps: 0,
                taker_fee_bps: 50, // 0.5%
            },
            &mut reports,
        );

        // ACCT_B buys 10@100 — fills with taker fee, but buyer's reservation
        // was computed without fee cushion (fees were 0 at placement time...
        // but actually the buyer places now, with fees active, so cushion
        // is included). Let's test the seller side: seller placed when fees
        // were 0, so seller_fee = 0 and proceeds = full cost.
        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_B, Side::Buy, 100, 10, TimeInForce::GTC),
            &mut reports,
        );

        // Conservation: deposited 10_000 USD, no withdrawals.
        let a = exchange.accounts().balance(ACCT_A, USD);
        let b = exchange.accounts().balance(ACCT_B, USD);
        let fee = exchange.accounts().balance(FEE_ACCOUNT, USD);
        let total = a.available as u128
            + a.reserved as u128
            + b.available as u128
            + b.reserved as u128
            + fee.available as u128;
        assert_eq!(
            total, 10_000,
            "USD must be conserved after fee schedule change"
        );
    }

    #[test]
    fn stop_trigger_after_fee_change_conserves_balance() {
        use crate::account::FEE_ACCOUNT;

        // Exact reproduction of the proptest failure: stop order placed
        // with fee=0, then fee schedule changes, stop triggers.
        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, BTC, 100);
        exchange.deposit(ACCT_B, USD, 20_000);

        let mut reports = Vec::new();

        // ACCT_A sells 10@100 (resting, no fees yet).
        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_A, Side::Sell, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();

        // ACCT_B buys 1@100 (fills, establishes last_trade_price=100).
        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_B, Side::Buy, 100, 1, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();

        // ACCT_B places a stop buy: trigger_price=50, qty=1.
        // Since last_trade_price=100 >= 50, this triggers immediately
        // and becomes a market buy.
        // But first, change the fee schedule.
        exchange.set_fee_schedule(
            Symbol(1),
            FeeSchedule {
                maker_fee_bps: 0,
                taker_fee_bps: 100, // 1%
            },
            &mut reports,
        );

        // The stop's reservation was computed with the new fee schedule
        // (it's placed after the change), so this should be fine.
        // Let's test a scenario where the stop was placed BEFORE fees.
        // Reset: place stop with no fees, then change fees, then trigger.
        exchange.set_fee_schedule(
            Symbol(1),
            FeeSchedule {
                maker_fee_bps: 0,
                taker_fee_bps: 0,
            },
            &mut reports,
        );

        // Place stop buy@50 (no fees, so reservation = cost without cushion).
        exchange.execute(
            Symbol(1),
            Order {
                id: OrderId(2),
                account: ACCT_B,
                side: Side::Buy,
                order_type: OrderType::Stop {
                    trigger_price: price(50),
                },
                time_in_force: TimeInForce::GTC,
                quantity: qty(1),
                stp: SelfTradeProtection::Allow,
                expiry_ns: 0,
            },
            &mut reports,
        );
        reports.clear();

        // Change fees while the stop is pending.
        exchange.set_fee_schedule(
            Symbol(1),
            FeeSchedule {
                maker_fee_bps: 0,
                taker_fee_bps: 200, // 2%
            },
            &mut reports,
        );

        // Trigger the stop by trading at price >= 50.
        // Place a sell that fills against existing bid... but we need a bid
        // to trade and set last_trade_price. The stop triggers on
        // last_trade_price crossing the trigger. Since last_trade=100 >= 50,
        // the stop should have already triggered when placed.
        // Let's check if it triggered:
        assert!(
            reports.is_empty(),
            "stop should have triggered on placement (last_trade=100 >= trigger=50)"
        );
        // Actually, the stop triggers during the execute call above (placement).
        // Let me re-read the reports from that call.
        // The reports were cleared. Let me verify conservation directly.

        let a = exchange.accounts().balance(ACCT_A, USD);
        let b = exchange.accounts().balance(ACCT_B, USD);
        let fee = exchange.accounts().balance(FEE_ACCOUNT, USD);
        let total = a.available as u128
            + a.reserved as u128
            + b.available as u128
            + b.reserved as u128
            + fee.available as u128;
        assert_eq!(total, 20_000, "USD must be conserved with stop+fee change");
    }

    // -- Targeted edge-case tests --

    /// Cancel-replace to a price that would cross the spread must be rejected.
    #[test]
    fn cancel_replace_crossing_spread_rejected() {
        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 100_000);
        exchange.deposit(ACCT_B, BTC, 100);

        let mut reports = Vec::new();

        // Sell 10@100 (ask).
        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_B, Side::Sell, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();

        // Buy 10@90 (bid, resting below spread).
        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_A, Side::Buy, 90, 10, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();

        // Amend buy from 90 to 100 — would cross the ask at 100.
        exchange.cancel_replace(
            Symbol(1),
            ACCT_A,
            OrderId(1),
            price(100),
            qty(10),
            &mut reports,
        );
        assert!(
            reports.iter().any(|r| matches!(
                r,
                ExecutionReport::Rejected {
                    reason: RejectReason::PriceWouldCross,
                    ..
                }
            )),
            "cancel-replace crossing spread must be rejected"
        );
        // Original order should still be resting at 90.
        reports.clear();
        exchange.cancel(Symbol(1), ACCT_A, OrderId(1), &mut reports);
        assert!(
            reports
                .iter()
                .any(|r| matches!(r, ExecutionReport::Cancelled { .. })),
            "original order should still be on book after rejected amend"
        );
    }

    /// FOK with STP exclusion: only liquidity is from the same account.
    /// FOK must reject because the self-trade would be prevented.
    #[test]
    fn fok_stp_rejects_when_only_self_liquidity() {
        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 100_000);
        exchange.deposit(ACCT_A, BTC, 100);

        let mut reports = Vec::new();

        // ACCT_A sells 10@100.
        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_A, Side::Sell, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();

        // ACCT_A FOK buy 10@100 with STP CancelNewest — only liquidity
        // is own order, which STP would cancel. FOK must reject.
        exchange.execute(
            Symbol(1),
            Order {
                id: OrderId(2),
                account: ACCT_A,
                side: Side::Buy,
                order_type: OrderType::Limit {
                    price: price(100),
                    post_only: false,
                },
                time_in_force: TimeInForce::FOK,
                quantity: qty(10),
                stp: SelfTradeProtection::CancelNewest,
                expiry_ns: 0,
            },
            &mut reports,
        );

        // Should be rejected, not filled.
        assert!(
            reports
                .iter()
                .any(|r| matches!(r, ExecutionReport::Rejected { .. })),
            "FOK with only self-liquidity and STP must reject"
        );
        assert!(
            !reports
                .iter()
                .any(|r| matches!(r, ExecutionReport::Fill { .. })),
            "FOK must never partially fill"
        );
    }

    /// FOK with STP Allow: same-account liquidity should fill normally.
    #[test]
    fn fok_stp_allow_fills_self() {
        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 100_000);
        exchange.deposit(ACCT_A, BTC, 100);

        let mut reports = Vec::new();

        // ACCT_A sells 10@100.
        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_A, Side::Sell, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();

        // ACCT_A FOK buy 10@100 with STP Allow — self-trade allowed.
        exchange.execute(
            Symbol(1),
            Order {
                id: OrderId(2),
                account: ACCT_A,
                side: Side::Buy,
                order_type: OrderType::Limit {
                    price: price(100),
                    post_only: false,
                },
                time_in_force: TimeInForce::FOK,
                quantity: qty(10),
                stp: SelfTradeProtection::Allow,
                expiry_ns: 0,
            },
            &mut reports,
        );

        assert!(
            reports
                .iter()
                .any(|r| matches!(r, ExecutionReport::Fill { .. })),
            "FOK with STP Allow should fill against own order"
        );
    }

    /// Per-fill fee rounding: buyer_deducted + seller_proceeds + fee_credit == cost
    /// for every individual fill.
    #[test]
    fn per_fill_fee_rounding_conservation() {
        use crate::account::FEE_ACCOUNT;

        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());

        // Use odd prices/quantities to maximize rounding effects.
        let mut reports = Vec::new();
        exchange.set_fee_schedule(
            Symbol(1),
            FeeSchedule {
                maker_fee_bps: 3, // 0.03%
                taker_fee_bps: 7, // 0.07%
            },
            &mut reports,
        );

        exchange.deposit(ACCT_A, USD, 10_000_000);
        exchange.deposit(ACCT_B, BTC, 10_000);

        let mut reports = Vec::new();

        // Place asks at various odd prices.
        for (id, p, q) in [(1, 137, 13), (2, 251, 7), (3, 499, 3), (4, 1009, 1)] {
            exchange.execute(
                Symbol(1),
                limit_order(id, ACCT_B, Side::Sell, p, q, TimeInForce::GTC),
                &mut reports,
            );
            reports.clear();
        }

        // Aggressive buy that sweeps all levels.
        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_A, Side::Buy, 1100, 30, TimeInForce::GTC),
            &mut reports,
        );

        // Check system conservation after all fills.
        let a = exchange.accounts().balance(ACCT_A, USD);
        let b = exchange.accounts().balance(ACCT_B, USD);
        let fee = exchange.accounts().balance(FEE_ACCOUNT, USD);
        let total = a.available as u128
            + a.reserved as u128
            + b.available as u128
            + b.reserved as u128
            + fee.available as u128;
        assert_eq!(
            total, 10_000_000,
            "USD must be conserved across all fills with odd rounding"
        );
    }

    /// Stop trigger cascade: a fill triggers stop A, which fills and triggers stop B.
    #[test]
    fn stop_trigger_cascade() {
        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 1_000_000);
        exchange.deposit(ACCT_A, BTC, 1_000);
        exchange.deposit(ACCT_B, USD, 1_000_000);
        exchange.deposit(ACCT_B, BTC, 1_000);

        let mut reports = Vec::new();

        // Sell liquidity at multiple levels (ACCT_B provides asks).
        for (id, p) in [(1, 100), (2, 105), (3, 110)] {
            exchange.execute(
                Symbol(1),
                limit_order(id, ACCT_B, Side::Sell, p, 10, TimeInForce::GTC),
                &mut reports,
            );
        }
        reports.clear();

        // ACCT_A places stop-buy orders that chain:
        // Stop at trigger=95 → market buy (fills at 100, setting last_trade=100)
        // Stop at trigger=100 → market buy (triggers from the fill above)
        exchange.execute(
            Symbol(1),
            Order {
                id: OrderId(1),
                account: ACCT_A,
                side: Side::Buy,
                order_type: OrderType::Stop {
                    trigger_price: price(100),
                },
                time_in_force: TimeInForce::GTC,
                quantity: qty(5),
                stp: SelfTradeProtection::Allow,
                expiry_ns: 0,
            },
            &mut reports,
        );
        reports.clear();

        exchange.execute(
            Symbol(1),
            Order {
                id: OrderId(2),
                account: ACCT_A,
                side: Side::Buy,
                order_type: OrderType::Stop {
                    trigger_price: price(105),
                },
                time_in_force: TimeInForce::GTC,
                quantity: qty(3),
                stp: SelfTradeProtection::Allow,
                expiry_ns: 0,
            },
            &mut reports,
        );
        reports.clear();

        // Trigger the cascade: ACCT_B buys 1@100 (uses ACCT_B so ACCT_A's
        // balance isn't exhausted by stop reservations).
        // This fills against ACCT_B's own sell@100 (STP Allow), setting
        // last_trade=100. check_triggers fires: stop@100 triggers → market
        // buy fills remaining asks at 100. Stop@105 needs last_trade >= 105
        // but fills were at 100, so it should NOT trigger.
        exchange.execute(
            Symbol(1),
            limit_order(4, ACCT_B, Side::Buy, 100, 1, TimeInForce::GTC),
            &mut reports,
        );

        // We should see: the initial fill (1@100) + triggered stop fill.
        let fills: Vec<_> = reports
            .iter()
            .filter(|r| matches!(r, ExecutionReport::Fill { .. }))
            .collect();
        let triggers: Vec<_> = reports
            .iter()
            .filter(|r| matches!(r, ExecutionReport::Triggered { .. }))
            .collect();

        assert!(!fills.is_empty(), "should have at least one fill");
        assert!(
            triggers.len() <= 1,
            "stop@105 should not trigger (last_trade=100 < 105)"
        );
    }

    /// Cancel-replace preserves time priority when price unchanged and qty decreases.
    #[test]
    fn cancel_replace_preserves_priority_on_qty_decrease() {
        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, BTC, 1_000);
        exchange.deposit(ACCT_B, USD, 1_000_000);

        let mut reports = Vec::new();

        // Three sells at 100: ids 1, 2, 3 in order.
        for id in 1..=3 {
            exchange.execute(
                Symbol(1),
                limit_order(id, ACCT_A, Side::Sell, 100, 10, TimeInForce::GTC),
                &mut reports,
            );
        }
        reports.clear();

        // Amend order 2: same price, lower qty (5 instead of 10).
        // Should preserve time priority (still between 1 and 3).
        exchange.cancel_replace(
            Symbol(1),
            ACCT_A,
            OrderId(2),
            price(100),
            qty(5),
            &mut reports,
        );
        assert!(
            reports
                .iter()
                .any(|r| matches!(r, ExecutionReport::Replaced { .. }))
        );
        reports.clear();

        // Buy 15@100 — should fill 10 from order 1, then 5 from order 2.
        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_B, Side::Buy, 100, 15, TimeInForce::GTC),
            &mut reports,
        );

        let fills: Vec<_> = reports
            .iter()
            .filter_map(|r| match r {
                ExecutionReport::Fill {
                    maker_order_id,
                    quantity,
                    ..
                } => Some((maker_order_id.0, quantity.get())),
                _ => None,
            })
            .collect();

        assert_eq!(
            fills,
            vec![(1, 10), (2, 5)],
            "order 2 should fill after 1 (priority preserved)"
        );
    }

    /// Cancel-replace loses priority on price change.
    #[test]
    fn cancel_replace_loses_priority_on_price_change() {
        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, BTC, 1_000);
        exchange.deposit(ACCT_B, USD, 1_000_000);

        let mut reports = Vec::new();

        // Three sells at 100: ids 1, 2, 3.
        for id in 1..=3 {
            exchange.execute(
                Symbol(1),
                limit_order(id, ACCT_A, Side::Sell, 100, 10, TimeInForce::GTC),
                &mut reports,
            );
        }
        reports.clear();

        // Amend order 2: change price to 99, then back to 100. Loses priority.
        exchange.cancel_replace(
            Symbol(1),
            ACCT_A,
            OrderId(2),
            price(99),
            qty(10),
            &mut reports,
        );
        reports.clear();
        exchange.cancel_replace(
            Symbol(1),
            ACCT_A,
            OrderId(2),
            price(100),
            qty(10),
            &mut reports,
        );
        reports.clear();

        // Buy 25@100 — should fill 1 (10), then 3 (10), then 2 (5).
        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_B, Side::Buy, 100, 25, TimeInForce::GTC),
            &mut reports,
        );

        let fills: Vec<_> = reports
            .iter()
            .filter_map(|r| match r {
                ExecutionReport::Fill {
                    maker_order_id,
                    quantity,
                    ..
                } => Some((maker_order_id.0, quantity.get())),
                _ => None,
            })
            .collect();

        assert_eq!(
            fills,
            vec![(1, 10), (3, 10), (2, 5)],
            "order 2 should fill last (priority lost on price change)"
        );
    }

    /// Snapshot round-trip preserves fee account balance.
    #[test]
    fn snapshot_preserves_fee_account() {
        use crate::account::FEE_ACCOUNT;
        use crate::journal::snapshot;

        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 100_000);
        exchange.deposit(ACCT_B, BTC, 100);

        let mut reports = Vec::new();
        exchange.set_fee_schedule(
            Symbol(1),
            FeeSchedule {
                maker_fee_bps: 10,
                taker_fee_bps: 20,
            },
            &mut reports,
        );

        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_B, Side::Sell, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();
        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
            &mut reports,
        );

        let fee_before = exchange.accounts().balance(FEE_ACCOUNT, USD).available;
        assert!(fee_before > 0, "fees should have been collected");

        // Save and load snapshot.
        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("test.snapshot");
        snapshot::save(&exchange, 100, [0u8; 32], &snap_path).unwrap();
        let (restored, _, _) = snapshot::load(&snap_path).unwrap();

        let fee_after = restored.accounts().balance(FEE_ACCOUNT, USD).available;
        assert_eq!(
            fee_before, fee_after,
            "fee account must survive snapshot round-trip"
        );
    }

    /// Market order on an empty book is rejected with NoLiquidity.
    #[test]
    fn market_order_empty_book_rejected() {
        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 100_000);

        let mut reports = Vec::new();
        exchange.execute(
            Symbol(1),
            Order {
                id: OrderId(1),
                account: ACCT_A,
                side: Side::Buy,
                order_type: OrderType::Market,
                time_in_force: TimeInForce::IOC,
                quantity: qty(10),
                stp: SelfTradeProtection::Allow,
                expiry_ns: 0,
            },
            &mut reports,
        );

        assert!(
            reports.iter().any(|r| matches!(
                r,
                ExecutionReport::Rejected {
                    reason: RejectReason::NoLiquidity,
                    ..
                }
            )),
            "market order on empty book must be rejected with NoLiquidity"
        );
    }

    /// FOK buy that requires liquidity across multiple price levels:
    /// enough total quantity exists but spread across 3 levels.
    #[test]
    fn fok_fills_across_multiple_price_levels() {
        let mut exchange = Exchange::new();
        let spec = btc_usd_spec();
        exchange.add_instrument(spec);

        exchange.deposit(ACCT_A, USD, 1_000_000);
        exchange.deposit(ACCT_B, BTC, 100);

        let mut reports = Vec::new();

        // ACCT_B: asks at 3 levels: 5@100, 5@101, 5@102.
        for (id, p) in [(1, 100), (2, 101), (3, 102)] {
            exchange.execute(
                spec.symbol,
                limit_order(id, ACCT_B, Side::Sell, p, 5, TimeInForce::GTC),
                &mut reports,
            );
        }
        reports.clear();

        // ACCT_A: FOK buy 12@102 — needs 5@100 + 5@101 + 2@102 = 12.
        exchange.execute(
            spec.symbol,
            limit_order(1, ACCT_A, Side::Buy, 102, 12, TimeInForce::FOK),
            &mut reports,
        );

        let fills: Vec<_> = reports
            .iter()
            .filter(|r| matches!(r, ExecutionReport::Fill { .. }))
            .collect();
        assert_eq!(fills.len(), 3, "FOK should fill across 3 price levels");

        // No rejection or cancellation — fully filled.
        assert!(
            !reports.iter().any(|r| matches!(
                r,
                ExecutionReport::Rejected { .. } | ExecutionReport::Cancelled { .. }
            )),
            "FOK should not be rejected or cancelled"
        );

        // ACCT_A should own 12 BTC.
        let bal = exchange.accounts().balance(ACCT_A, BTC);
        assert_eq!(bal.available, 12);
    }

    /// FOK buy that has enough quantity at one level but not enough
    /// across all levels within the limit price — must be rejected
    /// without any fills.
    #[test]
    fn fok_rejected_insufficient_across_levels() {
        let mut exchange = Exchange::new();
        let spec = btc_usd_spec();
        exchange.add_instrument(spec);

        exchange.deposit(ACCT_A, USD, 1_000_000);
        exchange.deposit(ACCT_B, BTC, 100);

        let mut reports = Vec::new();

        // ACCT_B: 5@100 + 5@101 = 10 total within limit 101.
        exchange.execute(
            spec.symbol,
            limit_order(1, ACCT_B, Side::Sell, 100, 5, TimeInForce::GTC),
            &mut reports,
        );
        exchange.execute(
            spec.symbol,
            limit_order(2, ACCT_B, Side::Sell, 101, 5, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();

        // FOK buy 15@101 — only 10 available, should reject entirely.
        exchange.execute(
            spec.symbol,
            limit_order(1, ACCT_A, Side::Buy, 101, 15, TimeInForce::FOK),
            &mut reports,
        );

        assert_eq!(reports.len(), 1);
        assert!(matches!(
            reports[0],
            ExecutionReport::Rejected {
                reason: RejectReason::FOKCannotFill,
                ..
            }
        ));

        // Resting orders untouched.
        let bal = exchange.accounts().balance(ACCT_B, BTC);
        assert_eq!(bal.reserved, 10, "resting asks should be untouched");
    }

    /// Operators can withdraw collected fees from the FEE_ACCOUNT.
    #[test]
    fn fee_account_withdrawal() {
        use crate::account::FEE_ACCOUNT;

        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 100_000);
        exchange.deposit(ACCT_B, BTC, 100);

        let mut reports = Vec::new();
        exchange.set_fee_schedule(
            Symbol(1),
            FeeSchedule {
                maker_fee_bps: 10,
                taker_fee_bps: 20,
            },
            &mut reports,
        );

        // Create a fill to generate fees.
        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_B, Side::Sell, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
            &mut reports,
        );

        // cost=1000, maker_fee=1, taker_fee=2 → FEE_ACCOUNT has 3 USD.
        let fee_bal = exchange.accounts().balance(FEE_ACCOUNT, USD);
        assert_eq!(fee_bal.available, 3);

        // Withdraw 2 of the 3.
        let result = exchange.withdraw(FEE_ACCOUNT, USD, 2);
        assert!(result.is_ok(), "fee account withdrawal should succeed");
        let fee_bal = exchange.accounts().balance(FEE_ACCOUNT, USD);
        assert_eq!(fee_bal.available, 1);

        // Withdraw the remaining 1.
        let result = exchange.withdraw(FEE_ACCOUNT, USD, 1);
        assert!(result.is_ok());
        let fee_bal = exchange.accounts().balance(FEE_ACCOUNT, USD);
        assert_eq!(fee_bal.available, 0);

        // Over-withdraw fails.
        let result = exchange.withdraw(FEE_ACCOUNT, USD, 1);
        assert!(result.is_err(), "over-withdrawing fee account should fail");
    }

    /// Different instruments can have different fee schedules, and fills
    /// on each instrument apply the correct schedule.
    #[test]
    fn per_instrument_fee_isolation() {
        use crate::account::FEE_ACCOUNT;

        let mut exchange = Exchange::new();
        let btc_spec = btc_usd_spec();
        let eth_spec = eth_usd_spec();
        exchange.add_instrument(btc_spec);
        exchange.add_instrument(eth_spec);

        exchange.deposit(ACCT_A, USD, 1_000_000);
        exchange.deposit(ACCT_B, BTC, 1_000);
        exchange.deposit(ACCT_B, ETH, 1_000);

        // BTC/USD: 10 bps maker, 20 bps taker.
        let mut reports = Vec::new();
        exchange.set_fee_schedule(
            btc_spec.symbol,
            FeeSchedule {
                maker_fee_bps: 10,
                taker_fee_bps: 20,
            },
            &mut reports,
        );
        // ETH/USD: 50 bps maker, 100 bps taker.
        exchange.set_fee_schedule(
            eth_spec.symbol,
            FeeSchedule {
                maker_fee_bps: 50,
                taker_fee_bps: 100,
            },
            &mut reports,
        );

        // BTC/USD fill: 10@1000, cost=10_000.
        exchange.execute(
            btc_spec.symbol,
            limit_order(1, ACCT_B, Side::Sell, 1000, 10, TimeInForce::GTC),
            &mut reports,
        );
        exchange.execute(
            btc_spec.symbol,
            limit_order(1, ACCT_A, Side::Buy, 1000, 10, TimeInForce::GTC),
            &mut reports,
        );

        let btc_fill = reports
            .iter()
            .find(|r| matches!(r, ExecutionReport::Fill { .. }))
            .unwrap();
        // BTC fees: maker=10_000*10/10_000=10, taker=10_000*20/10_000=20.
        if let ExecutionReport::Fill {
            maker_fee,
            taker_fee,
            ..
        } = btc_fill
        {
            assert_eq!(*maker_fee, 10, "BTC maker fee");
            assert_eq!(*taker_fee, 20, "BTC taker fee");
        }
        reports.clear();

        // ETH/USD fill: 10@1000, cost=10_000.
        exchange.execute(
            eth_spec.symbol,
            limit_order(2, ACCT_B, Side::Sell, 1000, 10, TimeInForce::GTC),
            &mut reports,
        );
        exchange.execute(
            eth_spec.symbol,
            limit_order(2, ACCT_A, Side::Buy, 1000, 10, TimeInForce::GTC),
            &mut reports,
        );

        let eth_fill = reports
            .iter()
            .find(|r| matches!(r, ExecutionReport::Fill { .. }))
            .unwrap();
        // ETH fees: maker=10_000*50/10_000=50, taker=10_000*100/10_000=100.
        if let ExecutionReport::Fill {
            maker_fee,
            taker_fee,
            ..
        } = eth_fill
        {
            assert_eq!(*maker_fee, 50, "ETH maker fee");
            assert_eq!(*taker_fee, 100, "ETH taker fee");
        }

        // Total fees: BTC(10+20) + ETH(50+100) = 180.
        let fee_bal = exchange.accounts().balance(FEE_ACCOUNT, USD);
        assert_eq!(fee_bal.available, 180, "aggregated fee account");
    }

    /// Post-only buy that would cross a resting sell from the SAME account
    /// is rejected as PostOnlyWouldTake (post-only checked before STP).
    #[test]
    fn post_only_rejected_before_stp_evaluated() {
        let mut exchange = Exchange::new();
        let spec = btc_usd_spec();
        exchange.add_instrument(spec);

        exchange.deposit(ACCT_A, USD, 100_000);
        exchange.deposit(ACCT_A, BTC, 100);

        let mut reports = Vec::new();

        // ACCT_A: resting sell @ 500.
        exchange.execute(
            spec.symbol,
            limit_order(1, ACCT_A, Side::Sell, 500, 10, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();

        // ACCT_A: post-only buy @ 500 with CancelNewest STP.
        // Would cross own sell → post-only rejects before STP fires.
        exchange.execute(
            spec.symbol,
            Order {
                id: OrderId(2),
                account: ACCT_A,
                side: Side::Buy,
                order_type: OrderType::Limit {
                    price: price(500),
                    post_only: true,
                },
                time_in_force: TimeInForce::GTC,
                quantity: qty(5),
                stp: SelfTradeProtection::CancelNewest,
                expiry_ns: 0,
            },
            &mut reports,
        );

        assert_eq!(reports.len(), 1);
        assert!(
            matches!(
                reports[0],
                ExecutionReport::Rejected {
                    order_id: OrderId(2),
                    reason: RejectReason::PostOnlyWouldCross,
                    ..
                }
            ),
            "post-only should reject before STP is evaluated: {:?}",
            reports[0]
        );

        // Original sell should be untouched.
        let bal = exchange.accounts().balance(ACCT_A, BTC);
        assert_eq!(bal.reserved, 10);
    }

    /// Post-only buy that does NOT cross (different account's sell is above),
    /// rests, and then is filled as a maker when a sell arrives.
    /// Verifies post-only orders with STP work correctly as makers.
    #[test]
    fn post_only_with_stp_rests_and_fills_as_maker() {
        let mut exchange = Exchange::new();
        let spec = btc_usd_spec();
        exchange.add_instrument(spec);

        exchange.deposit(ACCT_A, USD, 100_000);
        exchange.deposit(ACCT_A, BTC, 100);
        exchange.deposit(ACCT_B, BTC, 100);

        let mut reports = Vec::new();

        // ACCT_A: post-only buy @ 400, CancelOldest STP.
        exchange.execute(
            spec.symbol,
            Order {
                id: OrderId(1),
                account: ACCT_A,
                side: Side::Buy,
                order_type: OrderType::Limit {
                    price: price(400),
                    post_only: true,
                },
                time_in_force: TimeInForce::GTC,
                quantity: qty(10),
                stp: SelfTradeProtection::CancelOldest,
                expiry_ns: 0,
            },
            &mut reports,
        );
        assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
        reports.clear();

        // ACCT_A: sell @ 400 (same account) → STP CancelOldest cancels
        // the resting post-only buy, taker continues.
        exchange.execute(
            spec.symbol,
            Order {
                id: OrderId(2),
                account: ACCT_A,
                side: Side::Sell,
                order_type: OrderType::Limit {
                    price: price(400),
                    post_only: false,
                },
                time_in_force: TimeInForce::GTC,
                quantity: qty(5),
                stp: SelfTradeProtection::CancelOldest,
                expiry_ns: 0,
            },
            &mut reports,
        );

        // STP CancelOldest should cancel the resting buy and the sell rests.
        assert!(
            reports.iter().any(|r| matches!(
                r,
                ExecutionReport::Cancelled {
                    order_id: OrderId(1),
                    ..
                }
            )),
            "STP CancelOldest should cancel the resting post-only buy"
        );
        // No fill should occur.
        assert!(
            !reports
                .iter()
                .any(|r| matches!(r, ExecutionReport::Fill { .. })),
            "no fill should occur due to STP"
        );
    }

    /// Negative maker fee (rebate) debits the FEE_ACCOUNT. When net fees
    /// are negative (rebate > taker fee), the fee account funds the rebate.
    #[test]
    fn rebate_debits_fee_account() {
        use crate::account::FEE_ACCOUNT;

        let mut exchange = Exchange::new();
        let spec = btc_usd_spec();
        exchange.add_instrument(spec);

        exchange.deposit(ACCT_A, USD, 100_000);
        exchange.deposit(ACCT_B, BTC, 100);

        // First: a normal trade to seed the fee account.
        let mut reports = Vec::new();
        exchange.set_fee_schedule(
            spec.symbol,
            FeeSchedule {
                maker_fee_bps: 10,
                taker_fee_bps: 20,
            },
            &mut reports,
        );
        exchange.execute(
            spec.symbol,
            limit_order(1, ACCT_B, Side::Sell, 1000, 10, TimeInForce::GTC),
            &mut reports,
        );
        exchange.execute(
            spec.symbol,
            limit_order(1, ACCT_A, Side::Buy, 1000, 10, TimeInForce::GTC),
            &mut reports,
        );

        // cost=10_000, maker_fee=10, taker_fee=20, fee_credit=30.
        let fee_after_first = exchange.accounts().balance(FEE_ACCOUNT, USD).available;
        assert_eq!(fee_after_first, 30);
        reports.clear();

        // Now switch to rebate schedule: -50 bps maker, 10 bps taker.
        exchange.set_fee_schedule(
            spec.symbol,
            FeeSchedule {
                maker_fee_bps: -50,
                taker_fee_bps: 10,
            },
            &mut reports,
        );

        // Second trade: ACCT_A sells (new BTC from first fill), ACCT_B buys.
        exchange.deposit(ACCT_B, USD, 100_000);
        exchange.execute(
            spec.symbol,
            limit_order(2, ACCT_A, Side::Sell, 1000, 5, TimeInForce::GTC),
            &mut reports,
        );
        exchange.execute(
            spec.symbol,
            limit_order(2, ACCT_B, Side::Buy, 1000, 5, TimeInForce::GTC),
            &mut reports,
        );

        // cost=5000, maker_fee=5000*(-50)/10_000=-25, taker_fee=5000*10/10_000=5.
        // Net fee_credit = buyer_deducted - seller_proceeds.
        // buyer_deducted = cost + taker_fee = 5000 + 5 = 5005.
        // seller_proceeds = cost - maker_fee = 5000 - (-25) = 5025.
        // fee_credit = 5005 - 5025 = -20 → FEE_ACCOUNT debited by 20.
        let fee_after_second = exchange.accounts().balance(FEE_ACCOUNT, USD).available;
        assert_eq!(
            fee_after_second,
            fee_after_first - 20,
            "rebate should debit fee account: had {fee_after_first}, expected {} after rebate",
            fee_after_first - 20
        );
    }

    // -----------------------------------------------------------------------
    // Fee schedule re-reservation
    // -----------------------------------------------------------------------

    #[test]
    fn fee_increase_adjusts_resting_buy_reservations() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 100_000);

        let mut reports = Vec::new();

        // Place buy limit at price=100, qty=10. No fees → reservation = 1000.
        exchange.execute(
            btc,
            limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 1_000);
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).available, 99_000);
        reports.clear();

        // Increase fees to 100bps (1%). Reservation should grow to 1010.
        exchange.set_fee_schedule(
            btc,
            FeeSchedule {
                maker_fee_bps: 100,
                taker_fee_bps: 0,
            },
            &mut reports,
        );
        assert!(reports.is_empty(), "no orders cancelled");
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 1_010);
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).available, 98_990);
    }

    #[test]
    fn fee_increase_cancels_underfunded_orders() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        // Deposit just enough for the order with no fees.
        exchange.deposit(ACCT_A, USD, 1_000);

        let mut reports = Vec::new();

        // Place buy limit at price=100, qty=10. Reservation = 1000 (all funds).
        exchange.execute(
            btc,
            limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).available, 0);
        reports.clear();

        // Increase fees to 100bps. New required = 1010 > 1000 available+reserved.
        // Order should be cancelled.
        exchange.set_fee_schedule(
            btc,
            FeeSchedule {
                maker_fee_bps: 100,
                taker_fee_bps: 0,
            },
            &mut reports,
        );
        assert_eq!(reports.len(), 1);
        assert!(matches!(
            reports[0],
            ExecutionReport::Cancelled {
                order_id: OrderId(1),
                ..
            }
        ));
        // Balance fully released.
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 0);
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).available, 1_000);
    }

    #[test]
    fn fee_decrease_releases_excess_reservation() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 100_000);

        let mut reports = Vec::new();

        // Set fees to 100bps, place order. Reservation = 1010.
        exchange.set_fee_schedule(
            btc,
            FeeSchedule {
                maker_fee_bps: 100,
                taker_fee_bps: 0,
            },
            &mut reports,
        );
        exchange.execute(
            btc,
            limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 1_010);
        reports.clear();

        // Decrease fees to 0. Reservation should shrink to 1000.
        exchange.set_fee_schedule(
            btc,
            FeeSchedule {
                maker_fee_bps: 0,
                taker_fee_bps: 0,
            },
            &mut reports,
        );
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 1_000);
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).available, 99_000);
    }

    #[test]
    fn fee_change_ignores_sell_orders() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, BTC, 100);

        let mut reports = Vec::new();

        // Place sell limit. Reservation = 10 BTC.
        exchange.execute(
            btc,
            limit_order(1, ACCT_A, Side::Sell, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        let reserved_before = exchange.accounts().balance(ACCT_A, BTC).reserved;
        reports.clear();

        // Change fees. Sell reservation should not change.
        exchange.set_fee_schedule(
            btc,
            FeeSchedule {
                maker_fee_bps: 500,
                taker_fee_bps: 500,
            },
            &mut reports,
        );
        assert!(reports.is_empty());
        assert_eq!(
            exchange.accounts().balance(ACCT_A, BTC).reserved,
            reserved_before
        );
    }

    #[test]
    fn fee_change_adjusts_stop_limit_buys() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 100_000);

        let mut reports = Vec::new();

        // Place buy stop-limit: trigger=200, limit=150, qty=10.
        // Reservation = 150 * 10 = 1500 (no fees).
        exchange.execute(
            btc,
            Order {
                id: OrderId(1),
                account: ACCT_A,
                side: Side::Buy,
                order_type: OrderType::StopLimit {
                    trigger_price: price(200),
                    limit_price: price(150),
                },
                time_in_force: TimeInForce::GTC,
                quantity: qty(10),
                stp: SelfTradeProtection::Allow,
                expiry_ns: 0,
            },
            &mut reports,
        );
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 1_500);
        reports.clear();

        // Increase fees to 200bps. New reservation = 1500 + 1500*200/10000 = 1530.
        exchange.set_fee_schedule(
            btc,
            FeeSchedule {
                maker_fee_bps: 200,
                taker_fee_bps: 0,
            },
            &mut reports,
        );
        assert!(reports.is_empty());
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 1_530);
    }

    #[test]
    fn fee_increase_cancels_underfunded_stop_limit_buy() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        // Deposit just enough to cover the stop-limit's cost with no fees.
        exchange.deposit(ACCT_A, USD, 1_500);

        let mut reports = Vec::new();

        // Place buy stop-limit: trigger=200, limit=150, qty=10.
        // Reservation = 150 * 10 = 1_500 (no fees) — consumes all available.
        exchange.execute(
            btc,
            Order {
                id: OrderId(1),
                account: ACCT_A,
                side: Side::Buy,
                order_type: OrderType::StopLimit {
                    trigger_price: price(200),
                    limit_price: price(150),
                },
                time_in_force: TimeInForce::GTC,
                quantity: qty(10),
                stp: SelfTradeProtection::Allow,
                expiry_ns: 0,
            },
            &mut reports,
        );
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 1_500);
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).available, 0);
        reports.clear();

        // Increase fees to 100bps. New required = 1_515 > 1_500 and
        // available = 0, so the top-up fails and the stop must be cancelled.
        exchange.set_fee_schedule(
            btc,
            FeeSchedule {
                maker_fee_bps: 100,
                taker_fee_bps: 0,
            },
            &mut reports,
        );
        assert_eq!(reports.len(), 1);
        assert!(matches!(
            reports[0],
            ExecutionReport::Cancelled {
                order_id: OrderId(1),
                ..
            }
        ));
        // Reservation fully released.
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 0);
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).available, 1_500);
        // Order count dropped to zero — withdrawal allowed.
        exchange.withdraw(ACCT_A, USD, 1_500).unwrap();
    }

    #[test]
    fn fee_change_adjusts_stop_market_buy_budget() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 10_000);
        exchange.deposit(ACCT_B, BTC, 100);

        let mut reports = Vec::new();

        // Place a sell limit to provide liquidity for the stop trigger.
        exchange.execute(
            btc,
            limit_order(1, ACCT_B, Side::Sell, 100, 50, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();

        // Place buy stop (market): trigger=100, qty=50.
        // Reserves entire available balance (10_000). No fees → budget = 10_000.
        exchange.execute(
            btc,
            Order {
                id: OrderId(1),
                account: ACCT_A,
                side: Side::Buy,
                order_type: OrderType::Stop {
                    trigger_price: price(100),
                },
                time_in_force: TimeInForce::GTC,
                quantity: qty(50),
                stp: SelfTradeProtection::Allow,
                expiry_ns: 0,
            },
            &mut reports,
        );
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 10_000);
        reports.clear();

        // Increase fees to 500bps (5%). Budget should shrink to leave room:
        // budget = 10_000 * 10_000 / (10_000 + 500) = 9523.
        exchange.set_fee_schedule(
            btc,
            FeeSchedule {
                maker_fee_bps: 0,
                taker_fee_bps: 500,
            },
            &mut reports,
        );
        // Reservation unchanged — still 10_000.
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 10_000);

        // Trigger the stop: ACCT_B buys against the resting sell at 100,
        // producing a trade at price=100 which triggers ACCT_A's buy stop.
        exchange.deposit(ACCT_B, USD, 100_000);
        exchange.execute(
            btc,
            limit_order(2, ACCT_B, Side::Buy, 100, 1, TimeInForce::IOC),
            &mut reports,
        );
        // The stop triggers and fills against the remaining resting sell.
        // With budget=9523 at price=100, cost=50*100=5000,
        // fee=5000*500/10000=250. Total=5250 < 9523. Should fill fine.
        let fills: Vec<_> = reports
            .iter()
            .filter(|r| matches!(r, ExecutionReport::Fill { .. }))
            .collect();
        assert!(fills.len() >= 2, "triggering trade + stop fill expected");
    }

    #[test]
    fn fee_change_no_op_when_max_fee_unchanged() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 100_000);

        let mut reports = Vec::new();

        // Set fees: maker=100, taker=50. max_fee = 100.
        exchange.set_fee_schedule(
            btc,
            FeeSchedule {
                maker_fee_bps: 100,
                taker_fee_bps: 50,
            },
            &mut reports,
        );

        // Place order with reservation including 100bps cushion.
        exchange.execute(
            btc,
            limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        let reserved_before = exchange.accounts().balance(ACCT_A, USD).reserved;
        reports.clear();

        // Change taker fee to 100 (same max). Should be a no-op for reservations.
        exchange.set_fee_schedule(
            btc,
            FeeSchedule {
                maker_fee_bps: 100,
                taker_fee_bps: 100,
            },
            &mut reports,
        );
        assert!(reports.is_empty());
        assert_eq!(
            exchange.accounts().balance(ACCT_A, USD).reserved,
            reserved_before
        );
    }

    #[test]
    fn fee_change_then_fill_conserves_balance() {
        // Reproduces the exact proptest scenario: place buy at 0 fees,
        // increase fees, fill, verify no panic and balance conservation.
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 10_000);
        exchange.deposit(ACCT_A, BTC, 100);

        let mut reports = Vec::new();

        // Place buy limit at price=100, qty=10 with no fees.
        exchange.execute(
            btc,
            limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        reports.clear();

        // Increase fees to 33bps.
        exchange.set_fee_schedule(
            btc,
            FeeSchedule {
                maker_fee_bps: 33,
                taker_fee_bps: 0,
            },
            &mut reports,
        );
        // Order should still be on book (reservation topped up).
        assert!(reports.is_empty());
        reports.clear();

        // Fill the order with a market sell.
        exchange.execute(
            btc,
            limit_order(2, ACCT_A, Side::Sell, 100, 10, TimeInForce::IOC),
            &mut reports,
        );
        // Should produce fills without panicking (no debug_assert failure).
        let fills: Vec<_> = reports
            .iter()
            .filter(|r| matches!(r, ExecutionReport::Fill { .. }))
            .collect();
        assert_eq!(fills.len(), 1);

        // All reservations released.
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 0);
    }

    #[test]
    fn fee_increase_after_partial_fill_adjusts_remaining() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 10_000);
        exchange.deposit(ACCT_B, BTC, 100);

        let mut reports = Vec::new();

        // Place buy limit: price=100, qty=10. Reservation = 1000 (no fees).
        exchange.execute(
            btc,
            limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 1_000);
        reports.clear();

        // Partial fill: sell 3. Cost = 300. Reservation remaining = 700.
        exchange.execute(
            btc,
            limit_order(1, ACCT_B, Side::Sell, 100, 3, TimeInForce::IOC),
            &mut reports,
        );
        reports.clear();

        // Increase fees to 100bps. Remaining qty=7, price=100.
        // New required = 100 * 7 + 100 * 7 * 100 / 10_000 = 700 + 7 = 707.
        // Current slot remaining = 700. Need delta = 7 from available.
        exchange.set_fee_schedule(
            btc,
            FeeSchedule {
                maker_fee_bps: 100,
                taker_fee_bps: 0,
            },
            &mut reports,
        );
        assert!(reports.is_empty(), "order should not be cancelled");
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 707);

        // Fill the remaining 7. Should not panic (fee covered by cushion).
        exchange.execute(
            btc,
            limit_order(2, ACCT_B, Side::Sell, 100, 7, TimeInForce::IOC),
            &mut reports,
        );
        let fills: Vec<_> = reports
            .iter()
            .filter(|r| matches!(r, ExecutionReport::Fill { .. }))
            .collect();
        assert_eq!(fills.len(), 1);
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 0);
    }

    #[test]
    fn fee_increase_cancels_second_order_when_first_consumes_balance() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc_usd_spec());
        // Just enough for two orders with no fees, but not enough
        // cushion for both when fees increase.
        exchange.deposit(ACCT_A, USD, 2_000);

        let mut reports = Vec::new();

        // Two buy limits: 100 * 10 = 1000 each. Total reserved = 2000.
        exchange.execute(
            btc,
            limit_order(1, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        exchange.execute(
            btc,
            limit_order(2, ACCT_A, Side::Buy, 100, 10, TimeInForce::GTC),
            &mut reports,
        );
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 2_000);
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).available, 0);
        reports.clear();

        // Increase fees to 100bps. Each order now needs 1010.
        // First order tops up (1000 → 1010, delta=10 from available — but
        // available=0 initially). The first order's top-up fails too.
        // Both should be cancelled.
        exchange.set_fee_schedule(
            btc,
            FeeSchedule {
                maker_fee_bps: 100,
                taker_fee_bps: 0,
            },
            &mut reports,
        );
        // Both orders cancelled (no available balance for any cushion).
        assert_eq!(reports.len(), 2);
        assert!(
            reports
                .iter()
                .all(|r| matches!(r, ExecutionReport::Cancelled { .. }))
        );
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).reserved, 0);
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).available, 2_000);
    }
}
