//! Exchange: dispatches orders to per-instrument order books.
//!
//! All order books run on a single thread (LMAX-style). This keeps event
//! ordering deterministic and allows portfolio-wide risk checks (margin,
//! exposure limits) without cross-thread coordination.
//!
//! If throughput exceeds a single core, shard by instrument — each shard
//! stays single-threaded. Note: portfolio risk checks then require
//! cross-shard message passing, adding latency and complexity.

mod cancel_replace;
mod execute;
mod instrument;
mod snapshot_methods;
mod token_bucket;

use self::instrument::inst_mut;
use self::token_bucket::TokenBucket;
// Re-exported so existing crate paths (`crate::exchange::InstrumentState`,
// used by `crate::snapshot`) keep working after the move.
pub(crate) use self::instrument::InstrumentState;
use crate::account::AccountManager;
use crate::orderbook::OrderBook;
use crate::scheduler::{ScheduledTask, ScheduledTaskHeap, ScheduledTaskKind};
use crate::types::{
    AccountId, CircuitBreakerConfig, CurrencyId, ExecutionReport, FeeSchedule, FxHashSet, HashMap,
    HashMap4, InstrumentSpec, InstrumentStatus, OrderId, RejectReason, ReservationSlot, RiskLimits,
    Side, Symbol,
};

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
    /// Open-addressing (hashbrown) set rather than the project's
    /// HashMap4 (astenn) — `(AccountId, OrderId)` has unbounded
    /// distinct keys under the bench's churn pattern but bounded live
    /// count, exactly the workload extendible hashing handles poorly
    /// (directory grows with lifetime inserts). Hashbrown's backshift
    /// deletion keeps capacity tracking the live set.
    live_order_ids: FxHashSet<(AccountId, OrderId)>,
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
    /// Pre-allocated empty `OrderBook`s, populated by
    /// [`Self::with_seed_capacity`] and indexed by symbol. When
    /// `add_instrument` runs on the matching thread, it takes the book
    /// from this pool instead of allocating a fresh one — avoiding the
    /// 5–11 ms first-touch + mlock spike that would otherwise show up
    /// during seed (matching thread is mlock-MCL_FUTURE so any new
    /// allocation triggers a per-page lock, faulting thousands of pages
    /// at once). Empty slot in the pool means the book has been taken
    /// or was never pre-allocated for that symbol; `add_instrument`
    /// falls back to a fresh allocation in that case.
    instrument_pool: Vec<Option<OrderBook>>,
    /// When true, new order books are created with generous pre-allocation
    /// to avoid HashMap resize spikes on the hot path.
    presized: bool,
    /// Maximum number of open orders (resting limits + pending stops, across
    /// all instruments) per account. New submissions are rejected with
    /// `ExceedsMaxOpenOrders` once an account reaches this count. `0` means
    /// unlimited (opt-out). Bounds the per-account contribution to the
    /// global `order_index`/`stop_index` and the matching-stage hash maps;
    /// without it an authenticated client can submit unbounded resting
    /// limits at distinct prices and OOM the server (SEC-03).
    ///
    /// `u32` matches the type of the `order_counts` value field.
    ///
    /// Determinism note: the cap shapes Rejected reports, which are
    /// observable state. Primary and every replica must run with the same
    /// value or replay will diverge — the cap is operator config, not a
    /// journaled event, so it is the operator's responsibility to keep it
    /// consistent across the cluster (same shape as `--authorized-keys`).
    max_open_orders_per_account: u32,
    /// Per-account order-submission rate limit (token bucket, SEC-04).
    /// `max_orders_per_second` is the steady-state refill rate;
    /// `max_orders_burst` is the bucket capacity (max consecutive orders
    /// after a quiet period). `0` for either field disables the limiter
    /// (opt-out). Buckets are populated lazily in `order_buckets` on first
    /// submission per account.
    ///
    /// Determinism note: same as the open-orders cap above — Rejected
    /// reports are observable, so primary and every replica must run
    /// with matching values. The bucket math uses the journaled
    /// `ApplyCtx::now_ns` (stamped by the reader at ingest), not wall-
    /// clock, so bit-for-bit replay holds across the cluster.
    ///
    /// Snapshot continuity: per-account bucket state (`tokens` +
    /// `last_refill_ns`) is serialised in snapshot format v18+ via
    /// [`Exchange::snapshot_order_buckets`] /
    /// [`Exchange::restore_order_buckets`]. A replica restoring from a
    /// snapshot taken mid-throttle sees the same bucket state the
    /// primary had, so the very next event produces an identical
    /// accept/reject decision — no divergence window.
    ///
    /// `u32` for both fields: covers the realistic operator range
    /// (1..=10_000_000 orders/sec) without bloating the per-account
    /// `TokenBucket` row.
    max_orders_per_second: u32,
    max_orders_burst: u32,
    /// Per-account token-bucket state for the rate limiter. Lazily inserted
    /// on the first *submission attempt* (not first rest) per account, and
    /// evicted on the close path (`release_open_order`) once the account's
    /// open-order count reaches zero AND the bucket has refilled to full
    /// capacity at the current event time. See [`Exchange::try_evict_bucket`]
    /// for the policy and why "at full capacity" makes the eviction
    /// observationally equivalent to keeping the entry.
    ///
    /// Steady-state size therefore tracks accounts currently inside a
    /// throttle window (or holding open orders), not the cumulative ever-
    /// active account count.
    ///
    /// Empty when `max_orders_per_second == 0` or `max_orders_burst == 0`
    /// (limiter disabled). HashMap4 for the same reason as `order_counts`:
    /// fxhash on a 4-byte key is cheaper than the default hasher on the
    /// hot path.
    order_buckets: HashMap4<AccountId, TokenBucket>,
    /// `now_ns` of the most recently applied event. Stashed by
    /// [`Application::apply`](crate::application_impl) before dispatching
    /// to per-event methods (`execute`, `cancel`, etc.) so the rate limiter
    /// can read a deterministic clock without threading a parameter through
    /// every public method's signature. Initialised to `0` and overwritten
    /// every time `apply` is called — never reset between events; readers
    /// outside `apply` see whatever the last `apply` left here.
    ///
    /// Footgun for direct callers (tests, embedded users): if the rate
    /// limiter is active (`max_orders_per_second > 0 &&
    /// max_orders_burst > 0`) and a caller invokes `Exchange::execute`
    /// without ever calling [`Self::set_current_event_ts_ns`], the
    /// limiter operates against a frozen `now_ns = 0` clock — buckets
    /// don't refill, so each account hits a hard ceiling at its
    /// initial burst. Engine-library users who never activate the rate
    /// limiter (engine-default `max_orders_per_second == 0`) bypass
    /// the limiter entirely and can ignore this. Test code in this
    /// crate uses the `execute_at(exchange, now_ns, …)` helper in the
    /// test module to wrap the stamp + execute pair; embedded users
    /// must call `set_current_event_ts_ns` themselves before each
    /// `execute` (or call through `Application::apply`, which stamps).
    current_event_ts_ns: u64,
    /// Scratch buffer for consumed-slot drain during `execute`. Reused
    /// across calls to eliminate the per-event Vec allocation that
    /// `inst.book.drain_consumed_slots().collect()` would otherwise
    /// perform; the allocator's first-touch on a freshly-mmap'd page
    /// was the dominant source of the engine's deep-tail outliers
    /// (~100µs spikes at p99.99999 under realistic flow on the cherry
    /// EPYC box). Vec for sequential append + iterate; capacity held
    /// across calls via `mem::take` / put-back at the end of `execute`.
    scratch_consumed: Vec<(AccountId, OrderId, Side, ReservationSlot)>,
    /// Scratch buffer for `freed` tracking inside `execute`. Same
    /// rationale as `scratch_consumed`. Vec rather than HashSet because
    /// typical depth is small (0-5 entries) — linear `.contains()` beats
    /// hashing at this size. Caveat for future tuning: at 10M ord/s a
    /// pathological deep-cross (thousands of fills against a single
    /// aggressive market order) would push `freed.contains()` into
    /// O(n²) — switch to a small ahash set or sort+binary-search if
    /// that workload becomes realistic.
    scratch_freed: Vec<(AccountId, OrderId)>,
}

/// Default per-account open-order cap when no operator override is set.
/// Sized for active trading accounts (institutional market-makers running
/// hundreds of resting orders × dozens of instruments) while still
/// bounding worst-case `order_index` growth from a single rogue account.
pub const DEFAULT_MAX_OPEN_ORDERS_PER_ACCOUNT: u32 = 10_000;

/// Default per-account sustained order rate (orders/sec) when no operator
/// override is set. `0` = limiter disabled — engine library users not going
/// through the `melin-server` CLI start unthrottled. The CLI applies its
/// own non-zero default (see `--max-orders-per-second` in `crates/exchange/server`),
/// keeping production deployments protected while leaving in-process tests
/// and embedded users unaffected. Same opt-out shape as the open-orders
/// cap (which was switched to be on-by-default in SEC-03 because it has
/// no time dependency; the rate limiter does, and most non-server harnesses
/// don't advance `now_ns`).
pub const DEFAULT_MAX_ORDERS_PER_SECOND: u32 = 0;
/// Default per-account burst capacity (max consecutive orders after a
/// quiet period). Paired with `DEFAULT_MAX_ORDERS_PER_SECOND = 0`, this is
/// inert at the engine default — the CLI provides the production value.
pub const DEFAULT_MAX_ORDERS_BURST: u32 = 0;

impl Exchange {
    pub fn new() -> Self {
        Self {
            instruments: Vec::new(),
            accounts: AccountManager::new(),
            live_order_ids: FxHashSet::default(),
            order_counts: HashMap4::default(),
            key_hwm: HashMap::default(),
            scheduled_tasks: ScheduledTaskHeap::new(),
            instrument_pool: Vec::new(),
            presized: false,
            max_open_orders_per_account: DEFAULT_MAX_OPEN_ORDERS_PER_ACCOUNT,
            max_orders_per_second: DEFAULT_MAX_ORDERS_PER_SECOND,
            max_orders_burst: DEFAULT_MAX_ORDERS_BURST,
            order_buckets: HashMap4::default(),
            current_event_ts_ns: 0,
            // Match OrderBook::consumed_slots's 64-element pre-alloc so
            // typical fills (0-5 entries) never trigger growth.
            scratch_consumed: Vec::with_capacity(64),
            scratch_freed: Vec::with_capacity(64),
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
            // smaller than the lifetime total. hashbrown-backed: the
            // bounded-live-count + unbounded-distinct-keys workload
            // doesn't grow the table once warmup settles.
            live_order_ids: FxHashSet::with_capacity_and_hasher(1_000_000, Default::default()),
            // 1M accounts × ~32 bytes per entry ≈ 32 MB. Covers the
            // default benchmark (1M accounts) with no hot-path resizes.
            // Pages are faulted during prefault() via insert/clear.
            order_counts: HashMap4::with_capacity_and_hasher(1_000_000, Default::default()),
            key_hwm: HashMap::default(),
            scheduled_tasks: ScheduledTaskHeap::new(),
            instrument_pool: Vec::new(),
            presized: true,
            max_open_orders_per_account: DEFAULT_MAX_OPEN_ORDERS_PER_ACCOUNT,
            max_orders_per_second: DEFAULT_MAX_ORDERS_PER_SECOND,
            max_orders_burst: DEFAULT_MAX_ORDERS_BURST,
            // Same 1M sizing as `order_counts` — bucket count tracks the
            // active-account count.
            order_buckets: HashMap4::with_capacity_and_hasher(1_000_000, Default::default()),
            current_event_ts_ns: 0,
            scratch_consumed: Vec::with_capacity(64),
            scratch_freed: Vec::with_capacity(64),
        }
    }

    /// Create an Exchange pre-sized for a known bulk-seed workload.
    ///
    /// `num_accounts` and `num_instruments` are the seed counts. Each
    /// `ProvisionAccount` event creates 2 balance entries per instrument
    /// (base + quote), so the AccountManager's balance HashMap is sized
    /// to `num_accounts × num_instruments × 2`. Without this, seeding
    /// 100K accounts × 100 instruments hits multi-hundred-ms rehash
    /// stalls as the map grows — visible in T1's seed-phase outlier
    /// log as `matching execute outlier elapsed_us=1146403` near the
    /// end of the seed phase.
    ///
    /// Falls back to [`Self::with_capacity`]'s production defaults for
    /// the other collections (instruments, live_order_ids, order_counts).
    pub fn with_seed_capacity(num_accounts: usize, num_instruments: usize) -> Self {
        let balance_capacity = num_accounts
            .saturating_mul(num_instruments)
            .saturating_mul(2);
        // Pre-allocate one OrderBook per expected instrument, indexed by
        // symbol. AddInstrument on the matching thread pulls from this
        // pool instead of allocating fresh — see the field doc on
        // `instrument_pool`. Symbol convention is `Symbol(0)..Symbol(N-1)`
        // (matches the bench seed); off-pattern symbols fall back to fresh
        // allocation in `add_instrument`.
        let instrument_pool: Vec<Option<OrderBook>> = (0..num_instruments)
            .map(|i| Some(OrderBook::with_capacity(Symbol(i as u32))))
            .collect();
        Self {
            instruments: Vec::with_capacity(num_instruments.max(64)),
            accounts: AccountManager::with_balance_capacity(balance_capacity),
            live_order_ids: FxHashSet::with_capacity_and_hasher(1_000_000, Default::default()),
            order_counts: HashMap4::with_capacity_and_hasher(
                num_accounts.max(1_000_000),
                Default::default(),
            ),
            key_hwm: HashMap::default(),
            scheduled_tasks: ScheduledTaskHeap::new(),
            instrument_pool,
            presized: true,
            max_open_orders_per_account: DEFAULT_MAX_OPEN_ORDERS_PER_ACCOUNT,
            max_orders_per_second: DEFAULT_MAX_ORDERS_PER_SECOND,
            max_orders_burst: DEFAULT_MAX_ORDERS_BURST,
            // Match `order_counts` sizing here too.
            order_buckets: HashMap4::with_capacity_and_hasher(
                num_accounts.max(1_000_000),
                Default::default(),
            ),
            current_event_ts_ns: 0,
            scratch_consumed: Vec::with_capacity(64),
            scratch_freed: Vec::with_capacity(64),
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
        let mut live_order_ids: FxHashSet<(AccountId, OrderId)> = FxHashSet::default();
        for inst in &instruments {
            if let Some(inst) = inst.as_deref() {
                for ((account, order_id), _) in inst.book.active_order_slots() {
                    *order_counts.entry(account).or_default() += 1;
                    live_order_ids.insert((account, order_id));
                }
                for ((account, order_id), _) in inst.book.active_stop_slots() {
                    *order_counts.entry(account).or_default() += 1;
                    live_order_ids.insert((account, order_id));
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
            instrument_pool: Vec::new(),
            presized: false,
            max_open_orders_per_account: DEFAULT_MAX_OPEN_ORDERS_PER_ACCOUNT,
            max_orders_per_second: DEFAULT_MAX_ORDERS_PER_SECOND,
            max_orders_burst: DEFAULT_MAX_ORDERS_BURST,
            // Snapshot restore: limiter starts disabled by default and
            // the bucket map starts empty. `restore_state` calls
            // `restore_order_buckets` after `from_parts` to repopulate
            // from the snapshot's v18+ bucket section, and the server
            // wiring then reapplies the operator config (which, going
            // from disabled `(0, 0)` to active, preserves the restored
            // buckets — see `set_max_orders_per_second`).
            order_buckets: HashMap4::default(),
            current_event_ts_ns: 0,
            scratch_consumed: Vec::with_capacity(64),
            scratch_freed: Vec::with_capacity(64),
        }
    }

    /// Configure the per-account open-order cap (`0` = unlimited). See the
    /// field doc on `max_open_orders_per_account` for semantics and the
    /// primary/replica determinism constraint.
    pub fn set_max_open_orders_per_account(&mut self, max: u32) {
        self.max_open_orders_per_account = max;
    }

    /// Read back the configured per-account open-order cap. Test/admin only.
    pub fn max_open_orders_per_account(&self) -> u32 {
        self.max_open_orders_per_account
    }

    /// Configure the per-account order-submission rate limit (SEC-04).
    /// Argument semantics (active values, `0` = disabled, etc.) live on
    /// the `max_orders_per_second` / `max_orders_burst` field docs above.
    ///
    /// Bucket-clearing rule: existing per-account bucket state is
    /// cleared **only** when transitioning between two active
    /// configurations whose `(rate, burst)` values differ — the online-
    /// reconfig case where tokens credited at the old rate could over-
    /// credit under the new one. All other transitions preserve buckets:
    ///
    /// - **Initial activation** (previous config was `(0, _)` or
    ///   `(_, 0)`, i.e. limiter was disabled): buckets that exist on
    ///   the map can only have come from a snapshot restore via
    ///   `restore_order_buckets`, and that is exactly the state we
    ///   need to preserve to close the SEC-04 divergence window. A
    ///   fresh engine with no restored buckets is unaffected.
    /// - **Deactivation** (new config is `(0, _)` or `(_, 0)`): the
    ///   limiter is off, so bucket contents are unobserved. Keeping
    ///   them is harmless and avoids losing state if the operator
    ///   later re-enables with the same values.
    /// - **No-op reapply** (values unchanged): obviously preserve.
    ///
    /// Determinism: must match across primary and replicas — see the field
    /// docs on `max_orders_per_second` / `max_orders_burst`.
    ///
    /// Side effect (online-reconfig path only): clearing buckets resets
    /// every account to a full burst at next first-touch. Operators
    /// should treat online reconfiguration as a rare, audit-logged
    /// change — frequent re-tuning is effectively a throttle bypass.
    /// Engine library users embedding the matching core should gate the
    /// call behind their own auth path.
    pub fn set_max_orders_per_second(&mut self, rate: u32, burst: u32) {
        let was_active = self.max_orders_per_second > 0 && self.max_orders_burst > 0;
        let will_be_active = rate > 0 && burst > 0;
        let values_differ = rate != self.max_orders_per_second || burst != self.max_orders_burst;
        self.max_orders_per_second = rate;
        self.max_orders_burst = burst;
        if was_active && will_be_active && values_differ {
            // Online reconfig between two active configs — drop stale
            // tokens so the new rate applies uniformly from the next
            // event. Cheap: the limiter cleared accounts will repopulate
            // lazily on their first post-reconfig submission.
            self.order_buckets.clear();
        }
    }

    /// Read back the configured rate limit `(rate_per_sec, burst)`.
    /// Test/admin only.
    pub fn max_orders_per_second(&self) -> (u32, u32) {
        (self.max_orders_per_second, self.max_orders_burst)
    }

    /// Stash the current event's `now_ns` so per-event methods (`execute`,
    /// `cancel`, …) can read a deterministic clock without each method
    /// taking a `now_ns` parameter. Called by `Application::apply` exactly
    /// once per event before dispatch.
    #[inline]
    pub fn set_current_event_ts_ns(&mut self, now_ns: u64) {
        self.current_event_ts_ns = now_ns;
    }

    /// Decrement `account`'s open-order count by one and, if it just
    /// reached zero, drop the `order_counts` entry and try to evict the
    /// rate-limiter bucket. Single chokepoint for the per-event close
    /// paths (cancel, end-of-day, disable, GTD expiry, taker/maker
    /// completion) so the bucket-eviction policy lives in one place.
    /// Bulk close paths (`cancel_all`) decrement by N and call
    /// [`Self::try_evict_bucket`] directly.
    #[inline]
    fn release_open_order(&mut self, account: AccountId) {
        let Some(count) = self.order_counts.get_mut(&account) else {
            return;
        };
        *count = count.saturating_sub(1);
        if *count == 0 {
            self.order_counts.remove(&account);
            self.try_evict_bucket(account);
        }
    }

    /// Drop the rate-limiter bucket for `account` if (and only if) it
    /// has refilled back to full capacity at the current event time.
    ///
    /// Eviction is observationally equivalent to keeping a full bucket:
    /// a fresh bucket created on the next submission is initialised at
    /// burst (`TokenBucket::new`), and `refill` caps at burst, so an
    /// existing bucket at capacity would itself refill to burst on
    /// next access regardless of elapsed time. Buckets below capacity
    /// are *not* evicted — that would let an account escape a partial
    /// throttle by cancelling all its orders and trigger a free fresh
    /// burst on its next submission.
    ///
    /// Memory-bound rationale: without this hook, `order_buckets`
    /// would grow with every account that ever submitted an order
    /// (`order_counts` releases its entry, `order_buckets` does not),
    /// turning the limiter into a slow memory leak proportional to
    /// total ever-active accounts. With it, the steady-state size
    /// tracks accounts currently inside a throttle window, which is
    /// what an operator would expect to pay for.
    ///
    /// Determinism: uses `current_event_ts_ns` (stamped by `apply` and
    /// also by `drain_due_scheduled_tasks`), so the eviction decision
    /// reproduces bit-for-bit on every replica replaying the same
    /// journal — the snapshot's bucket-map shape is part of the
    /// replicated state.
    #[inline]
    fn try_evict_bucket(&mut self, account: AccountId) {
        let burst = self.max_orders_burst;
        let rate = self.max_orders_per_second;
        // Limiter disabled (either knob is zero). Two reasons to skip:
        //   (a) The hot path never inserts into the bucket map when
        //       disabled, so the common case is "map is empty, lookup
        //       wasted."
        //   (b) Deactivation transitions in `set_max_orders_per_second`
        //       *preserve* buckets so the operator can re-enable with
        //       the same values without losing per-account throttle
        //       state. Cleaning up on close-zero would silently erode
        //       that preserved state.
        if rate == 0 || burst == 0 {
            return;
        }
        let now_ns = self.current_event_ts_ns;
        let Some(bucket) = self.order_buckets.get_mut(&account) else {
            return;
        };
        bucket.refill(now_ns, rate, burst);
        if bucket.tokens >= burst as u64 {
            self.order_buckets.remove(&account);
        }
    }

    /// Current count of open orders (resting limits + pending stops +
    /// in-flight) for `account`, across all instruments. Returns `0` if
    /// the account has never traded. Used by proptests and admin queries
    /// to inspect the same counter the SEC-03 cap reads.
    pub fn open_order_count(&self, account: AccountId) -> u32 {
        self.order_counts.get(&account).copied().unwrap_or(0)
    }

    /// Pending count of scheduled tasks (including tombstones). Test-only
    /// helper for asserting heap state.
    #[cfg(test)]
    pub(crate) fn scheduled_task_count(&self) -> usize {
        self.scheduled_tasks.len()
    }

    /// Live rate-limiter bucket count. Used by the server's startup
    /// path to detect a primary↔replica config mismatch after snapshot
    /// restore (non-empty buckets paired with a disabled limiter
    /// indicates the operator forgot to wire the rate-limit config) and
    /// by tests to assert bucket-eviction behaviour.
    pub fn order_bucket_count(&self) -> usize {
        self.order_buckets.len()
    }

    /// Drain every scheduled task whose `fire_ns <= now_ns`. Called at the
    /// head of every event the matching stage processes, so time-driven work
    /// runs in lockstep with the journal. Tombstones — tasks that point to
    /// orders that have already been cancelled or filled — are silently
    /// dropped via the `find_gtd_expiry` lookup.
    pub fn drain_due_scheduled_tasks(&mut self, now_ns: u64, reports: &mut Vec<ExecutionReport>) {
        // `tick` reaches us without going through `Application::apply`,
        // so stamp the event clock here too — the bucket-eviction probe
        // in `release_open_order` reads `current_event_ts_ns` and would
        // otherwise see a stale stamp from the previous `apply` call.
        self.current_event_ts_ns = now_ns;
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
                        self.release_open_order(account);
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

    /// Number of active instruments (for diagnostics).
    pub fn instrument_count(&self) -> usize {
        self.instruments.iter().filter(|s| s.is_some()).count()
    }

    /// Set fat finger risk limits for an instrument. No-op if the
    /// instrument doesn't exist (matches previous behavior).
    pub fn set_risk_limits(&mut self, symbol: Symbol, limits: RiskLimits) {
        if let Some(inst) = inst_mut(&mut self.instruments, symbol) {
            inst.risk_limits = limits;
        }
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

        // Under the received-asset fee model, reservations are pure
        // notional and don't depend on the schedule — a fee change
        // simply takes effect on subsequent fills, with no need to
        // re-reserve resting orders or recompute stop-market budgets.
        // `reports` is unused here but kept in the signature so callers
        // (and journal replay) don't need to branch on the return shape.
        let _ = reports;
        inst.fee_schedule = schedule;
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
                    .insert((AccountId(i), OrderId(i as u64)));
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
            // Take a pre-allocated book from the pool if one is waiting at
            // this symbol's index — see `instrument_pool` field doc. Falls
            // back to fresh allocation otherwise; the matching thread is
            // mlock-MCL_FUTURE so the fresh path can stall for a few ms
            // while pages are locked. The pool path keeps that cost on
            // the main thread at startup.
            let book = self
                .instrument_pool
                .get_mut(idx)
                .and_then(|slot| slot.take())
                .unwrap_or_else(|| {
                    if self.presized {
                        OrderBook::with_capacity(spec.symbol)
                    } else {
                        OrderBook::new(spec.symbol)
                    }
                });
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
                    self.try_evict_bucket(account);
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

            // Collect before iterating so the `inst` mutable borrow is
            // released before we re-borrow `self` via `release_open_order`.
            let consumed: Vec<(AccountId, OrderId, Side, ReservationSlot)> =
                inst.book.drain_consumed_slots().collect();
            for (account, order_id, _, slot) in consumed {
                self.accounts.release(slot);
                self.live_order_ids.remove(&(account, order_id));
                self.release_open_order(account);
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

        // Release reservations — same pattern as end_of_day. Collect
        // before iterating so the `inst` borrow is released before
        // re-borrowing `self` via `release_open_order`.
        let consumed: Vec<(AccountId, OrderId, Side, ReservationSlot)> =
            inst.book.drain_consumed_slots().collect();
        for (account, order_id, _, slot) in consumed {
            self.accounts.release(slot);
            self.live_order_ids.remove(&(account, order_id));
            self.release_open_order(account);
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
            self.release_open_order(account);
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
}

impl Default for Exchange {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod cancel_replace_tests;
#[cfg(test)]
mod circuit_breaker_tests;
#[cfg(test)]
mod gtd_tests;
#[cfg(test)]
mod instrument_lifecycle_tests;
#[cfg(test)]
mod open_order_cap_tests;
#[cfg(test)]
mod stp_tests;
#[cfg(test)]
mod test_helpers;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod token_bucket_tests;
