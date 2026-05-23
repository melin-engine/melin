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
    AccountId, CircuitBreakerConfig, CurrencyId, ExecutionReport, FeeSchedule, FxHashSet, HashMap,
    HashMap4, InstrumentSpec, InstrumentStatus, Order, OrderId, OrderType, Price, Quantity,
    RejectReason, ReservationSlot, RiskLimits, Side, Symbol, TimeInForce,
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

/// Compute the required reservation for a buy-side order at a known
/// price: `price * qty` in quote currency. Pure notional — fees are
/// settled from the fill's received asset (industry-standard model),
/// not from the reservation. Uses u128 to avoid overflow.
#[inline]
fn required_notional(price: u64, qty: u64) -> u64 {
    let cost = price as u128 * qty as u128;
    // Saturate to u64::MAX — identical to try_reserve behavior.
    cost.min(u64::MAX as u128) as u64
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

/// Per-account token-bucket state for the order-submission rate limiter.
///
/// Sized to one cache line for the inevitable cache miss on first lookup
/// (16 B = `tokens`(8) + `last_refill_ns`(8); the rest of the line is
/// shared with the next entry in the HashMap4 bucket).
#[derive(Debug, Clone, Copy)]
struct TokenBucket {
    /// Available tokens. Decremented by 1 on every accepted order. Refilled
    /// up to `max_orders_burst` based on `now_ns - last_refill_ns`. `u64`
    /// rather than `u32` so the bucket capacity check is a single 64-bit
    /// compare against the configured burst (which fits in `u32` but is
    /// widened on read).
    tokens: u64,
    /// Wall-clock-equivalent timestamp (event `ts_ns`) of the last refill.
    /// Advanced by exactly the time consumed by tokens added during the
    /// last `refill_and_consume` call so that fractional time below one
    /// token is preserved across calls — e.g. at 1000 ord/s, two calls
    /// 600 µs and then 600 µs apart correctly issue exactly one token
    /// (not zero, not two).
    last_refill_ns: u64,
}

impl TokenBucket {
    /// Initialize a fresh bucket: full tokens, refill clock anchored at
    /// the current event time. First-touch sees a full burst — same shape
    /// as a real-world reservation system (you don't penalise an account
    /// for being newly active).
    #[inline]
    fn new(burst: u32, now_ns: u64) -> Self {
        Self {
            tokens: burst as u64,
            last_refill_ns: now_ns,
        }
    }

    /// Refill the bucket based on elapsed time, but do not consume.
    /// Used both by [`Self::refill_and_consume`] (the order-submission
    /// path) and by the bucket-eviction probe in
    /// [`Exchange::try_evict_bucket`], which needs to know whether a
    /// quiet account has converged back to full capacity without
    /// charging a token for the privilege.
    ///
    /// Integer math only — no floats — for cross-platform determinism.
    /// The refill formula is `earned = elapsed_ns * rate / 1e9`. Two cases:
    ///
    /// 1. The new token count caps at `burst` (bucket overflows). Any
    ///    elapsed time beyond the point at which the bucket reached
    ///    `burst` is "wasted" — there's no headroom to absorb new
    ///    tokens — so `last_refill_ns` is snapped to `now_ns` to
    ///    discard that idle slack. Without this snap, the wasted time
    ///    would accumulate as phantom credit on `last_refill_ns`,
    ///    letting subsequent close-spaced events draw the burst again
    ///    (issuing far more tokens than `rate` supports).
    /// 2. The new token count stays below `burst`. `last_refill_ns` is
    ///    advanced by exactly the time corresponding to tokens earned
    ///    (`earned * 1e9 / rate`) so sub-token fractional time
    ///    accumulates across calls — a 1000 ord/s bucket polled twice
    ///    600 µs apart correctly issues exactly one token, not zero.
    #[inline]
    fn refill(&mut self, now_ns: u64, rate: u32, burst: u32) {
        // Defensive cap: a tampered snapshot, a primary/replica `--max-orders-burst`
        // mismatch, or any future bug that produces `tokens > burst` would otherwise
        // grant unbounded credit on the next event (the `now_ns > last_refill_ns`
        // branch below can leave `tokens` untouched). Clamping at the point of use
        // keeps the bucket invariant `tokens <= burst` independent of how the state
        // was loaded. One cmp on the hot path.
        if self.tokens > burst as u64 {
            self.tokens = burst as u64;
        }
        // Clock can only go forward in our timeline (event ts_ns is
        // assigned by the reader at ingest and journaled). If we ever
        // see now_ns < last_refill_ns it means the operator changed the
        // clock or there is a bug upstream — be defensive: don't panic,
        // skip the refill (`refill_and_consume` will still allow consume
        // so we don't reject every order until time catches up). Locked
        // in by `rate_limit_clock_backwards_is_defensive_not_panic`.
        if now_ns > self.last_refill_ns {
            let elapsed = now_ns - self.last_refill_ns;
            // saturating_mul instead of u128: at u32::MAX rate × ~4.3e9
            // elapsed ns the product overflows u64. On overflow we cap at
            // u64::MAX which, divided by 1e9, still exceeds any u32 burst,
            // so the .min(burst) below yields the same result as the
            // u128 form (saturation absorbs the overflow case). The u64
            // form lets the compiler emit a magic-number multiply for
            // the constant-1e9 divide and a single `div` for /rate, vs
            // the ~50ns __udivti3 library call per event the u128 form
            // emitted on the matching hot path (perf: ~2.6% of total
            // CPU).
            let earned = (elapsed.saturating_mul(rate as u64) / 1_000_000_000).min(burst as u64);
            let new_tokens = (self.tokens + earned).min(burst as u64);
            if new_tokens >= burst as u64 {
                // Bucket is at capacity — discard any remaining elapsed
                // time so phantom credit can't accumulate. See doc above.
                self.last_refill_ns = now_ns;
            } else if earned > 0 {
                // Below cap and we earned tokens — advance by exactly the
                // time those tokens consumed, preserving fractional-token
                // time below one token's worth. We reach this branch only
                // when new_tokens < burst, so earned < burst ≤ u32::MAX,
                // and earned × 1e9 < 4.3e18 fits in u64 with room to spare.
                let consumed_ns = (earned * 1_000_000_000) / rate as u64;
                self.last_refill_ns += consumed_ns;
            }
            // else: earned == 0 (sub-token time elapsed, bucket below
            // cap) — leave last_refill_ns unchanged so the fractional
            // time accumulates into the next call.
            self.tokens = new_tokens;
        }
    }

    /// Refill the bucket based on elapsed time, then attempt to consume
    /// one token. Returns `true` if the order is allowed.
    #[inline]
    fn refill_and_consume(&mut self, now_ns: u64, rate: u32, burst: u32) -> bool {
        self.refill(now_ns, rate, burst);
        if self.tokens > 0 {
            self.tokens -= 1;
            true
        } else {
            false
        }
    }
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

    /// Snapshot per-key request sequence HWMs for serialization.
    pub fn snapshot_key_hwm(&self) -> Vec<(u64, u64)> {
        self.key_hwm
            .iter()
            .filter(|(_, hwm)| **hwm > 0)
            .map(|(&key, &hwm)| (key, hwm))
            .collect()
    }

    /// Snapshot per-account rate-limiter bucket state for serialization
    /// (SEC-04 v18+ snapshots). Each tuple is `(account, tokens,
    /// last_refill_ns)`. Returned in unspecified order — callers must
    /// not depend on stability across runs.
    ///
    /// Without this, a replica that restored from a snapshot taken at
    /// time T while the primary's bucket for some account A was
    /// partially depleted would re-initialise A's bucket lazily as full
    /// at the next event, while the primary kept the depleted state —
    /// the divergence window flagged in the SEC-04 audit. Closing it
    /// requires carrying the bucket map in the snapshot.
    pub(crate) fn snapshot_order_buckets(&self) -> Vec<(AccountId, u64, u64)> {
        self.order_buckets
            .iter()
            .map(|(&account, bucket)| (account, bucket.tokens, bucket.last_refill_ns))
            .collect()
    }

    /// Repopulate the rate-limiter bucket map from a deserialised
    /// snapshot. Called by `restore_state` after the rest of the engine
    /// is reconstructed. Existing entries are cleared first so a stale
    /// in-process bucket cannot survive a restore.
    ///
    /// Preserving exact bucket state (`tokens` + `last_refill_ns`) is
    /// what closes the SEC-04 snapshot-divergence window: the next
    /// event's `refill_and_consume` call will see the same elapsed-time
    /// math the primary would have, producing identical accept/reject
    /// decisions.
    pub(crate) fn restore_order_buckets(&mut self, buckets: Vec<(AccountId, u64, u64)>) {
        self.order_buckets.clear();
        for (account, tokens, last_refill_ns) in buckets {
            self.order_buckets.insert(
                account,
                TokenBucket {
                    tokens,
                    last_refill_ns,
                },
            );
        }
    }

    /// Number of active instruments (for diagnostics).
    pub fn instrument_count(&self) -> usize {
        self.instruments.iter().filter(|s| s.is_some()).count()
    }

    /// Iterate over instrument specs (for snapshot serialization).
    pub fn instrument_specs(&self) -> impl Iterator<Item = &InstrumentSpec> {
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
    pub fn snapshot_order_sides(&self) -> Vec<((AccountId, OrderId), Side)> {
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
    pub fn snapshot_risk_limits(&self) -> Vec<(Symbol, RiskLimits)> {
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

        // Under the received-asset fee model, reservations are pure
        // notional and don't depend on the schedule — a fee change
        // simply takes effect on subsequent fills, with no need to
        // re-reserve resting orders or recompute stop-market budgets.
        // `reports` is unused here but kept in the signature so callers
        // (and journal replay) don't need to branch on the return shape.
        let _ = reports;
        inst.fee_schedule = schedule;
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
    pub fn snapshot_circuit_breakers(&self) -> Vec<(Symbol, CircuitBreakerConfig)> {
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

    /// Submit an order to the matching engine for the given instrument.
    ///
    /// Validates the instrument exists, reserves funds, then executes.
    /// On fill, balances are updated. On reject/cancel, reserves are released.
    ///
    /// Under `feature = "skip-order-exec"` the body is short-circuited
    /// to a single `Rejected{NoLiquidity}` push, used by the server's
    /// transport-only benchmark build to isolate transport throughput
    /// from matching cost. Same wire shape — bench clients still see
    /// one response per `SubmitOrder` — but no order book / account
    /// state touched.
    #[inline]
    pub fn execute(&mut self, symbol: Symbol, order: Order, reports: &mut Vec<ExecutionReport>) {
        #[cfg(feature = "skip-order-exec")]
        {
            reports.push(ExecutionReport::Rejected {
                order_id: order.id,
                symbol,
                account: order.account,
                reason: RejectReason::NoLiquidity,
            });
            return;
        }
        #[cfg_attr(feature = "skip-order-exec", allow(unreachable_code))]
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
        if self.live_order_ids.contains(&(order.account, order.id)) {
            reports.push(ExecutionReport::Rejected {
                order_id: order.id,
                symbol,
                account: order.account,
                reason: RejectReason::DuplicateOrderId,
            });
            return;
        }

        // Existence already established by the `let Some(inst) = inst_ref(...)
        // else { ... return; }` guard at the top of `execute` (~line 1017).
        // The matcher is single-threaded and no instrument deregistration
        // runs between events, so the slot is still populated here.
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

        // Per-account open-order cap (SEC-03). Runs after every other
        // reject reason (UnknownSymbol, InstrumentDisabled, DuplicateOrderId,
        // TradingHalted, OutsidePriceBand, ExceedsMaxOrderQty,
        // ExceedsMaxNotional, InvalidExpiry) so an order that would have
        // been rejected for a venue-side or order-shape reason still
        // reports that reason — the cap is account-state, akin to
        // InsufficientBalance, and belongs adjacent to reservation.
        // Order: cap before reservation so a capped account doesn't churn
        // the slab. `order_counts` tracks (resting + pending stops +
        // in-flight) per account; `>=` rejects when accepting this order
        // would push the count past the limit. `0` = unlimited (opt-out).
        if self.max_open_orders_per_account > 0
            && self.order_counts.get(&order.account).copied().unwrap_or(0)
                >= self.max_open_orders_per_account
        {
            reports.push(ExecutionReport::Rejected {
                order_id: order.id,
                symbol,
                account: order.account,
                reason: RejectReason::ExceedsMaxOpenOrders,
            });
            return;
        }

        // Per-account order-submission rate limit (SEC-04). Token bucket
        // refilled at `max_orders_per_second`, capped at `max_orders_burst`,
        // metered against the journaled event timestamp
        // (`current_event_ts_ns`) so primary and replicas see identical
        // accept/reject decisions. Sits next to the open-orders cap above
        // because both are per-account policy gates that take effect
        // *before* any reservation work — a throttled order should not
        // perturb the slab or `order_counts`. Disabled when either knob
        // is `0`.
        if self.max_orders_per_second > 0 && self.max_orders_burst > 0 {
            let now_ns = self.current_event_ts_ns;
            let rate = self.max_orders_per_second;
            let burst = self.max_orders_burst;
            let bucket = self
                .order_buckets
                .entry(order.account)
                .or_insert_with(|| TokenBucket::new(burst, now_ns));
            if !bucket.refill_and_consume(now_ns, rate, burst) {
                reports.push(ExecutionReport::Rejected {
                    order_id: order.id,
                    symbol,
                    account: order.account,
                    reason: RejectReason::ExceedsOrderRate,
                });
                return;
            }
        }

        // Reserve pure notional (no fee cushion). Fees are settled from
        // the fill's received asset, not from this reservation, so a
        // schedule change after placement can never make the reservation
        // insufficient — by construction.
        let (reserved, slot) = match self.accounts.try_reserve(&order, &spec) {
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

        // For buy-side market/stop-market orders, pass a cost budget so
        // the matching engine stops before exceeding the reservation. The
        // budget is exactly the reservation amount — no fee carve-out
        // needed since fees come out of the buyer's base credit, not the
        // quote reservation.
        let quote_budget = match (order.side, order.order_type) {
            (Side::Buy, OrderType::Market) | (Side::Buy, OrderType::Stop { .. }) => Some(reserved),
            _ => None,
        };

        *self.order_counts.entry(order.account).or_default() += 1;
        // Tentatively claim the (account, order_id) slot for the live
        // dedup check. If the order closes within this `execute` call
        // (IOC/FOK fill, FOK kill, etc.) the entry is freed in the
        // `freed` loop below; if it rests, the entry stays put.
        self.live_order_ids.insert((order.account, order.id));

        let taker_account = order.account;
        let taker_id = order.id;
        let report_start = reports.len();

        // Take scratch buffers out of `self` BEFORE the `inst_mut` borrow
        // below. `inst` mutably borrows `self.instruments` for the rest
        // of the function, so we can't touch `self.scratch_*` once it's
        // live. `mem::take` swaps with an empty Vec (no allocation —
        // `Vec::new()` is const) and the populated buffer is restored
        // at the end. Net effect: the inner loop has the same shape as
        // before but no per-event Vec allocation.
        //
        // The leading `clear()` calls are belt-and-braces: the put-back
        // at function end leaves the field empty, so under normal
        // control flow the take yields an already-empty Vec. The clear
        // only does work if a previous `execute` panicked between take
        // and put-back, leaving stale entries in the scratch.
        let mut consumed = std::mem::take(&mut self.scratch_consumed);
        consumed.clear();
        let mut freed = std::mem::take(&mut self.scratch_freed);
        freed.clear();

        // Single mutable lookup: book, fees all from the same struct.
        // Existence was established by the `inst_ref` guard at the top of
        // `execute` (line ~1017); same single-threaded invariant as the
        // earlier re-lookup applies.
        let inst =
            inst_mut(&mut self.instruments, symbol).expect("instrument verified to exist above");
        let taker_rested = inst.book.execute(order, quote_budget, slot, reports);

        // Capture the fee schedule for use inside the loop (we need
        // `maker_side` to attribute maker_fee/taker_fee to base vs quote
        // legs, so fees must be computed alongside the maker/taker slot
        // lookup rather than in a separate pre-pass).
        let fee_schedule = inst.fee_schedule;

        // Process reports to update balances. Mirrors the old process_reports
        // logic but resolves slots from the book instead of a separate HashMap.
        //
        // consumed_slots: fully-filled or STP-cancelled makers, with their
        // reservation slots. Typically 0-5 entries per aggressive order.
        consumed.extend(inst.book.drain_consumed_slots());

        for report in &mut reports[report_start..] {
            match report {
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
                    // Dereference for clarity; the `&mut` references are
                    // used only to write maker_fee/taker_fee below.
                    let maker_order_id = *maker_order_id;
                    let taker_order_id = *taker_order_id;
                    let maker_account = *maker_account;
                    let fill_taker_account = *fill_taker_account;
                    let price = *price;
                    let quantity = *quantity;
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

                    // Compute fees from the schedule. The wire-format
                    // report carries fees in **quote currency** (cost-based)
                    // for both legs — that's the economic value of the
                    // fee, stable across A's received-asset settlement.
                    // Internally, fill() takes the buyer fee in base
                    // units and the seller fee in quote units (each
                    // deducted from that side's received asset).
                    let cost_i128 = price.get() as i128 * quantity.get() as i128;
                    let qty_i128 = quantity.get() as i128;
                    let (buyer_slot, seller_slot, buyer_fee_bps, seller_fee_bps) = match maker_side
                    {
                        Side::Buy => (
                            maker_slot,
                            taker_slot,
                            fee_schedule.maker_fee_bps,
                            fee_schedule.taker_fee_bps,
                        ),
                        Side::Sell => (
                            taker_slot,
                            maker_slot,
                            fee_schedule.taker_fee_bps,
                            fee_schedule.maker_fee_bps,
                        ),
                    };
                    let buyer_quote_fee_report =
                        (cost_i128 * buyer_fee_bps as i128 / 10_000) as i64;
                    let seller_quote_fee = (cost_i128 * seller_fee_bps as i128 / 10_000) as i64;
                    let buyer_base_fee = (qty_i128 * buyer_fee_bps as i128 / 10_000) as i64;
                    // Update the report fields (quote-denominated).
                    match maker_side {
                        Side::Buy => {
                            *maker_fee = buyer_quote_fee_report;
                            *taker_fee = seller_quote_fee;
                        }
                        Side::Sell => {
                            *maker_fee = seller_quote_fee;
                            *taker_fee = buyer_quote_fee_report;
                        }
                    }
                    self.accounts.fill(
                        buyer_slot,
                        seller_slot,
                        price,
                        quantity,
                        buyer_base_fee,
                        seller_quote_fee,
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
                    let order_id = *order_id;
                    let account = *account;
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
                    let order_id = *order_id;
                    let account = *account;
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
            self.live_order_ids.remove(&(account, order_id));
            self.release_open_order(account);
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

        // Clear before restoring so the next call starts from an empty
        // Vec; capacity is retained. (`consumed` is iterated by reference
        // in the loop above and may still hold entries; `freed` is also
        // by-reference in its loop. Neither is drained as a side effect.)
        consumed.clear();
        freed.clear();
        self.scratch_consumed = consumed;
        self.scratch_freed = freed;
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

        // 5. Adjust reservation atomically. Compute the new required
        // amount as pure notional. If insufficient balance, the original
        // reservation stays intact.
        let new_required = match side {
            Side::Buy => required_notional(new_price.get(), new_quantity.get()),
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

impl Default for Exchange {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[path = "exchange_tests.rs"]
mod tests;
