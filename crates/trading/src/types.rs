//! Shared trading types: the wire-level data model used by the matching
//! engine, the protocol codec, and the no-op transport binary.
//!
//! Prices use fixed-point integer representation (ticks) to avoid
//! floating-point non-determinism. One tick = smallest price increment.

use std::num::NonZeroU64;

/// Instrument/pair identifier.
///
/// Uses a `u32` rather than a string to avoid heap allocation and enable
/// fast hashing/comparison on the hot path. The mapping from human-readable
/// symbol names to numeric IDs is managed outside the matching engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Symbol(pub u32);

/// Client-assigned order identifier.
///
/// Uses `u64` — fits in a register, supports 18.4 quintillion unique IDs.
/// Assigned by the client, not the server. Must be **monotonically
/// increasing per account** — the exchange tracks a per-account high-water
/// mark and rejects any `OrderId <= max_seen` as a duplicate (see
/// `Exchange::max_order_id`). This prevents double-execution on
/// crash-recovery retry.
///
/// Used as a HashMap key throughout the engine (order_sides, order_index,
/// reservations), so cheap hashing matters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OrderId(pub u64);

/// Account/trader identifier.
///
/// Uses `u32` — same rationale as `Symbol`: no heap allocation, fast
/// hashing. Supports ~4 billion accounts, sufficient for any single exchange.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AccountId(pub u32);

/// Currency identifier (e.g., USD, BTC, ETH).
///
/// Uses `u32` — same pattern as `Symbol` and `AccountId`. The mapping from
/// human-readable currency codes to numeric IDs is managed outside the engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CurrencyId(pub u32);

/// Maps an instrument to its base and quote currencies.
///
/// Example: BTC/USD → base = BTC (what you buy/sell), quote = USD (what you pay with).
/// The account manager uses this to determine which balances to reserve and credit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InstrumentSpec {
    pub symbol: Symbol,
    pub base: CurrencyId,
    pub quote: CurrencyId,
}

/// Per-instrument risk limits for fat finger checks. Checked in
/// `Exchange::execute()` before balance reservation and matching.
///
/// `Option` fields: `None` means "no limit" (unconfigured instruments
/// pass all checks). Both fields use `Copy`-friendly types for zero-cost
/// hot-path access.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RiskLimits {
    /// Maximum order quantity (in lots). Rejects orders where
    /// `quantity > max_order_qty`.
    pub max_order_qty: Option<Quantity>,
    /// Maximum order notional value (price × quantity, in ticks).
    /// Uses `u64` for the configured ceiling — the actual comparison
    /// uses `u128` to avoid overflow on `price.get() * quantity.get()`.
    /// Applies only to orders with a known price (Limit, StopLimit);
    /// Market and Stop orders skip this check.
    pub max_order_notional: Option<u64>,
}

/// Per-instrument circuit breaker configuration. Checked in
/// `Exchange::execute()` after dedup and before fat finger checks.
///
/// Static price bands reject orders with a limit price outside
/// `[lower, upper]`. The `halted` flag rejects all new orders.
/// `Copy`-friendly for zero-cost hot-path access.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CircuitBreakerConfig {
    /// Inclusive lower bound for limit order prices. `None` = no lower bound.
    pub price_band_lower: Option<Price>,
    /// Inclusive upper bound for limit order prices. `None` = no upper bound.
    pub price_band_upper: Option<Price>,
    /// When true, reject all new orders for this instrument.
    pub halted: bool,
}

/// Per-instrument maker/taker fee schedule.
///
/// Fees are in basis points (1 bp = 0.01%), charged in quote currency
/// (cost-based): `fee = price * quantity * bps / 10_000`. The buyer's
/// fee is deducted from their reservation; the seller's fee is deducted
/// from their proceeds.
///
/// Negative values represent rebates — the exchange pays the trader.
/// Example: `maker_fee_bps = -10, taker_fee_bps = 20` means the maker
/// receives a 0.10% rebate while the taker pays 0.20%.
///
/// Uses `i16` to support the range -10000..10000, covering both fees
/// and rebates within basis-point precision.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FeeSchedule {
    /// Maker fee in basis points (-10000..10000). Negative = rebate.
    pub maker_fee_bps: i16,
    /// Taker fee in basis points (-10000..10000). Negative = rebate.
    pub taker_fee_bps: i16,
}

/// Price in ticks (fixed-point). A tick is the smallest price increment
/// for a given instrument.
///
/// Uses `NonZeroU64` rather than `u128` because: u64 supports prices up to
/// 18.4 quintillion ticks (sufficient for any real-world instrument), fits in
/// a single register, and keeps structs cache-line friendly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Price(pub NonZeroU64);

/// Quantity in lots.
///
/// Uses `NonZeroU64` because zero-quantity orders are invalid by definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Quantity(pub NonZeroU64);

impl Quantity {
    pub fn get(self) -> u64 {
        self.0.get()
    }

    /// Returns the remaining quantity after subtracting, or `None` if fully filled.
    pub fn checked_sub(self, other: Quantity) -> Option<Quantity> {
        self.0
            .get()
            .checked_sub(other.0.get())
            .and_then(NonZeroU64::new)
            .map(Quantity)
    }

    pub fn min(self, other: Quantity) -> Quantity {
        Quantity(self.0.min(other.0))
    }
}

impl Price {
    pub fn get(self) -> u64 {
        self.0.get()
    }
}

/// Instrument lifecycle status, managed by operator commands.
///
/// `#[repr(u8)]` for stable wire encoding (1 byte in snapshot/protocol).
/// Three-state lifecycle: Enabled (normal trading) → Disabled (no new orders,
/// all resting orders cancelled) → Removed (slot freed for reuse).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum InstrumentStatus {
    Enabled = 0,
    Disabled = 1,
    Removed = 2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderType {
    /// Execute immediately at the best available price.
    Market,
    /// Execute at the specified price or better.
    /// When `post_only` is true, the order is rejected if it would
    /// immediately match (cross the spread) — guarantees maker-only execution.
    Limit { price: Price, post_only: bool },
    /// Becomes a market order when the last trade price reaches the trigger.
    /// Stop buy triggers when price >= trigger; stop sell when price <= trigger.
    Stop { trigger_price: Price },
    /// Becomes a limit order when the last trade price reaches the trigger.
    StopLimit {
        trigger_price: Price,
        limit_price: Price,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeInForce {
    /// Good-Til-Cancelled: remains on the book until filled or cancelled.
    GTC,
    /// Immediate-Or-Cancel: fill what you can, cancel the rest.
    IOC,
    /// Fill-Or-Kill: fill entirely or cancel entirely.
    FOK,
    /// Day: rests on the book like GTC, but automatically cancelled when
    /// an `EndOfDay` event is processed.
    Day,
    /// Good-Till-Date: rests on the book until the specified expiry time,
    /// then automatically cancelled by the engine's scheduler when a `Tick`
    /// event arrives with `now_ns >= expiry_ns`.
    GTD,
}

/// Self-trade prevention mode, set per order.
///
/// Determines behavior when an incoming (taker) order would match against
/// a resting (maker) order from the same account.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SelfTradeProtection {
    /// Self-trades are allowed — no prevention.
    Allow,
    /// Cancel the incoming taker order's remaining quantity.
    /// The resting maker order stays on the book.
    #[default]
    CancelNewest,
    /// Cancel the resting maker order and continue matching the taker
    /// against remaining orders.
    CancelOldest,
    /// Cancel both the resting maker and the incoming taker's remaining quantity.
    CancelBoth,
}

/// An incoming order request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Order {
    pub id: OrderId,
    pub account: AccountId,
    pub side: Side,
    pub order_type: OrderType,
    pub time_in_force: TimeInForce,
    pub quantity: Quantity,
    /// Self-trade prevention mode.
    pub stp: SelfTradeProtection,
    /// Expiry time in nanoseconds since Unix epoch. Only meaningful when
    /// `time_in_force` is `GTD`. Zero for all other TIF variants.
    /// Compared against the `now_ns` of `Tick` events to drive scheduler
    /// cancellation.
    pub expiry_ns: u64,
}

/// Events emitted by the matching engine's hot path (order placement,
/// fills, cancels, etc.).
///
/// Kept small so the per-event scratch `Vec<ExecutionReport>` stays
/// cache-friendly. Query responses (`Stats`, `Position`) live in
/// [`QueryResponse`] and bypass the scratch vec entirely — they are
/// returned directly from `Application::apply` and written to the
/// output ring as `OutputPayload::QueryResponse`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionReport {
    /// Order was placed on the book (resting).
    Placed {
        order_id: OrderId,
        symbol: Symbol,
        account: AccountId,
        side: Side,
        price: Price,
        quantity: Quantity,
    },
    /// A trade occurred between two orders.
    Fill {
        maker_order_id: OrderId,
        taker_order_id: OrderId,
        symbol: Symbol,
        maker_account: AccountId,
        taker_account: AccountId,
        price: Price,
        quantity: Quantity,
        /// Fee charged to the maker in quote currency. Positive = fee
        /// deducted from proceeds, negative = rebate credited to the maker.
        maker_fee: i64,
        /// Fee charged to the taker in quote currency. Positive = fee
        /// deducted from proceeds, negative = rebate credited to the taker.
        taker_fee: i64,
    },
    /// Order was cancelled (or remainder cancelled for IOC).
    Cancelled {
        order_id: OrderId,
        symbol: Symbol,
        account: AccountId,
        remaining_quantity: Quantity,
    },
    /// A stop order was triggered by a trade at the given price.
    Triggered {
        order_id: OrderId,
        symbol: Symbol,
        account: AccountId,
        trigger_price: Price,
    },
    /// Order was rejected (e.g., market order on empty book, FOK can't fill).
    Rejected {
        order_id: OrderId,
        symbol: Symbol,
        account: AccountId,
        reason: RejectReason,
    },
    /// Order was amended via cancel-replace. Emitted on success.
    Replaced {
        order_id: OrderId,
        symbol: Symbol,
        account: AccountId,
        side: Side,
        old_price: Price,
        new_price: Price,
        old_remaining: Quantity,
        new_remaining: Quantity,
    },
    /// Instrument lifecycle status changed (disabled, enabled, or removed).
    InstrumentStatusChanged {
        symbol: Symbol,
        status: InstrumentStatus,
    },
}

/// 1:1 query responses returned directly from `Application::apply`,
/// bypassing the fan-out scratch vec. Routed through
/// `OutputPayload::QueryResponse` so the response stage can translate
/// them to the public wire format.
///
/// Kept separate from `ExecutionReport` to avoid inflating that enum's
/// size with the large `Position` balance array (392 B vs ~64 B).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
pub enum QueryResponse {
    /// Transport stats snapshot emitted in response to a `QueryStats`
    /// event. Internal — never journaled, never sent on the wire
    /// directly. The response stage translates this to
    /// `ResponseKind::StatsHeader` for the client.
    Stats {
        active_connections: u64,
        events_processed: u64,
        journal_sequence: u64,
    },
    /// Account balance snapshot emitted in response to `QueryPosition`.
    /// Internal — translated to `ResponseKind::PositionSnapshot` on the
    /// wire. Fixed array sized for the maximum number of currencies per
    /// account; `count` reports how many entries are populated.
    Position {
        account: AccountId,
        balances: [(CurrencyId, u64, u64); 16],
        count: u8,
    },
    /// Per-key request_seq HWM snapshot emitted in response to
    /// `QueryRequestSeq`. The engine returns the value its dedup gate
    /// has currently advanced to for the calling connection's key;
    /// reconnecting clients should set their next outbound seq to
    /// `hwm + 1` so subsequent requests bypass dedup. `0` for a key
    /// that has never authenticated before.
    RequestSeqHwm { hwm: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectReason {
    /// Market order with no liquidity on the opposite side.
    NoLiquidity,
    /// FOK order cannot be fully filled.
    FOKCannotFill,
    /// Account does not have sufficient available balance.
    InsufficientBalance,
    /// The account is not registered.
    UnknownAccount,
    /// The instrument is not registered.
    UnknownSymbol,
    /// Self-trade prevention triggered — order would match against
    /// the same account.
    SelfTradePrevented,
    /// Duplicate order ID — an order with this ID (or a higher one) was
    /// already submitted by this account. Prevents double-execution on
    /// crash-recovery retry.
    DuplicateOrderId,
    /// Order quantity exceeds the instrument's configured maximum.
    ExceedsMaxOrderQty,
    /// Order notional (price × quantity) exceeds the instrument's
    /// configured maximum.
    ExceedsMaxNotional,
    /// Trading is halted for this instrument (circuit breaker).
    TradingHalted,
    /// Order price is outside the instrument's configured price bands.
    OutsidePriceBand,
    /// Cancel-replace target order not found on the book.
    UnknownOrder,
    /// Cancel-replace new price would cross the opposite best price.
    /// Cancel and submit a new order to aggress.
    PriceWouldCross,
    /// Post-only order would immediately match against resting liquidity.
    PostOnlyWouldCross,
    /// Withdrawal rejected because the account has resting orders.
    /// Must CancelAll first.
    HasRestingOrders,
    /// Duplicate request — a request with this sequence number (or higher)
    /// was already processed for this authentication key. Prevents
    /// double-execution on retry after network failure.
    DuplicateRequest,
    /// Replication is enabled but the replica is disconnected. All
    /// state-mutating operations are rejected until the replica reconnects
    /// to preserve the durability guarantee.
    ReplicaDisconnected,
    /// GTD order with expiry_ns == 0 (missing expiry), or non-GTD order
    /// with expiry_ns != 0 (unexpected expiry).
    InvalidExpiry,
    /// Instrument is disabled — no new orders or amendments accepted.
    InstrumentDisabled,
    /// Account already has the maximum number of open orders (resting
    /// limits plus pending stops, across all instruments). Configured by
    /// the operator to bound order_index growth (SEC-03). Cancel an
    /// existing order before placing a new one.
    ExceedsMaxOpenOrders,
    /// Account has exceeded its order-submission rate limit (token
    /// bucket: sustained orders/sec + burst). Configured by the operator
    /// to prevent a single client from monopolizing the matching stage
    /// (SEC-04). Slow submission rate or wait for the bucket to refill.
    ExceedsOrderRate,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to create a Quantity in tests.
    fn qty(n: u64) -> Quantity {
        Quantity(NonZeroU64::new(n).unwrap())
    }

    #[test]
    fn quantity_checked_sub_partial() {
        assert_eq!(qty(10).checked_sub(qty(3)), Some(qty(7)));
    }

    #[test]
    fn quantity_checked_sub_exact_returns_none() {
        // Exact fill returns None (not zero), since Quantity wraps NonZeroU64.
        assert_eq!(qty(10).checked_sub(qty(10)), None);
    }

    #[test]
    fn quantity_checked_sub_overflow_returns_none() {
        assert_eq!(qty(3).checked_sub(qty(10)), None);
    }

    #[test]
    fn niche_optimization() {
        // Option<Price/Quantity> must be the same size as the inner type
        // thanks to NonZeroU64 — this is a design invariant we rely on.
        assert_eq!(
            std::mem::size_of::<Option<Price>>(),
            std::mem::size_of::<Price>()
        );
        assert_eq!(
            std::mem::size_of::<Option<Quantity>>(),
            std::mem::size_of::<Quantity>()
        );
    }
}
