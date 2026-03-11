//! Core types for the trading engine.
//!
//! Prices use fixed-point integer representation (ticks) to avoid
//! floating-point non-determinism. One tick = smallest price increment.

use std::num::NonZeroU64;

/// Instrument/pair identifier.
///
/// Uses a `u32` rather than a string to avoid heap allocation and enable
/// fast hashing/comparison on the hot path. The mapping from human-readable
/// symbol names to numeric IDs is managed outside the matching engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Symbol(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OrderId(pub u64);

/// Account/trader identifier.
///
/// Uses `u32` — same rationale as `Symbol`: no heap allocation, fast
/// hashing. Supports ~4 billion accounts, sufficient for any single exchange.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
    Limit { price: Price },
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
}

/// Events emitted by the matching engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionReport {
    /// Order was placed on the book (resting).
    Placed {
        order_id: OrderId,
        side: Side,
        price: Price,
        quantity: Quantity,
    },
    /// A trade occurred between two orders.
    Fill {
        maker_order_id: OrderId,
        taker_order_id: OrderId,
        maker_account: AccountId,
        taker_account: AccountId,
        price: Price,
        quantity: Quantity,
    },
    /// Order was cancelled (or remainder cancelled for IOC).
    Cancelled {
        order_id: OrderId,
        remaining_quantity: Quantity,
    },
    /// A stop order was triggered by a trade at the given price.
    Triggered {
        order_id: OrderId,
        trigger_price: Price,
    },
    /// Order was rejected (e.g., market order on empty book, FOK can't fill).
    Rejected {
        order_id: OrderId,
        reason: RejectReason,
    },
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
