//! Journal event model.
//!
//! Only input commands are journaled — not execution reports. The matching
//! engine is deterministic, so replaying inputs reproduces outputs identically.
//! This halves journal size and simplifies the format.

use crate::types::{
    AccountId, CircuitBreakerConfig, CurrencyId, InstrumentSpec, Order, OrderId, Price, Quantity,
    RiskLimits, Symbol,
};

/// An input event to be journaled for replay and crash recovery.
///
/// `Copy` because all fields are fixed-size primitives/newtypes (no heap).
/// This allows zero-cost passing on the hot path without clone overhead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalEvent {
    /// Register a new instrument with its currency pair.
    AddInstrument { spec: InstrumentSpec },
    /// Credit funds to an account.
    Deposit {
        account: AccountId,
        currency: CurrencyId,
        amount: u64,
    },
    /// Submit an order for matching.
    SubmitOrder { symbol: Symbol, order: Order },
    /// Cancel a resting or pending stop order.
    CancelOrder { symbol: Symbol, order_id: OrderId },
    /// Set fat finger risk limits for an instrument.
    SetRiskLimits { symbol: Symbol, limits: RiskLimits },
    /// Cancel all resting orders and pending stops for an account
    /// across all instruments (kill switch).
    CancelAll { account: AccountId },
    /// Set circuit breaker configuration for an instrument.
    SetCircuitBreaker {
        symbol: Symbol,
        config: CircuitBreakerConfig,
    },
    /// Atomically amend a resting limit order's price and/or quantity.
    CancelReplace {
        symbol: Symbol,
        order_id: OrderId,
        new_price: Price,
        new_quantity: Quantity,
    },
    /// Query server stats. Not journaled (no state change) — the journal
    /// stage skips this variant. Flows through the pipeline so the matching
    /// stage can read Exchange state without concurrency issues.
    QueryStats,
}
