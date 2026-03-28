//! Journal event model.
//!
//! Only input commands are journaled — not execution reports. The matching
//! engine is deterministic, so replaying inputs reproduces outputs identically.
//! This halves journal size and simplifies the format.

use crate::types::{
    AccountId, CircuitBreakerConfig, CurrencyId, FeeSchedule, InstrumentSpec, Order, OrderId,
    Price, Quantity, RiskLimits, Symbol,
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
    CancelOrder {
        symbol: Symbol,
        account: AccountId,
        order_id: OrderId,
    },
    /// Set fat finger risk limits for an instrument.
    SetRiskLimits { symbol: Symbol, limits: RiskLimits },
    /// Cancel all resting orders and pending stops for an account
    /// across all instruments (kill switch).
    CancelAll { account: AccountId },
    /// Debit available funds from an account. Rejects if the account has
    /// resting orders (must CancelAll first) or insufficient balance.
    /// Removes the balance entry when it reaches zero (memory cleanup
    /// for the sparse account storage model).
    Withdraw {
        account: AccountId,
        currency: CurrencyId,
        amount: u64,
    },
    /// Set circuit breaker configuration for an instrument.
    SetCircuitBreaker {
        symbol: Symbol,
        config: CircuitBreakerConfig,
    },
    /// Atomically amend a resting limit order's price and/or quantity.
    CancelReplace {
        symbol: Symbol,
        account: AccountId,
        order_id: OrderId,
        new_price: Price,
        new_quantity: Quantity,
    },
    /// Set the fee schedule (maker/taker fees) for an instrument.
    SetFeeSchedule {
        symbol: Symbol,
        schedule: FeeSchedule,
    },
    /// Provision an account with a deposit of `amount` in every currency
    /// of every registered instrument. Used for bulk seeding — one event
    /// replaces O(instruments) individual Deposit events.
    ///
    /// Internal only: not exposed via the wire protocol. Only the server's
    /// startup seeding path emits this event.
    ProvisionAccount { account: AccountId, amount: u64 },
    /// Cancel all resting orders and pending stops with `TimeInForce::Day`
    /// across all instruments. Triggered by an operator at end-of-session.
    EndOfDay,
    /// Expire all resting orders and pending stops with `TimeInForce::GTD`
    /// whose `expiry_ns` <= `timestamp_ns`. Triggered by an operator.
    ExpireOrders { timestamp_ns: u64 },
    /// Disable an instrument: reject new orders and cancel all resting
    /// orders and pending stops. Re-enable is possible.
    DisableInstrument { symbol: Symbol },
    /// Re-enable a disabled instrument for trading.
    EnableInstrument { symbol: Symbol },
    /// Permanently remove a disabled instrument. Only succeeds if the
    /// instrument is disabled and has no resting orders.
    RemoveInstrument { symbol: Symbol },
    /// Query server stats. Not journaled (no state change) — the journal
    /// stage skips this variant. Flows through the pipeline so the matching
    /// stage can read Exchange state without concurrency issues.
    QueryStats,
    /// First entry in every v6 journal. Contains random bytes (fresh journal)
    /// or the chain hash at the rotation boundary (rotated journal). Seeds
    /// the BLAKE3 hash chain for tamper evidence and replica consistency.
    GenesisHash { hash: [u8; 32] },
    /// Periodic hash chain checkpoint emitted every 100K events. Contains the
    /// running BLAKE3 chain hash so readers can verify integrity without
    /// recomputing from genesis. Written to the journal like any other entry
    /// and itself hashed into the chain for continuity.
    Checkpoint {
        chain_hash: [u8; 32],
        events_since_checkpoint: u64,
    },
}

// Compile-time guard: GenesisHash/Checkpoint must not inflate the enum.
// SubmitOrder (with Order) is the largest variant. If this fires,
// a new variant exceeded the previous max and InputSlot/ring buffer
// cache performance will degrade.
const _: () = assert!(
    std::mem::size_of::<JournalEvent>() <= 64,
    "JournalEvent grew beyond 64 bytes — check new variant sizes"
);
