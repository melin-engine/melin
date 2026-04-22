//! Wire protocol for the trading engine.
//!
//! Defines message types, binary codec, and transport abstraction.
//! Shared by the server and client crates.

pub mod auth;
pub mod blocking;
pub mod codec;
pub mod error;
pub mod message;
pub mod tcp;
pub mod transport;
pub mod uds;

#[cfg(test)]
mod fuzz_tests;

/// Re-export engine types that clients need to construct requests and
/// interpret responses, so they don't need a direct dependency on the
/// engine crate.
pub mod types {
    pub use melin_trading::types::{
        AccountId, CircuitBreakerConfig, CurrencyId, ExecutionReport, FeeSchedule, InstrumentSpec,
        InstrumentStatus, Order, OrderId, OrderType, Price, Quantity, RejectReason, RiskLimits,
        SelfTradeProtection, Side, Symbol, TimeInForce,
    };
}
