//! Wire protocol for the trading engine.
//!
//! Defines message types, binary codec, and transport abstraction.
//! Shared by the server and client crates.

pub mod codec;
pub mod error;
pub mod message;
pub mod tcp;
pub mod transport;

/// Re-export engine types that clients need to construct requests and
/// interpret responses, so they don't need a direct dependency on the
/// engine crate.
pub mod types {
    pub use trading_engine::types::{
        AccountId, ExecutionReport, Order, OrderId, OrderType, Price, Quantity, RejectReason, Side,
        Symbol, TimeInForce,
    };
}
