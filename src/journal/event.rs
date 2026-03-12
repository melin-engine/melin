//! Journal event model.
//!
//! Only input commands are journaled — not execution reports. The matching
//! engine is deterministic, so replaying inputs reproduces outputs identically.
//! This halves journal size and simplifies the format.

use crate::types::{AccountId, CurrencyId, InstrumentSpec, Order, OrderId, Symbol};

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
}
