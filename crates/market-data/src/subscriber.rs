//! Types for market-data session subscriptions and fan-out.
//!
//! These are the shared vocabulary between `MarketDataCore` (which
//! produces updates) and the md-gateway sessions (which consume them
//! and translate to FIX).

use melin_trading::types::{Price, Side, Symbol};

use crate::mirror::Level;

/// Opaque slot identifier for a downstream session. Assigned by the
/// md-gateway when a FIX session subscribes.
///
/// u32: supports up to ~4 billion concurrent sessions. At typical
/// gateway scale (< 1000 sessions), this is more than sufficient.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SessionSlotId(pub u32);

/// Commands sent from md-gateway sessions to MarketDataCore.
#[derive(Debug, Clone)]
pub enum MdCommand {
    /// Subscribe a session to one or more symbols.
    Subscribe {
        session: SessionSlotId,
        symbols: Vec<Symbol>,
    },
    /// Unsubscribe a session from all symbols.
    Unsubscribe { session: SessionSlotId },
}

/// Updates sent from MarketDataCore to md-gateway sessions.
#[derive(Debug, Clone)]
pub enum MdOutput {
    /// Full book snapshot for one symbol (sent on subscribe or reconnect).
    Snapshot {
        symbol: Symbol,
        bids: Vec<(Price, Level)>,
        asks: Vec<(Price, Level)>,
    },
    /// One or more levels changed on one symbol.
    LevelUpdate {
        symbol: Symbol,
        updates: Vec<LevelUpdate>,
    },
    /// A trade occurred.
    Trade {
        symbol: Symbol,
        price: Price,
        qty: u64,
    },
}

/// A single level change within an incremental update.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LevelUpdate {
    pub side: Side,
    pub price: Price,
    /// New aggregate state. `None` = level removed.
    pub level: Option<Level>,
}
