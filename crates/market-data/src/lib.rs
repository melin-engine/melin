//! Order book mirror and trade history for market data consumers.
//!
//! Pure library crate — no I/O, no threading. Defines data structures
//! and update logic that both the server-side event publisher and the
//! client-side `MarketDataCore` reuse.
//!
//! The `BookMirror` reconstructs a per-symbol L2 order book from the
//! `ExecutionReport` stream emitted by the matching engine. The
//! `OrderIndex` tracks resting orders so fills and cancels can be
//! resolved back to the correct price level. The `TradeRing` keeps a
//! bounded window of recent trades per symbol.

pub mod cold_start;
pub mod core;
pub mod index;
pub mod mirror;
#[cfg(test)]
mod proptests;
pub mod subscriber;
pub mod trade_ring;
