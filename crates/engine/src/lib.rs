#![cfg_attr(not(test), deny(clippy::unwrap_used))]

pub mod account;
pub mod application_impl;
pub mod exchange;
pub mod journal;
pub mod orderbook;
pub mod scheduler;
pub mod types;

// Re-exports of the shared trading wire types and codec. Extracted
// into `melin-trading` so the no-op transport binary can speak the
// same protocol without linking the matching engine; engine-internal
// code (and downstream consumers still on the old import paths)
// continue to reach them here.
pub use melin_trading::trading_event;
pub use melin_types::le;

#[cfg(test)]
mod fuzz_tests;
#[cfg(test)]
mod proptests;
