//! Shared trading types: the wire-level data model and wire codec used by
//! both the matching engine and the no-op transport binary. Nothing in
//! this crate knows how matching works; it just describes the shapes
//! that flow across the network and through the journal.
//!
//! Extracting these out of `melin-engine` is what lets the transport +
//! no-op binary run the same benchmark traffic without linking the
//! matching logic.

#![cfg_attr(not(test), deny(clippy::unwrap_used))]

pub mod le;
pub mod trading_event;
pub mod types;

pub use trading_event::TradingEvent;
pub use types::*;
