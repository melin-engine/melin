//! Durable write-ahead log for the trading engine.
//!
//! After the transport/app split, the codec, writer, reader,
//! replication channel, and error types live in the `melin-journal`
//! crate — this module re-exports them with `TradingEvent` bound in so
//! callers inside `melin-engine` can keep using `JournalEvent` without
//! generic annotations everywhere. Modules that still depend on the
//! `Exchange` (the synchronous `JournaledExchange` wrapper, the matching
//! pipeline, snapshot framing) stay here until Phase 3 moves them into
//! a transport-core crate.

pub mod engine;
pub mod pipeline;
pub mod snapshot;

pub use engine::{JournaledExchange, JournaledExchangeError};

/// The `TradingEvent`-parameterised journal types — aliased here so
/// engine/server code doesn't have to spell the generic every time.
pub type JournalEvent = melin_journal::JournalEvent<crate::trading_event::TradingEvent>;
pub type JournalEntry = melin_journal::JournalEntry<crate::trading_event::TradingEvent>;
pub type JournalReader = melin_journal::JournalReader<crate::trading_event::TradingEvent>;
pub type JournalWriter = melin_journal::JournalWriter<crate::trading_event::TradingEvent>;

pub use melin_journal::{
    AsyncWriteBatch, CHECKPOINT_INTERVAL, JournalError, RawJournalScanner, codec, replication,
    trace, wall_clock_nanos,
};
