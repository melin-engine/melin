//! Durable write-ahead log for the trading engine.
//!
//! After the transport/app split the codec, writer, reader, replication
//! channel, and generic pipeline live in `melin-journal` and
//! `melin-transport-core`. This module re-exports them with the
//! trading-bound aliases so engine-internal and server-side callers can
//! keep using `JournalEvent`, `InputSlot`, `Pipeline`, etc. without
//! spelling the generic every time. The legacy synchronous
//! `JournaledExchange` wrapper and the Exchange-aware snapshot framing
//! stay here because they're entangled with `Exchange` internals.

pub mod engine;
pub mod snapshot;

#[cfg(test)]
pub mod pipeline_tests;

pub use engine::{JournaledExchange, JournaledExchangeError};

/// The `TradingEvent`-parameterised journal types — aliased here so
/// engine/server code doesn't have to spell the generic every time.
pub type JournalEvent = melin_journal::JournalEvent<crate::trading_event::TradingEvent>;
pub type JournalEntry = melin_journal::JournalEntry<crate::trading_event::TradingEvent>;
pub type JournalReader = melin_journal::JournalReader<crate::trading_event::TradingEvent>;
pub type JournalWriter = melin_journal::JournalWriter<crate::trading_event::TradingEvent>;

/// Trading-bound aliases for the generic pipeline types (now living in
/// `melin-transport-core`). Server/bench callers use these so they
/// never have to spell `<Exchange>` or `<TradingEvent>` explicitly.
pub type InputSlot = pipeline::InputSlot<crate::trading_event::TradingEvent>;
pub type OutputSlot =
    pipeline::OutputSlot<crate::types::ExecutionReport, crate::types::QueryResponse>;
pub type OutputPayload =
    pipeline::OutputPayload<crate::types::ExecutionReport, crate::types::QueryResponse>;
pub type Pipeline = pipeline::Pipeline<crate::exchange::Exchange>;
pub type ReplicaPipeline = pipeline::ReplicaPipeline<crate::exchange::Exchange>;
pub type MatchingStage = pipeline::MatchingStage<crate::exchange::Exchange>;
pub type JournalStage = pipeline::JournalStage<crate::trading_event::TradingEvent>;

/// Re-export the generic pipeline module so callers reaching for raw
/// generic types (`pipeline::build_pipeline_with_replication::<A>`,
/// `pipeline::StageUtilization`) find them at the familiar path.
pub use melin_transport_core::pipeline;

pub use melin_journal::{
    AsyncWriteBatch, CHECKPOINT_INTERVAL, JournalError, RawJournalScanner, codec, replication,
    trace, wall_clock_nanos,
};
