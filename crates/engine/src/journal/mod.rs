//! Durable write-ahead log (WAL) for event sourcing, crash recovery,
//! deterministic replay, and regulatory audit trail.
//!
//! Journals input commands only (orders, cancels, deposits, instrument
//! registration) — not execution reports. Since the matching engine is
//! deterministic, replaying inputs reproduces outputs identically.

pub mod codec;
pub mod engine;
pub mod error;
pub mod event;
pub mod pipeline;
pub mod reader;
pub mod replication;
pub mod snapshot;
pub mod trace;
pub mod writer;

pub use engine::JournaledExchange;
pub use error::JournalError;
pub use event::JournalEvent;
pub use reader::JournalReader;
pub use writer::JournalWriter;
