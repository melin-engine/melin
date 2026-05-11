//! Durable write-ahead log for event-sourced applications.
//!
//! `melin-journal` is the transport-side persistence layer of Melin. It
//! owns the binary codec, the sync writer, the replay reader, snapshot
//! framing helpers, and the replication channel used to mirror durable
//! writes to replicas. Everything here is application-agnostic — the
//! journal never inspects application event variants. Instead, it
//! delegates to the [`melin_app::AppEvent`] trait, which callers
//! implement for their concrete event type.
//!
//! Responsibilities that live on the application side of the boundary
//! (matching, account state, risk logic, report encoding) stay in the
//! application crate; the journal stays usable for any Melin
//! `Application`.

#![cfg_attr(not(test), deny(clippy::unwrap_used))]

pub mod codec;
pub mod error;
pub mod event;
pub(crate) mod le;
pub mod preparer;
pub mod reader;
pub mod replication;
pub mod segment;
pub mod trace;
pub mod writer;

pub use error::JournalError;
pub use event::JournalEvent;
pub use reader::{JournalEntry, JournalReader, RawJournalScanner};
pub use writer::{
    AsyncWriteBatch, JournalWriter, checkpoint_interval, detect_sector_size, wall_clock_nanos,
};
