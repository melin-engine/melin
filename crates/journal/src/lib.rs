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

pub mod buffered_writer;
pub mod codec;
pub mod error;
pub mod event;
pub mod fresh_replica;
pub(crate) mod le;
pub mod mode;
pub(crate) mod prealloc;
pub mod preparer;
pub mod reader;
pub mod replication;
pub mod sector_writer;
pub mod segment;
pub mod write;

#[cfg(feature = "test-utils")]
pub mod test_utils;

pub use buffered_writer::BufferedWriter;
pub use error::JournalError;
pub use event::JournalEvent;
pub use fresh_replica::create_fresh_replica;
pub use mode::JournalWriterMode;
pub use reader::{JournalEntry, JournalReader, RawJournalScanner};
pub use sector_writer::{AsyncWriteBatch, SectorWriter, checkpoint_interval, detect_sector_size};
pub use write::JournalWrite;
