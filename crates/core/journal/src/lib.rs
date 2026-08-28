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
#[cfg(feature = "hash-chain")]
pub(crate) mod chain;
pub mod codec;
pub mod encoder;
pub mod error;
pub mod event;
pub(crate) mod le;
pub(crate) mod prealloc;
pub mod preparer;
pub mod reader;
pub mod replication;
pub mod segment;
pub mod segment_file;
pub mod write;
pub mod write_ring;

#[cfg(feature = "test-utils")]
pub mod test_utils;

pub use buffered_writer::BufferedWriter;
pub use codec::FileHeaderInfo;
pub use encoder::JournalEncoder;
pub use error::JournalError;
pub use event::JournalEvent;
pub use preparer::StagingMode;
pub use reader::{JournalEntry, JournalReader, RawJournalScanner};
pub use segment_file::SegmentFile;
pub use write::JournalWrite;

/// Random 32-byte chain anchor for a brand-new journal. Randomness (not
/// zeros) guarantees two independent journal lineages can never share a
/// chain value, so a snapshot or replica paired with the wrong cluster's
/// journal fails its first chain comparison.
pub(crate) fn fresh_anchor() -> Result<[u8; 32], error::JournalError> {
    let mut anchor = [0u8; 32];
    getrandom::fill(&mut anchor)
        .map_err(|e| error::JournalError::Io(std::io::Error::other(e.to_string())))?;
    Ok(anchor)
}
