//! Shared trait implemented by both concrete journal writers
//! ([`SectorWriter`] and [`BufferedWriter`]).
//!
//! The trait is what `JournalStage`, `Pipeline`, and `JournaledApp`
//! are generic over. Each call site picks a concrete writer at
//! construction time, so the trait is statically dispatched — no
//! runtime `match` on a writer variant.
//!
//! The trait is intentionally **not** dyn-compatible-by-design: it has
//! no consumers that need `Box<dyn JournalWrite>` and several methods
//! return `Self`. Keep it that way — the whole point of the refactor
//! is monomorphisation.
//!
//! [`SectorWriter`]: crate::sector_writer::SectorWriter
//! [`BufferedWriter`]: crate::buffered_writer::BufferedWriter

use std::path::{Path, PathBuf};

use melin_app::{AppEvent, unix_epoch_nanos};

use crate::buffered_writer::BufferedWriter;
use crate::error::JournalError;
use crate::event::JournalEvent;
use crate::sector_writer::SectorWriter;

/// Operations a journal writer must support to be drivable by the
/// pipeline's `JournalStage`. Excludes the variant-specific surfaces
/// (io_uring registration, async submit/confirm on the sector path;
/// `append`/`batch_append` convenience wrappers used only by tests
/// and benches) — those stay as inherent methods on the concrete
/// types.
pub trait JournalWrite<E: AppEvent>: Sized {
    // ---- constructors ----
    //
    // Trait-level constructors let generic code (e.g. `JournaledApp`)
    // build a writer of any concrete type without knowing which one.
    // Each implementor forwards to its inherent constructor.

    /// Create a fresh journal at `path`.
    fn create(path: &Path) -> Result<Self, JournalError>;

    /// Create a fresh journal that continues a previous segment's
    /// sequence numbers, anchored to `genesis_hash`.
    fn create_continuing(
        path: &Path,
        starting_sequence: u64,
        genesis_hash: [u8; 32],
    ) -> Result<Self, JournalError>;

    /// Open an existing journal for appending after recovery.
    fn open_append(
        path: &Path,
        last_seq: u64,
        valid_end: u64,
        chain_hash: Option<[u8; 32]>,
        events_since_checkpoint: u64,
    ) -> Result<Self, JournalError>;

    // ---- hot-path write API ----

    /// Allocate and return the next sequence number, advancing the
    /// internal counter.
    fn allocate_sequence(&mut self) -> u64;

    /// Encode a single event with a pre-assigned sequence number.
    /// Does not advance the sequence counter — see `allocate_sequence`.
    fn encode_event(
        &mut self,
        seq: u64,
        timestamp_ns: u64,
        event: &JournalEvent<E>,
        key_hash: u64,
        request_seq: u64,
    ) -> Result<(), JournalError>;

    /// Write the accumulated batch and force it to stable media.
    fn flush_batch_sync(&mut self) -> Result<(), JournalError>;

    /// Drop the pending batch without writing it.
    fn discard_batch_buf(&mut self);

    // ---- state queries ----

    /// Sequence number that the next `allocate_sequence` call will return.
    fn next_sequence(&self) -> u64;
    /// Force the next allocated sequence number — used by replicas to
    /// adopt the primary's numbering.
    fn set_next_sequence(&mut self, seq: u64);
    /// File offset of the last byte known to be durable on disk.
    fn valid_end(&self) -> u64;
    /// On-disk path of the active segment.
    fn path(&self) -> &Path;
    /// Current chain anchor, `None` when the `hash-chain` feature is off.
    fn chain_hash(&self) -> Option<[u8; 32]>;
    /// Number of user events written since the last checkpoint entry —
    /// drives checkpoint emission cadence.
    fn events_since_checkpoint(&self) -> u64;

    // ---- replication framing ----

    /// Slice of the pending batch covering only the most recently
    /// encoded user entry — what replicas need to advance their state.
    fn last_user_entry_replication_slice(&self) -> &[u8];

    // ---- segment management ----

    /// Close the active segment and open a fresh one; returns the
    /// archived path.
    fn rotate_segment(&mut self) -> Result<PathBuf, JournalError>;
    /// Read the first entry of the active segment (used by replication
    /// to bootstrap a fresh replica's chain anchor).
    fn read_genesis_entry(&self) -> Result<Vec<u8>, JournalError>;

    // ---- default convenience wrappers ----
    //
    // Built on the three primitives (`allocate_sequence`, `encode_event`,
    // `flush_batch_sync`). Used by engine lifecycle, tests, and benches —
    // never on the pipeline's hot path, which goes through the primitives
    // directly to avoid the extra trait dispatches on each event.

    /// Encode and durably flush a single event.
    #[inline]
    fn append(&mut self, event: &JournalEvent<E>) -> Result<u64, JournalError> {
        let seq = self.batch_append_with_ts(event, unix_epoch_nanos(), 0, 0)?;
        self.flush_batch_sync()?;
        Ok(seq)
    }

    /// Encode an event into the batch buffer with a caller-provided
    /// timestamp — lets the caller take one `clock_gettime` per batch
    /// instead of per event.
    #[inline]
    fn batch_append_with_ts(
        &mut self,
        event: &JournalEvent<E>,
        timestamp_ns: u64,
        key_hash: u64,
        request_seq: u64,
    ) -> Result<u64, JournalError> {
        let seq = self.allocate_sequence();
        self.encode_event(seq, timestamp_ns, event, key_hash, request_seq)?;
        Ok(seq)
    }
}

impl<E: AppEvent> JournalWrite<E> for SectorWriter<E> {
    #[inline]
    fn create(path: &Path) -> Result<Self, JournalError> {
        SectorWriter::create(path)
    }

    #[inline]
    fn create_continuing(
        path: &Path,
        starting_sequence: u64,
        genesis_hash: [u8; 32],
    ) -> Result<Self, JournalError> {
        SectorWriter::create_continuing(path, starting_sequence, genesis_hash)
    }

    #[inline]
    fn open_append(
        path: &Path,
        last_seq: u64,
        valid_end: u64,
        chain_hash: Option<[u8; 32]>,
        events_since_checkpoint: u64,
    ) -> Result<Self, JournalError> {
        SectorWriter::open_append(
            path,
            last_seq,
            valid_end,
            chain_hash,
            events_since_checkpoint,
        )
    }

    #[inline]
    fn allocate_sequence(&mut self) -> u64 {
        SectorWriter::allocate_sequence(self)
    }

    #[inline]
    fn encode_event(
        &mut self,
        seq: u64,
        timestamp_ns: u64,
        event: &JournalEvent<E>,
        key_hash: u64,
        request_seq: u64,
    ) -> Result<(), JournalError> {
        SectorWriter::encode_event(self, seq, timestamp_ns, event, key_hash, request_seq)
    }

    #[inline]
    fn flush_batch_sync(&mut self) -> Result<(), JournalError> {
        SectorWriter::flush_batch_sync(self)
    }

    #[inline]
    fn discard_batch_buf(&mut self) {
        SectorWriter::discard_batch_buf(self)
    }

    #[inline]
    fn next_sequence(&self) -> u64 {
        SectorWriter::next_sequence(self)
    }

    #[inline]
    fn set_next_sequence(&mut self, seq: u64) {
        SectorWriter::set_next_sequence(self, seq)
    }

    #[inline]
    fn valid_end(&self) -> u64 {
        SectorWriter::valid_end(self)
    }

    #[inline]
    fn path(&self) -> &Path {
        SectorWriter::path(self)
    }

    #[inline]
    fn chain_hash(&self) -> Option<[u8; 32]> {
        SectorWriter::chain_hash(self)
    }

    #[inline]
    fn events_since_checkpoint(&self) -> u64 {
        SectorWriter::events_since_checkpoint(self)
    }

    #[inline]
    fn last_user_entry_replication_slice(&self) -> &[u8] {
        SectorWriter::last_user_entry_replication_slice(self)
    }

    #[inline]
    fn rotate_segment(&mut self) -> Result<PathBuf, JournalError> {
        SectorWriter::rotate_segment(self)
    }

    #[inline]
    fn read_genesis_entry(&self) -> Result<Vec<u8>, JournalError> {
        SectorWriter::read_genesis_entry(self)
    }
}

impl<E: AppEvent> JournalWrite<E> for BufferedWriter<E> {
    #[inline]
    fn create(path: &Path) -> Result<Self, JournalError> {
        BufferedWriter::create(path)
    }

    #[inline]
    fn create_continuing(
        path: &Path,
        starting_sequence: u64,
        genesis_hash: [u8; 32],
    ) -> Result<Self, JournalError> {
        BufferedWriter::create_continuing(path, starting_sequence, genesis_hash)
    }

    #[inline]
    fn open_append(
        path: &Path,
        last_seq: u64,
        valid_end: u64,
        chain_hash: Option<[u8; 32]>,
        events_since_checkpoint: u64,
    ) -> Result<Self, JournalError> {
        BufferedWriter::open_append(
            path,
            last_seq,
            valid_end,
            chain_hash,
            events_since_checkpoint,
        )
    }

    #[inline]
    fn allocate_sequence(&mut self) -> u64 {
        BufferedWriter::allocate_sequence(self)
    }

    #[inline]
    fn encode_event(
        &mut self,
        seq: u64,
        timestamp_ns: u64,
        event: &JournalEvent<E>,
        key_hash: u64,
        request_seq: u64,
    ) -> Result<(), JournalError> {
        BufferedWriter::encode_event(self, seq, timestamp_ns, event, key_hash, request_seq)
    }

    #[inline]
    fn flush_batch_sync(&mut self) -> Result<(), JournalError> {
        BufferedWriter::flush_batch_sync(self)
    }

    #[inline]
    fn discard_batch_buf(&mut self) {
        BufferedWriter::discard_batch_buf(self)
    }

    #[inline]
    fn next_sequence(&self) -> u64 {
        BufferedWriter::next_sequence(self)
    }

    #[inline]
    fn set_next_sequence(&mut self, seq: u64) {
        BufferedWriter::set_next_sequence(self, seq)
    }

    #[inline]
    fn valid_end(&self) -> u64 {
        BufferedWriter::valid_end(self)
    }

    #[inline]
    fn path(&self) -> &Path {
        BufferedWriter::path(self)
    }

    #[inline]
    fn chain_hash(&self) -> Option<[u8; 32]> {
        BufferedWriter::chain_hash(self)
    }

    #[inline]
    fn events_since_checkpoint(&self) -> u64 {
        BufferedWriter::events_since_checkpoint(self)
    }

    #[inline]
    fn last_user_entry_replication_slice(&self) -> &[u8] {
        BufferedWriter::last_user_entry_replication_slice(self)
    }

    #[inline]
    fn rotate_segment(&mut self) -> Result<PathBuf, JournalError> {
        BufferedWriter::rotate_segment(self)
    }

    #[inline]
    fn read_genesis_entry(&self) -> Result<Vec<u8>, JournalError> {
        BufferedWriter::read_genesis_entry(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use melin_app::CodecError;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct TestEvent(u64);

    impl AppEvent for TestEvent {
        fn encoded_size(&self) -> usize {
            8
        }
        fn encode(&self, buf: &mut [u8]) -> usize {
            buf[..8].copy_from_slice(&self.0.to_le_bytes());
            8
        }
        fn decode(buf: &[u8]) -> Result<Self, CodecError> {
            if buf.len() < 8 {
                return Err(CodecError::Truncated);
            }
            Ok(TestEvent(u64::from_le_bytes(buf[..8].try_into().unwrap())))
        }
        fn is_query(&self) -> bool {
            false
        }
    }

    // Exercises every trait method against a fresh writer. Acts as a
    // typecheck (the bound `W: JournalWrite<TestEvent>` must hold for
    // both concrete writers) and a routing check (each delegate must
    // hit the matching inherent method).
    fn exercise<W: JournalWrite<TestEvent>>(writer: &mut W, expected_path: &Path) {
        assert_eq!(writer.path(), expected_path);
        // Use `valid_end` as the durability watermark: it advances on
        // both writers after a flush. `write_pos` is sector-aligned on
        // the O_DIRECT path and won't tip over for a single small event.
        let initial_valid_end = writer.valid_end();
        assert!(initial_valid_end > 0);
        assert_eq!(writer.events_since_checkpoint(), 0);

        // discard on an empty batch is a no-op but must not panic.
        writer.discard_batch_buf();
        writer.flush_batch_sync().unwrap();

        // Encode + flush one event via the trait, then verify it landed.
        let seq = writer.allocate_sequence();
        assert_eq!(seq + 1, writer.next_sequence());
        let ts = unix_epoch_nanos();
        writer
            .encode_event(seq, ts, &JournalEvent::App(TestEvent(seq)), 0, 0)
            .unwrap();

        // Replication framing slice should now be populated.
        assert!(!writer.last_user_entry_replication_slice().is_empty());

        writer.flush_batch_sync().unwrap();
        assert!(writer.valid_end() > initial_valid_end);

        // set_next_sequence overrides the counter — proves the setter
        // routes through the trait, not just past it.
        writer.set_next_sequence(42);
        assert_eq!(writer.next_sequence(), 42);

        // chain_hash() is feature-gated; we just assert it doesn't
        // panic regardless of the cfg state.
        let _ = writer.chain_hash();
    }

    #[test]
    fn trait_drives_buffered_writer() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("buf.journal");
        let mut writer = BufferedWriter::<TestEvent>::create(&path).unwrap();
        exercise(&mut writer, &path);
    }

    #[test]
    fn trait_drives_sector_writer() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sec.journal");
        let mut writer = SectorWriter::<TestEvent>::create(&path).unwrap();
        exercise(&mut writer, &path);
    }
}
