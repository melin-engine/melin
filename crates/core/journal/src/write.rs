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
    /// sequence numbers, anchored to `anchor_hash` (recorded in the file
    /// header; no entries are written, no sequence consumed).
    fn create_continuing(
        path: &Path,
        starting_sequence: u64,
        anchor_hash: [u8; 32],
    ) -> Result<Self, JournalError>;

    /// Open an existing journal for appending after recovery. The hash
    /// chain rebuilds itself from the header anchor plus the raw byte
    /// range up to `valid_end` — no chain state is threaded in.
    fn open_append(path: &Path, last_seq: u64, valid_end: u64) -> Result<Self, JournalError>;

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

    /// Write the accumulated batch, leaving durability to a later
    /// `fdatasync` on the returned descriptor.
    ///
    /// `Some(fd)` means bytes are in the page cache and a sync on `fd`
    /// is owed; `None` means nothing is owed — either the batch was
    /// empty, or this writer made the bytes durable inline.
    ///
    /// Splitting the write from the sync is not how the pipeline runs
    /// the buffered path any more — there the whole file moves to a
    /// writer thread (see
    /// `docs/internal/journal-writer-thread-2026-08.md`), because
    /// splitting them still left two threads issuing I/O against one
    /// inode. This stays for callers that write and sync on one thread
    /// but want the two steps separable.
    ///
    /// The default is the second case: it performs a full
    /// [`flush_batch_sync`](Self::flush_batch_sync) and reports nothing
    /// outstanding, so a writer that has no meaningful split (the
    /// `O_DIRECT` sector writer) keeps today's behaviour without an
    /// override, and a caller written against this method works with
    /// either. `BufferedWriter` overrides it with the real split.
    #[inline]
    fn write_batch(&mut self) -> Result<Option<std::os::fd::RawFd>, JournalError> {
        self.flush_batch_sync()?;
        Ok(None)
    }

    /// Drop the pending batch without writing it.
    fn discard_batch_buf(&mut self);

    // ---- segment handoff (writer-thread path) ----
    //
    // One capability with one implementor. Only `BufferedWriter` can give
    // up its file: its writes are plain positioned `pwrite`s, so any
    // thread holding the descriptor can issue them. `SectorWriter` drives
    // an io_uring submission queue bound to a registered fd — handing the
    // file over would mean handing the ring over — so it takes the
    // defaults below and keeps writing inline, which is what the io_uring
    // path already does asynchronously by other means.
    //
    // The defaults are written so a writer that never detaches can never
    // observe the rest: `detach_segment` returning `None` is the gate,
    // and every other method here is only reachable once it has returned
    // `Some`. See `docs/internal/journal-writer-thread-2026-08.md`.

    /// Give up the live segment's file so a writer thread can own it,
    /// leaving this writer able to encode but not to write.
    ///
    /// `None` means this writer cannot hand off and the caller must keep
    /// flushing inline.
    #[inline]
    fn detach_segment(&mut self) -> Option<crate::SegmentFile> {
        None
    }

    /// Take back a file previously handed out by
    /// [`detach_segment`](Self::detach_segment).
    ///
    /// The default drops it, which closes the descriptor — the correct
    /// disposal for a file this writer never owned. Unreachable in
    /// practice: a writer whose `detach_segment` returns `None` never
    /// hands one out for the caller to give back.
    #[inline]
    fn attach_segment(&mut self, segment: crate::SegmentFile) {
        drop(segment);
    }

    /// Hand the encoded batch off for another thread to write, taking
    /// `replacement` as the buffer to encode into next.
    ///
    /// `Some((bytes, offset))` is the batch and the segment offset it
    /// belongs at; `None` means nothing was pending and the caller keeps
    /// `replacement`. The default returns `None` — a writer that cannot
    /// detach never hands bytes over either.
    #[inline]
    fn take_batch(&mut self, replacement: Vec<u8>) -> Option<(Vec<u8>, u64)> {
        drop(replacement);
        None
    }

    /// The pair a rotation turns on: the new segment's first sequence,
    /// and the anchor linking its chain to the outgoing segment's tail.
    ///
    /// Only the encoder knows these, so a rotation performed on another
    /// thread has to be told them.
    #[inline]
    fn rotation_pair(&self) -> (u64, [u8; 32]) {
        (self.next_sequence(), self.chain_hash().unwrap_or([0u8; 32]))
    }

    /// Re-anchor the encoder onto a segment another thread has already
    /// rotated. Call only after that rotation succeeded — it discards the
    /// batch buffer and restarts the chain.
    #[inline]
    fn adopt_rotation(&mut self, starting_sequence: u64, anchor: [u8; 32]) {
        let _ = (starting_sequence, anchor);
    }

    // ---- state queries ----

    /// Sequence number that the next `allocate_sequence` call will return.
    fn next_sequence(&self) -> u64;
    /// First sequence of the active segment (the header's
    /// `starting_sequence`). Equal to `next_sequence` iff the live
    /// segment is empty. Used by rotation logic: the primary skips
    /// rotating an empty live segment, and replicas detect an
    /// already-adopted rotation boundary.
    fn segment_starting_sequence(&self) -> u64;
    /// Force the next allocated sequence number — used by replicas to
    /// adopt the primary's numbering.
    fn set_next_sequence(&mut self, seq: u64);
    /// File offset of the last byte known to be durable on disk.
    fn valid_end(&self) -> u64;
    /// On-disk path of the active segment.
    fn path(&self) -> &Path;
    /// Current chain value, `None` when the `hash-chain` feature is off.
    fn chain_hash(&self) -> Option<[u8; 32]>;

    // ---- replication framing ----

    /// Slice of the pending batch covering only the most recently
    /// encoded user entry — what replicas need to advance their state.
    fn last_user_entry_replication_slice(&self) -> &[u8];

    // ---- segment management ----

    /// Close the active segment and open a fresh one; returns the
    /// archived path.
    fn rotate_segment(&mut self) -> Result<PathBuf, JournalError>;
    /// Rotate adopting a pre-staged segment from the
    /// [`crate::preparer::SegmentPreparer`] (the fast path — no file
    /// creation/allocation on the calling thread). Each writer requires
    /// a preparer spawned in its matching mode: `spawn` for
    /// `SectorWriter`, `spawn_zero_fill` for `BufferedWriter`; both
    /// adopters reject a mismatched staging file.
    ///
    /// The default body discards the prepared segment and rotates
    /// synchronously — correct (the orphaned staging file is reclaimed
    /// by the next preparer cycle) but always the slow path, so real
    /// writers override it. It exists so adding this method is not a
    /// breaking change and so the fallback contract is spelled out in
    /// code.
    fn rotate_segment_with_prepared(
        &mut self,
        prepared: crate::preparer::PreparedSegment,
    ) -> Result<PathBuf, JournalError> {
        drop(prepared);
        self.rotate_segment()
    }
    /// Decoded file-header fields of the active segment (used by
    /// replication to bootstrap a fresh replica's chain anchor and
    /// starting sequence).
    fn read_header_info(&self) -> Result<crate::codec::FileHeaderInfo, JournalError>;

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
        anchor_hash: [u8; 32],
    ) -> Result<Self, JournalError> {
        SectorWriter::create_continuing(path, starting_sequence, anchor_hash)
    }

    #[inline]
    fn open_append(path: &Path, last_seq: u64, valid_end: u64) -> Result<Self, JournalError> {
        SectorWriter::open_append(path, last_seq, valid_end)
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
    fn segment_starting_sequence(&self) -> u64 {
        SectorWriter::segment_starting_sequence(self)
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
    fn last_user_entry_replication_slice(&self) -> &[u8] {
        SectorWriter::last_user_entry_replication_slice(self)
    }

    #[inline]
    fn rotate_segment(&mut self) -> Result<PathBuf, JournalError> {
        SectorWriter::rotate_segment(self)
    }

    #[inline]
    fn rotate_segment_with_prepared(
        &mut self,
        prepared: crate::preparer::PreparedSegment,
    ) -> Result<PathBuf, JournalError> {
        SectorWriter::rotate_segment_with_prepared(self, prepared)
    }

    #[inline]
    fn read_header_info(&self) -> Result<crate::codec::FileHeaderInfo, JournalError> {
        SectorWriter::read_header_info(self)
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
        anchor_hash: [u8; 32],
    ) -> Result<Self, JournalError> {
        BufferedWriter::create_continuing(path, starting_sequence, anchor_hash)
    }

    #[inline]
    fn open_append(path: &Path, last_seq: u64, valid_end: u64) -> Result<Self, JournalError> {
        BufferedWriter::open_append(path, last_seq, valid_end)
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
    fn write_batch(&mut self) -> Result<Option<std::os::fd::RawFd>, JournalError> {
        BufferedWriter::write_batch(self)
    }

    #[inline]
    fn discard_batch_buf(&mut self) {
        BufferedWriter::discard_batch_buf(self)
    }

    #[inline]
    fn detach_segment(&mut self) -> Option<crate::SegmentFile> {
        BufferedWriter::detach_segment(self)
    }

    #[inline]
    fn attach_segment(&mut self, segment: crate::SegmentFile) {
        BufferedWriter::attach_segment(self, segment)
    }

    #[inline]
    fn take_batch(&mut self, replacement: Vec<u8>) -> Option<(Vec<u8>, u64)> {
        BufferedWriter::take_batch(self, replacement)
    }

    #[inline]
    fn rotation_pair(&self) -> (u64, [u8; 32]) {
        BufferedWriter::rotation_pair(self)
    }

    #[inline]
    fn adopt_rotation(&mut self, starting_sequence: u64, anchor: [u8; 32]) {
        BufferedWriter::adopt_rotation(self, starting_sequence, anchor)
    }

    #[inline]
    fn next_sequence(&self) -> u64 {
        BufferedWriter::next_sequence(self)
    }

    #[inline]
    fn segment_starting_sequence(&self) -> u64 {
        BufferedWriter::segment_starting_sequence(self)
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
    fn last_user_entry_replication_slice(&self) -> &[u8] {
        BufferedWriter::last_user_entry_replication_slice(self)
    }

    #[inline]
    fn rotate_segment(&mut self) -> Result<PathBuf, JournalError> {
        BufferedWriter::rotate_segment(self)
    }

    #[inline]
    fn rotate_segment_with_prepared(
        &mut self,
        prepared: crate::preparer::PreparedSegment,
    ) -> Result<PathBuf, JournalError> {
        BufferedWriter::rotate_segment_with_prepared(self, prepared)
    }

    #[inline]
    fn read_header_info(&self) -> Result<crate::codec::FileHeaderInfo, JournalError> {
        BufferedWriter::read_header_info(self)
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
        // Header info round-trips through the trait: fresh journals
        // start at sequence 1, and the in-memory copy agrees.
        assert_eq!(writer.read_header_info().unwrap().starting_sequence, 1);
        assert_eq!(writer.segment_starting_sequence(), 1);

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
