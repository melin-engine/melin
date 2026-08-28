//! Buffered journal writer — page-cache writes with explicit `fdatasync`.
//!
//! The journal's only writer. Durability never depends on
//! capacitor-backed power-loss protection on the storage device, and
//! the on-disk format (`crate::codec` framing) is the same one the
//! retired O_DIRECT writer produced, so [`crate::reader::JournalReader`]
//! and [`crate::segment`] recovery read journals from either lineage
//! without modification.
//!
//! ## Durability contract
//!
//! Every call to [`BufferedWriter::flush_batch_sync`] issues a
//! single positioned `pwrite` followed by `fdatasync`. The call returns
//! only once the kernel reports the data is on stable media — honest
//! durability on any drive, PLP or not. On a drive with a volatile write
//! cache (NVMe `VWC=1`) this pays one device flush per batch; on a drive
//! that reports `VWC=0` (full PLP) the flush is a near-no-op.
//!
//! ## Why buffered
//!
//! An O_DIRECT writer carries machinery that exists *only* to satisfy
//! sector alignment: a partial-tail sector kept in memory, sector-rounded
//! `pwrite`, sector-aligned scratch buffers, sector-size detection. None
//! of it applies once writes go through the page cache. This module is
//! the clean half: a `Vec<u8>` batch buffer, plain `pwrite_all`, and
//! `fdatasync` for durability.
//!
//! ## Two halves
//!
//! The writer is a composition, not a monolith:
//! [`JournalEncoder`] owns the entry stream (sequence allocation,
//! framing, the hash chain, the batch buffer) and
//! [`SegmentFile`] owns the file (descriptor, write position,
//! pre-allocation, rotation). Nothing is shared between them — the
//! encoder produces bytes, the segment writes them.
//!
//! That is the same line the pipeline draws between its sequencing
//! thread and its disk thread, which is why it is a type boundary here
//! rather than a comment: a field can only be reached from the half
//! that owns it. `BufferedWriter` composes the two back together for
//! every caller that is single-threaded anyway — recovery, offline
//! tooling, `JournaledApp`, and the tests.

use std::path::{Path, PathBuf};

use melin_app::AppEvent;

use crate::codec;
use crate::encoder::{JournalEncoder, entry_size};
use crate::error::JournalError;
use crate::event::JournalEvent;
use crate::segment_file::SegmentFile;

/// Append-only journal writer that goes through the kernel page cache
/// and forces durability with `fdatasync` per flush.
///
/// Owns both halves of the journal (see the module docs) and drives
/// them from one thread.
pub struct BufferedWriter<E: AppEvent> {
    /// Entry stream: sequences, framing, chain, batch offsets.
    encoder: JournalEncoder<E>,
    /// Live segment on disk: descriptor, position, rotation.
    segment: SegmentFile,
    /// Destination the encoder appends into. Owned here because this
    /// writer is the single-threaded composition; the pipeline instead
    /// encodes straight into a hand-off ring slot. Pre-sized to
    /// [`BATCH_BUF_CAPACITY`] and grown only if a caller batches past
    /// it — flushing resets the length, never the allocation.
    batch_buf: Vec<u8>,
}

/// Destination capacity for the composed writer's batches. Sized so the
/// pipeline's normal flush cadence never has to grow it.
const BATCH_BUF_CAPACITY: usize = 512 * 1024;

impl<E: AppEvent> BufferedWriter<E> {
    /// Create a fresh journal file. The chain anchor is random salt so
    /// histories from different runs/clusters are never confusable.
    pub fn create(path: &Path) -> Result<Self, JournalError> {
        crate::preparer::cleanup_staging_orphan(path);
        Self::create_continuing(path, 1, crate::fresh_anchor()?)
    }

    /// Create a fresh journal that continues a previous segment's sequence
    /// numbers, anchored to `anchor_hash` (the prior segment's chain tip,
    /// or random salt for a brand-new journal). Both values are recorded
    /// in the file header; no entries are written.
    pub fn create_continuing(
        path: &Path,
        starting_sequence: u64,
        anchor_hash: [u8; 32],
    ) -> Result<Self, JournalError> {
        Ok(Self {
            segment: SegmentFile::create_continuing(path, starting_sequence, anchor_hash)?,
            encoder: JournalEncoder::new(starting_sequence, anchor_hash),
            batch_buf: vec![0u8; BATCH_BUF_CAPACITY],
        })
    }

    /// Open an existing journal for appending after recovery.
    ///
    /// `last_seq` is the sequence number of the last valid entry seen
    /// by the reader. `valid_end` is the byte offset immediately past
    /// that entry — new entries are written starting here, overwriting
    /// any trailing garbage from a partial crash write.
    ///
    /// The hash chain is rebuilt self-containedly: the anchor comes from
    /// the file header and the hasher re-absorbs the raw byte range
    /// `[ENTRY_OFFSET, valid_end)` — the chain is a pure function of
    /// those two inputs, so no chain state needs to be threaded in from
    /// the recovery walk.
    pub fn open_append(path: &Path, last_seq: u64, valid_end: u64) -> Result<Self, JournalError> {
        // The segment half opens and scrubs the file, and hands back
        // the decoded header the stream half needs to resume: the
        // anchor to rebuild the chain from, and the segment's first
        // sequence.
        let (segment, info) = SegmentFile::open_append(path, valid_end)?;
        let encoder = JournalEncoder::resume(
            path,
            info.starting_sequence,
            info.anchor_hash,
            last_seq,
            valid_end,
        )?;
        Ok(Self {
            encoder,
            segment,
            batch_buf: vec![0u8; BATCH_BUF_CAPACITY],
        })
    }

    /// Allocate and return the next sequence number, advancing the
    /// internal counter.
    pub fn allocate_sequence(&mut self) -> u64 {
        self.encoder.allocate_sequence()
    }

    /// Encode a single event with a pre-assigned sequence number.
    ///
    /// Does not advance the internal sequence counter — the caller
    /// owns sequencing (via [`allocate_sequence`](Self::allocate_sequence)
    /// on the primary or [`set_next_sequence`](Self::set_next_sequence)
    /// on a replica). The entry's raw bytes are absorbed into the
    /// segment hash chain; nothing else is emitted — the chain has no
    /// in-stream metadata.
    pub fn encode_event(
        &mut self,
        seq: u64,
        timestamp_ns: u64,
        event: &JournalEvent<E>,
        key_hash: u64,
        request_seq: u64,
    ) -> Result<(), JournalError> {
        // The encoder cannot grow a buffer it does not own, so keep a
        // whole entry's headroom ahead of it. `entry_size::<E>()` rather
        // than the cross-application ceiling: reserving the ceiling would
        // make an app with 9-byte events grow its batch buffer twenty
        // times sooner than it needs to. Growth is the rare oversize-batch
        // fallback — Vec's amortised growth absorbs it.
        let headroom = self.batch_buf.len() - self.encoder.batch_len();
        if headroom < entry_size::<E>() {
            let grown = self.batch_buf.len() + BATCH_BUF_CAPACITY;
            tracing::warn!(
                batch_len = self.encoder.batch_len(),
                capacity = self.batch_buf.len(),
                grown,
                "journal batch exceeded preallocated capacity — caller is \
                 batching more than capacity between flushes; raise \
                 BATCH_BUF_CAPACITY or flush more often"
            );
            self.batch_buf.resize(grown, 0);
        }
        self.encoder.encode_event(
            &mut self.batch_buf,
            seq,
            timestamp_ns,
            event,
            key_hash,
            request_seq,
        )
    }

    /// Write the accumulated batch and force it to stable media.
    ///
    /// Issues exactly one `pwrite` covering the whole batch, followed by
    /// `fdatasync`. Returns only when the kernel reports data is durable.
    pub fn flush_batch_sync(&mut self) -> Result<(), JournalError> {
        // Paced retry of a failed post-rotation dir fsync — a single
        // branch in steady state.
        self.segment.poll_dir_fsync_retry();
        if self.encoder.batch_len() == 0 {
            return Ok(());
        }
        self.segment
            .write_batch(self.encoder.pending_batch_bytes(&self.batch_buf))?;
        self.segment.sync()?;
        self.encoder.clear_batch();
        Ok(())
    }

    /// Drop the pending batch without writing it. Used by the
    /// `no-persist` path of the journal stage to keep the buffer
    /// bounded after replication has snapshotted the bytes.
    pub fn discard_batch_buf(&mut self) {
        self.encoder.clear_batch();
    }

    pub fn next_sequence(&self) -> u64 {
        self.encoder.next_sequence()
    }

    /// Set the next sequence number — used by the replica receiver to
    /// keep the writer's counter aligned with primary-assigned sequences.
    pub fn set_next_sequence(&mut self, seq: u64) {
        self.encoder.set_next_sequence(seq);
    }

    /// Current byte offset of the next entry. Always equal to
    /// [`valid_end`](Self::valid_end) on the buffered writer — there's
    /// no in-memory partial sector, so the on-disk end and the logical
    /// end coincide.
    pub fn write_pos(&self) -> u64 {
        self.segment.valid_end()
    }

    /// Byte offset of the end of valid on-disk data. Identical to
    /// `write_pos` here; kept as a separate method because callers
    /// speak in terms of the durable end, not the write cursor.
    pub fn valid_end(&self) -> u64 {
        self.segment.valid_end()
    }

    pub fn path(&self) -> &Path {
        self.segment.path()
    }

    /// Decoded file-header fields of the live segment (read from disk).
    /// Used at primary startup to hand replicas the segment's
    /// `(starting_sequence, anchor_hash)` so a fresh replica journal is
    /// byte-identical from the segment's first entry onward.
    pub fn read_header_info(&self) -> Result<codec::FileHeaderInfo, JournalError> {
        self.segment.read_header_info()
    }

    /// First sequence of the active segment (the header's
    /// `starting_sequence`). `next_sequence() == segment_starting_sequence()`
    /// means the live segment is empty.
    pub fn segment_starting_sequence(&self) -> u64 {
        self.encoder.segment_starting_sequence()
    }

    /// Current chain value: `BLAKE3(entry bytes so far || anchor)`, or
    /// the anchor itself for an empty segment. `None` when `hash-chain`
    /// is disabled. Non-destructive (clone + finalize).
    pub fn chain_hash(&self) -> Option<[u8; 32]> {
        self.encoder.chain_hash()
    }

    /// Encoded bytes that have been appended to the batch buffer but
    /// not yet flushed. Used by the journal stage to snapshot the
    /// pending bytes for replication.
    pub fn pending_batch_bytes(&self) -> &[u8] {
        self.encoder.pending_batch_bytes(&self.batch_buf)
    }

    /// Slice of the most-recent user entry, with the 2-byte magic
    /// stripped from the front and the 4-byte CRC stripped from the
    /// back — exact wire shape consumed by the replication stage.
    pub fn last_user_entry_replication_slice(&self) -> &[u8] {
        self.encoder
            .last_user_entry_replication_slice(&self.batch_buf)
    }

    /// Take the writer apart into its two halves so they can be driven
    /// from separate threads — the stream half stays with the
    /// sequencer, the file half moves to the disk thread.
    ///
    /// Any pending batch is flushed first: the halves are handed over
    /// quiesced, so nothing encoded-but-unwritten can be stranded on
    /// the wrong side of the boundary.
    pub fn into_halves(mut self) -> Result<(JournalEncoder<E>, SegmentFile), JournalError> {
        self.flush_batch_sync()?;
        Ok((self.encoder, self.segment))
    }

    /// Reassemble the writer from halves that were split by
    /// [`into_halves`](Self::into_halves) — how the pipeline hands the
    /// journal back on shutdown, for recovery and promotion to use
    /// single-threaded.
    pub fn from_halves(encoder: JournalEncoder<E>, segment: SegmentFile) -> Self {
        Self {
            encoder,
            segment,
            batch_buf: vec![0u8; BATCH_BUF_CAPACITY],
        }
    }

    /// Rotate the live segment in place.
    ///
    /// Flushes any pending batch durably, archives the live segment via
    /// [`crate::segment::archive_live`], and opens a fresh live segment
    /// at the original path whose header anchor is the outgoing
    /// segment's tail chain hash. Returns the path of the archived
    /// segment. No sequence number is consumed — the next event written
    /// gets exactly `next_sequence`.
    pub fn rotate_segment(&mut self) -> Result<PathBuf, JournalError> {
        self.rotate_segment_inner(None)
    }

    /// Rotate, adopting a pre-staged segment produced by
    /// [`crate::preparer::SegmentPreparer::spawn`].
    ///
    /// Same contract as [`Self::rotate_segment`], but the new segment
    /// is the preparer's zero-filled staging file instead of a fresh
    /// `create_continuing`. Two wins over the sync path:
    ///
    /// - rotation cost drops to two renames + a header pwrite + fsyncs
    ///   (no `create_new` + `posix_fallocate` on the journal thread),
    /// - the segment's extents are already *written*, so subsequent
    ///   appends generate no extent-conversion metadata and
    ///   `flush_batch_sync`'s `fdatasync` never has to force the
    ///   filesystem journal (the ~2 ms periodic pipeline freeze
    ///   documented in `docs/internal/journal-fsync-beat-2026-08.md`).
    ///
    /// On error the prepared file is consumed (renamed onto the live
    /// path then rolled back, or left as staging for the next preparer
    /// cycle to reclaim). Callers should re-arm the preparer after a
    /// successful return so the next rotation can also be fast.
    pub fn rotate_segment_with_prepared(
        &mut self,
        prepared: crate::preparer::PreparedSegment,
    ) -> Result<PathBuf, JournalError> {
        self.rotate_segment_inner(Some(prepared))
    }

    /// Shared rotation body. `prepared.is_some()` takes the fast path.
    ///
    /// Order matters across the two halves: flush first so the outgoing
    /// segment holds everything encoded, take the boundary values from
    /// the stream half, let the file half swap the segment, and
    /// re-anchor the stream only once that succeeded — on failure the
    /// live file is restored and the stream must keep describing it.
    fn rotate_segment_inner(
        &mut self,
        prepared: Option<crate::preparer::PreparedSegment>,
    ) -> Result<PathBuf, JournalError> {
        self.flush_batch_sync()?;

        let next_seq = self.encoder.next_sequence();
        // The new segment's header anchor is the outgoing segment's tail
        // chain hash, giving recovery a verifiable cross-segment link.
        // Zeros when hash-chain is disabled (nothing verifies them).
        let anchor = self.encoder.chain_hash().unwrap_or([0u8; 32]);

        let archived = self.segment.rotate(prepared, next_seq, anchor)?;
        self.encoder.begin_segment(next_seq, anchor);
        Ok(archived)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prealloc::prealloc_chunk_bytes;
    use crate::reader::JournalReader;
    use crate::write::JournalWrite;
    use melin_app::CodecError;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct TestEvent(u64);

    impl AppEvent for TestEvent {
        const MAX_ENCODED_SIZE: usize = 8;

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

    /// First user-event sequence. Chain metadata lives in the file
    /// header, so sequence 1 is a real event under every feature config.
    const FIRST_SEQ: u64 = 1;

    fn sample(n: u64) -> JournalEvent<TestEvent> {
        JournalEvent::App(TestEvent(n))
    }

    fn read_all_payloads(path: &Path) -> Vec<u64> {
        let mut reader = JournalReader::<TestEvent>::open(path).unwrap();
        let mut out = Vec::new();
        while let Some(entry) = reader.next_entry().unwrap() {
            if let JournalEvent::App(e) = entry.event {
                out.push(e.0);
            }
        }
        out
    }

    #[test]
    fn create_writes_header_and_preallocates() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let writer = BufferedWriter::<TestEvent>::create(&path).unwrap();
        assert_eq!(writer.next_sequence(), FIRST_SEQ);
        assert_eq!(writer.path(), path);
        #[cfg(feature = "hash-chain")]
        assert!(writer.chain_hash().is_some());
        #[cfg(not(feature = "hash-chain"))]
        assert!(writer.chain_hash().is_none());

        let file_len = std::fs::metadata(&path).unwrap().len();
        assert!(
            file_len >= prealloc_chunk_bytes(),
            "expected pre-allocated file, got {file_len} bytes"
        );
    }

    #[test]
    fn create_fails_if_file_exists() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let w = BufferedWriter::<TestEvent>::create(&path).unwrap();
        drop(w);

        assert!(BufferedWriter::<TestEvent>::create(&path).is_err());
    }

    /// An application narrower than the transport's own 8-byte payloads.
    /// Its reservation is the smallest the journal ever makes, so it is
    /// where a reservation that forgot `Tick` shows up.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct TinyEvent;

    impl AppEvent for TinyEvent {
        const MAX_ENCODED_SIZE: usize = 1;

        fn encoded_size(&self) -> usize {
            1
        }
        fn encode(&self, buf: &mut [u8]) -> usize {
            buf[0] = 0x5A;
            1
        }
        fn decode(_buf: &[u8]) -> Result<Self, CodecError> {
            Ok(TinyEvent)
        }
        fn is_query(&self) -> bool {
            false
        }
    }

    /// The headroom kept ahead of the encoder has to cover the *widest*
    /// entry the journal can write, not just the app's own — a tick is
    /// journaled whatever `E` is.
    ///
    /// Fills the batch buffer down to a gap too small for a tick, then
    /// writes one. A reservation derived from the app's width alone
    /// leaves that gap looking sufficient, so the buffer is not grown and
    /// the tick is refused mid-batch.
    #[test]
    fn a_tick_fits_the_headroom_a_narrow_app_reserves() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tiny.journal");
        let mut w = BufferedWriter::<TinyEvent>::create(&path).unwrap();

        let tick = JournalEvent::Tick { now_ns: u64::MAX };
        let tick_len = {
            let mut probe = [0u8; crate::encoder::MAX_ENTRY_SIZE];
            codec::encode(1, 0, 0, 0, &tick, &mut probe).unwrap()
        };

        while BATCH_BUF_CAPACITY - w.encoder.batch_len() >= tick_len {
            let seq = w.allocate_sequence();
            w.encode_event(seq, 1_000, &JournalEvent::App(TinyEvent), 0, 0)
                .expect("narrow entry");
        }

        let seq = w.allocate_sequence();
        w.encode_event(seq, 2_000, &tick, 0, 0)
            .expect("a tick must always fit the reserved headroom");
    }

    #[test]
    fn append_assigns_sequential_ids() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let mut writer = BufferedWriter::<TestEvent>::create(&path).unwrap();
        let seq1 = writer.append(&sample(1)).unwrap();
        let seq2 = writer.append(&sample(2)).unwrap();
        let seq3 = writer.append(&sample(3)).unwrap();

        assert_eq!(seq1, FIRST_SEQ);
        assert_eq!(seq2, FIRST_SEQ + 1);
        assert_eq!(seq3, FIRST_SEQ + 2);
        assert_eq!(writer.next_sequence(), FIRST_SEQ + 3);
    }

    #[test]
    fn append_round_trips_through_reader() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let mut writer = BufferedWriter::<TestEvent>::create(&path).unwrap();
        for i in 1..=5u64 {
            writer.append(&sample(i)).unwrap();
        }
        drop(writer);

        assert_eq!(read_all_payloads(&path), vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn batch_append_then_flush_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let mut writer = BufferedWriter::<TestEvent>::create(&path).unwrap();
        for i in 1..=10u64 {
            writer.batch_append_with_ts(&sample(i), 0, 0, 0).unwrap();
        }
        // Before flush, no user data has reached disk past the header.
        // After flush, all ten entries land in one pwrite.
        writer.flush_batch_sync().unwrap();
        drop(writer);

        assert_eq!(read_all_payloads(&path), (1..=10).collect::<Vec<_>>());
    }

    #[test]
    fn discard_batch_clears_pending_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let mut writer = BufferedWriter::<TestEvent>::create(&path).unwrap();
        writer.batch_append_with_ts(&sample(1), 0, 0, 0).unwrap();
        writer.batch_append_with_ts(&sample(2), 0, 0, 0).unwrap();
        assert!(!writer.pending_batch_bytes().is_empty());

        writer.discard_batch_buf();
        assert!(
            writer.pending_batch_bytes().is_empty(),
            "discard must clear the pending batch buffer"
        );
        assert_eq!(
            writer.last_user_entry_replication_slice().len(),
            0,
            "discard must invalidate the last-user-entry slice"
        );
    }

    #[test]
    fn open_append_resumes_after_drop() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let mut writer = BufferedWriter::<TestEvent>::create(&path).unwrap();
        writer.append(&sample(1)).unwrap();
        writer.append(&sample(2)).unwrap();
        let last_seq = writer.next_sequence() - 1;
        let valid_end = writer.valid_end();
        drop(writer);

        let mut reopened =
            BufferedWriter::<TestEvent>::open_append(&path, last_seq, valid_end).unwrap();
        reopened.append(&sample(3)).unwrap();
        reopened.append(&sample(4)).unwrap();
        drop(reopened);

        assert_eq!(read_all_payloads(&path), vec![1, 2, 3, 4]);
    }

    #[test]
    fn rotate_segment_continues_sequence_in_new_live() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let mut writer = BufferedWriter::<TestEvent>::create(&path).unwrap();
        writer.append(&sample(1)).unwrap();
        writer.append(&sample(2)).unwrap();
        let seq_before_rotate = writer.next_sequence();

        let sealed_end = writer.valid_end();
        let archived = writer.rotate_segment().unwrap();
        assert!(archived.exists(), "archived segment {archived:?} missing");
        // Rotation consumes no sequence number — chain metadata lives in
        // the new segment's header, not in the entry stream.
        assert_eq!(writer.next_sequence(), seq_before_rotate);
        // The archive is compacted to its data end — no allocation
        // padding survives sealing (bitwise-mirror property).
        assert_eq!(
            std::fs::metadata(&archived).unwrap().len(),
            sealed_end,
            "archive must be truncated to its valid data"
        );

        writer.append(&sample(3)).unwrap();
        drop(writer);

        // The live file contains only the new user entry. The archive
        // holds the pre-rotation entries.
        assert_eq!(read_all_payloads(&path), vec![3]);
        assert_eq!(read_all_payloads(&archived), vec![1, 2]);
    }

    /// The prepared-adoption fast path must be observationally
    /// identical to the synchronous rotation: same archive layout, same
    /// sequence continuation, same readable entries, and the new live
    /// segment must resume the chain from the outgoing tail.
    #[test]
    fn rotate_with_prepared_matches_sync_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let mut writer = BufferedWriter::<TestEvent>::create(&path).unwrap();
        writer.append(&sample(1)).unwrap();
        writer.append(&sample(2)).unwrap();
        let seq_before_rotate = writer.next_sequence();

        // Stage a zero-filled segment the way the pipeline's preparer
        // would (small threshold keeps the test fast).
        let preparer = crate::preparer::SegmentPreparer::spawn(
            path.clone(),
            1024 * 1024,
            0,
            crate::preparer::StagingMode::ZeroFill,
        );
        let mut prepared = None;
        for _ in 0..500 {
            if let Some(p) = preparer.take() {
                prepared = Some(p);
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let prepared = prepared.expect("preparer should stage a segment within 5 s");
        let zeroed_end = prepared.allocated_end;

        let sealed_end = writer.valid_end();
        let archived = writer.rotate_segment_with_prepared(prepared).unwrap();
        assert!(archived.exists(), "archived segment {archived:?} missing");
        // Sealing compacts the archive to its data end regardless of
        // rotation path (bitwise-mirror property across nodes).
        assert_eq!(
            std::fs::metadata(&archived).unwrap().len(),
            sealed_end,
            "archive must be truncated to its valid data"
        );
        // Rotation consumes no sequence number.
        assert_eq!(writer.next_sequence(), seq_before_rotate);
        // The adopted segment's pre-zeroed region is the allocation —
        // appends must not immediately re-fallocate.
        assert_eq!(writer.segment.allocated_end(), zeroed_end);
        // The staging file is gone (renamed onto the live path).
        assert!(!crate::preparer::staging_path(&path).exists());

        writer.append(&sample(3)).unwrap();
        drop(writer);
        preparer.shutdown();

        // Same observable layout as the sync-rotation test above.
        assert_eq!(read_all_payloads(&path), vec![3]);
        assert_eq!(read_all_payloads(&archived), vec![1, 2]);
    }

    /// `StagingMode::Allocate` must be adoptable on exactly the same
    /// terms as `ZeroFill`. The two modes differ only in whether the
    /// staged extents are materialised, which is a performance property
    /// of the flush path — everything the writer and reader observe
    /// (allocation end, archive layout, sequence continuation, readable
    /// entries) has to be identical, or the mode is not a tuning knob
    /// but a behaviour change.
    #[test]
    fn rotate_with_prepared_allocate_mode_matches_zero_fill() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let mut writer = BufferedWriter::<TestEvent>::create(&path).unwrap();
        writer.append(&sample(1)).unwrap();
        writer.append(&sample(2)).unwrap();
        let seq_before_rotate = writer.next_sequence();

        let preparer = crate::preparer::SegmentPreparer::spawn(
            path.clone(),
            1024 * 1024,
            0,
            crate::preparer::StagingMode::Allocate,
        );
        let mut prepared = None;
        for _ in 0..500 {
            if let Some(p) = preparer.take() {
                prepared = Some(p);
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let prepared = prepared.expect("preparer should stage a segment within 5 s");
        let allocated_end = prepared.allocated_end;

        let sealed_end = writer.valid_end();
        let archived = writer.rotate_segment_with_prepared(prepared).unwrap();
        assert!(archived.exists(), "archived segment {archived:?} missing");
        assert_eq!(
            std::fs::metadata(&archived).unwrap().len(),
            sealed_end,
            "archive must be truncated to its valid data"
        );
        assert_eq!(writer.next_sequence(), seq_before_rotate);
        // The allocation the adopter inherits is the staged one, so the
        // first appends do not re-`fallocate` — the same guarantee the
        // zero-fill path gives, reached without the staging pass.
        assert_eq!(writer.segment.allocated_end(), allocated_end);
        assert!(!crate::preparer::staging_path(&path).exists());

        writer.append(&sample(3)).unwrap();
        drop(writer);
        preparer.shutdown();

        assert_eq!(read_all_payloads(&path), vec![3]);
        assert_eq!(read_all_payloads(&archived), vec![1, 2]);
    }

    /// A crash between prepared adoption and the first append must
    /// leave a parseable empty journal (header durable, zero entries).
    #[test]
    fn prepared_adoption_leaves_parseable_empty_live() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let mut writer = BufferedWriter::<TestEvent>::create(&path).unwrap();
        writer.append(&sample(7)).unwrap();

        let preparer = crate::preparer::SegmentPreparer::spawn(
            path.clone(),
            1024 * 1024,
            0,
            crate::preparer::StagingMode::ZeroFill,
        );
        let mut prepared = None;
        for _ in 0..500 {
            if let Some(p) = preparer.take() {
                prepared = Some(p);
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let prepared = prepared.expect("prepared segment");

        writer.rotate_segment_with_prepared(prepared).unwrap();
        // Simulate the crash: drop without appending.
        drop(writer);
        preparer.shutdown();

        assert_eq!(
            read_all_payloads(&path),
            Vec::<u64>::new(),
            "empty live segment must parse cleanly"
        );
    }

    #[test]
    fn flush_with_empty_buffer_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let mut writer = BufferedWriter::<TestEvent>::create(&path).unwrap();
        let pos_before = writer.write_pos();

        // Flush twice in a row — second call has nothing pending and
        // must neither error nor advance write_pos.
        writer.flush_batch_sync().unwrap();
        writer.flush_batch_sync().unwrap();
        assert_eq!(writer.write_pos(), pos_before);
    }

    /// Prepared rotation anchors the new segment on the outgoing tail:
    /// an empty just-rotated segment's chain value equals the anchor,
    /// which must be the pre-rotation chain hash.
    #[cfg(feature = "hash-chain")]
    #[test]
    fn chain_hash_continues_across_prepared_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let mut writer = BufferedWriter::<TestEvent>::create(&path).unwrap();
        writer.append(&sample(1)).unwrap();
        let chain_before = writer.chain_hash().unwrap();

        let preparer = crate::preparer::SegmentPreparer::spawn(
            path.clone(),
            1024 * 1024,
            0,
            crate::preparer::StagingMode::ZeroFill,
        );
        let mut prepared = None;
        for _ in 0..500 {
            if let Some(p) = preparer.take() {
                prepared = Some(p);
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        writer
            .rotate_segment_with_prepared(prepared.expect("prepared segment"))
            .unwrap();
        preparer.shutdown();

        assert_eq!(writer.chain_hash(), Some(chain_before));
    }

    #[cfg(feature = "hash-chain")]
    #[test]
    fn chain_hash_continues_across_open_append() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let mut writer = BufferedWriter::<TestEvent>::create(&path).unwrap();
        writer.append(&sample(1)).unwrap();
        writer.append(&sample(2)).unwrap();
        let chain_before = writer.chain_hash().unwrap();
        let last_seq = writer.next_sequence() - 1;
        let valid_end = writer.valid_end();
        drop(writer);

        let reopened =
            BufferedWriter::<TestEvent>::open_append(&path, last_seq, valid_end).unwrap();

        // Without any new events, the chain hash must reproduce the
        // value captured before close — proves the self-contained
        // rebuild (header anchor + raw byte re-absorption) matches the
        // never-crashed writer's state.
        assert_eq!(reopened.chain_hash(), Some(chain_before));
    }

    #[test]
    fn last_user_entry_replication_slice_excludes_magic_and_crc() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let mut writer = BufferedWriter::<TestEvent>::create(&path).unwrap();
        writer.batch_append_with_ts(&sample(42), 0, 0, 0).unwrap();

        // The full encoded entry is [magic(2) | header | payload | CRC(4)].
        // The replication slice strips the leading magic and trailing CRC.
        let full = writer.encoder.last_user_entry_bytes(&writer.batch_buf);
        let repl = writer.last_user_entry_replication_slice();
        assert_eq!(repl.len(), full.len() - 6);
        assert_eq!(repl, &full[2..full.len() - 4]);
    }

    /// Garbage past `valid_end` from a torn pre-crash write must not
    /// resurface as decodable entries after `open_append`. We construct
    /// the scenario by appending one batch, capturing its `valid_end`,
    /// appending more, then dropping without flushing — but since the
    /// buffered writer flushes per `batch_append + sync` we instead
    /// simulate the torn-write by pwriting raw garbage past `valid_end`
    /// before reopening. The reopen path must scrub it.
    #[test]
    fn open_append_scrubs_garbage_past_valid_end() {
        use std::os::unix::fs::FileExt;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let mut writer = BufferedWriter::<TestEvent>::create(&path).unwrap();
        writer.append(&sample(11)).unwrap();
        writer.append(&sample(22)).unwrap();
        let valid_end = writer.valid_end();
        let last_seq = writer.next_sequence() - 1;
        drop(writer);

        // Splat 4 KiB of plausibly-magic-looking garbage past valid_end.
        // The journal magic is 0x4A 0x45 ("JE"); we fabricate a frame
        // header that would pass a naive scan: magic + plausible length.
        let mut garbage = vec![0xFFu8; 4096];
        garbage[0] = 0x4A;
        garbage[1] = 0x45;
        garbage[2] = 0x10; // length low byte — fake non-zero length
        garbage[3] = 0x00;
        let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.write_all_at(&garbage, valid_end).unwrap();
        file.sync_all().unwrap();
        drop(file);

        // Reopen via `open_append`. The fix must truncate the file so
        // the garbage is gone; otherwise a subsequent reader would
        // either fail with a CRC error past `valid_end` or worse, treat
        // the garbage as a valid frame.
        let reopened =
            BufferedWriter::<TestEvent>::open_append(&path, last_seq, valid_end).unwrap();
        drop(reopened);

        // Fresh reader: must see exactly the two pre-crash entries —
        // no extra frames decoded from the garbage.
        let payloads = read_all_payloads(&path);
        assert_eq!(payloads, vec![11, 22]);
    }

    /// Cross-segment chain continuity: after `rotate_segment`, the new
    /// segment's header anchor must equal the live segment's chain hash
    /// at the rotation moment. Without this, multi-segment recovery
    /// would report `SegmentChainBreak` against a journal that's
    /// actually intact.
    #[cfg(feature = "hash-chain")]
    #[test]
    fn rotate_segment_anchors_new_header_to_pre_rotate_tail() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let mut writer = BufferedWriter::<TestEvent>::create(&path).unwrap();
        writer.append(&sample(7)).unwrap();
        writer.append(&sample(8)).unwrap();
        let pre_rotate_chain = writer.chain_hash().expect("hash-chain enabled");
        let seq_at_rotate = writer.next_sequence();

        let archived = writer.rotate_segment().unwrap();
        assert!(archived.exists());

        // The new live segment's header carries the anchor + starting
        // sequence — no entries need to be read.
        let info = crate::segment::read_header_info(&path).unwrap();
        assert_eq!(
            info.anchor_hash, pre_rotate_chain,
            "new segment's anchor must equal the pre-rotation tail",
        );
        assert_eq!(info.starting_sequence, seq_at_rotate);

        // An empty segment's chain value is its anchor — the in-memory
        // writer agrees with the on-disk header.
        assert_eq!(writer.chain_hash(), Some(pre_rotate_chain));
    }

    /// Mid-segment `open_append` rebuilds the chain from the header
    /// anchor plus the raw on-disk bytes. Asserts the resumed writer's
    /// chain matches what a never-crashed writer would have produced,
    /// and that it keeps evolving identically for subsequent appends.
    #[cfg(feature = "hash-chain")]
    #[test]
    fn open_append_mid_segment_rebuilds_chain_from_raw_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let mut writer = BufferedWriter::<TestEvent>::create(&path).unwrap();
        writer.append(&sample(1)).unwrap();
        writer.append(&sample(2)).unwrap();
        writer.append(&sample(3)).unwrap();
        let chain_no_crash = writer.chain_hash().unwrap();

        let valid_end = writer.valid_end();
        let last_seq = writer.next_sequence() - 1;
        drop(writer);

        let mut reopened =
            BufferedWriter::<TestEvent>::open_append(&path, last_seq, valid_end).unwrap();
        assert_eq!(reopened.chain_hash(), Some(chain_no_crash));

        // The rebuilt hasher must continue identically: append one more
        // event and compare against a reader walking the whole segment.
        reopened.append(&sample(4)).unwrap();
        let chain_after = reopened.chain_hash().unwrap();
        drop(reopened);

        let mut reader = crate::reader::JournalReader::<TestEvent>::open(&path).unwrap();
        while reader.next_entry().unwrap().is_some() {}
        assert_eq!(reader.chain_hash(), Some(chain_after));
    }
}
