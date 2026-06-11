//! Buffered journal writer — page-cache writes with explicit `fdatasync`.
//!
//! Alternative to [`crate::sector_writer::SectorWriter`] for deployments that
//! cannot rely on capacitor-backed power-loss protection on the storage
//! device. The on-disk format is identical to the O_DIRECT writer
//! (`crate::codec` framing), so [`crate::reader::JournalReader`] and
//! [`crate::segment`] recovery work without modification.
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
//! ## Why a separate writer
//!
//! The O_DIRECT writer carries machinery that exists *only* to satisfy
//! sector alignment: a partial-tail sector kept in memory, sector-rounded
//! `pwrite`, sector-aligned scratch buffers, sector-size detection. None
//! of it applies once writes go through the page cache, so duplicating
//! those code paths into a single struct with `#[cfg]`s tangles the hot
//! path. This module is the clean half: a `Vec<u8>` batch buffer, plain
//! `pwrite_all`, and `fdatasync` for durability.

use std::fs::{File, OpenOptions};
use std::marker::PhantomData;
use std::os::fd::AsFd;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use melin_app::AppEvent;

#[cfg(feature = "hash-chain")]
use crate::chain::SegmentChain;
use crate::codec::{self, ENTRY_OFFSET, FILE_HEADER_SIZE, MAX_SECTOR_SIZE};
use crate::error::JournalError;
use crate::event::JournalEvent;

/// Maximum encoded entry size. Mirrors `writer::MAX_ENTRY_SIZE` — actual
/// entries are ~81-101 bytes; the array is sized generously so the
/// per-event encode scratch never spills to the heap.
const MAX_ENTRY_SIZE: usize = 144;

/// Batch buffer capacity. Matches `writer::BATCH_BUF_CAPACITY` so the
/// pipeline's flush cadence (sized against the O_DIRECT writer) applies
/// here unchanged.
const BATCH_BUF_CAPACITY: usize = 512 * 1024;

/// Fixed on-disk offset of the first journal entry. Defined in the codec
/// (`ENTRY_OFFSET = MAX_SECTOR_SIZE = 4096`) so both writer variants
/// produce interchangeable layouts. Renamed locally for legibility.
const HEADER_OFFSET: u64 = ENTRY_OFFSET;

// Pre-allocation chunk size is resolved by the shared `prealloc` module
// so a switch between this writer and `SectorWriter` doesn't change
// disk-space cadence under matched configuration.
use crate::prealloc::prealloc_chunk_bytes;

/// Append-only journal writer that goes through the kernel page cache
/// and forces durability with `fdatasync` per flush.
pub struct BufferedWriter<E: AppEvent> {
    // PhantomData carries the app event type for the methods that
    // read/write `JournalEvent<E>`. Zero-size — no runtime cost.
    _marker: PhantomData<fn(E) -> E>,
    file: File,
    // Scratch buffer for single-entry encoding. Fixed-size array — entry
    // sizes are bounded, so avoiding a Vec lets the hot path stay
    // allocation-free.
    buffer: [u8; MAX_ENTRY_SIZE],
    // Batch write buffer. Plain Vec<u8> because the page-cache path has
    // no alignment requirement. Pre-reserved to BATCH_BUF_CAPACITY at
    // construction; flushing only resets `batch_len`, not capacity.
    batch_buf: Vec<u8>,
    // Number of valid bytes in `batch_buf`. Acts as the write cursor —
    // new entries land at `batch_buf[batch_len..]`. Reset on every flush.
    batch_len: usize,
    next_sequence: u64,
    // First sequence of the active segment (the header's
    // `starting_sequence`), kept in memory so emptiness / rotation-
    // boundary checks need no header re-read.
    starting_sequence: u64,
    path: PathBuf,
    // Byte offset of the next entry to be written. Always points at the
    // end of valid data; no sector-tail bookkeeping, so `valid_end() ==
    // write_pos`.
    write_pos: u64,
    // Byte offset of the end of pre-allocated space. When `write_pos`
    // approaches this, another `prealloc_chunk_bytes()` is allocated.
    allocated_end: u64,
    #[cfg(feature = "hash-chain")]
    hash_chain: SegmentChain,
    // Debug-only monotonicity guard: every fresh seq must strictly
    // exceed this. Excluded from release builds — zero hot-path cost.
    #[cfg(debug_assertions)]
    last_encoded_seq: u64,
    // Byte range of the most-recent user entry within `batch_buf` —
    // `last_user_entry_replication_slice` ships it to replication
    // without a second encode pass.
    last_user_entry_offset: usize,
    last_user_entry_len: usize,
}

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
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;

        // Pre-allocate the first chunk so flushes don't pay extent-tree
        // growth latency for a while. ext4/xfs/btrfs all back this with
        // unwritten extents (no zero-fill cost) on the supported targets.
        let allocated_end = fallocate_chunk(&file, 0)?;

        // Write the file header at offset 0. The codec reserves the
        // first `MAX_SECTOR_SIZE` (= 4096) bytes for the header
        // regardless of writer mode or device sector size, so a
        // journal created here is layout-compatible with SectorWriter.
        let mut header_buf = [0u8; MAX_SECTOR_SIZE];
        codec::encode_file_header(
            &mut header_buf,
            MAX_SECTOR_SIZE,
            starting_sequence,
            anchor_hash,
        );
        write_all_at(&file, &header_buf, 0)?;

        // Flush the header durably before returning. Subsequent batch
        // flushes layer on top of a known-good header — a crash before
        // the next user write still leaves a parseable empty journal.
        file.sync_all()?;

        Ok(Self {
            _marker: PhantomData,
            file,
            buffer: [0u8; MAX_ENTRY_SIZE],
            batch_buf: vec![0u8; BATCH_BUF_CAPACITY],
            batch_len: 0,
            next_sequence: starting_sequence,
            starting_sequence,
            path: path.to_path_buf(),
            write_pos: HEADER_OFFSET,
            allocated_end,
            #[cfg(feature = "hash-chain")]
            hash_chain: SegmentChain::new(anchor_hash),
            #[cfg(debug_assertions)]
            last_encoded_seq: 0,
            last_user_entry_offset: 0,
            last_user_entry_len: 0,
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
        crate::preparer::cleanup_staging_orphan(path);
        let file = OpenOptions::new().read(true).write(true).open(path)?;

        // Validate the file header and extract the chain anchor. A
        // header that fails to decode means the file isn't a journal —
        // bail rather than overwrite it.
        let mut header_buf = [0u8; FILE_HEADER_SIZE];
        let n = file.read_at(&mut header_buf, 0)?;
        if n < FILE_HEADER_SIZE {
            return Err(JournalError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "journal file too short to read file header",
            )));
        }
        let info = codec::decode_file_header(&header_buf)?;

        // Truncate down to `valid_end` so any torn-write garbage past
        // it is gone before we resume appending. Without this, the
        // bytes between `valid_end` and the previous file length
        // survive on disk; subsequent readers (or offline tooling)
        // could mistake them for entries if they happen to start with
        // the journal magic. The CRC check would catch the lie, but
        // SectorWriter scrubs its tail sector for the same reason and
        // BufferedWriter should match the defensive posture. Truncate
        // then re-fallocate to restore the chunk-ahead allocation;
        // the kernel zero-fills the freshly extended region.
        let pre_truncate_len = file.metadata()?.len();
        if pre_truncate_len > valid_end {
            file.set_len(valid_end)?;
        }
        let allocated_end = fallocate_chunk(&file, valid_end)?;
        file.sync_all()?;

        Ok(Self {
            _marker: PhantomData,
            file,
            buffer: [0u8; MAX_ENTRY_SIZE],
            batch_buf: vec![0u8; BATCH_BUF_CAPACITY],
            batch_len: 0,
            next_sequence: last_seq + 1,
            starting_sequence: info.starting_sequence,
            path: path.to_path_buf(),
            write_pos: valid_end,
            allocated_end,
            #[cfg(feature = "hash-chain")]
            hash_chain: SegmentChain::rebuild_from_file(
                path,
                info.anchor_hash,
                ENTRY_OFFSET,
                valid_end,
            )?,
            #[cfg(debug_assertions)]
            last_encoded_seq: last_seq,
            last_user_entry_offset: 0,
            last_user_entry_len: 0,
        })
    }

    /// Allocate and return the next sequence number, advancing the
    /// internal counter.
    pub fn allocate_sequence(&mut self) -> u64 {
        let seq = self.next_sequence;
        self.next_sequence += 1;
        seq
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
        #[cfg(debug_assertions)]
        {
            debug_assert!(
                seq > self.last_encoded_seq,
                "encode_event: seq {seq} <= last_encoded_seq {} — \
                 this would emit a duplicate/backward sequence",
                self.last_encoded_seq
            );
            self.last_encoded_seq = seq;
        }

        let written = codec::encode(
            seq,
            timestamp_ns,
            key_hash,
            request_seq,
            event,
            &mut self.buffer,
        )?;

        // Absorb the full on-disk bytes (incl. CRC) — see crate::chain
        // for why the CRC is included.
        #[cfg(feature = "hash-chain")]
        self.hash_chain.absorb(&self.buffer[..written]);

        self.reserve_batch(written);
        let offset = self.batch_len;
        self.last_user_entry_offset = offset;
        self.batch_buf[offset..offset + written].copy_from_slice(&self.buffer[..written]);
        self.last_user_entry_len = written;
        self.batch_len += written;

        Ok(())
    }

    /// Grow the batch buffer if the incoming bytes wouldn't fit. The
    /// pre-reserved capacity covers the pipeline's normal flush cadence,
    /// so this is the rare oversize-batch fallback — Vec's amortised
    /// growth absorbs the cost.
    #[inline]
    fn reserve_batch(&mut self, adding: usize) {
        let needed = self.batch_len + adding;
        if needed > self.batch_buf.len() {
            tracing::warn!(
                current_len = self.batch_len,
                adding,
                capacity = self.batch_buf.len(),
                "buffered journal batch exceeded preallocated capacity — \
                 caller is batching more than capacity between flushes; \
                 raise BATCH_BUF_CAPACITY or flush more often"
            );
            self.batch_buf.resize(needed, 0);
        }
    }

    /// Write the accumulated batch and force it to stable media.
    ///
    /// Issues exactly one `pwrite` covering the whole batch, followed by
    /// `fdatasync`. Returns only when the kernel reports data is durable.
    pub fn flush_batch_sync(&mut self) -> Result<(), JournalError> {
        if self.batch_len == 0 {
            return Ok(());
        }
        self.ensure_allocated()?;

        let len = self.batch_len;
        write_all_at(&self.file, &self.batch_buf[..len], self.write_pos)?;

        // Honest durability: the call doesn't return until the kernel
        // reports the data is on stable media. On a drive with a
        // volatile write cache this issues a device-side flush; on a
        // PLP drive (VWC=0) the flush is a near-no-op.
        self.file.sync_data()?;

        self.write_pos += len as u64;
        self.batch_len = 0;
        self.last_user_entry_len = 0;
        Ok(())
    }

    /// Drop the pending batch without writing it. Used by the
    /// `no-persist` path of the journal stage to keep the buffer
    /// bounded after replication has snapshotted the bytes.
    pub fn discard_batch_buf(&mut self) {
        self.batch_len = 0;
        self.last_user_entry_len = 0;
    }

    pub fn next_sequence(&self) -> u64 {
        self.next_sequence
    }

    /// Set the next sequence number — used by the replica receiver to
    /// keep the writer's counter aligned with primary-assigned sequences.
    pub fn set_next_sequence(&mut self, seq: u64) {
        debug_assert!(
            seq >= self.next_sequence,
            "set_next_sequence({seq}) moves counter backward from {}",
            self.next_sequence
        );
        self.next_sequence = seq;
    }

    /// Current byte offset of the next entry. Always equal to
    /// [`valid_end`](Self::valid_end) on the buffered writer — there's
    /// no in-memory partial sector, so the on-disk end and the logical
    /// end coincide.
    pub fn write_pos(&self) -> u64 {
        self.write_pos
    }

    /// Byte offset of the end of valid on-disk data. Identical to
    /// `write_pos` here — kept as a separate method so callers can
    /// substitute the buffered and O_DIRECT writers behind a common
    /// interface without changing semantics.
    pub fn valid_end(&self) -> u64 {
        self.write_pos
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Decoded file-header fields of the live segment (read from disk).
    /// Used at primary startup to hand replicas the segment's
    /// `(starting_sequence, anchor_hash)` so a fresh replica journal is
    /// byte-identical from the segment's first entry onward.
    pub fn read_header_info(&self) -> Result<codec::FileHeaderInfo, JournalError> {
        crate::segment::read_header_info(&self.path)
    }

    /// First sequence of the active segment (the header's
    /// `starting_sequence`). `next_sequence() == segment_starting_sequence()`
    /// means the live segment is empty.
    pub fn segment_starting_sequence(&self) -> u64 {
        self.starting_sequence
    }

    /// Current chain value: `BLAKE3(entry bytes so far || anchor)`, or
    /// the anchor itself for an empty segment. `None` when `hash-chain`
    /// is disabled. Non-destructive (clone + finalize).
    pub fn chain_hash(&self) -> Option<[u8; 32]> {
        #[cfg(feature = "hash-chain")]
        {
            Some(self.hash_chain.value())
        }
        #[cfg(not(feature = "hash-chain"))]
        None
    }

    /// Encoded bytes that have been appended to the batch buffer but
    /// not yet flushed. Used by the journal stage to snapshot the
    /// pending bytes for replication.
    pub fn pending_batch_bytes(&self) -> &[u8] {
        &self.batch_buf[..self.batch_len]
    }

    /// Slice of the most-recent user entry, with the 2-byte magic
    /// stripped from the front and the 4-byte CRC stripped from the
    /// back — exact wire shape consumed by the replication stage.
    pub fn last_user_entry_replication_slice(&self) -> &[u8] {
        if self.last_user_entry_len == 0 {
            return &[];
        }
        let start = self.last_user_entry_offset;
        let end = start + self.last_user_entry_len;
        &self.batch_buf[start + 2..end - 4]
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
        self.flush_batch_sync()?;

        let path = self.path.clone();
        let next_seq = self.next_sequence;

        // The new segment's header anchor is the outgoing segment's tail
        // chain hash, giving recovery a verifiable cross-segment link.
        // Zeros when hash-chain is disabled (nothing verifies them).
        let anchor = self.chain_hash().unwrap_or([0u8; 32]);

        let archived = crate::segment::archive_live(&path)?;

        match Self::create_continuing(&path, next_seq, anchor) {
            Ok(new_writer) => {
                *self = new_writer;
                // Persist both the rename (archive_live) and the new
                // live file's dirent in a single dir fsync so recovery
                // sees a consistent post-rotation layout after a crash.
                crate::segment::fsync_parent_dir(&path)?;
                Ok(archived)
            }
            Err(e) => {
                // Best-effort rollback so the next recovery still finds
                // a live file at the canonical path. If rename-back
                // fails we surface the original error — recovery's
                // Phase B handles "archive present, no live" but the
                // in-process writer is unusable.
                if let Err(restore_err) = std::fs::rename(&archived, &path) {
                    tracing::warn!(
                        "rotate_segment: rename-back failed after create_continuing error: \
                         original={e}, restore={restore_err}"
                    );
                } else if let Err(fsync_err) = crate::segment::fsync_parent_dir(&path) {
                    // The rename succeeded but the dirent isn't durable
                    // yet. A crash here would leave recovery seeing the
                    // archive without the restored live, the same
                    // Phase-B state the success-path fsync protects
                    // against. Best-effort: log and surface the
                    // original error.
                    tracing::warn!(
                        "rotate_segment: dir fsync after rename-back failed: \
                         original={e}, fsync={fsync_err}"
                    );
                }
                Err(e)
            }
        }
    }

    /// Extend the file's pre-allocated region whenever the next write
    /// would land past it. Allocates one chunk at a time via
    /// `posix_fallocate` — extent allocation only, no zero-fill cost.
    fn ensure_allocated(&mut self) -> Result<(), JournalError> {
        let need = self.write_pos + self.batch_len as u64;
        if need <= self.allocated_end {
            return Ok(());
        }
        self.allocated_end = fallocate_chunk(&self.file, self.allocated_end)?;
        Ok(())
    }
}

/// Pre-allocate one chunk of disk blocks starting at `from`. Returns
/// the new end-of-allocation offset.
///
/// Allocates only the new range — not `[0, from + chunk)` — so the
/// fallocate call doesn't walk the entire extent tree on every
/// extension as the journal grows.
fn fallocate_chunk(file: &File, from: u64) -> Result<u64, JournalError> {
    let chunk = prealloc_chunk_bytes();
    rustix::fs::fallocate(
        file.as_fd(),
        rustix::fs::FallocateFlags::empty(),
        from,
        chunk,
    )
    .map_err(|e| JournalError::Io(std::io::Error::from_raw_os_error(e.raw_os_error())))?;
    Ok(from + chunk)
}

/// Write `buf` in full at `offset`, retrying short writes. `pwrite`
/// can return fewer bytes than requested on signal interruption or
/// when the kernel decides to split a large write; we loop until the
/// whole buffer is on its way.
fn write_all_at(file: &File, buf: &[u8], offset: u64) -> Result<(), JournalError> {
    let mut written = 0;
    while written < buf.len() {
        let n = file
            .write_at(&buf[written..], offset + written as u64)
            .map_err(JournalError::Io)?;
        if n == 0 {
            return Err(JournalError::Io(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "buffered journal write returned 0",
            )));
        }
        written += n;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::JournalReader;
    use crate::write::JournalWrite;
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

        let archived = writer.rotate_segment().unwrap();
        assert!(archived.exists(), "archived segment {archived:?} missing");
        // Rotation consumes no sequence number — chain metadata lives in
        // the new segment's header, not in the entry stream.
        assert_eq!(writer.next_sequence(), seq_before_rotate);

        writer.append(&sample(3)).unwrap();
        drop(writer);

        // The live file contains only the new user entry. The archive
        // holds the pre-rotation entries.
        assert_eq!(read_all_payloads(&path), vec![3]);
        assert_eq!(read_all_payloads(&archived), vec![1, 2]);
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
        let entry_start = writer.last_user_entry_offset;
        let entry_end = entry_start + writer.last_user_entry_len;
        let full = &writer.batch_buf[entry_start..entry_end];
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
