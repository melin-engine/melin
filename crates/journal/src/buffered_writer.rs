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

use crate::codec::{self, ENTRY_OFFSET, FILE_HEADER_SIZE, MAX_SECTOR_SIZE};
use crate::error::JournalError;
use crate::event::JournalEvent;
#[cfg(feature = "hash-chain")]
use crate::sector_writer::checkpoint_interval;
#[cfg(feature = "hash-chain")]
use melin_app::unix_epoch_nanos;

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
    path: PathBuf,
    // Byte offset of the next entry to be written. Always points at the
    // end of valid data; no sector-tail bookkeeping, so `valid_end() ==
    // write_pos`.
    write_pos: u64,
    // Byte offset of the end of pre-allocated space. When `write_pos`
    // approaches this, another `prealloc_chunk_bytes()` is allocated.
    allocated_end: u64,
    #[cfg(feature = "hash-chain")]
    hash_chain: Option<HashChain>,
    // Debug-only monotonicity guard: every fresh seq must strictly
    // exceed this. Excluded from release builds — zero hot-path cost.
    #[cfg(debug_assertions)]
    last_encoded_seq: u64,
    // Byte range of the most-recent user entry within `batch_buf` —
    // captured by `encode_event` before any auto-checkpoint emission so
    // `last_user_entry_replication_slice` returns the user entry only.
    last_user_entry_offset: usize,
    last_user_entry_len: usize,
}

/// Running BLAKE3 hash chain state. Mirrors the struct in
/// `writer.rs` — extracted as a private copy here to keep the buffered
/// writer free of any dependency on the O_DIRECT writer's internals.
#[cfg(feature = "hash-chain")]
struct HashChain {
    current_hash: [u8; 32],
    batch_hasher: blake3::Hasher,
    events_since_checkpoint: u64,
}

impl<E: AppEvent> BufferedWriter<E> {
    /// Create a fresh journal file with a random genesis hash.
    pub fn create(path: &Path) -> Result<Self, JournalError> {
        crate::preparer::cleanup_staging_orphan(path);
        #[cfg(feature = "hash-chain")]
        {
            let mut genesis = [0u8; 32];
            getrandom::fill(&mut genesis)
                .map_err(|e| JournalError::Io(std::io::Error::other(e.to_string())))?;
            Self::create_with_genesis(path, 1, genesis)
        }
        #[cfg(not(feature = "hash-chain"))]
        Self::create_without_chain(path, 1)
    }

    /// Create a fresh journal that continues a previous segment's sequence
    /// numbers, anchored to `genesis_hash` (the prior segment's chain tip).
    pub fn create_continuing(
        path: &Path,
        starting_sequence: u64,
        genesis_hash: [u8; 32],
    ) -> Result<Self, JournalError> {
        #[cfg(feature = "hash-chain")]
        {
            Self::create_with_genesis(path, starting_sequence, genesis_hash)
        }
        #[cfg(not(feature = "hash-chain"))]
        {
            let _ = genesis_hash;
            Self::create_without_chain(path, starting_sequence)
        }
    }

    #[cfg(feature = "hash-chain")]
    fn create_with_genesis(
        path: &Path,
        starting_sequence: u64,
        genesis: [u8; 32],
    ) -> Result<Self, JournalError> {
        let mut writer = Self::create_bare(path, starting_sequence)?;
        writer.emit_genesis_and_init_chain(genesis)?;
        Ok(writer)
    }

    #[cfg(not(feature = "hash-chain"))]
    fn create_without_chain(path: &Path, starting_sequence: u64) -> Result<Self, JournalError> {
        Self::create_bare(path, starting_sequence)
    }

    fn create_bare(path: &Path, starting_sequence: u64) -> Result<Self, JournalError> {
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
        // The meaningful fields occupy only the first ~8 bytes; the
        // rest is zero padding.
        let mut header_buf = [0u8; MAX_SECTOR_SIZE];
        codec::encode_file_header(&mut header_buf, MAX_SECTOR_SIZE);
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
            path: path.to_path_buf(),
            write_pos: HEADER_OFFSET,
            allocated_end,
            #[cfg(feature = "hash-chain")]
            hash_chain: None,
            #[cfg(debug_assertions)]
            last_encoded_seq: 0,
            last_user_entry_offset: 0,
            last_user_entry_len: 0,
        })
    }

    #[cfg(feature = "hash-chain")]
    fn emit_genesis_and_init_chain(&mut self, genesis: [u8; 32]) -> Result<(), JournalError> {
        let genesis_event: JournalEvent<E> = JournalEvent::GenesisHash { hash: genesis };
        let seq = self.next_sequence;
        let ts = unix_epoch_nanos();
        let written = codec::encode(seq, ts, 0, 0, &genesis_event, &mut self.buffer)?;

        // Initialize chain: hash the genesis entry bytes (excluding CRC).
        let entry_bytes = &self.buffer[..written - 4];
        let hash = blake3::hash(entry_bytes);
        self.hash_chain = Some(HashChain {
            current_hash: *hash.as_bytes(),
            batch_hasher: blake3::Hasher::new(),
            events_since_checkpoint: 0,
        });

        self.batch_buf[0..written].copy_from_slice(&self.buffer[..written]);
        self.last_user_entry_offset = 0;
        self.last_user_entry_len = written;
        self.batch_len = written;
        self.next_sequence += 1;
        self.flush_batch_sync()
    }

    /// Open an existing journal for appending after recovery.
    ///
    /// `last_seq` is the sequence number of the last valid entry seen
    /// by the reader. `valid_end` is the byte offset immediately past
    /// that entry — new entries are written starting here, overwriting
    /// any trailing garbage from a partial crash write.
    pub fn open_append(
        path: &Path,
        last_seq: u64,
        valid_end: u64,
        #[cfg_attr(not(feature = "hash-chain"), allow(unused_variables))] chain_hash: Option<
            [u8; 32],
        >,
        #[cfg_attr(not(feature = "hash-chain"), allow(unused_variables))]
        events_since_checkpoint: u64,
    ) -> Result<Self, JournalError> {
        crate::preparer::cleanup_staging_orphan(path);
        let file = OpenOptions::new().read(true).write(true).open(path)?;

        // Validate the file header is parseable. We don't actually need
        // the sector_size value (the buffered path has no alignment
        // requirement), but a header that fails to decode means the file
        // isn't a journal — bail rather than overwrite it.
        let mut header_buf = [0u8; FILE_HEADER_SIZE];
        let n = file.read_at(&mut header_buf, 0)?;
        if n < FILE_HEADER_SIZE {
            return Err(JournalError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "journal file too short to read file header",
            )));
        }
        codec::decode_file_header(&header_buf)?;

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

        #[allow(unused_mut)]
        let mut writer = Self {
            _marker: PhantomData,
            file,
            buffer: [0u8; MAX_ENTRY_SIZE],
            batch_buf: vec![0u8; BATCH_BUF_CAPACITY],
            batch_len: 0,
            next_sequence: last_seq + 1,
            path: path.to_path_buf(),
            write_pos: valid_end,
            allocated_end,
            // `events_since_checkpoint` is initialised to 0 here and
            // overwritten below by the reader-driven reconstruction
            // when the caller indicates mid-segment recovery. The
            // chain-hash parameter is only authoritative when the
            // resume lands exactly on a checkpoint boundary.
            #[cfg(feature = "hash-chain")]
            hash_chain: chain_hash.map(|h| HashChain {
                current_hash: h,
                batch_hasher: blake3::Hasher::new(),
                events_since_checkpoint: 0,
            }),
            #[cfg(debug_assertions)]
            last_encoded_seq: last_seq,
            last_user_entry_offset: 0,
            last_user_entry_len: 0,
        };

        // Mid-segment resume: the caller's `chain_hash` is the running
        // hash including unfinalised entries since the last checkpoint,
        // which doesn't decompose into (current_hash, batch_hasher)
        // arithmetically. To rebuild a hasher whose next checkpoint
        // matches what the writer would have produced without a crash,
        // we re-read the segment via JournalReader — which carries the
        // same accumulation rule — and adopt its (current_hash,
        // hasher, count) tuple. Without this, the next emitted
        // Checkpoint would carry a hash that disagrees with the
        // pre-crash invariant and verification would fail.
        #[cfg(feature = "hash-chain")]
        if events_since_checkpoint > 0
            && let Some(chain) = &mut writer.hash_chain
        {
            let mut reader = crate::reader::JournalReader::<E>::open(path)?;
            while reader.next_entry()?.is_some() {}
            if let Some((raw_hash, hasher, count)) = reader.take_chain_state() {
                chain.current_hash = raw_hash;
                chain.batch_hasher = hasher;
                chain.events_since_checkpoint = count;
            }
        }

        Ok(writer)
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
    /// on a replica). Also handles hash-chain bookkeeping and auto-emits
    /// a checkpoint entry when the interval is reached.
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

        #[cfg(feature = "hash-chain")]
        if let Some(chain) = &mut self.hash_chain {
            let entry_bytes_len = written - 4; // exclude CRC
            chain.batch_hasher.update(&self.buffer[..entry_bytes_len]);
            if !matches!(event, JournalEvent::GenesisHash { .. }) {
                chain.events_since_checkpoint += 1;
            }
        }

        self.reserve_batch(written);
        let offset = self.batch_len;
        self.last_user_entry_offset = offset;
        self.batch_buf[offset..offset + written].copy_from_slice(&self.buffer[..written]);
        self.last_user_entry_len = written;
        self.batch_len += written;

        // Auto-emit a checkpoint at the interval boundary.
        #[cfg(feature = "hash-chain")]
        if let Some(chain) = &mut self.hash_chain
            && chain.events_since_checkpoint >= checkpoint_interval()
        {
            chain.batch_hasher.update(&chain.current_hash);
            let checkpoint_hash = *chain.batch_hasher.finalize().as_bytes();
            chain.current_hash = checkpoint_hash;
            chain.batch_hasher = blake3::Hasher::new();
            let count = chain.events_since_checkpoint;
            self.emit_checkpoint(checkpoint_hash, count)?;
        }

        Ok(())
    }

    #[cfg(feature = "hash-chain")]
    fn emit_checkpoint(
        &mut self,
        chain_hash: [u8; 32],
        events_since_checkpoint: u64,
    ) -> Result<(), JournalError> {
        let checkpoint: JournalEvent<E> = JournalEvent::Checkpoint {
            chain_hash,
            events_since_checkpoint,
        };
        let seq = self.next_sequence;
        #[cfg(debug_assertions)]
        {
            debug_assert!(
                seq > self.last_encoded_seq,
                "emit_checkpoint: seq {seq} <= last_encoded_seq {}",
                self.last_encoded_seq
            );
            self.last_encoded_seq = seq;
        }
        let ts = unix_epoch_nanos();
        let written = codec::encode(seq, ts, 0, 0, &checkpoint, &mut self.buffer)?;

        if let Some(chain) = &mut self.hash_chain {
            chain.events_since_checkpoint = 0;
        }

        self.reserve_batch(written);
        self.batch_buf[self.batch_len..self.batch_len + written]
            .copy_from_slice(&self.buffer[..written]);
        self.batch_len += written;
        self.next_sequence += 1;
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

    /// Read the genesis entry bytes from the journal header. Mirrors
    /// [`crate::sector_writer::SectorWriter::read_genesis_entry`]; the
    /// fixed [`FILE_HEADER_SIZE`] offset replaces the sector-size
    /// constant used there. Used at primary startup to forward the
    /// genesis frame to replicas so the BLAKE3 chain seeds from the
    /// same bytes on both nodes.
    pub fn read_genesis_entry(&self) -> Result<Vec<u8>, JournalError> {
        let file = std::fs::File::open(&self.path)?;
        let offset = HEADER_OFFSET;
        let mut hdr4 = [0u8; 4];
        let n = file.read_at(&mut hdr4, offset)?;
        if n < 4 {
            return Err(JournalError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "journal too short to contain genesis entry",
            )));
        }
        let entry_len = u16::from_le_bytes([hdr4[2], hdr4[3]]) as usize;
        // EntryHeader (20) + payload + CRC (4) — same on-disk frame as
        // SectorWriter, only the file-header prefix differs.
        let total = 20 + entry_len + 4;
        let mut entry = vec![0u8; total];
        let n = file.read_at(&mut entry, offset)?;
        if n < total {
            return Err(JournalError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "journal truncated at genesis entry",
            )));
        }
        Ok(entry)
    }

    /// Current BLAKE3 chain hash if hash-chain is active. Includes any
    /// entries accumulated since the last checkpoint by cloning the
    /// batch hasher and finalising with the previous chain hash —
    /// non-destructive.
    pub fn chain_hash(&self) -> Option<[u8; 32]> {
        #[cfg(feature = "hash-chain")]
        {
            self.hash_chain.as_ref().map(|c| {
                if c.events_since_checkpoint == 0 {
                    c.current_hash
                } else {
                    let mut h = c.batch_hasher.clone();
                    h.update(&c.current_hash);
                    *h.finalize().as_bytes()
                }
            })
        }
        #[cfg(not(feature = "hash-chain"))]
        None
    }

    pub fn events_since_checkpoint(&self) -> u64 {
        #[cfg(feature = "hash-chain")]
        {
            self.hash_chain
                .as_ref()
                .map_or(0, |c| c.events_since_checkpoint)
        }
        #[cfg(not(feature = "hash-chain"))]
        0
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
    /// at the original path seeded with `GenesisHash(prev_chain_hash)`.
    /// Returns the path of the archived segment.
    pub fn rotate_segment(&mut self) -> Result<PathBuf, JournalError> {
        self.flush_batch_sync()?;

        let path = self.path.clone();
        let next_seq = self.next_sequence;

        // GenesisHash carries the chain state at the rotation boundary
        // so the new segment's chain anchors to the previous one.
        // Mirrors writer.rs's invariant: with hash-chain enabled,
        // chain_hash() must be Some after a successful flush; a None
        // here would silently anchor to zeros.
        #[allow(clippy::manual_unwrap_or_default)]
        let genesis = match self.chain_hash() {
            Some(h) => h,
            None => {
                #[cfg(feature = "hash-chain")]
                {
                    return Err(JournalError::Io(std::io::Error::other(
                        "rotate_segment: hash-chain enabled but chain_hash() is None — \
                         refusing to rotate with zero genesis",
                    )));
                }
                #[cfg(not(feature = "hash-chain"))]
                {
                    [0u8; 32]
                }
            }
        };

        let archived = crate::segment::archive_live(&path)?;

        match Self::create_continuing(&path, next_seq, genesis) {
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

    /// First user-event sequence: 2 with hash-chain (genesis takes 1), 1 without.
    #[cfg(feature = "hash-chain")]
    const FIRST_SEQ: u64 = 2;
    #[cfg(not(feature = "hash-chain"))]
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
        // Before flush, no user data has reached disk past the header
        // + genesis. After flush, all ten entries land in one pwrite.
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
        let chain_hash = writer.chain_hash();
        let events_since_checkpoint = writer.events_since_checkpoint();
        drop(writer);

        let mut reopened = BufferedWriter::<TestEvent>::open_append(
            &path,
            last_seq,
            valid_end,
            chain_hash,
            events_since_checkpoint,
        )
        .unwrap();
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
        // With hash-chain, the new segment emits a GenesisHash entry
        // at `seq_before_rotate` and advances the counter by one.
        // Without hash-chain, no genesis is emitted and the counter
        // is unchanged across rotation.
        #[cfg(feature = "hash-chain")]
        assert_eq!(writer.next_sequence(), seq_before_rotate + 1);
        #[cfg(not(feature = "hash-chain"))]
        assert_eq!(writer.next_sequence(), seq_before_rotate);

        writer.append(&sample(3)).unwrap();
        drop(writer);

        // The live file contains only the new user entry (plus a
        // genesis under hash-chain, which `read_all_payloads`
        // filters out). The archive holds the pre-rotation entries.
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
        let events_since_checkpoint = writer.events_since_checkpoint();
        drop(writer);

        let reopened = BufferedWriter::<TestEvent>::open_append(
            &path,
            last_seq,
            valid_end,
            Some(chain_before),
            events_since_checkpoint,
        )
        .unwrap();

        // Without any new events, the chain hash must reproduce the
        // value captured before close — proves the resume seeded both
        // `current_hash` and `events_since_checkpoint` correctly.
        assert_eq!(reopened.chain_hash(), Some(chain_before));
        assert_eq!(reopened.events_since_checkpoint(), events_since_checkpoint);
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
        let chain_before = writer.chain_hash();
        let events_since_checkpoint = writer.events_since_checkpoint();
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
        let reopened = BufferedWriter::<TestEvent>::open_append(
            &path,
            last_seq,
            valid_end,
            chain_before,
            events_since_checkpoint,
        )
        .unwrap();
        drop(reopened);

        // Fresh reader: must see exactly the two pre-crash entries plus
        // any genesis (under hash-chain) — no extra frames decoded from
        // the garbage.
        let payloads = read_all_payloads(&path);
        assert_eq!(payloads, vec![11, 22]);
    }

    /// Cross-segment chain continuity: after `rotate_segment`, the new
    /// segment's GenesisHash payload must equal the live segment's
    /// chain hash at the rotation moment. Without this, multi-segment
    /// recovery would report `SegmentChainBreak` against a journal
    /// that's actually intact.
    #[cfg(feature = "hash-chain")]
    #[test]
    fn rotate_segment_anchors_new_genesis_to_pre_rotate_tail() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let mut writer = BufferedWriter::<TestEvent>::create(&path).unwrap();
        writer.append(&sample(7)).unwrap();
        writer.append(&sample(8)).unwrap();
        let pre_rotate_chain = writer.chain_hash().expect("hash-chain enabled");

        let archived = writer.rotate_segment().unwrap();
        assert!(archived.exists());

        // The reader exposes the segment's GenesisHash payload via
        // `genesis_payload`. Walking the new live segment is enough to
        // populate it.
        let mut reader = crate::reader::JournalReader::<TestEvent>::open(&path).unwrap();
        while reader.next_entry().unwrap().is_some() {}
        let genesis = reader
            .genesis_payload()
            .expect("new segment must carry a genesis hash anchor");
        assert_eq!(
            genesis, pre_rotate_chain,
            "new segment's genesis must anchor to the pre-rotation tail",
        );
    }

    /// Mid-segment `open_append` with `events_since_checkpoint > 0`
    /// must reconstruct the running chain hash by replaying via the
    /// reader. Asserts that the resumed writer's chain state matches
    /// what a never-crashed writer would have produced.
    #[cfg(feature = "hash-chain")]
    #[test]
    fn open_append_mid_segment_rebuilds_chain_via_reader_replay() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let mut writer = BufferedWriter::<TestEvent>::create(&path).unwrap();
        // Three appends mid-segment — well below `checkpoint_interval`
        // so `events_since_checkpoint > 0` at the snapshot point.
        writer.append(&sample(1)).unwrap();
        writer.append(&sample(2)).unwrap();
        writer.append(&sample(3)).unwrap();
        let chain_no_crash = writer.chain_hash().unwrap();
        let events_no_crash = writer.events_since_checkpoint();
        assert!(events_no_crash > 0, "test setup: must be mid-segment");

        let valid_end = writer.valid_end();
        let last_seq = writer.next_sequence() - 1;
        drop(writer);

        // Resume — open_append must replay the segment via the reader
        // to seed the chain state, since the running hash includes
        // unfinalised entries that can't decompose arithmetically.
        let reopened = BufferedWriter::<TestEvent>::open_append(
            &path,
            last_seq,
            valid_end,
            // The caller's chain_hash is the running hash including
            // unfinalised entries — supply it; open_append replaces it
            // with the reader-derived state.
            Some(chain_no_crash),
            events_no_crash,
        )
        .unwrap();
        assert_eq!(reopened.chain_hash(), Some(chain_no_crash));
        assert_eq!(reopened.events_since_checkpoint(), events_no_crash);
    }
}
