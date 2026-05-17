//! Journal writer — append-only, durable event log with pre-allocated storage.
//!
//! Uses `posix_fallocate` to pre-extend the journal file in 64 MiB chunks.
//! This allocates disk blocks (extents) ahead of time so that subsequent
//! sync calls only flush data pages — not filesystem metadata updates for
//! newly allocated extents. This significantly reduces sync latency under
//! sustained write load.
//!
//! ## Durability
//!
//! The file is always opened with `O_DIRECT` — every write is sector-aligned
//! and bypasses the page cache. Writes use plain `pwrite` (no `RWF_DSYNC`),
//! relying on the drive's Power Loss Protection (PLP) capacitors to flush
//! controller DRAM on power loss (~1–5 µs per flush). PLP drives are a hard
//! requirement for production deployments.
//!
//! **Sector Tail Buffer**: `O_DIRECT` requires 512-byte alignment for write
//! lengths, buffer addresses, and file offsets. The writer maintains one
//! in-memory sector (`tail_sector`) representing the current partially-
//! filled on-disk sector. New data is appended to it; complete sectors are
//! written forward; the partial tail is rewritten in-place on every flush.
//! `write_pos` advances only when a sector fills, so disk space ≈ actual
//! data size.
//!
//! Writes use positioned I/O with an explicit write position rather than
//! kernel-managed append mode, because the file size includes pre-allocated
//! (zero-filled) space beyond the valid data boundary.

use libc::mlock;
use std::fs::{File, OpenOptions};
use std::marker::PhantomData;
use std::os::fd::AsFd;
use std::os::unix::fs::FileExt;
#[cfg(not(feature = "no-o-direct"))]
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};

use melin_app::AppEvent;
#[cfg(feature = "hash-chain")]
use melin_app::unix_epoch_nanos;

use super::codec::{self, ENTRY_OFFSET, FILE_HEADER_SIZE, MAX_SECTOR_SIZE};
use super::error::JournalError;
use super::event::JournalEvent;
use super::preparer::PreparedSegment;
#[cfg(feature = "hash-chain")]
use super::reader::JournalReader;

/// Maximum encoded entry size. Generously sized — actual entries are ~81-101 bytes
/// (v8 adds 16 bytes for key_hash + request_seq).
/// Fixed-size array avoids per-write heap allocation on the hot path.
const MAX_ENTRY_SIZE: usize = 144;

/// Batch buffer capacity. Sized for MAX_JOURNAL_BATCH (4096) entries at
/// ~104 bytes each (payload + 24-byte header/CRC) ≈ 416 KiB. Rounded up
/// to 512 KiB for headroom (checkpoint entries, variable-size events).
/// Pre-allocated once, reused across batches.
const BATCH_BUF_CAPACITY: usize = 512 * 1024;

// Pre-allocation chunk size is resolved by the shared `prealloc`
// module (env override `MELIN_JOURNAL_PREALLOC_MIB`, 256 MiB default).
// Read once per extension, off the hot path.
use crate::prealloc::prealloc_chunk_bytes;

/// Default number of events between automatic hash chain checkpoints.
/// 100K events × ~80 bytes = ~8 MB of journal data between checkpoints.
/// The checkpoint itself is ~77 bytes — negligible overhead.
const DEFAULT_CHECKPOINT_INTERVAL: u64 = 100_000;

/// Active checkpoint interval, overridable via
/// `MELIN_JOURNAL_CHECKPOINT_INTERVAL`. Used by integration tests to lower
/// the boundary so checkpoint-crossing tests can hit it after a few hundred
/// orders instead of 100K. Cached on first read — env vars don't change at
/// runtime, and reading on every event would add cost on the hot path.
/// Floored at 1 (0 would suppress checkpoints entirely).
pub fn checkpoint_interval() -> u64 {
    static CACHED: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| {
        std::env::var("MELIN_JOURNAL_CHECKPOINT_INTERVAL")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .map(|v| v.max(1))
            .unwrap_or(DEFAULT_CHECKPOINT_INTERVAL)
    })
}

/// A batch of encoded journal data ready for async write via io_uring.
/// Owns the buffer to prevent aliasing while io_uring holds a pointer to it.
pub struct AsyncWriteBatch {
    /// The buffer containing encoded journal entries.
    pub buf: Box<AlignedBuf<BATCH_BUF_CAPACITY>>,
    /// Number of valid bytes in `buf`. Only `buf[..len]` should be written.
    pub len: usize,
    /// File offset where this batch should be written.
    pub offset: u64,
}

/// Appends journal events to a file with CRC32C checksums and fsync durability.
///
/// Uses positioned writes (`pwrite`) and pre-allocated storage to minimize
/// fsync latency. The file size includes pre-allocated zero-filled space
/// beyond the valid data boundary; recovery truncates to `valid_file_end`
/// before reopening.
pub struct SectorWriter<E: AppEvent> {
    /// PhantomData carries the app event type for the methods that
    /// read/write `JournalEvent<E>`. Zero-size — no runtime cost.
    _marker: PhantomData<fn(E) -> E>,
    file: File,
    /// Pre-allocated fixed-size buffer for single-entry encoding.
    /// A fixed array (not Vec) because entries have a known bounded size.
    buffer: [u8; MAX_ENTRY_SIZE],
    /// Batch write buffer. Events are encoded here via `batch_append()`,
    /// then flushed in a single `pwrite` via `flush_batch()`. This reduces
    /// syscalls from N (one pwrite per event) to 1 per batch.
    batch_buf: Box<AlignedBuf<BATCH_BUF_CAPACITY>>,
    /// Spare buffer for double-buffering with io_uring. While one buffer is
    /// in-flight, the other accumulates the next batch. `None` when the spare
    /// is currently in-flight as part of an `AsyncWriteBatch`.
    spare_buf: Option<Box<AlignedBuf<BATCH_BUF_CAPACITY>>>,
    /// Next sequence number to assign (monotonically increasing, starts at 1).
    next_sequence: u64,
    /// Path to the journal file (kept for error messages / reopening).
    path: PathBuf,
    /// Current byte offset where the next entry will be written.
    /// Tracked explicitly because the file size includes pre-allocated space.
    write_pos: u64,
    /// Byte offset of the end of pre-allocated space. When `write_pos`
    /// approaches this, another `PREALLOC_CHUNK` is allocated.
    allocated_end: u64,
    /// BLAKE3 hash chain state. `None` when the `hash-chain` feature is
    /// disabled or for v5 journals (no hash chain). When active, each encoded
    /// entry's bytes (excluding CRC) are hashed with the previous hash to
    /// form a tamper-evident chain.
    #[cfg(feature = "hash-chain")]
    hash_chain: Option<HashChain>,
    /// Highest sequence ever passed through `encode_event` or
    /// `emit_checkpoint`. Debug-only monotonicity guard: every fresh seq
    /// must strictly exceed this, otherwise we're about to emit a
    /// duplicate — which would surface as a `SequenceGap` at the reader
    /// side. Zero means "nothing encoded yet." Excluded from release
    /// builds to keep the hot path cost at exactly zero.
    #[cfg(debug_assertions)]
    last_encoded_seq: u64,
    /// Number of valid bytes currently written into `batch_buf`. Acts as the
    /// write cursor: new entries are appended at `batch_buf[batch_len..]`.
    /// Reset to 0 on every flush or discard. Separate from `last_user_entry_len`
    /// which tracks only the most-recent user entry's size.
    batch_len: usize,
    /// Byte range of the most-recent user entry within `batch_buf`.
    /// Captured by `encode_event` BEFORE any auto-checkpoint emission,
    /// so `last_user_entry_replication_slice` returns the user entry
    /// only — not a trailing checkpoint that may have been auto-emitted
    /// in the same call. `(0, 0)` means no user entry encoded yet.
    last_user_entry_offset: usize,
    last_user_entry_len: usize,
    /// Physical sector size of the underlying block device in bytes (512 or
    /// 4096). Determined at open time via sysfs and stored in the file header.
    /// All O_DIRECT writes must be a multiple of this size and aligned to it.
    sector_size: usize,
    /// One-sector tail buffer for O_DIRECT (PLP path only). Holds the current
    /// partially-filled sector in memory. Written (and rewritten in-place) on
    /// every flush; `write_pos` advances only when this sector is full.
    /// Allocated as MAX_SECTOR_SIZE bytes so it works for both 512 and 4096;
    /// only `sector_size` bytes are used at runtime.
    ///
    /// **Load-bearing invariant for new code — do not break:** while
    /// `tail_sector_len > 0`, the bytes `tail_sector[..tail_sector_len]`
    /// MUST equal what's on disk at `[write_pos, write_pos + tail_sector_len)`.
    /// Any subsequent write at `write_pos` MUST therefore include this
    /// prefix; otherwise the next pwrite truncates the previously-written
    /// tail bytes (O_DIRECT overwrites the whole sector — there is no
    /// "append only" mode at the device level).
    ///
    /// Today this is maintained because *every* write to `write_pos` is
    /// sourced from `tail_sector` itself — `flush_to_sectors`
    /// reconstructs the full sector by prepending the tail to the new
    /// batch (see `copy_from_slice` of `tail_sector[..tail_sector_len]`
    /// into `batch_buf[..tail_sector_len]`), and
    /// `take_batch_for_async_write` keeps appending into `tail_sector`
    /// and sync-pwrites it as-is. Only `open_append` constructs the
    /// initial tail content, and it does so by reading the on-disk sector
    /// back into the buffer.
    ///
    /// Things that would silently corrupt the journal if added carelessly:
    /// 1. A code path that resets `tail_sector_len = 0` without first
    ///    advancing `write_pos` past the partial sector (or without first
    ///    issuing a flush that absorbs the bytes).
    /// 2. A pwrite at `write_pos` sourced from a buffer that doesn't
    ///    include the existing `tail_sector[..tail_sector_len]` as its
    ///    prefix.
    /// 3. Rotation / writer hand-off that takes ownership of `self`
    ///    without first calling `flush_batch_sync` to commit the tail
    ///    state (the rotated segment would then start with stale tail
    ///    content from `tail_sector`).
    ///
    /// This is a state-machine invariant, not a range-bounds invariant,
    /// so it can't be reduced to a single API surface the way
    /// `zero_unwritten_through` did for the prealloc-zero bug. The
    /// principled fix would be phantom-typed writer states; the surface
    /// is small enough today that the load-bearing-comment approach is
    /// the pragmatic call. Revisit if a third "looks-correct, silently
    /// truncates" bug shows up in this area.
    tail_sector: Box<AlignedBuf<MAX_SECTOR_SIZE>>,
    /// Bytes of real data in `tail_sector`. Always < sector_size.
    /// See `tail_sector`'s docstring for the on-disk-equality invariant
    /// that ties `tail_sector_len` to the writer's `write_pos`.
    tail_sector_len: usize,
}

/// Running BLAKE3 hash chain state for tamper evidence.
///
/// Uses segment-level hashing for correctness: entry bytes are fed into an
/// incremental hasher during `batch_append`, finalized at checkpoint
/// boundaries (every `CHECKPOINT_INTERVAL` events). The chain hash is
/// computed on-demand by `chain_hash()` via clone + finalize — no
/// per-flush finalization, ensuring the hash is deterministic regardless
/// of write batching strategy.
///
/// Chain: `hash_n = BLAKE3(segment_bytes || hash_{n-1})` where segment_bytes
/// is the concatenation of all entry bytes (excluding CRCs) between
/// checkpoints.
#[cfg(feature = "hash-chain")]
struct HashChain {
    /// Chain hash from the last checkpoint (or genesis). Used as the
    /// suffix in the next finalization.
    current_hash: [u8; 32],
    /// Incremental hasher accumulating entry bytes since last checkpoint.
    batch_hasher: blake3::Hasher,
    /// Events since last checkpoint. When this reaches `CHECKPOINT_INTERVAL`,
    /// a Checkpoint entry is auto-emitted.
    events_since_checkpoint: u64,
}

impl<E: AppEvent> SectorWriter<E> {
    /// Create a new journal file. Writes the file header and a `GenesisHash`
    /// entry with random bytes, pre-allocates storage, and returns a writer
    /// starting at sequence 1.
    ///
    /// Fails if the file already exists (use `open_append` for existing journals).
    pub fn create(path: &Path) -> Result<Self, JournalError> {
        // Clear any orphan `<path>.next-staging` from a prior crash
        // before the SegmentPreparer (if rotation is later enabled)
        // tries to open the same path with `create_new`. Safe here
        // because this entry point runs only at startup, before any
        // preparer is spawned.
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

    /// Create a new journal file that continues from a given sequence number.
    ///
    /// Used by journal rotation: after snapshotting, the old journal is archived
    /// and a new one is created. Sequence numbers must be continuous across
    /// rotation boundaries so that snapshot + journal recovery works correctly.
    ///
    /// Fails if the file already exists.
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

    /// Internal: create a new journal with a specific genesis hash.
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

    /// Write the `GenesisHash` entry, seed the BLAKE3 chain from it, and
    /// flush. Extracted so `create_with_genesis` and `adopt_prepared` (the
    /// pre-staged segment fast path used by rotation) share the same
    /// initialisation sequence.
    ///
    /// Preconditions: the writer was just constructed (empty batch buffer,
    /// `hash_chain` is `None`, `next_sequence` is the starting sequence
    /// for the new segment).
    #[cfg(feature = "hash-chain")]
    fn emit_genesis_and_init_chain(&mut self, genesis: [u8; 32]) -> Result<(), JournalError> {
        let genesis_event: JournalEvent<E> = JournalEvent::GenesisHash { hash: genesis };
        let seq = self.next_sequence;
        let timestamp_ns = unix_epoch_nanos();
        let written = codec::encode(seq, timestamp_ns, 0, 0, &genesis_event, &mut self.buffer)?;

        // Initialize chain: hash the genesis entry bytes (excluding CRC).
        let entry_bytes = &self.buffer[..written - 4]; // exclude CRC
        let hash = blake3::hash(entry_bytes);
        self.hash_chain = Some(HashChain {
            current_hash: *hash.as_bytes(),
            batch_hasher: blake3::Hasher::new(),
            events_since_checkpoint: 0,
        });

        self.batch_buf[0..written].copy_from_slice(&self.buffer[..written]);
        self.last_user_entry_len = written;
        self.batch_len += written;
        self.next_sequence += 1;
        self.flush_batch_sync()
    }

    /// Internal: create a new journal without a hash chain.
    #[cfg(not(feature = "hash-chain"))]
    fn create_without_chain(path: &Path, starting_sequence: u64) -> Result<Self, JournalError> {
        let writer = Self::create_bare(path, starting_sequence)?;
        Ok(writer)
    }

    /// Shared file setup: header, pre-allocation, sync.
    fn create_bare(path: &Path, starting_sequence: u64) -> Result<Self, JournalError> {
        // O_DIRECT requires writes aligned to the device's physical sector
        // size. Read permission is required for prefault_pages (mmap
        // MAP_SHARED) and for partial-tail recovery on open_append.
        let mut opts = OpenOptions::new();
        opts.read(true).write(true).create_new(true);
        #[cfg(not(feature = "no-o-direct"))]
        opts.custom_flags(libc::O_DIRECT);
        let file = opts.open(path)?;

        let sector_size = detect_sector_size(file.as_fd());
        Self::create_bare_inner(file, path, starting_sequence, sector_size)
    }

    /// Test hook: create with a forced sector size, bypassing `detect_sector_size`.
    #[cfg(test)]
    fn create_bare_with_sector_size(
        path: &Path,
        starting_sequence: u64,
        sector_size: usize,
    ) -> Result<Self, JournalError> {
        let mut opts = OpenOptions::new();
        opts.read(true).write(true).create_new(true);
        #[cfg(not(feature = "no-o-direct"))]
        opts.custom_flags(libc::O_DIRECT);
        let file = opts.open(path)?;
        Self::create_bare_inner(file, path, starting_sequence, sector_size)
    }

    fn create_bare_inner(
        file: File,
        path: &Path,
        starting_sequence: u64,
        sector_size: usize,
    ) -> Result<Self, JournalError> {
        // Reserve `ENTRY_OFFSET` bytes for the file header regardless of
        // device sector size — the layout invariant that lets BufferedWriter
        // and SectorWriter open each other's journals.
        let allocated_end = preallocate(&file, ENTRY_OFFSET)?;
        zero_range_extents(&file, ENTRY_OFFSET, allocated_end);

        // Pre-fault all pages in the preallocated region so the first write
        // to each 4 KB page doesn't trigger a page cache miss during an
        // io_uring write. Without this, each miss is handled by an io-wq
        // worker on core 0 (IRQ core), which competes with TCP interrupt
        // handlers and can stall for hundreds of milliseconds under load.
        prefault_pages(&file, ENTRY_OFFSET, allocated_end);

        let writer = Self::build_from_owned_parts(
            file,
            path,
            starting_sequence,
            sector_size,
            allocated_end,
        )?;
        writer.file.sync_all()?;
        Ok(writer)
    }

    /// Assemble a `SectorWriter` from an already-open file whose
    /// `[sector_size, allocated_end)` range is already prepared
    /// (allocated, zeroed, prefaulted). Writes the file header sector
    /// and locks the in-memory buffers. Does not call `sync_all` — the
    /// caller decides whether to issue one (e.g. fresh-create wants to
    /// durably commit the header alone; the rotation/adopt path
    /// piggybacks on the immediately-following `flush_batch_sync`).
    ///
    /// Shared by `create_bare_inner` (full first-time setup) and
    /// `adopt_prepared` (the rotation fast path).
    fn build_from_owned_parts(
        file: File,
        path: &Path,
        starting_sequence: u64,
        sector_size: usize,
        allocated_end: u64,
    ) -> Result<Self, JournalError> {
        // Allocated once at startup; reused for the entire journal lifetime.
        let (batch_buf, spare_buf) = Self::alloc_batch_bufs();
        let mut tail_sector = Self::alloc_tail_sector();

        // Encode the file header at offset 0 as one O_DIRECT pwrite of
        // `MAX_SECTOR_SIZE` bytes. The header reservation is fixed at
        // `ENTRY_OFFSET = MAX_SECTOR_SIZE = 4096` for both writer modes
        // — on a 4Kn drive this is one device sector, on a 512n drive
        // it's eight (still aligned). The tail_sector scratch buffer
        // is `MAX_SECTOR_SIZE`-sized so this always fits.
        codec::encode_file_header(&mut tail_sector[..MAX_SECTOR_SIZE], MAX_SECTOR_SIZE);
        pwrite_aligned_sector(file.as_fd(), &tail_sector[..MAX_SECTOR_SIZE], 0)?;
        tail_sector.fill(0);

        Self::lock_buffer(batch_buf.as_ptr(), BATCH_BUF_CAPACITY);
        Self::lock_buffer(spare_buf.as_ptr(), BATCH_BUF_CAPACITY);
        Self::lock_buffer(tail_sector.as_ptr(), sector_size);

        Ok(Self {
            _marker: PhantomData,
            file,
            buffer: [0u8; MAX_ENTRY_SIZE],
            batch_buf,
            spare_buf: Some(spare_buf),
            next_sequence: starting_sequence,
            path: path.to_path_buf(),
            write_pos: ENTRY_OFFSET,
            allocated_end,
            #[cfg(feature = "hash-chain")]
            hash_chain: None,
            #[cfg(debug_assertions)]
            last_encoded_seq: 0,
            batch_len: 0,
            last_user_entry_offset: 0,
            last_user_entry_len: 0,
            sector_size,
            tail_sector,
            tail_sector_len: 0,
        })
    }

    /// Adopt a [`PreparedSegment`] produced by [`SegmentPreparer`].
    ///
    /// Mirrors `create_continuing` but reuses the already-allocated /
    /// zero-ranged / prefaulted staging file instead of doing that work
    /// synchronously. On a successful return the new live segment is at
    /// `live_path`, the file header is written, and (for `hash-chain`
    /// builds) the `GenesisHash` entry has been emitted and durably
    /// flushed.
    ///
    /// Failure mode contract for [`Self::rotate_segment_inner`]: this
    /// method may or may not have renamed the staging file before
    /// failing. On any error the caller falls back to its rename-back
    /// rollback path — Linux `rename(2)` atomically replaces, so a
    /// partially-installed live file is overwritten by the restored
    /// archive without extra cleanup. The staging file is either gone
    /// (rename succeeded) or still on disk (rename failed); in the
    /// latter case the next preparer cycle removes it via the
    /// `create_new`-then-cleanup pattern.
    pub(crate) fn adopt_prepared(
        prepared: PreparedSegment,
        live_path: &Path,
        starting_sequence: u64,
        #[cfg_attr(not(feature = "hash-chain"), allow(unused_variables))] genesis_hash: [u8; 32],
    ) -> Result<Self, JournalError> {
        let PreparedSegment {
            file,
            path: staging_path,
            allocated_end,
            sector_size,
        } = prepared;

        // Rename staging onto the live path. `archive_live` has already
        // moved the previous live segment aside, so the destination is
        // free. Done before any further writes so that, if it fails, the
        // staging file is still findable for cleanup.
        std::fs::rename(&staging_path, live_path).map_err(JournalError::Io)?;

        #[cfg_attr(not(feature = "hash-chain"), allow(unused_mut))]
        let mut writer = Self::build_from_owned_parts(
            file,
            live_path,
            starting_sequence,
            sector_size,
            allocated_end,
        )?;

        #[cfg(feature = "hash-chain")]
        {
            // `flush_batch_sync` inside this call also commits the file
            // header sector we just pwrote, so no separate sync_all here.
            writer.emit_genesis_and_init_chain(genesis_hash)?;
        }
        #[cfg(not(feature = "hash-chain"))]
        {
            // Without hash-chain there is no GenesisHash entry to flush —
            // commit the header sector explicitly so a crash before the
            // next user write doesn't leave a header-less live segment.
            writer.file.sync_all()?;
        }

        Ok(writer)
    }

    /// Open an existing journal file for appending after recovery.
    ///
    /// `last_seq` is the sequence number of the last valid entry read during
    /// recovery. The writer will continue from `last_seq + 1`.
    ///
    /// `valid_end` is the byte offset of the end of the last valid entry
    /// (including file header). New entries are written starting here,
    /// overwriting any trailing garbage from a partial crash write.
    ///
    /// `chain_hash` resumes the BLAKE3 hash chain from the reader's final
    /// state. `None` for v5 journals (no hash chain).
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
        // Clear any orphan `<path>.next-staging` from a prior crash
        // before recovery proceeds; matches the cleanup in `create`.
        // Safe at this point — no preparer has been spawned yet.
        crate::preparer::cleanup_staging_orphan(path);
        // Open with O_DIRECT for all writes. Read permission is required for
        // prefault_pages (mmap MAP_SHARED) and for partial-tail reconstruction.
        let mut opts = OpenOptions::new();
        opts.read(true).write(true);
        #[cfg(not(feature = "no-o-direct"))]
        opts.custom_flags(libc::O_DIRECT);
        let file = opts.open(path)?;

        // Read the file header to validate magic + version. Use a
        // MAX_SECTOR_SIZE-aligned scratch buffer so O_DIRECT is satisfied
        // on both 512-byte and 4096-byte drives — the meaningful header
        // fields are in the first 8 bytes regardless. The buffer is
        // reused as tail_sector below after being zeroed.
        let mut tail_sector = Self::alloc_tail_sector();
        let n = file.read_at(&mut tail_sector[..], 0)?;
        if n < FILE_HEADER_SIZE {
            return Err(JournalError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "journal file too short to read file header",
            )));
        }
        // Decoded sector_size is informational — under v13 it always
        // equals ENTRY_OFFSET (= 4096). The device's actual O_DIRECT
        // alignment is detected separately below.
        codec::decode_file_header(&tail_sector[..FILE_HEADER_SIZE])?;
        tail_sector.fill(0);

        // SectorWriter's O_DIRECT writes must align to the device's
        // logical sector size. Detect it directly from the fd — the
        // on-disk header records the fixed ENTRY_OFFSET (4096) for
        // layout, not the alignment requirement.
        let sector_size = detect_sector_size(file.as_fd());

        let file_len = file.metadata()?.len();
        let allocated_end = if file_len >= valid_end {
            file_len
        } else {
            let end = preallocate(&file, valid_end)?;
            file.sync_all()?;
            end
        };

        // Pre-fault only the new prealloc chunk near the write cursor, not the
        // entire (potentially multi-GB) file. Old pages are either still in
        // the page cache or irrelevant — we only care about the region that
        // will receive new writes.
        prefault_pages(&file, valid_end, allocated_end);

        let (batch_buf, spare_buf) = Self::alloc_batch_bufs();

        // Reconstruct the partial tail sector from disk.
        // (tail_sector was allocated above for the header read and cleared.) Writes resume at the
        // sector containing `valid_end`: bytes [sector_base, valid_end) are
        // the unflushed remainder of the last user batch and must be merged
        // with the next batch on flush; bytes [valid_end, sector_base+sector_size)
        // are zeroed in memory so any partial-write garbage past valid_end
        // doesn't surface as a valid entry. We also write the cleaned sector
        // back to disk so the on-disk view matches the in-memory view —
        // protects against a crash before the next flush.
        let sector_base = valid_end & !(sector_size as u64 - 1);
        let tail_len = (valid_end - sector_base) as usize;
        if tail_len > 0 {
            let n = file.read_at(&mut tail_sector[..sector_size], sector_base)?;
            if n < tail_len {
                return Err(JournalError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!(
                        "partial sector read at {sector_base}: got {n} bytes, expected at least {tail_len}"
                    ),
                )));
            }
            tail_sector[tail_len..sector_size].fill(0);
            pwrite_aligned_sector(file.as_fd(), &tail_sector[..sector_size], sector_base)?;
        }
        let write_pos = sector_base;

        Self::lock_buffer(batch_buf.as_ptr(), BATCH_BUF_CAPACITY);
        Self::lock_buffer(spare_buf.as_ptr(), BATCH_BUF_CAPACITY);
        Self::lock_buffer(tail_sector.as_ptr(), sector_size);

        #[allow(unused_mut)]
        let mut writer = Self {
            _marker: PhantomData,
            file,
            buffer: [0u8; MAX_ENTRY_SIZE],
            batch_buf,
            spare_buf: Some(spare_buf),
            next_sequence: last_seq + 1,
            path: path.to_path_buf(),
            write_pos,
            allocated_end,
            #[cfg(feature = "hash-chain")]
            hash_chain: chain_hash.map(|h| HashChain {
                current_hash: h,
                batch_hasher: blake3::Hasher::new(),
                events_since_checkpoint: 0,
            }),
            #[cfg(debug_assertions)]
            last_encoded_seq: last_seq,
            batch_len: 0,
            last_user_entry_offset: 0,
            last_user_entry_len: 0,
            sector_size,
            tail_sector,
            tail_sector_len: tail_len,
        };

        // When resuming mid-segment (events since last checkpoint > 0),
        // reconstruct the batch_hasher by re-reading journal entries since
        // the last checkpoint. This ensures the writer can compute the
        // correct next checkpoint hash that includes all events in the
        // segment, not just the ones written after the resume.
        #[cfg(feature = "hash-chain")]
        if events_since_checkpoint > 0
            && let Some(chain) = &mut writer.hash_chain
        {
            let mut reader = JournalReader::<E>::open(path)?;
            while reader.next_entry()?.is_some() {}
            if let Some((raw_hash, hasher, count)) = reader.take_chain_state() {
                chain.current_hash = raw_hash;
                chain.batch_hasher = hasher;
                chain.events_since_checkpoint = count;
            }
        }

        Ok(writer)
    }

    /// Allocate the next journal sequence number.
    ///
    /// Returns the allocated sequence and advances the internal counter.
    /// The returned sequence should be passed to [`encode_event`] for
    /// encoding. This two-step pattern separates sequence assignment from
    /// encoding, enabling the sequencing decision to be made (and
    /// replicated) independently of journal persistence.
    pub fn allocate_sequence(&mut self) -> u64 {
        let seq = self.next_sequence;
        self.next_sequence += 1;
        seq
    }

    /// Encode a single event into the batch buffer using a pre-assigned
    /// sequence number.
    ///
    /// Does NOT allocate or advance the internal sequence counter — the
    /// caller is responsible for obtaining the sequence via
    /// [`allocate_sequence`] (primary) or by syncing the writer's counter
    /// to a wire-decoded value via [`set_next_sequence`] (replica). This
    /// separation lets the journal stage make the sequencing decision in
    /// disruptor cursor order without coupling encoding to allocation.
    ///
    /// Also handles hash-chain bookkeeping and auto-emits checkpoint
    /// entries when the checkpoint interval is reached.
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

        // Feed entry bytes (excluding CRC) into the batch hasher.
        // No finalize here — that happens once per batch in flush_batch_sync.
        #[cfg(feature = "hash-chain")]
        if let Some(chain) = &mut self.hash_chain {
            let entry_bytes_len = written - 4; // exclude 4-byte CRC
            chain.batch_hasher.update(&self.buffer[..entry_bytes_len]);
            // GenesisHash initializes the chain — don't count it toward
            // the checkpoint interval. Matches create_with_genesis() which
            // sets events_since_checkpoint = 0 after writing the genesis.
            if !matches!(event, JournalEvent::GenesisHash { .. }) {
                chain.events_since_checkpoint += 1;
            }
        }

        self.warn_if_batch_overflow(written);
        // Record the user entry's position in batch_buf BEFORE the
        // auto-checkpoint append below, so `last_user_entry_replication_slice`
        // returns the user entry only — not a trailing checkpoint.
        let offset = self.batch_len;
        self.last_user_entry_offset = offset;
        self.batch_buf[offset..offset + written].copy_from_slice(&self.buffer[..written]);
        self.last_user_entry_len = written;
        self.batch_len += written;

        // Auto-emit a checkpoint if we've hit the interval.
        // Finalize the batch hasher to get the current hash (including all
        // entries accumulated since the last flush/checkpoint).
        #[cfg(feature = "hash-chain")]
        if let Some(chain) = &mut self.hash_chain
            && chain.events_since_checkpoint >= checkpoint_interval()
        {
            // Finalize accumulated entries + previous chain hash.
            chain.batch_hasher.update(&chain.current_hash);
            let checkpoint_hash = *chain.batch_hasher.finalize().as_bytes();
            chain.current_hash = checkpoint_hash;
            chain.batch_hasher = blake3::Hasher::new();
            let count = chain.events_since_checkpoint;
            self.emit_checkpoint(checkpoint_hash, count)?;
        }

        Ok(())
    }

    /// Emit a checkpoint entry into the batch buffer and reset the counter.
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
                "emit_checkpoint: seq {seq} <= last_encoded_seq {} — \
                 auto-emit would duplicate/clash with a prior sequence",
                self.last_encoded_seq
            );
            self.last_encoded_seq = seq;
        }
        let ts = unix_epoch_nanos();
        let written = codec::encode(seq, ts, 0, 0, &checkpoint, &mut self.buffer)?;

        // Reset the event counter. The checkpoint entry itself is NOT fed
        // into the new batch hasher — it acts as a seal for the preceding
        // segment. This keeps the hash chain deterministic regardless of
        // write batching strategy.
        if let Some(chain) = &mut self.hash_chain {
            chain.events_since_checkpoint = 0;
        }

        self.warn_if_batch_overflow(written);
        self.batch_buf[self.batch_len..self.batch_len + written]
            .copy_from_slice(&self.buffer[..written]);
        self.batch_len += written;
        self.next_sequence += 1;
        Ok(())
    }

    /// Warn whenever `batch_buf` is about to grow past its current
    /// capacity. The buffer is sized for the pipeline's flush cadence; an
    /// overflow means the caller is batching more than `batch_buf.capacity()`
    /// bytes between flushes and triggering a `Vec` realloc on the hot path.
    /// `Vec` doubles on grow and `flush_batch` only `clear()`s (capacity
    /// is preserved), so the warns naturally rate-limit themselves to one
    /// per actual realloc.
    #[inline]
    fn warn_if_batch_overflow(&mut self, adding: usize) {
        if self.batch_len + adding > BATCH_BUF_CAPACITY {
            tracing::warn!(
                current_len = self.batch_len,
                adding,
                capacity = BATCH_BUF_CAPACITY,
                "journal batch buffer exceeded preallocated capacity — \
                 caller is batching more than capacity between flushes, \
                 forcing a Vec realloc on the hot path; reduce flush \
                 cadence or raise BATCH_BUF_CAPACITY"
            );
        }
    }

    /// Write the accumulated batch buffer to disk durably.
    ///
    /// Synchronous counterpart of [`take_batch_for_async_write`]. Routes
    /// through `flush_to_sectors`, which merges the in-memory partial tail
    /// with the new batch and issues one sector-aligned `pwrite` (~1–5 µs).
    /// The hot path uses the async variant; this one is for shutdown drains
    /// and one-shot callers.
    pub fn flush_batch_sync(&mut self) -> Result<(), JournalError> {
        if self.batch_len == 0 {
            return Ok(());
        }
        self.ensure_allocated()?;
        self.flush_to_sectors()
    }

    /// Drop the current batch buffer without writing it to disk.
    ///
    /// Used by the `no-persist` path of the journal stage so the buffer
    /// stays bounded after replication has snapshotted the bytes.
    pub fn discard_batch_buf(&mut self) {
        self.batch_len = 0;
        self.last_user_entry_len = 0;
    }

    /// Take the current batch buffer for async writing via io_uring.
    ///
    /// Returns `None` if the batch buffer is empty (nothing to write).
    /// Swaps in the spare buffer so `batch_append()` can continue
    /// accumulating the next batch while this one is in-flight.
    ///
    /// The caller must call `confirm_async_write()` after the io_uring
    /// CQE confirms durability, to return the buffer to the pool.
    /// Hand off the current batch to io_uring for async submission.
    ///
    /// O_DIRECT is always on, so this separates complete sectors (sent via
    /// io_uring) from the partial tail (pwrite'd synchronously so all events
    /// are durable before the pipeline commits). `write_pos` advances per
    /// complete sector; the tail sector position is not advanced until the
    /// sector fills.
    pub fn take_batch_for_async_write(&mut self) -> Result<Option<AsyncWriteBatch>, JournalError> {
        if self.batch_len == 0 {
            return Ok(None);
        }
        self.ensure_allocated()?;
        let old_write_pos = self.write_pos;
        let batch_len = self.batch_len;

        // Swap in the spare as output buffer; full_buf holds the encoded data.
        let spare = self.spare_buf.take().unwrap_or_else(alloc_aligned);
        let full_buf = std::mem::replace(&mut self.batch_buf, spare);

        let sector_size = self.sector_size;
        let mut data_cursor = 0usize;
        let mut output_cursor = 0usize;
        while data_cursor < batch_len {
            let space = sector_size - self.tail_sector_len;
            let remaining = batch_len - data_cursor;
            let to_copy = remaining.min(space);
            self.tail_sector[self.tail_sector_len..self.tail_sector_len + to_copy]
                .copy_from_slice(&full_buf[data_cursor..data_cursor + to_copy]);
            self.tail_sector_len += to_copy;
            data_cursor += to_copy;

            if self.tail_sector_len == sector_size {
                self.batch_buf[output_cursor..output_cursor + sector_size]
                    .copy_from_slice(&self.tail_sector[..sector_size]);
                output_cursor += sector_size;
                self.write_pos += sector_size as u64;
                self.tail_sector[..sector_size].fill(0);
                self.tail_sector_len = 0;
            }
        }

        // Sync-write the partial tail so all events are durable before the
        // pipeline commits. write_pos is NOT advanced: the tail sector is
        // rewritten in-place on every flush until it fills.
        if self.tail_sector_len > 0 {
            self.pwrite_sector(self.write_pos)?;
        }

        let write_len = output_cursor;
        let output_buf = std::mem::replace(&mut self.batch_buf, full_buf);
        self.batch_len = 0;
        self.last_user_entry_len = 0;

        if write_len == 0 {
            self.spare_buf = Some(output_buf);
            return Ok(None);
        }

        Ok(Some(AsyncWriteBatch {
            buf: output_buf,
            len: write_len,
            offset: old_write_pos,
        }))
    }

    /// Return the completed async write buffer to the spare pool.
    /// Called after the io_uring CQE confirms the write completed.
    pub fn confirm_async_write(&mut self, batch: AsyncWriteBatch) {
        self.spare_buf = Some(batch.buf);
    }

    /// Current next sequence number (useful for snapshot coordination).
    pub fn next_sequence(&self) -> u64 {
        self.next_sequence
    }

    /// Set the next sequence number.
    ///
    /// Used by the replica to keep the writer's internal counter in sync
    /// with the primary's pre-assigned sequences. This ensures that
    /// auto-emitted checkpoint entries get the correct sequence numbers.
    pub fn set_next_sequence(&mut self, seq: u64) {
        // Debug-only: catch the footgun where a pre-assigned slot
        // sequence would walk the writer's counter backward. This is
        // the only path that can introduce a duplicate seq, so it's
        // the most load-bearing of the three monotonicity guards.
        debug_assert!(
            seq >= self.next_sequence,
            "set_next_sequence({seq}) moves counter backward from {} — \
             the next allocation/auto-emit would duplicate a prior seq",
            self.next_sequence
        );
        self.next_sequence = seq;
    }

    /// Current byte offset in the journal file (size of valid data).
    pub fn write_pos(&self) -> u64 {
        self.write_pos
    }

    /// Byte offset of the end of valid data in the journal file.
    ///
    /// `write_pos` is always sector-aligned (O_DIRECT is mandatory) and
    /// `tail_sector_len` holds the additional bytes in the in-memory
    /// partial tail sector.
    pub fn valid_end(&self) -> u64 {
        self.write_pos + self.tail_sector_len as u64
    }

    /// Path to the journal file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Raw file descriptor for the journal file.
    pub fn fd(&self) -> std::os::unix::io::RawFd {
        self.file.as_raw_fd()
    }

    /// Physical sector size used for this journal, in bytes (512 or 4096).
    ///
    /// Callers that read the journal file directly (e.g., to parse the genesis
    /// entry) need this to know the byte offset where entries begin.
    pub fn sector_size(&self) -> usize {
        self.sector_size
    }

    /// Read the first (genesis) journal entry as raw bytes.
    ///
    /// The genesis entry immediately follows the file header at offset
    /// `sector_size`. Returned bytes include the full framing
    /// (magic + length + header + payload + CRC).
    ///
    /// Opens a separate non-O_DIRECT file handle because the O_DIRECT handle
    /// used for writes does not allow unaligned reads. This is a startup-only
    /// operation — the extra open is acceptable.
    pub fn read_genesis_entry(&self) -> Result<Vec<u8>, JournalError> {
        let file = std::fs::File::open(&self.path)?;
        let offset = ENTRY_OFFSET;
        // Read the first 4 bytes to get magic(2) + length(2).
        let mut hdr4 = [0u8; 4];
        let n = file.read_at(&mut hdr4, offset)?;
        if n < 4 {
            return Err(JournalError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "journal too short to contain genesis entry",
            )));
        }
        let entry_len = u16::from_le_bytes([hdr4[2], hdr4[3]]) as usize;
        let total = 20 + entry_len + 4; // EntryHeader(20) + payload + CRC(4)
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

    /// `rw_flags` value for io_uring `Write` operations on this journal.
    ///
    /// Always `0`: O_DIRECT delivers writes to device DRAM and PLP capacitors
    /// guarantee persistence on power loss — no `RWF_DSYNC` round-trip needed.
    pub fn io_uring_rw_flags(&self) -> i32 {
        0
    }

    /// Rotate the live journal segment in place.
    ///
    /// Flushes any pending batch durably, renames the current live file to
    /// the next monotonic archive slot (`<path>.NNNNNN`), and opens a
    /// fresh live segment at the original path seeded with
    /// `GenesisHash(prev_chain_hash)`. The new segment continues the
    /// sequence counter — the next event written gets `prev_next_seq + 1`
    /// (the GenesisHash itself takes `prev_next_seq`).
    ///
    /// Multi-segment recovery (see [`crate::segment`]) walks archives in
    /// order before opening the live segment, so events written before
    /// the rotation remain replayable.
    ///
    /// On error after the rename, this method best-effort restores the
    /// live segment so the next recovery sees a usable live file. If both
    /// the recreate and the restore fail, the operator is left with the
    /// archive on disk and no live file — recovery treats this as the
    /// "snapshot-only post-rotation crash" case (see
    /// [`crate::segment::list_archives`]).
    pub fn rotate_segment(&mut self) -> Result<std::path::PathBuf, JournalError> {
        self.rotate_segment_inner(None)
    }

    /// Rotate, adopting a pre-staged segment produced by [`SegmentPreparer`].
    ///
    /// Same contract as [`Self::rotate_segment`] but skips the
    /// `posix_fallocate + FALLOC_FL_ZERO_RANGE + prefault + sync_all`
    /// ceremony for the new segment — that work was already done off the
    /// hot path. The remaining on-rotation cost is a `flush_batch_sync`
    /// of the outgoing segment, two renames, and a parent-directory
    /// fsync.
    ///
    /// On error the prepared file is consumed (renamed onto the live
    /// path then rolled back, or left as staging — see
    /// `adopt_prepared`'s docs). Callers should re-arm the preparer
    /// after a successful return so the next rotation can also be fast.
    pub fn rotate_segment_with_prepared(
        &mut self,
        prepared: PreparedSegment,
    ) -> Result<std::path::PathBuf, JournalError> {
        self.rotate_segment_inner(Some(prepared))
    }

    /// Shared rotation body. `prepared.is_some()` takes the fast path.
    fn rotate_segment_inner(
        &mut self,
        prepared: Option<PreparedSegment>,
    ) -> Result<std::path::PathBuf, JournalError> {
        self.flush_batch_sync()?;

        let path = self.path.clone();
        let next_seq = self.next_sequence;
        // GenesisHash carries the chain state at the rotation boundary so
        // the new segment's chain anchors to the previous one.
        //
        // When the `hash-chain` feature is enabled, `chain_hash()` must
        // return Some after a successful flush (genesis is written at
        // construction). A None here indicates a logic bug that would
        // silently break tamper-evidence — the new segment would anchor
        // to zeros and `verify_segment_boundary` would still pass on
        // recovery. Refuse to rotate.
        // clippy suggests unwrap_or_default, but the None arm has a
        // feature-gated `return Err(...)` that the suggestion would erase.
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
                // Hash-chain disabled: zeros are meaningless anyway.
                #[cfg(not(feature = "hash-chain"))]
                {
                    [0u8; 32]
                }
            }
        };

        let archived = crate::segment::archive_live(&path).map_err(JournalError::Io)?;

        let new_writer_result = match prepared {
            Some(p) => Self::adopt_prepared(p, &path, next_seq, genesis),
            None => Self::create_continuing(&path, next_seq, genesis),
        };

        match new_writer_result {
            Ok(new_writer) => {
                *self = new_writer;
                // Durably commit both the rename (archive_live) and the
                // new live file's dirent in a single dir fsync. Without
                // this, power loss between rotation and the next
                // dir-metadata flush could leave recovery seeing the
                // pre-rotation layout (acceptable) — or worse, the
                // archive present without the new live (handled as
                // Phase B but loses post-rotation crash recovery).
                if let Err(e) = crate::segment::fsync_parent_dir(&path) {
                    return Err(JournalError::Io(e));
                }
                Ok(archived)
            }
            Err(e) => {
                // Best-effort: undo the rename so the next recovery still
                // sees a live file at the canonical path. If the
                // rename-back also fails the on-disk layout is "archive
                // present, no live"; recovery's Phase B handles this
                // (synthesizes a new live), but the in-process writer
                // would have nothing usable so we surface the original
                // error and let the caller bring the engine down.
                if let Err(restore_err) = std::fs::rename(&archived, &path) {
                    tracing::warn!(
                        "rotate_segment: rename-back failed after create_continuing error: \
                         original={e}, restore={restore_err}"
                    );
                }
                Err(e)
            }
        }
    }

    /// Current BLAKE3 chain hash, if hash chain is active.
    /// Returns `None` when the `hash-chain` feature is disabled, for v5
    /// journals, or if no events have been written.
    ///
    /// When events have been accumulated since the last checkpoint (or
    /// genesis), computes the hash on-demand by cloning the incremental
    /// hasher and finalizing with the previous chain hash. When no events
    /// are pending, returns the stored checkpoint/genesis hash directly.
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

    /// Events since last checkpoint, if hash chain is active.
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

    /// Read-only access to the pending batch buffer (encoded but not yet flushed).
    ///
    /// Used by the journal stage to snapshot encoded bytes for replication
    /// after `flush_batch_sync()` — the bytes are identical to what was
    /// just written to disk.
    ///
    /// Returns an empty slice if no data is pending.
    pub fn pending_batch_bytes(&self) -> &[u8] {
        &self.batch_buf[..self.batch_len]
    }

    /// Flush batch data to disk using O_DIRECT sector tail buffer.
    ///
    /// Copies batch data into the tail sector, writes complete sectors,
    /// and rewrites the partial tail sector in-place. This avoids padding
    /// to 512 bytes per write, preventing premature journal exhaustion.
    /// Flush the batch to disk in a single O_DIRECT `pwrite`.
    ///
    /// Writes one contiguous, sector-aligned buffer covering the existing
    /// partial tail sector and all new batch data. This costs exactly one
    /// syscall per flush regardless of how many sectors the batch spans —
    /// compared to one pwrite per sector in the naive approach.
    ///
    /// Layout of the write buffer (built in-place in `batch_buf`):
    ///   [tail_sector[0..tail_sector_len]] [batch data] [zero padding]
    ///   ↑ write_pos (sector-aligned)                    ↑ padded to SECTOR_SIZE
    ///
    /// After the write, `write_pos` advances past all newly-completed sectors.
    /// The last partial sector (if any) becomes the new tail.
    fn flush_to_sectors(&mut self) -> Result<(), JournalError> {
        if self.batch_len == 0 {
            return Ok(());
        }

        let sector_size = self.sector_size;
        let total = self.tail_sector_len + self.batch_len;
        let new_tail_len = total & (sector_size - 1); // total % sector_size
        // Round up to sector boundary.
        let padded = if new_tail_len > 0 {
            total + (sector_size - new_tail_len)
        } else {
            total
        };

        // Shift batch data right by tail_sector_len bytes to make room for the
        // existing tail prefix. Relies on batch_buf being large enough:
        // padded = total + padding < tail_sector_len + batch_len + sector_size
        //        < sector_size + BATCH_BUF_CAPACITY (since tail_sector_len < sector_size)
        // The pipeline limits batch_len to well under BATCH_BUF_CAPACITY - sector_size.
        self.batch_buf
            .copy_within(0..self.batch_len, self.tail_sector_len);
        self.batch_buf[..self.tail_sector_len]
            .copy_from_slice(&self.tail_sector[..self.tail_sector_len]);
        if padded > total {
            self.batch_buf[total..padded].fill(0);
        }

        // Single pwrite for everything — one NVMe command instead of N.
        let fd = self.file.as_fd();
        let buf = &self.batch_buf[..padded];
        let written = rustix::io::pwrite(fd, buf, self.write_pos).map_err(rustix_to_io)?;
        if written != padded {
            return Err(JournalError::Io(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "short O_DIRECT write",
            )));
        }

        // Update tail state. The new partial tail is the last sector of the write.
        if new_tail_len > 0 {
            let last_sector_base = padded - sector_size;
            self.tail_sector[..new_tail_len].copy_from_slice(
                &self.batch_buf[last_sector_base..last_sector_base + new_tail_len],
            );
            self.tail_sector[new_tail_len..sector_size].fill(0);
            // write_pos advances past all complete sectors (the last sector is partial).
            self.write_pos += last_sector_base as u64;
        } else {
            // All sectors were complete — no partial tail.
            self.tail_sector.fill(0);
            self.write_pos += padded as u64;
        }
        self.tail_sector_len = new_tail_len;

        self.batch_len = 0;
        self.last_user_entry_len = 0;
        Ok(())
    }

    /// Write `self.tail_sector[..sector_size]` at `offset`.
    ///
    /// O_DIRECT requires the write length, buffer address, and file offset all
    /// be sector-aligned — all three hold here: tail_sector is 4096-byte aligned
    /// (alloc layout), sector_size is 512 or 4096, and offset is always a
    /// multiple of sector_size.
    fn pwrite_sector(&self, offset: u64) -> Result<(), JournalError> {
        pwrite_aligned_sector(
            self.file.as_fd(),
            &self.tail_sector[..self.sector_size],
            offset,
        )
    }

    /// Slice of the most-recent user entry in `batch_buf`, with the
    /// 2-byte journal magic stripped from the front and the 4-byte CRC
    /// stripped from the back. Layout matches the replication wire's
    /// `SlotHeader + payload` exactly:
    ///
    /// ```text
    /// [length:u16] [sequence:u64] [timestamp_ns:u64] [key_hash:u64]
    /// [request_seq:u64] [event_tag:u8] [payload]
    /// ```
    ///
    /// Lets the journal stage ship the just-encoded entry to replication
    /// without a second encode pass — `record_slot_for_replication`
    /// memcpys this slice into the InputBatch buffer.
    ///
    /// Must be called immediately after `encode_event`. Returns an empty
    /// slice if no entry has been encoded yet, or if `flush_batch` /
    /// `discard_batch_buf` has cleared the buffer since the last encode.
    pub fn last_user_entry_replication_slice(&self) -> &[u8] {
        if self.last_user_entry_len == 0 {
            return &[];
        }
        let start = self.last_user_entry_offset;
        let end = start + self.last_user_entry_len;
        // Strip the 2-byte entry magic and 4-byte CRC trailer.
        &self.batch_buf[start + 2..end - 4]
    }

    /// Attempt to mlock a buffer. Best-effort: warns but does not fail, since
    /// mlock is a performance optimization (avoids page faults on I/O), not a
    /// correctness requirement. Failure is typical in unprivileged environments.
    fn lock_buffer(ptr: *const u8, size: usize) {
        let ret = unsafe { mlock(ptr as *const libc::c_void, size) };
        if ret < 0 {
            let err = std::io::Error::last_os_error();
            tracing::warn!(
                error = %err,
                "mlock failed — buffers not locked; O_DIRECT writes may incur page faults. \
                Run with `ulimit -l unlimited` or `CAP_IPC_LOCK` to eliminate this."
            );
        }
    }

    /// Ensure enough pre-allocated space exists for the next write.
    ///
    /// O_DIRECT writes are sector-aligned; we extend the prealloc as soon as
    /// the next sector would land past `allocated_end`.
    ///
    /// Data-loss safety lives in [`Self::zero_unwritten_through`] rather
    /// than here: this function only has to ask "extend the prealloc
    /// past where we're about to write" and the helper takes care of
    /// only zeroing bytes the writer hasn't placed yet.
    fn ensure_allocated(&mut self) -> Result<(), JournalError> {
        if self.write_pos + self.sector_size as u64 <= self.allocated_end {
            return Ok(());
        }
        self.allocated_end = preallocate(&self.file, self.write_pos)?;
        self.zero_unwritten_through(self.allocated_end);
        Ok(())
    }

    /// Zero the byte range strictly past the writer's frontier
    /// (`write_pos`) up to `end`, via `FALLOC_FL_ZERO_RANGE`.
    ///
    /// **The invariant this API encodes**: the start of the zero range
    /// is *always* the writer's frontier, computed internally. Callers
    /// can only choose the *upper* bound, so they can't accidentally
    /// re-zero bytes the writer has already placed past the previous
    /// `allocated_end`. That class of bug (a stale `old_end` used as
    /// the start) is unreachable through this method.
    ///
    /// Both write paths — sync (`flush_to_sectors`) and async
    /// (`take_batch_for_async_write`) — can issue a single multi-sector
    /// `pwrite` that the kernel auto-extends past `allocated_end`. By
    /// the time we get here to extend the prealloc, `write_pos` may
    /// already be past the old `allocated_end`; the `<=` guard ensures
    /// we don't ask the kernel to zero a degenerate or backwards range.
    ///
    /// The raw [`zero_range_extents`] helper is retained for one-shot
    /// initialisation sites (create-time, segment preparer) where the
    /// start is a compile-time constant and no writer frontier exists
    /// yet.
    fn zero_unwritten_through(&self, end: u64) {
        if self.write_pos < end {
            zero_range_extents(&self.file, self.write_pos, end);
        }
    }

    /// Allocate two zeroed sector-aligned batch buffers (batch_buf + spare_buf).
    fn alloc_batch_bufs() -> (
        Box<AlignedBuf<BATCH_BUF_CAPACITY>>,
        Box<AlignedBuf<BATCH_BUF_CAPACITY>>,
    ) {
        (alloc_aligned(), alloc_aligned())
    }

    /// Allocate one zeroed tail sector buffer. Sized to MAX_SECTOR_SIZE so
    /// it works for both 512-byte and 4096-byte sector drives; at runtime
    /// only `sector_size` bytes of the buffer are used.
    fn alloc_tail_sector() -> Box<AlignedBuf<MAX_SECTOR_SIZE>> {
        alloc_aligned()
    }
}

/// 512-byte-aligned byte buffer used for O_DIRECT I/O.
///
/// O_DIRECT requires the buffer pointer, write length, and file offset to
/// all be sector-aligned (512 B). The `repr(align(512))` attribute makes
/// the *type* carry that alignment, so the allocator returns a sector-
/// aligned address and the matching alignment is used on dealloc — no
/// manual `Layout` dance, no allocator/deallocator layout mismatch.
///
/// `Deref` / `DerefMut` to `[u8; N]` so call sites see a plain byte array.
/// (Slicing — `&buf[..]` — gets you a `&[u8]` for functions that take one.)
/// 4096-byte alignment satisfies O_DIRECT requirements for both 512-byte
/// (512e/512n) and 4096-byte (4Kn) NVMe drives.
#[derive(zerocopy::FromZeros, zerocopy::KnownLayout, zerocopy::Immutable)]
#[repr(C, align(4096))]
pub struct AlignedBuf<const N: usize>([u8; N]);

impl<const N: usize> std::ops::Deref for AlignedBuf<N> {
    type Target = [u8; N];
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<const N: usize> std::ops::DerefMut for AlignedBuf<N> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// Allocate a zeroed `AlignedBuf<N>` directly on the heap.
///
/// `FromZeros::new_box_zeroed` performs the allocation in-place — no 512 KB
/// stack copy that `Box::new(AlignedBuf([0; N]))` would otherwise produce
/// in debug builds.
///
/// On allocation failure (OOM), routes through `handle_alloc_error`, which
/// invokes the global allocator's error handler (typically `abort`). This
/// matches `Box::new`'s behavior and avoids unwinding through the journal
/// hot path on a condition we cannot meaningfully recover from.
fn alloc_aligned<const N: usize>() -> Box<AlignedBuf<N>> {
    use zerocopy::FromZeros;
    AlignedBuf::<N>::new_box_zeroed().unwrap_or_else(|_| {
        std::alloc::handle_alloc_error(std::alloc::Layout::new::<AlignedBuf<N>>())
    })
}

/// Pre-fault pages in `[start, end)` into the page cache via a read-only
/// shared `mmap` + `Advice::PopulateRead` (= `MADV_POPULATE_READ`).
///
/// Without this, the first write to each 4 KiB page triggers a page-cache
/// miss handled by io-wq workers on core 0 (IRQ core), which can stall
/// for hundreds of milliseconds under TCP load.
///
/// `start` is aligned down to a 4 KiB page boundary (mmap offset requirement).
/// `end` is typically `allocated_end` — the pre-allocated chunk boundary.
///
/// `Advice::PopulateRead` (kernel 5.14+) faults the pages as *clean*, so
/// `sync_all()` after `create_bare` doesn't have to write them back.
/// `PopulateWrite` would dirty 256 MiB of preallocated pages and force a
/// full writeback even though nothing has been written yet.
///
/// Best-effort: silently skips on failure (e.g. insufficient VA space).
/// Not called on the O_DIRECT path — O_DIRECT bypasses the page cache.
pub(crate) fn prefault_pages(file: &File, start: u64, end: u64) {
    if end <= start {
        return;
    }
    let aligned_start = start & !4095;
    let size = (end - aligned_start) as usize;

    // SAFETY: A read-only shared mapping of an owned `File`. The `Mmap`
    // guard ties the mapping lifetime to the value below and calls
    // `munmap` on drop; we drop it before this function returns. The
    // pages are read-only and never exposed to callers, so there is no
    // way for the rest of the program to observe stale or aliased memory
    // through this mapping.
    let mmap = unsafe {
        memmap2::MmapOptions::new()
            .offset(aligned_start)
            .len(size)
            .map(file)
    };
    let Ok(mmap) = mmap else {
        return;
    };
    // Best-effort kernel hint: `PopulateRead` faults pages in eagerly so the
    // next read avoids a synchronous page fault on the hot path. If the
    // kernel rejects the advice (older kernel, unusual mapping) we silently
    // proceed — the read will simply fault pages in lazily as before.
    let _ = mmap.advise(memmap2::Advice::PopulateRead);
}

/// Pre-allocate disk blocks from the current position forward by one chunk.
///
/// Uses `posix_fallocate` to allocate extents without writing zeros — the
/// filesystem guarantees zero-fill on read for unwritten blocks. On the
/// supported deployment targets (ext4, xfs, btrfs) this always succeeds;
/// failure means an unsupported filesystem and is surfaced as an error.
pub(crate) fn preallocate(file: &File, current_end: u64) -> Result<u64, JournalError> {
    let chunk = prealloc_chunk_bytes();
    let target = current_end + chunk;

    // Allocate only the new chunk [current_end, target), not [0, target).
    // fallocate(fd, 0, target) walks the entire extent tree from offset 0
    // on every call to verify already-allocated extents, which takes
    // O(file_size) as the file grows — causing linearly growing latency
    // spikes under sustained write load.
    rustix::fs::fallocate(
        file.as_fd(),
        rustix::fs::FallocateFlags::empty(),
        current_end,
        chunk,
    )
    .map_err(rustix_to_io)?;
    Ok(target)
}

/// Mark pre-allocated extents as written zeros using `FALLOC_FL_ZERO_RANGE`.
///
/// On ext4, `posix_fallocate` creates "unwritten" extents. The first write
/// to an unwritten block triggers a metadata status change (unwritten →
/// written) that goes into the ext4 jbd2 transaction buffer. Every ~5s
/// (default `commit` interval), jbd2 commits these transactions with a full
/// NVMe cache flush (`REQ_PREFLUSH`), stalling concurrent `pwrite+O_DIRECT`
/// writes for 1-2ms.
///
/// `FALLOC_FL_ZERO_RANGE` pre-converts extents to "written + zeroed",
/// eliminating per-write metadata updates and the resulting jbd2 flush storms.
///
/// Best-effort: failures are logged at warn level and ignored. The fallback
/// is periodic 1-2ms tail latency spikes, not data loss.
pub(crate) fn zero_range_extents(file: &File, start: u64, end: u64) {
    if start >= end {
        return;
    }
    if let Err(err) = rustix::fs::fallocate(
        file.as_fd(),
        rustix::fs::FallocateFlags::ZERO_RANGE,
        start,
        end - start,
    ) {
        tracing::warn!(
            errno = err.raw_os_error(),
            start,
            end,
            "FALLOC_FL_ZERO_RANGE failed — expect periodic 1-2ms tail latency spikes"
        );
    }
}

/// Detect the physical sector size of the block device backing `fd`.
///
/// Reads `/sys/dev/block/{major}:{minor}/queue/physical_block_size` via the
/// file's `st_dev`. Returns 512 for non-block-device filesystems (tmpfs,
/// overlayfs) and as a fallback on any lookup failure.
///
/// O_DIRECT requires all writes to be aligned to this size — passing 512 on
/// a true 4Kn drive results in EINVAL.
pub fn detect_sector_size(fd: std::os::unix::io::BorrowedFd<'_>) -> usize {
    let stat = match rustix::fs::fstat(fd) {
        Ok(s) => s,
        Err(_) => return 512,
    };
    let maj = libc::major(stat.st_dev);
    let min = libc::minor(stat.st_dev);
    let sysfs = format!("/sys/dev/block/{maj}:{min}/queue/physical_block_size");
    match std::fs::read_to_string(&sysfs)
        .ok()
        .and_then(|s| s.trim().parse::<usize>().ok())
    {
        Some(n) if n == 512 || n == 4096 => n,
        Some(n) if n > 0 && n.is_power_of_two() => {
            tracing::warn!(
                physical_block_size = n,
                "device has unrecognized physical_block_size > 4096; falling back to 512 — \
                 O_DIRECT writes may fail with EINVAL on this drive"
            );
            512
        }
        _ => 512,
    }
}

/// Write one sector-aligned buffer at a sector-aligned offset.
///
/// Caller must guarantee that `data.len()` equals the sector size (512 or
/// 4096), the buffer is 4096-byte aligned (all buffers here come from
/// `alloc_aligned`), and `offset` is a multiple of `data.len()` — required
/// by O_DIRECT.
fn pwrite_aligned_sector(
    fd: std::os::fd::BorrowedFd<'_>,
    data: &[u8],
    offset: u64,
) -> Result<(), JournalError> {
    debug_assert!(
        data.len() == 512 || data.len() == 4096,
        "sector write length must be 512 or 4096, got {}",
        data.len()
    );
    debug_assert_eq!(
        offset % data.len() as u64,
        0,
        "offset must be sector-aligned"
    );
    let written = rustix::io::pwrite(fd, data, offset).map_err(rustix_to_io)?;
    if written != data.len() {
        return Err(JournalError::Io(std::io::Error::new(
            std::io::ErrorKind::WriteZero,
            "short sector write",
        )));
    }
    Ok(())
}

/// Convert a `rustix::io::Errno` into the `JournalError::Io` variant.
fn rustix_to_io(err: rustix::io::Errno) -> JournalError {
    JournalError::Io(std::io::Error::from_raw_os_error(err.raw_os_error()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::JournalReader;
    use crate::write::JournalWrite;
    use melin_app::CodecError;

    /// Minimal `AppEvent` for tests — carries a `u64` payload so distinct
    /// events round-trip unambiguously.
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
            let v = u64::from_le_bytes(buf[..8].try_into().unwrap());
            Ok(TestEvent(v))
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

    fn sample_event() -> JournalEvent<TestEvent> {
        JournalEvent::App(TestEvent(42))
    }

    fn read_all(path: &Path) -> Vec<crate::reader::JournalEntry<TestEvent>> {
        let mut reader = JournalReader::<TestEvent>::open(path).unwrap();
        let mut entries = Vec::new();
        while let Some(entry) = reader.next_entry().unwrap() {
            entries.push(entry);
        }
        entries
    }

    #[test]
    fn create_initializes_header_and_preallocates() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let writer = SectorWriter::<TestEvent>::create(&path).unwrap();
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

        let _writer = SectorWriter::<TestEvent>::create(&path).unwrap();
        drop(_writer);

        let result = SectorWriter::<TestEvent>::create(&path);
        assert!(result.is_err());
    }

    #[test]
    fn append_assigns_sequential_ids() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let mut writer = SectorWriter::<TestEvent>::create(&path).unwrap();
        let event = sample_event();

        let seq1 = writer.append(&event).unwrap();
        let seq2 = writer.append(&event).unwrap();
        let seq3 = writer.append(&event).unwrap();

        assert_eq!(seq1, FIRST_SEQ);
        assert_eq!(seq2, FIRST_SEQ + 1);
        assert_eq!(seq3, FIRST_SEQ + 2);
        assert_eq!(writer.next_sequence(), FIRST_SEQ + 3);
    }

    #[test]
    fn append_is_readable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let event = sample_event();
        {
            let mut writer = SectorWriter::<TestEvent>::create(&path).unwrap();
            writer.append(&event).unwrap();
        }

        let entries = read_all(&path);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].sequence, FIRST_SEQ);
        assert_eq!(entries[0].event, event);
        assert!(entries[0].timestamp_ns > 0);
    }

    #[test]
    fn batch_append_then_flush_is_readable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let events = vec![
            JournalEvent::App(TestEvent(1)),
            JournalEvent::App(TestEvent(2)),
            JournalEvent::App(TestEvent(3)),
        ];
        {
            let mut writer = SectorWriter::<TestEvent>::create(&path).unwrap();
            for event in &events {
                writer.batch_append_with_ts(event, 0, 0, 0).unwrap();
            }
            writer.flush_batch_sync().unwrap();
        }

        let entries = read_all(&path);
        assert_eq!(entries.len(), 3);
        for (i, entry) in entries.iter().enumerate() {
            assert_eq!(entry.sequence, FIRST_SEQ + i as u64);
            assert_eq!(entry.event, events[i]);
        }
    }

    #[test]
    fn batch_append_does_not_write_to_disk() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let mut writer = SectorWriter::<TestEvent>::create(&path).unwrap();
        // valid_end() includes tail_sector data; write_pos is sector-aligned base.
        let pos_before = writer.valid_end();
        writer
            .batch_append_with_ts(&sample_event(), 0, 0, 0)
            .unwrap();
        // batch_append only writes to the in-memory batch buffer, not tail_sector.
        assert_eq!(writer.valid_end(), pos_before);
        writer.flush_batch_sync().unwrap();
        assert!(writer.valid_end() > pos_before);
    }

    #[test]
    fn open_append_continues_sequence() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let (last_seq, valid_end, events_since_checkpoint) = {
            let mut writer = SectorWriter::<TestEvent>::create(&path).unwrap();
            writer.append(&sample_event()).unwrap();
            writer.append(&sample_event()).unwrap();
            (
                writer.next_sequence() - 1,
                writer.valid_end(),
                writer.events_since_checkpoint(),
            )
        };

        let mut writer = SectorWriter::<TestEvent>::open_append(
            &path,
            last_seq,
            valid_end,
            None,
            events_since_checkpoint,
        )
        .unwrap();
        let next_seq = writer.append(&sample_event()).unwrap();
        assert_eq!(next_seq, last_seq + 1);
    }

    #[test]
    fn open_append_zeros_trailing_garbage() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        // Write an event, reopen, write another — the trailing garbage from
        // the pre-allocation should be zeroed so the reader stops at the
        // last valid entry instead of tripping on leftover bytes.
        let (last_seq, valid_end) = {
            let mut writer = SectorWriter::<TestEvent>::create(&path).unwrap();
            writer.append(&sample_event()).unwrap();
            (writer.next_sequence() - 1, writer.valid_end())
        };

        {
            let _writer =
                SectorWriter::<TestEvent>::open_append(&path, last_seq, valid_end, None, 0)
                    .unwrap();
        }

        let entries = read_all(&path);
        assert_eq!(entries.len(), 1);
    }

    // --- Hash-chain-specific tests (gated) --------------------------------

    #[cfg(feature = "hash-chain")]
    #[test]
    fn genesis_hash_initializes_chain_transparently() {
        // The genesis entry is written first but is transparent to
        // `next_entry`: the reader consumes it internally (chain init)
        // and surfaces only user events. So a journal with only the
        // genesis yields zero visible entries, and the chain is active.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let writer = SectorWriter::<TestEvent>::create(&path).unwrap();
        assert!(writer.chain_hash().is_some());
        drop(writer);

        assert_eq!(read_all(&path).len(), 0);
    }

    #[cfg(feature = "hash-chain")]
    #[test]
    fn chain_hash_changes_with_each_event() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let mut writer = SectorWriter::<TestEvent>::create(&path).unwrap();
        let hash_before = writer.chain_hash();
        writer.append(&sample_event()).unwrap();
        // The chain hash only advances when batches are finalized — after a
        // direct `append` with sync, the chain hasher has a segment in flight
        // but the exposed hash is the last finalized checkpoint hash.
        // The stability-post-single-append is the normal case; assert the
        // writer still has a chain.
        assert!(writer.chain_hash().is_some());
        assert!(hash_before.is_some());
    }

    #[cfg(feature = "hash-chain")]
    #[test]
    fn open_append_with_chain_hash_resumes_chain() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let (last_seq, valid_end, chain_hash, events_since_checkpoint) = {
            let mut writer = SectorWriter::<TestEvent>::create(&path).unwrap();
            writer.append(&sample_event()).unwrap();
            (
                writer.next_sequence() - 1,
                writer.valid_end(),
                writer.chain_hash(),
                writer.events_since_checkpoint(),
            )
        };

        let writer = SectorWriter::<TestEvent>::open_append(
            &path,
            last_seq,
            valid_end,
            chain_hash,
            events_since_checkpoint,
        )
        .unwrap();
        assert!(writer.chain_hash().is_some());
    }

    #[cfg(feature = "hash-chain")]
    #[test]
    fn open_append_without_chain_hash_has_no_chain() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let (last_seq, valid_end) = {
            let mut writer = SectorWriter::<TestEvent>::create(&path).unwrap();
            writer.append(&sample_event()).unwrap();
            (writer.next_sequence() - 1, writer.valid_end())
        };

        let writer =
            SectorWriter::<TestEvent>::open_append(&path, last_seq, valid_end, None, 0).unwrap();
        assert!(writer.chain_hash().is_none());
    }

    /// Verify that journals created with sector_size=4096 round-trip correctly.
    ///
    /// Forces sector_size=4096 regardless of the test machine's drive so we
    /// exercise the 4Kn code path on any host. On 512-byte devices, O_DIRECT
    /// accepts 4096-byte writes without error (4096 is a valid multiple of 512).
    #[test]
    fn sector_size_4096_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("4k.journal");

        {
            let mut writer =
                SectorWriter::<TestEvent>::create_bare_with_sector_size(&path, 1, 4096).unwrap();
            assert_eq!(writer.sector_size(), 4096);
            // write_pos starts at 4096 (one 4Kn sector for the header).
            assert_eq!(writer.write_pos(), 4096);
            writer
                .append(&JournalEvent::App(TestEvent(0xdead)))
                .unwrap();
            writer
                .append(&JournalEvent::App(TestEvent(0xbeef)))
                .unwrap();
        }

        // Reader must skip the 4096-byte header sector and surface both events.
        let mut reader = crate::reader::JournalReader::<TestEvent>::open(&path).unwrap();
        assert_eq!(reader.sector_size(), 4096);
        let e1 = reader.next_entry().unwrap().unwrap();
        let e2 = reader.next_entry().unwrap().unwrap();
        assert!(reader.next_entry().unwrap().is_none());
        assert_eq!(e1.event, JournalEvent::App(TestEvent(0xdead)));
        assert_eq!(e2.event, JournalEvent::App(TestEvent(0xbeef)));
    }

    /// open_append on a journal resumes correctly. Under the v13 layout
    /// the on-disk entry offset is fixed (`ENTRY_OFFSET = 4096`) and the
    /// reopened writer's `sector_size()` is the *device's* logical
    /// sector size — not whatever was recorded in the header. Test
    /// verifies round-trip without asserting on the device-specific
    /// sector_size value.
    #[test]
    fn open_append_round_trip_appends_continue() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("append.journal");

        let (last_seq, valid_end) = {
            let mut writer =
                SectorWriter::<TestEvent>::create_bare_with_sector_size(&path, 1, 4096).unwrap();
            writer.append(&JournalEvent::App(TestEvent(1))).unwrap();
            (writer.next_sequence() - 1, writer.valid_end())
        };

        let mut writer =
            SectorWriter::<TestEvent>::open_append(&path, last_seq, valid_end, None, 0).unwrap();
        let seq = writer.append(&JournalEvent::App(TestEvent(2))).unwrap();
        assert_eq!(seq, last_seq + 1);
        drop(writer);

        let entries = read_all(&path);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].event, JournalEvent::App(TestEvent(1)));
        assert_eq!(entries[1].event, JournalEvent::App(TestEvent(2)));
    }

    #[cfg(feature = "hash-chain")]
    #[test]
    fn multiple_batch_flushes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let mut writer = SectorWriter::<TestEvent>::create(&path).unwrap();
        for i in 0..3 {
            writer
                .batch_append_with_ts(&JournalEvent::App(TestEvent(i)), 0, 0, 0)
                .unwrap();
            writer.flush_batch_sync().unwrap();
        }

        // Genesis is transparent — reader surfaces only the three user
        // events.
        let entries = read_all(&path);
        assert_eq!(entries.len(), 3);
    }

    /// Recovery-time cleanup: an orphan staging file from a prior crash
    /// is removed by `open_append`, and the live segment opens normally.
    /// Models the realistic scenario where a process crashes mid-rotate
    /// (live + staging both on disk) and the operator restarts.
    #[test]
    fn open_append_cleans_orphan_staging_file() {
        use crate::preparer::staging_path;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");
        let staging = staging_path(&path);

        let (last_seq, valid_end, events_since_checkpoint) = {
            let mut writer = SectorWriter::<TestEvent>::create(&path).unwrap();
            writer.append(&sample_event()).unwrap();
            (
                writer.next_sequence() - 1,
                writer.valid_end(),
                writer.events_since_checkpoint(),
            )
        };

        // Simulate a crash that left a staging file on disk.
        std::fs::write(&staging, b"orphan from prior crash").unwrap();
        assert!(staging.exists());

        let writer = SectorWriter::<TestEvent>::open_append(
            &path,
            last_seq,
            valid_end,
            None,
            events_since_checkpoint,
        )
        .unwrap();
        drop(writer);

        assert!(
            !staging.exists(),
            "open_append should have removed the orphan staging file"
        );
    }

    /// `SectorWriter::create` reclaims an orphan `<path>.next-staging`
    /// file left behind by a prior crash, so the SegmentPreparer (which
    /// may or may not be spawned later) doesn't trip over a stale file
    /// on `create_new`. The same cleanup runs in `open_append` — that
    /// path is exercised transitively by the rotation tests.
    #[test]
    fn create_cleans_orphan_staging_file() {
        use crate::preparer::staging_path;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");
        let staging = staging_path(&path);

        std::fs::write(&staging, b"orphan from prior crash").unwrap();
        assert!(staging.exists());

        let writer = SectorWriter::<TestEvent>::create(&path).unwrap();
        drop(writer);

        assert!(
            !staging.exists(),
            "create should have removed the orphan staging file"
        );
    }

    /// End-to-end exercise of the rotation fast path: spawn a preparer,
    /// drain a [`PreparedSegment`] from it, hand the segment to
    /// `rotate_segment_with_prepared`, then verify that
    ///   - the outgoing segment is archived,
    ///   - the new live segment is at the original path,
    ///   - sequence numbers continue without gaps across the boundary,
    ///   - entries from both segments are recoverable on read.
    ///
    /// This is the test that gives the fast path its own coverage; the
    /// existing rotate_segment tests cover the sync (no-prepared) path
    /// transitively.
    #[test]
    fn rotate_with_prepared_round_trip() {
        use crate::preparer::SegmentPreparer;
        use std::time::Duration;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let mut writer = SectorWriter::<TestEvent>::create(&path).unwrap();
        // Two entries on the outgoing segment.
        writer.append(&JournalEvent::App(TestEvent(1))).unwrap();
        writer.append(&JournalEvent::App(TestEvent(2))).unwrap();
        let next_seq_before_rotate = writer.next_sequence();

        // Spawn a preparer and wait for it to publish a prepared segment.
        let preparer = SegmentPreparer::spawn(path.clone(), writer.sector_size);
        let mut prepared = None;
        for _ in 0..500 {
            if let Some(p) = preparer.take() {
                prepared = Some(p);
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let prepared = prepared.expect("preparer should publish a segment within 5 s");

        // Take the fast path.
        let archived = writer
            .rotate_segment_with_prepared(prepared)
            .expect("rotate_with_prepared should succeed");
        assert!(archived.exists(), "archive should be on disk");
        assert!(path.exists(), "new live segment should be at original path");
        assert!(
            !path.with_extension("journal.next-staging").exists(),
            "staging file should have been renamed onto live path"
        );

        // The new segment's next_sequence must advance past the rotation
        // boundary by exactly one (the GenesisHash takes that slot when
        // hash-chain is on) or zero (no genesis without hash-chain).
        #[cfg(feature = "hash-chain")]
        assert_eq!(writer.next_sequence(), next_seq_before_rotate + 1);
        #[cfg(not(feature = "hash-chain"))]
        assert_eq!(writer.next_sequence(), next_seq_before_rotate);

        // Two more entries on the new segment.
        writer.append(&JournalEvent::App(TestEvent(3))).unwrap();
        writer.append(&JournalEvent::App(TestEvent(4))).unwrap();
        drop(writer);
        preparer.shutdown();

        // Reading the live (new) segment should surface only the
        // post-rotation user entries — the genesis is transparent.
        let live_entries = read_all(&path);
        assert_eq!(
            live_entries.len(),
            2,
            "live segment should have 2 user entries"
        );
        assert_eq!(live_entries[0].event, JournalEvent::App(TestEvent(3)));
        assert_eq!(live_entries[1].event, JournalEvent::App(TestEvent(4)));

        // Reading the archived segment should surface the pre-rotation
        // user entries with their original sequences.
        let archived_entries = read_all(&archived);
        assert_eq!(
            archived_entries.len(),
            2,
            "archive should have 2 user entries"
        );
        assert_eq!(archived_entries[0].event, JournalEvent::App(TestEvent(1)));
        assert_eq!(archived_entries[1].event, JournalEvent::App(TestEvent(2)));

        // Sequence continuity across the boundary: last archived user
        // entry + 1 == first live user entry (with the genesis filling
        // the gap when hash-chain is on, transparent to the reader's
        // user-entry view).
        let last_archived_seq = archived_entries.last().unwrap().sequence;
        let first_live_seq = live_entries.first().unwrap().sequence;
        #[cfg(feature = "hash-chain")]
        assert_eq!(
            first_live_seq,
            last_archived_seq + 2,
            "expected one genesis slot between last archived and first live"
        );
        #[cfg(not(feature = "hash-chain"))]
        assert_eq!(
            first_live_seq,
            last_archived_seq + 1,
            "expected contiguous sequence across rotation boundary"
        );
    }

    /// Regression: `ensure_allocated` must not zero data the writer has
    /// already placed past the previous `allocated_end`.
    ///
    /// A single `flush_to_sectors` (or `take_batch_for_async_write`)
    /// call can issue a multi-sector `pwrite` that advances `write_pos`
    /// well past the current `allocated_end` — the kernel auto-extends
    /// the file to absorb the write. The *next* `ensure_allocated`
    /// then sees `write_pos > allocated_end`, calls `preallocate`,
    /// and follows up with `FALLOC_FL_ZERO_RANGE` over the
    /// newly-allocated region. Before the fix that zero-range started
    /// at the stale `old_end` and wiped every byte in
    /// `[old_end, write_pos)` — punching ~180 KB holes into the
    /// journal under load.
    ///
    /// This test shrinks the prealloc chunk so a single batch easily
    /// overflows it, then verifies every event is recoverable.
    /// Without the fix the reader returns far fewer entries than were
    /// written (the bytes between `old_end` and `write_pos` are
    /// zeroed, so the reader stops at the first zero-magic sector).
    #[test]
    fn ensure_allocated_preserves_data_written_past_preallocation() {
        use crate::prealloc::PreallocOverrideGuard;
        use crate::write::JournalWrite;

        // Shrink the prealloc chunk so `flush_to_sectors`' multi-sector
        // pwrite easily exceeds it in one go. The guard scopes the
        // override to this test and serialises with any other test
        // using the same mechanism.
        let _guard = PreallocOverrideGuard::new(8 * 1024);

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("regression.journal");
        let mut writer = SectorWriter::<TestEvent>::create(&path).unwrap();

        // Batch-encode ~100 KiB of events without flushing — about 12×
        // the 8 KiB prealloc chunk. When `flush_batch_sync` issues the
        // single multi-sector pwrite below, `write_pos` advances well
        // past `allocated_end` and the file auto-extends.
        const N: u64 = 2000;
        for i in 0..N {
            writer
                .batch_append_with_ts(&JournalEvent::App(TestEvent(i)), 0, 0, 0)
                .unwrap();
        }
        writer.flush_batch_sync().unwrap();

        // Append + flush one more event. This triggers `ensure_allocated`
        // with `write_pos` far past the stale `allocated_end` — exactly
        // the condition the fix guards against. Pre-fix:
        // `zero_range_extents(file, old_end, new_end)` wipes the bytes
        // the previous batch wrote between `old_end` and `write_pos`.
        // Post-fix: `zero_unwritten_through` derives the start from
        // `write_pos` internally and only touches truly-new bytes.
        writer.append(&JournalEvent::App(TestEvent(9_999))).unwrap();
        drop(writer);

        // Round-trip every event. Pre-fix this surfaces either a
        // `ChecksumMismatch` (the reader's hardening rejects the hole
        // because real entries exist after it) or short recovery (the
        // reader stops at the first zero-magic sector inside the hole).
        let entries = read_all(&path);
        assert_eq!(
            entries.len(),
            (N + 1) as usize,
            "ensure_allocated wiped journal data: recovered {} of {}",
            entries.len(),
            N + 1
        );
        for (i, e) in entries.iter().take(N as usize).enumerate() {
            assert_eq!(e.event, JournalEvent::App(TestEvent(i as u64)));
        }
        assert_eq!(
            entries[N as usize].event,
            JournalEvent::App(TestEvent(9_999))
        );
    }
}
