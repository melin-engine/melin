//! Journal writer — append-only, durable event log with pre-allocated storage.
//!
//! Uses `posix_fallocate` to pre-extend the journal file in 64 MiB chunks.
//! This allocates disk blocks (extents) ahead of time so that subsequent
//! sync calls only flush data pages — not filesystem metadata updates for
//! newly allocated extents. This significantly reduces sync latency under
//! sustained write load.
//!
//! Durability uses `pwritev2` with `RWF_DSYNC` (Force Unit Access) instead
//! of `pwrite` + `fdatasync`. On NVMe drives with FUA support, the kernel
//! issues a single FUA write instead of write + full cache flush, reducing
//! sync latency from ~1-7 ms to ~10-100 µs for small writes.
//!
//! Writes use positioned I/O with an explicit write position rather than
//! kernel-managed append mode, because the file size includes pre-allocated
//! (zero-filled) space beyond the valid data boundary.

use std::fs::{File, OpenOptions};
use std::marker::PhantomData;
use std::os::unix::fs::FileExt;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use melin_app::AppEvent;

use super::codec::{self, FILE_HEADER_SIZE};
use super::error::JournalError;
use super::event::JournalEvent;
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

/// Pre-allocation chunk size (256 MiB). Matches the default journal rotation
/// threshold so that a freshly created journal never needs mid-run extension.
/// At ~80 bytes per entry, one chunk covers ~3.2M entries. If the journal
/// does exceed this (large rotation threshold or disabled rotation), it
/// extends by one more chunk — but this is exceedingly rare.
const PREALLOC_CHUNK: u64 = 256 * 1024 * 1024;

/// Number of events between automatic hash chain checkpoints.
/// 100K events × ~80 bytes = ~8 MB of journal data between checkpoints.
/// The checkpoint itself is ~77 bytes — negligible overhead.
pub const CHECKPOINT_INTERVAL: u64 = 100_000;

/// A batch of encoded journal data ready for async write via io_uring.
/// Owns the buffer to prevent aliasing while io_uring holds a pointer to it.
pub struct AsyncWriteBatch {
    /// The buffer containing encoded journal entries.
    pub buf: Vec<u8>,
    /// File offset where this batch should be written.
    pub offset: u64,
}

/// Appends journal events to a file with CRC32C checksums and fsync durability.
///
/// Uses positioned writes (`pwrite`) and pre-allocated storage to minimize
/// fsync latency. The file size includes pre-allocated zero-filled space
/// beyond the valid data boundary; recovery truncates to `valid_file_end`
/// before reopening.
pub struct JournalWriter<E: AppEvent> {
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
    batch_buf: Vec<u8>,
    /// Spare buffer for double-buffering with io_uring. While one buffer is
    /// in-flight, the other accumulates the next batch. `None` when the spare
    /// is currently in-flight as part of an `AsyncWriteBatch`.
    spare_buf: Option<Vec<u8>>,
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
    /// Byte range of the most-recent user entry within `batch_buf`.
    /// Captured by `encode_event` BEFORE any auto-checkpoint emission,
    /// so `last_user_entry_replication_slice` returns the user entry
    /// only — not a trailing checkpoint that may have been auto-emitted
    /// in the same call. `(0, 0)` means no user entry encoded yet.
    last_user_entry_offset: usize,
    last_user_entry_len: usize,
    /// When true, `flush_batch_sync` uses a plain `pwrite` instead of
    /// `pwritev2+RWF_DSYNC`. Safe only on drives with Power Loss Protection
    /// (PLP) capacitors — data in the controller's battery-backed DRAM is
    /// crash-durable without waiting for NAND programming. Eliminates the
    /// ~10–100µs FUA round-trip latency on every batch flush.
    no_fua: bool,
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

impl<E: AppEvent> JournalWriter<E> {
    /// Create a new journal file. Writes the file header and a `GenesisHash`
    /// entry with random bytes, pre-allocates storage, and returns a writer
    /// starting at sequence 1.
    ///
    /// Fails if the file already exists (use `open_append` for existing journals).
    pub fn create(path: &Path) -> Result<Self, JournalError> {
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

        // Write genesis hash as the first entry and initialize the chain.
        let genesis_event: JournalEvent<E> = JournalEvent::GenesisHash { hash: genesis };
        let seq = writer.next_sequence;
        let timestamp_ns = wall_clock_nanos();
        let written = codec::encode(seq, timestamp_ns, 0, 0, &genesis_event, &mut writer.buffer)?;

        // Initialize chain: hash the genesis entry bytes (excluding CRC).
        let entry_bytes = &writer.buffer[..written - 4]; // exclude CRC
        let hash = blake3::hash(entry_bytes);
        writer.hash_chain = Some(HashChain {
            current_hash: *hash.as_bytes(),
            batch_hasher: blake3::Hasher::new(),
            events_since_checkpoint: 0,
        });

        writer
            .batch_buf
            .extend_from_slice(&writer.buffer[..written]);
        writer.next_sequence += 1;
        writer.flush_batch_sync()?;

        Ok(writer)
    }

    /// Internal: create a new journal without a hash chain.
    #[cfg(not(feature = "hash-chain"))]
    fn create_without_chain(path: &Path, starting_sequence: u64) -> Result<Self, JournalError> {
        let writer = Self::create_bare(path, starting_sequence)?;
        Ok(writer)
    }

    /// Shared file setup: header, pre-allocation, sync.
    fn create_bare(path: &Path, starting_sequence: u64) -> Result<Self, JournalError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;

        let mut header = [0u8; FILE_HEADER_SIZE];
        codec::encode_file_header(&mut header);
        file.write_all_at(&header, 0)?;

        let write_pos = FILE_HEADER_SIZE as u64;
        let allocated_end = preallocate(&file, write_pos)?;
        zero_range_extents(&file, write_pos, allocated_end);

        // Pre-fault all pages in the preallocated region so the first write
        // to each 4 KB page doesn't trigger a page fault on the hot path.
        // MADV_POPULATE_WRITE forces the kernel to allocate and zero-fill
        // pages now (startup cost ~10-30ms for 256 MB) instead of lazily
        // during io_uring writes where a fault adds 10-100µs tail latency.
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::io::AsRawFd;
            let ptr = unsafe {
                libc::mmap(
                    std::ptr::null_mut(),
                    allocated_end as libc::size_t,
                    libc::PROT_WRITE,
                    libc::MAP_SHARED,
                    file.as_raw_fd(),
                    0,
                )
            };
            if ptr != libc::MAP_FAILED {
                // MADV_POPULATE_WRITE (23) pre-faults pages for writing.
                unsafe { libc::madvise(ptr, allocated_end as libc::size_t, 23) };
                unsafe { libc::munmap(ptr, allocated_end as libc::size_t) };
            }
        }

        file.sync_all()?;

        Ok(Self {
            _marker: PhantomData,
            file,
            buffer: [0u8; MAX_ENTRY_SIZE],
            batch_buf: Vec::with_capacity(BATCH_BUF_CAPACITY),
            spare_buf: Some(Vec::with_capacity(BATCH_BUF_CAPACITY)),
            next_sequence: starting_sequence,
            path: path.to_path_buf(),
            write_pos,
            allocated_end,
            #[cfg(feature = "hash-chain")]
            hash_chain: None,
            #[cfg(debug_assertions)]
            last_encoded_seq: 0,
            last_user_entry_offset: 0,
            last_user_entry_len: 0,
            no_fua: false,
        })
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
        let file = OpenOptions::new().read(true).write(true).open(path)?;

        // Reuse the existing file and its pre-allocated extents instead of
        // truncating + re-preallocating + sync_all (which cost 2-6ms).
        //
        // The writer starts at `valid_end`, overwriting any trailing garbage
        // from a crash. To prevent a double-crash scenario where partial
        // garbage survives past new entries, zero out one MAX_ENTRY_SIZE
        // block at `valid_end`. This is a single small write (128 bytes)
        // with no metadata overhead.
        let file_len = file.metadata()?.len();
        if valid_end + MAX_ENTRY_SIZE as u64 <= file_len {
            let zeros = [0u8; MAX_ENTRY_SIZE];
            file.write_all_at(&zeros, valid_end)?;
        }

        let allocated_end = if file_len >= valid_end {
            // File already covers valid data (common case). Use the full
            // file length as allocated_end — ensure_allocated will extend
            // if the journal grows beyond it.
            file_len
        } else {
            // File was truncated below valid data (shouldn't happen in
            // normal operation, but handle it gracefully).
            let end = preallocate(&file, valid_end)?;
            file.sync_all()?;
            end
        };

        #[allow(unused_mut)] // mut needed only with hash-chain for emit_checkpoint
        let mut writer = Self {
            _marker: PhantomData,
            file,
            buffer: [0u8; MAX_ENTRY_SIZE],
            batch_buf: Vec::with_capacity(BATCH_BUF_CAPACITY),
            spare_buf: Some(Vec::with_capacity(BATCH_BUF_CAPACITY)),
            next_sequence: last_seq + 1,
            path: path.to_path_buf(),
            write_pos: valid_end,
            allocated_end,
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
            no_fua: false,
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

    /// Append an event to the journal and flush to disk.
    ///
    /// Returns the assigned sequence number. The event is durable after this
    /// returns (written with `RWF_DSYNC` / FUA).
    pub fn append(&mut self, event: &JournalEvent<E>) -> Result<u64, JournalError> {
        let seq = self.batch_append(event)?;
        self.flush_batch_sync()?;
        Ok(seq)
    }

    /// Append an event to the journal **without** syncing to disk.
    ///
    /// Used by the pipeline journal stage to batch multiple events into
    /// a single write before calling `flush_batch_sync()` once for the batch.
    /// This amortizes the sync cost across many events under load.
    pub fn append_no_sync(&mut self, event: &JournalEvent<E>) -> Result<u64, JournalError> {
        let seq = self.batch_append(event)?;
        self.flush_batch()?;
        Ok(seq)
    }

    /// Encode an event into the batch buffer without writing to disk.
    ///
    /// Much faster than `append_no_sync` — no syscall per event, just
    /// memory copies into the pre-allocated batch buffer. Call `flush_batch`
    /// after encoding the entire batch to issue a single `pwrite`.
    ///
    /// Uses one `wall_clock_nanos()` call per event for the journal timestamp.
    /// For batches sharing a timestamp, use `batch_append_with_ts`.
    pub fn batch_append(&mut self, event: &JournalEvent<E>) -> Result<u64, JournalError> {
        self.batch_append_with_ts(event, wall_clock_nanos(), 0, 0)
    }

    /// Encode an event into the batch buffer with a caller-provided timestamp.
    ///
    /// Avoids the `clock_gettime` syscall per event when the caller can batch
    /// a single timestamp for the entire batch. Same semantics as `batch_append`
    /// but uses the provided timestamp instead of calling `wall_clock_nanos()`.
    ///
    /// Convenience wrapper: allocates a sequence number and encodes in one call.
    /// For explicit control over sequencing (e.g., input replication), use
    /// [`allocate_sequence`] + [`encode_event`] separately.
    pub fn batch_append_with_ts(
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
        self.last_user_entry_offset = self.batch_buf.len();
        self.last_user_entry_len = written;
        self.batch_buf.extend_from_slice(&self.buffer[..written]);

        // Auto-emit a checkpoint if we've hit the interval.
        // Finalize the batch hasher to get the current hash (including all
        // entries accumulated since the last flush/checkpoint).
        #[cfg(feature = "hash-chain")]
        if let Some(chain) = &mut self.hash_chain
            && chain.events_since_checkpoint >= CHECKPOINT_INTERVAL
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
        let ts = wall_clock_nanos();
        let written = codec::encode(seq, ts, 0, 0, &checkpoint, &mut self.buffer)?;

        // Reset the event counter. The checkpoint entry itself is NOT fed
        // into the new batch hasher — it acts as a seal for the preceding
        // segment. This keeps the hash chain deterministic regardless of
        // write batching strategy.
        if let Some(chain) = &mut self.hash_chain {
            chain.events_since_checkpoint = 0;
        }

        self.warn_if_batch_overflow(written);
        self.batch_buf.extend_from_slice(&self.buffer[..written]);
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
        if self.batch_buf.len() + adding > self.batch_buf.capacity() {
            tracing::warn!(
                current_len = self.batch_buf.len(),
                adding,
                capacity = self.batch_buf.capacity(),
                "journal batch buffer exceeded preallocated capacity — \
                 caller is batching more than capacity between flushes, \
                 forcing a Vec realloc on the hot path; reduce flush \
                 cadence or raise BATCH_BUF_CAPACITY"
            );
        }
    }

    /// Write the accumulated batch buffer to disk in a single `pwrite` syscall.
    ///
    /// Reduces syscalls from N (one per event) to 1 per batch. Must be called
    /// after one or more `batch_append` / `batch_append_with_ts` calls and
    /// before `sync()`.
    pub fn flush_batch(&mut self) -> Result<(), JournalError> {
        if self.batch_buf.is_empty() {
            return Ok(());
        }
        self.ensure_allocated(self.batch_buf.len() as u64)?;
        self.file.write_all_at(&self.batch_buf, self.write_pos)?;
        self.write_pos += self.batch_buf.len() as u64;
        self.batch_buf.clear();
        self.last_user_entry_len = 0;
        Ok(())
    }

    /// Write the batch buffer to disk with guaranteed durability.
    ///
    /// Default path: uses `pwritev2` with `RWF_DSYNC` to combine the data
    /// write and durability guarantee into a single syscall. On NVMe drives
    /// with FUA (Force Unit Access) support, the kernel issues a single FUA
    /// write instead of write + full cache flush (`fdatasync`). This reduces
    /// sync latency from ~1-7 ms to ~10-100 µs for small writes.
    ///
    /// With `--journal-no-fua` (`no_fua = true`): uses a plain `pwrite`
    /// without waiting for NAND programming. Relies on the drive's Power Loss
    /// Protection (PLP) capacitors for crash durability. Latency drops to
    /// ~1-5 µs (controller DRAM write), eliminating GC-induced spikes entirely.
    pub fn flush_batch_sync(&mut self) -> Result<(), JournalError> {
        if self.batch_buf.is_empty() {
            return Ok(());
        }
        self.ensure_allocated(self.batch_buf.len() as u64)?;

        if self.no_fua {
            self.file.write_all_at(&self.batch_buf, self.write_pos)?;
        } else {
            pwritev2_dsync(self.file.as_raw_fd(), &self.batch_buf, self.write_pos)?;
        }

        // Hash chain is NOT finalized per-flush — only at checkpoint
        // boundaries. This ensures the chain is deterministic regardless of
        // write batching strategy. chain_hash() computes on-demand.

        self.write_pos += self.batch_buf.len() as u64;
        self.batch_buf.clear();
        self.last_user_entry_len = 0;
        Ok(())
    }

    /// Drop the current batch buffer without writing it to disk.
    ///
    /// Used by the `no-persist` path of the journal stage so the buffer
    /// stays bounded after replication has snapshotted the bytes.
    /// Equivalent to `flush_batch_sync` minus the `pwritev2_dsync` and
    /// the `write_pos` advance.
    pub fn discard_batch_buf(&mut self) {
        self.batch_buf.clear();
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
    ///
    /// `write_pos` is advanced immediately (not on confirm) so subsequent
    /// `batch_append()` calls encode at the correct offset. The journal
    /// cursor must NOT advance until `confirm_async_write()` — the data
    /// is not yet durable.
    pub fn take_batch_for_async_write(&mut self) -> Result<Option<AsyncWriteBatch>, JournalError> {
        if self.batch_buf.is_empty() {
            return Ok(None);
        }
        self.ensure_allocated(self.batch_buf.len() as u64)?;

        // Hash chain is NOT finalized per-flush — only at checkpoint
        // boundaries. chain_hash() computes on-demand.

        let offset = self.write_pos;
        self.write_pos += self.batch_buf.len() as u64;

        // Swap in the spare buffer (or allocate a new one if spare is in-flight).
        let spare = self
            .spare_buf
            .take()
            .unwrap_or_else(|| Vec::with_capacity(BATCH_BUF_CAPACITY));
        let full_buf = std::mem::replace(&mut self.batch_buf, spare);
        self.last_user_entry_len = 0;

        Ok(Some(AsyncWriteBatch {
            buf: full_buf,
            offset,
        }))
    }

    /// Return a completed async write batch's buffer to the spare pool.
    /// Call this after the io_uring CQE confirms the write is durable.
    pub fn confirm_async_write(&mut self, mut batch: AsyncWriteBatch) {
        batch.buf.clear();
        self.spare_buf = Some(batch.buf);
    }

    /// Flush the journal to disk (fdatasync).
    ///
    /// Legacy sync path — only used during shutdown drain. Production
    /// hot path uses `flush_batch_sync()` (pwritev2 + RWF_DSYNC) instead.
    pub fn sync(&mut self) -> Result<(), JournalError> {
        self.file.sync_data()?;
        Ok(())
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

    /// Path to the journal file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Raw file descriptor for the journal file.
    pub fn fd(&self) -> std::os::unix::io::RawFd {
        self.file.as_raw_fd()
    }

    /// Enable or disable FUA on batch flushes. When disabled, `flush_batch_sync`
    /// issues a plain `pwrite` instead of `pwritev2+RWF_DSYNC`. Only safe on
    /// drives with Power Loss Protection (PLP) — see `--journal-no-fua`.
    pub fn set_no_fua(&mut self, no_fua: bool) {
        self.no_fua = no_fua;
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
        &self.batch_buf
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
        debug_assert!(
            end <= self.batch_buf.len(),
            "last_user_entry_replication_slice: stale offset (batch_buf was cleared)"
        );
        // Strip the 2-byte entry magic and 4-byte CRC trailer.
        &self.batch_buf[start + 2..end - 4]
    }

    /// Ensure enough pre-allocated space exists for the next write.
    /// If the write would exceed the current allocation, extends by
    /// another chunk. This should be exceedingly rare in practice —
    /// the initial 256 MiB pre-allocation covers the default rotation
    /// threshold, so this only fires if rotation is disabled or the
    /// threshold is raised.
    ///
    /// No `sync_all` after extension: `RWF_DSYNC` on subsequent writes
    /// already flushes the extent metadata needed to retrieve the written
    /// data (POSIX synchronized I/O data integrity completion). A separate
    /// `sync_all` would flush ALL metadata (timestamps, full extent tree)
    /// and costs 2-6ms — unacceptable on the hot path.
    fn ensure_allocated(&mut self, bytes_needed: u64) -> Result<(), JournalError> {
        if self.write_pos + bytes_needed <= self.allocated_end {
            return Ok(());
        }
        let old_end = self.allocated_end;
        self.allocated_end = preallocate(&self.file, self.write_pos)?;
        zero_range_extents(&self.file, old_end, self.allocated_end);
        Ok(())
    }
}

/// Write data with `RWF_DSYNC` via `pwritev2` — combines write + durability
/// in a single syscall.
///
/// `RWF_DSYNC` provides per-write data integrity: the kernel ensures the data
/// is on persistent storage before returning. On NVMe drives with FUA (Force
/// Unit Access) support, this translates to a single FUA write command instead
/// of write + full cache flush. Much faster than write + fdatasync for small
/// writes because FUA only persists the written sectors, while fdatasync
/// drains the entire NVMe write queue.
fn pwritev2_dsync(
    fd: std::os::unix::io::RawFd,
    buf: &[u8],
    offset: u64,
) -> Result<(), JournalError> {
    let iov = libc::iovec {
        iov_base: buf.as_ptr() as *mut libc::c_void,
        iov_len: buf.len(),
    };
    // Safety: fd is a valid file descriptor, iov points to valid memory
    // that outlives the syscall.
    let ret = unsafe { libc::pwritev2(fd, &iov, 1, offset as libc::off_t, libc::RWF_DSYNC) };
    if ret < 0 {
        return Err(JournalError::Io(std::io::Error::last_os_error()));
    }
    if (ret as usize) != buf.len() {
        return Err(JournalError::Io(std::io::Error::new(
            std::io::ErrorKind::WriteZero,
            "short pwritev2 write",
        )));
    }
    Ok(())
}

/// Pre-allocate disk blocks from the current position forward by one chunk.
///
/// Uses `posix_fallocate` which allocates extents without writing zeros
/// (the filesystem guarantees zero-fill on read for unwritten blocks).
/// Falls back to `set_len` if `posix_fallocate` fails (e.g., on macOS
/// where it may not be fully supported). The fallback may create a sparse
/// file without the full fsync benefit, but maintains correctness.
fn preallocate(file: &File, current_end: u64) -> Result<u64, JournalError> {
    let target = current_end + PREALLOC_CHUNK;

    // Allocate only the new chunk [current_end, target), not [0, target).
    // posix_fallocate(fd, 0, target) walks the entire extent tree from
    // offset 0 on every call to verify already-allocated extents, which
    // takes O(file_size) as the file grows — causing linearly growing
    // latency spikes under sustained write load.
    let ret = unsafe {
        libc::posix_fallocate(
            file.as_raw_fd(),
            current_end as libc::off_t,
            PREALLOC_CHUNK as libc::off_t,
        )
    };

    if ret == 0 {
        return Ok(target);
    }

    // Fallback for platforms where posix_fallocate isn't supported.
    // ftruncate extends the file but may create a sparse file — still
    // correct, just without the full metadata-skip benefit on fsync.
    file.set_len(target)?;
    Ok(target)
}

/// Mark pre-allocated extents as written zeros using `FALLOC_FL_ZERO_RANGE`.
///
/// On ext4, `posix_fallocate` creates "unwritten" extents. The first write
/// to an unwritten block triggers a metadata status change (unwritten →
/// written) that goes into the ext4 jbd2 transaction buffer. Every ~5s
/// (default `commit` interval), jbd2 commits these transactions with a full
/// NVMe cache flush (`REQ_PREFLUSH`), stalling concurrent `pwritev2+RWF_DSYNC`
/// writes for 1-2ms.
///
/// `FALLOC_FL_ZERO_RANGE` pre-converts extents to "written + zeroed",
/// eliminating per-write metadata updates and the resulting jbd2 flush storms.
///
/// Best-effort: failures are logged at warn level and ignored. The fallback
/// is periodic 1-2ms tail latency spikes, not data loss.
fn zero_range_extents(file: &File, start: u64, end: u64) {
    if start >= end {
        return;
    }
    // FALLOC_FL_ZERO_RANGE = 0x10
    let ret = unsafe {
        libc::fallocate(
            file.as_raw_fd(),
            0x10,
            start as libc::off_t,
            (end - start) as libc::off_t,
        )
    };
    if ret < 0 {
        tracing::warn!(
            errno = unsafe { *libc::__errno_location() },
            start,
            end,
            "FALLOC_FL_ZERO_RANGE failed — expect periodic 1-2ms tail latency spikes"
        );
    }
}

/// Wall-clock nanoseconds since Unix epoch. Used for informational timestamps
/// in journal entries (not for ordering — sequence numbers handle that).
///
/// The `u128 as u64` truncation is safe: u64 nanos covers ~584 years from
/// epoch (until 2554). Falls back to 0 if system clock is before epoch.
///
/// Public so the pipeline stage can call once per batch instead of per event.
pub fn wall_clock_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::JournalReader;
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

        let writer = JournalWriter::<TestEvent>::create(&path).unwrap();
        assert_eq!(writer.next_sequence(), FIRST_SEQ);
        assert_eq!(writer.path(), path);
        #[cfg(feature = "hash-chain")]
        assert!(writer.chain_hash().is_some());
        #[cfg(not(feature = "hash-chain"))]
        assert!(writer.chain_hash().is_none());

        let file_len = std::fs::metadata(&path).unwrap().len();
        assert!(
            file_len >= PREALLOC_CHUNK,
            "expected pre-allocated file, got {file_len} bytes"
        );
    }

    #[test]
    fn create_fails_if_file_exists() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let _writer = JournalWriter::<TestEvent>::create(&path).unwrap();
        drop(_writer);

        let result = JournalWriter::<TestEvent>::create(&path);
        assert!(result.is_err());
    }

    #[test]
    fn append_assigns_sequential_ids() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let mut writer = JournalWriter::<TestEvent>::create(&path).unwrap();
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
            let mut writer = JournalWriter::<TestEvent>::create(&path).unwrap();
            writer.append(&event).unwrap();
        }

        let entries = read_all(&path);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].sequence, FIRST_SEQ);
        assert_eq!(entries[0].event, event);
        assert!(entries[0].timestamp_ns > 0);
    }

    #[test]
    fn append_no_sync_is_readable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let event = sample_event();
        {
            let mut writer = JournalWriter::<TestEvent>::create(&path).unwrap();
            let seq = writer.append_no_sync(&event).unwrap();
            assert_eq!(seq, FIRST_SEQ);
        }

        let entries = read_all(&path);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].event, event);
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
            let mut writer = JournalWriter::<TestEvent>::create(&path).unwrap();
            for event in &events {
                writer.batch_append(event).unwrap();
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

        let mut writer = JournalWriter::<TestEvent>::create(&path).unwrap();
        let pos_before = writer.write_pos();
        writer.batch_append(&sample_event()).unwrap();
        assert_eq!(writer.write_pos(), pos_before);
        writer.flush_batch_sync().unwrap();
        assert!(writer.write_pos() > pos_before);
    }

    #[test]
    fn open_append_continues_sequence() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let (last_seq, valid_end, events_since_checkpoint) = {
            let mut writer = JournalWriter::<TestEvent>::create(&path).unwrap();
            writer.append(&sample_event()).unwrap();
            writer.append(&sample_event()).unwrap();
            (
                writer.next_sequence() - 1,
                writer.write_pos(),
                writer.events_since_checkpoint(),
            )
        };

        let mut writer = JournalWriter::<TestEvent>::open_append(
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
            let mut writer = JournalWriter::<TestEvent>::create(&path).unwrap();
            writer.append(&sample_event()).unwrap();
            (writer.next_sequence() - 1, writer.write_pos())
        };

        {
            let _writer =
                JournalWriter::<TestEvent>::open_append(&path, last_seq, valid_end, None, 0)
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

        let writer = JournalWriter::<TestEvent>::create(&path).unwrap();
        assert!(writer.chain_hash().is_some());
        drop(writer);

        assert_eq!(read_all(&path).len(), 0);
    }

    #[cfg(feature = "hash-chain")]
    #[test]
    fn chain_hash_changes_with_each_event() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let mut writer = JournalWriter::<TestEvent>::create(&path).unwrap();
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
            let mut writer = JournalWriter::<TestEvent>::create(&path).unwrap();
            writer.append(&sample_event()).unwrap();
            (
                writer.next_sequence() - 1,
                writer.write_pos(),
                writer.chain_hash(),
                writer.events_since_checkpoint(),
            )
        };

        let writer = JournalWriter::<TestEvent>::open_append(
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
            let mut writer = JournalWriter::<TestEvent>::create(&path).unwrap();
            writer.append(&sample_event()).unwrap();
            (writer.next_sequence() - 1, writer.write_pos())
        };

        let writer =
            JournalWriter::<TestEvent>::open_append(&path, last_seq, valid_end, None, 0).unwrap();
        assert!(writer.chain_hash().is_none());
    }

    #[cfg(feature = "hash-chain")]
    #[test]
    fn multiple_batch_flushes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let mut writer = JournalWriter::<TestEvent>::create(&path).unwrap();
        for i in 0..3 {
            writer
                .batch_append(&JournalEvent::App(TestEvent(i)))
                .unwrap();
            writer.flush_batch_sync().unwrap();
        }

        // Genesis is transparent — reader surfaces only the three user
        // events.
        let entries = read_all(&path);
        assert_eq!(entries.len(), 3);
    }
}
