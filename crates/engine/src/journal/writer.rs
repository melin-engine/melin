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
use std::os::unix::fs::FileExt;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

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
pub struct JournalWriter {
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

impl JournalWriter {
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
        let genesis_event = JournalEvent::GenesisHash { hash: genesis };
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
            let mut reader = JournalReader::open(path)?;
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
    pub fn append(&mut self, event: &JournalEvent) -> Result<u64, JournalError> {
        let seq = self.batch_append(event)?;
        self.flush_batch_sync()?;
        Ok(seq)
    }

    /// Append an event to the journal **without** syncing to disk.
    ///
    /// Used by the pipeline journal stage to batch multiple events into
    /// a single write before calling `flush_batch_sync()` once for the batch.
    /// This amortizes the sync cost across many events under load.
    pub fn append_no_sync(&mut self, event: &JournalEvent) -> Result<u64, JournalError> {
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
    pub fn batch_append(&mut self, event: &JournalEvent) -> Result<u64, JournalError> {
        self.batch_append_with_ts(event, wall_clock_nanos(), 0, 0)
    }

    /// Encode an event into the batch buffer with a caller-provided timestamp.
    ///
    /// Avoids the `clock_gettime` syscall per event when the caller can batch
    /// a single timestamp for the entire batch. Same semantics as `batch_append`
    /// but uses the provided timestamp instead of calling `wall_clock_nanos()`.
    pub fn batch_append_with_ts(
        &mut self,
        event: &JournalEvent,
        timestamp_ns: u64,
        key_hash: u64,
        request_seq: u64,
    ) -> Result<u64, JournalError> {
        let seq = self.next_sequence;
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
            chain.events_since_checkpoint += 1;
        }

        self.batch_buf.extend_from_slice(&self.buffer[..written]);
        self.next_sequence += 1;

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

        Ok(seq)
    }

    /// Emit a checkpoint entry into the batch buffer and reset the counter.
    #[cfg(feature = "hash-chain")]
    fn emit_checkpoint(
        &mut self,
        chain_hash: [u8; 32],
        events_since_checkpoint: u64,
    ) -> Result<(), JournalError> {
        let checkpoint = JournalEvent::Checkpoint {
            chain_hash,
            events_since_checkpoint,
        };
        let seq = self.next_sequence;
        let ts = wall_clock_nanos();
        let written = codec::encode(seq, ts, 0, 0, &checkpoint, &mut self.buffer)?;

        // Reset the event counter. The checkpoint entry itself is NOT fed
        // into the new batch hasher — it acts as a seal for the preceding
        // segment. This keeps the hash chain deterministic regardless of
        // write batching strategy.
        if let Some(chain) = &mut self.hash_chain {
            chain.events_since_checkpoint = 0;
        }

        self.batch_buf.extend_from_slice(&self.buffer[..written]);
        self.next_sequence += 1;
        Ok(())
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
        Ok(())
    }

    /// Write the batch buffer to disk with guaranteed durability (FUA).
    ///
    /// Uses `pwritev2` with `RWF_DSYNC` to combine the data write and
    /// durability guarantee into a single syscall. On NVMe drives with
    /// FUA (Force Unit Access) support, the kernel issues a single FUA
    /// write instead of write + full cache flush (`fdatasync`). This
    /// reduces sync latency from ~1-7 ms to ~10-100 µs for small writes.
    ///
    /// Falls back to plain `pwrite` when the `no-fsync` feature is enabled.
    pub fn flush_batch_sync(&mut self) -> Result<(), JournalError> {
        if self.batch_buf.is_empty() {
            return Ok(());
        }
        self.ensure_allocated(self.batch_buf.len() as u64)?;

        #[cfg(not(feature = "no-fsync"))]
        {
            pwritev2_dsync(self.file.as_raw_fd(), &self.batch_buf, self.write_pos)?;
        }
        #[cfg(feature = "no-fsync")]
        {
            self.file.write_all_at(&self.batch_buf, self.write_pos)?;
        }

        // Hash chain is NOT finalized per-flush — only at checkpoint
        // boundaries. This ensures the chain is deterministic regardless of
        // write batching strategy. chain_hash() computes on-demand.

        self.write_pos += self.batch_buf.len() as u64;
        self.batch_buf.clear();
        Ok(())
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

    /// Prepare a raw byte buffer for async writing via io_uring.
    ///
    /// Used by the replica journal stage to write pre-encoded bytes
    /// Reserve a file offset for an upcoming raw async write from the
    /// replication ring, and eagerly advance `write_pos` and
    /// `next_sequence` by the recorded batch size. The caller submits
    /// an io_uring Write against the returned offset using a pointer
    /// into the raw-batch ring slot (no intermediate buffer), and must
    /// not advance the journal cursor until the CQE confirms durability.
    ///
    /// Unlike the primary path's `take_batch_for_async_write`, there is
    /// no owned buffer to carry through the CQE here — the slot memory
    /// is pinned by the raw-batch ring protocol, and the receiver
    /// releases it by dropping the [`super::pipeline::RawBatchSlot`]
    /// handle after the CQE lands. Consequently there is no "confirm"
    /// counterpart: the writer state is already consistent at the end
    /// of this call.
    pub fn reserve_raw_async_write(
        &mut self,
        len: u64,
        entry_count: u64,
    ) -> Result<u64, JournalError> {
        self.ensure_allocated(len)?;
        let offset = self.write_pos;
        self.write_pos += len;
        self.next_sequence += entry_count;
        Ok(offset)
    }

    /// Flush the journal to disk (fdatasync).
    ///
    /// Legacy sync path — only used during shutdown drain. Production
    /// hot path uses `flush_batch_sync()` (pwritev2 + RWF_DSYNC) instead.
    pub fn sync(&mut self) -> Result<(), JournalError> {
        #[cfg(not(feature = "no-fsync"))]
        self.file.sync_data()?;
        Ok(())
    }

    /// Current next sequence number (useful for snapshot coordination).
    pub fn next_sequence(&self) -> u64 {
        self.next_sequence
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

    /// Write pre-encoded journal bytes directly to the file with durability.
    ///
    /// Used by the replication receiver to write bytes received from the
    /// primary without re-encoding. The bytes must be valid journal entries
    /// (the caller is responsible for CRC and sequence validation).
    ///
    /// Advances `write_pos` and `next_sequence` to account for the written
    /// data. Does NOT update the hash chain — the receiver tracks chain
    /// state separately if needed.
    pub fn write_raw_sync(&mut self, data: &[u8], entry_count: u64) -> Result<(), JournalError> {
        if data.is_empty() {
            return Ok(());
        }
        self.ensure_allocated(data.len() as u64)?;

        #[cfg(not(feature = "no-fsync"))]
        {
            pwritev2_dsync(self.file.as_raw_fd(), data, self.write_pos)?;
        }
        #[cfg(feature = "no-fsync")]
        {
            self.file.write_all_at(data, self.write_pos)?;
        }

        self.write_pos += data.len() as u64;
        self.next_sequence += entry_count;
        Ok(())
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
        self.allocated_end = preallocate(&self.file, self.write_pos)?;
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
#[cfg(not(feature = "no-fsync"))]
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
    use std::num::NonZeroU64;

    use super::*;
    use crate::journal::reader::JournalReader;
    use crate::types::*;

    /// First user-event sequence: 2 with hash-chain (genesis takes 1), 1 without.
    #[cfg(feature = "hash-chain")]
    const FIRST_SEQ: u64 = 2;
    #[cfg(not(feature = "hash-chain"))]
    const FIRST_SEQ: u64 = 1;

    fn nz(v: u64) -> NonZeroU64 {
        NonZeroU64::new(v).unwrap()
    }

    fn sample_event() -> JournalEvent {
        JournalEvent::SubmitOrder {
            symbol: Symbol(1),
            order: Order {
                id: OrderId(1),
                account: AccountId(1),
                side: Side::Buy,
                order_type: OrderType::Limit {
                    price: Price(nz(100)),
                    post_only: false,
                },
                time_in_force: TimeInForce::GTC,
                quantity: Quantity(nz(10)),
                stp: SelfTradeProtection::CancelNewest,
                expiry_ns: 0,
            },
        }
    }

    /// Helper: write events, drop writer, read back all entries.
    fn read_all(path: &Path) -> Vec<crate::journal::reader::JournalEntry> {
        let mut reader = JournalReader::open(path).unwrap();
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

        let writer = JournalWriter::create(&path).unwrap();
        // With hash-chain, genesis consumes seq 1 so next is 2; without, next is 1.
        assert_eq!(writer.next_sequence(), FIRST_SEQ);
        assert_eq!(writer.path(), path);
        // Hash chain is active only with the feature.
        #[cfg(feature = "hash-chain")]
        assert!(writer.chain_hash().is_some());
        #[cfg(not(feature = "hash-chain"))]
        assert!(writer.chain_hash().is_none());

        // File should be pre-allocated (64 MiB chunk).
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

        let _writer = JournalWriter::create(&path).unwrap();
        drop(_writer);

        // Second create on same path should fail (create_new).
        let result = JournalWriter::create(&path);
        assert!(result.is_err());
    }

    #[test]
    fn append_assigns_sequential_ids() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let mut writer = JournalWriter::create(&path).unwrap();
        let event = sample_event();

        // With hash-chain, genesis consumed seq 1, so user events start at 2.
        // Without hash-chain, user events start at 1.
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
            let mut writer = JournalWriter::create(&path).unwrap();
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
            let mut writer = JournalWriter::create(&path).unwrap();
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
            JournalEvent::Deposit {
                account: AccountId(1),
                currency: CurrencyId(0),
                amount: 100,
            },
            JournalEvent::Deposit {
                account: AccountId(2),
                currency: CurrencyId(0),
                amount: 200,
            },
            sample_event(),
        ];

        {
            let mut writer = JournalWriter::create(&path).unwrap();
            for event in &events {
                writer.batch_append(event).unwrap();
            }
            // Nothing written to disk yet — flush the batch.
            writer.flush_batch().unwrap();
        }

        let entries = read_all(&path);
        assert_eq!(entries.len(), events.len());
        for (i, (entry, expected)) in entries.iter().zip(events.iter()).enumerate() {
            assert_eq!(entry.sequence, (i as u64) + FIRST_SEQ);
            assert_eq!(&entry.event, expected);
        }
    }

    #[test]
    fn batch_append_with_ts_uses_provided_timestamp() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let event = sample_event();
        let fixed_ts: u64 = 1_700_000_000_000_000_000; // a specific nanos value

        {
            let mut writer = JournalWriter::create(&path).unwrap();
            let seq = writer.batch_append_with_ts(&event, fixed_ts, 0, 0).unwrap();
            assert_eq!(seq, FIRST_SEQ);
            writer.flush_batch().unwrap();
        }

        let entries = read_all(&path);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].timestamp_ns, fixed_ts);
        assert_eq!(entries[0].event, event);
    }

    #[test]
    fn flush_batch_sync_is_readable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let event = sample_event();
        {
            let mut writer = JournalWriter::create(&path).unwrap();
            writer.batch_append(&event).unwrap();
            writer.batch_append(&event).unwrap();
            writer.flush_batch_sync().unwrap();
        }

        let entries = read_all(&path);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].sequence, FIRST_SEQ);
        assert_eq!(entries[1].sequence, FIRST_SEQ + 1);
    }

    #[test]
    fn flush_batch_on_empty_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let mut writer = JournalWriter::create(&path).unwrap();
        // Flushing empty batch should succeed without error.
        writer.flush_batch().unwrap();
        writer.flush_batch_sync().unwrap();

        // File should still be readable with zero entries.
        let entries = read_all(&path);
        assert_eq!(entries.len(), 0);
    }

    #[test]
    fn multiple_batch_flushes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        {
            let mut writer = JournalWriter::create(&path).unwrap();

            // First batch.
            writer.batch_append(&sample_event()).unwrap();
            writer.batch_append(&sample_event()).unwrap();
            writer.flush_batch().unwrap();

            // Second batch.
            writer.batch_append(&sample_event()).unwrap();
            writer.flush_batch_sync().unwrap();
        }

        let entries = read_all(&path);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].sequence, FIRST_SEQ);
        assert_eq!(entries[1].sequence, FIRST_SEQ + 1);
        assert_eq!(entries[2].sequence, FIRST_SEQ + 2);
    }

    #[test]
    fn open_append_continues_sequence() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        // Write 3 events, close.
        {
            let mut writer = JournalWriter::create(&path).unwrap();
            for _ in 0..3 {
                writer.append(&sample_event()).unwrap();
            }
        }

        // Recovery: read to find valid_end and last_seq.
        let (last_seq, valid_end) = {
            let mut reader = JournalReader::open(&path).unwrap();
            while reader.next_entry().unwrap().is_some() {}
            (reader.last_sequence().unwrap(), reader.valid_file_end())
        };

        // Reopen and append more.
        let extra = JournalEvent::CancelOrder {
            symbol: Symbol(1),
            account: AccountId(1),
            order_id: OrderId(42),
        };
        {
            let mut writer =
                JournalWriter::open_append(&path, last_seq, valid_end, None, 0).unwrap();
            // With hash-chain: Genesis(1) + 3 user events(2,3,4) → last_seq=4, next=5
            // Without: 3 user events(1,2,3) → last_seq=3, next=4
            assert_eq!(writer.next_sequence(), FIRST_SEQ + 3);
            let seq = writer.append(&extra).unwrap();
            assert_eq!(seq, FIRST_SEQ + 3);
        }

        // Read back all 4 entries (3 original + 1 new, genesis is transparent).
        let entries = read_all(&path);
        assert_eq!(entries.len(), 4);
        assert_eq!(entries[3].sequence, FIRST_SEQ + 3);
        assert_eq!(entries[3].event, extra);
    }

    #[test]
    fn open_append_reuses_preallocation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        {
            let mut writer = JournalWriter::create(&path).unwrap();
            writer.append(&sample_event()).unwrap();
        }

        let (last_seq, valid_end) = {
            let mut reader = JournalReader::open(&path).unwrap();
            while reader.next_entry().unwrap().is_some() {}
            (reader.last_sequence().unwrap(), reader.valid_file_end())
        };

        // open_append reuses the existing pre-allocation (no truncation).
        let _writer = JournalWriter::open_append(&path, last_seq, valid_end, None, 0).unwrap();

        // File should retain its original pre-allocation (no truncation).
        let file_len = std::fs::metadata(&path).unwrap().len();
        assert!(
            file_len > valid_end,
            "expected file to retain pre-allocation beyond valid_end: len={file_len}, valid_end={valid_end}"
        );
    }

    #[test]
    fn open_append_zeros_trailing_garbage() {
        // Simulates a double-crash scenario:
        // 1. Write entries, simulate crash (truncate mid-entry → trailing garbage)
        // 2. Recover (open_append at valid_end), write a short entry
        // 3. Simulate second crash (the short entry doesn't fully overwrite garbage)
        // 4. Second recovery must succeed without CorruptEntry
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        // Create journal with several entries.
        {
            let mut writer = JournalWriter::create(&path).unwrap();
            for _ in 0..10 {
                writer.append(&sample_event()).unwrap();
            }
        }

        // Find valid_end, then inject fake garbage bytes after it
        // (simulating a partial write from a crash).
        let valid_end = {
            let mut reader = JournalReader::open(&path).unwrap();
            while reader.next_entry().unwrap().is_some() {}
            reader.valid_file_end()
        };

        // Record last_seq before injecting garbage (reader can't read
        // past garbage without returning an error).
        let last_seq = {
            let mut reader = JournalReader::open(&path).unwrap();
            while reader.next_entry().unwrap().is_some() {}
            reader.last_sequence().unwrap()
        };

        // Write garbage that looks like a partial entry (non-zero, but not
        // a valid entry). This simulates bytes from an interrupted pwrite.
        {
            let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            // Write bytes that could be misinterpreted as an entry start
            // if not properly cleared: a valid-looking magic + some payload.
            let garbage = [0x45, 0x4A, 0x20, 0x00, 0xFF, 0xFF, 0xFF, 0xFF];
            file.write_all_at(&garbage, valid_end).unwrap();
        }

        // First recovery: open_append should zero out the garbage.
        {
            let mut writer =
                JournalWriter::open_append(&path, last_seq, valid_end, None, 0).unwrap();
            // Write only one small entry (doesn't cover all the garbage bytes).
            writer
                .append(&JournalEvent::Deposit {
                    account: crate::types::AccountId(1),
                    currency: crate::types::CurrencyId(0),
                    amount: 1,
                })
                .unwrap();
        }

        // Second recovery: should succeed cleanly (no CorruptEntry from
        // leftover garbage bytes).
        let mut reader = JournalReader::open(&path).unwrap();
        let mut count = 0;
        while reader.next_entry().unwrap().is_some() {
            count += 1;
        }
        // 10 original events + 1 new event after first recovery.
        assert_eq!(count, 11);
    }

    #[test]
    fn batch_append_does_not_write_to_disk() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let mut writer = JournalWriter::create(&path).unwrap();
        writer.batch_append(&sample_event()).unwrap();
        writer.batch_append(&sample_event()).unwrap();
        // Data is buffered but not flushed — reader should see zero entries.
        let entries = read_all(&path);
        assert_eq!(entries.len(), 0);

        // Now flush — entries appear.
        writer.flush_batch().unwrap();
        let entries = read_all(&path);
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn append_flushes_previously_buffered_data() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let cancel = JournalEvent::CancelOrder {
            symbol: Symbol(1),
            account: AccountId(1),
            order_id: OrderId(99),
        };

        {
            let mut writer = JournalWriter::create(&path).unwrap();
            // Buffer two events without flushing.
            writer.batch_append(&sample_event()).unwrap();
            writer.batch_append(&sample_event()).unwrap();
            // append() calls batch_append + flush_batch_sync, so all three
            // events should be flushed together.
            writer.append(&cancel).unwrap();
        }

        let entries = read_all(&path);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].sequence, FIRST_SEQ);
        assert_eq!(entries[1].sequence, FIRST_SEQ + 1);
        assert_eq!(entries[2].sequence, FIRST_SEQ + 2);
        assert_eq!(entries[2].event, cancel);
    }

    #[cfg(feature = "hash-chain")]
    #[test]
    fn genesis_hash_written_as_first_entry() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let writer = JournalWriter::create(&path).unwrap();
        assert!(writer.chain_hash().is_some());
        assert_eq!(writer.events_since_checkpoint(), 0);
        drop(writer);

        // Read the raw journal to confirm GenesisHash is the first entry.
        let mut reader = JournalReader::open(&path).unwrap();
        // next_entry() skips GenesisHash transparently — returns None
        // for an empty journal (no user events).
        assert!(reader.next_entry().unwrap().is_none());
        // But the reader should have initialized the hash chain from genesis.
        assert!(reader.chain_hash().is_some());
    }

    #[cfg(feature = "hash-chain")]
    #[test]
    fn chain_hash_changes_with_each_event() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let mut writer = JournalWriter::create(&path).unwrap();
        let h0 = writer.chain_hash().unwrap();

        writer.append(&sample_event()).unwrap();
        let h1 = writer.chain_hash().unwrap();
        assert_ne!(h0, h1);

        writer.append(&sample_event()).unwrap();
        let h2 = writer.chain_hash().unwrap();
        assert_ne!(h1, h2);
    }

    #[cfg(feature = "hash-chain")]
    #[test]
    fn reader_chain_hash_matches_writer() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let writer_hash;
        {
            let mut writer = JournalWriter::create(&path).unwrap();
            for _ in 0..10 {
                writer.append(&sample_event()).unwrap();
            }
            writer_hash = writer.chain_hash().unwrap();
        }

        let mut reader = JournalReader::open(&path).unwrap();
        while reader.next_entry().unwrap().is_some() {}
        assert_eq!(reader.chain_hash().unwrap(), writer_hash);
    }

    #[cfg(feature = "hash-chain")]
    #[test]
    fn checkpoint_auto_emitted_at_interval() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let checkpoint_hash;
        {
            let mut writer = JournalWriter::create(&path).unwrap();
            for _ in 0..CHECKPOINT_INTERVAL {
                writer.batch_append(&sample_event()).unwrap();
            }
            writer.flush_batch().unwrap();
            // After exactly CHECKPOINT_INTERVAL events, a checkpoint should
            // have been emitted and the counter reset.
            assert_eq!(writer.events_since_checkpoint(), 0);
            checkpoint_hash = writer.chain_hash().unwrap();
        }

        // Reader should transparently skip the checkpoint and genesis.
        let mut reader = JournalReader::open(&path).unwrap();
        let mut count = 0u64;
        while reader.next_entry().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, CHECKPOINT_INTERVAL);
        // Reader's chain hash should match writer's.
        assert_eq!(reader.chain_hash().unwrap(), checkpoint_hash);
    }

    #[cfg(feature = "hash-chain")]
    #[test]
    fn multiple_checkpoints_emitted() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let writer_hash;
        {
            let mut writer = JournalWriter::create(&path).unwrap();
            // Write 2.5 intervals — should emit 2 checkpoints.
            for _ in 0..CHECKPOINT_INTERVAL * 5 / 2 {
                writer.batch_append(&sample_event()).unwrap();
            }
            writer.flush_batch().unwrap();
            // 250K events / 100K interval = 2 checkpoints emitted.
            // Counter should be at 50K (half of third interval).
            assert_eq!(writer.events_since_checkpoint(), CHECKPOINT_INTERVAL / 2);
            writer_hash = writer.chain_hash().unwrap();
        }

        // Reader must transparently skip both checkpoints + genesis.
        let mut reader = JournalReader::open(&path).unwrap();
        let mut count = 0u64;
        while reader.next_entry().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, CHECKPOINT_INTERVAL * 5 / 2);
        assert_eq!(reader.chain_hash().unwrap(), writer_hash);
    }

    #[cfg(feature = "hash-chain")]
    #[test]
    fn open_append_with_chain_hash_resumes_chain() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let (last_seq, valid_end, chain_hash, events_since);
        {
            let mut writer = JournalWriter::create(&path).unwrap();
            for _ in 0..50 {
                writer.append(&sample_event()).unwrap();
            }
            chain_hash = writer.chain_hash();
            events_since = writer.events_since_checkpoint();
            drop(writer);

            let mut reader = JournalReader::open(&path).unwrap();
            while reader.next_entry().unwrap().is_some() {}
            last_seq = reader.last_sequence().unwrap();
            valid_end = reader.valid_file_end();
        }

        // Reopen with chain hash — chain should resume.
        let mut writer =
            JournalWriter::open_append(&path, last_seq, valid_end, chain_hash, events_since)
                .unwrap();
        assert_eq!(writer.chain_hash(), chain_hash);
        assert_eq!(writer.events_since_checkpoint(), events_since);

        // Append more events — chain should continue.
        for _ in 0..10 {
            writer.append(&sample_event()).unwrap();
        }
        let final_hash = writer.chain_hash().unwrap();
        assert_ne!(final_hash, chain_hash.unwrap());
        drop(writer);

        // Reader should see all 60 events with correct chain.
        let mut reader = JournalReader::open(&path).unwrap();
        let mut count = 0u64;
        while reader.next_entry().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 60);
        assert_eq!(reader.chain_hash().unwrap(), final_hash);
    }

    #[test]
    fn open_append_without_chain_hash_has_no_chain() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        {
            let mut writer = JournalWriter::create(&path).unwrap();
            writer.append(&sample_event()).unwrap();
        }

        let (last_seq, valid_end) = {
            let mut reader = JournalReader::open(&path).unwrap();
            while reader.next_entry().unwrap().is_some() {}
            (reader.last_sequence().unwrap(), reader.valid_file_end())
        };

        // Open with None chain — simulates v5 recovery.
        let mut writer = JournalWriter::open_append(&path, last_seq, valid_end, None, 0).unwrap();
        assert!(writer.chain_hash().is_none());

        // Appending events should work, just no hash chain.
        writer.append(&sample_event()).unwrap();
        assert!(writer.chain_hash().is_none());
    }

    #[cfg(feature = "hash-chain")]
    #[test]
    fn batch_crossing_checkpoint_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let writer_hash;
        {
            let mut writer = JournalWriter::create(&path).unwrap();
            // Append 99_995 events (5 short of checkpoint).
            for _ in 0..CHECKPOINT_INTERVAL - 5 {
                writer.batch_append(&sample_event()).unwrap();
            }
            assert_eq!(writer.events_since_checkpoint(), CHECKPOINT_INTERVAL - 5);

            // Append 10 more — should cross the checkpoint boundary
            // mid-batch.
            for _ in 0..10 {
                writer.batch_append(&sample_event()).unwrap();
            }
            // 5 events pushed it to CHECKPOINT_INTERVAL, then
            // checkpoint emitted and reset, then 5 more.
            assert_eq!(writer.events_since_checkpoint(), 5);

            // Flush everything in one pwrite.
            writer.flush_batch().unwrap();
            writer_hash = writer.chain_hash().unwrap();
        }

        // Reader should see exactly CHECKPOINT_INTERVAL + 5 user events.
        let mut reader = JournalReader::open(&path).unwrap();
        let mut count = 0u64;
        while reader.next_entry().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, CHECKPOINT_INTERVAL + 5);
        assert_eq!(reader.chain_hash().unwrap(), writer_hash);
    }

    #[test]
    fn write_raw_sync_produces_readable_journal() {
        let dir = tempfile::tempdir().unwrap();
        let primary_path = dir.path().join("primary.journal");
        let replica_path = dir.path().join("replica.journal");

        // Write events to the primary journal normally.
        let events = vec![
            JournalEvent::Deposit {
                account: AccountId(1),
                currency: CurrencyId(0),
                amount: 100,
            },
            JournalEvent::Deposit {
                account: AccountId(2),
                currency: CurrencyId(0),
                amount: 200,
            },
            sample_event(),
        ];

        let primary_genesis;
        let raw_bytes;
        let entry_count;
        {
            let mut writer = JournalWriter::create(&primary_path).unwrap();
            primary_genesis = writer.chain_hash().unwrap_or([0u8; 32]);

            // Encode events into batch buffer, then snapshot the bytes.
            for event in &events {
                writer.batch_append(event).unwrap();
            }
            raw_bytes = writer.pending_batch_bytes().to_vec();
            entry_count = events.len() as u64;
            writer.flush_batch_sync().unwrap();
        }

        // Create the replica journal with the same genesis hash.
        {
            let mut replica =
                JournalWriter::create_continuing(&replica_path, 1, primary_genesis).unwrap();
            // Write the raw bytes captured from the primary.
            replica.write_raw_sync(&raw_bytes, entry_count).unwrap();
            assert_eq!(replica.next_sequence(), FIRST_SEQ + entry_count);
        }

        // Read back from the replica journal — should see the same events.
        let mut reader = JournalReader::open(&replica_path).unwrap();
        for (i, expected) in events.iter().enumerate() {
            let entry = reader.next_entry().unwrap().unwrap();
            assert_eq!(entry.sequence, (i as u64) + FIRST_SEQ);
            assert_eq!(&entry.event, expected);
        }
        assert!(reader.next_entry().unwrap().is_none());
    }

    #[test]
    fn write_raw_sync_advances_write_pos() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raw_pos.journal");

        let mut writer = JournalWriter::create(&path).unwrap();
        let pos_before = writer.write_pos();

        let data = [0x4A, 0x45, 0x00, 0x01, 0x02, 0x00, 0x00, 0x00]; // fake entry bytes
        writer.write_raw_sync(&data, 1).unwrap();

        assert_eq!(writer.write_pos(), pos_before + data.len() as u64);
        // With hash-chain: genesis(1) + next(2) + raw(1) = 3.
        // Without: next(1) + raw(1) = 2.
        assert_eq!(writer.next_sequence(), FIRST_SEQ + 1);
    }

    #[test]
    fn write_raw_sync_empty_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raw_empty.journal");

        let mut writer = JournalWriter::create(&path).unwrap();
        let pos = writer.write_pos();
        let seq = writer.next_sequence();

        writer.write_raw_sync(&[], 0).unwrap();

        assert_eq!(writer.write_pos(), pos);
        assert_eq!(writer.next_sequence(), seq);
    }

    #[test]
    fn reserve_raw_async_write_advances_position() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");
        let mut writer = JournalWriter::create(&path).unwrap();
        let pos_before = writer.write_pos;
        let seq_before = writer.next_sequence;

        let offset = writer.reserve_raw_async_write(128, 3).unwrap();

        // Offset returned is the pre-reservation position; writer state
        // advances eagerly so subsequent reservations don't collide.
        assert_eq!(offset, pos_before);
        assert_eq!(writer.write_pos, pos_before + 128);
        assert_eq!(writer.next_sequence, seq_before + 3);
    }

    #[test]
    fn reserve_raw_async_write_does_not_touch_spare_buffer() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");
        let mut writer = JournalWriter::create(&path).unwrap();

        // Spare buffer belongs to the primary double-buffering path; the
        // raw-batch ring owns its own slot memory, so reservation must
        // not borrow or release the spare buffer in either direction.
        let _ = writer.spare_buf.take();
        assert!(writer.spare_buf.is_none());

        let _ = writer.reserve_raw_async_write(64, 1).unwrap();
        assert!(
            writer.spare_buf.is_none(),
            "raw reservations must leave spare_buf alone"
        );
    }
}
