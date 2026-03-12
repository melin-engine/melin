//! Journal writer — append-only, durable event log with pre-allocated storage.
//!
//! Uses `posix_fallocate` to pre-extend the journal file in 64 MiB chunks.
//! This allocates disk blocks (extents) ahead of time so that subsequent
//! `sync_data()` calls only flush dirty data pages — not filesystem metadata
//! updates for newly allocated extents. This significantly reduces fsync
//! latency under sustained write load.
//!
//! Writes use `write_all_at` (pwrite) with an explicit write position rather
//! than kernel-managed append mode, because the file size includes
//! pre-allocated (zero-filled) space beyond the valid data boundary.

use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use super::codec::{self, FILE_HEADER_SIZE};
use super::error::JournalError;
use super::event::JournalEvent;

/// Maximum encoded entry size. Generously sized — actual entries are ~65-85 bytes.
/// Fixed-size array avoids per-write heap allocation on the hot path.
const MAX_ENTRY_SIZE: usize = 128;

/// Pre-allocation chunk size (64 MiB). Large enough to amortize the cost of
/// extent metadata updates across many entries. At ~80 bytes per entry,
/// one chunk covers ~800K entries before the next allocation is needed.
const PREALLOC_CHUNK: u64 = 64 * 1024 * 1024;

/// Appends journal events to a file with CRC32C checksums and fsync durability.
///
/// Uses positioned writes (`pwrite`) and pre-allocated storage to minimize
/// fsync latency. The file size includes pre-allocated zero-filled space
/// beyond the valid data boundary; recovery truncates to `valid_file_end`
/// before reopening.
pub struct JournalWriter {
    file: File,
    /// Pre-allocated fixed-size buffer to avoid per-write heap allocation.
    /// A fixed array (not Vec) because entries have a known bounded size.
    buffer: [u8; MAX_ENTRY_SIZE],
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
}

impl JournalWriter {
    /// Create a new journal file. Writes the file header, pre-allocates
    /// storage, and returns a writer.
    ///
    /// Fails if the file already exists (use `open_append` for existing journals).
    pub fn create(path: &Path) -> Result<Self, JournalError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;

        // Write file header at offset 0.
        let mut header = [0u8; FILE_HEADER_SIZE];
        codec::encode_file_header(&mut header);
        file.write_all_at(&header, 0)?;

        let write_pos = FILE_HEADER_SIZE as u64;

        // Pre-allocate the first chunk. This extends the file and allocates
        // disk blocks so subsequent writes don't trigger extent allocation.
        let allocated_end = preallocate(&file, write_pos)?;

        // Sync both the header and the extent metadata from fallocate.
        // This is a one-time cost at journal creation.
        file.sync_all()?;

        Ok(Self {
            file,
            buffer: [0u8; MAX_ENTRY_SIZE],
            next_sequence: 1,
            path: path.to_path_buf(),
            write_pos,
            allocated_end,
        })
    }

    /// Open an existing journal file for appending after recovery.
    ///
    /// `last_seq` is the sequence number of the last valid entry read during
    /// recovery. The writer will continue from `last_seq + 1`.
    ///
    /// `valid_end` is the byte offset of the end of the last valid entry
    /// (including file header). The file is truncated to this point to remove
    /// any trailing garbage or pre-allocated space, then re-allocated.
    pub fn open_append(path: &Path, last_seq: u64, valid_end: u64) -> Result<Self, JournalError> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;

        // Truncate to remove trailing garbage from crash + old pre-allocated space.
        file.set_len(valid_end)?;

        // Re-allocate from the valid end forward.
        let allocated_end = preallocate(&file, valid_end)?;

        // Sync the truncation and new extent allocation.
        file.sync_all()?;

        Ok(Self {
            file,
            buffer: [0u8; MAX_ENTRY_SIZE],
            next_sequence: last_seq + 1,
            path: path.to_path_buf(),
            write_pos: valid_end,
            allocated_end,
        })
    }

    /// Append an event to the journal and flush to disk.
    ///
    /// Returns the assigned sequence number. The event is durable after this
    /// returns (fsync'd).
    pub fn append(&mut self, event: &JournalEvent) -> Result<u64, JournalError> {
        let seq = self.next_sequence;
        let timestamp_ns = wall_clock_nanos();

        let written = codec::encode(seq, timestamp_ns, event, &mut self.buffer)?;
        self.ensure_allocated(written as u64)?;
        self.file
            .write_all_at(&self.buffer[..written], self.write_pos)?;
        self.write_pos += written as u64;
        self.file.sync_data()?;

        self.next_sequence += 1;
        Ok(seq)
    }

    /// Append an event to the journal **without** flushing to disk.
    ///
    /// Used by the pipeline journal stage to batch multiple events into
    /// a single write before calling `sync()` once for the batch.
    /// This amortizes the fsync cost across many events under load.
    pub fn append_no_sync(&mut self, event: &JournalEvent) -> Result<u64, JournalError> {
        let seq = self.next_sequence;
        let timestamp_ns = wall_clock_nanos();

        let written = codec::encode(seq, timestamp_ns, event, &mut self.buffer)?;
        self.ensure_allocated(written as u64)?;
        self.file
            .write_all_at(&self.buffer[..written], self.write_pos)?;
        self.write_pos += written as u64;

        self.next_sequence += 1;
        Ok(seq)
    }

    /// Flush the journal to disk (fsync).
    ///
    /// Called after one or more `append_no_sync` calls to make the batch
    /// durable in a single I/O operation. Because storage is pre-allocated,
    /// this only flushes data pages — no extent metadata updates needed.
    pub fn sync(&mut self) -> Result<(), JournalError> {
        self.file.sync_data()?;
        Ok(())
    }

    /// Current next sequence number (useful for snapshot coordination).
    pub fn next_sequence(&self) -> u64 {
        self.next_sequence
    }

    /// Path to the journal file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Ensure enough pre-allocated space exists for the next write.
    /// If the write would exceed the current allocation, extends by
    /// another chunk. This is rare — once per ~800K entries.
    fn ensure_allocated(&mut self, bytes_needed: u64) -> Result<(), JournalError> {
        if self.write_pos + bytes_needed <= self.allocated_end {
            return Ok(());
        }
        self.allocated_end = preallocate(&self.file, self.write_pos)?;
        // sync_all to persist the new extent metadata. This is a rare
        // cost — amortized over ~800K entries per chunk.
        self.file.sync_all()?;
        Ok(())
    }
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

    let ret = unsafe { libc::posix_fallocate(file.as_raw_fd(), 0, target as libc::off_t) };

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
fn wall_clock_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}
