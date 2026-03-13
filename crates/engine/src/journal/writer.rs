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

/// Maximum encoded entry size. Generously sized — actual entries are ~65-85 bytes.
/// Fixed-size array avoids per-write heap allocation on the hot path.
const MAX_ENTRY_SIZE: usize = 128;

/// Batch buffer capacity. Sized for MAX_JOURNAL_BATCH (1024) entries at
/// ~80 bytes each = ~80 KiB. Pre-allocated once, reused across batches.
const BATCH_BUF_CAPACITY: usize = 128 * 1024;

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
    /// Pre-allocated fixed-size buffer for single-entry encoding.
    /// A fixed array (not Vec) because entries have a known bounded size.
    buffer: [u8; MAX_ENTRY_SIZE],
    /// Batch write buffer. Events are encoded here via `batch_append()`,
    /// then flushed in a single `pwrite` via `flush_batch()`. This reduces
    /// syscalls from N (one pwrite per event) to 1 per batch.
    batch_buf: Vec<u8>,
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
            batch_buf: Vec::with_capacity(BATCH_BUF_CAPACITY),
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
            batch_buf: Vec::with_capacity(BATCH_BUF_CAPACITY),
            next_sequence: last_seq + 1,
            path: path.to_path_buf(),
            write_pos: valid_end,
            allocated_end,
        })
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
        let seq = self.next_sequence;
        let timestamp_ns = wall_clock_nanos();
        let written = codec::encode(seq, timestamp_ns, event, &mut self.buffer)?;
        self.batch_buf.extend_from_slice(&self.buffer[..written]);
        self.next_sequence += 1;
        Ok(seq)
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
    ) -> Result<u64, JournalError> {
        let seq = self.next_sequence;
        let written = codec::encode(seq, timestamp_ns, event, &mut self.buffer)?;
        self.batch_buf.extend_from_slice(&self.buffer[..written]);
        self.next_sequence += 1;
        Ok(seq)
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

        self.write_pos += self.batch_buf.len() as u64;
        self.batch_buf.clear();
        Ok(())
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

    /// Path to the journal file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Raw file descriptor for the journal file.
    pub fn fd(&self) -> std::os::unix::io::RawFd {
        self.file.as_raw_fd()
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
        #[cfg(not(feature = "no-fsync"))]
        self.file.sync_all()?;
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
                },
                time_in_force: TimeInForce::GTC,
                quantity: Quantity(nz(10)),
                stp: SelfTradeProtection::CancelNewest,
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
        assert_eq!(writer.next_sequence(), 1);
        assert_eq!(writer.path(), path);

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

        let seq1 = writer.append(&event).unwrap();
        let seq2 = writer.append(&event).unwrap();
        let seq3 = writer.append(&event).unwrap();

        assert_eq!(seq1, 1);
        assert_eq!(seq2, 2);
        assert_eq!(seq3, 3);
        assert_eq!(writer.next_sequence(), 4);
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
        assert_eq!(entries[0].sequence, 1);
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
            assert_eq!(seq, 1);
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
            assert_eq!(entry.sequence, (i as u64) + 1);
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
            let seq = writer.batch_append_with_ts(&event, fixed_ts).unwrap();
            assert_eq!(seq, 1);
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
        assert_eq!(entries[0].sequence, 1);
        assert_eq!(entries[1].sequence, 2);
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
        assert_eq!(entries[0].sequence, 1);
        assert_eq!(entries[1].sequence, 2);
        assert_eq!(entries[2].sequence, 3);
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
            order_id: OrderId(42),
        };
        {
            let mut writer = JournalWriter::open_append(&path, last_seq, valid_end).unwrap();
            assert_eq!(writer.next_sequence(), 4);
            let seq = writer.append(&extra).unwrap();
            assert_eq!(seq, 4);
        }

        // Read back all 4 entries.
        let entries = read_all(&path);
        assert_eq!(entries.len(), 4);
        assert_eq!(entries[3].sequence, 4);
        assert_eq!(entries[3].event, extra);
    }

    #[test]
    fn open_append_truncates_preallocation() {
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

        // open_append truncates to valid_end then re-preallocates.
        let _writer = JournalWriter::open_append(&path, last_seq, valid_end).unwrap();

        // File should be re-preallocated from valid_end.
        let file_len = std::fs::metadata(&path).unwrap().len();
        assert_eq!(file_len, valid_end + PREALLOC_CHUNK);
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
        assert_eq!(entries[0].sequence, 1);
        assert_eq!(entries[1].sequence, 2);
        assert_eq!(entries[2].sequence, 3);
        assert_eq!(entries[2].event, cancel);
    }
}
