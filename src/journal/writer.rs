//! Journal writer — append-only, durable event log.
//!
//! Phase 1 durability: flush (fsync) after every append. Correct but slow
//! (~microseconds per fsync). The ~100ns budget applies to matching, not I/O.
//! Async I/O via ring buffer is a future optimization.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use super::codec::{self, FILE_HEADER_SIZE};
use super::error::JournalError;
use super::event::JournalEvent;

/// Maximum encoded entry size. Generously sized — actual entries are ~65-85 bytes.
/// Fixed-size array avoids per-write heap allocation on the hot path.
const MAX_ENTRY_SIZE: usize = 128;

/// Appends journal events to a file with CRC32C checksums and fsync durability.
pub struct JournalWriter {
    file: File,
    /// Pre-allocated fixed-size buffer to avoid per-write heap allocation.
    /// A fixed array (not Vec) because entries have a known bounded size.
    buffer: [u8; MAX_ENTRY_SIZE],
    /// Next sequence number to assign (monotonically increasing, starts at 1).
    next_sequence: u64,
    /// Path to the journal file (kept for error messages / reopening).
    path: PathBuf,
}

impl JournalWriter {
    /// Create a new journal file. Writes the file header and returns a writer.
    ///
    /// Fails if the file already exists (use `open_append` for existing journals).
    pub fn create(path: &Path) -> Result<Self, JournalError> {
        let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;

        let mut header = [0u8; FILE_HEADER_SIZE];
        codec::encode_file_header(&mut header);
        file.write_all(&header)?;
        file.sync_data()?;

        Ok(Self {
            file,
            buffer: [0u8; MAX_ENTRY_SIZE],
            next_sequence: 1,
            path: path.to_path_buf(),
        })
    }

    /// Open an existing journal file for appending after recovery.
    ///
    /// `last_seq` is the sequence number of the last valid entry read during
    /// recovery. The writer will continue from `last_seq + 1`.
    ///
    /// `valid_end` is the byte offset of the end of the last valid entry
    /// (including file header). The file is truncated to this point to remove
    /// any trailing garbage from a partial write during a crash.
    pub fn open_append(path: &Path, last_seq: u64, valid_end: u64) -> Result<Self, JournalError> {
        // Truncate trailing garbage from crash before opening for append.
        let file = OpenOptions::new().write(true).open(path)?;
        file.set_len(valid_end)?;
        file.sync_data()?;
        drop(file);

        let file = OpenOptions::new().append(true).open(path)?;

        Ok(Self {
            file,
            buffer: [0u8; MAX_ENTRY_SIZE],
            next_sequence: last_seq + 1,
            path: path.to_path_buf(),
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
        self.file.write_all(&self.buffer[..written])?;
        self.file.sync_data()?;

        self.next_sequence += 1;
        Ok(seq)
    }

    /// Current next sequence number (useful for snapshot coordination).
    pub fn next_sequence(&self) -> u64 {
        self.next_sequence
    }

    /// Path to the journal file.
    pub fn path(&self) -> &Path {
        &self.path
    }
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
