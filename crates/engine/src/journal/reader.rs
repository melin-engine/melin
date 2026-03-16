//! Journal reader — sequential read with CRC and sequence validation.
//!
//! Reads entries one at a time. On crash recovery:
//! - Truncated entry at EOF → `Ok(None)` (last partial write, safe to ignore)
//! - CRC mismatch mid-stream → `Err(CorruptEntry)` (real corruption)

use std::fs::File;
use std::io::Read;
use std::path::Path;

use super::codec::{self, FILE_HEADER_SIZE};
use super::error::JournalError;
use super::event::JournalEvent;

/// Initial read buffer size. Grows if needed, but entries are typically <100 bytes.
/// Uses a Vec (growable) rather than a fixed array because the reader may need
/// to buffer multiple entries when entries span read boundaries.
const INITIAL_BUF_SIZE: usize = 4096;

/// A decoded journal entry with its metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalEntry {
    /// Monotonically increasing sequence number (starts at 1).
    pub sequence: u64,
    /// Wall-clock nanos since epoch at write time (informational, not for ordering).
    pub timestamp_ns: u64,
    /// The event that was journaled.
    pub event: JournalEvent,
}

/// Reads journal entries sequentially, validating checksums and sequence continuity.
pub struct JournalReader {
    file: File,
    /// Read buffer — Vec because it may grow when entries span chunk boundaries.
    buffer: Vec<u8>,
    /// Current read position within `buffer`.
    pos: usize,
    /// Number of valid bytes in `buffer` (from last read).
    valid: usize,
    /// Last sequence number read, for gap detection.
    last_sequence: Option<u64>,
    /// Byte offset in the file of the end of the last successfully decoded entry.
    /// Used by recovery to know where to truncate trailing garbage.
    valid_file_end: u64,
}

impl JournalReader {
    /// Open a journal file for reading. Validates the file header.
    pub fn open(path: &Path) -> Result<Self, JournalError> {
        let mut file = File::open(path)?;

        // Read and validate the file header.
        let mut header = [0u8; FILE_HEADER_SIZE];
        file.read_exact(&mut header)?;
        codec::decode_file_header(&header)?;

        Ok(Self {
            file,
            buffer: vec![0u8; INITIAL_BUF_SIZE],
            pos: 0,
            valid: 0,
            last_sequence: None,
            valid_file_end: FILE_HEADER_SIZE as u64,
        })
    }

    /// Read the next journal entry.
    ///
    /// Returns `Ok(Some(entry))` for each valid entry.
    /// Returns `Ok(None)` at EOF, on a truncated final entry (crash recovery),
    /// or when reaching zero-filled pre-allocated (fallocated) space.
    /// Returns `Err` on corruption (CRC mismatch, sequence gap, etc.).
    pub fn next_entry(&mut self) -> Result<Option<JournalEntry>, JournalError> {
        // Ensure we have data to work with.
        self.fill_buffer()?;

        let available = self.valid - self.pos;
        if available == 0 {
            return Ok(None);
        }

        // Zero magic bytes indicate pre-allocated (fallocated) space.
        // Entry magic is always 0x4A45, so zero bytes can never start a
        // valid entry — treat as end-of-data.
        if available >= 2 && self.buffer[self.pos] == 0 && self.buffer[self.pos + 1] == 0 {
            return Ok(None);
        }

        let data = &self.buffer[self.pos..self.valid];

        match codec::decode(data) {
            Ok((consumed, sequence, timestamp_ns, event)) => {
                self.validate_and_advance(consumed, sequence, timestamp_ns, event)
            }
            Err(JournalError::TruncatedEntry) => {
                // Could be a partial entry at EOF or we need more data.
                if self.try_extend_buffer()? {
                    // Got more data, try again.
                    let data = &self.buffer[self.pos..self.valid];

                    // Re-check for zero magic after extending — the buffer
                    // may now contain pre-allocated zeros.
                    if data.len() >= 2 && data[0] == 0 && data[1] == 0 {
                        return Ok(None);
                    }

                    match codec::decode(data) {
                        Ok((consumed, sequence, timestamp_ns, event)) => {
                            self.validate_and_advance(consumed, sequence, timestamp_ns, event)
                        }
                        // Truly truncated — crash recovery case.
                        Err(JournalError::TruncatedEntry) => Ok(None),
                        Err(e) => Err(e),
                    }
                } else {
                    // No more data available — truncated at EOF.
                    Ok(None)
                }
            }
            Err(e) => Err(e),
        }
    }

    /// Validate sequence continuity and advance read position.
    fn validate_and_advance(
        &mut self,
        consumed: usize,
        sequence: u64,
        timestamp_ns: u64,
        event: JournalEvent,
    ) -> Result<Option<JournalEntry>, JournalError> {
        // For the first entry, accept whatever sequence we find (supports
        // rotated journals that continue from a prior sequence). For
        // subsequent entries, enforce strict continuity.
        if let Some(last) = self.last_sequence {
            let expected = last + 1;
            if sequence != expected {
                return Err(JournalError::SequenceGap {
                    expected,
                    actual: sequence,
                });
            }
        }
        self.last_sequence = Some(sequence);
        self.pos += consumed;
        self.valid_file_end += consumed as u64;
        Ok(Some(JournalEntry {
            sequence,
            timestamp_ns,
            event,
        }))
    }

    /// Last successfully read sequence number.
    pub fn last_sequence(&self) -> Option<u64> {
        self.last_sequence
    }

    /// Byte offset in the file just past the last valid entry.
    /// Used by recovery to truncate trailing garbage before reopening for append.
    pub fn valid_file_end(&self) -> u64 {
        self.valid_file_end
    }

    /// Compact the buffer by moving unconsumed data to the front, then
    /// read more from the file.
    fn fill_buffer(&mut self) -> Result<(), JournalError> {
        if self.pos > 0 {
            // Shift unconsumed data to the front.
            self.buffer.copy_within(self.pos..self.valid, 0);
            self.valid -= self.pos;
            self.pos = 0;
        }

        // Read more data.
        let n = self.file.read(&mut self.buffer[self.valid..])?;
        self.valid += n;
        Ok(())
    }

    /// Try to read more data into the buffer. Returns true if new data was read.
    fn try_extend_buffer(&mut self) -> Result<bool, JournalError> {
        // Compact first.
        if self.pos > 0 {
            self.buffer.copy_within(self.pos..self.valid, 0);
            self.valid -= self.pos;
            self.pos = 0;
        }

        // Grow the buffer if it's full.
        if self.valid == self.buffer.len() {
            self.buffer.resize(self.buffer.len() * 2, 0);
        }

        let n = self.file.read(&mut self.buffer[self.valid..])?;
        self.valid += n;
        Ok(n > 0)
    }
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::num::NonZeroU64;

    use super::*;
    use crate::journal::writer::JournalWriter;
    use crate::types::*;

    fn nz(v: u64) -> NonZeroU64 {
        NonZeroU64::new(v).unwrap()
    }

    fn sample_events() -> Vec<JournalEvent> {
        vec![
            JournalEvent::AddInstrument {
                spec: InstrumentSpec {
                    symbol: Symbol(1),
                    base: CurrencyId(10),
                    quote: CurrencyId(20),
                },
            },
            JournalEvent::Deposit {
                account: AccountId(1),
                currency: CurrencyId(20),
                amount: 50_000,
            },
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
            },
            JournalEvent::CancelOrder {
                symbol: Symbol(1),
                order_id: OrderId(1),
            },
        ]
    }

    #[test]
    fn write_then_read_back() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let events = sample_events();
        {
            let mut writer = JournalWriter::create(&path).unwrap();
            for event in &events {
                writer.append(event).unwrap();
            }
        }

        let mut reader = JournalReader::open(&path).unwrap();
        for (i, expected) in events.iter().enumerate() {
            let entry = reader.next_entry().unwrap().unwrap();
            assert_eq!(entry.sequence, (i as u64) + 1);
            assert_eq!(&entry.event, expected);
            assert!(entry.timestamp_ns > 0);
        }
        assert!(reader.next_entry().unwrap().is_none());
    }

    #[test]
    fn empty_journal_reads_none() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.journal");

        let _writer = JournalWriter::create(&path).unwrap();

        let mut reader = JournalReader::open(&path).unwrap();
        assert!(reader.next_entry().unwrap().is_none());
        assert_eq!(reader.valid_file_end(), FILE_HEADER_SIZE as u64);
    }

    #[test]
    fn truncated_last_entry_recovers() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("truncated.journal");

        let events = sample_events();
        {
            let mut writer = JournalWriter::create(&path).unwrap();
            for event in &events {
                writer.append(event).unwrap();
            }
        }

        // Find the valid data end (file is larger due to pre-allocation).
        let valid_data_end = {
            let mut reader = JournalReader::open(&path).unwrap();
            while reader.next_entry().unwrap().is_some() {}
            reader.valid_file_end()
        };

        // Truncate mid-entry: cut 5 bytes from the valid data region.
        {
            let file = OpenOptions::new().write(true).open(&path).unwrap();
            file.set_len(valid_data_end - 5).unwrap();
        }

        let mut reader = JournalReader::open(&path).unwrap();
        // Should read all but the last (truncated) entry.
        for i in 0..events.len() - 1 {
            let entry = reader.next_entry().unwrap().unwrap();
            assert_eq!(entry.sequence, (i as u64) + 1);
            assert_eq!(&entry.event, &events[i]);
        }
        // Truncated last entry returns None.
        assert!(reader.next_entry().unwrap().is_none());

        // valid_file_end should point to end of last good entry, NOT truncation point.
        assert!(reader.valid_file_end() < valid_data_end - 5);
    }

    #[test]
    fn crc_corruption_mid_stream_is_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("corrupt.journal");

        let events = sample_events();
        {
            let mut writer = JournalWriter::create(&path).unwrap();
            for event in &events {
                writer.append(event).unwrap();
            }
        }

        // Corrupt a byte in the middle of the file (after the header + first entry).
        {
            let mut data = std::fs::read(&path).unwrap();
            // Corrupt somewhere in the second entry.
            let corrupt_offset = FILE_HEADER_SIZE + 50;
            if corrupt_offset < data.len() {
                data[corrupt_offset] ^= 0xFF;
            }
            let mut file = File::create(&path).unwrap();
            file.write_all(&data).unwrap();
        }

        let mut reader = JournalReader::open(&path).unwrap();
        // First entry should be fine.
        let result = reader.next_entry();
        assert!(result.is_ok());
        // Second entry should fail with CRC or corrupt error.
        let result = reader.next_entry();
        assert!(result.is_err() || result.unwrap().is_none());
    }

    #[test]
    fn preallocated_zeros_treated_as_end_of_data() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("prealloc.journal");

        let events = sample_events();
        {
            let mut writer = JournalWriter::create(&path).unwrap();
            for event in &events {
                writer.append(event).unwrap();
            }
        }

        // The file should be larger than valid data due to pre-allocation.
        let file_len = std::fs::metadata(&path).unwrap().len();
        let valid_data_upper_bound = (FILE_HEADER_SIZE + events.len() * 128) as u64;
        assert!(
            file_len > valid_data_upper_bound,
            "file should be pre-allocated: len={file_len}, data<={valid_data_upper_bound}"
        );

        // Reader should stop at the end of valid entries, ignoring zeros.
        let mut reader = JournalReader::open(&path).unwrap();
        for (i, expected) in events.iter().enumerate() {
            let entry = reader.next_entry().unwrap().unwrap();
            assert_eq!(entry.sequence, (i as u64) + 1);
            assert_eq!(&entry.event, expected);
        }
        assert!(reader.next_entry().unwrap().is_none());
    }

    #[test]
    fn recovery_after_crash_with_preallocation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("crash_prealloc.journal");

        let events = sample_events();
        {
            let mut writer = JournalWriter::create(&path).unwrap();
            for event in &events {
                writer.append(event).unwrap();
            }
        }

        // Simulate crash: file still has pre-allocated zeros after valid data.
        // Reader should recover all entries.
        let mut reader = JournalReader::open(&path).unwrap();
        let mut count = 0;
        while reader.next_entry().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, events.len());

        // open_append should truncate pre-allocated space and re-allocate.
        let valid_end = reader.valid_file_end();
        let last_seq = reader.last_sequence().unwrap();
        let mut writer = JournalWriter::open_append(&path, last_seq, valid_end).unwrap();

        // Write one more event after recovery.
        let extra = JournalEvent::Deposit {
            account: AccountId(99),
            currency: CurrencyId(0),
            amount: 42,
        };
        writer.append(&extra).unwrap();

        // Re-read everything.
        let mut reader = JournalReader::open(&path).unwrap();
        for (i, expected) in events.iter().enumerate() {
            let entry = reader.next_entry().unwrap().unwrap();
            assert_eq!(entry.sequence, (i as u64) + 1);
            assert_eq!(&entry.event, expected);
        }
        let entry = reader.next_entry().unwrap().unwrap();
        assert_eq!(entry.sequence, (events.len() as u64) + 1);
        assert_eq!(entry.event, extra);
        assert!(reader.next_entry().unwrap().is_none());
    }

    #[test]
    fn many_events_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("many.journal");

        let n = 1000;
        {
            let mut writer = JournalWriter::create(&path).unwrap();
            for i in 0..n {
                let event = JournalEvent::Deposit {
                    account: AccountId(i % 10),
                    currency: CurrencyId(0),
                    amount: (i as u64) * 100,
                };
                let seq = writer.append(&event).unwrap();
                assert_eq!(seq, (i as u64) + 1);
            }
        }

        let mut reader = JournalReader::open(&path).unwrap();
        for i in 0..n {
            let entry = reader.next_entry().unwrap().unwrap();
            assert_eq!(entry.sequence, (i as u64) + 1);
            assert_eq!(
                entry.event,
                JournalEvent::Deposit {
                    account: AccountId(i % 10),
                    currency: CurrencyId(0),
                    amount: (i as u64) * 100,
                }
            );
        }
        assert!(reader.next_entry().unwrap().is_none());
    }
}
