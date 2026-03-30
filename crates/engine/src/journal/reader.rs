//! Journal reader — sequential read with CRC and sequence validation.
//!
//! Reads entries one at a time. On crash recovery:
//! - Truncated entry at EOF → `Ok(None)` (last partial write, safe to ignore)
//! - CRC mismatch mid-stream → `Err(CorruptEntry)` (real corruption)

use std::fs::File;
use std::io::Read;
use std::path::Path;

use super::codec::{self, FILE_HEADER_SIZE, ENTRY_HEADER_SIZE, CRC_SIZE};
use super::error::JournalError;
use crate::le;
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
    /// Hash of the client's Ed25519 public key (v8+). Zero for internal/seed
    /// events or when reading pre-v8 journals.
    pub key_hash: u64,
    /// Per-key request sequence number (v8+). Zero for pre-v8 journals.
    pub request_seq: u64,
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
    /// Journal format version from the file header. Used to determine entry
    /// layout during decoding (v8+ has key_hash/request_seq fields).
    version: u16,
    /// BLAKE3 hash chain verification state. Initialized when a GenesisHash
    /// entry is read, updated on each normal entry, verified at Checkpoints.
    /// `None` when `hash-chain` feature is disabled or for v5 journals.
    #[cfg(feature = "hash-chain")]
    hash_chain: Option<ReaderHashChain>,
}

/// Hash chain state maintained by the reader for verification.
/// Uses the same batch-level hashing as the writer: entry bytes are fed
/// into an incremental hasher, finalized at checkpoints.
#[cfg(feature = "hash-chain")]
struct ReaderHashChain {
    /// Chain hash from the last checkpoint (or genesis).
    current_hash: [u8; 32],
    /// Incremental hasher accumulating entry bytes since last checkpoint.
    batch_hasher: blake3::Hasher,
    /// Events since last checkpoint (for verification against Checkpoint entries).
    events_since_checkpoint: u64,
}

impl JournalReader {
    /// Open a journal file for reading. Validates the file header.
    pub fn open(path: &Path) -> Result<Self, JournalError> {
        let mut file = File::open(path)?;

        // Read and validate the file header.
        let mut header = [0u8; FILE_HEADER_SIZE];
        file.read_exact(&mut header)?;
        let version = codec::decode_file_header(&header)?;

        Ok(Self {
            file,
            buffer: vec![0u8; INITIAL_BUF_SIZE],
            pos: 0,
            valid: 0,
            last_sequence: None,
            valid_file_end: FILE_HEADER_SIZE as u64,
            version,
            #[cfg(feature = "hash-chain")]
            hash_chain: None,
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

        match codec::decode(data, self.version) {
            Ok((consumed, sequence, timestamp_ns, key_hash, request_seq, event)) => self
                .validate_and_advance(
                    consumed,
                    sequence,
                    timestamp_ns,
                    key_hash,
                    request_seq,
                    event,
                ),
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

                    match codec::decode(data, self.version) {
                        Ok((consumed, sequence, timestamp_ns, key_hash, request_seq, event)) => {
                            self.validate_and_advance(
                                consumed,
                                sequence,
                                timestamp_ns,
                                key_hash,
                                request_seq,
                                event,
                            )
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

    /// Validate sequence continuity, update hash chain, and advance read position.
    ///
    /// For `GenesisHash` and `Checkpoint` entries, processes them internally
    /// and returns the next real event via recursive call — they are
    /// transparent to callers.
    fn validate_and_advance(
        &mut self,
        consumed: usize,
        sequence: u64,
        timestamp_ns: u64,
        key_hash: u64,
        request_seq: u64,
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

        // --- BLAKE3 hash chain verification (feature-gated) ---
        #[cfg(feature = "hash-chain")]
        {
            // Raw entry bytes excluding CRC (for hash chain computation).
            let entry_bytes_end = self.pos + consumed - 4;

            // Handle GenesisHash: (re)initialize the chain.
            if let JournalEvent::GenesisHash { .. } = &event {
                let genesis_hash = blake3::hash(&self.buffer[self.pos..entry_bytes_end]);
                self.hash_chain = Some(ReaderHashChain {
                    current_hash: *genesis_hash.as_bytes(),
                    batch_hasher: blake3::Hasher::new(),
                    events_since_checkpoint: 0,
                });
                self.last_sequence = Some(sequence);
                self.pos += consumed;
                self.valid_file_end += consumed as u64;
                return self.next_entry();
            }

            // Checkpoint: finalize accumulated batch hash and verify against
            // the checkpoint's recorded hash.
            if let JournalEvent::Checkpoint {
                chain_hash,
                events_since_checkpoint,
            } = &event
            {
                if let Some(chain) = &mut self.hash_chain {
                    // Finalize: feed the checkpoint entry bytes + previous
                    // chain hash, then compare with the recorded hash.
                    chain.batch_hasher.update(&self.buffer[self.pos..entry_bytes_end]);
                    chain.batch_hasher.update(&chain.current_hash);
                    let computed = *chain.batch_hasher.finalize().as_bytes();

                    if computed != *chain_hash {
                        return Err(JournalError::HashChainMismatch {
                            sequence,
                            expected: *chain_hash,
                            actual: computed,
                        });
                    }
                    if chain.events_since_checkpoint != *events_since_checkpoint {
                        return Err(JournalError::CorruptEntry {
                            sequence,
                            reason: "checkpoint event count mismatch",
                        });
                    }

                    chain.current_hash = computed;
                    chain.batch_hasher = blake3::Hasher::new();
                    chain.events_since_checkpoint = 0;
                }
                self.last_sequence = Some(sequence);
                self.pos += consumed;
                self.valid_file_end += consumed as u64;
                return self.next_entry();
            }

            // Normal event: feed bytes into incremental batch hasher.
            if let Some(chain) = &mut self.hash_chain {
                chain.batch_hasher.update(&self.buffer[self.pos..entry_bytes_end]);
                chain.events_since_checkpoint += 1;
            }
        }

        // Without hash-chain, skip GenesisHash/Checkpoint (no state change).
        #[cfg(not(feature = "hash-chain"))]
        if matches!(
            event,
            JournalEvent::GenesisHash { .. } | JournalEvent::Checkpoint { .. }
        ) {
            self.last_sequence = Some(sequence);
            self.pos += consumed;
            self.valid_file_end += consumed as u64;
            return self.next_entry();
        }

        self.last_sequence = Some(sequence);
        self.pos += consumed;
        self.valid_file_end += consumed as u64;

        Ok(Some(JournalEntry {
            sequence,
            timestamp_ns,
            key_hash,
            request_seq,
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

    /// Current BLAKE3 chain hash after all entries read so far.
    /// Returns `None` when `hash-chain` is disabled, for v5 journals, or
    /// if no events have been read.
    pub fn chain_hash(&self) -> Option<[u8; 32]> {
        #[cfg(feature = "hash-chain")]
        {
            self.hash_chain.as_ref().map(|c| c.current_hash)
        }
        #[cfg(not(feature = "hash-chain"))]
        None
    }

    /// Events since last checkpoint in the hash chain.
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

    /// Seed the hash chain from a snapshot's chain hash.
    pub fn seed_chain_hash(&mut self, chain_hash: [u8; 32], _snap_sequence: u64) {
        #[cfg(feature = "hash-chain")]
        {
            if chain_hash == [0u8; 32] {
                return;
            }
            self.hash_chain = Some(ReaderHashChain {
                current_hash: chain_hash,
                batch_hasher: blake3::Hasher::new(),
                events_since_checkpoint: 0,
            });
        }
        #[cfg(not(feature = "hash-chain"))]
        {
            let _ = chain_hash;
        }
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

// ---------------------------------------------------------------------------
// RawJournalScanner — lightweight raw byte reader for replication catch-up
// ---------------------------------------------------------------------------

/// Reads raw journal entry bytes without full decoding. Used by the
/// replication sender to stream historical entries to a catching-up
/// replica. Only extracts entry boundaries (via the length field) and
/// sequence numbers — no CRC validation, no event parsing.
///
/// The journal was already validated when written; re-validating during
/// catch-up would add unnecessary CPU overhead for millions of entries.
pub struct RawJournalScanner {
    file: File,
    /// Read buffer — entries are read into this, then raw bytes copied out.
    buf: Vec<u8>,
    /// Current read position within `buf`.
    pos: usize,
    /// Number of valid bytes in `buf`.
    valid: usize,
}

impl RawJournalScanner {
    /// Open a journal file for raw scanning. Validates the file header.
    pub fn open(path: &Path) -> Result<Self, JournalError> {
        let mut file = File::open(path)?;
        let mut header = [0u8; FILE_HEADER_SIZE];
        file.read_exact(&mut header)?;
        // Validate header (version check) but don't store version —
        // the scanner only reads entry boundaries, not event payloads.
        let _version = codec::decode_file_header(&header)?;

        Ok(Self {
            file,
            buf: vec![0u8; 64 * 1024], // 64 KiB read buffer
            pos: 0,
            valid: 0,
        })
    }

    /// Peek at the first entry's sequence number without advancing.
    /// Returns `None` if the file has no entries (empty or only header).
    pub fn first_sequence(&mut self) -> Result<Option<u64>, JournalError> {
        self.ensure_available(ENTRY_HEADER_SIZE)?;
        let available = self.valid - self.pos;
        if available < ENTRY_HEADER_SIZE {
            return Ok(None);
        }
        // Zero magic = pre-allocated space, no entries.
        if self.buf[self.pos] == 0 && self.buf[self.pos + 1] == 0 {
            return Ok(None);
        }
        // Sequence is at offset 4 within the entry (after magic + length).
        Ok(Some(le::get_u64(&self.buf[self.pos + 4..])))
    }

    /// Skip forward past all entries with sequence ≤ `target_seq`.
    /// After this call, the next `read_raw_batch` will return entries
    /// starting from the first entry with sequence > `target_seq`.
    pub fn skip_to_after(&mut self, target_seq: u64) -> Result<(), JournalError> {
        loop {
            self.ensure_available(ENTRY_HEADER_SIZE)?;
            let available = self.valid - self.pos;
            if available < ENTRY_HEADER_SIZE {
                return Ok(()); // EOF
            }
            // Zero magic = end of data.
            if self.buf[self.pos] == 0 && self.buf[self.pos + 1] == 0 {
                return Ok(());
            }
            let entry_seq = le::get_u64(&self.buf[self.pos + 4..]);
            if entry_seq > target_seq {
                return Ok(()); // Found the first entry past target.
            }
            // Skip this entry.
            let payload_len = le::get_u16(&self.buf[self.pos + 2..]) as usize;
            let total = ENTRY_HEADER_SIZE + payload_len + CRC_SIZE;
            self.ensure_available(total)?;
            if self.valid - self.pos < total {
                return Ok(()); // Truncated entry at EOF.
            }
            self.pos += total;
        }
    }

    /// Read raw entry bytes into `out`, up to `max_bytes` total.
    /// Returns `Some((entry_count, end_sequence))` with the number of
    /// entries and the last sequence in the batch. Returns `None` at EOF
    /// or when no complete entry fits within `max_bytes`.
    pub fn read_raw_batch(
        &mut self,
        out: &mut Vec<u8>,
        max_bytes: usize,
    ) -> Result<Option<(u32, u64)>, JournalError> {
        let mut count = 0u32;
        let mut end_seq = 0u64;
        let batch_start = out.len();

        loop {
            self.ensure_available(ENTRY_HEADER_SIZE)?;
            let available = self.valid - self.pos;
            if available < ENTRY_HEADER_SIZE {
                break; // EOF
            }
            if self.buf[self.pos] == 0 && self.buf[self.pos + 1] == 0 {
                break; // Pre-allocated space.
            }

            let payload_len = le::get_u16(&self.buf[self.pos + 2..]) as usize;
            let total = ENTRY_HEADER_SIZE + payload_len + CRC_SIZE;

            // Don't exceed max_bytes (but always include at least one entry).
            if count > 0 && (out.len() - batch_start) + total > max_bytes {
                break;
            }

            self.ensure_available(total)?;
            if self.valid - self.pos < total {
                break; // Truncated entry at EOF.
            }

            end_seq = le::get_u64(&self.buf[self.pos + 4..]);
            out.extend_from_slice(&self.buf[self.pos..self.pos + total]);
            self.pos += total;
            count += 1;
        }

        if count == 0 {
            Ok(None)
        } else {
            Ok(Some((count, end_seq)))
        }
    }

    /// Ensure at least `needed` bytes are available in the buffer.
    /// Compacts and refills as needed.
    fn ensure_available(&mut self, needed: usize) -> Result<(), JournalError> {
        while self.valid - self.pos < needed {
            // Compact: move remaining data to the start.
            if self.pos > 0 {
                self.buf.copy_within(self.pos..self.valid, 0);
                self.valid -= self.pos;
                self.pos = 0;
            }
            // Grow buffer if needed.
            if self.buf.len() < needed {
                self.buf.resize(needed, 0);
            }
            // Read more data.
            let n = self.file.read(&mut self.buf[self.valid..])?;
            if n == 0 {
                return Ok(()); // EOF — caller checks available bytes.
            }
            self.valid += n;
        }
        Ok(())
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

    /// First user-event sequence: 2 with hash-chain (genesis takes 1), 1 without.
    #[cfg(feature = "hash-chain")]
    const FIRST_SEQ: u64 = 2;
    #[cfg(not(feature = "hash-chain"))]
    const FIRST_SEQ: u64 = 1;

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
                        post_only: false,
                    },
                    time_in_force: TimeInForce::GTC,
                    quantity: Quantity(nz(10)),
                    stp: SelfTradeProtection::CancelNewest,
                    expiry_ns: 0,
                },
            },
            JournalEvent::CancelOrder {
                symbol: Symbol(1),
                account: AccountId(42),
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
            assert_eq!(entry.sequence, (i as u64) + FIRST_SEQ);
            assert_eq!(&entry.event, expected);
            assert!(entry.timestamp_ns > 0);
        }
        assert!(reader.next_entry().unwrap().is_none());
        // Hash chain is active only with the feature.
        #[cfg(feature = "hash-chain")]
        assert!(reader.chain_hash().is_some());
        #[cfg(not(feature = "hash-chain"))]
        assert!(reader.chain_hash().is_none());
    }

    #[test]
    fn empty_journal_reads_none() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.journal");

        let _writer = JournalWriter::create(&path).unwrap();

        let mut reader = JournalReader::open(&path).unwrap();
        // Genesis entry is transparent — returns None for no user events.
        assert!(reader.next_entry().unwrap().is_none());
        // With hash-chain, valid_file_end includes the genesis entry.
        // Without hash-chain, valid_file_end is at the file header.
        #[cfg(feature = "hash-chain")]
        assert!(reader.valid_file_end() > FILE_HEADER_SIZE as u64);
        #[cfg(not(feature = "hash-chain"))]
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
            assert_eq!(entry.sequence, (i as u64) + FIRST_SEQ);
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

        // Read the journal to find valid data end and entry positions.
        // Then corrupt within the third entry's payload (second user entry).
        let valid_data_end = {
            let mut reader = JournalReader::open(&path).unwrap();
            while reader.next_entry().unwrap().is_some() {}
            reader.valid_file_end()
        };

        // Corrupt a byte roughly in the middle of the valid data —
        // well past the genesis + first user entry.
        {
            let mut data = std::fs::read(&path).unwrap();
            let corrupt_offset = (FILE_HEADER_SIZE as u64 + valid_data_end) / 2;
            let corrupt_offset = corrupt_offset as usize;
            if corrupt_offset < data.len() {
                data[corrupt_offset] ^= 0xFF;
            }
            let mut file = File::create(&path).unwrap();
            file.write_all(&data).unwrap();
        }

        let mut reader = JournalReader::open(&path).unwrap();
        // Read entries until we hit an error or unexpected end.
        let mut found_error = false;
        loop {
            match reader.next_entry() {
                Ok(Some(_)) => continue,
                Ok(None) => break,
                Err(_) => {
                    found_error = true;
                    break;
                }
            }
        }
        assert!(found_error, "expected corruption to be detected");
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
        // Genesis entry is transparent.
        let mut reader = JournalReader::open(&path).unwrap();
        for (i, expected) in events.iter().enumerate() {
            let entry = reader.next_entry().unwrap().unwrap();
            assert_eq!(entry.sequence, (i as u64) + FIRST_SEQ);
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
        let mut writer = JournalWriter::open_append(&path, last_seq, valid_end, None, 0).unwrap();

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
            assert_eq!(entry.sequence, (i as u64) + FIRST_SEQ);
            assert_eq!(&entry.event, expected);
        }
        let entry = reader.next_entry().unwrap().unwrap();
        assert_eq!(entry.sequence, (events.len() as u64) + FIRST_SEQ);
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
                assert_eq!(seq, (i as u64) + FIRST_SEQ);
            }
        }

        let mut reader = JournalReader::open(&path).unwrap();
        for i in 0..n {
            let entry = reader.next_entry().unwrap().unwrap();
            assert_eq!(entry.sequence, (i as u64) + FIRST_SEQ);
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

    #[cfg(feature = "hash-chain")]
    #[test]
    fn corrupted_checkpoint_detected() {
        use crate::journal::writer::CHECKPOINT_INTERVAL;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("corrupt_checkpoint.journal");

        // Write enough events to trigger a checkpoint.
        {
            let mut writer = JournalWriter::create(&path).unwrap();
            for _ in 0..CHECKPOINT_INTERVAL + 10 {
                writer
                    .batch_append(&JournalEvent::Deposit {
                        account: AccountId(1),
                        currency: CurrencyId(0),
                        amount: 100,
                    })
                    .unwrap();
            }
            writer.flush_batch().unwrap();
        }

        // Find the checkpoint entry in the raw data and corrupt its chain_hash.
        // The checkpoint has tag 10 (TAG_CHECKPOINT).
        {
            let mut data = std::fs::read(&path).unwrap();
            // Scan for the checkpoint tag. Entry format:
            // magic(2) + length(2) + seq(8) + ts(8) + tag(1) + payload...
            let mut offset = FILE_HEADER_SIZE;
            let mut found = false;
            while offset + 25 < data.len() {
                let magic = u16::from_le_bytes([data[offset], data[offset + 1]]);
                if magic != 0x4A45 {
                    break;
                }
                let length = u16::from_le_bytes([data[offset + 2], data[offset + 3]]) as usize;
                let total = 20 + length + 4;
                let tag = data[offset + 20];
                if tag == 10 {
                    // Corrupt the chain_hash in the checkpoint payload.
                    // Payload starts at offset+21 (after tag).
                    data[offset + 21] ^= 0xFF;
                    // Fix CRC so it's not caught by CRC check first.
                    let data_end = offset + 20 + length;
                    let new_crc = crc32c::crc32c(&data[offset..data_end]);
                    data[data_end..data_end + 4].copy_from_slice(&new_crc.to_le_bytes());
                    found = true;
                    break;
                }
                offset += total;
            }
            assert!(found, "checkpoint entry not found in journal");
            std::fs::write(&path, &data).unwrap();
        }

        // Reading should fail with HashChainMismatch at the checkpoint.
        let mut reader = JournalReader::open(&path).unwrap();
        let mut found_mismatch = false;
        loop {
            match reader.next_entry() {
                Ok(Some(_)) => continue,
                Ok(None) => break,
                Err(JournalError::HashChainMismatch { .. }) => {
                    found_mismatch = true;
                    break;
                }
                Err(e) => panic!("unexpected error: {e}"),
            }
        }
        assert!(found_mismatch, "expected HashChainMismatch error");
    }

    #[cfg(feature = "hash-chain")]
    #[test]
    fn v5_journal_has_no_hash_chain() {
        use std::os::unix::fs::FileExt;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v5.journal");

        // Create a v5 journal manually: write v5 header + one raw entry.
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();

        // v5 file header: JOUR magic + version 5 + reserved.
        let mut header = [0u8; 8];
        header[0..4].copy_from_slice(&0x4A4F_5552u32.to_le_bytes());
        header[4..6].copy_from_slice(&5u16.to_le_bytes());
        file.write_all_at(&header, 0).unwrap();

        // Write a Deposit entry at sequence 1.
        let event = JournalEvent::Deposit {
            account: AccountId(1),
            currency: CurrencyId(0),
            amount: 100,
        };
        let mut buf = [0u8; 144];
        let written = crate::journal::codec::encode(1, 1000, 0, 0, &event, &mut buf).unwrap();
        file.write_all_at(&buf[..written], 8).unwrap();
        drop(file);

        // Read back: should work, chain_hash should be None.
        let mut reader = JournalReader::open(&path).unwrap();
        let entry = reader.next_entry().unwrap().unwrap();
        assert_eq!(entry.sequence, 1);
        assert_eq!(entry.event, event);
        assert!(reader.next_entry().unwrap().is_none());
        assert!(
            reader.chain_hash().is_none(),
            "v5 journal should have no hash chain"
        );
    }

    #[cfg(feature = "hash-chain")]
    #[test]
    fn checkpoint_event_count_mismatch_detected() {
        use crate::journal::writer::CHECKPOINT_INTERVAL;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("count_mismatch.journal");

        {
            let mut writer = JournalWriter::create(&path).unwrap();
            for _ in 0..CHECKPOINT_INTERVAL + 10 {
                writer
                    .batch_append(&JournalEvent::Deposit {
                        account: AccountId(1),
                        currency: CurrencyId(0),
                        amount: 100,
                    })
                    .unwrap();
            }
            writer.flush_batch().unwrap();
        }

        // Find the checkpoint and corrupt its events_since_checkpoint field
        // (at offset +32 in the payload, after the 32-byte chain_hash).
        {
            let mut data = std::fs::read(&path).unwrap();
            let mut offset = FILE_HEADER_SIZE;
            let mut found = false;
            while offset + 25 < data.len() {
                let magic = u16::from_le_bytes([data[offset], data[offset + 1]]);
                if magic != 0x4A45 {
                    break;
                }
                let length = u16::from_le_bytes([data[offset + 2], data[offset + 3]]) as usize;
                let total = 20 + length + 4;
                let tag = data[offset + 20];
                if tag == 10 {
                    // events_since_checkpoint is at payload offset 32 (after
                    // 32-byte chain_hash). Payload starts at offset+21.
                    let count_offset = offset + 21 + 32;
                    // Write a wrong count (keep chain_hash correct).
                    data[count_offset..count_offset + 8].copy_from_slice(&12345u64.to_le_bytes());
                    // Fix CRC.
                    let data_end = offset + 20 + length;
                    let new_crc = crc32c::crc32c(&data[offset..data_end]);
                    data[data_end..data_end + 4].copy_from_slice(&new_crc.to_le_bytes());
                    found = true;
                    break;
                }
                offset += total;
            }
            assert!(found, "checkpoint not found");
            std::fs::write(&path, &data).unwrap();
        }

        // Reading should fail with CorruptEntry (event count mismatch).
        let mut reader = JournalReader::open(&path).unwrap();
        let mut found_error = false;
        loop {
            match reader.next_entry() {
                Ok(Some(_)) => continue,
                Ok(None) => break,
                Err(JournalError::CorruptEntry {
                    reason: "checkpoint event count mismatch",
                    ..
                }) => {
                    found_error = true;
                    break;
                }
                Err(e) => panic!("unexpected error: {e}"),
            }
        }
        assert!(found_error, "expected checkpoint event count mismatch");
    }

    #[cfg(feature = "hash-chain")]
    #[test]
    fn tamper_between_checkpoints_detected_at_next_checkpoint() {
        use crate::journal::writer::CHECKPOINT_INTERVAL;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tamper.journal");

        // Write 200K events (2 checkpoints).
        {
            let mut writer = JournalWriter::create(&path).unwrap();
            for _ in 0..CHECKPOINT_INTERVAL * 2 {
                writer
                    .batch_append(&JournalEvent::Deposit {
                        account: AccountId(1),
                        currency: CurrencyId(0),
                        amount: 100,
                    })
                    .unwrap();
            }
            writer.flush_batch().unwrap();
        }

        // Corrupt a normal entry between the first and second checkpoints.
        // Change the amount field, then fix CRC so the entry passes CRC
        // validation. The hash chain will silently diverge, and the second
        // checkpoint should detect the mismatch.
        {
            let mut data = std::fs::read(&path).unwrap();
            let mut offset = FILE_HEADER_SIZE;
            let mut entry_count = 0u64;
            let mut tampered = false;

            while offset + 25 < data.len() {
                let magic = u16::from_le_bytes([data[offset], data[offset + 1]]);
                if magic != 0x4A45 {
                    break;
                }
                let length = u16::from_le_bytes([data[offset + 2], data[offset + 3]]) as usize;
                let total = 20 + length + 4;
                let tag = data[offset + 20];

                // Skip genesis (tag 9) and first checkpoint (tag 10).
                if tag != 9 && tag != 10 {
                    entry_count += 1;
                }

                // Tamper with an entry in the second interval (after first
                // checkpoint, before second). Pick entry ~150K.
                if entry_count == CHECKPOINT_INTERVAL + CHECKPOINT_INTERVAL / 2 && !tampered {
                    // Flip a payload byte.
                    data[offset + 22] ^= 0xFF;
                    // Fix CRC so it passes CRC check.
                    let data_end = offset + 20 + length;
                    let new_crc = crc32c::crc32c(&data[offset..data_end]);
                    data[data_end..data_end + 4].copy_from_slice(&new_crc.to_le_bytes());
                    tampered = true;
                }

                offset += total;
            }
            assert!(tampered, "failed to tamper with entry");
            std::fs::write(&path, &data).unwrap();
        }

        // Reading should succeed until the second checkpoint, then fail
        // with HashChainMismatch.
        let mut reader = JournalReader::open(&path).unwrap();
        let mut found_mismatch = false;
        loop {
            match reader.next_entry() {
                Ok(Some(_)) => continue,
                Ok(None) => break,
                Err(JournalError::HashChainMismatch { .. }) => {
                    found_mismatch = true;
                    break;
                }
                Err(e) => panic!("unexpected error: {e}"),
            }
        }
        assert!(
            found_mismatch,
            "tampered entry should be caught at next checkpoint"
        );
    }

    #[cfg(feature = "hash-chain")]
    #[test]
    fn crash_recovery_preserves_chain_continuity() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("crash_chain.journal");

        // Write some events, then simulate crash.
        let chain_before_crash;
        {
            let mut writer = JournalWriter::create(&path).unwrap();
            for _ in 0..50 {
                writer
                    .append(&JournalEvent::Deposit {
                        account: AccountId(1),
                        currency: CurrencyId(0),
                        amount: 100,
                    })
                    .unwrap();
            }
            chain_before_crash = writer.chain_hash().unwrap();
        }

        // Simulate crash by truncating the last entry.
        let (last_seq, valid_end, chain_hash, events_since) = {
            let mut reader = JournalReader::open(&path).unwrap();
            while reader.next_entry().unwrap().is_some() {}
            let valid_end = reader.valid_file_end();
            // Truncate 5 bytes from the valid region to simulate partial write.
            let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            file.set_len(valid_end - 5).unwrap();
            drop(file);

            // Re-read to get state after truncation.
            let mut reader2 = JournalReader::open(&path).unwrap();
            let mut count = 0;
            while reader2.next_entry().unwrap().is_some() {
                count += 1;
            }
            assert_eq!(count, 49); // one entry lost to truncation
            (
                reader2.last_sequence().unwrap(),
                reader2.valid_file_end(),
                reader2.chain_hash(),
                reader2.events_since_checkpoint(),
            )
        };

        // Recover and continue writing.
        let mut writer =
            JournalWriter::open_append(&path, last_seq, valid_end, chain_hash, events_since)
                .unwrap();
        // Chain should NOT equal the pre-crash hash (lost one event).
        assert_ne!(writer.chain_hash().unwrap(), chain_before_crash);

        // Write 10 more events.
        for _ in 0..10 {
            writer
                .append(&JournalEvent::Deposit {
                    account: AccountId(1),
                    currency: CurrencyId(0),
                    amount: 100,
                })
                .unwrap();
        }
        let final_hash = writer.chain_hash().unwrap();
        drop(writer);

        // Full re-read should produce the same chain hash.
        let mut reader = JournalReader::open(&path).unwrap();
        let mut count = 0;
        while reader.next_entry().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 59); // 49 + 10
        assert_eq!(reader.chain_hash().unwrap(), final_hash);
    }
}
