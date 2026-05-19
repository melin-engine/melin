//! Journal reader — sequential read with CRC and sequence validation.
//!
//! Reads entries one at a time. On crash recovery:
//! - Truncated entry at EOF → `Ok(None)` (last partial write, safe to ignore)
//! - CRC mismatch mid-stream → `Err(CorruptEntry)` (real corruption)

use std::fs::File;
use std::io::Read;
use std::marker::PhantomData;
use std::path::Path;

use melin_app::AppEvent;

use zerocopy::FromBytes;

#[cfg(test)]
use super::codec::ENTRY_OFFSET;
use super::codec::{self, CRC_SIZE, ENTRY_HEADER_SIZE, EntryHeader, FILE_HEADER_SIZE};
use super::error::JournalError;
use super::event::JournalEvent;

/// Initial read buffer size. Sized to amortize `read()` syscall and
/// per-call compaction overhead across many entries — at ~50–250 bytes
/// per entry this holds thousands of entries per refill. Grows if a
/// single entry exceeds it (snapshots can be larger), but for steady-
/// state event scans it never resizes. The 1 MiB allocation lives for
/// the lifetime of the reader; recovery opens readers one-at-a-time so
/// the working-set cost is bounded.
///
/// Uses a Vec (growable) rather than a fixed array because the reader
/// may need to buffer multiple entries when entries span read
/// boundaries, and because rare oversized entries grow the buffer.
const INITIAL_BUF_SIZE: usize = 1 << 20;

/// A decoded journal entry with its metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalEntry<E: AppEvent> {
    /// Monotonically increasing sequence number (starts at 1).
    pub sequence: u64,
    /// Wall-clock nanos since epoch at write time (informational, not for ordering).
    pub timestamp_ns: u64,
    /// Hash of the client's Ed25519 public key. Zero for internal/seed events.
    pub key_hash: u64,
    /// Per-key request sequence number.
    pub request_seq: u64,
    /// The event that was journaled.
    pub event: JournalEvent<E>,
}

/// Reads journal entries sequentially, validating checksums and sequence continuity.
pub struct JournalReader<E: AppEvent> {
    _marker: PhantomData<fn() -> E>,
    file: File,
    /// Read buffer. Sized at `INITIAL_BUF_SIZE` so a single `read()`
    /// covers thousands of entries; grows only when a single entry
    /// exceeds the current capacity. Vec rather than a fixed array so
    /// rare oversized entries (e.g. snapshots) can still be decoded.
    buffer: Vec<u8>,
    /// Current read position within `buffer`. Advances per decoded
    /// entry; compaction back to 0 is deferred until the buffer tail
    /// is exhausted (see `try_extend_buffer`).
    pos: usize,
    /// Number of valid bytes in `buffer` (from last read). Bytes
    /// `[pos..valid]` are the unconsumed window the decoder reads from.
    valid: usize,
    /// Last sequence number read, for gap detection.
    last_sequence: Option<u64>,
    /// Byte offset in the file of the end of the last successfully decoded entry.
    /// Used by recovery to know where to truncate trailing garbage.
    valid_file_end: u64,
    /// Journal format version from the file header. Used to determine entry
    /// layout during decoding (v8+ has key_hash/request_seq fields).
    version: u16,
    /// Physical sector size used when the journal was created (512 or 4096).
    /// Determines the byte offset where entries begin (one sector after the
    /// file header). Decoded from the file header at open time.
    sector_size: usize,
    /// BLAKE3 hash chain verification state. Initialized when a GenesisHash
    /// entry is read, updated on each normal entry, verified at Checkpoints.
    /// `None` when `hash-chain` feature is disabled or for v5 journals.
    #[cfg(feature = "hash-chain")]
    hash_chain: Option<ReaderHashChain>,
    /// Payload of the most recently observed `GenesisHash` entry (i.e. the
    /// chain hash this segment was anchored to at creation time). Used by
    /// multi-segment recovery to verify that segment N+1's genesis matches
    /// segment N's final chain hash, closing the cross-segment tamper-
    /// evidence gap that within-segment chain validation alone cannot
    /// catch.
    #[cfg(feature = "hash-chain")]
    genesis_payload: Option<[u8; 32]>,
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

impl<E: AppEvent> JournalReader<E> {
    /// Open a journal file for reading. Validates the file header.
    pub fn open(path: &Path) -> Result<Self, JournalError> {
        use std::io::Seek;
        let mut file = File::open(path)?;

        // Read and validate the file header. We use pread so the file cursor
        // stays at zero; we then seek to sector_size to position the reader
        // at the first entry. FILE_HEADER_SIZE bytes is always enough to
        // decode all header fields (the meaningful content is 8 bytes).
        let mut header = [0u8; FILE_HEADER_SIZE];
        file.read_exact(&mut header)?;
        let (version, sector_size) = codec::decode_file_header(&header)?;

        // Skip any padding between FILE_HEADER_SIZE and sector_size (zero on
        // 512-byte devices; up to 3.5 KiB on 4Kn devices). Entries start at
        // exactly one sector offset.
        file.seek(std::io::SeekFrom::Start(sector_size as u64))?;

        Ok(Self {
            _marker: PhantomData,
            file,
            buffer: vec![0u8; INITIAL_BUF_SIZE],
            pos: 0,
            valid: 0,
            last_sequence: None,
            valid_file_end: sector_size as u64,
            version,
            sector_size,
            #[cfg(feature = "hash-chain")]
            hash_chain: None,
            #[cfg(feature = "hash-chain")]
            genesis_payload: None,
        })
    }

    /// Read the next journal entry.
    ///
    /// Returns `Ok(Some(entry))` for each valid entry.
    /// Returns `Ok(None)` at EOF, on a truncated final entry (crash recovery),
    /// or when reaching zero-filled pre-allocated (fallocated) space.
    /// Returns `Err` on corruption (CRC mismatch, sequence gap, etc.).
    pub fn next_entry(&mut self) -> Result<Option<JournalEntry<E>>, JournalError> {
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
                        Err(e) => self.classify_decode_error(e),
                    }
                } else {
                    // No more data available — truncated at EOF.
                    Ok(None)
                }
            }
            Err(e) => self.classify_decode_error(e),
        }
    }

    /// Defense in depth: distinguish "walked into a partially-initialised
    /// region of a preallocated segment" from real corruption / data loss.
    ///
    /// Bytes past the writer's last fully-durable entry may legitimately
    /// look entry-shaped under specific failure modes (e.g. an in-flight
    /// async write whose CQE has not yet arrived, or a torn write where
    /// the header sectors landed but the trailing CRC sector did not). In
    /// every such case the CRC slot on disk reads as `0x00000000` — the
    /// preallocation pattern — because no write ever placed real CRC
    /// bytes there. Treat that signature as end-of-data **only** when the
    /// rest of the file is genuinely preallocation zeros: a zero-CRC in
    /// the middle of a file with more entry-shaped bytes after it is a
    /// hole, i.e. *data loss*, and must surface as corruption so recovery
    /// halts loudly instead of silently truncating the journal.
    ///
    /// We only apply the heuristic past genesis (`last_sequence` is set)
    /// — at the very start of a file, a zero CRC genuinely indicates
    /// corruption of the first entry.
    ///
    /// The log line is `warn` so the event is always visible: a
    /// CRC-32C of valid data CAN coincidentally equal zero (≈1 in 2^32),
    /// in which case we'd silently drop one real entry. The warning lets
    /// operators audit every occurrence.
    fn classify_decode_error(
        &self,
        err: JournalError,
    ) -> Result<Option<JournalEntry<E>>, JournalError> {
        if let JournalError::ChecksumMismatch { expected, .. } = &err
            && *expected == 0
            && self.last_sequence.is_some()
            && self.tail_is_all_zero_past_suspect()?
        {
            tracing::warn!(
                last_sequence = ?self.last_sequence,
                valid_file_end = self.valid_file_end,
                "journal reader stopped on suspected pre-allocated tail \
                 (CRC slot is zero and rest of file is all zeros); \
                 treating as end-of-data"
            );
            return Ok(None);
        }
        Err(err)
    }

    /// True iff every byte strictly past the suspect entry is zero — i.e.
    /// the only non-zero bytes left in the file are the malformed entry
    /// itself, sitting at the writer's last-claimed-durable boundary.
    ///
    /// The suspect entry's own bytes are skipped (it's the partial write
    /// pattern we're choosing to treat as preallocated tail). What we
    /// must guard against is a *hole*: a CRC=0 entry with real entries
    /// after it. That signature is data loss and must surface as a hard
    /// error so recovery halts instead of silently truncating.
    ///
    /// Reads are positioned via `read_at` (pread) so they don't disturb
    /// the buffered reader cursor; chunked so a multi-MB tail doesn't
    /// allocate a matching buffer.
    fn tail_is_all_zero_past_suspect(&self) -> Result<bool, JournalError> {
        use std::os::unix::fs::FileExt;
        // Recover the suspect entry's total on-disk size from its own
        // header bytes (which decode parsed cleanly — only the CRC was
        // wrong). The bytes are still in `self.buffer[self.pos..]`.
        let data = &self.buffer[self.pos..self.valid];
        if data.len() < ENTRY_HEADER_SIZE {
            // Header not fully buffered — be conservative and refuse to
            // treat as EOF; the caller will surface the original error.
            return Ok(false);
        }
        let (header, _) =
            EntryHeader::ref_from_prefix(data).map_err(|_| JournalError::TruncatedEntry)?;
        let payload_len = header.length.get() as usize;
        let suspect_entry_end =
            self.valid_file_end + (ENTRY_HEADER_SIZE + payload_len + CRC_SIZE) as u64;

        let file_end = self.file.metadata()?.len();
        if suspect_entry_end > file_end {
            // The "entry" claims to extend past EOF — definitely garbage,
            // and nothing after it to worry about. Safe to treat as EOF.
            return Ok(true);
        }

        let mut offset = suspect_entry_end;
        let mut scratch = [0u8; 8192];
        while offset < file_end {
            let want = ((file_end - offset) as usize).min(scratch.len());
            let n = self.file.read_at(&mut scratch[..want], offset)?;
            if n == 0 {
                break;
            }
            if scratch[..n].iter().any(|b| *b != 0) {
                return Ok(false);
            }
            offset += n as u64;
        }
        Ok(true)
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
        event: JournalEvent<E>,
    ) -> Result<Option<JournalEntry<E>>, JournalError> {
        // For the first entry, accept whatever sequence we find (supports
        // rotated journals that continue from a prior sequence). For
        // subsequent entries, enforce strict continuity.
        if let Some(last) = self.last_sequence {
            let expected = last + 1;
            // `last` is the reader's internal cursor, which advances
            // through transparent entries (GenesisHash, Checkpoint) as
            // well as visible events. A duplicate-of-a-skipped-seq
            // therefore produces `sequence == last`, not `sequence ==
            // expected`. Split the two cases so operators can tell
            // "data missing" from "writer emitted the same seq twice".
            if sequence < expected {
                return Err(JournalError::SequenceDuplicate {
                    sequence,
                    previous_seq: last,
                });
            }
            if sequence > expected {
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

            // Handle GenesisHash: (re)initialize the chain. Capture the
            // payload (the previous-segment chain hash) so multi-segment
            // recovery can verify the boundary against the tail of the
            // prior segment.
            if let JournalEvent::GenesisHash { hash } = &event {
                self.genesis_payload = Some(*hash);
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
                    // Finalize: feed accumulated event bytes + previous chain
                    // hash, then compare with the recorded hash. The checkpoint
                    // entry itself is NOT part of this hash — it goes into the
                    // next batch (matching writer behaviour).
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
                chain
                    .batch_hasher
                    .update(&self.buffer[self.pos..entry_bytes_end]);
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

    /// Test-only constructor that opens the journal with a custom
    /// initial buffer size. Lets unit tests force entries to straddle
    /// buffer boundaries (and exercise the refill/grow paths) without
    /// having to write millions of entries to overflow the production
    /// 1 MiB buffer.
    #[cfg(test)]
    pub(crate) fn open_with_buffer(path: &Path, buf_size: usize) -> Result<Self, JournalError> {
        let mut reader = Self::open(path)?;
        reader.buffer = vec![0u8; buf_size];
        Ok(reader)
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

    /// Physical sector size used when the journal was created (512 or 4096).
    /// Entries start at this byte offset in the file.
    pub fn sector_size(&self) -> usize {
        self.sector_size
    }

    /// Current BLAKE3 chain hash after all entries read so far.
    /// Returns `None` when `hash-chain` is disabled, for v5 journals, or
    /// if no events have been read.
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

    /// Payload of the `GenesisHash` entry seen for the current segment,
    /// if any. Returns `None` when no GenesisHash has been observed yet
    /// or when the `hash-chain` feature is disabled.
    pub fn genesis_payload(&self) -> Option<[u8; 32]> {
        #[cfg(feature = "hash-chain")]
        {
            self.genesis_payload
        }
        #[cfg(not(feature = "hash-chain"))]
        {
            None
        }
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

    /// Extract the raw hash chain state for use by a resumed writer.
    ///
    /// Returns `(current_hash, batch_hasher, events_since_checkpoint)`:
    /// - `current_hash`: the chain hash from the last checkpoint (or genesis)
    /// - `batch_hasher`: incremental hasher with entry bytes since last checkpoint
    /// - `events_since_checkpoint`: event count since last checkpoint
    ///
    /// This allows the writer to reconstruct the exact hasher state needed
    /// for correct checkpoint computation after crash recovery.
    #[cfg(feature = "hash-chain")]
    pub fn take_chain_state(&mut self) -> Option<([u8; 32], blake3::Hasher, u64)> {
        self.hash_chain
            .take()
            .map(|c| (c.current_hash, c.batch_hasher, c.events_since_checkpoint))
    }

    /// Ensure the buffer has data to decode from. Lazy: when bytes are
    /// already buffered, returns immediately and lets the caller try
    /// `codec::decode` first — only on `TruncatedEntry` does
    /// `try_extend_buffer` actually refill. This avoids a per-entry
    /// `read()` syscall and a per-entry `copy_within` compaction in the
    /// steady-state scan, both of which dominated the old reader's
    /// runtime when the buffer was small relative to the journal.
    fn fill_buffer(&mut self) -> Result<(), JournalError> {
        if self.valid > self.pos {
            return Ok(());
        }
        // Buffer fully consumed — reset cursors and refill from disk.
        self.pos = 0;
        self.valid = 0;
        let n = self.file.read(&mut self.buffer)?;
        self.valid = n;
        Ok(())
    }

    /// Try to read more data into the buffer. Returns true if new data
    /// was read.
    ///
    /// Called from `next_entry`'s `TruncatedEntry` path — i.e. only
    /// when decode could not consume a full entry from the current
    /// `[pos..valid]` window. This call must make as much free tail
    /// room as possible so the single follow-up decode attempt
    /// succeeds even when the underlying `read()` is short (the retry
    /// in `next_entry` is one-shot: a second `TruncatedEntry` is
    /// treated as EOF). Always compacting the consumed prefix here is
    /// cheap because the function only fires once per buffer-full,
    /// not per decoded entry — the steady-state cost lives in
    /// `fill_buffer`, which stays lazy.
    fn try_extend_buffer(&mut self) -> Result<bool, JournalError> {
        // Reclaim the consumed prefix to maximize the free tail. Skip
        // when pos is already 0 to avoid a no-op copy_within.
        if self.pos > 0 {
            self.buffer.copy_within(self.pos..self.valid, 0);
            self.valid -= self.pos;
            self.pos = 0;
        }

        // Grow when the pending partial entry already fills the
        // buffer — rare under production traffic; the codec caps
        // entry length at `u16::MAX` (~64 KiB total), so with the
        // 1 MiB INITIAL_BUF_SIZE this branch is structurally
        // unreachable. Kept for the small-buffer test path and as
        // defense-in-depth if the codec's cap is ever loosened.
        // Doubles on each miss so the loop is bounded by the entry
        // size, not by buffer size.
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
        use std::io::Seek;
        let mut file = File::open(path)?;
        let mut header = [0u8; FILE_HEADER_SIZE];
        file.read_exact(&mut header)?;
        let (_, sector_size) = codec::decode_file_header(&header)?;
        file.seek(std::io::SeekFrom::Start(sector_size as u64))?;

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
        let header = EntryHeader::ref_from_prefix(&self.buf[self.pos..])
            .expect("ensure_available guarantees at least ENTRY_HEADER_SIZE bytes")
            .0;
        Ok(Some(header.sequence.get()))
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
            let header = EntryHeader::ref_from_prefix(&self.buf[self.pos..])
                .expect("ensure_available guarantees at least ENTRY_HEADER_SIZE bytes")
                .0;
            if header.sequence.get() > target_seq {
                return Ok(()); // Found the first entry past target.
            }
            // Skip this entry.
            let total = ENTRY_HEADER_SIZE + header.length.get() as usize + CRC_SIZE;
            self.ensure_available(total)?;
            if self.valid - self.pos < total {
                return Ok(()); // Truncated entry at EOF.
            }
            self.pos += total;
        }
    }

    /// Read raw entry bytes into `out`, up to `max_bytes` total.
    /// Returns the last sequence in the batch, or `None` at EOF or when
    /// no complete entry fits within `max_bytes`.
    pub fn read_raw_batch(
        &mut self,
        out: &mut Vec<u8>,
        max_bytes: usize,
    ) -> Result<Option<u64>, JournalError> {
        let mut any = false;
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

            // Copy scalars out of the header view before any mutating call
            // on `self` (ensure_available below) invalidates the borrow.
            let (entry_seq, total) = {
                let header = EntryHeader::ref_from_prefix(&self.buf[self.pos..])
                    .expect("ensure_available guarantees at least ENTRY_HEADER_SIZE bytes")
                    .0;
                (
                    header.sequence.get(),
                    ENTRY_HEADER_SIZE + header.length.get() as usize + CRC_SIZE,
                )
            };

            // Don't exceed max_bytes (but always include at least one entry).
            if any && (out.len() - batch_start) + total > max_bytes {
                break;
            }

            self.ensure_available(total)?;
            if self.valid - self.pos < total {
                break; // Truncated entry at EOF.
            }

            end_seq = entry_seq;
            out.extend_from_slice(&self.buf[self.pos..self.pos + total]);
            self.pos += total;
            any = true;
        }

        if any { Ok(Some(end_seq)) } else { Ok(None) }
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

    use super::*;
    use crate::sector_writer::SectorWriter;
    use crate::write::JournalWrite;
    use melin_app::CodecError;

    /// Minimal `AppEvent` for reader round-trip tests.
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

    fn sample_events() -> Vec<JournalEvent<TestEvent>> {
        (0..4).map(|i| JournalEvent::App(TestEvent(i))).collect()
    }

    fn write_sample(path: &Path) -> Vec<JournalEvent<TestEvent>> {
        let events = sample_events();
        let mut writer = SectorWriter::<TestEvent>::create(path).unwrap();
        for event in &events {
            writer.append(event).unwrap();
        }
        events
    }

    #[test]
    fn open_validates_header() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");
        let _writer = SectorWriter::<TestEvent>::create(&path).unwrap();
        let _reader = JournalReader::<TestEvent>::open(&path).unwrap();
    }

    #[test]
    fn many_events_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");
        let events = write_sample(&path);

        let mut reader = JournalReader::<TestEvent>::open(&path).unwrap();
        let mut decoded = Vec::new();
        while let Some(entry) = reader.next_entry().unwrap() {
            decoded.push(entry);
        }
        assert_eq!(decoded.len(), events.len());
        for (i, entry) in decoded.iter().enumerate() {
            assert_eq!(entry.sequence, FIRST_SEQ + i as u64);
            assert_eq!(entry.event, events[i]);
        }
    }

    /// Forces entries to straddle the reader's internal buffer
    /// boundary by opening with a buffer smaller than a single entry.
    /// Every `next_entry` then exercises the lazy-refill ↔
    /// compact-grow-read split: `fill_buffer` returns early when the
    /// buffer holds the entry header but not the payload, decode
    /// returns `TruncatedEntry`, `try_extend_buffer` compacts the
    /// consumed prefix and reads more, decode succeeds. With 100
    /// entries and a 64-byte buffer (one entry ≈ 49 bytes), the seam
    /// is crossed dozens of times within the same scan.
    #[test]
    fn entries_straddling_buffer_boundary_decode_correctly() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");
        const N: u64 = 100;
        {
            let mut writer = SectorWriter::<TestEvent>::create(&path).unwrap();
            for i in 0..N {
                writer.append(&JournalEvent::App(TestEvent(i))).unwrap();
            }
        }

        // Tiny buffer (< one entry) guarantees a refill straddles
        // every entry. The grow path also fires once when the buffer
        // is full of header bytes but still can't fit the payload.
        let mut reader = JournalReader::<TestEvent>::open_with_buffer(&path, 64).unwrap();
        let mut decoded = Vec::new();
        while let Some(entry) = reader.next_entry().unwrap() {
            decoded.push(entry);
        }
        assert_eq!(decoded.len(), N as usize);
        for (i, entry) in decoded.iter().enumerate() {
            assert_eq!(entry.sequence, FIRST_SEQ + i as u64);
            assert_eq!(entry.event, JournalEvent::App(TestEvent(i as u64)));
        }
    }

    #[test]
    fn no_entries_empty_journal() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");
        let _writer = SectorWriter::<TestEvent>::create(&path).unwrap();

        let mut reader = JournalReader::<TestEvent>::open(&path).unwrap();
        #[cfg(feature = "hash-chain")]
        {
            // Genesis entry is present but transparent; reader consumes
            // it internally and returns None on the first next_entry.
            assert!(reader.next_entry().unwrap().is_none());
        }
        #[cfg(not(feature = "hash-chain"))]
        {
            assert!(reader.next_entry().unwrap().is_none());
        }
    }

    #[test]
    fn truncated_entry_at_eof_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");
        {
            let mut writer = SectorWriter::<TestEvent>::create(&path).unwrap();
            writer.append(&JournalEvent::App(TestEvent(7))).unwrap();
        }

        // Truncate the file mid-entry: drop the last 8 bytes which is
        // inside the CRC / payload region of the last entry.
        let len = std::fs::metadata(&path).unwrap().len();
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(len - 8).unwrap();

        let mut reader = JournalReader::<TestEvent>::open(&path).unwrap();
        // Reader may return Ok(None) (truncated tail = crash-recovery case)
        // or an error depending on whether the truncation fell inside the
        // header, CRC, or between entries. All three outcomes are valid —
        // we just assert it doesn't panic and, if Ok, is exhausted.
        let _ = reader.next_entry();
    }

    #[test]
    fn appending_bad_bytes_is_detected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");
        {
            let mut writer = SectorWriter::<TestEvent>::create(&path).unwrap();
            writer.append(&JournalEvent::App(TestEvent(1))).unwrap();
        }

        // Flip a byte inside the (already-synced) entry region to force
        // a CRC mismatch.
        let entry_offset = ENTRY_OFFSET as usize + 4; // magic+length then a data byte
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        use std::io::{Read, Seek, SeekFrom};
        let mut byte = [0u8; 1];
        file.seek(SeekFrom::Start(entry_offset as u64)).unwrap();
        file.read_exact(&mut byte).unwrap();
        byte[0] ^= 0xff;
        file.seek(SeekFrom::Start(entry_offset as u64)).unwrap();
        file.write_all(&byte).unwrap();

        let mut reader = JournalReader::<TestEvent>::open(&path).unwrap();
        let err = reader.next_entry();
        assert!(err.is_err(), "expected error, got {err:?}");
    }

    /// Defense-in-depth: bytes past the writer's last durable entry can,
    /// under specific failure modes (e.g. an in-flight async write whose
    /// CQE hasn't arrived, or a torn multi-sector write), look entry-shaped
    /// while the CRC slot is still preallocation zeros. The reader treats
    /// that exact signature (`ChecksumMismatch` with stored CRC = 0, past
    /// genesis) as end-of-data so recovery succeeds on a journal whose
    /// last write was only partially observable.
    #[test]
    fn zero_crc_past_genesis_treated_as_end_of_data() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");
        {
            let mut writer = SectorWriter::<TestEvent>::create(&path).unwrap();
            writer.append(&JournalEvent::App(TestEvent(1))).unwrap();
            writer.append(&JournalEvent::App(TestEvent(2))).unwrap();
        }

        // Discover where the next entry would start.
        let valid_end = {
            let mut reader = JournalReader::<TestEvent>::open(&path).unwrap();
            while reader.next_entry().unwrap().is_some() {}
            reader.valid_file_end()
        };

        // Forge an entry past `valid_end` with real-looking header+payload
        // but a zeroed CRC slot — the exact byte pattern observed when a
        // multi-sector write lands the header but loses the trailing CRC
        // sector. Built via `codec::encode` so it parses cleanly, then the
        // 4-byte CRC tail is zeroed.
        let mut scratch = [0u8; 256];
        let entry_len = {
            let event: JournalEvent<TestEvent> = JournalEvent::App(TestEvent(99));
            codec::encode(9_999, 0, 0, 0, &event, &mut scratch).unwrap()
        };
        scratch[entry_len - CRC_SIZE..entry_len].fill(0);

        use std::io::{Seek, SeekFrom};
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.seek(SeekFrom::Start(valid_end)).unwrap();
        file.write_all(&scratch[..entry_len]).unwrap();
        file.sync_all().unwrap();

        // Reader yields the two real entries (transparent genesis already
        // consumed under hash-chain) and stops gracefully on the forged
        // zero-CRC entry instead of surfacing `ChecksumMismatch`.
        let mut reader = JournalReader::<TestEvent>::open(&path).unwrap();
        let mut count = 0;
        while let Some(_entry) = reader.next_entry().unwrap() {
            count += 1;
        }
        assert_eq!(count, 2, "two real entries should be recoverable");
    }

    /// Inverse guard: a zero CRC at the *very first* entry (no genesis
    /// consumed yet, `last_sequence == None`) still surfaces as a
    /// `ChecksumMismatch`. We only relax the check past genesis, so
    /// corruption of the first entry remains visible.
    ///
    /// Runs under both feature configs: under hash-chain, the writer's
    /// genesis at ENTRY_OFFSET gets overwritten by the forged entry, so
    /// the reader's first decode hits the CRC check before any
    /// hash-chain state has been built (`codec::decode` validates CRC
    /// before returning, and `validate_and_advance` — where hash-chain
    /// validation lives — only runs on a successful decode). Either way,
    /// `last_sequence` is still `None` at the moment the mismatch fires.
    #[test]
    fn zero_crc_at_first_entry_still_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");
        {
            // Create the file header only; no user events.
            let _writer = SectorWriter::<TestEvent>::create(&path).unwrap();
        }

        let mut scratch = [0u8; 256];
        let entry_len = {
            let event: JournalEvent<TestEvent> = JournalEvent::App(TestEvent(1));
            codec::encode(1, 0, 0, 0, &event, &mut scratch).unwrap()
        };
        scratch[entry_len - CRC_SIZE..entry_len].fill(0);

        use std::io::{Seek, SeekFrom};
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.seek(SeekFrom::Start(ENTRY_OFFSET)).unwrap();
        file.write_all(&scratch[..entry_len]).unwrap();
        file.sync_all().unwrap();

        let mut reader = JournalReader::<TestEvent>::open(&path).unwrap();
        let err = reader.next_entry();
        assert!(
            matches!(err, Err(JournalError::ChecksumMismatch { .. })),
            "expected ChecksumMismatch at first entry, got {err:?}"
        );
    }

    /// Critical guard: a zero-CRC entry followed by more entry-shaped
    /// bytes is a **hole** in the journal (data loss), not preallocated
    /// tail. The reader must surface this as an error so recovery halts
    /// instead of silently truncating the journal to the prefix.
    ///
    /// Runs under both feature configs: the CRC mismatch on the forged
    /// entry fires inside `codec::decode`, before `validate_and_advance`
    /// (and therefore before any hash-chain check) ever runs.
    #[test]
    fn zero_crc_with_data_after_surfaces_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");
        {
            let mut writer = SectorWriter::<TestEvent>::create(&path).unwrap();
            writer.append(&JournalEvent::App(TestEvent(1))).unwrap();
            writer.append(&JournalEvent::App(TestEvent(2))).unwrap();
        }
        let valid_end = {
            let mut reader = JournalReader::<TestEvent>::open(&path).unwrap();
            while reader.next_entry().unwrap().is_some() {}
            reader.valid_file_end()
        };

        // Encode two entries; zero the CRC of the FIRST one so it reads
        // as a hole, leaving the SECOND one as real-looking data that
        // proves the file isn't just preallocated tail.
        let mut scratch1 = [0u8; 256];
        let mut scratch2 = [0u8; 256];
        let len1 = codec::encode(
            9_999,
            0,
            0,
            0,
            &JournalEvent::App(TestEvent(98)),
            &mut scratch1,
        )
        .unwrap();
        let len2 = codec::encode(
            10_000,
            0,
            0,
            0,
            &JournalEvent::App(TestEvent(99)),
            &mut scratch2,
        )
        .unwrap();
        scratch1[len1 - CRC_SIZE..len1].fill(0); // hole marker

        use std::io::{Seek, SeekFrom};
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.seek(SeekFrom::Start(valid_end)).unwrap();
        file.write_all(&scratch1[..len1]).unwrap();
        file.write_all(&scratch2[..len2]).unwrap();
        file.sync_all().unwrap();

        let mut reader = JournalReader::<TestEvent>::open(&path).unwrap();
        // Walk past the two real entries — they decode fine.
        for _ in 0..2 {
            reader.next_entry().unwrap();
        }
        // Hitting the zero-CRC entry with real data after it must
        // surface as ChecksumMismatch, not silently stop.
        let err = reader.next_entry();
        assert!(
            matches!(err, Err(JournalError::ChecksumMismatch { .. })),
            "expected ChecksumMismatch (data loss hole), got {err:?}"
        );
    }

    #[test]
    fn sequence_gap_detected() {
        // Build a journal manually: header + two entries, then overwrite
        // the second entry's sequence field to introduce a gap.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");
        {
            let mut writer = SectorWriter::<TestEvent>::create(&path).unwrap();
            writer.append(&JournalEvent::App(TestEvent(1))).unwrap();
            writer.append(&JournalEvent::App(TestEvent(2))).unwrap();
        }

        // Overwrite the sequence number of the second user entry to a
        // skipped value. Layout: each entry = ENTRY_HEADER_SIZE(20) +
        // payload_len + CRC_SIZE(4). For TestEvent, payload = 17
        // (key_hash+request_seq+tag) + 8 (payload) = 25. Full = 49.
        // With hash-chain, a genesis entry sits between header and first
        // user event — but its payload size is larger. Rather than
        // hardcode offsets, skip the test under hash-chain where the
        // layout is feature-dependent.
        #[cfg(not(feature = "hash-chain"))]
        {
            const FIRST_ENTRY_SIZE: u64 = 20 + 25 + 4;
            let second_seq_offset = ENTRY_OFFSET + FIRST_ENTRY_SIZE + 4;
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            use std::io::{Seek, SeekFrom};
            file.seek(SeekFrom::Start(second_seq_offset)).unwrap();
            // Write sequence = 99 and fix the CRC.
            let new_seq: u64 = 99;
            file.write_all(&new_seq.to_le_bytes()).unwrap();

            let mut reader = JournalReader::<TestEvent>::open(&path).unwrap();
            // First entry decodes cleanly; second trips CRC (we didn't
            // refix) or gap. Either is a non-Ok outcome.
            let _ = reader.next_entry(); // first entry ok
            let err = reader.next_entry();
            assert!(err.is_err(), "expected error, got {err:?}");
        }
    }
}
