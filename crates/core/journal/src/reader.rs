//! Journal reader — sequential read with CRC and sequence validation.
//!
//! Reads entries one at a time, and decides where a segment's entries
//! end. That decision is the journal's whole recovery rule, so it lives
//! here, in one place:
//!
//! - **Whole entries are never a torn write.** An entry whose CRC
//!   verifies was written in full by the writer, so anything wrong with
//!   it — a sequence gap or duplicate, a first entry off the header's
//!   `starting_sequence`, an event the codec refuses — is corruption, as
//!   is a `length` wider than any entry of its type (a torn write leaves
//!   bytes as written or as zeros, so it can only shrink a length).
//! - **A malformed entry** (zero or bad magic, a CRC mismatch, an entry
//!   running past EOF) **ends the entries** — if what follows allows it.
//!   In the [`SegmentKind::Live`] segment, a crash can leave at most one
//!   unsynced drain ([`crate::write_ring::MAX_UNSYNCED_BYTES`]) half
//!   written, in any pattern of written and unwritten sectors, after the
//!   last synced byte. So the stop is a torn, never-acknowledged tail when
//!   every non-zero byte from the stopping entry's start lies within that
//!   span, and only zeros follow to the end of the file. Anything else is
//!   [`JournalError::UnrecoverableTail`]. An [`SegmentKind::Archived`]
//!   segment was synced in full before it was archived, so nothing but
//!   zeros (allocation padding a compaction did not trim) may follow its
//!   last entry.
//!
//! Recovery that accepts a torn tail logs it (`warn!`) with its extent.

use std::fs::File;
use std::io::Read;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use melin_app::AppEvent;

use zerocopy::FromBytes;

#[cfg(test)]
use super::codec::ENTRY_OFFSET;
use super::codec::{self, CRC_SIZE, ENTRY_HEADER_SIZE, ENTRY_MAGIC, EntryHeader, FILE_HEADER_SIZE};
use super::error::JournalError;
use super::event::JournalEvent;
use crate::write_ring::MAX_UNSYNCED_BYTES;

/// Which end-of-entries rule a [`JournalReader`] applies (see the module
/// docs).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentKind {
    /// The segment being appended to: it may end in one torn,
    /// never-synced write.
    Live,
    /// An archived segment, synced in full before it was archived: its
    /// entries may be followed by zeros only.
    Archived,
}

/// A torn write the reader stopped at and recovery discards: the bytes
/// from `offset` (where the last whole entry ends) through the last
/// non-zero byte, `len` bytes in all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TornTail {
    pub offset: u64,
    pub len: u64,
}

/// Width of the zero runs the end-of-entries scan compares against.
/// Comparing `u8` slices is a `memcmp`, which keeps the scan of a
/// preallocated tail at memory speed even in unoptimised builds; a
/// per-byte loop does not. 64 KiB: a `static` of zeros, so it costs
/// address space, not memory.
const ZERO_RUN: usize = 64 * 1024;
static ZEROS: [u8; ZERO_RUN] = [0; ZERO_RUN];

/// Offset of the first non-zero byte of `bytes`.
fn first_nonzero(bytes: &[u8]) -> Option<usize> {
    let mut base = 0;
    for run in bytes.chunks(ZERO_RUN) {
        if run != &ZEROS[..run.len()] {
            return run.iter().position(|b| *b != 0).map(|i| base + i);
        }
        base += run.len();
    }
    None
}

/// Offset of the last non-zero byte of `bytes`.
fn last_nonzero(bytes: &[u8]) -> Option<usize> {
    let mut end = bytes.len();
    for run in bytes.rchunks(ZERO_RUN) {
        let base = end - run.len();
        if run != &ZEROS[..run.len()] {
            return run.iter().rposition(|b| *b != 0).map(|i| base + i);
        }
        end = base;
    }
    None
}

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

/// Tell the kernel this handle will be read start-to-end, so readahead
/// runs ahead of the reader instead of being rebuilt per `read()`.
///
/// Every reader in this module is a single forward scan, and the scan is
/// what sets restart and failover time. `fill_buffer` issues one blocking
/// `read()` at a time, so replay throughput is bounded by how much the
/// kernel keeps in flight, not by the buffer size: with the default
/// 128 KiB readahead window a 1 MiB refill is several dependent device
/// round trips. `POSIX_FADV_SEQUENTIAL` doubles that window. The
/// chain rebuild in `chain.rs` is the same shape of scan (positional
/// reads share the handle's readahead state) and uses this too.
///
/// The gap this closes is small on a local NVMe, where a round trip is
/// tens of microseconds, and large on network-attached storage (EBS and
/// friends), where it is closer to a millisecond and readahead depth is
/// the whole story.
///
/// Best-effort: the hint is an optimisation with no correctness content,
/// and a kernel that rejects it (or a filesystem that ignores it) must
/// not turn a recoverable journal into a failed startup. Hence the
/// discarded `Result`.
pub(crate) fn advise_sequential(file: &File) {
    // Best-effort hint; a failure changes nothing the caller can act on.
    let _ = rustix::fs::fadvise(file, 0, None, rustix::fs::Advice::Sequential);
}

/// A decoded journal entry with its metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalEntry<E: AppEvent> {
    /// Monotonically increasing sequence number (starts at 1).
    pub sequence: u64,
    /// Wall-clock nanos since epoch at write time (informational, not for ordering).
    pub timestamp_ns: u64,
    /// Hash of the client's Ed25519 public key. Zero for internal/seed events.
    pub key_hash: u64,
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
    /// Which end-of-entries rule applies.
    kind: SegmentKind,
    /// The segment's path, for [`JournalError::UnrecoverableTail`].
    path: PathBuf,
    /// Set once the entries have ended (cleanly or at a torn tail), so
    /// later calls return `Ok(None)` without scanning the file again.
    ended: bool,
    /// The torn write the entries ended at, if they ended at one.
    torn_tail: Option<TornTail>,
    /// Byte offset where entries begin (one header reservation). Decoded
    /// from the file header at open time.
    sector_size: usize,
    /// Sequence number the segment's first entry must carry, from the
    /// file header. Validated against the first decoded entry so a
    /// segment spliced in from elsewhere in the lineage fails fast.
    starting_sequence: u64,
    /// The lineage's genesis length from the file header (see
    /// [`codec::FileHeaderInfo::genesis_entries`]).
    genesis_entries: Option<u64>,
    /// Segment hash chain, seeded from the header anchor and fed every
    /// entry's raw bytes. Same definition as the writers' (see
    /// [`crate::chain`]), so reader and writer values agree at every
    /// sequence.
    #[cfg(feature = "hash-chain")]
    chain: crate::chain::SegmentChain,
}

impl<E: AppEvent> JournalReader<E> {
    /// Open a live segment for reading (see [`SegmentKind::Live`]).
    /// Validates the file header.
    pub fn open(path: &Path) -> Result<Self, JournalError> {
        Self::open_segment(path, SegmentKind::Live)
    }

    /// Open an archived segment for reading (see
    /// [`SegmentKind::Archived`]). Validates the file header.
    pub fn open_archived(path: &Path) -> Result<Self, JournalError> {
        Self::open_segment(path, SegmentKind::Archived)
    }

    /// Open a segment of either kind for reading. Validates the file
    /// header.
    pub fn open_segment(path: &Path, kind: SegmentKind) -> Result<Self, JournalError> {
        use std::io::Seek;
        let mut file = File::open(path)?;
        advise_sequential(&file);

        // Read and validate the file header. We use pread so the file cursor
        // stays at zero; we then seek to sector_size to position the reader
        // at the first entry. FILE_HEADER_SIZE bytes is always enough to
        // decode all header fields (the meaningful content is 8 bytes).
        let mut header = [0u8; FILE_HEADER_SIZE];
        file.read_exact(&mut header)?;
        let info = codec::decode_file_header(&header)?;

        // Skip any padding between FILE_HEADER_SIZE and sector_size (zero on
        // 512-byte devices; up to 3.5 KiB on 4Kn devices). Entries start at
        // exactly one sector offset.
        file.seek(std::io::SeekFrom::Start(info.sector_size as u64))?;

        Ok(Self {
            _marker: PhantomData,
            file,
            buffer: vec![0u8; INITIAL_BUF_SIZE],
            pos: 0,
            valid: 0,
            last_sequence: None,
            valid_file_end: info.sector_size as u64,
            kind,
            path: path.to_path_buf(),
            ended: false,
            torn_tail: None,
            sector_size: info.sector_size,
            starting_sequence: info.starting_sequence,
            genesis_entries: info.genesis_entries,
            #[cfg(feature = "hash-chain")]
            chain: crate::chain::SegmentChain::new(info.anchor_hash),
        })
    }

    /// Read the next journal entry.
    ///
    /// Returns `Ok(Some(entry))` for each valid entry, and `Ok(None)` once
    /// the entries end: at EOF, at zeros, or at a torn write the module's
    /// rule accepts (see [`torn_tail`](Self::torn_tail)). Returns `Err` on
    /// corruption — a whole entry that is wrong (sequence gap or
    /// duplicate, an undecodable event, an over-long length) or a stop
    /// the rule does not accept ([`JournalError::UnrecoverableTail`]).
    pub fn next_entry(&mut self) -> Result<Option<JournalEntry<E>>, JournalError> {
        if self.ended {
            return Ok(None);
        }
        // Ensure we have data to work with.
        self.fill_buffer()?;
        if self.valid == self.pos {
            // EOF exactly at an entry boundary.
            return self.end_of_entries(None);
        }

        // Zero magic: no entry starts here (the magic is never zero).
        // Whether that is the end is decided from what follows.
        if self.zero_magic() {
            return self.end_of_entries(None);
        }

        let mut decoded = codec::decode(&self.buffer[self.pos..self.valid]);
        // A partial entry in the buffer: read more and decode again, until
        // the entry is whole or the file has nothing more to give. Looping
        // (rather than retrying once) keeps a short `read()` — legal on
        // any file, seen on FUSE/NFS — from passing a whole mid-file
        // entry to the torn-tail rule as one running past EOF. Each pass
        // reads at least one byte, so the loop ends at EOF.
        while matches!(decoded, Err(JournalError::TruncatedEntry)) && self.try_extend_buffer()? {
            if self.zero_magic() {
                return self.end_of_entries(None);
            }
            decoded = codec::decode(&self.buffer[self.pos..self.valid]);
        }

        match decoded {
            Ok((consumed, sequence, timestamp_ns, key_hash, event)) => {
                self.validate_and_advance(consumed, sequence, timestamp_ns, key_hash, event)
            }
            Err(e) if self.is_torn_shape(&e) => self.end_of_entries(Some(e)),
            Err(e) => Err(e),
        }
    }

    /// Whether the buffered bytes at the read position begin with two
    /// zero bytes where an entry's magic would be.
    fn zero_magic(&self) -> bool {
        self.valid - self.pos >= 2 && self.buffer[self.pos] == 0 && self.buffer[self.pos + 1] == 0
    }

    /// Whether a decode failure is one a torn write can produce: the
    /// entry runs past EOF, its CRC does not match, or its magic is
    /// wrong. All three are decided before the entry is trusted. Every
    /// other failure is of a check that runs on bytes a torn write cannot
    /// produce — a length wider than any entry of its type (checked once
    /// the magic is right), or anything checked after the CRC verified.
    fn is_torn_shape(&self, err: &JournalError) -> bool {
        match err {
            JournalError::TruncatedEntry | JournalError::ChecksumMismatch { .. } => true,
            // `decode` checks the magic first: with the right magic, a
            // `CorruptEntry` is the length cap or a post-CRC check.
            JournalError::CorruptEntry { .. } => {
                let bytes = &self.buffer[self.pos..self.valid];
                bytes.len() < 2 || u16::from_le_bytes([bytes[0], bytes[1]]) != ENTRY_MAGIC
            }
            _ => false,
        }
    }

    /// The entries have stopped at `valid_file_end` — on zeros or EOF
    /// (`cause` `None`), or on a malformed entry (`cause`). Decide from
    /// the rest of the file whether that is the end of the segment's
    /// data (see the module docs) or corruption.
    fn end_of_entries(
        &mut self,
        cause: Option<JournalError>,
    ) -> Result<Option<JournalEntry<E>>, JournalError> {
        use std::io::Seek;
        let offset = self.valid_file_end;
        // How far from the stop a byte may be written and not synced.
        let reach = match self.kind {
            SegmentKind::Live => offset.saturating_add(MAX_UNSYNCED_BYTES),
            SegmentKind::Archived => offset,
        };
        let scan = self.scan_tail(offset, reach);
        // The scan reused the read buffer. Drop what it held and put the
        // cursor back at the stop, so that a call after an error reads
        // the same bytes and reaches the same verdict, never a clean end.
        self.pos = 0;
        self.valid = 0;
        self.file.seek(std::io::SeekFrom::Start(offset))?;
        let (last_within, beyond) = scan?;
        if let Some(nonzero_at) = beyond {
            return Err(JournalError::UnrecoverableTail {
                path: self.path.clone(),
                offset,
                last_sequence: self.last_sequence,
                nonzero_at,
                cause: cause.map(Box::new),
            });
        }
        self.ended = true;
        if let Some(last) = last_within {
            let torn = TornTail {
                offset,
                len: last + 1 - offset,
            };
            // `warn`: handled, but the operator should know a write was
            // torn — and an entry whose valid CRC happens to be zero (one
            // in 2^32) would also end up here.
            tracing::warn!(
                path = %self.path.display(),
                offset,
                discarded_bytes = torn.len,
                last_sequence = ?self.last_sequence,
                cause = %cause.as_ref().map_or_else(|| "zeros".to_string(), |e| e.to_string()),
                "journal segment ends in a torn write that was never acknowledged; \
                 recovery discards it"
            );
            self.torn_tail = Some(torn);
        }
        Ok(None)
    }

    /// Scan the file from `from` to EOF. Returns the offset of the last
    /// non-zero byte before `reach`, and of the first one at or after it
    /// (the scan stops there). Positioned reads (`pread`), so the scan
    /// does not move the file cursor; it reuses the read buffer, which
    /// nothing reads once the entries have ended.
    fn scan_tail(
        &mut self,
        from: u64,
        reach: u64,
    ) -> Result<(Option<u64>, Option<u64>), JournalError> {
        use std::os::unix::fs::FileExt;
        let file_end = self.file.metadata()?.len();
        let mut last_within = None;
        let mut offset = from;
        while offset < file_end {
            let want = (file_end - offset).min(self.buffer.len() as u64) as usize;
            let n = match self.file.read_at(&mut self.buffer[..want], offset) {
                Ok(0) => break,
                Ok(n) => n,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e.into()),
            };
            let split = reach.saturating_sub(offset).min(n as u64) as usize;
            let (within, beyond) = self.buffer[..n].split_at(split);
            if let Some(i) = last_nonzero(within) {
                last_within = Some(offset + i as u64);
            }
            if let Some(i) = first_nonzero(beyond) {
                return Ok((last_within, Some(offset + (split + i) as u64)));
            }
            offset += n as u64;
        }
        Ok((last_within, None))
    }

    /// Validate sequence continuity, update the hash chain, and advance
    /// the read position. Every decoded entry is surfaced to the caller —
    /// the entry stream contains no reader-internal control entries.
    fn validate_and_advance(
        &mut self,
        consumed: usize,
        sequence: u64,
        timestamp_ns: u64,
        key_hash: u64,
        event: JournalEvent<E>,
    ) -> Result<Option<JournalEntry<E>>, JournalError> {
        // The first entry must carry the header's starting_sequence —
        // catches a segment spliced in from elsewhere in the lineage.
        // Subsequent entries enforce strict continuity.
        match self.last_sequence {
            None => {
                if sequence != self.starting_sequence {
                    return Err(JournalError::SequenceGap {
                        expected: self.starting_sequence,
                        actual: sequence,
                    });
                }
            }
            Some(last) => {
                let expected = last + 1;
                // Split below-vs-above so operators can tell "data
                // missing" from "writer emitted the same seq twice".
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
        }

        // Absorb the entry's raw on-disk bytes (header + payload + CRC)
        // into the segment chain. Verification happens at the consumers'
        // compare points: snapshot anchor, segment boundary, divergence
        // frames — the entry stream itself carries no chain metadata.
        #[cfg(feature = "hash-chain")]
        self.chain
            .absorb(&self.buffer[self.pos..self.pos + consumed]);

        self.last_sequence = Some(sequence);
        self.pos += consumed;
        self.valid_file_end += consumed as u64;

        Ok(Some(JournalEntry {
            sequence,
            timestamp_ns,
            key_hash,
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

    /// The torn write the entries ended at, once
    /// [`next_entry`](Self::next_entry) has returned `Ok(None)` on one;
    /// `None` when they ended cleanly, or have not ended yet. Only a live
    /// segment can end in one.
    pub fn torn_tail(&self) -> Option<TornTail> {
        self.torn_tail
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

    /// Current chain value after all entries read so far:
    /// `BLAKE3(entry bytes so far || anchor)`, or the anchor itself when
    /// no entries have been read. `None` when `hash-chain` is disabled.
    ///
    /// Computed on demand by cloning the incremental hasher —
    /// non-destructive and O(log absorbed bytes).
    pub fn chain_hash(&self) -> Option<[u8; 32]> {
        #[cfg(feature = "hash-chain")]
        {
            Some(self.chain.value())
        }
        #[cfg(not(feature = "hash-chain"))]
        None
    }

    /// Segment anchor from the file header. `None` when `hash-chain` is
    /// disabled. Multi-segment recovery compares this against the
    /// previous segment's tail chain hash to verify lineage continuity.
    pub fn anchor(&self) -> Option<[u8; 32]> {
        #[cfg(feature = "hash-chain")]
        {
            Some(self.chain.anchor())
        }
        #[cfg(not(feature = "hash-chain"))]
        None
    }

    /// Sequence number the segment's first entry carries, from the file
    /// header. Available before any entry is read.
    pub fn starting_sequence(&self) -> u64 {
        self.starting_sequence
    }

    /// The lineage's genesis length, from the file header: `Some(n)`
    /// when the genesis is sequences `1..=n`, `None` when unknown (a v15
    /// header). See [`codec::FileHeaderInfo::genesis_entries`].
    pub fn genesis_entries(&self) -> Option<u64> {
        self.genesis_entries
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
    /// `[pos..valid]` window. `next_entry` calls it until the entry
    /// decodes or this returns false (EOF), so a short `read()` costs
    /// another pass rather than a misread end of data. Compacting
    /// first makes as much free tail room as possible, so one pass
    /// normally suffices. Always compacting the consumed prefix here is
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
        // Also a pure forward scan, and on a smaller buffer than
        // `JournalReader`'s, so it depends on readahead even more.
        advise_sequential(&file);
        let mut header = [0u8; FILE_HEADER_SIZE];
        file.read_exact(&mut header)?;
        let info = codec::decode_file_header(&header)?;
        file.seek(std::io::SeekFrom::Start(info.sector_size as u64))?;

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

    /// Like [`Self::read_raw_batch`] but never reads past
    /// `stop_after_seq`: an entry with a higher sequence is left
    /// unconsumed for the next call. Used by chain validation, which
    /// must absorb the raw bytes of exactly the entries up to a target
    /// sequence.
    pub fn read_raw_batch_until(
        &mut self,
        out: &mut Vec<u8>,
        max_bytes: usize,
        stop_after_seq: u64,
    ) -> Result<Option<u64>, JournalError> {
        self.read_raw_batch_inner(out, max_bytes, Some(stop_after_seq))
    }

    /// Read raw entry bytes into `out`, up to `max_bytes` total.
    /// Returns the last sequence in the batch, or `None` at EOF or when
    /// no complete entry fits within `max_bytes`.
    pub fn read_raw_batch(
        &mut self,
        out: &mut Vec<u8>,
        max_bytes: usize,
    ) -> Result<Option<u64>, JournalError> {
        self.read_raw_batch_inner(out, max_bytes, None)
    }

    fn read_raw_batch_inner(
        &mut self,
        out: &mut Vec<u8>,
        max_bytes: usize,
        stop_after_seq: Option<u64>,
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

            // Bounded read: leave entries past the stop sequence
            // unconsumed (next call starts exactly there).
            if let Some(stop) = stop_after_seq
                && entry_seq > stop
            {
                break;
            }

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
    use crate::buffered_writer::BufferedWriter;
    use crate::write::JournalWrite;
    use melin_app::CodecError;

    /// Minimal `AppEvent` for reader round-trip tests.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct TestEvent(u64);

    impl AppEvent for TestEvent {
        const MAX_ENCODED_SIZE: usize = 8;

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

    /// First user-event sequence. Chain metadata lives in the file
    /// header, so sequence 1 is a real event under every feature config.
    const FIRST_SEQ: u64 = 1;

    fn sample_events() -> Vec<JournalEvent<TestEvent>> {
        (0..4).map(|i| JournalEvent::App(TestEvent(i))).collect()
    }

    fn write_sample(path: &Path) -> Vec<JournalEvent<TestEvent>> {
        let events = sample_events();
        let mut writer = BufferedWriter::<TestEvent>::create(path).unwrap();
        for event in &events {
            writer.append(event).unwrap();
        }
        events
    }

    #[test]
    fn open_validates_header() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");
        let _writer = BufferedWriter::<TestEvent>::create(&path).unwrap();
        let _reader = JournalReader::<TestEvent>::open(&path).unwrap();
    }

    /// The documented upgrade path depends on old-format journal *files*
    /// being rejected fail-fast at open — never decoded best-effort
    /// (field layouts differ across versions, so a lenient reader would
    /// produce corrupt state mid-replay). The codec-level version check
    /// is covered in `codec::tests`; this pins the file-level behavior
    /// the operator actually sees. The header CRC is fixed up after the
    /// version patch so the version check is provably the only fault.
    #[test]
    fn open_rejects_old_format_version_file() {
        use std::io::{Read, Seek, SeekFrom};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");
        write_sample(&path);

        // Header layout (see codec.rs): magic u32 | format_version u16 @4
        // | ... The version is checked before the CRC, and v13 has the
        // v15 layout (header_crc u32 @48, CRC over bytes 0..48), so the
        // file is patched into a well-formed v13 header.
        let mut header = [0u8; 52];
        {
            let mut f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            f.read_exact(&mut header).unwrap();
            header[4..6].copy_from_slice(&13u16.to_le_bytes());
            let crc = crc32c::crc32c(&header[..48]);
            header[48..52].copy_from_slice(&crc.to_le_bytes());
            f.seek(SeekFrom::Start(0)).unwrap();
            f.write_all(&header).unwrap();
            // Flush the patched header before reopening it below.
            f.sync_all().unwrap();
        }

        match JournalReader::<TestEvent>::open(&path) {
            Err(JournalError::UnsupportedVersion { version }) => assert_eq!(version, 13),
            Err(other) => panic!("expected UnsupportedVersion for a v13 file, got {other:?}"),
            Ok(_) => panic!("reader opened a v13 file"),
        }
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
    /// entries and a 32-byte buffer (one entry ≈ 41 bytes), the seam
    /// is crossed dozens of times within the same scan.
    #[test]
    fn entries_straddling_buffer_boundary_decode_correctly() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");
        const N: u64 = 100;
        {
            let mut writer = BufferedWriter::<TestEvent>::create(&path).unwrap();
            for i in 0..N {
                writer.append(&JournalEvent::App(TestEvent(i))).unwrap();
            }
        }

        // Tiny buffer (< one entry) guarantees a refill straddles
        // every entry. The grow path also fires once when the buffer
        // is full of header bytes but still can't fit the payload.
        let mut reader = JournalReader::<TestEvent>::open_with_buffer(&path, 32).unwrap();
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
        let _writer = BufferedWriter::<TestEvent>::create(&path).unwrap();

        let mut reader = JournalReader::<TestEvent>::open(&path).unwrap();
        assert!(reader.next_entry().unwrap().is_none());
        // An empty segment's chain value is its anchor.
        #[cfg(feature = "hash-chain")]
        assert_eq!(reader.chain_hash(), reader.anchor());
    }

    #[test]
    fn truncated_entry_at_eof_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");
        {
            let mut writer = BufferedWriter::<TestEvent>::create(&path).unwrap();
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

    /// A flipped byte in an archived segment's entry: archives are synced
    /// whole, so a CRC mismatch there is never a torn write. (In the live
    /// segment the same bytes, with only zeros after them, cannot be told
    /// from one.)
    #[test]
    fn appending_bad_bytes_is_detected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");
        {
            let mut writer = BufferedWriter::<TestEvent>::create(&path).unwrap();
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

        let mut reader = JournalReader::<TestEvent>::open_archived(&path).unwrap();
        let err = reader.next_entry();
        match err {
            Err(JournalError::UnrecoverableTail {
                offset,
                last_sequence: None,
                nonzero_at,
                cause: Some(cause),
                ..
            }) => {
                assert_eq!(offset, ENTRY_OFFSET);
                assert_eq!(nonzero_at, ENTRY_OFFSET);
                assert!(
                    matches!(*cause, JournalError::ChecksumMismatch { .. }),
                    "{cause:?}"
                );
            }
            other => panic!("expected UnrecoverableTail, got {other:?}"),
        }
    }

    /// A repeated sequence number mid-stream surfaces as
    /// `SequenceDuplicate`, distinct from `SequenceGap`, so operators
    /// can tell "writer emitted the same seq twice" from "entries
    /// missing". The writers' debug asserts make this unreachable in
    /// process; the forged entry simulates an external tool or storage
    /// anomaly producing it on disk.
    #[test]
    fn duplicate_sequence_detected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");
        {
            let mut writer = BufferedWriter::<TestEvent>::create(&path).unwrap();
            writer.append(&JournalEvent::App(TestEvent(1))).unwrap();
            writer.append(&JournalEvent::App(TestEvent(2))).unwrap();
        }
        let valid_end = {
            let mut reader = JournalReader::<TestEvent>::open(&path).unwrap();
            while reader.next_entry().unwrap().is_some() {}
            reader.valid_file_end()
        };

        // Forge a fully valid entry (correct CRC) that re-uses seq 2.
        let mut scratch = [0u8; 256];
        let len = codec::encode(2, 0, 0, &JournalEvent::App(TestEvent(99)), &mut scratch).unwrap();
        use std::io::{Seek, SeekFrom};
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.seek(SeekFrom::Start(valid_end)).unwrap();
        file.write_all(&scratch[..len]).unwrap();
        file.sync_all().unwrap();

        let mut reader = JournalReader::<TestEvent>::open(&path).unwrap();
        reader.next_entry().unwrap().expect("first entry");
        reader.next_entry().unwrap().expect("second entry");
        let err = reader.next_entry();
        assert!(
            matches!(
                err,
                Err(JournalError::SequenceDuplicate {
                    sequence: 2,
                    previous_seq: 2
                })
            ),
            "expected SequenceDuplicate, got {err:?}"
        );
    }

    /// A torn multi-sector write can land an entry's header and payload
    /// but not the sector holding its CRC, which then reads as
    /// preallocation zeros. With only zeros after it, the live segment's
    /// entries end there.
    #[test]
    fn zero_crc_past_first_entry_treated_as_end_of_data() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");
        {
            let mut writer = BufferedWriter::<TestEvent>::create(&path).unwrap();
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
            codec::encode(9_999, 0, 0, &event, &mut scratch).unwrap()
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

        // Reader yields the two real entries and stops gracefully on the
        // forged zero-CRC entry instead of surfacing `ChecksumMismatch`.
        let mut reader = JournalReader::<TestEvent>::open(&path).unwrap();
        let mut count = 0;
        while let Some(_entry) = reader.next_entry().unwrap() {
            count += 1;
        }
        assert_eq!(count, 2, "two real entries should be recoverable");
        let torn = reader.torn_tail().expect("the torn entry is reported");
        assert_eq!(torn.offset, valid_end);
        assert!(torn.len > 0 && torn.len <= (entry_len - CRC_SIZE) as u64);
        // Ended: later calls neither rescan nor change their answer.
        assert!(reader.next_entry().unwrap().is_none());
    }

    /// The first write to a fresh segment can be torn like any other:
    /// a zero CRC on the first entry, with only zeros after it, is a
    /// torn tail too.
    #[test]
    fn zero_crc_at_first_entry_is_a_torn_tail() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");
        {
            // Create the file header only; no user events.
            let _writer = BufferedWriter::<TestEvent>::create(&path).unwrap();
        }

        let mut scratch = [0u8; 256];
        let entry_len = {
            let event: JournalEvent<TestEvent> = JournalEvent::App(TestEvent(1));
            codec::encode(1, 0, 0, &event, &mut scratch).unwrap()
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
        assert!(reader.next_entry().unwrap().is_none());
        assert_eq!(reader.last_sequence(), None);
        assert_eq!(reader.valid_file_end(), ENTRY_OFFSET);
        assert_eq!(reader.torn_tail().map(|t| t.offset), Some(ENTRY_OFFSET));
    }

    /// Critical guard: a zero-CRC entry followed by more entry-shaped
    /// bytes further away than one unsynced drain can reach is a
    /// **hole** in synced data, not a torn write. The reader must refuse
    /// so recovery halts instead of silently truncating the journal to
    /// the prefix. (Within that reach, the same bytes are what a drain
    /// whose sectors landed out of order leaves, and read as a torn tail.)
    ///
    /// Runs under both feature configs: the CRC mismatch on the forged
    /// entry fires inside `codec::decode`, before `validate_and_advance`
    /// (and therefore before any hash-chain check) ever runs.
    #[test]
    fn zero_crc_with_data_past_one_drain_surfaces_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");
        {
            let mut writer = BufferedWriter::<TestEvent>::create(&path).unwrap();
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
            &JournalEvent::App(TestEvent(98)),
            &mut scratch1,
        )
        .unwrap();
        let len2 = codec::encode(
            10_000,
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
        file.seek(SeekFrom::Start(valid_end + MAX_UNSYNCED_BYTES))
            .unwrap();
        file.write_all(&scratch2[..len2]).unwrap();
        file.sync_all().unwrap();

        let mut reader = JournalReader::<TestEvent>::open(&path).unwrap();
        // Walk past the two real entries — they decode fine.
        for _ in 0..2 {
            reader.next_entry().unwrap();
        }
        // Hitting the zero-CRC entry with real data past one drain must
        // be refused, not silently stop.
        match reader.next_entry() {
            Err(JournalError::UnrecoverableTail {
                offset,
                last_sequence: Some(2),
                nonzero_at,
                cause: Some(cause),
                ..
            }) => {
                assert_eq!(offset, valid_end);
                assert_eq!(nonzero_at, valid_end + MAX_UNSYNCED_BYTES);
                assert!(
                    matches!(*cause, JournalError::ChecksumMismatch { expected: 0, .. }),
                    "{cause:?}"
                );
            }
            other => panic!("expected UnrecoverableTail (data loss hole), got {other:?}"),
        }
    }

    /// The bound is exact: a non-zero byte at the last offset one drain
    /// can reach is a torn tail, one byte further is corruption.
    #[test]
    fn the_torn_tail_bound_is_one_drain_from_the_stop() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");
        {
            let mut writer = BufferedWriter::<TestEvent>::create(&path).unwrap();
            writer.append(&JournalEvent::App(TestEvent(1))).unwrap();
        }
        let valid_end = ENTRY_OFFSET + (codec::ENTRY_FRAMING_SIZE + 8) as u64;
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        use std::os::unix::fs::FileExt;

        let last_reachable = valid_end + MAX_UNSYNCED_BYTES - 1;
        file.write_all_at(&[0xA5], last_reachable).unwrap();
        let mut reader = JournalReader::<TestEvent>::open(&path).unwrap();
        assert!(reader.next_entry().unwrap().is_some());
        assert!(reader.next_entry().unwrap().is_none());
        assert_eq!(
            reader.torn_tail(),
            Some(TornTail {
                offset: valid_end,
                len: MAX_UNSYNCED_BYTES
            })
        );

        file.write_all_at(&[0], last_reachable).unwrap();
        file.write_all_at(&[0xA5], last_reachable + 1).unwrap();
        let mut reader = JournalReader::<TestEvent>::open(&path).unwrap();
        assert!(reader.next_entry().unwrap().is_some());
        match reader.next_entry() {
            Err(JournalError::UnrecoverableTail {
                nonzero_at,
                cause: None,
                ..
            }) => assert_eq!(nonzero_at, last_reachable + 1),
            other => panic!("expected UnrecoverableTail, got {other:?}"),
        }
    }

    /// A whole entry — CRC valid — whose sequence skips ahead is
    /// corruption wherever it sits, the live segment's tail included: a
    /// crash leaves bytes as written or zeros, never a whole entry with
    /// the wrong sequence.
    #[test]
    fn sequence_gap_detected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");
        {
            let mut writer = BufferedWriter::<TestEvent>::create(&path).unwrap();
            writer.append(&JournalEvent::App(TestEvent(1))).unwrap();
        }
        let valid_end = ENTRY_OFFSET + (codec::ENTRY_FRAMING_SIZE + 8) as u64;
        let mut scratch = [0u8; 256];
        let len = codec::encode(99, 0, 0, &JournalEvent::App(TestEvent(2)), &mut scratch).unwrap();
        use std::os::unix::fs::FileExt;
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.write_all_at(&scratch[..len], valid_end).unwrap();

        let mut reader = JournalReader::<TestEvent>::open(&path).unwrap();
        reader.next_entry().unwrap().expect("first entry");
        let err = reader.next_entry();
        assert!(
            matches!(
                err,
                Err(JournalError::SequenceGap {
                    expected: 2,
                    actual: 99
                })
            ),
            "expected SequenceGap, got {err:?}"
        );
    }

    /// Finding 7 at the reader: a length wider than any entry of the
    /// event type is corruption before the CRC is looked at — even on the
    /// live segment's last entry with only zeros after it, where the CRC
    /// slot the length points at would read as zeros.
    #[test]
    fn an_over_long_length_is_corruption_not_a_torn_tail() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");
        {
            let mut writer = BufferedWriter::<TestEvent>::create(&path).unwrap();
            writer.append(&JournalEvent::App(TestEvent(1))).unwrap();
            writer.append(&JournalEvent::App(TestEvent(2))).unwrap();
        }
        let second = ENTRY_OFFSET + (codec::ENTRY_FRAMING_SIZE + 8) as u64;
        use std::os::unix::fs::FileExt;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let mut length = [0u8; 2];
        file.read_exact_at(&mut length, second + 2).unwrap();
        // One more byte than the widest `TestEvent` entry.
        let widest = (codec::ENTRY_META_SIZE + TestEvent::MAX_ENCODED_SIZE) as u16;
        assert_eq!(u16::from_le_bytes(length), widest);
        file.write_all_at(&(widest + 1).to_le_bytes(), second + 2)
            .unwrap();

        let mut reader = JournalReader::<TestEvent>::open(&path).unwrap();
        reader.next_entry().unwrap().expect("first entry");
        let err = reader.next_entry();
        assert!(
            matches!(err, Err(JournalError::CorruptEntry { sequence: 2, .. })),
            "expected CorruptEntry, got {err:?}"
        );
    }

    /// An archived segment may end in zeros (allocation padding a
    /// compaction did not trim) — and only in zeros.
    #[test]
    fn an_archived_segment_may_end_in_zeros_only() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");
        {
            let mut writer = BufferedWriter::<TestEvent>::create(&path).unwrap();
            writer.append(&JournalEvent::App(TestEvent(1))).unwrap();
        }
        let mut reader = JournalReader::<TestEvent>::open_archived(&path).unwrap();
        assert!(reader.next_entry().unwrap().is_some());
        assert!(
            reader.next_entry().unwrap().is_none(),
            "padding is not data"
        );

        let valid_end = ENTRY_OFFSET + (codec::ENTRY_FRAMING_SIZE + 8) as u64;
        use std::os::unix::fs::FileExt;
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.write_all_at(&[1], valid_end + 4096).unwrap();
        let mut reader = JournalReader::<TestEvent>::open_archived(&path).unwrap();
        assert!(reader.next_entry().unwrap().is_some());
        match reader.next_entry() {
            Err(JournalError::UnrecoverableTail {
                offset, nonzero_at, ..
            }) => {
                assert_eq!(offset, valid_end);
                assert_eq!(nonzero_at, valid_end + 4096);
            }
            other => panic!("expected UnrecoverableTail, got {other:?}"),
        }
        // A refusal is not an end: asking again refuses again.
        assert!(
            matches!(
                reader.next_entry(),
                Err(JournalError::UnrecoverableTail { .. })
            ),
            "a second call must not read as a clean end"
        );
        // The same bytes in a live segment are within one drain: torn.
        let mut reader = JournalReader::<TestEvent>::open(&path).unwrap();
        assert!(reader.next_entry().unwrap().is_some());
        assert!(reader.next_entry().unwrap().is_none());
        assert!(reader.torn_tail().is_some());
    }

    /// The header's `starting_sequence` pins the first entry: a segment
    /// whose first entry carries a different sequence (e.g. an archive
    /// renamed into the wrong lineage slot) is rejected at the first
    /// decode rather than silently re-based.
    #[test]
    fn first_entry_must_match_header_starting_sequence() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");
        {
            // Continue from sequence 100 — header records 100.
            let mut writer =
                BufferedWriter::<TestEvent>::create_continuing(&path, 100, [0u8; 32], Some(0))
                    .unwrap();
            writer.append(&JournalEvent::App(TestEvent(1))).unwrap();
        }

        // Reading the intact segment works and starts at 100.
        let mut reader = JournalReader::<TestEvent>::open(&path).unwrap();
        assert_eq!(reader.starting_sequence(), 100);
        let entry = reader.next_entry().unwrap().expect("entry");
        assert_eq!(entry.sequence, 100);

        // Forge: rewrite the first entry with sequence 200 (valid CRC).
        let mut scratch = [0u8; 256];
        let len = codec::encode(200, 0, 0, &JournalEvent::App(TestEvent(1)), &mut scratch).unwrap();
        use std::io::{Seek, SeekFrom};
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.seek(SeekFrom::Start(ENTRY_OFFSET)).unwrap();
        file.write_all(&scratch[..len]).unwrap();
        file.sync_all().unwrap();

        let mut reader = JournalReader::<TestEvent>::open(&path).unwrap();
        let err = reader.next_entry();
        assert!(
            matches!(err, Err(JournalError::SequenceGap { expected: 100, .. })),
            "expected SequenceGap at first entry, got {err:?}"
        );
    }
}
