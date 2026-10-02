//! Journal error types.

use std::fmt;

/// Format a 32-byte hash as a short hex prefix (first 8 bytes) for
/// operator-facing diagnostics. Public so downstream crates (e.g.
/// `melin-transport-core`) can produce the same format when surfacing
/// chain-hash mismatches from their own error types.
pub fn hex_prefix(hash: &[u8; 32]) -> String {
    hash.iter()
        .take(8)
        .map(|b| format!("{b:02x}"))
        .collect::<String>()
        + "..."
}

/// Errors that can occur during journal operations.
///
/// Every variant describes a transport-level failure: I/O, framing,
/// CRC/chain integrity, or version/format mismatch. App-level rejections
/// (a request the application's own rules refuse) are the app's concern
/// and propagate through the app's own error type alongside this one —
/// kept application-agnostic so the journal crate stays usable by any
/// application.
#[derive(Debug)]
pub enum JournalError {
    /// Underlying I/O error.
    Io(std::io::Error),
    /// The kernel refused a write or a sync of the live segment's data:
    /// an append's `pwrite`, its `fdatasync`, the header write and
    /// `fsync` of a new segment, or the `fsync` that seals a reopened
    /// segment before appends resume.
    ///
    /// Kept apart from [`Self::Io`] because of what it leaves behind.
    /// After a failed write-back, Linux marks the pages clean, keeps
    /// their contents in the page cache and reports the error once, so a
    /// process that opens the segment again in place reads data the
    /// device never took as if it were durable, and its next sync
    /// succeeds. Restarting in place is unsafe until the host reboots;
    /// a node stopped by this error says so through its exit status.
    /// An error before anything was written (allocating space, creating
    /// a file, reading) is [`Self::Io`]. A failed rotation is never
    /// fatal, whatever its class: its rollback discards the new segment.
    WriteFailed(std::io::Error),
    /// File does not start with expected magic bytes.
    InvalidFile,
    /// Journal format version is not supported by this build.
    UnsupportedVersion { version: u16 },
    /// An entry failed validation (e.g., unknown event tag, bad field values).
    CorruptEntry { sequence: u64, reason: &'static str },
    /// CRC32C checksum does not match the entry data.
    ChecksumMismatch {
        sequence: u64,
        expected: u32,
        actual: u32,
    },
    /// Sequence numbers skipped forward — entries between `expected`
    /// and `actual` are missing. Typical causes: file truncation,
    /// corrupted entry skipped by the caller, bug dropping a batch.
    SequenceGap { expected: u64, actual: u64 },
    /// Sequence number already seen — the decoded entry re-uses a
    /// sequence that was observed earlier in this read pass. Distinct
    /// from `SequenceGap`: a gap means *missing* entries, a duplicate
    /// means the writer emitted the same seq twice.
    SequenceDuplicate { sequence: u64, previous_seq: u64 },
    /// Entry is incomplete (likely a crash during write).
    TruncatedEntry,
    /// A successor segment's header anchor does not equal the preceding
    /// segment's final chain hash. Indicates tampering with an archived
    /// segment's contents, a missing segment between two surviving
    /// archives, or a foreign segment spliced into the chain. Reported
    /// with the boundary segment's archive index.
    SegmentChainBreak {
        /// Archive index of the segment whose header anchor was found to
        /// disagree with the previous segment's tail. The bare live
        /// segment uses `index = 0` for diagnostics only.
        index: u32,
        expected: [u8; 32],
        actual: [u8; 32],
    },
    /// A replica's local chain value at a primary-announced stream
    /// position (a rotation boundary's tail hash, or a periodic
    /// `ChainCheck`) disagrees with the primary's — the replica's
    /// journal has divergent history (e.g. an ex-primary rejoining after
    /// failover with a journaled-but-unreplicated suffix). The replica
    /// must be re-seeded via snapshot resync; its local journal is
    /// audit-trail material and must be archived, never deleted.
    ReplicaChainDivergence {
        sequence: u64,
        expected: [u8; 32],
        actual: [u8; 32],
    },
    /// A replica was handed a primary-assigned sequence that is not the
    /// next one its journal expects. Nothing was written. A replica's
    /// stream is contiguous by construction, so this is a bug upstream of
    /// the journal (a session re-sending entries the replica already
    /// holds, or skipping some), never corruption on disk.
    ReplicaSequenceMismatch { expected: u64, actual: u64 },
    /// The encoder was asked to write a sequence at or below the last one
    /// it wrote. Nothing was written. Refused in every build: journaled
    /// out of order, the entry would make the journal unrecoverable
    /// (`SequenceDuplicate` at the next start).
    SequenceRegression { sequence: u64, last_encoded: u64 },
    /// A segment's entries stop early, at byte `offset` of `path`, and
    /// what follows cannot be a torn write a crash left behind: a
    /// non-zero byte sits at `nonzero_at`, beyond what one unsynced write
    /// can reach in the live segment, or anywhere after the stop in a
    /// sealed archive (which is synced before it is archived). Recovery
    /// refuses rather than discard data that may have been acknowledged.
    ///
    /// `cause` is why the entry at `offset` did not decode (`None` when
    /// it reads as zeros); `last_sequence` is the last entry that did.
    UnrecoverableTail {
        path: std::path::PathBuf,
        offset: u64,
        last_sequence: Option<u64>,
        nonzero_at: u64,
        cause: Option<Box<JournalError>>,
    },
}

impl fmt::Display for JournalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "journal I/O error: {e}"),
            Self::WriteFailed(e) => write!(
                f,
                "journal write failed: {e} (the data may not be on the device; do not \
                 restart this node in place without rebooting the host)"
            ),
            Self::InvalidFile => write!(f, "invalid journal file (bad magic)"),
            Self::UnsupportedVersion { version } => {
                write!(f, "unsupported journal format version: {version}")
            }
            Self::CorruptEntry { sequence, reason } => {
                write!(f, "corrupt entry at sequence {sequence}: {reason}")
            }
            Self::ChecksumMismatch {
                sequence,
                expected,
                actual,
            } => write!(
                f,
                "checksum mismatch at sequence {sequence}: expected {expected:#010x}, got {actual:#010x}"
            ),
            Self::SequenceGap { expected, actual } => {
                write!(f, "sequence gap: expected {expected}, got {actual}")
            }
            Self::SequenceDuplicate {
                sequence,
                previous_seq,
            } => write!(
                f,
                "sequence duplicate: {sequence} already seen \
                 (immediately after seq {previous_seq})"
            ),
            Self::TruncatedEntry => write!(f, "truncated entry at end of journal"),
            Self::SegmentChainBreak {
                index,
                expected,
                actual,
            } => write!(
                f,
                "segment chain break at archive {index}: header anchor {} does not match \
                 previous segment's final chain hash {}",
                hex_prefix(actual),
                hex_prefix(expected)
            ),
            Self::ReplicaChainDivergence {
                sequence,
                expected,
                actual,
            } => write!(
                f,
                "replica chain divergence at stream position {sequence}: local chain \
                 {} does not match the primary's {} — divergent history, \
                 snapshot resync required",
                hex_prefix(actual),
                hex_prefix(expected)
            ),
            Self::ReplicaSequenceMismatch { expected, actual } => write!(
                f,
                "replica refused to journal sequence {actual}: the next sequence is \
                 {expected}"
            ),
            Self::SequenceRegression {
                sequence,
                last_encoded,
            } => write!(
                f,
                "refused to journal sequence {sequence}: sequence {last_encoded} is \
                 already journaled"
            ),
            Self::UnrecoverableTail {
                path,
                offset,
                last_sequence,
                nonzero_at,
                cause,
            } => {
                write!(
                    f,
                    "journal segment {} stops at byte {offset} (",
                    path.display()
                )?;
                match cause {
                    Some(cause) => write!(f, "{cause}")?,
                    None => write!(f, "zeros")?,
                }
                match last_sequence {
                    Some(seq) => write!(f, ", after sequence {seq}")?,
                    None => write!(f, ", before its first entry")?,
                }
                write!(
                    f,
                    ") but holds data at byte {nonzero_at}: not a torn write, refusing to \
                     discard it"
                )
            }
        }
    }
}

impl std::error::Error for JournalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) | Self::WriteFailed(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for JournalError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
