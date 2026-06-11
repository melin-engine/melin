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
/// (insufficient balance, risk limits, unknown account) are the app's
/// concern and propagate through the app's own error type alongside
/// this one — kept trading-agnostic so the journal crate stays usable
/// by any application.
#[derive(Debug)]
pub enum JournalError {
    /// Underlying I/O error.
    Io(std::io::Error),
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
    /// The journal's recorded sector size is smaller than the device's physical
    /// sector size. O_DIRECT writes would fail with EINVAL. The journal must be
    /// re-created on the target device or moved back to the original device.
    SectorSizeMismatch { journal: usize, device: usize },
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
}

impl fmt::Display for JournalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "journal I/O error: {e}"),
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
            Self::SectorSizeMismatch { journal, device } => write!(
                f,
                "journal sector size ({journal}) is smaller than the device's physical \
                 sector size ({device}); re-create the journal or move it to the original device"
            ),
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
        }
    }
}

impl std::error::Error for JournalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for JournalError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
