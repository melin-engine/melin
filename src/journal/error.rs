//! Journal error types.

use std::fmt;

/// Errors that can occur during journal operations.
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
    /// Sequence numbers are not contiguous.
    SequenceGap { expected: u64, actual: u64 },
    /// Entry is incomplete (likely a crash during write).
    TruncatedEntry,
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
            Self::TruncatedEntry => write!(f, "truncated entry at end of journal"),
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
