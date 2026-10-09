//! Journal error types.

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};

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
    /// The kernel failed a sync of journal data: the live segment's
    /// `fdatasync` after an append, the `fsync` of a new segment's
    /// header, or the `fsync` that seals a reopened segment before
    /// appends resume (the first sync to see a write-back error left by
    /// the previous process).
    ///
    /// Kept apart from [`Self::Io`] because of what it leaves behind.
    /// After a failed write-back, Linux marks the pages clean, keeps
    /// their contents in the page cache and reports the error once, so a
    /// process that opens the segment again in place reads data the
    /// device never took as if it were durable, and its next sync
    /// succeeds. Restarting in place is unsafe until the host reboots;
    /// a node stopped by this error says so through its exit status.
    ///
    /// A failed `pwrite`/`pwritev` of an append (or of a header) is
    /// [`Self::Io`], not this: a buffered write the kernel refused left
    /// no clean-but-unwritten pages behind. Pages dirtied by earlier,
    /// successful writes may still fail their write-back after the
    /// process exits, but that error is then reported to the first sync
    /// on a newly opened descriptor, which is the reopen's `fsync` at
    /// the next start, already in this class. Allocating space,
    /// creating a file and reading are [`Self::Io`] too: nothing was
    /// written. A failed rotation is never fatal, whatever its class:
    /// its rollback discards the new segment.
    ///
    /// The segment preparer's own syncs of a staging file (the
    /// background preallocation, before the journal writes anything into
    /// it) are [`Self::Io`] too, and so stay off the latch: the file holds
    /// only zeros or an extent allocation, never journal data, and is
    /// never adopted after a failure (the next prepare, or the next
    /// start, removes it), so nothing a restart could misread is left
    /// behind. The rotation's syncs differ: they flush the header the
    /// journal wrote into the adopted segment.
    ///
    /// Construct it with [`Self::write_failed`], which also sets the
    /// process-wide latch read by [`write_failure_latched`].
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

/// Set, never cleared outside tests, once any [`JournalError::WriteFailed`]
/// has been built through [`JournalError::write_failed`] in this process.
///
/// A process-wide static rather than state carried by the error, because
/// the error is what gets lost: on its way out of the runtime it can be
/// formatted into a message, or replaced by another failure seen at the
/// same teardown, and a chain walk then no longer finds it. What the
/// latch answers ("did the kernel fail a journal sync in this process's
/// life?") is a property of the process, which is what the exit status
/// reports. An `AtomicBool` because it is written from whichever thread
/// hit the failure (the journal's disk thread, the replication receiver)
/// and read once on the way out; it is off every hot path.
static WRITE_FAILURE_LATCHED: AtomicBool = AtomicBool::new(false);

impl JournalError {
    /// A [`Self::WriteFailed`] for `error`, latching the failure for the
    /// process (see [`write_failure_latched`]). Every production site
    /// builds the variant through this.
    pub fn write_failed(error: std::io::Error) -> Self {
        WRITE_FAILURE_LATCHED.store(true, Ordering::SeqCst);
        Self::WriteFailed(error)
    }
}

/// Whether this process has seen a journal write failure (a
/// [`JournalError::WriteFailed`] built through
/// [`JournalError::write_failed`]), whatever became of the error since.
///
/// It also latches a failure that did not stop the node (a failed
/// rotation, which rolls back): the device failed a sync during this
/// process's life, and a node that later stops on any error should not
/// be restarted in place either.
pub fn write_failure_latched() -> bool {
    WRITE_FAILURE_LATCHED.load(Ordering::SeqCst)
}

/// Clear the latch, for tests that share a process. See
/// `test_utils::reset_write_failure_latch`.
#[cfg(feature = "test-utils")]
pub(crate) fn reset_write_failure_latch() {
    WRITE_FAILURE_LATCHED.store(false, Ordering::SeqCst);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Building the error through its constructor latches it for the
    /// process; nothing in this crate's tests clears the latch, so the
    /// assertion holds whatever runs in parallel.
    #[test]
    fn write_failed_latches_the_failure() {
        let error = JournalError::write_failed(std::io::Error::from_raw_os_error(libc::EIO));
        assert!(matches!(error, JournalError::WriteFailed(_)));
        assert!(write_failure_latched());
    }
}
