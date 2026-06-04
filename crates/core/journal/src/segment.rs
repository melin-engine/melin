//! Journal segment naming and discovery.
//!
//! A journal is a sequence of segment files. The live (currently-written)
//! segment is at the bare path; archived segments — produced by rotation —
//! are named `<path>.NNNNNN` with a zero-padded six-digit monotonically
//! increasing index. Recovery walks archives in order, then the live
//! segment.
//!
//! Monotonic naming (vs. cascading `.1`, `.2`) means a rotation is one
//! `rename(2)` regardless of how many archives already exist, and the
//! sort order matches the chronological order of rotations.

use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use crate::codec::{self, FileHeaderInfo};
use crate::error::JournalError;

/// Width of the zero-padded archive index. Six digits accommodates one
/// million rotations — over a century at hourly rotations.
const ARCHIVE_INDEX_WIDTH: usize = 6;

/// Summary of a verified journal lineage, returned by [`verify_lineage`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LineageReport {
    /// Number of segments walked (archives + live).
    pub segments: usize,
    /// Total entries across all segments.
    pub entries: u64,
    /// Sequence of the first entry in the lineage. `None` when every
    /// segment is empty.
    pub first_sequence: Option<u64>,
    /// Sequence of the last entry in the lineage. `None` when every
    /// segment is empty.
    pub last_sequence: Option<u64>,
    /// `starting_sequence` of the oldest segment — where the on-disk
    /// history begins. Callers with snapshot context can check it
    /// against their required floor (recovery does; offline audits
    /// report it for the operator).
    pub lineage_start: u64,
    /// Tail chain hash after the last segment. `None` when the
    /// `hash-chain` feature is disabled.
    pub tail_chain_hash: Option<[u8; 32]>,
}

/// Walk a journal lineage (archives in monotonic order, then the live
/// segment) and verify every cross-segment invariant:
///
/// - each segment's entries are dense and CRC-valid, with the first
///   entry matching the header's `starting_sequence` (enforced by the
///   reader);
/// - each successor's header anchor equals its predecessor's tail
///   chain hash (`SegmentChainBreak` otherwise);
/// - each successor's `starting_sequence` continues the sequence space
///   exactly (`SequenceGap` otherwise), including across empty
///   segments (rotation consumes no sequence).
///
/// This is the offline-audit counterpart of recovery's walk — recovery
/// interleaves the same checks with replay, while this function only
/// reads. It does not validate against a snapshot (it has none), so a
/// trimmed-but-internally-consistent lineage passes; callers judge
/// `lineage_start` themselves.
pub fn verify_lineage<E: melin_app::AppEvent>(live: &Path) -> Result<LineageReport, JournalError> {
    let mut segments: Vec<PathBuf> = list_archives(live)?.into_iter().map(|(_, p)| p).collect();
    if live.exists() {
        segments.push(live.to_path_buf());
    }
    if segments.is_empty() {
        return Err(JournalError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "no journal segments on disk",
        )));
    }

    let mut prev_tail: Option<[u8; 32]> = None;
    let mut expected_start: Option<u64> = None;
    let mut lineage_start: u64 = 0;
    let mut first_sequence: Option<u64> = None;
    let mut last_sequence: Option<u64> = None;
    let mut entries: u64 = 0;
    let mut tail_chain_hash: Option<[u8; 32]> = None;

    for (index, path) in segments.iter().enumerate() {
        let mut reader = crate::reader::JournalReader::<E>::open(path)?;

        if index == 0 {
            lineage_start = reader.starting_sequence();
        }
        if let (Some(expected), Some(actual)) = (prev_tail, reader.anchor())
            && expected != actual
        {
            return Err(JournalError::SegmentChainBreak {
                index: index as u32,
                expected,
                actual,
            });
        }
        if let Some(expected) = expected_start
            && reader.starting_sequence() != expected
        {
            return Err(JournalError::SequenceGap {
                expected,
                actual: reader.starting_sequence(),
            });
        }

        while let Some(entry) = reader.next_entry()? {
            if first_sequence.is_none() {
                first_sequence = Some(entry.sequence);
            }
            last_sequence = Some(entry.sequence);
            entries += 1;
        }

        if let Some(tail) = reader.chain_hash() {
            prev_tail = Some(tail);
            tail_chain_hash = Some(tail);
        }
        expected_start = Some(
            reader
                .last_sequence()
                .map(|s| s + 1)
                .unwrap_or_else(|| reader.starting_sequence()),
        );
    }

    Ok(LineageReport {
        segments: segments.len(),
        entries,
        first_sequence,
        last_sequence,
        lineage_start,
        tail_chain_hash,
    })
}

/// Read and validate a segment's file header, returning its decoded
/// fields (`starting_sequence`, `anchor_hash`, …).
///
/// Opens a plain (non-O_DIRECT) handle — startup/diagnostic path only.
/// Used by replication bootstrap to hand a replica the live segment's
/// identity, and by recovery to verify cross-segment lineage.
pub fn read_header_info(path: &Path) -> Result<FileHeaderInfo, JournalError> {
    let file = std::fs::File::open(path)?;
    let mut buf = [0u8; codec::FILE_HEADER_SIZE];
    // read_exact_at loops on short preads (legal under POSIX — NFS,
    // signal interruption) instead of treating them as truncation; a
    // genuinely short file still surfaces as UnexpectedEof.
    file.read_exact_at(&mut buf, 0)?;
    codec::decode_file_header(&buf)
}

/// Build the path for archive number `n`.
pub fn archive_path(live: &Path, n: u32) -> PathBuf {
    PathBuf::from(format!(
        "{}.{:0width$}",
        live.display(),
        n,
        width = ARCHIVE_INDEX_WIDTH
    ))
}

/// Parse an archive index out of a path stem, given the live path.
///
/// Returns `Some(n)` if `candidate` is `<live>.NNNNNN` for some non-zero
/// monotonic `n` (any width — width-6 zero-padded is the canonical form,
/// but we accept narrower numerics too so legacy `.1`/`.2` archives are
/// at least discovered, even if they will never be written by the
/// current writer).
fn parse_archive_index(live: &Path, candidate: &Path) -> Option<u32> {
    let live_name = live.file_name()?.to_str()?;
    let cand_name = candidate.file_name()?.to_str()?;
    let suffix = cand_name.strip_prefix(live_name)?.strip_prefix('.')?;
    // Reject suffixes that contain another dot — e.g. `.snap.tmp`,
    // `.snapshot.prev`, or pre-existing snapshot files that share the
    // journal stem. Archives are pure integers.
    if suffix.contains('.') {
        return None;
    }
    let n: u32 = suffix.parse().ok()?;
    if n == 0 { None } else { Some(n) }
}

/// List archived segments for a live journal path, sorted ascending by
/// index. Each entry is `(index, path)`. Missing parent directory is
/// treated as "no archives yet".
///
/// Errors with `InvalidData` if two entries map to the same index — this
/// happens only if a legacy `.N` archive coexists with the canonical
/// zero-padded `.NNNNNN` form for the same N. Recovery cannot
/// disambiguate which segment's hash anchors the next, so we refuse to
/// proceed rather than risk a non-deterministic walk order.
pub fn list_archives(live: &Path) -> std::io::Result<Vec<(u32, PathBuf)>> {
    let dir = match live.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        _ => PathBuf::from("."),
    };
    let read_dir = match std::fs::read_dir(&dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    // Vec because output is sorted before return; small (rotations are
    // infrequent), so allocation cost is negligible vs. recovery I/O.
    let mut out: Vec<(u32, PathBuf)> = Vec::new();
    for entry in read_dir {
        let entry = entry?;
        let path = entry.path();
        if let Some(n) = parse_archive_index(live, &path) {
            out.push((n, path));
        }
    }
    out.sort_by_key(|(n, _)| *n);
    for w in out.windows(2) {
        if w[0].0 == w[1].0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "duplicate archive index {} for live journal {}: {} and {}",
                    w[0].0,
                    live.display(),
                    w[0].1.display(),
                    w[1].1.display()
                ),
            ));
        }
    }
    Ok(out)
}

/// Path of the next archive slot for `live` (one past the current max).
/// Returns `<live>.000001` when no archives exist.
pub fn next_archive_path(live: &Path) -> std::io::Result<PathBuf> {
    let archives = list_archives(live)?;
    let next_n = archives.last().map(|(n, _)| *n + 1).unwrap_or(1);
    Ok(archive_path(live, next_n))
}

/// Rename the live journal to the next archive slot. Returns the
/// archive path the live file was renamed to.
///
/// The caller is responsible for any flushing/syncing of the live file
/// before this call, and for opening a new live segment afterward. The
/// directory entry is *not* fsynced here — call [`fsync_parent_dir`]
/// after the new live segment has been created and its dirent is
/// written, so a single dir fsync covers both metadata changes.
pub fn archive_live(live: &Path) -> std::io::Result<PathBuf> {
    let target = next_archive_path(live)?;
    std::fs::rename(live, &target)?;
    Ok(target)
}

/// Fsync the parent directory of `live` to durably commit dirent
/// changes (renames, file creations). Without this, a rename + new-file
/// pair that has reached the page cache may be lost on power loss even
/// though both file contents are durable.
///
/// Treats a missing parent directory as a no-op (the rename would have
/// already failed in that case).
pub fn fsync_parent_dir(live: &Path) -> std::io::Result<()> {
    let dir = match live.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        _ => PathBuf::from("."),
    };
    // Open with read-only — fsync(2) on a directory fd flushes its
    // metadata regardless of open mode.
    let f = std::fs::File::open(&dir)?;
    f.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffered_writer::BufferedWriter;
    use crate::event::JournalEvent;
    use crate::write::JournalWrite;
    use melin_app::{AppEvent, CodecError};

    /// Minimal `AppEvent` for lineage tests.
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

    /// Build `live` with `events_per_phase` entries between rotations.
    /// `phases.len() - 1` rotations are performed (the last phase stays
    /// in the live segment). A phase count of 0 produces an empty
    /// segment.
    fn build_lineage(live: &Path, phases: &[u64]) {
        let mut writer = BufferedWriter::<TestEvent>::create(live).unwrap();
        let mut value = 0u64;
        for (i, &count) in phases.iter().enumerate() {
            for _ in 0..count {
                value += 1;
                writer.append(&JournalEvent::App(TestEvent(value))).unwrap();
            }
            if i + 1 < phases.len() {
                writer.rotate_segment().unwrap();
            }
        }
    }

    #[test]
    fn verify_lineage_accepts_intact_multi_segment_journal() {
        let dir = tempfile::tempdir().unwrap();
        let live = dir.path().join("j.journal");
        // Includes an empty middle segment from back-to-back rotation.
        build_lineage(&live, &[2, 0, 3]);

        let report = verify_lineage::<TestEvent>(&live).unwrap();
        assert_eq!(report.segments, 3);
        assert_eq!(report.entries, 5);
        assert_eq!(report.lineage_start, 1);
        assert_eq!(report.first_sequence, Some(1));
        assert_eq!(report.last_sequence, Some(5));
        #[cfg(feature = "hash-chain")]
        assert!(report.tail_chain_hash.is_some());
    }

    #[test]
    fn verify_lineage_rejects_missing_middle_segment() {
        let dir = tempfile::tempdir().unwrap();
        let live = dir.path().join("j.journal");
        build_lineage(&live, &[2, 2, 2]);
        std::fs::remove_file(archive_path(&live, 2)).unwrap();

        let err = verify_lineage::<TestEvent>(&live).unwrap_err();
        assert!(
            matches!(
                err,
                JournalError::SegmentChainBreak { .. } | JournalError::SequenceGap { .. }
            ),
            "expected lineage break, got {err:?}"
        );
    }

    #[test]
    fn verify_lineage_reports_trimmed_start() {
        // A trimmed-but-consistent prefix passes verification (no
        // snapshot context here) but the report exposes where history
        // begins so callers can judge.
        let dir = tempfile::tempdir().unwrap();
        let live = dir.path().join("j.journal");
        build_lineage(&live, &[2, 2, 2]);
        std::fs::remove_file(archive_path(&live, 1)).unwrap();

        let report = verify_lineage::<TestEvent>(&live).unwrap();
        assert_eq!(report.lineage_start, 3);
        assert_eq!(report.first_sequence, Some(3));
        assert_eq!(report.last_sequence, Some(6));
    }

    #[test]
    fn archive_path_pads_to_six_digits() {
        let live = PathBuf::from("/tmp/foo.journal");
        assert_eq!(
            archive_path(&live, 1),
            PathBuf::from("/tmp/foo.journal.000001")
        );
        assert_eq!(
            archive_path(&live, 999_999),
            PathBuf::from("/tmp/foo.journal.999999")
        );
    }

    #[test]
    fn next_archive_starts_at_one() {
        let dir = tempfile::tempdir().unwrap();
        let live = dir.path().join("j.bin");
        // No archives yet — even without the live file existing.
        assert_eq!(next_archive_path(&live).unwrap(), archive_path(&live, 1));
    }

    #[test]
    fn next_archive_picks_max_plus_one() {
        let dir = tempfile::tempdir().unwrap();
        let live = dir.path().join("j.bin");
        std::fs::write(archive_path(&live, 1), b"").unwrap();
        std::fs::write(archive_path(&live, 2), b"").unwrap();
        std::fs::write(archive_path(&live, 5), b"").unwrap();
        assert_eq!(next_archive_path(&live).unwrap(), archive_path(&live, 6));
    }

    #[test]
    fn list_archives_sorted_and_excludes_unrelated() {
        let dir = tempfile::tempdir().unwrap();
        let live = dir.path().join("j.bin");
        std::fs::write(archive_path(&live, 3), b"").unwrap();
        std::fs::write(archive_path(&live, 1), b"").unwrap();
        std::fs::write(archive_path(&live, 2), b"").unwrap();
        // Unrelated files in the same dir must not be listed.
        std::fs::write(dir.path().join("other.bin"), b"").unwrap();
        std::fs::write(dir.path().join("j.bin.snapshot"), b"").unwrap();
        std::fs::write(dir.path().join("j.bin.snap.tmp"), b"").unwrap();
        let archives = list_archives(&live).unwrap();
        let nums: Vec<u32> = archives.iter().map(|(n, _)| *n).collect();
        assert_eq!(nums, vec![1, 2, 3]);
    }

    #[test]
    fn archive_live_moves_file() {
        let dir = tempfile::tempdir().unwrap();
        let live = dir.path().join("j.bin");
        std::fs::write(&live, b"hello").unwrap();
        let archived = archive_live(&live).unwrap();
        assert_eq!(archived, archive_path(&live, 1));
        assert!(!live.exists());
        assert_eq!(std::fs::read(&archived).unwrap(), b"hello");

        // A second rotation goes to .000002 even though the live file is
        // recreated in between.
        std::fs::write(&live, b"second").unwrap();
        let archived2 = archive_live(&live).unwrap();
        assert_eq!(archived2, archive_path(&live, 2));
    }

    #[test]
    fn duplicate_archive_indices_are_rejected() {
        // Coexisting `.1` (legacy) and `.000001` (canonical) collide on
        // index 1 — recovery would walk both non-deterministically and
        // the cross-segment hash chain would fail on the second copy.
        let dir = tempfile::tempdir().unwrap();
        let live = dir.path().join("j.bin");
        std::fs::write(PathBuf::from(format!("{}.1", live.display())), b"").unwrap();
        std::fs::write(archive_path(&live, 1), b"").unwrap();
        let err = list_archives(&live).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn legacy_short_index_is_discovered() {
        // Pre-existing `.1` archives from the legacy cascade scheme should
        // be visible to recovery so their events aren't silently lost.
        let dir = tempfile::tempdir().unwrap();
        let live = dir.path().join("j.bin");
        let legacy = PathBuf::from(format!("{}.1", live.display()));
        std::fs::write(&legacy, b"").unwrap();
        let archives = list_archives(&live).unwrap();
        assert_eq!(archives.len(), 1);
        assert_eq!(archives[0].0, 1);
    }
}
