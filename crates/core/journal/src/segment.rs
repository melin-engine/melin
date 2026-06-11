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
    /// `Some((expected, found))` when the LIVE segment's tail had a
    /// sequence gap — the crash artifact recovery tolerates and
    /// truncates (e.g. an async write completing out of order at the
    /// moment of a crash). Entries before the gap verified normally;
    /// entries after it are exactly what recovery would discard, and
    /// none of them were ever acknowledged (persist-before-ack). A gap
    /// anywhere in a sealed archive is NOT tolerated — that's
    /// corruption and surfaces as an error instead. A cleanly shut
    /// journal must report `None` here.
    pub live_tail_gap: Option<(u64, u64)>,
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
/// reads. It mirrors recovery's crash tolerance exactly: a sequence gap
/// at the LIVE segment's tail is reported (not fatal) because recovery
/// would truncate there, while a gap inside any sealed archive is an
/// error. It does not validate against a snapshot (it has none), so a
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
    let mut live_tail_gap: Option<(u64, u64)> = None;

    for (index, path) in segments.iter().enumerate() {
        let mut reader = crate::reader::JournalReader::<E>::open(path)?;
        // Gap tolerance applies only to the live segment (recovery's
        // `allow_partial_tail`) — never to a sealed archive, and never
        // to a trailing archive standing in for a missing live.
        let is_live = path == live;

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

        loop {
            match reader.next_entry() {
                Ok(Some(entry)) => {
                    if first_sequence.is_none() {
                        first_sequence = Some(entry.sequence);
                    }
                    last_sequence = Some(entry.sequence);
                    entries += 1;
                }
                Ok(None) => break,
                Err(JournalError::SequenceGap { expected, actual }) if is_live => {
                    // Same crash shape recovery truncates at; everything
                    // before the gap verified, nothing after it was ever
                    // acknowledged. Report rather than fail so an
                    // operator auditing a crashed-but-recoverable
                    // journal can tell this apart from tampering.
                    live_tail_gap = Some((expected, actual));
                    break;
                }
                Err(e) => return Err(e),
            }
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
        live_tail_gap,
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

/// Where a target sequence falls relative to one segment file's entries.
#[cfg(feature = "hash-chain")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChainValueAt {
    /// The segment contains the sequence; this is the chain value
    /// through that entry (`BLAKE3(entry bytes ..= seq || anchor)`).
    Value([u8; 32]),
    /// The segment's entries end before the sequence — on the live
    /// segment this means the peer claims history this node never
    /// journaled.
    BeyondTip,
}

/// Chain value of one segment file at `seq` — the value a writer would
/// have reported right after encoding that entry. Walks raw entry bytes
/// (header-decode only, no payload decode) absorbing them into the
/// segment chain, exactly like recovery's `rebuild_from_file`, but
/// stops at `seq` instead of `valid_end`.
///
/// The caller must have picked the containing segment
/// (`starting_sequence <= seq`); `seq == starting_sequence - 1` has no
/// entry to stop at — that boundary's value is the header anchor, which
/// the caller reads directly via [`read_header_info`].
///
/// Cold path: replication handshake validation only.
#[cfg(feature = "hash-chain")]
pub fn chain_value_at(path: &Path, seq: u64) -> Result<ChainValueAt, JournalError> {
    let info = read_header_info(path)?;
    debug_assert!(
        info.starting_sequence <= seq,
        "caller must pick the containing segment"
    );
    let mut scanner = crate::reader::RawJournalScanner::open(path)?;
    let mut chain = crate::chain::SegmentChain::new(info.anchor_hash);
    // 1 MiB batches: large enough to amortize syscalls, small enough to
    // keep this diagnostic path's working set flat.
    let mut buf = Vec::with_capacity(1 << 20);
    loop {
        buf.clear();
        match scanner.read_raw_batch_until(&mut buf, 1 << 20, seq)? {
            Some(end) => {
                chain.absorb(&buf);
                if end == seq {
                    return Ok(ChainValueAt::Value(chain.value()));
                }
            }
            None => return Ok(ChainValueAt::BeyondTip),
        }
    }
}

/// Raw byte prefix of a segment file: the file header plus every entry
/// through `through_seq`, exactly as on disk. Shipping this to a
/// snapshot-seeded replica makes its live segment a byte-copy of the
/// primary's, so its segment boundaries (and chain values) align from
/// birth. `through_seq == starting_sequence - 1` returns the header
/// alone; `None` when the segment's entries end before `through_seq`.
///
/// Whole-buffer rather than streaming — same trade-off as the snapshot
/// transfer (`std::fs::read`): bounded by segment size, and operators
/// rotate before seeding to keep the prefix small.
pub fn read_segment_prefix(path: &Path, through_seq: u64) -> Result<Option<Vec<u8>>, JournalError> {
    let info = read_header_info(path)?;
    let mut out = vec![0u8; codec::ENTRY_OFFSET as usize];
    let file = std::fs::File::open(path)?;
    file.read_exact_at(&mut out, 0)?;
    if through_seq == info.starting_sequence.saturating_sub(1) {
        return Ok(Some(out));
    }
    debug_assert!(
        info.starting_sequence <= through_seq,
        "caller must pick the containing segment"
    );
    let mut scanner = crate::reader::RawJournalScanner::open(path)?;
    loop {
        match scanner.read_raw_batch_until(&mut out, 1 << 20, through_seq)? {
            Some(end) => {
                if end == through_seq {
                    return Ok(Some(out));
                }
            }
            None => return Ok(None),
        }
    }
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

    /// `chain_value_at` must reproduce exactly what the writer reported
    /// after encoding each entry, and flag a sequence past the tail.
    #[cfg(feature = "hash-chain")]
    #[test]
    fn chain_value_at_matches_writer_at_every_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let live = dir.path().join("chain_at.journal");

        let mut writer = BufferedWriter::<TestEvent>::create(&live).unwrap();
        let mut expected = Vec::new();
        for v in 1..=3u64 {
            writer.append(&JournalEvent::App(TestEvent(v))).unwrap();
            expected.push(writer.chain_hash().expect("hash-chain enabled"));
        }
        drop(writer);

        for (i, exp) in expected.iter().enumerate() {
            let seq = i as u64 + 1;
            assert_eq!(
                chain_value_at(&live, seq).unwrap(),
                ChainValueAt::Value(*exp),
                "chain at seq {seq} must match the writer's reported value"
            );
        }
        assert_eq!(
            chain_value_at(&live, 4).unwrap(),
            ChainValueAt::BeyondTip,
            "a sequence past the tail must be flagged, not silently hashed"
        );
    }

    /// `read_segment_prefix` returns exact on-disk byte prefixes:
    /// header-only at the opening boundary, header + entries through the
    /// target otherwise, `None` past the tail.
    #[test]
    fn read_segment_prefix_returns_exact_byte_prefixes() {
        let dir = tempfile::tempdir().unwrap();
        let live = dir.path().join("prefix.journal");
        build_lineage(&live, &[3]);
        let full = std::fs::read(&live).unwrap();

        let header_only = read_segment_prefix(&live, 0).unwrap().unwrap();
        assert_eq!(header_only.len() as u64, codec::ENTRY_OFFSET);
        assert_eq!(header_only[..], full[..codec::ENTRY_OFFSET as usize]);

        let through_2 = read_segment_prefix(&live, 2).unwrap().unwrap();
        assert!(through_2.len() as u64 > codec::ENTRY_OFFSET);
        assert_eq!(through_2[..], full[..through_2.len()]);

        let through_3 = read_segment_prefix(&live, 3).unwrap().unwrap();
        assert!(through_3.len() > through_2.len());
        assert_eq!(through_3[..], full[..through_3.len()]);

        assert!(
            read_segment_prefix(&live, 4).unwrap().is_none(),
            "a sequence past the tail has no prefix"
        );
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

    /// Forge a valid-CRC entry with a skipped sequence at `path`'s
    /// valid data end — the io_uring out-of-order-completion crash
    /// shape (a later write landed, an earlier one didn't).
    fn forge_gap_entry(path: &Path, gap_seq: u64) {
        let valid_end = {
            let mut reader = crate::reader::JournalReader::<TestEvent>::open(path).unwrap();
            while reader.next_entry().unwrap().is_some() {}
            reader.valid_file_end()
        };
        let mut scratch = [0u8; 256];
        let len = crate::codec::encode(
            gap_seq,
            0,
            0,
            0,
            &JournalEvent::App(TestEvent(99)),
            &mut scratch,
        )
        .unwrap();
        let file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
        file.write_all_at(&scratch[..len], valid_end).unwrap();
        file.sync_all().unwrap();
    }

    /// A sequence gap at the LIVE tail is the crash artifact recovery
    /// truncates — the verifier reports it instead of failing, so an
    /// operator can tell "normal crash tail" from tampering.
    #[test]
    fn verify_lineage_reports_gap_at_live_tail() {
        let dir = tempfile::tempdir().unwrap();
        let live = dir.path().join("j.journal");
        build_lineage(&live, &[2, 2]); // archive(1-2) + live(3-4)
        forge_gap_entry(&live, 6); // expected 5, found 6

        let report = verify_lineage::<TestEvent>(&live).unwrap();
        assert_eq!(report.live_tail_gap, Some((5, 6)));
        // Everything before the gap verified normally.
        assert_eq!(report.entries, 4);
        assert_eq!(report.last_sequence, Some(4));
    }

    /// The same gap inside a SEALED archive is corruption, not a crash
    /// artifact — the verifier must fail, exactly like recovery.
    #[test]
    fn verify_lineage_rejects_gap_inside_archive() {
        let dir = tempfile::tempdir().unwrap();
        let live = dir.path().join("j.journal");
        // One segment with a forged gap, then rotate so it gets sealed.
        let mut writer = BufferedWriter::<TestEvent>::create(&live).unwrap();
        writer.append(&JournalEvent::App(TestEvent(1))).unwrap();
        writer.append(&JournalEvent::App(TestEvent(2))).unwrap();
        drop(writer);
        forge_gap_entry(&live, 4); // expected 3, found 4
        std::fs::rename(&live, archive_path(&live, 1)).unwrap();
        // Recreate an (empty) live so the archive isn't the last word.
        drop(BufferedWriter::<TestEvent>::create_continuing(&live, 5, [0u8; 32]).unwrap());

        let err = verify_lineage::<TestEvent>(&live).unwrap_err();
        assert!(
            matches!(
                err,
                JournalError::SequenceGap {
                    expected: 3,
                    actual: 4
                }
            ),
            "expected hard SequenceGap inside the archive, got {err:?}"
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
