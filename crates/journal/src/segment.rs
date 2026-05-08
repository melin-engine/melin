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

use std::path::{Path, PathBuf};

/// Width of the zero-padded archive index. Six digits accommodates one
/// million rotations — over a century at hourly rotations.
const ARCHIVE_INDEX_WIDTH: usize = 6;

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
/// before this call, and for opening a new live segment afterward.
pub fn archive_live(live: &Path) -> std::io::Result<PathBuf> {
    let target = next_archive_path(live)?;
    std::fs::rename(live, &target)?;
    Ok(target)
}

#[cfg(test)]
mod tests {
    use super::*;

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
