//! Local-lineage archival for snapshot resyncs.
//!
//! When a replica is re-seeded from a snapshot, its existing journal is
//! moved aside — never deleted. A *divergent* journal (chain mismatch
//! with the primary) is audit-trail material: under the `disk` ack policy
//! it may hold acked orders that did not survive a failover, exactly
//! the data an operator or regulator will want to reconcile. A merely
//! *stale* journal (history pruned on the primary) is kept for the same
//! conservative reason — the primary may have pruned the only other
//! copy. Operators reclaim the space once reconciled.

use std::io;
use std::path::{Path, PathBuf};

use tracing::{info, warn};

/// Why a lineage is being archived — becomes part of the directory
/// name so operators can triage at a glance.
#[derive(Debug, Clone, Copy)]
pub enum ArchiveReason {
    /// The primary reported `HashMismatch`: this journal's history
    /// forked from the primary's.
    Divergent,
    /// Routine resync (history pruned on the primary, or an operator-
    /// forced reseed) — the journal is consistent, just unusable as a
    /// catch-up base.
    Resync,
}

impl ArchiveReason {
    fn tag(self) -> &'static str {
        match self {
            Self::Divergent => "divergent",
            Self::Resync => "resync",
        }
    }
}

/// Move the entire local lineage (live segment, archives, snapshot)
/// into a sibling directory `<journal-name>.<reason>.<n>/`, lowest free
/// `n`. Returns the directory, or `None` when there was nothing to
/// archive (fresh replica). The parent directory is fsynced so the
/// renames survive a crash.
///
/// Counter-based naming rather than a timestamp: deterministic,
/// collision-free, and meaningful in sorted listings.
pub fn archive_local_lineage(
    journal_path: &Path,
    snapshot_path: &Path,
    reason: ArchiveReason,
) -> io::Result<Option<PathBuf>> {
    let mut to_move: Vec<PathBuf> = melin_journal::segment::list_archives(journal_path)
        .map_err(|e| io::Error::other(format!("archive discovery: {e}")))?
        .into_iter()
        .map(|(_, p)| p)
        .collect();
    if journal_path.exists() {
        to_move.push(journal_path.to_path_buf());
    }
    if snapshot_path.exists() {
        to_move.push(snapshot_path.to_path_buf());
    }
    if to_move.is_empty() {
        return Ok(None);
    }

    let parent = journal_path.parent().unwrap_or_else(|| Path::new("."));
    let stem = journal_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("journal");
    let dir = (0u32..)
        .map(|n| parent.join(format!("{stem}.{}.{n}", reason.tag())))
        .find(|p| !p.exists())
        .expect("u32 archive-dir counter space exhausted");
    std::fs::create_dir(&dir)?;

    for src in &to_move {
        let dst = dir.join(src.file_name().expect("archived paths have file names"));
        std::fs::rename(src, &dst)?;
    }
    if let Err(e) = melin_journal::segment::fsync_parent_dir(journal_path) {
        // Best-effort: the renames themselves succeeded; only their
        // crash-durability is in doubt. Don't fail the resync over it.
        warn!(error = %e, "fsync after lineage archival failed");
    }

    info!(
        dir = %dir.display(),
        files = to_move.len(),
        reason = reason.tag(),
        "local journal lineage archived before resync"
    );
    Ok(Some(dir))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestEvent;
    use melin_journal::{BufferedWriter, JournalEvent, JournalWrite};

    fn build_lineage(dir: &Path) -> (PathBuf, PathBuf) {
        let live = dir.join("a.journal");
        let snap = dir.join("a.snapshot");
        let mut w = BufferedWriter::<TestEvent>::create(&live).unwrap();
        w.append(&JournalEvent::App(TestEvent::Add(1))).unwrap();
        w.rotate_segment().unwrap();
        w.append(&JournalEvent::App(TestEvent::Add(2))).unwrap();
        drop(w);
        std::fs::write(&snap, b"snapshot-bytes").unwrap();
        (live, snap)
    }

    /// Everything (live + archives + snapshot) moves into one reason-
    /// tagged directory; the original paths are free for the resync.
    #[test]
    fn archives_whole_lineage_and_frees_paths() {
        let dir = tempfile::tempdir().unwrap();
        let (live, snap) = build_lineage(dir.path());

        let archived = archive_local_lineage(&live, &snap, ArchiveReason::Divergent)
            .unwrap()
            .expect("lineage present");

        assert_eq!(archived, dir.path().join("a.journal.divergent.0"));
        assert!(!live.exists());
        assert!(!snap.exists());
        assert!(!dir.path().join("a.journal.000001").exists());
        assert!(archived.join("a.journal").exists());
        assert!(archived.join("a.journal.000001").exists());
        assert!(archived.join("a.snapshot").exists());
    }

    /// Repeated archival picks the next free counter instead of
    /// clobbering earlier evidence.
    #[test]
    fn repeated_archival_never_clobbers() {
        let dir = tempfile::tempdir().unwrap();
        let (live, snap) = build_lineage(dir.path());
        let first = archive_local_lineage(&live, &snap, ArchiveReason::Divergent)
            .unwrap()
            .unwrap();

        let (live, snap) = build_lineage(dir.path());
        let second = archive_local_lineage(&live, &snap, ArchiveReason::Divergent)
            .unwrap()
            .unwrap();

        assert_ne!(first, second);
        assert!(first.join("a.journal").exists());
        assert!(second.join("a.journal").exists());
    }

    /// A fresh replica has nothing to archive — no directory created.
    #[test]
    fn fresh_replica_archives_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let live = dir.path().join("none.journal");
        let snap = dir.path().join("none.snapshot");
        assert!(
            archive_local_lineage(&live, &snap, ArchiveReason::Resync)
                .unwrap()
                .is_none()
        );
    }
}
