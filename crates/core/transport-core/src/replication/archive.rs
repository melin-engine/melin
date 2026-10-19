//! Local-lineage archival for snapshot resyncs.
//!
//! When a replica is re-seeded from a snapshot, its existing journal is
//! moved aside — never deleted. A *divergent* journal (chain mismatch
//! with the primary) is audit-trail material: under the `disk` ack policy
//! it may hold acked events that did not survive a failover, exactly
//! the data an operator or regulator will want to reconcile. A merely
//! *stale* journal (history pruned on the primary) is kept for the same
//! conservative reason — the primary may have pruned the only other
//! copy. Operators reclaim the space once reconciled.

use std::ffi::OsString;
use std::io;
use std::path::{Path, PathBuf};

use melin_journal::segment::{fsync_dir, parent_dir};
use tracing::{info, warn};

use crate::node_store::NodeStore;

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

/// Where [`archive_local_lineage`] put the old lineage. Each field is
/// `Some` when that side had material to move. When the snapshot shares
/// the journal's directory both name the same archive, so a caller
/// looking for the archived snapshot reads `snapshot_dir` whatever the
/// layout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchivedLineage {
    /// `<journal dir>/<journal-name>.<reason>.<n>/`: the archived
    /// segments and the live one.
    pub journal_dir: Option<PathBuf>,
    /// `<snapshot dir>/<snapshot-name>.<reason>.<n>/`, or the journal's
    /// archive when the two share a directory: the snapshot and its
    /// `.prev`.
    pub snapshot_dir: Option<PathBuf>,
}

/// Move the local lineage aside for a resync: the journal's segments,
/// live and archived, and the snapshot with its `.prev` (the temp file is
/// never history and stays). Returns where they went, or `None` when
/// there was nothing to archive (a fresh replica).
///
/// When the snapshot and the journal share a directory, everything goes
/// into one `<journal-name>.<reason>.<n>/` beside the journal. Otherwise
/// the journal's material goes there and the snapshot's into
/// `<snapshot-name>.<reason>.<n>/` beside the snapshot, with the same
/// `n`: the lowest free in both directories, even when only one side has
/// material, so the two halves of one resync pair up and two resyncs
/// never pose as one. Each rename stays inside one directory, so on one
/// filesystem, wherever the snapshot is.
///
/// Counter-based naming rather than a timestamp: deterministic,
/// collision-free, and meaningful in sorted listings.
pub fn archive_local_lineage(
    store: &NodeStore,
    reason: ArchiveReason,
) -> io::Result<Option<ArchivedLineage>> {
    let journal_path = store.journal();
    let mut journal_material: Vec<PathBuf> = melin_journal::segment::list_archives(journal_path)
        .map_err(|e| io::Error::other(format!("archive discovery: {e}")))?
        .into_iter()
        .map(|(_, p)| p)
        .collect();
    if journal_path.try_exists()? {
        journal_material.push(journal_path.to_path_buf());
    }
    // Vec of at most two, in the order they are moved.
    let mut snapshot_material: Vec<PathBuf> = Vec::with_capacity(2);
    for path in [store.snapshot().to_path_buf(), store.snapshot_prev()] {
        if path.try_exists()? {
            snapshot_material.push(path);
        }
    }
    if journal_material.is_empty() && snapshot_material.is_empty() {
        return Ok(None);
    }

    let journal_parent = parent_dir(journal_path);
    let snapshot_parent = parent_dir(store.snapshot());
    let journal_series = |n| archive_dir(journal_path, reason, n);
    let snapshot_series = |n| archive_dir(store.snapshot(), reason, n);
    let shared = store.snapshot_shares_journal_dir()?;

    let archived = if shared {
        let n = lowest_free_index(|n| journal_series(n)?.try_exists())?;
        let dir = journal_series(n)?;
        std::fs::create_dir(&dir)?;
        for src in journal_material.iter().chain(&snapshot_material) {
            move_into(src, &dir)?;
        }
        ArchivedLineage {
            journal_dir: (!journal_material.is_empty()).then(|| dir.clone()),
            snapshot_dir: (!snapshot_material.is_empty()).then_some(dir),
        }
    } else {
        let n = lowest_free_index(|n| {
            Ok(journal_series(n)?.try_exists()? || snapshot_series(n)?.try_exists()?)
        })?;
        let move_side = |material: &[PathBuf], dir: PathBuf| -> io::Result<Option<PathBuf>> {
            if material.is_empty() {
                return Ok(None);
            }
            std::fs::create_dir(&dir)?;
            for src in material {
                move_into(src, &dir)?;
            }
            Ok(Some(dir))
        };
        ArchivedLineage {
            journal_dir: move_side(&journal_material, journal_series(n)?)?,
            snapshot_dir: move_side(&snapshot_material, snapshot_series(n)?)?,
        }
    };

    // Durability of the moves: a rename changes the directory it leaves
    // and the one it enters, so both new archive directories and both
    // source parents (the journal's and the snapshot's) are synced.
    // Without the snapshot's parent, a power loss could bring the old
    // lineage's snapshot back beside the re-seeded journal. Best-effort:
    // the renames themselves succeeded, only their crash-durability is in
    // doubt, so a failure does not fail the resync.
    //
    // A `Vec` of at most four borrowed paths, built once per resync: no
    // set is worth it. `dedup` removes only adjacent repeats, which is
    // enough as long as each pair that can be equal is pushed next to each
    // other. Two can: the two archive directories (journal and snapshot in
    // one directory share one archive), and the two parents (the same
    // case). An archive directory is new, so never equal to a parent.
    let mut to_sync: Vec<&Path> = Vec::with_capacity(4);
    to_sync.extend(archived.journal_dir.as_deref());
    to_sync.extend(archived.snapshot_dir.as_deref());
    to_sync.push(journal_parent);
    to_sync.push(snapshot_parent);
    to_sync.dedup();
    for dir in to_sync {
        if let Err(e) = fsync_dir(dir) {
            warn!(error = %e, dir = %dir.display(), "fsync after lineage archival failed");
        }
    }

    info!(
        journal_dir = ?archived.journal_dir,
        snapshot_dir = ?archived.snapshot_dir,
        journal_files = journal_material.len(),
        snapshot_files = snapshot_material.len(),
        reason = reason.tag(),
        "local lineage archived before resync"
    );
    Ok(Some(archived))
}

/// `<parent of path>/<file name of path>.<reason>.<n>`.
fn archive_dir(path: &Path, reason: ArchiveReason, n: u32) -> io::Result<PathBuf> {
    let name = path
        .file_name()
        .ok_or_else(|| io::Error::other(format!("{} names no file to archive", path.display())))?;
    let mut dir_name = OsString::from(name);
    dir_name.push(format!(".{}.{n}", reason.tag()));
    Ok(parent_dir(path).join(dir_name))
}

/// Rename `src` into `dir`, keeping its file name.
fn move_into(src: &Path, dir: &Path) -> io::Result<()> {
    let name = src
        .file_name()
        .ok_or_else(|| io::Error::other(format!("{} names no file", src.display())))?;
    std::fs::rename(src, dir.join(name))
}

/// The lowest index `taken` reports free.
fn lowest_free_index(mut taken: impl FnMut(u32) -> io::Result<bool>) -> io::Result<u32> {
    for n in 0..=u32::MAX {
        if !taken(n)? {
            return Ok(n);
        }
    }
    Err(io::Error::other("archive index space exhausted"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestEvent;
    use melin_journal::{BufferedWriter, JournalEvent, JournalWrite};

    /// A journal (one archived segment and the live one) at
    /// `journal_dir/<name>`, and a snapshot with its `.prev` at the
    /// store's snapshot path.
    fn build_lineage(store: &NodeStore) {
        build_journal(store.journal());
        build_snapshot(store);
    }

    fn build_journal(live: &Path) {
        let mut w = BufferedWriter::<TestEvent>::create(live).unwrap();
        w.append(&JournalEvent::App(TestEvent::Add(1))).unwrap();
        w.rotate_segment().unwrap();
        w.append(&JournalEvent::App(TestEvent::Add(2))).unwrap();
    }

    fn build_snapshot(store: &NodeStore) {
        std::fs::write(store.snapshot(), b"snapshot-bytes").unwrap();
        std::fs::write(store.snapshot_prev(), b"previous-snapshot").unwrap();
    }

    fn file_name(path: &Path) -> &std::ffi::OsStr {
        path.file_name().unwrap()
    }

    /// The journal's and the snapshot's paths are free, and each archived
    /// file is in the directory reported for its side.
    fn assert_archived(store: &NodeStore, archived: &ArchivedLineage) {
        let journal_dir = archived.journal_dir.as_ref().expect("journal archived");
        let snapshot_dir = archived.snapshot_dir.as_ref().expect("snapshot archived");
        let segment = melin_journal::segment::archive_path(store.journal(), 1);
        for (src, dir) in [
            (store.journal().to_path_buf(), journal_dir),
            (segment, journal_dir),
            (store.snapshot().to_path_buf(), snapshot_dir),
            (store.snapshot_prev(), snapshot_dir),
        ] {
            assert!(!src.exists(), "{} still in place", src.display());
            assert!(
                dir.join(file_name(&src)).exists(),
                "{} not archived",
                src.display()
            );
        }
    }

    /// Default layout: everything (live, archives, snapshot, `.prev`)
    /// moves into one reason-tagged directory beside the journal; the
    /// original paths are free for the resync, and the temp file stays.
    #[test]
    fn archives_whole_lineage_and_frees_paths() {
        let dir = tempfile::tempdir().unwrap();
        let store = NodeStore::resolve(dir.path().join("a.journal"), None).unwrap();
        build_lineage(&store);
        std::fs::write(store.snapshot_tmp(), b"half-written").unwrap();

        let archived = archive_local_lineage(&store, ArchiveReason::Divergent)
            .unwrap()
            .expect("lineage present");

        let expected = dir.path().join("a.journal.divergent.0");
        assert_eq!(
            archived,
            ArchivedLineage {
                journal_dir: Some(expected.clone()),
                snapshot_dir: Some(expected),
            }
        );
        assert_archived(&store, &archived);
        assert!(
            store.snapshot_tmp().exists(),
            "the temp file is not history"
        );
    }

    /// Repeated archival picks the next free counter instead of
    /// clobbering earlier evidence.
    #[test]
    fn repeated_archival_never_clobbers() {
        let dir = tempfile::tempdir().unwrap();
        let store = NodeStore::resolve(dir.path().join("a.journal"), None).unwrap();
        build_lineage(&store);
        let first = archive_local_lineage(&store, ArchiveReason::Divergent)
            .unwrap()
            .unwrap();

        build_lineage(&store);
        let second = archive_local_lineage(&store, ArchiveReason::Divergent)
            .unwrap()
            .unwrap();

        assert_ne!(first, second);
        assert!(first.journal_dir.unwrap().join("a.journal").exists());
        assert!(second.journal_dir.unwrap().join("a.journal").exists());
    }

    /// A fresh replica has nothing to archive — no directory created.
    #[test]
    fn fresh_replica_archives_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let store = NodeStore::resolve(dir.path().join("none.journal"), None).unwrap();
        assert!(
            archive_local_lineage(&store, ArchiveReason::Resync)
                .unwrap()
                .is_none()
        );
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }

    /// The snapshot in another directory on the same filesystem: it and
    /// its `.prev` are archived beside it, under its own name, and nothing
    /// of the snapshot is in the journal's archive.
    #[test]
    fn a_snapshot_elsewhere_is_archived_beside_itself() {
        let journal_dir = tempfile::tempdir().unwrap();
        let snapshot_dir = tempfile::tempdir().unwrap();
        let store = NodeStore::resolve(
            journal_dir.path().join("a.journal"),
            Some(snapshot_dir.path().join("node-a.snapshot")),
        )
        .unwrap();
        build_lineage(&store);

        let archived = archive_local_lineage(&store, ArchiveReason::Divergent)
            .unwrap()
            .unwrap();

        assert_eq!(
            archived,
            ArchivedLineage {
                journal_dir: Some(journal_dir.path().join("a.journal.divergent.0")),
                snapshot_dir: Some(snapshot_dir.path().join("node-a.snapshot.divergent.0")),
            }
        );
        assert_archived(&store, &archived);
        let journal_archive: Vec<_> = std::fs::read_dir(archived.journal_dir.unwrap())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert!(
            journal_archive
                .iter()
                .all(|name| !name.to_string_lossy().contains("snapshot")),
            "{journal_archive:?}"
        );
    }

    /// An index free in one directory and taken in the other: both sides
    /// take the next index free in both.
    #[test]
    fn the_index_is_free_on_both_sides() {
        let journal_dir = tempfile::tempdir().unwrap();
        let snapshot_dir = tempfile::tempdir().unwrap();
        let store = NodeStore::resolve(
            journal_dir.path().join("a.journal"),
            Some(snapshot_dir.path().join("a.snapshot")),
        )
        .unwrap();
        std::fs::create_dir(journal_dir.path().join("a.journal.resync.0")).unwrap();
        std::fs::create_dir(snapshot_dir.path().join("a.snapshot.resync.1")).unwrap();
        build_lineage(&store);

        let archived = archive_local_lineage(&store, ArchiveReason::Resync)
            .unwrap()
            .unwrap();

        assert_eq!(
            archived,
            ArchivedLineage {
                journal_dir: Some(journal_dir.path().join("a.journal.resync.2")),
                snapshot_dir: Some(snapshot_dir.path().join("a.snapshot.resync.2")),
            }
        );
    }

    /// Two nodes whose journals share a file name, in different
    /// directories, with one shared snapshot directory: each node's
    /// snapshot is archived under its own snapshot name, and neither
    /// journal side skips an index because of the other node.
    #[test]
    fn nodes_sharing_a_snapshot_directory_keep_their_own_series() {
        let snapshots = tempfile::tempdir().unwrap();
        let node_dirs = [tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap()];
        let stores = ["node-a", "node-b"].map(|node| {
            let journal_dir = &node_dirs[usize::from(node == "node-b")];
            NodeStore::resolve(
                journal_dir.path().join("melin.journal"),
                Some(snapshots.path().join(format!("{node}.snapshot"))),
            )
            .unwrap()
        });
        for store in &stores {
            build_lineage(store);
        }

        for (store, node) in stores.iter().zip(["node-a", "node-b"]) {
            let archived = archive_local_lineage(store, ArchiveReason::Divergent)
                .unwrap()
                .unwrap();
            let journal_parent = parent_dir(store.journal());
            assert_eq!(
                archived,
                ArchivedLineage {
                    journal_dir: Some(journal_parent.join("melin.journal.divergent.0")),
                    snapshot_dir: Some(
                        snapshots
                            .path()
                            .join(format!("{node}.snapshot.divergent.0"))
                    ),
                }
            );
        }
    }

    /// Only a snapshot, then only a journal: each archive names one side,
    /// and the second skips the index the first took on the other side,
    /// so the two never pose as halves of one resync.
    #[test]
    fn one_sided_archives_still_share_the_index_series() {
        let journal_dir = tempfile::tempdir().unwrap();
        let snapshot_dir = tempfile::tempdir().unwrap();
        let store = NodeStore::resolve(
            journal_dir.path().join("a.journal"),
            Some(snapshot_dir.path().join("a.snapshot")),
        )
        .unwrap();

        build_snapshot(&store);
        let snapshot_only = archive_local_lineage(&store, ArchiveReason::Resync)
            .unwrap()
            .unwrap();
        assert_eq!(
            snapshot_only,
            ArchivedLineage {
                journal_dir: None,
                snapshot_dir: Some(snapshot_dir.path().join("a.snapshot.resync.0")),
            }
        );
        assert!(!journal_dir.path().join("a.journal.resync.0").exists());

        build_journal(store.journal());
        let journal_only = archive_local_lineage(&store, ArchiveReason::Resync)
            .unwrap()
            .unwrap();
        assert_eq!(
            journal_only,
            ArchivedLineage {
                journal_dir: Some(journal_dir.path().join("a.journal.resync.1")),
                snapshot_dir: None,
            }
        );
    }

    /// The snapshot on another filesystem than the journal. Before the
    /// snapshot was archived beside itself, this failed with EXDEV.
    #[test]
    fn a_snapshot_on_another_filesystem_is_archived_there() {
        let journal_dir = tempfile::tempdir().unwrap();
        let Some(second_fs) = crate::test_fs::second_filesystem_dir(journal_dir.path()) else {
            return;
        };
        let snapshot_dir = tempfile::tempdir_in(second_fs).unwrap();
        let store = NodeStore::resolve(
            journal_dir.path().join("a.journal"),
            Some(snapshot_dir.path().join("a.snapshot")),
        )
        .unwrap();
        build_lineage(&store);

        let archived = archive_local_lineage(&store, ArchiveReason::Divergent)
            .unwrap()
            .unwrap();

        assert_eq!(
            archived.snapshot_dir,
            Some(snapshot_dir.path().join("a.snapshot.divergent.0"))
        );
        assert_archived(&store, &archived);
    }

    #[test]
    fn the_lowest_free_index_is_the_first_not_taken() {
        assert_eq!(lowest_free_index(|n| Ok(n < 3)).unwrap(), 3);
        assert_eq!(lowest_free_index(|_| Ok(false)).unwrap(), 0);
        let err = lowest_free_index(|_| Err(io::Error::other("probe"))).unwrap_err();
        assert_eq!(err.to_string(), "probe");
    }
}
