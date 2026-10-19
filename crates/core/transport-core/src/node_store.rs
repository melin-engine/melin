//! Where a node keeps its journal and its snapshot.
//!
//! [`NodeStore`] is the one resolver of both paths. Every role that reads,
//! writes, serves or archives the snapshot (primary boot, replica
//! recovery, the shadow stage, snapshot transfer to a replica, a
//! resync's archive) takes its path from the same store, so no role can
//! look for the snapshot somewhere another role does not write it. The
//! store is built once, when the node starts, and passed down.

use std::fmt;
use std::io;
use std::path::{Path, PathBuf};

use melin_journal::segment::parent_dir;

use crate::snapshot::{PREV_SUFFIX, TMP_SUFFIX, with_suffix};

/// The extension of the snapshot path derived from the journal's when no
/// explicit one is given: `melin.journal` → `melin.snapshot`.
const DERIVED_SNAPSHOT_EXTENSION: &str = "snapshot";

/// A node's journal and snapshot locations, paired.
///
/// Paired rather than a snapshot newtype on its own, because catch-up and
/// archival need both: holding them together means no caller can combine
/// one node's snapshot with another node's journal. The fields are
/// private and [`NodeStore::resolve`] is the only constructor, so a store
/// in hand has passed its start-up checks.
///
/// `PathBuf`s, owned: the store is cloned once per thread that serves the
/// snapshot (the replication senders' workers, the shadow stage), never
/// on the event path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeStore {
    journal: PathBuf,
    snapshot: PathBuf,
}

/// Why [`NodeStore::resolve`] refused a layout. Each is a configuration
/// a node must not start on: the message names the paths involved and
/// what to do.
#[derive(Debug)]
pub enum NodeStoreError {
    /// The snapshot path names no file: it is empty, or ends in `..`, a
    /// separator or `.`.
    SnapshotNamesNoFile { snapshot: PathBuf },
    /// The snapshot's directory does not exist. The shadow stage would
    /// fail every save.
    SnapshotDirMissing { snapshot: PathBuf, dir: PathBuf },
    /// The snapshot's directory is not a directory.
    SnapshotDirNotADirectory { snapshot: PathBuf, dir: PathBuf },
    /// The snapshot path names a directory (or a link to one): boot could
    /// not read it as a snapshot, nor the shadow stage replace it.
    SnapshotIsADirectory { snapshot: PathBuf },
    /// The snapshot path is the journal path: the shadow's first save
    /// would replace the journal.
    SnapshotIsJournal { path: PathBuf },
    /// An explicit snapshot path holds nothing and the node has no
    /// journal, while a snapshot sits at the path derived from the
    /// journal's. The node would start a new history beside the one it
    /// was meant to continue.
    DerivedSnapshotLeftBehind { snapshot: PathBuf, derived: PathBuf },
    /// The layout could not be inspected.
    Io { path: PathBuf, error: io::Error },
}

impl fmt::Display for NodeStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SnapshotNamesNoFile { snapshot } => write!(
                f,
                "snapshot path {} names no file: give --snapshot-path a file name",
                snapshot.display()
            ),
            Self::SnapshotDirMissing { snapshot, dir } => write!(
                f,
                "the snapshot's directory {} does not exist (snapshot path {}): create it, \
                 or set --snapshot-path to a file in an existing directory",
                dir.display(),
                snapshot.display()
            ),
            Self::SnapshotDirNotADirectory { snapshot, dir } => write!(
                f,
                "the snapshot's directory {} is not a directory (snapshot path {}): set \
                 --snapshot-path to a file in an existing directory",
                dir.display(),
                snapshot.display()
            ),
            Self::SnapshotIsADirectory { snapshot } => write!(
                f,
                "snapshot path {} is a directory: set --snapshot-path to a file in it, \
                 e.g. {}",
                snapshot.display(),
                snapshot.join("melin.snapshot").display()
            ),
            Self::SnapshotIsJournal { path } => write!(
                f,
                "the snapshot path is the journal path ({}): the first snapshot would \
                 replace the journal. Set --snapshot-path to another file",
                path.display()
            ),
            Self::DerivedSnapshotLeftBehind { snapshot, derived } => write!(
                f,
                "--snapshot-path {} holds no snapshot and this node has no journal, but a \
                 snapshot exists at {}, the path derived from the journal's: starting would \
                 begin a new history from genesis beside it. Move {} to {} to continue \
                 from it, or remove it to start a new history",
                snapshot.display(),
                derived.display(),
                derived.display(),
                snapshot.display()
            ),
            Self::Io { path, error } => {
                write!(f, "cannot inspect {}: {error}", path.display())
            }
        }
    }
}

impl std::error::Error for NodeStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { error, .. } => Some(error),
            Self::SnapshotNamesNoFile { .. }
            | Self::SnapshotDirMissing { .. }
            | Self::SnapshotDirNotADirectory { .. }
            | Self::SnapshotIsADirectory { .. }
            | Self::SnapshotIsJournal { .. }
            | Self::DerivedSnapshotLeftBehind { .. } => None,
        }
    }
}

impl NodeStore {
    /// Resolve a node's store from its journal path and its explicit
    /// snapshot path, if any. Without one the snapshot is the journal's
    /// path with a `.snapshot` extension.
    ///
    /// Refuses (see [`NodeStoreError`]) a snapshot path that names no
    /// file or names a directory, whose directory is missing or not a
    /// directory, or that is the journal path; and an explicit path that
    /// holds nothing on a node with no journal while the derived path
    /// holds a snapshot.
    /// Reads the filesystem, writes nothing.
    pub fn resolve(
        journal: PathBuf,
        snapshot_path: Option<PathBuf>,
    ) -> Result<Self, NodeStoreError> {
        let derived = journal.with_extension(DERIVED_SNAPSHOT_EXTENSION);
        let explicit = snapshot_path.is_some();
        let snapshot = snapshot_path.unwrap_or_else(|| derived.clone());

        if snapshot.file_name().is_none() {
            return Err(NodeStoreError::SnapshotNamesNoFile { snapshot });
        }
        let dir = parent_dir(&snapshot).to_path_buf();
        // The siblings are spelled by appending to the path as written, so a
        // trailing separator or `.` (`snaps/`, `snaps/.`), which `file_name`
        // and `parent` look through, puts `.tmp` and `.prev` inside `snaps`
        // rather than beside it. Checked on the sibling itself, so it holds
        // for any spelling that would split them from the snapshot.
        if parent_dir(&with_suffix(&snapshot, TMP_SUFFIX)) != dir {
            return Err(NodeStoreError::SnapshotNamesNoFile { snapshot });
        }
        match std::fs::metadata(&dir) {
            Ok(meta) if meta.is_dir() => {}
            Ok(_) => return Err(NodeStoreError::SnapshotDirNotADirectory { snapshot, dir }),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                return Err(NodeStoreError::SnapshotDirMissing { snapshot, dir });
            }
            Err(error) => return Err(NodeStoreError::Io { path: dir, error }),
        }
        // `metadata` follows a link: a link to a directory is as unusable.
        match std::fs::metadata(&snapshot) {
            Ok(meta) if meta.is_dir() => {
                return Err(NodeStoreError::SnapshotIsADirectory { snapshot });
            }
            Ok(_) => {}
            // Nothing there yet (or a dangling link): the first save makes it.
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(NodeStoreError::Io {
                    path: snapshot,
                    error,
                });
            }
        }
        if same_location(&snapshot, &journal)? {
            return Err(NodeStoreError::SnapshotIsJournal { path: snapshot });
        }
        if explicit
            && !try_exists(&snapshot)?
            && !journal_lineage_exists(&journal).map_err(|error| NodeStoreError::Io {
                path: journal.clone(),
                error,
            })?
            && try_exists(&derived)?
        {
            return Err(NodeStoreError::DerivedSnapshotLeftBehind { snapshot, derived });
        }
        Ok(Self { journal, snapshot })
    }

    /// The live journal segment. Archived segments sit beside it.
    pub fn journal(&self) -> &Path {
        &self.journal
    }

    /// The node's snapshot: read at boot, served to replicas, written by
    /// the shadow stage.
    pub fn snapshot(&self) -> &Path {
        &self.snapshot
    }

    /// `<snapshot>.tmp`: where a snapshot is written before it is renamed
    /// over [`snapshot`](Self::snapshot), by the shadow stage and by a
    /// replica receiving one. A sibling, so the rename stays on one
    /// filesystem.
    pub fn snapshot_tmp(&self) -> PathBuf {
        with_suffix(&self.snapshot, TMP_SUFFIX)
    }

    /// `<snapshot>.prev`: the previous snapshot, kept by each save as a
    /// one-deep rollback.
    pub fn snapshot_prev(&self) -> PathBuf {
        with_suffix(&self.snapshot, PREV_SUFFIX)
    }

    /// Whether any journal segment, live or archived, is on disk.
    pub fn journal_lineage_exists(&self) -> io::Result<bool> {
        journal_lineage_exists(&self.journal)
    }

    /// Whether the snapshot and the journal live in the same directory,
    /// compared after canonicalisation (`./a` and `a` are one directory).
    pub(crate) fn snapshot_shares_journal_dir(&self) -> io::Result<bool> {
        let journal_dir = std::fs::canonicalize(parent_dir(&self.journal))?;
        let snapshot_dir = std::fs::canonicalize(parent_dir(&self.snapshot))?;
        Ok(journal_dir == snapshot_dir)
    }
}

/// Whether a journal segment, live or archived, is at `journal`.
fn journal_lineage_exists(journal: &Path) -> io::Result<bool> {
    Ok(journal.try_exists()? || !melin_journal::segment::list_archives(journal)?.is_empty())
}

/// [`Path::try_exists`], its error carrying the path.
fn try_exists(path: &Path) -> Result<bool, NodeStoreError> {
    path.try_exists().map_err(|error| NodeStoreError::Io {
        path: path.to_path_buf(),
        error,
    })
}

/// Whether `snapshot` and `journal` name the same file, however they are
/// spelled. `snapshot`'s directory is known to exist; a journal whose
/// directory does not is in another directory, so another file.
fn same_location(snapshot: &Path, journal: &Path) -> Result<bool, NodeStoreError> {
    if snapshot == journal {
        return Ok(true);
    }
    if snapshot.file_name() != journal.file_name() {
        return Ok(false);
    }
    let canonical_dir = |path: &Path| {
        let dir = parent_dir(path);
        std::fs::canonicalize(dir).map_err(|error| (dir.to_path_buf(), error))
    };
    let snapshot_dir =
        canonical_dir(snapshot).map_err(|(path, error)| NodeStoreError::Io { path, error })?;
    match canonical_dir(journal) {
        Ok(journal_dir) => Ok(journal_dir == snapshot_dir),
        Err((_, error)) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err((path, error)) => Err(NodeStoreError::Io { path, error }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn refusal(journal: &Path, snapshot: Option<&Path>) -> NodeStoreError {
        NodeStore::resolve(journal.to_path_buf(), snapshot.map(Path::to_path_buf))
            .expect_err("the layout is refused")
    }

    #[test]
    fn the_snapshot_is_derived_from_the_journal() {
        let dir = tempfile::tempdir().unwrap();
        let store = NodeStore::resolve(dir.path().join("x.journal"), None).unwrap();
        assert_eq!(store.journal(), dir.path().join("x.journal"));
        assert_eq!(store.snapshot(), dir.path().join("x.snapshot"));
        assert_eq!(store.snapshot_tmp(), dir.path().join("x.snapshot.tmp"));
        assert_eq!(store.snapshot_prev(), dir.path().join("x.snapshot.prev"));
    }

    #[test]
    fn a_journal_with_no_extension_gains_one() {
        let dir = tempfile::tempdir().unwrap();
        let store = NodeStore::resolve(dir.path().join("journal"), None).unwrap();
        assert_eq!(store.snapshot(), dir.path().join("journal.snapshot"));
    }

    /// The default `melin.journal`: a bare name, whose parent is `Some("")`.
    /// Taken as the current directory, not refused as missing.
    #[test]
    fn the_default_bare_journal_resolves_in_the_current_directory() {
        let store = NodeStore::resolve(PathBuf::from("melin.journal"), None).unwrap();
        assert_eq!(store.snapshot(), Path::new("melin.snapshot"));
    }

    #[test]
    fn an_explicit_path_is_used_as_given() {
        let journal_dir = tempfile::tempdir().unwrap();
        let snapshot_dir = tempfile::tempdir().unwrap();
        let snapshot = snapshot_dir.path().join("node-a.snap");
        let store =
            NodeStore::resolve(journal_dir.path().join("a.journal"), Some(snapshot.clone()))
                .unwrap();
        assert_eq!(store.snapshot(), snapshot);
        assert_eq!(store.journal(), journal_dir.path().join("a.journal"));
        assert_eq!(
            store.snapshot_tmp(),
            snapshot_dir.path().join("node-a.snap.tmp")
        );
        assert_eq!(
            store.snapshot_prev(),
            snapshot_dir.path().join("node-a.snap.prev")
        );
    }

    #[test]
    fn a_missing_snapshot_directory_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("nowhere");
        let err = refusal(
            &dir.path().join("a.journal"),
            Some(&missing.join("a.snapshot")),
        );
        assert!(
            matches!(&err, NodeStoreError::SnapshotDirMissing { dir, .. } if *dir == missing),
            "{err:?}"
        );
        assert!(err.to_string().contains(&missing.display().to_string()));
        // A derived path in a missing directory is refused the same way.
        let err = refusal(&missing.join("a.journal"), None);
        assert!(
            matches!(err, NodeStoreError::SnapshotDirMissing { .. }),
            "{err:?}"
        );
    }

    #[test]
    fn a_snapshot_directory_that_is_a_file_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("plain-file");
        std::fs::write(&file, b"").unwrap();
        let err = refusal(
            &dir.path().join("a.journal"),
            Some(&file.join("a.snapshot")),
        );
        assert!(
            matches!(&err, NodeStoreError::SnapshotDirNotADirectory { dir, .. } if *dir == file),
            "{err:?}"
        );
        assert!(err.to_string().contains("is not a directory"), "{err}");
    }

    #[test]
    fn a_snapshot_path_that_is_a_directory_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let snaps = dir.path().join("snaps");
        std::fs::create_dir(&snaps).unwrap();
        let err = refusal(&dir.path().join("a.journal"), Some(&snaps));
        assert!(
            matches!(&err, NodeStoreError::SnapshotIsADirectory { snapshot } if *snapshot == snaps),
            "{err:?}"
        );
        assert!(err.to_string().contains("is a directory"), "{err}");

        // Through a link to a directory, too.
        let link = dir.path().join("snaps-link");
        std::os::unix::fs::symlink(&snaps, &link).unwrap();
        let err = refusal(&dir.path().join("a.journal"), Some(&link));
        assert!(
            matches!(err, NodeStoreError::SnapshotIsADirectory { .. }),
            "{err:?}"
        );

        // A dangling link is nothing there yet: accepted.
        let dangling = dir.path().join("dangling");
        std::os::unix::fs::symlink(dir.path().join("gone"), &dangling).unwrap();
        NodeStore::resolve(dir.path().join("a.journal"), Some(dangling)).unwrap();
    }

    #[test]
    fn a_snapshot_path_naming_no_file_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let err = refusal(&dir.path().join("a.journal"), Some(&dir.path().join("..")));
        assert!(
            matches!(err, NodeStoreError::SnapshotNamesNoFile { .. }),
            "{err:?}"
        );

        // A trailing separator or `.` on a directory that does not exist:
        // `file_name` sees `snaps`, but `.tmp` and `.prev` would be spelled
        // inside the missing `snaps`, so the shadow could never save.
        for spelling in ["snaps/", "snaps/."] {
            let err = refusal(
                &dir.path().join("a.journal"),
                Some(&dir.path().join(spelling)),
            );
            assert!(
                matches!(err, NodeStoreError::SnapshotNamesNoFile { .. }),
                "{spelling}: {err:?}"
            );
        }
    }

    #[test]
    fn a_snapshot_path_equal_to_the_journal_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let journal = dir.path().join("a.journal");
        let err = refusal(&journal, Some(&journal));
        assert!(
            matches!(err, NodeStoreError::SnapshotIsJournal { .. }),
            "{err:?}"
        );
        assert!(err.to_string().contains("is the journal path"), "{err}");

        // Spelled differently, the same file: through a `..` (which a path
        // comparison does not resolve, unlike a `.`) and through a link.
        std::fs::create_dir(dir.path().join("sub")).unwrap();
        let respelled = dir.path().join("sub").join("..").join("a.journal");
        let err = refusal(&journal, Some(&respelled));
        assert!(
            matches!(err, NodeStoreError::SnapshotIsJournal { .. }),
            "{err:?}"
        );
        std::os::unix::fs::symlink(dir.path(), dir.path().join("link")).unwrap();
        let err = refusal(&journal, Some(&dir.path().join("link").join("a.journal")));
        assert!(
            matches!(err, NodeStoreError::SnapshotIsJournal { .. }),
            "{err:?}"
        );

        // The same file name in another directory is another file, also
        // when the journal's directory does not exist yet.
        let other = tempfile::tempdir().unwrap();
        NodeStore::resolve(journal.clone(), Some(other.path().join("a.journal"))).unwrap();
        NodeStore::resolve(
            dir.path().join("not-yet").join("a.journal"),
            Some(journal.clone()),
        )
        .unwrap();

        // A journal already ending in `.snapshot` derives itself.
        let err = refusal(&dir.path().join("a.snapshot"), None);
        assert!(
            matches!(err, NodeStoreError::SnapshotIsJournal { .. }),
            "{err:?}"
        );
    }

    /// An operator adding `--snapshot-path` to a node on the snapshot-only
    /// upgrade layout: the snapshot is at the derived path, nothing at the
    /// new one, and no journal. Refused, naming both paths.
    #[test]
    fn a_derived_snapshot_left_behind_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let snapshot_dir = tempfile::tempdir().unwrap();
        let journal = dir.path().join("a.journal");
        let derived = dir.path().join("a.snapshot");
        let explicit = snapshot_dir.path().join("a.snapshot");
        std::fs::write(&derived, b"old").unwrap();

        let err = refusal(&journal, Some(&explicit));
        assert!(
            matches!(&err, NodeStoreError::DerivedSnapshotLeftBehind { snapshot, derived: d }
                if *snapshot == explicit && *d == derived),
            "{err:?}"
        );
        let message = err.to_string();
        assert!(
            message.contains(&explicit.display().to_string()),
            "{message}"
        );
        assert!(
            message.contains(&derived.display().to_string()),
            "{message}"
        );
    }

    #[test]
    fn an_explicit_empty_path_is_accepted_when_nothing_is_left_behind() {
        let dir = tempfile::tempdir().unwrap();
        let snapshot_dir = tempfile::tempdir().unwrap();
        let journal = dir.path().join("a.journal");
        let derived = dir.path().join("a.snapshot");
        let explicit = snapshot_dir.path().join("a.snapshot");

        // No derived file: a first boot.
        NodeStore::resolve(journal.clone(), Some(explicit.clone())).unwrap();

        // A journal present: the node continues its journal, and the
        // derived file is a leftover recovery never reads.
        std::fs::write(&derived, b"old").unwrap();
        std::fs::write(&journal, b"").unwrap();
        NodeStore::resolve(journal.clone(), Some(explicit.clone())).unwrap();

        // Only an archived segment: still a journal lineage.
        std::fs::remove_file(&journal).unwrap();
        std::fs::write(melin_journal::segment::archive_path(&journal, 1), b"").unwrap();
        NodeStore::resolve(journal.clone(), Some(explicit.clone())).unwrap();

        // A snapshot at the explicit path: the node continues from it.
        std::fs::remove_file(melin_journal::segment::archive_path(&journal, 1)).unwrap();
        std::fs::write(&explicit, b"new").unwrap();
        NodeStore::resolve(journal, Some(explicit)).unwrap();
    }

    /// The siblings the store names are the files `snapshot::save` actually
    /// writes: `.prev` after a second save, and `.tmp` (never left behind
    /// by a successful save, so checked by name against what `save` opened).
    #[test]
    fn the_siblings_are_the_names_save_writes() {
        use crate::cursors::WireSeq;
        use crate::test_support::TestApp;

        let dir = tempfile::tempdir().unwrap();
        let store = NodeStore::resolve(dir.path().join("j.journal"), None).unwrap();
        let save = || {
            crate::snapshot::save::<TestApp>(
                &TestApp::new(),
                WireSeq::new(1),
                [0; 32],
                crate::lineage::Lineage::GENESIS,
                None,
                store.snapshot(),
            )
            .unwrap();
        };
        save();
        save();
        let mut names: Vec<PathBuf> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        names.sort();
        assert_eq!(
            names,
            [store.snapshot().to_path_buf(), store.snapshot_prev()]
        );

        // A save that cannot publish leaves its temp file behind: make both
        // renames fail by putting a non-empty directory at the snapshot
        // path and at `.prev` (a rename onto one is refused).
        for occupied in [store.snapshot().to_path_buf(), store.snapshot_prev()] {
            std::fs::remove_file(&occupied).unwrap();
            std::fs::create_dir(&occupied).unwrap();
            std::fs::write(occupied.join("occupant"), b"").unwrap();
        }
        let failed = crate::snapshot::save::<TestApp>(
            &TestApp::new(),
            WireSeq::new(1),
            [0; 32],
            crate::lineage::Lineage::GENESIS,
            None,
            store.snapshot(),
        );
        assert!(failed.is_err());
        assert!(
            store.snapshot_tmp().is_file(),
            "the save wrote the store's temp name"
        );
    }

    #[test]
    fn the_lineage_check_sees_live_and_archived_segments() {
        let dir = tempfile::tempdir().unwrap();
        let store = NodeStore::resolve(dir.path().join("a.journal"), None).unwrap();
        assert!(!store.journal_lineage_exists().unwrap());
        std::fs::write(
            melin_journal::segment::archive_path(store.journal(), 1),
            b"",
        )
        .unwrap();
        assert!(store.journal_lineage_exists().unwrap());
    }

    #[test]
    fn the_shared_directory_check_canonicalises() {
        let dir = tempfile::tempdir().unwrap();
        let other = tempfile::tempdir().unwrap();
        let same = NodeStore::resolve(
            dir.path().join("a.journal"),
            Some(dir.path().join(".").join("b.snap")),
        )
        .unwrap();
        assert!(same.snapshot_shares_journal_dir().unwrap());
        let apart = NodeStore::resolve(
            dir.path().join("a.journal"),
            Some(other.path().join("b.snap")),
        )
        .unwrap();
        assert!(!apart.snapshot_shares_journal_dir().unwrap());
    }

    #[test]
    fn every_refusal_is_an_error_with_a_message() {
        let io = NodeStoreError::Io {
            path: PathBuf::from("/x"),
            error: io::Error::other("boom"),
        };
        assert!(std::error::Error::source(&io).is_some());
        assert!(io.to_string().contains("/x"));
        let missing = NodeStoreError::SnapshotNamesNoFile {
            snapshot: PathBuf::from(".."),
        };
        assert!(std::error::Error::source(&missing).is_none());
        assert!(missing.to_string().contains("names no file"));
    }
}
