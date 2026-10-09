//! Injected `fdatasync` failures on a live segment, for tests that need
//! a journal whose device rejected a sync.
//!
//! The failure this stands in for (the kernel reporting a writeback error
//! on `fdatasync`) cannot be provoked from a test without root or a
//! device-mapper target, and the path it drives (the disk thread latches
//! the error, the node stops and reports it as a journal write failure) is
//! too important to leave unexercised end to end. The injected error
//! enters [`crate::SegmentFile::sync`] exactly where the real one would,
//! so everything above it, its classification included, runs unchanged.
//!
//! Keyed by the live segment's path, so tests running in parallel in one
//! process arm only their own journal.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Paths whose next sync fails, one entry per armed failure. A `Vec`
/// over a set: a test arms one path at a time, so the scan is over a
/// handful of entries at most, and arming a path twice must fail two
/// syncs. Behind a `Mutex` because arming and taking happen on different
/// threads (the test's and the journal's disk thread), and neither is
/// hot when anything is armed.
static ARMED: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

/// How many entries [`ARMED`] holds, so a sync with nothing armed (every
/// sync of every other test in the process) skips the lock. `usize`: it
/// counts entries in a `Vec`.
static PENDING: AtomicUsize = AtomicUsize::new(0);

fn armed() -> std::sync::MutexGuard<'static, Vec<PathBuf>> {
    // A test that panicked while holding the lock leaves the list itself
    // intact (every update is a single push or remove), so recover it
    // rather than fail every later test in the process.
    ARMED
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Make the next sync of the live segment at `path` fail with `EIO`.
pub(crate) fn arm(path: &Path) {
    armed().push(path.to_path_buf());
    PENDING.fetch_add(1, Ordering::Release);
}

/// The failure armed for `path`, consumed, or `None` when nothing is
/// armed for it.
pub(crate) fn take(path: &Path) -> Option<io::Error> {
    if PENDING.load(Ordering::Acquire) == 0 {
        return None;
    }
    let mut armed = armed();
    let index = armed.iter().position(|p| p == path)?;
    armed.swap_remove(index);
    PENDING.fetch_sub(1, Ordering::Release);
    Some(io::Error::from_raw_os_error(libc::EIO))
}

#[cfg(test)]
mod tests {
    use crate::{JournalError, SegmentFile};

    /// A refused sync of the live segment is a write failure, not a plain
    /// I/O error: the class the node's exit status reports. One armed
    /// failure fails one sync of that segment and no other.
    #[test]
    fn a_failed_sync_is_a_write_failure() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("armed.journal");
        let other_path = dir.path().join("other.journal");
        let segment = SegmentFile::create_continuing(&path, 1, [0; 32], Some(0)).expect("create");
        let other =
            SegmentFile::create_continuing(&other_path, 1, [0; 32], Some(0)).expect("create");

        super::arm(&path);
        other.sync().expect("another segment's sync is untouched");
        match segment.sync() {
            Err(JournalError::WriteFailed(e)) => assert_eq!(e.raw_os_error(), Some(libc::EIO)),
            other => panic!("expected a write failure, got {other:?}"),
        }
        segment
            .sync()
            .expect("the failure is consumed when it fires");
    }
}
