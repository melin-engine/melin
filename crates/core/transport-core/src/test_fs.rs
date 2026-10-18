//! Test-only discovery of a directory on another filesystem than a given
//! one — see [`second_filesystem_dir`]. Compiled for this crate's tests
//! and, under the `test-utils` feature, for any crate's.

use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

/// Names another mount to try before `/dev/shm`.
const SECOND_FS_ENV: &str = "MELIN_TEST_SECOND_FS";

/// A directory on another filesystem (another `st_dev`) than `first`, for
/// a test that must cross one. The caller makes its own scratch directory
/// inside it (`tempfile::tempdir_in`).
///
/// Tries `MELIN_TEST_SECOND_FS`, then `/dev/shm` (tmpfs on Linux), and
/// takes the first that exists on a device other than `first`'s. A
/// candidate on the same device (a developer building under a tmpfs
/// `/dev/shm`) counts as no second filesystem, never as a failure.
///
/// When none qualifies, panics under `CI`, so CI cannot silently lose the
/// case, and elsewhere prints why and returns `None`: the caller then
/// skips its cross-filesystem case.
///
/// # Panics
///
/// When `first` cannot be inspected, or under `CI` when no candidate
/// qualifies.
pub fn second_filesystem_dir(first: &Path) -> Option<PathBuf> {
    let first_dev = std::fs::metadata(first)
        .unwrap_or_else(|e| panic!("inspect {}: {e}", first.display()))
        .dev();
    // A Vec of at most two candidates, in the order they are tried.
    let candidates: Vec<PathBuf> = std::env::var_os(SECOND_FS_ENV)
        .map(PathBuf::from)
        .into_iter()
        .chain(std::iter::once(PathBuf::from("/dev/shm")))
        .collect();
    let mut rejected = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        match std::fs::metadata(&candidate) {
            Ok(meta) if meta.is_dir() && meta.dev() != first_dev => return Some(candidate),
            Ok(meta) if meta.is_dir() => {
                rejected.push(format!("{}: same filesystem", candidate.display()));
            }
            Ok(_) => rejected.push(format!("{}: not a directory", candidate.display())),
            Err(e) => rejected.push(format!("{}: {e}", candidate.display())),
        }
    }
    let why = format!(
        "no directory on another filesystem than {} ({}); set {SECOND_FS_ENV} to one",
        first.display(),
        rejected.join("; ")
    );
    assert!(std::env::var_os("CI").is_none(), "{why}");
    eprintln!("skipping the cross-filesystem case: {why}");
    None
}
