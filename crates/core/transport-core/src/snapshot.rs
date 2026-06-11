//! Application-generic snapshot file format.
//!
//! Framing layout (little-endian):
//!
//! | Field            | Type     | Bytes | Purpose                        |
//! |------------------|----------|-------|--------------------------------|
//! | file_magic       | u32      | 4     | `0x534E4150` ("SNAP")          |
//! | transport_version| u16      | 2     | This file's framing version    |
//! | app_version      | u16      | 2     | `A::APP_VERSION` at save time  |
//! | sequence         | u64      | 8     | Journal sequence at snapshot   |
//! | chain_hash       | [u8; 32] | 32    | BLAKE3 hash chain state        |
//! | app_payload      | var      | var   | Bytes from `A::snapshot`       |
//! | crc32c           | u32      | 4     | CRC32C over everything above   |
//!
//! The transport owns the framing (magic, versions, sequence, chain hash,
//! CRC) and the application owns the payload bytes. Atomic file rename
//! keeps the snapshot crash-safe: the `.tmp` file is fully written and
//! fsynced before the rename.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use melin_app::Application;

use crate::cursors::WireSeq;
use tracing::warn;

const SNAP_MAGIC: u32 = 0x534E_4150;
const TRANSPORT_VERSION: u16 = 1;
const HEADER_SIZE: usize = 4 + 2 + 2 + 8 + 32; // magic + t_ver + a_ver + seq + hash
const CRC_SIZE: usize = 4;
const MAX_SNAPSHOT_SIZE: u64 = 256 * 1024 * 1024;

/// Suffix appended to the snapshot path for the one-deep rollback file.
const PREV_SUFFIX: &str = ".prev";

/// Suffix used for the in-progress write before the atomic publish rename.
const TMP_SUFFIX: &str = ".tmp";

/// Append `suffix` to a snapshot path. Keeps the full filename intact
/// (e.g. `melin.snapshot` → `melin.snapshot.prev`) so operators can run
/// `mv melin.snapshot.prev melin.snapshot` to roll back. Using
/// [`Path::with_extension`] is wrong here — it would replace the existing
/// extension instead of appending, which breaks multi-dot filenames.
fn with_suffix(path: &Path, suffix: &str) -> std::path::PathBuf {
    let mut buf = path.as_os_str().to_owned();
    buf.push(suffix);
    std::path::PathBuf::from(buf)
}

/// Error surfaced by [`save`] and [`load`].
#[derive(Debug)]
pub enum SnapshotError {
    Io(std::io::Error),
    Truncated,
    BadMagic,
    UnsupportedTransportVersion(u16),
    UnsupportedAppVersion(u16),
    ChecksumMismatch { expected: u32, actual: u32 },
    TooLarge(u64),
}

impl std::fmt::Display for SnapshotError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "snapshot I/O: {e}"),
            Self::Truncated => f.write_str("snapshot file truncated"),
            Self::BadMagic => f.write_str("snapshot file has bad magic (not a Melin snapshot)"),
            Self::UnsupportedTransportVersion(v) => {
                write!(
                    f,
                    "snapshot transport version {v} not supported by this build"
                )
            }
            Self::UnsupportedAppVersion(v) => {
                write!(
                    f,
                    "snapshot app payload version {v} not supported by this build"
                )
            }
            Self::ChecksumMismatch { expected, actual } => write!(
                f,
                "snapshot CRC mismatch: expected {expected:#010x}, got {actual:#010x}"
            ),
            Self::TooLarge(size) => write!(
                f,
                "snapshot file size {size} exceeds {MAX_SNAPSHOT_SIZE} byte cap"
            ),
        }
    }
}

impl std::error::Error for SnapshotError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for SnapshotError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

/// Save a snapshot of the current application state to `path`. Writes
/// to `<path>.tmp` first and atomically renames on success — on crash
/// the partial tmp file is discarded on recovery and the previous
/// snapshot (if any) is untouched.
///
/// `journal_sequence` is the recovery resume point — typed [`WireSeq`]
/// because recording any other space (e.g. a ring position) here would
/// make recovery replay already-applied events on top of restored state.
pub fn save<A: Application>(
    app: &A,
    journal_sequence: WireSeq,
    chain_hash: [u8; 32],
    path: &Path,
) -> Result<(), SnapshotError> {
    save_with_limit::<A>(app, journal_sequence, chain_hash, path, MAX_SNAPSHOT_SIZE)
}

/// Internal form of [`save`] taking an explicit size cap. Factored out so
/// tests can exercise the cap without allocating a 256 MiB payload.
fn save_with_limit<A: Application>(
    app: &A,
    journal_sequence: WireSeq,
    chain_hash: [u8; 32],
    path: &Path,
    max_size: u64,
) -> Result<(), SnapshotError> {
    let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);
    // Transport header.
    buf.extend_from_slice(&SNAP_MAGIC.to_le_bytes());
    buf.extend_from_slice(&TRANSPORT_VERSION.to_le_bytes());
    buf.extend_from_slice(&A::APP_VERSION.to_le_bytes());
    buf.extend_from_slice(&journal_sequence.get().to_le_bytes());
    buf.extend_from_slice(&chain_hash);
    // App payload.
    app.snapshot(&mut buf)?;
    // CRC over everything written so far.
    let crc = crc32c::crc32c(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());

    // Refuse to write a file the matching `load` would reject. Without
    // this guard a runaway `Application::snapshot` produces a file that
    // every subsequent recovery fails on.
    if buf.len() as u64 > max_size {
        return Err(SnapshotError::TooLarge(buf.len() as u64));
    }

    let tmp_path = with_suffix(path, TMP_SUFFIX);

    {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;
        file.write_all(&buf)?;
        file.sync_all()?;
    }

    // Resolve the parent dir once. POSIX renames are atomic wrt readers
    // but the directory entries they create/remove are not durable until
    // the parent's metadata is fsynced — without that, a crash can lose
    // the rename on ext4/xfs. Empty parent ("snap" with no separator)
    // denotes CWD; use "." so the open succeeds rather than ENOENT.
    let parent = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        _ => std::path::PathBuf::from("."),
    };

    // Rotate the previous snapshot to `<path>.prev` before clobbering it.
    // Operators rely on this as a one-deep rollback target — see
    // `docs/operations.md` "Snapshot rotation". On first save the source
    // doesn't exist (NotFound) and the rename does nothing. Other errors
    // (e.g. EACCES, ENOSPC) are non-fatal — per the documented best-effort
    // contract we proceed with the save rather than fail it — but the
    // operator loses their rollback point, so surface it as a warning
    // rather than swallowing it.
    let prev_path = with_suffix(path, PREV_SUFFIX);
    let rotated = match std::fs::rename(path, &prev_path) {
        Ok(()) => true,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
        Err(e) => {
            warn!(
                error = %e,
                path = %path.display(),
                prev = %prev_path.display(),
                "snapshot rotation to .prev failed; rollback point lost for this save",
            );
            false
        }
    };

    // Fsync the parent dir between the two renames so the rotation is
    // durable before the publish overwrites `path`. Without this, a crash
    // between the rotation and the publish rename could leave the dir
    // metadata in a state where neither `path` nor `path.prev` is present
    // — the rollback point would be silently lost. Skip the extra fsync
    // when no rotation happened (first save) to avoid a syscall on the
    // common path.
    if rotated {
        File::open(&parent)?.sync_all()?;
    }

    std::fs::rename(&tmp_path, path)?;
    File::open(&parent)?.sync_all()?;

    Ok(())
}

/// Load a snapshot from `path`. Returns the restored application plus
/// the journal sequence and chain hash recorded at save time so the
/// caller can resume the journal from the right spot.
pub fn load<A: Application>(path: &Path) -> Result<(A, u64, [u8; 32]), SnapshotError> {
    let mut file = File::open(path)?;
    let file_size = file.seek(SeekFrom::End(0))?;
    if file_size > MAX_SNAPSHOT_SIZE {
        return Err(SnapshotError::TooLarge(file_size));
    }
    if (file_size as usize) < HEADER_SIZE + CRC_SIZE {
        return Err(SnapshotError::Truncated);
    }
    file.seek(SeekFrom::Start(0))?;

    let mut bytes = Vec::with_capacity(file_size as usize);
    file.read_to_end(&mut bytes)?;

    // The Truncated check above guarantees the header + CRC fit, so each
    // fixed-width slice is the exact size of the destination array and the
    // `try_into` cannot fail.
    let data_end = bytes.len() - CRC_SIZE;
    let expected_crc = u32::from_le_bytes(
        bytes[data_end..data_end + CRC_SIZE]
            .try_into()
            .expect("CRC slice size guaranteed by Truncated check"),
    );
    let actual_crc = crc32c::crc32c(&bytes[..data_end]);
    if expected_crc != actual_crc {
        return Err(SnapshotError::ChecksumMismatch {
            expected: expected_crc,
            actual: actual_crc,
        });
    }

    let magic = u32::from_le_bytes(
        bytes[0..4]
            .try_into()
            .expect("magic slice size fixed by HEADER_SIZE layout"),
    );
    if magic != SNAP_MAGIC {
        return Err(SnapshotError::BadMagic);
    }
    let transport_version = u16::from_le_bytes(
        bytes[4..6]
            .try_into()
            .expect("transport_version slice size fixed by HEADER_SIZE layout"),
    );
    if transport_version != TRANSPORT_VERSION {
        return Err(SnapshotError::UnsupportedTransportVersion(
            transport_version,
        ));
    }
    let app_version = u16::from_le_bytes(
        bytes[6..8]
            .try_into()
            .expect("app_version slice size fixed by HEADER_SIZE layout"),
    );
    if app_version != A::APP_VERSION {
        return Err(SnapshotError::UnsupportedAppVersion(app_version));
    }
    let sequence = u64::from_le_bytes(
        bytes[8..16]
            .try_into()
            .expect("sequence slice size fixed by HEADER_SIZE layout"),
    );
    let mut chain_hash = [0u8; 32];
    chain_hash.copy_from_slice(&bytes[16..48]);

    // App payload occupies everything between the header and the CRC.
    let app = A::restore(&mut &bytes[HEADER_SIZE..data_end])?;

    Ok((app, sequence, chain_hash))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestApp;
    use std::fs::OpenOptions;

    /// Build a syntactically-valid snapshot byte vector with caller-chosen
    /// header fields and trailing app payload. Used by negative-path tests
    /// so each test can vary exactly one field while the CRC stays correct
    /// (i.e. the failure must come from the semantic check, not the CRC).
    fn craft_snapshot(
        magic: u32,
        transport_version: u16,
        app_version: u16,
        sequence: u64,
        chain_hash: [u8; 32],
        app_payload: &[u8],
    ) -> Vec<u8> {
        let mut buf = Vec::with_capacity(HEADER_SIZE + app_payload.len() + CRC_SIZE);
        buf.extend_from_slice(&magic.to_le_bytes());
        buf.extend_from_slice(&transport_version.to_le_bytes());
        buf.extend_from_slice(&app_version.to_le_bytes());
        buf.extend_from_slice(&sequence.to_le_bytes());
        buf.extend_from_slice(&chain_hash);
        buf.extend_from_slice(app_payload);
        let crc = crc32c::crc32c(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        buf
    }

    fn populated_app() -> TestApp {
        let mut a = TestApp::new();
        a.total = 12_345;
        a.ticks = 7;
        a.key_hwm.insert(0xAA, 1);
        a.key_hwm.insert(0xBB, 42);
        a
    }

    #[test]
    fn save_load_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let app = populated_app();

        // [0u8; 32] is `JournaledApp`'s documented "hash chain disabled /
        // not yet initialized" sentinel — see `journaled_app.rs`
        // `unwrap_or([0u8; 32])`. Cover it alongside a populated hash so
        // the sentinel can't silently regress (no special-casing in
        // save/load today, just an intent guard).
        for (label, chain) in [("populated", [0xCDu8; 32]), ("zero sentinel", [0u8; 32])] {
            let path = dir.path().join(format!("snap.{label}"));
            save::<TestApp>(&app, WireSeq::new(999), chain, &path).unwrap();

            let (restored, seq, ch) = load::<TestApp>(&path).unwrap();
            assert_eq!(seq, 999, "{label}");
            assert_eq!(ch, chain, "{label}");
            assert_eq!(restored, app, "{label}");
        }
    }

    #[test]
    fn second_save_rotates_previous_to_prev() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("melin.snapshot");
        let prev_path = dir.path().join("melin.snapshot.prev");

        let app = populated_app();

        // First save — no previous snapshot exists; `.prev` is not created.
        save::<TestApp>(&app, WireSeq::new(1), [0x11; 32], &path).unwrap();
        assert!(path.exists());
        assert!(
            !prev_path.exists(),
            "no .prev file should be created on first save"
        );
        let first_bytes = std::fs::read(&path).unwrap();

        // Second save — previous snapshot must be rotated to .prev verbatim.
        save::<TestApp>(&app, WireSeq::new(2), [0x22; 32], &path).unwrap();
        assert!(path.exists());
        assert!(prev_path.exists(), "second save must produce a .prev file");
        assert_eq!(
            std::fs::read(&prev_path).unwrap(),
            first_bytes,
            ".prev must contain the first snapshot's bytes byte-for-byte"
        );

        // Both files must round-trip independently with their own metadata.
        let (_, seq_curr, hash_curr) = load::<TestApp>(&path).unwrap();
        assert_eq!(seq_curr, 2);
        assert_eq!(hash_curr, [0x22; 32]);

        let (_, seq_prev, hash_prev) = load::<TestApp>(&prev_path).unwrap();
        assert_eq!(seq_prev, 1);
        assert_eq!(hash_prev, [0x11; 32]);
    }

    #[test]
    fn bad_magic_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("snap");
        let bytes = craft_snapshot(
            0xDEAD_BEEF,
            TRANSPORT_VERSION,
            TestApp::APP_VERSION,
            0,
            [0u8; 32],
            &[0u8; 20], // arbitrary payload, matches restore requirements minus dedup map
        );
        std::fs::write(&path, &bytes).unwrap();
        match load::<TestApp>(&path) {
            Err(SnapshotError::BadMagic) => {}
            other => panic!("expected BadMagic, got {other:?}"),
        }
    }

    #[test]
    fn unsupported_transport_version_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("snap");
        let bytes = craft_snapshot(
            SNAP_MAGIC,
            999,
            TestApp::APP_VERSION,
            0,
            [0u8; 32],
            &[0u8; 20],
        );
        std::fs::write(&path, &bytes).unwrap();
        match load::<TestApp>(&path) {
            Err(SnapshotError::UnsupportedTransportVersion(999)) => {}
            other => panic!("expected UnsupportedTransportVersion(999), got {other:?}"),
        }
    }

    #[test]
    fn unsupported_app_version_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("snap");
        // Use a valid TestApp payload (so restore would otherwise succeed)
        // and set the header's app_version to a different value.
        let mut payload = Vec::new();
        populated_app().snapshot(&mut payload).unwrap();
        let bytes = craft_snapshot(
            SNAP_MAGIC,
            TRANSPORT_VERSION,
            TestApp::APP_VERSION + 1,
            0,
            [0u8; 32],
            &payload,
        );
        std::fs::write(&path, &bytes).unwrap();
        match load::<TestApp>(&path) {
            Err(SnapshotError::UnsupportedAppVersion(v)) if v == TestApp::APP_VERSION + 1 => {}
            other => panic!("expected UnsupportedAppVersion, got {other:?}"),
        }
    }

    #[test]
    fn checksum_mismatch_detected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("snap");
        save::<TestApp>(&populated_app(), WireSeq::new(0), [0u8; 32], &path).unwrap();
        // Flip one bit inside the payload region (after the header, before
        // the trailing CRC). The mutated byte recomputes to a different
        // CRC than the one written at save time.
        let bytes = std::fs::read(&path).unwrap();
        let mut mutated = bytes.clone();
        let idx = HEADER_SIZE + 1;
        mutated[idx] ^= 0xFF;
        std::fs::write(&path, &mutated).unwrap();

        match load::<TestApp>(&path) {
            Err(SnapshotError::ChecksumMismatch { .. }) => {}
            other => panic!("expected ChecksumMismatch, got {other:?}"),
        }
    }

    #[test]
    fn truncated_file_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("snap");
        // Anything shorter than HEADER_SIZE + CRC_SIZE trips the truncation
        // guard before CRC/magic are consulted.
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        f.write_all(&[0u8; HEADER_SIZE + CRC_SIZE - 1]).unwrap();
        drop(f);

        match load::<TestApp>(&path) {
            Err(SnapshotError::Truncated) => {}
            other => panic!("expected Truncated, got {other:?}"),
        }
    }

    #[test]
    fn save_rejects_oversize_payload() {
        // `load` caps at MAX_SNAPSHOT_SIZE; `save` must symmetrically
        // refuse to write a file that the matching `load` would reject.
        // A small limit lets us trip the guard without allocating the
        // real 256 MiB cap.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("snap");
        let app = populated_app();

        match save_with_limit::<TestApp>(
            &app,
            WireSeq::new(0),
            [0u8; 32],
            &path,
            /* max_size */ 16,
        ) {
            Err(SnapshotError::TooLarge(_)) => {}
            other => panic!("expected TooLarge, got {other:?}"),
        }

        // Rejection happens before any file write — no `.tmp` or final
        // file should be left behind.
        assert!(
            !path.exists(),
            "save_with_limit must not leave the target file on size-check failure"
        );
        let tmp = path.with_extension("tmp");
        assert!(
            !tmp.exists(),
            "save_with_limit must not leave a .tmp on size-check failure"
        );
    }

    #[test]
    fn too_large_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("snap");
        // Sparse file one byte larger than the cap — set_len avoids
        // actually writing 256 MiB to disk.
        let f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        f.set_len(MAX_SNAPSHOT_SIZE + 1).unwrap();
        drop(f);

        match load::<TestApp>(&path) {
            Err(SnapshotError::TooLarge(size)) if size == MAX_SNAPSHOT_SIZE + 1 => {}
            other => panic!("expected TooLarge, got {other:?}"),
        }
    }
}
