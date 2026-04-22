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

const SNAP_MAGIC: u32 = 0x534E_4150;
const TRANSPORT_VERSION: u16 = 1;
const HEADER_SIZE: usize = 4 + 2 + 2 + 8 + 32; // magic + t_ver + a_ver + seq + hash
const CRC_SIZE: usize = 4;
const MAX_SNAPSHOT_SIZE: u64 = 256 * 1024 * 1024;

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
pub fn save<A: Application>(
    app: &A,
    journal_sequence: u64,
    chain_hash: [u8; 32],
    path: &Path,
) -> Result<(), SnapshotError> {
    let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);
    // Transport header.
    buf.extend_from_slice(&SNAP_MAGIC.to_le_bytes());
    buf.extend_from_slice(&TRANSPORT_VERSION.to_le_bytes());
    buf.extend_from_slice(&A::APP_VERSION.to_le_bytes());
    buf.extend_from_slice(&journal_sequence.to_le_bytes());
    buf.extend_from_slice(&chain_hash);
    // App payload.
    app.snapshot(&mut buf)?;
    // CRC over everything written so far.
    let crc = crc32c::crc32c(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());

    // Atomic rename via `.tmp`.
    let mut tmp_path = path.to_path_buf();
    let tmp_ext = match path.extension() {
        Some(e) => format!("{}.tmp", e.to_string_lossy()),
        None => "tmp".to_string(),
    };
    tmp_path.set_extension(tmp_ext);

    {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;
        file.write_all(&buf)?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp_path, path)?;
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
