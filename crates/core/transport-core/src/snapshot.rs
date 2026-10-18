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
//! | epoch            | u64      | 8     | Fencing epoch at snapshot       |
//! | genesis_entries  | u64      | 8     | Entries the lineage's genesis occupies (v3) |
//! | lineage_flags    | u8       | 1     | Which lineage fields are known (v4) |
//! | ack_policy       | u8       | 1     | Ack policy in force at snapshot (v4) |
//! | previous_policy  | u8       | 1     | Policy before the transition to it (v4) |
//! | policy_wire_seq  | u64      | 8     | Wire sequence of that transition (v4) |
//! | app_payload      | var      | var   | Bytes from `A::snapshot`       |
//! | crc32c           | u32      | 4     | CRC32C over everything above   |
//!
//! The transport owns the framing (magic, versions, sequence, chain hash,
//! CRC) and the application owns the payload bytes. Atomic file rename
//! keeps the snapshot crash-safe: the `.tmp` file is fully written and
//! fsynced before the rename.
//!
//! `genesis_entries` is the journal header's lineage metadata of the
//! same name (see `melin_journal::FileHeaderInfo::genesis_entries`),
//! carried so a node that boots from the snapshot alone — no journal
//! segment left to read it from — starts its new segment with it.
//!
//! `ack_policy` is the policy the lineage's newest `AckPolicyChange` at or
//! before the snapshot's sequence put in force (see [`Lineage`]). Replay
//! skips the entries the snapshot covers, and their segments may be gone,
//! so the snapshot is the only record of it a recovery may have. With it
//! go the transition that put it in force (see [`LineagePolicy`]): the
//! policy before it, its entry's wire sequence and whether it was
//! unwitnessed, so a primary starting from the snapshot applies that
//! transition again rather than assume it took effect.
//!
//! `lineage_flags` says which of the optional fields hold a value (bit 0:
//! `genesis_entries`; bit 1: `ack_policy` and `policy_wire_seq`; bit 2:
//! `previous_policy`; bit 3: the transition was unwitnessed). Bits 2 and 3
//! need bit 1, and an absent field is written as zero.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::Path;

use melin_app::Application;
use melin_journal::{AckPolicyByte, AckPolicyChange};

use crate::cursors::WireSeq;
use crate::lineage::{Lineage, LineagePolicy};
use tracing::warn;

const SNAP_MAGIC: u32 = 0x534E_4150;
// v2 appended the fencing `epoch` field after `chain_hash`. Writes always
// produce v2; reads accept v1 as well (epoch defaults to 0) so a node
// upgraded in place can still bootstrap from its pre-fencing snapshot —
// a v1 file predates any promotion, so epoch 0 is exact, and journal
// replay re-applies any later `EpochBump`s on top. Unlike the journal's
// strict `FORMAT_VERSION` gate (where replay semantics could silently
// change), the v1 layout is a strict prefix of v2, so the best-effort
// decode is unambiguous.
//
// v3 appended `genesis_entries` after `epoch`, the same way. A v2 (or
// v1) file reads with the genesis length unknown, as the journal reads a
// v15 header; and a snapshot of a lineage whose length is unknown is
// still written as v2, which has no field to hold one, so the layout
// tracks the lineage's journal headers rather than the build.
//
// v4 appended the lineage's ack policy, with a flags byte saying which
// of it and the genesis length are known, so either can be unknown
// without the other. Writes produce v4 whenever the lineage has
// something v2 cannot hold, and v2 otherwise (both fields unknown: a
// lineage from before either existed), keeping the rule that the layout
// tracks the lineage. v3 is read only. The policy's transition (the
// policy before it, its sequence, unwitnessed) is part of v4: no release
// wrote v4 before it, so it needed no version of its own.
const TRANSPORT_VERSION: u16 = 4;
const TRANSPORT_VERSION_V3: u16 = 3;
const TRANSPORT_VERSION_V2: u16 = 2;
const TRANSPORT_VERSION_V1: u16 = 1;
const HEADER_SIZE_V1: usize = 4 + 2 + 2 + 8 + 32; // magic + t_ver + a_ver + seq + hash
const HEADER_SIZE_V2: usize = HEADER_SIZE_V1 + 8; // v2 appends the epoch
const HEADER_SIZE_V3: usize = HEADER_SIZE_V2 + 8; // v3 appends genesis_entries
// v4 appends lineage_flags + ack_policy + previous_policy + policy_wire_seq
const HEADER_SIZE: usize = HEADER_SIZE_V3 + 1 + 1 + 1 + 8;

/// `lineage_flags` bit: the `genesis_entries` field holds a value.
const LINEAGE_GENESIS_KNOWN: u8 = 1 << 0;
/// `lineage_flags` bit: the `ack_policy` and `policy_wire_seq` fields
/// hold a value.
const LINEAGE_ACK_POLICY_KNOWN: u8 = 1 << 1;
/// `lineage_flags` bit: the `previous_policy` field holds a value. Only
/// with [`LINEAGE_ACK_POLICY_KNOWN`].
const LINEAGE_PREVIOUS_KNOWN: u8 = 1 << 2;
/// `lineage_flags` bit: the transition to `ack_policy` was unwitnessed.
/// Only with [`LINEAGE_ACK_POLICY_KNOWN`].
const LINEAGE_UNWITNESSED: u8 = 1 << 3;
/// Every `lineage_flags` bit a save may set.
const LINEAGE_FLAGS: u8 =
    LINEAGE_GENESIS_KNOWN | LINEAGE_ACK_POLICY_KNOWN | LINEAGE_PREVIOUS_KNOWN | LINEAGE_UNWITNESSED;

/// The largest header any version has — what a reader that wants the
/// header alone must read.
pub const MAX_HEADER_SIZE: usize = HEADER_SIZE;
const CRC_SIZE: usize = 4;
const MAX_SNAPSHOT_SIZE: u64 = 256 * 1024 * 1024;

/// Suffix appended to the snapshot path for the one-deep rollback file.
pub(crate) const PREV_SUFFIX: &str = ".prev";

/// Suffix used for the in-progress write before the atomic publish rename.
pub(crate) const TMP_SUFFIX: &str = ".tmp";

/// Append `suffix` to a snapshot path. Keeps the full filename intact
/// (e.g. `melin.snapshot` → `melin.snapshot.prev`) so operators can run
/// `mv melin.snapshot.prev melin.snapshot` to roll back. Using
/// [`Path::with_extension`] is wrong here — it would replace the existing
/// extension instead of appending, which breaks multi-dot filenames.
pub(crate) fn with_suffix(path: &Path, suffix: &str) -> std::path::PathBuf {
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
    ChecksumMismatch {
        expected: u32,
        actual: u32,
    },
    TooLarge(u64),
    /// `A::restore` returned without reading this many bytes of the app
    /// payload: it and the `A::snapshot` that wrote the file disagree
    /// about the layout, so the state it returned is not the one saved.
    UnreadPayload(usize),
    /// A v4 header's lineage fields are not what a save writes: a
    /// reserved `lineage_flags` bit is set, or a field the flags mark
    /// absent is not zero. Refused rather than guessed at, so a header
    /// parses only if it re-encodes to the same bytes.
    MalformedLineage {
        flags: u8,
    },
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
            Self::UnreadPayload(n) => write!(
                f,
                "snapshot app payload has {n} bytes the application did not read \
                 (layout changed without an APP_VERSION bump?)"
            ),
            Self::MalformedLineage { flags } => write!(
                f,
                "snapshot lineage fields are malformed (flags {flags:#04x}: a reserved \
                 bit is set, or a field marked absent is not zero)"
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

/// Parsed snapshot framing header — everything before the app payload.
///
/// The single decoder for the header layout, shared by [`load`] and the
/// replication snapshot-transfer path (`replication::catchup`). Keeping
/// one parse site is what guarantees a layout/version change cannot be
/// applied to one reader and missed in the other.
#[derive(Debug, Clone, Copy)]
pub struct SnapshotHeader {
    /// `A::APP_VERSION` recorded at save time. Validated by [`load`]
    /// (which knows the concrete `A`); informational elsewhere.
    pub app_version: u16,
    /// Journal sequence the snapshot is anchored at.
    pub sequence: u64,
    /// BLAKE3 hash-chain state at the anchor sequence.
    pub chain_hash: [u8; 32],
    /// The lineage at the anchor sequence. Its epoch is `0` for v1
    /// files, which predate fencing — exact, not approximate: a v1
    /// snapshot can only have been taken before any promotion was
    /// journaled. Its ack policy is `None` for v1 to v3 files, which
    /// predate the journaled policy, and so for the same reason exact.
    pub lineage: Lineage,
    /// The lineage's genesis length, as the journal header records it
    /// (`melin_journal::FileHeaderInfo::genesis_entries`). `None` for v1
    /// and v2 files, and v4 files that do not know it — unknown, never
    /// "no genesis".
    pub genesis_entries: Option<u64>,
    /// Bytes the header occupies in this file (version-dependent). The
    /// app payload starts at this offset.
    pub len: usize,
}

impl SnapshotHeader {
    /// Decode and validate the framing header at the start of `bytes`.
    /// Accepts transport versions 1 (no epoch), 2 (no genesis length),
    /// 3 (no ack policy) and 4. Does not touch the app payload or the
    /// trailing CRC — callers own those checks.
    pub fn parse(bytes: &[u8]) -> Result<Self, SnapshotError> {
        if bytes.len() < HEADER_SIZE_V1 {
            return Err(SnapshotError::Truncated);
        }
        // Each fixed-width slice below is length-guaranteed by the checks
        // above, so the `try_into`s cannot fail.
        let magic = u32::from_le_bytes(
            bytes[0..4]
                .try_into()
                .expect("magic slice size fixed by header layout"),
        );
        if magic != SNAP_MAGIC {
            return Err(SnapshotError::BadMagic);
        }
        let transport_version = u16::from_le_bytes(
            bytes[4..6]
                .try_into()
                .expect("transport_version slice size fixed by header layout"),
        );
        let len = match transport_version {
            TRANSPORT_VERSION_V1 => HEADER_SIZE_V1,
            TRANSPORT_VERSION_V2 => HEADER_SIZE_V2,
            TRANSPORT_VERSION_V3 => HEADER_SIZE_V3,
            TRANSPORT_VERSION => HEADER_SIZE,
            other => return Err(SnapshotError::UnsupportedTransportVersion(other)),
        };
        if bytes.len() < len {
            return Err(SnapshotError::Truncated);
        }
        let app_version = u16::from_le_bytes(
            bytes[6..8]
                .try_into()
                .expect("app_version slice size fixed by header layout"),
        );
        let sequence = u64::from_le_bytes(
            bytes[8..16]
                .try_into()
                .expect("sequence slice size fixed by header layout"),
        );
        let mut chain_hash = [0u8; 32];
        chain_hash.copy_from_slice(&bytes[16..48]);
        let epoch = if transport_version == TRANSPORT_VERSION_V1 {
            0
        } else {
            u64::from_le_bytes(
                bytes[48..56]
                    .try_into()
                    .expect("epoch slice size fixed by header layout"),
            )
        };
        let genesis_field = (len >= HEADER_SIZE_V3).then(|| {
            u64::from_le_bytes(
                bytes[56..64]
                    .try_into()
                    .expect("genesis_entries slice size fixed by header layout"),
            )
        });
        let (genesis_entries, ack_policy) = match transport_version {
            TRANSPORT_VERSION => {
                let flags = bytes[64];
                let policy = bytes[65];
                let previous = bytes[66];
                let policy_wire_seq = u64::from_le_bytes(
                    bytes[67..75]
                        .try_into()
                        .expect("policy_wire_seq slice size fixed by header layout"),
                );
                let genesis = genesis_field.expect("v4 holds the genesis_entries field");
                let has = |bit: u8| flags & bit != 0;
                let malformed = flags & !LINEAGE_FLAGS != 0
                    || (!has(LINEAGE_GENESIS_KNOWN) && genesis != 0)
                    || (!has(LINEAGE_ACK_POLICY_KNOWN)
                        && (policy != 0
                            || policy_wire_seq != 0
                            || has(LINEAGE_PREVIOUS_KNOWN | LINEAGE_UNWITNESSED)))
                    || (!has(LINEAGE_PREVIOUS_KNOWN) && previous != 0);
                if malformed {
                    return Err(SnapshotError::MalformedLineage { flags });
                }
                (
                    has(LINEAGE_GENESIS_KNOWN).then_some(genesis),
                    has(LINEAGE_ACK_POLICY_KNOWN).then(|| LineagePolicy {
                        change: AckPolicyChange {
                            policy: AckPolicyByte(policy),
                            unwitnessed: has(LINEAGE_UNWITNESSED),
                        },
                        wire_seq: WireSeq::new(policy_wire_seq),
                        previous: has(LINEAGE_PREVIOUS_KNOWN).then_some(AckPolicyByte(previous)),
                    }),
                )
            }
            // v3 always knows the genesis length; v1 and v2 know neither.
            _ => (genesis_field, None),
        };
        Ok(Self {
            app_version,
            sequence,
            chain_hash,
            lineage: Lineage { epoch, ack_policy },
            genesis_entries,
            len,
        })
    }
}

/// Save a snapshot of the current application state to `path`. Writes
/// to `<path>.tmp` first and atomically renames on success — on crash
/// the partial tmp file is discarded on recovery and the previous
/// snapshot (if any) is untouched. The file is written back as it is
/// written, a few MiB at a time, so a large snapshot never leaves more
/// than that dirty in the page cache.
///
/// `journal_sequence` is the recovery resume point — typed [`WireSeq`]
/// because recording any other space (e.g. a ring position) here would
/// make recovery replay already-applied events on top of restored state.
///
/// `lineage` is the lineage at `journal_sequence`: the epoch and the ack
/// policy in force there.
///
/// `genesis_entries` is the lineage's genesis length from the journal
/// header. A lineage that knows neither it nor an ack policy writes a v2
/// file, which has a field for neither; any other a v4 file.
///
/// Returns the size of the file written, in bytes: the shadow stage logs
/// it with each save, so an operator sees what a save costs. `u64`, the
/// type of a file length and of the size cap.
pub fn save<A: Application>(
    app: &A,
    journal_sequence: WireSeq,
    chain_hash: [u8; 32],
    lineage: Lineage,
    genesis_entries: Option<u64>,
    path: &Path,
) -> Result<u64, SnapshotError> {
    save_with_limit::<A>(
        app,
        journal_sequence,
        chain_hash,
        lineage,
        genesis_entries,
        path,
        SaveLimits::DEFAULT,
    )
}

/// How much of a snapshot's write may be dirty in the page cache at once.
///
/// 4 MiB: large enough that the two `sync_file_range` calls a chunk
/// costs are noise beside the chunk's own device time, and that the
/// chunk in flight spans several of the block layer's largest requests,
/// so the device sees a deep sequential stream; small enough that the
/// two chunks a save leaves dirty or under writeback at once (see
/// [`write_bounded`]) sit far below any sane `vm.dirty_background_bytes`
/// (the LAN bench's hosts run at 32 MiB, with a 64 MiB hard limit). A
/// multiple of every page size Linux uses, so chunks start on page
/// boundaries and no page is in two chunks.
const WRITEBACK_CHUNK: NonZeroUsize = NonZeroUsize::new(4 * 1024 * 1024).expect("non-zero");

/// The bounds [`save_with_limit`] writes under. Parameters rather than
/// constants so tests can reach the size cap and cross chunk boundaries
/// with small payloads.
#[derive(Debug, Clone, Copy)]
struct SaveLimits {
    /// The largest file a save writes, in bytes: `load`'s cap.
    max_size: u64,
    /// The writeback chunk. `NonZeroUsize`: a zero chunk splits nothing,
    /// and `slice::chunks` panics on it.
    chunk: NonZeroUsize,
}

impl SaveLimits {
    const DEFAULT: Self = Self {
        max_size: MAX_SNAPSHOT_SIZE,
        chunk: WRITEBACK_CHUNK,
    };
}

/// What [`write_bounded`] writes to: a file whose byte ranges it can push
/// to the device. A trait, statically dispatched, so a test can record
/// the writeback calls a save makes, which no in-process test could
/// otherwise see; [`File`] is the one production implementation.
trait WritebackTarget: Write {
    /// Start writing back `[offset, offset + len)` without waiting.
    fn start_writeback(&self, offset: u64, len: NonZeroU64) -> std::io::Result<()>;
    /// Write back `[offset, offset + len)` and wait until it is on the device.
    fn writeback_and_wait(&self, offset: u64, len: NonZeroU64) -> std::io::Result<()>;
}

impl WritebackTarget for File {
    fn start_writeback(&self, offset: u64, len: NonZeroU64) -> std::io::Result<()> {
        melin_journal::segment::start_writeback(self, offset, len)
    }

    fn writeback_and_wait(&self, offset: u64, len: NonZeroU64) -> std::io::Result<()> {
        melin_journal::segment::writeback_and_wait(self, offset, len)
    }
}

/// Write `buf` to the start of `file` (opened empty) with no more than two
/// chunks of it dirty or under writeback at any moment, so a large
/// snapshot cannot fill the page cache with dirty pages and push every
/// other writer on the host, the journal included, into the kernel's
/// dirty-page throttling.
///
/// After writing chunk N it starts chunk N's writeback without waiting
/// (`SYNC_FILE_RANGE_WRITE`), then waits for chunk N-1's to finish
/// (`WAIT_BEFORE | WRITE | WAIT_AFTER`, which also writes and waits for
/// any of its pages the first call skipped because they were already
/// under writeback). Starting N before waiting on N-1 keeps the device
/// busy across the wait, so the save streams rather than stopping at
/// every chunk. While chunk N+1 is written, only N (under writeback) and
/// N+1 (dirty) are outstanding.
///
/// The save is still device-bound, as it was with one `write_all` and a
/// `sync_all` that wrote everything at the end; what bounding adds is two
/// syscalls a chunk and a shallower device queue (one chunk in flight
/// rather than the whole file). That is the right trade even though the
/// shadow stalls ingress if a save outlasts the input ring's headroom:
/// an unbounded save makes the journal thread, on the hot path, wait in
/// `balance_dirty_pages` once global dirty memory passes the limit,
/// which costs every in-flight request its latency, where a slightly
/// longer save costs nothing unless it outlasts that headroom.
///
/// Not a durability point: the caller's `sync_all` still writes the
/// metadata and flushes the device's cache.
fn write_bounded<F: WritebackTarget>(
    file: &mut F,
    buf: &[u8],
    chunk: NonZeroUsize,
) -> std::io::Result<()> {
    // The chunk written before this one: its offset and length.
    let mut previous: Option<(u64, NonZeroU64)> = None;
    let mut offset: u64 = 0;
    for piece in buf.chunks(chunk.get()) {
        // `write_all`: a short write is retried, never taken for the chunk.
        file.write_all(piece)?;
        let len = NonZeroU64::new(piece.len() as u64).expect("`chunks` yields no empty slice");
        file.start_writeback(offset, len)?;
        if let Some((prev_offset, prev_len)) = previous {
            file.writeback_and_wait(prev_offset, prev_len)?;
        }
        previous = Some((offset, len));
        offset += len.get();
    }
    Ok(())
}

/// Internal form of [`save`] taking explicit [`SaveLimits`]. Factored out
/// so tests can exercise the cap without allocating a 256 MiB payload,
/// and the writeback chunking with a payload of a few bytes.
fn save_with_limit<A: Application>(
    app: &A,
    journal_sequence: WireSeq,
    chain_hash: [u8; 32],
    lineage: Lineage,
    genesis_entries: Option<u64>,
    path: &Path,
    limits: SaveLimits,
) -> Result<u64, SnapshotError> {
    let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);
    let v4 = genesis_entries.is_some() || lineage.ack_policy.is_some();
    let transport_version = if v4 {
        TRANSPORT_VERSION
    } else {
        TRANSPORT_VERSION_V2
    };
    // Transport header.
    buf.extend_from_slice(&SNAP_MAGIC.to_le_bytes());
    buf.extend_from_slice(&transport_version.to_le_bytes());
    buf.extend_from_slice(&A::APP_VERSION.to_le_bytes());
    buf.extend_from_slice(&journal_sequence.get().to_le_bytes());
    buf.extend_from_slice(&chain_hash);
    buf.extend_from_slice(&lineage.epoch.to_le_bytes());
    if v4 {
        let mut flags = 0u8;
        if genesis_entries.is_some() {
            flags |= LINEAGE_GENESIS_KNOWN;
        }
        if let Some(policy) = lineage.ack_policy {
            flags |= LINEAGE_ACK_POLICY_KNOWN;
            if policy.previous.is_some() {
                flags |= LINEAGE_PREVIOUS_KNOWN;
            }
            if policy.change.unwitnessed {
                flags |= LINEAGE_UNWITNESSED;
            }
        }
        // An absent field is written as zero (see `SnapshotHeader::parse`).
        buf.extend_from_slice(&genesis_entries.unwrap_or(0).to_le_bytes());
        buf.push(flags);
        buf.push(lineage.ack_policy.map_or(0, |p| p.policy().0));
        buf.push(
            lineage
                .ack_policy
                .and_then(|p| p.previous)
                .map_or(0, |p| p.0),
        );
        buf.extend_from_slice(
            &lineage
                .ack_policy
                .map_or(0, |p| p.wire_seq.get())
                .to_le_bytes(),
        );
    }
    // App payload.
    app.snapshot(&mut buf)?;
    // CRC over everything written so far.
    let crc = crc32c::crc32c(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());

    // Refuse to write a file the matching `load` would reject. Without
    // this guard a runaway `Application::snapshot` produces a file that
    // every subsequent recovery fails on.
    let size = buf.len() as u64;
    if size > limits.max_size {
        return Err(SnapshotError::TooLarge(size));
    }

    let tmp_path = with_suffix(path, TMP_SUFFIX);

    {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;
        write_bounded(&mut file, &buf, limits.chunk)?;
        file.sync_all()?;
    }

    // Resolve the parent dir once. POSIX renames are atomic wrt readers
    // but the directory entries they create/remove are not durable until
    // the parent's metadata is fsynced — without that, a crash can lose
    // the rename on ext4/xfs. Empty parent ("snap" with no separator)
    // denotes CWD (see `parent_dir`).
    use melin_journal::segment::{fsync_dir, parent_dir};
    let parent = parent_dir(path);

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
        fsync_dir(parent)?;
    }

    std::fs::rename(&tmp_path, path)?;
    fsync_dir(parent)?;

    Ok(size)
}

/// Load a snapshot from `path`. Returns the restored application with the
/// snapshot's [`SnapshotHeader`]: the journal sequence and chain hash to
/// resume the journal from, the lineage to seed the node's with, and the
/// genesis length.
pub fn load<A: Application>(path: &Path) -> Result<(A, SnapshotHeader), SnapshotError> {
    let mut file = File::open(path)?;
    let file_size = file.seek(SeekFrom::End(0))?;
    if file_size > MAX_SNAPSHOT_SIZE {
        return Err(SnapshotError::TooLarge(file_size));
    }
    if (file_size as usize) < HEADER_SIZE_V1 + CRC_SIZE {
        return Err(SnapshotError::Truncated);
    }
    file.seek(SeekFrom::Start(0))?;

    let mut bytes = Vec::with_capacity(file_size as usize);
    file.read_to_end(&mut bytes)?;

    // The Truncated check above guarantees at least a v1 header + CRC, so
    // the CRC slice is the exact size of the destination array and the
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

    // The header is parsed from the bytes before the CRC, so a v2–v4
    // header that would overlap the CRC region (a payload-less file whose
    // size passed the v1 minimum above) reads as truncated, never as
    // fields decoded from the checksum.
    let header = SnapshotHeader::parse(&bytes[..data_end])?;
    if header.app_version != A::APP_VERSION {
        return Err(SnapshotError::UnsupportedAppVersion(header.app_version));
    }

    // App payload occupies everything between the header and the CRC.
    let mut payload = &bytes[header.len..data_end];
    let app = A::restore(&mut payload)?;
    // `restore` must read exactly what `snapshot` wrote. Bytes left over
    // mean the two disagree about the layout (typically a `snapshot` that
    // grew a field without an `APP_VERSION` bump, read back by a `restore`
    // that stops short of it), and the state is not the saved one.
    if !payload.is_empty() {
        return Err(SnapshotError::UnreadPayload(payload.len()));
    }

    Ok((app, header))
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
    /// A v3 file gets a genesis length of 0, a current-version file a
    /// known genesis length of 0 and no ack policy; any other version
    /// gets the v2 layout.
    fn craft_snapshot(
        magic: u32,
        transport_version: u16,
        app_version: u16,
        sequence: u64,
        chain_hash: [u8; 32],
        epoch: u64,
        app_payload: &[u8],
    ) -> Vec<u8> {
        let lineage_fields: Vec<u8> = match transport_version {
            TRANSPORT_VERSION_V3 => vec![0; 8],
            TRANSPORT_VERSION => v4_lineage_fields(0, LINEAGE_GENESIS_KNOWN, 0, 0, 0),
            _ => Vec::new(),
        };
        craft_snapshot_with_lineage_fields(
            magic,
            transport_version,
            app_version,
            sequence,
            chain_hash,
            epoch,
            &lineage_fields,
            app_payload,
        )
    }

    /// A v4 header's bytes after the epoch, in layout order.
    fn v4_lineage_fields(
        genesis: u64,
        flags: u8,
        policy: u8,
        previous: u8,
        policy_wire_seq: u64,
    ) -> Vec<u8> {
        let mut fields = genesis.to_le_bytes().to_vec();
        fields.extend_from_slice(&[flags, policy, previous]);
        fields.extend_from_slice(&policy_wire_seq.to_le_bytes());
        fields
    }

    /// [`craft_snapshot`] with the bytes after the epoch given verbatim.
    #[allow(clippy::too_many_arguments)]
    fn craft_snapshot_with_lineage_fields(
        magic: u32,
        transport_version: u16,
        app_version: u16,
        sequence: u64,
        chain_hash: [u8; 32],
        epoch: u64,
        lineage_fields: &[u8],
        app_payload: &[u8],
    ) -> Vec<u8> {
        let mut buf = Vec::with_capacity(HEADER_SIZE + app_payload.len() + CRC_SIZE);
        buf.extend_from_slice(&magic.to_le_bytes());
        buf.extend_from_slice(&transport_version.to_le_bytes());
        buf.extend_from_slice(&app_version.to_le_bytes());
        buf.extend_from_slice(&sequence.to_le_bytes());
        buf.extend_from_slice(&chain_hash);
        buf.extend_from_slice(&epoch.to_le_bytes());
        buf.extend_from_slice(lineage_fields);
        buf.extend_from_slice(app_payload);
        let crc = crc32c::crc32c(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        buf
    }

    /// A lineage at `epoch` with no ack policy journaled.
    fn at_epoch(epoch: u64) -> Lineage {
        Lineage {
            epoch,
            ack_policy: None,
        }
    }

    /// A lineage policy: `policy` in force since the transition at
    /// `wire_seq` from `previous`.
    fn policy(
        policy: u8,
        unwitnessed: bool,
        wire_seq: u64,
        previous: Option<u8>,
    ) -> Option<LineagePolicy> {
        Some(LineagePolicy {
            change: AckPolicyChange {
                policy: AckPolicyByte(policy),
                unwitnessed,
            },
            wire_seq: WireSeq::new(wire_seq),
            previous: previous.map(AckPolicyByte),
        })
    }

    /// The application's snapshot payload bytes.
    fn payload_of(app: &TestApp) -> Vec<u8> {
        let mut payload = Vec::new();
        app.snapshot(&mut payload).unwrap();
        payload
    }

    fn populated_app() -> TestApp {
        let mut a = TestApp::new();
        a.total = 12_345;
        a.ticks = 7;
        a.per_key_total.insert(0xAA, 1);
        a.per_key_total.insert(0xBB, 42);
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
            let lineage = Lineage {
                epoch: 7,
                ack_policy: policy(1, false, 990, Some(2)),
            };
            save::<TestApp>(&app, WireSeq::new(999), chain, lineage, Some(4), &path).unwrap();

            let (restored, header) = load::<TestApp>(&path).unwrap();
            assert_eq!(header.sequence, 999, "{label}");
            assert_eq!(header.chain_hash, chain, "{label}");
            assert_eq!(header.lineage, lineage, "{label}");
            assert_eq!(header.genesis_entries, Some(4), "{label}");
            assert_eq!(restored, app, "{label}");
        }
    }

    /// Every combination of a known or unknown genesis length and ack
    /// policy (with or without a policy before its transition, witnessed
    /// or not) round-trips: v4 for any that knows one of them, whose flags
    /// say which, and v2 (which has a field for neither) for the one that
    /// knows none. An unknown field reads back as unknown, never as a
    /// default value.
    #[test]
    fn lineage_fields_round_trip_and_only_a_lineage_knowing_neither_writes_v2() {
        let dir = tempfile::tempdir().unwrap();
        let app = populated_app();
        for genesis_entries in [Some(0), Some(12), None] {
            // Byte 0 is `disk`, the value an absent field is written as:
            // the flags, not the byte, must tell them apart.
            for ack_policy in [
                policy(0, false, 1, None),
                policy(3, false, 4, Some(0)),
                policy(0, true, u64::MAX, Some(2)),
                policy(1, true, 2, None),
                None,
            ] {
                let label = format!("{genesis_entries:?} {ack_policy:?}");
                let path = dir.path().join(format!("snap.{label}"));
                let lineage = Lineage {
                    epoch: 3,
                    ack_policy,
                };
                save::<TestApp>(
                    &app,
                    WireSeq::new(5),
                    [0x42; 32],
                    lineage,
                    genesis_entries,
                    &path,
                )
                .unwrap();
                let bytes = std::fs::read(&path).unwrap();
                let version = u16::from_le_bytes([bytes[4], bytes[5]]);
                let knows_one = genesis_entries.is_some() || ack_policy.is_some();
                let (expected_version, expected_len) = if knows_one {
                    (4, HEADER_SIZE)
                } else {
                    (2, HEADER_SIZE_V2)
                };
                assert_eq!(version, expected_version, "{label}");

                let (restored, header) = load::<TestApp>(&path).unwrap();
                assert_eq!(restored, app, "{label}");
                assert_eq!(header.genesis_entries, genesis_entries, "{label}");
                assert_eq!(header.lineage, lineage, "{label}");
                assert_eq!(header.sequence, 5, "{label}");
                assert_eq!(header.len, expected_len, "{label}");
            }
        }
    }

    /// The v4 header bytes, pinned: a layout change has to change this
    /// test, not slip through a round trip that reads what it wrote.
    #[test]
    fn v4_header_golden_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("snap");
        let lineage = Lineage {
            epoch: 0x0102,
            ack_policy: policy(3, true, 0x0908, Some(2)),
        };
        save::<TestApp>(
            &TestApp::new(),
            WireSeq::new(0x0A0B),
            [0xCC; 32],
            lineage,
            Some(0x07),
            &path,
        )
        .unwrap();
        let bytes = std::fs::read(&path).unwrap();
        let mut expected = Vec::new();
        expected.extend_from_slice(&SNAP_MAGIC.to_le_bytes());
        expected.extend_from_slice(&[4, 0]); // transport version
        expected.extend_from_slice(&TestApp::APP_VERSION.to_le_bytes());
        expected.extend_from_slice(&[0x0B, 0x0A, 0, 0, 0, 0, 0, 0]); // sequence
        expected.extend_from_slice(&[0xCC; 32]); // chain hash
        expected.extend_from_slice(&[0x02, 0x01, 0, 0, 0, 0, 0, 0]); // epoch
        expected.extend_from_slice(&[0x07, 0, 0, 0, 0, 0, 0, 0]); // genesis_entries
        expected.push(0b1111); // lineage_flags: all known, unwitnessed
        expected.push(3); // ack_policy
        expected.push(2); // previous_policy
        expected.extend_from_slice(&[0x08, 0x09, 0, 0, 0, 0, 0, 0]); // policy_wire_seq
        assert_eq!(&bytes[..HEADER_SIZE], &expected[..]);
        assert_eq!(HEADER_SIZE, 75);

        // A lineage whose first change is its policy: no previous, and
        // witnessed, so those flags and the previous byte stay clear.
        let lineage = Lineage {
            epoch: 0,
            ack_policy: policy(1, false, 0x05, None),
        };
        save::<TestApp>(
            &TestApp::new(),
            WireSeq::new(9),
            [0; 32],
            lineage,
            None,
            &path,
        )
        .unwrap();
        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(
            &bytes[HEADER_SIZE_V3..HEADER_SIZE],
            &[0b10, 1, 0, 0x05, 0, 0, 0, 0, 0, 0, 0][..]
        );
    }

    /// A v4 header whose lineage fields a save could not have written —
    /// a reserved flag bit, or a value behind a cleared flag — is
    /// refused, by the shared parser and so by `load`.
    #[test]
    fn malformed_v4_lineage_fields_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("snap");
        let payload = payload_of(&populated_app());
        for (label, genesis, flags, policy, previous, policy_seq) in [
            ("reserved flag bit", 0, 0b1_0000, 0, 0, 0),
            ("high reserved bit", 0, 0b1000_0011, 0, 0, 1),
            ("genesis behind a cleared flag", 5, 0b10, 1, 0, 1),
            ("policy behind a cleared flag", 0, 0b01, 2, 0, 0),
            ("policy seq behind a cleared flag", 0, 0b01, 0, 0, 3),
            ("previous behind a cleared flag", 0, 0b11, 1, 2, 3),
            ("previous without a policy", 0, 0b101, 0, 0, 0),
            ("unwitnessed without a policy", 0, 0b1001, 0, 0, 0),
        ] {
            let fields = v4_lineage_fields(genesis, flags, policy, previous, policy_seq);
            let bytes = craft_snapshot_with_lineage_fields(
                SNAP_MAGIC,
                TRANSPORT_VERSION,
                TestApp::APP_VERSION,
                9,
                [0; 32],
                1,
                &fields,
                &payload,
            );
            match SnapshotHeader::parse(&bytes) {
                Err(SnapshotError::MalformedLineage { flags: got }) => {
                    assert_eq!(got, flags, "{label}")
                }
                other => panic!("{label}: expected MalformedLineage, got {other:?}"),
            }
            std::fs::write(&path, &bytes).unwrap();
            assert!(
                matches!(
                    load::<TestApp>(&path),
                    Err(SnapshotError::MalformedLineage { .. })
                ),
                "{label}"
            );
        }
        // The message names the flags, for an operator reading the log.
        assert!(
            SnapshotError::MalformedLineage { flags: 0x84 }
                .to_string()
                .contains("0x84")
        );
    }

    /// A v3 snapshot — what releases before v4 wrote for a lineage that
    /// knows its genesis length — still loads, with its genesis length,
    /// and with no ack policy: exact, as v3 predates the journaled policy.
    #[test]
    fn v3_snapshot_loads_with_no_ack_policy() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("snap.v3");
        let app = populated_app();
        let bytes = craft_snapshot_with_lineage_fields(
            SNAP_MAGIC,
            TRANSPORT_VERSION_V3,
            TestApp::APP_VERSION,
            44,
            [0x5A; 32],
            2,
            &12u64.to_le_bytes(),
            &payload_of(&app),
        );
        std::fs::write(&path, &bytes).unwrap();

        let (restored, header) = load::<TestApp>(&path).unwrap();
        assert_eq!(restored, app);
        assert_eq!(header.sequence, 44);
        assert_eq!(header.chain_hash, [0x5A; 32]);
        assert_eq!(header.lineage, at_epoch(2));
        assert_eq!(header.genesis_entries, Some(12));
        assert_eq!(header.len, HEADER_SIZE_V3);
    }

    #[test]
    fn second_save_rotates_previous_to_prev() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("melin.snapshot");
        let prev_path = dir.path().join("melin.snapshot.prev");

        let app = populated_app();

        // First save — no previous snapshot exists; `.prev` is not created.
        let first_size = save::<TestApp>(
            &app,
            WireSeq::new(1),
            [0x11; 32],
            at_epoch(0),
            Some(0),
            &path,
        )
        .unwrap();
        assert!(path.exists());
        assert!(
            !prev_path.exists(),
            "no .prev file should be created on first save"
        );
        let first_bytes = std::fs::read(&path).unwrap();
        assert_eq!(
            first_size,
            first_bytes.len() as u64,
            "save returns the size of the file it wrote"
        );

        // Second save — previous snapshot must be rotated to .prev verbatim.
        // A smaller state, so the returned size is the new file's, not
        // the one it replaced.
        let second_size = save::<TestApp>(
            &TestApp::new(),
            WireSeq::new(2),
            [0x22; 32],
            at_epoch(0),
            Some(0),
            &path,
        )
        .unwrap();
        assert!(path.exists());
        assert_eq!(second_size, std::fs::metadata(&path).unwrap().len());
        assert!(
            second_size < first_size,
            "an empty state's snapshot is smaller than a populated one's"
        );
        assert!(prev_path.exists(), "second save must produce a .prev file");
        assert_eq!(
            std::fs::read(&prev_path).unwrap(),
            first_bytes,
            ".prev must contain the first snapshot's bytes byte-for-byte"
        );

        // Both files must round-trip independently with their own metadata.
        let (_, curr) = load::<TestApp>(&path).unwrap();
        assert_eq!(curr.sequence, 2);
        assert_eq!(curr.chain_hash, [0x22; 32]);

        let (_, prev) = load::<TestApp>(&prev_path).unwrap();
        assert_eq!(prev.sequence, 1);
        assert_eq!(prev.chain_hash, [0x11; 32]);
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
            0,
            &[0u8; 20], // arbitrary payload, matches restore requirements minus per-key map
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
            0,
            &[0u8; 20],
        );
        std::fs::write(&path, &bytes).unwrap();
        match load::<TestApp>(&path) {
            Err(SnapshotError::UnsupportedTransportVersion(999)) => {}
            other => panic!("expected UnsupportedTransportVersion(999), got {other:?}"),
        }
    }

    /// Build a v1-layout snapshot (pre-fencing: no epoch field). Mirrors
    /// what a pre-upgrade binary wrote to disk.
    fn craft_snapshot_v1(sequence: u64, chain_hash: [u8; 32], app_payload: &[u8]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(HEADER_SIZE_V1 + app_payload.len() + CRC_SIZE);
        buf.extend_from_slice(&SNAP_MAGIC.to_le_bytes());
        buf.extend_from_slice(&TRANSPORT_VERSION_V1.to_le_bytes());
        buf.extend_from_slice(&TestApp::APP_VERSION.to_le_bytes());
        buf.extend_from_slice(&sequence.to_le_bytes());
        buf.extend_from_slice(&chain_hash);
        buf.extend_from_slice(app_payload);
        let crc = crc32c::crc32c(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        buf
    }

    /// A pre-fencing (v1) snapshot must keep loading after the upgrade —
    /// rejecting it would brick any node whose journal archives were
    /// pruned per the documented retention guidance. The epoch defaults
    /// to 0, which is exact: a v1 file predates any journaled promotion.
    #[test]
    fn v1_snapshot_loads_with_epoch_zero() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("snap.v1");
        let app = populated_app();
        let mut payload = Vec::new();
        app.snapshot(&mut payload).unwrap();
        let bytes = craft_snapshot_v1(777, [0xCD; 32], &payload);
        std::fs::write(&path, &bytes).unwrap();

        let (restored, loaded) = load::<TestApp>(&path).unwrap();
        assert_eq!(restored, app, "v1 payload must restore unchanged");
        assert_eq!(loaded.sequence, 777);
        assert_eq!(loaded.chain_hash, [0xCD; 32]);
        assert_eq!(
            loaded.lineage,
            at_epoch(0),
            "v1 snapshots predate fencing and the journaled policy"
        );

        // The shared header parser agrees with `load` (catch-up uses it
        // directly on the raw bytes).
        let header = SnapshotHeader::parse(&bytes).unwrap();
        assert_eq!(header.sequence, 777);
        assert_eq!(header.chain_hash, [0xCD; 32]);
        assert_eq!(header.lineage, at_epoch(0));
        assert_eq!(header.genesis_entries, None);
        assert_eq!(header.len, HEADER_SIZE_V1);
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
            0,
            &payload,
        );
        std::fs::write(&path, &bytes).unwrap();
        match load::<TestApp>(&path) {
            Err(SnapshotError::UnsupportedAppVersion(v)) if v == TestApp::APP_VERSION + 1 => {}
            other => panic!("expected UnsupportedAppVersion, got {other:?}"),
        }
    }

    #[test]
    fn unread_payload_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("snap");
        // A valid payload with bytes after it: what a `snapshot` that grew
        // a field looks like to a `restore` that was not taught about it.
        let mut payload = Vec::new();
        populated_app().snapshot(&mut payload).unwrap();
        payload.extend_from_slice(&[0xEE; 3]);
        let bytes = craft_snapshot(
            SNAP_MAGIC,
            TRANSPORT_VERSION,
            TestApp::APP_VERSION,
            0,
            [0u8; 32],
            0,
            &payload,
        );
        std::fs::write(&path, &bytes).unwrap();
        match load::<TestApp>(&path) {
            Err(SnapshotError::UnreadPayload(3)) => {}
            other => panic!("expected UnreadPayload(3), got {other:?}"),
        }
    }

    #[test]
    fn checksum_mismatch_detected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("snap");
        save::<TestApp>(
            &populated_app(),
            WireSeq::new(0),
            [0u8; 32],
            at_epoch(0),
            Some(0),
            &path,
        )
        .unwrap();
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
        // Anything shorter than the *v1* header + CRC trips the truncation
        // guard before CRC/magic are consulted — the v1 minimum, not v2,
        // because v1 files remain loadable.
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        f.write_all(&[0u8; HEADER_SIZE_V1 + CRC_SIZE - 1]).unwrap();
        drop(f);

        match load::<TestApp>(&path) {
            Err(SnapshotError::Truncated) => {}
            other => panic!("expected Truncated, got {other:?}"),
        }

        // A v2 header cut off before its epoch field, a v3 one before its
        // genesis length, or a v4 one before its policy's sequence, must also
        // read as truncated, not as a short payload: the file passes the
        // v1-size gate, so the version-aware header parse is what must
        // catch it.
        for (version, cut) in [
            (TRANSPORT_VERSION_V2, HEADER_SIZE_V2 - 4), // mid-epoch
            (TRANSPORT_VERSION_V3, HEADER_SIZE_V3 - 4), // mid-genesis_entries
            (TRANSPORT_VERSION, HEADER_SIZE - 1),       // mid-policy_wire_seq
        ] {
            let bytes = craft_snapshot(
                SNAP_MAGIC,
                version,
                TestApp::APP_VERSION,
                0,
                [0u8; 32],
                0,
                &[],
            );
            // Keep CRC validity out of the way: rewrite the trailing CRC
            // over the truncated prefix so only the length check can fail.
            let mut short = bytes[..cut].to_vec();
            let crc = crc32c::crc32c(&short);
            short.extend_from_slice(&crc.to_le_bytes());
            std::fs::write(&path, &short).unwrap();
            match load::<TestApp>(&path) {
                Err(SnapshotError::Truncated) => {}
                other => panic!("expected Truncated for cut-off v{version} header, got {other:?}"),
            }
        }
    }

    /// A v2 snapshot — what every release before v3 wrote — still loads,
    /// with its epoch, and with the genesis length and ack policy unknown.
    #[test]
    fn v2_snapshot_loads_with_an_unknown_genesis_length() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("snap.v2");
        let app = populated_app();
        let bytes = craft_snapshot(
            SNAP_MAGIC,
            TRANSPORT_VERSION_V2,
            TestApp::APP_VERSION,
            31,
            [0x9A; 32],
            6,
            &payload_of(&app),
        );
        std::fs::write(&path, &bytes).unwrap();

        let (restored, header) = load::<TestApp>(&path).unwrap();
        assert_eq!(restored, app);
        assert_eq!(header.sequence, 31);
        assert_eq!(header.lineage, at_epoch(6));
        assert_eq!(header.genesis_entries, None);
        assert_eq!(header.len, HEADER_SIZE_V2);
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
            at_epoch(0),
            Some(0),
            &path,
            capped_at(16),
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

    /// A snapshot of exactly the cap is written (the cap is inclusive,
    /// as `load`'s is), and the size returned is the file's: the
    /// header, the application payload and the CRC.
    #[test]
    fn save_at_exactly_the_cap_returns_the_file_size() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("snap");
        let app = populated_app();
        let expected = (HEADER_SIZE + payload_of(&app).len() + CRC_SIZE) as u64;

        let size = save_with_limit::<TestApp>(
            &app,
            WireSeq::new(3),
            [0x5A; 32],
            at_epoch(1),
            Some(0),
            &path,
            capped_at(expected),
        )
        .unwrap();
        assert_eq!(size, expected);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), expected);
        let (restored, header) = load::<TestApp>(&path).unwrap();
        assert_eq!(restored, app);
        assert_eq!(header.sequence, 3);

        // One byte under it refuses, and leaves the saved file alone.
        match save_with_limit::<TestApp>(
            &app,
            WireSeq::new(4),
            [0x5A; 32],
            at_epoch(1),
            Some(0),
            &path,
            capped_at(expected - 1),
        ) {
            Err(SnapshotError::TooLarge(size)) => assert_eq!(size, expected),
            other => panic!("expected TooLarge, got {other:?}"),
        }
        assert_eq!(load::<TestApp>(&path).unwrap().1.sequence, 3);
    }

    /// The default limits with the size cap at `max_size`.
    fn capped_at(max_size: u64) -> SaveLimits {
        SaveLimits {
            max_size,
            ..SaveLimits::DEFAULT
        }
    }

    /// An application with no state: its snapshot payload is empty, so
    /// the file is the header and the CRC alone.
    #[derive(Debug, Default, PartialEq, Eq)]
    struct Stateless;

    impl Application for Stateless {
        type Event = crate::test_support::TestEvent;
        type Report = crate::test_support::TestReport;
        type QueryResponse = crate::test_support::TestQuery;
        type Sizing = ();

        const APP_VERSION: u16 = 1;

        fn apply(
            &mut self,
            _event: Self::Event,
            _ctx: &melin_app::ApplyCtx,
            _out: &mut Vec<Self::Report>,
        ) {
        }

        fn build_reject(_event: &Self::Event, _reason: melin_app::RejectReason) -> Self::Report {
            crate::test_support::TestReport { total_after: 0 }
        }

        fn snapshot<W: Write>(&self, _w: &mut W) -> std::io::Result<()> {
            Ok(())
        }

        fn restore<R: Read>(_r: &mut R) -> std::io::Result<Self> {
            Ok(Self)
        }
    }

    /// Save `app` to a fresh path twice (sequences 1 and 2) with every
    /// interesting chunk size: the whole file in exactly one chunk, one
    /// chunk plus one byte, several chunks with a short last one, and a
    /// byte at a time. Each save must write the same bytes as one at the
    /// default chunk (where the file is a single chunk), return the
    /// file's length, round-trip through `load`, and rotate the first
    /// save to `.prev` intact.
    fn assert_chunked_saves_round_trip<A>(app: &A, label: &str)
    where
        A: Application + PartialEq + std::fmt::Debug,
    {
        let dir = tempfile::tempdir().unwrap();
        let save_at = |seq: u64, path: &Path, chunk: NonZeroUsize| {
            save_with_limit::<A>(
                app,
                WireSeq::new(seq),
                [seq as u8; 32],
                at_epoch(seq),
                Some(seq),
                path,
                SaveLimits {
                    chunk,
                    ..SaveLimits::DEFAULT
                },
            )
            .unwrap()
        };
        let reference = |seq: u64| {
            let path = dir.path().join(format!("reference.{seq}"));
            save_at(seq, &path, WRITEBACK_CHUNK);
            std::fs::read(&path).unwrap()
        };
        let (first_ref, second_ref) = (reference(1), reference(2));
        let size = first_ref.len();
        assert!(size < WRITEBACK_CHUNK.get(), "{label}: one default chunk");

        for chunk in [size, size - 1, 16, 1] {
            let label = format!("{label}, {size}-byte file in {chunk}-byte chunks");
            let chunk = NonZeroUsize::new(chunk).unwrap();
            let path = dir.path().join(format!("snap.{chunk}"));
            let prev_path = with_suffix(&path, PREV_SUFFIX);
            for (seq, expected) in [(1, &first_ref), (2, &second_ref)] {
                let written = save_at(seq, &path, chunk);
                let bytes = std::fs::read(&path).unwrap();
                assert_eq!(written, bytes.len() as u64, "{label}: size returned");
                assert_eq!(&bytes, expected, "{label}: bytes of save {seq}");
                let (restored, header) = load::<A>(&path).unwrap();
                assert_eq!(&restored, app, "{label}");
                assert_eq!(header.sequence, seq, "{label}");
            }
            assert_eq!(std::fs::read(&prev_path).unwrap(), first_ref, "{label}");
            assert_eq!(load::<A>(&prev_path).unwrap().1.sequence, 1, "{label}");
            assert!(!with_suffix(&path, TMP_SUFFIX).exists(), "{label}");
        }
    }

    #[test]
    fn chunked_save_round_trips_at_every_chunk_boundary() {
        assert_chunked_saves_round_trip(&populated_app(), "populated");
    }

    #[test]
    fn chunked_save_of_an_empty_app_payload_round_trips() {
        let mut payload = Vec::new();
        Stateless.snapshot(&mut payload).unwrap();
        assert!(payload.is_empty());
        assert_chunked_saves_round_trip(&Stateless, "empty payload");
    }

    /// A payload of several default chunks, not a multiple of one: the
    /// chunk size a real save uses, at the size it exists for.
    #[test]
    fn save_spanning_several_default_chunks_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("snap");
        let mut app = TestApp::new();
        // 16 bytes an entry: a little over two and a half chunks.
        let entries = (WRITEBACK_CHUNK.get() * 5 / 2) / 16 + 7;
        app.per_key_total = (1..=entries as u64).map(|k| (k, k * 3)).collect();
        let size =
            save::<TestApp>(&app, WireSeq::new(8), [1; 32], at_epoch(0), None, &path).unwrap();
        assert!(size > 2 * WRITEBACK_CHUNK.get() as u64);
        assert_ne!(size % WRITEBACK_CHUNK.get() as u64, 0);
        assert_eq!(size, std::fs::metadata(&path).unwrap().len());
        let (restored, header) = load::<TestApp>(&path).unwrap();
        assert_eq!(restored, app);
        assert_eq!(header.sequence, 8);
    }

    /// One call [`write_bounded`] makes on its target, in order.
    #[derive(Debug, PartialEq, Eq)]
    enum Call {
        /// Bytes written: their offset and length.
        Write(u64, usize),
        Start(u64, u64),
        Wait(u64, u64),
    }

    /// A [`WritebackTarget`] that records every call and fails the
    /// writeback call numbered `fail_at` (counting both kinds from zero).
    #[derive(Default)]
    struct Recorder {
        // `RefCell`: the writeback methods take `&self`, like `File`'s.
        calls: std::cell::RefCell<Vec<Call>>,
        written: Vec<u8>,
        fail_at: Option<usize>,
    }

    impl Recorder {
        fn writeback(&self, call: Call) -> std::io::Result<()> {
            let mut calls = self.calls.borrow_mut();
            let n = calls
                .iter()
                .filter(|c| !matches!(c, Call::Write(..)))
                .count();
            calls.push(call);
            if self.fail_at == Some(n) {
                return Err(std::io::Error::other("injected writeback failure"));
            }
            Ok(())
        }
    }

    impl Write for Recorder {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            // A short write every time, so `write_all` must loop.
            let n = buf.len().min(3);
            self.calls
                .get_mut()
                .push(Call::Write(self.written.len() as u64, n));
            self.written.extend_from_slice(&buf[..n]);
            Ok(n)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl WritebackTarget for Recorder {
        fn start_writeback(&self, offset: u64, len: NonZeroU64) -> std::io::Result<()> {
            self.writeback(Call::Start(offset, len.get()))
        }

        fn writeback_and_wait(&self, offset: u64, len: NonZeroU64) -> std::io::Result<()> {
            self.writeback(Call::Wait(offset, len.get()))
        }
    }

    /// Each chunk's writeback starts once all of it is written, and the
    /// previous chunk's is waited on right after, so no more than two
    /// chunks are ever outstanding; the last chunk is left to `sync_all`.
    #[test]
    fn write_bounded_starts_each_chunk_then_waits_on_the_previous() {
        let buf: Vec<u8> = (0..20u8).collect();
        let mut rec = Recorder::default();
        write_bounded(&mut rec, &buf, NonZeroUsize::new(8).unwrap()).unwrap();
        assert_eq!(rec.written, buf);
        let writeback: Vec<Call> = rec
            .calls
            .take()
            .into_iter()
            .filter(|c| !matches!(c, Call::Write(..)))
            .collect();
        assert_eq!(
            writeback,
            [
                Call::Start(0, 8),
                Call::Start(8, 8),
                Call::Wait(0, 8),
                Call::Start(16, 4),
                Call::Wait(8, 8),
            ]
        );
    }

    /// Every byte of a chunk is written before its writeback starts, and
    /// no byte of the next chunk is written before the previous chunk's
    /// wait returns.
    #[test]
    fn write_bounded_interleaves_writes_and_writeback_per_chunk() {
        let buf = [7u8; 10];
        let mut rec = Recorder::default();
        write_bounded(&mut rec, &buf, NonZeroUsize::new(5).unwrap()).unwrap();
        assert_eq!(
            rec.calls.take(),
            [
                Call::Write(0, 3),
                Call::Write(3, 2),
                Call::Start(0, 5),
                Call::Write(5, 3),
                Call::Write(8, 2),
                Call::Start(5, 5),
                Call::Wait(0, 5),
            ]
        );
    }

    /// An empty buffer writes nothing and asks for no writeback.
    #[test]
    fn write_bounded_of_nothing_makes_no_calls() {
        let mut rec = Recorder::default();
        write_bounded(&mut rec, &[], NonZeroUsize::new(4).unwrap()).unwrap();
        assert!(rec.calls.take().is_empty());
    }

    /// A failed writeback call, of either kind, ends the write with its
    /// error and writes nothing more.
    #[test]
    fn write_bounded_propagates_writeback_errors() {
        let buf = [1u8; 12];
        // Calls: Start(0) #0, Start(4) #1, Wait(0) #2, Start(8) #3, Wait(4) #4.
        for (fail_at, written) in [(0, 4), (2, 8), (4, 12)] {
            let mut rec = Recorder {
                fail_at: Some(fail_at),
                ..Recorder::default()
            };
            let err = write_bounded(&mut rec, &buf, NonZeroUsize::new(4).unwrap()).unwrap_err();
            assert_eq!(err.to_string(), "injected writeback failure");
            assert_eq!(rec.written.len(), written, "failing call {fail_at}");
        }
    }

    /// The default chunk is page-aligned under every page size Linux
    /// uses (4, 16 and 64 KiB), so no page straddles two chunks.
    #[test]
    fn default_chunk_is_page_aligned() {
        assert_eq!(WRITEBACK_CHUNK.get() % (64 * 1024), 0);
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
