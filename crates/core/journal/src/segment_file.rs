//! The journal's file half: everything that touches the live segment on
//! disk, and nothing that touches the entry stream.
//!
//! [`SegmentFile`] owns the live segment's descriptor, its write
//! position, its pre-allocation horizon, and segment rotation. It knows
//! nothing about sequence numbers, framing, or the hash chain — those
//! live in [`crate::encoder::JournalEncoder`], and the split is exactly
//! the line the pipeline draws between its sequencing thread and its
//! disk thread. Keeping the two halves in separate types makes "which
//! thread owns this field" a property the compiler checks rather than a
//! convention a comment asserts.
//!
//! [`crate::buffered_writer::BufferedWriter`] composes both halves back
//! into the single-threaded writer that recovery, tooling, and tests
//! drive.

use std::fs::{File, OpenOptions};
use std::io::IoSlice;
use std::os::fd::AsFd;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use crate::codec::{self, ENTRY_OFFSET, FILE_HEADER_SIZE, MAX_SECTOR_SIZE};
use crate::error::JournalError;
use crate::prealloc::prealloc_chunk_bytes;

/// Fixed on-disk offset of the first journal entry. Defined in the codec
/// (`ENTRY_OFFSET = MAX_SECTOR_SIZE = 4096`), independent of the
/// device's sector size. Renamed locally for legibility.
const HEADER_OFFSET: u64 = ENTRY_OFFSET;

/// The live journal segment as a file: descriptor, position, allocation
/// horizon, and rotation.
///
/// Every method here is an I/O operation or a query about one. A batch
/// arrives as bytes the caller has already framed — this type never
/// inspects them.
pub struct SegmentFile {
    file: File,
    path: PathBuf,
    // Byte offset of the next entry to be written. Always points at the
    // end of written data; there is no in-memory partial sector, so the
    // on-disk end and the logical end coincide.
    write_pos: u64,
    // Byte offset of the end of pre-allocated space. When `write_pos`
    // approaches this, another `prealloc_chunk_bytes()` is allocated.
    allocated_end: u64,
    // Retries a failed post-rotation directory fsync until it succeeds
    // (see `crate::segment::DirFsyncRetry`). Polled from the flush
    // path — a single branch in steady state.
    dir_fsync_retry: crate::segment::DirFsyncRetry,
}

impl SegmentFile {
    /// Create a fresh segment whose header records `starting_sequence`
    /// and `anchor_hash`. Fails if a file already exists at `path`.
    ///
    /// Deliberately does **not** clear an orphan staging file: this is
    /// also the rotation fallback path, and by then a preparer is
    /// usually alive with a staging file in flight that unlinking would
    /// destroy. Orphan cleanup belongs to the startup entry points only
    /// (see `preparer::cleanup_staging_orphan`).
    pub fn create_continuing(
        path: &Path,
        starting_sequence: u64,
        anchor_hash: [u8; 32],
    ) -> Result<Self, JournalError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;

        // Pre-allocate the first chunk so flushes don't pay extent-tree
        // growth latency for a while. ext4/xfs/btrfs all back this with
        // unwritten extents (no zero-fill cost) on the supported targets.
        let allocated_end = fallocate_chunk(&file, 0)?;

        write_header(&file, starting_sequence, anchor_hash)?;

        // Flush the header durably before returning. Subsequent batch
        // flushes layer on top of a known-good header — a crash before
        // the next user write still leaves a parseable empty journal.
        file.sync_all()?;

        Ok(Self {
            file,
            path: path.to_path_buf(),
            write_pos: HEADER_OFFSET,
            allocated_end,
            dir_fsync_retry: crate::segment::DirFsyncRetry::new(),
        })
    }

    /// Open an existing segment for appending at `valid_end`, the byte
    /// offset immediately past the last entry recovery accepted.
    ///
    /// Returns the decoded file header alongside the open segment: the
    /// caller needs its `anchor_hash` and `starting_sequence` to
    /// resume the entry stream, and reading it is this half's business
    /// because the bytes are on disk.
    pub fn open_append(
        path: &Path,
        valid_end: u64,
    ) -> Result<(Self, codec::FileHeaderInfo), JournalError> {
        crate::preparer::cleanup_staging_orphan(path);
        let file = OpenOptions::new().read(true).write(true).open(path)?;

        // Validate the file header and extract the chain anchor. A
        // header that fails to decode means the file isn't a journal —
        // bail rather than overwrite it.
        let mut header_buf = [0u8; FILE_HEADER_SIZE];
        let n = file.read_at(&mut header_buf, 0)?;
        if n < FILE_HEADER_SIZE {
            return Err(JournalError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "journal file too short to read file header",
            )));
        }
        let info = codec::decode_file_header(&header_buf)?;

        // Truncate down to `valid_end` so any torn-write garbage past
        // it is gone before we resume appending. Without this, the
        // bytes between `valid_end` and the previous file length
        // survive on disk; subsequent readers (or offline tooling)
        // could mistake them for entries if they happen to start with
        // the journal magic. The CRC check would catch the lie; this is
        // the belt-and-braces half — no stale bytes past the valid end
        // in the first place. Truncate then re-fallocate to restore the
        // chunk-ahead allocation; the kernel zero-fills the freshly
        // extended region.
        let pre_truncate_len = file.metadata()?.len();
        if pre_truncate_len > valid_end {
            file.set_len(valid_end)?;
        }
        let allocated_end = fallocate_chunk(&file, valid_end)?;
        file.sync_all()?;

        Ok((
            Self {
                file,
                path: path.to_path_buf(),
                write_pos: valid_end,
                allocated_end,
                dir_fsync_retry: crate::segment::DirFsyncRetry::new(),
            },
            info,
        ))
    }

    /// Write `bytes` at the current position, extending the
    /// pre-allocated region first if the write would run past it.
    ///
    /// Returns once the bytes are in the page cache — durability is
    /// [`sync`](Self::sync)'s job, and keeping the two separate is what
    /// lets the pipeline overlap a stalled flush with further writes.
    pub fn write_batch(&mut self, bytes: &[u8]) -> Result<(), JournalError> {
        if bytes.is_empty() {
            return Ok(());
        }
        self.ensure_allocated(bytes.len() as u64)?;
        write_all_at(&self.file, bytes, self.write_pos)?;
        self.write_pos += bytes.len() as u64;
        Ok(())
    }

    /// Write several already-framed batches at the current position in
    /// one `pwritev`, extending the pre-allocated region first.
    ///
    /// The bytes land as the plain concatenation of `bufs`, exactly as
    /// the equivalent run of [`write_batch`](Self::write_batch) calls
    /// would leave them — this only collapses the syscalls. That is the
    /// point: a backlog that built up behind a slow device costs one
    /// write call to clear instead of one per batch, and those calls sit
    /// *in front of* the `fdatasync`, so they delay durability for every
    /// batch behind them. Measured at 64×4 KiB: 382 µs of `pwrite`
    /// against 59 µs of `pwritev`.
    ///
    /// `bufs` is taken by mutable reference because resuming a short
    /// write consumes it (see `write_all_vectored_at`).
    pub fn write_vectored(&mut self, bufs: &mut [IoSlice<'_>]) -> Result<(), JournalError> {
        let total: usize = bufs.iter().map(|b| b.len()).sum();
        if total == 0 {
            return Ok(());
        }
        self.ensure_allocated(total as u64)?;
        let file = &self.file;
        let start = self.write_pos;
        write_all_vectored_at(bufs, start, |slices, offset| {
            rustix::io::pwritev(file, slices, offset)
                .map_err(|e| std::io::Error::from_raw_os_error(e.raw_os_error()))
        })?;
        self.write_pos += total as u64;
        Ok(())
    }

    /// Force everything written so far to stable media.
    ///
    /// Honest durability: the call doesn't return until the kernel
    /// reports the data is on stable media. On a drive with a volatile
    /// write cache this issues a device-side flush; on a PLP drive
    /// (VWC=0) the flush is a near-no-op.
    pub fn sync(&self) -> Result<(), JournalError> {
        self.file.sync_data()?;
        Ok(())
    }

    /// Paced retry of a failed post-rotation directory fsync — a single
    /// branch in steady state. Call from the flush path.
    pub fn poll_dir_fsync_retry(&mut self) {
        self.dir_fsync_retry.poll();
    }

    /// Byte offset of the end of written data.
    pub fn valid_end(&self) -> u64 {
        self.write_pos
    }

    /// End of the pre-allocated region. Test-only: the invariant worth
    /// pinning is that an adopted prepared segment brings its pre-zeroed
    /// region along as the allocation, so appends don't immediately
    /// re-fallocate and lose the pre-written-extents property.
    #[cfg(test)]
    pub(crate) fn allocated_end(&self) -> u64 {
        self.allocated_end
    }

    /// On-disk path of the live segment.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Decoded file-header fields of the live segment (read from disk).
    /// Used at primary startup to hand replicas the segment's
    /// `(starting_sequence, anchor_hash)` so a fresh replica journal is
    /// byte-identical from the segment's first entry onward.
    pub fn read_header_info(&self) -> Result<codec::FileHeaderInfo, JournalError> {
        crate::segment::read_header_info(&self.path)
    }

    /// Archive the live segment and install a fresh one whose header
    /// records `starting_sequence` and `anchor_hash`, adopting
    /// `prepared` when the background preparer has a staged segment
    /// ready. Returns the archived path.
    ///
    /// The caller must have flushed everything it wants sealed into the
    /// outgoing segment — this method writes no entry bytes.
    ///
    /// On error the live segment is restored at the canonical path and
    /// this `SegmentFile` still describes it, so the caller can keep
    /// writing to the current segment.
    pub fn rotate(
        &mut self,
        prepared: Option<crate::preparer::PreparedSegment>,
        starting_sequence: u64,
        anchor_hash: [u8; 32],
    ) -> Result<PathBuf, JournalError> {
        let path = self.path.clone();
        // Data end of the outgoing segment, captured while `self` still
        // describes it — the archive is compacted to this after the
        // rotation commits.
        let sealed_end = self.write_pos;

        let archived = crate::segment::archive_live(&path)?;

        let installed = match prepared {
            Some(p) => self.install_prepared(p, &path, starting_sequence, anchor_hash),
            None => self.install_fresh(&path, starting_sequence, anchor_hash),
        };

        match installed {
            Ok(()) => {
                // Persist both the rename (archive_live) and the new
                // live file's dirent in a single dir fsync so recovery
                // sees a consistent post-rotation layout after a crash.
                // The rotation is already committed at this point, so a
                // fsync failure must not surface as a rotation failure
                // — see `DirFsyncRetry` for why; a failure is retried
                // from the flush path.
                self.dir_fsync_retry.after_rotation(&path);
                // Drop the sealed segment's allocation padding (see
                // `compact_archive` for why). Best-effort — the
                // rotation is committed either way.
                crate::segment::compact_archive(&archived, sealed_end);
                Ok(archived)
            }
            Err(e) => {
                // Best-effort rollback so the next recovery still finds
                // a live file at the canonical path. If rename-back
                // fails we surface the original error — recovery's
                // Phase B handles "archive present, no live" but the
                // in-process writer is unusable.
                if let Err(restore_err) = std::fs::rename(&archived, &path) {
                    tracing::warn!(
                        "rotate: rename-back failed after segment install error: \
                         original={e}, restore={restore_err}"
                    );
                } else if let Err(fsync_err) = crate::segment::fsync_parent_dir(&path) {
                    // The rename succeeded but the dirent isn't durable
                    // yet. A crash here would leave recovery seeing the
                    // archive without the restored live, the same
                    // Phase-B state the success-path fsync protects
                    // against. Best-effort: log and surface the
                    // original error.
                    tracing::warn!(
                        "rotate: dir fsync after rename-back failed: \
                         original={e}, fsync={fsync_err}"
                    );
                }
                Err(e)
            }
        }
    }

    /// Install a fresh (allocated, not pre-written) segment at
    /// `live_path`. The synchronous fallback taken when the preparer
    /// has nothing staged: `create_new` + `posix_fallocate` + header +
    /// `sync_all`, tens of milliseconds on NVMe.
    ///
    /// Installs in place rather than replacing `self` wholesale so a
    /// pending dir-fsync retry survives the rotation — dropping it
    /// would silently abandon an un-synced dirent from an earlier
    /// failed rotation.
    fn install_fresh(
        &mut self,
        live_path: &Path,
        starting_sequence: u64,
        anchor_hash: [u8; 32],
    ) -> Result<(), JournalError> {
        let fresh = Self::create_continuing(live_path, starting_sequence, anchor_hash)?;
        // Commit point — nothing below can fail. Dropping the old
        // `self.file` closes the outgoing (now archived) segment's fd.
        self.file = fresh.file;
        self.write_pos = fresh.write_pos;
        self.allocated_end = fresh.allocated_end;
        Ok(())
    }

    /// Point this segment at a freshly prepared (zero-filled) file.
    ///
    /// All fallible work (header pwrite, fsync, rename) happens before
    /// any field is assigned, so on error the segment continues on the
    /// current file unchanged — the caller's rename-back rollback
    /// restores the archived live file.
    fn install_prepared(
        &mut self,
        prepared: crate::preparer::PreparedSegment,
        live_path: &Path,
        starting_sequence: u64,
        anchor_hash: [u8; 32],
    ) -> Result<(), JournalError> {
        let crate::preparer::PreparedSegment {
            file,
            path: staging_path,
            allocated_end,
        } = prepared;

        // Write the file header into the staging file *before* the
        // rename, so the file appearing at the live path is always
        // complete. The reverse order would open a window where a
        // header-write failure plus a failed rename-back leaves an
        // all-zeros live file that recovery rejects as invalid instead
        // of handling as the "no live file" Phase-B case. The staging
        // file is zero-filled including `[0, ENTRY_OFFSET)`, so this
        // pwrite lands in already-written extents like every append
        // after it.
        write_header(&file, starting_sequence, anchor_hash)?;
        // Commit the header durably before the rename — a crash before
        // the next user write must still leave a parseable empty
        // journal, matching `create_continuing`. `sync_data`, not
        // `sync_all`: this runs on the journal thread at every
        // rotation, and a full fsync forces the filesystem log for the
        // timestamp metadata — the stall class this whole design
        // removes. The header pwrite changes no file size (the staging
        // file is pre-written to full length) and no allocation (the
        // extents are written), so the data-only flush covers
        // everything the header needs; the preparer already made the
        // file's size and allocation durable at staging time.
        file.sync_data()?;

        // Rename staging onto the live path. `archive_live` has already
        // moved the previous live segment aside, so the destination is
        // free. On failure the fully-written staging file stays on disk
        // for the next preparer cycle to reclaim.
        std::fs::rename(&staging_path, live_path).map_err(JournalError::Io)?;

        // Commit point — nothing below can fail. Dropping the old
        // `self.file` closes the outgoing (now archived) segment's fd.
        self.file = file;
        self.write_pos = HEADER_OFFSET;
        self.allocated_end = allocated_end;
        Ok(())
    }

    /// Extend the file's pre-allocated region whenever the next write
    /// would land past it. Allocates one chunk at a time via
    /// `posix_fallocate` — extent allocation only, no zero-fill cost.
    /// In the prepared-segment flow this only fires when a segment
    /// outgrows its pre-zeroed region (threshold overshoot past the
    /// preparer's margin, or a replica following larger-than-expected
    /// primary segments) — appends past here generate extent-conversion
    /// metadata again until the next rotation.
    fn ensure_allocated(&mut self, adding: u64) -> Result<(), JournalError> {
        let need = self.write_pos + adding;
        if need <= self.allocated_end {
            return Ok(());
        }
        self.allocated_end = fallocate_chunk(&self.file, self.allocated_end)?;
        Ok(())
    }
}

/// Write the file header at offset 0. The codec reserves the first
/// `MAX_SECTOR_SIZE` (= 4096) bytes for the header regardless of the
/// device's sector size, so the layout is fixed across deployments.
fn write_header(
    file: &File,
    starting_sequence: u64,
    anchor_hash: [u8; 32],
) -> Result<(), JournalError> {
    let mut header_buf = [0u8; MAX_SECTOR_SIZE];
    codec::encode_file_header(
        &mut header_buf,
        MAX_SECTOR_SIZE,
        starting_sequence,
        anchor_hash,
    );
    write_all_at(file, &header_buf, 0)
}

/// Pre-allocate one chunk of disk blocks starting at `from`. Returns
/// the new end-of-allocation offset.
///
/// Allocates only the new range — not `[0, from + chunk)` — so the
/// fallocate call doesn't walk the entire extent tree on every
/// extension as the journal grows.
fn fallocate_chunk(file: &File, from: u64) -> Result<u64, JournalError> {
    let chunk = prealloc_chunk_bytes();
    rustix::fs::fallocate(
        file.as_fd(),
        rustix::fs::FallocateFlags::empty(),
        from,
        chunk,
    )
    .map_err(|e| JournalError::Io(std::io::Error::from_raw_os_error(e.raw_os_error())))?;
    Ok(from + chunk)
}

/// Write every byte of `bufs` at `offset`, resuming across short
/// writes.
///
/// Vectored writes make resumption harder than the single-buffer case:
/// the kernel reports one byte count spanning the whole iovec array, so
/// a partial write has to be mapped back onto the array before
/// retrying. [`IoSlice::advance_slices`] does that mapping — it drops
/// fully-written slices and trims the first partial one. Getting it
/// wrong would silently duplicate or skip journal bytes, which is why
/// the write call is a parameter: the test drives this loop with a
/// writer that deliberately writes only a few bytes at a time.
fn write_all_vectored_at(
    bufs: &mut [IoSlice<'_>],
    offset: u64,
    mut write: impl FnMut(&[IoSlice<'_>], u64) -> std::io::Result<usize>,
) -> Result<(), JournalError> {
    let mut remaining = &mut *bufs;
    let mut offset = offset;
    while !remaining.is_empty() {
        let n = write(remaining, offset).map_err(JournalError::Io)?;
        if n == 0 {
            return Err(JournalError::Io(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "vectored journal write returned 0",
            )));
        }
        offset += n as u64;
        IoSlice::advance_slices(&mut remaining, n);
    }
    Ok(())
}

/// Write `buf` in full at `offset`, retrying short writes. `pwrite`
/// can return fewer bytes than requested on signal interruption or
/// when the kernel decides to split a large write; we loop until the
/// whole buffer is on its way.
fn write_all_at(file: &File, buf: &[u8], offset: u64) -> Result<(), JournalError> {
    let mut written = 0;
    while written < buf.len() {
        let n = file
            .write_at(&buf[written..], offset + written as u64)
            .map_err(JournalError::Io)?;
        if n == 0 {
            return Err(JournalError::Io(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "buffered journal write returned 0",
            )));
        }
        written += n;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A prepared segment whose staging file has been unlinked: the
    /// header write and its sync land on the still-open inode, then the
    /// rename onto the live path fails with NotFound. The cheapest
    /// deterministic way to drive `rotate` into its install-failure
    /// path.
    fn doomed_prepared(dir: &Path) -> crate::preparer::PreparedSegment {
        let staging = dir.join("ghost.staging");
        let file = File::create(&staging).unwrap();
        std::fs::remove_file(&staging).unwrap();
        crate::preparer::PreparedSegment {
            file,
            path: staging,
            allocated_end: 4096,
        }
    }

    /// A rotation that fails while installing the new segment must
    /// leave the journal exactly as it found it: the live segment back
    /// at the canonical path, no archive stranded beside it, and this
    /// `SegmentFile` still describing (and able to append to) the
    /// segment it had before.
    ///
    /// Without the rename-back, recovery would see an archive with no
    /// live file — the Phase-B layout — for what was only a transient
    /// failure.
    #[test]
    fn failed_rotation_rolls_back_to_the_live_segment() {
        let dir = tempfile::tempdir().unwrap();
        let live = dir.path().join("test.journal");

        let mut segment = SegmentFile::create_continuing(&live, 1, [7u8; 32]).unwrap();
        segment.write_batch(b"first batch").unwrap();
        segment.sync().unwrap();
        let end_before = segment.valid_end();

        let err = segment
            .rotate(Some(doomed_prepared(dir.path())), 2, [9u8; 32])
            .expect_err("install must fail when the staging file is gone");
        assert!(
            !err.to_string().is_empty(),
            "the install error must be surfaced, not swallowed"
        );

        assert!(live.exists(), "rename-back must restore the live segment");
        assert!(
            crate::segment::list_archives(&live).unwrap().is_empty(),
            "a rolled-back rotation must leave no archive behind"
        );

        // Still the same segment, still writable: the position never
        // moved and appends continue where they left off.
        assert_eq!(segment.valid_end(), end_before);
        segment.write_batch(b"second batch").unwrap();
        segment.sync().unwrap();
        assert_eq!(
            segment.valid_end(),
            end_before + b"second batch".len() as u64
        );
        assert!(
            std::fs::metadata(&live).unwrap().len() >= segment.valid_end(),
            "the restored live file must hold both batches"
        );
    }

    /// A vectored write must resume correctly across short writes.
    ///
    /// This is the one place the batched write path is harder than the
    /// single-buffer one: the kernel reports a single byte count for
    /// the whole iovec array, so resuming means mapping that count back
    /// onto the array — dropping the slices already written and
    /// trimming the one cut in half. Get it wrong and the journal
    /// silently gains duplicated or missing bytes, which no CRC would
    /// catch because each entry is individually well-formed.
    ///
    /// Driven with a writer that never accepts more than `limit` bytes,
    /// swept across every limit from 1 byte to the whole run, so the
    /// cut lands at every possible position including exactly on and
    /// either side of each slice boundary.
    #[test]
    fn vectored_writes_resume_across_short_writes() {
        let parts: [&[u8]; 4] = [b"alpha", b"", b"bravo-longer", b"c"];
        let expected: Vec<u8> = parts.concat();

        for limit in 1..=expected.len() + 2 {
            let mut sink = Vec::new();
            let mut calls = 0usize;
            let mut bufs: Vec<IoSlice<'_>> = parts.iter().map(|p| IoSlice::new(p)).collect();

            write_all_vectored_at(&mut bufs, 0, |slices, offset| {
                calls += 1;
                assert_eq!(
                    offset as usize,
                    sink.len(),
                    "limit {limit}: offset must track bytes already written"
                );
                // Accept at most `limit` bytes of whatever remains.
                let mut taken = 0;
                for s in slices {
                    if taken == limit {
                        break;
                    }
                    let n = (limit - taken).min(s.len());
                    sink.extend_from_slice(&s[..n]);
                    taken += n;
                    if n < s.len() {
                        break;
                    }
                }
                Ok(taken)
            })
            .unwrap_or_else(|e| panic!("limit {limit}: {e}"));

            assert_eq!(
                sink, expected,
                "limit {limit}: resumed write produced the wrong bytes after {calls} calls"
            );
        }
    }

    /// A writer that accepts nothing must fail rather than spin: the
    /// resume loop would otherwise never terminate.
    #[test]
    fn a_vectored_write_that_accepts_nothing_is_an_error() {
        let mut bufs = [IoSlice::new(b"data")];
        let err = write_all_vectored_at(&mut bufs, 0, |_, _| Ok(0))
            .expect_err("a zero-byte write must not loop forever");
        assert!(err.to_string().contains("returned 0"), "got: {err}");
    }

    /// Batches written vectored must land as the plain concatenation —
    /// byte-identical to the same batches written one at a time, since
    /// this only collapses the syscalls.
    #[test]
    fn vectored_batches_land_as_one_concatenation() {
        let dir = tempfile::tempdir().unwrap();

        let one_at_a_time = dir.path().join("sequential.journal");
        let mut a = SegmentFile::create_continuing(&one_at_a_time, 1, [5u8; 32]).unwrap();
        for part in [b"first".as_slice(), b"second", b"third"] {
            a.write_batch(part).unwrap();
        }
        a.sync().unwrap();

        let vectored = dir.path().join("vectored.journal");
        let mut b = SegmentFile::create_continuing(&vectored, 1, [5u8; 32]).unwrap();
        let mut bufs = [
            IoSlice::new(b"first"),
            IoSlice::new(b"second"),
            IoSlice::new(b"third"),
        ];
        b.write_vectored(&mut bufs).unwrap();
        b.sync().unwrap();

        assert_eq!(a.valid_end(), b.valid_end(), "write positions must agree");
        let left = std::fs::read(&one_at_a_time).unwrap();
        let right = std::fs::read(&vectored).unwrap();
        assert_eq!(
            left[..a.valid_end() as usize],
            right[..b.valid_end() as usize],
            "vectored and sequential writes must produce identical bytes"
        );
    }

    /// An all-empty run must not issue a write at all — under
    /// `no-persist`, and for query-only batches, every staged batch is
    /// byte-empty and the position must not move.
    #[test]
    fn a_vectored_write_of_nothing_leaves_the_position_alone() {
        let dir = tempfile::tempdir().unwrap();
        let live = dir.path().join("empty.journal");
        let mut segment = SegmentFile::create_continuing(&live, 1, [0u8; 32]).unwrap();
        let before = segment.valid_end();

        segment.write_vectored(&mut []).unwrap();
        assert_eq!(segment.valid_end(), before);
    }

    /// The happy path's contract: the outgoing segment is archived, a
    /// fresh live file takes its place with the write position back at
    /// the header boundary, and the header carries the caller's
    /// boundary values.
    #[test]
    fn rotation_archives_and_installs_a_fresh_segment() {
        let dir = tempfile::tempdir().unwrap();
        let live = dir.path().join("test.journal");

        let mut segment = SegmentFile::create_continuing(&live, 1, [1u8; 32]).unwrap();
        segment.write_batch(b"sealed content").unwrap();
        segment.sync().unwrap();

        let archived = segment.rotate(None, 42, [2u8; 32]).unwrap();

        assert!(archived.exists(), "outgoing segment must be archived");
        assert!(live.exists(), "a fresh live segment must be in place");
        assert_eq!(
            segment.valid_end(),
            HEADER_OFFSET,
            "the fresh segment starts right after its header"
        );
        let info = segment.read_header_info().unwrap();
        assert_eq!(info.starting_sequence, 42);
        assert_eq!(info.anchor_hash, [2u8; 32]);
    }
}
