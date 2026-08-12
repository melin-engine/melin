//! Background segment preparer — pre-stages the next journal segment off
//! the rotation hot path.
//!
//! ## Why
//!
//! With size-driven rotation enabled, every `max_journal_bytes` written
//! the journal stage calls `SectorWriter::rotate_segment`, which creates
//! the next segment file via `posix_fallocate(+chunk)` +
//! `FALLOC_FL_ZERO_RANGE` + `prefault_pages` + `sync_all`. On PLP-class
//! NVMe drives that ceremony is a ~38 ms synchronous stall — directly
//! visible in p99.99 of the order pipeline.
//!
//! The preparer moves that work to a dedicated thread:
//!
//!   1. At construction (and after every rotation) the journal stage
//!      calls [`SegmentPreparer::arm`].
//!   2. The worker opens `<live>.next-staging`, runs the same
//!      `preallocate + zero_range + prefault + sync_all` sequence, and
//!      parks the result in `slot`.
//!   3. At rotation time the writer calls
//!      [`SegmentPreparer::take`]; if it returns `Some`, the writer
//!      renames the staging file into place and writes the file header
//!      (which carries the boundary's sequence + chain anchor). Cost:
//!      two renames + one dir fsync. The ~38 ms is gone.
//!   4. If `take` returns `None` (the worker hasn't caught up, manual
//!      rotation arrived early, or preparation errored), the writer
//!      falls back to today's synchronous path.
//!
//! ## Prepare modes
//!
//! The staging work differs by writer:
//!
//! - **Sector** ([`SegmentPreparer::spawn`]): `posix_fallocate` +
//!   `FALLOC_FL_ZERO_RANGE` + prefault, matching what
//!   `SectorWriter::create_bare_inner` does synchronously. Extents stay
//!   *unwritten* — fine for O_DIRECT, which never calls `fdatasync`.
//!
//! - **Zero-fill** ([`SegmentPreparer::spawn_zero_fill`]): physically
//!   writes zeros over the whole segment and syncs. This is the
//!   `BufferedWriter` mode, and the physical writes are the point:
//!   `FALLOC_FL_ZERO_RANGE` leaves extents unwritten, so every append
//!   still converts them and every conversion is a logged filesystem
//!   metadata transaction. Those transactions periodically force the
//!   filesystem journal *inside the writer's `fdatasync`* (measured on
//!   XFS as a ~2 ms CIL-force stall every 10.24 s that froze the whole
//!   order pipeline — see `docs/internal/journal-fsync-beat-2026-08.md`).
//!   Appends into pre-written extents carry no metadata, so `fdatasync`
//!   stays on its data-only fast path. The cost is writing every
//!   segment twice (zeros, then data) — sequential, off the hot path,
//!   and documented as the write-amplification trade-off.

use std::fs::{File, OpenOptions};
use std::io;
#[cfg(not(feature = "no-o-direct"))]
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::codec::ENTRY_OFFSET;
use crate::error::JournalError;
use crate::sector_writer::{preallocate, prefault_pages, zero_range_extents};

/// A fully-prepared journal segment file ready to be adopted by a
/// writer on the next rotation.
///
/// In sector mode ([`SegmentPreparer::spawn`]) the file has:
///   - extents allocated for `[sector_size, allocated_end)` via
///     `posix_fallocate`,
///   - those extents marked zeroed via `FALLOC_FL_ZERO_RANGE`,
///   - the corresponding pages prefaulted into the page cache,
///   - `sync_all` issued so the allocation is durable across crashes.
///
/// In zero-fill mode ([`SegmentPreparer::spawn_zero_fill`]) the file has
/// real zeros physically written over `[0, allocated_end)` and synced,
/// so every extent is *written* (not merely allocated) — see the module
/// docs for why that distinction is the entire point.
///
/// The file header is *not* yet written — the adopting writer
/// (`SectorWriter::install_new_segment` /
/// `BufferedWriter::install_prepared_segment`) writes it at adopt time
/// so it reflects the rotation boundary's sequence + chain anchor.
pub struct PreparedSegment {
    /// File handle, reused by the writer after rename. Opened with
    /// O_DIRECT in sector mode, plain in zero-fill mode.
    pub file: File,
    /// Path of the staging file (`<live>.next-staging`). The adopter
    /// renames it onto the live path.
    pub path: PathBuf,
    /// End of the pre-allocated (sector) / pre-zeroed (zero-fill)
    /// region (matches the writer's `allocated_end`).
    pub allocated_end: u64,
    /// Sector size detected at open time — must match the live file.
    /// `0` in zero-fill mode: the buffered writer has no alignment
    /// requirement and ignores it.
    pub sector_size: usize,
}

/// How the worker stages a segment. Fixed at spawn time — one preparer
/// serves one writer, and the two writers need incompatible staging
/// (O_DIRECT + fallocate vs plain fd + physical zeros).
///
/// Copy: two words, read once per prepare cycle.
#[derive(Clone, Copy)]
enum PrepareMode {
    /// `posix_fallocate` + `FALLOC_FL_ZERO_RANGE` + prefault, O_DIRECT
    /// handle. For `SectorWriter`.
    Sector { sector_size: usize },
    /// Physically write `bytes` of zeros and sync, plain handle. For
    /// `BufferedWriter`.
    ZeroFill { bytes: u64 },
}

/// Extra zeroed bytes past the rotation threshold in zero-fill mode.
/// The size trigger is evaluated per flushed batch (≤ 512 KiB), so the
/// live segment overshoots the threshold by at most a batch plus
/// whatever a manual-rotation command lags by; 8 MiB of margin keeps
/// even those writes inside the pre-written region. Writes past the
/// margin fall back to `posix_fallocate` extension (rare, and only
/// costs the metadata-per-append behavior this mode exists to avoid).
const ZERO_FILL_MARGIN_BYTES: u64 = 8 * 1024 * 1024;

/// Manages a background thread that pre-stages the next segment.
///
/// Owned by `JournalStage` (one per pipeline), survives across rotations.
/// Construction spawns the worker; [`shutdown`](Self::shutdown) or `Drop`
/// joins it.
pub struct SegmentPreparer {
    state: Arc<State>,
    handle: Option<JoinHandle<()>>,
}

/// Shared state between the public `SegmentPreparer` and the worker
/// thread. Arc-wrapped so the worker keeps it alive even if the
/// `SegmentPreparer` handle is moved/dropped mid-operation.
struct State {
    /// Path of the live journal segment. The staging path is derived as
    /// `<live_path>.next-staging`.
    live_path: PathBuf,
    /// Staging strategy, fixed at spawn (see [`PrepareMode`]).
    mode: PrepareMode,
    /// Mutex<Option<…>> because the slot is mutated from two threads
    /// (worker writes, adopter takes) and has at most one entry. No
    /// contention on the hot path — the lock is only acquired at
    /// rotation time and on prepare completion.
    slot: Mutex<Option<PreparedSegment>>,
    /// `true` when an arm has been requested. The mutex is paired with
    /// `notify` so the worker can block on a `Condvar::wait` without
    /// busy-spinning.
    armed: Mutex<bool>,
    /// Wakes the worker when `armed` flips to `true` or `shutdown` flips
    /// to `true`.
    notify: Condvar,
    /// Signals the worker to exit. Checked at every loop iteration and
    /// during backoff sleeps.
    shutdown: AtomicBool,
}

impl SegmentPreparer {
    /// Spawn the worker thread. Arms immediately so the first rotation
    /// can adopt instead of paying the sync cost.
    ///
    /// Also clears any orphan `<live>.next-staging` file left behind by
    /// a crashed prior run — these files have no header and are not
    /// recognised by `segment::list_archives`, but leaving them on disk
    /// would cause `create_new` to fail at the next prepare.
    pub fn spawn(live_path: PathBuf, sector_size: usize) -> Self {
        Self::spawn_with_mode(live_path, PrepareMode::Sector { sector_size })
    }

    /// Spawn a worker that stages segments by physically writing zeros
    /// (buffered-writer mode — see the module docs for why real writes
    /// rather than `FALLOC_FL_ZERO_RANGE`).
    ///
    /// `rotate_threshold_bytes` is the size trigger the pipeline rotates
    /// at (`max_journal_bytes`); the staged file is zeroed to that plus
    /// [`ZERO_FILL_MARGIN_BYTES`]. Pass `0` when the threshold is
    /// unknown locally (replica mode — rotation follows the primary's
    /// announced boundaries): the worker falls back to one prealloc
    /// chunk, which matches the primary's segments under the default
    /// matched configuration.
    pub fn spawn_zero_fill(live_path: PathBuf, rotate_threshold_bytes: u64) -> Self {
        let bytes = if rotate_threshold_bytes > 0 {
            rotate_threshold_bytes + ZERO_FILL_MARGIN_BYTES
        } else {
            crate::prealloc::prealloc_chunk_bytes()
        };
        Self::spawn_with_mode(live_path, PrepareMode::ZeroFill { bytes })
    }

    fn spawn_with_mode(live_path: PathBuf, mode: PrepareMode) -> Self {
        cleanup_staging_orphan(&live_path);

        let state = Arc::new(State {
            live_path,
            mode,
            slot: Mutex::new(None),
            // Pre-arm at startup so the worker prepares the first spare
            // segment in parallel with engine warm-up. The first rotation
            // then has a ready segment to adopt.
            armed: Mutex::new(true),
            notify: Condvar::new(),
            shutdown: AtomicBool::new(false),
        });

        let worker_state = Arc::clone(&state);
        let handle = std::thread::Builder::new()
            .name("journal-prep".into())
            .spawn(move || worker_loop(worker_state))
            .expect("failed to spawn journal-prep thread");

        Self {
            state,
            handle: Some(handle),
        }
    }

    /// Request preparation of the next segment. Idempotent — if the
    /// worker is already preparing or a `PreparedSegment` is already in
    /// the slot, the signal coalesces.
    pub fn arm(&self) {
        // Acquire arm mutex first, then notify under the same lock so we
        // can't lose a wakeup against the worker's wait condition.
        let mut armed = self
            .state
            .armed
            .lock()
            .expect("preparer armed mutex poisoned");
        *armed = true;
        self.state.notify.notify_one();
    }

    /// Drain the prepared-segment slot. Returns `Some` only if the
    /// worker has finished a preparation that has not yet been adopted.
    /// Called by `SectorWriter::rotate_segment` to decide between the
    /// fast adopt path and the sync fallback.
    pub fn take(&self) -> Option<PreparedSegment> {
        self.state
            .slot
            .lock()
            .expect("preparer slot mutex poisoned")
            .take()
    }

    /// Signal the worker to exit and join the thread. Idempotent; safe
    /// to call from `Drop`.
    pub fn shutdown(mut self) {
        self.shutdown_inner();
    }

    fn shutdown_inner(&mut self) {
        self.state.shutdown.store(true, Ordering::Release);
        // Wake the worker if it's parked on `notify.wait`.
        {
            let mut armed = self
                .state
                .armed
                .lock()
                .expect("preparer armed mutex poisoned");
            *armed = true;
        }
        self.state.notify.notify_one();
        if let Some(h) = self.handle.take() {
            // Best-effort join: a panic in the worker has already been
            // logged by Rust's default panic handler.
            if let Err(e) = h.join() {
                tracing::warn!(?e, "journal-prep thread panicked during shutdown");
            }
        }
    }
}

impl Drop for SegmentPreparer {
    fn drop(&mut self) {
        if self.handle.is_some() {
            self.shutdown_inner();
        }
    }
}

/// Worker thread: wait for an arm signal, prepare one segment, repeat.
///
/// Errors during preparation log a warning and back off for ~30 s
/// (interrupted by shutdown) so transient ENOSPC / RO-FS conditions
/// don't busy-loop the thread.
fn worker_loop(state: Arc<State>) {
    // Spawned from the journal thread, which is pinned to a single core
    // and runs SCHED_FIFO in tuned deployments — child threads inherit
    // both. Left in place, this worker either starves behind the
    // busy-spinning journal thread (a same-priority FIFO peer on the
    // same core never runs, so the fast path never arms) or executes
    // the ~38 ms allocate ceremony ON the journal core whenever the
    // journal thread blocks. Reset to the default mask and SCHED_OTHER
    // before doing any work, like every other child of a pinned thread.
    if let Err(e) = melin_app::affinity::clear_affinity() {
        tracing::warn!(error = e, "failed to clear segment-preparer affinity");
    }
    loop {
        // Wait for arm or shutdown.
        let mut armed = match state.armed.lock() {
            Ok(g) => g,
            Err(_) => return, // poisoned; just exit
        };
        while !*armed && !state.shutdown.load(Ordering::Acquire) {
            armed = match state.notify.wait(armed) {
                Ok(g) => g,
                Err(_) => return,
            };
        }
        if state.shutdown.load(Ordering::Acquire) {
            return;
        }
        *armed = false;
        drop(armed);

        // If a previous preparation is still waiting to be adopted, skip
        // this cycle — the slot has capacity for one.
        let occupied = state.slot.lock().map(|g| g.is_some()).unwrap_or(false);
        if occupied {
            continue;
        }

        match prepare_one(&state.live_path, state.mode) {
            Ok(prepared) => {
                if let Ok(mut g) = state.slot.lock() {
                    *g = Some(prepared);
                }
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "journal segment preparer failed; will retry after backoff"
                );
                backoff_sleep(&state);
            }
        }
    }
}

/// Sleep ~30 s in 1 s increments so a shutdown signal is acted on
/// promptly even after a preparation failure.
fn backoff_sleep(state: &State) {
    for _ in 0..30 {
        if state.shutdown.load(Ordering::Acquire) {
            return;
        }
        std::thread::sleep(Duration::from_secs(1));
    }
}

/// Create the staging file and run the expensive staging steps for the
/// preparer's mode.
///
/// Neither mode writes a file header — the header is application data
/// that depends on the rotation-boundary state and is written by the
/// adopting writer after the rename.
fn prepare_one(live_path: &Path, mode: PrepareMode) -> Result<PreparedSegment, JournalError> {
    let staging = staging_path(live_path);

    // Remove any stale staging file. A leftover here is normally
    // cleaned by `SegmentPreparer::spawn`, but `create_new` would fail
    // with AlreadyExists if a race or external operator left one
    // behind. Treat NotFound as success.
    match std::fs::remove_file(&staging) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        // Surface other errors via the create_new below — the caller
        // will see the real fault and log it.
        Err(_) => {}
    }

    match mode {
        PrepareMode::Sector { sector_size } => prepare_sector(&staging, sector_size),
        PrepareMode::ZeroFill { bytes } => prepare_zero_filled(&staging, bytes),
    }
}

/// Sector-mode staging: O_DIRECT handle, allocation-only preparation.
/// Mirrors the prep done in `SectorWriter::create_bare_inner`.
fn prepare_sector(staging: &Path, sector_size: usize) -> Result<PreparedSegment, JournalError> {
    let mut opts = OpenOptions::new();
    opts.read(true).write(true).create_new(true);
    #[cfg(not(feature = "no-o-direct"))]
    opts.custom_flags(libc::O_DIRECT);
    let file = opts.open(staging)?;

    // Reserve `ENTRY_OFFSET` for the file header (written later by
    // `install_new_segment`) — matches `create_bare_inner` so adoption
    // is a simple header pwrite, not a re-allocate.
    let allocated_end = preallocate(&file, ENTRY_OFFSET)?;
    zero_range_extents(&file, ENTRY_OFFSET, allocated_end);
    prefault_pages(&file, ENTRY_OFFSET, allocated_end);
    file.sync_all()?;

    Ok(PreparedSegment {
        file,
        path: staging.to_path_buf(),
        allocated_end,
        sector_size,
    })
}

/// Zero-fill staging: plain (page-cache) handle, `bytes` of physical
/// zeros written over `[0, bytes)` and synced.
///
/// The header region `[0, ENTRY_OFFSET)` is zeroed too, so the
/// adopter's header pwrite also lands in written extents. Writing goes
/// through the page cache on purpose: the pages the buffered writer is
/// about to append into are left resident, which doubles as the
/// prefault the sector mode does explicitly.
fn prepare_zero_filled(staging: &Path, bytes: u64) -> Result<PreparedSegment, JournalError> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(staging)?;

    // 1 MiB chunk: large enough that the copy loop is bandwidth-bound,
    // small enough to stay cache/allocator friendly. Heap Vec (not a
    // stack array) — 1 MiB would overflow default thread stacks.
    let zeros = vec![0u8; 1024 * 1024];
    let mut offset: u64 = 0;
    while offset < bytes {
        let n = (bytes - offset).min(zeros.len() as u64) as usize;
        let mut written = 0;
        while written < n {
            let w = std::os::unix::fs::FileExt::write_at(
                &file,
                &zeros[written..n],
                offset + written as u64,
            )?;
            if w == 0 {
                return Err(JournalError::Io(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "zero-fill write returned 0",
                )));
            }
            written += w;
        }
        offset += n as u64;
    }
    file.sync_all()?;

    Ok(PreparedSegment {
        file,
        path: staging.to_path_buf(),
        allocated_end: bytes,
        // Not applicable — the buffered writer has no alignment
        // requirement and ignores this field.
        sector_size: 0,
    })
}

/// Remove a stale `<live>.next-staging` file left behind by a prior
/// process that crashed mid-prepare or rotated without consuming the
/// staged segment.
///
/// Called from two places:
///   - [`SegmentPreparer::spawn`] when rotation is enabled (the
///     preparer would otherwise fail at `create_new` on the same path).
///   - [`crate::sector_writer::SectorWriter::create`] and `::open_append` so
///     the orphan is reclaimed even when rotation is disabled (no
///     preparer ever runs).
///
/// Must NOT be called once the preparer is alive — the worker may have
/// an in-flight staging file whose fd is still valid even after
/// unlink. The two startup entry points above are guaranteed to run
/// before any preparer can be spawned.
///
/// Best-effort: NotFound is the common case (no prior crash). Other
/// errors are logged but not propagated — the next `create_new` will
/// surface the real fault if cleanup truly failed.
pub(crate) fn cleanup_staging_orphan(live_path: &Path) {
    let staging = staging_path(live_path);
    match std::fs::remove_file(&staging) {
        Ok(()) => {
            tracing::info!(
                path = %staging.display(),
                "removed orphan journal staging file from a prior run"
            );
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => {
            tracing::warn!(
                error = %e,
                path = %staging.display(),
                "could not remove orphan journal staging file"
            );
        }
    }
}

/// `<live>.next-staging` — sibling of the live segment, same directory.
///
/// Using `OsString::push` rather than `with_extension` because the live
/// path normally already has an extension (`.journal`) and
/// `with_extension` would replace it.
pub(crate) fn staging_path(live: &Path) -> PathBuf {
    let mut s = live.as_os_str().to_owned();
    s.push(".next-staging");
    PathBuf::from(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Spawning, arming, and shutdown round-trips without leaking the
    /// worker thread or the staging file.
    #[test]
    fn spawn_prepare_shutdown_roundtrip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let live = dir.path().join("test.journal");
        // Live file doesn't need to exist — the preparer only touches
        // the staging sibling.

        let preparer = SegmentPreparer::spawn(live.clone(), 4096);

        // Wait up to 5 s for the worker to publish a prepared segment.
        // 256 MiB fallocate on tmpfs is sub-millisecond, but the bounded
        // wait protects against an unexpectedly slow CI host.
        let mut prepared = None;
        for _ in 0..500 {
            if let Some(p) = preparer.take() {
                prepared = Some(p);
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let prepared = prepared.expect("preparer should publish a segment within 5 s");

        assert_eq!(prepared.sector_size, 4096);
        assert_eq!(prepared.path, staging_path(&live));
        assert!(prepared.allocated_end > 4096);

        // Drop the file before shutdown so the staging file can be
        // cleaned by the test harness.
        let staging = prepared.path.clone();
        drop(prepared);

        preparer.shutdown();

        // Staging file still on disk (we took ownership and dropped it
        // without renaming). Cleanup is the adopter's responsibility in
        // production; here we just verify nothing else leaked.
        assert!(staging.exists(), "staging file should still exist on disk");
    }

    /// `spawn` removes a leftover staging file from a prior crash.
    #[test]
    fn spawn_cleans_orphan_staging_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let live = dir.path().join("test.journal");
        let staging = staging_path(&live);

        std::fs::write(&staging, b"orphan from prior crash").expect("write orphan");
        assert!(staging.exists());

        let preparer = SegmentPreparer::spawn(live, 4096);

        // The orphan should be gone immediately; the worker will then
        // create a fresh staging file.
        // Wait for the worker to produce a fresh prepared segment to
        // confirm spawn() didn't fail mid-cleanup.
        let mut prepared = None;
        for _ in 0..500 {
            if let Some(p) = preparer.take() {
                prepared = Some(p);
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            prepared.is_some(),
            "worker should produce a fresh segment after orphan cleanup"
        );

        preparer.shutdown();
    }

    /// Zero-fill mode physically writes zeros over the whole target
    /// range (header region included) with a plain, non-O_DIRECT
    /// handle, and reports the zeroed end as `allocated_end`.
    #[test]
    fn zero_fill_mode_writes_real_zeros() {
        let dir = tempfile::tempdir().expect("tempdir");
        let live = dir.path().join("test.journal");

        // 3 MiB threshold → staged size = threshold + margin.
        let threshold: u64 = 3 * 1024 * 1024;
        let preparer = SegmentPreparer::spawn_zero_fill(live.clone(), threshold);

        let mut prepared = None;
        for _ in 0..500 {
            if let Some(p) = preparer.take() {
                prepared = Some(p);
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let prepared = prepared.expect("preparer should publish a segment within 5 s");

        let expected = threshold + ZERO_FILL_MARGIN_BYTES;
        assert_eq!(prepared.allocated_end, expected);
        assert_eq!(prepared.sector_size, 0, "not applicable in zero-fill mode");
        assert_eq!(
            std::fs::metadata(&prepared.path)
                .expect("stat staging")
                .len(),
            expected,
            "file length must equal the zeroed end — real writes, not allocation"
        );

        // Spot-check content is zeros at the start, middle, and end.
        use std::os::unix::fs::FileExt;
        let mut buf = [0xAAu8; 4096];
        for offset in [0, expected / 2, expected - 4096] {
            prepared
                .file
                .read_exact_at(&mut buf, offset)
                .expect("read staged bytes");
            assert!(
                buf.iter().all(|&b| b == 0),
                "staged bytes at {offset} must be zero"
            );
        }

        drop(prepared);
        preparer.shutdown();
    }

    /// `arm` after `take` triggers a second preparation. Verifies the
    /// post-rotation rearm path used by the journal stage.
    #[test]
    fn rearm_after_take_produces_second_segment() {
        let dir = tempfile::tempdir().expect("tempdir");
        let live = dir.path().join("test.journal");

        let preparer = SegmentPreparer::spawn(live, 4096);

        // First prepared segment.
        let mut first = None;
        for _ in 0..500 {
            if let Some(p) = preparer.take() {
                first = Some(p);
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let first = first.expect("first prepared segment");
        // Simulate adoption by dropping + removing the staging file.
        std::fs::remove_file(&first.path).expect("remove first staging file");
        drop(first);

        // Re-arm and wait for the second.
        preparer.arm();
        let mut second = None;
        for _ in 0..500 {
            if let Some(p) = preparer.take() {
                second = Some(p);
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            second.is_some(),
            "preparer should produce a second segment after rearm"
        );

        preparer.shutdown();
    }
}
