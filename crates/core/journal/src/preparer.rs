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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
/// Copy: two words, read once per prepare cycle. The zero-fill *size*
/// is deliberately not in here — it adapts to observed segment sizes
/// at every re-arm (see `State::zero_fill_target`), so freezing it at
/// spawn would be wrong.
#[derive(Clone, Copy)]
enum PrepareMode {
    /// `posix_fallocate` + `FALLOC_FL_ZERO_RANGE` + prefault, O_DIRECT
    /// handle. For `SectorWriter`.
    Sector { sector_size: usize },
    /// Physically write zeros (current `State::zero_fill_target` bytes)
    /// and sync, plain handle. For `BufferedWriter`.
    ZeroFill,
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
    /// Zero-fill mode only: how many bytes the next staged segment is
    /// zeroed to. Seeded at spawn (threshold + margin, or one prealloc
    /// chunk + margin when the threshold is unknown) and updated on
    /// every [`SegmentPreparer::arm_with_observed_len`] so the target
    /// tracks the segment sizes the deployment actually produces —
    /// critical on replicas, which never know the primary's
    /// `max_journal_bytes` and would otherwise stage a fixed default
    /// that silently under- or over-shoots it. `AtomicU64`: written by
    /// the journal thread at rotation time, read by the worker; no
    /// ordering dependency beyond the value itself (`Relaxed`) — a
    /// stale read stages one segment at the previous size, which the
    /// margin absorbs.
    zero_fill_target: AtomicU64,
    /// Floor for `zero_fill_target` updates: the configured rotation
    /// threshold + margin when known, else 0. Keeps an early manual
    /// rotation (tiny observed segment) from shrinking the target below
    /// what size-driven rotation needs.
    zero_fill_floor: u64,
    /// Core the worker pins itself to; `0` = unpinned. Read once at
    /// worker startup.
    pin_core: usize,
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
    /// `pin_core`: core to pin the worker to, `0` = unpinned (the
    /// worker floats on the default mask at `SCHED_OTHER`). Pinning
    /// keeps staging I/O bursts off the IRQ core and the pipeline
    /// cores deterministically — same convention as the shadow and
    /// event-publisher threads.
    pub fn spawn(live_path: PathBuf, sector_size: usize, pin_core: usize) -> Self {
        Self::spawn_with_mode(
            live_path,
            PrepareMode::Sector { sector_size },
            0,
            0,
            pin_core,
        )
    }

    /// Spawn a worker that stages segments by physically writing zeros
    /// (buffered-writer mode — see the module docs for why real writes
    /// rather than `FALLOC_FL_ZERO_RANGE`).
    ///
    /// `rotate_threshold_bytes` is the size trigger the pipeline rotates
    /// at (`max_journal_bytes`); the staged file is initially zeroed to
    /// that plus [`ZERO_FILL_MARGIN_BYTES`]. Pass `0` when the threshold
    /// is unknown locally (replica mode — rotation follows the primary's
    /// announced boundaries): the initial target falls back to one
    /// prealloc chunk plus margin, and every
    /// [`arm_with_observed_len`](Self::arm_with_observed_len) after a
    /// rotation retunes it to the segment sizes actually observed, so a
    /// replica converges on the primary's real segment size after its
    /// first adoption regardless of configuration mismatch.
    /// `pin_core` as on [`spawn`](Self::spawn): `0` = unpinned.
    pub fn spawn_zero_fill(
        live_path: PathBuf,
        rotate_threshold_bytes: u64,
        pin_core: usize,
    ) -> Self {
        let floor = if rotate_threshold_bytes > 0 {
            rotate_threshold_bytes + ZERO_FILL_MARGIN_BYTES
        } else {
            0
        };
        let initial = if floor > 0 {
            floor
        } else {
            crate::prealloc::prealloc_chunk_bytes() + ZERO_FILL_MARGIN_BYTES
        };
        Self::spawn_with_mode(live_path, PrepareMode::ZeroFill, initial, floor, pin_core)
    }

    fn spawn_with_mode(
        live_path: PathBuf,
        mode: PrepareMode,
        zero_fill_target: u64,
        zero_fill_floor: u64,
        pin_core: usize,
    ) -> Self {
        cleanup_staging_orphan(&live_path);

        let state = Arc::new(State {
            live_path,
            mode,
            zero_fill_target: AtomicU64::new(zero_fill_target),
            zero_fill_floor,
            pin_core,
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

    /// [`arm`](Self::arm) plus a zero-fill target update from the size
    /// of the segment that just rotated out. Called by the journal
    /// stage after every rotation so staged segments track the sizes
    /// the deployment actually produces (a replica's only source of
    /// truth for the primary's segment size). No-op sizing-wise in
    /// sector mode.
    pub fn arm_with_observed_len(&self, observed_len: u64) {
        if matches!(self.state.mode, PrepareMode::ZeroFill) && observed_len > 0 {
            let target = (observed_len + ZERO_FILL_MARGIN_BYTES).max(self.state.zero_fill_floor);
            self.state.zero_fill_target.store(target, Ordering::Relaxed);
        }
        self.arm();
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
    // the staging work ON the journal core whenever the journal thread
    // blocks. Reset to the default mask and SCHED_OTHER first, like
    // every other child of a pinned thread — then apply the configured
    // pin (if any) on top, so a pinned worker still runs SCHED_OTHER.
    if let Err(e) = melin_app::affinity::clear_affinity() {
        tracing::warn!(error = e, "failed to clear segment-preparer affinity");
    }
    melin_app::affinity::pin_thread("journal-prep", state.pin_core);
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

        match prepare_one(&state) {
            Ok(prepared) => {
                if let Ok(mut g) = state.slot.lock() {
                    *g = Some(prepared);
                }
            }
            Err(e) => {
                // A shutdown mid-prepare surfaces as an aborted zero
                // fill — exit quietly instead of warning about it.
                if state.shutdown.load(Ordering::Acquire) {
                    return;
                }
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
fn prepare_one(state: &State) -> Result<PreparedSegment, JournalError> {
    let staging = staging_path(&state.live_path);

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

    match state.mode {
        PrepareMode::Sector { sector_size } => prepare_sector(&staging, sector_size),
        PrepareMode::ZeroFill => prepare_zero_filled(
            &staging,
            state.zero_fill_target.load(Ordering::Relaxed),
            &state.shutdown,
        ),
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
///
/// Checks `shutdown` between chunks: zeroing scales with the rotation
/// threshold (multi-GiB segments take seconds at device bandwidth), and
/// `SegmentPreparer::shutdown` joins this thread — an unchecked loop
/// would hold up process exit for the remainder of the fill.
fn prepare_zero_filled(
    staging: &Path,
    bytes: u64,
    shutdown: &AtomicBool,
) -> Result<PreparedSegment, JournalError> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(staging)?;

    // Fast path: FALLOC_FL_WRITE_ZEROES (kernel ≥ 6.16 with fs
    // support) allocates *written* zeroed extents via the device's
    // Write Zeroes command — no data crosses the bus, so there is no
    // bandwidth burst to pace and no write amplification at all. The
    // pages are not left resident, so prefault them like the sector
    // path does (reading zeroed extents is a cheap sequential read).
    if try_fallocate_write_zeroes(&file, bytes) {
        prefault_pages(&file, 0, bytes);
        file.sync_all()?;
        return Ok(PreparedSegment {
            file,
            path: staging.to_path_buf(),
            allocated_end: bytes,
            // Not applicable — the buffered writer has no alignment
            // requirement and ignores this field.
            sector_size: 0,
        });
    }

    // Fallback: physically write the zeros, paced against the DEVICE
    // clock. An unpaced fill saturates the journal device — the ~2 ms
    // fsync beat this mode exists to fix was measured being replaced by
    // ~10 ms fsync stalls whenever a batch fsync queued behind the
    // staging burst. And pacing must not be clocked off the buffered
    // `write_all_at` calls: those return at memcpy speed (the writes
    // only dirty the page cache), so sleeping a multiple of *their*
    // wall time still dirtied gigabytes per second and left the device
    // flooded by async writeback — measured as ~7.5 ms rotation-
    // adjacent fsync stalls that only faded once the kernel's own
    // dirty-page throttling kicked in mid-run.
    //
    // The double-window pattern makes the loop device-clocked:
    //
    // - write chunk N (memcpy into cache), start async writeback on it;
    // - wait for chunk N-1's writeback to COMPLETE
    //   (`WAIT_BEFORE|WRITE|WAIT_AFTER`) — at most two chunks are ever
    //   in flight, and each iteration's wall time now includes real
    //   device time for one chunk;
    // - sleep 3× that wall time: a genuine ~25% device duty, so a live
    //   batch fsync waits behind at most one 2 MiB chunk (~1.5 ms at
    //   NVMe bandwidth). Self-adapting — no bandwidth constant — and
    //   staging duration stays ≈ 4× (segment bytes / device bandwidth),
    //   well inside any rotation period whose segment the device can
    //   write once.
    //
    // Data pacing alone is not enough: `sync_file_range` never logs
    // filesystem metadata, so the extent allocations for the entire
    // fill accumulate in the XFS CIL until something forces the log.
    // Leaving them all to the terminal `sync_all` detonates one
    // segment-sized log force — and log forces serialize
    // filesystem-wide, so a hot-path fdatasync (primary's or a
    // replica's) landing in that window queues behind it (measured as
    // ~9.4 ms rotation-adjacent pipeline stalls). The periodic
    // `sync_data` below caps each force at ~64 MiB worth of allocation
    // metadata, small enough that a colliding fdatasync waits
    // sub-millisecond — and the force runs here, on the preparer
    // thread, off the hot path.
    //
    // Writes go through the page cache deliberately: the pages the
    // buffered writer is about to append into stay resident, so its
    // partial trailing-page writes never pay a read-modify-write
    // device read on the hot path (which O_DIRECT staging would
    // reintroduce). std's `write_all_at` handles short writes and
    // EINTR retries. Heap Vec (not a stack array) — 2 MiB would
    // overflow default thread stacks.
    const ZERO_CHUNK: usize = 2 * 1024 * 1024;
    // Log-force cadence for the paced fill (see the comment above): at
    // ~25% device duty this is one small `sync_data` every ~100 ms,
    // each logging only the allocations made since the previous one.
    const METADATA_SYNC_INTERVAL: u64 = 64 * 1024 * 1024;
    let zeros = vec![0u8; ZERO_CHUNK];
    let mut offset: u64 = 0;
    let mut prev_chunk: Option<(u64, u64)> = None;
    let mut last_metadata_sync: u64 = 0;
    while offset < bytes {
        if shutdown.load(Ordering::Acquire) {
            return Err(JournalError::Io(io::Error::new(
                io::ErrorKind::Interrupted,
                "zero-fill aborted by shutdown",
            )));
        }
        let chunk_start = std::time::Instant::now();
        let n = (bytes - offset).min(zeros.len() as u64) as usize;
        std::os::unix::fs::FileExt::write_all_at(&file, &zeros[..n], offset)?;
        start_background_writeback(&file, offset, n as u64);
        if let Some((prev_off, prev_len)) = prev_chunk {
            wait_for_writeback(&file, prev_off, prev_len);
        }
        prev_chunk = Some((offset, n as u64));
        offset += n as u64;

        // Incremental log force: flushes the pending chunk's data plus
        // the extent-allocation metadata accumulated since the last
        // force. Runs before the pacing sleep so its wall time counts
        // toward this iteration's device-duty accounting.
        if offset - last_metadata_sync >= METADATA_SYNC_INTERVAL {
            file.sync_data()?;
            last_metadata_sync = offset;
        }

        if offset < bytes {
            std::thread::sleep(chunk_start.elapsed() * 3);
        }
    }
    // Cheap by construction: at most the final < 64 MiB of allocation
    // metadata (plus timestamps) remains unforced here.
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

/// Wait for writeback of `[offset, offset + len)` to complete
/// (`WAIT_BEFORE|WRITE|WAIT_AFTER`). The device-clock half of the
/// zero-fill's double-window pacing — the wait is the point, it makes
/// the loop's wall time reflect real device time. Best-effort like
/// [`start_background_writeback`]: on failure the fill just paces less
/// accurately and the final `sync_all` remains the durability point.
fn wait_for_writeback(file: &File, offset: u64, len: u64) {
    use std::os::fd::AsRawFd;
    // SAFETY: plain syscall on an owned, open fd; no memory is passed.
    // Result deliberately dropped — see the function docs.
    let _ = unsafe {
        libc::sync_file_range(
            file.as_raw_fd(),
            offset as libc::off64_t,
            len as libc::off64_t,
            libc::SYNC_FILE_RANGE_WAIT_BEFORE
                | libc::SYNC_FILE_RANGE_WRITE
                | libc::SYNC_FILE_RANGE_WAIT_AFTER,
        )
    };
}

/// `FALLOC_FL_WRITE_ZEROES` — not yet in the `libc` crate; merged
/// upstream in Linux 6.16 (mode bit after UNSHARE_RANGE = 0x40).
/// Allocates extents as physically-written zeros using the block
/// device's Write Zeroes command.
const FALLOC_FL_WRITE_ZEROES: libc::c_int = 0x80;

/// Whether `FALLOC_FL_WRITE_ZEROES` is worth attempting. Starts
/// optimistic; the first unsupported-kernel/filesystem error flips it
/// off for the process lifetime so every later prepare skips straight
/// to the paced fill. `Relaxed`: a racing extra probe is harmless.
static WRITE_ZEROES_SUPPORTED: AtomicBool = AtomicBool::new(true);

/// Try to zero `[0, bytes)` via `FALLOC_FL_WRITE_ZEROES`. Returns
/// `false` (and remembers the verdict) when the kernel or filesystem
/// doesn't support it; any other error also falls back to the paced
/// fill, which will surface a persistent fault through its own I/O
/// errors.
fn try_fallocate_write_zeroes(file: &File, bytes: u64) -> bool {
    use std::os::fd::AsRawFd;
    if !WRITE_ZEROES_SUPPORTED.load(Ordering::Relaxed) {
        return false;
    }
    // SAFETY: plain syscall on an owned, open fd; no memory is passed.
    let rc = unsafe {
        libc::fallocate(
            file.as_raw_fd(),
            FALLOC_FL_WRITE_ZEROES,
            0,
            bytes as libc::off64_t,
        )
    };
    if rc == 0 {
        return true;
    }
    let errno = io::Error::last_os_error();
    if matches!(
        errno.raw_os_error(),
        Some(libc::EOPNOTSUPP) | Some(libc::EINVAL) | Some(libc::ENOSYS)
    ) {
        WRITE_ZEROES_SUPPORTED.store(false, Ordering::Relaxed);
        tracing::info!(
            error = %errno,
            "FALLOC_FL_WRITE_ZEROES unsupported; zero-fill staging will use paced writes"
        );
    } else {
        tracing::warn!(
            error = %errno,
            "FALLOC_FL_WRITE_ZEROES failed; falling back to paced writes for this prepare"
        );
    }
    false
}

/// Ask the kernel to start (not wait for) writeback of `[offset,
/// offset + len)`. Best-effort pacing for the zero fill — a failure
/// only means the final `sync_all` flushes more at once, so the error
/// is deliberately ignored (the fill's durability comes from
/// `sync_all`, not from here).
fn start_background_writeback(file: &File, offset: u64, len: u64) {
    use std::os::fd::AsRawFd;
    // SAFETY: plain syscall on an owned, open fd; no memory is passed.
    // Result deliberately dropped — see the function docs.
    let _ = unsafe {
        libc::sync_file_range(
            file.as_raw_fd(),
            offset as libc::off64_t,
            len as libc::off64_t,
            libc::SYNC_FILE_RANGE_WRITE,
        )
    };
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

        let preparer = SegmentPreparer::spawn(live.clone(), 4096, 0);

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

        let preparer = SegmentPreparer::spawn(live, 4096, 0);

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
        let preparer = SegmentPreparer::spawn_zero_fill(live.clone(), threshold, 0);

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

    /// `arm_with_observed_len` retunes the zero-fill target: the next
    /// staged segment tracks the observed segment size (the replica
    /// path, where the primary's threshold is unknown), while the
    /// configured floor keeps a small observation from shrinking a
    /// size-driven primary's target.
    #[test]
    fn zero_fill_target_adapts_to_observed_len() {
        let dir = tempfile::tempdir().expect("tempdir");
        let live = dir.path().join("test.journal");

        // Replica-style spawn: threshold unknown (0) → chunk fallback.
        let _prealloc_guard = crate::prealloc::PreallocOverrideGuard::new(1024 * 1024);
        let preparer = SegmentPreparer::spawn_zero_fill(live, 0, 0);

        let take_one = || {
            for _ in 0..500 {
                if let Some(p) = preparer.take() {
                    return p;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            panic!("preparer should stage a segment within 5 s");
        };

        // Initial staging uses the chunk fallback.
        let first = take_one();
        assert_eq!(
            first.allocated_end,
            1024 * 1024 + ZERO_FILL_MARGIN_BYTES,
            "initial replica target = prealloc chunk + margin"
        );
        std::fs::remove_file(&first.path).expect("consume first staging");
        drop(first);

        // Observation retunes the target (no floor when threshold is 0).
        let observed: u64 = 3 * 1024 * 1024;
        preparer.arm_with_observed_len(observed);
        let second = take_one();
        assert_eq!(
            second.allocated_end,
            observed + ZERO_FILL_MARGIN_BYTES,
            "staged size must track the observed segment size"
        );

        drop(second);
        preparer.shutdown();
    }

    /// `arm` after `take` triggers a second preparation. Verifies the
    /// post-rotation rearm path used by the journal stage.
    #[test]
    fn rearm_after_take_produces_second_segment() {
        let dir = tempfile::tempdir().expect("tempdir");
        let live = dir.path().join("test.journal");

        let preparer = SegmentPreparer::spawn(live, 4096, 0);

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
