//! Background segment preparer — pre-stages the next journal segment off
//! the rotation hot path.
//!
//! ## Why
//!
//! With size-driven rotation enabled, every `max_journal_bytes` written
//! the journal stage calls `BufferedWriter::rotate_segment`, which
//! creates the next segment file and materialises its extents before
//! the first append can land. On PLP-class NVMe drives that ceremony is
//! a ~38 ms synchronous stall — directly visible in p99.99 of the event
//! pipeline.
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
//!      writes the file header (which carries the boundary's sequence +
//!      chain anchor) and renames the staging file into place. Cost:
//!      two renames + one dir fsync. The ~38 ms is gone. Until that
//!      rename, the taken segment holds the staging path and the worker
//!      does not re-stage there (see [`PreparedSegment`]).
//!   4. If `take` returns `None` (the worker hasn't caught up, manual
//!      rotation arrived early, or preparation errored), the writer
//!      falls back to today's synchronous path.
//!
//! ## Zero-fill staging
//!
//! [`StagingMode::ZeroFill`], the default, physically writes zeros over
//! the whole segment and syncs, and the physical writes are the point:
//! `FALLOC_FL_ZERO_RANGE` leaves extents unwritten, so every append
//! still converts them and every conversion is a logged filesystem
//! metadata transaction. Those transactions periodically force the
//! filesystem journal *inside the writer's `fdatasync`* (measured on XFS
//! as a ~2 ms CIL-force stall every 10.24 s that froze the whole event
//! pipeline — see `docs/internal/journal-fsync-beat-2026-08.md`).
//! Appends into pre-written extents carry no metadata, so `fdatasync`
//! stays on its data-only fast path. The cost is writing every segment
//! twice (zeros, then data) — sequential, off the hot path, and
//! documented as the write-amplification trade-off.
//!
//! ## When that trade inverts: [`StagingMode::Allocate`]
//!
//! Buying metadata-free `fdatasync` with sequential bandwidth is a good
//! deal on a local NVMe, where sequential bandwidth is close to free.
//! It is not obviously a good deal on network-attached storage (EBS and
//! friends), where bandwidth is a metered, purchased resource that the
//! staging pass and the hot path draw from the same budget, and where
//! the pacing below makes the preparer keep up only while
//!
//! ```text
//!     journal data rate < device bandwidth / 4
//! ```
//!
//! — a ratio in which the segment size cancels, so raising the rotation
//! threshold does not help. Past that point a deployment pays the
//! staging cost *and* still falls back to synchronous rotation, which is
//! the worst of both.
//!
//! [`StagingMode::Allocate`] is the other end of the trade: a plain
//! `fallocate`, no staging pass at all, unwritten extents, and the
//! periodic log force back on the flush path. It also needs no
//! prefault — reads of an unwritten extent are served from the zero page
//! without touching the device, which is exactly what the written
//! extents of `ZeroFill` give up.
//!
//! Which one wins on a given volume is an empirical question about that
//! volume, not something to settle from first principles. The mode
//! exists so it can be measured; `ZeroFill` remains the default because
//! it is what the tracing in the document above was done against.

use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::error::JournalError;
use crate::prealloc::Prealloc;

/// A fully-prepared journal segment file ready to be adopted by the
/// writer on the next rotation.
///
/// The file reads as zeros over `[0, allocated_end)`, its length and
/// allocation are durable, and whether the extents are *written* rather
/// than merely allocated is recorded in `written` — see the module docs
/// for why that distinction matters.
///
/// The file header is *not* yet written — the adopting writer
/// (`SegmentFile::install_prepared`) writes it at adopt time so it
/// reflects the rotation boundary's sequence + chain anchor.
///
/// A taken segment holds the staging path for as long as it exists. The
/// worker stages every segment at that one path, and it does not start
/// the next preparation until the taken segment is gone; the only way to
/// move the file into place, [`rename_onto`](Self::rename_onto), consumes
/// it and releases the path after the rename. Nothing else could stop the
/// worker from re-staging between [`SegmentPreparer::take`] and the
/// adopter's rename (an arm coalesced during the previous preparation
/// lets it start as soon as the slot is empty). That unlinked the taken
/// file and created a header-less one in its place, the rename then
/// installed the new file as the live segment, and the writer appended
/// a whole segment's entries to the unlinked inode, gone from the journal.
pub struct PreparedSegment {
    staged: Staged,
    /// Released (dropped) by `rename_onto` after the rename, or with the
    /// segment when it is dropped without being renamed.
    lease: StagingLease,
}

impl PreparedSegment {
    /// File handle, reused by the writer after the rename.
    pub fn file(&self) -> &File {
        &self.staged.file
    }

    /// Path of the staging file (`<live>.next-staging`).
    pub fn path(&self) -> &Path {
        &self.staged.path
    }

    /// End of the staged region (matches the writer's `allocated_end`).
    pub fn allocated_end(&self) -> u64 {
        self.staged.allocated_end
    }

    /// Whether `[0, allocated_end)` is physically written
    /// ([`StagingMode::ZeroFill`]) or only allocated
    /// ([`StagingMode::Allocate`]). The adopter uses it to tell a
    /// segment that *lost* its pre-written property by outgrowing the
    /// staged region from one that never had it.
    pub fn written(&self) -> bool {
        self.staged.written
    }

    /// Rename the staged file onto `live_path` and hand back its handle.
    ///
    /// Consumes the segment, so the staging path goes back to the worker
    /// only once the rename has happened, or failed: on failure the file
    /// stays at the staging path, and the next preparation removes it.
    pub fn rename_onto(self, live_path: &Path) -> io::Result<File> {
        let Self { staged, lease } = self;
        let renamed = std::fs::rename(&staged.path, live_path);
        // Not a moment earlier: until here the worker must not touch the
        // staging path.
        drop(lease);
        renamed.map(|()| staged.file)
    }

    /// A segment no preparer staged, for tests that drive an adoption by
    /// hand. Nothing waits on its release.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn for_test(file: File, path: PathBuf, allocated_end: u64, written: bool) -> Self {
        Self {
            staged: Staged {
                file,
                path,
                allocated_end,
                written,
            },
            lease: StagingLease { state: None },
        }
    }
}

/// A staged file before it is handed out: what the worker produces and
/// parks in the slot.
struct Staged {
    file: File,
    path: PathBuf,
    allocated_end: u64,
    written: bool,
}

/// The claim a taken [`PreparedSegment`] holds on the staging path.
/// Dropping it frees the slot and wakes a worker waiting to re-stage.
struct StagingLease {
    /// `None` only for [`PreparedSegment::for_test`], which no worker
    /// waits on. An `Option` rather than a dummy `State`: there is no
    /// slot to free.
    state: Option<Arc<State>>,
}

impl Drop for StagingLease {
    fn drop(&mut self) {
        let Some(state) = self.state.take() else {
            return;
        };
        // A poisoned lock means some thread panicked while holding it;
        // the slot is a plain enum, valid at every point, so free it
        // anyway: left `Leased`, the worker would wait for good.
        let mut slot = state
            .slot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        debug_assert!(
            matches!(*slot, Slot::Leased),
            "a lease exists only while the slot is leased"
        );
        *slot = Slot::Free;
        state.released.notify_one();
    }
}

/// The preparer's one-entry hand-off, and who owns the staging path.
enum Slot {
    /// Nothing staged and nothing handed out: the path is the worker's.
    Free,
    /// Staged, waiting for [`SegmentPreparer::take`].
    Ready(Staged),
    /// Taken: the [`PreparedSegment`] owns the path until it drops.
    Leased,
}

/// How the preparer materialises a staged segment's extents.
///
/// The two modes trade filesystem-metadata cost on the flush path
/// against device bandwidth off it; see the module docs for which way
/// the trade points on which class of storage.
///
/// A plain `Copy` enum rather than a config struct: there are exactly
/// two behaviours, they share no parameters, and the worker branches on
/// it once per prepare.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StagingMode {
    /// Physically materialise zeroed *written* extents over the whole
    /// segment, so appends generate no extent-conversion metadata and
    /// `fdatasync` stays data-only. Costs one extra pass over every
    /// segment. The default, and what the fsync-beat tracing was done
    /// against.
    #[default]
    ZeroFill,
    /// Allocate the extents and stop. No staging pass, no write
    /// amplification, no prefault needed; appends convert unwritten
    /// extents and `fdatasync` periodically forces the filesystem log.
    /// For volumes where the bandwidth `ZeroFill` spends costs more than
    /// the log forces it avoids.
    Allocate,
}

/// Extra staged bytes past the rotation threshold. The size trigger is
/// evaluated per flushed batch (≤ 512 KiB), so the live segment
/// overshoots the threshold by at most a batch plus whatever a
/// manual-rotation command lags by; 8 MiB of margin keeps even those
/// writes inside the staged region. Writes past the margin fall back to
/// `posix_fallocate` extension (rare; under `ZeroFill` it also costs the
/// metadata-per-append behaviour that mode exists to avoid, and the
/// writer logs the transition). A node's first segment, which no
/// preparer staged, reserves the same margin (see
/// [`Prealloc::for_rotation`]).
pub(crate) const STAGE_MARGIN_BYTES: u64 = 8 * 1024 * 1024;

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
    /// How staged extents are materialised. Fixed at spawn: changing it
    /// mid-run would leave already-staged segments in the other mode,
    /// and the choice is a property of the volume, which does not change
    /// under a running server.
    mode: StagingMode,
    /// How many bytes the next staged segment is sized to (zeroed or
    /// allocated, per `mode`). Seeded at spawn (threshold + margin, or
    /// the journal's prealloc chunk + margin when the threshold is
    /// unknown) and
    /// updated on
    /// every [`SegmentPreparer::arm_with_observed_len`] so the target
    /// tracks the segment sizes the deployment actually produces —
    /// critical on replicas, which never know the primary's
    /// `max_journal_bytes` and would otherwise stage a fixed default
    /// that silently under- or over-shoots it. `AtomicU64`: written by
    /// the journal thread at rotation time, read by the worker; no
    /// ordering dependency beyond the value itself (`Relaxed`) — a
    /// stale read stages one segment at the previous size, which the
    /// margin absorbs.
    stage_target: AtomicU64,
    /// Floor for `stage_target` updates: the configured rotation
    /// threshold + margin when known, else 0. Keeps an early manual
    /// rotation (tiny observed segment) from shrinking the target below
    /// what size-driven rotation needs.
    stage_floor: u64,
    /// A mutex because the slot is mutated from three places (the
    /// worker stages, the adopter takes, the lease frees) and holds at
    /// most one entry. No contention on the hot path — the lock is only
    /// acquired at rotation time and on prepare completion.
    slot: Mutex<Slot>,
    /// Paired with `slot`: wakes the worker when a lease is released or
    /// `shutdown` flips to `true`.
    released: Condvar,
    /// Times the worker has waited on a lease — the observation that it
    /// reached that point, which a test cannot get from the file system
    /// (the waiting is the absence of an action).
    #[cfg(test)]
    lease_waits: AtomicU64,
    /// Times the worker has skipped an arm because a staged segment was
    /// still waiting to be taken — like `lease_waits`, an absence of
    /// action a test can only observe through a counter.
    #[cfg(test)]
    ready_skips: AtomicU64,
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
    /// Spawn the worker thread, staging segments in `mode` (see the
    /// module docs for the trade the two modes sit at either end of).
    /// Arms immediately so the first rotation can adopt instead of
    /// paying the sync cost.
    ///
    /// Also clears any orphan `<live>.next-staging` file left behind by
    /// a crashed prior run — these files have no header and are not
    /// recognised by `segment::list_archives`, but leaving them on disk
    /// would cause `create_new` to fail at the next prepare.
    ///
    /// `rotate_threshold_bytes` is the size trigger the pipeline rotates
    /// at (`max_journal_bytes`); the staged file is initially sized to
    /// that plus `STAGE_MARGIN_BYTES`. Pass `0` when the threshold
    /// is unknown locally (replica mode — rotation follows the primary's
    /// announced boundaries): the initial target falls back to one
    /// `prealloc` chunk (the journal's, from its own node's threshold)
    /// plus margin, and every
    /// [`arm_with_observed_len`](Self::arm_with_observed_len) after a
    /// rotation retunes it to the segment sizes actually observed, so a
    /// replica converges on the primary's real segment size after its
    /// first adoption regardless of configuration mismatch.
    ///
    /// `pin_core`: core to pin the worker to, `0` = unpinned (the
    /// worker runs on the process's home CPU mask at `SCHED_OTHER`). Pinning
    /// keeps staging I/O bursts off the IRQ core and the pipeline
    /// cores deterministically — same convention as the shadow and
    /// event-publisher threads.
    pub fn spawn(
        live_path: PathBuf,
        rotate_threshold_bytes: u64,
        prealloc: Prealloc,
        pin_core: usize,
        mode: StagingMode,
    ) -> Self {
        let stage_floor = if rotate_threshold_bytes > 0 {
            rotate_threshold_bytes + STAGE_MARGIN_BYTES
        } else {
            0
        };
        let stage_target = if stage_floor > 0 {
            stage_floor
        } else {
            prealloc.chunk_bytes() + STAGE_MARGIN_BYTES
        };

        cleanup_staging_orphan(&live_path);

        let state = Arc::new(State {
            live_path,
            mode,
            stage_target: AtomicU64::new(stage_target),
            stage_floor,
            slot: Mutex::new(Slot::Free),
            released: Condvar::new(),
            #[cfg(test)]
            lease_waits: AtomicU64::new(0),
            #[cfg(test)]
            ready_skips: AtomicU64::new(0),
            // Pre-arm at startup so the worker prepares the first spare
            // segment in parallel with server warm-up. The first rotation
            // then has a ready segment to adopt.
            armed: Mutex::new(true),
            notify: Condvar::new(),
            shutdown: AtomicBool::new(false),
        });

        let worker_state = Arc::clone(&state);
        // Hand the worker its scheduling context before it exists. A
        // child inherits its creator's mask and policy at creation, and
        // the caller's placement is not ours to assume: the journal stage
        // starts this from whichever thread spawns its sequencing thread,
        // which may be pinned. A child cannot move itself off a core
        // whose real-time occupant never yields, because moving itself
        // requires running — so the reset cannot happen inside the worker.
        // A stuck preparer is kept rather than fail the journal: it is an
        // optimisation, and until it runs, rotation takes the synchronous
        // path it falls back to whenever no segment is staged.
        let handle = melin_app::affinity::spawn_placed("journal-prep", pin_core, move || {
            worker_loop(worker_state)
        })
        .or_else(melin_app::affinity::SpawnPlacedError::keep_stuck)
        .expect("failed to spawn journal-prep thread");

        Self {
            state,
            handle: Some(handle),
        }
    }

    /// [`arm`](Self::arm) plus a staging-target update from the size
    /// of the segment that just rotated out. Called by the journal
    /// stage after every rotation so staged segments track the sizes
    /// the deployment actually produces (a replica's only source of
    /// truth for the primary's segment size).
    pub fn arm_with_observed_len(&self, observed_len: u64) {
        if observed_len > 0 {
            let target = (observed_len + STAGE_MARGIN_BYTES).max(self.state.stage_floor);
            self.state.stage_target.store(target, Ordering::Relaxed);
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
    /// worker has finished a preparation that has not yet been taken.
    /// Called by the journal stage to decide between the fast adopt path
    /// and the sync fallback. The returned segment holds the staging
    /// path until it is renamed or dropped (see [`PreparedSegment`]).
    pub fn take(&self) -> Option<PreparedSegment> {
        let mut slot = self
            .state
            .slot
            .lock()
            .expect("preparer slot mutex poisoned");
        match std::mem::replace(&mut *slot, Slot::Leased) {
            Slot::Ready(staged) => Some(PreparedSegment {
                staged,
                lease: StagingLease {
                    state: Some(Arc::clone(&self.state)),
                },
            }),
            // Nothing staged (`Free`), or already taken (`Leased`): put
            // it back as it was.
            other => {
                *slot = other;
                None
            }
        }
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
        // And if it's waiting on a lease. Notified under the slot lock,
        // which the worker holds from its shutdown check until it waits,
        // so the wakeup cannot fall between the two.
        {
            // A poisoned slot lock still guards a valid enum, and the
            // wakeup is all that is needed here.
            let _slot = self
                .state
                .slot
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            self.state.released.notify_one();
        }
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
    // Already placed and pinned: `spawn` created this thread through
    // `spawn_placed`, because a child of a pinned SCHED_FIFO parent cannot
    // reconfigure itself — it would have to run first, on a core whose
    // occupant never yields.
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

        // Claim the staging path. A preparation still waiting to be taken
        // means this cycle has nothing to do — the slot has capacity for
        // one. A taken one still owns the path until the adopter's rename
        // (see `PreparedSegment`): wait it out, then prepare, since this
        // arm asked for a segment and nothing has staged one since.
        {
            let Ok(mut slot) = state.slot.lock() else {
                return; // poisoned; just exit
            };
            while matches!(*slot, Slot::Leased) {
                if state.shutdown.load(Ordering::Acquire) {
                    return;
                }
                #[cfg(test)]
                state.lease_waits.fetch_add(1, Ordering::Relaxed);
                slot = match state.released.wait(slot) {
                    Ok(g) => g,
                    Err(_) => return,
                };
            }
            if matches!(*slot, Slot::Ready(_)) {
                #[cfg(test)]
                state.ready_skips.fetch_add(1, Ordering::Relaxed);
                continue;
            }
        }
        // `Free` from here until this cycle stages: only the worker moves
        // the slot out of `Free`, so no segment holds the staging path.

        match prepare_one(&state) {
            Ok(staged) => {
                let Ok(mut slot) = state.slot.lock() else {
                    return; // poisoned; just exit
                };
                debug_assert!(
                    matches!(*slot, Slot::Free),
                    "only the worker moves the slot out of Free"
                );
                *slot = Slot::Ready(staged);
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
/// adopting writer, just before its rename.
///
/// Called only with the slot `Free`, so no taken segment holds the
/// staging path: whatever is there is stale.
fn prepare_one(state: &State) -> Result<Staged, JournalError> {
    let staging = staging_path(&state.live_path);

    // Remove any stale staging file: one a failed adoption left behind
    // (its rename never happened), or an orphan an external operator
    // left (a crashed run's is cleaned by `SegmentPreparer::spawn`).
    // `create_new` would fail with AlreadyExists on either. Treat
    // NotFound as success.
    match std::fs::remove_file(&staging) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        // Surface other errors via the create_new below — the caller
        // will see the real fault and log it. Traced here anyway
        // because the fault the caller reports is an unhelpful
        // AlreadyExists, not the EACCES/EIO that actually happened.
        Err(e) => {
            tracing::debug!(
                error = %e,
                path = %staging.display(),
                "could not remove stale staging file before prepare"
            );
        }
    }

    let bytes = state.stage_target.load(Ordering::Relaxed);
    match state.mode {
        StagingMode::ZeroFill => prepare_zero_filled(&staging, bytes, &state.shutdown),
        StagingMode::Allocate => prepare_allocated(&staging, bytes),
    }
}

/// Allocate-only staging: extents reserved, nothing materialised.
///
/// No paced pass and no prefault, because there is no device work to
/// pace and nothing to fault in: an unwritten extent reads as zeros
/// straight from the zero page, so the buffered writer's partial
/// trailing-page appends cost no device read either. That is the
/// property [`prepare_zero_filled`] deliberately gives up in exchange
/// for keeping `fdatasync` off the filesystem log, and the reason this
/// mode's staging is close to free.
///
/// `sync_all`, not `sync_data`: the whole product of this function *is*
/// metadata (the extent allocation), and it has to be durable before the
/// adopting writer renames the file into place.
fn prepare_allocated(staging: &Path, bytes: u64) -> Result<Staged, JournalError> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(staging)?;

    rustix::fs::fallocate(&file, rustix::fs::FallocateFlags::empty(), 0, bytes)
        .map_err(|e| JournalError::Io(io::Error::from_raw_os_error(e.raw_os_error())))?;
    file.sync_all()?;

    Ok(Staged {
        file,
        path: staging.to_path_buf(),
        allocated_end: bytes,
        written: false,
    })
}

/// Fault a file range into the page cache, best-effort.
///
/// The buffered writer's partial trailing-page appends would otherwise
/// pay a read-modify-write device read on the hot path, and a cold
/// page fault under load costs far more than the mapping does here.
///
/// `start` is aligned down to a 4 KiB page boundary (mmap offset
/// requirement). `Advice::PopulateRead` (kernel 5.14+) faults the pages
/// as *clean*, so a following `sync_all` doesn't have to write them
/// back — `PopulateWrite` would dirty the whole staged region and force
/// a full writeback even though nothing has been written yet.
///
/// Best-effort: silently skips on failure (e.g. insufficient VA space).
fn prefault_pages(file: &File, start: u64, end: u64) {
    if end <= start {
        return;
    }
    let aligned_start = start & !4095;
    let size = (end - aligned_start) as usize;

    // SAFETY: A read-only shared mapping of an owned `File`. The `Mmap`
    // guard ties the mapping lifetime to the value below and calls
    // `munmap` on drop; we drop it before this function returns. The
    // pages are read-only and never exposed to callers, so there is no
    // way for the rest of the program to observe stale or aliased memory
    // through this mapping.
    let mmap = unsafe {
        memmap2::MmapOptions::new()
            .offset(aligned_start)
            .len(size)
            .map(file)
    };
    let Ok(mmap) = mmap else {
        return;
    };
    // Best-effort kernel hint: `PopulateRead` faults pages in eagerly so the
    // next read avoids a synchronous page fault on the hot path. If the
    // kernel rejects the advice (older kernel, unusual mapping) we silently
    // proceed — the read will simply fault pages in lazily as before.
    let _ = mmap.advise(memmap2::Advice::PopulateRead);
}

/// Staging window size for both paced passes.
///
/// The window must be SMALL: an off-CPU trace of the journal thread
/// showed its per-batch fdatasyncs queueing behind in-flight staging
/// chunks for the duration of the fill — with 2 MiB chunks and a
/// two-chunk window that was 1-2 ms added to every hot-path flush in
/// the staging window (~7% of the rotation cycle), which alone put
/// end-to-end p99.9 at ~1.5 ms. One 256 KiB chunk is ~90 µs of device
/// time, keeping a colliding flush's detour well under the hot path's
/// own ~300 µs flush floor — while staying large enough that
/// sequential NVMe transfers run at full per-command efficiency.
const STAGING_WINDOW_BYTES: usize = 256 * 1024;

/// Sleep multiplier applied to each window's measured wall time: 3×
/// gives a ~25% device duty, so staging duration is ≈ 4× (segment
/// bytes / device bandwidth). `u32` because that is what
/// `Duration: Mul` takes.
const STAGING_DUTY_SLEEP_MULTIPLIER: u32 = 3;

/// Log-force cadence for the paced zero-fill: at ~25% device duty this
/// is one small `sync_data` every ~100 ms, each logging only the
/// allocations made since the previous one.
const METADATA_SYNC_INTERVAL_BYTES: u64 = 64 * 1024 * 1024;

/// Drive `step` over `[0, bytes)` in [`STAGING_WINDOW_BYTES`] windows
/// under the single-window pacing discipline, aborting on `shutdown`.
///
/// This is the one place the discipline lives, because it is the
/// property staging correctness rests on and it has already been got
/// wrong twice (see `docs/internal/journal-fsync-beat-2026-08.md`):
///
/// - `step` must complete its window's *device* work before returning,
///   so exactly one window is ever in flight and each iteration's wall
///   time is real device time. That caps how much staging I/O a
///   colliding hot-path fdatasync can ever queue behind — the failure
///   mode that put p99.9 at ~1.5 ms when two 2 MiB chunks were allowed
///   in flight.
/// - Sleeping a multiple of that *measured* time keeps the loop
///   device-clocked and self-adapting, with no bandwidth constant to
///   mis-tune. Clocking off anything cheaper (memcpy-speed
///   `write_all_at` returns, say) floods the device via async
///   writeback instead — measured as ~7.5 ms rotation-adjacent fsync
///   stalls.
///
/// `shutdown` is checked per window because staging scales with the
/// rotation threshold (multi-GiB segments take seconds at device
/// bandwidth) and `SegmentPreparer::shutdown` joins this thread — an
/// unchecked loop would hold up process exit for the remainder of the
/// pass.
fn paced_over_segment(
    bytes: u64,
    shutdown: &AtomicBool,
    mut step: impl FnMut(u64, usize) -> Result<(), JournalError>,
) -> Result<(), JournalError> {
    let mut offset: u64 = 0;
    while offset < bytes {
        if shutdown.load(Ordering::Acquire) {
            return Err(JournalError::Io(io::Error::new(
                io::ErrorKind::Interrupted,
                "segment staging aborted by shutdown",
            )));
        }
        let window_start = std::time::Instant::now();
        let n = (bytes - offset).min(STAGING_WINDOW_BYTES as u64) as usize;
        step(offset, n)?;
        offset += n as u64;
        if offset < bytes {
            std::thread::sleep(window_start.elapsed() * STAGING_DUTY_SLEEP_MULTIPLIER);
        }
    }
    Ok(())
}

/// Zero-fill staging: plain (page-cache) handle, `bytes` of zeros
/// materialised over `[0, bytes)` as *written* extents, then synced.
///
/// The header region `[0, ENTRY_OFFSET)` is covered too, so the
/// adopter's header pwrite also lands in written extents.
///
/// Both paths below leave the segment's pages resident in the page
/// cache. That is deliberate and load-bearing, not an optimisation:
/// the buffered writer's partial trailing-page appends would otherwise
/// pay a read-modify-write device read on the hot path.
///
/// Both paths are paced by [`paced_over_segment`] and abort on
/// `shutdown`.
fn prepare_zero_filled(
    staging: &Path,
    bytes: u64,
    shutdown: &AtomicBool,
) -> Result<Staged, JournalError> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(staging)?;

    // Fast path: FALLOC_FL_WRITE_ZEROES (kernel ≥ 6.16 with filesystem
    // support) allocates *written* zeroed extents via the device's
    // Write Zeroes command. The fallocate itself moves no data over
    // the bus and costs no write amplification at all — but it leaves
    // the pages non-resident, and because the extents are genuinely
    // *written* (the whole point over ZERO_RANGE) the filesystem no
    // longer knows the range is zeros: faulting them in is a real
    // sequential read off the journal device, not the free zero-page
    // fill the sector path's unwritten extents get.
    //
    // So the prefault is paced exactly like the write fallback below.
    // An unpaced whole-segment read burst alongside the live journal
    // is the same collision class as the write burst — the trace that
    // forced the single-window shape showed ~1.2-1.4 ms of in-flight
    // staging I/O landing directly in p99.9, and reads consume the
    // same queue slots.
    //
    // PROPHYLACTIC: unlike every other claim in this module, this one
    // is reasoned rather than traced. The path is dormant on the bench
    // fleet (Debian 6.12; needs ≥ 6.16), so it has never been
    // exercised under load, and the collision may well be milder than
    // the write case — on a drive where Write Zeroes is an
    // FTL-metadata operation the reads may be served from the mapping
    // table without NAND access. Pacing it costs nothing here and
    // removes the possibility of a kernel upgrade silently
    // reintroducing a stall class; measure before assuming the pacing
    // is what makes it safe.
    if try_fallocate_write_zeroes(&file, bytes) {
        paced_over_segment(bytes, shutdown, |offset, n| {
            prefault_pages(&file, offset, offset + n as u64);
            Ok(())
        })?;
        file.sync_all()?;
        return Ok(Staged {
            file,
            path: staging.to_path_buf(),
            allocated_end: bytes,
            written: true,
        });
    }

    // Fallback: physically write the zeros, paced against the DEVICE
    // clock. An unpaced fill saturates the journal device — the ~2 ms
    // fsync beat this mode exists to fix was measured being replaced by
    // ~10 ms fsync stalls whenever a batch fsync queued behind the
    // staging burst.
    //
    // Each window writes a chunk (memcpy into cache) and then waits for
    // ITS writeback (`WAIT_BEFORE|WRITE|WAIT_AFTER`), which is what
    // makes the window's wall time real device time — see
    // `paced_over_segment` for why that discipline is the load-bearing
    // part.
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
    // std's `write_all_at` handles short writes and EINTR retries.
    // Heap Vec (not a stack array) to keep the worker's stack
    // footprint trivial.
    let zeros = vec![0u8; STAGING_WINDOW_BYTES];
    let mut last_metadata_sync: u64 = 0;
    paced_over_segment(bytes, shutdown, |offset, n| {
        std::os::unix::fs::FileExt::write_all_at(&file, &zeros[..n], offset)?;
        wait_for_writeback(&file, offset, n as u64);
        // Incremental log force for the extent-allocation metadata
        // accumulated since the last force (the data is already on
        // disk chunk-by-chunk). Runs inside the window so its wall
        // time counts toward this iteration's device-duty accounting.
        let end = offset + n as u64;
        if end - last_metadata_sync >= METADATA_SYNC_INTERVAL_BYTES {
            file.sync_data()?;
            last_metadata_sync = end;
        }
        Ok(())
    })?;
    // Cheap by construction: at most the final < 64 MiB of allocation
    // metadata (plus timestamps) remains unforced here.
    file.sync_all()?;

    Ok(Staged {
        file,
        path: staging.to_path_buf(),
        allocated_end: bytes,
        written: true,
    })
}

/// Start writeback of `[offset, offset + len)` and wait for it to
/// complete (`WAIT_BEFORE|WRITE|WAIT_AFTER`). The device clock of the
/// zero-fill's [`paced_over_segment`] window — the wait is the point:
/// it makes the window's wall time reflect real device time and
/// guarantees no staging IO stays in flight into the next one.
/// Best-effort: on failure the fill just paces less accurately and the
/// final `sync_all` remains the durability point.
fn wait_for_writeback(file: &File, offset: u64, len: u64) {
    // A zero-length window has nothing to wait for (and `sync_file_range`
    // would read the zero as "to the end of the file").
    let Some(len) = std::num::NonZeroU64::new(len) else {
        return;
    };
    // Result deliberately dropped: best-effort, see the function docs.
    let _ = crate::segment::writeback_and_wait(file, offset, len);
}

/// `FALLOC_FL_WRITE_ZEROES` — not yet in the `libc` crate; merged
/// upstream in Linux 6.16. `0x80`, the next mode bit above
/// `FALLOC_FL_UNSHARE_RANGE` (`0x40`). Allocates extents as
/// physically-written zeros using the block device's Write Zeroes
/// command.
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

/// Remove a stale `<live>.next-staging` file left behind by a prior
/// process that crashed mid-prepare or rotated without consuming the
/// staged segment.
///
/// Called from two places:
///   - [`SegmentPreparer::spawn`] when rotation is enabled
///     (the preparer would otherwise fail at `create_new` on the same
///     path).
///   - [`crate::buffered_writer::BufferedWriter::create`] and
///     `::open_append` so the orphan is reclaimed even when rotation is
///     disabled (no preparer ever runs).
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

/// Wait for the worker to stage a segment, and take it. Staging a few
/// MiB on tmpfs is milliseconds; the bound only keeps a broken preparer
/// from hanging the suite.
#[cfg(test)]
pub(crate) fn take_within(preparer: &SegmentPreparer) -> PreparedSegment {
    let mut taken = None;
    wait_until(
        || {
            taken = preparer.take();
            taken.is_some()
        },
        "preparer should stage a segment within 10 s",
    );
    taken.expect("wait_until returned, so take yielded a segment")
}

/// Poll `done` until it holds, failing with `msg` after 10 s: the one
/// bound every preparer test waits under.
#[cfg(test)]
pub(crate) fn wait_until(mut done: impl FnMut() -> bool, msg: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !done() {
        assert!(std::time::Instant::now() < deadline, "{msg}");
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::PREALLOC;
    use std::os::unix::fs::MetadataExt;

    /// Spawning, arming, and shutdown round-trips without leaking the
    /// worker thread or the staging file.
    #[test]
    fn spawn_prepare_shutdown_roundtrip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let live = dir.path().join("test.journal");
        // Live file doesn't need to exist — the preparer only touches
        // the staging sibling.

        let threshold: u64 = 1024 * 1024;
        let preparer =
            SegmentPreparer::spawn(live.clone(), threshold, PREALLOC, 0, StagingMode::ZeroFill);

        let prepared = take_within(&preparer);

        assert_eq!(prepared.path(), staging_path(&live));
        assert_eq!(prepared.allocated_end(), threshold + STAGE_MARGIN_BYTES);

        // Drop the file before shutdown so the staging file can be
        // cleaned by the test harness.
        let staging = prepared.path().to_path_buf();
        drop(prepared);

        preparer.shutdown();

        // Staging file still on disk (we took ownership and dropped it
        // without renaming). Cleanup is the adopter's responsibility in
        // production; here we just verify nothing else leaked.
        assert!(staging.exists(), "staging file should still exist on disk");
    }

    /// `StagingMode::Allocate` stages a segment of the same size and
    /// with the same observable contents as `ZeroFill` — the reader's
    /// end-of-data detection keys off zero bytes, so an allocate-mode
    /// segment that read as anything else would break replay.
    ///
    /// The distinguishing property of the mode (extents allocated but
    /// not materialised, so the staging pass moves no data) is not
    /// asserted here: whether the blocks are physically reserved is a
    /// filesystem decision, and tmpfs, ext4 and xfs each report it
    /// differently. What must hold everywhere is the contract below.
    #[test]
    fn allocate_mode_stages_a_zero_reading_segment() {
        let dir = tempfile::tempdir().expect("tempdir");
        let live = dir.path().join("test.journal");

        let threshold: u64 = 1024 * 1024;
        let preparer =
            SegmentPreparer::spawn(live.clone(), threshold, PREALLOC, 0, StagingMode::Allocate);

        let prepared = take_within(&preparer);

        assert_eq!(prepared.path(), staging_path(&live));
        assert_eq!(prepared.allocated_end(), threshold + STAGE_MARGIN_BYTES);

        let staging = prepared.path().to_path_buf();
        drop(prepared);
        preparer.shutdown();

        // `fallocate` extends the file to the allocated end, so the
        // adopter sees the same length a zero-filled stage would leave.
        let len = std::fs::metadata(&staging).expect("staging metadata").len();
        assert_eq!(len, threshold + STAGE_MARGIN_BYTES);

        let contents = std::fs::read(&staging).expect("read staging");
        assert!(
            contents.iter().all(|b| *b == 0),
            "an allocate-mode segment must read as zeros"
        );
    }

    /// `spawn` removes a leftover staging file from a prior crash.
    #[test]
    fn spawn_cleans_orphan_staging_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let live = dir.path().join("test.journal");
        let staging = staging_path(&live);

        std::fs::write(&staging, b"orphan from prior crash").expect("write orphan");
        assert!(staging.exists());

        let preparer =
            SegmentPreparer::spawn(live, 1024 * 1024, PREALLOC, 0, StagingMode::ZeroFill);

        // The orphan should be gone immediately; the worker will then
        // create a fresh staging file.
        // Wait for the worker to produce a fresh prepared segment to
        // confirm spawn() didn't fail mid-cleanup.
        let prepared = take_within(&preparer);
        drop(prepared);

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
        let preparer =
            SegmentPreparer::spawn(live.clone(), threshold, PREALLOC, 0, StagingMode::ZeroFill);

        let prepared = take_within(&preparer);

        let expected = threshold + STAGE_MARGIN_BYTES;
        assert_eq!(prepared.allocated_end(), expected);
        assert_eq!(
            std::fs::metadata(prepared.path())
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
                .file()
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

    /// `arm_with_observed_len` retunes the staging target: the next
    /// staged segment tracks the observed segment size (the replica
    /// path, where the primary's threshold is unknown), while the
    /// configured floor keeps a small observation from shrinking a
    /// size-driven primary's target.
    #[test]
    fn stage_target_adapts_to_observed_len() {
        let dir = tempfile::tempdir().expect("tempdir");
        let live = dir.path().join("test.journal");

        // Replica-style spawn: threshold unknown (0) → chunk fallback.
        let preparer = SegmentPreparer::spawn(live, 0, PREALLOC, 0, StagingMode::ZeroFill);

        // Initial staging uses the chunk fallback.
        let first = take_within(&preparer);
        assert_eq!(
            first.allocated_end(),
            PREALLOC.chunk_bytes() + STAGE_MARGIN_BYTES,
            "initial replica target = prealloc chunk + margin"
        );
        std::fs::remove_file(first.path()).expect("consume first staging");
        drop(first);

        // Observation retunes the target (no floor when threshold is 0).
        let observed: u64 = 3 * 1024 * 1024;
        preparer.arm_with_observed_len(observed);
        let second = take_within(&preparer);
        assert_eq!(
            second.allocated_end(),
            observed + STAGE_MARGIN_BYTES,
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

        let preparer =
            SegmentPreparer::spawn(live, 1024 * 1024, PREALLOC, 0, StagingMode::ZeroFill);

        // First prepared segment.
        let first = take_within(&preparer);
        // Simulate adoption by dropping + removing the staging file.
        std::fs::remove_file(first.path()).expect("remove first staging file");
        drop(first);

        // Re-arm and wait for the second.
        preparer.arm();
        drop(take_within(&preparer));

        preparer.shutdown();
    }

    /// Regression: an arm that is already pending when a segment is
    /// taken (in the journal stage, one coalesced while the worker was
    /// staging that segment, by a rotation that had to take the
    /// synchronous path) must not let the worker re-stage before the
    /// adopter has renamed the taken file onto the live path. It did:
    /// the worker unlinked the taken file and created a new, header-less
    /// one at the staging path, the adopter's rename installed that one
    /// as the live segment, and the writer appended the segment's
    /// entries to the unlinked inode. The next rotation then wrote its
    /// header (the next segment's anchor) into the archive and failed
    /// its own rename, and recovery found a segment chain break.
    ///
    /// Driven step by step: the worker's wait is observed through
    /// `lease_waits`, so nothing here depends on how the threads are
    /// scheduled.
    #[test]
    fn taken_segment_keeps_the_staging_path_until_renamed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let live = dir.path().join("test.journal");
        let staging = staging_path(&live);
        let preparer = SegmentPreparer::spawn(
            live.clone(),
            1024 * 1024,
            PREALLOC,
            0,
            StagingMode::ZeroFill,
        );

        let taken = take_within(&preparer);
        let taken_ino = taken.file().metadata().expect("fstat taken").ino();
        preparer.arm();
        wait_for_lease_wait(&preparer);

        assert!(
            preparer.take().is_none(),
            "nothing can be staged while the staging path is leased"
        );
        assert_eq!(
            ino(&staging),
            taken_ino,
            "the worker must leave the taken file at the staging path"
        );

        let file = taken.rename_onto(&live).expect("install the taken segment");
        assert_eq!(
            ino(&live),
            taken_ino,
            "the live segment must be the file the adopter took"
        );
        drop(file);

        // Released: the worker serves the arm it waited with, at the
        // same path, and leaves the installed segment alone.
        let next = take_within(&preparer);
        assert_eq!(next.path(), staging);
        assert_ne!(ino(&staging), taken_ino);
        assert_eq!(ino(&live), taken_ino);
        drop(next);
        preparer.shutdown();
    }

    /// A failed rename gives the staging path back too: the adoption is
    /// over, and the worker removes the leftover and stages afresh.
    #[test]
    fn failed_rename_releases_the_staging_path() {
        let dir = tempfile::tempdir().expect("tempdir");
        let live = dir.path().join("test.journal");
        let preparer = SegmentPreparer::spawn(
            live.clone(),
            1024 * 1024,
            PREALLOC,
            0,
            StagingMode::ZeroFill,
        );

        let taken = take_within(&preparer);
        let taken_ino = taken.file().metadata().expect("fstat taken").ino();
        preparer.arm();
        wait_for_lease_wait(&preparer);

        let unreachable = dir.path().join("no-such-dir").join("test.journal");
        assert!(
            taken.rename_onto(&unreachable).is_err(),
            "a rename into a missing directory fails"
        );

        let next = take_within(&preparer);
        assert_eq!(next.path(), staging_path(&live));
        assert_ne!(
            ino(next.path()),
            taken_ino,
            "the next segment is a fresh file, not the failed adoption's"
        );
        drop(next);
        preparer.shutdown();
    }

    /// Shutdown wakes a worker waiting on a lease; the join must not
    /// wait for the adoption to finish.
    #[test]
    fn shutdown_wakes_a_worker_waiting_on_a_lease() {
        let dir = tempfile::tempdir().expect("tempdir");
        let live = dir.path().join("test.journal");
        let preparer =
            SegmentPreparer::spawn(live, 1024 * 1024, PREALLOC, 0, StagingMode::ZeroFill);

        let taken = take_within(&preparer);
        preparer.arm();
        wait_for_lease_wait(&preparer);

        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let joiner = std::thread::spawn(move || {
            preparer.shutdown();
            done_tx.send(()).expect("test thread is waiting");
        });
        done_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("shutdown must return while a lease is outstanding");
        joiner.join().expect("shutdown thread");
        // Released after the worker is gone: nothing left to wake.
        drop(taken);
    }

    /// An arm arriving while a staged segment is still waiting to be
    /// taken is a no-op: the slot holds one segment, so the worker skips
    /// the cycle and leaves the staged file, inode and all, for `take`.
    #[test]
    fn arm_while_ready_keeps_the_staged_segment() {
        let dir = tempfile::tempdir().expect("tempdir");
        let live = dir.path().join("test.journal");
        let staging = staging_path(&live);
        let preparer =
            SegmentPreparer::spawn(live, 1024 * 1024, PREALLOC, 0, StagingMode::ZeroFill);

        wait_until(
            || matches!(*preparer.state.slot.lock().expect("slot"), Slot::Ready(_)),
            "the startup arm should stage a segment within 10 s",
        );
        let staged_ino = ino(&staging);

        preparer.arm();
        wait_until(
            || preparer.state.ready_skips.load(Ordering::Relaxed) > 0,
            "the worker should skip the arm within 10 s",
        );
        assert_eq!(
            ino(&staging),
            staged_ino,
            "the skipped arm must leave the staged file in place"
        );

        let taken = preparer.take().expect("the staged segment is still there");
        assert_eq!(taken.path(), staging);
        assert_eq!(
            taken.file().metadata().expect("fstat taken").ino(),
            staged_ino,
            "take must yield the segment staged before the arm"
        );
        drop(taken);
        preparer.shutdown();
    }

    /// Wait until the worker has consumed an arm and found the staging
    /// path leased. Bounded like [`take_within`].
    fn wait_for_lease_wait(preparer: &SegmentPreparer) {
        wait_until(
            || preparer.state.lease_waits.load(Ordering::Relaxed) > 0,
            "the worker should wait on the lease within 10 s",
        );
    }

    fn ino(path: &Path) -> u64 {
        std::fs::metadata(path).expect("stat").ino()
    }
}
