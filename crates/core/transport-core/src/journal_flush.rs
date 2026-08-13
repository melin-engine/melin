//! Journal flush executor — the durability half of the journal stage.
//!
//! The journal stage's loop is sequential today: encode → `pwrite` →
//! `fdatasync` → publish cursors. A stalled `fdatasync` therefore stalls
//! encoding and the replication feed, which is what caps the durability
//! gate's ability to mask disk latency. This module holds the seam that
//! moves `fdatasync` off the journal thread: the journal thread hands
//! over a [`FlushWatermark`] and continues; an executor syncs and
//! publishes.
//!
//! See `docs/internal/journal-async-flush-2026-08.md` for the full
//! design argument. The three properties this module exists to hold:
//!
//! 1. **Sample before sync, publish the sample.** `fdatasync` only
//!    covers data dirtied before the call, so a watermark re-read after
//!    the syscall returns could claim durability for bytes the sync
//!    never covered. The executor publishes its pre-sync local copy.
//! 2. **Coalescing.** `fdatasync` is cumulative, so the handoff is a
//!    latest-value cell rather than a queue — one sync covers every
//!    batch written while it ran. A queue would also need a full
//!    policy, which would re-block the journal thread.
//! 3. **Termination.** Every wait on the executor also watches poison
//!    and shutdown, so a dead executor surfaces its error instead of
//!    wedging the journal thread.
//!
//! The publication itself is injected rather than hard-wired: production
//! stores the durable cursor, `FsyncState`, the advertised tip and the
//! input-ring progress cursor; tests observe. Same for the sync call,
//! which is what lets the tests below stall and fail a flush
//! deterministically.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use melin_pipeline::seqlock::{NoPadding, SeqLockReader, SeqLockWriter, split};

use crate::cursors::{RingPos, WireSeq};

/// Everything the executor needs to publish a completed flush, sampled
/// on the journal thread at submit time.
///
/// The executor holds no reference to the writer or the input-ring
/// consumer, so every value it publishes must ride in here. Read
/// atomically as one unit — tearing `journal_seq` against `chain_hash`
/// would hand a replica a mismatched handshake hash, which is the exact
/// TOCTOU the seqlock exists to prevent.
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct FlushWatermark {
    /// Monotonic submit counter. The executor's "is there new work?"
    /// test, and the space [`FlushHandle::drain`] waits in.
    ///
    /// Deliberately not `journal_seq`: a batch of queries advances ring
    /// progress without journaling anything, so `journal_seq` can repeat
    /// across submits. Gating on it would leave that batch's progress
    /// unpublished and eventually stall the producer against a ring that
    /// never drains.
    pub submit_seq: u64,
    /// Highest wire sequence covered by this watermark
    /// (`writer.next_sequence() - 1` after the batch's `pwrite`).
    pub journal_seq: WireSeq,
    /// Input-ring position the flush covers — the value that becomes
    /// `Consumer::set_progress`. On a replica this is what gates
    /// persisted acks, so it must never be published before the sync.
    pub ring_progress: RingPos,
    /// `consumer.next_read()` at submit time, for `FsyncState`. **Not**
    /// the same value as `ring_progress`: at a mid-batch mark barrier
    /// only a prefix of the read batch is encoded, and the shadow
    /// snapshot compares this field for exact equality against its own
    /// cursor.
    pub input_ring_seq: RingPos,
    /// BLAKE3 chain hash after the batch. `[0u8; 32]` when hash-chain is
    /// disabled.
    pub chain_hash: [u8; 32],
    /// Rotation epoch. Bumped by the journal thread after a segment
    /// swap; a watermark whose generation no longer matches the live
    /// segment is inert.
    pub generation: u64,
    /// Descriptor to sync, or [`NOTHING_TO_SYNC`] when the bytes are
    /// already durable — which is what `JournalWrite::write_batch`
    /// reports as `None` (an empty batch, or a writer like the
    /// `O_DIRECT` sector writer that syncs inline). Such a watermark
    /// still publishes: cursors have to advance even when no new
    /// durability work was owed.
    ///
    /// A raw fd rather than an owned handle: the cell is a `Copy`
    /// payload and `Arc<File>` is neither `Copy` nor padding-free.
    /// Validity rests on the rotation drain — the journal thread never
    /// closes the live `File` while a sync is in flight.
    ///
    /// Widened from `RawFd` (`i32`) to `i64` so the struct stays
    /// padding-free without an explicit filler field.
    pub fd: i64,
}

/// `fd` value meaning "no sync is owed for this watermark".
///
/// Distinct from a real descriptor by construction — the kernel never
/// hands out a negative fd — so the executor can branch on it without a
/// second field.
pub const NOTHING_TO_SYNC: i64 = -1;

// Safety: `repr(C)` over padding-free fields — `WireSeq` and `RingPos`
// are `repr(transparent)` over `u64`, the rest are primitives and a byte
// array — with the assertion below proving the size equals the sum of
// the field sizes. Under `repr(C)`, that equality rules out padding.
unsafe impl NoPadding for FlushWatermark {}
// Compile-time proof for the impl above; fails the build if a future
// field introduces padding.
const _: () = assert!(
    size_of::<FlushWatermark>()
        == size_of::<u64>()
            + size_of::<WireSeq>()
            + size_of::<RingPos>()
            + size_of::<RingPos>()
            + size_of::<[u8; 32]>()
            + size_of::<u64>()
            + size_of::<i64>()
);

/// Cross-thread state shared by the journal thread and its executor.
///
/// Three atomics rather than a channel: the journal thread's reads are
/// on the hot path (the self-clock consults `published_submit` once per
/// loop) and none of the transitions need a wakeup — the executor is
/// already spinning on the cell.
#[derive(Debug, Default)]
pub struct FlushShared {
    /// Highest `submit_seq` the executor has finished publishing. Drives
    /// both the self-clock (`published_submit == last_submitted` means
    /// the disk is idle, so submit now) and [`FlushHandle::drain`].
    published_submit: AtomicU64,
    /// Highest `journal_seq` published, for the flush-lag gauge. Split
    /// from `published_submit` because operators want lag in wire-seq
    /// space, while the protocol needs a counter that always advances.
    published_journal_seq: AtomicU64,
    /// Latched when a sync fails. The executor stops publishing and the
    /// journal thread surfaces the error through its fatal-shutdown
    /// path one iteration later.
    poisoned: AtomicBool,
    /// Raw OS error code behind `poisoned`, for the operator-facing
    /// message. Stored before `poisoned` is set, so any thread that
    /// observes the flag with `Acquire` sees this too.
    poison_errno: AtomicU64,
}

impl FlushShared {
    /// Highest `journal_seq` the executor has made durable and
    /// published.
    #[inline]
    pub fn published_journal_seq(&self) -> WireSeq {
        WireSeq::new(self.published_journal_seq.load(Ordering::Acquire))
    }

    /// Whether a sync has failed. Checked once per journal-loop
    /// iteration.
    #[inline]
    pub fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Acquire)
    }

    /// The `errno` behind [`is_poisoned`](Self::is_poisoned), or `None`
    /// if healthy. `0` is reported as `None` — a failed sync always
    /// carries an OS error.
    pub fn poison_errno(&self) -> Option<i32> {
        if !self.is_poisoned() {
            return None;
        }
        let raw = self.poison_errno.load(Ordering::Acquire);
        (raw != 0).then_some(raw as i32)
    }
}

/// Outcome of [`FlushHandle::drain`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainOutcome {
    /// The executor published everything submitted so far. The caller
    /// may now swap the segment.
    Drained,
    /// The executor failed a sync and will never publish again. The
    /// caller must abandon the rotation and surface the error rather
    /// than wait.
    Poisoned,
    /// Shutdown was requested mid-wait.
    ShuttingDown,
}

/// Journal-thread half of the seam.
///
/// Not `Clone`: it owns the seqlock's unique writer handle, which is
/// what makes "exactly one thread publishes watermarks" a compile-time
/// property rather than a documented obligation.
pub struct FlushHandle {
    cell: SeqLockWriter<FlushWatermark>,
    shared: Arc<FlushShared>,
    /// Mirrors the last submitted `submit_seq` without an atomic load —
    /// the journal thread is the only writer of the cell.
    last_submitted: u64,
}

impl FlushHandle {
    /// Hand a completed `pwrite` to the executor. Never blocks: the cell
    /// is latest-value, so overwriting a watermark the executor has not
    /// consumed is the coalescing path, not a lost update.
    ///
    /// `submit_seq` is assigned here so callers cannot get it wrong.
    #[inline]
    pub fn submit(&mut self, mut watermark: FlushWatermark) {
        self.last_submitted += 1;
        watermark.submit_seq = self.last_submitted;
        self.cell.store(watermark);
    }

    /// Whether the executor has caught up with every submit — i.e. the
    /// disk is idle and a submit now would start a sync immediately.
    ///
    /// This is the self-clock: submitting only when idle (or when the
    /// batch hits its size or age bound) reproduces the batch-size
    /// distribution today's inline flush produces, because that inline
    /// flush is itself a self-clock.
    #[inline]
    pub fn is_idle(&self) -> bool {
        self.shared.published_submit.load(Ordering::Acquire) == self.last_submitted
    }

    /// Submits not yet published, in wire-seq space — the flush-lag
    /// health gauge. Zero in steady state; a growing value is a stalling
    /// disk the pipeline is riding through.
    #[inline]
    pub fn lag(&self, submitted_journal_seq: WireSeq) -> u64 {
        submitted_journal_seq
            .get()
            .saturating_sub(self.shared.published_journal_seq.load(Ordering::Acquire))
    }

    /// Block until the executor has published everything submitted so
    /// far, or until it poisons, or until shutdown.
    ///
    /// Required before any segment swap: the executor holds a raw fd
    /// from the cell, so closing the live `File` with a sync in flight
    /// would land that sync on a closed — or worse, recycled —
    /// descriptor. The `generation` field guards a stale *publication*;
    /// only this drain guards the *syscall*.
    ///
    /// Checks "drained" before "poisoned" deliberately: an executor that
    /// published up to the target and then failed on a later watermark
    /// has still satisfied this caller.
    pub fn drain(&self, shutdown: &AtomicBool, busy_spin: bool) -> DrainOutcome {
        let target = self.last_submitted;
        let mut idle_spins: u32 = 0;
        loop {
            if self.shared.published_submit.load(Ordering::Acquire) >= target {
                return DrainOutcome::Drained;
            }
            if self.shared.is_poisoned() {
                return DrainOutcome::Poisoned;
            }
            if shutdown.load(Ordering::Relaxed) {
                return DrainOutcome::ShuttingDown;
            }
            crate::pipeline::idle_wait(&mut idle_spins, busy_spin);
        }
    }

    /// Shared state, for wiring the health endpoint and the journal
    /// thread's poison check.
    #[inline]
    pub fn shared(&self) -> &Arc<FlushShared> {
        &self.shared
    }
}

/// The publication half of a flush: everything that follows an inline
/// `flush_batch_sync` in the journal stage today.
///
/// Owns the handles rather than borrowing them from the journal stage,
/// because the executor thread runs with no reference to the writer or
/// the input-ring consumer. Collecting them here is what makes the move
/// off the journal thread a change of *owner* rather than a change of
/// behaviour — and it keeps the single-writer property on the
/// `FsyncState` seqlock structural, since this struct holds the only
/// writer handle.
pub struct CursorPublisher {
    /// The journal consumer's `processed` counter. Producers gate slot
    /// reuse on it and — load-bearing — the replica ack path gates
    /// persisted acks on it, so it must only ever advance behind a
    /// completed sync.
    progress: Arc<melin_pipeline::padding::Sequence>,
    /// Post-fsync state for the shadow snapshot stage and replica
    /// handshakes. `None` when shadow snapshots are disabled.
    fsync_state: Option<SeqLockWriter<crate::pipeline::FsyncState>>,
    /// Highest wire seq durably persisted, read by the durability gate,
    /// the health endpoint, and the replica reconnect handshake.
    durable_wire_seq: Option<crate::cursors::DurableWireSeqCursor>,
    /// Control-plane advertised tip. Wired on primaries only — on a
    /// replica the replication receiver owns the tip at its in-memory
    /// accepted position.
    advertised_tip: Option<crate::cursors::AdvertisedJournalTip>,
}

impl CursorPublisher {
    /// Build a publisher over the journal consumer's progress counter.
    /// The optional handles are installed separately, matching how the
    /// pipeline wires them.
    pub fn new(progress: Arc<melin_pipeline::padding::Sequence>) -> Self {
        Self {
            progress,
            fsync_state: None,
            durable_wire_seq: None,
            advertised_tip: None,
        }
    }

    pub fn set_fsync_state(&mut self, writer: SeqLockWriter<crate::pipeline::FsyncState>) {
        self.fsync_state = Some(writer);
    }

    pub fn set_durable_wire_seq(&mut self, cursor: crate::cursors::DurableWireSeqCursor) {
        self.durable_wire_seq = Some(cursor);
    }

    pub fn set_advertised_tip(&mut self, tip: crate::cursors::AdvertisedJournalTip) {
        self.advertised_tip = Some(tip);
    }

    /// Publish post-flush writer state *without* advancing ring
    /// progress.
    ///
    /// Used where the writer's durable state changed but no new input
    /// was consumed — after a rotation, whose fresh segment gives shadow
    /// observers a new genesis-anchored chain value.
    pub fn publish_state(&mut self, w: &FlushWatermark) {
        if let Some(ref mut publisher) = self.fsync_state {
            publisher.store(crate::pipeline::FsyncState {
                journal_seq: w.journal_seq,
                chain_hash: w.chain_hash,
                input_ring_seq: w.input_ring_seq,
            });
        }
        if let Some(ref cursor) = self.durable_wire_seq {
            cursor.store(w.journal_seq);
        }
        if let Some(ref tip) = self.advertised_tip {
            // `advance`, not a plain store: across a promotion the
            // receiver left the tip at its in-memory accepted position,
            // which the new primary's journal only reaches after the
            // drained ring is flushed — a plain store would regress the
            // advertised tip in that window.
            tip.advance(w.journal_seq);
        }
    }

    /// Full post-flush publication: ring progress first, then writer
    /// state.
    ///
    /// The order matches the inline path it replaces. Ring progress is
    /// the persist-before-ack boundary on replicas, so a caller that
    /// reaches this must have completed the watermark's sync.
    pub fn publish(&mut self, w: &FlushWatermark) {
        self.progress
            .get()
            .store(w.ring_progress.get(), Ordering::Release);
        self.publish_state(w);
    }
}

/// Create a linked handle/executor pair.
///
/// The caller spawns a thread running [`run_flush_executor`] with the
/// returned reader and shared state, and keeps the handle on the journal
/// thread.
pub fn flush_channel() -> (FlushHandle, SeqLockReader<FlushWatermark>, Arc<FlushShared>) {
    let (writer, reader) = split(FlushWatermark::default());
    let shared = Arc::new(FlushShared::default());
    let handle = FlushHandle {
        cell: writer,
        shared: Arc::clone(&shared),
        last_submitted: 0,
    };
    (handle, reader, shared)
}

/// Executor loop: consume the latest watermark, sync, publish.
///
/// `sync_fd` performs the durability call (`fdatasync` in production).
/// `publish` performs everything that follows an inline flush today —
/// the durable wire-seq cursor, `FsyncState`, the advertised journal
/// tip, and `Consumer::set_progress`.
///
/// Idles through [`crate::pipeline::idle_wait`] like every other stage,
/// so `--yield-idle` applies here too and a submit never needs a wakeup
/// syscall on the journal thread.
///
/// Returns when `shutdown` is set. A poisoned executor keeps idling
/// rather than exiting so it stays joinable in every state without a
/// dedicated unpark or exit signal.
pub fn run_flush_executor<S, P>(
    cell: SeqLockReader<FlushWatermark>,
    shared: Arc<FlushShared>,
    mut sync_fd: S,
    mut publish: P,
    busy_spin: bool,
    shutdown: &AtomicBool,
) where
    S: FnMut(i64) -> std::io::Result<()>,
    P: FnMut(&FlushWatermark),
{
    let mut last_published: u64 = 0;
    let mut last_synced_seq: u64 = 0;
    let mut idle_spins: u32 = 0;

    loop {
        if shutdown.load(Ordering::Relaxed) {
            return;
        }
        if shared.poisoned.load(Ordering::Acquire) {
            // Stay alive and joinable, but never publish again: a frozen
            // cursor can only be over-conservative, and the journal
            // thread is already tearing the pipeline down.
            crate::pipeline::idle_wait(&mut idle_spins, busy_spin);
            continue;
        }

        // Sample before the sync. Everything below publishes `watermark`
        // — never a re-read — because `fdatasync` covers only what was
        // dirty when it was called.
        let watermark = cell.load();
        if watermark.submit_seq <= last_published {
            crate::pipeline::idle_wait(&mut idle_spins, busy_spin);
            continue;
        }
        idle_spins = 0;

        // Two cases skip the syscall and publish anyway, because in both
        // the watermark claims nothing that is not already durable:
        //
        // - a batch that journaled nothing (all queries) still advances
        //   ring progress, and every sequence it covers is at or below
        //   `last_synced_seq`;
        // - `NOTHING_TO_SYNC` — the writer made the bytes durable inline,
        //   so there is no descriptor to force.
        //
        // Skipping the publish instead would freeze ring progress and
        // eventually stall the producer against a ring that never drains.
        if watermark.fd != NOTHING_TO_SYNC && watermark.journal_seq.get() > last_synced_seq {
            if let Err(e) = sync_fd(watermark.fd) {
                // Store the cause before the flag so an `Acquire` reader
                // of `poisoned` is guaranteed to see it.
                shared
                    .poison_errno
                    .store(e.raw_os_error().unwrap_or(0) as u64, Ordering::Release);
                shared.poisoned.store(true, Ordering::Release);
                continue;
            }
            last_synced_seq = watermark.journal_seq.get();
        }

        publish(&watermark);
        last_published = watermark.submit_seq;
        // `published_journal_seq` first: a reader that sees the submit
        // counter advance must not then read a stale lag.
        shared
            .published_journal_seq
            .store(watermark.journal_seq.get(), Ordering::Release);
        shared
            .published_submit
            .store(last_published, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, channel};
    use std::time::{Duration, Instant};

    /// Wall-clock bound for "this wait must not hang". Generous — the
    /// tests assert termination, not latency, and a loaded CI box should
    /// not flake.
    const TERMINATION_TIMEOUT: Duration = Duration::from_secs(5);

    fn watermark(journal_seq: u64, ring_progress: u64) -> FlushWatermark {
        FlushWatermark {
            // Overwritten by `submit`; set to a poison value so a test
            // that reads it without submitting fails loudly.
            submit_seq: u64::MAX,
            journal_seq: WireSeq::new(journal_seq),
            ring_progress: RingPos::new(ring_progress),
            input_ring_seq: RingPos::new(ring_progress),
            chain_hash: [journal_seq as u8; 32],
            generation: 0,
            fd: 7,
        }
    }

    /// Records every publication the executor performs.
    #[derive(Default)]
    struct Published(Mutex<Vec<FlushWatermark>>);

    impl Published {
        fn record(&self, w: &FlushWatermark) {
            self.0.lock().expect("publish log poisoned").push(*w);
        }
        fn all(&self) -> Vec<FlushWatermark> {
            self.0.lock().expect("publish log poisoned").clone()
        }
        fn last(&self) -> Option<FlushWatermark> {
            self.0.lock().expect("publish log poisoned").last().copied()
        }
        fn count(&self) -> usize {
            self.0.lock().expect("publish log poisoned").len()
        }
    }

    /// A sync call the test drives: each invocation announces itself on
    /// `entered` and blocks until the test sends on `release`.
    ///
    /// Never panics and never blocks past teardown. A panic here would
    /// surface as a scope panic and mask the assertion that actually
    /// failed; a block past teardown would wedge the scope's join. Both
    /// turn a test failure into a hung suite.
    struct GatedSync<'a> {
        entered: Sender<i64>,
        release: Receiver<()>,
        shutdown: &'a AtomicBool,
    }

    impl GatedSync<'_> {
        fn call(&mut self, fd: i64) -> std::io::Result<()> {
            if self.entered.send(fd).is_err() {
                return Ok(()); // test moved on
            }
            loop {
                match self.release.recv_timeout(Duration::from_millis(20)) {
                    Ok(()) => return Ok(()),
                    Err(RecvTimeoutError::Disconnected) => return Ok(()),
                    Err(RecvTimeoutError::Timeout) => {
                        if self.shutdown.load(Ordering::Relaxed) {
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    /// Sets `shutdown` on drop, including while unwinding from a failed
    /// assertion.
    ///
    /// Without it, an assertion inside `std::thread::scope` hangs the
    /// suite rather than failing it: the panic skips the explicit
    /// `shutdown.store`, and the scope then waits forever to join an
    /// executor that was never told to stop. Every test below asserts
    /// inside a scope, so every test needs this.
    struct ShutdownGuard<'a>(&'a AtomicBool);

    impl Drop for ShutdownGuard<'_> {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Relaxed);
        }
    }

    /// Arms a watchdog that trips `shutdown` after [`TERMINATION_TIMEOUT`].
    ///
    /// `ShutdownGuard` covers a panicking assertion, but not a *blocked*
    /// one: `drain` called on the test thread legitimately waits until
    /// the executor catches up, poisons, or shuts down, so a bug that
    /// stops the executor publishing would wedge the test thread inside
    /// the scope where the guard can never run. Tripping `shutdown`
    /// turns that into a `ShuttingDown` outcome, which fails the
    /// assertion. Exits as soon as `shutdown` is set normally, so a
    /// passing test pays milliseconds, not the full timeout.
    fn arm_watchdog<'s>(s: &'s std::thread::Scope<'s, '_>, shutdown: &'s AtomicBool) {
        s.spawn(move || {
            let deadline = Instant::now() + TERMINATION_TIMEOUT;
            while !shutdown.load(Ordering::Relaxed) {
                if Instant::now() >= deadline {
                    shutdown.store(true, Ordering::Relaxed);
                    return;
                }
                std::thread::sleep(Duration::from_millis(5));
            }
        });
    }

    /// Spin until `cond` holds, panicking with `what` if it never does.
    /// Every wait in these tests is bounded so a broken termination
    /// property fails the test instead of hanging the suite.
    fn wait_until(what: &str, mut cond: impl FnMut() -> bool) {
        let deadline = Instant::now() + TERMINATION_TIMEOUT;
        while !cond() {
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            std::thread::yield_now();
        }
    }

    #[test]
    fn watermark_carries_every_field_the_executor_publishes() {
        // The executor holds no reference to the writer or consumer, so
        // anything missing from the cell is unrecoverable on its side.
        let (mut handle, cell, shared) = flush_channel();
        let published = Published::default();
        let shutdown = AtomicBool::new(false);

        let mut sent = watermark(42, 100);
        sent.chain_hash = [0xAB; 32];
        sent.input_ring_seq = RingPos::new(105);
        sent.generation = 3;
        sent.fd = 11;
        handle.submit(sent);

        std::thread::scope(|s| {
            // Fail, don't hang — for a panicking assertion and for a
            // blocked one respectively.
            let _guard = ShutdownGuard(&shutdown);
            arm_watchdog(s, &shutdown);
            s.spawn(|| {
                run_flush_executor(
                    cell,
                    shared,
                    |_| Ok(()),
                    |w| published.record(w),
                    true,
                    &shutdown,
                );
            });
            wait_until("the watermark to be published", || published.count() == 1);
            shutdown.store(true, Ordering::Relaxed);
        });

        let got = published.last().expect("one publication");
        assert_eq!(got.journal_seq, WireSeq::new(42));
        assert_eq!(got.ring_progress, RingPos::new(100));
        assert_eq!(
            got.input_ring_seq,
            RingPos::new(105),
            "distinct from ring_progress"
        );
        assert_eq!(got.chain_hash, [0xAB; 32]);
        assert_eq!(got.generation, 3);
        assert_eq!(got.fd, 11);
        assert_eq!(got.submit_seq, 1, "submit assigns the counter");
    }

    #[test]
    fn publication_uses_the_pre_sync_sample_not_a_re_read() {
        // The correctness rule of the whole design: `fdatasync` only
        // covers data dirtied before the call, so publishing a watermark
        // read *after* the syscall returns would claim durability for
        // bytes the sync never covered.
        let (mut handle, cell, shared) = flush_channel();
        let published = Published::default();
        let shutdown = AtomicBool::new(false);
        let (entered_tx, entered_rx) = channel();
        let (release_tx, release_rx) = channel();
        let mut gate = GatedSync {
            entered: entered_tx,
            release: release_rx,
            shutdown: &shutdown,
        };

        handle.submit(watermark(10, 100));

        std::thread::scope(|s| {
            // Fail, don't hang — for a panicking assertion and for a
            // blocked one respectively.
            let _guard = ShutdownGuard(&shutdown);
            arm_watchdog(s, &shutdown);
            s.spawn(|| {
                run_flush_executor(
                    cell,
                    shared,
                    |fd| gate.call(fd),
                    |w| published.record(w),
                    true,
                    &shutdown,
                );
            });

            // Executor is now inside the sync for seq 10.
            entered_rx
                .recv_timeout(TERMINATION_TIMEOUT)
                .expect("sync entered");
            // A later batch lands while that sync is still running. Its
            // bytes are NOT covered by the in-flight fdatasync.
            handle.submit(watermark(99, 900));
            release_tx.send(()).expect("executor alive");

            wait_until("the first publication", || published.count() >= 1);
            let first = published.all()[0];
            assert_eq!(
                first.journal_seq,
                WireSeq::new(10),
                "published a watermark the completed sync did not cover"
            );
            assert_eq!(first.ring_progress, RingPos::new(100));

            // The later batch is covered by the *next* sync.
            entered_rx
                .recv_timeout(TERMINATION_TIMEOUT)
                .expect("second sync entered");
            release_tx.send(()).expect("executor alive");
            wait_until("the second publication", || published.count() >= 2);
            assert_eq!(published.all()[1].journal_seq, WireSeq::new(99));

            shutdown.store(true, Ordering::Relaxed);
            // Unblock any sync the executor entered before observing the flag.
            let _ = release_tx.send(());
        });
    }

    #[test]
    fn intermediate_watermarks_coalesce_into_the_newest() {
        // `fdatasync` is cumulative, so a queue would make the executor
        // iterate work it would discard — and would need a full policy
        // that re-blocks the journal thread. The cell drops the
        // intermediates by construction.
        let (mut handle, cell, shared) = flush_channel();
        let published = Published::default();
        let shutdown = AtomicBool::new(false);
        let (entered_tx, entered_rx) = channel();
        let (release_tx, release_rx) = channel();
        let mut gate = GatedSync {
            entered: entered_tx,
            release: release_rx,
            shutdown: &shutdown,
        };

        handle.submit(watermark(1, 10));

        std::thread::scope(|s| {
            // Fail, don't hang — for a panicking assertion and for a
            // blocked one respectively.
            let _guard = ShutdownGuard(&shutdown);
            arm_watchdog(s, &shutdown);
            s.spawn(|| {
                run_flush_executor(
                    cell,
                    shared,
                    |fd| gate.call(fd),
                    |w| published.record(w),
                    true,
                    &shutdown,
                );
            });

            entered_rx
                .recv_timeout(TERMINATION_TIMEOUT)
                .expect("first sync entered");
            // 50 batches arrive during one stalled flush.
            for i in 2..=51u64 {
                handle.submit(watermark(i, i * 10));
            }
            release_tx.send(()).expect("executor alive");

            // One more sync covers all 50.
            entered_rx
                .recv_timeout(TERMINATION_TIMEOUT)
                .expect("second sync entered");
            release_tx.send(()).expect("executor alive");
            wait_until("the coalesced publication", || {
                published
                    .last()
                    .is_some_and(|w| w.journal_seq == WireSeq::new(51))
            });

            let count = published.count();
            assert!(
                count <= 3,
                "expected the 50 intermediates to coalesce, got {count} publications"
            );

            shutdown.store(true, Ordering::Relaxed);
            let _ = release_tx.send(());
        });
    }

    #[test]
    fn drain_returns_when_the_executor_catches_up() {
        let (mut handle, cell, shared) = flush_channel();
        let shutdown = AtomicBool::new(false);

        std::thread::scope(|s| {
            // Fail, don't hang — for a panicking assertion and for a
            // blocked one respectively.
            let _guard = ShutdownGuard(&shutdown);
            arm_watchdog(s, &shutdown);
            s.spawn(|| {
                run_flush_executor(cell, shared, |_| Ok(()), |_| {}, true, &shutdown);
            });

            for i in 1..=8u64 {
                handle.submit(watermark(i, i * 10));
            }
            assert_eq!(handle.drain(&shutdown, true), DrainOutcome::Drained);
            assert!(handle.is_idle(), "drained implies idle");

            shutdown.store(true, Ordering::Relaxed);
        });
    }

    #[test]
    fn drain_aborts_on_poison_instead_of_wedging() {
        // Without this, a failed sync wedges the journal thread at the
        // next rotation boundary instead of surfacing the stored error.
        let (mut handle, cell, shared) = flush_channel();
        let shutdown = AtomicBool::new(false);

        std::thread::scope(|s| {
            // Fail, don't hang — for a panicking assertion and for a
            // blocked one respectively.
            let _guard = ShutdownGuard(&shutdown);
            arm_watchdog(s, &shutdown);
            s.spawn(|| {
                run_flush_executor(
                    cell,
                    shared,
                    |_| Err(std::io::Error::from_raw_os_error(28)), // ENOSPC
                    |_| panic!("a failed sync must never publish"),
                    true,
                    &shutdown,
                );
            });

            handle.submit(watermark(1, 10));
            wait_until("poison to latch", || handle.shared().is_poisoned());

            // Bounded: a drain that ignores poison would otherwise spin
            // forever and hang the suite instead of failing it.
            let (tx, rx) = channel();
            let (h, sd) = (&handle, &shutdown);
            let waiter = s.spawn(move || {
                let _ = tx.send(h.drain(sd, true));
            });
            let outcome = rx.recv_timeout(TERMINATION_TIMEOUT);
            // Releases the waiter whether or not it honoured poison, so
            // the scope can close and the assertion below can report.
            shutdown.store(true, Ordering::Relaxed);
            waiter.join().expect("drain thread");

            assert_eq!(
                outcome
                    .expect("drain must abort on poison, not wait for a cursor that never moves"),
                DrainOutcome::Poisoned
            );
            assert_eq!(handle.shared().poison_errno(), Some(28));
        });
    }

    #[test]
    fn drain_abandons_on_shutdown() {
        // A sync that never returns (wedged device) must not stop
        // shutdown from making progress.
        let (mut handle, cell, shared) = flush_channel();
        let shutdown = AtomicBool::new(false);
        let (entered_tx, entered_rx) = channel();
        let (release_tx, release_rx) = channel();
        let mut gate = GatedSync {
            entered: entered_tx,
            release: release_rx,
            shutdown: &shutdown,
        };

        std::thread::scope(|s| {
            // Fail, don't hang — for a panicking assertion and for a
            // blocked one respectively.
            let _guard = ShutdownGuard(&shutdown);
            arm_watchdog(s, &shutdown);
            s.spawn(|| {
                run_flush_executor(cell, shared, |fd| gate.call(fd), |_| {}, true, &shutdown);
            });

            handle.submit(watermark(1, 10));
            entered_rx
                .recv_timeout(TERMINATION_TIMEOUT)
                .expect("sync entered");

            // The executor is parked inside the sync and will never
            // publish this watermark.
            let (tx, rx) = channel();
            let (h, sd) = (&handle, &shutdown);
            let waiter = s.spawn(move || {
                let _ = tx.send(h.drain(sd, true));
            });
            std::thread::yield_now();
            shutdown.store(true, Ordering::Relaxed);
            let outcome = rx.recv_timeout(TERMINATION_TIMEOUT);
            waiter.join().expect("drain thread");
            assert_eq!(
                outcome.expect("drain must abandon on shutdown, not wait on a wedged sync"),
                DrainOutcome::ShuttingDown
            );

            release_tx.send(()).expect("executor alive");
        });
    }

    #[test]
    fn a_poisoned_executor_publishes_nothing_further_and_still_joins() {
        // Cursors freeze on a failed sync — a frozen cursor can only be
        // over-conservative — and the thread stays joinable without a
        // dedicated unpark signal, which is what lets shutdown always
        // join it.
        let (mut handle, cell, shared) = flush_channel();
        let published = Published::default();
        let shutdown = AtomicBool::new(false);
        let fail = AtomicBool::new(false);

        std::thread::scope(|s| {
            // Fail, don't hang — for a panicking assertion and for a
            // blocked one respectively.
            let _guard = ShutdownGuard(&shutdown);
            arm_watchdog(s, &shutdown);
            let executor = s.spawn(|| {
                run_flush_executor(
                    cell,
                    shared,
                    |_| {
                        if fail.load(Ordering::Acquire) {
                            Err(std::io::Error::from_raw_os_error(5)) // EIO
                        } else {
                            Ok(())
                        }
                    },
                    |w| published.record(w),
                    true,
                    &shutdown,
                );
            });

            handle.submit(watermark(1, 10));
            assert_eq!(handle.drain(&shutdown, true), DrainOutcome::Drained);
            let healthy = published.count();
            assert_eq!(healthy, 1);

            fail.store(true, Ordering::Release);
            handle.submit(watermark(2, 20));
            wait_until("poison to latch", || handle.shared().is_poisoned());

            // Submits after the failure are never published.
            handle.submit(watermark(3, 30));
            std::thread::yield_now();
            assert_eq!(published.count(), healthy, "published after a failed sync");
            assert_eq!(
                handle.shared().published_journal_seq(),
                WireSeq::new(1),
                "the durable cursor must not advance past the last good sync"
            );

            shutdown.store(true, Ordering::Relaxed);
            executor
                .join()
                .expect("a poisoned executor must still join");
        });
    }

    #[test]
    fn a_progress_only_watermark_publishes_without_a_redundant_sync() {
        // A batch of queries journals nothing but still advances ring
        // progress. Gating the executor on `journal_seq` would leave
        // that progress unpublished and eventually stall the producer
        // against a ring that never drains.
        let (mut handle, cell, shared) = flush_channel();
        let published = Published::default();
        let shutdown = AtomicBool::new(false);
        let syncs = AtomicU64::new(0);

        std::thread::scope(|s| {
            // Fail, don't hang — for a panicking assertion and for a
            // blocked one respectively.
            let _guard = ShutdownGuard(&shutdown);
            arm_watchdog(s, &shutdown);
            s.spawn(|| {
                run_flush_executor(
                    cell,
                    shared,
                    |_| {
                        syncs.fetch_add(1, Ordering::Release);
                        Ok(())
                    },
                    |w| published.record(w),
                    true,
                    &shutdown,
                );
            });

            handle.submit(watermark(5, 100));
            assert_eq!(handle.drain(&shutdown, true), DrainOutcome::Drained);
            assert_eq!(syncs.load(Ordering::Acquire), 1);

            // Same journal_seq, further ring progress.
            handle.submit(watermark(5, 140));
            assert_eq!(handle.drain(&shutdown, true), DrainOutcome::Drained);

            assert_eq!(
                syncs.load(Ordering::Acquire),
                1,
                "no new bytes to make durable — the previous sync already covers them"
            );
            let last = published.last().expect("progress-only publication");
            assert_eq!(
                last.ring_progress,
                RingPos::new(140),
                "progress must still publish"
            );

            shutdown.store(true, Ordering::Relaxed);
        });
    }

    #[test]
    fn a_nothing_to_sync_watermark_publishes_without_calling_sync() {
        // `JournalWrite::write_batch` reports `None` when nothing is
        // owed — an empty batch, or a writer that syncs inline. Cursors
        // must still advance, or ring progress freezes and the producer
        // eventually stalls against a ring that never drains.
        let (mut handle, cell, shared) = flush_channel();
        let published = Published::default();
        let shutdown = AtomicBool::new(false);
        let syncs = AtomicU64::new(0);

        std::thread::scope(|s| {
            // Fail, don't hang — for a panicking assertion and for a
            // blocked one respectively.
            let _guard = ShutdownGuard(&shutdown);
            arm_watchdog(s, &shutdown);
            s.spawn(|| {
                run_flush_executor(
                    cell,
                    shared,
                    |_| {
                        syncs.fetch_add(1, Ordering::Release);
                        Ok(())
                    },
                    |w| published.record(w),
                    true,
                    &shutdown,
                );
            });

            let mut w = watermark(9, 90);
            w.fd = NOTHING_TO_SYNC;
            handle.submit(w);
            assert_eq!(handle.drain(&shutdown, true), DrainOutcome::Drained);

            assert_eq!(
                syncs.load(Ordering::Acquire),
                0,
                "nothing was owed — the executor must not force a descriptor"
            );
            let got = published.last().expect("cursors must still advance");
            assert_eq!(got.ring_progress, RingPos::new(90));
            assert_eq!(got.journal_seq, WireSeq::new(9));

            shutdown.store(true, Ordering::Relaxed);
        });
    }

    #[test]
    fn publisher_advances_ring_progress_only_on_a_full_publish() {
        // `publish_state` is for the rotation paths, where the writer's
        // durable state changed but no new input was consumed. Advancing
        // ring progress there would publish a position no flush covers —
        // ack-before-persist on a replica.
        use crate::cursors::{AdvertisedJournalTip, DurableWireSeqCursor};
        use std::sync::atomic::AtomicU64 as StdAtomicU64;

        let progress = Arc::new(melin_pipeline::padding::Sequence::new(StdAtomicU64::new(0)));
        let durable = DurableWireSeqCursor::detached(WireSeq::new(0));
        let tip = AdvertisedJournalTip::new(WireSeq::new(0));

        let mut publisher = CursorPublisher::new(Arc::clone(&progress));
        publisher.set_durable_wire_seq(durable.clone());
        publisher.set_advertised_tip(tip.clone());

        let w = FlushWatermark {
            submit_seq: 1,
            journal_seq: WireSeq::new(77),
            ring_progress: RingPos::new(500),
            input_ring_seq: RingPos::new(505),
            chain_hash: [1u8; 32],
            generation: 0,
            fd: NOTHING_TO_SYNC,
        };

        publisher.publish_state(&w);
        assert_eq!(
            progress.get().load(Ordering::Acquire),
            0,
            "publish_state must not move the persist-before-ack boundary"
        );
        assert_eq!(durable.load(), WireSeq::new(77));
        assert_eq!(tip.load(), WireSeq::new(77));

        publisher.publish(&w);
        assert_eq!(progress.get().load(Ordering::Acquire), 500);
    }

    #[test]
    fn is_idle_and_lag_track_the_executor() {
        let (mut handle, cell, shared) = flush_channel();
        let shutdown = AtomicBool::new(false);
        let (entered_tx, entered_rx) = channel();
        let (release_tx, release_rx) = channel();
        let mut gate = GatedSync {
            entered: entered_tx,
            release: release_rx,
            shutdown: &shutdown,
        };

        assert!(handle.is_idle(), "nothing submitted yet");
        assert_eq!(handle.lag(WireSeq::new(0)), 0);

        std::thread::scope(|s| {
            // Fail, don't hang — for a panicking assertion and for a
            // blocked one respectively.
            let _guard = ShutdownGuard(&shutdown);
            arm_watchdog(s, &shutdown);
            s.spawn(|| {
                run_flush_executor(cell, shared, |fd| gate.call(fd), |_| {}, true, &shutdown);
            });

            handle.submit(watermark(50, 500));
            assert!(!handle.is_idle(), "a submit is outstanding");
            entered_rx
                .recv_timeout(TERMINATION_TIMEOUT)
                .expect("sync entered");
            assert_eq!(handle.lag(WireSeq::new(50)), 50, "nothing published yet");

            release_tx.send(()).expect("executor alive");
            assert_eq!(handle.drain(&shutdown, true), DrainOutcome::Drained);
            assert!(handle.is_idle());
            assert_eq!(handle.lag(WireSeq::new(50)), 0);

            shutdown.store(true, Ordering::Relaxed);
            let _ = release_tx.send(());
        });
    }
}
