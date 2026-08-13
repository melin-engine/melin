//! Journal writer thread — sole owner of the live segment's file.
//!
//! The async flush (see `journal-async-flush-2026-08.md`) moved
//! `fdatasync` off the journal thread but left `pwrite` on it, so two
//! threads issued I/O against one inode. That produced ~8 ms stalls
//! *inside* small writes, reproduced only under concurrency. This module
//! is the fix: one thread does the writing and the syncing, and the
//! journal thread hands it work through a queue.
//!
//! See `docs/internal/journal-writer-thread-2026-08.md` for the full
//! argument. The properties this module exists to hold:
//!
//! 1. **One thread touches the file.** Nothing here is callable from the
//!    encode path; the writer's loop is the only I/O against the
//!    segment.
//! 2. **Publish after the sync, never before.** Cursors carry a
//!    persist-before-ack meaning, so a batch's watermark is published
//!    only once an `fdatasync` issued *after* its bytes were written has
//!    returned.
//! 3. **Queue depth keeps the encoder moving.** A stalled sync now
//!    blocks writes too, so the encoder can only run ahead as far as it
//!    has buffers. Depth is what keeps it encoding — and therefore
//!    keeps the replication feed alive — through a stall. A double
//!    buffer would freeze it after one batch.
//!
//! Coalescing falls out of depth: the writer drains everything queued,
//! writes each batch, and covers them all with a single `fdatasync`,
//! which is cumulative.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};

use crate::cursors::WireSeq;
use crate::journal_flush::FlushWatermark;

/// Batches the encoder may run ahead by while the writer is busy.
///
/// Sized to absorb one sync's duration, not an arbitrary stall: ~4 ms
/// typical and ~8 ms observed outliers against the measured write rate
/// is a few hundred KB, so a handful of `BATCH_BUF_CAPACITY` buffers
/// covers it with room to spare. Past that the disk is genuinely broken
/// and backpressure is the correct answer rather than more buffering.
///
/// Deliberately a constant, not a knob: the previous branch shipped a
/// tuning parameter (`--group-commit-us`) that a later change silently
/// turned into a no-op, and this value is load-bearing for the masking
/// property rather than a performance dial.
pub const WRITE_QUEUE_DEPTH: usize = 8;

/// One encoded batch handed from the encoder to the writer.
///
/// Carries the bytes *and* everything the writer must publish once they
/// are durable — the writer holds no reference to the journal writer or
/// the input-ring consumer, so anything it publishes has to ride along.
#[derive(Debug)]
pub struct WriteBatch {
    /// Encoded journal bytes, written with one positioned write at
    /// `offset`.
    pub bytes: Vec<u8>,
    /// Byte offset in the live segment. The encoder tracks it because it
    /// knows the batch sizes; the writer trusts it, so the two must not
    /// disagree (asserted in debug builds by the writer).
    pub offset: u64,
    /// What to publish once these bytes are durable.
    ///
    /// `FlushWatermark::fd` is unused here — the writer owns the file,
    /// so there is no descriptor to hand over. It disappears with the
    /// flush-executor module.
    pub watermark: FlushWatermark,
}

/// Producer half, held by the journal thread.
///
/// Not `Clone`: single-producer is what makes the ordering the writer
/// relies on — rotation ordered against the entry stream — a property of
/// the type rather than a convention.
pub struct WriteQueue {
    work: SyncSender<WriteBatch>,
    /// Emptied buffers coming back from the writer, so steady state
    /// allocates nothing.
    recycled: Receiver<Vec<u8>>,
    shared: Arc<WriterShared>,
    submitted: u64,
}

/// Consumer half, moved onto the writer thread.
pub struct WriteQueueConsumer {
    work: Receiver<WriteBatch>,
    recycled: SyncSender<Vec<u8>>,
    shared: Arc<WriterShared>,
}

/// Cross-thread state the journal thread reads to pace itself and to
/// notice failure.
#[derive(Debug, Default)]
pub struct WriterShared {
    /// Highest `submit_seq` the writer has made durable and published.
    published: AtomicU64,
    /// Highest `journal_seq` published, for the flush-lag gauge.
    published_journal_seq: AtomicU64,
    /// Byte offset past the last durable write — the writer's
    /// `valid_end`, republished because the rotation size trigger lives
    /// on the journal thread. Monotonic within a segment, so a slightly
    /// stale read costs at most one batch of lateness.
    valid_end: AtomicU64,
    /// Latched when a write or sync fails.
    poisoned: AtomicBool,
    /// `errno` behind `poisoned`. Stored before the flag, so an
    /// `Acquire` reader of the flag sees it.
    poison_errno: AtomicU64,
}

impl WriterShared {
    #[inline]
    pub fn published_journal_seq(&self) -> WireSeq {
        WireSeq::new(self.published_journal_seq.load(Ordering::Acquire))
    }

    /// Byte offset past the last durable write.
    #[inline]
    pub fn valid_end(&self) -> u64 {
        self.valid_end.load(Ordering::Acquire)
    }

    #[inline]
    pub fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Acquire)
    }

    /// `errno` behind [`is_poisoned`](Self::is_poisoned), or `0` if the
    /// failure carried none. Callers decide liveness from the flag,
    /// never from this value — an error type that erases
    /// `raw_os_error()` would otherwise silently disable the fatal path,
    /// which is a defect the previous branch actually shipped.
    #[inline]
    pub fn poison_errno(&self) -> i32 {
        self.poison_errno.load(Ordering::Acquire) as i32
    }
}

/// Outcome of handing a batch to the writer.
///
/// The rejecting variants carry the batch back rather than dropping it:
/// a batch the encoder has already sequenced and hashed is not
/// discardable, so the caller must retry it or fail.
#[derive(Debug)]
pub enum Submitted {
    /// Queued. The encoder may continue.
    Ok,
    /// The queue is full: the writer is behind. The batch comes back so
    /// the caller keeps its buffer, and the caller must retry rather
    /// than drop it.
    Full(Box<WriteBatch>),
    /// The writer has failed and will never drain again.
    Poisoned(Box<WriteBatch>),
}

impl WriteQueue {
    /// Hand an encoded batch to the writer. Never blocks.
    ///
    /// `submit_seq` is assigned here so callers cannot get it wrong, and
    /// so the writer's ordering and the flush-lag gauge share one
    /// counter.
    pub fn submit(&mut self, mut batch: WriteBatch) -> Submitted {
        if self.shared.is_poisoned() {
            return Submitted::Poisoned(Box::new(batch));
        }
        self.submitted += 1;
        batch.watermark.submit_seq = self.submitted;
        batch.watermark.submit_ts = crate::trace::mono_trace_ns();
        match self.work.try_send(batch) {
            Ok(()) => Submitted::Ok,
            Err(TrySendError::Full(batch)) => {
                // Not queued, so it never happened: roll the counter back
                // or the next submit would leave a gap and the writer
                // would publish a `submit_seq` the encoder never sent.
                self.submitted -= 1;
                Submitted::Full(Box::new(batch))
            }
            Err(TrySendError::Disconnected(batch)) => {
                self.submitted -= 1;
                Submitted::Poisoned(Box::new(batch))
            }
        }
    }

    /// Take a buffer for the next batch: a recycled one if the writer
    /// has returned any, else a fresh allocation.
    ///
    /// Steady state allocates nothing — the writer returns every buffer
    /// it drains.
    pub fn take_buffer(&mut self, capacity: usize) -> Vec<u8> {
        match self.recycled.try_recv() {
            Ok(mut buf) => {
                buf.clear();
                buf
            }
            Err(_) => Vec::with_capacity(capacity),
        }
    }

    /// Whether the writer has caught up with every submit — the disk is
    /// idle, so a submit now starts work immediately.
    #[inline]
    pub fn is_idle(&self) -> bool {
        self.shared.published.load(Ordering::Acquire) == self.submitted
    }

    /// Submits not yet durable, in wire-seq space: the flush-lag gauge.
    #[inline]
    pub fn lag(&self, submitted_journal_seq: WireSeq) -> u64 {
        submitted_journal_seq
            .get()
            .saturating_sub(self.shared.published_journal_seq.load(Ordering::Acquire))
    }

    #[inline]
    pub fn shared(&self) -> &Arc<WriterShared> {
        &self.shared
    }
}

/// Create a linked queue pair plus the shared state.
pub fn write_queue(depth: usize) -> (WriteQueue, WriteQueueConsumer, Arc<WriterShared>) {
    let (work_tx, work_rx) = sync_channel(depth);
    // Same depth: the writer can never hold more buffers than the queue
    // admits, so returning one can never block it.
    let (recycled_tx, recycled_rx) = sync_channel(depth);
    let shared = Arc::new(WriterShared::default());
    (
        WriteQueue {
            work: work_tx,
            recycled: recycled_rx,
            shared: Arc::clone(&shared),
            submitted: 0,
        },
        WriteQueueConsumer {
            work: work_rx,
            recycled: recycled_tx,
            shared: Arc::clone(&shared),
        },
        shared,
    )
}

/// What the writer does with a batch's bytes. Injected so the tests can
/// stall and fail I/O deterministically, and so the writer loop is
/// independent of which journal writer is underneath.
pub trait SegmentIo {
    /// Write `bytes` at `offset`. No durability implied.
    fn write_at(&mut self, bytes: &[u8], offset: u64) -> std::io::Result<()>;
    /// Force everything written so far to stable media.
    fn sync(&mut self) -> std::io::Result<()>;
}

impl SegmentIo for melin_journal::SegmentFile {
    fn write_at(&mut self, bytes: &[u8], offset: u64) -> std::io::Result<()> {
        // The encoder tracks the offset because the file lives here, not
        // with it. Disagreement means the two have drifted — a torn or
        // overlapping journal — so check it where both values are in
        // hand rather than discovering it in a reader later.
        debug_assert_eq!(
            offset,
            self.valid_end(),
            "batch offset disagrees with the segment's write position: \
             the encoder and the writer have drifted apart"
        );
        self.append(bytes).map_err(io_error)
    }

    fn sync(&mut self) -> std::io::Result<()> {
        melin_journal::SegmentFile::sync(self).map_err(io_error)
    }
}

/// Unwrap a `JournalError` back to the OS error underneath.
///
/// The executor records failures as an `errno` and the journal stage
/// turns that into its fatal shutdown, so re-wrapping would erase
/// `raw_os_error()` and silently disable the whole path — a defect the
/// flush-executor branch actually shipped.
fn io_error(e: melin_journal::JournalError) -> std::io::Error {
    match e {
        melin_journal::JournalError::Io(io) => io,
        other => std::io::Error::other(other),
    }
}

/// Writer loop: drain the queue, write every batch, sync once, publish.
///
/// The single `sync` per drain is the point of queue depth: `fdatasync`
/// is cumulative, so covering N batches costs one call rather than N.
///
/// Publication happens strictly after that sync returns, and republishes
/// the *last drained* watermark — every earlier one it covers is
/// implied, since cursors are monotonic positions rather than events.
///
/// Returns when `shutdown` is set. A poisoned writer keeps idling rather
/// than exiting, so it stays joinable in every state without a dedicated
/// wake-up.
pub fn run_writer<Io, P>(
    queue: WriteQueueConsumer,
    mut io: Io,
    mut publish: P,
    busy_spin: bool,
    shutdown: &AtomicBool,
) where
    Io: SegmentIo,
    P: FnMut(&FlushWatermark),
{
    let mut idle_spins: u32 = 0;
    #[cfg(feature = "latency-trace")]
    let mut handoff_rec =
        crate::trace::register_stage("journal-write: submit → publish (write + sync + publish)");

    loop {
        if shutdown.load(Ordering::Relaxed) {
            #[cfg(feature = "latency-trace")]
            handoff_rec.flush();
            return;
        }
        if queue.shared.is_poisoned() {
            // Never publish again: a frozen cursor can only be
            // over-conservative, and the journal thread is already
            // tearing the pipeline down.
            crate::pipeline::idle_wait(&mut idle_spins, busy_spin);
            continue;
        }

        let Ok(first) = queue.work.try_recv() else {
            crate::pipeline::idle_wait(&mut idle_spins, busy_spin);
            continue;
        };
        idle_spins = 0;

        // Drain everything queued, not just the one batch: the sync
        // below covers all of them either way, so writing them together
        // converts queue depth into fewer, larger syncs.
        let mut last = None;
        let mut batch = Some(first);
        while let Some(b) = batch.take() {
            if let Err(e) = io.write_at(&b.bytes, b.offset) {
                poison(&queue.shared, &e);
                break;
            }
            let end = b.offset + b.bytes.len() as u64;
            last = Some((b.watermark, end));
            // Hand the buffer back for reuse. A full recycle channel
            // cannot happen (same depth as the work queue), but dropping
            // the buffer would only cost an allocation, never
            // correctness.
            let _ = queue.recycled.try_send(b.bytes);
            batch = queue.work.try_recv().ok();
        }

        let Some((watermark, end)) = last else {
            continue; // write failed; poisoned above
        };
        if queue.shared.is_poisoned() {
            continue;
        }

        if let Err(e) = io.sync() {
            poison(&queue.shared, &e);
            continue;
        }

        publish(&watermark);
        #[cfg(feature = "latency-trace")]
        handoff_rec.record_elapsed(watermark.submit_ts, crate::trace::mono_trace_ns());
        // `valid_end` and the journal seq before the submit counter: a
        // reader that sees the counter advance must not then read stale
        // companions.
        queue.shared.valid_end.store(end, Ordering::Release);
        queue
            .shared
            .published_journal_seq
            .store(watermark.journal_seq.get(), Ordering::Release);
        queue
            .shared
            .published
            .store(watermark.submit_seq, Ordering::Release);
    }
}

fn poison(shared: &WriterShared, e: &std::io::Error) {
    shared
        .poison_errno
        .store(e.raw_os_error().unwrap_or(0) as u64, Ordering::Release);
    shared.poisoned.store(true, Ordering::Release);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cursors::RingPos;
    use std::sync::Mutex;
    use std::sync::mpsc::{Receiver as StdReceiver, Sender, channel};
    use std::time::{Duration, Instant};

    const TERMINATION_TIMEOUT: Duration = Duration::from_secs(5);

    fn watermark(journal_seq: u64, ring_progress: u64) -> FlushWatermark {
        FlushWatermark {
            submit_seq: u64::MAX, // overwritten by `submit`
            journal_seq: WireSeq::new(journal_seq),
            ring_progress: RingPos::new(ring_progress),
            input_ring_seq: RingPos::new(ring_progress),
            chain_hash: [journal_seq as u8; 32],
            fd: crate::journal_flush::NOTHING_TO_SYNC,
            submit_ts: crate::trace::mono_trace_ns(),
        }
    }

    fn batch(journal_seq: u64, ring_progress: u64, offset: u64, len: usize) -> WriteBatch {
        WriteBatch {
            bytes: vec![journal_seq as u8; len],
            offset,
            watermark: watermark(journal_seq, ring_progress),
        }
    }

    /// Records what the writer did, in order.
    #[derive(Default)]
    struct Recorder {
        writes: Mutex<Vec<(u64, usize)>>,
        syncs: AtomicU64,
        /// Each publication with the sync count observed at that moment.
        /// A publication that happened before its sync shows zero — the
        /// persist-before-ack violation, made visible.
        published: Mutex<Vec<(FlushWatermark, u64)>>,
    }

    impl Recorder {
        fn write_count(&self) -> usize {
            self.writes.lock().expect("writes").len()
        }
        fn syncs(&self) -> u64 {
            self.syncs.load(Ordering::Acquire)
        }
        fn published(&self) -> Vec<(FlushWatermark, u64)> {
            self.published.lock().expect("published").clone()
        }
        /// Record a publication together with how many syncs had
        /// completed when it happened.
        fn record_publish(&self, w: &FlushWatermark) {
            let syncs = self.syncs();
            self.published.lock().expect("published").push((*w, syncs));
        }
    }

    /// `SegmentIo` that records, and optionally gates or fails.
    struct TestIo<'a> {
        rec: &'a Recorder,
        /// Announces each sync and waits for release, when present.
        gate: Option<(Sender<()>, StdReceiver<()>)>,
        fail_write: Option<i32>,
        fail_sync: Option<i32>,
        shutdown: &'a AtomicBool,
    }

    impl SegmentIo for TestIo<'_> {
        fn write_at(&mut self, bytes: &[u8], offset: u64) -> std::io::Result<()> {
            if let Some(errno) = self.fail_write {
                return Err(std::io::Error::from_raw_os_error(errno));
            }
            self.rec
                .writes
                .lock()
                .expect("writes")
                .push((offset, bytes.len()));
            Ok(())
        }

        fn sync(&mut self) -> std::io::Result<()> {
            if let Some(errno) = self.fail_sync {
                return Err(std::io::Error::from_raw_os_error(errno));
            }
            self.rec.syncs.fetch_add(1, Ordering::Release);
            if let Some((entered, release)) = &self.gate {
                // Never panic and never block past teardown: either turns
                // a test failure into a hung suite.
                if entered.send(()).is_ok() {
                    loop {
                        match release.recv_timeout(Duration::from_millis(20)) {
                            Ok(()) => break,
                            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                                if self.shutdown.load(Ordering::Relaxed) {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            Ok(())
        }
    }

    /// Sets `shutdown` on drop, including while unwinding from a failed
    /// assertion — without it an assertion inside `thread::scope` hangs
    /// the suite instead of failing it.
    struct ShutdownGuard<'a>(&'a AtomicBool);
    impl Drop for ShutdownGuard<'_> {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Relaxed);
        }
    }

    /// `Submitted` carries the rejected batch, so it has no `PartialEq`
    /// — the rejecting variants are the interesting ones and comparing
    /// them by value would mean comparing buffers.
    #[track_caller]
    fn expect_queued(outcome: Submitted) {
        assert!(
            matches!(outcome, Submitted::Ok),
            "expected the batch to queue, got {outcome:?}"
        );
    }

    fn wait_until(what: &str, mut cond: impl FnMut() -> bool) {
        let deadline = Instant::now() + TERMINATION_TIMEOUT;
        while !cond() {
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            std::thread::yield_now();
        }
    }

    #[test]
    fn batches_are_written_in_order_and_published_after_the_sync() {
        let rec = Recorder::default();
        let shutdown = AtomicBool::new(false);
        let (mut q, consumer, _shared) = write_queue(WRITE_QUEUE_DEPTH);

        std::thread::scope(|s| {
            let _guard = ShutdownGuard(&shutdown);
            let io = TestIo {
                rec: &rec,
                gate: None,
                fail_write: None,
                fail_sync: None,
                shutdown: &shutdown,
            };
            s.spawn(|| run_writer(consumer, io, |w| rec.record_publish(w), true, &shutdown));

            for i in 1..=4u64 {
                expect_queued(q.submit(batch(i, i * 10, i * 100, 16)));
            }
            wait_until("all batches published", || q.is_idle());

            let writes = rec.writes.lock().expect("writes").clone();
            assert_eq!(
                writes.iter().map(|(o, _)| *o).collect::<Vec<_>>(),
                vec![100, 200, 300, 400],
                "writes must reach the segment in submit order"
            );
            let published = rec.published();
            assert!(!published.is_empty());
            assert_eq!(
                published.last().expect("last").0.journal_seq,
                WireSeq::new(4)
            );
            // Persist-before-ack: a cursor carries a durability claim, so
            // a publication must never precede the sync backing it.
            for (w, syncs_at_publish) in &published {
                assert!(
                    *syncs_at_publish >= 1,
                    "published journal_seq {} with {syncs_at_publish} syncs completed — \
                     the cursor claimed durability the disk had not confirmed",
                    w.journal_seq.get()
                );
            }
            shutdown.store(true, Ordering::Relaxed);
        });
    }

    #[test]
    fn a_queued_run_is_covered_by_one_sync() {
        // Queue depth exists to keep the encoder moving, but it also
        // converts into fewer, larger syncs: `fdatasync` is cumulative,
        // so a drained run needs exactly one.
        let rec = Recorder::default();
        let shutdown = AtomicBool::new(false);
        let (mut q, consumer, _shared) = write_queue(WRITE_QUEUE_DEPTH);
        let (entered_tx, entered_rx) = channel();
        let (release_tx, release_rx) = channel();

        std::thread::scope(|s| {
            let _guard = ShutdownGuard(&shutdown);
            let io = TestIo {
                rec: &rec,
                gate: Some((entered_tx, release_rx)),
                fail_write: None,
                fail_sync: None,
                shutdown: &shutdown,
            };
            s.spawn(|| run_writer(consumer, io, |w| rec.record_publish(w), true, &shutdown));

            // First batch parks the writer inside its sync.
            expect_queued(q.submit(batch(1, 10, 0, 8)));
            entered_rx
                .recv_timeout(TERMINATION_TIMEOUT)
                .expect("first sync entered");

            // Everything that arrives while it is stuck queues up.
            for i in 2..=6u64 {
                expect_queued(q.submit(batch(i, i * 10, i * 100, 8)));
            }
            release_tx.send(()).expect("writer alive");

            // One more sync covers all five.
            entered_rx
                .recv_timeout(TERMINATION_TIMEOUT)
                .expect("second sync entered");
            release_tx.send(()).expect("writer alive");
            wait_until("the queued run to drain", || q.is_idle());

            assert_eq!(rec.write_count(), 6, "every batch must be written");
            assert_eq!(
                rec.syncs(),
                2,
                "five queued batches must share one sync, not take one each"
            );

            shutdown.store(true, Ordering::Relaxed);
            let _ = release_tx.send(());
        });
    }

    #[test]
    fn a_full_queue_hands_the_batch_back_instead_of_dropping_it() {
        // Backpressure, not loss: the caller keeps its buffer and must
        // retry. Dropping would tear a hole in the journal.
        let rec = Recorder::default();
        let shutdown = AtomicBool::new(false);
        let (mut q, consumer, _shared) = write_queue(2);
        let (entered_tx, entered_rx) = channel();
        let (release_tx, release_rx) = channel();

        std::thread::scope(|s| {
            let _guard = ShutdownGuard(&shutdown);
            let io = TestIo {
                rec: &rec,
                gate: Some((entered_tx, release_rx)),
                fail_write: None,
                fail_sync: None,
                shutdown: &shutdown,
            };
            s.spawn(|| run_writer(consumer, io, |w| rec.record_publish(w), true, &shutdown));

            expect_queued(q.submit(batch(1, 10, 0, 8)));
            entered_rx
                .recv_timeout(TERMINATION_TIMEOUT)
                .expect("sync entered");

            // Fill the queue while the writer is parked, then overflow.
            let mut rejected = None;
            for i in 2..=8u64 {
                if let Submitted::Full(b) = q.submit(batch(i, i * 10, i * 100, 8)) {
                    rejected = Some(b);
                    break;
                }
            }
            let rejected = rejected.expect("a depth-2 queue must fill");
            assert_eq!(
                rejected.bytes.len(),
                8,
                "the batch comes back intact so the caller keeps its buffer"
            );

            // The rejected submit must not consume a sequence number, or
            // the writer would wait on a `submit_seq` that never arrives.
            let before = q.submitted;
            release_tx.send(()).expect("writer alive");
            // The batches that queued behind the parked sync form their
            // own drained run, with its own sync to release.
            entered_rx
                .recv_timeout(TERMINATION_TIMEOUT)
                .expect("second sync entered");
            release_tx.send(()).expect("writer alive");
            wait_until("the queue to drain", || q.is_idle());
            assert_eq!(q.submitted, before, "a refused submit must not count");

            shutdown.store(true, Ordering::Relaxed);
            let _ = release_tx.send(());
        });
    }

    #[test]
    fn buffers_are_recycled_rather_than_reallocated() {
        let rec = Recorder::default();
        let shutdown = AtomicBool::new(false);
        let (mut q, consumer, _shared) = write_queue(WRITE_QUEUE_DEPTH);

        std::thread::scope(|s| {
            let _guard = ShutdownGuard(&shutdown);
            let io = TestIo {
                rec: &rec,
                gate: None,
                fail_write: None,
                fail_sync: None,
                shutdown: &shutdown,
            };
            s.spawn(|| run_writer(consumer, io, |_| {}, true, &shutdown));

            let mut b = batch(1, 10, 0, 8);
            b.bytes.reserve(4096);
            let capacity = b.bytes.capacity();
            expect_queued(q.submit(b));
            wait_until("the batch to drain", || q.is_idle());

            let reused = q.take_buffer(64);
            assert_eq!(
                reused.capacity(),
                capacity,
                "the writer's returned buffer must come back, not a fresh allocation"
            );
            assert!(reused.is_empty(), "recycled buffers arrive cleared");

            shutdown.store(true, Ordering::Relaxed);
        });
    }

    #[test]
    fn a_failed_sync_poisons_and_publishes_nothing_further() {
        let rec = Recorder::default();
        let shutdown = AtomicBool::new(false);
        let (mut q, consumer, shared) = write_queue(WRITE_QUEUE_DEPTH);

        std::thread::scope(|s| {
            let _guard = ShutdownGuard(&shutdown);
            let io = TestIo {
                rec: &rec,
                gate: None,
                fail_write: None,
                fail_sync: Some(5), // EIO
                shutdown: &shutdown,
            };
            s.spawn(|| {
                run_writer(
                    consumer,
                    io,
                    |_| panic!("a failed sync must never publish"),
                    true,
                    &shutdown,
                )
            });

            expect_queued(q.submit(batch(1, 10, 0, 8)));
            wait_until("poison to latch", || shared.is_poisoned());
            assert_eq!(shared.poison_errno(), 5);
            assert_eq!(
                shared.published_journal_seq(),
                WireSeq::new(0),
                "the durable cursor must not advance past the last good sync"
            );

            // Later submits are refused rather than silently queued
            // behind a writer that will never drain them.
            assert!(matches!(
                q.submit(batch(2, 20, 100, 8)),
                Submitted::Poisoned(_)
            ));

            shutdown.store(true, Ordering::Relaxed);
        });
    }

    #[test]
    fn a_failed_write_poisons_before_any_sync() {
        // The write is what carries the data; if it fails there is
        // nothing to make durable and nothing to publish.
        let rec = Recorder::default();
        let shutdown = AtomicBool::new(false);
        let (mut q, consumer, shared) = write_queue(WRITE_QUEUE_DEPTH);

        std::thread::scope(|s| {
            let _guard = ShutdownGuard(&shutdown);
            let io = TestIo {
                rec: &rec,
                gate: None,
                fail_write: Some(28), // ENOSPC
                fail_sync: None,
                shutdown: &shutdown,
            };
            s.spawn(|| {
                run_writer(
                    consumer,
                    io,
                    |_| panic!("a failed write must never publish"),
                    true,
                    &shutdown,
                )
            });

            expect_queued(q.submit(batch(1, 10, 0, 8)));
            wait_until("poison to latch", || shared.is_poisoned());
            assert_eq!(shared.poison_errno(), 28);
            assert_eq!(
                rec.syncs(),
                0,
                "a failed write must not be followed by a sync"
            );

            shutdown.store(true, Ordering::Relaxed);
        });
    }

    #[test]
    fn valid_end_tracks_the_last_durable_write() {
        // The rotation size trigger reads this from the journal thread,
        // so it must reflect written-and-synced bytes, never merely
        // queued ones.
        let rec = Recorder::default();
        let shutdown = AtomicBool::new(false);
        let (mut q, consumer, shared) = write_queue(WRITE_QUEUE_DEPTH);

        std::thread::scope(|s| {
            let _guard = ShutdownGuard(&shutdown);
            let io = TestIo {
                rec: &rec,
                gate: None,
                fail_write: None,
                fail_sync: None,
                shutdown: &shutdown,
            };
            s.spawn(|| run_writer(consumer, io, |_| {}, true, &shutdown));

            assert_eq!(shared.valid_end(), 0);
            expect_queued(q.submit(batch(1, 10, 4096, 128)));
            wait_until("valid_end to advance", || shared.valid_end() == 4096 + 128);

            shutdown.store(true, Ordering::Relaxed);
        });
    }
}
