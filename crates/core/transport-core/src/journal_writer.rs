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
    pub watermark: FlushWatermark,
}

/// One rotation, ordered against the entry stream by riding the same
/// queue the batches do.
///
/// This is what replaces the async-flush branch's drain protocol. There,
/// the journal thread held the file and the flush thread held a raw
/// descriptor into it, so rotating meant proving no sync was in flight
/// before closing it. Here the file never leaves the writer, and FIFO
/// ordering does the same job structurally: a rotation cannot overtake
/// the batches that belong to the outgoing segment, because it is behind
/// them in the queue.
pub struct RotateCommand {
    /// First sequence of the new segment (the header's
    /// `starting_sequence`).
    pub starting_sequence: u64,
    /// Chain anchor linking the new segment to the outgoing one's tail.
    pub anchor: [u8; 32],
    /// Pre-staged segment from the preparer, when one was ready.
    pub prepared: Option<melin_journal::preparer::PreparedSegment>,
    /// Where the archived path (or the failure) goes back.
    ///
    /// Every command gets exactly one reply, including from a poisoned
    /// writer — the journal thread blocks here, so a silently dropped
    /// command would wedge the pipeline rather than fail it.
    reply: SyncSender<Result<std::path::PathBuf, melin_journal::JournalError>>,
}

/// Work handed to the writer, in submission order.
enum WriteItem {
    Batch(WriteBatch),
    Rotate(RotateCommand),
    /// Publish this state, with nothing new to write.
    ///
    /// Rotation changes the writer's durable state — a fresh segment,
    /// hence a new genesis-anchored chain value — without consuming
    /// input, and observers need a state consistent with the new on-disk
    /// layout. The publisher lives on the writer thread, so that update
    /// has to travel the same queue to stay ordered behind the batches it
    /// follows. It carries no bytes, so it forces no sync: the rotation
    /// it reports already made everything durable.
    Publish(FlushWatermark),
}

/// Producer half, held by the journal thread.
///
/// Not `Clone`: single-producer is what makes the ordering the writer
/// relies on — rotation ordered against the entry stream — a property of
/// the type rather than a convention.
pub struct WriteQueue {
    work: SyncSender<WriteItem>,
    /// Emptied buffers coming back from the writer, so steady state
    /// allocates nothing.
    recycled: Receiver<Vec<u8>>,
    shared: Arc<WriterShared>,
    submitted: u64,
}

/// Consumer half, moved onto the writer thread.
pub struct WriteQueueConsumer {
    work: Receiver<WriteItem>,
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
        match self.work.try_send(WriteItem::Batch(batch)) {
            Ok(()) => Submitted::Ok,
            Err(TrySendError::Full(item)) => {
                // Not queued, so it never happened: roll the counter back
                // or the next submit would leave a gap and the writer
                // would publish a `submit_seq` the encoder never sent.
                self.submitted -= 1;
                Submitted::Full(Box::new(unwrap_batch(item)))
            }
            Err(TrySendError::Disconnected(item)) => {
                self.submitted -= 1;
                Submitted::Poisoned(Box::new(unwrap_batch(item)))
            }
        }
    }

    /// Rotate the live segment, behind everything already submitted.
    ///
    /// Blocks until the writer has done it. Rotation is cold (a few per
    /// gigabyte) and the caller has nothing useful to do meanwhile: it
    /// must know whether the rotation succeeded before it re-anchors its
    /// chain, and a failure has to leave the encoder still writing to the
    /// *old* segment. Making this asynchronous would mean an encoder
    /// whose chain has moved to a segment that does not exist.
    ///
    /// The wait is bounded by the queue ahead of it plus one rotation, so
    /// it is the same stall the inline path already pays — minus the
    /// separate drain the flush-executor design needed.
    pub fn rotate(
        &mut self,
        starting_sequence: u64,
        anchor: [u8; 32],
        prepared: Option<melin_journal::preparer::PreparedSegment>,
    ) -> Result<std::path::PathBuf, melin_journal::JournalError> {
        self.submit_rotation(starting_sequence, anchor, prepared)?
            .wait()
    }

    /// Queue a rotation without waiting for it.
    ///
    /// The two halves of [`rotate`](Self::rotate), split so a test can
    /// establish "the rotation is queued behind these batches" as a fact
    /// rather than a race — which is the property being tested.
    fn submit_rotation(
        &mut self,
        starting_sequence: u64,
        anchor: [u8; 32],
        prepared: Option<melin_journal::preparer::PreparedSegment>,
    ) -> Result<PendingRotation, melin_journal::JournalError> {
        // Depth 1: exactly one reply, and the writer never blocks
        // delivering it.
        let (reply, replies) = sync_channel(1);
        let command = RotateCommand {
            starting_sequence,
            anchor,
            prepared,
            reply,
        };
        // Blocking send, unlike `submit`: the caller is already waiting,
        // and the queue ahead drains on its own.
        self.work
            .send(WriteItem::Rotate(command))
            .map_err(|_| writer_gone("submitting the rotation"))?;
        Ok(PendingRotation { replies })
    }

    /// Publish `watermark` with nothing new to write — see
    /// [`WriteItem::Publish`].
    ///
    /// Blocking, like [`rotate`](Self::rotate) and for the same reason:
    /// its callers are rotation-completion paths that have just waited
    /// for the writer anyway, and dropping the update would leave
    /// observers describing a segment layout that no longer exists.
    pub fn publish_state(&mut self, mut watermark: FlushWatermark) {
        // The *current* submit count, not the next one: a publish adds
        // no work, so the writer storing this as `published` must still
        // read as idle. Leaving it at zero would drive `published`
        // backwards, and `is_idle` — which shutdown waits on — would
        // never be true again.
        watermark.submit_seq = self.submitted;
        watermark.submit_ts = crate::trace::mono_trace_ns();
        // Discarded deliberately: the only failure is a writer thread
        // that unwound, which the journal loop's poison check already
        // reports. There is nothing useful to do with a state update for
        // a pipeline that is coming down.
        let _ = self.work.send(WriteItem::Publish(watermark));
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

/// A rotation the writer has been given but has not answered yet.
struct PendingRotation {
    replies: Receiver<Result<std::path::PathBuf, melin_journal::JournalError>>,
}

impl PendingRotation {
    /// Block until the writer answers.
    ///
    /// A poisoned writer still replies (with an error), so this can only
    /// outlive the writer thread if it unwound — which drops the sender
    /// and disconnects the channel.
    fn wait(self) -> Result<std::path::PathBuf, melin_journal::JournalError> {
        self.replies
            .recv()
            .map_err(|_| writer_gone("awaiting the rotation"))?
    }
}

/// Recover the batch from an item the channel handed back.
///
/// `try_send` returns whatever it could not deliver, and only `submit`
/// ever sends a `Batch` — a `Rotate` coming back here would mean the
/// channel returned something nobody put in.
fn unwrap_batch(item: WriteItem) -> WriteBatch {
    match item {
        WriteItem::Batch(b) => b,
        _ => unreachable!("the work channel returned a non-batch to the batch submit path"),
    }
}

/// The writer thread is gone without answering — it unwound.
fn writer_gone(during: &str) -> melin_journal::JournalError {
    melin_journal::JournalError::Io(std::io::Error::other(format!(
        "journal writer thread is gone ({during})"
    )))
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
    /// Archive the live segment and open a fresh one anchored to
    /// `(starting_sequence, anchor)`. Returns the archived path.
    ///
    /// A failure must leave the live segment usable, because the encoder
    /// keeps writing to it: the rotation is reported back and retried
    /// after a backoff.
    fn rotate(
        &mut self,
        starting_sequence: u64,
        anchor: [u8; 32],
        prepared: Option<melin_journal::preparer::PreparedSegment>,
    ) -> Result<std::path::PathBuf, melin_journal::JournalError>;
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

    fn rotate(
        &mut self,
        starting_sequence: u64,
        anchor: [u8; 32],
        prepared: Option<melin_journal::preparer::PreparedSegment>,
    ) -> Result<std::path::PathBuf, melin_journal::JournalError> {
        melin_journal::SegmentFile::rotate(self, starting_sequence, anchor, prepared)
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
/// A rotation reached mid-drain ends the run: it must land *after* the
/// batches ahead of it are durable, and `SegmentFile::rotate` archives
/// the file out from under anything still buffered.
///
/// Returns when `shutdown` is set, handing back the segment so the
/// journal thread can re-attach it. A poisoned writer keeps idling
/// rather than exiting, so it stays joinable in every state without a
/// dedicated wake-up — and keeps answering rotations (with an error), so
/// a journal thread blocked on one is never stranded.
pub fn run_writer<Io, P>(
    queue: WriteQueueConsumer,
    mut io: Io,
    mut publish: P,
    busy_spin: bool,
    shutdown: &AtomicBool,
) -> Io
where
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
            return io;
        }

        let Ok(first) = queue.work.try_recv() else {
            crate::pipeline::idle_wait(&mut idle_spins, busy_spin);
            continue;
        };
        idle_spins = 0;

        if queue.shared.is_poisoned() {
            // Nothing more will ever reach the disk, and publishing again
            // would advance a cursor past bytes that never landed. Still
            // answer rotations: the journal thread blocks on the reply,
            // so silence here would wedge the teardown it is heading for.
            refuse(&queue, first);
            continue;
        }

        // Drain everything queued, not just the one item: one `fdatasync`
        // covers all of them either way, so writing them together
        // converts queue depth into fewer, larger syncs.
        let mut last = None;
        let mut wrote = false;
        let mut rotation = None;
        let mut next = Some(first);
        while let Some(item) = next.take() {
            let b = match item {
                WriteItem::Batch(b) => b,
                // Stop the run here. The batches already written are
                // synced and published below, and only then does the
                // file get archived.
                WriteItem::Rotate(c) => {
                    rotation = Some(c);
                    break;
                }
                // Rides to the publish below without forcing a sync of
                // its own; if batches precede it in this run, their sync
                // still happens first.
                WriteItem::Publish(w) => {
                    last = Some(w);
                    next = queue.work.try_recv().ok();
                    continue;
                }
            };
            if let Err(e) = io.write_at(&b.bytes, b.offset) {
                poison(&queue.shared, &e);
                break;
            }
            last = Some(b.watermark);
            wrote = true;
            // Hand the buffer back for reuse. A full recycle channel
            // cannot happen (same depth as the work queue), but dropping
            // the buffer would only cost an allocation, never
            // correctness.
            let _ = queue.recycled.try_send(b.bytes);
            next = queue.work.try_recv().ok();
        }

        if !queue.shared.is_poisoned()
            && let Some(watermark) = last
        {
            // Nothing written means nothing to force: a publish-only run
            // reports state a completed rotation already made durable.
            let synced = if wrote { io.sync() } else { Ok(()) };
            if let Err(e) = synced {
                poison(&queue.shared, &e);
            } else {
                publish(&watermark);
                #[cfg(feature = "latency-trace")]
                handoff_rec.record_elapsed(watermark.submit_ts, crate::trace::mono_trace_ns());
                // The journal seq before the submit counter: a reader
                // that sees the counter advance must not then read a
                // stale companion.
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

        if let Some(command) = rotation {
            let result = if queue.shared.is_poisoned() {
                // The outgoing segment has bytes that never reached the
                // disk. Sealing it would publish that hole as a complete
                // segment.
                Err(writer_gone("the writer failed before the rotation"))
            } else {
                io.rotate(command.starting_sequence, command.anchor, command.prepared)
            };
            reply(command.reply, result);
        }
    }
}

/// Answer a command that arrived at a writer which will never run again.
///
/// Batches are dropped — their bytes are already lost with the failed
/// sync — but a rotation's caller is *blocked*, so it gets its error.
fn refuse(queue: &WriteQueueConsumer, item: WriteItem) {
    match item {
        WriteItem::Batch(b) => {
            let _ = queue.recycled.try_send(b.bytes);
        }
        WriteItem::Rotate(c) => reply(
            c.reply,
            Err(writer_gone("the writer failed before the rotation")),
        ),
        // Dropped: publishing after a failure would advance cursors the
        // disk never backed.
        WriteItem::Publish(_) => {}
    }
}

/// Send a rotation's outcome back.
///
/// A closed channel means the caller unwound between submitting and
/// waiting; nothing is owed to a receiver that is gone, and the writer
/// must keep running so the rest of the teardown can join it.
fn reply(
    tx: SyncSender<Result<std::path::PathBuf, melin_journal::JournalError>>,
    result: Result<std::path::PathBuf, melin_journal::JournalError>,
) {
    let _ = tx.send(result);
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
        /// Each rotation with the highest `journal_seq` published when it
        /// happened. That is the invariant in one number: a rotation may
        /// only archive the outgoing segment once everything encoded into
        /// it is durable, and publication is what says so.
        rotations: Mutex<Vec<(u64, u64)>>,
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
        fn rotations(&self) -> Vec<(u64, u64)> {
            self.rotations.lock().expect("rotations").clone()
        }
        /// Highest `journal_seq` published so far, `0` if nothing has been.
        fn published_seq(&self) -> u64 {
            self.published
                .lock()
                .expect("published")
                .last()
                .map_or(0, |(w, _)| w.journal_seq.get())
        }
    }

    /// `SegmentIo` that records, and optionally gates or fails.
    struct TestIo<'a> {
        rec: &'a Recorder,
        /// Announces each sync and waits for release, when present.
        gate: Option<(Sender<()>, StdReceiver<()>)>,
        fail_write: Option<i32>,
        fail_sync: Option<i32>,
        fail_rotate: bool,
        shutdown: &'a AtomicBool,
    }

    impl<'a> TestIo<'a> {
        fn new(rec: &'a Recorder, shutdown: &'a AtomicBool) -> Self {
            Self {
                rec,
                gate: None,
                fail_write: None,
                fail_sync: None,
                fail_rotate: false,
                shutdown,
            }
        }
        fn gated(mut self, entered: Sender<()>, release: StdReceiver<()>) -> Self {
            self.gate = Some((entered, release));
            self
        }
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

        fn rotate(
            &mut self,
            starting_sequence: u64,
            _anchor: [u8; 32],
            _prepared: Option<melin_journal::preparer::PreparedSegment>,
        ) -> Result<std::path::PathBuf, melin_journal::JournalError> {
            if self.fail_rotate {
                return Err(melin_journal::JournalError::Io(
                    std::io::Error::from_raw_os_error(28), // ENOSPC
                ));
            }
            let published = self.rec.published_seq();
            self.rec
                .rotations
                .lock()
                .expect("rotations")
                .push((starting_sequence, published));
            Ok(std::path::PathBuf::from(format!(
                "archive-{starting_sequence}"
            )))
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
            let io = TestIo::new(&rec, &shutdown);
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
            let io = TestIo::new(&rec, &shutdown).gated(entered_tx, release_rx);
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
            let io = TestIo::new(&rec, &shutdown).gated(entered_tx, release_rx);
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
            let io = TestIo::new(&rec, &shutdown);
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
                fail_sync: Some(5), // EIO
                ..TestIo::new(&rec, &shutdown)
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
                fail_write: Some(28), // ENOSPC
                ..TestIo::new(&rec, &shutdown)
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
    fn a_rotation_lands_after_the_batches_queued_before_it() {
        // The ordering the queue exists to provide. Entries encoded
        // before a rotation belong to the *outgoing* segment; a rotation
        // that overtook them would archive the file with their bytes
        // still unwritten, sealing a segment around a hole.
        let rec = Recorder::default();
        let shutdown = AtomicBool::new(false);
        let (mut q, consumer, _shared) = write_queue(WRITE_QUEUE_DEPTH);

        // Fill the queue *before* the writer exists, so the rotation is
        // provably behind four unwritten batches rather than racing them.
        // Parking a running writer and queueing behind it does not
        // establish that: `spawn` returning does not mean the rotation
        // has been sent, so the writer can finish the batches first and
        // meet the rotation on its own — with nothing to overtake, the
        // ordering goes untested.
        for i in 1..=4u64 {
            expect_queued(q.submit(batch(i, i * 10, i * 100, 8)));
        }
        let pending = q
            .submit_rotation(99, [7u8; 32], None)
            .expect("the queue has room for the rotation");

        std::thread::scope(|s| {
            let _guard = ShutdownGuard(&shutdown);
            let io = TestIo::new(&rec, &shutdown);
            s.spawn(|| run_writer(consumer, io, |w| rec.record_publish(w), true, &shutdown));

            let archived = pending
                .wait()
                .expect("the rotation must reach the writer and come back");
            assert_eq!(archived, std::path::PathBuf::from("archive-99"));

            assert_eq!(
                rec.write_count(),
                4,
                "every batch queued ahead of the rotation must be written first"
            );
            let (seq, published_at_rotation) = rec.rotations()[0];
            assert_eq!(seq, 99);
            assert_eq!(
                published_at_rotation, 4,
                "the segment was archived with only journal_seq {published_at_rotation} durable, \
                 but batches through 4 belong to it — the rotation overtook them and sealed the \
                 outgoing segment around bytes that had not been forced"
            );

            shutdown.store(true, Ordering::Relaxed);
        });
    }

    #[test]
    fn a_publish_only_item_advances_cursors_without_a_redundant_sync() {
        // A batch of queries journals nothing but still advances ring
        // progress, and a completed rotation changes durable state
        // without consuming input. Both must publish — freezing ring
        // progress would eventually stall the producer against a ring
        // that never drains — and neither owes a sync, because neither
        // wrote anything.
        let rec = Recorder::default();
        let shutdown = AtomicBool::new(false);
        let (mut q, consumer, _shared) = write_queue(WRITE_QUEUE_DEPTH);

        std::thread::scope(|s| {
            let _guard = ShutdownGuard(&shutdown);
            let io = TestIo::new(&rec, &shutdown);
            s.spawn(|| run_writer(consumer, io, |w| rec.record_publish(w), true, &shutdown));

            q.publish_state(watermark(9, 90));
            wait_until("the publish to land", || !rec.published().is_empty());

            let (w, _) = rec.published()[0];
            assert_eq!(w.journal_seq, WireSeq::new(9));
            assert_eq!(w.ring_progress, RingPos::new(90));
            assert_eq!(
                rec.write_count(),
                0,
                "a publish carries no bytes, so nothing may be written"
            );
            assert_eq!(
                rec.syncs(),
                0,
                "nothing was written, so forcing the disk would be pure latency"
            );
            assert!(
                q.is_idle(),
                "a publish adds no work, so it must leave the queue idle — a publish that \
                 drove `published` backwards would wedge the shutdown wait on `is_idle`"
            );

            shutdown.store(true, Ordering::Relaxed);
        });
    }

    #[test]
    fn a_publish_after_a_batch_leaves_the_queue_idle() {
        // The same wedge, reached the other way: with submits already
        // counted, a publish that stamped the wrong counter would leave
        // `published` behind `submitted` with no work outstanding.
        let rec = Recorder::default();
        let shutdown = AtomicBool::new(false);
        let (mut q, consumer, _shared) = write_queue(WRITE_QUEUE_DEPTH);

        std::thread::scope(|s| {
            let _guard = ShutdownGuard(&shutdown);
            let io = TestIo::new(&rec, &shutdown);
            s.spawn(|| run_writer(consumer, io, |w| rec.record_publish(w), true, &shutdown));

            for i in 1..=3u64 {
                expect_queued(q.submit(batch(i, i * 10, i * 100, 8)));
            }
            wait_until("the batches to drain", || q.is_idle());

            // Not a publication count — the writer coalesces a drained
            // run into one — so wait on the value this publish carries.
            q.publish_state(watermark(4, 40));
            wait_until("the publish to land", || rec.published_seq() == 4);
            assert!(
                q.is_idle(),
                "the queue must be idle after a publish, not stuck one behind"
            );

            shutdown.store(true, Ordering::Relaxed);
        });
    }

    #[test]
    fn a_batch_followed_by_a_publish_still_syncs_the_batch() {
        // The publish must not suppress the sync its own run owes: the
        // batch ahead of it in the queue is real data, and publishing
        // its position without forcing it would be ack-before-persist.
        let rec = Recorder::default();
        let shutdown = AtomicBool::new(false);
        let (mut q, consumer, _shared) = write_queue(WRITE_QUEUE_DEPTH);

        // Both queued before the writer starts, so they land in one
        // drained run — the case where the two could interfere.
        expect_queued(q.submit(batch(1, 10, 0, 8)));
        q.publish_state(watermark(2, 20));

        std::thread::scope(|s| {
            let _guard = ShutdownGuard(&shutdown);
            let io = TestIo::new(&rec, &shutdown);
            s.spawn(|| run_writer(consumer, io, |w| rec.record_publish(w), true, &shutdown));

            wait_until("the run to drain", || !rec.published().is_empty());
            for (w, syncs_at_publish) in rec.published() {
                assert!(
                    syncs_at_publish >= 1,
                    "published journal_seq {} with {syncs_at_publish} syncs completed — \
                     the publish rode past the batch's sync instead of behind it",
                    w.journal_seq.get()
                );
            }

            shutdown.store(true, Ordering::Relaxed);
        });
    }

    #[test]
    fn lag_reports_what_is_submitted_but_not_yet_durable() {
        // The health gauge operators read to see the pipeline riding
        // through a stalling disk.
        let rec = Recorder::default();
        let shutdown = AtomicBool::new(false);
        let (mut q, consumer, _shared) = write_queue(WRITE_QUEUE_DEPTH);
        let (entered_tx, entered_rx) = channel();
        let (release_tx, release_rx) = channel();

        assert!(q.is_idle(), "nothing submitted yet");
        assert_eq!(q.lag(WireSeq::new(0)), 0);

        std::thread::scope(|s| {
            let _guard = ShutdownGuard(&shutdown);
            let io = TestIo::new(&rec, &shutdown).gated(entered_tx, release_rx);
            s.spawn(|| run_writer(consumer, io, |_| {}, true, &shutdown));

            expect_queued(q.submit(batch(50, 500, 0, 8)));
            assert!(!q.is_idle(), "a submit is outstanding");
            entered_rx
                .recv_timeout(TERMINATION_TIMEOUT)
                .expect("sync entered");
            assert_eq!(
                q.lag(WireSeq::new(50)),
                50,
                "nothing published yet, so everything submitted is lag"
            );

            release_tx.send(()).expect("writer alive");
            wait_until("the batch to drain", || q.is_idle());
            assert_eq!(q.lag(WireSeq::new(50)), 0);

            shutdown.store(true, Ordering::Relaxed);
            let _ = release_tx.send(());
        });
    }

    #[test]
    fn a_failed_rotation_comes_back_as_an_error_rather_than_hanging() {
        // The journal thread blocks on the reply and only re-anchors its
        // chain on success. A dropped reply would wedge it; a lost error
        // would move its chain onto a segment that does not exist.
        let rec = Recorder::default();
        let shutdown = AtomicBool::new(false);
        let (mut q, consumer, _shared) = write_queue(WRITE_QUEUE_DEPTH);

        std::thread::scope(|s| {
            let _guard = ShutdownGuard(&shutdown);
            let io = TestIo {
                fail_rotate: true,
                ..TestIo::new(&rec, &shutdown)
            };
            s.spawn(|| run_writer(consumer, io, |_| {}, true, &shutdown));

            let err = q.rotate(99, [7u8; 32], None).expect_err("rotation fails");
            let melin_journal::JournalError::Io(io) = err else {
                panic!("a rotation failure must arrive as the I/O error it was")
            };
            assert_eq!(
                io.raw_os_error(),
                Some(28),
                "the errno must survive the trip back, or the operator sees no cause"
            );
            // The writer stays usable: a failed rotation leaves the live
            // segment in place, so the encoder keeps writing to it.
            expect_queued(q.submit(batch(1, 10, 0, 8)));
            wait_until("the writer to keep working", || q.is_idle());

            shutdown.store(true, Ordering::Relaxed);
        });
    }

    #[test]
    fn a_poisoned_writer_still_answers_a_rotation() {
        // Otherwise the journal thread blocks forever on a reply that
        // never comes — a wedged pipeline instead of a failed one, on
        // exactly the path (a dying disk) where operators need the error.
        let rec = Recorder::default();
        let shutdown = AtomicBool::new(false);
        let (mut q, consumer, shared) = write_queue(WRITE_QUEUE_DEPTH);

        std::thread::scope(|s| {
            let _guard = ShutdownGuard(&shutdown);
            let io = TestIo {
                fail_sync: Some(5), // EIO
                ..TestIo::new(&rec, &shutdown)
            };
            s.spawn(|| run_writer(consumer, io, |_| {}, true, &shutdown));

            expect_queued(q.submit(batch(1, 10, 0, 8)));
            wait_until("poison to latch", || shared.is_poisoned());

            q.rotate(99, [7u8; 32], None)
                .expect_err("a poisoned writer must refuse, not seal a segment with a hole");
            assert!(
                rec.rotations().is_empty(),
                "a poisoned writer must not archive the outgoing segment"
            );

            shutdown.store(true, Ordering::Relaxed);
        });
    }

    #[test]
    fn shutdown_hands_the_segment_back() {
        // The stage re-attaches it, so the writer it returns can flush,
        // rotate and be inspected like one that never handed off.
        let rec = Recorder::default();
        let shutdown = AtomicBool::new(false);
        let (mut q, consumer, _shared) = write_queue(WRITE_QUEUE_DEPTH);

        std::thread::scope(|s| {
            let _guard = ShutdownGuard(&shutdown);
            let io = TestIo::new(&rec, &shutdown);
            let writer = s.spawn(|| run_writer(consumer, io, |_| {}, true, &shutdown));

            expect_queued(q.submit(batch(1, 10, 0, 8)));
            wait_until("the batch to drain", || q.is_idle());
            shutdown.store(true, Ordering::Relaxed);

            let returned = writer.join().expect("the writer exits cleanly");
            assert_eq!(
                returned.rec.write_count(),
                1,
                "the returned handle must be the one that did the writing"
            );
        });
    }
}
