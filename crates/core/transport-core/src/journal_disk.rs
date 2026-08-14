//! The journal's disk thread: writes, syncs, and publishes durability.
//!
//! The journal stage is split in two. The sequencing thread orders
//! events, allocates sequences, encodes them, chains them, and feeds
//! replicas — all in memory. Everything that touches the device happens
//! here instead: `pwrite`, `fdatasync`, segment rotation, and the
//! cursor publication that follows a successful sync.
//!
//! ## Why publication lives here
//!
//! Every cursor this thread publishes means "durable", and only this
//! thread knows when a batch became durable. Publishing from the
//! sequencer would mean publishing at submit time, which on a replica
//! would let the ack path acknowledge entries the disk has not taken —
//! the persist-before-ack guarantee is exactly this ordering:
//!
//! ```text
//! write batches → fdatasync → publish cursors → release ring slots
//! ```
//!
//! Slots are released last, so the sequencer's drain test ("consumer
//! caught up to my cursor") also proves the cursors are published. The
//! rotation rendezvous and shutdown both lean on that.
//!
//! ## Failure
//!
//! A write or sync error is as fatal here as it was inline: the journal
//! is broken. This thread stops publishing (cursors freeze, so no ack
//! can cover non-durable data — the durability gate holds, which is
//! correct), records the error, and parks. The sequencer notices the
//! poison flag and surfaces the original error through the usual fatal
//! shutdown path.
//!
//! An *unexpected* exit — a panic — must not hang the pipeline either.
//! Every wait on this thread (claim a slot, drain, await a rotation)
//! spins on `poisoned` with no timeout, so a thread that unwound
//! silently would strand the sequencer with no diagnostic.
//!
//! Which mechanism prevents that depends on the profile. Release builds
//! set `panic = "abort"`, so a panic here takes the process down
//! immediately — loud, and impossible to hang. Debug and test builds
//! unwind, and there [`PoisonOnUnwind`] converts the unwind into the
//! same poison a write failure produces, so the sequencer reports a
//! broken journal instead of spinning. The guarantee holds in both;
//! only the failure's shape differs.

use std::io::IoSlice;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use melin_journal::JournalError;
use melin_journal::preparer::PreparedSegment;
use melin_journal::segment_file::SegmentFile;
use melin_journal::write_ring::{JournalWriteConsumer, JournalWriteMeta};
use melin_pipeline::padding::Sequence;
use melin_pipeline::seqlock::SeqLockWriter;

use crate::cursors::{AdvertisedJournalTip, DurableWireSeqCursor, RingPos, WireSeq};
use crate::pipeline::{FsyncState, idle_wait};

/// Iovecs per vectored write. Comfortably under every supported
/// kernel's `IOV_MAX` (1024 on Linux) and at least the write ring's
/// default depth, so a full backlog clears in one syscall.
const MAX_IOV: usize = 64;

const _: () = assert!(MAX_IOV >= melin_journal::write_ring::DEFAULT_CAPACITY);

/// A rotation the sequencer wants performed at the current boundary.
///
/// Carries what the file half cannot know: the boundary's sequence and
/// the outgoing segment's chain tail, which become the new segment's
/// header. The staged segment (when the preparer has one) rides along
/// so the fast path stays on this thread.
pub struct RotateRequest {
    pub prepared: Option<PreparedSegment>,
    pub starting_sequence: u64,
    pub anchor_hash: [u8; 32],
}

/// Rendezvous slot for a rotation: request in, outcome out.
///
/// One `Mutex` for both directions because a rotation is strictly cold
/// (a few per gigabyte) and the threads are never both interested at
/// once — the sequencer waits for the outcome it asked for.
enum RotateSlot {
    Idle,
    Requested(RotateRequest),
    Done(Result<std::path::PathBuf, JournalError>),
}

/// Control block shared by the sequencing and disk threads.
///
/// Everything here is cold: a stop flag, the poison latch, the rotation
/// rendezvous, and a gauge. The hot path between the threads is the
/// write ring, not this.
pub struct DiskControl {
    /// Set by the sequencer when the pipeline is shutting down. The
    /// disk thread drains what is already published, then exits.
    stop: AtomicBool,
    /// Latched when a write or sync fails. The sequencer polls this
    /// once per loop and turns it into a fatal shutdown.
    poisoned: AtomicBool,
    /// The error behind `poisoned`, taken by the sequencer so the
    /// operator sees the original cause rather than a generic message.
    error: Mutex<Option<JournalError>>,
    /// Rotation rendezvous — see [`RotateSlot`].
    rotate: Mutex<RotateSlot>,
}

impl Default for DiskControl {
    fn default() -> Self {
        Self::new()
    }
}

impl DiskControl {
    pub fn new() -> Self {
        Self {
            stop: AtomicBool::new(false),
            poisoned: AtomicBool::new(false),
            error: Mutex::new(None),
            rotate: Mutex::new(RotateSlot::Idle),
        }
    }

    /// Ask the disk thread to stop once it has drained.
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Release);
    }

    /// Whether the journal has failed. Checked once per sequencer loop.
    pub fn poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Acquire)
    }

    /// Take the failure, if any. Returns `None` once taken — the
    /// sequencer surfaces it exactly once.
    pub fn take_error(&self) -> Option<JournalError> {
        self.error
            .lock()
            .expect("journal disk error mutex poisoned")
            .take()
    }

    /// Submit a rotation. The caller must have drained the write ring
    /// first, so the disk thread has nothing in flight to order against
    /// the segment swap.
    ///
    /// Panics if a rotation is already outstanding — one at a time is
    /// structural (the sequencer blocks until it has its answer).
    pub fn request_rotation(&self, request: RotateRequest) {
        let mut slot = self.rotate.lock().expect("journal rotate mutex poisoned");
        assert!(
            matches!(*slot, RotateSlot::Idle),
            "a rotation is already outstanding"
        );
        *slot = RotateSlot::Requested(request);
    }

    /// Collect a finished rotation's outcome, or `None` while it is
    /// still running.
    pub fn take_rotation_result(&self) -> Option<Result<std::path::PathBuf, JournalError>> {
        let mut slot = self.rotate.lock().expect("journal rotate mutex poisoned");
        match std::mem::replace(&mut *slot, RotateSlot::Idle) {
            RotateSlot::Done(result) => Some(result),
            // Not finished — put the state back untouched.
            other => {
                *slot = other;
                None
            }
        }
    }

    /// Latch a failure and its cause.
    fn poison(&self, error: JournalError) {
        // Store the error before the flag so a sequencer that observes
        // the flag always finds the cause.
        *self
            .error
            .lock()
            .expect("journal disk error mutex poisoned") = Some(error);
        self.poisoned.store(true, Ordering::Release);
    }

    /// Take a pending rotation request, if one is waiting.
    fn take_rotation_request(&self) -> Option<RotateRequest> {
        let mut slot = self.rotate.lock().expect("journal rotate mutex poisoned");
        match std::mem::replace(&mut *slot, RotateSlot::Idle) {
            RotateSlot::Requested(request) => Some(request),
            other => {
                *slot = other;
                None
            }
        }
    }

    /// Publish a rotation's outcome back to the sequencer.
    fn finish_rotation(&self, result: Result<std::path::PathBuf, JournalError>) {
        let mut slot = self.rotate.lock().expect("journal rotate mutex poisoned");
        *slot = RotateSlot::Done(result);
    }
}

/// Latches poison if the disk thread leaves its loop by unwinding.
///
/// The sequencer's every wait on this thread is a spin gated on
/// `poisoned`, with no timeout — a panic that left the flag clear would
/// hang the pipeline rather than fail it. Disarmed on the normal exits,
/// so this only ever fires on a panic.
///
/// Dead code in release builds, which are `panic = "abort"`: there the
/// process is already gone. This covers debug and test builds, and it
/// keeps the guarantee from depending on a profile setting that lives
/// in a different file.
struct PoisonOnUnwind {
    control: Arc<DiskControl>,
    armed: bool,
}

impl PoisonOnUnwind {
    fn new(control: Arc<DiskControl>) -> Self {
        Self {
            control,
            armed: true,
        }
    }

    /// The loop returned normally — whatever it recorded stands.
    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for PoisonOnUnwind {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // Do not overwrite a cause already latched: a poisoning error
        // followed by a panic in the same teardown should still report
        // the error the operator needs.
        if self.control.poisoned() {
            return;
        }
        self.control.poison(JournalError::Io(std::io::Error::other(
            "journal disk thread panicked",
        )));
    }
}

/// The cursors a batch's durability makes true. Held by the disk thread
/// because it is the only thread that knows when that moment is.
///
/// Each handle is optional and independent: a standalone node has no
/// advertised tip, shadow snapshots may be off, and the tests wire only
/// what they assert on.
pub struct DurabilityCursors {
    /// Input-ring consumer progress. Gates slot reuse upstream and, on
    /// a replica, persisted acks.
    pub input_progress: Arc<Sequence>,
    /// Highest wire seq durably persisted — the response stage's
    /// `persisted` cursor and the replica handshake value.
    pub durable_wire_seq: Option<DurableWireSeqCursor>,
    /// Control-plane advertised tip (primary only).
    pub advertised_tip: Option<AdvertisedJournalTip>,
    /// Post-fsync state for shadow snapshots and replica handshakes.
    pub fsync_state: Option<SeqLockWriter<FsyncState>>,
}

impl DurabilityCursors {
    /// Publish everything a durable batch makes true.
    ///
    /// Ring progress goes first, matching the order the inline sync
    /// point used, so nothing downstream sees a different interleaving
    /// than it did before the split.
    fn publish(&mut self, meta: &JournalWriteMeta) {
        // `Release`, exactly as `Consumer::set_progress` published it
        // from the inline sync point — readers pair with `Acquire`.
        self.input_progress
            .get()
            .store(meta.ring_progress, Ordering::Release);
        if let Some(ref mut publisher) = self.fsync_state {
            publisher.store(FsyncState {
                journal_seq: WireSeq::new(meta.journal_seq),
                chain_hash: meta.chain_hash,
                input_ring_seq: RingPos::new(meta.input_ring_seq),
            });
        }
        if let Some(ref cursor) = self.durable_wire_seq {
            cursor.store(WireSeq::new(meta.journal_seq));
        }
        if let Some(ref tip) = self.advertised_tip {
            // `advance`, not a plain store: across a promotion the
            // receiver left the tip at its in-memory accepted position,
            // which the new primary's journal only reaches after the
            // drained ring is flushed — a plain store would regress the
            // advertised tip in that window.
            tip.advance(WireSeq::new(meta.journal_seq));
        }
    }
}

/// The disk thread's owned state.
pub struct JournalDisk {
    segment: SegmentFile,
    batches: JournalWriteConsumer,
    cursors: DurabilityCursors,
    control: Arc<DiskControl>,
    /// When true, never yield to the OS scheduler — spin with PAUSE.
    /// Same discipline as the other pipeline stages.
    busy_spin: bool,
    /// Test-only failure injection. The failures this path exists for
    /// (EIO, ENOSPC) cannot be provoked portably from a unit test, and
    /// the branch they drive — freeze durability, latch the cause — is
    /// too important to leave unexercised.
    #[cfg(test)]
    fail_next_sync: Option<JournalError>,
    /// Test-only panic injection, for the same reason: a panic on this
    /// thread cannot be provoked from outside it, and the guarantee it
    /// drives — the sequencer fails instead of spinning forever — is
    /// otherwise untestable.
    #[cfg(test)]
    panic_next_drain: bool,
}

impl JournalDisk {
    pub fn new(
        segment: SegmentFile,
        batches: JournalWriteConsumer,
        cursors: DurabilityCursors,
        control: Arc<DiskControl>,
        busy_spin: bool,
    ) -> Self {
        Self {
            segment,
            batches,
            cursors,
            control,
            busy_spin,
            #[cfg(test)]
            fail_next_sync: None,
            #[cfg(test)]
            panic_next_drain: false,
        }
    }

    /// Make the next sync fail with `error` (tests only).
    #[cfg(test)]
    fn inject_sync_failure(&mut self, error: JournalError) {
        self.fail_next_sync = Some(error);
    }

    /// Make the next drain panic (tests only).
    #[cfg(test)]
    fn inject_panic(&mut self) {
        self.panic_next_drain = true;
    }

    /// Run until the sequencer stops the thread, returning the live
    /// segment so the writer can be reassembled for recovery and
    /// promotion.
    ///
    /// Returns on poison too: the segment comes back either way, and
    /// the error travels through the control block rather than the
    /// return type, because the sequencer polls for it long before the
    /// thread is joined.
    pub fn run(mut self) -> SegmentFile {
        let guard = PoisonOnUnwind::new(Arc::clone(&self.control));
        self.run_loop();
        guard.disarm();
        self.segment
    }

    /// The loop proper, split out so [`PoisonOnUnwind`] covers every
    /// path through it — including the ones that are not supposed to
    /// exist.
    fn run_loop(&mut self) {
        let mut idle_spins: u32 = 0;
        loop {
            match self.drain_and_sync() {
                Ok(true) => {
                    idle_spins = 0;
                    // More may have arrived while we were syncing.
                    continue;
                }
                Ok(false) => {}
                Err(e) => {
                    tracing::error!(error = %e, "journal disk write failed — journal is broken");
                    self.control.poison(e);
                    return;
                }
            }

            // Rotation only ever arrives on a drained ring (the
            // sequencer waits for that before asking), so acting on it
            // here cannot reorder against pending batches.
            if let Some(request) = self.control.take_rotation_request() {
                self.rotate(request);
                idle_spins = 0;
                continue;
            }

            if self.control.stop.load(Ordering::Acquire) {
                return;
            }

            idle_wait(&mut idle_spins, self.busy_spin);
        }
    }

    /// Write every published batch, sync once, then publish and
    /// release. Returns whether anything was consumed.
    ///
    /// One `fdatasync` per drain rather than per batch is where the
    /// group-commit win lives: a backlog built up during a stall costs
    /// exactly one sync to clear.
    fn drain_and_sync(&mut self) -> Result<bool, JournalError> {
        #[cfg(test)]
        assert!(
            !self.panic_next_drain,
            "injected journal disk panic (test seam)"
        );

        let Some(meta) = self.batches.stage_ready() else {
            return Ok(false);
        };
        let bytes_written = self.batches.staged_total_bytes();

        if bytes_written > 0 {
            // One `pwritev` for the whole backlog rather than one
            // `pwrite` each. These calls run *before* the sync, so they
            // delay durability for every batch behind them — at 64
            // batches the difference measured 382 µs against 59 µs.
            //
            // Chunked at `MAX_IOV` because a vectored write is capped
            // by the kernel's `IOV_MAX`; with the default ring depth
            // this is a single pass. Empty batches carry cursors only
            // and contribute no iovec.
            let mut iov = [IoSlice::new(&[]); MAX_IOV];
            let mut n = 0usize;
            for i in 0..self.batches.staged() {
                let bytes = self.batches.staged_bytes(i);
                if bytes.is_empty() {
                    continue;
                }
                iov[n] = IoSlice::new(bytes);
                n += 1;
                if n == MAX_IOV {
                    self.segment.write_vectored(&mut iov[..n])?;
                    n = 0;
                }
            }
            if n > 0 {
                self.segment.write_vectored(&mut iov[..n])?;
            }
        }

        // Paced retry of a failed post-rotation dir fsync. Driven per
        // *drain*, not per sync: a byte-empty batch still ticks it, so a
        // `no-persist` node — which never writes and so would otherwise
        // never retry — and a journal that goes quiet right after a
        // failed rotation both still recover their un-synced dirent.
        // One branch when nothing is pending.
        self.segment.poll_dir_fsync_retry();

        // A batch can be byte-empty and still carry cursors: one made
        // only of queries (never journaled), or any batch under
        // `no-persist`. There is nothing to make durable, so skip the
        // sync — but still publish, because the input-ring slots those
        // events occupy have to be released upstream.
        if bytes_written > 0 {
            #[cfg(test)]
            if let Some(injected) = self.fail_next_sync.take() {
                return Err(injected);
            }
            self.segment.sync()?;
        }

        // Durable: publish first, release second. The sequencer's drain
        // test observes the release, so this order is what makes
        // "drained" imply "published".
        self.cursors.publish(&meta);
        self.batches.commit();
        Ok(true)
    }

    /// Execute a rendezvous rotation and hand the outcome back.
    ///
    /// A failure is *not* poison: the file half restores the live
    /// segment, so journaling continues on it. The sequencer applies
    /// its usual backoff and retries at a later boundary.
    fn rotate(&mut self, request: RotateRequest) {
        let result = self.segment.rotate(
            request.prepared,
            request.starting_sequence,
            request.anchor_hash,
        );
        self.control.finish_rotation(result);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use melin_journal::write_ring::{JournalWriteProducer, build_journal_write_ring};
    use std::sync::atomic::AtomicU64;

    fn cursors() -> (DurabilityCursors, Arc<Sequence>, DurableWireSeqCursor) {
        let progress = Arc::new(Sequence::new(AtomicU64::new(0)));
        let durable = DurableWireSeqCursor::detached(WireSeq::new(0));
        (
            DurabilityCursors {
                input_progress: Arc::clone(&progress),
                durable_wire_seq: Some(durable.clone()),
                advertised_tip: None,
                fsync_state: None,
            },
            progress,
            durable,
        )
    }

    fn segment(dir: &std::path::Path) -> SegmentFile {
        SegmentFile::create_continuing(&dir.join("test.journal"), 1, [0u8; 32]).unwrap()
    }

    fn submit(producer: &mut JournalWriteProducer, bytes: &[u8], meta: JournalWriteMeta) {
        let mut claim = producer.try_claim().unwrap();
        claim.bytes_mut()[..bytes.len()].copy_from_slice(bytes);
        producer.publish(claim, meta);
    }

    fn meta(len: usize, journal_seq: u64, progress: u64) -> JournalWriteMeta {
        JournalWriteMeta {
            len: len as u32,
            journal_seq,
            chain_hash: [journal_seq as u8; 32],
            ring_progress: progress,
            input_ring_seq: progress,
        }
    }

    /// The steady-state contract: batches land on disk, and the cursors
    /// that mean "durable" only move once they have.
    #[test]
    fn batches_land_on_disk_and_publish_their_cursors() {
        let dir = tempfile::tempdir().unwrap();
        let (mut producer, consumer) = build_journal_write_ring(4);
        let (cursors, progress, durable) = cursors();
        let control = Arc::new(DiskControl::new());
        let mut disk = JournalDisk::new(
            segment(dir.path()),
            consumer,
            cursors,
            Arc::clone(&control),
            false,
        );

        submit(&mut producer, b"alpha", meta(5, 11, 100));
        submit(&mut producer, b"beta", meta(4, 12, 200));

        assert!(disk.drain_and_sync().unwrap(), "both batches were written");

        // One drain covers both, and publication reflects the LAST
        // batch — every earlier one is durable by then too.
        assert_eq!(durable.load(), WireSeq::new(12));
        assert_eq!(progress.get().load(Ordering::Acquire), 200);
        assert!(producer.drained(), "slots released after the sync");

        let written = std::fs::read(dir.path().join("test.journal")).unwrap();
        let start = 4096; // entries begin past the header
        assert_eq!(&written[start..start + 9], b"alphabeta");
    }

    /// Nothing published means nothing to do — and, critically, no
    /// cursor movement. A spurious publication here would advance
    /// durability over data that was never written.
    #[test]
    fn an_empty_ring_publishes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let (producer, consumer) = build_journal_write_ring(4);
        let (cursors, progress, durable) = cursors();
        let mut disk = JournalDisk::new(
            segment(dir.path()),
            consumer,
            cursors,
            Arc::new(DiskControl::new()),
            false,
        );

        assert!(!disk.drain_and_sync().unwrap(), "nothing to write");
        assert_eq!(durable.load(), WireSeq::new(0));
        assert_eq!(progress.get().load(Ordering::Acquire), 0);
        assert!(producer.drained());
    }

    /// A rotation is executed and its outcome handed back, and the
    /// fresh segment carries the boundary values the sequencer supplied
    /// — the file half cannot derive those itself.
    #[test]
    fn rotation_rendezvous_returns_the_archived_path() {
        let dir = tempfile::tempdir().unwrap();
        let (mut producer, consumer) = build_journal_write_ring(4);
        let (cursors, _, _) = cursors();
        let control = Arc::new(DiskControl::new());
        let mut disk = JournalDisk::new(
            segment(dir.path()),
            consumer,
            cursors,
            Arc::clone(&control),
            false,
        );

        submit(&mut producer, b"sealed", meta(6, 1, 10));
        disk.drain_and_sync().unwrap();

        control.request_rotation(RotateRequest {
            prepared: None,
            starting_sequence: 99,
            anchor_hash: [3u8; 32],
        });
        assert!(
            control.take_rotation_result().is_none(),
            "no outcome before the disk thread runs it"
        );

        let request = control.take_rotation_request().expect("request is pending");
        disk.rotate(request);

        let archived = control
            .take_rotation_result()
            .expect("outcome is available")
            .expect("rotation succeeded");
        assert!(archived.exists());
        assert!(
            control.take_rotation_result().is_none(),
            "an outcome is delivered once"
        );

        let info = disk.segment.read_header_info().unwrap();
        assert_eq!(info.starting_sequence, 99);
        assert_eq!(info.anchor_hash, [3u8; 32]);
    }

    /// A failed sync must freeze durability: the run loop latches the
    /// error with its cause and returns, and no cursor moves. Anything
    /// else would let an ack cover data the disk rejected.
    #[test]
    fn a_sync_failure_poisons_without_publishing() {
        let dir = tempfile::tempdir().unwrap();
        let (mut producer, consumer) = build_journal_write_ring(4);
        let (cursors, progress, durable) = cursors();
        let control = Arc::new(DiskControl::new());
        let mut disk = JournalDisk::new(
            segment(dir.path()),
            consumer,
            cursors,
            Arc::clone(&control),
            false,
        );
        disk.inject_sync_failure(JournalError::Io(std::io::Error::new(
            std::io::ErrorKind::StorageFull,
            "no space left on device",
        )));

        submit(&mut producer, b"doomed", meta(6, 5, 50));

        // The loop must return rather than spin on a broken journal.
        let _segment = disk.run();

        assert!(control.poisoned(), "the failure must latch");
        let error = control.take_error().expect("cause is preserved");
        assert!(
            error.to_string().contains("no space left on device"),
            "the operator must see the original cause, got: {error}"
        );
        assert!(control.take_error().is_none(), "cause is taken once");

        assert_eq!(durable.load(), WireSeq::new(0), "durability must not move");
        assert_eq!(progress.get().load(Ordering::Acquire), 0);
        assert!(
            !producer.drained(),
            "slots stay held — the batch was never made durable"
        );
    }

    /// A panic must latch poison. Every wait the sequencer performs on
    /// this thread — claiming a slot, draining, awaiting a rotation —
    /// is an untimed spin gated on that flag, so a thread that unwound
    /// without setting it would hang the whole pipeline instead of
    /// failing it.
    ///
    /// This is the unwinding half of the guarantee, which is the half
    /// tests can reach: release builds are `panic = "abort"` and never
    /// get here, because the process is already gone.
    #[test]
    fn a_panicking_disk_thread_poisons_rather_than_stranding_the_sequencer() {
        let dir = tempfile::tempdir().unwrap();
        let (mut producer, consumer) = build_journal_write_ring(4);
        let (cursors, progress, durable) = cursors();
        let control = Arc::new(DiskControl::new());
        let mut disk = JournalDisk::new(
            segment(dir.path()),
            consumer,
            cursors,
            Arc::clone(&control),
            false,
        );
        disk.inject_panic();

        submit(&mut producer, b"unreachable", meta(11, 3, 30));

        // Panic on the thread, exactly as it would reach the sequencer.
        let joined = std::thread::spawn(move || disk.run()).join();
        assert!(joined.is_err(), "the injected panic must unwind the thread");

        assert!(
            control.poisoned(),
            "a panic must latch poison — the sequencer's waits spin on this flag \
             with no timeout"
        );
        let error = control.take_error().expect("a cause must be recorded");
        assert!(
            error.to_string().contains("panicked"),
            "the cause must name the panic, got: {error}"
        );

        // Nothing became durable, so nothing may have been published.
        assert_eq!(durable.load(), WireSeq::new(0));
        assert_eq!(progress.get().load(Ordering::Acquire), 0);
        assert!(!producer.drained());
    }

    /// A clean exit must NOT poison — otherwise every shutdown would
    /// surface a phantom journal failure.
    #[test]
    fn a_clean_exit_leaves_the_journal_unpoisoned() {
        let dir = tempfile::tempdir().unwrap();
        let (_producer, consumer) = build_journal_write_ring(4);
        let (cursors, _, _) = cursors();
        let control = Arc::new(DiskControl::new());
        let disk = JournalDisk::new(
            segment(dir.path()),
            consumer,
            cursors,
            Arc::clone(&control),
            false,
        );

        control.stop();
        let _segment = disk.run();

        assert!(!control.poisoned(), "a stop is not a failure");
        assert!(control.take_error().is_none());
    }

    /// A byte-empty batch — every batch under `no-persist`, and any
    /// query-only batch — is still a full drain: it skips only the
    /// sync. Everything else has to run, because the cursors it carries
    /// are what release the input-ring slots upstream, and the drain is
    /// also what paces the post-rotation dir-fsync retry.
    #[test]
    fn a_byte_empty_batch_skips_only_the_sync() {
        let dir = tempfile::tempdir().unwrap();
        let (mut producer, consumer) = build_journal_write_ring(4);
        let (cursors, progress, _) = cursors();
        let mut disk = JournalDisk::new(
            segment(dir.path()),
            consumer,
            cursors,
            Arc::new(DiskControl::new()),
            false,
        );
        // A sync failure injected but never consumed proves the sync
        // was skipped; the drain still has to do everything else.
        disk.inject_sync_failure(JournalError::Io(std::io::Error::other("must not fire")));

        submit(&mut producer, b"", meta(0, 9, 90));
        assert!(
            disk.drain_and_sync().unwrap(),
            "a cursor-only batch is still work"
        );

        assert!(
            disk.fail_next_sync.is_some(),
            "an empty batch must not reach the sync"
        );
        assert_eq!(
            progress.get().load(Ordering::Acquire),
            90,
            "the cursors ride an empty batch"
        );
        assert!(producer.drained(), "the slot is released");
    }

    /// A rotation that fails is not poison: the file half restores the
    /// live segment, so the sequencer can back off and retry while
    /// journaling continues.
    #[test]
    fn a_failed_rotation_is_reported_without_poisoning() {
        let dir = tempfile::tempdir().unwrap();
        let (_producer, consumer) = build_journal_write_ring(4);
        let (cursors, _, _) = cursors();
        let control = Arc::new(DiskControl::new());
        let mut disk = JournalDisk::new(
            segment(dir.path()),
            consumer,
            cursors,
            Arc::clone(&control),
            false,
        );

        // A prepared segment whose staging file is gone: the rename
        // onto the live path fails after the header write, driving the
        // install-failure path.
        let staging = dir.path().join("ghost.staging");
        let file = std::fs::File::create(&staging).unwrap();
        std::fs::remove_file(&staging).unwrap();
        disk.rotate(RotateRequest {
            prepared: Some(PreparedSegment {
                file,
                path: staging,
                allocated_end: 4096,
            }),
            starting_sequence: 7,
            anchor_hash: [1u8; 32],
        });

        let outcome = control.take_rotation_result().expect("outcome delivered");
        assert!(outcome.is_err(), "the rotation must report failure");
        assert!(
            !control.poisoned(),
            "a failed rotation must not poison the journal"
        );
        assert!(
            dir.path().join("test.journal").exists(),
            "the live segment must be restored"
        );
    }

    /// The thread exits on the stop flag, after draining what was
    /// already published — a shutdown must not strand a submitted
    /// batch.
    #[test]
    fn stop_drains_before_exiting() {
        let dir = tempfile::tempdir().unwrap();
        let (mut producer, consumer) = build_journal_write_ring(4);
        let (cursors, progress, durable) = cursors();
        let control = Arc::new(DiskControl::new());
        let disk = JournalDisk::new(
            segment(dir.path()),
            consumer,
            cursors,
            Arc::clone(&control),
            false,
        );

        submit(&mut producer, b"last words", meta(10, 42, 4242));
        control.stop();

        let segment = disk.run();

        assert_eq!(durable.load(), WireSeq::new(42), "final batch is durable");
        assert_eq!(progress.get().load(Ordering::Acquire), 4242);
        assert!(producer.drained());
        assert!(
            segment.valid_end() > 4096,
            "the segment came back with the batch written"
        );
    }
}
