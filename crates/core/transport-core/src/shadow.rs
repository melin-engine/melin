//! Shadow snapshot stage — replays journal events on a cloned application to
//! produce periodic snapshots without blocking the hot path.
//!
//! Generic over `A: Application`. The shadow consumer is gated on the journal
//! stage (sees only fsynced events), so snapshots are always consistent with
//! durable state. The chain hash is read from a seqlock published by the
//! journal stage after each fsync batch.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use tracing::{error, info, warn};

use crate::cursors::WireSeq;
use crate::dispatch::dispatch;
use crate::lineage::Lineage;
use crate::pipeline::{FsyncState, InputSlot};
use crate::snapshot;
use melin_app::amortized_timer::AmortizedTimer;
use melin_app::{Application, ApplyCtx};
use melin_pipeline::ring;
use melin_pipeline::seqlock::SeqLockReader;
use melin_pipeline::wait::WaitStrategy;

/// Maximum events consumed per batch. Matches the journal stage batch size
/// for consistent throughput characteristics.
const SHADOW_BATCH_SIZE: usize = 4096;

/// Run the shadow snapshot stage.
///
/// Consumes events from the input ring (gated on journal fsync), replays them
/// on a cloned application, and saves periodic snapshots. The snapshot's
/// journal sequence and chain hash are read from the journal stage's
/// [`FsyncState`] seqlock — only saved when the shadow's ring cursor
/// matches the fsync boundary, guaranteeing the triple (app state,
/// journal_seq, chain_hash) is self-consistent.
///
/// `genesis_entries` is the lineage's genesis length from the journal
/// header, stamped into every snapshot so a boot from the snapshot alone
/// still knows it. Constant for the stage's life: it is fixed when the
/// lineage begins.
///
/// `starting_wire_seq` is the wire sequence the journal allocates to the
/// first event the shadow will consume (the journal writer's next
/// sequence when the pipeline is built). The shadow counts from it the
/// way the journal allocates, so a lineage event it folds is recorded at
/// its own entry's sequence (see
/// [`LineagePolicy::after`](crate::lineage::LineagePolicy::after)).
#[allow(clippy::too_many_arguments)]
pub fn run<A: Application>(
    mut consumer: ring::Consumer<InputSlot<A::Event>>,
    mut app: A,
    snapshot_path: PathBuf,
    snapshot_interval: Duration,
    fsync_state: SeqLockReader<FsyncState>,
    shutdown: &AtomicBool,
    wait: WaitStrategy,
    initial_lineage: Lineage,
    starting_wire_seq: u64,
    genesis_entries: Option<u64>,
) {
    // Scratch buffer for app methods that require a reports Vec.
    // Cleared after each call — shadow discards all reports.
    let mut reports: Vec<A::Report> = Vec::with_capacity(64);

    // Batch buffer for consume_batch — stack-allocated InputSlot array would
    // be too large, so use a Vec that's allocated once and reused.
    let mut batch: Vec<InputSlot<A::Event>> = Vec::with_capacity(SHADOW_BATCH_SIZE);
    batch.resize_with(SHADOW_BATCH_SIZE, InputSlot::default);

    // Snapshot-interval check on the busy-spin hot loop. A naive
    // `last_snapshot.elapsed() >= snapshot_interval` per iteration ran
    // `__vdso_clock_gettime` at loop frequency, which showed up in
    // perf profiles as ~10 % of this process's total cycles landing on
    // `clock_gettime` — for a check that fires at most once every
    // 50 min (default `snapshot_interval_ms=3_000_000`). `AmortizedTimer`
    // defers the clock read to roughly 1 Hz, collapsing the overhead
    // to a single `AND` + predictable branch per iteration.
    let mut snapshot_timer = AmortizedTimer::new();
    // Whether a save is due, set by the timer and kept until a save lands
    // or fails. Its per-iteration check is a field load, so the timer's
    // amortization above still holds while no save is due.
    let mut schedule = SnapshotSchedule::default();
    let mut waiter = wait.waiter();
    // Track whether any events have been consumed. Prevents snapshotting
    // empty state before the first event arrives.
    let mut has_events = false;
    // Highest event timestamp the shadow's scheduler has drained against.
    // See `dispatch::dispatch` for the per-event drain rationale.
    let mut last_drain_ns: u64 = 0;
    // The lineage (fencing epoch and ack policy) as of the shadow's
    // consumed position. Seeded from the lineage the live pipeline started
    // at, because the shadow only sees events published *after* boot — a
    // lineage event already folded into the recovered app state never
    // crosses the ring. Advanced by replaying lineage events and stamped
    // into each snapshot so a snapshot-bootstrapped node restores it.
    let mut shadow_lineage = initial_lineage;
    // The wire sequence of the next journaled event, shadowing the
    // journal's allocator: one per event it journals, decided by the
    // allocator's own rule (`JournalEvent::takes_wire_seq`). A primary's
    // input slots carry no sequence (the journal stage allocates it), so
    // the shadow counts.
    let mut next_wire_seq = starting_wire_seq;

    loop {
        if shutdown.load(Ordering::Relaxed) {
            info!("shadow stage shutting down");
            return;
        }

        let count = consumer.consume_batch(&mut batch, SHADOW_BATCH_SIZE);
        if count > 0 {
            waiter.reset();
            has_events = true;
        }

        // Replay each event on the shadow app. last_drain_ns lives
        // outside the loop so the per-event drain stays monotonic across
        // batches.
        for slot in &batch[..count] {
            // The shadow reads the input ring before the journal stage
            // drops queries, so it sees them. The matching stage answers
            // a query through `Application::query`, which changes
            // nothing — no clock advance, no state change — so the
            // shadow skips it, whatever its slot's timestamp says, or its
            // snapshot would hold state the primary never had.
            if slot.event.is_query() {
                continue;
            }
            let wire_seq = next_wire_seq;
            if slot.event.takes_wire_seq() {
                next_wire_seq = next_wire_seq.saturating_add(1);
                // A replica's slots carry the primary's sequence: the
                // count must agree with it.
                debug_assert!(
                    slot.sequence == 0 || slot.sequence == wire_seq,
                    "shadow wire seq {wire_seq} drifted from the slot's {}",
                    slot.sequence
                );
            }
            // The shadow produces no output: the matching stage replies
            // for every event.
            dispatch(
                &mut app,
                slot.event,
                &ApplyCtx {
                    now_ns: slot.timestamp_ns,
                    key_hash: slot.key_hash,
                },
                &mut last_drain_ns,
                |event| shadow_lineage.observe(WireSeq::new(wire_seq), event),
                &mut reports,
            );
            reports.clear();
        }

        // Check if a snapshot is due, and attempt it while it is: after a
        // batch, and when idle too — events may have been consumed before
        // the interval elapsed, and no more may arrive. Idle is also where
        // a deferred save usually lands: the shadow has drained the ring
        // and the journal publishes the fsync boundary that matches it.
        // `has_events` keeps an empty state from being snapshotted before
        // the first event. After a batch the waiter was just reset, so
        // `spinning()` is true and the timer stays amortized.
        if has_events
            && snapshot_timer
                .tick(snapshot_interval, waiter.spinning())
                .is_some()
        {
            tick_schedule(&mut schedule, &snapshot_path);
        }
        if schedule.should_attempt() {
            attempt_save::<A>(
                &mut schedule,
                &app,
                &consumer,
                &fsync_state,
                &snapshot_path,
                shadow_lineage,
                genesis_entries,
            );
        }

        if count == 0 {
            waiter.idle();
        }
    }
}

/// When the shadow attempts a snapshot save: the decision alone, with no
/// clock and no I/O, so every transition is unit-tested.
///
/// A timer tick makes a save due. While it is due, the shadow attempts it
/// on every consumed batch and every idle pass, so a save deferred
/// because the shadow was not at an fsync boundary lands at the next one
/// rather than an interval later. A save that succeeds clears it; so does
/// one that fails, whose next attempt waits for the next tick, so that a
/// persistent I/O error costs one attempt (and one log line) an interval
/// rather than one per loop iteration.
#[derive(Debug, Default)]
struct SnapshotSchedule {
    due: bool,
    /// Attempts at the save now due, the deferred ones included. `u64`:
    /// a save is attempted at loop frequency (millions a second) while
    /// it waits, so a `u32` could wrap within an interval of a workload
    /// that never aligns; the count saturates rather than wraps all the
    /// same, since it only reaches the logs.
    attempts: u64,
}

impl SnapshotSchedule {
    /// The snapshot interval elapsed: a save is due. Returns the attempts
    /// already made when the previous tick's save is still due, i.e. it
    /// has been deferred for a whole interval; the save stays due and its
    /// count runs on.
    fn on_tick(&mut self) -> Option<u64> {
        let overdue = self.due.then_some(self.attempts);
        self.due = true;
        overdue
    }

    /// Whether to attempt the save on this iteration. Read on every
    /// iteration of the shadow's busy loop, so a field load and nothing
    /// more.
    #[inline(always)]
    fn should_attempt(&self) -> bool {
        self.due
    }

    /// An attempt found the shadow off the fsync boundary: the save stays
    /// due.
    fn on_deferred(&mut self) {
        self.attempts = self.attempts.saturating_add(1);
    }

    /// The save landed. Returns the attempts it took, this one included.
    fn on_saved(&mut self) -> u64 {
        self.finish()
    }

    /// The save failed. It is no longer due: the next attempt waits for
    /// the next tick. Returns the attempts it took, this one included.
    fn on_failed(&mut self) -> u64 {
        self.finish()
    }

    fn finish(&mut self) -> u64 {
        let attempts = self.attempts.saturating_add(1);
        self.due = false;
        self.attempts = 0;
        attempts
    }
}

/// A timer tick: make a save due, and say so when the one before it is
/// still waiting a whole interval later — the shadow has not met an fsync
/// boundary since, and the node has no fresh snapshot to recover from.
/// Logged at most once an interval.
fn tick_schedule(schedule: &mut SnapshotSchedule, path: &std::path::Path) {
    if let Some(attempts) = schedule.on_tick() {
        warn!(
            attempts,
            path = %path.display(),
            "shadow snapshot deferred for a whole interval: the shadow has not \
             reached a journal fsync boundary since it fell due; still retrying"
        );
    }
}

/// Attempt the due save and log what came of it.
fn attempt_save<A: Application>(
    schedule: &mut SnapshotSchedule,
    app: &A,
    consumer: &ring::Consumer<InputSlot<A::Event>>,
    fsync_state: &SeqLockReader<FsyncState>,
    path: &std::path::Path,
    lineage: Lineage,
    genesis_entries: Option<u64>,
) {
    // Exhaustive: a new outcome must be decided here, not fall through.
    match try_save_snapshot::<A>(app, consumer, fsync_state, path, lineage, genesis_entries) {
        SaveOutcome::Deferred => schedule.on_deferred(),
        SaveOutcome::Saved {
            journal_seq,
            bytes,
            elapsed,
        } => {
            let attempts = schedule.on_saved();
            info!(
                journal_seq = journal_seq.get(),
                bytes,
                // Fractional: a small state saves in well under a
                // millisecond, which a whole count would show as 0.
                elapsed_ms = elapsed.as_secs_f64() * 1000.0,
                attempts,
                path = %path.display(),
                "shadow snapshot saved"
            );
        }
        SaveOutcome::Failed { journal_seq, error } => {
            let attempts = schedule.on_failed();
            error!(
                journal_seq = journal_seq.get(),
                error = %error,
                attempts,
                path = %path.display(),
                "shadow snapshot failed"
            );
        }
    }
}

/// What came of one attempt at a save.
enum SaveOutcome {
    /// The snapshot at `journal_seq` is on disk: `bytes` long, written in
    /// `elapsed` (serialization, write, syncs and renames).
    Saved {
        journal_seq: WireSeq,
        bytes: u64,
        elapsed: Duration,
    },
    /// The shadow was not at the journal's last fsync boundary; nothing
    /// was written.
    Deferred,
    /// The save of the snapshot at `journal_seq` failed.
    Failed {
        journal_seq: WireSeq,
        error: snapshot::SnapshotError,
    },
}

/// Save a shadow snapshot if the shadow's ring cursor is aligned with
/// the journal stage's last fsync boundary. When aligned, journal_seq
/// and chain_hash from [`FsyncState`] correspond exactly to the
/// shadow's app state.
///
/// When not aligned (shadow mid-batch or journal fsynced again since
/// shadow's last consume), the snapshot is deferred: the schedule
/// retries on the next batch or idle pass. That check is what runs on
/// every iteration while a save is due, so it is kept to a lock-free
/// seqlock read of [`FsyncState`] (an L1 hit until the journal publishes
/// again) and one comparison; the save itself is out of line.
#[inline]
fn try_save_snapshot<A: Application>(
    app: &A,
    consumer: &ring::Consumer<InputSlot<A::Event>>,
    fsync_state: &SeqLockReader<FsyncState>,
    path: &std::path::Path,
    lineage: Lineage,
    genesis_entries: Option<u64>,
) -> SaveOutcome {
    let state = fsync_state.load();
    // Both ring-index space — compare the raw positions.
    if state.input_ring_seq.get() != consumer.next_read() {
        return SaveOutcome::Deferred;
    }
    save_aligned::<A>(app, &state, path, lineage, genesis_entries)
}

/// The save proper, at an fsync boundary. Cold and out of line: it runs
/// once an interval, and keeps the per-iteration check above small.
#[cold]
#[inline(never)]
fn save_aligned<A: Application>(
    app: &A,
    state: &FsyncState,
    path: &std::path::Path,
    lineage: Lineage,
    genesis_entries: Option<u64>,
) -> SaveOutcome {
    let started = Instant::now();
    match snapshot::save::<A>(
        app,
        state.journal_seq,
        state.chain_hash,
        lineage,
        genesis_entries,
        path,
    ) {
        Ok(bytes) => SaveOutcome::Saved {
            journal_seq: state.journal_seq,
            bytes,
            elapsed: started.elapsed(),
        },
        Err(error) => SaveOutcome::Failed {
            journal_seq: state.journal_seq,
            error,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cursors::RingPos;
    use crate::pipeline::InputSlot;
    use crate::test_support::{TestApp, TestEvent};
    use melin_journal::JournalEvent;
    use melin_pipeline::ring::DisruptorBuilder;
    use melin_pipeline::seqlock;
    use std::sync::{Arc, Mutex};
    use std::thread::JoinHandle;

    // --- SnapshotSchedule ---

    #[test]
    fn schedule_attempts_nothing_before_the_first_tick() {
        let schedule = SnapshotSchedule::default();
        assert!(!schedule.should_attempt());
    }

    #[test]
    fn schedule_keeps_a_deferred_save_due() {
        let mut schedule = SnapshotSchedule::default();
        assert_eq!(schedule.on_tick(), None, "nothing was due before");
        assert!(schedule.should_attempt());
        schedule.on_deferred();
        assert!(schedule.should_attempt(), "a deferred save stays due");
        schedule.on_deferred();
        assert!(schedule.should_attempt());
    }

    #[test]
    fn schedule_saves_at_the_aligned_attempt_after_deferred_ones() {
        let mut schedule = SnapshotSchedule::default();
        schedule.on_tick();
        schedule.on_deferred();
        schedule.on_deferred();
        assert_eq!(schedule.on_saved(), 3, "two deferred, then the save");
        assert!(!schedule.should_attempt(), "a landed save is no longer due");
    }

    #[test]
    fn schedule_counts_one_attempt_for_a_save_that_lands_first_time() {
        let mut schedule = SnapshotSchedule::default();
        schedule.on_tick();
        assert_eq!(schedule.on_saved(), 1);
    }

    #[test]
    fn schedule_clears_a_failed_save_until_the_next_tick() {
        let mut schedule = SnapshotSchedule::default();
        schedule.on_tick();
        schedule.on_deferred();
        assert_eq!(schedule.on_failed(), 2);
        assert!(
            !schedule.should_attempt(),
            "a failed save is not retried before the next tick"
        );
        assert_eq!(
            schedule.on_tick(),
            None,
            "a failed save is not reported as overdue at the next tick"
        );
        assert!(schedule.should_attempt());
        assert_eq!(
            schedule.on_failed(),
            1,
            "the count restarts after a failure"
        );
    }

    #[test]
    fn schedule_resets_attempts_after_a_save() {
        let mut schedule = SnapshotSchedule::default();
        schedule.on_tick();
        schedule.on_deferred();
        schedule.on_deferred();
        schedule.on_saved();
        schedule.on_tick();
        assert_eq!(schedule.on_saved(), 1, "the next save counts from zero");
    }

    #[test]
    fn schedule_reports_a_save_still_due_at_the_next_tick() {
        let mut schedule = SnapshotSchedule::default();
        schedule.on_tick();
        schedule.on_deferred();
        schedule.on_deferred();
        assert_eq!(
            schedule.on_tick(),
            Some(2),
            "deferred for a whole interval: overdue, with its attempts so far"
        );
        assert!(schedule.should_attempt(), "still due");
        schedule.on_deferred();
        assert_eq!(schedule.on_saved(), 4, "the count runs on across the tick");
    }

    // --- The shadow's save, threaded ---

    /// The shadow thread's log, captured. A subscriber of the shadow's
    /// thread alone, so tests running beside it neither see nor disturb
    /// it. Not `melin_test_node::logs` nor `stall_guards`' sink: the one
    /// crate is built on this one, the other is another crate's
    /// integration test.
    #[derive(Clone, Default)]
    struct Logs(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for Logs {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl Logs {
        /// The captured lines carrying `message`.
        fn lines(&self, message: &str) -> Vec<String> {
            String::from_utf8(self.0.lock().unwrap().clone())
                .unwrap()
                .lines()
                .filter(|line| line.contains(message))
                .map(str::to_owned)
                .collect()
        }
    }

    /// The value of `field=` on a log line.
    fn field<'a>(line: &'a str, field: &str) -> &'a str {
        let start = line
            .find(&format!(" {field}="))
            .unwrap_or_else(|| panic!("no {field}= in {line:?}"))
            + field.len()
            + 2;
        line[start..].split_whitespace().next().unwrap()
    }

    /// A shadow stage on its own thread, its log captured, saving to
    /// `path` every `interval`.
    fn spawn_shadow(
        consumer: ring::Consumer<InputSlot<TestEvent>>,
        path: PathBuf,
        interval: Duration,
        fsync_state: SeqLockReader<FsyncState>,
        shutdown: &Arc<AtomicBool>,
    ) -> (JoinHandle<()>, Logs) {
        let logs = Logs::default();
        let writer = logs.clone();
        let shutdown = Arc::clone(shutdown);
        let handle = std::thread::Builder::new()
            .name("test-shadow-logged".into())
            .spawn(move || {
                let subscriber = tracing_subscriber::fmt()
                    .with_ansi(false)
                    .with_max_level(tracing::Level::INFO)
                    .with_writer(move || writer.clone())
                    .finish();
                tracing::subscriber::with_default(subscriber, || {
                    run(
                        consumer,
                        TestApp::new(),
                        path,
                        interval,
                        fsync_state,
                        &shutdown,
                        WaitStrategy::SpinThenYield,
                        Lineage::GENESIS,
                        1,       // starting_wire_seq
                        Some(0), // genesis_entries
                    );
                });
            })
            .unwrap();
        (handle, logs)
    }

    fn add(n: u64) -> InputSlot<TestEvent> {
        InputSlot {
            connection_id: 0,
            key_hash: 0,
            sequence: 0,
            timestamp_ns: 0,
            event: JournalEvent::App(TestEvent::Add(n)),
            publish_ts: Default::default(),
            recv_ts: Default::default(),
        }
    }

    /// The journal's fsync boundary after `events` events, one each.
    fn boundary(events: u64, chain_hash: [u8; 32]) -> FsyncState {
        FsyncState {
            journal_seq: WireSeq::new(events),
            chain_hash,
            input_ring_seq: RingPos::new(events),
        }
    }

    fn wait_for(path: &std::path::Path, deadline: Instant) -> bool {
        while !path.exists() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        path.exists()
    }

    /// A save deferred at its tick lands as soon as the shadow meets the
    /// fsync boundary, not an interval later. The boundary is behind the
    /// shadow at the first tick and catches up well before the second;
    /// the snapshot must exist before the second tick could have fired,
    /// and its line must count more than one attempt, which proves the
    /// first tick found the shadow off the boundary (so a late first
    /// tick cannot pass the test by saving at once).
    #[test]
    fn deferred_save_lands_at_the_next_boundary_not_the_next_tick() {
        const INTERVAL: Duration = Duration::from_secs(4);
        let (mut producer, mut consumers) = DisruptorBuilder::<InputSlot<TestEvent>>::new(64)
            .add_consumer()
            .build(WaitStrategy::SpinThenYield);
        // Before the shadow's timer starts, so every tick is later than
        // the same multiple of the interval from here.
        let started = Instant::now();
        // The boundary before the event: misaligned once it is consumed.
        let (mut fsync_writer, fsync_state) = seqlock::split(boundary(0, [0; 32]));
        let shutdown = Arc::new(AtomicBool::new(false));
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.snapshot");
        let (handle, logs) = spawn_shadow(
            consumers.pop().unwrap(),
            path.clone(),
            INTERVAL,
            fsync_state,
            &shutdown,
        );
        producer.publish(add(9));

        // Past the first tick, with a margin for a late one.
        std::thread::sleep((INTERVAL + INTERVAL * 3 / 8).saturating_sub(started.elapsed()));
        assert!(!path.exists(), "no save off the fsync boundary");
        fsync_writer.store(boundary(1, [0x77; 32]));

        // Before the second tick, which is no earlier than this.
        let saved = wait_for(&path, started + INTERVAL * 2 - Duration::from_millis(100));
        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
        assert!(saved, "the deferred save must land before the next tick");

        let (restored, header) = snapshot::load::<TestApp>(&path).unwrap();
        assert_eq!(restored.total, 9);
        assert_eq!(header.sequence, 1);
        assert_eq!(header.chain_hash, [0x77; 32]);
        let saved = logs.lines("shadow snapshot saved");
        assert_eq!(saved.len(), 1, "{saved:?}");
        let attempts: u64 = field(&saved[0], "attempts").parse().unwrap();
        assert!(attempts > 1, "the first attempt was deferred: {saved:?}");
        let bytes: u64 = field(&saved[0], "bytes").parse().unwrap();
        assert_eq!(bytes, std::fs::metadata(&path).unwrap().len());
        let elapsed_ms: f64 = field(&saved[0], "elapsed_ms").parse().unwrap();
        assert!(elapsed_ms >= 0.0);
        assert_eq!(field(&saved[0], "journal_seq"), "1");
    }

    /// A save still deferred a whole interval after it fell due is
    /// reported, once a tick rather than once an attempt, and keeps being
    /// retried until it lands.
    #[test]
    fn save_deferred_for_an_interval_warns_once_a_tick_and_still_lands() {
        const INTERVAL: Duration = Duration::from_millis(50);
        let (mut producer, mut consumers) = DisruptorBuilder::<InputSlot<TestEvent>>::new(64)
            .add_consumer()
            .build(WaitStrategy::SpinThenYield);
        let started = Instant::now();
        let (mut fsync_writer, fsync_state) = seqlock::split(boundary(0, [0; 32]));
        let shutdown = Arc::new(AtomicBool::new(false));
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.snapshot");
        let (handle, logs) = spawn_shadow(
            consumers.pop().unwrap(),
            path.clone(),
            INTERVAL,
            fsync_state,
            &shutdown,
        );
        producer.publish(add(4));

        // Wait for the overdue report, then align.
        let deadline = Instant::now() + Duration::from_secs(10);
        while logs.lines("deferred for a whole interval").is_empty() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(!path.exists(), "no save off the fsync boundary");
        // More events while the save is due: it lands at the boundary
        // that covers them, with their state.
        producer.publish(add(5));
        fsync_writer.store(boundary(2, [0x33; 32]));
        let saved = wait_for(&path, Instant::now() + Duration::from_secs(10));
        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
        let ran = started.elapsed();
        assert!(saved, "the save must land once aligned");
        let (restored, header) = snapshot::load::<TestApp>(&path).unwrap();
        assert_eq!(restored.total, 9);
        assert_eq!(header.sequence, 2);

        let overdue = logs.lines("deferred for a whole interval");
        assert!(!overdue.is_empty(), "an overdue save is reported");
        // At most one report a tick: a report per attempt would be
        // thousands.
        let ticks = (ran.as_millis() / INTERVAL.as_millis()) as usize + 1;
        assert!(
            overdue.len() <= ticks,
            "{} reports in {ran:?}",
            overdue.len()
        );
        assert!(overdue[0].contains("WARN"), "{overdue:?}");
        let saved = logs.lines("shadow snapshot saved");
        assert!(!saved.is_empty());
        let attempts: u64 = field(&saved[0], "attempts").parse().unwrap();
        let reported: u64 = field(&overdue[overdue.len() - 1], "attempts")
            .parse()
            .unwrap();
        assert!(
            attempts > reported,
            "the save counts every attempt since it fell due: {saved:?} {overdue:?}"
        );
    }

    /// A save that fails is not retried at loop frequency: one attempt,
    /// and one error line, a tick.
    #[test]
    fn failed_save_is_retried_once_a_tick() {
        const INTERVAL: Duration = Duration::from_millis(50);
        let (mut producer, mut consumers) = DisruptorBuilder::<InputSlot<TestEvent>>::new(64)
            .add_consumer()
            .build(WaitStrategy::SpinThenYield);
        let started = Instant::now();
        let (_fsync_writer, fsync_state) = seqlock::split(boundary(1, [0; 32]));
        let shutdown = Arc::new(AtomicBool::new(false));
        let dir = tempfile::tempdir().unwrap();
        // A directory that does not exist: every save fails.
        let path = dir.path().join("missing").join("test.snapshot");
        let (handle, logs) = spawn_shadow(
            consumers.pop().unwrap(),
            path.clone(),
            INTERVAL,
            fsync_state,
            &shutdown,
        );
        producer.publish(add(1));

        // Several intervals' worth of failures.
        let deadline = Instant::now() + Duration::from_secs(10);
        while logs.lines("shadow snapshot failed").len() < 3 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
        let ran = started.elapsed();

        let failed = logs.lines("shadow snapshot failed");
        assert!(failed.len() >= 3, "{failed:?}");
        let ticks = (ran.as_millis() / INTERVAL.as_millis()) as usize + 1;
        assert!(
            failed.len() <= ticks,
            "{} failures in {ran:?}",
            failed.len()
        );
        for line in &failed {
            assert!(line.contains("ERROR"), "{line}");
            assert_eq!(field(line, "attempts"), "1", "{line}");
        }
        assert!(logs.lines("shadow snapshot saved").is_empty());
        assert!(
            logs.lines("deferred for a whole interval").is_empty(),
            "a failed save is not a deferred one"
        );
    }

    #[test]
    fn shadow_shutdown_exits_promptly() {
        let (_, mut consumers) = DisruptorBuilder::<InputSlot<TestEvent>>::new(64)
            .add_consumer()
            .build(WaitStrategy::SpinThenYield);
        let consumer = consumers.pop().unwrap();

        let app = TestApp::new();
        let (_writer, fsync_state) = seqlock::split(FsyncState::default());
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown2 = Arc::clone(&shutdown);

        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("test.snapshot");

        let handle = std::thread::Builder::new()
            .name("test-shadow".into())
            .spawn(move || {
                run(
                    consumer,
                    app,
                    snap_path,
                    Duration::from_secs(3600), // won't fire during test
                    fsync_state,
                    &shutdown2,
                    WaitStrategy::SpinThenYield,
                    Lineage::GENESIS,
                    1,       // starting_wire_seq
                    Some(0), // genesis_entries
                );
            })
            .unwrap();

        // Give it a moment to start, then signal shutdown.
        std::thread::sleep(Duration::from_millis(50));
        shutdown.store(true, Ordering::Relaxed);

        // Should exit promptly.
        handle.join().unwrap();
    }

    #[test]
    fn shadow_takes_snapshot_at_interval() {
        let (mut producer, mut consumers) = DisruptorBuilder::<InputSlot<TestEvent>>::new(64)
            .add_consumer()
            .build(WaitStrategy::SpinThenYield);
        let consumer = consumers.pop().unwrap();

        let app = TestApp::new();
        // Pre-set input_ring_seq = 2 (the ring cursor after consuming
        // both events below). Shadow only saves when aligned.
        let (_writer, fsync_state) = seqlock::split(FsyncState {
            journal_seq: WireSeq::new(3),
            chain_hash: [0xAB; 32],
            input_ring_seq: RingPos::new(2),
        });
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown2 = Arc::clone(&shutdown);

        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("test.snapshot");
        let snap_path2 = snap_path.clone();

        // Very short interval so the snapshot fires quickly.
        let handle = std::thread::Builder::new()
            .name("test-shadow".into())
            .spawn(move || {
                run(
                    consumer,
                    app,
                    snap_path2,
                    Duration::from_millis(50),
                    fsync_state,
                    &shutdown2,
                    WaitStrategy::SpinThenYield,
                    Lineage::GENESIS,
                    1,       // starting_wire_seq
                    Some(0), // genesis_entries
                );
            })
            .unwrap();

        // Publish both events before the interval elapses so the snapshot
        // captures both adds. The idle-check fires the snapshot after the
        // 50ms interval even without new events arriving.
        producer.publish(InputSlot {
            connection_id: 0,
            key_hash: 0,
            sequence: 0,
            timestamp_ns: 0,
            event: JournalEvent::App(TestEvent::Add(1000)),
            publish_ts: Default::default(),
            recv_ts: Default::default(),
        });
        producer.publish(InputSlot {
            connection_id: 0,
            key_hash: 0,
            sequence: 0,
            timestamp_ns: 0,
            event: JournalEvent::App(TestEvent::Add(500)),
            publish_ts: Default::default(),
            recv_ts: Default::default(),
        });

        // Wait for the snapshot to be written (idle-check triggers it
        // after the 50ms interval elapses). Generous deadline because
        // nextest runs many tests concurrently and the shadow worker can
        // be starved on a busy machine — the test still completes
        // quickly in the common case via the tight poll.
        let deadline = Instant::now() + Duration::from_secs(10);
        while !snap_path.exists() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();

        // Verify the snapshot file was created and is loadable, and that
        // both adds are reflected in the restored app's running total.
        assert!(snap_path.exists(), "snapshot file should exist");
        let (restored, header) = snapshot::load::<TestApp>(&snap_path).unwrap();
        assert_eq!(header.chain_hash, [0xAB; 32]); // chain hash from the seqlock
        assert_eq!(restored.total, 1500);
    }

    /// The shadow sees queries, which the matching stage answers through
    /// `Application::query` without changing anything — not per-key
    /// state, not the clock. A query slot must therefore leave the
    /// shadow's state untouched too, whatever timestamp it carries, or a
    /// restore would hold state the primary never had: a clock run ahead,
    /// per-key state the application never saw a write for.
    #[test]
    fn query_slot_changes_nothing() {
        const KEY: u64 = 0xDEAD_BEEF;
        let (mut producer, mut consumers) = DisruptorBuilder::<InputSlot<TestEvent>>::new(64)
            .add_consumer()
            .build(WaitStrategy::SpinThenYield);
        let consumer = consumers.pop().unwrap();

        // The ring cursor after both slots below; the shadow saves only
        // when aligned with it.
        let (_writer, fsync_state) = seqlock::split(FsyncState {
            journal_seq: WireSeq::new(1),
            chain_hash: [0; 32],
            input_ring_seq: RingPos::new(2),
        });
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown2 = Arc::clone(&shutdown);

        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("test.snapshot");
        let snap_path2 = snap_path.clone();

        let handle = std::thread::Builder::new()
            .name("test-shadow".into())
            .spawn(move || {
                run(
                    consumer,
                    TestApp::new(),
                    snap_path2,
                    Duration::from_millis(50),
                    fsync_state,
                    &shutdown2,
                    WaitStrategy::SpinThenYield,
                    Lineage::GENESIS,
                    1,       // starting_wire_seq
                    Some(0), // genesis_entries
                );
            })
            .unwrap();

        // A query under a client key, with a timestamp that would drain
        // the clock, then a write no client submitted.
        producer.publish(InputSlot {
            connection_id: 1,
            key_hash: KEY,
            sequence: 0,
            timestamp_ns: 1_000,
            event: JournalEvent::App(TestEvent::Query),
            publish_ts: Default::default(),
            recv_ts: Default::default(),
        });
        producer.publish(InputSlot {
            connection_id: 0,
            key_hash: 0,
            sequence: 0,
            timestamp_ns: 0,
            event: JournalEvent::App(TestEvent::Add(7)),
            publish_ts: Default::default(),
            recv_ts: Default::default(),
        });

        let deadline = Instant::now() + Duration::from_secs(10);
        while !snap_path.exists() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();

        let (restored, _) = snapshot::load::<TestApp>(&snap_path).unwrap();
        assert_eq!(restored.total, 7, "the write after the query is applied");
        assert_eq!(restored.ticks, 0, "a query must not advance the clock");
        assert!(
            restored.per_key_total.is_empty(),
            "a query must not reach apply under its key"
        );
    }

    #[test]
    fn has_events_gate_prevents_idle_snapshot() {
        // The has_events gate (set true after the first consumed batch)
        // is what stops the shadow from writing an empty-state snapshot
        // immediately at startup. Operationally this matters: a snapshot
        // dropped before the first event arrives could overwrite the
        // last valid one on disk during a quick restart, leaving the
        // operator with a zero-state recovery target.
        let (_producer, mut consumers) = DisruptorBuilder::<InputSlot<TestEvent>>::new(64)
            .add_consumer()
            .build(WaitStrategy::SpinThenYield);
        let consumer = consumers.pop().unwrap();

        let app = TestApp::new();
        let (_writer, fsync_state) = seqlock::split(FsyncState::default());
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown2 = Arc::clone(&shutdown);

        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("test.snapshot");
        let snap_path2 = snap_path.clone();

        let handle = std::thread::Builder::new()
            .name("test-shadow-idle".into())
            .spawn(move || {
                run(
                    consumer,
                    app,
                    snap_path2,
                    Duration::from_millis(20),
                    fsync_state,
                    &shutdown2,
                    WaitStrategy::SpinThenYield,
                    Lineage::GENESIS,
                    1,       // starting_wire_seq
                    Some(0), // genesis_entries
                );
            })
            .unwrap();

        // Sleep well past several intervals — if has_events were
        // misordered, the idle-tick branch would already have written
        // a snapshot file.
        std::thread::sleep(Duration::from_millis(200));
        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();

        assert!(
            !snap_path.exists(),
            "idle shadow must not write a snapshot before any event arrives"
        );
    }

    #[test]
    fn snapshot_picks_up_updated_chain_hash() {
        // chain_hash_lock is loaded on each save_snapshot call, not
        // cached at startup. A mid-run hash update (the journal stage
        // publishes after every fsync batch) must be reflected in the
        // very next snapshot the shadow writes.
        let (mut producer, mut consumers) = DisruptorBuilder::<InputSlot<TestEvent>>::new(64)
            .add_consumer()
            .build(WaitStrategy::SpinThenYield);
        let consumer = consumers.pop().unwrap();

        let app = TestApp::new();
        let (mut fsync_state_writer, fsync_state) = seqlock::split(FsyncState {
            journal_seq: WireSeq::new(2),
            chain_hash: [0x11; 32],
            input_ring_seq: RingPos::new(1),
        });
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown2 = Arc::clone(&shutdown);

        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("test.snapshot");
        let snap_path2 = snap_path.clone();

        let handle = std::thread::Builder::new()
            .name("test-shadow-chain".into())
            .spawn(move || {
                run(
                    consumer,
                    app,
                    snap_path2,
                    Duration::from_millis(30),
                    fsync_state,
                    &shutdown2,
                    WaitStrategy::SpinThenYield,
                    Lineage::GENESIS,
                    1,       // starting_wire_seq
                    Some(0), // genesis_entries
                );
            })
            .unwrap();

        // Phase 1: publish one event, wait for first snapshot.
        producer.publish(InputSlot {
            connection_id: 0,
            key_hash: 0,
            sequence: 0,
            timestamp_ns: 0,
            event: JournalEvent::App(TestEvent::Add(1)),
            publish_ts: Default::default(),
            recv_ts: Default::default(),
        });

        let deadline = Instant::now() + Duration::from_secs(10);
        while !snap_path.exists() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(snap_path.exists(), "first snapshot must be written");
        let (_, initial) = snapshot::load::<TestApp>(&snap_path).unwrap();
        assert_eq!(
            initial.chain_hash, [0x11; 32],
            "first snapshot has initial hash"
        );

        // Phase 2: update FsyncState (new hash + advanced ring cursor),
        // drive another event, wait for the second snapshot.
        fsync_state_writer.store(FsyncState {
            journal_seq: WireSeq::new(3),
            chain_hash: [0x22; 32],
            input_ring_seq: RingPos::new(2),
        });
        producer.publish(InputSlot {
            connection_id: 0,
            key_hash: 0,
            sequence: 0,
            timestamp_ns: 0,
            event: JournalEvent::App(TestEvent::Add(1)),
            publish_ts: Default::default(),
            recv_ts: Default::default(),
        });

        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Ok((_, header)) = snapshot::load::<TestApp>(&snap_path)
                && header.chain_hash == [0x22; 32]
            {
                break;
            }
            if Instant::now() >= deadline {
                panic!("snapshot did not pick up updated chain hash within deadline");
            }
            std::thread::sleep(Duration::from_millis(5));
        }

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn multi_batch_replay_accumulates_into_snapshot() {
        // End-to-end: events arriving across multiple consume_batch
        // iterations all reach the shadow app and the eventual snapshot
        // reflects the running total. last_drain_ns persists across
        // batches in the run loop, but the test focuses on the
        // accumulated app-event side — Add semantics make the sum
        // load-bearing and easy to assert.
        let (mut producer, mut consumers) = DisruptorBuilder::<InputSlot<TestEvent>>::new(64)
            .add_consumer()
            .build(WaitStrategy::SpinThenYield);
        let consumer = consumers.pop().unwrap();

        let app = TestApp::new();
        // 5 events total (10,20,30,40,50). Pre-set input_ring_seq = 5
        // so the alignment check passes once shadow consumes all.
        let (_writer, fsync_state) = seqlock::split(FsyncState {
            journal_seq: WireSeq::new(6),
            chain_hash: [0xCD; 32],
            input_ring_seq: RingPos::new(5),
        });
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown2 = Arc::clone(&shutdown);

        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("test.snapshot");
        let snap_path2 = snap_path.clone();

        let handle = std::thread::Builder::new()
            .name("test-shadow-batches".into())
            .spawn(move || {
                run(
                    consumer,
                    app,
                    snap_path2,
                    Duration::from_millis(20),
                    fsync_state,
                    &shutdown2,
                    WaitStrategy::SpinThenYield,
                    Lineage::GENESIS,
                    1,       // starting_wire_seq
                    Some(0), // genesis_entries
                );
            })
            .unwrap();

        // Publish three small batches with brief gaps so the consumer
        // sees them as separate consume_batch iterations rather than one
        // big drain.
        for batch in [&[10u64, 20u64][..], &[30u64, 40u64][..], &[50u64][..]] {
            for &n in batch {
                producer.publish(InputSlot {
                    connection_id: 0,
                    key_hash: 0,
                    sequence: 0,
                    timestamp_ns: 0,
                    event: JournalEvent::App(TestEvent::Add(n)),
                    publish_ts: Default::default(),
                    recv_ts: Default::default(),
                });
            }
            std::thread::sleep(Duration::from_millis(5));
        }

        // Wait for a snapshot whose restored total reflects all batches
        // (10+20+30+40+50 = 150). Polling the total handles the race
        // between snapshot emission and the next event arriving.
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Ok((restored, _)) = snapshot::load::<TestApp>(&snap_path)
                && restored.total == 150
            {
                break;
            }
            if Instant::now() >= deadline {
                let observed = snapshot::load::<TestApp>(&snap_path)
                    .map(|(a, _)| a.total)
                    .unwrap_or(u64::MAX);
                panic!("snapshot did not reach total=150 (observed={observed})");
            }
            std::thread::sleep(Duration::from_millis(5));
        }

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }
}
