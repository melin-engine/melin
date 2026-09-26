//! Acceptance tests for strictly increasing sequencer time: the clock
//! scenarios the call-trace property test (`call_trace_tests`) does not
//! generate, each driven through a live pipeline by the stamping producer
//! on the clock's manual time source.
//!
//! Each scenario asserts the stamps the clock issued, then that the
//! application receives the same calls, `tick` with its time included, on
//! every path: the live run, recovery from genesis, and a restore from the
//! shadow's snapshot at every anchor followed by replay of the rest.

#![cfg(all(test, not(feature = "no-persist")))]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use melin_journal::replication::REPLICATION_RING_CAPACITY;
use melin_journal::{JournalEntry, JournalError, JournalEvent, JournalReader, TimeFloor};
use melin_pipeline::padding::Sequence;
use melin_pipeline::ring;
use melin_pipeline::wait::WaitStrategy;

use crate::call_trace_tests::{
    Call, RecordingApp, Step, Writer, chain_after_each_entry, first_difference, shadow_snapshots,
    wait_for,
};
use crate::clock::{
    ClockControl, DEFAULT_JUMP_LIMIT, LEAD_WARNING, SequencerClock, StampingProducer,
};
use crate::cursors::DurableWireSeqCursor;
use crate::fence::FenceState;
use crate::journaled_app::JournaledApp;
use crate::pipeline::{
    InputSlot, MAX_JOURNAL_BATCH, OutputSlot, StageWaits, build_pipeline_with_replication,
};
use crate::test_support::ManualClocks;
use crate::trace::mono_trace_ns;

const T0: u64 = 1_700_000_000_000_000_000;
const MS: u64 = 1_000_000;
const HOUR: u64 = 3_600_000 * MS;
/// The one client every write comes from.
const KEY: u64 = 7;

/// A primary: a live pipeline whose single producer stamps through a
/// sequencer clock reading `ManualClocks`, seeded at its journal's floor
/// as `run_as_primary` seeds it.
struct Node {
    producer: StampingProducer<Step, ManualClocks>,
    control: Arc<ClockControl>,
    journal: PathBuf,
    rotate: Arc<AtomicBool>,
    durable: DurableWireSeqCursor,
    journal_progress: Arc<Sequence>,
    matching_progress: Arc<Sequence>,
    /// The durable sequence the node started from.
    start_seq: u64,
    /// Slots published this run, every one journaled (no queries here).
    published: u64,
    /// Entries surely past the last boundary: counted from this run's
    /// start or its last rotation, not counting the tick a rotation
    /// straddles. The journal stage skips a rotation of an empty live
    /// segment, so [`Node::rotate`] needs one.
    past_boundary: u64,
    /// Archives expected once every requested rotation has happened.
    archives: usize,
    next_id: u64,
    shutdown: Arc<AtomicBool>,
    t_journal: JoinHandle<Result<Writer, JournalError>>,
    t_matching: JoinHandle<RecordingApp>,
    /// Held so the matching stage's output ring keeps a live consumer.
    _outputs: Vec<ring::Consumer<OutputSlot<(), melin_app::NoQuery>>>,
}

impl Node {
    fn start(journal: &Path, app: RecordingApp, writer: Writer, clocks: ManualClocks) -> Self {
        let floor = writer.last_timestamp();
        let archives = melin_journal::segment::list_archives(journal)
            .unwrap()
            .len();
        let pipeline = build_pipeline_with_replication(
            app,
            writer,
            Duration::ZERO,
            Arc::new(AtomicU64::new(0)),
            false,
            MAX_JOURNAL_BATCH,
            REPLICATION_RING_CAPACITY,
            StageWaits::uniform(WaitStrategy::SpinThenYield),
            false,
            false,
            Arc::new(FenceState::new(0)),
        );
        let rotate = Arc::new(AtomicBool::new(false));
        let mut journal_stage = pipeline.journal_stage;
        journal_stage.set_rotation(0, Some(Arc::clone(&rotate)));
        let durable = pipeline.cursors.durable_wire_seq();
        let start_seq = durable.load().get();
        let control = Arc::new(ClockControl::new());
        let clock = SequencerClock::new(clocks, DEFAULT_JUMP_LIMIT, floor, Arc::clone(&control));

        let shutdown = Arc::new(AtomicBool::new(false));
        let t_journal = {
            let shutdown = Arc::clone(&shutdown);
            std::thread::spawn(move || journal_stage.run(&shutdown))
        };
        let t_matching = {
            let shutdown = Arc::clone(&shutdown);
            let matching_stage = pipeline.matching_stage;
            std::thread::spawn(move || matching_stage.run(&shutdown))
        };
        Self {
            producer: StampingProducer::new(pipeline.input_producer, clock),
            control,
            journal: journal.to_path_buf(),
            rotate,
            durable,
            journal_progress: pipeline.cursors.journal_ring_arc(),
            matching_progress: pipeline.cursors.matching_ring_arc(),
            start_seq,
            published: 0,
            past_boundary: 0,
            archives,
            next_id: start_seq + 1,
            shutdown,
            t_journal,
            t_matching,
            _outputs: pipeline.output_consumers,
        }
    }

    /// One batch of `n` client writes, stamped from one clock reading as
    /// the reader stamps a receive.
    fn write(&mut self, n: u64) {
        let reading = self.producer.read_clock();
        let mut batch = self.producer.batch(reading);
        for _ in 0..n {
            let id = self.next_id;
            self.next_id += 1;
            batch
                .try_push_with(|slot| {
                    slot.connection_id = 1;
                    slot.key_hash = KEY;
                    slot.sequence = 0;
                    slot.event = JournalEvent::App(Step::Write(id));
                    slot.publish_ts = mono_trace_ns();
                    slot.recv_ts = mono_trace_ns();
                })
                .expect("the ring has room");
        }
        batch.commit();
        self.published += n;
        self.past_boundary += n;
    }

    fn tick(&mut self) {
        self.producer.try_publish_tick().expect("the ring has room");
        self.published += 1;
        self.past_boundary += 1;
    }

    /// A promotion's epoch bump, published as `run_as_primary` publishes
    /// it.
    fn epoch_bump(&mut self, epoch: u64) {
        self.producer
            .publish_internal(JournalEvent::EpochBump { epoch });
        self.published += 1;
        self.past_boundary += 1;
    }

    /// Rotate the journal, across a tick. The journal stage acts on a
    /// request after the next batch it submits and skips an empty live
    /// segment, so the request is made once it has taken every published
    /// slot (the live segment must hold an entry), and the tick after it
    /// may land on either side of the boundary.
    fn rotate(&mut self) {
        assert!(
            self.past_boundary > 0,
            "journal an entry before rotating: the journal stage skips an empty live segment"
        );
        let published = self.published;
        let progress = Arc::clone(&self.journal_progress);
        wait_for("the journal stage to take every published slot", || {
            progress.get().load(Ordering::Acquire) == published
        });
        self.rotate.store(true, Ordering::Release);
        self.tick();
        self.past_boundary = 0;
        self.archives += 1;
        let (journal, archives) = (self.journal.clone(), self.archives);
        wait_for("a rotation", || {
            melin_journal::segment::list_archives(&journal)
                .unwrap()
                .len()
                == archives
        });
    }

    /// Wait for every published slot to be durable and applied, stop the
    /// stages, and return the application. A journal stage that stopped on
    /// its own (it refused an entry) fails the test with its error.
    ///
    /// The writer the journal stage hands back must report the last stamp
    /// as its floor: it is the writer a promotion takes over when a
    /// replica's pipeline is torn down, and the new primary's clock is
    /// seeded from it.
    fn stop(self) -> RecordingApp {
        let target = self.start_seq + self.published;
        wait_for("the journal to reach the last entry", || {
            self.durable.load().get() == target || self.t_journal.is_finished()
        });
        if self.durable.load().get() != target {
            let error = self.t_journal.join().unwrap().err();
            panic!("the journal stage stopped short of the last entry: {error:?}");
        }
        wait_for("the matching stage to apply the last entry", || {
            self.matching_progress.get().load(Ordering::Acquire) == self.published
        });
        self.shutdown.store(true, Ordering::Relaxed);
        let writer = self
            .t_journal
            .join()
            .unwrap()
            .expect("the journal stage stops cleanly");
        // Every stamp the clock issued here was journaled (no queries).
        assert_eq!(
            writer.last_timestamp(),
            self.producer.last_stamp(),
            "the writer handed back at teardown lost the time floor"
        );
        self.t_matching.join().unwrap()
    }
}

/// Every entry in the journal, archives first, then the live segment.
fn entries(journal: &Path) -> Vec<JournalEntry<Step>> {
    let mut segments: Vec<PathBuf> = melin_journal::segment::list_archives(journal)
        .unwrap()
        .into_iter()
        .map(|(_, path)| path)
        .collect();
    segments.push(journal.to_path_buf());
    let mut out = Vec::new();
    for segment in segments {
        let mut reader = JournalReader::<Step>::open(&segment).unwrap();
        while let Some(entry) = reader.next_entry().unwrap() {
            out.push(entry);
        }
    }
    out
}

/// The journaled stamps, in order.
fn stamps(journal: &Path) -> Vec<u64> {
    entries(journal)
        .iter()
        .map(|entry| entry.timestamp.as_ns())
        .collect()
}

/// The journal as the slots the shadow stage would have consumed.
fn slots_from_journal(journal: &Path) -> Vec<InputSlot<Step>> {
    entries(journal)
        .into_iter()
        .map(|entry| InputSlot {
            connection_id: u64::from(entry.key_hash != 0),
            key_hash: entry.key_hash,
            sequence: 0,
            timestamp: entry.timestamp,
            event: entry.event,
            publish_ts: mono_trace_ns(),
            recv_ts: mono_trace_ns(),
        })
        .collect()
}

/// The shadow's snapshot at every anchor of `journal`, saved under
/// `scratch` (which must not exist yet).
fn anchors(scratch: &Path, journal: &Path) -> Vec<PathBuf> {
    std::fs::create_dir(scratch).unwrap();
    shadow_snapshots(
        scratch,
        &slots_from_journal(journal),
        &chain_after_each_entry(journal),
    )
}

/// The application receives `live`'s calls on recovery from genesis and
/// on a restore at every anchor. `scratch` must not exist yet.
#[track_caller]
fn assert_every_path_agrees(scratch: &Path, journal: &Path, live: &[Call]) {
    let (recovered, _writer) =
        JournaledApp::<RecordingApp, Writer>::recover(RecordingApp::default(), journal)
            .unwrap()
            .into_parts();
    assert!(
        recovered.calls == live,
        "recovery from genesis: {}",
        first_difference(live, &recovered.calls)
    );
    for (i, snapshot) in anchors(scratch, journal).iter().enumerate() {
        let (restored, _writer) =
            JournaledApp::<RecordingApp, Writer>::recover_from_snapshot(snapshot, journal)
                .unwrap()
                .into_parts();
        assert!(
            restored.calls == live,
            "restore at anchor {}: {}",
            i + 1,
            first_difference(live, &restored.calls)
        );
    }
}

fn recovered(journal: &Path) -> (RecordingApp, Writer) {
    JournaledApp::<RecordingApp, Writer>::recover(RecordingApp::default(), journal)
        .unwrap()
        .into_parts()
}

/// A node restarted on a wall clock an hour behind its journal holds
/// time one nanosecond per event past the journal's last stamp, and
/// follows the wall clock again once it passes that stamp.
#[test]
fn a_restart_across_a_clock_step_back_holds_time_until_the_wall_clock_catches_up() {
    let dir = tempfile::tempdir().unwrap();
    let journal = dir.path().join("node.journal");
    let clocks = ManualClocks::at(T0);
    let mut node = Node::start(
        &journal,
        RecordingApp::default(),
        Writer::create(&journal).unwrap(),
        clocks.clone(),
    );
    node.write(3);
    clocks.advance(MS);
    node.tick();
    clocks.advance(MS);
    node.write(2);
    node.stop();
    let before = stamps(&journal);
    assert_eq!(
        before,
        [T0, T0 + 1, T0 + 2, T0 + MS, T0 + 2 * MS, T0 + 2 * MS + 1]
    );
    let last = T0 + 2 * MS + 1;

    let clocks = ManualClocks::at(last - HOUR);
    let (app, writer) = recovered(&journal);
    let mut node = Node::start(&journal, app, writer, clocks.clone());
    assert_eq!(
        node.control.offset_ns(),
        (HOUR + 1) as i64,
        "seeded an hour behind the journal's last stamp: held"
    );
    node.write(2);
    clocks.advance(MS);
    node.tick();
    clocks.advance(HOUR);
    node.write(1);
    let live = node.stop().calls;

    assert_eq!(
        stamps(&journal)[before.len()..],
        [last + 1, last + 2, last + 3, last + MS],
        "held past the journal's last stamp, then the wall clock once past it"
    );
    assert_every_path_agrees(&dir.path().join("anchors"), &journal, &live);
}

/// A snapshot taken while time is held records a stamp behind the
/// journal's tail. A node booted from it seeds its clock from the journal
/// it replays, not from the snapshot, so it stamps past the tail.
#[test]
fn a_node_booted_from_a_snapshot_taken_while_time_was_held_stamps_past_the_journal() {
    let dir = tempfile::tempdir().unwrap();
    let journal = dir.path().join("node.journal");
    let clocks = ManualClocks::at(T0);
    let mut node = Node::start(
        &journal,
        RecordingApp::default(),
        Writer::create(&journal).unwrap(),
        clocks.clone(),
    );
    node.write(2);
    clocks.step_wall(T0 - HOUR);
    node.write(3);
    clocks.advance(MS);
    node.tick();
    node.write(2);
    node.stop();
    assert_eq!(
        stamps(&journal),
        [T0, T0 + 1, T0 + 2, T0 + 3, T0 + 4, T0 + 5, T0 + 6, T0 + 7],
        "held from the step back on"
    );

    // The anchor at entry 4 sits inside the held run, four entries short
    // of the tail. The wall clock is still behind when the node boots.
    let snapshot = &anchors(&dir.path().join("window"), &journal)[3];
    assert_eq!(
        crate::snapshot::load::<RecordingApp>(snapshot)
            .unwrap()
            .floor,
        TimeFloor::After(melin_app::SequencerTime::from_ns(T0 + 3))
    );
    let (app, writer) =
        JournaledApp::<RecordingApp, Writer>::recover_from_snapshot(snapshot, &journal)
            .unwrap()
            .into_parts();
    assert_eq!(
        writer.last_timestamp().as_ns(),
        T0 + 7,
        "the journal's tail"
    );
    clocks.advance(MS);
    let mut node = Node::start(&journal, app, writer, clocks.clone());
    node.write(2);
    let live = node.stop().calls;

    assert_eq!(stamps(&journal)[8..], [T0 + 8, T0 + 9]);
    assert_every_path_agrees(&dir.path().join("anchors"), &journal, &live);
}

/// A promoted replica whose clock runs behind the old primary's seeds
/// past the journal it mirrors, warns (its offset past the threshold), and
/// holds time until its own clock passes the last stamp.
#[test]
fn a_failover_to_a_slower_clock_holds_time_past_the_old_primarys_last_stamp() {
    let dir = tempfile::tempdir().unwrap();
    let primary_journal = dir.path().join("primary.journal");
    let clocks = ManualClocks::at(T0);
    let mut primary = Node::start(
        &primary_journal,
        RecordingApp::default(),
        Writer::create(&primary_journal).unwrap(),
        clocks.clone(),
    );
    primary.write(3);
    clocks.advance(MS);
    primary.tick();
    primary.write(2);
    primary.stop();
    let last = *stamps(&primary_journal).last().unwrap();

    // The replica's journal is a byte copy of the primary's.
    let journal = dir.path().join("replica.journal");
    std::fs::copy(&primary_journal, &journal).unwrap();
    let skew = 300 * MS;
    let clocks = ManualClocks::at(last - skew);
    let (app, writer) = recovered(&journal);
    let mut node = Node::start(&journal, app, writer, clocks.clone());
    assert_eq!(node.control.offset_ns(), (skew + 1) as i64);
    assert!(
        Duration::from_nanos(skew) > LEAD_WARNING,
        "past the seeding warning's threshold"
    );
    node.epoch_bump(1);
    node.write(2);
    clocks.advance(skew + 100 * MS);
    node.write(1);
    let live = node.stop().calls;

    assert_eq!(
        stamps(&journal)[6..],
        [last + 1, last + 2, last + 3, last + 100 * MS],
        "the epoch bump and writes held past the old primary's last stamp"
    );
    assert_every_path_agrees(&dir.path().join("anchors"), &journal, &live);
}

/// A forward jump past the limit is refused: time keeps flowing at the
/// boot clock's rate from where it was. `CLOCK-ACCEPT` then moves the
/// clock to the wall clock, which it follows from there.
#[test]
fn a_refused_forward_jump_keeps_time_flowing_until_the_operator_accepts_it() {
    let dir = tempfile::tempdir().unwrap();
    let journal = dir.path().join("node.journal");
    let clocks = ManualClocks::at(T0);
    let mut node = Node::start(
        &journal,
        RecordingApp::default(),
        Writer::create(&journal).unwrap(),
        clocks.clone(),
    );
    node.write(1);
    clocks.step_wall(T0 + HOUR);
    clocks.advance(MS);
    node.write(1);
    clocks.advance(MS);
    node.tick();
    assert_eq!(node.control.jumps_refused(), 1);
    assert_eq!(node.control.offset_ns(), -(HOUR as i64), "an hour behind");

    assert!(node.control.request_accept());
    clocks.advance(MS);
    node.write(1);
    assert_eq!(node.control.offset_ns(), 0, "in step with the wall clock");
    clocks.advance(MS);
    node.tick();
    let live = node.stop().calls;

    assert_eq!(
        stamps(&journal),
        [
            T0,
            T0 + MS,
            T0 + 2 * MS,
            T0 + HOUR + 3 * MS,
            T0 + HOUR + 4 * MS
        ],
        "the boot clock's rate while refused, the wall clock once accepted"
    );
    assert_every_path_agrees(&dir.path().join("anchors"), &journal, &live);
}

/// A replica resynced from a snapshot at a segment boundary receives a
/// seed with no entries, so its journal opens at the snapshot's stamp.
/// Promoted before any entry arrives, on a clock an hour behind, it stamps
/// past that stamp, and a restore from the transferred snapshot replays
/// what it ran.
#[test]
fn a_replica_resynced_at_a_segment_boundary_is_seeded_from_the_snapshots_stamp() {
    let dir = tempfile::tempdir().unwrap();
    let primary_journal = dir.path().join("primary.journal");
    let clocks = ManualClocks::at(T0);
    let mut primary = Node::start(
        &primary_journal,
        RecordingApp::default(),
        Writer::create(&primary_journal).unwrap(),
        clocks.clone(),
    );
    primary.write(3);
    primary.rotate();
    clocks.advance(MS);
    primary.write(2);
    primary.stop();

    // The snapshot the primary serves, at the last entry of the archived
    // segment, and the seed it sends with it: the live segment's prefix
    // through that entry, its header alone.
    let boundary = melin_journal::segment::read_header_info(&primary_journal)
        .unwrap()
        .starting_sequence
        - 1;
    let boundary_stamp = entries(&primary_journal)[boundary as usize - 1].timestamp;
    let snapshot = anchors(&dir.path().join("primary-anchors"), &primary_journal)
        [boundary as usize - 1]
        .clone();
    let seed = melin_journal::segment::read_segment_prefix(&primary_journal, boundary)
        .unwrap()
        .unwrap();

    // The replica installs both as its resync does.
    let journal = dir.path().join("replica.journal");
    std::fs::write(&journal, &seed).unwrap();
    let seed_len = seed.len() as u64;
    assert_eq!(
        melin_journal::segment::verify_segment_prefix(&journal, boundary, seed_len).unwrap(),
        None,
        "a boundary seed holds no entries"
    );
    let loaded = crate::snapshot::load::<RecordingApp>(&snapshot).unwrap();
    assert_eq!(loaded.floor, TimeFloor::After(boundary_stamp));
    let writer = Writer::open_append(&journal, boundary, seed_len, loaded.floor).unwrap();
    assert_eq!(writer.last_timestamp(), boundary_stamp);

    let clocks = ManualClocks::at(boundary_stamp.as_ns() - HOUR);
    let mut node = Node::start(&journal, loaded.app, writer, clocks.clone());
    node.epoch_bump(1);
    node.write(2);
    let live = node.stop().calls;

    let b = boundary_stamp.as_ns();
    assert_eq!(stamps(&journal), [b + 1, b + 2, b + 3]);
    let (restored, _writer) =
        JournaledApp::<RecordingApp, Writer>::recover_from_snapshot(&snapshot, &journal)
            .unwrap()
            .into_parts();
    assert!(
        restored.calls == live,
        "restore from the transferred snapshot: {}",
        first_difference(&live, &restored.calls)
    );
}
