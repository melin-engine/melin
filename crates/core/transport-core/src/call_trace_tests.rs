//! The runtime's half of determinism: the calls it makes into an
//! application depend on the journal alone.
//!
//! A recording application logs every call it receives. A generated
//! history (application writes and queries, journaled ticks, epoch
//! bumps, rotations at random points) runs through the live pipeline,
//! then through
//! recovery from genesis, then through a restore from a snapshot at every
//! anchor followed by replay of the rest. Each must hand the application
//! the calls the live run did. The snapshots are the shadow stage's own,
//! taken as it reaches each anchor, so the restored prefix is what
//! production would restore.
//!
//! This is the runtime's half only. That identical calls build identical
//! state is the application's half, and depends on the application.
//!
//! The recording application and the replay checks are shared with the
//! clock's acceptance tests (`clock_acceptance_tests`), which drive the
//! same comparison through clock scenarios the generator does not produce.

#![cfg(all(test, not(feature = "no-persist")))]

use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use melin_app::{
    AppEvent, Application, ApplyCtx, CodecError, QueryCtx, RejectReason, SequencerTime,
};
use melin_journal::replication::REPLICATION_RING_CAPACITY;
use melin_journal::{BufferedWriter, JournalEvent, JournalReader};
use melin_pipeline::ring;
use melin_pipeline::wait::WaitStrategy;
use proptest::prelude::*;

use crate::cursors::{RingPos, WireSeq};
use crate::fence::FenceState;
use crate::journaled_app::JournaledApp;
use crate::pipeline::{
    FsyncState, InputSlot, MAX_JOURNAL_BATCH, StageWaits, build_pipeline_with_replication,
};
use crate::trace::mono_trace_ns;

/// Deadline for any single wait on a pipeline thread. Generous: the
/// suite runs many busy-spinning stages at once.
const WAIT_TIMEOUT: Duration = Duration::from_secs(30);

/// An application event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Step {
    /// A write, carrying an identifier unique within the history, which
    /// the recording names `apply` calls by (`ApplyCtx` carries no
    /// sequence).
    Write(u64),
    /// A query: seen by the live matching stage and the shadow, never
    /// journaled.
    Query,
}

impl Step {
    const TAG_WRITE: u8 = 1;
    const TAG_QUERY: u8 = 2;
}

impl AppEvent for Step {
    const MAX_ENCODED_SIZE: usize = 1 + 8;

    fn encoded_size(&self) -> usize {
        match self {
            Step::Write(_) => 1 + 8,
            Step::Query => 1,
        }
    }

    fn encode(&self, buf: &mut [u8]) -> usize {
        match self {
            Step::Write(id) => {
                buf[0] = Self::TAG_WRITE;
                buf[1..9].copy_from_slice(&id.to_le_bytes());
                9
            }
            Step::Query => {
                buf[0] = Self::TAG_QUERY;
                1
            }
        }
    }

    fn decode(buf: &[u8]) -> Result<Self, CodecError> {
        match buf.split_first() {
            Some((&Self::TAG_WRITE, id)) => {
                let bytes: [u8; 8] = id.try_into().map_err(|_| CodecError::Truncated)?;
                Ok(Step::Write(u64::from_le_bytes(bytes)))
            }
            Some((&Self::TAG_QUERY, _)) => Ok(Step::Query),
            Some((&tag, _)) => Err(CodecError::UnknownTag(tag)),
            None => Err(CodecError::Truncated),
        }
    }

    fn is_query(&self) -> bool {
        matches!(self, Step::Query)
    }
}

/// One call the runtime made into the application.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Call {
    Tick { now_ns: u64 },
    Apply { id: u64, now_ns: u64, key_hash: u64 },
}

/// Records every call it receives, and snapshots the record, so a
/// restored instance carries the calls made before its anchor.
#[derive(Debug, Default)]
pub(crate) struct RecordingApp {
    // Vec: calls are only appended and compared in order.
    pub(crate) calls: Vec<Call>,
}

impl RecordingApp {
    const TAG_TICK: u8 = 0;
    const TAG_APPLY: u8 = 1;
}

impl Application for RecordingApp {
    type Event = Step;
    type Report = ();
    type QueryResponse = melin_app::NoQuery;
    type Sizing = ();

    const APP_VERSION: u16 = 1;

    fn apply(&mut self, event: Step, ctx: &ApplyCtx, _out: &mut Vec<()>) {
        let Step::Write(id) = event else {
            // Panics the stage thread, which fails the test at its join.
            unreachable!("the runtime never hands a query to apply");
        };
        self.calls.push(Call::Apply {
            id,
            now_ns: ctx.now.as_ns(),
            key_hash: ctx.key_hash,
        });
    }

    /// Answered live only, from `&self`: a query cannot be recorded,
    /// which is right, since no journal could reproduce it.
    fn query(&self, _event: Step, _ctx: &QueryCtx) -> Option<melin_app::NoQuery> {
        None
    }

    fn tick(&mut self, now: SequencerTime, _out: &mut Vec<()>) {
        self.calls.push(Call::Tick {
            now_ns: now.as_ns(),
        });
    }

    fn build_reject(_event: &Step, _reason: RejectReason) {}

    fn snapshot<W: Write>(&self, w: &mut W) -> io::Result<()> {
        // u64: a length prefix that no history in this test approaches.
        w.write_all(&(self.calls.len() as u64).to_le_bytes())?;
        for call in &self.calls {
            match *call {
                Call::Tick { now_ns } => {
                    w.write_all(&[Self::TAG_TICK])?;
                    w.write_all(&now_ns.to_le_bytes())?;
                }
                Call::Apply {
                    id,
                    now_ns,
                    key_hash,
                } => {
                    w.write_all(&[Self::TAG_APPLY])?;
                    w.write_all(&id.to_le_bytes())?;
                    w.write_all(&now_ns.to_le_bytes())?;
                    w.write_all(&key_hash.to_le_bytes())?;
                }
            }
        }
        Ok(())
    }

    fn restore<R: Read>(r: &mut R) -> io::Result<Self> {
        fn read_u64<R: Read>(r: &mut R) -> io::Result<u64> {
            let mut buf = [0u8; 8];
            r.read_exact(&mut buf)?;
            Ok(u64::from_le_bytes(buf))
        }
        let len = read_u64(r)?;
        let mut calls = Vec::new();
        for _ in 0..len {
            let mut tag = [0u8; 1];
            r.read_exact(&mut tag)?;
            calls.push(match tag[0] {
                Self::TAG_TICK => Call::Tick {
                    now_ns: read_u64(r)?,
                },
                Self::TAG_APPLY => Call::Apply {
                    id: read_u64(r)?,
                    now_ns: read_u64(r)?,
                    key_hash: read_u64(r)?,
                },
                other => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unknown call tag {other}"),
                    ));
                }
            });
        }
        Ok(Self { calls })
    }
}

/// One step of a generated history.
#[derive(Debug, Clone, Copy)]
enum Op {
    /// A client write under one of a few keys.
    App { key_hash: u64 },
    /// A client query: published, answered live, never journaled. Where
    /// one sits, the journal's sequence and the input ring's position
    /// part, and the shadow's snapshot anchor pairs the two.
    Query { key_hash: u64 },
    /// A journaled clock tick.
    Tick,
    /// A promotion's epoch bump.
    EpochBump,
    /// Ask the journal stage to rotate after the next batch.
    Rotate,
}

/// An entry to journal, with the gap between its stamp and the previous
/// entry's. Never zero: the sequencer clock issues strictly increasing
/// stamps. Before it did, one reader batch shared a stamp, and a snapshot
/// taken inside such a run made a restored node call `tick` once more
/// than the live one; this test, with zero gaps allowed, shrank to that.
#[derive(Debug, Clone, Copy)]
struct Planned {
    op: Op,
    stamp_gap_ns: u64,
}

fn planned() -> impl Strategy<Value = Planned> {
    let op = prop_oneof![
        6 => (0u64..3).prop_map(|key_hash| Op::App { key_hash }),
        2 => (0u64..3).prop_map(|key_hash| Op::Query { key_hash }),
        2 => Just(Op::Tick),
        1 => Just(Op::EpochBump),
        1 => Just(Op::Rotate),
    ];
    (op, 1u64..=3).prop_map(|(op, stamp_gap_ns)| Planned { op, stamp_gap_ns })
}

pub(crate) type Writer = BufferedWriter<Step>;

/// Turn a plan into the input slots a primary's producer would publish,
/// and the indices (into those slots) after which a rotation is asked
/// for.
fn build_slots(plan: &[Planned]) -> (Vec<InputSlot<Step>>, Vec<usize>) {
    let mut slots = Vec::new();
    let mut rotate_after = Vec::new();
    let mut stamp = 1_000_000_000u64;
    let mut epoch = 0u64;
    for (id, p) in plan.iter().enumerate() {
        // A rotation's gap is simply added to the next entry's.
        stamp += p.stamp_gap_ns;
        let (connection_id, key_hash, event) = match p.op {
            Op::Rotate => {
                rotate_after.push(slots.len());
                continue;
            }
            Op::App { key_hash } => (1, key_hash, JournalEvent::App(Step::Write(id as u64))),
            Op::Query { key_hash } => (1, key_hash, JournalEvent::App(Step::Query)),
            Op::Tick => (0, 0, JournalEvent::Tick),
            Op::EpochBump => {
                epoch += 1;
                (0, 0, JournalEvent::EpochBump { epoch })
            }
        };
        // Queries are published unstamped, as the stamping producer
        // publishes them.
        let timestamp = if event.is_query() {
            SequencerTime::default()
        } else {
            SequencerTime::from_ns(stamp)
        };
        slots.push(InputSlot {
            connection_id,
            key_hash,
            sequence: 0,
            timestamp,
            event,
            publish_ts: mono_trace_ns(),
            recv_ts: mono_trace_ns(),
        });
    }
    (slots, rotate_after)
}

/// How many of `slots` the journal records: all but the queries.
fn journaled_count(slots: &[InputSlot<Step>]) -> u64 {
    slots.iter().filter(|s| !s.event.is_query()).count() as u64
}

/// Poll `done` until it holds, panicking with `what` past the deadline.
#[track_caller]
pub(crate) fn wait_for(what: &str, mut done: impl FnMut() -> bool) {
    let start = Instant::now();
    while !done() {
        assert!(
            start.elapsed() < WAIT_TIMEOUT,
            "timed out waiting for {what}"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// Run the slots through the live pipeline, rotating where asked, and
/// return the calls the matching stage made.
fn run_live(journal: &Path, slots: &[InputSlot<Step>], rotate_after: &[usize]) -> Vec<Call> {
    let writer = Writer::create(journal).unwrap();
    let mut pipeline = build_pipeline_with_replication(
        RecordingApp::default(),
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
    pipeline
        .journal_stage
        .set_rotation(0, Some(Arc::clone(&rotate)));
    let durable = pipeline.cursors.durable_wire_seq();
    let journal_progress = pipeline.cursors.journal_ring_arc();
    let mut producer = pipeline.input_producer;
    let journal_stage = pipeline.journal_stage;
    let matching_stage = pipeline.matching_stage;

    let shutdown = Arc::new(AtomicBool::new(false));
    let t_journal = {
        let shutdown = Arc::clone(&shutdown);
        std::thread::spawn(move || journal_stage.run(&shutdown))
    };
    let t_matching = {
        let shutdown = Arc::clone(&shutdown);
        std::thread::spawn(move || matching_stage.run(&shutdown))
    };

    // The journal stage acts on a rotation request after the next batch it
    // submits, and skips it when the live segment holds no entry, as it
    // does in production. So a rotation is asked for only once the stage
    // has taken everything published so far, and only when a journaled
    // slot surely lies past the last boundary: then it happens, whichever
    // batch acts on it, and its archive confirms it before the next one.
    // The slot published right after a request may fall on either side of
    // its boundary, so it is not counted as past it.
    let mut rotations = 0usize;
    let mut journaled_past_boundary = 0u64;
    for (i, slot) in slots.iter().enumerate() {
        let request = rotate_after.contains(&i) && journaled_past_boundary > 0;
        if request {
            wait_for("the journal stage to take every published slot", || {
                journal_progress.get().load(Ordering::Acquire) == i as u64
            });
            rotate.store(true, Ordering::Release);
        }
        producer.publish(*slot);
        if request {
            rotations += 1;
            wait_for("a rotation", || {
                melin_journal::segment::list_archives(journal)
                    .unwrap()
                    .len()
                    == rotations
            });
            journaled_past_boundary = 0;
        } else if !slot.event.is_query() {
            journaled_past_boundary += 1;
        }
    }
    let journaled = journaled_count(slots);
    wait_for("the journal to reach the last slot", || {
        durable.load().get() == journaled
    });

    shutdown.store(true, Ordering::Relaxed);
    t_journal.join().unwrap().unwrap();
    t_matching.join().unwrap().calls
}

/// The chain hash after each journaled entry, indexed by sequence
/// (index 0 unused), walking the archives and then the live segment.
pub(crate) fn chain_after_each_entry(journal: &Path) -> Vec<[u8; 32]> {
    let mut chain = vec![[0u8; 32]];
    let mut segments: Vec<PathBuf> = melin_journal::segment::list_archives(journal)
        .unwrap()
        .into_iter()
        .map(|(_, path)| path)
        .collect();
    segments.push(journal.to_path_buf());
    for segment in segments {
        let mut reader = JournalReader::<Step>::open(&segment).unwrap();
        while let Some(entry) = reader.next_entry().unwrap() {
            assert_eq!(entry.sequence as usize, chain.len(), "entries in order");
            chain.push(reader.chain_hash().unwrap_or([0u8; 32]));
        }
    }
    chain
}

/// Feed the slots to the shadow stage one at a time, with the fsync
/// state at each anchor, and save the snapshot it writes there to
/// `anchor-<k>.snapshot`. Returns their paths, indexed by anchor.
///
/// An anchor is each journaled slot. Its fsync state pairs the slot's
/// journal sequence with its input-ring position, which counts every
/// query before it, as the journal stage publishes them: it counts a
/// query toward its ring progress without journaling it. An anchor on a
/// query itself would repeat the previous anchor's sequence and state,
/// indistinguishable from it, so a query only moves the ring position of
/// the anchors after it.
pub(crate) fn shadow_snapshots(
    dir: &Path,
    slots: &[InputSlot<Step>],
    chain: &[[u8; 32]],
) -> Vec<PathBuf> {
    let (mut producer, mut consumers) = ring::DisruptorBuilder::<InputSlot<Step>>::new(64)
        .add_consumer()
        .build(WaitStrategy::SpinThenYield);
    let consumer = consumers.pop().unwrap();
    let (mut fsync_writer, fsync_reader) = melin_pipeline::seqlock::split(FsyncState::default());
    let shadow_path = dir.join("shadow.snapshot");
    let shutdown = Arc::new(AtomicBool::new(false));
    let t_shadow = {
        let (shadow_path, shutdown) = (shadow_path.clone(), Arc::clone(&shutdown));
        std::thread::spawn(move || {
            crate::shadow::run(
                consumer,
                RecordingApp::default(),
                shadow_path,
                Duration::from_millis(1),
                fsync_reader,
                &shutdown,
                WaitStrategy::SpinThenYield,
                0,
            )
        })
    };

    let mut anchors = Vec::with_capacity(slots.len());
    for (i, slot) in slots.iter().enumerate() {
        if slot.event.is_query() {
            // The shadow consumes it and runs past the fsync state's ring
            // position, so it saves nothing until the next anchor.
            producer.publish(*slot);
            continue;
        }
        let anchor = anchors.len() as u64 + 1;
        fsync_writer.store(FsyncState {
            journal_seq: WireSeq::new(anchor),
            chain_hash: chain[anchor as usize],
            // The anchor entry's stamp, which recovery checks against the
            // journal's.
            last_timestamp: slot.timestamp,
            input_ring_seq: RingPos::new(i as u64 + 1),
        });
        producer.publish(*slot);
        // The shadow saves repeatedly while it sits at the anchor, and
        // each save briefly moves the file aside, so a failed load is
        // retried rather than treated as an error.
        let mut saved = None;
        wait_for("the shadow's snapshot at an anchor", || {
            saved = crate::snapshot::load::<RecordingApp>(&shadow_path)
                .ok()
                .filter(|loaded| loaded.sequence == anchor);
            saved.is_some()
        });
        let loaded = saved.unwrap();
        let path = dir.join(format!("anchor-{anchor}.snapshot"));
        crate::snapshot::save(
            &loaded.app,
            WireSeq::new(loaded.sequence),
            loaded.chain_hash,
            loaded.epoch,
            loaded.floor.time(),
            &path,
        )
        .unwrap();
        anchors.push(path);
    }
    shutdown.store(true, Ordering::Relaxed);
    t_shadow.join().unwrap();
    anchors
}

/// Where two call traces part, for a readable failure.
pub(crate) fn first_difference(a: &[Call], b: &[Call]) -> String {
    let at = a.iter().zip(b).take_while(|(x, y)| x == y).count();
    format!(
        "first difference at call {at}: live {:?}, other {:?} (lengths {} and {})",
        a.get(at),
        b.get(at),
        a.len(),
        b.len()
    )
}

fn check_history(plan: &[Planned]) -> Result<(), TestCaseError> {
    let (slots, rotate_after) = build_slots(plan);
    prop_assume!(!slots.is_empty());
    let dir = tempfile::tempdir().unwrap();
    let journal = dir.path().join("trace.journal");

    let live = run_live(&journal, &slots, &rotate_after);

    let (recovered, _writer) =
        JournaledApp::<RecordingApp, Writer>::recover(RecordingApp::default(), &journal)
            .unwrap()
            .into_parts();
    prop_assert!(
        recovered.calls == live,
        "recovery from genesis: {}",
        first_difference(&live, &recovered.calls)
    );

    let chain = chain_after_each_entry(&journal);
    prop_assert_eq!(
        chain.len() as u64,
        journaled_count(&slots) + 1,
        "every write, tick and epoch bump journaled, no query"
    );
    for (i, snapshot) in shadow_snapshots(dir.path(), &slots, &chain)
        .iter()
        .enumerate()
    {
        let (restored, _writer) =
            JournaledApp::<RecordingApp, Writer>::recover_from_snapshot(snapshot, &journal)
                .unwrap()
                .into_parts();
        prop_assert!(
            restored.calls == live,
            "restore at anchor {}: {}",
            i + 1,
            first_difference(&live, &restored.calls)
        );
    }
    Ok(())
}

proptest! {
    // Each case runs a pipeline, a shadow stage and one recovery per
    // anchor, so the case count is kept low.
    #![proptest_config(ProptestConfig::with_cases(24))]

    #[test]
    fn calls_into_the_application_depend_on_the_journal_alone(
        plan in proptest::collection::vec(planned(), 1..32),
    ) {
        check_history(&plan)?;
    }
}
