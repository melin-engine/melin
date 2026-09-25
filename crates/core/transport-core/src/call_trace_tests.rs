//! The runtime's half of determinism: the calls it makes into an
//! application depend on the journal alone.
//!
//! A recording application logs every call it receives. A generated
//! history (application events, journaled ticks, epoch bumps, rotations
//! at random points) runs through the live pipeline, then through
//! recovery from genesis, then through a restore from a snapshot at every
//! anchor followed by replay of the rest. Each must hand the application
//! the calls the live run did. The snapshots are the shadow stage's own,
//! taken as it reaches each anchor, so the restored prefix is what
//! production would restore.
//!
//! This is the runtime's half only. That identical calls build identical
//! state is the application's half, and depends on the application.

#![cfg(all(test, not(feature = "no-persist")))]

use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use melin_app::{AppEvent, Application, ApplyCtx, CodecError, RejectReason, SequencerTime};
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

/// An application event: an identifier unique within the history, which
/// the recording names `apply` calls by (`ApplyCtx` carries no sequence).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Step(u64);

impl AppEvent for Step {
    const MAX_ENCODED_SIZE: usize = 8;

    fn encoded_size(&self) -> usize {
        8
    }

    fn encode(&self, buf: &mut [u8]) -> usize {
        buf[..8].copy_from_slice(&self.0.to_le_bytes());
        8
    }

    fn decode(buf: &[u8]) -> Result<Self, CodecError> {
        let bytes: [u8; 8] = buf.try_into().map_err(|_| CodecError::Truncated)?;
        Ok(Step(u64::from_le_bytes(bytes)))
    }

    fn is_query(&self) -> bool {
        false
    }
}

/// One call the runtime made into the application.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Call {
    Tick { now_ns: u64 },
    Apply { id: u64, now_ns: u64, key_hash: u64 },
}

/// Records every call it receives, and snapshots the record, so a
/// restored instance carries the calls made before its anchor.
#[derive(Debug, Default)]
struct RecordingApp {
    // Vec: calls are only appended and compared in order.
    calls: Vec<Call>,
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
        self.calls.push(Call::Apply {
            id: event.0,
            now_ns: ctx.now_ns,
            key_hash: ctx.key_hash,
        });
    }

    fn tick(&mut self, now_ns: u64, _out: &mut Vec<()>) {
        self.calls.push(Call::Tick { now_ns });
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
        2 => Just(Op::Tick),
        1 => Just(Op::EpochBump),
        1 => Just(Op::Rotate),
    ];
    (op, 1u64..=3).prop_map(|(op, stamp_gap_ns)| Planned { op, stamp_gap_ns })
}

type Writer = BufferedWriter<Step>;

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
            Op::App { key_hash } => (1, key_hash, JournalEvent::App(Step(id as u64))),
            Op::Tick => (0, 0, JournalEvent::Tick { now_ns: stamp }),
            Op::EpochBump => {
                epoch += 1;
                (0, 0, JournalEvent::EpochBump { epoch })
            }
        };
        slots.push(InputSlot {
            connection_id,
            key_hash,
            sequence: 0,
            timestamp: SequencerTime::from_ns(stamp),
            event,
            publish_ts: mono_trace_ns(),
            recv_ts: mono_trace_ns(),
        });
    }
    (slots, rotate_after)
}

/// Poll `done` until it holds, panicking with `what` past the deadline.
#[track_caller]
fn wait_for(what: &str, mut done: impl FnMut() -> bool) {
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

    // A rotation takes effect after the batch that follows the request,
    // so each one is confirmed by its archive before the next is asked
    // for; otherwise two requests could collapse into one rotation.
    let mut rotations = 0usize;
    let mut rotation_pending = false;
    for (i, slot) in slots.iter().enumerate() {
        if rotate_after.contains(&i) && !rotation_pending {
            rotate.store(true, Ordering::Release);
            rotation_pending = true;
        }
        producer.publish(*slot);
        if rotation_pending {
            rotations += 1;
            wait_for("a rotation", || {
                melin_journal::segment::list_archives(journal)
                    .unwrap()
                    .len()
                    == rotations
            });
            rotation_pending = false;
        }
    }
    let journaled = slots.len() as u64;
    wait_for("the journal to reach the last slot", || {
        durable.load().get() == journaled
    });

    shutdown.store(true, Ordering::Relaxed);
    t_journal.join().unwrap().unwrap();
    t_matching.join().unwrap().calls
}

/// The chain hash after each journaled entry, indexed by sequence
/// (index 0 unused), walking the archives and then the live segment.
fn chain_after_each_entry(journal: &Path) -> Vec<[u8; 32]> {
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
fn shadow_snapshots(dir: &Path, slots: &[InputSlot<Step>], chain: &[[u8; 32]]) -> Vec<PathBuf> {
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
        let anchor = i as u64 + 1;
        fsync_writer.store(FsyncState {
            journal_seq: WireSeq::new(anchor),
            chain_hash: chain[anchor as usize],
            input_ring_seq: RingPos::new(anchor),
        });
        producer.publish(*slot);
        // The shadow saves repeatedly while it sits at the anchor, and
        // each save briefly moves the file aside, so a failed load is
        // retried rather than treated as an error.
        let mut saved = None;
        wait_for("the shadow's snapshot at an anchor", || {
            saved = crate::snapshot::load::<RecordingApp>(&shadow_path)
                .ok()
                .filter(|(_, seq, _, _)| *seq == anchor);
            saved.is_some()
        });
        let (app, seq, chain_hash, epoch) = saved.unwrap();
        let path = dir.join(format!("anchor-{anchor}.snapshot"));
        crate::snapshot::save(&app, WireSeq::new(seq), chain_hash, epoch, &path).unwrap();
        anchors.push(path);
    }
    shutdown.store(true, Ordering::Relaxed);
    t_shadow.join().unwrap();
    anchors
}

/// Where two call traces part, for a readable failure.
fn first_difference(a: &[Call], b: &[Call]) -> String {
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
    prop_assert_eq!(chain.len(), slots.len() + 1, "every slot journaled");
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
