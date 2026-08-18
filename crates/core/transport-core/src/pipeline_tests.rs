//! Application-agnostic pipeline tests.
//!
//! These exercise the journal stage, matching stage, and combined
//! pipeline against `TestApp` / `TestEvent` rather than any concrete
//! business engine. They used to live in `melin-exchange-core` only because the
//! pipeline source was extracted from there; now that the pipeline lives
//! here, the infrastructure-level tests do too.
//!
//! Business-flavoured pipeline tests (halt-gate behaviour, etc.) remain
//! in the engine crate where the trading-specific reject shapes and
//! event variants are natural.

#![cfg(test)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use melin_journal::replication::REPLICATION_RING_CAPACITY;
// Only the journal-reading tests touch these, and those are gated off
// under no-persist (every read needs a really-persisted journal file).
#[cfg(not(feature = "no-persist"))]
use melin_journal::JournalReader;
use melin_journal::{BufferedWriter, JournalEvent};
use melin_pipeline::ring;

#[cfg(not(feature = "no-persist"))]
use crate::cursors::SlotAcked;
use crate::cursors::{DurableWireSeqCursor, WireSeq};
// Recovery replays a journal from disk, so every use sits in a test that
// no-persist gates off — same reason as `JournalReader` above.
#[cfg(not(feature = "no-persist"))]
use crate::journaled_app::JournaledApp;
#[cfg(all(feature = "hash-chain", not(feature = "no-persist")))]
use crate::pipeline::build_replica_pipeline;
// Only the hash-chain mark tests touch these.
#[cfg(all(feature = "hash-chain", not(feature = "no-persist")))]
use crate::pipeline::{AdoptedRotation, StreamMark};
use crate::pipeline::{
    InputSlot, JournalStage, MAX_JOURNAL_BATCH, MatchingStage, OutputPayload, OutputSlot,
    build_pipeline_with_replication,
};
use crate::test_support::{TestApp, TestEvent, TestQuery, TestReport};
use crate::trace::mono_trace_ns;

type Writer = BufferedWriter<TestEvent>;
type TestInput = InputSlot<TestEvent>;
type TestOutput = OutputSlot<TestReport, TestQuery>;

/// Wall-clock budget for a test thread waiting on pipeline output.
///
/// Generous because these tests run concurrently with the rest of the
/// suite, each spawning busy-spinning stage threads, so the machine is
/// heavily oversubscribed.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// Back-off for a test thread polling a pipeline output ring. Panics
/// once [`DRAIN_TIMEOUT`] of wall time has passed since `start`.
///
/// Spins briefly, then **yields**. A pure `spin_loop()` wait starves
/// the very stage thread it is waiting on when the suite oversubscribes
/// the CPU, and a spin-*count* budget then runs out without the
/// pipeline ever having been scheduled — which showed up as flaky
/// "timeout draining outputs" failures under `--features latency-trace`,
/// where the stages do more work per event.
///
/// Takes `&mut u32` rather than owning the counter so the spin/yield
/// switchover survives across loop iterations.
#[track_caller]
fn drain_backoff(spins: &mut u32, start: std::time::Instant, what: &str) {
    assert!(
        start.elapsed() < DRAIN_TIMEOUT,
        "timeout after {DRAIN_TIMEOUT:?} {what}"
    );
    if *spins < 1000 {
        *spins += 1;
        std::hint::spin_loop();
    } else {
        std::thread::yield_now();
    }
}

/// A standalone durable-wire-seq handle for matching-stage-only tests
/// (nothing publishes into it; the stage reads it once per batch).
fn dummy_durable_cursor() -> DurableWireSeqCursor {
    DurableWireSeqCursor::detached(WireSeq::new(0))
}

/// First user-event sequence. Chain metadata lives in the file header,
/// so sequence 1 is a real event under every feature config. Only
/// referenced from journal-reader assertions, which are themselves
/// gated on `not(no-persist)`.
#[cfg(not(feature = "no-persist"))]
const FIRST_SEQ: u64 = 1;

/// Build an input slot carrying a single `TestEvent::Add(n)`. Primary-
/// side producers leave `sequence = 0`; the journal stage assigns it at
/// encode time. Tests that simulate replica input pass a pre-assigned
/// sequence via the builder method below.
fn add_slot(n: u64, timestamp_ns: u64) -> TestInput {
    InputSlot {
        connection_id: 1,
        key_hash: 0,
        request_seq: 0,
        sequence: 0,
        timestamp_ns,
        event: JournalEvent::App(TestEvent::Add(n)),
        publish_ts: mono_trace_ns(),
        recv_ts: mono_trace_ns(),
    }
}

/// Like `add_slot` but with a pre-assigned journal sequence — simulates
/// the slot shape the replication receiver produces.
fn add_slot_with_seq(n: u64, sequence: u64, timestamp_ns: u64) -> TestInput {
    InputSlot {
        sequence,
        ..add_slot(n, timestamp_ns)
    }
}

/// Primary path: `slot.sequence == 0` so the JournalStage allocates
/// sequences from the writer at encode time, in publish order. The
/// encoded entries must carry consecutive sequences starting from
/// `FIRST_SEQ`.
#[test]
fn journal_stage_allocates_primary_sequences() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("pipeline_journal.journal");

    let writer = Writer::create(&path).unwrap();

    let (mut producer, mut consumers) = ring::DisruptorBuilder::<TestInput>::new(64)
        .add_consumer()
        .build();

    let consumer = consumers.pop().unwrap();
    let stage = JournalStage::new(writer, consumer, Duration::ZERO, MAX_JOURNAL_BATCH, false);

    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown2 = Arc::clone(&shutdown);

    producer.publish(add_slot(7, 1_000_000_000));
    producer.publish(add_slot(11, 1_000_000_001));

    let handle = std::thread::spawn(move || stage.run(&shutdown2));

    std::thread::sleep(Duration::from_millis(50));
    shutdown.store(true, Ordering::Relaxed);
    let _writer = handle.join().unwrap();

    // Verify events were journaled with consecutive sequences starting
    // from FIRST_SEQ — proving the journal stage (not the producer)
    // allocated them.
    #[cfg(not(feature = "no-persist"))]
    {
        let mut reader = JournalReader::<TestEvent>::open(&path).unwrap();
        let entry1 = reader.next_entry().unwrap().unwrap();
        assert_eq!(entry1.sequence, FIRST_SEQ);
        assert!(matches!(entry1.event, JournalEvent::App(TestEvent::Add(7))));
        let entry2 = reader.next_entry().unwrap().unwrap();
        assert_eq!(entry2.sequence, FIRST_SEQ + 1);
        assert!(matches!(
            entry2.event,
            JournalEvent::App(TestEvent::Add(11))
        ));
        assert!(reader.next_entry().unwrap().is_none());
    }
}

#[test]
fn matching_stage_processes_events() {
    let app = TestApp::new();

    let (mut input_producer, mut consumers) = ring::DisruptorBuilder::<TestInput>::new(64)
        .add_consumer()
        .build();
    let consumer = consumers.pop().unwrap();

    let (output_producer, mut output_consumers) = ring::DisruptorBuilder::<TestOutput>::new(64)
        .add_consumer()
        .build();
    let mut output_consumer = output_consumers.pop().unwrap();

    // Durable cursor and counters not used in this test — create dummies.
    let dummy_cursor = dummy_durable_cursor();
    let events_counter = Arc::new(AtomicU64::new(0));
    let active_conns = Arc::new(AtomicU64::new(0));
    let stage = MatchingStage::new(
        app,
        consumer,
        output_producer,
        events_counter,
        dummy_cursor,
        active_conns,
        None, // standalone — no halt check
        Arc::new(crate::fence::FenceState::new(0)),
        false,
        1, // starting_wire_seq (test does not exercise the gate)
    );

    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown2 = Arc::clone(&shutdown);

    let mut slot = add_slot(42, 0);
    slot.connection_id = 42;
    input_producer.publish(slot);

    let handle = std::thread::spawn(move || stage.run(&shutdown2));

    let mut spins = 0u32;
    let drain_start = std::time::Instant::now();
    let output = loop {
        if let Some((_, slot)) = output_consumer.try_consume() {
            break slot;
        }
        drain_backoff(&mut spins, drain_start, "waiting for output");
    };

    assert_eq!(output.connection_id, 42);
    assert_eq!(output.input_seq, 0);
    assert!(matches!(
        output.payload,
        OutputPayload::Report(TestReport { total_after: 42 })
    ));
    // The single-report slot also carries the request terminator — the
    // response stage emits the wire BatchEnd from this flag, saving the
    // separate BatchEnd-payload slot.
    assert!(output.is_last_in_request);

    shutdown.store(true, Ordering::Relaxed);
    let _app = handle.join().unwrap();
}

/// Pin the matching stage's `wire_seq` stamping rule against future
/// drift. The response stage's durability gate depends on
/// `OutputSlot.wire_seq` being in lockstep with what the journal stage's
/// allocator would assign — same starting value, same per-event rule
/// (advance for App-non-query / Tick, hold flat for `Query`). The
/// lockstep is the load-bearing piece behind `fix(durability): gate on
/// wire-seq`; this test fails fast if either side's rule changes
/// without the other tracking it.
#[test]
fn matching_stage_stamps_wire_seq_in_journal_lockstep() {
    let app = TestApp::new();

    // Big enough to hold the 6 input events + a `Shutdown` sentinel
    // without backpressure stalling the producer mid-publish.
    let (mut input_producer, mut consumers) = ring::DisruptorBuilder::<TestInput>::new(64)
        .add_consumer()
        .build();
    let consumer = consumers.pop().unwrap();

    let (output_producer, mut output_consumers) = ring::DisruptorBuilder::<TestOutput>::new(64)
        .add_consumer()
        .build();
    let mut output_consumer = output_consumers.pop().unwrap();

    let dummy_cursor = dummy_durable_cursor();
    let events_counter = Arc::new(AtomicU64::new(0));
    let active_conns = Arc::new(AtomicU64::new(0));
    // Pick a non-1 starting value (10) so an off-by-`starting-1` regression
    // — the exact bug this fix addresses — would visibly miss every
    // assertion below rather than coincidentally satisfy them when
    // `starting == 1` makes input-seq and wire-seq numerically agree.
    const STARTING_WIRE_SEQ: u64 = 10;
    // Keep a handle so we can assert the `EpochBump` below advances the
    // observed epoch (it is sequenced like any non-query event but never
    // reaches the application).
    let fence = Arc::new(crate::fence::FenceState::new(0));
    let stage = MatchingStage::new(
        app,
        consumer,
        output_producer,
        events_counter,
        dummy_cursor,
        active_conns,
        None,
        Arc::clone(&fence),
        false,
        STARTING_WIRE_SEQ,
    );

    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown2 = Arc::clone(&shutdown);
    let handle = std::thread::spawn(move || stage.run(&shutdown2));

    // Input sequence mixes every event class the rule cares about. The
    // expected wire_seq column is what the journal stage's allocator
    // would assign under the same rule (allocate for App / Tick,
    // `continue` past Query); the matching stage must produce the same
    // values into `OutputSlot.wire_seq`.
    //
    //   #  | event                | journal allocates | wire_seq stamped
    //   ---+----------------------+-------------------+-------------------
    //   1  | App(Add 1)           | yes → 10          | 10
    //   2  | Query                | no                | 10  (= 11 - 1)
    //   3  | App(Add 2)           | yes → 11          | 11
    //   4  | App(Add 3)           | yes → 12          | 12
    //   5  | Tick                 | yes → 13          | 13
    //   6  | EpochBump{7}         | yes → 14          | 14
    //
    // All slots carry `connection_id = 1` so events that produce no
    // application reports (Tick, EpochBump) still emit a `BatchEnd`
    // terminator on the output ring; that way every input event appears
    // exactly once in the assertions below regardless of payload shape.
    // `EpochBump` is the regression guard for the seq-allocation policy:
    // it must follow the *same* allocate rule as App/Tick (it is not a
    // query), so wire space and journal-allocator space stay in lockstep.
    let conn_id = 1u64;
    let mut publish = |event: JournalEvent<TestEvent>| {
        input_producer.publish(InputSlot {
            connection_id: conn_id,
            key_hash: 0,
            request_seq: 0,
            sequence: 0,
            timestamp_ns: 0,
            event,
            publish_ts: mono_trace_ns(),
            recv_ts: mono_trace_ns(),
        });
    };
    publish(JournalEvent::App(TestEvent::Add(1)));
    publish(JournalEvent::App(TestEvent::Query));
    publish(JournalEvent::App(TestEvent::Add(2)));
    publish(JournalEvent::App(TestEvent::Add(3)));
    publish(JournalEvent::Tick { now_ns: 1 });
    publish(JournalEvent::EpochBump { epoch: 7 });

    // Drain six output slots — one per input event under the
    // connection-id-1 invariant above.
    let mut outputs: Vec<TestOutput> = Vec::with_capacity(6);
    let mut spins = 0u32;
    let drain_start = std::time::Instant::now();
    while outputs.len() < 6 {
        if let Some((_, slot)) = output_consumer.try_consume() {
            outputs.push(slot);
        } else {
            drain_backoff(&mut spins, drain_start, "draining outputs");
        }
    }

    let actual: Vec<u64> = outputs.iter().map(|s| s.wire_seq).collect();
    assert_eq!(
        actual,
        vec![10, 10, 11, 12, 13, 14],
        "wire_seq stamping diverged from the journal allocator's per-event rule"
    );

    // The EpochBump must have advanced the observed epoch without ever
    // touching application state.
    assert_eq!(
        fence.epoch(),
        7,
        "EpochBump did not advance the observed epoch"
    );

    // Sanity-check the output payload shape so a future change that
    // accidentally drops one event without us noticing (and shifts the
    // wire_seq sequence by one) gets caught here rather than in the
    // integration suite.
    let payload_kinds: Vec<&'static str> = outputs
        .iter()
        .map(|s| match &s.payload {
            OutputPayload::Report(_) => "Report",
            OutputPayload::QueryResponse(_) => "QueryResponse",
            OutputPayload::BatchEnd => "BatchEnd",
            OutputPayload::EngineError => "EngineError",
        })
        .collect();
    assert_eq!(
        payload_kinds,
        vec![
            "Report",
            "QueryResponse",
            "Report",
            "Report",
            "BatchEnd",
            "BatchEnd"
        ],
    );

    shutdown.store(true, Ordering::Relaxed);
    let _app = handle.join().unwrap();
}

/// Regression tripwire for the pre-v14 durability-gate hole: the
/// response gate compares `OutputSlot.wire_seq` (stamped by the
/// matching stage) against `last_seq` (published by the journal stage
/// from the writer's allocator) and against replica ack cursors —
/// which echo allocator sequences stamped on shipped entries. Before
/// v14, writer-internal entries (auto-emitted checkpoints, rotation
/// genesis) consumed allocator sequences without ever crossing the
/// input ring, so wire space fell permanently behind allocator space —
/// one sequence per checkpoint/rotation — and the replica clauses of
/// `hybrid` / `durably-replicated` became vacuous within seconds of
/// uptime: the gate released client acks before any replica held the
/// order. v14 made the two spaces identical by removing every
/// writer-internal sequence consumer; this test fails if one
/// reappears.
///
/// The rule-table lockstep test above cannot catch this class — those
/// entries never appear on the input ring, so no stamping rule is
/// consulted. Instead, drive the real journal + matching stages over a
/// stream that includes a segment rotation (a historical drift source)
/// and assert the three views of the high-water mark agree exactly:
///
///   1. the highest `wire_seq` stamped on the output ring,
///   2. `last_seq` — the gate's primary `persisted` cursor,
///   3. the last sequence in the on-disk lineage — what a replica
///      would ack, since shipped entries carry on-disk sequences.
#[cfg(all(feature = "hash-chain", not(feature = "no-persist")))]
#[test]
fn allocator_wire_seq_and_gate_cursor_agree_across_rotation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("gate_space_agreement.journal");

    let writer = Writer::create(&path).unwrap();
    let active_conns = Arc::new(AtomicU64::new(0));
    let mut out = build_pipeline_with_replication(
        TestApp::new(),
        writer,
        Duration::ZERO,
        active_conns,
        false,
        MAX_JOURNAL_BATCH,
        REPLICATION_RING_CAPACITY,
        false,
        false,
        false,
        Arc::new(crate::fence::FenceState::new(0)),
    );
    let mut input_producer = out.input_producer;
    let mut journal_stage = out.journal_stage;
    let matching_stage = out.matching_stage;
    let last_seq = out.cursors.durable_wire_seq();
    let mut output_consumer = out.output_consumers.pop().unwrap();

    let rotate_flag = Arc::new(AtomicBool::new(false));
    journal_stage.set_rotation(
        /* max_journal_bytes */ 0,
        Some(Arc::clone(&rotate_flag)),
    );

    let shutdown = Arc::new(AtomicBool::new(false));
    let s1 = Arc::clone(&shutdown);
    let s2 = Arc::clone(&shutdown);
    let t_journal = std::thread::spawn(move || journal_stage.run(&s1));
    let t_matching = std::thread::spawn(move || matching_stage.run(&s2));

    // All slots carry `connection_id = 1` so every event — including
    // the report-less Tick — emits exactly one output slot (same
    // invariant as the lockstep test above), letting the drain below
    // count inputs 1:1.
    let mut req_seq = 0u64;
    let mut publish = |event: JournalEvent<TestEvent>| {
        req_seq += 1;
        input_producer.publish(InputSlot {
            connection_id: 1,
            key_hash: 1,
            request_seq: req_seq,
            sequence: 0,
            timestamp_ns: 1_000_000_000 + req_seq,
            event,
            publish_ts: mono_trace_ns(),
            recv_ts: mono_trace_ns(),
        });
    };

    // Pre-rotation phase: the allocator assigns 1, 2, holds flat for
    // the query, then 3 for the tick.
    publish(JournalEvent::App(TestEvent::Add(100)));
    publish(JournalEvent::App(TestEvent::Add(200)));
    publish(JournalEvent::App(TestEvent::Query));
    publish(JournalEvent::Tick { now_ns: 1 });

    // Wait until the pre-rotation entries are durably in the live
    // segment (last_seq is published post-fsync) so the rotation
    // boundary genuinely splits the stream. Polled — fixed sleeps
    // flake on slow CI machines.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while last_seq.load().get() < 3 && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    rotate_flag.store(true, Ordering::Release);

    // Post-rotation phase. The rotation itself must consume no
    // sequence: 4 and 5.
    publish(JournalEvent::App(TestEvent::Add(50)));
    let archive_path = std::path::PathBuf::from(format!("{}.000001", path.display()));
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !archive_path.exists() && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(archive_path.exists(), "rotation did not produce an archive");
    publish(JournalEvent::App(TestEvent::Add(1000)));

    // Drain one output slot per input event and pin the stamped wire
    // seqs. A change here means the allocator/wire rule moved — update
    // only in lockstep with the journal stage's allocation rule.
    let mut outputs: Vec<TestOutput> = Vec::with_capacity(6);
    let mut spins = 0u32;
    let drain_start = std::time::Instant::now();
    while outputs.len() < 6 {
        if let Some((_, slot)) = output_consumer.try_consume() {
            outputs.push(slot);
        } else {
            drain_backoff(&mut spins, drain_start, "draining outputs");
        }
    }
    let wire_seqs: Vec<u64> = outputs.iter().map(|s| s.wire_seq).collect();
    assert_eq!(
        wire_seqs,
        vec![1, 2, 2, 3, 4, 5],
        "wire_seq stamping diverged from the journal allocator's rule"
    );
    const MAX_WIRE_SEQ: u64 = 5;

    // View 2: the gate's primary `persisted` cursor must converge on
    // exactly the wire high-water mark. Poll for catch-up (the fsync
    // publish runs on the journal thread), then assert equality — an
    // allocator running ahead of wire space overshoots and fails
    // immediately rather than timing out.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while last_seq.load().get() < MAX_WIRE_SEQ && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        last_seq.load().get(),
        MAX_WIRE_SEQ,
        "gate persisted cursor diverged from wire space — a writer-internal \
         entry is consuming sequences again (the pre-v14 vacuous-gate bug)"
    );

    shutdown.store(true, Ordering::Relaxed);
    let _writer = t_journal.join().unwrap();
    let _app = t_matching.join().unwrap();

    // View 3: what a replica would ack. Shipped entries carry on-disk
    // sequences, so the lineage's last sequence is the replica-side
    // view of the same high-water mark. Two segments prove the
    // rotation actually exercised the historical drift source.
    let report = melin_journal::segment::verify_lineage::<TestEvent>(&path).unwrap();
    assert_eq!(report.segments, 2, "expected archive + live after rotation");
    assert_eq!(
        report.last_sequence,
        Some(MAX_WIRE_SEQ),
        "on-disk lineage diverged from wire space — replica acks would run \
         ahead of the response gate's wire_seq (the pre-v14 vacuous-gate bug)"
    );
    assert_eq!(
        report.entries, 5,
        "five allocated events expected (the query is not journaled)"
    );
}

/// Recovery-seam sibling of
/// [`allocator_wire_seq_and_gate_cursor_agree_across_rotation`]: the
/// agreement must *survive recovery*. The pipeline builder derives
/// `starting_wire_seq` (and the gate cursor's initial value) from the
/// recovered writer's allocator, which `open_append` reconstitutes from
/// the on-disk lineage — a misinitialization anywhere along that chain
/// re-opens the off-by-`starting` gate hole the lockstep test's
/// `STARTING_WIRE_SEQ = 10` comment warns about, but only on restarted
/// nodes, where no fresh-journal test can see it.
///
/// Phase 1 journals four events across a rotation and shuts down.
/// Phase 2 recovers through the production path (`recover` →
/// `into_parts` → pipeline builder), then asserts:
///   - the gate cursor resumes at exactly the recovered high-water mark
///     (before any new event),
///   - a query arriving before any post-recovery allocation stamps that
///     same mark (the gate must satisfy it from recovered state),
///   - new allocations continue the wire space with no gap or overlap,
///   - the on-disk lineage tail agrees after the second shutdown.
#[cfg(all(feature = "hash-chain", not(feature = "no-persist")))]
#[test]
fn recovery_resumes_allocator_wire_and_gate_agreement() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("gate_recovery_agreement.journal");

    // Slot builder shared by both phases. `request_seq` increases
    // monotonically across the recovery boundary so replayed dedup
    // state can never collide with phase-2 traffic.
    let mut req_seq = 0u64;
    let mut make_slot = |event: JournalEvent<TestEvent>| {
        req_seq += 1;
        InputSlot {
            connection_id: 1,
            key_hash: 1,
            request_seq: req_seq,
            sequence: 0,
            timestamp_ns: 1_000_000_000 + req_seq,
            event,
            publish_ts: mono_trace_ns(),
            recv_ts: mono_trace_ns(),
        }
    };

    // --- Phase 1: journal events 1..=4 across a rotation, shut down ---
    {
        let writer = Writer::create(&path).unwrap();
        let active_conns = Arc::new(AtomicU64::new(0));
        let out = build_pipeline_with_replication(
            TestApp::new(),
            writer,
            Duration::ZERO,
            active_conns,
            false,
            MAX_JOURNAL_BATCH,
            REPLICATION_RING_CAPACITY,
            false,
            false,
            false,
            Arc::new(crate::fence::FenceState::new(0)),
        );
        let mut input_producer = out.input_producer;
        let mut journal_stage = out.journal_stage;
        let matching_stage = out.matching_stage;
        let last_seq = out.cursors.durable_wire_seq();

        let rotate_flag = Arc::new(AtomicBool::new(false));
        journal_stage.set_rotation(
            /* max_journal_bytes */ 0,
            Some(Arc::clone(&rotate_flag)),
        );

        let shutdown = Arc::new(AtomicBool::new(false));
        let s1 = Arc::clone(&shutdown);
        let s2 = Arc::clone(&shutdown);
        let t_journal = std::thread::spawn(move || journal_stage.run(&s1));
        let t_matching = std::thread::spawn(move || matching_stage.run(&s2));

        for n in 1..=3u64 {
            input_producer.publish(make_slot(JournalEvent::App(TestEvent::Add(n))));
        }
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while last_seq.load().get() < 3 && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        rotate_flag.store(true, Ordering::Release);
        input_producer.publish(make_slot(JournalEvent::App(TestEvent::Add(4))));

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while last_seq.load().get() < 4 && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(last_seq.load().get(), 4, "phase 1 fsync");

        shutdown.store(true, Ordering::Relaxed);
        let _writer = t_journal.join().unwrap();
        let _app = t_matching.join().unwrap();
    }

    // --- Phase 2: recover and continue ---
    let engine = JournaledApp::<TestApp, Writer>::recover(TestApp::new(), &path).unwrap();
    assert_eq!(engine.app().total, 1 + 2 + 3 + 4, "recovered state");
    assert_eq!(engine.next_sequence(), 5, "recovered allocator position");
    let (app, writer) = engine.into_parts();

    let active_conns = Arc::new(AtomicU64::new(0));
    let mut out = build_pipeline_with_replication(
        app,
        writer,
        Duration::ZERO,
        active_conns,
        false,
        MAX_JOURNAL_BATCH,
        REPLICATION_RING_CAPACITY,
        false,
        false,
        false,
        Arc::new(crate::fence::FenceState::new(0)),
    );
    let mut input_producer = out.input_producer;
    let journal_stage = out.journal_stage;
    let matching_stage = out.matching_stage;
    let last_seq = out.cursors.durable_wire_seq();
    let mut output_consumer = out.output_consumers.pop().unwrap();

    // The gate cursor must resume at exactly the recovered high-water
    // mark — before any new event is published. A writer-internal
    // entry consumed during recovery/reopen would overshoot here.
    assert_eq!(
        last_seq.load().get(),
        4,
        "gate persisted cursor must resume at the recovered high-water mark"
    );

    let shutdown = Arc::new(AtomicBool::new(false));
    let s1 = Arc::clone(&shutdown);
    let s2 = Arc::clone(&shutdown);
    let t_journal = std::thread::spawn(move || journal_stage.run(&s1));
    let t_matching = std::thread::spawn(move || matching_stage.run(&s2));

    // A query before any post-recovery allocation must stamp the
    // recovered mark (4) — the gate satisfies it from recovered state.
    // Then two allocations continue the space: 5 and 6.
    input_producer.publish(make_slot(JournalEvent::App(TestEvent::Query)));
    input_producer.publish(make_slot(JournalEvent::App(TestEvent::Add(5))));
    input_producer.publish(make_slot(JournalEvent::Tick { now_ns: 1 }));

    let mut outputs: Vec<TestOutput> = Vec::with_capacity(3);
    let mut spins = 0u32;
    let drain_start = std::time::Instant::now();
    while outputs.len() < 3 {
        if let Some((_, slot)) = output_consumer.try_consume() {
            outputs.push(slot);
        } else {
            drain_backoff(&mut spins, drain_start, "draining outputs");
        }
    }
    let wire_seqs: Vec<u64> = outputs.iter().map(|s| s.wire_seq).collect();
    assert_eq!(
        wire_seqs,
        vec![4, 5, 6],
        "post-recovery wire space must continue the recovered allocator \
         with no gap or overlap"
    );

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while last_seq.load().get() < 6 && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        last_seq.load().get(),
        6,
        "gate persisted cursor diverged from wire space after recovery"
    );

    shutdown.store(true, Ordering::Relaxed);
    let _writer = t_journal.join().unwrap();
    let _app = t_matching.join().unwrap();

    let report = melin_journal::segment::verify_lineage::<TestEvent>(&path).unwrap();
    assert_eq!(
        report.last_sequence,
        Some(6),
        "on-disk lineage tail diverged from wire space after recovery"
    );
    assert_eq!(report.entries, 6, "six allocated events across both phases");
}

/// Replica half of the sequence-space invariant: the replica's ack
/// cursors (`last_seq` feeds the reconnect handshake and, through
/// `FsyncState`, the durable ack the primary's gate counts) must track
/// the primary-stamped sequences exactly — a replica-*local* rotation
/// must consume none. Rotations are local in production (segment
/// boundaries diverge across nodes), so a writer-internal entry on the
/// replica side would inflate its acks relative to the primary's wire
/// space even with a fully-correct primary — the mirror image of the
/// pre-v14 drift, invisible to every primary-side test.
///
/// Feed the replica pipeline pre-assigned sequences (the slot shape the
/// replication receiver produces), rotate its journal mid-stream, and
/// assert its durable cursor and on-disk lineage land exactly on the
/// primary's high-water mark. The dense-sequence walk inside
/// `verify_lineage` additionally fails loudly if a local entry ever
/// collides with a primary-stamped sequence.
#[cfg(all(feature = "hash-chain", not(feature = "no-persist")))]
#[test]
fn replica_ack_cursor_tracks_primary_sequences_across_local_rotation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("replica_local_rotation.journal");

    // Fresh-replica creation path: segment header identity comes from
    // the primary's StreamStart in production.
    let writer = Writer::create_continuing(&path, 1, [0xB7u8; 32]).unwrap();
    let replica = build_replica_pipeline(
        TestApp::new(),
        writer,
        MAX_JOURNAL_BATCH,
        Duration::ZERO,
        false,
        false,
        Arc::new(crate::fence::FenceState::new(0)),
    );
    let mut input_producer = replica.input_producer;
    let mut journal_stage = replica.journal_stage;
    let matching_stage = replica.matching_stage;
    let last_seq = replica.cursors.durable_wire_seq();

    let rotate_flag = Arc::new(AtomicBool::new(false));
    journal_stage.set_rotation(
        /* max_journal_bytes */ 0,
        Some(Arc::clone(&rotate_flag)),
    );

    let shutdown = Arc::new(AtomicBool::new(false));
    let s1 = Arc::clone(&shutdown);
    let s2 = Arc::clone(&shutdown);
    let t_journal = std::thread::spawn(move || journal_stage.run(&s1));
    let t_matching = std::thread::spawn(move || matching_stage.run(&s2));

    // Primary-stamped stream, sequences 1..=3, then a local rotation,
    // then 4..=5. The replica must consume the stamped values verbatim.
    for seq in 1..=3u64 {
        input_producer.publish(add_slot_with_seq(seq * 10, seq, 1_000_000_000 + seq));
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while last_seq.load().get() < 3 && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    rotate_flag.store(true, Ordering::Release);

    input_producer.publish(add_slot_with_seq(40, 4, 1_000_000_004));
    let archive_path = std::path::PathBuf::from(format!("{}.000001", path.display()));
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !archive_path.exists() && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(archive_path.exists(), "rotation did not produce an archive");
    input_producer.publish(add_slot_with_seq(50, 5, 1_000_000_005));

    // The durable ack cursor must converge on exactly the last
    // primary-stamped sequence. Overshoot means a replica-local entry
    // consumed a sequence — the replica would ack events the primary
    // never sent.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while last_seq.load().get() < 5 && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        last_seq.load().get(),
        5,
        "replica ack cursor diverged from primary-stamped sequences — a \
         replica-local writer entry is consuming sequences"
    );

    shutdown.store(true, Ordering::Relaxed);
    let _writer = t_journal.join().unwrap();
    let _app = t_matching.join().unwrap();

    let report = melin_journal::segment::verify_lineage::<TestEvent>(&path).unwrap();
    assert_eq!(report.segments, 2, "expected archive + live after rotation");
    assert_eq!(
        report.last_sequence,
        Some(5),
        "replica on-disk lineage diverged from the primary-stamped stream"
    );
    assert_eq!(report.entries, 5, "exactly the five primary entries");
}

/// Verify the JournalStage uses pre-assigned sequences and timestamps
/// when `InputSlot.sequence != 0` (replica mode). The encoded journal
/// entries must carry the primary's sequence numbers, not locally
/// allocated ones.
#[test]
fn journal_stage_uses_preassigned_sequences() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("preseq.journal");

    let writer = Writer::create(&path).unwrap();

    let (mut producer, mut consumers) = ring::DisruptorBuilder::<TestInput>::new(64)
        .add_consumer()
        .build();

    let consumer = consumers.pop().unwrap();
    let stage = JournalStage::new(writer, consumer, Duration::ZERO, MAX_JOURNAL_BATCH, false);

    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown2 = Arc::clone(&shutdown);

    // Publish events with pre-assigned sequences (simulating replica
    // mode). Start at sequence 1 — the fresh journal's header records
    // starting_sequence = 1 and the reader enforces it.
    producer.publish(add_slot_with_seq(7, 1, 1_700_000_000_000_000_000));
    producer.publish(add_slot_with_seq(11, 2, 1_700_000_000_000_000_001));

    let handle = std::thread::spawn(move || stage.run(&shutdown2));

    std::thread::sleep(Duration::from_millis(50));
    shutdown.store(true, Ordering::Relaxed);
    let _writer = handle.join().unwrap();

    #[cfg(not(feature = "no-persist"))]
    {
        let mut reader = JournalReader::<TestEvent>::open(&path).unwrap();

        let entry1 = reader.next_entry().unwrap().unwrap();
        assert_eq!(entry1.sequence, 1);
        assert_eq!(entry1.timestamp_ns, 1_700_000_000_000_000_000);
        assert!(matches!(entry1.event, JournalEvent::App(TestEvent::Add(7))));

        let entry2 = reader.next_entry().unwrap().unwrap();
        assert_eq!(entry2.sequence, 2);
        assert_eq!(entry2.timestamp_ns, 1_700_000_000_000_000_001);
        assert!(matches!(
            entry2.event,
            JournalEvent::App(TestEvent::Add(11))
        ));

        assert!(reader.next_entry().unwrap().is_none());
    }
}

#[test]
fn full_pipeline_journal_and_matching_parallel() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("full_pipeline.journal");

    let writer = Writer::create(&path).unwrap();
    let active_conns = Arc::new(AtomicU64::new(0));
    let mut out = build_pipeline_with_replication(
        TestApp::new(),
        writer,
        Duration::ZERO,
        active_conns,
        false,
        MAX_JOURNAL_BATCH,
        REPLICATION_RING_CAPACITY,
        false,
        false,
        false,
        Arc::new(crate::fence::FenceState::new(0)),
    );
    let mut input_producer = out.input_producer;
    let journal_stage = out.journal_stage;
    let matching_stage = out.matching_stage;
    let journal_cursor = out.cursors.journal_ring_arc();
    let mut output_consumer = out.output_consumers.pop().unwrap();

    let shutdown = Arc::new(AtomicBool::new(false));
    let s1 = Arc::clone(&shutdown);
    let s2 = Arc::clone(&shutdown);

    let t_journal = std::thread::spawn(move || journal_stage.run(&s1));
    let t_matching = std::thread::spawn(move || matching_stage.run(&s2));

    // Primary-side producer leaves `sequence: 0`; the journal stage
    // assigns the sequence at encode time.
    input_producer.publish(add_slot(123, 1_000_000_000));

    let output = loop {
        if let Some((_, slot)) = output_consumer.try_consume() {
            break slot;
        }
        std::hint::spin_loop();
    };

    assert!(matches!(output.payload, OutputPayload::Report(_)));
    assert_eq!(output.input_seq, 0);

    // Wait for journal to confirm durability (cursor > input_seq).
    loop {
        let cursor = journal_cursor.get().load(Ordering::Acquire);
        if cursor > output.input_seq {
            break;
        }
        std::hint::spin_loop();
    }

    shutdown.store(true, Ordering::Relaxed);
    let _writer = t_journal.join().unwrap();
    let _app = t_matching.join().unwrap();

    #[cfg(not(feature = "no-persist"))]
    {
        let mut reader = JournalReader::<TestEvent>::open(&path).unwrap();
        let entry = reader.next_entry().unwrap().unwrap();
        assert!(matches!(
            entry.event,
            JournalEvent::App(TestEvent::Add(123))
        ));
    }
}

#[test]
#[cfg(not(feature = "no-persist"))]
fn journal_stage_sends_replication_batches() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("repl_pipeline.journal");

    let writer = Writer::create(&path).unwrap();
    let active_conns = Arc::new(AtomicU64::new(0));
    let mut out = build_pipeline_with_replication(
        TestApp::new(),
        writer,
        Duration::ZERO,
        active_conns,
        true,
        MAX_JOURNAL_BATCH,
        REPLICATION_RING_CAPACITY,
        false,
        false,
        false,
        Arc::new(crate::fence::FenceState::new(0)),
    );
    let mut output_consumer = out.output_consumers.pop().unwrap();

    let (mut repl_consumer, _repl_consumer_2) = out
        .replication_consumers
        .expect("replication should be enabled");

    // Mark a replica connected so the matching stage doesn't halt and
    // the journal stage publishes to replication rings.
    if let Some(ref count) = out.replicas_connected {
        count.store(1, Ordering::Relaxed);
    }
    if let Some(ref rp) = out.replication_ring_progress {
        rp.active_flags[0].store(true, Ordering::Relaxed);
    }

    let journal_stage = out.journal_stage;
    let matching_stage = out.matching_stage;
    let mut input_producer = out.input_producer;
    let journal_cursor = out.cursors.journal_ring_arc();
    let replica_slots = out.cursors.replica_slot_cursors();

    let shutdown = Arc::new(AtomicBool::new(false));
    let s1 = Arc::clone(&shutdown);
    let s2 = Arc::clone(&shutdown);

    let t_journal = std::thread::spawn(move || journal_stage.run(&s1));
    let t_matching = std::thread::spawn(move || matching_stage.run(&s2));

    input_producer.publish(add_slot(77, 1_000_000_000));

    let output = loop {
        if let Some((_, slot)) = output_consumer.try_consume() {
            break slot;
        }
        std::hint::spin_loop();
    };
    assert!(matches!(output.payload, OutputPayload::Report(_)));

    loop {
        let cursor = journal_cursor.get().load(Ordering::Acquire);
        if cursor > output.input_seq {
            break;
        }
        std::hint::spin_loop();
    }

    // The journal stage should have published a replication batch with
    // the exact same bytes it wrote to disk.
    let (repl_meta, repl_data) = loop {
        if let Some((meta, data)) = repl_consumer.try_read() {
            let data_copy = data.to_vec();
            repl_consumer.commit();
            break (meta, data_copy);
        }
        std::hint::spin_loop();
    };
    assert!(
        repl_meta.end_sequence > 0,
        "replication batch should have events"
    );
    assert!(!repl_data.is_empty(), "replication batch should have data");

    // Wire frame: [length:u32][type:0x21][count:u16][slots...]. Decode
    // and verify the slot's sequence + event match what we submitted.
    let payload_len =
        u32::from_le_bytes(repl_data[..4].try_into().expect("4-byte length prefix")) as usize;
    assert_eq!(repl_data.len(), 4 + payload_len);
    let payload = &repl_data[4..];
    let slots: Vec<TestInput> =
        crate::replication_wire::try_decode_input_batch(payload).expect("InputBatch decode");
    assert!(
        !slots.is_empty(),
        "InputBatch should carry at least one slot"
    );
    let first = &slots[0];
    assert_eq!(
        first.sequence, FIRST_SEQ,
        "first slot's sequence should match journal first user event"
    );
    assert!(matches!(first.event, JournalEvent::App(TestEvent::Add(77))));

    replica_slots.store(
        0,
        SlotAcked::from_acked(WireSeq::new(repl_meta.end_sequence)),
    );
    let journal_pos = journal_cursor.get().load(Ordering::Acquire);
    let repl_acked = replica_slots
        .quorum_acked()
        .expect("slot 0 engaged above")
        .get();
    let effective = journal_pos.min(repl_acked + 1);
    assert!(
        effective > output.input_seq,
        "both cursors should have advanced"
    );

    shutdown.store(true, Ordering::Relaxed);
    let _writer = t_journal.join().unwrap();
    let _app = t_matching.join().unwrap();
}

#[test]
fn replica_quorum_always_starts_disengaged() {
    let dir = tempfile::tempdir().unwrap();

    // Standalone mode.
    {
        let path = dir.path().join("standalone.journal");
        let writer = Writer::create(&path).unwrap();
        let active_conns = Arc::new(AtomicU64::new(0));

        let out = build_pipeline_with_replication(
            TestApp::new(),
            writer,
            Duration::ZERO,
            active_conns,
            false,
            MAX_JOURNAL_BATCH,
            REPLICATION_RING_CAPACITY,
            false,
            false,
            false,
            Arc::new(crate::fence::FenceState::new(0)),
        );
        assert!(out.replication_consumers.is_none());
        assert_eq!(out.cursors.load_replica_quorum_acked(), None);
    }

    // Replication enabled — the quorum still starts disengaged.
    {
        let path = dir.path().join("repl_enabled.journal");
        let writer = Writer::create(&path).unwrap();
        let active_conns = Arc::new(AtomicU64::new(0));

        let out = build_pipeline_with_replication(
            TestApp::new(),
            writer,
            Duration::ZERO,
            active_conns,
            true,
            MAX_JOURNAL_BATCH,
            REPLICATION_RING_CAPACITY,
            false,
            false,
            false,
            Arc::new(crate::fence::FenceState::new(0)),
        );
        assert!(out.replication_consumers.is_some());
        assert_eq!(
            out.cursors.load_replica_quorum_acked(),
            None,
            "replica quorum should start disengaged even when replication is enabled"
        );
    }
}

/// High-volume soak: a large multi-batch run through the journal stage
/// must produce a journal whose user sequences are dense — no gaps, no
/// duplicates — when scanned back. (Historically this guarded against
/// in-stream Checkpoint entries colliding with user sequences; those
/// entries no longer exist, but the dense-sequence invariant remains
/// the property `journal_verify` audits in production.)
#[cfg(all(feature = "hash-chain", not(feature = "no-persist")))]
#[test]
fn primary_journal_sequences_contiguous_across_many_batches() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("many_batches.journal");
    let writer = Writer::create(&path).unwrap();

    let total: u64 = 200_100;
    let cap = ((total as usize) + MAX_JOURNAL_BATCH).next_power_of_two();
    let (mut producer, mut consumers) = ring::DisruptorBuilder::<TestInput>::new(cap)
        .add_consumer()
        .build();
    let consumer = consumers.pop().unwrap();

    let stage = JournalStage::new(writer, consumer, Duration::ZERO, MAX_JOURNAL_BATCH, false);
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown2 = Arc::clone(&shutdown);
    let handle = std::thread::spawn(move || stage.run(&shutdown2));

    for i in 0..total {
        producer.publish(add_slot(i + 1, 1_000_000_000 + i));
    }

    std::thread::sleep(Duration::from_millis(1000));
    shutdown.store(true, Ordering::Relaxed);
    let _writer = handle.join().unwrap();

    let mut reader = JournalReader::<TestEvent>::open(&path).unwrap();
    let mut count = 0u64;
    loop {
        match reader.next_entry() {
            Ok(Some(_)) => count += 1,
            Ok(None) => break,
            Err(e) => {
                let dump = dump_journal_for_diagnosis(&path, "primary_journal");
                panic!(
                    "journal read error after {count} user entries \
                     (last_sequence = {:?}, valid_file_end = {}): {e}\n  \
                     raw journal copied to: {dump}",
                    reader.last_sequence(),
                    reader.valid_file_end(),
                );
            }
        }
    }
    if count != total {
        let dump = dump_journal_for_diagnosis(&path, "primary_journal_count");
        panic!(
            "expected all {total} user events to be recoverable from the journal, \
             got {count}\n  raw journal copied to: {dump}"
        );
    }
}

/// Copy a failing journal to a stable `/tmp/` path keyed by test name +
/// pid so the byte pattern at the read failure can be inspected with
/// `xxd` after the test panics. Returns the dump path; on any I/O error
/// returns a short diagnostic instead of panicking inside a panic path.
#[cfg(all(feature = "hash-chain", not(feature = "no-persist")))]
fn dump_journal_for_diagnosis(src: &std::path::Path, label: &str) -> String {
    let pid = std::process::id();
    let dst = format!("/tmp/journal-failure-{label}-{pid}.dump");
    match std::fs::copy(src, &dst) {
        Ok(bytes) => format!("{dst} ({bytes} bytes)"),
        Err(e) => format!("<failed to copy {}: {e}>", src.display()),
    }
}

/// End-to-end primary → replica test. The primary's journal stage
/// publishes replication batches; a relay thread decodes the wire
/// frames and republishes them onto the replica's input ring. Both
/// journals must end up with contiguous app sequences covering every
/// published event — and, because the replica re-encodes the same
/// (seq, timestamp, key, payload) tuples over the same anchor, the two
/// journals must be chain-identical (the bitwise-mirror property).
///
/// Scope: neither side rotates here, so both journals are single
/// segments sharing one anchor. The rotating counterpart is
/// `primary_driven_rotation_mirrors_segmentation_on_replica`, where the
/// replica adopts the primary's announced boundaries and the mirror
/// property holds per segment.
#[cfg(all(feature = "hash-chain", not(feature = "no-persist")))]
#[test]
fn primary_and_replica_journals_contiguous_and_chain_identical() {
    let dir = tempfile::tempdir().unwrap();
    let primary_path = dir.path().join("primary.journal");
    let replica_path = dir.path().join("replica.journal");

    // Shared anchor so the two writers seed identical BLAKE3 chains.
    // In production the replica gets this via the bootstrap handshake
    // (the primary ships its live segment's header info).
    let shared_anchor = [0xA5u8; 32];

    // -------- primary --------
    let primary_writer = Writer::create_continuing(&primary_path, 1, shared_anchor).unwrap();
    let primary_active_conns = Arc::new(AtomicU64::new(0));
    let mut primary = build_pipeline_with_replication(
        TestApp::new(),
        primary_writer,
        Duration::ZERO,
        primary_active_conns,
        true,
        MAX_JOURNAL_BATCH,
        REPLICATION_RING_CAPACITY,
        false,
        false,
        false,
        Arc::new(crate::fence::FenceState::new(0)),
    );

    // -------- replica --------
    let replica_writer = Writer::create_continuing(&replica_path, 1, shared_anchor).unwrap();
    let replica = build_replica_pipeline(
        TestApp::new(),
        replica_writer,
        MAX_JOURNAL_BATCH,
        Duration::ZERO,
        false,
        false,
        Arc::new(crate::fence::FenceState::new(0)),
    );

    // Mark a replica as connected so the primary doesn't halt and
    // its journal stage actually publishes to the replication ring.
    if let Some(ref count) = primary.replicas_connected {
        count.store(1, Ordering::Relaxed);
    }
    if let Some(ref rp) = primary.replication_ring_progress {
        rp.active_flags[0].store(true, Ordering::Relaxed);
    }

    let (mut repl_c0, mut repl_c1) = primary.replication_consumers.expect("replication enabled");
    let mut replica_input = replica.input_producer;

    let primary_shutdown = Arc::new(AtomicBool::new(false));
    let replica_shutdown = Arc::new(AtomicBool::new(false));
    let relay_shutdown = Arc::new(AtomicBool::new(false));

    // --- relay thread: pump primary replication ring → replica input ring ---
    let relay_stop = Arc::clone(&relay_shutdown);
    let t_relay = std::thread::spawn(move || {
        loop {
            let mut got_something = false;
            if let Some((_meta, data)) = repl_c0.try_read() {
                let payload_len =
                    u32::from_le_bytes(data[..4].try_into().expect("4-byte length prefix"))
                        as usize;
                let payload = &data[4..4 + payload_len];
                let slots: Vec<TestInput> =
                    crate::replication_wire::try_decode_input_batch(payload)
                        .expect("relay InputBatch decode");
                for slot in slots {
                    replica_input.publish(InputSlot {
                        connection_id: 0,
                        key_hash: slot.key_hash,
                        request_seq: slot.request_seq,
                        sequence: slot.sequence,
                        timestamp_ns: slot.timestamp_ns,
                        event: slot.event,
                        publish_ts: mono_trace_ns(),
                        recv_ts: mono_trace_ns(),
                    });
                }
                repl_c0.commit();
                got_something = true;
            }
            if repl_c1.try_read().is_some() {
                repl_c1.commit();
                got_something = true;
            }
            if !got_something {
                if relay_stop.load(Ordering::Relaxed) {
                    return;
                }
                std::hint::spin_loop();
            }
        }
    });

    // --- primary + replica pipeline threads ---
    let mut primary_output = primary.output_consumers.pop().unwrap();
    let primary_out_shutdown = Arc::new(AtomicBool::new(false));
    let primary_out_stop = Arc::clone(&primary_out_shutdown);
    let t_primary_out = std::thread::spawn(move || {
        while !primary_out_stop.load(Ordering::Relaxed) {
            if primary_output.try_consume().is_some() {
                continue;
            }
            std::hint::spin_loop();
        }
    });

    let mut replica_drain = replica.drain_consumer;
    let replica_drain_stop = Arc::new(AtomicBool::new(false));
    let replica_drain_stop2 = Arc::clone(&replica_drain_stop);
    let t_replica_drain = std::thread::spawn(move || {
        while !replica_drain_stop2.load(Ordering::Relaxed) {
            if replica_drain.try_consume().is_some() {
                continue;
            }
            std::hint::spin_loop();
        }
    });

    let p_j_stop = Arc::clone(&primary_shutdown);
    let p_m_stop = Arc::clone(&primary_shutdown);
    let t_p_journal = std::thread::spawn(move || primary.journal_stage.run(&p_j_stop));
    let t_p_matching = std::thread::spawn(move || primary.matching_stage.run(&p_m_stop));

    let r_j_stop = Arc::clone(&replica_shutdown);
    let r_m_stop = Arc::clone(&replica_shutdown);
    let t_r_journal = std::thread::spawn(move || replica.journal_stage.run(&r_j_stop));
    let t_r_matching = std::thread::spawn(move || replica.matching_stage.run(&r_m_stop));

    // Enough events to span many fsync batches and replication frames.
    let total: u64 = 50_250;
    for i in 0..total {
        primary
            .input_producer
            .publish(add_slot(i + 1, 1_000_000_000 + i));
    }

    std::thread::sleep(Duration::from_millis(3000));

    primary_shutdown.store(true, Ordering::Relaxed);
    let primary_journal_result = t_p_journal.join().unwrap();
    let _ = t_p_matching.join().unwrap();
    relay_shutdown.store(true, Ordering::Relaxed);
    let _ = t_relay.join();
    std::thread::sleep(Duration::from_millis(500));
    replica_shutdown.store(true, Ordering::Relaxed);
    let replica_journal_result = t_r_journal.join().unwrap();
    let _ = t_r_matching.join().unwrap();
    primary_journal_result.expect("primary journal stage must exit cleanly");
    replica_journal_result.expect("replica journal stage must exit cleanly");
    primary_out_shutdown.store(true, Ordering::Relaxed);
    let _ = t_primary_out.join();
    replica_drain_stop.store(true, Ordering::Relaxed);
    let _ = t_replica_drain.join();

    let scan = |label: &str, path: &std::path::Path| -> (u64, Option<[u8; 32]>) {
        let mut reader = JournalReader::<TestEvent>::open(path).unwrap();
        let mut count = 0u64;
        loop {
            match reader.next_entry() {
                Ok(Some(_)) => count += 1,
                Ok(None) => break,
                Err(e) => {
                    let dump = dump_journal_for_diagnosis(path, label);
                    panic!(
                        "{label} journal read error after {count} user entries \
                         (last_sequence = {:?}, valid_file_end = {}): {e}\n  \
                         raw journal copied to: {dump}",
                        reader.last_sequence(),
                        reader.valid_file_end(),
                    );
                }
            }
        }
        (count, reader.chain_hash())
    };

    let (primary_count, primary_chain) = scan("primary", &primary_path);
    let (replica_count, replica_chain) = scan("replica", &replica_path);
    if primary_count != total {
        let dump = dump_journal_for_diagnosis(&primary_path, "primary_count");
        panic!(
            "expected all {total} user events recoverable from the primary journal, \
             got {primary_count}\n  raw journal copied to: {dump}"
        );
    }
    if replica_count != total {
        let dump = dump_journal_for_diagnosis(&replica_path, "replica_count");
        panic!(
            "expected all {total} user events recoverable from the replica journal, \
             got {replica_count}\n  raw journal copied to: {dump}"
        );
    }

    assert_eq!(
        primary_count, total,
        "expected all {total} user events recoverable from the primary journal"
    );
    assert_eq!(
        replica_count, total,
        "expected all {total} user events recoverable from the replica journal"
    );

    // Bitwise-mirror property: same anchor + same entry bytes ⇒ same
    // chain value. This is the invariant divergence detection rests on.
    assert_eq!(
        primary_chain.expect("hash-chain enabled"),
        replica_chain.expect("hash-chain enabled"),
        "replica journal must be chain-identical to the primary's"
    );
}

/// Manual-rotation path: setting the operator flag rotates the live
/// journal at the next fsync boundary. (1) Both pre- and post-rotation
/// events end up in their respective segments, (2) the live segment
/// continues taking new events, (3) full recovery via `JournaledApp`
/// walks archive + live and reproduces the cumulative state.
#[cfg(not(feature = "no-persist"))]
#[test]
fn journal_stage_rotates_on_manual_request() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rotate_manual.journal");
    let writer = Writer::create(&path).unwrap();

    let (mut producer, mut consumers) = ring::DisruptorBuilder::<TestInput>::new(64)
        .add_consumer()
        .build();
    let consumer = consumers.pop().unwrap();

    let mut stage = JournalStage::new(writer, consumer, Duration::ZERO, MAX_JOURNAL_BATCH, false);
    let rotate_flag = Arc::new(AtomicBool::new(false));
    stage.set_rotation(
        /* max_journal_bytes */ 0,
        Some(Arc::clone(&rotate_flag)),
    );

    let shutdown = Arc::new(AtomicBool::new(false));
    let s = Arc::clone(&shutdown);
    let handle = std::thread::spawn(move || stage.run(&s));

    // Publish an Add event with a unique request_seq so every event
    // survives dedup at recovery time.
    let mut req_seq: u64 = 0;
    let mut publish_add = |amount: u64| {
        req_seq += 1;
        producer.publish(InputSlot {
            connection_id: 1,
            key_hash: 1,
            request_seq: req_seq,
            sequence: 0,
            timestamp_ns: 1_000_000_000 + req_seq,
            event: JournalEvent::App(TestEvent::Add(amount)),
            publish_ts: mono_trace_ns(),
            recv_ts: mono_trace_ns(),
        });
    };

    publish_add(100);
    publish_add(200);

    // Wait until phase-1 events are fsynced into the live segment so
    // the archive captures them. Polled rather than fixed-sleep so a
    // slow CI machine doesn't intermittently rotate early.
    let archive_path = std::path::PathBuf::from(format!("{}.000001", path.display()));
    let pre_size_path = path.clone();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline
        && (!pre_size_path.exists()
            || std::fs::metadata(&pre_size_path)
                .map(|m| m.len())
                .unwrap_or(0)
                < 4096)
    {
        std::thread::sleep(Duration::from_millis(20));
    }

    rotate_flag.store(true, Ordering::Release);

    // A third event after the flag — fsyncing it drives the journal
    // stage past a `maybe_rotate` boundary.
    publish_add(50);

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !archive_path.exists() && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        archive_path.exists(),
        "archive {} should exist after manual rotation",
        archive_path.display()
    );

    // Post-rotation event must land in the live (post-rotation) segment.
    publish_add(1000);

    std::thread::sleep(Duration::from_millis(150));

    shutdown.store(true, Ordering::Relaxed);
    let _writer = handle.join().unwrap();

    // Recovery via the multi-segment walker should produce a TestApp
    // with total = 100 + 200 + 50 + 1000 = 1350.
    let recovered =
        JournaledApp::<TestApp, BufferedWriter<TestEvent>>::recover(TestApp::new(), &path).unwrap();
    assert_eq!(
        recovered.app().total,
        1350,
        "all Adds across the rotation must replay"
    );
}

/// Replica-side primary-driven rotation: a rotation queued between
/// sequences 2 and 3 must split the encode batch at exactly that
/// boundary — entries 1..=2 land in the archived segment, entry 3 in a
/// fresh live segment whose header anchor is the announced tail hash.
/// Single-threaded and sentinel-driven, so the mid-batch barrier path
/// is exercised deterministically.
#[cfg(all(feature = "hash-chain", not(feature = "no-persist")))]
#[test]
fn adopted_rotation_splits_batch_at_announced_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("adopt_split.journal");
    let anchor = [0x7Au8; 32];

    // Reference writer: computes the chain tail at the boundary the
    // primary would announce. Same anchor + same entry tuples ⇒ the
    // replica's local tail must equal it.
    let tail_at_2 = {
        let ref_path = dir.path().join("reference.journal");
        let mut w = Writer::create_continuing(&ref_path, 1, anchor).unwrap();
        for seq in 1..=2u64 {
            assert_eq!(w.allocate_sequence(), seq);
            w.encode_event(
                seq,
                1_000_000_000 + seq,
                &JournalEvent::App(TestEvent::Add(seq)),
                0,
                0,
            )
            .unwrap();
        }
        w.flush_batch_sync().unwrap();
        w.chain_hash().expect("hash-chain enabled")
    };

    let writer = Writer::create_continuing(&path, 1, anchor).unwrap();
    let (mut producer, mut consumers) = ring::DisruptorBuilder::<TestInput>::new(64)
        .add_consumer()
        .build();
    let consumer = consumers.pop().unwrap();
    let mut stage = JournalStage::new(writer, consumer, Duration::ZERO, MAX_JOURNAL_BATCH, false);

    let rotations: crate::pipeline::StreamMarkQueue =
        Arc::new(std::sync::Mutex::new(std::collections::VecDeque::new()));
    rotations
        .lock()
        .unwrap()
        .push_back(StreamMark::Rotate(AdoptedRotation {
            boundary_seq: 2,
            tail_hash: tail_at_2,
        }));
    stage.set_stream_marks(Arc::clone(&rotations));

    // Everything pre-published, sentinel last: one read_batch spans the
    // boundary, forcing the split + inline flush + adoption.
    for seq in 1..=3u64 {
        producer.publish(add_slot_with_seq(seq, seq, 1_000_000_000 + seq));
    }
    producer.publish(TestInput::shutdown_sentinel());

    let shutdown = AtomicBool::new(false);
    let writer = stage.run(&shutdown).expect("clean exit via sentinel");

    let archive = std::path::PathBuf::from(format!("{}.000001", path.display()));
    assert!(
        archive.exists(),
        "adopted rotation must archive the outgoing segment"
    );
    let arch_info = melin_journal::segment::read_header_info(&archive).unwrap();
    assert_eq!(arch_info.starting_sequence, 1);
    assert_eq!(arch_info.anchor_hash, anchor);

    let live_info = melin_journal::segment::read_header_info(&path).unwrap();
    assert_eq!(
        live_info.starting_sequence, 3,
        "live continues past the boundary"
    );
    assert_eq!(
        live_info.anchor_hash, tail_at_2,
        "live segment must be anchored at the announced (verified) tail"
    );
    assert_eq!(writer.next_sequence(), 4, "rotation consumes no sequence");

    // Entry placement: 1..=2 archived, 3 live.
    let mut arch_reader = JournalReader::<TestEvent>::open(&archive).unwrap();
    let mut archived_seqs = Vec::new();
    while let Some(e) = arch_reader.next_entry().unwrap() {
        archived_seqs.push(e.sequence);
    }
    assert_eq!(archived_seqs, vec![1, 2]);
    let mut live_reader = JournalReader::<TestEvent>::open(&path).unwrap();
    let mut live_seqs = Vec::new();
    while let Some(e) = live_reader.next_entry().unwrap() {
        live_seqs.push(e.sequence);
    }
    assert_eq!(live_seqs, vec![3]);
}

/// The input-ring progress published at a mid-batch mark barrier must
/// cover only the entries actually encoded, never the whole read batch.
///
/// This is persist-before-ack on a replica. The barrier hands over a
/// *prefix* of the read batch — everything up to the announced boundary
/// — while the tail sits unencoded until the rotation completes.
/// Publishing `next_read` there would advance the cursor the ack path
/// gates on past entries the journal has not taken, letting the replica
/// acknowledge data it has not persisted. Segment placement stays
/// correct either way, which is why the existing adoption tests cannot
/// see this: the entries still land in the right files, the *cursor*
/// just lies about them.
///
/// Driven with a deliberately wrong tail hash so the stage stops at the
/// barrier with a divergence error, freezing the cursor at exactly the
/// value the barrier published.
#[cfg(all(feature = "hash-chain", not(feature = "no-persist")))]
#[test]
fn mid_batch_barrier_commits_only_the_encoded_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("barrier_progress.journal");
    let anchor = [0x5Cu8; 32];

    let writer = Writer::create_continuing(&path, 1, anchor).unwrap();
    let (mut producer, mut consumers) = ring::DisruptorBuilder::<TestInput>::new(64)
        .add_consumer()
        .build();
    let consumer = consumers.pop().unwrap();
    // The cursor the ack path reads. Captured before the stage takes
    // the consumer — the disk thread publishes into this same counter.
    let progress = consumer.progress_counter();

    let mut stage = JournalStage::new(writer, consumer, Duration::ZERO, MAX_JOURNAL_BATCH, false);
    let marks: crate::pipeline::StreamMarkQueue =
        Arc::new(std::sync::Mutex::new(std::collections::VecDeque::new()));
    marks
        .lock()
        .unwrap()
        .push_back(StreamMark::Rotate(AdoptedRotation {
            boundary_seq: 2,
            // Not the local tail: the barrier submits the prefix, then
            // the quiesced resolve rejects the rotation and the stage
            // exits, leaving the cursor where the barrier put it.
            tail_hash: [0xEEu8; 32],
        }));
    stage.set_stream_marks(Arc::clone(&marks));

    // All three pre-published, so one `read_batch` spans the boundary
    // and the split is forced.
    for seq in 1..=3u64 {
        producer.publish(add_slot_with_seq(seq, seq, 1_000_000_000 + seq));
    }

    let shutdown = AtomicBool::new(false);
    // `expect_err` needs the Ok type to be Debug, and the writer is not.
    let err = match stage.run(&shutdown) {
        Err(e) => e,
        Ok(_) => panic!("a wrong tail hash must be reported as divergence"),
    };
    assert!(
        matches!(
            err,
            melin_journal::JournalError::ReplicaChainDivergence { sequence: 2, .. }
        ),
        "expected divergence at the boundary, got: {err}"
    );

    // Ring slots 0 and 1 hold seqs 1 and 2 — the boundary. Slot 2
    // (seq 3) sits past it and was never encoded.
    assert_eq!(
        progress.get().load(Ordering::Acquire),
        2,
        "progress must stop at the boundary; publishing the whole read \
         batch would let the replica ack seq 3, which was never journaled"
    );

    // Corroborate from the journal itself: the cursor claims two
    // entries are durable, and exactly two are.
    let mut reader = JournalReader::<TestEvent>::open(&path).unwrap();
    let mut seqs = Vec::new();
    while let Some(e) = reader.next_entry().unwrap() {
        seqs.push(e.sequence);
    }
    assert_eq!(seqs, vec![1, 2], "only the encoded prefix may be on disk");
}

/// Regression: the shadow-snapshot double-apply window.
///
/// The `FsyncState` pair a mid-batch mark barrier publishes must be
/// self-consistent — `input_ring_seq` at the encoded prefix, the same
/// position as ring progress, not the read cursor. It used to be the
/// read cursor, and two facts then combined into a wrong snapshot:
///
/// (a) the barrier's pair claimed a ring position one whole tail past
///     what its `journal_seq` covered;
/// (b) `DurabilityCursors::publish` stores ring progress *before* the
///     seqlock, so when the tail batch became durable there was a window
///     in which progress already read the tail's end while `FsyncState`
///     still held the barrier's stale pair.
///
/// Inside that window the shadow — gated on journal progress — consumed
/// through the tail, landed exactly on the stale `input_ring_seq`,
/// passed the exact-equality alignment gate, and persisted a snapshot
/// whose state held the tail but whose header resumed recovery from the
/// prefix — replaying the tail onto itself.
///
/// Fixture: same divergence freeze as
/// `mid_batch_barrier_commits_only_the_encoded_prefix` — the stage
/// stops with exactly the barrier's pair published through the real
/// disk thread. Fact (b) is then reconstructed by hand: the first store
/// of `publish` (progress → tail end) is performed, the second (the
/// seqlock) is not. The real shadow stage runs against the same ring,
/// in the window, and must NOT snapshot: it sits at the tail's end,
/// which the stale pair — now consistent — no longer claims.
#[cfg(all(feature = "hash-chain", not(feature = "no-persist")))]
#[test]
fn barrier_fsync_state_pair_is_self_consistent_for_the_shadow() {
    use crate::pipeline::FsyncState;
    use crate::{shadow, snapshot};

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("shadow_window.journal");
    let snap_path = dir.path().join("shadow_window.snapshot");
    let anchor = [0x5Cu8; 32];

    let writer = Writer::create_continuing(&path, 1, anchor).unwrap();
    // journal(0), shadow(1) gated on journal — the production wiring
    // (`build_input_disruptor`) minus the matching stage.
    let (mut producer, mut consumers) = ring::DisruptorBuilder::<TestInput>::new(64)
        .add_consumer()
        .add_consumer_after(0)
        .build();
    let shadow_consumer = consumers.pop().unwrap();
    let journal_consumer = consumers.pop().unwrap();
    let progress = journal_consumer.progress_counter();
    // Lets the test observe how far the shadow has consumed.
    let shadow_progress = shadow_consumer.progress_counter();

    let mut stage = JournalStage::new(
        writer,
        journal_consumer,
        Duration::ZERO,
        MAX_JOURNAL_BATCH,
        false,
    );
    let (fsync_writer, fsync_reader) = melin_pipeline::seqlock::split(FsyncState::default());
    stage.set_chain_hash_lock(fsync_writer);
    let marks: crate::pipeline::StreamMarkQueue =
        Arc::new(std::sync::Mutex::new(std::collections::VecDeque::new()));
    marks
        .lock()
        .unwrap()
        .push_back(StreamMark::Rotate(AdoptedRotation {
            boundary_seq: 2,
            // Wrong tail: barrier submits the prefix, then the quiesced
            // resolve rejects the rotation and the stage exits with the
            // barrier's pair as the last thing published.
            tail_hash: [0xEEu8; 32],
        }));
    stage.set_stream_marks(Arc::clone(&marks));

    // One read batch spanning the boundary: seqs 1,2 | 3.
    for seq in 1..=3u64 {
        producer.publish(add_slot_with_seq(seq, seq, 1_000_000_000 + seq));
    }
    let shutdown = AtomicBool::new(false);
    match stage.run(&shutdown) {
        Err(melin_journal::JournalError::ReplicaChainDivergence { sequence: 2, .. }) => {}
        Err(e) => panic!("expected divergence at the boundary, got: {e}"),
        Ok(_) => panic!("a wrong tail hash must be reported as divergence"),
    }

    // The pair the barrier published: both halves at the encoded
    // prefix (ring slots 0,1 = seqs 1,2), never the read cursor (3).
    let barrier = fsync_reader.load();
    assert_eq!(progress.get().load(Ordering::Acquire), 2, "prefix progress");
    assert_eq!(
        barrier.journal_seq.get(),
        2,
        "journal_seq covers the prefix"
    );
    assert_eq!(
        barrier.input_ring_seq.get(),
        2,
        "input_ring_seq must be the position journal_seq covers, not the read cursor"
    );

    // The real shadow stage against the same ring. Gated on progress
    // (2), it consumes seqs 1,2, aligns with the pair, and snapshots
    // the correct (state, header) — the positive half of the contract.
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown2 = Arc::clone(&shutdown);
    let snap_path2 = snap_path.clone();
    let handle = std::thread::Builder::new()
        .name("test-shadow-window".into())
        .spawn(move || {
            shadow::run(
                shadow_consumer,
                TestApp::new(),
                snap_path2,
                Duration::from_millis(20),
                fsync_reader,
                &shutdown2,
                false,
                0,
            );
        })
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !snap_path.exists() && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(snap_path.exists(), "the aligned shadow must snapshot");
    let (restored, journal_seq, _, _) = snapshot::load::<TestApp>(&snap_path).unwrap();
    assert_eq!(
        restored.total, 3,
        "state = Add(1)+Add(2), exactly the prefix"
    );
    assert_eq!(journal_seq, 2);

    // Now the window: the disk thread, publishing the tail batch, stores
    // progress first and the seqlock second. Freeze it between the two.
    // The shadow consumes seq 3 (state = 6) and sits at next_read == 3
    // beside a stale pair claiming 2 — the gate must reject, over
    // several timer intervals, and the file must stay the prefix
    // snapshot. Before the fix the stale pair claimed 3 and this is
    // where the shadow wrote (total 6, journal_seq 2).
    progress.get().store(3, Ordering::Release);
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while shadow_progress.get().load(Ordering::Acquire) < 3 {
        assert!(
            std::time::Instant::now() < deadline,
            "shadow must consume the tail once progress reaches it"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    // Several 20 ms snapshot intervals with the shadow parked at 3.
    std::thread::sleep(Duration::from_millis(200));
    shutdown.store(true, Ordering::Relaxed);
    handle.join().unwrap();

    let (restored, journal_seq, _, _) = snapshot::load::<TestApp>(&snap_path).unwrap();
    assert_eq!(
        (restored.total, journal_seq),
        (3, 2),
        "a stale-but-consistent pair must not let the shadow snapshot past its journal_seq"
    );
}

/// Invariant sweep for the shadow's (journal_seq, input_ring_seq,
/// chain_hash) triple across a replica run with several adopted
/// rotations, real disk thread, real shadow, events arriving in bursts
/// so batch edges fall wherever they fall relative to the marks.
///
/// Two independent nets, neither of which depends on hitting a timing
/// window:
///
/// 1. A sampler spins on the `FsyncState` seqlock for the whole run
///    and records every distinct pair it sees. Every ring slot `i`
///    holds `Add(i + 1)` (sentinel last), so a pair is consistent iff
///    `journal_seq == min(input_ring_seq, N)`. Any batch — steady
///    state, mid-batch barrier, batch-end mark, shutdown drain — that
///    published a pair covering different prefixes fails here, no
///    matter how briefly it was published (a pair stays visible until
///    the next batch is durable, ≥ one fsync).
/// 2. The shadow snapshots on every aligned landing (interval zero).
///    Every distinct snapshot the test manages to copy is recovered
///    with `JournaledApp::recover_from_snapshot` against the real
///    multi-segment journal: the header's `journal_seq` and
///    `chain_hash` must let replay reach exactly the full-run total,
///    whatever point the snapshot was taken at.
#[cfg(all(feature = "hash-chain", not(feature = "no-persist")))]
#[test]
fn fsync_state_pairs_stay_consistent_across_adopted_rotations() {
    use crate::pipeline::FsyncState;
    use crate::{shadow, snapshot};
    use std::collections::{BTreeMap, BTreeSet};

    const N: u64 = 40;
    // Mixed placement: early in the first burst, at a burst edge, and
    // deep inside later bursts.
    const BOUNDARIES: [u64; 4] = [3, 5, 12, 25];
    const BURSTS: [std::ops::RangeInclusive<u64>; 4] = [1..=5, 6..=10, 11..=20, 21..=N];
    let anchor = [0x3Du8; 32];
    let dir = tempfile::tempdir().unwrap();

    // Reference chain tails at each announced boundary; the reference
    // rotates where the primary would, so its chain is comparable.
    let tails: Vec<[u8; 32]> = {
        let ref_dir = dir.path().join("reference");
        std::fs::create_dir(&ref_dir).unwrap();
        let mut w =
            Writer::create_continuing(&ref_dir.join("reference.journal"), 1, anchor).unwrap();
        let mut tails = Vec::new();
        for seq in 1..=N {
            assert_eq!(w.allocate_sequence(), seq);
            w.encode_event(
                seq,
                1_000_000_000 + seq,
                &JournalEvent::App(TestEvent::Add(seq)),
                0,
                0,
            )
            .unwrap();
            if BOUNDARIES.contains(&seq) {
                w.flush_batch_sync().unwrap();
                tails.push(w.chain_hash().expect("hash-chain enabled"));
                w.rotate_segment().unwrap();
            }
        }
        w.flush_batch_sync().unwrap();
        tails
    };

    let path = dir.path().join("sweep.journal");
    let snap_path = dir.path().join("sweep.snapshot");
    let writer = Writer::create_continuing(&path, 1, anchor).unwrap();
    let (mut producer, mut consumers) = ring::DisruptorBuilder::<TestInput>::new(64)
        .add_consumer()
        .add_consumer_after(0)
        .build();
    let shadow_consumer = consumers.pop().unwrap();
    let journal_consumer = consumers.pop().unwrap();
    let shadow_progress = shadow_consumer.progress_counter();

    let mut stage = JournalStage::new(
        writer,
        journal_consumer,
        Duration::ZERO,
        MAX_JOURNAL_BATCH,
        false,
    );
    let (fsync_writer, fsync_reader) = melin_pipeline::seqlock::split(FsyncState::default());
    stage.set_chain_hash_lock(fsync_writer);
    let marks: crate::pipeline::StreamMarkQueue =
        Arc::new(std::sync::Mutex::new(std::collections::VecDeque::new()));
    for (boundary_seq, tail_hash) in BOUNDARIES.into_iter().zip(tails) {
        marks
            .lock()
            .unwrap()
            .push_back(StreamMark::Rotate(AdoptedRotation {
                boundary_seq,
                tail_hash,
            }));
    }
    stage.set_stream_marks(Arc::clone(&marks));

    // Net 1: the sampler.
    let stop = Arc::new(AtomicBool::new(false));
    let sampler = {
        let reader = fsync_reader.clone();
        let stop = Arc::clone(&stop);
        std::thread::Builder::new()
            .name("test-fsync-sampler".into())
            .spawn(move || {
                // BTreeSet: small, and sorted output reads well on failure.
                let mut seen: BTreeSet<(u64, u64)> = BTreeSet::new();
                while !stop.load(Ordering::Relaxed) {
                    let s = reader.load();
                    seen.insert((s.journal_seq.get(), s.input_ring_seq.get()));
                    std::thread::yield_now();
                }
                seen
            })
            .unwrap()
    };

    // Net 2: the shadow, snapshotting on every aligned landing.
    let shadow_shutdown = Arc::new(AtomicBool::new(false));
    let shadow_handle = {
        let shutdown = Arc::clone(&shadow_shutdown);
        let snap_path = snap_path.clone();
        std::thread::Builder::new()
            .name("test-shadow-sweep".into())
            .spawn(move || {
                shadow::run(
                    shadow_consumer,
                    TestApp::new(),
                    snap_path,
                    Duration::ZERO,
                    fsync_reader,
                    &shutdown,
                    false,
                    0,
                );
            })
            .unwrap()
    };
    // Snapshot copies keyed by header journal_seq. Best-effort sampling:
    // each `save` is tmp-write + rename, so any file we open is
    // complete, but between its two renames (`path` → `.prev`, then
    // `.tmp` → `path`) there is briefly no file at `path` at all — a
    // NotFound here just means "try again next round".
    let mut snapshots: BTreeMap<u64, std::path::PathBuf> = BTreeMap::new();
    let collect = |snapshots: &mut BTreeMap<u64, std::path::PathBuf>| {
        if let Ok((_, seq, _, _)) = snapshot::load::<TestApp>(&snap_path)
            && !snapshots.contains_key(&seq)
        {
            let copy = dir.path().join(format!("snap-{seq}.snapshot"));
            // The shadow may replace the file between our load and copy;
            // a copy of a *newer* complete snapshot is still a valid
            // sample, keyed by whatever it turns out to hold.
            match std::fs::copy(&snap_path, &copy) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
                Err(e) => panic!("copying the shadow snapshot: {e}"),
            }
            let (_, actual, _, _) = snapshot::load::<TestApp>(&copy).unwrap();
            snapshots.insert(actual, copy);
        }
    };

    let stage_shutdown = Arc::new(AtomicBool::new(false));
    let stage_handle = {
        let shutdown = Arc::clone(&stage_shutdown);
        std::thread::Builder::new()
            .name("test-journal-sweep".into())
            .spawn(move || stage.run(&shutdown))
            .unwrap()
    };
    for burst in BURSTS {
        for seq in burst {
            producer.publish(add_slot_with_seq(seq, seq, 1_000_000_000 + seq));
        }
        for _ in 0..4 {
            std::thread::sleep(Duration::from_millis(5));
            collect(&mut snapshots);
        }
    }
    producer.publish(TestInput::shutdown_sentinel());
    let writer = stage_handle
        .join()
        .unwrap()
        .expect("every announced tail matches the reference chain");
    assert_eq!(writer.next_sequence(), N + 1);

    // Let the shadow reach the last event and write its final snapshot,
    // sampling copies along the way. (The sentinel slot itself is never
    // released by progress when it arrives with nothing pending, so the
    // shadow ends at `N`.)
    let deadline = std::time::Instant::now() + DRAIN_TIMEOUT;
    loop {
        collect(&mut snapshots);
        if snapshots.contains_key(&N) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "shadow must snapshot at seq {N}; shadow progress {}, snapshots seen at {:?}",
            shadow_progress.get().load(Ordering::Acquire),
            snapshots.keys().collect::<Vec<_>>()
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    shadow_shutdown.store(true, Ordering::Relaxed);
    shadow_handle.join().unwrap();
    stop.store(true, Ordering::Relaxed);
    let seen = sampler.join().unwrap();

    // Net 1: every published pair describes one prefix.
    assert!(
        seen.len() >= 2,
        "sampler must have observed several distinct pairs, saw {seen:?}"
    );
    for &(journal_seq, ring_pos) in &seen {
        assert_eq!(
            journal_seq,
            ring_pos.min(N),
            "FsyncState pair (journal_seq {journal_seq}, input_ring_seq {ring_pos}) covers \
             two different prefixes; all pairs seen: {seen:?}"
        );
    }

    // Net 2: snapshot + replay ≡ full replay, for every snapshot taken.
    let full_total: u64 = N * (N + 1) / 2;
    for (seq, copy) in &snapshots {
        let recovered = JournaledApp::<TestApp, Writer>::recover_from_snapshot(copy, &path)
            .unwrap_or_else(|e| {
                panic!("snapshot at seq {seq} must recover against the journal: {e}")
            });
        assert_eq!(
            recovered.app().total,
            full_total,
            "snapshot at seq {seq} + replay must reach the full-run total"
        );
    }
}

/// Regression: a `Rotate` with a trailing `ChainCheck` queued inside
/// the SAME read batch. After adopting the rotation mid-batch, the
/// encode loop must re-bound the remaining span at the chain check
/// instead of encoding past it — which surfaced as a fatal
/// "position already passed" ordering error and killed the replica.
#[cfg(all(feature = "hash-chain", not(feature = "no-persist")))]
#[test]
fn adopted_rotation_honors_second_mark_in_same_batch() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("adopt_two_marks.journal");
    let anchor = [0x7Au8; 32];

    // Reference writer: chain tails at the rotation boundary (2) and at
    // the trailing chain check (4). The primary that announces
    // ChainCheck(4) has itself rotated at 2, so the reference must
    // rotate too for its chain at 4 to be comparable.
    let (tail_at_2, tail_at_4) = {
        let ref_path = dir.path().join("reference.journal");
        let mut w = Writer::create_continuing(&ref_path, 1, anchor).unwrap();
        let mut tail_at_2 = [0u8; 32];
        for seq in 1..=4u64 {
            assert_eq!(w.allocate_sequence(), seq);
            w.encode_event(
                seq,
                1_000_000_000 + seq,
                &JournalEvent::App(TestEvent::Add(seq)),
                0,
                0,
            )
            .unwrap();
            if seq == 2 {
                w.flush_batch_sync().unwrap();
                tail_at_2 = w.chain_hash().expect("hash-chain enabled");
                w.rotate_segment().unwrap();
            }
        }
        w.flush_batch_sync().unwrap();
        (tail_at_2, w.chain_hash().expect("hash-chain enabled"))
    };

    let writer = Writer::create_continuing(&path, 1, anchor).unwrap();
    let (mut producer, mut consumers) = ring::DisruptorBuilder::<TestInput>::new(64)
        .add_consumer()
        .build();
    let consumer = consumers.pop().unwrap();
    let mut stage = JournalStage::new(writer, consumer, Duration::ZERO, MAX_JOURNAL_BATCH, false);

    let marks: crate::pipeline::StreamMarkQueue =
        Arc::new(std::sync::Mutex::new(std::collections::VecDeque::new()));
    {
        let mut q = marks.lock().unwrap();
        q.push_back(StreamMark::Rotate(AdoptedRotation {
            boundary_seq: 2,
            tail_hash: tail_at_2,
        }));
        q.push_back(StreamMark::ChainCheck {
            sequence: 4,
            chain_hash: tail_at_4,
        });
    }
    stage.set_stream_marks(Arc::clone(&marks));

    // Everything pre-published, sentinel last: one read_batch spans
    // both marks.
    for seq in 1..=5u64 {
        producer.publish(add_slot_with_seq(seq, seq, 1_000_000_000 + seq));
    }
    producer.publish(TestInput::shutdown_sentinel());

    let shutdown = AtomicBool::new(false);
    let writer = stage
        .run(&shutdown)
        .expect("rotation + trailing chain check in one batch must both resolve");
    assert_eq!(writer.next_sequence(), 6);

    // Rotation at 2: entries 1..=2 archived, 3..=5 live, live anchored
    // at the boundary tail.
    let archive = std::path::PathBuf::from(format!("{}.000001", path.display()));
    assert!(
        archive.exists(),
        "rotation must archive the outgoing segment"
    );
    let live_info = melin_journal::segment::read_header_info(&path).unwrap();
    assert_eq!(live_info.starting_sequence, 3);
    assert_eq!(live_info.anchor_hash, tail_at_2);
    let mut live_reader = JournalReader::<TestEvent>::open(&path).unwrap();
    let mut live_seqs = Vec::new();
    while let Some(e) = live_reader.next_entry().unwrap() {
        live_seqs.push(e.sequence);
    }
    assert_eq!(live_seqs, vec![3, 4, 5]);
}

/// A `Rotate` announcing an all-zeros tail comes from a primary built
/// without `hash-chain` (a real BLAKE3 tail is never zeros). The
/// replica must adopt it with the chain comparison skipped — not judge
/// the mixed-feature pair divergent on every rotation.
#[cfg(all(feature = "hash-chain", not(feature = "no-persist")))]
#[test]
fn adopted_rotation_with_zero_tail_skips_chain_comparison() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("adopt_zero_tail.journal");

    let writer = Writer::create_continuing(&path, 1, [0x7Au8; 32]).unwrap();
    let (mut producer, mut consumers) = ring::DisruptorBuilder::<TestInput>::new(64)
        .add_consumer()
        .build();
    let consumer = consumers.pop().unwrap();
    let mut stage = JournalStage::new(writer, consumer, Duration::ZERO, MAX_JOURNAL_BATCH, false);

    let rotations: crate::pipeline::StreamMarkQueue =
        Arc::new(std::sync::Mutex::new(std::collections::VecDeque::new()));
    rotations
        .lock()
        .unwrap()
        .push_back(StreamMark::Rotate(AdoptedRotation {
            boundary_seq: 2,
            tail_hash: [0u8; 32], // chain-less primary's sentinel
        }));
    stage.set_stream_marks(Arc::clone(&rotations));

    for seq in 1..=3u64 {
        producer.publish(add_slot_with_seq(seq, seq, 1_000_000_000 + seq));
    }
    producer.publish(TestInput::shutdown_sentinel());

    let shutdown = AtomicBool::new(false);
    let writer = stage
        .run(&shutdown)
        .expect("zero-tail rotation must adopt, not diverge");
    assert_eq!(writer.next_sequence(), 4);
    assert!(
        std::path::PathBuf::from(format!("{}.000001", path.display())).exists(),
        "rotation must still happen"
    );
}

/// A primary-announced rotation whose tail hash disagrees with the
/// replica's local chain at the boundary is divergent history: the
/// journal stage must fail with `ReplicaChainDivergence` (tearing the
/// pipeline down for snapshot resync), and must NOT rotate or write
/// past the boundary.
#[cfg(all(feature = "hash-chain", not(feature = "no-persist")))]
#[test]
fn adopted_rotation_with_wrong_tail_hash_is_divergence() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("adopt_diverge.journal");

    let writer = Writer::create_continuing(&path, 1, [0x7Au8; 32]).unwrap();
    let (mut producer, mut consumers) = ring::DisruptorBuilder::<TestInput>::new(64)
        .add_consumer()
        .build();
    let consumer = consumers.pop().unwrap();
    let mut stage = JournalStage::new(writer, consumer, Duration::ZERO, MAX_JOURNAL_BATCH, false);

    let rotations: crate::pipeline::StreamMarkQueue =
        Arc::new(std::sync::Mutex::new(std::collections::VecDeque::new()));
    rotations
        .lock()
        .unwrap()
        .push_back(StreamMark::Rotate(AdoptedRotation {
            boundary_seq: 2,
            tail_hash: [0xBBu8; 32], // not the replica's tail at 2
        }));
    stage.set_stream_marks(Arc::clone(&rotations));

    for seq in 1..=3u64 {
        producer.publish(add_slot_with_seq(seq, seq, 1_000_000_000 + seq));
    }
    producer.publish(TestInput::shutdown_sentinel());

    let shutdown = AtomicBool::new(false);
    let err = match stage.run(&shutdown) {
        Err(e) => e,
        Ok(_) => panic!("divergent tail hash must be fatal"),
    };
    assert!(
        matches!(
            err,
            melin_journal::JournalError::ReplicaChainDivergence { sequence: 2, .. }
        ),
        "got: {err}"
    );

    // No rotation happened, and nothing past the boundary was written.
    let archive = std::path::PathBuf::from(format!("{}.000001", path.display()));
    assert!(!archive.exists(), "divergence must not rotate");
    let mut reader = JournalReader::<TestEvent>::open(&path).unwrap();
    let mut seqs = Vec::new();
    while let Some(e) = reader.next_entry().unwrap() {
        seqs.push(e.sequence);
    }
    assert_eq!(
        seqs,
        vec![1, 2],
        "entries up to the boundary are durable; nothing past it"
    );
}

/// A ChainCheck mark queued mid-batch must verify against the encoded
/// chain at exactly its position — no rotation, no flush — and a wrong
/// hash must surface as divergence.
#[cfg(all(feature = "hash-chain", not(feature = "no-persist")))]
#[test]
fn chain_check_mark_verifies_at_exact_position() {
    let anchor = [0x21u8; 32];
    let dir = tempfile::tempdir().unwrap();

    // Reference chain value at sequence 2.
    let chain_at_2 = {
        let ref_path = dir.path().join("reference.journal");
        let mut w = Writer::create_continuing(&ref_path, 1, anchor).unwrap();
        for seq in 1..=2u64 {
            assert_eq!(w.allocate_sequence(), seq);
            w.encode_event(
                seq,
                1_000_000_000 + seq,
                &JournalEvent::App(TestEvent::Add(seq)),
                0,
                0,
            )
            .unwrap();
        }
        w.flush_batch_sync().unwrap();
        w.chain_hash().expect("hash-chain enabled")
    };

    let run_with_check = |name: &str, expected: [u8; 32]| {
        let path = dir.path().join(format!("{name}.journal"));
        let writer = Writer::create_continuing(&path, 1, anchor).unwrap();
        let (mut producer, mut consumers) = ring::DisruptorBuilder::<TestInput>::new(64)
            .add_consumer()
            .build();
        let consumer = consumers.pop().unwrap();
        let mut stage =
            JournalStage::new(writer, consumer, Duration::ZERO, MAX_JOURNAL_BATCH, false);
        let marks: crate::pipeline::StreamMarkQueue =
            Arc::new(std::sync::Mutex::new(std::collections::VecDeque::new()));
        marks.lock().unwrap().push_back(StreamMark::ChainCheck {
            sequence: 2,
            chain_hash: expected,
        });
        stage.set_stream_marks(Arc::clone(&marks));
        for seq in 1..=3u64 {
            producer.publish(add_slot_with_seq(seq, seq, 1_000_000_000 + seq));
        }
        producer.publish(TestInput::shutdown_sentinel());
        let shutdown = AtomicBool::new(false);
        (path, stage.run(&shutdown))
    };

    // Truthful check: passes, no rotation.
    let (path, result) = run_with_check("ok", chain_at_2);
    assert!(result.is_ok(), "matching chain check must pass");
    assert!(
        !std::path::PathBuf::from(format!("{}.000001", path.display())).exists(),
        "a chain check must not rotate"
    );

    // Lying check: divergence at exactly the marked position.
    let (_path, result) = run_with_check("diverge", [0xEEu8; 32]);
    let err = match result {
        Err(e) => e,
        Ok(_) => panic!("mismatched chain check must be fatal"),
    };
    assert!(
        matches!(
            err,
            melin_journal::JournalError::ReplicaChainDivergence { sequence: 2, .. }
        ),
        "got: {err}"
    );
}

/// The primary emits a live-stream ChainCheck after every
/// CHAIN_CHECK_INTERVAL_BATCHES published batches, carrying its chain
/// value at the emission position. Batches are forced one-per-fsync by
/// waiting for each publish to land before sending the next slot.
#[cfg(all(feature = "hash-chain", not(feature = "no-persist")))]
#[test]
fn primary_emits_chain_check_every_interval() {
    use crate::replication::protocol::{PrimaryMessage, decode_primary_message};
    use crate::replication_wire::{MSG_INPUT_BATCH, peek_frame_tag};
    use melin_pipeline::seqlock;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("checks.journal");
    let writer = Writer::create(&path).unwrap();

    let (mut producer, mut consumers) = ring::DisruptorBuilder::<TestInput>::new(256)
        .add_consumer()
        .build();
    let consumer = consumers.pop().unwrap();
    let mut stage = JournalStage::new(writer, consumer, Duration::ZERO, MAX_JOURNAL_BATCH, false);

    let (repl_producer_0, mut repl_consumers_0) =
        melin_journal::replication::build_replication_ring(1, REPLICATION_RING_CAPACITY);
    let (repl_producer_1, _repl_consumers_1) =
        melin_journal::replication::build_replication_ring(1, REPLICATION_RING_CAPACITY);
    let evict = [
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(false)),
    ];
    let active = [
        Arc::new(AtomicBool::new(true)),
        Arc::new(AtomicBool::new(false)),
    ];
    stage.set_replication_producers(
        [repl_producer_0, repl_producer_1],
        [Arc::clone(&evict[0]), Arc::clone(&evict[1])],
        [Arc::clone(&active[0]), Arc::clone(&active[1])],
    );
    let (fsync_writer, fsync_state) = seqlock::split(crate::pipeline::FsyncState::default());
    stage.set_chain_hash_lock(fsync_writer);

    let shutdown = Arc::new(AtomicBool::new(false));
    let s = Arc::clone(&shutdown);
    let handle = std::thread::spawn(move || stage.run(&s));

    // One slot per fsync batch: wait until each lands before the next.
    let total = 70u64;
    for seq in 1..=total {
        producer.publish(add_slot(seq, 1_000_000_000 + seq));
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while fsync_state.load().journal_seq.get() < seq {
            assert!(
                std::time::Instant::now() < deadline,
                "fsync of seq {seq} never landed"
            );
            std::hint::spin_loop();
        }
    }
    let chain_at_64 = {
        // The check is emitted at the 64th batch boundary = sequence 64;
        // recompute the expected hash from the journal file.
        melin_journal::segment::chain_value_at(&path, 64).unwrap()
    };
    shutdown.store(true, Ordering::Relaxed);
    handle.join().unwrap().unwrap();

    // Classify the ring: 70 input batches and exactly one ChainCheck
    // (the 128th batch never happened), positioned after batch 64.
    let mut input_batches = 0u32;
    let mut checks = Vec::new();
    while let Some((_meta, data)) = repl_consumers_0[0].try_read() {
        if peek_frame_tag(data).unwrap() == MSG_INPUT_BATCH {
            input_batches += 1;
        } else {
            match decode_primary_message(&data[4..]).unwrap() {
                PrimaryMessage::ChainCheck {
                    sequence,
                    chain_hash,
                } => checks.push((sequence, chain_hash, input_batches)),
                other => panic!("unexpected control frame: {other:?}"),
            }
        }
        repl_consumers_0[0].commit();
    }
    assert_eq!(input_batches, total as u32);
    assert_eq!(checks.len(), 1, "exactly one check in 70 batches");
    let (sequence, chain_hash, after_batches) = checks[0];
    assert_eq!(sequence, 64);
    assert_eq!(after_batches, 64, "check rides right behind the 64th batch");
    match chain_at_64 {
        melin_journal::segment::ChainValueAt::Value(v) => assert_eq!(chain_hash, v),
        other => panic!("chain at 64 must exist: {other:?}"),
    }
}

/// End-to-end primary → replica with primary-driven rotation: the
/// primary rotates twice mid-stream (manual trigger), announcing each
/// boundary over the replication ring; the relay forwards `Rotate`
/// frames into the replica's adopted-rotation queue exactly where they
/// appear in the stream. The replica must reproduce the primary's
/// segmentation file-for-file: same archive count, same header
/// identities, same per-segment chain values (bitwise-mirror property).
#[cfg(all(feature = "hash-chain", not(feature = "no-persist")))]
#[test]
fn primary_driven_rotation_mirrors_segmentation_on_replica() {
    let dir = tempfile::tempdir().unwrap();
    let primary_path = dir.path().join("primary.journal");
    let replica_path = dir.path().join("replica.journal");

    let shared_anchor = [0xA5u8; 32];

    let primary_writer = Writer::create_continuing(&primary_path, 1, shared_anchor).unwrap();
    let primary_active_conns = Arc::new(AtomicU64::new(0));
    let mut primary = build_pipeline_with_replication(
        TestApp::new(),
        primary_writer,
        Duration::ZERO,
        primary_active_conns,
        true,
        MAX_JOURNAL_BATCH,
        REPLICATION_RING_CAPACITY,
        false,
        false,
        false,
        Arc::new(crate::fence::FenceState::new(0)),
    );
    let rotate_flag = Arc::new(AtomicBool::new(false));
    primary
        .journal_stage
        .set_rotation(0, Some(Arc::clone(&rotate_flag)));
    let p_util = primary.journal_stage.utilization();

    let replica_writer = Writer::create_continuing(&replica_path, 1, shared_anchor).unwrap();
    let mut replica = build_replica_pipeline(
        TestApp::new(),
        replica_writer,
        MAX_JOURNAL_BATCH,
        Duration::ZERO,
        false,
        false,
        Arc::new(crate::fence::FenceState::new(0)),
    );
    let rotations: crate::pipeline::StreamMarkQueue =
        Arc::new(std::sync::Mutex::new(std::collections::VecDeque::new()));
    replica
        .journal_stage
        .set_stream_marks(Arc::clone(&rotations));
    let r_util = replica.journal_stage.utilization();

    if let Some(ref count) = primary.replicas_connected {
        count.store(1, Ordering::Relaxed);
    }
    if let Some(ref rp) = primary.replication_ring_progress {
        rp.active_flags[0].store(true, Ordering::Relaxed);
    }

    let (mut repl_c0, mut repl_c1) = primary.replication_consumers.expect("replication enabled");
    let mut replica_input = replica.input_producer;

    let primary_shutdown = Arc::new(AtomicBool::new(false));
    let replica_shutdown = Arc::new(AtomicBool::new(false));
    let relay_shutdown = Arc::new(AtomicBool::new(false));

    // Relay: pump primary replication ring → replica input ring,
    // routing Rotate frames into the adopted-rotation queue in stream
    // order (push before any later slot is published — the receiver's
    // ordering contract).
    let relay_stop = Arc::clone(&relay_shutdown);
    let relay_rotations = Arc::clone(&rotations);
    let t_relay = std::thread::spawn(move || {
        loop {
            let mut got_something = false;
            if let Some((_meta, data)) = repl_c0.try_read() {
                let payload_len =
                    u32::from_le_bytes(data[..4].try_into().expect("4-byte length prefix"))
                        as usize;
                let payload = &data[4..4 + payload_len];
                if crate::replication_wire::peek_frame_tag(data).unwrap()
                    == crate::replication_wire::MSG_INPUT_BATCH
                {
                    let slots: Vec<TestInput> =
                        crate::replication_wire::try_decode_input_batch(payload)
                            .expect("relay InputBatch decode");
                    for slot in slots {
                        replica_input.publish(InputSlot {
                            connection_id: 0,
                            key_hash: slot.key_hash,
                            request_seq: slot.request_seq,
                            sequence: slot.sequence,
                            timestamp_ns: slot.timestamp_ns,
                            event: slot.event,
                            publish_ts: mono_trace_ns(),
                            recv_ts: mono_trace_ns(),
                        });
                    }
                } else {
                    match crate::replication::protocol::decode_primary_message(payload)
                        .expect("relay control decode")
                    {
                        crate::replication::protocol::PrimaryMessage::Rotate {
                            boundary_seq,
                            tail_hash,
                        } => relay_rotations
                            .lock()
                            .unwrap()
                            .push_back(StreamMark::Rotate(AdoptedRotation {
                                boundary_seq,
                                tail_hash,
                            })),
                        crate::replication::protocol::PrimaryMessage::ChainCheck {
                            sequence,
                            chain_hash,
                        } => relay_rotations
                            .lock()
                            .unwrap()
                            .push_back(StreamMark::ChainCheck {
                                sequence,
                                chain_hash,
                            }),
                        other => panic!("unexpected control frame on ring: {other:?}"),
                    }
                }
                repl_c0.commit();
                got_something = true;
            }
            if repl_c1.try_read().is_some() {
                repl_c1.commit();
                got_something = true;
            }
            if !got_something {
                if relay_stop.load(Ordering::Relaxed) {
                    return;
                }
                std::hint::spin_loop();
            }
        }
    });

    let mut primary_output = primary.output_consumers.pop().unwrap();
    let primary_out_shutdown = Arc::new(AtomicBool::new(false));
    let primary_out_stop = Arc::clone(&primary_out_shutdown);
    let t_primary_out = std::thread::spawn(move || {
        while !primary_out_stop.load(Ordering::Relaxed) {
            if primary_output.try_consume().is_some() {
                continue;
            }
            std::hint::spin_loop();
        }
    });

    let mut replica_drain = replica.drain_consumer;
    let replica_drain_stop = Arc::new(AtomicBool::new(false));
    let replica_drain_stop2 = Arc::clone(&replica_drain_stop);
    let t_replica_drain = std::thread::spawn(move || {
        while !replica_drain_stop2.load(Ordering::Relaxed) {
            if replica_drain.try_consume().is_some() {
                continue;
            }
            std::hint::spin_loop();
        }
    });

    let p_j_stop = Arc::clone(&primary_shutdown);
    let p_m_stop = Arc::clone(&primary_shutdown);
    let t_p_journal = std::thread::spawn(move || primary.journal_stage.run(&p_j_stop));
    let t_p_matching = std::thread::spawn(move || primary.matching_stage.run(&p_m_stop));

    let r_j_stop = Arc::clone(&replica_shutdown);
    let r_m_stop = Arc::clone(&replica_shutdown);
    let t_r_journal = std::thread::spawn(move || replica.journal_stage.run(&r_j_stop));
    let t_r_matching = std::thread::spawn(move || replica.matching_stage.run(&r_m_stop));

    // Three phases with a manual rotation between each: flip the flag,
    // then keep publishing — the rotation fires at the next fsync
    // boundary, wherever that lands. The assertions below don't depend
    // on the exact boundary, only on the replica mirroring it.
    let mut next: u64 = 0;
    let mut publish_phase = |n: u64, producer: &mut ring::Producer<TestInput>| {
        for _ in 0..n {
            next += 1;
            producer.publish(add_slot(next, 1_000_000_000 + next));
        }
    };
    publish_phase(2_000, &mut primary.input_producer);
    std::thread::sleep(Duration::from_millis(300));
    rotate_flag.store(true, Ordering::Release);
    publish_phase(2_000, &mut primary.input_producer);
    std::thread::sleep(Duration::from_millis(300));
    rotate_flag.store(true, Ordering::Release);
    publish_phase(1_000, &mut primary.input_producer);
    std::thread::sleep(Duration::from_millis(500));

    primary_shutdown.store(true, Ordering::Relaxed);
    let primary_journal_result = t_p_journal.join().unwrap();
    let _ = t_p_matching.join().unwrap();
    relay_shutdown.store(true, Ordering::Relaxed);
    let _ = t_relay.join();
    std::thread::sleep(Duration::from_millis(500));
    replica_shutdown.store(true, Ordering::Relaxed);
    let replica_journal_result = t_r_journal.join().unwrap();
    let _ = t_r_matching.join().unwrap();
    primary_journal_result.expect("primary journal stage must exit cleanly");
    replica_journal_result.expect("replica journal stage must exit cleanly");
    primary_out_shutdown.store(true, Ordering::Relaxed);
    let _ = t_primary_out.join();
    replica_drain_stop.store(true, Ordering::Relaxed);
    let _ = t_replica_drain.join();

    // Per-segment fingerprint: header identity + entry count + chain
    // value at EOF. Chain equality per segment ⇒ byte-identical entry
    // streams under identical framing — the bitwise-mirror property.
    fn fingerprint(path: &std::path::Path) -> (u64, [u8; 32], u64, Option<[u8; 32]>) {
        let info = melin_journal::segment::read_header_info(path).unwrap();
        let mut reader = JournalReader::<TestEvent>::open(path).unwrap();
        let mut count = 0u64;
        while reader.next_entry().unwrap().is_some() {
            count += 1;
        }
        (
            info.starting_sequence,
            info.anchor_hash,
            count,
            reader.chain_hash(),
        )
    }

    let segment_files = |live: &std::path::Path| -> Vec<std::path::PathBuf> {
        let mut files: Vec<std::path::PathBuf> = melin_journal::segment::list_archives(live)
            .unwrap()
            .into_iter()
            .map(|(_, p)| p)
            .collect();
        files.push(live.to_path_buf());
        files
    };

    let primary_files = segment_files(&primary_path);
    let replica_files = segment_files(&replica_path);
    assert!(
        primary_files.len() >= 2,
        "at least one rotation must have fired on the primary (got {} segment files)",
        primary_files.len()
    );
    assert_eq!(
        primary_files.len(),
        replica_files.len(),
        "replica must mirror the primary's segment count"
    );
    for (p, r) in primary_files.iter().zip(replica_files.iter()) {
        assert_eq!(
            fingerprint(p),
            fingerprint(r),
            "segment mismatch: {} vs {}",
            p.display(),
            r.display()
        );
    }

    // Rotation accounting on the /healthz-surfaced counters: every
    // primary rotation is mirrored by exactly one replica adoption.
    // The primary is manual-only here (max_journal_bytes == 0), so by
    // policy its preparer stays unarmed and every rotation takes the
    // sync path; the replica's adoptions may land on either path
    // depending on whether the preparer's staging won the race.
    let rotations = (primary_files.len() - 1) as u64;
    assert_eq!(
        p_util.rotations_sync_fallback.load(Ordering::Relaxed),
        rotations,
        "manual-only primary rotations must all take the sync path"
    );
    assert_eq!(
        p_util.rotations_fast_path.load(Ordering::Relaxed),
        0,
        "manual-only primary must not have a preparer"
    );
    assert_eq!(
        r_util.rotations_fast_path.load(Ordering::Relaxed)
            + r_util.rotations_sync_fallback.load(Ordering::Relaxed),
        rotations,
        "each adopted rotation must be counted"
    );
}

/// Size-threshold rotation: setting a small `max_journal_bytes`
/// causes the stage to rotate without operator intervention. The
/// threshold is engaged after the first batch crosses the limit.
#[cfg(not(feature = "no-persist"))]
#[test]
fn journal_stage_rotates_on_size_threshold() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rotate_size.journal");
    let writer = Writer::create(&path).unwrap();

    let (mut producer, mut consumers) = ring::DisruptorBuilder::<TestInput>::new(64)
        .add_consumer()
        .build();
    let consumer = consumers.pop().unwrap();

    let mut stage = JournalStage::new(writer, consumer, Duration::ZERO, MAX_JOURNAL_BATCH, false);
    // Tiny threshold — any non-empty fsync will cross it.
    stage.set_rotation(/* max_journal_bytes */ 1, None);

    let shutdown = Arc::new(AtomicBool::new(false));
    let s = Arc::clone(&shutdown);
    let handle = std::thread::spawn(move || stage.run(&s));

    producer.publish(InputSlot {
        connection_id: 1,
        key_hash: 1,
        request_seq: 1,
        sequence: 0,
        timestamp_ns: 1_000_000_000,
        event: JournalEvent::App(TestEvent::Add(42)),
        publish_ts: mono_trace_ns(),
        recv_ts: mono_trace_ns(),
    });

    let archive_path = std::path::PathBuf::from(format!("{}.000001", path.display()));
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !archive_path.exists() && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }

    shutdown.store(true, Ordering::Relaxed);
    let _ = handle.join().unwrap();

    assert!(
        archive_path.exists(),
        "size-threshold rotation should have produced {}",
        archive_path.display()
    );
}

/// The run-startup sequence must arm the background preparer when
/// rotation recurs, so steady-state rotations adopt the pre-zeroed
/// segment instead of stalling on the synchronous allocate.
/// Regression test for the unwired `enable_preparer` — without the
/// startup call, every rotation is a sync fallback forever, and each
/// fallback segment also re-introduces per-append extent-conversion
/// metadata inside `fdatasync` until the next rotation (see
/// `docs/internal/journal-fsync-beat-2026-08.md`).
#[cfg(not(feature = "no-persist"))]
#[test]
fn size_rotation_uses_prepared_fast_path_after_warmup() {
    // The loop below rotates open-endedly until the fast path engages;
    // at the default 256 MiB prealloc chunk each rotation would
    // materialize a full archive plus a staged sidecar (real memory on
    // tmpfs), and disk pressure would make failure self-reinforcing
    // via the preparer's 30 s error backoff. Shrink the chunk.
    let _prealloc_guard = melin_journal::test_utils::PreallocOverrideGuard::new(1024 * 1024);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fast_rotate.journal");
    let writer = Writer::create(&path).unwrap();

    let (mut producer, mut consumers) = ring::DisruptorBuilder::<TestInput>::new(1024)
        .add_consumer()
        .build();
    let consumer = consumers.pop().unwrap();

    let mut stage = JournalStage::new(writer, consumer, Duration::ZERO, MAX_JOURNAL_BATCH, false);
    // Tiny threshold — every non-empty fsync rotates.
    stage.set_rotation(/* max_journal_bytes */ 1, None);
    let util = stage.utilization();

    let shutdown = Arc::new(AtomicBool::new(false));
    let s = Arc::clone(&shutdown);
    let handle = std::thread::spawn(move || stage.run(&s));

    // Each publish lands in its own fsync (threshold 1) and rotates.
    // Early rotations may race the preparer's initial staging and fall
    // back; once the worker catches up, a rotation must hit the fast
    // path. Keep publishing until one does (bounded by the deadline).
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let mut n = 0u64;
    while util.rotations_fast_path.load(Ordering::Relaxed) == 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "no fast-path rotation within deadline (sync fallbacks: {})",
            util.rotations_sync_fallback.load(Ordering::Relaxed)
        );
        n += 1;
        producer.publish(add_slot(n, 1_000_000_000 + n));
        std::thread::sleep(Duration::from_millis(20));
    }

    shutdown.store(true, Ordering::Relaxed);
    handle.join().unwrap().unwrap();
}

/// `enable_preparer` arms exactly when rotation recurs on a
/// predictable cadence: size-driven rotation or replica adoption
/// (primary-announced rotations arrive at the primary's cadence).
/// Manual-only rotation must NOT arm it — the cadence is
/// unpredictable and the staged segment may never be consumed.
#[cfg(not(feature = "no-persist"))]
#[test]
fn preparer_arms_for_size_and_replica_modes_only() {
    let dir = tempfile::tempdir().unwrap();
    let mk_stage = |name: &str| {
        let writer = Writer::create(&dir.path().join(name)).unwrap();
        let (_p, mut consumers) = ring::DisruptorBuilder::<TestInput>::new(64)
            .add_consumer()
            .build();
        JournalStage::new(
            writer,
            consumers.pop().unwrap(),
            Duration::ZERO,
            MAX_JOURNAL_BATCH,
            false,
        )
    };

    let mut size_driven = mk_stage("size.journal");
    size_driven.set_rotation(1024, None);
    size_driven.enable_preparer();
    assert!(
        size_driven.preparer_enabled(),
        "size-driven rotation must arm the preparer"
    );

    let mut replica = mk_stage("replica.journal");
    replica.set_stream_marks(Arc::new(std::sync::Mutex::new(
        std::collections::VecDeque::new(),
    )));
    replica.enable_preparer();
    assert!(
        replica.preparer_enabled(),
        "replica adoption must arm the preparer"
    );

    let mut manual_only = mk_stage("manual.journal");
    manual_only.set_rotation(0, Some(Arc::new(AtomicBool::new(false))));
    manual_only.enable_preparer();
    assert!(
        !manual_only.preparer_enabled(),
        "manual-only rotation must not arm the preparer"
    );
}

/// The size-driven rotation trigger measures the live segment from a
/// counter the sequencer keeps, because the file itself now lives on
/// the disk thread. That counter has to track the segment's real size:
/// if it drifted the deployment would rotate at the wrong cadence, or
/// (drifting low) never rotate at all while the journal grew unbounded.
///
/// Rotate at a real threshold and check the archive against it — the
/// counter runs at most one in-flight batch ahead of the file, so the
/// sealed segment must land within a batch of the threshold rather than
/// wildly past it.
#[cfg(not(feature = "no-persist"))]
#[test]
fn size_trigger_tracks_the_segments_real_size() {
    const THRESHOLD: u64 = 16 * 1024;
    /// Generous bound on one encoded entry (`MAX_ENTRY_SIZE` is 144).
    const ONE_ENTRY: u64 = 256;

    let _prealloc_guard = melin_journal::test_utils::PreallocOverrideGuard::new(1024 * 1024);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sized.journal");
    let writer = Writer::create(&path).unwrap();

    let (mut producer, mut consumers) = ring::DisruptorBuilder::<TestInput>::new(1024)
        .add_consumer()
        .build();
    let consumer = consumers.pop().unwrap();

    let mut stage = JournalStage::new(writer, consumer, Duration::ZERO, MAX_JOURNAL_BATCH, false);
    stage.set_rotation(THRESHOLD, None);
    // One event per batch: publish, wait for it to become durable, then
    // publish the next. Free-running publication would batch hundreds
    // of events per rotation check, and the overshoot from batching
    // would swamp the counter error this test exists to detect.
    let durable = DurableWireSeqCursor::detached(WireSeq::new(0));
    stage.set_last_seq_publisher(durable.clone());

    let shutdown = Arc::new(AtomicBool::new(false));
    let s = Arc::clone(&shutdown);
    let handle = std::thread::spawn(move || stage.run(&s));

    let archive = std::path::PathBuf::from(format!("{}.000001", path.display()));
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut n = 0u64;
    while !archive.exists() {
        assert!(
            std::time::Instant::now() < deadline,
            "no size-driven rotation within deadline after {n} events"
        );
        n += 1;
        producer.publish(add_slot(n, 1_000_000_000 + n));
        let mut spins = 0u32;
        while durable.load() < WireSeq::new(n) {
            assert!(
                std::time::Instant::now() < deadline,
                "event {n} never became durable"
            );
            drain_backoff(&mut spins, std::time::Instant::now(), "durable cursor");
        }
    }

    shutdown.store(true, Ordering::Relaxed);
    handle.join().unwrap().unwrap();

    // With one event in flight at a time, the sealed segment must land
    // within a single entry of the threshold. A counter running ahead
    // of the segment rotates early and seals short; one lagging behind
    // seals long.
    let sealed = std::fs::metadata(&archive).unwrap().len();
    assert!(
        sealed >= THRESHOLD,
        "rotated before the threshold: sealed {sealed} < {THRESHOLD} — \
         the size counter is running ahead of the segment"
    );
    assert!(
        sealed <= THRESHOLD + ONE_ENTRY,
        "rotated past the threshold: sealed {sealed} > {THRESHOLD} + {ONE_ENTRY} — \
         the size counter is lagging the segment"
    );
}

/// The sequencer skips the per-batch chain value when nothing reads it
/// (`chain_hash_observed`) — it costs a BLAKE3 clone and finalize on
/// the hand-off path. This is the other half of that bargain: with a
/// publisher attached, `FsyncState.chain_hash` must carry the real
/// chain at the batch's last sequence.
///
/// The shadow stage stamps this value into every snapshot it writes, so
/// a regression to a constant would produce snapshots that fail chain
/// verification against the journal they claim to describe — and only
/// at recovery time, on a node that has already lost its memory state.
#[cfg(all(feature = "hash-chain", not(feature = "no-persist")))]
#[test]
fn fsync_state_carries_the_real_chain_hash_when_a_publisher_is_attached() {
    use melin_pipeline::seqlock;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("observed_chain.journal");
    let writer = Writer::create(&path).unwrap();

    let (mut producer, mut consumers) = ring::DisruptorBuilder::<TestInput>::new(64)
        .add_consumer()
        .build();
    let consumer = consumers.pop().unwrap();

    let mut stage = JournalStage::new(writer, consumer, Duration::ZERO, MAX_JOURNAL_BATCH, false);
    let (fsync_writer, fsync_state) = seqlock::split(crate::pipeline::FsyncState::default());
    stage.set_chain_hash_lock(fsync_writer);

    let shutdown = Arc::new(AtomicBool::new(false));
    let s = Arc::clone(&shutdown);
    let handle = std::thread::spawn(move || stage.run(&s));

    // One event per batch, so the published state describes exactly the
    // sequence we then verify against the file.
    const EVENTS: u64 = 8;
    for seq in 1..=EVENTS {
        producer.publish(add_slot(seq, 1_000_000_000 + seq));
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while fsync_state.load().journal_seq.get() < seq {
            assert!(
                std::time::Instant::now() < deadline,
                "seq {seq} never became durable"
            );
            std::hint::spin_loop();
        }
        let published = fsync_state.load();
        assert_eq!(published.journal_seq.get(), seq);
        assert_ne!(
            published.chain_hash, [0u8; 32],
            "seq {seq}: the chain value was not computed"
        );
    }

    shutdown.store(true, Ordering::Relaxed);
    handle.join().unwrap().unwrap();

    // The published value must equal the chain the journal file itself
    // produces at that sequence — not merely be non-zero.
    let expected = match melin_journal::segment::chain_value_at(&path, EVENTS).unwrap() {
        melin_journal::segment::ChainValueAt::Value(v) => v,
        other => panic!("journal ends before seq {EVENTS}: {other:?}"),
    };
    assert_eq!(
        fsync_state.load().chain_hash,
        expected,
        "published chain value disagrees with the on-disk journal at seq {EVENTS}"
    );
}

/// A batch that encodes no bytes still has to travel to the disk
/// thread, because the input-ring slots its events occupy are released
/// by the cursors it carries — not by the write.
///
/// Queries are never journaled, so a query-only batch is byte-empty;
/// under `no-persist` *every* batch is. Dropping such a batch as
/// "nothing to write" leaves those slots held forever: the input ring
/// fills, producers block on backpressure, and the pipeline wedges with
/// no error anywhere. A hang, not a failure — which is exactly why it
/// needs a test rather than trust.
///
/// Deliberately not gated on `no-persist`: the query path makes the
/// batch byte-empty under every feature configuration, so this guards
/// the same code in both.
#[test]
fn cursor_only_batches_still_release_their_input_slots() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("queries_only.journal");
    let writer = Writer::create(&path).unwrap();

    let (mut producer, mut consumers) = ring::DisruptorBuilder::<TestInput>::new(64)
        .add_consumer()
        .build();
    let consumer = consumers.pop().unwrap();
    let progress = consumer.progress_counter();

    let stage = JournalStage::new(writer, consumer, Duration::ZERO, MAX_JOURNAL_BATCH, false);
    let shutdown = Arc::new(AtomicBool::new(false));
    let s = Arc::clone(&shutdown);
    let handle = std::thread::spawn(move || stage.run(&s));

    const QUERIES: u64 = 6;
    for n in 0..QUERIES {
        producer.publish(TestInput {
            event: JournalEvent::App(TestEvent::Query),
            ..add_slot(n, 1_000_000_000 + n)
        });
    }

    // Nothing is written, so durability cannot be observed through the
    // journal — the released slots are the only evidence, and they are
    // the thing that matters.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let mut spins = 0u32;
    while progress.get().load(Ordering::Acquire) < QUERIES {
        assert!(
            std::time::Instant::now() < deadline,
            "query-only batches never released their input slots (progress {}) \
             — the input ring would fill and the pipeline would wedge",
            progress.get().load(Ordering::Acquire)
        );
        drain_backoff(&mut spins, std::time::Instant::now(), "slot release");
    }

    shutdown.store(true, Ordering::Relaxed);
    let writer = handle.join().unwrap().unwrap();
    assert_eq!(
        writer.next_sequence(),
        1,
        "queries must consume no journal sequence"
    );
}

/// `melin_journal_disk_lag_batches` is documented as zero in steady
/// state, and operators are told to alert on a *sustained* non-zero
/// value. That only holds if the gauge is refreshed after the disk
/// catches up. Stored solely at submit time it freezes at whatever was
/// in flight when the last batch was handed over, so every burst that
/// ends leaves a permanent phantom stall on the dashboard — the exact
/// false positive the alert is supposed to distinguish from a real one.
///
/// Publish enough to put the disk behind, then let the stage idle and
/// require the gauge to come back to zero on its own.
#[cfg(not(feature = "no-persist"))]
#[test]
fn disk_lag_gauge_returns_to_zero_once_the_journal_quiesces() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lag.journal");
    let writer = Writer::create(&path).unwrap();

    let (mut producer, mut consumers) = ring::DisruptorBuilder::<TestInput>::new(1024)
        .add_consumer()
        .build();
    let consumer = consumers.pop().unwrap();

    const EVENTS: u64 = 512;

    let mut stage = JournalStage::new(writer, consumer, Duration::ZERO, MAX_JOURNAL_BATCH, false);
    let utilization = stage.utilization();
    // Durability of the last event is the "the stage has quiesced"
    // signal. Without it the gauge would be read before the stage ever
    // submitted a batch, and its untouched initial zero would satisfy
    // the assertion for the wrong reason.
    let durable = DurableWireSeqCursor::detached(WireSeq::new(0));
    stage.set_last_seq_publisher(durable.clone());

    let shutdown = Arc::new(AtomicBool::new(false));
    let s = Arc::clone(&shutdown);
    let handle = std::thread::spawn(move || stage.run(&s));

    for n in 1..=EVENTS {
        producer.publish(add_slot(n, 1_000_000_000 + n));
    }

    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut spins = 0u32;
    while durable.load() < WireSeq::new(EVENTS) {
        assert!(
            std::time::Instant::now() < deadline,
            "the burst never became durable"
        );
        drain_backoff(&mut spins, std::time::Instant::now(), "durable cursor");
    }

    // Everything is on disk and no further batch will ever be
    // submitted. The gauge must reflect that.
    loop {
        let lag = utilization.journal_disk_lag.load(Ordering::Relaxed);
        if lag == 0 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "disk lag gauge stuck at {lag} long after the journal quiesced — \
             it is only refreshed when a batch is submitted"
        );
        drain_backoff(&mut spins, std::time::Instant::now(), "disk lag gauge");
    }

    shutdown.store(true, Ordering::Relaxed);
    handle.join().unwrap().unwrap();
}

/// ROTATE storm: many rapid sets of the flag between fsync
/// boundaries collapse to a single rotation, not one rotation per
/// store. Validates the `compare_exchange(true → false)` consume in
/// `maybe_rotate`.
#[cfg(not(feature = "no-persist"))]
#[test]
fn rotate_storm_collapses_to_single_rotation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("storm.journal");
    let writer = Writer::create(&path).unwrap();

    let (mut producer, mut consumers) = ring::DisruptorBuilder::<TestInput>::new(64)
        .add_consumer()
        .build();
    let consumer = consumers.pop().unwrap();

    let mut stage = JournalStage::new(writer, consumer, Duration::ZERO, MAX_JOURNAL_BATCH, false);
    let rotate_flag = Arc::new(AtomicBool::new(false));
    stage.set_rotation(
        /* max_journal_bytes */ 0,
        Some(Arc::clone(&rotate_flag)),
    );

    let shutdown = Arc::new(AtomicBool::new(false));
    let s = Arc::clone(&shutdown);
    let handle = std::thread::spawn(move || stage.run(&s));

    let mut req_seq: u64 = 0;
    let mut publish = |amount: u64| {
        req_seq += 1;
        producer.publish(InputSlot {
            connection_id: 1,
            key_hash: 1,
            request_seq: req_seq,
            sequence: 0,
            timestamp_ns: 1_000_000 + req_seq,
            event: JournalEvent::App(TestEvent::Add(amount)),
            publish_ts: mono_trace_ns(),
            recv_ts: mono_trace_ns(),
        });
    };

    publish(1);
    std::thread::sleep(Duration::from_millis(100));

    // Storm of 100 rapid sets while the stage is idle — only the next
    // fsync gets to observe-and-clear the flag.
    for _ in 0..100 {
        rotate_flag.store(true, Ordering::Release);
    }

    // Trigger an fsync. Stage observes the flag once, CAS-clears it,
    // rotates; the remaining 99 stores collapse onto the same rotation.
    publish(2);

    let archive_001 = std::path::PathBuf::from(format!("{}.000001", path.display()));
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !archive_001.exists() && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    publish(3);
    std::thread::sleep(Duration::from_millis(200));

    shutdown.store(true, Ordering::Relaxed);
    let _ = handle.join().unwrap();

    assert!(archive_001.exists(), ".000001 must exist");
    let archive_002 = std::path::PathBuf::from(format!("{}.000002", path.display()));
    assert!(
        !archive_002.exists(),
        "storm must collapse to a single rotation, but .000002 exists"
    );
}

/// Post-rotation events must land in the *live* segment, not in the
/// just-archived one. Rotation closes the old live fd (now pointing at
/// the archived inode) and opens a new one; anything that keeps
/// writing through a stale handle silently appends to the archive,
/// which recovery then reads as a segment overlap.
#[cfg(not(feature = "no-persist"))]
#[test]
fn post_rotation_events_land_in_live_not_archive() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("post_rot.journal");
    let writer = Writer::create(&path).unwrap();

    let (mut producer, mut consumers) = ring::DisruptorBuilder::<TestInput>::new(1024)
        .add_consumer()
        .build();
    let consumer = consumers.pop().unwrap();

    let mut stage = JournalStage::new(writer, consumer, Duration::ZERO, MAX_JOURNAL_BATCH, false);
    let rotate_flag = Arc::new(AtomicBool::new(false));
    stage.set_rotation(
        /* max_journal_bytes */ 0,
        Some(Arc::clone(&rotate_flag)),
    );

    let shutdown = Arc::new(AtomicBool::new(false));
    let s = Arc::clone(&shutdown);
    let handle = std::thread::spawn(move || stage.run(&s));

    let mut req_seq: u64 = 0;
    let mut publish = |producer: &mut ring::Producer<TestInput>, amount: u64| {
        req_seq += 1;
        producer.publish(InputSlot {
            connection_id: 1,
            key_hash: 1,
            request_seq: req_seq,
            sequence: 0,
            timestamp_ns: 1_000_000 + req_seq,
            event: JournalEvent::App(TestEvent::Add(amount)),
            publish_ts: mono_trace_ns(),
            recv_ts: mono_trace_ns(),
        });
    };

    // Phase 1 — a batch of events written and synced before the
    // rotation is requested.
    const PRE: u64 = 50;
    for i in 1..=PRE {
        publish(&mut producer, i);
    }

    rotate_flag.store(true, Ordering::Release);
    let archive_001 = std::path::PathBuf::from(format!("{}.000001", path.display()));
    publish(&mut producer, 9_999);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !archive_001.exists() && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(archive_001.exists(), "archive must be created by rotation");

    // Phase 2 — fresh burst after rotation. Must land in the new live
    // segment, not the archived one.
    const POST: u64 = 50;
    for i in 1..=POST {
        publish(&mut producer, 10_000 + i);
    }
    std::thread::sleep(Duration::from_millis(300));

    shutdown.store(true, Ordering::Relaxed);
    let _ = handle.join().unwrap();

    // Read each segment directly and collect every entry's sequence.
    fn collect_app_seqs(p: &std::path::Path) -> Vec<u64> {
        let mut reader = JournalReader::<TestEvent>::open(p).unwrap();
        let mut out = Vec::new();
        while let Some(entry) = reader.next_entry().unwrap() {
            if matches!(entry.event, JournalEvent::App(_)) {
                out.push(entry.sequence);
            }
        }
        out
    }
    let archive_seqs = collect_app_seqs(&archive_001);
    let live_seqs = collect_app_seqs(&path);

    assert!(
        !archive_seqs.is_empty(),
        "archive must contain the pre-rotation events"
    );
    assert!(
        !live_seqs.is_empty(),
        "live segment must contain post-rotation events"
    );
    let archive_max = *archive_seqs.iter().max().unwrap();
    let live_min = *live_seqs.iter().min().unwrap();
    assert!(
        archive_max < live_min,
        "post-rotation events leaked into the archive: archive max={archive_max} \
         live min={live_min} archive_seqs={archive_seqs:?} live_seqs={live_seqs:?}"
    );
    let archive_set: std::collections::HashSet<u64> = archive_seqs.iter().copied().collect();
    for s in &live_seqs {
        assert!(
            !archive_set.contains(s),
            "seq {s} present in both archive and live — the rotation \
             left a stale write handle behind"
        );
    }
}

/// Every event published through the pipeline must land on disk in
/// input order, exactly once. The end-to-end floor for the journal
/// stage: publish five events, shut down, read the segment back.
#[cfg(not(feature = "no-persist"))]
#[test]
fn pipeline_journals_every_event_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("parity.journal");
    let writer = Writer::create(&path).unwrap();

    let (mut producer, mut consumers) = ring::DisruptorBuilder::<TestInput>::new(64)
        .add_consumer()
        .build();
    let consumer = consumers.pop().unwrap();
    let stage = JournalStage::new(writer, consumer, Duration::ZERO, MAX_JOURNAL_BATCH, false);

    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown2 = Arc::clone(&shutdown);

    for amount in [10u64, 20, 30, 40, 50] {
        producer.publish(InputSlot {
            connection_id: 1,
            key_hash: 0,
            request_seq: 0,
            sequence: 0,
            timestamp_ns: 1_000_000_000 + amount,
            event: JournalEvent::App(TestEvent::Add(amount)),
            publish_ts: mono_trace_ns(),
            recv_ts: mono_trace_ns(),
        });
    }

    let handle = std::thread::spawn(move || stage.run(&shutdown2));
    std::thread::sleep(Duration::from_millis(100));
    shutdown.store(true, Ordering::Relaxed);
    let _writer = handle.join().unwrap();

    let mut reader = JournalReader::<TestEvent>::open(&path).unwrap();
    let mut seqs = Vec::new();
    while let Some(entry) = reader.next_entry().unwrap() {
        if let JournalEvent::App(_) = entry.event {
            seqs.push(entry.sequence);
        }
    }
    assert_eq!(
        seqs,
        vec![1, 2, 3, 4, 5],
        "every published event must be journaled once, in input order"
    );
}

/// End-to-end pin for the stats-query surface: `ApplyCtx.journal_sequence`
/// must report the durable wire seq — the same space as the health
/// endpoint's `journal_seq` gauge — both live and, critically, after
/// recovery, where the journal ring cursor restarts near zero while the
/// durable cursor resumes at the recovered high-water mark. Guards the
/// space fix that moved the stats surface off the ring cursor.
///
/// Requires a really-persisted journal: the second half recovers from
/// the file the first half wrote, and `no-persist` writes none.
#[cfg(not(feature = "no-persist"))]
#[test]
fn stats_query_reports_durable_wire_seq_across_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("stats_query_durable.journal");

    fn slot(event: JournalEvent<TestEvent>) -> TestInput {
        InputSlot {
            connection_id: 1,
            key_hash: 0,
            request_seq: 0,
            sequence: 0,
            timestamp_ns: 0,
            event,
            publish_ts: mono_trace_ns(),
            recv_ts: mono_trace_ns(),
        }
    }

    fn drain_query(output_consumer: &mut ring::Consumer<TestOutput>) -> TestQuery {
        let mut spins = 0u32;
        let drain_start = std::time::Instant::now();
        loop {
            if let Some((_, out_slot)) = output_consumer.try_consume() {
                if let OutputPayload::QueryResponse(q) = out_slot.payload {
                    return q;
                }
            } else {
                crate::pipeline_tests::drain_backoff(
                    &mut spins,
                    drain_start,
                    "draining query response",
                );
            }
        }
    }

    // --- Phase 1: fresh journal — the query reports the live durable seq.
    {
        let writer = Writer::create(&path).unwrap();
        let mut out = build_pipeline_with_replication(
            TestApp::new(),
            writer,
            Duration::ZERO,
            Arc::new(AtomicU64::new(0)),
            false,
            MAX_JOURNAL_BATCH,
            REPLICATION_RING_CAPACITY,
            false,
            false,
            false,
            Arc::new(crate::fence::FenceState::new(0)),
        );
        let mut input_producer = out.input_producer;
        let journal_stage = out.journal_stage;
        let matching_stage = out.matching_stage;
        let last_seq = out.cursors.durable_wire_seq();
        let mut output_consumer = out.output_consumers.pop().unwrap();

        let shutdown = Arc::new(AtomicBool::new(false));
        let s1 = Arc::clone(&shutdown);
        let s2 = Arc::clone(&shutdown);
        let t_journal = std::thread::spawn(move || journal_stage.run(&s1));
        let t_matching = std::thread::spawn(move || matching_stage.run(&s2));

        for n in 1..=5u64 {
            input_producer.publish(slot(JournalEvent::App(TestEvent::Add(n))));
        }
        // Wait for the fsync to land before querying: the durable cursor
        // then sits at exactly 5 and cannot move (queries are never
        // journaled), so the assertion below is race-free.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while last_seq.load().get() < 5 && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(last_seq.load().get(), 5, "phase 1 fsync");

        input_producer.publish(slot(JournalEvent::App(TestEvent::Query)));
        let q = drain_query(&mut output_consumer);
        assert_eq!(q.total, 1 + 2 + 3 + 4 + 5);
        assert_eq!(
            q.journal_sequence, 5,
            "live stats query must report the durable wire seq"
        );

        shutdown.store(true, Ordering::Relaxed);
        let _writer = t_journal.join().unwrap();
        let _app = t_matching.join().unwrap();
    }

    // --- Phase 2: recover — the ring cursor restarts at zero, but the
    // stats surface must keep reporting the recovered high-water mark.
    let engine = JournaledApp::<TestApp, Writer>::recover(TestApp::new(), &path).unwrap();
    let (app, writer) = engine.into_parts();
    let mut out = build_pipeline_with_replication(
        app,
        writer,
        Duration::ZERO,
        Arc::new(AtomicU64::new(0)),
        false,
        MAX_JOURNAL_BATCH,
        REPLICATION_RING_CAPACITY,
        false,
        false,
        false,
        Arc::new(crate::fence::FenceState::new(0)),
    );
    let mut input_producer = out.input_producer;
    let journal_stage = out.journal_stage;
    let matching_stage = out.matching_stage;
    let mut output_consumer = out.output_consumers.pop().unwrap();

    let shutdown = Arc::new(AtomicBool::new(false));
    let s1 = Arc::clone(&shutdown);
    let s2 = Arc::clone(&shutdown);
    let t_journal = std::thread::spawn(move || journal_stage.run(&s1));
    let t_matching = std::thread::spawn(move || matching_stage.run(&s2));

    // The query is the FIRST slot consumed after boot — the journal ring
    // cursor is still ~0, so reading the ring instead of the durable
    // cursor (the pre-fix behaviour) would report ~0 here, not 5.
    input_producer.publish(slot(JournalEvent::App(TestEvent::Query)));
    let q = drain_query(&mut output_consumer);
    assert_eq!(q.total, 15, "recovered state");
    assert_eq!(
        q.journal_sequence, 5,
        "post-recovery stats query must report the recovered durable high-water, not the ring position"
    );

    shutdown.store(true, Ordering::Relaxed);
    let _writer = t_journal.join().unwrap();
    let _app = t_matching.join().unwrap();
}
