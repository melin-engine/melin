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
use melin_journal::{BufferedWriter, JournalEvent, JournalReader, SectorWriter};
use melin_pipeline::padding::Sequence;
use melin_pipeline::ring;

use crate::journaled_app::JournaledApp;
#[cfg(all(feature = "hash-chain", not(feature = "no-persist")))]
use crate::pipeline::build_replica_pipeline;
use crate::pipeline::{
    InputSlot, JournalStage, JournalStageRun, MAX_JOURNAL_BATCH, MatchingStage, OutputPayload,
    OutputSlot, build_pipeline_with_replication,
};
use crate::test_support::{TestApp, TestEvent, TestQuery, TestReport};
use crate::trace::mono_trace_ns;

// Pipeline tests historically exercised the io_uring path under the
// sector writer; keep them on that writer here so the io_uring
// specialization stays covered by the infrastructure tests.
type Writer = SectorWriter<TestEvent>;
type TestInput = InputSlot<TestEvent>;
type TestOutput = OutputSlot<TestReport, TestQuery>;

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

    // Journal cursor and counters not used in this test — create dummies.
    let dummy_cursor = Arc::new(Sequence::new(AtomicU64::new(0)));
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

    let mut attempts = 0;
    let output = loop {
        if let Some((_, slot)) = output_consumer.try_consume() {
            break slot;
        }
        attempts += 1;
        if attempts > 1_000_000 {
            panic!("timeout waiting for output");
        }
        std::hint::spin_loop();
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

    let dummy_cursor = Arc::new(Sequence::new(AtomicU64::new(0)));
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
    let mut spins = 0u64;
    while outputs.len() < 6 {
        if let Some((_, slot)) = output_consumer.try_consume() {
            outputs.push(slot);
        } else {
            spins += 1;
            assert!(spins < 10_000_000, "timeout draining outputs");
            std::hint::spin_loop();
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
    let last_seq = Arc::clone(&out.last_seq);
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
    while last_seq.load(Ordering::Acquire) < 3 && std::time::Instant::now() < deadline {
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
    let mut spins = 0u64;
    while outputs.len() < 6 {
        if let Some((_, slot)) = output_consumer.try_consume() {
            outputs.push(slot);
        } else {
            spins += 1;
            assert!(spins < 10_000_000, "timeout draining outputs");
            std::hint::spin_loop();
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
    while last_seq.load(Ordering::Acquire) < MAX_WIRE_SEQ && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        last_seq.load(Ordering::Acquire),
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
        let last_seq = Arc::clone(&out.last_seq);

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
        while last_seq.load(Ordering::Acquire) < 3 && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        rotate_flag.store(true, Ordering::Release);
        input_producer.publish(make_slot(JournalEvent::App(TestEvent::Add(4))));

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while last_seq.load(Ordering::Acquire) < 4 && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(last_seq.load(Ordering::Acquire), 4, "phase 1 fsync");

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
    let last_seq = Arc::clone(&out.last_seq);
    let mut output_consumer = out.output_consumers.pop().unwrap();

    // The gate cursor must resume at exactly the recovered high-water
    // mark — before any new event is published. A writer-internal
    // entry consumed during recovery/reopen would overshoot here.
    assert_eq!(
        last_seq.load(Ordering::Acquire),
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
    let mut spins = 0u64;
    while outputs.len() < 3 {
        if let Some((_, slot)) = output_consumer.try_consume() {
            outputs.push(slot);
        } else {
            spins += 1;
            assert!(spins < 10_000_000, "timeout draining outputs");
            std::hint::spin_loop();
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
    while last_seq.load(Ordering::Acquire) < 6 && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        last_seq.load(Ordering::Acquire),
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
    let last_seq = Arc::clone(&replica.last_seq);

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
    while last_seq.load(Ordering::Acquire) < 3 && std::time::Instant::now() < deadline {
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
    while last_seq.load(Ordering::Acquire) < 5 && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        last_seq.load(Ordering::Acquire),
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
    let journal_cursor = out.journal_cursor;
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
    let journal_cursor = out.journal_cursor;
    let replication_cursor = out.replication_cursor;

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

    replication_cursor.store(repl_meta.end_sequence + 1, Ordering::Release);
    let journal_pos = journal_cursor.get().load(Ordering::Acquire);
    let repl_pos = replication_cursor.load(Ordering::Acquire);
    let effective = journal_pos.min(repl_pos);
    assert!(
        effective > output.input_seq,
        "both cursors should have advanced"
    );

    shutdown.store(true, Ordering::Relaxed);
    let _writer = t_journal.join().unwrap();
    let _app = t_matching.join().unwrap();
}

#[test]
fn replication_cursor_always_starts_at_max() {
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
        assert_eq!(out.replication_cursor.load(Ordering::Relaxed), u64::MAX);
    }

    // Replication enabled — cursor still starts at u64::MAX.
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
            out.replication_cursor.load(Ordering::Relaxed),
            u64::MAX,
            "replication cursor should start at MAX even when enabled"
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
/// segments sharing one anchor. Chain equality only holds while
/// segment boundaries align — an unaligned rotation on either node
/// legitimately breaks it (the entry *stream* stays identical). Do not
/// extend this test with rotation and keep the equality assert;
/// cross-node comparison under rotation is the primary-driven-rotation
/// roadmap item.
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
/// just-archived one. Regression test for the io_uring fixed-file-
/// registration bug: rotation closes the old live fd (now pointing
/// at the archived inode) and opens a new one, but io_uring's
/// `register_files` table still references the old fd unless we call
/// `register_files_update`. Without the update, every subsequent SQE
/// submitted with `types::Fixed(0)` writes into the archived inode
/// rather than the new live file.
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

    // Phase 1 — enough events to cross one sector so io_uring's async-
    // write path engages (not the partial-tail sync fallback that
    // bypasses the registered fd).
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
            "seq {s} present in both archive and live — io_uring fd \
             reregistration regression?"
        );
    }
}

/// Cross-writer parity: the same input sequence published through
/// the pipeline must produce the same on-disk app sequences and
/// event count under both writer specializations. Guards against
/// either writer silently dropping or reordering events when one is
/// changed in isolation.
#[cfg(not(feature = "no-persist"))]
#[test]
fn pipeline_journal_contents_match_across_writer_modes() {
    // Macro instead of a generic fn because each writer has its own
    // `run` method (no common trait beyond what JournalStageRun
    // exposes here, which is enough — but the macro keeps the test
    // shape identical to the original).
    macro_rules! run_for {
        ($writer_ty:ty) => {{
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("parity.journal");
            let writer = <$writer_ty>::create(&path).unwrap();

            let (mut producer, mut consumers) = ring::DisruptorBuilder::<TestInput>::new(64)
                .add_consumer()
                .build();
            let consumer = consumers.pop().unwrap();
            let stage = JournalStage::<TestEvent, $writer_ty>::new(
                writer,
                consumer,
                Duration::ZERO,
                MAX_JOURNAL_BATCH,
                false,
            );

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
            seqs
        }};
    }

    let sector = run_for!(SectorWriter<TestEvent>);
    let buffered = run_for!(BufferedWriter<TestEvent>);
    assert_eq!(
        sector, buffered,
        "writer modes diverged on app-event sequences"
    );
    assert_eq!(sector.len(), 5);
}
