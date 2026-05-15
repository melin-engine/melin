//! Application-agnostic pipeline tests.
//!
//! These exercise the journal stage, matching stage, and combined
//! pipeline against `TestApp` / `TestEvent` rather than any concrete
//! business engine. They used to live in `melin-engine` only because the
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

use melin_disruptor::padding::Sequence;
use melin_disruptor::ring;
use melin_journal::replication::REPLICATION_RING_CAPACITY;
use melin_journal::{BufferedWriter, JournalEvent, JournalReader, SectorWriter};

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

/// First user-event sequence: 2 with hash-chain (genesis takes 1), 1 without.
/// Only referenced from journal-reader assertions, which are themselves
/// gated on `not(no-persist)`.
#[cfg(all(feature = "hash-chain", not(feature = "no-persist")))]
const FIRST_SEQ: u64 = 2;
#[cfg(all(not(feature = "hash-chain"), not(feature = "no-persist")))]
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
#[allow(dead_code)]
fn add_slot_with_seq(n: u64, sequence: u64, timestamp_ns: u64) -> TestInput {
    InputSlot {
        sequence,
        ..add_slot(n, timestamp_ns)
    }
}

/// Drain the output ring, returning every report seen up to and
/// including the request-terminator slot.
#[allow(dead_code)]
fn collect_reports(output: &mut ring::Consumer<TestOutput>) -> Vec<TestReport> {
    let mut reports = Vec::new();
    loop {
        if let Some((_, slot)) = output.try_consume() {
            if let OutputPayload::Report(r) = slot.payload {
                reports.push(r);
            }
            if slot.is_last_in_request {
                return reports;
            }
        }
        std::hint::spin_loop();
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
        false,
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

    // Publish events with pre-assigned sequences (simulating replica mode).
    // Start at sequence 2: when the hash-chain feature is enabled,
    // SectorWriter::create writes a GenesisHash at sequence 1, so the
    // next expected sequence is 2. The reader enforces strict continuity.
    producer.publish(add_slot_with_seq(7, 2, 1_700_000_000_000_000_000));
    producer.publish(add_slot_with_seq(11, 3, 1_700_000_000_000_000_001));

    let handle = std::thread::spawn(move || stage.run(&shutdown2));

    std::thread::sleep(Duration::from_millis(50));
    shutdown.store(true, Ordering::Relaxed);
    let _writer = handle.join().unwrap();

    #[cfg(not(feature = "no-persist"))]
    {
        let mut reader = JournalReader::<TestEvent>::open(&path).unwrap();

        let entry1 = reader.next_entry().unwrap().unwrap();
        assert_eq!(entry1.sequence, 2);
        assert_eq!(entry1.timestamp_ns, 1_700_000_000_000_000_000);
        assert!(matches!(entry1.event, JournalEvent::App(TestEvent::Add(7))));

        let entry2 = reader.next_entry().unwrap().unwrap();
        assert_eq!(entry2.sequence, 3);
        assert_eq!(entry2.timestamp_ns, 1_700_000_000_000_000_001);
        assert!(matches!(
            entry2.event,
            JournalEvent::App(TestEvent::Add(11))
        ));

        assert!(reader.next_entry().unwrap().is_none());
    }
}

/// Verify that the JournalStage detects divergence when a primary
/// checkpoint carries a chain hash that doesn't match the replica's.
/// The stage must return a fatal error, not silently continue.
#[cfg(feature = "hash-chain")]
#[test]
fn divergence_detected_on_checkpoint_hash_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("divergence.journal");

    let writer = Writer::create(&path).unwrap();

    let (mut producer, mut consumers) = ring::DisruptorBuilder::<TestInput>::new(64)
        .add_consumer()
        .build();

    let consumer = consumers.pop().unwrap();
    let stage = JournalStage::new(writer, consumer, Duration::ZERO, MAX_JOURNAL_BATCH, false);

    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown2 = Arc::clone(&shutdown);

    // Normal event with pre-assigned sequence, then a checkpoint whose
    // chain hash deliberately doesn't match anything the stage could
    // have computed locally.
    producer.publish(add_slot_with_seq(1, 100, 1_000_000_000));
    producer.publish(InputSlot {
        connection_id: 0,
        key_hash: 0,
        request_seq: 0,
        sequence: 101,
        timestamp_ns: 1_000_000_001,
        event: JournalEvent::Checkpoint {
            chain_hash: [0xFF; 32],
            events_since_checkpoint: 1,
        },
        publish_ts: mono_trace_ns(),
        recv_ts: mono_trace_ns(),
    });

    let handle = std::thread::spawn(move || stage.run(&shutdown2));

    std::thread::sleep(Duration::from_millis(100));
    shutdown.store(true, Ordering::Relaxed);
    let result = handle.join().unwrap();

    match result {
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("divergence detected"),
                "error should mention divergence: {msg}"
            );
        }
        Ok(_) => panic!("expected divergence error, got Ok"),
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
        );
        assert!(out.replication_consumers.is_some());
        assert_eq!(
            out.replication_cursor.load(Ordering::Relaxed),
            u64::MAX,
            "replication cursor should start at MAX even when enabled"
        );
    }
}

/// Regression guard for the production failure mode:
///
///     error at entry 100001: sequence gap: expected N+1, got N
///
/// reported by `journal_verify` after a dual-replica LAN bench run.
/// The signature (expected = last + 1, actual = last) is produced by
/// the reader when an auto-emitted Checkpoint at seq X is followed
/// by a normal event that re-uses seq X — the Checkpoint is skipped
/// transparently, advances the reader's internal `last_sequence` to
/// X, then the duplicate event fails the strict-continuity check.
#[cfg(all(feature = "hash-chain", not(feature = "no-persist")))]
#[test]
fn primary_journal_sequences_contiguous_across_checkpoint_boundary() {
    use melin_journal::sector_writer::checkpoint_interval;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("checkpoint_boundary.journal");
    let writer = Writer::create(&path).unwrap();

    let total: u64 = checkpoint_interval() * 2 + 100;
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
            Err(e) => panic!(
                "journal read error after {count} user entries \
                 (last_sequence = {:?}): {e}",
                reader.last_sequence()
            ),
        }
    }
    assert_eq!(
        count, total,
        "expected all {total} user events to be recoverable from the journal"
    );
}

/// End-to-end primary → replica test. The primary's journal stage
/// publishes replication batches; a relay thread decodes the wire
/// frames and republishes them onto the replica's input ring (skipping
/// the primary's checkpoint frames, since the replica re-derives its
/// chain hash from its own genesis here). Both journals must end up
/// with contiguous app sequences covering every published event.
#[cfg(all(feature = "hash-chain", not(feature = "no-persist")))]
#[test]
fn primary_and_replica_journals_contiguous_across_checkpoint_boundary() {
    use melin_journal::sector_writer::checkpoint_interval;

    let dir = tempfile::tempdir().unwrap();
    let primary_path = dir.path().join("primary.journal");
    let replica_path = dir.path().join("replica.journal");

    // Shared genesis hash so the two writers seed identical BLAKE3
    // chains. In production the replica gets this via snapshot
    // transfer; here we hard-code it so the chain-hash divergence
    // check inside the replica's JournalStage doesn't short-circuit
    // the test at the first auto-emitted Checkpoint.
    let shared_genesis = [0xA5u8; 32];

    // -------- primary --------
    let primary_writer = Writer::create_continuing(&primary_path, 1, shared_genesis).unwrap();
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
    );

    // -------- replica --------
    let replica_writer = Writer::create_continuing(&replica_path, 1, shared_genesis).unwrap();
    let replica = build_replica_pipeline(
        TestApp::new(),
        replica_writer,
        MAX_JOURNAL_BATCH,
        Duration::ZERO,
        false,
        false,
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
                    // Skip the primary's auto-emitted Checkpoint frames:
                    // the replica seeds its chain hash from its own (test-
                    // local) genesis, so verify_primary_checkpoint would
                    // diverge on the first one and kill the replica
                    // JournalStage. The replica still auto-emits its own
                    // Checkpoints at the same sequence positions.
                    if matches!(slot.event, JournalEvent::Checkpoint { .. }) {
                        continue;
                    }
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

    let total: u64 = checkpoint_interval() * 5 + 250;
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

    let scan = |label: &str, path: &std::path::Path| -> u64 {
        let mut reader = JournalReader::<TestEvent>::open(path).unwrap();
        let mut count = 0u64;
        loop {
            match reader.next_entry() {
                Ok(Some(_)) => count += 1,
                Ok(None) => break,
                Err(e) => panic!(
                    "{label} journal read error after {count} user entries \
                     (last_sequence = {:?}): {e}",
                    reader.last_sequence()
                ),
            }
        }
        count
    };

    let primary_count = scan("primary", &primary_path);
    let replica_count = scan("replica", &replica_path);

    assert_eq!(
        primary_count, total,
        "expected all {total} user events recoverable from the primary journal"
    );
    assert_eq!(
        replica_count, total,
        "expected all {total} user events recoverable from the replica journal"
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

    // Read each segment directly. Collect every entry's sequence,
    // skipping the GenesisHash anchors used for chain continuity.
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
