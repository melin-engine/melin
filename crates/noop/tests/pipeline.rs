//! End-to-end architectural test: assemble `Pipeline<NoopApp>` from
//! `melin-transport-core` with no trace of `melin-engine` in the dep
//! tree. Publishes a handful of events, drives the matching stage to
//! shutdown, and verifies the expected number of noop-rejection reports
//! came out of the output ring.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use melin_journal::JournalWriter;
use melin_journal::trace::trace_ts;
use melin_noop::NoopApp;
use melin_trading::trading_event::TradingEvent;
use melin_trading::types::{AccountId, CurrencyId};
use melin_transport_core::pipeline::{InputSlot, OutputPayload, build_pipeline_with_replication};

#[test]
fn pipeline_with_noop_app_runs_events_to_output() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("noop.journal");
    let writer: JournalWriter<TradingEvent> = JournalWriter::create(&path).unwrap();

    let mut pipeline = build_pipeline_with_replication::<NoopApp>(
        NoopApp::new(),
        writer,
        Duration::ZERO,
        Arc::new(AtomicU64::new(0)),
        false, // enable_replication
        4096,  // max_journal_batch
        1 << 16,
        false, // busy_spin
        false, // enable_event_publisher
        false, // enable_shadow
    );

    let shutdown = Arc::new(AtomicBool::new(false));

    let matching_shutdown = Arc::clone(&shutdown);
    let matching_stage = pipeline.matching_stage;
    let matching_handle = std::thread::spawn(move || matching_stage.run(&matching_shutdown));

    let journal_shutdown = Arc::clone(&shutdown);
    let journal_stage = pipeline.journal_stage;
    let journal_handle = std::thread::spawn(move || {
        // Ignore the returned writer — the test cares about the report
        // path, not about cleanly reclaiming the writer.
        let _ = journal_stage.run(&journal_shutdown);
    });

    // Publish three deposit events. Noop ignores them beyond counting
    // and emitting a Rejected(NoLiquidity) per event.
    const N: u64 = 3;
    let producer = pipeline.input_producer.clone();
    for i in 0..N {
        producer.publish(InputSlot {
            connection_id: 1,
            key_hash: 0,
            request_seq: 0,
            sequence: 0,
            timestamp_ns: 1_000_000_000 + i,
            event: melin_journal::JournalEvent::App(TradingEvent::Deposit {
                account: AccountId(1),
                currency: CurrencyId(1),
                amount: 100 + i,
            }),
            publish_ts: trace_ts(),
            recv_ts: trace_ts(),
        });
    }
    drop(producer);

    // Give the stages a moment to process before signalling shutdown.
    let output = &mut pipeline.output_consumers[0];
    let mut reports_seen = 0u64;
    let mut batch_ends = 0u64;
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while reports_seen < N && std::time::Instant::now() < deadline {
        if let Some((_seq, slot)) = output.try_consume() {
            match slot.payload {
                OutputPayload::Report(_) => reports_seen += 1,
                OutputPayload::BatchEnd => batch_ends += 1,
                OutputPayload::QueryResponse(_) => {}
                OutputPayload::EngineError => panic!("unexpected engine error"),
            }
        } else {
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    shutdown.store(true, Ordering::Relaxed);
    let _app = matching_handle.join().unwrap();
    journal_handle.join().unwrap();

    assert_eq!(reports_seen, N, "expected one Rejected report per event");
    let _ = batch_ends; // BatchEnd markers trail the reports; the loop
    // may exit before consuming them — the report count is the
    // authoritative signal.
}
