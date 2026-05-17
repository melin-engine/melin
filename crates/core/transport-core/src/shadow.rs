//! Shadow snapshot stage — replays journal events on a cloned application to
//! produce periodic snapshots without blocking the hot path.
//!
//! Generic over `A: Application`. The shadow consumer is gated on the journal
//! stage (sees only fsynced events), so snapshots are always consistent with
//! durable state. The chain hash is read from a SeqLock published by the
//! journal stage after each fsync batch.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tracing::{error, info};

use crate::pipeline::InputSlot;
use crate::snapshot;
use melin_app::amortized_timer::AmortizedTimer;
use melin_app::{Application, ApplyCtx};
use melin_disruptor::ring;
use melin_disruptor::seqlock::SeqLock;
use melin_journal::JournalEvent;

/// Maximum events consumed per batch. Matches the journal stage batch size
/// for consistent throughput characteristics.
const SHADOW_BATCH_SIZE: usize = 4096;

/// Spin-wait idle hint — same pattern as other pipeline stages.
#[inline(always)]
fn idle_wait(idle_spins: &mut u32, busy_spin: bool) {
    if busy_spin || *idle_spins < 1000 {
        *idle_spins = idle_spins.wrapping_add(1);
        std::hint::spin_loop();
    } else {
        std::thread::yield_now();
    }
}

/// Run the shadow snapshot stage.
///
/// Consumes events from the input ring (gated on journal fsync), replays them
/// on a cloned application, and saves periodic snapshots with the BLAKE3 chain
/// hash read from the journal stage's SeqLock.
pub fn run<A: Application>(
    mut consumer: ring::Consumer<InputSlot<A::Event>>,
    mut app: A,
    snapshot_path: PathBuf,
    snapshot_interval: Duration,
    chain_hash_lock: Arc<SeqLock<[u8; 32]>>,
    shutdown: &AtomicBool,
    busy_spin: bool,
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
    let mut idle_spins: u32 = 0;
    // Track whether any events have been consumed. Prevents snapshotting
    // empty state before the first event arrives.
    let mut has_events = false;
    // Highest event timestamp the shadow's scheduler has drained against.
    // See `dispatch_event` for the per-event drain rationale.
    let mut last_drain_ns: u64 = 0;

    loop {
        if shutdown.load(Ordering::Relaxed) {
            info!("shadow stage shutting down");
            return;
        }

        let count = consumer.consume_batch(&mut batch, SHADOW_BATCH_SIZE);
        if count == 0 {
            // Check snapshot timer even when idle — events may have been
            // consumed before the interval elapsed, and no more events
            // will arrive to trigger the post-consume check.
            if has_events
                && snapshot_timer
                    .tick(snapshot_interval, busy_spin || idle_spins < 1000)
                    .is_some()
            {
                let last_seq = consumer.next_read().saturating_sub(1);
                save_snapshot::<A>(&app, last_seq, &chain_hash_lock, &snapshot_path);
            }
            idle_wait(&mut idle_spins, busy_spin);
            continue;
        }
        idle_spins = 0;
        has_events = true;

        // Replay each event on the shadow app. last_drain_ns lives
        // outside the loop so the per-event drain stays monotonic across
        // batches.
        for slot in &batch[..count] {
            dispatch_event(
                &mut app,
                &slot.event,
                slot.timestamp_ns,
                slot.key_hash,
                slot.request_seq,
                &mut last_drain_ns,
                &mut reports,
            );
        }

        // Check if a snapshot is due.
        if snapshot_timer.tick(snapshot_interval, true).is_some() {
            let last_seq = consumer.next_read() - 1;
            save_snapshot::<A>(&app, last_seq, &chain_hash_lock, &snapshot_path);
        }
    }
}

/// Save a shadow snapshot, logging success or failure.
fn save_snapshot<A: Application>(
    app: &A,
    sequence: u64,
    chain_hash_lock: &Arc<SeqLock<[u8; 32]>>,
    path: &std::path::Path,
) {
    let chain_hash = chain_hash_lock.load();
    match snapshot::save::<A>(app, sequence, chain_hash, path) {
        Ok(()) => {
            info!(
                sequence,
                path = %path.display(),
                "shadow snapshot saved"
            );
        }
        Err(e) => {
            error!(
                sequence,
                error = %e,
                path = %path.display(),
                "shadow snapshot failed"
            );
        }
    }
}

/// Dispatch a single journal event to the shadow app.
///
/// Mirrors `JournaledApp::replay_entry`: rebuild per-key HWM via
/// `check_request_seq`, drain the scheduler clock if `timestamp_ns`
/// advanced, then hand the event to `apply` or `tick`. Without the
/// `check_request_seq` call, the shadow snapshot's `key_hwm` would be
/// empty and a restore would let previously-rejected duplicate
/// `request_seq` values through. `last_drain_ns` is caller-tracked
/// across the consume loop so the drain stays monotonic.
pub fn dispatch_event<A: Application>(
    app: &mut A,
    event: &JournalEvent<A::Event>,
    timestamp_ns: u64,
    key_hash: u64,
    request_seq: u64,
    last_drain_ns: &mut u64,
    reports: &mut Vec<A::Report>,
) {
    reports.clear();

    // Gate on `!is_query` to match the matching stage (`pipeline.rs`
    // `check_request_seq` call site). The shadow reads from the pre-journal
    // input ring — unlike `JournaledApp::replay_entry`, which sees only
    // non-queries because the journal stage drops queries — so advancing
    // HWM on queries here would push shadow's `key_hwm` above primary's and
    // cause post-restore to reject legitimate non-duplicate requests.
    // Return discarded: shadow applies the event regardless of the dedup
    // decision (matches `replay_entry` for non-queries).
    if !event.is_query() {
        let _ = app.check_request_seq(key_hash, request_seq);
    }

    if timestamp_ns > *last_drain_ns {
        *last_drain_ns = timestamp_ns;
        app.tick(timestamp_ns, reports);
    }

    match *event {
        JournalEvent::App(e) => {
            // The shadow is strictly a secondary observer — the canonical
            // answer (and journal sequence number) is produced by the
            // matching stage. `ApplyCtx` is supplied with the fields the
            // shadow can cheaply compute; `journal_sequence` / connection
            // counts are live-pipeline-only. `key_hash` is threaded so
            // that any self-introspecting query the app supports stays
            // consistent between live and shadow paths.
            let ctx = ApplyCtx {
                now_ns: timestamp_ns,
                journal_sequence: 0,
                active_connections: 0,
                events_processed: 0,
                key_hash,
            };
            // Query response discarded — shadow is a secondary observer,
            // it does not produce client-facing output.
            let _ = app.apply(e, &ctx, reports);
        }
        JournalEvent::Tick { now_ns } => {
            // Defensive: the head-of-event drain typically already advanced
            // the clock to this point. Re-draining via `now_ns` keeps the
            // contract consistent for callers that pass `timestamp_ns = 0`
            // (tests, manually constructed events).
            app.tick(now_ns, reports);
        }
        JournalEvent::GenesisHash { .. } | JournalEvent::Checkpoint { .. } => {
            // Hash chain metadata — no application state change.
        }
        JournalEvent::Shutdown => {
            // Pipeline-only sentinel — handled at the run-loop level by
            // exiting; should never reach this dispatch.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::InputSlot;
    use crate::test_support::{TestApp, TestEvent};
    use melin_disruptor::ring::DisruptorBuilder;
    use std::time::Instant;

    #[test]
    fn shadow_shutdown_exits_promptly() {
        let (_, mut consumers) = DisruptorBuilder::<InputSlot<TestEvent>>::new(64)
            .add_consumer()
            .build();
        let consumer = consumers.pop().unwrap();

        let app = TestApp::new();
        let chain_hash = Arc::new(SeqLock::new([0u8; 32]));
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
                    chain_hash,
                    &shutdown2,
                    false,
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
            .build();
        let consumer = consumers.pop().unwrap();

        let app = TestApp::new();
        let chain_hash = Arc::new(SeqLock::new([0xAB; 32]));
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
                    chain_hash,
                    &shutdown2,
                    false,
                );
            })
            .unwrap();

        // Publish both events before the interval elapses so the snapshot
        // captures both adds. The idle-check fires the snapshot after the
        // 50ms interval even without new events arriving.
        producer.publish(InputSlot {
            connection_id: 0,
            key_hash: 0,
            request_seq: 0,
            sequence: 0,
            timestamp_ns: 0,
            event: JournalEvent::App(TestEvent::Add(1000)),
            publish_ts: Default::default(),
            recv_ts: Default::default(),
        });
        producer.publish(InputSlot {
            connection_id: 0,
            key_hash: 0,
            request_seq: 0,
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
        let (restored, _seq, chain) = snapshot::load::<TestApp>(&snap_path).unwrap();
        assert_eq!(chain, [0xAB; 32]); // chain hash from SeqLock
        assert_eq!(restored.total, 1500);
    }
}
