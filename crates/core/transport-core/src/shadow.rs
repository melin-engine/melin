//! Shadow snapshot stage — replays journal events on a cloned application to
//! produce periodic snapshots without blocking the hot path.
//!
//! Generic over `A: Application`. The shadow consumer is gated on the journal
//! stage (sees only fsynced events), so snapshots are always consistent with
//! durable state. The chain hash is read from a seqlock published by the
//! journal stage after each fsync batch.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tracing::{error, info};

use crate::dispatch::dispatch;
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
pub fn run<A: Application>(
    mut consumer: ring::Consumer<InputSlot<A::Event>>,
    mut app: A,
    snapshot_path: PathBuf,
    snapshot_interval: Duration,
    fsync_state: SeqLockReader<FsyncState>,
    shutdown: &AtomicBool,
    wait: WaitStrategy,
    initial_epoch: u64,
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
    let mut waiter = wait.waiter();
    // Track whether any events have been consumed. Prevents snapshotting
    // empty state before the first event arrives.
    let mut has_events = false;
    // Fencing epoch as of the shadow's consumed position. Seeded from the
    // recovered epoch (the live pipeline's starting epoch) because the
    // shadow only sees events published *after* boot — any `EpochBump`
    // already folded into the recovered app state never crosses the ring.
    // Advanced by replaying `EpochBump` events and stamped into each
    // snapshot so a snapshot-bootstrapped node restores the right epoch.
    let mut shadow_epoch: u64 = initial_epoch;

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
                    .tick(snapshot_interval, waiter.spinning())
                    .is_some()
            {
                try_save_snapshot::<A>(&app, &consumer, &fsync_state, &snapshot_path, shadow_epoch);
            }
            waiter.idle();
            continue;
        }
        waiter.reset();
        has_events = true;

        // Replay each event on the shadow app, through the same stateless
        // dispatch as the matching stage and recovery.
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
            // The shadow produces no output: the matching stage replies
            // for every event.
            dispatch(
                &mut app,
                slot.event,
                &ApplyCtx {
                    now: slot.timestamp,
                    key_hash: slot.key_hash,
                },
                |epoch| crate::fence::observe_into(&mut shadow_epoch, epoch),
                &mut reports,
            );
            reports.clear();
        }

        // Check if a snapshot is due.
        if snapshot_timer.tick(snapshot_interval, true).is_some() {
            try_save_snapshot::<A>(&app, &consumer, &fsync_state, &snapshot_path, shadow_epoch);
        }
    }
}

/// Save a shadow snapshot if the shadow's ring cursor is aligned with
/// the journal stage's last fsync boundary. When aligned, journal_seq,
/// chain_hash and last_timestamp from [`FsyncState`] correspond exactly
/// to the shadow's app state.
///
/// When not aligned (shadow mid-batch or journal fsynced again since
/// shadow's last consume), the snapshot is deferred — the next timer
/// tick retries.
fn try_save_snapshot<A: Application>(
    app: &A,
    consumer: &ring::Consumer<InputSlot<A::Event>>,
    fsync_state: &SeqLockReader<FsyncState>,
    path: &std::path::Path,
    epoch: u64,
) {
    let state = fsync_state.load();
    // Both ring-index space — compare the raw positions.
    if state.input_ring_seq.get() != consumer.next_read() {
        return;
    }
    match snapshot::save::<A>(
        app,
        state.journal_seq,
        state.chain_hash,
        epoch,
        state.last_timestamp,
        path,
    ) {
        Ok(()) => {
            info!(
                journal_seq = state.journal_seq.get(),
                path = %path.display(),
                "shadow snapshot saved"
            );
        }
        Err(e) => {
            error!(
                journal_seq = state.journal_seq.get(),
                error = %e,
                path = %path.display(),
                "shadow snapshot failed"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cursors::{RingPos, WireSeq};
    use crate::pipeline::InputSlot;
    use crate::test_support::{TestApp, TestEvent};
    use melin_app::SequencerTime;
    use melin_journal::JournalEvent;
    use melin_pipeline::ring::DisruptorBuilder;
    use melin_pipeline::seqlock;
    use std::sync::Arc;
    use std::time::Instant;

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
                    0, // initial_epoch
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
            last_timestamp: SequencerTime::from_ns(7_000),
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
                    0, // initial_epoch
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
            timestamp: SequencerTime::default(),
            event: JournalEvent::App(TestEvent::Add(1000)),
            publish_ts: Default::default(),
            recv_ts: Default::default(),
        });
        producer.publish(InputSlot {
            connection_id: 0,
            key_hash: 0,
            sequence: 0,
            timestamp: SequencerTime::default(),
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
        let loaded = snapshot::load::<TestApp>(&snap_path).unwrap();
        assert_eq!(loaded.chain_hash, [0xAB; 32]); // chain hash from the seqlock
        // The stamp too comes from the seqlock, the journal stage's, and
        // not from the shadow's last slot (these slots carry zero).
        assert_eq!(
            loaded.floor,
            melin_journal::TimeFloor::After(SequencerTime::from_ns(7_000))
        );
        assert_eq!(loaded.app.total, 1500);
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
            last_timestamp: SequencerTime::default(),
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
                    0, // initial_epoch
                );
            })
            .unwrap();

        // A query under a client key, with a timestamp that would drain
        // the clock, then a write no client submitted.
        producer.publish(InputSlot {
            connection_id: 1,
            key_hash: KEY,
            sequence: 0,
            timestamp: SequencerTime::from_ns(1_000),
            event: JournalEvent::App(TestEvent::Query),
            publish_ts: Default::default(),
            recv_ts: Default::default(),
        });
        producer.publish(InputSlot {
            connection_id: 0,
            key_hash: 0,
            sequence: 0,
            timestamp: SequencerTime::default(),
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

        let restored = snapshot::load::<TestApp>(&snap_path).unwrap().app;
        assert_eq!(restored.total, 7, "the write after the query is applied");
        assert_eq!(
            restored.ticks, 1,
            "one tick for the write; a query must not advance the clock"
        );
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
                    0, // initial_epoch
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
            last_timestamp: SequencerTime::default(),
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
                    0, // initial_epoch
                );
            })
            .unwrap();

        // Phase 1: publish one event, wait for first snapshot.
        producer.publish(InputSlot {
            connection_id: 0,
            key_hash: 0,
            sequence: 0,
            timestamp: SequencerTime::default(),
            event: JournalEvent::App(TestEvent::Add(1)),
            publish_ts: Default::default(),
            recv_ts: Default::default(),
        });

        let deadline = Instant::now() + Duration::from_secs(10);
        while !snap_path.exists() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(snap_path.exists(), "first snapshot must be written");
        let hash_initial = snapshot::load::<TestApp>(&snap_path).unwrap().chain_hash;
        assert_eq!(hash_initial, [0x11; 32], "first snapshot has initial hash");

        // Phase 2: update FsyncState (new hash + advanced ring cursor),
        // drive another event, wait for the second snapshot.
        fsync_state_writer.store(FsyncState {
            journal_seq: WireSeq::new(3),
            chain_hash: [0x22; 32],
            last_timestamp: SequencerTime::default(),
            input_ring_seq: RingPos::new(2),
        });
        producer.publish(InputSlot {
            connection_id: 0,
            key_hash: 0,
            sequence: 0,
            timestamp: SequencerTime::default(),
            event: JournalEvent::App(TestEvent::Add(1)),
            publish_ts: Default::default(),
            recv_ts: Default::default(),
        });

        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Ok(loaded) = snapshot::load::<TestApp>(&snap_path)
                && loaded.chain_hash == [0x22; 32]
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
        // reflects the running total. Add semantics make the sum
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
            last_timestamp: SequencerTime::default(),
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
                    0, // initial_epoch
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
                    timestamp: SequencerTime::default(),
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
            if let Ok(loaded) = snapshot::load::<TestApp>(&snap_path)
                && loaded.app.total == 150
            {
                break;
            }
            if Instant::now() >= deadline {
                let observed = snapshot::load::<TestApp>(&snap_path)
                    .map(|loaded| loaded.app.total)
                    .unwrap_or(u64::MAX);
                panic!("snapshot did not reach total=150 (observed={observed})");
            }
            std::thread::sleep(Duration::from_millis(5));
        }

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }
}
