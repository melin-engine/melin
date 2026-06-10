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

use crate::pipeline::{FsyncState, InputSlot};
use crate::snapshot;
use melin_app::amortized_timer::AmortizedTimer;
use melin_app::{Application, ApplyCtx};
use melin_journal::JournalEvent;
use melin_pipeline::ring;
use melin_pipeline::seqlock::SeqLock;

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
/// on a cloned application, and saves periodic snapshots. The snapshot's
/// journal sequence and chain hash are read from the journal stage's
/// [`FsyncState`] SeqLock — only saved when the shadow's ring cursor
/// matches the fsync boundary, guaranteeing the triple (app state,
/// journal_seq, chain_hash) is self-consistent.
pub fn run<A: Application>(
    mut consumer: ring::Consumer<InputSlot<A::Event>>,
    mut app: A,
    snapshot_path: PathBuf,
    snapshot_interval: Duration,
    fsync_state: Arc<SeqLock<FsyncState>>,
    shutdown: &AtomicBool,
    busy_spin: bool,
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
    let mut idle_spins: u32 = 0;
    // Track whether any events have been consumed. Prevents snapshotting
    // empty state before the first event arrives.
    let mut has_events = false;
    // Highest event timestamp the shadow's scheduler has drained against.
    // See `dispatch_event` for the per-event drain rationale.
    let mut last_drain_ns: u64 = 0;
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
                    .tick(snapshot_interval, busy_spin || idle_spins < 1000)
                    .is_some()
            {
                try_save_snapshot::<A>(&app, &consumer, &fsync_state, &snapshot_path, shadow_epoch);
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
                &mut shadow_epoch,
                &mut reports,
            );
        }

        // Check if a snapshot is due.
        if snapshot_timer.tick(snapshot_interval, true).is_some() {
            try_save_snapshot::<A>(&app, &consumer, &fsync_state, &snapshot_path, shadow_epoch);
        }
    }
}

/// Save a shadow snapshot if the shadow's ring cursor is aligned with
/// the journal stage's last fsync boundary. When aligned, journal_seq
/// and chain_hash from [`FsyncState`] correspond exactly to the
/// shadow's app state.
///
/// When not aligned (shadow mid-batch or journal fsynced again since
/// shadow's last consume), the snapshot is deferred — the next timer
/// tick retries.
fn try_save_snapshot<A: Application>(
    app: &A,
    consumer: &ring::Consumer<InputSlot<A::Event>>,
    fsync_state: &SeqLock<FsyncState>,
    path: &std::path::Path,
    epoch: u64,
) {
    let state = fsync_state.load();
    if state.input_ring_seq != consumer.next_read() {
        return;
    }
    match snapshot::save::<A>(app, state.journal_seq, state.chain_hash, epoch, path) {
        Ok(()) => {
            info!(
                journal_seq = state.journal_seq,
                path = %path.display(),
                "shadow snapshot saved"
            );
        }
        Err(e) => {
            error!(
                journal_seq = state.journal_seq,
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
fn dispatch_event<A: Application>(
    app: &mut A,
    event: &JournalEvent<A::Event>,
    timestamp_ns: u64,
    key_hash: u64,
    request_seq: u64,
    last_drain_ns: &mut u64,
    epoch: &mut u64,
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
        JournalEvent::EpochBump { epoch: bump } => {
            // Lineage metadata — advance the shadow's tracked epoch so the
            // next snapshot records it. Never touches application state.
            *epoch = (*epoch).max(bump);
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
    use melin_pipeline::ring::DisruptorBuilder;
    use std::time::Instant;

    #[test]
    fn shadow_shutdown_exits_promptly() {
        let (_, mut consumers) = DisruptorBuilder::<InputSlot<TestEvent>>::new(64)
            .add_consumer()
            .build();
        let consumer = consumers.pop().unwrap();

        let app = TestApp::new();
        let fsync_state = Arc::new(SeqLock::new(FsyncState::default()));
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
                    false,
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
            .build();
        let consumer = consumers.pop().unwrap();

        let app = TestApp::new();
        // Pre-set input_ring_seq = 2 (the ring cursor after consuming
        // both events below). Shadow only saves when aligned.
        let fsync_state = Arc::new(SeqLock::new(FsyncState {
            journal_seq: 3,
            chain_hash: [0xAB; 32],
            input_ring_seq: 2,
        }));
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
                    false,
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
        let (restored, _seq, chain, _epoch) = snapshot::load::<TestApp>(&snap_path).unwrap();
        assert_eq!(chain, [0xAB; 32]); // chain hash from SeqLock
        assert_eq!(restored.total, 1500);
    }

    // ------------------------------------------------------------------
    // dispatch_event contract tests
    //
    // dispatch_event has four observable behaviours:
    //   - App events advance per-key HWM (gated on !is_query) and reach
    //     Application::apply
    //   - Query events skip the HWM advance
    //   - `timestamp_ns` drives a monotonic Application::tick drain
    //   - Transport variants (Tick / Shutdown)
    //     are handled without touching app-event state
    //
    // Each test below pins one of those behaviours. The fixture is the
    // app-agnostic TestApp — we used to cross-check against a real
    // trading Exchange + its direct method API, but that's the engine's
    // job; here we only validate dispatch_event's control flow.
    // ------------------------------------------------------------------

    const KEY: u64 = 0xDEAD_BEEF;

    fn dispatch(app: &mut TestApp, event: &JournalEvent<TestEvent>, ts: u64, seq: u64) {
        let mut reports = Vec::new();
        let mut drain = 0u64;
        let mut epoch = 0u64;
        dispatch_event(
            app,
            event,
            ts,
            KEY,
            seq,
            &mut drain,
            &mut epoch,
            &mut reports,
        );
    }

    #[test]
    fn app_event_advances_hwm_and_reaches_apply() {
        let mut app = TestApp::new();
        let mut reports = Vec::new();
        let mut drain = 0u64;

        // Non-query Add: HWM should bump to seq, total should bump by n.
        dispatch_event(
            &mut app,
            &JournalEvent::App(TestEvent::Add(42)),
            0,
            KEY,
            10,
            &mut drain,
            &mut 0u64,
            &mut reports,
        );

        assert_eq!(app.total, 42, "apply must have run");
        assert_eq!(app.key_hwm.get(&KEY).copied(), Some(10));
    }

    #[test]
    fn query_event_does_not_advance_hwm() {
        // Regression: the shadow reads from the pre-journal input ring so
        // it sees queries (the matching stage filters them out at the
        // !is_query gate). Advancing HWM on queries would push shadow's
        // key_hwm above primary's and a post-restore could reject
        // legitimate non-duplicate requests.
        let mut app = TestApp::new();
        dispatch(&mut app, &JournalEvent::App(TestEvent::Query), 0, 100);

        // HWM unchanged, so a same-seq non-query still passes.
        assert!(app.key_hwm.get(&KEY).copied().unwrap_or(0) < 100);
        assert!(app.check_request_seq(KEY, 100));
    }

    #[test]
    fn timestamp_drives_monotonic_clock_drain() {
        // The drain trips Application::tick when `timestamp_ns >
        // last_drain_ns`. Across one dispatch_event call the local
        // `last_drain_ns` is initialised to 0 so any positive timestamp
        // fires exactly one tick. A second dispatch with a backward
        // timestamp on the same caller-tracked drain must NOT re-tick.
        let mut app = TestApp::new();
        let mut reports = Vec::new();
        let mut drain = 0u64;

        // Forward timestamp: one tick.
        dispatch_event(
            &mut app,
            &JournalEvent::App(TestEvent::Add(1)),
            100,
            KEY,
            1,
            &mut drain,
            &mut 0u64,
            &mut reports,
        );
        assert_eq!(app.ticks, 1, "forward timestamp must drain clock once");
        assert_eq!(drain, 100, "caller-tracked drain must advance to 100");

        // Backward timestamp on the same caller-tracked drain: no new tick.
        dispatch_event(
            &mut app,
            &JournalEvent::App(TestEvent::Add(1)),
            50,
            KEY,
            2,
            &mut drain,
            &mut 0u64,
            &mut reports,
        );
        assert_eq!(app.ticks, 1, "backward timestamp must not re-drain");

        // Equal timestamp: also no new tick (strict greater-than gate).
        dispatch_event(
            &mut app,
            &JournalEvent::App(TestEvent::Add(1)),
            100,
            KEY,
            3,
            &mut drain,
            &mut 0u64,
            &mut reports,
        );
        assert_eq!(app.ticks, 1, "equal timestamp must not re-drain");

        // New forward timestamp resumes draining.
        dispatch_event(
            &mut app,
            &JournalEvent::App(TestEvent::Add(1)),
            200,
            KEY,
            4,
            &mut drain,
            &mut 0u64,
            &mut reports,
        );
        assert_eq!(app.ticks, 2);
    }

    #[test]
    fn tick_variant_advances_clock_state_only() {
        // JournalEvent::Tick { now_ns } reaches Application::tick (which
        // bumps TestApp::ticks) and never reaches apply (which would bump
        // TestApp::total).
        let mut app = TestApp::new();
        dispatch(&mut app, &JournalEvent::Tick { now_ns: 1_000 }, 0, 1);

        assert_eq!(app.total, 0, "Tick variant must not call apply");
        assert!(app.ticks >= 1, "Tick variant must call Application::tick");
    }

    #[test]
    fn transport_variants_are_state_noops() {
        // Shutdown carries pipeline-control metadata and must never
        // mutate app state. (It shouldn't even reach dispatch_event in
        // practice — the run loop exits on it — but the match arm exists
        // as defence in depth and is exercised here.)
        let mut app = TestApp::new();
        dispatch(&mut app, &JournalEvent::Shutdown, 0, 3);

        assert_eq!(app.total, 0, "no app-event state change");
        assert_eq!(app.ticks, 0, "no clock drain (timestamp_ns was 0)");
    }

    #[test]
    fn key_hash_zero_bypasses_hwm_dedup() {
        // Transport-internal events (Tick) and
        // any seed-time inserts use key_hash=0 to opt out of per-key
        // dedup. dispatch_event must hand those events to apply
        // regardless of the request_seq value — TestApp::check_request_seq
        // mirrors Exchange::check_request_seq in returning true for
        // key_hash=0 without consulting the HWM map.
        let mut app = TestApp::new();
        let mut reports = Vec::new();
        let mut drain = 0u64;

        for _ in 0..3 {
            dispatch_event(
                &mut app,
                &JournalEvent::App(TestEvent::Add(7)),
                0,
                0, // key_hash sentinel
                1, // same seq each time — would be a duplicate for any real key
                &mut drain,
                &mut 0u64,
                &mut reports,
            );
        }
        assert_eq!(app.total, 21, "every internal event must apply");
        assert!(
            app.key_hwm.is_empty(),
            "key_hash=0 must not allocate an HWM entry"
        );
    }

    #[test]
    fn duplicate_request_seq_still_applies_event() {
        // dispatch_event discards check_request_seq's return value — even
        // when the matching stage would have rejected the event as a
        // duplicate, the shadow still applies it. This mirrors
        // JournaledApp::replay_entry's non-query branch, and the
        // shadow_vs_primary divergence assumes both paths apply the same
        // bytes regardless of dedup outcome. Without this, a primary
        // that re-replays the same journal segment would diverge from a
        // shadow that skipped duplicates.
        let mut app = TestApp::new();
        let mut reports = Vec::new();
        let mut drain = 0u64;

        // First dispatch at seq=10 — advances HWM and applies.
        dispatch_event(
            &mut app,
            &JournalEvent::App(TestEvent::Add(5)),
            0,
            KEY,
            10,
            &mut drain,
            &mut 0u64,
            &mut reports,
        );
        assert_eq!(app.total, 5);
        assert_eq!(app.key_hwm.get(&KEY).copied(), Some(10));

        // Second dispatch at seq=10 — dedup gate would reject (seq not
        // strictly greater than HWM), but apply still runs.
        dispatch_event(
            &mut app,
            &JournalEvent::App(TestEvent::Add(5)),
            0,
            KEY,
            10,
            &mut drain,
            &mut 0u64,
            &mut reports,
        );
        assert_eq!(
            app.total, 10,
            "apply must run even when dedup would have rejected"
        );
        assert_eq!(
            app.key_hwm.get(&KEY).copied(),
            Some(10),
            "HWM must not regress"
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
            .build();
        let consumer = consumers.pop().unwrap();

        let app = TestApp::new();
        let fsync_state = Arc::new(SeqLock::new(FsyncState::default()));
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
                    false,
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
            .build();
        let consumer = consumers.pop().unwrap();

        let app = TestApp::new();
        let fsync_state = Arc::new(SeqLock::new(FsyncState {
            journal_seq: 2,
            chain_hash: [0x11; 32],
            input_ring_seq: 1,
        }));
        let fsync_state_writer = Arc::clone(&fsync_state);
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
                    false,
                    0, // initial_epoch
                );
            })
            .unwrap();

        // Phase 1: publish one event, wait for first snapshot.
        producer.publish(InputSlot {
            connection_id: 0,
            key_hash: 0,
            request_seq: 0,
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
        let (_, _, hash_initial, _) = snapshot::load::<TestApp>(&snap_path).unwrap();
        assert_eq!(hash_initial, [0x11; 32], "first snapshot has initial hash");

        // Phase 2: update FsyncState (new hash + advanced ring cursor),
        // drive another event, wait for the second snapshot.
        fsync_state_writer.store(FsyncState {
            journal_seq: 3,
            chain_hash: [0x22; 32],
            input_ring_seq: 2,
        });
        producer.publish(InputSlot {
            connection_id: 0,
            key_hash: 0,
            request_seq: 0,
            sequence: 0,
            timestamp_ns: 0,
            event: JournalEvent::App(TestEvent::Add(1)),
            publish_ts: Default::default(),
            recv_ts: Default::default(),
        });

        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Ok((_, _, hash, _)) = snapshot::load::<TestApp>(&snap_path)
                && hash == [0x22; 32]
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
            .build();
        let consumer = consumers.pop().unwrap();

        let app = TestApp::new();
        // 5 events total (10,20,30,40,50). Pre-set input_ring_seq = 5
        // so the alignment check passes once shadow consumes all.
        let fsync_state = Arc::new(SeqLock::new(FsyncState {
            journal_seq: 6,
            chain_hash: [0xCD; 32],
            input_ring_seq: 5,
        }));
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
                    false,
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
                    request_seq: 0,
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
            if let Ok((restored, _, _, _)) = snapshot::load::<TestApp>(&snap_path)
                && restored.total == 150
            {
                break;
            }
            if Instant::now() >= deadline {
                let observed = snapshot::load::<TestApp>(&snap_path)
                    .map(|(a, _, _, _)| a.total)
                    .unwrap_or(u64::MAX);
                panic!("snapshot did not reach total=150 (observed={observed})");
            }
            std::thread::sleep(Duration::from_millis(5));
        }

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }
}
