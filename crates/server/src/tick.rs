//! Tick generator thread: publishes `JournalEvent::Tick { now_ns }` onto
//! the engine input ring at a fixed cadence.
//!
//! Time enters the engine through this thread. Every other event (orders,
//! cancels, admin commands) carries a `timestamp_ns` set at publish, but the
//! matching stage's scheduler needs *intermediate* clock progress so that
//! time-driven tasks (GTD expiry, volatility halts, session transitions)
//! fire even during quiet periods. Ticks are journaled like any input event,
//! so replay reproduces the same firing order deterministically.
//!
//! ## Monotonic clamp
//!
//! `SystemTime::now()` can step backwards (NTP, manual clock skew). Since
//! the scheduler keys tasks by `fire_ns` and pops with `fire_ns <= now_ns`,
//! a backwards jump would be silently absorbed but a future jump followed by
//! a backward jump could re-fire tasks that were already drained at replay
//! time. To keep replay fully deterministic, the tick thread emits a strictly
//! monotonic stream: each tick's `now_ns` is `max(prev + 1, raw_now_ns)`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use tracing::{debug, info, warn};

use melin_disruptor::ring;
use melin_engine::journal::event::JournalEvent;
use melin_engine::journal::pipeline::{InputSlot, Sequencer};
use melin_engine::journal::trace::trace_ts;
use melin_engine::journal::writer::wall_clock_nanos;

/// How often to wake up while waiting for the next tick deadline. Bounds
/// shutdown latency to this interval — short enough that ctrl-C feels
/// responsive, long enough not to spin.
const SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Run the tick generator loop. Returns when `shutdown` is set.
///
/// `cadence` is the wall-clock interval between ticks. The thread sleeps
/// in chunks of at most [`SHUTDOWN_POLL_INTERVAL`] so a shutdown signal
/// is observed promptly even when `cadence` is large.
///
/// If the input ring is full at publish time, the tick is *dropped* — the
/// next successful tick still carries the latest wall-clock time, so a
/// missed tick only delays scheduler firings by one cadence at worst.
///
/// Caveat: the dropped tick still consumed a journal sequence number from
/// the [`Sequencer`] (this matches the existing pattern in `crates/server/
/// src/reader.rs`). On a clean shutdown the gap is harmless, but a *crash*
/// while the input ring was saturated could leave the journal with a
/// missing sequence — recovery would error with `SequenceGap`. The input
/// ring is sized at 1M slots, so saturation is reachable only when the
/// matching stage is fully stalled, in which case the sequence-gap risk
/// is the smaller problem. Sequence reclamation on publish-fail is a
/// project-wide architectural concern and not addressed here.
pub fn run(
    producer: ring::MultiProducer<InputSlot>,
    sequencer: Arc<Sequencer>,
    cadence: Duration,
    shutdown: &AtomicBool,
) {
    info!(
        cadence_ms = cadence.as_millis() as u64,
        "tick generator starting"
    );

    // Local high-water mark for the strict-monotonic clamp.
    let mut last_now_ns: u64 = 0;
    // Anchor the cadence schedule on a monotonic clock so it never drifts
    // with wall-clock NTP corrections. The first tick fires after one
    // cadence — there's no value in a tick at startup since the heap is
    // empty until features schedule into it.
    let start = Instant::now();
    let mut next_deadline = start + cadence;
    let mut ticks_published: u64 = 0;
    let mut ticks_dropped: u64 = 0;

    loop {
        if shutdown.load(Ordering::Relaxed) {
            info!(
                ticks_published,
                ticks_dropped, "tick generator shutting down"
            );
            return;
        }

        let now = Instant::now();
        if now < next_deadline {
            let remaining = next_deadline - now;
            std::thread::sleep(remaining.min(SHUTDOWN_POLL_INTERVAL));
            continue;
        }

        // Deadline reached. Emit a tick with monotonic-clamped wall-clock
        // time, then advance the deadline. If we fell more than one cadence
        // behind (e.g. paused process), skip ahead so we don't burst-emit
        // stale ticks.
        let raw_now_ns = wall_clock_nanos();
        let now_ns = clamp_monotonic(raw_now_ns, last_now_ns);
        last_now_ns = now_ns;

        if publish_tick(&producer, &sequencer, now_ns) {
            ticks_published += 1;
        } else {
            ticks_dropped += 1;
            debug!("tick dropped — input ring full");
        }

        // Re-anchor the deadline to *now* if we're more than one cadence
        // behind. Prevents tight-loop catch-up after long pauses.
        let elapsed_since_deadline = Instant::now().saturating_duration_since(next_deadline);
        if elapsed_since_deadline > cadence {
            warn!(
                lag_ms = elapsed_since_deadline.as_millis() as u64,
                "tick generator fell behind by more than one cadence; resetting schedule"
            );
            next_deadline = Instant::now() + cadence;
        } else {
            next_deadline += cadence;
        }
    }
}

/// Strict-monotonic clamp: tick `n+1` is at least `tick n + 1 ns`.
///
/// Pulled out for unit testing — the loop above is timing-driven and hard
/// to exercise directly.
fn clamp_monotonic(raw_now_ns: u64, last_now_ns: u64) -> u64 {
    if last_now_ns == 0 {
        // First tick: trust the wall clock unless it's exactly zero (which
        // `wall_clock_nanos` returns only on a pre-epoch system clock).
        raw_now_ns.max(1)
    } else if raw_now_ns > last_now_ns {
        raw_now_ns
    } else {
        last_now_ns + 1
    }
}

/// Publish one Tick event. Returns true on success, false if the ring is full.
fn publish_tick(
    producer: &ring::MultiProducer<InputSlot>,
    sequencer: &Sequencer,
    now_ns: u64,
) -> bool {
    let seq = sequencer.next();
    producer
        .try_publish(InputSlot {
            // Internal/server-originated: no client connection, no auth key.
            // key_hash=0 is exempt from idempotency dedup in the engine.
            connection_id: 0,
            key_hash: 0,
            request_seq: 0,
            sequence: seq,
            timestamp_ns: now_ns,
            event: JournalEvent::Tick { now_ns },
            publish_ts: trace_ts(),
            recv_ts: trace_ts(),
        })
        .is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_tick_uses_raw_when_nonzero() {
        assert_eq!(clamp_monotonic(1_000, 0), 1_000);
    }

    #[test]
    fn first_tick_clamps_zero_to_one() {
        // `wall_clock_nanos` returns 0 only on a pre-epoch clock — bump
        // to 1 so the journal's per-tick monotonic invariant holds even in
        // that pathological case.
        assert_eq!(clamp_monotonic(0, 0), 1);
    }

    #[test]
    fn forward_clock_passes_through() {
        assert_eq!(clamp_monotonic(2_000, 1_000), 2_000);
    }

    #[test]
    fn backward_clock_clamped_to_prev_plus_one() {
        assert_eq!(clamp_monotonic(500, 1_000), 1_001);
    }

    #[test]
    fn equal_clock_clamped_to_prev_plus_one() {
        assert_eq!(clamp_monotonic(1_000, 1_000), 1_001);
    }

    #[test]
    fn shutdown_observed_promptly() {
        use melin_disruptor::ring::DisruptorBuilder;

        let (producer, _consumers) = DisruptorBuilder::<InputSlot>::new(64)
            .add_consumer()
            .build_multi_producer();

        let sequencer = Arc::new(Sequencer::new(1));
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_setter = Arc::clone(&shutdown);

        let handle = std::thread::spawn({
            let producer = producer.clone();
            let sequencer = Arc::clone(&sequencer);
            let shutdown = Arc::clone(&shutdown);
            move || run(producer, sequencer, Duration::from_millis(10), &shutdown)
        });

        // Let it run a few cadences, then signal shutdown.
        std::thread::sleep(Duration::from_millis(60));
        shutdown_setter.store(true, Ordering::Relaxed);

        // Should observe shutdown within at most one SHUTDOWN_POLL_INTERVAL.
        let join_start = Instant::now();
        handle.join().expect("tick thread panicked");
        let join_elapsed = join_start.elapsed();
        assert!(
            join_elapsed < SHUTDOWN_POLL_INTERVAL + Duration::from_millis(50),
            "tick thread took {join_elapsed:?} to observe shutdown"
        );
    }

    #[test]
    fn ticks_emit_strictly_monotonic_now_ns() {
        use melin_disruptor::ring::DisruptorBuilder;

        let (producer, mut consumers) = DisruptorBuilder::<InputSlot>::new(64)
            .add_consumer()
            .build_multi_producer();
        let consumer = consumers.remove(0);
        // Drain in this thread; the run thread publishes.
        let mut consumer = consumer;

        let sequencer = Arc::new(Sequencer::new(1));
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_setter = Arc::clone(&shutdown);

        let handle = std::thread::spawn({
            let producer = producer.clone();
            let sequencer = Arc::clone(&sequencer);
            let shutdown = Arc::clone(&shutdown);
            move || run(producer, sequencer, Duration::from_millis(5), &shutdown)
        });

        // Collect a handful of ticks from the ring.
        let mut now_ns_seen = Vec::new();
        let deadline = Instant::now() + Duration::from_millis(500);
        let batch_cap = 16;
        let mut batch = vec![InputSlot::default(); batch_cap];
        while now_ns_seen.len() < 5 && Instant::now() < deadline {
            let n = consumer.consume_batch(&mut batch, batch_cap);
            for slot in &batch[..n] {
                if let JournalEvent::Tick { now_ns } = slot.event {
                    now_ns_seen.push(now_ns);
                }
            }
            if n == 0 {
                std::thread::sleep(Duration::from_millis(2));
            }
        }

        shutdown_setter.store(true, Ordering::Relaxed);
        handle.join().expect("tick thread panicked");

        assert!(
            now_ns_seen.len() >= 3,
            "expected at least 3 ticks, got {}",
            now_ns_seen.len()
        );
        for pair in now_ns_seen.windows(2) {
            assert!(
                pair[1] > pair[0],
                "tick now_ns must be strictly increasing: {pair:?}"
            );
        }
    }
}
