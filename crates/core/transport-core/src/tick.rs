//! Tick-generation helpers shared by every ingress transport (io_uring
//! reader, DPDK poll thread, future replacements). Each transport
//! embeds the scheduler-clock tick generator into its existing ingress
//! loop instead of running it as a separate thread, which keeps the
//! input ring single-producer in steady state and removes one source
//! of multi-producer ordering races.
//!
//! ## Monotonic clamp
//!
//! `SystemTime::now()` can step backwards (NTP, manual clock skew). The
//! application's scheduler may key due-task firings on `now_ns >= fire_ns`,
//! so a backwards step followed by a forward step could re-fire tasks
//! that were already drained on replay. Clamping each tick's `now_ns` to
//! `max(prev + 1, raw_now_ns)` keeps the journaled stream strictly
//! monotonic, so live and replay produce byte-identical state.
//!
//! ## What this module is *not*
//!
//! There is no longer a standalone tick thread. The matching stage
//! also advances its scheduler clock from `slot.timestamp_ns` on every
//! event (see `dispatch::dispatch`), so the tick is the safety
//! net that keeps time moving forward during quiet periods rather than
//! the sole source of clock progress.

use std::time::{Duration, Instant};

use crate::pipeline::InputSlot;
use crate::trace::mono_trace_ns;
use melin_app::{AppEvent, unix_epoch_nanos};
use melin_journal::JournalEvent;
use melin_pipeline::ring;

/// Strict-monotonic clamp on the wall-clock timestamp emitted by each tick.
/// `last_now_ns == 0` is the initial-state sentinel — the first tick is
/// stamped with `raw_now_ns` (or `1` if even the wall clock returns 0,
/// which only happens on a pre-epoch system clock).
pub fn clamp_monotonic(raw_now_ns: u64, last_now_ns: u64) -> u64 {
    if last_now_ns == 0 {
        raw_now_ns.max(1)
    } else if raw_now_ns > last_now_ns {
        raw_now_ns
    } else {
        last_now_ns + 1
    }
}

/// Publish a `JournalEvent::Tick { now_ns }` onto the input ring.
///
/// Internal/server-originated: no client connection, no auth key.
/// `key_hash = 0`, the identity of events no client submitted.
///
/// `sequence: 0` because the journal stage is the authoritative sequence
/// allocator on the primary — see `InputSlot::sequence`.
///
/// On a full ring the publish drops; the next successful tick still
/// carries the latest wall-clock time, so a missed tick only delays
/// scheduler firings by one cadence at worst.
pub fn publish_tick<E: AppEvent>(producer: &mut ring::Producer<InputSlot<E>>, now_ns: u64) {
    // try_publish drop is intentional: on a full ring we'd rather skip a
    // tick than block the ingress thread (see fn doc).
    let _ = producer.try_publish(InputSlot {
        connection_id: 0,
        key_hash: 0,
        sequence: 0,
        timestamp_ns: now_ns,
        event: JournalEvent::Tick { now_ns },
        publish_ts: mono_trace_ns(),
        recv_ts: mono_trace_ns(),
    });
}

/// When the next tick is due, and the timestamp the last one carried.
///
/// The ingress loop owns one and decides how often to ask it — the
/// io_uring reader on every completion drain (with a kernel timeout armed
/// for [`Self::next_deadline`] so a quiet ring still wakes), the DPDK poll
/// thread once every few thousand polls. What a due tick publishes, and
/// how the schedule recovers from a stall, is the same for both and lives
/// here.
pub struct TickSchedule {
    cadence: Duration,
    next_deadline: Instant,
    /// Timestamp the last published tick carried, the floor for the next
    /// one's monotonic clamp. `0` until the first tick — the sentinel
    /// [`clamp_monotonic`] expects. Wall-clock nanoseconds, so `u64` like
    /// every `now_ns` in the journal.
    last_now_ns: u64,
}

impl TickSchedule {
    /// A schedule whose first tick falls one `cadence` after `now`.
    pub fn new(cadence: Duration, now: Instant) -> Self {
        Self {
            cadence,
            next_deadline: now + cadence,
            last_now_ns: 0,
        }
    }

    /// When the next tick is due.
    #[inline]
    pub fn next_deadline(&self) -> Instant {
        self.next_deadline
    }

    /// Publish a tick if one is due at `now`, stamped with the wall clock
    /// clamped monotonic against the previous tick. Returns whether a tick
    /// was published (or dropped on a full ring — see [`publish_tick`]),
    /// so the caller can re-arm any timer it keyed on the old deadline.
    #[inline]
    pub fn publish_if_due<E: AppEvent>(
        &mut self,
        now: Instant,
        producer: &mut ring::Producer<InputSlot<E>>,
    ) -> bool {
        if now < self.next_deadline {
            return false;
        }
        let now_ns = clamp_monotonic(unix_epoch_nanos(), self.last_now_ns);
        self.last_now_ns = now_ns;
        publish_tick(producer, now_ns);
        self.advance(Instant::now());
        true
    }

    /// Move the deadline past a tick published at `now`. Normally one
    /// cadence on from the old deadline, which keeps the long-run rate
    /// exact; but a loop that fell more than a cadence behind restarts
    /// from `now` instead — catching up would burst-emit a tick per
    /// missed cadence, each carrying nearly the same timestamp.
    fn advance(&mut self, now: Instant) {
        let late_by = now.saturating_duration_since(self.next_deadline);
        self.next_deadline = if late_by > self.cadence {
            now + self.cadence
        } else {
            self.next_deadline + self.cadence
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_deadline_is_one_cadence_out() {
        let t0 = Instant::now();
        let s = TickSchedule::new(Duration::from_millis(10), t0);
        assert_eq!(s.next_deadline(), t0 + Duration::from_millis(10));
    }

    #[test]
    fn an_on_time_tick_keeps_the_cadence_grid() {
        let cadence = Duration::from_millis(10);
        let t0 = Instant::now();
        let mut s = TickSchedule::new(cadence, t0);
        // Fired a little late: the next deadline stays on the grid
        // rather than drifting by the lateness.
        s.advance(t0 + cadence + Duration::from_millis(3));
        assert_eq!(s.next_deadline(), t0 + 2 * cadence);
    }

    #[test]
    fn a_tick_exactly_one_cadence_late_still_keeps_the_grid() {
        let cadence = Duration::from_millis(10);
        let t0 = Instant::now();
        let mut s = TickSchedule::new(cadence, t0);
        s.advance(t0 + 2 * cadence);
        assert_eq!(s.next_deadline(), t0 + 2 * cadence);
    }

    #[test]
    fn a_stalled_loop_restarts_the_grid_instead_of_bursting() {
        let cadence = Duration::from_millis(10);
        let t0 = Instant::now();
        let mut s = TickSchedule::new(cadence, t0);
        let late = t0 + 5 * cadence;
        s.advance(late);
        assert_eq!(s.next_deadline(), late + cadence);
    }

    #[test]
    fn publish_if_due_publishes_a_clamped_tick_only_once_due() {
        use crate::test_support::TestEvent;
        use melin_pipeline::ring::DisruptorBuilder;
        use melin_pipeline::wait::WaitStrategy;

        let (mut producer, mut consumers) = DisruptorBuilder::<InputSlot<TestEvent>>::new(8)
            .add_consumer()
            .build(WaitStrategy::SpinThenYield);
        let mut consumer = consumers.pop().expect("one consumer");
        let tick_of = |slot: InputSlot<TestEvent>| match slot.event {
            JournalEvent::Tick { now_ns } => {
                assert_eq!(slot.timestamp_ns, now_ns);
                assert_eq!(
                    (slot.connection_id, slot.key_hash, slot.sequence),
                    (0, 0, 0)
                );
                now_ns
            }
            other => panic!("expected a tick, got {other:?}"),
        };

        // A long cadence so the re-arm below is never "late by more than
        // a cadence", whatever the test machine's scheduling.
        let cadence = Duration::from_secs(3600);
        let t0 = Instant::now();
        let mut s = TickSchedule::new(cadence, t0);

        // Before the deadline: nothing published, deadline untouched.
        assert!(!s.publish_if_due(t0 + cadence - Duration::from_nanos(1), &mut producer));
        assert_eq!(s.next_deadline(), t0 + cadence);
        assert!(consumer.try_consume().is_none());

        // At the deadline: the first tick carries the wall clock.
        let wall_before = unix_epoch_nanos();
        assert!(s.publish_if_due(t0 + cadence, &mut producer));
        let (_, slot) = consumer.try_consume().expect("a tick on the ring");
        let first = tick_of(slot);
        assert!(first >= wall_before.max(1));
        assert_eq!(s.last_now_ns, first);
        assert_eq!(s.next_deadline(), t0 + 2 * cadence, "re-armed on the grid");

        // A previous tick stamped ahead of the wall clock (a backwards
        // step since): the next is clamped to one past it.
        let ahead = unix_epoch_nanos() + 3_600_000_000_000;
        s.last_now_ns = ahead;
        assert!(s.publish_if_due(t0 + 2 * cadence, &mut producer));
        let (_, slot) = consumer.try_consume().expect("a second tick");
        assert_eq!(tick_of(slot), ahead + 1);
        assert_eq!(s.last_now_ns, ahead + 1);
        assert!(consumer.try_consume().is_none(), "one tick per due call");
    }

    #[test]
    fn first_tick_uses_raw_when_nonzero() {
        assert_eq!(clamp_monotonic(1_000, 0), 1_000);
    }

    #[test]
    fn first_tick_clamps_zero_to_one() {
        // `unix_epoch_nanos` returns 0 only on a pre-epoch clock — bump
        // to 1 so the journal's per-tick monotonic invariant holds even
        // in that pathological case.
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
}
