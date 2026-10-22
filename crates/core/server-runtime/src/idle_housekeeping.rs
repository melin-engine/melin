//! The response stages' idle housekeeping cadence, shared by the kernel
//! TCP stage (`response::run`) and the DPDK one (`dpdk_response::run`).
//!
//! Everything a response stage does while its output ring is empty, but
//! for flushing and early replies, is periodic work on sub-second to
//! second cadences: the heartbeat scan, the ack policy's idle re-check
//! (`DurabilityGate::idle_recheck`, rate-limited inside the gate) and the
//! latency recorders' flush. Each needs the current time only to discover
//! that it has nothing to do, so one amortized timer decides when the idle
//! path reads the clock at all, and each task keeps its own coarser
//! interval on top of that one read.

use std::time::{Duration, Instant};

use melin_app::amortized_timer::AmortizedTimer;
use melin_pipeline::wait::Waiter;

/// Cadence at which the idle path does its housekeeping *clock read*. The
/// heartbeat scan, the policy re-check and the trace-stats flush each keep
/// their own (coarser) interval; this only bounds how often the loop asks
/// the clock what time it is, so it must stay at or below the finest of
/// them (`trace::IDLE_FLUSH_INTERVAL`).
pub(crate) const IDLE_HOUSEKEEPING_INTERVAL: Duration = Duration::from_millis(10);

// Checked, not trusted: the read comes at least as often as the finest
// task it paces.
#[cfg(feature = "latency-trace")]
const _: () = assert!(
    IDLE_HOUSEKEEPING_INTERVAL.as_nanos()
        <= melin_transport_core::trace::IDLE_FLUSH_INTERVAL.as_nanos()
);

/// How often the idle path scans the connections for a due heartbeat. A
/// heartbeat interval is seconds, so a connection's heartbeat is late by at
/// most this much (plus one housekeeping interval).
const HEARTBEAT_SCAN_INTERVAL: Duration = Duration::from_secs(1);

/// The idle path's housekeeping timer and the last run of each task it
/// paces. One per response stage.
pub(crate) struct IdleHousekeeping {
    timer: AmortizedTimer,
    last_heartbeat_scan: Instant,
    #[cfg(feature = "latency-trace")]
    last_stats_flush: Instant,
}

/// What one housekeeping tick found due, at the tick's one clock read.
pub(crate) struct Due {
    /// The tick's clock read, for every task it runs.
    pub(crate) now: Instant,
    /// Scan the connections for a due heartbeat.
    pub(crate) heartbeat_scan: bool,
    /// Hand the stage's buffered latency samples to the stats registry.
    /// Without it, every sample the thread recorded stays in its
    /// thread-local buffer once traffic stops, which is exactly when the
    /// bench scrapes `/stats-dump`.
    #[cfg(feature = "latency-trace")]
    pub(crate) stats_flush: bool,
}

impl IdleHousekeeping {
    pub(crate) fn new() -> Self {
        let now = Instant::now();
        Self {
            timer: AmortizedTimer::new(),
            last_heartbeat_scan: now,
            #[cfg(feature = "latency-trace")]
            last_stats_flush: now,
        }
    }

    /// Call on every idle iteration, before `waiter.idle()`. Returns what
    /// is due when the housekeeping interval has passed, and `None`
    /// otherwise (with no clock read while the timer is masked; unmasked,
    /// every call reads the clock).
    ///
    /// The timer's iteration mask is engaged only once the stage has been
    /// busy-spinning idle for a sustained stretch (the waiter resets on
    /// every consumed batch). That is deliberate: the mask trades cadence
    /// for clock reads, and it should only do so when clock reads are the
    /// loop's whole cost. A stage that is nearly saturated reaches the idle
    /// path a handful of times between batches: there the read is already
    /// rare, and masking it would stretch the one-second heartbeat scan
    /// into minutes. A yielding thread never masks either: the yield
    /// syscall dwarfs a vDSO read.
    ///
    /// "Busy-spinning, and has been for a while": under spin-then-yield a
    /// loop past its budget is no longer spinning, so the conjunction is
    /// only ever true under `BusySpin`.
    #[inline]
    pub(crate) fn tick(&mut self, waiter: &Waiter) -> Option<Due> {
        let masked = waiter.spinning() && waiter.past_spin_budget();
        self.timer.tick(IDLE_HOUSEKEEPING_INTERVAL, masked)?;
        Some(self.due_at(Instant::now()))
    }

    /// What is due at `now`, a housekeeping tick's clock read; each task
    /// found due counts as run at `now`.
    fn due_at(&mut self, now: Instant) -> Due {
        Due {
            now,
            heartbeat_scan: elapsed(&mut self.last_heartbeat_scan, now, HEARTBEAT_SCAN_INTERVAL),
            #[cfg(feature = "latency-trace")]
            stats_flush: elapsed(
                &mut self.last_stats_flush,
                now,
                melin_transport_core::trace::IDLE_FLUSH_INTERVAL,
            ),
        }
    }
}

/// Whether `interval` has passed since `*last`, moving `*last` to `now`
/// when it has.
fn elapsed(last: &mut Instant, now: Instant, interval: Duration) -> bool {
    let due = now.saturating_duration_since(*last) >= interval;
    if due {
        *last = now;
    }
    due
}

#[cfg(test)]
mod tests {
    use super::*;
    use melin_pipeline::wait::WaitStrategy;

    /// A stage whose last scan is a full scan interval old: as on a
    /// nearly saturated stage, which reaches its idle path only briefly
    /// between batches.
    fn overdue() -> IdleHousekeeping {
        let mut housekeeping = IdleHousekeeping::new();
        housekeeping.last_heartbeat_scan =
            Instant::now().checked_sub(HEARTBEAT_SCAN_INTERVAL).unwrap();
        // The timer's own interval passes, so its next unmasked tick reads
        // the clock and fires.
        std::thread::sleep(IDLE_HOUSEKEEPING_INTERVAL);
        housekeeping
    }

    /// The heartbeat scan is due from the first housekeeping tick a scan
    /// interval after the last one, while the waiter is still spinning
    /// within its budget: the timer is masked only past the budget. With
    /// the scan's own timer masked whenever the waiter spun, a stage that
    /// spins only briefly between batches went many seconds unscanned.
    #[test]
    fn a_brief_spin_finds_the_heartbeat_scan_due_on_the_first_tick() {
        for strategy in [WaitStrategy::BusySpin, WaitStrategy::SpinThenYield] {
            let waiter = strategy.waiter();
            assert!(waiter.spinning() && !waiter.past_spin_budget());
            let mut housekeeping = overdue();
            let due = housekeeping
                .tick(&waiter)
                .expect("unmasked: the first tick fires");
            assert!(due.heartbeat_scan, "{strategy:?}");
            assert_eq!(housekeeping.last_heartbeat_scan, due.now);
        }
    }

    /// A thread that has yielded past its budget never masks: the yield
    /// already paid more than a clock read.
    #[test]
    fn a_yielding_waiter_finds_the_heartbeat_scan_due_on_the_first_tick() {
        let mut waiter = WaitStrategy::SpinThenYield.waiter();
        while waiter.spinning() {
            waiter.idle();
        }
        let mut housekeeping = overdue();
        let due = housekeeping
            .tick(&waiter)
            .expect("unmasked: the first tick fires");
        assert!(due.heartbeat_scan);
    }

    /// Busy-spinning past its budget, the stage masks the clock read: the
    /// first tick does not read it, so nothing is due yet.
    #[test]
    fn a_sustained_busy_spin_masks_the_clock_read() {
        let mut waiter = WaitStrategy::BusySpin.waiter();
        while !waiter.past_spin_budget() {
            waiter.idle();
        }
        let mut housekeeping = overdue();
        let scanned = housekeeping.last_heartbeat_scan;
        assert!(housekeeping.tick(&waiter).is_none());
        assert_eq!(housekeeping.last_heartbeat_scan, scanned, "no scan counted");
    }

    /// The scan runs once per scan interval, measured from the tick that
    /// last ran it, whatever the housekeeping ticks in between.
    #[test]
    fn the_heartbeat_scan_is_due_once_per_scan_interval() {
        let mut housekeeping = IdleHousekeeping::new();
        let start = housekeeping.last_heartbeat_scan;
        let just_short = start + HEARTBEAT_SCAN_INTERVAL - Duration::from_nanos(1);
        assert!(!housekeeping.due_at(just_short).heartbeat_scan);
        let at = start + HEARTBEAT_SCAN_INTERVAL;
        assert!(housekeeping.due_at(at).heartbeat_scan);
        assert!(
            !housekeeping
                .due_at(at + IDLE_HOUSEKEEPING_INTERVAL)
                .heartbeat_scan
        );
        assert!(
            housekeeping
                .due_at(at + HEARTBEAT_SCAN_INTERVAL)
                .heartbeat_scan
        );
    }

    /// The recorders' flush keeps its own interval, from the same reads.
    #[cfg(feature = "latency-trace")]
    #[test]
    fn the_stats_flush_is_due_once_per_flush_interval() {
        use melin_transport_core::trace::IDLE_FLUSH_INTERVAL;
        let mut housekeeping = IdleHousekeeping::new();
        let start = housekeeping.last_stats_flush;
        assert!(
            !housekeeping
                .due_at(start + IDLE_HOUSEKEEPING_INTERVAL)
                .stats_flush
        );
        assert!(housekeeping.due_at(start + IDLE_FLUSH_INTERVAL).stats_flush);
        assert!(
            !housekeeping
                .due_at(start + IDLE_FLUSH_INTERVAL + IDLE_HOUSEKEEPING_INTERVAL)
                .stats_flush
        );
    }
}
