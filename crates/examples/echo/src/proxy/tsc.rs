//! Cycle-counter timing for the send loop.
//!
//! `Instant::now()` costs a vDSO call per read; the counter costs a few
//! cycles and, once calibrated, converts to nanoseconds with one multiply.
//! Everything on the hot path is a raw tick: the schedule is kept in
//! ticks, the tick is what a request carries across the wire, and the
//! conversion happens once per sample as it is recorded.
//!
//! Mirrors the sequencer's bench harness clock, staged in the exchange
//! repository as `melin-bench-harness::clock`. When that crate moves here
//! this module is what it replaces.

use std::time::{Duration, Instant};

/// Read the counter. Serialising (`rdtscp`), so the read cannot be hoisted
/// above the work it is meant to follow.
#[cfg(target_arch = "x86_64")]
#[inline(always)]
pub fn rdtscp() -> u64 {
    let mut aux = 0u32;
    // SAFETY: `rdtscp` reads a counter and an auxiliary register; it
    // touches no memory and has no preconditions on x86_64.
    unsafe { core::arch::x86_64::__rdtscp(&mut aux) }
}

/// Read the virtual counter after an instruction barrier, the arm64
/// equivalent of a serialising `rdtscp`.
#[cfg(target_arch = "aarch64")]
#[inline(always)]
pub fn rdtscp() -> u64 {
    let count: u64;
    // SAFETY: reads a system register after a barrier; no memory is
    // touched and the stack is untouched.
    unsafe {
        core::arch::asm!(
            "isb",
            "mrs {}, cntvct_el0",
            out(reg) count,
            options(nostack, nomem),
        );
    }
    count
}

/// How long the counter is measured against the wall clock. The factor's
/// error is bounded by the scheduler's jitter on that sleep divided by its
/// length, and the factor is what turns the target rate into a period --
/// so a longer window is a rate closer to the one the output file names.
const CALIBRATION: Duration = Duration::from_millis(200);

/// The counter, calibrated: ticks per nanosecond, and a `(tick, UNIX
/// nanos)` pair taken at calibration so any later tick converts to wall
/// time without a clock call.
///
/// Two sources of error worth knowing. The anchor is captured wall clock
/// first and tick second, so derived wall times undershoot by one clock
/// call -- constant, and in the direction that makes "is the deadline
/// past?" fire early rather than late. And the factor drifts linearly by
/// its own measurement error, on the order of a hundred parts per
/// million: a latency is the difference of two ticks, so that error is a
/// hundred parts per million *of the latency*, well below anything a run
/// resolves.
#[derive(Clone, Copy)]
pub struct TscClock {
    ticks_per_ns: f64,
    /// Precomputed so the hot path multiplies rather than divides.
    ns_per_tick: f64,
    anchor_tsc: u64,
    anchor_unix_ns: u64,
}

impl TscClock {
    /// Sleep `CALIBRATION` and measure the counter against a monotonic
    /// clock. Call once, on the core the loop will run on.
    pub fn calibrate() -> Self {
        for _ in 0..1_000 {
            std::hint::black_box(rdtscp());
        }
        // Wall clock first, then the tick: see the struct docs.
        let anchor_unix_ns = melin_app::unix_epoch_nanos();
        let anchor_tsc = rdtscp();
        let wall = Instant::now();
        std::thread::sleep(CALIBRATION);
        let later_tsc = rdtscp();
        // f64 throughout: the ratio is fractional, and at ~3 ticks per
        // nanosecond a u64 tick count converts to f64 without loss for any
        // interval shorter than a century.
        let elapsed_ns = wall.elapsed().as_nanos() as f64;
        let ticks_per_ns = later_tsc.saturating_sub(anchor_tsc) as f64 / elapsed_ns;
        Self {
            ticks_per_ns,
            ns_per_tick: 1.0 / ticks_per_ns,
            anchor_tsc,
            anchor_unix_ns,
        }
    }

    /// Wall time, in nanoseconds since the UNIX epoch, at tick `ts`.
    /// Saturates at the anchor for a tick older than it.
    #[inline(always)]
    pub fn unix_ns(&self, ts: u64) -> u64 {
        self.anchor_unix_ns + (ts.saturating_sub(self.anchor_tsc) as f64 * self.ns_per_tick) as u64
    }

    /// Nanoseconds from tick `from` to tick `to`; zero if `to` is earlier.
    #[inline(always)]
    pub fn elapsed_ns(&self, from: u64, to: u64) -> u64 {
        (to.saturating_sub(from) as f64 * self.ns_per_tick) as u64
    }

    /// Ticks in `ns` nanoseconds, rounded to the nearest tick.
    #[inline(always)]
    pub fn ticks(&self, ns: u64) -> u64 {
        (ns as f64 * self.ticks_per_ns).round() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn elapsed_matches_the_wall_clock() {
        let clock = TscClock::calibrate();
        let from = rdtscp();
        let wall = Instant::now();
        std::thread::sleep(Duration::from_millis(20));
        let to = rdtscp();
        let expected = wall.elapsed().as_nanos() as u64;
        let measured = clock.elapsed_ns(from, to);
        // A loaded test machine can oversleep by milliseconds, so the
        // bound is loose; what it catches is a units or factor mistake,
        // which would be off by orders of magnitude.
        assert!(
            measured.abs_diff(expected) < 5_000_000,
            "measured {measured} ns vs wall {expected} ns"
        );
    }

    #[test]
    fn unix_ns_tracks_the_wall_clock_and_saturates_before_the_anchor() {
        let clock = TscClock::calibrate();
        let derived = clock.unix_ns(rdtscp());
        let wall = melin_app::unix_epoch_nanos();
        assert!(derived.abs_diff(wall) < 1_000_000, "{derived} vs {wall}");
        assert_eq!(
            clock.unix_ns(clock.anchor_tsc.saturating_sub(1_000)),
            clock.anchor_unix_ns
        );
    }

    #[test]
    fn ticks_and_nanoseconds_round_trip() {
        let clock = TscClock::calibrate();
        let ticks = clock.ticks(1_000_000);
        let back = clock.elapsed_ns(0, ticks);
        // One tick of rounding either way.
        assert!(back.abs_diff(1_000_000) <= 2, "{back}");
    }
}
