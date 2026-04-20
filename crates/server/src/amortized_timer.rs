//! Periodic-event timer safe to call from a busy-spin hot loop.
//!
//! Calling `Instant::now()` on every iteration of a thread that busy-
//! spins at ~10 M iterations/s costs ~15–25 % of a CPU core on Linux
//! x86_64 — the vDSO `clock_gettime(CLOCK_MONOTONIC)` is fast (~15–25 ns)
//! but not free, and the hot path has nothing else in it to amortize
//! that cost against. Multiple stages in this crate busy-spin; a naive
//! `last.elapsed() >= period` check in any of them is a measurable
//! throughput tax (seen on the replica receiver, the primary per-slot
//! replication handler, and the shadow snapshot stage — in the latter,
//! ~10 % of total process cycles landed on `__vdso_clock_gettime`
//! before this timer was adopted).
//!
//! [`AmortizedTimer::tick`] reads the clock only once every
//! [`Self::CHECK_MASK`] + 1 iterations (~1 M), so the common path is a
//! single `AND` + predictable branch. Under sustained busy-spin the
//! timer still fires with well under 100 ms of jitter versus the
//! requested `period`, which is tight enough for ~1 Hz diagnostic
//! logging or snapshot-interval checks.
//!
//! Not a general-purpose rate limiter: it is tuned for
//! once-per-second-ish checks called from loops that execute millions
//! of times per second. Use a real clock-based timer for anything
//! needing tight timing or for infrequent loops.

pub(crate) struct AmortizedTimer {
    last: std::time::Instant,
    iter: u64,
}

impl AmortizedTimer {
    /// Power-of-two mask so `iter & CHECK_MASK` lowers to `AND`.
    /// At ~10 M loop iters/s this yields ~10 clock reads per second.
    const CHECK_MASK: u64 = (1 << 20) - 1;

    pub(crate) fn new() -> Self {
        Self {
            last: std::time::Instant::now(),
            iter: 0,
        }
    }

    /// Call every loop iteration. Returns `Some(elapsed)` when `period`
    /// has passed since the last successful tick (and the internal
    /// timestamp is advanced); otherwise `None`.
    ///
    /// The returned `elapsed` is the real time since the previous tick,
    /// suitable for computing per-interval rates without a second
    /// clock read.
    #[inline]
    pub(crate) fn tick(&mut self, period: std::time::Duration) -> Option<std::time::Duration> {
        self.iter = self.iter.wrapping_add(1);
        if self.iter & Self::CHECK_MASK != 0 {
            return None;
        }
        let elapsed = self.last.elapsed();
        if elapsed < period {
            return None;
        }
        self.last = std::time::Instant::now();
        Some(elapsed)
    }
}
