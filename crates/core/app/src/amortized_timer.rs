//! Periodic-event timer safe to call from a busy-spin hot loop.
//!
//! Calling `Instant::now()` on every iteration of a thread that busy-
//! spins at ~10 M iterations/s costs ~15–25 % of a CPU core on Linux
//! x86_64 — the vDSO `clock_gettime(CLOCK_MONOTONIC)` is fast (~15–25 ns)
//! but not free, and the hot path has nothing else in it to amortize
//! that cost against. Multiple stages in the transport busy-spin; a naive
//! `last.elapsed() >= period` check in any of them is a measurable
//! throughput tax (seen on the replica receiver, the primary per-slot
//! replication handler, and the shadow snapshot stage — in the latter,
//! ~10 % of total process cycles landed on `__vdso_clock_gettime`
//! before this timer was adopted).
//!
//! [`AmortizedTimer::tick`] reads the clock only once every
//! [`Self::CHECK_MASK`] + 1 iterations (~1 M) **while spinning**. When
//! the caller has fallen back to `yield_now()` the loop rate drops to
//! scheduler timeslice frequency (typically hundreds of iterations per
//! second), so the mask would delay a 5 s heartbeat by 2^20 × yield_latency
//! ≈ 17 minutes. Passing `spinning = false` bypasses the mask and reads
//! the clock every iteration; the yield syscall already paid orders of
//! magnitude more than a vDSO clock read.
//!
//! Not a general-purpose rate limiter: it is tuned for
//! once-per-second-ish checks called from loops that execute millions
//! of times per second. Use a real clock-based timer for anything
//! needing tight timing or for infrequent loops.

pub struct AmortizedTimer {
    last: std::time::Instant,
    // Iteration counter used only when `spinning = true` in `tick`.
    iter: u64,
}

impl Default for AmortizedTimer {
    fn default() -> Self {
        Self::new()
    }
}

impl AmortizedTimer {
    /// Power-of-two mask so `iter & CHECK_MASK` lowers to `AND`.
    /// At ~10 M loop iters/s this yields ~10 clock reads per second.
    const CHECK_MASK: u64 = (1 << 20) - 1;

    pub fn new() -> Self {
        Self {
            last: std::time::Instant::now(),
            iter: 0,
        }
    }

    /// Call every loop iteration. Returns `Some(elapsed)` when `period`
    /// has passed since the last successful tick (and the internal
    /// timestamp is advanced); otherwise `None`.
    ///
    /// `spinning` must be `true` while the caller's loop is in its busy-spin
    /// phase (no syscall per iteration). When `false` — i.e. the loop has
    /// fallen back to `yield_now()` — the clock is read every call because
    /// the yield syscall already costs far more than a vDSO read.
    ///
    /// The returned `elapsed` is the real time since the previous tick,
    /// suitable for computing per-interval rates without a second clock read.
    #[inline]
    pub fn tick(
        &mut self,
        period: std::time::Duration,
        spinning: bool,
    ) -> Option<std::time::Duration> {
        if spinning {
            self.iter = self.iter.wrapping_add(1);
            if self.iter & Self::CHECK_MASK != 0 {
                return None;
            }
        }
        let elapsed = self.last.elapsed();
        if elapsed < period {
            return None;
        }
        self.last = std::time::Instant::now();
        Some(elapsed)
    }
}
