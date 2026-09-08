//! How a thread waits for the other side of a ring.
//!
//! Every blocking wait in the pipeline — a consumer polling an empty
//! ring, a producer blocked on a full one, a stage waiting for a cursor
//! another thread publishes — goes through a [`WaitStrategy`]. There is
//! deliberately no other way to wait: a `spin_loop` that never yields is
//! correct only when the thread owns its core, and a thread that shares
//! a core with the one it is waiting on burns a whole scheduler slice
//! per hop. Routing every wait through one type means the choice is
//! made once, at startup, from what the deployment actually looks like,
//! rather than site by site.
//!
//! Two strategies exist today:
//!
//! - [`BusySpin`](WaitStrategy::BusySpin): spin with `PAUSE`, never
//!   yield. The production default on isolated cores (`isolcpus`),
//!   where the thread is the only thing that will ever run there and a
//!   yield would be a wasted syscall on the latency path.
//! - [`SpinThenYield`](WaitStrategy::SpinThenYield): spin for a short
//!   budget, then `sched_yield` on every further idle iteration. For
//!   shared cores — tests, CI, development boxes — where the thread
//!   being waited on may be queued behind the waiter on the same CPU.
//!
//! The per-loop state lives in a [`Waiter`]: it counts consecutive idle
//! iterations so `SpinThenYield` knows when the budget is spent, and it
//! exposes whether the loop is still in its spin phase, which amortized
//! timers use to decide whether a clock read is worth masking.

/// Idle iterations to spin before `SpinThenYield` starts yielding —
/// about a microsecond, long enough to catch a reply that is already in
/// flight without paying a syscall for it.
// u32 — the counter saturates here, so it never needs more range.
const SPIN_BUDGET: u32 = 1000;

/// How a thread waits when the ring it polls has nothing for it, or,
/// on the producer side, when the ring has no room.
///
/// `Copy` so it can be handed to every stage and stored in every
/// producer without ceremony: it is two variants and no state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WaitStrategy {
    /// Spin with `PAUSE` and never yield. The thread owns its core.
    BusySpin,
    /// Spin for a short budget, then `sched_yield` on every further
    /// idle iteration. The thread may share its core with the thread
    /// it is waiting on.
    SpinThenYield,
}

impl WaitStrategy {
    /// Per-loop idle state for this strategy. One per polling loop;
    /// call [`Waiter::idle`] on every iteration that found nothing and
    /// [`Waiter::reset`] on every iteration that found work.
    #[inline]
    pub fn waiter(self) -> Waiter {
        Waiter {
            strategy: self,
            idle: 0,
        }
    }

    /// Block until `ready` returns true, waiting between polls
    /// according to this strategy.
    ///
    /// `ready` is polled at least once. A caller that also needs to give
    /// up — on shutdown, on a dead peer — folds that condition into
    /// `ready` and checks which one fired afterwards.
    #[inline]
    pub fn wait_until(self, mut ready: impl FnMut() -> bool) {
        let mut waiter = self.waiter();
        while !ready() {
            waiter.idle();
        }
    }
}

/// Idle state of one polling loop under a [`WaitStrategy`].
///
/// Cheap enough for the hot path: under `BusySpin` an idle iteration is
/// a saturating increment, a predictable branch, and a `PAUSE`.
pub struct Waiter {
    strategy: WaitStrategy,
    /// Consecutive idle iterations, saturating at [`SPIN_BUDGET`].
    /// Counted under every strategy — not only the yielding one — so
    /// [`Self::past_spin_budget`] means the same thing everywhere.
    // u32 — see `SPIN_BUDGET`.
    idle: u32,
}

impl Waiter {
    /// Wait out one idle iteration.
    #[inline(always)]
    pub fn idle(&mut self) {
        // Decide from the count *before* this iteration, so that
        // `spinning()` — which reads the same count — is exactly the
        // answer to "will the next call spin?".
        let spin = self.spinning();
        if self.idle < SPIN_BUDGET {
            self.idle += 1;
        }
        if spin {
            std::hint::spin_loop();
        } else {
            std::thread::yield_now();
        }
    }

    /// The loop found work: the next idle stretch starts from a fresh
    /// budget.
    #[inline(always)]
    pub fn reset(&mut self) {
        self.idle = 0;
    }

    /// Whether the next [`idle`](Self::idle) call will spin rather than
    /// yield — always true under `BusySpin`.
    ///
    /// This is what an amortized timer wants to know: while spinning,
    /// the loop runs millions of iterations a second and a clock read
    /// per iteration is a measurable tax, so the timer masks it; once
    /// the loop yields, each iteration already pays a syscall and the
    /// mask would only delay the timer.
    #[inline(always)]
    pub fn spinning(&self) -> bool {
        self.strategy == WaitStrategy::BusySpin || self.idle < SPIN_BUDGET
    }

    /// Whether the loop has been idle for at least the spin budget —
    /// "sustained idle", under every strategy. Distinct from
    /// [`spinning`](Self::spinning): a `BusySpin` loop is both spinning
    /// and, once quiet long enough, past its budget, whereas under
    /// `SpinThenYield` the two are never true together — so
    /// `spinning() && past_spin_budget()` reads as "busy-spinning, and
    /// has been for a while".
    #[inline(always)]
    pub fn past_spin_budget(&self) -> bool {
        self.idle >= SPIN_BUDGET
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

    #[test]
    fn spin_then_yield_spins_for_the_budget_then_yields() {
        let mut w = WaitStrategy::SpinThenYield.waiter();
        assert!(w.spinning());
        assert!(!w.past_spin_budget());
        for _ in 0..SPIN_BUDGET - 1 {
            w.idle();
        }
        assert!(
            w.spinning(),
            "one spin left in the budget: the next idle must still spin"
        );
        w.idle();
        assert!(!w.spinning(), "budget spent: the next idle must yield");
        assert!(w.past_spin_budget());
        // Further idle iterations keep yielding and must not wrap the
        // counter back into the spin phase.
        for _ in 0..(SPIN_BUDGET * 4) {
            w.idle();
        }
        assert!(!w.spinning());
    }

    #[test]
    fn reset_restores_the_spin_phase() {
        let mut w = WaitStrategy::SpinThenYield.waiter();
        for _ in 0..(SPIN_BUDGET * 2) {
            w.idle();
        }
        assert!(!w.spinning());
        w.reset();
        assert!(w.spinning());
        assert!(!w.past_spin_budget());
    }

    #[test]
    fn busy_spin_always_spins_but_still_reports_sustained_idle() {
        let mut w = WaitStrategy::BusySpin.waiter();
        for _ in 0..(SPIN_BUDGET * 2) {
            w.idle();
        }
        assert!(w.spinning(), "BusySpin never yields");
        assert!(
            w.past_spin_budget(),
            "the sustained-idle signal is independent of the strategy"
        );
    }

    #[test]
    fn wait_until_polls_at_least_once_and_returns_when_ready() {
        for strategy in [WaitStrategy::BusySpin, WaitStrategy::SpinThenYield] {
            let polls = AtomicU32::new(0);
            strategy.wait_until(|| {
                polls.fetch_add(1, Ordering::Relaxed);
                true
            });
            assert_eq!(polls.load(Ordering::Relaxed), 1);
        }
    }

    /// Liveness across threads: a waiter blocked on a flag another
    /// thread sets well after the spin budget is spent comes back once
    /// it is set. (Whether the yield actually hands the CPU over is a
    /// scheduler property no unit test can pin down — under
    /// `SCHED_OTHER` the kernel timeslices a pure spinner too. What this
    /// checks is that the yield phase keeps polling.)
    #[test]
    fn spin_then_yield_keeps_polling_past_the_budget() {
        let flag = Arc::new(AtomicBool::new(false));
        let setter = {
            let flag = Arc::clone(&flag);
            std::thread::spawn(move || {
                // Well past the waiter's spin budget so the test does
                // not pass by the flag being set before the wait begins.
                std::thread::sleep(std::time::Duration::from_millis(20));
                flag.store(true, Ordering::Release);
            })
        };
        WaitStrategy::SpinThenYield.wait_until(|| flag.load(Ordering::Acquire));
        setter.join().expect("setter thread");
    }
}
