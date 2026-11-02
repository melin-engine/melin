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
//! Three strategies exist today:
//!
//! - [`BusySpin`](WaitStrategy::BusySpin): spin with `PAUSE`, never
//!   yield. The production default on isolated cores (`isolcpus`),
//!   where the thread is the only thing that will ever run there and a
//!   yield would be a wasted syscall on the latency path.
//! - [`SpinThenYield`](WaitStrategy::SpinThenYield): spin for a short
//!   budget, then `sched_yield` on every further idle iteration. For
//!   shared cores where the thread being waited on may be queued behind
//!   the waiter on the same CPU, and an idle thread may keep its core
//!   busy.
//! - [`SpinYieldPark`](WaitStrategy::SpinYieldPark): spin, then yield,
//!   each for a short budget, then sleep, backing off up to
//!   [`PARK_MAX`]. For threads with no core of their own — development
//!   boxes, CI, tests, containers under a CPU quota — where an idle
//!   thread that yields forever still shows as a busy core and burns the
//!   quota of the threads that have work.
//!
//! The per-loop state lives in a [`Waiter`]: it counts consecutive idle
//! iterations so the strategy knows which phase it is in, and it exposes
//! whether the loop is still in its spin phase, which amortized timers
//! use to decide whether a clock read is worth masking.

use std::time::Duration;

/// Idle iterations to spin before `SpinThenYield` and `SpinYieldPark`
/// start yielding — about a microsecond, long enough to catch a reply
/// that is already in flight without paying a syscall for it.
// u32 — the counter saturates at `IDLE_CEILING`, so it never needs more
// range.
const SPIN_BUDGET: u32 = 1000;

/// Idle iterations `SpinYieldPark` yields for, after the spin budget,
/// before it starts sleeping. On a core with nothing else to run a yield
/// returns at once, so this is tens of microseconds of a thread still
/// polling: enough to catch the next request of a burst without paying a
/// sleep's wakeup for it.
// u32 — see `SPIN_BUDGET`.
const YIELD_BUDGET: u32 = 100;

/// Where the idle counter saturates: the last phase boundary any
/// strategy reads.
// u32 — see `SPIN_BUDGET`.
const IDLE_CEILING: u32 = SPIN_BUDGET + YIELD_BUDGET;

/// `SpinYieldPark`'s first sleep. Linux adds its default timer slack
/// (50 µs) to every sleep of a thread under the normal scheduler, so a
/// shorter request would buy no earlier wakeup.
pub const PARK_MIN: Duration = Duration::from_micros(50);

/// `SpinYieldPark`'s longest sleep, which each sleep doubles towards:
/// what a request arriving at a long-idle thread can wait, plus the
/// timer slack, before that thread looks again. A request crosses a few
/// such threads on its way to a reply, so this bounds the first reply
/// after a quiet spell to around a millisecond, while an idle thread
/// wakes only a few thousand times a second.
pub const PARK_MAX: Duration = Duration::from_micros(200);

/// How a thread waits when the ring it polls has nothing for it, or,
/// on the producer side, when the ring has no room.
///
/// `Copy` so it can be handed to every stage and stored in every
/// producer without ceremony: it is three variants and no state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WaitStrategy {
    /// Spin with `PAUSE` and never yield. The thread owns its core.
    BusySpin,
    /// Spin for a short budget, then `sched_yield` on every further
    /// idle iteration. The thread may share its core with the thread
    /// it is waiting on.
    SpinThenYield,
    /// Spin for a short budget, yield for another, then sleep on every
    /// further idle iteration, from [`PARK_MIN`] doubling up to
    /// [`PARK_MAX`]. The thread may share its core like a yielding one,
    /// and costs next to no CPU while idle, at the price of answering up
    /// to [`PARK_MAX`] late after an idle spell. What a thread with no
    /// core of its own waits with unless told otherwise.
    SpinYieldPark,
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
            park: PARK_MIN,
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

/// What one idle iteration does: the phase a [`Waiter`] is in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Spin,
    Yield,
    Park,
}

/// Idle state of one polling loop under a [`WaitStrategy`].
///
/// Cheap enough for the hot path: under `BusySpin` an idle iteration is
/// a saturating increment, a predictable branch, and a `PAUSE`.
pub struct Waiter {
    strategy: WaitStrategy,
    /// Consecutive idle iterations, saturating at [`IDLE_CEILING`].
    /// Counted under every strategy — not only the ones that leave the
    /// spin phase — so [`Self::past_spin_budget`] means the same thing
    /// everywhere.
    // u32 — see `SPIN_BUDGET`.
    idle: u32,
    /// The next sleep under `SpinYieldPark`: [`PARK_MIN`] at the start of
    /// every idle stretch, doubled by each sleep up to [`PARK_MAX`].
    /// Unread under the other strategies. A `Duration` because that is
    /// what `thread::sleep` takes, and this is only touched on the sleep
    /// path, where its size costs nothing.
    park: Duration,
}

impl Waiter {
    /// Wait out one idle iteration.
    #[inline(always)]
    pub fn idle(&mut self) {
        // Decide from the count *before* this iteration, so that
        // `spinning()` — which reads the same phase — is exactly the
        // answer to "will the next call spin?".
        let phase = self.phase();
        if self.idle < IDLE_CEILING {
            self.idle += 1;
        }
        match phase {
            Phase::Spin => std::hint::spin_loop(),
            Phase::Yield => std::thread::yield_now(),
            Phase::Park => self.park(),
        }
    }

    /// Sleep for the current backoff and double it. Out of line so that
    /// the idle path every strategy inlines stays a spin or a yield.
    #[cold]
    #[inline(never)]
    fn park(&mut self) {
        std::thread::sleep(self.park);
        self.park = (self.park * 2).min(PARK_MAX);
    }

    /// The phase the next [`idle`](Self::idle) call runs in. `BusySpin`
    /// is tested first so that its loops pay one predictable branch.
    #[inline(always)]
    fn phase(&self) -> Phase {
        if self.strategy == WaitStrategy::BusySpin || self.idle < SPIN_BUDGET {
            Phase::Spin
        } else if self.strategy == WaitStrategy::SpinThenYield || self.idle < IDLE_CEILING {
            Phase::Yield
        } else {
            Phase::Park
        }
    }

    /// The loop found work: the next idle stretch starts from a fresh
    /// budget, and sleeps, if it gets that far, from the shortest.
    #[inline(always)]
    pub fn reset(&mut self) {
        self.idle = 0;
        self.park = PARK_MIN;
    }

    /// Whether the next [`idle`](Self::idle) call will spin rather than
    /// yield or sleep — always true under `BusySpin`.
    ///
    /// This is what an amortized timer wants to know: while spinning,
    /// the loop runs millions of iterations a second and a clock read
    /// per iteration is a measurable tax, so the timer masks it; once
    /// the loop yields or sleeps, each iteration already pays a syscall
    /// and the mask would only delay the timer.
    #[inline(always)]
    pub fn spinning(&self) -> bool {
        self.phase() == Phase::Spin
    }

    /// Whether the loop has been idle for at least the spin budget —
    /// "sustained idle", under every strategy. Distinct from
    /// [`spinning`](Self::spinning): a `BusySpin` loop is both spinning
    /// and, once quiet long enough, past its budget, whereas under the
    /// other strategies the two are never true together — so
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
        // Further idle iterations keep yielding: they must not wrap the
        // counter back into the spin phase, nor, past the yield budget
        // `SpinYieldPark` reads, move on to sleeping.
        for _ in 0..(IDLE_CEILING * 4) {
            w.idle();
        }
        assert_eq!(w.phase(), Phase::Yield);
    }

    #[test]
    fn spin_yield_park_spins_then_yields_then_parks() {
        let mut w = WaitStrategy::SpinYieldPark.waiter();
        for _ in 0..SPIN_BUDGET {
            assert_eq!(w.phase(), Phase::Spin);
            w.idle();
        }
        assert!(w.past_spin_budget());
        for _ in 0..YIELD_BUDGET {
            assert_eq!(w.phase(), Phase::Yield);
            assert!(!w.spinning());
            w.idle();
        }
        assert_eq!(w.phase(), Phase::Park);
        assert!(!w.spinning());
    }

    /// Each sleep doubles the next, up to the cap, and stays there: the
    /// counter saturating must not drop a long-idle thread back into
    /// yielding.
    #[test]
    fn spin_yield_park_backs_off_to_the_cap() {
        let mut w = WaitStrategy::SpinYieldPark.waiter();
        for _ in 0..IDLE_CEILING {
            w.idle();
        }
        assert_eq!(w.park, PARK_MIN);
        let mut expected = PARK_MIN;
        while expected < PARK_MAX {
            w.idle();
            expected = (expected * 2).min(PARK_MAX);
            assert_eq!(w.park, expected);
            assert_eq!(w.phase(), Phase::Park);
        }
        w.idle();
        assert_eq!(w.park, PARK_MAX);
        assert_eq!(w.phase(), Phase::Park);
    }

    /// Work found mid-sleep starts the next idle stretch over: spinning
    /// first, and sleeping, when it gets there, from the shortest sleep
    /// again rather than from where the last stretch left off.
    #[test]
    fn reset_restores_the_spin_phase_and_the_shortest_park() {
        let mut w = WaitStrategy::SpinYieldPark.waiter();
        for _ in 0..IDLE_CEILING + 2 {
            w.idle();
        }
        assert!(w.park > PARK_MIN);
        w.reset();
        assert_eq!(w.phase(), Phase::Spin);
        assert!(!w.past_spin_budget());
        assert_eq!(w.park, PARK_MIN);
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
        for strategy in [
            WaitStrategy::BusySpin,
            WaitStrategy::SpinThenYield,
            WaitStrategy::SpinYieldPark,
        ] {
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
    /// checks is that the yield and sleep phases keep polling.)
    #[test]
    fn leaving_the_spin_phase_keeps_polling() {
        for strategy in [WaitStrategy::SpinThenYield, WaitStrategy::SpinYieldPark] {
            let flag = Arc::new(AtomicBool::new(false));
            let setter = {
                let flag = Arc::clone(&flag);
                std::thread::spawn(move || {
                    // Well past both budgets, so the test does not pass
                    // by the flag being set before the waiter sleeps.
                    std::thread::sleep(std::time::Duration::from_millis(20));
                    flag.store(true, Ordering::Release);
                })
            };
            strategy.wait_until(|| flag.load(Ordering::Acquire));
            setter.join().expect("setter thread");
        }
    }
}
