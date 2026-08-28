//! The schedule: the tick at which each request is due.
//!
//! Open-loop pacing. The client sends on a fixed schedule rather than as
//! fast as the server answers, and records each latency against the tick
//! the request was *due* -- the standard correction for coordinated
//! omission, without which a stall anywhere hides itself by also pausing
//! the sender. If the loop falls behind, `pop_due` keeps returning the
//! slots it missed until it has caught up, each stamped with its original
//! time, so the backlog shows up as latency rather than as a lower rate.
//!
//! Mirrors `melin-bench-harness::pacing` (staged in the exchange
//! repository), minus the multi-connection stagger: there is one
//! connection here.

pub struct PaceClock {
    /// Ticks between consecutive due times.
    period_ticks: u64,
    /// The next due tick.
    next_due: u64,
}

impl PaceClock {
    /// A schedule of one slot every `period_ticks`, the first at `start`.
    /// The period is clamped to at least one tick, so an absurd rate
    /// cannot make every instant due forever.
    pub fn new(period_ticks: u64, start: u64) -> Self {
        Self {
            period_ticks: period_ticks.max(1),
            next_due: start,
        }
    }

    /// If a slot is due at `now`, its scheduled tick -- not `now` -- and
    /// the schedule advances; otherwise `None`.
    #[inline(always)]
    pub fn pop_due(&mut self, now: u64) -> Option<u64> {
        if now >= self.next_due {
            let due = self.next_due;
            self.next_due = due.saturating_add(self.period_ticks);
            Some(due)
        } else {
            None
        }
    }

    #[cfg(test)]
    pub fn period_ticks(&self) -> u64 {
        self.period_ticks
    }

    #[cfg(test)]
    pub fn next_due(&self) -> u64 {
        self.next_due
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slots_are_due_on_the_period_and_carry_their_scheduled_tick() {
        let mut p = PaceClock::new(1_000, 5_000);
        assert_eq!(p.pop_due(4_999), None);
        assert_eq!(p.pop_due(5_000), Some(5_000));
        assert_eq!(p.pop_due(5_999), None);
        // Late by 500: the slot's own tick comes back, not `now`.
        assert_eq!(p.pop_due(6_500), Some(6_000));
        assert_eq!(p.next_due(), 7_000);
    }

    #[test]
    fn a_loop_that_fell_behind_catches_up_slot_by_slot() {
        let mut p = PaceClock::new(1_000, 0);
        // Nothing happened for three and a half periods.
        assert_eq!(p.pop_due(3_500), Some(0));
        assert_eq!(p.pop_due(3_500), Some(1_000));
        assert_eq!(p.pop_due(3_500), Some(2_000));
        assert_eq!(p.pop_due(3_500), Some(3_000));
        assert_eq!(p.pop_due(3_500), None);
    }

    #[test]
    fn the_period_is_at_least_one_tick() {
        assert_eq!(PaceClock::new(0, 0).period_ticks(), 1);
        assert_eq!(PaceClock::new(7, 0).period_ticks(), 7);
    }
}
