//! Flow control strategies for the publisher.
//!
//! The sender loop's [`crate::sender::SenderLoop`] tracks per-receiver
//! state derived from incoming Status Messages and feeds it into a
//! [`FlowControl`] strategy to compute the next `publisher_limit` for
//! the publication log.
//!
//! Two strategies, picked at sender construction:
//!
//! - [`FlowControl::Min`] — pace the publisher to the **slowest**
//!   receiver. Used for **replication**: every replica must keep up,
//!   no exceptions.
//! - [`FlowControl::Max`] — pace the publisher to the **fastest**
//!   receiver. Receivers that fall more than `slow_consumer_threshold`
//!   bytes behind are evicted from the flow-control calculation. Used
//!   for **market-data fan-out**: a slow consumer must not stall the
//!   feed for everyone.

use std::collections::HashMap;

/// Per-receiver state tracked by the sender; the input to
/// [`FlowControl::compute_publisher_limit`] and
/// [`FlowControl::find_slow_consumers`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReceiverState {
    /// Highest position the receiver has acknowledged consuming
    /// (from its most recent Status Message).
    pub consumption_position: u64,
    /// `receiver_window` from its most recent Status Message — how
    /// many bytes past `consumption_position` it can still buffer.
    pub receiver_window: u32,
}

#[derive(Debug, Clone, Copy)]
pub enum FlowControl {
    /// Pace the publisher to the slowest receiver. The publisher
    /// limit is `min(consumption) + min(receiver_window)`. No
    /// receivers are ever evicted — backpressure propagates to the
    /// engine if a replica falls behind.
    Min,
    /// Pace the publisher to the fastest receiver. Receivers whose
    /// consumption position lags the fastest by more than
    /// `slow_consumer_threshold` bytes are returned by
    /// [`Self::find_slow_consumers`] for the sender to evict; they
    /// are then no longer considered for the limit calculation.
    Max { slow_consumer_threshold: u64 },
}

impl FlowControl {
    /// Compute the next `publisher_limit` from current receiver state,
    /// or `None` if there are no receivers (caller should leave the
    /// log's existing limit unchanged).
    ///
    /// `Min`: `min(r.consumption_position + r.receiver_window)` — the
    /// per-receiver "max acceptable position", taken as a minimum so
    /// no receiver overflows its window. **Not** `min_pos +
    /// min_window` — those differ when no single receiver has both
    /// the smallest pos and smallest window.
    ///
    /// `Max`: `max(r.consumption_position + r.receiver_window)` — the
    /// publisher tracks the fastest receiver; slower ones fall
    /// behind and may be evicted by [`Self::find_slow_consumers`].
    pub fn compute_publisher_limit(&self, receivers: &HashMap<u64, ReceiverState>) -> Option<u64> {
        if receivers.is_empty() {
            return None;
        }
        // Each receiver's "max acceptable position" — the highest byte
        // position it can absorb given its current consumption and
        // advertised window.
        let acceptances = receivers.values().map(|r| {
            r.consumption_position
                .saturating_add(r.receiver_window as u64)
        });
        let limit = match self {
            FlowControl::Min => acceptances.min().expect("non-empty"),
            FlowControl::Max { .. } => acceptances.max().expect("non-empty"),
        };
        Some(limit)
    }

    /// Identify receivers that should be evicted because they're too
    /// far behind. `Min` returns an empty list (it never evicts).
    /// `Max` returns receivers whose `consumption_position` lags the
    /// fastest by more than `slow_consumer_threshold`.
    pub fn find_slow_consumers(&self, receivers: &HashMap<u64, ReceiverState>) -> Vec<u64> {
        match self {
            FlowControl::Min => Vec::new(),
            FlowControl::Max {
                slow_consumer_threshold,
            } => {
                if receivers.is_empty() {
                    return Vec::new();
                }
                let max_pos = receivers
                    .values()
                    .map(|r| r.consumption_position)
                    .max()
                    .expect("non-empty");
                receivers
                    .iter()
                    .filter(|(_, r)| {
                        max_pos.saturating_sub(r.consumption_position) > *slow_consumer_threshold
                    })
                    .map(|(id, _)| *id)
                    .collect()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rs(pos: u64, window: u32) -> ReceiverState {
        ReceiverState {
            consumption_position: pos,
            receiver_window: window,
        }
    }

    #[test]
    fn empty_receivers_returns_none_for_both_strategies() {
        let empty = HashMap::new();
        assert_eq!(FlowControl::Min.compute_publisher_limit(&empty), None);
        assert_eq!(
            FlowControl::Max {
                slow_consumer_threshold: 1024
            }
            .compute_publisher_limit(&empty),
            None
        );
    }

    #[test]
    fn min_with_single_receiver_returns_pos_plus_window() {
        let mut rcvs = HashMap::new();
        rcvs.insert(1, rs(1000, 4096));
        assert_eq!(FlowControl::Min.compute_publisher_limit(&rcvs), Some(5096));
    }

    #[test]
    fn min_picks_smallest_per_receiver_acceptance() {
        let mut rcvs = HashMap::new();
        // R1: pos=1000, window=8192 → acceptance 9192
        // R2: pos=2000, window=4096 → acceptance 6096
        // Min: smallest acceptance wins so no receiver overflows.
        // Note this is NOT `min_pos + min_window` (= 1000 + 4096 =
        // 5096) — that's strictly more conservative than necessary.
        rcvs.insert(1, rs(1000, 8192));
        rcvs.insert(2, rs(2000, 4096));
        assert_eq!(FlowControl::Min.compute_publisher_limit(&rcvs), Some(6096));
    }

    #[test]
    fn max_with_single_receiver_returns_pos_plus_window() {
        let mut rcvs = HashMap::new();
        rcvs.insert(1, rs(1000, 4096));
        assert_eq!(
            FlowControl::Max {
                slow_consumer_threshold: 1024
            }
            .compute_publisher_limit(&rcvs),
            Some(5096)
        );
    }

    #[test]
    fn max_picks_largest_per_receiver_acceptance() {
        let mut rcvs = HashMap::new();
        // R1: acceptance = 1000 + 8192 = 9192 (the larger one, despite smaller pos)
        // R2: acceptance = 2000 + 4096 = 6096
        // Max: largest acceptance wins. Note this is NOT `max_pos +
        // max_window` (= 2000 + 8192 = 10192).
        rcvs.insert(1, rs(1000, 8192));
        rcvs.insert(2, rs(2000, 4096));
        assert_eq!(
            FlowControl::Max {
                slow_consumer_threshold: 1024 * 1024
            }
            .compute_publisher_limit(&rcvs),
            Some(9192)
        );
    }

    #[test]
    fn min_never_finds_slow_consumers() {
        let mut rcvs = HashMap::new();
        rcvs.insert(1, rs(0, 4096));
        rcvs.insert(2, rs(1_000_000_000, 4096));
        assert!(FlowControl::Min.find_slow_consumers(&rcvs).is_empty());
    }

    #[test]
    fn max_finds_consumers_lagging_beyond_threshold() {
        let mut rcvs = HashMap::new();
        rcvs.insert(1, rs(10_000, 4096));
        rcvs.insert(2, rs(5_000, 4096));
        rcvs.insert(3, rs(2_000, 4096));
        let fc = FlowControl::Max {
            slow_consumer_threshold: 4_000,
        };
        // Max pos = 10_000. Threshold = 4_000.
        // R1 lag = 0 (not slow). R2 lag = 5_000 (slow). R3 lag = 8_000 (slow).
        let mut slow = fc.find_slow_consumers(&rcvs);
        slow.sort(); // HashMap iteration is unspecified
        assert_eq!(slow, vec![2, 3]);
    }

    #[test]
    fn max_finds_no_slow_consumers_when_all_within_threshold() {
        let mut rcvs = HashMap::new();
        rcvs.insert(1, rs(10_000, 4096));
        // R2 lag = 4_000, equals threshold (`>` not `>=`), so NOT slow.
        rcvs.insert(2, rs(6_000, 4096));
        let fc = FlowControl::Max {
            slow_consumer_threshold: 4_000,
        };
        assert!(fc.find_slow_consumers(&rcvs).is_empty());
    }

    #[test]
    fn max_with_empty_receivers_returns_no_slow_consumers() {
        let empty = HashMap::new();
        let fc = FlowControl::Max {
            slow_consumer_threshold: 1024,
        };
        assert!(fc.find_slow_consumers(&empty).is_empty());
    }

    #[test]
    fn saturating_arithmetic_prevents_overflow() {
        let mut rcvs = HashMap::new();
        rcvs.insert(1, rs(u64::MAX - 100, u32::MAX));
        // Without saturating: u64::MAX - 100 + u32::MAX would overflow.
        assert_eq!(
            FlowControl::Min.compute_publisher_limit(&rcvs),
            Some(u64::MAX),
        );
    }
}
