//! Engine-internal scheduler for time-driven tasks.
//!
//! The scheduler is fed by `JournalEvent::Tick { now_ns }` events published
//! by a dedicated tick thread. Every event entering the matching stage —
//! tick or otherwise — first drains all due tasks, so the scheduler runs
//! deterministically in lockstep with the journal.
//!
//! Tasks are stored in a min-heap keyed on `fire_ns`. A binary heap is the
//! natural fit: peek-min and pop-min are both O(log n), and the heap never
//! needs ordered iteration outside of (re)building from order state.
//!
//! ## Tombstones
//!
//! `BinaryHeap` does not support arbitrary removal, so cancelling or filling
//! a GTD order does *not* remove its scheduled task. The task lingers as a
//! tombstone until its deadline, at which point the drain logic looks up
//! the order, finds it absent, and silently drops the entry. With per-account
//! `OrderId` HWM enforcement, an order id is unique forever, so a tombstone
//! can never accidentally match a different order.
//!
//! Memory cost: under sustained high-cancel-rate GTD traffic the heap can
//! grow to roughly `concurrent_gtd_orders + cancel_rate × avg_lifetime`.
//! Heap operations stay `O(log n)` so latency is bounded; only memory is
//! sensitive. If this becomes a constraint, a parallel `(account, order_id)
//! → handle` index plus heap-with-removal would let cancel paths reap their
//! tasks immediately. We accept the simpler design until profiling justifies
//! the extra bookkeeping.
//!
//! ## Persistence
//!
//! The heap is *derived state* — every current task variant is reconstructible
//! by walking the order books. Snapshots therefore omit the heap; recovery
//! rebuilds it by scanning resting GTD orders and pending GTD stops. When
//! task kinds that don't map to order state are added (e.g. session
//! transitions), the snapshot will need explicit storage for them.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use crate::types::{AccountId, OrderId, Symbol};

/// A single scheduled task waiting in the engine's min-heap.
///
/// Ordered by `fire_ns` so that wrapping in `Reverse` turns the
/// max-heap (`BinaryHeap` default) into the min-heap we want.
// Derive `Ord` after `fire_ns` so the natural ordering matches the heap's
// scheduling intent — tasks compare by deadline, then by kind for stable
// ordering of co-firing tasks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ScheduledTask {
    /// Wall-clock deadline in nanoseconds since epoch. The task fires when
    /// the engine processes any event whose `now_ns >= fire_ns`.
    pub fire_ns: u64,
    /// What to do at `fire_ns`.
    pub kind: ScheduledTaskKind,
}

/// Discriminator for what kind of work fires at `ScheduledTask::fire_ns`.
// Field order matters for `Ord`: `ExpireOrder` sorts by (symbol, account,
// order_id), giving deterministic ordering for tasks that share a deadline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ScheduledTaskKind {
    /// Cancel a GTD order whose `expiry_ns` has been reached. Looks the
    /// order up by `(symbol, account, order_id)` and only cancels if the
    /// order is still present and still GTD — otherwise the task is a
    /// tombstone and is silently dropped.
    ExpireOrder {
        symbol: Symbol,
        account: AccountId,
        order_id: OrderId,
    },
}

/// Min-heap of pending scheduled tasks.
///
/// Wraps `BinaryHeap<Reverse<ScheduledTask>>` to keep the `Reverse` plumbing
/// out of every caller. `BinaryHeap` is preferred over `BTreeSet` here
/// because the only operations on the hot path are `peek-min`, `pop-min`,
/// and `push` — all O(log n) — and we never need ordered iteration.
#[derive(Debug, Default)]
pub struct ScheduledTaskHeap {
    inner: BinaryHeap<Reverse<ScheduledTask>>,
}

impl ScheduledTaskHeap {
    /// Construct an empty heap.
    pub fn new() -> Self {
        Self {
            inner: BinaryHeap::new(),
        }
    }

    /// Total number of pending tasks (including tombstones).
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// True when no tasks are scheduled.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Push a new task onto the heap.
    pub fn push(&mut self, task: ScheduledTask) {
        self.inner.push(Reverse(task));
    }

    /// Pop the next task whose `fire_ns <= now_ns`, if any.
    /// Returns `None` once the head is in the future (or the heap is empty).
    pub fn pop_due(&mut self, now_ns: u64) -> Option<ScheduledTask> {
        match self.inner.peek() {
            Some(Reverse(task)) if task.fire_ns <= now_ns => self.inner.pop().map(|r| r.0),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task(fire_ns: u64) -> ScheduledTask {
        ScheduledTask {
            fire_ns,
            kind: ScheduledTaskKind::ExpireOrder {
                symbol: Symbol(1),
                account: AccountId(1),
                order_id: OrderId(fire_ns),
            },
        }
    }

    #[test]
    fn empty_heap_pops_nothing() {
        let mut heap = ScheduledTaskHeap::new();
        assert!(heap.pop_due(u64::MAX).is_none());
    }

    #[test]
    fn pop_due_fires_only_past_tasks() {
        let mut heap = ScheduledTaskHeap::new();
        heap.push(task(100));
        heap.push(task(50));
        heap.push(task(200));

        assert_eq!(heap.pop_due(40), None, "all tasks still in the future");

        let first = heap.pop_due(150).unwrap();
        assert_eq!(first.fire_ns, 50);
        let second = heap.pop_due(150).unwrap();
        assert_eq!(second.fire_ns, 100);
        assert_eq!(heap.pop_due(150), None, "200 is still in the future");

        let third = heap.pop_due(200).unwrap();
        assert_eq!(third.fire_ns, 200);
        assert!(heap.is_empty());
    }

    #[test]
    fn pop_due_orders_by_fire_ns() {
        let mut heap = ScheduledTaskHeap::new();
        // Push out of order; pop must still come back in fire_ns order.
        for f in [300, 100, 200] {
            heap.push(task(f));
        }
        let mut order = Vec::new();
        while let Some(t) = heap.pop_due(u64::MAX) {
            order.push(t.fire_ns);
        }
        assert_eq!(order, vec![100, 200, 300]);
    }

    /// Co-firing tasks must order deterministically — the field order on
    /// `ScheduledTaskKind::ExpireOrder` (symbol, account, order_id) is
    /// load-bearing for reproducible drain output, and a future refactor
    /// reordering those fields would silently break replay determinism.
    #[test]
    fn co_firing_tasks_order_deterministically() {
        let make = |sym: u32, acct: u32, ord: u64| ScheduledTask {
            fire_ns: 100,
            kind: ScheduledTaskKind::ExpireOrder {
                symbol: Symbol(sym),
                account: AccountId(acct),
                order_id: OrderId(ord),
            },
        };

        // Same fire_ns → ordered by symbol, then account, then order_id.
        assert!(make(1, 1, 1) < make(2, 1, 1), "symbol differentiates first");
        assert!(
            make(1, 1, 1) < make(1, 2, 1),
            "account differentiates second"
        );
        assert!(
            make(1, 1, 1) < make(1, 1, 2),
            "order_id differentiates last"
        );

        // fire_ns dominates regardless of kind ordering.
        let early_high = ScheduledTask {
            fire_ns: 50,
            kind: ScheduledTaskKind::ExpireOrder {
                symbol: Symbol(99),
                account: AccountId(99),
                order_id: OrderId(99),
            },
        };
        let late_low = make(0, 0, 0);
        assert!(early_high < late_low, "fire_ns wins over kind ordering");
    }
}
