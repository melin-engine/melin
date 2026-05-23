//! Pending-stop side: mirrors `BookSide` but stores `PendingStop`s
//! keyed by trigger price. See `StopSide` for the design rationale.

use std::num::NonZeroU64;

use super::book_side::{INVALID_NODE, LevelHead, SnapshotNodeMapping};
use crate::types::{
    AccountId, OrderId, Price, Quantity, ReservationSlot, SelfTradeProtection, Side, TimeInForce,
};

/// A pending stop order waiting to be triggered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PendingStop {
    pub(super) id: OrderId,
    pub(super) account: AccountId,
    pub(super) side: Side,
    pub(super) trigger_price: Price,
    pub(super) quantity: Quantity,
    pub(super) time_in_force: TimeInForce,
    /// If `Some`, becomes a limit order at this price when triggered.
    /// If `None`, becomes a market order.
    pub(super) limit_price: Option<Price>,
    /// Maximum quote currency cost for buy-side market/stop-market orders.
    /// Prevents fills from exceeding the reserved amount. `None` for sell-side
    /// orders and limit/stop-limit buys (where cost is bounded by price × qty).
    pub(super) quote_budget: Option<u64>,
    /// Self-trade prevention mode, preserved from the original order.
    pub(super) stp: SelfTradeProtection,
    /// Expiry time in nanoseconds (GTD orders). Zero for non-GTD.
    pub(super) expiry_ns: u64,
    /// Reservation slot, carried through from the original submission so
    /// the slot is available when the stop triggers and converts to a
    /// limit/market order.
    pub(super) reservation: ReservationSlot,
}

impl PendingStop {
    /// Create a new pending stop order (used by snapshot restore).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        id: OrderId,
        account: AccountId,
        side: Side,
        trigger_price: Price,
        quantity: Quantity,
        time_in_force: TimeInForce,
        limit_price: Option<Price>,
        quote_budget: Option<u64>,
        stp: SelfTradeProtection,
        expiry_ns: u64,
        reservation: ReservationSlot,
    ) -> Self {
        Self {
            id,
            account,
            side,
            trigger_price,
            quantity,
            time_in_force,
            limit_price,
            quote_budget,
            stp,
            expiry_ns,
            reservation,
        }
    }

    pub(crate) fn id(&self) -> OrderId {
        self.id
    }

    pub(crate) fn account(&self) -> AccountId {
        self.account
    }

    pub(crate) fn side(&self) -> Side {
        self.side
    }

    pub(crate) fn trigger_price(&self) -> Price {
        self.trigger_price
    }

    pub(crate) fn quantity(&self) -> Quantity {
        self.quantity
    }

    pub(crate) fn time_in_force(&self) -> TimeInForce {
        self.time_in_force
    }

    pub(crate) fn limit_price(&self) -> Option<Price> {
        self.limit_price
    }

    pub(crate) fn quote_budget(&self) -> Option<u64> {
        self.quote_budget
    }

    pub(crate) fn stp(&self) -> SelfTradeProtection {
        self.stp
    }

    pub(crate) fn expiry_ns(&self) -> u64 {
        self.expiry_ns
    }
}

/// One side of the pending-stop book (either all buy stops or all sell
/// stops). Same slab + intrusive-FIFO design as `BookSide` but storing
/// `PendingStop`s, so cancelling an individual stop is O(1) regardless
/// of how many other stops share its trigger price.
///
/// **Why mirror `BookSide`:** the access patterns are nearly identical
/// (per-trigger-price FIFO, range queries during `check_triggers`, bulk
/// drain when a level fires). A second sorted `Vec<(Price, LevelHead)>`
/// keeps the level-walk cache-friendly — important because
/// `check_triggers` runs after every match.
#[derive(Debug)]
pub(crate) struct StopSide {
    /// Sorted ascending by trigger Price. Binary search for all lookups.
    levels: Vec<(Price, LevelHead)>,
    /// Slab of stop nodes. Same lifecycle rules as `BookSide::nodes`:
    /// indices stable for the lifetime of the stop, freed slots recycled
    /// LIFO via `free_head` chain through `next`.
    nodes: Vec<StopNode>,
    /// Head of the free list, or `INVALID_NODE` if empty.
    free_head: u32,
}

impl Default for StopSide {
    fn default() -> Self {
        Self {
            levels: Vec::new(),
            nodes: Vec::new(),
            free_head: INVALID_NODE,
        }
    }
}

/// A node in the per-trigger-price intrusive doubly-linked list of
/// pending stops. Mirrors `OrderNode`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct StopNode {
    pub(crate) stop: PendingStop,
    /// Previous node at this trigger price, or `INVALID_NODE` at head.
    /// On free, set to `INVALID_NODE` (free list is singly linked).
    prev: u32,
    /// Next node at this trigger price, or `INVALID_NODE` at tail. While
    /// freed, points at the next free slot.
    next: u32,
}

impl StopSide {
    pub(super) fn with_capacity(node_capacity: usize) -> Self {
        Self {
            levels: Vec::with_capacity(64),
            nodes: Vec::with_capacity(node_capacity),
            free_head: INVALID_NODE,
        }
    }

    #[inline]
    fn search(&self, price: Price) -> Result<usize, usize> {
        self.levels.binary_search_by_key(&price, |(p, _)| *p)
    }

    #[inline]
    fn alloc_node(&mut self, stop: PendingStop) -> u32 {
        if self.free_head != INVALID_NODE {
            let idx = self.free_head;
            let node = &mut self.nodes[idx as usize];
            self.free_head = node.next;
            node.stop = stop;
            node.prev = INVALID_NODE;
            node.next = INVALID_NODE;
            idx
        } else {
            let idx = self.nodes.len() as u32;
            self.nodes.push(StopNode {
                stop,
                prev: INVALID_NODE,
                next: INVALID_NODE,
            });
            idx
        }
    }

    #[inline]
    fn free_node(&mut self, idx: u32) {
        let node = &mut self.nodes[idx as usize];
        node.prev = INVALID_NODE;
        node.next = self.free_head;
        self.free_head = idx;
    }

    /// Push `stop` onto the back of its trigger-price level. Returns the
    /// stable slab index that the caller stores in `OrderBook::stop_index`
    /// for O(1) cancel.
    pub(crate) fn add(&mut self, price: Price, stop: PendingStop) -> u32 {
        let new_idx = self.alloc_node(stop);
        match self.search(price) {
            Ok(level_idx) => {
                let old_tail = self.levels[level_idx].1.tail;
                self.levels[level_idx].1.tail = new_idx;
                self.levels[level_idx].1.len += 1;
                self.nodes[new_idx as usize].prev = old_tail;
                self.nodes[old_tail as usize].next = new_idx;
            }
            Err(level_idx) => {
                self.levels.insert(
                    level_idx,
                    (
                        price,
                        LevelHead {
                            head: new_idx,
                            tail: new_idx,
                            len: 1,
                        },
                    ),
                );
            }
        }
        new_idx
    }

    /// Splice `node_idx` out of `level_idx`, free the slab slot, and
    /// remove the level if it became empty. Mirrors
    /// `BookSide::unlink_node_at_level`.
    fn unlink_node_at_level(&mut self, level_idx: usize, node_idx: u32) -> PendingStop {
        let prev = self.nodes[node_idx as usize].prev;
        let next = self.nodes[node_idx as usize].next;

        if prev != INVALID_NODE {
            self.nodes[prev as usize].next = next;
        }
        if next != INVALID_NODE {
            self.nodes[next as usize].prev = prev;
        }

        let head = &mut self.levels[level_idx].1;
        if head.head == node_idx {
            head.head = next;
        }
        if head.tail == node_idx {
            head.tail = prev;
        }
        head.len -= 1;
        let became_empty = head.len == 0;

        let stop = self.nodes[node_idx as usize].stop;
        self.free_node(node_idx);
        if became_empty {
            self.levels.remove(level_idx);
        }
        stop
    }

    /// O(1) removal given the slab index from `stop_index`.
    pub(crate) fn remove_node(&mut self, price: Price, node_idx: u32) -> Option<PendingStop> {
        let level_idx = self.search(price).ok()?;
        Some(self.unlink_node_at_level(level_idx, node_idx))
    }

    /// Borrow a stop by slab index. Used by `find_gtd_expiry` to read
    /// the pending stop directly from `stop_index`'s handle.
    #[inline]
    pub(crate) fn node(&self, idx: u32) -> &StopNode {
        &self.nodes[idx as usize]
    }

    /// Drain every stop at `price` into `out` (preserving FIFO) and
    /// remove the level. Used by `check_triggers` to fire all stops at
    /// a given trigger price. Caller is responsible for clearing
    /// `stop_index` entries.
    pub(crate) fn drain_level(&mut self, price: Price, out: &mut Vec<PendingStop>) {
        let Ok(level_idx) = self.search(price) else {
            return;
        };
        let head = self.levels[level_idx].1;
        let mut cur = head.head;
        while cur != INVALID_NODE {
            let node = self.nodes[cur as usize];
            out.push(node.stop);
            let next = node.next;
            self.free_node(cur);
            cur = next;
        }
        self.levels.remove(level_idx);
    }

    /// Iterate every pending stop on this side in (trigger price asc,
    /// FIFO within a level) order. Used by snapshot, kill switch, GTD
    /// scan, etc. Not on the hot path.
    pub(crate) fn for_each_stop<F: FnMut(&PendingStop)>(&self, mut f: F) {
        for (_, head) in &self.levels {
            let mut cur = head.head;
            while cur != INVALID_NODE {
                let n = &self.nodes[cur as usize];
                f(&n.stop);
                cur = n.next;
            }
        }
    }

    /// Mutable variant. Used by `inject_reservation_slots` on restore.
    pub(crate) fn for_each_stop_mut<F: FnMut(&mut PendingStop)>(&mut self, mut f: F) {
        for (_, head) in &self.levels {
            let mut cur = head.head;
            while cur != INVALID_NODE {
                let next = self.nodes[cur as usize].next;
                f(&mut self.nodes[cur as usize].stop);
                cur = next;
            }
        }
    }

    /// Iterate trigger prices in ascending order. Used by
    /// `check_triggers` to collect prices ≤ trade (buys) and ≥ trade
    /// (sells, via `.rev()`).
    pub(crate) fn prices_ascending(&self) -> impl DoubleEndedIterator<Item = Price> + '_ {
        self.levels.iter().map(|(p, _)| *p)
    }

    /// True if no pending stops remain on this side.
    pub(crate) fn is_empty(&self) -> bool {
        self.levels.is_empty()
    }

    /// Snapshot: walk levels in ascending trigger-price order, yielding
    /// `(price, ordered_stops)` with FIFO preserved within a level.
    /// Allocates per level — only used by the snapshot codec.
    pub(crate) fn levels_snapshot(&self) -> Vec<(Price, Vec<PendingStop>)> {
        self.levels
            .iter()
            .map(|(price, head)| {
                let mut v = Vec::with_capacity(head.len as usize);
                let mut cur = head.head;
                while cur != INVALID_NODE {
                    let n = &self.nodes[cur as usize];
                    v.push(n.stop);
                    cur = n.next;
                }
                (*price, v)
            })
            .collect()
    }

    /// Reconstruct from snapshot levels and return the
    /// `(account, order_id) -> node_idx` mapping so the caller can
    /// populate `stop_index` with valid handles.
    pub(crate) fn from_levels_snapshot(
        levels: Vec<(Price, Vec<PendingStop>)>,
    ) -> (Self, SnapshotNodeMapping) {
        let total: usize = levels.iter().map(|(_, v)| v.len()).sum();
        let mut side = Self::with_capacity(total.max(64));
        let mut mapping = Vec::with_capacity(total);
        for (price, stops) in levels {
            for stop in stops {
                let key = (stop.account, stop.id);
                let idx = side.add(price, stop);
                mapping.push((key, idx));
            }
        }
        (side, mapping)
    }

    /// Touch every slab page so first-use page faults happen at startup.
    /// No-op when populated — same contract as `BookSide::prefault`.
    pub(super) fn prefault(&mut self) {
        if !self.nodes.is_empty() {
            return;
        }
        let dummy = StopNode {
            stop: PendingStop {
                id: OrderId(0),
                account: AccountId(0),
                side: Side::Buy,
                trigger_price: Price(NonZeroU64::new(1).expect("non-zero literal")),
                quantity: Quantity(NonZeroU64::new(1).expect("non-zero literal")),
                time_in_force: TimeInForce::GTC,
                limit_price: None,
                quote_budget: None,
                stp: SelfTradeProtection::Allow,
                expiry_ns: 0,
                reservation: ReservationSlot::DUMMY,
            },
            prev: INVALID_NODE,
            next: INVALID_NODE,
        };
        let cap = self.nodes.capacity();
        for _ in 0..cap {
            self.nodes.push(dummy);
        }
        self.nodes.clear();
        self.free_head = INVALID_NODE;
    }
}
