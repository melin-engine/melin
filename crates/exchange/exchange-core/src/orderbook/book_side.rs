//! Resting limit-order book side: sorted price levels backed by a slab +
//! intrusive doubly-linked FIFO per level. See `BookSide` for the full
//! storage rationale.

use std::num::NonZeroU64;

use crate::types::{AccountId, OrderId, Price, Quantity, ReservationSlot, Side, TimeInForce};

/// Sentinel for "no node" in the intrusive doubly-linked lists used by
/// `BookSide`. `u32::MAX` saves 4 bytes vs `Option<u32>` and keeps `OrderNode`
/// a tight 64 bytes (one cache line) on x86_64.
pub(super) const INVALID_NODE: u32 = u32::MAX;

/// Snapshot-restore output: `(account, order_id)` paired with the slab
/// index assigned to that resting order. `OrderBook::restore` consumes
/// this to populate `order_index` with valid node handles.
pub(crate) type SnapshotNodeMapping = Vec<((AccountId, OrderId), u32)>;

/// A resting order on the book (the unfilled portion of a limit order).
///
/// Carries the `ReservationSlot` so that fill and cancel paths can
/// resolve the balance reservation in O(1) without a separate HashMap
/// lookup (eliminates the old `order_info` map from Exchange).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RestingOrder {
    pub(super) id: OrderId,
    pub(super) account: AccountId,
    pub(super) remaining: Quantity,
    /// Stored to support selective cancellation (e.g., EndOfDay cancels
    /// only Day orders, not GTC). IOC/FOK orders never rest, so this
    /// is always GTC, Day, or GTD in practice.
    pub(super) time_in_force: TimeInForce,
    /// Expiry time in nanoseconds (GTD orders). Zero for non-GTD.
    pub(super) expiry_ns: u64,
    /// Side of the order (Buy or Sell). Stored here so fill reports
    /// can determine buyer/seller without an external lookup.
    pub(super) side: Side,
    /// Handle into the reservation slab. Embedded here so fill and
    /// cancel paths can release/adjust the reservation in O(1) via
    /// direct Vec index, eliminating the per-order HashMap lookup that
    /// previously dominated the engine profile (~14% of cycles).
    pub(super) reservation: ReservationSlot,
}

impl RestingOrder {
    /// Create a new resting order (used by snapshot restore).
    pub(crate) fn new(
        id: OrderId,
        account: AccountId,
        remaining: Quantity,
        time_in_force: TimeInForce,
        expiry_ns: u64,
        side: Side,
        reservation: ReservationSlot,
    ) -> Self {
        Self {
            id,
            account,
            remaining,
            time_in_force,
            expiry_ns,
            side,
            reservation,
        }
    }

    pub(crate) fn id(&self) -> OrderId {
        self.id
    }

    pub(crate) fn account(&self) -> AccountId {
        self.account
    }

    pub(crate) fn remaining(&self) -> Quantity {
        self.remaining
    }

    pub(crate) fn time_in_force(&self) -> TimeInForce {
        self.time_in_force
    }

    pub(crate) fn expiry_ns(&self) -> u64 {
        self.expiry_ns
    }
}

/// One side of the order book (either all bids or all asks).
///
/// **Storage layout:** a sorted `Vec<(Price, LevelHead)>` of price levels,
/// each holding `(head, tail, len)` of an intrusive doubly-linked FIFO list
/// of `OrderNode`s. All nodes (across all price levels on this side) live in
/// a single slab `Vec<OrderNode>`; freed nodes form a singly-linked free
/// list via `next`. Indices (`u32`) are stable for the lifetime of an order
/// on the book, which lets `OrderBook::order_index` map an
/// `(AccountId, OrderId)` directly to its node — making cancel and amend
/// O(1) instead of O(level_depth).
///
/// **Why per-side and not a `BTreeMap`:** typical books have 5-20 active
/// levels — the sorted `Vec` fits in 1-3 L1 cache lines and binary search
/// has zero pointer-chasing. A `BTreeMap` would allocate a node per level.
///
/// **Time priority:** `head` is the oldest order at a price (matches
/// first); `tail` is the newest. Matching pops from `head`; new resting
/// orders splice onto `tail`.
#[derive(Debug)]
pub(crate) struct BookSide {
    /// Sorted ascending by Price. Binary search for all lookups.
    levels: Vec<(Price, LevelHead)>,
    /// Slab of order nodes. Indices are stable; freed slots are recycled
    /// via the `free_head` free list.
    nodes: Vec<OrderNode>,
    /// Head of the free list, or `INVALID_NODE` if empty. Free nodes
    /// chain through `OrderNode::next`. `Default` on `u32` would give 0,
    /// which is a valid node index — so we hand-implement `Default` to
    /// initialize this to `INVALID_NODE`.
    free_head: u32,
}

impl Default for BookSide {
    fn default() -> Self {
        Self {
            levels: Vec::new(),
            nodes: Vec::new(),
            free_head: INVALID_NODE,
        }
    }
}

/// Per-price-level head/tail of the intrusive list.
/// `len` lets `available_quantity` and balance audits skip walking
/// dead levels and gives O(1) "is this level empty?" checks.
#[derive(Debug, Clone, Copy)]
pub(super) struct LevelHead {
    /// Index of the oldest order (front of FIFO). `INVALID_NODE` only
    /// during transient unlink-then-relink sequences — invariant: a level
    /// in `levels` always has at least one node.
    pub(super) head: u32,
    /// Index of the newest order (back of FIFO).
    pub(super) tail: u32,
    /// Number of orders at this price. `u32` is plenty — even a pathological
    /// 4 billion-deep level would exhaust the slab first.
    pub(super) len: u32,
}

/// A node in the per-level intrusive doubly-linked list.
///
/// **Layout:** `RestingOrder` is 40 bytes plus two `u32` links — 48 bytes
/// total. Forcing 64-byte alignment was tested and *regressed* throughput
/// ~4% on the realistic-flow bench because sequential level walks
/// (`available_quantity`, `for_each_order`) lost cache density that
/// outweighed the per-node single-line read on cancel. The 48-byte
/// natural layout wins on this workload.
#[derive(Debug, Clone, Copy)]
pub(crate) struct OrderNode {
    pub(crate) order: RestingOrder,
    /// Previous node in this level's FIFO, or `INVALID_NODE` at the head.
    /// On free, this is set to `INVALID_NODE` (the free list is singly
    /// linked through `next`).
    prev: u32,
    /// Next node in this level's FIFO, or `INVALID_NODE` at the tail.
    /// While freed, this points at the next free slot.
    next: u32,
}

impl BookSide {
    /// Pre-allocate the slab. Used by `with_capacity` to avoid resize stalls
    /// once warm. The free list is left empty — `alloc_node` will push fresh
    /// entries until the Vec reaches its capacity, at which point freed
    /// nodes get reused in LIFO order.
    pub(super) fn with_capacity(node_capacity: usize) -> Self {
        Self {
            levels: Vec::with_capacity(64),
            nodes: Vec::with_capacity(node_capacity),
            free_head: INVALID_NODE,
        }
    }

    /// Touch every slab page so first-use page faults happen at startup
    /// rather than on the hot path. Mirrors the HashMap prefault on
    /// `OrderBook`. Pushes dummy nodes up to `capacity()` then clears
    /// the Vec — `Vec::clear` retains the allocation (and its physical
    /// pages), so subsequent `alloc_node` writes hit warm memory.
    ///
    /// **No-op when the slab is non-empty.** `Exchange::prefault` is
    /// called once at startup *after* snapshot restore has placed
    /// orders. Clearing a populated slab would leave dangling
    /// `LevelHead.head`/`tail` indices pointing at empty memory.
    /// Idempotent and safe to re-run on an empty book.
    pub(super) fn prefault(&mut self) {
        if !self.nodes.is_empty() {
            // Already has live orders → pages are faulted by the
            // existing nodes; touching them again would corrupt state.
            return;
        }
        // Build a dummy node once and reuse via `Copy`.
        let dummy = OrderNode {
            order: RestingOrder {
                id: OrderId(0),
                account: AccountId(0),
                remaining: Quantity(NonZeroU64::new(1).expect("non-zero literal")),
                time_in_force: TimeInForce::GTC,
                expiry_ns: 0,
                side: Side::Buy,
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
        // Free list stays empty: subsequent `alloc_node` calls take the
        // fresh-push path, overwriting the warm pages from index 0.
        self.free_head = INVALID_NODE;
    }

    /// Binary search for a price level. Returns `Ok(index)` if found,
    /// `Err(index)` for the insertion point.
    #[inline]
    fn search(&self, price: Price) -> Result<usize, usize> {
        self.levels.binary_search_by_key(&price, |(p, _)| *p)
    }

    /// Allocate a slab slot for `order`. Reuses a freed node if available,
    /// else grows the slab. Returns the stable node index. Caller must
    /// link the node into a level.
    #[inline]
    fn alloc_node(&mut self, order: RestingOrder) -> u32 {
        if self.free_head != INVALID_NODE {
            let idx = self.free_head;
            let node = &mut self.nodes[idx as usize];
            self.free_head = node.next;
            node.order = order;
            node.prev = INVALID_NODE;
            node.next = INVALID_NODE;
            idx
        } else {
            // Slab full — push a new entry. `as u32` is fine: the slab is
            // bounded by HashMap capacity (4K-ish) in practice.
            let idx = self.nodes.len() as u32;
            self.nodes.push(OrderNode {
                order,
                prev: INVALID_NODE,
                next: INVALID_NODE,
            });
            idx
        }
    }

    /// Return a node to the free list. Caller must have already unlinked
    /// it from its level. The freed node's `prev`/`next` are clobbered.
    #[inline]
    fn free_node(&mut self, idx: u32) {
        let node = &mut self.nodes[idx as usize];
        node.prev = INVALID_NODE;
        node.next = self.free_head;
        self.free_head = idx;
    }

    /// Push `order` onto the back (newest end) of the price level. Creates
    /// the level if it doesn't exist. Returns the stable slab index that
    /// the caller should store in `OrderBook::order_index` for O(1) cancel.
    pub(crate) fn add(&mut self, price: Price, order: RestingOrder) -> u32 {
        let new_idx = self.alloc_node(order);
        match self.search(price) {
            Ok(level_idx) => {
                // Splice onto the tail of an existing level.
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

    /// Splice `node_idx` out of the level at `level_idx`, free the slab
    /// slot, and remove the level from `levels` if it became empty.
    /// Returns the removed `RestingOrder`. Caller has already located the
    /// level — used by `remove_node` and `pop_front` to skip a redundant
    /// binary search on the hot path.
    fn unlink_node_at_level(&mut self, level_idx: usize, node_idx: u32) -> RestingOrder {
        let prev = self.nodes[node_idx as usize].prev;
        let next = self.nodes[node_idx as usize].next;

        // Splice out of the doubly-linked list.
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

        let order = self.nodes[node_idx as usize].order;
        self.free_node(node_idx);
        if became_empty {
            self.levels.remove(level_idx);
        }
        order
    }

    /// Remove a node from the book in O(1) given its slab index and the
    /// price level it belongs to. Frees the slab slot. Removes the price
    /// level from `levels` if it becomes empty. Returns the removed
    /// `RestingOrder`, or `None` if `price` doesn't match a live level.
    pub(crate) fn remove_node(&mut self, price: Price, node_idx: u32) -> Option<RestingOrder> {
        let level_idx = self.search(price).ok()?;
        Some(self.unlink_node_at_level(level_idx, node_idx))
    }

    /// Pop the front (oldest, highest-priority) order at `price`.
    /// Frees the slab slot and removes the level if it becomes empty.
    /// Used by the matching loop and STP `CancelOldest`/`CancelBoth`.
    /// Returns `(node_idx, order)` so callers can clean up auxiliary
    /// state. Shares `unlink_node_at_level` with `remove_node` so we
    /// only do one binary search.
    pub(crate) fn pop_front(&mut self, price: Price) -> Option<(u32, RestingOrder)> {
        let level_idx = self.search(price).ok()?;
        let head_idx = self.levels[level_idx].1.head;
        let order = self.unlink_node_at_level(level_idx, head_idx);
        Some((head_idx, order))
    }

    /// Index of the front (oldest) node at `price`, or `None` if no level.
    /// Cheap query used by the matching loop's outer guard.
    #[inline]
    pub(crate) fn front_node_idx(&self, price: Price) -> Option<u32> {
        let level_idx = self.search(price).ok()?;
        Some(self.levels[level_idx].1.head)
    }

    /// Borrow a node by slab index. Used by the matching loop to read the
    /// front maker's metadata without locking the borrow checker.
    #[inline]
    pub(crate) fn node(&self, idx: u32) -> &OrderNode {
        &self.nodes[idx as usize]
    }

    /// Mutably borrow a node by slab index. Used to apply partial fills
    /// in-place.
    #[inline]
    pub(crate) fn node_mut(&mut self, idx: u32) -> &mut OrderNode {
        &mut self.nodes[idx as usize]
    }

    /// Iterate every order on this side, calling `f` with the price level
    /// and a reference to each order. Walks levels in ascending price
    /// order, and within a level walks oldest→newest. Used by snapshot,
    /// fee-schedule re-reservation, and bulk-cancel paths.
    pub(crate) fn for_each_order<F: FnMut(Price, &RestingOrder)>(&self, mut f: F) {
        for (price, head) in &self.levels {
            let mut cur = head.head;
            while cur != INVALID_NODE {
                let n = &self.nodes[cur as usize];
                f(*price, &n.order);
                cur = n.next;
            }
        }
    }

    /// Mutable variant of `for_each_order`. Used by snapshot-restore slot
    /// injection to patch reservation slots in place.
    pub(crate) fn for_each_order_mut<F: FnMut(Price, &mut RestingOrder)>(&mut self, mut f: F) {
        for (price, head) in &self.levels {
            let mut cur = head.head;
            while cur != INVALID_NODE {
                // Split borrow: read links before handing &mut order to `f`.
                let next = self.nodes[cur as usize].next;
                f(*price, &mut self.nodes[cur as usize].order);
                cur = next;
            }
        }
    }

    /// Iterate price levels (ascending) yielding only prices. Used by the
    /// matching engine to collect a snapshot of prices to visit before
    /// mutating the book.
    pub(crate) fn prices_ascending(&self) -> impl DoubleEndedIterator<Item = Price> + '_ {
        self.levels.iter().map(|(p, _)| *p)
    }

    /// Snapshot: walk every level in ascending order, yielding
    /// `(price, ordered_orders)` where `ordered_orders` preserves time
    /// priority (oldest first). Used by the snapshot codec — not on the
    /// hot path, so the per-level `Vec` allocation is fine.
    pub(crate) fn levels_snapshot(&self) -> Vec<(Price, Vec<RestingOrder>)> {
        self.levels
            .iter()
            .map(|(price, head)| {
                let mut v = Vec::with_capacity(head.len as usize);
                let mut cur = head.head;
                while cur != INVALID_NODE {
                    let n = &self.nodes[cur as usize];
                    v.push(n.order);
                    cur = n.next;
                }
                (*price, v)
            })
            .collect()
    }

    /// Reconstruct a `BookSide` from pre-sorted snapshot levels.
    /// Returns `(side, mapping)` where `mapping` records the slab index
    /// assigned to each `(account, order_id)` so the caller can populate
    /// `OrderBook::order_index` with valid node indices.
    pub(crate) fn from_levels_snapshot(
        levels: Vec<(Price, Vec<RestingOrder>)>,
    ) -> (Self, SnapshotNodeMapping) {
        // Pre-size the slab to the total order count to avoid re-allocations.
        let total: usize = levels.iter().map(|(_, v)| v.len()).sum();
        let mut side = Self::with_capacity(total.max(64));
        let mut mapping = Vec::with_capacity(total);
        for (price, orders) in levels {
            for order in orders {
                let key = (order.account, order.id);
                let idx = side.add(price, order);
                mapping.push((key, idx));
            }
        }
        (side, mapping)
    }

    /// True if no resting orders remain on this side.
    pub(crate) fn is_empty(&self) -> bool {
        self.levels.is_empty()
    }

    /// Best price on this side: highest for bids, lowest for asks. Since
    /// `levels` is sorted ascending, callers pick `last()` for bids and
    /// `first()` for asks.
    pub(super) fn first_price(&self) -> Option<Price> {
        self.levels.first().map(|(p, _)| *p)
    }

    pub(super) fn last_price(&self) -> Option<Price> {
        self.levels.last().map(|(p, _)| *p)
    }

    /// Total available quantity at prices that would match the given limit.
    /// If `exclude_account` is `Some`, orders from that account are skipped
    /// (used for FOK pre-check with STP CancelNewest/CancelBoth).
    ///
    /// Walks levels from best→worst until `limit` is exceeded; within a
    /// level, walks the linked list head→tail (which is order-agnostic
    /// for summing).
    pub(super) fn available_quantity(
        &self,
        side: Side,
        limit: Option<Price>,
        exclude_account: Option<AccountId>,
    ) -> u64 {
        let mut total: u64 = 0;
        // Closure: walk one level's intrusive list and accumulate qty.
        // Captured outside the match so it isn't duplicated.
        let walk = |head_idx: u32, total: &mut u64| {
            let mut cur = head_idx;
            while cur != INVALID_NODE {
                let n = &self.nodes[cur as usize];
                if exclude_account.is_none_or(|acct| acct != n.order.account) {
                    *total = total.saturating_add(n.order.remaining.get());
                }
                cur = n.next;
            }
        };
        match side {
            Side::Buy => {
                // Bids: iterate from highest price downward.
                for (price, head) in self.levels.iter().rev() {
                    if let Some(limit) = limit
                        && *price < limit
                    {
                        break;
                    }
                    walk(head.head, &mut total);
                }
            }
            Side::Sell => {
                // Asks: iterate from lowest price upward.
                for (price, head) in &self.levels {
                    if let Some(limit) = limit
                        && *price > limit
                    {
                        break;
                    }
                    walk(head.head, &mut total);
                }
            }
        }
        total
    }
}
