//! Order book with price-time priority matching.
//!
//! Bids are stored in descending price order, asks in ascending.
//! Within a price level, orders are matched FIFO.

use std::num::NonZeroU64;

mod book_side;
mod stop_side;

use book_side::INVALID_NODE;
pub(crate) use book_side::{BookSide, RestingOrder};
pub(crate) use stop_side::{PendingStop, StopSide};

use crate::slab_map::SlabMap;
use crate::types::{
    AccountId, ExecutionReport, Order, OrderId, OrderType, Price, Quantity, RejectReason,
    ReservationSlot, SelfTradeProtection, Side, Symbol, TimeInForce,
};

/// Central limit order book for a single instrument.
#[derive(Debug)]
pub struct OrderBook {
    /// Symbol this book belongs to. Carried on every emitted
    /// `ExecutionReport` so downstream consumers (gateways, mirrors)
    /// can route events without external context.
    symbol: Symbol,
    bids: BookSide,
    asks: BookSide,
    /// O(1) lookup mapping `(account, order_id)` to a resting order's
    /// location and slab handle. Keyed by `(AccountId, OrderId)` to
    /// eliminate cross-account collisions — different accounts can
    /// independently use the same OrderId without index conflicts.
    ///
    /// `SlabMap` (hashbrown lookup + dense Vec slab) replaces the
    /// previous astenn HashMap so the structure stays bounded under
    /// high-churn workloads: astenn's extendible-hashing directory
    /// grew with lifetime inserts (not live count), producing first-
    /// touch page-fault outliers in the deep tail. Hashbrown's Robin
    /// Hood + backshift deletion plus the slab's freelist keep the
    /// memory footprint proportional to peak live entries.
    ///
    /// The 4-tuple value stores:
    /// - `Side` — which `BookSide` slab the node lives in
    /// - `Price` — the price level (used to update `LevelHead` on remove)
    /// - `ReservationSlot` — so cancel/amend release balance without an
    ///   extra HashMap lookup
    /// - `u32` — the slab index, making `BookSide::remove_node` O(1)
    ///   instead of an O(level_depth) `VecDeque` scan
    order_index: SlabMap<(Side, Price, ReservationSlot, u32)>,
    /// BTreeMap keyed by trigger price so we can efficiently find all stops
    /// that should fire at a given trade price. Stop buys trigger when price
    /// rises (iterate from lowest trigger up), stop sells when price falls
    /// (iterate from highest trigger down).
    /// Pending stop orders keyed by trigger price, mirroring the
    /// limit-side `BookSide`: a slab + intrusive FIFO per trigger so
    /// individual cancel is O(1) regardless of level depth.
    stop_buys: StopSide,
    stop_sells: StopSide,
    /// Tracks which order IDs are pending stops, for cancel support.
    /// Keyed by (AccountId, OrderId) to match order_index and eliminate
    /// cross-account collisions. Value tuple carries the slab index so
    /// cancel can splice the stop out without scanning its trigger
    /// level. Same `SlabMap` rationale as `order_index`.
    stop_index: SlabMap<(Side, Price, u32)>,
    /// Last trade price, used to determine which stops to trigger.
    last_trade_price: Option<Price>,
    /// Reusable buffers to avoid per-order allocations on the hot path.
    /// Cleared and reused each call. Capacity grows to high-water mark and stays.
    trigger_price_buf: Vec<Price>,
    triggered_buf: Vec<PendingStop>,
    /// Reusable buffer for `match_against()` to collect matchable price levels.
    /// We can't iterate the BTreeMap and mutate it simultaneously (filled makers
    /// are removed during matching), so prices are collected first. This buffer
    /// avoids a heap allocation on every aggressive order.
    match_price_buf: Vec<Price>,
    /// Reservation slots from orders consumed during the last `execute()` or
    /// `cancel()` call. Filled makers and STP-cancelled makers push their
    /// slots here so the Exchange can release reservations without a HashMap
    /// lookup. Cleared at the start of each operation.
    consumed_slots: Vec<(AccountId, OrderId, Side, ReservationSlot)>,
}

impl OrderBook {
    pub fn new(symbol: Symbol) -> Self {
        Self {
            symbol,
            bids: BookSide::default(),
            asks: BookSide::default(),
            order_index: SlabMap::new(),
            stop_buys: StopSide::default(),
            stop_sells: StopSide::default(),
            stop_index: SlabMap::new(),
            last_trade_price: None,
            // Hot-path scratch buffers reused across orders (cleared at
            // the top of each match). 64 capacity covers the typical
            // sweep width — an aggressive order that crosses more than
            // 64 levels (or fills more than 64 makers) is rare. Pre-sizing
            // here means a fresh book never reallocates during its first
            // burst of activity.
            trigger_price_buf: Vec::with_capacity(64),
            triggered_buf: Vec::with_capacity(64),
            match_price_buf: Vec::with_capacity(64),
            consumed_slots: Vec::with_capacity(64),
        }
    }

    /// Create an OrderBook pre-sized for production workloads.
    ///
    /// Capacity is intentionally modest (4K order slots, 1K stop slots) so
    /// the hash tables fit in L2 cache (~160 KB). Oversized tables cause
    /// random probes to miss L2 on every access (~40-80 ns per miss),
    /// dominating the cost of cancel and cancel-replace operations.
    /// Hashbrown resizes by doubling, so a 4K→8K resize moves ~128 KB —
    /// a one-time ~5 µs stall that appears in p99.99 at most.
    pub fn with_capacity(symbol: Symbol) -> Self {
        // Pre-size each side's slab to ~2K nodes — half the order_index
        // capacity, since orders split roughly bid/ask. Avoids growing the
        // slab during the warmup phase of a hot book.
        Self {
            symbol,
            bids: BookSide::with_capacity(2_048),
            asks: BookSide::with_capacity(2_048),
            // One entry per resting order for O(1) cancel lookups. 4096
            // slots covers typical book depth (100-2000 orders) without
            // hot-path resize. `SlabMap` keeps the structure bounded by
            // peak live entries under churn, so this is a tighter
            // "expected steady-state size" than the previous astenn
            // capacity (which had to be over-allocated to hide the
            // lifetime-insert growth pathology).
            order_index: SlabMap::with_capacity(4_096),
            // Stops are ~3% of order flow so a 1K slab covers a hot
            // book without wasted space.
            stop_buys: StopSide::with_capacity(1_024),
            stop_sells: StopSide::with_capacity(1_024),
            stop_index: SlabMap::with_capacity(1_024),
            last_trade_price: None,
            trigger_price_buf: Vec::with_capacity(64),
            triggered_buf: Vec::with_capacity(64),
            // Typical aggressive order sweeps a handful of price levels.
            match_price_buf: Vec::with_capacity(64),
            consumed_slots: Vec::with_capacity(64),
        }
    }

    /// Touch all pre-allocated HashMap pages so page faults happen at startup,
    /// not on the hot path. Insert dummy entries up to capacity, then clear.
    pub fn prefault(&mut self) {
        let cap = self.order_index.capacity();
        for i in 0..cap {
            self.order_index.insert(
                (AccountId(0), OrderId(i as u64)),
                (
                    Side::Buy,
                    Price(std::num::NonZeroU64::new(1).expect("non-zero literal")),
                    ReservationSlot::DUMMY,
                    INVALID_NODE,
                ),
            );
        }
        self.order_index.clear();

        let cap = self.stop_index.capacity();
        for i in 0..cap {
            self.stop_index.insert(
                (AccountId(0), OrderId(i as u64)),
                (
                    Side::Buy,
                    Price(std::num::NonZeroU64::new(1).expect("non-zero literal")),
                    INVALID_NODE,
                ),
            );
        }
        self.stop_index.clear();

        // Touch every slab page on both sides so the first matching
        // pop / cancel after warmup doesn't pay a page-fault stall.
        self.bids.prefault();
        self.asks.prefault();
        self.stop_buys.prefault();
        self.stop_sells.prefault();
    }

    /// Reconstruct an OrderBook from pre-built parts (used by snapshot restore).
    ///
    /// The order_index entries initially have `ReservationSlot::DUMMY`.
    /// Call `inject_reservation_slots()` after account restore to set
    /// the real slot values.
    pub(crate) fn from_parts(
        symbol: Symbol,
        bids: BookSide,
        asks: BookSide,
        order_index: SlabMap<(Side, Price, ReservationSlot, u32)>,
        stop_buys: StopSide,
        stop_sells: StopSide,
        stop_index: SlabMap<(Side, Price, u32)>,
        last_trade_price: Option<Price>,
    ) -> Self {
        Self {
            symbol,
            bids,
            asks,
            order_index,
            stop_buys,
            stop_sells,
            stop_index,
            last_trade_price,
            // Same hot-path scratch buffers as `new()`; pre-sized so the
            // first match after snapshot restore doesn't realloc. See
            // `new()` for the capacity rationale.
            trigger_price_buf: Vec::with_capacity(64),
            triggered_buf: Vec::with_capacity(64),
            match_price_buf: Vec::with_capacity(64),
            consumed_slots: Vec::with_capacity(64),
        }
    }

    // --- Snapshot accessors ---

    pub(crate) fn bids(&self) -> &BookSide {
        &self.bids
    }

    pub(crate) fn asks(&self) -> &BookSide {
        &self.asks
    }

    pub(crate) fn stop_buys(&self) -> &StopSide {
        &self.stop_buys
    }

    pub(crate) fn stop_sells(&self) -> &StopSide {
        &self.stop_sells
    }

    pub(crate) fn last_trade_price(&self) -> Option<Price> {
        self.last_trade_price
    }

    /// Snapshot the order index as a Vec for serialization.
    /// Serialized as (order_id, account, side, price) for wire compatibility.
    /// ReservationSlot is NOT serialized here — it's restored from
    /// AccountManager's reservation slab during snapshot restore.
    pub(crate) fn snapshot_order_index(&self) -> Vec<(OrderId, AccountId, Side, Price)> {
        self.order_index
            .iter()
            .map(|(&(account, id), &(side, price, _slot, _node))| (id, account, side, price))
            .collect()
    }

    /// Snapshot the stop index as a Vec for serialization.
    /// Serialized as (order_id, account, side, price) for wire compatibility.
    pub(crate) fn snapshot_stop_index(&self) -> Vec<(OrderId, AccountId, Side, Price)> {
        self.stop_index
            .iter()
            .map(|(&(account, id), &(side, price, _node))| (id, account, side, price))
            .collect()
    }

    /// Look up a resting order's location and reservation slot from the index.
    /// O(1) HashMap lookup — no VecDeque scan. Returns `None` if the order is
    /// not on the book.
    pub(crate) fn peek_order_location(
        &self,
        account: AccountId,
        order_id: OrderId,
    ) -> Option<(Side, Price, ReservationSlot)> {
        self.order_index
            .get(&(account, order_id))
            .map(|&(side, price, slot, _node_idx)| (side, price, slot))
    }

    /// Best bid price (highest), or `None` if the bid side is empty.
    pub(crate) fn best_bid(&self) -> Option<Price> {
        self.bids.last_price()
    }

    /// Best ask price (lowest), or `None` if the ask side is empty.
    pub(crate) fn best_ask(&self) -> Option<Price> {
        self.asks.first_price()
    }

    /// Replace a resting order's price and/or quantity in-place.
    ///
    /// Time priority rules:
    /// - Same price, qty decrease → keep position (in-place update)
    /// - Same price, qty increase → lose position (back of queue)
    /// - Price change → lose position (remove + re-add at new level)
    ///
    /// Returns `(account, old_price, old_remaining)` on success, or `None` if
    /// the order is not found. The account is returned so the caller can avoid
    /// a separate index lookup.
    pub(crate) fn replace_order(
        &mut self,
        account: AccountId,
        order_id: OrderId,
        new_price: Price,
        new_quantity: Quantity,
    ) -> Option<(Price, Quantity)> {
        let &(side, old_price, slot, node_idx) = self.order_index.get(&(account, order_id))?;
        let book_side = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };

        if old_price == new_price {
            // Same price level — O(1) via direct slab index, no list scan.
            let node = book_side.node_mut(node_idx);
            let old_remaining = node.order.remaining;

            if new_quantity <= old_remaining {
                // Qty decrease (or same) → in-place update, keep priority.
                node.order.remaining = new_quantity;
            } else {
                // Qty increase → unlink and append to tail (lose priority).
                // The slab index changes because `add` allocates a fresh
                // node slot for the re-insert, so we must update
                // `order_index` accordingly.
                let mut order = book_side.remove_node(old_price, node_idx)?;
                order.remaining = new_quantity;
                let new_node_idx = book_side.add(old_price, order);
                self.order_index
                    .insert((account, order_id), (side, old_price, slot, new_node_idx));
            }
            Some((old_price, old_remaining))
        } else {
            // Price change → remove from old level, add to new level.
            // Both ends are O(1) — no list scan on either side.
            let mut order = book_side.remove_node(old_price, node_idx)?;
            let old_remaining = order.remaining;
            order.remaining = new_quantity;

            // `add` returns the new slab index; record it so future cancels
            // on this order remain O(1).
            let new_node_idx = book_side.add(new_price, order);
            self.order_index
                .insert((account, order_id), (side, new_price, slot, new_node_idx));

            Some((old_price, old_remaining))
        }
    }

    /// Process an incoming order, appending execution reports to `reports`.
    ///
    /// `quote_budget` limits the total quote currency cost for buy-side market
    /// orders (where the fill price is unknown at reservation time). Pass the
    /// reserved amount so the matching engine stops before exceeding it.
    /// `None` for sells and limit buys (cost bounded by price × quantity).
    /// Process an incoming order, appending execution reports to `reports`.
    ///
    /// `reservation` is the taker's reservation slot from the account manager,
    /// threaded through so it can be embedded in the resting order if it
    /// places on the book. Consumed maker slots are collected in
    /// `consumed_slots` (call `drain_consumed_slots()` after).
    /// Process an incoming order, appending execution reports to `reports`.
    ///
    /// Returns `true` if the taker order rested on the book (as a resting
    /// limit or pending stop), `false` if it was fully consumed (filled,
    /// cancelled, or rejected). The caller uses this to decide whether to
    /// release leftover reservation surplus without a HashMap lookup.
    pub fn execute(
        &mut self,
        order: Order,
        quote_budget: Option<u64>,
        reservation: ReservationSlot,
        reports: &mut Vec<ExecutionReport>,
    ) -> bool {
        self.consumed_slots.clear();
        match order.order_type {
            OrderType::Limit { price, .. } => {
                self.execute_limit(order, price, reservation, reports);
            }
            OrderType::Market => self.execute_market(order, quote_budget, reservation, reports),
            OrderType::Stop { trigger_price } => {
                self.add_stop(order, trigger_price, None, quote_budget, reservation);
            }
            OrderType::StopLimit {
                trigger_price,
                limit_price,
            } => {
                self.add_stop(order, trigger_price, Some(limit_price), None, reservation);
            }
        }
        self.check_triggers(reports);
        // The taker rested if it's still in an index. Stops always insert
        // into stop_index; limits insert into order_index via place_on_book.
        // Triggered stops that were fully consumed are removed from both.
        // Markets never rest (no index entry).
        //
        // Avoid HashMap lookup: stops always rest (unless triggered and
        // consumed, in which case check_triggers pushed to consumed_slots).
        // Limits rest only if place_on_book was called (Placed report).
        // We can't reliably check Placed reports (no account field), so
        // use the order_index lookup only for limit orders that had fills
        // but weren't freed — the common case (fully filled or cancelled)
        // is handled by the consumed_slots/freed logic in Exchange.
        //
        // For now, use a fast path: stops always return true (check_triggers
        // handles consumed stops via consumed_slots). Markets always return
        // false. Limits check order_index (one lookup instead of two).
        match order.order_type {
            OrderType::Stop { .. } | OrderType::StopLimit { .. } => {
                // Stop may have triggered during check_triggers. If it was
                // consumed, it's in consumed_slots. Return true here so the
                // Exchange skips the taker leftover release; the consumed
                // loop handles it.
                true
            }
            OrderType::Market => false,
            OrderType::Limit { .. } => self.order_index.contains_key(&(order.account, order.id)),
        }
    }

    /// Drain consumed slots from the last `execute()` or `cancel()` call.
    /// Each entry is (account, order_id, side, reservation_slot) for a
    /// maker that was fully filled or STP-cancelled.
    pub fn drain_consumed_slots(
        &mut self,
    ) -> std::vec::Drain<'_, (AccountId, OrderId, Side, ReservationSlot)> {
        self.consumed_slots.drain(..)
    }

    /// Cancel a resting or pending stop order by (account, order_id).
    ///
    /// Returns the `(Side, ReservationSlot)` of the cancelled order (if found)
    /// so the caller can release the reservation directly.
    pub fn cancel(
        &mut self,
        account: AccountId,
        order_id: OrderId,
        reports: &mut Vec<ExecutionReport>,
    ) -> Option<(Side, ReservationSlot)> {
        // Try resting orders first. O(1): the index gives us the slab
        // node directly, so removal is a constant-time linked-list splice.
        if let Some((side, price, slot, node_idx)) = self.order_index.remove(&(account, order_id)) {
            let book_side = match side {
                Side::Buy => &mut self.bids,
                Side::Sell => &mut self.asks,
            };
            if let Some(order) = book_side.remove_node(price, node_idx) {
                reports.push(ExecutionReport::Cancelled {
                    order_id,
                    symbol: self.symbol,
                    account,
                    remaining_quantity: order.remaining,
                });
            }
            return Some((side, slot));
        }

        // Try pending stops. O(1): the slab index from `stop_index`
        // pinpoints the node, so removal is a constant-time linked-list
        // splice — no scan over other stops sharing the trigger price.
        if let Some((side, trigger_price, node_idx)) = self.stop_index.remove(&(account, order_id))
        {
            let stops = match side {
                Side::Buy => &mut self.stop_buys,
                Side::Sell => &mut self.stop_sells,
            };
            if let Some(stop) = stops.remove_node(trigger_price, node_idx) {
                let slot = stop.reservation;
                reports.push(ExecutionReport::Cancelled {
                    order_id,
                    symbol: self.symbol,
                    account: stop.account,
                    remaining_quantity: stop.quantity,
                });
                return Some((side, slot));
            }
        }
        None
    }

    /// Cancel all resting orders and pending stops belonging to the given
    /// account. Used by the kill switch. Produces one `Cancelled` report
    /// per removed order.
    ///
    /// Scans the book linearly — O(total_orders) — which is acceptable
    /// since kill switch is a rare emergency operation, not on the hot path.
    pub fn cancel_all_for_account(
        &mut self,
        account: AccountId,
        reports: &mut Vec<ExecutionReport>,
    ) {
        self.consumed_slots.clear();
        // Collect matching order IDs by scanning the book sides directly.
        // We scan the price levels (not order_index) because RestingOrder
        // carries the account field we need to filter on.
        let mut to_cancel: Vec<OrderId> = Vec::new();

        self.bids.for_each_order(|_, order| {
            if order.account == account {
                to_cancel.push(order.id);
            }
        });
        self.asks.for_each_order(|_, order| {
            if order.account == account {
                to_cancel.push(order.id);
            }
        });

        // Scan pending stops.
        self.stop_buys.for_each_stop(|stop| {
            if stop.account == account {
                to_cancel.push(stop.id);
            }
        });
        self.stop_sells.for_each_stop(|stop| {
            if stop.account == account {
                to_cancel.push(stop.id);
            }
        });

        // Cancel each collected order. cancel() handles removal from
        // order_index/stop_index, BookSide levels, and report generation.
        // Collect returned slots into consumed_slots for the caller.
        for id in to_cancel {
            if let Some((side, slot)) = self.cancel(account, id, reports) {
                self.consumed_slots.push((account, id, side, slot));
            }
        }
    }

    /// Cancel all resting orders and pending stops with `TimeInForce::Day`.
    /// Called at end-of-session. GTC orders are left untouched.
    pub fn cancel_day_orders(&mut self, reports: &mut Vec<ExecutionReport>) {
        self.consumed_slots.clear();
        let mut to_cancel: Vec<(AccountId, OrderId)> = Vec::new();

        self.bids.for_each_order(|_, order| {
            if order.time_in_force == TimeInForce::Day {
                to_cancel.push((order.account, order.id));
            }
        });
        self.asks.for_each_order(|_, order| {
            if order.time_in_force == TimeInForce::Day {
                to_cancel.push((order.account, order.id));
            }
        });

        self.stop_buys.for_each_stop(|stop| {
            if stop.time_in_force == TimeInForce::Day {
                to_cancel.push((stop.account, stop.id));
            }
        });
        self.stop_sells.for_each_stop(|stop| {
            if stop.time_in_force == TimeInForce::Day {
                to_cancel.push((stop.account, stop.id));
            }
        });

        for (account, id) in to_cancel {
            if let Some((side, slot)) = self.cancel(account, id, reports) {
                self.consumed_slots.push((account, id, side, slot));
            }
        }
    }

    fn execute_limit(
        &mut self,
        order: Order,
        price: Price,
        reservation: ReservationSlot,
        reports: &mut Vec<ExecutionReport>,
    ) {
        // Post-only: reject if the order would cross the spread.
        if let OrderType::Limit {
            post_only: true, ..
        } = order.order_type
        {
            let would_cross = match order.side {
                Side::Buy => self.best_ask().is_some_and(|ask| price >= ask),
                Side::Sell => self.best_bid().is_some_and(|bid| price <= bid),
            };
            if would_cross {
                reports.push(ExecutionReport::Rejected {
                    order_id: order.id,
                    symbol: self.symbol,
                    account: order.account,
                    reason: RejectReason::PostOnlyWouldCross,
                });
                return;
            }
        }

        let opposite = self.opposite_side(order.side);

        // FOK: check if we can fill entirely before doing anything.
        // With STP enabled, same-account orders won't fill (they get cancelled
        // or block matching), so exclude them from the available quantity check.
        if order.time_in_force == TimeInForce::FOK {
            let exclude = match order.stp {
                SelfTradeProtection::Allow => None,
                _ => Some(order.account),
            };
            let available =
                opposite.available_quantity(Self::opposite(order.side), Some(price), exclude);
            if available < order.quantity.get() {
                reports.push(ExecutionReport::Rejected {
                    order_id: order.id,
                    symbol: self.symbol,
                    account: order.account,
                    reason: RejectReason::FOKCannotFill,
                });
                return;
            }
        }

        let (remaining, stp_cancelled) = self.match_against(
            order.id,
            order.account,
            order.side,
            order.quantity,
            Some(price),
            None,
            order.stp,
            reports,
        );

        match remaining {
            Some(rem) => {
                if stp_cancelled {
                    // STP terminated matching — cancel the taker regardless of TIF.
                    reports.push(ExecutionReport::Cancelled {
                        order_id: order.id,
                        symbol: self.symbol,
                        account: order.account,
                        remaining_quantity: rem,
                    });
                } else {
                    match order.time_in_force {
                        // GTC, Day, and GTD all rest on the book. Day orders
                        // are bulk-cancelled by EndOfDay; GTD orders are
                        // cancelled by the scheduler when their expiry fires.
                        TimeInForce::GTC | TimeInForce::Day | TimeInForce::GTD => {
                            self.place_on_book(
                                order.id,
                                order.account,
                                order.side,
                                price,
                                rem,
                                order.time_in_force,
                                order.expiry_ns,
                                reservation,
                                reports,
                            );
                        }
                        TimeInForce::IOC | TimeInForce::FOK => {
                            reports.push(ExecutionReport::Cancelled {
                                order_id: order.id,
                                symbol: self.symbol,
                                account: order.account,
                                remaining_quantity: rem,
                            });
                        }
                    }
                }
            }
            None => {
                // Fully filled, nothing to do.
            }
        }
    }

    fn execute_market(
        &mut self,
        order: Order,
        quote_budget: Option<u64>,
        _reservation: ReservationSlot,
        reports: &mut Vec<ExecutionReport>,
    ) {
        let opposite = self.opposite_side(order.side);

        // FOK: check if we can fill entirely.
        if order.time_in_force == TimeInForce::FOK {
            let exclude = match order.stp {
                SelfTradeProtection::Allow => None,
                _ => Some(order.account),
            };
            let available = opposite.available_quantity(Self::opposite(order.side), None, exclude);
            if available < order.quantity.get() {
                reports.push(ExecutionReport::Rejected {
                    order_id: order.id,
                    symbol: self.symbol,
                    account: order.account,
                    reason: RejectReason::FOKCannotFill,
                });
                return;
            }
        }

        // Reject market order on empty book.
        if opposite.is_empty() {
            reports.push(ExecutionReport::Rejected {
                order_id: order.id,
                symbol: self.symbol,
                account: order.account,
                reason: RejectReason::NoLiquidity,
            });
            return;
        }

        let (remaining, _stp_cancelled) = self.match_against(
            order.id,
            order.account,
            order.side,
            order.quantity,
            None,
            quote_budget,
            order.stp,
            reports,
        );

        if let Some(rem) = remaining {
            // Market order couldn't fully fill — cancel remainder.
            // (STP cancellation also results in cancelling the remainder.)
            reports.push(ExecutionReport::Cancelled {
                order_id: order.id,
                symbol: self.symbol,
                account: order.account,
                remaining_quantity: rem,
            });
        }
    }

    /// Match an incoming order against the opposite side of the book.
    #[allow(clippy::too_many_arguments)]
    ///
    /// `quote_budget` caps the total quote cost for buy-side market orders,
    /// preventing fills from exceeding the reserved amount. Ignored for sells
    /// and limit buys.
    ///
    /// Returns `(remaining_qty, stp_cancelled)`:
    /// - `remaining_qty`: `None` if fully filled, `Some(qty)` if unfilled remainder.
    /// - `stp_cancelled`: `true` if STP terminated matching (taker should be cancelled,
    ///   not placed on book).
    fn match_against(
        &mut self,
        taker_id: OrderId,
        taker_account: AccountId,
        taker_side: Side,
        mut quantity: Quantity,
        price_limit: Option<Price>,
        mut quote_budget: Option<u64>,
        stp: SelfTradeProtection,
        reports: &mut Vec<ExecutionReport>,
    ) -> (Option<Quantity>, bool) {
        let opposite = match taker_side {
            Side::Buy => &mut self.asks,
            Side::Sell => &mut self.bids,
        };

        // Collect the prices we need to visit into a reusable buffer. We can't
        // iterate the BTreeMap and mutate it simultaneously (filled makers are
        // removed), so prices are collected first. The buffer lives on OrderBook
        // to avoid a heap allocation on every aggressive order.
        self.match_price_buf.clear();
        match taker_side {
            Side::Buy => {
                // Buy matches against asks (lowest first).
                self.match_price_buf.extend(
                    opposite
                        .prices_ascending()
                        .take_while(|&p| price_limit.is_none_or(|limit| p <= limit)),
                );
            }
            Side::Sell => {
                // Sell matches against bids (highest first).
                self.match_price_buf.extend(
                    opposite
                        .prices_ascending()
                        .rev()
                        .take_while(|&p| price_limit.is_none_or(|limit| p >= limit)),
                );
            }
        };

        let mut stp_cancelled = false;

        // Iterate from a index to avoid borrowing self.match_price_buf while
        // mutating self through opposite. The buffer won't be modified during
        // the loop, so index-based access is safe and equivalent to iter().
        let mut price_idx = 0;
        'outer: while price_idx < self.match_price_buf.len() {
            let price = self.match_price_buf[price_idx];
            price_idx += 1;

            // Walk this price level's intrusive FIFO from the head. `pop_front`
            // already removes the level when it empties, so we don't need a
            // separate `remove_level` after the inner loop exits.
            while let Some(maker_idx) = opposite.front_node_idx(price) {
                // Snapshot the maker fields we need; releasing the borrow so
                // we can mutate via `pop_front`/`node_mut` on subsequent
                // branches without aliasing.
                let maker_node = opposite.node(maker_idx);
                let maker_account = maker_node.order.account;
                let maker_id = maker_node.order.id;
                let maker_remaining = maker_node.order.remaining;
                let maker_side = maker_node.order.side;
                let maker_reservation = maker_node.order.reservation;

                // Self-trade prevention: check if taker and maker belong to
                // the same account before generating a fill.
                if stp != SelfTradeProtection::Allow && maker_account == taker_account {
                    match stp {
                        SelfTradeProtection::Allow => unreachable!(),
                        SelfTradeProtection::CancelNewest => {
                            // Cancel the taker, leave the maker on the book.
                            stp_cancelled = true;
                            break 'outer;
                        }
                        SelfTradeProtection::CancelOldest => {
                            // Cancel the maker, continue matching the taker.
                            // Safe: this iteration was entered via the
                            // `while let Some(_) = opposite.front_node_idx(price)`
                            // guard above and `opposite` has not been mutated
                            // since (we only read maker fields). An empty pop
                            // here would indicate a serious bookkeeping bug —
                            // a panic surfaces it instead of silently dropping
                            // a fill, which would corrupt balances.
                            opposite.pop_front(price).expect("front existed");
                            self.order_index.remove(&(maker_account, maker_id));
                            self.consumed_slots.push((
                                maker_account,
                                maker_id,
                                maker_side,
                                maker_reservation,
                            ));
                            reports.push(ExecutionReport::Cancelled {
                                order_id: maker_id,
                                symbol: self.symbol,
                                account: maker_account,
                                remaining_quantity: maker_remaining,
                            });
                            continue;
                        }
                        SelfTradeProtection::CancelBoth => {
                            // Cancel the maker and the taker.
                            // Same invariant as the CancelOldest arm above:
                            // front was just read via `front_node_idx`, no
                            // intervening mutation. A panic here would catch
                            // a slab/level desync rather than silently leaking
                            // a reservation.
                            opposite.pop_front(price).expect("front existed");
                            self.order_index.remove(&(maker_account, maker_id));
                            self.consumed_slots.push((
                                maker_account,
                                maker_id,
                                maker_side,
                                maker_reservation,
                            ));
                            reports.push(ExecutionReport::Cancelled {
                                order_id: maker_id,
                                symbol: self.symbol,
                                account: maker_account,
                                remaining_quantity: maker_remaining,
                            });
                            return (Some(quantity), true);
                        }
                    }
                }

                let mut fill_qty = quantity.min(maker_remaining);

                // Enforce quote budget: limit fill to what the taker can afford.
                if let Some(budget) = quote_budget {
                    let cost = (price.get() as u128) * (fill_qty.get() as u128);
                    if cost > budget as u128 {
                        // Can only afford a partial fill at this price.
                        let affordable = budget / price.get();
                        if affordable == 0 {
                            // Can't afford even 1 lot — stop matching.
                            break 'outer;
                        }
                        // Safety: affordable > 0 checked above.
                        fill_qty = Quantity(NonZeroU64::new(affordable).expect("affordable > 0"))
                            .min(fill_qty);
                    }
                }

                // Fees are zero here — the Exchange computes and sets
                // them after matching, before balance updates.
                reports.push(ExecutionReport::Fill {
                    maker_order_id: maker_id,
                    taker_order_id: taker_id,
                    symbol: self.symbol,
                    maker_account,
                    taker_account,
                    price,
                    quantity: fill_qty,
                    maker_fee: 0,
                    taker_fee: 0,
                });
                self.last_trade_price = Some(price);

                // Deduct cost from budget after the fill.
                if let Some(budget) = &mut quote_budget {
                    let cost = price.get().saturating_mul(fill_qty.get());
                    *budget = budget.saturating_sub(cost);
                }

                match maker_remaining.checked_sub(fill_qty) {
                    Some(new_remaining) => {
                        // Partial maker fill — update remaining in place.
                        opposite.node_mut(maker_idx).order.remaining = new_remaining;
                    }
                    None => {
                        // Maker fully filled — remove from book and record
                        // the slot so the Exchange can release the reservation.
                        // Safe: same invariant as the STP arms above — we
                        // entered this iteration via `front_node_idx(price)`
                        // and `opposite` has not been mutated since (we only
                        // read maker fields and computed the fill locally;
                        // the `Some` arm that calls `node_mut` is a sibling,
                        // not a predecessor, of this branch).
                        opposite.pop_front(price).expect("front existed");
                        self.order_index.remove(&(maker_account, maker_id));
                        self.consumed_slots.push((
                            maker_account,
                            maker_id,
                            maker_side,
                            maker_reservation,
                        ));
                    }
                }

                match quantity.checked_sub(fill_qty) {
                    Some(new_qty) => {
                        quantity = new_qty;
                        // If budget is exhausted, stop matching.
                        if quote_budget == Some(0) {
                            break 'outer;
                        }
                    }
                    None => {
                        // Taker fully filled.
                        return (None, false);
                    }
                }
            }
        }

        (Some(quantity), stp_cancelled)
    }

    fn add_stop(
        &mut self,
        order: Order,
        trigger_price: Price,
        limit_price: Option<Price>,
        quote_budget: Option<u64>,
        reservation: ReservationSlot,
    ) {
        let stop = PendingStop {
            id: order.id,
            account: order.account,
            side: order.side,
            trigger_price,
            quantity: order.quantity,
            time_in_force: order.time_in_force,
            limit_price,
            quote_budget,
            stp: order.stp,
            expiry_ns: order.expiry_ns,
            reservation,
        };
        let stops = match order.side {
            Side::Buy => &mut self.stop_buys,
            Side::Sell => &mut self.stop_sells,
        };
        let node_idx = stops.add(trigger_price, stop);
        // Record the slab index so cancel of this stop is O(1).
        self.stop_index.insert(
            (order.account, order.id),
            (order.side, trigger_price, node_idx),
        );
    }

    /// Check if the last trade price triggers any pending stop orders.
    /// Triggered stops are converted to market/limit orders and executed.
    ///
    /// Uses pre-allocated buffers (`trigger_price_buf`, `triggered_buf`) to
    /// avoid per-order heap allocations on the hot path. Buffers grow to
    /// high-water mark and stay — no per-call allocation after warmup.
    fn check_triggers(&mut self, reports: &mut Vec<ExecutionReport>) {
        let Some(trade_price) = self.last_trade_price else {
            return;
        };

        // Fast path: skip all BTreeMap iteration when no stops are pending.
        // Stops are ~3% of order flow; the other 97% of orders pay zero cost.
        if self.stop_buys.is_empty() && self.stop_sells.is_empty() {
            return;
        }

        // Stop buys: trigger when trade price >= trigger price.
        // Collect all triggers at or below the trade price (ascending order).
        self.trigger_price_buf.clear();
        self.trigger_price_buf.extend(
            self.stop_buys
                .prices_ascending()
                .take_while(|&p| p <= trade_price),
        );

        self.triggered_buf.clear();
        for &price in &self.trigger_price_buf {
            let before = self.triggered_buf.len();
            self.stop_buys.drain_level(price, &mut self.triggered_buf);
            for stop in &self.triggered_buf[before..] {
                self.stop_index.remove(&(stop.account, stop.id));
            }
        }

        // Stop sells: trigger when trade price <= trigger price.
        // Collect all triggers at or above the trade price (descending order).
        self.trigger_price_buf.clear();
        self.trigger_price_buf.extend(
            self.stop_sells
                .prices_ascending()
                .rev()
                .take_while(|&p| p >= trade_price),
        );

        for &price in &self.trigger_price_buf {
            let before = self.triggered_buf.len();
            self.stop_sells.drain_level(price, &mut self.triggered_buf);
            for stop in &self.triggered_buf[before..] {
                self.stop_index.remove(&(stop.account, stop.id));
            }
        }

        // Execute triggered stops as market/limit orders.
        // Take the buffer to avoid borrowing `self` while calling `execute_*`.
        // `std::mem::take` swaps in an empty Vec (no allocation) and returns
        // the populated one. After the loop, swap it back to retain capacity.
        let mut triggered = std::mem::take(&mut self.triggered_buf);
        for stop in triggered.drain(..) {
            reports.push(ExecutionReport::Triggered {
                order_id: stop.id,
                symbol: self.symbol,
                account: stop.account,
                trigger_price: stop.trigger_price,
            });

            // Triggered stops become regular limit/market orders — never post-only,
            // since the intent is to execute when the trigger fires.
            let order_type = match stop.limit_price {
                Some(price) => OrderType::Limit {
                    price,
                    post_only: false,
                },
                None => OrderType::Market,
            };

            let order = Order {
                id: stop.id,
                account: stop.account,
                side: stop.side,
                order_type,
                time_in_force: stop.time_in_force,
                quantity: stop.quantity,
                stp: stop.stp,
                expiry_ns: stop.expiry_ns,
            };

            // Re-enter execute but skip check_triggers to avoid recursion —
            // triggered orders are market/limit, so they won't re-add stops.
            match order.order_type {
                OrderType::Limit { price, .. } => {
                    self.execute_limit(order, price, stop.reservation, reports);
                }
                OrderType::Market => {
                    self.execute_market(order, stop.quote_budget, stop.reservation, reports);
                }
                OrderType::Stop { .. } | OrderType::StopLimit { .. } => {
                    unreachable!("triggered stops become market or limit orders")
                }
            }

            // If the triggered order didn't rest on the book (fully filled
            // or cancelled), record its slot so the Exchange can free it.
            if !self.order_index.contains_key(&(stop.account, stop.id)) {
                self.consumed_slots
                    .push((stop.account, stop.id, stop.side, stop.reservation));
            }
        }
        self.triggered_buf = triggered;
    }

    #[allow(clippy::too_many_arguments)]
    fn place_on_book(
        &mut self,
        id: OrderId,
        account: AccountId,
        side: Side,
        price: Price,
        quantity: Quantity,
        time_in_force: TimeInForce,
        expiry_ns: u64,
        reservation: ReservationSlot,
        reports: &mut Vec<ExecutionReport>,
    ) {
        let book_side = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        let node_idx = book_side.add(
            price,
            RestingOrder {
                id,
                account,
                remaining: quantity,
                time_in_force,
                expiry_ns,
                side,
                reservation,
            },
        );
        // Record the slab index so cancel/amend stays O(1).
        self.order_index
            .insert((account, id), (side, price, reservation, node_idx));
        reports.push(ExecutionReport::Placed {
            order_id: id,
            symbol: self.symbol,
            account,
            side,
            price,
            quantity,
        });
    }

    /// Returns true if the book has no resting orders and no pending stops.
    pub fn is_empty(&self) -> bool {
        self.bids.is_empty()
            && self.asks.is_empty()
            && self.stop_buys.is_empty()
            && self.stop_sells.is_empty()
    }

    /// Cancel ALL resting orders and pending stops regardless of account or TIF.
    /// Used when disabling an instrument. Same pattern as `cancel_day_orders` —
    /// collect IDs first, then cancel to avoid borrowing conflicts.
    pub fn cancel_all_orders(&mut self, reports: &mut Vec<ExecutionReport>) {
        self.consumed_slots.clear();
        let mut to_cancel: Vec<(AccountId, OrderId)> = Vec::new();

        self.bids
            .for_each_order(|_, order| to_cancel.push((order.account, order.id)));
        self.asks
            .for_each_order(|_, order| to_cancel.push((order.account, order.id)));

        self.stop_buys
            .for_each_stop(|stop| to_cancel.push((stop.account, stop.id)));
        self.stop_sells
            .for_each_stop(|stop| to_cancel.push((stop.account, stop.id)));

        for (account, id) in to_cancel {
            if let Some((side, slot)) = self.cancel(account, id, reports) {
                self.consumed_slots.push((account, id, side, slot));
            }
        }
    }

    fn opposite_side(&self, side: Side) -> &BookSide {
        match side {
            Side::Buy => &self.asks,
            Side::Sell => &self.bids,
        }
    }

    fn opposite(side: Side) -> Side {
        match side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        }
    }

    /// Inject real reservation slots into order_index and resting orders
    /// after snapshot restore. Called once after AccountManager rebuilds
    /// the reservation slab and returns slot assignments.
    pub(crate) fn inject_reservation_slots(
        &mut self,
        slots: &[((AccountId, OrderId), ReservationSlot)],
    ) {
        // Build a lookup for fast injection.
        let slot_map: std::collections::HashMap<(AccountId, OrderId), ReservationSlot> =
            slots.iter().copied().collect();

        // Patch order_index entries.
        for (key, val) in self.order_index.iter_mut() {
            if let Some(&slot) = slot_map.get(key) {
                val.2 = slot;
            }
        }

        // Patch resting orders in bids and asks.
        self.bids.for_each_order_mut(|_, order| {
            if let Some(&slot) = slot_map.get(&(order.account, order.id)) {
                order.reservation = slot;
            }
        });
        self.asks.for_each_order_mut(|_, order| {
            if let Some(&slot) = slot_map.get(&(order.account, order.id)) {
                order.reservation = slot;
            }
        });

        // Patch pending stops.
        self.stop_buys.for_each_stop_mut(|stop| {
            if let Some(&slot) = slot_map.get(&(stop.account, stop.id)) {
                stop.reservation = slot;
            }
        });
        self.stop_sells.for_each_stop_mut(|stop| {
            if let Some(&slot) = slot_map.get(&(stop.account, stop.id)) {
                stop.reservation = slot;
            }
        });
    }

    /// Collect (account, order_id) → (side, slot) for all resting orders.
    /// Used by Exchange snapshot to serialize order_sides and active
    /// reservation slot assignments.
    pub(crate) fn active_order_slots(
        &self,
    ) -> impl Iterator<Item = ((AccountId, OrderId), (Side, ReservationSlot))> + '_ {
        self.order_index
            .iter()
            .map(|(&key, &(side, _price, slot, _node))| (key, (side, slot)))
    }

    /// Collect (account, order_id) → (side, slot) for all pending stops.
    /// Called once at snapshot encode time, not on the hot path — the Vec
    /// allocation is fine.
    pub(crate) fn active_stop_slots(&self) -> Vec<((AccountId, OrderId), (Side, ReservationSlot))> {
        let mut out = Vec::new();
        let mut push = |s: &PendingStop| {
            out.push(((s.account, s.id), (s.side, s.reservation)));
        };
        self.stop_buys.for_each_stop(&mut push);
        self.stop_sells.for_each_stop(&mut push);
        out
    }

    /// Iterate every GTD order on the book, resting or pending stop, yielding
    /// `(account, order_id, expiry_ns)`. Used after snapshot restore to
    /// rebuild the scheduler heap from order state.
    pub(crate) fn iter_gtd_orders(&self) -> Vec<(AccountId, OrderId, u64)> {
        // Called once at snapshot restore — not a hot path, so collect
        // into a Vec rather than fight the borrow checker for a streaming
        // iterator over the slab-walking closures.
        let mut out = Vec::new();
        let mut push_if_gtd = |order: &RestingOrder| {
            if order.time_in_force == TimeInForce::GTD {
                out.push((order.account, order.id, order.expiry_ns));
            }
        };
        self.bids.for_each_order(|_, o| push_if_gtd(o));
        self.asks.for_each_order(|_, o| push_if_gtd(o));
        let mut push_stop_if_gtd = |s: &PendingStop| {
            if s.time_in_force == TimeInForce::GTD {
                out.push((s.account, s.id, s.expiry_ns));
            }
        };
        self.stop_buys.for_each_stop(&mut push_stop_if_gtd);
        self.stop_sells.for_each_stop(&mut push_stop_if_gtd);
        out
    }

    /// Look up a resting limit or pending stop by `(account, order_id)` and
    /// return its `expiry_ns` *only if* it is GTD. `None` covers four cases:
    /// the order is not on the book, it isn't GTD, the lookup index points
    /// to a stale entry, or the order_id collides with a removed entry —
    /// the scheduler treats all four as silent tombstones.
    ///
    /// Hot path: called once per scheduled task drain. The level lookup is
    /// `O(log levels)` via `BookSide::search`; the in-level scan is `O(L)`
    /// in the queue length at that price (which cancel/cancel-replace pay
    /// too).
    pub(crate) fn find_gtd_expiry(&self, account: AccountId, order_id: OrderId) -> Option<u64> {
        if let Some(&(side, _price, _slot, node_idx)) = self.order_index.get(&(account, order_id)) {
            // O(1): the slab index points directly at the order.
            let book_side = match side {
                Side::Buy => &self.bids,
                Side::Sell => &self.asks,
            };
            let order = &book_side.node(node_idx).order;
            if order.id == order_id && order.account == account {
                return (order.time_in_force == TimeInForce::GTD).then_some(order.expiry_ns);
            }
        }
        if let Some(&(side, _trigger, node_idx)) = self.stop_index.get(&(account, order_id)) {
            // O(1): stop_index already gave us the slab handle.
            let stops = match side {
                Side::Buy => &self.stop_buys,
                Side::Sell => &self.stop_sells,
            };
            let stop = &stops.node(node_idx).stop;
            if stop.id == order_id && stop.account == account {
                return (stop.time_in_force == TimeInForce::GTD).then_some(stop.expiry_ns);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::*;
    use crate::types::Symbol;

    const TEST_SYMBOL: Symbol = Symbol(1);

    fn qty(n: u64) -> Quantity {
        Quantity(NonZeroU64::new(n).unwrap())
    }

    fn price(n: u64) -> Price {
        Price(NonZeroU64::new(n).unwrap())
    }

    /// Default test account — most orderbook tests don't care about account identity.
    const TEST_ACCOUNT: AccountId = AccountId(1);

    fn limit_order(id: u64, side: Side, p: u64, q: u64, tif: TimeInForce) -> Order {
        Order {
            id: OrderId(id),
            account: TEST_ACCOUNT,
            side,
            order_type: OrderType::Limit {
                price: price(p),
                post_only: false,
            },
            time_in_force: tif,
            quantity: qty(q),
            stp: SelfTradeProtection::Allow,
            expiry_ns: 0,
        }
    }

    fn market_order(id: u64, side: Side, q: u64, tif: TimeInForce) -> Order {
        Order {
            id: OrderId(id),
            account: TEST_ACCOUNT,
            side,
            order_type: OrderType::Market,
            time_in_force: tif,
            quantity: qty(q),
            stp: SelfTradeProtection::Allow,
            expiry_ns: 0,
        }
    }

    // -- Limit order placement --

    #[test]
    fn limit_order_rests_on_empty_book() {
        let mut book = OrderBook::new(TEST_SYMBOL);
        let mut reports = Vec::new();
        book.execute(
            limit_order(1, Side::Buy, 100, 10, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );

        assert_eq!(reports.len(), 1);
        assert!(matches!(
            reports[0],
            ExecutionReport::Placed {
                order_id: OrderId(1),
                symbol: TEST_SYMBOL,
                account: TEST_ACCOUNT,
                side: Side::Buy,
                ..
            }
        ));
        // Verify the order is resting: a matching sell should fill.
        reports.clear();
        book.execute(
            limit_order(2, Side::Sell, 100, 10, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        assert!(matches!(reports[0], ExecutionReport::Fill { .. }));
        assert!(book.is_empty());
    }

    #[test]
    fn non_crossing_limit_orders_both_rest() {
        let mut book = OrderBook::new(TEST_SYMBOL);
        let mut reports = Vec::new();

        // Bid at 100, ask at 200 — no cross.
        book.execute(
            limit_order(1, Side::Buy, 100, 10, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        book.execute(
            limit_order(2, Side::Sell, 200, 10, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );

        assert_eq!(reports.len(), 2);
        assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
        assert!(matches!(reports[1], ExecutionReport::Placed { .. }));

        // Verify both sides have liquidity.
        reports.clear();
        book.execute(
            market_order(3, Side::Sell, 10, TimeInForce::IOC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        assert!(matches!(reports[0], ExecutionReport::Fill { .. }));
        reports.clear();
        book.execute(
            market_order(4, Side::Buy, 10, TimeInForce::IOC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        assert!(matches!(reports[0], ExecutionReport::Fill { .. }));
        assert!(book.is_empty());
    }

    // -- Limit order matching --

    #[test]
    fn limit_buy_matches_resting_ask() {
        let mut book = OrderBook::new(TEST_SYMBOL);
        let mut reports = Vec::new();

        book.execute(
            limit_order(1, Side::Sell, 100, 10, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        reports.clear();

        // Buy at 100 should match the resting sell.
        book.execute(
            limit_order(2, Side::Buy, 100, 10, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );

        assert_eq!(reports.len(), 1);
        assert_eq!(
            reports[0],
            ExecutionReport::Fill {
                maker_order_id: OrderId(1),
                taker_order_id: OrderId(2),
                symbol: TEST_SYMBOL,
                maker_account: TEST_ACCOUNT,
                taker_account: TEST_ACCOUNT,
                price: price(100),
                quantity: qty(10),
                maker_fee: 0,
                taker_fee: 0,
            }
        );

        assert!(book.is_empty());
    }

    #[test]
    fn limit_buy_matches_at_better_price() {
        let mut book = OrderBook::new(TEST_SYMBOL);
        let mut reports = Vec::new();

        // Resting ask at 90.
        book.execute(
            limit_order(1, Side::Sell, 90, 10, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        reports.clear();

        // Buy limit at 100 should match at the maker's price (90).
        book.execute(
            limit_order(2, Side::Buy, 100, 10, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );

        assert_eq!(reports.len(), 1);
        assert_eq!(
            reports[0],
            ExecutionReport::Fill {
                maker_order_id: OrderId(1),
                taker_order_id: OrderId(2),
                symbol: TEST_SYMBOL,
                maker_account: TEST_ACCOUNT,
                taker_account: TEST_ACCOUNT,
                price: price(90),
                quantity: qty(10),
                maker_fee: 0,
                taker_fee: 0,
            }
        );

        assert!(book.is_empty());
    }

    #[test]
    fn partial_fill_remainder_rests() {
        let mut book = OrderBook::new(TEST_SYMBOL);
        let mut reports = Vec::new();

        book.execute(
            limit_order(1, Side::Sell, 100, 5, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        reports.clear();

        // Buy 10, only 5 available — partial fill, rest goes on book.
        book.execute(
            limit_order(2, Side::Buy, 100, 10, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );

        assert_eq!(reports.len(), 2);
        assert_eq!(
            reports[0],
            ExecutionReport::Fill {
                maker_order_id: OrderId(1),
                taker_order_id: OrderId(2),
                symbol: TEST_SYMBOL,
                maker_account: TEST_ACCOUNT,
                taker_account: TEST_ACCOUNT,
                price: price(100),
                quantity: qty(5),
                maker_fee: 0,
                taker_fee: 0,
            }
        );
        assert_eq!(
            reports[1],
            ExecutionReport::Placed {
                order_id: OrderId(2),
                symbol: TEST_SYMBOL,
                account: TEST_ACCOUNT,
                side: Side::Buy,
                price: price(100),
                quantity: qty(5),
            }
        );

        // Consume the resting remainder by selling 5 into it.
        reports.clear();
        book.execute(
            limit_order(3, Side::Sell, 100, 5, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        assert_eq!(reports.len(), 1);
        assert!(matches!(reports[0], ExecutionReport::Fill { quantity, .. } if quantity == qty(5)));
        assert!(book.is_empty());
    }

    #[test]
    fn price_time_priority() {
        let mut book = OrderBook::new(TEST_SYMBOL);
        let mut reports = Vec::new();

        // Two asks at price 100, first one should fill first.
        book.execute(
            limit_order(1, Side::Sell, 100, 5, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        book.execute(
            limit_order(2, Side::Sell, 100, 5, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        reports.clear();

        // Buy 7: should fill 5 from order 1 (first in queue), then 2 from order 2.
        book.execute(
            limit_order(3, Side::Buy, 100, 7, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );

        assert_eq!(reports.len(), 2);
        assert_eq!(
            reports[0],
            ExecutionReport::Fill {
                maker_order_id: OrderId(1),
                taker_order_id: OrderId(3),
                symbol: TEST_SYMBOL,
                maker_account: TEST_ACCOUNT,
                taker_account: TEST_ACCOUNT,
                price: price(100),
                quantity: qty(5),
                maker_fee: 0,
                taker_fee: 0,
            }
        );
        assert_eq!(
            reports[1],
            ExecutionReport::Fill {
                maker_order_id: OrderId(2),
                taker_order_id: OrderId(3),
                symbol: TEST_SYMBOL,
                maker_account: TEST_ACCOUNT,
                taker_account: TEST_ACCOUNT,
                price: price(100),
                quantity: qty(2),
                maker_fee: 0,
                taker_fee: 0,
            }
        );

        // Order 2 should still have 3 remaining on the book.
        reports.clear();
        book.execute(
            market_order(4, Side::Buy, 3, TimeInForce::IOC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        assert!(matches!(reports[0], ExecutionReport::Fill { quantity, .. } if quantity == qty(3)));
        assert!(book.is_empty());
    }

    #[test]
    fn price_priority_best_price_first() {
        let mut book = OrderBook::new(TEST_SYMBOL);
        let mut reports = Vec::new();

        // Asks at 110, then 100. Buy should hit 100 first.
        book.execute(
            limit_order(1, Side::Sell, 110, 5, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        book.execute(
            limit_order(2, Side::Sell, 100, 5, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        reports.clear();

        book.execute(
            limit_order(3, Side::Buy, 110, 3, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );

        assert_eq!(reports.len(), 1);
        assert_eq!(
            reports[0],
            ExecutionReport::Fill {
                maker_order_id: OrderId(2),
                taker_order_id: OrderId(3),
                symbol: TEST_SYMBOL,
                maker_account: TEST_ACCOUNT,
                taker_account: TEST_ACCOUNT,
                price: price(100),
                quantity: qty(3),
                maker_fee: 0,
                taker_fee: 0,
            }
        );

        // Ask at 110 (5 remaining) and bid at 100 (2 remaining from partial) should still be on book.
        reports.clear();
        book.execute(
            market_order(4, Side::Buy, 7, TimeInForce::IOC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        assert!(matches!(reports[0], ExecutionReport::Fill { quantity, .. } if quantity == qty(2)));
        assert!(matches!(reports[1], ExecutionReport::Fill { quantity, .. } if quantity == qty(5)));
        assert!(book.is_empty());
    }

    // -- Market orders --

    #[test]
    fn market_buy_fills_against_asks() {
        let mut book = OrderBook::new(TEST_SYMBOL);
        let mut reports = Vec::new();

        book.execute(
            limit_order(1, Side::Sell, 100, 10, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        reports.clear();

        book.execute(
            market_order(2, Side::Buy, 10, TimeInForce::IOC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );

        assert_eq!(reports.len(), 1);
        assert!(matches!(reports[0], ExecutionReport::Fill { .. }));
        assert!(book.is_empty());
    }

    #[test]
    fn market_order_rejected_on_empty_book() {
        let mut book = OrderBook::new(TEST_SYMBOL);
        let mut reports = Vec::new();

        book.execute(
            market_order(1, Side::Buy, 10, TimeInForce::IOC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );

        assert_eq!(reports.len(), 1);
        assert_eq!(
            reports[0],
            ExecutionReport::Rejected {
                order_id: OrderId(1),
                symbol: TEST_SYMBOL,
                account: TEST_ACCOUNT,
                reason: RejectReason::NoLiquidity,
            }
        );
    }

    #[test]
    fn market_order_partial_fill_cancels_remainder() {
        let mut book = OrderBook::new(TEST_SYMBOL);
        let mut reports = Vec::new();

        book.execute(
            limit_order(1, Side::Sell, 100, 5, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        reports.clear();

        // Market buy for 10, only 5 available.
        book.execute(
            market_order(2, Side::Buy, 10, TimeInForce::IOC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );

        assert_eq!(reports.len(), 2);
        assert!(matches!(reports[0], ExecutionReport::Fill { quantity, .. } if quantity == qty(5)));
        assert_eq!(
            reports[1],
            ExecutionReport::Cancelled {
                order_id: OrderId(2),
                symbol: TEST_SYMBOL,
                account: TEST_ACCOUNT,
                remaining_quantity: qty(5),
            }
        );
        assert!(book.is_empty());
    }

    // -- IOC --

    #[test]
    fn ioc_limit_cancels_unfilled_remainder() {
        let mut book = OrderBook::new(TEST_SYMBOL);
        let mut reports = Vec::new();

        book.execute(
            limit_order(1, Side::Sell, 100, 5, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        reports.clear();

        book.execute(
            limit_order(2, Side::Buy, 100, 10, TimeInForce::IOC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );

        assert_eq!(reports.len(), 2);
        assert!(matches!(reports[0], ExecutionReport::Fill { .. }));
        assert_eq!(
            reports[1],
            ExecutionReport::Cancelled {
                order_id: OrderId(2),
                symbol: TEST_SYMBOL,
                account: TEST_ACCOUNT,
                remaining_quantity: qty(5),
            }
        );
        assert!(book.is_empty());
    }

    // -- FOK --

    #[test]
    fn fok_rejected_when_insufficient_quantity() {
        let mut book = OrderBook::new(TEST_SYMBOL);
        let mut reports = Vec::new();

        book.execute(
            limit_order(1, Side::Sell, 100, 5, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        reports.clear();

        // FOK buy for 10, only 5 available — should reject without any fills.
        book.execute(
            limit_order(2, Side::Buy, 100, 10, TimeInForce::FOK),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );

        assert_eq!(reports.len(), 1);
        assert_eq!(
            reports[0],
            ExecutionReport::Rejected {
                order_id: OrderId(2),
                symbol: TEST_SYMBOL,
                account: TEST_ACCOUNT,
                reason: RejectReason::FOKCannotFill,
            }
        );

        // The resting ask should be untouched.
        reports.clear();
        book.execute(
            market_order(3, Side::Buy, 5, TimeInForce::IOC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        assert!(matches!(reports[0], ExecutionReport::Fill { quantity, .. } if quantity == qty(5)));
        assert!(book.is_empty());
    }

    #[test]
    fn fok_fills_entirely_when_sufficient() {
        let mut book = OrderBook::new(TEST_SYMBOL);
        let mut reports = Vec::new();

        book.execute(
            limit_order(1, Side::Sell, 100, 10, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        reports.clear();

        book.execute(
            limit_order(2, Side::Buy, 100, 10, TimeInForce::FOK),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );

        assert_eq!(reports.len(), 1);
        assert!(matches!(reports[0], ExecutionReport::Fill { .. }));
        assert!(book.is_empty());
    }

    // -- Cancel --

    #[test]
    fn cancel_resting_order() {
        let mut book = OrderBook::new(TEST_SYMBOL);
        let mut reports = Vec::new();

        book.execute(
            limit_order(1, Side::Buy, 100, 10, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        reports.clear();

        book.cancel(TEST_ACCOUNT, OrderId(1), &mut reports);

        assert_eq!(reports.len(), 1);
        assert_eq!(
            reports[0],
            ExecutionReport::Cancelled {
                order_id: OrderId(1),
                symbol: TEST_SYMBOL,
                account: TEST_ACCOUNT,
                remaining_quantity: qty(10),
            }
        );
        assert!(book.is_empty());
    }

    #[test]
    fn cancel_unknown_order_is_noop() {
        let mut book = OrderBook::new(TEST_SYMBOL);
        let mut reports = Vec::new();

        book.cancel(TEST_ACCOUNT, OrderId(999), &mut reports);

        assert!(reports.is_empty());
    }

    #[test]
    fn cancelled_order_does_not_match() {
        let mut book = OrderBook::new(TEST_SYMBOL);
        let mut reports = Vec::new();

        book.execute(
            limit_order(1, Side::Sell, 100, 10, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        book.cancel(TEST_ACCOUNT, OrderId(1), &mut reports);
        reports.clear();

        // Market buy should find no liquidity.
        book.execute(
            market_order(2, Side::Buy, 10, TimeInForce::IOC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );

        assert_eq!(reports.len(), 1);
        assert!(matches!(
            reports[0],
            ExecutionReport::Rejected {
                reason: RejectReason::NoLiquidity,
                ..
            }
        ));
    }

    // -- Intrusive-list edge cases --
    //
    // These tests cover the slab + doubly-linked list under operations that
    // exercise the prev/next splicing logic directly — middle-of-FIFO
    // cancellation, slab reuse cycles, and `replace_order` paths. The
    // higher-level matching tests above all hit head/tail patterns; without
    // these, a bug that left dangling `prev`/`next` links on cancel (or
    // failed to refresh `LevelHead.head`/`tail`/`len`) could go undetected.

    /// Cancelling a node with both prev and next neighbors must splice it
    /// cleanly so the remaining FIFO order is preserved. A bug that forgot
    /// to update either `prev.next` or `next.prev` would surface here as
    /// a wrong fill order or a panic.
    #[test]
    fn cancel_middle_of_level_preserves_fifo() {
        let mut book = OrderBook::new(TEST_SYMBOL);
        let mut reports = Vec::new();

        // Three asks at the same price — head=1, middle=2, tail=3.
        for id in 1..=3 {
            book.execute(
                limit_order(id, Side::Sell, 100, 5, TimeInForce::GTC),
                None,
                ReservationSlot::DUMMY,
                &mut reports,
            );
        }
        reports.clear();

        // Cancel the middle order. List must become 1 <-> 3 with no
        // dangling links to the freed slot.
        book.cancel(TEST_ACCOUNT, OrderId(2), &mut reports);
        assert_eq!(reports.len(), 1);
        assert!(matches!(
            reports[0],
            ExecutionReport::Cancelled {
                order_id: OrderId(2),
                ..
            }
        ));
        reports.clear();

        // A buy that exhausts both remaining makers must fill 1 first, 3 second.
        book.execute(
            limit_order(4, Side::Buy, 100, 10, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        let fill_makers: Vec<OrderId> = reports
            .iter()
            .filter_map(|r| match r {
                ExecutionReport::Fill { maker_order_id, .. } => Some(*maker_order_id),
                _ => None,
            })
            .collect();
        assert_eq!(fill_makers, vec![OrderId(1), OrderId(3)]);
        assert!(
            book.is_empty(),
            "book should be empty after exhausting both makers"
        );
    }

    /// Cancelling the head of a multi-order level promotes the next order
    /// to head and leaves prev=INVALID — verified by feeding a market buy
    /// and checking the new head fills first.
    #[test]
    fn cancel_head_promotes_next_to_head() {
        let mut book = OrderBook::new(TEST_SYMBOL);
        let mut reports = Vec::new();

        for id in 1..=3 {
            book.execute(
                limit_order(id, Side::Sell, 100, 5, TimeInForce::GTC),
                None,
                ReservationSlot::DUMMY,
                &mut reports,
            );
        }
        reports.clear();

        book.cancel(TEST_ACCOUNT, OrderId(1), &mut reports);
        reports.clear();

        // 2 should now be the head.
        book.execute(
            limit_order(4, Side::Buy, 100, 5, TimeInForce::IOC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        assert!(matches!(
            reports[0],
            ExecutionReport::Fill {
                maker_order_id: OrderId(2),
                ..
            }
        ));
    }

    /// Cancelling the tail of a multi-order level reduces tail to its
    /// predecessor; a subsequent newly-placed order at the same price
    /// must splice onto the (new) tail correctly.
    #[test]
    fn cancel_tail_then_add_keeps_fifo() {
        let mut book = OrderBook::new(TEST_SYMBOL);
        let mut reports = Vec::new();

        for id in 1..=3 {
            book.execute(
                limit_order(id, Side::Sell, 100, 5, TimeInForce::GTC),
                None,
                ReservationSlot::DUMMY,
                &mut reports,
            );
        }
        reports.clear();

        // Cancel tail (3). List is now 1 <-> 2.
        book.cancel(TEST_ACCOUNT, OrderId(3), &mut reports);
        reports.clear();

        // Place a fresh order; it should reuse the freed slab slot but
        // splice onto the new tail (2) — list becomes 1 <-> 2 <-> 4.
        book.execute(
            limit_order(4, Side::Sell, 100, 5, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        reports.clear();

        // Sweep all three: order must be 1, 2, 4.
        book.execute(
            limit_order(5, Side::Buy, 100, 15, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        let makers: Vec<OrderId> = reports
            .iter()
            .filter_map(|r| match r {
                ExecutionReport::Fill { maker_order_id, .. } => Some(*maker_order_id),
                _ => None,
            })
            .collect();
        assert_eq!(makers, vec![OrderId(1), OrderId(2), OrderId(4)]);
    }

    /// Cancelling the only order at a price must remove the whole level,
    /// not leave a zero-length entry. Verified by re-placing at that
    /// price and checking it's the new top of book.
    #[test]
    fn cancel_only_order_removes_level() {
        let mut book = OrderBook::new(TEST_SYMBOL);
        let mut reports = Vec::new();

        book.execute(
            limit_order(1, Side::Sell, 100, 5, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        book.cancel(TEST_ACCOUNT, OrderId(1), &mut reports);
        assert!(
            book.is_empty(),
            "single-order level must be removed on cancel"
        );
        assert_eq!(book.best_ask(), None);

        // Re-place at the same price; level is recreated.
        reports.clear();
        book.execute(
            limit_order(2, Side::Sell, 100, 5, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        assert_eq!(book.best_ask(), Some(price(100)));
    }

    /// Stress slab reuse: many cycles of place+cancel followed by a final
    /// batch that must still match in correct FIFO order. Catches free-list
    /// cycles, stale `prev`/`next` on reused slots, and `len` drift.
    #[test]
    fn repeated_alloc_free_preserves_book_invariants() {
        let mut book = OrderBook::new(TEST_SYMBOL);
        let mut reports = Vec::new();

        // 200 cycles of place-then-cancel at the same price churn the
        // slab through many alloc/free transitions.
        for id in 1..=200 {
            book.execute(
                limit_order(id, Side::Sell, 100, 1, TimeInForce::GTC),
                None,
                ReservationSlot::DUMMY,
                &mut reports,
            );
            book.cancel(TEST_ACCOUNT, OrderId(id), &mut reports);
        }
        assert!(
            book.is_empty(),
            "book should be empty after symmetric place/cancel"
        );
        reports.clear();

        // Final batch: 5 orders at the same price. After all the slab
        // churn, FIFO must still be respected.
        for id in 1000..=1004 {
            book.execute(
                limit_order(id, Side::Sell, 100, 1, TimeInForce::GTC),
                None,
                ReservationSlot::DUMMY,
                &mut reports,
            );
        }
        reports.clear();

        book.execute(
            limit_order(2000, Side::Buy, 100, 5, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        let makers: Vec<OrderId> = reports
            .iter()
            .filter_map(|r| match r {
                ExecutionReport::Fill { maker_order_id, .. } => Some(*maker_order_id),
                _ => None,
            })
            .collect();
        assert_eq!(
            makers,
            vec![
                OrderId(1000),
                OrderId(1001),
                OrderId(1002),
                OrderId(1003),
                OrderId(1004),
            ]
        );
        assert!(book.is_empty());
    }

    /// `replace_order` qty-increase at the same price must move the order
    /// to the back of the FIFO. Direct `OrderBook` test (the Exchange-level
    /// `cancel_replace_qty_increase_loses_priority` exercises the same
    /// behavior through more layers).
    #[test]
    fn replace_order_same_price_qty_increase_loses_priority() {
        let mut book = OrderBook::new(TEST_SYMBOL);
        let mut reports = Vec::new();

        for id in 1..=3 {
            book.execute(
                limit_order(id, Side::Sell, 100, 5, TimeInForce::GTC),
                None,
                ReservationSlot::DUMMY,
                &mut reports,
            );
        }
        reports.clear();

        // Increase order 1's qty — must drop to back of queue.
        let result = book.replace_order(TEST_ACCOUNT, OrderId(1), price(100), qty(7));
        assert!(result.is_some());

        // A buy of 5 should hit order 2 first (the new head), not order 1.
        book.execute(
            limit_order(99, Side::Buy, 100, 5, TimeInForce::IOC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        assert!(matches!(
            reports[0],
            ExecutionReport::Fill {
                maker_order_id: OrderId(2),
                ..
            }
        ));
    }

    /// `replace_order` qty-decrease keeps priority — front-of-queue order
    /// stays at the front after shrinking.
    #[test]
    fn replace_order_same_price_qty_decrease_keeps_priority() {
        let mut book = OrderBook::new(TEST_SYMBOL);
        let mut reports = Vec::new();

        for id in 1..=3 {
            book.execute(
                limit_order(id, Side::Sell, 100, 5, TimeInForce::GTC),
                None,
                ReservationSlot::DUMMY,
                &mut reports,
            );
        }
        reports.clear();

        // Decrease order 1's qty — keeps head position.
        book.replace_order(TEST_ACCOUNT, OrderId(1), price(100), qty(3));

        book.execute(
            limit_order(99, Side::Buy, 100, 3, TimeInForce::IOC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        assert!(matches!(
            reports[0],
            ExecutionReport::Fill { maker_order_id: OrderId(1), quantity, .. } if quantity == qty(3)
        ));
    }

    /// `replace_order` to a different price unlinks from the old level
    /// (removing it if empty) and splices onto the new level's tail.
    #[test]
    fn replace_order_to_different_price_relocates() {
        let mut book = OrderBook::new(TEST_SYMBOL);
        let mut reports = Vec::new();

        // One order at 100 (sole occupant), one at 110 (sole occupant).
        book.execute(
            limit_order(1, Side::Sell, 100, 5, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        book.execute(
            limit_order(2, Side::Sell, 110, 5, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        reports.clear();

        // Move order 1 from 100 → 110. The 100 level must be removed
        // (was its sole occupant); 110 must now have [2, 1] in FIFO.
        book.replace_order(TEST_ACCOUNT, OrderId(1), price(110), qty(5));
        assert_eq!(
            book.best_ask(),
            Some(price(110)),
            "after relocate, best ask must be the new price (no orphan empty level at 100)"
        );

        // Sweep both: maker order must be 2 (older at 110), then 1.
        book.execute(
            limit_order(3, Side::Buy, 110, 10, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        let makers: Vec<OrderId> = reports
            .iter()
            .filter_map(|r| match r {
                ExecutionReport::Fill { maker_order_id, .. } => Some(*maker_order_id),
                _ => None,
            })
            .collect();
        assert_eq!(makers, vec![OrderId(2), OrderId(1)]);
    }

    /// `prefault` must be a no-op on a non-empty book. `Exchange::prefault`
    /// can be called after snapshot restore has placed orders, so wiping
    /// the slab would leave `LevelHead` indices pointing at empty memory
    /// and crash the next operation that touches them.
    #[test]
    fn prefault_on_populated_book_is_noop() {
        let mut book = OrderBook::with_capacity(TEST_SYMBOL);
        let mut reports = Vec::new();
        book.execute(
            limit_order(1, Side::Sell, 100, 5, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );

        // Calling prefault now must NOT clear the slab.
        book.prefault();

        // The resting order must still match.
        reports.clear();
        book.execute(
            limit_order(2, Side::Buy, 100, 5, TimeInForce::IOC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        assert!(matches!(
            reports[0],
            ExecutionReport::Fill {
                maker_order_id: OrderId(1),
                ..
            }
        ));
    }

    /// `prefault` must be idempotent and must not corrupt subsequent
    /// book operations. Run it twice on a pre-sized book, then exercise
    /// matching to confirm the slab is in a usable state.
    #[test]
    fn prefault_is_idempotent_and_safe() {
        let mut book = OrderBook::with_capacity(TEST_SYMBOL);
        book.prefault();
        book.prefault();

        let mut reports = Vec::new();
        book.execute(
            limit_order(1, Side::Sell, 100, 5, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        book.execute(
            limit_order(2, Side::Buy, 100, 5, TimeInForce::IOC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        // Should produce one Placed and one Fill — proves the slab pages
        // are usable and the linked-list invariants survived the prefault.
        assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
        assert!(matches!(
            reports[1],
            ExecutionReport::Fill {
                maker_order_id: OrderId(1),
                taker_order_id: OrderId(2),
                ..
            }
        ));
        assert!(book.is_empty());
    }

    // -- Multi-level matching --

    #[test]
    fn market_order_sweeps_multiple_price_levels() {
        let mut book = OrderBook::new(TEST_SYMBOL);
        let mut reports = Vec::new();

        book.execute(
            limit_order(1, Side::Sell, 100, 5, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        book.execute(
            limit_order(2, Side::Sell, 101, 5, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        book.execute(
            limit_order(3, Side::Sell, 102, 5, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        reports.clear();

        book.execute(
            market_order(4, Side::Buy, 12, TimeInForce::IOC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );

        // Should fill 5@100, 5@101, 2@102.
        assert_eq!(reports.len(), 3);
        assert_eq!(
            reports[0],
            ExecutionReport::Fill {
                maker_order_id: OrderId(1),
                taker_order_id: OrderId(4),
                symbol: TEST_SYMBOL,
                maker_account: TEST_ACCOUNT,
                taker_account: TEST_ACCOUNT,
                price: price(100),
                quantity: qty(5),
                maker_fee: 0,
                taker_fee: 0,
            }
        );
        assert_eq!(
            reports[1],
            ExecutionReport::Fill {
                maker_order_id: OrderId(2),
                taker_order_id: OrderId(4),
                symbol: TEST_SYMBOL,
                maker_account: TEST_ACCOUNT,
                taker_account: TEST_ACCOUNT,
                price: price(101),
                quantity: qty(5),
                maker_fee: 0,
                taker_fee: 0,
            }
        );
        assert_eq!(
            reports[2],
            ExecutionReport::Fill {
                maker_order_id: OrderId(3),
                taker_order_id: OrderId(4),
                symbol: TEST_SYMBOL,
                maker_account: TEST_ACCOUNT,
                taker_account: TEST_ACCOUNT,
                price: price(102),
                quantity: qty(2),
                maker_fee: 0,
                taker_fee: 0,
            }
        );

        // Order 3 still has 3 remaining on the book.
        reports.clear();
        book.execute(
            market_order(5, Side::Buy, 3, TimeInForce::IOC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        assert!(matches!(reports[0], ExecutionReport::Fill { quantity, .. } if quantity == qty(3)));
        assert!(book.is_empty());
    }

    // -- Sell-side matching --

    #[test]
    fn limit_sell_matches_resting_bid() {
        let mut book = OrderBook::new(TEST_SYMBOL);
        let mut reports = Vec::new();

        book.execute(
            limit_order(1, Side::Buy, 100, 10, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        reports.clear();

        book.execute(
            limit_order(2, Side::Sell, 100, 10, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );

        assert_eq!(reports.len(), 1);
        assert_eq!(
            reports[0],
            ExecutionReport::Fill {
                maker_order_id: OrderId(1),
                taker_order_id: OrderId(2),
                symbol: TEST_SYMBOL,
                maker_account: TEST_ACCOUNT,
                taker_account: TEST_ACCOUNT,
                price: price(100),
                quantity: qty(10),
                maker_fee: 0,
                taker_fee: 0,
            }
        );
        assert!(book.is_empty());
    }

    #[test]
    fn sell_matches_best_bid_first() {
        let mut book = OrderBook::new(TEST_SYMBOL);
        let mut reports = Vec::new();

        // Bids at 90 and 100. Sell should hit 100 first.
        book.execute(
            limit_order(1, Side::Buy, 90, 5, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        book.execute(
            limit_order(2, Side::Buy, 100, 5, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        reports.clear();

        book.execute(
            limit_order(3, Side::Sell, 90, 3, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );

        assert_eq!(reports.len(), 1);
        assert_eq!(
            reports[0],
            ExecutionReport::Fill {
                maker_order_id: OrderId(2),
                taker_order_id: OrderId(3),
                symbol: TEST_SYMBOL,
                maker_account: TEST_ACCOUNT,
                taker_account: TEST_ACCOUNT,
                price: price(100),
                quantity: qty(3),
                maker_fee: 0,
                taker_fee: 0,
            }
        );

        // Bid at 90 (5) and bid at 100 (2 remaining) should still be on book.
        reports.clear();
        book.execute(
            market_order(4, Side::Sell, 7, TimeInForce::IOC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        assert!(matches!(reports[0], ExecutionReport::Fill { quantity, .. } if quantity == qty(2)));
        assert!(matches!(reports[1], ExecutionReport::Fill { quantity, .. } if quantity == qty(5)));
        assert!(book.is_empty());
    }

    // -- Stop orders --

    fn stop_order(id: u64, side: Side, trigger: u64, q: u64, tif: TimeInForce) -> Order {
        Order {
            id: OrderId(id),
            account: TEST_ACCOUNT,
            side,
            order_type: OrderType::Stop {
                trigger_price: price(trigger),
            },
            time_in_force: tif,
            quantity: qty(q),
            stp: SelfTradeProtection::Allow,
            expiry_ns: 0,
        }
    }

    fn stop_limit_order(
        id: u64,
        side: Side,
        trigger: u64,
        limit: u64,
        q: u64,
        tif: TimeInForce,
    ) -> Order {
        Order {
            id: OrderId(id),
            account: TEST_ACCOUNT,
            side,
            order_type: OrderType::StopLimit {
                trigger_price: price(trigger),
                limit_price: price(limit),
            },
            time_in_force: tif,
            quantity: qty(q),
            stp: SelfTradeProtection::Allow,
            expiry_ns: 0,
        }
    }

    #[test]
    fn stop_buy_triggers_on_trade_at_trigger_price() {
        let mut book = OrderBook::new(TEST_SYMBOL);
        let mut reports = Vec::new();

        // Place a resting ask at 100 and a stop buy that triggers at 100.
        book.execute(
            limit_order(1, Side::Sell, 100, 10, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        book.execute(
            stop_order(2, Side::Buy, 100, 5, TimeInForce::IOC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        reports.clear();

        // A trade at 100 should trigger the stop buy.
        book.execute(
            limit_order(3, Side::Buy, 100, 5, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );

        // Order 3 fills against order 1 (5@100), then stop triggers and fills (5@100).
        assert!(matches!(
            reports[0],
            ExecutionReport::Fill {
                taker_order_id: OrderId(3),
                ..
            }
        ));
        assert_eq!(
            reports[1],
            ExecutionReport::Triggered {
                order_id: OrderId(2),
                symbol: TEST_SYMBOL,
                account: TEST_ACCOUNT,
                trigger_price: price(100),
            }
        );
        assert!(matches!(
            reports[2],
            ExecutionReport::Fill {
                taker_order_id: OrderId(2),
                ..
            }
        ));
        assert!(book.is_empty());
    }

    #[test]
    fn stop_sell_triggers_on_trade_at_trigger_price() {
        let mut book = OrderBook::new(TEST_SYMBOL);
        let mut reports = Vec::new();

        // Place a resting bid at 100 and a stop sell that triggers at 100.
        book.execute(
            limit_order(1, Side::Buy, 100, 10, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        book.execute(
            stop_order(2, Side::Sell, 100, 5, TimeInForce::IOC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        reports.clear();

        // A trade at 100 should trigger the stop sell.
        book.execute(
            limit_order(3, Side::Sell, 100, 5, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );

        assert!(matches!(
            reports[0],
            ExecutionReport::Fill {
                taker_order_id: OrderId(3),
                ..
            }
        ));
        assert_eq!(
            reports[1],
            ExecutionReport::Triggered {
                order_id: OrderId(2),
                symbol: TEST_SYMBOL,
                account: TEST_ACCOUNT,
                trigger_price: price(100),
            }
        );
        assert!(matches!(
            reports[2],
            ExecutionReport::Fill {
                taker_order_id: OrderId(2),
                ..
            }
        ));
        assert!(book.is_empty());
    }

    #[test]
    fn stop_buy_does_not_trigger_below_price() {
        let mut book = OrderBook::new(TEST_SYMBOL);
        let mut reports = Vec::new();

        // Stop buy at 110, but trade happens at 100.
        book.execute(
            limit_order(1, Side::Sell, 100, 10, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        book.execute(
            stop_order(2, Side::Buy, 110, 5, TimeInForce::IOC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        reports.clear();

        book.execute(
            limit_order(3, Side::Buy, 100, 5, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );

        // Only the limit order fills, stop is NOT triggered.
        assert_eq!(reports.len(), 1);
        assert!(matches!(
            reports[0],
            ExecutionReport::Fill {
                taker_order_id: OrderId(3),
                ..
            }
        ));
        // Stop and remaining ask still on book.
        assert!(!book.is_empty());
    }

    /// Stop buy triggers when trade price is ABOVE the trigger price
    /// (not just at it). Trigger condition: trade_price >= trigger_price.
    #[test]
    fn stop_buy_triggers_above_trigger_price() {
        let mut book = OrderBook::new(TEST_SYMBOL);
        let mut reports = Vec::new();

        // Resting asks at 100 and 110.
        book.execute(
            limit_order(1, Side::Sell, 100, 5, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        book.execute(
            limit_order(2, Side::Sell, 110, 10, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        // Stop buy triggers at 95 — should fire when trade happens at 100.
        book.execute(
            stop_order(3, Side::Buy, 95, 5, TimeInForce::IOC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        reports.clear();

        // Trade at 100 (above trigger 95) → stop should trigger.
        book.execute(
            limit_order(4, Side::Buy, 100, 5, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );

        assert!(
            reports.iter().any(|r| matches!(
                r,
                ExecutionReport::Triggered {
                    order_id: OrderId(3),
                    ..
                }
            )),
            "stop buy should trigger when trade price (100) > trigger price (95)"
        );
    }

    /// Stop sell triggers when trade price is BELOW the trigger price
    /// (not just at it). Trigger condition: trade_price <= trigger_price.
    #[test]
    fn stop_sell_triggers_below_trigger_price() {
        let mut book = OrderBook::new(TEST_SYMBOL);
        let mut reports = Vec::new();

        // Resting bids at 100 and 90.
        book.execute(
            limit_order(1, Side::Buy, 100, 5, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        book.execute(
            limit_order(2, Side::Buy, 90, 10, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        // Stop sell triggers at 105 — should fire when trade happens at 100.
        book.execute(
            stop_order(3, Side::Sell, 105, 5, TimeInForce::IOC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        reports.clear();

        // Trade at 100 (below trigger 105) → stop should trigger.
        book.execute(
            limit_order(4, Side::Sell, 100, 5, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );

        assert!(
            reports.iter().any(|r| matches!(
                r,
                ExecutionReport::Triggered {
                    order_id: OrderId(3),
                    ..
                }
            )),
            "stop sell should trigger when trade price (100) < trigger price (105)"
        );
    }

    /// Stop sell does NOT trigger when trade price is above trigger price.
    #[test]
    fn stop_sell_does_not_trigger_above_price() {
        let mut book = OrderBook::new(TEST_SYMBOL);
        let mut reports = Vec::new();

        // Resting bid at 100 and stop sell at trigger 90.
        book.execute(
            limit_order(1, Side::Buy, 100, 10, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        book.execute(
            stop_order(2, Side::Sell, 90, 5, TimeInForce::IOC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        reports.clear();

        // Trade at 100 (above trigger 90) → stop sell should NOT trigger.
        book.execute(
            limit_order(3, Side::Sell, 100, 5, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );

        assert_eq!(reports.len(), 1, "only the limit fill, no trigger");
        assert!(matches!(
            reports[0],
            ExecutionReport::Fill {
                taker_order_id: OrderId(3),
                ..
            }
        ));
    }

    #[test]
    fn stop_limit_triggers_and_rests() {
        let mut book = OrderBook::new(TEST_SYMBOL);
        let mut reports = Vec::new();

        // Resting ask at 100, stop-limit buy: trigger at 100, limit at 95.
        book.execute(
            limit_order(1, Side::Sell, 100, 10, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        book.execute(
            stop_limit_order(2, Side::Buy, 100, 95, 5, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        reports.clear();

        // Trade at 100 triggers the stop, but limit price 95 < ask 100, so it rests.
        book.execute(
            limit_order(3, Side::Buy, 100, 5, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );

        assert!(matches!(
            reports[0],
            ExecutionReport::Fill {
                taker_order_id: OrderId(3),
                ..
            }
        ));
        assert_eq!(
            reports[1],
            ExecutionReport::Triggered {
                order_id: OrderId(2),
                symbol: TEST_SYMBOL,
                account: TEST_ACCOUNT,
                trigger_price: price(100),
            }
        );
        // The stop-limit becomes a limit buy at 95, which rests (no asks at 95).
        assert!(matches!(
            reports[2],
            ExecutionReport::Placed {
                order_id: OrderId(2),
                symbol: TEST_SYMBOL,
                account: TEST_ACCOUNT,
                side: Side::Buy,
                ..
            }
        ));
    }

    #[test]
    /// Cancelling a stop in the middle of a multi-stop trigger level
    /// must splice cleanly. Mirrors `cancel_middle_of_level_preserves_fifo`
    /// for the stop side.
    fn stop_cancel_middle_of_level_preserves_fifo() {
        let mut book = OrderBook::new(TEST_SYMBOL);
        let mut reports = Vec::new();

        // Three buy stops at the same trigger price — head=1, mid=2, tail=3.
        for id in 1..=3 {
            book.execute(
                stop_order(id, Side::Buy, 100, 5, TimeInForce::GTC),
                None,
                ReservationSlot::DUMMY,
                &mut reports,
            );
        }
        reports.clear();

        // Cancel the middle stop. Subsequent trigger must fire 1 then 3.
        book.cancel(TEST_ACCOUNT, OrderId(2), &mut reports);
        assert!(matches!(
            reports[0],
            ExecutionReport::Cancelled {
                order_id: OrderId(2),
                ..
            }
        ));
        reports.clear();

        // Place a sell at 100 then a buy that crosses it to drive a trade
        // at 100, which triggers both buy stops. They convert to market
        // orders but the book is empty post-trade, so they reject.
        // What matters here is the *Triggered* report order: 1 then 3.
        book.execute(
            limit_order(10, Side::Sell, 100, 1, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        book.execute(
            limit_order(11, Side::Buy, 100, 1, TimeInForce::IOC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        let triggered: Vec<OrderId> = reports
            .iter()
            .filter_map(|r| match r {
                ExecutionReport::Triggered { order_id, .. } => Some(*order_id),
                _ => None,
            })
            .collect();
        assert_eq!(triggered, vec![OrderId(1), OrderId(3)]);
    }

    /// Stops at the same trigger price must fire FIFO. After 5 stops
    /// are placed and a trade hits the trigger, the Triggered reports
    /// must come out in placement order.
    #[test]
    fn stops_fire_in_fifo_order_at_same_trigger() {
        let mut book = OrderBook::new(TEST_SYMBOL);
        let mut reports = Vec::new();

        for id in 1..=5 {
            book.execute(
                stop_order(id, Side::Buy, 100, 1, TimeInForce::GTC),
                None,
                ReservationSlot::DUMMY,
                &mut reports,
            );
        }
        reports.clear();

        // Drive a trade at 100 to fire all five.
        book.execute(
            limit_order(10, Side::Sell, 100, 1, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        book.execute(
            limit_order(11, Side::Buy, 100, 1, TimeInForce::IOC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        let triggered: Vec<OrderId> = reports
            .iter()
            .filter_map(|r| match r {
                ExecutionReport::Triggered { order_id, .. } => Some(*order_id),
                _ => None,
            })
            .collect();
        assert_eq!(
            triggered,
            vec![OrderId(1), OrderId(2), OrderId(3), OrderId(4), OrderId(5)]
        );
    }

    /// Stress slab reuse on the stop side. 200 cycles of place+cancel
    /// followed by a final batch that must trigger in correct FIFO order.
    /// Catches stale `prev`/`next` on reused stop slots.
    #[test]
    fn stop_repeated_alloc_free_preserves_fifo() {
        let mut book = OrderBook::new(TEST_SYMBOL);
        let mut reports = Vec::new();

        for id in 1..=200 {
            book.execute(
                stop_order(id, Side::Buy, 100, 1, TimeInForce::GTC),
                None,
                ReservationSlot::DUMMY,
                &mut reports,
            );
            book.cancel(TEST_ACCOUNT, OrderId(id), &mut reports);
        }
        reports.clear();

        // Final batch.
        for id in 1000..=1003 {
            book.execute(
                stop_order(id, Side::Buy, 100, 1, TimeInForce::GTC),
                None,
                ReservationSlot::DUMMY,
                &mut reports,
            );
        }
        reports.clear();

        // Drive a trade to fire them.
        book.execute(
            limit_order(10, Side::Sell, 100, 1, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        book.execute(
            limit_order(11, Side::Buy, 100, 1, TimeInForce::IOC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        let triggered: Vec<OrderId> = reports
            .iter()
            .filter_map(|r| match r {
                ExecutionReport::Triggered { order_id, .. } => Some(*order_id),
                _ => None,
            })
            .collect();
        assert_eq!(
            triggered,
            vec![OrderId(1000), OrderId(1001), OrderId(1002), OrderId(1003)]
        );
    }

    #[test]
    fn cancel_pending_stop_order() {
        let mut book = OrderBook::new(TEST_SYMBOL);
        let mut reports = Vec::new();

        book.execute(
            stop_order(1, Side::Buy, 100, 10, TimeInForce::IOC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        reports.clear();

        book.cancel(TEST_ACCOUNT, OrderId(1), &mut reports);

        assert_eq!(reports.len(), 1);
        assert_eq!(
            reports[0],
            ExecutionReport::Cancelled {
                order_id: OrderId(1),
                symbol: TEST_SYMBOL,
                account: TEST_ACCOUNT,
                remaining_quantity: qty(10),
            }
        );
        assert!(book.is_empty());
    }

    #[test]
    fn cancelled_stop_does_not_trigger() {
        let mut book = OrderBook::new(TEST_SYMBOL);
        let mut reports = Vec::new();

        book.execute(
            limit_order(1, Side::Sell, 100, 10, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        book.execute(
            stop_order(2, Side::Buy, 100, 5, TimeInForce::IOC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );
        book.cancel(TEST_ACCOUNT, OrderId(2), &mut reports);
        reports.clear();

        // Trade at 100 — cancelled stop should not trigger.
        book.execute(
            limit_order(3, Side::Buy, 100, 10, TimeInForce::GTC),
            None,
            ReservationSlot::DUMMY,
            &mut reports,
        );

        assert_eq!(reports.len(), 1);
        assert!(matches!(reports[0], ExecutionReport::Fill { .. }));
        assert!(book.is_empty());
    }
}
