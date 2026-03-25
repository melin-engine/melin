//! Order book with price-time priority matching.
//!
//! Bids are stored in descending price order, asks in ascending.
//! Within a price level, orders are matched FIFO.

use std::collections::{BTreeMap, VecDeque};

use std::num::NonZeroU64;

use crate::types::{
    AccountId, ExecutionReport, HashMap, Order, OrderId, OrderType, Price, Quantity, RejectReason,
    SelfTradeProtection, Side, TimeInForce,
};

/// A resting order on the book (the unfilled portion of a limit order).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RestingOrder {
    id: OrderId,
    account: AccountId,
    remaining: Quantity,
}

/// A pending stop order waiting to be triggered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PendingStop {
    id: OrderId,
    account: AccountId,
    side: Side,
    trigger_price: Price,
    quantity: Quantity,
    time_in_force: TimeInForce,
    /// If `Some`, becomes a limit order at this price when triggered.
    /// If `None`, becomes a market order.
    limit_price: Option<Price>,
    /// Maximum quote currency cost for buy-side market/stop-market orders.
    /// Prevents fills from exceeding the reserved amount. `None` for sell-side
    /// orders and limit/stop-limit buys (where cost is bounded by price × qty).
    quote_budget: Option<u64>,
    /// Self-trade prevention mode, preserved from the original order.
    stp: SelfTradeProtection,
}

/// One side of the order book (either all bids or all asks).
///
/// Uses a sorted `Vec` instead of `BTreeMap` for price levels. Typical books
/// have 5-20 active levels per side — at ~32 bytes per entry (Price + VecDeque
/// header), the entire side fits in 1-3 L1 cache lines. Binary search gives
/// O(log n) lookup with zero pointer chasing; insert/remove shift ~160-640
/// bytes, which is a single memcpy in L1. BTreeMap's node-per-entry layout
/// causes cache misses on every traversal.
#[derive(Debug, Default)]
pub(crate) struct BookSide {
    /// Sorted ascending by Price. Binary search for all lookups.
    /// VecDeque at each level: FIFO queue for time priority.
    levels: Vec<(Price, VecDeque<RestingOrder>)>,
}

impl RestingOrder {
    /// Create a new resting order (used by snapshot restore).
    pub(crate) fn new(id: OrderId, account: AccountId, remaining: Quantity) -> Self {
        Self {
            id,
            account,
            remaining,
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
}

impl BookSide {
    /// Binary search for a price level. Returns `Ok(index)` if found,
    /// `Err(index)` for the insertion point.
    #[inline]
    fn search(&self, price: Price) -> Result<usize, usize> {
        self.levels.binary_search_by_key(&price, |(p, _)| *p)
    }

    /// Iterate over price levels in ascending order.
    pub(crate) fn levels_iter(&self) -> impl Iterator<Item = (&Price, &VecDeque<RestingOrder>)> {
        self.levels.iter().map(|(p, q)| (p, q))
    }

    /// Reconstruct a BookSide from pre-sorted levels (used by snapshot restore).
    /// Input must be sorted ascending by Price.
    pub(crate) fn from_levels(levels: Vec<(Price, VecDeque<RestingOrder>)>) -> Self {
        Self { levels }
    }

    fn add(&mut self, price: Price, order: RestingOrder) {
        match self.search(price) {
            Ok(idx) => self.levels[idx].1.push_back(order),
            Err(idx) => {
                let mut queue = VecDeque::new();
                queue.push_back(order);
                self.levels.insert(idx, (price, queue));
            }
        }
    }

    /// Get a mutable reference to the queue at a price level.
    fn get_mut(&mut self, price: Price) -> Option<&mut VecDeque<RestingOrder>> {
        match self.search(price) {
            Ok(idx) => Some(&mut self.levels[idx].1),
            Err(_) => None,
        }
    }

    /// Get an immutable reference to the queue at a price level.
    fn get(&self, price: Price) -> Option<&VecDeque<RestingOrder>> {
        match self.search(price) {
            Ok(idx) => Some(&self.levels[idx].1),
            Err(_) => None,
        }
    }

    /// Remove the price level entirely.
    fn remove_level(&mut self, price: Price) {
        if let Ok(idx) = self.search(price) {
            self.levels.remove(idx);
        }
    }

    /// Remove a resting order and return both its account and remaining quantity.
    /// Used by cancel paths that need the account for `ExecutionReport::Cancelled`.
    fn remove_with_account(
        &mut self,
        price: Price,
        account: AccountId,
        order_id: OrderId,
    ) -> Option<(AccountId, Quantity)> {
        let idx = self.search(price).ok()?;
        let level = &mut self.levels[idx].1;
        // Match on both account and order_id — two accounts may have the
        // same OrderId resting at the same price level.
        let pos = level
            .iter()
            .position(|o| o.id == order_id && o.account == account)?;
        let order = level.remove(pos).expect("position was valid");
        if level.is_empty() {
            self.levels.remove(idx);
        }
        Some((order.account, order.remaining))
    }

    fn is_empty(&self) -> bool {
        self.levels.is_empty()
    }

    /// Total available quantity at prices that would match the given limit price.
    /// If `exclude_account` is `Some`, orders from that account are skipped
    /// (used for FOK pre-check with STP CancelNewest/CancelBoth).
    fn available_quantity(
        &self,
        side: Side,
        limit: Option<Price>,
        exclude_account: Option<AccountId>,
    ) -> u64 {
        let mut total: u64 = 0;
        match side {
            Side::Buy => {
                // Bids: iterate from highest price downward
                for (price, level) in self.levels.iter().rev() {
                    if let Some(limit) = limit
                        && *price < limit
                    {
                        break;
                    }
                    for order in level {
                        if exclude_account.is_some_and(|acct| acct == order.account) {
                            continue;
                        }
                        total = total.saturating_add(order.remaining.get());
                    }
                }
            }
            Side::Sell => {
                // Asks: iterate from lowest price upward
                for (price, level) in &self.levels {
                    if let Some(limit) = limit
                        && *price > limit
                    {
                        break;
                    }
                    for order in level {
                        if exclude_account.is_some_and(|acct| acct == order.account) {
                            continue;
                        }
                        total = total.saturating_add(order.remaining.get());
                    }
                }
            }
        }
        total
    }
}

/// Central limit order book for a single instrument.
#[derive(Debug)]
pub struct OrderBook {
    bids: BookSide,
    asks: BookSide,
    /// HashMap: O(1) amortized lookup for cancel operations. Maps
    /// (account, order_id) to its location (side, price) so we don't
    /// need to scan the book. Keyed by (AccountId, OrderId) to eliminate
    /// cross-account collisions — different accounts can independently
    /// use the same OrderId without index conflicts.
    order_index: HashMap<(AccountId, OrderId), (Side, Price)>,
    /// BTreeMap keyed by trigger price so we can efficiently find all stops
    /// that should fire at a given trade price. Stop buys trigger when price
    /// rises (iterate from lowest trigger up), stop sells when price falls
    /// (iterate from highest trigger down).
    stop_buys: BTreeMap<Price, Vec<PendingStop>>,
    stop_sells: BTreeMap<Price, Vec<PendingStop>>,
    /// Tracks which order IDs are pending stops, for cancel support.
    /// Keyed by (AccountId, OrderId) to match order_index and eliminate
    /// cross-account collisions.
    stop_index: HashMap<(AccountId, OrderId), (Side, Price)>,
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
}

impl Default for OrderBook {
    fn default() -> Self {
        Self::new()
    }
}

impl OrderBook {
    pub fn new() -> Self {
        Self {
            bids: BookSide::default(),
            asks: BookSide::default(),
            order_index: HashMap::default(),
            stop_buys: BTreeMap::new(),
            stop_sells: BTreeMap::new(),
            stop_index: HashMap::default(),
            last_trade_price: None,
            trigger_price_buf: Vec::new(),
            triggered_buf: Vec::new(),
            match_price_buf: Vec::new(),
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
    pub fn with_capacity() -> Self {
        Self {
            bids: BookSide::default(),
            asks: BookSide::default(),
            // One entry per resting order for O(1) cancel lookups.
            // 4096 slots ≈ 128 KB (key 12 B + value 16 B + control 1 B per
            // slot) — fits in L2 cache for fast probes. Typical book depth
            // is 100-2000 orders; resize cost at 4K is ~5 µs.
            order_index: HashMap::with_capacity_and_hasher(4_096, Default::default()),
            // BTreeMap is node-allocated — no resize spikes.
            stop_buys: BTreeMap::new(),
            stop_sells: BTreeMap::new(),
            stop_index: HashMap::with_capacity_and_hasher(1_024, Default::default()),
            last_trade_price: None,
            trigger_price_buf: Vec::with_capacity(64),
            triggered_buf: Vec::with_capacity(64),
            // Typical aggressive order sweeps a handful of price levels.
            match_price_buf: Vec::with_capacity(64),
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
                ),
            );
        }
        self.stop_index.clear();
    }

    /// Reconstruct an OrderBook from pre-built parts (used by snapshot restore).
    pub(crate) fn from_parts(
        bids: BookSide,
        asks: BookSide,
        order_index: HashMap<(AccountId, OrderId), (Side, Price)>,
        stop_buys: BTreeMap<Price, Vec<PendingStop>>,
        stop_sells: BTreeMap<Price, Vec<PendingStop>>,
        stop_index: HashMap<(AccountId, OrderId), (Side, Price)>,
        last_trade_price: Option<Price>,
    ) -> Self {
        Self {
            bids,
            asks,
            order_index,
            stop_buys,
            stop_sells,
            stop_index,
            last_trade_price,
            trigger_price_buf: Vec::new(),
            triggered_buf: Vec::new(),
            match_price_buf: Vec::new(),
        }
    }

    // --- Snapshot accessors ---

    pub(crate) fn bids(&self) -> &BookSide {
        &self.bids
    }

    pub(crate) fn asks(&self) -> &BookSide {
        &self.asks
    }

    pub(crate) fn stop_buys(&self) -> &BTreeMap<Price, Vec<PendingStop>> {
        &self.stop_buys
    }

    pub(crate) fn stop_sells(&self) -> &BTreeMap<Price, Vec<PendingStop>> {
        &self.stop_sells
    }

    pub(crate) fn last_trade_price(&self) -> Option<Price> {
        self.last_trade_price
    }

    /// Snapshot the order index as a Vec for serialization.
    /// Serialized as (order_id, account, side, price) for wire compatibility.
    pub(crate) fn snapshot_order_index(&self) -> Vec<(OrderId, AccountId, Side, Price)> {
        self.order_index
            .iter()
            .map(|(&(account, id), &(side, price))| (id, account, side, price))
            .collect()
    }

    /// Snapshot the stop index as a Vec for serialization.
    /// Serialized as (order_id, account, side, price) for wire compatibility.
    pub(crate) fn snapshot_stop_index(&self) -> Vec<(OrderId, AccountId, Side, Price)> {
        self.stop_index
            .iter()
            .map(|(&(account, id), &(side, price))| (id, account, side, price))
            .collect()
    }

    /// Check if a resting order with the given (account, order_id) exists on the book.
    pub(crate) fn has_order(&self, account: AccountId, id: OrderId) -> bool {
        self.order_index.contains_key(&(account, id))
    }

    /// Check if a pending stop with the given (account, order_id) exists on the book.
    pub(crate) fn has_stop(&self, account: AccountId, id: OrderId) -> bool {
        self.stop_index.contains_key(&(account, id))
    }

    /// Look up a resting order's location from the index: (side, price).
    /// O(1) HashMap lookup — no VecDeque scan. Returns `None` if the order is
    /// not on the book.
    pub(crate) fn peek_order_location(
        &self,
        account: AccountId,
        order_id: OrderId,
    ) -> Option<(Side, Price)> {
        self.order_index.get(&(account, order_id)).copied()
    }

    /// Look up a resting order's current state: (side, price, remaining).
    /// Returns `None` if the order is not on the book.
    /// NOTE: This performs an O(n) VecDeque scan to find `remaining`. If you
    /// only need side/price, use `peek_order_location` instead.
    #[allow(dead_code)]
    pub(crate) fn get_resting_order(
        &self,
        account: AccountId,
        order_id: OrderId,
    ) -> Option<(Side, Price, Quantity)> {
        let &(side, price) = self.order_index.get(&(account, order_id))?;
        let book_side = match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
        };
        let level = book_side.get(price)?;
        let order = level
            .iter()
            .find(|o| o.id == order_id && o.account == account)?;
        Some((side, price, order.remaining))
    }

    /// Best bid price (highest), or `None` if the bid side is empty.
    pub(crate) fn best_bid(&self) -> Option<Price> {
        self.bids.levels.last().map(|(p, _)| *p)
    }

    /// Best ask price (lowest), or `None` if the ask side is empty.
    pub(crate) fn best_ask(&self) -> Option<Price> {
        self.asks.levels.first().map(|(p, _)| *p)
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
        let &(side, old_price) = self.order_index.get(&(account, order_id))?;
        let book_side = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };

        if old_price == new_price {
            // Same price level — check if we can keep time priority.
            let level = book_side.get_mut(old_price)?;
            let pos = level
                .iter()
                .position(|o| o.id == order_id && o.account == account)?;
            let old_remaining = level[pos].remaining;

            if new_quantity <= old_remaining {
                // Qty decrease (or same) → in-place update, keep priority.
                level[pos].remaining = new_quantity;
            } else {
                // Qty increase → remove and push to back (lose priority).
                let mut order = level.remove(pos).expect("position was valid");
                order.remaining = new_quantity;
                level.push_back(order);
            }
            Some((old_price, old_remaining))
        } else {
            // Price change → remove from old level, add to new level.
            // Manipulate the VecDeque directly to preserve the RestingOrder
            // (including account), since BookSide::remove only returns Quantity.
            let old_level = book_side.get_mut(old_price)?;
            let pos = old_level
                .iter()
                .position(|o| o.id == order_id && o.account == account)?;
            let mut order = old_level.remove(pos).expect("position was valid");
            let old_remaining = order.remaining;
            order.remaining = new_quantity;

            if old_level.is_empty() {
                book_side.remove_level(old_price);
            }

            // Add at back of new price level (loses time priority).
            book_side.add(new_price, order);

            // Update the order index to reflect the new price.
            self.order_index
                .insert((account, order_id), (side, new_price));

            Some((old_price, old_remaining))
        }
    }

    /// Process an incoming order, appending execution reports to `reports`.
    ///
    /// `quote_budget` limits the total quote currency cost for buy-side market
    /// orders (where the fill price is unknown at reservation time). Pass the
    /// reserved amount so the matching engine stops before exceeding it.
    /// `None` for sells and limit buys (cost bounded by price × quantity).
    pub fn execute(
        &mut self,
        order: Order,
        quote_budget: Option<u64>,
        reports: &mut Vec<ExecutionReport>,
    ) {
        match order.order_type {
            OrderType::Limit { price, .. } => self.execute_limit(order, price, reports),
            OrderType::Market => self.execute_market(order, quote_budget, reports),
            OrderType::Stop { trigger_price } => {
                self.add_stop(order, trigger_price, None, quote_budget);
            }
            OrderType::StopLimit {
                trigger_price,
                limit_price,
            } => {
                self.add_stop(order, trigger_price, Some(limit_price), None);
            }
        }
        self.check_triggers(reports);
    }

    /// Cancel a resting or pending stop order by (account, order_id).
    pub fn cancel(
        &mut self,
        account: AccountId,
        order_id: OrderId,
        reports: &mut Vec<ExecutionReport>,
    ) {
        // Try resting orders first.
        if let Some((side, price)) = self.order_index.remove(&(account, order_id)) {
            let book_side = match side {
                Side::Buy => &mut self.bids,
                Side::Sell => &mut self.asks,
            };
            if let Some((_acct, remaining)) =
                book_side.remove_with_account(price, account, order_id)
            {
                reports.push(ExecutionReport::Cancelled {
                    order_id,
                    account,
                    remaining_quantity: remaining,
                });
            }
            return;
        }

        // Try pending stops.
        if let Some((side, trigger_price)) = self.stop_index.remove(&(account, order_id)) {
            let stops = match side {
                Side::Buy => &mut self.stop_buys,
                Side::Sell => &mut self.stop_sells,
            };
            if let Some(level) = stops.get_mut(&trigger_price)
                && let Some(pos) = level
                    .iter()
                    .position(|s| s.id == order_id && s.account == account)
            {
                let stop = level.remove(pos);
                if level.is_empty() {
                    stops.remove(&trigger_price);
                }
                reports.push(ExecutionReport::Cancelled {
                    order_id,
                    account: stop.account,
                    remaining_quantity: stop.quantity,
                });
            }
        }
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
        // Collect matching order IDs by scanning the book sides directly.
        // We scan the price levels (not order_index) because RestingOrder
        // carries the account field we need to filter on.
        let mut to_cancel: Vec<OrderId> = Vec::new();

        for (_, queue) in &self.bids.levels {
            for order in queue {
                if order.account == account {
                    to_cancel.push(order.id);
                }
            }
        }
        for (_, queue) in &self.asks.levels {
            for order in queue {
                if order.account == account {
                    to_cancel.push(order.id);
                }
            }
        }

        // Scan pending stops.
        for stops in self.stop_buys.values() {
            for stop in stops {
                if stop.account == account {
                    to_cancel.push(stop.id);
                }
            }
        }
        for stops in self.stop_sells.values() {
            for stop in stops {
                if stop.account == account {
                    to_cancel.push(stop.id);
                }
            }
        }

        // Cancel each collected order. cancel() handles removal from
        // order_index/stop_index, BookSide levels, and report generation.
        for id in to_cancel {
            self.cancel(account, id, reports);
        }
    }

    fn execute_limit(&mut self, order: Order, price: Price, reports: &mut Vec<ExecutionReport>) {
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
                        account: order.account,
                        remaining_quantity: rem,
                    });
                } else {
                    match order.time_in_force {
                        TimeInForce::GTC => {
                            self.place_on_book(
                                order.id,
                                order.account,
                                order.side,
                                price,
                                rem,
                                reports,
                            );
                        }
                        TimeInForce::IOC | TimeInForce::FOK => {
                            reports.push(ExecutionReport::Cancelled {
                                order_id: order.id,
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
                // Buy matches against asks (lowest first)
                self.match_price_buf.extend(
                    opposite
                        .levels
                        .iter()
                        .map(|(p, _)| p)
                        .take_while(|&&p| price_limit.is_none_or(|limit| p <= limit))
                        .copied(),
                );
            }
            Side::Sell => {
                // Sell matches against bids (highest first)
                self.match_price_buf.extend(
                    opposite
                        .levels
                        .iter()
                        .rev()
                        .map(|(p, _)| p)
                        .take_while(|&&p| price_limit.is_none_or(|limit| p >= limit))
                        .copied(),
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
            let Some(level) = opposite.get_mut(price) else {
                continue;
            };

            while let Some(maker) = level.front_mut() {
                // Self-trade prevention: check if taker and maker belong to
                // the same account before generating a fill.
                if stp != SelfTradeProtection::Allow && maker.account == taker_account {
                    match stp {
                        SelfTradeProtection::Allow => unreachable!(),
                        SelfTradeProtection::CancelNewest => {
                            // Cancel the taker, leave the maker on the book.
                            stp_cancelled = true;
                            break 'outer;
                        }
                        SelfTradeProtection::CancelOldest => {
                            // Cancel the maker, continue matching the taker.
                            let cancelled_maker = level.pop_front().expect("front existed");
                            self.order_index
                                .remove(&(cancelled_maker.account, cancelled_maker.id));
                            reports.push(ExecutionReport::Cancelled {
                                order_id: cancelled_maker.id,
                                account: cancelled_maker.account,
                                remaining_quantity: cancelled_maker.remaining,
                            });
                            continue;
                        }
                        SelfTradeProtection::CancelBoth => {
                            // Cancel the maker and the taker.
                            let cancelled_maker = level.pop_front().expect("front existed");
                            self.order_index
                                .remove(&(cancelled_maker.account, cancelled_maker.id));
                            reports.push(ExecutionReport::Cancelled {
                                order_id: cancelled_maker.id,
                                account: cancelled_maker.account,
                                remaining_quantity: cancelled_maker.remaining,
                            });
                            if level.is_empty() {
                                opposite.remove_level(price);
                            }
                            return (Some(quantity), true);
                        }
                    }
                }

                let mut fill_qty = quantity.min(maker.remaining);

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
                    maker_order_id: maker.id,
                    taker_order_id: taker_id,
                    maker_account: maker.account,
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

                match maker.remaining.checked_sub(fill_qty) {
                    Some(new_remaining) => {
                        maker.remaining = new_remaining;
                    }
                    None => {
                        // Maker fully filled — remove from book.
                        let filled_maker = level.pop_front().expect("front existed");
                        self.order_index
                            .remove(&(filled_maker.account, filled_maker.id));
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
                        if level.is_empty() {
                            opposite.remove_level(price);
                        }
                        return (None, false);
                    }
                }
            }

            // Level fully consumed.
            opposite.remove_level(price);
        }

        (Some(quantity), stp_cancelled)
    }

    fn add_stop(
        &mut self,
        order: Order,
        trigger_price: Price,
        limit_price: Option<Price>,
        quote_budget: Option<u64>,
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
        };
        let stops = match order.side {
            Side::Buy => &mut self.stop_buys,
            Side::Sell => &mut self.stop_sells,
        };
        stops.entry(trigger_price).or_default().push(stop);
        self.stop_index
            .insert((order.account, order.id), (order.side, trigger_price));
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

        // Stop buys: trigger when trade price >= trigger price.
        // Collect all triggers at or below the trade price (ascending order).
        self.trigger_price_buf.clear();
        self.trigger_price_buf.extend(
            self.stop_buys
                .keys()
                .take_while(|&&p| p <= trade_price)
                .copied(),
        );

        self.triggered_buf.clear();
        for &price in &self.trigger_price_buf {
            if let Some(stops) = self.stop_buys.remove(&price) {
                for stop in &stops {
                    self.stop_index.remove(&(stop.account, stop.id));
                }
                self.triggered_buf.extend(stops);
            }
        }

        // Stop sells: trigger when trade price <= trigger price.
        // Collect all triggers at or above the trade price (descending order).
        self.trigger_price_buf.clear();
        self.trigger_price_buf.extend(
            self.stop_sells
                .keys()
                .rev()
                .take_while(|&&p| p >= trade_price)
                .copied(),
        );

        for &price in &self.trigger_price_buf {
            if let Some(stops) = self.stop_sells.remove(&price) {
                for stop in &stops {
                    self.stop_index.remove(&(stop.account, stop.id));
                }
                self.triggered_buf.extend(stops);
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
            };

            // Re-enter execute but skip check_triggers to avoid recursion —
            // triggered orders are market/limit, so they won't re-add stops.
            match order.order_type {
                OrderType::Limit { price, .. } => self.execute_limit(order, price, reports),
                OrderType::Market => {
                    self.execute_market(order, stop.quote_budget, reports);
                }
                OrderType::Stop { .. } | OrderType::StopLimit { .. } => {
                    unreachable!("triggered stops become market or limit orders")
                }
            }
        }
        self.triggered_buf = triggered;
    }

    fn place_on_book(
        &mut self,
        id: OrderId,
        account: AccountId,
        side: Side,
        price: Price,
        quantity: Quantity,
        reports: &mut Vec<ExecutionReport>,
    ) {
        let book_side = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        book_side.add(
            price,
            RestingOrder {
                id,
                account,
                remaining: quantity,
            },
        );
        self.order_index.insert((account, id), (side, price));
        reports.push(ExecutionReport::Placed {
            order_id: id,
            side,
            price,
            quantity,
        });
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.bids.is_empty()
            && self.asks.is_empty()
            && self.stop_buys.is_empty()
            && self.stop_sells.is_empty()
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
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::*;

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
        }
    }

    // -- Limit order placement --

    #[test]
    fn limit_order_rests_on_empty_book() {
        let mut book = OrderBook::new();
        let mut reports = Vec::new();
        book.execute(
            limit_order(1, Side::Buy, 100, 10, TimeInForce::GTC),
            None,
            &mut reports,
        );

        assert_eq!(reports.len(), 1);
        assert!(matches!(
            reports[0],
            ExecutionReport::Placed {
                order_id: OrderId(1),
                side: Side::Buy,
                ..
            }
        ));
        // Verify the order is resting: a matching sell should fill.
        reports.clear();
        book.execute(
            limit_order(2, Side::Sell, 100, 10, TimeInForce::GTC),
            None,
            &mut reports,
        );
        assert!(matches!(reports[0], ExecutionReport::Fill { .. }));
        assert!(book.is_empty());
    }

    #[test]
    fn non_crossing_limit_orders_both_rest() {
        let mut book = OrderBook::new();
        let mut reports = Vec::new();

        // Bid at 100, ask at 200 — no cross.
        book.execute(
            limit_order(1, Side::Buy, 100, 10, TimeInForce::GTC),
            None,
            &mut reports,
        );
        book.execute(
            limit_order(2, Side::Sell, 200, 10, TimeInForce::GTC),
            None,
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
            &mut reports,
        );
        assert!(matches!(reports[0], ExecutionReport::Fill { .. }));
        reports.clear();
        book.execute(
            market_order(4, Side::Buy, 10, TimeInForce::IOC),
            None,
            &mut reports,
        );
        assert!(matches!(reports[0], ExecutionReport::Fill { .. }));
        assert!(book.is_empty());
    }

    // -- Limit order matching --

    #[test]
    fn limit_buy_matches_resting_ask() {
        let mut book = OrderBook::new();
        let mut reports = Vec::new();

        book.execute(
            limit_order(1, Side::Sell, 100, 10, TimeInForce::GTC),
            None,
            &mut reports,
        );
        reports.clear();

        // Buy at 100 should match the resting sell.
        book.execute(
            limit_order(2, Side::Buy, 100, 10, TimeInForce::GTC),
            None,
            &mut reports,
        );

        assert_eq!(reports.len(), 1);
        assert_eq!(
            reports[0],
            ExecutionReport::Fill {
                maker_order_id: OrderId(1),
                taker_order_id: OrderId(2),
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
        let mut book = OrderBook::new();
        let mut reports = Vec::new();

        // Resting ask at 90.
        book.execute(
            limit_order(1, Side::Sell, 90, 10, TimeInForce::GTC),
            None,
            &mut reports,
        );
        reports.clear();

        // Buy limit at 100 should match at the maker's price (90).
        book.execute(
            limit_order(2, Side::Buy, 100, 10, TimeInForce::GTC),
            None,
            &mut reports,
        );

        assert_eq!(reports.len(), 1);
        assert_eq!(
            reports[0],
            ExecutionReport::Fill {
                maker_order_id: OrderId(1),
                taker_order_id: OrderId(2),
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
        let mut book = OrderBook::new();
        let mut reports = Vec::new();

        book.execute(
            limit_order(1, Side::Sell, 100, 5, TimeInForce::GTC),
            None,
            &mut reports,
        );
        reports.clear();

        // Buy 10, only 5 available — partial fill, rest goes on book.
        book.execute(
            limit_order(2, Side::Buy, 100, 10, TimeInForce::GTC),
            None,
            &mut reports,
        );

        assert_eq!(reports.len(), 2);
        assert_eq!(
            reports[0],
            ExecutionReport::Fill {
                maker_order_id: OrderId(1),
                taker_order_id: OrderId(2),
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
            &mut reports,
        );
        assert_eq!(reports.len(), 1);
        assert!(matches!(reports[0], ExecutionReport::Fill { quantity, .. } if quantity == qty(5)));
        assert!(book.is_empty());
    }

    #[test]
    fn price_time_priority() {
        let mut book = OrderBook::new();
        let mut reports = Vec::new();

        // Two asks at price 100, first one should fill first.
        book.execute(
            limit_order(1, Side::Sell, 100, 5, TimeInForce::GTC),
            None,
            &mut reports,
        );
        book.execute(
            limit_order(2, Side::Sell, 100, 5, TimeInForce::GTC),
            None,
            &mut reports,
        );
        reports.clear();

        // Buy 7: should fill 5 from order 1 (first in queue), then 2 from order 2.
        book.execute(
            limit_order(3, Side::Buy, 100, 7, TimeInForce::GTC),
            None,
            &mut reports,
        );

        assert_eq!(reports.len(), 2);
        assert_eq!(
            reports[0],
            ExecutionReport::Fill {
                maker_order_id: OrderId(1),
                taker_order_id: OrderId(3),
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
            &mut reports,
        );
        assert!(matches!(reports[0], ExecutionReport::Fill { quantity, .. } if quantity == qty(3)));
        assert!(book.is_empty());
    }

    #[test]
    fn price_priority_best_price_first() {
        let mut book = OrderBook::new();
        let mut reports = Vec::new();

        // Asks at 110, then 100. Buy should hit 100 first.
        book.execute(
            limit_order(1, Side::Sell, 110, 5, TimeInForce::GTC),
            None,
            &mut reports,
        );
        book.execute(
            limit_order(2, Side::Sell, 100, 5, TimeInForce::GTC),
            None,
            &mut reports,
        );
        reports.clear();

        book.execute(
            limit_order(3, Side::Buy, 110, 3, TimeInForce::GTC),
            None,
            &mut reports,
        );

        assert_eq!(reports.len(), 1);
        assert_eq!(
            reports[0],
            ExecutionReport::Fill {
                maker_order_id: OrderId(2),
                taker_order_id: OrderId(3),
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
            &mut reports,
        );
        assert!(matches!(reports[0], ExecutionReport::Fill { quantity, .. } if quantity == qty(2)));
        assert!(matches!(reports[1], ExecutionReport::Fill { quantity, .. } if quantity == qty(5)));
        assert!(book.is_empty());
    }

    // -- Market orders --

    #[test]
    fn market_buy_fills_against_asks() {
        let mut book = OrderBook::new();
        let mut reports = Vec::new();

        book.execute(
            limit_order(1, Side::Sell, 100, 10, TimeInForce::GTC),
            None,
            &mut reports,
        );
        reports.clear();

        book.execute(
            market_order(2, Side::Buy, 10, TimeInForce::IOC),
            None,
            &mut reports,
        );

        assert_eq!(reports.len(), 1);
        assert!(matches!(reports[0], ExecutionReport::Fill { .. }));
        assert!(book.is_empty());
    }

    #[test]
    fn market_order_rejected_on_empty_book() {
        let mut book = OrderBook::new();
        let mut reports = Vec::new();

        book.execute(
            market_order(1, Side::Buy, 10, TimeInForce::IOC),
            None,
            &mut reports,
        );

        assert_eq!(reports.len(), 1);
        assert_eq!(
            reports[0],
            ExecutionReport::Rejected {
                order_id: OrderId(1),
                account: TEST_ACCOUNT,
                reason: RejectReason::NoLiquidity,
            }
        );
    }

    #[test]
    fn market_order_partial_fill_cancels_remainder() {
        let mut book = OrderBook::new();
        let mut reports = Vec::new();

        book.execute(
            limit_order(1, Side::Sell, 100, 5, TimeInForce::GTC),
            None,
            &mut reports,
        );
        reports.clear();

        // Market buy for 10, only 5 available.
        book.execute(
            market_order(2, Side::Buy, 10, TimeInForce::IOC),
            None,
            &mut reports,
        );

        assert_eq!(reports.len(), 2);
        assert!(matches!(reports[0], ExecutionReport::Fill { quantity, .. } if quantity == qty(5)));
        assert_eq!(
            reports[1],
            ExecutionReport::Cancelled {
                order_id: OrderId(2),
                account: TEST_ACCOUNT,
                remaining_quantity: qty(5),
            }
        );
        assert!(book.is_empty());
    }

    // -- IOC --

    #[test]
    fn ioc_limit_cancels_unfilled_remainder() {
        let mut book = OrderBook::new();
        let mut reports = Vec::new();

        book.execute(
            limit_order(1, Side::Sell, 100, 5, TimeInForce::GTC),
            None,
            &mut reports,
        );
        reports.clear();

        book.execute(
            limit_order(2, Side::Buy, 100, 10, TimeInForce::IOC),
            None,
            &mut reports,
        );

        assert_eq!(reports.len(), 2);
        assert!(matches!(reports[0], ExecutionReport::Fill { .. }));
        assert_eq!(
            reports[1],
            ExecutionReport::Cancelled {
                order_id: OrderId(2),
                account: TEST_ACCOUNT,
                remaining_quantity: qty(5),
            }
        );
        assert!(book.is_empty());
    }

    // -- FOK --

    #[test]
    fn fok_rejected_when_insufficient_quantity() {
        let mut book = OrderBook::new();
        let mut reports = Vec::new();

        book.execute(
            limit_order(1, Side::Sell, 100, 5, TimeInForce::GTC),
            None,
            &mut reports,
        );
        reports.clear();

        // FOK buy for 10, only 5 available — should reject without any fills.
        book.execute(
            limit_order(2, Side::Buy, 100, 10, TimeInForce::FOK),
            None,
            &mut reports,
        );

        assert_eq!(reports.len(), 1);
        assert_eq!(
            reports[0],
            ExecutionReport::Rejected {
                order_id: OrderId(2),
                account: TEST_ACCOUNT,
                reason: RejectReason::FOKCannotFill,
            }
        );

        // The resting ask should be untouched.
        reports.clear();
        book.execute(
            market_order(3, Side::Buy, 5, TimeInForce::IOC),
            None,
            &mut reports,
        );
        assert!(matches!(reports[0], ExecutionReport::Fill { quantity, .. } if quantity == qty(5)));
        assert!(book.is_empty());
    }

    #[test]
    fn fok_fills_entirely_when_sufficient() {
        let mut book = OrderBook::new();
        let mut reports = Vec::new();

        book.execute(
            limit_order(1, Side::Sell, 100, 10, TimeInForce::GTC),
            None,
            &mut reports,
        );
        reports.clear();

        book.execute(
            limit_order(2, Side::Buy, 100, 10, TimeInForce::FOK),
            None,
            &mut reports,
        );

        assert_eq!(reports.len(), 1);
        assert!(matches!(reports[0], ExecutionReport::Fill { .. }));
        assert!(book.is_empty());
    }

    // -- Cancel --

    #[test]
    fn cancel_resting_order() {
        let mut book = OrderBook::new();
        let mut reports = Vec::new();

        book.execute(
            limit_order(1, Side::Buy, 100, 10, TimeInForce::GTC),
            None,
            &mut reports,
        );
        reports.clear();

        book.cancel(TEST_ACCOUNT, OrderId(1), &mut reports);

        assert_eq!(reports.len(), 1);
        assert_eq!(
            reports[0],
            ExecutionReport::Cancelled {
                order_id: OrderId(1),
                account: TEST_ACCOUNT,
                remaining_quantity: qty(10),
            }
        );
        assert!(book.is_empty());
    }

    #[test]
    fn cancel_unknown_order_is_noop() {
        let mut book = OrderBook::new();
        let mut reports = Vec::new();

        book.cancel(TEST_ACCOUNT, OrderId(999), &mut reports);

        assert!(reports.is_empty());
    }

    #[test]
    fn cancelled_order_does_not_match() {
        let mut book = OrderBook::new();
        let mut reports = Vec::new();

        book.execute(
            limit_order(1, Side::Sell, 100, 10, TimeInForce::GTC),
            None,
            &mut reports,
        );
        book.cancel(TEST_ACCOUNT, OrderId(1), &mut reports);
        reports.clear();

        // Market buy should find no liquidity.
        book.execute(
            market_order(2, Side::Buy, 10, TimeInForce::IOC),
            None,
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

    // -- Multi-level matching --

    #[test]
    fn market_order_sweeps_multiple_price_levels() {
        let mut book = OrderBook::new();
        let mut reports = Vec::new();

        book.execute(
            limit_order(1, Side::Sell, 100, 5, TimeInForce::GTC),
            None,
            &mut reports,
        );
        book.execute(
            limit_order(2, Side::Sell, 101, 5, TimeInForce::GTC),
            None,
            &mut reports,
        );
        book.execute(
            limit_order(3, Side::Sell, 102, 5, TimeInForce::GTC),
            None,
            &mut reports,
        );
        reports.clear();

        book.execute(
            market_order(4, Side::Buy, 12, TimeInForce::IOC),
            None,
            &mut reports,
        );

        // Should fill 5@100, 5@101, 2@102.
        assert_eq!(reports.len(), 3);
        assert_eq!(
            reports[0],
            ExecutionReport::Fill {
                maker_order_id: OrderId(1),
                taker_order_id: OrderId(4),
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
            &mut reports,
        );
        assert!(matches!(reports[0], ExecutionReport::Fill { quantity, .. } if quantity == qty(3)));
        assert!(book.is_empty());
    }

    // -- Sell-side matching --

    #[test]
    fn limit_sell_matches_resting_bid() {
        let mut book = OrderBook::new();
        let mut reports = Vec::new();

        book.execute(
            limit_order(1, Side::Buy, 100, 10, TimeInForce::GTC),
            None,
            &mut reports,
        );
        reports.clear();

        book.execute(
            limit_order(2, Side::Sell, 100, 10, TimeInForce::GTC),
            None,
            &mut reports,
        );

        assert_eq!(reports.len(), 1);
        assert_eq!(
            reports[0],
            ExecutionReport::Fill {
                maker_order_id: OrderId(1),
                taker_order_id: OrderId(2),
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
        let mut book = OrderBook::new();
        let mut reports = Vec::new();

        // Bids at 90 and 100. Sell should hit 100 first.
        book.execute(
            limit_order(1, Side::Buy, 90, 5, TimeInForce::GTC),
            None,
            &mut reports,
        );
        book.execute(
            limit_order(2, Side::Buy, 100, 5, TimeInForce::GTC),
            None,
            &mut reports,
        );
        reports.clear();

        book.execute(
            limit_order(3, Side::Sell, 90, 3, TimeInForce::GTC),
            None,
            &mut reports,
        );

        assert_eq!(reports.len(), 1);
        assert_eq!(
            reports[0],
            ExecutionReport::Fill {
                maker_order_id: OrderId(2),
                taker_order_id: OrderId(3),
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
        }
    }

    #[test]
    fn stop_buy_triggers_on_trade_at_trigger_price() {
        let mut book = OrderBook::new();
        let mut reports = Vec::new();

        // Place a resting ask at 100 and a stop buy that triggers at 100.
        book.execute(
            limit_order(1, Side::Sell, 100, 10, TimeInForce::GTC),
            None,
            &mut reports,
        );
        book.execute(
            stop_order(2, Side::Buy, 100, 5, TimeInForce::IOC),
            None,
            &mut reports,
        );
        reports.clear();

        // A trade at 100 should trigger the stop buy.
        book.execute(
            limit_order(3, Side::Buy, 100, 5, TimeInForce::GTC),
            None,
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
        let mut book = OrderBook::new();
        let mut reports = Vec::new();

        // Place a resting bid at 100 and a stop sell that triggers at 100.
        book.execute(
            limit_order(1, Side::Buy, 100, 10, TimeInForce::GTC),
            None,
            &mut reports,
        );
        book.execute(
            stop_order(2, Side::Sell, 100, 5, TimeInForce::IOC),
            None,
            &mut reports,
        );
        reports.clear();

        // A trade at 100 should trigger the stop sell.
        book.execute(
            limit_order(3, Side::Sell, 100, 5, TimeInForce::GTC),
            None,
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
        let mut book = OrderBook::new();
        let mut reports = Vec::new();

        // Stop buy at 110, but trade happens at 100.
        book.execute(
            limit_order(1, Side::Sell, 100, 10, TimeInForce::GTC),
            None,
            &mut reports,
        );
        book.execute(
            stop_order(2, Side::Buy, 110, 5, TimeInForce::IOC),
            None,
            &mut reports,
        );
        reports.clear();

        book.execute(
            limit_order(3, Side::Buy, 100, 5, TimeInForce::GTC),
            None,
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

    #[test]
    fn stop_limit_triggers_and_rests() {
        let mut book = OrderBook::new();
        let mut reports = Vec::new();

        // Resting ask at 100, stop-limit buy: trigger at 100, limit at 95.
        book.execute(
            limit_order(1, Side::Sell, 100, 10, TimeInForce::GTC),
            None,
            &mut reports,
        );
        book.execute(
            stop_limit_order(2, Side::Buy, 100, 95, 5, TimeInForce::GTC),
            None,
            &mut reports,
        );
        reports.clear();

        // Trade at 100 triggers the stop, but limit price 95 < ask 100, so it rests.
        book.execute(
            limit_order(3, Side::Buy, 100, 5, TimeInForce::GTC),
            None,
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
                trigger_price: price(100),
            }
        );
        // The stop-limit becomes a limit buy at 95, which rests (no asks at 95).
        assert!(matches!(
            reports[2],
            ExecutionReport::Placed {
                order_id: OrderId(2),
                side: Side::Buy,
                ..
            }
        ));
    }

    #[test]
    fn cancel_pending_stop_order() {
        let mut book = OrderBook::new();
        let mut reports = Vec::new();

        book.execute(
            stop_order(1, Side::Buy, 100, 10, TimeInForce::IOC),
            None,
            &mut reports,
        );
        reports.clear();

        book.cancel(TEST_ACCOUNT, OrderId(1), &mut reports);

        assert_eq!(reports.len(), 1);
        assert_eq!(
            reports[0],
            ExecutionReport::Cancelled {
                order_id: OrderId(1),
                account: TEST_ACCOUNT,
                remaining_quantity: qty(10),
            }
        );
        assert!(book.is_empty());
    }

    #[test]
    fn cancelled_stop_does_not_trigger() {
        let mut book = OrderBook::new();
        let mut reports = Vec::new();

        book.execute(
            limit_order(1, Side::Sell, 100, 10, TimeInForce::GTC),
            None,
            &mut reports,
        );
        book.execute(
            stop_order(2, Side::Buy, 100, 5, TimeInForce::IOC),
            None,
            &mut reports,
        );
        book.cancel(TEST_ACCOUNT, OrderId(2), &mut reports);
        reports.clear();

        // Trade at 100 — cancelled stop should not trigger.
        book.execute(
            limit_order(3, Side::Buy, 100, 10, TimeInForce::GTC),
            None,
            &mut reports,
        );

        assert_eq!(reports.len(), 1);
        assert!(matches!(reports[0], ExecutionReport::Fill { .. }));
        assert!(book.is_empty());
    }
}
