//! Bounded ring buffer of recent trades per symbol.
//!
//! Keeps the last `CAPACITY` trades. Older trades are silently
//! overwritten when the ring wraps. Used by the market-data gateway
//! to serve trade history in `MarketDataSnapshotFullRefresh` and
//! incremental `MDEntryType=Trade` updates.

use melin_trading::types::{OrderId, Price, Quantity};

/// A single trade.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Trade {
    pub maker_order_id: OrderId,
    pub taker_order_id: OrderId,
    pub price: Price,
    pub quantity: Quantity,
}

/// Fixed-capacity ring buffer for trades.
///
/// VecDeque would work but over-allocates and doesn't enforce a hard cap.
/// A fixed array with a write cursor is simpler, cache-friendlier (one
/// contiguous allocation), and guarantees O(1) push with no realloc.
pub struct TradeRing {
    /// Fixed-size backing store. Trades are stored contiguously; the
    /// write cursor wraps at CAPACITY.
    buf: Box<[Option<Trade>]>,
    /// Next write position (wraps at capacity).
    head: usize,
    /// Number of trades stored (saturates at capacity).
    len: usize,
}

/// Default ring capacity: 4096 trades per symbol.
///
/// At 40 bytes per Option<Trade> slot this is ~160 KiB.
/// Configurable at construction via `with_capacity`.
const DEFAULT_CAPACITY: usize = 4096;

impl Default for TradeRing {
    fn default() -> Self {
        Self::new()
    }
}

impl TradeRing {
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buf: vec![None; capacity].into_boxed_slice(),
            head: 0,
            len: 0,
        }
    }

    /// Push a trade, overwriting the oldest if full.
    pub fn push(&mut self, trade: Trade) {
        if self.buf.is_empty() {
            return;
        }
        self.buf[self.head] = Some(trade);
        self.head = (self.head + 1) % self.buf.len();
        if self.len < self.buf.len() {
            self.len += 1;
        }
    }

    /// Number of trades currently stored.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the ring is empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Iterate trades from oldest to newest.
    pub fn iter(&self) -> impl Iterator<Item = &Trade> {
        let cap = self.buf.len();
        // If not full, oldest is at index 0; if full, oldest is at head.
        let start = if self.len < cap { 0 } else { self.head };
        (0..self.len).map(move |i| {
            let idx = (start + i) % cap;
            // Safety: all indices in [0..len) are written.
            self.buf[idx].as_ref().unwrap()
        })
    }

    /// Remove all trades.
    pub fn clear(&mut self) {
        for slot in self.buf.iter_mut() {
            *slot = None;
        }
        self.head = 0;
        self.len = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroU64;

    fn trade(id: u64, price: u64, qty: u64) -> Trade {
        Trade {
            maker_order_id: OrderId(id),
            taker_order_id: OrderId(id + 1000),
            price: Price(NonZeroU64::new(price).unwrap()),
            quantity: Quantity(NonZeroU64::new(qty).unwrap()),
        }
    }

    #[test]
    fn empty_ring() {
        let ring = TradeRing::with_capacity(4);
        assert!(ring.is_empty());
        assert_eq!(ring.len(), 0);
        assert_eq!(ring.iter().count(), 0);
    }

    #[test]
    fn push_and_iterate() {
        let mut ring = TradeRing::with_capacity(4);
        ring.push(trade(1, 100, 10));
        ring.push(trade(2, 200, 20));
        assert_eq!(ring.len(), 2);

        let trades: Vec<_> = ring.iter().collect();
        assert_eq!(trades.len(), 2);
        assert_eq!(trades[0].maker_order_id, OrderId(1));
        assert_eq!(trades[1].maker_order_id, OrderId(2));
    }

    #[test]
    fn wraps_at_capacity() {
        let mut ring = TradeRing::with_capacity(3);
        ring.push(trade(1, 100, 1));
        ring.push(trade(2, 200, 2));
        ring.push(trade(3, 300, 3));
        ring.push(trade(4, 400, 4)); // overwrites trade 1
        assert_eq!(ring.len(), 3);

        let trades: Vec<_> = ring.iter().collect();
        assert_eq!(trades[0].maker_order_id, OrderId(2)); // oldest
        assert_eq!(trades[1].maker_order_id, OrderId(3));
        assert_eq!(trades[2].maker_order_id, OrderId(4)); // newest
    }

    #[test]
    fn clear_resets() {
        let mut ring = TradeRing::with_capacity(4);
        ring.push(trade(1, 100, 1));
        ring.push(trade(2, 200, 2));
        ring.clear();
        assert!(ring.is_empty());
        assert_eq!(ring.iter().count(), 0);
    }

    #[test]
    fn double_wrap() {
        let mut ring = TradeRing::with_capacity(2);
        for i in 1..=10 {
            ring.push(trade(i, i * 100, i));
        }
        assert_eq!(ring.len(), 2);
        let trades: Vec<_> = ring.iter().collect();
        assert_eq!(trades[0].maker_order_id, OrderId(9));
        assert_eq!(trades[1].maker_order_id, OrderId(10));
    }
}
