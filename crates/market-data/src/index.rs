//! Order index: tracks resting orders so fills and cancels can resolve
//! back to the correct price level without scanning the book.

use melin_trading::types::{AccountId, OrderId, Price, Quantity, Side, Symbol};

/// Metadata for a single resting order, stored in the index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RestingOrder {
    pub symbol: Symbol,
    pub side: Side,
    pub price: Price,
    pub remaining: Quantity,
    pub account: AccountId,
}

/// Maps `OrderId → RestingOrder` for all orders currently on the book.
///
/// HashMap (not BTreeMap): order lookups are by ID only, never range-scanned.
/// Uses the engine's HashMap (FxHash + extendible hashing) to avoid
/// rehash spikes — extendible hashing grows one bucket at a time instead
/// of rehashing the entire table at once.
pub struct OrderIndex {
    /// Maps order_id to its resting state. One entry per resting order.
    map: melin_engine::types::HashMap<OrderId, RestingOrder>,
}

impl Default for OrderIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl OrderIndex {
    pub fn new() -> Self {
        Self {
            map: melin_engine::types::HashMap::default(),
        }
    }

    /// Insert a newly placed order.
    pub fn insert(&mut self, order_id: OrderId, order: RestingOrder) {
        self.map.insert(order_id, order);
    }

    /// Look up a resting order by ID.
    pub fn get(&self, order_id: &OrderId) -> Option<&RestingOrder> {
        self.map.get(order_id)
    }

    /// Mutably look up a resting order (for updating remaining quantity).
    pub fn get_mut(&mut self, order_id: &OrderId) -> Option<&mut RestingOrder> {
        self.map.get_mut(order_id)
    }

    /// Remove an order from the index (fully filled or cancelled).
    pub fn remove(&mut self, order_id: &OrderId) -> Option<RestingOrder> {
        self.map.remove(order_id)
    }

    /// Number of resting orders currently tracked.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Whether the index is empty.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Remove all entries.
    pub fn clear(&mut self) {
        self.map.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroU64;

    fn price(n: u64) -> Price {
        Price(NonZeroU64::new(n).unwrap())
    }

    fn qty(n: u64) -> Quantity {
        Quantity(NonZeroU64::new(n).unwrap())
    }

    #[test]
    fn insert_and_lookup() {
        let mut idx = OrderIndex::new();
        let order = RestingOrder {
            symbol: Symbol(1),
            side: Side::Buy,
            price: price(100),
            remaining: qty(10),
            account: AccountId(1),
        };
        idx.insert(OrderId(1), order);
        assert_eq!(idx.len(), 1);
        assert_eq!(idx.get(&OrderId(1)), Some(&order));
        assert_eq!(idx.get(&OrderId(2)), None);
    }

    #[test]
    fn remove_returns_order() {
        let mut idx = OrderIndex::new();
        let order = RestingOrder {
            symbol: Symbol(1),
            side: Side::Sell,
            price: price(200),
            remaining: qty(5),
            account: AccountId(2),
        };
        idx.insert(OrderId(42), order);
        assert_eq!(idx.remove(&OrderId(42)), Some(order));
        assert!(idx.is_empty());
        assert_eq!(idx.remove(&OrderId(42)), None);
    }

    #[test]
    fn mutate_remaining() {
        let mut idx = OrderIndex::new();
        idx.insert(
            OrderId(1),
            RestingOrder {
                symbol: Symbol(1),
                side: Side::Buy,
                price: price(100),
                remaining: qty(10),
                account: AccountId(1),
            },
        );
        idx.get_mut(&OrderId(1)).unwrap().remaining = qty(5);
        assert_eq!(idx.get(&OrderId(1)).unwrap().remaining, qty(5));
    }
}
