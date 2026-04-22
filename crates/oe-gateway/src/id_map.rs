//! Bidirectional mapping between FIX ClOrdID (string) and Melin OrderId (u64).
//!
//! Melin requires strictly monotonic OrderIds per account. FIX ClOrdIDs
//! are arbitrary strings assigned by the client. The gateway generates
//! sequential u64 IDs and maintains the mapping for the session lifetime.

use std::collections::HashMap;

use melin_trading::types::OrderId;

/// Per-session ClOrdID ↔ OrderId map.
pub struct ClOrdIdMap {
    /// FIX ClOrdID → Melin OrderId.
    to_order_id: HashMap<String, OrderId>,
    /// Melin OrderId → FIX ClOrdID.
    to_clord_id: HashMap<OrderId, String>,
    /// Next OrderId to assign. Starts at 1, increments monotonically.
    next_id: u64,
}

impl ClOrdIdMap {
    pub fn new() -> Self {
        Self {
            to_order_id: HashMap::new(),
            to_clord_id: HashMap::new(),
            next_id: 1,
        }
    }

    /// Register a new ClOrdID and assign a monotonic OrderId.
    /// Returns the assigned OrderId.
    ///
    /// If the ClOrdID was already registered, returns the existing OrderId
    /// without allocating a new one.
    pub fn insert(&mut self, clord_id: &str) -> OrderId {
        if let Some(&id) = self.to_order_id.get(clord_id) {
            return id;
        }
        let id = OrderId(self.next_id);
        self.next_id += 1;
        self.to_order_id.insert(clord_id.to_owned(), id);
        self.to_clord_id.insert(id, clord_id.to_owned());
        id
    }

    /// Look up the Melin OrderId for a FIX ClOrdID.
    pub fn get_order_id(&self, clord_id: &str) -> Option<OrderId> {
        self.to_order_id.get(clord_id).copied()
    }

    /// Look up the FIX ClOrdID for a Melin OrderId.
    pub fn get_clord_id(&self, order_id: OrderId) -> Option<&str> {
        self.to_clord_id.get(&order_id).map(|s| s.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_assigns_monotonic_ids() {
        let mut map = ClOrdIdMap::new();
        let id1 = map.insert("ORD001");
        let id2 = map.insert("ORD002");
        let id3 = map.insert("ORD003");
        assert_eq!(id1, OrderId(1));
        assert_eq!(id2, OrderId(2));
        assert_eq!(id3, OrderId(3));
    }

    #[test]
    fn duplicate_insert_returns_same_id() {
        let mut map = ClOrdIdMap::new();
        let id1 = map.insert("ORD001");
        let id2 = map.insert("ORD001");
        assert_eq!(id1, id2);
        // Only one ID consumed: a third distinct insert should yield 2.
        let id3 = map.insert("ORD002");
        assert_eq!(id3, OrderId(2));
    }

    #[test]
    fn bidirectional_lookup() {
        let mut map = ClOrdIdMap::new();
        let id = map.insert("MY_ORDER");
        assert_eq!(map.get_order_id("MY_ORDER"), Some(id));
        assert_eq!(map.get_clord_id(id), Some("MY_ORDER"));
    }

    #[test]
    fn unknown_lookup_returns_none() {
        let map = ClOrdIdMap::new();
        assert_eq!(map.get_order_id("UNKNOWN"), None);
        assert_eq!(map.get_clord_id(OrderId(999)), None);
    }
}
