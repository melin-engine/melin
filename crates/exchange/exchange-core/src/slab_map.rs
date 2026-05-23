//! `SlabMap`: a churn-tolerant map from `(AccountId, OrderId)` → dense slot
//! storage. Splits the role the per-book `order_index` / `stop_index`
//! HashMaps played into two pieces:
//!
//! 1. **Lookup**: `(AccountId, OrderId)` → `u32` slot_id via `std`'s
//!    `HashMap` (which is `hashbrown` under the hood). Hashbrown uses Robin
//!    Hood + backshift deletion, so the table's capacity tracks *live*
//!    entries, not lifetime inserts — fixing the deep-tail page-fault
//!    cluster that extendible hashing (astenn) exhibited under our
//!    high-churn workload (~269 M unique inserts over 90 s with bounded
//!    live count).
//!
//! 2. **Storage**: `Vec<Slot<V>>` indexed by `slot_id`. Slots are reused
//!    on remove via a freelist embedded in the vacant variants, so the
//!    storage stays bounded by peak live count even though total inserts
//!    are unbounded. This is the classic slab pattern (Linux kernel,
//!    `slab` crate); we inline it rather than pull a dependency.
//!
//! The slot's identity is internal — external callers continue to use
//! `(AccountId, OrderId)`. The key is stored alongside the value in each
//! slot so iteration can yield (key, value) pairs without holding a
//! second borrow on the lookup map.

use rustc_hash::FxBuildHasher;
use std::collections::HashMap;

use melin_types::types::{AccountId, OrderId};

/// Either an occupied entry or a freelist link to the next vacant slot.
///
/// Packing the freelist into the slot itself (rather than a separate Vec)
/// makes `insert` a single `Vec` index + variant flip — no Vec push.
#[derive(Debug)]
enum Slot<V> {
    Occupied {
        key: (AccountId, OrderId),
        value: V,
    },
    /// `u32::MAX` marks the freelist tail.
    Vacant {
        next_free: u32,
    },
}

/// Lookup-and-slab map keyed by `(AccountId, OrderId)`.
///
/// The const generic on `V` is the value type. The map is monomorphic in
/// the key (always `(AccountId, OrderId)`) because that's the only key
/// shape the engine uses; generalising would add a type parameter for
/// negligible benefit.
#[derive(Debug)]
pub(crate) struct SlabMap<V> {
    /// `(AccountId, OrderId)` → slot_id (`u32`). hashbrown-backed via
    /// `std`. `FxBuildHasher` matches the hasher astenn used so the
    /// per-key cost stays similar.
    lookup: HashMap<(AccountId, OrderId), u32, FxBuildHasher>,
    /// Slot storage. Index = slot_id.
    slots: Vec<Slot<V>>,
    /// Head of the freelist, or `u32::MAX` if no free slots.
    next_free: u32,
}

impl<V> SlabMap<V> {
    /// Create an empty slab map. Equivalent to `with_capacity(0)`; mirrors
    /// the `Vec::new` / `HashMap::new` shape that callers expect.
    pub(crate) fn new() -> Self {
        Self::with_capacity(0)
    }

    /// Create an empty slab map with the given initial capacity for both
    /// the lookup and the slot storage. Pre-sizing both avoids a
    /// reallocation when the bench's prefault pass touches every slot.
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            lookup: HashMap::with_capacity_and_hasher(capacity, FxBuildHasher),
            slots: Vec::with_capacity(capacity),
            next_free: u32::MAX,
        }
    }

    /// Insert an entry. Returns the previous value at `key` if it was
    /// present (mirroring `std::collections::HashMap::insert`). On
    /// overwrite the existing slot is reused (no slot churn / freelist
    /// movement).
    pub(crate) fn insert(&mut self, key: (AccountId, OrderId), value: V) -> Option<V> {
        if let Some(&slot_id) = self.lookup.get(&key) {
            // Existing entry: overwrite the value in place and return
            // the prior value so callers can detect collisions.
            let prev = std::mem::replace(
                &mut self.slots[slot_id as usize],
                Slot::Occupied { key, value },
            );
            return match prev {
                Slot::Occupied { value, .. } => Some(value),
                Slot::Vacant { .. } => None,
            };
        }
        let slot_id = if self.next_free == u32::MAX {
            let id = self.slots.len() as u32;
            self.slots.push(Slot::Occupied { key, value });
            id
        } else {
            let id = self.next_free;
            let prev =
                std::mem::replace(&mut self.slots[id as usize], Slot::Occupied { key, value });
            self.next_free = match prev {
                Slot::Vacant { next_free } => next_free,
                Slot::Occupied { .. } => {
                    // Freelist invariant violated — should never happen.
                    // Fall back to growing rather than corrupting state.
                    u32::MAX
                }
            };
            id
        };
        self.lookup.insert(key, slot_id);
        None
    }

    /// Remove an entry. Returns the stored value if `key` was present.
    pub(crate) fn remove(&mut self, key: &(AccountId, OrderId)) -> Option<V> {
        let slot_id = self.lookup.remove(key)?;
        let prev = std::mem::replace(
            &mut self.slots[slot_id as usize],
            Slot::Vacant {
                next_free: self.next_free,
            },
        );
        self.next_free = slot_id;
        match prev {
            Slot::Occupied { value, .. } => Some(value),
            // The lookup said this slot was occupied; if the slot itself
            // disagrees the invariants are already broken. Return None
            // rather than panicking on the hot path.
            Slot::Vacant { .. } => None,
        }
    }

    /// Look up an entry without removing it.
    pub(crate) fn get(&self, key: &(AccountId, OrderId)) -> Option<&V> {
        let &slot_id = self.lookup.get(key)?;
        match &self.slots[slot_id as usize] {
            Slot::Occupied { value, .. } => Some(value),
            Slot::Vacant { .. } => None,
        }
    }

    /// True iff `key` is present.
    pub(crate) fn contains_key(&self, key: &(AccountId, OrderId)) -> bool {
        self.lookup.contains_key(key)
    }

    /// Number of live entries. Test-only today; kept on the public API
    /// because callers will reach for `len()` to mirror std collections.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.lookup.len()
    }

    /// Capacity of the underlying lookup hashmap (peak slot count). Used
    /// by the bench's capacity-report diagnostic to detect growth past
    /// the prefaulted region.
    pub(crate) fn capacity(&self) -> usize {
        self.lookup.capacity()
    }

    /// Iterate occupied entries as `(&key, &value)` pairs. Skips slots in
    /// the freelist. Order is implementation-defined and not stable.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (&(AccountId, OrderId), &V)> {
        self.slots.iter().filter_map(|slot| match slot {
            Slot::Occupied { key, value } => Some((key, value)),
            Slot::Vacant { .. } => None,
        })
    }

    /// Iterate occupied entries as `(&key, &mut value)` pairs.
    pub(crate) fn iter_mut(&mut self) -> impl Iterator<Item = (&(AccountId, OrderId), &mut V)> {
        self.slots.iter_mut().filter_map(|slot| match slot {
            Slot::Occupied { key, value } => Some((&*key, value)),
            Slot::Vacant { .. } => None,
        })
    }

    /// Drop all entries. Capacity is retained for reuse.
    pub(crate) fn clear(&mut self) {
        self.lookup.clear();
        self.slots.clear();
        self.next_free = u32::MAX;
    }
}

impl<V> FromIterator<((AccountId, OrderId), V)> for SlabMap<V> {
    /// Build a `SlabMap` from an iterator of `(key, value)` pairs. The
    /// `size_hint` lower bound is used to pre-size the underlying lookup
    /// and slab so a known-length producer (e.g. snapshot restore) avoids
    /// rehash / Vec-growth allocations during the build.
    fn from_iter<I: IntoIterator<Item = ((AccountId, OrderId), V)>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let (lower, _) = iter.size_hint();
        let mut map = Self::with_capacity(lower);
        for (key, value) in iter {
            map.insert(key, value);
        }
        map
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(a: u32, o: u64) -> (AccountId, OrderId) {
        (AccountId(a), OrderId(o))
    }

    #[test]
    fn insert_get_remove() {
        let mut m: SlabMap<u32> = SlabMap::with_capacity(16);
        m.insert(key(1, 1), 100);
        m.insert(key(1, 2), 200);
        assert_eq!(m.get(&key(1, 1)), Some(&100));
        assert_eq!(m.get(&key(1, 2)), Some(&200));
        assert_eq!(m.len(), 2);
        assert_eq!(m.remove(&key(1, 1)), Some(100));
        assert_eq!(m.get(&key(1, 1)), None);
        assert_eq!(m.len(), 1);
    }

    #[test]
    fn insert_overwrites_existing_slot() {
        let mut m: SlabMap<u32> = SlabMap::with_capacity(16);
        assert_eq!(m.insert(key(1, 1), 100), None, "fresh insert returns None");
        assert_eq!(
            m.insert(key(1, 1), 200),
            Some(100),
            "overwrite returns the prior value (mirroring HashMap::insert)",
        );
        assert_eq!(m.get(&key(1, 1)), Some(&200));
        assert_eq!(m.len(), 1);
    }

    #[test]
    fn slot_recycled_after_remove() {
        let mut m: SlabMap<u32> = SlabMap::with_capacity(16);
        m.insert(key(1, 1), 100);
        m.remove(&key(1, 1));
        m.insert(key(2, 2), 200);
        // Slot reuse is the load-bearing property: insert-remove-insert
        // must NOT grow the slab Vec past 1 entry.
        assert_eq!(m.slots.len(), 1, "freed slot must be reused");
    }

    #[test]
    fn high_churn_bounded_storage() {
        // 100 cycles of "insert a batch, then delete that batch" with
        // unique keys per cycle. Total inserts = 10,000; live count
        // stays at ~100 throughout. Slab storage must stay bounded
        // by the peak live count, not by total inserts (the property
        // that motivated this refactor).
        let mut m: SlabMap<u64> = SlabMap::with_capacity(128);
        for cycle in 0u64..100 {
            let base = cycle * 100;
            for i in 0..100 {
                m.insert(key(1, base + i), base + i);
            }
            for i in 0..100 {
                m.remove(&key(1, base + i));
            }
        }
        assert_eq!(m.len(), 0);
        // Pool stays at ~100 slots (one per peak live entry). Without
        // freelist reuse, slots.len() would be 10,000.
        assert!(m.slots.len() <= 200, "slab grew to {}", m.slots.len());
    }

    #[test]
    fn iter_visits_all_occupied() {
        let mut m: SlabMap<u32> = SlabMap::with_capacity(16);
        m.insert(key(1, 1), 10);
        m.insert(key(1, 2), 20);
        m.insert(key(1, 3), 30);
        m.remove(&key(1, 2));
        let mut seen: Vec<(u32, u64, u32)> = m.iter().map(|(k, v)| (k.0.0, k.1.0, *v)).collect();
        seen.sort();
        assert_eq!(seen, vec![(1, 1, 10), (1, 3, 30)]);
    }

    #[test]
    fn iter_mut_can_modify_values() {
        let mut m: SlabMap<u32> = SlabMap::with_capacity(16);
        m.insert(key(1, 1), 10);
        m.insert(key(1, 2), 20);
        for (_, v) in m.iter_mut() {
            *v *= 2;
        }
        assert_eq!(m.get(&key(1, 1)), Some(&20));
        assert_eq!(m.get(&key(1, 2)), Some(&40));
    }

    #[test]
    fn clear_drops_everything() {
        let mut m: SlabMap<u32> = SlabMap::with_capacity(16);
        m.insert(key(1, 1), 10);
        m.insert(key(1, 2), 20);
        m.clear();
        assert_eq!(m.len(), 0);
        assert_eq!(m.get(&key(1, 1)), None);
        // After clear, inserts should still work (no stale freelist).
        m.insert(key(2, 1), 100);
        assert_eq!(m.get(&key(2, 1)), Some(&100));
    }
}
