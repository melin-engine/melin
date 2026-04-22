//! Engine-internal types and re-exports of the shared trading wire types.
//!
//! Wire-level data (Symbol, Order, ExecutionReport, etc.) lives in
//! `melin-trading` so the no-op transport binary can speak the same
//! protocol without pulling in the matching engine. Engine-internal
//! types that the matching pipeline needs but external consumers don't
//! (reservation slab handles, the `astenn`-backed HashMap aliases) stay
//! here.

pub use melin_trading::types::*;

/// HashMap with FxHash and extendible hashing (via `astenn`).
///
/// Uses `FxBuildHasher` (fast non-cryptographic hash) with `astenn::HashMap`
/// which grows one bucket at a time instead of rehashing the entire table.
/// Each insert that triggers growth only touches entries in the splitting
/// bucket — bounded O(bucket_size) cost regardless of total table size.
///
/// Replaces `rustc_hash::FxHashMap` (hashbrown) which rehashes all entries
/// at once when load factor is exceeded — causing deterministic latency
/// spikes on the hot path when account population exceeds pre-allocated
/// capacity.
///
/// Default bucket size is 8 (hash array fits in one cache line).
pub type HashMap<K, V> = astenn::HashMap<K, V, rustc_hash::FxBuildHasher>;

/// HashMap variant with 4-entry buckets for hot-path maps where point
/// lookups dominate. Smaller buckets mean fewer comparisons per probe
/// and a tighter working set in L1, at the cost of more frequent splits
/// during growth.
pub type HashMap4<K, V> = astenn::HashMap<K, V, rustc_hash::FxBuildHasher, 4>;

/// Opaque handle to a reservation in the slab. O(1) Vec-indexed access,
/// no hashing. Valid from `try_reserve` until `release` or fill completion.
///
/// u32 index: supports up to ~4 billion concurrent reservations. At 2M
/// pre-allocated slots this is more than sufficient.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReservationSlot(pub(crate) u32);

impl ReservationSlot {
    /// Sentinel value for prefault dummy entries and snapshot restore
    /// (before real slots are injected). Never used in production matching.
    pub const DUMMY: Self = Self(u32::MAX);
}
