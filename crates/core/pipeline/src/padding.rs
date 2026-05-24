//! Cache-line alignment to prevent false sharing between cores.
//!
//! Modern x86 CPUs use 64-byte cache lines. When two threads write to
//! different fields that share a cache line, the line bounces between
//! cores (false sharing), adding ~50-100ns per access. Padding each
//! hot atomic to its own cache line eliminates this.

use std::sync::atomic::AtomicU64;

/// Aligns the inner value to a 64-byte cache line boundary.
///
/// 64 bytes matches x86/ARM cache lines. The `repr(align(64))` ensures
/// the struct starts on a cache line boundary and `AtomicU64` (8 bytes)
/// plus padding fills the rest, preventing any other data from sharing
/// the same cache line.
#[repr(align(64))]
pub struct CachePadded<T> {
    value: T,
}

impl<T> CachePadded<T> {
    /// Wrap a value with cache-line padding.
    pub const fn new(value: T) -> Self {
        Self { value }
    }

    /// Access the inner value.
    pub fn get(&self) -> &T {
        &self.value
    }

    /// Mutably access the inner value.
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.value
    }
}

/// A cache-line-padded atomic sequence counter.
///
/// Used as the coordination primitive between producer and consumers.
/// Each sequence counter lives on its own cache line so concurrent
/// reads/writes from different cores don't cause false sharing.
pub type Sequence = CachePadded<AtomicU64>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alignment_is_64_bytes() {
        assert_eq!(std::mem::align_of::<CachePadded<AtomicU64>>(), 64);
    }

    #[test]
    fn size_is_at_least_64_bytes() {
        // Must fill a full cache line.
        assert!(std::mem::size_of::<CachePadded<AtomicU64>>() >= 64);
    }
}
