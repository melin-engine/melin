//! Shared storage primitives used by both the publication and
//! subscription log buffers.
//!
//! - [`LogStorage`] — aligned, zero-initialized heap buffer that backs
//!   the three rotating term buffers in either direction.
//! - [`CachePadded`] — 64-byte alignment wrapper so hot atomics live on
//!   their own cache line (no false sharing).

use std::alloc::{Layout, alloc_zeroed, dealloc};
use std::ptr::NonNull;

/// 64-byte cache-line padding for hot atomics. Local copy so this crate
/// stays free of `melin-disruptor` for one tiny type.
#[repr(align(64))]
pub(crate) struct CachePadded<T>(pub(crate) T);

impl<T> CachePadded<T> {
    pub(crate) const fn new(value: T) -> Self {
        Self(value)
    }
    pub(crate) fn get(&self) -> &T {
        &self.0
    }
}

/// Aligned, zero-initialized heap storage. Allocated once at construction;
/// never resized. 64-byte alignment guarantees that 32-byte slots within
/// the buffer can host `DataFrame` headers (8-byte alignment requirement).
pub(crate) struct LogStorage {
    ptr: NonNull<u8>,
    layout: Layout,
}

// SAFETY: The buffer is owned by the LogStorage; concurrent access is
// coordinated by the surrounding atomic protocols (single producer +
// single sender on the publication side, single receiver + single
// subscriber on the subscription side).
unsafe impl Send for LogStorage {}
unsafe impl Sync for LogStorage {}

impl LogStorage {
    pub(crate) fn new(size: usize) -> Self {
        let layout = Layout::from_size_align(size, 64).expect("invalid log layout");
        // SAFETY: layout has nonzero size — callers pass 3 * term_length
        // and term_length is validated to be at least 64 KiB upstream.
        let raw = unsafe { alloc_zeroed(layout) };
        // Construction-time allocation. Failing here means the host can't
        // give us 3 * term_length bytes of contiguous heap, which is
        // unrecoverable for a transport that needs the log buffer to
        // exist. Propagating an error would force every caller into the
        // same panic path one frame up; we panic directly with a clear
        // message instead.
        let ptr = NonNull::new(raw).expect("rumcast: log buffer allocation failed");
        Self { ptr, layout }
    }

    #[inline]
    pub(crate) fn as_ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }
}

impl Drop for LogStorage {
    fn drop(&mut self) {
        // SAFETY: same layout used to allocate.
        unsafe { dealloc(self.ptr.as_ptr(), self.layout) };
    }
}

/// Round `n` up to the next multiple of `alignment`. `alignment` MUST be
/// a power of two (debug-asserted). Used to align fragment sizes within
/// the term buffers on both publication and subscription sides.
#[inline]
pub(crate) fn align_up(n: u32, alignment: u32) -> u32 {
    debug_assert!(alignment.is_power_of_two());
    (n + alignment - 1) & !(alignment - 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn align_up_basic() {
        assert_eq!(align_up(0, 32), 0);
        assert_eq!(align_up(1, 32), 32);
        assert_eq!(align_up(31, 32), 32);
        assert_eq!(align_up(32, 32), 32);
        assert_eq!(align_up(33, 32), 64);
        assert_eq!(align_up(100, 32), 128);
    }
}
