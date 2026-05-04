//! Bounded single-producer / single-consumer ring buffer.
//!
//! Cache-padded head and tail atomics on separate cache lines so the
//! producer and consumer don't false-share. Power-of-two capacity so
//! indexing is `idx & mask` rather than a modulo. No locks — both
//! `try_push` and `try_pop` are wait-free and complete in O(1) memory
//! operations on the hot path.
//!
//! Used by `io_uring_endpoint` to fan out classified frames from the
//! poller thread to the two consumer halves without a shared mutex.
//!
//! Hand-rolled rather than pulled from `crossbeam-channel` for two
//! reasons: (1) we need the ring to live behind an `Arc` shared
//! between producer and consumer with no extra indirection, and
//! (2) the rumcast crate keeps its dependency footprint minimal.
//!
//! # Memory ordering
//!
//! Producer publishes via `Release` on `tail`; consumer pairs with
//! `Acquire` on `tail`. Symmetrically for `head`. This forms a
//! release/acquire chain so writes to slot bytes by the producer are
//! visible to the consumer the moment it observes the new tail value.
//!
//! # Safety invariants
//!
//! - Only the holder of `Producer<T>` may write to slots in
//!   `[head, tail)`-relative indices on the producer side.
//! - Only the holder of `Consumer<T>` may read/take slots from those
//!   same indices on the consumer side.
//! - Capacity must be a power of two (asserted at construction).

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Cache-line aligned wrapper. 64 bytes matches x86_64 / aarch64
/// cache line size; pad to 128 to also defeat adjacent-line
/// prefetchers (Intel's "spatial prefetcher" pulls pairs of lines).
#[repr(align(128))]
struct CachePadded<T>(T);

impl<T> CachePadded<T> {
    const fn new(t: T) -> Self {
        Self(t)
    }
}

impl<T> std::ops::Deref for CachePadded<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}

struct SpscRing<T> {
    /// Slots are `MaybeUninit` because positions outside `[head, tail)`
    /// are uninitialized — the ring grows to capacity over time.
    slots: Box<[UnsafeCell<MaybeUninit<T>>]>,
    /// Always `slots.len() - 1`, with `slots.len()` a power of two.
    /// Stored as a field rather than recomputed so the hot path stays
    /// branch-free.
    mask: usize,
    /// Consumer's index. Producer reads via `Acquire`; consumer reads
    /// `Relaxed` (it owns the value) and stores `Release`.
    head: CachePadded<AtomicUsize>,
    /// Producer's index. Consumer reads via `Acquire`; producer reads
    /// `Relaxed` and stores `Release`.
    tail: CachePadded<AtomicUsize>,
}

// Safety: SpscRing presents a producer/consumer split where each side
// only touches its own atomic and its own half of the slot array.
// All inter-thread synchronization goes through the two atomics with
// release/acquire ordering; T must be Send for the handoff itself.
unsafe impl<T: Send> Send for SpscRing<T> {}
unsafe impl<T: Send> Sync for SpscRing<T> {}

impl<T> Drop for SpscRing<T> {
    fn drop(&mut self) {
        // Drop the still-in-flight items between head and tail.
        // Other indices hold uninitialized memory and must not be
        // dropped. Using `Relaxed` is fine here: the ring is being
        // dropped, so by the borrow checker no other thread holds
        // a reference to it.
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Relaxed);
        let mut idx = head;
        while idx != tail {
            let slot = &self.slots[idx & self.mask];
            // Safety: position `idx` is in `[head, tail)`, which the
            // ring's invariants say is initialized.
            unsafe { (*slot.get()).assume_init_drop() };
            idx = idx.wrapping_add(1);
        }
    }
}

/// Producer end. Single-owner: there is exactly one `Producer<T>`
/// per ring, never cloned.
pub struct Producer<T> {
    ring: Arc<SpscRing<T>>,
}

/// Consumer end. Single-owner: there is exactly one `Consumer<T>`
/// per ring, never cloned.
pub struct Consumer<T> {
    ring: Arc<SpscRing<T>>,
}

// Send is fine — Producer/Consumer are single-owner handles to a ring
// that's already Sync.
unsafe impl<T: Send> Send for Producer<T> {}
unsafe impl<T: Send> Send for Consumer<T> {}

/// Reserved slot in the producer's ring. Hold the claim while
/// initializing the slot in place; call [`commit`] to make it
/// visible to the consumer.
///
/// Borrows the producer mutably — at most one outstanding claim at
/// a time. Dropping a claim without committing leaves the slot
/// reserved-but-unpublished; the next `try_claim` from the same
/// producer reuses the same slot, so no leak occurs.
///
/// [`commit`]: Claim::commit
pub struct Claim<'a, T> {
    slot: &'a UnsafeCell<MaybeUninit<T>>,
    tail: usize,
    tail_atomic: &'a AtomicUsize,
}

impl<'a, T> Claim<'a, T> {
    /// Raw pointer to the uninitialized slot. The caller must fully
    /// initialize the `T` (every field, no partial writes) before
    /// calling [`commit`].
    #[inline]
    pub fn as_mut_ptr(&self) -> *mut T {
        self.slot.get() as *mut T
    }

    /// Convenience: write `item` into the slot, then commit.
    ///
    /// # Safety
    ///
    /// Equivalent to writing `item` to the slot pointer and committing.
    /// Marked unsafe purely because it's the path used by the in-place
    /// constructor below; callers using this directly should prefer
    /// `Producer::try_push`.
    #[inline]
    unsafe fn write(self, item: T) {
        // Safety: `as_mut_ptr` returns a pointer to uninitialized
        // memory we've reserved exclusively; writing `item` initializes
        // the slot before publication.
        unsafe { self.as_mut_ptr().write(item) };
        // Safety: post-write the slot is fully initialized; commit
        // publishes it.
        unsafe { self.commit() };
    }

    /// Publish the slot to the consumer.
    ///
    /// # Safety
    ///
    /// The memory at [`as_mut_ptr`] must be a fully-initialized `T`.
    /// The release-store on `tail` synchronizes with the consumer's
    /// acquire-load.
    #[inline]
    pub unsafe fn commit(self) {
        self.tail_atomic
            .store(self.tail.wrapping_add(1), Ordering::Release);
    }
}

impl<T> Producer<T> {
    /// Push one item. Returns `Err(item)` if the ring is full.
    /// Convenience wrapper over `try_claim` for callers who already
    /// have a fully-formed `T`; LMAX-style hot paths should prefer
    /// `try_claim` to avoid a stack→ring memcpy of large items.
    #[allow(dead_code)]
    #[inline]
    pub fn try_push(&mut self, item: T) -> Result<(), T> {
        match self.try_claim() {
            None => Err(item),
            Some(claim) => {
                // Safety: claim's slot is uninitialized; we initialize
                // by writing `item` into it, then publish.
                unsafe {
                    claim.write(item);
                }
                Ok(())
            }
        }
    }

    /// Reserve the next ring slot without writing to it. The slot
    /// returned by [`Claim::as_mut_ptr`] is uninitialized; the caller
    /// is responsible for fully initializing the `T` before invoking
    /// [`Claim::commit`]. The slot is not visible to the consumer
    /// until commit.
    ///
    /// This is the LMAX in-place construction path used to avoid a
    /// stack→ring memcpy of large `T`s on the hot path.
    #[inline]
    pub fn try_claim(&mut self) -> Option<Claim<'_, T>> {
        let ring = &*self.ring;
        // Producer owns `tail`.
        let tail = ring.tail.load(Ordering::Relaxed);
        // Acquire on `head` to observe consumer progress.
        let head = ring.head.load(Ordering::Acquire);
        if tail.wrapping_sub(head) == ring.slots.len() {
            return None;
        }
        let idx = tail & ring.mask;
        Some(Claim {
            slot: &ring.slots[idx],
            tail,
            tail_atomic: &ring.tail,
        })
    }

    /// Approximate count of in-flight items. Producer-side estimate;
    /// the true value may have grown smaller by the time the caller
    /// reads it (consumer made progress). Useful only for diagnostics.
    #[allow(dead_code)]
    pub fn len_approx(&self) -> usize {
        let tail = self.ring.tail.load(Ordering::Relaxed);
        let head = self.ring.head.load(Ordering::Relaxed);
        tail.wrapping_sub(head)
    }
}

impl<T> Consumer<T> {
    /// Pop one item. Returns `None` if the ring is empty.
    #[inline]
    pub fn try_pop(&mut self) -> Option<T> {
        let ring = &*self.ring;
        // Consumer owns `head`.
        let head = ring.head.load(Ordering::Relaxed);
        // Acquire on `tail` synchronizes with the producer's release
        // when it advanced `tail` — guarantees the slot bytes are
        // visible.
        let tail = ring.tail.load(Ordering::Acquire);
        if head == tail {
            return None;
        }
        let idx = head & ring.mask;
        // Safety: position `head` is in `[head_old, tail)` and
        // therefore initialized by the producer; we have exclusive
        // read/take access as the sole consumer.
        let item = unsafe { (*ring.slots[idx].get()).assume_init_read() };
        // Release: signals to the producer that the slot is free for
        // overwrite.
        ring.head.store(head.wrapping_add(1), Ordering::Release);
        Some(item)
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        let head = self.ring.head.load(Ordering::Relaxed);
        let tail = self.ring.tail.load(Ordering::Acquire);
        head == tail
    }
}

/// Construct a fresh ring with `capacity` slots. `capacity` must be
/// a power of two; panics otherwise.
pub fn channel<T>(capacity: usize) -> (Producer<T>, Consumer<T>) {
    assert!(
        capacity.is_power_of_two() && capacity > 0,
        "SPSC capacity must be a non-zero power of two"
    );
    let mut slots = Vec::with_capacity(capacity);
    for _ in 0..capacity {
        slots.push(UnsafeCell::new(MaybeUninit::uninit()));
    }
    let ring = Arc::new(SpscRing {
        slots: slots.into_boxed_slice(),
        mask: capacity - 1,
        head: CachePadded::new(AtomicUsize::new(0)),
        tail: CachePadded::new(AtomicUsize::new(0)),
    });
    (
        Producer {
            ring: Arc::clone(&ring),
        },
        Consumer { ring },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_pop_round_trip_preserves_order() {
        let (mut p, mut c) = channel::<u32>(4);
        for i in 0..4 {
            p.try_push(i).unwrap();
        }
        for i in 0..4 {
            assert_eq!(c.try_pop(), Some(i));
        }
        assert_eq!(c.try_pop(), None);
    }

    #[test]
    fn full_ring_returns_err() {
        let (mut p, mut c) = channel::<u32>(2);
        p.try_push(1).unwrap();
        p.try_push(2).unwrap();
        assert_eq!(p.try_push(3), Err(3));
        c.try_pop();
        // After consuming one, push succeeds again.
        p.try_push(4).unwrap();
    }

    #[test]
    fn wrap_across_index_space() {
        // Push and pop more than capacity to exercise the wrap.
        let (mut p, mut c) = channel::<u32>(4);
        for i in 0..100 {
            p.try_push(i).unwrap();
            assert_eq!(c.try_pop(), Some(i));
        }
        assert!(c.is_empty());
    }

    #[test]
    fn drop_runs_for_in_flight_items() {
        use std::sync::atomic::AtomicUsize;
        static DROPS: AtomicUsize = AtomicUsize::new(0);
        struct CountsDrop;
        impl Drop for CountsDrop {
            fn drop(&mut self) {
                DROPS.fetch_add(1, Ordering::Relaxed);
            }
        }
        DROPS.store(0, Ordering::Relaxed);
        {
            let (mut p, mut c) = channel::<CountsDrop>(4);
            assert!(p.try_push(CountsDrop).is_ok());
            assert!(p.try_push(CountsDrop).is_ok());
            assert!(p.try_push(CountsDrop).is_ok());
            // Consumer pops one — that one is dropped via assume_init_read.
            drop(c.try_pop());
            // Two remain in flight; ring drop should drop them.
            drop(p);
            drop(c);
        }
        assert_eq!(DROPS.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn cross_thread_handoff() {
        // Sanity check: ten thousand items through a real thread pair.
        let (mut p, mut c) = channel::<u64>(64);
        let producer = std::thread::spawn(move || {
            let mut i = 0u64;
            while i < 10_000 {
                if p.try_push(i).is_ok() {
                    i += 1;
                } else {
                    std::hint::spin_loop();
                }
            }
        });
        let mut next = 0u64;
        while next < 10_000 {
            match c.try_pop() {
                Some(v) => {
                    assert_eq!(v, next);
                    next += 1;
                }
                None => std::hint::spin_loop(),
            }
        }
        producer.join().unwrap();
    }

    #[test]
    #[should_panic(expected = "power of two")]
    fn rejects_non_power_of_two_capacity() {
        let _ = channel::<u8>(3);
    }

    #[test]
    fn claim_then_commit_publishes_slot() {
        let (mut p, mut c) = channel::<u32>(4);
        let claim = p.try_claim().expect("claim");
        // Write directly into the slot without going through try_push.
        unsafe {
            claim.as_mut_ptr().write(0xDEAD_BEEF);
            claim.commit();
        }
        assert_eq!(c.try_pop(), Some(0xDEAD_BEEF));
    }

    #[test]
    fn claim_returns_none_when_full() {
        let (mut p, _c) = channel::<u32>(2);
        let _claim_a = p.try_claim().expect("first claim");
        // Holding a claim reserves a slot, but until commit() the
        // tail isn't advanced — so a second try_claim sees the same
        // slot. We need to commit first to fill the ring, not just
        // claim.
        unsafe {
            _claim_a.write(1);
        }
        let claim_b = p.try_claim().expect("second claim");
        unsafe {
            claim_b.write(2);
        }
        // Ring now full.
        assert!(p.try_claim().is_none());
    }
}
