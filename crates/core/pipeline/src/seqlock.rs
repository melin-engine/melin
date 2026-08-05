//! SeqLock — a lock-free primitive for sharing a small `Copy` value from
//! exactly one writer to one or more readers.
//!
//! The writer increments a sequence counter before and after updating the
//! value. Readers retry if the counter changed during their read (torn
//! read detection). Zero contention when writer and readers operate at
//! different frequencies.
//!
//! Used to share the BLAKE3 chain hash (32 bytes) from the journal stage
//! to the shadow snapshot stage without a mutex on the hot path.
//!
//! # Single-writer is enforced by the type system
//!
//! [`split`] hands out one [`SeqLockWriter`] (neither `Clone` nor
//! duplicable, and `store` takes `&mut self`) and any number of
//! [`SeqLockReader`]s. A second writer is a compile error rather than a
//! documented obligation, because the protocol does not merely race under
//! concurrent writes — it silently reports success on corrupt data:
//!
//! ```text
//! W1: seq 0 -> 1  (odd, "write in progress")
//! W2: seq 1 -> 2  (even, "idle")  <-- readers now see a clean lock
//! W1, W2: write the value concurrently
//! R:  seq1 = 2 (even) -> reads a torn value -> seq2 = 2 -> returns it
//! ```
//!
//! The counter's atomicity was never the missing piece; the even/odd
//! invariant is, and no read-modify-write restores it.
//!
//! # The payload copy is atomic — not plain, not volatile
//!
//! A reader's payload load intentionally races with the writer's store;
//! the sequence check discards torn values. But in the Rust memory model
//! a racing non-atomic access is a data race — undefined behavior even
//! when the value is thrown away — and volatile does not help: volatile
//! only pins the number of real accesses the compiler emits, it does not
//! make them atomic (Miri flags both the plain and the volatile variant
//! as UB). The payload is therefore copied through word-sized `Relaxed`
//! atomics — the same construction crossbeam's seqlock uses — with the
//! fences below providing the ordering. On x86 and AArch64 a `Relaxed`
//! atomic load/store compiles to a plain mov / `ldr`+`str`, so this
//! costs nothing over the volatile version.
//!
//! One requirement follows: `T` must not contain padding bytes, because
//! reading a word that covers uninitialized padding is itself UB — even
//! single-threaded, before any race enters the picture. Like the
//! single-writer invariant above, this is enforced by the type system
//! rather than documented: [`split`] bounds the payload by [`NoPadding`],
//! so a padded type is a compile error:
//!
//! ```compile_fail
//! #[derive(Clone, Copy)]
//! struct Padded {
//!     a: u8,
//!     b: u64, // 7 padding bytes between `a` and `b`
//! }
//! // the trait bound `Padded: NoPadding` is not satisfied
//! let _ = melin_pipeline::seqlock::split(Padded { a: 1, b: 2 });
//! ```

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

/// Marker for payload types whose every byte is initialized — no padding.
///
/// The seqlock copies its payload through word-sized atomics; a word that
/// covers padding bytes would read uninitialized memory, which is UB
/// regardless of the seqlock protocol (see the module docs). The trait is
/// `unsafe` because the compiler cannot check the property; implementors
/// assert it.
///
/// # Safety
/// Every byte of a value of the implementing type must always be
/// initialized: no padding bytes, no `MaybeUninit` (or similar
/// possibly-uninitialized) fields. Where the layout allows, back the
/// `unsafe impl` with a const assertion that `size_of` equals the sum of
/// the field sizes — under `repr(C)`, that equality rules out padding.
pub unsafe trait NoPadding: Copy {}

// Safety: primitive integers are their size in initialized bytes — no
// padding.
unsafe impl NoPadding for u8 {}
unsafe impl NoPadding for u16 {}
unsafe impl NoPadding for u32 {}
unsafe impl NoPadding for u64 {}
unsafe impl NoPadding for u128 {}
unsafe impl NoPadding for usize {}
unsafe impl NoPadding for i8 {}
unsafe impl NoPadding for i16 {}
unsafe impl NoPadding for i32 {}
unsafe impl NoPadding for i64 {}
unsafe impl NoPadding for i128 {}
unsafe impl NoPadding for isize {}

// Safety: an array of padding-free elements is padding-free — its size is
// exactly `N * size_of::<T>()`.
unsafe impl<T: NoPadding, const N: usize> NoPadding for [T; N] {}

/// Create a linked writer/reader pair over `value`.
///
/// The writer is unique for the lifetime of the pair; readers are `Clone`
/// and may be shared across any number of threads. The [`NoPadding`]
/// bound is load-bearing — see the module docs.
pub fn split<T: NoPadding>(value: T) -> (SeqLockWriter<T>, SeqLockReader<T>) {
    let cell = Arc::new(SeqLockCell::new(value));
    (
        SeqLockWriter {
            cell: Arc::clone(&cell),
        },
        SeqLockReader { cell },
    )
}

/// The sole writer of a seqlock value.
///
/// Deliberately not `Clone`, and [`store`](Self::store) takes `&mut self`,
/// so the single-writer invariant the protocol depends on cannot be broken
/// without `unsafe`.
pub struct SeqLockWriter<T: NoPadding> {
    cell: Arc<SeqLockCell<T>>,
}

impl<T: NoPadding> SeqLockWriter<T> {
    /// Publish a new value. The sequence counter goes odd before the write
    /// and back to even after, so a reader that observes the mid-write
    /// state retries.
    #[inline]
    pub fn store(&mut self, value: T) {
        let cell = &*self.cell;

        // `&mut self` on a non-Clone handle means we are the only writer,
        // so a plain load/store beats a `fetch_add` here — the counter is
        // never contended, only observed. Relaxed is fine because the
        // Release fence below orders the value write against it.
        let seq = cell.sequence.load(Ordering::Relaxed);
        // Odd sequence = write in progress.
        cell.sequence.store(seq.wrapping_add(1), Ordering::Relaxed);
        // Fence: the sequence increment is visible before the value write.
        std::sync::atomic::fence(Ordering::Release);

        // Safety: the writer handle is unique and `store` takes `&mut
        // self`, so no other write overlaps this one; `src` is a live
        // stack value; `dst` is valid for the cell's lifetime and
        // 8-aligned (`Aligned`). Racing reader loads are atomic, so the
        // per-word atomic stores race with nothing non-atomic (see the
        // module docs for why the copy must be atomic).
        unsafe { atomic_store_copy(&raw const value, cell.value.0.get()) };

        // Fence: the value write is visible before the sequence goes back
        // to even.
        std::sync::atomic::fence(Ordering::Release);
        cell.sequence.store(seq.wrapping_add(2), Ordering::Relaxed);
    }
}

/// A reader of a seqlock value. Cheap to clone and share across threads.
pub struct SeqLockReader<T: NoPadding> {
    cell: Arc<SeqLockCell<T>>,
}

impl<T: NoPadding> Clone for SeqLockReader<T> {
    fn clone(&self) -> Self {
        Self {
            cell: Arc::clone(&self.cell),
        }
    }
}

impl<T: NoPadding> SeqLockReader<T> {
    /// Read the current value. Retries automatically on torn reads (writer
    /// was mid-update). Lock-free and wait-free in practice — retries only
    /// happen if a read overlaps a write, which is vanishingly rare when
    /// writer and reader operate at different frequencies (e.g., writer per
    /// fsync batch, reader per snapshot).
    pub fn load(&self) -> T {
        let cell = &*self.cell;
        loop {
            let seq1 = cell.sequence.load(Ordering::Acquire);
            if seq1 & 1 != 0 {
                // Writer is mid-update — spin and retry.
                std::hint::spin_loop();
                continue;
            }

            // This load may race with the writer's store; the seq2 check
            // below discards any torn result. The copy is atomic per
            // word, so the race is defined behavior, and the bytes land
            // in a `MaybeUninit` that is only assumed initialized after
            // the sequence check proves they came from one complete
            // `store` (see the module docs).
            //
            // Safety: `src` is valid for the cell's lifetime and
            // 8-aligned (`Aligned`); `dst` is a live stack buffer.
            let mut value = MaybeUninit::<T>::uninit();
            unsafe { atomic_load_copy(cell.value.0.get(), value.as_mut_ptr()) };

            // On weakly-ordered architectures (ARM/AArch64), the Relaxed
            // payload loads above can be reordered past a subsequent
            // atomic load at a different address. This Acquire fence
            // ensures the payload copy completes before we re-read the
            // sequence counter — without it, we could observe seq1==seq2
            // while `value` contains a torn read.
            std::sync::atomic::fence(Ordering::Acquire);
            let seq2 = cell.sequence.load(Ordering::Relaxed);
            if seq1 == seq2 {
                // Safety: the sequence did not change across the copy,
                // so every word came from the same completed `store` of
                // a valid `T` — the bytes are initialized and coherent.
                return unsafe { value.assume_init() };
            }
            // Sequence changed — writer updated during our read. Retry.
            std::hint::spin_loop();
        }
    }
}

/// The shared storage behind a writer/reader pair.
///
/// Private: handing this out directly would restore the footgun that
/// [`split`] exists to remove.
///
/// Cache-line padded so the sequence counter and value do not false-share
/// with whatever the allocator places next to them.
#[repr(align(64))]
struct SeqLockCell<T: NoPadding> {
    /// Even = idle (safe to read), odd = write in progress. `u64` rather
    /// than `u32` so wrap-around is unreachable in practice: at one write
    /// per nanosecond it takes ~584 years, and a wrap that landed exactly
    /// between a reader's two counter loads would be needed to fool the
    /// torn-read check.
    sequence: AtomicU64,
    value: Aligned<T>,
}

/// Payload storage forced to 8-byte alignment so the copy helpers can
/// access it in `AtomicU64` chunks regardless of `T`'s own alignment
/// (`[u8; 32]` is only 1-aligned). `repr(C)` pins the cell at offset 0
/// so the guarantee transfers to the payload itself.
#[repr(C, align(8))]
struct Aligned<T>(UnsafeCell<T>);

impl<T: NoPadding> SeqLockCell<T> {
    fn new(value: T) -> Self {
        Self {
            sequence: AtomicU64::new(0),
            value: Aligned(UnsafeCell::new(value)),
        }
    }
}

/// Copy `size_of::<T>()` bytes from `src` into `dst` with per-word
/// `Relaxed` atomic stores: `AtomicU64` chunks, then an `AtomicU8` tail
/// for payload sizes that are not a multiple of 8. Ordering relative to
/// the sequence counter comes from the fences in `store`.
///
/// # Safety
/// - `src` must be valid for reads of `size_of::<T>()` bytes with no
///   concurrent writes (it is the writer's stack value), and every one of
///   those bytes must be initialized — guaranteed by the [`NoPadding`]
///   bound at the public boundary.
/// - `dst` must be valid for writes of `size_of::<T>()` bytes, 8-byte
///   aligned, and concurrently accessed only through atomics.
unsafe fn atomic_store_copy<T>(src: *const T, dst: *mut T) {
    let size = size_of::<T>();
    let src = src.cast::<u8>();
    let dst = dst.cast::<u8>();
    let mut offset = 0;
    // The `AtomicU64` casts are aligned: `dst` is 8-aligned and `offset`
    // is a multiple of 8. `src` has only `T`'s alignment — which can be
    // 1 — hence `read_unaligned`.
    while offset + 8 <= size {
        let chunk = unsafe { src.add(offset).cast::<u64>().read_unaligned() };
        let slot = unsafe { &*dst.add(offset).cast::<AtomicU64>() };
        slot.store(chunk, Ordering::Relaxed);
        offset += 8;
    }
    while offset < size {
        let byte = unsafe { *src.add(offset) };
        let slot = unsafe { &*dst.add(offset).cast::<AtomicU8>() };
        slot.store(byte, Ordering::Relaxed);
        offset += 1;
    }
}

/// Mirror of [`atomic_store_copy`]: per-word `Relaxed` atomic loads from
/// the shared payload into a private buffer. The result may be torn if a
/// write overlapped the copy — the caller must validate the sequence
/// counter before treating `dst` as initialized.
///
/// # Safety
/// - `src` must be valid for reads of `size_of::<T>()` bytes, 8-byte
///   aligned, and concurrently written only through atomics.
/// - `dst` must be valid for writes of `size_of::<T>()` bytes with no
///   concurrent access (it is the reader's stack buffer).
unsafe fn atomic_load_copy<T>(src: *const T, dst: *mut T) {
    let size = size_of::<T>();
    let src = src.cast::<u8>();
    let dst = dst.cast::<u8>();
    let mut offset = 0;
    while offset + 8 <= size {
        let slot = unsafe { &*src.add(offset).cast::<AtomicU64>() };
        let chunk = slot.load(Ordering::Relaxed);
        unsafe { dst.add(offset).cast::<u64>().write_unaligned(chunk) };
        offset += 8;
    }
    while offset < size {
        let slot = unsafe { &*src.add(offset).cast::<AtomicU8>() };
        let byte = slot.load(Ordering::Relaxed);
        unsafe { *dst.add(offset) = byte };
        offset += 1;
    }
}

// Safety: T is Copy (no interior pointers), the writer handle is unique,
// and the seqlock protocol ensures readers never return a partially
// written value.
unsafe impl<T: NoPadding + Send> Send for SeqLockCell<T> {}
unsafe impl<T: NoPadding + Send> Sync for SeqLockCell<T> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_and_load() {
        let (mut w, r) = split(42u64);
        assert_eq!(r.load(), 42);
        w.store(99);
        assert_eq!(r.load(), 99);
    }

    #[test]
    fn load_returns_latest_value() {
        let (mut w, r) = split([0u8; 32]);
        let expected = [0xAB; 32];
        w.store(expected);
        assert_eq!(r.load(), expected);
    }

    #[test]
    fn reader_clones_observe_the_same_cell() {
        let (mut w, r1) = split(0u64);
        let r2 = r1.clone();
        w.store(7);
        assert_eq!((r1.load(), r2.load()), (7, 7));
    }

    #[test]
    fn concurrent_writer_reader_no_torn_reads() {
        let (mut writer_lock, lock) = split([0u8; 32]);

        let iterations = 100_000;

        let writer = std::thread::spawn(move || {
            for i in 0..iterations {
                // Write a uniform-byte array so torn reads are detectable:
                // if the reader sees mixed bytes, the seqlock failed.
                let byte = (i % 256) as u8;
                writer_lock.store([byte; 32]);
            }
        });

        // Reader: verify every read is a uniform array (no torn reads).
        let mut reads = 0u64;
        while !writer.is_finished() {
            let value = lock.load();
            // All 32 bytes must be the same — a torn read would mix
            // bytes from two different writes.
            assert!(
                value.iter().all(|&b| b == value[0]),
                "torn read detected: {:?}",
                value
            );
            reads += 1;
        }
        writer.join().unwrap();

        // Sanity: we actually did some reads.
        assert!(reads > 0);
    }

    #[test]
    fn multiple_reader_threads_see_no_torn_reads() {
        let (mut writer_lock, lock) = split([0u8; 32]);
        let done = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let readers: Vec<_> = (0..3)
            .map(|_| {
                let lock = lock.clone();
                let done = Arc::clone(&done);
                std::thread::spawn(move || {
                    let mut reads = 0u64;
                    while !done.load(Ordering::Relaxed) {
                        let value = lock.load();
                        assert!(
                            value.iter().all(|&b| b == value[0]),
                            "torn read detected: {:?}",
                            value
                        );
                        reads += 1;
                    }
                    reads
                })
            })
            .collect();

        for i in 0..100_000u32 {
            writer_lock.store([(i % 256) as u8; 32]);
        }
        done.store(true, Ordering::Relaxed);

        for r in readers {
            assert!(r.join().unwrap() > 0);
        }
    }
}
