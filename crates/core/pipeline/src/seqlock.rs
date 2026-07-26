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

use std::cell::UnsafeCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Create a linked writer/reader pair over `value`.
///
/// The writer is unique for the lifetime of the pair; readers are `Clone`
/// and may be shared across any number of threads.
pub fn split<T: Copy>(value: T) -> (SeqLockWriter<T>, SeqLockReader<T>) {
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
pub struct SeqLockWriter<T: Copy> {
    cell: Arc<SeqLockCell<T>>,
}

impl<T: Copy> SeqLockWriter<T> {
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
        // self`, so no other write and no reader-visible aliasing can
        // overlap this one.
        unsafe { *cell.value.get() = value };

        // Fence: the value write is visible before the sequence goes back
        // to even.
        std::sync::atomic::fence(Ordering::Release);
        cell.sequence.store(seq.wrapping_add(2), Ordering::Relaxed);
    }
}

/// A reader of a seqlock value. Cheap to clone and share across threads.
pub struct SeqLockReader<T: Copy> {
    cell: Arc<SeqLockCell<T>>,
}

impl<T: Copy> Clone for SeqLockReader<T> {
    fn clone(&self) -> Self {
        Self {
            cell: Arc::clone(&self.cell),
        }
    }
}

impl<T: Copy> SeqLockReader<T> {
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

            // Safety: sequence is even, so no write is in progress. The
            // Acquire on seq1 ensures we see the completed write, and the
            // seq2 check below discards the value if that stopped holding.
            let value = unsafe { *cell.value.get() };

            // On weakly-ordered architectures (ARM/AArch64), the plain
            // load of `value` above can be reordered past a subsequent
            // atomic load at a different address. This Acquire fence
            // ensures the value read completes before we re-read the
            // sequence counter — without it, we could observe seq1==seq2
            // while `value` contains a torn read.
            std::sync::atomic::fence(Ordering::Acquire);
            let seq2 = cell.sequence.load(Ordering::Relaxed);
            if seq1 == seq2 {
                return value;
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
struct SeqLockCell<T: Copy> {
    /// Even = idle (safe to read), odd = write in progress. `u64` rather
    /// than `u32` so wrap-around is unreachable in practice: at one write
    /// per nanosecond it takes ~584 years, and a wrap that landed exactly
    /// between a reader's two counter loads would be needed to fool the
    /// torn-read check.
    sequence: AtomicU64,
    value: UnsafeCell<T>,
}

impl<T: Copy> SeqLockCell<T> {
    fn new(value: T) -> Self {
        Self {
            sequence: AtomicU64::new(0),
            value: UnsafeCell::new(value),
        }
    }
}

// Safety: T is Copy (no interior pointers), the writer handle is unique,
// and the seqlock protocol ensures readers never return a partially
// written value.
unsafe impl<T: Copy + Send> Send for SeqLockCell<T> {}
unsafe impl<T: Copy + Send> Sync for SeqLockCell<T> {}

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
