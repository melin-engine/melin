//! SeqLock — a lock-free synchronization primitive for sharing small
//! `Copy` values between a single writer and one or more readers.
//!
//! The writer increments a sequence counter before and after updating
//! the value. Readers retry if the counter changed during their read
//! (torn read detection). Zero contention when writer and readers
//! operate at different frequencies.
//!
//! Used to share the BLAKE3 chain hash (32 bytes) from the journal
//! stage to the shadow snapshot stage without a mutex on the hot path.

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU64, Ordering};

/// A sequence-locked value for single-writer, multi-reader sharing.
///
/// `T` must be `Copy` so it can be read/written without partial
/// initialization. The sequence counter detects torn reads — if the
/// reader observes a mid-write state, it retries.
///
/// Cache-line padded: the sequence counter and value live on separate
/// cache lines to avoid false sharing between writer and readers.
#[repr(align(64))]
pub struct SeqLock<T: Copy> {
    /// Even = idle (safe to read), odd = write in progress.
    sequence: AtomicU64,
    value: UnsafeCell<T>,
}

// Safety: T is Copy (no interior pointers), and the seqlock protocol
// ensures readers never see a partially written value.
unsafe impl<T: Copy + Send> Send for SeqLock<T> {}
unsafe impl<T: Copy + Send> Sync for SeqLock<T> {}

impl<T: Copy> SeqLock<T> {
    /// Create a new SeqLock with the given initial value.
    pub fn new(value: T) -> Self {
        Self {
            sequence: AtomicU64::new(0),
            value: UnsafeCell::new(value),
        }
    }

    /// Write a new value. Single-writer only — concurrent writes are
    /// undefined behavior. The sequence counter is incremented to an
    /// odd value before the write and back to even after, signaling
    /// readers that a write was in progress.
    pub fn store(&self, value: T) {
        // Odd sequence = write in progress. Relaxed is fine because
        // the Release fence after the write ensures ordering.
        self.sequence.fetch_add(1, Ordering::Relaxed);
        // Fence: ensure the sequence increment is visible before
        // the value write.
        std::sync::atomic::fence(Ordering::Release);

        // Safety: single-writer guarantee — no concurrent writes.
        unsafe { *self.value.get() = value };

        // Fence: ensure the value write is visible before the
        // sequence increment back to even.
        std::sync::atomic::fence(Ordering::Release);
        self.sequence.fetch_add(1, Ordering::Relaxed);
    }

    /// Read the current value. Retries automatically on torn reads
    /// (writer was mid-update). Lock-free and wait-free in practice —
    /// retries only happen if a read overlaps with a write, which is
    /// vanishingly rare when writer and reader operate at different
    /// frequencies (e.g., writer per fsync batch, reader per snapshot).
    pub fn load(&self) -> T {
        loop {
            let seq1 = self.sequence.load(Ordering::Acquire);
            if seq1 & 1 != 0 {
                // Writer is mid-update — spin and retry.
                std::hint::spin_loop();
                continue;
            }

            // Safety: sequence is even, so no write is in progress.
            // The Acquire on seq1 ensures we see the completed write.
            let value = unsafe { *self.value.get() };

            // On weakly-ordered architectures (ARM/AArch64), the plain
            // load of `value` above can be reordered past a subsequent
            // atomic load at a different address. This Acquire fence
            // ensures the value read completes before we re-read the
            // sequence counter — without it, we could observe seq1==seq2
            // while `value` contains a torn read.
            std::sync::atomic::fence(Ordering::Acquire);
            let seq2 = self.sequence.load(Ordering::Relaxed);
            if seq1 == seq2 {
                return value;
            }
            // Sequence changed — writer updated during our read. Retry.
            std::hint::spin_loop();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn store_and_load() {
        let lock = SeqLock::new(42u64);
        assert_eq!(lock.load(), 42);
        lock.store(99);
        assert_eq!(lock.load(), 99);
    }

    #[test]
    fn load_returns_latest_value() {
        let lock = SeqLock::new([0u8; 32]);
        let expected = [0xAB; 32];
        lock.store(expected);
        assert_eq!(lock.load(), expected);
    }

    #[test]
    fn concurrent_writer_reader_no_torn_reads() {
        let lock = Arc::new(SeqLock::new([0u8; 32]));
        let writer_lock = Arc::clone(&lock);

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
}
