//! Single-producer, single-consumer (SPSC) ring buffer.
//!
//! Simpler than the multi-consumer disruptor — no dependency chains,
//! just one producer and one consumer coordinated via two atomic counters.
//! Used for the output path (matching → response) where there's exactly
//! one writer and one reader.
//!
//! Counting model: `head` counts total items published, `tail` counts total
//! items consumed. Both start at 0. Available = head - tail. Slot index =
//! count & mask. No sentinel values or wrapping tricks.

use std::cell::UnsafeCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::padding::CachePadded;

/// Error returned when the SPSC queue is full.
#[derive(Debug, PartialEq, Eq)]
pub struct Full;

/// Shared state between producer and consumer.
struct Shared<T> {
    /// Slot array. Power-of-two length for bitmask indexing.
    slots: Box<[UnsafeCell<T>]>,
    /// Bitmask: capacity - 1.
    mask: u64,
    /// Total items published (producer writes, consumer reads).
    head: CachePadded<AtomicU64>,
    /// Total items consumed (consumer writes, producer reads).
    tail: CachePadded<AtomicU64>,
}

// Safety: producer only writes slots and head; consumer only reads slots and
// writes tail. No concurrent access to the same slot due to sequence coordination.
unsafe impl<T: Send> Send for Shared<T> {}
unsafe impl<T: Send> Sync for Shared<T> {}

/// Producer end of the SPSC queue.
pub struct Producer<T> {
    shared: Arc<Shared<T>>,
    /// Cached tail value to reduce atomic reads.
    cached_tail: u64,
    /// Slot index of the next write. Equals `shared.head + pending` where
    /// `pending` is the number of in-place writes accumulated since the
    /// last [`Self::flush`]. Tracked locally so producer-side advance is
    /// a relaxed register, not an atomic store.
    // u64 — sequence counter, never wraps in any realistic uptime.
    local_head: u64,
}

/// Consumer end of the SPSC queue.
pub struct Consumer<T> {
    shared: Arc<Shared<T>>,
    /// Cached head value to reduce atomic reads.
    cached_head: u64,
}

/// Create a new SPSC queue with the given capacity (must be power of two).
///
/// Returns `(Producer, Consumer)` to be moved to separate threads.
pub fn channel<T: Copy + Default>(capacity: usize) -> (Producer<T>, Consumer<T>) {
    assert!(
        capacity.is_power_of_two(),
        "capacity must be a power of two"
    );
    assert!(capacity >= 2, "capacity must be at least 2");

    let slots: Vec<UnsafeCell<T>> = (0..capacity)
        .map(|_| UnsafeCell::new(T::default()))
        .collect();

    let shared = Arc::new(Shared {
        slots: slots.into_boxed_slice(),
        mask: (capacity - 1) as u64,
        head: CachePadded::new(AtomicU64::new(0)),
        tail: CachePadded::new(AtomicU64::new(0)),
    });

    let producer = Producer {
        shared: Arc::clone(&shared),
        cached_tail: 0,
        local_head: 0,
    };

    let consumer = Consumer {
        shared,
        cached_head: 0,
    };

    (producer, consumer)
}

impl<T: Copy + Default> Producer<T> {
    /// Try to fill the next slot in place. Returns the sequence number on
    /// success. The write is **not** visible to the consumer until
    /// [`Self::flush`] (or [`Self::try_publish`]) executes the Release.
    ///
    /// Returns `Err(Full)` without invoking the closure if the ring cannot
    /// accommodate one more entry.
    ///
    /// This is the building block for batch publishes: the caller can
    /// accumulate many in-place writes — each just touches a slot, with
    /// no atomic store — and pay the Release cost once at the end.
    pub fn try_push_with<F: FnOnce(&mut T)>(&mut self, f: F) -> Result<u64, Full> {
        let seq = self.local_head;
        let capacity = self.shared.mask + 1;

        if seq - self.cached_tail >= capacity {
            self.cached_tail = self.shared.tail.get().load(Ordering::Acquire);
            if seq - self.cached_tail >= capacity {
                return Err(Full);
            }
        }

        let idx = (seq & self.shared.mask) as usize;
        // Safety: backpressure check above confirms the consumer is not
        // reading this slot, and the SPSC contract gives us the only writer.
        unsafe { f(&mut *self.shared.slots[idx].get()) };
        self.local_head = seq + 1;
        Ok(seq)
    }

    /// Blocking variant of [`Self::try_push_with`]. Spins until space is
    /// available, flushing pending writes mid-spin so the consumer can drain
    /// when the ring is saturated.
    pub fn push_with<F: FnOnce(&mut T)>(&mut self, f: F) -> u64 {
        let capacity = self.shared.mask + 1;
        loop {
            let seq = self.local_head;
            if seq - self.cached_tail < capacity {
                let idx = (seq & self.shared.mask) as usize;
                // Safety: as in `try_push_with`.
                unsafe { f(&mut *self.shared.slots[idx].get()) };
                self.local_head = seq + 1;
                return seq;
            }
            self.cached_tail = self.shared.tail.get().load(Ordering::Acquire);
            if seq - self.cached_tail < capacity {
                continue;
            }
            // No space: flush whatever is pending so the consumer can
            // advance the tail. Without this we'd deadlock against a
            // consumer that has already drained everything we've Released.
            self.flush();
            std::hint::spin_loop();
        }
    }

    /// Make all in-place writes accumulated since the last flush visible
    /// to the consumer with a single Release store on the head cursor.
    /// No-op when no writes are pending.
    #[inline]
    pub fn flush(&mut self) {
        let committed = self.shared.head.get().load(Ordering::Relaxed);
        if self.local_head > committed {
            // Release: consumer sees all in-place slot writes before the
            // updated head.
            self.shared
                .head
                .get()
                .store(self.local_head, Ordering::Release);
        }
    }

    /// Try to publish a value (write + immediate flush). Returns the
    /// sequence number, or `Err(Full)`.
    pub fn try_publish(&mut self, value: T) -> Result<u64, Full> {
        let seq = self.try_push_with(|slot| *slot = value)?;
        self.flush();
        Ok(seq)
    }

    /// Publish a value, spinning until space is available.
    pub fn publish(&mut self, value: T) -> u64 {
        let seq = self.push_with(|slot| *slot = value);
        self.flush();
        seq
    }
}

impl<T: Copy + Default> Consumer<T> {
    /// Try to read the next entry. Returns `None` if empty.
    pub fn try_consume(&mut self) -> Option<(u64, T)> {
        let tail = self.shared.tail.get().load(Ordering::Relaxed);

        if self.cached_head <= tail {
            // Re-read head in case producer has advanced.
            self.cached_head = self.shared.head.get().load(Ordering::Acquire);
            if self.cached_head <= tail {
                return None;
            }
        }

        let idx = (tail & self.shared.mask) as usize;
        // Safety: producer has written this slot and won't overwrite until we advance tail.
        let value = unsafe { *self.shared.slots[idx].get() };
        // Release store so producer sees our progress.
        self.shared.tail.get().store(tail + 1, Ordering::Release);
        Some((tail, value))
    }

    /// Read a batch of entries into `buf`. Returns the number read (up to `max`).
    pub fn consume_batch(&mut self, buf: &mut [T], max: usize) -> usize {
        let tail = self.shared.tail.get().load(Ordering::Relaxed);

        // Re-read head for latest count.
        self.cached_head = self.shared.head.get().load(Ordering::Acquire);
        let available = self.cached_head - tail;
        if available == 0 {
            return 0;
        }

        let count = available.min(max as u64).min(buf.len() as u64) as usize;
        for (i, slot) in buf.iter_mut().enumerate().take(count) {
            let idx = ((tail + i as u64) & self.shared.mask) as usize;
            // Safety: same as try_consume.
            *slot = unsafe { *self.shared.slots[idx].get() };
        }

        self.shared
            .tail
            .get()
            .store(tail + count as u64, Ordering::Release);
        count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_publish_consume() {
        let (mut producer, mut consumer) = channel::<u64>(4);

        producer.try_publish(10).unwrap();
        producer.try_publish(20).unwrap();

        assert_eq!(consumer.try_consume(), Some((0, 10)));
        assert_eq!(consumer.try_consume(), Some((1, 20)));
        assert_eq!(consumer.try_consume(), None);
    }

    #[test]
    fn full_buffer() {
        let (mut producer, mut consumer) = channel::<u64>(4);

        for i in 0..4 {
            assert!(producer.try_publish(i).is_ok());
        }
        assert_eq!(producer.try_publish(99), Err(Full));

        consumer.try_consume();
        assert!(producer.try_publish(99).is_ok());
    }

    #[test]
    fn wrap_around() {
        let (mut producer, mut consumer) = channel::<u64>(4);

        for i in 0..20u64 {
            producer.publish(i);
            let (seq, val) = consumer.try_consume().unwrap();
            assert_eq!(seq, i);
            assert_eq!(val, i);
        }
    }

    #[test]
    fn batch_consume() {
        let (mut producer, mut consumer) = channel::<u64>(16);

        for i in 0..8u64 {
            producer.publish(i * 10);
        }

        let mut buf = [0u64; 32];
        let count = consumer.consume_batch(&mut buf, 32);
        assert_eq!(count, 8);
        for (i, item) in buf.iter().enumerate().take(8) {
            assert_eq!(*item, i as u64 * 10);
        }
    }

    #[test]
    fn concurrent_spsc() {
        let (mut producer, mut consumer) = channel::<u64>(1024);
        let count = 100_000u64;

        let consumer_thread = std::thread::spawn(move || {
            let mut received = Vec::with_capacity(count as usize);
            loop {
                if let Some((_, val)) = consumer.try_consume() {
                    received.push(val);
                    if received.len() == count as usize {
                        break;
                    }
                } else {
                    std::hint::spin_loop();
                }
            }
            received
        });

        for i in 0..count {
            producer.publish(i);
        }

        let received = consumer_thread.join().unwrap();
        assert_eq!(received.len(), count as usize);
        for (i, val) in received.iter().enumerate() {
            assert_eq!(*val, i as u64);
        }
    }

    #[test]
    fn publish_returns_correct_sequence() {
        let (mut producer, _consumer) = channel::<u64>(8);
        assert_eq!(producer.publish(1), 0);
        assert_eq!(producer.publish(2), 1);
        assert_eq!(producer.publish(3), 2);
    }

    #[test]
    fn in_place_batch_invisible_until_flush() {
        // Pending writes must not leak to the consumer before flush.
        let (mut producer, mut consumer) = channel::<u64>(8);
        producer.try_push_with(|s| *s = 1).unwrap();
        producer.try_push_with(|s| *s = 2).unwrap();
        producer.try_push_with(|s| *s = 3).unwrap();
        assert!(consumer.try_consume().is_none(), "no flush — no visibility");

        producer.flush();
        assert_eq!(consumer.try_consume(), Some((0, 1)));
        assert_eq!(consumer.try_consume(), Some((1, 2)));
        assert_eq!(consumer.try_consume(), Some((2, 3)));
        assert_eq!(consumer.try_consume(), None);
    }

    #[test]
    fn try_push_with_full_does_not_invoke_closure() {
        let (mut producer, _consumer) = channel::<u64>(4);
        for _ in 0..4 {
            producer.try_push_with(|s| *s = 7).unwrap();
        }
        producer.flush();
        let mut called = false;
        let r = producer.try_push_with(|s| {
            called = true;
            *s = 99;
        });
        assert_eq!(r, Err(Full));
        assert!(!called, "closure must not run on Full");
    }

    #[test]
    fn flush_is_idempotent_and_zero_pending_is_noop() {
        let (mut producer, mut consumer) = channel::<u64>(4);
        producer.flush(); // no-op, no writes pending
        producer.try_push_with(|s| *s = 42).unwrap();
        producer.flush();
        producer.flush(); // second flush — also a no-op
        assert_eq!(consumer.try_consume(), Some((0, 42)));
    }

    #[test]
    fn push_with_internal_flush_breaks_full_deadlock() {
        // Ring of 8 — caller never explicitly flushes. The only way the
        // consumer ever sees a write is `push_with`'s own mid-spin flush
        // when it observes a full ring. This proves that path is wired
        // correctly: without it, the producer would write 8 slots, find
        // the ring full on the 9th, and spin forever because nothing
        // was Released for the consumer to drain.
        let (mut producer, mut consumer) = channel::<u64>(8);
        let consumer_thread = std::thread::spawn(move || {
            let mut received = Vec::with_capacity(20);
            loop {
                if let Some((_, v)) = consumer.try_consume() {
                    received.push(v);
                    if received.len() == 20 {
                        break;
                    }
                } else {
                    std::hint::spin_loop();
                }
            }
            received
        });

        for i in 0..20u64 {
            producer.push_with(|s| *s = i);
        }
        // Final flush for whatever the internal mid-spin flushes left
        // pending after the last write.
        producer.flush();
        let received = consumer_thread.join().unwrap();
        assert_eq!(received, (0..20u64).collect::<Vec<_>>());
    }

    #[test]
    fn try_push_with_panicking_closure_does_not_advance_cursor() {
        // If the caller's closure panics partway through filling a slot,
        // `local_head` must NOT advance — otherwise the next push would
        // skip the partial slot and the consumer would observe stale data.
        // The slot itself contains junk after a panic; correctness relies
        // on the next write reusing the same index and overwriting it.
        let (mut producer, mut consumer) = channel::<u64>(8);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = producer.try_push_with(|_| panic!("synthetic"));
        }));
        assert!(result.is_err());
        // Cursor unchanged: the next try_publish should land at seq 0.
        assert_eq!(producer.try_publish(42).unwrap(), 0);
        assert_eq!(consumer.try_consume(), Some((0, 42)));
    }

    #[test]
    fn try_publish_after_pending_writes_flushes_all() {
        // try_publish does write + flush. If there are pending in-place
        // writes from earlier try_push_with calls, they must be flushed
        // along with the new value.
        let (mut producer, mut consumer) = channel::<u64>(8);
        producer.try_push_with(|s| *s = 10).unwrap();
        producer.try_push_with(|s| *s = 20).unwrap();
        producer.try_publish(30).unwrap();
        assert_eq!(consumer.try_consume(), Some((0, 10)));
        assert_eq!(consumer.try_consume(), Some((1, 20)));
        assert_eq!(consumer.try_consume(), Some((2, 30)));
    }
}
