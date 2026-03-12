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
    };

    let consumer = Consumer {
        shared,
        cached_head: 0,
    };

    (producer, consumer)
}

impl<T: Copy + Default> Producer<T> {
    /// Try to publish a value. Returns the sequence number, or `Err(Full)`.
    pub fn try_publish(&mut self, value: T) -> Result<u64, Full> {
        let head = self.shared.head.get().load(Ordering::Relaxed);
        let capacity = self.shared.mask + 1;

        // Check if buffer is full.
        if head - self.cached_tail >= capacity {
            // Re-read tail in case consumer has advanced.
            self.cached_tail = self.shared.tail.get().load(Ordering::Acquire);
            if head - self.cached_tail >= capacity {
                return Err(Full);
            }
        }

        let idx = (head & self.shared.mask) as usize;
        // Safety: consumer won't read this slot until we advance head.
        unsafe { *self.shared.slots[idx].get() = value };
        // Release store so consumer sees the written data.
        self.shared.head.get().store(head + 1, Ordering::Release);
        Ok(head)
    }

    /// Publish a value, spinning until space is available.
    pub fn publish(&mut self, value: T) -> u64 {
        loop {
            match self.try_publish(value) {
                Ok(seq) => return seq,
                Err(Full) => std::hint::spin_loop(),
            }
        }
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
        for i in 0..8 {
            assert_eq!(buf[i], i as u64 * 10);
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
}
