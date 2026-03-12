//! Multi-consumer disruptor ring buffer.
//!
//! One producer publishes to a shared ring buffer. N consumers read from it,
//! each gated on a dependency (the producer cursor or another consumer's cursor).
//! This enables pipeline topologies where consumer B only processes entries
//! after consumer A has finished with them.
//!
//! Counting model: the producer cursor and each consumer cursor track the
//! *next* sequence to publish/read. Both start at 0. Slot index = seq & mask.
//! The ring buffer size must be a power of two for bitmask indexing.

use std::cell::UnsafeCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::padding::{CachePadded, Sequence};

/// Error returned when the ring buffer is full and the producer cannot publish.
#[derive(Debug, PartialEq, Eq)]
pub struct Full;

/// Shared ring buffer storage. Passive — does not track cursors.
///
/// Uses `UnsafeCell` for interior mutability: the producer writes slots,
/// consumers read them, coordination is handled by atomic sequences external
/// to this struct. `Box<[UnsafeCell<T>]>` is heap-allocated once at creation
/// and never reallocated.
struct RingBuffer<T> {
    /// Slot array. Power-of-two length for bitmask indexing.
    slots: Box<[UnsafeCell<T>]>,
    /// Bitmask for converting sequence → slot index (capacity - 1).
    mask: u64,
}

// Safety: slots are only accessed through sequence-coordinated producer/consumer
// protocol. The producer writes a slot only after confirming consumers have
// moved past it. Consumers read only after the producer has advanced past it.
unsafe impl<T: Send> Send for RingBuffer<T> {}
unsafe impl<T: Send> Sync for RingBuffer<T> {}

impl<T: Copy + Default> RingBuffer<T> {
    fn new(capacity: usize) -> Self {
        assert!(
            capacity.is_power_of_two(),
            "capacity must be a power of two"
        );
        assert!(capacity >= 2, "capacity must be at least 2");

        let slots: Vec<UnsafeCell<T>> = (0..capacity)
            .map(|_| UnsafeCell::new(T::default()))
            .collect();

        Self {
            slots: slots.into_boxed_slice(),
            mask: (capacity - 1) as u64,
        }
    }

    /// Write a value into the slot at `sequence`.
    ///
    /// # Safety
    /// The caller must guarantee no other thread is reading or writing this slot.
    unsafe fn write(&self, sequence: u64, value: T) {
        let idx = (sequence & self.mask) as usize;
        unsafe { *self.slots[idx].get() = value };
    }

    /// Read the value from the slot at `sequence`.
    ///
    /// # Safety
    /// The caller must guarantee the slot has been written and won't be overwritten.
    unsafe fn read(&self, sequence: u64) -> T {
        let idx = (sequence & self.mask) as usize;
        unsafe { *self.slots[idx].get() }
    }
}

/// Shared state between the producer and all consumers.
struct Shared<T> {
    buffer: RingBuffer<T>,
    /// Producer cursor: total items published. Starts at 0.
    /// Consumers read this to know how far the producer has advanced.
    published: Sequence,
}

/// Producer end of the disruptor. Publishes entries to the ring buffer.
///
/// Gated on the slowest consumer to prevent overwriting unread entries
/// (backpressure). Only one producer is supported.
pub struct Producer<T> {
    shared: Arc<Shared<T>>,
    /// Sequences of all "gate" consumers (terminal consumers whose progress
    /// limits the producer). The producer cannot advance more than `capacity`
    /// ahead of the minimum gate sequence.
    gates: Vec<Arc<Sequence>>,
    /// Cached minimum gate value to avoid reading atomics on every publish.
    cached_gate_min: u64,
}

/// Consumer end of the disruptor. Reads entries from the ring buffer.
///
/// Each consumer has its own progress counter and is gated on a dependency
/// counter (either the producer's published count or another consumer's
/// processed count).
pub struct Consumer<T> {
    shared: Arc<Shared<T>>,
    /// This consumer's processed count: how many entries it has finished.
    /// Other consumers or the producer may read this to gate their progress.
    processed: Arc<Sequence>,
    /// The dependency's progress counter. This consumer must not read past it.
    dependency: DependencyKind<T>,
    /// Next sequence this consumer will read.
    next_read: u64,
    /// Cached dependency value to reduce atomic reads.
    cached_dep: u64,
}

/// A consumer's dependency is either the producer (reads `shared.published`)
/// or another consumer (reads that consumer's `processed`).
enum DependencyKind<T> {
    /// Gated on the producer's published count.
    Producer(Arc<Shared<T>>),
    /// Gated on another consumer's processed count.
    Consumer(Arc<Sequence>),
}

impl<T> DependencyKind<T> {
    fn load(&self) -> u64 {
        match self {
            DependencyKind::Producer(shared) => shared.published.get().load(Ordering::Acquire),
            DependencyKind::Consumer(seq) => seq.get().load(Ordering::Acquire),
        }
    }
}

impl<T: Copy + Default> Producer<T> {
    /// Try to publish a value. Returns `Err(Full)` if all slots are occupied
    /// (consumers haven't caught up).
    pub fn try_publish(&mut self, value: T) -> Result<u64, Full> {
        let seq = self.shared.published.get().load(Ordering::Relaxed);
        let capacity = self.shared.buffer.mask + 1;

        // Backpressure: can't write if we'd overwrite a slot consumers haven't read.
        if seq - self.cached_gate_min >= capacity {
            // Re-read all gate sequences.
            let mut min = u64::MAX;
            for gate in &self.gates {
                let g = gate.get().load(Ordering::Acquire);
                if g < min {
                    min = g;
                }
            }
            self.cached_gate_min = min;
            if seq - min >= capacity {
                return Err(Full);
            }
        }

        // Safety: backpressure check ensures no consumer is reading this slot.
        unsafe { self.shared.buffer.write(seq, value) };
        // Release: consumers reading published will see the written data.
        self.shared
            .published
            .get()
            .store(seq + 1, Ordering::Release);
        Ok(seq)
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
    /// Try to read the next entry. Returns `None` if no new entry is available.
    pub fn try_consume(&mut self) -> Option<(u64, T)> {
        if self.available() == 0 {
            return None;
        }

        let seq = self.next_read;
        // Safety: dependency has advanced past this sequence.
        let value = unsafe { self.shared.buffer.read(seq) };
        self.next_read = seq + 1;
        // Release: producer/upstream consumers see our progress.
        self.processed.get().store(seq + 1, Ordering::Release);
        Some((seq, value))
    }

    /// Read a batch of entries. Returns the number of entries read (up to `max`
    /// and `buf.len()`). Advances the consumer's progress counter once for the batch.
    ///
    /// Batch consumption is critical for the journal stage: one `sync_data()`
    /// call covers all entries in the batch.
    pub fn consume_batch(&mut self, buf: &mut [T], max: usize) -> usize {
        // Always re-read dependency for batch operations.
        self.cached_dep = self.dependency.load();
        let available = self.cached_dep.saturating_sub(self.next_read);
        if available == 0 {
            return 0;
        }

        let count = available.min(max as u64).min(buf.len() as u64) as usize;
        for (i, slot) in buf.iter_mut().enumerate().take(count) {
            let seq = self.next_read + i as u64;
            // Safety: dependency guarantees slot is valid.
            *slot = unsafe { self.shared.buffer.read(seq) };
        }

        self.next_read += count as u64;
        // Single release store for the whole batch.
        self.processed
            .get()
            .store(self.next_read, Ordering::Release);
        count
    }

    /// Number of entries available to read.
    fn available(&mut self) -> u64 {
        // Fast path: use cached dependency value.
        if self.cached_dep > self.next_read {
            return self.cached_dep - self.next_read;
        }

        // Slow path: re-read dependency.
        self.cached_dep = self.dependency.load();

        self.cached_dep.saturating_sub(self.next_read)
    }
}

/// Builder for constructing a disruptor with a producer and multiple consumers
/// in a dependency chain.
pub struct DisruptorBuilder<T: Copy + Default> {
    shared: Arc<Shared<T>>,
    /// Each entry: (consumer_processed_counter, dependency_index)
    /// dependency_index: None = gated on producer, Some(i) = gated on consumer i
    consumers: Vec<(Arc<Sequence>, Option<usize>)>,
}

impl<T: Copy + Default> DisruptorBuilder<T> {
    /// Create a new builder with the given ring buffer capacity (must be power of two).
    pub fn new(capacity: usize) -> Self {
        let shared = Arc::new(Shared {
            buffer: RingBuffer::new(capacity),
            published: CachePadded::new(AtomicU64::new(0)),
        });

        Self {
            shared,
            consumers: Vec::new(),
        }
    }

    /// Add a consumer gated on the producer (reads directly after publish).
    pub fn add_consumer(mut self) -> Self {
        let seq = Arc::new(CachePadded::new(AtomicU64::new(0)));
        self.consumers.push((seq, None));
        self
    }

    /// Add a consumer gated on a previously added consumer (by index).
    pub fn add_consumer_after(mut self, dependency_index: usize) -> Self {
        assert!(
            dependency_index < self.consumers.len(),
            "dependency index out of bounds"
        );
        let seq = Arc::new(CachePadded::new(AtomicU64::new(0)));
        self.consumers.push((seq, Some(dependency_index)));
        self
    }

    /// Build the disruptor, returning the producer and consumers (in order added).
    ///
    /// The producer is gated on all terminal consumers (those no other consumer
    /// depends on) for backpressure.
    pub fn build(self) -> (Producer<T>, Vec<Consumer<T>>) {
        // Identify terminal consumers (no other consumer depends on them).
        let depended_on: Vec<usize> = self.consumers.iter().filter_map(|(_, dep)| *dep).collect();

        let mut gates = Vec::new();
        for (i, (seq, _)) in self.consumers.iter().enumerate() {
            if !depended_on.contains(&i) {
                gates.push(Arc::clone(seq));
            }
        }

        let consumers: Vec<Consumer<T>> = self
            .consumers
            .iter()
            .map(|(seq, dep_idx)| {
                let dependency = match dep_idx {
                    None => DependencyKind::Producer(Arc::clone(&self.shared)),
                    Some(idx) => DependencyKind::Consumer(Arc::clone(&self.consumers[*idx].0)),
                };
                Consumer {
                    shared: Arc::clone(&self.shared),
                    processed: Arc::clone(seq),
                    dependency,
                    next_read: 0,
                    cached_dep: 0,
                }
            })
            .collect();

        let producer = Producer {
            shared: Arc::clone(&self.shared),
            gates,
            cached_gate_min: 0,
        };

        (producer, consumers)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_consumer_publish_consume() {
        let (mut producer, mut consumers) = DisruptorBuilder::<u64>::new(4).add_consumer().build();

        assert_eq!(consumers.len(), 1);

        assert_eq!(producer.try_publish(10).unwrap(), 0);
        assert_eq!(producer.try_publish(20).unwrap(), 1);
        assert_eq!(producer.try_publish(30).unwrap(), 2);

        let c = &mut consumers[0];
        assert_eq!(c.try_consume(), Some((0, 10)));
        assert_eq!(c.try_consume(), Some((1, 20)));
        assert_eq!(c.try_consume(), Some((2, 30)));
        assert_eq!(c.try_consume(), None);
    }

    #[test]
    fn full_buffer_returns_error() {
        let (mut producer, mut consumers) = DisruptorBuilder::<u64>::new(4).add_consumer().build();

        for i in 0..4 {
            assert!(producer.try_publish(i).is_ok());
        }

        assert_eq!(producer.try_publish(99), Err(Full));

        consumers[0].try_consume();
        assert!(producer.try_publish(99).is_ok());
    }

    #[test]
    fn wrap_around() {
        let (mut producer, mut consumers) = DisruptorBuilder::<u64>::new(4).add_consumer().build();

        for i in 0..20u64 {
            producer.publish(i);
            let (seq, val) = consumers[0].try_consume().unwrap();
            assert_eq!(seq, i);
            assert_eq!(val, i);
        }
    }

    #[test]
    fn batch_consume() {
        let (mut producer, mut consumers) = DisruptorBuilder::<u64>::new(16).add_consumer().build();

        for i in 0..10u64 {
            producer.publish(i * 100);
        }

        let mut buf = [0u64; 32];
        let count = consumers[0].consume_batch(&mut buf, 32);
        assert_eq!(count, 10);
        for i in 0..10 {
            assert_eq!(buf[i], i as u64 * 100);
        }

        assert_eq!(consumers[0].consume_batch(&mut buf, 32), 0);
    }

    #[test]
    fn batch_consume_limited_by_max() {
        let (mut producer, mut consumers) = DisruptorBuilder::<u64>::new(16).add_consumer().build();

        for i in 0..10u64 {
            producer.publish(i);
        }

        let mut buf = [0u64; 32];
        let count = consumers[0].consume_batch(&mut buf, 5);
        assert_eq!(count, 5);

        let count = consumers[0].consume_batch(&mut buf, 32);
        assert_eq!(count, 5);
    }

    #[test]
    fn two_consumers_chained() {
        let (mut producer, mut consumers) = DisruptorBuilder::<u64>::new(8)
            .add_consumer()
            .add_consumer_after(0)
            .build();

        producer.publish(42);
        producer.publish(43);

        // Consumer 1 can't read — consumer 0 hasn't processed anything.
        assert_eq!(consumers[1].try_consume(), None);

        // Consumer 0 reads.
        assert_eq!(consumers[0].try_consume(), Some((0, 42)));

        // Now consumer 1 can read seq 0.
        assert_eq!(consumers[1].try_consume(), Some((0, 42)));
        // But not seq 1 — consumer 0 hasn't consumed it.
        assert_eq!(consumers[1].try_consume(), None);

        assert_eq!(consumers[0].try_consume(), Some((1, 43)));
        assert_eq!(consumers[1].try_consume(), Some((1, 43)));
    }

    #[test]
    fn producer_gated_on_terminal_consumer() {
        let (mut producer, mut consumers) = DisruptorBuilder::<u64>::new(4)
            .add_consumer()
            .add_consumer_after(0)
            .build();

        for i in 0..4u64 {
            producer.publish(i);
        }

        // Consumer 0 reads all, but consumer 1 (terminal) hasn't.
        for _ in 0..4 {
            consumers[0].try_consume();
        }

        assert_eq!(producer.try_publish(99), Err(Full));

        // Consumer 1 reads one.
        consumers[1].try_consume();

        assert!(producer.try_publish(99).is_ok());
    }

    #[test]
    fn concurrent_publish_consume() {
        let (mut producer, mut consumers) =
            DisruptorBuilder::<u64>::new(1024).add_consumer().build();

        let mut consumer = consumers.pop().unwrap();
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
    fn concurrent_chained_consumers() {
        let (mut producer, mut consumers) = DisruptorBuilder::<u64>::new(1024)
            .add_consumer()
            .add_consumer_after(0)
            .build();

        let count = 50_000u64;
        let mut consumer1 = consumers.pop().unwrap();
        let mut consumer0 = consumers.pop().unwrap();

        let t0 = std::thread::spawn(move || {
            let mut sum = 0u64;
            for _ in 0..count {
                loop {
                    if let Some((_, val)) = consumer0.try_consume() {
                        sum += val;
                        break;
                    }
                    std::hint::spin_loop();
                }
            }
            sum
        });

        let t1 = std::thread::spawn(move || {
            let mut sum = 0u64;
            for _ in 0..count {
                loop {
                    if let Some((_, val)) = consumer1.try_consume() {
                        sum += val;
                        break;
                    }
                    std::hint::spin_loop();
                }
            }
            sum
        });

        for i in 0..count {
            producer.publish(i);
        }

        let expected: u64 = (0..count).sum();
        assert_eq!(t0.join().unwrap(), expected);
        assert_eq!(t1.join().unwrap(), expected);
    }

    #[test]
    #[should_panic(expected = "capacity must be a power of two")]
    fn non_power_of_two_panics() {
        DisruptorBuilder::<u64>::new(3).add_consumer().build();
    }

    #[test]
    fn publish_returns_correct_sequence() {
        let (mut producer, _consumers) = DisruptorBuilder::<u64>::new(8).add_consumer().build();

        assert_eq!(producer.publish(1), 0);
        assert_eq!(producer.publish(2), 1);
        assert_eq!(producer.publish(3), 2);
    }
}
