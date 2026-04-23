//! Single-producer multi-consumer disruptor ring buffer.
//!
//! One writer (`Producer`), N consumers. The producer publishes entries
//! with a plain release store on the cursor. Consumers read gated on a
//! dependency — either the producer's cursor or another consumer's
//! progress — enabling pipeline topologies where consumer B only
//! processes entries after consumer A has finished with them.
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

    /// Mutable slot reference for in-place construction.
    ///
    /// # Safety
    /// The caller must guarantee no other thread is reading or writing this slot.
    // Standard UnsafeCell interior-mutability pattern — the &mut T is
    // minted from &self through the UnsafeCell. Producer/consumer
    // coordination via the atomic sequences keeps this sound.
    #[allow(clippy::mut_from_ref)]
    unsafe fn slot_mut(&self, sequence: u64) -> &mut T {
        let idx = (sequence & self.mask) as usize;
        unsafe { &mut *self.slots[idx].get() }
    }
}

/// Shared state between the producer and all consumers.
struct Shared<T> {
    buffer: RingBuffer<T>,
    /// Producer cursor: total items published. Starts at 0.
    cursor: Sequence,
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

/// A consumer's dependency is either the producer (reads the cursor)
/// or another consumer (reads that consumer's `processed`).
enum DependencyKind<T> {
    /// Gated on the producer's cursor (directly readable as published count).
    Producer(Arc<Shared<T>>),
    /// Gated on another consumer's processed count.
    Consumer(Arc<Sequence>),
}

impl<T> DependencyKind<T> {
    /// Load the highest sequence this consumer is allowed to read up to.
    fn load(&self) -> u64 {
        match self {
            DependencyKind::Producer(shared) => shared.cursor.get().load(Ordering::Acquire),
            DependencyKind::Consumer(seq) => seq.get().load(Ordering::Acquire),
        }
    }
}

impl<T: Copy + Default> Producer<T> {
    /// Try to publish a value. Returns `Err(Full)` if all slots are occupied
    /// (consumers haven't caught up).
    pub fn try_publish(&mut self, value: T) -> Result<u64, Full> {
        let seq = self.shared.cursor.get().load(Ordering::Relaxed);
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
        // Release: consumers reading cursor will see the written data.
        self.shared.cursor.get().store(seq + 1, Ordering::Release);
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

    /// Publish by filling the next slot in place. Spins until space is
    /// available, then runs `f(&mut slot)` directly on the ring entry —
    /// avoiding the byte-copy `publish`/`try_publish` perform when given
    /// a `T` by value.
    ///
    /// Hot paths publishing large `InputSlot`-sized entries should prefer
    /// this API: at 10M orders/sec a ~100-byte per-publish memcpy shows up
    /// as ~30% of the ingest core in `perf annotate` (SSE `movdqu`/`movdqa`
    /// pairs). Writing fields directly into the slot removes the copy.
    ///
    /// The Release store on the cursor orders all writes performed by `f`
    /// before consumers observe the advanced cursor.
    pub fn publish_with<F: FnOnce(&mut T)>(&mut self, f: F) -> u64 {
        let capacity = self.shared.buffer.mask + 1;
        // Spin until space is available (single-producer: seq doesn't move
        // underneath us).
        loop {
            let seq = self.shared.cursor.get().load(Ordering::Relaxed);
            if seq - self.cached_gate_min < capacity {
                // Safety: backpressure check confirmed no consumer is reading
                // this slot; single-producer → no concurrent writer.
                let slot = unsafe { self.shared.buffer.slot_mut(seq) };
                f(slot);
                // Release: consumers see the slot writes before the cursor.
                self.shared.cursor.get().store(seq + 1, Ordering::Release);
                return seq;
            }
            // Re-read gate sequences before spinning.
            let mut min = u64::MAX;
            for gate in &self.gates {
                let g = gate.get().load(Ordering::Acquire);
                if g < min {
                    min = g;
                }
            }
            self.cached_gate_min = min;
            if seq - min >= capacity {
                std::hint::spin_loop();
            }
        }
    }

    /// Check if the ring has space for one more entry without publishing.
    ///
    /// Returns `Ok(seq)` with the sequence that the next `publish_claimed`
    /// will use. Returns `Err(Full)` if backpressured.
    ///
    /// Single-producer only. The caller can use `seq` to pre-write data
    /// into a side buffer, then call `publish_claimed` to make it visible.
    pub fn try_claim(&mut self) -> Result<u64, Full> {
        let seq = self.shared.cursor.get().load(Ordering::Relaxed);
        let capacity = self.shared.buffer.mask + 1;

        if seq - self.cached_gate_min >= capacity {
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
        Ok(seq)
    }

    /// Publish a value at a previously claimed sequence.
    ///
    /// Must be called after a successful `try_claim()`. The caller should
    /// have written any side-buffer data between `try_claim` and this call.
    /// The Release store on the cursor ensures all prior writes (including
    /// side-buffer writes) are visible to consumers.
    ///
    /// # Safety contract
    /// The `seq` must be the value returned by the most recent `try_claim`.
    /// No other `try_publish` or `publish_claimed` may have been called
    /// between `try_claim` and this call.
    pub fn publish_claimed(&mut self, seq: u64, value: T) {
        // Safety: try_claim verified the slot is free.
        unsafe { self.shared.buffer.write(seq, value) };
        // Release: consumers see both the slot data AND any prior writes
        // (e.g., side-buffer data written between try_claim and this call).
        self.shared.cursor.get().store(seq + 1, Ordering::Release);
    }

    /// Peek at the current cursor value (next sequence to be published).
    ///
    /// Only meaningful for single-producer use. The returned sequence is
    /// the slot that will be written by the next successful `try_publish`.
    pub fn peek_cursor(&self) -> u64 {
        self.shared.cursor.get().load(Ordering::Relaxed)
    }

    /// Capacity of the ring buffer.
    pub fn capacity(&self) -> u64 {
        self.shared.buffer.mask + 1
    }

    /// Returns a type-erased handle for reading the producer cursor.
    pub fn cursor_reader(&self) -> Box<dyn QueueCursor>
    where
        T: Send + 'static,
    {
        Box::new(SharedCursor(Arc::clone(&self.shared)))
    }
}

/// Read-only handle to a disruptor producer cursor for monitoring.
/// Type-erased so monitoring code doesn't depend on pipeline slot types.
pub trait QueueCursor: Send + Sync {
    /// Load the current cursor value (total items published).
    fn load(&self) -> u64;
}

/// Type-erased wrapper around `Arc<Shared<T>>` for reading the producer cursor.
/// One Box allocation at creation; one virtual dispatch + one atomic read per call.
struct SharedCursor<T>(Arc<Shared<T>>);

// Safety: SharedCursor only reads the atomic cursor — no access to buffer slots.
// Shared<T> has `unsafe impl Send + Sync for T: Send`.
unsafe impl<T: Send> Send for SharedCursor<T> {}
unsafe impl<T: Send> Sync for SharedCursor<T> {}

impl<T: Send> QueueCursor for SharedCursor<T> {
    fn load(&self) -> u64 {
        self.0.cursor.get().load(Ordering::Relaxed)
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
    /// For consumers that need to defer cursor advancement (e.g., the journal
    /// stage must fsync before signaling downstream), use [`read_batch`] +
    /// [`commit`] instead.
    pub fn consume_batch(&mut self, buf: &mut [T], max: usize) -> usize {
        let count = self.read_batch(buf, max);
        if count > 0 {
            self.commit(count);
        }
        count
    }

    /// Read a batch of entries **without** advancing the progress counter.
    ///
    /// The entries are copied into `buf` and `next_read` advances internally,
    /// but downstream consumers won't see the progress until [`commit`] is
    /// called. This is critical for the journal stage: it must fsync before
    /// signaling the matching stage that entries are durable.
    ///
    /// Returns the number of entries read (up to `max` and `buf.len()`).
    pub fn read_batch(&mut self, buf: &mut [T], max: usize) -> usize {
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
        count
    }

    /// Advance the progress counter by `count` entries, making them visible
    /// to downstream consumers and the producer (for backpressure).
    ///
    /// Must be called after [`read_batch`] once the entries have been
    /// durably processed (e.g., after fsync).
    pub fn commit(&mut self, _count: usize) {
        // Release store so downstream consumers see our progress.
        self.processed
            .get()
            .store(self.next_read, Ordering::Release);
    }

    /// Set the progress counter to an explicit sequence number.
    ///
    /// Unlike [`commit`] which publishes `next_read`, this publishes an
    /// arbitrary sequence. Used by the io_uring journal stage to commit
    /// only the events covered by a completed fsync, while `next_read`
    /// may have advanced further during the async fsync wait.
    pub fn set_progress(&self, seq: u64) {
        self.processed.get().store(seq, Ordering::Release);
    }

    /// Fast-forward this consumer past any unread entries so it is
    /// positioned at the producer's current cursor.
    ///
    /// Used when a consumer has been disconnected from its external
    /// work queue (e.g., a replica was evicted) and unread ring entries
    /// are no longer semantically valid — replaying them would deliver
    /// stale data to whatever the consumer is rewired to next.
    /// `next_read` and the published `processed` counter are set in
    /// lock-step so downstream gates (and the producer's backpressure
    /// check) see a consistent up-to-date cursor.
    pub fn skip_to_dependency(&mut self) {
        let dep = self.dependency.load();
        self.next_read = dep;
        self.cached_dep = dep;
        self.processed.get().store(dep, Ordering::Release);
    }

    /// Returns a shared reference to this consumer's progress counter.
    ///
    /// External code (e.g., the response stage) can read this to determine
    /// how far this consumer has progressed, enabling out-of-band gating
    /// without a direct disruptor dependency.
    pub fn progress_counter(&self) -> Arc<Sequence> {
        Arc::clone(&self.processed)
    }

    /// Current read position (next sequence to be read).
    ///
    /// Used by the io_uring journal stage to snapshot the sequence after
    /// encoding a batch, so it knows which position the in-flight fsync
    /// covers.
    pub fn next_read(&self) -> u64 {
        self.next_read
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
            cursor: CachePadded::new(AtomicU64::new(0)),
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

    /// Identify terminal consumers and collect their progress counters as gates.
    fn collect_gates(&self) -> Vec<Arc<Sequence>> {
        let depended_on: Vec<usize> = self.consumers.iter().filter_map(|(_, dep)| *dep).collect();
        let mut gates = Vec::new();
        for (i, (seq, _)) in self.consumers.iter().enumerate() {
            if !depended_on.contains(&i) {
                gates.push(Arc::clone(seq));
            }
        }
        gates
    }

    /// Build consumers with the given dependency kind for producer-gated consumers.
    fn build_consumers(&self, producer_dep: impl Fn() -> DependencyKind<T>) -> Vec<Consumer<T>> {
        self.consumers
            .iter()
            .map(|(seq, dep_idx)| {
                let dependency = match dep_idx {
                    None => producer_dep(),
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
            .collect()
    }

    /// Build a single-producer disruptor. Returns `(Producer, Vec<Consumer>)`.
    ///
    /// The producer is gated on all terminal consumers (those no other consumer
    /// depends on) for backpressure.
    pub fn build(self) -> (Producer<T>, Vec<Consumer<T>>) {
        let gates = self.collect_gates();
        let consumers = self.build_consumers(|| DependencyKind::Producer(Arc::clone(&self.shared)));

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
        for (i, item) in buf.iter().enumerate().take(10) {
            assert_eq!(*item, i as u64 * 100);
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

    #[test]
    fn publish_with_fills_in_place() {
        let (mut producer, mut consumers) = DisruptorBuilder::<u64>::new(4).add_consumer().build();

        assert_eq!(producer.publish_with(|slot| *slot = 111), 0);
        assert_eq!(producer.publish_with(|slot| *slot = 222), 1);
        assert_eq!(producer.publish_with(|slot| *slot = 333), 2);

        assert_eq!(consumers[0].try_consume(), Some((0, 111)));
        assert_eq!(consumers[0].try_consume(), Some((1, 222)));
        assert_eq!(consumers[0].try_consume(), Some((2, 333)));
    }

    #[test]
    fn publish_with_blocks_and_resumes_after_consume() {
        // 4-slot ring with one consumer — producer is gated on that consumer.
        let (mut producer, mut consumers) = DisruptorBuilder::<u64>::new(4).add_consumer().build();

        for i in 0..4u64 {
            producer.publish_with(|slot| *slot = i);
        }

        // Ring is full. Drain one slot on a helper thread so the producer
        // can resume. Use a thread because publish_with spins.
        let mut consumer = consumers.pop().unwrap();
        let t = std::thread::spawn(move || {
            // Give the producer a moment to enter its spin.
            std::thread::sleep(std::time::Duration::from_millis(20));
            consumer.try_consume().unwrap();
            consumer
        });

        let seq = producer.publish_with(|slot| *slot = 99);
        assert_eq!(seq, 4);
        let mut consumer = t.join().unwrap();

        // Consumer already popped seq=0 (10) above.
        assert_eq!(consumer.try_consume(), Some((1, 1)));
        assert_eq!(consumer.try_consume(), Some((2, 2)));
        assert_eq!(consumer.try_consume(), Some((3, 3)));
        assert_eq!(consumer.try_consume(), Some((4, 99)));
    }
}
