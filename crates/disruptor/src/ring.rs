//! Multi-consumer disruptor ring buffer.
//!
//! Supports both single-producer and multi-producer modes:
//!
//! - **Single-producer** (`Producer`): one writer, simple store-based publishing.
//! - **Multi-producer** (`MultiProducer`): N writers, CAS-based slot claiming
//!   with per-slot generation flags for consumer visibility (LMAX pattern).
//!
//! N consumers read from the ring buffer, each gated on a dependency (the
//! producer cursor or another consumer's cursor). This enables pipeline
//! topologies where consumer B only processes entries after consumer A
//! has finished with them.
//!
//! Counting model: the producer cursor and each consumer cursor track the
//! *next* sequence to publish/read. Both start at 0. Slot index = seq & mask.
//! The ring buffer size must be a power of two for bitmask indexing.

use std::cell::UnsafeCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};

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
    /// Producer cursor: total items published (single-producer) or total items
    /// claimed (multi-producer). Starts at 0.
    cursor: Sequence,
    /// Per-slot generation flags for multi-producer mode. Each slot stores
    /// `(seq >> shift) as i32` after the producer writes to it. Consumers scan
    /// this array to find the highest contiguous published sequence.
    /// `None` in single-producer mode (cursor is the published counter).
    /// Initialized to -1 (never published). Using `AtomicI32` without cache
    /// padding — false sharing on adjacent slots is acceptable since producers
    /// write to different slots concurrently.
    available: Option<Box<[AtomicI32]>>,
    /// log2(capacity) — used to compute generation: `seq >> shift`.
    /// Only meaningful when `available` is `Some`.
    shift: u32,
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
    /// Gated on a single-producer's cursor (directly readable as published count).
    Producer(Arc<Shared<T>>),
    /// Gated on a multi-producer. Must scan the available flags to find the
    /// highest contiguous published sequence.
    MultiProducer(Arc<Shared<T>>),
    /// Gated on another consumer's processed count.
    Consumer(Arc<Sequence>),
}

impl<T> DependencyKind<T> {
    /// Load the highest sequence this consumer is allowed to read up to.
    ///
    /// `from` is the consumer's current read position — used by multi-producer
    /// to scan the available flags from the consumer's position. Ignored in
    /// single-producer and consumer-dependency modes.
    fn load(&self, from: u64) -> u64 {
        match self {
            DependencyKind::Producer(shared) => shared.cursor.get().load(Ordering::Acquire),
            DependencyKind::MultiProducer(shared) => {
                // Scan the available flags to find the highest contiguous
                // published sequence starting from `from`. The cursor is the
                // upper bound (highest claimed), but some claimed slots may
                // not be published yet.
                let cursor = shared.cursor.get().load(Ordering::Acquire);
                let available = shared
                    .available
                    .as_ref()
                    .expect("MultiProducer requires available buffer");
                let shift = shared.shift;
                let mask = shared.buffer.mask;

                let mut seq = from;
                while seq < cursor {
                    let idx = (seq & mask) as usize;
                    let expected = (seq >> shift) as i32;
                    if available[idx].load(Ordering::Acquire) != expected {
                        break;
                    }
                    seq += 1;
                }
                seq
            }
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
}

/// Multi-producer end of the disruptor. Multiple threads can publish
/// concurrently without external synchronization.
///
/// Uses CAS-based slot claiming (LMAX multi-producer pattern):
/// 1. `fetch_add` on the cursor to claim a unique sequence
/// 2. Write the value to the claimed slot
/// 3. Set a per-slot generation flag so consumers know the slot is ready
///
/// `Clone + Send + Sync` — each reader thread gets its own clone.
pub struct MultiProducer<T> {
    shared: Arc<Shared<T>>,
    /// Sequences of all "gate" consumers (terminal consumers whose progress
    /// limits the producer). Read on every publish for backpressure.
    gates: Arc<[Arc<Sequence>]>,
}

impl<T> Clone for MultiProducer<T> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            gates: Arc::clone(&self.gates),
        }
    }
}

// Safety: MultiProducer uses only atomic operations for concurrent publishing.
// Slot writes are safe because each producer claims a unique sequence via CAS,
// and backpressure ensures consumers have moved past the claimed slot.
unsafe impl<T: Send> Send for MultiProducer<T> {}
unsafe impl<T: Send> Sync for MultiProducer<T> {}

impl<T: Copy + Default> MultiProducer<T> {
    /// Try to publish a value. Returns `Err(Full)` if all slots are occupied
    /// (consumers haven't caught up).
    ///
    /// Uses CAS to claim a unique sequence. Multiple threads can call this
    /// concurrently. On contention, the CAS retries until it succeeds or
    /// the ring is full.
    pub fn try_publish(&self, value: T) -> Result<u64, Full> {
        let capacity = self.shared.buffer.mask + 1;
        let available = self
            .shared
            .available
            .as_ref()
            .expect("MultiProducer requires available buffer");

        loop {
            let current = self.shared.cursor.get().load(Ordering::Relaxed);

            // Backpressure: can't write if we'd overwrite a slot consumers
            // haven't read. No caching — reads gates fresh each attempt.
            // With 2 gates, this is 2 atomic loads (cheap vs CAS cost).
            let min_gate = self
                .gates
                .iter()
                .map(|g| g.get().load(Ordering::Acquire))
                .min()
                .unwrap_or(0);

            // saturating_sub: `current` may be stale (another producer advanced
            // the cursor since our read), so `min_gate > current` is possible.
            // In that case, the ring is definitely not full.
            if current.saturating_sub(min_gate) >= capacity {
                return Err(Full);
            }

            // CAS to claim the slot. On failure (another producer claimed it
            // first), retry with the updated cursor value.
            match self.shared.cursor.get().compare_exchange_weak(
                current,
                current + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    // Claimed sequence `current`. Write the slot data.
                    // Safety: backpressure ensures consumers have moved past this
                    // slot, and no other producer claims the same sequence.
                    unsafe { self.shared.buffer.write(current, value) };

                    // Publish: set the generation flag so consumers know this slot
                    // is ready. Release ordering ensures consumers see the written
                    // data before the flag.
                    let idx = (current & self.shared.buffer.mask) as usize;
                    let generation = (current >> self.shared.shift) as i32;
                    available[idx].store(generation, Ordering::Release);

                    return Ok(current);
                }
                Err(_) => {
                    // CAS failed — another producer got there first.
                    // Hint to the CPU before retrying to reduce contention.
                    std::hint::spin_loop();
                }
            }
        }
    }

    /// Publish a value, spinning until space is available.
    pub fn publish(&self, value: T) -> u64 {
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
        self.cached_dep = self.dependency.load(self.next_read);
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
        self.cached_dep = self.dependency.load(self.next_read);

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
            available: None,
            shift: 0,
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

    /// Build a multi-producer disruptor. Returns `(MultiProducer, Vec<Consumer>)`.
    ///
    /// The `MultiProducer` is `Clone + Send + Sync` — each writer thread gets
    /// its own clone. No external synchronization (mutex) is needed.
    ///
    /// Internally allocates a per-slot generation flag array for consumer
    /// visibility tracking. With 1M slots at 4 bytes each, this is ~4 MiB.
    pub fn build_multi_producer(self) -> (MultiProducer<T>, Vec<Consumer<T>>) {
        // Rebuild shared with available buffer.
        let capacity = (self.shared.buffer.mask + 1) as usize;
        let shift = capacity.trailing_zeros();

        // Initialize all flags to -1 (never published).
        let available: Box<[AtomicI32]> = (0..capacity)
            .map(|_| AtomicI32::new(-1))
            .collect::<Vec<_>>()
            .into_boxed_slice();

        let shared = Arc::new(Shared {
            buffer: RingBuffer::new(capacity),
            cursor: CachePadded::new(AtomicU64::new(0)),
            available: Some(available),
            shift,
        });

        // Re-create builder state with the new shared.
        let builder = Self {
            shared: Arc::clone(&shared),
            consumers: self.consumers,
        };

        let gates = builder.collect_gates();
        let consumers =
            builder.build_consumers(|| DependencyKind::MultiProducer(Arc::clone(&shared)));

        let producer = MultiProducer {
            shared,
            gates: gates.into(),
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

    // --- Multi-producer tests ---

    #[test]
    fn multi_producer_basic_publish_consume() {
        let (producer, mut consumers) = DisruptorBuilder::<u64>::new(8)
            .add_consumer()
            .build_multi_producer();

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
    fn multi_producer_full_buffer() {
        let (producer, mut consumers) = DisruptorBuilder::<u64>::new(4)
            .add_consumer()
            .build_multi_producer();

        for i in 0..4 {
            assert!(producer.try_publish(i).is_ok());
        }
        assert_eq!(producer.try_publish(99), Err(Full));

        consumers[0].try_consume();
        assert!(producer.try_publish(99).is_ok());
    }

    #[test]
    fn multi_producer_wrap_around() {
        let (producer, mut consumers) = DisruptorBuilder::<u64>::new(4)
            .add_consumer()
            .build_multi_producer();

        for i in 0..20u64 {
            producer.publish(i);
            let (seq, val) = consumers[0].try_consume().unwrap();
            assert_eq!(seq, i);
            assert_eq!(val, i);
        }
    }

    #[test]
    fn multi_producer_batch_consume() {
        let (producer, mut consumers) = DisruptorBuilder::<u64>::new(16)
            .add_consumer()
            .build_multi_producer();

        for i in 0..10u64 {
            producer.publish(i * 100);
        }

        let mut buf = [0u64; 32];
        let count = consumers[0].consume_batch(&mut buf, 32);
        assert_eq!(count, 10);
        for i in 0..10 {
            assert_eq!(buf[i], i as u64 * 100);
        }
    }

    #[test]
    fn multi_producer_concurrent_two_producers() {
        let (producer, mut consumers) = DisruptorBuilder::<u64>::new(1024)
            .add_consumer()
            .build_multi_producer();

        let count_per_producer = 5_000u64;
        let total = count_per_producer * 2;

        let mut consumer = consumers.pop().unwrap();

        let p1 = producer.clone();
        let p2 = producer;

        // Producer 1: publishes odd values (1, 3, 5, ...)
        let t1 = std::thread::spawn(move || {
            for i in 0..count_per_producer {
                p1.publish(i * 2 + 1);
            }
        });

        // Producer 2: publishes even values (0, 2, 4, ...)
        let t2 = std::thread::spawn(move || {
            for i in 0..count_per_producer {
                p2.publish(i * 2);
            }
        });

        // Consumer: collect all values.
        let mut received = Vec::with_capacity(total as usize);
        loop {
            if let Some((_, val)) = consumer.try_consume() {
                received.push(val);
                if received.len() == total as usize {
                    break;
                }
            } else {
                std::hint::spin_loop();
            }
        }

        t1.join().unwrap();
        t2.join().unwrap();

        // All values should be present (order may vary due to concurrent publishing).
        assert_eq!(received.len(), total as usize);
        received.sort();
        for (i, val) in received.iter().enumerate() {
            assert_eq!(*val, i as u64);
        }
    }

    #[test]
    fn multi_producer_concurrent_many_producers() {
        let num_producers = 8;
        let count_per_producer = 1_000u64;
        let total = count_per_producer * num_producers as u64;

        let (producer, mut consumers) = DisruptorBuilder::<u64>::new(4096)
            .add_consumer()
            .build_multi_producer();

        let mut consumer = consumers.pop().unwrap();

        let handles: Vec<_> = (0..num_producers)
            .map(|p| {
                let prod = producer.clone();
                let offset = p as u64 * count_per_producer;
                std::thread::spawn(move || {
                    for i in 0..count_per_producer {
                        prod.publish(offset + i);
                    }
                })
            })
            .collect();

        let mut received = Vec::with_capacity(total as usize);
        loop {
            if let Some((_, val)) = consumer.try_consume() {
                received.push(val);
                if received.len() == total as usize {
                    break;
                }
            } else {
                std::hint::spin_loop();
            }
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(received.len(), total as usize);
        received.sort();
        for (i, val) in received.iter().enumerate() {
            assert_eq!(*val, i as u64);
        }
    }

    #[test]
    fn multi_producer_two_parallel_consumers() {
        let (producer, mut consumers) = DisruptorBuilder::<u64>::new(1024)
            .add_consumer()
            .add_consumer()
            .build_multi_producer();

        let count = 5_000u64;

        let mut c1 = consumers.pop().unwrap();
        let mut c0 = consumers.pop().unwrap();

        let t0 = std::thread::spawn(move || {
            let mut sum = 0u64;
            for _ in 0..count {
                loop {
                    if let Some((_, val)) = c0.try_consume() {
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
                    if let Some((_, val)) = c1.try_consume() {
                        sum += val;
                        break;
                    }
                    std::hint::spin_loop();
                }
            }
            sum
        });

        // Two producers publish concurrently.
        let p1 = producer.clone();
        let p2 = producer;
        let pt1 = std::thread::spawn(move || {
            for i in 0..count / 2 {
                p1.publish(i);
            }
        });
        let pt2 = std::thread::spawn(move || {
            for i in count / 2..count {
                p2.publish(i);
            }
        });

        pt1.join().unwrap();
        pt2.join().unwrap();

        let expected: u64 = (0..count).sum();
        assert_eq!(t0.join().unwrap(), expected);
        assert_eq!(t1.join().unwrap(), expected);
    }
}
