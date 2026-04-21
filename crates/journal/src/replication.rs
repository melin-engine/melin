//! Replication ring buffer — lock-free, pre-allocated byte batch transfer
//! from the journal stage to one or more replication sender threads.
//!
//! Uses the generic disruptor ring for sequencing and backpressure, with a
//! side array of pre-allocated 128 KiB byte buffers indexed by `seq & mask`.
//! The journal stage copies batch bytes into the buffer, then publishes the
//! metadata slot. Consumers read the metadata and reference the buffer
//! directly (zero-copy read).
//!
//! This design avoids heap allocation on the journal thread. The only cost
//! is a memcpy into the pre-allocated buffer (~80 KiB per batch).

use std::cell::UnsafeCell;
use std::sync::Arc;

use melin_disruptor::padding::Sequence;

/// Maximum batch buffer size. Matches `BATCH_BUF_CAPACITY` in writer.rs.
/// Each ring slot has one pre-allocated chunk of this size.
const CHUNK_SIZE: usize = 512 * 1024;

/// Capacity of the replication ring (number of batch slots).
/// 2^8 = 256 slots. At 512 KiB per slot, total buffer = 128 MiB per
/// ring (256 MiB for dual replication). Provides ~156ms of buffering
/// at 1635 batches/sec (6.7M events/sec ÷ 4096 events/batch), enough
/// to absorb TCP send latency without evicting replicas during bursts.
pub const REPLICATION_RING_CAPACITY: usize = 1 << 8;

/// Metadata for one replication batch. Small and `Copy` — carried in the
/// disruptor ring slot. The actual byte data lives in the shared buffer
/// array at the same ring index.
#[derive(Debug, Clone, Copy, Default)]
pub struct ReplicationMeta {
    /// Number of valid bytes in the corresponding buffer chunk.
    pub len: u32,
    /// Sequence number of the last journal entry in this batch.
    pub end_sequence: u64,
}

/// Shared pre-allocated byte buffers, one per ring slot.
///
/// Thread safety: the disruptor protocol ensures mutual exclusion.
/// The producer writes to slot N only after all consumers have advanced
/// past N (backpressure). Consumers read slot N only after the producer
/// has published it (cursor gating). No concurrent access occurs.
struct SharedBuffers {
    chunks: Box<[UnsafeCell<[u8; CHUNK_SIZE]>]>,
    mask: u64,
}

unsafe impl Send for SharedBuffers {}
unsafe impl Sync for SharedBuffers {}

/// Producer end of the replication ring. Owned by the journal stage thread.
///
/// The publish sequence is:
/// 1. Write byte data into `buffers[next_seq & mask]`
/// 2. Publish metadata slot via disruptor (includes Release fence)
/// 3. Consumer sees metadata and can safely read the buffer
///
/// Step 1 happens BEFORE step 2's Release store, ensuring the consumer
/// sees the buffer data when it reads the metadata.
pub struct ReplicationProducer {
    inner: melin_disruptor::ring::Producer<ReplicationMeta>,
    buffers: Arc<SharedBuffers>,
}

/// Error returned by [`ReplicationProducer::try_publish_timeout`] when
/// the ring remained full for the entire timeout duration.
#[derive(Debug)]
pub struct BackpressureTimeout;

impl ReplicationProducer {
    /// Publish a batch of encoded journal bytes to the ring.
    ///
    /// Copies `data` into a pre-allocated buffer (no heap allocation), then
    /// publishes the metadata. Spins if the ring is full (backpressure from
    /// the slowest consumer).
    ///
    /// # Panics
    /// Panics if `data.len() > CHUNK_SIZE` (128 KiB).
    pub fn publish(&mut self, data: &[u8], end_sequence: u64) {
        assert!(
            data.len() <= CHUNK_SIZE,
            "replication batch too large: {} > {CHUNK_SIZE}",
            data.len()
        );

        // We need to write the byte buffer BEFORE the disruptor's Release
        // store that makes the slot visible to consumers. The single-producer
        // disruptor reads its own cursor to determine the next sequence, so
        // we can peek at the current cursor to know which slot we'll write to.
        //
        // However, try_publish may fail (Full) if the ring is backpressured.
        // In that case, we spin and retry. Writing the buffer before each
        // attempt is harmless — the slot is not yet visible to consumers
        // (the old data at that position has already been consumed due to
        // the backpressure check), and we'll overwrite it on the next attempt
        // with the same data.
        //
        // The disruptor's try_publish does:
        //   1. Read cursor (seq)
        //   2. Backpressure check against gate consumers
        //   3. buffer.write(seq, value)  [copies metadata into slot]
        //   4. cursor.store(seq + 1, Release)  [makes slot visible]
        //
        // We insert our buffer write between steps 2 and 3 conceptually.
        // Since we call it before try_publish and the metadata write (step 3)
        // is ordered before the Release store (step 4), both the buffer data
        // and metadata are visible when the consumer loads the cursor.

        // Two-phase publish: claim a slot (backpressure check), write the
        // byte buffer, then publish metadata with a Release fence that makes
        // both the buffer and metadata visible to consumers atomically.
        loop {
            match self.inner.try_claim() {
                Ok(seq) => {
                    // Slot claimed — write byte data into the pre-allocated buffer.
                    let idx = (seq & self.buffers.mask) as usize;
                    unsafe {
                        let chunk = &mut *self.buffers.chunks[idx].get();
                        chunk[..data.len()].copy_from_slice(data);
                    }

                    // Publish metadata. The Release store in publish_claimed
                    // ensures the buffer write above is visible to consumers.
                    self.inner.publish_claimed(
                        seq,
                        ReplicationMeta {
                            len: data.len() as u32,
                            end_sequence,
                        },
                    );
                    return;
                }
                Err(_) => std::hint::spin_loop(),
            }
        }
    }

    /// Non-blocking publish attempt. Returns `Ok(())` if the ring had
    /// space, `Err(BackpressureTimeout)` if the ring was full.
    pub fn try_publish(
        &mut self,
        data: &[u8],
        end_sequence: u64,
    ) -> Result<(), BackpressureTimeout> {
        assert!(
            data.len() <= CHUNK_SIZE,
            "replication batch too large: {} > {CHUNK_SIZE}",
            data.len()
        );
        match self.inner.try_claim() {
            Ok(seq) => {
                self.write_and_publish(seq, data, end_sequence);
                Ok(())
            }
            Err(_) => Err(BackpressureTimeout),
        }
    }

    /// Publish with a timeout. Returns `Ok(())` on success,
    /// `Err(BackpressureTimeout)` if the ring remained full for longer
    /// than `timeout`. Only calls
    /// `Instant::now()` when the ring is full (backpressure path), so the
    /// normal fast path has zero overhead.
    pub fn try_publish_timeout(
        &mut self,
        data: &[u8],
        end_sequence: u64,
        timeout: std::time::Duration,
    ) -> Result<(), BackpressureTimeout> {
        assert!(
            data.len() <= CHUNK_SIZE,
            "replication batch too large: {} > {CHUNK_SIZE}",
            data.len()
        );

        // Fast path: try once before touching the clock.
        if let Ok(seq) = self.inner.try_claim() {
            self.write_and_publish(seq, data, end_sequence);
            return Ok(());
        }

        // Slow path: ring is full, spin with timeout.
        let deadline = std::time::Instant::now() + timeout;
        loop {
            match self.inner.try_claim() {
                Ok(seq) => {
                    self.write_and_publish(seq, data, end_sequence);
                    return Ok(());
                }
                Err(_) => {
                    if std::time::Instant::now() >= deadline {
                        return Err(BackpressureTimeout);
                    }
                    std::hint::spin_loop();
                }
            }
        }
    }

    /// Write byte data and publish metadata for a claimed slot.
    fn write_and_publish(&mut self, seq: u64, data: &[u8], end_sequence: u64) {
        let idx = (seq & self.buffers.mask) as usize;
        unsafe {
            let chunk = &mut *self.buffers.chunks[idx].get();
            chunk[..data.len()].copy_from_slice(data);
        }
        self.inner.publish_claimed(
            seq,
            ReplicationMeta {
                len: data.len() as u32,
                end_sequence,
            },
        );
    }

    /// Type-erased handle for reading the producer's published sequence.
    /// Used to gate on ring drain (all consumers have consumed all
    /// published batches) without depending on replica TCP acks.
    pub fn cursor_reader(&self) -> Box<dyn melin_disruptor::ring::QueueCursor> {
        self.inner.cursor_reader()
    }
}

/// Consumer end of the replication ring. One per replica sender thread.
///
/// Uses two-phase consumption: `try_read` peeks at the next batch without
/// advancing the cursor, and `commit` releases the slot back to the producer.
/// The byte slice from `try_read` is valid until `commit` is called.
pub struct ReplicationConsumer {
    inner: melin_disruptor::ring::Consumer<ReplicationMeta>,
    buffers: Arc<SharedBuffers>,
    /// Metadata from the last `try_read`, held until `commit`.
    pending_meta: Option<ReplicationMeta>,
    /// Sequence of the last read (for buffer indexing).
    pending_seq: u64,
}

impl ReplicationConsumer {
    /// Try to read the next replication batch without advancing the cursor.
    ///
    /// Returns `Some((meta, data_slice))` if a batch is available. The byte
    /// slice is valid until `commit()` is called, which releases the slot
    /// back to the producer.
    ///
    /// Must call `commit()` before calling `try_read` again.
    pub fn try_read(&mut self) -> Option<(ReplicationMeta, &[u8])> {
        debug_assert!(
            self.pending_meta.is_none(),
            "must commit() before reading again"
        );
        let mut buf = [ReplicationMeta::default(); 1];
        let count = self.inner.read_batch(&mut buf, 1);
        if count == 0 {
            return None;
        }
        let meta = buf[0];
        // read_batch advanced next_read but NOT the processed counter.
        // The slot is safe to read until commit().
        let seq = self.inner.next_read() - 1; // read_batch advanced past it
        let idx = (seq & self.buffers.mask) as usize;
        let data = unsafe {
            let chunk = &*self.buffers.chunks[idx].get();
            &chunk[..meta.len as usize]
        };
        self.pending_meta = Some(meta);
        self.pending_seq = seq;
        Some((meta, data))
    }

    /// Release the last read slot back to the producer.
    ///
    /// Must be called after `try_read` returns `Some` and before the next
    /// `try_read`. After this call, the byte slice from `try_read` is invalid.
    pub fn commit(&mut self) {
        if self.pending_meta.take().is_some() {
            self.inner.commit(1);
        }
    }

    /// Progress counter for this consumer.
    pub fn progress_counter(&self) -> Arc<Sequence> {
        self.inner.progress_counter()
    }

    /// Fast-forward this consumer past any unread entries so it sits at
    /// the producer's current cursor.
    ///
    /// Called by the replication sender when a replica is evicted so
    /// the stranded ring entries (published before eviction but never
    /// drained to the wire) don't get replayed to a future replica on
    /// the same slot. Without this, a replica that reconnects and
    /// completes catch-up via on-disk journal files can re-receive the
    /// pre-eviction ring contents once live streaming resumes,
    /// acknowledging them with sequences that lag the primary's
    /// journal by however many events were in the ring at eviction.
    /// That in turn stalls the primary's `replication_cursor` at the
    /// evicted-position slot value and gates the response stage.
    pub fn skip_to_producer(&mut self) {
        self.inner.skip_to_dependency();
        self.pending_meta = None;
        self.pending_seq = 0;
    }
}

/// Build a replication ring with one producer and `num_consumers` consumers.
///
/// Returns the producer (for the journal stage) and a Vec of consumers
/// (one per replica sender thread).
pub fn build_replication_ring(
    num_consumers: usize,
    capacity: usize,
) -> (ReplicationProducer, Vec<ReplicationConsumer>) {
    assert!(num_consumers > 0, "need at least one consumer");
    assert!(
        capacity.is_power_of_two(),
        "replication ring capacity must be a power of two, got {capacity}"
    );

    let mut builder = melin_disruptor::ring::DisruptorBuilder::<ReplicationMeta>::new(capacity);
    for _ in 0..num_consumers {
        builder = builder.add_consumer();
    }
    let (inner_producer, inner_consumers) = builder.build();

    // Pre-allocate byte buffers — one 128 KiB chunk per ring slot.
    let chunks: Vec<UnsafeCell<[u8; CHUNK_SIZE]>> = (0..capacity)
        .map(|_| UnsafeCell::new([0u8; CHUNK_SIZE]))
        .collect();
    let buffers = Arc::new(SharedBuffers {
        chunks: chunks.into_boxed_slice(),
        mask: (capacity - 1) as u64,
    });

    let producer = ReplicationProducer {
        inner: inner_producer,
        buffers: Arc::clone(&buffers),
    };

    let consumers = inner_consumers
        .into_iter()
        .map(|c| ReplicationConsumer {
            inner: c,
            buffers: Arc::clone(&buffers),
            pending_meta: None,
            pending_seq: 0,
        })
        .collect();

    (producer, consumers)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_batch_round_trip() {
        let (mut producer, mut consumers) = build_replication_ring(1, REPLICATION_RING_CAPACITY);
        let consumer = &mut consumers[0];

        let data = b"hello replication ring";
        producer.publish(data, 42);

        let (meta, received) = consumer.try_read().unwrap();
        assert_eq!(meta.end_sequence, 42);
        assert_eq!(received, data);
        consumer.commit();
    }

    #[test]
    fn multiple_batches() {
        let (mut producer, mut consumers) = build_replication_ring(1, REPLICATION_RING_CAPACITY);
        let consumer = &mut consumers[0];

        for i in 0..10u64 {
            let data = format!("batch {i}");
            producer.publish(data.as_bytes(), i);
        }

        for i in 0..10u64 {
            let (meta, received) = consumer.try_read().unwrap();
            assert_eq!(meta.end_sequence, i);
            let expected = format!("batch {i}");
            assert_eq!(received, expected.as_bytes());
            consumer.commit();
        }

        assert!(consumer.try_read().is_none());
    }

    #[test]
    fn two_consumers_independent_progress() {
        let (mut producer, mut consumers) = build_replication_ring(2, REPLICATION_RING_CAPACITY);
        let mut c1 = consumers.pop().unwrap();
        let mut c0 = consumers.pop().unwrap();

        producer.publish(b"first", 1);
        producer.publish(b"second", 2);

        // c0 reads both.
        let (m, d) = c0.try_read().unwrap();
        assert_eq!(d, b"first");
        assert_eq!(m.end_sequence, 1);
        c0.commit();
        let (m, d) = c0.try_read().unwrap();
        assert_eq!(d, b"second");
        assert_eq!(m.end_sequence, 2);
        c0.commit();

        // c1 reads independently.
        let (_, d) = c1.try_read().unwrap();
        assert_eq!(d, b"first");
        c1.commit();
        let (_, d) = c1.try_read().unwrap();
        assert_eq!(d, b"second");
        c1.commit();

        assert!(c0.try_read().is_none());
        assert!(c1.try_read().is_none());
    }

    #[test]
    fn large_batch_fills_chunk() {
        let (mut producer, mut consumers) = build_replication_ring(1, REPLICATION_RING_CAPACITY);
        let consumer = &mut consumers[0];

        let data = vec![0xFFu8; CHUNK_SIZE];
        producer.publish(&data, 99);

        let (meta, received) = consumer.try_read().unwrap();
        assert_eq!(meta.len as usize, CHUNK_SIZE);
        assert_eq!(meta.end_sequence, 99);
        assert_eq!(received.len(), CHUNK_SIZE);
        assert!(received.iter().all(|&b| b == 0xFF));
        consumer.commit();
    }

    #[test]
    fn wrap_around() {
        let (mut producer, mut consumers) = build_replication_ring(1, REPLICATION_RING_CAPACITY);
        let consumer = &mut consumers[0];

        for i in 0..REPLICATION_RING_CAPACITY as u64 * 3 {
            let data = i.to_le_bytes();
            producer.publish(&data, i);
            let (meta, received) = consumer.try_read().unwrap();
            assert_eq!(meta.end_sequence, i);
            assert_eq!(received, &data);
            consumer.commit();
        }
    }

    #[test]
    fn concurrent_producer_consumer() {
        let (mut producer, mut consumers) = build_replication_ring(1, REPLICATION_RING_CAPACITY);
        let mut consumer = consumers.pop().unwrap();

        let count = 10_000u64;

        let consumer_thread = std::thread::spawn(move || {
            let mut received = Vec::new();
            loop {
                if let Some((meta, data)) = consumer.try_read() {
                    let val = u64::from_le_bytes(data.try_into().unwrap());
                    assert_eq!(val, meta.end_sequence);
                    received.push(val);
                    consumer.commit();
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
            producer.publish(&i.to_le_bytes(), i);
        }

        let received = consumer_thread.join().unwrap();
        assert_eq!(received.len(), count as usize);
        for (i, val) in received.iter().enumerate() {
            assert_eq!(*val, i as u64);
        }
    }

    #[test]
    fn try_publish_timeout_succeeds_when_ring_has_space() {
        let (mut producer, mut consumers) = build_replication_ring(1, REPLICATION_RING_CAPACITY);
        let consumer = &mut consumers[0];

        let result = producer.try_publish_timeout(b"data", 1, std::time::Duration::from_millis(10));
        assert!(result.is_ok());

        let (meta, data) = consumer.try_read().unwrap();
        assert_eq!(meta.end_sequence, 1);
        assert_eq!(data, b"data");
        consumer.commit();
    }

    #[test]
    fn try_publish_timeout_fails_when_ring_full() {
        // Capacity 2: fill both slots without consuming → ring is full.
        let (mut producer, _consumers) = build_replication_ring(1, 2);

        producer.publish(b"a", 1);
        producer.publish(b"b", 2);

        // Ring is full — timeout should fire quickly.
        let start = std::time::Instant::now();
        let result = producer.try_publish_timeout(b"c", 3, std::time::Duration::from_millis(50));
        let elapsed = start.elapsed();

        assert!(result.is_err());
        assert!(elapsed >= std::time::Duration::from_millis(50));
        assert!(elapsed < std::time::Duration::from_millis(200));
    }
}
