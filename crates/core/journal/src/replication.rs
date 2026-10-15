//! Replication ring buffer — lock-free, pre-allocated byte batch transfer
//! from the journal stage to one or more replication sender threads.
//!
//! Uses the generic disruptor ring for sequencing and backpressure, with a
//! side array of pre-allocated byte buffers, one [`CHUNK_SIZE`] chunk per
//! slot, indexed by `seq & mask`. The journal stage copies batch bytes into
//! the buffer, then publishes the metadata slot. Consumers read the
//! metadata and reference the buffer directly (zero-copy read).
//!
//! This design avoids heap allocation on the journal thread. The only cost
//! is one copy of each batch into its slot's buffer.

use std::cell::UnsafeCell;
use std::sync::Arc;

use melin_pipeline::padding::Sequence;
use melin_pipeline::wait::WaitStrategy;

/// Maximum batch buffer size. Matches `BATCH_BUF_CAPACITY` in writer.rs.
/// Each ring slot has one pre-allocated chunk of this size.
///
/// Public because it is a hard bound on the producer, not an internal
/// detail: a batch too large for a slot is refused by
/// [`ReplicationProducer::try_publish`] with a panic, since a replication
/// frame cannot be split or dropped without tearing a replica's journal.
/// The transport sizes its journal batch so this can never happen — see
/// `max_journal_batch` — and needs the number to do it. Smaller than the
/// journal's own hand-off chunk, so it, not the journal, is what bounds a
/// wide-event application's batch.
pub const CHUNK_SIZE: usize = 512 * 1024;

// A slot must hold at least one entry of any application, or the batch
// sizing that divides by this would compute a zero-length batch. The
// journal's write ring asserts the same against its own chunk.
const _: () = assert!(crate::encoder::MAX_ENTRY_SIZE <= CHUNK_SIZE);

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
    inner: melin_pipeline::ring::Producer<ReplicationMeta>,
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
    /// publishes the metadata. Waits, per the ring's [`WaitStrategy`], if
    /// the ring is full (backpressure from the slowest consumer).
    ///
    /// # Panics
    /// Panics if `data.len() > CHUNK_SIZE` (512 KiB).
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
        // In that case, we wait and retry. Writing the buffer before each
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
        let mut waiter = self.inner.wait_strategy().waiter();
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
                Err(_) => waiter.idle(),
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

        // Slow path: ring is full, wait with timeout.
        let deadline = std::time::Instant::now() + timeout;
        let mut waiter = self.inner.wait_strategy().waiter();
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
                    waiter.idle();
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
    pub fn cursor_reader(&self) -> Box<dyn melin_pipeline::ring::QueueCursor> {
        self.inner.cursor_reader()
    }
}

/// Consumer end of the replication ring. One per replica sender thread.
///
/// Reads are peeks: [`Self::peek`] lends the next batch as a
/// [`ReplicationBatch`] without consuming it, and only
/// [`ReplicationBatch::commit`] consumes it and releases its slot to the
/// producer. A batch that is not committed (the replica's socket has no
/// room for it, say) is simply dropped, and the next `peek` lends the same
/// batch again: no batch can be read past without being committed (only
/// [`Self::skip_to_producer`] skips, deliberately, on eviction), and none
/// committed without being read. The borrow checker holds the rest: a
/// [`ReplicationBatch`] borrows the consumer, so no second batch can be
/// peeked while one is held.
pub struct ReplicationConsumer {
    inner: melin_pipeline::ring::Consumer<ReplicationMeta>,
    buffers: Arc<SharedBuffers>,
}

/// A batch lent by [`ReplicationConsumer::peek`], not yet consumed.
///
/// Consumed only by [`Self::commit`]. Dropped instead, it stays first in the
/// ring and the next `peek` lends it again. Its bytes are the slot's, valid
/// for as long as the batch is held: the producer cannot reuse the slot
/// before it is committed.
///
/// A batch dropped uncommitted is peeked again:
///
/// ```
/// # use melin_journal::replication::build_replication_ring;
/// # use melin_pipeline::wait::WaitStrategy;
/// let (mut producer, mut consumers) = build_replication_ring(1, 2, WaitStrategy::SpinThenYield);
/// let consumer = &mut consumers[0];
/// producer.publish(b"frame", 1);
/// let held = consumer.peek().expect("published");
/// drop(held); // no room to send it, say
/// let again = consumer.peek().expect("the same batch");
/// assert_eq!(again.data(), b"frame");
/// again.commit();
/// ```
///
/// and no second batch can be peeked while one is held:
///
/// ```compile_fail
/// # use melin_journal::replication::build_replication_ring;
/// # use melin_pipeline::wait::WaitStrategy;
/// let (mut producer, mut consumers) = build_replication_ring(1, 2, WaitStrategy::SpinThenYield);
/// let consumer = &mut consumers[0];
/// producer.publish(b"frame", 1);
/// let held = consumer.peek().expect("published");
/// let next = consumer.peek(); // the first is still held
/// held.commit();
/// drop(next);
/// ```
#[must_use = "a batch is consumed only by `commit`; dropped, it is peeked again"]
pub struct ReplicationBatch<'a> {
    inner: &'a mut melin_pipeline::ring::Consumer<ReplicationMeta>,
    meta: ReplicationMeta,
    data: &'a [u8],
}

impl ReplicationBatch<'_> {
    /// The batch's length and last journal sequence.
    pub fn meta(&self) -> ReplicationMeta {
        self.meta
    }

    /// The batch's bytes: a wire-ready frame. Borrowed from the batch, not
    /// the ring, so they cannot outlive the commit that frees the slot.
    pub fn data(&self) -> &[u8] {
        self.data
    }

    /// Consume the batch: release its slot to the producer, so that the
    /// next `peek` lends the batch after it.
    pub fn commit(self) {
        self.inner.commit_consumed(1);
    }
}

impl ReplicationConsumer {
    /// The next batch, without consuming it: the same batch on every call
    /// until it is committed. `None` when the ring holds nothing unread.
    pub fn peek(&mut self) -> Option<ReplicationBatch<'_>> {
        let (first, _) = self.inner.peek_batch(1);
        let meta = *first.first()?;
        let idx = (self.inner.next_read() & self.buffers.mask) as usize;
        // SAFETY: `peek_batch` returned the slot at `next_read`, so the
        // producer has published it, and it cannot claim the slot again
        // until this consumer's progress passes it, which only
        // `ReplicationBatch::commit` does. The bytes are lent for the batch's
        // lifetime, which ends at that commit at the latest.
        let data = unsafe {
            let chunk = &*self.buffers.chunks[idx].get();
            &chunk[..meta.len as usize]
        };
        Some(ReplicationBatch {
            inner: &mut self.inner,
            meta,
            data,
        })
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
    /// That in turn stalls the slot's acked-progress cursor at the
    /// pre-eviction position and gates the response stage.
    pub fn skip_to_producer(&mut self) {
        self.inner.skip_to_dependency();
    }
}

/// Build a replication ring with one producer and `num_consumers` consumers.
///
/// Returns the producer (for the journal stage) and a Vec of consumers
/// (one per replica sender thread). `producer_wait` is how the producer's
/// blocking publish paths wait on a full ring.
pub fn build_replication_ring(
    num_consumers: usize,
    capacity: usize,
    producer_wait: WaitStrategy,
) -> (ReplicationProducer, Vec<ReplicationConsumer>) {
    assert!(num_consumers > 0, "need at least one consumer");
    assert!(
        capacity.is_power_of_two(),
        "replication ring capacity must be a power of two, got {capacity}"
    );

    let mut builder = melin_pipeline::ring::DisruptorBuilder::<ReplicationMeta>::new(capacity);
    for _ in 0..num_consumers {
        builder = builder.add_consumer();
    }
    let (inner_producer, inner_consumers) = builder.build(producer_wait);

    // Pre-allocate byte buffers — one [`CHUNK_SIZE`] chunk per ring slot.
    let buffers = Arc::new(SharedBuffers {
        chunks: crate::write_ring::alloc_zeroed_chunk_slab::<CHUNK_SIZE>(capacity),
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
        })
        .collect();

    (producer, consumers)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_batch_round_trip() {
        let (mut producer, mut consumers) =
            build_replication_ring(1, REPLICATION_RING_CAPACITY, WaitStrategy::SpinThenYield);
        let consumer = &mut consumers[0];

        let data = b"hello replication ring";
        producer.publish(data, 42);

        let batch = consumer.peek().unwrap();
        assert_eq!(batch.meta().end_sequence, 42);
        assert_eq!(batch.data(), data);
        batch.commit();
        assert!(consumer.peek().is_none(), "consumed");
    }

    /// A batch peeked and dropped without a commit is lent again, not read
    /// past, and keeps its slot from the producer meanwhile: every batch
    /// comes out once, in order, however many times it was peeked first.
    #[test]
    fn an_uncommitted_batch_is_peeked_again_and_keeps_its_slot() {
        let (mut producer, mut consumers) =
            build_replication_ring(1, 2, WaitStrategy::SpinThenYield);
        let consumer = &mut consumers[0];
        assert!(consumer.peek().is_none(), "nothing published");

        producer.publish(b"first", 1);
        producer.publish(b"second", 2);
        let batch = consumer.peek().expect("the first batch");
        assert_eq!(
            (batch.meta().end_sequence, batch.data()),
            (1, &b"first"[..])
        );
        // Not committed (no room for it, say).
        drop(batch);

        // The ring is full while the batch is unconsumed: the producer
        // cannot take its slot, and the next peek lends the same bytes.
        assert!(producer.try_publish(b"third", 3).is_err());
        let batch = consumer.peek().expect("the same batch again");
        assert_eq!(
            (batch.meta().end_sequence, batch.data()),
            (1, &b"first"[..])
        );
        batch.commit();

        producer
            .try_publish(b"third", 3)
            .expect("the slot is free again");
        let batch = consumer.peek().expect("the second batch");
        assert_eq!(
            (batch.meta().end_sequence, batch.data()),
            (2, &b"second"[..])
        );
        batch.commit();
        consumer.skip_to_producer();
        assert!(consumer.peek().is_none(), "skipped past the third");
    }

    #[test]
    fn multiple_batches() {
        let (mut producer, mut consumers) =
            build_replication_ring(1, REPLICATION_RING_CAPACITY, WaitStrategy::SpinThenYield);
        let consumer = &mut consumers[0];

        for i in 0..10u64 {
            let data = format!("batch {i}");
            producer.publish(data.as_bytes(), i);
        }

        for i in 0..10u64 {
            let batch = consumer.peek().unwrap();
            assert_eq!(batch.meta().end_sequence, i);
            let expected = format!("batch {i}");
            assert_eq!(batch.data(), expected.as_bytes());
            batch.commit();
        }

        assert!(consumer.peek().is_none());
    }

    #[test]
    fn two_consumers_independent_progress() {
        let (mut producer, mut consumers) =
            build_replication_ring(2, REPLICATION_RING_CAPACITY, WaitStrategy::SpinThenYield);
        let mut c1 = consumers.pop().unwrap();
        let mut c0 = consumers.pop().unwrap();

        producer.publish(b"first", 1);
        producer.publish(b"second", 2);

        // c0 reads both.
        let b = c0.peek().unwrap();
        assert_eq!(b.data(), b"first");
        assert_eq!(b.meta().end_sequence, 1);
        b.commit();
        let b = c0.peek().unwrap();
        assert_eq!(b.data(), b"second");
        assert_eq!(b.meta().end_sequence, 2);
        b.commit();

        // c1 reads independently.
        let b = c1.peek().unwrap();
        assert_eq!(b.data(), b"first");
        b.commit();
        let b = c1.peek().unwrap();
        assert_eq!(b.data(), b"second");
        b.commit();

        assert!(c0.peek().is_none());
        assert!(c1.peek().is_none());
    }

    #[test]
    fn large_batch_fills_chunk() {
        let (mut producer, mut consumers) =
            build_replication_ring(1, REPLICATION_RING_CAPACITY, WaitStrategy::SpinThenYield);
        let consumer = &mut consumers[0];

        let data = vec![0xFFu8; CHUNK_SIZE];
        producer.publish(&data, 99);

        let batch = consumer.peek().unwrap();
        assert_eq!(batch.meta().len as usize, CHUNK_SIZE);
        assert_eq!(batch.meta().end_sequence, 99);
        assert_eq!(batch.data().len(), CHUNK_SIZE);
        assert!(batch.data().iter().all(|&b| b == 0xFF));
        batch.commit();
    }

    #[test]
    fn wrap_around() {
        let (mut producer, mut consumers) =
            build_replication_ring(1, REPLICATION_RING_CAPACITY, WaitStrategy::SpinThenYield);
        let consumer = &mut consumers[0];

        for i in 0..REPLICATION_RING_CAPACITY as u64 * 3 {
            let data = i.to_le_bytes();
            producer.publish(&data, i);
            let batch = consumer.peek().unwrap();
            assert_eq!(batch.meta().end_sequence, i);
            assert_eq!(batch.data(), &data);
            batch.commit();
        }
    }

    #[test]
    fn concurrent_producer_consumer() {
        let (mut producer, mut consumers) =
            build_replication_ring(1, REPLICATION_RING_CAPACITY, WaitStrategy::SpinThenYield);
        let mut consumer = consumers.pop().unwrap();

        let count = 10_000u64;

        let consumer_thread = std::thread::spawn(move || {
            let mut received = Vec::new();
            loop {
                if let Some(batch) = consumer.peek() {
                    let val = u64::from_le_bytes(batch.data().try_into().unwrap());
                    assert_eq!(val, batch.meta().end_sequence);
                    received.push(val);
                    batch.commit();
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
        let (mut producer, mut consumers) =
            build_replication_ring(1, REPLICATION_RING_CAPACITY, WaitStrategy::SpinThenYield);
        let consumer = &mut consumers[0];

        let result = producer.try_publish_timeout(b"data", 1, std::time::Duration::from_millis(10));
        assert!(result.is_ok());

        let batch = consumer.peek().unwrap();
        assert_eq!(batch.meta().end_sequence, 1);
        assert_eq!(batch.data(), b"data");
        batch.commit();
    }

    #[test]
    fn try_publish_timeout_fails_when_ring_full() {
        // Capacity 2: fill both slots without consuming → ring is full.
        let (mut producer, _consumers) = build_replication_ring(1, 2, WaitStrategy::SpinThenYield);

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
