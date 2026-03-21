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

use trading_disruptor::padding::Sequence;

/// Maximum batch buffer size. Matches `BATCH_BUF_CAPACITY` in writer.rs.
/// Each ring slot has one pre-allocated chunk of this size.
const CHUNK_SIZE: usize = 128 * 1024;

/// Capacity of the replication ring (number of batch slots).
/// 2^6 = 64 slots. At 128 KiB per slot, total buffer = 8 MiB.
/// Provides buffering for ~64K events (64 batches * 1024 events/batch)
/// before backpressure reaches the journal stage.
pub const REPLICATION_RING_CAPACITY: usize = 1 << 6;

/// Metadata for one replication batch. Small and `Copy` — carried in the
/// disruptor ring slot. The actual byte data lives in the shared buffer
/// array at the same ring index.
#[derive(Debug, Clone, Copy, Default)]
pub struct ReplicationMeta {
    /// Number of valid bytes in the corresponding buffer chunk.
    pub len: u32,
    /// Sequence number of the last journal entry in this batch.
    pub end_sequence: u64,
    /// BLAKE3 chain hash after all entries in this batch.
    pub chain_hash: [u8; 32],
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
    inner: trading_disruptor::ring::Producer<ReplicationMeta>,
    buffers: Arc<SharedBuffers>,
}

impl ReplicationProducer {
    /// Publish a batch of encoded journal bytes to the ring.
    ///
    /// Copies `data` into a pre-allocated buffer (no heap allocation), then
    /// publishes the metadata. Spins if the ring is full (backpressure from
    /// the slowest consumer).
    ///
    /// # Panics
    /// Panics if `data.len() > CHUNK_SIZE` (128 KiB).
    pub fn publish(&mut self, data: &[u8], end_sequence: u64, chain_hash: [u8; 32]) {
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
                            chain_hash,
                        },
                    );
                    return;
                }
                Err(_) => std::hint::spin_loop(),
            }
        }
    }
}

/// Consumer end of the replication ring. One per replica sender thread.
///
/// Uses two-phase consumption: `try_read` peeks at the next batch without
/// advancing the cursor, and `commit` releases the slot back to the producer.
/// The byte slice from `try_read` is valid until `commit` is called.
pub struct ReplicationConsumer {
    inner: trading_disruptor::ring::Consumer<ReplicationMeta>,
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
}

/// Build a replication ring with one producer and `num_consumers` consumers.
///
/// Returns the producer (for the journal stage) and a Vec of consumers
/// (one per replica sender thread).
pub fn build_replication_ring(
    num_consumers: usize,
) -> (ReplicationProducer, Vec<ReplicationConsumer>) {
    assert!(num_consumers > 0, "need at least one consumer");

    let mut builder = trading_disruptor::ring::DisruptorBuilder::<ReplicationMeta>::new(
        REPLICATION_RING_CAPACITY,
    );
    for _ in 0..num_consumers {
        builder = builder.add_consumer();
    }
    let (inner_producer, inner_consumers) = builder.build();

    // Pre-allocate byte buffers — one 128 KiB chunk per ring slot.
    // Total: 64 * 128 KiB = 8 MiB.
    let chunks: Vec<UnsafeCell<[u8; CHUNK_SIZE]>> = (0..REPLICATION_RING_CAPACITY)
        .map(|_| UnsafeCell::new([0u8; CHUNK_SIZE]))
        .collect();
    let buffers = Arc::new(SharedBuffers {
        chunks: chunks.into_boxed_slice(),
        mask: (REPLICATION_RING_CAPACITY - 1) as u64,
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
        let (mut producer, mut consumers) = build_replication_ring(1);
        let consumer = &mut consumers[0];

        let data = b"hello replication ring";
        let chain = [0xAB; 32];
        producer.publish(data, 42, chain);

        let (meta, received) = consumer.try_read().unwrap();
        assert_eq!(meta.end_sequence, 42);
        assert_eq!(meta.chain_hash, chain);
        assert_eq!(received, data);
        consumer.commit();
    }

    #[test]
    fn multiple_batches() {
        let (mut producer, mut consumers) = build_replication_ring(1);
        let consumer = &mut consumers[0];

        for i in 0..10u64 {
            let data = format!("batch {i}");
            producer.publish(data.as_bytes(), i, [i as u8; 32]);
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
        let (mut producer, mut consumers) = build_replication_ring(2);
        let mut c1 = consumers.pop().unwrap();
        let mut c0 = consumers.pop().unwrap();

        producer.publish(b"first", 1, [0; 32]);
        producer.publish(b"second", 2, [0; 32]);

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
        let (mut producer, mut consumers) = build_replication_ring(1);
        let consumer = &mut consumers[0];

        let data = vec![0xFFu8; CHUNK_SIZE];
        producer.publish(&data, 99, [0x11; 32]);

        let (meta, received) = consumer.try_read().unwrap();
        assert_eq!(meta.len as usize, CHUNK_SIZE);
        assert_eq!(meta.end_sequence, 99);
        assert_eq!(received.len(), CHUNK_SIZE);
        assert!(received.iter().all(|&b| b == 0xFF));
        consumer.commit();
    }

    #[test]
    fn wrap_around() {
        let (mut producer, mut consumers) = build_replication_ring(1);
        let consumer = &mut consumers[0];

        for i in 0..REPLICATION_RING_CAPACITY as u64 * 3 {
            let data = i.to_le_bytes();
            producer.publish(&data, i, [0; 32]);
            let (meta, received) = consumer.try_read().unwrap();
            assert_eq!(meta.end_sequence, i);
            assert_eq!(received, &data);
            consumer.commit();
        }
    }

    #[test]
    fn concurrent_producer_consumer() {
        let (mut producer, mut consumers) = build_replication_ring(1);
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
            producer.publish(&i.to_le_bytes(), i, [0; 32]);
        }

        let received = consumer_thread.join().unwrap();
        assert_eq!(received.len(), count as usize);
        for (i, val) in received.iter().enumerate() {
            assert_eq!(*val, i as u64);
        }
    }
}
