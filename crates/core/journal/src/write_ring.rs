//! Journal write ring — the hand-off from the sequencing thread to the
//! disk thread.
//!
//! Same shape as [`crate::replication`]'s ring: a disruptor carrying
//! small `Copy` metadata, plus a side array of pre-allocated byte chunks
//! indexed by `seq & mask`. One difference matters — the producer hands
//! out its chunk **writable**, so the sequencer encodes entries straight
//! into the slot the disk thread will write from. The split costs no
//! copy that the single-threaded writer didn't already pay.
//!
//! ## Why the two-phase consumer
//!
//! The bytes live in the ring, not in the descriptor, so a slot must not
//! be reusable until its bytes are *durable* — not merely read. The
//! disk thread therefore reads descriptors, writes and syncs them, and
//! only then commits, which releases the slots. That makes slot release
//! and the durability boundary the same event, and it gives the
//! sequencer an exact drain test: consumer progress caught up to the
//! producer cursor means everything published is on disk and its cursors
//! are published.
//!
//! ## Backpressure
//!
//! A full ring stalls the sequencer at its next claim, which stops it
//! draining the input disruptor, which backpressures the producers —
//! the same chain as a slow journal today, just with a deeper buffer in
//! front of it. The ring's depth is therefore how long a disk stall is
//! absorbed before it reaches clients.

use std::cell::UnsafeCell;
use std::sync::Arc;

use melin_pipeline::padding::Sequence;
use melin_pipeline::ring;

pub use melin_pipeline::ring::Full;

/// Bytes per slot.
///
/// Must hold the largest batch the pipeline can encode: its cap is
/// `MAX_JOURNAL_BATCH` (4096) events of at most
/// [`crate::encoder::MAX_ENTRY_SIZE`] (144) bytes = 576 KiB. The
/// sequencer also stops filling a slot once fewer than one entry's
/// headroom remains, so the bound is enforced at both ends.
pub const CHUNK_SIZE: usize = 640 * 1024;

// Compile-time proof of the paragraph above: a full-size batch fits in
// one slot. Keep in sync with `MAX_JOURNAL_BATCH` in the pipeline.
const _: () = assert!(4096 * crate::encoder::MAX_ENTRY_SIZE <= CHUNK_SIZE);

/// Slots in the ring.
///
/// At [`CHUNK_SIZE`] per slot this is 40 MiB of staging. In batch terms
/// it is 64 batches; in time terms, at the ~1 K batches/s a loaded
/// pipeline produces, roughly 60 ms of disk stall absorbed before the
/// sequencer itself stalls — after which the input ring keeps absorbing
/// as it does today.
pub const DEFAULT_CAPACITY: usize = 64;

/// Descriptor for one batch handed to the disk thread.
///
/// Everything the disk thread must publish *after* the batch is durable
/// travels with the batch, because the sequencer has moved on by then
/// and its state no longer describes this batch.
#[derive(Debug, Clone, Copy, Default)]
pub struct JournalWriteMeta {
    /// Valid bytes in the corresponding chunk.
    pub len: u32,
    /// Highest journal sequence in this batch — the durable wire-seq
    /// cursor and `FsyncState.journal_seq` once it lands.
    pub journal_seq: u64,
    /// Chain value after this batch. `[0u8; 32]` with `hash-chain` off.
    pub chain_hash: [u8; 32],
    /// Input-ring position the disk thread publishes as consumer
    /// progress once the batch is durable. This is what gates slot
    /// reuse upstream and, on a replica, persisted acks — so it must
    /// never be published before the sync returns.
    ///
    /// Also published as `FsyncState.input_ring_seq`, deliberately the
    /// same value: it is the ring position `journal_seq` covers, and the
    /// shadow snapshot's alignment gate relies on the two describing
    /// the same prefix. Publishing the read cursor there instead let a
    /// mid-batch mark barrier (prefix encoded, tail not) hand the shadow
    /// a pair whose `journal_seq` covered less than its ring position —
    /// a snapshot header that under-reported the folded-in events.
    pub ring_progress: u64,
}

/// Pre-allocated chunks, one per ring slot.
///
/// A boxed slice of whole chunks, so all 64 slots are one contiguous
/// allocation rather than 64 scattered ones — the slots are written in
/// rotation, and consecutive batches land [`CHUNK_SIZE`] apart, so the
/// encode path's locality depends on it.
///
/// Thread safety: the disruptor protocol provides mutual exclusion. The
/// producer writes slot N only after every consumer has advanced past it
/// (backpressure), and the consumer reads slot N only after the producer
/// published it (cursor gating). No concurrent access to one slot ever
/// occurs.
struct SharedChunks {
    chunks: Box<[UnsafeCell<[u8; CHUNK_SIZE]>]>,
    mask: u64,
}

// Safety: see the type docs — the disruptor serialises access to each
// slot, and the chunks are never aliased across the boundary.
unsafe impl Send for SharedChunks {}
unsafe impl Sync for SharedChunks {}

/// A claimed slot's chunk, owned by the sequencer until it publishes.
///
/// Deliberately not tied to the producer's borrow: the sequencer holds
/// this across a whole batch while still calling `&mut self` methods on
/// itself. Handing it back to [`JournalWriteProducer::publish`] by value
/// is what makes "no writes after publish" a type-level property.
pub struct ClaimedChunk {
    chunks: Arc<SharedChunks>,
    seq: u64,
}

impl ClaimedChunk {
    /// The slot's writable bytes — where the encoder appends.
    pub fn bytes_mut(&mut self) -> &mut [u8] {
        let idx = (self.seq & self.chunks.mask) as usize;
        // Safety: this handle is unique (the producer issues one at a
        // time) and the slot is claimed but unpublished, so no consumer
        // can be reading it.
        unsafe { &mut *self.chunks.chunks[idx].get() }
    }

    /// The slot's bytes for reading back what has been encoded — the
    /// replication framing slices out of here.
    pub fn bytes(&self) -> &[u8] {
        let idx = (self.seq & self.chunks.mask) as usize;
        // Safety: as above.
        unsafe { &*self.chunks.chunks[idx].get() }
    }
}

/// Producer end, owned by the sequencing thread.
pub struct JournalWriteProducer {
    inner: ring::Producer<JournalWriteMeta>,
    chunks: Arc<SharedChunks>,
    /// Sequence of the outstanding claim, if any. One at a time: the
    /// sequencer fills a slot before starting the next.
    claimed: Option<u64>,
    /// The consumer's progress counter, for [`Self::drained`].
    consumer_progress: Arc<Sequence>,
}

impl JournalWriteProducer {
    /// Claim the next slot and take its chunk.
    ///
    /// `Err(Full)` means every slot is still awaiting durability — the
    /// disk thread is behind, and the caller must retry rather than
    /// drop or overwrite anything.
    pub fn try_claim(&mut self) -> Result<ClaimedChunk, Full> {
        debug_assert!(
            self.claimed.is_none(),
            "a slot is already claimed — publish it before claiming another"
        );
        let seq = self.inner.try_claim()?;
        self.claimed = Some(seq);
        Ok(ClaimedChunk {
            chunks: Arc::clone(&self.chunks),
            seq,
        })
    }

    /// Publish the claimed chunk with its descriptor, making both
    /// visible to the disk thread.
    ///
    /// Taking `claim` by value ends the sequencer's write access at
    /// exactly the point the disk thread gains read access.
    pub fn publish(&mut self, claim: ClaimedChunk, meta: JournalWriteMeta) {
        let seq = self.claimed.take().expect("publish without a claimed slot");
        debug_assert_eq!(seq, claim.seq, "publishing a chunk from a different claim");
        debug_assert!(
            meta.len as usize <= CHUNK_SIZE,
            "batch length {} exceeds the slot size",
            meta.len
        );
        drop(claim);
        // The chunk write above happens-before this Release store, so a
        // consumer that sees the descriptor sees the bytes.
        self.inner.publish_claimed(seq, meta);
    }

    /// Whether the disk thread has caught up: every published batch is
    /// durable and its cursors are published.
    ///
    /// The consumer commits only after publishing, so this is an exact
    /// test rather than an approximation — it is what the rotation
    /// rendezvous and shutdown wait on.
    pub fn drained(&self) -> bool {
        self.consumed() >= self.inner.peek_cursor()
    }

    /// Batches the disk thread has released, in ring sequence terms.
    /// `Acquire` so a caller that observes the drain also observes
    /// everything the disk thread published before committing.
    fn consumed(&self) -> u64 {
        self.consumer_progress
            .get()
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Ring depth in slots, for sizing and gauges.
    pub fn capacity(&self) -> u64 {
        self.inner.capacity()
    }

    /// Batches published but not yet durable — the flush-lag gauge.
    ///
    /// `Relaxed`, unlike [`drained`](Self::drained): this feeds a gauge,
    /// and nothing is ordered against it. `drained` is the read that
    /// carries the acquire edge.
    pub fn in_flight(&self) -> u64 {
        let consumed = self
            .consumer_progress
            .get()
            .load(std::sync::atomic::Ordering::Relaxed);
        self.inner.peek_cursor().saturating_sub(consumed)
    }
}

/// Consumer end, owned by the disk thread.
///
/// Two-phase like the replication consumer:
/// [`stage_ready`](Self::stage_ready) takes every published batch
/// without releasing any slot, and [`commit`](Self::commit) releases
/// them all. The byte slices stay valid until that commit.
///
/// Staging returns positions rather than slices so the caller can hold
/// *all* of them at once — that is what lets the disk thread build one
/// iovec array and issue a single `pwritev` for a whole backlog. A
/// `try_read`-per-batch shape cannot: each call would borrow the
/// consumer mutably, so no two slices could coexist.
pub struct JournalWriteConsumer {
    inner: ring::Consumer<JournalWriteMeta>,
    chunks: Arc<SharedChunks>,
    /// Ring sequences staged since the last commit, in order. Released
    /// together, because one `fdatasync` covers all of them.
    staged: Vec<u64>,
    /// Valid byte length of each staged slot, parallel to `staged`.
    staged_len: Vec<u32>,
}

impl JournalWriteConsumer {
    /// Take every published batch, leaving all their slots held.
    ///
    /// Returns the last batch's descriptor — the one whose cursors
    /// become true once the whole staged run is durable, since every
    /// earlier batch is durable by then too. `None` when nothing is
    /// published.
    pub fn stage_ready(&mut self) -> Option<JournalWriteMeta> {
        let mut buf = [JournalWriteMeta::default(); 1];
        let mut last = None;
        while self.inner.read_batch(&mut buf, 1) == 1 {
            // `read_batch` advanced `next_read` but not the published
            // progress counter, so the slot stays ours until `commit`.
            self.staged.push(self.inner.next_read() - 1);
            self.staged_len.push(buf[0].len);
            last = Some(buf[0]);
        }
        last
    }

    /// Number of batches staged since the last commit.
    pub fn staged(&self) -> usize {
        self.staged.len()
    }

    /// The `i`th staged batch's bytes.
    ///
    /// Borrows `&self`, so every staged slice can be held at once —
    /// see the type docs.
    pub fn staged_bytes(&self, i: usize) -> &[u8] {
        let seq = self.staged[i];
        let len = self.staged_len[i] as usize;
        let idx = (seq & self.chunks.mask) as usize;
        // Safety: the slot is published (the producer cannot touch it)
        // and not yet committed (no other reader can take it).
        unsafe {
            let chunk = &*self.chunks.chunks[idx].get();
            &chunk[..len]
        }
    }

    /// Total bytes across everything staged.
    pub fn staged_total_bytes(&self) -> usize {
        self.staged_len.iter().map(|&l| l as usize).sum()
    }

    /// Release every slot staged since the last commit. Call only once
    /// the batches are durable — this is what lets the sequencer reuse
    /// them, and what its drain test observes.
    pub fn commit(&mut self) {
        if !self.staged.is_empty() {
            self.inner.commit();
            self.staged.clear();
            self.staged_len.clear();
        }
    }
}

/// Build the hand-off ring: producer for the sequencing thread,
/// consumer for the disk thread.
pub fn build_journal_write_ring(capacity: usize) -> (JournalWriteProducer, JournalWriteConsumer) {
    assert!(
        capacity.is_power_of_two(),
        "journal write ring capacity must be a power of two, got {capacity}"
    );

    let (inner_producer, mut inner_consumers) =
        ring::DisruptorBuilder::<JournalWriteMeta>::new(capacity)
            .add_consumer()
            .build();
    let inner_consumer = inner_consumers
        .pop()
        .expect("builder was asked for one consumer");

    let chunks: Vec<UnsafeCell<[u8; CHUNK_SIZE]>> = (0..capacity)
        .map(|_| UnsafeCell::new([0u8; CHUNK_SIZE]))
        .collect();
    let chunks = Arc::new(SharedChunks {
        chunks: chunks.into_boxed_slice(),
        mask: (capacity - 1) as u64,
    });

    let producer = JournalWriteProducer {
        inner: inner_producer,
        chunks: Arc::clone(&chunks),
        claimed: None,
        consumer_progress: inner_consumer.progress_counter(),
    };
    let consumer = JournalWriteConsumer {
        inner: inner_consumer,
        chunks,
        // Sized to the ring: staging can never exceed its depth, so
        // the disk thread never allocates on the drain path.
        staged: Vec::with_capacity(capacity),
        staged_len: Vec::with_capacity(capacity),
    };
    (producer, consumer)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(len: usize, journal_seq: u64) -> JournalWriteMeta {
        JournalWriteMeta {
            len: len as u32,
            journal_seq,
            chain_hash: [journal_seq as u8; 32],
            ring_progress: journal_seq * 10,
        }
    }

    /// Bytes written into a claimed chunk arrive at the consumer
    /// verbatim, with their descriptor.
    #[test]
    fn published_bytes_and_meta_round_trip() {
        let (mut producer, mut consumer) = build_journal_write_ring(4);

        let mut claim = producer.try_claim().unwrap();
        claim.bytes_mut()[..5].copy_from_slice(b"hello");
        producer.publish(claim, meta(5, 7));

        let got_meta = consumer.stage_ready().expect("batch must be visible");
        assert_eq!(consumer.staged(), 1);
        assert_eq!(consumer.staged_bytes(0), b"hello");
        assert_eq!(got_meta.journal_seq, 7);
        assert_eq!(got_meta.ring_progress, 70);
        assert!(
            consumer.stage_ready().is_none(),
            "only one batch was published"
        );
    }

    /// The drain test is exact: it only reports drained once the
    /// consumer has committed, which the disk thread does after the
    /// sync. Reading alone must not satisfy it — that would let a
    /// rotation proceed over batches that are merely in the page cache.
    #[test]
    fn drain_waits_for_commit_not_merely_read() {
        let (mut producer, mut consumer) = build_journal_write_ring(4);
        assert!(producer.drained(), "an empty ring is drained");

        let claim = producer.try_claim().unwrap();
        producer.publish(claim, meta(0, 1));
        assert!(!producer.drained(), "published but not consumed");

        consumer.stage_ready().expect("batch available");
        assert!(
            !producer.drained(),
            "staging is not durable — drain must still block"
        );

        consumer.commit();
        assert!(producer.drained(), "commit releases the drain");
    }

    /// A full ring refuses the claim rather than overwriting a slot
    /// whose bytes are still awaiting durability.
    #[test]
    fn full_ring_refuses_the_claim() {
        let (mut producer, mut consumer) = build_journal_write_ring(2);

        for seq in 0..2 {
            let claim = producer.try_claim().unwrap();
            producer.publish(claim, meta(0, seq));
        }
        assert!(
            producer.try_claim().is_err(),
            "a full ring must refuse, never wrap onto unread bytes"
        );
        assert_eq!(producer.in_flight(), 2);

        // Releasing the staged run frees the claims it covered.
        consumer.stage_ready().expect("batches available");
        consumer.commit();
        assert!(producer.try_claim().is_ok());
    }

    /// Slots are reused as the ring wraps, and the bytes that come back
    /// out are the ones written for that lap — not a stale lap's.
    #[test]
    fn chunks_carry_fresh_bytes_across_wraps() {
        let (mut producer, mut consumer) = build_journal_write_ring(2);

        for lap in 0..6u8 {
            let mut claim = producer.try_claim().unwrap();
            claim.bytes_mut()[..4].copy_from_slice(&[lap; 4]);
            producer.publish(claim, meta(4, lap as u64));

            let got = consumer.stage_ready().expect("batch available");
            assert_eq!(
                consumer.staged_bytes(0),
                &[lap; 4],
                "lap {lap} read stale bytes"
            );
            assert_eq!(got.journal_seq, lap as u64);
            consumer.commit();
        }
    }

    /// One commit releases every slot read since the last one — the
    /// disk thread reads a backlog, syncs once, then releases the lot.
    #[test]
    fn one_commit_releases_every_slot_read() {
        let (mut producer, mut consumer) = build_journal_write_ring(4);

        for seq in 0..3 {
            let claim = producer.try_claim().unwrap();
            producer.publish(claim, meta(0, seq));
        }
        // One staging pass takes all three — that is what lets the disk
        // thread hold every slice at once for a single vectored write.
        let last = consumer.stage_ready().expect("batches available");
        assert_eq!(consumer.staged(), 3);
        assert_eq!(last.journal_seq, 2, "the last descriptor is returned");
        assert!(!producer.drained());

        consumer.commit();
        assert_eq!(consumer.staged(), 0);
        assert!(producer.drained(), "all three slots released together");
    }
}
