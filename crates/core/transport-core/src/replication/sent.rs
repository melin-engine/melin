//! Sent high-water tracking for one replica connection.
//!
//! [`SentHighWater`] is the primary's record of the highest sequence it
//! has actually streamed to a replica — the bound
//! [`ReplicaCursors::record_ack`] checks acks against, and the value
//! heartbeats advertise. The type exists for the same reason
//! [`ReplicaCursors`] does: the bound is load-bearing (an ack beyond it
//! evicts the replica), so its updates must be impossible to get wrong
//! at the call sites. Two guarantees by construction:
//!
//! - **Monotonic within a connection** — [`SentHighWater::advance`] is
//!   a saturating max, so a ring chunk already covered by catch-up can
//!   never regress the bound below the catch-up end. A regression
//!   would make the replica's next truthful ack (`in_memory` up to the
//!   catch-up end) look like a protocol violation and falsely evict a
//!   healthy replica — exactly the "never a load effect" guarantee the
//!   ack-sanity invariant promises.
//! - **One overlap-drain implementation** — [`SentHighWater::drain_overlap`]
//!   derives its skip/forward decision from the bound itself, so the
//!   kernel-TCP and DPDK senders cannot diverge on the drain condition
//!   (each previously restated it, and the two copies had already
//!   drifted: TCP compared against the catch-up end, DPDK against the
//!   handshake sequence).
//!
//! [`ReplicaCursors`]: super::cursors::ReplicaCursors
//! [`ReplicaCursors::record_ack`]: super::cursors::ReplicaCursors::record_ack

use melin_journal::replication::ReplicationConsumer;

/// Highest primary sequence streamed to one replica connection.
///
/// Plain `u64`, not atomic: the value is owned by the slot's sender
/// (the per-slot handler thread on the kernel-TCP path, the
/// single-threaded driver loop on the DPDK path) and never shared —
/// unlike [`ReplicaCursors`], whose cursors are read concurrently by
/// the response gate and the health endpoint.
///
/// [`ReplicaCursors`]: super::cursors::ReplicaCursors
pub struct SentHighWater(u64);

impl SentHighWater {
    /// Seed after handshake + catch-up: everything up to `catchup_end`
    /// has been streamed. `CatchUpResult::Ok` is monotonic from the
    /// handshake value, so the max normally resolves to `catchup_end`;
    /// taking it explicitly makes that assumption load-bearing instead
    /// of implicit.
    pub fn seed(handshake_last_sequence: u64, catchup_end: u64) -> Self {
        Self(handshake_last_sequence.max(catchup_end))
    }

    /// Record that sequences up to `end_sequence` have been streamed.
    /// Saturating max — an already-covered chunk must not regress the
    /// bound (see the module docs for why a regression is an eviction
    /// hazard).
    #[inline]
    pub fn advance(&mut self, end_sequence: u64) {
        self.0 = self.0.max(end_sequence);
    }

    /// The current bound — passed to `record_ack` as
    /// `highest_sent_sequence` and advertised in heartbeats.
    #[inline]
    pub fn get(&self) -> u64 {
        self.0
    }

    /// Drain ring entries already covered by what has been streamed
    /// (catch-up may overlap the ring), then forward the first chunk
    /// beyond the high-water and stop — later entries stay in the ring
    /// for the live streaming loop. Ring chunks are wire-ready
    /// `InputBatch` frames; `forward` pushes the bytes onto the
    /// transport as-is.
    ///
    /// The first uncovered chunk is forwarded here rather than left for
    /// the live loop because the two-phase consumer has no un-peek:
    /// once `try_read` has returned a chunk, the only way to leave the
    /// ring usable is to `commit` (consume) it. An uncommitted peek
    /// would be silently skipped by the live loop's next `try_read`,
    /// dropping the chunk's bytes from the wire entirely — a replica
    /// stream gap, not a latency detail.
    ///
    /// On `Err` from `forward`, the failing chunk is left uncommitted;
    /// callers tear the connection down on drain failure and the slot
    /// cleanup's `skip_to_producer` resets the pending read.
    pub fn drain_overlap<E>(
        &mut self,
        consumer: &mut ReplicationConsumer,
        mut forward: impl FnMut(&[u8]) -> Result<(), E>,
    ) -> Result<(), E> {
        while let Some((meta, data)) = consumer.try_read() {
            if meta.end_sequence > self.0 {
                forward(data)?;
                consumer.commit();
                self.advance(meta.end_sequence);
                break;
            }
            consumer.commit();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use melin_journal::replication::build_replication_ring;

    /// Single-consumer ring pre-loaded with `(end_sequence, payload)`
    /// chunks.
    fn ring_with_chunks(chunks: &[(u64, &[u8])]) -> ReplicationConsumer {
        let (mut producer, mut consumers) = build_replication_ring(1, 8);
        for &(end_sequence, data) in chunks {
            producer.publish(data, end_sequence);
        }
        consumers.pop().expect("ring built with one consumer")
    }

    /// Infallible forwarder that records what was sent.
    fn recording_forward(sent: &mut Vec<Vec<u8>>) -> impl FnMut(&[u8]) -> Result<(), ()> + '_ {
        |data| {
            sent.push(data.to_vec());
            Ok(())
        }
    }

    #[test]
    fn seed_takes_the_max_of_handshake_and_catchup_end() {
        assert_eq!(SentHighWater::seed(100, 300).get(), 300);
        // Defensive: catch-up is monotonic from the handshake value,
        // but if that ever breaks the seed must not regress.
        assert_eq!(SentHighWater::seed(100, 50).get(), 100);
        assert_eq!(SentHighWater::seed(0, 0).get(), 0);
    }

    #[test]
    fn advance_never_regresses() {
        let mut sent = SentHighWater::seed(0, 10);
        sent.advance(25);
        assert_eq!(sent.get(), 25);
        // A lower end_sequence (re-sent / already-covered chunk) holds
        // the bound rather than regressing it.
        sent.advance(5);
        assert_eq!(sent.get(), 25);
    }

    #[test]
    fn drain_discards_chunks_covered_by_the_high_water() {
        let mut consumer = ring_with_chunks(&[(150, b"a"), (250, b"b")]);
        let mut sent = SentHighWater::seed(100, 300);
        let mut forwarded = Vec::new();
        sent.drain_overlap(&mut consumer, recording_forward(&mut forwarded))
            .expect("infallible forward");
        // Both chunks were already streamed by catch-up: nothing
        // forwarded, bound unchanged, ring fully drained.
        assert!(forwarded.is_empty());
        assert_eq!(sent.get(), 300);
        assert!(consumer.try_read().is_none());
    }

    #[test]
    fn drain_forwards_first_uncovered_chunk_then_stops() {
        let mut consumer = ring_with_chunks(&[(250, b"covered"), (350, b"live"), (400, b"later")]);
        let mut sent = SentHighWater::seed(100, 300);
        let mut forwarded = Vec::new();
        sent.drain_overlap(&mut consumer, recording_forward(&mut forwarded))
            .expect("infallible forward");
        // The covered chunk is discarded, the first chunk beyond the
        // high-water is forwarded and advances the bound, and the rest
        // stays in the ring for the live loop.
        assert_eq!(forwarded, vec![b"live".to_vec()]);
        assert_eq!(sent.get(), 350);
        let (meta, data) = consumer.try_read().expect("later chunk left for live loop");
        assert_eq!(meta.end_sequence, 400);
        assert_eq!(data, b"later");
        consumer.commit();
    }

    #[test]
    fn covered_chunk_does_not_regress_the_bound_below_catchup_end() {
        // The false-eviction hazard this type exists to prevent: a
        // stale ring chunk in (handshake, catchup_end] — published by
        // the journal stage racing a previous teardown's
        // skip_to_producer — must not drag the bound below the
        // catch-up end. If it did, the replica's next truthful ack
        // (in_memory = catchup_end) would exceed the bound and trip
        // the ack-sanity invariant, evicting a healthy replica.
        let mut consumer = ring_with_chunks(&[(200, b"stale")]);
        let mut sent = SentHighWater::seed(100, 300);
        let mut forwarded = Vec::new();
        sent.drain_overlap(&mut consumer, recording_forward(&mut forwarded))
            .expect("infallible forward");
        assert!(forwarded.is_empty(), "covered chunk must not be re-sent");
        assert_eq!(sent.get(), 300, "bound must hold at the catch-up end");
    }

    #[test]
    fn drain_discards_chunk_ending_exactly_at_the_high_water() {
        // Boundary: a chunk ending exactly at the bound carries nothing
        // beyond what catch-up already streamed — discarded, not
        // re-sent (strict `>`). Mirrors the ack-sanity boundary in
        // `cursors`, where acking exactly the high-water is valid.
        let mut consumer = ring_with_chunks(&[(300, b"boundary")]);
        let mut sent = SentHighWater::seed(100, 300);
        let mut forwarded = Vec::new();
        sent.drain_overlap(&mut consumer, recording_forward(&mut forwarded))
            .expect("infallible forward");
        assert!(forwarded.is_empty());
        assert_eq!(sent.get(), 300);
        assert!(consumer.try_read().is_none());
    }

    #[test]
    fn drain_on_empty_ring_is_a_noop() {
        let mut consumer = ring_with_chunks(&[]);
        let mut sent = SentHighWater::seed(0, 42);
        let mut forwarded = Vec::new();
        sent.drain_overlap(&mut consumer, recording_forward(&mut forwarded))
            .expect("infallible forward");
        assert!(forwarded.is_empty());
        assert_eq!(sent.get(), 42);
    }

    #[test]
    fn drain_propagates_forward_errors_and_holds_the_bound() {
        let mut consumer = ring_with_chunks(&[(350, b"live")]);
        let mut sent = SentHighWater::seed(100, 300);
        let result = sent.drain_overlap(&mut consumer, |_| Err("socket gone"));
        assert_eq!(result, Err("socket gone"));
        // The failed chunk did not advance the bound; the caller tears
        // the connection down and skip_to_producer resets the ring.
        assert_eq!(sent.get(), 300);
        consumer.skip_to_producer();
        assert!(consumer.try_read().is_none());
    }
}
