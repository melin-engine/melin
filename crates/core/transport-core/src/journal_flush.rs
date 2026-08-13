//! What the journal stage publishes once a batch is durable, and who
//! holds the handles to publish it.
//!
//! The journal stage's loop used to be sequential: encode → `pwrite` →
//! `fdatasync` → publish cursors. Both I/O halves now live on the writer
//! thread (see [`crate::journal_writer`]), which means the values a
//! publication carries are sampled on one thread and published on
//! another. This module holds the two types that makes possible:
//!
//! - [`FlushWatermark`] — everything a publication needs, sampled at the
//!   moment the batch was handed over. **Sample before sync, publish the
//!   sample:** `fdatasync` covers only data dirtied before the call, so
//!   a value re-read afterwards could claim durability for bytes the
//!   sync never covered.
//! - [`CursorPublisher`] — the handles themselves, owned as a unit. The
//!   writer thread runs with no reference to the journal writer or the
//!   input-ring consumer, so collecting them here is what makes moving
//!   publication off the journal thread a change of *owner* rather than
//!   a change of behaviour. It also keeps the `FsyncState` seqlock's
//!   single-writer rule structural: this struct holds the only writer
//!   handle.
//!
//! See `docs/internal/journal-writer-thread-2026-08.md` for the design
//! argument.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use melin_pipeline::seqlock::{NoPadding, SeqLockWriter};

use crate::cursors::{RingPos, WireSeq};

/// Everything the writer thread needs to publish a completed flush,
/// sampled on the journal thread at handoff time.
///
/// The writer thread holds no reference to the journal writer or the
/// input-ring consumer, so every value it publishes must ride in here.
/// It is also `NoPadding` because the shadow stage reads the derived
/// `FsyncState` through a seqlock — tearing `journal_seq` against
/// `chain_hash` would hand a replica a mismatched handshake hash, the
/// exact TOCTOU that seqlock exists to prevent.
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct FlushWatermark {
    /// Monotonic submit counter, assigned by `WriteQueue::submit`. The
    /// space the self-clock and the shutdown drain wait in.
    ///
    /// Deliberately not `journal_seq`: a batch of queries advances ring
    /// progress without journaling anything, so `journal_seq` can repeat
    /// across submits. Gating on it would leave that batch's progress
    /// unpublished and eventually stall the producer against a ring that
    /// never drains.
    pub submit_seq: u64,
    /// Highest wire sequence covered by this watermark
    /// (`writer.next_sequence() - 1` at handoff).
    pub journal_seq: WireSeq,
    /// Input-ring position the flush covers — the value that becomes
    /// `Consumer::set_progress`. On a replica this is what gates
    /// persisted acks, so it must never be published before the sync.
    pub ring_progress: RingPos,
    /// `consumer.next_read()` at submit time, for `FsyncState`. **Not**
    /// the same value as `ring_progress`: at a mid-batch mark barrier
    /// only a prefix of the read batch is encoded, and the shadow
    /// snapshot compares this field for exact equality against its own
    /// cursor.
    pub input_ring_seq: RingPos,
    /// BLAKE3 chain hash after the batch. `[0u8; 32]` when hash-chain is
    /// disabled.
    pub chain_hash: [u8; 32],
    /// When the journal thread submitted this watermark. The writer
    /// thread measures against it to produce the submit→publish
    /// histogram — the direct read of "what would have been an inline
    /// stall", and the instrument the `--cores` placement experiment
    /// needs.
    ///
    /// `u64` under `latency-trace`, a zero-sized `()` otherwise, so the
    /// field costs nothing in production builds.
    pub submit_ts: crate::trace::MonoTraceInstant,
}

// Safety: `repr(C)` over padding-free fields — `WireSeq` and `RingPos`
// are `repr(transparent)` over `u64`, the rest are primitives and a byte
// array — with the assertion below proving the size equals the sum of
// the field sizes. Under `repr(C)`, that equality rules out padding.
unsafe impl NoPadding for FlushWatermark {}
// Compile-time proof for the impl above; fails the build if a future
// field introduces padding.
const _: () = assert!(
    size_of::<FlushWatermark>()
        == size_of::<u64>()
            + size_of::<WireSeq>()
            + size_of::<RingPos>()
            + size_of::<RingPos>()
            + size_of::<[u8; 32]>()
            + size_of::<crate::trace::MonoTraceInstant>()
);

/// The publication half of a flush: everything that follows an inline
/// `flush_batch_sync`.
///
/// Owns the handles rather than borrowing them from the journal stage,
/// because the writer thread runs with no reference to the journal
/// writer or the input-ring consumer. Collecting them here is what makes
/// the move off the journal thread a change of *owner* rather than a
/// change of behaviour — and it keeps the single-writer property on the
/// `FsyncState` seqlock structural, since this struct holds the only
/// writer handle.
pub struct CursorPublisher {
    /// The journal consumer's `processed` counter. Producers gate slot
    /// reuse on it and — load-bearing — the replica ack path gates
    /// persisted acks on it, so it must only ever advance behind a
    /// completed sync.
    progress: Arc<melin_pipeline::padding::Sequence>,
    /// Post-fsync state for the shadow snapshot stage and replica
    /// handshakes. `None` when shadow snapshots are disabled.
    fsync_state: Option<SeqLockWriter<crate::pipeline::FsyncState>>,
    /// Highest wire seq durably persisted, read by the durability gate,
    /// the health endpoint, and the replica reconnect handshake.
    durable_wire_seq: Option<crate::cursors::DurableWireSeqCursor>,
    /// Control-plane advertised tip. Wired on primaries only — on a
    /// replica the replication receiver owns the tip at its in-memory
    /// accepted position.
    advertised_tip: Option<crate::cursors::AdvertisedJournalTip>,
}

impl CursorPublisher {
    /// Build a publisher over the journal consumer's progress counter.
    /// The optional handles are installed separately, matching how the
    /// pipeline wires them.
    pub fn new(progress: Arc<melin_pipeline::padding::Sequence>) -> Self {
        Self {
            progress,
            fsync_state: None,
            durable_wire_seq: None,
            advertised_tip: None,
        }
    }

    pub fn set_fsync_state(&mut self, writer: SeqLockWriter<crate::pipeline::FsyncState>) {
        self.fsync_state = Some(writer);
    }

    pub fn set_durable_wire_seq(&mut self, cursor: crate::cursors::DurableWireSeqCursor) {
        self.durable_wire_seq = Some(cursor);
    }

    pub fn set_advertised_tip(&mut self, tip: crate::cursors::AdvertisedJournalTip) {
        self.advertised_tip = Some(tip);
    }

    /// Publish post-flush writer state *without* advancing ring
    /// progress.
    ///
    /// Used where the writer's durable state changed but no new input
    /// was consumed — after a rotation, whose fresh segment gives shadow
    /// observers a new genesis-anchored chain value.
    pub fn publish_state(&mut self, w: &FlushWatermark) {
        if let Some(ref mut publisher) = self.fsync_state {
            publisher.store(crate::pipeline::FsyncState {
                journal_seq: w.journal_seq,
                chain_hash: w.chain_hash,
                input_ring_seq: w.input_ring_seq,
            });
        }
        if let Some(ref cursor) = self.durable_wire_seq {
            cursor.store(w.journal_seq);
        }
        if let Some(ref tip) = self.advertised_tip {
            // `advance`, not a plain store: across a promotion the
            // receiver left the tip at its in-memory accepted position,
            // which the new primary's journal only reaches after the
            // drained ring is flushed — a plain store would regress the
            // advertised tip in that window.
            tip.advance(w.journal_seq);
        }
    }

    /// Full post-flush publication: ring progress first, then writer
    /// state.
    ///
    /// The order matches the inline path it replaces. Ring progress is
    /// the persist-before-ack boundary on replicas, so a caller that
    /// reaches this must have completed the watermark's sync.
    pub fn publish(&mut self, w: &FlushWatermark) {
        self.progress
            .get()
            .store(w.ring_progress.get(), Ordering::Release);
        self.publish_state(w);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publisher_advances_ring_progress_only_on_a_full_publish() {
        // `publish_state` is for the rotation paths, where the writer's
        // durable state changed but no new input was consumed. Advancing
        // ring progress there would publish a position no flush covers —
        // ack-before-persist on a replica.
        use crate::cursors::{AdvertisedJournalTip, DurableWireSeqCursor};
        use std::sync::atomic::AtomicU64;

        let progress = Arc::new(melin_pipeline::padding::Sequence::new(AtomicU64::new(0)));
        let durable = DurableWireSeqCursor::detached(WireSeq::new(0));
        let tip = AdvertisedJournalTip::new(WireSeq::new(0));

        let mut publisher = CursorPublisher::new(Arc::clone(&progress));
        publisher.set_durable_wire_seq(durable.clone());
        publisher.set_advertised_tip(tip.clone());

        let w = FlushWatermark {
            submit_seq: 1,
            journal_seq: WireSeq::new(77),
            ring_progress: RingPos::new(500),
            input_ring_seq: RingPos::new(505),
            chain_hash: [1u8; 32],
            submit_ts: crate::trace::mono_trace_ns(),
        };

        publisher.publish_state(&w);
        assert_eq!(
            progress.get().load(Ordering::Acquire),
            0,
            "publish_state must not move the persist-before-ack boundary"
        );
        assert_eq!(durable.load(), WireSeq::new(77));
        assert_eq!(tip.load(), WireSeq::new(77));

        publisher.publish(&w);
        assert_eq!(progress.get().load(Ordering::Acquire), 500);
    }
}
