//! The journal's stream half: sequence allocation, entry framing, the
//! segment hash chain, and the batch buffer they accumulate into.
//!
//! [`JournalEncoder`] turns events into the exact bytes that belong on
//! disk and hands them over as a slice. It never opens, writes, or syncs
//! a file — that is [`crate::segment_file::SegmentFile`]'s half, and the
//! split is the line the pipeline draws between its sequencing thread
//! and its disk thread.
//!
//! Distinct from [`crate::codec`], which is the pure framing function:
//! the codec encodes one entry into a buffer, the encoder owns the
//! sequence counter, the chain state, and the batch the codec's output
//! accumulates into.
//!
//! [`crate::buffered_writer::BufferedWriter`] composes this half with
//! `SegmentFile` back into the single-threaded writer that recovery,
//! tooling, and tests drive.

use std::marker::PhantomData;
use std::path::Path;

use melin_app::AppEvent;

#[cfg(feature = "hash-chain")]
use crate::chain::SegmentChain;
use crate::codec;
#[cfg(feature = "hash-chain")]
use crate::codec::ENTRY_OFFSET;
use crate::error::JournalError;
use crate::event::JournalEvent;

/// Maximum encoded entry size. Mirrors `writer::MAX_ENTRY_SIZE` — actual
/// entries are ~81-101 bytes; the array is sized generously so the
/// per-event encode scratch never spills to the heap.
const MAX_ENTRY_SIZE: usize = 144;

/// Batch buffer capacity. Sized so the pipeline's normal flush cadence
/// never has to grow it.
const BATCH_BUF_CAPACITY: usize = 512 * 1024;

/// Sequencing, framing, and chaining for one journal segment.
pub struct JournalEncoder<E: AppEvent> {
    // PhantomData carries the app event type for the methods that
    // encode `JournalEvent<E>`. Zero-size — no runtime cost.
    _marker: PhantomData<fn(E) -> E>,
    // Scratch buffer for single-entry encoding. Fixed-size array — entry
    // sizes are bounded, so avoiding a Vec lets the hot path stay
    // allocation-free.
    buffer: [u8; MAX_ENTRY_SIZE],
    // Batch write buffer. Plain Vec<u8> because the page-cache path has
    // no alignment requirement. Pre-reserved to BATCH_BUF_CAPACITY at
    // construction; clearing only resets `batch_len`, not capacity.
    batch_buf: Vec<u8>,
    // Number of valid bytes in `batch_buf`. Acts as the write cursor —
    // new entries land at `batch_buf[batch_len..]`. Reset on every flush.
    batch_len: usize,
    next_sequence: u64,
    // First sequence of the active segment (the header's
    // `starting_sequence`), kept in memory so emptiness / rotation-
    // boundary checks need no header re-read.
    starting_sequence: u64,
    #[cfg(feature = "hash-chain")]
    hash_chain: SegmentChain,
    // Debug-only monotonicity guard: every fresh seq must strictly
    // exceed this. Excluded from release builds — zero hot-path cost.
    #[cfg(debug_assertions)]
    last_encoded_seq: u64,
    // Byte range of the most-recent user entry within `batch_buf` —
    // `last_user_entry_replication_slice` ships it to replication
    // without a second encode pass.
    last_user_entry_offset: usize,
    last_user_entry_len: usize,
}

impl<E: AppEvent> JournalEncoder<E> {
    /// Start a stream at the beginning of a segment: the next event
    /// gets `starting_sequence`, and the chain starts at `anchor_hash`
    /// (the previous segment's tail, or random salt for a brand-new
    /// journal).
    pub fn new(starting_sequence: u64, anchor_hash: [u8; 32]) -> Self {
        // The chain is the anchor's only consumer; with `hash-chain`
        // compiled out the parameter stays in the signature so callers
        // don't have to be feature-aware.
        #[cfg(not(feature = "hash-chain"))]
        let _ = anchor_hash;
        Self {
            _marker: PhantomData,
            buffer: [0u8; MAX_ENTRY_SIZE],
            batch_buf: vec![0u8; BATCH_BUF_CAPACITY],
            batch_len: 0,
            next_sequence: starting_sequence,
            starting_sequence,
            #[cfg(feature = "hash-chain")]
            hash_chain: SegmentChain::new(anchor_hash),
            #[cfg(debug_assertions)]
            last_encoded_seq: 0,
            last_user_entry_offset: 0,
            last_user_entry_len: 0,
        }
    }

    /// Resume a stream partway through a segment after recovery.
    ///
    /// The hash chain is rebuilt self-containedly: the anchor comes from
    /// the file header and the hasher re-absorbs the raw byte range
    /// `[ENTRY_OFFSET, valid_end)` of `path` — the chain is a pure
    /// function of those two inputs, so no chain state needs to be
    /// threaded in from the recovery walk. (Reading those bytes is the
    /// one place this half touches a file, and it is a one-shot read at
    /// startup, not ownership of the descriptor.)
    pub fn resume(
        path: &Path,
        starting_sequence: u64,
        anchor_hash: [u8; 32],
        last_seq: u64,
        valid_end: u64,
    ) -> Result<Self, JournalError> {
        // Chain-rebuild inputs only — see `new`.
        #[cfg(not(feature = "hash-chain"))]
        let _ = (path, anchor_hash, valid_end);
        Ok(Self {
            _marker: PhantomData,
            buffer: [0u8; MAX_ENTRY_SIZE],
            batch_buf: vec![0u8; BATCH_BUF_CAPACITY],
            batch_len: 0,
            next_sequence: last_seq + 1,
            starting_sequence,
            #[cfg(feature = "hash-chain")]
            hash_chain: SegmentChain::rebuild_from_file(
                path,
                anchor_hash,
                ENTRY_OFFSET,
                valid_end,
            )?,
            #[cfg(debug_assertions)]
            last_encoded_seq: last_seq,
            last_user_entry_offset: 0,
            last_user_entry_len: 0,
        })
    }

    /// Re-anchor the stream onto a fresh segment after a rotation: the
    /// chain restarts from `anchor_hash` (the outgoing segment's tail)
    /// and the batch is empty.
    ///
    /// No sequence is consumed — the next event still gets
    /// `starting_sequence`. Keeps the existing `batch_buf` allocation
    /// rather than building a throwaway replacement per rotation.
    pub fn begin_segment(&mut self, starting_sequence: u64, anchor_hash: [u8; 32]) {
        self.starting_sequence = starting_sequence;
        self.batch_len = 0;
        self.last_user_entry_offset = 0;
        self.last_user_entry_len = 0;
        #[cfg(feature = "hash-chain")]
        {
            self.hash_chain = SegmentChain::new(anchor_hash);
        }
        #[cfg(debug_assertions)]
        {
            self.last_encoded_seq = 0;
        }
        // Silences the unused-parameter warning when the chain is
        // compiled out; the anchor has no other consumer here.
        #[cfg(not(feature = "hash-chain"))]
        let _ = anchor_hash;
    }

    /// Allocate and return the next sequence number, advancing the
    /// internal counter.
    pub fn allocate_sequence(&mut self) -> u64 {
        let seq = self.next_sequence;
        self.next_sequence += 1;
        seq
    }

    /// Encode a single event with a pre-assigned sequence number.
    ///
    /// Does not advance the internal sequence counter — the caller
    /// owns sequencing (via [`allocate_sequence`](Self::allocate_sequence)
    /// on the primary or [`set_next_sequence`](Self::set_next_sequence)
    /// on a replica). The entry's raw bytes are absorbed into the
    /// segment hash chain; nothing else is emitted — the chain has no
    /// in-stream metadata.
    pub fn encode_event(
        &mut self,
        seq: u64,
        timestamp_ns: u64,
        event: &JournalEvent<E>,
        key_hash: u64,
        request_seq: u64,
    ) -> Result<(), JournalError> {
        #[cfg(debug_assertions)]
        {
            debug_assert!(
                seq > self.last_encoded_seq,
                "encode_event: seq {seq} <= last_encoded_seq {} — \
                 this would emit a duplicate/backward sequence",
                self.last_encoded_seq
            );
            self.last_encoded_seq = seq;
        }

        let written = codec::encode(
            seq,
            timestamp_ns,
            key_hash,
            request_seq,
            event,
            &mut self.buffer,
        )?;

        // Absorb the full on-disk bytes (incl. CRC) — see crate::chain
        // for why the CRC is included.
        #[cfg(feature = "hash-chain")]
        self.hash_chain.absorb(&self.buffer[..written]);

        self.reserve_batch(written);
        let offset = self.batch_len;
        self.last_user_entry_offset = offset;
        self.batch_buf[offset..offset + written].copy_from_slice(&self.buffer[..written]);
        self.last_user_entry_len = written;
        self.batch_len += written;

        Ok(())
    }

    /// Grow the batch buffer if the incoming bytes wouldn't fit. The
    /// pre-reserved capacity covers the pipeline's normal flush cadence,
    /// so this is the rare oversize-batch fallback — Vec's amortised
    /// growth absorbs the cost.
    #[inline]
    fn reserve_batch(&mut self, adding: usize) {
        let needed = self.batch_len + adding;
        if needed > self.batch_buf.len() {
            tracing::warn!(
                current_len = self.batch_len,
                adding,
                capacity = self.batch_buf.len(),
                "buffered journal batch exceeded preallocated capacity — \
                 caller is batching more than capacity between flushes; \
                 raise BATCH_BUF_CAPACITY or flush more often"
            );
            self.batch_buf.resize(needed, 0);
        }
    }

    /// Encoded bytes accumulated since the last flush — what the disk
    /// half writes.
    pub fn pending_batch_bytes(&self) -> &[u8] {
        &self.batch_buf[..self.batch_len]
    }

    /// Drop the pending batch. Called once its bytes have been written
    /// (or, under `no-persist`, deliberately discarded) — the buffer
    /// keeps its capacity either way.
    pub fn clear_batch(&mut self) {
        self.batch_len = 0;
        self.last_user_entry_len = 0;
    }

    /// Sequence number the next [`allocate_sequence`](Self::allocate_sequence)
    /// call will return.
    pub fn next_sequence(&self) -> u64 {
        self.next_sequence
    }

    /// Set the next sequence number — used by the replica receiver to
    /// keep the counter aligned with primary-assigned sequences.
    pub fn set_next_sequence(&mut self, seq: u64) {
        debug_assert!(
            seq >= self.next_sequence,
            "set_next_sequence({seq}) moves counter backward from {}",
            self.next_sequence
        );
        self.next_sequence = seq;
    }

    /// First sequence of the active segment (the header's
    /// `starting_sequence`). `next_sequence() == segment_starting_sequence()`
    /// means the live segment is empty.
    pub fn segment_starting_sequence(&self) -> u64 {
        self.starting_sequence
    }

    /// Current chain value: `BLAKE3(entry bytes so far || anchor)`, or
    /// the anchor itself for an empty segment. `None` when `hash-chain`
    /// is disabled. Non-destructive (clone + finalize).
    pub fn chain_hash(&self) -> Option<[u8; 32]> {
        #[cfg(feature = "hash-chain")]
        {
            Some(self.hash_chain.value())
        }
        #[cfg(not(feature = "hash-chain"))]
        None
    }

    /// The most-recent user entry's full on-disk bytes, magic and CRC
    /// included. Test-only counterpart to
    /// [`last_user_entry_replication_slice`](Self::last_user_entry_replication_slice),
    /// which the replication-framing test compares against.
    #[cfg(test)]
    pub(crate) fn last_user_entry_bytes(&self) -> &[u8] {
        let start = self.last_user_entry_offset;
        &self.batch_buf[start..start + self.last_user_entry_len]
    }

    /// Slice of the most-recent user entry, with the 2-byte magic
    /// stripped from the front and the 4-byte CRC stripped from the
    /// back — exact wire shape consumed by the replication stage.
    pub fn last_user_entry_replication_slice(&self) -> &[u8] {
        if self.last_user_entry_len == 0 {
            return &[];
        }
        let start = self.last_user_entry_offset;
        let end = start + self.last_user_entry_len;
        &self.batch_buf[start + 2..end - 4]
    }
}
