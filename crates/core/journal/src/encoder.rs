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

/// Ceiling on one encoded entry, for **any** application.
///
/// This is the width of the encoder's scratch buffer — one per encoder,
/// not one per event — so it is nearly free to set generously, and it is
/// not what bounds memory. 1088 covers the largest entry a client can
/// induce: the runtime caps a client frame at 1024 bytes, of which 1 goes
/// to the tag, leaving a 1023-byte payload, a 1024-byte event and a
/// 1057-byte entry.
///
/// What an individual application costs is [`entry_size`], which is what
/// callers should reserve and what the transport divides a hand-off chunk
/// by. An app declaring an [`AppEvent::MAX_ENCODED_SIZE`] that does not
/// fit under this ceiling fails to compile — see [`JournalEncoder`].
///
/// Public because it is what the encoder's scratch is sized to, and
/// because the rings a batch lands in assert against it — a slot too
/// small for a single entry of *some* application would make batch sizing
/// compute a zero-length batch.
pub const MAX_ENTRY_SIZE: usize = 1088;

/// Bytes one entry of `E` can occupy: framing plus the widest payload
/// the journal can put in it.
///
/// That payload is the app's declared bound *or*
/// [`TRANSPORT_PAYLOAD_SIZE`](codec::TRANSPORT_PAYLOAD_SIZE), whichever
/// is larger — `Tick` and `EpochBump` are journaled whatever `E` is, so
/// an app narrower than 8 bytes still has to leave room for them.
///
/// This, not [`MAX_ENTRY_SIZE`], is the per-application reservation. An
/// app with 9-byte events reserves 42 bytes per entry and is unaffected
/// by another app's wider payloads.
pub const fn entry_size<E: AppEvent>() -> usize {
    // `Ord::max` is not const, hence the branch.
    let payload = if E::MAX_ENCODED_SIZE > codec::TRANSPORT_PAYLOAD_SIZE {
        E::MAX_ENCODED_SIZE
    } else {
        codec::TRANSPORT_PAYLOAD_SIZE
    };
    codec::ENTRY_FRAMING_SIZE + payload
}

/// Sequencing, framing, and chaining for one journal segment.
///
/// # The destination buffer
///
/// The encoder tracks *where* it is in the batch but does not own the
/// bytes: every call takes the destination. That is what lets the
/// pipeline encode straight into a hand-off ring slot while the
/// single-threaded writer encodes into its own `Vec` — one encoder, no
/// copy in either case.
///
/// The caller must pass the **same** destination for every event of a
/// batch, and must not disturb the bytes already in it; the offsets
/// this type records index into that buffer. A batch ends at
/// [`clear_batch`](Self::clear_batch), after which a different
/// destination is fine.
pub struct JournalEncoder<E: AppEvent> {
    // PhantomData carries the app event type for the methods that
    // encode `JournalEvent<E>`. Zero-size — no runtime cost.
    _marker: PhantomData<fn(E) -> E>,
    // Scratch buffer for single-entry encoding. Fixed-size array — entry
    // sizes are bounded, so avoiding a Vec lets the hot path stay
    // allocation-free.
    buffer: [u8; MAX_ENTRY_SIZE],
    // Bytes written into the caller's destination so far. Acts as the
    // write cursor — new entries land at `dst[batch_len..]`. Reset by
    // `clear_batch`.
    batch_len: usize,
    next_sequence: u64,
    // First sequence of the active segment (the header's
    // `starting_sequence`), kept in memory so emptiness / rotation-
    // boundary checks need no header re-read.
    starting_sequence: u64,
    #[cfg(feature = "hash-chain")]
    hash_chain: SegmentChain,
    // Monotonicity guard, in every build: each encoded seq must strictly
    // exceed this. One compare per entry buys a journal that cannot be
    // written out of order, which recovery would otherwise refuse
    // (audit finding 37). `u64` like every sequence.
    last_encoded_seq: u64,
    // Byte range of the most-recent user entry within the destination
    // buffer — `replication_slice` ships it to replication without a
    // second encode pass.
    last_user_entry_offset: usize,
    last_user_entry_len: usize,
}

impl<E: AppEvent> JournalEncoder<E> {
    /// Compile-time proof that `E`'s declared bound fits one entry.
    ///
    /// An associated const in a generic impl is only evaluated where it
    /// is used, so [`new`](Self::new) and [`resume`](Self::resume) force
    /// it. The effect is that an application declaring a
    /// `MAX_ENCODED_SIZE` the journal cannot carry fails to build,
    /// instead of failing on the journal thread the first time such an
    /// event is submitted.
    const FITS_ONE_ENTRY: () = assert!(
        entry_size::<E>() <= MAX_ENTRY_SIZE,
        "AppEvent::MAX_ENCODED_SIZE exceeds what one journal entry can \
         hold (MAX_ENTRY_SIZE minus framing)"
    );

    /// Start a stream at the beginning of a segment: the next event
    /// gets `starting_sequence`, and the chain starts at `anchor_hash`
    /// (the previous segment's tail, or random salt for a brand-new
    /// journal).
    pub fn new(starting_sequence: u64, anchor_hash: [u8; 32]) -> Self {
        let _: () = Self::FITS_ONE_ENTRY;
        // The chain is the anchor's only consumer; with `hash-chain`
        // compiled out the parameter stays in the signature so callers
        // don't have to be feature-aware.
        #[cfg(not(feature = "hash-chain"))]
        let _ = anchor_hash;
        Self {
            _marker: PhantomData,
            buffer: [0u8; MAX_ENTRY_SIZE],
            batch_len: 0,
            next_sequence: starting_sequence,
            starting_sequence,
            #[cfg(feature = "hash-chain")]
            hash_chain: SegmentChain::new(anchor_hash),
            // Nothing below the segment's first sequence belongs in it.
            last_encoded_seq: starting_sequence.saturating_sub(1),
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
        let _: () = Self::FITS_ONE_ENTRY;
        // Chain-rebuild inputs only — see `new`.
        #[cfg(not(feature = "hash-chain"))]
        let _ = (path, anchor_hash, valid_end);
        Ok(Self {
            _marker: PhantomData,
            buffer: [0u8; MAX_ENTRY_SIZE],
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
    /// `starting_sequence`. The monotonicity guard carries over: a new
    /// segment continues the sequence, it does not restart it.
    pub fn begin_segment(&mut self, starting_sequence: u64, anchor_hash: [u8; 32]) {
        self.starting_sequence = starting_sequence;
        self.batch_len = 0;
        self.last_user_entry_offset = 0;
        self.last_user_entry_len = 0;
        #[cfg(feature = "hash-chain")]
        {
            self.hash_chain = SegmentChain::new(anchor_hash);
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

    /// Take a sequence assigned elsewhere — a replica adopting the
    /// primary's numbering — and advance the counter past it, exactly as
    /// [`allocate_sequence`](Self::allocate_sequence) would have.
    ///
    /// It must be the next sequence. A replica's stream is contiguous,
    /// so anything else means entries sent twice or skipped upstream;
    /// journaling them would duplicate history or leave a hole, and the
    /// counter would move with them. Refused in every build with
    /// [`JournalError::ReplicaSequenceMismatch`], the counter left where
    /// it was.
    #[inline]
    pub fn adopt_sequence(&mut self, seq: u64) -> Result<u64, JournalError> {
        if seq != self.next_sequence {
            return Err(JournalError::ReplicaSequenceMismatch {
                expected: self.next_sequence,
                actual: seq,
            });
        }
        Ok(self.allocate_sequence())
    }

    /// Encode a single event with a pre-assigned sequence number into
    /// `dst`, appending at the batch's current offset.
    ///
    /// Does not advance the internal sequence counter — the caller
    /// owns sequencing (via [`allocate_sequence`](Self::allocate_sequence)
    /// on the primary or [`adopt_sequence`](Self::adopt_sequence) on a
    /// replica). The entry's raw bytes are absorbed into the segment
    /// hash chain; nothing else is emitted — the chain has no in-stream
    /// metadata.
    ///
    /// `seq` must exceed every sequence encoded before it, or the entry
    /// is refused with [`JournalError::SequenceRegression`]: written, it
    /// would make the journal unrecoverable.
    ///
    /// `dst` must have [`entry_size::<E>()`](entry_size) bytes free past
    /// [`batch_len`](Self::batch_len) — the encoder cannot grow a
    /// buffer it does not own, so a short destination is a caller bug
    /// and is refused rather than silently truncated. It must also be
    /// the same buffer used for the rest of the batch (see the type
    /// docs).
    ///
    /// A refused entry, for either reason, leaves the encoder exactly as
    /// it was, so the caller can retry it.
    pub fn encode_event(
        &mut self,
        dst: &mut [u8],
        seq: u64,
        timestamp_ns: u64,
        event: &JournalEvent<E>,
        key_hash: u64,
    ) -> Result<(), JournalError> {
        if seq <= self.last_encoded_seq {
            return Err(JournalError::SequenceRegression {
                sequence: seq,
                last_encoded: self.last_encoded_seq,
            });
        }

        let written = codec::encode(seq, timestamp_ns, key_hash, event, &mut self.buffer)?;

        let offset = self.batch_len;
        if dst.len() - offset < written {
            return Err(JournalError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "journal batch destination too small: {} bytes free at offset {offset}, \
                     entry needs {written} (caller must reserve entry_size::<E>() = {} \
                     per event)",
                    dst.len() - offset,
                    entry_size::<E>()
                ),
            )));
        }

        // Absorb the full on-disk bytes (incl. CRC) — see crate::chain
        // for why the CRC is included. After the capacity check, so a
        // refused entry leaves the chain untouched and the batch
        // re-encodable into a fresh destination.
        #[cfg(feature = "hash-chain")]
        self.hash_chain.absorb(&self.buffer[..written]);

        self.last_user_entry_offset = offset;
        dst[offset..offset + written].copy_from_slice(&self.buffer[..written]);
        self.last_user_entry_len = written;
        self.batch_len += written;
        // Only now: an entry refused above must stay encodable.
        self.last_encoded_seq = seq;

        Ok(())
    }

    /// Bytes encoded into the destination since the last
    /// [`clear_batch`](Self::clear_batch).
    pub fn batch_len(&self) -> usize {
        self.batch_len
    }

    /// The pending batch's bytes, read back out of the destination the
    /// caller has been encoding into — what the disk half writes.
    pub fn pending_batch_bytes<'a>(&self, dst: &'a [u8]) -> &'a [u8] {
        &dst[..self.batch_len]
    }

    /// End the batch. Called once its bytes have been written (or,
    /// under `no-persist`, deliberately discarded); the destination is
    /// the caller's to reuse or replace afterwards.
    pub fn clear_batch(&mut self) {
        self.batch_len = 0;
        self.last_user_entry_len = 0;
    }

    /// Sequence number the next [`allocate_sequence`](Self::allocate_sequence)
    /// call will return.
    pub fn next_sequence(&self) -> u64 {
        self.next_sequence
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
    pub(crate) fn last_user_entry_bytes<'a>(&self, dst: &'a [u8]) -> &'a [u8] {
        let start = self.last_user_entry_offset;
        &dst[start..start + self.last_user_entry_len]
    }

    /// Slice of the most-recent user entry within `dst`, with the
    /// 2-byte magic stripped from the front and the CRC trailer kept —
    /// exact wire shape consumed by the replication stage. The CRC is the
    /// one this entry is journaled with, so a replica can prove the entry
    /// it is about to journal is byte-identical to this one.
    pub fn last_user_entry_replication_slice<'a>(&self, dst: &'a [u8]) -> &'a [u8] {
        if self.last_user_entry_len == 0 {
            return &[];
        }
        let start = self.last_user_entry_offset;
        let end = start + self.last_user_entry_len;
        &dst[start + codec::ENTRY_MAGIC_SIZE..end]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::JournalEvent;
    use melin_app::CodecError;

    /// Variable-width event, so the difference between "what this value
    /// encodes to" and "what this type can encode to" is observable.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum VarEvent {
        Narrow,
        Wide,
    }

    impl AppEvent for VarEvent {
        const MAX_ENCODED_SIZE: usize = 64;

        fn encoded_size(&self) -> usize {
            match self {
                VarEvent::Narrow => 1,
                VarEvent::Wide => Self::MAX_ENCODED_SIZE,
            }
        }

        fn encode(&self, buf: &mut [u8]) -> usize {
            let n = self.encoded_size();
            buf[..n].fill(0x5A);
            n
        }

        fn decode(buf: &[u8]) -> Result<Self, CodecError> {
            match buf.len() {
                1 => Ok(VarEvent::Narrow),
                Self::MAX_ENCODED_SIZE => Ok(VarEvent::Wide),
                _ => Err(CodecError::Truncated),
            }
        }

        fn is_query(&self) -> bool {
            false
        }
    }

    /// Declares less than the 8-byte payload the transport's own
    /// variants carry, so "what the app can encode" and "what the widest
    /// entry costs" are different numbers.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct TinyEvent;

    impl AppEvent for TinyEvent {
        const MAX_ENCODED_SIZE: usize = 1;

        fn encoded_size(&self) -> usize {
            1
        }

        fn encode(&self, buf: &mut [u8]) -> usize {
            buf[0] = 0x5A;
            1
        }

        fn decode(_buf: &[u8]) -> Result<Self, CodecError> {
            Ok(TinyEvent)
        }

        fn is_query(&self) -> bool {
            false
        }
    }

    fn encode_len<E: AppEvent>(event: JournalEvent<E>) -> usize {
        let mut buf = [0u8; MAX_ENTRY_SIZE];
        crate::codec::encode(1, 0, 0, &event, &mut buf).expect("encodes")
    }

    fn encode_app_len(event: VarEvent) -> usize {
        encode_len(JournalEvent::App(event))
    }

    #[test]
    fn entry_size_is_framing_plus_the_declared_bound() {
        assert_eq!(
            entry_size::<VarEvent>(),
            crate::codec::ENTRY_FRAMING_SIZE + VarEvent::MAX_ENCODED_SIZE
        );
    }

    /// The declared bound must describe reality, not merely exceed it:
    /// the widest event has to encode to exactly `entry_size`. A bound
    /// that is too generous silently shortens every fsync batch.
    #[test]
    fn widest_event_encodes_to_exactly_entry_size() {
        assert_eq!(encode_app_len(VarEvent::Wide), entry_size::<VarEvent>());
    }

    #[test]
    fn entry_size_bounds_every_variant() {
        for event in [VarEvent::Narrow, VarEvent::Wide] {
            assert!(
                encode_app_len(event) <= entry_size::<VarEvent>(),
                "{event:?} encoded past the declared bound"
            );
        }
    }

    /// `entry_size` is what every caller reserves, and the journal writes
    /// more than app events: `Tick` and `EpochBump` carry an 8-byte
    /// payload whatever `E` is. An app narrower than that must still
    /// reserve enough for them, or a tick lands in a hole too small for
    /// it and a durable write fails.
    #[test]
    fn entry_size_bounds_the_transport_variants() {
        for event in [
            JournalEvent::<TinyEvent>::Tick { now_ns: u64::MAX },
            JournalEvent::<TinyEvent>::EpochBump { epoch: u64::MAX },
        ] {
            let len = encode_len(event);
            assert!(
                len <= entry_size::<TinyEvent>(),
                "{event:?} encodes to {len}, past the {} reserved per entry",
                entry_size::<TinyEvent>()
            );
        }
    }

    /// A narrow-event application must not be charged for the
    /// cross-application ceiling — that is the whole point of deriving
    /// the reservation from `E` rather than using `MAX_ENTRY_SIZE`.
    #[test]
    fn narrow_events_reserve_far_less_than_the_ceiling() {
        assert!(entry_size::<VarEvent>() < MAX_ENTRY_SIZE / 4);
    }

    const EVENT: JournalEvent<TinyEvent> = JournalEvent::App(TinyEvent);

    fn encoder_from(first: u64) -> JournalEncoder<TinyEvent> {
        JournalEncoder::new(first, [7u8; 32])
    }

    /// Room for a few entries: the destination a caller reserves.
    fn destination() -> Vec<u8> {
        vec![0u8; 4 * entry_size::<TinyEvent>()]
    }

    /// Audit finding 37: the guard used to be a `debug_assert!`, so a
    /// release build journaled a repeated or backward sequence and the
    /// journal then refused to recover. Refused in every build now, and
    /// the refusal writes nothing.
    #[test]
    fn a_repeated_or_backward_sequence_is_refused() {
        let mut encoder = encoder_from(1);
        let mut dst = destination();
        encoder.encode_event(&mut dst, 1, 0, &EVENT, 0).unwrap();
        encoder.encode_event(&mut dst, 2, 0, &EVENT, 0).unwrap();
        let before = (encoder.batch_len(), encoder.chain_hash());
        let bytes_before = dst.clone();
        for seq in [2, 1, 0] {
            assert!(
                matches!(
                    encoder.encode_event(&mut dst, seq, 0, &EVENT, 0),
                    Err(JournalError::SequenceRegression { sequence, last_encoded: 2 })
                        if sequence == seq
                ),
                "sequence {seq} after 2"
            );
        }
        assert_eq!(
            (encoder.batch_len(), encoder.chain_hash()),
            before,
            "a refused entry leaves the batch and the chain untouched"
        );
        assert_eq!(dst, bytes_before, "a refused entry writes nothing");
        encoder
            .encode_event(&mut dst, 3, 0, &EVENT, 0)
            .expect("the next sequence still encodes");
    }

    /// The guard starts below the segment's first sequence, not at zero,
    /// and a new segment continues it rather than resetting it: rotation
    /// continues the sequence.
    #[test]
    fn the_guard_starts_at_the_first_sequence_and_survives_rotation() {
        let mut encoder = encoder_from(5);
        let mut dst = destination();
        assert!(matches!(
            encoder.encode_event(&mut dst, 4, 0, &EVENT, 0),
            Err(JournalError::SequenceRegression {
                sequence: 4,
                last_encoded: 4
            })
        ));
        encoder.encode_event(&mut dst, 5, 0, &EVENT, 0).unwrap();
        encoder.clear_batch();

        encoder.begin_segment(6, [9u8; 32]);
        assert!(matches!(
            encoder.encode_event(&mut dst, 5, 0, &EVENT, 0),
            Err(JournalError::SequenceRegression {
                sequence: 5,
                last_encoded: 5
            })
        ));
        encoder
            .encode_event(&mut dst, 6, 0, &EVENT, 0)
            .expect("the new segment's first sequence encodes");
    }

    /// A resumed encoder guards from the recovered tail, not from zero.
    #[test]
    fn a_resumed_encoder_guards_from_the_recovered_tail() {
        let entry_offset = crate::codec::ENTRY_OFFSET;
        // An empty entry range: the chain rebuild reads nothing (the file
        // need not exist), so the guard is all this exercises.
        let path = std::path::Path::new("/nonexistent/resume.journal");
        let mut encoder =
            JournalEncoder::<TinyEvent>::resume(path, 1, [7u8; 32], 9, entry_offset).unwrap();
        assert!(matches!(
            encoder.encode_event(&mut destination(), 9, 0, &EVENT, 0),
            Err(JournalError::SequenceRegression {
                sequence: 9,
                last_encoded: 9
            })
        ));
        encoder
            .encode_event(&mut destination(), 10, 0, &EVENT, 0)
            .expect("the sequence after the tail encodes");
    }

    /// The guard advances only once an entry is actually encoded: an
    /// entry refused for lack of room must still encode, under the same
    /// sequence, into a destination that has room.
    #[test]
    fn an_entry_refused_for_space_stays_encodable() {
        let mut encoder = encoder_from(1);
        let mut short = vec![0u8; 1];
        assert!(matches!(
            encoder.encode_event(&mut short, 1, 0, &EVENT, 0),
            Err(JournalError::Io(_))
        ));
        encoder
            .encode_event(&mut destination(), 1, 0, &EVENT, 0)
            .expect("the same sequence encodes into a destination with room");
    }

    /// A replica adopts the primary's sequence only if it is the next
    /// one. A repeat, a step back and a skip ahead are all refused, and
    /// none of them moves the counter.
    #[test]
    fn adopting_a_sequence_requires_the_next_one() {
        let mut encoder = encoder_from(3);
        assert_eq!(encoder.adopt_sequence(3).unwrap(), 3);
        assert_eq!(encoder.next_sequence(), 4);
        for seq in [3, 2, 5] {
            assert!(
                matches!(
                    encoder.adopt_sequence(seq),
                    Err(JournalError::ReplicaSequenceMismatch { expected: 4, actual })
                        if actual == seq
                ),
                "sequence {seq} when 4 is next"
            );
            assert_eq!(encoder.next_sequence(), 4, "a refusal leaves the counter");
        }
        assert_eq!(encoder.adopt_sequence(4).unwrap(), 4);
    }
}
