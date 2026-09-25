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

use melin_app::{AppEvent, SequencerTime};

#[cfg(feature = "hash-chain")]
use crate::chain::SegmentChain;
use crate::codec;
#[cfg(feature = "hash-chain")]
use crate::codec::ENTRY_OFFSET;
use crate::error::JournalError;
use crate::event::JournalEvent;
use crate::time_floor::TimeFloor;

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
    // The journal's time floor: the stamp of the last entry encoded, or
    // the floor the stream was opened with before the first. Carried
    // across `begin_segment`, since time strictly increases across the
    // whole journal, not per segment.
    last_timestamp: SequencerTime,
    // Debug-only monotonicity guard: every fresh seq must strictly
    // exceed this. Excluded from release builds — zero hot-path cost.
    #[cfg(debug_assertions)]
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
    /// gets `starting_sequence`, the chain starts at `anchor_hash` (the
    /// previous segment's tail, or random salt for a brand-new journal),
    /// and `floor` is the time that precedes it.
    pub fn new(starting_sequence: u64, anchor_hash: [u8; 32], floor: TimeFloor) -> Self {
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
            last_timestamp: opened_floor(floor, starting_sequence),
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
    ///
    /// The time floor, on the other hand, is threaded in (`floor`, the
    /// stamp of the entry at `last_seq`): the chain rebuild does not
    /// parse entries, and the format cannot be read backwards to find the
    /// last one.
    pub fn resume(
        path: &Path,
        starting_sequence: u64,
        anchor_hash: [u8; 32],
        last_seq: u64,
        valid_end: u64,
        floor: TimeFloor,
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
            last_timestamp: opened_floor(floor, last_seq + 1),
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
    /// `starting_sequence`. The time floor is left alone: time strictly
    /// increases across segments, and this is the one piece of state a
    /// new segment does not reset.
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

    /// Encode a single event with a pre-assigned sequence number into
    /// `dst`, appending at the batch's current offset.
    ///
    /// Does not advance the internal sequence counter — the caller
    /// owns sequencing (via [`allocate_sequence`](Self::allocate_sequence)
    /// on the primary or [`set_next_sequence`](Self::set_next_sequence)
    /// on a replica). The entry's raw bytes are absorbed into the
    /// segment hash chain; nothing else is emitted — the chain has no
    /// in-stream metadata.
    ///
    /// `dst` must have [`entry_size::<E>()`](entry_size) bytes free past
    /// [`batch_len`](Self::batch_len) — the encoder cannot grow a
    /// buffer it does not own, so a short destination is a caller bug
    /// and is refused rather than silently truncated. It must also be
    /// the same buffer used for the rest of the batch (see the type
    /// docs).
    ///
    /// `timestamp` must be strictly later than the journal's floor
    /// ([`last_timestamp`](Self::last_timestamp)); anything else is
    /// refused with [`JournalError::TimestampRegression`], leaving the
    /// encoder unchanged. This is the one check behind every write: the
    /// journal stage on a primary and on a replica, and every writer
    /// built on this encoder.
    pub fn encode_event(
        &mut self,
        dst: &mut [u8],
        seq: u64,
        timestamp: SequencerTime,
        event: &JournalEvent<E>,
        key_hash: u64,
    ) -> Result<(), JournalError> {
        // Time strictly increases across the whole journal. Checked first,
        // so a refused entry leaves the encoder exactly as it was: chain,
        // batch and floor untouched.
        if timestamp <= self.last_timestamp {
            return Err(JournalError::TimestampRegression {
                sequence: seq,
                previous: self.last_timestamp,
                timestamp,
            });
        }
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

        let written = codec::encode(seq, timestamp.as_ns(), key_hash, event, &mut self.buffer)?;

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
        self.last_timestamp = timestamp;

        Ok(())
    }

    /// The journal's time floor: the stamp of the last entry encoded, or
    /// the floor the stream opened with before the first. The stamp the
    /// next entry must exceed.
    pub fn last_timestamp(&self) -> SequencerTime {
        self.last_timestamp
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
    pub(crate) fn last_user_entry_bytes<'a>(&self, dst: &'a [u8]) -> &'a [u8] {
        let start = self.last_user_entry_offset;
        &dst[start..start + self.last_user_entry_len]
    }

    /// Slice of the most-recent user entry within `dst`, with the
    /// 2-byte magic stripped from the front and the 4-byte CRC stripped
    /// from the back — exact wire shape consumed by the replication
    /// stage.
    pub fn last_user_entry_replication_slice<'a>(&self, dst: &'a [u8]) -> &'a [u8] {
        if self.last_user_entry_len == 0 {
            return &[];
        }
        let start = self.last_user_entry_offset;
        let end = start + self.last_user_entry_len;
        &dst[start + 2..end - 4]
    }
}

/// The floor a stream opens with, as a stamp. An unknown floor is the one
/// place the journal's time guarantee is waived, so it is said out loud.
fn opened_floor(floor: TimeFloor, next_sequence: u64) -> SequencerTime {
    if floor == TimeFloor::Unknown {
        tracing::warn!(
            next_sequence,
            "journal opened with no time floor: it continues from a snapshot that \
             predates recorded stamps, so the next entry's time is not checked \
             against the history before it"
        );
    }
    floor.time()
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

    fn stamp(ns: u64) -> SequencerTime {
        SequencerTime::from_ns(ns)
    }

    const ENTRY: JournalEvent<TinyEvent> = JournalEvent::App(TinyEvent);

    fn destination() -> Vec<u8> {
        vec![0u8; MAX_ENTRY_SIZE * 8]
    }

    /// An equal or earlier stamp is refused, and the refusal leaves the
    /// encoder as it was: nothing absorbed into the chain, nothing
    /// appended to the batch, the floor where it stood. The next entry
    /// then encodes as if the refused one had never been offered.
    #[test]
    fn a_stamp_not_later_than_the_floor_is_refused_and_changes_nothing() {
        let mut encoder = JournalEncoder::<TinyEvent>::new(1, [7; 32], TimeFloor::Genesis);
        let mut dst = destination();
        encoder
            .encode_event(&mut dst, 1, stamp(10), &ENTRY, 0)
            .unwrap();
        let before = (encoder.batch_len(), encoder.chain_hash());

        for refused in [stamp(10), stamp(9)] {
            match encoder.encode_event(&mut dst, 2, refused, &ENTRY, 0) {
                Err(JournalError::TimestampRegression {
                    sequence: 2,
                    previous,
                    timestamp,
                }) => {
                    assert_eq!(previous, stamp(10));
                    assert_eq!(timestamp, refused);
                }
                other => panic!("expected TimestampRegression, got {other:?}"),
            }
            assert_eq!((encoder.batch_len(), encoder.chain_hash()), before);
            assert_eq!(encoder.last_timestamp(), stamp(10));
        }
        encoder
            .encode_event(&mut dst, 2, stamp(11), &ENTRY, 0)
            .unwrap();
        assert_eq!(encoder.last_timestamp(), stamp(11));
    }

    /// The floor a stream opens with is enforced from the first entry:
    /// genesis refuses a zero stamp, and a stream resumed partway through
    /// history refuses anything not past the last stamp before it.
    #[test]
    fn the_floor_a_stream_opens_with_binds_its_first_entry() {
        let mut dst = destination();
        let mut genesis = JournalEncoder::<TinyEvent>::new(1, [7; 32], TimeFloor::Genesis);
        assert!(
            genesis
                .encode_event(&mut dst, 1, stamp(0), &ENTRY, 0)
                .is_err()
        );

        let mut dst = destination();
        let mut resumed =
            JournalEncoder::<TinyEvent>::new(40, [7; 32], TimeFloor::After(stamp(100)));
        assert!(
            resumed
                .encode_event(&mut dst, 40, stamp(100), &ENTRY, 0)
                .is_err()
        );
        resumed
            .encode_event(&mut dst, 40, stamp(101), &ENTRY, 0)
            .unwrap();
    }

    /// `begin_segment` resets the starting sequence, the batch and the
    /// chain, and must leave the floor alone: time strictly increases
    /// across segments. Resetting it with the rest is the natural mistake,
    /// and it would open a hole at every rotation on every node, primary
    /// and replica alike, with nothing else failing. Pinned here, on the
    /// method itself, because it has two callers (the writer's rotation
    /// and the journal stage's) and a test on either would leave the
    /// other unpinned.
    #[test]
    fn begin_segment_keeps_the_time_floor() {
        let mut encoder = JournalEncoder::<TinyEvent>::new(1, [7; 32], TimeFloor::Genesis);
        let mut dst = destination();
        encoder
            .encode_event(&mut dst, 1, stamp(10), &ENTRY, 0)
            .unwrap();
        encoder.clear_batch();

        encoder.begin_segment(2, [8; 32]);
        assert_eq!(encoder.last_timestamp(), stamp(10));
        assert!(
            encoder
                .encode_event(&mut dst, 2, stamp(5), &ENTRY, 0)
                .is_err(),
            "the new segment's first entry is judged against the old segment's last"
        );
        encoder
            .encode_event(&mut dst, 2, stamp(11), &ENTRY, 0)
            .unwrap();
    }
}
