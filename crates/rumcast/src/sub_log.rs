//! Subscription-side three-term log buffer.
//!
//! Mirror of [`crate::pub_log::PublicationLog`] but inverted for the
//! subscriber side: incoming UDP fragments arrive at random offsets
//! (they may be reordered or duplicated by the network) and the
//! subscriber drains contiguous fragments from the lowest unconsumed
//! position upward. Gaps in the byte stream block the subscriber's
//! progress until the missing fragments arrive (typically via NAK +
//! retransmit handled in the receiver loop, Task #7).
//!
//! # Concurrency model
//!
//! - **Single receiver** (the network thread) calls [`on_fragment`] to
//!   write incoming bytes into the right partition. It also performs
//!   term rotation when fragments for a future term arrive and the
//!   subscriber has consumed past the oldest resident term.
//! - **Single subscriber** (the application thread) calls [`poll`] to
//!   drain delivered fragments and advance [`subscriber_position`].
//! - Synchronization between the two:
//!     * Receiver writes fragment payload and header bytes 4..32 with
//!       non-atomic stores, then publishes the fragment by writing
//!       `frame_length` (the first 4 bytes of the header) with a
//!       `Release` store on `AtomicU32`.
//!     * Subscriber reads `frame_length` with an `Acquire` load — when
//!       it sees a non-zero value, all preceding writes are visible.
//!     * Receiver's rotation reads `subscriber_position` with `Acquire`
//!       to confirm the subscriber has consumed the term it wants to
//!       evict.
//!
//! [`on_fragment`]: SubscriptionLog::on_fragment
//! [`poll`]: SubscriptionLog::poll
//! [`subscriber_position`]: SubscriptionLog::subscriber_position

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use crate::pub_log::FRAGMENT_ALIGNMENT;
use crate::storage::{CachePadded, LogStorage, align_up};
use crate::wire::{
    DataFrame, FrameView, ParseError, data_flags, parse_frame, position, term_id_from_position,
    term_length_bits, term_offset_from_position,
};

/// Configuration for a [`SubscriptionLog`].
#[derive(Debug, Clone, Copy)]
pub struct SubscriptionConfig {
    pub session_id: u32,
    pub stream_id: u32,
    /// First term the subscriber expects to see. Typically learned from
    /// the publisher's `SetupFrame`.
    pub initial_term_id: u32,
    /// Bytes per term. Must match the publisher's term_length and pass
    /// [`term_length_bits`] validation.
    pub term_length: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigError {
    InvalidTermLength,
}

impl SubscriptionConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if term_length_bits(self.term_length).is_none() {
            return Err(ConfigError::InvalidTermLength);
        }
        Ok(())
    }
}

/// Outcome of [`SubscriptionLog::on_fragment`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcceptResult {
    /// Fragment written. Duplicates of an already-received fragment also
    /// return `Accepted` — the second write rewrites identical bytes.
    Accepted,
    /// `term_id` is older than what we still hold; the subscriber has
    /// already advanced past it. Silently dropped.
    TooOld,
    /// `term_id` is more than two terms ahead of the oldest resident
    /// term and the subscriber hasn't consumed enough to make room for
    /// it. Receiver should drop and rely on retransmit once the
    /// subscriber catches up.
    TooFarAhead,
    /// Fragment failed input validation (offset misaligned, length out
    /// of range, parse error, etc.).
    Malformed,
}

pub struct SubscriptionLog {
    config: SubscriptionConfig,
    term_length_bits: u32,

    storage: LogStorage,

    /// `term_id` currently held by each partition. Updated by the
    /// receiver during rotation. Subscriber reads with `Acquire` to
    /// locate the partition for its current position.
    term_ids: [CachePadded<AtomicU32>; 3],

    /// Highest byte offset any fragment has reached in each partition.
    /// Maintained by the receiver; the receiver loop (Task #7) uses
    /// this for gap detection when scheduling NAKs.
    high_water_marks: [CachePadded<AtomicU32>; 3],

    /// Highest contiguous position consumed by the subscriber. Written
    /// by [`poll`] (subscriber thread), read by [`on_fragment`] and by
    /// status-message generation (receiver thread).
    ///
    /// [`poll`]: Self::poll
    /// [`on_fragment`]: Self::on_fragment
    subscriber_position: CachePadded<AtomicU64>,
}

impl SubscriptionLog {
    pub fn new(config: SubscriptionConfig) -> Result<Self, ConfigError> {
        config.validate()?;
        let bits = term_length_bits(config.term_length).expect("validated above");
        let storage = LogStorage::new((config.term_length as usize) * 3);

        let start_position = position(config.initial_term_id, 0, bits);

        Ok(Self {
            config,
            term_length_bits: bits,
            storage,
            term_ids: [
                CachePadded::new(AtomicU32::new(config.initial_term_id)),
                CachePadded::new(AtomicU32::new(config.initial_term_id.wrapping_add(1))),
                CachePadded::new(AtomicU32::new(config.initial_term_id.wrapping_add(2))),
            ],
            high_water_marks: [
                CachePadded::new(AtomicU32::new(0)),
                CachePadded::new(AtomicU32::new(0)),
                CachePadded::new(AtomicU32::new(0)),
            ],
            subscriber_position: CachePadded::new(AtomicU64::new(start_position)),
        })
    }

    pub fn config(&self) -> &SubscriptionConfig {
        &self.config
    }

    pub fn term_length_bits(&self) -> u32 {
        self.term_length_bits
    }

    /// Highest contiguous position the subscriber has consumed up to.
    /// Used by status-message generation in the receiver loop.
    #[inline]
    pub fn subscriber_position(&self) -> u64 {
        self.subscriber_position.get().load(Ordering::Acquire)
    }

    /// Highest offset any fragment has touched in `partition`.
    /// `partition` must be in `0..3` — out-of-range panics.
    /// Used by the receiver loop's gap detection to schedule NAKs for
    /// missing offsets in `[subscriber_position, partition_hwm)`.
    pub fn partition_high_water_mark(&self, partition: usize) -> u32 {
        self.high_water_marks[partition]
            .get()
            .load(Ordering::Acquire)
    }

    /// `term_id` currently held by `partition`. `partition` must be in
    /// `0..3` — out-of-range panics.
    pub fn partition_term_id(&self, partition: usize) -> u32 {
        self.term_ids[partition].get().load(Ordering::Acquire)
    }

    /// Advance the high-water mark of the partition currently holding
    /// `term_id` to at least `term_offset`. Called by the receiver loop
    /// when a `SetupFrame` arrives advertising the publisher's current
    /// position — without this, a silently-dropped *tail* fragment
    /// (kernel rmem overflow, lost packet on the wire) is permanently
    /// undetected because `partition_high_water_mark` only advances on
    /// successfully-received data fragments. Bumping the HWM via Setup
    /// gives `detect_and_schedule_gap` something to NAK against.
    ///
    /// No-op if the partition isn't resident (a Setup for a not-yet-
    /// allocated term; data fragments will rotate on arrival and gap
    /// detection will fire then) or if HWM is already past `term_offset`.
    /// Does NOT rotate partitions itself: rotating speculatively on a
    /// control frame would risk evicting still-in-use payload.
    #[inline]
    pub fn advertise_publisher_position(&self, advert_term_id: u32, advert_term_offset: u32) {
        let bits = self.term_length_bits;
        let sub_pos = self.subscriber_position();
        if position(advert_term_id, advert_term_offset, bits) <= sub_pos {
            return;
        }
        let sub_term_id = (sub_pos >> bits) as u32;
        let term_length = self.config.term_length;

        // Determine the HWM bump for the subscriber's *current* term.
        // `detect_and_schedule_gap` scans the partition holding
        // `sub_term_id` — bumping HWM in any other partition is dead
        // code from the gap detector's perspective.
        //
        // If the publisher has rotated past `sub_term_id`, the rest of
        // sub_term is missing — bump HWM to full term_length so the
        // gap covers the tail. If the publisher is still in sub_term,
        // bump HWM to the advertised offset.
        let target_offset_in_sub_term = if advert_term_id == sub_term_id {
            advert_term_offset.min(term_length)
        } else {
            // advert_term_id > sub_term_id (advert_pos > sub_pos checked)
            term_length
        };

        if let Some(partition) = self.find_partition(sub_term_id) {
            let hwm = self.high_water_marks[partition].get();
            let cur = hwm.load(Ordering::Relaxed);
            if target_offset_in_sub_term > cur {
                // Release pairs with the Acquire in
                // `partition_high_water_mark` — gap detection on the
                // receiver loop reads HWM with Acquire.
                hwm.store(target_offset_in_sub_term, Ordering::Release);
            }
        }

        // If the publisher is in a term *past* sub_term, also bump HWM
        // in any intermediate or advertised terms that happen to be
        // resident. Subscriber will rotate to them only after consuming
        // the gap-filled fragments from sub_term, but having HWM ready
        // avoids a second-pass NAK delay once it does rotate.
        if advert_term_id != sub_term_id
            && let Some(p) = self.find_partition(advert_term_id)
        {
            let hwm = self.high_water_marks[p].get();
            let cur = hwm.load(Ordering::Relaxed);
            let clamped = advert_term_offset.min(term_length);
            if clamped > cur {
                hwm.store(clamped, Ordering::Release);
            }
        }
    }

    /// Write an incoming fragment into the log at `(term_id, term_offset)`.
    /// `fragment` must contain exactly one valid `DataFrame` whose header
    /// `frame_length` equals `fragment.len()`.
    ///
    /// Idempotent: receiving the same fragment twice rewrites identical
    /// bytes the second time.
    ///
    /// Defensive entry point: re-parses the fragment to confirm the
    /// header agrees with the caller-supplied `term_id` / `term_offset`
    /// and `fragment.len()`. Use this from callers whose upstream
    /// hasn't already parsed the fragment via [`parse_frame`].
    ///
    /// Receiver loops that already parsed once for dispatch should
    /// prefer [`on_fragment_parsed`] to skip the second parse — at
    /// high packet rates that duplicate parse adds ~15–25 ns per
    /// fragment.
    ///
    /// [`on_fragment_parsed`]: Self::on_fragment_parsed
    pub fn on_fragment(&self, term_id: u32, term_offset: u32, fragment: &[u8]) -> AcceptResult {
        // Re-parse the fragment to confirm its header agrees with the
        // length the caller supplied. Catches caller bugs at the cost
        // of one extra parse per fragment.
        let parsed = match parse_frame(fragment) {
            Ok(view) => view,
            Err(ParseError::Misaligned) => return AcceptResult::Malformed,
            Err(_) => return AcceptResult::Malformed,
        };
        let header = match parsed {
            FrameView::Data { header, .. } => header,
            _ => return AcceptResult::Malformed,
        };
        if header.term_id != term_id || header.term_offset != term_offset {
            return AcceptResult::Malformed;
        }
        // parse_frame guarantees `fragment.len() >= frame_length`; we
        // additionally require equality so callers don't smuggle
        // trailing bytes past the frame.
        if header.common.frame_length as usize != fragment.len() {
            return AcceptResult::Malformed;
        }
        self.on_fragment_parsed(term_id, term_offset, fragment)
    }

    /// Fast-path entry point: caller has already parsed the fragment
    /// via [`parse_frame`] and confirmed its header's `term_id` /
    /// `term_offset` agree with the arguments and that the slice is
    /// exactly `frame_length` bytes long. Skips the redundant
    /// `parse_frame` call but still does the cheap bounds /
    /// alignment checks that `parse_frame` doesn't cover (term_offset
    /// alignment, term_length bound) — so this remains memory-safe
    /// even if the caller's assertions are wrong: the worst case is
    /// a `Malformed` return.
    pub fn on_fragment_parsed(
        &self,
        term_id: u32,
        term_offset: u32,
        fragment: &[u8],
    ) -> AcceptResult {
        // Bounds + alignment that `parse_frame` does NOT enforce —
        // these protect the unchecked pointer writes below.
        if fragment.len() < DataFrame::HEADER_LEN {
            return AcceptResult::Malformed;
        }
        if !term_offset.is_multiple_of(FRAGMENT_ALIGNMENT) {
            return AcceptResult::Malformed;
        }
        let Some(frag_end) = (term_offset as usize).checked_add(fragment.len()) else {
            return AcceptResult::Malformed;
        };
        if frag_end > self.config.term_length as usize {
            return AcceptResult::Malformed;
        }
        let frame_length = fragment.len() as u32;

        // ---- Locate or rotate into a partition ----
        let partition = match self.find_partition(term_id) {
            Some(p) => p,
            None => match self.try_rotate_for(term_id) {
                Some(p) => p,
                None => {
                    // Determine whether it's too old or too far ahead
                    // for the diagnostic return value. The oldest term
                    // is the lowest term_id across partitions (ignoring
                    // wraparound; with u32 term_id and ~140-year
                    // lifetime per the wire docs, wraparound is not a
                    // v1 concern).
                    let mut oldest = u32::MAX;
                    for i in 0..3 {
                        let t = self.term_ids[i].get().load(Ordering::Acquire);
                        if t < oldest {
                            oldest = t;
                        }
                    }
                    return if term_id < oldest {
                        AcceptResult::TooOld
                    } else {
                        AcceptResult::TooFarAhead
                    };
                }
            },
        };

        // ---- Write payload, header bytes 4..32, then frame_length ----
        // SAFETY: bounds are confirmed by the validation above.
        let dst_base = unsafe {
            self.storage
                .as_ptr()
                .add(self.partition_base(partition) + term_offset as usize)
        };
        unsafe {
            // Header bytes after the 4-byte frame_length field.
            std::ptr::copy_nonoverlapping(
                fragment.as_ptr().add(4),
                dst_base.add(4),
                DataFrame::HEADER_LEN - 4,
            );
            // Payload bytes (everything past the 32-byte header).
            if (frame_length as usize) > DataFrame::HEADER_LEN {
                std::ptr::copy_nonoverlapping(
                    fragment.as_ptr().add(DataFrame::HEADER_LEN),
                    dst_base.add(DataFrame::HEADER_LEN),
                    fragment.len() - DataFrame::HEADER_LEN,
                );
            }
        }
        // SAFETY: dst_base is 32-byte aligned (offsets within a
        // 64-aligned partition base, and term_offset is itself 32-aligned
        // per validation above). u32 needs 4-byte alignment.
        let frame_length_atomic = unsafe { AtomicU32::from_ptr(dst_base as *mut u32) };
        // Release: pairs with the subscriber's Acquire load on the same
        // address; everything written above becomes visible.
        frame_length_atomic.store(frame_length, Ordering::Release);

        // ---- Update HWM ----
        let aligned_total = align_up(frame_length, FRAGMENT_ALIGNMENT);
        let new_hwm = term_offset + aligned_total;
        let hwm = self.high_water_marks[partition].get();
        let cur = hwm.load(Ordering::Relaxed);
        if new_hwm > cur {
            // Release pairs with the Acquire in `partition_high_water_mark`
            // so other threads (e.g. an external monitor) observing a
            // bumped HWM can rely on the fragment bytes having reached
            // the buffer too. Cost is zero on x86 and a single barrier
            // on ARM.
            hwm.store(new_hwm, Ordering::Release);
        }

        AcceptResult::Accepted
    }

    /// Drain contiguous fragments from `subscriber_position` upward.
    /// Calls `handler` once per delivered fragment with a borrowed
    /// [`FrameView`] over the log buffer. Padding frames are skipped
    /// silently (no handler call) but still advance position.
    ///
    /// Returns the number of bytes consumed (including padding and the
    /// header overhead of every delivered fragment). Stops when:
    /// - the next slot's `frame_length` is zero (gap or end of stream),
    /// - delivering the next fragment would exceed `max_bytes`, or
    /// - the next term's partition isn't resident in the log.
    ///
    /// **Single-subscriber contract**: only one thread may call `poll`
    /// concurrently. The receiver thread (which calls [`on_fragment`])
    /// is a separate thread; that's fine. Two callers of `poll` racing
    /// would double-deliver fragments and corrupt `subscriber_position`.
    ///
    /// [`on_fragment`]: Self::on_fragment
    pub fn poll<F: FnMut(FrameView<'_>)>(&self, max_bytes: u32, mut handler: F) -> u32 {
        let mut pos = self.subscriber_position.get().load(Ordering::Acquire);
        let mut consumed: u32 = 0;
        let term_length = self.config.term_length;

        while consumed < max_bytes {
            let term_id = term_id_from_position(pos, self.term_length_bits);
            let term_offset = term_offset_from_position(pos, self.term_length_bits);

            // Find the partition holding this term. If none does, we
            // either haven't received any fragment for it yet or the
            // receiver hasn't rotated to make room — either way, stop.
            let partition = match self.find_partition(term_id) {
                Some(p) => p,
                None => break,
            };

            // SAFETY: term_offset < term_length (strictly, since pos
            // less-than next term boundary). Slot pointer is 32-byte
            // aligned because term_offset is always 32-aligned (we
            // advance by aligned totals).
            let slot_ptr = unsafe {
                self.storage
                    .as_ptr()
                    .add(self.partition_base(partition) + term_offset as usize)
            };
            let frame_length_atomic = unsafe { AtomicU32::from_ptr(slot_ptr as *mut u32) };
            // Acquire: pairs with the receiver's Release store on the
            // same address. Once we observe a non-zero value, the rest
            // of the header and the payload are visible.
            let frame_length = frame_length_atomic.load(Ordering::Acquire);
            if frame_length == 0 {
                break;
            }
            // Sanity: a malformed frame_length would let us read past
            // the term — bail out rather than UB. Receiver should never
            // write a fragment that overflows its term, so this only
            // triggers on a bug.
            if (term_offset as u64) + (frame_length as u64) > term_length as u64 {
                break;
            }

            // SAFETY: frame_length is bounded by term_length above.
            let fragment_slice = unsafe {
                core::slice::from_raw_parts(slot_ptr as *const u8, frame_length as usize)
            };

            let view = match parse_frame(fragment_slice) {
                Ok(v) => v,
                Err(_) => break, // bug: unparsable bytes appeared in the log
            };

            let aligned_total = align_up(frame_length, FRAGMENT_ALIGNMENT);

            // Strict budget: do not overshoot `max_bytes`. If this
            // fragment would push us past, leave it for the next poll
            // call (position stays where it is, fragment is not
            // delivered). This may stall progress if `max_bytes` is
            // smaller than a single fragment — caller's responsibility
            // to size the budget appropriately. saturating_add guards
            // the arithmetic when `consumed` is near u32::MAX.
            if consumed.saturating_add(aligned_total) > max_bytes {
                break;
            }

            match view {
                FrameView::Data { header, .. } => {
                    if header.common.flags & data_flags::PADDING != 0 {
                        // Padding: advance without delivering.
                    } else {
                        handler(view);
                    }
                }
                _ => break, // only Data frames live in the log
            }

            pos += aligned_total as u64;
            consumed += aligned_total;
        }

        // Release: a subsequent `on_fragment` rotation reads
        // subscriber_position with Acquire to know what's safe to evict.
        self.subscriber_position.get().store(pos, Ordering::Release);
        consumed
    }

    fn partition_base(&self, partition: usize) -> usize {
        partition * (self.config.term_length as usize)
    }

    fn find_partition(&self, term_id: u32) -> Option<usize> {
        (0..3).find(|&i| self.term_ids[i].get().load(Ordering::Acquire) == term_id)
    }

    /// Try to rotate the partition holding the oldest term to hold
    /// `target_term_id`. Returns `Some(partition)` on success, `None`
    /// if the oldest term cannot yet be evicted (subscriber hasn't
    /// consumed past it) or if `target_term_id` isn't in the
    /// "oldest..oldest+2" sliding window we maintain.
    fn try_rotate_for(&self, target_term_id: u32) -> Option<usize> {
        // Find the partition holding the oldest term.
        let mut oldest_term = u32::MAX;
        let mut oldest_partition = 0usize;
        for i in 0..3 {
            let t = self.term_ids[i].get().load(Ordering::Acquire);
            if t < oldest_term {
                oldest_term = t;
                oldest_partition = i;
            }
        }

        // Only accept rotations into the "next" slot — i.e. the new
        // term must be exactly `oldest_term + 3` (so it slots into the
        // freed partition without skipping). Bigger jumps require
        // multiple rotations which the receiver loop must drive in
        // sequence.
        if target_term_id != oldest_term.wrapping_add(3) {
            return None;
        }

        // Subscriber must have consumed past the END of the oldest
        // term — that's the start of (oldest_term + 1).
        let evict_threshold = position(oldest_term.wrapping_add(1), 0, self.term_length_bits);
        let sub_pos = self.subscriber_position.get().load(Ordering::Acquire);
        if sub_pos < evict_threshold {
            return None;
        }

        // Reclaim the partition: zero its bytes (so stale data from
        // the previous term doesn't appear as a fragment for the new
        // term), reset HWM, then publish the new term_id with Release.
        // Zeroing inline takes O(term_length); a background cleaner is
        // a deferred optimization (Task DEFER #C analog).
        let base = self.partition_base(oldest_partition);
        // SAFETY: partition_base + term_length <= storage.len().
        unsafe {
            std::ptr::write_bytes(
                self.storage.as_ptr().add(base),
                0,
                self.config.term_length as usize,
            );
        }
        self.high_water_marks[oldest_partition]
            .get()
            .store(0, Ordering::Relaxed);
        // Release: pairs with the subscriber's Acquire on term_ids
        // (via find_partition). The subscriber must observe the new
        // term_id only after the zeroing is complete.
        self.term_ids[oldest_partition]
            .get()
            .store(target_term_id, Ordering::Release);

        Some(oldest_partition)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::{DataFrame, data_flags};

    fn cfg() -> SubscriptionConfig {
        SubscriptionConfig {
            session_id: 1,
            stream_id: 2,
            initial_term_id: 100,
            term_length: 64 * 1024,
        }
    }

    fn pos(term_id: u32, term_offset: u32) -> u64 {
        position(term_id, term_offset, 16)
    }

    /// Build a valid DataFrame fragment (header + payload) for a test.
    /// 8-byte aligned via the boxed slice's allocator.
    fn build_fragment(term_id: u32, term_offset: u32, flags: u8, payload: &[u8]) -> Vec<u8> {
        let header = DataFrame::new(1, 2, term_id, term_offset, flags, payload.len() as u32);
        let mut buf = Vec::with_capacity(DataFrame::HEADER_LEN + payload.len());
        buf.extend_from_slice(bytemuck::bytes_of(&header));
        buf.extend_from_slice(payload);
        buf
    }

    #[test]
    fn fresh_log_at_initial_position() {
        let log = SubscriptionLog::new(cfg()).unwrap();
        assert_eq!(log.subscriber_position(), pos(100, 0));
        assert_eq!(log.partition_term_id(0), 100);
        assert_eq!(log.partition_term_id(1), 101);
        assert_eq!(log.partition_term_id(2), 102);
        assert_eq!(log.partition_high_water_mark(0), 0);
    }

    #[test]
    fn invalid_term_length_rejected() {
        let mut bad = cfg();
        bad.term_length = 1234;
        assert_eq!(bad.validate(), Err(ConfigError::InvalidTermLength));
    }

    #[test]
    fn single_fragment_write_and_poll() {
        let log = SubscriptionLog::new(cfg()).unwrap();
        let payload = vec![0xAB; 64];
        let frag = build_fragment(100, 0, data_flags::UNFRAGMENTED, &payload);
        assert_eq!(log.on_fragment(100, 0, &frag), AcceptResult::Accepted);

        let mut delivered: Vec<Vec<u8>> = Vec::new();
        let consumed = log.poll(1024, |view| match view {
            FrameView::Data { payload, .. } => delivered.push(payload.to_vec()),
            _ => panic!("expected Data"),
        });

        // 32 header + 64 payload = 96 bytes (already aligned).
        assert_eq!(consumed, 96);
        assert_eq!(delivered.len(), 1);
        assert_eq!(delivered[0], payload);
        assert_eq!(log.subscriber_position(), pos(100, 0) + 96);
    }

    #[test]
    fn out_of_order_fragments_only_contiguous_prefix_consumed() {
        let log = SubscriptionLog::new(cfg()).unwrap();
        // Send fragment #2 before #1.
        let frag2 = build_fragment(100, 96, data_flags::UNFRAGMENTED, &[2u8; 64]);
        assert_eq!(log.on_fragment(100, 96, &frag2), AcceptResult::Accepted);

        let mut count = 0;
        log.poll(1024, |_| count += 1);
        // No delivery — gap at offset 0..96.
        assert_eq!(count, 0);
        assert_eq!(log.subscriber_position(), pos(100, 0));

        // Now send the missing fragment #1.
        let frag1 = build_fragment(100, 0, data_flags::UNFRAGMENTED, &[1u8; 64]);
        assert_eq!(log.on_fragment(100, 0, &frag1), AcceptResult::Accepted);

        let mut delivered: Vec<u8> = Vec::new();
        log.poll(1024, |view| match view {
            FrameView::Data { payload, .. } => delivered.push(payload[0]),
            _ => panic!("expected Data"),
        });
        assert_eq!(delivered, vec![1, 2]);
        assert_eq!(log.subscriber_position(), pos(100, 192));
    }

    #[test]
    fn duplicate_fragment_idempotent() {
        let log = SubscriptionLog::new(cfg()).unwrap();
        let frag = build_fragment(100, 0, data_flags::UNFRAGMENTED, &[0xCDu8; 32]);
        assert_eq!(log.on_fragment(100, 0, &frag), AcceptResult::Accepted);
        // Second write of the same bytes — accepted, no duplicate delivery.
        assert_eq!(log.on_fragment(100, 0, &frag), AcceptResult::Accepted);

        let mut count = 0;
        log.poll(1024, |_| count += 1);
        assert_eq!(count, 1, "duplicate fragment must not be delivered twice");
    }

    #[test]
    fn padding_frame_skipped_without_delivery() {
        let log = SubscriptionLog::new(cfg()).unwrap();
        // A padding frame at offset 0 spanning 64 bytes total.
        let frag = build_fragment(100, 0, data_flags::PADDING, &[0u8; 32]);
        assert_eq!(log.on_fragment(100, 0, &frag), AcceptResult::Accepted);

        let mut count = 0;
        let consumed = log.poll(1024, |_| count += 1);
        // Padding's not delivered, but position still advances.
        assert_eq!(count, 0);
        assert_eq!(consumed, 64);
        assert_eq!(log.subscriber_position(), pos(100, 64));
    }

    #[test]
    fn poll_respects_max_bytes() {
        let log = SubscriptionLog::new(cfg()).unwrap();
        for i in 0..5u32 {
            let frag = build_fragment(100, i * 96, data_flags::UNFRAGMENTED, &[i as u8; 64]);
            log.on_fragment(100, i * 96, &frag);
        }
        // max_bytes=200 fits two 96-byte fragments (192). Third would
        // overflow, so we stop.
        let mut count = 0;
        let consumed = log.poll(200, |_| count += 1);
        assert_eq!(count, 2);
        assert_eq!(consumed, 192);
        assert_eq!(log.subscriber_position(), pos(100, 192));
    }

    #[test]
    fn poll_crosses_term_boundary_via_padding() {
        let log = SubscriptionLog::new(cfg()).unwrap();
        // Padding frame fills the whole first term in one shot.
        let term_length = cfg().term_length;
        let pad_payload = vec![0u8; (term_length - DataFrame::HEADER_LEN as u32) as usize];
        let pad = build_fragment(100, 0, data_flags::PADDING, &pad_payload);
        log.on_fragment(100, 0, &pad);
        // Then a real fragment in term 101.
        let frag = build_fragment(101, 0, data_flags::UNFRAGMENTED, &[0xAA; 64]);
        log.on_fragment(101, 0, &frag);

        let mut count = 0;
        let consumed = log.poll(u32::MAX, |view| match view {
            FrameView::Data { header, .. } => {
                assert_eq!(header.term_id, 101);
                count += 1;
            }
            _ => panic!("expected Data"),
        });
        assert_eq!(count, 1);
        // term_length (padding) + 96 (fragment in term 101) — but max u32
        // limits us; the assertion is about total advance.
        assert_eq!(consumed, term_length + 96);
        assert_eq!(log.subscriber_position(), pos(101, 96));
    }

    #[test]
    fn fragment_with_misaligned_offset_rejected() {
        let log = SubscriptionLog::new(cfg()).unwrap();
        let frag = build_fragment(100, 16, data_flags::UNFRAGMENTED, &[0u8; 32]);
        assert_eq!(
            log.on_fragment(100, 16, &frag),
            AcceptResult::Malformed,
            "offset 16 is not a multiple of FRAGMENT_ALIGNMENT (32)"
        );
    }

    #[test]
    fn fragment_extending_past_term_rejected() {
        let log = SubscriptionLog::new(cfg()).unwrap();
        let term_length = cfg().term_length;
        // Offset near term-end with payload that overflows.
        let bad_offset = term_length - 64;
        let payload = vec![0u8; 128]; // header(32) + 128 = 160 > 64 residual
        let frag = build_fragment(100, bad_offset, data_flags::UNFRAGMENTED, &payload);
        assert_eq!(
            log.on_fragment(100, bad_offset, &frag),
            AcceptResult::Malformed
        );
    }

    #[test]
    fn fragment_with_mismatched_header_term_id_rejected() {
        let log = SubscriptionLog::new(cfg()).unwrap();
        // Build header claiming term 999 but pass it in as term 100.
        let bad_frag = build_fragment(999, 0, data_flags::UNFRAGMENTED, &[0u8; 32]);
        assert_eq!(log.on_fragment(100, 0, &bad_frag), AcceptResult::Malformed);
    }

    #[test]
    fn fragment_for_too_old_term_dropped() {
        let log = SubscriptionLog::new(cfg()).unwrap();
        // term 99 is below initial_term_id (100).
        let frag = build_fragment(99, 0, data_flags::UNFRAGMENTED, &[0u8; 32]);
        assert_eq!(log.on_fragment(99, 0, &frag), AcceptResult::TooOld);
    }

    #[test]
    fn fragment_for_far_ahead_term_when_subscriber_lagging() {
        let log = SubscriptionLog::new(cfg()).unwrap();
        // term 103 is one past the highest resident (102), but the
        // subscriber hasn't consumed any of term 100 yet, so the
        // partition holding 100 cannot be evicted.
        let frag = build_fragment(103, 0, data_flags::UNFRAGMENTED, &[0u8; 32]);
        assert_eq!(log.on_fragment(103, 0, &frag), AcceptResult::TooFarAhead);
    }

    #[test]
    fn fragment_more_than_one_term_jump_rejected_even_when_subscriber_caught_up() {
        let log = SubscriptionLog::new(cfg()).unwrap();
        let term_length = cfg().term_length;
        // Consume all of term 100 via padding + poll.
        let pad_payload = vec![0u8; (term_length - DataFrame::HEADER_LEN as u32) as usize];
        let pad = build_fragment(100, 0, data_flags::PADDING, &pad_payload);
        log.on_fragment(100, 0, &pad);
        log.poll(u32::MAX, |_| {});

        // Subscriber is at start of term 101. Resident terms are
        // {100, 101, 102}. Asking for term 104 means we'd need to skip
        // a rotation step (104 = 100 + 4, not 100 + 3). try_rotate_for
        // requires exactly +3, so this is rejected.
        let frag = build_fragment(104, 0, data_flags::UNFRAGMENTED, &[0u8; 32]);
        assert_eq!(log.on_fragment(104, 0, &frag), AcceptResult::TooFarAhead);
        // Partition assignments unchanged.
        assert_eq!(log.partition_term_id(0), 100);
    }

    #[test]
    fn rotation_at_exact_eviction_threshold() {
        let log = SubscriptionLog::new(cfg()).unwrap();
        let term_length = cfg().term_length;
        // Consume exactly to the term boundary — subscriber_position
        // == position(101, 0) == evict_threshold for evicting term 100.
        let pad_payload = vec![0u8; (term_length - DataFrame::HEADER_LEN as u32) as usize];
        let pad = build_fragment(100, 0, data_flags::PADDING, &pad_payload);
        log.on_fragment(100, 0, &pad);
        log.poll(u32::MAX, |_| {});
        assert_eq!(log.subscriber_position(), pos(101, 0));

        // At the exact threshold (sub_pos >= evict_threshold), rotation
        // proceeds — accept the fragment.
        let frag = build_fragment(103, 0, data_flags::UNFRAGMENTED, &[0xCCu8; 32]);
        assert_eq!(log.on_fragment(103, 0, &frag), AcceptResult::Accepted);
        assert_eq!(log.partition_term_id(0), 103);
    }

    #[test]
    fn rotation_when_subscriber_consumed_oldest_term() {
        let log = SubscriptionLog::new(cfg()).unwrap();
        let term_length = cfg().term_length;

        // Fill term 100 with one big padding frame, then poll past it
        // so subscriber_position lands at start of term 101.
        let pad_payload = vec![0u8; (term_length - DataFrame::HEADER_LEN as u32) as usize];
        let pad = build_fragment(100, 0, data_flags::PADDING, &pad_payload);
        log.on_fragment(100, 0, &pad);
        let consumed = log.poll(u32::MAX, |_| {});
        assert_eq!(consumed, term_length);
        assert_eq!(log.subscriber_position(), pos(101, 0));

        // Now a fragment for term 103 should rotate partition 0
        // (which held 100) to hold 103.
        let frag = build_fragment(103, 0, data_flags::UNFRAGMENTED, &[0xEEu8; 32]);
        assert_eq!(log.on_fragment(103, 0, &frag), AcceptResult::Accepted);
        assert_eq!(log.partition_term_id(0), 103);
        // Other partitions still hold their original assignments.
        assert_eq!(log.partition_term_id(1), 101);
        assert_eq!(log.partition_term_id(2), 102);
    }

    #[test]
    fn fragmentation_flags_round_trip_through_poll() {
        let log = SubscriptionLog::new(cfg()).unwrap();
        // Three fragments of one logical message: BEGIN, interior (0), END.
        let f1 = build_fragment(100, 0, data_flags::BEGIN_FRAGMENT, &[0x01u8; 64]);
        let f2 = build_fragment(100, 96, 0, &[0x02u8; 64]);
        let f3 = build_fragment(100, 192, data_flags::END_FRAGMENT, &[0x03u8; 64]);
        log.on_fragment(100, 0, &f1);
        log.on_fragment(100, 96, &f2);
        log.on_fragment(100, 192, &f3);

        let mut observed_flags: Vec<u8> = Vec::new();
        let mut observed_fills: Vec<u8> = Vec::new();
        log.poll(1024, |view| match view {
            FrameView::Data { header, payload } => {
                observed_flags.push(header.common.flags);
                observed_fills.push(payload[0]);
            }
            _ => panic!("expected Data"),
        });
        assert_eq!(
            observed_flags,
            vec![data_flags::BEGIN_FRAGMENT, 0, data_flags::END_FRAGMENT]
        );
        assert_eq!(observed_fills, vec![0x01, 0x02, 0x03]);
    }

    #[test]
    fn multi_gap_fill_delivers_in_order() {
        let log = SubscriptionLog::new(cfg()).unwrap();
        // Receive #1 and #3 (out of order, two gaps total).
        let f0 = build_fragment(100, 0, data_flags::UNFRAGMENTED, &[0xA0u8; 64]);
        let f2 = build_fragment(100, 192, data_flags::UNFRAGMENTED, &[0xA2u8; 64]);
        log.on_fragment(100, 0, &f0);
        log.on_fragment(100, 192, &f2);

        // Only #1 deliverable; gap blocks #3.
        let mut count = 0;
        log.poll(1024, |_| count += 1);
        assert_eq!(count, 1);
        assert_eq!(log.subscriber_position(), pos(100, 96));

        // Fill the gap with #2.
        let f1 = build_fragment(100, 96, data_flags::UNFRAGMENTED, &[0xA1u8; 64]);
        log.on_fragment(100, 96, &f1);

        // Now both #2 and #3 deliverable in one poll.
        let mut delivered: Vec<u8> = Vec::new();
        log.poll(1024, |view| match view {
            FrameView::Data { payload, .. } => delivered.push(payload[0]),
            _ => panic!("expected Data"),
        });
        assert_eq!(delivered, vec![0xA1, 0xA2]);
        assert_eq!(log.subscriber_position(), pos(100, 288));
    }

    #[test]
    fn high_water_mark_advances_with_each_fragment() {
        let log = SubscriptionLog::new(cfg()).unwrap();
        // Receive fragment at offset 96 first (out of order).
        let frag = build_fragment(100, 96, data_flags::UNFRAGMENTED, &[0u8; 64]);
        log.on_fragment(100, 96, &frag);
        // HWM should be at the END of that fragment (96 + 96 = 192).
        assert_eq!(log.partition_high_water_mark(0), 192);

        // Earlier fragment doesn't decrease HWM.
        let frag = build_fragment(100, 0, data_flags::UNFRAGMENTED, &[0u8; 64]);
        log.on_fragment(100, 0, &frag);
        assert_eq!(log.partition_high_water_mark(0), 192);

        // Later fragment extends HWM.
        let frag = build_fragment(100, 192, data_flags::UNFRAGMENTED, &[0u8; 64]);
        log.on_fragment(100, 192, &frag);
        assert_eq!(log.partition_high_water_mark(0), 288);
    }

    #[test]
    fn subscriber_position_visible_to_other_thread() {
        use std::sync::Arc;
        use std::thread;

        let log = Arc::new(SubscriptionLog::new(cfg()).unwrap());
        let target = pos(100, 0) + 96;

        let receiver = {
            let log = Arc::clone(&log);
            thread::spawn(move || {
                let frag = build_fragment(100, 0, data_flags::UNFRAGMENTED, &[0u8; 64]);
                log.on_fragment(100, 0, &frag);
            })
        };
        receiver.join().unwrap();

        // Drain from a different thread.
        let observer = {
            let log = Arc::clone(&log);
            thread::spawn(move || {
                let mut count = 0;
                loop {
                    log.poll(1024, |_| count += 1);
                    if log.subscriber_position() >= target {
                        return count;
                    }
                    std::hint::spin_loop();
                }
            })
        };
        let count = observer.join().unwrap();
        assert!(count >= 1);
    }
}
