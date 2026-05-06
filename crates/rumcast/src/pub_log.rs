//! Publication-side three-term log buffer.
//!
//! The publisher writes message fragments into one of three rotating term
//! buffers. The active term receives new claims; when it fills, a padding
//! frame is written to mark the term-end and the active partition rotates
//! to the next term. The previously-active term remains resident — its
//! bytes are the retransmit source for NAKs from subscribers — until the
//! publisher cycles back to that partition (after two more rotations).
//!
//! # Concurrency model
//!
//! - **Single producer** (typically the engine thread) calls [`try_claim`]
//!   and [`Claim::publish`]. Multiple outstanding claims from the same
//!   producer are NOT supported — finish or drop one before claiming again.
//! - **Single sender** (the network thread) reads [`publisher_position`]
//!   and the bytes up to that position. Synchronization with the producer
//!   is via a release-store on `publisher_position`; the sender pairs it
//!   with an acquire-load.
//! - **Sender's flow control** writes [`set_publisher_limit`] from the
//!   network thread; the producer reads it on every claim. Acquire/Release
//!   pair.
//!
//! [`try_claim`]: PublicationLog::try_claim
//! [`publisher_position`]: PublicationLog::publisher_position
//! [`set_publisher_limit`]: PublicationLog::set_publisher_limit

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use crate::storage::{CachePadded, LogStorage, align_up};
use crate::wire::{DataFrame, data_flags, position, term_length_bits};

/// Required per-fragment alignment within a term buffer. Matches
/// `DataFrame::HEADER_LEN` (32) so headers always sit at 32-byte
/// boundaries — also the natural cache half-line on x86/ARM.
pub const FRAGMENT_ALIGNMENT: u32 = 32;

const _: () = assert!(FRAGMENT_ALIGNMENT as usize == DataFrame::HEADER_LEN);

/// Configuration for a [`PublicationLog`].
#[derive(Debug, Clone, Copy)]
pub struct PublicationConfig {
    pub session_id: u32,
    pub stream_id: u32,
    /// First term written. Subsequent terms increment by 1.
    pub initial_term_id: u32,
    /// Bytes per term. Must be a power of two in `64 KiB..=1 GiB`.
    /// See [`term_length_bits`] for the validator.
    pub term_length: u32,
    /// Maximum bytes per fragment, including the 32-byte header. Must be
    /// a multiple of [`FRAGMENT_ALIGNMENT`] and not exceed `term_length`.
    pub mtu: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigError {
    /// `term_length` failed [`term_length_bits`] validation.
    InvalidTermLength,
    /// `mtu` < the minimum (one header + one alignment unit of payload).
    MtuTooSmall,
    /// `mtu` > `term_length`.
    MtuTooLarge,
    /// `mtu` is not a multiple of [`FRAGMENT_ALIGNMENT`].
    MtuNotAligned,
}

impl PublicationConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if term_length_bits(self.term_length).is_none() {
            return Err(ConfigError::InvalidTermLength);
        }
        if self.mtu < DataFrame::HEADER_LEN as u32 + FRAGMENT_ALIGNMENT {
            return Err(ConfigError::MtuTooSmall);
        }
        if self.mtu > self.term_length {
            return Err(ConfigError::MtuTooLarge);
        }
        if !self.mtu.is_multiple_of(FRAGMENT_ALIGNMENT) {
            return Err(ConfigError::MtuNotAligned);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimError {
    /// Aligned `header + payload` would exceed [`PublicationConfig::mtu`].
    /// The caller should fragment the message into MTU-sized chunks
    /// (sender loop responsibility, not the log's).
    PayloadTooLarge { payload_len: u32, max_payload: u32 },
    /// Writing this fragment would push past [`publisher_limit`].
    /// The caller should wait for the sender's flow control to advance
    /// the limit (typically once subscribers send status messages) and
    /// retry.
    ///
    /// [`publisher_limit`]: PublicationLog::publisher_limit
    BackPressure { wanted_position: u64, limit: u64 },
}

/// Three-term publication log. See module docs for the concurrency model.
pub struct PublicationLog {
    config: PublicationConfig,
    /// Cached `log2(term_length)` so position math is a shift, not a div.
    term_length_bits: u32,

    storage: LogStorage,

    /// Per-partition tail (highest reserved offset within the term).
    /// Reset to 0 when a partition becomes the new active term.
    /// Producer-only writes; sender does not read these directly (it
    /// reads `publisher_position` instead).
    tails: [CachePadded<AtomicU32>; 3],

    /// `term_id` currently held by each partition. Updated on rotation.
    /// Sender reads to disambiguate retransmit requests.
    term_ids: [CachePadded<AtomicU32>; 3],

    /// Index of the partition currently receiving claims (0, 1, or 2).
    active_partition: CachePadded<AtomicU32>,

    /// Total bytes published. Sender's primary read; also used to derive
    /// status-message and retransmit calculations.
    publisher_position: CachePadded<AtomicU64>,

    /// Maximum position the producer is allowed to reach. Sender writes
    /// this from its flow-control loop. Producer reads on every claim.
    publisher_limit: CachePadded<AtomicU64>,
}

impl PublicationLog {
    /// Construct a new publication log. Allocates `3 * term_length` bytes,
    /// zero-initialized.
    pub fn new(config: PublicationConfig) -> Result<Self, ConfigError> {
        config.validate()?;
        let bits = term_length_bits(config.term_length).expect("validated above");
        let storage = LogStorage::new((config.term_length as usize) * 3);

        // Positions are absolute: `position(term_id, offset)` =
        // `(term_id << bits) | offset`. The producer starts at
        // position(initial_term_id, 0).
        let start_position = position(config.initial_term_id, 0, bits);

        Ok(Self {
            config,
            term_length_bits: bits,
            storage,
            tails: [
                CachePadded::new(AtomicU32::new(0)),
                CachePadded::new(AtomicU32::new(0)),
                CachePadded::new(AtomicU32::new(0)),
            ],
            term_ids: [
                CachePadded::new(AtomicU32::new(config.initial_term_id)),
                CachePadded::new(AtomicU32::new(config.initial_term_id.wrapping_add(1))),
                CachePadded::new(AtomicU32::new(config.initial_term_id.wrapping_add(2))),
            ],
            active_partition: CachePadded::new(AtomicU32::new(0)),
            publisher_position: CachePadded::new(AtomicU64::new(start_position)),
            // Initial limit allows the producer to fill exactly the first
            // term without any sender input — useful for tests and for
            // the common "publish before any subscriber connects" case.
            // The sender's flow control should overwrite this almost
            // immediately once a subscriber sends its first status message.
            publisher_limit: CachePadded::new(AtomicU64::new(
                start_position + config.term_length as u64,
            )),
        })
    }

    pub fn config(&self) -> &PublicationConfig {
        &self.config
    }

    pub fn term_length_bits(&self) -> u32 {
        self.term_length_bits
    }

    /// Total bytes published so far. Read by the sender thread.
    #[inline]
    pub fn publisher_position(&self) -> u64 {
        self.publisher_position.get().load(Ordering::Acquire)
    }

    /// Current upper bound on what the producer is allowed to write.
    #[inline]
    pub fn publisher_limit(&self) -> u64 {
        self.publisher_limit.get().load(Ordering::Acquire)
    }

    /// Set the upper bound for the producer. Called by the sender's flow
    /// control as subscriber status messages arrive. The producer sees the
    /// new limit on its next claim attempt.
    #[inline]
    pub fn set_publisher_limit(&self, limit: u64) {
        self.publisher_limit.get().store(limit, Ordering::Release);
    }

    /// Try to claim space for one fragment with `payload_len` bytes of
    /// payload. The returned [`Claim`] holds a writable region for the
    /// payload; call [`Claim::publish`] to make the fragment visible to
    /// the sender thread.
    ///
    /// On `BackPressure`, retry after the sender advances the publisher
    /// limit. On `PayloadTooLarge`, the caller (typically the sender's
    /// fragmentation logic) should split the message into MTU-sized
    /// chunks.
    pub fn try_claim(&self, payload_len: u32) -> Result<Claim<'_>, ClaimError> {
        let header_len = DataFrame::HEADER_LEN as u32;
        let total = align_up(header_len + payload_len, FRAGMENT_ALIGNMENT);
        if total > self.config.mtu {
            // mtu >= header_len + FRAGMENT_ALIGNMENT (config validation),
            // so this subtraction can't underflow.
            return Err(ClaimError::PayloadTooLarge {
                payload_len,
                max_payload: self.config.mtu - header_len,
            });
        }

        // Bounded loop instead of recursion: at most one rotation is
        // needed (after rotating, the new term is empty so any claim
        // <= mtu fits). Loop guarantees no stack growth even if a
        // future bug breaks the "fresh term always fits" invariant.
        for _ in 0..2 {
            // Snapshot active partition state (single producer → no race).
            let active = self.active_partition.get().load(Ordering::Relaxed);
            let term_id = self.term_ids[active as usize].get().load(Ordering::Relaxed);
            let tail = self.tails[active as usize].get().load(Ordering::Relaxed);

            if tail + total > self.config.term_length {
                // Doesn't fit in the active term. Write a padding frame
                // covering [tail, term_length) and rotate. The padding
                // write advances publisher_position so the sender — and
                // ultimately the receiver — knows to skip and rotate too.
                self.write_padding_and_rotate(active, term_id, tail);
                continue;
            }

            let claim_position = position(term_id, tail, self.term_length_bits);
            let claim_end = claim_position + total as u64;
            let limit = self.publisher_limit.get().load(Ordering::Acquire);
            if claim_end > limit {
                return Err(ClaimError::BackPressure {
                    wanted_position: claim_end,
                    limit,
                });
            }

            // SAFETY: tail + total <= term_length (checked above), so the
            // pointer stays within the partition's [base, base + term_length)
            // range, and the partition lies fully within the storage buffer.
            let payload_ptr = unsafe {
                self.storage
                    .as_ptr()
                    .add(self.partition_base(active) + tail as usize + DataFrame::HEADER_LEN)
            };

            return Ok(Claim {
                log: self,
                partition: active,
                term_id,
                term_offset: tail,
                position: claim_position,
                payload_len,
                total,
                payload_ptr,
            });
        }
        // Validation guarantees mtu <= term_length, so a freshly rotated
        // term has room for any claim. Reaching this is a logic bug.
        unreachable!("rotation produced a term that still cannot fit a valid claim");
    }

    /// Bytes currently resident for the term identified by `term_id`,
    /// in the half-open range `[term_offset, term_offset + length)`.
    /// Returns `None` if the requested term is no longer in the log
    /// (already overwritten by a later term) or if the requested range
    /// extends past `publisher_position`.
    ///
    /// Used by the sender on a NAK to retransmit lost fragments.
    ///
    /// # Race condition with the producer
    ///
    /// The returned borrow points directly into the log buffer. If the
    /// producer rotates 3 times during the borrow's lifetime — covering
    /// 3 * term_length bytes of new publishes — it may overwrite the
    /// bytes underneath. The sender (Task #6) must therefore either:
    ///
    /// 1. Consume the slice promptly (single sendmsg call within
    ///    microseconds — far faster than 3 term rotations on any
    ///    realistic workload), or
    /// 2. Re-check the resident `term_id` after copying out (the sender
    ///    has access to that via the partition state).
    ///
    /// For the v1 sender, option (1) is sufficient: even at 10 GbE line
    /// rate (1.25 GB/s), three 16 MiB term rotations take ~38 ms — far
    /// longer than any single retransmit takes.
    pub fn retransmit_window(&self, term_id: u32, term_offset: u32, length: u32) -> Option<&[u8]> {
        // Guard against u32 overflow on `term_offset + length`.
        let end = term_offset.checked_add(length)?;
        if end > self.config.term_length {
            return None;
        }
        // Find the partition that currently holds this term_id.
        for partition in 0..3 {
            let resident = self.term_ids[partition].get().load(Ordering::Acquire);
            if resident == term_id {
                let start = self.partition_base(partition as u32) + term_offset as usize;
                // Don't return bytes the producer hasn't published yet:
                // clamp to publisher_position.
                let resident_pos = position(term_id, term_offset, self.term_length_bits);
                let pub_pos = self.publisher_position();
                if resident_pos + length as u64 > pub_pos {
                    // Asked for bytes past the high-water mark.
                    return None;
                }
                // SAFETY: start + length <= partition_base + term_length
                // <= storage.len(). Acquire-load on term_ids paired with
                // Release-store on rotation/publish ensures we don't read
                // stale partition data.
                let slice = unsafe {
                    core::slice::from_raw_parts(
                        self.storage.as_ptr().add(start) as *const u8,
                        length as usize,
                    )
                };
                return Some(slice);
            }
        }
        None
    }

    /// Return the bytes of the single fragment starting at
    /// `start_position`, or `None` if no fragment is yet published
    /// there (or it has been overwritten by a later term).
    ///
    /// Sender drain loop: walk forward by `align_up(fragment.len(),
    /// FRAGMENT_ALIGNMENT)` to find the next fragment.
    ///
    /// Single `Acquire` load on `publisher_position` plus the partition
    /// scan; no extra atomic on the slot's `frame_length` is needed
    /// because the publisher's `Release`-store on `publisher_position`
    /// happens-after the header memcpy.
    pub fn published_fragment(&self, start_position: u64) -> Option<&[u8]> {
        let pub_pos = self.publisher_position();
        if start_position >= pub_pos {
            return None;
        }
        let term_length = self.config.term_length as u64;
        let term_id = (start_position / term_length) as u32;
        let term_offset = (start_position % term_length) as u32;

        let partition =
            (0..3).find(|&i| self.term_ids[i].get().load(Ordering::Acquire) == term_id)?;

        let slot_start = self.partition_base(partition as u32) + term_offset as usize;
        // SAFETY: slot_start is within [0, 3*term_length); partition_base
        // is a multiple of term_length, term_offset is a valid in-term
        // offset (< term_length per the position-decoding math). slot_ptr
        // is 32-byte aligned (partition_base 64-aligned, term_offset is
        // a multiple of 32 because positions advance by aligned totals).
        let slot_ptr = unsafe { self.storage.as_ptr().add(slot_start) };

        // SAFETY: pub_pos > start_position means the publisher's
        // Release-store on publisher_position has already taken effect,
        // so its preceding non-atomic header memcpy is visible to us
        // via the Acquire pair on `publisher_position()`. Plain reads
        // are safe.
        let frame_length_bytes = unsafe { core::slice::from_raw_parts(slot_ptr, 4) };
        let frame_length = u32::from_le_bytes(frame_length_bytes.try_into().expect("4 bytes"));
        if frame_length == 0 {
            // Shouldn't happen given pub_pos > start_position, but bail
            // out rather than UB if the buffer is in an unexpected state.
            return None;
        }
        // Sanity: fragment must fit within the term.
        if (term_offset as u64) + (frame_length as u64) > term_length {
            return None;
        }
        // The fragment's last byte must also be within the published
        // window. The publisher always advances pub_pos by the aligned
        // total (>= frame_length), so this is normally true.
        if start_position + (frame_length as u64) > pub_pos {
            return None;
        }
        // SAFETY: bounds confirmed above; the slice borrows from the
        // resident partition for as long as it remains assigned to
        // this term_id (see retransmit_window's race-condition note).
        let slice = unsafe { core::slice::from_raw_parts(slot_ptr, frame_length as usize) };
        Some(slice)
    }

    /// Read the published bytes in `[start_position, end_position)` from
    /// the contiguous publish stream. Used by the sender to drain newly
    /// published bytes into outgoing UDP packets. Returns `None` if any
    /// part of the requested range crosses out of the resident window
    /// (caller asked for ancient bytes that have been overwritten).
    pub fn published_window(&self, start_position: u64, length: u32) -> Option<&[u8]> {
        let end_position = start_position + length as u64;
        let pub_pos = self.publisher_position();
        if end_position > pub_pos {
            return None;
        }
        let term_length = self.config.term_length as u64;
        let start_term_id = (start_position / term_length) as u32;
        let start_term_offset = (start_position % term_length) as u32;
        // Reject ranges that span a term boundary — caller must split.
        let end_minus_one = end_position - 1;
        let end_term_id = (end_minus_one / term_length) as u32;
        if start_term_id != end_term_id {
            return None;
        }
        self.retransmit_window(start_term_id, start_term_offset, length)
    }

    fn partition_base(&self, partition: u32) -> usize {
        (partition as usize) * (self.config.term_length as usize)
    }

    fn write_padding_and_rotate(&self, partition: u32, term_id: u32, tail: u32) {
        let pad_len = self.config.term_length - tail;

        // pad_len == 0 means the term filled exactly on the previous
        // claim — nothing to pad, just rotate. pad_len > 0 implies it's
        // at least one header (FRAGMENT_ALIGNMENT) because mtu and
        // term_length are both aligned, so any residual is a multiple
        // of FRAGMENT_ALIGNMENT >= HEADER_LEN.
        if pad_len > 0 {
            debug_assert!(
                pad_len >= DataFrame::HEADER_LEN as u32,
                "term_length and mtu both align to FRAGMENT_ALIGNMENT, \
                 so any non-zero residual is at least one header"
            );

            let payload_len = pad_len - DataFrame::HEADER_LEN as u32;
            let header = DataFrame::new(
                self.config.session_id,
                self.config.stream_id,
                term_id,
                tail,
                data_flags::PADDING,
                payload_len,
            );
            debug_assert_eq!(header.common.frame_length, pad_len);

            // SAFETY: tail + DataFrame::HEADER_LEN <= term_length
            // (residual >= one header).
            let dst = unsafe {
                self.storage
                    .as_ptr()
                    .add(self.partition_base(partition) + tail as usize)
            };
            // SAFETY: header is repr(C) Pod, source and dst do not
            // overlap, dst points to writable buffer space we own (the
            // single-producer protocol prevents any concurrent writer).
            unsafe {
                core::ptr::copy_nonoverlapping(
                    bytemuck::bytes_of(&header).as_ptr(),
                    dst,
                    DataFrame::HEADER_LEN,
                );
            }

            // Mark the term full. Release pairs with the sender's
            // Acquire on tails/publisher_position so the padding header
            // is visible before the position advances.
            self.tails[partition as usize]
                .get()
                .store(self.config.term_length, Ordering::Release);

            // Bump publisher_position so the sender (and ultimately
            // subscribers) emit the padding frame on the wire.
            self.publisher_position
                .get()
                .fetch_add(pad_len as u64, Ordering::Release);
        }

        self.rotate(partition, term_id);
    }

    fn rotate(&self, prev_active: u32, prev_term_id: u32) {
        let next_active = (prev_active + 1) % 3;
        let next_term_id = prev_term_id.wrapping_add(1);

        // The next partition currently holds term `prev_term_id - 2`
        // (its previous occupant). Its bytes remain there until we
        // overwrite them. We don't zero — `retransmit_window` filters
        // by term_id match, so callers asking for the old term_id will
        // miss after we update term_ids[next_active] below.
        //
        // Order of operations matters:
        //   1. tails[next] = 0 (Release): claims into the new term must
        //      observe a fresh tail before they happen.
        //   2. term_ids[next] = next_term_id (Release): retransmit
        //      callers see the new term_id only after the tail reset.
        //   3. active_partition = next (Release): publishes the rotation.
        self.tails[next_active as usize]
            .get()
            .store(0, Ordering::Release);
        self.term_ids[next_active as usize]
            .get()
            .store(next_term_id, Ordering::Release);
        self.active_partition
            .get()
            .store(next_active, Ordering::Release);
    }
}

/// Reservation handle returned by [`PublicationLog::try_claim`]. Holds a
/// writable region for the fragment payload. Call [`Claim::publish`] to
/// make the fragment visible to the sender thread; dropping without
/// publishing leaves the slot un-reserved (next claim returns the same
/// position).
pub struct Claim<'a> {
    log: &'a PublicationLog,
    partition: u32,
    term_id: u32,
    term_offset: u32,
    position: u64,
    payload_len: u32,
    total: u32,
    payload_ptr: *mut u8,
}

// Claim is intentionally NOT `Send`. A claim represents a producer's
// reservation on the log buffer; only the producer thread that made the
// claim may fill and publish it. Sending the Claim across threads would
// break the single-producer protocol.

impl Claim<'_> {
    pub fn position(&self) -> u64 {
        self.position
    }
    pub fn term_id(&self) -> u32 {
        self.term_id
    }
    pub fn term_offset(&self) -> u32 {
        self.term_offset
    }
    pub fn payload_len(&self) -> u32 {
        self.payload_len
    }

    /// Mutable slice into the reserved payload region. Caller writes
    /// fragment bytes here before calling [`publish`].
    ///
    /// [`publish`]: Claim::publish
    pub fn payload_mut(&mut self) -> &mut [u8] {
        // SAFETY: payload_ptr was computed at try_claim time inside the
        // active partition's bounds; no other thread accesses this region
        // until publish() advances publisher_position with a Release
        // store.
        unsafe { core::slice::from_raw_parts_mut(self.payload_ptr, self.payload_len as usize) }
    }

    /// Convenience: copy `src` into the payload region. Panics if `src`
    /// is not exactly `payload_len` bytes — partial fragments are a bug
    /// in the caller, not a runtime condition to silently tolerate.
    pub fn write_payload(&mut self, src: &[u8]) {
        assert_eq!(
            src.len(),
            self.payload_len as usize,
            "payload size mismatch"
        );
        self.payload_mut().copy_from_slice(src);
    }

    /// Make the fragment visible. Writes the [`DataFrame`] header
    /// (with the configured flags applied), advances the partition's
    /// tail, and bumps `publisher_position` with a release store so the
    /// sender thread observes the new fragment.
    ///
    /// `flags` should be one of [`data_flags::UNFRAGMENTED`],
    /// `BEGIN_FRAGMENT`, `END_FRAGMENT`, or `0` for an interior fragment
    /// of a multi-fragment message.
    ///
    /// [`DataFrame`]: crate::wire::DataFrame
    /// [`data_flags::UNFRAGMENTED`]: crate::wire::data_flags::UNFRAGMENTED
    pub fn publish(self, flags: u8) {
        let header = DataFrame::new(
            self.log.config.session_id,
            self.log.config.stream_id,
            self.term_id,
            self.term_offset,
            flags,
            self.payload_len,
        );
        // SAFETY: the header region [base + term_offset, base + term_offset
        // + HEADER_LEN) is inside the active partition; single-producer
        // protocol guarantees no concurrent writer.
        let dst = unsafe {
            self.log
                .storage
                .as_ptr()
                .add(self.log.partition_base(self.partition) + self.term_offset as usize)
        };
        unsafe {
            core::ptr::copy_nonoverlapping(
                bytemuck::bytes_of(&header).as_ptr(),
                dst,
                DataFrame::HEADER_LEN,
            );
        }
        // Advance partition tail (Relaxed: only the producer reads).
        self.log.tails[self.partition as usize]
            .get()
            .store(self.term_offset + self.total, Ordering::Relaxed);
        // Release: the sender thread's Acquire on publisher_position
        // synchronizes-with this store, so all the byte writes above
        // (header + payload) become visible to the sender.
        self.log
            .publisher_position
            .get()
            .store(self.position + self.total as u64, Ordering::Release);
    }
}

// No explicit Drop impl: tail wasn't advanced and publisher_position
// wasn't bumped during try_claim, so dropping an unpublished Claim is a
// no-op. The next try_claim returns the same position and overwrites
// whatever the caller wrote (if anything).

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::FrameView;

    fn cfg() -> PublicationConfig {
        PublicationConfig {
            session_id: 1,
            stream_id: 2,
            initial_term_id: 100,
            term_length: 64 * 1024,
            mtu: 1024,
        }
    }

    #[test]
    fn config_validation() {
        assert_eq!(cfg().validate(), Ok(()));
        let mut bad = cfg();
        bad.term_length = 1234;
        assert_eq!(bad.validate(), Err(ConfigError::InvalidTermLength));
        let mut bad = cfg();
        bad.mtu = 31;
        assert_eq!(bad.validate(), Err(ConfigError::MtuTooSmall));
        let mut bad = cfg();
        bad.mtu = bad.term_length + 32;
        assert_eq!(bad.validate(), Err(ConfigError::MtuTooLarge));
        let mut bad = cfg();
        bad.mtu = 1000; // not multiple of 32
        assert_eq!(bad.validate(), Err(ConfigError::MtuNotAligned));
    }

    /// Helper: absolute position(term_id, term_offset) for the test cfg.
    fn pos(term_id: u32, term_offset: u32) -> u64 {
        position(term_id, term_offset, 16) // bits=16 for cfg's 64 KiB term
    }

    #[test]
    fn fresh_log_is_at_initial_position() {
        let log = PublicationLog::new(cfg()).unwrap();
        // Positions are absolute: initial = position(initial_term_id, 0).
        assert_eq!(log.publisher_position(), pos(100, 0));
        assert_eq!(
            log.publisher_limit(),
            pos(100, 0) + cfg().term_length as u64
        );
        assert_eq!(log.config().session_id, 1);
        assert_eq!(log.term_length_bits(), 16);
    }

    #[test]
    fn payload_too_large_rejected_before_claim() {
        let log = PublicationLog::new(cfg()).unwrap();
        let max = cfg().mtu - DataFrame::HEADER_LEN as u32;
        let result = log.try_claim(max + 1);
        match result {
            Err(ClaimError::PayloadTooLarge { max_payload, .. }) => {
                assert_eq!(max_payload, max);
            }
            Err(other) => panic!("expected PayloadTooLarge, got Err({other:?})"),
            Ok(_) => panic!("expected PayloadTooLarge, got Ok(_)"),
        }
    }

    #[test]
    fn single_publish_advances_position_and_writes_header() {
        let log = PublicationLog::new(cfg()).unwrap();
        let mut claim = log.try_claim(64).unwrap();
        assert_eq!(claim.position(), pos(100, 0));
        assert_eq!(claim.term_id(), 100);
        assert_eq!(claim.term_offset(), 0);
        claim.payload_mut().fill(0xAB);
        claim.publish(data_flags::UNFRAGMENTED);

        // 64 bytes payload + 32 bytes header = 96 bytes (already aligned).
        assert_eq!(log.publisher_position(), pos(100, 0) + 96);

        // Header must be readable via parse_frame from the published window.
        let bytes = log.published_window(pos(100, 0), 96).unwrap();
        let view = crate::wire::parse_frame(bytes).unwrap();
        match view {
            FrameView::Data { header, payload } => {
                assert_eq!(header.common.flags, data_flags::UNFRAGMENTED);
                assert_eq!(header.term_id, 100);
                assert_eq!(header.term_offset, 0);
                assert_eq!(payload.len(), 64);
                assert!(payload.iter().all(|&b| b == 0xAB));
            }
            other => panic!("expected Data, got {other:?}"),
        }
    }

    #[test]
    fn unaligned_payload_rounded_up_in_total_size() {
        let log = PublicationLog::new(cfg()).unwrap();
        // 50 bytes payload → 32 header + 50 = 82 → aligned up to 96.
        let mut claim = log.try_claim(50).unwrap();
        claim.payload_mut().fill(0x5A);
        claim.publish(data_flags::UNFRAGMENTED);
        assert_eq!(log.publisher_position(), pos(100, 0) + 96);
    }

    #[test]
    fn claim_drop_without_publish_does_not_advance_state() {
        let log = PublicationLog::new(cfg()).unwrap();
        {
            let mut c = log.try_claim(64).unwrap();
            c.payload_mut().fill(0xFF);
            // drop without publish
        }
        assert_eq!(log.publisher_position(), pos(100, 0));
        // Next claim returns the same position.
        let c2 = log.try_claim(64).unwrap();
        assert_eq!(c2.position(), pos(100, 0));
    }

    #[test]
    fn sequential_publishes_advance_monotonically() {
        let log = PublicationLog::new(cfg()).unwrap();
        let start = pos(100, 0);
        let mut expected = start;
        for i in 0..10 {
            let mut c = log.try_claim(96).unwrap();
            c.payload_mut().fill(i as u8);
            c.publish(data_flags::UNFRAGMENTED);
            expected += 128; // 32 header + 96 payload, already aligned
            assert_eq!(log.publisher_position(), expected);
        }
    }

    #[test]
    fn back_pressure_on_limit_exhaustion() {
        let log = PublicationLog::new(cfg()).unwrap();
        let start = pos(100, 0);
        // Set the limit one slot short of what the next claim wants.
        log.set_publisher_limit(start + 64);
        // 32 + 64 = 96 needed, only 64 allowed past start: rejected.
        match log.try_claim(64) {
            Err(ClaimError::BackPressure {
                wanted_position,
                limit,
            }) => {
                assert_eq!(wanted_position, start + 96);
                assert_eq!(limit, start + 64);
            }
            Err(other) => panic!("expected BackPressure, got Err({other:?})"),
            Ok(_) => panic!("expected BackPressure, got Ok(_)"),
        }
        // Advance limit; claim now succeeds.
        log.set_publisher_limit(start + 1024);
        let claim = log.try_claim(64).unwrap();
        assert_eq!(claim.position(), start);
    }

    #[test]
    fn term_rotation_writes_padding_and_advances_metadata() {
        let cfg = PublicationConfig {
            session_id: 1,
            stream_id: 2,
            initial_term_id: 100,
            term_length: 64 * 1024,
            mtu: 1024,
        };
        let log = PublicationLog::new(cfg).unwrap();
        // Lift the limit so flow control doesn't interfere.
        log.set_publisher_limit(u64::MAX);

        let frags_per_term = (cfg.term_length / cfg.mtu) as usize;
        for _ in 0..frags_per_term {
            let payload = cfg.mtu - DataFrame::HEADER_LEN as u32;
            let mut c = log.try_claim(payload).unwrap();
            c.payload_mut().fill(0x11);
            c.publish(data_flags::UNFRAGMENTED);
        }
        // We've just filled the first term exactly: position(101, 0).
        assert_eq!(log.publisher_position(), pos(101, 0));
        // active_partition still 0; rotation only happens on the next claim.
        assert_eq!(log.active_partition.get().load(Ordering::Acquire), 0);

        // One more claim triggers padding (zero residual) then rotation.
        let mut c = log.try_claim(64).unwrap();
        c.payload_mut().fill(0x22);
        c.publish(data_flags::UNFRAGMENTED);
        // active_partition is now 1, term_id 101.
        assert_eq!(log.active_partition.get().load(Ordering::Acquire), 1);
        assert_eq!(log.term_ids[1].get().load(Ordering::Acquire), 101);
        // Position advanced by 96 (one 64-byte payload + 32 header) into term 101.
        assert_eq!(log.publisher_position(), pos(101, 96));
    }

    #[test]
    fn term_rotation_with_partial_term_writes_padding_frame() {
        let cfg = PublicationConfig {
            session_id: 1,
            stream_id: 2,
            initial_term_id: 100,
            term_length: 64 * 1024,
            mtu: 1024,
        };
        let log = PublicationLog::new(cfg).unwrap();
        log.set_publisher_limit(u64::MAX);

        // Fill 32 KiB (half the term).
        let half_count = (cfg.term_length / 2 / cfg.mtu) as usize;
        for _ in 0..half_count {
            let mut c = log.try_claim(cfg.mtu - 32).unwrap();
            c.payload_mut().fill(0x33);
            c.publish(data_flags::UNFRAGMENTED);
        }
        assert_eq!(log.publisher_position(), pos(100, cfg.term_length / 2));

        // Fill the rest of term 100 with 32 more 1024-byte fragments,
        // landing exactly at the term boundary.
        for _ in 0..32 {
            let mut c = log.try_claim(cfg.mtu - 32).unwrap();
            c.payload_mut().fill(0x44);
            c.publish(data_flags::UNFRAGMENTED);
        }
        assert_eq!(log.publisher_position(), pos(101, 0));
        // One more triggers padding (zero residual) + rotation into term 101.
        let mut c = log.try_claim(64).unwrap();
        c.payload_mut().fill(0x55);
        c.publish(data_flags::UNFRAGMENTED);
        assert_eq!(log.active_partition.get().load(Ordering::Acquire), 1);
        assert_eq!(log.publisher_position(), pos(101, 96));
    }

    #[test]
    fn padding_frame_emitted_when_partial_residual() {
        // mtu = 3072 doesn't divide 64 KiB evenly, leaving a partial
        // residual at the term-end. After 21 fragments we sit at
        // 21 * 3072 = 64 512 bytes; residual = 1024 bytes (< mtu) so the
        // 22nd claim must emit a padding frame covering the residual and
        // rotate into the next term.
        let cfg = PublicationConfig {
            session_id: 1,
            stream_id: 2,
            initial_term_id: 200,
            term_length: 64 * 1024,
            mtu: 3072,
        };
        let log = PublicationLog::new(cfg).unwrap();
        log.set_publisher_limit(u64::MAX);

        for _ in 0..21 {
            let c = log.try_claim(cfg.mtu - 32).unwrap();
            c.publish(data_flags::UNFRAGMENTED);
        }
        let bits = term_length_bits(cfg.term_length).unwrap();
        let start = position(200, 0, bits);
        assert_eq!(log.publisher_position(), start + 21 * 3072);

        // Residual 1024 < 3072 → pad + rotate + new claim.
        let c = log.try_claim(cfg.mtu - 32).unwrap();
        c.publish(data_flags::UNFRAGMENTED);

        // Final position: start + 21*3072 + 1024 (padding) + 3072 (new).
        // Padding fills the term boundary, so we land at (term 201, offset 3072).
        assert_eq!(log.publisher_position(), position(201, 3072, bits));
        assert_eq!(log.active_partition.get().load(Ordering::Acquire), 1);

        // Inspect the padding frame on the wire.
        let pad_offset = 21 * 3072;
        let pad_bytes = log.retransmit_window(200, pad_offset, 1024).unwrap();
        let view = crate::wire::parse_frame(pad_bytes).unwrap();
        match view {
            FrameView::Data { header, payload } => {
                assert_eq!(header.common.flags, data_flags::PADDING);
                assert_eq!(header.term_id, 200);
                assert_eq!(header.term_offset, pad_offset);
                assert_eq!(payload.len(), 1024 - 32);
            }
            other => panic!("expected padding Data frame, got {other:?}"),
        }
    }

    #[test]
    fn retransmit_window_returns_resident_term_bytes() {
        let log = PublicationLog::new(cfg()).unwrap();
        let mut c = log.try_claim(64).unwrap();
        c.payload_mut().fill(0x77);
        c.publish(data_flags::UNFRAGMENTED);

        // Fragment occupies term_id=100, term_offset=0..96.
        let bytes = log.retransmit_window(100, 0, 96).unwrap();
        assert_eq!(bytes.len(), 96);
        // Bytes [0..32] = header; [32..96] = payload.
        assert!(bytes[32..].iter().all(|&b| b == 0x77));
    }

    #[test]
    fn retransmit_window_returns_none_for_unwritten_bytes() {
        let log = PublicationLog::new(cfg()).unwrap();
        // Nothing published yet — asking for any byte should fail.
        assert_eq!(log.retransmit_window(100, 0, 32), None);
    }

    #[test]
    fn retransmit_window_rejects_overflowing_range() {
        let log = PublicationLog::new(cfg()).unwrap();
        // term_offset + length would overflow u32.
        assert_eq!(log.retransmit_window(100, u32::MAX - 10, 100), None);
        // term_offset + length fits in u32 but exceeds term_length.
        assert_eq!(
            log.retransmit_window(100, cfg().term_length - 10, 100),
            None
        );
    }

    #[test]
    fn retransmit_window_returns_none_for_overwritten_term() {
        let cfg = PublicationConfig {
            session_id: 1,
            stream_id: 2,
            initial_term_id: 100,
            term_length: 64 * 1024,
            mtu: 1024,
        };
        let log = PublicationLog::new(cfg).unwrap();
        log.set_publisher_limit(u64::MAX);

        // Publish one fragment in term 100.
        let c = log.try_claim(cfg.mtu - 32).unwrap();
        c.publish(data_flags::UNFRAGMENTED);

        // Force three rotations: fill terms 100, 101, 102 to capacity,
        // then start writing term 103. After that, partition 0 (which
        // held term 100) now holds term 103 — term 100 is gone.
        let frags_per_term = (cfg.term_length / cfg.mtu) as usize;
        // We already wrote 1 fragment in term 100; finish it.
        for _ in 0..(frags_per_term - 1) {
            let c = log.try_claim(cfg.mtu - 32).unwrap();
            c.publish(data_flags::UNFRAGMENTED);
        }
        // Fill term 101.
        for _ in 0..frags_per_term {
            let c = log.try_claim(cfg.mtu - 32).unwrap();
            c.publish(data_flags::UNFRAGMENTED);
        }
        // Fill term 102.
        for _ in 0..frags_per_term {
            let c = log.try_claim(cfg.mtu - 32).unwrap();
            c.publish(data_flags::UNFRAGMENTED);
        }
        // One fragment in term 103 — this kicks partition 0 over to 103.
        let c = log.try_claim(cfg.mtu - 32).unwrap();
        c.publish(data_flags::UNFRAGMENTED);

        // Term 100 is no longer resident.
        assert_eq!(log.retransmit_window(100, 0, 32), None);
        // Term 103 IS resident (in partition 0).
        assert!(log.retransmit_window(103, 0, 32).is_some());
    }

    #[test]
    fn published_fragment_walks_log_one_fragment_at_a_time() {
        let log = PublicationLog::new(cfg()).unwrap();
        log.set_publisher_limit(u64::MAX);
        // Publish three fragments of different sizes.
        let sizes = [32u32, 64, 96];
        for &size in &sizes {
            let mut c = log.try_claim(size).unwrap();
            c.payload_mut().fill(size as u8);
            c.publish(data_flags::UNFRAGMENTED);
        }
        // Walk via published_fragment.
        let mut pos = pos(100, 0);
        let mut seen: Vec<u32> = Vec::new();
        while let Some(frag) = log.published_fragment(pos) {
            let frame_length = u32::from_le_bytes(frag[0..4].try_into().unwrap());
            seen.push(frame_length - DataFrame::HEADER_LEN as u32);
            pos += align_up(frame_length, FRAGMENT_ALIGNMENT) as u64;
        }
        assert_eq!(seen, vec![32, 64, 96]);
    }

    #[test]
    fn published_fragment_returns_none_past_published_position() {
        let log = PublicationLog::new(cfg()).unwrap();
        // Nothing published — any position returns None.
        assert!(log.published_fragment(pos(100, 0)).is_none());
        // Publish one fragment; queries past it return None.
        let c = log.try_claim(64).unwrap();
        c.publish(data_flags::UNFRAGMENTED);
        assert!(log.published_fragment(pos(100, 96)).is_none());
        // But the query at 0 succeeds.
        assert!(log.published_fragment(pos(100, 0)).is_some());
    }

    #[test]
    fn published_window_rejects_cross_term_range() {
        let log = PublicationLog::new(cfg()).unwrap();
        log.set_publisher_limit(u64::MAX);
        // Fill term 100 to capacity.
        let frags = cfg().term_length / cfg().mtu;
        for _ in 0..frags {
            let c = log.try_claim(cfg().mtu - 32).unwrap();
            c.publish(data_flags::UNFRAGMENTED);
        }
        // Position now at pos(101, 0). Query that spans the term boundary.
        let boundary = pos(101, 0);
        assert_eq!(log.published_window(boundary - 32, 64), None);
    }

    #[test]
    fn published_window_returns_none_for_unpublished_bytes() {
        let log = PublicationLog::new(cfg()).unwrap();
        let start = pos(100, 0);
        // Nothing published — even querying at start returns None.
        assert_eq!(log.published_window(start, 32), None);
        // Publish 96 bytes.
        let c = log.try_claim(64).unwrap();
        c.publish(data_flags::UNFRAGMENTED);
        // Asking past publisher_position returns None.
        assert_eq!(log.published_window(start, 128), None);
        // Asking within returns Some.
        assert!(log.published_window(start, 96).is_some());
    }

    #[test]
    fn claim_with_zero_payload_writes_header_only() {
        let log = PublicationLog::new(cfg()).unwrap();
        let claim = log.try_claim(0).unwrap();
        assert_eq!(claim.payload_len(), 0);
        claim.publish(data_flags::UNFRAGMENTED);
        // 32-byte header, no payload, already aligned.
        assert_eq!(log.publisher_position(), pos(100, 0) + 32);

        let bytes = log.published_window(pos(100, 0), 32).unwrap();
        match crate::wire::parse_frame(bytes).unwrap() {
            FrameView::Data { header, payload } => {
                assert_eq!(header.common.frame_length, 32);
                assert!(payload.is_empty());
            }
            other => panic!("expected Data, got {other:?}"),
        }
    }

    #[test]
    fn claim_with_max_payload_fills_an_mtu() {
        let log = PublicationLog::new(cfg()).unwrap();
        let max = cfg().mtu - DataFrame::HEADER_LEN as u32;
        let mut claim = log.try_claim(max).unwrap();
        assert_eq!(claim.payload_len(), max);
        claim.payload_mut().fill(0xEE);
        claim.publish(data_flags::UNFRAGMENTED);
        // Total = mtu (already aligned).
        assert_eq!(log.publisher_position(), pos(100, 0) + cfg().mtu as u64);
    }

    #[test]
    fn rotation_cycles_through_all_three_partitions() {
        let cfg = PublicationConfig {
            session_id: 1,
            stream_id: 2,
            initial_term_id: 100,
            term_length: 64 * 1024,
            mtu: 1024,
        };
        let log = PublicationLog::new(cfg).unwrap();
        log.set_publisher_limit(u64::MAX);

        let frags_per_term = (cfg.term_length / cfg.mtu) as usize;
        // Initial: active=0 holds term 100, partitions[1]=101, [2]=102.
        assert_eq!(log.active_partition.get().load(Ordering::Acquire), 0);
        assert_eq!(log.term_ids[0].get().load(Ordering::Acquire), 100);
        assert_eq!(log.term_ids[1].get().load(Ordering::Acquire), 101);
        assert_eq!(log.term_ids[2].get().load(Ordering::Acquire), 102);

        // Fill term 100, then one fragment in term 101 to force rotation 0→1.
        for _ in 0..frags_per_term {
            let c = log.try_claim(cfg.mtu - 32).unwrap();
            c.publish(data_flags::UNFRAGMENTED);
        }
        let c = log.try_claim(cfg.mtu - 32).unwrap();
        c.publish(data_flags::UNFRAGMENTED);
        assert_eq!(log.active_partition.get().load(Ordering::Acquire), 1);

        // Fill the rest of term 101, then one fragment in term 102 → rotation 1→2.
        for _ in 0..(frags_per_term - 1) {
            let c = log.try_claim(cfg.mtu - 32).unwrap();
            c.publish(data_flags::UNFRAGMENTED);
        }
        let c = log.try_claim(cfg.mtu - 32).unwrap();
        c.publish(data_flags::UNFRAGMENTED);
        assert_eq!(log.active_partition.get().load(Ordering::Acquire), 2);

        // Fill the rest of term 102, then one fragment in term 103 → rotation 2→0
        // (wraparound). Partition 0 must now hold term 103, replacing term 100.
        for _ in 0..(frags_per_term - 1) {
            let c = log.try_claim(cfg.mtu - 32).unwrap();
            c.publish(data_flags::UNFRAGMENTED);
        }
        let c = log.try_claim(cfg.mtu - 32).unwrap();
        c.publish(data_flags::UNFRAGMENTED);
        assert_eq!(log.active_partition.get().load(Ordering::Acquire), 0);
        assert_eq!(log.term_ids[0].get().load(Ordering::Acquire), 103);
        // Other partitions still hold their post-rotation term_ids.
        assert_eq!(log.term_ids[1].get().load(Ordering::Acquire), 101);
        assert_eq!(log.term_ids[2].get().load(Ordering::Acquire), 102);
    }

    #[test]
    fn fragmentation_flags_round_trip_through_three_fragments() {
        let log = PublicationLog::new(cfg()).unwrap();
        // Three fragments of a single logical message.
        let bits = log.term_length_bits();
        let start = position(100, 0, bits);

        let mut c = log.try_claim(64).unwrap();
        c.payload_mut().fill(0x01);
        c.publish(data_flags::BEGIN_FRAGMENT);

        let mut c = log.try_claim(64).unwrap();
        c.payload_mut().fill(0x02);
        // Interior fragment: neither BEGIN nor END.
        c.publish(0);

        let mut c = log.try_claim(64).unwrap();
        c.payload_mut().fill(0x03);
        c.publish(data_flags::END_FRAGMENT);

        // Each fragment is 96 bytes total.
        let expected_flags = [data_flags::BEGIN_FRAGMENT, 0, data_flags::END_FRAGMENT];
        let expected_fills = [0x01u8, 0x02, 0x03];
        for (i, (flags, fill)) in expected_flags.iter().zip(expected_fills.iter()).enumerate() {
            let frag_pos = start + (i as u64) * 96;
            let bytes = log.published_window(frag_pos, 96).unwrap();
            match crate::wire::parse_frame(bytes).unwrap() {
                FrameView::Data { header, payload } => {
                    assert_eq!(header.common.flags, *flags, "fragment {i} flags");
                    assert!(payload.iter().all(|&b| b == *fill), "fragment {i} payload");
                }
                other => panic!("expected Data, got {other:?}"),
            }
        }
    }

    #[test]
    fn publisher_position_visible_to_other_thread() {
        use std::sync::Arc;
        use std::thread;

        let log = Arc::new(PublicationLog::new(cfg()).unwrap());
        log.set_publisher_limit(u64::MAX);
        let target = pos(100, 0) + 96;

        let reader = {
            let log = Arc::clone(&log);
            thread::spawn(move || {
                // Spin until publisher reaches at least the first fragment.
                loop {
                    if log.publisher_position() >= target {
                        break log.publisher_position();
                    }
                    std::hint::spin_loop();
                }
            })
        };

        let mut c = log.try_claim(64).unwrap();
        c.payload_mut().fill(0xCC);
        c.publish(data_flags::UNFRAGMENTED);

        let observed = reader.join().unwrap();
        assert!(observed >= target);
    }
}
