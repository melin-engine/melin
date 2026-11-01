//! How far ahead of its write position a journal segment reserves disk
//! blocks: [`Prealloc`].
//!
//! A value each writer carries, not a process-wide setting. One process
//! can hold several journals (a test runs a cluster's nodes as threads
//! of one process), and each is sized from its own node's rotation
//! threshold, so a global would let one node's sizing leak into
//! another's.

use std::num::NonZeroU64;

use crate::preparer::STAGE_MARGIN_BYTES;

/// The chunk a journal with no smaller rotation threshold reserves at a
/// time (256 MiB): the default rotation threshold, so a segment of the
/// default size fills one chunk and rotates without ever extending.
const DEFAULT_CHUNK_BYTES: NonZeroU64 = NonZeroU64::new(256 * 1024 * 1024).expect("not zero");

/// How many bytes a journal segment reserves (`fallocate`) ahead of its
/// writes: once when the segment is created or reopened, and again each
/// time a write would run past the reservation.
///
/// Reserving ahead keeps extent allocation off the flush path, and a
/// segment never needs more than it holds when it rotates. So a journal
/// that rotates at a size threshold reserves that threshold, plus the
/// margin a segment can overshoot it by, and no more: anything past
/// that is space no write reaches before rotation trims it away, and on
/// a filesystem backed by memory (tmpfs) it is memory.
///
/// A newtype over a non-zero byte count rather than a bare `u64`: a zero
/// reservation is not a smaller one but a broken one (`fallocate`
/// refuses a zero length), and the type keeps a count of bytes from
/// being confused with the MiB the operator configures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Prealloc {
    chunk_bytes: NonZeroU64,
}

impl Prealloc {
    /// The reservation of a journal with no size-driven rotation, and the
    /// largest any rotation threshold leads to.
    pub const DEFAULT: Self = Self {
        chunk_bytes: DEFAULT_CHUNK_BYTES,
    };

    /// The reservation for a journal whose live segment rotates once it
    /// reaches `max_journal_bytes` (`0`: no size-driven rotation, which
    /// takes [`DEFAULT`](Self::DEFAULT)).
    ///
    /// The threshold plus the staging margin the background preparer
    /// sizes the segments it stages with, so a node's first segment
    /// reserves what every later one is staged with; capped at
    /// [`DEFAULT`](Self::DEFAULT), past which a larger segment extends
    /// one default chunk at a time, as it always has.
    pub fn for_rotation(max_journal_bytes: u64) -> Self {
        if max_journal_bytes == 0 {
            return Self::DEFAULT;
        }
        let bytes = max_journal_bytes
            .saturating_add(STAGE_MARGIN_BYTES)
            .min(DEFAULT_CHUNK_BYTES.get());
        Self {
            chunk_bytes: NonZeroU64::new(bytes).expect("a threshold plus the margin is not zero"),
        }
    }

    /// An explicit reservation, for tooling and tests that size a
    /// journal directly rather than from a rotation threshold.
    pub const fn from_bytes(chunk_bytes: NonZeroU64) -> Self {
        Self { chunk_bytes }
    }

    /// Bytes reserved at a time.
    pub const fn chunk_bytes(self) -> u64 {
        self.chunk_bytes.get()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1024 * 1024;

    #[test]
    fn no_size_driven_rotation_reserves_the_default() {
        assert_eq!(Prealloc::for_rotation(0), Prealloc::DEFAULT);
    }

    /// A small threshold reserves the threshold and the staging margin:
    /// what the segment can hold before it rotates, and what the
    /// preparer stages every later segment with.
    #[test]
    fn a_small_threshold_reserves_the_threshold_and_the_margin() {
        assert_eq!(
            Prealloc::for_rotation(8 * MIB).chunk_bytes(),
            8 * MIB + STAGE_MARGIN_BYTES
        );
        assert_eq!(
            Prealloc::for_rotation(1).chunk_bytes(),
            1 + STAGE_MARGIN_BYTES
        );
    }

    /// The default threshold keeps the default reservation, and a larger
    /// one is capped at it: a node configured as it always was reserves
    /// what it always did.
    #[test]
    fn the_default_threshold_and_larger_ones_reserve_the_default() {
        assert_eq!(Prealloc::for_rotation(256 * MIB), Prealloc::DEFAULT);
        assert_eq!(Prealloc::for_rotation(4096 * MIB), Prealloc::DEFAULT);
        assert_eq!(Prealloc::for_rotation(u64::MAX), Prealloc::DEFAULT);
    }
}
