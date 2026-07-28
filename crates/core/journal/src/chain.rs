//! Segment-anchored BLAKE3 hash chain, shared by both writers and the
//! reader so all three compute byte-identical values.
//!
//! ## Definition
//!
//! Every segment carries a 32-byte **anchor** in its file header: random
//! salt for a fresh journal, the previous segment's tail hash after a
//! rotation. The chain value after entry `S` is a pure function of the
//! anchor and the raw on-disk bytes:
//!
//! ```text
//! chain(S) = BLAKE3(entry_bytes[first ..= S] || anchor)
//! ```
//!
//! where `entry_bytes` are the entries exactly as written on disk —
//! header, payload, *and* CRC trailer. For an empty segment the chain
//! value is the anchor itself, so rotating an empty segment propagates
//! the anchor unchanged.
//!
//! Including the CRC trailer (it is a pure function of the other bytes,
//! so it adds no adversarial strength either way) means the chain over a
//! byte range can be recomputed without decoding entries — see
//! [`SegmentChain::rebuild_from_file`], used by `open_append` to resume
//! after recovery, and by offline audit tooling.
//!
//! ## Why no in-stream finalize points
//!
//! The value at any sequence is computed on demand by cloning the
//! incremental hasher and finalizing with the anchor — O(log absorbed
//! bytes), done only at fsync boundaries, snapshots, and rotation, never
//! per entry. Because there is no finalize *schedule*, the value at a
//! given sequence depends only on `(anchor, bytes)`: any two parties
//! that share a segment's anchor and bytes agree on every chain value,
//! regardless of how their writes were batched.

use std::os::unix::fs::FileExt;
use std::path::Path;

use crate::error::JournalError;

/// Incremental chain state for one segment.
///
/// `Hasher` clone-and-finalize keeps `value()` non-destructive; the
/// hasher's internal state is fixed-size (chunk state plus a logarithmic
/// CV stack), so cloning stays cheap even after absorbing gigabytes.
pub(crate) struct SegmentChain {
    /// Anchor from the segment's file header.
    anchor: [u8; 32],
    /// Incremental hasher over the raw on-disk bytes of every entry
    /// absorbed so far (header + payload + CRC).
    hasher: blake3::Hasher,
    /// False until the first entry byte is absorbed — the chain value of
    /// an empty segment is the anchor itself.
    dirty: bool,
}

impl SegmentChain {
    /// Fresh chain at the start of a segment.
    pub(crate) fn new(anchor: [u8; 32]) -> Self {
        Self {
            anchor,
            hasher: blake3::Hasher::new(),
            dirty: false,
        }
    }

    /// Absorb the raw on-disk bytes of one entry (header + payload + CRC).
    #[inline]
    pub(crate) fn absorb(&mut self, entry_bytes: &[u8]) {
        self.hasher.update(entry_bytes);
        self.dirty = true;
    }

    /// Current chain value: the anchor for an empty segment, otherwise
    /// `BLAKE3(absorbed bytes || anchor)`. Non-destructive.
    pub(crate) fn value(&self) -> [u8; 32] {
        self.value_with_pending(&[])
    }

    /// Chain value as if `pending` had been absorbed first.
    ///
    /// Writers append entries to a batch buffer and absorb the whole
    /// batch in one `update()` at flush time (BLAKE3 only reaches its
    /// SIMD multi-block path when fed many bytes at once). Between the
    /// last flush and the next, `pending` holds the encoded-but-not-yet-
    /// absorbed bytes, and the chain value must still account for them —
    /// callers read it at fsync publication, snapshots, and rotation,
    /// which do not all coincide with a flush.
    ///
    /// Equivalent to `absorb(pending)` followed by `value()`, but
    /// non-destructive: the hasher is cloned, so the pending bytes stay
    /// absorbable exactly once later.
    pub(crate) fn value_with_pending(&self, pending: &[u8]) -> [u8; 32] {
        if !self.dirty && pending.is_empty() {
            return self.anchor;
        }
        let mut h = self.hasher.clone();
        h.update(pending);
        h.update(&self.anchor);
        *h.finalize().as_bytes()
    }

    /// Segment anchor (the header value this chain was seeded from).
    pub(crate) fn anchor(&self) -> [u8; 32] {
        self.anchor
    }

    /// Rebuild the chain by absorbing the raw byte range
    /// `[entry_offset, valid_end)` of the segment at `path`.
    ///
    /// Opens a separate plain (non-O_DIRECT) handle so the read needs no
    /// alignment; this is a recovery-only path. The byte range must end
    /// exactly at the last valid entry's boundary (the reader's
    /// `valid_file_end`), otherwise the rebuilt value diverges from what
    /// the writer would have computed.
    pub(crate) fn rebuild_from_file(
        path: &Path,
        anchor: [u8; 32],
        entry_offset: u64,
        valid_end: u64,
    ) -> Result<Self, JournalError> {
        let mut chain = Self::new(anchor);
        if valid_end <= entry_offset {
            return Ok(chain);
        }
        let file = std::fs::File::open(path)?;
        // 1 MiB scratch: large enough to amortize syscalls over thousands
        // of entries, small enough to keep recovery's working set flat.
        let mut scratch = vec![0u8; 1 << 20];
        let mut offset = entry_offset;
        while offset < valid_end {
            let want = ((valid_end - offset) as usize).min(scratch.len());
            let n = file.read_at(&mut scratch[..want], offset)?;
            if n == 0 {
                return Err(JournalError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "journal shorter than valid_end while rebuilding chain",
                )));
            }
            chain.absorb(&scratch[..n]);
            offset += n as u64;
        }
        Ok(chain)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_chain_value_is_anchor() {
        let anchor = [0x5a; 32];
        let chain = SegmentChain::new(anchor);
        assert_eq!(chain.value(), anchor);
    }

    #[test]
    fn value_is_pure_function_of_bytes_and_anchor() {
        let anchor = [7u8; 32];
        // Same bytes absorbed in different slicings produce the same value.
        let mut a = SegmentChain::new(anchor);
        a.absorb(b"hello");
        a.absorb(b"world");
        let mut b = SegmentChain::new(anchor);
        b.absorb(b"helloworld");
        assert_eq!(a.value(), b.value());

        // Different anchor or different bytes produce different values.
        let mut c = SegmentChain::new([8u8; 32]);
        c.absorb(b"helloworld");
        assert_ne!(a.value(), c.value());
        let mut d = SegmentChain::new(anchor);
        d.absorb(b"helloworlD");
        assert_ne!(a.value(), d.value());
    }

    #[test]
    fn value_is_non_destructive() {
        let mut chain = SegmentChain::new([1u8; 32]);
        chain.absorb(b"abc");
        let v1 = chain.value();
        let v2 = chain.value();
        assert_eq!(v1, v2);
        chain.absorb(b"def");
        assert_ne!(chain.value(), v1);
    }

    #[test]
    fn rebuild_from_file_matches_incremental() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("seg");
        let payload: Vec<u8> = (0u8..=255).cycle().take(3 << 20).collect();
        // Simulate a header region the chain must skip.
        let mut file_bytes = vec![0u8; 4096];
        file_bytes.extend_from_slice(&payload);
        std::fs::write(&path, &file_bytes).unwrap();

        let anchor = [9u8; 32];
        let mut live = SegmentChain::new(anchor);
        live.absorb(&payload);

        let rebuilt =
            SegmentChain::rebuild_from_file(&path, anchor, 4096, file_bytes.len() as u64).unwrap();
        assert_eq!(rebuilt.value(), live.value());
        assert_eq!(rebuilt.anchor(), anchor);
    }

    #[test]
    fn rebuild_from_empty_range_is_anchor() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("seg");
        std::fs::write(&path, vec![0u8; 4096]).unwrap();
        let anchor = [3u8; 32];
        let rebuilt = SegmentChain::rebuild_from_file(&path, anchor, 4096, 4096).unwrap();
        assert_eq!(rebuilt.value(), anchor);
    }
}
