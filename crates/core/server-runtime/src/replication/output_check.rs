//! A replica's check of its output against its primary's: the windows its
//! drain closes on its digest board, compared with the chain values the
//! primary streams after its batches (`Digest` frames). A mismatch stops
//! the node through its divergence latch, which the comparator trips.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use melin_transport_core::WireSeq;
use melin_transport_core::divergence::DivergenceLatch;
use melin_transport_core::output_digest::{
    ChainValue, Comparison, DigestBoardReader, DigestComparator, DigestKind, OUTPUT_DIGEST_WINDOW,
    first_window_end,
};
use tracing::{debug, warn};

/// One replica pipeline's output check. It lives as long as the pipeline,
/// so its local windows survive a reconnect; a rebuild (a resync) makes a
/// new one with the new pipeline's board.
pub(crate) struct OutputCheck {
    comparator: DigestComparator<DivergenceLatch>,
    /// The pipeline drain's board: this copy's closed windows.
    board: DigestBoardReader,
    /// `ReplicaControlPlane::output_checks`: one added per window found
    /// equal.
    checks: Arc<AtomicU64>,
    /// A digest kind this build does not know was warned of this session:
    /// one line per session, not one per frame.
    warned_unknown_kind: bool,
}

/// What a step of the check found, for the receiver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Checked {
    /// Nothing that stops the stream: compared equal, waiting, skipped.
    Continue,
    /// The outputs differ, and the node has stopped: the receiver
    /// publishes nothing more and tells its primary.
    Diverged,
}

/// A `Digest` frame no correct primary sends: the primary's protocol bug,
/// never a divergence.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum DigestRefusal {
    /// The frame names a window past the stream position: a primary sends
    /// a digest only after the entries it covers, so entries before it
    /// were lost. The same break as a skipped slot.
    Ahead(String),
    /// Kind 0, or a window end that is not a positive multiple of `K`.
    Malformed(String),
}

/// Check a `Digest` frame against the protocol, at `stream_position`, the
/// last entry the stream has delivered. `Ok(None)`: a kind this build does
/// not know, skipped whole (a later kind needs no protocol version).
pub(super) fn check_digest_frame(
    kind: u8,
    window_end: u64,
    stream_position: u64,
) -> Result<Option<DigestKind>, DigestRefusal> {
    if kind == 0 {
        return Err(DigestRefusal::Malformed("digest frame of kind 0".into()));
    }
    let Some(kind) = DigestKind::from_byte(kind) else {
        return Ok(None);
    };
    // Exhaustive: a new kind decides its own window rule here.
    match kind {
        DigestKind::Output => {
            if window_end == 0 || !window_end.is_multiple_of(OUTPUT_DIGEST_WINDOW) {
                return Err(DigestRefusal::Malformed(format!(
                    "output digest at {window_end}, not a positive multiple of \
                     {OUTPUT_DIGEST_WINDOW}"
                )));
            }
        }
    }
    if window_end > stream_position {
        return Err(DigestRefusal::Ahead(format!(
            "output digest at {window_end} ahead of the stream at {stream_position}"
        )));
    }
    Ok(Some(kind))
}

impl OutputCheck {
    /// The check for a pipeline whose first live position is `first_live`,
    /// reading its drain's `board` and stopping the node through `latch`.
    pub(crate) fn new(
        latch: DivergenceLatch,
        board: DigestBoardReader,
        first_live: WireSeq,
        checks: Arc<AtomicU64>,
    ) -> Self {
        Self {
            comparator: DigestComparator::new(latch, board.size(), first_window_end(first_live)),
            board,
            checks,
            warned_unknown_kind: false,
        }
    }

    /// A new session with a primary: the run ends, the values waiting
    /// from the last session are dropped, and the first usable value of
    /// this one anchors anew. A primary restarted since may have started
    /// its chain afresh.
    pub(super) fn begin_session(&mut self) {
        self.comparator.begin_session();
        self.warned_unknown_kind = false;
    }

    /// Take the windows this copy has closed since the last call.
    pub(super) fn absorb(&mut self) -> Checked {
        let found = self.comparator.absorb(&self.board);
        self.record(found)
    }

    /// The primary's `Digest` frame of `kind` (`None`: unknown, see
    /// [`check_digest_frame`]), its chain value at `window_end`.
    pub(super) fn expected(
        &mut self,
        kind: Option<DigestKind>,
        kind_byte: u8,
        window_end: u64,
        chain: [u8; 16],
    ) -> Checked {
        match kind {
            Some(DigestKind::Output) => {
                let found = self
                    .comparator
                    .push_expected(WireSeq::new(window_end), ChainValue::from_bytes(chain));
                self.record(found)
            }
            None => {
                if !self.warned_unknown_kind {
                    self.warned_unknown_kind = true;
                    // `warn!`: a primary on a newer build sends a check
                    // this one cannot make; the output check still runs.
                    warn!(
                        kind = kind_byte,
                        "the primary sends digests of a kind this build does not know; \
                         skipping them (the output check is unaffected)"
                    );
                }
                Checked::Continue
            }
        }
    }

    fn record(&mut self, found: Comparison<melin_transport_core::divergence::Halted>) -> Checked {
        if found.matched > 0 {
            self.checks
                .fetch_add(u64::from(found.matched), Ordering::Relaxed);
        }
        if let Some(after) = found.broke_after {
            debug!(
                after = after.get(),
                "output check: a window of this copy was overwritten before it was \
                 compared; the check anchors anew"
            );
        }
        if let Some(at) = found.anchored {
            debug!(
                at = at.get(),
                "output check anchored on the primary's chain"
            );
        }
        match found.mismatch {
            // The latch logged the stop and owns its notice: the
            // receiver's loop sends it, by the latch's cause, whether this
            // call or another tripped it, so the token is not needed here.
            Some((_divergence, _halted)) => Checked::Diverged,
            None => Checked::Continue,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const K: u64 = OUTPUT_DIGEST_WINDOW;

    #[test]
    fn a_digest_frame_is_checked_against_the_protocol() {
        assert_eq!(
            check_digest_frame(1, 2 * K, 2 * K + 5),
            Ok(Some(DigestKind::Output))
        );
        assert_eq!(
            check_digest_frame(1, 2 * K, 2 * K),
            Ok(Some(DigestKind::Output))
        );
        assert!(matches!(
            check_digest_frame(1, 3 * K, 3 * K - 1),
            Err(DigestRefusal::Ahead(_))
        ));
        assert!(matches!(
            check_digest_frame(0, K, K),
            Err(DigestRefusal::Malformed(_))
        ));
        assert!(matches!(
            check_digest_frame(1, 0, K),
            Err(DigestRefusal::Malformed(_))
        ));
        assert!(matches!(
            check_digest_frame(1, K + 1, 2 * K),
            Err(DigestRefusal::Malformed(_))
        ));
        // An unknown kind is skipped whole, its window not judged by
        // kind 1's rules.
        assert_eq!(check_digest_frame(9, 7, 0), Ok(None));
    }
}
