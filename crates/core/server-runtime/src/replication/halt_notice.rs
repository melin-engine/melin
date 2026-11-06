//! The `Halt` a stopped primary owes each replica streaming from it.
//!
//! Once a node has stopped for divergence, every live replication session
//! it holds tells its replica so with a `Halt` frame before closing: the
//! session whose replica told it (that replica waits for the answer), and
//! every other, so its replica stops too. The frame goes at a frame
//! boundary, once nothing of an earlier frame is still being sent, and
//! after the node's marker is durable, so that a crash cannot leave a node
//! that restarts into the cluster it has just stopped. A hung disk must not
//! hold the notice, though: past [`HALT_NOTICE_BOUND`] it goes anyway, and
//! past twice that a session that could not send it gives up.
//!
//! One timing rule for both transports' senders, which call
//! [`HaltNotice::step`] from their own loops.

use std::time::{Duration, Instant};

use melin_transport_core::divergence::{DivergenceLatch, HALT_NOTICE_BOUND};

/// What a sender does next about its session's `Halt`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HaltStep {
    /// Keep the session as it is: an earlier frame is still going out, or
    /// the marker is not yet durable and the bound has not passed.
    Wait,
    /// Send the `Halt` frame now, once.
    Send,
    /// Close the session: the `Halt` went out, or could not in time.
    Close,
}

/// One session's `Halt`, from the moment the session saw its node stopped.
#[derive(Debug)]
pub(crate) struct HaltNotice {
    started: Instant,
    /// How long the frame waits for the marker; the session gives up at
    /// twice it. [`HALT_NOTICE_BOUND`] in production, shorter in tests.
    bound: Duration,
    sent: bool,
}

impl HaltNotice {
    /// A notice whose session has just seen its node stopped.
    pub(crate) fn new() -> Self {
        Self::with_bound(HALT_NOTICE_BOUND)
    }

    pub(crate) fn with_bound(bound: Duration) -> Self {
        Self {
            started: Instant::now(),
            bound,
            sent: false,
        }
    }

    /// The next step, given whether the session has nothing of an earlier
    /// frame in flight (`idle`).
    pub(crate) fn step(&mut self, idle: bool, latch: &DivergenceLatch) -> HaltStep {
        let waited = self.started.elapsed();
        if waited >= 2 * self.bound {
            return HaltStep::Close;
        }
        if !idle {
            return HaltStep::Wait;
        }
        if self.sent {
            return HaltStep::Close;
        }
        if latch.marker_settled().is_none() {
            if waited < self.bound {
                return HaltStep::Wait;
            }
            // `error!`: the notice leaves before the node's own restart is
            // known to be refused, which only a hung or failing disk does.
            tracing::error!(
                marker = %latch.marker().display(),
                "telling a replica the cluster stopped before the divergence marker was \
                 confirmed written"
            );
        }
        self.sent = true;
        HaltStep::Send
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    use melin_transport_core::divergence::{
        FsSync, HaltCause, HaltedBy, MarkerOutcome, MarkerSync, NoticePath,
    };
    use melin_transport_core::fence::FenceState;

    use super::*;

    fn cause() -> HaltCause {
        HaltCause::ClusterHalted {
            by: HaltedBy::Replica { slot: 0 },
            via: NoticePath::Ack,
        }
    }

    fn latch(dir: &std::path::Path, sync: Arc<dyn MarkerSync>) -> DivergenceLatch {
        DivergenceLatch::new(
            Arc::new(FenceState::new(0)),
            Arc::new(AtomicBool::new(false)),
            dir.join("n.journal.diverged"),
            sync,
        )
    }

    /// With the marker durable, the frame goes at the first idle step,
    /// once, and the next idle step closes; a frame still in flight holds
    /// both.
    #[test]
    fn the_halt_goes_once_at_a_frame_boundary_after_the_marker() {
        let dir = tempfile::tempdir().expect("tempdir");
        let latch = latch(dir.path(), Arc::new(FsSync));
        let _halted = latch.trip_with(cause());
        assert!(matches!(
            latch.wait_marker(HALT_NOTICE_BOUND),
            MarkerOutcome::Written
        ));
        let mut notice = HaltNotice::new();
        assert_eq!(notice.step(false, &latch), HaltStep::Wait, "mid-frame");
        assert_eq!(notice.step(true, &latch), HaltStep::Send);
        assert_eq!(
            notice.step(false, &latch),
            HaltStep::Wait,
            "the halt in flight"
        );
        assert_eq!(notice.step(true, &latch), HaltStep::Close);
    }

    /// A disk that never answers.
    struct Hung;
    impl MarkerSync for Hung {
        fn sync_file(&self, _: &std::fs::File) -> std::io::Result<()> {
            std::thread::park();
            Ok(())
        }
        fn sync_dir(&self, _: &std::path::Path) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// A hung marker holds the frame for the bound, not longer; a session
    /// that cannot send it gives up at twice the bound.
    #[test]
    fn a_hung_marker_delays_the_halt_by_the_bound_only() {
        let dir = tempfile::tempdir().expect("tempdir");
        let latch = latch(dir.path(), Arc::new(Hung));
        let _halted = latch.trip_with(cause());
        let bound = Duration::from_millis(30);

        let mut notice = HaltNotice::with_bound(bound);
        assert_eq!(notice.step(true, &latch), HaltStep::Wait);
        std::thread::sleep(bound);
        assert_eq!(notice.step(true, &latch), HaltStep::Send);

        let mut stuck = HaltNotice::with_bound(bound);
        std::thread::sleep(2 * bound);
        assert_eq!(stuck.step(false, &latch), HaltStep::Close);
    }
}
