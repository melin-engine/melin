//! Tests that drive the response stage directly, with a hand-held journal
//! cursor, rather than through a running node. They live inside the crate
//! rather than under `tests/` because the stage, its early-reply channel
//! and its ring sizing are private: an application starts the stage
//! through the server, never by hand.

mod response_flush_before_gate;
mod response_flush_within_batch;

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use melin_transport_core::divergence::{DivergenceLatch, FsSync};
use melin_transport_core::fence::FenceState;

/// A divergence latch for a stage under test that nothing trips: its own
/// fence and shutdown flag, its marker at a path never written.
pub(crate) fn idle_latch() -> DivergenceLatch {
    DivergenceLatch::new(
        Arc::new(FenceState::new(0)),
        Arc::new(AtomicBool::new(false)),
        std::path::PathBuf::from("/nonexistent/idle.journal.diverged"),
        Arc::new(FsSync),
    )
}
