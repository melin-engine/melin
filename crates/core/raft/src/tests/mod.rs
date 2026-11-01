//! Tests that drive more than one module at once: a three-node election,
//! the RPC transport end to end, and the storage compliance suite. They
//! live inside the crate rather than under `tests/` because what they
//! exercise (the RPC server, the client factory, the file store) is
//! private: the crate's only public surface is what the server runtime
//! needs to start the driver.

mod election;
mod pre_version_wire;
mod rpc_loopback;
mod storage_suite;

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::recency::TipSource;
use melin_transport_core::cursors::AdvertisedJournalTip;
use melin_transport_core::fence::FenceState;

/// A ready advertised journal tip at (epoch, seq). Most tests want (0, 0)
/// so recency filters nothing; the recency tests raise it.
fn tip_at(epoch: u64, seq: u64) -> Arc<TipSource> {
    let fence = Arc::new(FenceState::new(0));
    fence.observe_epoch(epoch);
    Arc::new(TipSource {
        fence,
        seq: AdvertisedJournalTip::new(melin_transport_core::WireSeq::new(seq)),
        ready: Arc::new(AtomicBool::new(true)),
    })
}
