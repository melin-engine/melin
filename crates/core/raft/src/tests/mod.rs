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
mod voter_cap;

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::recency::TipSource;
use base64::Engine;
use ed25519_dalek::SigningKey;
use melin_app::auth::{AuthorizedKeys, NoRoles};
use melin_transport_core::cursors::AdvertisedJournalTip;
use melin_transport_core::fence::FenceState;

/// An authorized-keys table listing each `(key, role, name)` entry, in
/// order. Roles are the runtime's own (`replication`, `operator`): the
/// control plane needs no application roles.
pub(crate) fn authorized_keys_table(entries: &[(&SigningKey, &str, &str)]) -> AuthorizedKeys {
    let table: String = entries
        .iter()
        .map(|(key, role, name)| {
            format!(
                "{role} {} {name}\n",
                base64::engine::general_purpose::STANDARD.encode(key.verifying_key().to_bytes())
            )
        })
        .collect();
    AuthorizedKeys::parse::<NoRoles>(&table).unwrap()
}

/// The vote and the log entries a store at `dir` holds once reopened:
/// what the raft core will read.
pub(crate) async fn reopened(
    dir: &std::path::Path,
) -> (Option<crate::types::Vote>, Vec<crate::types::Entry>) {
    use openraft::RaftLogReader;
    use openraft::storage::RaftLogStorage;
    let (mut log, _sm) = crate::storage::open(dir).expect("reopen");
    let vote = log.read_vote().await.expect("read vote");
    let entries = log.try_get_log_entries(0..).await.expect("read log");
    (vote, entries)
}

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
