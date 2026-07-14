//! openraft type configuration for the Melin control plane.

// The declare_raft_types! default for `SnapshotData` expands to an
// unqualified `Cursor<Vec<u8>>` — the import is for the macro expansion.
use std::io::Cursor;

use serde::Deserialize;
use serde::Serialize;

/// Application request carried in the control-plane log.
///
/// The control plane replicates no application data — leader election and
/// membership *are* the payload, and openraft writes those entries itself
/// (blank entries at leader establishment, membership entries on config
/// change). openraft still requires an app-data type, so this is a unit
/// no-op. Auto-promotion reads the elected **term**, never the log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlRequest {
    Noop,
}

/// Response from the control-plane state machine. There is nothing to say.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlResponse;

openraft::declare_raft_types!(
    /// Melin control-plane types. Everything not named here uses the
    /// openraft default: `NodeId = u64`, `Node = BasicNode`,
    /// `Entry = openraft::Entry<TypeConfig>`,
    /// `SnapshotData = Cursor<Vec<u8>>`, `AsyncRuntime = TokioRuntime`.
    pub TypeConfig:
        D = ControlRequest,
        R = ControlResponse,
);

/// Node identifier — matches `--raft-node-id`.
pub type NodeId = u64;
/// Node metadata: the peer's control-plane `host:port`.
pub type Node = openraft::BasicNode;
pub type Entry = openraft::Entry<TypeConfig>;
pub type LogId = openraft::LogId<NodeId>;
pub type Vote = openraft::Vote<NodeId>;
pub type StorageError = openraft::StorageError<NodeId>;
