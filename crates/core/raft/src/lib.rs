//! Control-plane consensus for Melin, built on openraft.
//!
//! Carries **leader election, static membership, and fencing epochs only** —
//! event flow stays on the synchronous replication data plane
//! (`melin-server-runtime`), and nothing on the ~100ns hot path calls into
//! this crate. See `docs/replication.md` ("Limitations") for the operator
//! contract this implements and `docs/internal/raft-control-plane.md` for
//! the design.
//!
//! The server runtime is the crate's only consumer, so its public surface
//! is just what starting the driver takes: [`spawn`] with its
//! configuration, the node's journal tip it votes on, and the
//! supersession policy that steps a superseded primary down, plus
//! [`QUORUM_ACK_MAX_AGE`], the threshold the promotion policy applies to
//! the quorum-ack age the driver publishes, and [`LeaderConfirm`], the
//! blocking quorum round trip that confirms this node still leads. The RPC
//! server, wire format, authentication and file store stay private.

mod auth;
mod driver;
mod network;
mod recency;
mod rpc_server;
mod storage;
mod types;
mod wire;

#[cfg(test)]
mod tests;

pub use driver::{
    LeaderConfirm, LeaderConfirmError, QUORUM_ACK_MAX_AGE, RaftConfig, RaftHandles, RaftPeer, spawn,
};
pub use recency::{JournalTip, PeerTips, TipSource};
pub use rpc_server::SupersessionPolicy;
