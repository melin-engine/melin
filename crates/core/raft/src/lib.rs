//! Control-plane Raft for the Melin cluster.
//!
//! Wraps tikv's `raft` crate (etcd-lineage state machine) for **leader
//! election, membership, and fencing-epoch allocation only** — order flow
//! stays on the existing replication data plane, so the matching hot path
//! pays nothing for consensus (see `docs/roadmap.md`, "Control-plane
//! Raft").
//!
//! Design pillars:
//!
//! - **Caller-driven**: no async runtime. The server hosts one control-
//!   plane thread that calls [`tick`](raft::RawNode::tick), feeds inbound
//!   peer messages via `step`, and drains the ready state — the same
//!   drive-it-yourself model as the rest of the codebase.
//! - **Durable votes**: `HardState` (current term, voted-for) and log
//!   entries are fsynced *before* any message referencing them is sent,
//!   via [`storage::FileStorage`].
//! - **Term = fencing epoch**: the Raft term doubles as the replication
//!   fencing epoch (`docs/replication.md`, "Fencing epochs") — a newly
//!   elected leader journals `EpochBump { epoch: term }`, which makes
//!   epochs unique per tenure and closes the documented dual-promotion
//!   collision.
//! - **Journal-tip recency votes**: because order data replicates
//!   out-of-band, the vote rule is extended at the RPC boundary —
//!   candidates advertise their journal tip and voters drop vote
//!   requests from candidates behind their own tip
//!   ([`recency`]). Dropping a vote request can only delay an election
//!   (liveness), never violate Raft safety.

pub mod node;
pub mod recency;
mod slog_bridge;
pub mod storage;
pub mod wire;

pub use node::{ControlNode, Drained};
pub use slog_bridge::tracing_logger;

// Re-export the pinned raft-rs surface consumers need, so downstream
// crates depend on `melin-raft` alone and cannot drift onto a different
// raft-rs rev.
pub use raft::{
    Config, RawNode, StateRole,
    eraftpb::{ConfState, Entry, HardState, Message, MessageType, Snapshot},
};
