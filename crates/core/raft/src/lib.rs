//! Control-plane consensus for Melin, built on openraft.
//!
//! Carries **leader election, static membership, and fencing epochs only** —
//! order flow stays on the synchronous replication data plane
//! (`melin-server-runtime`), and nothing on the ~100ns hot path calls into
//! this crate. See `docs/replication.md` ("Limitations") for the operator
//! contract this implements and `docs/internal/raft-control-plane.md` for
//! the design.

pub mod auth;
pub mod network;
pub mod rpc_server;
pub mod storage;
pub mod types;
pub mod wire;
