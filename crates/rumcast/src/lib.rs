//! Reliable UDP multicast/unicast transport — a stripped-down, Melin-tuned
//! reimplementation of the Aeron messaging model.
//!
//! # What this crate does
//!
//! Provides a NAK-driven reliable transport over UDP that supports both
//! unicast (replication: primary → replica) and multicast (market-data
//! fan-out: engine → many subscribers) on the same protocol.
//!
//! # What this crate does NOT do
//!
//! Aeron ships features Melin does not need. The following are explicit
//! non-goals — adding any of them requires a scope-change discussion:
//!
//! - Multi-language clients (Rust-only end-to-end).
//! - Separate Media Driver process (Melin is a single binary).
//! - Aeron Archive (Melin uses [`melin-journal`] for durable storage).
//! - Aeron Cluster / Raft (replication is a separate concern).
//! - Multi-Destination Cast (MDC), spy subscriptions, tagged flow control.
//! - Stream/session multiplexing at the wire level (channels are typed
//!   and known at startup).
//! - Channel URI parser (typed config structs instead).
//!
//! # Design pillars
//!
//! - **Single writer per publication** — fast path is one atomic increment
//!   plus a memcpy plus a release store. No locks, no allocation.
//! - **Position-as-sequence** — a 64-bit position uniquely identifies every
//!   byte in a stream; gaps are detected by position, not by per-message seq.
//! - **NAK-based reliability** — receivers detect gaps, send NAKs; senders
//!   retransmit from the still-resident log buffer. No per-message ACK tax.
//! - **Pluggable transport substrate** — kernel UDP today, DPDK tomorrow,
//!   RDMA later. Hot path monomorphizes through the [`Transport`] trait.
//!
//! [`melin-journal`]: ../melin_journal/index.html
//! [`Transport`]: crate::transport::Transport

#[cfg(not(target_endian = "little"))]
compile_error!(
    "melin-rumcast wire format assumes a little-endian target; \
     bytemuck-based zero-copy header decode is unsafe on big-endian"
);

#[cfg(feature = "io-uring")]
pub mod io_uring_endpoint;

pub mod counters;
pub mod flow_control;
pub mod muxed_receiver;
pub mod muxed_sender;
pub mod pub_log;
pub mod receiver;
pub mod sender;
pub mod shared_udp;
// SPSC ring is only consumed by the io_uring endpoint today; gating
// it behind the same feature keeps clippy from flagging the module
// as dead code in the default build.
#[cfg(feature = "io-uring")]
mod spsc;
mod storage;
pub mod sub_log;
pub mod transport;
pub mod wire;
