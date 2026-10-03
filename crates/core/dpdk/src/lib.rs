//! DPDK kernel-bypass transport for the sequencer.
//!
//! Bypasses the Linux kernel network stack entirely by talking directly
//! to the NIC via DPDK's userspace Poll Mode Driver (PMD). TCP/IP
//! processing is handled by smoltcp, a userspace TCP/IP stack.
//!
//! # Feature flag
//!
//! Requires the `dpdk-sys` feature and libdpdk installed. Without the
//! feature this crate is an empty shell, letting it live in the workspace
//! without requiring system dependencies.

// Ungated: MAC parsing and the peer-MAC convention are pure logic with
// no libdpdk dependency, and operators configure them through the same
// CLI whether or not this build has the transport compiled in.
pub mod mac;
pub use mac::{MacAddr, MacParseError, PeerMacSource, parse_mac, resolve_peer_mac, try_parse_mac};
// Ungated for the same reason: which received frames may reach the TCP
// stack is decided from offload flags and frame bytes alone, so the
// policy is tested on any host, with or without libdpdk.
pub mod rx_checksum;
// Ungated for the same reason: what a node does differently on a
// process-wide EAL is decided from its configuration alone.
mod eal_sharing;

#[cfg(feature = "dpdk-sys")]
mod dpdk;
#[cfg(feature = "dpdk-sys")]
pub use dpdk::*;
