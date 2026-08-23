//! DPDK kernel-bypass transport for the trading engine.
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

#[cfg(feature = "dpdk-sys")]
mod dpdk;
#[cfg(feature = "dpdk-sys")]
pub use dpdk::*;
