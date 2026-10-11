//! DPDK kernel-bypass transport for the sequencer.
//!
//! Bypasses the Linux kernel network stack entirely by talking directly
//! to the NIC via DPDK's userspace Poll Mode Driver (PMD). TCP/IP
//! processing is handled by smoltcp, a userspace TCP/IP stack.
//!
//! Two users of one transport:
//!
//! - **A node**: `DpdkTransport`, listening for clients and replicas and
//!   dialing its primary, configured by a [`DpdkConfig`].
//! - **A client**: `NicHandle`, the same transport constructed listening on
//!   no port, which dials TCP streams to a node, one at a time, and
//!   services all of them in one device poll, configured by the node's own
//!   [`NicConfig`]. Its API is std types only. `melin-client`'s `dpdk`
//!   feature runs its polled connection on it, and `melin-shm-proxy` dials
//!   through it.
//!
//! # Feature flag
//!
//! The transport and the NIC handle require the `dpdk-sys` feature and
//! libdpdk installed (and libclang, for the generated bindings). Without
//! the feature the crate holds only what needs no libdpdk: the
//! configuration ([`DpdkConfig`], [`NicConfig`]), MAC parsing and the
//! peer-MAC convention ([`mac`]), the ARP frames ([`arp`]), and the
//! policies tested on any host ([`rx_checksum`], [`peer_liveness`]). A
//! binary maps its flags onto the configuration in any build, and the
//! crate lives in the workspace without requiring system dependencies.

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
// Ungated for the same reason: when a long-lived link's peer counts as
// gone is TCP-stack logic, tested over an in-memory wire.
pub mod peer_liveness;
pub use peer_liveness::PeerLiveness;
// Ungated for the same reason: the ARP frames a transport sends and
// injects are a fixed byte layout, tested on any host.
pub mod arp;
// Ungated: configuration is plain data, mapped from the same flags whether
// or not this build has the transport compiled in.
mod config;
pub use config::{DpdkConfig, NicConfig};
// Ungated for the same reason as `peer_liveness`: the client NIC handle's
// clock, local ports, dial wait and stream I/O are TCP-stack logic, tested
// over an in-memory wire. The handle itself is in the gated `dpdk` module.
mod nic;
#[cfg(test)]
mod test_wire;

#[cfg(feature = "dpdk-sys")]
mod dpdk;
#[cfg(feature = "dpdk-sys")]
pub use dpdk::*;
