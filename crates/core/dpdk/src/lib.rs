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
//!
//! The `clap` feature, independent of `dpdk-sys` and also needing no
//! libdpdk, adds `DpdkArgs`: the `--dpdk-*` flags as one clap argument
//! group, which the node, the shm-proxy and the echo client flatten into
//! their command lines, and its mapping onto a [`NicConfig`] with the one
//! validation of them.

// Ungated: MAC parsing and the peer-MAC convention are pure logic with
// no libdpdk dependency, and operators configure them through the same
// CLI whether or not this build has the transport compiled in.
pub mod mac;
pub use mac::{
    MacAddr, MacParseError, PeerMacSource, fmt_mac, parse_mac, resolve_peer_mac, try_parse_mac,
};
// Ungated for the same reason: which received frames may reach the TCP
// stack is decided from offload flags and frame bytes alone, so the
// policy is tested on any host, with or without libdpdk.
pub mod rx_checksum;
// What a NIC group does differently on an EAL it shares (the process-wide
// one, or its node's other group's) is decided from its configuration
// alone, so it is tested on any host; only the gated transport calls it, so
// it is compiled for that and for the tests.
#[cfg(any(test, feature = "dpdk-sys"))]
mod eal_sharing;
// Ungated for the same reason as `rx_checksum`: when a long-lived link's
// peer counts as gone is TCP-stack logic, tested over an in-memory wire.
// What in it acts on a socket is compiled for the transport and the tests.
pub mod peer_liveness;
pub use peer_liveness::PeerLiveness;
// Ungated for the same reason: the ARP frames a transport sends and
// injects are a fixed byte layout, tested on any host.
pub mod arp;
// Ungated: configuration is plain data, mapped from the same flags whether
// or not this build has the transport compiled in.
mod config;
pub use config::{DpdkConfig, NicConfig, ReplicationNic, SocketBuffers};
// Ungated by `dpdk-sys` for the same reason, behind `clap` only: the flags
// mapped onto that configuration, declared once for every binary.
#[cfg(feature = "clap")]
mod args;
#[cfg(feature = "clap")]
pub use args::{DpdkArgs, DpdkArgsError, parse_mac_flag};
// The client NIC handle's clock, local ports, dial wait and stream I/O are
// TCP-stack logic, tested over an in-memory wire on any host; the handle
// itself is in the gated `dpdk` module, and is their only caller, so they
// are compiled for it and for the tests. So is `dial_addr`, whose callers
// all dial over a NIC.
#[cfg(any(test, feature = "dpdk-sys"))]
mod nic;
#[cfg(feature = "dpdk-sys")]
pub use nic::dial_addr;
// Which listening socket a SYN finds, and when a port's backlog hands a
// connection out and refills, is TCP-stack logic, tested over an in-memory
// wire on any host; the transport that accepts through it is in the gated
// `dpdk` module, and is its only caller, so it is compiled for that and for
// the tests.
#[cfg(any(test, feature = "dpdk-sys"))]
mod listen_backlog;
#[cfg(test)]
mod test_wire;

#[cfg(feature = "dpdk-sys")]
mod dpdk;
#[cfg(feature = "dpdk-sys")]
pub use dpdk::*;
