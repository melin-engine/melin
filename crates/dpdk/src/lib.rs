//! DPDK kernel-bypass transport for the trading engine.
//!
//! Bypasses the Linux kernel network stack entirely by talking directly
//! to the NIC via DPDK's userspace Poll Mode Driver (PMD). TCP/IP
//! processing is handled by smoltcp, a userspace TCP/IP stack.
//!
//! # Architecture
//!
//! ```text
//! NIC ←→ DPDK PMD (rte_eth_rx/tx_burst)
//!          ↕
//!       smoltcp (TCP/IP in userspace)
//!          ↕
//!       DpdkTransport (frame parsing, connection management)
//!          ↕
//!       Disruptor pipeline (journal → matching → response)
//! ```
//!
//! All NIC I/O and TCP processing happens on a single dedicated poll
//! thread, matching the single-threaded LMAX architecture. The response
//! stage writes encoded frames into per-connection lock-free queues;
//! the poll thread drains them into smoltcp sockets.
//!
//! # Prerequisites
//!
//! - DPDK >= 22.11 installed (`pkg-config --cflags --libs libdpdk`)
//! - Hugepages configured (e.g., `echo 1024 > /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages`)
//! - NIC bound to DPDK-compatible driver (`dpdk-devbind.py -b vfio-pci <pci-id>`)
//! - For testing without a real NIC: `--vdev net_tap0` EAL argument
//!
//! # Feature flag
//!
//! Enabled via `--features dpdk` on the server crate. Mutually exclusive
//! with the default epoll transport and `io-uring`.

pub mod device;
pub mod eal;
mod ffi;
pub mod mempool;
pub mod port;
pub mod transport;

pub use eal::{Eal, EalError};
pub use mempool::{Mempool, MempoolError};
pub use port::{Port, PortError};
pub use transport::{AcceptedConnection, DpdkConfig, DpdkTransport};

/// Re-export smoltcp types used by the server's DPDK transport module.
/// This ensures the server uses the same smoltcp crate instance as melin-dpdk,
/// avoiding type mismatches when melin-dpdk is excluded from the workspace.
pub use smoltcp::iface::SocketHandle;
