//! DPDK ethernet port configuration and lifecycle.
//!
//! A "port" is a physical or virtual NIC managed by DPDK. Configuration
//! involves setting up RX/TX queues, descriptor counts, and offload
//! features. The port must be started before traffic flows.
//!
//! For this trading engine we use a single RX queue and a single TX queue
//! on a single port — all NIC I/O happens on one dedicated poll thread,
//! matching the single-threaded LMAX architecture.

use crate::ffi;
use crate::mempool::Mempool;

/// Number of RX descriptors per queue. Each descriptor holds one mbuf.
/// 1024 is standard for 10GbE+ NICs; lower values reduce ring memory
/// but risk drops under burst load.
const RX_DESC: u16 = 1024;

/// Number of TX descriptors per queue.
const TX_DESC: u16 = 1024;

/// Configured DPDK ethernet port, ready for `start()`.
pub struct Port {
    port_id: u16,
    started: bool,
}

impl Port {
    /// Configure a DPDK port with a single RX queue and a single TX queue.
    ///
    /// `port_id` is the DPDK port index (typically 0 for the first NIC
    /// bound to DPDK). `mempool` provides the mbuf pool for RX DMA.
    pub fn configure(port_id: u16, mempool: &Mempool) -> Result<Self, PortError> {
        // Get port info for default RX/TX config.
        let mut dev_info: ffi::rte_eth_dev_info = unsafe { std::mem::zeroed() };
        let ret = unsafe { ffi::rte_eth_dev_info_get(port_id, &mut dev_info) };
        if ret != 0 {
            return Err(PortError::InfoFailed(ret));
        }

        // Minimal port configuration: no RSS (single queue), no offloads.
        // Zero-initialized rte_eth_conf disables all optional features.
        let port_conf: ffi::rte_eth_conf = unsafe { std::mem::zeroed() };

        // Configure port with 1 RX queue + 1 TX queue.
        let ret = unsafe { ffi::rte_eth_dev_configure(port_id, 1, 1, &port_conf) };
        if ret != 0 {
            return Err(PortError::ConfigureFailed(ret));
        }

        // NUMA socket of this port — allocate queues on the same socket
        // for optimal DMA locality.
        let socket_id = unsafe { ffi::rte_eth_dev_socket_id(port_id) };

        // Setup RX queue 0.
        let ret = unsafe {
            ffi::rte_eth_rx_queue_setup(
                port_id,
                0, // queue_id
                RX_DESC,
                socket_id as libc::c_uint,
                std::ptr::null(), // default RX config
                mempool.as_raw(),
            )
        };
        if ret != 0 {
            return Err(PortError::RxQueueFailed(ret));
        }

        // Setup TX queue 0.
        let ret = unsafe {
            ffi::rte_eth_tx_queue_setup(
                port_id,
                0, // queue_id
                TX_DESC,
                socket_id as libc::c_uint,
                std::ptr::null(), // default TX config
            )
        };
        if ret != 0 {
            return Err(PortError::TxQueueFailed(ret));
        }

        // Enable promiscuous mode so we receive all packets (needed for
        // ARP responses and when IP doesn't match NIC hardware filter).
        let ret = unsafe { ffi::rte_eth_promiscuous_enable(port_id) };
        if ret != 0 {
            tracing::warn!(port_id, ret, "failed to enable promiscuous mode");
        }

        tracing::info!(
            port_id,
            rx_desc = RX_DESC,
            tx_desc = TX_DESC,
            "DPDK port configured"
        );

        Ok(Port {
            port_id,
            started: false,
        })
    }

    /// Start the port. After this call, the NIC begins receiving packets
    /// into the RX queue and the TX queue is active.
    pub fn start(&mut self) -> Result<(), PortError> {
        let ret = unsafe { ffi::rte_eth_dev_start(self.port_id) };
        if ret != 0 {
            return Err(PortError::StartFailed(ret));
        }
        self.started = true;

        tracing::info!(port_id = self.port_id, "DPDK port started");

        Ok(())
    }

    /// Get the MAC address of this port.
    pub fn mac_addr(&self) -> [u8; 6] {
        let mut addr: ffi::rte_ether_addr = unsafe { std::mem::zeroed() };
        unsafe {
            ffi::rte_eth_macaddr_get(self.port_id, &mut addr);
        }
        addr.addr_bytes
    }

    /// The DPDK port index.
    pub fn port_id(&self) -> u16 {
        self.port_id
    }
}

impl Drop for Port {
    fn drop(&mut self) {
        if self.started {
            unsafe {
                ffi::rte_eth_dev_stop(self.port_id);
            }
            tracing::info!(port_id = self.port_id, "DPDK port stopped");
        }
    }
}

#[derive(Debug)]
pub enum PortError {
    InfoFailed(i32),
    ConfigureFailed(i32),
    RxQueueFailed(i32),
    TxQueueFailed(i32),
    StartFailed(i32),
}

impl std::fmt::Display for PortError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PortError::InfoFailed(c) => write!(f, "rte_eth_dev_info_get failed: {c}"),
            PortError::ConfigureFailed(c) => write!(f, "rte_eth_dev_configure failed: {c}"),
            PortError::RxQueueFailed(c) => write!(f, "rte_eth_rx_queue_setup failed: {c}"),
            PortError::TxQueueFailed(c) => write!(f, "rte_eth_tx_queue_setup failed: {c}"),
            PortError::StartFailed(c) => write!(f, "rte_eth_dev_start failed: {c}"),
        }
    }
}

impl std::error::Error for PortError {}
