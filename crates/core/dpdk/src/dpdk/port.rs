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

/// Which checksum offloads the NIC supports.
#[derive(Debug, Clone, Copy, Default)]
pub struct ChecksumOffloads {
    pub rx_ip: bool,
    pub rx_tcp: bool,
    pub tx_ip: bool,
    pub tx_tcp: bool,
}

impl ChecksumOffloads {
    /// Intersection of two offload sets — only capabilities supported
    /// by both are retained. Used for multi-port setups where all ports
    /// must agree on offload capabilities.
    pub fn intersect(self, other: Self) -> Self {
        ChecksumOffloads {
            rx_ip: self.rx_ip && other.rx_ip,
            rx_tcp: self.rx_tcp && other.rx_tcp,
            tx_ip: self.tx_ip && other.tx_ip,
            tx_tcp: self.tx_tcp && other.tx_tcp,
        }
    }
}

/// Configured DPDK ethernet port, ready for `start()`.
pub struct Port {
    port_id: u16,
    /// Number of RX/TX queue pairs configured on this port.
    pub num_queues: u16,
    started: bool,
    /// Hardware checksum offloads enabled on this port.
    pub offloads: ChecksumOffloads,
}

impl Port {
    /// Configure a DPDK port with the given number of RX/TX queue pairs.
    ///
    /// `port_id` is the DPDK port index (typically 0 for the first NIC
    /// bound to DPDK). `mempool` provides the mbuf pool for RX DMA.
    /// `vlan_id` enables hardware VLAN strip (RX) and insert (TX) for
    /// dedicated NIC mode where the kernel isn't handling VLAN tags.
    /// When `num_queues > 1`, RSS (Receive Side Scaling) is enabled to
    /// distribute TCP/IP flows across RX queues.
    pub fn configure(port_id: u16, mempool: &Mempool) -> Result<Self, PortError> {
        Self::configure_with_vlan(port_id, mempool, None, 1)
    }

    /// Configure with optional VLAN hardware offload and N queue pairs.
    pub fn configure_with_vlan(
        port_id: u16,
        mempool: &Mempool,
        vlan_id: Option<u16>,
        num_queues: u16,
    ) -> Result<Self, PortError> {
        // Get port info for default RX/TX config.
        let mut dev_info: ffi::rte_eth_dev_info = unsafe { std::mem::zeroed() };
        let ret = unsafe { ffi::rte_eth_dev_info_get(port_id, &mut dev_info) };
        if ret != 0 {
            return Err(PortError::InfoFailed(ret));
        }

        // Clamp to NIC's maximum supported queues. TAP devices only
        // support 1 queue; real NICs typically support 4-128.
        let max_queues = dev_info.max_rx_queues.min(dev_info.max_tx_queues).max(1);
        let requested_queues = num_queues;
        let num_queues = num_queues.min(max_queues);
        if num_queues < requested_queues {
            tracing::warn!(
                port_id,
                requested = requested_queues,
                actual = num_queues,
                "clamped queue count to NIC maximum"
            );
        }

        // Query NIC capabilities and enable hardware checksum offloads
        // where supported. Checksum offload eliminates per-packet software
        // checksum computation in smoltcp — the NIC computes/verifies instead.
        let rx_cksum_wanted = unsafe { ffi::dpdk_rx_offload_checksum() };
        let tx_cksum_wanted = unsafe { ffi::dpdk_tx_offload_checksum() };

        let mut rx_offloads = dev_info.rx_offload_capa & rx_cksum_wanted;
        let mut tx_offloads = dev_info.tx_offload_capa & tx_cksum_wanted;

        // Enable VLAN strip/insert if a VLAN ID is specified (dedicated NIC mode).
        // Strip removes the 4-byte 802.1Q tag on RX so smoltcp sees plain Ethernet.
        // Insert adds it back on TX so the switch routes to the correct VLAN.
        let vlan_strip = unsafe { ffi::dpdk_rx_offload_vlan_strip() };
        let vlan_insert = unsafe { ffi::dpdk_tx_offload_vlan_insert() };
        if vlan_id.is_some() {
            if dev_info.rx_offload_capa & vlan_strip != 0 {
                rx_offloads |= vlan_strip;
                tracing::info!(port_id, "VLAN strip enabled (RX)");
            } else {
                tracing::warn!(port_id, "NIC does not support RX VLAN strip");
            }
            if dev_info.tx_offload_capa & vlan_insert != 0 {
                tx_offloads |= vlan_insert;
                tracing::info!(port_id, "VLAN insert enabled (TX)");
            } else {
                tracing::warn!(port_id, "NIC does not support TX VLAN insert");
            }
        }

        let mut port_conf: ffi::rte_eth_conf = unsafe { std::mem::zeroed() };
        port_conf.rxmode.offloads = rx_offloads;
        port_conf.txmode.offloads = tx_offloads;

        // Enable RSS when multiple RX queues are requested. The NIC
        // hashes TCP/IP flows across queues so each poll thread handles
        // a disjoint set of connections.
        if num_queues > 1 {
            let rss_hf = unsafe { ffi::dpdk_eth_rss_ip() | ffi::dpdk_eth_rss_tcp() };
            // Only request hash types the NIC supports.
            let supported_hf = dev_info.flow_type_rss_offloads;
            port_conf.rx_adv_conf.rss_conf.rss_hf = rss_hf & supported_hf;
            port_conf.rx_adv_conf.rss_conf.rss_key = std::ptr::null_mut(); // NIC default key
            port_conf.rx_adv_conf.rss_conf.rss_key_len = 0;
            tracing::info!(
                port_id,
                num_queues,
                rss_hf = rss_hf & supported_hf,
                "RSS enabled"
            );
        }

        let offloads = ChecksumOffloads {
            rx_ip: rx_offloads & (1u64 << 1) != 0, // RTE_ETH_RX_OFFLOAD_IPV4_CKSUM
            rx_tcp: rx_offloads & (1u64 << 3) != 0, // RTE_ETH_RX_OFFLOAD_TCP_CKSUM
            tx_ip: tx_offloads & (1u64 << 1) != 0, // RTE_ETH_TX_OFFLOAD_IPV4_CKSUM
            tx_tcp: tx_offloads & (1u64 << 3) != 0, // RTE_ETH_TX_OFFLOAD_TCP_CKSUM
        };

        // Configure port with N RX queues + N TX queues (one pair per
        // poll thread). Each thread reads from its RX queue and writes
        // to its TX queue.
        let ret =
            unsafe { ffi::rte_eth_dev_configure(port_id, num_queues, num_queues, &port_conf) };
        if ret != 0 {
            return Err(PortError::ConfigureFailed(ret));
        }

        // NUMA socket of this port — allocate queues on the same socket
        // for optimal DMA locality.
        let socket_id = unsafe { ffi::rte_eth_dev_socket_id(port_id) };

        // Setup N RX queues.
        for q in 0..num_queues {
            let ret = unsafe {
                ffi::rte_eth_rx_queue_setup(
                    port_id,
                    q,
                    RX_DESC,
                    socket_id as libc::c_uint,
                    std::ptr::null(), // default RX config
                    mempool.as_raw(),
                )
            };
            if ret != 0 {
                return Err(PortError::RxQueueFailed(ret));
            }
        }

        // Setup N TX queues.
        for q in 0..num_queues {
            let ret = unsafe {
                ffi::rte_eth_tx_queue_setup(
                    port_id,
                    q,
                    TX_DESC,
                    socket_id as libc::c_uint,
                    std::ptr::null(), // default TX config
                )
            };
            if ret != 0 {
                return Err(PortError::TxQueueFailed(ret));
            }
        }

        // Enable promiscuous mode so we receive all packets (needed for
        // ARP responses and when IP doesn't match NIC hardware filter).
        let ret = unsafe { ffi::rte_eth_promiscuous_enable(port_id) };
        if ret != 0 {
            tracing::warn!(port_id, ret, "failed to enable promiscuous mode");
        }

        tracing::info!(
            port_id,
            num_queues,
            rx_desc = RX_DESC,
            tx_desc = TX_DESC,
            ?offloads,
            "DPDK port configured"
        );

        Ok(Port {
            port_id,
            num_queues,
            started: false,
            offloads,
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

    /// Whether the port's link is up (non-blocking query). For memif this
    /// reflects whether the peer has completed the control-plane handshake,
    /// so callers can wait for the link before pumping rx/tx (pumping
    /// concurrently with the in-progress handshake races it).
    pub fn link_up(&self) -> bool {
        let mut link: ffi::rte_eth_link = unsafe { std::mem::zeroed() };
        // Non-blocking: reads cached link state, no device round-trip.
        // `rte_eth_link` is a union (val64 | bitfield struct), so reading the
        // link_status bit goes through the bindgen-generated union arms.
        unsafe {
            ffi::rte_eth_link_get_nowait(self.port_id, &mut link);
            link.__bindgen_anon_1.__bindgen_anon_1.link_status() != 0
        }
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
