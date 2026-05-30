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
        Self::configure_internal(port_id, mempool, vlan_id, num_queues, false)
    }

    /// Configure in bifurcated mode (mlx5): the kernel netdev keeps
    /// ownership of the device; DPDK only receives traffic matching
    /// rules installed with `install_src_ipv4_steering()` after start.
    ///
    /// Internally enables `rte_flow_isolate()` before `rte_eth_dev_configure`,
    /// which mlx5 requires as the very first operation. Promiscuous mode
    /// is NOT enabled in this path — the goal is to leave non-matching
    /// traffic (SSH, ARP, etc.) with the kernel.
    pub fn configure_bifurcated(
        port_id: u16,
        mempool: &Mempool,
        vlan_id: Option<u16>,
        num_queues: u16,
    ) -> Result<Self, PortError> {
        Self::configure_internal(port_id, mempool, vlan_id, num_queues, true)
    }

    fn configure_internal(
        port_id: u16,
        mempool: &Mempool,
        vlan_id: Option<u16>,
        num_queues: u16,
        bifurcated: bool,
    ) -> Result<Self, PortError> {
        // For PMDs like mlx5, `rte_flow_isolate` must be called as the
        // very first operation on the port, before `rte_eth_dev_configure`.
        // Isolated mode means the PMD does not install any default
        // catch-all RSS rule — DPDK only sees traffic that matches
        // explicit rules added later via `install_src_ipv4_steering`.
        if bifurcated {
            let ret = unsafe { ffi::dpdk_flow_isolate(port_id) };
            if ret != 0 {
                return Err(PortError::FlowIsolateFailed(ret));
            }
            tracing::info!(port_id, "rte_flow isolated mode enabled");
        }
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
        // In bifurcated mode this is harmful — promiscuous would race
        // the kernel netdev for incoming frames. We rely entirely on
        // explicit `rte_flow` rules to capture our traffic instead.
        if !bifurcated {
            let ret = unsafe { ffi::rte_eth_promiscuous_enable(port_id) };
            if ret != 0 {
                tracing::warn!(port_id, ret, "failed to enable promiscuous mode");
            }
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

    /// Install a flow steering rule that captures all IPv4 packets with
    /// the given source IPv4 address into RX queue 0. Used in bifurcated
    /// mode (mlx5) to send traffic from the configured peer into DPDK
    /// while leaving everything else (SSH, ARP, other tenants) with the
    /// kernel. Must be called AFTER `start()`.
    ///
    /// The flow handle returned by `rte_flow_create` is intentionally
    /// dropped — the rule lives for the lifetime of the port and is
    /// torn down by `rte_eth_dev_stop` in `Port::drop`.
    pub fn install_src_ipv4_steering(
        &mut self,
        src_ipv4: std::net::Ipv4Addr,
    ) -> Result<(), PortError> {
        // `rte_flow_item_ipv4.hdr.src_addr` is a `rte_be32_t` (network
        // byte order). Reading wire-order octets into a u32 using
        // native-endian semantics produces the same in-memory byte
        // layout the matcher expects on both LE and BE hosts.
        let src_be = u32::from_ne_bytes(src_ipv4.octets());
        let mut err_type: i32 = 0;
        let ret = unsafe {
            ffi::dpdk_install_src_ipv4_steering(self.port_id, src_be, &mut err_type)
        };
        if ret != 0 {
            return Err(PortError::FlowRuleFailed { ret, err_type });
        }
        tracing::info!(
            port_id = self.port_id,
            peer_ip = %src_ipv4,
            "rte_flow steering rule installed (src IPv4 -> queue 0)"
        );
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
    FlowIsolateFailed(i32),
    FlowRuleFailed { ret: i32, err_type: i32 },
}

impl std::fmt::Display for PortError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PortError::InfoFailed(c) => write!(f, "rte_eth_dev_info_get failed: {c}"),
            PortError::ConfigureFailed(c) => write!(f, "rte_eth_dev_configure failed: {c}"),
            PortError::RxQueueFailed(c) => write!(f, "rte_eth_rx_queue_setup failed: {c}"),
            PortError::TxQueueFailed(c) => write!(f, "rte_eth_tx_queue_setup failed: {c}"),
            PortError::StartFailed(c) => write!(f, "rte_eth_dev_start failed: {c}"),
            PortError::FlowIsolateFailed(c) => write!(f, "rte_flow_isolate failed: {c}"),
            PortError::FlowRuleFailed { ret, err_type } => write!(
                f,
                "rte_flow_create failed: ret={ret} err_type={err_type}"
            ),
        }
    }
}

impl std::error::Error for PortError {}
