//! DPDK ethernet port configuration and lifecycle.
//!
//! A "port" is a physical or virtual NIC managed by DPDK. Configuration
//! involves setting up RX/TX queues, descriptor counts, and offload
//! features. The port must be started before traffic flows.
//!
//! For the sequencer we use a single RX queue and a single TX queue
//! on a single port — all client I/O happens on one dedicated poll
//! thread, matching the single-threaded LMAX architecture — or, with
//! replication on a queue of its own, a second queue pair for it.

use crate::ffi;
use crate::mempool::Mempool;
use crate::steering::{EtherTypeRule, QueuePlan, SteeringRule, TooFewQueues};
use std::ptr::NonNull;

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
    /// Configure a DPDK port with the queue pairs `queues` asks for.
    ///
    /// `port_id` is the DPDK port index (typically 0 for the first NIC
    /// bound to DPDK). `mempool` provides the mbuf pool for RX DMA.
    /// `vlan_id` enables hardware VLAN strip (RX) and insert (TX) for
    /// dedicated NIC mode where the kernel isn't handling VLAN tags.
    /// With [`QueuePlan::Rss`] of more than one queue, RSS distributes
    /// TCP/IP flows across RX queues; with one, as a node runs, there is
    /// no RSS. With [`QueuePlan::ReplicationSplit`] the port gets two
    /// queue pairs, or is refused when it offers fewer, and RSS requests
    /// no hash field: the caller pins its redirection table to the client
    /// queue after `start()` ([`Self::pin_rss_reta`]) unless the port is
    /// isolated, and from then on the hash no longer matters.
    ///
    /// When `bifurcated` is true the PMD opens in isolated mode (mlx5
    /// requires `rte_flow_isolate()` as the very first operation), the
    /// kernel netdev keeps ownership of the device, and promiscuous
    /// mode is left off — the caller is then responsible for installing
    /// explicit steering rules via [`Self::install_steering_rule`] after
    /// `start()`. Non-matching traffic stays with the kernel.
    pub fn configure(
        port_id: u16,
        mempool: &Mempool,
        vlan_id: Option<u16>,
        queues: QueuePlan,
        bifurcated: bool,
    ) -> Result<Self, PortError> {
        // For PMDs like mlx5, `rte_flow_isolate` must be called as the
        // very first operation on the port, before `rte_eth_dev_configure`.
        // Isolated mode means the PMD does not install any default
        // catch-all RSS rule — DPDK only sees traffic that matches
        // explicit rules added later via `install_steering_rule`.
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
        // support 1 queue; real NICs typically support 4-128. The
        // replication split is refused instead: its second queue is not
        // optional.
        let requested_queues = queues.requested_queue_pairs();
        let num_queues = queues
            .queue_pairs_on(dev_info.max_rx_queues, dev_info.max_tx_queues)
            .map_err(PortError::TooFewQueues)?;
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
        // On receive the NIC only *reports* a bad checksum, it does not drop
        // the frame: the device acts on that report (see `rx_checksum`).
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
        if queues == QueuePlan::ReplicationSplit {
            // RSS mode, which ethdev requires for the redirection table
            // update that pins it to the client queue, requesting no hash
            // field. That request is not relied on: a PMD may read an
            // empty set as its default (mlx5 is believed to hash IP
            // then), so between `start()` and the pin a packet can still
            // reach queue 1. Once the table is pinned, every entry is the
            // client queue and the hash no longer matters.
            port_conf.rxmode.mq_mode = unsafe { ffi::dpdk_eth_mq_rx_rss() } as _;
            port_conf.rx_adv_conf.rss_conf.rss_hf = 0;
            // Isolated mode delivers only what the flow rules capture, so
            // the table is never pinned there; the line says so.
            let default_rx = if bifurcated {
                "isolated, unmatched traffic stays with the kernel"
            } else {
                "RSS table to be pinned to queue 0"
            };
            tracing::info!(
                port_id,
                default_rx,
                "two queue pairs for the replication split"
            );
        } else if num_queues > 1 {
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

    /// Install one `rte_flow` steering rule, as a [`SteeringPlan`] gives
    /// it: in bifurcated mode (mlx5), the capture of the peer's traffic
    /// into DPDK, leaving everything else (SSH, ARP, other tenants) with
    /// the kernel; with the replication split, the rules sending
    /// replication to its queue. Must be called AFTER `start()` (and after
    /// [`Self::pin_rss_reta`], which may restart the port).
    ///
    /// The flow handle returned by `rte_flow_create` is intentionally
    /// dropped — the rule lives for the lifetime of the port and is
    /// torn down when `Port::drop` stops and closes the device.
    ///
    /// [`SteeringPlan`]: crate::SteeringPlan
    pub fn install_steering_rule(&mut self, rule: &SteeringRule) -> Result<(), PortError> {
        // Addresses and ports are big-endian on the wire and in the
        // pattern's headers. Reading wire-order octets with native-endian
        // semantics produces the in-memory layout the matcher expects on
        // both LE and BE hosts.
        let ffi_rule = ffi::dpdk_steering_rule {
            priority: rule.priority,
            src_ipv4_be: rule
                .src_ipv4
                .map_or(0, |ip| u32::from_ne_bytes(ip.octets())),
            dst_ipv4_be: rule
                .dst_ipv4
                .map_or(0, |ip| u32::from_ne_bytes(ip.octets())),
            tcp_src_port_be: rule.tcp_src_port.map_or(0, u16::to_be),
            tcp_dst_port_be: rule.tcp_dst_port.map_or(0, u16::to_be),
            eth_type_be: 0,
            queue: rule.queue,
            has_src_ipv4: rule.src_ipv4.is_some().into(),
            has_dst_ipv4: rule.dst_ipv4.is_some().into(),
            has_tcp_src_port: rule.tcp_src_port.is_some().into(),
            has_tcp_dst_port: rule.tcp_dst_port.is_some().into(),
            has_eth_type: 0,
        };
        self.create_flow(&ffi_rule, rule)?;
        Ok(())
    }

    /// Install `rule`, an [`EtherTypeRule`], keeping its handle, so that
    /// it can be removed before the port closes ([`RemovableRule::remove`]):
    /// a replica's ARP rule, removed at promotion. Called as
    /// [`Self::install_steering_rule`] is, after `start()` and the pin.
    pub(crate) fn install_removable_rule(
        &mut self,
        rule: &EtherTypeRule,
    ) -> Result<RemovableRule, PortError> {
        let ffi_rule = ffi::dpdk_steering_rule {
            priority: rule.priority,
            eth_type_be: rule.ether_type.to_be(),
            queue: rule.queue,
            has_eth_type: 1,
            ..Default::default()
        };
        let flow = self.create_flow(&ffi_rule, rule)?;
        Ok(RemovableRule {
            port_id: self.port_id,
            flow,
            rule: *rule,
        })
    }

    /// The one call to the shim's install, for either kind of rule,
    /// `shown` as the rule is logged: the flow's handle, or the PMD's
    /// refusal.
    fn create_flow(
        &mut self,
        ffi_rule: &ffi::dpdk_steering_rule,
        shown: &dyn std::fmt::Display,
    ) -> Result<NonNull<ffi::rte_flow>, PortError> {
        let mut flow: *mut ffi::rte_flow = std::ptr::null_mut();
        let mut error = FlowError::new();
        let ret = unsafe {
            ffi::dpdk_install_steering_rule(
                self.port_id,
                ffi_rule,
                &mut flow,
                &mut error.err_type,
                error.message.as_mut_ptr(),
                error.message.len(),
            )
        };
        let flow = match NonNull::new(flow) {
            Some(flow) if ret == 0 => flow,
            // A success with no handle is not one the shim returns, and
            // would leave a rule nothing can remove: reported as a failure.
            _ => {
                let (err_type, message) = error.into_parts();
                return Err(PortError::FlowRuleFailed {
                    ret,
                    err_type,
                    message,
                });
            }
        };
        tracing::info!(port_id = self.port_id, rule = %shown, "rte_flow steering rule installed");
        Ok(flow)
    }

    /// Point every entry of the port's RSS redirection table at `queue`,
    /// so that whatever the default RSS receives lands there. Must be
    /// called after `start()`, and before the steering rules: a PMD may
    /// restart the port to apply it (mlx5 does). The port must have been
    /// configured with [`QueuePlan::ReplicationSplit`], whose RSS mode the
    /// update needs.
    pub fn pin_rss_reta(&mut self, queue: u16) -> Result<(), PortError> {
        let ret = unsafe { ffi::dpdk_pin_rss_reta(self.port_id, queue) };
        if ret != 0 {
            return Err(PortError::RetaPinFailed { queue, ret });
        }
        tracing::info!(
            port_id = self.port_id,
            queue,
            "RSS redirection table pinned to one queue"
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

/// A steering rule installed with its handle kept
/// ([`Port::install_removable_rule`]), so that it can be removed before
/// its port closes. Dropped unremoved, it does nothing: the rule goes with
/// the port, as every other rule does.
///
/// Crate-private, and held only beside the port it was installed on, in
/// the NIC group that owns both (`NicResources`), so that whoever reaches
/// the rule holds the port open: that is what makes [`Self::remove`]
/// sound, the handle being valid only while its port is open.
pub(crate) struct RemovableRule {
    port_id: u16,
    flow: NonNull<ffi::rte_flow>,
    /// The rule as installed, to name it when it is removed.
    rule: EtherTypeRule,
}

// SAFETY: the handle is a token for the `rte_flow` calls of its port, which
// may be made from any thread: ethdev serializes them per port for a PMD
// that does not declare them thread-safe itself.
unsafe impl Send for RemovableRule {}

impl RemovableRule {
    /// Remove the rule from its port, which must still be open: the NIC
    /// steers what it matched as if it had never been installed.
    pub(crate) fn remove(self) -> Result<(), PortError> {
        let mut error = FlowError::new();
        // SAFETY: the flow was created on this port and not yet destroyed
        // (`remove` takes the rule by value), and the port is open: see the
        // type's documentation.
        let ret = unsafe {
            ffi::dpdk_remove_steering_rule(
                self.port_id,
                self.flow.as_ptr(),
                &mut error.err_type,
                error.message.as_mut_ptr(),
                error.message.len(),
            )
        };
        if ret != 0 {
            let (err_type, message) = error.into_parts();
            return Err(PortError::FlowRuleRemovalFailed {
                ret,
                err_type,
                message,
            });
        }
        tracing::info!(port_id = self.port_id, rule = %self.rule, "rte_flow steering rule removed");
        Ok(())
    }

    /// The rule as installed.
    pub(crate) fn rule(&self) -> &EtherTypeRule {
        &self.rule
    }

    /// The port it was installed on.
    pub(crate) fn port_id(&self) -> u16 {
        self.port_id
    }
}

/// Where the shim reports an `rte_flow` call's failure: the error type and
/// the PMD's explanation.
struct FlowError {
    err_type: i32,
    /// Room for the PMD's explanation, which names what it refused (a
    /// priority out of range, an unsupported item); longer is cut.
    message: [libc::c_char; 256],
}

impl FlowError {
    fn new() -> Self {
        FlowError {
            err_type: 0,
            message: [0; 256],
        }
    }

    /// The error type and the message, as `PortError` carries them.
    fn into_parts(self) -> (i32, String) {
        // NUL-terminated by the shim, which truncates to fit, and zeroed
        // here before the call when the shim writes nothing.
        let message = unsafe { std::ffi::CStr::from_ptr(self.message.as_ptr()) }
            .to_string_lossy()
            .into_owned();
        (self.err_type, message)
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
        // Close the port (after stop) to release its RX/TX queues and, for
        // vdev PMDs like net_tap, the backing kernel device — BEFORE EAL
        // teardown. `rte_eal_cleanup()` walks the vdev bus and removes every
        // device; a port left merely *stopped* (not closed) makes the tap
        // PMD's remove path dereference freed queue state and segfault. The
        // contract is stop → close → cleanup. Closing is valid on any
        // configured port (a `Port` always represents one), started or not.
        let ret = unsafe { ffi::rte_eth_dev_close(self.port_id) };
        if ret != 0 {
            // Best-effort: we are tearing down and have nowhere to propagate.
            tracing::warn!(
                port_id = self.port_id,
                ret,
                "rte_eth_dev_close returned non-zero"
            );
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
    /// `rte_flow_create` refused a steering rule: its return, the
    /// `rte_flow` error type, and the PMD's message (empty when it gave
    /// none).
    FlowRuleFailed {
        ret: i32,
        err_type: i32,
        message: String,
    },
    /// `rte_flow_destroy` refused to remove a steering rule: its return,
    /// the `rte_flow` error type, and the PMD's message (empty when it gave
    /// none). The rule is still installed.
    FlowRuleRemovalFailed {
        ret: i32,
        err_type: i32,
        message: String,
    },
    /// The port offers fewer queue pairs than its [`QueuePlan`] needs.
    TooFewQueues(TooFewQueues),
    /// The RSS redirection table could not be pinned to `queue`: the
    /// negative error code (`-ENOTSUP` when the port has no table).
    RetaPinFailed {
        queue: u16,
        ret: i32,
    },
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
            PortError::FlowRuleFailed {
                ret,
                err_type,
                message,
            } => write_flow_error(f, "rte_flow_create", *ret, *err_type, message),
            PortError::FlowRuleRemovalFailed {
                ret,
                err_type,
                message,
            } => write_flow_error(f, "rte_flow_destroy", *ret, *err_type, message),
            PortError::TooFewQueues(e) => write!(f, "{e}"),
            PortError::RetaPinFailed { queue, ret } => write!(
                f,
                "pinning the RSS redirection table to queue {queue} failed: {ret}"
            ),
        }
    }
}

impl std::error::Error for PortError {}

/// An `rte_flow` call's failure as [`PortError`] displays it: the call,
/// its return and error type, and the PMD's message when it gave one.
fn write_flow_error(
    f: &mut std::fmt::Formatter<'_>,
    call: &str,
    ret: i32,
    err_type: i32,
    message: &str,
) -> std::fmt::Result {
    write!(f, "{call} failed: ret={ret} err_type={err_type}")?;
    if !message.is_empty() {
        write!(f, " ({message})")?;
    }
    Ok(())
}
