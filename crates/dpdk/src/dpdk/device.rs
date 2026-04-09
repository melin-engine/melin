//! smoltcp `Device` implementation backed by DPDK `rx_burst`/`tx_burst`.
//!
//! This is the bridge between the userspace TCP stack (smoltcp) and the
//! NIC driver (DPDK). smoltcp calls `receive()` to get inbound Ethernet
//! frames and `transmit()` to send outbound frames. We translate these
//! into DPDK mbuf operations via C wrapper functions (see inline_wrappers.c).
//!
//! The device is single-threaded — it's called from the DPDK poll thread
//! only. No synchronization needed.

use smoltcp::phy::{self, Checksum, ChecksumCapabilities, Device, DeviceCapabilities, Medium};
use smoltcp::time::Instant;

use crate::ffi;
use crate::port::ChecksumOffloads;

/// Maximum burst size for rx_burst / tx_burst.
/// 32 is the typical sweet spot: amortizes per-call overhead without
/// adding excessive latency from batch processing.
const BURST_SIZE: usize = 32;

/// Default MTU for standard Ethernet. Override with `DpdkDevice::set_mtu()`
/// for jumbo frames (9000) which reduce TCP segment count ~6x.
const DEFAULT_MTU: usize = 1500;

/// Per-port RX state for multi-port polling.
struct RxPort {
    port_id: u16,
    /// NIC queue index for this port. With RSS, each poll thread reads
    /// from a different queue on the same port.
    queue_id: u16,
    /// Staging buffer for received mbufs.
    rx_buf: [*mut ffi::rte_mbuf; BURST_SIZE],
    rx_count: usize,
    rx_cursor: usize,
}

/// smoltcp device backed by one or more DPDK ports.
///
/// RX polls all ports (for LACP bonds where the switch may hash traffic
/// to either bond member's VF). TX always goes through the first port —
/// the switch/bond handles egress distribution.
pub struct DpdkDevice {
    /// Per-port RX state. One entry per DPDK port.
    rx_ports: Vec<RxPort>,
    /// Index into `rx_ports` currently being drained.
    active_rx: usize,
    /// Port used for all TX (first port in the list).
    tx_port_id: u16,
    /// TX queue index. With RSS, each poll thread writes to its own
    /// TX queue to avoid contention.
    tx_queue_id: u16,
    mempool: *mut ffi::rte_mempool,
    /// MTU (Maximum Transmission Unit). 1500 for standard Ethernet,
    /// 9000 for jumbo frames (6x fewer TCP segments).
    mtu: usize,
    /// Hardware checksum offloads supported by the NIC (intersection
    /// of all ports' capabilities).
    offloads: ChecksumOffloads,
    /// Cached TX offload flags (computed once at init, reused per packet).
    tx_ol_flags: u64,
    /// VLAN ID for TX insert offload. 0 = no VLAN tagging.
    tx_vlan_id: u16,
    /// Injected frames to feed into smoltcp's RX path (e.g., crafted ARP
    /// replies to seed the neighbor cache on SR-IOV VFs that drop broadcast).
    inject_queue: Vec<Vec<u8>>,
    /// (source_mac, source_ip) pairs learned from incoming IPv4 Ethernet
    /// frames. Drained by the transport to seed smoltcp's neighbor cache
    /// via crafted ARP replies (workaround for SR-IOV VFs that drop
    /// broadcast ARP).
    learned_neighbors: Vec<([u8; 6], [u8; 4])>,
    /// IP → last-seeded timestamp. Only re-seed a neighbor after a cooldown
    /// period to avoid flooding the injected frame queue on every packet from
    /// a known peer. Cooldown must be shorter than smoltcp's neighbor cache
    /// expiry (~60s) to prevent stale entries.
    ///
    /// O(1) lookup — checked on every IPv4 packet in collect_rx_batch,
    /// so lookup cost matters at high packet rates.
    known_neighbors: std::collections::HashMap<[u8; 4], std::time::Instant>,
    /// Reusable mbuf buffer for collect_rx_batch() to avoid per-poll allocation.
    batch_mbufs: Vec<*mut ffi::rte_mbuf>,
    /// Reusable injected-frames buffer for collect_rx_batch().
    batch_injected: Vec<Vec<u8>>,
    /// Pending TX mbufs accumulated during smoltcp poll. Flushed in a
    /// single `tx_burst(N)` call via `flush_tx()` after each poll cycle.
    tx_batch: Vec<*mut ffi::rte_mbuf>,
}

// SAFETY: DpdkDevice is only used from the single DPDK poll thread.
unsafe impl Send for DpdkDevice {}

impl DpdkDevice {
    /// Create a new device backed by one or more DPDK ports.
    ///
    /// `port_ids` lists all ports to poll for RX. The first port is also
    /// used for TX. For LACP bonds, pass both VF port IDs so traffic
    /// arriving on either bond member is received.
    /// `queue_id` selects which RX/TX queue pair this device uses on
    /// each port. With RSS, each poll thread gets a different queue_id.
    pub fn new(
        port_ids: &[u16],
        mempool: *mut ffi::rte_mempool,
        offloads: ChecksumOffloads,
        queue_id: u16,
    ) -> Self {
        assert!(!port_ids.is_empty(), "at least one DPDK port required");

        // Pre-compute TX offload flags once — these are the same for every
        // outbound IPv4/TCP packet.
        let mut tx_ol_flags: u64 = 0;
        if offloads.tx_ip {
            tx_ol_flags |= unsafe { ffi::dpdk_tx_offload_ipv4_cksum() };
        }
        if offloads.tx_tcp {
            tx_ol_flags |= unsafe { ffi::dpdk_tx_offload_tcp_cksum() };
        }
        if tx_ol_flags != 0 {
            tracing::info!("DPDK TX checksum offload enabled (flags=0x{tx_ol_flags:x})");
        }

        let rx_ports = port_ids
            .iter()
            .map(|&port_id| RxPort {
                port_id,
                queue_id,
                rx_buf: [std::ptr::null_mut(); BURST_SIZE],
                rx_count: 0,
                rx_cursor: 0,
            })
            .collect();

        DpdkDevice {
            rx_ports,
            active_rx: 0,
            tx_port_id: port_ids[0],
            tx_queue_id: queue_id,
            mempool,
            mtu: DEFAULT_MTU,
            offloads,
            tx_ol_flags,
            tx_vlan_id: 0,
            inject_queue: Vec::new(),
            learned_neighbors: Vec::new(),
            known_neighbors: std::collections::HashMap::with_capacity(64),
            batch_mbufs: Vec::with_capacity(BURST_SIZE * port_ids.len()),
            batch_injected: Vec::new(),
            tx_batch: Vec::with_capacity(BURST_SIZE),
        }
    }

    /// Flush all pending TX mbufs in a single `tx_burst(N)` call.
    /// Call after each `iface.poll()` cycle to batch outgoing packets.
    pub fn flush_tx(&mut self) {
        if self.tx_batch.is_empty() {
            return;
        }
        let count = self.tx_batch.len();
        let sent = unsafe {
            ffi::dpdk_eth_tx_burst(
                self.tx_port_id,
                self.tx_queue_id,
                self.tx_batch.as_mut_ptr(),
                count as u16,
            )
        } as usize;
        // Free any unsent mbufs (TX queue full).
        for mbuf in &self.tx_batch[sent..] {
            unsafe {
                ffi::dpdk_pktmbuf_free(*mbuf);
            }
        }
        if sent < count {
            tracing::debug!(sent, total = count, "TX burst partial — queue full");
        }
        self.tx_batch.clear();
    }

    /// Poll all ports for received packets.
    ///
    /// If the current port's buffer is exhausted, tries each port starting
    /// from the current one. With LACP, this ensures traffic arriving on
    /// either bond member's VF is received.
    pub fn poll_rx(&mut self) {
        // Still draining current burst — nothing to do.
        let active = &self.rx_ports[self.active_rx];
        if active.rx_cursor < active.rx_count {
            return;
        }

        // Try each port, starting from the current one.
        let n = self.rx_ports.len();
        for i in 0..n {
            let idx = (self.active_rx + i) % n;
            let port = &mut self.rx_ports[idx];

            // SAFETY: port is started, rx_buf is correctly sized.
            let count = unsafe {
                ffi::dpdk_eth_rx_burst(
                    port.port_id,
                    port.queue_id,
                    port.rx_buf.as_mut_ptr(),
                    BURST_SIZE as u16,
                )
            };

            if count > 0 {
                port.rx_count = count as usize;
                port.rx_cursor = 0;
                self.active_rx = idx;
                return;
            }
        }
    }

    /// Set the MTU. Call before creating the smoltcp Interface so that
    /// capabilities() reports the correct value. Use 9000 for jumbo frames.
    pub fn set_mtu(&mut self, mtu: usize) {
        self.mtu = mtu;
    }

    /// Collect all pending frames for batch ingress processing.
    ///
    /// Polls all ports via `rx_burst`, performs MAC learning, and drains
    /// injected frames (ARP replies for neighbor seeding). Returns an
    /// `RxBatch` that owns the mbufs (freed on drop) and provides frame
    /// data access.
    ///
    /// After this call, `Device::receive()` returns `None` — the batch
    /// owns all received frames. Call `iface.poll()` after processing
    /// the batch for egress and maintenance (ingress will be a no-op).
    pub fn collect_rx_batch(&mut self) -> RxBatch {
        // Reuse pre-allocated buffers to avoid per-poll heap allocation.
        let mut mbufs = std::mem::take(&mut self.batch_mbufs);
        mbufs.clear();

        let now = std::time::Instant::now();

        for port in &mut self.rx_ports {
            // SAFETY: port is started, rx_buf is correctly sized.
            let count = unsafe {
                ffi::dpdk_eth_rx_burst(
                    port.port_id,
                    port.queue_id,
                    port.rx_buf.as_mut_ptr(),
                    BURST_SIZE as u16,
                )
            };

            for i in 0..count as usize {
                let mbuf = port.rx_buf[i];

                // MAC learning (same as Device::receive path).
                let (data_ptr, data_len) = unsafe {
                    let buf_addr = ffi::dpdk_mbuf_buf_addr(mbuf).cast::<u8>();
                    let data_off = ffi::dpdk_mbuf_data_off(mbuf) as usize;
                    (
                        buf_addr.add(data_off),
                        ffi::dpdk_mbuf_data_len(mbuf) as usize,
                    )
                };
                if data_len >= 34 {
                    let data = unsafe { std::slice::from_raw_parts(data_ptr, data_len) };
                    if data[12] == 0x08 && data[13] == 0x00 {
                        let mut src_mac = [0u8; 6];
                        src_mac.copy_from_slice(&data[6..12]);
                        let mut src_ip = [0u8; 4];
                        src_ip.copy_from_slice(&data[26..30]);
                        // Re-seed every 30s — must be shorter than smoltcp's
                        // neighbor cache expiry (~60s) but long enough to
                        // avoid injecting ARP replies on every packet.
                        const RESEED_SECS: u64 = 30;
                        let needs_seed = match self.known_neighbors.get_mut(&src_ip) {
                            Some(last) => {
                                if now.duration_since(*last).as_secs() >= RESEED_SECS {
                                    *last = now;
                                    true
                                } else {
                                    false
                                }
                            }
                            None => {
                                self.known_neighbors.insert(src_ip, now);
                                true
                            }
                        };
                        if needs_seed {
                            self.learned_neighbors.push((src_mac, src_ip));
                        }
                    }
                }

                mbufs.push(mbuf);
            }

            // Reset port rx state so Device::receive() returns None.
            port.rx_count = 0;
            port.rx_cursor = 0;
        }

        let injected = std::mem::take(&mut self.inject_queue);

        RxBatch { mbufs, injected }
    }

    /// Set the VLAN ID for TX insert offload. When set, every outgoing
    /// frame gets a VLAN tag inserted by the NIC. Used in dedicated NIC
    /// mode where the kernel isn't handling VLAN tags.
    pub fn set_vlan_id(&mut self, vlan_id: u16) {
        self.tx_vlan_id = vlan_id;
        // Add TX_VLAN flag to the pre-computed offload flags.
        self.tx_ol_flags |= unsafe { ffi::dpdk_tx_vlan_flag() };
        tracing::info!(vlan_id, "DPDK TX VLAN insert enabled");
    }

    /// Send a raw Ethernet frame out the NIC, bypassing smoltcp.
    ///
    /// Used for gratuitous ARP on startup (switch MAC learning) and other
    /// control frames that aren't part of a TCP connection.
    pub fn send_raw_frame(&mut self, frame: &[u8]) {
        let mbuf = unsafe { ffi::dpdk_pktmbuf_alloc(self.mempool) };
        assert!(!mbuf.is_null(), "mbuf alloc failed for raw frame TX");
        unsafe {
            let buf_addr = ffi::dpdk_mbuf_buf_addr(mbuf).cast::<u8>();
            let data_off = ffi::dpdk_mbuf_data_off(mbuf) as usize;
            let data_ptr = buf_addr.add(data_off);
            std::ptr::copy_nonoverlapping(frame.as_ptr(), data_ptr, frame.len());
            ffi::dpdk_mbuf_set_data_len(mbuf, frame.len() as u16);
            ffi::dpdk_mbuf_set_pkt_len(mbuf, frame.len() as u32);

            // Set VLAN tag if configured.
            if self.tx_vlan_id != 0 {
                ffi::dpdk_mbuf_set_ol_flags(mbuf, ffi::dpdk_tx_vlan_flag());
                ffi::dpdk_mbuf_set_vlan_tci(mbuf, self.tx_vlan_id);
            }
        }
        self.tx_batch.push(mbuf);
        self.flush_tx();
    }

    /// Inject a raw Ethernet frame into smoltcp's RX path.
    /// Used to seed the neighbor cache with crafted ARP replies on SR-IOV
    /// VFs that can't receive broadcast ARP.
    pub fn inject_rx(&mut self, frame: Vec<u8>) {
        self.inject_queue.push(frame);
    }

    /// Learned (source_mac, source_ip) pairs from incoming IPv4 frames.
    /// Drained by the transport to seed smoltcp's neighbor cache.
    pub fn take_learned_neighbors(&mut self) -> Vec<([u8; 6], [u8; 4])> {
        std::mem::take(&mut self.learned_neighbors)
    }

    /// Capabilities accessor for use by DpdkDeviceRef.
    pub fn capabilities(&self) -> DeviceCapabilities {
        let mut caps = DeviceCapabilities::default();
        caps.medium = Medium::Ethernet;
        caps.max_transmission_unit = self.mtu;
        caps.max_burst_size = Some(BURST_SIZE);

        // Tell smoltcp which checksums the NIC handles in hardware.
        // `Checksum::None` means "don't compute or verify" — the NIC does it.
        let mut checksums = ChecksumCapabilities::default();
        if self.offloads.rx_ip && self.offloads.tx_ip {
            checksums.ipv4 = Checksum::None;
        } else if self.offloads.tx_ip {
            checksums.ipv4 = Checksum::Rx; // verify on RX only
        } else if self.offloads.rx_ip {
            checksums.ipv4 = Checksum::Tx; // compute on TX only
        }
        if self.offloads.rx_tcp && self.offloads.tx_tcp {
            checksums.tcp = Checksum::None;
        } else if self.offloads.tx_tcp {
            checksums.tcp = Checksum::Rx;
        } else if self.offloads.rx_tcp {
            checksums.tcp = Checksum::Tx;
        }
        caps.checksum = checksums;

        caps
    }
}

impl Device for DpdkDevice {
    type RxToken<'a> = DpdkRxToken;
    type TxToken<'a> = DpdkTxToken<'a>;

    fn receive(&mut self, _timestamp: Instant) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        // Drain injected frames first (crafted ARP replies for neighbor
        // cache seeding). These are owned Vec<u8> buffers, not DPDK mbufs.
        if let Some(frame) = self.inject_queue.pop() {
            let rx_token = DpdkRxToken::Injected(frame);
            let tx_token = DpdkTxToken {
                mempool: self.mempool,
                tx_ol_flags: self.tx_ol_flags,
                tx_vlan_id: self.tx_vlan_id,
                tx_batch: &mut self.tx_batch,
            };
            return Some((rx_token, tx_token));
        }

        let active = &mut self.rx_ports[self.active_rx];
        if active.rx_cursor >= active.rx_count {
            return None;
        }

        let mbuf = active.rx_buf[active.rx_cursor];
        active.rx_cursor += 1;

        // Read packet data via C accessors (avoids direct struct field access
        // on bindgen-generated types with complex unions/bitfields).
        let (data_ptr, data_len) = unsafe {
            let buf_addr = ffi::dpdk_mbuf_buf_addr(mbuf).cast::<u8>();
            let data_off = ffi::dpdk_mbuf_data_off(mbuf) as usize;
            let ptr = buf_addr.add(data_off);
            let len = ffi::dpdk_mbuf_data_len(mbuf) as usize;
            (ptr, len)
        };

        // MAC learning is handled in collect_rx_batch() which runs before
        // poll_ingress_batch(). This Device::receive() path only fires for
        // smoltcp-internal egress (e.g., ARP responses) — no need to learn
        // MACs from our own outbound frames.

        // Pass the mbuf directly to the RxToken. The token holds the raw
        // pointer and frees it after smoltcp consumes the packet data.
        // This avoids any copy or allocation — smoltcp reads directly
        // from DPDK hugepage memory.
        let rx_token = DpdkRxToken::Mbuf {
            mbuf,
            data_ptr: data_ptr as *const u8,
            data_len,
        };
        let tx_token = DpdkTxToken {
            mempool: self.mempool,
            tx_ol_flags: self.tx_ol_flags,
            tx_vlan_id: self.tx_vlan_id,
            tx_batch: &mut self.tx_batch,
        };

        Some((rx_token, tx_token))
    }

    fn transmit(&mut self, _timestamp: Instant) -> Option<Self::TxToken<'_>> {
        Some(DpdkTxToken {
            mempool: self.mempool,
            tx_ol_flags: self.tx_ol_flags,
            tx_vlan_id: self.tx_vlan_id,
            tx_batch: &mut self.tx_batch,
        })
    }

    fn capabilities(&self) -> DeviceCapabilities {
        self.capabilities()
    }
}

/// RX token: holds one received Ethernet frame.
///
/// Two variants:
/// - `Mbuf`: zero-copy from DPDK hugepage memory. The mbuf is freed after consume.
/// - `Injected`: owned buffer for crafted frames (e.g., ARP replies to seed
///   the neighbor cache on SR-IOV VFs that can't receive broadcast ARP).
pub enum DpdkRxToken {
    /// Zero-copy: smoltcp reads directly from hugepage-backed mbuf memory.
    Mbuf {
        mbuf: *mut ffi::rte_mbuf,
        data_ptr: *const u8,
        data_len: usize,
    },
    /// Injected frame (owned buffer, no DPDK mbuf).
    Injected(Vec<u8>),
}

impl phy::RxToken for DpdkRxToken {
    fn consume<R, F>(self, f: F) -> R
    where
        F: FnOnce(&[u8]) -> R,
    {
        match self {
            DpdkRxToken::Mbuf {
                mbuf,
                data_ptr,
                data_len,
            } => {
                // SAFETY: data_ptr points into the mbuf's data area which remains
                // valid until rte_pktmbuf_free is called. We call f() first, then free.
                let data = unsafe { std::slice::from_raw_parts(data_ptr, data_len) };
                let result = f(data);
                unsafe {
                    ffi::dpdk_pktmbuf_free(mbuf);
                }
                result
            }
            DpdkRxToken::Injected(ref frame) => f(frame),
        }
    }
}

/// TX token: allocates an mbuf and queues it for batched transmission.
pub struct DpdkTxToken<'a> {
    mempool: *mut ffi::rte_mempool,
    /// Pre-computed TX offload flags (IPv4 + TCP checksum + VLAN insert).
    tx_ol_flags: u64,
    /// VLAN ID for TX insert. 0 = no VLAN tagging.
    tx_vlan_id: u16,
    /// Batch buffer — mbufs are pushed here and flushed via `flush_tx()`.
    tx_batch: &'a mut Vec<*mut ffi::rte_mbuf>,
}

impl<'a> phy::TxToken for DpdkTxToken<'a> {
    fn consume<R, F>(self, len: usize, f: F) -> R
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        let mbuf = unsafe { ffi::dpdk_pktmbuf_alloc(self.mempool) };
        assert!(!mbuf.is_null(), "mbuf alloc failed — mempool exhausted");

        // Get mutable slice via C accessors. Cast from *mut c_void to
        // *mut u8 (dpdk_mbuf_buf_addr returns void*).
        let data_ptr = unsafe {
            let buf_addr = ffi::dpdk_mbuf_buf_addr(mbuf).cast::<u8>();
            let data_off = ffi::dpdk_mbuf_data_off(mbuf) as usize;
            buf_addr.add(data_off)
        };
        let buf = unsafe { std::slice::from_raw_parts_mut(data_ptr, len) };

        let result = f(buf);

        // Set packet length via C accessors.
        unsafe {
            ffi::dpdk_mbuf_set_data_len(mbuf, len as u16);
            ffi::dpdk_mbuf_set_pkt_len(mbuf, len as u32);

            // Hardware checksum offload for IPv4/TCP packets only.
            //
            // DPDK TX checksum offload requires:
            //   - ol_flags set to indicate which checksums to offload
            //   - l2_len/l3_len set so the NIC can locate headers
            //   - TCP pseudo-header checksum pre-filled in the TCP
            //     checksum field (the NIC adds the data portion on top)
            //
            // We only set offload flags on IPv4+TCP frames. ARP and other
            // non-IP frames must NOT have offload flags set.
            //
            // Frame layout (smoltcp, no VLAN/IP options):
            //   [0..14]  Ethernet header (EtherType at 12..14)
            //   [14..34] IPv4 header (protocol at 23)
            //   [34..]   TCP header + payload (checksum at 50..52)
            if self.tx_ol_flags != 0
                && len >= 54
                && *data_ptr.add(12) == 0x08
                && *data_ptr.add(13) == 0x00  // EtherType: IPv4
                && *data_ptr.add(23) == 6
            // Protocol: TCP
            {
                // Compute TCP pseudo-header checksum and write it into
                // the TCP checksum field (offset 50). The NIC adds the
                // TCP header+payload checksum on top.
                let phdr_cksum =
                    ipv4_pseudo_header_checksum(std::slice::from_raw_parts(data_ptr, len));
                // Write in native byte order — DPDK/NIC expects the
                // pseudo-header checksum as a native-endian u16.
                let cksum_bytes = phdr_cksum.to_ne_bytes();
                *data_ptr.add(50) = cksum_bytes[0];
                *data_ptr.add(51) = cksum_bytes[1];

                ffi::dpdk_mbuf_set_ol_flags(mbuf, self.tx_ol_flags);
                ffi::dpdk_mbuf_set_tx_offload(mbuf, 14, 20, 0);
            }

            // VLAN insert: set TCI on every outgoing frame (not just TCP).
            // The ol_flags TX_VLAN bit is already in tx_ol_flags if vlan_id != 0.
            if self.tx_vlan_id != 0 {
                // Ensure TX_VLAN flag is set even for non-TCP frames (ARP etc.)
                // that skipped the checksum block above.
                let current_flags = ffi::dpdk_mbuf_ol_flags(mbuf);
                if current_flags & ffi::dpdk_tx_vlan_flag() == 0 {
                    ffi::dpdk_mbuf_set_ol_flags(mbuf, current_flags | ffi::dpdk_tx_vlan_flag());
                }
                ffi::dpdk_mbuf_set_vlan_tci(mbuf, self.tx_vlan_id);
            }
        }

        // Queue for batched transmission — flushed via flush_tx().
        self.tx_batch.push(mbuf);

        result
    }
}

/// Compute the IPv4 TCP pseudo-header checksum matching DPDK's convention
/// (`rte_ipv4_phdr_cksum`): sum 16-bit words in native byte order, fold,
/// return non-complemented.
///
/// The result is written directly into the TCP checksum field (bytes 50..52
/// of the Ethernet frame) in native byte order. The NIC adds the TCP
/// header+payload checksum on top and complements to produce the final value.
///
/// `frame` is a complete Ethernet frame (14-byte Ethernet + IPv4 + TCP).
/// Reads directly from the frame buffer — no intermediate copy.
#[inline(always)]
fn ipv4_pseudo_header_checksum(frame: &[u8]) -> u16 {
    // Sum the pseudo-header fields directly from the frame as native-endian
    // u16 words (matching DPDK's rte_raw_cksum convention).
    //
    // Pseudo-header: src_ip(4) + dst_ip(4) + zero_proto(2) + tcp_len(2)
    // = 6 native-endian u16 additions.
    let tcp_len = (frame.len() - 34) as u16;

    // Read src_ip (frame[26..30]) and dst_ip (frame[30..34]) as 4 native u16s.
    let sum: u32 = u16::from_ne_bytes([frame[26], frame[27]]) as u32
        + u16::from_ne_bytes([frame[28], frame[29]]) as u32
        + u16::from_ne_bytes([frame[30], frame[31]]) as u32
        + u16::from_ne_bytes([frame[32], frame[33]]) as u32
        + u16::from_ne_bytes([0, 6]) as u32 // zero + protocol (TCP=6)
        + u16::from_ne_bytes(tcp_len.to_be_bytes()) as u32;

    // Fold 32-bit sum to 16-bit.
    let folded = (sum & 0xFFFF) + (sum >> 16);
    ((folded & 0xFFFF) + (folded >> 16)) as u16
}

/// Batch of received frames from `DpdkDevice::collect_rx_batch()`.
///
/// Holds raw mbuf pointers (freed on drop) and injected frames. Frame
/// data remains valid until the batch is dropped — callers can build
/// `&[&[u8]]` slices for `Interface::poll_ingress_batch()`.
pub struct RxBatch {
    /// NIC mbufs — data lives in hugepage memory until drop frees them.
    mbufs: Vec<*mut ffi::rte_mbuf>,
    /// Injected frames (ARP replies for neighbor seeding). Owned buffers.
    injected: Vec<Vec<u8>>,
}

// SAFETY: RxBatch is only used from the single DPDK poll thread.
unsafe impl Send for RxBatch {}

impl RxBatch {
    /// Total number of frames (NIC + injected).
    pub fn len(&self) -> usize {
        self.mbufs.len() + self.injected.len()
    }

    pub fn is_empty(&self) -> bool {
        self.mbufs.is_empty() && self.injected.is_empty()
    }

    /// Drain any newly injected frames from the device into this batch.
    /// Used to include ARP replies seeded after `collect_rx_batch()` but
    /// before `poll_ingress_batch()`, so smoltcp learns the neighbor
    /// before processing the SYN in the same batch.
    pub fn append_injected(&mut self, device: &mut DpdkDevice) {
        self.injected.append(&mut device.inject_queue);
    }

    /// Write frame slices into a caller-provided `MaybeUninit` array.
    /// Returns the number of slices written. Injected frames first (ARP),
    /// then NIC frames. Zero heap allocation.
    ///
    /// # Safety
    /// The caller must ensure `out` has at least `self.len()` elements.
    pub fn write_slices<'a>(&'a self, out: &mut [std::mem::MaybeUninit<&'a [u8]>]) -> usize {
        let mut i = 0;
        for frame in &self.injected {
            out[i] = std::mem::MaybeUninit::new(frame.as_slice());
            i += 1;
        }
        for &mbuf in &self.mbufs {
            // SAFETY: mbuf data is valid until drop/recycle frees it.
            let data = unsafe {
                let buf_addr = ffi::dpdk_mbuf_buf_addr(mbuf).cast::<u8>();
                let data_off = ffi::dpdk_mbuf_data_off(mbuf) as usize;
                let ptr = buf_addr.add(data_off);
                let len = ffi::dpdk_mbuf_data_len(mbuf) as usize;
                std::slice::from_raw_parts(ptr, len)
            };
            out[i] = std::mem::MaybeUninit::new(data);
            i += 1;
        }
        i
    }
}

impl RxBatch {
    /// Free mbufs and return the reusable Vec buffers to the device.
    /// Must be called instead of dropping to avoid per-poll allocation.
    pub fn recycle(mut self, device: &mut DpdkDevice) {
        for &mbuf in &self.mbufs {
            unsafe {
                ffi::dpdk_pktmbuf_free(mbuf);
            }
        }
        let mut mbufs = std::mem::take(&mut self.mbufs);
        mbufs.clear();
        device.batch_mbufs = mbufs;
        let mut injected = std::mem::take(&mut self.injected);
        injected.clear();
        device.batch_injected = injected;
        // Drop runs but mbufs/injected are now empty Vecs — no double-free.
    }
}

impl Drop for RxBatch {
    fn drop(&mut self) {
        // Fallback if recycle() wasn't called (e.g., panic unwinding).
        for &mbuf in &self.mbufs {
            unsafe {
                ffi::dpdk_pktmbuf_free(mbuf);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rx_batch_empty() {
        let batch = RxBatch {
            mbufs: Vec::new(),
            injected: Vec::new(),
        };
        assert!(batch.is_empty());
        assert_eq!(batch.len(), 0);
        assert!(batch.as_slices().is_empty());
    }

    #[test]
    fn rx_batch_injected_only() {
        let arp_frame = vec![0xFFu8; 42];
        let tcp_frame = vec![0xAAu8; 60];
        let batch = RxBatch {
            mbufs: Vec::new(),
            injected: vec![arp_frame.clone(), tcp_frame.clone()],
        };
        assert!(!batch.is_empty());
        assert_eq!(batch.len(), 2);
        let slices = batch.as_slices();
        assert_eq!(slices.len(), 2);
        assert_eq!(slices[0], &arp_frame[..]);
        assert_eq!(slices[1], &tcp_frame[..]);
    }

    #[test]
    fn rx_batch_injected_ordering() {
        // Injected frames must come before NIC frames in the slice array
        // so ARP replies seed the neighbor cache before TCP SYNs.
        let batch = RxBatch {
            mbufs: Vec::new(),
            injected: vec![vec![1, 2, 3], vec![4, 5, 6]],
        };
        let slices = batch.as_slices();
        assert_eq!(slices[0], &[1, 2, 3]);
        assert_eq!(slices[1], &[4, 5, 6]);
    }
}
