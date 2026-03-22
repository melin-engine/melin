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

/// MTU for standard Ethernet (no jumbo frames).
const MTU: usize = 1500;

/// smoltcp device backed by a DPDK port.
pub struct DpdkDevice {
    port_id: u16,
    mempool: *mut ffi::rte_mempool,
    /// Staging buffer for received mbufs.
    rx_buf: [*mut ffi::rte_mbuf; BURST_SIZE],
    rx_count: usize,
    rx_cursor: usize,
    /// Hardware checksum offloads supported by the NIC.
    offloads: ChecksumOffloads,
    /// Cached TX offload flags (computed once at init, reused per packet).
    tx_ol_flags: u64,
    /// Injected frames to feed into smoltcp's RX path (e.g., crafted ARP
    /// replies to seed the neighbor cache on SR-IOV VFs that drop broadcast).
    inject_queue: Vec<Vec<u8>>,
    /// (source_mac, source_ip) pairs learned from incoming IPv4 Ethernet
    /// frames. Drained by the transport to seed smoltcp's neighbor cache
    /// via crafted ARP replies (workaround for SR-IOV VFs that drop
    /// broadcast ARP).
    learned_neighbors: Vec<([u8; 6], [u8; 4])>,
    /// Set of IPs we've already seeded to avoid repeated injections.
    known_neighbors: std::collections::HashSet<[u8; 4]>,
}

// SAFETY: DpdkDevice is only used from the single DPDK poll thread.
unsafe impl Send for DpdkDevice {}

impl DpdkDevice {
    /// Create a new device for the given DPDK port.
    pub fn new(port_id: u16, mempool: *mut ffi::rte_mempool, offloads: ChecksumOffloads) -> Self {
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

        DpdkDevice {
            port_id,
            mempool,
            rx_buf: [std::ptr::null_mut(); BURST_SIZE],
            rx_count: 0,
            rx_cursor: 0,
            offloads,
            tx_ol_flags,
            inject_queue: Vec::new(),
            learned_neighbors: Vec::new(),
            known_neighbors: std::collections::HashSet::new(),
        }
    }

    /// Poll the NIC for received packets.
    pub fn poll_rx(&mut self) {
        if self.rx_cursor < self.rx_count {
            return;
        }

        // SAFETY: port is started, rx_buf is correctly sized.
        let count = unsafe {
            ffi::dpdk_eth_rx_burst(self.port_id, 0, self.rx_buf.as_mut_ptr(), BURST_SIZE as u16)
        };

        self.rx_count = count as usize;
        self.rx_cursor = 0;
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
        caps.max_transmission_unit = MTU;
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
    type TxToken<'a> = DpdkTxToken;

    fn receive(&mut self, _timestamp: Instant) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        // Drain injected frames first (crafted ARP replies for neighbor
        // cache seeding). These are owned Vec<u8> buffers, not DPDK mbufs.
        if let Some(frame) = self.inject_queue.pop() {
            let rx_token = DpdkRxToken::Injected(frame);
            let tx_token = DpdkTxToken {
                port_id: self.port_id,
                mempool: self.mempool,
                tx_ol_flags: self.tx_ol_flags,
            };
            return Some((rx_token, tx_token));
        }

        if self.rx_cursor >= self.rx_count {
            return None;
        }

        let mbuf = self.rx_buf[self.rx_cursor];
        self.rx_cursor += 1;

        // Read packet data via C accessors (avoids direct struct field access
        // on bindgen-generated types with complex unions/bitfields).
        let (data_ptr, data_len) = unsafe {
            let buf_addr = ffi::dpdk_mbuf_buf_addr(mbuf).cast::<u8>();
            let data_off = ffi::dpdk_mbuf_data_off(mbuf) as usize;
            let ptr = buf_addr.add(data_off);
            let len = ffi::dpdk_mbuf_data_len(mbuf) as usize;
            (ptr, len)
        };

        // Learn source MAC+IP from incoming IPv4 frames to seed smoltcp's
        // neighbor cache. On SR-IOV VFs that drop broadcast ARP, this is the
        // only way smoltcp can learn peer MACs. Costs ~5ns per packet (two
        // cache-hot byte reads from the mbuf we're already touching).
        if data_len >= 34 {
            let data = unsafe { std::slice::from_raw_parts(data_ptr as *const u8, data_len) };
            // EtherType at offset 12: 0x0800 = IPv4
            if data[12] == 0x08 && data[13] == 0x00 {
                let mut src_mac = [0u8; 6];
                src_mac.copy_from_slice(&data[6..12]);
                let mut src_ip = [0u8; 4];
                src_ip.copy_from_slice(&data[26..30]);
                if !self.known_neighbors.contains(&src_ip) {
                    self.known_neighbors.insert(src_ip);
                    self.learned_neighbors.push((src_mac, src_ip));
                }
            }
        }

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
            port_id: self.port_id,
            mempool: self.mempool,
            tx_ol_flags: self.tx_ol_flags,
        };

        Some((rx_token, tx_token))
    }

    fn transmit(&mut self, _timestamp: Instant) -> Option<Self::TxToken<'_>> {
        Some(DpdkTxToken {
            port_id: self.port_id,
            mempool: self.mempool,
            tx_ol_flags: self.tx_ol_flags,
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

/// TX token: allocates an mbuf and sends one Ethernet frame.
pub struct DpdkTxToken {
    port_id: u16,
    mempool: *mut ffi::rte_mempool,
    /// Pre-computed TX offload flags (IPv4 + TCP checksum offload).
    tx_ol_flags: u64,
}

impl phy::TxToken for DpdkTxToken {
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
        }

        let mut tx_mbuf = mbuf;
        let sent = unsafe { ffi::dpdk_eth_tx_burst(self.port_id, 0, &mut tx_mbuf, 1) };
        if sent == 0 {
            unsafe {
                ffi::dpdk_pktmbuf_free(mbuf);
            }
            tracing::debug!("TX queue full, dropped packet");
        }

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
fn ipv4_pseudo_header_checksum(frame: &[u8]) -> u16 {
    // Build the 12-byte pseudo-header in memory, then sum as native u16s
    // (matching DPDK's rte_raw_cksum which uses memcpy into native u16).
    //
    //   [0..4]  src_ip  (from frame[26..30])
    //   [4..8]  dst_ip  (from frame[30..34])
    //   [8..10] zero + protocol (0x00, 0x06)
    //   [10..12] TCP segment length (big-endian)
    let tcp_len = (frame.len() - 34) as u16;
    let mut phdr = [0u8; 12];
    phdr[0..4].copy_from_slice(&frame[26..30]); // src_ip
    phdr[4..8].copy_from_slice(&frame[30..34]); // dst_ip
    phdr[8] = 0;
    phdr[9] = 6; // TCP protocol
    phdr[10..12].copy_from_slice(&tcp_len.to_be_bytes());

    // Sum as native-endian 16-bit words (same as DPDK's rte_raw_cksum).
    let mut sum: u32 = 0;
    for chunk in phdr.chunks_exact(2) {
        sum += u16::from_ne_bytes([chunk[0], chunk[1]]) as u32;
    }

    // Fold 32-bit sum to 16-bit.
    sum = (sum & 0xFFFF) + (sum >> 16);
    sum = (sum & 0xFFFF) + (sum >> 16);

    sum as u16
}
