//! smoltcp `Device` implementation backed by DPDK `rx_burst`/`tx_burst`.
//!
//! This is the bridge between the userspace TCP stack (smoltcp) and the
//! NIC driver (DPDK). smoltcp calls `receive()` to get inbound Ethernet
//! frames and `transmit()` to send outbound frames. We translate these
//! into DPDK mbuf operations via C wrapper functions (see inline_wrappers.c).
//!
//! The device is single-threaded — it's called from the DPDK poll thread
//! only. No synchronization needed.

use smoltcp::phy::{self, Device, DeviceCapabilities, Medium};
use smoltcp::time::Instant;

use crate::ffi;

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
}

// SAFETY: DpdkDevice is only used from the single DPDK poll thread.
unsafe impl Send for DpdkDevice {}

impl DpdkDevice {
    /// Create a new device for the given DPDK port.
    pub fn new(port_id: u16, mempool: *mut ffi::rte_mempool) -> Self {
        DpdkDevice {
            port_id,
            mempool,
            rx_buf: [std::ptr::null_mut(); BURST_SIZE],
            rx_count: 0,
            rx_cursor: 0,
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

    /// Capabilities accessor for use by DpdkDeviceRef.
    pub fn capabilities(&self) -> DeviceCapabilities {
        let mut caps = DeviceCapabilities::default();
        caps.medium = Medium::Ethernet;
        caps.max_transmission_unit = MTU;
        caps.max_burst_size = Some(BURST_SIZE);
        caps
    }
}

impl Device for DpdkDevice {
    type RxToken<'a> = DpdkRxToken;
    type TxToken<'a> = DpdkTxToken;

    fn receive(&mut self, _timestamp: Instant) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        if self.rx_cursor >= self.rx_count {
            return None;
        }

        let mbuf = self.rx_buf[self.rx_cursor];
        self.rx_cursor += 1;

        // Read packet data via C accessors (avoids direct struct field access
        // on bindgen-generated types with complex unions/bitfields).
        let (data_ptr, data_len) = unsafe {
            let buf_addr = ffi::dpdk_mbuf_buf_addr(mbuf);
            let data_off = ffi::dpdk_mbuf_data_off(mbuf) as usize;
            let ptr = buf_addr.add(data_off);
            let len = ffi::dpdk_mbuf_data_len(mbuf) as usize;
            (ptr, len)
        };

        // Copy packet data. smoltcp's RxToken takes ownership via closure,
        // but the mbuf must be freed back to the pool after consumption.
        let mut buf = vec![0u8; data_len];
        unsafe {
            std::ptr::copy_nonoverlapping(data_ptr as *const u8, buf.as_mut_ptr(), data_len);
            ffi::dpdk_pktmbuf_free(mbuf);
        }

        let rx_token = DpdkRxToken { buf };
        let tx_token = DpdkTxToken {
            port_id: self.port_id,
            mempool: self.mempool,
        };

        Some((rx_token, tx_token))
    }

    fn transmit(&mut self, _timestamp: Instant) -> Option<Self::TxToken<'_>> {
        Some(DpdkTxToken {
            port_id: self.port_id,
            mempool: self.mempool,
        })
    }

    fn capabilities(&self) -> DeviceCapabilities {
        self.capabilities()
    }
}

/// RX token: holds one received Ethernet frame.
pub struct DpdkRxToken {
    buf: Vec<u8>,
}

impl phy::RxToken for DpdkRxToken {
    fn consume<R, F>(self, f: F) -> R
    where
        F: FnOnce(&[u8]) -> R,
    {
        f(&self.buf)
    }
}

/// TX token: allocates an mbuf and sends one Ethernet frame.
pub struct DpdkTxToken {
    port_id: u16,
    mempool: *mut ffi::rte_mempool,
}

impl phy::TxToken for DpdkTxToken {
    fn consume<R, F>(self, len: usize, f: F) -> R
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        let mbuf = unsafe { ffi::dpdk_pktmbuf_alloc(self.mempool) };
        assert!(!mbuf.is_null(), "mbuf alloc failed — mempool exhausted");

        // Get mutable slice via C accessors.
        let data_ptr = unsafe {
            let buf_addr = ffi::dpdk_mbuf_buf_addr(mbuf);
            let data_off = ffi::dpdk_mbuf_data_off(mbuf) as usize;
            buf_addr.add(data_off)
        };
        let buf = unsafe { std::slice::from_raw_parts_mut(data_ptr, len) };

        let result = f(buf);

        // Set packet length via C accessors.
        unsafe {
            ffi::dpdk_mbuf_set_data_len(mbuf, len as u16);
            ffi::dpdk_mbuf_set_pkt_len(mbuf, len as u32);
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
