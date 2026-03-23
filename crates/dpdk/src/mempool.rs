//! DPDK packet memory pool (mempool) for zero-copy mbuf management.
//!
//! A mempool is a fixed-size pool of `rte_mbuf` objects allocated from
//! hugepage memory. Each mbuf wraps a contiguous data buffer for one
//! packet. Mbufs are allocated before `rx_burst` and freed after
//! `tx_burst` (or after the application is done with the packet).
//!
//! Using a pre-allocated pool avoids per-packet heap allocation — critical
//! for the ~100ns/order latency budget.

use crate::ffi;

/// Default number of mbufs in the pool. Must be > (RX_DESC + TX_DESC) per
/// port × number of ports, plus headroom for in-flight packets in smoltcp.
/// 8192 is conservative for a single-port, single-queue setup.
const DEFAULT_NUM_MBUFS: u32 = 8192;

/// Per-mbuf cache size. DPDK maintains a per-lcore cache to avoid
/// contention on the pool's ring. 256 is the typical sweet spot.
const MBUF_CACHE_SIZE: u32 = 256;

/// Wrapper around a DPDK `rte_mempool`. Frees the pool on drop.
pub struct Mempool {
    raw: *mut ffi::rte_mempool,
}

// Mempool is allocated from shared hugepage memory and accessed by the
// NIC DMA engine + our poll thread. Send is safe because we only access
// it from the DPDK poll thread after initialization.
unsafe impl Send for Mempool {}

impl Mempool {
    /// Create a new packet mempool on the given NUMA socket.
    ///
    /// `socket_id` should match the NIC's NUMA node for optimal DMA
    /// locality (avoids cross-socket memory access on multi-socket systems).
    pub fn create(name: &str, socket_id: i32) -> Result<Self, MempoolError> {
        Self::create_with_size(name, DEFAULT_NUM_MBUFS, socket_id)
    }

    /// Create a mempool with a specific number of mbufs.
    pub fn create_with_size(
        name: &str,
        num_mbufs: u32,
        socket_id: i32,
    ) -> Result<Self, MempoolError> {
        let buf_size = unsafe { ffi::dpdk_mbuf_default_buf_size() };
        Self::create_full(name, num_mbufs, buf_size, socket_id)
    }

    /// Create a mempool for jumbo frames. `mtu` is the desired MTU (e.g.,
    /// 9000). The mbuf data room is sized to hold one full frame plus
    /// the RTE_PKTMBUF_HEADROOM (128 bytes).
    pub fn create_for_mtu(
        name: &str,
        num_mbufs: u32,
        mtu: u16,
        socket_id: i32,
    ) -> Result<Self, MempoolError> {
        // Ethernet frame = 14 (header) + MTU + 4 (FCS, may be stripped).
        // Add 128 bytes for RTE_PKTMBUF_HEADROOM.
        let buf_size = mtu + 14 + 4 + 128;
        Self::create_full(name, num_mbufs, buf_size, socket_id)
    }

    fn create_full(
        name: &str,
        num_mbufs: u32,
        buf_size: u16,
        socket_id: i32,
    ) -> Result<Self, MempoolError> {
        let c_name = std::ffi::CString::new(name).map_err(|_| MempoolError::InvalidName)?;

        // SAFETY: EAL is initialized. We pass valid parameters and check
        // the return value. `rte_pktmbuf_pool_create` allocates from
        // hugepage memory on the specified NUMA socket.
        let raw = unsafe {
            ffi::rte_pktmbuf_pool_create(
                c_name.as_ptr(),
                num_mbufs,
                MBUF_CACHE_SIZE,
                0, // priv_size: no per-mbuf private data
                buf_size,
                socket_id,
            )
        };

        if raw.is_null() {
            return Err(MempoolError::CreateFailed);
        }

        tracing::info!(name, num_mbufs, socket_id, "created DPDK mempool");
        Ok(Mempool { raw })
    }

    /// Raw pointer to the underlying `rte_mempool`.
    /// Needed by ethdev queue setup and `rte_pktmbuf_alloc`.
    pub fn as_raw(&self) -> *mut ffi::rte_mempool {
        self.raw
    }
}

impl Drop for Mempool {
    fn drop(&mut self) {
        // SAFETY: we own the mempool and it was successfully created.
        unsafe {
            ffi::dpdk_mempool_free(self.raw);
        }
    }
}

#[derive(Debug)]
pub enum MempoolError {
    InvalidName,
    CreateFailed,
}

impl std::fmt::Display for MempoolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MempoolError::InvalidName => write!(f, "mempool name contains null byte"),
            MempoolError::CreateFailed => write!(f, "rte_pktmbuf_pool_create failed"),
        }
    }
}

impl std::error::Error for MempoolError {}
