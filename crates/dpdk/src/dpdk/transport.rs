//! High-level DPDK transport: combines EAL, port, mempool, and smoltcp
//! into a single poll-driven interface for the trading server.
//!
//! The transport owns the DPDK port and smoltcp interface. The server's
//! DPDK poll thread calls `poll()` in a tight loop to drive all I/O.

use std::net::Ipv4Addr;
use std::sync::Arc;

use crate::ffi;

use smoltcp::iface::{Config, Interface, SocketHandle, SocketSet};
use smoltcp::socket::tcp::{self, OpaqueFrameHandle, State};
use smoltcp::time::Instant;
use smoltcp::wire::{EthernetAddress, HardwareAddress, IpAddress, IpCidr, Ipv4Address};

use crate::device::DpdkDevice;
use crate::eal::Eal;
use crate::mempool::Mempool;
use crate::port::{ChecksumOffloads, Port};

/// Apply low-latency TCP tuning to a smoltcp socket.
///
/// Called on every socket (listen + accepted) to configure for trading:
/// - Nagle disabled (TCP_NODELAY): send small messages immediately
/// - Delayed ACK disabled: ACK every segment without waiting 10ms
/// - RTO floor lowered to 10ms (default 1s is 10,000x the LAN RTT)
/// - Initial RTO lowered to 50ms (first retransmit before any RTT sample)
/// - Initial congestion window raised to 64 KiB (skip slow start on LAN)
///
/// These settings sacrifice marginal bandwidth efficiency and RFC
/// compliance for latency on a trusted, dedicated LAN.
fn tune_socket(socket: &mut tcp::Socket<'_>) {
    socket.set_nagle_enabled(false);
    socket.set_ack_delay(None);
    socket.set_min_rto(smoltcp::time::Duration::from_millis(1));
    socket.set_initial_rto(smoltcp::time::Duration::from_millis(50));
    socket.set_initial_congestion_window(64 * 1024);
}

/// Maximum concurrent TCP connections. Exposed so callers can pre-size
/// per-connection state vectors that parallel the smoltcp socket set.
pub const MAX_CONNECTIONS: usize = 1024;

/// TCP listen port for trading connections.
const LISTEN_PORT: u16 = 9876;

/// Retain callback for zero-copy RX segments. Called by smoltcp when it
/// stores a segment in the zero-copy array. Bumps the mbuf refcount so
/// the mbuf survives after the RxBatch is recycled.
fn retain_mbuf(handle: OpaqueFrameHandle) {
    let mut ptr_bytes = [0u8; 8];
    ptr_bytes.copy_from_slice(&handle.as_bytes()[..8]);
    let mbuf = usize::from_ne_bytes(ptr_bytes) as *mut ffi::rte_mbuf;
    if !mbuf.is_null() {
        unsafe { ffi::dpdk_mbuf_refcnt_update(mbuf, 1) };
    }
}

/// Release callback for zero-copy RX segments. Called by smoltcp when the
/// application consumes a segment via `recv_zero_copy`. Frees the mbuf
/// (decrements refcount — reaches 0 after retain+recycle+release).
fn release_mbuf(handle: OpaqueFrameHandle) {
    let mut ptr_bytes = [0u8; 8];
    ptr_bytes.copy_from_slice(&handle.as_bytes()[..8]);
    let mbuf = usize::from_ne_bytes(ptr_bytes) as *mut ffi::rte_mbuf;
    if !mbuf.is_null() {
        unsafe { ffi::dpdk_pktmbuf_free(mbuf) };
    }
}

/// Encode an mbuf pointer into an OpaqueFrameHandle (first 8 bytes).
fn mbuf_to_handle(mbuf: *mut ffi::rte_mbuf) -> OpaqueFrameHandle {
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&(mbuf as usize).to_ne_bytes());
    OpaqueFrameHandle::from_bytes(bytes)
}

/// Maximum TX queue size per connection (bytes). If a client falls behind
/// and the queue exceeds this, the connection is dropped.
const MAX_TX_QUEUE_SIZE: usize = 64 * 1024;

/// smoltcp TCP RX buffer size. Determines the advertised receive window.
/// 64 KiB provides enough window for pipelined trading (256+ in-flight
/// messages at ~100 bytes each).
const SOCKET_RX_BUF_SIZE: usize = 64 * 1024;

/// smoltcp TCP TX buffer size. Controls how much response data queues
/// per connection before dispatch_burst generates TCP segments. Smaller
/// values reduce per-socket egress burst size, improving p99 latency
/// with many connections (fewer segments serialized per egress pass).
/// 16 KiB ≈ 11 segments at 1500 MTU vs 43 segments with 64 KiB.
const SOCKET_TX_BUF_SIZE: usize = 16 * 1024;

/// How often to refresh the smoltcp timestamp (in poll iterations).
/// smoltcp only needs millisecond-precision timestamps for TCP timers
/// (retransmit, keepalive). Refreshing every 100 iterations at ~1MHz
/// poll rate gives ~10ms resolution — plenty for TCP timers.
const TIMESTAMP_REFRESH_INTERVAL: u32 = 100;

/// Configuration for the DPDK transport.
#[derive(Clone)]
pub struct DpdkConfig {
    pub eal_args: Vec<String>,
    /// DPDK port IDs to poll. The first port is used for TX; all ports
    /// are polled for RX. For LACP bonds, pass both VF port IDs (e.g.,
    /// `vec![0, 1]`) so traffic arriving on either bond member is received.
    pub port_ids: Vec<u16>,
    pub ip_addr: Ipv4Addr,
    pub prefix_len: u8,
    pub gateway: Option<Ipv4Addr>,
    pub listen_port: u16,
    /// MTU for the DPDK interface. 1500 for standard Ethernet, 9000 for
    /// jumbo frames (6x fewer TCP segments, ~6x less per-segment overhead).
    pub mtu: usize,
    /// VLAN ID for hardware strip/insert. Used in dedicated NIC mode where
    /// the kernel isn't handling VLAN tags. None = no VLAN offload (SR-IOV
    /// mode where the PF handles VLAN tagging).
    pub vlan_id: Option<u16>,
    /// Number of RX/TX queue pairs per port. Each queue pair is polled
    /// by a separate thread. When > 1, RSS is enabled on the NIC to
    /// distribute TCP/IP flows across queues. Default: 1.
    pub num_queues: u16,
}

impl Default for DpdkConfig {
    fn default() -> Self {
        DpdkConfig {
            eal_args: Vec::new(),
            port_ids: vec![0],
            ip_addr: Ipv4Addr::new(10, 0, 0, 1),
            prefix_len: 24,
            gateway: None,
            listen_port: LISTEN_PORT,
            mtu: 1500,
            vlan_id: None,
            num_queues: 1,
        }
    }
}

/// A new TCP connection accepted by the transport.
pub struct AcceptedConnection {
    pub handle: SocketHandle,
    pub peer: std::net::SocketAddr,
    /// The local TCP port the connection was accepted on. With a single
    /// listener this is always the same value (config.listen_port); with
    /// multiple listeners (added via `DpdkTransport::add_listener`) the
    /// caller uses this to dispatch the connection to the right handler
    /// (e.g. trading port → client logic, replication port → replication
    /// state machine).
    pub listen_port: u16,
}

/// Shared DPDK resources created once and shared across all poll threads.
///
/// Fields are ordered so that DPDK resources are dropped before EAL
/// cleanup: ports → mempool → EAL. Rust drops fields in declaration
/// order, and `rte_mempool_free` requires EAL to still be alive.
///
/// Held via `Arc` — the last poll thread to exit drops these resources.
pub struct DpdkShared {
    _ports: Vec<Port>,
    _mempool: Mempool,
    _eal: Eal,
    /// Intersection of all ports' checksum offload capabilities.
    pub offloads: ChecksumOffloads,
    /// MAC address of the first port (used for all smoltcp interfaces).
    pub mac: [u8; 6],
    /// Raw mempool pointer for DpdkDevice creation. Thread-safe — DPDK
    /// mempools use per-lcore caches for lock-free alloc/free.
    pub mempool_raw: *mut crate::ffi::rte_mempool,
    /// Actual number of queue pairs configured (may be less than requested
    /// if the NIC doesn't support that many).
    pub num_queues: u16,
}

// Safety: DpdkShared fields are either DPDK thread-safe (mempool) or
// never accessed after init (ports, eal). The mempool_raw pointer is
// only used for mbuf alloc/free which DPDK guarantees is thread-safe.
unsafe impl Send for DpdkShared {}
unsafe impl Sync for DpdkShared {}

/// Per-thread DPDK transport. Owns its own smoltcp Interface and
/// SocketSet. Each poll thread gets one of these.
///
/// All methods must be called from the owning poll thread.
pub struct DpdkTransport {
    _shared: Arc<DpdkShared>,
    device: DpdkDevice,
    iface: Interface,
    sockets: SocketSet<'static>,
    /// (port, handle) for every TCP listening socket the transport
    /// currently maintains. Initialised with one entry from
    /// `config.listen_port`; callers can add more via `add_listener`.
    /// `check_listener` iterates this list, accepts any socket that
    /// transitioned to Established, and replaces it with a fresh
    /// listener on the same port — so the slot for that port stays
    /// receptive while the accepted connection moves into `accepted`.
    listeners: Vec<(u16, SocketHandle)>,
    accepted: Vec<AcceptedConnection>,
    /// Per-connection TX buffers, indexed by `SocketHandle::index()`.
    /// Dense `Vec<Option<_>>` instead of a HashMap — HashMap hashing
    /// showed up prominently on the DPDK poll core under throughput.
    /// `None` = slot free (no socket or no pending TX). Uses a cursor
    /// inside TxQueue to avoid O(n) drain on partial sends.
    tx_queues: Vec<Option<TxQueue>>,
    /// Cached smoltcp timestamp. Refreshed periodically, not every poll.
    cached_timestamp: Instant,
    /// Poll iteration counter for timestamp refresh.
    poll_count: u32,
    /// Total pending TX bytes across all connections. Avoids iterating
    /// tx_queues.values().any() on every poll cycle.
    pending_tx_bytes: usize,
}

/// Per-connection TX queue with cursor to avoid drain() memmoves.
struct TxQueue {
    buf: Vec<u8>,
    /// Read cursor — bytes before this have already been sent.
    cursor: usize,
}

impl TxQueue {
    fn new() -> Self {
        TxQueue {
            buf: Vec::new(),
            cursor: 0,
        }
    }

    /// Pending bytes to send.
    fn pending(&self) -> &[u8] {
        &self.buf[self.cursor..]
    }

    /// Total queued bytes (including already-sent prefix).
    fn queued_bytes(&self) -> usize {
        self.buf.len() - self.cursor
    }

    /// Advance the cursor after a successful send.
    fn advance(&mut self, n: usize) {
        self.cursor += n;
        // Compact at 25% waste — tighter memory footprint under sustained
        // send backpressure without excessive memmove frequency.
        if self.cursor > self.buf.len() / 4 && self.cursor > 4096 {
            self.buf.drain(..self.cursor);
            self.cursor = 0;
        }
    }

    /// Append data to the queue.
    fn push(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }
}

impl DpdkShared {
    /// Initialize shared DPDK resources: EAL, mempool, ports.
    /// Call once before spawning poll threads.
    pub fn init(config: &DpdkConfig) -> Result<Arc<Self>, Box<dyn std::error::Error>> {
        let eal_args: Vec<&str> = config.eal_args.iter().map(|s| s.as_str()).collect();
        let eal = Eal::init(&eal_args)?;

        let port_count = eal.port_count();
        for &pid in &config.port_ids {
            if pid >= port_count {
                return Err(
                    format!("DPDK port {} not found (available: {})", pid, port_count).into(),
                );
            }
        }

        // Scale mempool for number of queues and ports.
        let num_mbufs: u32 =
            8192 * (config.port_ids.len() as u32).max(1) * (config.num_queues as u32).max(1);
        let mempool = if config.mtu > 1500 {
            Mempool::create_for_mtu("pktmbuf_pool", num_mbufs, config.mtu as u16, 0)?
        } else {
            Mempool::create_with_size("pktmbuf_pool", num_mbufs, 0)?
        };

        // Configure and start all ports with N queue pairs.
        let mut ports = Vec::with_capacity(config.port_ids.len());
        let mut combined_offloads: Option<ChecksumOffloads> = None;
        for &pid in &config.port_ids {
            let mut port =
                Port::configure_with_vlan(pid, &mempool, config.vlan_id, config.num_queues)?;
            port.start()?;
            combined_offloads = Some(match combined_offloads {
                None => port.offloads,
                Some(prev) => prev.intersect(port.offloads),
            });
            ports.push(port);
        }
        let offloads = combined_offloads.unwrap_or_default();
        let mac = ports[0].mac_addr();
        let mempool_raw = mempool.as_raw();

        // Use the actual queue count from the first port (may be less
        // than requested if the NIC doesn't support that many).
        let actual_queues = ports[0].num_queues;

        Ok(Arc::new(DpdkShared {
            _ports: ports,
            _mempool: mempool,
            _eal: eal,
            offloads,
            mac,
            mempool_raw,
            num_queues: actual_queues,
        }))
    }
}

impl DpdkTransport {
    /// Create a per-thread transport from shared resources.
    ///
    /// Each poll thread calls this with a unique `queue_id` (0..N-1).
    /// The transport gets its own DpdkDevice, smoltcp Interface, and
    /// SocketSet — no shared mutable state between threads.
    pub fn from_shared(
        shared: &Arc<DpdkShared>,
        config: &DpdkConfig,
        queue_id: u16,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let mut device = DpdkDevice::new(
            &config.port_ids,
            shared.mempool_raw,
            shared.offloads,
            queue_id,
        );
        if config.mtu != 1500 {
            device.set_mtu(config.mtu);
            tracing::info!(mtu = config.mtu, queue_id, "DPDK jumbo frames enabled");
        }
        if let Some(vlan_id) = config.vlan_id {
            device.set_vlan_id(vlan_id);
        }

        let hw_addr = HardwareAddress::Ethernet(EthernetAddress(shared.mac));
        let iface_config = Config::new(hw_addr);
        let now = Instant::from_millis(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
        );
        let mut iface = Interface::new(iface_config, &mut DpdkDeviceRef(&device), now);

        let ip = Ipv4Address::new(
            config.ip_addr.octets()[0],
            config.ip_addr.octets()[1],
            config.ip_addr.octets()[2],
            config.ip_addr.octets()[3],
        );
        iface.update_ip_addrs(|addrs| {
            addrs
                .push(IpCidr::new(IpAddress::Ipv4(ip), config.prefix_len))
                .expect("IP address capacity");
        });

        if let Some(gw) = config.gateway {
            let gw_addr = Ipv4Address::new(
                gw.octets()[0],
                gw.octets()[1],
                gw.octets()[2],
                gw.octets()[3],
            );
            iface
                .routes_mut()
                .add_default_ipv4_route(gw_addr)
                .expect("default route capacity");
        }

        let mut sockets = SocketSet::new(Vec::with_capacity(MAX_CONNECTIONS));

        let listen_socket = {
            let rx_buf = tcp::SocketBuffer::new(vec![0u8; SOCKET_RX_BUF_SIZE]);
            let tx_buf = tcp::SocketBuffer::new(vec![0u8; SOCKET_TX_BUF_SIZE]);
            let mut socket = tcp::Socket::new(rx_buf, tx_buf);
            tune_socket(&mut socket);
            socket
                .listen(config.listen_port)
                .map_err(|e| format!("TCP listen failed: {e}"))?;
            socket
        };
        let listen_handle = sockets.add(listen_socket);

        tracing::info!(
            ip = %config.ip_addr,
            port = config.listen_port,
            mac = ?shared.mac,
            queue_id,
            "DPDK transport initialized"
        );

        Ok(DpdkTransport {
            _shared: Arc::clone(shared),
            device,
            iface,
            sockets,
            listeners: vec![(config.listen_port, listen_handle)],
            accepted: Vec::new(),
            // Pre-allocate all MAX_CONNECTIONS slots so index lookup is
            // always in-bounds. Each empty slot is a single discriminant
            // tag — no heap allocation per slot.
            tx_queues: (0..MAX_CONNECTIONS).map(|_| None).collect(),
            cached_timestamp: now,
            poll_count: 0,
            pending_tx_bytes: 0,
        })
    }

    /// Like `from_shared` but overrides the listen port.
    ///
    /// Used by the replication sender to listen on the replication port
    /// instead of the trading port, while sharing the same DPDK NIC and
    /// IP address.
    pub fn from_shared_with_port(
        shared: &Arc<DpdkShared>,
        config: &DpdkConfig,
        queue_id: u16,
        listen_port: u16,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let mut overridden = config.clone();
        overridden.listen_port = listen_port;
        Self::from_shared(shared, &overridden, queue_id)
    }

    /// Open an outbound TCP connection to a remote endpoint.
    ///
    /// Creates a new smoltcp TCP socket, calls `socket.connect()` to
    /// initiate the TCP handshake, and returns the socket handle. The
    /// caller must poll the transport until `is_connected(handle)` returns
    /// true before sending data.
    pub fn connect_to(
        &mut self,
        remote_ip: std::net::Ipv4Addr,
        remote_port: u16,
        local_port: u16,
    ) -> SocketHandle {
        let rx_buf = tcp::SocketBuffer::new(vec![0u8; SOCKET_RX_BUF_SIZE]);
        let tx_buf = tcp::SocketBuffer::new(vec![0u8; SOCKET_TX_BUF_SIZE]);
        let mut socket = tcp::Socket::new(rx_buf, tx_buf);
        tune_socket(&mut socket);
        socket.set_zero_copy_retain_fn(retain_mbuf);
        socket.set_zero_copy_release_fn(release_mbuf);

        let remote_addr = Ipv4Address::new(
            remote_ip.octets()[0],
            remote_ip.octets()[1],
            remote_ip.octets()[2],
            remote_ip.octets()[3],
        );
        let local_ip = self.iface.ipv4_addr().expect("interface has IPv4 address");
        socket
            .connect(
                self.iface.context(),
                (IpAddress::Ipv4(remote_addr), remote_port),
                (IpAddress::Ipv4(local_ip), local_port),
            )
            .expect("smoltcp connect failed");

        self.sockets.add(socket)
    }

    /// Check if a socket has completed the TCP handshake and is ready
    /// for data transfer (both send and receive directions open).
    pub fn is_connected(&mut self, handle: SocketHandle) -> bool {
        let socket = self.sockets.get_mut::<tcp::Socket>(handle);
        socket.may_send() && socket.may_recv()
    }

    /// Convenience: initialize shared resources + create a single-queue transport.
    /// Equivalent to `DpdkShared::init(config)` + `DpdkTransport::from_shared(..., 0)`.
    pub fn init(config: &DpdkConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let shared = DpdkShared::init(config)?;
        Self::from_shared(&shared, config, 0)
    }

    /// Run one poll iteration.
    pub fn poll(&mut self) -> Instant {
        // Refresh the smoltcp timestamp periodically, not every poll.
        // smoltcp only needs ms-precision for TCP retransmit/keepalive timers.
        self.poll_count = self.poll_count.wrapping_add(1);
        if self.poll_count.is_multiple_of(TIMESTAMP_REFRESH_INTERVAL) {
            self.cached_timestamp = Instant::from_millis(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64,
            );
        }

        // Batch ingress: poll all ports in one pass. MAC learning happens
        // inside collect_rx_batch() for every IPv4 frame.
        let mut batch = self.device.collect_rx_batch();

        // Seed neighbor cache from MACs learned in THIS batch (not the
        // previous one). This ensures smoltcp knows the client's MAC
        // before processing the SYN that arrived in the same batch —
        // without this, the SYN-ACK would stall waiting for ARP
        // resolution that can never complete on SR-IOV VFs (broadcast
        // ARP is dropped by the PF).
        for (mac, ip_bytes) in self.device.take_learned_neighbors() {
            let ip = Ipv4Addr::new(ip_bytes[0], ip_bytes[1], ip_bytes[2], ip_bytes[3]);
            self.seed_neighbor(ip, mac);
        }
        // Drain any ARP frames injected by seed_neighbor() into the
        // batch so they're processed before the SYN.
        batch.append_injected(&mut self.device);

        if !batch.is_empty() {
            // Build (slice, handle) pairs for zero-copy ingress.
            // No batch-level refcount bump — the socket's retain callback
            // bumps only mbufs that are actually stored as zero-copy segments.
            const MAX_SLICES: usize = 128;
            let mut zc_buf: [std::mem::MaybeUninit<(&[u8], OpaqueFrameHandle)>; MAX_SLICES] =
                [std::mem::MaybeUninit::uninit(); MAX_SLICES];
            let count = batch.write_slices_with_handles(&mut zc_buf, mbuf_to_handle);

            // SAFETY: write_slices_with_handles initialized exactly `count` elements.
            let frames = unsafe {
                std::slice::from_raw_parts(
                    zc_buf.as_ptr().cast::<(&[u8], OpaqueFrameHandle)>(),
                    count,
                )
            };
            self.iface.poll_ingress_batch_zero_copy(
                self.cached_timestamp,
                &mut self.device,
                &mut self.sockets,
                frames,
            );
            // Flush TX generated by ingress processing (ACKs, window updates).
            self.device.flush_tx();
        }
        let rx_had_data = !batch.is_empty();
        batch.recycle(&mut self.device);

        // Egress + maintenance (TCP timers, ARP, socket_egress).
        // Skip when idle: no RX data, no pending TX, and timers not due.
        // Piggyback timer checks on the timestamp refresh interval.
        let has_pending_tx = self.pending_tx_bytes > 0;
        if rx_had_data
            || has_pending_tx
            || self.poll_count.is_multiple_of(TIMESTAMP_REFRESH_INTERVAL)
        {
            self.flush_tx_queues();
            self.iface
                .poll(self.cached_timestamp, &mut self.device, &mut self.sockets);
            self.device.flush_tx();
            self.check_listener();
        }

        self.cached_timestamp
    }

    fn check_listener(&mut self) {
        // Iterate every (port, handle) pair: accept any whose listen
        // socket has progressed to Established, replace it with a fresh
        // listener on the same port. Each port is independent — the
        // trading port and the replication port (when both are used by
        // the same transport) keep their own listener slots.
        let mut i = 0;
        while i < self.listeners.len() {
            let (port, handle) = self.listeners[i];
            let socket = self.sockets.get_mut::<tcp::Socket>(handle);
            if socket.state() != State::Established {
                i += 1;
                continue;
            }

            let peer = match socket.remote_endpoint() {
                Some(remote) => match remote.addr {
                    IpAddress::Ipv4(ip) => {
                        let octets = ip.octets();
                        std::net::SocketAddr::new(
                            std::net::IpAddr::V4(Ipv4Addr::new(
                                octets[0], octets[1], octets[2], octets[3],
                            )),
                            remote.port,
                        )
                    }
                },
                None => {
                    i += 1;
                    continue;
                }
            };

            // Register zero-copy callbacks on the accepted socket before
            // it processes any data segments.
            let accepted_socket = self.sockets.get_mut::<tcp::Socket>(handle);
            accepted_socket.set_zero_copy_retain_fn(retain_mbuf);
            accepted_socket.set_zero_copy_release_fn(release_mbuf);

            // Replace the listener slot in-place so the port stays
            // receptive while the just-accepted handle is moved out
            // into `accepted`.
            let new_listener = {
                let rx_buf = tcp::SocketBuffer::new(vec![0u8; SOCKET_RX_BUF_SIZE]);
                let tx_buf = tcp::SocketBuffer::new(vec![0u8; SOCKET_TX_BUF_SIZE]);
                let mut socket = tcp::Socket::new(rx_buf, tx_buf);
                tune_socket(&mut socket);
                socket.listen(port).expect("re-listen after accept");
                socket
            };
            let new_handle = self.sockets.add(new_listener);
            self.listeners[i] = (port, new_handle);

            self.accepted.push(AcceptedConnection {
                handle,
                peer,
                listen_port: port,
            });

            tracing::debug!(peer = %peer, listen_port = port, "DPDK: TCP connection accepted");
            // Don't advance `i` — re-check this slot in case a fresh SYN
            // already completed the handshake on the new listener within
            // the same poll cycle.
        }
    }

    /// Add another TCP port to listen on, sharing the same DPDK NIC and
    /// smoltcp interface. Returns immediately; the new listener is
    /// active on the next `poll()`. Used when a single transport handles
    /// multiple distinct services (e.g. trading on 9876 and replication
    /// on 9877) without needing separate queues / threads.
    pub fn add_listener(&mut self, port: u16) -> Result<(), Box<dyn std::error::Error>> {
        let rx_buf = tcp::SocketBuffer::new(vec![0u8; SOCKET_RX_BUF_SIZE]);
        let tx_buf = tcp::SocketBuffer::new(vec![0u8; SOCKET_TX_BUF_SIZE]);
        let mut socket = tcp::Socket::new(rx_buf, tx_buf);
        tune_socket(&mut socket);
        socket
            .listen(port)
            .map_err(|e| format!("TCP listen on port {port} failed: {e}"))?;
        let handle = self.sockets.add(socket);
        self.listeners.push((port, handle));
        tracing::info!(port, "DPDK transport: added listener");
        Ok(())
    }

    fn flush_tx_queues(&mut self) {
        // Iterate occupied sockets (smoltcp's iter_mut skips empties) and
        // look up each socket's TX slot by handle index. Avoids a
        // HashMap lookup on the hot path.
        let Self {
            tx_queues,
            sockets,
            pending_tx_bytes,
            ..
        } = self;
        for (handle, socket) in sockets.iter_mut() {
            let Some(queue) = tx_queues.get_mut(handle.index()).and_then(|s| s.as_mut()) else {
                continue;
            };
            if queue.queued_bytes() == 0 {
                continue;
            }
            // Only TCP sockets are ever added to the set; the enum has a
            // single variant enabled by feature flags.
            let smoltcp::socket::Socket::Tcp(socket) = socket;
            if !socket.can_send() {
                continue;
            }
            let sent = socket.send_slice(queue.pending()).unwrap_or(0);
            if sent > 0 {
                queue.advance(sent);
                *pending_tx_bytes -= sent;
            }
        }
    }

    /// Take all newly accepted connections.
    pub fn take_accepted(&mut self) -> Vec<AcceptedConnection> {
        std::mem::take(&mut self.accepted)
    }

    /// Read available data from a connection into an external buffer.
    pub fn recv(&mut self, handle: SocketHandle, buf: &mut [u8]) -> usize {
        let socket = self.sockets.get_mut::<tcp::Socket>(handle);
        if !socket.can_recv() {
            return 0;
        }
        socket.recv_slice(buf).unwrap_or(0)
    }

    /// Append available data from a connection directly into a Vec.
    /// Uses zero-copy RX: reads directly from DPDK mbuf memory, then
    /// releases the mbuf via the registered release callback.
    pub fn recv_into_vec(&mut self, handle: SocketHandle, dest: &mut Vec<u8>) -> usize {
        let socket = self.sockets.get_mut::<tcp::Socket>(handle);
        if !socket.can_recv() {
            return 0;
        }
        socket
            .recv_zero_copy(|data| {
                if dest.try_reserve(data.len()).is_err() {
                    return 0;
                }
                dest.extend_from_slice(data);
                data.len()
            })
            .unwrap_or(0)
    }

    /// Queue data to be sent on a connection. Returns false if the
    /// connection's TX queue exceeds the size limit (client fell behind).
    pub fn queue_send(&mut self, handle: SocketHandle, data: &[u8]) -> bool {
        // Slot is always in-bounds: tx_queues is pre-sized to
        // MAX_CONNECTIONS and smoltcp never hands out a handle beyond
        // its socket capacity (also MAX_CONNECTIONS).
        let slot = &mut self.tx_queues[handle.index()];
        let queue = slot.get_or_insert_with(TxQueue::new);
        if queue.queued_bytes() + data.len() > MAX_TX_QUEUE_SIZE {
            return false;
        }
        queue.push(data);
        self.pending_tx_bytes += data.len();
        true
    }

    /// Currently queued TX bytes for the given socket. Used by replication
    /// to back-pressure ring reads when the wire can't keep up — reading a
    /// batch we can't actually queue would advance the ring cursor without
    /// the data ever reaching the replica.
    pub fn tx_queue_bytes(&self, handle: SocketHandle) -> usize {
        self.tx_queues[handle.index()]
            .as_ref()
            .map_or(0, |q| q.queued_bytes())
    }

    /// Maximum bytes that `queue_send` will accept per connection before
    /// returning `false`. Exposed so callers can size their batches.
    pub const fn max_tx_queue_size() -> usize {
        MAX_TX_QUEUE_SIZE
    }

    /// Check if a connection is still open.
    pub fn is_active(&mut self, handle: SocketHandle) -> bool {
        let socket = self.sockets.get_mut::<tcp::Socket>(handle);
        socket.is_active()
    }

    /// Close a connection (sends FIN) and remove from the socket set.
    /// The socket is fully removed so its tuple doesn't block future
    /// connections from the same source port.
    pub fn close(&mut self, handle: SocketHandle) {
        let socket = self.sockets.get_mut::<tcp::Socket>(handle);
        socket.abort();
        self.sockets.remove(handle);
        if let Some(q) = self.tx_queues[handle.index()].take() {
            self.pending_tx_bytes -= q.queued_bytes();
        }
    }

    /// Send a gratuitous ARP out the NIC so the switch learns our MAC.
    ///
    /// SR-IOV VFs can't receive broadcast ARP (promiscuous mode unsupported),
    /// so remote hosts' ARP requests go unanswered and the switch never learns
    /// the VF's MAC from an ARP reply. A gratuitous ARP (ARP request where
    /// sender and target IP are both ours) is broadcast by definition, and
    /// when the switch sees it *sourced* from our MAC, it installs a forwarding
    /// entry. After this, unicast frames destined to our MAC reach the VF.
    ///
    /// Call once after transport initialization.
    pub fn send_gratuitous_arp(&mut self) {
        let our_mac = self._shared.mac;
        let our_ip = self.iface.ipv4_addr().expect("interface has IPv4 address");

        // Gratuitous ARP: Ethernet broadcast + ARP request where
        // sender_ip == target_ip (RFC 5227).
        let mut frame = [0u8; 42]; // 14 (eth) + 28 (ARP) = 42 bytes

        // Ethernet header: broadcast destination.
        frame[0..6].copy_from_slice(&[0xff, 0xff, 0xff, 0xff, 0xff, 0xff]);
        frame[6..12].copy_from_slice(&our_mac);
        frame[12..14].copy_from_slice(&[0x08, 0x06]); // EtherType: ARP

        // ARP payload.
        frame[14..16].copy_from_slice(&[0x00, 0x01]); // hardware type: Ethernet
        frame[16..18].copy_from_slice(&[0x08, 0x00]); // protocol type: IPv4
        frame[18] = 6; // hardware addr len
        frame[19] = 4; // protocol addr len
        frame[20..22].copy_from_slice(&[0x00, 0x01]); // operation: request
        frame[22..28].copy_from_slice(&our_mac); // sender hardware addr
        frame[28..32].copy_from_slice(&our_ip.octets()); // sender protocol addr
        frame[32..38].copy_from_slice(&[0x00; 6]); // target hardware addr (zero)
        frame[38..42].copy_from_slice(&our_ip.octets()); // target protocol addr (= sender)

        self.device.send_raw_frame(&frame);

        tracing::info!(
            mac = ?our_mac,
            ip = %our_ip,
            "sent gratuitous ARP (switch MAC learning)"
        );
    }

    /// Seed smoltcp's neighbor cache by injecting a crafted ARP reply.
    ///
    /// SR-IOV VFs on Intel X710 (and similar NICs) can't receive broadcast
    /// frames, so ARP resolution fails. This method injects a fake ARP reply
    /// into smoltcp's RX path so it learns the IP→MAC mapping without
    /// needing a real ARP exchange.
    ///
    /// Call this for any peer IP that smoltcp needs to reach (e.g., bench
    /// clients, gateways) when running on an SR-IOV VF.
    pub fn seed_neighbor(&mut self, ip: Ipv4Addr, mac: [u8; 6]) {
        let our_mac = self._shared.mac;
        let our_ip = self.iface.ipv4_addr().expect("interface has IPv4 address");

        // Craft an ARP reply Ethernet frame:
        //   Ethernet: dst=our_mac, src=peer_mac, type=0x0806 (ARP)
        //   ARP: reply, sender_hw=peer_mac, sender_ip=peer_ip,
        //        target_hw=our_mac, target_ip=our_ip
        let mut frame = [0u8; 42]; // 14 (eth) + 28 (ARP) = 42 bytes

        // Ethernet header
        frame[0..6].copy_from_slice(&our_mac); // dst MAC (us)
        frame[6..12].copy_from_slice(&mac); // src MAC (peer)
        frame[12..14].copy_from_slice(&[0x08, 0x06]); // EtherType: ARP

        // ARP payload
        frame[14..16].copy_from_slice(&[0x00, 0x01]); // hardware type: Ethernet
        frame[16..18].copy_from_slice(&[0x08, 0x00]); // protocol type: IPv4
        frame[18] = 6; // hardware addr len
        frame[19] = 4; // protocol addr len
        frame[20..22].copy_from_slice(&[0x00, 0x02]); // operation: reply
        frame[22..28].copy_from_slice(&mac); // sender hardware addr
        frame[28..32].copy_from_slice(&ip.octets()); // sender protocol addr
        frame[32..38].copy_from_slice(&our_mac); // target hardware addr
        frame[38..42].copy_from_slice(&our_ip.octets()); // target protocol addr

        self.device.inject_rx(frame.to_vec());

        tracing::debug!(
            peer_ip = %ip,
            peer_mac = ?mac,
            "seeded neighbor cache with ARP reply"
        );
    }
}

/// Temporary wrapper for Interface::new capability probing.
struct DpdkDeviceRef<'a>(&'a DpdkDevice);

impl<'a> smoltcp::phy::Device for DpdkDeviceRef<'a> {
    type RxToken<'b>
        = crate::device::DpdkRxToken
    where
        Self: 'b;
    type TxToken<'b>
        = crate::device::DpdkTxToken<'b>
    where
        Self: 'b;

    fn receive(&mut self, _timestamp: Instant) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        None
    }

    fn transmit(&mut self, _timestamp: Instant) -> Option<Self::TxToken<'_>> {
        None
    }

    fn capabilities(&self) -> smoltcp::phy::DeviceCapabilities {
        self.0.capabilities()
    }
}
