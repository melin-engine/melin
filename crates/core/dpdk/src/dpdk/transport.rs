//! High-level DPDK transport: combines EAL, port, mempool, and smoltcp
//! into a single poll-driven interface for the server.
//!
//! The transport owns the DPDK port and smoltcp interface. The server's
//! DPDK poll thread calls `poll()` in a tight loop to drive all I/O.

use std::net::Ipv4Addr;
use std::sync::Arc;

use crate::ffi;

use smoltcp::iface::{Config, Interface, SocketHandle, SocketSet};
use smoltcp::socket::tcp::{self, OpaqueFrameHandle};
use smoltcp::time::Instant;
use smoltcp::wire::{EthernetAddress, HardwareAddress, IpAddress, IpCidr, Ipv4Address};

use crate::config::{NicConfig, ReplicationNic, SOCKET_RX_BUF_SIZE, SocketBuffers};
use crate::device::DpdkDevice;
use crate::eal::Eal;
use crate::eal_sharing::{EalGroup, OnEal};
use crate::listen_backlog::{ListenerConfig, Listeners, MAX_LISTENING_SOCKETS, tune_socket};
use crate::mempool::Mempool;
use crate::peer_liveness::{self, PeerLiveness, StackClock};
use crate::port::{ChecksumOffloads, Port};

// Defined outside the `dpdk-sys` gate (`crate::config`); re-exported here so
// `melin_dpdk::transport::DpdkConfig` still names it.
pub use crate::config::DpdkConfig;

/// Maximum concurrent TCP connections. Exposed so callers can pre-size
/// per-connection state vectors that parallel the smoltcp socket set.
pub const MAX_CONNECTIONS: usize = 1024;

// Every port's listening sockets share the socket set with its
// connections: whatever they take leaves most of it to connections.
const _: () = assert!(MAX_LISTENING_SOCKETS * 8 <= MAX_CONNECTIONS);

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

/// Maximum TX queue size per connection (bytes) for a socket opened
/// without sizes of its own: a dialled stream (`connect_to`, the client
/// NIC handle's). If the peer falls behind and the queue exceeds this, the
/// connection is dropped. The client port has its own limit, in
/// [`SocketBuffers::DEFAULT`].
const MAX_TX_QUEUE_SIZE: usize = 64 * 1024;

/// smoltcp TCP TX buffer size of a socket opened without sizes of its own
/// (`connect_to`). Smaller values reduce per-socket egress burst size
/// (fewer segments serialized per egress pass). 16 KiB ≈ 11 segments at
/// 1500 MTU vs 43 segments with 64 KiB. The client port has its own, in
/// [`SocketBuffers::DEFAULT`].
const SOCKET_TX_BUF_SIZE: usize = 16 * 1024;

/// How often to refresh the smoltcp timestamp (in poll iterations).
/// smoltcp only needs millisecond-precision timestamps for TCP timers
/// (retransmit, keepalive). Refreshing every 100 iterations at ~1MHz
/// poll rate gives ~10ms resolution — plenty for TCP timers.
const TIMESTAMP_REFRESH_INTERVAL: u32 = 100;

/// How often the statically configured gateway is re-seeded into the
/// neighbor cache, in milliseconds.
///
/// smoltcp expires a neighbor entry 60 s after it was filled, and the
/// gateway is the one entry nothing refreshes on its own: inbound frames
/// teach us `(source MAC, source IP)`, and a packet forwarded by the
/// gateway carries the *originator's* IP, never the gateway's own. Since
/// off-subnet destinations resolve through the default route's next hop —
/// the gateway's IP — that entry expiring silences every off-subnet
/// destination until an ARP that the PF swallows in bifurcated L3 mode.
///
/// 20 s gives three re-seeds per lifetime, so two consecutive misses
/// (a stalled poll loop, a coarse cached timestamp) still leave the entry
/// live. The cost is one injected 42-byte ARP frame per interval.
const GATEWAY_RESEED_INTERVAL_MS: i64 = 20_000;

/// smoltcp's neighbor entry lifetime. Not exposed by the crate (it is
/// `pub(crate)` there), so this is a copy: if a future fastcp release
/// shortens it, nothing here breaks — the const assert below only pins
/// our own interval against the value we believe. Verify it against
/// `iface::neighbor::Cache::ENTRY_LIFETIME` when bumping fastcp.
const NEIGHBOR_ENTRY_LIFETIME_MS: i64 = 60_000;

// A re-seed interval at or above the entry lifetime would leave a window
// where the gateway is unresolvable — the exact failure this timer exists
// to prevent.
const _: () = assert!(GATEWAY_RESEED_INTERVAL_MS < NEIGHBOR_ENTRY_LIFETIME_MS);

/// Whether the gateway entry is due for a re-seed at `now`.
///
/// Split out from [`DpdkTransport::poll`] so the interval arithmetic —
/// the part that can be wrong — is testable without a NIC. Re-seeds when
/// the interval has elapsed, and also when `now` runs backwards: the poll
/// loop's cached timestamp is sampled from the wall clock, so a clock step
/// must not park the entry until the clock catches up again.
fn gateway_reseed_due(now: Instant, last_seeded: Instant) -> bool {
    let elapsed = now.total_millis() - last_seeded.total_millis();
    !(0..GATEWAY_RESEED_INTERVAL_MS).contains(&elapsed)
}

/// A new TCP connection accepted by the transport.
pub struct AcceptedConnection {
    pub handle: SocketHandle,
    pub peer: std::net::SocketAddr,
    /// The local TCP port the connection was accepted on. With a single
    /// listener this is always the same value (config.listen_port); with
    /// multiple listeners (added via `DpdkTransport::add_listener_with_buffers`) the
    /// caller uses this to dispatch the connection to the right handler
    /// (e.g. client port → client logic, replication port → replication
    /// state machine).
    pub listen_port: u16,
}

/// A NIC group: the ports of one NIC (one port, or an LACP bond's), the
/// mbuf pool on their NUMA node, and the MAC every interface on them
/// takes, created once and shared across the poll threads of its queues.
///
/// It sits on an EAL it shares with every other group of the process
/// built on it (see [`NodeNics`]), and holds its share of it: the ports
/// and the pool are released first, ports then pool (`rte_mempool_free`
/// requires EAL alive, and a port's queues hold the pool's mbufs), and
/// an EAL the node owns is cleaned up when the last group on it drops.
///
/// Held via `Arc` — the last poll thread to exit drops these resources.
pub struct DpdkShared {
    _nic: OnEal<NicResources, NodeEal>,
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

/// What a NIC group releases before its share of the EAL: its ports,
/// then its pool (fields drop in declaration order).
struct NicResources {
    _ports: Vec<Port>,
    _mempool: Mempool,
}

/// A node's NIC groups, all on one EAL: the client NIC's, and, when its
/// replication has a NIC of its own ([`ReplicationNic::Port`]), that
/// NIC's.
///
/// Each group has its own ports, its own mbuf pool on its own NUMA node,
/// its own MAC and offloads, and its own queue count. The EAL is shared:
/// acquired once, from the client NIC's EAL arguments, which must allow
/// both NICs' devices (both PCI addresses, or both virtual devices), and,
/// when the node owns it, cleaned up after the last group drops, whichever
/// that is.
pub struct NodeNics {
    /// The client NIC's group, with the node's queue count
    /// ([`DpdkConfig::num_queues`]): what [`DpdkShared::init`] brings up.
    /// In [`ReplicationNic::Shared`] and [`ReplicationNic::Queue`] modes,
    /// replication runs on it too.
    pub client: Arc<DpdkShared>,
    /// The replication NIC's group in [`ReplicationNic::Port`] mode, with
    /// one queue (its one poll thread); `None` in the other modes.
    pub replication: Option<Arc<DpdkShared>>,
}

impl NodeNics {
    /// Bring up the NIC groups `config` asks for on one EAL: initialized
    /// from the client NIC's EAL arguments, or, in a process that runs a
    /// process-wide EAL, that one, shared, as [`DpdkShared::init`] does.
    ///
    /// A replication NIC is refused before EAL is touched when it names
    /// EAL arguments other than the client NIC's (they could only be
    /// ignored) or a port of the client NIC's.
    pub fn init(config: &DpdkConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let replication_nic = match &config.replication {
            ReplicationNic::Shared | ReplicationNic::Queue => None,
            ReplicationNic::Port(nic) => {
                crate::eal_sharing::check_further_group(&config.nic, nic)?;
                Some(nic)
            }
        };
        let eal = Arc::new(NodeEal::acquire(&config.nic)?);
        // The client group first, as `DpdkShared::init` brings it up: it
        // keeps the pool name of a node with one NIC. Should the
        // replication group then fail, the client group drops here and
        // with it the EAL, last.
        let client = DpdkShared::init_on(
            Arc::clone(&eal),
            EalGroup::First,
            &config.nic,
            config.num_queues,
        )?;
        let replication = replication_nic
            .map(|nic| DpdkShared::init_on(eal, EalGroup::Further, nic, 1))
            .transpose()
            .map_err(|e| format!("replication NIC: {e}"))?;
        Ok(NodeNics {
            client,
            replication,
        })
    }
}

/// The EAL a node's resources sit on.
///
/// An enum rather than always borrowing a process-wide EAL: a node that
/// owns its EAL must still clean it up last, after its ports and pool, so
/// that a deployed node's teardown is the one it has always had.
pub(crate) enum NodeEal {
    /// Initialized for this node from its EAL arguments and cleaned up
    /// when the last of the node's NIC groups drops: one node per process,
    /// as deployed.
    Own(Eal),
    /// The process-wide EAL ([`Eal::init_process_wide`]), shared with the
    /// process's other nodes. Dropping the node leaves it be: it is never
    /// cleaned up, so the next node can still use it.
    ProcessWide(&'static Eal),
}

impl NodeEal {
    /// The EAL for a transport on `config`'s NIC: initialized from its EAL
    /// arguments, or, in a process that runs a process-wide EAL, that one,
    /// shared (its arguments must then be empty).
    pub(crate) fn acquire(config: &NicConfig) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(match Eal::process_wide() {
            None => {
                let eal_args: Vec<&str> = config.eal_args.iter().map(|s| s.as_str()).collect();
                NodeEal::Own(Eal::init(&eal_args)?)
            }
            Some(eal) => {
                // A failed process-wide init is final: report its error
                // rather than attempting a second `rte_eal_init`.
                let eal = eal?;
                crate::eal_sharing::check_shared_eal_args(&config.eal_args)?;
                NodeEal::ProcessWide(eal)
            }
        })
    }

    fn eal(&self) -> &Eal {
        match self {
            NodeEal::Own(eal) => eal,
            NodeEal::ProcessWide(eal) => eal,
        }
    }
}

/// Per-thread DPDK transport. Owns its own smoltcp Interface and
/// SocketSet. Each poll thread gets one of these.
///
/// All methods must be called from the owning poll thread.
pub struct DpdkTransport {
    device: DpdkDevice,
    iface: Interface,
    sockets: SocketSet<'static>,
    /// Every port the transport listens on, each with a backlog of
    /// listening sockets. Initialised with `config.listen_port` (no port
    /// for `from_shared_unlistening`); callers can add more via
    /// `add_listener_with_buffers` and `add_client_listener`.
    /// `check_listener` moves every connection whose handshake completed
    /// into `accepted` and replaces its socket with a fresh listening one,
    /// so the port keeps taking handshakes meanwhile.
    listeners: Listeners,
    accepted: Vec<AcceptedConnection>,
    /// Per-connection TX buffers, indexed by `SocketHandle::index()`.
    /// Dense `Vec<Option<_>>` instead of a HashMap — HashMap hashing
    /// showed up prominently on the DPDK poll core under throughput.
    /// `None` = slot free (no socket or no pending TX). Uses a cursor
    /// inside TxQueue to avoid O(n) drain on partial sends.
    tx_queues: Vec<Option<TxQueue>>,
    /// Per-connection TX queue limit, indexed by `SocketHandle::index()`.
    /// Defaults to `MAX_TX_QUEUE_SIZE`; overridden for replication sockets
    /// via `add_listener_with_buffers`.
    tx_queue_limits: Vec<usize>,
    /// The stack's monotonic clock, read into `cached_timestamp`.
    clock: StackClock,
    /// Cached smoltcp timestamp. Refreshed periodically, not every poll.
    cached_timestamp: Instant,
    /// Poll iteration counter for timestamp refresh.
    poll_count: u32,
    /// Total pending TX bytes across all connections. Avoids iterating
    /// tx_queues.values().any() on every poll cycle.
    pending_tx_bytes: usize,
    /// Statically configured gateway, kept so `poll` can re-seed it before
    /// its neighbor entry expires. `None` when the deployment is
    /// single-subnet (no gateway configured) — nothing to refresh then.
    /// A tuple rather than two fields: the IP is useless without the MAC,
    /// and holding them together makes half-configured unrepresentable.
    gateway: Option<(Ipv4Addr, [u8; 6])>,
    /// When the gateway was last seeded. Only read when `gateway` is set.
    gateway_seeded_at: Instant,
    /// Operator-supplied MAC for the peer this transport dials out to,
    /// carried over from `NicConfig` so an outbound connect can seed the
    /// neighbour cache without the caller having to thread config down.
    /// `None` = fall back to the derived convention.
    peer_mac: Option<[u8; 6]>,
    /// Shared EAL/mempool/ports. MUST be the last field so it drops LAST:
    /// Rust drops fields in declaration order, and `device`/`sockets`/
    /// `tx_queues` return their in-flight mbufs to the mempool when they
    /// drop. Dropping `_shared` first (when this is the last `Arc` holder)
    /// would free the mempool and run `rte_eal_cleanup` while those mbufs
    /// are still outstanding — a use-after-free that segfaults on any clean
    /// teardown (e.g. an error-exit from the receiver loop, or graceful
    /// shutdown). Keep it pinned to the end.
    _shared: Arc<DpdkShared>,
}

/// How a connection's received bytes reach its reader.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RxPath {
    /// Held in the received mbufs, read through `recv_into_vec`: the
    /// node's sockets, polled by [`DpdkTransport::poll`].
    ZeroCopy,
    /// Copied into the socket's receive buffer, read with `recv_slice`:
    /// the client NIC handle's streams, polled by
    /// [`DpdkTransport::poll_copying`]. A socket on this path registers no
    /// zero-copy callbacks, so it must never be fed by `poll`, whose
    /// zero-copy ingress would hold its data where `recv_slice` cannot see
    /// it.
    Copying,
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
    /// Initialize the node's client NIC group: EAL, mempool, ports.
    /// Call once before spawning poll threads.
    ///
    /// EAL is initialized here from `config.nic.eal_args`, and cleaned up
    /// when the resources drop — unless the process runs a process-wide EAL
    /// ([`Eal::init_process_wide`]), which the node then shares: its EAL
    /// arguments must be empty, its ports must be its own, and dropping it
    /// leaves EAL running for the process's other nodes.
    ///
    /// The client NIC only, whatever `config.replication` says:
    /// [`NodeNics::init`] brings up a replication NIC of its own beside it.
    pub fn init(config: &DpdkConfig) -> Result<Arc<Self>, Box<dyn std::error::Error>> {
        Self::init_on(
            Arc::new(NodeEal::acquire(&config.nic)?),
            EalGroup::First,
            &config.nic,
            config.num_queues,
        )
    }

    /// The NIC group for `config` with `num_queues` queue pairs, on an EAL
    /// already acquired: shared with the other groups on it, and taken
    /// apart from the group so that a caller can tell a failure to bring
    /// EAL up from a failure of the NIC on it (the client's NIC handle
    /// names its file prefix for the first). `group` says whether it is
    /// the EAL's first group, which decides its pool's name.
    ///
    /// On failure, whatever the group had set up is released before its
    /// share of `eal`: function parameters drop after the body's locals.
    pub(crate) fn init_on(
        eal: Arc<NodeEal>,
        group: EalGroup,
        config: &NicConfig,
        num_queues: u16,
    ) -> Result<Arc<Self>, Box<dyn std::error::Error>> {
        let port_count = eal.eal().port_count();
        if config.port_ids.is_empty() {
            return Err("DPDK transport requires at least one port id".into());
        }
        for &pid in &config.port_ids {
            if pid >= port_count {
                return Err(
                    format!("DPDK port {} not found (available: {})", pid, port_count).into(),
                );
            }
        }

        // Place the mempool on the NIC's own NUMA node. Every RX descriptor
        // and every transmitted frame is DMA'd to or from these buffers, so a
        // pool on the wrong node puts a cross-socket interconnect hop on the
        // whole packet path. `rte_eth_dev_socket_id` returns -1 when the
        // device's node is unknown, which is DPDK's `SOCKET_ID_ANY` and the
        // right value to pass through unchanged.
        //
        // A single pool cannot straddle nodes, so with ports on different
        // nodes the first port's node wins and the rest pay the hop.
        let socket_id = unsafe { ffi::rte_eth_dev_socket_id(config.port_ids[0]) };
        if let Some(&mismatched) = config.port_ids[1..]
            .iter()
            .find(|&&pid| unsafe { ffi::rte_eth_dev_socket_id(pid) } != socket_id)
        {
            tracing::warn!(
                port = mismatched,
                mempool_socket_id = socket_id,
                "DPDK ports span NUMA nodes; mempool follows the first port"
            );
        }

        // Scale mempool for number of queues and ports.
        let num_mbufs: u32 =
            8192 * (config.port_ids.len() as u32).max(1) * (num_queues as u32).max(1);
        let pool_name = crate::eal_sharing::mempool_name(
            matches!(*eal, NodeEal::ProcessWide(_)),
            group,
            config.port_ids[0],
        );
        let mempool = if config.mtu > 1500 {
            Mempool::create_for_mtu(&pool_name, num_mbufs, config.mtu as u16, socket_id)
        } else {
            Mempool::create_with_size(&pool_name, num_mbufs, socket_id)
        }
        .map_err(|e| format!("mempool {pool_name}: {e}"))?;

        // Configure and start all ports with N queue pairs.
        //
        // In bifurcated mode (peer_ip set), the port is configured with
        // `rte_flow` isolation enabled BEFORE configure, and a steering
        // rule is installed after start to capture the peer's traffic
        // into DPDK queue 0. Everything else stays with the kernel.
        let bifurcated = config.peer_ip.is_some();
        let mut ports = Vec::with_capacity(config.port_ids.len());
        let mut combined_offloads: Option<ChecksumOffloads> = None;
        for &pid in &config.port_ids {
            // Each failure names its port: on an LACP bond, which member
            // failed is the first thing to know.
            let mut port = Port::configure(pid, &mempool, config.vlan_id, num_queues, bifurcated)
                .map_err(|e| format!("port {pid} configure: {e}"))?;
            port.start().map_err(|e| format!("port {pid} start: {e}"))?;
            if let Some(peer) = config.peer_ip {
                port.install_src_ipv4_steering(peer)
                    .map_err(|e| format!("port {pid} steering rule for {peer}: {e}"))?;
            }
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
            _nic: OnEal::new(
                NicResources {
                    _ports: ports,
                    _mempool: mempool,
                },
                eal,
            ),
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
        let mut transport = Self::build(shared, &config.nic, queue_id)?;
        transport.listen_for_clients(config)?;
        tracing::info!(
            ip = %config.nic.ip_addr,
            port = config.listen_port,
            mac = ?shared.mac,
            queue_id,
            rx_buf = config.client_buffers.rx_buf_size,
            tx_buf = config.client_buffers.tx_buf_size,
            tx_queue = config.client_buffers.tx_queue_limit,
            "DPDK transport initialized"
        );
        Ok(transport)
    }

    /// Like [`Self::from_shared`], but listening on no port: for a
    /// transport that only dials out, such as a replica's link to its
    /// primary. The stack refuses (RST) a connection attempt to any port of
    /// it. Listeners can be added later: a promoted replica adds its client
    /// port to the transport it ran on with [`Self::add_client_listener`].
    pub fn from_shared_unlistening(
        shared: &Arc<DpdkShared>,
        config: &DpdkConfig,
        queue_id: u16,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let transport = Self::build(shared, &config.nic, queue_id)?;
        tracing::info!(
            ip = %config.nic.ip_addr,
            mac = ?shared.mac,
            queue_id,
            "DPDK transport initialized (no listener)"
        );
        Ok(transport)
    }

    /// The constructors' common body: the interface, listening on nothing.
    /// Takes the NIC's configuration rather than a node's whole
    /// [`DpdkConfig`]: the interface and its neighbour seeding need nothing
    /// of a node's.
    fn build(
        shared: &Arc<DpdkShared>,
        config: &NicConfig,
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
        let clock = StackClock::start();
        let now = clock.now();
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

        let sockets = SocketSet::new(Vec::with_capacity(MAX_CONNECTIONS));

        let mut transport = DpdkTransport {
            _shared: Arc::clone(shared),
            device,
            iface,
            sockets,
            // Accepted handles index the per-connection tables, sized for
            // MAX_CONNECTIONS.
            listeners: Listeners::new(MAX_CONNECTIONS),
            accepted: Vec::new(),
            // Pre-allocate all MAX_CONNECTIONS slots so index lookup is
            // always in-bounds. Each empty slot is a single discriminant
            // tag — no heap allocation per slot.
            tx_queues: (0..MAX_CONNECTIONS).map(|_| None).collect(),
            tx_queue_limits: vec![MAX_TX_QUEUE_SIZE; MAX_CONNECTIONS],
            clock,
            cached_timestamp: now,
            poll_count: 0,
            pending_tx_bytes: 0,
            gateway: config.gateway.zip(config.gateway_mac),
            gateway_seeded_at: now,
            peer_mac: config.peer_mac,
        };

        // In bifurcated L3 mode, ARP for the gateway would not match our
        // src-IP flow rule, so the gateway's ARP reply lands in the
        // kernel instead of smoltcp. Seed the gateway MAC statically
        // (operators source it from `ip neigh` on the host) so smoltcp
        // can route its first outbound packet without needing ARP.
        // `poll` re-seeds it on `GATEWAY_RESEED_INTERVAL_MS` from here.
        if let Some((gw, gw_mac)) = transport.gateway {
            transport.seed_neighbor(gw, gw_mac);
        }

        Ok(transport)
    }

    /// Open an outbound TCP connection to a remote endpoint.
    ///
    /// Creates a new smoltcp TCP socket, calls `socket.connect()` to
    /// initiate the TCP handshake, and returns the socket handle. The
    /// caller must poll the transport until `is_connected(handle)` returns
    /// true before sending data.
    ///
    /// # Errors
    ///
    /// As [`Self::connect_to_with_buffers`].
    pub fn connect_to(
        &mut self,
        remote_ip: std::net::Ipv4Addr,
        remote_port: u16,
        local_port: u16,
    ) -> Result<SocketHandle, tcp::ConnectError> {
        self.connect_to_with_buffers(
            remote_ip,
            remote_port,
            local_port,
            SOCKET_RX_BUF_SIZE,
            SOCKET_TX_BUF_SIZE,
            MAX_TX_QUEUE_SIZE,
            tcp::DEFAULT_DISPATCH_BURST_LIMIT,
        )
    }

    /// Like `connect_to` but with custom buffer sizes, TX queue limit and
    /// egress burst limit. Used for replication connections that need larger
    /// RX/TX buffers. See [`Self::add_listener_with_buffers`] for what
    /// `dispatch_burst_limit` controls.
    ///
    /// # Errors
    ///
    /// The stack refused to open the socket: an endpoint it cannot address
    /// (an unspecified address, a zero port) or, in principle, a socket
    /// not in its closed state, which a fresh one always is. No socket is
    /// added to the set then, so there is nothing to close. A caller that
    /// retries its dials treats it as a dial that failed.
    pub fn connect_to_with_buffers(
        &mut self,
        remote_ip: std::net::Ipv4Addr,
        remote_port: u16,
        local_port: u16,
        rx_buf_size: usize,
        tx_buf_size: usize,
        tx_queue_limit: usize,
        dispatch_burst_limit: usize,
    ) -> Result<SocketHandle, tcp::ConnectError> {
        self.open_connection(
            remote_ip,
            remote_port,
            local_port,
            rx_buf_size,
            tx_buf_size,
            tx_queue_limit,
            dispatch_burst_limit,
            RxPath::ZeroCopy,
        )
    }

    /// Like [`Self::connect_to_with_buffers`] for a socket read by copy
    /// ([`Self::tcp_socket`] and `recv_slice`), polled by
    /// [`Self::poll_copying`] and never by [`Self::poll`]: the client NIC
    /// handle's dial. Equal receive and transmit buffers and the default
    /// egress burst.
    pub(crate) fn connect_copying(
        &mut self,
        remote: std::net::SocketAddrV4,
        local_port: u16,
        buf_size: usize,
    ) -> Result<SocketHandle, tcp::ConnectError> {
        self.open_connection(
            *remote.ip(),
            remote.port(),
            local_port,
            buf_size,
            buf_size,
            MAX_TX_QUEUE_SIZE,
            tcp::DEFAULT_DISPATCH_BURST_LIMIT,
            RxPath::Copying,
        )
    }

    /// The transport's one connect: a tuned socket (see [`tune_socket`]),
    /// connected and added to the set.
    #[allow(clippy::too_many_arguments)] // the two public entry points' union
    fn open_connection(
        &mut self,
        remote_ip: std::net::Ipv4Addr,
        remote_port: u16,
        local_port: u16,
        rx_buf_size: usize,
        tx_buf_size: usize,
        tx_queue_limit: usize,
        dispatch_burst_limit: usize,
        rx_path: RxPath,
    ) -> Result<SocketHandle, tcp::ConnectError> {
        let rx_buf = tcp::SocketBuffer::new(vec![0u8; rx_buf_size]);
        let tx_buf = tcp::SocketBuffer::new(vec![0u8; tx_buf_size]);
        let mut socket = tcp::Socket::new(rx_buf, tx_buf);
        tune_socket(&mut socket);
        if rx_path == RxPath::ZeroCopy {
            socket.set_zero_copy_retain_fn(retain_mbuf);
            socket.set_zero_copy_release_fn(release_mbuf);
        }
        socket.set_dispatch_burst_limit(dispatch_burst_limit);

        let remote_addr = Ipv4Address::new(
            remote_ip.octets()[0],
            remote_ip.octets()[1],
            remote_ip.octets()[2],
            remote_ip.octets()[3],
        );
        let local_ip = self.iface.ipv4_addr().expect("interface has IPv4 address");
        socket.connect(
            self.iface.context(),
            (IpAddress::Ipv4(remote_addr), remote_port),
            (IpAddress::Ipv4(local_ip), local_port),
        )?;

        let handle = self.sockets.add(socket);
        if handle.index() < self.tx_queue_limits.len() {
            self.tx_queue_limits[handle.index()] = tx_queue_limit;
        }
        Ok(handle)
    }

    /// Check if a socket has completed the TCP handshake and is ready
    /// for data transfer (both send and receive directions open). A
    /// handle that has been removed (caller raced a `close`) reads as
    /// "not connected" — never panic on the caller's behalf.
    ///
    /// Unlike [`Self::is_active`], false once the peer has closed its
    /// half (its FIN), as soon as the bytes it sent before are read: the
    /// test for a link that needs both directions. See `link_up` in
    /// [`crate::peer_liveness`].
    pub fn is_connected(&mut self, handle: SocketHandle) -> bool {
        match self.sockets.try_get_mut::<tcp::Socket>(handle) {
            Some(socket) => peer_liveness::link_up(socket),
            None => {
                tracing::warn!(
                    handle = ?handle,
                    site = "is_connected",
                    "DPDK: stale SocketHandle observed"
                );
                false
            }
        }
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
        // smoltcp only needs ms-precision for TCP retransmit/keepalive timers,
        // but a monotonic clock (see `StackClock`).
        self.poll_count = self.poll_count.wrapping_add(1);
        if self.poll_count.is_multiple_of(TIMESTAMP_REFRESH_INTERVAL) {
            self.cached_timestamp = self.clock.now();
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

        // Inbound frames refresh every peer we talk to, but never the
        // gateway (see GATEWAY_RESEED_INTERVAL_MS), so its entry is the
        // one that has to be re-seeded on a timer or off-subnet traffic
        // stops resolving 60 s after startup.
        if let Some((gw, gw_mac)) = self.gateway
            && gateway_reseed_due(self.cached_timestamp, self.gateway_seeded_at)
        {
            self.seed_neighbor(gw, gw_mac);
            self.gateway_seeded_at = self.cached_timestamp;
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

    /// One service step at the caller's time `now`, for the client NIC
    /// handle: poll the device, run the stack over every socket in the
    /// set (ingress, then egress and its timers), and flush the device's
    /// transmit queue. Every call does all three, since a stream's sends
    /// go straight into its socket, with nothing queued above it for
    /// [`Self::poll`]'s idle check to see.
    ///
    /// Received bytes are copied into the sockets' receive buffers
    /// ([`RxPath::Copying`]), as the shm-proxy and the exchange's bench
    /// have always driven the stack. The gateway is re-seeded on its
    /// interval as in `poll`; neighbours are not learned from received
    /// frames, which a dialing client does not need: it seeds the one it
    /// dials.
    pub(crate) fn poll_copying(&mut self, now: Instant) {
        self.cached_timestamp = now;
        if let Some((gw, gw_mac)) = self.gateway
            && gateway_reseed_due(now, self.gateway_seeded_at)
        {
            self.seed_neighbor(gw, gw_mac);
            self.gateway_seeded_at = now;
        }
        self.device.poll_rx();
        self.iface.poll(now, &mut self.device, &mut self.sockets);
        self.device.flush_tx();
    }

    /// The stack's time as of the latest poll.
    pub(crate) fn stack_now(&self) -> Instant {
        self.cached_timestamp
    }

    /// The MAC address of the transport's first port.
    pub(crate) fn mac(&self) -> [u8; 6] {
        self._shared.mac
    }

    /// The TCP socket behind `handle`, if it is still in the set.
    pub(crate) fn tcp_socket(&mut self, handle: SocketHandle) -> Option<&mut tcp::Socket<'static>> {
        self.sockets.try_get_mut::<tcp::Socket>(handle)
    }

    fn check_listener(&mut self) {
        // Every port's completed handshakes, each socket replaced by a
        // fresh listening one (see `Listeners::accept`). The accepted
        // socket gets the zero-copy callbacks before it processes any
        // data segment, and its port's TX queue limit.
        let Self {
            listeners,
            iface,
            sockets,
            tx_queue_limits,
            accepted,
            ..
        } = self;
        listeners.accept(iface, sockets, |socket, conn| {
            socket.set_zero_copy_retain_fn(retain_mbuf);
            socket.set_zero_copy_release_fn(release_mbuf);
            // In bounds: the backlog places no socket beyond
            // MAX_CONNECTIONS, the table's length.
            if let Some(limit) = tx_queue_limits.get_mut(conn.handle.index()) {
                *limit = conn.config.tx_queue_limit;
            }
            accepted.push(AcceptedConnection {
                handle: conn.handle,
                peer: conn.peer,
                listen_port: conn.config.port,
            });
            tracing::debug!(
                peer = %conn.peer,
                listen_port = conn.config.port,
                "DPDK: TCP connection accepted"
            );
        });
    }

    /// Add another TCP port to listen on, sharing the same DPDK NIC and
    /// smoltcp interface, with the given sizes for the sockets accepted on
    /// it. Returns immediately; the new listener is active on the next
    /// `poll()`. Used when a single transport handles multiple distinct
    /// services (clients and replication) without separate queues or
    /// threads: replication needs larger TX buffers to keep up with journal
    /// batch throughput. The client port goes through
    /// [`Self::add_client_listener`] instead, which takes its sizes from the
    /// node's configuration.
    ///
    /// The port keeps a backlog of sockets listening, so that it takes
    /// several handshakes at once, and each of them holds these buffers
    /// and a slot of the socket set from the moment the listener is added.
    /// The larger the buffers, the shorter the backlog: it is cut to keep
    /// what the listening sockets hold bounded, down to two sockets.
    ///
    /// `dispatch_burst_limit` caps how many data segments a socket accepted
    /// here may dispatch in one egress pass. The stack repeats the pass over
    /// every socket until one sends nothing, so the low default (tuned to keep
    /// one client from delaying its peers under fan-in) makes a large
    /// single-flow burst cost a pass per few segments. Bulk flows want a value
    /// covering a whole in-flight window in one pass.
    pub fn add_listener_with_buffers(
        &mut self,
        port: u16,
        rx_buf_size: usize,
        tx_buf_size: usize,
        tx_queue_limit: usize,
        dispatch_burst_limit: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.listen(ListenerConfig {
            port,
            rx_buf_size,
            tx_buf_size,
            tx_queue_limit,
            dispatch_burst_limit,
        })?;
        tracing::info!(port, "DPDK transport: added listener");
        Ok(())
    }

    /// Add the node's client listener, on `config.listen_port` with
    /// `config.client_buffers`: what [`Self::from_shared`] gives a primary
    /// at boot, for a transport built without it (a replica's, on
    /// promotion).
    pub fn add_client_listener(
        &mut self,
        config: &DpdkConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.listen_for_clients(config)?;
        tracing::info!(
            port = config.listen_port,
            rx_buf = config.client_buffers.rx_buf_size,
            tx_buf = config.client_buffers.tx_buf_size,
            tx_queue = config.client_buffers.tx_queue_limit,
            "DPDK transport: added client listener"
        );
        Ok(())
    }

    /// The client listener, sized by `config.client_buffers`, for
    /// [`Self::from_shared`] and [`Self::add_client_listener`] alike, so
    /// that a promoted replica's clients get what a booted primary's do.
    fn listen_for_clients(
        &mut self,
        config: &DpdkConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let SocketBuffers {
            rx_buf_size,
            tx_buf_size,
            tx_queue_limit,
        } = config.client_buffers;
        self.listen(ListenerConfig {
            port: config.listen_port,
            rx_buf_size,
            tx_buf_size,
            tx_queue_limit,
            // Client port: keep the fan-in default so one client's burst
            // cannot delay its peers within an egress pass.
            dispatch_burst_limit: tcp::DEFAULT_DISPATCH_BURST_LIMIT,
        })
    }

    /// The listeners' common body: listen on `config.port`, with the
    /// backlog its buffers allow (see `listen_backlog`). Each listening
    /// socket becomes an accepted connection, so it is sized as one;
    /// `check_listener` re-listens with the same sizes.
    fn listen(&mut self, config: ListenerConfig) -> Result<(), Box<dyn std::error::Error>> {
        self.listeners.add(&mut self.sockets, config)?;
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
    /// A stale handle (closed/removed concurrently) reads as "nothing
    /// available" — never panic on the caller's behalf.
    pub fn recv(&mut self, handle: SocketHandle, buf: &mut [u8]) -> usize {
        let Some(socket) = self.sockets.try_get_mut::<tcp::Socket>(handle) else {
            tracing::warn!(handle = ?handle, site = "recv", "DPDK: stale SocketHandle observed");
            return 0;
        };
        if !socket.can_recv() {
            return 0;
        }
        socket.recv_slice(buf).unwrap_or(0)
    }

    /// Append available data from a connection directly into a Vec.
    /// Uses zero-copy RX: reads directly from DPDK mbuf memory, then
    /// releases the mbuf via the registered release callback. Stale
    /// handle → 0 bytes appended, no panic.
    pub fn recv_into_vec(&mut self, handle: SocketHandle, dest: &mut Vec<u8>) -> usize {
        let Some(socket) = self.sockets.try_get_mut::<tcp::Socket>(handle) else {
            tracing::warn!(handle = ?handle, site = "recv_into_vec", "DPDK: stale SocketHandle observed");
            return 0;
        };
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
        let limit = self.tx_queue_limits[handle.index()];
        let slot = &mut self.tx_queues[handle.index()];
        let queue = slot.get_or_insert_with(TxQueue::new);
        if queue.queued_bytes() + data.len() > limit {
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

    /// Whether everything queued for this connection has been delivered:
    /// the TX queue is empty and the TCP send buffer holds nothing, i.e.
    /// every byte handed to the socket has been acknowledged by the peer.
    ///
    /// `close` aborts the socket and discards both, so a caller that must get a
    /// last frame to the peer before closing waits on this first. A stale
    /// handle has nothing left to deliver and reads as drained.
    pub fn tx_drained(&mut self, handle: SocketHandle) -> bool {
        if self.tx_queue_bytes(handle) != 0 {
            return false;
        }
        self.sockets
            .try_get_mut::<tcp::Socket>(handle)
            .is_none_or(|socket| socket.send_queue() == 0)
    }

    /// Maximum bytes that `queue_send` will accept for this connection
    /// before returning `false`. Replication sockets get a higher limit
    /// via `add_listener_with_buffers`.
    pub fn max_tx_queue_size(&self, handle: SocketHandle) -> usize {
        self.tx_queue_limits[handle.index()]
    }

    /// Check if a connection is still open. A stale handle reads as
    /// "not active" — never panic on the caller's behalf.
    pub fn is_active(&mut self, handle: SocketHandle) -> bool {
        match self.sockets.try_get_mut::<tcp::Socket>(handle) {
            Some(socket) => socket.is_active(),
            None => {
                tracing::warn!(
                    handle = ?handle,
                    site = "is_active",
                    "DPDK: stale SocketHandle observed"
                );
                false
            }
        }
    }

    /// Whether the peer has closed its side of an established connection:
    /// its FIN has arrived and the socket is half-closed (close-wait),
    /// which the stack still counts as active. With nothing left to read,
    /// the connection is over, as it is on kernel TCP at EOF. A stale
    /// handle reads as closed.
    pub fn peer_closed(&mut self, handle: SocketHandle) -> bool {
        match self.sockets.try_get_mut::<tcp::Socket>(handle) {
            Some(socket) => !socket.may_recv(),
            None => true,
        }
    }

    /// Close a connection (abort, discarding unsent data; no FIN) and
    /// remove it from the socket set.
    /// The socket is fully removed so its tuple doesn't block future
    /// connections from the same source port. Idempotent: a second
    /// `close` for a handle that has already been removed is a no-op,
    /// the TX queue (if any) is still drained for accounting.
    pub fn close(&mut self, handle: SocketHandle) {
        if let Some(socket) = self.sockets.try_get_mut::<tcp::Socket>(handle) {
            socket.abort();
            self.sockets.remove(handle);
            // The interface indexes established connections by 4-tuple for
            // O(1) segment demultiplexing, and cannot see a `SocketSet`
            // removal. Left in place, the entry holds its slot for the life of
            // the process; once the index stops accepting inserts, every later
            // connection is unindexed for its whole life and each of its
            // segments costs a linear scan over all sockets.
            self.iface.forget_tcp_socket(handle);
        } else {
            tracing::warn!(
                handle = ?handle,
                site = "close",
                "DPDK: close called on already-removed handle"
            );
        }
        if let Some(q) = self.tx_queues[handle.index()].take() {
            self.pending_tx_bytes -= q.queued_bytes();
        }
    }

    /// Close a connection like [`Self::close`], but tell the peer: the
    /// socket's RST goes out before it is removed, so the peer's stack
    /// ends the connection at once instead of waiting on its own
    /// deadline. For links whose peer acts on the end of the session
    /// (replication); unsent data is discarded, so a caller with a last
    /// frame to deliver waits on [`Self::tx_drained`] first.
    ///
    /// Idempotent like `close`. See `abort_announced` in
    /// [`crate::peer_liveness`].
    pub fn reset(&mut self, handle: SocketHandle) {
        // Whatever is still queued above the socket would never be sent:
        // the socket is aborted below. Drop it first so the egress pass
        // does not hand it to the socket.
        if let Some(q) = self.tx_queues[handle.index()].take() {
            self.pending_tx_bytes -= q.queued_bytes();
        }
        // Abort, one egress pass, flush, and only then remove (and forget
        // in the interface's index, as `close` does): see
        // `reset_announced` for why the order matters.
        let removed = peer_liveness::reset_announced(
            &mut self.iface,
            &mut self.device,
            &mut self.sockets,
            handle,
            self.cached_timestamp,
            DpdkDevice::flush_tx,
        );
        if !removed {
            tracing::warn!(
                handle = ?handle,
                site = "reset",
                "DPDK: reset called on already-removed handle"
            );
        }
    }

    /// Arm `liveness` on an established connection: probe it while idle,
    /// and reset it once the peer has been silent for the rule's timeout,
    /// after which [`Self::is_active`] reads false. A stale handle is
    /// ignored (it has nothing left to watch).
    pub fn set_peer_liveness(&mut self, handle: SocketHandle, liveness: &PeerLiveness) {
        if let Some(socket) = self.sockets.try_get_mut::<tcp::Socket>(handle) {
            liveness.apply(socket);
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
        let frame = crate::arp::gratuitous_request(our_mac, our_ip);

        if self.device.send_raw_frame(&frame) {
            tracing::info!(
                mac = ?our_mac,
                ip = %our_ip,
                "sent gratuitous ARP (switch MAC learning)"
            );
        } else {
            // Best effort: the switch also learns from our first outbound
            // unicast, and `send_raw_frame` has already logged the shortage.
            tracing::warn!(
                mac = ?our_mac,
                ip = %our_ip,
                "gratuitous ARP not sent; switch MAC learning deferred to first outbound frame"
            );
        }
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

        // An ARP reply from the peer, addressed to us: sender = (mac, ip),
        // target = (our_mac, our_ip).
        let frame = crate::arp::reply(our_mac, our_ip, mac, ip);

        self.device.inject_rx(frame.to_vec());

        tracing::debug!(
            peer_ip = %ip,
            peer_mac = ?mac,
            "seeded neighbor cache with ARP reply"
        );
    }

    /// Seed the neighbour cache for a peer this node dials out to,
    /// preferring the configured MAC over the derived fallback.
    ///
    /// Returns which source was used so the caller can log it. That
    /// distinction is worth surfacing: when the address is wrong there is
    /// no error to observe, only a connect that never completes.
    pub fn seed_peer_neighbor(&mut self, peer_ip: Ipv4Addr) -> crate::mac::PeerMacSource {
        let (mac, source) = crate::mac::resolve_peer_mac(self.peer_mac, peer_ip);
        self.seed_neighbor(peer_ip, mac);
        source
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

#[cfg(test)]
mod tests {
    use super::*;
    use smoltcp::time::Duration;

    #[test]
    fn reseed_is_not_due_before_the_interval_elapses() {
        let seeded = Instant::from_millis(1_000);
        assert!(!gateway_reseed_due(seeded, seeded));
        assert!(!gateway_reseed_due(
            seeded + Duration::from_millis(GATEWAY_RESEED_INTERVAL_MS as u64 - 1),
            seeded
        ));
    }

    #[test]
    fn reseed_is_due_once_the_interval_elapses() {
        let seeded = Instant::from_millis(1_000);
        assert!(gateway_reseed_due(
            seeded + Duration::from_millis(GATEWAY_RESEED_INTERVAL_MS as u64),
            seeded
        ));
        assert!(gateway_reseed_due(
            seeded + Duration::from_millis(600_000),
            seeded
        ));
    }

    /// The poll loop samples its timestamp from the wall clock, so a step
    /// backwards must re-seed rather than park the entry until the clock
    /// catches up — which for a large step outlasts the 60 s lifetime.
    #[test]
    fn reseed_is_due_when_the_clock_steps_backwards() {
        let seeded = Instant::from_millis(600_000);
        assert!(gateway_reseed_due(Instant::from_millis(599_999), seeded));
        assert!(gateway_reseed_due(Instant::from_millis(0), seeded));
    }

    /// A replication NIC that cannot sit beside the client NIC is refused
    /// before EAL is touched: this test initializes no EAL, so an attempt
    /// to would fail it with EAL's error rather than this one.
    #[test]
    fn a_replication_nic_that_cannot_share_the_eal_is_refused_before_eal_init() {
        let client = NicConfig {
            eal_args: vec!["--this-is-not-an-eal-flag".into()],
            port_ids: vec![0],
            ..NicConfig::default()
        };
        let config = |replication: NicConfig| DpdkConfig {
            nic: client.clone(),
            replication: ReplicationNic::Port(replication),
            ..DpdkConfig::default()
        };
        let refused = |config: DpdkConfig| match NodeNics::init(&config) {
            Ok(_) => panic!("refused"),
            Err(e) => e.to_string(),
        };

        let shared_port = refused(config(NicConfig {
            port_ids: vec![1, 0],
            ..client.clone()
        }));
        assert!(
            shared_port.contains("port 0 is in both NICs"),
            "{shared_port}"
        );

        let own_eal_args = refused(config(NicConfig {
            eal_args: vec!["-l".into(), "2".into()],
            port_ids: vec![1],
            ..client.clone()
        }));
        assert!(own_eal_args.contains("(-l 2)"), "{own_eal_args}");
    }
}
