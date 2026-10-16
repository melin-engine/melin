//! Start nodes for an integration test on whichever transport the build
//! selects, through one entry point, so that one test body runs on both.
//!
//! - **Kernel TCP** (the default). Exactly what the tests did before this
//!   crate: bind the client listener (on `127.0.0.1:0`, so the kernel
//!   picks the port, or on an address from [`addrs`]), run the node with
//!   `server::run_with_listener` on a thread, and stop it through its
//!   shutdown flag.
//! - **DPDK** (the `dpdk` feature). The test process must run under
//!   `scripts/dpdk/netns-runner.sh`, which builds a bridge with one veth
//!   per node slot, in namespaces of the process's own, and publishes the
//!   layout in the environment. The first node initializes one EAL for
//!   the whole process; each node then gets a slot (an interface, an IP
//!   and an af_packet port of its own) and runs on it with
//!   `server::run_with_shutdown`. A process can so run a cluster, and
//!   restart a node, as on kernel TCP. A slot is released when its node
//!   is stopped or joined.
//!
//! A cluster test takes each node's addresses from [`addrs`] before
//! starting any of them, so that it can point a replica at its primary
//! (`replica_of`, `replication_bind`) the same way on both transports; on
//! DPDK the launcher fills in what the replica needs on top (the
//! primary's MAC). Endpoints that are kernel TCP by design (health,
//! admin, events, raft) are left as the test configures them: under the
//! runner they bind on the namespace's own loopback.
//!
//! Without the runner, a DPDK build fails the first node it is asked for,
//! saying how to run it.
//!
//! A test whose client runs on DPDK too opens the client's NIC with
//! `open_dpdk_client` (DPDK only): an af_packet device on the runner's
//! client pair, a veth of its own apart from the node slots, on the same
//! process-wide EAL as the nodes. The peer MAC is the test's to pass
//! (`mac_of`), as a replica's is the launcher's.
//!
//! A test that checks the context a node's threads start in uses
//! [`sched`]: the node is started from a thread at `SCHED_BATCH`, and its
//! placed threads must not be found at it.
//!
//! A test that wants its nodes' log in its report, when it fails, calls
//! [`logs::capture`].
//!
//! A test reads a running node as an operator does, through its health
//! gauges and admin commands, with [`probe`].
//!
//! See `docs/internal/dpdk-testing.md`.

#[cfg(any(test, feature = "dpdk"))]
mod layout;
pub mod logs;
pub mod probe;
pub mod sched;

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use melin_app::Application;
use melin_app::decoder::RequestDecoder;
use melin_app::encoder::ResponseEncoder;
use melin_server_runtime::StartupEvents;
use melin_server_runtime::server::ServerConfig;

/// How long a test should give a node to start serving, from [`start`]
/// to its first authenticated client.
///
/// A DPDK node starts its port before it answers ARP, and the first node
/// of a process initializes EAL too, which on a shared CI runner takes
/// far longer than binding a socket.
pub const STARTUP_LIMIT: Duration = if cfg!(feature = "dpdk") {
    Duration::from_secs(45)
} else {
    Duration::from_secs(10)
};

/// Where a node is reached, decided before it starts: see [`addrs`].
#[derive(Debug, Clone, Copy)]
pub struct Addrs {
    client: SocketAddr,
    replication: SocketAddr,
    /// The runner's node slot these addresses are on.
    #[cfg(feature = "dpdk")]
    slot: usize,
}

impl Addrs {
    /// Where clients connect: [`Node::addr`] once the node is started.
    pub fn client(&self) -> SocketAddr {
        self.client
    }

    /// Where the node listens for replicas, if it is given
    /// `replication_bind`, and so what its replicas take as `replica_of`.
    pub fn replication(&self) -> SocketAddr {
        self.replication
    }
}

/// The addresses of node `slot` of this test (`0` for the first node, `1`
/// for the second, ...), for [`start_at`].
///
/// - Kernel TCP: two loopback addresses from `free_addr`, the test's own
///   port allocator (`|| free_addr(PORT_BASE)`, with
///   `melin_transport_core::test_ports::free_addr` and the test file's
///   port range); `slot` plays no part, and every call returns new ports.
/// - DPDK: the runner's slot `slot`, its IP with the fixed client and
///   replication ports; `free_addr` is not called. The same slot always
///   has the same addresses, so a node restarted on its slot keeps them,
///   and two nodes cannot share one at a time.
///
/// The allocator is the caller's rather than this crate's so that the
/// test-only feature it needs stays in the tests' dev-dependencies: as a
/// dependency of this crate it would reach every workspace-wide build.
pub fn addrs(slot: usize, free_addr: impl FnMut() -> SocketAddr) -> Addrs {
    #[cfg(not(feature = "dpdk"))]
    {
        let _ = slot;
        let mut free_addr = free_addr;
        Addrs {
            client: free_addr(),
            replication: free_addr(),
        }
    }
    #[cfg(feature = "dpdk")]
    {
        drop(free_addr);
        dpdk::addrs(slot)
    }
}

/// The address a test's own sockets use to reach the nodes, and the one
/// a socket the nodes must reach should be bound on (a scripted peer, for
/// one): `127.0.0.1` on kernel TCP, the kernel side of the runner's
/// network on DPDK.
pub fn local_ip() -> IpAddr {
    #[cfg(not(feature = "dpdk"))]
    {
        IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
    }
    #[cfg(feature = "dpdk")]
    {
        IpAddr::V4(dpdk::layout().client_ip)
    }
}

/// Where a client on DPDK sits on the runner's network: its own veth
/// pair, apart from the node slots ([`dpdk_client`]).
#[cfg(feature = "dpdk")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DpdkClient {
    /// The IP the client's NIC takes ([`open_dpdk_client`] sets it).
    pub ip: std::net::Ipv4Addr,
    /// The MAC of the client's interface, which its DPDK port takes.
    pub mac: [u8; 6],
    /// The width of the runner's network, which the client's NIC takes
    /// with its IP. `u8`, as `NicConfig::prefix_len` is.
    pub prefix_len: u8,
}

/// The runner's seat for a client on DPDK. Panics when the process is not
/// under the runner, or under one that builds no such seat.
#[cfg(feature = "dpdk")]
pub fn dpdk_client() -> DpdkClient {
    dpdk::client()
}

/// The MAC behind `addr`'s IP on the runner's network: a node's (from
/// [`Node::addr`] or [`Addrs::client`]), the kernel side's, or the DPDK
/// client's. What a client on DPDK passes as its peer MAC
/// (`NicConfig::peer_mac`), as the launcher passes a replica its
/// primary's: the runner's MACs are deliberately not the `02:00:<ip>`
/// convention a dialer falls back to without one, so a client reaches its
/// node only if the MAC was passed through. Panics for an address off the
/// runner's network.
#[cfg(feature = "dpdk")]
pub fn mac_of(addr: SocketAddr) -> [u8; 6] {
    dpdk::mac_of(addr)
}

/// Open the client's DPDK NIC on the runner's client pair, for a test
/// whose client runs on DPDK beside its nodes: dial on it with
/// `melin_client::StreamConnection::connect_dpdk_on`, or drive streams on
/// it directly.
///
/// The NIC is on the process-wide EAL the nodes share (initialized here if
/// no node has done it yet), on an af_packet device of its own attached
/// for the client's interface, under a name no node slot's device takes.
/// Built as a node's is: `config.port_ids` becomes the device's port, and
/// `config.ip_addr` and `config.prefix_len` the runner's for the client;
/// everything else is the test's, the peer MAC first ([`mac_of`]). The
/// handle names its mbuf pool after its port, as a node on the shared EAL
/// does, so it never collides with a node's.
///
/// One per process: the device stays attached for the life of the
/// process, as the EAL does, so a test keeps the handle and dials again on
/// it (lend it, `&mut`) rather than opening another. A plain
/// [`NicHandle`](melin_dpdk::NicHandle), so a test driving several streams
/// on it uses it as any client would.
///
/// Panics when `config.eal_args` is not empty (the shared EAL is set up
/// already, and a handle on it refuses arguments of its own), when called
/// a second time in the process, after [`dpdk_client_process_eal_args`]
/// has handed the device to a child, when the process is not under the
/// runner, or when the NIC cannot be brought up; each is a failed test.
#[cfg(feature = "dpdk")]
pub fn open_dpdk_client(config: melin_dpdk::NicConfig) -> melin_dpdk::NicHandle {
    dpdk::open_client(config)
}

/// EAL arguments for a process of its own on the runner's client pair,
/// for a test whose DPDK client is a separate binary it runs as a child
/// (the shm-proxy): the launcher's (no hugepages, no PCI scan, the main
/// lcore on the first CPU this process may use), the client's af_packet
/// device as the process's one port (port 0), and `file_prefix`, which
/// keeps that EAL's runtime files apart from this process's own. The
/// child takes [`dpdk_client`]'s IP, prefix and MAC with them, and the
/// node's MAC from [`mac_of`].
///
/// The child owns the client pair's device in its process, so this
/// process must not open it too: panics if [`open_dpdk_client`] has been
/// called, and makes a later call to it panic; panics too when the process
/// is not under the runner.
#[cfg(feature = "dpdk")]
pub fn dpdk_client_process_eal_args(file_prefix: &str) -> Vec<String> {
    dpdk::client_process_eal_args(file_prefix)
}

/// A node running on its own thread of this process.
///
/// Stopped when dropped, if [`Node::stop`] or [`Node::join`] has not ended
/// it first: a test that fails mid-way does not leave its node running
/// behind it, where later tests of the same process would find its threads
/// (a test that reads every thread of the process) or, on DPDK, its runner
/// slot still held. The drop never panics, so it cannot turn a failing
/// test's panic into a second one: it asks the node to stop, waits for it
/// as long as a clean shutdown may take at most, and ignores what it
/// returns. A node that
/// does not stop in time is left running, and on DPDK keeps its slot.
/// [`Node::stop`] is still the way to end a node whose outcome the test
/// checks.
pub struct Node {
    /// Where clients connect: `127.0.0.1` and a kernel-assigned port, or
    /// the address [`start_at`] was given.
    addr: SocketAddr,
    /// The node's thread. `Some` until [`Node::stop`], [`Node::join`] or
    /// the drop takes it, so that each of them ends the node once.
    thread: Option<JoinHandle<Result<(), String>>>,
    /// The node's shutdown flag: set by [`Node::stop`], or by the node
    /// itself when it fences.
    shutdown: Arc<AtomicBool>,
    /// The runner slot the node holds until it is stopped or joined.
    #[cfg(feature = "dpdk")]
    slot: usize,
}

impl Node {
    /// The address clients connect to.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Whether the node has returned, on its own or because it was told
    /// to stop.
    pub fn is_finished(&self) -> bool {
        self.thread().is_finished()
    }

    /// The node's thread, held until the node is ended.
    fn thread(&self) -> &JoinHandle<Result<(), String>> {
        self.thread
            .as_ref()
            .expect("held until stop, join or the drop takes it")
    }

    /// Take the node's thread, to end the node.
    fn take_thread(&mut self) -> JoinHandle<Result<(), String>> {
        self.thread
            .take()
            .expect("held until stop, join or the drop takes it")
    }

    /// Whether the node's shutdown flag is set. The test sets it only
    /// through [`Node::stop`], so before that it being set means the node
    /// stopped itself (a fenced node does).
    pub fn shutdown_requested(&self) -> bool {
        self.shutdown.load(Ordering::Relaxed)
    }

    /// Wait for the node to return on its own, and return what it
    /// returned. For a node that is expected to fail, or to stop itself;
    /// [`Node::stop`] is the way to end one that is serving. Panics if the
    /// node's thread panicked.
    pub fn join(mut self) -> Result<(), String> {
        let result = self.take_thread().join().expect("node thread panicked");
        #[cfg(feature = "dpdk")]
        dpdk::release(self.slot);
        result
    }
}

/// How long a node may take to stop once asked: its clean shutdown on the
/// way out of a test, or its drop.
const SHUTDOWN_LIMIT: Duration = Duration::from_secs(20);

impl Drop for Node {
    fn drop(&mut self) {
        // Ended already, by `stop` or `join`.
        let Some(thread) = self.thread.take() else {
            return;
        };
        self.request_stop();
        let deadline = std::time::Instant::now() + SHUTDOWN_LIMIT;
        while !thread.is_finished() {
            if std::time::Instant::now() >= deadline {
                // Not a panic: this may run during a failing test's unwind.
                eprintln!(
                    "melin-test-node: a node dropped without being stopped did not stop within \
                     {SHUTDOWN_LIMIT:?}; left running"
                );
                return;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        // Dropped result: the node was dropped, not stopped, so nothing
        // checks how it ended; a failing test's own panic is the report.
        let _ = thread.join();
        #[cfg(feature = "dpdk")]
        if let Err(e) = dpdk::try_release(self.slot) {
            eprintln!("melin-test-node: {e}; the slot stays held");
        }
    }
}

/// Most connections a test node accepts unless its test says otherwise:
/// far more than any test opens, and small enough that the node's
/// io_uring rings stay small (see [`config`]).
pub const MAX_CONNECTIONS: u64 = 64;

/// The configuration a test starts its nodes from: production defaults
/// ([`ServerConfig::default`]), sized for a test rather than a
/// deployment. Override fields on top of it as with the default:
/// `ServerConfig { standalone: true, ..melin_test_node::config() }`.
///
/// What differs, and why — a parallel suite runs many nodes on one host,
/// as one user, and the kernel charges both of these against that user's
/// locked-memory limit (`RLIMIT_MEMLOCK`, a few megabytes under systemd's
/// default):
///
/// - **`max_connections`** is [`MAX_CONNECTIONS`]. Each node's reader and
///   response stage size their io_uring rings from it, and ring memory
///   counts against the limit for every process of the user, until the
///   kernel has finished tearing a ring down, which happens asynchronously
///   — the rings of nodes that just stopped still count while the next
///   ones start. Rings sized for the production cap take a sizeable share
///   of the default limit each, so a handful of parallel nodes exhausted
///   it; sized for a test's few clients they are a couple of pages each,
///   and dozens of nodes fit.
/// - **`no_mlock`** is set: a node locks all of its memory by default,
///   which against the default limit fails (and warns) on every node.
///   [`start`] and [`start_at`] set it whatever the config says.
///
/// A test that exercises the connection cap sets its own
/// `max_connections`.
pub fn config() -> ServerConfig {
    ServerConfig {
        max_connections: MAX_CONNECTIONS,
        no_mlock: true,
        ..ServerConfig::default()
    }
}

/// Start a node running application `A` with `config`, on this build's
/// transport, wherever it is free to: on kernel TCP a kernel-assigned
/// port, on DPDK the first free slot. Returns as soon as its thread is
/// running: a client retries until the node serves (within
/// [`STARTUP_LIMIT`]).
///
/// Build `config` from [`config`]. `config.bind` is overwritten with the
/// address the transport gives the node; on DPDK so are the `dpdk_*`
/// fields. `no_mlock` is set: a test node never locks its memory (see
/// [`config`]). Everything else is the test's. A node that replicates, or
/// that another must reach before it starts, wants [`start_at`].
///
/// Panics when the node cannot be started; that is a failed test.
pub fn start<A>(
    config: ServerConfig,
    startup: StartupEvents<A::Event>,
    sizing: A::Sizing,
    decoder: impl RequestDecoder<Event = A::Event> + 'static,
    encoder: impl ResponseEncoder<Report = A::Report, Query = A::QueryResponse> + 'static,
) -> Node
where
    A: Application + Send + 'static,
    A::Event: Send + Sync + 'static,
    A::Report: Send + 'static,
    A::QueryResponse: Send + 'static,
    A::Sizing: Send + 'static,
{
    #[cfg(not(feature = "dpdk"))]
    {
        kernel::start::<A>(None, config, startup, sizing, decoder, encoder, None)
    }
    #[cfg(feature = "dpdk")]
    {
        let claim = dpdk::claim_first_free();
        let addrs = dpdk::addrs(claim.slot());
        dpdk::start_claimed::<A>(claim, &addrs, config, startup, sizing, decoder, encoder)
    }
}

/// [`start`], at `addrs` (from [`addrs`]): `config.bind` becomes
/// `addrs.client()`. The test sets `replication_bind` to
/// `addrs.replication()` on a node replicas attach to, and `replica_of`
/// to its primary's `replication()`, as it would set any other field.
///
/// On DPDK the node takes `addrs`' slot, which must be free (its last
/// node stopped or joined), a `replication_bind` must be on the slot's IP,
/// and a `replica_of` on the runner's network: the launcher fills in the
/// primary's MAC (`dpdk.peer_mac`) from it.
pub fn start_at<A>(
    addrs: &Addrs,
    config: ServerConfig,
    startup: StartupEvents<A::Event>,
    sizing: A::Sizing,
    decoder: impl RequestDecoder<Event = A::Event> + 'static,
    encoder: impl ResponseEncoder<Report = A::Report, Query = A::QueryResponse> + 'static,
) -> Node
where
    A: Application + Send + 'static,
    A::Event: Send + Sync + 'static,
    A::Report: Send + 'static,
    A::QueryResponse: Send + 'static,
    A::Sizing: Send + 'static,
{
    #[cfg(not(feature = "dpdk"))]
    {
        kernel::start::<A>(Some(addrs), config, startup, sizing, decoder, encoder, None)
    }
    #[cfg(feature = "dpdk")]
    {
        dpdk::start::<A>(addrs, config, startup, sizing, decoder, encoder)
    }
}

/// [`start_at`], with the application's event publisher, as a binary that
/// has one hands it to the runtime: it runs when `config.event_bind` is
/// set, which the test sets as it would any other field.
///
/// Kernel TCP only: a DPDK node never runs the event publisher (see "DPDK
/// never runs the event publisher" in
/// `docs/internal/transport-divergences-2026-10.md`), so there is nothing
/// for a test of it to start there.
#[cfg(not(feature = "dpdk"))]
pub fn start_at_with_publisher<A>(
    addrs: &Addrs,
    config: ServerConfig,
    startup: StartupEvents<A::Event>,
    sizing: A::Sizing,
    decoder: impl RequestDecoder<Event = A::Event> + 'static,
    encoder: impl ResponseEncoder<Report = A::Report, Query = A::QueryResponse> + 'static,
    publisher: melin_server_runtime::server::EventPublisherFn<A>,
) -> Node
where
    A: Application + Send + 'static,
    A::Event: Send + Sync + 'static,
    A::Report: Send + 'static,
    A::QueryResponse: Send + 'static,
    A::Sizing: Send + 'static,
{
    kernel::start::<A>(
        Some(addrs),
        config,
        startup,
        sizing,
        decoder,
        encoder,
        Some(publisher),
    )
}

#[cfg(not(feature = "dpdk"))]
mod kernel {
    use std::net::{Ipv4Addr, SocketAddr, TcpStream};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use melin_app::Application;
    use melin_app::decoder::RequestDecoder;
    use melin_app::encoder::ResponseEncoder;
    use melin_server_runtime::StartupEvents;
    use melin_server_runtime::server::{self, EventPublisherFn, ServerConfig};
    use melin_wire_protocol::tcp::BlockingTcpListener;

    use super::{Addrs, Node};

    pub(super) fn start<A>(
        addrs: Option<&Addrs>,
        mut config: ServerConfig,
        startup: StartupEvents<A::Event>,
        sizing: A::Sizing,
        decoder: impl RequestDecoder<Event = A::Event> + 'static,
        encoder: impl ResponseEncoder<Report = A::Report, Query = A::QueryResponse> + 'static,
        publisher: Option<EventPublisherFn<A>>,
    ) -> Node
    where
        A: Application + Send + 'static,
        A::Event: Send + Sync + 'static,
        A::Report: Send + 'static,
        A::QueryResponse: Send + 'static,
        A::Sizing: Send + 'static,
    {
        // Bound here and handed to the runtime: on port 0 the kernel picks
        // a free port, so a parallel suite cannot race for one.
        let bind = addrs.map_or(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), Addrs::client);
        let listener = BlockingTcpListener::bind(bind)
            .unwrap_or_else(|e| panic!("bind the client listener on {bind}: {e}"));
        config.bind = listener.local_addr().expect("the listener's address");
        let addr = config.bind;
        // Never lock a test node's memory: see `config`.
        config.no_mlock = true;

        let shutdown = Arc::new(AtomicBool::new(false));
        let sd = Arc::clone(&shutdown);
        let thread = std::thread::spawn(move || -> Result<(), String> {
            server::run_with_listener::<A>(
                listener, config, startup, sizing, decoder, encoder, publisher, sd,
            )
            .map_err(|e| e.to_string())
        });
        Node {
            addr,
            thread: Some(thread),
            shutdown,
        }
    }

    impl Node {
        /// Stop the node cleanly and wait for it; panics if it failed.
        pub fn stop(mut self) {
            let thread = self.take_thread();
            self.request_stop();
            thread
                .join()
                .expect("node thread panicked")
                .expect("node returned an error");
        }

        /// Ask the node to stop, without waiting for it.
        pub(super) fn request_stop(&self) {
            self.shutdown.store(true, Ordering::Relaxed);
            // Best-effort poke so the accept loop wakes and sees the flag;
            // whether the connect itself succeeds is irrelevant.
            let _ = TcpStream::connect_timeout(&self.addr, Duration::from_millis(100));
        }
    }
}

#[cfg(feature = "dpdk")]
mod dpdk {
    use std::io;
    use std::net::{IpAddr, SocketAddr};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex, OnceLock};
    use std::time::{Duration, Instant};

    use melin_app::Application;
    use melin_app::decoder::RequestDecoder;
    use melin_app::encoder::ResponseEncoder;
    use melin_dpdk::{Eal, NicConfig, NicHandle, try_parse_mac};
    use melin_server_runtime::StartupEvents;
    use melin_server_runtime::server::{self, ServerConfig};

    use super::layout::{self, Layout};
    use super::{Addrs, DpdkClient, Node, SHUTDOWN_LIMIT};

    /// The runner's layout, read once. Kept with its error, so that every
    /// node asked for in a process not under the runner says why.
    static LAYOUT: OnceLock<Result<Layout, String>> = OnceLock::new();

    /// Which slots hold a node, indexed by slot. A `Vec<bool>` under a
    /// `Mutex`: a handful of slots, claimed and released a few times per
    /// test, so the lock is never contended and a bitmap would buy
    /// nothing. Sized from the layout on first use.
    static IN_USE: Mutex<Vec<bool>> = Mutex::new(Vec::new());

    pub(super) fn layout() -> &'static Layout {
        match LAYOUT.get_or_init(Layout::from_env) {
            Ok(layout) => layout,
            Err(e) => panic!("{e}"),
        }
    }

    pub(super) fn addrs(slot: usize) -> Addrs {
        let ip = layout().slot(slot).ip;
        Addrs {
            client: SocketAddr::from((ip, layout::NODE_PORT)),
            replication: SocketAddr::from((ip, layout::REPLICATION_PORT)),
            slot,
        }
    }

    /// A slot claimed for a node that is still being started. Dropping it
    /// (a check in [`start_claimed`] panicking, or the device failing to
    /// attach) frees the slot again, so that a test which catches the
    /// panic does not find the slot held by a node that never ran; once
    /// the device is attached, [`SlotClaim::into_node`] hands the slot to
    /// the node, which frees it in [`release`].
    pub(super) struct SlotClaim {
        slot: usize,
    }

    impl SlotClaim {
        pub(super) fn slot(&self) -> usize {
            self.slot
        }

        fn into_node(self) -> usize {
            let slot = self.slot;
            std::mem::forget(self);
            slot
        }
    }

    impl Drop for SlotClaim {
        fn drop(&mut self) {
            in_use()[self.slot] = false;
        }
    }

    /// Claim the lowest slot no node holds, in one step so two threads
    /// cannot take the same one. Panics when every one is held.
    pub(super) fn claim_first_free() -> SlotClaim {
        let count = layout().slot_count();
        let mut in_use = in_use();
        if in_use.len() < count {
            in_use.resize(count, false);
        }
        let slot = (0..count).find(|&slot| !in_use[slot]).unwrap_or_else(|| {
            panic!(
                "every one of the runner's {count} node slots holds a node: stop one first, \
                 or run one test per process (cargo nextest does)"
            )
        });
        in_use[slot] = true;
        SlotClaim { slot }
    }

    fn in_use() -> std::sync::MutexGuard<'static, Vec<bool>> {
        // A poisoned lock means a test panicked while holding it, between
        // two plain assignments: the flags are still whole.
        IN_USE
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Mark `slot` as holding a node. Panics if it already does.
    fn claim(slot: usize) -> SlotClaim {
        let mut in_use = in_use();
        if in_use.len() <= slot {
            in_use.resize(slot + 1, false);
        }
        assert!(
            !in_use[slot],
            "node slot {slot} already holds a node: stop or join it before starting another \
             there, or run one test per process (cargo nextest does)"
        );
        in_use[slot] = true;
        SlotClaim { slot }
    }

    /// Detach the slot's device, which its node has closed, so that the
    /// next node on the slot can attach it again, and free the slot.
    pub(super) fn release(slot: usize) {
        try_release(slot).unwrap_or_else(|e| panic!("{e}"));
    }

    /// [`release`], returning the failure rather than panicking: for a
    /// node's drop, which may run during a failing test's unwind. The slot
    /// stays held if its device cannot be detached, so no later node takes
    /// a slot whose device is still attached.
    pub(super) fn try_release(slot: usize) -> Result<(), String> {
        let (device, _) = layout::vdev(slot, &layout().slot(slot).iface);
        eal()
            .detach_vdev(&device)
            .map_err(|e| format!("detach {device} after its node returned: {e}"))?;
        in_use()[slot] = false;
        Ok(())
    }

    /// The process-wide EAL, initialized by the first node.
    ///
    /// On a thread of its own: EAL pins the thread that initializes it to
    /// its main lcore, and every thread that one spawns inherits that. The
    /// test's threads, and the nodes they start, keep every CPU the
    /// process may use.
    fn eal() -> &'static Eal {
        if let Some(init) = Eal::process_wide() {
            return init.unwrap_or_else(|e| panic!("initialize the process-wide EAL: {e}"));
        }
        let args = layout::eal_args(first_allowed_cpu());
        let init = std::thread::Builder::new()
            .name("eal-init".into())
            .spawn(move || {
                let args: Vec<&str> = args.iter().map(String::as_str).collect();
                Eal::init_process_wide(&args)
            })
            .expect("spawn the EAL init thread")
            .join()
            .expect("EAL init thread panicked");
        init.unwrap_or_else(|e| panic!("initialize the process-wide EAL: {e}"))
    }

    /// Whether this process has opened the DPDK client's NIC: its device
    /// is attached once and never detached (see [`open_client`]).
    static CLIENT_OPENED: AtomicBool = AtomicBool::new(false);

    /// Whether this process has handed the client pair's device to a child
    /// (see [`client_process_eal_args`]), which then owns it: this process
    /// must not open it as well. Never cleared: the launcher cannot tell
    /// when the child has let the device go.
    static CLIENT_HANDED_OUT: AtomicBool = AtomicBool::new(false);

    /// `mac`, a MAC the layout has already checked.
    fn mac_bytes(mac: &str) -> [u8; 6] {
        try_parse_mac(mac).unwrap_or_else(|e| panic!("the runner's layout: {e}"))
    }

    fn client_seat() -> &'static layout::Slot {
        layout().dpdk_client().unwrap_or_else(|e| panic!("{e}"))
    }

    pub(super) fn client() -> DpdkClient {
        let seat = client_seat();
        DpdkClient {
            ip: seat.ip,
            mac: mac_bytes(&seat.mac),
            prefix_len: layout().prefix_len,
        }
    }

    pub(super) fn client_process_eal_args(file_prefix: &str) -> Vec<String> {
        assert!(
            !CLIENT_OPENED.load(Ordering::Relaxed),
            "this process has opened the DPDK client's NIC, so a child cannot take the client \
             pair's device too: run the child's client in a test binary that does not open one"
        );
        CLIENT_HANDED_OUT.store(true, Ordering::Relaxed);
        layout::client_process_eal_args(first_allowed_cpu(), &client_seat().iface, file_prefix)
    }

    pub(super) fn mac_of(addr: SocketAddr) -> [u8; 6] {
        let mac = match addr.ip() {
            IpAddr::V4(ip) => layout().mac_of(ip),
            IpAddr::V6(_) => None,
        };
        mac_bytes(mac.unwrap_or_else(|| {
            panic!(
                "{addr} is not on the runner's network: take a node's address from \
                 Node::addr or melin_test_node::addrs"
            )
        }))
    }

    /// Attach the client's device on the process-wide EAL and open the NIC
    /// handle on its port; see [`super::open_dpdk_client`].
    ///
    /// The device is never detached. A handle on the shared EAL closes its
    /// port when it drops but leaves the device attached, and attaching a
    /// name twice fails, so a second handle would need the first's device
    /// detached at exactly the right moment, for no test that needs one:
    /// a client reconnects on the handle it has.
    pub(super) fn open_client(mut config: NicConfig) -> NicHandle {
        assert!(
            config.eal_args.is_empty(),
            "the DPDK client shares the process-wide EAL, which is already set up, so its own EAL \
             arguments ({}) cannot take effect: leave NicConfig::eal_args empty",
            config.eal_args.join(" ")
        );
        let seat = client_seat();
        assert!(
            !CLIENT_HANDED_OUT.load(Ordering::Relaxed),
            "this process has handed the DPDK client pair's device to a child \
             (dpdk_client_process_eal_args), which owns it: open the client's NIC in a test \
             binary that does not hand it out"
        );
        assert!(
            !CLIENT_OPENED.swap(true, Ordering::Relaxed),
            "the DPDK client's NIC is already open in this process: keep the handle and dial \
             again on it (lend it to StreamConnection::connect_dpdk_on as `&mut`)"
        );
        let (device, device_args) = layout::client_vdev(&seat.iface);
        let port = eal()
            .attach_vdev(&device, &device_args)
            .unwrap_or_else(|e| panic!("attach {device} for the DPDK client: {e}"));
        // As a node's: a port, not EAL arguments (checked empty above), and
        // the runner's address. The handle names its pool after the port.
        config.port_ids = vec![port];
        config.ip_addr = seat.ip;
        config.prefix_len = layout().prefix_len;
        NicHandle::open(&config)
            .unwrap_or_else(|e| panic!("open the DPDK client's NIC on {device} (port {port}): {e}"))
    }

    /// Start a node on `addrs`' slot, which must be free; see
    /// [`start_claimed`].
    pub(super) fn start<A>(
        addrs: &Addrs,
        config: ServerConfig,
        startup: StartupEvents<A::Event>,
        sizing: A::Sizing,
        decoder: impl RequestDecoder<Event = A::Event> + 'static,
        encoder: impl ResponseEncoder<Report = A::Report, Query = A::QueryResponse> + 'static,
    ) -> Node
    where
        A: Application + Send + 'static,
        A::Event: Send + Sync + 'static,
        A::Report: Send + 'static,
        A::QueryResponse: Send + 'static,
        A::Sizing: Send + 'static,
    {
        let claim = claim(addrs.slot);
        start_claimed::<A>(claim, addrs, config, startup, sizing, decoder, encoder)
    }

    /// Start a node on `addrs`' slot, which `claim` holds.
    pub(super) fn start_claimed<A>(
        claim: SlotClaim,
        addrs: &Addrs,
        mut config: ServerConfig,
        startup: StartupEvents<A::Event>,
        sizing: A::Sizing,
        decoder: impl RequestDecoder<Event = A::Event> + 'static,
        encoder: impl ResponseEncoder<Report = A::Report, Query = A::QueryResponse> + 'static,
    ) -> Node
    where
        A: Application + Send + 'static,
        A::Event: Send + Sync + 'static,
        A::Report: Send + 'static,
        A::QueryResponse: Send + 'static,
        A::Sizing: Send + 'static,
    {
        assert_eq!(claim.slot(), addrs.slot, "the claim is for another slot");
        let layout = layout();
        let slot = layout.slot(addrs.slot);

        if let Some(bind) = config.replication_bind {
            assert_eq!(
                bind.ip(),
                IpAddr::V4(slot.ip),
                "replication_bind {bind} is not on this node's slot: on DPDK the node replicates \
                 from its own IP; take the address from melin_test_node::addrs"
            );
        }
        config.dpdk.peer_mac = config.replica_of.map(|primary| {
            let mac = match primary.ip() {
                IpAddr::V4(ip) => layout.mac_of(ip),
                IpAddr::V6(_) => None,
            };
            mac.unwrap_or_else(|| {
                panic!(
                    "replica_of {primary} is not on the runner's network, so a DPDK replica \
                     cannot reach it: take a node's address from melin_test_node::addrs, and \
                     bind a test's own peer on melin_test_node::local_ip"
                )
            })
            .to_owned()
        });

        let (device, device_args) = layout::vdev(addrs.slot, &slot.iface);
        let port = eal()
            .attach_vdev(&device, &device_args)
            .unwrap_or_else(|e| panic!("attach {device} for node slot {}: {e}", addrs.slot));
        // Attached: from here the slot is the node's, freed by `release`
        // when it returns (which detaches the device too).
        let slot_id = claim.into_node();

        config.bind = addrs.client;
        // The node shares the process-wide EAL: it takes a port, not
        // arguments.
        config.dpdk.eal_args = String::new();
        config.dpdk.ports = vec![port];
        config.dpdk.ip = Some(slot.ip);
        config.dpdk.prefix_len = layout.prefix_len;
        // Never lock a test node's memory: see `config`.
        config.no_mlock = true;

        let shutdown = Arc::new(AtomicBool::new(false));
        let sd = Arc::clone(&shutdown);
        let thread = std::thread::Builder::new()
            .name(format!("node-{slot_id}"))
            .spawn(move || -> Result<(), String> {
                server::run_with_shutdown::<A>(config, startup, sizing, decoder, encoder, None, sd)
                    .map_err(|e| e.to_string())
            })
            .expect("spawn the node thread");
        Node {
            addr: addrs.client,
            thread: Some(thread),
            shutdown,
            slot: slot_id,
        }
    }

    impl Node {
        /// Stop the node cleanly and wait for it; panics if it failed.
        pub fn stop(self) {
            if self.is_finished() {
                panic!(
                    "the node stopped before the test asked it to: {:?}",
                    self.join()
                );
            }
            self.request_stop();
            let deadline = Instant::now() + SHUTDOWN_LIMIT;
            while !self.is_finished() {
                assert!(
                    Instant::now() < deadline,
                    "the node did not shut down within {SHUTDOWN_LIMIT:?}"
                );
                std::thread::sleep(Duration::from_millis(50));
            }
            self.join().expect("node returned an error");
        }

        /// Ask the node to stop, without waiting for it: its poll loop
        /// reads the flag on every pass.
        pub(super) fn request_stop(&self) {
            self.shutdown.store(true, Ordering::Relaxed);
        }

        /// Cut the node off the runner's network, as a pulled cable or a
        /// crashed host would: the bridge-side end of its interface goes
        /// down, so nothing it sends arrives and nothing reaches it, and
        /// no peer is told. For the deadlines a link's ends keep on a peer
        /// that has gone without a word. The node itself runs on; stop it
        /// as usual (its last words go nowhere).
        ///
        /// DPDK only: a kernel-TCP node shares the host's loopback with
        /// every other, so there is no link of its own to cut.
        pub fn cut_off(&self) {
            let port = format!("{}{BRIDGE_PORT_SUFFIX}", layout().slot(self.slot).iface);
            set_link_down(&port).unwrap_or_else(|e| panic!("bring {port} down: {e}"));
        }
    }

    /// Appended to a slot's interface to name its bridge-side peer
    /// (`BRIDGE_PORT_SUFFIX` in `scripts/dpdk/veth-setup.py`, which names
    /// them).
    const BRIDGE_PORT_SUFFIX: &str = "-br";

    /// Clear `IFF_UP` on interface `name`, as `ip link set NAME down` does.
    /// The runner's user namespace owns the network namespace, so the
    /// test may.
    fn set_link_down(name: &str) -> io::Result<()> {
        use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

        // SAFETY: `ifreq` is plain old data; all-zero is a valid value
        // (an empty name, no flags).
        let mut req: libc::ifreq = unsafe { std::mem::zeroed() };
        // The name, NUL-terminated by the zeroing: one byte is kept back.
        if name.len() >= req.ifr_name.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("interface name {name} is too long"),
            ));
        }
        for (dst, &src) in req.ifr_name.iter_mut().zip(name.as_bytes()) {
            *dst = src as libc::c_char;
        }
        // SAFETY: plain socket(2) call; the result is checked below.
        let fd = unsafe { libc::socket(libc::AF_INET, libc::SOCK_DGRAM | libc::SOCK_CLOEXEC, 0) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: `fd` was just returned by socket(2) and is owned by
        // nothing else; `OwnedFd` closes it.
        let socket = unsafe { OwnedFd::from_raw_fd(fd) };
        // SAFETY: `req` is a live, writable ifreq naming the interface,
        // which SIOCGIFFLAGS fills in and SIOCSIFFLAGS reads.
        unsafe {
            if libc::ioctl(socket.as_raw_fd(), libc::SIOCGIFFLAGS as _, &mut req) != 0 {
                return Err(io::Error::last_os_error());
            }
            req.ifr_ifru.ifru_flags &= !(libc::IFF_UP as libc::c_short);
            if libc::ioctl(socket.as_raw_fd(), libc::SIOCSIFFLAGS as _, &req) != 0 {
                return Err(io::Error::last_os_error());
            }
        }
        Ok(())
    }

    /// The first CPU this process may run on, for EAL's main lcore: CPU 0
    /// need not be in the set on a restricted host.
    fn first_allowed_cpu() -> usize {
        // SAFETY: `cpu_set_t` is plain old data; all-zero is the empty set.
        let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
        // SAFETY: `set` is a live, writable cpu_set_t of the size passed.
        let rc = unsafe { libc::sched_getaffinity(0, size_of::<libc::cpu_set_t>(), &mut set) };
        assert_eq!(rc, 0, "sched_getaffinity: {}", io::Error::last_os_error());
        (0..libc::CPU_SETSIZE as usize)
            // SAFETY: `c` is below CPU_SETSIZE, inside the set.
            .find(|&c| unsafe { libc::CPU_ISSET(c, &set) })
            .expect("this process may run on at least one CPU")
    }
}
