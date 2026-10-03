//! Start a node for an integration test on whichever transport the build
//! selects, through one entry point, so that one test body runs on both.
//!
//! - **Kernel TCP** (the default). Exactly what the tests did before this
//!   crate: bind the client listener on `127.0.0.1:0` so the kernel picks
//!   the port, run the node with `server::run_with_listener` on a thread,
//!   and stop it through its shutdown flag.
//! - **DPDK** (the `dpdk` feature). The test process must run under
//!   `scripts/dpdk/netns-runner.sh`, which builds a veth pair in
//!   namespaces of the process's own and publishes the layout in the
//!   environment. The node takes the layout's first slot (interface and
//!   IP), runs on it through the `net_af_packet` PMD with
//!   `server::run`, and is stopped the way an operator stops one, with
//!   SIGTERM: `run` owns its shutdown flag and sets it from its signal
//!   handler.
//!
//! Without the runner, a DPDK build fails the first node it is asked for,
//! saying how to run it. EAL initialises once per process and cannot be
//! initialised again, so a DPDK build runs one node per process: under
//! nextest, which runs each test in a process of its own, that is one node
//! per test. A second node fails loudly rather than tripping over EAL.
//!
//! Endpoints that are kernel TCP by design (health, admin, replication on
//! the kernel path, raft) are left as the test configures them: under the
//! runner they bind on the namespace's own loopback.
//!
//! See `docs/internal/dpdk-transparent-tests.md`.

#[cfg(any(test, feature = "dpdk"))]
mod layout;

use std::net::SocketAddr;
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
/// A DPDK node initialises EAL and its port before it answers ARP, which
/// on a shared CI runner takes far longer than binding a socket.
pub const STARTUP_LIMIT: Duration = if cfg!(feature = "dpdk") {
    Duration::from_secs(45)
} else {
    Duration::from_secs(10)
};

/// A node running on its own thread of this process.
///
/// No `Drop`: a test that fails mid-way leaves its node running until the
/// process exits, as the tests did before this crate. Stopping it from a
/// destructor would run during the unwind and could turn one clear panic
/// into a second, muddier one.
pub struct Node {
    /// Where clients connect: `127.0.0.1` and a kernel-assigned port on
    /// kernel TCP, the slot's IP and a fixed port on DPDK.
    addr: SocketAddr,
    thread: JoinHandle<Result<(), String>>,
    /// The flag `run_with_listener` polls. The DPDK node has none of ours:
    /// `server::run` makes its own and sets it on SIGTERM.
    #[cfg(not(feature = "dpdk"))]
    shutdown: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl Node {
    /// The address clients connect to.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
}

/// Start a node running application `A` with `config`, on this build's
/// transport, and return as soon as its thread is running: a client
/// retries until the node serves (within [`STARTUP_LIMIT`]).
///
/// `config.bind` is overwritten with the address the transport gives the
/// node; on DPDK so are the `dpdk_*` fields. Everything else is the
/// test's.
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
        kernel::start::<A>(config, startup, sizing, decoder, encoder)
    }
    #[cfg(feature = "dpdk")]
    {
        dpdk::start::<A>(config, startup, sizing, decoder, encoder)
    }
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
    use melin_server_runtime::server::{self, ServerConfig};
    use melin_wire_protocol::tcp::BlockingTcpListener;

    use super::Node;

    pub(super) fn start<A>(
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
        // Bound here and handed to the runtime, so the kernel picks a free
        // port and a parallel suite cannot race for one.
        let listener = BlockingTcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .expect("bind the client listener");
        config.bind = listener.local_addr().expect("the listener's address");
        let addr = config.bind;

        let shutdown = Arc::new(AtomicBool::new(false));
        let sd = Arc::clone(&shutdown);
        let thread = std::thread::spawn(move || -> Result<(), String> {
            server::run_with_listener::<A>(
                listener, config, startup, sizing, decoder, encoder, None, sd,
            )
            .map_err(|e| e.to_string())
        });
        Node {
            addr,
            thread,
            shutdown,
        }
    }

    impl Node {
        /// Stop the node cleanly and wait for it; panics if it failed.
        pub fn stop(self) {
            self.shutdown.store(true, Ordering::Relaxed);
            // Best-effort poke so the accept loop wakes and sees the flag;
            // whether the connect itself succeeds is irrelevant.
            let _ = TcpStream::connect_timeout(&self.addr, Duration::from_millis(100));
            self.thread
                .join()
                .expect("node thread panicked")
                .expect("node returned an error");
        }
    }
}

#[cfg(feature = "dpdk")]
mod dpdk {
    use std::io;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    use melin_app::Application;
    use melin_app::decoder::RequestDecoder;
    use melin_app::encoder::ResponseEncoder;
    use melin_server_runtime::StartupEvents;
    use melin_server_runtime::server::{self, ServerConfig};

    use super::layout::{self, Layout};
    use super::{Node, STARTUP_LIMIT};

    /// How long a clean shutdown may take.
    const SHUTDOWN_LIMIT: Duration = Duration::from_secs(20);

    /// Set by the first node this process starts. EAL cannot be
    /// initialised a second time in one process, so a second node is
    /// refused here, with the reason, rather than failing inside EAL.
    static STARTED: AtomicBool = AtomicBool::new(false);

    pub(super) fn start<A>(
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
        let layout = Layout::from_env().unwrap_or_else(|e| panic!("{e}"));
        assert!(
            !STARTED.swap(true, Ordering::SeqCst),
            "a second DPDK node in one process: EAL initialises once per process. Run one \
             test per process (cargo nextest does), and see step 2 of \
             docs/internal/dpdk-transparent-tests.md for tests that need two nodes or a restart"
        );

        let slot = layout.slot(0);
        config.bind = SocketAddr::from((slot.ip, layout::NODE_PORT));
        config.dpdk_eal_args = layout::eal_args(slot.iface, first_allowed_cpu());
        config.dpdk_ports = vec![0];
        config.dpdk_ip = slot.ip.to_string();
        config.dpdk_prefix_len = layout.prefix_len;
        let addr = config.bind;

        let thread = std::thread::Builder::new()
            .name("node".into())
            .spawn(move || -> Result<(), String> {
                server::run::<A>(config, startup, sizing, decoder, encoder, None)
                    .map_err(|e| e.to_string())
            })
            .expect("spawn the node thread");
        Node { addr, thread }
    }

    impl Node {
        /// Stop the node cleanly and wait for it; panics if it failed.
        ///
        /// Sends SIGTERM to this process, which `server::run` turns into a
        /// clean shutdown. Waits first for `run` to have installed that
        /// handler: a SIGTERM before it would take the default action and
        /// kill the test process outright.
        pub fn stop(self) {
            let Node { thread, .. } = self;
            let deadline = Instant::now() + STARTUP_LIMIT;
            while !sigterm_is_handled() {
                assert!(
                    !thread.is_finished(),
                    "the node stopped before the test asked it to: {:?}",
                    thread.join()
                );
                assert!(
                    Instant::now() < deadline,
                    "the node did not install its signal handler within {STARTUP_LIMIT:?}"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
            assert!(
                !thread.is_finished(),
                "the node stopped before the test asked it to: {:?}",
                thread.join()
            );

            // A node already tearing itself down (a pipeline thread died) but
            // not yet finished slips past the check above; `run`'s handler
            // then sees its flag already set and exits the process with
            // status 1 instead of this test reporting the node's error.
            // Closing that window needs a way to stop `run` other than a
            // signal, a production change step 1 does not make; it only
            // affects a test that is failing anyway.
            //
            // SAFETY: kill(2) on our own pid, whose SIGTERM handler is
            // installed (checked above).
            let rc = unsafe { libc::kill(libc::getpid(), libc::SIGTERM) };
            assert_eq!(rc, 0, "SIGTERM: {}", io::Error::last_os_error());
            let deadline = Instant::now() + SHUTDOWN_LIMIT;
            while !thread.is_finished() {
                assert!(
                    Instant::now() < deadline,
                    "the node did not shut down within {SHUTDOWN_LIMIT:?} of SIGTERM"
                );
                std::thread::sleep(Duration::from_millis(50));
            }
            thread
                .join()
                .expect("node thread panicked")
                .expect("node returned an error");
        }
    }

    /// Whether SIGTERM has a handler, rather than its default action or an
    /// inherited "ignore" (under which the stop signal would be lost).
    fn sigterm_is_handled() -> bool {
        // SAFETY: `sigaction` is plain old data; all-zero is a valid value
        // for the kernel to overwrite.
        let mut current: libc::sigaction = unsafe { std::mem::zeroed() };
        // SAFETY: a null new action only queries; `current` is live and
        // writable for the call.
        let rc = unsafe { libc::sigaction(libc::SIGTERM, std::ptr::null(), &mut current) };
        assert_eq!(rc, 0, "sigaction(SIGTERM): {}", io::Error::last_os_error());
        current.sa_sigaction != libc::SIG_DFL && current.sa_sigaction != libc::SIG_IGN
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
