/// jemalloc: thread-local caches eliminate allocator lock contention,
/// giving more predictable latency than glibc malloc under high throughput.
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use clap::Parser;
#[cfg(not(feature = "dpdk"))]
use melin_protocol::tcp::BlockingTcpListener;
use melin_server::server::ServerConfig;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(true)
        .with_thread_names(true)
        .init();

    // Route panics through tracing so a thread that dies mid-event
    // shows up in container logs instead of disappearing into a
    // half-flushed stderr write. The hook fires before the default
    // panic handler tears the thread down, so we still get the
    // backtrace via RUST_BACKTRACE if set.
    let prev_panic_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let location = info
            .location()
            .map(|l| format!("{}:{}:{}", l.file(), l.line(), l.column()))
            .unwrap_or_else(|| "<unknown>".to_string());
        let payload = info.payload();
        let msg = if let Some(s) = payload.downcast_ref::<&'static str>() {
            (*s).to_string()
        } else if let Some(s) = payload.downcast_ref::<String>() {
            s.clone()
        } else {
            "<non-string panic payload>".to_string()
        };
        let thread = std::thread::current();
        let thread_name = thread.name().unwrap_or("<unnamed>");
        tracing::error!(thread = thread_name, location = %location, message = %msg, "thread panicked");
        prev_panic_hook(info);
    }));

    let shutdown = Arc::new(AtomicBool::new(false));
    install_shutdown_handler(&shutdown);

    let config = ServerConfig::parse();

    #[cfg(feature = "dpdk")]
    {
        let dpdk_config = dpdk_config_from(&config);
        melin_server::server::run_dpdk(config, dpdk_config, shutdown)
    }

    #[cfg(not(feature = "dpdk"))]
    {
        let listener = BlockingTcpListener::bind(config.bind)?;
        melin_server::server::run_with_shutdown(listener, config, shutdown)
    }
}

// ---------------------------------------------------------------------------
// Signal handling
// ---------------------------------------------------------------------------

/// Pointer to the shared shutdown flag, set once before signals can fire.
/// `AtomicUsize` stores the raw pointer as an integer — signal-safe.
static SHUTDOWN_PTR: AtomicUsize = AtomicUsize::new(0);

/// Signal handler for SIGINT/SIGTERM. Sets the shutdown flag.
/// Second signal force-exits (user is impatient).
extern "C" fn signal_handler(_sig: libc::c_int) {
    let ptr = SHUTDOWN_PTR.load(Ordering::Relaxed);
    if ptr != 0 {
        let flag = unsafe { &*(ptr as *const AtomicBool) };
        if flag.swap(true, Ordering::Relaxed) {
            // Already set — second signal. Force exit immediately.
            // Use _exit (not std::process::exit) because atexit handlers
            // and stdio flushes are not signal-safe and can deadlock.
            unsafe { libc::_exit(1) };
        }
    }
}

/// Install SIGINT/SIGTERM handlers that flip `shutdown` on first signal
/// and force-exit on the second. The caller must keep the `Arc` alive
/// for the program's lifetime — we publish its pointer to a signal-safe
/// static so the handler can reach the flag.
fn install_shutdown_handler(shutdown: &Arc<AtomicBool>) {
    SHUTDOWN_PTR.store(Arc::as_ptr(shutdown) as usize, Ordering::Relaxed);
    unsafe {
        libc::signal(
            libc::SIGINT,
            signal_handler as *const () as libc::sighandler_t,
        );
        libc::signal(
            libc::SIGTERM,
            signal_handler as *const () as libc::sighandler_t,
        );
    }
}

// ---------------------------------------------------------------------------
// DPDK config
// ---------------------------------------------------------------------------

#[cfg(feature = "dpdk")]
fn dpdk_config_from(cfg: &ServerConfig) -> melin_dpdk::DpdkConfig {
    melin_dpdk::DpdkConfig {
        eal_args: cfg
            .dpdk_eal_args
            .split_whitespace()
            .map(String::from)
            .collect(),
        port_ids: cfg.dpdk_ports.clone(),
        ip_addr: cfg.dpdk_ip.parse().expect("invalid --dpdk-ip address"),
        prefix_len: cfg.dpdk_prefix_len,
        gateway: cfg
            .dpdk_gateway
            .as_deref()
            .map(|s| s.parse().expect("invalid --dpdk-gateway address")),
        listen_port: cfg.bind.port(),
        mtu: cfg.dpdk_mtu,
        vlan_id: cfg.dpdk_vlan,
        num_queues: dpdk_num_queues(cfg),
    }
}

/// Always one I/O queue. The primary handles both trading and
/// replication connections from the same poll thread (the replication
/// state machine lives inside `run_dpdk_poll`'s main loop, dispatched
/// off `AcceptedConnection::listen_port`). The replica likewise has
/// only one DPDK consumer. Multi-queue + RSS was previously used to
/// split client and replication onto separate queues, but DPDK's
/// per-driver flow-steering quirks (iavf in particular) made the
/// queue assignment non-deterministic — see the
/// `feat/dpdk-per-port-egress` branch for the failed attempts.
#[cfg(feature = "dpdk")]
fn dpdk_num_queues(_cfg: &ServerConfig) -> u16 {
    1
}
