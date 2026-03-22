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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(true)
        .with_thread_names(true)
        .init();

    let shutdown = Arc::new(AtomicBool::new(false));

    // Store the pointer for the signal handler before installing handlers.
    // The Arc keeps the AtomicBool alive for the program's lifetime.
    SHUTDOWN_PTR.store(Arc::as_ptr(&shutdown) as usize, Ordering::Relaxed);

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

    let config = ServerConfig::parse();

    #[cfg(feature = "dpdk")]
    {
        let dpdk_config = melin_dpdk::DpdkConfig {
            eal_args: config
                .dpdk_eal_args
                .split_whitespace()
                .map(String::from)
                .collect(),
            port_id: config.dpdk_port,
            ip_addr: config.dpdk_ip.parse().expect("invalid --dpdk-ip address"),
            prefix_len: config.dpdk_prefix_len,
            gateway: config
                .dpdk_gateway
                .as_deref()
                .map(|s| s.parse().expect("invalid --dpdk-gateway address")),
            listen_port: config.bind.port(),
        };
        melin_server::server::run_dpdk(config, dpdk_config, shutdown)
    }

    #[cfg(not(feature = "dpdk"))]
    {
        let listener = BlockingTcpListener::bind(config.bind)?;
        melin_server::server::run_with_shutdown(listener, config, shutdown)
    }
}
