/// jemalloc: thread-local caches eliminate allocator lock contention,
/// giving more predictable latency than glibc malloc under high throughput.
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// jemalloc tuning, applied at allocator init via the well-known
/// `malloc_conf` symbol. Set for tail-latency stability:
///
/// - `background_thread:true` — spawn a dedicated thread to do page
///   purging asynchronously instead of synchronously on the allocating
///   thread. Default jemalloc does the purge work on whatever thread
///   happens to free memory, which on the matching/journal hot path
///   shows up as occasional multi-millisecond stalls in `process_event`.
/// - `dirty_decay_ms:60000` / `muzzy_decay_ms:60000` — hold dirty/muzzy
///   pages for 60 s (vs the 10 s default) before reclaiming. Trades
///   marginally higher steady-state RSS for fewer purge events; with
///   the background thread this also bounds how often that thread runs.
///
/// The trailing NUL is required: jemalloc reads `malloc_conf` as a C
/// string. `non_upper_case_globals` is the documented spelling — the
/// symbol name has to match exactly.
#[allow(non_upper_case_globals)]
#[unsafe(export_name = "malloc_conf")]
pub static malloc_conf: &[u8] =
    b"background_thread:true,dirty_decay_ms:60000,muzzy_decay_ms:60000\0";

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

    if !config.no_mlock {
        try_lock_memory();
    }

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
// Memory locking
// ---------------------------------------------------------------------------

/// Pin the entire process address space into RAM with `mlockall`.
///
/// Without locking, the kernel can fault out engine pages on memory
/// pressure, surfacing as 100µs–10ms tail spikes the next time the
/// matching thread touches the evicted page. Pinning is best-effort:
/// it requires `CAP_IPC_LOCK` (or root) and `RLIMIT_MEMLOCK` raised
/// to a value larger than the resident set. We raise the rlimit
/// ourselves to [`libc::RLIM_INFINITY`] before calling `mlockall`,
/// but the rlimit raise itself needs `CAP_SYS_RESOURCE` (also held
/// by root) to go above the hard ceiling. On a non-privileged dev
/// run we log a warning and continue — the server still works,
/// just without the tail-latency benefit. Use `--no-mlock` to skip
/// this entirely without the warning noise.
fn try_lock_memory() {
    // Raise RLIMIT_MEMLOCK so mlockall isn't artificially capped at
    // 64 KiB (typical default). EPERM here is non-fatal: the existing
    // hard limit may already be high enough, in which case mlockall
    // will succeed regardless.
    let unlim = libc::rlimit {
        rlim_cur: libc::RLIM_INFINITY,
        rlim_max: libc::RLIM_INFINITY,
    };
    let setrlimit_rc = unsafe { libc::setrlimit(libc::RLIMIT_MEMLOCK, &unlim) };
    if setrlimit_rc != 0 {
        let err = std::io::Error::last_os_error();
        tracing::warn!(
            error = %err,
            "could not raise RLIMIT_MEMLOCK; mlockall may fail or be capped"
        );
    }

    // Lock current AND future mappings — covers heap growth, mmap'd
    // journal regions, and lazily-faulted stacks of threads spawned
    // later. ONFAULT (lock pages only when they're first accessed)
    // would be cheaper but defeats the purpose: we want the whole
    // working set resident before any latency-sensitive work runs.
    let rc = unsafe { libc::mlockall(libc::MCL_CURRENT | libc::MCL_FUTURE) };
    if rc == 0 {
        tracing::info!("mlockall(MCL_CURRENT | MCL_FUTURE) succeeded");
    } else {
        let err = std::io::Error::last_os_error();
        tracing::warn!(
            error = %err,
            "mlockall failed; running without memory lock — tail latency may be affected. \
             Pass --no-mlock to suppress this warning."
        );
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
