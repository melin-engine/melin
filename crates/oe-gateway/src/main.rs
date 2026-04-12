//! FIX 4.4 order entry gateway for Melin.
//!
//! Single-threaded io_uring event loop that multiplexes all FIX client
//! connections and their corresponding Melin server connections on one
//! core. No threads on the hot path, no mutexes, no shared state.
//!
//! Usage:
//!   melin-oe-gateway --config gateway.toml [--core N]

mod config;
mod event_loop;
mod id_map;
mod metrics;
mod price;
mod session;
#[cfg(test)]
mod test_stub;
mod translate;

use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tracing::{info, warn};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing.
    tracing_subscriber::fmt()
        .with_env_filter(
            // Discard try_from_default_env error: it returns Err
            // when RUST_LOG is unset, which is the normal case.
            // Fall back to a sensible default.
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // Parse CLI args.
    let args: Vec<String> = std::env::args().collect();
    let mut config_path: Option<&str> = None;
    let mut pin_core: Option<usize> = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--config" if i + 1 < args.len() => {
                config_path = Some(&args[i + 1]);
                i += 2;
            }
            "--core" if i + 1 < args.len() => {
                // Discard parse error: a malformed --core value
                // leaves `pin_core` as None, which means "do not pin
                // to any core". A later warn! covers the case.
                pin_core = args[i + 1].parse().ok();
                if pin_core.is_none() {
                    warn!(value = %args[i + 1], "invalid --core value, not pinning");
                }
                i += 2;
            }
            _ => {
                eprintln!("usage: melin-oe-gateway --config <path> [--core N]");
                std::process::exit(1);
            }
        }
    }

    let config_path = config_path.unwrap_or_else(|| {
        eprintln!("usage: melin-oe-gateway --config <path> [--core N]");
        std::process::exit(1);
    });

    let config = config::GatewayConfig::load(std::path::Path::new(config_path))?;

    info!(
        listen = %config.listen_addr,
        server = %config.server_addr,
        sessions = config.sessions.len(),
        symbols = config.symbols.len(),
        "FIX gateway starting"
    );

    // Pin to a dedicated CPU core if requested.
    if let Some(core) = pin_core {
        pin_to_core(core);
        info!(core, "pinned to CPU core");
    }

    // Shutdown signal.
    let shutdown = Arc::new(AtomicBool::new(false));
    setup_signal_handler(&shutdown)?;

    // Bind the TCP listener.
    let listener = TcpListener::bind(config.listen_addr)?;
    info!(addr = %config.listen_addr, "listening for FIX connections");

    // Leak the config into a 'static reference. The config lives for the
    // program's lifetime, so this is safe.
    let config: &'static config::GatewayConfig = Box::leak(Box::new(config));

    // Allocate the metrics surface and leak it as 'static — same
    // pattern as the config: lives for the program's lifetime.
    let metrics = metrics::GatewayMetrics::leak_default();

    // Spawn the Prometheus /metrics endpoint if configured.
    let metrics_join = if let Some(addr) = config.metrics_addr {
        Some(metrics::spawn_metrics_endpoint(
            addr,
            metrics,
            shutdown.clone(),
        )?)
    } else {
        None
    };

    // Create and run the single-threaded io_uring gateway.
    let mut gateway = event_loop::Gateway::new(listener, config, metrics)?;
    gateway.run(&shutdown)?;

    if let Some(j) = metrics_join {
        // The endpoint thread polls `shutdown` every 100 ms so this
        // returns within ~100 ms of run() exit. A panic on this
        // thread should be surfaced — it would otherwise leave the
        // process running with a silently dead /metrics endpoint.
        if let Err(payload) = j.join() {
            tracing::error!(?payload, "metrics endpoint thread panicked");
        }
    }

    info!("FIX gateway shut down");
    Ok(())
}

/// Pin the current thread to a specific CPU core via sched_setaffinity.
fn pin_to_core(core: usize) {
    unsafe {
        let mut cpuset: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut cpuset);
        libc::CPU_SET(core, &mut cpuset);
        let rc = libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &cpuset);
        if rc != 0 {
            warn!(
                core,
                "failed to pin to core (sched_setaffinity returned {rc})"
            );
        }
    }
}

/// Set up signal handling: block SIGINT/SIGTERM on the main thread, then
/// spawn a dedicated thread that waits for them via sigwait and sets the
/// shutdown flag. This is the only additional thread — it's off the hot path.
fn setup_signal_handler(shutdown: &Arc<AtomicBool>) -> std::io::Result<()> {
    let flag = Arc::clone(shutdown);

    // Block signals on the main thread so they're delivered to the signal thread.
    unsafe {
        let mut sigset: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut sigset);
        libc::sigaddset(&mut sigset, libc::SIGINT);
        libc::sigaddset(&mut sigset, libc::SIGTERM);
        libc::sigprocmask(libc::SIG_BLOCK, &sigset, std::ptr::null_mut());
    }

    // Spawn the dedicated signal-handling thread. Failure to spawn
    // here would leave the gateway with no graceful shutdown path —
    // surface it as a startup error rather than dropping the result.
    std::thread::Builder::new()
        .name("signal".into())
        .spawn(move || {
            unsafe {
                let mut sigset: libc::sigset_t = std::mem::zeroed();
                libc::sigemptyset(&mut sigset);
                libc::sigaddset(&mut sigset, libc::SIGINT);
                libc::sigaddset(&mut sigset, libc::SIGTERM);
                let mut sig: libc::c_int = 0;
                libc::sigwait(&sigset, &mut sig);
            }
            flag.store(true, Ordering::Relaxed);
        })?;
    Ok(())
}
