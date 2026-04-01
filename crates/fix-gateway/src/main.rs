//! FIX 4.2 order entry gateway for Melin.
//!
//! Accepts FIX TCP connections from trading clients, translates messages
//! to Melin's binary wire protocol, and forwards them to melin-server.
//!
//! Usage:
//!   melin-fix-gateway --config gateway.toml
//!
//! See the config module for TOML format documentation.

mod config;
mod fix;
mod id_map;
mod price;
mod session;
mod translate;

use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tracing::{error, info};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // Parse CLI args.
    let args: Vec<String> = std::env::args().collect();
    let config_path = if args.len() >= 3 && args[1] == "--config" {
        &args[2]
    } else {
        eprintln!("usage: melin-fix-gateway --config <path>");
        std::process::exit(1);
    };

    let config = config::GatewayConfig::load(std::path::Path::new(config_path))?;

    info!(
        listen = %config.listen_addr,
        server = %config.server_addr,
        sessions = config.sessions.len(),
        symbols = config.symbols.len(),
        "FIX gateway starting"
    );

    // Shutdown signal.
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_flag = Arc::clone(&shutdown);
    ctrlc_handler(&shutdown_flag);

    // TCP accept loop.
    let listener = TcpListener::bind(config.listen_addr)?;
    listener.set_nonblocking(false)?;
    // Set a timeout so we can check for shutdown.
    // TcpListener doesn't have set_timeout, so we use SO_RCVTIMEO
    // on the underlying socket via a short accept timeout approach.
    // Instead, we'll set nonblocking and poll.
    listener.set_nonblocking(true)?;

    info!(addr = %config.listen_addr, "listening for FIX connections");

    // Leak the config into a 'static reference so session threads can
    // borrow it without lifetime issues. The config lives for the
    // program's lifetime, so this is safe.
    let config: &'static config::GatewayConfig = Box::leak(Box::new(config));

    while !shutdown.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((stream, addr)) => {
                info!(peer = %addr, "accepted FIX connection");
                let shutdown_ref = Arc::clone(&shutdown);
                std::thread::Builder::new()
                    .name(format!("fix-{addr}"))
                    .spawn(move || {
                        session::run_session(stream, config, &shutdown_ref);
                    })
                    .expect("spawn session thread");
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                // No pending connection — sleep briefly and retry.
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
            Err(e) => {
                error!(error = %e, "accept failed");
            }
        }
    }

    info!("FIX gateway shutting down");
    Ok(())
}

fn ctrlc_handler(shutdown: &Arc<AtomicBool>) {
    let flag = Arc::clone(shutdown);
    let _ = std::thread::Builder::new()
        .name("signal".into())
        .spawn(move || {
            // Block on sigwait for SIGINT/SIGTERM.
            unsafe {
                let mut sigset: libc::sigset_t = std::mem::zeroed();
                libc::sigemptyset(&mut sigset);
                libc::sigaddset(&mut sigset, libc::SIGINT);
                libc::sigaddset(&mut sigset, libc::SIGTERM);
                libc::sigprocmask(libc::SIG_BLOCK, &sigset, std::ptr::null_mut());
                let mut sig: libc::c_int = 0;
                libc::sigwait(&sigset, &mut sig);
            }
            flag.store(true, Ordering::Relaxed);
        });
}
