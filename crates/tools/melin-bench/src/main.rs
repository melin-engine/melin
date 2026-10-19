//! `melin-bench`: a closed-loop load generator for the example
//! applications. `--clients` connections, each keeping `--window`
//! requests in flight, all driven from one thread over kernel TCP or a
//! DPDK NIC; replies arriving inside the measured phase are timed, and the
//! run's throughput and latency percentiles are printed and, with
//! `--json`, written in the shape the Exchange Core's bench writes, so
//! the LAN suite (`scripts/bench/`) reads every application's results the
//! same way.
//!
//! A run: dial and authenticate every client, then warm up, measure and
//! cool down under the same load, and finally stop sending and wait for
//! the requests still in flight. The latency of a request is from its
//! send to its reply frame, not to the batch end after it. On kernel TCP,
//! the requests a connection sends in one pass of the loop leave in one
//! write, not one each.

#![cfg_attr(not(test), deny(clippy::unwrap_used))]

mod app;
mod batched;
#[cfg(feature = "dpdk")]
mod dpdk;
mod load;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use melin_client::{
    DEFAULT_TIMEOUT, NonBlockingTcp, PolledConnection, REQUEST_HEADER_LEN, SigningKey, key,
};

use app::{App, AppKind};
use batched::Batched;
use load::{Outcome, Plan, Streams};

#[derive(Clone, Copy, PartialEq, Eq, Debug, ValueEnum)]
enum TransportKind {
    Kernel,
    Dpdk,
}

#[derive(Parser, Debug)]
#[command(
    name = "melin-bench",
    about = "Load a Melin example application and time its replies"
)]
struct Cli {
    /// The application the node runs.
    #[arg(long, value_enum, required_unless_present = "print_authorized_key")]
    app: Option<AppKind>,

    /// The node's client address.
    #[arg(long, default_value = "127.0.0.1:9876")]
    addr: SocketAddr,

    /// The signing key (PEM or a raw 32-byte seed) every client
    /// authenticates with.
    #[arg(long)]
    key: PathBuf,

    /// Print the `authorized_keys` line for `--key` in this role, and exit.
    #[arg(long, value_name = "ROLE")]
    print_authorized_key: Option<String>,

    /// Connections to the node.
    #[arg(long, default_value_t = 1)]
    clients: usize,

    /// Requests each connection keeps in flight.
    #[arg(long, default_value_t = 1)]
    window: usize,

    /// Bytes per echo request (echo only).
    #[arg(long, default_value_t = 64)]
    size: usize,

    /// How long the measured phase runs.
    #[arg(long, default_value = "10s", value_parser = humantime::parse_duration)]
    duration: Duration,

    /// Load before the measured phase, its replies not timed.
    #[arg(long, default_value = "5s", value_parser = humantime::parse_duration)]
    warmup_duration: Duration,

    /// Load after the measured phase, its replies not timed: the
    /// measured phase's last replies arrive under the same load as its
    /// first.
    #[arg(long, default_value = "1s", value_parser = humantime::parse_duration)]
    cooldown_duration: Duration,

    /// Write the results as JSON to this file.
    #[arg(long)]
    json: Option<PathBuf>,

    /// Pin the loop's thread to this core once the connections are up.
    #[arg(long)]
    core: Option<usize>,

    #[arg(long, value_enum, default_value_t = TransportKind::Kernel)]
    transport: TransportKind,

    #[cfg(feature = "dpdk")]
    #[command(flatten)]
    dpdk: melin_dpdk::DpdkArgs,
}

type Error = Box<dyn std::error::Error>;

fn main() -> ExitCode {
    match run(Cli::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::from(2)
        }
    }
}

fn run(cli: Cli) -> Result<(), Error> {
    let key = key::load_signing_key(&cli.key)?;
    if let Some(role) = &cli.print_authorized_key {
        println!(
            "{}",
            key::authorized_keys_line(role, &key.verifying_key(), "melin-bench")
        );
        return Ok(());
    }
    let Some(kind) = cli.app else {
        return Err("--app is required".into());
    };
    let app = App::new(kind, cli.size)?;
    if cli.clients == 0 || cli.window == 0 {
        return Err("--clients and --window must be at least 1".into());
    }
    if cli.duration.is_zero() {
        return Err("--duration must be longer than zero".into());
    }
    let plan = Plan {
        window: cli.window,
        warmup: cli.warmup_duration,
        duration: cli.duration,
        cooldown: cli.cooldown_duration,
        timeout: DEFAULT_TIMEOUT,
    };
    if cli.core.is_some() {
        // Before anything narrows this thread's CPU mask (EAL pins the
        // thread that initialises it), as the runtime's helper asks.
        melin_app::affinity::capture_home_mask();
    }

    let outcome = match cli.transport {
        TransportKind::Kernel => {
            #[cfg(feature = "dpdk")]
            if let Some(flag) = cli.dpdk.first_given() {
                return Err(format!(
                    "{flag} is for --transport dpdk: without it this run would load the kernel \
                     path"
                )
                .into());
            }
            let frame_len = REQUEST_HEADER_LEN + app.max_body();
            let mut streams = Kernel::connect(cli.addr, cli.clients, cli.window, frame_len)?;
            measure(&mut streams, &cli, &key, app, plan)?
        }
        #[cfg(feature = "dpdk")]
        TransportKind::Dpdk => {
            let mut streams = dpdk::Nic::connect(&cli.dpdk, cli.addr, cli.clients)?;
            measure(&mut streams, &cli, &key, app, plan)?
        }
        #[cfg(not(feature = "dpdk"))]
        TransportKind::Dpdk => {
            return Err(
                "this melin-bench was built without the `dpdk` feature: rebuild with \
                        `--features dpdk` (needs libdpdk) for --transport dpdk"
                    .into(),
            );
        }
    };

    let results = Results {
        app,
        clients: cli.clients,
        window: cli.window,
        plan,
        outcome,
    };
    print!("{}", results.summary());
    if let Some(path) = &cli.json {
        std::fs::write(path, results.json())
            .map_err(|e| format!("cannot write {}: {e}", path.display()))?;
        eprintln!("Results written to {}", path.display());
    }
    Ok(())
}

/// Pin if asked, then run the load over `streams`.
fn measure<S: Streams>(
    streams: &mut S,
    cli: &Cli,
    key: &SigningKey,
    app: App,
    plan: Plan,
) -> Result<Outcome, Error> {
    if let Some(core) = cli.core {
        // After the connections are up: a DPDK NIC's EAL pins the thread
        // that initialises it to its main lcore, which would undo a pin
        // taken before.
        let core =
            melin_app::affinity::pin_to_core(core).map_err(|e| format!("--core {core}: {e}"))?;
        eprintln!("pinned to core {core}");
    }
    let connections = (0..cli.clients)
        .map(|_| PolledConnection::new(key))
        .collect();
    Ok(load::run(streams, connections, app, plan)?)
}

/// Kernel sockets, one per client, each writing a pass's requests at once,
/// on a clock of the process's own.
struct Kernel {
    // A `Vec` indexed by client: the loop visits every one in turn.
    sockets: Vec<Batched<NonBlockingTcp>>,
    origin: Instant,
}

impl Kernel {
    /// Dial `clients` sockets to `addr`, each holding up to `window`
    /// requests of `frame_len` bytes between writes.
    fn connect(
        addr: SocketAddr,
        clients: usize,
        window: usize,
        frame_len: usize,
    ) -> Result<Self, Error> {
        let sockets = (0..clients)
            .map(|_| {
                NonBlockingTcp::connect(addr, DEFAULT_TIMEOUT)
                    .map(|socket| Batched::new(socket, window, frame_len))
            })
            .collect::<Result<_, _>>()
            .map_err(|e| format!("cannot connect to {addr}: {e}"))?;
        Ok(Self {
            sockets,
            origin: Instant::now(),
        })
    }
}

impl Streams for Kernel {
    type Stream<'a> = &'a mut Batched<NonBlockingTcp>;

    fn now_ns(&self) -> u64 {
        load::nanos(self.origin.elapsed())
    }

    fn service(&mut self, _now_ns: u64) {}

    fn stream(&mut self, client: usize) -> &mut Batched<NonBlockingTcp> {
        &mut self.sockets[client]
    }

    fn end_sends(&mut self, client: usize) -> std::io::Result<()> {
        self.sockets[client].write()
    }
}

/// A run's results, for the console and for `--json`.
struct Results {
    app: App,
    clients: usize,
    window: usize,
    plan: Plan,
    outcome: Outcome,
}

/// The percentiles reported, with their JSON keys: the Exchange Core
/// bench's, so the suite reads both.
const PERCENTILES: [(&str, f64); 6] = [
    ("p50", 50.0),
    ("p90", 90.0),
    ("p99", 99.0),
    ("p99.9", 99.9),
    ("p99.99", 99.99),
    ("p99.999", 99.999),
];

impl Results {
    fn measured_requests(&self) -> u64 {
        self.outcome.latency.len()
    }

    fn throughput(&self) -> f64 {
        self.measured_requests() as f64 / self.outcome.measured.as_secs_f64()
    }

    fn summary(&self) -> String {
        let latency = &self.outcome.latency;
        let mut out = format!(
            "=== {} — {} clients × window {} ===\n  throughput: {:.0} requests/sec ({} timed)\n",
            self.app.name(),
            self.clients,
            self.window,
            self.throughput(),
            self.measured_requests(),
        );
        out.push_str(&format!("  {:<8} {}\n", "min", micros(latency.min())));
        for (label, p) in PERCENTILES {
            out.push_str(&format!(
                "  {label:<8} {}\n",
                micros(latency.value_at_quantile(p / 100.0))
            ));
        }
        out.push_str(&format!("  {:<8} {}\n", "max", micros(latency.max())));
        out
    }

    fn json(&self) -> String {
        let latency = &self.outcome.latency;
        let us = |ns: u64| ns as f64 / 1_000.0;
        let mut percentiles = format!("\"min_us\":{}", us(latency.min()));
        for (label, p) in PERCENTILES {
            percentiles.push_str(&format!(
                ",\"{label}_us\":{}",
                us(latency.value_at_quantile(p / 100.0))
            ));
        }
        percentiles.push_str(&format!(",\"max_us\":{}", us(latency.max())));
        let mut out = format!(
            "{{\"label\":\"{}\",\"clients\":{},\"window\":{},\"measured_requests\":{},\
             \"completed_requests\":{},\"warmup_ms\":{},\"measured_ms\":{},\"cooldown_ms\":{},\"wall_ms\":{},\
             \"throughput_ops\":{:.1},\"latency\":{{{percentiles}}}}}",
            self.app.name(),
            self.clients,
            self.window,
            self.measured_requests(),
            self.outcome.completed,
            self.plan.warmup.as_millis(),
            self.outcome.measured.as_millis(),
            self.plan.cooldown.as_millis(),
            self.outcome.wall.as_millis(),
            self.throughput(),
        );
        out.push('\n');
        out
    }
}

/// `12.3 µs`.
fn micros(ns: u64) -> String {
    format!("{:.1} µs", ns as f64 / 1_000.0)
}
