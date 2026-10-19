//! `melin-bench`: a closed-loop load generator for the example
//! applications. `--clients` connections, each keeping `--window`
//! requests in flight, driven over kernel TCP from `--threads` threads
//! (one by default), or from one thread over a DPDK NIC; replies arriving
//! inside the measured phase are timed, and the
//! run's throughput and latency percentiles are printed and, with
//! `--json`, written in the shape the Exchange Core's bench writes, so
//! the LAN suite (`scripts/bench/`) reads every application's results the
//! same way.
//!
//! A run: dial and authenticate every client, then warm up, measure and
//! cool down under the same load, every thread over the same window, and
//! finally stop sending and wait for the requests still in flight. The latency of a request is from its
//! send to its reply frame, not to the batch end after it. On kernel TCP,
//! the requests a connection sends in one pass of the loop leave in one
//! write, not one each.

#![cfg_attr(not(test), deny(clippy::unwrap_used))]

mod app;
mod batched;
#[cfg(feature = "dpdk")]
mod dpdk;
mod load;
mod shard;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use melin_client::{DEFAULT_TIMEOUT, NonBlockingTcp, REQUEST_HEADER_LEN, key};

use app::{App, AppKind, ClientId};
use batched::Batched;
use load::{Outcome, Plan, Streams};
use shard::Shards;

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

    /// Load threads, the clients dealt among them round-robin, each
    /// connecting and loading its own. Kernel TCP only: a DPDK NIC is
    /// driven from one thread.
    #[arg(long, default_value_t = 1)]
    threads: usize,

    /// Pin load thread `i` to core `CORE + i` once its connections are up.
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
    let shards = Shards::new(cli.threads, cli.clients)?;
    if cli.transport == TransportKind::Dpdk && shards.threads() > 1 {
        return Err(format!(
            "--threads {} is for --transport kernel: a DPDK NIC is driven from one thread",
            cli.threads
        )
        .into());
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

    let outcomes = match cli.transport {
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
            // One origin for every thread's clock: the run's start is read
            // on one of them and compared on all.
            let origin = Instant::now();
            shard::run(shards, |index, ids, shared| {
                let connect = |ids: &[ClientId]| {
                    let streams = Kernel::connect(cli.addr, ids, cli.window, frame_len, origin)?;
                    pin(cli.core, index)?;
                    Ok(streams)
                };
                load::shard(connect, ids, &key, app, plan, shared)
            })
        }
        #[cfg(feature = "dpdk")]
        TransportKind::Dpdk => shard::run(shards, |index, ids, shared| {
            let connect = |ids: &[ClientId]| {
                let streams =
                    dpdk::Nic::connect(&cli.dpdk, cli.addr, ids).map_err(|e| e.to_string())?;
                pin(cli.core, index)?;
                Ok(streams)
            };
            load::shard(connect, ids, &key, app, plan, shared)
        }),
        #[cfg(not(feature = "dpdk"))]
        TransportKind::Dpdk => {
            return Err(
                "this melin-bench was built without the `dpdk` feature: rebuild with \
                        `--features dpdk` (needs libdpdk) for --transport dpdk"
                    .into(),
            );
        }
    }?;

    let results = Results {
        app,
        clients: cli.clients,
        window: cli.window,
        threads: shards.threads(),
        plan,
        outcome: load::merge(outcomes)?,
    };
    print!("{}", results.summary());
    if let Some(path) = &cli.json {
        std::fs::write(path, results.json())
            .map_err(|e| format!("cannot write {}: {e}", path.display()))?;
        eprintln!("Results written to {}", path.display());
    }
    Ok(())
}

/// Pin load thread `index` to `core + index`, if asked. After its
/// connections are up: a DPDK NIC's EAL pins the thread that initialises
/// it to its main lcore, which would undo a pin taken before.
fn pin(core: Option<usize>, index: usize) -> Result<(), String> {
    let Some(core) = core else {
        return Ok(());
    };
    let wanted = core + index;
    let pinned = melin_app::affinity::pin_to_core(wanted)
        .map_err(|e| format!("--core {core}: load thread {index} on core {wanted}: {e}"))?;
    eprintln!("load thread {index} pinned to core {pinned}");
    Ok(())
}

/// Kernel sockets, one per client, each writing a pass's requests at once,
/// on a clock every thread of the run shares.
struct Kernel {
    // A `Vec` indexed by the thread's client: the loop visits every one in
    // turn.
    sockets: Vec<Batched<NonBlockingTcp>>,
    origin: Instant,
}

impl Kernel {
    /// Dial a socket to `addr` for each of `clients`, each holding up to
    /// `window` requests of `frame_len` bytes between writes, on a clock
    /// counting from `origin`.
    fn connect(
        addr: SocketAddr,
        clients: &[ClientId],
        window: usize,
        frame_len: usize,
        origin: Instant,
    ) -> Result<Self, String> {
        let sockets = clients
            .iter()
            .map(|client| {
                NonBlockingTcp::connect(addr, DEFAULT_TIMEOUT)
                    .map(|socket| Batched::new(socket, window, frame_len))
                    .map_err(|e| format!("client {client}: cannot connect to {addr}: {e}"))
            })
            .collect::<Result<_, _>>()?;
        Ok(Self { sockets, origin })
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
    threads: usize,
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
            "=== {} — {} clients × window {}, {} load thread(s) ===\n  throughput: {:.0} \
             requests/sec ({} timed)\n",
            self.app.name(),
            self.clients,
            self.window,
            self.threads,
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
            "{{\"label\":\"{}\",\"clients\":{},\"window\":{},\"threads\":{},\"measured_requests\":{},\
             \"completed_requests\":{},\"warmup_ms\":{},\"measured_ms\":{},\"cooldown_ms\":{},\"wall_ms\":{},\
             \"throughput_ops\":{:.1},\"latency\":{{{percentiles}}}}}",
            self.app.name(),
            self.clients,
            self.window,
            self.threads,
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
