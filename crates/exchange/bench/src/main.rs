//! Trading engine benchmark suite with three modes:
//!
//! **`--mode=roundtrip`** (default): Full end-to-end benchmark. By default, boots
//! the server in-process and connects via TCP loopback. With `--addr=<ip:port>`,
//! connects to a remote engine instead (LAN benchmark mode). With `--uds`,
//! uses Unix domain sockets. Measures client-perceived latency including
//! transport, queuing, journaling, and matching.
//!
//! **`--mode=pipeline`**: Server pipeline without network transport. Publishes
//! events directly to the disruptor ring buffer and consumes responses from the
//! output SPSC queue. Isolates journal + matching stage latency from TCP/UDS
//! overhead.
//!
//! **`--mode=engine`**: Matching engine only. Calls `Exchange::execute()` directly
//! in a tight loop — no disruptor, no journal, no I/O. Measures pure matching
//! engine throughput and latency.
//!
//! All modes use the realistic order flow generator: a mix of limit orders
//! and cancels with power-law price/size distributions, multiple accounts,
//! and resting book depth. Orders are generated on-the-fly inside the hot
//! loop so memory stays bounded regardless of run length.
//!
//! Run length is wall-clock-driven. Each phase is a duration:
//!
//! * `--warmup-duration` (default 5s) — primes caches; samples discarded.
//! * `--duration`        (default 60s) — measured into the histogram.
//! * `--cooldown-duration` (default 5s) — drains the journal/network tail;
//!   samples discarded.
//!
//! Completions are classified by *receive time* against shared phase
//! deadlines, so all bench threads agree on which phase a sample belongs
//! to without further coordination.
//!
//! Usage:
//!     cargo run --release --bin melin-bench -- \
//!         [--mode=roundtrip|pipeline|engine] [--uds] [--addr=<ip:port>] \
//!         [--health-addr=<ip:port>] [--clients=N] [--window=N] \
//!         [--bench-threads=N] [--warmup-duration=5s] [--duration=60s] \
//!         [--cooldown-duration=5s] [--group-commit-us=N]
//!
//! Default: roundtrip mode, TCP transport, 60 s measured.

// Under `--features dpdk`, the entire TCP-path code in this file is
// unreachable from the dispatch in `main`. Suppress the resulting
// dead-code warnings rather than cfg-gating every TCP helper
// individually.
#![cfg_attr(feature = "dpdk", allow(dead_code))]

mod generator;
mod health_poller;
mod stats_client;

#[cfg(feature = "dpdk")]
mod dpdk;

/// jemalloc: thread-local caches eliminate allocator lock contention,
/// giving more predictable latency than glibc malloc under high throughput.
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(not(feature = "dpdk"))]
use std::collections::VecDeque;

#[cfg(not(feature = "dpdk"))]
use std::io::Write;
use std::num::NonZeroU64;
#[cfg(not(feature = "dpdk"))]
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;

#[cfg(not(feature = "dpdk"))]
use melin_protocol::codec;
#[cfg(not(feature = "dpdk"))]
use melin_protocol::message::ResponseKind;
#[cfg(not(feature = "dpdk"))]
use melin_protocol::transport::BlockingTransportListener;
#[cfg(not(feature = "dpdk"))]
use melin_server::server::ServerConfig;
use melin_types::types::*;

/// Number of completed orders between latency time-series samples.
/// Each sample captures interval p99/p99.9 (reset after each sample),
/// giving temporal variation rather than cumulative smoothing.
const SAMPLE_INTERVAL: usize = 1_000;

/// Default measured-phase duration.
const DEFAULT_DURATION: Duration = Duration::from_secs(60);

/// Default warmup duration — primes caches, branch predictors, allocator
/// arenas, and the disruptor ring before measurement starts.
const DEFAULT_WARMUP: Duration = Duration::from_secs(5);

/// Default cooldown duration — drains the final fsync-tail batches whose
/// per-event cost isn't amortised across a full window. The samples
/// recorded during cooldown are discarded.
const DEFAULT_COOLDOWN: Duration = Duration::from_secs(5);

/// Default number of orders in flight simultaneously per client. Controls the
/// level of pipelining — enough to keep the server pipeline saturated (journal +
/// matching stages overlap), small enough that per-order latency reflects
/// actual processing time rather than unbounded queueing.
const DEFAULT_WINDOW: usize = 64;

/// Default number of concurrent client connections.
const DEFAULT_CLIENTS: usize = 16;

/// Default number of bench client threads. Each thread manages a subset of
/// connections via io_uring. Pinned to cores 7-10 (2 physical + 2 HT siblings
/// on 8C/16T). With 4 bench + 6 server (3 pipeline + 2 reader + 1 repl-sender)
/// = 10 pinned threads total, leaving core 0 for OS/IRQ.
const DEFAULT_BENCH_THREADS: usize = 4;

/// Maximum frame payload size (matches protocol).
#[cfg(not(feature = "dpdk"))]
const MAX_FRAME_SIZE: usize = 1024;

/// Clap value parser: accept any humantime-recognised duration (`30s`,
/// `2m`, `500ms`, …). Surfaces parse errors as clap-friendly strings.
fn parse_duration(s: &str) -> Result<humantime::Duration, String> {
    s.parse::<humantime::Duration>()
        .map_err(|e| format!("invalid duration `{s}`: {e}"))
}

/// `BenchPhases` carries the three wall-clock durations that drive every
/// bench loop: warmup (priming), measured (recorded into the histogram),
/// and cooldown (final drain whose samples are discarded).
#[derive(Clone, Copy)]
pub(crate) struct BenchPhases {
    pub warmup: Duration,
    pub measured: Duration,
    pub cooldown: Duration,
}

impl BenchPhases {
    /// Deadlines relative to a shared `start` instant.
    pub(crate) fn deadlines(self, start: Instant) -> PhaseDeadlines {
        let warmup_end = start + self.warmup;
        let measured_end = warmup_end + self.measured;
        let cooldown_end = measured_end + self.cooldown;
        PhaseDeadlines {
            warmup_end,
            measured_end,
            cooldown_end,
        }
    }
}

/// Wall-clock cutoffs for the three phases.
#[derive(Clone, Copy)]
pub(crate) struct PhaseDeadlines {
    pub warmup_end: Instant,
    pub measured_end: Instant,
    pub cooldown_end: Instant,
}

// ---------------------------------------------------------------------------
// TSC (Time Stamp Counter) utilities for low-overhead per-order timing
// ---------------------------------------------------------------------------

/// Read the TSC with a serializing instruction (`rdtscp`). Returns raw tick
/// count. ~4ns overhead vs ~15-25ns for `Instant::now()` via vDSO.
/// `rdtscp` waits for all prior instructions to complete before reading,
/// preventing the CPU from reordering the timestamp relative to the work
/// being measured.
#[cfg(target_arch = "x86_64")]
#[inline(always)]
pub(crate) fn rdtscp() -> u64 {
    unsafe {
        let mut _aux: u32 = 0;
        core::arch::x86_64::__rdtscp(&mut _aux)
    }
}

/// Read the ARM virtual counter (`cntvct_el0`). ~2-5ns overhead,
/// equivalent to x86's `rdtscp`. An `isb` (instruction synchronization
/// barrier) serializes the pipeline to prevent reordering the read
/// relative to the work being measured.
#[cfg(target_arch = "aarch64")]
#[inline(always)]
pub(crate) fn rdtscp() -> u64 {
    let cnt: u64;
    unsafe {
        core::arch::asm!(
            "isb",
            "mrs {}, cntvct_el0",
            out(reg) cnt,
            options(nostack, nomem),
        );
    }
    cnt
}

/// Calibrate TSC/counter ticks per nanosecond by measuring a short sleep
/// against `Instant::now()`. Returns the conversion factor (ticks / ns).
#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
pub(crate) fn calibrate_tsc() -> f64 {
    calibrate_tsc_clock().ticks_per_ns
}

/// Anchored TSC clock: ticks-per-ns plus a `(tsc, unix_ns)` pair captured at
/// calibration time. Lets the hot path turn any later `rdtscp()` reading
/// into a UNIX-nanos timestamp without a `clock_gettime()` vDSO call —
/// previously `~25 ns` per event and visible in flamegraphs as ~6 % of
/// the bench's `pipeline-pub` thread.
///
/// Two sources of error to be aware of when reading derived timestamps:
///
/// - **Anchor-capture offset** (~30–50 ns, constant): the calibration
///   loop reads `unix_ns` first and the TSC second, so derived values
///   undershoot truth by the time it takes one `clock_gettime` call to
///   complete (plus a few cycles of bookkeeping). Choosing
///   undershoot is deliberate — a "did we pass deadline X?" check
///   downstream falsing earlier is safer than falsing later.
/// - **Linear drift** from the calibration's `ticks_per_ns` measurement
///   error. On a 10 ms sleep against `Instant::now()`, that's typically
///   bounded by sleep jitter (single-digit µs) plus the host's TSC
///   stability vs the kernel's `CLOCK_MONOTONIC`. Empirically ~100 ppm
///   on this fleet, so a 60 s bench drifts up to ~6 ms — well below
///   anything `Exchange`'s GTD scheduler or SEC-04 rate limiter
///   exercises at the flows we publish, but a bench that toggles
///   either would need a fresher anchor.
#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
#[derive(Clone, Copy)]
pub(crate) struct TscClock {
    pub(crate) ticks_per_ns: f64,
    /// Inverse of `ticks_per_ns`, precomputed so the hot path uses
    /// multiplication instead of division (a few cycles per event).
    pub(crate) ns_per_tick: f64,
    /// TSC reading at calibration time. Pairs with `anchor_unix_ns`.
    pub(crate) anchor_tsc: u64,
    /// UNIX nanos at calibration time. Pairs with `anchor_tsc`.
    pub(crate) anchor_unix_ns: u64,
}

#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
impl TscClock {
    /// Convert a TSC reading taken later in this process to UNIX
    /// nanoseconds. Saturates at the anchor if `ts < anchor_tsc`
    /// (shouldn't happen on any monotonic counter, but defensive
    /// against unexpected CPU migrations on cores with un-synchronised
    /// TSCs). See the struct docs for the small constant offset and the
    /// linear drift bound.
    #[inline(always)]
    pub(crate) fn unix_ns(&self, ts: u64) -> u64 {
        let delta_ticks = ts.saturating_sub(self.anchor_tsc);
        self.anchor_unix_ns + (delta_ticks as f64 * self.ns_per_tick) as u64
    }
}

/// Calibrate TSC and capture an anchor pair (`tsc`, `unix_ns`) so the hot
/// path can derive wall-clock timestamps from `rdtscp()` alone.
///
/// `anchor_unix_ns` is sampled *before* `anchor_tsc` so the natural
/// inter-call delay (one vDSO `clock_gettime`, ~25–50 ns) pushes the
/// recorded UNIX-nanos slightly into the past relative to the TSC
/// anchor. The result: `TscClock::unix_ns(ts)` always returns a value
/// no later than what `clock_gettime` would have returned at the same
/// `ts`. See `TscClock` docs for the full error model.
#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
pub(crate) fn calibrate_tsc_clock() -> TscClock {
    // Warm up the counter path.
    for _ in 0..100 {
        let _ = rdtscp();
    }

    let duration = Duration::from_millis(10);
    // Order matters: capture `anchor_unix_ns` *before* `anchor_tsc` so
    // any inter-call slippage rounds derived timestamps earlier rather
    // than later (see fn docs).
    let anchor_unix_ns = melin_app::unix_epoch_nanos();
    let anchor_tsc = rdtscp();
    let t0_wall = Instant::now();
    std::thread::sleep(duration);
    let t1_tsc = rdtscp();
    let elapsed_ns = t0_wall.elapsed().as_nanos() as f64;
    let elapsed_tsc = (t1_tsc - anchor_tsc) as f64;
    let ticks_per_ns = elapsed_tsc / elapsed_ns;
    TscClock {
        ticks_per_ns,
        ns_per_tick: 1.0 / ticks_per_ns,
        anchor_tsc,
        anchor_unix_ns,
    }
}

/// Convert counter tick delta to nanoseconds using a pre-calibrated factor.
#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
#[inline(always)]
pub(crate) fn tsc_to_ns(ticks: u64, ticks_per_ns: f64) -> u64 {
    (ticks as f64 / ticks_per_ns) as u64
}

/// One latency time-series sample: interval percentiles at a point in time.
/// Captured every `SAMPLE_INTERVAL` completed orders using an interval
/// histogram (snapshot + reset), so each sample reflects recent behavior
/// rather than cumulative averages.
pub(crate) struct LatencySample {
    /// Seconds elapsed since measurement start.
    elapsed_secs: f64,
    /// Interval p99 latency in microseconds.
    p99_us: f64,
    /// Interval p99.9 latency in microseconds.
    p999_us: f64,
    /// Interval p99.99 latency in microseconds.
    p9999_us: f64,
}

/// Time-series of latency samples for chart display and stability plots.
pub(crate) type TimeSeries = Vec<LatencySample>;

/// Record a latency sample if `SAMPLE_INTERVAL` orders have accumulated
/// in the interval histogram. Resets the interval histogram after sampling.
pub(crate) fn maybe_sample(
    interval_hist: &mut Histogram<u64>,
    interval_count: &mut usize,
    series: &mut TimeSeries,
    start: Instant,
) {
    if *interval_count >= SAMPLE_INTERVAL {
        if !interval_hist.is_empty() {
            series.push(LatencySample {
                elapsed_secs: start.elapsed().as_secs_f64(),
                p99_us: interval_hist.value_at_quantile(0.99) as f64 / 1000.0,
                p999_us: interval_hist.value_at_quantile(0.999) as f64 / 1000.0,
                p9999_us: interval_hist.value_at_quantile(0.9999) as f64 / 1000.0,
            });
        }
        interval_hist.reset();
        *interval_count = 0;
    }
}

/// Benchmark CLI arguments.
#[derive(clap::Parser)]
#[command(name = "melin-bench", about = "Matching engine benchmark suite")]
struct BenchArgs {
    /// Benchmark mode: roundtrip (full server), pipeline (no network), engine (matching only).
    #[arg(long, default_value = "roundtrip")]
    mode: String,
    /// Use Unix domain sockets instead of TCP (roundtrip mode only).
    #[arg(long)]
    uds: bool,
    /// Connect to a remote engine instead of spawning an embedded server (roundtrip mode only).
    #[arg(long)]
    addr: Option<std::net::SocketAddr>,
    /// Length of the measured phase. Accepts humantime values
    /// (e.g. `30s`, `2m`, `500ms`).
    #[arg(long, default_value_t = humantime::Duration::from(DEFAULT_DURATION), value_parser = parse_duration)]
    duration: humantime::Duration,
    /// Orders in flight per client (pipelining depth).
    #[arg(long, default_value_t = DEFAULT_WINDOW)]
    window: usize,
    /// Number of concurrent client connections.
    #[arg(long, default_value_t = DEFAULT_CLIENTS)]
    clients: usize,
    /// Number of bench client threads. Each thread gets its own io_uring ring.
    #[arg(long, default_value_t = DEFAULT_BENCH_THREADS)]
    bench_threads: usize,
    /// Group commit coalescing delay in microseconds.
    #[arg(long, default_value_t = 0)]
    group_commit_us: u64,
    /// Warmup duration before measurement starts. Lets caches, branch
    /// predictors, and allocator arenas settle. Accepts humantime values.
    #[arg(long, default_value_t = humantime::Duration::from(DEFAULT_WARMUP), value_parser = parse_duration)]
    warmup_duration: humantime::Duration,
    /// Cooldown duration after measurement ends. The bench's final batch
    /// flushes a small number of events whose `fdatasync` cost isn't
    /// amortised across a full batch, inflating the run-max with a
    /// drain-tail artefact that doesn't reflect steady-state behaviour.
    /// Samples recorded during cooldown are discarded.
    #[arg(long, default_value_t = humantime::Duration::from(DEFAULT_COOLDOWN), value_parser = parse_duration)]
    cooldown_duration: humantime::Duration,
    /// Path for the journal file. Defaults to a temporary directory.
    /// Use this to place the journal on a dedicated disk for benchmarking.
    #[arg(long)]
    journal: Option<std::path::PathBuf>,
    /// Journal writer mode (`buffered` | `sector`). Defaults to
    /// `buffered`. `sector` is experimental — see
    /// docs/journal-writer-modes.md before benchmarking with it.
    #[arg(
        long,
        default_value_t = melin_server::JournalWriterMode::default(),
        value_parser = melin_server::JournalWriterMode::parse,
    )]
    journal_writer: melin_server::JournalWriterMode,
    /// Number of trading accounts.
    #[arg(long, default_value_t = 10_000)]
    accounts: u32,
    /// Number of instruments.
    #[arg(long, default_value_t = 100)]
    instruments: u32,
    /// Write results to a JSON file. Useful for building saturation curves
    /// from multiple runs with different load levels.
    #[arg(long)]
    json: Option<std::path::PathBuf>,
    /// Path to a 32-byte raw Ed25519 private key file for authentication
    /// (required for remote mode with --addr, auto-generated for embedded).
    #[arg(long)]
    key: Option<std::path::PathBuf>,

    // --- DPDK options (only with --features dpdk) ---
    /// DPDK EAL arguments (space-separated).
    #[arg(long, default_value = "", allow_hyphen_values = true)]
    dpdk_eal_args: String,
    /// DPDK port IDs, comma-separated (default: "0"). For LACP bonds use "0,1".
    #[arg(long, default_value = "0", value_delimiter = ',')]
    dpdk_ports: Vec<u16>,
    /// Local IPv4 address for the DPDK bench interface.
    #[arg(long, default_value = "10.0.0.2")]
    dpdk_ip: String,
    /// IPv4 prefix length for the DPDK bench interface.
    #[arg(long, default_value_t = 24)]
    dpdk_prefix_len: u8,
    /// IPv4 gateway for the DPDK bench interface.
    #[arg(long)]
    dpdk_gateway: Option<String>,
    /// MTU for the DPDK interface. Use 9000 for jumbo frames. Must match server.
    #[arg(long, default_value_t = 1500)]
    dpdk_mtu: usize,
    /// VLAN ID for hardware strip/insert. Required for dedicated NIC mode.
    #[arg(long)]
    dpdk_vlan: Option<u16>,
    /// CPU core for the DPDK bench poll thread.
    #[arg(long, default_value_t = 7)]
    dpdk_core: usize,
    /// First CPU core for bench thread pinning. Thread i is pinned to core
    /// bench_cores + i. When omitted, bench threads are not pinned (OS
    /// scheduler decides). For local benchmarks use 7 (avoids server cores
    /// 1-6). For remote benchmarks on a dedicated machine, use 1 with
    /// isolcpus for tighter measurements.
    #[arg(long)]
    bench_cores: Option<usize>,
    /// Health endpoint address to poll for server metrics during the run
    /// (roundtrip mode only). For embedded mode, auto-detected from server
    /// config. For remote mode (`--addr`), must be provided explicitly.
    #[arg(long)]
    health_addr: Option<std::net::SocketAddr>,
    /// Maximum events per journal fsync batch (pipeline mode only). Smaller
    /// values reduce tail latency, larger values improve throughput with
    /// real fsync. Default 4096. Try 256 for low-latency no-persist runs.
    #[arg(long, default_value_t = 4096)]
    max_journal_batch: usize,
}

fn main() {
    // Initialize tracing so pipeline-stats and latency-trace output is visible.
    tracing_subscriber::fmt()
        .with_target(true)
        .with_thread_names(true)
        .init();

    let args = <BenchArgs as clap::Parser>::parse();
    let json_path = args.json.as_deref();
    let phases = BenchPhases {
        warmup: args.warmup_duration.into(),
        measured: args.duration.into(),
        cooldown: args.cooldown_duration.into(),
    };

    match args.mode.as_str() {
        "engine" => {
            run_engine_bench(phases, args.accounts, args.instruments, json_path);
        }
        "pipeline" => {
            run_pipeline_bench(
                phases,
                args.window,
                args.group_commit_us,
                args.journal,
                json_path,
                args.max_journal_batch,
                args.journal_writer,
            );
        }
        "roundtrip" => {
            #[cfg(feature = "dpdk")]
            {
                let addr = args.addr.unwrap_or_else(|| {
                    eprintln!("error: --addr is required for DPDK mode (no embedded server)");
                    std::process::exit(1);
                });
                let key_path = args.key.as_deref().unwrap_or_else(|| {
                    eprintln!("error: --key is required for DPDK mode");
                    std::process::exit(1);
                });
                let key = load_signing_key(key_path);

                dpdk::run_dpdk_roundtrip(
                    dpdk::DpdkBenchConfig {
                        eal_args: args
                            .dpdk_eal_args
                            .split_whitespace()
                            .map(String::from)
                            .collect(),
                        port_ids: args.dpdk_ports.clone(),
                        local_ip: args.dpdk_ip.parse().expect("invalid --dpdk-ip"),
                        prefix_len: args.dpdk_prefix_len,
                        gateway: args
                            .dpdk_gateway
                            .as_deref()
                            .map(|s| s.parse().expect("invalid --dpdk-gateway")),
                        server_addr: addr,
                        mtu: args.dpdk_mtu,
                        vlan_id: args.dpdk_vlan,
                    },
                    phases,
                    args.window,
                    args.clients,
                    json_path,
                    &key,
                    args.accounts,
                    args.instruments,
                    args.dpdk_core,
                    args.health_addr,
                );
            }

            #[cfg(not(feature = "dpdk"))]
            {
                run_roundtrip_bench(
                    args.uds,
                    phases,
                    args.window,
                    args.clients,
                    args.bench_threads,
                    args.group_commit_us,
                    args.addr,
                    args.journal,
                    args.accounts,
                    args.instruments,
                    json_path,
                    args.key.as_deref(),
                    args.bench_cores,
                    args.health_addr,
                );
            }
        }
        other => {
            eprintln!("unknown mode: {other} (expected: engine, pipeline, roundtrip)");
            std::process::exit(1);
        }
    }
}

// ===========================================================================
// Engine-only benchmark
// ===========================================================================

/// Engine-only benchmark with realistic order flow. Calls `Exchange::execute()`
/// and `Exchange::cancel()` directly in a tight loop — no disruptor, no journal,
/// no I/O. Uses the generator to produce a mix of limit orders and cancels with
/// power-law price/size distributions, multiple accounts, and resting book depth.
/// Orders are generated on-the-fly inside the loop; `next_event()` is invoked
/// *before* the per-order `rdtscp()` so RNG cost stays outside the measured
/// window.
fn run_engine_bench(
    phases: BenchPhases,
    num_accounts: u32,
    num_instruments: u32,
    json_path: Option<&std::path::Path>,
) {
    use generator::{GeneratedEvent, GeneratorConfig, OrderFlowGenerator};

    #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
    let ticks_per_ns = calibrate_tsc();
    #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
    eprintln!(
        "TSC calibration: {:.3} GHz ({:.2} ticks/ns)",
        ticks_per_ns, ticks_per_ns
    );

    let config = GeneratorConfig {
        num_accounts,
        num_instruments,
        ..Default::default()
    };

    let mut exchange = melin_engine::exchange::Exchange::with_capacity();

    // Register instruments.
    for i in 1..=num_instruments {
        exchange.add_instrument(InstrumentSpec {
            symbol: Symbol(i),
            base: CurrencyId(i * 2 - 1),
            quote: CurrencyId(i * 2),
        });
    }

    // Provision all accounts with generous balances in all currencies.
    for acct in 1..=num_accounts {
        exchange.provision_account(AccountId(acct), u64::MAX / 4);
    }

    exchange.prefault();

    let mut flow = OrderFlowGenerator::new(config);

    let mut reports = Vec::with_capacity(256);
    let mut histogram =
        Histogram::<u64>::new_with_bounds(1, 10_000_000_000, 3).expect("histogram bounds");

    let phase_start = Instant::now();
    let deadlines = phases.deadlines(phase_start);

    // Warmup — drive the engine at full speed but discard timings. Polling
    // `Instant::now()` every iteration is fine because the warmup body is
    // already many hundreds of ns of work; the clock read is negligible
    // and stops the loop precisely without burning extra cycles.
    while Instant::now() < deadlines.warmup_end {
        reports.clear();
        let event = flow.next_event();
        match event {
            GeneratedEvent::Submit { symbol, order } => {
                exchange.execute(symbol, order, &mut reports);
            }
            GeneratedEvent::Cancel {
                symbol,
                account,
                order_id,
            } => {
                exchange.cancel(symbol, account, order_id, &mut reports);
            }
            GeneratedEvent::CancelReplace {
                symbol,
                account,
                order_id,
                new_price,
                new_quantity,
            } => {
                exchange.cancel_replace(
                    symbol,
                    account,
                    order_id,
                    new_price,
                    new_quantity,
                    &mut reports,
                );
            }
        }
    }

    // Measured run.
    let mut interval_hist =
        Histogram::<u64>::new_with_bounds(1, 10_000_000_000, 3).expect("interval histogram");
    let mut interval_count: usize = 0;
    let mut series: TimeSeries = Vec::new();

    let mut submits: u64 = 0;
    let mut cancels: u64 = 0;
    let mut amends: u64 = 0;

    // Track the N slowest orders for post-run diagnostics.
    // Min-heap by latency: the smallest is at the top so we can
    // efficiently evict it when a slower order arrives. Wrapped in a
    // local struct because `GeneratedEvent` isn't Ord — heap ordering
    // is by `latency_ns` only.
    const SLOWEST_N: usize = 10;
    #[derive(Clone, Copy)]
    struct SlowEntry {
        latency_ns: u64,
        event: GeneratedEvent,
        num_reports: usize,
        offset_us: u64,
    }
    impl PartialEq for SlowEntry {
        fn eq(&self, o: &Self) -> bool {
            self.latency_ns == o.latency_ns
        }
    }
    impl Eq for SlowEntry {}
    impl PartialOrd for SlowEntry {
        fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(o))
        }
    }
    impl Ord for SlowEntry {
        fn cmp(&self, o: &Self) -> std::cmp::Ordering {
            self.latency_ns.cmp(&o.latency_ns)
        }
    }
    let mut slowest: std::collections::BinaryHeap<std::cmp::Reverse<SlowEntry>> =
        std::collections::BinaryHeap::with_capacity(SLOWEST_N + 1);

    // Measured phase: record latencies until `measured_end` passes. We
    // poll `Instant::now()` only once per ~DEADLINE_POLL_INTERVAL
    // iterations because every per-order `Instant::now()` (~15-25 ns
    // vDSO) would inflate the engine measurement that we're trying to
    // capture in the hundreds-of-ns range. The slop is at most
    // `interval / throughput`; at 3 M ops/s × 1024 iters that's ~340 µs
    // of samples that could land just past `measured_end` and still be
    // recorded into the histogram. Negligible at any practical run
    // length, and `wall` is clamped to `phases.measured` below so
    // throughput math stays exact.
    let start = Instant::now();
    let mut iter_since_check: u32 = 0;
    const DEADLINE_POLL_INTERVAL: u32 = 1024;
    let mut measured_orders: u64 = 0;
    loop {
        if iter_since_check >= DEADLINE_POLL_INTERVAL {
            if Instant::now() >= deadlines.measured_end {
                break;
            }
            iter_since_check = 0;
        }
        iter_since_check += 1;
        reports.clear();
        let event = flow.next_event();

        #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
        let t0 = rdtscp();
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        let t0 = Instant::now();

        match event {
            GeneratedEvent::Submit { symbol, order } => {
                exchange.execute(symbol, order, &mut reports);
                submits += 1;
            }
            GeneratedEvent::Cancel {
                symbol,
                account,
                order_id,
            } => {
                exchange.cancel(symbol, account, order_id, &mut reports);
                cancels += 1;
            }
            GeneratedEvent::CancelReplace {
                symbol,
                account,
                order_id,
                new_price,
                new_quantity,
            } => {
                exchange.cancel_replace(
                    symbol,
                    account,
                    order_id,
                    new_price,
                    new_quantity,
                    &mut reports,
                );
                amends += 1;
            }
        }

        #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
        let elapsed_ns = tsc_to_ns(rdtscp() - t0, ticks_per_ns);
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        let elapsed_ns = t0.elapsed().as_nanos() as u64;

        histogram.record(elapsed_ns).expect("record");
        interval_hist.record(elapsed_ns).expect("record interval");
        interval_count += 1;
        measured_orders += 1;
        maybe_sample(&mut interval_hist, &mut interval_count, &mut series, start);

        // Track top-N slowest using a min-heap capped at SLOWEST_N.
        // Only compute wall-clock offset when actually inserting (rare path).
        if slowest.len() < SLOWEST_N {
            let offset_us = start.elapsed().as_micros() as u64;
            slowest.push(std::cmp::Reverse(SlowEntry {
                latency_ns: elapsed_ns,
                event,
                num_reports: reports.len(),
                offset_us,
            }));
        } else if let Some(&std::cmp::Reverse(SlowEntry {
            latency_ns: min_ns, ..
        })) = slowest.peek()
            && elapsed_ns > min_ns
        {
            let offset_us = start.elapsed().as_micros() as u64;
            slowest.pop();
            slowest.push(std::cmp::Reverse(SlowEntry {
                latency_ns: elapsed_ns,
                event,
                num_reports: reports.len(),
                offset_us,
            }));
        }
    }
    // Clamp to `phases.measured` so the reported throughput divisor
    // matches the configured measured-phase length even when the
    // deadline-poll slop overruns by up to `DEADLINE_POLL_INTERVAL`
    // iterations. Mirrors the cap in pipeline/roundtrip/DPDK paths.
    let wall = start.elapsed().min(phases.measured);

    // Cooldown — keep driving the engine to absorb any drain-tail
    // artefacts (none here in engine mode, but symmetric with the other
    // bench paths makes the phase model uniform). Samples are not
    // recorded; the histogram is sealed at this point.
    while Instant::now() < deadlines.cooldown_end {
        reports.clear();
        let event = flow.next_event();
        match event {
            GeneratedEvent::Submit { symbol, order } => {
                exchange.execute(symbol, order, &mut reports);
            }
            GeneratedEvent::Cancel {
                symbol,
                account,
                order_id,
            } => {
                exchange.cancel(symbol, account, order_id, &mut reports);
            }
            GeneratedEvent::CancelReplace {
                symbol,
                account,
                order_id,
                new_price,
                new_quantity,
            } => {
                exchange.cancel_replace(
                    symbol,
                    account,
                    order_id,
                    new_price,
                    new_quantity,
                    &mut reports,
                );
            }
        }
    }

    let total_events = submits + cancels + amends;
    let cancel_pct = if total_events > 0 {
        cancels as f64 / total_events as f64 * 100.0
    } else {
        0.0
    };
    let amend_pct = if total_events > 0 {
        amends as f64 / total_events as f64 * 100.0
    } else {
        0.0
    };

    print_results(
        "Realistic Order Flow",
        measured_orders as usize,
        phases,
        &histogram,
        wall,
        &[
            format!("  Accounts: {num_accounts}, Instruments: {num_instruments}"),
            format!(
                "  Submits: {submits}, Cancels: {cancels} ({cancel_pct:.1}%), Amends: {amends} ({amend_pct:.1}%)"
            ),
        ],
        json_path,
        &series,
        &[],
        // Engine mode runs the matching engine in-process with no
        // server / health endpoint, so there's nothing to fetch.
        &stats_client::Body::Empty,
    );

    // Print the slowest orders for tail latency diagnosis.
    let mut sorted: Vec<_> = slowest.into_iter().map(|std::cmp::Reverse(e)| e).collect();
    sorted.sort_by_key(|b| std::cmp::Reverse(b.latency_ns)); // descending by latency
    println!("\n  Slowest {SLOWEST_N} Orders");
    for entry in &sorted {
        let latency_us = entry.latency_ns as f64 / 1000.0;
        let offset_ms = entry.offset_us as f64 / 1000.0;
        let event = entry.event;
        let num_reports = entry.num_reports;
        println!("    {latency_us:>7.2}µs  @{offset_ms:>7.1}ms  reports={num_reports}  {event:?}");
    }
}

// ===========================================================================
// Pipeline benchmark (disruptor + journal + matching, no network)
// ===========================================================================

/// Pipeline benchmark. Builds the full disruptor pipeline (journal stage +
/// matching stage) but bypasses TCP/UDS transport. The bench thread publishes
/// InputSlots directly to the input Producer and drains OutputSlots from the
/// SPSC consumer. Measures pipeline latency without network overhead.
#[allow(clippy::too_many_arguments)]
fn run_pipeline_bench(
    phases: BenchPhases,
    window: usize,
    group_commit_us: u64,
    journal_path: Option<std::path::PathBuf>,
    json_path: Option<&std::path::Path>,
    max_journal_batch: usize,
    journal_writer_mode: melin_server::JournalWriterMode,
) {
    use melin_server::{BufferedWriter, JournalWriterMode, SectorWriter};

    // Set up exchange with one instrument and funded account.
    let mut app =
        melin_server::exchange_app::ServerApp(melin_engine::exchange::Exchange::with_capacity());
    app.add_instrument(InstrumentSpec {
        symbol: Symbol(1),
        base: CurrencyId(1),
        quote: CurrencyId(2),
    });
    app.deposit(AccountId(1), CurrencyId(1), u64::MAX / 2);
    app.deposit(AccountId(1), CurrencyId(2), u64::MAX / 2);
    app.prefault();

    let tmp_dir = tempdir();
    let effective_journal = journal_path.unwrap_or_else(|| tmp_dir.join("pipeline-bench.journal"));

    let cfg = PipelineInnerCfg {
        group_commit_us,
        max_journal_batch,
        phases,
        window,
        json_path,
    };

    // The two arms are forced by monomorphisation — each writer has
    // its own journal-stage loop (`run_sync` for buffered,
    // `run_uring` for sector), so we cannot construct a single
    // `dyn` writer and call once.
    match journal_writer_mode {
        JournalWriterMode::Buffered => run_pipeline_inner(
            app,
            BufferedWriter::create(&effective_journal).expect("create journal"),
            cfg,
        ),
        JournalWriterMode::Sector => run_pipeline_inner(
            app,
            SectorWriter::create(&effective_journal).expect("create journal"),
            cfg,
        ),
    }

    let _ = std::fs::remove_dir_all(&tmp_dir);
}

/// Non-writer args for [`run_pipeline_inner`]. Bundled so the two
/// monomorphised call sites in [`run_pipeline_bench`] stay one-liners.
struct PipelineInnerCfg<'a> {
    group_commit_us: u64,
    max_journal_batch: usize,
    phases: BenchPhases,
    window: usize,
    json_path: Option<&'a std::path::Path>,
}

/// Pipeline-mode body, generic over the journal writer so we get a
/// statically-dispatched `run_sync` or `run_uring` per writer.
fn run_pipeline_inner<W>(app: melin_server::App, writer: W, cfg: PipelineInnerCfg<'_>)
where
    W: melin_server::JournalWrite<melin_trading::trading_event::TradingEvent> + Send + 'static,
    melin_server::JournalStage<W>: melin_server::pipeline::JournalStageRun<
            melin_trading::trading_event::TradingEvent,
            Writer = W,
        >,
{
    use melin_server::InputSlot;
    use melin_server::JournalEvent;
    use melin_server::pipeline::{JournalStageRun, build_pipeline_with_replication};
    use melin_server::trace::mono_trace_ns;

    let PipelineInnerCfg {
        group_commit_us,
        max_journal_batch,
        phases,
        window,
        json_path,
    } = cfg;

    let nz = |v: u64| NonZeroU64::new(v).expect("non-zero");

    let group_commit_delay = Duration::from_micros(group_commit_us);
    let active_conns = Arc::new(AtomicU64::new(0));
    let mut out = build_pipeline_with_replication(
        app,
        writer,
        group_commit_delay,
        active_conns,
        false, // no replication
        max_journal_batch,
        melin_server::journal_replication::REPLICATION_RING_CAPACITY,
        true,  // busy_spin — match production default (yield_idle=false)
        false, // event_publisher
        false, // shadow
    );
    let mut output_consumer = out.output_consumers.pop().expect("response consumer");

    let shutdown = Arc::new(AtomicBool::new(false));

    // Spawn journal and matching stage threads.
    let shutdown_j = Arc::clone(&shutdown);
    let journal_stage = out.journal_stage;
    let journal_handle = std::thread::Builder::new()
        .name("journal".into())
        .spawn(move || {
            if let Err(e) = melin_app::affinity::pin_to_core(1) {
                eprintln!("warning: could not pin journal to core 1: {e}");
            }
            journal_stage.run(&shutdown_j)
        })
        .expect("spawn journal thread");

    let shutdown_m = Arc::clone(&shutdown);
    let matching_stage = out.matching_stage;
    let matching_handle = std::thread::Builder::new()
        .name("matching".into())
        .spawn(move || {
            if let Err(e) = melin_app::affinity::pin_to_core(2) {
                eprintln!("warning: could not pin matching to core 2: {e}");
            }
            matching_stage.run(&shutdown_m)
        })
        .expect("spawn matching thread");

    // Single shared start so both threads agree on warmup/measured/cooldown
    // deadlines. Pinned threads compute their own `Instant::now()` against
    // this clock without further coordination.
    let phase_start = Instant::now();
    let deadlines = phases.deadlines(phase_start);
    let pub_stop = Arc::new(AtomicBool::new(false));

    // Split publish and drain into separate threads so the publisher
    // keeps the disruptor fed while the drainer processes BatchEnds.
    // Without this, a single thread alternates publish→drain, starving
    // the journal stage between drain phases and halving throughput.
    //
    // Coordination: inflight counter (AtomicU64) for window gating,
    // lock-free SPSC ring for timestamps (publisher → drainer).
    // Using melin_disruptor::spsc instead of std::sync::mpsc::sync_channel
    // eliminates the mutex overhead per order (~2-5µs tail reduction).
    let inflight = Arc::new(AtomicU64::new(0));
    // TSC ticks instead of Instant::now() for the latency measurement
    // (~4 ns vs ~15-25 ns per timestamp). The clock also carries an
    // epoch pair so we derive the engine-facing `timestamp_ns` from the
    // same `rdtscp()` reading the latency histogram already uses,
    // removing the per-event `clock_gettime()` that previously
    // dominated the publisher thread's profile (~9 B cycles / 6 % of
    // its samples on a 30 s capture).
    let tsc_clock = calibrate_tsc_clock();
    let ticks_per_ns = tsc_clock.ticks_per_ns;
    // SPSC channel requires capacity >= 2; clamp so `--window=1` (useful
    // for isolating pure pipeline latency without queueing) doesn't panic.
    let ts_capacity = window.next_power_of_two().max(2);
    let (mut ts_tx, mut ts_rx) = melin_disruptor::spsc::channel::<u64>(ts_capacity);

    // Publisher thread: continuously feeds events into the disruptor.
    // `sequence: 0` — the journal stage allocates sequences in disruptor
    // cursor order at encode time.
    let mut producer = out.input_producer;
    let inflight_pub = Arc::clone(&inflight);
    let pub_stop_p = Arc::clone(&pub_stop);
    let publish_handle = std::thread::Builder::new()
        .name("pipeline-pub".into())
        .spawn(move || {
            if let Err(e) = melin_app::affinity::pin_to_core(3) {
                eprintln!("warning: could not pin pipeline-pub to core 3: {e}");
            }
            // Publish until the drain thread signals stop (set once the
            // cooldown deadline passes and the inflight queue is drained).
            // OrderId is a free-running u64; no risk of overflow at any
            // realistic bench duration.
            let mut i: u64 = 0;
            while !pub_stop_p.load(Ordering::Relaxed) {
                let order_id = OrderId(i + 1);
                let side = if i.is_multiple_of(2) {
                    Side::Buy
                } else {
                    Side::Sell
                };
                i += 1;

                // Spin-wait for window capacity OR a stop signal — we
                // must not block forever if the drain thread already
                // told us to stop while the window is full.
                while inflight_pub.load(Ordering::Acquire) >= window as u64 {
                    if pub_stop_p.load(Ordering::Relaxed) {
                        return;
                    }
                    std::hint::spin_loop();
                }

                let ts = rdtscp();
                producer.publish(InputSlot {
                    connection_id: 0,
                    key_hash: 0,
                    request_seq: 0,
                    sequence: 0,
                    timestamp_ns: tsc_clock.unix_ns(ts),
                    event: JournalEvent::App(
                        melin_trading::trading_event::TradingEvent::SubmitOrder {
                            symbol: Symbol(1),
                            order: Order {
                                id: order_id,
                                account: AccountId(1),
                                side,
                                order_type: OrderType::Limit {
                                    price: Price(nz(100)),
                                    post_only: false,
                                },
                                time_in_force: TimeInForce::GTC,
                                quantity: Quantity(nz(1)),
                                stp: SelfTradeProtection::Allow,
                                expiry_ns: 0,
                            },
                        },
                    ),
                    publish_ts: mono_trace_ns(),
                    recv_ts: mono_trace_ns(),
                });
                inflight_pub.fetch_add(1, Ordering::Release);
                ts_tx.publish(ts);
            }
        })
        .expect("spawn pipeline publish thread");

    // Drain thread (this thread): consume output SPSC and record latency.
    let mut histogram =
        Histogram::<u64>::new_with_bounds(1, 10_000_000_000, 3).expect("histogram bounds");
    let mut measured_orders: u64 = 0;
    let mut measured_start: Option<Instant> = None;
    let start = phase_start;

    // Drain until cooldown ends. Classify each completion by *receive*
    // time against `deadlines`: anything within `[warmup_end, measured_end)`
    // contributes to the histogram. We don't gate the queue read on the
    // deadline — the inflight ring may still be draining when we exit.
    loop {
        let now = Instant::now();
        if now >= deadlines.cooldown_end {
            break;
        }
        let Some((_seq, slot)) = output_consumer.try_consume() else {
            std::hint::spin_loop();
            continue;
        };
        // The matching stage now signals end-of-request via the
        // `is_last_in_request` flag on the final slot for one input
        // event, instead of a separate `OutputPayload::BatchEnd` slot.
        // The flag is set on the last Report (or QueryResponse, or
        // a BatchEnd-payload slot when the event produced no payload).
        if slot.is_last_in_request {
            let (_, sent_at) = loop {
                if let Some(v) = ts_rx.try_consume() {
                    break v;
                }
                std::hint::spin_loop();
            };
            inflight.fetch_sub(1, Ordering::Release);
            let latency_ns = tsc_to_ns(rdtscp() - sent_at, ticks_per_ns);
            if now >= deadlines.warmup_end && now < deadlines.measured_end {
                if measured_start.is_none() {
                    measured_start = Some(now);
                }
                histogram.record(latency_ns).expect("record");
                measured_orders += 1;
            }
        }
    }

    // Tell the publisher to stop and join it. The publisher checks the
    // flag both at top-of-loop and inside its window-spin, so it cannot
    // be stuck waiting forever even with a full inflight window.
    pub_stop.store(true, Ordering::Relaxed);
    publish_handle.join().expect("publisher thread");

    let end = Instant::now();
    let measured_wall = measured_start
        .map(|s| end.duration_since(s).min(phases.measured))
        .unwrap_or_else(|| start.elapsed());

    // Shutdown pipeline threads.
    shutdown.store(true, Ordering::Relaxed);

    let mut extra_lines = Vec::new();
    if group_commit_us > 0 {
        extra_lines.push(format!("  Group commit delay: {group_commit_us} µs"));
    }
    extra_lines.push(format!("  Window: {window}"));

    print_results(
        "Pipeline (no network)",
        measured_orders as usize,
        phases,
        &histogram,
        measured_wall,
        &extra_lines,
        json_path,
        &Vec::new(),
        &[],
        // Pipeline mode runs the disruptor stages in-process with no
        // server / health endpoint, so there's nothing to fetch.
        &stats_client::Body::Empty,
    );

    println!();
    println!("=== Pipeline Latency Trace ===");
    println!();

    // Wait for pipeline threads to finish and print trace reports.
    let _ = journal_handle.join();
    let _ = matching_handle.join();
}

// ===========================================================================
// Roundtrip benchmark (full server with network transport)
// ===========================================================================

/// Full end-to-end roundtrip benchmark through the server with TCP or UDS.
///
/// When `remote_addr` is `Some`, connects to a remote engine instead of
/// spawning an embedded server. This is the mode used for LAN benchmarks
/// where the engine runs on a separate machine.
#[allow(clippy::too_many_arguments)]
#[cfg(not(feature = "dpdk"))]
fn run_roundtrip_bench(
    use_uds: bool,
    phases: BenchPhases,
    window: usize,
    num_clients: usize,
    bench_threads: usize,
    group_commit_us: u64,
    remote_addr: Option<std::net::SocketAddr>,
    journal_path: Option<std::path::PathBuf>,
    num_accounts: u32,
    num_instruments: u32,
    json_path: Option<&std::path::Path>,
    key_path: Option<&std::path::Path>,
    bench_core_start: Option<usize>,
    health_addr: Option<std::net::SocketAddr>,
) {
    // Remote mode: connect to an external engine, no embedded server.
    if let Some(addr) = remote_addr {
        if use_uds {
            eprintln!("error: --addr and --uds are mutually exclusive");
            std::process::exit(1);
        }

        let key_path = key_path.unwrap_or_else(|| {
            eprintln!("error: --key is required for remote mode (--addr)");
            std::process::exit(1);
        });
        let key = load_signing_key(key_path);
        let shutdown = Arc::new(AtomicBool::new(false));

        let connect = || {
            let stream = connect_tcp(addr);
            stream.set_nodelay(true).expect("set TCP_NODELAY");
            let read_stream = stream.try_clone().expect("clone TCP stream");
            (read_stream, stream)
        };

        run_roundtrip_inner(
            connect,
            &format!("TCP {addr}"),
            phases,
            window,
            num_clients,
            bench_threads,
            group_commit_us,
            shutdown,
            json_path,
            &key,
            num_accounts,
            num_instruments,
            bench_core_start,
            health_addr,
        );
        return;
    }

    // Local mode: spawn an embedded server.
    // Generate a deterministic bench key and matching authorized_keys file.
    let bench_key = ed25519_dalek::SigningKey::from_bytes(&[0xBE; 32]);
    let tmp_dir = tempdir();
    let keys_path = tmp_dir.join("authorized_keys");
    let pub_key_b64 = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        bench_key.verifying_key().to_bytes(),
    );
    std::fs::write(&keys_path, format!("trader {pub_key_b64} bench\n"))
        .expect("write authorized_keys");

    let effective_journal = journal_path.unwrap_or_else(|| tmp_dir.join("bench.journal"));

    let config = ServerConfig {
        journal: effective_journal,
        snapshot: None,
        group_commit_us,
        accounts: num_accounts,
        instruments: num_instruments,
        // Disable connection timeout for benchmarks — pre-generation
        // can take longer than the default 30s for large runs.
        connection_timeout_secs: 0,
        authorized_keys: keys_path,
        // Single-node durability for the embedded bench server: ack on
        // local persistence alone. The default `Hybrid` mode waits for
        // `in_memory>=2` replica acks that never arrive when nothing else
        // is connected, which would stall every response.
        durability_mode: melin_server::durability_policy::DurabilityMode::Local,
        ..ServerConfig::default()
    };

    let shutdown = Arc::new(AtomicBool::new(false));

    // Capture health bind address before config is moved into the server thread.
    let effective_health_addr = health_addr.or(config.health_bind);

    if use_uds {
        use melin_protocol::uds::BlockingUdsListener;

        let sock_path = tmp_dir.join("bench.sock");
        let listener = BlockingUdsListener::bind(&sock_path).expect("bind UDS");
        start_server(listener, config, Arc::clone(&shutdown));

        let sock_path_ref = &sock_path;
        let connect = || {
            let stream = connect_uds(sock_path_ref);
            let read_stream = stream.try_clone().expect("clone UDS stream");
            (read_stream, stream)
        };

        run_roundtrip_inner(
            connect,
            "Unix domain socket",
            phases,
            window,
            num_clients,
            bench_threads,
            group_commit_us,
            shutdown,
            json_path,
            &bench_key,
            num_accounts,
            num_instruments,
            bench_core_start,
            effective_health_addr,
        );
    } else {
        use melin_protocol::tcp::BlockingTcpListener;

        let listener = BlockingTcpListener::bind("127.0.0.1:0".parse().expect("valid addr"))
            .expect("bind TCP");
        let addr = listener.local_addr().expect("local addr");
        start_server(listener, config, Arc::clone(&shutdown));

        let connect = || {
            let stream = connect_tcp(addr);
            stream.set_nodelay(true).expect("set TCP_NODELAY");
            let read_stream = stream.try_clone().expect("clone TCP stream");
            (read_stream, stream)
        };

        run_roundtrip_inner(
            connect,
            "TCP loopback",
            phases,
            window,
            num_clients,
            bench_threads,
            group_commit_us,
            shutdown,
            json_path,
            &bench_key,
            num_accounts,
            num_instruments,
            bench_core_start,
            effective_health_addr,
        );
    }

    let _ = std::fs::remove_dir_all(&tmp_dir);
}

/// Load a 32-byte raw Ed25519 private key from a file.
fn load_signing_key(path: &std::path::Path) -> ed25519_dalek::SigningKey {
    let bytes = std::fs::read(path)
        .unwrap_or_else(|e| panic!("cannot read key file {}: {e}", path.display()));
    if bytes.len() != 32 {
        panic!(
            "key file must be exactly 32 bytes (raw Ed25519 seed), got {}",
            bytes.len()
        );
    }
    let mut seed = [0u8; 32];
    seed.copy_from_slice(&bytes);
    ed25519_dalek::SigningKey::from_bytes(&seed)
}

/// Start the server on a background thread. The listener is already bound,
/// so the client can connect immediately (connections queue in the kernel
/// backlog until the server calls `accept()`).
#[cfg(not(feature = "dpdk"))]
fn start_server<L: BlockingTransportListener>(
    listener: L,
    config: ServerConfig,
    shutdown: Arc<AtomicBool>,
) {
    std::thread::Builder::new()
        .name("server".into())
        .spawn(move || {
            if let Err(e) = melin_server::server::run_with_shutdown(listener, config, shutdown) {
                eprintln!("server error: {e}");
            }
        })
        .expect("spawn server thread");
}

/// Connect to TCP server with retry (up to 50 attempts, 10ms apart).
///
/// Also enables `SO_BUSY_POLL` on the connected socket. The bench's
/// io_uring loop already busy-spins on CQEs, so the kernel's NIC
/// busy-poll uses cycles that would otherwise be wasted spinning, and
/// it removes the softirq → wakeup handoff from every server response
/// — tightening the bench's measurement floor so we observe the
/// server's true latency rather than the bench's own client-side
/// scheduler jitter.
#[cfg(not(feature = "dpdk"))]
fn connect_tcp(addr: std::net::SocketAddr) -> std::net::TcpStream {
    use std::os::unix::io::AsRawFd;
    let mut last_err = None;
    for _ in 0..50 {
        match std::net::TcpStream::connect(addr) {
            Ok(s) => {
                // Best-effort SO_BUSY_POLL; failure is logged via stderr
                // but does not abort the bench (the kernel may reject
                // it without CAP_NET_ADMIN, in which case we measure
                // with the default scheduler-wakeup cost — still
                // accurate, just slightly noisier).
                let val: libc::c_int = 50;
                let rc = unsafe {
                    libc::setsockopt(
                        s.as_raw_fd(),
                        libc::SOL_SOCKET,
                        libc::SO_BUSY_POLL,
                        &val as *const libc::c_int as *const libc::c_void,
                        std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                    )
                };
                if rc != 0 {
                    let err = std::io::Error::last_os_error();
                    eprintln!("warning: SO_BUSY_POLL setsockopt failed: {err}");
                }
                return s;
            }
            Err(e) => {
                last_err = Some(e);
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }
    panic!("failed to connect after 50 attempts: {}", last_err.unwrap());
}

/// Perform challenge-response auth handshake on a new connection.
/// Must be called before the stream is set to non-blocking mode.
#[cfg(not(feature = "dpdk"))]
fn auth_handshake(
    stream: &mut (impl std::io::Read + std::io::Write),
    key: &ed25519_dalek::SigningKey,
) {
    use ed25519_dalek::Signer;
    use melin_protocol::message::Request;

    // Read Challenge frame.
    let mut len_buf = [0u8; 4];
    std::io::Read::read_exact(stream, &mut len_buf).expect("read Challenge length");
    let len = u32::from_le_bytes(len_buf) as usize;
    assert!(len <= MAX_FRAME_SIZE, "Challenge frame too large: {len}");
    let mut payload = [0u8; 128];
    std::io::Read::read_exact(stream, &mut payload[..len]).expect("read Challenge payload");
    let response = codec::decode_response(&payload[..len]).expect("decode Challenge");
    let nonce = match response {
        ResponseKind::Challenge { nonce } => nonce,
        other => panic!("expected Challenge, got {other:?}"),
    };

    // Sign nonce + ephemerals (TCP bench uses zero ephs) — see
    // `melin_protocol::auth::auth_signing_payload`.
    let signing_payload = melin_protocol::auth::auth_signing_payload(&nonce);
    let signature = key.sign(&signing_payload);
    let request = Request::ChallengeResponse {
        signature: signature.to_bytes(),
        public_key: key.verifying_key().to_bytes(),
    };
    let mut buf = [0u8; 256];
    let written = codec::encode_request(&request, 0, &mut buf).expect("encode ChallengeResponse");
    std::io::Write::write_all(stream, &buf[..written]).expect("send ChallengeResponse");
    std::io::Write::flush(stream).expect("flush ChallengeResponse");

    // Read ServerReady.
    std::io::Read::read_exact(stream, &mut len_buf).expect("read ServerReady length");
    let len = u32::from_le_bytes(len_buf) as usize;
    assert!(len <= MAX_FRAME_SIZE, "ServerReady frame too large: {len}");
    std::io::Read::read_exact(stream, &mut payload[..len]).expect("read ServerReady payload");
    let response = codec::decode_response(&payload[..len]).expect("decode ServerReady");
    assert!(
        matches!(response, ResponseKind::ServerReady),
        "expected ServerReady, got {response:?}"
    );
}

/// Connect to UDS server with retry (up to 50 attempts, 10ms apart).
#[cfg(not(feature = "dpdk"))]
fn connect_uds(path: &std::path::Path) -> std::os::unix::net::UnixStream {
    let mut last_err = None;
    for _ in 0..50 {
        match std::os::unix::net::UnixStream::connect(path) {
            Ok(s) => return s,
            Err(e) => {
                last_err = Some(e);
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }
    panic!("failed to connect after 50 attempts: {}", last_err.unwrap());
}

// Orchestration
// ---------------------------------------------------------------------------

/// Create connections, distribute across bench threads, run, report results.
#[allow(clippy::too_many_arguments)]
#[cfg(not(feature = "dpdk"))]
fn run_roundtrip_inner<R, W, F>(
    connect: F,
    transport_name: &str,
    phases: BenchPhases,
    window: usize,
    num_clients: usize,
    bench_threads: usize,
    group_commit_us: u64,
    shutdown: Arc<AtomicBool>,
    json_path: Option<&std::path::Path>,
    key: &ed25519_dalek::SigningKey,
    num_accounts: u32,
    num_instruments: u32,
    bench_core_start: Option<usize>,
    health_addr: Option<std::net::SocketAddr>,
) where
    R: std::io::Read + std::io::Write + AsRawFd + Send + 'static,
    W: Write + AsRawFd + Send + 'static,
    F: Fn() -> (R, W) + Sync,
{
    run_uring_roundtrip(
        connect,
        transport_name,
        phases,
        window,
        num_clients,
        bench_threads,
        group_commit_us,
        shutdown,
        json_path,
        key,
        num_accounts,
        num_instruments,
        bench_core_start,
        health_addr,
    );
}

// ===========================================================================
// Progress reporting
// ===========================================================================

/// Spawn a background thread that prints periodic progress to stderr.
/// Returns a handle; the thread exits when `shutdown` is set to true.
///
/// Pinned to core 0 (OS/IRQ core) so it never preempts bench I/O threads.
/// Uses raw `write(2)` on fd 2 instead of `eprintln!` to avoid the stderr
/// mutex, which can block bench threads that also write to stderr.
pub(crate) fn spawn_progress_reporter(
    completed: Arc<AtomicU64>,
    phases: BenchPhases,
    shutdown: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    let total_duration = phases.warmup + phases.measured + phases.cooldown;
    std::thread::Builder::new()
        .name("progress".into())
        .spawn(move || {
            // Pin to core 0 so the progress thread never lands on a bench
            // core and causes involuntary preemption or TLB shootdowns.
            let _ = melin_app::affinity::pin_to_core(0);

            let start = Instant::now();
            let mut last_completed: u64 = 0;
            let mut last_time = start;
            // Print interval is 5s, but poll the shutdown flag every 100ms so
            // bench cleanup doesn't have to wait the full interval to exit.
            const PRINT_INTERVAL: Duration = Duration::from_secs(5);
            const POLL_INTERVAL: Duration = Duration::from_millis(100);

            'outer: loop {
                let mut waited = Duration::ZERO;
                while waited < PRINT_INTERVAL {
                    if shutdown.load(Ordering::Relaxed) {
                        break 'outer;
                    }
                    std::thread::sleep(POLL_INTERVAL);
                    waited += POLL_INTERVAL;
                }

                let now = Instant::now();
                let current = completed.load(Ordering::Relaxed);
                let dt = now.duration_since(last_time).as_secs_f64();
                let delta = current.saturating_sub(last_completed);
                let rate = delta as f64 / dt;
                let elapsed = now.duration_since(start).as_secs_f64();
                let total_secs = total_duration.as_secs_f64();
                let pct = if total_secs > 0.0 {
                    (elapsed / total_secs * 100.0).min(100.0)
                } else {
                    100.0
                };
                let phase = if elapsed < phases.warmup.as_secs_f64() {
                    "warmup"
                } else if elapsed < (phases.warmup + phases.measured).as_secs_f64() {
                    "measured"
                } else {
                    "cooldown"
                };

                // Format into a stack buffer and write(2) directly to fd 2.
                // Avoids the stderr mutex that eprintln! holds, which can
                // block bench threads doing eprintln! on error paths.
                use std::io::Write as _;
                let mut buf = [0u8; 192];
                let mut cursor = std::io::Cursor::new(&mut buf[..]);
                let _ = writeln!(
                    cursor,
                    "  [{elapsed:.1}s/{total_secs:.0}s {pct:.0}% {phase}] {current} measured orders  {:.0}K/s",
                    rate / 1000.0,
                );
                let len = cursor.position() as usize;
                // Best-effort write — progress display is non-critical.
                unsafe {
                    libc::write(2, buf.as_ptr() as *const libc::c_void, len);
                }

                last_completed = current;
                last_time = now;
            }
        })
        .expect("spawn progress thread")
}

// ===========================================================================
// io_uring roundtrip benchmark
// ===========================================================================

/// io_uring-based roundtrip benchmark. Each bench thread runs its own
/// io_uring ring with RECV for reads and SEND for writes.
#[cfg(not(feature = "dpdk"))]
#[allow(clippy::too_many_arguments)]
fn run_uring_roundtrip<R, W, F>(
    connect: F,
    transport_name: &str,
    phases: BenchPhases,
    window: usize,
    num_clients: usize,
    bench_threads: usize,
    group_commit_us: u64,
    shutdown: Arc<AtomicBool>,
    json_path: Option<&std::path::Path>,
    key: &ed25519_dalek::SigningKey,
    num_accounts: u32,
    num_instruments: u32,
    bench_core_start: Option<usize>,
    health_addr: Option<std::net::SocketAddr>,
) where
    R: std::io::Read + std::io::Write + AsRawFd + Send + 'static,
    W: Write + AsRawFd + Send + 'static,
    F: Fn() -> (R, W) + Sync,
{
    // Build a generator per client. With on-the-fly generation the loop
    // is never starved by a pre-allocated cap; phases are driven entirely
    // by the wall-clock deadlines defined by `phases`. Each generator
    // gets a non-overlapping `start_order_id` slice from `OrderId` space.
    //
    // `ORDER_ID_STRIDE` reserves a generous block per client so a long
    // bench at 10 M/s (≈ 6e11 orders/min) still fits a u64 slot without
    // colliding across clients. 2^48 ≈ 2.8e14 ids — three orders of
    // magnitude beyond any realistic run.
    const ORDER_ID_STRIDE: u64 = 1u64 << 48;
    let per_client: Vec<generator::OrderFlowGenerator> = (0..num_clients)
        .map(|client_id| {
            generator::OrderFlowGenerator::new(generator::GeneratorConfig {
                num_accounts,
                num_instruments,
                start_order_id: ORDER_ID_STRIDE * (client_id as u64) + 1,
                ..Default::default()
            })
        })
        .collect();
    eprintln!("  per-client generators initialised for {num_clients} clients");

    // Connect and auth all clients in parallel via rayon — independent
    // network handshakes that amortise nicely across a thread pool.
    use rayon::prelude::*;
    let setup_start = Instant::now();
    let connected: Vec<(R, W)> = (0..num_clients)
        .into_par_iter()
        .map(|_| {
            let (mut read_stream, write_stream) = connect();
            auth_handshake(&mut read_stream, key);
            (read_stream, write_stream)
        })
        .collect();
    eprintln!(
        "  all {num_clients} clients connected ({:.1}s)",
        setup_start.elapsed().as_secs_f64(),
    );

    let num_threads = bench_threads.min(num_clients);

    // Attach per-client generator and distribute round-robin across bench threads.
    let mut thread_conns: Vec<Vec<UringBenchConn>> = (0..num_threads).map(|_| Vec::new()).collect();
    for (i, ((read_stream, write_stream), flow)) in
        connected.into_iter().zip(per_client).enumerate()
    {
        let read_fd = read_stream.as_raw_fd();
        let write_fd = write_stream.as_raw_fd();

        thread_conns[i % num_threads].push(UringBenchConn {
            read_fd,
            write_fd,
            _read_owner: Box::new(read_stream),
            _write_owner: Box::new(write_stream),
            recv_buf: Box::new([0u8; URING_RECV_BUF_SIZE]),
            parse_buf: Vec::with_capacity(MAX_FRAME_SIZE + 4),
            recv_pending: false,
            send_buf: Vec::with_capacity(4096),
            send_pending: false,
            flow,
            inflight_ts: VecDeque::with_capacity(window),
        });
    }

    let progress = Arc::new(AtomicU64::new(0));
    let progress_shutdown = Arc::new(AtomicBool::new(false));
    let progress_handle = spawn_progress_reporter(
        Arc::clone(&progress),
        phases,
        Arc::clone(&progress_shutdown),
    );

    // Start health poller before bench threads.
    let health_poller = health_addr.map(health_poller::HealthPoller::start);

    // Shared start instant — every bench thread derives its phase
    // deadlines from this so they classify completions consistently.
    let start = Instant::now();
    let deadlines = phases.deadlines(start);

    // Spawn io_uring bench threads, each with its own ring and connection subset.
    let handles: Vec<_> = thread_conns
        .into_iter()
        .enumerate()
        .map(|(i, conns)| {
            let pin_core = bench_core_start.map(|s| s + i);
            let bench_start = start;
            let thread_progress = Arc::clone(&progress);
            std::thread::Builder::new()
                .name(format!("bench-{i}"))
                .spawn(move || {
                    if let Some(core_id) = pin_core
                        && let Err(e) = melin_app::affinity::pin_to_core(core_id)
                    {
                        eprintln!("warning: could not pin bench-{i} to core {core_id}: {e}");
                    }
                    run_uring_loop(conns, window, bench_start, deadlines, thread_progress)
                })
                .expect("spawn bench thread")
        })
        .collect();

    // Collect and merge histograms from all threads. Track the earliest
    // measured_start — measurement begins when the first thread exits
    // warmup, so the wall time covers all measured orders from all threads.
    let mut histogram =
        Histogram::<u64>::new_with_bounds(1, 10_000_000_000, 3).expect("histogram bounds");
    let mut earliest_measured_start: Option<Instant> = None;
    let mut all_series: TimeSeries = Vec::new();

    for handle in handles {
        let (h, s, ms) = handle.join().expect("bench thread panicked");
        histogram.add(&h).expect("merge histograms");
        if let Some(t) = ms {
            earliest_measured_start =
                Some(earliest_measured_start.map_or(t, |prev: Instant| prev.min(t)));
        }
        all_series.extend(s);
    }

    // Snapshot end time BEFORE joining the progress thread: that thread
    // sleeps in 5-second increments and only checks shutdown after each
    // sleep, so progress_handle.join() can block up to ~5s and would
    // otherwise inflate `measured_wall` for short benches.
    let end = Instant::now();

    // Stop progress reporter.
    progress_shutdown.store(true, Ordering::Relaxed);
    let _ = progress_handle.join();

    // Collect health samples.
    let health_samples = health_poller.map(|p| p.stop()).unwrap_or_default();

    // Measure throughput over the measured phase only — from when the
    // first thread finished warmup until either `end` (captured above,
    // pre-join) or `start + warmup + measured`, whichever is sooner.
    // `end` lands inside cooldown when threads exited via the wall-clock
    // deadline, so capping at `phases.measured` keeps the divisor
    // honest.
    let measured_wall = earliest_measured_start
        .map(|s| end.duration_since(s).min(phases.measured))
        .unwrap_or_else(|| start.elapsed());

    let mut extra_lines = Vec::new();
    if group_commit_us > 0 {
        extra_lines.push(format!("  Group commit delay: {group_commit_us} µs"));
    }
    extra_lines.push(format!("  Transport: {transport_name}"));
    extra_lines.push(if let Some(start) = bench_core_start {
        format!(
            "  Bench threads: {num_threads} (io_uring, cores {start}-{})",
            start + num_threads - 1,
        )
    } else {
        format!("  Bench threads: {num_threads} (io_uring, unpinned)")
    });
    extra_lines.push(format!("  Window: {window}, Clients: {num_clients}"));

    // Sort time-series by elapsed time for stable plot output.
    all_series.sort_by(|a, b| a.elapsed_secs.partial_cmp(&b.elapsed_secs).unwrap());

    // Fetch the server-side per-stage histogram dump before the
    // server (or its embedded form) shuts down. Best-effort — a
    // missing dump never aborts the run; print_results renders an
    // appropriate "feature off" / "no data" line instead.
    let server_stages = match health_addr {
        Some(addr) => stats_client::fetch(addr),
        None => stats_client::Body::Empty,
    };

    print_results(
        "Roundtrip",
        histogram.len() as usize,
        phases,
        &histogram,
        measured_wall,
        &extra_lines,
        json_path,
        &all_series,
        &health_samples,
        &server_stages,
    );

    println!();
    println!("=== Pipeline Latency Trace ===");
    println!();
    shutdown.store(true, Ordering::Relaxed);
    std::thread::sleep(Duration::from_millis(200));
}

/// Size of per-connection recv buffer for io_uring RECV.
#[cfg(not(feature = "dpdk"))]
const URING_RECV_BUF_SIZE: usize = 4096;

/// Flag bit in io_uring user_data to distinguish SEND from RECV CQEs.
/// Bit 63 set = SEND completion, clear = RECV completion.
#[cfg(not(feature = "dpdk"))]
const SEND_FLAG: u64 = 1 << 63;

/// Per-connection state for the io_uring benchmark event loop.
#[cfg(not(feature = "dpdk"))]
struct UringBenchConn {
    read_fd: RawFd,
    write_fd: RawFd,
    /// Owns the read half — keeps the fd alive.
    _read_owner: Box<dyn Send>,
    /// Owns the write half — keeps the fd alive.
    _write_owner: Box<dyn Send>,

    // Recv state
    recv_buf: Box<[u8; URING_RECV_BUF_SIZE]>,
    parse_buf: Vec<u8>,
    recv_pending: bool,

    // Send state
    send_buf: Vec<u8>,
    send_pending: bool,

    // Pipelining state — orders are generated on-the-fly. There is no
    // pre-allocated cap: the loop runs until the wall-clock cooldown
    // deadline expires.
    flow: generator::OrderFlowGenerator,
    /// TSC tick at send time. `u64` instead of `Instant` to avoid
    /// ~15-25ns vDSO overhead per timestamp on the hot path.
    inflight_ts: VecDeque<u64>,
}

/// io_uring event loop for all benchmark connections. Single-threaded:
/// uses RECV for reads and SEND for writes through one io_uring ring.
/// Returns the cumulative histogram and (when `chart` feature is enabled)
/// a time-series of interval latency percentiles for visualization.
#[cfg(not(feature = "dpdk"))]
fn run_uring_loop(
    mut connections: Vec<UringBenchConn>,
    window: usize,
    bench_start: Instant,
    deadlines: PhaseDeadlines,
    progress: Arc<AtomicU64>,
) -> (Histogram<u64>, TimeSeries, Option<Instant>) {
    use io_uring::{IoUring, opcode, types};

    let ticks_per_ns = calibrate_tsc();
    // 4096 entries: supports up to 1024 connections per thread (RECV +
    // SEND per connection, plus headroom for partial-send resubmissions).
    let mut ring = IoUring::new(4096).expect("create io_uring for bench");
    let mut histogram =
        Histogram::<u64>::new_with_bounds(1, 10_000_000_000, 3).expect("histogram bounds");
    // Timestamp of the first measured (post-warmup) latency recording.
    // Used to compute throughput over the measured phase only.
    let mut measured_start: Option<Instant> = None;

    let mut interval_hist =
        Histogram::<u64>::new_with_bounds(1, 10_000_000_000, 3).expect("interval histogram");
    let mut interval_count: usize = 0;
    let mut series: TimeSeries = Vec::new();

    // Pre-allocated CQE collection buffer. Must collect CQEs before
    // processing because the CQ borrow must end before mutating connections.
    // Avoids per-iteration heap allocation from `.collect()`.
    let mut cqes: Vec<(u64, i32)> = Vec::with_capacity(1024);

    // Submit initial RECVs for all connections.
    for (i, conn) in connections.iter_mut().enumerate() {
        let sqe = opcode::Recv::new(
            types::Fd(conn.read_fd),
            conn.recv_buf.as_mut_ptr(),
            URING_RECV_BUF_SIZE as u32,
        )
        .build()
        .user_data(i as u64);
        unsafe {
            ring.submission().push(&sqe).expect("SQ full");
        }
        conn.recv_pending = true;
    }

    // Fill initial send windows.
    uring_fill_windows(&mut ring, &mut connections, window, &deadlines);

    loop {
        // Wall-clock-driven termination. The histogram is sealed at
        // `measured_end`, so any inflight responses left after we break
        // would only land in cooldown and be discarded anyway.
        if Instant::now() >= deadlines.cooldown_end {
            break;
        }
        match ring.submit_and_wait(1) {
            Ok(_) => {}
            Err(ref e) if e.raw_os_error() == Some(libc::EINTR) => continue,
            Err(e) => panic!("io_uring submit_and_wait: {e}"),
        }

        // Sample the wall clock *after* the blocking wait and reuse it
        // for the phase classifier on every CQE in this batch. Saves a
        // vDSO call per response — at multi-M ops/s the per-CQE
        // `Instant::now()` (~15-25 ns) was visible in profiles. Outer
        // iters batch many CQEs and phase boundaries are coarse (5 s
        // warmup, 60 s measured), so reusing one timestamp across a
        // batch misclassifies at most a handful of samples around the
        // warmup/measured boundary — far below run-to-run noise.
        let now = Instant::now();

        cqes.clear();
        cqes.extend(ring.completion().map(|cqe| (cqe.user_data(), cqe.result())));

        for &(token, result) in cqes.iter() {
            if token & SEND_FLAG != 0 {
                // ── SEND completion ──
                let idx = (token & !SEND_FLAG) as usize;
                let conn = &mut connections[idx];
                conn.send_pending = false;

                assert!(result >= 0, "send error: {result}");
                let sent = result as usize;
                if sent >= conn.send_buf.len() {
                    conn.send_buf.clear();
                } else {
                    // Partial send — drain and resubmit.
                    conn.send_buf.drain(..sent);
                    let sqe = opcode::Send::new(
                        types::Fd(conn.write_fd),
                        conn.send_buf.as_ptr(),
                        conn.send_buf.len() as u32,
                    )
                    .build()
                    .user_data(idx as u64 | SEND_FLAG);
                    unsafe {
                        ring.submission().push(&sqe).expect("SQ full");
                    }
                    conn.send_pending = true;
                }
            } else {
                // ── RECV completion ──
                let idx = token as usize;
                assert!(result > 0, "recv error or disconnect: {result}");

                let n_bytes = result as usize;
                let conn = &mut connections[idx];
                conn.recv_pending = false;
                conn.parse_buf.extend_from_slice(&conn.recv_buf[..n_bytes]);

                // Parse complete frames.
                let mut cursor = 0;
                while cursor + 4 <= conn.parse_buf.len() {
                    let len_bytes: [u8; 4] = conn.parse_buf[cursor..cursor + 4]
                        .try_into()
                        .expect("4 bytes");
                    let frame_len = u32::from_le_bytes(len_bytes) as usize;
                    if cursor + 4 + frame_len > conn.parse_buf.len() {
                        break;
                    }

                    let frame = &conn.parse_buf[cursor + 4..cursor + 4 + frame_len];
                    let response = codec::decode_response(frame).expect("decode response");
                    cursor += 4 + frame_len;

                    if matches!(response, ResponseKind::BatchEnd) {
                        let sent_tsc = conn.inflight_ts.pop_front().expect(
                            "inflight timestamp desync: got BatchEnd without matching send",
                        );
                        let latency_ns = tsc_to_ns(rdtscp() - sent_tsc, ticks_per_ns);
                        // Phase classification by *receive* time, using
                        // the outer-iter `now`. Once `measured_end`
                        // passes the histogram is sealed; any further
                        // completions fall through silently.
                        if now >= deadlines.warmup_end && now < deadlines.measured_end {
                            if measured_start.is_none() {
                                measured_start = Some(now);
                            }
                            histogram.record(latency_ns).expect("record");
                            interval_hist.record(latency_ns).expect("record interval");
                            interval_count += 1;
                            maybe_sample(
                                &mut interval_hist,
                                &mut interval_count,
                                &mut series,
                                bench_start,
                            );
                            progress.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                if cursor > 0 {
                    // Shift remaining bytes to front without allocating.
                    // `copy_within` + `truncate` avoids the O(n) memmove
                    // overhead of `Vec::drain` which must drop + shift.
                    let remaining = conn.parse_buf.len() - cursor;
                    conn.parse_buf.copy_within(cursor.., 0);
                    conn.parse_buf.truncate(remaining);
                }

                // Re-arm RECV. The outer loop's wall-clock check is the
                // only exit; pending CQEs after cooldown are drained
                // implicitly when the io_uring drops at function exit.
                let sqe = opcode::Recv::new(
                    types::Fd(conn.read_fd),
                    conn.recv_buf.as_mut_ptr(),
                    URING_RECV_BUF_SIZE as u32,
                )
                .build()
                .user_data(idx as u64);
                unsafe {
                    ring.submission().push(&sqe).expect("SQ full");
                }
                conn.recv_pending = true;
            }
        }

        // Refill send windows for connections with capacity.
        uring_fill_windows(&mut ring, &mut connections, window, &deadlines);
    }

    (histogram, series, measured_start)
}

/// Fill send windows for all connections that have capacity and no pending send.
/// Builds a length-prefixed send buffer and submits SEND SQEs. Stops issuing
/// new frames once the cooldown deadline has passed — the loop above will
/// then terminate as soon as `submit_and_wait` returns (or immediately if
/// the queue is empty).
#[cfg(not(feature = "dpdk"))]
fn uring_fill_windows(
    ring: &mut io_uring::IoUring,
    connections: &mut [UringBenchConn],
    window: usize,
    deadlines: &PhaseDeadlines,
) {
    use io_uring::{opcode, types};

    // Past cooldown: do nothing. We want the loop to wind down, not to
    // queue more sends that will arrive after the run is reported.
    if Instant::now() >= deadlines.cooldown_end {
        return;
    }

    for (i, conn) in connections.iter_mut().enumerate() {
        if conn.send_pending {
            continue;
        }

        // Fill the send buffer with as many frames as the window allows.
        // Each frame is encoded directly into `send_buf` as `[u32 LE len][payload]`.
        while conn.inflight_ts.len() < window {
            conn.flow.next_wire_frame(&mut conn.send_buf);
            conn.inflight_ts.push_back(rdtscp());
        }

        if !conn.send_buf.is_empty() {
            let sqe = opcode::Send::new(
                types::Fd(conn.write_fd),
                conn.send_buf.as_ptr(),
                conn.send_buf.len() as u32,
            )
            .build()
            .user_data(i as u64 | SEND_FLAG);
            unsafe {
                ring.submission().push(&sqe).expect("SQ full");
            }
            conn.send_pending = true;
        }
    }
}

// ===========================================================================
// Shared reporting
// ===========================================================================

/// Print a latency histogram in µs. Adaptive nines: only prints p99.9, p99.99,
/// etc. when `sample_count` is large enough (10×  per extra nine).
pub(crate) fn print_latency_histogram(hist: &Histogram<u64>, sample_count: usize) {
    println!("    min:     {:>8.2} µs", hist.min() as f64 / 1_000.0);
    println!(
        "    p50:     {:>8.2} µs",
        hist.value_at_quantile(0.50) as f64 / 1_000.0
    );
    println!(
        "    p90:     {:>8.2} µs",
        hist.value_at_quantile(0.90) as f64 / 1_000.0
    );
    let mut nines = 2;
    let mut threshold = 1_000usize;
    while threshold <= sample_count {
        let quantile = 1.0 - 10.0f64.powi(-(nines as i32));
        let label = if nines <= 2 {
            "p99".to_string()
        } else {
            format!("p99.{}", "9".repeat(nines - 2))
        };
        let value = hist.value_at_quantile(quantile) as f64 / 1_000.0;
        let padded = format!("{label}:");
        println!("    {padded:<9}{value:>8.2} µs");
        nines += 1;
        threshold *= 10;
    }
    println!("    max:     {:>8.2} µs", hist.max() as f64 / 1_000.0);
}

/// Print benchmark results: header, throughput, latency histogram.
/// Optionally writes results to a JSON file for post-processing.
#[allow(clippy::too_many_arguments)]
pub(crate) fn print_results(
    label: &str,
    measured_orders: usize,
    phases: BenchPhases,
    histogram: &Histogram<u64>,
    wall: Duration,
    extra_lines: &[String],
    json_path: Option<&std::path::Path>,
    series: &[LatencySample],
    health_samples: &[health_poller::HealthSample],
    server_stages: &stats_client::Body,
) {
    let throughput = (measured_orders as f64) / wall.as_secs_f64();
    let wall_ms = wall.as_micros() as f64 / 1000.0;

    println!(
        "=== {label} Benchmark ({measured_orders} measured, warmup={} measured={} cooldown={}) ===",
        humantime::format_duration(phases.warmup),
        humantime::format_duration(phases.measured),
        humantime::format_duration(phases.cooldown),
    );
    for line in extra_lines {
        println!("{line}");
    }
    println!();
    println!("  Throughput");
    println!("    wall time:  {wall_ms:.2} ms");
    println!(
        "    throughput: {throughput:.0} orders/sec ({:.2} µs/order)",
        1_000_000.0 / throughput
    );
    println!();
    println!("  Per-Order Latency");
    print_latency_histogram(histogram, measured_orders);

    // Print health summary if we have samples.
    if !health_samples.is_empty() {
        let duration = health_samples.last().map_or(0.0, |s| s.elapsed_secs)
            - health_samples.first().map_or(0.0, |s| s.elapsed_secs);
        let peak_depth = health_samples
            .iter()
            .map(|s| s.input_queue_depth)
            .max()
            .unwrap_or(0);
        let capacity = health_samples
            .iter()
            .map(|s| s.input_queue_capacity)
            .max()
            .unwrap_or(0);
        let final_events = health_samples.last().map_or(0, |s| s.events_processed);
        println!();
        println!(
            "  Health ({} samples over {duration:.1}s)",
            health_samples.len()
        );
        if capacity > 0 {
            let pct = peak_depth as f64 / capacity as f64 * 100.0;
            println!("    peak queue depth: {peak_depth} / {capacity} ({pct:.1}%)");
        } else {
            println!("    peak queue depth: {peak_depth}");
        }
        println!("    events processed: {final_events}");
    }

    // Server-side per-stage decomposition (tick-to-trade). Fetched
    // from the server's /stats-dump endpoint at end of run; only
    // populated for the roundtrip mode against a server built with
    // --features latency-trace.
    stats_client::render_console(server_stages);

    // Write JSON results if requested.
    if let Some(path) = json_path {
        use std::io::Write;

        let throughput = (measured_orders as f64) / wall.as_secs_f64();
        let mut percentiles = String::from("{");
        percentiles.push_str(&format!(
            "\"min_us\":{:.2},\"p50_us\":{:.2},\"p90_us\":{:.2}",
            histogram.min() as f64 / 1000.0,
            histogram.value_at_quantile(0.50) as f64 / 1000.0,
            histogram.value_at_quantile(0.90) as f64 / 1000.0,
        ));
        let mut n = 2;
        let mut t = 1_000usize;
        while t <= measured_orders {
            let q = 1.0 - 10.0f64.powi(-(n as i32));
            let label = if n <= 2 {
                "p99_us".to_string()
            } else {
                format!("p99{}_us", ".9".repeat(n - 2))
            };
            percentiles.push_str(&format!(
                ",\"{}\":{:.2}",
                label,
                histogram.value_at_quantile(q) as f64 / 1000.0
            ));
            n += 1;
            t *= 10;
        }
        percentiles.push_str(&format!(
            ",\"max_us\":{:.2}}}",
            histogram.max() as f64 / 1000.0
        ));

        // Serialize time-series data for stability plots.
        let ts_json = if series.is_empty() {
            String::from("[]")
        } else {
            let entries: Vec<String> = series
                .iter()
                .map(|s| {
                    format!(
                        "{{\"elapsed_secs\":{:.3},\"p99_us\":{:.2},\"p999_us\":{:.2},\"p9999_us\":{:.2}}}",
                        s.elapsed_secs, s.p99_us, s.p999_us, s.p9999_us,
                    )
                })
                .collect();
            format!("[{}]", entries.join(","))
        };

        // Serialize health samples (fixed fields + any extra metrics).
        let health_json = if health_samples.is_empty() {
            String::from("[]")
        } else {
            let entries: Vec<String> = health_samples
                .iter()
                .map(|s| {
                    let mut json = format!(
                        "{{\"elapsed_secs\":{:.3},\"active_connections\":{},\"events_processed\":{},\"journal_sequence\":{},\"replication_lag\":{},\"input_queue_depth\":{},\"input_queue_capacity\":{},\"pipeline_healthy\":{},\"trading_active\":{}",
                        s.elapsed_secs,
                        s.active_connections,
                        s.events_processed,
                        s.journal_sequence,
                        s.replication_lag,
                        s.input_queue_depth,
                        s.input_queue_capacity,
                        s.pipeline_healthy,
                        s.trading_active,
                    );
                    // Append extra metrics (per-replica replication stats, etc.).
                    // Sorted for deterministic output. Prometheus label syntax
                    // like `metric{slot="0"}` is sanitized to `metric_slot_0`
                    // for valid JSON keys.
                    let mut keys: Vec<&String> = s.extra.keys().collect();
                    keys.sort();
                    for key in keys {
                        let val = s.extra[key];
                        // Sanitize Prometheus label syntax for JSON keys:
                        // melin_replica_lag{slot="0"} → melin_replica_lag_slot_0
                        let safe_key: String = key
                            .chars()
                            .filter_map(|c| match c {
                                '{' | '=' => Some('_'),
                                '}' | '"' => None,
                                other => Some(other),
                            })
                            .collect();
                        // Emit integers without decimal point for cleaner JSON.
                        if val == val.trunc() && val.abs() < u64::MAX as f64 {
                            json.push_str(&format!(",\"{safe_key}\":{}", val as i64));
                        } else {
                            json.push_str(&format!(",\"{safe_key}\":{val:.3}"));
                        }
                    }
                    json.push('}');
                    json
                })
                .collect();
            format!("[{}]", entries.join(","))
        };

        let stages_json = stats_client::render_json(server_stages);

        let json = format!(
            "{{\"label\":\"{label}\",\"measured_orders\":{measured_orders},\"warmup_ms\":{:.2},\"measured_ms\":{:.2},\"cooldown_ms\":{:.2},\"wall_ms\":{:.2},\"throughput_ops\":{:.0},\"latency\":{percentiles},\"time_series\":{ts_json},\"health\":{health_json},\"server_stages\":{stages_json}}}",
            phases.warmup.as_secs_f64() * 1000.0,
            phases.measured.as_secs_f64() * 1000.0,
            phases.cooldown.as_secs_f64() * 1000.0,
            wall.as_secs_f64() * 1000.0,
            throughput,
        );

        let mut file = std::fs::File::create(path).expect("create json file");
        file.write_all(json.as_bytes()).expect("write json");
        file.write_all(b"\n").expect("write newline");
        eprintln!("Results written to {}", path.display());
    }
}

/// Create a temporary directory that persists for the process lifetime.
fn tempdir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("melin-bench-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

#[cfg(test)]
#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
mod tsc_clock_tests {
    use super::*;

    /// A freshly calibrated `TscClock`, queried immediately, must agree
    /// with `melin_app::unix_epoch_nanos()` to within a millisecond.
    /// That window catches both flipped-sign anchor regressions
    /// (derived value diverges by anchor_unix_ns) and a units mix-up in
    /// `ns_per_tick` (the elapsed delta is small immediately after
    /// calibration, so any factor error would still surface as a few-µs
    /// drift before the kernel clock advances by the same amount).
    #[test]
    fn freshly_calibrated_clock_matches_wall_clock_within_1ms() {
        let clock = calibrate_tsc_clock();
        let derived = clock.unix_ns(rdtscp());
        let now_unix = melin_app::unix_epoch_nanos();
        let diff = derived.abs_diff(now_unix);
        assert!(
            diff < 1_000_000,
            "derived {derived} vs wall {now_unix}, |Δ| = {diff} ns"
        );
    }

    /// `unix_ns` must not underflow when the supplied TSC reading is
    /// older than the anchor (which can happen if a thread migrated to
    /// a core with an out-of-sync TSC, or simply if a TSC reading
    /// captured pre-calibration is fed in by mistake).
    #[test]
    fn unix_ns_saturates_on_pre_anchor_tsc() {
        let clock = calibrate_tsc_clock();
        let value = clock.unix_ns(clock.anchor_tsc.saturating_sub(1_000));
        assert_eq!(value, clock.anchor_unix_ns);
    }
}
