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
//! All modes use the realistic order flow generator: a mix of limit orders and
//! cancels with power-law price/size distributions, multiple accounts, and
//! resting book depth. Events are pre-generated before the measured run.
//!
//! Usage:
//!     cargo run --release -p trading-bench [-- [--mode=roundtrip|pipeline|engine] [--uds] [--addr=<ip:port>] [--clients=N] [--window=N] [--group-commit-us=N] [--bench-threads=N] <order_pairs>]
//!
//! Default: roundtrip mode, TCP transport, 1 client, 1,000,000 order pairs.

mod generator;

/// jemalloc: thread-local caches eliminate allocator lock contention,
/// giving more predictable latency than glibc malloc under high throughput.
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::collections::VecDeque;
#[cfg(not(feature = "io-uring"))]
use std::io;
use std::io::Write;
use std::num::NonZeroU64;
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;

use trading_engine::types::*;
#[cfg(not(feature = "io-uring"))]
use trading_protocol::blocking::BlockingFrameWriter;
use trading_protocol::codec;
use trading_protocol::message::ResponseKind;
use trading_protocol::transport::BlockingTransportListener;
use trading_server::server::ServerConfig;

/// Number of completed orders between latency time-series samples.
/// Each sample captures interval p99/p99.9 (reset after each sample),
/// giving temporal variation rather than cumulative smoothing.
#[cfg(feature = "chart")]
const SAMPLE_INTERVAL: usize = 1_000;

/// Number of order pairs (buy + sell) per benchmark run.
const DEFAULT_PAIRS: usize = 1_000_000;

/// Default warmup orders (not measured) per client to prime the pipeline and caches.
const WARMUP_ORDERS: usize = 100_000;

/// Default number of orders in flight simultaneously per client. Controls the
/// level of pipelining — enough to keep the server pipeline saturated (journal +
/// matching stages overlap), small enough that per-order latency reflects
/// actual processing time rather than unbounded queueing.
const DEFAULT_WINDOW: usize = 64;

/// Default number of concurrent client connections.
const DEFAULT_CLIENTS: usize = 16;

/// Default number of bench client threads. Each thread manages a subset of
/// connections via epoll. Pinned to cores 6-9 (2 physical + 2 HT siblings
/// on 8C/16T). With 4 bench + 5 server (3 pipeline + 2 reader) = 9 pinned
/// threads total, leaving core 0 for OS/IRQ and 6 cores free.
const DEFAULT_BENCH_THREADS: usize = 4;

/// First CPU core for bench thread pinning. Server uses cores 1-3 (pipeline)
/// and 4-5 (readers), so bench threads start at core 6 to avoid contention
/// for L1/L2 cache and reduce involuntary context switches. Thread i is
/// pinned to core `BENCH_CORE_START + i`.
const BENCH_CORE_START: usize = 6;

/// Maximum frame payload size (matches protocol).
const MAX_FRAME_SIZE: usize = 1024;

/// Maximum epoll events per wait call.
#[cfg(not(feature = "io-uring"))]
const MAX_EPOLL_EVENTS: usize = 64;

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
fn rdtscp() -> u64 {
    unsafe {
        let mut _aux: u32 = 0;
        core::arch::x86_64::__rdtscp(&mut _aux)
    }
}

/// Calibrate TSC ticks per nanosecond by measuring a short sleep against
/// `Instant::now()`. Returns the conversion factor (ticks / ns).
#[cfg(target_arch = "x86_64")]
fn calibrate_tsc() -> f64 {
    // Warm up the TSC path.
    for _ in 0..100 {
        let _ = rdtscp();
    }

    let duration = Duration::from_millis(10);
    let t0_tsc = rdtscp();
    let t0_wall = Instant::now();
    std::thread::sleep(duration);
    let t1_tsc = rdtscp();
    let elapsed_ns = t0_wall.elapsed().as_nanos() as f64;
    let elapsed_tsc = (t1_tsc - t0_tsc) as f64;
    elapsed_tsc / elapsed_ns
}

/// Convert TSC tick delta to nanoseconds using a pre-calibrated factor.
#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn tsc_to_ns(ticks: u64, ticks_per_ns: f64) -> u64 {
    (ticks as f64 / ticks_per_ns) as u64
}

/// One latency time-series sample: interval percentiles at a point in time.
/// Captured every `SAMPLE_INTERVAL` completed orders using an interval
/// histogram (snapshot + reset), so each sample reflects recent behavior
/// rather than cumulative averages.
#[cfg(feature = "chart")]
struct LatencySample {
    /// Seconds elapsed since measurement start.
    elapsed_secs: f64,
    /// Interval p99 latency in microseconds.
    p99_us: f64,
    /// Interval p99.9 latency in microseconds.
    p999_us: f64,
    /// Interval p99.99 latency in microseconds.
    p9999_us: f64,
}

/// Time-series of latency samples for chart display.
/// Empty Vec when chart feature is disabled (no heap allocation).
#[cfg(feature = "chart")]
type TimeSeries = Vec<LatencySample>;
#[cfg(all(not(feature = "chart"), feature = "io-uring"))]
type TimeSeries = Vec<()>;

/// Record a latency sample if `SAMPLE_INTERVAL` orders have accumulated
/// in the interval histogram. Resets the interval histogram after sampling.
#[cfg(feature = "chart")]
fn maybe_sample(
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
#[command(name = "trading-bench", about = "Matching engine benchmark suite")]
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
    /// Number of order pairs (buy + sell) to benchmark.
    #[arg(default_value_t = DEFAULT_PAIRS)]
    pairs: usize,
    /// Orders in flight per client (pipelining depth).
    #[arg(long, default_value_t = DEFAULT_WINDOW)]
    window: usize,
    /// Number of concurrent client connections.
    #[arg(long, default_value_t = DEFAULT_CLIENTS)]
    clients: usize,
    /// Number of bench client threads (ignored with io-uring).
    #[arg(long, default_value_t = DEFAULT_BENCH_THREADS)]
    bench_threads: usize,
    /// Group commit coalescing delay in microseconds.
    #[arg(long, default_value_t = 0)]
    group_commit_us: u64,
    /// Warmup orders per client (not measured). Higher values let caches,
    /// branch predictors, and allocator settle before measurement starts.
    #[arg(long, default_value_t = WARMUP_ORDERS)]
    warmup: usize,
    /// Path for the journal file. Defaults to a temporary directory.
    /// Use this to place the journal on a dedicated disk for benchmarking.
    #[arg(long)]
    journal: Option<std::path::PathBuf>,
    /// Number of trading accounts.
    #[arg(long, default_value_t = 1000)]
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
}

fn main() {
    // Initialize tracing so pipeline-stats and latency-trace output is visible.
    tracing_subscriber::fmt()
        .with_target(true)
        .with_thread_names(true)
        .init();

    let args = <BenchArgs as clap::Parser>::parse();
    let json_path = args.json.as_deref();

    match args.mode.as_str() {
        "engine" => {
            run_engine_bench(
                args.pairs,
                args.warmup,
                args.accounts,
                args.instruments,
                json_path,
            );
        }
        "pipeline" => {
            run_pipeline_bench(
                args.pairs,
                args.window,
                args.group_commit_us,
                args.warmup,
                args.journal,
                json_path,
            );
        }
        "roundtrip" => {
            run_roundtrip_bench(
                args.uds,
                args.pairs,
                args.window,
                args.clients,
                args.bench_threads,
                args.group_commit_us,
                args.addr,
                args.warmup,
                args.journal,
                args.accounts,
                args.instruments,
                json_path,
                args.key.as_deref(),
            );
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
/// All events are pre-generated before the measured run so RNG overhead doesn't
/// pollute per-order timing.
fn run_engine_bench(
    total_pairs: usize,
    warmup: usize,
    num_accounts: u32,
    num_instruments: u32,
    json_path: Option<&std::path::Path>,
) {
    use generator::{GeneratedEvent, GeneratorConfig, OrderFlowGenerator};

    #[cfg(target_arch = "x86_64")]
    let ticks_per_ns = calibrate_tsc();
    #[cfg(target_arch = "x86_64")]
    eprintln!(
        "TSC calibration: {:.3} GHz ({:.2} ticks/ns)",
        ticks_per_ns, ticks_per_ns
    );

    let config = GeneratorConfig {
        num_accounts,
        num_instruments,
        ..Default::default()
    };

    let mut exchange = trading_engine::exchange::Exchange::with_capacity();

    // Register instruments.
    for i in 1..=num_instruments {
        exchange.add_instrument(InstrumentSpec {
            symbol: Symbol(i),
            base: CurrencyId(i * 2 - 1),
            quote: CurrencyId(i * 2),
        });
    }

    // Deposit generous balances for all accounts across all currencies.
    for acct in 1..=num_accounts {
        for i in 1..=num_instruments {
            exchange.deposit(AccountId(acct), CurrencyId(i * 2 - 1), u64::MAX / 4);
            exchange.deposit(AccountId(acct), CurrencyId(i * 2), u64::MAX / 4);
        }
    }

    exchange.prefault();

    let total_events = warmup + total_pairs * 2;

    // Pre-generate all events so RNG overhead doesn't pollute timing.
    eprintln!("Pre-generating {total_events} events...");
    let mut flow = OrderFlowGenerator::new(config);
    let events = flow.generate_events(total_events);
    eprintln!("Pre-generation complete.");

    let mut reports = Vec::with_capacity(256);
    let mut histogram =
        Histogram::<u64>::new_with_bounds(1, 10_000_000_000, 3).expect("histogram bounds");

    // Warmup.
    for event in &events[..warmup] {
        reports.clear();
        match *event {
            GeneratedEvent::Submit { symbol, order } => {
                exchange.execute(symbol, order, &mut reports);
            }
            GeneratedEvent::Cancel { symbol, order_id } => {
                exchange.cancel(symbol, order_id, &mut reports);
            }
        }
    }

    // Measured run.
    #[cfg(feature = "chart")]
    let mut interval_hist =
        Histogram::<u64>::new_with_bounds(1, 10_000_000_000, 3).expect("interval histogram");
    #[cfg(feature = "chart")]
    let mut interval_count: usize = 0;
    #[cfg(feature = "chart")]
    let mut series: TimeSeries = Vec::new();

    let mut submits: u64 = 0;
    let mut cancels: u64 = 0;

    let start = Instant::now();
    for event in &events[warmup..] {
        reports.clear();

        #[cfg(target_arch = "x86_64")]
        let t0 = rdtscp();
        #[cfg(not(target_arch = "x86_64"))]
        let t0 = Instant::now();

        match *event {
            GeneratedEvent::Submit { symbol, order } => {
                exchange.execute(symbol, order, &mut reports);
                submits += 1;
            }
            GeneratedEvent::Cancel { symbol, order_id } => {
                exchange.cancel(symbol, order_id, &mut reports);
                cancels += 1;
            }
        }

        #[cfg(target_arch = "x86_64")]
        let elapsed_ns = tsc_to_ns(rdtscp() - t0, ticks_per_ns);
        #[cfg(not(target_arch = "x86_64"))]
        let elapsed_ns = t0.elapsed().as_nanos() as u64;

        histogram.record(elapsed_ns).expect("record");
        #[cfg(feature = "chart")]
        {
            interval_hist.record(elapsed_ns).expect("record interval");
            interval_count += 1;
            maybe_sample(&mut interval_hist, &mut interval_count, &mut series, start);
        }
    }
    let wall = start.elapsed();

    let measured = events.len() - warmup;
    let cancel_pct = if submits + cancels > 0 {
        cancels as f64 / (submits + cancels) as f64 * 100.0
    } else {
        0.0
    };

    print_results(
        "Realistic Order Flow",
        measured,
        warmup,
        &histogram,
        wall,
        &[
            format!("  Accounts: {num_accounts}, Instruments: {num_instruments}"),
            format!("  Submits: {submits}, Cancels: {cancels} ({cancel_pct:.1}% cancel)"),
        ],
        json_path,
    );
    #[cfg(feature = "chart")]
    show_chart(&series, &histogram);
}

// ===========================================================================
// Pipeline benchmark (disruptor + journal + matching, no network)
// ===========================================================================

/// Pipeline benchmark. Builds the full disruptor pipeline (journal stage +
/// matching stage) but bypasses TCP/UDS transport. The bench thread publishes
/// InputSlots directly to the MultiProducer and drains OutputSlots from the
/// SPSC consumer. Measures pipeline latency without network overhead.
fn run_pipeline_bench(
    total_pairs: usize,
    window: usize,
    group_commit_us: u64,
    warmup: usize,
    journal_path: Option<std::path::PathBuf>,
    json_path: Option<&std::path::Path>,
) {
    use trading_engine::journal::JournalWriter;
    use trading_engine::journal::event::JournalEvent;
    use trading_engine::journal::pipeline::{InputSlot, build_pipeline};
    use trading_engine::journal::trace::trace_ts;

    let nz = |v: u64| NonZeroU64::new(v).expect("non-zero");

    // Set up exchange with one instrument and funded account.
    let mut exchange = trading_engine::exchange::Exchange::with_capacity();
    exchange.add_instrument(InstrumentSpec {
        symbol: Symbol(1),
        base: CurrencyId(1),
        quote: CurrencyId(2),
    });
    exchange.deposit(AccountId(1), CurrencyId(1), u64::MAX / 2);
    exchange.deposit(AccountId(1), CurrencyId(2), u64::MAX / 2);
    exchange.prefault();

    let tmp_dir = tempdir();
    let effective_journal = journal_path.unwrap_or_else(|| tmp_dir.join("pipeline-bench.journal"));
    let writer = JournalWriter::create(&effective_journal).expect("create journal");

    let group_commit_delay = Duration::from_micros(group_commit_us);
    let active_conns = Arc::new(AtomicU64::new(0));
    let (
        producer,
        journal_stage,
        matching_stage,
        mut output_consumer,
        _journal_cursor,
        _events_processed,
    ) = build_pipeline(exchange, writer, group_commit_delay, active_conns);

    let shutdown = Arc::new(AtomicBool::new(false));

    // Spawn journal and matching stage threads.
    let shutdown_j = Arc::clone(&shutdown);
    let journal_handle = std::thread::Builder::new()
        .name("journal".into())
        .spawn(move || journal_stage.run(&shutdown_j))
        .expect("spawn journal thread");

    let shutdown_m = Arc::clone(&shutdown);
    let matching_handle = std::thread::Builder::new()
        .name("matching".into())
        .spawn(move || matching_stage.run(&shutdown_m))
        .expect("spawn matching thread");

    let total_orders = warmup + total_pairs * 2;
    let mut histogram =
        Histogram::<u64>::new_with_bounds(1, 10_000_000_000, 3).expect("histogram bounds");

    // Track in-flight timestamps for windowed pipelining.
    // VecDeque for FIFO: push_back on publish, pop_front on BatchEnd.
    let mut inflight_ts: VecDeque<Instant> = VecDeque::with_capacity(window);
    let mut completed = 0usize;

    let start = Instant::now();

    for i in 0..total_orders {
        let order_id = OrderId((i as u64) + 1);
        let side = if i % 2 == 0 { Side::Buy } else { Side::Sell };

        // Wait for window capacity before publishing.
        while inflight_ts.len() >= window {
            drain_output(
                &mut output_consumer,
                &mut inflight_ts,
                &mut histogram,
                &mut completed,
                warmup,
            );
        }

        let ts = Instant::now();
        producer.publish(InputSlot {
            connection_id: 0,
            event: JournalEvent::SubmitOrder {
                symbol: Symbol(1),
                order: Order {
                    id: order_id,
                    account: AccountId(1),
                    side,
                    order_type: OrderType::Limit {
                        price: Price(nz(100)),
                    },
                    time_in_force: TimeInForce::GTC,
                    quantity: Quantity(nz(1)),
                    stp: SelfTradeProtection::Allow,
                },
            },
            publish_ts: trace_ts(),
            recv_ts: trace_ts(),
        });
        inflight_ts.push_back(ts);
    }

    // Drain remaining responses.
    while completed < total_orders {
        drain_output(
            &mut output_consumer,
            &mut inflight_ts,
            &mut histogram,
            &mut completed,
            warmup,
        );
    }

    let wall = start.elapsed();

    // Shutdown pipeline threads.
    shutdown.store(true, Ordering::Relaxed);

    let mut extra_lines = Vec::new();
    if group_commit_us > 0 {
        extra_lines.push(format!("  Group commit delay: {group_commit_us} µs"));
    }
    extra_lines.push(format!("  Window: {window}"));

    print_results(
        "Pipeline (no network)",
        total_pairs * 2,
        warmup,
        &histogram,
        wall,
        &extra_lines,
        json_path,
    );

    println!();
    println!("=== Pipeline Latency Trace ===");
    println!();

    // Wait for pipeline threads to finish and print trace reports.
    let _ = journal_handle.join();
    let _ = matching_handle.join();

    let _ = std::fs::remove_dir_all(&tmp_dir);
}

/// Drain available OutputSlots from the SPSC consumer, recording latency
/// for each BatchEnd response.
fn drain_output(
    consumer: &mut trading_disruptor::spsc::Consumer<trading_engine::journal::pipeline::OutputSlot>,
    inflight_ts: &mut VecDeque<Instant>,
    histogram: &mut Histogram<u64>,
    completed: &mut usize,
    warmup: usize,
) {
    use trading_engine::journal::pipeline::OutputPayload;

    loop {
        let Some((_seq, slot)) = consumer.try_consume() else {
            std::hint::spin_loop();
            return;
        };
        if matches!(slot.payload, OutputPayload::BatchEnd) {
            let sent_at = inflight_ts.pop_front().expect("inflight timestamp");
            let latency_ns = sent_at.elapsed().as_nanos() as u64;
            if *completed >= warmup {
                histogram.record(latency_ns).expect("record");
            }
            *completed += 1;
        }
    }
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
fn run_roundtrip_bench(
    use_uds: bool,
    pairs: usize,
    window: usize,
    num_clients: usize,
    bench_threads: usize,
    group_commit_us: u64,
    remote_addr: Option<std::net::SocketAddr>,
    warmup: usize,
    journal_path: Option<std::path::PathBuf>,
    num_accounts: u32,
    num_instruments: u32,
    json_path: Option<&std::path::Path>,
    key_path: Option<&std::path::Path>,
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
            pairs,
            window,
            num_clients,
            bench_threads,
            group_commit_us,
            shutdown,
            warmup,
            json_path,
            &key,
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
        ..ServerConfig::default()
    };

    let shutdown = Arc::new(AtomicBool::new(false));

    if use_uds {
        use trading_protocol::uds::BlockingUdsListener;

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
            pairs,
            window,
            num_clients,
            bench_threads,
            group_commit_us,
            shutdown,
            warmup,
            json_path,
            &bench_key,
        );
    } else {
        use trading_protocol::tcp::BlockingTcpListener;

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
            pairs,
            window,
            num_clients,
            bench_threads,
            group_commit_us,
            shutdown,
            warmup,
            json_path,
            &bench_key,
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
fn start_server<L: BlockingTransportListener>(
    listener: L,
    config: ServerConfig,
    shutdown: Arc<AtomicBool>,
) {
    std::thread::Builder::new()
        .name("server".into())
        .spawn(move || {
            if let Err(e) = trading_server::server::run_with_shutdown(listener, config, shutdown) {
                eprintln!("server error: {e}");
            }
        })
        .expect("spawn server thread");
}

/// Connect to TCP server with retry (up to 50 attempts, 10ms apart).
fn connect_tcp(addr: std::net::SocketAddr) -> std::net::TcpStream {
    let mut last_err = None;
    for _ in 0..50 {
        match std::net::TcpStream::connect(addr) {
            Ok(s) => return s,
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
fn auth_handshake(
    stream: &mut (impl std::io::Read + std::io::Write),
    key: &ed25519_dalek::SigningKey,
) {
    use ed25519_dalek::Signer;
    use trading_protocol::message::Request;

    // Read Challenge frame.
    let mut len_buf = [0u8; 4];
    std::io::Read::read_exact(stream, &mut len_buf).expect("read Challenge length");
    let len = u32::from_le_bytes(len_buf) as usize;
    assert!(len <= MAX_FRAME_SIZE, "Challenge frame too large: {len}");
    let mut payload = [0u8; 64];
    std::io::Read::read_exact(stream, &mut payload[..len]).expect("read Challenge payload");
    let response = codec::decode_response(&payload[..len]).expect("decode Challenge");
    let nonce = match response {
        ResponseKind::Challenge { nonce } => nonce,
        other => panic!("expected Challenge, got {other:?}"),
    };

    // Sign and send ChallengeResponse.
    let signature = key.sign(&nonce);
    let request = Request::ChallengeResponse {
        signature: signature.to_bytes(),
        public_key: key.verifying_key().to_bytes(),
    };
    let mut buf = [0u8; 256];
    let written = codec::encode_request(&request, &mut buf).expect("encode ChallengeResponse");
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

// ---------------------------------------------------------------------------
// Per-connection state
// ---------------------------------------------------------------------------

/// State for one benchmark connection in the epoll event loop.
#[cfg(not(feature = "io-uring"))]
struct BenchConnection<W: Write> {
    // --- Write side (blocking, buffered) ---
    writer: BlockingFrameWriter<W>,

    // --- Read side (non-blocking, incremental frame parsing) ---
    /// Keeps the read-side fd alive. Dropping this closes the fd.
    _read_stream: Box<dyn AsRawFdSend>,
    fd: RawFd,
    /// 4-byte length prefix buffer.
    len_buf: [u8; 4],
    len_filled: usize,
    /// Frame payload buffer (reused across frames, avoids allocation).
    payload_buf: [u8; MAX_FRAME_SIZE],
    payload_len: usize,
    payload_filled: usize,
    reading_payload: bool,

    // --- Pipelining state ---
    /// Pre-encoded request frames for this connection.
    frames: Vec<Vec<u8>>,
    /// Next frame index to send.
    send_cursor: usize,
    /// FIFO of send timestamps for in-flight orders.
    /// Push on send, pop on BatchEnd to compute round-trip latency.
    inflight_ts: VecDeque<Instant>,
    /// Number of BatchEnd responses received (including warmup).
    batch_count: usize,
    /// Total orders this connection must process.
    total_orders: usize,
    /// True when this connection has received all responses.
    done: bool,
}

/// Trait alias for types that are both `AsRawFd` and `Send`.
/// Used to erase the concrete stream type (TCP or UDS) behind a trait object.
#[cfg(not(feature = "io-uring"))]
trait AsRawFdSend: AsRawFd + Send {}
#[cfg(not(feature = "io-uring"))]
impl<T: AsRawFd + Send> AsRawFdSend for T {}

// ---------------------------------------------------------------------------
// Non-blocking frame parsing (adapted from server reader.rs)
// ---------------------------------------------------------------------------

/// Result of attempting to read a complete frame.
#[cfg(not(feature = "io-uring"))]
enum FrameResult {
    /// A complete frame was read; valid bytes are in the connection's payload_buf.
    Complete,
    /// No more data available (EAGAIN/EWOULDBLOCK).
    WouldBlock,
    /// Peer disconnected.
    Disconnected,
    /// I/O error.
    Error,
}

/// Try to read one complete frame from a non-blocking fd.
/// On `FrameResult::Complete`, the frame payload is in
/// `conn.payload_buf[..conn.payload_len]`.
#[cfg(not(feature = "io-uring"))]
fn try_read_frame<W: Write>(conn: &mut BenchConnection<W>) -> FrameResult {
    // Step 1: Read 4-byte length prefix.
    if !conn.reading_payload {
        match nonblocking_fill(conn.fd, &mut conn.len_buf, conn.len_filled, 4) {
            FillResult::Complete(filled) => {
                conn.len_filled = filled;
                if filled < 4 {
                    return FrameResult::WouldBlock;
                }
                let len = u32::from_le_bytes(conn.len_buf) as usize;
                assert!(
                    len <= MAX_FRAME_SIZE,
                    "frame too large: {len} (max {MAX_FRAME_SIZE})"
                );
                conn.payload_len = len;
                conn.payload_filled = 0;
                conn.reading_payload = true;
                conn.len_filled = 0;
            }
            FillResult::Disconnected => return FrameResult::Disconnected,
            FillResult::Error => return FrameResult::Error,
        }
    }

    // Step 2: Read payload.
    if conn.reading_payload {
        match nonblocking_fill(
            conn.fd,
            &mut conn.payload_buf,
            conn.payload_filled,
            conn.payload_len,
        ) {
            FillResult::Complete(filled) => {
                conn.payload_filled = filled;
                if filled < conn.payload_len {
                    return FrameResult::WouldBlock;
                }
                conn.reading_payload = false;
                FrameResult::Complete
            }
            FillResult::Disconnected => FrameResult::Disconnected,
            FillResult::Error => FrameResult::Error,
        }
    } else {
        FrameResult::WouldBlock
    }
}

#[cfg(not(feature = "io-uring"))]
enum FillResult {
    /// Progressed to `filled` bytes. If `filled < target`, EAGAIN.
    Complete(usize),
    Disconnected,
    Error,
}

/// Non-blocking read into `buf[filled..target]`.
#[cfg(not(feature = "io-uring"))]
fn nonblocking_fill(fd: RawFd, buf: &mut [u8], mut filled: usize, target: usize) -> FillResult {
    while filled < target {
        let n = unsafe {
            libc::read(
                fd,
                buf[filled..target].as_mut_ptr() as *mut libc::c_void,
                target - filled,
            )
        };
        if n > 0 {
            filled += n as usize;
        } else if n == 0 {
            return FillResult::Disconnected;
        } else {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::WouldBlock {
                return FillResult::Complete(filled);
            }
            return FillResult::Error;
        }
    }
    FillResult::Complete(filled)
}

// ---------------------------------------------------------------------------
// Epoll event loop (runs on each bench thread)
// ---------------------------------------------------------------------------

/// Run the epoll event loop for a subset of connections. Returns the
/// latency histogram for this thread's connections.
#[cfg(not(feature = "io-uring"))]
fn run_epoll_loop<W: Write>(
    mut connections: Vec<BenchConnection<W>>,
    window: usize,
) -> Histogram<u64> {
    let num_conns = connections.len();

    // Create epoll instance for this thread.
    let epoll_fd = unsafe { libc::epoll_create1(0) };
    assert!(epoll_fd >= 0, "epoll_create1 failed");

    // Register all read fds with epoll (edge-triggered).
    for (i, conn) in connections.iter().enumerate() {
        let mut ev = libc::epoll_event {
            events: (libc::EPOLLIN | libc::EPOLLET) as u32,
            u64: i as u64,
        };
        let ret = unsafe { libc::epoll_ctl(epoll_fd, libc::EPOLL_CTL_ADD, conn.fd, &mut ev) };
        assert!(ret == 0, "epoll_ctl failed");
    }

    let mut histogram =
        Histogram::<u64>::new_with_bounds(1, 10_000_000_000, 3).expect("histogram bounds");
    let mut events = vec![libc::epoll_event { events: 0, u64: 0 }; MAX_EPOLL_EVENTS];
    let mut done_count: usize = 0;

    // Initial fill: send up to `window` frames per connection.
    send_pending(&mut connections, window);

    while done_count < num_conns {
        let can_send = connections
            .iter()
            .any(|c| !c.done && c.inflight_ts.len() < window && c.send_cursor < c.total_orders);
        let timeout_ms = if can_send { 0 } else { -1 };

        let nfds = unsafe {
            libc::epoll_wait(
                epoll_fd,
                events.as_mut_ptr(),
                MAX_EPOLL_EVENTS as i32,
                timeout_ms,
            )
        };

        if nfds < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            panic!("epoll_wait error: {err}");
        }

        // Process readable connections.
        for event in &events[..nfds as usize] {
            let idx = event.u64 as usize;
            let conn = &mut connections[idx];
            if conn.done {
                continue;
            }

            // Edge-triggered: drain all available data.
            loop {
                match try_read_frame(conn) {
                    FrameResult::Complete => {
                        let frame = &conn.payload_buf[..conn.payload_len];
                        let response = codec::decode_response(frame).expect("decode response");

                        if matches!(response, ResponseKind::BatchEnd) {
                            let sent_at = conn.inflight_ts.pop_front().expect(
                                "inflight timestamp desync: got BatchEnd without matching send",
                            );
                            let latency_ns = sent_at.elapsed().as_nanos() as u64;

                            if conn.batch_count >= warmup {
                                histogram.record(latency_ns).expect("record");
                            }

                            conn.batch_count += 1;
                            if conn.batch_count >= conn.total_orders {
                                conn.done = true;
                                done_count += 1;
                                break;
                            }
                        }
                    }
                    FrameResult::WouldBlock => break,
                    FrameResult::Disconnected => panic!("server disconnected unexpectedly"),
                    FrameResult::Error => panic!("read error"),
                }
            }
        }

        // Refill windows after receiving responses.
        send_pending(&mut connections, window);
    }

    unsafe {
        libc::close(epoll_fd);
    }

    histogram
}

// ---------------------------------------------------------------------------
// Orchestration
// ---------------------------------------------------------------------------

/// Create connections, distribute across bench threads, run, report results.
#[allow(clippy::too_many_arguments)]
fn run_roundtrip_inner<R, W, F>(
    connect: F,
    transport_name: &str,
    total_pairs: usize,
    window: usize,
    num_clients: usize,
    #[cfg_attr(feature = "io-uring", allow(unused_variables))] bench_threads: usize,
    group_commit_us: u64,
    shutdown: Arc<AtomicBool>,
    warmup: usize,
    json_path: Option<&std::path::Path>,
    key: &ed25519_dalek::SigningKey,
) where
    R: std::io::Read + std::io::Write + AsRawFd + Send + 'static,
    W: Write + AsRawFd + Send + 'static,
    F: Fn() -> (R, W),
{
    // io_uring path: single-threaded event loop using io_uring RECV/SEND.
    #[cfg(feature = "io-uring")]
    {
        run_uring_roundtrip(
            connect,
            transport_name,
            total_pairs,
            window,
            num_clients,
            bench_threads,
            group_commit_us,
            shutdown,
            warmup,
            json_path,
            key,
        );
    }

    // Epoll path: multi-threaded event loop using epoll reads + blocking writes.
    #[cfg(not(feature = "io-uring"))]
    {
        run_epoll_roundtrip(
            connect,
            transport_name,
            total_pairs,
            window,
            num_clients,
            bench_threads,
            group_commit_us,
            shutdown,
            warmup,
            json_path,
            key,
        );
    }
}

/// Epoll-based roundtrip benchmark. Uses epoll for reads and blocking
/// writes via BlockingFrameWriter.
#[cfg(not(feature = "io-uring"))]
fn run_epoll_roundtrip<R, W, F>(
    connect: F,
    transport_name: &str,
    total_pairs: usize,
    window: usize,
    num_clients: usize,
    bench_threads: usize,
    group_commit_us: u64,
    shutdown: Arc<AtomicBool>,
    warmup: usize,
    json_path: Option<&std::path::Path>,
    key: &ed25519_dalek::SigningKey,
) where
    R: std::io::Read + std::io::Write + AsRawFd + Send + 'static,
    W: Write + AsRawFd + Send + 'static,
    F: Fn() -> (R, W),
{
    let pairs_per_client = total_pairs / num_clients;
    let remainder = total_pairs % num_clients;

    // Create all connections upfront, then distribute round-robin to threads.
    let num_threads = bench_threads.min(num_clients);
    let mut thread_conns: Vec<Vec<BenchConnection<W>>> =
        (0..num_threads).map(|_| Vec::new()).collect();

    for client_id in 0..num_clients {
        let (mut read_stream, write_stream) = connect();

        // Challenge-response auth while the socket is still blocking.
        auth_handshake(&mut read_stream, key);

        let fd = read_stream.as_raw_fd();
        let writer = BlockingFrameWriter::new(write_stream);

        // Set non-blocking on the read fd.
        unsafe {
            let flags = libc::fcntl(fd, libc::F_GETFL);
            libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
        }

        let client_pairs = if client_id == num_clients - 1 {
            pairs_per_client + remainder
        } else {
            pairs_per_client
        };
        let total_orders = warmup + client_pairs * 2;

        let order_id_offset: u64 = (0..client_id)
            .map(|c| {
                let p = if c == num_clients - 1 {
                    pairs_per_client + remainder
                } else {
                    pairs_per_client
                };
                (warmup + p * 2) as u64
            })
            .sum();

        let frames = {
            let mut flow = generator::OrderFlowGenerator::new(generator::GeneratorConfig {
                num_accounts,
                num_instruments,
                start_order_id: order_id_offset + 1,
                ..Default::default()
            });
            flow.generate_frames(total_orders)
        };

        let conn = BenchConnection {
            writer,
            _read_stream: Box::new(read_stream),
            fd,
            len_buf: [0u8; 4],
            len_filled: 0,
            payload_buf: [0u8; MAX_FRAME_SIZE],
            payload_len: 0,
            payload_filled: 0,
            reading_payload: false,
            frames,
            send_cursor: 0,
            inflight_ts: VecDeque::with_capacity(window),
            batch_count: 0,
            total_orders,
            done: false,
        };

        // Round-robin distribution across threads.
        thread_conns[client_id % num_threads].push(conn);
    }

    // Spawn bench threads.
    let barrier = Arc::new(std::sync::Barrier::new(num_threads + 1));
    let mut handles = Vec::with_capacity(num_threads);

    for (i, conns) in thread_conns.into_iter().enumerate() {
        let barrier = Arc::clone(&barrier);
        let core_id = BENCH_CORE_START + i;
        let handle = std::thread::Builder::new()
            .name(format!("bench-{i}"))
            .spawn(move || {
                if let Err(e) = trading_server::affinity::pin_to_core(core_id) {
                    eprintln!("warning: bench-{i} could not pin to core {core_id}: {e}");
                }
                barrier.wait();
                run_epoll_loop(conns, window)
            })
            .expect("spawn bench thread");
        handles.push(handle);
    }

    // Release all threads simultaneously.
    barrier.wait();
    let blast_start = Instant::now();

    // Collect and merge histograms.
    let mut merged_histogram =
        Histogram::<u64>::new_with_bounds(1, 10_000_000_000, 3).expect("histogram bounds");
    for handle in handles {
        let histogram = handle.join().expect("bench thread panicked");
        merged_histogram.add(&histogram).expect("merge histograms");
    }

    let blast_duration = blast_start.elapsed();

    let mut extra_lines = Vec::new();
    if group_commit_us > 0 {
        extra_lines.push(format!("  Group commit delay: {group_commit_us} µs"));
    }
    extra_lines.push(format!("  Transport: {transport_name}"));
    extra_lines.push(format!("  Bench threads: {num_threads}"));
    extra_lines.push(format!("  Window: {window}, Clients: {num_clients}"));

    print_results(
        "Roundtrip",
        total_pairs * 2,
        warmup * num_clients,
        &merged_histogram,
        blast_duration,
        &extra_lines,
        json_path,
    );

    // Signal server shutdown so pipeline threads can clean up and print
    // latency-trace reports (if the feature is enabled).
    println!();
    println!("=== Pipeline Latency Trace ===");
    println!();
    shutdown.store(true, Ordering::Relaxed);
    // Give pipeline threads time to drain and print reports.
    std::thread::sleep(Duration::from_millis(200));
}

// ===========================================================================
// io_uring roundtrip benchmark
// ===========================================================================

/// io_uring-based roundtrip benchmark. Uses a single thread with io_uring
/// RECV for reads and io_uring SEND for writes, replacing the multi-threaded
/// epoll + blocking-write approach.
#[cfg(feature = "io-uring")]
#[allow(clippy::too_many_arguments)]
fn run_uring_roundtrip<R, W, F>(
    connect: F,
    transport_name: &str,
    total_pairs: usize,
    window: usize,
    num_clients: usize,
    bench_threads: usize,
    group_commit_us: u64,
    shutdown: Arc<AtomicBool>,
    warmup: usize,
    json_path: Option<&std::path::Path>,
    key: &ed25519_dalek::SigningKey,
) where
    R: std::io::Read + std::io::Write + AsRawFd + Send + 'static,
    W: Write + AsRawFd + Send + 'static,
    F: Fn() -> (R, W),
{
    let pairs_per_client = total_pairs / num_clients;
    let remainder = total_pairs % num_clients;

    let mut connections: Vec<UringBenchConn> = Vec::with_capacity(num_clients);

    for client_id in 0..num_clients {
        let (mut read_stream, write_stream) = connect();

        // Challenge-response auth while the socket is still blocking.
        auth_handshake(&mut read_stream, key);

        let read_fd = read_stream.as_raw_fd();
        let write_fd = write_stream.as_raw_fd();

        let client_pairs = if client_id == num_clients - 1 {
            pairs_per_client + remainder
        } else {
            pairs_per_client
        };
        let total_orders = warmup + client_pairs * 2;

        let order_id_offset: u64 = (0..client_id)
            .map(|c| {
                let p = if c == num_clients - 1 {
                    pairs_per_client + remainder
                } else {
                    pairs_per_client
                };
                (warmup + p * 2) as u64
            })
            .sum();

        let frames = {
            let mut flow = generator::OrderFlowGenerator::new(generator::GeneratorConfig {
                num_accounts: 2,
                start_order_id: order_id_offset + 1,
                ..Default::default()
            });
            flow.generate_frames(total_orders)
        };

        connections.push(UringBenchConn {
            read_fd,
            write_fd,
            _read_owner: Box::new(read_stream),
            _write_owner: Box::new(write_stream),
            recv_buf: Box::new([0u8; URING_RECV_BUF_SIZE]),
            parse_buf: Vec::with_capacity(MAX_FRAME_SIZE + 4),
            recv_pending: false,
            send_buf: Vec::with_capacity(4096),
            send_pending: false,
            frames,
            send_cursor: 0,
            inflight_ts: VecDeque::with_capacity(window),
            batch_count: 0,
            total_orders,
            done: false,
        });
    }

    let num_threads = bench_threads.min(num_clients);

    // Distribute connections round-robin across bench threads.
    let mut thread_conns: Vec<Vec<UringBenchConn>> = (0..num_threads).map(|_| Vec::new()).collect();
    for (i, conn) in connections.into_iter().enumerate() {
        thread_conns[i % num_threads].push(conn);
    }

    let start = Instant::now();

    // Spawn io_uring bench threads, each with its own ring and connection subset.
    let handles: Vec<_> = thread_conns
        .into_iter()
        .enumerate()
        .map(|(i, conns)| {
            let core_id = BENCH_CORE_START + i;
            let bench_start = start;
            std::thread::Builder::new()
                .name(format!("bench-{i}"))
                .spawn(move || {
                    if let Err(e) = trading_server::affinity::pin_to_core(core_id) {
                        eprintln!("warning: could not pin bench-{i} to core {core_id}: {e}");
                    }
                    run_uring_loop(conns, window, bench_start, warmup)
                })
                .expect("spawn bench thread")
        })
        .collect();

    // Collect and merge histograms from all threads.
    let mut histogram =
        Histogram::<u64>::new_with_bounds(1, 10_000_000_000, 3).expect("histogram bounds");
    #[cfg(feature = "chart")]
    let mut _series: TimeSeries = Vec::new();
    #[cfg(not(feature = "chart"))]
    let _series: TimeSeries = Vec::new();

    for handle in handles {
        let (h, s) = handle.join().expect("bench thread panicked");
        histogram.add(&h).expect("merge histograms");
        #[cfg(feature = "chart")]
        _series.extend(s);
        #[cfg(not(feature = "chart"))]
        let _ = s;
    }

    let wall = start.elapsed();

    let mut extra_lines = Vec::new();
    if group_commit_us > 0 {
        extra_lines.push(format!("  Group commit delay: {group_commit_us} µs"));
    }
    extra_lines.push(format!("  Transport: {transport_name}"));
    extra_lines.push(format!(
        "  Bench threads: {num_threads} (io_uring, cores {BENCH_CORE_START}-{})",
        BENCH_CORE_START + num_threads - 1
    ));
    extra_lines.push(format!("  Window: {window}, Clients: {num_clients}"));

    print_results(
        "Roundtrip",
        total_pairs * 2,
        warmup * num_clients,
        &histogram,
        wall,
        &extra_lines,
        json_path,
    );

    println!();
    println!("=== Pipeline Latency Trace ===");
    println!();
    shutdown.store(true, Ordering::Relaxed);
    std::thread::sleep(Duration::from_millis(200));

    #[cfg(feature = "chart")]
    show_chart(&_series, &histogram);
}

/// Size of per-connection recv buffer for io_uring RECV.
#[cfg(feature = "io-uring")]
const URING_RECV_BUF_SIZE: usize = 4096;

/// Flag bit in io_uring user_data to distinguish SEND from RECV CQEs.
/// Bit 63 set = SEND completion, clear = RECV completion.
#[cfg(feature = "io-uring")]
const SEND_FLAG: u64 = 1 << 63;

/// Per-connection state for the io_uring benchmark event loop.
#[cfg(feature = "io-uring")]
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

    // Pipelining state
    frames: Vec<Vec<u8>>,
    send_cursor: usize,
    inflight_ts: VecDeque<Instant>,
    batch_count: usize,
    total_orders: usize,
    done: bool,
}

/// io_uring event loop for all benchmark connections. Single-threaded:
/// uses RECV for reads and SEND for writes through one io_uring ring.
/// Returns the cumulative histogram and (when `chart` feature is enabled)
/// a time-series of interval latency percentiles for visualization.
#[cfg(feature = "io-uring")]
fn run_uring_loop(
    mut connections: Vec<UringBenchConn>,
    window: usize,
    bench_start: Instant,
    warmup: usize,
) -> (Histogram<u64>, TimeSeries) {
    use io_uring::{IoUring, opcode, types};

    let n = connections.len();
    let mut ring = IoUring::new(1024).expect("create io_uring for bench");
    let mut histogram =
        Histogram::<u64>::new_with_bounds(1, 10_000_000_000, 3).expect("histogram bounds");
    let mut done_count: usize = 0;

    #[cfg(feature = "chart")]
    let mut interval_hist =
        Histogram::<u64>::new_with_bounds(1, 10_000_000_000, 3).expect("interval histogram");
    #[cfg(feature = "chart")]
    let mut interval_count: usize = 0;
    #[cfg(feature = "chart")]
    let mut series: TimeSeries = Vec::new();
    #[cfg(not(feature = "chart"))]
    let series = TimeSeries::new();
    let _ = &bench_start; // used only with chart feature

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
    uring_fill_windows(&mut ring, &mut connections, window);

    while done_count < n {
        match ring.submit_and_wait(1) {
            Ok(_) => {}
            Err(ref e) if e.raw_os_error() == Some(libc::EINTR) => continue,
            Err(e) => panic!("io_uring submit_and_wait: {e}"),
        }

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
                        let sent_at = conn.inflight_ts.pop_front().expect(
                            "inflight timestamp desync: got BatchEnd without matching send",
                        );
                        let latency_ns = sent_at.elapsed().as_nanos() as u64;
                        if conn.batch_count >= warmup {
                            histogram.record(latency_ns).expect("record");
                            #[cfg(feature = "chart")]
                            {
                                interval_hist.record(latency_ns).expect("record interval");
                                interval_count += 1;
                                maybe_sample(
                                    &mut interval_hist,
                                    &mut interval_count,
                                    &mut series,
                                    bench_start,
                                );
                            }
                        }
                        conn.batch_count += 1;
                        if conn.batch_count >= conn.total_orders {
                            conn.done = true;
                            done_count += 1;
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

                // Resubmit RECV if connection is still active.
                if !conn.done {
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
        }

        // Refill send windows for connections with capacity.
        uring_fill_windows(&mut ring, &mut connections, window);
    }

    (histogram, series)
}

/// Fill send windows for all connections that have capacity and no pending send.
/// Builds a length-prefixed send buffer and submits SEND SQEs.
#[cfg(feature = "io-uring")]
fn uring_fill_windows(
    ring: &mut io_uring::IoUring,
    connections: &mut [UringBenchConn],
    window: usize,
) {
    use io_uring::{opcode, types};

    for (i, conn) in connections.iter_mut().enumerate() {
        if conn.done || conn.send_pending {
            continue;
        }

        // Fill the send buffer with as many frames as the window allows.
        while conn.inflight_ts.len() < window && conn.send_cursor < conn.total_orders {
            let ts = Instant::now();
            let frame = &conn.frames[conn.send_cursor];
            // Write the length-prefixed wire frame into the send buffer.
            let len = frame.len() as u32;
            conn.send_buf.extend_from_slice(&len.to_le_bytes());
            conn.send_buf.extend_from_slice(frame);
            conn.inflight_ts.push_back(ts);
            conn.send_cursor += 1;
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

/// Send frames on all connections that have window capacity.
/// Each connection is flushed independently after its batch, simulating
/// real clients that send and flush without coordinating with each other.
#[cfg(not(feature = "io-uring"))]
fn send_pending<W: Write>(connections: &mut [BenchConnection<W>], window: usize) {
    for conn in connections.iter_mut() {
        if conn.done {
            continue;
        }

        let mut unflushed = 0;
        while conn.inflight_ts.len() < window && conn.send_cursor < conn.total_orders {
            let ts = Instant::now();
            conn.writer
                .write_frame(&conn.frames[conn.send_cursor])
                .expect("write_frame");
            conn.inflight_ts.push_back(ts);
            conn.send_cursor += 1;
            unflushed += 1;
        }

        if unflushed > 0 {
            conn.writer.flush().expect("flush");
        }
    }
}

// ===========================================================================
// Shared reporting
// ===========================================================================

/// Print benchmark results: header, throughput, latency histogram.
/// Optionally writes results to a JSON file for post-processing.
fn print_results(
    label: &str,
    measured_orders: usize,
    warmup_orders: usize,
    histogram: &Histogram<u64>,
    wall: Duration,
    extra_lines: &[String],
    json_path: Option<&std::path::Path>,
) {
    let total_orders = measured_orders + warmup_orders;
    let throughput = (total_orders as f64) / wall.as_secs_f64();
    let wall_ms = wall.as_micros() as f64 / 1000.0;

    println!("=== {label} Benchmark ({measured_orders} measured, {warmup_orders} warmup) ===");
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
    println!("    min:     {:>8.2} µs", histogram.min() as f64 / 1000.0);
    println!(
        "    p50:     {:>8.2} µs",
        histogram.value_at_quantile(0.50) as f64 / 1000.0
    );
    println!(
        "    p90:     {:>8.2} µs",
        histogram.value_at_quantile(0.90) as f64 / 1000.0
    );
    // Print the highest meaningful p9X percentiles based on sample size.
    // Each additional 9 requires 10x more samples for statistical support.
    // p99 needs >=1K, p99.9 needs >=10K, p99.99 needs >=100K, etc.
    let mut nines = 2; // start at p99
    let mut threshold = 1_000usize;
    while threshold <= measured_orders {
        let quantile = 1.0 - 10.0f64.powi(-(nines as i32));
        // Format: p99, p99.9, p99.99, p99.999, ...
        let label = if nines <= 2 {
            "p99".to_string()
        } else {
            format!("p99.{}", "9".repeat(nines - 2))
        };
        let value = histogram.value_at_quantile(quantile) as f64 / 1000.0;
        let padded = format!("{label}:");
        println!("    {padded:<9}{value:>8.2} µs");
        nines += 1;
        threshold *= 10;
    }
    println!("    max:     {:>8.2} µs", histogram.max() as f64 / 1000.0);

    // Write JSON results if requested.
    if let Some(path) = json_path {
        use std::io::Write;

        let throughput = (total_orders as f64) / wall.as_secs_f64();
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

        let json = format!(
            "{{\"label\":\"{label}\",\"measured_orders\":{measured_orders},\"warmup_orders\":{warmup_orders},\"wall_ms\":{:.2},\"throughput_ops\":{:.0},\"latency\":{percentiles}}}",
            wall.as_secs_f64() * 1000.0,
            throughput,
        );

        let mut file = std::fs::File::create(path).expect("create json file");
        file.write_all(json.as_bytes()).expect("write json");
        file.write_all(b"\n").expect("write newline");
        eprintln!("Results written to {}", path.display());
    }
}

/// Display a latency percentile chart using ratatui, then wait for a keypress.
#[cfg(feature = "chart")]
fn show_chart(series: &TimeSeries, histogram: &Histogram<u64>) {
    use std::io::stdout;

    use crossterm::event::{self, Event, KeyCode};
    use crossterm::execute;
    use crossterm::terminal::{EnterAlternateScreen, LeaveAlternateScreen};
    use ratatui::Terminal;
    use ratatui::backend::CrosstermBackend;
    use ratatui::layout::{Constraint, Layout};
    use ratatui::style::{Color, Modifier, Style};
    use ratatui::symbols::Marker;
    use ratatui::text::Span;
    use ratatui::widgets::{
        Axis, Bar, BarChart, BarGroup, Block, Borders, Chart, Dataset, GraphType,
    };

    if series.is_empty() && histogram.is_empty() {
        return;
    }

    // --- Prepare tail stability data ---
    let p99_data: Vec<(f64, f64)> = series.iter().map(|s| (s.elapsed_secs, s.p99_us)).collect();
    let p999_data: Vec<(f64, f64)> = series.iter().map(|s| (s.elapsed_secs, s.p999_us)).collect();
    let p9999_data: Vec<(f64, f64)> = series
        .iter()
        .map(|s| (s.elapsed_secs, s.p9999_us))
        .collect();

    let x_max = series.last().map(|s| s.elapsed_secs).unwrap_or(1.0);
    let y_max = series
        .iter()
        .map(|s| s.p9999_us)
        .fold(0.0f64, f64::max)
        .max(1.0)
        * 1.1;

    // --- Prepare histogram data ---
    // Build log-scale buckets from the HDR histogram for display.
    let hist_buckets: Vec<(String, u64)> = {
        let quantiles = [
            0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 0.999, 0.9999,
        ];
        let mut buckets = Vec::new();
        for window in quantiles.windows(2) {
            let lo = histogram.value_at_quantile(window[0]) as f64 / 1000.0;
            let hi = histogram.value_at_quantile(window[1]) as f64 / 1000.0;
            let pct = ((window[1] - window[0]) * 100.0) as u64;
            let label = format!("{:.0}-{:.0}µs", lo, hi);
            buckets.push((label, pct));
        }
        buckets
    };

    // Enter TUI.
    crossterm::terminal::enable_raw_mode().expect("enable raw mode");
    let mut stdout = stdout();
    execute!(stdout, EnterAlternateScreen).expect("enter alternate screen");
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend).expect("create terminal");

    let mut tab: usize = 0; // 0 = tail stability, 1 = histogram

    loop {
        terminal
            .draw(|frame| {
                let area = Layout::default()
                    .constraints([Constraint::Percentage(100)])
                    .split(frame.area())[0];

                match tab {
                    0 => {
                        // Tail latency stability over time.
                        let datasets = vec![
                            Dataset::default()
                                .name("p99")
                                .marker(Marker::Braille)
                                .graph_type(GraphType::Line)
                                .style(Style::default().fg(Color::Cyan))
                                .data(&p99_data),
                            Dataset::default()
                                .name("p99.9")
                                .marker(Marker::Braille)
                                .graph_type(GraphType::Line)
                                .style(Style::default().fg(Color::Yellow))
                                .data(&p999_data),
                            Dataset::default()
                                .name("p99.99")
                                .marker(Marker::Braille)
                                .graph_type(GraphType::Line)
                                .style(Style::default().fg(Color::Red))
                                .data(&p9999_data),
                        ];

                        let x_labels = vec![
                            Span::raw("0s"),
                            Span::raw(format!("{:.1}s", x_max / 2.0)),
                            Span::raw(format!("{:.1}s", x_max)),
                        ];
                        let y_labels = vec![
                            Span::raw("0"),
                            Span::raw(format!("{:.0} µs", y_max / 2.0)),
                            Span::raw(format!("{:.0} µs", y_max)),
                        ];

                        let chart = Chart::new(datasets)
                            .block(
                                Block::default()
                                    .title(
                                        " Tail Latency Stability [Tab: switch view | q: exit] ",
                                    )
                                    .title_style(
                                        Style::default()
                                            .fg(Color::White)
                                            .add_modifier(Modifier::BOLD),
                                    )
                                    .borders(Borders::ALL),
                            )
                            .x_axis(
                                Axis::default()
                                    .title("Time")
                                    .bounds([0.0, x_max])
                                    .labels(x_labels),
                            )
                            .y_axis(
                                Axis::default()
                                    .title("Latency")
                                    .bounds([0.0, y_max])
                                    .labels(y_labels),
                            );

                        frame.render_widget(chart, area);
                    }
                    1 => {
                        // Latency distribution histogram.
                        let bars: Vec<Bar> = hist_buckets
                            .iter()
                            .map(|(label, pct)| {
                                Bar::default()
                                    .label(label.as_str().into())
                                    .value(*pct)
                                    .style(Style::default().fg(Color::Cyan))
                            })
                            .collect();

                        let bar_chart = BarChart::default()
                            .block(
                                Block::default()
                                    .title(
                                        " Latency Distribution (% of orders) [Tab: switch | q: exit] ",
                                    )
                                    .title_style(
                                        Style::default()
                                            .fg(Color::White)
                                            .add_modifier(Modifier::BOLD),
                                    )
                                    .borders(Borders::ALL),
                            )
                            .data(BarGroup::default().bars(&bars))
                            .bar_width(
                                ((area.width as usize).saturating_sub(4) / bars.len().max(1))
                                    .max(3) as u16,
                            )
                            .bar_gap(1);

                        frame.render_widget(bar_chart, area);
                    }
                    _ => {}
                }
            })
            .expect("draw chart");

        // Handle input.
        if let Ok(Event::Key(key)) = event::read() {
            match key.code {
                KeyCode::Tab => tab = (tab + 1) % 2,
                KeyCode::Char('q') | KeyCode::Esc => break,
                _ => break,
            }
        }
    }

    // Restore terminal.
    crossterm::terminal::disable_raw_mode().expect("disable raw mode");
    execute!(terminal.backend_mut(), LeaveAlternateScreen).expect("leave alternate screen");
}

/// Create a temporary directory that persists for the process lifetime.
fn tempdir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("trading-bench-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}
