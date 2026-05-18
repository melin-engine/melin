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
use melin_protocol::message::ResponseKind;
#[cfg(not(feature = "dpdk"))]
use melin_protocol::transport::BlockingTransportListener;
#[cfg(not(feature = "dpdk"))]
use melin_server::runtime::server::ServerConfig;
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

// ---------------------------------------------------------------------------
// Open-loop pacing
// ---------------------------------------------------------------------------

/// Slack tolerance for late sends. Any send issued more than this far past
/// its scheduled time counts toward `late_sends`. Set wider than the
/// natural event-loop fill granularity (one `submit_and_wait` cycle, on
/// the order of one RTT for kernel transports) so submit-cycle jitter
/// does not inflate the count. A non-zero value here means the bench is
/// structurally behind its schedule — back-pressure from the server or
/// the inflight cap — not that individual sends are a few microseconds
/// late.
pub(crate) const PACE_LATE_SLACK_NS: u64 = 1_000_000;

/// Per-connection open-loop scheduler. Each connection advances on its own
/// schedule (rate is split across connections at construction time), which
/// avoids cross-thread atomic contention on a shared cursor without
/// changing the aggregate target rate.
///
/// All arithmetic is in TSC ticks: the uring/dpdk hot paths already keep
/// per-frame timing in ticks, so reusing the same unit lets the scheduled
/// timestamp flow directly into `inflight_ts` (no per-send conversion).
#[derive(Clone, Copy)]
pub(crate) struct PaceClock {
    /// Ticks between consecutive scheduled sends on this connection.
    period_ticks: u64,
    /// TSC tick of the next scheduled send.
    next_due_ticks: u64,
}

impl PaceClock {
    /// Build a pacer for one connection given the *aggregate* target rate
    /// (orders/sec across all connections), the connection count it is
    /// shared with, the TSC calibration factor, the bench-start TSC, and
    /// the connection's index within the run. `conn_index` is used to
    /// stagger the first send by a fraction of one period — this avoids a
    /// thundering herd at `start_tsc` while preserving the aggregate rate.
    pub(crate) fn new(
        target_rate: u64,
        clients: u64,
        ticks_per_ns: f64,
        start_tsc: u64,
        conn_index: u64,
    ) -> Self {
        debug_assert!(target_rate > 0, "PaceClock::new requires target_rate > 0");
        debug_assert!(clients > 0, "PaceClock::new requires clients > 0");
        let rate_per_conn = target_rate as f64 / clients as f64;
        let period_ns = 1_000_000_000.0 / rate_per_conn;
        // u64 ticks: a period of ~10 ns at 3 GHz is ~30 ticks; rounding to
        // the nearest tick is well below clock skew across the run.
        let period_ticks = (period_ns * ticks_per_ns).round().max(1.0) as u64;
        // Stagger first send by conn_index * (period / clients). For
        // single-thread runs this leaves a uniform offset; for multi-thread
        // runs threads stay slightly out of phase, which is closer to real
        // client behavior.
        let stagger = period_ticks
            .saturating_mul(conn_index)
            .checked_div(clients)
            .unwrap_or(0);
        Self {
            period_ticks,
            next_due_ticks: start_tsc.saturating_add(stagger),
        }
    }

    /// If the next scheduled send is due at `now_ticks`, return its
    /// scheduled TSC and advance the cursor; otherwise return `None`. The
    /// returned tick is the *scheduled* time, not `now_ticks` — pushing
    /// the scheduled time into the latency record is the standard fix for
    /// coordinated omission.
    #[inline]
    pub(crate) fn pop_due(&mut self, now_ticks: u64) -> Option<u64> {
        if now_ticks >= self.next_due_ticks {
            let scheduled = self.next_due_ticks;
            self.next_due_ticks = self.next_due_ticks.saturating_add(self.period_ticks);
            Some(scheduled)
        } else {
            None
        }
    }

    /// Unconditionally return the next scheduled tick and advance the
    /// cursor. Intended for synchronous loops (engine mode) where the
    /// caller spin-waits until the returned tick before doing work; for
    /// event-loop callers see `pop_due`.
    #[inline]
    pub(crate) fn advance(&mut self) -> u64 {
        let scheduled = self.next_due_ticks;
        self.next_due_ticks = self.next_due_ticks.saturating_add(self.period_ticks);
        scheduled
    }

    /// Reverse the most recent `pop_due` or `advance` so that the popped
    /// scheduled slot is re-issued next call. Used by transports that
    /// pop optimistically but may need to roll back when the wire send
    /// fails — without it, a transient send error would drop a scheduled
    /// slot and skew the achieved rate downward. Only the DPDK path
    /// currently rolls back (smoltcp can return Ok(0) on transient
    /// back-pressure); the kernel-TCP uring path never reaches a state
    /// where a popped frame isn't queued for send.
    #[cfg_attr(not(feature = "dpdk"), allow(dead_code))]
    #[inline]
    pub(crate) fn unpop(&mut self) {
        self.next_due_ticks = self.next_due_ticks.saturating_sub(self.period_ticks);
    }

    #[cfg(test)]
    pub(crate) fn period_ticks(&self) -> u64 {
        self.period_ticks
    }

    #[cfg(test)]
    pub(crate) fn next_due_ticks(&self) -> u64 {
        self.next_due_ticks
    }
}

/// Aggregate pacing telemetry shared across bench threads. Updated lock-free.
#[derive(Default)]
pub(crate) struct PaceStats {
    /// Sends whose actual submission time exceeded `scheduled + slack`.
    /// A non-zero value indicates back-pressure from the server or
    /// inflight cap.
    pub late_sends: AtomicU64,
    /// Maximum observed `actual_send_tsc - scheduled_tsc` in ticks. Read
    /// once at end-of-run and converted to µs for reporting.
    pub max_send_delay_ticks: AtomicU64,
    /// Total scheduled sends (issued or skipped). Useful for the progress
    /// reporter when target-rate is set.
    pub scheduled: AtomicU64,
}

impl PaceStats {
    /// Record a paced send. `now_ticks` is the actual submission time;
    /// `scheduled_ticks` is what `PaceClock::pop_due` returned. If the
    /// delay exceeds `PACE_LATE_SLACK_NS`, `late_sends` is incremented.
    #[inline]
    pub(crate) fn record_send(&self, now_ticks: u64, scheduled_ticks: u64, ticks_per_ns: f64) {
        let delay_ticks = now_ticks.saturating_sub(scheduled_ticks);
        // Lazy max via CAS loop. Contention is essentially nil — only one
        // writer per bench thread, and at multi-M ops/s the value moves
        // monotonically toward the run max.
        let mut prev = self.max_send_delay_ticks.load(Ordering::Relaxed);
        while delay_ticks > prev {
            match self.max_send_delay_ticks.compare_exchange_weak(
                prev,
                delay_ticks,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => prev = actual,
            }
        }
        let slack_ticks = (PACE_LATE_SLACK_NS as f64 * ticks_per_ns) as u64;
        if delay_ticks > slack_ticks {
            self.late_sends.fetch_add(1, Ordering::Relaxed);
        }
        self.scheduled.fetch_add(1, Ordering::Relaxed);
    }
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
    /// Target send rate in orders/sec (open-loop pacing). `0` (default)
    /// disables pacing and falls back to closed-loop window-filling.
    /// When set, each client thread schedules sends at fixed intervals
    /// and pushes the *scheduled* timestamp into the latency histogram —
    /// the standard fix for coordinated omission. `--window` still acts
    /// as a hard inflight cap; if the server stalls and the cap engages
    /// the bench reports `late_sends` rather than silently absorbing the
    /// back-pressure.
    #[arg(long, default_value_t = 0)]
    target_rate: u64,
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
    /// Fail the run (exit code 2) if more than this percent of acknowledged
    /// requests were rejected. Default 50.0% — the realistic-flow
    /// generator naturally produces a few percent of rejections (FOK
    /// can't fill, market orders on cold books, cancels of consumed
    /// orders), so the threshold is set to catch catastrophic misconfig
    /// ("most orders rejected") rather than noise. Set to 100.0 to
    /// disable; lower it for production-flow runs where rejections
    /// should be near-zero.
    #[arg(long, default_value_t = 50.0)]
    max_reject_pct: f64,
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

    // --target-rate requires a non-zero --window: with window=0 the bench
    // cannot keep any inflight sends, so paced sends would never reach the
    // server. Fail loud rather than silently producing a 0-throughput run.
    if args.target_rate > 0 && args.window == 0 {
        eprintln!("error: --target-rate requires --window > 0 (current: 0)");
        std::process::exit(1);
    }

    match args.mode.as_str() {
        "engine" => {
            run_engine_bench(
                phases,
                args.accounts,
                args.instruments,
                json_path,
                args.target_rate,
                args.max_reject_pct,
            );
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
                args.target_rate,
                args.max_reject_pct,
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
                    args.max_reject_pct,
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
                    args.target_rate,
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
                    args.target_rate,
                    args.max_reject_pct,
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
    target_rate: u64,
    max_reject_pct: f64,
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

    #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
    let pace_stats = PaceStats::default();
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        if target_rate > 0 {
            eprintln!(
                "warning: --target-rate ignored on this architecture (requires TSC; x86_64 or aarch64)"
            );
        }
    }

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

    // Outcome counters span both measured and cooldown loops below so a
    // misconfiguration where every order is rejected fails the run loud
    // — see [`OutcomeReport`] doc.
    let mut outcomes = OutcomeReport::default();

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

    // Open-loop pacer for engine mode. Built here — *after* warmup — so
    // its TSC anchor lines up with the measured-phase start. Building it
    // before warmup would leave the schedule stale by `warmup_duration`
    // by the time the measured loop began, blasting through every
    // already-due slot and recording huge spurious late counts.
    #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
    let mut pacer = if target_rate > 0 {
        Some(PaceClock::new(target_rate, 1, ticks_per_ns, rdtscp(), 0))
    } else {
        None
    };

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

        // With pacing, spin until the next scheduled tick, then measure
        // from that tick (not the actual call time) so any "behind
        // schedule" engine slowness shows up as queueing latency rather
        // than being absorbed silently. Without pacing, the loop runs
        // hot and `t0` is just the per-call start tick.
        #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
        let t0 = if let Some(p) = pacer.as_mut() {
            let scheduled = p.advance();
            while rdtscp() < scheduled {
                std::hint::spin_loop();
            }
            let now = rdtscp();
            pace_stats.record_send(now, scheduled, ticks_per_ns);
            scheduled
        } else {
            rdtscp()
        };
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

        // Outcome tally runs *after* `elapsed_ns` was computed above, so
        // walking the reports vec is not billed to the engine-call
        // measurement. One BatchEnd per input event mirrors the
        // network-bench accounting (one BatchEnd per request).
        outcomes.batch_ends += 1;
        for r in reports.iter() {
            outcomes.record_execution_report(r);
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
        // Tally cooldown outcomes too — `OutcomeReport` covers the whole
        // run, mirroring the network bench's cross-phase accounting.
        outcomes.batch_ends += 1;
        for r in reports.iter() {
            outcomes.record_execution_report(r);
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

    #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
    let pacing_report = if target_rate > 0 {
        let max_delay_ns = tsc_to_ns(
            pace_stats.max_send_delay_ticks.load(Ordering::Relaxed),
            ticks_per_ns,
        );
        Some(PacingReport {
            target_rate,
            scheduled: pace_stats.scheduled.load(Ordering::Relaxed),
            late_sends: pace_stats.late_sends.load(Ordering::Relaxed),
            max_send_delay_us: max_delay_ns as f64 / 1_000.0,
        })
    } else {
        None
    };
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    let pacing_report: Option<PacingReport> = None;

    let mut extra_lines = vec![
        format!("  Accounts: {num_accounts}, Instruments: {num_instruments}"),
        format!(
            "  Submits: {submits}, Cancels: {cancels} ({cancel_pct:.1}%), Amends: {amends} ({amend_pct:.1}%)"
        ),
    ];
    if let Some(p) = pacing_report.as_ref() {
        extra_lines.push(format!(
            "  Target rate: {} ops/s (scheduled {}, late {}, max send delay {:.1} µs)",
            p.target_rate, p.scheduled, p.late_sends, p.max_send_delay_us,
        ));
    }

    print_results(
        "Realistic Order Flow",
        measured_orders as usize,
        phases,
        &histogram,
        wall,
        &extra_lines,
        json_path,
        &series,
        &[],
        // Engine mode runs the matching engine in-process with no
        // server / health endpoint, so there's nothing to fetch.
        &stats_client::Body::Empty,
        pacing_report.as_ref(),
        Some(&outcomes),
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

    enforce_rejection_threshold(&outcomes, max_reject_pct);
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
    target_rate: u64,
    max_reject_pct: f64,
) {
    use melin_journal::{BufferedWriter, SectorWriter};
    use melin_server::JournalWriterMode;

    // Set up exchange with one instrument and funded account.
    let mut app = melin_server::domain::exchange_app::ServerApp(
        melin_engine::exchange::Exchange::with_capacity(),
    );
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
        target_rate,
        max_reject_pct,
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
    target_rate: u64,
    max_reject_pct: f64,
}

/// Pipeline-mode body, generic over the journal writer so we get a
/// statically-dispatched `run_sync` or `run_uring` per writer.
// Module-scope imports for `run_pipeline_inner`'s where clause —
// the bound has to name `TradingEvent` / `JournalStage` /
// `JournalStageRun` in the signature scope, not the body scope.
use melin_server::pipeline::{JournalStage, JournalStageRun};
use melin_trading::trading_event::TradingEvent;

fn run_pipeline_inner<W>(app: melin_server::App, writer: W, cfg: PipelineInnerCfg<'_>)
where
    W: melin_server::JournalWrite<TradingEvent> + Send + 'static,
    JournalStage<TradingEvent, W>: JournalStageRun<TradingEvent, Writer = W>,
{
    use melin_journal::JournalEvent;
    use melin_server::pipeline::{InputSlot, build_pipeline_with_replication};
    use melin_server::trace::mono_trace_ns;
    use melin_transport_core::pipeline::OutputPayload;

    let PipelineInnerCfg {
        group_commit_us,
        max_journal_batch,
        phases,
        window,
        json_path,
        target_rate,
        max_reject_pct,
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
    let pace_stats = Arc::new(PaceStats::default());
    let pace_stats_pub = Arc::clone(&pace_stats);
    let publish_handle = std::thread::Builder::new()
        .name("pipeline-pub".into())
        .spawn(move || {
            if let Err(e) = melin_app::affinity::pin_to_core(3) {
                eprintln!("warning: could not pin pipeline-pub to core 3: {e}");
            }
            // Pacer is built inside the thread so its TSC start aligns
            // with the publisher's pinned-core clock. Pipeline mode has
            // one publisher, so `clients=1` keeps the period == the
            // aggregate target.
            let (mut pacer, warmup_end_tsc) = if target_rate > 0 {
                let start_tsc = rdtscp();
                let warmup_ticks = (phases.warmup.as_nanos() as f64 * ticks_per_ns) as u64;
                (
                    Some(PaceClock::new(target_rate, 1, ticks_per_ns, start_tsc, 0)),
                    start_tsc.saturating_add(warmup_ticks),
                )
            } else {
                (None, 0)
            };
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

                // With pacing, gate on the schedule and record the
                // scheduled tick (coordinated-omission fix). Without
                // pacing, fall back to the actual send tick.
                let ts = if let Some(p) = pacer.as_mut() {
                    // Spin until the next scheduled slot is due. Done
                    // here rather than re-entering the outer loop to
                    // avoid mutating the outer order-id counter on
                    // every retry.
                    let (now_tsc, scheduled) = loop {
                        if pub_stop_p.load(Ordering::Relaxed) {
                            return;
                        }
                        let now_tsc = rdtscp();
                        if let Some(scheduled) = p.pop_due(now_tsc) {
                            break (now_tsc, scheduled);
                        }
                        std::hint::spin_loop();
                    };
                    if now_tsc >= warmup_end_tsc {
                        pace_stats_pub.record_send(now_tsc, scheduled, ticks_per_ns);
                    }
                    scheduled
                } else {
                    rdtscp()
                };
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
    let mut outcomes = OutcomeReport::default();
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
            // Capture `rdtscp()` BEFORE the outcome tally below so the
            // histogram reflects only the pipeline roundtrip, not the
            // bench's post-processing cost.
            let latency_ns = tsc_to_ns(rdtscp() - sent_at, ticks_per_ns);
            if now >= deadlines.warmup_end && now < deadlines.measured_end {
                if measured_start.is_none() {
                    measured_start = Some(now);
                }
                histogram.record(latency_ns).expect("record");
                measured_orders += 1;
            }
            // One request boundary per `is_last_in_request` flag — the
            // in-process equivalent of one wire `BatchEnd` frame.
            outcomes.batch_ends += 1;
        }
        // Tally the payload variant *after* the latency capture above.
        // Report payloads count as their inner execution-report variant;
        // EngineError payloads are tracked separately. BatchEnd /
        // QueryResponse payloads carry no order-acceptance signal.
        match slot.payload {
            OutputPayload::Report(report) => outcomes.record_execution_report(&report),
            OutputPayload::EngineError => outcomes.engine_errors += 1,
            OutputPayload::BatchEnd | OutputPayload::QueryResponse(_) => {}
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
    if target_rate > 0 {
        let scheduled = pace_stats.scheduled.load(Ordering::Relaxed);
        let late = pace_stats.late_sends.load(Ordering::Relaxed);
        let max_delay_us = tsc_to_ns(
            pace_stats.max_send_delay_ticks.load(Ordering::Relaxed),
            ticks_per_ns,
        ) as f64
            / 1_000.0;
        extra_lines.push(format!(
            "  Target rate: {target_rate} ops/s (scheduled {scheduled}, late {late}, max send delay {max_delay_us:.1} µs)"
        ));
    }

    let pacing_report = if target_rate > 0 {
        let max_delay_ns = tsc_to_ns(
            pace_stats.max_send_delay_ticks.load(Ordering::Relaxed),
            ticks_per_ns,
        );
        Some(PacingReport {
            target_rate,
            scheduled: pace_stats.scheduled.load(Ordering::Relaxed),
            late_sends: pace_stats.late_sends.load(Ordering::Relaxed),
            max_send_delay_us: max_delay_ns as f64 / 1_000.0,
        })
    } else {
        None
    };

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
        pacing_report.as_ref(),
        Some(&outcomes),
    );

    enforce_rejection_threshold(&outcomes, max_reject_pct);

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
    target_rate: u64,
    max_reject_pct: f64,
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
            target_rate,
            max_reject_pct,
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
        durability_mode: melin_server::runtime::durability_policy::DurabilityMode::Local,
        ..ServerConfig::default()
    };
    // Wire the trading AppFactory: replication / seed paths take it
    // as an argument to `run_with_shutdown`. The bench server runs
    // standalone but still bulk-seeds via the same code path as the
    // binary, so the factory must be constructed even for in-process
    // benchmarks.
    let factory: Arc<dyn melin_app::app_factory::AppFactory<App = melin_server::App>> =
        Arc::new(melin_server::domain::app_factory::ExchangeAppFactory::new(
            melin_server::domain::app_factory::ExchangeAppFactoryConfig {
                accounts: config.accounts,
                instruments: config.instruments,
                max_orders_per_account: config.max_orders_per_account,
                max_orders_per_second: config.max_orders_per_second,
                max_orders_burst: config.max_orders_burst,
            },
        ));

    let shutdown = Arc::new(AtomicBool::new(false));

    // Capture health bind address before config is moved into the server thread.
    let effective_health_addr = health_addr.or(config.health_bind);

    if use_uds {
        use melin_protocol::uds::BlockingUdsListener;

        let sock_path = tmp_dir.join("bench.sock");
        let listener = BlockingUdsListener::bind(&sock_path).expect("bind UDS");
        start_server(
            listener,
            config,
            Arc::clone(&factory),
            Arc::clone(&shutdown),
        );

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
            target_rate,
            max_reject_pct,
        );
    } else {
        use melin_protocol::tcp::BlockingTcpListener;

        let listener = BlockingTcpListener::bind("127.0.0.1:0".parse().expect("valid addr"))
            .expect("bind TCP");
        let addr = listener.local_addr().expect("local addr");
        start_server(
            listener,
            config,
            Arc::clone(&factory),
            Arc::clone(&shutdown),
        );

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
            target_rate,
            max_reject_pct,
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
    factory: Arc<dyn melin_app::app_factory::AppFactory<App = melin_server::App>>,
    shutdown: Arc<AtomicBool>,
) {
    // Trading-side codecs constructed at the call boundary, mirroring
    // the binary in `crates/exchange/server/src/main.rs`.
    let decoder: melin_server::runtime::reader::RequestDecoderArc<melin_server::App> =
        Arc::new(melin_server::domain::request::ExchangeRequestDecoder);
    let encoder: melin_server::runtime::response::ResponseEncoderArc<melin_server::App> =
        Arc::new(melin_server::domain::response_encoder::ExchangeResponseEncoder);
    // The bench has no event subscribers; pass `None` so the runtime
    // never allocates the publisher consumer slot.
    let event_publisher: Option<
        melin_server::runtime::server::EventPublisherFn<melin_server::App>,
    > = None;
    std::thread::Builder::new()
        .name("server".into())
        .spawn(move || {
            if let Err(e) = melin_server::runtime::server::run_with_shutdown(
                listener,
                config,
                factory,
                decoder,
                encoder,
                event_publisher,
                shutdown,
            ) {
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
    target_rate: u64,
    max_reject_pct: f64,
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
        target_rate,
        max_reject_pct,
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
    target_rate: u64,
    pace_stats: Arc<PaceStats>,
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
                let mut buf = [0u8; 256];
                let mut cursor = std::io::Cursor::new(&mut buf[..]);
                if target_rate > 0 {
                    let scheduled = pace_stats.scheduled.load(Ordering::Relaxed);
                    let late = pace_stats.late_sends.load(Ordering::Relaxed);
                    let _ = writeln!(
                        cursor,
                        "  [{elapsed:.1}s/{total_secs:.0}s {pct:.0}% {phase}] scheduled {scheduled} / done {current} / late {late}  {:.0}K/s",
                        rate / 1000.0,
                    );
                } else {
                    let _ = writeln!(
                        cursor,
                        "  [{elapsed:.1}s/{total_secs:.0}s {pct:.0}% {phase}] {current} measured orders  {:.0}K/s",
                        rate / 1000.0,
                    );
                }
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
    target_rate: u64,
    max_reject_pct: f64,
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
            pacer: None,
            outcomes: OutcomeReport::default(),
        });
    }

    let progress = Arc::new(AtomicU64::new(0));
    let progress_shutdown = Arc::new(AtomicBool::new(false));
    let pace_stats = Arc::new(PaceStats::default());
    let progress_handle = spawn_progress_reporter(
        Arc::clone(&progress),
        phases,
        Arc::clone(&progress_shutdown),
        target_rate,
        Arc::clone(&pace_stats),
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
            let thread_pace_stats = Arc::clone(&pace_stats);
            // Global-conn-index mapping mirrors the round-robin
            // distribution above (`thread_conns[i % num_threads]`):
            // this thread's local conn `k` is global conn
            // `thread_idx + k * num_threads`. Passed in so the pacer
            // stagger spreads first sends across *all* connections, not
            // just within each thread.
            let thread_idx = i;
            let total_threads = num_threads;
            std::thread::Builder::new()
                .name(format!("bench-{i}"))
                .spawn(move || {
                    if let Some(core_id) = pin_core
                        && let Err(e) = melin_app::affinity::pin_to_core(core_id)
                    {
                        eprintln!("warning: could not pin bench-{i} to core {core_id}: {e}");
                    }
                    run_uring_loop(
                        conns,
                        window,
                        bench_start,
                        deadlines,
                        thread_progress,
                        target_rate,
                        num_clients,
                        thread_idx,
                        total_threads,
                        phases,
                        thread_pace_stats,
                    )
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
    let mut outcomes = OutcomeReport::default();

    for handle in handles {
        let (h, s, ms, o) = handle.join().expect("bench thread panicked");
        histogram.add(&h).expect("merge histograms");
        if let Some(t) = ms {
            earliest_measured_start =
                Some(earliest_measured_start.map_or(t, |prev: Instant| prev.min(t)));
        }
        all_series.extend(s);
        outcomes.merge(&o);
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

    // Calibrate once for both the human-readable line and the JSON
    // report; calibration sleeps ~50 ms so calling it twice is wasteful.
    // TSC drift between bench threads on the same socket is well below
    // µs, so a single calibration here is fine for the report.
    let pacing_report = if target_rate > 0 {
        let ticks_per_ns = calibrate_tsc();
        let scheduled = pace_stats.scheduled.load(Ordering::Relaxed);
        let late = pace_stats.late_sends.load(Ordering::Relaxed);
        let max_delay_us = tsc_to_ns(
            pace_stats.max_send_delay_ticks.load(Ordering::Relaxed),
            ticks_per_ns,
        ) as f64
            / 1_000.0;
        extra_lines.push(format!(
            "  Target rate: {target_rate} ops/s (scheduled {scheduled}, late {late}, max send delay {max_delay_us:.1} µs)"
        ));
        Some(PacingReport {
            target_rate,
            scheduled,
            late_sends: late,
            max_send_delay_us: max_delay_us,
        })
    } else {
        None
    };

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
        pacing_report.as_ref(),
        Some(&outcomes),
    );

    println!();
    println!("=== Pipeline Latency Trace ===");
    println!();
    shutdown.store(true, Ordering::Relaxed);
    std::thread::sleep(Duration::from_millis(200));

    enforce_rejection_threshold(&outcomes, max_reject_pct);
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
    /// ~15-25ns vDSO overhead per timestamp on the hot path. With
    /// open-loop pacing enabled this stores the *scheduled* TSC instead
    /// of the actual submission TSC — the standard coordinated-omission
    /// fix.
    inflight_ts: VecDeque<u64>,
    /// Open-loop scheduler (when `--target-rate > 0`). Materialised
    /// inside the per-thread bench loop where TSC calibration runs;
    /// constructed `None` initially.
    pacer: Option<PaceClock>,
    /// Counts every execution-report variant received on this
    /// connection. Merged into the run-wide [`OutcomeReport`] after the
    /// thread joins. Kept per-conn (not per-thread) so the recv hot path
    /// touches only this conn's cache lines.
    outcomes: OutcomeReport,
}

/// io_uring event loop for all benchmark connections. Single-threaded:
/// uses RECV for reads and SEND for writes through one io_uring ring.
/// Returns the cumulative histogram and (when `chart` feature is enabled)
/// a time-series of interval latency percentiles for visualization.
#[cfg(not(feature = "dpdk"))]
#[allow(clippy::too_many_arguments)]
fn run_uring_loop(
    mut connections: Vec<UringBenchConn>,
    window: usize,
    bench_start: Instant,
    deadlines: PhaseDeadlines,
    progress: Arc<AtomicU64>,
    target_rate: u64,
    total_clients: usize,
    thread_idx: usize,
    total_threads: usize,
    phases: BenchPhases,
    pace_stats: Arc<PaceStats>,
) -> (Histogram<u64>, TimeSeries, Option<Instant>, OutcomeReport) {
    use io_uring::{IoUring, opcode, types};

    let ticks_per_ns = calibrate_tsc();

    // `warmup_end_tsc` lets pace_stats.record_send skip telemetry for
    // sends scheduled during warmup. Without this gate, `scheduled` and
    // `late_sends` cover all phases while `achieved_rate` covers
    // measured-only — dividing one by the other in the JSON would
    // overestimate the effective load by the warmup ratio.
    let warmup_end_tsc = if target_rate > 0 {
        let warmup_ticks = (phases.warmup.as_nanos() as f64 * ticks_per_ns) as u64;
        rdtscp().saturating_add(warmup_ticks)
    } else {
        0
    };

    // Materialise pacers now that we have a calibration factor and a
    // local TSC reading. Each connection gets its own scheduler keyed off
    // the same `start_tsc`; the global conn index (which spans threads)
    // staggers the first send across the whole run, not just within one
    // thread.
    if target_rate > 0 {
        let start_tsc = rdtscp();
        let clients = total_clients.max(1) as u64;
        for (local_idx, conn) in connections.iter_mut().enumerate() {
            // Round-robin distribution: this thread's local conn `k` is
            // global conn `thread_idx + k * total_threads`.
            let global_idx = (thread_idx + local_idx * total_threads) as u64;
            conn.pacer = Some(PaceClock::new(
                target_rate,
                clients,
                ticks_per_ns,
                start_tsc,
                global_idx,
            ));
        }
    }
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
    uring_fill_windows(
        &mut ring,
        &mut connections,
        window,
        &deadlines,
        &pace_stats,
        ticks_per_ns,
        warmup_end_tsc,
    );

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
                        // `rdtscp()` is captured FIRST — before any
                        // per-frame bookkeeping (outcome tally, parse
                        // buffer compaction) — so the histogram reflects
                        // only the wire roundtrip, not the bench's own
                        // post-processing cost.
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

                    // Tally outcomes across every phase (warmup,
                    // measured, cooldown). Runs *after* the latency
                    // capture above so the histogram measures the wire
                    // roundtrip only — adding this counter increment
                    // before `rdtscp()` would inflate every sample by
                    // the cost of this match.
                    conn.outcomes.record(&response);
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
        uring_fill_windows(
            &mut ring,
            &mut connections,
            window,
            &deadlines,
            &pace_stats,
            ticks_per_ns,
            warmup_end_tsc,
        );
    }

    let mut outcomes = OutcomeReport::default();
    for conn in &connections {
        outcomes.merge(&conn.outcomes);
    }

    (histogram, series, measured_start, outcomes)
}

/// Fill send windows for all connections that have capacity and no pending send.
/// Builds a length-prefixed send buffer and submits SEND SQEs. Stops issuing
/// new frames once the cooldown deadline has passed — the loop above will
/// then terminate as soon as `submit_and_wait` returns (or immediately if
/// the queue is empty).
#[cfg(not(feature = "dpdk"))]
#[allow(clippy::too_many_arguments)]
fn uring_fill_windows(
    ring: &mut io_uring::IoUring,
    connections: &mut [UringBenchConn],
    window: usize,
    deadlines: &PhaseDeadlines,
    pace_stats: &PaceStats,
    ticks_per_ns: f64,
    warmup_end_tsc: u64,
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
        // When pacing is active, `pop_due` gates each push by the
        // schedule; the recorded timestamp is the *scheduled* TSC, which
        // is what closes the coordinated-omission loophole.
        while conn.inflight_ts.len() < window {
            let send_tsc = if let Some(pacer) = conn.pacer.as_mut() {
                let now_tsc = rdtscp();
                match pacer.pop_due(now_tsc) {
                    Some(scheduled) => {
                        // Gate telemetry on warmup-end so `scheduled` /
                        // `late_sends` reflect the same phase as the
                        // throughput divisor (`achieved_rate`).
                        if now_tsc >= warmup_end_tsc {
                            pace_stats.record_send(now_tsc, scheduled, ticks_per_ns);
                        }
                        scheduled
                    }
                    None => break,
                }
            } else {
                rdtscp()
            };
            conn.flow.next_wire_frame(&mut conn.send_buf);
            conn.inflight_ts.push_back(send_tsc);
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

/// Stable ordering of [`RejectReason`] variants used as the index space
/// for [`OutcomeReport::reject_reasons`]. Adding a new reject variant is
/// a compile error inside [`reject_reason_index`] until the entry is
/// appended here too — keep the two in sync.
pub(crate) const REJECT_REASONS: &[(RejectReason, &str)] = &[
    (RejectReason::NoLiquidity, "NoLiquidity"),
    (RejectReason::FOKCannotFill, "FOKCannotFill"),
    (RejectReason::InsufficientBalance, "InsufficientBalance"),
    (RejectReason::UnknownAccount, "UnknownAccount"),
    (RejectReason::UnknownSymbol, "UnknownSymbol"),
    (RejectReason::SelfTradePrevented, "SelfTradePrevented"),
    (RejectReason::DuplicateOrderId, "DuplicateOrderId"),
    (RejectReason::ExceedsMaxOrderQty, "ExceedsMaxOrderQty"),
    (RejectReason::ExceedsMaxNotional, "ExceedsMaxNotional"),
    (RejectReason::TradingHalted, "TradingHalted"),
    (RejectReason::OutsidePriceBand, "OutsidePriceBand"),
    (RejectReason::UnknownOrder, "UnknownOrder"),
    (RejectReason::PriceWouldCross, "PriceWouldCross"),
    (RejectReason::PostOnlyWouldCross, "PostOnlyWouldCross"),
    (RejectReason::HasRestingOrders, "HasRestingOrders"),
    (RejectReason::DuplicateRequest, "DuplicateRequest"),
    (RejectReason::ReplicaDisconnected, "ReplicaDisconnected"),
    (RejectReason::InvalidExpiry, "InvalidExpiry"),
    (RejectReason::InstrumentDisabled, "InstrumentDisabled"),
    (RejectReason::ExceedsMaxOpenOrders, "ExceedsMaxOpenOrders"),
    (RejectReason::ExceedsOrderRate, "ExceedsOrderRate"),
];

fn reject_reason_index(reason: RejectReason) -> usize {
    // `RejectReason` is not `#[repr(u8)]`, so the discriminant isn't a
    // stable index. An exhaustive match makes adding a new variant a
    // compile error until both this function and `REJECT_REASONS` above
    // are updated.
    let idx = match reason {
        RejectReason::NoLiquidity => 0,
        RejectReason::FOKCannotFill => 1,
        RejectReason::InsufficientBalance => 2,
        RejectReason::UnknownAccount => 3,
        RejectReason::UnknownSymbol => 4,
        RejectReason::SelfTradePrevented => 5,
        RejectReason::DuplicateOrderId => 6,
        RejectReason::ExceedsMaxOrderQty => 7,
        RejectReason::ExceedsMaxNotional => 8,
        RejectReason::TradingHalted => 9,
        RejectReason::OutsidePriceBand => 10,
        RejectReason::UnknownOrder => 11,
        RejectReason::PriceWouldCross => 12,
        RejectReason::PostOnlyWouldCross => 13,
        RejectReason::HasRestingOrders => 14,
        RejectReason::DuplicateRequest => 15,
        RejectReason::ReplicaDisconnected => 16,
        RejectReason::InvalidExpiry => 17,
        RejectReason::InstrumentDisabled => 18,
        RejectReason::ExceedsMaxOpenOrders => 19,
        RejectReason::ExceedsOrderRate => 20,
    };
    // Catch silent label/index swaps: an exhaustive match would still
    // type-check if two arms had their integers swapped, but the
    // `REJECT_REASONS` table would then mislabel counts at print time.
    // The existing `reject_reasons_indices_are_unique_and_match_table_length`
    // test calls this for every variant, so a swap explodes there.
    debug_assert_eq!(
        REJECT_REASONS[idx].0, reason,
        "REJECT_REASONS table and reject_reason_index match arms diverged at idx {idx}",
    );
    idx
}

/// Counts of execution-report variants observed by the bench client over
/// the lifetime of a run. Folded across connections and bench threads to
/// surface the rejection ratio in the run summary — without this, a
/// misconfigured run where every order is rejected looks identical to a
/// clean run in the latency histogram.
///
/// Plain `u64` fields (not atomics) because each connection is owned by
/// a single bench thread; merging happens after thread join.
#[derive(Default, Clone)]
pub(crate) struct OutcomeReport {
    /// `BatchEnd` frames received — one per acknowledged request, so
    /// this is the denominator for the rejection ratio.
    pub batch_ends: u64,
    pub placed: u64,
    pub fills: u64,
    pub cancelled: u64,
    pub triggered: u64,
    pub replaced: u64,
    pub instrument_status: u64,
    pub rejected: u64,
    pub engine_errors: u64,
    pub server_busy: u64,
    /// Per-reason rejection counts. Index space defined by
    /// [`REJECT_REASONS`] / [`reject_reason_index`].
    pub reject_reasons: [u64; REJECT_REASONS.len()],
}

impl OutcomeReport {
    /// Increment the counter that matches `response`. Untracked variants
    /// (handshake / market-data / stats frames) are ignored.
    #[inline]
    pub fn record(&mut self, response: &ResponseKind) {
        match response {
            ResponseKind::BatchEnd => self.batch_ends += 1,
            ResponseKind::Report(report) => self.record_execution_report(report),
            ResponseKind::EngineError => self.engine_errors += 1,
            ResponseKind::ServerBusy => self.server_busy += 1,
            // Non-trading frames (Challenge, ServerReady, Heartbeat,
            // AuthFailed, stats/market-data snapshots) — not part of the
            // request/ack accounting.
            _ => {}
        }
    }

    /// Increment the counter for a single execution-report variant.
    /// Used by in-process bench modes (engine, pipeline) which observe
    /// the matching stage's reports directly without going through the
    /// wire `ResponseKind::Report` wrapper.
    #[inline]
    pub fn record_execution_report(&mut self, report: &ExecutionReport) {
        match report {
            ExecutionReport::Placed { .. } => self.placed += 1,
            ExecutionReport::Fill { .. } => self.fills += 1,
            ExecutionReport::Cancelled { .. } => self.cancelled += 1,
            ExecutionReport::Triggered { .. } => self.triggered += 1,
            ExecutionReport::Replaced { .. } => self.replaced += 1,
            ExecutionReport::InstrumentStatusChanged { .. } => self.instrument_status += 1,
            ExecutionReport::Rejected { reason, .. } => {
                self.rejected += 1;
                self.reject_reasons[reject_reason_index(*reason)] += 1;
            }
        }
    }

    pub fn merge(&mut self, other: &OutcomeReport) {
        self.batch_ends += other.batch_ends;
        self.placed += other.placed;
        self.fills += other.fills;
        self.cancelled += other.cancelled;
        self.triggered += other.triggered;
        self.replaced += other.replaced;
        self.instrument_status += other.instrument_status;
        self.rejected += other.rejected;
        self.engine_errors += other.engine_errors;
        self.server_busy += other.server_busy;
        for (a, b) in self
            .reject_reasons
            .iter_mut()
            .zip(other.reject_reasons.iter())
        {
            *a += *b;
        }
    }

    /// Fraction of acknowledged requests that were rejected. Returns
    /// `0.0` when no batches were observed, so callers comparing against
    /// a threshold treat a zero-response run as "no rejections seen"
    /// rather than 100% — a stalled run is a separate failure mode and
    /// is already surfaced by the throughput line.
    pub fn rejection_ratio(&self) -> f64 {
        if self.batch_ends == 0 {
            0.0
        } else {
            self.rejected as f64 / self.batch_ends as f64
        }
    }
}

/// Fail the run with a non-zero exit if more than `max_pct` percent of
/// acknowledged requests were rejected. The CLI default is 50% — the
/// generator naturally produces a few percent of rejections, so the
/// gate targets catastrophic misconfig ("most orders rejected") rather
/// than noise. Lower `max_pct` for production-flow runs where rejections
/// should be near-zero; set it to 100.0 to disable.
pub(crate) fn enforce_rejection_threshold(outcomes: &OutcomeReport, max_pct: f64) {
    let pct = outcomes.rejection_ratio() * 100.0;
    if pct > max_pct {
        eprintln!(
            "error: rejection ratio {pct:.2}% exceeds --max-reject-pct {max_pct:.2}% \
             ({} rejected of {} acknowledged requests). Likely a misconfiguration — \
             check account funding, instrument symbols, and risk limits.",
            outcomes.rejected, outcomes.batch_ends,
        );
        std::process::exit(2);
    }
}

/// End-of-run pacing report. `None` when `--target-rate` is unset (the
/// closed-loop case); rendered into the JSON output and the console
/// summary lines otherwise.
pub(crate) struct PacingReport {
    pub target_rate: u64,
    pub scheduled: u64,
    pub late_sends: u64,
    pub max_send_delay_us: f64,
}

/// Print the outcome summary: acknowledged request count, rejection
/// ratio, and the top reject reasons. Surfaces misconfigured runs (e.g.
/// every order rejected with `InsufficientBalance`) that the latency
/// histogram would otherwise hide.
pub(crate) fn print_outcome_summary(outcomes: &OutcomeReport) {
    println!();
    println!("  Outcomes ({} acknowledged requests)", outcomes.batch_ends);
    if outcomes.batch_ends == 0 {
        println!("    (no responses observed — bench may have stalled before any ack)");
        return;
    }
    let total = outcomes.batch_ends as f64;
    let pct = |n: u64| n as f64 / total * 100.0;
    println!(
        "    rejected:  {:>10} ({:.2}%)",
        outcomes.rejected,
        pct(outcomes.rejected)
    );
    println!("    placed:    {:>10}", outcomes.placed);
    println!("    fills:     {:>10}", outcomes.fills);
    println!("    cancelled: {:>10}", outcomes.cancelled);
    if outcomes.triggered > 0 {
        println!("    triggered: {:>10}", outcomes.triggered);
    }
    if outcomes.replaced > 0 {
        println!("    replaced:  {:>10}", outcomes.replaced);
    }
    if outcomes.engine_errors > 0 {
        println!(
            "    engine errors: {} ({:.2}%)",
            outcomes.engine_errors,
            pct(outcomes.engine_errors)
        );
    }
    if outcomes.server_busy > 0 {
        println!(
            "    server-busy:   {} ({:.2}%)",
            outcomes.server_busy,
            pct(outcomes.server_busy)
        );
    }
    if outcomes.rejected > 0 {
        let mut reasons: Vec<(&str, u64)> = REJECT_REASONS
            .iter()
            .enumerate()
            .filter_map(|(i, (_, name))| {
                let count = outcomes.reject_reasons[i];
                if count > 0 {
                    Some((*name, count))
                } else {
                    None
                }
            })
            .collect();
        // Descending by count so the dominant reason is on top.
        reasons.sort_by_key(|r| std::cmp::Reverse(r.1));
        println!("    reject reasons:");
        for (name, count) in reasons.iter().take(5) {
            println!("      {name}: {count} ({:.2}%)", pct(*count));
        }
    }
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
    pacing: Option<&PacingReport>,
    outcomes: Option<&OutcomeReport>,
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

    // Print outcome summary if we tracked responses.
    if let Some(outcomes) = outcomes {
        print_outcome_summary(outcomes);
    }

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

        // Outcome fragment: emitted only when response tracking was on,
        // so the schema for in-process modes (engine, pipeline) that
        // don't observe wire responses is unchanged.
        let outcomes_json = match outcomes {
            Some(o) => {
                let mut reasons = String::from("{");
                let mut first = true;
                for (i, (_, name)) in REJECT_REASONS.iter().enumerate() {
                    let count = o.reject_reasons[i];
                    if count == 0 {
                        continue;
                    }
                    if !first {
                        reasons.push(',');
                    }
                    first = false;
                    reasons.push_str(&format!("\"{name}\":{count}"));
                }
                reasons.push('}');
                format!(
                    ",\"outcomes\":{{\"batch_ends\":{},\"placed\":{},\"fills\":{},\"cancelled\":{},\"triggered\":{},\"replaced\":{},\"rejected\":{},\"engine_errors\":{},\"server_busy\":{},\"reject_reasons\":{reasons}}}",
                    o.batch_ends,
                    o.placed,
                    o.fills,
                    o.cancelled,
                    o.triggered,
                    o.replaced,
                    o.rejected,
                    o.engine_errors,
                    o.server_busy,
                )
            }
            None => String::new(),
        };

        // Pacing fragment: emitted only when target-rate was set, so the
        // schema for closed-loop runs is unchanged.
        let pacing_json = match pacing {
            Some(p) => format!(
                ",\"pacing\":{{\"target_rate\":{},\"scheduled\":{},\"achieved_rate\":{:.0},\"late_sends\":{},\"max_send_delay_us\":{:.2}}}",
                p.target_rate, p.scheduled, throughput, p.late_sends, p.max_send_delay_us,
            ),
            None => String::new(),
        };

        let json = format!(
            "{{\"label\":\"{label}\",\"measured_orders\":{measured_orders},\"warmup_ms\":{:.2},\"measured_ms\":{:.2},\"cooldown_ms\":{:.2},\"wall_ms\":{:.2},\"throughput_ops\":{:.0},\"latency\":{percentiles},\"time_series\":{ts_json},\"health\":{health_json},\"server_stages\":{stages_json}{pacing_json}{outcomes_json}}}",
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
mod outcome_report_tests {
    use super::*;

    fn dummy_order() -> (OrderId, Symbol, AccountId) {
        (OrderId(1), Symbol(0), AccountId(7))
    }

    fn one_qty() -> Quantity {
        Quantity(NonZeroU64::new(1).unwrap())
    }

    fn one_price() -> Price {
        Price(NonZeroU64::new(100).unwrap())
    }

    #[test]
    fn records_each_variant_into_the_right_bucket() {
        let (oid, sym, acc) = dummy_order();
        let mut r = OutcomeReport::default();
        r.record(&ResponseKind::BatchEnd);
        r.record(&ResponseKind::Report(ExecutionReport::Placed {
            order_id: oid,
            symbol: sym,
            account: acc,
            side: Side::Buy,
            price: one_price(),
            quantity: one_qty(),
        }));
        r.record(&ResponseKind::Report(ExecutionReport::Rejected {
            order_id: oid,
            symbol: sym,
            account: acc,
            reason: RejectReason::InsufficientBalance,
        }));
        r.record(&ResponseKind::EngineError);
        r.record(&ResponseKind::ServerBusy);
        // Heartbeat is intentionally untracked.
        r.record(&ResponseKind::Heartbeat);

        assert_eq!(r.batch_ends, 1);
        assert_eq!(r.placed, 1);
        assert_eq!(r.rejected, 1);
        assert_eq!(r.engine_errors, 1);
        assert_eq!(r.server_busy, 1);
        assert_eq!(
            r.reject_reasons[reject_reason_index(RejectReason::InsufficientBalance)],
            1
        );
    }

    #[test]
    fn merge_sums_all_fields_including_reason_buckets() {
        let (oid, sym, acc) = dummy_order();
        let mut a = OutcomeReport::default();
        a.record(&ResponseKind::Report(ExecutionReport::Rejected {
            order_id: oid,
            symbol: sym,
            account: acc,
            reason: RejectReason::NoLiquidity,
        }));
        a.record(&ResponseKind::BatchEnd);

        let mut b = OutcomeReport::default();
        b.record(&ResponseKind::Report(ExecutionReport::Rejected {
            order_id: oid,
            symbol: sym,
            account: acc,
            reason: RejectReason::NoLiquidity,
        }));
        b.record(&ResponseKind::BatchEnd);
        b.record(&ResponseKind::BatchEnd);

        a.merge(&b);
        assert_eq!(a.batch_ends, 3);
        assert_eq!(a.rejected, 2);
        assert_eq!(
            a.reject_reasons[reject_reason_index(RejectReason::NoLiquidity)],
            2
        );
    }

    #[test]
    fn rejection_ratio_is_zero_when_no_batches_observed() {
        let r = OutcomeReport::default();
        // A stalled run with zero responses returns 0.0 rather than NaN
        // or 1.0 — distinct failure mode, surfaced by throughput, not by
        // the threshold check.
        assert_eq!(r.rejection_ratio(), 0.0);
    }

    #[test]
    fn rejection_ratio_divides_rejected_by_batch_ends() {
        let r = OutcomeReport {
            batch_ends: 1000,
            rejected: 25,
            ..OutcomeReport::default()
        };
        assert!((r.rejection_ratio() - 0.025).abs() < 1e-9);
    }

    #[test]
    fn record_execution_report_and_record_agree_on_report_variants() {
        // Engine and pipeline modes call `record_execution_report`
        // directly; the network bench reaches it via `record` ->
        // `ResponseKind::Report(_)`. Both paths must produce identical
        // counter state for the same input.
        let (oid, sym, acc) = dummy_order();
        let rep = ExecutionReport::Rejected {
            order_id: oid,
            symbol: sym,
            account: acc,
            reason: RejectReason::ExceedsMaxOrderQty,
        };

        let mut via_direct = OutcomeReport::default();
        via_direct.record_execution_report(&rep);

        let mut via_wire = OutcomeReport::default();
        via_wire.record(&ResponseKind::Report(rep));

        assert_eq!(via_direct.rejected, via_wire.rejected);
        assert_eq!(via_direct.reject_reasons, via_wire.reject_reasons);
    }

    #[test]
    fn reject_reasons_indices_are_unique_and_match_table_length() {
        // Sanity check: REJECT_REASONS and reject_reason_index must stay
        // in lockstep. Indices must cover [0, len) without collisions.
        let mut seen = vec![false; REJECT_REASONS.len()];
        for (reason, _) in REJECT_REASONS {
            let idx = reject_reason_index(*reason);
            assert!(idx < REJECT_REASONS.len(), "index {idx} out of range");
            assert!(!seen[idx], "duplicate index {idx} for {reason:?}");
            seen[idx] = true;
        }
        assert!(seen.iter().all(|b| *b), "missing variant in REJECT_REASONS");
    }
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

#[cfg(test)]
mod pace_clock_tests {
    use super::*;

    // 1 tick = 1 ns for predictable arithmetic in these tests.
    const TICKS_PER_NS: f64 = 1.0;

    #[test]
    fn period_matches_aggregate_rate_split_across_clients() {
        // 1 M orders/sec / 4 clients = 250 k/sec per client = 4 µs period.
        let p = PaceClock::new(1_000_000, 4, TICKS_PER_NS, 0, 0);
        assert_eq!(p.period_ticks(), 4_000);
    }

    #[test]
    fn advance_returns_scheduled_and_steps_by_period() {
        let mut p = PaceClock::new(1_000_000, 1, TICKS_PER_NS, 5_000, 0);
        assert_eq!(p.advance(), 5_000);
        assert_eq!(p.advance(), 6_000);
        assert_eq!(p.advance(), 7_000);
    }

    #[test]
    fn unpop_reverses_one_step() {
        let mut p = PaceClock::new(1_000_000, 1, TICKS_PER_NS, 5_000, 0);
        assert_eq!(p.advance(), 5_000);
        assert_eq!(p.advance(), 6_000);
        p.unpop();
        // After unpop, the next advance re-issues 6_000.
        assert_eq!(p.advance(), 6_000);
        assert_eq!(p.advance(), 7_000);
    }

    #[test]
    fn pop_due_is_monotonic_and_paced() {
        let mut p = PaceClock::new(1_000_000, 1, TICKS_PER_NS, 0, 0);
        // 1 µs period at 1 M/s; first 3 sends due at 0, 1000, 2000.
        assert_eq!(p.pop_due(0), Some(0));
        assert_eq!(p.pop_due(999), None);
        assert_eq!(p.pop_due(1_000), Some(1_000));
        assert_eq!(p.pop_due(2_500), Some(2_000));
        // After popping at 2_500, next due is 3_000.
        assert_eq!(p.next_due_ticks(), 3_000);
    }

    #[test]
    fn stagger_offsets_conns_within_one_period() {
        let p0 = PaceClock::new(1_000_000, 4, TICKS_PER_NS, 10_000, 0);
        let p1 = PaceClock::new(1_000_000, 4, TICKS_PER_NS, 10_000, 1);
        let p2 = PaceClock::new(1_000_000, 4, TICKS_PER_NS, 10_000, 2);
        let p3 = PaceClock::new(1_000_000, 4, TICKS_PER_NS, 10_000, 3);
        // period = 4 µs / 4 conns = 1 µs offsets.
        assert_eq!(p0.next_due_ticks(), 10_000);
        assert_eq!(p1.next_due_ticks(), 11_000);
        assert_eq!(p2.next_due_ticks(), 12_000);
        assert_eq!(p3.next_due_ticks(), 13_000);
    }

    /// Regression pin for the multi-thread stagger bug: when bench
    /// threads each constructed pacers using their *thread-local* conn
    /// index instead of the global one, every thread's conn-0 fired at
    /// the same offset, collapsing the herd. Modelling that here: four
    /// conns distributed round-robin across two threads use global
    /// indices 0..3; using local indices 0..1 on each thread would
    /// produce two pacers at the 10_000 anchor and two at the 12_000
    /// stagger — never covering the full period.
    #[test]
    fn stagger_uses_global_index_across_threads() {
        // 1 M aggregate, 4 clients → 4 µs period, 1 µs stagger.
        // Round-robin distribution across 2 threads: thread 0 owns
        // global conns {0, 2}, thread 1 owns {1, 3}.
        let global_indices = [0u64, 2, 1, 3];
        let dues: Vec<u64> = global_indices
            .iter()
            .map(|&i| PaceClock::new(1_000_000, 4, TICKS_PER_NS, 10_000, i).next_due_ticks())
            .collect();
        let mut sorted = dues.clone();
        sorted.sort();
        // First sends cover the whole period at 1 µs spacing.
        assert_eq!(sorted, vec![10_000, 11_000, 12_000, 13_000]);

        // Bug sibling: using the thread-local index (0, 1, 0, 1)
        // collapses two pairs onto the same tick.
        let local_indices = [0u64, 1, 0, 1];
        let buggy: Vec<u64> = local_indices
            .iter()
            .map(|&i| PaceClock::new(1_000_000, 4, TICKS_PER_NS, 10_000, i).next_due_ticks())
            .collect();
        // Two pacers at 10_000 and two at 11_000 — herd flattened only
        // within each thread, not across them.
        let mut buggy_sorted = buggy.clone();
        buggy_sorted.sort();
        assert_eq!(buggy_sorted, vec![10_000, 10_000, 11_000, 11_000]);
    }

    #[test]
    fn period_clamps_to_at_least_one_tick() {
        // Absurdly high rate would round period_ns to 0; clamp prevents
        // an infinite loop in `pop_due` (which would otherwise see every
        // `now` as due forever).
        let p = PaceClock::new(u64::MAX / 2, 1, TICKS_PER_NS, 0, 0);
        assert!(p.period_ticks() >= 1);
    }

    #[test]
    fn record_send_increments_late_when_past_slack() {
        let stats = PaceStats::default();
        // delay just over slack → late.
        stats.record_send(PACE_LATE_SLACK_NS + 1, 0, TICKS_PER_NS);
        // delay just under slack → not late.
        stats.record_send(PACE_LATE_SLACK_NS - 1, 0, TICKS_PER_NS);
        // delay = 0 → not late.
        stats.record_send(0, 0, TICKS_PER_NS);
        assert_eq!(stats.late_sends.load(Ordering::Relaxed), 1);
        assert_eq!(stats.scheduled.load(Ordering::Relaxed), 3);
        // Max should track the largest delay observed.
        assert_eq!(
            stats.max_send_delay_ticks.load(Ordering::Relaxed),
            PACE_LATE_SLACK_NS + 1
        );
    }
}
