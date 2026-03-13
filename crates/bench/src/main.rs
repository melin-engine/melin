//! Trading engine benchmark suite with three modes:
//!
//! **`--mode=roundtrip`** (default): Full end-to-end benchmark. Boots the server
//! in-process, connects via TCP (default) or Unix domain socket (`--uds`), and
//! blasts order pairs through the complete network round-trip path. Measures
//! client-perceived latency including transport, queuing, journaling, and matching.
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
//! All modes use self-trade pairs (buy then sell at the same price from the same
//! account — net zero balance change, unlimited cycles).
//!
//! Usage:
//!     cargo run --release -p trading-bench [-- [--mode=roundtrip|pipeline|engine] [--uds] [--clients=N] [--window=N] [--group-commit-us=N] [--bench-threads=N] <order_pairs>]
//!
//! Default: roundtrip mode, TCP transport, 1 client, 1,000,000 order pairs.

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
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;

use trading_engine::types::*;
#[cfg(not(feature = "io-uring"))]
use trading_protocol::blocking::BlockingFrameWriter;
use trading_protocol::codec;
use trading_protocol::message::{Request, ResponseKind};
use trading_protocol::transport::BlockingTransportListener;
use trading_server::server::ServerConfig;

/// Number of order pairs (buy + sell) per benchmark run.
const DEFAULT_PAIRS: usize = 1_000_000;

/// Warmup orders (not measured) per client to prime the pipeline and caches.
const WARMUP_ORDERS: usize = 1_000;

/// Default number of orders in flight simultaneously per client. Controls the
/// level of pipelining — enough to keep the server pipeline saturated (journal +
/// matching stages overlap), small enough that per-order latency reflects
/// actual processing time rather than unbounded queueing.
const DEFAULT_WINDOW: usize = 64;

/// Default number of concurrent client connections.
const DEFAULT_CLIENTS: usize = 1;

/// Default number of bench client threads. Each thread manages a subset of
/// connections via epoll. 4 threads is enough to saturate the server pipeline
/// without oversubscribing cores (4 bench + 5 server = 9 threads on 16 cores).
const DEFAULT_BENCH_THREADS: usize = 4;

/// Maximum frame payload size (matches protocol).
const MAX_FRAME_SIZE: usize = 1024;

/// Maximum epoll events per wait call.
#[cfg(not(feature = "io-uring"))]
const MAX_EPOLL_EVENTS: usize = 64;

fn main() {
    // Initialize tracing so pipeline-stats and latency-trace output is visible.
    tracing_subscriber::fmt()
        .with_target(true)
        .with_thread_names(true)
        .init();

    let args: Vec<String> = std::env::args().skip(1).collect();
    let mode: String = parse_flag(&args, "--mode=").unwrap_or_else(|| "roundtrip".into());
    let use_uds = args.iter().any(|a| a == "--uds");

    let pairs: usize = args
        .iter()
        .filter(|a| !a.starts_with("--"))
        .find_map(|s| s.parse().ok())
        .unwrap_or(DEFAULT_PAIRS);

    match mode.as_str() {
        "engine" => {
            warn_ignored_flags(
                &args,
                &[
                    "--clients=",
                    "--bench-threads=",
                    "--window=",
                    "--group-commit-us=",
                    "--uds",
                ],
            );
            run_engine_bench(pairs);
        }
        "pipeline" => {
            warn_ignored_flags(&args, &["--clients=", "--bench-threads=", "--uds"]);
            let window: usize = parse_flag(&args, "--window=").unwrap_or(DEFAULT_WINDOW);
            let group_commit_us: u64 = parse_flag(&args, "--group-commit-us=").unwrap_or(0);
            run_pipeline_bench(pairs, window, group_commit_us);
        }
        "roundtrip" => {
            let window: usize = parse_flag(&args, "--window=").unwrap_or(DEFAULT_WINDOW);
            let group_commit_us: u64 = parse_flag(&args, "--group-commit-us=").unwrap_or(0);
            let num_clients: usize = parse_flag(&args, "--clients=").unwrap_or(DEFAULT_CLIENTS);
            let bench_threads: usize =
                parse_flag(&args, "--bench-threads=").unwrap_or(DEFAULT_BENCH_THREADS);
            run_roundtrip_bench(
                use_uds,
                pairs,
                window,
                num_clients,
                bench_threads,
                group_commit_us,
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

/// Pure matching engine benchmark. Calls `Exchange::execute()` directly in a
/// tight loop with no disruptor, journal, or I/O. Measures the raw cost of
/// order matching and balance management.
fn run_engine_bench(total_pairs: usize) {
    let nz = |v: u64| NonZeroU64::new(v).expect("non-zero");

    let mut exchange = trading_engine::exchange::Exchange::with_capacity();
    exchange.add_instrument(InstrumentSpec {
        symbol: Symbol(1),
        base: CurrencyId(1),
        quote: CurrencyId(2),
    });
    // Deposit enough for all orders. Each buy reserves price(100) * qty(1) = 100
    // quote currency, each sell reserves 1 base currency. Self-trades release
    // immediately, so a generous initial deposit avoids balance exhaustion.
    exchange.deposit(AccountId(1), CurrencyId(1), u64::MAX / 2);
    exchange.deposit(AccountId(1), CurrencyId(2), u64::MAX / 2);

    exchange.prefault();

    let total_orders = WARMUP_ORDERS + total_pairs * 2;
    let mut reports = Vec::with_capacity(256);
    let mut histogram =
        Histogram::<u64>::new_with_bounds(1, 10_000_000_000, 3).expect("histogram bounds");

    // Warmup: prime caches and allocator.
    for i in 0..WARMUP_ORDERS {
        let order_id = OrderId((i as u64) + 1);
        let side = if i % 2 == 0 { Side::Buy } else { Side::Sell };
        reports.clear();
        exchange.execute(
            Symbol(1),
            Order {
                id: order_id,
                account: AccountId(1),
                side,
                order_type: OrderType::Limit {
                    price: Price(nz(100)),
                },
                time_in_force: TimeInForce::GTC,
                quantity: Quantity(nz(1)),
            },
            &mut reports,
        );
    }

    // Measured run.
    let start = Instant::now();
    for i in WARMUP_ORDERS..total_orders {
        let order_id = OrderId((i as u64) + 1);
        let side = if i % 2 == 0 { Side::Buy } else { Side::Sell };
        reports.clear();

        let t0 = Instant::now();
        exchange.execute(
            Symbol(1),
            Order {
                id: order_id,
                account: AccountId(1),
                side,
                order_type: OrderType::Limit {
                    price: Price(nz(100)),
                },
                time_in_force: TimeInForce::GTC,
                quantity: Quantity(nz(1)),
            },
            &mut reports,
        );
        let elapsed_ns = t0.elapsed().as_nanos() as u64;
        histogram.record(elapsed_ns).expect("record");
    }
    let wall = start.elapsed();

    print_results(
        "Engine-Only",
        total_pairs * 2,
        WARMUP_ORDERS,
        &histogram,
        wall,
        &[],
    );
}

// ===========================================================================
// Pipeline benchmark (disruptor + journal + matching, no network)
// ===========================================================================

/// Pipeline benchmark. Builds the full disruptor pipeline (journal stage +
/// matching stage) but bypasses TCP/UDS transport. The bench thread publishes
/// InputSlots directly to the MultiProducer and drains OutputSlots from the
/// SPSC consumer. Measures pipeline latency without network overhead.
fn run_pipeline_bench(total_pairs: usize, window: usize, group_commit_us: u64) {
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
    let journal_path = tmp_dir.join("pipeline-bench.journal");
    let writer = JournalWriter::create(&journal_path).expect("create journal");

    let group_commit_delay = Duration::from_micros(group_commit_us);
    let (producer, journal_stage, matching_stage, mut output_consumer, _journal_cursor) =
        build_pipeline(exchange, writer, group_commit_delay);

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

    let total_orders = WARMUP_ORDERS + total_pairs * 2;
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
                WARMUP_ORDERS,
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
            WARMUP_ORDERS,
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
        WARMUP_ORDERS,
        &histogram,
        wall,
        &extra_lines,
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
fn run_roundtrip_bench(
    use_uds: bool,
    pairs: usize,
    window: usize,
    num_clients: usize,
    bench_threads: usize,
    group_commit_us: u64,
) {
    let tmp_dir = tempdir();
    let journal_path = tmp_dir.join("bench.journal");

    let config = ServerConfig {
        journal_path,
        snapshot_path: None,
        group_commit_delay: Duration::from_micros(group_commit_us),
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
        );
    }

    let _ = std::fs::remove_dir_all(&tmp_dir);
}

/// Parse a `--key=value` flag from the argument list.
fn parse_flag<T: std::str::FromStr>(args: &[String], prefix: &str) -> Option<T> {
    args.iter()
        .find_map(|a| a.strip_prefix(prefix))
        .and_then(|s| s.parse().ok())
}

/// Warn if the user passed flags that are ignored in the current mode.
fn warn_ignored_flags(args: &[String], ignored: &[&str]) {
    for &prefix in ignored {
        if args.iter().any(|a| a.starts_with(prefix) || a == prefix) {
            let name = prefix.trim_end_matches('=');
            eprintln!("warning: {name} is ignored in this mode");
        }
    }
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
                            let sent_at = conn.inflight_ts.pop_front().expect("inflight timestamp");
                            let latency_ns = sent_at.elapsed().as_nanos() as u64;

                            if conn.batch_count >= WARMUP_ORDERS {
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
fn run_roundtrip_inner<R, W, F>(
    connect: F,
    transport_name: &str,
    total_pairs: usize,
    window: usize,
    num_clients: usize,
    #[cfg_attr(feature = "io-uring", allow(unused_variables))] bench_threads: usize,
    group_commit_us: u64,
    shutdown: Arc<AtomicBool>,
) where
    R: AsRawFd + Send + 'static,
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
            group_commit_us,
            shutdown,
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
) where
    R: AsRawFd + Send + 'static,
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
        let (read_stream, write_stream) = connect();
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
        let total_orders = WARMUP_ORDERS + client_pairs * 2;

        let order_id_offset: u64 = (0..client_id)
            .map(|c| {
                let p = if c == num_clients - 1 {
                    pairs_per_client + remainder
                } else {
                    pairs_per_client
                };
                (WARMUP_ORDERS + p * 2) as u64
            })
            .sum();

        let frames = encode_frames(total_orders, order_id_offset);

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
        let handle = std::thread::Builder::new()
            .name(format!("bench-{i}"))
            .spawn(move || {
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
        WARMUP_ORDERS * num_clients,
        &merged_histogram,
        blast_duration,
        &extra_lines,
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
fn run_uring_roundtrip<R, W, F>(
    connect: F,
    transport_name: &str,
    total_pairs: usize,
    window: usize,
    num_clients: usize,
    group_commit_us: u64,
    shutdown: Arc<AtomicBool>,
) where
    R: AsRawFd + Send + 'static,
    W: Write + AsRawFd + Send + 'static,
    F: Fn() -> (R, W),
{
    let pairs_per_client = total_pairs / num_clients;
    let remainder = total_pairs % num_clients;

    let mut connections: Vec<UringBenchConn> = Vec::with_capacity(num_clients);

    for client_id in 0..num_clients {
        let (read_stream, write_stream) = connect();
        let read_fd = read_stream.as_raw_fd();
        let write_fd = write_stream.as_raw_fd();

        let client_pairs = if client_id == num_clients - 1 {
            pairs_per_client + remainder
        } else {
            pairs_per_client
        };
        let total_orders = WARMUP_ORDERS + client_pairs * 2;

        let order_id_offset: u64 = (0..client_id)
            .map(|c| {
                let p = if c == num_clients - 1 {
                    pairs_per_client + remainder
                } else {
                    pairs_per_client
                };
                (WARMUP_ORDERS + p * 2) as u64
            })
            .sum();

        let frames = encode_frames(total_orders, order_id_offset);

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

    let start = Instant::now();
    let histogram = run_uring_loop(connections, window);
    let wall = start.elapsed();

    let mut extra_lines = Vec::new();
    if group_commit_us > 0 {
        extra_lines.push(format!("  Group commit delay: {group_commit_us} µs"));
    }
    extra_lines.push(format!("  Transport: {transport_name}"));
    extra_lines.push("  Bench threads: 1 (io_uring)".to_string());
    extra_lines.push(format!("  Window: {window}, Clients: {num_clients}"));

    print_results(
        "Roundtrip",
        total_pairs * 2,
        WARMUP_ORDERS * num_clients,
        &histogram,
        wall,
        &extra_lines,
    );

    println!();
    println!("=== Pipeline Latency Trace ===");
    println!();
    shutdown.store(true, Ordering::Relaxed);
    std::thread::sleep(Duration::from_millis(200));
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
#[cfg(feature = "io-uring")]
fn run_uring_loop(mut connections: Vec<UringBenchConn>, window: usize) -> Histogram<u64> {
    use io_uring::{IoUring, opcode, types};

    let n = connections.len();
    let mut ring = IoUring::new(1024).expect("create io_uring for bench");
    let mut histogram =
        Histogram::<u64>::new_with_bounds(1, 10_000_000_000, 3).expect("histogram bounds");
    let mut done_count: usize = 0;

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
                        let sent_at = conn.inflight_ts.pop_front().expect("inflight timestamp");
                        let latency_ns = sent_at.elapsed().as_nanos() as u64;
                        if conn.batch_count >= WARMUP_ORDERS {
                            histogram.record(latency_ns).expect("record");
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

    histogram
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

/// Pre-encode all request frames for one connection.
fn encode_frames(total_orders: usize, order_id_offset: u64) -> Vec<Vec<u8>> {
    let nz = |v: u64| NonZeroU64::new(v).expect("non-zero");
    let mut frames = Vec::with_capacity(total_orders);
    let mut encode_buf = [0u8; 128];

    for i in 0..total_orders {
        let order_id = OrderId(order_id_offset + (i as u64) + 1);
        let side = if i % 2 == 0 { Side::Buy } else { Side::Sell };

        let request = Request::SubmitOrder {
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
            },
        };

        let written = codec::encode_request(&request, &mut encode_buf).expect("encode");
        frames.push(encode_buf[4..written].to_vec());
    }

    frames
}

// ===========================================================================
// Shared reporting
// ===========================================================================

/// Print benchmark results: header, throughput, latency histogram.
fn print_results(
    label: &str,
    measured_orders: usize,
    warmup_orders: usize,
    histogram: &Histogram<u64>,
    wall: Duration,
    extra_lines: &[String],
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
    println!("    min:    {:>8.2} µs", histogram.min() as f64 / 1000.0);
    println!(
        "    p50:    {:>8.2} µs",
        histogram.value_at_quantile(0.50) as f64 / 1000.0
    );
    println!(
        "    p90:    {:>8.2} µs",
        histogram.value_at_quantile(0.90) as f64 / 1000.0
    );
    println!(
        "    p99:    {:>8.2} µs",
        histogram.value_at_quantile(0.99) as f64 / 1000.0
    );
    println!(
        "    p99.9:  {:>8.2} µs",
        histogram.value_at_quantile(0.999) as f64 / 1000.0
    );
    println!("    max:    {:>8.2} µs", histogram.max() as f64 / 1000.0);
}

/// Create a temporary directory that persists for the process lifetime.
fn tempdir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("trading-bench-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}
