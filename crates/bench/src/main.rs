//! End-to-end pipelined benchmark for the trading engine.
//!
//! Boots the server in-process, connects via TCP (default) or Unix domain
//! socket (`--uds`), and blasts order pairs (buy then sell at the same
//! price from the same account — self-trade, net zero balance change,
//! unlimited cycles).
//!
//! Uses a **small pool of epoll client threads** (default 4), each
//! multiplexing a subset of connections via epoll. This avoids the
//! thread oversubscription of 1-thread-per-client-direction (128 threads
//! for 64 clients) while maintaining I/O parallelism. Total threads:
//! ~4 bench + 5 server = 9 on 16 cores — no oversubscription.
//!
//! Closed-loop windowed pipelining: maintains a fixed number of in-flight
//! orders per connection. Measures per-order round-trip latency under load.
//!
//! Zero async — the server accept loop, pipeline threads, and this bench
//! loop all use blocking or epoll-based I/O. No tokio dependency.
//!
//! Usage:
//!     cargo run --release -p trading-bench [-- [--uds] [--clients=N] [--window=N] [--group-commit-us=N] [--bench-threads=N] <order_pairs>]
//!
//! Default: TCP transport, 1 client, 1,000,000 order pairs (2,000,000 total orders).

use std::collections::VecDeque;
use std::io::{self, Write};
use std::num::NonZeroU64;
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;

use trading_engine::types::*;
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
const MAX_EPOLL_EVENTS: usize = 64;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let use_uds = args.iter().any(|a| a == "--uds");

    let window: usize = parse_flag(&args, "--window=").unwrap_or(DEFAULT_WINDOW);
    let group_commit_us: u64 = parse_flag(&args, "--group-commit-us=").unwrap_or(0);
    let num_clients: usize = parse_flag(&args, "--clients=").unwrap_or(DEFAULT_CLIENTS);
    let bench_threads: usize =
        parse_flag(&args, "--bench-threads=").unwrap_or(DEFAULT_BENCH_THREADS);

    let pairs: usize = args
        .iter()
        .filter(|a| !a.starts_with("--"))
        .find_map(|s| s.parse().ok())
        .unwrap_or(DEFAULT_PAIRS);

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
            let writer = BlockingFrameWriter::new(stream);
            (read_stream, writer)
        };

        run_bench(
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
            let writer = BlockingFrameWriter::new(stream);
            (read_stream, writer)
        };

        run_bench(
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
trait AsRawFdSend: AsRawFd + Send {}
impl<T: AsRawFd + Send> AsRawFdSend for T {}

// ---------------------------------------------------------------------------
// Non-blocking frame parsing (adapted from server reader.rs)
// ---------------------------------------------------------------------------

/// Result of attempting to read a complete frame.
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

enum FillResult {
    /// Progressed to `filled` bytes. If `filled < target`, EAGAIN.
    Complete(usize),
    Disconnected,
    Error,
}

/// Non-blocking read into `buf[filled..target]`.
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
fn run_bench<R, W, F>(
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
    W: Write + Send + 'static,
    F: Fn() -> (R, BlockingFrameWriter<W>),
{
    let pairs_per_client = total_pairs / num_clients;
    let remainder = total_pairs % num_clients;

    // Create all connections upfront, then distribute round-robin to threads.
    let num_threads = bench_threads.min(num_clients);
    let mut thread_conns: Vec<Vec<BenchConnection<W>>> =
        (0..num_threads).map(|_| Vec::new()).collect();

    for client_id in 0..num_clients {
        let (read_stream, writer) = connect();
        let fd = read_stream.as_raw_fd();

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

    // --- Report ---
    let total_measured = total_pairs * 2;
    let total_orders = total_measured + (WARMUP_ORDERS * num_clients);
    let throughput = (total_orders as f64) / blast_duration.as_secs_f64();
    let wall_ms = blast_duration.as_micros() as f64 / 1000.0;

    println!(
        "=== Pipelined Benchmark ({total_measured} orders, {} warmup, window={window}, clients={num_clients}) ===",
        WARMUP_ORDERS * num_clients
    );
    if group_commit_us > 0 {
        println!("  Group commit delay: {group_commit_us} µs");
    }
    println!();
    println!("  Transport: {transport_name}");
    println!("  Bench threads: {num_threads}");
    println!();
    println!("  Throughput");
    println!("    wall time:  {wall_ms:.2} ms");
    println!(
        "    throughput: {throughput:.0} orders/sec ({:.2} µs/order)",
        1_000_000.0 / throughput
    );
    println!();
    println!("  Per-Order Round-Trip Latency (all clients merged)");
    println!(
        "    min:    {:>8.2} µs",
        merged_histogram.min() as f64 / 1000.0
    );
    println!(
        "    p50:    {:>8.2} µs",
        merged_histogram.value_at_quantile(0.50) as f64 / 1000.0
    );
    println!(
        "    p90:    {:>8.2} µs",
        merged_histogram.value_at_quantile(0.90) as f64 / 1000.0
    );
    println!(
        "    p99:    {:>8.2} µs",
        merged_histogram.value_at_quantile(0.99) as f64 / 1000.0
    );
    println!(
        "    p99.9:  {:>8.2} µs",
        merged_histogram.value_at_quantile(0.999) as f64 / 1000.0
    );
    println!(
        "    max:    {:>8.2} µs",
        merged_histogram.max() as f64 / 1000.0
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

/// Send frames on all connections that have window capacity.
/// Each connection is flushed independently after its batch, simulating
/// real clients that send and flush without coordinating with each other.
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

/// Create a temporary directory that persists for the process lifetime.
fn tempdir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("trading-bench-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}
