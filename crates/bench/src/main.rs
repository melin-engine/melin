//! End-to-end pipelined benchmark for the trading engine.
//!
//! Boots the server in-process, connects via TCP (default) or Unix domain
//! socket (`--uds`), and blasts order pairs (buy then sell at the same
//! price from the same account — self-trade, net zero balance change,
//! unlimited cycles).
//!
//! Uses closed-loop windowed pipelining: maintains a fixed number of
//! in-flight orders to keep the pipeline saturated without unbounded
//! queue buildup. Measures per-order round-trip latency under load.
//!
//! Multi-client mode (`--clients=N`) spawns N independent client
//! connections, each with its own pipelining loop. This pushes aggregate
//! event rates beyond the single-client transport bottleneck (~200K/s).
//!
//! Zero async — the server accept loop, pipeline threads, and clients all
//! use blocking I/O. No tokio dependency.
//!
//! Usage:
//!     cargo run --release -p trading-bench [-- [--uds] [--clients=N] [--window=N] [--group-commit-us=N] <order_pairs>]
//!
//! Default: TCP transport, 1 client, 1,000,000 order pairs (2,000,000 total orders).

use std::io::{Read, Write};
use std::num::NonZeroU64;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc as std_mpsc;
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;

use trading_engine::types::*;
use trading_protocol::blocking::{BlockingFrameReader, BlockingFrameWriter};
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

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let use_uds = args.iter().any(|a| a == "--uds");

    let window: usize = parse_flag(&args, "--window=").unwrap_or(DEFAULT_WINDOW);
    let group_commit_us: u64 = parse_flag(&args, "--group-commit-us=").unwrap_or(0);
    let num_clients: usize = parse_flag(&args, "--clients=").unwrap_or(DEFAULT_CLIENTS);

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

        // Factory that creates a new UDS connection.
        let sock_path_ref = &sock_path;
        let connect = || {
            let stream = connect_uds(sock_path_ref);
            let reader = BlockingFrameReader::new(stream.try_clone().expect("clone UDS stream"));
            let writer = BlockingFrameWriter::new(stream);
            (reader, writer)
        };

        run_multi_client(
            connect,
            "Unix domain socket",
            pairs,
            window,
            num_clients,
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
            let reader = BlockingFrameReader::new(stream.try_clone().expect("clone TCP stream"));
            let writer = BlockingFrameWriter::new(stream);
            (reader, writer)
        };

        run_multi_client(
            connect,
            "TCP loopback",
            pairs,
            window,
            num_clients,
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

/// Connect to TCP server with retry.
fn connect_tcp(addr: std::net::SocketAddr) -> std::net::TcpStream {
    for attempt in 1..=50 {
        match std::net::TcpStream::connect(addr) {
            Ok(s) => return s,
            Err(_) if attempt < 50 => std::thread::sleep(Duration::from_millis(10)),
            Err(e) => panic!("failed to connect after 50 attempts: {e}"),
        }
    }
    unreachable!()
}

/// Connect to UDS server with retry.
fn connect_uds(path: &std::path::Path) -> std::os::unix::net::UnixStream {
    for attempt in 1..=50 {
        match std::os::unix::net::UnixStream::connect(path) {
            Ok(s) => return s,
            Err(_) if attempt < 50 => std::thread::sleep(Duration::from_millis(10)),
            Err(e) => panic!("failed to connect after 50 attempts: {e}"),
        }
    }
    unreachable!()
}

/// Spawn N client connections, each running its own pipelining loop, and
/// report aggregate throughput and merged latency histograms.
///
/// The `connect` closure creates a new (reader, writer) pair for each client.
/// All clients synchronize via a barrier before starting their blast loops
/// to ensure they measure concurrent load, not staggered connection setup.
fn run_multi_client<R, W, F>(
    connect: F,
    transport_name: &str,
    total_pairs: usize,
    window: usize,
    num_clients: usize,
    group_commit_us: u64,
    shutdown: Arc<AtomicBool>,
) where
    R: Read + Send + 'static,
    W: Write + Send + 'static,
    F: Fn() -> (BlockingFrameReader<R>, BlockingFrameWriter<W>),
{
    // Divide work evenly across clients. Remaining pairs go to the last client.
    let pairs_per_client = total_pairs / num_clients;
    let remainder = total_pairs % num_clients;

    // Barrier: all clients + main thread wait until everyone is connected and
    // ready, so we measure concurrent load, not staggered setup.
    let barrier = Arc::new(Barrier::new(num_clients + 1));

    // Collect client thread handles. Each returns (histogram, duration).
    let mut handles = Vec::with_capacity(num_clients);

    for client_id in 0..num_clients {
        let (reader, writer) = connect();
        let barrier = Arc::clone(&barrier);

        // Last client picks up remainder pairs.
        let client_pairs = if client_id == num_clients - 1 {
            pairs_per_client + remainder
        } else {
            pairs_per_client
        };

        // Order ID offset: each client gets a unique range to avoid collisions.
        // Client 0: [1, 2*pairs_0 + warmup], Client 1: [offset_1, ...], etc.
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

        let handle = std::thread::Builder::new()
            .name(format!("client-{client_id}"))
            .spawn(move || {
                run_client_loop(
                    reader,
                    writer,
                    client_pairs,
                    window,
                    order_id_offset,
                    barrier,
                )
            })
            .expect("spawn client thread");

        handles.push(handle);
    }

    // Wait for all clients to be ready, then release them simultaneously.
    barrier.wait();
    let blast_start = Instant::now();

    // Collect results from all clients.
    // Histogram range: 1 ns to 10 s, 3 significant digits.
    let mut merged_histogram =
        Histogram::<u64>::new_with_bounds(1, 10_000_000_000, 3).expect("histogram bounds");
    for handle in handles {
        let (histogram, _duration) = handle.join().expect("client thread panicked");
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

/// Run a single client's pipelining loop. Called from a dedicated thread.
///
/// Returns `(histogram, duration)` — the latency histogram (excluding warmup)
/// and the wall-clock duration of the measured blast.
fn run_client_loop<R: Read + Send + 'static, W: Write + Send>(
    reader: BlockingFrameReader<R>,
    mut writer: BlockingFrameWriter<W>,
    pairs: usize,
    window: usize,
    order_id_offset: u64,
    barrier: Arc<Barrier>,
) -> (Histogram<u64>, Duration) {
    let total_orders = WARMUP_ORDERS + (pairs * 2);
    let nz = |v: u64| NonZeroU64::new(v).expect("non-zero");

    // Pre-encode all request frames.
    // Alternating buy/sell at the same price from Account 1 creates
    // self-trades with net zero balance change — unlimited cycles.
    let mut encoded_frames: Vec<Vec<u8>> = Vec::with_capacity(total_orders);
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
        encoded_frames.push(encode_buf[4..written].to_vec());
    }

    // --- Closed-loop windowed pipelining ---
    //
    // Bounded sync channel acts as flow control: the sender blocks when
    // WINDOW timestamps are queued (WINDOW orders in-flight). The receiver
    // pops a timestamp on each BatchEnd, unblocking the sender.
    let (ts_tx, ts_rx) = std_mpsc::sync_channel::<Instant>(window);

    // Spawn receiver thread: reads responses, records per-order latency on each BatchEnd.
    let recv_handle = std::thread::Builder::new()
        .name("bench-reader".into())
        .spawn(move || {
            let mut reader = reader;
            // Histogram range: 1 ns to 10 s, 3 significant digits.
            let mut histogram =
                Histogram::<u64>::new_with_bounds(1, 10_000_000_000, 3).expect("histogram bounds");
            let mut batch_count: usize = 0;

            loop {
                let frame = reader
                    .read_frame()
                    .expect("read_frame")
                    .expect("server disconnected unexpectedly");

                let response = codec::decode_response(&frame).expect("decode response");
                if matches!(response, ResponseKind::BatchEnd) {
                    let sent_at = ts_rx.recv().expect("timestamp channel closed");
                    let latency_ns = sent_at.elapsed().as_nanos() as u64;

                    // Skip warmup orders.
                    if batch_count >= WARMUP_ORDERS {
                        histogram.record(latency_ns).expect("record");
                    }

                    batch_count += 1;
                    if batch_count >= total_orders {
                        break;
                    }
                }
            }

            histogram
        })
        .expect("spawn reader thread");

    // Wait for all clients to be connected and encoded before starting.
    barrier.wait();

    // Sender: pushes timestamp then frame. Blocks when WINDOW orders are
    // in-flight (bounded sync channel). Flushes periodically and always
    // before the channel would block (to prevent deadlock — the receiver
    // needs to see frames on the socket to drain the window).
    let blast_start = Instant::now();
    let mut unflushed: usize = 0;

    for frame in &encoded_frames {
        let ts = Instant::now();

        // Try non-blocking send. If the window is full, flush buffered
        // writes first (so the receiver can process responses and drain
        // the window), then do a blocking send.
        match ts_tx.try_send(ts) {
            Ok(()) => {}
            Err(std_mpsc::TrySendError::Full(ts)) => {
                if unflushed > 0 {
                    writer.flush().expect("flush");
                    unflushed = 0;
                }
                ts_tx.send(ts).expect("timestamp send");
            }
            Err(std_mpsc::TrySendError::Disconnected(_)) => panic!("receiver disconnected"),
        }

        writer.write_frame(frame).expect("write_frame");
        unflushed += 1;

        // Flush periodically to amortize syscall overhead while keeping
        // the pipeline fed.
        if unflushed >= 16 {
            writer.flush().expect("flush");
            unflushed = 0;
        }
    }
    if unflushed > 0 {
        writer.flush().expect("flush");
    }

    // Wait for all responses and get the histogram back.
    let histogram = recv_handle.join().expect("receiver thread panicked");
    let blast_duration = blast_start.elapsed();

    (histogram, blast_duration)
}

/// Create a temporary directory that persists for the process lifetime.
fn tempdir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("trading-bench-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}
