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
//! Zero async — the server accept loop, pipeline threads, and client all
//! use blocking I/O. No tokio dependency.
//!
//! Usage:
//!     cargo run --release -p trading-bench [-- [--uds] <order_pairs>]
//!
//! Default: TCP transport, 1,000,000 order pairs (2,000,000 total orders).

use std::io::{Read, Write};
use std::num::NonZeroU64;
use std::path::PathBuf;
use std::sync::Arc;
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

/// Warmup orders (not measured) to prime the pipeline and caches.
const WARMUP_ORDERS: usize = 1_000;

/// Number of orders in flight simultaneously. Controls the level of
/// pipelining — enough to keep the server pipeline saturated (journal +
/// matching stages overlap), small enough that per-order latency reflects
/// actual processing time rather than unbounded queueing.
const WINDOW: usize = 64;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let use_uds = args.iter().any(|a| a == "--uds");
    let pairs: usize = args
        .iter()
        .filter(|a| *a != "--uds")
        .find_map(|s| s.parse().ok())
        .unwrap_or(DEFAULT_PAIRS);

    let tmp_dir = tempdir();
    let journal_path = tmp_dir.join("bench.journal");

    let config = ServerConfig {
        journal_path,
        snapshot_path: None,
        ..ServerConfig::default()
    };

    let shutdown = Arc::new(AtomicBool::new(false));

    if use_uds {
        use trading_protocol::uds::BlockingUdsListener;

        let sock_path = tmp_dir.join("bench.sock");
        let listener = BlockingUdsListener::bind(&sock_path).expect("bind UDS");
        start_server(listener, config, Arc::clone(&shutdown));

        let stream = connect_uds(&sock_path);
        let reader = BlockingFrameReader::new(stream.try_clone().expect("clone UDS stream"));
        let writer = BlockingFrameWriter::new(stream);
        run_bench_loop(reader, writer, "Unix domain socket", pairs, shutdown);
    } else {
        use trading_protocol::tcp::BlockingTcpListener;

        let listener = BlockingTcpListener::bind("127.0.0.1:0".parse().expect("valid addr"))
            .expect("bind TCP");
        let addr = listener.local_addr().expect("local addr");
        start_server(listener, config, Arc::clone(&shutdown));

        let stream = connect_tcp(addr);
        stream.set_nodelay(true).expect("set TCP_NODELAY");
        let reader = BlockingFrameReader::new(stream.try_clone().expect("clone TCP stream"));
        let writer = BlockingFrameWriter::new(stream);
        run_bench_loop(reader, writer, "TCP loopback", pairs, shutdown);
    }

    let _ = std::fs::remove_dir_all(&tmp_dir);
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

/// Run the core benchmark loop: encode, send, receive, report.
///
/// Pure blocking I/O — no async runtime anywhere.
fn run_bench_loop<R: Read + Send + 'static, W: Write + Send>(
    reader: BlockingFrameReader<R>,
    mut writer: BlockingFrameWriter<W>,
    transport_name: &str,
    pairs: usize,
    shutdown: Arc<AtomicBool>,
) {
    let total_orders = WARMUP_ORDERS + (pairs * 2);
    let nz = |v: u64| NonZeroU64::new(v).expect("non-zero");

    // Pre-encode all request frames.
    // Alternating buy/sell at the same price from Account 1 creates
    // self-trades with net zero balance change — unlimited cycles.
    let mut encoded_frames: Vec<Vec<u8>> = Vec::with_capacity(total_orders);
    let mut encode_buf = [0u8; 128];

    for i in 0..total_orders {
        let order_id = OrderId((i as u64) + 1);
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
    let (ts_tx, ts_rx) = std_mpsc::sync_channel::<Instant>(WINDOW);

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

    // --- Report ---
    let measured_orders = pairs * 2;
    // Throughput uses total_orders (including warmup) since blast_duration
    // covers the entire run. The pipeline is warm for all but the first
    // few hundred orders, so this is representative of steady state.
    let throughput = (total_orders as f64) / blast_duration.as_secs_f64();
    let wall_ms = blast_duration.as_micros() as f64 / 1000.0;

    println!(
        "=== Pipelined Benchmark ({measured_orders} orders, {WARMUP_ORDERS} warmup, window={WINDOW}) ==="
    );
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
    println!("  Per-Order Round-Trip Latency");
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

    // Signal server shutdown so pipeline threads can clean up and print
    // latency-trace reports (if the feature is enabled).
    println!();
    println!("=== Pipeline Latency Trace ===");
    println!();
    shutdown.store(true, Ordering::Relaxed);
    // Give pipeline threads time to drain and print reports.
    std::thread::sleep(Duration::from_millis(200));
}

/// Create a temporary directory that persists for the process lifetime.
fn tempdir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("trading-bench-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}
