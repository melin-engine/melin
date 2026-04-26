//! Single-client rumcast roundtrip bench. Mirrors the TCP/DPDK
//! roundtrip pattern but uses a rumcast publication for orders out and
//! a rumcast subscription for responses in. Reuses
//! [`crate::generator::OrderFlowGenerator`] and `melin-protocol`'s
//! codec — only the I/O substrate differs from the TCP path.
//!
//! Phase 1 wire-up: single bench thread, single client, kernel UDP.
//! Multi-client (Phase 3) and busy-spin idle strategy come later.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;

use melin_protocol::codec;
use melin_protocol::message::ResponseKind;
use melin_rumcast::pub_log::{PublicationConfig, PublicationLog};
use melin_rumcast::receiver::{ReceiverConfig, ReceiverLoop};
use melin_rumcast::sender::{SenderConfig, SenderLoop};
use melin_rumcast::sub_log::{SubscriptionConfig, SubscriptionLog};
use melin_rumcast::transport::KernelUdp;
use melin_rumcast::wire::{FrameView, data_flags};

use crate::generator::{GeneratorConfig, OrderFlowGenerator};

// MUST match the constants in `melin-server/src/rumcast_transport.rs`.
// The two ends share the wire format; if these drift, the bench gets
// nothing back.
const RUMCAST_SESSION_ID: u32 = 0xCAFEBABE;
const RUMCAST_ORDERS_STREAM: u32 = 1;
const RUMCAST_RESP_STREAM: u32 = 2;
const TERM_LENGTH: u32 = 16 * 1024 * 1024;
const MTU: u32 = 1408;
const INITIAL_TERM_ID: u32 = 1;

/// Per-receiver id used in our SMs back to the server. Phase 1 single
/// client; Phase 3 will allocate per-client.
const BENCH_RECEIVER_ID: u64 = 1;

pub struct RumcastBenchConfig {
    pub server_addr: SocketAddr,
    pub bind: SocketAddr,
    pub pairs: usize,
    pub window: usize,
    pub warmup: usize,
    pub accounts: u32,
    pub instruments: u32,
    pub json_path: Option<PathBuf>,
    /// Busy-spin between tick iterations instead of the default 10µs
    /// sleep. Lower latency on isolated cores; burns a CPU. Match the
    /// server's idle strategy for apples-to-apples comparison.
    pub busy_spin: bool,
}

pub fn run_rumcast_roundtrip(cfg: RumcastBenchConfig) {
    eprintln!(
        "rumcast roundtrip: server={} bind={} pairs={} window={} warmup={}",
        cfg.server_addr, cfg.bind, cfg.pairs, cfg.window, cfg.warmup
    );

    // ---- Pre-generate frames (same as TCP/DPDK paths) ----
    //
    // OrderFlowGenerator returns Vec<Vec<u8>> where each inner vec is
    // the codec output WITHOUT the 4-byte length prefix (see
    // generator.rs:295-297). The TCP path then prepends the prefix
    // before sending; rumcast doesn't, because the rumcast DataFrame
    // already provides per-message framing. So we publish each inner
    // vec directly into the rumcast publication.
    let total_msgs = cfg.warmup + cfg.pairs * 2;
    let mut generator = OrderFlowGenerator::new(GeneratorConfig {
        num_accounts: cfg.accounts.max(1),
        num_instruments: cfg.instruments.max(1),
        start_order_id: 1,
        ..Default::default()
    });
    let frames = generator.generate_frames(total_msgs);
    eprintln!("pre-generated {} order frames", frames.len());

    // ---- Rumcast endpoints ----

    // Outbound: orders publication → server.
    let orders_pub = Arc::new(
        PublicationLog::new(PublicationConfig {
            session_id: RUMCAST_SESSION_ID,
            stream_id: RUMCAST_ORDERS_STREAM,
            initial_term_id: INITIAL_TERM_ID,
            term_length: TERM_LENGTH,
            mtu: MTU,
        })
        .expect("orders publication config"),
    );
    orders_pub.set_publisher_limit(u64::MAX); // single client; we trust ourselves
    let orders_socket =
        KernelUdp::bind("0.0.0.0:0".parse::<SocketAddr>().unwrap()).expect("orders socket bind");
    let mut orders_send_config = SenderConfig::defaults(cfg.server_addr);
    orders_send_config.setup_interval = Duration::from_millis(100);
    orders_send_config.heartbeat_interval = Duration::from_millis(50);
    orders_send_config.max_drain_per_tick = 1024 * 1024;
    let orders_sender = SenderLoop::new(Arc::clone(&orders_pub), orders_socket, orders_send_config);

    // Inbound: responses subscription ← server.
    let resp_sub = Arc::new(
        SubscriptionLog::new(SubscriptionConfig {
            session_id: RUMCAST_SESSION_ID,
            stream_id: RUMCAST_RESP_STREAM,
            initial_term_id: INITIAL_TERM_ID,
            term_length: TERM_LENGTH,
        })
        .expect("responses subscription config"),
    );
    let resp_socket = KernelUdp::bind(cfg.bind).expect("responses socket bind");
    let mut resp_recv_config = ReceiverConfig::defaults(cfg.server_addr, BENCH_RECEIVER_ID);
    resp_recv_config.sm_interval = Duration::from_millis(2);
    resp_recv_config.nak_backoff_min = Duration::from_micros(50);
    resp_recv_config.nak_backoff_jitter = Duration::from_micros(50);
    resp_recv_config.max_recv_per_tick = 1024;
    let resp_receiver = ReceiverLoop::new(Arc::clone(&resp_sub), resp_socket, resp_recv_config);

    // ---- Background tick threads ----
    let shutdown = Arc::new(AtomicBool::new(false));
    let mut handles: Vec<thread::JoinHandle<()>> = Vec::new();

    let busy_spin = cfg.busy_spin;
    {
        let shutdown = Arc::clone(&shutdown);
        let mut sender = orders_sender;
        handles.push(
            thread::Builder::new()
                .name("rumcast-bench-orders-send".into())
                .spawn(move || tick_loop(&shutdown, busy_spin, || sender.tick()))
                .expect("spawn orders-send"),
        );
    }
    {
        let shutdown = Arc::clone(&shutdown);
        let mut receiver = resp_receiver;
        handles.push(
            thread::Builder::new()
                .name("rumcast-bench-resp-recv".into())
                .spawn(move || tick_loop(&shutdown, busy_spin, || receiver.tick()))
                .expect("spawn resp-recv"),
        );
    }

    // ---- Bench loop (windowed pipelining) ----
    //
    // Mirror the TCP path: maintain `window` orders in flight at all
    // times. Push `inflight_send_ts.push_back(now)` on send, pop on
    // BatchEnd recv, record latency. Skip the first `warmup` samples.
    let mut inflight: std::collections::VecDeque<Instant> =
        std::collections::VecDeque::with_capacity(cfg.window);
    let mut hist =
        Histogram::<u64>::new_with_bounds(1, 10_000_000_000, 3).expect("histogram bounds");
    let mut total_sent = 0usize;
    let mut total_received = 0usize;
    let mut bench_start = Instant::now();

    while total_received < total_msgs {
        // Push orders up to the window cap.
        while inflight.len() < cfg.window && total_sent < total_msgs {
            let payload = &frames[total_sent];
            // Spin-claim — single producer; backpressure rare.
            loop {
                match orders_pub.try_claim(payload.len() as u32) {
                    Ok(mut claim) => {
                        claim.payload_mut().copy_from_slice(payload);
                        claim.publish(data_flags::UNFRAGMENTED);
                        break;
                    }
                    Err(_) => std::hint::spin_loop(),
                }
            }
            inflight.push_back(Instant::now());
            total_sent += 1;
            if total_sent == cfg.warmup {
                // Reset the bench start so throughput excludes warmup.
                bench_start = Instant::now();
                hist.reset();
                eprintln!("warmup complete, starting measured phase");
            }
        }

        // Drain responses up to window's worth.
        let mut drained_now = 0usize;
        resp_sub.poll(64 * 1024, |view| {
            if let FrameView::Data { header, payload } = view
                && header.common.flags & data_flags::PADDING == 0
            {
                let kind = match codec::decode_response(payload) {
                    Ok(k) => k,
                    Err(e) => {
                        eprintln!("decode_response: {e:?}");
                        return;
                    }
                };
                if matches!(kind, ResponseKind::BatchEnd)
                    && let Some(sent_ts) = inflight.pop_front()
                {
                    let latency_ns = sent_ts.elapsed().as_nanos() as u64;
                    if total_received >= cfg.warmup {
                        let _ = hist.record(latency_ns);
                    }
                    total_received += 1;
                    drained_now += 1;
                }
            }
        });

        // No progress? Either yield (default, friendly to the OS) or
        // busy-spin (matches `--rumcast-busy-spin`, lowest latency on
        // an isolated core).
        if drained_now == 0 {
            if busy_spin {
                std::hint::spin_loop();
            } else {
                thread::sleep(Duration::from_micros(10));
            }
        }
    }

    let elapsed = bench_start.elapsed();
    let measured = total_msgs - cfg.warmup;
    println!();
    println!("=== rumcast roundtrip ({} measured msgs) ===", measured);
    println!("  elapsed:    {:?}", elapsed);
    println!(
        "  throughput: {:.2} K msgs/sec",
        (measured as f64 / elapsed.as_secs_f64()) / 1_000.0
    );
    println!("  min:    {:>10} ns", hist.min());
    println!("  p50:    {:>10} ns", hist.value_at_quantile(0.50));
    println!("  p90:    {:>10} ns", hist.value_at_quantile(0.90));
    println!("  p99:    {:>10} ns", hist.value_at_quantile(0.99));
    println!("  p99.9:  {:>10} ns", hist.value_at_quantile(0.999));
    println!("  p99.99: {:>10} ns", hist.value_at_quantile(0.9999));
    println!("  max:    {:>10} ns", hist.max());

    if let Some(path) = cfg.json_path.as_ref() {
        let json = serde_json::json!({
            "transport": "rumcast",
            "measured_msgs": measured,
            "elapsed_ns": elapsed.as_nanos(),
            "throughput_msgs_per_sec": (measured as f64 / elapsed.as_secs_f64()),
            "latency_ns": {
                "min": hist.min(),
                "p50": hist.value_at_quantile(0.50),
                "p90": hist.value_at_quantile(0.90),
                "p99": hist.value_at_quantile(0.99),
                "p99_9": hist.value_at_quantile(0.999),
                "p99_99": hist.value_at_quantile(0.9999),
                "max": hist.max(),
            },
        });
        if let Err(e) = std::fs::write(path, serde_json::to_string_pretty(&json).unwrap()) {
            eprintln!("failed to write JSON results to {}: {e}", path.display());
        }
    }

    shutdown.store(true, Ordering::Release);
    for h in handles {
        let _ = h.join();
    }
}

/// Generic tick loop body shared by the bench's sender / receiver
/// threads. Mirrors `melin_server::rumcast_transport::tick_loop`.
/// `busy_spin = true` → `spin_loop` hint between ticks (lowest
/// latency, burns a CPU). `busy_spin = false` → 10µs sleep.
#[inline]
fn tick_loop<F: FnMut() -> R, R>(shutdown: &AtomicBool, busy_spin: bool, mut tick: F) {
    while !shutdown.load(Ordering::Acquire) {
        let _ = tick();
        if busy_spin {
            std::hint::spin_loop();
        } else {
            thread::sleep(Duration::from_micros(10));
        }
    }
}
