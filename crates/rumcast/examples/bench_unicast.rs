//! Loopback unicast benchmark: 1 publisher → sender → kernel UDP →
//! receiver → 1 subscriber. Reports throughput (msgs/sec, MB/sec) and
//! one-way latency percentiles (p50 / p90 / p99 / p99.9 / p99.99).
//!
//! Run with: `cargo run --release --example bench_unicast -p melin-rumcast`
//!
//! Output is human-readable; if you want machine-readable, pipe stdout
//! through your favourite parser (the format is stable enough for awk).
//!
//! Knobs at the top of `main` — adjust to taste.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use melin_rumcast::pub_log::{PublicationConfig, PublicationLog};
use melin_rumcast::receiver::{ReceiverConfig, ReceiverLoop};
use melin_rumcast::sender::{SenderConfig, SenderLoop};
use melin_rumcast::sub_log::{SubscriptionConfig, SubscriptionLog};
use melin_rumcast::transport::{KernelUdp, UdpTransport};
use melin_rumcast::wire::{FrameView, data_flags};

const SESSION_ID: u32 = 0xCAFE;
const STREAM_ID: u32 = 0xBABE;
const TERM_LENGTH: u32 = 16 * 1024 * 1024; // 16 MiB
const MTU: u32 = 1408; // typical Ethernet payload after Aeron-style headers
const INITIAL_TERM: u32 = 1;
const PAYLOAD_BYTES: u32 = 64;

const WARMUP_MSGS: usize = 5_000;
const THROUGHPUT_MSGS: usize = 200_000;
const LATENCY_MSGS: usize = 10_000;

fn loopback(port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
}

fn main() {
    println!("rumcast unicast loopback benchmark");
    println!(
        "  term_length={} mtu={} payload={} warmup={} throughput={} latency={}",
        TERM_LENGTH, MTU, PAYLOAD_BYTES, WARMUP_MSGS, THROUGHPUT_MSGS, LATENCY_MSGS
    );

    let pub_log = Arc::new(
        PublicationLog::new(PublicationConfig {
            session_id: SESSION_ID,
            stream_id: STREAM_ID,
            initial_term_id: INITIAL_TERM,
            term_length: TERM_LENGTH,
            mtu: MTU,
        })
        .unwrap(),
    );
    pub_log.set_publisher_limit(u64::MAX);

    let sub_log = Arc::new(
        SubscriptionLog::new(SubscriptionConfig {
            session_id: SESSION_ID,
            stream_id: STREAM_ID,
            initial_term_id: INITIAL_TERM,
            term_length: TERM_LENGTH,
        })
        .unwrap(),
    );

    let sub_socket = KernelUdp::bind(loopback(0)).unwrap();
    let sub_addr = sub_socket.local_addr().unwrap();
    let pub_socket = KernelUdp::bind(loopback(0)).unwrap();
    let pub_addr = pub_socket.local_addr().unwrap();

    let mut sender_config = SenderConfig::defaults(sub_addr);
    sender_config.setup_interval = Duration::from_secs(3600);
    sender_config.heartbeat_interval = Duration::from_secs(3600);
    sender_config.max_drain_per_tick = 1024 * 1024; // 1 MiB per tick
    let sender = SenderLoop::new(Arc::clone(&pub_log), pub_socket, sender_config);

    let mut receiver_config = ReceiverConfig::defaults(pub_addr, 1);
    receiver_config.sm_interval = Duration::from_millis(2);
    receiver_config.nak_backoff_min = Duration::from_micros(50);
    receiver_config.nak_backoff_jitter = Duration::from_micros(50);
    receiver_config.max_recv_per_tick = 4096;
    let receiver = ReceiverLoop::new(Arc::clone(&sub_log), sub_socket, receiver_config);

    let shutdown = Arc::new(AtomicBool::new(false));
    let send_h = spawn_sender(sender, Arc::clone(&shutdown));
    let recv_h = spawn_receiver(receiver, Arc::clone(&shutdown));

    // ---- Warmup ----
    eprintln!("warming up...");
    let warmup_done = Arc::new(AtomicBool::new(false));
    let warmup_log = Arc::clone(&sub_log);
    let warmup_flag = Arc::clone(&warmup_done);
    let warmup_h = thread::spawn(move || {
        let mut delivered = 0usize;
        while !warmup_flag.load(Ordering::Acquire) || delivered < WARMUP_MSGS {
            warmup_log.poll(64 * 1024, |view| {
                if matches!(view, FrameView::Data { .. }) {
                    delivered += 1;
                }
            });
            if delivered >= WARMUP_MSGS {
                return;
            }
            thread::sleep(Duration::from_micros(10));
        }
    });
    for _ in 0..WARMUP_MSGS {
        let mut claim = pub_log.try_claim(PAYLOAD_BYTES).unwrap();
        claim.payload_mut().fill(0);
        claim.publish(data_flags::UNFRAGMENTED);
    }
    // Wait for subscriber to fully drain warmup before starting timed runs.
    warmup_done.store(true, Ordering::Release);
    warmup_h.join().unwrap();

    // ---- Throughput ----
    eprintln!("running throughput phase ({} msgs)...", THROUGHPUT_MSGS);
    let tput_start = Instant::now();
    let tput_log = Arc::clone(&sub_log);
    let tput_h = thread::spawn(move || {
        let mut delivered = 0usize;
        while delivered < THROUGHPUT_MSGS {
            tput_log.poll(64 * 1024, |view| {
                if matches!(view, FrameView::Data { .. }) {
                    delivered += 1;
                }
            });
        }
    });
    for _ in 0..THROUGHPUT_MSGS {
        loop {
            match pub_log.try_claim(PAYLOAD_BYTES) {
                Ok(mut c) => {
                    c.payload_mut().fill(0);
                    c.publish(data_flags::UNFRAGMENTED);
                    break;
                }
                Err(_) => std::hint::spin_loop(),
            }
        }
    }
    tput_h.join().unwrap();
    let tput_elapsed = tput_start.elapsed();

    let bytes_per_msg = (32 + PAYLOAD_BYTES) as f64;
    let msgs_per_sec = THROUGHPUT_MSGS as f64 / tput_elapsed.as_secs_f64();
    let mb_per_sec =
        (THROUGHPUT_MSGS as f64 * bytes_per_msg) / tput_elapsed.as_secs_f64() / 1_000_000.0;

    println!();
    println!("=== Throughput ===");
    println!("  elapsed:    {:?}", tput_elapsed);
    println!(
        "  messages:   {} ({:.2} M msgs/sec)",
        THROUGHPUT_MSGS,
        msgs_per_sec / 1e6
    );
    println!(
        "  wire bytes: {:.2} MB/sec (incl. 32-byte header)",
        mb_per_sec
    );

    // ---- Latency ----
    eprintln!("running latency phase ({} msgs)...", LATENCY_MSGS);
    // Use a u64 nanosecond timestamp in payload[0..8], measure delta on
    // recv. Single-threaded send loop with a brief sleep between
    // messages so the recv side isn't always queue-bound (which would
    // mostly measure queue depth, not transit latency).
    let lat_log = Arc::clone(&sub_log);
    let samples_arc: Arc<std::sync::Mutex<Vec<u64>>> =
        Arc::new(std::sync::Mutex::new(Vec::with_capacity(LATENCY_MSGS)));
    let samples_for_thread = Arc::clone(&samples_arc);
    let lat_done = Arc::new(AtomicBool::new(false));
    let lat_done_flag = Arc::clone(&lat_done);
    let lat_h = thread::spawn(move || {
        let mut delivered = 0usize;
        let mut samples = samples_for_thread.lock().unwrap();
        while !lat_done_flag.load(Ordering::Acquire) || delivered < LATENCY_MSGS {
            lat_log.poll(64 * 1024, |view| {
                if let FrameView::Data { payload, .. } = view
                    && payload.len() >= 8
                {
                    let send_ns = u64::from_le_bytes(payload[0..8].try_into().expect("8 bytes"));
                    let recv_ns = wall_clock_nanos();
                    samples.push(recv_ns.saturating_sub(send_ns));
                    delivered += 1;
                }
            });
            if delivered >= LATENCY_MSGS {
                return;
            }
        }
    });
    for _ in 0..LATENCY_MSGS {
        loop {
            match pub_log.try_claim(PAYLOAD_BYTES) {
                Ok(mut c) => {
                    let now_ns = wall_clock_nanos();
                    c.payload_mut()[0..8].copy_from_slice(&now_ns.to_le_bytes());
                    c.publish(data_flags::UNFRAGMENTED);
                    break;
                }
                Err(_) => std::hint::spin_loop(),
            }
        }
        // Pace at ~100k msgs/sec so we measure transit, not queue depth.
        thread::sleep(Duration::from_micros(10));
    }
    lat_done.store(true, Ordering::Release);
    lat_h.join().unwrap();

    let mut samples = samples_arc.lock().unwrap().clone();
    samples.sort_unstable();
    let p = |q: f64| -> u64 {
        let idx = ((samples.len() as f64) * q).min(samples.len() as f64 - 1.0) as usize;
        samples[idx]
    };
    println!();
    println!("=== One-way latency ({} samples) ===", samples.len());
    println!("  min:    {:>10} ns", samples.first().copied().unwrap_or(0));
    println!("  p50:    {:>10} ns", p(0.50));
    println!("  p90:    {:>10} ns", p(0.90));
    println!("  p99:    {:>10} ns", p(0.99));
    println!("  p99.9:  {:>10} ns", p(0.999));
    println!("  p99.99: {:>10} ns", p(0.9999));
    println!("  max:    {:>10} ns", samples.last().copied().unwrap_or(0));

    shutdown.store(true, Ordering::Release);
    send_h.join().unwrap();
    recv_h.join().unwrap();
}

fn spawn_sender(
    mut sender: SenderLoop<KernelUdp>,
    shutdown: Arc<AtomicBool>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        while !shutdown.load(Ordering::Acquire) {
            let _ = sender.tick();
        }
    })
}

fn spawn_receiver(
    mut receiver: ReceiverLoop<KernelUdp>,
    shutdown: Arc<AtomicBool>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        while !shutdown.load(Ordering::Acquire) {
            let _ = receiver.tick();
        }
    })
}

fn wall_clock_nanos() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}
