//! Single-client rumcast roundtrip bench. Mirrors the TCP/DPDK
//! roundtrip pattern but uses a rumcast publication for orders out and
//! a rumcast subscription for responses in. Reuses
//! [`crate::generator::OrderFlowGenerator`] and `melin-protocol`'s
//! codec — only the I/O substrate differs from the TCP path.
//!
//! Phase 2 wire-up: full pure-UDP authentication (Ed25519 plus
//! X25519 plus per-message BLAKE3 keyed-MAC envelopes) before the
//! measured roundtrip phase begins. The handshake itself runs once
//! at startup and is amortized over the entire run, so the
//! steady-state numbers the bench reports reflect data-plane MAC
//! verify cost only.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use ed25519_dalek::SigningKey;
use hdrhistogram::Histogram;

use melin_protocol::codec;
use melin_protocol::message::{Request, ResponseKind};
use melin_protocol::session::{ClientHandshake, encode_envelope, verify_and_decode_envelope};
use melin_rumcast::pub_log::{PublicationConfig, PublicationLog};
use melin_rumcast::receiver::{ReceiverConfig, ReceiverLoop};
use melin_rumcast::sender::{SenderConfig, SenderLoop};
use melin_rumcast::shared_udp::SharedUdp;
use melin_rumcast::sub_log::{SubscriptionConfig, SubscriptionLog};
use melin_rumcast::wire::{FrameView, data_flags};

use crate::generator::{GeneratorConfig, OrderFlowGenerator};

// MUST match the constants in `melin-server/src/rumcast_transport.rs`.
// The two ends share the wire format; if these drift, the bench gets
// nothing back. `session_id` is NOT a constant — each bench run picks
// a fresh random 32-bit value (Aeron convention) so concurrent bench
// instances against one server don't collide.
const RUMCAST_ORDERS_STREAM: u32 = 1;
const RUMCAST_RESP_STREAM: u32 = 2;
const TERM_LENGTH: u32 = 1024 * 1024;
const MTU: u32 = 1408;
const INITIAL_TERM_ID: u32 = 1;

/// Per-receiver id used in our SMs back to the server. Phase 1 single
/// client; Phase 3 will allocate per-client.
const BENCH_RECEIVER_ID: u64 = 1;

/// Reusable envelope buffer size. The largest inner frame is a
/// codec::encode_request output (≤168B per the codec doc) plus the
/// 24-byte envelope header — 2 KiB gives generous headroom and one
/// allocation per bench run.
const ENVELOPE_BUF_SIZE: usize = 2048;

/// Upper bound on the wait between handshake send and receipt of
/// each control reply. Far longer than any realistic LAN RTT — bails
/// out if the server isn't responding rather than hanging the bench.
const HANDSHAKE_DEADLINE: Duration = Duration::from_secs(10);

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
    /// Client's long-term Ed25519 identity. The server's
    /// `authorized_keys` file must list this key under a permission
    /// that allows order submission (e.g. `trader`). Loaded from
    /// `--key` at the CLI.
    pub signing_key: SigningKey,
}

pub fn run_rumcast_roundtrip(cfg: RumcastBenchConfig) {
    // Per-connect random session_id. Each bench instance picks
    // independently; the 32-bit space makes collisions across
    // concurrent benches against the same server astronomically
    // unlikely. Same convention Aeron uses for publication identity.
    let session_id = generate_session_id();
    eprintln!(
        "rumcast roundtrip: server={} bind={} session_id={:#010x} pairs={} window={} warmup={}",
        cfg.server_addr, cfg.bind, session_id, cfg.pairs, cfg.window, cfg.warmup
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

    // ---- Rumcast endpoints (single shared socket) ----
    //
    // SharedUdp gives us one bound port with two `UdpTransport`
    // halves. The orders Sender uses the send half; the resp
    // Receiver uses the recv half. Internal demux routes
    // Data/Setup/Heartbeat to the recv half, NAK/StatusMessage to
    // the send half. Shared socket means the bench's orders
    // publisher source addr equals its resp subscriber addr — so
    // the server's auto-discovered per-session response dst lands
    // back here correctly.
    let shared = SharedUdp::bind(cfg.bind).expect("shared socket bind");
    let (send_half, recv_half) = shared.split();

    // Outbound: orders publication → server.
    let orders_pub = Arc::new(
        PublicationLog::new(PublicationConfig {
            session_id,
            stream_id: RUMCAST_ORDERS_STREAM,
            initial_term_id: INITIAL_TERM_ID,
            term_length: TERM_LENGTH,
            mtu: MTU,
        })
        .expect("orders publication config"),
    );
    orders_pub.set_publisher_limit(u64::MAX); // single client; we trust ourselves
    let mut orders_send_config = SenderConfig::defaults(cfg.server_addr);
    orders_send_config.setup_interval = Duration::from_millis(100);
    orders_send_config.heartbeat_interval = Duration::from_millis(50);
    orders_send_config.max_drain_per_tick = 1024 * 1024;
    let orders_sender = SenderLoop::new(Arc::clone(&orders_pub), send_half, orders_send_config);

    // Inbound: responses subscription ← server.
    let resp_sub = Arc::new(
        SubscriptionLog::new(SubscriptionConfig {
            session_id,
            stream_id: RUMCAST_RESP_STREAM,
            initial_term_id: INITIAL_TERM_ID,
            term_length: TERM_LENGTH,
        })
        .expect("responses subscription config"),
    );
    let mut resp_recv_config = ReceiverConfig::defaults(cfg.server_addr, BENCH_RECEIVER_ID);
    resp_recv_config.sm_interval = Duration::from_millis(2);
    resp_recv_config.nak_backoff_min = Duration::from_micros(50);
    resp_recv_config.nak_backoff_jitter = Duration::from_micros(50);
    resp_recv_config.max_recv_per_tick = 1024;
    let resp_receiver = ReceiverLoop::new(Arc::clone(&resp_sub), recv_half, resp_recv_config);

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

    // ---- Handshake (Heartbeat → Challenge → ChallengeResponse → ServerReady) ----
    //
    // Runs once before the measured phase. The `session_token`
    // returned here is the BLAKE3 keyed-MAC key both sides use for
    // the rest of the run; loss of the key zeroizes via x25519-dalek's
    // ZeroizeOnDrop when the ClientHandshake helper is consumed.
    let session_token = perform_handshake(&cfg.signing_key, &orders_pub, &resp_sub);
    eprintln!("handshake complete; entering measured phase");

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

    // Per-session counters tracked locally — sender increments
    // outbound_seq before each publish; receiver advances
    // last_inbound_seq on each accepted envelope.
    let mut outbound_seq: u64 = 0;
    let mut last_inbound_seq: u64 = 0;

    // Reusable envelope buffer. Sized for any inner payload + the
    // 24-byte envelope header. The pre-generated frames are all
    // codec::encode_request output — at most ~140B per the codec
    // doc, well under MTU. 2 KiB is generous and one allocation.
    let mut envelope_buf = vec![0u8; ENVELOPE_BUF_SIZE];

    while total_received < total_msgs {
        // Push orders up to the window cap.
        while inflight.len() < cfg.window && total_sent < total_msgs {
            let inner = &frames[total_sent];
            outbound_seq += 1;
            let env_len = encode_envelope(
                &session_token,
                session_id,
                outbound_seq,
                inner,
                &mut envelope_buf,
            )
            .expect("envelope buf large enough");
            // Spin-claim — single producer; backpressure rare.
            loop {
                match orders_pub.try_claim(env_len as u32) {
                    Ok(mut claim) => {
                        claim
                            .payload_mut()
                            .copy_from_slice(&envelope_buf[..env_len]);
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
                // Envelope verify first — drops anything tampered
                // with, replayed, or addressed to a different
                // session. Replay-state tracker advances on success.
                let (seq, decoded_inner) = match verify_and_decode_envelope(
                    &session_token,
                    session_id,
                    last_inbound_seq,
                    payload,
                ) {
                    Ok(x) => x,
                    Err(e) => {
                        // At steady state every server response is
                        // a valid envelope. A failure here means
                        // either a stray pre-handshake frame or a
                        // real bug — surface it loud.
                        eprintln!("envelope verify failed: {e:?}");
                        return;
                    }
                };
                last_inbound_seq = seq;

                let kind = match codec::decode_response(decoded_inner) {
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

/// Pick a fresh 32-bit `session_id` for this bench run via the OS
/// CSPRNG. Two bench instances against the same server pick
/// independently — the 32-bit space makes accidental collisions
/// astronomically unlikely (~10^-10 at 65k concurrent peers).
fn generate_session_id() -> u32 {
    let mut bytes = [0u8; 4];
    getrandom::fill(&mut bytes).expect("getrandom for session_id");
    u32::from_le_bytes(bytes)
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

/// Run the four-message rumcast handshake:
///
/// 1. Bench → server: `Request::Heartbeat` (kickoff — UDP has no
///    `accept` event for the server to react to, so the client has
///    to speak first).
/// 2. Server → bench: `ResponseKind::Challenge { nonce, server_eph }`.
/// 3. Bench → server: `Request::ChallengeResponse { sig, pubkey,
///    client_eph }` signed via [`ClientHandshake::finish`].
/// 4. Server → bench: `ResponseKind::ServerReady`.
///
/// Returns the per-session BLAKE3 keyed-MAC token both sides have
/// derived from the X25519 ECDH + KDF.
///
/// Panics on protocol error or timeout — the bench is a benchmark
/// tool, not a production client; if the handshake doesn't complete
/// cleanly there's nothing useful to measure.
fn perform_handshake(
    signing_key: &SigningKey,
    orders_pub: &PublicationLog,
    resp_sub: &SubscriptionLog,
) -> [u8; 32] {
    // Source 32 bytes of CSPRNG-grade randomness for the X25519
    // ephemeral. getrandom blocks on a freshly-booted Linux kernel
    // until enough entropy is available — fine for our use case
    // (one-shot at startup).
    let mut x25519_secret_bytes = [0u8; 32];
    getrandom::fill(&mut x25519_secret_bytes).expect("getrandom for X25519 ephemeral");

    let handshake = ClientHandshake::new(signing_key, x25519_secret_bytes);

    // Step 1: Heartbeat kickoff.
    let mut buf = vec![0u8; 256];
    let written =
        codec::encode_request(&Request::Heartbeat, 0, &mut buf).expect("encode Heartbeat");
    publish_blocking(orders_pub, &buf[4..written]);

    // Step 2: receive Challenge.
    let challenge_payload = recv_match(resp_sub, Instant::now() + HANDSHAKE_DEADLINE, |bytes| {
        matches!(
            codec::decode_response(bytes),
            Ok(ResponseKind::Challenge { .. })
        )
    })
    .expect("Challenge from server");
    let (nonce, server_eph) = match codec::decode_response(&challenge_payload).unwrap() {
        ResponseKind::Challenge {
            nonce,
            server_x25519_eph,
        } => (nonce, server_x25519_eph),
        other => panic!("expected Challenge, got {other:?}"),
    };

    // Step 3: complete handshake → ChallengeResponse + token.
    let completed = handshake.finish(&nonce, &server_eph);
    let written = codec::encode_request(&completed.challenge_response, 0, &mut buf)
        .expect("encode ChallengeResponse");
    publish_blocking(orders_pub, &buf[4..written]);

    // Step 4: wait for ServerReady (or AuthFailed).
    let server_ready_or_failed =
        recv_match(resp_sub, Instant::now() + HANDSHAKE_DEADLINE, |bytes| {
            matches!(
                codec::decode_response(bytes),
                Ok(ResponseKind::ServerReady) | Ok(ResponseKind::AuthFailed)
            )
        })
        .expect("ServerReady or AuthFailed from server");
    match codec::decode_response(&server_ready_or_failed).unwrap() {
        ResponseKind::ServerReady => {}
        ResponseKind::AuthFailed => {
            panic!(
                "server returned AuthFailed during handshake — \
                 is this client's pubkey listed in the server's \
                 authorized_keys file with trader permission?"
            );
        }
        other => panic!("expected ServerReady or AuthFailed, got {other:?}"),
    }

    completed.session_token
}

/// Spin-claim and publish a payload. Used during the handshake
/// where backpressure is rare (one-shot small frames) and on the
/// hot path (orders).
fn publish_blocking(pub_log: &PublicationLog, payload: &[u8]) {
    loop {
        match pub_log.try_claim(payload.len() as u32) {
            Ok(mut claim) => {
                claim.payload_mut().copy_from_slice(payload);
                claim.publish(data_flags::UNFRAGMENTED);
                return;
            }
            Err(_) => std::hint::spin_loop(),
        }
    }
}

/// Poll the subscription log until a Data fragment passes the
/// supplied predicate or the deadline expires. Returns the matched
/// payload bytes. Used for the four handshake replies — once we
/// switch to envelope-wrapped traffic the bench loop polls inline.
fn recv_match(
    sub: &SubscriptionLog,
    deadline: Instant,
    predicate: impl Fn(&[u8]) -> bool,
) -> Option<Vec<u8>> {
    while Instant::now() < deadline {
        let mut found: Option<Vec<u8>> = None;
        sub.poll(64 * 1024, |view| {
            if found.is_some() {
                return;
            }
            if let FrameView::Data { header, payload } = view
                && header.common.flags & data_flags::PADDING == 0
                && predicate(payload)
            {
                found = Some(payload.to_vec());
            }
        });
        if let Some(bytes) = found {
            return Some(bytes);
        }
        thread::sleep(Duration::from_millis(2));
    }
    None
}
