//! Multi-client rumcast roundtrip bench. Mirrors the TCP/DPDK
//! roundtrip pattern but uses a [`MuxedSender`] for outbound orders
//! and a [`MuxedReceiver`] for inbound responses. Each client gets its
//! own `session_id`, [`PublicationLog`], envelope token, and inflight
//! deque — same primitives the server already uses on its side, so
//! one bench process can drive N concurrent authenticated sessions
//! through one shared UDP socket.
//!
//! Threading: a single main thread drives both muxer ticks and the
//! bench logic. The ticks are cheap when nothing is pending, and
//! co-locating them with the order-generation loop avoids the
//! scheduler-jitter window between bg-tick and main-bench threads
//! that the original Phase-1 single-client design tolerated.

use std::collections::VecDeque;
use std::collections::hash_map::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use ed25519_dalek::SigningKey;
use hdrhistogram::Histogram;

use melin_protocol::codec;
use melin_protocol::message::{Request, ResponseKind};
use melin_protocol::session::{ClientHandshake, encode_envelope, verify_and_decode_envelope};
use melin_rumcast::flow_control::FlowControl;
use melin_rumcast::muxed_receiver::{MuxedReceiver, MuxedReceiverConfig};
use melin_rumcast::muxed_sender::{MuxedSender, MuxedSenderConfig};
use melin_rumcast::pub_log::PublicationLog;
use melin_rumcast::shared_udp::{SharedUdp, SharedUdpRecv, SharedUdpSend};
use melin_rumcast::wire::{FrameView, data_flags};

use crate::generator::{GeneratorConfig, OrderFlowGenerator};

// MUST match the constants in `melin-server/src/rumcast_transport.rs`.
const RUMCAST_ORDERS_STREAM: u32 = 1;
const RUMCAST_RESP_STREAM: u32 = 2;
const TERM_LENGTH: u32 = 1024 * 1024;
const MTU: u32 = 1408;
const INITIAL_TERM_ID: u32 = 1;

/// Per-receiver id used in our SMs back to the server. Multi-client
/// uses a single MuxedReceiver, so we still send a single
/// `receiver_id` — the server side disambiguates per-session via
/// `session_id`, not `receiver_id`, and `flow_control = Min` doesn't
/// care about receiver count.
const BENCH_RECEIVER_ID: u64 = 1;

/// Reusable envelope buffer size. Largest inner frame is
/// codec::encode_request output (≤168B) plus 24-byte envelope header.
const ENVELOPE_BUF_SIZE: usize = 2048;

/// Upper bound on a single handshake step. Far longer than any
/// realistic LAN RTT — bails out if the server isn't responding.
const HANDSHAKE_DEADLINE: Duration = Duration::from_secs(10);

pub struct RumcastBenchConfig {
    pub server_addr: SocketAddr,
    pub bind: SocketAddr,
    pub pairs: usize,
    pub window: usize,
    pub warmup: usize,
    /// Number of concurrent rumcast sessions. Each gets its own random
    /// `session_id`, handshake, envelope token, and per-session
    /// inflight deque. Comparable to the TCP path's `--clients`.
    pub clients: usize,
    pub accounts: u32,
    pub instruments: u32,
    pub json_path: Option<PathBuf>,
    /// When `true`, the idle path of the bench loop parks on the
    /// response socket for ~100 µs (`ppoll`) instead of busy-spinning.
    /// Default is `false` (busy-spin) which gives lowest latency on
    /// isolated cores; flip to `true` on shared cores to free CPU at
    /// the cost of an idle-wake upper bound.
    pub yield_idle: bool,
    /// NAPI busy-poll budget in microseconds for the response socket.
    /// `0` disables. See `KernelUdp::set_busy_poll` for the sysctl /
    /// privilege requirements.
    pub busy_poll_us: u32,
    /// Enable `UDP_GRO` on the bench response socket so coalesced
    /// incoming datagrams (from a server using UDP-GSO) get fanned
    /// out as separate logical frames by `recv_batch`.
    pub udp_gro: bool,
    /// Client's long-term Ed25519 identity. The server's
    /// `authorized_keys` file must list this key under a permission
    /// that allows order submission (e.g. `trader`). All N concurrent
    /// sessions reuse the same identity — same as the TCP multi-client
    /// path, where every client connection authenticates as the same
    /// trader pubkey.
    pub signing_key: SigningKey,
    /// CPU core for bench thread pinning. When `Some(c)`, pins the
    /// main hot-loop thread to core `c`. When `None`, unpinned.
    pub bench_core_start: Option<usize>,
}

pub fn run_rumcast_roundtrip(cfg: RumcastBenchConfig) {
    assert!(cfg.clients >= 1, "clients must be >= 1");

    if let Some(core) = cfg.bench_core_start
        && let Err(e) = melin_server::affinity::pin_to_core(core)
    {
        eprintln!("warning: could not pin rumcast bench loop to core {core}: {e}");
    }

    // Each session gets a distinct session_id. Picking a random base
    // and incrementing keeps inter-bench collisions astronomically
    // unlikely while guaranteeing intra-bench uniqueness without
    // collision-retry logic.
    let base_session_id = generate_session_id();
    let session_ids: Vec<u32> = (0..cfg.clients)
        .map(|i| base_session_id.wrapping_add(i as u32))
        .collect();
    let session_idx: HashMap<u32, usize> = session_ids
        .iter()
        .enumerate()
        .map(|(i, sid)| (*sid, i))
        .collect();
    eprintln!(
        "rumcast roundtrip: server={} bind={} clients={} session_id_base={:#010x} pairs={} window={} warmup={}",
        cfg.server_addr, cfg.bind, cfg.clients, base_session_id, cfg.pairs, cfg.window, cfg.warmup
    );

    // ---- Pre-generate frames per client (disjoint order_id ranges) ----
    //
    // Each client gets `(warmup + pairs_per_client * 2)` frames, with
    // `start_order_id` offset so concurrent clients can't submit the
    // same order_id (the engine would reject duplicates as
    // self-trades or ignore them). Mirrors the TCP roundtrip path's
    // partitioning logic in `run_uring_roundtrip`.
    let pairs_per_client = cfg.pairs / cfg.clients;
    let remainder = cfg.pairs % cfg.clients;
    let mut per_client_frames: Vec<Vec<Vec<u8>>> = Vec::with_capacity(cfg.clients);
    let mut order_id_offset: u64 = 0;
    for client_id in 0..cfg.clients {
        let client_pairs = if client_id == cfg.clients - 1 {
            pairs_per_client + remainder
        } else {
            pairs_per_client
        };
        let total_orders = cfg.warmup + client_pairs * 2;
        let mut flow = OrderFlowGenerator::new(GeneratorConfig {
            num_accounts: cfg.accounts.max(1),
            num_instruments: cfg.instruments.max(1),
            start_order_id: order_id_offset + 1,
            ..Default::default()
        });
        per_client_frames.push(flow.generate_frames(total_orders));
        order_id_offset += total_orders as u64;
    }
    let total_msgs: usize = per_client_frames.iter().map(|v| v.len()).sum();
    eprintln!(
        "pre-generated {total_msgs} order frames across {} clients",
        cfg.clients
    );

    // ---- Rumcast endpoints (SharedUdp inline demux) ----
    //
    // One bound kernel socket; `SharedUdp` demultiplexes inbound
    // frames inline in the calling thread's `recv_from` — no
    // background poller, no SPSC ring crossings. Data/Setup/HB
    // route to the recv half; NAK/SM route to the send half.
    // The single hot-loop thread drives everything.
    let endpoint = SharedUdp::bind(cfg.bind).expect("SharedUdp bind");
    // Absorb burst traffic from the server without kernel drops. The default
    // rmem_max (208KB) is far below a 16-client × 128-window burst (~400KB);
    // without this, the kernel drops frames immediately and NAK storms ensue.
    if let Err(e) = endpoint.set_recv_buffer_bytes(32 * 1024 * 1024) {
        eprintln!("warning: could not bump bench socket SO_RCVBUF: {e}");
    }
    // EPERM is the common case (no CAP_NET_ADMIN, sysctl floor too
    // low); fall back rather than refuse to bench.
    if cfg.busy_poll_us > 0
        && let Err(e) = endpoint.set_busy_poll(cfg.busy_poll_us)
    {
        eprintln!(
            "warning: could not enable SO_BUSY_POLL on bench socket ({} us): {e}",
            cfg.busy_poll_us
        );
    }
    // UDP_GRO: ENOPROTOOPT on pre-5.0 kernels; non-fatal.
    if cfg.udp_gro
        && let Err(e) = endpoint.set_udp_gro(true)
    {
        eprintln!(
            "warning: could not enable UDP_GRO on bench socket: {e}; continuing without GRO fan-out"
        );
    }
    let (send_half, recv_half) = endpoint.split();

    let max_sessions = (cfg.clients as u32).saturating_add(4);
    let mut muxed_sender = MuxedSender::new(
        send_half,
        MuxedSenderConfig {
            stream_id: RUMCAST_ORDERS_STREAM,
            initial_term_id: INITIAL_TERM_ID,
            term_length: TERM_LENGTH,
            mtu: MTU,
            setup_interval: Duration::from_millis(100),
            heartbeat_interval: Duration::from_millis(50),
            max_drain_per_tick: 1024 * 1024,
            max_control_per_tick: 32,
            // Min flow control: pace publisher to slowest receiver.
            // For the single-server setup we have one receiver per
            // session, so `Min` and `Max` are equivalent.
            flow_control: FlowControl::Min,
            max_sessions,
        },
    );

    let mut muxed_receiver = MuxedReceiver::new(
        recv_half,
        MuxedReceiverConfig {
            stream_id: RUMCAST_RESP_STREAM,
            receiver_id: BENCH_RECEIVER_ID,
            initial_term_id: INITIAL_TERM_ID,
            term_length: TERM_LENGTH,
            sm_interval: Duration::from_millis(2),
            nak_backoff_min: Duration::from_micros(50),
            nak_backoff_jitter: Duration::from_micros(50),
            max_recv_per_tick: 1024,
            max_sessions,
        },
    );

    // Allocate per-session outbound publogs upfront (the muxer holds
    // its own clone; we keep ours for try_claim on the hot path).
    // SubscriptionLogs on the receive side are auto-allocated lazily
    // on first inbound frame — no upfront call needed.
    let pub_logs: Vec<Arc<PublicationLog>> = session_ids
        .iter()
        .map(|sid| {
            let log = muxed_sender
                .create_session(*sid, cfg.server_addr)
                .expect("create_session");
            // Single-client per session; we trust ourselves to keep
            // the producer ahead of the receiver. Removes the wait-
            // for-first-SM stall during handshake.
            log.set_publisher_limit(u64::MAX);
            log
        })
        .collect();

    // Force initial Setup frames out so the server allocates per-
    // session state ASAP rather than waiting one full setup_interval.
    for sid in &session_ids {
        muxed_sender.send_setup_now(*sid);
    }

    // ---- Per-session handshakes (sequential) ----
    //
    // Each session does the four-message handshake independently. On
    // loopback this takes a few ms per session; sequential keeps the
    // code simple and the muxer's single-thread contract intact.
    let yield_idle = cfg.yield_idle;
    let mut session_tokens: Vec<[u8; 32]> = Vec::with_capacity(cfg.clients);
    let handshake_start = Instant::now();
    for (i, sid) in session_ids.iter().enumerate() {
        let token = perform_handshake(
            &cfg.signing_key,
            *sid,
            &pub_logs[i],
            &mut muxed_sender,
            &mut muxed_receiver,
        );
        session_tokens.push(token);
    }
    eprintln!(
        "handshakes complete ({} sessions, {:.1}ms); entering measured phase",
        cfg.clients,
        handshake_start.elapsed().as_secs_f64() * 1000.0
    );

    // ---- Per-session bench state ----
    // TSC ticks recorded at publish time; popped on matching BatchEnd.
    let mut inflight: Vec<VecDeque<u64>> = (0..cfg.clients)
        .map(|_| VecDeque::with_capacity(cfg.window))
        .collect();
    let mut outbound_seq: Vec<u64> = vec![0; cfg.clients];
    let mut last_inbound_seq: Vec<u64> = vec![0; cfg.clients];
    let mut total_sent: Vec<usize> = vec![0; cfg.clients];
    let mut total_received: Vec<usize> = vec![0; cfg.clients];
    let mut warmup_record: Vec<usize> = vec![0; cfg.clients];
    let per_client_total: Vec<usize> = per_client_frames.iter().map(|v| v.len()).collect();

    let mut hist =
        Histogram::<u64>::new_with_bounds(1, 10_000_000_000, 3).expect("histogram bounds");
    let mut envelope_buf = vec![0u8; ENVELOPE_BUF_SIZE];
    let mut bench_start = Instant::now();
    let mut warmup_done = cfg.warmup == 0;

    // Diagnostic counters (env-gated, dumped to stderr every ~1s).
    // Mirror of the server-side `RUMCAST_DIAG` instrumentation:
    // tells us whether the bench is spinning on `try_claim` (pub_log
    // backpressure → publisher_limit not advancing → server SMs not
    // arriving) vs. starved on inbound (server not responding).
    let diag_enabled = std::env::var("RUMCAST_DIAG")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    // TSC calibration for sub-nanosecond-overhead latency timestamps.
    // Same approach as the TCP bench — avoids clock_gettime on the hot path.
    let ticks_per_ns = crate::calibrate_tsc();
    let mut diag_iters: u64 = 0;
    let mut diag_claim_ok: u64 = 0;
    let mut diag_claim_bp: u64 = 0;
    let mut diag_responses: u64 = 0;
    let mut diag_recv_frags: u64 = 0;
    let mut diag_send_frags: u64 = 0;
    let mut diag_sms_received: u64 = 0;
    let mut diag_naks_received: u64 = 0;
    let mut diag_topup_skipped_sessions: u64 = 0;
    let mut diag_send_errors: u64 = 0;
    let mut diag_partition_misses: u64 = 0;
    let mut diag_last_dump = Instant::now();
    // Per-stage cumulative wall time, ns. Gated on diag_enabled — same
    // shape as the server's session_translator instrumentation.
    let mut diag_recv_tick_ns: u64 = 0;
    let mut diag_topup_ns: u64 = 0;
    let mut diag_send_tick_ns: u64 = 0;
    let mut diag_poll_ns: u64 = 0;

    // ---- Hot loop ----
    //
    // Per iteration: tick muxers, top up each session's outbound
    // window, drain any inbound responses across all sessions. Single
    // thread keeps muxer access lock-free (`MuxedSender::tick` /
    // `MuxedReceiver::tick` need `&mut self`).
    let measured_total = (total_msgs - cfg.warmup * cfg.clients) as u64;
    let progress = Arc::new(AtomicU64::new(0));
    let progress_shutdown = Arc::new(AtomicBool::new(false));
    let progress_handle = crate::spawn_progress_reporter(
        Arc::clone(&progress),
        measured_total,
        Arc::clone(&progress_shutdown),
    );
    let mut total_received_overall = 0usize;
    while total_received_overall < total_msgs {
        diag_iters += 1;
        // Drain inbound first so the top-up below can react to any
        // SMs that just arrived (publisher_limit advancement). The
        // sender's tick lives AFTER top-up so freshly published
        // fragments go out on the wire in the same iteration —
        // otherwise a publish recorded its `inflight` timestamp
        // here and the actual datagram wouldn't leave until the
        // next iter (a 2 ms park sat in between, dominating
        // single-msg round-trip latency).
        let t_recv_start = if diag_enabled {
            Some(Instant::now())
        } else {
            None
        };
        let recv_stats = muxed_receiver.tick();
        let t_recv_end = if diag_enabled {
            Some(Instant::now())
        } else {
            None
        };
        if let (Some(a), Some(b)) = (t_recv_start, t_recv_end) {
            diag_recv_tick_ns += b.duration_since(a).as_nanos() as u64;
        }
        diag_recv_frags += recv_stats.fragments_accepted as u64;

        // Top up each session's outbound window.
        //
        // CRITICAL: try_claim is non-blocking — on backpressure
        // (publisher_limit not advanced because the server hasn't
        // sent SMs yet, or our last SM got dropped), `break` out
        // and let the outer loop re-tick the muxers. The previous
        // version spun forever on Err here, never returning to
        // tick(), so SMs would never be processed and the bench
        // would deadlock the moment a single session's pub_log
        // filled. Same shape as the server-side spin_publish bug
        // we already fixed.
        for s in 0..cfg.clients {
            let target = per_client_total[s];
            while inflight[s].len() < cfg.window && total_sent[s] < target {
                let inner = &per_client_frames[s][total_sent[s]];
                outbound_seq[s] += 1;
                let env_len = encode_envelope(
                    &session_tokens[s],
                    session_ids[s],
                    outbound_seq[s],
                    inner,
                    &mut envelope_buf,
                )
                .expect("envelope buf large enough");
                match pub_logs[s].try_claim(env_len as u32) {
                    Ok(mut claim) => {
                        claim
                            .payload_mut()
                            .copy_from_slice(&envelope_buf[..env_len]);
                        claim.publish(data_flags::UNFRAGMENTED);
                        diag_claim_ok += 1;
                        // We bumped outbound_seq above. Since the
                        // publish succeeded, keep the new seq —
                        // sequence integrity preserved.
                        inflight[s].push_back(crate::rdtscp());
                        total_sent[s] += 1;
                    }
                    Err(_) => {
                        // Roll back the seq bump so we don't gap
                        // the receiver when we retry next iter.
                        outbound_seq[s] -= 1;
                        diag_claim_bp += 1;
                        diag_topup_skipped_sessions += 1;
                        break;
                    }
                }
            }
        }

        // Flush freshly published fragments + process control
        // (NAKs / SMs) immediately. Was at the top of the loop body;
        // moved after top-up so a publish→wire round-trip happens in
        // a single iteration.
        let t_topup_end = if diag_enabled {
            Some(Instant::now())
        } else {
            None
        };
        if let (Some(a), Some(b)) = (t_recv_end, t_topup_end) {
            diag_topup_ns += b.duration_since(a).as_nanos() as u64;
        }
        let send_stats = muxed_sender.tick();
        let t_send_end = if diag_enabled {
            Some(Instant::now())
        } else {
            None
        };
        if let (Some(a), Some(b)) = (t_topup_end, t_send_end) {
            diag_send_tick_ns += b.duration_since(a).as_nanos() as u64;
        }
        diag_send_frags += send_stats.fragments_sent as u64;
        diag_naks_received += send_stats.naks_received as u64;
        diag_sms_received += send_stats.sms_received as u64;
        diag_send_errors += send_stats.send_errors as u64;
        diag_partition_misses += send_stats.partition_misses as u64;

        // Drain responses for all sessions in one poll pass. The poll
        // callback routes by `session_id` and updates per-session
        // inflight state.
        let mut drained_now = 0usize;
        muxed_receiver.poll(64 * 1024, |sid, _src, view| {
            if let FrameView::Data { header, payload } = view
                && header.common.flags & data_flags::PADDING == 0
            {
                let s = match session_idx.get(&sid) {
                    Some(i) => *i,
                    None => return, // stray frame from a foreign session
                };
                let (seq, decoded_inner) = match verify_and_decode_envelope(
                    &session_tokens[s],
                    sid,
                    last_inbound_seq[s],
                    payload,
                ) {
                    Ok(x) => x,
                    Err(e) => {
                        eprintln!("envelope verify failed (session {sid:#010x}): {e:?}");
                        return;
                    }
                };
                last_inbound_seq[s] = seq;

                let kind = match codec::decode_response(decoded_inner) {
                    Ok(k) => k,
                    Err(e) => {
                        eprintln!("decode_response (session {sid:#010x}): {e:?}");
                        return;
                    }
                };
                if matches!(kind, ResponseKind::BatchEnd)
                    && let Some(sent_tsc) = inflight[s].pop_front()
                {
                    let latency_ns = crate::tsc_to_ns(crate::rdtscp() - sent_tsc, ticks_per_ns);
                    if total_received[s] >= cfg.warmup {
                        let _ = hist.record(latency_ns);
                        progress.fetch_add(1, Ordering::Relaxed);
                    } else {
                        warmup_record[s] += 1;
                    }
                    total_received[s] += 1;
                    drained_now += 1;
                }
            }
        });
        let t_poll_end = if diag_enabled {
            Some(Instant::now())
        } else {
            None
        };
        if let (Some(a), Some(b)) = (t_send_end, t_poll_end) {
            diag_poll_ns += b.duration_since(a).as_nanos() as u64;
        }
        total_received_overall += drained_now;
        diag_responses += drained_now as u64;

        if diag_enabled {
            let now = t_poll_end.unwrap();
            if now.duration_since(diag_last_dump) >= Duration::from_secs(1) {
                let inflight_max = inflight.iter().map(|q| q.len()).max().unwrap_or(0);
                let inflight_sum: usize = inflight.iter().map(|q| q.len()).sum();
                let sent_sum: usize = total_sent.iter().sum();
                let recv_sum: usize = total_received.iter().sum();
                let to_ms = |ns: u64| ns as f64 / 1_000_000.0;
                eprintln!(
                    "[bench-diag] iters={} \
                     recv_ms={:.1} topup_ms={:.1} send_ms={:.1} poll_ms={:.1} \
                     claim_ok={} claim_bp={} topup_skipped={} \
                     responses={} recv_frags={} send_frags={} send_errors={} \
                     partition_misses={} sms_recv={} naks_recv={} \
                     inflight_max={} inflight_sum={} sent={} recv={}",
                    diag_iters,
                    to_ms(diag_recv_tick_ns),
                    to_ms(diag_topup_ns),
                    to_ms(diag_send_tick_ns),
                    to_ms(diag_poll_ns),
                    diag_claim_ok,
                    diag_claim_bp,
                    diag_topup_skipped_sessions,
                    diag_responses,
                    diag_recv_frags,
                    diag_send_frags,
                    diag_send_errors,
                    diag_partition_misses,
                    diag_sms_received,
                    diag_naks_received,
                    inflight_max,
                    inflight_sum,
                    sent_sum,
                    recv_sum,
                );
                diag_iters = 0;
                diag_claim_ok = 0;
                diag_claim_bp = 0;
                diag_responses = 0;
                diag_recv_frags = 0;
                diag_send_frags = 0;
                diag_send_errors = 0;
                diag_partition_misses = 0;
                diag_sms_received = 0;
                diag_naks_received = 0;
                diag_topup_skipped_sessions = 0;
                diag_recv_tick_ns = 0;
                diag_topup_ns = 0;
                diag_send_tick_ns = 0;
                diag_poll_ns = 0;
                diag_last_dump = now;
            }
        }

        // Reset bench_start once every session has drained its warmup
        // quota. Until then we record into a holding histogram that
        // gets discarded.
        if !warmup_done && warmup_record.iter().all(|&n| n >= cfg.warmup) {
            bench_start = Instant::now();
            hist.reset();
            warmup_done = true;
            eprintln!("warmup complete, starting measured phase");
        }

        if drained_now == 0 {
            if yield_idle {
                // 100 µs (ppoll) cap: small enough not to dominate
                // single-message tail latency, large enough to free
                // the CPU between iterations on a shared host. The
                // socket wakes on POLLIN before the timeout so
                // typical RTT is unaffected.
                muxed_receiver.park(Duration::from_micros(100));
            } else {
                std::hint::spin_loop();
            }
        }
    }

    // Snapshot elapsed BEFORE joining the progress thread: that thread sleeps
    // in 5-second increments and only checks shutdown after each sleep, so
    // join() can block up to ~5s and would otherwise pollute `elapsed` —
    // turning a 200ms bench into a 5s "elapsed" reading.
    let elapsed = bench_start.elapsed();
    progress_shutdown.store(true, Ordering::Relaxed);
    let _ = progress_handle.join();
    let measured = total_msgs - cfg.warmup * cfg.clients;
    println!();
    println!(
        "=== rumcast roundtrip (clients={}, {} measured msgs) ===",
        cfg.clients, measured
    );
    if let Some(core) = cfg.bench_core_start {
        println!("  Bench core:  {} (hot-loop)", core);
    } else {
        println!("  Bench core:  unpinned");
    }
    println!("  elapsed:    {:?}", elapsed);
    println!(
        "  throughput: {:.2} K msgs/sec",
        (measured as f64 / elapsed.as_secs_f64()) / 1_000.0
    );
    println!("  Latency");
    crate::print_latency_histogram(&hist, measured);

    if let Some(path) = cfg.json_path.as_ref() {
        let json = serde_json::json!({
            "transport": "rumcast",
            "clients": cfg.clients,
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
}

/// Pick a fresh 32-bit `session_id` via the OS CSPRNG. The bench
/// allocates `clients` IDs by incrementing from this base.
fn generate_session_id() -> u32 {
    let mut bytes = [0u8; 4];
    getrandom::fill(&mut bytes).expect("getrandom for session_id");
    u32::from_le_bytes(bytes)
}

/// Run the four-message handshake for one session over the muxer.
///
/// Mirrors the single-session helper but operates through the muxed
/// primitives so we don't have to expose per-session
/// `SubscriptionLog` Arcs from the receiver. Drives ticks inline.
///
/// Panics on protocol error or timeout — the bench is a benchmark
/// tool, not a production client; if any single session can't
/// authenticate there's nothing useful to measure.
fn perform_handshake(
    signing_key: &SigningKey,
    session_id: u32,
    pub_log: &PublicationLog,
    muxed_sender: &mut MuxedSender<SharedUdpSend>,
    muxed_receiver: &mut MuxedReceiver<SharedUdpRecv>,
) -> [u8; 32] {
    let mut x25519_secret_bytes = [0u8; 32];
    getrandom::fill(&mut x25519_secret_bytes).expect("getrandom for X25519 ephemeral");

    let handshake = ClientHandshake::new(signing_key, x25519_secret_bytes);

    // Step 1: Heartbeat kickoff.
    let mut buf = vec![0u8; 256];
    let written =
        codec::encode_request(&Request::Heartbeat, 0, &mut buf).expect("encode Heartbeat");
    publish_blocking(pub_log, &buf[4..written]);

    // Step 2: receive Challenge for this session.
    let challenge_payload = recv_match(
        muxed_receiver,
        muxed_sender,
        session_id,
        Instant::now() + HANDSHAKE_DEADLINE,
        |bytes| {
            matches!(
                codec::decode_response(bytes),
                Ok(ResponseKind::Challenge { .. })
            )
        },
    )
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
    publish_blocking(pub_log, &buf[4..written]);

    // Step 4: wait for ServerReady (or AuthFailed).
    let server_ready_or_failed = recv_match(
        muxed_receiver,
        muxed_sender,
        session_id,
        Instant::now() + HANDSHAKE_DEADLINE,
        |bytes| {
            matches!(
                codec::decode_response(bytes),
                Ok(ResponseKind::ServerReady) | Ok(ResponseKind::AuthFailed)
            )
        },
    )
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

/// Spin-claim and publish a single-fragment payload via a per-session
/// publog. Used for the four handshake frames where backpressure is
/// rare. The hot path uses inline `try_claim` to avoid the function-
/// call indirection.
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

/// Drive the muxers and poll the per-session subscription log until a
/// Data fragment for `target_sid` passes the supplied predicate or
/// `deadline` expires. Used for the four handshake replies — once we
/// switch to envelope-wrapped traffic, the bench loop polls inline.
fn recv_match(
    muxed_receiver: &mut MuxedReceiver<SharedUdpRecv>,
    muxed_sender: &mut MuxedSender<SharedUdpSend>,
    target_sid: u32,
    deadline: Instant,
    predicate: impl Fn(&[u8]) -> bool,
) -> Option<Vec<u8>> {
    while Instant::now() < deadline {
        muxed_sender.tick();
        muxed_receiver.tick();
        let mut found: Option<Vec<u8>> = None;
        muxed_receiver.poll(64 * 1024, |sid, _src, view| {
            if found.is_some() || sid != target_sid {
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
        std::thread::sleep(Duration::from_millis(2));
    }
    None
}
