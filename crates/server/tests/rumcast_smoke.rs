//! Smoke test for the rumcast standalone server. Spawns
//! `run_rumcast` in a thread, then walks the full pure-UDP
//! handshake (Heartbeat → Challenge → ChallengeResponse →
//! ServerReady) and submits a single envelope-wrapped order to
//! verify the order → engine → response path round-trips end to
//! end over the rumcast wire format with auth enabled.
//!
//! Only compiled / run when the `rumcast` feature is enabled. Run
//! with: `cargo test -p melin-server --features rumcast --test
//! rumcast_smoke -- --nocapture`.

#![cfg(feature = "rumcast")]

use std::io::Write;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};
use std::num::NonZeroU64;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use base64::Engine;
use ed25519_dalek::SigningKey;
use melin_protocol::codec;
use melin_protocol::message::{Request, ResponseKind};
use melin_protocol::session::{
    ClientHandshake, ENVELOPE_OVERHEAD, encode_envelope, verify_and_decode_envelope,
};
use melin_rumcast::pub_log::{PublicationConfig, PublicationLog};
use melin_rumcast::receiver::{ReceiverConfig, ReceiverLoop};
use melin_rumcast::sender::{SenderConfig, SenderLoop};
use melin_rumcast::shared_udp::SharedUdp;
use melin_rumcast::sub_log::{SubscriptionConfig, SubscriptionLog};
use melin_rumcast::wire::{FrameView, data_flags};
use melin_server::rumcast_transport::{RumcastConfig, run_rumcast};
use melin_server::server::ServerConfig;
use melin_trading::types::{
    AccountId, Order, OrderId, OrderType, Price, Quantity, SelfTradeProtection, Side, Symbol,
    TimeInForce,
};

// MUST match the wire-format constants in melin-server and
// melin-bench. `session_id` is NOT a constant — each test run picks
// a fresh random 32-bit value (Aeron convention).
const RUMCAST_ORDERS_STREAM: u32 = 1;
const RUMCAST_RESP_STREAM: u32 = 2;
const TERM_LENGTH: u32 = 1024 * 1024;
const MTU: u32 = 1408;
const INITIAL_TERM_ID: u32 = 1;
const BENCH_RECEIVER_ID: u64 = 1;

/// Pick a fresh 32-bit `session_id` for this test client. Tests use
/// random IDs both to exercise the per-session demux path and to
/// avoid the `0xCAFEBABE` collision when running tests in parallel
/// against the same loopback range.
fn generate_session_id() -> u32 {
    let mut bytes = [0u8; 4];
    getrandom::fill(&mut bytes).expect("getrandom");
    u32::from_le_bytes(bytes)
}

/// Find an unused UDP port by binding ephemeral and dropping.
fn free_udp_port() -> u16 {
    let s = UdpSocket::bind("127.0.0.1:0").unwrap();
    s.local_addr().unwrap().port()
}

fn loopback(port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
}

/// Write an `authorized_keys` file granting trader permission to
/// each of the given Ed25519 verifying keys. Returns the file path.
fn write_authorized_keys(dir: &std::path::Path, keys: &[&SigningKey]) -> PathBuf {
    let path = dir.join("authorized_keys");
    let mut content = String::new();
    for (i, key) in keys.iter().enumerate() {
        let pub_b64 =
            base64::engine::general_purpose::STANDARD.encode(key.verifying_key().to_bytes());
        content.push_str(&format!("trader {pub_b64} rumcast-smoke-test-{i}\n"));
    }
    let mut f = std::fs::File::create(&path).expect("create authorized_keys");
    f.write_all(content.as_bytes()).expect("write keys");
    path
}

/// Spin-claim and publish raw bytes onto a rumcast publication.
/// Used both for unwrapped handshake messages and envelope-wrapped
/// data-plane traffic.
fn rumcast_publish(pub_log: &PublicationLog, payload: &[u8]) {
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
/// payload bytes.
fn rumcast_recv_match(
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

#[test]
fn rumcast_order_round_trip() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_test_writer()
        .try_init();

    let server_port = free_udp_port();
    let server_addr = loopback(server_port);

    // Temp directory for the journal + authorized_keys — destroyed
    // when `_tmp` drops.
    let _tmp = tempfile::tempdir().unwrap();
    let journal_path = _tmp.path().join("test.journal");

    // Client identity: deterministic key from a fixed seed keeps
    // the test's auth path reproducible. Session_id is random per
    // run — the protocol uses it as a connection identifier, not
    // an auth-bound value, so randomness here doesn't affect
    // determinism of the path under test.
    let client_key = SigningKey::from_bytes(&[0xAB; 32]);
    let authorized_keys_path = write_authorized_keys(_tmp.path(), &[&client_key]);
    let session_id = generate_session_id();

    // ---- Server config ----
    let server_config = ServerConfig {
        bind: server_addr,
        journal: journal_path.clone(),
        accounts: 4,
        instruments: 4,
        authorized_keys: authorized_keys_path,
        ..ServerConfig::default()
    };

    // ---- Spawn server thread ----
    let shutdown = Arc::new(AtomicBool::new(false));
    let server_shutdown = Arc::clone(&shutdown);
    let server_handle = thread::Builder::new()
        .name("test-rumcast-server".into())
        .spawn(move || {
            run_rumcast(
                server_config,
                RumcastConfig { bind: server_addr },
                server_shutdown,
            )
            .map_err(|e| e.to_string())
        })
        .unwrap();

    // Server takes a moment to seed_and_drain + bind. Sleep
    // generously — the journal create + first fsync can take tens
    // of ms on some filesystems.
    thread::sleep(Duration::from_millis(500));

    // ---- Client-side rumcast endpoints (single shared socket) ----
    //
    // SharedUdp gives us one bound port with two `UdpTransport`
    // halves. The orders Sender uses the send half (its outbound
    // packets carry this socket's port as the source addr — which
    // becomes the server's auto-discovered `effective_dst`, so
    // responses land back here). The resp Receiver uses the recv
    // half. Internal demux routes Data/Setup/Heartbeat to the recv
    // half, NAK/StatusMessage to the send half.
    let shared = SharedUdp::bind(loopback(0)).unwrap();
    let (send_half, recv_half) = shared.split();

    let orders_pub = Arc::new(
        PublicationLog::new(PublicationConfig {
            session_id,
            stream_id: RUMCAST_ORDERS_STREAM,
            initial_term_id: INITIAL_TERM_ID,
            term_length: TERM_LENGTH,
            mtu: MTU,
        })
        .unwrap(),
    );
    orders_pub.set_publisher_limit(u64::MAX);
    let mut orders_send_config = SenderConfig::defaults(server_addr);
    orders_send_config.setup_interval = Duration::from_millis(50);
    orders_send_config.heartbeat_interval = Duration::from_millis(25);
    let mut orders_sender = SenderLoop::new(Arc::clone(&orders_pub), send_half, orders_send_config);

    let resp_sub = Arc::new(
        SubscriptionLog::new(SubscriptionConfig {
            session_id,
            stream_id: RUMCAST_RESP_STREAM,
            initial_term_id: INITIAL_TERM_ID,
            term_length: TERM_LENGTH,
        })
        .unwrap(),
    );
    let mut resp_recv_config = ReceiverConfig::defaults(server_addr, BENCH_RECEIVER_ID);
    resp_recv_config.sm_interval = Duration::from_millis(50);
    let mut resp_receiver = ReceiverLoop::new(Arc::clone(&resp_sub), recv_half, resp_recv_config);

    // ---- Tick threads ----
    let tick_shutdown = Arc::new(AtomicBool::new(false));
    let send_tick = {
        let s = Arc::clone(&tick_shutdown);
        thread::spawn(move || {
            while !s.load(Ordering::Acquire) {
                let _ = orders_sender.tick();
                thread::sleep(Duration::from_micros(50));
            }
        })
    };
    let recv_tick = {
        let s = Arc::clone(&tick_shutdown);
        thread::spawn(move || {
            while !s.load(Ordering::Acquire) {
                let _ = resp_receiver.tick();
                thread::sleep(Duration::from_micros(50));
            }
        })
    };

    // ---- Handshake ----
    //
    // Step 1: send Heartbeat to kick off the server's state
    // machine.
    let mut buf = vec![0u8; 256];
    let written = codec::encode_request(&Request::Heartbeat, 0, &mut buf).unwrap();
    rumcast_publish(&orders_pub, &buf[4..written]);

    // Step 2: receive Challenge.
    let challenge_bytes = rumcast_recv_match(
        &resp_sub,
        Instant::now() + Duration::from_secs(5),
        |payload| {
            matches!(
                codec::decode_response(payload),
                Ok(ResponseKind::Challenge { .. })
            )
        },
    )
    .expect("Challenge from server");
    let (server_nonce, server_eph) = match codec::decode_response(&challenge_bytes).unwrap() {
        ResponseKind::Challenge {
            nonce,
            server_x25519_eph,
        } => (nonce, server_x25519_eph),
        other => panic!("expected Challenge, got {other:?}"),
    };

    // Step 3: complete handshake → ChallengeResponse + token.
    // Deterministic X25519 ephemeral so the test is reproducible.
    let handshake = ClientHandshake::new(&client_key, [0xCD; 32]);
    let completed = handshake.finish(&server_nonce, &server_eph);
    let session_token = completed.session_token;

    let written = codec::encode_request(&completed.challenge_response, 0, &mut buf).unwrap();
    rumcast_publish(&orders_pub, &buf[4..written]);

    // Step 4: wait for ServerReady.
    rumcast_recv_match(
        &resp_sub,
        Instant::now() + Duration::from_secs(5),
        |payload| {
            matches!(
                codec::decode_response(payload),
                Ok(ResponseKind::ServerReady)
            )
        },
    )
    .expect("ServerReady from server");

    // ---- Submit one order, envelope-wrapped ----
    //
    // Use account 1, symbol 0 (within the seeded set).
    let order = Order {
        id: OrderId(1),
        account: AccountId(1),
        side: Side::Buy,
        order_type: OrderType::Limit {
            price: Price(NonZeroU64::new(100).unwrap()),
            post_only: false,
        },
        time_in_force: TimeInForce::GTC,
        quantity: Quantity(NonZeroU64::new(10).unwrap()),
        stp: SelfTradeProtection::Allow,
        expiry_ns: 0,
    };
    let request = Request::SubmitOrder {
        symbol: Symbol(0),
        order,
    };
    let written = codec::encode_request(&request, /* seq */ 1, &mut buf).unwrap();
    let inner = &buf[4..written];

    let mut envelope = vec![0u8; ENVELOPE_OVERHEAD + inner.len()];
    encode_envelope(
        &session_token,
        session_id,
        /* seq */ 1,
        inner,
        &mut envelope,
    )
    .unwrap();
    rumcast_publish(&orders_pub, &envelope);

    // ---- Wait for envelope-wrapped BatchEnd response ----
    //
    // The server wraps every authenticated response in a fresh
    // envelope under the same token; client-side replay-state
    // tracker (last_inbound_seq) starts at 0 and advances on each
    // accepted frame.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut last_inbound_seq: u64 = 0;
    let mut got_batch_end = false;
    while Instant::now() < deadline && !got_batch_end {
        resp_sub.poll(64 * 1024, |view| {
            if let FrameView::Data { header, payload } = view
                && header.common.flags & data_flags::PADDING == 0
            {
                match verify_and_decode_envelope(
                    &session_token,
                    session_id,
                    last_inbound_seq,
                    payload,
                ) {
                    Ok((seq, decoded_inner)) => {
                        last_inbound_seq = seq;
                        if let Ok(ResponseKind::BatchEnd) = codec::decode_response(decoded_inner) {
                            got_batch_end = true;
                        }
                    }
                    Err(_) => {
                        // Could be a non-envelope frame (in
                        // practice everything post-auth IS an
                        // envelope, but an out-of-band Setup or
                        // Heartbeat frame sneaking through
                        // wouldn't decode either). Ignore.
                    }
                }
            }
        });
        thread::sleep(Duration::from_millis(5));
    }

    // ---- Cleanup ----
    tick_shutdown.store(true, Ordering::Release);
    let _ = send_tick.join();
    let _ = recv_tick.join();
    shutdown.store(true, Ordering::Release);
    let server_join_deadline = Instant::now() + Duration::from_secs(2);
    while !server_handle.is_finished() && Instant::now() < server_join_deadline {
        thread::sleep(Duration::from_millis(50));
    }
    if server_handle.is_finished() {
        let _ = server_handle.join();
    }

    assert!(
        got_batch_end,
        "did not receive envelope-wrapped BatchEnd within 5s — \
         server didn't roundtrip the order through the rumcast auth path"
    );
}

// ---------------------------------------------------------------------------
// Multi-client end-to-end
// ---------------------------------------------------------------------------

/// Run one client's full auth + order + BatchEnd-wait flow against
/// an already-running server. Returns `true` iff the client received
/// its own envelope-wrapped `BatchEnd` within the deadline.
///
/// Each call sets up its own `SharedUdp` (single bound socket per
/// client — required for the server's auto-discovered per-session
/// dst to land back at this client's subscriber), its own
/// `PublicationLog` keyed by `session_id`, and its own tick
/// threads. Multiple `run_one_client` invocations on different
/// session_ids run independently.
#[allow(clippy::too_many_arguments)]
fn run_one_client(
    server_addr: SocketAddr,
    signing_key: SigningKey,
    session_id: u32,
    x25519_seed: [u8; 32],
    order_id: u64,
    account_id: u32,
) -> bool {
    let shared = SharedUdp::bind(loopback(0)).unwrap();
    let (send_half, recv_half) = shared.split();

    let orders_pub = Arc::new(
        PublicationLog::new(PublicationConfig {
            session_id,
            stream_id: RUMCAST_ORDERS_STREAM,
            initial_term_id: INITIAL_TERM_ID,
            term_length: TERM_LENGTH,
            mtu: MTU,
        })
        .unwrap(),
    );
    orders_pub.set_publisher_limit(u64::MAX);
    let mut orders_send_config = SenderConfig::defaults(server_addr);
    orders_send_config.setup_interval = Duration::from_millis(50);
    orders_send_config.heartbeat_interval = Duration::from_millis(25);
    let mut orders_sender = SenderLoop::new(Arc::clone(&orders_pub), send_half, orders_send_config);

    let resp_sub = Arc::new(
        SubscriptionLog::new(SubscriptionConfig {
            session_id,
            stream_id: RUMCAST_RESP_STREAM,
            initial_term_id: INITIAL_TERM_ID,
            term_length: TERM_LENGTH,
        })
        .unwrap(),
    );
    let mut resp_recv_config = ReceiverConfig::defaults(server_addr, BENCH_RECEIVER_ID);
    resp_recv_config.sm_interval = Duration::from_millis(50);
    let mut resp_receiver = ReceiverLoop::new(Arc::clone(&resp_sub), recv_half, resp_recv_config);

    let tick_shutdown = Arc::new(AtomicBool::new(false));
    let send_tick = {
        let s = Arc::clone(&tick_shutdown);
        thread::spawn(move || {
            while !s.load(Ordering::Acquire) {
                let _ = orders_sender.tick();
                thread::sleep(Duration::from_micros(50));
            }
        })
    };
    let recv_tick = {
        let s = Arc::clone(&tick_shutdown);
        thread::spawn(move || {
            while !s.load(Ordering::Acquire) {
                let _ = resp_receiver.tick();
                thread::sleep(Duration::from_micros(50));
            }
        })
    };

    // Handshake.
    let mut buf = vec![0u8; 256];
    let written = codec::encode_request(&Request::Heartbeat, 0, &mut buf).unwrap();
    rumcast_publish(&orders_pub, &buf[4..written]);

    let challenge_bytes = rumcast_recv_match(
        &resp_sub,
        Instant::now() + Duration::from_secs(5),
        |payload| {
            matches!(
                codec::decode_response(payload),
                Ok(ResponseKind::Challenge { .. })
            )
        },
    )
    .expect("Challenge from server");
    let (server_nonce, server_eph) = match codec::decode_response(&challenge_bytes).unwrap() {
        ResponseKind::Challenge {
            nonce,
            server_x25519_eph,
        } => (nonce, server_x25519_eph),
        other => panic!("expected Challenge, got {other:?}"),
    };

    let handshake = ClientHandshake::new(&signing_key, x25519_seed);
    let completed = handshake.finish(&server_nonce, &server_eph);
    let session_token = completed.session_token;

    let written = codec::encode_request(&completed.challenge_response, 0, &mut buf).unwrap();
    rumcast_publish(&orders_pub, &buf[4..written]);

    rumcast_recv_match(
        &resp_sub,
        Instant::now() + Duration::from_secs(5),
        |payload| {
            matches!(
                codec::decode_response(payload),
                Ok(ResponseKind::ServerReady)
            )
        },
    )
    .expect("ServerReady from server");

    // Submit one envelope-wrapped order.
    let order = Order {
        id: OrderId(order_id),
        account: AccountId(account_id),
        side: Side::Buy,
        order_type: OrderType::Limit {
            price: Price(NonZeroU64::new(100).unwrap()),
            post_only: false,
        },
        time_in_force: TimeInForce::GTC,
        quantity: Quantity(NonZeroU64::new(10).unwrap()),
        stp: SelfTradeProtection::Allow,
        expiry_ns: 0,
    };
    let request = Request::SubmitOrder {
        symbol: Symbol(0),
        order,
    };
    let written = codec::encode_request(&request, /*seq*/ 1, &mut buf).unwrap();
    let inner = &buf[4..written];
    let mut envelope = vec![0u8; ENVELOPE_OVERHEAD + inner.len()];
    encode_envelope(
        &session_token,
        session_id,
        /*seq*/ 1,
        inner,
        &mut envelope,
    )
    .unwrap();
    rumcast_publish(&orders_pub, &envelope);

    // Wait for envelope-wrapped BatchEnd.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut last_inbound_seq: u64 = 0;
    let mut got_batch_end = false;
    while Instant::now() < deadline && !got_batch_end {
        resp_sub.poll(64 * 1024, |view| {
            if let FrameView::Data { header, payload } = view
                && header.common.flags & data_flags::PADDING == 0
                && let Ok((seq, decoded_inner)) = verify_and_decode_envelope(
                    &session_token,
                    session_id,
                    last_inbound_seq,
                    payload,
                )
            {
                last_inbound_seq = seq;
                if let Ok(ResponseKind::BatchEnd) = codec::decode_response(decoded_inner) {
                    got_batch_end = true;
                }
            }
        });
        thread::sleep(Duration::from_millis(5));
    }

    tick_shutdown.store(true, Ordering::Release);
    let _ = send_tick.join();
    let _ = recv_tick.join();

    got_batch_end
}

#[test]
fn rumcast_two_clients_concurrent() {
    // Two clients connect concurrently with distinct session_ids
    // and distinct Ed25519 identities. Each completes its own
    // handshake, submits an order, and must receive its own
    // envelope-wrapped BatchEnd. If the server's per-session
    // routing leaks (e.g. responses going to the wrong client),
    // one of them silently fails.
    //
    // Pre-#33 (single static `client_addr`) this test would silently
    // fail for the second client. With #32 (SharedUdp) and #33
    // (auto-discovered per-session dst), it should pass.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_test_writer()
        .try_init();

    let server_port = free_udp_port();
    let server_addr = loopback(server_port);

    let _tmp = tempfile::tempdir().unwrap();
    let journal_path = _tmp.path().join("test.journal");

    // Two distinct deterministic keys.
    let key_a = SigningKey::from_bytes(&[0xA1; 32]);
    let key_b = SigningKey::from_bytes(&[0xB2; 32]);
    let authorized_keys_path = write_authorized_keys(_tmp.path(), &[&key_a, &key_b]);

    let server_config = ServerConfig {
        bind: server_addr,
        journal: journal_path.clone(),
        accounts: 4,
        instruments: 4,
        authorized_keys: authorized_keys_path,
        ..ServerConfig::default()
    };

    let shutdown = Arc::new(AtomicBool::new(false));
    let server_shutdown = Arc::clone(&shutdown);
    let server_handle = thread::Builder::new()
        .name("test-rumcast-server-multi".into())
        .spawn(move || {
            run_rumcast(
                server_config,
                RumcastConfig { bind: server_addr },
                server_shutdown,
            )
            .map_err(|e| e.to_string())
        })
        .unwrap();

    thread::sleep(Duration::from_millis(500));

    // Each client picks its own random session_id (32-bit, OS
    // CSPRNG). We assert they're distinct after generation —
    // collision probability is ~1/2^32 per pair, but if it
    // happens the test would silently succeed with one client
    // rather than fail loud, so we re-roll.
    let session_a = generate_session_id();
    let mut session_b = generate_session_id();
    while session_a == session_b {
        session_b = generate_session_id();
    }

    // Spawn one thread per client. Each uses a distinct random
    // session_id, signing key, X25519 seed, order_id, and
    // account_id so cross-routing leakage manifests as a
    // wrong-client BatchEnd or no BatchEnd at all.
    let client_a = thread::spawn(move || {
        run_one_client(
            server_addr,
            key_a,
            session_a,
            [0xC1; 32],
            /*order_id*/ 1,
            /*account_id*/ 1,
        )
    });
    let client_b = thread::spawn(move || {
        run_one_client(
            server_addr,
            key_b,
            session_b,
            [0xC2; 32],
            /*order_id*/ 2,
            /*account_id*/ 2,
        )
    });

    let got_a = client_a.join().expect("client A panicked");
    let got_b = client_b.join().expect("client B panicked");

    // Cleanup.
    shutdown.store(true, Ordering::Release);
    let server_join_deadline = Instant::now() + Duration::from_secs(2);
    while !server_handle.is_finished() && Instant::now() < server_join_deadline {
        thread::sleep(Duration::from_millis(50));
    }
    if server_handle.is_finished() {
        let _ = server_handle.join();
    }

    assert!(
        got_a,
        "client A did not receive its BatchEnd — multi-client routing broken on the A side"
    );
    assert!(
        got_b,
        "client B did not receive its BatchEnd — multi-client routing broken on the B side"
    );
}
