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
use melin_rumcast::sub_log::{SubscriptionConfig, SubscriptionLog};
use melin_rumcast::transport::KernelUdp;
use melin_rumcast::wire::{FrameView, data_flags};
use melin_server::rumcast_transport::{RumcastConfig, run_rumcast};
use melin_server::server::ServerConfig;
use melin_trading::types::{
    AccountId, Order, OrderId, OrderType, Price, Quantity, SelfTradeProtection, Side, Symbol,
    TimeInForce,
};

// MUST match the constants in melin-server's rumcast_transport.rs and
// melin-bench's rumcast.rs. Mismatch = silent no-traffic.
const RUMCAST_SESSION_ID: u32 = 0xCAFEBABE;
const RUMCAST_ORDERS_STREAM: u32 = 1;
const RUMCAST_RESP_STREAM: u32 = 2;
const TERM_LENGTH: u32 = 16 * 1024 * 1024;
const MTU: u32 = 1408;
const INITIAL_TERM_ID: u32 = 1;
const BENCH_RECEIVER_ID: u64 = 1;

/// Find an unused UDP port by binding ephemeral and dropping.
fn free_udp_port() -> u16 {
    let s = UdpSocket::bind("127.0.0.1:0").unwrap();
    s.local_addr().unwrap().port()
}

fn loopback(port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
}

/// Write an `authorized_keys` file granting trader permission to
/// the given Ed25519 verifying key. Returns the file path.
fn write_authorized_keys(dir: &std::path::Path, key: &SigningKey) -> PathBuf {
    let pub_b64 = base64::engine::general_purpose::STANDARD.encode(key.verifying_key().to_bytes());
    let path = dir.join("authorized_keys");
    let content = format!("trader {pub_b64} rumcast-smoke-test\n");
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
    let bench_resp_port = free_udp_port();
    let server_addr = loopback(server_port);
    let bench_addr = loopback(bench_resp_port);

    // Temp directory for the journal + authorized_keys — destroyed
    // when `_tmp` drops.
    let _tmp = tempfile::tempdir().unwrap();
    let journal_path = _tmp.path().join("test.journal");

    // Client identity: deterministic key from a fixed seed makes the
    // test reproducible and avoids needing a CSPRNG dep here.
    let client_key = SigningKey::from_bytes(&[0xAB; 32]);
    let authorized_keys_path = write_authorized_keys(_tmp.path(), &client_key);

    // ---- Server config ----
    let server_config = ServerConfig {
        bind: server_addr,
        journal: journal_path.clone(),
        accounts: 4,
        instruments: 4,
        rumcast_client_addr: Some(bench_addr),
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
                RumcastConfig {
                    bind: server_addr,
                    client_addr: bench_addr,
                },
                server_shutdown,
            )
            .map_err(|e| e.to_string())
        })
        .unwrap();

    // Server takes a moment to seed_and_drain + bind. Sleep
    // generously — the journal create + first fsync can take tens
    // of ms on some filesystems.
    thread::sleep(Duration::from_millis(500));

    // ---- Client-side rumcast endpoints ----
    let orders_pub = Arc::new(
        PublicationLog::new(PublicationConfig {
            session_id: RUMCAST_SESSION_ID,
            stream_id: RUMCAST_ORDERS_STREAM,
            initial_term_id: INITIAL_TERM_ID,
            term_length: TERM_LENGTH,
            mtu: MTU,
        })
        .unwrap(),
    );
    orders_pub.set_publisher_limit(u64::MAX);
    let orders_socket = KernelUdp::bind(loopback(0)).unwrap();
    let mut orders_send_config = SenderConfig::defaults(server_addr);
    orders_send_config.setup_interval = Duration::from_millis(50);
    orders_send_config.heartbeat_interval = Duration::from_millis(25);
    let mut orders_sender =
        SenderLoop::new(Arc::clone(&orders_pub), orders_socket, orders_send_config);

    let resp_sub = Arc::new(
        SubscriptionLog::new(SubscriptionConfig {
            session_id: RUMCAST_SESSION_ID,
            stream_id: RUMCAST_RESP_STREAM,
            initial_term_id: INITIAL_TERM_ID,
            term_length: TERM_LENGTH,
        })
        .unwrap(),
    );
    let resp_socket = KernelUdp::bind(bench_addr).unwrap();
    let mut resp_recv_config = ReceiverConfig::defaults(server_addr, BENCH_RECEIVER_ID);
    resp_recv_config.sm_interval = Duration::from_millis(50);
    let mut resp_receiver = ReceiverLoop::new(Arc::clone(&resp_sub), resp_socket, resp_recv_config);

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
        RUMCAST_SESSION_ID,
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
                    RUMCAST_SESSION_ID,
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
