//! End-to-end integration tests: full publisher ↔ subscriber pipelines
//! over real UDP loopback, including a loss-injection wrapper to
//! exercise NAK-driven recovery and multi-subscriber multicast fan-out.

use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use melin_rumcast::flow_control::FlowControl;
use melin_rumcast::pub_log::{PublicationConfig, PublicationLog};
use melin_rumcast::receiver::{ReceiverConfig, ReceiverLoop};
use melin_rumcast::sender::{SenderConfig, SenderLoop};
use melin_rumcast::sub_log::{SubscriptionConfig, SubscriptionLog};
use melin_rumcast::transport::{KernelUdp, UdpTransport};
use melin_rumcast::wire::{FrameView, data_flags};

const SESSION_ID: u32 = 0xCAFE;
const STREAM_ID: u32 = 0xBABE;
const TERM_LENGTH: u32 = 64 * 1024;
const MTU: u32 = 1024;
const INITIAL_TERM: u32 = 100;

fn loopback(port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
}

fn pub_cfg() -> PublicationConfig {
    PublicationConfig {
        session_id: SESSION_ID,
        stream_id: STREAM_ID,
        initial_term_id: INITIAL_TERM,
        term_length: TERM_LENGTH,
        mtu: MTU,
    }
}

fn sub_cfg() -> SubscriptionConfig {
    SubscriptionConfig {
        session_id: SESSION_ID,
        stream_id: STREAM_ID,
        initial_term_id: INITIAL_TERM,
        term_length: TERM_LENGTH,
    }
}

/// UDP transport wrapper that lets a test drop selected sends. Useful
/// for exercising NAK-driven recovery deterministically (drop send #N,
/// observe the retransmit).
struct LossyTransport {
    inner: KernelUdp,
    send_count: AtomicU32,
    drop_when: Box<dyn Fn(u32) -> bool + Send + Sync>,
}

impl LossyTransport {
    fn new<F>(inner: KernelUdp, drop_when: F) -> Self
    where
        F: Fn(u32) -> bool + Send + Sync + 'static,
    {
        Self {
            inner,
            send_count: AtomicU32::new(0),
            drop_when: Box::new(drop_when),
        }
    }
}

impl UdpTransport for LossyTransport {
    fn send_to(&self, dst: SocketAddr, bytes: &[u8]) -> io::Result<usize> {
        let n = self.send_count.fetch_add(1, Ordering::Relaxed);
        if (self.drop_when)(n) {
            return Ok(bytes.len()); // black-hole: pretend we sent
        }
        self.inner.send_to(dst, bytes)
    }

    fn recv_from(&self, buf: &mut [u8]) -> io::Result<Option<(SocketAddr, usize)>> {
        self.inner.recv_from(buf)
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    fn join_multicast_v4(&self, g: Ipv4Addr, i: Ipv4Addr) -> io::Result<()> {
        self.inner.join_multicast_v4(g, i)
    }

    fn leave_multicast_v4(&self, g: Ipv4Addr, i: Ipv4Addr) -> io::Result<()> {
        self.inner.leave_multicast_v4(g, i)
    }
}

/// Spawn a sender tick loop that exits when `shutdown` becomes true.
fn spawn_sender<T: UdpTransport + 'static>(
    mut sender: SenderLoop<T>,
    shutdown: Arc<AtomicBool>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        while !shutdown.load(Ordering::Acquire) {
            let _ = sender.tick();
            // Yield rather than spin so the test runner doesn't get
            // starved when many tests run in parallel.
            thread::sleep(Duration::from_micros(50));
        }
    })
}

/// Spawn a receiver tick loop.
fn spawn_receiver<T: UdpTransport + 'static>(
    mut receiver: ReceiverLoop<T>,
    shutdown: Arc<AtomicBool>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        while !shutdown.load(Ordering::Acquire) {
            let _ = receiver.tick();
            thread::sleep(Duration::from_micros(50));
        }
    })
}

/// Spawn a subscriber thread that polls `log` and pushes each
/// delivered fragment's first payload byte into `out` (used as a
/// fingerprint for ordering / completeness assertions).
fn spawn_subscriber(
    log: Arc<SubscriptionLog>,
    out: Arc<std::sync::Mutex<Vec<u8>>>,
    shutdown: Arc<AtomicBool>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        while !shutdown.load(Ordering::Acquire) {
            log.poll(64 * 1024, |view| {
                if let FrameView::Data { payload, .. } = view
                    && !payload.is_empty()
                {
                    out.lock().unwrap().push(payload[0]);
                }
            });
            thread::sleep(Duration::from_micros(50));
        }
        // Final drain after shutdown to catch any in-flight fragments.
        log.poll(64 * 1024, |view| {
            if let FrameView::Data { payload, .. } = view
                && !payload.is_empty()
            {
                out.lock().unwrap().push(payload[0]);
            }
        });
    })
}

/// Wait until `cond()` is true, or panic on timeout.
fn wait_until<F: FnMut() -> bool>(timeout: Duration, mut cond: F, what: &str) {
    let deadline = Instant::now() + timeout;
    while !cond() {
        if Instant::now() > deadline {
            panic!("timeout waiting for: {what}");
        }
        thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn unicast_round_trip_in_order() {
    // Publisher publishes 50 fragments via SenderLoop → kernel UDP →
    // ReceiverLoop → SubscriptionLog. Subscriber thread polls and
    // collects a fingerprint per fragment.
    let pub_log = Arc::new(PublicationLog::new(pub_cfg()).unwrap());
    pub_log.set_publisher_limit(u64::MAX); // disable back-pressure

    let sub_log = Arc::new(SubscriptionLog::new(sub_cfg()).unwrap());
    let sub_socket = KernelUdp::bind(loopback(0)).unwrap();
    let sub_addr = sub_socket.local_addr().unwrap();
    let pub_socket = KernelUdp::bind(loopback(0)).unwrap();
    let pub_addr = pub_socket.local_addr().unwrap();

    let sender_config = {
        let mut c = SenderConfig::defaults(sub_addr);
        c.setup_interval = Duration::from_secs(3600); // disable in test
        c.heartbeat_interval = Duration::from_secs(3600);
        c
    };
    let sender = SenderLoop::new(Arc::clone(&pub_log), pub_socket, sender_config);

    let receiver_config = {
        let mut c = ReceiverConfig::defaults(pub_addr, 1);
        c.sm_interval = Duration::from_millis(2);
        c.nak_backoff_min = Duration::from_micros(100);
        c.nak_backoff_jitter = Duration::from_micros(100);
        c
    };
    let receiver = ReceiverLoop::new(Arc::clone(&sub_log), sub_socket, receiver_config);

    let shutdown = Arc::new(AtomicBool::new(false));
    let collected = Arc::new(std::sync::Mutex::new(Vec::new()));

    let send_h = spawn_sender(sender, Arc::clone(&shutdown));
    let recv_h = spawn_receiver(receiver, Arc::clone(&shutdown));
    let sub_h = spawn_subscriber(
        Arc::clone(&sub_log),
        Arc::clone(&collected),
        Arc::clone(&shutdown),
    );

    // Engine publishes 50 fragments; payload[0] is the message ordinal.
    for i in 0u8..50 {
        let mut claim = pub_log.try_claim(64).unwrap();
        claim.payload_mut().fill(i);
        claim.publish(data_flags::UNFRAGMENTED);
    }

    // Wait for all 50 to be delivered to the subscriber.
    wait_until(
        Duration::from_secs(5),
        || collected.lock().unwrap().len() >= 50,
        "subscriber to receive all 50 fragments",
    );

    shutdown.store(true, Ordering::Release);
    send_h.join().unwrap();
    recv_h.join().unwrap();
    sub_h.join().unwrap();

    let got = collected.lock().unwrap().clone();
    assert!(got.len() >= 50, "expected ≥50, got {}", got.len());
    let expected: Vec<u8> = (0u8..50).collect();
    assert_eq!(&got[..50], &expected[..]);
}

#[test]
fn recovery_via_nak_after_loss() {
    // Drop the 1st data send (the very first fragment). Publisher's
    // SETUP/HB are not in the data path here (intervals are 1 hr),
    // so send #0 is the first DataFrame. Subscriber detects gap,
    // NAKs, sender retransmits — that retransmit goes through, since
    // drop_when only matches send #0.
    let pub_log = Arc::new(PublicationLog::new(pub_cfg()).unwrap());
    pub_log.set_publisher_limit(u64::MAX);

    let sub_log = Arc::new(SubscriptionLog::new(sub_cfg()).unwrap());
    let sub_socket = KernelUdp::bind(loopback(0)).unwrap();
    let sub_addr = sub_socket.local_addr().unwrap();
    // Publisher's outbound transport drops send #0 (the first
    // DataFrame). Recv side passes through unchanged.
    let pub_socket_inner = KernelUdp::bind(loopback(0)).unwrap();
    let pub_addr = pub_socket_inner.local_addr().unwrap();
    let pub_socket = LossyTransport::new(pub_socket_inner, |n| n == 0);

    let sender_config = {
        let mut c = SenderConfig::defaults(sub_addr);
        c.setup_interval = Duration::from_secs(3600);
        c.heartbeat_interval = Duration::from_secs(3600);
        c
    };
    let sender = SenderLoop::new(Arc::clone(&pub_log), pub_socket, sender_config);

    let receiver_config = {
        let mut c = ReceiverConfig::defaults(pub_addr, 1);
        // Aggressive: short NAK backoff so recovery is fast in the
        // test. SMs every 2ms so the publisher learns about us.
        c.sm_interval = Duration::from_millis(2);
        c.nak_backoff_min = Duration::from_micros(200);
        c.nak_backoff_jitter = Duration::from_micros(200);
        c
    };
    let receiver = ReceiverLoop::new(Arc::clone(&sub_log), sub_socket, receiver_config);

    let shutdown = Arc::new(AtomicBool::new(false));
    let collected = Arc::new(std::sync::Mutex::new(Vec::new()));

    let send_h = spawn_sender(sender, Arc::clone(&shutdown));
    let recv_h = spawn_receiver(receiver, Arc::clone(&shutdown));
    let sub_h = spawn_subscriber(
        Arc::clone(&sub_log),
        Arc::clone(&collected),
        Arc::clone(&shutdown),
    );

    // Engine publishes 10 fragments. Fragment 0 is dropped on the
    // initial send; the subscriber NAKs and gets it via retransmit.
    for i in 0u8..10 {
        let mut claim = pub_log.try_claim(64).unwrap();
        claim.payload_mut().fill(i);
        claim.publish(data_flags::UNFRAGMENTED);
    }

    wait_until(
        Duration::from_secs(5),
        || collected.lock().unwrap().len() >= 10,
        "subscriber to receive all 10 fragments after loss recovery",
    );

    shutdown.store(true, Ordering::Release);
    send_h.join().unwrap();
    recv_h.join().unwrap();
    sub_h.join().unwrap();

    let got = collected.lock().unwrap().clone();
    let expected: Vec<u8> = (0u8..10).collect();
    assert_eq!(
        &got[..10],
        &expected[..],
        "after NAK recovery, all fragments must be delivered in order"
    );
}

/// Sanity-check that this host actually routes multicast through the
/// expected interface. Sends a probe from `pub_socket` to `(group,
/// port)` (which `sub_socket` has joined) and waits up to `timeout`
/// for it to arrive. Returns true if delivery works.
///
/// On Linux without explicit `IP_MULTICAST_IF`, the kernel can pick
/// a non-loopback interface for multicast egress; an UNSPECIFIED-bound
/// subscriber on the same host then never sees the traffic. v1
/// KernelUdp doesn't expose that setsockopt; we detect the problem
/// here and let the calling test bail out gracefully.
fn multicast_loopback_works(
    pub_socket: &KernelUdp,
    sub_socket: &KernelUdp,
    group: Ipv4Addr,
    port: u16,
    timeout: Duration,
) -> bool {
    let probe = b"rumcast-mcast-probe";
    if pub_socket
        .send_to(SocketAddr::new(IpAddr::V4(group), port), probe)
        .is_err()
    {
        return false;
    }
    let deadline = Instant::now() + timeout;
    let mut buf = [0u8; 64];
    while Instant::now() < deadline {
        if let Ok(Some((_, len))) = sub_socket.recv_from(&mut buf)
            && &buf[..len] == probe
        {
            return true;
        }
        thread::sleep(Duration::from_millis(1));
    }
    false
}

#[test]
fn multicast_fan_out_to_subscriber() {
    // 1 publisher → multicast group → 1 subscriber on the same host.
    // (Two-receiver multicast on the same port needs SO_REUSEPORT,
    // which v1 KernelUdp doesn't expose — deferred to a future
    // transport feature.) Skipped gracefully if multicast loopback
    // isn't routable on the host (Linux defaults).
    let group = Ipv4Addr::new(239, 1, 2, 7);
    let mcast_port = {
        let scratch = UdpSocket::bind("0.0.0.0:0").unwrap();
        scratch.local_addr().unwrap().port()
    };

    let bind_subscriber = || -> Option<KernelUdp> {
        let socket = KernelUdp::bind(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            mcast_port,
        ))
        .ok()?;
        socket.set_multicast_loop_v4(true).ok()?;
        socket
            .join_multicast_v4(group, Ipv4Addr::UNSPECIFIED)
            .ok()?;
        Some(socket)
    };
    let Some(sub_socket) = bind_subscriber() else {
        eprintln!("skipping: no multicast-capable interface available");
        return;
    };

    let pub_socket = KernelUdp::bind(loopback(0)).unwrap();
    pub_socket.set_multicast_loop_v4(true).unwrap();
    pub_socket.set_multicast_ttl_v4(1).unwrap();
    let pub_addr = pub_socket.local_addr().unwrap();

    if !multicast_loopback_works(
        &pub_socket,
        &sub_socket,
        group,
        mcast_port,
        Duration::from_millis(200),
    ) {
        eprintln!("skipping: multicast loopback not routable on this host");
        return;
    }

    let pub_log = Arc::new(PublicationLog::new(pub_cfg()).unwrap());
    pub_log.set_publisher_limit(u64::MAX);
    let sub_log = Arc::new(SubscriptionLog::new(sub_cfg()).unwrap());

    let sender_config = {
        let mut c = SenderConfig::defaults(SocketAddr::new(IpAddr::V4(group), mcast_port));
        c.setup_interval = Duration::from_secs(3600);
        c.heartbeat_interval = Duration::from_secs(3600);
        c
    };
    let sender = SenderLoop::new(Arc::clone(&pub_log), pub_socket, sender_config);

    let receiver_config = {
        let mut c = ReceiverConfig::defaults(pub_addr, 1);
        c.sm_interval = Duration::from_millis(5);
        c.nak_backoff_min = Duration::from_micros(200);
        c.nak_backoff_jitter = Duration::from_micros(200);
        c
    };
    let receiver = ReceiverLoop::new(Arc::clone(&sub_log), sub_socket, receiver_config);

    let shutdown = Arc::new(AtomicBool::new(false));
    let collected = Arc::new(std::sync::Mutex::new(Vec::new()));
    let send_h = spawn_sender(sender, Arc::clone(&shutdown));
    let recv_h = spawn_receiver(receiver, Arc::clone(&shutdown));
    let sub_h = spawn_subscriber(
        Arc::clone(&sub_log),
        Arc::clone(&collected),
        Arc::clone(&shutdown),
    );

    for i in 0u8..20 {
        let mut claim = pub_log.try_claim(64).unwrap();
        claim.payload_mut().fill(i);
        claim.publish(data_flags::UNFRAGMENTED);
    }

    wait_until(
        Duration::from_secs(5),
        || collected.lock().unwrap().len() >= 20,
        "multicast subscriber to receive all 20 fragments",
    );

    shutdown.store(true, Ordering::Release);
    send_h.join().unwrap();
    recv_h.join().unwrap();
    sub_h.join().unwrap();

    let got = collected.lock().unwrap().clone();
    let expected: Vec<u8> = (0u8..20).collect();
    assert_eq!(&got[..20], &expected[..]);
}

#[test]
fn unicast_drives_term_rotation_on_both_sides() {
    // Publish enough fragments to fill more than one term (forces a
    // padding frame + rotation on the publisher, and a corresponding
    // rotation on the subscriber). 800 × 96-byte fragments = 76.8 KiB
    // > 64 KiB term length.
    let pub_log = Arc::new(PublicationLog::new(pub_cfg()).unwrap());
    pub_log.set_publisher_limit(u64::MAX);

    let sub_log = Arc::new(SubscriptionLog::new(sub_cfg()).unwrap());
    let sub_socket = KernelUdp::bind(loopback(0)).unwrap();
    let sub_addr = sub_socket.local_addr().unwrap();
    let pub_socket = KernelUdp::bind(loopback(0)).unwrap();
    let pub_addr = pub_socket.local_addr().unwrap();

    let sender_config = {
        let mut c = SenderConfig::defaults(sub_addr);
        c.setup_interval = Duration::from_secs(3600);
        c.heartbeat_interval = Duration::from_secs(3600);
        // Bigger drain budget so each sender tick can clear many
        // fragments — keeps the test fast.
        c.max_drain_per_tick = 64 * 1024;
        c
    };
    let sender = SenderLoop::new(Arc::clone(&pub_log), pub_socket, sender_config);

    let receiver_config = {
        let mut c = ReceiverConfig::defaults(pub_addr, 1);
        c.sm_interval = Duration::from_millis(2);
        c.nak_backoff_min = Duration::from_micros(100);
        c.nak_backoff_jitter = Duration::from_micros(100);
        c.max_recv_per_tick = 256;
        c
    };
    let receiver = ReceiverLoop::new(Arc::clone(&sub_log), sub_socket, receiver_config);

    let shutdown = Arc::new(AtomicBool::new(false));
    let collected = Arc::new(std::sync::Mutex::new(Vec::new()));
    let send_h = spawn_sender(sender, Arc::clone(&shutdown));
    let recv_h = spawn_receiver(receiver, Arc::clone(&shutdown));
    let sub_h = spawn_subscriber(
        Arc::clone(&sub_log),
        Arc::clone(&collected),
        Arc::clone(&shutdown),
    );

    // 800 fragments. Use the byte ordinal `i % 256` as the fingerprint.
    const N: usize = 800;
    for i in 0..N {
        let mut claim = pub_log.try_claim(64).unwrap();
        claim.payload_mut().fill((i % 256) as u8);
        claim.publish(data_flags::UNFRAGMENTED);
    }

    wait_until(
        Duration::from_secs(10),
        || collected.lock().unwrap().len() >= N,
        "subscriber to receive all 800 fragments across term rotations",
    );

    shutdown.store(true, Ordering::Release);
    send_h.join().unwrap();
    recv_h.join().unwrap();
    sub_h.join().unwrap();

    let got = collected.lock().unwrap().clone();
    assert!(got.len() >= N, "expected ≥{N}, got {}", got.len());
    let expected: Vec<u8> = (0..N).map(|i| (i % 256) as u8).collect();
    assert_eq!(
        &got[..N],
        &expected[..],
        "all 800 fragments must arrive in order across at least one term rotation"
    );
}

#[test]
fn min_flow_control_back_pressures_publisher() {
    // No subscribers connected: the publisher should fill exactly the
    // first term (per the log's startup default), then BackPressure.
    // This proves the Min strategy is doing nothing wrong in the
    // no-receiver case (and validates the startup default).
    let pub_log = Arc::new(PublicationLog::new(pub_cfg()).unwrap());
    let pub_socket = KernelUdp::bind(loopback(0)).unwrap();
    let sender_config = {
        let mut c = SenderConfig::defaults(loopback(1));
        c.setup_interval = Duration::from_secs(3600);
        c.heartbeat_interval = Duration::from_secs(3600);
        c.flow_control = FlowControl::Min;
        c
    };
    let sender = SenderLoop::new(Arc::clone(&pub_log), pub_socket, sender_config);

    let shutdown = Arc::new(AtomicBool::new(false));
    let send_h = spawn_sender(sender, Arc::clone(&shutdown));

    // Fill the first term to the byte. mtu = 1024 → 64 fragments of
    // (mtu - 32) payload exactly fill 64 KiB.
    let payload_size = MTU - 32;
    let frags_per_term = (TERM_LENGTH / MTU) as usize;
    for _ in 0..frags_per_term {
        let claim = pub_log.try_claim(payload_size).unwrap();
        claim.publish(data_flags::UNFRAGMENTED);
    }
    // Allow the sender thread to drain so publisher_position catches
    // up internally — though it doesn't matter for this assertion.
    thread::sleep(Duration::from_millis(20));

    // The 65th claim must hit BackPressure: with no subscribers, the
    // limit hasn't moved past the initial first-term ceiling.
    match pub_log.try_claim(payload_size) {
        Err(melin_rumcast::pub_log::ClaimError::BackPressure { .. }) => {}
        Err(other) => panic!("expected BackPressure with no subscribers, got Err({other:?})"),
        Ok(_) => panic!("expected BackPressure with no subscribers, got Ok(_)"),
    }

    shutdown.store(true, Ordering::Release);
    send_h.join().unwrap();
}
