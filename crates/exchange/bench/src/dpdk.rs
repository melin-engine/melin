//! DPDK kernel-bypass benchmark client.
//!
//! Replaces the io_uring event loop with a single-threaded smoltcp
//! poll loop over DPDK. All connections share one smoltcp interface on
//! one DPDK port. The bench client initiates outbound TCP connections
//! to the server via `socket.connect()`.
//!
//! This module is only compiled with `--features dpdk`.

use std::collections::VecDeque;
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use hdrhistogram::Histogram;
use smoltcp::iface::{Config, Interface, SocketHandle, SocketSet};
use smoltcp::socket::tcp::{self, State};
use smoltcp::time::Instant as SmolInstant;
use smoltcp::wire::{EthernetAddress, HardwareAddress, IpAddress, IpCidr, Ipv4Address};

use melin_dpdk::device::DpdkDevice;
use melin_dpdk::eal::Eal;
use melin_dpdk::mempool::Mempool;
use melin_dpdk::port::Port;
use melin_protocol::codec;
use melin_protocol::message::ResponseKind;

use crate::generator;
use crate::{BenchPhases, TimeSeries, maybe_sample, print_results, spawn_progress_reporter};

/// TCP socket buffer size. 64KB gives plenty of headroom for pipelined
/// frames in flight.
const SOCKET_BUF_SIZE: usize = 65536;

/// How often to refresh the smoltcp timestamp (poll iterations).
/// During connection setup (ARP + TCP handshake), smoltcp needs
/// advancing timestamps to drive retransmit timers. Using 1 here
/// (refresh every poll) to avoid stalls. The SystemTime call is
/// vDSO-accelerated (~20ns) so the overhead is negligible.
const TIMESTAMP_REFRESH_INTERVAL: u32 = 1;

/// Per-connection state for the DPDK benchmark.
struct DpdkBenchConn {
    handle: SocketHandle,
    /// Accumulated received bytes for frame parsing.
    parse_buf: Vec<u8>,
    /// Order generator — produces frames on-the-fly. No pre-allocated cap:
    /// the bench runs until the wall-clock cooldown deadline expires.
    flow: generator::OrderFlowGenerator,
    /// Reusable scratch buffer holding the current wire frame
    /// (`[u32 LE len][payload]`) until it's fully sent. Sized for the
    /// largest encoded request.
    scratch_frame: Vec<u8>,
    /// A frame that was pulled from the generator but never attempted on
    /// the wire (socket couldn't send or returned an error). Held here so
    /// the next poll iteration can retry without re-generating — the
    /// generator is stateful and cannot be rewound. Distinct from
    /// `send_pending`, which holds *partial-send remainders* of frames
    /// that have already started hitting the wire.
    pending_unsent: Option<Vec<u8>>,
    /// Pending send bytes (partial frame that didn't fit in smoltcp TX buffer).
    send_pending: Vec<u8>,
    /// FIFO of send timestamps (TSC ticks) for in-flight orders.
    /// `u64` instead of `Instant` to avoid ~15-25ns vDSO overhead per
    /// timestamp on the hot path. With pacing enabled this stores the
    /// *scheduled* TSC, not the actual send TSC (coordinated-omission fix).
    inflight_ts: VecDeque<u64>,
    /// Open-loop scheduler when `--target-rate > 0`. Built after auth
    /// handshake so its TSC anchor is close to the measurement start.
    pacer: Option<crate::PaceClock>,
}

/// DPDK benchmark configuration.
pub struct DpdkBenchConfig {
    pub eal_args: Vec<String>,
    /// DPDK port IDs to poll. First port is used for TX; all are polled
    /// for RX. For LACP bonds, pass both VF port IDs.
    pub port_ids: Vec<u16>,
    pub local_ip: Ipv4Addr,
    pub prefix_len: u8,
    pub gateway: Option<Ipv4Addr>,
    pub server_addr: std::net::SocketAddr,
    /// MTU for the DPDK interface. Must match the server's MTU.
    pub mtu: usize,
    /// VLAN ID for hardware strip/insert. Required for dedicated NIC mode.
    pub vlan_id: Option<u16>,
}

/// Run the DPDK roundtrip benchmark.
#[allow(clippy::too_many_arguments)]
pub fn run_dpdk_roundtrip(
    config: DpdkBenchConfig,
    phases: BenchPhases,
    window: usize,
    num_clients: usize,
    json_path: Option<&std::path::Path>,
    key: &ed25519_dalek::SigningKey,
    num_accounts: u32,
    num_instruments: u32,
    core_id: usize,
    health_addr: Option<std::net::SocketAddr>,
    target_rate: u64,
) {
    // Pin to dedicated core.
    if let Err(e) = melin_app::affinity::pin_to_core(core_id) {
        eprintln!("warning: could not pin bench to core {core_id}: {e}");
    }

    // Initialize DPDK.
    let eal_args: Vec<&str> = config.eal_args.iter().map(|s| s.as_str()).collect();
    let eal = Eal::init(&eal_args).expect("EAL init failed");
    let port_count = eal.port_count();
    for &pid in &config.port_ids {
        assert!(
            pid < port_count,
            "DPDK port {pid} not found (available: {port_count})",
        );
    }

    // Use more mbufs for the bench client — many connections with large windows.
    // Increase for extra ports.
    let num_mbufs: u32 = if config.port_ids.len() > 1 {
        24576
    } else {
        16384
    };
    let mempool = if config.mtu > 1500 {
        Mempool::create_for_mtu("bench_pool", num_mbufs, config.mtu as u16, 0)
            .expect("mempool failed")
    } else {
        Mempool::create_with_size("bench_pool", num_mbufs, 0).expect("mempool failed")
    };

    // Configure and start all ports. Use intersection of offload caps.
    let mut ports = Vec::with_capacity(config.port_ids.len());
    let mut combined_offloads: Option<melin_dpdk::port::ChecksumOffloads> = None;
    for &pid in &config.port_ids {
        let mut port = Port::configure_with_vlan(pid, &mempool, config.vlan_id, 1)
            .expect("port config failed");
        port.start().expect("port start failed");
        combined_offloads = Some(match combined_offloads {
            None => port.offloads,
            Some(prev) => prev.intersect(port.offloads),
        });
        ports.push(port);
    }
    let offloads = combined_offloads.unwrap_or_default();

    let mac = ports[0].mac_addr();
    let mut device = DpdkDevice::new(&config.port_ids, mempool.as_raw(), offloads, 0);
    if config.mtu != 1500 {
        device.set_mtu(config.mtu);
        eprintln!("  DPDK jumbo frames: MTU {}", config.mtu);
    }
    if let Some(vlan_id) = config.vlan_id {
        device.set_vlan_id(vlan_id);
    }

    let hw_addr = HardwareAddress::Ethernet(EthernetAddress(mac));
    let iface_config = Config::new(hw_addr);
    let now = SmolInstant::from_millis(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64,
    );
    let mut iface = Interface::new(iface_config, &mut device, now);

    let ip = Ipv4Address::new(
        config.local_ip.octets()[0],
        config.local_ip.octets()[1],
        config.local_ip.octets()[2],
        config.local_ip.octets()[3],
    );
    iface.update_ip_addrs(|addrs| {
        addrs
            .push(IpCidr::new(IpAddress::Ipv4(ip), config.prefix_len))
            .expect("IP address capacity");
    });

    if let Some(gw) = config.gateway {
        let gw_addr = Ipv4Address::new(
            gw.octets()[0],
            gw.octets()[1],
            gw.octets()[2],
            gw.octets()[3],
        );
        iface
            .routes_mut()
            .add_default_ipv4_route(gw_addr)
            .expect("default route");
    }

    // SR-IOV VFs can't receive broadcast ARP, so normal ARP resolution
    // fails. Two workarounds:
    // 1. Send a gratuitous ARP so the switch learns our MAC.
    // 2. Seed the server's MAC into smoltcp's neighbor cache via a crafted
    //    ARP reply. The server's VF MAC is derived from its DPDK IP
    //    (02:00:IP[0]:IP[1]:IP[2]:IP[3]) — same scheme as dpdk-setup-sriov.sh.
    {
        let our_ip = config.local_ip.octets();
        let mut frame = [0u8; 42];
        // Ethernet header: broadcast destination
        frame[0..6].copy_from_slice(&[0xff; 6]);
        frame[6..12].copy_from_slice(&mac);
        frame[12..14].copy_from_slice(&[0x08, 0x06]); // ARP
        // ARP: Ethernet + IPv4, request
        frame[14..16].copy_from_slice(&[0x00, 0x01]); // hardware type: Ethernet
        frame[16..18].copy_from_slice(&[0x08, 0x00]); // protocol type: IPv4
        frame[18] = 6; // hardware size
        frame[19] = 4; // protocol size
        frame[20..22].copy_from_slice(&[0x00, 0x01]); // opcode: request
        frame[22..28].copy_from_slice(&mac); // sender MAC
        frame[28..32].copy_from_slice(&our_ip); // sender IP
        frame[32..38].copy_from_slice(&[0xff; 6]); // target MAC
        frame[38..42].copy_from_slice(&our_ip); // target IP = sender IP (gratuitous)
        device.send_raw_frame(&frame);

        // Seed the server's MAC into our neighbor cache.
        let server_ip_bytes = match config.server_addr.ip() {
            std::net::IpAddr::V4(v4) => v4.octets(),
            _ => panic!("IPv6 not supported"),
        };
        let server_mac = [
            0x02,
            0x00,
            server_ip_bytes[0],
            server_ip_bytes[1],
            server_ip_bytes[2],
            server_ip_bytes[3],
        ];
        // Inject a crafted ARP reply into smoltcp so it learns the server's MAC.
        let mut arp_reply = [0u8; 42];
        arp_reply[0..6].copy_from_slice(&mac); // dest: us
        arp_reply[6..12].copy_from_slice(&server_mac); // src: server
        arp_reply[12..14].copy_from_slice(&[0x08, 0x06]); // ARP
        arp_reply[14..16].copy_from_slice(&[0x00, 0x01]); // HW type
        arp_reply[16..18].copy_from_slice(&[0x08, 0x00]); // Proto type
        arp_reply[18] = 6;
        arp_reply[19] = 4;
        arp_reply[20..22].copy_from_slice(&[0x00, 0x02]); // opcode: reply
        arp_reply[22..28].copy_from_slice(&server_mac);
        arp_reply[28..32].copy_from_slice(&server_ip_bytes);
        arp_reply[32..38].copy_from_slice(&mac);
        arp_reply[38..42].copy_from_slice(&our_ip);
        device.inject_rx(arp_reply.to_vec());

        // Process the injected ARP reply so smoltcp populates its cache.
        // Use a temporary empty socket set — no sockets needed for ARP.
        let mut tmp_sockets = SocketSet::new(Vec::new());
        device.poll_rx();
        iface.poll(now, &mut device, &mut tmp_sockets);
        device.flush_tx();

        eprintln!(
            "  ARP: sent gratuitous, seeded server MAC {:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
            server_mac[0],
            server_mac[1],
            server_mac[2],
            server_mac[3],
            server_mac[4],
            server_mac[5]
        );
    }

    let mut sockets = SocketSet::new(Vec::with_capacity(num_clients + 1));
    let mut cached_ts = now;
    let mut poll_count: u32 = 0;

    let server_ip = match config.server_addr.ip() {
        std::net::IpAddr::V4(v4) => v4,
        _ => panic!("IPv6 not supported"),
    };
    let server_endpoint = smoltcp::wire::IpEndpoint {
        addr: IpAddress::Ipv4(Ipv4Address::new(
            server_ip.octets()[0],
            server_ip.octets()[1],
            server_ip.octets()[2],
            server_ip.octets()[3],
        )),
        port: config.server_addr.port(),
    };

    // Helper: poll NIC + smoltcp.
    let poll = |device: &mut DpdkDevice,
                iface: &mut Interface,
                sockets: &mut SocketSet,
                poll_count: &mut u32,
                cached_ts: &mut SmolInstant| {
        *poll_count = poll_count.wrapping_add(1);
        if poll_count.is_multiple_of(TIMESTAMP_REFRESH_INTERVAL) {
            *cached_ts = SmolInstant::from_millis(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64,
            );
        }
        device.poll_rx();
        iface.poll(*cached_ts, device, sockets);
        device.flush_tx();
    };

    // --- Connect, auth, and set up each client sequentially ---
    //
    // Each socket must be created, connected, and authenticated one at a
    // time. smoltcp's connect() sends the SYN on the next poll — if we
    // create all sockets upfront, all SYNs go out simultaneously and the
    // server may accept them in arbitrary order, causing auth deadlocks.
    // Build a generator per client. Phases are driven by wall-clock
    // deadlines on the shared `start` instant, so there is no
    // per-client total to compute — generators run indefinitely.
    //
    // ORDER_ID_STRIDE matches the io_uring path: 2^48 ids per client,
    // more than three orders of magnitude beyond any realistic run at
    // 10 M/s.
    const ORDER_ID_STRIDE: u64 = 1u64 << 48;
    let per_client: Vec<generator::OrderFlowGenerator> = (0..num_clients)
        .map(|client_id| {
            generator::OrderFlowGenerator::new(generator::GeneratorConfig {
                num_accounts,
                num_instruments,
                start_order_id: ORDER_ID_STRIDE * (client_id as u64) + 1,
                ..Default::default()
            })
        })
        .collect();
    eprintln!("  per-client generators initialised for {num_clients} clients");

    // Sequential connect + auth — smoltcp's TCP stack is single-threaded
    // and shared across all sockets via the same `Interface` poll loop.
    // Per-client SYN-then-auth ordering avoids the auth-deadlock risk of
    // sending all SYNs at once and accepting them in arbitrary order.
    let mut connections: Vec<DpdkBenchConn> = Vec::with_capacity(num_clients);
    let setup_start = Instant::now();
    eprintln!("  connecting {num_clients} clients via DPDK...");
    for (client_id, flow) in per_client.into_iter().enumerate() {
        let rx_buf = tcp::SocketBuffer::new(vec![0u8; SOCKET_BUF_SIZE]);
        let tx_buf = tcp::SocketBuffer::new(vec![0u8; SOCKET_BUF_SIZE]);
        let mut socket = tcp::Socket::new(rx_buf, tx_buf);
        // Low-latency TCP tuning for dedicated LAN.
        socket.set_nagle_enabled(false);
        socket.set_ack_delay(None);
        socket.set_min_rto(smoltcp::time::Duration::from_millis(10));
        socket.set_initial_rto(smoltcp::time::Duration::from_millis(50));
        socket.set_initial_congestion_window(64 * 1024);

        // Randomize ephemeral port base to avoid colliding with TIME_WAIT
        // entries from a previous bench run against the same server.
        let local_port = 49152 + (std::process::id() as u16 % 8192) + client_id as u16;
        socket
            .connect(
                iface.context(),
                server_endpoint,
                (IpAddress::Ipv4(ip), local_port),
            )
            .unwrap_or_else(|e| panic!("connect failed for client {client_id}: {e}"));

        let handle = sockets.add(socket);

        connections.push(DpdkBenchConn {
            handle,
            parse_buf: Vec::with_capacity(1028),
            flow,
            scratch_frame: Vec::with_capacity(generator::MAX_REQUEST_FRAME_BYTES),
            pending_unsent: None,
            send_pending: Vec::new(),
            inflight_ts: VecDeque::with_capacity(window),
            pacer: None,
        });

        // Wait for TCP handshake to complete.
        loop {
            poll(
                &mut device,
                &mut iface,
                &mut sockets,
                &mut poll_count,
                &mut cached_ts,
            );
            let s = sockets.get_mut::<tcp::Socket>(handle);
            if s.state() == State::Established {
                break;
            }
        }

        // Auth handshake.
        {
            let conn = std::slice::from_mut(connections.last_mut().unwrap());
            dpdk_auth_all(
                conn,
                &mut device,
                &mut iface,
                &mut sockets,
                &mut poll_count,
                &mut cached_ts,
                key,
            );
        }

        eprintln!("  client {}/{num_clients}: connected", client_id + 1);
    }
    eprintln!(
        "  all {num_clients} clients ready ({:.1}s setup)",
        setup_start.elapsed().as_secs_f64(),
    );

    // --- Main benchmark loop ---
    let progress = Arc::new(AtomicU64::new(0));
    let progress_shutdown = Arc::new(AtomicBool::new(false));
    let pace_stats = Arc::new(crate::PaceStats::default());
    let progress_handle = spawn_progress_reporter(
        Arc::clone(&progress),
        phases,
        Arc::clone(&progress_shutdown),
        target_rate,
        Arc::clone(&pace_stats),
    );

    let health_poller = health_addr.map(crate::health_poller::HealthPoller::start);

    let ticks_per_ns = crate::calibrate_tsc();
    let start = Instant::now();
    let deadlines = phases.deadlines(start);
    let mut measured_start: Option<Instant> = None;

    // Build per-connection pacers anchored at the bench-start TSC. With
    // the DPDK loop running single-threaded over all connections, a
    // shared anchor is sufficient; staggered first sends via
    // `conn_index` keep the connections out of phase within one period.
    // `warmup_end_tsc` gates pace_stats telemetry so `scheduled` /
    // `late_sends` cover the same phase as `achieved_rate`.
    let warmup_end_tsc: u64 = if target_rate > 0 {
        let start_tsc = crate::rdtscp();
        let clients = num_clients.max(1) as u64;
        for (idx, conn) in connections.iter_mut().enumerate() {
            conn.pacer = Some(crate::PaceClock::new(
                target_rate,
                clients,
                ticks_per_ns,
                start_tsc,
                idx as u64,
            ));
        }
        let warmup_ticks = (phases.warmup.as_nanos() as f64 * ticks_per_ns) as u64;
        start_tsc.saturating_add(warmup_ticks)
    } else {
        0
    };

    let mut histogram =
        Histogram::<u64>::new_with_bounds(1, 10_000_000_000, 3).expect("histogram bounds");
    let mut interval_hist =
        Histogram::<u64>::new_with_bounds(1, 10_000_000_000, 3).expect("interval histogram");
    let mut interval_count: usize = 0;
    let mut series: TimeSeries = Vec::new();

    // Outer-loop wall-time histogram, gated on at least one BatchEnd
    // received this iteration AND on being past warmup so we measure
    // steady-state.
    #[cfg(feature = "latency-trace")]
    let mut poll_iter_hist =
        Histogram::<u64>::new_with_bounds(1, 1_000_000_000, 3).expect("poll iter histogram bounds");

    // Per-component histograms for the poll closure's three sub-calls.
    // The outer poll iter is ~1 µs typical (p99 ~1.5 µs) — nowhere near
    // the 30 µs we want to attribute. Splitting into `poll_rx`,
    // `iface.poll`, `flush_tx` tells us if any one of them occasionally
    // takes ~30 µs. Most likely candidate: `iface.poll` when smoltcp's
    // TCP state machine fires a timer-driven action (delayed ACK,
    // window update, retransmit). Recorded only on calls invoked from
    // the main measurement loop, never on connect-phase calls.
    #[cfg(feature = "latency-trace")]
    let mut poll_rx_hist =
        Histogram::<u64>::new_with_bounds(1, 1_000_000_000, 3).expect("poll_rx hist bounds");
    #[cfg(feature = "latency-trace")]
    let mut iface_poll_hist =
        Histogram::<u64>::new_with_bounds(1, 1_000_000_000, 3).expect("iface.poll hist bounds");
    #[cfg(feature = "latency-trace")]
    let mut flush_tx_hist =
        Histogram::<u64>::new_with_bounds(1, 1_000_000_000, 3).expect("flush_tx hist bounds");

    // Poll every N connections to keep the NIC busy during the
    // connection iteration. Without this, smoltcp's TX buffer fills
    // up with 16 connections at window 256, and the NIC sits idle
    // until the next flush_tx() at the end of the loop.
    const POLL_EVERY_N_CONNS: usize = 4;

    loop {
        // Stamp the start of each outer iter for the poll_iter histogram.
        // Cheap: rdtsc is a few cycles. The compiler elides this when the
        // feature is off (MonoTraceInstant = ()).
        #[cfg(feature = "latency-trace")]
        let iter_start_tsc = crate::rdtscp();
        #[cfg(feature = "latency-trace")]
        let mut work_done_this_iter = false;

        // Inlined poll with per-component timing. Mirrors the `poll`
        // closure at the top of the function, except with rdtscp stamps
        // around `poll_rx`, `iface.poll`, and `flush_tx`. Only the main
        // loop's poll call is instrumented; connect-phase calls go
        // through the original closure unchanged.
        poll_count = poll_count.wrapping_add(1);
        if poll_count.is_multiple_of(TIMESTAMP_REFRESH_INTERVAL) {
            cached_ts = SmolInstant::from_millis(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64,
            );
        }
        #[cfg(feature = "latency-trace")]
        let t0 = crate::rdtscp();
        device.poll_rx();
        #[cfg(feature = "latency-trace")]
        let t1 = crate::rdtscp();
        iface.poll(cached_ts, &mut device, &mut sockets);
        #[cfg(feature = "latency-trace")]
        let t2 = crate::rdtscp();
        device.flush_tx();
        #[cfg(feature = "latency-trace")]
        let t3 = crate::rdtscp();
        #[cfg(feature = "latency-trace")]
        {
            poll_rx_hist
                .record(crate::tsc_to_ns(t1 - t0, ticks_per_ns))
                .ok();
            iface_poll_hist
                .record(crate::tsc_to_ns(t2 - t1, ticks_per_ns))
                .ok();
            flush_tx_hist
                .record(crate::tsc_to_ns(t3 - t2, ticks_per_ns))
                .ok();
        }

        // Sample the wall clock once per outer iter and reuse it for
        // both the cooldown break and the per-completion phase
        // classifier below. Saves one vDSO call (~15-25 ns) per
        // BatchEnd; at multi-M ops/s the per-completion `Instant::now()`
        // was visible in profiles. Phase boundaries are coarse (5 s
        // warmup, 60 s measured) so the reused timestamp misclassifies
        // at most a handful of samples on a boundary — orders of
        // magnitude below run-to-run noise.
        //
        // Wall-clock-driven termination: stop once cooldown ends. The
        // histogram is sealed at `measured_end` so any responses that
        // would arrive in the cooldown tail are already discarded by
        // the phase classifier; abandoning them doesn't change the
        // reported latencies.
        let now = Instant::now();
        if now >= deadlines.cooldown_end {
            break;
        }

        for (i, conn) in connections.iter_mut().enumerate() {
            // Mid-iteration poll to flush TX and receive new data.
            if i > 0 && i % POLL_EVERY_N_CONNS == 0 {
                poll(
                    &mut device,
                    &mut iface,
                    &mut sockets,
                    &mut poll_count,
                    &mut cached_ts,
                );
            }

            let socket = sockets.get_mut::<tcp::Socket>(conn.handle);

            // --- Send: fill the window ---
            // First, drain any pending partial send.
            if !conn.send_pending.is_empty()
                && socket.can_send()
                && let Ok(sent) = socket.send_slice(&conn.send_pending)
            {
                if sent >= conn.send_pending.len() {
                    conn.send_pending.clear();
                } else if sent > 0 {
                    // Partial send — compact. This is rare (only under
                    // backpressure) so the memmove cost is acceptable.
                    conn.send_pending.drain(..sent);
                }
            }

            // Send new frames while window has room. Each frame is generated
            // on-the-fly into `scratch_frame` as [u32 LE len][payload]. When
            // the socket can't accept any bytes, the unsent frame is parked
            // in `pending_unsent` and `inflight_ts` is *not* advanced — so
            // the recorded send timestamp reflects the moment bytes actually
            // start hitting the wire, not the moment generation completed.
            //
            // With pacing, `pop_due` gates each new frame on the schedule
            // and the recorded timestamp is the *scheduled* tick (closes
            // the coordinated-omission loophole).
            while conn.send_pending.is_empty()
                && conn.inflight_ts.len() < window
                && socket.can_send()
            {
                // Pacing gate first — if the next slot isn't due yet,
                // stop filling for this conn this iteration.
                let paced_ts = if let Some(p) = conn.pacer.as_mut() {
                    let now_tsc = crate::rdtscp();
                    match p.pop_due(now_tsc) {
                        Some(scheduled) => {
                            // Gate telemetry on warmup-end so `scheduled` /
                            // `late_sends` align with the
                            // measured-phase `achieved_rate` divisor.
                            if now_tsc >= warmup_end_tsc {
                                pace_stats.record_send(now_tsc, scheduled, ticks_per_ns);
                            }
                            Some(scheduled)
                        }
                        None => break,
                    }
                } else {
                    None
                };

                // Reuse a previously-generated-but-never-sent frame if there
                // is one; otherwise pull a fresh frame from the generator.
                if let Some(prev) = conn.pending_unsent.take() {
                    conn.scratch_frame = prev;
                } else {
                    conn.scratch_frame.clear();
                    conn.flow.next_wire_frame(&mut conn.scratch_frame);
                }
                let wire_frame = &conn.scratch_frame;
                match socket.send_slice(wire_frame) {
                    Ok(n) if n == wire_frame.len() => {
                        conn.inflight_ts
                            .push_back(paced_ts.unwrap_or_else(crate::rdtscp));
                    }
                    Ok(n) if n > 0 => {
                        // Partial send — remainder goes to send_pending; the
                        // frame has started hitting the wire so it counts.
                        conn.send_pending.extend_from_slice(&wire_frame[n..]);
                        conn.inflight_ts
                            .push_back(paced_ts.unwrap_or_else(crate::rdtscp));
                    }
                    Ok(_) => {
                        // Zero bytes sent — socket transiently full. Park the
                        // frame and exit; retry on the next poll iteration.
                        // Roll back the pacer so the same scheduled slot
                        // is re-issued for the parked frame.
                        if paced_ts.is_some()
                            && let Some(p) = conn.pacer.as_mut()
                        {
                            p.unpop();
                        }
                        conn.pending_unsent = Some(std::mem::take(&mut conn.scratch_frame));
                        break;
                    }
                    Err(e) => {
                        // Client-side TCP error — debug! per project log-level
                        // convention (silent at default RUST_LOG level so a
                        // noisy network doesn't spam stderr). Park the frame
                        // for retry. Roll back the pacer (see above).
                        tracing::debug!("dpdk send_slice error: {e:?}");
                        if paced_ts.is_some()
                            && let Some(p) = conn.pacer.as_mut()
                        {
                            p.unpop();
                        }
                        conn.pending_unsent = Some(std::mem::take(&mut conn.scratch_frame));
                        break;
                    }
                }
            }

            // --- Recv: drain directly into parse_buf (no intermediate copy) ---
            while socket.can_recv() {
                match socket.recv(|data| {
                    conn.parse_buf.extend_from_slice(data);
                    (data.len(), ())
                }) {
                    Ok(()) => {}
                    Err(_) => break,
                }
            }

            // Parse frames from parse_buf.
            let mut cursor = 0;
            while cursor + 4 <= conn.parse_buf.len() {
                let frame_len = u32::from_le_bytes([
                    conn.parse_buf[cursor],
                    conn.parse_buf[cursor + 1],
                    conn.parse_buf[cursor + 2],
                    conn.parse_buf[cursor + 3],
                ]) as usize;

                if cursor + 4 + frame_len > conn.parse_buf.len() {
                    break;
                }

                let payload = &conn.parse_buf[cursor + 4..cursor + 4 + frame_len];
                if let Ok(response) = codec::decode_response(payload)
                    && matches!(response, ResponseKind::BatchEnd)
                {
                    // Always pop the inflight entry to keep the FIFO
                    // aligned with sends — without this, cooldown
                    // completions would leak the queue.  Phase
                    // classification by *receive* time, reusing the
                    // outer-iter `now`: past `measured_end` we discard
                    // the sample, before `warmup_end` we discard too.
                    if let Some(sent_tsc) = conn.inflight_ts.pop_front()
                        && now >= deadlines.warmup_end
                        && now < deadlines.measured_end
                    {
                        if measured_start.is_none() {
                            measured_start = Some(now);
                        }
                        let latency_ns = crate::tsc_to_ns(crate::rdtscp() - sent_tsc, ticks_per_ns);
                        histogram.record(latency_ns).ok();
                        interval_hist.record(latency_ns).ok();
                        interval_count += 1;
                        maybe_sample(&mut interval_hist, &mut interval_count, &mut series, start);
                        progress.fetch_add(1, Ordering::Relaxed);
                        #[cfg(feature = "latency-trace")]
                        {
                            work_done_this_iter = true;
                        }
                    }
                }

                cursor += 4 + frame_len;
            }

            // Compact parse buffer.
            if cursor > 0 {
                let remaining = conn.parse_buf.len() - cursor;
                conn.parse_buf.copy_within(cursor.., 0);
                conn.parse_buf.truncate(remaining);
            }
        }

        // Record this iteration's wall-time if it produced any measured
        // latency samples. Skip warmup/idle iters so the percentiles
        // describe steady-state poll behaviour, not connection setup.
        #[cfg(feature = "latency-trace")]
        if work_done_this_iter {
            let elapsed_ns = crate::tsc_to_ns(crate::rdtscp() - iter_start_tsc, ticks_per_ns);
            poll_iter_hist.record(elapsed_ns).ok();
        }
    }

    // Snapshot end time BEFORE joining the progress thread: that thread
    // sleeps in 5-second increments and only checks shutdown after each
    // sleep, so progress_handle.join() can block up to ~5s and would
    // otherwise inflate `measured_wall` for short benches.
    let end = Instant::now();

    // Stop progress reporter.
    progress_shutdown.store(true, Ordering::Relaxed);
    let _ = progress_handle.join();

    let measured_wall = measured_start
        .map(|s| end.duration_since(s).min(phases.measured))
        .unwrap_or_else(|| start.elapsed());

    series.sort_by(|a, b| a.elapsed_secs.partial_cmp(&b.elapsed_secs).unwrap());

    let mut extra_lines = vec![
        format!("  Transport: DPDK (smoltcp)"),
        format!("  DPDK core: {core_id}"),
        format!("  Window: {window}, Clients: {num_clients}"),
    ];
    let pacing_report = if target_rate > 0 {
        let scheduled = pace_stats.scheduled.load(Ordering::Relaxed);
        let late = pace_stats.late_sends.load(Ordering::Relaxed);
        let max_delay_us = crate::tsc_to_ns(
            pace_stats.max_send_delay_ticks.load(Ordering::Relaxed),
            ticks_per_ns,
        ) as f64
            / 1_000.0;
        extra_lines.push(format!(
            "  Target rate: {target_rate} ops/s (scheduled {scheduled}, late {late}, max send delay {max_delay_us:.1} µs)"
        ));
        Some(crate::PacingReport {
            target_rate,
            scheduled,
            late_sends: late,
            max_send_delay_us: max_delay_us,
        })
    } else {
        None
    };

    #[cfg(feature = "latency-trace")]
    {
        let us = |ns: u64| ns as f64 / 1000.0;
        let report = |name: &str, h: &Histogram<u64>| {
            if h.is_empty() {
                return;
            }
            eprintln!(
                "  {name}\n\
                 \x20   samples: {samples}\n\
                 \x20   min:    {min:>8.2} µs\n\
                 \x20   p50:    {p50:>8.2} µs\n\
                 \x20   p90:    {p90:>8.2} µs\n\
                 \x20   p99:    {p99:>8.2} µs\n\
                 \x20   p99.9:  {p999:>8.2} µs\n\
                 \x20   p99.99: {p9999:>8.2} µs\n\
                 \x20   max:    {max:>8.2} µs",
                samples = h.len(),
                min = us(h.min()),
                p50 = us(h.value_at_quantile(0.50)),
                p90 = us(h.value_at_quantile(0.90)),
                p99 = us(h.value_at_quantile(0.99)),
                p999 = us(h.value_at_quantile(0.999)),
                p9999 = us(h.value_at_quantile(0.9999)),
                max = us(h.max()),
            );
        };
        report(
            "bench poll: outer iteration (work-iters only)",
            &poll_iter_hist,
        );
        report("bench poll: device.poll_rx()", &poll_rx_hist);
        report("bench poll: iface.poll() (smoltcp)", &iface_poll_hist);
        report("bench poll: device.flush_tx()", &flush_tx_hist);
    }

    // Fetch the server-side per-stage histogram dump before the
    // server shuts down. Best-effort; missing data is rendered as a
    // one-line note in print_results.
    let server_stages = match health_addr {
        Some(addr) => crate::stats_client::fetch(addr),
        None => crate::stats_client::Body::Empty,
    };

    print_results(
        "Roundtrip",
        histogram.len() as usize,
        phases,
        &histogram,
        measured_wall,
        &extra_lines,
        json_path,
        &series,
        &health_poller.map(|p| p.stop()).unwrap_or_default(),
        &server_stages,
        pacing_report.as_ref(),
    );
}

/// Run the auth handshake for all connections over smoltcp.
/// Polls until all connections complete Challenge → ChallengeResponse → ServerReady.
fn dpdk_auth_all(
    connections: &mut [DpdkBenchConn],
    device: &mut DpdkDevice,
    iface: &mut Interface,
    sockets: &mut SocketSet<'_>,
    poll_count: &mut u32,
    cached_ts: &mut SmolInstant,
    key: &ed25519_dalek::SigningKey,
) {
    use ed25519_dalek::Signer;
    use melin_protocol::message::Request;

    // Auth states per connection.
    #[derive(PartialEq)]
    enum AuthPhase {
        WaitChallenge,
        WaitServerReady,
        Done,
    }

    let mut phases: Vec<AuthPhase> = connections
        .iter()
        .map(|_| AuthPhase::WaitChallenge)
        .collect();
    let mut recv_buf = [0u8; 512];

    loop {
        // Poll NIC + smoltcp.
        *poll_count = poll_count.wrapping_add(1);
        if poll_count.is_multiple_of(TIMESTAMP_REFRESH_INTERVAL) {
            *cached_ts = SmolInstant::from_millis(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64,
            );
        }
        device.poll_rx();
        iface.poll(*cached_ts, device, sockets);
        device.flush_tx();

        let mut all_done = true;

        for (i, conn) in connections.iter_mut().enumerate() {
            if phases[i] == AuthPhase::Done {
                continue;
            }
            all_done = false;

            let socket = sockets.get_mut::<tcp::Socket>(conn.handle);
            if !socket.can_recv() {
                continue;
            }

            let n = socket.recv_slice(&mut recv_buf).unwrap_or(0);
            if n > 0 {
                conn.parse_buf.extend_from_slice(&recv_buf[..n]);
            }

            // Try to extract a frame.
            if conn.parse_buf.len() < 4 {
                continue;
            }
            let frame_len = u32::from_le_bytes([
                conn.parse_buf[0],
                conn.parse_buf[1],
                conn.parse_buf[2],
                conn.parse_buf[3],
            ]) as usize;
            if conn.parse_buf.len() < 4 + frame_len {
                continue;
            }

            // Borrow payload directly from parse_buf — no allocation needed.
            // Use a cursor approach: process the frame, then compact once.
            let consumed = 4 + frame_len;

            match &phases[i] {
                AuthPhase::WaitChallenge => {
                    let response = codec::decode_response(&conn.parse_buf[4..consumed])
                        .expect("decode Challenge");
                    let nonce = match response {
                        ResponseKind::Challenge { nonce } => nonce,
                        other => panic!("client {i}: expected Challenge, got {other:?}"),
                    };

                    // Sign nonce + ephemerals (DPDK TCP uses zero ephs)
                    // — see `melin_protocol::auth::auth_signing_payload`.
                    let signing_payload = melin_protocol::auth::auth_signing_payload(&nonce);
                    let signature = key.sign(&signing_payload);
                    let request = Request::ChallengeResponse {
                        signature: signature.to_bytes(),
                        public_key: key.verifying_key().to_bytes(),
                    };
                    let mut buf = [0u8; 256];
                    let written = codec::encode_request(&request, 0, &mut buf)
                        .expect("encode ChallengeResponse");

                    let socket = sockets.get_mut::<tcp::Socket>(conn.handle);
                    socket
                        .send_slice(&buf[..written])
                        .expect("send ChallengeResponse");

                    phases[i] = AuthPhase::WaitServerReady;
                }
                AuthPhase::WaitServerReady => {
                    let response = codec::decode_response(&conn.parse_buf[4..consumed])
                        .expect("decode ServerReady");
                    assert!(
                        matches!(response, ResponseKind::ServerReady),
                        "client {i}: expected ServerReady, got {response:?}"
                    );
                    phases[i] = AuthPhase::Done;
                }
                AuthPhase::Done => unreachable!(),
            }

            // Compact parse buffer after processing the frame.
            conn.parse_buf.drain(..consumed);
        }

        if all_done {
            break;
        }
    }

    // Clear parse buffers after auth (they may have leftover bytes).
    for conn in connections.iter_mut() {
        conn.parse_buf.clear();
    }
}
