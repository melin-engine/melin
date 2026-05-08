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
use crate::{TimeSeries, maybe_sample, print_results, spawn_progress_reporter};

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
    /// Pre-encoded request frames for this connection.
    frames: Vec<Vec<u8>>,
    /// Next frame index to send.
    send_cursor: usize,
    /// Pending send bytes (partial frame that didn't fit in smoltcp TX buffer).
    send_pending: Vec<u8>,
    /// FIFO of send timestamps (TSC ticks) for in-flight orders.
    /// `u64` instead of `Instant` to avoid ~15-25ns vDSO overhead per
    /// timestamp on the hot path.
    inflight_ts: VecDeque<u64>,
    /// Number of BatchEnd responses received (including warmup).
    batch_count: usize,
    /// Total orders this connection must process.
    total_orders: usize,
    /// True when all responses received.
    done: bool,
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
    total_pairs: usize,
    window: usize,
    num_clients: usize,
    warmup: usize,
    json_path: Option<&std::path::Path>,
    key: &ed25519_dalek::SigningKey,
    num_accounts: u32,
    num_instruments: u32,
    core_id: usize,
    health_addr: Option<std::net::SocketAddr>,
) {
    // Pin to dedicated core.
    if let Err(e) = melin_server::affinity::pin_to_core(core_id) {
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
    let pairs_per_client = total_pairs / num_clients;
    let remainder = total_pairs % num_clients;

    // Pre-generate wire frames for all clients in parallel. Frame
    // generation is pure CPU (no DPDK or smoltcp state), so it can
    // run on a rayon pool while the main thread later drives the
    // single DPDK transport sequentially. This dominates setup
    // time at ~12.6M frames/client.
    use rayon::prelude::*;
    eprintln!("  generating frames for {num_clients} clients...");
    let all_frames: Vec<(Vec<Vec<u8>>, usize)> = (0..num_clients)
        .into_par_iter()
        .map(|client_id| {
            let client_pairs = if client_id == num_clients - 1 {
                pairs_per_client + remainder
            } else {
                pairs_per_client
            };
            let total_orders = warmup + client_pairs * 2;
            let order_id_offset: u64 = (0..client_id)
                .map(|c| {
                    let p = if c == num_clients - 1 {
                        pairs_per_client + remainder
                    } else {
                        pairs_per_client
                    };
                    (warmup + p * 2) as u64
                })
                .sum();
            let mut flow = generator::OrderFlowGenerator::new(generator::GeneratorConfig {
                num_accounts,
                num_instruments,
                start_order_id: order_id_offset + 1,
                ..Default::default()
            });
            // Pre-build wire frames: [u32 LE length][payload].
            // Single send_slice per frame instead of two (prefix + payload).
            let frames: Vec<Vec<u8>> = flow
                .generate_frames(total_orders)
                .into_iter()
                .map(|payload| {
                    let mut wire = Vec::with_capacity(4 + payload.len());
                    wire.extend_from_slice(&(payload.len() as u32).to_le_bytes());
                    wire.extend_from_slice(&payload);
                    wire
                })
                .collect();
            (frames, total_orders)
        })
        .collect();
    eprintln!("  frames generated for all {num_clients} clients");

    // Sequential connect + auth — smoltcp's TCP stack is single-threaded
    // and shared across all sockets via the same `Interface` poll loop.
    // Per-client SYN-then-auth ordering avoids the auth-deadlock risk of
    // sending all SYNs at once and accepting them in arbitrary order.
    let mut connections: Vec<DpdkBenchConn> = Vec::with_capacity(num_clients);
    let setup_start = Instant::now();
    eprintln!("  connecting {num_clients} clients via DPDK...");
    for (client_id, (frames, total_orders)) in all_frames.into_iter().enumerate() {
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
            frames,
            send_cursor: 0,
            send_pending: Vec::new(),
            inflight_ts: VecDeque::with_capacity(window),
            batch_count: 0,
            total_orders,
            done: false,
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
    let total_all_orders: u64 = (warmup * num_clients + total_pairs * 2) as u64;
    let progress = Arc::new(AtomicU64::new(0));
    let progress_shutdown = Arc::new(AtomicBool::new(false));
    let progress_handle = spawn_progress_reporter(
        Arc::clone(&progress),
        total_all_orders,
        Arc::clone(&progress_shutdown),
    );

    let health_poller = health_addr.map(crate::health_poller::HealthPoller::start);

    let ticks_per_ns = crate::calibrate_tsc();
    let start = Instant::now();
    let mut measured_start: Option<Instant> = None;

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
        // feature is off (TraceTimestamp = ()).
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

        let mut all_done = true;

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

            if conn.done {
                continue;
            }
            all_done = false;

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

            // Send new frames while window has room.
            while conn.send_pending.is_empty()
                && conn.inflight_ts.len() < window
                && conn.send_cursor < conn.total_orders
                && socket.can_send()
            {
                // Frames are pre-built with length prefix: [u32 LE len][payload].
                let wire_frame = &conn.frames[conn.send_cursor];
                match socket.send_slice(wire_frame) {
                    Ok(n) if n == wire_frame.len() => {}
                    Ok(n) if n > 0 => {
                        // Partial send — buffer remainder.
                        conn.send_pending.extend_from_slice(&wire_frame[n..]);
                    }
                    Ok(_) => break,
                    Err(_) => break,
                }
                conn.inflight_ts.push_back(crate::rdtscp());
                conn.send_cursor += 1;
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
                    conn.batch_count += 1;
                    progress.fetch_add(1, Ordering::Relaxed);

                    if conn.batch_count > warmup {
                        if measured_start.is_none() {
                            measured_start = Some(Instant::now());
                        }
                        if let Some(sent_tsc) = conn.inflight_ts.pop_front() {
                            let latency_ns =
                                crate::tsc_to_ns(crate::rdtscp() - sent_tsc, ticks_per_ns);
                            histogram.record(latency_ns).ok();
                            interval_hist.record(latency_ns).ok();
                            interval_count += 1;
                            maybe_sample(
                                &mut interval_hist,
                                &mut interval_count,
                                &mut series,
                                start,
                            );
                            #[cfg(feature = "latency-trace")]
                            {
                                work_done_this_iter = true;
                            }
                        }
                    } else {
                        conn.inflight_ts.pop_front();
                    }

                    if conn.batch_count >= conn.total_orders {
                        conn.done = true;
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

        if all_done {
            break;
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
        .map(|s| end.duration_since(s))
        .unwrap_or_else(|| start.elapsed());

    series.sort_by(|a, b| a.elapsed_secs.partial_cmp(&b.elapsed_secs).unwrap());

    let extra_lines = vec![
        format!("  Transport: DPDK (smoltcp)"),
        format!("  DPDK core: {core_id}"),
        format!("  Window: {window}, Clients: {num_clients}"),
    ];

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
        total_pairs * 2,
        warmup * num_clients,
        &histogram,
        measured_wall,
        &extra_lines,
        json_path,
        &series,
        &health_poller.map(|p| p.stop()).unwrap_or_default(),
        &server_stages,
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
                    let (nonce, server_eph) = match response {
                        ResponseKind::Challenge {
                            nonce,
                            server_x25519_eph,
                        } => (nonce, server_x25519_eph),
                        other => panic!("client {i}: expected Challenge, got {other:?}"),
                    };

                    // Sign nonce + ephemerals (DPDK TCP uses zero ephs)
                    // — see `melin_protocol::auth::auth_signing_payload`.
                    let client_x25519_eph = [0u8; 32];
                    let signing_payload = melin_protocol::auth::auth_signing_payload(
                        &nonce,
                        &server_eph,
                        &client_x25519_eph,
                    );
                    let signature = key.sign(&signing_payload);
                    let request = Request::ChallengeResponse {
                        signature: signature.to_bytes(),
                        public_key: key.verifying_key().to_bytes(),
                        client_x25519_eph,
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
