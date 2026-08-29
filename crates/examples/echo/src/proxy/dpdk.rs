//! The client side over DPDK: TCP in userspace, polled from this thread,
//! on a NIC the kernel never sees.
//!
//! The socket is driven directly -- `send_slice` into it, then the stack
//! polled and the NIC flushed in the same iteration -- rather than through
//! the runtime's transport and its queue. Whatever the loop found in the
//! rings is on the wire before it turns.
//!
//! What the client has to know that the kernel would have found out for
//! it: its own address (`--dpdk-ip`), and the server's MAC. The second is
//! the one to get right. A userspace stack on a port the kernel does not
//! own cannot ARP for the server on every fabric, so the address is seeded
//! into the neighbour cache before the first frame -- from
//! `--dpdk-peer-mac` if given, otherwise derived from the server's IP by
//! the SR-IOV convention. That convention is wrong wherever the port keeps
//! a real hardware address -- an AWS ENI does -- and being wrong is silent:
//! frames go to a MAC nothing owns and the connect times out. On such a
//! fabric the flag is not optional.

use std::io;
use std::net::{Ipv4Addr, SocketAddrV4};

use clap::Args;
use melin_dpdk::device::DpdkDevice;
use melin_dpdk::{Eal, Mempool, PeerMacSource, Port, resolve_peer_mac, try_parse_mac};
use smoltcp::iface::{Config, Interface, SocketHandle, SocketSet};
use smoltcp::socket::tcp::{self, State};
use smoltcp::time::Instant;
use smoltcp::wire::{EthernetAddress, HardwareAddress, IpAddress, IpCidr, Ipv4Address};

use crate::arp;
use crate::transport::Transport;
use crate::tsc::{TscClock, rdtscp};

/// Socket buffers. In flight at any one time is the rate times the round
/// trip -- a few hundred kilobytes at a million a second and a few
/// hundred microseconds -- and a full buffer is not an error here, only a
/// send that takes fewer bytes; this keeps it rare.
const SOCKET_BUFFER: usize = 256 * 1024;
/// Packet buffers for one port at one queue. Plenty for one connection.
const MBUFS: u32 = 8192;
/// How long the TCP handshake may take before the connect is called off.
/// Well past any real handshake; long enough to see a wrong peer MAC as
/// a timeout rather than a hang.
const CONNECT_TIMEOUT_NS: u64 = 5_000_000_000;

#[derive(Args)]
pub struct DpdkArgs {
    /// EAL arguments, space separated, passed straight to `rte_eal_init`.
    #[arg(
        long,
        default_value = "--huge-dir /dev/hugepages --file-prefix echo-bench"
    )]
    pub dpdk_eal_args: String,
    /// DPDK port ID of the NIC to take over.
    #[arg(long, default_value_t = 0)]
    pub dpdk_port: u16,
    /// This client's IPv4 address on the DPDK interface. Must be the one
    /// the fabric assigned to it: a VPC drops a source it does not own.
    #[arg(long)]
    pub dpdk_ip: Option<Ipv4Addr>,
    #[arg(long, default_value_t = 24)]
    pub dpdk_prefix_len: u8,
    /// Default route, for a server outside the prefix.
    #[arg(long)]
    pub dpdk_gateway: Option<Ipv4Addr>,
    /// The server's MAC. See the module documentation for why this is
    /// required on a fabric that assigns addresses itself.
    #[arg(long)]
    pub dpdk_peer_mac: Option<String>,
    /// Must match the server's.
    #[arg(long, default_value_t = 1500)]
    pub dpdk_mtu: usize,
}

pub struct DpdkTcp {
    // Declaration order is drop order: the stack and device go before the
    // port, pool and EAL they were built on.
    sockets: SocketSet<'static>,
    iface: Interface,
    device: DpdkDevice,
    handle: SocketHandle,
    /// The stack's clock, refreshed from the calibrated counter on every
    /// poll: millisecond resolution is all its timers need, and no clock
    /// call is made for it.
    now: Instant,
    _port: Port,
    _mempool: Mempool,
    _eal: Eal,
}

impl DpdkTcp {
    pub fn connect(
        args: &DpdkArgs,
        server: SocketAddrV4,
        clock: &TscClock,
    ) -> Result<Self, String> {
        let local_ip = args
            .dpdk_ip
            .ok_or("--dpdk-ip is required with --transport dpdk")?;
        let peer_mac = args
            .dpdk_peer_mac
            .as_deref()
            .map(try_parse_mac)
            .transpose()
            .map_err(|e| format!("--dpdk-peer-mac: {e}"))?;

        let eal_args: Vec<&str> = args.dpdk_eal_args.split_whitespace().collect();
        let eal = Eal::init(&eal_args).map_err(|e| format!("EAL init failed: {e:?}"))?;
        if args.dpdk_port >= eal.port_count() {
            return Err(format!(
                "DPDK port {} not found ({} available)",
                args.dpdk_port,
                eal.port_count()
            ));
        }

        let mempool = if args.dpdk_mtu > 1500 {
            // u16: the largest MTU any NIC takes fits comfortably.
            Mempool::create_for_mtu("shm-proxy", MBUFS, args.dpdk_mtu as u16, 0)
        } else {
            Mempool::create_with_size("shm-proxy", MBUFS, 0)
        }
        .map_err(|e| format!("mempool: {e:?}"))?;

        let mut port = Port::configure(args.dpdk_port, &mempool, None, 1, false)
            .map_err(|e| format!("port {} configure: {e:?}", args.dpdk_port))?;
        port.start()
            .map_err(|e| format!("port {} start: {e:?}", args.dpdk_port))?;
        let mac = port.mac_addr();

        let mut device = DpdkDevice::new(&[args.dpdk_port], mempool.as_raw(), port.offloads, 0);
        if args.dpdk_mtu != 1500 {
            device.set_mtu(args.dpdk_mtu);
        }

        let mut now = stack_time(clock);
        let mut iface = Interface::new(
            Config::new(HardwareAddress::Ethernet(EthernetAddress(mac))),
            &mut device,
            now,
        );
        let ip = ipv4(local_ip);
        iface.update_ip_addrs(|addrs| {
            addrs
                .push(IpCidr::new(IpAddress::Ipv4(ip), args.dpdk_prefix_len))
                .expect("a fresh interface has room for one address");
        });
        if let Some(gateway) = args.dpdk_gateway {
            iface
                .routes_mut()
                .add_default_ipv4_route(ipv4(gateway))
                .expect("a fresh interface has room for one route");
        }

        // Announce ourselves to the fabric, and seed the server into our
        // own neighbour cache by injecting the reply we would like to
        // have received.
        device.send_raw_frame(&arp::gratuitous_request(mac, local_ip.octets()));
        let (server_mac, source) = resolve_peer_mac(peer_mac, *server.ip());
        device.inject_rx(
            arp::reply(mac, local_ip.octets(), server_mac, server.ip().octets()).to_vec(),
        );
        let mut sockets = SocketSet::new(Vec::new());
        device.poll_rx();
        iface.poll(now, &mut device, &mut sockets);
        device.flush_tx();
        let source = match source {
            PeerMacSource::Supplied => "supplied",
            PeerMacSource::DerivedSrIov => "derived from the IP by the SR-IOV convention",
        };
        eprintln!(
            "DPDK: port {} as {local_ip} ({}), server {server} at {} ({source})",
            args.dpdk_port,
            fmt_mac(mac),
            fmt_mac(server_mac)
        );
        if peer_mac.is_none() {
            eprintln!(
                "warning: no --dpdk-peer-mac; on a fabric that assigns MACs itself (an AWS ENI) \
                 the derived address is wrong and the connect below will time out"
            );
        }

        let rx = tcp::SocketBuffer::new(vec![0u8; SOCKET_BUFFER]);
        let tx = tcp::SocketBuffer::new(vec![0u8; SOCKET_BUFFER]);
        let mut socket = tcp::Socket::new(rx, tx);
        tune(&mut socket);
        // A different ephemeral port per process, so a socket left in
        // TIME_WAIT by the previous run does not collide. The truncation
        // is the point: any value in the range will do.
        let local_port = 49_152 + (std::process::id() as u16 % 8_192);
        socket
            .connect(
                iface.context(),
                (IpAddress::Ipv4(ipv4(*server.ip())), server.port()),
                (IpAddress::Ipv4(ip), local_port),
            )
            .map_err(|e| format!("connect to {server}: {e:?}"))?;
        let handle = sockets.add(socket);

        let deadline = rdtscp().saturating_add(clock.ticks(CONNECT_TIMEOUT_NS));
        loop {
            let tick = rdtscp();
            if tick >= deadline {
                return Err(format!(
                    "TCP connect to {server} over DPDK timed out: no SYN-ACK. \
                     Is the server's DPDK interface up, and is --dpdk-peer-mac its real MAC?"
                ));
            }
            now = stack_time_at(clock, tick);
            device.poll_rx();
            iface.poll(now, &mut device, &mut sockets);
            device.flush_tx();
            match sockets.get_mut::<tcp::Socket>(handle).state() {
                State::Established => break,
                State::Closed | State::TimeWait => {
                    return Err(format!("TCP connect to {server} over DPDK was refused"));
                }
                _ => {}
            }
        }

        Ok(Self {
            sockets,
            iface,
            device,
            handle,
            now,
            _port: port,
            _mempool: mempool,
            _eal: eal,
        })
    }
}

/// The runtime's own low-latency TCP settings, applied to this socket.
fn tune(socket: &mut tcp::Socket<'_>) {
    socket.set_nagle_enabled(false);
    socket.set_ack_delay(None);
    // The stack's retransmit floor. Its estimator is whole milliseconds,
    // so this is as low as it goes; the default is tuned for the
    // internet and would stall a lost segment for tens of milliseconds.
    socket.set_min_rto(smoltcp::time::Duration::from_millis(1));
    socket.set_initial_rto(smoltcp::time::Duration::from_millis(1));
    socket.set_initial_congestion_window(64 * 1024);
}

fn ipv4(addr: Ipv4Addr) -> Ipv4Address {
    let [a, b, c, d] = addr.octets();
    Ipv4Address::new(a, b, c, d)
}

fn fmt_mac(mac: [u8; 6]) -> String {
    mac.iter()
        .map(|b| format!("{b:02x}"))
        .collect::<Vec<_>>()
        .join(":")
}

fn stack_time(clock: &TscClock) -> Instant {
    stack_time_at(clock, rdtscp())
}

/// The stack's notion of now, from the counter. i64 micros: what the
/// stack's `Instant` is made of, and centuries away from overflowing.
#[inline(always)]
fn stack_time_at(clock: &TscClock, tick: u64) -> Instant {
    Instant::from_micros((clock.unix_ns(tick) / 1_000) as i64)
}

impl Transport for DpdkTcp {
    #[inline(always)]
    fn send(&mut self, data: &[u8]) -> io::Result<usize> {
        let socket = self.sockets.get_mut::<tcp::Socket>(self.handle);
        if !socket.can_send() {
            return Ok(0);
        }
        socket
            .send_slice(data)
            .map_err(|e| io::Error::other(format!("send: {e:?}")))
    }

    #[inline(always)]
    fn service(&mut self, now_unix_ns: u64) {
        // Same conversion as `stack_time_at`, without a second counter read.
        self.now = Instant::from_micros((now_unix_ns / 1_000) as i64);
        self.device.poll_rx();
        self.iface
            .poll(self.now, &mut self.device, &mut self.sockets);
        self.device.flush_tx();
    }

    #[inline(always)]
    fn recv(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let socket = self.sockets.get_mut::<tcp::Socket>(self.handle);
        if socket.can_recv() {
            return socket
                .recv_slice(buf)
                .map_err(|e| io::Error::other(format!("recv: {e:?}")));
        }
        if socket.is_active() {
            Ok(0)
        } else {
            Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "the server closed the connection",
            ))
        }
    }

    fn name(&self) -> &'static str {
        "DPDK"
    }
}
