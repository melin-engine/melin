//! The client side over DPDK: TCP in userspace, polled from this thread,
//! on a NIC the kernel never sees.
//!
//! The NIC is `melin-dpdk`'s client NIC handle: the node's own transport,
//! listening on no port, with one stream dialed on it. The stream's send
//! goes straight into its socket, and the service step polls the device,
//! runs the stack and flushes the NIC in the same iteration, so whatever
//! the loop found in the rings is on the wire before it turns. The step
//! runs on this proxy's calibrated clock: the loop already reads it, and
//! passes the reading on.
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
//! fabric the flag is not optional. Behind a gateway that shares the port
//! with the kernel (`--dpdk-gateway-mac`), the gateway's MAC is seeded
//! instead, and that is the one to get right.

use std::io;
use std::net::{Ipv4Addr, SocketAddrV4};

use clap::Args;
use melin_dpdk::{NicConfig, NicHandle, StreamId, fmt_mac, try_parse_mac};

use crate::transport::Transport;
use crate::tsc::{TscClock, rdtscp};

/// Socket buffers. In flight at any one time is the rate times the round
/// trip -- a few hundred kilobytes at a million a second and a few
/// hundred microseconds -- and a full buffer is not an error here, only a
/// send that takes fewer bytes; this keeps it rare.
const SOCKET_BUFFER: usize = 256 * 1024;
/// How long the TCP handshake may take before the connect is called off.
/// Well past any real handshake; long enough to see a wrong peer MAC as
/// a timeout rather than a hang.
const CONNECT_TIMEOUT_NS: u64 = 5_000_000_000;

#[derive(Args)]
pub struct DpdkArgs {
    /// EAL arguments, space separated, passed straight to `rte_eal_init`.
    /// The `=` is required, same as the server's flag: the value starts
    /// with a dash, and requiring the joined form keeps a forgotten value
    /// from silently swallowing the next flag as the EAL string.
    #[arg(
        long,
        require_equals = true,
        default_value = "--huge-dir /dev/hugepages --file-prefix shm-proxy"
    )]
    pub dpdk_eal_args: String,
    /// DPDK port ID of the NIC to take over.
    #[arg(long, default_value_t = 0)]
    pub dpdk_port: u16,
    /// DPDK port IDs (comma-separated), in place of `--dpdk-port`, as the
    /// server's flag: for an LACP bond, both ports, so traffic arriving on
    /// either member is received. The first one transmits.
    #[arg(long, value_delimiter = ',', conflicts_with = "dpdk_port")]
    pub dpdk_ports: Vec<u16>,
    /// This client's IPv4 address on the DPDK interface. Must be the one
    /// the fabric assigned to it: a VPC drops a source it does not own.
    #[arg(long)]
    pub dpdk_ip: Option<Ipv4Addr>,
    #[arg(long, default_value_t = 24)]
    pub dpdk_prefix_len: u8,
    /// Default route, for a server outside the prefix.
    #[arg(long)]
    pub dpdk_gateway: Option<Ipv4Addr>,
    /// Peer IPv4 for the bifurcated `rte_flow` steering rule, as the
    /// server's flag: the port opens in isolated mode and only IPv4
    /// packets from this address are delivered into DPDK; everything else
    /// stays with the kernel netdev. For a port shared with the kernel.
    #[arg(long)]
    pub dpdk_peer_ip: Option<Ipv4Addr>,
    /// The `--dpdk-gateway`'s MAC (aa:bb:cc:dd:ee:ff), for the L3 mode of a
    /// port shared with the kernel, as the server's flag: the gateway is
    /// seeded with it and no gratuitous ARP goes out. Required there
    /// because the gateway's ARP reply would not match the steering rule
    /// and would be eaten by the kernel. Source it from `ip neigh`.
    #[arg(long, requires = "dpdk_gateway")]
    pub dpdk_gateway_mac: Option<String>,
    /// The server's MAC. See the module documentation for why this is
    /// required on a fabric that assigns addresses itself.
    #[arg(long)]
    pub dpdk_peer_mac: Option<String>,
    /// Must match the server's.
    #[arg(long, default_value_t = 1500)]
    pub dpdk_mtu: usize,
    /// VLAN ID for hardware strip and insert, as the server's flag: for a
    /// dedicated NIC whose VLAN tags the kernel does not handle. Not
    /// needed on an SR-IOV VF, whose PF tags.
    #[arg(long)]
    pub dpdk_vlan: Option<u16>,
}

pub struct DpdkTcp {
    /// The one stream, dialed on `nic`. Closed with the handle: dropping
    /// the handle resets every stream still open before the NIC goes.
    stream: StreamId,
    nic: NicHandle,
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
        let gateway_mac = args
            .dpdk_gateway_mac
            .as_deref()
            .map(try_parse_mac)
            .transpose()
            .map_err(|e| format!("--dpdk-gateway-mac: {e}"))?;
        let port_ids = if args.dpdk_ports.is_empty() {
            vec![args.dpdk_port]
        } else {
            args.dpdk_ports.clone()
        };
        let first_port = port_ids[0];

        let mut nic = NicHandle::open(&NicConfig {
            eal_args: args
                .dpdk_eal_args
                .split_whitespace()
                .map(String::from)
                .collect(),
            port_ids,
            ip_addr: local_ip,
            prefix_len: args.dpdk_prefix_len,
            gateway: args.dpdk_gateway,
            peer_ip: args.dpdk_peer_ip,
            gateway_mac,
            peer_mac,
            mtu: args.dpdk_mtu,
            vlan_id: args.dpdk_vlan,
        })
        .map_err(|e| e.to_string())?;
        // The connect line: where the dial goes, before it goes, since a
        // wrong MAC shows only as a dial that times out.
        let next_hop = nic.next_hop(*server.ip());
        eprintln!(
            "DPDK: port {first_port} as {local_ip} ({}), server {server} {next_hop}",
            fmt_mac(nic.mac())
        );
        if let Some(warning) = next_hop.warning() {
            eprintln!("warning: {warning}");
        }

        // The loop's clock drives the stack from the first SYN on.
        let deadline = clock.unix_ns(rdtscp()).saturating_add(CONNECT_TIMEOUT_NS);
        let stream = nic
            .dial(server, SOCKET_BUFFER, deadline, || clock.unix_ns(rdtscp()))
            .map_err(|e| match e.kind() {
                // Behind a gateway, the handle's own message, which names
                // the gateway's MAC, is the one to read.
                io::ErrorKind::TimedOut if gateway_mac.is_none() => format!(
                    "TCP connect to {server} over DPDK timed out: no SYN-ACK. \
                     Is the server's DPDK interface up, and is --dpdk-peer-mac its real MAC?"
                ),
                io::ErrorKind::ConnectionRefused => {
                    format!("TCP connect to {server} over DPDK was refused")
                }
                _ => e.to_string(),
            })?;
        Ok(Self { stream, nic })
    }
}

impl Transport for DpdkTcp {
    #[inline(always)]
    fn send(&mut self, data: &[u8]) -> io::Result<usize> {
        self.nic.stream(&self.stream).send(data)
    }

    #[inline(always)]
    fn service(&mut self, now_unix_ns: u64) {
        self.nic.service(now_unix_ns);
    }

    #[inline(always)]
    fn recv(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.nic.stream(&self.stream).recv(buf)
    }

    fn name(&self) -> &'static str {
        "DPDK"
    }
}
