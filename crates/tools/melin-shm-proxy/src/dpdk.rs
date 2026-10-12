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
use std::net::SocketAddrV4;

use clap::Args;
use melin_dpdk::{DpdkArgsError, NicConfig, NicHandle, StreamId};

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

/// The proxy's EAL arguments when `--dpdk-eal-args` is not given (the
/// node's flag defaults to none): a file prefix of its own, so that it
/// can start beside a node, and the usual hugepage mount.
const DEFAULT_EAL_ARGS: &str = "--huge-dir /dev/hugepages --file-prefix shm-proxy";

/// The `--dpdk-*` flags: the node's own declaration (`melin-dpdk`), with
/// the proxy's EAL default, and its single `--dpdk-port`, which predates
/// the list and stays as the harness scripts use it. `--dpdk-ports`
/// replaces it for a bond, the two conflict, and the list has no default
/// here so that an empty one means it was not given: then the port is
/// `--dpdk-port`'s, 0 by default.
// `about = None`: this doc comment is not the proxy's about text.
#[derive(Args, Debug)]
#[command(
    about = None,
    long_about = None,
    mut_arg("dpdk_eal_args", |arg| arg.default_value(DEFAULT_EAL_ARGS)),
    mut_arg("dpdk_ports", |arg| arg.default_value(None::<&'static str>))
)]
pub struct ProxyDpdkArgs {
    #[command(flatten)]
    pub nic: melin_dpdk::DpdkArgs,
    /// DPDK port ID of the NIC to take over.
    #[arg(long, default_value_t = 0, conflicts_with = "dpdk_ports")]
    pub dpdk_port: u16,
}

impl ProxyDpdkArgs {
    /// The flags as the NIC handle's configuration: the node's mapping and
    /// validation, `--dpdk-ip` required, and `--dpdk-port` as the one port
    /// when `--dpdk-ports` is not given.
    fn nic_config(&self) -> Result<NicConfig, DpdkArgsError> {
        if self.nic.ports.is_empty() {
            melin_dpdk::DpdkArgs {
                ports: vec![self.dpdk_port],
                ..self.nic.clone()
            }
            .nic_config()
        } else {
            self.nic.nic_config()
        }
    }
}

pub struct DpdkTcp {
    /// The one stream, dialed on `nic`. Closed with the handle: dropping
    /// the handle resets every stream still open before the NIC goes.
    stream: StreamId,
    nic: NicHandle,
}

impl DpdkTcp {
    pub fn connect(
        args: &ProxyDpdkArgs,
        server: SocketAddrV4,
        clock: &TscClock,
    ) -> Result<Self, String> {
        let config = args.nic_config().map_err(|e| e.to_string())?;
        let mut nic = NicHandle::open(&config).map_err(|e| e.to_string())?;
        // The connect line: where the dial goes, before it goes, since a
        // wrong MAC shows only as a dial that times out.
        eprintln!("{}", nic.connect_line(server));
        if let Some(warning) = nic.next_hop(*server.ip()).warning() {
            eprintln!("warning: {warning}");
        }

        // The loop's clock drives the stack from the first SYN on. A dial
        // that fails is reported with the handle's own message: a timeout
        // names the MAC the SYN went to, the first thing to check.
        let deadline = clock.unix_ns(rdtscp()).saturating_add(CONNECT_TIMEOUT_NS);
        let stream = nic
            .dial(server, SOCKET_BUFFER, deadline, || clock.unix_ns(rdtscp()))
            .map_err(|e| e.to_string())?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Cli;
    use clap::{CommandFactory, Parser};
    use std::net::Ipv4Addr;

    fn parse(extra: &[&str]) -> Result<ProxyDpdkArgs, clap::Error> {
        let mut args = vec!["shm-proxy", "--transport", "dpdk"];
        args.extend_from_slice(extra);
        Cli::try_parse_from(args).map(|cli| cli.dpdk)
    }

    #[test]
    fn the_command_line_is_well_formed() {
        Cli::command().debug_assert();
    }

    /// The proxy's defaults, which the harness relies on: its own EAL file
    /// prefix, and port 0 by `--dpdk-port`.
    #[test]
    fn the_defaults_are_the_proxy_s() {
        let args = parse(&["--dpdk-ip", "10.0.0.2"]).unwrap();
        assert_eq!(args.nic.eal_args, DEFAULT_EAL_ARGS);
        assert_eq!(args.dpdk_port, 0);
        assert!(
            args.nic.ports.is_empty(),
            "--dpdk-ports has no default here"
        );
        let config = args.nic_config().unwrap();
        assert_eq!(
            config.eal_args,
            ["--huge-dir", "/dev/hugepages", "--file-prefix", "shm-proxy"]
        );
        assert_eq!(config.port_ids, [0]);
        assert_eq!(config.ip_addr, Ipv4Addr::new(10, 0, 0, 2));
        assert_eq!(config.prefix_len, 24);
        assert_eq!(config.mtu, 1500);
    }

    #[test]
    fn one_port_or_a_list_but_not_both() {
        let one = parse(&["--dpdk-ip", "10.0.0.2", "--dpdk-port", "3"]).unwrap();
        assert_eq!(one.nic_config().unwrap().port_ids, [3]);
        let list = parse(&["--dpdk-ip", "10.0.0.2", "--dpdk-ports", "1,0"]).unwrap();
        assert_eq!(list.nic_config().unwrap().port_ids, [1, 0]);

        for flags in [
            ["--dpdk-port", "1", "--dpdk-ports", "2"],
            ["--dpdk-ports", "2", "--dpdk-port", "1"],
        ] {
            let error = parse(&flags).unwrap_err();
            assert_eq!(
                error.kind(),
                clap::error::ErrorKind::ArgumentConflict,
                "{flags:?}"
            );
        }
    }

    /// The node's validation and wording, `--dpdk-ip` required.
    #[test]
    fn the_flags_are_checked_by_the_node_s_mapping() {
        let missing = parse(&[]).unwrap().nic_config().unwrap_err();
        assert_eq!(missing.to_string(), "--dpdk-ip is required");

        let bad = parse(&["--dpdk-ip", "10.0.0.2", "--dpdk-peer-mac", "02:99"])
            .unwrap()
            .nic_config()
            .unwrap_err()
            .to_string();
        assert!(bad.starts_with("invalid --dpdk-peer-mac: "), "{bad}");

        assert!(parse(&["--dpdk-gateway-mac", "0c:42:a1:00:00:01"]).is_err());
        assert!(parse(&["--dpdk-eal-args", "--no-pci"]).is_err());
    }
}
