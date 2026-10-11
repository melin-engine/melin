//! The echo client over DPDK: its `--dpdk-*` flags, and the dial.
//!
//! The flags are the node's and the shm-proxy's, one per field of
//! `NicConfig` (re-exported by `melin-client`), and mapped onto it field
//! for field. What an operator has to know about each (why the peer MAC is
//! not optional on a fabric that assigns MACs, why a client beside a node
//! needs an EAL file prefix of its own) is written on `NicConfig`'s
//! fields, so the help here stays short.
//!
//! The ports are `--dpdk-ports`, a list, as the node's flag and the
//! Exchange Core bench's: the field is a list (an LACP bond is two ports).
//! The proxy's single `--dpdk-port` predates the list and is not repeated.
//!
//! The connection owns its NIC: the client makes one connection and exits,
//! so EAL is initialised once and cleaned up with it.

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use clap::Args;
use melin_client::{
    DEFAULT_TIMEOUT, DpdkStream, NicConfig, NicHandle, PeerMacSource, SigningKey, StreamConnection,
    try_parse_mac,
};

use crate::Error;

/// The stream's receive and transmit buffers, in bytes. One request is in
/// flight at a time, at most `MAX_PAYLOAD` bytes and its header, and its
/// reply is one echo and a batch end: a few hundred bytes each way. 64 KiB
/// is room to spare, and the size the round trip's DPDK client runs with.
/// Not the proxy's 256 KiB, which covers a pipelined rate times a round
/// trip, never in flight in a closed loop of one. The transmit ring wraps
/// every few hundred requests; a request that meets the wrap is framed in
/// the connection's scratch buffer and copied, a copy of a few hundred
/// bytes that the larger buffer would only make rarer.
const SOCKET_BUFFER: usize = 64 * 1024;

#[derive(Args)]
pub struct DpdkArgs {
    /// EAL arguments, space separated, passed to `rte_eal_init`. The `=`
    /// is required, as on the node: the value starts with a dash. Without
    /// a `--file-prefix`, the client's own (`melin-client`) is added.
    #[arg(long, default_value = "", require_equals = true)]
    pub dpdk_eal_args: String,
    /// DPDK port IDs, comma separated; the first transmits. Two for an
    /// LACP bond.
    // A `Vec`, as the field it maps onto: ordered, one or two entries.
    #[arg(long, default_value = "0", value_delimiter = ',')]
    pub dpdk_ports: Vec<u16>,
    /// This client's IPv4 address on the DPDK interface (required with
    /// `--transport dpdk`): on a cloud fabric, the one it assigned.
    #[arg(long)]
    pub dpdk_ip: Option<Ipv4Addr>,
    /// The prefix length of `--dpdk-ip`'s subnet.
    #[arg(long, default_value_t = 24)]
    pub dpdk_prefix_len: u8,
    /// Default route, for a server outside the subnet.
    #[arg(long)]
    pub dpdk_gateway: Option<Ipv4Addr>,
    /// Peer IPv4 for the bifurcated `rte_flow` steering rule, on a port
    /// shared with the kernel: only this source's packets reach DPDK.
    #[arg(long)]
    pub dpdk_peer_ip: Option<Ipv4Addr>,
    /// The `--dpdk-gateway`'s MAC (aa:bb:cc:dd:ee:ff), for the L3 mode of a
    /// port shared with the kernel. Source it from `ip neigh`.
    #[arg(long, requires = "dpdk_gateway")]
    pub dpdk_gateway_mac: Option<String>,
    /// The server's MAC (aa:bb:cc:dd:ee:ff). Not optional where the fabric
    /// assigns MACs (an AWS ENI, a port shared with the kernel): without
    /// it the SR-IOV convention is assumed, and the dial times out.
    #[arg(long)]
    pub dpdk_peer_mac: Option<String>,
    /// MTU of the DPDK interface; must match the server's.
    #[arg(long, default_value_t = 1500)]
    pub dpdk_mtu: usize,
    /// VLAN ID for hardware strip and insert, on a dedicated NIC. Not on an
    /// SR-IOV VF, whose PF tags.
    #[arg(long)]
    pub dpdk_vlan: Option<u16>,
}

impl DpdkArgs {
    /// The first `--dpdk-*` flag set away from its default, if any: what a
    /// kernel run refuses, since it would ignore the flag.
    ///
    /// The defaults compared against are `NicConfig`'s, which the flags'
    /// own are (the tests hold them together). A flag given at its default
    /// value cannot be told from one not given, and changes nothing.
    pub fn first_given(&self) -> Option<&'static str> {
        let node = NicConfig::default();
        let given = [
            (
                "--dpdk-eal-args",
                self.dpdk_eal_args.split_whitespace().next().is_some(),
            ),
            ("--dpdk-ports", self.dpdk_ports != node.port_ids),
            ("--dpdk-ip", self.dpdk_ip.is_some()),
            ("--dpdk-prefix-len", self.dpdk_prefix_len != node.prefix_len),
            ("--dpdk-gateway", self.dpdk_gateway.is_some()),
            ("--dpdk-peer-ip", self.dpdk_peer_ip.is_some()),
            ("--dpdk-gateway-mac", self.dpdk_gateway_mac.is_some()),
            ("--dpdk-peer-mac", self.dpdk_peer_mac.is_some()),
            ("--dpdk-mtu", self.dpdk_mtu != node.mtu),
            ("--dpdk-vlan", self.dpdk_vlan.is_some()),
        ];
        given
            .into_iter()
            .find_map(|(flag, given)| given.then_some(flag))
    }

    /// The flags as the NIC handle's configuration, field for field.
    ///
    /// A struct literal, not `..Default::default()`: a field `NicConfig`
    /// gains is a flag this client lacks, and should fail the build here.
    fn nic_config(&self) -> Result<NicConfig, String> {
        let ip_addr = self
            .dpdk_ip
            .ok_or("--dpdk-ip is required with --transport dpdk")?;
        let gateway_mac = self
            .dpdk_gateway_mac
            .as_deref()
            .map(try_parse_mac)
            .transpose()
            .map_err(|e| format!("--dpdk-gateway-mac: {e}"))?;
        let peer_mac = self
            .dpdk_peer_mac
            .as_deref()
            .map(try_parse_mac)
            .transpose()
            .map_err(|e| format!("--dpdk-peer-mac: {e}"))?;
        Ok(NicConfig {
            eal_args: self
                .dpdk_eal_args
                .split_whitespace()
                .map(String::from)
                .collect(),
            port_ids: self.dpdk_ports.clone(),
            ip_addr,
            prefix_len: self.dpdk_prefix_len,
            gateway: self.dpdk_gateway,
            peer_ip: self.dpdk_peer_ip,
            gateway_mac,
            peer_mac,
            mtu: self.dpdk_mtu,
            vlan_id: self.dpdk_vlan,
        })
    }
}

/// The server a DPDK dial can reach: IPv4, as the transport is, and not
/// loopback. `--server` defaults to loopback, which a kernel socket
/// reaches and a NIC never does; left to the dial, that default would
/// wait out the timeout and then blame the peer MAC.
pub fn check_server(server: SocketAddr) -> Result<SocketAddrV4, Error> {
    let server = match server {
        SocketAddr::V4(v4) => v4,
        SocketAddr::V6(_) => return Err("the DPDK transport is IPv4 only".into()),
    };
    if server.ip().is_loopback() {
        return Err(format!(
            "--server {server}: a DPDK NIC cannot reach loopback; give the node's address on \
             the DPDK network"
        )
        .into());
    }
    Ok(server)
}

/// Bring the NIC up from `args`, dial `server` on it and authenticate:
/// the DPDK twin of `Connection::connect`, with the same timeout for the
/// dial, the handshake and every read after.
///
/// What `StreamConnection::connect_dpdk` does, in its two steps, so that
/// the line saying which MAC the dial goes to is on stderr while the dial
/// waits, as the proxy's is: a wrong MAC is silent until the dial times
/// out. The connection owns the NIC either way.
pub fn connect(
    args: &DpdkArgs,
    server: SocketAddrV4,
    key: &SigningKey,
) -> Result<StreamConnection<DpdkStream>, Error> {
    let config = args.nic_config()?;
    let nic = NicHandle::open(&config).map_err(melin_client::Error::Nic)?;
    announce(&nic, &config, server);
    let connection =
        StreamConnection::connect_dpdk_on(nic, server, key, SOCKET_BUFFER, DEFAULT_TIMEOUT)?;
    Ok(connection)
}

/// The proxy's connect line, on stderr: the port, the client's address and
/// MAC, and the MAC the dial will address the server (or the gateway) by,
/// with where it came from. A warning when no peer MAC was given and the
/// convention's stands in for it.
fn announce(nic: &NicHandle, config: &NicConfig, server: SocketAddrV4) {
    let port = config
        .port_ids
        .first()
        .map_or_else(|| "?".to_owned(), u16::to_string);
    let local_ip = config.ip_addr;
    let mac = fmt_mac(nic.mac());
    match config.gateway.zip(config.gateway_mac) {
        Some((gateway, gateway_mac)) => eprintln!(
            "DPDK: port {port} as {local_ip} ({mac}), server {server} via gateway {gateway} \
             at {} (supplied)",
            fmt_mac(gateway_mac)
        ),
        None => {
            let (server_mac, source) = nic.peer_mac(*server.ip());
            let source = match source {
                PeerMacSource::Supplied => "supplied",
                PeerMacSource::DerivedSrIov => "derived from the IP by the SR-IOV convention",
            };
            eprintln!(
                "DPDK: port {port} as {local_ip} ({mac}), server {server} at {} ({source})",
                fmt_mac(server_mac)
            );
            if config.peer_mac.is_none() {
                eprintln!(
                    "warning: no --dpdk-peer-mac; on a fabric that assigns MACs itself (an AWS \
                     ENI) the derived address is wrong and the connect below will time out"
                );
            }
        }
    }
}

/// `aa:bb:cc:dd:ee:ff`.
fn fmt_mac(mac: [u8; 6]) -> String {
    mac.iter()
        .map(|b| format!("{b:02x}"))
        .collect::<Vec<_>>()
        .join(":")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Cli;
    use clap::Parser;

    fn parse(extra: &[&str]) -> Result<Cli, clap::Error> {
        let mut args = vec!["echo-client", "--key", "k.pem", "--transport", "dpdk"];
        args.extend_from_slice(extra);
        Cli::try_parse_from(args)
    }

    /// Every flag lands on its own field, and the defaults are the node's.
    #[test]
    fn the_flags_map_onto_the_nic_config_one_to_one() {
        let cli = parse(&[
            "--dpdk-eal-args=--vdev=net_tap0 --no-pci --file-prefix=x",
            "--dpdk-ports",
            "0,1",
            "--dpdk-ip",
            "10.0.0.2",
            "--dpdk-prefix-len",
            "16",
            "--dpdk-gateway",
            "10.0.255.254",
            "--dpdk-peer-ip",
            "10.0.0.1",
            "--dpdk-gateway-mac",
            "0c:42:a1:00:00:01",
            "--dpdk-peer-mac",
            "02:99:01:00:00:01",
            "--dpdk-mtu",
            "9000",
            "--dpdk-vlan",
            "42",
        ])
        .unwrap();
        let NicConfig {
            eal_args,
            port_ids,
            ip_addr,
            prefix_len,
            gateway,
            peer_ip,
            gateway_mac,
            peer_mac,
            mtu,
            vlan_id,
        } = cli.dpdk.nic_config().unwrap();
        assert_eq!(eal_args, ["--vdev=net_tap0", "--no-pci", "--file-prefix=x"]);
        assert_eq!(port_ids, [0, 1]);
        assert_eq!(ip_addr, Ipv4Addr::new(10, 0, 0, 2));
        assert_eq!(prefix_len, 16);
        assert_eq!(gateway, Some(Ipv4Addr::new(10, 0, 255, 254)));
        assert_eq!(peer_ip, Some(Ipv4Addr::new(10, 0, 0, 1)));
        assert_eq!(gateway_mac, Some([0x0c, 0x42, 0xa1, 0, 0, 1]));
        assert_eq!(peer_mac, Some([0x02, 0x99, 0x01, 0, 0, 1]));
        assert_eq!(mtu, 9000);
        assert_eq!(vlan_id, Some(42));

        let defaults = parse(&["--dpdk-ip", "10.0.0.2"])
            .unwrap()
            .dpdk
            .nic_config()
            .unwrap();
        let node = NicConfig::default();
        assert_eq!(defaults.eal_args, node.eal_args);
        assert_eq!(defaults.port_ids, node.port_ids);
        assert_eq!(defaults.prefix_len, node.prefix_len);
        assert_eq!(defaults.mtu, node.mtu);
        assert_eq!(defaults.gateway, None);
        assert_eq!(defaults.peer_mac, None);
        assert_eq!(defaults.vlan_id, None);
    }

    #[test]
    fn the_address_is_required_and_the_macs_are_checked() {
        let missing = parse(&[]).unwrap().dpdk.nic_config().unwrap_err();
        assert!(missing.contains("--dpdk-ip"), "{missing}");

        let bad = parse(&["--dpdk-ip", "10.0.0.2", "--dpdk-peer-mac", "02:99"])
            .unwrap()
            .dpdk
            .nic_config()
            .unwrap_err();
        assert!(bad.starts_with("--dpdk-peer-mac: "), "{bad}");

        // The gateway's MAC means nothing without the gateway.
        assert!(parse(&["--dpdk-gateway-mac", "0c:42:a1:00:00:01"]).is_err());
        // The joined form only, so a forgotten value cannot swallow a flag.
        assert!(parse(&["--dpdk-eal-args", "--no-pci"]).is_err());
    }

    /// The default `--server` is loopback, which is refused before the NIC
    /// is brought up, as is an IPv6 address.
    #[test]
    fn the_server_must_be_one_a_nic_can_reach() {
        let loopback = check_server("127.0.0.1:9876".parse().unwrap()).unwrap_err();
        assert!(loopback.to_string().contains("loopback"), "{loopback}");
        let v6 = check_server("[::1]:9876".parse().unwrap()).unwrap_err();
        assert!(v6.to_string().contains("IPv4"), "{v6}");
        assert_eq!(
            check_server("10.0.0.1:9876".parse().unwrap()).unwrap(),
            "10.0.0.1:9876".parse::<SocketAddrV4>().unwrap()
        );
    }

    #[test]
    fn macs_print_as_colon_separated_hex() {
        assert_eq!(fmt_mac([0x02, 0x99, 0x01, 0, 0, 0xff]), "02:99:01:00:00:ff");
    }
}
