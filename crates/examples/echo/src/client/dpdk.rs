//! The echo client over DPDK: the dial, and its `--dpdk-*` flags.
//!
//! The flags are the node's own declaration, `melin-dpdk`'s `DpdkArgs`,
//! which the client flattens into its command line, with the node's
//! validation and mapping onto `NicConfig` (re-exported by `melin-client`).
//! What an operator has to know about each (why the peer MAC is not
//! optional on a fabric that assigns MACs, why a client beside a node
//! needs an EAL file prefix of its own) is written on `NicConfig`'s
//! fields. The client differs from the node in one default only: it has
//! no address of its own to assume, so `--dpdk-ip` is required.
//!
//! The connection owns its NIC: the client makes one connection and exits,
//! so EAL is initialised once and cleaned up with it.

use std::net::{SocketAddr, SocketAddrV4};

use melin_client::{
    DEFAULT_TIMEOUT, DpdkStream, NicConfig, NicHandle, SigningKey, StreamConnection, fmt_mac,
};

use melin_dpdk::DpdkArgs;

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
    let config = args.nic_config().map_err(|e| e.to_string())?;
    let nic = NicHandle::open(&config).map_err(melin_client::Error::Nic)?;
    announce(&nic, &config, server);
    let connection =
        StreamConnection::connect_dpdk_on(nic, server, key, SOCKET_BUFFER, DEFAULT_TIMEOUT)?;
    Ok(connection)
}

/// The proxy's connect line, on stderr: the port, the client's address and
/// MAC, and where the dial goes (the server's MAC, or the gateway's) with
/// where that came from, as the NIC handle says it. A warning when no peer
/// MAC was given and the convention's stands in for it.
fn announce(nic: &NicHandle, config: &NicConfig, server: SocketAddrV4) {
    let port = config
        .port_ids
        .first()
        .map_or_else(|| "?".to_owned(), u16::to_string);
    let next_hop = nic.next_hop(*server.ip());
    eprintln!(
        "DPDK: port {port} as {} ({}), server {server} {next_hop}",
        config.ip_addr,
        fmt_mac(nic.mac())
    );
    if let Some(warning) = next_hop.warning() {
        eprintln!("warning: {warning}");
    }
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

    /// The client's flags are the node's (whose mapping `melin-dpdk`
    /// tests), flattened with no default changed: the address is required,
    /// and the rest default as the node's do.
    #[test]
    fn the_flags_are_the_node_s_with_the_address_required() {
        let cli = parse(&["--dpdk-ip", "10.0.0.2", "--dpdk-ports=0,1"]).unwrap();
        let config = cli.dpdk.nic_config().unwrap();
        assert_eq!(config.ip_addr, std::net::Ipv4Addr::new(10, 0, 0, 2));
        assert_eq!(config.port_ids, [0, 1]);
        assert_eq!(
            DpdkArgs {
                ip: None,
                ports: vec![0],
                ..cli.dpdk
            },
            DpdkArgs::default()
        );

        let missing = parse(&[]).unwrap().dpdk.nic_config().unwrap_err();
        assert_eq!(missing.to_string(), "--dpdk-ip is required");

        let bad = parse(&["--dpdk-ip", "10.0.0.2", "--dpdk-peer-mac", "02:99"])
            .unwrap()
            .dpdk
            .nic_config()
            .unwrap_err()
            .to_string();
        assert!(bad.starts_with("invalid --dpdk-peer-mac: "), "{bad}");

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
}
