//! What a DPDK transport is configured with: the NIC, and what a node
//! adds on top of it.
//!
//! Plain data with no libdpdk dependency, outside the `dpdk-sys` gate like
//! `mac`: a binary declares its flags and maps them onto these structs
//! whether or not its build has the transport compiled in.

use std::net::Ipv4Addr;

/// TCP listen port for client connections.
const LISTEN_PORT: u16 = 9876;

/// The NIC side of a DPDK transport's configuration: how EAL is brought
/// up, which ports are driven, and how the interface on them is addressed
/// and reaches its peers.
///
/// Nothing here is particular to a node: it is everything a transport on
/// the NIC needs, whether it listens or only dials out. [`DpdkConfig`]
/// composes it with what only a node serving connections has, and a
/// client's NIC handle takes it as it is.
///
/// One field per DPDK flag of the node (`--dpdk-eal-args`,
/// `--dpdk-ports`, `--dpdk-ip`, `--dpdk-prefix-len`, `--dpdk-gateway`,
/// `--dpdk-peer-ip`, `--dpdk-gateway-mac`, `--dpdk-peer-mac`,
/// `--dpdk-mtu`, `--dpdk-vlan`). A plain struct rather than a command-line
/// one: each binary declares its own flags and maps them onto it, and what
/// an operator has to know about a field is written here, once, so the
/// flags' help can stay short.
#[derive(Clone, Debug)]
pub struct NicConfig {
    /// Arguments passed to `rte_eal_init`, one per element (`--dpdk-eal-args`,
    /// split on whitespace). Owned strings because they come from an
    /// operator's flag. Must be empty on a process-wide EAL, which the
    /// transport shares.
    ///
    /// The file prefix (`--file-prefix`) matters wherever two DPDK
    /// processes share a host. A process without one of its own shares the
    /// default runtime directory with every other, and the second to start
    /// fails to take the runtime lock: a client beside a node, the usual
    /// local arrangement, needs a prefix the node does not use. A client's
    /// NIC handle adds one of its own (`melin-client`) when these arguments
    /// name none; a second client on the same host needs yet another.
    pub eal_args: Vec<String>,
    /// DPDK port IDs to poll (`--dpdk-ports`). The first port is used for
    /// TX; all ports are polled for RX. For LACP bonds, pass both VF port
    /// IDs (e.g., `vec![0, 1]`) so traffic arriving on either bond member
    /// is received. A `Vec` because the order carries meaning (the first
    /// transmits) and the count is one or two.
    pub port_ids: Vec<u16>,
    /// This interface's IPv4 address (`--dpdk-ip`). On a cloud fabric, the
    /// one the fabric assigned to the port: a VPC drops a source address it
    /// did not assign.
    pub ip_addr: Ipv4Addr,
    /// The prefix length of [`ip_addr`](Self::ip_addr)'s subnet
    /// (`--dpdk-prefix-len`): a peer inside it is reached directly, one
    /// outside it through [`gateway`](Self::gateway).
    pub prefix_len: u8,
    /// The default route (`--dpdk-gateway`), for a peer outside the
    /// subnet. With [`gateway_mac`](Self::gateway_mac) as well, the
    /// transport runs in L3 mode: the gateway is seeded from that MAC, and
    /// no gratuitous ARP goes out.
    pub gateway: Option<Ipv4Addr>,
    /// Peer IPv4 to capture into DPDK via an `rte_flow` steering rule
    /// (`--dpdk-peer-ip`). When Some, the port is configured in bifurcated mode: kernel
    /// keeps every packet that does NOT come from this source IP, so
    /// the management plane (SSH, ARP, neighbor tenants) keeps working
    /// while DPDK steals only this peer's traffic. Used for L3 setups
    /// where the public NIC is shared with the kernel.
    pub peer_ip: Option<Ipv4Addr>,
    /// MAC address to seed into smoltcp's neighbor cache for the
    /// gateway IP (`--dpdk-gateway-mac`). Required in bifurcated L3 mode because the gateway's
    /// ARP reply would not match our flow rule (different source IP) —
    /// it would be eaten by the kernel. Operators source this MAC from
    /// the kernel's existing `ip neigh` cache and pass it in.
    pub gateway_mac: Option<[u8; 6]>,
    /// MAC address of the peer this transport dials out to
    /// (`--dpdk-peer-mac`): the primary, for a replica; the node, for a
    /// client. Only a connection *initiator* needs it: an acceptor learns
    /// the peer's address from the inbound frame.
    ///
    /// The address is seeded into the neighbour cache before the first
    /// frame rather than resolved by ARP, which a userspace stack on a port
    /// the kernel does not own cannot rely on (in bifurcated mode ARP never
    /// reaches DPDK at all). When None the initiator falls back to the
    /// SR-IOV `02:00:<ip>` convention, which holds only where the VFs were
    /// given such addresses (`dpdk-setup.sh` does) and is wrong on any port
    /// that keeps an address the fabric or the hardware assigned: an AWS
    /// ENI, an mlx5 in bifurcated mode sharing the kernel netdev's. Being
    /// wrong is silent, the frames go to a MAC nothing owns and the
    /// connect times out, so on such a fabric this field is not optional.
    /// See [`crate::mac::resolve_peer_mac`].
    pub peer_mac: Option<[u8; 6]>,
    /// MTU for the DPDK interface (`--dpdk-mtu`); must match the peer's.
    /// 1500 for standard Ethernet, 9000 for jumbo frames (6x fewer TCP
    /// segments, ~6x less per-segment overhead). A `usize` because that is
    /// what the stack's device capabilities take.
    pub mtu: usize,
    /// VLAN ID for hardware strip/insert (`--dpdk-vlan`). Used in dedicated NIC mode where
    /// the kernel isn't handling VLAN tags. None = no VLAN offload (SR-IOV
    /// mode where the PF handles VLAN tagging).
    pub vlan_id: Option<u16>,
}

impl Default for NicConfig {
    fn default() -> Self {
        NicConfig {
            eal_args: Vec::new(),
            port_ids: vec![0],
            ip_addr: Ipv4Addr::new(10, 0, 0, 1),
            prefix_len: 24,
            gateway: None,
            peer_ip: None,
            gateway_mac: None,
            peer_mac: None,
            mtu: 1500,
            vlan_id: None,
        }
    }
}

/// Configuration for the DPDK transport of a node: its NIC, the port it
/// listens on, and how many queue pairs it polls.
#[derive(Clone, Debug)]
pub struct DpdkConfig {
    /// The NIC the node's transport runs on.
    pub nic: NicConfig,
    pub listen_port: u16,
    /// Number of RX/TX queue pairs per port. Each queue pair is polled
    /// by a separate thread. When > 1, RSS is enabled on the NIC to
    /// distribute TCP/IP flows across queues. Default: 1.
    pub num_queues: u16,
}

impl Default for DpdkConfig {
    fn default() -> Self {
        DpdkConfig {
            nic: NicConfig::default(),
            listen_port: LISTEN_PORT,
            num_queues: 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The defaults the node's configuration had before its NIC fields
    /// moved into a struct of their own, field by field.
    #[test]
    fn the_defaults_are_the_node_config_defaults() {
        let config = DpdkConfig::default();
        assert_eq!(config.listen_port, 9876);
        assert_eq!(config.num_queues, 1);

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
        } = config.nic;
        assert!(eal_args.is_empty());
        assert_eq!(port_ids, vec![0]);
        assert_eq!(ip_addr, Ipv4Addr::new(10, 0, 0, 1));
        assert_eq!(prefix_len, 24);
        assert_eq!(gateway, None);
        assert_eq!(peer_ip, None);
        assert_eq!(gateway_mac, None);
        assert_eq!(peer_mac, None);
        assert_eq!(mtu, 1500);
        assert_eq!(vlan_id, None);
    }
}
