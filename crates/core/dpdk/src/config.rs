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
/// composes it with what only a node serving connections has.
#[derive(Clone, Debug)]
pub struct NicConfig {
    /// Arguments passed to `rte_eal_init`, one per element. Owned strings
    /// because they come from an operator's flag, split on whitespace.
    /// Must be empty on a process-wide EAL, which the transport shares.
    pub eal_args: Vec<String>,
    /// DPDK port IDs to poll. The first port is used for TX; all ports
    /// are polled for RX. For LACP bonds, pass both VF port IDs (e.g.,
    /// `vec![0, 1]`) so traffic arriving on either bond member is received.
    /// A `Vec` because the order carries meaning (the first transmits) and
    /// the count is one or two.
    pub port_ids: Vec<u16>,
    pub ip_addr: Ipv4Addr,
    pub prefix_len: u8,
    pub gateway: Option<Ipv4Addr>,
    /// Peer IPv4 to capture into DPDK via an `rte_flow` steering rule.
    /// When Some, the port is configured in bifurcated mode: kernel
    /// keeps every packet that does NOT come from this source IP, so
    /// the management plane (SSH, ARP, neighbor tenants) keeps working
    /// while DPDK steals only this peer's traffic. Used for L3 setups
    /// where the public NIC is shared with the kernel.
    pub peer_ip: Option<Ipv4Addr>,
    /// MAC address to seed into smoltcp's neighbor cache for the
    /// gateway IP. Required in bifurcated L3 mode because the gateway's
    /// ARP reply would not match our flow rule (different source IP) —
    /// it would be eaten by the kernel. Operators source this MAC from
    /// the kernel's existing `ip neigh` cache and pass it in.
    pub gateway_mac: Option<[u8; 6]>,
    /// MAC address of the peer this transport dials out to — the primary,
    /// for a replica. Only a connection *initiator* needs it: an acceptor
    /// learns the peer's address from the inbound frame. When None the
    /// initiator falls back to the SR-IOV `02:00:<ip>` convention, which
    /// is wrong on any port that keeps a real hardware address (an mlx5
    /// in bifurcated mode shares the kernel netdev's). See
    /// [`crate::mac::resolve_peer_mac`].
    pub peer_mac: Option<[u8; 6]>,
    /// MTU for the DPDK interface. 1500 for standard Ethernet, 9000 for
    /// jumbo frames (6x fewer TCP segments, ~6x less per-segment overhead).
    /// A `usize` because that is what the stack's device capabilities take.
    pub mtu: usize,
    /// VLAN ID for hardware strip/insert. Used in dedicated NIC mode where
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
