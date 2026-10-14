//! The node's `--dpdk-replication-*` flags: where its replication runs on
//! DPDK, and their mapping onto a [`ReplicationNic`].
//!
//! Node-only, so declared here rather than in `melin-dpdk`'s `DpdkArgs`,
//! which every client flattens too. Plain data with no libdpdk dependency,
//! declared and mapped in every build like the `--dpdk-*` flags, so the
//! mapping is tested on any host; only a DPDK node consumes it.

// Without `dpdk` nothing but the tests maps the flags: the kernel-TCP
// node ignores the DPDK flags, as it does `--dpdk-*`.
#![cfg_attr(not(feature = "dpdk"), allow(dead_code))]

use std::net::Ipv4Addr;

use melin_dpdk::{
    DpdkArgsError, NicConfig, ReplicationNic, ReplicationPorts, SteeringError, parse_mac_flag,
};

/// The replication flags, flattened into the node's command line.
///
/// The help is kept short: what each mode is for is written on
/// [`ReplicationNic`]'s variants. With none of them given, replication
/// runs on the client queue and its poll thread, as it always has.
///
/// Which combinations are refused is decided by the mapping alone
/// ([`replication_nic`](Self::replication_nic)), not also by clap: one
/// check and one wording, whether the value came from the command line or
/// was built in code.
// `about = None`, as `DpdkArgs`: a flattened struct's doc comment must not
// become the about text of the node's command line.
#[derive(clap::Args, Debug, Clone, Default, PartialEq, Eq)]
#[command(about = None, long_about = None)]
pub struct DpdkReplicationArgs {
    /// Run replication on a second queue of the client NIC's ports,
    /// steered to it by the NIC (mlx5). Requires --replication-bind (on a
    /// port other than --bind's) or --replica-of. Not with the port-mode
    /// flags.
    #[arg(long = "dpdk-replication-queue", id = "dpdk_replication_queue")]
    pub queue: bool,

    /// DPDK port IDs (comma-separated) of a NIC of replication's own
    /// (a second ENI, a second port). Requires --dpdk-replication-ip;
    /// must not be one of --dpdk-ports.
    // A `Vec` with no default, as `--dpdk-ports` is one: ordered (the
    // first transmits), one or two entries. Empty is "not given": an empty
    // value does not parse.
    #[arg(
        long = "dpdk-replication-ports",
        id = "dpdk_replication_ports",
        value_name = "DPDK_REPLICATION_PORTS",
        value_delimiter = ','
    )]
    pub ports: Vec<u16>,

    /// IPv4 address of the replication NIC, in the client NIC's subnet
    /// and other than --dpdk-ip. Requires --dpdk-replication-ports.
    #[arg(
        long = "dpdk-replication-ip",
        id = "dpdk_replication_ip",
        value_name = "DPDK_REPLICATION_IP"
    )]
    pub ip: Option<Ipv4Addr>,

    /// MAC (aa:bb:cc:dd:ee:ff) of the primary's replication NIC, which a
    /// replica's replication NIC dials, as --dpdk-peer-mac is for the
    /// client NIC. Requires --dpdk-replication-ports and
    /// --dpdk-replication-ip.
    // A `String`, parsed by the mapping, for the reason `DpdkArgs` gives
    // for its MACs.
    #[arg(
        long = "dpdk-replication-peer-mac",
        id = "dpdk_replication_peer_mac",
        value_name = "DPDK_REPLICATION_PEER_MAC"
    )]
    pub peer_mac: Option<String>,
}

/// Why the node's DPDK flags are not a configuration. Displays as the
/// error the node prints, naming the flag, in the wording of
/// [`DpdkArgsError`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeDpdkArgsError {
    /// A `--dpdk-*` flag of the client NIC, as its own mapping reports it;
    /// or a malformed `--dpdk-replication-peer-mac`, reported the same way.
    Nic(DpdkArgsError),
    /// `--dpdk-replication-queue` with a port-mode flag: replication runs
    /// on a queue of the client NIC or on a NIC of its own, not both.
    QueueWithPortFlag {
        /// The first port-mode flag given.
        flag: &'static str,
    },
    /// A port-mode flag without another it needs: the replication NIC
    /// needs ports and an address.
    Requires {
        /// The flag given.
        flag: &'static str,
        /// The flag missing, or both flags.
        required: &'static str,
    },
    /// A `--dpdk-replication-ports` id that is also a `--dpdk-ports` id:
    /// one port cannot be both NICs.
    PortIsClientPort {
        /// The port named by both.
        port: u16,
    },
    /// `--dpdk-replication-ip` equal to the client NIC's address.
    IpIsClientIp {
        /// The address both NICs would have.
        ip: Ipv4Addr,
    },
    /// `--dpdk-replication-ip` outside the client NIC's subnet: the
    /// replication NIC takes the client NIC's prefix length and gateway, so
    /// it must sit in the same subnet.
    IpOutsideClientSubnet {
        /// The replication NIC's address.
        ip: Ipv4Addr,
        /// The client NIC's address.
        client_ip: Ipv4Addr,
        /// The client NIC's prefix length.
        prefix_len: u8,
    },
    /// `--dpdk-replication-queue` with replication the NIC cannot steer
    /// to a queue of its own, as the steering plan refuses it; displayed
    /// naming the node's flags, which the plan's own wording cannot.
    Steering(SteeringError),
}

impl std::fmt::Display for NodeDpdkArgsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeDpdkArgsError::Nic(error) => error.fmt(f),
            NodeDpdkArgsError::QueueWithPortFlag { flag } => write!(
                f,
                "invalid {QUEUE_FLAG}: it cannot be combined with {flag}; replication runs on \
                 a queue of the client NIC or on a NIC of its own, not both"
            ),
            NodeDpdkArgsError::Requires { flag, required } => {
                write!(f, "invalid {flag}: it requires {required}")
            }
            NodeDpdkArgsError::PortIsClientPort { port } => write!(
                f,
                "invalid {PORTS_FLAG}: port {port} is also in --dpdk-ports; the replication NIC \
                 needs ports of its own"
            ),
            NodeDpdkArgsError::IpIsClientIp { ip } => write!(
                f,
                "invalid {IP_FLAG}: {ip} is the client NIC's address (--dpdk-ip); the \
                 replication NIC needs an address of its own"
            ),
            NodeDpdkArgsError::IpOutsideClientSubnet {
                ip,
                client_ip,
                prefix_len,
            } => write!(
                f,
                "invalid {IP_FLAG}: {ip} is outside the client NIC's subnet \
                 ({client_ip}/{prefix_len}); the replication NIC takes its prefix length and \
                 gateway, so it must be in that subnet"
            ),
            NodeDpdkArgsError::Steering(SteeringError::NoReplicationPort) => write!(
                f,
                "invalid {QUEUE_FLAG}: it requires --replication-bind or --replica-of; a node \
                 with neither (a standalone one) has no replication to steer to a queue of its \
                 own"
            ),
            NodeDpdkArgsError::Steering(SteeringError::ReplicationPortIsClientPort { port }) => {
                write!(
                    f,
                    "invalid --replication-bind: port {port} is also --bind's; with {QUEUE_FLAG} \
                     the NIC tells replication from client traffic by port, so the two must differ"
                )
            }
            NodeDpdkArgsError::Steering(error) => write!(f, "invalid {QUEUE_FLAG}: {error}"),
        }
    }
}

impl std::error::Error for NodeDpdkArgsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            // Its own message is this one; its source is the cause.
            NodeDpdkArgsError::Nic(error) => error.source(),
            _ => None,
        }
    }
}

impl From<DpdkArgsError> for NodeDpdkArgsError {
    fn from(error: DpdkArgsError) -> Self {
        NodeDpdkArgsError::Nic(error)
    }
}

const QUEUE_FLAG: &str = "--dpdk-replication-queue";
const PORTS_FLAG: &str = "--dpdk-replication-ports";
const IP_FLAG: &str = "--dpdk-replication-ip";
const PEER_MAC_FLAG: &str = "--dpdk-replication-peer-mac";
/// What the peer MAC alone is missing: both, in one error.
const PORTS_AND_IP_FLAGS: &str = "--dpdk-replication-ports and --dpdk-replication-ip";

impl DpdkReplicationArgs {
    /// The flags as where replication runs, given the client NIC they sit
    /// beside, and the validation of them, before any NIC is touched.
    ///
    /// No flag is [`ReplicationNic::Shared`], `--dpdk-replication-queue`
    /// alone [`ReplicationNic::Queue`], steering `replication_ports` (the node's
    /// replication ports, from `--replication-bind` and `--replica-of`;
    /// whether they can be steered is the steering plan's to say, see
    /// [`NodeDpdkArgsError::Steering`]), and the port-mode flags
    /// [`ReplicationNic::Port`], a NIC with its own ports, address and peer
    /// MAC and everything else the client NIC's. Of several problems, the
    /// one reported is the first of: a mix of the modes, a port-mode flag
    /// missing another it needs, a malformed peer MAC, a port the client
    /// NIC has, the client NIC's address, an address outside the client
    /// NIC's subnet.
    pub fn replication_nic(
        &self,
        client: &NicConfig,
        replication_ports: ReplicationPorts,
    ) -> Result<ReplicationNic, NodeDpdkArgsError> {
        // Destructured, so that a flag added here must be mapped.
        let DpdkReplicationArgs {
            queue,
            ports,
            ip,
            peer_mac,
        } = self;
        let port_flags = [
            (PORTS_FLAG, !ports.is_empty()),
            (IP_FLAG, ip.is_some()),
            (PEER_MAC_FLAG, peer_mac.is_some()),
        ];
        let first_port_flag = port_flags
            .into_iter()
            .find_map(|(flag, given)| given.then_some(flag));
        let Some(first_port_flag) = first_port_flag else {
            return Ok(if *queue {
                ReplicationNic::Queue(replication_ports)
            } else {
                ReplicationNic::Shared
            });
        };
        if *queue {
            return Err(NodeDpdkArgsError::QueueWithPortFlag {
                flag: first_port_flag,
            });
        }
        let requires = |required| NodeDpdkArgsError::Requires {
            flag: first_port_flag,
            required,
        };
        let ip = match (ports.is_empty(), *ip) {
            (false, Some(ip)) => ip,
            (false, None) => return Err(requires(IP_FLAG)),
            (true, Some(_)) => return Err(requires(PORTS_FLAG)),
            (true, None) => return Err(requires(PORTS_AND_IP_FLAGS)),
        };
        let peer_mac = parse_mac_flag(peer_mac.as_deref(), PEER_MAC_FLAG)?;
        // A struct literal, not `..client.clone()`: a field `NicConfig`
        // gains must be decided here, inherited or not.
        let nic = NicConfig {
            // Process-wide: one EAL serves both NICs.
            eal_args: client.eal_args.clone(),
            port_ids: ports.clone(),
            ip_addr: ip,
            prefix_len: client.prefix_len,
            gateway: client.gateway,
            // No bifurcated capture: `--dpdk-peer-ip` names the client
            // NIC's peer, and no flag names the replication NIC's.
            peer_ip: None,
            gateway_mac: client.gateway_mac,
            peer_mac,
            mtu: client.mtu,
            vlan_id: client.vlan_id,
        };
        // The check `NodeNics::init` repeats on any configuration, so a
        // configuration built in code is refused too; here it is reported
        // in terms of the flags.
        if let Some(port) = nic.port_shared_with(client) {
            return Err(NodeDpdkArgsError::PortIsClientPort { port });
        }
        if ip == client.ip_addr {
            return Err(NodeDpdkArgsError::IpIsClientIp { ip });
        }
        if !same_subnet(ip, client.ip_addr, client.prefix_len) {
            return Err(NodeDpdkArgsError::IpOutsideClientSubnet {
                ip,
                client_ip: client.ip_addr,
                prefix_len: client.prefix_len,
            });
        }
        Ok(ReplicationNic::Port(nic))
    }
}

/// Whether `a` and `b` share their first `prefix_len` bits. A prefix
/// length past 32 compares the whole address, as one of 32 does.
fn same_subnet(a: Ipv4Addr, b: Ipv4Addr, prefix_len: u8) -> bool {
    // `checked_shl` because a shift by 32 (a prefix of 0) overflows a
    // `u32`; that prefix's mask is empty.
    let mask = u32::MAX
        .checked_shl(32 - u32::from(prefix_len.min(32)))
        .unwrap_or(0);
    u32::from(a) & mask == u32::from(b) & mask
}

/// Refuse a replication mode this build cannot run yet, rather than run
/// it as [`ReplicationNic::Shared`] and leave the operator believing
/// replication has a queue or a NIC of its own. Checked before EAL init.
// The modes land one at a time; each arm becomes `Ok(())` as its mode does.
pub(crate) fn check_replication_nic_implemented(
    replication: &ReplicationNic,
) -> Result<(), String> {
    let (mode, flags) = match replication {
        ReplicationNic::Shared => return Ok(()),
        ReplicationNic::Queue(_) => ("on a queue of its own", QUEUE_FLAG.to_owned()),
        ReplicationNic::Port(_) => (
            "on a NIC of its own",
            format!("{PORTS_FLAG}, {IP_FLAG} and {PEER_MAC_FLAG}"),
        ),
    };
    Err(format!(
        "DPDK replication {mode} is not implemented yet; omit {flags} to run replication on \
         the client queue"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{CommandFactory, Parser};

    #[derive(Parser, Debug)]
    #[command(name = "node")]
    struct Cli {
        #[command(flatten)]
        replication: DpdkReplicationArgs,
    }

    fn parse(flags: &[&str]) -> Result<DpdkReplicationArgs, clap::Error> {
        Cli::try_parse_from(std::iter::once("node").chain(flags.iter().copied()))
            .map(|c| c.replication)
    }

    /// A client NIC with every inheritable field away from its default, so
    /// that each is seen to be inherited rather than defaulted.
    fn client() -> NicConfig {
        NicConfig {
            eal_args: vec!["--file-prefix=node".into(), "-l".into(), "0-11".into()],
            port_ids: vec![0, 1],
            ip_addr: Ipv4Addr::new(10, 0, 0, 1),
            prefix_len: 16,
            gateway: Some(Ipv4Addr::new(10, 0, 255, 254)),
            peer_ip: Some(Ipv4Addr::new(10, 0, 0, 9)),
            gateway_mac: Some([0x0c, 0x42, 0xa1, 0, 0, 1]),
            peer_mac: Some([0x02, 0x99, 0, 0, 0, 1]),
            mtu: 9000,
            vlan_id: Some(42),
        }
    }

    /// A replica that listens for replicas of its own once promoted, on a
    /// port other than its primary's.
    const PORTS: ReplicationPorts = ReplicationPorts {
        listen: Some(9877),
        primary: Some(9900),
    };

    fn map(flags: &[&str]) -> Result<ReplicationNic, NodeDpdkArgsError> {
        parse(flags)
            .expect("the flags parse")
            .replication_nic(&client(), PORTS)
    }

    #[test]
    fn no_flag_is_shared() {
        let args = parse(&[]).unwrap();
        assert_eq!(args, DpdkReplicationArgs::default());
        assert_eq!(map(&[]), Ok(ReplicationNic::Shared));
    }

    /// Queue mode carries the replication ports it steers, as given.
    #[test]
    fn the_queue_flag_alone_is_queue_with_the_replication_ports() {
        assert_eq!(
            map(&["--dpdk-replication-queue"]),
            Ok(ReplicationNic::Queue(PORTS))
        );
        // Whether they can be steered is the plan's to say, not the
        // mapping's.
        let none = ReplicationPorts::default();
        assert_eq!(
            parse(&["--dpdk-replication-queue"])
                .unwrap()
                .replication_nic(&client(), none),
            Ok(ReplicationNic::Queue(none))
        );
    }

    /// The plan's refusals, worded with the node's flags.
    #[test]
    fn a_steering_error_names_the_node_s_flags() {
        let message = NodeDpdkArgsError::Steering(SteeringError::NoReplicationPort).to_string();
        assert!(
            message.starts_with("invalid --dpdk-replication-queue: "),
            "{message}"
        );
        for flag in ["--replication-bind", "--replica-of"] {
            assert!(message.contains(flag), "{message}");
        }

        let message =
            NodeDpdkArgsError::Steering(SteeringError::ReplicationPortIsClientPort { port: 9876 })
                .to_string();
        assert!(
            message.starts_with("invalid --replication-bind: port 9876 "),
            "{message}"
        );
        assert!(message.contains("--bind"), "{message}");
        assert!(message.contains(QUEUE_FLAG), "{message}");

        let error = SteeringError::SeveralClientQueues { num_queues: 2 };
        assert_eq!(
            NodeDpdkArgsError::Steering(error).to_string(),
            format!("invalid --dpdk-replication-queue: {error}")
        );
    }

    #[test]
    fn the_port_flags_are_a_nic_inheriting_the_client_s_link_settings() {
        let ReplicationNic::Port(nic) = map(&[
            "--dpdk-replication-ports",
            "3,2",
            "--dpdk-replication-ip",
            "10.0.1.1",
            "--dpdk-replication-peer-mac",
            "02:99:00:00:00:02",
        ])
        .unwrap() else {
            panic!("port mode");
        };
        let client = client();
        // Its own: ports (in order), address and peer MAC; and no
        // bifurcated capture.
        assert_eq!(nic.port_ids, [3, 2]);
        assert_eq!(nic.ip_addr, Ipv4Addr::new(10, 0, 1, 1));
        assert_eq!(nic.peer_mac, Some([0x02, 0x99, 0, 0, 0, 2]));
        assert_eq!(nic.peer_ip, None);
        // The client NIC's: everything else.
        assert_eq!(nic.eal_args, client.eal_args);
        assert_eq!(nic.prefix_len, client.prefix_len);
        assert_eq!(nic.gateway, client.gateway);
        assert_eq!(nic.gateway_mac, client.gateway_mac);
        assert_eq!(nic.mtu, client.mtu);
        assert_eq!(nic.vlan_id, client.vlan_id);
    }

    /// The peer MAC is optional, as `--dpdk-peer-mac` is: without one, a
    /// dial falls back to the MAC convention.
    #[test]
    fn the_port_mode_peer_mac_is_optional() {
        let ReplicationNic::Port(nic) = map(&[
            "--dpdk-replication-ports=2",
            "--dpdk-replication-ip=10.0.1.1",
        ])
        .unwrap() else {
            panic!("port mode");
        };
        assert_eq!(nic.peer_mac, None);
        assert_eq!(nic.port_ids, [2]);
    }

    #[test]
    fn queue_and_port_flags_are_refused_naming_the_first_port_flag() {
        for (flags, flag) in [
            (
                &["--dpdk-replication-queue", "--dpdk-replication-ports=2"][..],
                PORTS_FLAG,
            ),
            (
                &["--dpdk-replication-queue", "--dpdk-replication-ip=10.0.1.1"],
                IP_FLAG,
            ),
            (
                &[
                    "--dpdk-replication-queue",
                    "--dpdk-replication-peer-mac=02:99:00:00:00:02",
                ],
                PEER_MAC_FLAG,
            ),
            (
                &[
                    "--dpdk-replication-queue",
                    "--dpdk-replication-ports=2",
                    "--dpdk-replication-ip=10.0.1.1",
                ],
                PORTS_FLAG,
            ),
        ] {
            let error = map(flags).unwrap_err();
            assert_eq!(
                error,
                NodeDpdkArgsError::QueueWithPortFlag { flag },
                "{flags:?}"
            );
            let message = error.to_string();
            assert!(
                message.starts_with("invalid --dpdk-replication-queue: "),
                "{message}"
            );
            assert!(message.contains(flag), "{message}");
        }
    }

    #[test]
    fn incomplete_port_flags_are_refused_naming_what_is_missing() {
        for (flags, flag, required) in [
            (&["--dpdk-replication-ports=2"][..], PORTS_FLAG, IP_FLAG),
            (&["--dpdk-replication-ip=10.0.1.1"], IP_FLAG, PORTS_FLAG),
            (
                &["--dpdk-replication-peer-mac=02:99:00:00:00:02"],
                PEER_MAC_FLAG,
                PORTS_AND_IP_FLAGS,
            ),
            (
                &[
                    "--dpdk-replication-ip=10.0.1.1",
                    "--dpdk-replication-peer-mac=02:99:00:00:00:02",
                ],
                IP_FLAG,
                PORTS_FLAG,
            ),
            (
                &[
                    "--dpdk-replication-ports=2",
                    "--dpdk-replication-peer-mac=02:99:00:00:00:02",
                ],
                PORTS_FLAG,
                IP_FLAG,
            ),
        ] {
            let error = map(flags).unwrap_err();
            assert_eq!(
                error,
                NodeDpdkArgsError::Requires { flag, required },
                "{flags:?}"
            );
            assert_eq!(
                error.to_string(),
                format!("invalid {flag}: it requires {required}")
            );
        }
    }

    /// In `DpdkArgs`'s wording, and through its error, naming the flag
    /// and the value.
    #[test]
    fn a_malformed_peer_mac_is_refused_naming_the_flag_and_the_value() {
        let error = map(&[
            "--dpdk-replication-ports=2",
            "--dpdk-replication-ip=10.0.1.1",
            "--dpdk-replication-peer-mac=02:99",
        ])
        .unwrap_err();
        assert!(matches!(
            error,
            NodeDpdkArgsError::Nic(DpdkArgsError::InvalidMac {
                flag: PEER_MAC_FLAG,
                ..
            })
        ));
        let message = error.to_string();
        assert!(
            message.starts_with("invalid --dpdk-replication-peer-mac: "),
            "{message}"
        );
        assert!(message.contains("'02:99'"), "{message}");
        assert!(std::error::Error::source(&error).is_some());
    }

    #[test]
    fn a_client_port_is_refused() {
        // The client NIC has ports 0 and 1; either is refused, wherever it
        // stands in the list.
        for (ports, port) in [("1", 1), ("2,0", 0), ("0,1", 0)] {
            let error = map(&[
                &format!("--dpdk-replication-ports={ports}"),
                "--dpdk-replication-ip=10.0.1.1",
            ])
            .unwrap_err();
            assert_eq!(error, NodeDpdkArgsError::PortIsClientPort { port });
            let message = error.to_string();
            assert!(
                message.starts_with("invalid --dpdk-replication-ports: "),
                "{message}"
            );
            assert!(message.contains(&format!("port {port} ")), "{message}");
        }
    }

    #[test]
    fn the_client_address_is_refused() {
        let error = map(&[
            "--dpdk-replication-ports=2",
            "--dpdk-replication-ip=10.0.0.1",
        ])
        .unwrap_err();
        let ip = Ipv4Addr::new(10, 0, 0, 1);
        assert_eq!(error, NodeDpdkArgsError::IpIsClientIp { ip });
        let message = error.to_string();
        assert!(
            message.starts_with("invalid --dpdk-replication-ip: 10.0.0.1 "),
            "{message}"
        );
        assert!(std::error::Error::source(&error).is_none());
    }

    /// The client NIC is 10.0.0.1/16: 10.0.255.254 is inside it, 10.1.0.1
    /// is not.
    #[test]
    fn an_address_outside_the_client_subnet_is_refused() {
        let flags = |ip: &str| {
            map(&[
                "--dpdk-replication-ports=2",
                &format!("--dpdk-replication-ip={ip}"),
            ])
        };
        assert!(matches!(flags("10.0.255.254"), Ok(ReplicationNic::Port(_))));

        let error = flags("10.1.0.1").unwrap_err();
        assert_eq!(
            error,
            NodeDpdkArgsError::IpOutsideClientSubnet {
                ip: Ipv4Addr::new(10, 1, 0, 1),
                client_ip: Ipv4Addr::new(10, 0, 0, 1),
                prefix_len: 16,
            }
        );
        let message = error.to_string();
        assert!(
            message.starts_with("invalid --dpdk-replication-ip: 10.1.0.1 "),
            "{message}"
        );
        assert!(message.contains("(10.0.0.1/16)"), "{message}");
    }

    #[test]
    fn subnets_compare_the_prefix_bits_only() {
        let a = Ipv4Addr::new(10, 0, 0, 1);
        assert!(same_subnet(a, Ipv4Addr::new(10, 0, 0, 254), 24));
        assert!(!same_subnet(a, Ipv4Addr::new(10, 0, 1, 1), 24));
        assert!(same_subnet(a, Ipv4Addr::new(10, 0, 1, 1), 23));
        assert!(!same_subnet(a, Ipv4Addr::new(10, 0, 0, 2), 32));
        // No prefix: every address is in the one subnet.
        assert!(same_subnet(a, Ipv4Addr::new(192, 168, 0, 1), 0));
        // Past 32, as 32.
        assert!(!same_subnet(a, Ipv4Addr::new(10, 0, 0, 2), 33));
        assert!(same_subnet(a, a, u8::MAX));
    }

    #[test]
    fn a_malformed_address_or_port_is_refused_by_the_parser() {
        assert!(parse(&["--dpdk-replication-ip=10.0.1"]).is_err());
        assert!(parse(&["--dpdk-replication-ports=x"]).is_err());
        assert!(parse(&["--dpdk-replication-ports="]).is_err());
        assert!(parse(&["--dpdk-replication-queue=yes"]).is_err());
    }

    /// The client NIC's own errors pass through in their own wording.
    #[test]
    fn a_client_nic_error_displays_as_its_own() {
        let error = NodeDpdkArgsError::from(DpdkArgsError::NoPorts);
        assert_eq!(error.to_string(), DpdkArgsError::NoPorts.to_string());
    }

    #[test]
    fn only_shared_runs_until_the_other_modes_land() {
        assert_eq!(
            check_replication_nic_implemented(&ReplicationNic::Shared),
            Ok(())
        );
        let queue = check_replication_nic_implemented(&ReplicationNic::Queue(PORTS)).unwrap_err();
        assert!(queue.contains("not implemented yet"), "{queue}");
        assert!(queue.contains(QUEUE_FLAG), "{queue}");
        let port = check_replication_nic_implemented(&ReplicationNic::Port(client())).unwrap_err();
        assert!(port.contains("not implemented yet"), "{port}");
        for flag in [PORTS_FLAG, IP_FLAG, PEER_MAC_FLAG] {
            assert!(port.contains(flag), "{port}");
        }
    }

    #[test]
    fn flattening_leaves_the_binary_its_own_about() {
        let command = Cli::command();
        assert!(command.get_about().is_none());
        command.debug_assert();
    }
}
