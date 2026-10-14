//! The `--dpdk-*` flags, declared once for every binary that brings a NIC
//! up: the node, and the clients beside it.
//!
//! Behind the `clap` feature, and outside the `dpdk-sys` gate like
//! [`NicConfig`]: the flags are plain data, declared and validated whether
//! or not the build has the transport compiled in.

use std::net::Ipv4Addr;

use crate::config::NicConfig;
use crate::mac::{MacAddr, MacParseError, try_parse_mac};

/// The DPDK flags, one per field of [`NicConfig`], for a binary to
/// flatten into its own command line (`#[command(flatten)]`).
///
/// The help here is kept short: what an operator has to know about a flag
/// (why the peer MAC is not optional on a fabric that assigns MACs, why a
/// client beside a node needs an EAL file prefix of its own) is written on
/// the [`NicConfig`] field the flag maps onto, once.
///
/// [`nic_config`](Self::nic_config) and
/// [`nic_config_or`](Self::nic_config_or) are the one mapping and the one
/// validation: a binary that requires `--dpdk-ip` calls the first, one
/// that defaults it the second. A binary that changes a flag's default for
/// its own command line does so with `mut_arg` on the arg's id (the flag's
/// name, snake-cased: `dpdk_eal_args`), which is also the id to name in a
/// `conflicts_with` or `requires` of its own. The flags' argument group is
/// `DpdkArgs`, the struct's name, as clap names a group: a struct of the
/// binary's own that flattens this one must be named otherwise.
///
/// [`Default`] is what parsing an empty command line gives, so a program
/// can build one in code with `..DpdkArgs::default()`.
// `about = None`: a flattened struct's doc comment would otherwise become
// the about text of the binary flattening it, where that binary sets none.
#[derive(clap::Args, Debug, Clone, PartialEq, Eq)]
#[command(about = None, long_about = None)]
pub struct DpdkArgs {
    /// DPDK EAL arguments (space-separated). Example: --dpdk-eal-args="-l 0-7 --huge-dir /dev/hugepages".
    /// Passed directly to rte_eal_init. Only used when compiled with --features dpdk.
    /// The `=` is required: the value itself starts with a dash, and requiring
    /// the joined form keeps a forgotten value from silently swallowing the
    /// next flag as the EAL string.
    // A `String`, split on whitespace by the mapping, rather than a list:
    // the operator writes it as one quoted argument.
    #[arg(
        long = "dpdk-eal-args",
        id = "dpdk_eal_args",
        value_name = "DPDK_EAL_ARGS",
        default_value = "",
        require_equals = true
    )]
    pub eal_args: String,

    /// DPDK port IDs (comma-separated). For LACP bonds, pass both VF ports
    /// (e.g., "0,1") so traffic arriving on either bond member is received.
    // A `Vec`, as the field it maps onto: ordered, one or two entries.
    #[arg(
        long = "dpdk-ports",
        id = "dpdk_ports",
        value_name = "DPDK_PORTS",
        default_value = "0",
        value_delimiter = ','
    )]
    pub ports: Vec<u16>,

    /// IPv4 address for the DPDK interface (e.g., "10.0.0.1").
    // An `Option` with no default here: a client must name its own address,
    // and a node gives one (`mut_arg` for the help, `nic_config_or` for the
    // value).
    #[arg(long = "dpdk-ip", id = "dpdk_ip", value_name = "DPDK_IP")]
    pub ip: Option<Ipv4Addr>,

    /// IPv4 prefix length for the DPDK interface. Default: 24.
    #[arg(
        long = "dpdk-prefix-len",
        id = "dpdk_prefix_len",
        value_name = "DPDK_PREFIX_LEN",
        default_value_t = 24
    )]
    pub prefix_len: u8,

    /// IPv4 gateway for the DPDK interface (optional, needed for cross-subnet traffic).
    #[arg(
        long = "dpdk-gateway",
        id = "dpdk_gateway",
        value_name = "DPDK_GATEWAY"
    )]
    pub gateway: Option<Ipv4Addr>,

    /// Peer IPv4 used for the bifurcated `rte_flow` steering rule. When
    /// set, the DPDK port opens in isolated mode and only IPv4 packets
    /// sourced from this address are delivered into DPDK queue 0 —
    /// everything else stays with the kernel netdev. Required for L3
    /// setups that share the public NIC with the kernel (SSH, etc.).
    #[arg(
        long = "dpdk-peer-ip",
        id = "dpdk_peer_ip",
        value_name = "DPDK_PEER_IP"
    )]
    pub peer_ip: Option<Ipv4Addr>,

    /// Gateway MAC (aa:bb:cc:dd:ee:ff) seeded into smoltcp for the
    /// `--dpdk-gateway` IP. Required in L3 bifurcated mode because the
    /// gateway's ARP reply would not match the steering rule and would
    /// be eaten by the kernel. Source from `ip neigh` on the host.
    // A `String`, parsed by the mapping rather than by clap, so that a
    // value built in code is checked by the same parser with the same
    // error as one from the command line.
    #[arg(
        long = "dpdk-gateway-mac",
        id = "dpdk_gateway_mac",
        value_name = "DPDK_GATEWAY_MAC",
        requires = "dpdk_gateway"
    )]
    pub gateway_mac: Option<String>,

    /// MAC (aa:bb:cc:dd:ee:ff) of the peer this interface dials: the
    /// primary named by `--replica-of` on a replica, the node on a client.
    /// Required where the peer's port keeps a hardware MAC (mlx5
    /// bifurcated, AWS ENI), or the connect times out. Read it from
    /// `/sys/class/net/<iface>/address` on the peer.
    // A `String` for the same reason as `gateway_mac`.
    #[arg(
        long = "dpdk-peer-mac",
        id = "dpdk_peer_mac",
        value_name = "DPDK_PEER_MAC"
    )]
    pub peer_mac: Option<String>,

    /// MTU for the DPDK interface. Use 9000 for jumbo frames (6x fewer TCP
    /// segments). Requires switch and PF MTU to be set accordingly.
    // A `usize`, as the field it maps onto.
    #[arg(
        long = "dpdk-mtu",
        id = "dpdk_mtu",
        value_name = "DPDK_MTU",
        default_value_t = 1500
    )]
    pub mtu: usize,

    /// VLAN ID for hardware VLAN strip/insert. Required in dedicated NIC
    /// mode (dpdk-setup-dedicated.sh) where the kernel doesn't handle VLAN
    /// tags. Not needed for SR-IOV mode (the PF handles VLAN tagging).
    #[arg(long = "dpdk-vlan", id = "dpdk_vlan", value_name = "DPDK_VLAN")]
    pub vlan: Option<u16>,
}

impl Default for DpdkArgs {
    /// The flags' own defaults, which are [`NicConfig::default`]'s with no
    /// address (the tests hold the three together).
    fn default() -> Self {
        DpdkArgs {
            eal_args: String::new(),
            ports: vec![0],
            ip: None,
            prefix_len: 24,
            gateway: None,
            peer_ip: None,
            gateway_mac: None,
            peer_mac: None,
            mtu: 1500,
            vlan: None,
        }
    }
}

/// Why a set of DPDK flags is not a [`NicConfig`]. Displays as the error a
/// binary prints, naming the flag (and the value, where one was given).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DpdkArgsError {
    /// No `--dpdk-ip`, in a binary that gives the address no default.
    IpRequired,
    /// `--dpdk-ports` named no port. The command line cannot produce this
    /// (an empty value does not parse); a value built in code can.
    NoPorts,
    /// `--dpdk-gateway-mac` without `--dpdk-gateway`: there is no gateway
    /// to seed it for. Refused by clap on a command line; checked again for
    /// a value built in code.
    GatewayMacWithoutGateway,
    /// A `--dpdk-*-mac` value that is not an Ethernet address.
    InvalidMac {
        /// The flag, `--dpdk-gateway-mac` or `--dpdk-peer-mac`.
        flag: &'static str,
        /// The parse error, which names the value given.
        error: MacParseError,
    },
}

impl std::fmt::Display for DpdkArgsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DpdkArgsError::IpRequired => f.write_str("--dpdk-ip is required"),
            DpdkArgsError::NoPorts => f.write_str("invalid --dpdk-ports: no port given"),
            DpdkArgsError::GatewayMacWithoutGateway => {
                f.write_str("invalid --dpdk-gateway-mac: it requires --dpdk-gateway")
            }
            DpdkArgsError::InvalidMac { flag, error } => write!(f, "invalid {flag}: {error}"),
        }
    }
}

impl std::error::Error for DpdkArgsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            DpdkArgsError::InvalidMac { error, .. } => Some(error),
            _ => None,
        }
    }
}

impl DpdkArgs {
    /// The flags as a NIC configuration, with `--dpdk-ip` required: what a
    /// client calls, which has no address to assume.
    pub fn nic_config(&self) -> Result<NicConfig, DpdkArgsError> {
        let ip = self.ip.ok_or(DpdkArgsError::IpRequired)?;
        self.nic_config_at(ip)
    }

    /// The flags as a NIC configuration, with `default_ip` where no
    /// `--dpdk-ip` was given: what a binary with an address of its own to
    /// assume calls (the node's is [`NicConfig::default`]'s).
    pub fn nic_config_or(&self, default_ip: Ipv4Addr) -> Result<NicConfig, DpdkArgsError> {
        self.nic_config_at(self.ip.unwrap_or(default_ip))
    }

    /// The mapping, field for field, and the validation clap cannot do.
    ///
    /// Every value is checked here, before any NIC is touched, and a bad one
    /// returned as an error: a peer MAC is only used when a dial goes out,
    /// which on a replica is inside a running node's reconnect loop. The
    /// order of the checks is the order of the flags, so of several bad
    /// values the first flag's is the one reported.
    fn nic_config_at(&self, ip_addr: Ipv4Addr) -> Result<NicConfig, DpdkArgsError> {
        // Destructured, and the result a struct literal rather than
        // `..Default::default()`: a field either side gains must be mapped
        // here, and fails the build until it is.
        let DpdkArgs {
            eal_args,
            ports,
            ip: _,
            prefix_len,
            gateway,
            peer_ip,
            gateway_mac,
            peer_mac,
            mtu,
            vlan,
        } = self;
        if ports.is_empty() {
            return Err(DpdkArgsError::NoPorts);
        }
        if gateway_mac.is_some() && gateway.is_none() {
            return Err(DpdkArgsError::GatewayMacWithoutGateway);
        }
        let gateway_mac = parse_mac_flag(gateway_mac.as_deref(), "--dpdk-gateway-mac")?;
        let peer_mac = parse_mac_flag(peer_mac.as_deref(), "--dpdk-peer-mac")?;
        Ok(NicConfig {
            eal_args: split_eal_args(eal_args),
            port_ids: ports.clone(),
            ip_addr,
            prefix_len: *prefix_len,
            gateway: *gateway,
            peer_ip: *peer_ip,
            gateway_mac,
            peer_mac,
            mtu: *mtu,
            vlan_id: *vlan,
        })
    }

    /// The first flag set away from its default, if any, in the order the
    /// flags are declared: what a binary that ignores the flags on some
    /// run (a client on the kernel transport) refuses, since the operator
    /// meant something by it.
    ///
    /// Compared against [`Default`], which is the flags' own defaults: a
    /// binary that changes a default with `mut_arg` cannot use this. A flag
    /// given at its default value cannot be told from one not given, and
    /// changes nothing; neither does an EAL string of whitespace alone.
    pub fn first_given(&self) -> Option<&'static str> {
        let DpdkArgs {
            eal_args,
            ports,
            ip,
            prefix_len,
            gateway,
            peer_ip,
            gateway_mac,
            peer_mac,
            mtu,
            vlan,
        } = self;
        let default = DpdkArgs::default();
        [
            (
                "--dpdk-eal-args",
                split_eal_args(eal_args) != split_eal_args(&default.eal_args),
            ),
            ("--dpdk-ports", *ports != default.ports),
            ("--dpdk-ip", *ip != default.ip),
            ("--dpdk-prefix-len", *prefix_len != default.prefix_len),
            ("--dpdk-gateway", *gateway != default.gateway),
            ("--dpdk-peer-ip", *peer_ip != default.peer_ip),
            ("--dpdk-gateway-mac", *gateway_mac != default.gateway_mac),
            ("--dpdk-peer-mac", *peer_mac != default.peer_mac),
            ("--dpdk-mtu", *mtu != default.mtu),
            ("--dpdk-vlan", *vlan != default.vlan),
        ]
        .into_iter()
        .find_map(|(flag, given)| given.then_some(flag))
    }
}

/// An optional MAC flag's value, parsed, with a bad one reported as
/// [`DpdkArgsError::InvalidMac`] naming `flag`: the one parse every
/// `--dpdk-*-mac` flag goes through, this crate's and a binary's own.
pub fn parse_mac_flag(
    value: Option<&str>,
    flag: &'static str,
) -> Result<Option<MacAddr>, DpdkArgsError> {
    value
        .map(try_parse_mac)
        .transpose()
        .map_err(|error| DpdkArgsError::InvalidMac { flag, error })
}

/// `--dpdk-eal-args` as `rte_eal_init` takes it, one argument per element.
/// No quoting: an EAL argument has no spaces in it.
fn split_eal_args(eal_args: &str) -> Vec<String> {
    eal_args.split_whitespace().map(String::from).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{CommandFactory, Parser};

    // A binary with the flags and nothing else, the way one flattens them.
    // No doc comment: it would be this command's about text, which one
    // test checks the flattened struct does not set.
    #[derive(Parser, Debug)]
    #[command(name = "nic")]
    struct Cli {
        #[command(flatten)]
        dpdk: DpdkArgs,
    }

    fn parse(flags: &[&str]) -> Result<DpdkArgs, clap::Error> {
        Cli::try_parse_from(std::iter::once("nic").chain(flags.iter().copied())).map(|c| c.dpdk)
    }

    #[test]
    fn every_flag_maps_onto_its_field() {
        let args = parse(&[
            "--dpdk-eal-args=--vdev=net_tap0  --no-pci --file-prefix=x",
            "--dpdk-ports",
            "1,0",
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
        } = args.nic_config().unwrap();
        assert_eq!(eal_args, ["--vdev=net_tap0", "--no-pci", "--file-prefix=x"]);
        assert_eq!(port_ids, [1, 0], "the order is kept: the first transmits");
        assert_eq!(ip_addr, Ipv4Addr::new(10, 0, 0, 2));
        assert_eq!(prefix_len, 16);
        assert_eq!(gateway, Some(Ipv4Addr::new(10, 0, 255, 254)));
        assert_eq!(peer_ip, Some(Ipv4Addr::new(10, 0, 0, 1)));
        assert_eq!(gateway_mac, Some([0x0c, 0x42, 0xa1, 0, 0, 1]));
        assert_eq!(peer_mac, Some([0x02, 0x99, 0x01, 0, 0, 1]));
        assert_eq!(mtu, 9000);
        assert_eq!(vlan_id, Some(42));

        // A given address wins over a binary's default.
        let defaulted = args.nic_config_or(Ipv4Addr::new(10, 0, 0, 9)).unwrap();
        assert_eq!(defaulted.ip_addr, Ipv4Addr::new(10, 0, 0, 2));
    }

    /// An empty command line, `Default`, and `NicConfig::default()` agree,
    /// field by field; and those are the node's flag defaults as they were
    /// before the flags were shared.
    #[test]
    fn the_defaults_are_the_nic_config_defaults() {
        let parsed = parse(&[]).unwrap();
        assert_eq!(parsed, DpdkArgs::default());

        let node = NicConfig::default();
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
        } = parsed.nic_config_or(node.ip_addr).unwrap();
        assert_eq!(eal_args, node.eal_args);
        assert_eq!(port_ids, node.port_ids);
        assert_eq!(ip_addr, node.ip_addr);
        assert_eq!(prefix_len, node.prefix_len);
        assert_eq!(gateway, node.gateway);
        assert_eq!(peer_ip, node.peer_ip);
        assert_eq!(gateway_mac, node.gateway_mac);
        assert_eq!(peer_mac, node.peer_mac);
        assert_eq!(mtu, node.mtu);
        assert_eq!(vlan_id, node.vlan_id);

        // The node's flags before: `--dpdk-eal-args ""`, `--dpdk-ports 0`,
        // `--dpdk-prefix-len 24`, `--dpdk-mtu 1500`, the rest unset.
        assert!(eal_args.is_empty());
        assert_eq!(port_ids, [0]);
        assert_eq!(prefix_len, 24);
        assert_eq!(mtu, 1500);
    }

    #[test]
    fn the_address_is_required_unless_the_binary_gives_one() {
        let args = parse(&[]).unwrap();
        let missing = args.nic_config().unwrap_err();
        assert_eq!(missing, DpdkArgsError::IpRequired);
        assert_eq!(missing.to_string(), "--dpdk-ip is required");

        let defaulted = args.nic_config_or(Ipv4Addr::new(10, 0, 0, 2)).unwrap();
        assert_eq!(defaulted.ip_addr, Ipv4Addr::new(10, 0, 0, 2));
    }

    #[test]
    fn a_malformed_address_is_refused_naming_the_flag() {
        for flag in ["--dpdk-ip", "--dpdk-gateway", "--dpdk-peer-ip"] {
            let error = parse(&[flag, "10.0.0"]).unwrap_err().to_string();
            assert!(error.contains(flag), "{error}");
            assert!(error.contains("'10.0.0'"), "{error}");
        }
    }

    /// The node's wording, `invalid <flag>: ...`, naming the value too.
    #[test]
    fn a_malformed_mac_is_refused_naming_the_flag_and_the_value() {
        let args = parse(&["--dpdk-ip", "10.0.0.2", "--dpdk-peer-mac", "02:99"]).unwrap();
        let error = args.nic_config().unwrap_err();
        assert!(matches!(
            error,
            DpdkArgsError::InvalidMac {
                flag: "--dpdk-peer-mac",
                ..
            }
        ));
        let message = error.to_string();
        assert!(
            message.starts_with("invalid --dpdk-peer-mac: "),
            "{message}"
        );
        assert!(message.contains("'02:99'"), "{message}");

        let args = parse(&[
            "--dpdk-gateway",
            "10.0.0.254",
            "--dpdk-gateway-mac",
            "not-a-mac",
        ])
        .unwrap();
        let message = args
            .nic_config_or(Ipv4Addr::new(10, 0, 0, 1))
            .unwrap_err()
            .to_string();
        assert!(
            message.starts_with("invalid --dpdk-gateway-mac: "),
            "{message}"
        );
        assert!(message.contains("not-a-mac"), "{message}");
    }

    /// Of several bad values, the first flag's is reported.
    #[test]
    fn the_first_bad_flag_is_the_one_reported() {
        let args = DpdkArgs {
            gateway: Some(Ipv4Addr::new(10, 0, 0, 254)),
            gateway_mac: Some("bad".into()),
            peer_mac: Some("worse".into()),
            ..DpdkArgs::default()
        };
        let message = args.nic_config().unwrap_err().to_string();
        assert_eq!(message, "--dpdk-ip is required");
        let message = args
            .nic_config_or(Ipv4Addr::new(10, 0, 0, 1))
            .unwrap_err()
            .to_string();
        assert!(message.contains("--dpdk-gateway-mac"), "{message}");
    }

    /// The joined form only: separated, a value starting with a dash would
    /// be taken for a flag, and a forgotten value would swallow the next
    /// flag as the EAL string.
    #[test]
    fn the_eal_args_take_the_joined_form_only() {
        let args = parse(&["--dpdk-eal-args=-l 0-7 --huge-dir /dev/hugepages"]).unwrap();
        assert_eq!(args.eal_args, "-l 0-7 --huge-dir /dev/hugepages");
        assert_eq!(
            args.nic_config_or(Ipv4Addr::LOCALHOST).unwrap().eal_args,
            ["-l", "0-7", "--huge-dir", "/dev/hugepages"]
        );

        assert!(parse(&["--dpdk-eal-args", "-l 0-7"]).is_err());
        // A forgotten value does not swallow the next flag.
        assert!(parse(&["--dpdk-eal-args", "--dpdk-mtu=9000"]).is_err());
    }

    #[test]
    fn the_gateway_mac_requires_the_gateway() {
        // On the command line, clap refuses it.
        let error = parse(&["--dpdk-gateway-mac", "0c:42:a1:00:00:01"])
            .unwrap_err()
            .to_string();
        assert!(error.contains("--dpdk-gateway"), "{error}");
        assert!(
            parse(&[
                "--dpdk-gateway-mac",
                "0c:42:a1:00:00:01",
                "--dpdk-gateway",
                "10.0.0.254"
            ])
            .is_ok()
        );

        // Built in code, the mapping does.
        let args = DpdkArgs {
            gateway_mac: Some("0c:42:a1:00:00:01".into()),
            ..DpdkArgs::default()
        };
        assert_eq!(
            args.nic_config_or(Ipv4Addr::LOCALHOST).unwrap_err(),
            DpdkArgsError::GatewayMacWithoutGateway
        );
    }

    #[test]
    fn no_port_is_refused() {
        let args = DpdkArgs {
            ports: Vec::new(),
            ..DpdkArgs::default()
        };
        let error = args.nic_config_or(Ipv4Addr::LOCALHOST).unwrap_err();
        assert_eq!(error, DpdkArgsError::NoPorts);
        assert!(error.to_string().contains("--dpdk-ports"), "{error}");
        // And an empty value does not parse.
        assert!(parse(&["--dpdk-ports="]).is_err());
    }

    #[test]
    fn the_first_flag_given_is_named() {
        assert_eq!(parse(&[]).unwrap().first_given(), None);
        // Whitespace alone, or a flag at its default value, changes nothing.
        assert_eq!(
            parse(&["--dpdk-eal-args=  ", "--dpdk-ports=0", "--dpdk-mtu=1500"])
                .unwrap()
                .first_given(),
            None
        );

        for (flags, expected) in [
            (&["--dpdk-eal-args=--no-pci"][..], "--dpdk-eal-args"),
            (&["--dpdk-ports=1"], "--dpdk-ports"),
            (&["--dpdk-ip=10.0.0.2"], "--dpdk-ip"),
            (&["--dpdk-prefix-len=16"], "--dpdk-prefix-len"),
            (&["--dpdk-gateway=10.0.0.254"], "--dpdk-gateway"),
            (&["--dpdk-peer-ip=10.0.0.1"], "--dpdk-peer-ip"),
            (
                &[
                    "--dpdk-gateway-mac=0c:42:a1:00:00:01",
                    "--dpdk-gateway=10.0.0.254",
                ],
                "--dpdk-gateway",
            ),
            (&["--dpdk-peer-mac=02:99:01:00:00:01"], "--dpdk-peer-mac"),
            (&["--dpdk-mtu=9000"], "--dpdk-mtu"),
            (&["--dpdk-vlan=42"], "--dpdk-vlan"),
        ] {
            assert_eq!(
                parse(flags).unwrap().first_given(),
                Some(expected),
                "{flags:?}"
            );
        }
        // The gateway's MAC alone, built in code, is named for itself.
        let args = DpdkArgs {
            gateway_mac: Some("0c:42:a1:00:00:01".into()),
            ..DpdkArgs::default()
        };
        assert_eq!(args.first_given(), Some("--dpdk-gateway-mac"));
    }

    /// Flattened, the struct adds its flags and nothing else: its doc
    /// comment does not become the binary's about text.
    #[test]
    fn flattening_leaves_the_binary_its_own_about() {
        let command = Cli::command();
        assert!(command.get_about().is_none());
        assert!(command.get_long_about().is_none());
        command.debug_assert();
    }

    /// A binary changes a default by the arg's id, as the node and the
    /// proxy do.
    #[test]
    fn a_binary_can_change_a_default_by_the_flag_id() {
        #[derive(Parser)]
        #[command(name = "node", mut_arg("dpdk_ip", |a| a.default_value("10.0.0.1")))]
        struct Node {
            #[command(flatten)]
            dpdk: DpdkArgs,
        }
        let node = Node::try_parse_from(["node"]).unwrap();
        assert_eq!(node.dpdk.ip, Some(Ipv4Addr::new(10, 0, 0, 1)));
        Node::command().debug_assert();
    }
}
