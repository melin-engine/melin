//! The network `scripts/dpdk/netns-runner.sh` built for this process, as
//! it publishes it in the environment, and the EAL setup for nodes on it.
//!
//! Compiled for the `dpdk` feature, and for the unit tests on every
//! build: parsing is plain code, and checking it should not need libdpdk.

use std::net::Ipv4Addr;

/// The DPDK interfaces, one per node slot, space-separated.
pub const ENV_IFACES: &str = "MELIN_NETNS_DPDK_IFACES";
/// The IP each slot's node owns, in slot order, space-separated.
pub const ENV_NODE_IPS: &str = "MELIN_NETNS_NODE_IPS";
/// The MAC of each slot's interface, which its DPDK port takes, in slot
/// order, space-separated.
pub const ENV_NODE_MACS: &str = "MELIN_NETNS_NODE_MACS";
/// The prefix length of the subnet every slot and the client share.
pub const ENV_PREFIX_LEN: &str = "MELIN_NETNS_PREFIX_LEN";
/// The kernel side's address: where the test's own sockets live.
pub const ENV_CLIENT_IP: &str = "MELIN_NETNS_CLIENT_IP";
/// The MAC of the kernel side's interface.
pub const ENV_CLIENT_MAC: &str = "MELIN_NETNS_CLIENT_MAC";
/// The interface a DPDK client attaches to: a veth pair of its own, built
/// as a node slot's is but published apart from them, so that no node is
/// ever started on it.
pub const ENV_DPDK_CLIENT_IFACE: &str = "MELIN_NETNS_DPDK_CLIENT_IFACE";
/// The IP the DPDK client owns on its interface.
pub const ENV_DPDK_CLIENT_IP: &str = "MELIN_NETNS_DPDK_CLIENT_IP";
/// The MAC of the DPDK client's interface, which its port takes.
pub const ENV_DPDK_CLIENT_MAC: &str = "MELIN_NETNS_DPDK_CLIENT_MAC";

/// The name of the DPDK client's af_packet device: not of the
/// `net_af_packet<slot>` form a node slot's takes ([`vdev`]), so the two
/// cannot collide however many slots the runner builds. The driver is
/// found by the name's prefix, which this keeps.
pub const CLIENT_VDEV: &str = "net_af_packet_client";

/// The client port of every DPDK node. Fixed rather than picked: each
/// node owns its IP outright, in namespaces no other test shares, so
/// nothing else can be on it. The DPDK launcher's alone, like
/// [`Layout::from_env`]: the unit tests parse a layout of their own.
#[cfg(feature = "dpdk")]
pub const NODE_PORT: u16 = 9876;
/// The replication port of every DPDK node, fixed for the same reason.
#[cfg(feature = "dpdk")]
pub const REPLICATION_PORT: u16 = 9877;

/// One veth seat on the runner's network, a node slot or the DPDK
/// client's: the interface its DPDK port attaches to, the IP its owner
/// takes there, and the interface's MAC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Slot {
    pub iface: String,
    pub ip: Ipv4Addr,
    pub mac: String,
}

/// The runner's layout.
#[derive(Debug)]
pub struct Layout {
    /// One entry per node slot, indexed by slot. `Vec` because the runner
    /// decides the count; a handful at most, read once.
    slots: Vec<Slot>,
    /// `u8` because that is `ServerConfig::dpdk.prefix_len`'s width, and
    /// an IPv4 prefix is at most 32.
    pub prefix_len: u8,
    pub client_ip: Ipv4Addr,
    pub client_mac: String,
    /// The DPDK client's seat, apart from the node slots. `None` under a
    /// runner that publishes none (one older than the seat, or another
    /// project's copy): the node slots stand without it, and only a test
    /// that asks for a DPDK client fails, saying why.
    dpdk_client: Option<Slot>,
}

impl Layout {
    /// The layout from this process's environment, or an error saying how
    /// to run under the runner.
    #[cfg(feature = "dpdk")]
    pub fn from_env() -> Result<Layout, String> {
        Layout::parse(|name| match std::env::var(name) {
            Ok(value) => Ok(Some(value)),
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(std::env::VarError::NotUnicode(_)) => {
                Err(format!("{name} is set but is not valid UTF-8"))
            }
        })
    }

    /// The layout from `var`, which looks a variable up by name: `None`
    /// when it is unset, an error when it is set but unreadable.
    pub fn parse(var: impl Fn(&str) -> Result<Option<String>, String>) -> Result<Layout, String> {
        let Some(node_ips) = var(ENV_NODE_IPS)? else {
            return Err(format!(
                "this test was built with the `dpdk` feature, so its nodes run on DPDK, on the \
                 network scripts/dpdk/netns-runner.sh builds, but {ENV_NODE_IPS} is not set, \
                 so this process is not running under it. Run the tests through the runner, \
                 with the `dpdk` nextest profile (one test at a time: every DPDK node \
                 busy-polls a core), from the repository root:\n  \
                 CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER=\"$PWD/scripts/dpdk/netns-runner.sh\" \
                 cargo nextest run --profile dpdk -p <package> --features <package>/dpdk\n\
                 or build without the feature to run them on kernel TCP."
            ));
        };
        let required = |name: &str| {
            var(name)?.ok_or_else(|| {
                format!("{ENV_NODE_IPS} is set but {name} is not: a partial runner layout")
            })
        };
        let ifaces = required(ENV_IFACES)?;
        let macs = required(ENV_NODE_MACS)?;
        let prefix_len = required(ENV_PREFIX_LEN)?;
        let client_ip = required(ENV_CLIENT_IP)?;
        let client_mac = required(ENV_CLIENT_MAC)?;

        let parse_ip = |name: &str, ip: &str| {
            ip.parse::<Ipv4Addr>()
                .map_err(|e| format!("{name}: '{ip}' is not an IPv4 address: {e}"))
        };
        let ips = node_ips
            .split_whitespace()
            .map(|ip| parse_ip(ENV_NODE_IPS, ip))
            .collect::<Result<Vec<_>, _>>()?;
        let ifaces: Vec<&str> = ifaces.split_whitespace().collect();
        let macs = macs
            .split_whitespace()
            .map(|mac| check_mac(ENV_NODE_MACS, mac))
            .collect::<Result<Vec<_>, _>>()?;
        if ips.is_empty() || ips.len() != ifaces.len() || ips.len() != macs.len() {
            return Err(format!(
                "{ENV_NODE_IPS} lists {} node IPs, {ENV_IFACES} {} interfaces and \
                 {ENV_NODE_MACS} {} MACs: the runner publishes one of each per node slot",
                ips.len(),
                ifaces.len(),
                macs.len()
            ));
        }
        let prefix_len = match prefix_len.trim().parse::<u8>() {
            Ok(len) if len <= 32 => len,
            _ => {
                return Err(format!(
                    "{ENV_PREFIX_LEN}: '{prefix_len}' is not an IPv4 prefix length"
                ));
            }
        };
        let slots: Vec<Slot> = ifaces
            .into_iter()
            .zip(ips)
            .zip(macs)
            .map(|((iface, ip), mac)| Slot {
                iface: iface.to_owned(),
                ip,
                mac,
            })
            .collect();
        let client_ip = parse_ip(ENV_CLIENT_IP, client_ip.trim())?;

        // All three or none: a runner without the seat publishes none of
        // them, and a partial set is a broken runner.
        let dpdk_client = match (
            var(ENV_DPDK_CLIENT_IFACE)?,
            var(ENV_DPDK_CLIENT_IP)?,
            var(ENV_DPDK_CLIENT_MAC)?,
        ) {
            (None, None, None) => None,
            (Some(iface), Some(ip), Some(mac)) => {
                let seat = Slot {
                    iface: iface.trim().to_owned(),
                    ip: parse_ip(ENV_DPDK_CLIENT_IP, ip.trim())?,
                    mac: check_mac(ENV_DPDK_CLIENT_MAC, mac.trim())?,
                };
                if seat.iface.is_empty() || seat.iface.contains(char::is_whitespace) {
                    return Err(format!(
                        "{ENV_DPDK_CLIENT_IFACE}: '{iface}' is not one interface name"
                    ));
                }
                let taken = seat.ip == client_ip
                    || slots
                        .iter()
                        .any(|slot| slot.ip == seat.ip || slot.iface == seat.iface);
                if taken {
                    return Err(format!(
                        "{ENV_DPDK_CLIENT_IFACE}/{ENV_DPDK_CLIENT_IP} ({} on {}) collide with a \
                         node slot or the kernel side: the DPDK client's seat is a pair of its own",
                        seat.ip, seat.iface
                    ));
                }
                Some(seat)
            }
            _ => {
                return Err(format!(
                    "{ENV_DPDK_CLIENT_IFACE}, {ENV_DPDK_CLIENT_IP} and {ENV_DPDK_CLIENT_MAC} are \
                     published together or not at all: a partial runner layout"
                ));
            }
        };

        Ok(Layout {
            slots,
            prefix_len,
            client_ip,
            client_mac: check_mac(ENV_CLIENT_MAC, client_mac.trim())?,
            dpdk_client,
        })
    }

    /// The DPDK client's seat, or an error saying that this runner
    /// publishes none.
    pub fn dpdk_client(&self) -> Result<&Slot, String> {
        self.dpdk_client.as_ref().ok_or_else(|| {
            format!(
                "this test runs a client on DPDK, but the runner publishes no seat for one \
                 ({ENV_DPDK_CLIENT_IFACE}, {ENV_DPDK_CLIENT_IP}, {ENV_DPDK_CLIENT_MAC} unset): \
                 run it under this repository's scripts/dpdk/netns-runner.sh, which builds one"
            )
        })
    }

    /// How many node slots the runner built.
    pub fn slot_count(&self) -> usize {
        self.slots.len()
    }

    /// Node slot `index`. Panics past the last one: the test asked for more
    /// nodes than the runner built links for.
    pub fn slot(&self, index: usize) -> &Slot {
        self.slots.get(index).unwrap_or_else(|| {
            panic!(
                "node slot {index} requested, but the runner built {} (see {ENV_NODE_IPS})",
                self.slots.len()
            )
        })
    }

    /// The MAC behind `ip` on this network (a slot's, the kernel side's,
    /// or the DPDK client's), or `None` when `ip` is not on it.
    ///
    /// A replica on DPDK dials its primary, and a client on DPDK its node,
    /// with the peer's MAC already seeded (`--dpdk-peer-mac`,
    /// `NicConfig::peer_mac`); on af_packet the derived fallback is wrong,
    /// as on any port that keeps a real hardware address.
    pub fn mac_of(&self, ip: Ipv4Addr) -> Option<&str> {
        if ip == self.client_ip {
            return Some(&self.client_mac);
        }
        self.slots
            .iter()
            .chain(&self.dpdk_client)
            .find(|slot| slot.ip == ip)
            .map(|slot| slot.mac.as_str())
    }
}

/// `mac` as given, once it is checked to be six colon-separated hex
/// octets. Kept as text: the node's configuration takes it as text, and
/// parses it again itself.
fn check_mac(name: &str, mac: &str) -> Result<String, String> {
    let octets: Vec<&str> = mac.split(':').collect();
    let well_formed = octets.len() == 6
        && octets
            .iter()
            .all(|o| o.len() == 2 && o.bytes().all(|b| b.is_ascii_hexdigit()));
    if well_formed {
        Ok(mac.to_owned())
    } else {
        Err(format!("{name}: '{mac}' is not a MAC address"))
    }
}

/// EAL arguments for the process-wide EAL every node of the process
/// shares: no hugepages, no PCI scan, the main lcore on `cpu`. No device:
/// each node's is attached when it starts ([`vdev`]).
///
/// `--in-memory` is not among them: EAL refuses it with `--no-huge`,
/// which implies legacy memory, and the runner's private `/var/run` makes
/// it unnecessary. 512 MiB holds the mbuf pools of every slot's node and
/// of the DPDK client at once with room to spare.
pub fn eal_args(cpu: usize) -> Vec<String> {
    ["--no-huge", "-m", "512", "--no-pci", "-l", &cpu.to_string()]
        .map(String::from)
        .to_vec()
}

/// The af_packet device for slot `index` on `iface`: its name, which is
/// also its port's, and its driver arguments.
pub fn vdev(index: usize, iface: &str) -> (String, String) {
    (format!("net_af_packet{index}"), format!("iface={iface}"))
}

/// The DPDK client's af_packet device on `iface`: its name
/// ([`CLIENT_VDEV`]), which is also its port's, and its driver arguments.
pub fn client_vdev(iface: &str) -> (String, String) {
    (CLIENT_VDEV.to_owned(), format!("iface={iface}"))
}

/// EAL arguments for a process of its own on the DPDK client's pair: a
/// sidecar a test runs as a child (the shm-proxy), which initializes EAL
/// itself where the test process shares the launcher's. The launcher's
/// own ([`eal_args`]), the client's af_packet device on `iface`
/// ([`client_vdev`]) as the process's one port, and `file_prefix`, so
/// that its EAL's runtime files stay apart from the test process's in the
/// runner's private `/var/run`.
pub fn client_process_eal_args(cpu: usize, iface: &str, file_prefix: &str) -> Vec<String> {
    let (device, device_args) = client_vdev(iface);
    let mut args = eal_args(cpu);
    args.push(format!("--vdev={device},{device_args}"));
    args.extend(["--file-prefix".to_owned(), file_prefix.to_owned()]);
    args
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env<'a>(
        vars: &'a [(&'a str, &'a str)],
    ) -> impl Fn(&str) -> Result<Option<String>, String> + 'a {
        move |name| {
            Ok(vars
                .iter()
                .find(|(n, _)| *n == name)
                .map(|(_, v)| v.to_string()))
        }
    }

    /// The runner's layout as it publishes it, with `overrides` replacing
    /// variables, or with `None` dropping them.
    fn runner_env(
        overrides: &[(&'static str, Option<&'static str>)],
    ) -> Vec<(&'static str, &'static str)> {
        let mut vars = vec![
            (ENV_IFACES, "dpdk0 dpdk1 dpdk2"),
            (ENV_NODE_IPS, "10.99.0.2 10.99.0.3 10.99.0.4"),
            (
                ENV_NODE_MACS,
                "02:99:00:00:00:02 02:99:00:00:00:03 02:99:00:00:00:04",
            ),
            (ENV_PREFIX_LEN, "24"),
            (ENV_CLIENT_IP, "10.99.0.1"),
            (ENV_CLIENT_MAC, "02:99:00:00:00:01"),
            (ENV_DPDK_CLIENT_IFACE, "dpdkc"),
            (ENV_DPDK_CLIENT_IP, "10.99.0.100"),
            (ENV_DPDK_CLIENT_MAC, "02:99:00:00:00:64"),
        ];
        for &(name, value) in overrides {
            vars.retain(|(n, _)| *n != name);
            if let Some(value) = value {
                vars.push((name, value));
            }
        }
        vars
    }

    #[test]
    fn an_unreadable_variable_is_reported_as_such() {
        let vars = runner_env(&[]);
        let err = Layout::parse(|name| {
            if name == ENV_IFACES {
                Err(format!("{name} is set but is not valid UTF-8"))
            } else {
                env(&vars)(name)
            }
        })
        .expect_err("an unreadable variable");
        assert!(err.contains(ENV_IFACES), "{err}");
        assert!(!err.contains("partial"), "{err}");
    }

    #[test]
    fn the_runners_layout_parses() {
        let vars = runner_env(&[]);
        let layout = Layout::parse(env(&vars)).expect("a full layout");
        assert_eq!(layout.prefix_len, 24);
        assert_eq!(layout.slot_count(), 3);
        assert_eq!(
            layout.slot(0),
            &Slot {
                iface: "dpdk0".into(),
                ip: Ipv4Addr::new(10, 99, 0, 2),
                mac: "02:99:00:00:00:02".into(),
            }
        );
        assert_eq!(layout.client_ip, Ipv4Addr::new(10, 99, 0, 1));
        assert_eq!(layout.client_mac, "02:99:00:00:00:01");
        assert_eq!(
            layout.dpdk_client().expect("the DPDK client's seat"),
            &Slot {
                iface: "dpdkc".into(),
                ip: Ipv4Addr::new(10, 99, 0, 100),
                mac: "02:99:00:00:00:64".into(),
            }
        );
    }

    /// The DPDK client's seat is apart from the node slots: it adds no
    /// slot, so a node started on the first free slot can never take it.
    #[test]
    fn the_dpdk_client_is_not_a_node_slot() {
        let vars = runner_env(&[]);
        let layout = Layout::parse(env(&vars)).expect("a full layout");
        let seat = layout.dpdk_client().expect("the DPDK client's seat");
        assert_eq!(layout.slot_count(), 3);
        for index in 0..layout.slot_count() {
            assert_ne!(layout.slot(index).iface, seat.iface);
            assert_ne!(layout.slot(index).ip, seat.ip);
        }
    }

    /// A runner that publishes no seat for a DPDK client still runs every
    /// node test; only asking for the seat fails, naming what is missing.
    #[test]
    fn a_layout_without_a_dpdk_client_serves_the_nodes() {
        let vars = runner_env(&[
            (ENV_DPDK_CLIENT_IFACE, None),
            (ENV_DPDK_CLIENT_IP, None),
            (ENV_DPDK_CLIENT_MAC, None),
        ]);
        let layout = Layout::parse(env(&vars)).expect("a node-only layout");
        assert_eq!(layout.slot_count(), 3);
        let err = layout.dpdk_client().expect_err("no seat");
        assert!(err.contains(ENV_DPDK_CLIENT_IP), "{err}");
        assert!(err.contains("netns-runner.sh"), "{err}");
    }

    /// The MACs the runner publishes are not the `02:00:<ip>` convention a
    /// DPDK dialer falls back to without a peer MAC, so a test whose
    /// replica or client reaches its peer proves the MAC was passed
    /// through. This checks `runner_env`, a copy of the scheme
    /// `netns-runner.sh` uses, not the script itself: the check on the real
    /// runner's MACs is the `assert_ne!` each DPDK round-trip case makes at
    /// run time, on the CI `dpdk` job.
    #[test]
    fn no_published_mac_is_the_sriov_convention() {
        let vars = runner_env(&[]);
        let layout = Layout::parse(env(&vars)).expect("a full layout");
        let seat = layout.dpdk_client().expect("the DPDK client's seat");
        let ips = (0..layout.slot_count())
            .map(|index| layout.slot(index).ip)
            .chain([layout.client_ip, seat.ip]);
        for ip in ips {
            let o = ip.octets();
            let convention = format!("02:00:{:02x}:{:02x}:{:02x}:{:02x}", o[0], o[1], o[2], o[3]);
            let mac = layout.mac_of(ip).expect("a MAC for every published IP");
            assert!(!mac.eq_ignore_ascii_case(&convention), "{ip}: {mac}");
        }
    }

    #[test]
    fn slots_pair_interfaces_ips_and_macs_in_order() {
        let vars = runner_env(&[(ENV_NODE_IPS, Some("10.99.0.2  10.99.0.3 10.99.0.4"))]);
        let layout = Layout::parse(env(&vars)).expect("three slots");
        let slot = layout.slot(2);
        assert_eq!(slot.iface, "dpdk2");
        assert_eq!(slot.ip, Ipv4Addr::new(10, 99, 0, 4));
        assert_eq!(slot.mac, "02:99:00:00:00:04");
    }

    #[test]
    fn a_peer_mac_is_found_for_a_slot_or_the_client_and_nothing_else() {
        let vars = runner_env(&[]);
        let layout = Layout::parse(env(&vars)).expect("a full layout");
        assert_eq!(
            layout.mac_of(Ipv4Addr::new(10, 99, 0, 3)),
            Some("02:99:00:00:00:03")
        );
        assert_eq!(
            layout.mac_of(Ipv4Addr::new(10, 99, 0, 1)),
            Some("02:99:00:00:00:01")
        );
        assert_eq!(
            layout.mac_of(Ipv4Addr::new(10, 99, 0, 100)),
            Some("02:99:00:00:00:64")
        );
        assert_eq!(layout.mac_of(Ipv4Addr::LOCALHOST), None);
    }

    #[test]
    fn no_runner_says_how_to_run() {
        let err = Layout::parse(env(&[])).expect_err("no layout");
        assert!(err.contains("netns-runner.sh"), "{err}");
        assert!(
            err.contains("CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER"),
            "{err}"
        );
    }

    #[test]
    fn a_partial_or_inconsistent_layout_is_refused() {
        for overrides in [
            &[(ENV_IFACES, None)][..],
            &[(ENV_NODE_MACS, None)][..],
            &[(ENV_PREFIX_LEN, None)][..],
            &[(ENV_CLIENT_IP, None)][..],
            &[(ENV_CLIENT_MAC, None)][..],
            &[(ENV_IFACES, Some("dpdk0"))][..],
            &[(ENV_NODE_MACS, Some("02:99:00:00:00:02"))][..],
            &[
                (ENV_NODE_IPS, Some("")),
                (ENV_IFACES, Some("")),
                (ENV_NODE_MACS, Some("")),
            ][..],
            &[(ENV_NODE_IPS, Some("10.99.0.2 10.99.0.3 10.99.0.256"))][..],
            &[(ENV_PREFIX_LEN, Some("33"))][..],
            &[(ENV_CLIENT_IP, Some("10.99.0"))][..],
            &[(ENV_CLIENT_MAC, Some("02:99:00:00:00"))][..],
            &[(ENV_DPDK_CLIENT_IFACE, None)][..],
            &[(ENV_DPDK_CLIENT_IP, None), (ENV_DPDK_CLIENT_MAC, None)][..],
            &[(ENV_DPDK_CLIENT_IFACE, Some(""))][..],
            &[(ENV_DPDK_CLIENT_IFACE, Some("dpdkc dpdkd"))][..],
            &[(ENV_DPDK_CLIENT_IFACE, Some("dpdk1"))][..],
            &[(ENV_DPDK_CLIENT_IP, Some("10.99.0.3"))][..],
            &[(ENV_DPDK_CLIENT_IP, Some("10.99.0.1"))][..],
            &[(ENV_DPDK_CLIENT_IP, Some("10.99.0"))][..],
            &[(ENV_DPDK_CLIENT_MAC, Some("02:99:00:00:00:6"))][..],
            &[(
                ENV_NODE_MACS,
                Some("02:99:00:00:00:02 02:99:00:00:00:03 02:99:00:00:00:zz"),
            )][..],
        ] {
            let vars = runner_env(overrides);
            assert!(Layout::parse(env(&vars)).is_err(), "{overrides:?}");
        }
    }

    #[test]
    #[should_panic(expected = "node slot 3 requested")]
    fn a_slot_past_the_last_panics() {
        let vars = runner_env(&[]);
        let layout = Layout::parse(env(&vars)).expect("a full layout");
        let _ = layout.slot(3).ip;
    }

    #[test]
    fn eal_args_attach_no_device_and_name_the_core() {
        assert_eq!(eal_args(3).join(" "), "--no-huge -m 512 --no-pci -l 3");
    }

    #[test]
    fn each_slot_has_a_device_of_its_own() {
        assert_eq!(
            vdev(1, "dpdk1"),
            ("net_af_packet1".to_owned(), "iface=dpdk1".to_owned())
        );
        assert_ne!(vdev(0, "dpdk0").0, vdev(1, "dpdk1").0);
    }

    /// The client's device keeps the driver's name as its prefix, which is
    /// how EAL finds the driver, and is not of the `net_af_packet<slot>`
    /// form, so no slot's can be named like it however many there are.
    #[test]
    fn the_dpdk_clients_device_is_named_apart_from_every_slots() {
        let (name, args) = client_vdev("dpdkc");
        assert_eq!(args, "iface=dpdkc");
        let suffix = name
            .strip_prefix("net_af_packet")
            .expect("the af_packet driver's name as the prefix");
        assert!(
            !suffix.is_empty() && !suffix.bytes().all(|b| b.is_ascii_digit()),
            "{name} could be a slot's"
        );
        for index in [0, 1, 2, 9, 10, 255] {
            assert_ne!(vdev(index, "dpdkc").0, name);
        }
    }

    /// A process of its own on the client pair: the launcher's arguments,
    /// the client's device as its one port, and its own file prefix, as
    /// one whitespace-separated string a binary's `--dpdk-eal-args` takes.
    #[test]
    fn a_client_process_has_the_device_and_a_prefix_of_its_own() {
        assert_eq!(
            client_process_eal_args(3, "dpdkc", "sidecar").join(" "),
            "--no-huge -m 512 --no-pci -l 3 --vdev=net_af_packet_client,iface=dpdkc \
             --file-prefix sidecar"
        );
    }
}
