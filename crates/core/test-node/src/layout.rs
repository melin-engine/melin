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
/// The interfaces of the node slots' second seats, one per slot, in slot
/// order, space-separated: where a node whose replication has a NIC of
/// its own attaches that NIC.
pub const ENV_REPLICATION_IFACES: &str = "MELIN_NETNS_REPLICATION_IFACES";
/// The IP each slot's replication NIC takes, in slot order.
pub const ENV_REPLICATION_IPS: &str = "MELIN_NETNS_REPLICATION_IPS";
/// The MAC of each slot's replication interface, in slot order.
pub const ENV_REPLICATION_MACS: &str = "MELIN_NETNS_REPLICATION_MACS";

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
    /// Each node slot's second seat, for a replication NIC of its own,
    /// indexed by slot as `slots` is. `None` under a runner that publishes
    /// none, as for `dpdk_client`: only a test that asks for one fails.
    replication: Option<Vec<Slot>>,
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

        let slots = parse_seats(
            [ENV_IFACES, ENV_NODE_IPS, ENV_NODE_MACS],
            [&ifaces, &node_ips, &macs],
        )?;
        if slots.is_empty() {
            return Err(format!(
                "{ENV_NODE_IPS} lists no node IP: the runner publishes at least one node slot"
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

        // All three or none, as the DPDK client's seat; one seat per node
        // slot, each a pair of its own.
        let replication = match (
            var(ENV_REPLICATION_IFACES)?,
            var(ENV_REPLICATION_IPS)?,
            var(ENV_REPLICATION_MACS)?,
        ) {
            (None, None, None) => None,
            (Some(ifaces), Some(ips), Some(macs)) => {
                let seats = parse_seats(
                    [
                        ENV_REPLICATION_IFACES,
                        ENV_REPLICATION_IPS,
                        ENV_REPLICATION_MACS,
                    ],
                    [&ifaces, &ips, &macs],
                )?;
                if seats.len() != slots.len() {
                    return Err(format!(
                        "{ENV_REPLICATION_IPS} lists {} replication seats for {} node slots: \
                         the runner publishes one per slot",
                        seats.len(),
                        slots.len()
                    ));
                }
                let collides = |own: &Slot| {
                    own.ip == client_ip
                        || slots
                            .iter()
                            .chain(&dpdk_client)
                            .any(|seat| seat.ip == own.ip || seat.iface == own.iface)
                };
                if let Some(seat) = seats.iter().find(|own| collides(own)) {
                    return Err(format!(
                        "{ENV_REPLICATION_IFACES}/{ENV_REPLICATION_IPS} ({} on {}) collide with \
                         a node slot, the DPDK client or the kernel side: a replication seat is \
                         a pair of its own",
                        seat.ip, seat.iface
                    ));
                }
                Some(seats)
            }
            _ => {
                return Err(format!(
                    "{ENV_REPLICATION_IFACES}, {ENV_REPLICATION_IPS} and {ENV_REPLICATION_MACS} \
                     are published together or not at all: a partial runner layout"
                ));
            }
        };

        Ok(Layout {
            slots,
            prefix_len,
            client_ip,
            client_mac: check_mac(ENV_CLIENT_MAC, client_mac.trim())?,
            dpdk_client,
            replication,
        })
    }

    /// Node slot `index`'s replication seat, for a node whose replication
    /// has a NIC of its own, or an error saying that this runner publishes
    /// none. Panics past the last slot, as [`Self::slot`] does.
    pub fn replication_seat(&self, index: usize) -> Result<&Slot, String> {
        let seats = self.replication.as_ref().ok_or_else(|| {
            format!(
                "this test runs a node whose replication has a NIC of its own, but the runner \
                 publishes no seat for one ({ENV_REPLICATION_IFACES}, {ENV_REPLICATION_IPS}, \
                 {ENV_REPLICATION_MACS} unset): run it under this repository's \
                 scripts/dpdk/netns-runner.sh, which builds one per node slot"
            )
        })?;
        // The slot's own check and message first: there are as many seats
        // as slots.
        self.slot(index);
        Ok(&seats[index])
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

    /// The MAC behind `ip` on this network (a slot's, a slot's replication
    /// seat's, the kernel side's, or the DPDK client's), or `None` when
    /// `ip` is not on it.
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
            .chain(self.replication.iter().flatten())
            .find(|slot| slot.ip == ip)
            .map(|slot| slot.mac.as_str())
    }
}

/// `ip` as an IPv4 address, or an error naming variable `name`.
fn parse_ip(name: &str, ip: &str) -> Result<Ipv4Addr, String> {
    ip.parse::<Ipv4Addr>()
        .map_err(|e| format!("{name}: '{ip}' is not an IPv4 address: {e}"))
}

/// Seats from three space-separated lists published together, paired in
/// order: their interfaces, IPs and MACs (`values`, from the variables
/// `names`). One of each per seat; none at all is no seat.
fn parse_seats(names: [&str; 3], values: [&str; 3]) -> Result<Vec<Slot>, String> {
    let [ifaces_name, ips_name, macs_name] = names;
    let [ifaces, ips, macs] = values;
    let ips = ips
        .split_whitespace()
        .map(|ip| parse_ip(ips_name, ip))
        .collect::<Result<Vec<_>, _>>()?;
    let ifaces: Vec<&str> = ifaces.split_whitespace().collect();
    let macs = macs
        .split_whitespace()
        .map(|mac| check_mac(macs_name, mac))
        .collect::<Result<Vec<_>, _>>()?;
    if ips.len() != ifaces.len() || ips.len() != macs.len() {
        return Err(format!(
            "{ips_name} lists {} IPs, {ifaces_name} {} interfaces and {macs_name} {} MACs: the \
             runner publishes one of each per seat",
            ips.len(),
            ifaces.len(),
            macs.len()
        ));
    }
    Ok(ifaces
        .into_iter()
        .zip(ips)
        .zip(macs)
        .map(|((iface, ip), mac)| Slot {
            iface: iface.to_owned(),
            ip,
            mac,
        })
        .collect())
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

/// `device` (a name and its driver arguments, from [`vdev`]) with
/// `queue_pairs` RX/TX queue pairs, each a socket on the interface, rather
/// than the driver's one. One leaves the arguments as they are.
pub fn with_queue_pairs((name, args): (String, String), queue_pairs: u16) -> (String, String) {
    if queue_pairs == 1 {
        return (name, args);
    }
    (name, format!("{args},qpairs={queue_pairs}"))
}

/// The af_packet device for slot `index`'s replication seat on `iface`:
/// its name, which is also its port's, and its driver arguments. Named
/// apart from every slot's ([`vdev`]) and the DPDK client's
/// ([`CLIENT_VDEV`]), keeping the driver's name as its prefix.
pub fn replication_vdev(index: usize, iface: &str) -> (String, String) {
    (
        format!("net_af_packet_repl{index}"),
        format!("iface={iface}"),
    )
}

/// The DPDK client's af_packet device on `iface`: its name
/// ([`CLIENT_VDEV`]), which is also its port's, and its driver arguments.
pub fn client_vdev(iface: &str) -> (String, String) {
    (CLIENT_VDEV.to_owned(), format!("iface={iface}"))
}

/// EAL arguments for a process that initializes EAL itself rather than
/// sharing the launcher's: the launcher's own ([`eal_args`]) and
/// `devices` (each a name and its driver arguments, from [`vdev`] or
/// [`client_vdev`]), allowed at init, so that their ports are numbered
/// from 0 in the order given.
pub fn own_eal_args(cpu: usize, devices: &[(String, String)]) -> Vec<String> {
    let mut args = eal_args(cpu);
    args.extend(
        devices
            .iter()
            .map(|(device, device_args)| format!("--vdev={device},{device_args}")),
    );
    args
}

/// EAL arguments for a process of its own on the DPDK client's pair: a
/// sidecar a test runs as a child (the shm-proxy), which initializes EAL
/// itself where the test process shares the launcher's. [`own_eal_args`]
/// with the client's af_packet device on `iface` ([`client_vdev`]) as the
/// process's one port, and `file_prefix`, so that its EAL's runtime files
/// stay apart from the test process's in the runner's private `/var/run`.
pub fn client_process_eal_args(cpu: usize, iface: &str, file_prefix: &str) -> Vec<String> {
    let mut args = own_eal_args(cpu, &[client_vdev(iface)]);
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
            (ENV_REPLICATION_IFACES, "dpdk0r dpdk1r dpdk2r"),
            (ENV_REPLICATION_IPS, "10.99.0.12 10.99.0.13 10.99.0.14"),
            (
                ENV_REPLICATION_MACS,
                "02:99:00:00:00:0c 02:99:00:00:00:0d 02:99:00:00:00:0e",
            ),
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

    /// Each node slot has a replication seat of its own, apart from every
    /// slot and the DPDK client, and its MAC is found by its IP.
    #[test]
    fn each_slot_has_a_replication_seat_of_its_own() {
        let vars = runner_env(&[]);
        let layout = Layout::parse(env(&vars)).expect("a full layout");
        assert_eq!(
            layout.replication_seat(1).expect("a replication seat"),
            &Slot {
                iface: "dpdk1r".into(),
                ip: Ipv4Addr::new(10, 99, 0, 13),
                mac: "02:99:00:00:00:0d".into(),
            }
        );
        assert_eq!(layout.slot_count(), 3, "the seats add no slot");
        assert_eq!(
            layout.mac_of(Ipv4Addr::new(10, 99, 0, 14)),
            Some("02:99:00:00:00:0e")
        );
    }

    /// A runner that publishes no replication seats still runs every node
    /// test; only asking for a seat fails, naming what is missing.
    #[test]
    fn a_layout_without_replication_seats_serves_the_nodes() {
        let vars = runner_env(&[
            (ENV_REPLICATION_IFACES, None),
            (ENV_REPLICATION_IPS, None),
            (ENV_REPLICATION_MACS, None),
        ]);
        let layout = Layout::parse(env(&vars)).expect("a layout without replication seats");
        assert_eq!(layout.slot_count(), 3);
        let err = layout.replication_seat(0).expect_err("no seat");
        assert!(err.contains(ENV_REPLICATION_IPS), "{err}");
        assert!(err.contains("netns-runner.sh"), "{err}");
    }

    #[test]
    #[should_panic(expected = "node slot 3 requested")]
    fn a_replication_seat_past_the_last_slot_panics() {
        let vars = runner_env(&[]);
        let layout = Layout::parse(env(&vars)).expect("a full layout");
        let _ = layout.replication_seat(3);
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
            .flat_map(|index| {
                [
                    layout.slot(index).ip,
                    layout
                        .replication_seat(index)
                        .expect("a replication seat")
                        .ip,
                ]
            })
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
            // Replication seats: partial, one short of the slots, a seat
            // on a slot's, the DPDK client's or the kernel side's address
            // or interface, a malformed entry.
            &[(ENV_REPLICATION_MACS, None)][..],
            &[
                (ENV_REPLICATION_IFACES, Some("dpdk0r dpdk1r")),
                (ENV_REPLICATION_IPS, Some("10.99.0.12 10.99.0.13")),
                (
                    ENV_REPLICATION_MACS,
                    Some("02:99:00:00:00:0c 02:99:00:00:00:0d"),
                ),
            ][..],
            &[(ENV_REPLICATION_IPS, Some("10.99.0.12 10.99.0.3 10.99.0.14"))][..],
            &[(
                ENV_REPLICATION_IPS,
                Some("10.99.0.12 10.99.0.100 10.99.0.14"),
            )][..],
            &[(ENV_REPLICATION_IPS, Some("10.99.0.1 10.99.0.13 10.99.0.14"))][..],
            &[(ENV_REPLICATION_IFACES, Some("dpdk0r dpdk1 dpdk2r"))][..],
            &[(ENV_REPLICATION_IFACES, Some("dpdk0r dpdkc dpdk2r"))][..],
            &[(ENV_REPLICATION_IPS, Some("10.99.0.12 10.99.0.13 10.99.0"))][..],
            &[(
                ENV_REPLICATION_MACS,
                Some("02:99:00:00:00:0c 02:99:00:00:00:0d 02:99:00:00:00:0"),
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

    #[test]
    fn a_device_takes_more_queue_pairs_as_a_driver_argument() {
        assert_eq!(
            with_queue_pairs(vdev(1, "dpdk1"), 2),
            (
                "net_af_packet1".to_owned(),
                "iface=dpdk1,qpairs=2".to_owned()
            )
        );
        assert_eq!(with_queue_pairs(vdev(1, "dpdk1"), 1), vdev(1, "dpdk1"));
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

    /// A slot's replication device is named apart from every slot's and
    /// the client's, with the driver's name as its prefix, and one per
    /// slot.
    #[test]
    fn a_replication_device_is_named_apart_from_every_other() {
        let (name, args) = replication_vdev(1, "dpdk1r");
        assert_eq!(name, "net_af_packet_repl1");
        assert_eq!(args, "iface=dpdk1r");
        assert!(name.starts_with("net_af_packet"));
        assert_ne!(name, CLIENT_VDEV);
        assert_ne!(replication_vdev(0, "a").0, replication_vdev(1, "a").0);
        for index in [0, 1, 2, 9, 10, 255] {
            assert_ne!(vdev(index, "dpdk1r").0, name);
        }
    }

    /// A process owning its EAL with node slots' devices: each allowed at
    /// init, in the order given, which is the order of their ports.
    #[test]
    fn an_owned_eal_allows_the_devices_in_order() {
        assert_eq!(
            own_eal_args(3, &[vdev(1, "dpdk1"), vdev(0, "dpdk0")]).join(" "),
            "--no-huge -m 512 --no-pci -l 3 --vdev=net_af_packet1,iface=dpdk1 \
             --vdev=net_af_packet0,iface=dpdk0"
        );
        assert_eq!(own_eal_args(3, &[]), eal_args(3));
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
