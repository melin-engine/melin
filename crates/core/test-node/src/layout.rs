//! The network `scripts/dpdk/netns-runner.sh` built for this process, as
//! it publishes it in the environment, and the EAL setup for nodes on it.
//!
//! Compiled for the `dpdk` feature, and for the unit tests on every
//! build: parsing is plain code, and checking it should not need libdpdk.

// Without `dpdk`, only the unit tests use this module, and not all of it
// (the environment lookup, the ports): what they leave unused is used by
// the DPDK launcher, which is not compiled.
#![cfg_attr(not(feature = "dpdk"), allow(dead_code))]

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

/// The client port of every DPDK node. Fixed rather than picked: each
/// node owns its IP outright, in namespaces no other test shares, so
/// nothing else can be on it.
pub const NODE_PORT: u16 = 9876;
/// The replication port of every DPDK node, fixed for the same reason.
pub const REPLICATION_PORT: u16 = 9877;

/// One node slot: the interface its DPDK port attaches to, the IP its
/// node owns there, and the interface's MAC.
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
    /// `u8` because that is `ServerConfig::dpdk_prefix_len`'s width, and
    /// an IPv4 prefix is at most 32.
    pub prefix_len: u8,
    pub client_ip: Ipv4Addr,
    pub client_mac: String,
}

impl Layout {
    /// The layout from this process's environment, or an error saying how
    /// to run under the runner.
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
                 one at a time (every DPDK node busy-polls a core), from the repository \
                 root:\n  \
                 CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER=\"$PWD/scripts/dpdk/netns-runner.sh\" \
                 cargo nextest run -p <package> --features dpdk -j 1\n\
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
        Ok(Layout {
            slots: ifaces
                .into_iter()
                .zip(ips)
                .zip(macs)
                .map(|((iface, ip), mac)| Slot {
                    iface: iface.to_owned(),
                    ip,
                    mac,
                })
                .collect(),
            prefix_len,
            client_ip: parse_ip(ENV_CLIENT_IP, client_ip.trim())?,
            client_mac: check_mac(ENV_CLIENT_MAC, client_mac.trim())?,
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

    /// The MAC behind `ip` on this network (a slot's, or the kernel
    /// side's), or `None` when `ip` is not on it.
    ///
    /// A replica on DPDK dials its primary with the primary's MAC already
    /// seeded (`--dpdk-peer-mac`); on af_packet the derived fallback is
    /// wrong, as on any port that keeps a real hardware address.
    pub fn mac_of(&self, ip: Ipv4Addr) -> Option<&str> {
        if ip == self.client_ip {
            return Some(&self.client_mac);
        }
        self.slots
            .iter()
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
/// it unnecessary. 512 MiB holds the mbuf pools of every slot's node at
/// once with room to spare.
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
}
