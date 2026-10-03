//! The network `scripts/dpdk/netns-runner.sh` built for this process, as
//! it publishes it in the environment, and the EAL arguments for a node
//! on it.
//!
//! Compiled for the `dpdk` feature, and for the unit tests on every
//! build: parsing is plain code, and checking it should not need libdpdk.

// Without `dpdk`, only the unit tests use this module, and not all of it
// (the environment lookup, the port): what they leave unused is used by
// the DPDK launcher, which is not compiled.
#![cfg_attr(not(feature = "dpdk"), allow(dead_code))]

use std::net::Ipv4Addr;

/// The DPDK interfaces, one per node slot, space-separated.
pub const ENV_IFACES: &str = "MELIN_NETNS_DPDK_IFACES";
/// The IP each slot's node owns, in slot order, space-separated.
pub const ENV_NODE_IPS: &str = "MELIN_NETNS_NODE_IPS";
/// The prefix length of the subnet every slot and the client share.
pub const ENV_PREFIX_LEN: &str = "MELIN_NETNS_PREFIX_LEN";

/// The client port of every DPDK node. Fixed rather than picked: each
/// node owns its IP outright, in namespaces no other test shares, so
/// nothing else can be on it.
pub const NODE_PORT: u16 = 9876;

/// The runner's layout.
#[derive(Debug)]
pub struct Layout {
    /// One entry per node slot. `Vec` because the runner decides the
    /// count; a handful at most, read once.
    slots: Vec<(String, Ipv4Addr)>,
    /// `u8` because that is `ServerConfig::dpdk_prefix_len`'s width, and
    /// an IPv4 prefix is at most 32.
    pub prefix_len: u8,
}

/// One node slot: the interface its DPDK port attaches to and the IP its
/// node owns there.
pub struct Slot<'a> {
    pub iface: &'a str,
    pub ip: Ipv4Addr,
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
                "this test was built with the `dpdk` feature, so its node runs on DPDK, on the \
                 network scripts/dpdk/netns-runner.sh builds — and {ENV_NODE_IPS} is not set, \
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
        let prefix_len = required(ENV_PREFIX_LEN)?;

        let ips = node_ips
            .split_whitespace()
            .map(|ip| {
                ip.parse::<Ipv4Addr>()
                    .map_err(|e| format!("{ENV_NODE_IPS}: '{ip}' is not an IPv4 address: {e}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let ifaces: Vec<String> = ifaces.split_whitespace().map(String::from).collect();
        if ips.is_empty() || ips.len() != ifaces.len() {
            return Err(format!(
                "{ENV_NODE_IPS} lists {} node IPs and {ENV_IFACES} {} interfaces: the runner \
                 publishes one of each per node slot",
                ips.len(),
                ifaces.len()
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
            slots: ifaces.into_iter().zip(ips).collect(),
            prefix_len,
        })
    }

    /// Node slot `index`. Panics past the last one: the test asked for more
    /// nodes than the runner built links for.
    pub fn slot(&self, index: usize) -> Slot<'_> {
        let (iface, ip) = self.slots.get(index).unwrap_or_else(|| {
            panic!(
                "node slot {index} requested, but the runner built {} (see {ENV_NODE_IPS})",
                self.slots.len()
            )
        });
        Slot { iface, ip: *ip }
    }
}

/// EAL arguments for one node on `iface` through af_packet, with no
/// hugepages and no PCI scan, its main lcore on `cpu`.
///
/// `--in-memory` is not among them: EAL refuses it with `--no-huge`,
/// which implies legacy memory, and the runner's private `/var/run` makes
/// it unnecessary.
pub fn eal_args(iface: &str, cpu: usize) -> String {
    format!("--no-huge -m 512 --no-pci --vdev=net_af_packet0,iface={iface} -l {cpu}")
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

    #[test]
    fn an_unreadable_variable_is_reported_as_such() {
        let err = Layout::parse(|name| {
            if name == ENV_IFACES {
                Err(format!("{name} is set but is not valid UTF-8"))
            } else {
                Ok(Some(match name {
                    ENV_NODE_IPS => "10.99.0.2".to_string(),
                    _ => "24".to_string(),
                }))
            }
        })
        .expect_err("an unreadable variable");
        assert!(err.contains(ENV_IFACES), "{err}");
        assert!(!err.contains("partial"), "{err}");
    }

    #[test]
    fn the_runners_layout_parses() {
        let layout = Layout::parse(env(&[
            (ENV_IFACES, "veth0"),
            (ENV_NODE_IPS, "10.99.0.2"),
            (ENV_PREFIX_LEN, "24"),
        ]))
        .expect("a full layout");
        assert_eq!(layout.prefix_len, 24);
        let slot = layout.slot(0);
        assert_eq!(slot.iface, "veth0");
        assert_eq!(slot.ip, Ipv4Addr::new(10, 99, 0, 2));
    }

    #[test]
    fn slots_pair_interfaces_and_ips_in_order() {
        let layout = Layout::parse(env(&[
            (ENV_IFACES, "veth0 veth2"),
            (ENV_NODE_IPS, "10.99.0.2  10.99.0.3"),
            (ENV_PREFIX_LEN, "24"),
        ]))
        .expect("two slots");
        assert_eq!(layout.slot(1).iface, "veth2");
        assert_eq!(layout.slot(1).ip, Ipv4Addr::new(10, 99, 0, 3));
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
        for vars in [
            &[(ENV_NODE_IPS, "10.99.0.2"), (ENV_PREFIX_LEN, "24")][..],
            &[(ENV_NODE_IPS, "10.99.0.2"), (ENV_IFACES, "veth0")][..],
            &[
                (ENV_NODE_IPS, "10.99.0.2 10.99.0.3"),
                (ENV_IFACES, "veth0"),
                (ENV_PREFIX_LEN, "24"),
            ][..],
            &[(ENV_NODE_IPS, ""), (ENV_IFACES, ""), (ENV_PREFIX_LEN, "24")][..],
            &[
                (ENV_NODE_IPS, "10.99.0.256"),
                (ENV_IFACES, "veth0"),
                (ENV_PREFIX_LEN, "24"),
            ][..],
            &[
                (ENV_NODE_IPS, "10.99.0.2"),
                (ENV_IFACES, "veth0"),
                (ENV_PREFIX_LEN, "33"),
            ][..],
        ] {
            assert!(Layout::parse(env(vars)).is_err(), "{vars:?}");
        }
    }

    #[test]
    #[should_panic(expected = "node slot 1 requested")]
    fn a_slot_past_the_last_panics() {
        let layout = Layout::parse(env(&[
            (ENV_IFACES, "veth0"),
            (ENV_NODE_IPS, "10.99.0.2"),
            (ENV_PREFIX_LEN, "24"),
        ]))
        .expect("a full layout");
        let _ = layout.slot(1).ip;
    }

    #[test]
    fn eal_args_name_the_interface_and_core() {
        assert_eq!(
            eal_args("veth0", 3),
            "--no-huge -m 512 --no-pci --vdev=net_af_packet0,iface=veth0 -l 3"
        );
    }
}
