//! What changes for a NIC group when it shares its EAL: with the other
//! nodes of a process that runs a process-wide EAL
//! (`Eal::init_process_wide`), or with the other groups of one node (a
//! client NIC and a replication NIC of its own, on one EAL).
//!
//! Plain logic with no libdpdk dependency, so it is tested on every host,
//! like `mac` and `rx_checksum`. `DpdkShared` and `NodeNics` are its only
//! callers.

// Without `dpdk-sys` nothing but the tests calls into this module.
#![cfg_attr(not(feature = "dpdk-sys"), allow(dead_code))]

use std::sync::Arc;

use crate::config::NicConfig;

/// The mbuf pool's name when the node owns EAL: the name it has always
/// had, so the one-node-per-process deployment is unchanged.
const OWN_EAL_POOL: &str = "pktmbuf_pool";

/// Refuse EAL arguments for a node that is to share the process-wide EAL.
///
/// EAL is already initialized, so they could only be ignored, and an
/// argument silently ignored (a core list, a device) is a misconfiguration
/// nobody would find. The process-wide EAL is set up by whoever initialized
/// it, and a node on it is given a port, not arguments.
pub(crate) fn check_shared_eal_args(node_eal_args: &[String]) -> Result<(), String> {
    if node_eal_args.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "this process runs a process-wide DPDK EAL, which the node shares, so its own EAL \
             arguments ({}) cannot take effect; leave them empty and give the node its port",
            node_eal_args.join(" ")
        ))
    }
}

/// Which of the NIC groups on one EAL a group is: the one the EAL was
/// acquired for, or one brought up beside it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EalGroup {
    /// The group the EAL was acquired for: a node's client NIC, or a
    /// client's NIC handle. The only group of every process but a node
    /// whose replication has a NIC of its own.
    First,
    /// A group brought up beside the first, on its EAL: a node's
    /// replication NIC.
    Further,
}

/// The name of a NIC group's mbuf pool.
///
/// DPDK pool names are unique per process. The first group on an EAL the
/// node owns is alone in its process but for the node's further groups, and
/// keeps the fixed name. Every other group, a further group of the node or
/// any group on the process-wide EAL (whose nodes can run at once), is named
/// after its first port, which no two groups running at once share
/// ([`check_further_group`] refuses a further group a port of the first's,
/// and the process-wide EAL's nodes each have ports of their own). A group
/// that stops frees its pool, so the next group on that port can take the
/// name again.
pub(crate) fn mempool_name(shared_eal: bool, group: EalGroup, first_port: u16) -> String {
    if shared_eal || group == EalGroup::Further {
        format!("{OWN_EAL_POOL}_{first_port}")
    } else {
        OWN_EAL_POOL.to_owned()
    }
}

/// Refuse a further NIC group (`further`) that cannot sit beside the first
/// (`first`) on its EAL, before EAL is touched.
///
/// - **Its EAL arguments** must be the first group's, or none: the EAL is
///   acquired once, from the first group's, so any other arguments could
///   only be ignored (as [`check_shared_eal_args`] refuses for the same
///   reason). The node's replication NIC takes the client NIC's, so its own
///   are the same.
/// - **Its ports** must be its own: a port is configured and started by one
///   owner, and two groups would also name their pools alike.
pub(crate) fn check_further_group(first: &NicConfig, further: &NicConfig) -> Result<(), String> {
    if !further.eal_args.is_empty() && further.eal_args != first.eal_args {
        return Err(format!(
            "a second DPDK NIC shares the first NIC's EAL, so its own EAL arguments ({}) cannot \
             take effect; give it the first NIC's ({}) or none, and allow both NICs' devices in \
             those",
            further.eal_args.join(" "),
            first.eal_args.join(" ")
        ));
    }
    if let Some(port) = further.port_shared_with(first) {
        return Err(format!(
            "DPDK port {port} is in both NICs; each NIC needs ports of its own"
        ));
    }
    Ok(())
}

/// A NIC group's resources (`R`: its ports and mbuf pool) and its share of
/// the EAL (`E`) they sit on, released in that order.
///
/// The EAL is shared by every group on it through an `Arc`, so that it is
/// cleaned up only once the last group has released its resources:
/// `rte_mempool_free` and closing a port need EAL alive. Generic so that
/// this order is tested without libdpdk.
pub(crate) struct OnEal<R, E> {
    /// Declared before `_eal`, so dropped before it: Rust drops fields in
    /// declaration order. Held for their drop, hence the underscores (the
    /// resources are lent out too, by [`Self::resources`]).
    _resources: R,
    _eal: Arc<E>,
}

impl<R, E> OnEal<R, E> {
    pub(crate) fn new(resources: R, eal: Arc<E>) -> Self {
        OnEal {
            _resources: resources,
            _eal: eal,
        }
    }

    /// The resources, for what they offer besides their drop (a NIC
    /// group's steering rules to remove at promotion), while the EAL
    /// under them is held.
    pub(crate) fn resources(&self) -> &R {
        &self._resources
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    #[test]
    fn a_node_owning_eal_keeps_the_pool_name_it_always_had() {
        assert_eq!(mempool_name(false, EalGroup::First, 0), "pktmbuf_pool");
        assert_eq!(mempool_name(false, EalGroup::First, 3), "pktmbuf_pool");
    }

    #[test]
    fn nodes_sharing_eal_name_their_pools_after_their_port() {
        assert_eq!(mempool_name(true, EalGroup::First, 0), "pktmbuf_pool_0");
        assert_ne!(
            mempool_name(true, EalGroup::First, 1),
            mempool_name(true, EalGroup::First, 2)
        );
    }

    /// A node's replication NIC beside its client NIC: on an EAL the node
    /// owns, the client NIC keeps the fixed name and the replication NIC's
    /// pool is named after its port, so the two never collide, whatever
    /// the ports; on the process-wide EAL both are named after their
    /// ports, which are distinct.
    #[test]
    fn a_further_group_names_its_pool_after_its_port() {
        for first_port in [0, 1, 7] {
            for further_port in [0, 1, 7] {
                let own_first = mempool_name(false, EalGroup::First, first_port);
                let own_further = mempool_name(false, EalGroup::Further, further_port);
                assert_eq!(own_further, format!("pktmbuf_pool_{further_port}"));
                assert_ne!(own_first, own_further);
                if first_port != further_port {
                    assert_ne!(
                        mempool_name(true, EalGroup::First, first_port),
                        mempool_name(true, EalGroup::Further, further_port)
                    );
                }
            }
        }
    }

    /// `RTE_MEMPOOL_NAMESIZE`: 26 bytes, NUL included, once DPDK has
    /// taken its ring and pool prefixes out of the 32-byte memzone name
    /// (`rte_mempool.h`). A longer name fails pool creation at startup.
    #[test]
    fn the_longest_pool_name_fits_dpdks_limit() {
        assert!(mempool_name(true, EalGroup::First, u16::MAX).len() < 26);
        assert!(mempool_name(false, EalGroup::Further, u16::MAX).len() < 26);
    }

    #[test]
    fn a_node_sharing_eal_takes_no_eal_arguments() {
        assert!(check_shared_eal_args(&[]).is_ok());
        let err = check_shared_eal_args(&["-l".into(), "3".into()]).expect_err("refused");
        assert!(err.contains("-l 3"), "{err}");
    }

    fn nic(eal_args: &[&str], port_ids: &[u16]) -> NicConfig {
        NicConfig {
            eal_args: eal_args.iter().map(|&arg| arg.to_owned()).collect(),
            port_ids: port_ids.to_vec(),
            ..NicConfig::default()
        }
    }

    #[test]
    fn a_further_group_takes_the_first_groups_eal_arguments_or_none() {
        let both_devices = ["-l", "0-3", "-a", "0000:01:00.0", "-a", "0000:02:00.0"];
        let first = nic(&both_devices, &[0]);
        assert_eq!(
            check_further_group(&first, &nic(&both_devices, &[1])),
            Ok(())
        );
        assert_eq!(check_further_group(&first, &nic(&[], &[1])), Ok(()));

        let err = check_further_group(&first, &nic(&["-l", "4"], &[1])).expect_err("refused");
        assert!(err.contains("(-l 4)"), "{err}");
        assert!(err.contains("-a 0000:02:00.0"), "{err}");
    }

    #[test]
    fn a_further_group_needs_ports_of_its_own() {
        let first = nic(&[], &[0, 1]);
        assert_eq!(check_further_group(&first, &nic(&[], &[2, 3])), Ok(()));
        let err = check_further_group(&first, &nic(&[], &[2, 1])).expect_err("refused");
        assert!(err.contains("port 1 "), "{err}");
    }

    /// What a group's resources or the EAL record when they drop.
    struct Dropped<'a> {
        name: &'static str,
        log: &'a Mutex<Vec<&'static str>>,
    }

    impl Drop for Dropped<'_> {
        fn drop(&mut self) {
            self.log.lock().unwrap().push(self.name);
        }
    }

    /// The EAL outlives every group's resources, whichever group goes
    /// last, and is released with the last of them: a node's teardown
    /// frees its ports and pools before `rte_eal_cleanup`, as it did with
    /// one group.
    #[test]
    fn the_eal_is_released_after_the_last_groups_resources() {
        for client_first in [true, false] {
            let log = Mutex::new(Vec::new());
            let eal = Arc::new(Dropped {
                name: "eal",
                log: &log,
            });
            let client = OnEal::new(
                Dropped {
                    name: "client",
                    log: &log,
                },
                Arc::clone(&eal),
            );
            let replication = OnEal::new(
                Dropped {
                    name: "replication",
                    log: &log,
                },
                eal,
            );
            assert_eq!(client._resources.name, "client");
            assert!(Arc::ptr_eq(&client._eal, &replication._eal));

            if client_first {
                drop(client);
                assert_eq!(*log.lock().unwrap(), ["client"]);
                drop(replication);
                assert_eq!(*log.lock().unwrap(), ["client", "replication", "eal"]);
            } else {
                drop(replication);
                assert_eq!(*log.lock().unwrap(), ["replication"]);
                drop(client);
                assert_eq!(*log.lock().unwrap(), ["replication", "client", "eal"]);
            }
        }
    }

    #[test]
    fn a_lone_group_releases_its_resources_then_the_eal() {
        let log = Mutex::new(Vec::new());
        let group = OnEal::new(
            Dropped {
                name: "group",
                log: &log,
            },
            Arc::new(Dropped {
                name: "eal",
                log: &log,
            }),
        );
        drop(group);
        assert_eq!(*log.lock().unwrap(), ["group", "eal"]);
    }
}
