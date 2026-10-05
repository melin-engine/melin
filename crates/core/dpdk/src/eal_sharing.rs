//! What changes for a node when its process runs a process-wide EAL
//! (`Eal::init_process_wide`) instead of the node initializing its own.
//!
//! Plain logic with no libdpdk dependency, so it is tested on every host,
//! like `mac` and `rx_checksum`. `DpdkShared::init` is its only caller.

// Without `dpdk-sys` nothing but the tests calls into this module.
#![cfg_attr(not(feature = "dpdk-sys"), allow(dead_code))]

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

/// The name of a node's mbuf pool.
///
/// DPDK pool names are unique per process. A node that owns EAL is alone
/// in its process and keeps the fixed name. Nodes sharing the process-wide
/// EAL can run at once, so theirs is named after the node's first port,
/// which no two running nodes share. A node that stops frees its pool, so
/// the next node on that port can take the name again.
pub(crate) fn mempool_name(shared_eal: bool, first_port: u16) -> String {
    if shared_eal {
        format!("{OWN_EAL_POOL}_{first_port}")
    } else {
        OWN_EAL_POOL.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_node_owning_eal_keeps_the_pool_name_it_always_had() {
        assert_eq!(mempool_name(false, 0), "pktmbuf_pool");
        assert_eq!(mempool_name(false, 3), "pktmbuf_pool");
    }

    #[test]
    fn nodes_sharing_eal_name_their_pools_after_their_port() {
        assert_eq!(mempool_name(true, 0), "pktmbuf_pool_0");
        assert_ne!(mempool_name(true, 1), mempool_name(true, 2));
    }

    /// `RTE_MEMPOOL_NAMESIZE`: 26 bytes, NUL included, once DPDK has
    /// taken its ring and pool prefixes out of the 32-byte memzone name
    /// (`rte_mempool.h`). A longer name fails pool creation at startup.
    #[test]
    fn the_longest_pool_name_fits_dpdks_limit() {
        assert!(mempool_name(true, u16::MAX).len() < 26);
    }

    #[test]
    fn a_node_sharing_eal_takes_no_eal_arguments() {
        assert!(check_shared_eal_args(&[]).is_ok());
        let err = check_shared_eal_args(&["-l".into(), "3".into()]).expect_err("refused");
        assert!(err.contains("-l 3"), "{err}");
    }
}
