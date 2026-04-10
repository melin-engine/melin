//! Build script for DPDK FFI bindings.
//!
//! 1. Uses pkg-config to locate the DPDK installation.
//! 2. Compiles C wrapper functions for DPDK static inline functions
//!    (rx_burst, tx_burst, pktmbuf_alloc/free) that bindgen can't handle.
//! 3. Generates Rust FFI bindings via bindgen for non-inline DPDK functions
//!    and our wrapper functions.
//!
//! Requires DPDK >= 22.11 installed and discoverable via pkg-config.

fn main() {
    #[cfg(feature = "dpdk-sys")]
    generate_bindings();
}

#[cfg(feature = "dpdk-sys")]
fn generate_bindings() {
    // Locate DPDK via pkg-config.
    let dpdk = pkg_config::Config::new()
        .atleast_version("22.11")
        .probe("libdpdk")
        .expect(
            "DPDK not found. Install DPDK >= 22.11 and ensure pkg-config can find it.\n\
             On Fedora: dnf install dpdk-devel\n\
             On Ubuntu: apt install libdpdk-dev\n\
             Verify: pkg-config --cflags --libs libdpdk",
        );

    // Collect include paths.
    let include_args: Vec<String> = dpdk
        .include_paths
        .iter()
        .map(|p| format!("-I{}", p.display()))
        .collect();

    // Compile the C wrapper functions for inline DPDK functions.
    // Use the system clang explicitly — the Android NDK clang (if present
    // in PATH) fails on DPDK's AVX intrinsic headers.
    let system_clang = find_system_clang();
    let mut cc = cc::Build::new();
    cc.file("src/dpdk/inline_wrappers.c");
    if let Some(ref clang) = system_clang {
        cc.compiler(clang);
    }
    for path in &dpdk.include_paths {
        cc.include(path);
    }
    // DPDK headers require these on some platforms.
    cc.flag_if_supported("-march=native");
    cc.compile("dpdk_wrappers");

    // Find the system clang's resource directory for system headers (stddef.h,
    // AVX intrinsics). Passing --resource-dir forces bindgen's internal
    // libclang to use the system clang's headers instead of whichever clang
    // happens to be first in PATH (e.g., Android NDK clang, which has
    // broken AVX headers).
    let clang_extra: Vec<String> = if let Some(resource_dir) = find_clang_resource_dir() {
        vec![
            format!("-I{}/include", resource_dir),
            format!("-resource-dir={}", resource_dir),
        ]
    } else {
        Vec::new()
    };

    // Generate bindings for:
    // - Non-inline DPDK functions (rte_eal_init, rte_eth_dev_configure, etc.)
    // - Our C wrapper functions (dpdk_eth_rx_burst, dpdk_pktmbuf_alloc, etc.)
    // - Struct types (rte_mbuf, rte_mempool, rte_eth_conf, etc.)
    let bindings = bindgen::Builder::default()
        .clang_args(&clang_extra)
        .header_contents(
            "dpdk_wrapper.h",
            "\
            #include <rte_eal.h>\n\
            #include <rte_ethdev.h>\n\
            #include <rte_mbuf.h>\n\
            #include <rte_mempool.h>\n\
            #include <rte_lcore.h>\n\
            \n\
            /* Declarations for our C wrappers (defined in inline_wrappers.c) */\n\
            uint16_t dpdk_eth_rx_burst(uint16_t port_id, uint16_t queue_id,\n\
                                       struct rte_mbuf **rx_pkts, uint16_t nb_pkts);\n\
            uint16_t dpdk_eth_tx_burst(uint16_t port_id, uint16_t queue_id,\n\
                                       struct rte_mbuf **tx_pkts, uint16_t nb_pkts);\n\
            struct rte_mbuf *dpdk_pktmbuf_alloc(struct rte_mempool *mp);\n\
            void dpdk_pktmbuf_free(struct rte_mbuf *m);\n\
            void dpdk_mbuf_refcnt_update(struct rte_mbuf *m, int16_t value);\n\
            void dpdk_mempool_free(struct rte_mempool *mp);\n\
            uint16_t dpdk_mbuf_default_buf_size(void);\n\
            uint16_t dpdk_mbuf_data_off(const struct rte_mbuf *m);\n\
            uint16_t dpdk_mbuf_data_len(const struct rte_mbuf *m);\n\
            void dpdk_mbuf_set_data_len(struct rte_mbuf *m, uint16_t len);\n\
            uint32_t dpdk_mbuf_pkt_len(const struct rte_mbuf *m);\n\
            void dpdk_mbuf_set_pkt_len(struct rte_mbuf *m, uint32_t len);\n\
            void *dpdk_mbuf_buf_addr(const struct rte_mbuf *m);\n\
            uint64_t dpdk_mbuf_ol_flags(const struct rte_mbuf *m);\n\
            void dpdk_mbuf_set_ol_flags(struct rte_mbuf *m, uint64_t flags);\n\
            void dpdk_mbuf_set_tx_offload(struct rte_mbuf *m,\n\
                                           uint64_t l2_len, uint64_t l3_len,\n\
                                           uint64_t l4_len);\n\
            uint64_t dpdk_tx_offload_ipv4_cksum(void);\n\
            uint64_t dpdk_tx_offload_tcp_cksum(void);\n\
            uint64_t dpdk_rx_offload_checksum(void);\n\
            uint64_t dpdk_tx_offload_checksum(void);\n\
            uint64_t dpdk_rx_offload_vlan_strip(void);\n\
            uint64_t dpdk_tx_offload_vlan_insert(void);\n\
            uint64_t dpdk_tx_vlan_flag(void);\n\
            void dpdk_mbuf_set_vlan_tci(struct rte_mbuf *m, uint16_t vlan_tci);\n\
            uint64_t dpdk_eth_rss_ip(void);\n\
            uint64_t dpdk_eth_rss_tcp(void);\n\
            uint64_t dpdk_eth_mq_rx_rss(void);\n\
            ",
        )
        .clang_args(&include_args)
        // Non-inline DPDK functions.
        .allowlist_function("rte_eal_init")
        .allowlist_function("rte_eal_cleanup")
        .allowlist_function("rte_pktmbuf_pool_create")
        .allowlist_function("rte_eth_dev_configure")
        .allowlist_function("rte_eth_dev_count_avail")
        .allowlist_function("rte_eth_dev_info_get")
        .allowlist_function("rte_eth_dev_start")
        .allowlist_function("rte_eth_dev_stop")
        .allowlist_function("rte_eth_rx_queue_setup")
        .allowlist_function("rte_eth_tx_queue_setup")
        .allowlist_function("rte_eth_dev_socket_id")
        .allowlist_function("rte_eth_macaddr_get")
        .allowlist_function("rte_eth_promiscuous_enable")
        .allowlist_function("rte_eth_link_get_nowait")
        .allowlist_function("rte_socket_id")
        // Our C wrapper functions.
        .allowlist_function("dpdk_eth_rx_burst")
        .allowlist_function("dpdk_eth_tx_burst")
        .allowlist_function("dpdk_pktmbuf_alloc")
        .allowlist_function("dpdk_pktmbuf_free")
        .allowlist_function("dpdk_mbuf_refcnt_update")
        .allowlist_function("dpdk_mempool_free")
        .allowlist_function("dpdk_mbuf_default_buf_size")
        .allowlist_function("dpdk_mbuf_data_off")
        .allowlist_function("dpdk_mbuf_data_len")
        .allowlist_function("dpdk_mbuf_set_data_len")
        .allowlist_function("dpdk_mbuf_pkt_len")
        .allowlist_function("dpdk_mbuf_set_pkt_len")
        .allowlist_function("dpdk_mbuf_buf_addr")
        .allowlist_function("dpdk_mbuf_ol_flags")
        .allowlist_function("dpdk_mbuf_set_ol_flags")
        .allowlist_function("dpdk_mbuf_set_tx_offload")
        .allowlist_function("dpdk_tx_offload_ipv4_cksum")
        .allowlist_function("dpdk_tx_offload_tcp_cksum")
        .allowlist_function("dpdk_rx_offload_checksum")
        .allowlist_function("dpdk_tx_offload_checksum")
        .allowlist_function("dpdk_rx_offload_vlan_strip")
        .allowlist_function("dpdk_tx_offload_vlan_insert")
        .allowlist_function("dpdk_tx_vlan_flag")
        .allowlist_function("dpdk_mbuf_set_vlan_tci")
        // RSS constants.
        .allowlist_function("dpdk_eth_rss_ip")
        .allowlist_function("dpdk_eth_rss_tcp")
        .allowlist_function("dpdk_eth_mq_rx_rss")
        // Types.
        .allowlist_type("rte_mbuf")
        .allowlist_type("rte_mempool")
        .allowlist_type("rte_eth_conf")
        .allowlist_type("rte_eth_dev_info")
        .allowlist_type("rte_eth_link")
        .allowlist_type("rte_eth_rxconf")
        .allowlist_type("rte_eth_txconf")
        .allowlist_type("rte_ether_addr")
        // Derive traits for FFI types.
        .derive_default(true)
        .derive_debug(true)
        // Use core types where possible.
        .use_core()
        .generate()
        .expect("failed to generate DPDK bindings");

    let out_path = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("dpdk_bindings.rs"))
        .expect("failed to write dpdk_bindings.rs");
}

/// Find the system clang, preferring /usr/bin/clang over whatever is in PATH.
/// This avoids the Android NDK clang which can't compile DPDK's AVX headers.
#[cfg(feature = "dpdk-sys")]
fn find_system_clang() -> Option<String> {
    for path in ["/usr/bin/clang", "/usr/local/bin/clang"] {
        if std::path::Path::new(path).exists() {
            return Some(path.to_string());
        }
    }
    None
}

#[cfg(feature = "dpdk-sys")]
/// Find the clang resource directory containing stddef.h.
fn find_clang_resource_dir() -> Option<String> {
    // Use system clang to find the resource directory, matching the
    // compiler used for the wrapper compilation above.
    let clang = find_system_clang().unwrap_or_else(|| "clang".to_string());
    if let Ok(output) = std::process::Command::new(&clang)
        .arg("--print-resource-dir")
        .output()
        && output.status.success()
    {
        let dir = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if std::path::Path::new(&format!("{dir}/include/stddef.h")).exists() {
            return Some(dir);
        }
    }

    // Fallback: scan /usr/lib/clang/*/include/stddef.h.
    if let Ok(entries) = std::fs::read_dir("/usr/lib/clang") {
        for entry in entries.flatten() {
            let candidate = entry.path().join("include/stddef.h");
            if candidate.exists() {
                return entry.path().to_str().map(String::from);
            }
        }
    }

    None
}
