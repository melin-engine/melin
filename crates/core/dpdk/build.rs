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

/// Where the C wrappers and the header they share with the bindings are,
/// relative to the package root, which a build script runs in.
#[cfg(feature = "dpdk-sys")]
const WRAPPER_DIR: &str = "src/dpdk";

#[cfg(feature = "dpdk-sys")]
fn generate_bindings() {
    // Cargo re-runs a build script whenever any file in the package changes —
    // but ONLY until the script emits its first `cargo:rerun-if-*` directive,
    // after which the listed conditions become the complete set. `pkg_config`
    // below emits a long list of `rerun-if-env-changed` lines, so without
    // these two the C wrapper is never recompiled when its source changes:
    // editing `inline_wrappers.c` silently does nothing until `cargo clean`,
    // and a binary can be built against a stale object.
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=src/dpdk/inline_wrappers.c");
    println!("cargo:rerun-if-changed=src/dpdk/steering_rule.h");

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
    cc.include(WRAPPER_DIR);
    if let Some(ref clang) = system_clang {
        cc.compiler(clang);
    }
    for path in &dpdk.include_paths {
        cc.include(path);
    }
    // The target's ISA, not the build machine's: both are the same in a
    // native build, and a cross build (the LAN suite builds on the operator's
    // machine for the bench hosts) compiles for the CPU rustc is told about.
    if std::env::var("CARGO_CFG_TARGET_ARCH").as_deref() == Ok("x86_64") {
        // DPDK's inline headers use SSSE3 and SSE4.2 intrinsics
        // unconditionally (`rte_memcpy.h` reaches `_mm_alignr_epi8`), so an
        // ISA baseline is a hard requirement rather than a tuning choice.
        // x86-64-v2 — SSE3, SSSE3, SSE4.1, SSE4.2, POPCNT — is DPDK's own x86
        // minimum.
        //
        // This used to be `flag_if_supported("-march=native")` alone, which
        // turned that requirement into a best-effort: clang cannot always
        // probe the host CPU (hosted CI runners are the case that caught us),
        // the flag was then dropped without comment, and the build failed
        // deep inside DPDK's headers with "needs target feature ssse3" — on
        // some machines and not others.
        cc.flag("-march=x86-64-v2");
    }
    // Then the CPU the Rust code is built for, so the wrapper matches it. A
    // later `-march` wins, so this raises the baseline; losing it costs
    // codegen quality instead of breaking the build.
    match rust_target_cpu().as_deref() {
        // `.cargo/config.toml`'s default: the build machine, when clang can
        // detect it. Only meaningful in a native build; a cross build names
        // its CPU.
        Some("native") | None => {
            if std::env::var("HOST") == std::env::var("TARGET") {
                cc.flag_if_supported("-march=native");
            }
        }
        // rustc's CPU names are LLVM's, which clang takes: as `-march` on
        // x86, as `-mcpu` elsewhere.
        Some(cpu) if std::env::var("CARGO_CFG_TARGET_ARCH").as_deref() == Ok("x86_64") => {
            cc.flag(format!("-march={cpu}"));
        }
        Some(cpu) => {
            cc.flag(format!("-mcpu={cpu}"));
        }
    }
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
            #include <rte_dev.h>\n\
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
            uint64_t dpdk_rx_ip_cksum_mask(void);\n\
            uint64_t dpdk_rx_ip_cksum_good(void);\n\
            uint64_t dpdk_rx_ip_cksum_bad(void);\n\
            uint64_t dpdk_rx_ip_cksum_none(void);\n\
            uint64_t dpdk_rx_l4_cksum_mask(void);\n\
            uint64_t dpdk_rx_l4_cksum_good(void);\n\
            uint64_t dpdk_rx_l4_cksum_bad(void);\n\
            uint64_t dpdk_rx_l4_cksum_none(void);\n\
            uint64_t dpdk_rx_offload_checksum(void);\n\
            uint64_t dpdk_tx_offload_checksum(void);\n\
            uint64_t dpdk_rx_offload_vlan_strip(void);\n\
            uint64_t dpdk_tx_offload_vlan_insert(void);\n\
            uint64_t dpdk_tx_vlan_flag(void);\n\
            void dpdk_mbuf_set_vlan_tci(struct rte_mbuf *m, uint16_t vlan_tci);\n\
            uint64_t dpdk_eth_rss_ip(void);\n\
            uint64_t dpdk_eth_rss_tcp(void);\n\
            uint64_t dpdk_eth_mq_rx_rss(void);\n\
            int dpdk_flow_isolate(uint16_t port_id);\n\
            int dpdk_thread_register(void);\n\
            unsigned int dpdk_lcore_id(void);\n\
            #include \"steering_rule.h\"\n\
            ",
        )
        .clang_args(&include_args)
        .clang_arg(format!("-I{WRAPPER_DIR}"))
        // Non-inline DPDK functions.
        .allowlist_function("rte_eal_init")
        .allowlist_function("rte_eal_cleanup")
        // Virtual-device hotplug, for a process hosting several nodes on
        // one EAL (`Eal::attach_vdev`).
        .allowlist_function("rte_eal_hotplug_add")
        .allowlist_function("rte_eal_hotplug_remove")
        .allowlist_function("rte_eth_dev_get_port_by_name")
        .allowlist_function("rte_pktmbuf_pool_create")
        .allowlist_function("rte_eth_dev_configure")
        .allowlist_function("rte_eth_dev_count_avail")
        .allowlist_function("rte_eth_dev_info_get")
        .allowlist_function("rte_eth_dev_start")
        .allowlist_function("rte_eth_dev_stop")
        .allowlist_function("rte_eth_dev_close")
        .allowlist_function("rte_eth_rx_queue_setup")
        .allowlist_function("rte_eth_tx_queue_setup")
        .allowlist_function("rte_eth_dev_socket_id")
        .allowlist_function("rte_eth_macaddr_get")
        .allowlist_function("rte_eth_promiscuous_enable")
        .allowlist_function("rte_eth_link_get_nowait")
        .allowlist_function("rte_socket_id")
        // A poll thread of the node's own registered as an lcore
        // (`LcoreRegistration`), for its mempool cache.
        .allowlist_function("rte_thread_unregister")
        .allowlist_function("dpdk_thread_register")
        .allowlist_function("dpdk_lcore_id")
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
        .allowlist_function("dpdk_rx_ip_cksum_mask")
        .allowlist_function("dpdk_rx_ip_cksum_good")
        .allowlist_function("dpdk_rx_ip_cksum_bad")
        .allowlist_function("dpdk_rx_ip_cksum_none")
        .allowlist_function("dpdk_rx_l4_cksum_mask")
        .allowlist_function("dpdk_rx_l4_cksum_good")
        .allowlist_function("dpdk_rx_l4_cksum_bad")
        .allowlist_function("dpdk_rx_l4_cksum_none")
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
        // rte_flow steering for bifurcated PMD mode.
        .allowlist_function("dpdk_flow_isolate")
        .allowlist_function("dpdk_install_steering_rule")
        .allowlist_function("dpdk_pin_rss_reta")
        // Types.
        .allowlist_type("dpdk_steering_rule")
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

/// The CPU rustc builds this crate for (`-C target-cpu`), the last one
/// given winning as it does for rustc, or `None` when none is.
#[cfg(feature = "dpdk-sys")]
fn rust_target_cpu() -> Option<String> {
    // Cargo hands a build script the target's flags joined by 0x1f.
    let flags = std::env::var("CARGO_ENCODED_RUSTFLAGS").unwrap_or_default();
    let mut cpu = None;
    let mut after_c = false;
    for flag in flags.split('\x1f') {
        let codegen = if after_c {
            Some(flag)
        } else {
            flag.strip_prefix("-C")
        };
        after_c = flag == "-C";
        if let Some(value) = codegen.and_then(|c| c.strip_prefix("target-cpu=")) {
            cpu = Some(value.to_string());
        }
    }
    cpu
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
