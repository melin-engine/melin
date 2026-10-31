//! Build script for DPDK FFI bindings.
//!
//! 1. Uses pkg-config to locate the DPDK installation.
//! 2. Compiles C wrapper functions for DPDK static inline functions
//!    (rx_burst, tx_burst, pktmbuf_alloc/free) that bindgen can't handle.
//! 3. Generates Rust FFI bindings via bindgen for non-inline DPDK functions
//!    and our wrapper functions.
//!
//! Requires DPDK >= 22.11 installed and discoverable via pkg-config.

// Which clang headers bindgen parses with; its unit tests are in the crate
// (see the module's documentation).
#[cfg(feature = "dpdk-sys")]
#[path = "build/resource_dir.rs"]
mod resource_dir;

fn main() {
    #[cfg(feature = "dpdk-sys")]
    generate_bindings();
}

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
    println!("cargo:rerun-if-changed=build/resource_dir.rs");
    println!("cargo:rerun-if-changed=src/dpdk/inline_wrappers.c");
    // clang-sys's choice of libclang, and so the headers chosen for it
    // below. Its own build script declares this, but for itself only.
    println!("cargo:rerun-if-env-changed=LIBCLANG_PATH");

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

    // The clang headers (stddef.h, the intrinsics) of the libclang bindgen
    // loads, which need not be the `clang` binary's: see
    // `bindgen_resource_dir_args`.
    let clang_extra = bindgen_resource_dir_args();

    // Generate bindings for:
    // - Non-inline DPDK functions (rte_eal_init, rte_eth_dev_configure, etc.)
    // - Our C wrapper functions (dpdk_eth_rx_burst, dpdk_pktmbuf_alloc, etc.)
    // - Struct types (rte_mbuf, rte_mempool, rte_eth_conf, etc.)
    let bindings = bindgen::Builder::default()
        // bindgen otherwise runs the first `clang` it finds (`CLANG_PATH`,
        // `llvm-config --bindir`, then `PATH`) and adds that compiler's
        // search paths as `-isystem`, its resource headers among them — which
        // outrank libclang's own, so a `clang` of another LLVM release (a
        // distribution default newer than the libclang clang-sys picks, an
        // Android NDK's first in PATH) breaks the parse in the intrinsics
        // headers. libclang derives the system search paths itself, as the
        // compiler of its release would, so nothing is lost.
        .detect_include_paths(false)
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
            int dpdk_install_src_ipv4_steering(uint16_t port_id,\n\
                                                uint32_t src_ipv4_be,\n\
                                                int *err_type);\n\
            ",
        )
        .clang_args(&include_args)
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
        .allowlist_function("dpdk_install_src_ipv4_steering")
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

/// The `-resource-dir` argument naming the clang headers of the libclang
/// bindgen loads, or none, leaving libclang its built-in default.
///
/// clang-sys picks the libclang (`LIBCLANG_PATH`, else the newest it finds),
/// which need not be the `clang` binary's release, and another release's
/// headers fail the parse (see `build/resource_dir.rs`). The default is not
/// relied on when a match exists: libclang derives it from the path it was
/// loaded through, which a distribution's symlink can make a directory that
/// does not exist.
#[cfg(feature = "dpdk-sys")]
fn bindgen_resource_dir_args() -> Vec<String> {
    // Loads libclang on this thread, as `generate` does later: bindgen
    // keeps the one instance it loads for the process.
    let version = bindgen::clang_version();
    let library = clang_sys::get_library()
        .expect("bindgen::clang_version loads libclang on the calling thread");
    let libclang = library.path();
    let Some((major, minor)) = version.parsed else {
        println!(
            "cargo:warning=bindgen loads libclang from {}, whose version ({:?}) could not \
             be read; generating the DPDK bindings with libclang's built-in headers",
            libclang.display(),
            version.full,
        );
        return Vec::new();
    };
    // An unresolvable path still names the library as loaded, which is
    // searched anyway; resolving it only adds a second place to look.
    let canonical = std::fs::canonicalize(libclang).unwrap_or_else(|_| libclang.to_path_buf());
    let candidates =
        resource_dir::subdirectories(&resource_dir::roots(libclang, &canonical, major));
    match resource_dir::select(major, candidates, system_clang_resource_dir) {
        Ok(dir) => {
            // Not a directive: cargo keeps it in the build script's output
            // file and shows it under `-vv`.
            println!(
                "libclang {major}.{minor} at {}: clang resource directory {dir}",
                libclang.display()
            );
            vec![format!("-resource-dir={dir}")]
        }
        Err(skipped) => {
            let skipped = if skipped.is_empty() {
                "none found".to_string()
            } else {
                skipped.join(", ")
            };
            println!(
                "cargo:warning=bindgen loads libclang {major}.{minor} from {}, and no clang \
                 resource directory of that version was found (examined: {skipped}); \
                 generating the DPDK bindings with libclang's built-in headers. If that \
                 fails, install the clang headers of that LLVM version, or point \
                 LIBCLANG_PATH at a libclang whose headers are installed.",
                libclang.display()
            );
            Vec::new()
        }
    }
}

/// The system clang's resource directory, as it reports it: the last
/// candidate, consulted only when no primary candidate matched.
/// `None` when there is no system clang or it does not answer.
#[cfg(feature = "dpdk-sys")]
fn system_clang_resource_dir() -> Option<std::path::PathBuf> {
    let clang = find_system_clang()?;
    // A clang that cannot run or answer offers no candidate; the search goes
    // on without it, and a miss overall is reported with what was examined.
    let output = std::process::Command::new(clang)
        .arg("--print-resource-dir")
        .output()
        .ok()
        .filter(|output| output.status.success())?;
    let dir = String::from_utf8(output.stdout).ok()?;
    Some(std::path::PathBuf::from(dir.trim()))
}
