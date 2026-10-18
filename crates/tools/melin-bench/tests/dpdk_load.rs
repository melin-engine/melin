//! The binary over DPDK against an echo node on DPDK: several streams on
//! one NIC, every request answered in the node's journal. Runs only with
//! the `dpdk` feature, under `scripts/dpdk/netns-runner.sh`: the binary
//! takes the runner's client veth pair in a process of its own
//! (`melin_test_node::dpdk_client_process_eal_args`).

mod common;

use std::net::SocketAddr;

use melin_client::fmt_mac;

use common::{field, journaled_echoes, load, run, start_echo};

#[test]
fn every_echo_answered_over_dpdk_is_journaled() {
    let dir = tempfile::tempdir().expect("tempdir");
    let node = start_echo(dir.path(), "writer");
    let SocketAddr::V4(server) = node.addr() else {
        panic!("a DPDK node is on IPv4: {}", node.addr());
    };
    let seat = melin_test_node::dpdk_client();
    let addr = server.to_string();
    let ip = seat.ip.to_string();
    let prefix_len = seat.prefix_len.to_string();
    let node_mac = fmt_mac(melin_test_node::mac_of(node.addr()));
    // In full, with `=`: the value starts with a dash.
    let eal = format!(
        "--dpdk-eal-args={}",
        melin_test_node::dpdk_client_process_eal_args("melin-bench-test").join(" ")
    );
    let dpdk = [
        "--app",
        "echo",
        "--size",
        "48",
        "--transport",
        "dpdk",
        "--dpdk-ip",
        &ip,
        "--dpdk-prefix-len",
        &prefix_len,
        "--dpdk-peer-mac",
        &node_mac,
        &eal,
    ];

    let json = load(dir.path(), &[&dpdk[..], &["--addr", &addr]].concat());

    // A loopback address is refused before the NIC comes up.
    let refused = run(
        dir.path(),
        &[&dpdk[..], &["--addr", "127.0.0.1:9876"]].concat(),
    );
    node.stop();
    assert_eq!(refused.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&refused.stderr);
    assert!(stderr.contains("loopback"), "{stderr}");

    assert_eq!(
        journaled_echoes(dir.path(), 48),
        field(&json, "completed_requests"),
        "{json}"
    );
}
