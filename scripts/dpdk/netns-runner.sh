#!/usr/bin/env bash
# Cargo target runner: run a test binary in namespaces of its own, on a
# network DPDK nodes can use. No NIC, no hugepages, no root.
#
# Set as cargo's target runner, which nextest honours. For each process it
# starts (each test, under nextest), it:
#
#   1. re-executes itself under `unshare -rnm` (new user, network and
#      mount namespaces);
#   2. builds a bridge carrying the client's address, with one veth pair
#      per node slot: the node's end for DPDK, the other end on the
#      bridge. Every interface up, TX checksum offload off on every veth
#      (veth-setup.py), `lo` up. The nodes reach each other (replication)
#      and the client reaches every node;
#   3. mounts a private tmpfs on /var/run, where EAL insists on creating
#      its runtime directory;
#   4. publishes the layout in the environment (below);
#   5. execs the test binary with its arguments.
#
# Nothing outside the namespaces is touched: the links, the addresses and
# the tmpfs vanish with the process. Tests built with a `dpdk` feature
# start their nodes through the shared launcher (crates/core/test-node),
# which reads the layout and refuses to run without it. See
# docs/internal/dpdk-transparent-tests.md.
#
# Usage (one test at a time: every DPDK node busy-polls a core):
#   CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER="$PWD/scripts/dpdk/netns-runner.sh" cargo nextest run -p melin-example-echo --features dpdk -j 1
#
# Or by hand: scripts/dpdk/netns-runner.sh BINARY [ARGS...]
#
# Prerequisites:
#   - libdpdk (the runtime libraries, including the af_packet PMD)
#   - util-linux `unshare`, and python3 (standard library only) for the
#     network setup: neither iproute2 nor ethtool is needed
#   - unprivileged user namespaces. Ubuntu 24.04 restricts them through
#     AppArmor; lift that with
#       sudo sysctl -w kernel.apparmor_restrict_unprivileged_userns=0
#   - the veth and bridge drivers, loaded: a user namespace cannot load a
#     module (`sudo modprobe -a veth bridge`; Docker loads both)
#
# Layout published to the test (space-separated lists, one entry per node
# slot, in slot order; three slots, the largest cluster a test runs):
#   MELIN_NETNS_DPDK_IFACES   interfaces DPDK attaches to (no address)
#   MELIN_NETNS_NODE_IPS      the IP each slot's node owns on its interface
#   MELIN_NETNS_NODE_MACS     each interface's MAC, which its DPDK port takes
#   MELIN_NETNS_PREFIX_LEN    prefix length of the shared subnet
#   MELIN_NETNS_CLIENT_IP     the kernel side's address (on the bridge),
#                             where clients connect from
#   MELIN_NETNS_CLIENT_MAC    the bridge's MAC

set -euo pipefail

# Marks the copy of this script running inside the namespaces.
INNER_ENV=MELIN_NETNS_RUNNER_INNER

if [[ $# -eq 0 ]]; then
    echo "usage: $0 BINARY [ARGS...]" >&2
    exit 2
fi

if [[ -z "${!INNER_ENV:-}" ]]; then
    for tool in unshare python3; do
        if ! command -v "$tool" &>/dev/null; then
            echo "netns-runner: error: $tool not found" >&2
            exit 1
        fi
    done
    export "$INNER_ENV=1"
    # Not exec'd, so a failure can be explained. No `unshare -rnm true`
    # probe up front, which would add a namespace to every test: only a
    # failed run checks whether the namespaces were what failed.
    status=0
    unshare -rnm -- "$(realpath "${BASH_SOURCE[0]}")" "$@" || status=$?
    if [[ $status -ne 0 ]] && ! unshare -rnm true 2>/dev/null; then
        echo "netns-runner: error: cannot create an unprivileged user + network + mount namespace" >&2
        echo "  (\`unshare -rnm true\` failed: $(unshare -rnm true 2>&1 || true))" >&2
        echo "  On Ubuntu 24.04 and later, AppArmor restricts them by default. Lift it with:" >&2
        echo "    sudo sysctl -w kernel.apparmor_restrict_unprivileged_userns=0" >&2
        echo "  Elsewhere, check kernel.unprivileged_userns_clone and user.max_user_namespaces." >&2
    fi
    exit "$status"
fi

# ---------------------------------------------------------------------------
# Inside the namespaces from here on.
# ---------------------------------------------------------------------------

unset "$INNER_ENV"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

BRIDGE=br0
CLIENT_IP=10.99.0.1
PREFIX_LEN=24
# Locally administered, and deliberately not the `02:00:<ip>` convention a
# DPDK replica falls back to without `--dpdk-peer-mac`: a replica reaches
# its primary here only if it was given the primary's real MAC, as on any
# NIC that keeps its hardware address.
CLIENT_MAC=02:99:00:00:00:01
DPDK_IFACES=()
NODE_IPS=()
NODE_MACS=()
for slot in 0 1 2; do
    DPDK_IFACES+=("dpdk$slot")
    NODE_IPS+=("10.99.0.$((slot + 2))")
    NODE_MACS+=("02:99:00:00:00:0$((slot + 2))")
done

slots=()
for i in "${!DPDK_IFACES[@]}"; do
    slots+=("${DPDK_IFACES[$i]}=${NODE_MACS[$i]}")
done
if ! python3 "$SCRIPT_DIR/veth-setup.py" --bridge "$BRIDGE" "$CLIENT_IP/$PREFIX_LEN" "$CLIENT_MAC" "${slots[@]}"; then
    echo "netns-runner: error: network setup failed" >&2
    exit 1
fi

# The tmpfs below hides whatever lives under /run, so a temporary
# directory there (TMPDIR=/run/user/$UID, say) would vanish: the test's
# temporary files go to /tmp instead.
case "$(realpath -m "${TMPDIR:-/tmp}")" in
    /run | /run/* | /var/run | /var/run/*) export TMPDIR=/tmp ;;
esac

# EAL insists on creating /var/run/dpdk, and inside the user namespace it
# believes it is root, so it does not fall back to a per-user directory.
# A private tmpfs, gone with the mount namespace.
if ! mount -t tmpfs tmpfs /var/run; then
    echo "netns-runner: error: mounting a tmpfs on /var/run failed" >&2
    exit 1
fi

export MELIN_NETNS_DPDK_IFACES="${DPDK_IFACES[*]}"
export MELIN_NETNS_NODE_IPS="${NODE_IPS[*]}"
export MELIN_NETNS_NODE_MACS="${NODE_MACS[*]}"
export MELIN_NETNS_PREFIX_LEN="$PREFIX_LEN"
export MELIN_NETNS_CLIENT_IP="$CLIENT_IP"
export MELIN_NETNS_CLIENT_MAC="$CLIENT_MAC"

exec "$@"
