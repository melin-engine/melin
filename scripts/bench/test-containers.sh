#!/usr/bin/env bash
# Run the LAN suite against four podman containers on this machine: a
# rehearsal of the whole suite (sysroot, cross build, deploy, cluster,
# health verdict, summary) without bench hosts. The figures it prints
# measure nothing; whether it gets to print them is the test.
#
# The containers run Debian, as the bench hosts do, on this machine's
# architecture (so the build is for that architecture, not x86_64), on
# one podman network that stands in for the private VLAN. They are
# reached over SSH on ports published on localhost, through an ssh_config
# written for the run (the suite's SSH_CONFIG). Kernel TCP by default;
# DPDK=1 runs the suite's DPDK passes on af_packet instead (below).
#
# Safe for a laptop: the containers are unprivileged (the suite's IRQ
# pinning fails harmlessly inside them), though without a seccomp filter
# (podman's refuses io_uring), each is capped at CPUS cores, and
# the nodes run unpinned and yielding (`--cores none`) rather than
# spinning on the default layout's cores.
#
# Usage:
#   ./scripts/bench/test-containers.sh [echo|notary|ec]    # default echo
#
# Environment:
#   WORKLOADS   passed to the suite (default throughput,single)
#   CPUS        cores each container may use (default 2)
#   PLATFORM    a podman platform for the containers (e.g. linux/amd64,
#               which needs qemu-user-static, to rehearse the x86_64
#               build under emulation; slow, and not every kernel runs it)
#   KEEP=1      leave the containers running afterwards
#   DPDK=1      rehearse the suite's DPDK passes, on af_packet over the
#               containers' veths: TRANSPORTS=dpdk (or as set, e.g.
#               kernel,dpdk) and DPDK_REPLICATION=shared,port (or as set;
#               queue mode is refused, as af_packet has one queue and no
#               rte_flow). The node containers get a second network,
#               melin-bench-repl, for port mode's replication NICs
# Any other suite setting (THROUGHPUT_CLIENTS, ...) passes through:
# TRACE=1 rehearses a traced suite (the traced build, the primary's
# /stats-dump after each run and its verdict): on kernel TCP with no
# poll loop to summarise, and with DPDK=1 with the poll loops.
#
# DPDK=1, in detail. The host image gains libdpdk-dev (a layer of its
# own, localhost/melin-bench-host-dpdk, so the kernel rehearsal's image is
# untouched): the DPDK binaries' libraries, the af_packet driver, and
# what the suite's sysroot check looks for. The rig writes each
# container's /etc/melin-dpdk.conf itself (lib.sh's af_packet_conf), so
# the suite never runs dpdk-setup.sh: DPDK on af_packet on the
# container's interface, sharing its MAC (so the veth delivers DPDK's
# frames without promiscuous mode), at an address of its own beside the
# kernel's, which the kernel keeps. Both networks are created with
# subnets of the rig's choosing, two /24s of one /16 (lib.sh's
# rehearsal_subnets), podman allocating from each one's lower half and
# DPDK taking addresses from the top, so that the two never collide. Each
# container also gets:
#   - CAP_NET_RAW, which af_packet's sockets need (podman's default set
#     lacks it);
#   - CAP_NET_ADMIN, which af_packet's promiscuous switch needs (an
#     SIOCSIFFLAGS on the interface). Promiscuous mode itself is not
#     needed on the interface's own MAC, and failing to enable it is a
#     warning in the node's log, but starting the port then replays the
#     setting as a disable, and DPDK 22.11 takes that failure as fatal:
#     without it no port starts. It reaches the container's own network
#     namespace only. Nothing else: no hugepages (`--no-huge`), no device
#     to bind;
#   - IPv4 forwarding off: the kernel sees every frame to DPDK's address
#     (the MAC is shared), and a forwarding kernel would send each back
#     out, duplicated, with redirects. Not forwarding, it drops them.
# The checksum offload the netns test runner switches off is left on:
# there it is for frames the kernel's own TCP sends to a DPDK port, whose
# checksums are left for the NIC to finish; here every flow on a DPDK
# address is DPDK to DPDK, its checksums computed in full by DPDK's TCP
# stack, and the kernel's own flows (SSH, the health endpoint) stay on
# the kernel's addresses.
#
# Unverified until a first run: that DPDK's frames reach it at all. They
# travel at layer 2 only, between the containers' veths on the bridge
# that rootless podman's netavark sets up in its own network namespace,
# for an address podman never assigned; that works if the bridge does
# plain learning and flooding (ARP included) with no br_netfilter or
# anti-spoofing filter on it. If it does not, the rig's own checks all
# pass and the first DPDK run fails: connects to the primary's DPDK
# addresses (the replicas', the bench client's) time out, and the nodes'
# logs show no connection arriving.
#
# Not exercised with DPDK=1: queue mode (mlx5's rte_flow and second
# queue), mlx5 itself, hugepages, dpdk-setup.sh, and any figure.

set -euo pipefail

APP="${1:-echo}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"
DPDK_ON=$(switch_setting DPDK "${DPDK:-}") || exit 1
CPUS="${CPUS:-2}"
NETWORK=melin-bench-net
# DPDK=1: the second network, for port mode's replication NICs, on the
# node containers only (the load generator runs no replication).
REPL_NETWORK=melin-bench-repl
IMAGE=localhost/melin-bench-host
DPDK_IMAGE=localhost/melin-bench-host-dpdk
NAMES=(server client replica replica2)
NODE_NAMES=(server replica replica2)

# `is_node <name>`: whether the container named `name` runs a node.
is_node() {
    [[ " ${NODE_NAMES[*]} " == *" $1 "* ]]
}

# `on_network <container> <network> <field>`: the container's address
# (`IPAddress`) or MAC (`MacAddress`) on a network, as podman gave it.
on_network() {
    podman inspect -f "{{(index .NetworkSettings.Networks \"$2\").$3}}" "$1"
}

PLATFORM_ARGS=()
[[ -n "${PLATFORM:-}" ]] && PLATFORM_ARGS=(--platform "$PLATFORM")

# The transports the suite runs: kernel TCP alone, or with DPDK=1 the
# user's choice, DPDK by default. Queue mode is refused before anything
# is built, as the suite would refuse it on the containers' af_packet.
SUITE_TRANSPORTS=kernel
RUN_IMAGE="$IMAGE"
if [[ "$DPDK_ON" == 1 ]]; then
    SUITE_TRANSPORTS="${TRANSPORTS:-dpdk}"
    RUN_IMAGE="$DPDK_IMAGE"
    export DPDK_REPLICATION="${DPDK_REPLICATION:-shared,port}"
    modes=$(dpdk_replication_modes "$DPDK_REPLICATION") || exit 1
    if [[ " ${modes} " == *" queue "* ]]; then
        echo "error: DPDK_REPLICATION=queue: DPDK=1 runs the suite on af_packet, ${AF_PACKET_NO_QUEUE_MODE} (rehearse shared and port: DPDK_REPLICATION=shared,port)" >&2
        exit 1
    fi
fi

if ! command -v podman >/dev/null; then
    echo "error: podman not found" >&2
    exit 1
fi

WORK_DIR="$(mktemp -d -t melin-bench-containers.XXXXXX)"
cleanup() {
    if [[ "${KEEP:-0}" == 1 ]]; then
        echo "containers left running (KEEP=1); ssh config: ${WORK_DIR}/ssh_config"
        return
    fi
    local name
    for name in "${NAMES[@]}"; do
        podman rm -f "melin-bench-${name}" >/dev/null 2>&1 || true
    done
    podman network rm "$NETWORK" >/dev/null 2>&1 || true
    # Whatever the run: a KEEP=1 DPDK=1 run leaves it behind, and its
    # containers are gone now. Absent, the failure is the expected one.
    podman network rm "$REPL_NETWORK" >/dev/null 2>&1 || true
    rm -rf "$WORK_DIR"
}
trap cleanup EXIT

# A key of the rig's own, so nothing of the operator's is installed in
# the containers.
ssh-keygen -q -t ed25519 -N '' -f "${WORK_DIR}/id_ed25519"

echo "=== Building the host image ==="
# What server-setup.sh installs that the suite uses, without the host
# tuning, which a container cannot do: build-essential for the sysroot,
# curl for the health samples, procps for pkill.
podman build "${PLATFORM_ARGS[@]}" -q -t "$IMAGE" -f - "$WORK_DIR" <<'EOF' >/dev/null
FROM docker.io/library/debian:bookworm
RUN apt-get update -qq \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends \
      openssh-server build-essential curl procps ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && mkdir -p /run/sshd /root/.ssh \
 && chmod 700 /root/.ssh
CMD ["/usr/sbin/sshd", "-D", "-e"]
EOF
if [[ "$DPDK_ON" == 1 ]]; then
    # On top of the kernel image rather than in it: the kernel rehearsal's
    # image stays as it was. libdpdk-dev, as server-setup.sh installs it:
    # the sysroot's headers and pkg-config file, and the runtime libraries
    # with every driver, af_packet's among them.
    echo "  with libdpdk-dev (${DPDK_IMAGE})"
    podman build "${PLATFORM_ARGS[@]}" -q -t "$DPDK_IMAGE" --build-arg "BASE=${IMAGE}" -f - "$WORK_DIR" <<'EOF' >/dev/null
ARG BASE
FROM ${BASE}
RUN apt-get update -qq \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends libdpdk-dev \
 && rm -rf /var/lib/apt/lists/*
EOF
fi

echo "=== Starting the hosts ==="
RUN_ARGS=()
if [[ "$DPDK_ON" == 1 ]]; then
    # The networks are the rig's, on subnets it chooses: the containers
    # go first, as a network in use cannot be removed, then any network
    # of these names a kernel rehearsal or a KEEP=1 run left behind.
    for name in "${NAMES[@]}"; do
        podman rm -f "melin-bench-${name}" >/dev/null 2>&1 || true
    done
    for network in "$NETWORK" "$REPL_NETWORK"; do
        if podman network exists "$network"; then podman network rm "$network" >/dev/null; fi
    done
    mapfile -t networks < <(podman network ls -q)
    existing=""
    if (( ${#networks[@]} > 0 )); then
        existing=$(podman_subnets "$(podman network inspect "${networks[@]}")") || exit 1
    fi
    # Word-split on purpose: the subnets are space-separated.
    # shellcheck disable=SC2086
    subnets=$(rehearsal_subnets $existing) || exit 1
    read -r CLIENTS_SUBNET REPL_SUBNET <<< "$subnets"
    podman network create --subnet "$CLIENTS_SUBNET" --ip-range "$(lower_half "$CLIENTS_SUBNET")" "$NETWORK" >/dev/null
    podman network create --subnet "$REPL_SUBNET" --ip-range "$(lower_half "$REPL_SUBNET")" "$REPL_NETWORK" >/dev/null
    echo "  networks: ${NETWORK} on ${CLIENTS_SUBNET}, ${REPL_NETWORK} on ${REPL_SUBNET}"
    RUN_ARGS=(--cap-add NET_RAW --cap-add NET_ADMIN --sysctl net.ipv4.ip_forward=0)
else
    podman network exists "$NETWORK" || podman network create "$NETWORK" >/dev/null
fi
SSH_CONFIG="${WORK_DIR}/ssh_config"
: > "$SSH_CONFIG"
for name in "${NAMES[@]}"; do
    container="melin-bench-${name}"
    podman rm -f "$container" >/dev/null 2>&1 || true
    NETWORK_ARGS=(--network "$NETWORK")
    if [[ "$DPDK_ON" == 1 ]] && is_node "$name"; then
        NETWORK_ARGS+=(--network "$REPL_NETWORK")
    fi
    # Seccomp unconfined: podman's default profile refuses io_uring, which
    # the kernel transport is built on (the node exits with ENOSYS). The
    # container stays unprivileged.
    podman run "${PLATFORM_ARGS[@]}" -d --name "$container" --hostname "$container" \
        "${NETWORK_ARGS[@]}" -p 127.0.0.1::22 --cpus "$CPUS" \
        --security-opt seccomp=unconfined "${RUN_ARGS[@]}" "$RUN_IMAGE" >/dev/null
    podman exec -i "$container" sh -c 'cat > /root/.ssh/authorized_keys && chmod 600 /root/.ssh/authorized_keys' \
        < "${WORK_DIR}/id_ed25519.pub"
    port=$(podman port "$container" 22 | head -1)
    port="${port##*:}"
    cat >> "$SSH_CONFIG" <<EOF
Host ${container}
    HostName 127.0.0.1
    Port ${port}
    User root
    IdentityFile ${WORK_DIR}/id_ed25519
    IdentitiesOnly yes
EOF
    echo "  ${container}: ssh on 127.0.0.1:${port}"
done
SERVER_IP=$(on_network melin-bench-server "$NETWORK" IPAddress)
echo "  server on the network at ${SERVER_IP}"

for name in "${NAMES[@]}"; do
    for _ in $(seq 30); do
        ssh -F "$SSH_CONFIG" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
            -o LogLevel=ERROR -o BatchMode=yes "melin-bench-${name}" true 2>/dev/null && continue 2
        sleep 1
    done
    echo "error: melin-bench-${name} does not answer on SSH" >&2
    exit 1
done

# `iface_with_mac <container> <MAC>`: the container's interface with the
# MAC, or nothing.
iface_with_mac() {
    podman exec "$1" sh -c 'for dev in /sys/class/net/*; do [ "$(cat "$dev/address")" = "$1" ] && basename "$dev"; done; true' sh "$2"
}

# DPDK=1: each container's /etc/melin-dpdk.conf, after the checks a
# first run could otherwise only fail on obscurely, inside a node.
if [[ "$DPDK_ON" == 1 ]]; then
    echo ""
    echo "=== Configuring DPDK on af_packet ==="
    # DPDK's addresses, from the top of each network, past every one
    # podman gave (from the bottom half, so none should be up there).
    taken=()
    for name in "${NAMES[@]}"; do taken+=("$(on_network "melin-bench-${name}" "$NETWORK" IPAddress)"); done
    addresses=$(high_free_addresses "$CLIENTS_SUBNET" "${#NAMES[@]}" "${taken[@]}") || exit 1
    read -ra DPDK_IPS <<< "$addresses"
    taken=()
    for name in "${NODE_NAMES[@]}"; do taken+=("$(on_network "melin-bench-${name}" "$REPL_NETWORK" IPAddress)"); done
    addresses=$(high_free_addresses "$REPL_SUBNET" "${#NODE_NAMES[@]}" "${taken[@]}") || exit 1
    read -ra REPL_IPS <<< "$addresses"
    node=0
    for i in "${!NAMES[@]}"; do
        name="${NAMES[$i]}"
        container="melin-bench-${name}"
        # The capabilities the containers' sshd, and so the suite's
        # sessions, hold: PID 1's.
        caps=$(podman exec "$container" awk '$1 == "CapEff:" { print $2 }' /proc/1/status)
        if ! has_capability "$caps" "$CAP_NET_RAW"; then
            echo "error: ${container} lacks CAP_NET_RAW (CapEff ${caps:-unknown}), which af_packet needs; podman dropped --cap-add NET_RAW" >&2
            exit 1
        fi
        if ! has_capability "$caps" "$CAP_NET_ADMIN"; then
            echo "error: ${container} lacks CAP_NET_ADMIN (CapEff ${caps:-unknown}), without which af_packet's ports fail to start; podman dropped --cap-add NET_ADMIN" >&2
            exit 1
        fi
        if ! podman exec "$container" sh -c 'ls /usr/lib/*/dpdk/pmds-*/librte_net_af_packet.so* >/dev/null 2>&1'; then
            echo "error: ${container} has no af_packet driver for DPDK (librte_net_af_packet); is libdpdk-dev in ${DPDK_IMAGE}?" >&2
            exit 1
        fi
        cpus=$(podman exec "$container" awk '$1 == "Cpus_allowed_list:" { print $2 }' /proc/1/status)
        mac=$(on_network "$container" "$NETWORK" MacAddress)
        iface=$(iface_with_mac "$container" "$mac")
        if [[ -z "$iface" ]]; then
            echo "error: ${container} has no interface with its ${NETWORK} MAC (${mac:-none})" >&2
            exit 1
        fi
        repl=()
        if is_node "$name"; then
            repl_mac=$(on_network "$container" "$REPL_NETWORK" MacAddress)
            repl_iface=$(iface_with_mac "$container" "$repl_mac")
            if [[ -z "$repl_iface" ]]; then
                echo "error: ${container} has no interface with its ${REPL_NETWORK} MAC (${repl_mac:-none})" >&2
                exit 1
            fi
            repl=("$repl_iface" "${REPL_IPS[$node]}" "$repl_mac")
            node=$((node + 1))
        fi
        conf=$(af_packet_conf "$cpus" "$iface" "${DPDK_IPS[$i]}" "$mac" "${repl[@]}") || exit 1
        podman exec -i "$container" sh -c 'cat > /etc/melin-dpdk.conf' <<< "$conf"
        line="  ${container}: DPDK ${DPDK_IPS[$i]} on ${iface}"
        (( ${#repl[@]} > 0 )) && line+=", replication ${repl[1]} on ${repl[0]}"
        echo "$line"
    done
fi

echo ""
# Short phases: the rehearsal is of the suite, not of the figures. The
# nodes unpinned and yielding, without locking their memory (a container's
# memlock limit is small), on small journal segments.
NODE_ARGS="--cores none --no-mlock --max-journal-mib 64"
SSH_CONFIG="$SSH_CONFIG" \
TRANSPORTS="$SUITE_TRANSPORTS" \
WORKLOADS="${WORKLOADS:-throughput,single}" \
THROUGHPUT_DURATION="${THROUGHPUT_DURATION:-5s}" \
SINGLE_DURATION="${SINGLE_DURATION:-5s}" \
WARMUP_DURATION="${WARMUP_DURATION:-1s}" \
COOLDOWN_DURATION="${COOLDOWN_DURATION:-1s}" \
BENCH_CORE="${BENCH_CORE:-0}" \
SERVER_EXTRA_ARGS="${SERVER_EXTRA_ARGS:-$NODE_ARGS}" \
REPLICA_EXTRA_ARGS="${REPLICA_EXTRA_ARGS:-$NODE_ARGS}" \
    "${SCRIPT_DIR}/lan-bench-suite.sh" "$APP" \
    melin-bench-server melin-bench-client "$SERVER_IP" melin-bench-replica melin-bench-replica2
