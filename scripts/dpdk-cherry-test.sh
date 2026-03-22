#!/usr/bin/env bash
# Quick DPDK test on Cherry servers using TAP virtual device.
#
# Runs the DPDK server on the server machine with a TAP device and
# the regular kernel TCP bench client on the bench machine.
#
# Usage:
#   ./scripts/dpdk-cherry-test.sh <server-public-ip> <bench-public-ip> <server-vlan-ip>
#
# Example:
#   ./scripts/dpdk-cherry-test.sh 84.32.176.135 84.32.176.145 10.189.210.12

set -euo pipefail

if [[ $# -lt 3 ]]; then
    echo "usage: $0 <server-public-ip> <bench-public-ip> <server-vlan-ip>"
    exit 1
fi

SERVER_PUB="$1"
BENCH_PUB="$2"
SERVER_VLAN="$3"
SSH_USER="${4:-root}"

SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
SERVER="${SSH_USER}@${SERVER_PUB}"
BENCH="${SSH_USER}@${BENCH_PUB}"
REPO="~/workspace/trading"
BRANCH="feat/dpdk-transport"

# DPDK IP: server VLAN IP + 100 in last octet
IFS='.' read -r a b c d <<< "$SERVER_VLAN"
DPDK_IP="${a}.${b}.${c}.$((d + 100))"
TAP_IP="${a}.${b}.${c}.$((d + 101))"

echo "============================================================"
echo "  DPDK Cherry Test (TAP mode)"
echo "  Server:  ${SERVER} (VLAN: ${SERVER_VLAN})"
echo "  Bench:   ${BENCH}"
echo "  DPDK IP: ${DPDK_IP} (smoltcp)"
echo "  TAP IP:  ${TAP_IP} (kernel routing)"
echo "============================================================"
echo ""

# --- Build on both machines ---
echo "=== Building ==="
echo "  Server (DPDK)..."
ssh $SSH_OPTS "$SERVER" "cd ${REPO} && git fetch origin && git checkout ${BRANCH} && git pull origin ${BRANCH} && \
    source ~/.cargo/env && cargo build --release -p melin-server --features dpdk --no-default-features && \
    cargo build --release -p melin-admin --bin melin-keygen" 2>&1 | tail -3
echo "  OK"

echo "  Bench..."
ssh $SSH_OPTS "$BENCH" "cd ${REPO} && git fetch origin && git checkout ${BRANCH} && git pull origin ${BRANCH} && \
    source ~/.cargo/env && cargo build --release -p melin-bench" 2>&1 | tail -3
echo "  OK"
echo ""

# --- Auth keys ---
echo "=== Auth keys ==="
ssh $SSH_OPTS "$SERVER" "cd ${REPO} && \
    if [[ ! -f bench.key ]]; then \
        ./target/release/melin-keygen bench admin; \
    fi && \
    echo \"admin \$(cat bench.pub) bench\" > authorized_keys"
ssh $SSH_OPTS "$SERVER" "cat ${REPO}/bench.key" | ssh $SSH_OPTS "$BENCH" "cat > ${REPO}/bench.key"
echo "  Done"
echo ""

# --- Setup server ---
echo "=== Setting up server ==="
ssh $SSH_OPTS "$SERVER" "\
    echo 256 > /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages && \
    mkdir -p /mnt/huge_2m && \
    mount -t hugetlbfs -o pagesize=2M nodev /mnt/huge_2m 2>/dev/null; \
    rm -f /mnt/journal/bench.journal* && \
    rm -rf /var/run/dpdk/rte && \
    pkill -x melin-server 2>/dev/null; \
    sleep 1 && \
    echo '  Hugepages: '\"  \$(cat /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages)\"' x 2MB'"
echo ""

# --- Start server ---
echo "=== Starting DPDK server ==="
ssh $SSH_OPTS "$SERVER" "cd ${REPO} && \
    RUST_LOG=info nohup ./target/release/melin-server \
        --bind 0.0.0.0:9876 \
        --journal /mnt/journal/bench.journal \
        --authorized-keys authorized_keys \
        --standalone \
        --dpdk-eal-args='--vdev=net_tap0 --no-pci --huge-dir=/mnt/huge_2m' \
        --dpdk-ip ${DPDK_IP} \
        --dpdk-prefix-len 24 \
    >/tmp/melin-server.log 2>&1 </dev/null &"

echo "  Waiting for TAP..."
sleep 3

# Configure kernel routing to the TAP. Use /32 to avoid adding a subnet
# route that conflicts with the bond VLAN's 10.x.x.0/24 route.
ssh $SSH_OPTS "$SERVER" "\
    if ip link show dtap0 >/dev/null 2>&1; then \
        ip addr add ${TAP_IP}/32 dev dtap0 2>/dev/null || true; \
        ip link set dtap0 up; \
        echo '  dtap0 up with ${TAP_IP}/32'; \
    else \
        echo '  ERROR: dtap0 not found'; \
        tail -20 /tmp/melin-server.log; \
        exit 1; \
    fi"

# Verify server is running
if ! ssh $SSH_OPTS "$SERVER" "pgrep -x melin-server" >/dev/null 2>&1; then
    echo "  ERROR: Server died"
    ssh $SSH_OPTS "$SERVER" "cat /tmp/melin-server.log"
    exit 1
fi
echo "  Server running (PID: $(ssh $SSH_OPTS "$SERVER" "pgrep -x melin-server"))"
echo ""

# --- Run benchmark ---
echo "=== Running benchmark ==="
echo "  1000 order pairs, 1 client, window 1"
echo ""

ssh $SSH_OPTS "$BENCH" "cd ${REPO} && source ~/.cargo/env && \
    ./target/release/melin-bench \
        --addr ${DPDK_IP}:9876 \
        --key bench.key \
        --clients 1 \
        --window 1 \
        --warmup 100 \
        1000"

RESULT=$?
echo ""

# --- Cleanup ---
echo "=== Stopping server ==="
ssh $SSH_OPTS "$SERVER" "pkill -x melin-server 2>/dev/null; true"

if [[ $RESULT -eq 0 ]]; then
    echo ""
    echo "============================================================"
    echo "  DPDK CHERRY TEST: PASSED"
    echo "============================================================"
else
    echo ""
    echo "  --- Server log ---"
    ssh $SSH_OPTS "$SERVER" "tail -30 /tmp/melin-server.log"
    exit 1
fi
