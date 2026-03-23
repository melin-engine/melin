#!/usr/bin/env bash
# Run a LAN benchmark with DPDK kernel-bypass on the server.
#
# Same as lan-bench.sh but the server uses DPDK instead of kernel TCP.
# The bench client uses regular kernel TCP — no DPDK needed on the bench machine.
#
# Usage:
#   ./scripts/dpdk-lan-bench.sh <server-public-ip> <bench-public-ip> <server-vlan-ip> [user] [-- bench-args...]
#
# Examples:
#   # Defaults (100M pairs, 16 clients, window 256):
#   ./scripts/dpdk-lan-bench.sh 84.32.176.135 84.32.176.145 10.181.29.15
#
#   # Custom bench args:
#   ./scripts/dpdk-lan-bench.sh 84.32.176.135 84.32.176.145 10.181.29.15 root \
#       -- 50000000 --clients 32 --window 128
#
# Prerequisites:
#   - SSH access to both machines (as root)
#   - Server: DPDK installed (dpdk-devel/libdpdk-dev), hugepages configured,
#     NIC bound to vfio-pci (run dpdk-setup-sriov.sh first for SR-IOV)
#   - A VLAN/private network between the two machines
#   - The bench machine can reach <server-vlan-ip> over the private network
#
# Environment variables:
#   DPDK_EAL_ARGS       Server EAL arguments (default: "" = default PCI scan)
#   DPDK_PORT           Server DPDK port ID (default: 0)
#   DPDK_CORE           Server DPDK poll thread core (default: 7)
#   HUGE_DIR            Hugepage mount point (default: /mnt/huge_2m)
#   BENCH_DPDK_EAL_ARGS Bench EAL arguments (default: "" = default PCI scan)
#   BENCH_DPDK_PORT     Bench DPDK port ID (default: 0)
#   BENCH_DPDK_CORE     Bench DPDK poll thread core (default: 7)
#   BENCH_BRANCH        Git branch to checkout on both machines
#   USE_KERNEL_TCP_BENCH Set to 1 to use kernel TCP on bench side (no DPDK)

set -euo pipefail

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
POSITIONAL=()
BENCH_EXTRA_ARGS=""

while [[ $# -gt 0 ]]; do
    if [[ "$1" == "--" ]]; then
        shift
        BENCH_EXTRA_ARGS="$*"
        break
    fi
    POSITIONAL+=("$1")
    shift
done

if [[ ${#POSITIONAL[@]} -lt 3 ]]; then
    echo "usage: $0 <server-public-ip> <bench-public-ip> <server-vlan-ip> [user] [-- bench-args...]"
    exit 1
fi

SERVER_PUB="${POSITIONAL[0]}"
BENCH_PUB="${POSITIONAL[1]}"
SERVER_VLAN="${POSITIONAL[2]}"
SSH_USER="${POSITIONAL[3]:-root}"

SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
SERVER="${SSH_USER}@${SERVER_PUB}"
BENCH="${SSH_USER}@${BENCH_PUB}"

REPO_DIR="~/workspace/trading"
JOURNAL_PATH="${JOURNAL_PATH:-/mnt/journal/bench.journal}"
SNAPSHOT_PATH="${SNAPSHOT_PATH:-/mnt/journal/bench.snapshot}"
PORT=9876

# Read DPDK config from the server (written by dpdk-setup-sriov.sh).
# Can be overridden via env vars.
DPDK_CONF=$(ssh $SSH_OPTS "$SERVER" "cat /etc/melin-dpdk.conf 2>/dev/null" || true)
if [[ -n "$DPDK_CONF" ]]; then
    eval "$DPDK_CONF"
    echo "  Loaded DPDK config from server: IP=${DPDK_IP}, port=${DPDK_PORT}"
else
    echo "  No /etc/melin-dpdk.conf on server — using defaults/env vars"
fi

# Allow env var overrides after config file.
DPDK_IP="${DPDK_IP:-${SERVER_VLAN}}"
DPDK_PORT="${DPDK_PORT:-0}"
DPDK_CORE="${DPDK_CORE:-7}"
DPDK_PREFIX="${DPDK_PREFIX:-24}"
DPDK_EAL_ARGS="${DPDK_EAL_ARGS:-}"
HUGE_DIR="${HUGE_DIR:-/mnt/huge_2m}"

# Bench-side DPDK config. Read from bench machine if available.
if [[ "${USE_KERNEL_TCP_BENCH:-0}" != "1" ]]; then
    BENCH_DPDK_CONF=$(ssh $SSH_OPTS "$BENCH" "cat /etc/melin-dpdk.conf 2>/dev/null" || true)
    if [[ -n "$BENCH_DPDK_CONF" ]]; then
        # Parse into BENCH_DPDK_* vars (prefix to avoid overwriting server vars).
        BENCH_DPDK_IP=$(echo "$BENCH_DPDK_CONF" | grep "^DPDK_IP=" | cut -d= -f2)
        BENCH_DPDK_PREFIX_VAL=$(echo "$BENCH_DPDK_CONF" | grep "^DPDK_PREFIX=" | cut -d= -f2)
        echo "  Loaded bench DPDK config: IP=${BENCH_DPDK_IP}"
    fi
fi
BENCH_DPDK_IP="${BENCH_DPDK_IP:-}"
BENCH_DPDK_PREFIX_VAL="${BENCH_DPDK_PREFIX_VAL:-${DPDK_PREFIX}}"
BENCH_DPDK_PORT="${BENCH_DPDK_PORT:-0}"
BENCH_DPDK_CORE="${BENCH_DPDK_CORE:-7}"
BENCH_DPDK_EAL_ARGS="${BENCH_DPDK_EAL_ARGS:-}"

# Default bench args if none provided.
if [[ -z "$BENCH_EXTRA_ARGS" ]]; then
    BENCH_EXTRA_ARGS="100000000 --clients 16 --window 256"
fi

echo "============================================================"
echo "  DPDK LAN Benchmark"
echo "  Server: ${SERVER} (VLAN: ${SERVER_VLAN})"
echo "  Bench:  ${BENCH}"
echo "  DPDK IP: ${DPDK_IP}/${DPDK_PREFIX}"
echo "  DPDK EAL: ${DPDK_EAL_ARGS:-<default PCI scan>}"
echo "  DPDK port: ${DPDK_PORT}, core: ${DPDK_CORE}"
echo "  Bench args: ${BENCH_EXTRA_ARGS}"
echo "============================================================"
echo ""

# ---------------------------------------------------------------------------
# 1. Build on both machines
# ---------------------------------------------------------------------------
echo "=== Building ==="

GIT_CMD="git pull --ff-only"
if [[ -n "${BENCH_BRANCH:-}" ]]; then
    GIT_CMD="git fetch origin && git checkout ${BENCH_BRANCH} && git pull origin ${BENCH_BRANCH}"
fi

echo "  Building DPDK server on ${SERVER}..."
ssh $SSH_OPTS "$SERVER" "cd ${REPO_DIR} && ${GIT_CMD} && source ~/.cargo/env && \
    cargo build --release -p melin-server --features dpdk --no-default-features && \
    cargo build --release -p melin-admin --bin melin-keygen" 2>&1 | tail -3
echo "  server build: OK"

echo "  Building bench on ${BENCH}..."
if [[ "${USE_KERNEL_TCP_BENCH:-0}" == "1" ]]; then
    ssh $SSH_OPTS "$BENCH" "cd ${REPO_DIR} && ${GIT_CMD} && source ~/.cargo/env && \
        cargo build --release -p melin-bench -p melin-admin" 2>&1 | tail -3
    echo "  bench build: OK (kernel TCP)"
else
    ssh $SSH_OPTS "$BENCH" "cd ${REPO_DIR} && ${GIT_CMD} && source ~/.cargo/env && \
        cargo build --release -p melin-bench --features dpdk --no-default-features && \
        cargo build --release -p melin-admin --bin melin-keygen" 2>&1 | tail -3
    echo "  bench build: OK (DPDK)"
fi
echo ""

# ---------------------------------------------------------------------------
# 2. Auth keys
# ---------------------------------------------------------------------------
echo "=== Setting up auth keys ==="
ssh $SSH_OPTS "$BENCH" "cd ${REPO_DIR} && \
    if [[ ! -f bench.key ]]; then \
        source ~/.cargo/env && \
        ./target/release/melin-keygen bench admin && \
        echo 'Generated bench.key'; \
    else \
        echo 'bench.key already exists'; \
    fi"

AUTH_LINE=$(ssh $SSH_OPTS "$BENCH" "cd ${REPO_DIR} && cat bench.pub | xargs -I{} echo 'admin {} bench'")
ssh $SSH_OPTS "$SERVER" "cd ${REPO_DIR} && echo '${AUTH_LINE}' > authorized_keys"
echo "  Auth keys configured."
echo ""

# ---------------------------------------------------------------------------
# 3. Setup hugepages on server
# ---------------------------------------------------------------------------
echo "=== Setting up hugepages ==="
for host_label in "server:$SERVER" "bench:$BENCH"; do
    label="${host_label%%:*}"
    host="${host_label#*:}"
    if [[ "$label" == "bench" && "${USE_KERNEL_TCP_BENCH:-0}" == "1" ]]; then
        continue
    fi
    echo "  ${label} (${host}):"
    ssh $SSH_OPTS "$host" "\
        HP=\$(cat /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages 2>/dev/null || echo 0); \
        if [[ \"\$HP\" -lt 256 ]]; then \
            echo 256 > /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages; \
        fi; \
        mkdir -p ${HUGE_DIR}; \
        if ! mount | grep -q '${HUGE_DIR}'; then \
            mount -t hugetlbfs -o pagesize=2M nodev ${HUGE_DIR} 2>/dev/null || true; \
            echo '    Mounted 2MB hugetlbfs at ${HUGE_DIR}'; \
        fi; \
        echo \"    Hugepages: \$(cat /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages) x 2MB\""
done
echo ""

# ---------------------------------------------------------------------------
# 4. Clean old journal
# ---------------------------------------------------------------------------
echo "=== Cleaning old journal ==="
ssh $SSH_OPTS "$SERVER" "rm -f ${JOURNAL_PATH} ${JOURNAL_PATH}.* ${SNAPSHOT_PATH} ${SNAPSHOT_PATH}.* 2>/dev/null; echo 'Cleaned.'"
echo ""

# ---------------------------------------------------------------------------
# 5. Start DPDK server
# ---------------------------------------------------------------------------
echo "=== Starting DPDK server ==="
ssh $SSH_OPTS "$SERVER" "pkill -x melin-server 2>/dev/null; true"
sleep 1

# Build the EAL args string.
EAL_FULL="${DPDK_EAL_ARGS}"
if [[ -n "$EAL_FULL" ]]; then
    EAL_FULL="${EAL_FULL} --huge-dir=${HUGE_DIR}"
else
    EAL_FULL="--huge-dir=${HUGE_DIR}"
fi

ssh $SSH_OPTS "$SERVER" "RUST_LOG=info nohup ${REPO_DIR}/target/release/melin-server \
        --bind 0.0.0.0:${PORT} \
        --journal ${JOURNAL_PATH} \
        --authorized-keys ${REPO_DIR}/authorized_keys \
        --standalone \
        --dpdk-eal-args='${EAL_FULL}' \
        --dpdk-ip ${DPDK_IP} \
        --dpdk-prefix-len ${DPDK_PREFIX} \
        --dpdk-ports ${DPDK_PORT} \
        --dpdk-core ${DPDK_CORE} \
    >/tmp/melin-server.log 2>&1 </dev/null &" </dev/null

# Wait for server. Can't use nc -z (smoltcp doesn't respond to kernel probes).
# Instead, try a short bench run as a health check.
echo "  Waiting for DPDK server to start..."
sleep 3

# Check if the server process is alive.
for i in $(seq 1 30); do
    if ssh $SSH_OPTS "$SERVER" "pgrep -x melin-server" >/dev/null 2>&1; then
        echo "  Server process is running (PID: $(ssh $SSH_OPTS "$SERVER" "pgrep -x melin-server"))"
        break
    fi
    if [[ $i -eq 30 ]]; then
        echo "  ERROR: Server process not found."
        echo "  --- Server log ---"
        ssh $SSH_OPTS "$SERVER" "tail -30 /tmp/melin-server.log" 2>/dev/null || true
        exit 1
    fi
    sleep 1
done

# Give smoltcp time to bring up the TCP listener and respond to ARP.
sleep 2
echo ""

# ---------------------------------------------------------------------------
# 6. Run benchmark
# ---------------------------------------------------------------------------
echo "=== Running DPDK benchmark ==="
echo ""

if [[ "${USE_KERNEL_TCP_BENCH:-0}" == "1" ]]; then
    ssh $SSH_OPTS "$BENCH" "cd ${REPO_DIR} && source ~/.cargo/env && \
        ./target/release/melin-bench \
            --addr ${DPDK_IP}:${PORT} \
            --key bench.key \
            --json /tmp/dpdk-bench-results.json \
            ${BENCH_EXTRA_ARGS}" 2>&1
else
    # Build bench DPDK EAL args.
    BENCH_EAL_FULL="${BENCH_DPDK_EAL_ARGS}"
    if [[ -n "$BENCH_EAL_FULL" ]]; then
        BENCH_EAL_FULL="${BENCH_EAL_FULL} --huge-dir=${HUGE_DIR}"
    else
        BENCH_EAL_FULL="--huge-dir=${HUGE_DIR}"
    fi

    BENCH_DPDK_ARGS="--dpdk-eal-args='${BENCH_EAL_FULL}' --dpdk-ports ${BENCH_DPDK_PORT} --dpdk-core ${BENCH_DPDK_CORE}"
    if [[ -n "$BENCH_DPDK_IP" ]]; then
        BENCH_DPDK_ARGS="${BENCH_DPDK_ARGS} --dpdk-ip ${BENCH_DPDK_IP} --dpdk-prefix-len ${BENCH_DPDK_PREFIX_VAL}"
    fi

    ssh $SSH_OPTS "$BENCH" "cd ${REPO_DIR} && source ~/.cargo/env && \
        ./target/release/melin-bench \
            --addr ${DPDK_IP}:${PORT} \
            --key bench.key \
            --json /tmp/dpdk-bench-results.json \
            ${BENCH_DPDK_ARGS} \
            ${BENCH_EXTRA_ARGS}" 2>&1
fi

BENCH_EXIT=$?
echo ""

# ---------------------------------------------------------------------------
# 7. Collect results
# ---------------------------------------------------------------------------
if [[ $BENCH_EXIT -eq 0 ]]; then
    echo "=== Results ==="
    scp $SSH_OPTS "$BENCH:/tmp/dpdk-bench-results.json" /tmp/dpdk-bench-results.json 2>/dev/null && \
        echo "  JSON results saved to /tmp/dpdk-bench-results.json" || true
fi

# ---------------------------------------------------------------------------
# 8. Stop server
# ---------------------------------------------------------------------------
echo ""
echo "=== Stopping server ==="
ssh $SSH_OPTS "$SERVER" "pkill -x melin-server 2>/dev/null; true"
echo "  Done."

if [[ $BENCH_EXIT -ne 0 ]]; then
    echo ""
    echo "  --- Server log (last 30 lines) ---"
    ssh $SSH_OPTS "$SERVER" "tail -30 /tmp/melin-server.log" 2>/dev/null || true
    exit 1
fi
