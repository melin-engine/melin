#!/usr/bin/env bash
# End-to-end test: DPDK server ↔ DPDK bench on a single machine.
#
# Uses two TAP virtual devices bridged together so both the server
# and bench client use DPDK+smoltcp — no kernel TCP involved.
#
# Network topology:
#   server (192.168.200.1) → dtap0 → br-dpdk → dtap1 ← bench (192.168.200.2)
#
# Each DPDK process uses a separate --file-prefix to avoid hugepage conflicts.
#
# Usage:
#   sudo ./scripts/dpdk-e2e-test.sh

set -euo pipefail

if [[ -n "${SUDO_USER:-}" ]]; then
    REAL_HOME=$(eval echo "~$SUDO_USER")
    export PATH="$REAL_HOME/.cargo/bin:$PATH"
    export RUSTUP_HOME="${RUSTUP_HOME:-$REAL_HOME/.rustup}"
    export CARGO_HOME="${CARGO_HOME:-$REAL_HOME/.cargo}"
fi

if [[ $EUID -ne 0 ]]; then
    echo "error: must run as root" >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
TMPDIR=$(mktemp -d)

SERVER_IP="192.168.200.1"
BENCH_IP="192.168.200.2"
PREFIX=24
PORT=9876
BRIDGE="br-dpdk"
SERVER_TAP="dtap0"
BENCH_TAP="dtap1"

cleanup() {
    echo ""
    echo "=== Cleanup ==="
    kill "$SERVER_PID" 2>/dev/null && wait "$SERVER_PID" 2>/dev/null || true
    kill "$BENCH_PID" 2>/dev/null && wait "$BENCH_PID" 2>/dev/null || true
    ip link set "$BRIDGE" down 2>/dev/null || true
    ip link del "$BRIDGE" 2>/dev/null || true
    rm -rf /var/run/dpdk/server /var/run/dpdk/bench 2>/dev/null || true
    if [[ "${MOUNTED_HUGE:-}" == "1" ]]; then
        umount /mnt/huge_2m 2>/dev/null || true
    fi
    rm -rf "$TMPDIR"
    echo "  Done."
}
trap cleanup EXIT

echo "============================================================"
echo "  DPDK End-to-End Test (bridged TAP devices)"
echo "  Server: ${SERVER_IP}:${PORT} (${SERVER_TAP})"
echo "  Bench:  ${BENCH_IP} (${BENCH_TAP})"
echo "============================================================"
echo ""

# --- Clean stale state ---
rm -rf /var/run/dpdk/server /var/run/dpdk/bench 2>/dev/null || true
ip link del "$BRIDGE" 2>/dev/null || true

# --- Hugepages ---
echo "=== Hugepages ==="
HP=$(cat /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages 2>/dev/null || echo 0)
if [[ "$HP" -lt 512 ]]; then
    echo 512 > /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages
fi
if ! mount | grep -q "pagesize=2M"; then
    mkdir -p /mnt/huge_2m
    mount -t hugetlbfs -o pagesize=2M nodev /mnt/huge_2m
    MOUNTED_HUGE=1
fi
echo "  $(cat /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages) x 2MB"
echo ""

# --- Build ---
echo "=== Building ==="
cd "$PROJECT_DIR"
cargo build --release -p melin-server --features dpdk --no-default-features --quiet 2>&1
cargo build --release -p melin-bench --features dpdk --no-default-features --quiet 2>&1
cargo build --release --bin melin-keygen --quiet 2>&1
echo "  OK"
echo ""

# --- Auth keys ---
echo "=== Auth keys ==="
cd "$TMPDIR"
"$PROJECT_DIR/target/release/melin-keygen" bench trader
echo "trader $(cat bench.pub | tr -d '\n') bench" > authorized_keys
echo "  Generated"
echo ""

# --- Create bridge ---
echo "=== Creating bridge ==="
ip link add "$BRIDGE" type bridge
ip link set "$BRIDGE" type bridge stp_state 0 forward_delay 0
ip link set "$BRIDGE" up

# Disable netfilter on bridge traffic. Fedora/RHEL enable
# bridge-nf-call-iptables by default, which applies firewall rules
# to bridged L2 frames. This silently drops ARP replies and TCP SYNs
# forwarded between the TAP ports.
sysctl -qw net.bridge.bridge-nf-call-iptables=0 2>/dev/null || true
sysctl -qw net.bridge.bridge-nf-call-ip6tables=0 2>/dev/null || true
sysctl -qw net.bridge.bridge-nf-call-arptables=0 2>/dev/null || true

echo "  $BRIDGE created (STP off, nf-call disabled)"
echo ""

# --- Start server ---
echo "=== Starting DPDK server ==="
RUST_LOG=info \
"$PROJECT_DIR/target/release/melin-server" \
    --bind "0.0.0.0:$PORT" \
    --journal "$TMPDIR/smoke.journal" \
    --authorized-keys "$TMPDIR/authorized_keys" \
    --standalone \
    --accounts 100 \
    --instruments 10 \
    --dpdk-eal-args="--vdev=net_tap0 --no-pci --log-level=6 --huge-dir=/mnt/huge_2m --file-prefix=server" \
    --dpdk-ip "$SERVER_IP" \
    --dpdk-prefix-len "$PREFIX" \
    > "$TMPDIR/server.log" 2>&1 &
SERVER_PID=$!

echo "  Waiting for $SERVER_TAP..."
for i in $(seq 1 20); do
    ip link show "$SERVER_TAP" &>/dev/null && break
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "  ERROR: Server died"; cat "$TMPDIR/server.log"; exit 1
    fi
    sleep 0.5
done
ip link show "$SERVER_TAP" &>/dev/null || { echo "  ERROR: $SERVER_TAP not created"; exit 1; }
ip link set "$SERVER_TAP" master "$BRIDGE"
ip link set "$SERVER_TAP" up
echo "  $SERVER_TAP attached to $BRIDGE (PID: $SERVER_PID)"
sleep 1

# --- Start bench ---
echo ""
echo "=== Starting DPDK bench ==="
RUST_LOG=info \
"$PROJECT_DIR/target/release/melin-bench" \
    --addr "$SERVER_IP:$PORT" \
    --key "$TMPDIR/bench.key" \
    --clients 1 \
    --window 1 \
    --warmup 50 \
    --dpdk-eal-args="--vdev=net_tap1 --no-pci --log-level=6 --huge-dir=/mnt/huge_2m --file-prefix=bench" \
    --dpdk-ip "$BENCH_IP" \
    --dpdk-prefix-len "$PREFIX" \
    500 \
    > "$TMPDIR/bench.log" 2>&1 &
BENCH_PID=$!

echo "  Waiting for $BENCH_TAP..."
for i in $(seq 1 20); do
    if ip link show "$BENCH_TAP" &>/dev/null; then
        ip link set "$BENCH_TAP" master "$BRIDGE"
        ip link set "$BENCH_TAP" up
        echo "  $BENCH_TAP attached to $BRIDGE (PID: $BENCH_PID)"
        break
    fi
    if ! kill -0 "$BENCH_PID" 2>/dev/null; then
        echo "  ERROR: Bench died"; cat "$TMPDIR/bench.log"
        echo "  --- Server log ---"; tail -20 "$TMPDIR/server.log"; exit 1
    fi
    sleep 0.2
done
ip link show "$BENCH_TAP" &>/dev/null || { echo "  ERROR: $BENCH_TAP not created"; exit 1; }
echo ""

# --- Wait for bench ---
echo "=== Waiting for benchmark (timeout 60s) ==="
TIMEOUT=60
for i in $(seq 1 $TIMEOUT); do
    if ! kill -0 "$BENCH_PID" 2>/dev/null; then break; fi
    sleep 1
done
if kill -0 "$BENCH_PID" 2>/dev/null; then
    echo "  TIMEOUT after ${TIMEOUT}s"
    kill "$BENCH_PID" 2>/dev/null; wait "$BENCH_PID" 2>/dev/null || true
    BENCH_EXIT=1
elif wait "$BENCH_PID" 2>/dev/null; then
    BENCH_EXIT=0
else
    BENCH_EXIT=$?
fi

echo ""
echo "=== Bench output ==="
cat "$TMPDIR/bench.log"
echo ""

if [[ $BENCH_EXIT -eq 0 ]]; then
    echo "============================================================"
    echo "  DPDK E2E TEST: PASSED"
    echo "============================================================"
else
    echo "============================================================"
    echo "  DPDK E2E TEST: FAILED (exit code $BENCH_EXIT)"
    echo "============================================================"
    echo ""
    echo "  --- Server log (last 30 lines) ---"
    tail -30 "$TMPDIR/server.log"
    exit 1
fi
