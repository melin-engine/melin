#!/usr/bin/env bash
# End-to-end DPDK smoke test: both server and bench use DPDK.
#
# Uses a veth pair with DPDK's af_packet PMD. No real NIC needed —
# tests the full DPDK transport path on both sides.
#
# Flow:
#   1. Allocate hugepages
#   2. Build server + bench with --features dpdk
#   3. Generate auth keys
#   4. Create veth pair (veth0 <-> veth1)
#   5. Start server with af_packet on veth1
#   6. Start bench with af_packet on veth0
#   7. Run benchmark
#
# Usage:
#   sudo ./scripts/dpdk-e2e-smoke-test.sh
#
# Prerequisites:
#   - DPDK >= 22.11 installed (dnf install dpdk-devel)
#   - Must run as root (hugepages + veth creation)

set -euo pipefail

# Ensure cargo/rustup work when running under sudo.
if [[ -n "${SUDO_USER:-}" ]]; then
    REAL_HOME=$(eval echo "~$SUDO_USER")
    export PATH="$REAL_HOME/.cargo/bin:$PATH"
    export RUSTUP_HOME="${RUSTUP_HOME:-$REAL_HOME/.rustup}"
    export CARGO_HOME="${CARGO_HOME:-$REAL_HOME/.cargo}"
fi

if [[ $EUID -ne 0 ]]; then
    echo "error: must run as root (hugepages + veth creation)" >&2
    echo "usage: sudo $0" >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
TMPDIR=$(mktemp -d)

# IP configuration: server .1, bench .2, same /24 subnet.
SERVER_IP="192.168.200.1"
BENCH_IP="192.168.200.2"
PREFIX=24
PORT=9876

# Veth pair: veth0 (bench side) <-> veth1 (server side).
VETH_BENCH="dpdk-bench"
VETH_SERVER="dpdk-engine"

cleanup() {
    echo ""
    echo "=== Cleanup ==="

    # Kill server if running.
    if [[ -n "${SERVER_PID:-}" ]]; then
        kill "$SERVER_PID" 2>/dev/null && wait "$SERVER_PID" 2>/dev/null || true
        echo "  Server stopped"
    fi

    # Remove veth pair (deleting one end removes both).
    ip link del "$VETH_BENCH" 2>/dev/null || true
    echo "  Veth pair removed"

    # Remove DPDK runtime files.
    rm -rf /var/run/dpdk/server /var/run/dpdk/bench

    # Unmount 2MB hugepages if we mounted them.
    if [[ "${MOUNTED_HUGE_2M:-}" == "1" ]]; then
        umount "$HUGE_2M_MOUNT" 2>/dev/null || true
    fi

    # Restore target/ ownership.
    if [[ -n "${SUDO_USER:-}" ]]; then
        chown -R "$SUDO_USER:$SUDO_USER" "$PROJECT_DIR/target" 2>/dev/null || true
        echo "  Restored target/ ownership to $SUDO_USER"
    fi

    rm -rf "$TMPDIR"
    echo "  Temp dir cleaned: $TMPDIR"
}
trap cleanup EXIT

echo "============================================================"
echo "  DPDK E2E Smoke Test (server + bench, veth + af_packet)"
echo "  Server: $SERVER_IP:$PORT (smoltcp on $VETH_SERVER)"
echo "  Bench:  $BENCH_IP (smoltcp on $VETH_BENCH)"
echo "  Temp:   $TMPDIR"
echo "============================================================"
echo ""

# --- 0. Clean stale DPDK state ---
rm -rf /var/run/dpdk/server /var/run/dpdk/bench

# --- 1. Hugepages ---
echo "=== Hugepages ==="
HUGEPAGE_COUNT=$(cat /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages 2>/dev/null || echo 0)
if [[ "$HUGEPAGE_COUNT" -lt 512 ]]; then
    echo "  Allocating 512 x 2MB hugepages (need enough for two DPDK processes)..."
    echo 512 > /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages
    HUGEPAGE_COUNT=$(cat /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages)
fi
echo "  Hugepages available: $HUGEPAGE_COUNT x 2MB"

HUGE_2M_MOUNT="/mnt/huge_2m"
if ! mount | grep -q "pagesize=2M"; then
    mkdir -p "$HUGE_2M_MOUNT"
    mount -t hugetlbfs -o pagesize=2M nodev "$HUGE_2M_MOUNT"
    MOUNTED_HUGE_2M=1
    echo "  Mounted 2MB hugetlbfs at $HUGE_2M_MOUNT"
else
    HUGE_2M_MOUNT=$(mount | grep "pagesize=2M" | awk '{print $3}' | head -1)
    echo "  2MB hugetlbfs already mounted at $HUGE_2M_MOUNT"
fi
echo ""

# --- 2. Build ---
echo "=== Building ==="
cd "$PROJECT_DIR"

echo "  Building server with DPDK..."
cargo build --release -p melin-server --features dpdk --no-default-features --quiet 2>&1
echo "  server: OK"

echo "  Building bench with DPDK..."
cargo build --release -p melin-bench --features dpdk --no-default-features --quiet 2>&1
echo "  bench: OK"

echo "  Building keygen..."
cargo build --release --bin melin-keygen --quiet 2>&1
echo "  keygen: OK"
echo ""

# --- 3. Auth keys ---
echo "=== Auth keys ==="
cd "$TMPDIR"
"$PROJECT_DIR/target/release/melin-keygen" bench trader
echo "trader $(cat bench.pub | tr -d '\n') bench" > authorized_keys
echo "  Generated bench.key + authorized_keys"
echo ""

# --- 4. Create veth pair ---
echo "=== Creating veth pair ==="
ip link add "$VETH_BENCH" type veth peer name "$VETH_SERVER"
ip link set "$VETH_BENCH" up
ip link set "$VETH_SERVER" up
# Disable checksum offload — veth + af_packet can have issues with it.
ethtool -K "$VETH_BENCH" tx off rx off 2>/dev/null || true
ethtool -K "$VETH_SERVER" tx off rx off 2>/dev/null || true
echo "  $VETH_BENCH <-> $VETH_SERVER (up)"
echo ""

# --- 5. Start server ---
echo "=== Starting DPDK server ==="
RUST_LOG=info \
"$PROJECT_DIR/target/release/melin-server" \
    --bind "0.0.0.0:$PORT" \
    --journal "$TMPDIR/smoke.journal" \
    --authorized-keys "$TMPDIR/authorized_keys" \
    --standalone \
    --accounts 100 \
    --instruments 10 \
    --dpdk-eal-args="--vdev=net_af_packet0,iface=$VETH_SERVER --no-pci --log-level=6 --huge-dir=$HUGE_2M_MOUNT --file-prefix=server" \
    --dpdk-ip "$SERVER_IP" \
    --dpdk-prefix-len "$PREFIX" \
    > "$TMPDIR/server.log" 2>&1 &
SERVER_PID=$!
echo "  Server PID: $SERVER_PID"

# Wait for server to start (no TAP to watch for — check log for "listening").
echo "  Waiting for server..."
WAIT=0
while ! grep -q "DPDK transport listening" "$TMPDIR/server.log" 2>/dev/null; do
    sleep 0.5
    WAIT=$((WAIT + 1))
    if [[ $WAIT -gt 20 ]]; then
        echo "  ERROR: Server not ready after 10s"
        echo "  --- Server log ---"
        cat "$TMPDIR/server.log"
        exit 1
    fi
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "  ERROR: Server died"
        echo "  --- Server log ---"
        cat "$TMPDIR/server.log"
        exit 1
    fi
done
echo "  Server ready"
echo ""

# --- 6. Run bench ---
echo "=== Running DPDK bench ==="
echo "  1000 order pairs, 16 client, window 256"
echo ""

"$PROJECT_DIR/target/release/melin-bench" \
    --addr "$SERVER_IP:$PORT" \
    --key "$TMPDIR/bench.key" \
    --clients 16 \
    --window 256 \
    --warmup 100 \
    --dpdk-eal-args="--vdev=net_af_packet1,iface=$VETH_BENCH --no-pci --log-level=6 --huge-dir=$HUGE_2M_MOUNT --file-prefix=bench" \
    --dpdk-ip "$BENCH_IP" \
    --dpdk-prefix-len "$PREFIX" \
    --dpdk-core 5 \
    300000 \
    2>&1 | tee "$TMPDIR/bench.log"

BENCH_EXIT=$?

echo ""
if [[ $BENCH_EXIT -eq 0 ]]; then
    echo "============================================================"
    echo "  DPDK E2E SMOKE TEST: PASSED"
    echo "============================================================"
else
    echo "============================================================"
    echo "  DPDK E2E SMOKE TEST: FAILED (bench exit code $BENCH_EXIT)"
    echo "============================================================"
    echo ""
    echo "  --- Server log (last 50 lines) ---"
    tail -50 "$TMPDIR/server.log"
    exit 1
fi
