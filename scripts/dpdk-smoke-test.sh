#!/usr/bin/env bash
# Smoke test for the DPDK kernel-bypass transport.
#
# Uses DPDK's virtual TAP device (--vdev net_tap0) to test the full
# DPDK transport path without a real NIC. DPDK creates a kernel-visible
# TAP interface that regular TCP clients can connect through.
#
# Flow:
#   1. Allocate hugepages (if needed)
#   2. Build server with --features dpdk (no io-uring)
#   3. Build keygen + bench (default features)
#   4. Generate auth keys
#   5. Start server with DPDK TAP device
#   6. Configure TAP interface IP on kernel side
#   7. Run a short benchmark through the TAP interface
#   8. Verify orders were processed
#
# Usage:
#   sudo ./scripts/dpdk-smoke-test.sh
#
# Prerequisites:
#   - DPDK >= 22.11 installed (dnf install dpdk-devel)
#   - Must run as root (hugepages + TAP interface config)

set -euo pipefail

if [[ $EUID -ne 0 ]]; then
    echo "error: must run as root (hugepages + TAP interface)" >&2
    echo "usage: sudo $0" >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
TMPDIR=$(mktemp -d)

# DPDK TAP interface IP configuration.
# Server (DPDK side) gets .1, kernel TAP side gets .2.
DPDK_IP="192.168.200.1"
TAP_IP="192.168.200.2"
DPDK_PREFIX=24
DPDK_PORT=9876
TAP_IFACE="dtap0"  # DPDK names TAP interfaces "dtap0" by default

cleanup() {
    echo ""
    echo "=== Cleanup ==="

    # Kill server if running.
    if [[ -n "${SERVER_PID:-}" ]]; then
        kill "$SERVER_PID" 2>/dev/null && wait "$SERVER_PID" 2>/dev/null || true
        echo "  Server stopped (PID $SERVER_PID)"
    fi

    # Remove TAP interface IP (interface is auto-removed when DPDK exits).
    ip addr del "$TAP_IP/$DPDK_PREFIX" dev "$TAP_IFACE" 2>/dev/null || true

    # Clean up temp dir.
    rm -rf "$TMPDIR"
    echo "  Temp dir cleaned: $TMPDIR"
}
trap cleanup EXIT

echo "============================================================"
echo "  DPDK Smoke Test (TAP virtual device)"
echo "  DPDK IP:  $DPDK_IP:$DPDK_PORT"
echo "  TAP IP:   $TAP_IP (kernel side)"
echo "  Temp dir: $TMPDIR"
echo "============================================================"
echo ""

# --- 1. Hugepages ---
echo "=== Hugepages ==="
HUGEPAGE_COUNT=$(cat /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages 2>/dev/null || echo 0)
if [[ "$HUGEPAGE_COUNT" -lt 256 ]]; then
    echo "  Allocating 256 x 2MB hugepages..."
    echo 256 > /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages
    HUGEPAGE_COUNT=$(cat /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages)
fi
echo "  Hugepages available: $HUGEPAGE_COUNT x 2MB"

# Ensure hugepage mount exists.
if ! mount | grep -q hugetlbfs; then
    mkdir -p /dev/hugepages
    mount -t hugetlbfs nodev /dev/hugepages
    echo "  Mounted hugetlbfs at /dev/hugepages"
fi
echo ""

# --- 2. Build ---
echo "=== Building ==="

echo "  Building server with DPDK transport..."
cd "$PROJECT_DIR"
cargo build --release -p melin-server --features dpdk --no-default-features --quiet 2>&1
echo "  server build: OK"

echo "  Building keygen + bench (default features)..."
cargo build --release --bin melin-keygen --bin melin-bench --quiet 2>&1
echo "  keygen + bench build: OK"
echo ""

# --- 3. Auth keys ---
echo "=== Auth keys ==="
cd "$TMPDIR"
"$PROJECT_DIR/target/release/melin-keygen" bench admin 2>/dev/null
echo "admin $(cat admin.pub | tr -d '\n') bench" > authorized_keys
echo "  Generated bench.key + authorized_keys"
echo ""

# --- 4. Start DPDK server ---
echo "=== Starting DPDK server ==="
RUST_LOG=info,melin_server=debug,melin_dpdk=debug \
"$PROJECT_DIR/target/release/melin-server" \
    --bind "0.0.0.0:$DPDK_PORT" \
    --journal "$TMPDIR/smoke.journal" \
    --authorized-keys "$TMPDIR/authorized_keys" \
    --standalone \
    --accounts 100 \
    --instruments 10 \
    --dpdk-eal-args "--vdev net_tap0 --no-pci --log-level 6" \
    --dpdk-ip "$DPDK_IP" \
    --dpdk-prefix-len "$DPDK_PREFIX" \
    > "$TMPDIR/server.log" 2>&1 &
SERVER_PID=$!
echo "  Server PID: $SERVER_PID"
echo "  Log: $TMPDIR/server.log"

# Wait for DPDK to create the TAP interface.
echo "  Waiting for TAP interface ($TAP_IFACE)..."
WAIT=0
while ! ip link show "$TAP_IFACE" &>/dev/null; do
    sleep 0.5
    WAIT=$((WAIT + 1))
    if [[ $WAIT -gt 20 ]]; then
        echo "  ERROR: TAP interface not created after 10s"
        echo "  --- Server log (last 30 lines) ---"
        tail -30 "$TMPDIR/server.log"
        exit 1
    fi
    # Check if server died.
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "  ERROR: Server process died"
        echo "  --- Server log ---"
        cat "$TMPDIR/server.log"
        exit 1
    fi
done
echo "  TAP interface $TAP_IFACE is up"

# --- 5. Configure TAP interface ---
echo ""
echo "=== Configuring TAP interface ==="
ip addr add "$TAP_IP/$DPDK_PREFIX" dev "$TAP_IFACE"
ip link set "$TAP_IFACE" up
echo "  $TAP_IFACE: $TAP_IP/$DPDK_PREFIX (up)"

# Wait for smoltcp to be ready (ARP, TCP listen).
sleep 1

# Quick connectivity check — try to establish a TCP connection.
echo "  Testing TCP connectivity to $DPDK_IP:$DPDK_PORT..."
if timeout 3 bash -c "echo > /dev/tcp/$DPDK_IP/$DPDK_PORT" 2>/dev/null; then
    echo "  TCP connection: OK"
else
    echo "  WARNING: TCP connection failed (may be normal if smoltcp needs ARP)"
    echo "  Continuing anyway..."
fi
echo ""

# --- 6. Run benchmark ---
echo "=== Running smoke benchmark ==="
echo "  1000 order pairs, 1 client, window 1 (single-order latency)"

"$PROJECT_DIR/target/release/melin-bench" \
    --addr "$DPDK_IP:$DPDK_PORT" \
    --key "$TMPDIR/bench.key" \
    --clients 1 \
    --window 1 \
    --warmup 100 \
    1000 \
    2>&1 | tee "$TMPDIR/bench.log"

BENCH_EXIT=$?

echo ""
if [[ $BENCH_EXIT -eq 0 ]]; then
    echo "============================================================"
    echo "  DPDK SMOKE TEST: PASSED"
    echo "============================================================"
else
    echo "============================================================"
    echo "  DPDK SMOKE TEST: FAILED (bench exit code $BENCH_EXIT)"
    echo "============================================================"
    echo ""
    echo "  --- Server log (last 50 lines) ---"
    tail -50 "$TMPDIR/server.log"
    exit 1
fi
