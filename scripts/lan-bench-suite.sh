#!/usr/bin/env bash
# Run the three README benchmarks on a LAN setup (two Cherry servers).
#
# Reproduces:
#   1. Peak throughput with full durability (fsync)
#   2. Peak throughput without persistence (no-persist)
#   3. Single-order latency (1 client, no pipelining, full durability)
#
# Usage:
#   ./scripts/lan-bench-suite.sh <server-public-ip> <bench-public-ip> <server-vlan-ip> [user]
#
# Example:
#   ./scripts/lan-bench-suite.sh 84.32.176.142 84.32.176.143 10.0.0.1 pierre
#
# Prerequisites:
#   - Same as lan-bench.sh (SSH access, cherry-deploy.sh setup, VLAN)
#   - Run bench-isolate.sh on both machines before this script for stable numbers

set -euo pipefail

if [[ $# -lt 3 ]]; then
    echo "usage: $0 <server-public-ip> <bench-public-ip> <server-vlan-ip> [user]"
    exit 1
fi

SERVER_PUB="$1"
BENCH_PUB="$2"
SERVER_VLAN="$3"
SSH_USER="${4:-root}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LAN_BENCH="${SCRIPT_DIR}/lan-bench.sh"

SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
SERVER="${SSH_USER}@${SERVER_PUB}"
BENCH="${SSH_USER}@${BENCH_PUB}"
REPO_DIR="~/workspace/trading"

RESULTS_DIR="/tmp/lan-bench-suite-$(date +%Y%m%d-%H%M%S)"
mkdir -p "${RESULTS_DIR}"

echo "============================================================"
echo "  README Benchmark Suite"
echo "  Server: ${SERVER_PUB} (VLAN: ${SERVER_VLAN})"
echo "  Bench:  ${BENCH_PUB}"
echo "  Results: ${RESULTS_DIR}"
echo "============================================================"
echo ""

# ---------------------------------------------------------------------------
# Build both binaries upfront (release + no-persist variant)
# ---------------------------------------------------------------------------
echo "=== Building release binaries on both machines ==="
for HOST in "${SERVER}" "${BENCH}"; do
    echo "  Building on ${HOST}..."
    ssh $SSH_OPTS "$HOST" "cd ${REPO_DIR} && git pull --ff-only && source ~/.cargo/env && \
        cargo build --release && \
        cargo build --release --features no-persist" 2>&1 | tail -3
done
echo "  Builds complete."
echo ""

# Prevent lan-bench.sh from rebuilding (we already built).
export CARGO_BUILD_FLAGS="--release"

# ---------------------------------------------------------------------------
# 1. Peak throughput — full durability (fsync)
# ---------------------------------------------------------------------------
echo ""
echo "============================================================"
echo "  [1/3] Peak throughput — full durability"
echo "  100M pairs, 16 clients, window 256"
echo "============================================================"
echo ""

"${LAN_BENCH}" "$SERVER_PUB" "$BENCH_PUB" "$SERVER_VLAN" "$SSH_USER" \
    -- --accounts 1000 --instruments 100 \
    -- 100000000 --clients 16 --window 256

cp /tmp/lan-bench-results.json "${RESULTS_DIR}/1-fsync.json" 2>/dev/null || true

# ---------------------------------------------------------------------------
# 2. Peak throughput — no persistence
# ---------------------------------------------------------------------------
echo ""
echo "============================================================"
echo "  [2/3] Peak throughput — no persistence"
echo "  100M pairs, 32 clients, window 192"
echo "============================================================"
echo ""

# For no-persist, we need to swap the server binary. The lan-bench.sh script
# always uses target/release/trading-server, so we swap it temporarily.
echo "  Swapping in no-persist server binary..."
ssh $SSH_OPTS "$SERVER" "cd ${REPO_DIR} && \
    cp target/release/trading-server target/release/trading-server.bak && \
    cp target/release/trading-server target/release/trading-server.persist && \
    find target/release/deps -name 'trading_server-*' -newer target/release/trading-server -executable 2>/dev/null | head -1 | xargs -I{} cp {} target/release/trading-server || true"

# The no-persist build produces the binary with the no-persist feature compiled in.
# We need to explicitly copy it. The feature flag is compiled into the binary at build time.
ssh $SSH_OPTS "$SERVER" "cd ${REPO_DIR} && source ~/.cargo/env && \
    cargo build --release --features no-persist 2>&1 | tail -1 && \
    cp target/release/trading-server target/release/trading-server.nopersist && \
    cp target/release/trading-server.nopersist target/release/trading-server"

"${LAN_BENCH}" "$SERVER_PUB" "$BENCH_PUB" "$SERVER_VLAN" "$SSH_USER" \
    -- --accounts 1000 --instruments 100 \
    -- 100000000 --clients 32 --window 192

cp /tmp/lan-bench-results.json "${RESULTS_DIR}/2-no-persist.json" 2>/dev/null || true

# Restore the normal (durable) binary.
echo "  Restoring durable server binary..."
ssh $SSH_OPTS "$SERVER" "cd ${REPO_DIR} && \
    cp target/release/trading-server.persist target/release/trading-server 2>/dev/null || true && \
    rm -f target/release/trading-server.bak target/release/trading-server.persist target/release/trading-server.nopersist"

# ---------------------------------------------------------------------------
# 3. Single-order latency — full durability, 1 client, no pipelining
# ---------------------------------------------------------------------------
echo ""
echo "============================================================"
echo "  [3/3] Single-order latency — full durability"
echo "  1M pairs, 1 client, window 1"
echo "============================================================"
echo ""

"${LAN_BENCH}" "$SERVER_PUB" "$BENCH_PUB" "$SERVER_VLAN" "$SSH_USER" \
    -- --accounts 1000 --instruments 100 \
    -- 1000000 --clients 1 --window 1

cp /tmp/lan-bench-results.json "${RESULTS_DIR}/3-single-order.json" 2>/dev/null || true

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "============================================================"
echo "  Suite complete. Results in ${RESULTS_DIR}/"
echo "============================================================"
echo ""
ls -la "${RESULTS_DIR}/"
