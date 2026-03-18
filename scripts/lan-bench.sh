#!/usr/bin/env bash
# Run a LAN benchmark across two Cherry servers.
#
# Deploys, builds, starts the engine on the server, runs the benchmark
# from the bench machine, collects results.
#
# Usage:
#   ./scripts/lan-bench.sh <server-public-ip> <bench-public-ip> <server-vlan-ip> [user]
#
# Example:
#   ./scripts/lan-bench.sh 84.32.176.142 84.32.176.143 10.0.0.1
#   ./scripts/lan-bench.sh 84.32.176.142 84.32.176.143 10.0.0.1 pierre
#
# Prerequisites:
#   - SSH access to both machines (as root by default, or as [user])
#   - Both machines have been set up via cherry-deploy.sh (or cherry-setup.sh)
#   - A VLAN/private network between the two machines
#   - The bench machine can reach <server-vlan-ip> over the private network
#
# What it does:
#   1. Builds the latest code on both machines (git pull + cargo build)
#   2. Generates auth keys on the bench machine
#   3. Starts the engine on the server (with bench-isolate.sh)
#   4. Runs the benchmark from the bench machine
#   5. Prints results and copies the JSON output locally

set -euo pipefail

if [[ $# -lt 3 ]]; then
    echo "usage: $0 <server-public-ip> <bench-public-ip> <server-vlan-ip> [user]"
    echo ""
    echo "  server-public-ip  SSH-reachable IP of the engine server"
    echo "  bench-public-ip   SSH-reachable IP of the benchmark client machine"
    echo "  server-vlan-ip    VLAN/private IP of the engine server (used for trading traffic)"
    echo "  user              SSH username (default: root)"
    echo ""
    echo "example:"
    echo "  $0 84.32.176.142 84.32.176.143 10.0.0.1"
    echo "  $0 84.32.176.142 84.32.176.143 10.0.0.1 pierre"
    exit 1
fi

SERVER_PUB="$1"
BENCH_PUB="$2"
SERVER_VLAN="$3"
SSH_USER="${4:-root}"

SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
SERVER="${SSH_USER}@${SERVER_PUB}"
BENCH="${SSH_USER}@${BENCH_PUB}"

REPO_DIR="~/workspace/trading"
JOURNAL_PATH="${JOURNAL_PATH:-/mnt/journal/bench.journal}"
SNAPSHOT_PATH="${SNAPSHOT_PATH:-/mnt/journal/bench.snapshot}"
BIND_ADDR="${SERVER_VLAN}:9876"
PAIRS="${PAIRS:-100000000}"
WINDOW="${WINDOW:-256}"
CLIENTS="${CLIENTS:-16}"
CARGO_BUILD_FLAGS="${CARGO_BUILD_FLAGS:---release}"

echo "=== LAN Benchmark ==="
echo "  Server:     ${SERVER} (VLAN: ${SERVER_VLAN})"
echo "  Bench:      ${BENCH}"
echo "  Pairs:      ${PAIRS} ($(( PAIRS * 2 )) orders)"
echo "  Clients:    ${CLIENTS}"
echo "  Window:     ${WINDOW}"
echo ""

# ---------------------------------------------------------------------------
# 1. Build on both machines
# ---------------------------------------------------------------------------
build_remote() {
    local host="$1"
    local label="$2"
    echo "=== Building on ${label} (${host}) ==="
    ssh $SSH_OPTS "$host" "cd ${REPO_DIR} && git pull --ff-only && source ~/.cargo/env && cargo build ${CARGO_BUILD_FLAGS}" 2>&1 | tail -3
    echo "  ${label} build: OK"
    echo ""
}

build_remote "$SERVER" "server"
build_remote "$BENCH" "bench"

# ---------------------------------------------------------------------------
# 2. Generate auth keys on the bench machine (if not already present)
# ---------------------------------------------------------------------------
echo "=== Setting up auth keys ==="
ssh $SSH_OPTS "$BENCH" "cd ${REPO_DIR} && \
    if [[ ! -f bench.key ]]; then \
        source ~/.cargo/env && \
        cargo run --release -p trading-admin --bin trading-keygen -- bench admin && \
        echo 'Generated bench.key'; \
    else \
        echo 'bench.key already exists'; \
    fi"

# Copy the authorized_keys line to the server.
AUTH_LINE=$(ssh $SSH_OPTS "$BENCH" "cd ${REPO_DIR} && cat bench.pub | xargs -I{} echo 'admin {} bench'")
ssh $SSH_OPTS "$SERVER" "cd ${REPO_DIR} && echo '${AUTH_LINE}' > authorized_keys"
echo "  Auth keys configured."
echo ""

# ---------------------------------------------------------------------------
# 3. Clean old journal on the server
# ---------------------------------------------------------------------------
echo "=== Cleaning old journal ==="
ssh $SSH_OPTS "$SERVER" "rm -f ${JOURNAL_PATH} ${JOURNAL_PATH}.* ${SNAPSHOT_PATH} ${SNAPSHOT_PATH}.* 2>/dev/null; echo 'Cleaned.'"
echo ""

# ---------------------------------------------------------------------------
# 4. Start the engine on the server
# ---------------------------------------------------------------------------
echo "=== Starting engine on server ==="
# Kill any existing trading-server process.
ssh $SSH_OPTS "$SERVER" "pkill -x trading-server 2>/dev/null; true"
sleep 1

# Start the server in the background. For production-grade numbers, run
# bench-isolate.sh separately before this script (CPU governor, IRQ pinning).
ssh $SSH_OPTS "$SERVER" "RUST_LOG=info nohup ${REPO_DIR}/target/release/trading-server \
        --bind ${BIND_ADDR} \
        --journal ${JOURNAL_PATH} \
        --authorized-keys ${REPO_DIR}/authorized_keys \
        --accounts ${ACCOUNTS:-1000} \
        --instruments ${INSTRUMENTS:-100} \
    >/tmp/trading-server.log 2>&1 </dev/null &" </dev/null

# Wait for the server to be ready.
echo "  Waiting for server to start..."
for i in $(seq 1 120); do
    if ssh $SSH_OPTS "$BENCH" "nc -z ${SERVER_VLAN} 9876" 2>/dev/null; then
        echo "  Server is ready (took ${i}s)."
        break
    fi
    if [[ $i -eq 120 ]]; then
        echo "  ERROR: Server did not start within 120s. Check /tmp/trading-server.log on server."
        ssh $SSH_OPTS "$SERVER" "tail -20 /tmp/trading-server.log" 2>/dev/null || true
        exit 1
    fi
    sleep 1
done
echo ""

# ---------------------------------------------------------------------------
# 5. Copy the bench key to the bench machine and run the benchmark
# ---------------------------------------------------------------------------
echo "=== Running benchmark ==="
echo "  ${PAIRS} order pairs, ${CLIENTS} clients, window ${WINDOW}"
echo ""

ssh $SSH_OPTS "$BENCH" "cd ${REPO_DIR} && source ~/.cargo/env && \
    ./target/release/trading-bench ${PAIRS} \
        --addr ${BIND_ADDR} \
        --window ${WINDOW} \
        --clients ${CLIENTS} \
        --key bench.key \
        --json /tmp/bench-results.json"

echo ""

# ---------------------------------------------------------------------------
# 6. Collect results
# ---------------------------------------------------------------------------
echo "=== Results ==="
ssh $SSH_OPTS "$BENCH" "cat /tmp/bench-results.json" 2>/dev/null | tee /tmp/lan-bench-results.json
echo ""

# Copy results locally.
scp $SSH_OPTS -q "$BENCH":/tmp/bench-results.json /tmp/lan-bench-results.json 2>/dev/null || true
echo "  Results saved to /tmp/lan-bench-results.json"
echo ""

# ---------------------------------------------------------------------------
# 7. Stop the server
# ---------------------------------------------------------------------------
echo "=== Stopping server ==="
ssh $SSH_OPTS "$SERVER" "pkill -INT -x trading-server 2>/dev/null; true"
sleep 2
echo "  Server stopped."

echo ""
echo "=== Benchmark complete ==="
