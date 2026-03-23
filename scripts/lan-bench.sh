#!/usr/bin/env bash
# Run a LAN benchmark across two Cherry servers.
#
# Deploys, builds, starts the engine on the server, runs the benchmark
# from the bench machine, collects results.
#
# Usage:
#   ./scripts/lan-bench.sh <server-public-ip> <bench-public-ip> <server-vlan-ip> [user] [-- server-args... -- bench-args...]
#
# Examples:
#   # Defaults (100M pairs, 16 clients, window 256):
#   ./scripts/lan-bench.sh 84.32.176.142 84.32.176.143 10.0.0.1
#
#   # Custom server and bench args:
#   ./scripts/lan-bench.sh 84.32.176.142 84.32.176.143 10.0.0.1 pierre \
#       -- --accounts 2000 --instruments 50 \
#       -- 50000000 --clients 32 --window 128
#
#   # Only custom bench args (empty server args):
#   ./scripts/lan-bench.sh 84.32.176.142 84.32.176.143 10.0.0.1 pierre \
#       -- -- 200000000 --clients 8 --window 64
#
# The first "--" separates positional args from extra server args.
# The second "--" separates server args from extra bench args.
# Server always gets: --bind, --journal, --authorized-keys.
# Bench always gets: --addr, --key, --json.
#
# Prerequisites:
#   - SSH access to both machines (as root by default, or as [user])
#   - Both machines have been set up via cherry-deploy.sh (or cherry-setup.sh)
#   - A VLAN/private network between the two machines
#   - The bench machine can reach <server-vlan-ip> over the private network

set -euo pipefail

# ---------------------------------------------------------------------------
# Parse arguments: positional args, then "-- server-args -- bench-args"
# ---------------------------------------------------------------------------
POSITIONAL=()
SERVER_EXTRA_ARGS=""
BENCH_EXTRA_ARGS=""

# Collect positional args until first "--"
while [[ $# -gt 0 ]]; do
    if [[ "$1" == "--" ]]; then
        shift
        break
    fi
    POSITIONAL+=("$1")
    shift
done

# Collect server args until second "--"
while [[ $# -gt 0 ]]; do
    if [[ "$1" == "--" ]]; then
        shift
        break
    fi
    SERVER_EXTRA_ARGS="${SERVER_EXTRA_ARGS} $1"
    shift
done

# Remaining args are bench args
BENCH_EXTRA_ARGS="$*"

if [[ ${#POSITIONAL[@]} -lt 3 ]]; then
    echo "usage: $0 <server-public-ip> <bench-public-ip> <server-vlan-ip> [user] [-- server-args... -- bench-args...]"
    echo ""
    echo "  server-public-ip  SSH-reachable IP of the engine server"
    echo "  bench-public-ip   SSH-reachable IP of the benchmark client machine"
    echo "  server-vlan-ip    VLAN/private IP of the engine server (used for trading traffic)"
    echo "  user              SSH username (default: root)"
    echo ""
    echo "  After '--', extra args are passed to melin-server."
    echo "  After a second '--', extra args are passed to melin-bench."
    echo ""
    echo "examples:"
    echo "  $0 84.32.176.142 84.32.176.143 10.0.0.1"
    echo "  $0 84.32.176.142 84.32.176.143 10.0.0.1 pierre -- --accounts 2000 -- 50000000 --clients 32"
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
BIND_ADDR="${SERVER_VLAN}:9876"
CARGO_BUILD_FLAGS="${CARGO_BUILD_FLAGS:---release}"

echo "=== LAN Benchmark ==="
echo "  Server:      ${SERVER} (VLAN: ${SERVER_VLAN})"
echo "  Bench:       ${BENCH}"
echo "  Server args: --bind ${BIND_ADDR} --journal ${JOURNAL_PATH} --authorized-keys ...${SERVER_EXTRA_ARGS}"
echo "  Bench args:  --addr ${BIND_ADDR} --key bench.key --json ...${BENCH_EXTRA_ARGS}"
echo ""

# ---------------------------------------------------------------------------
# 1. Build on both machines
# ---------------------------------------------------------------------------
build_remote() {
    local host="$1"
    local label="$2"
    echo "=== Building on ${label} (${host}) ==="
    # BENCH_BRANCH env var: checkout a specific branch instead of pulling main.
    # Usage: BENCH_BRANCH=feat/replication ./scripts/lan-bench.sh ...
    local git_cmd="git pull --ff-only"
    if [[ -n "${BENCH_BRANCH:-}" ]]; then
        git_cmd="git fetch origin && git checkout ${BENCH_BRANCH} && git pull origin ${BENCH_BRANCH}"
    fi
    ssh $SSH_OPTS "$host" "cd ${REPO_DIR} && ${git_cmd} && source ~/.cargo/env && cargo build ${CARGO_BUILD_FLAGS}" 2>&1 | tail -3
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
        cargo run --release -p melin-admin --bin melin-keygen -- bench admin && \
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
# Kill any existing melin-server process.
ssh $SSH_OPTS "$SERVER" "pkill -x melin-server 2>/dev/null; true"
sleep 1

# Start the server in the background. For production-grade numbers, run
# bench-isolate.sh separately before this script (CPU governor, IRQ pinning).
ssh $SSH_OPTS "$SERVER" "RUST_LOG=info nohup ${REPO_DIR}/target/release/melin-server \
        --bind ${BIND_ADDR} \
        --journal ${JOURNAL_PATH} \
        --authorized-keys ${REPO_DIR}/authorized_keys \
        ${SERVER_EXTRA_ARGS} \
    >/tmp/melin-server.log 2>&1 </dev/null &" </dev/null

# Wait for the server to be ready.
echo "  Waiting for server to start..."
for i in $(seq 1 120); do
    if ssh $SSH_OPTS "$BENCH" "nc -z ${SERVER_VLAN} 9876" 2>/dev/null; then
        echo "  Server is ready (took ${i}s)."
        break
    fi
    if [[ $i -eq 120 ]]; then
        echo "  ERROR: Server did not start within 120s. Check /tmp/melin-server.log on server."
        ssh $SSH_OPTS "$SERVER" "tail -20 /tmp/melin-server.log" 2>/dev/null || true
        exit 1
    fi
    sleep 1
done
echo ""

# ---------------------------------------------------------------------------
# 5. Run the benchmark
# ---------------------------------------------------------------------------
echo "=== Running benchmark ==="
echo ""

ssh $SSH_OPTS "$BENCH" "cd ${REPO_DIR} && source ~/.cargo/env && \
    ./target/release/melin-bench \
        --addr ${BIND_ADDR} \
        --key bench.key \
        --json /tmp/bench-results.json \
        --bench-cores 1 \
        ${BENCH_EXTRA_ARGS}"

echo ""

# ---------------------------------------------------------------------------
# 6. Collect results
# ---------------------------------------------------------------------------
echo "=== Collecting results ==="
scp $SSH_OPTS -q "$BENCH":/tmp/bench-results.json /tmp/lan-bench-results.json 2>/dev/null || true
echo "  Results saved to /tmp/lan-bench-results.json"
echo ""

# ---------------------------------------------------------------------------
# 7. Stop the server
# ---------------------------------------------------------------------------
echo "=== Stopping server ==="
ssh $SSH_OPTS "$SERVER" "pkill -INT -x melin-server 2>/dev/null; true"
sleep 2
echo "  Server stopped."

echo ""
echo "=== Benchmark complete ==="
