#!/usr/bin/env bash
# Run the README benchmarks on a LAN setup (two Cherry servers).
#
# Reproduces:
#   1. Peak throughput with full durability (fsync)
#   2. Peak throughput without persistence (no-persist)
#   3. Single-order latency (1 client, no pipelining, full durability)
#   4. Parameter sweeps (window, instruments)
#   5. Peak throughput with synchronous replication (optional, needs replica)
#
# Usage:
#   ./scripts/lan-bench-suite.sh <server-public-ip> <bench-public-ip> <server-vlan-ip> [user] [replica-public-ip] [replica-vlan-ip]
#
# Examples:
#   # Without replication (2 servers):
#   ./scripts/lan-bench-suite.sh 84.32.176.142 84.32.176.143 10.0.0.1
#
#   # With replication (3 servers):
#   ./scripts/lan-bench-suite.sh 84.32.176.142 84.32.176.143 10.0.0.1 root 84.32.176.144 10.0.0.3
#
# Prerequisites:
#   - Same as lan-bench.sh (SSH access, cherry-deploy.sh setup, VLAN)
#   - Run bench-isolate.sh on both machines before this script for stable numbers

set -euo pipefail

if [[ $# -lt 3 ]]; then
    echo "usage: $0 <server-public-ip> <bench-public-ip> <server-vlan-ip> [user] [replica-public-ip] [replica-vlan-ip]"
    exit 1
fi

SERVER_PUB="$1"
BENCH_PUB="$2"
SERVER_VLAN="$3"
SSH_USER="${4:-root}"
REPLICA_PUB="${5:-}"
REPLICA_VLAN="${6:-}"
REPLICA="${REPLICA_PUB:+${SSH_USER}@${REPLICA_PUB}}"
REPL_PORT=9877

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
echo "  Server:  ${SERVER_PUB} (VLAN: ${SERVER_VLAN})"
echo "  Bench:   ${BENCH_PUB}"
if [[ -n "$REPLICA_PUB" ]]; then
echo "  Replica: ${REPLICA_PUB} (VLAN: ${REPLICA_VLAN})"
fi
echo "  Results: ${RESULTS_DIR}"
echo "============================================================"
echo ""

# ---------------------------------------------------------------------------
# Build both binaries upfront (release + no-persist variant)
# ---------------------------------------------------------------------------
# BENCH_BRANCH env var: checkout a specific branch on all machines.
# Usage: BENCH_BRANCH=feat/replication ./scripts/lan-bench-suite.sh ...
GIT_CMD="git pull --ff-only"
if [[ -n "${BENCH_BRANCH:-}" ]]; then
    GIT_CMD="git fetch origin && git checkout ${BENCH_BRANCH} && git pull origin ${BENCH_BRANCH}"
    echo "=== Using branch: ${BENCH_BRANCH} ==="
    echo ""
fi

echo "=== Building release binaries on all machines ==="
BUILD_HOSTS=("${SERVER}" "${BENCH}")
if [[ -n "$REPLICA" ]]; then
    BUILD_HOSTS+=("${REPLICA}")
fi
for HOST in "${BUILD_HOSTS[@]}"; do
    echo "  Building on ${HOST}..."
    ssh $SSH_OPTS "$HOST" "cd ${REPO_DIR} && ${GIT_CMD} && source ~/.cargo/env && \
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
    -- -- 100000000 --clients 16 --window 256

cp /tmp/lan-bench-results.json "${RESULTS_DIR}/1-fsync.json" 2>/dev/null || true

# ---------------------------------------------------------------------------
# 2. Peak throughput — no persistence
# ---------------------------------------------------------------------------
echo ""
echo "============================================================"
echo "  [2/3] Peak throughput — no persistence"
echo "  100M pairs, 16 clients, window 384"
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
    -- -- 100000000 --clients 16 --window 384

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
    -- -- 1000000 --clients 1 --window 1

cp /tmp/lan-bench-results.json "${RESULTS_DIR}/3-single-order.json" 2>/dev/null || true

# ---------------------------------------------------------------------------
# Helper: run a sweep and collect results into a subdirectory.
# Usage: run_sweep <sweep-name> <orders> <configs...>
#   Each config is: "label:--clients N --window W --accounts A --instruments I"
#   The label is used for the JSON filename.
# ---------------------------------------------------------------------------
ORDERS_PER_SWEEP=10000000

run_sweep() {
    local sweep_name="$1"
    shift
    local sweep_dir="${RESULTS_DIR}/sweep-${sweep_name}"
    mkdir -p "${sweep_dir}"

    echo ""
    echo "============================================================"
    echo "  Sweep: ${sweep_name}"
    echo "  ${ORDERS_PER_SWEEP} orders per point"
    echo "============================================================"
    echo ""

    for config in "$@"; do
        local label="${config%%:*}"
        local bench_args="${config#*:}"
        echo "--- ${label} ---"

        "${LAN_BENCH}" "$SERVER_PUB" "$BENCH_PUB" "$SERVER_VLAN" "$SSH_USER" \
            -- -- ${ORDERS_PER_SWEEP} ${bench_args}

        cp /tmp/lan-bench-results.json "${sweep_dir}/${label}.json" 2>/dev/null || true
        echo ""
    done
}

# ---------------------------------------------------------------------------
# 4. Sweeps — one parameter at a time, others held fixed
# ---------------------------------------------------------------------------

# 4a. Window sweep (fixed clients=16, accounts/instruments=server defaults)
run_sweep "window" \
    "w32:--clients 16 --window 32" \
    "w64:--clients 16 --window 64" \
    "w128:--clients 16 --window 128" \
    "w256:--clients 16 --window 256" \
    "w512:--clients 16 --window 512"

# Accounts sweep removed: seeding cost is O(accounts × instruments),
# so 10M+ accounts take hours to seed. Needs lazy account creation
# or fast-path seeding before this is viable.

# 4c. Instruments sweep (fixed clients=16, window=128)
INST_SWEEP_DIR="${RESULTS_DIR}/sweep-instruments"
mkdir -p "${INST_SWEEP_DIR}"
echo ""
echo "============================================================"
echo "  Sweep: instruments"
echo "  ${ORDERS_PER_SWEEP} orders per point"
echo "============================================================"
echo ""
for inst in 10 100 1000; do
    label="i${inst}"
    echo "--- instruments=${inst} ---"
    "${LAN_BENCH}" "$SERVER_PUB" "$BENCH_PUB" "$SERVER_VLAN" "$SSH_USER" \
        -- --instruments "${inst}" \
        -- ${ORDERS_PER_SWEEP} --clients 16 --window 128
    cp /tmp/lan-bench-results.json "${INST_SWEEP_DIR}/${label}.json" 2>/dev/null || true
    echo ""
done

# ---------------------------------------------------------------------------
# 5. Replication benchmark (optional — requires replica server)
# ---------------------------------------------------------------------------
if [[ -n "$REPLICA_PUB" && -n "$REPLICA_VLAN" ]]; then
    echo ""
    echo "============================================================"
    echo "  [5] Peak throughput — full durability + sync replication"
    echo "  100M pairs, 16 clients, window 256"
    echo "============================================================"
    echo ""

    # Clean journals on both primary and replica.
    JOURNAL_PATH="/mnt/journal/bench.journal"
    REPLICA_JOURNAL="/mnt/journal/replica.journal"
    ssh $SSH_OPTS "$SERVER" "rm -f ${JOURNAL_PATH} ${JOURNAL_PATH}.* 2>/dev/null; true"
    ssh $SSH_OPTS "$REPLICA" "rm -f ${REPLICA_JOURNAL} ${REPLICA_JOURNAL}.* 2>/dev/null; true"

    # Start primary first — it listens for replica connections on REPL_PORT.
    echo "  Starting primary on ${SERVER} with --replication-bind..."
    ssh $SSH_OPTS "$SERVER" "pkill -x trading-server 2>/dev/null; true"
    sleep 1
    ssh $SSH_OPTS "$SERVER" "RUST_LOG=info nohup ${REPO_DIR}/target/release/trading-server \
            --bind ${SERVER_VLAN}:9876 \
            --journal ${JOURNAL_PATH} \
            --authorized-keys ${REPO_DIR}/authorized_keys \
            --replication-bind ${SERVER_VLAN}:${REPL_PORT} \
        >/tmp/trading-server.log 2>&1 </dev/null &" </dev/null

    echo "  Waiting for primary to start..."
    for i in $(seq 1 120); do
        if ssh $SSH_OPTS "$BENCH" "nc -z ${SERVER_VLAN} 9876" 2>/dev/null; then
            echo "  Primary is ready (took ${i}s)."
            break
        fi
        if [[ $i -eq 120 ]]; then
            echo "  ERROR: Primary did not start. Check /tmp/trading-server.log"
            ssh $SSH_OPTS "$SERVER" "tail -20 /tmp/trading-server.log" 2>/dev/null || true
        fi
        sleep 1
    done

    # Start replica — connects to primary's replication port.
    # Must start after primary is listening, otherwise connect() fails.
    echo "  Starting replica on ${REPLICA}..."
    ssh $SSH_OPTS "$REPLICA" "pkill -x trading-server 2>/dev/null; true"
    sleep 1
    ssh $SSH_OPTS "$REPLICA" "RUST_LOG=info nohup ${REPO_DIR}/target/release/trading-server \
            --replica-of ${SERVER_VLAN}:${REPL_PORT} \
            --journal ${REPLICA_JOURNAL} \
        >/tmp/trading-replica.log 2>&1 </dev/null &" </dev/null

    # Wait for replica to connect and complete handshake.
    echo "  Waiting for replica to connect..."
    for i in $(seq 1 30); do
        if ssh $SSH_OPTS "$SERVER" "grep -q 'replica connected' /tmp/trading-server.log 2>/dev/null"; then
            echo "  Replica connected (took ${i}s)."
            break
        fi
        if [[ $i -eq 30 ]]; then
            echo "  WARNING: replica may not have connected. Check /tmp/trading-replica.log"
            ssh $SSH_OPTS "$REPLICA" "tail -5 /tmp/trading-replica.log" 2>/dev/null || true
        fi
        sleep 1
    done

    # Run the benchmark against the primary (same as fsync benchmark).
    echo "  Running benchmark..."
    ssh $SSH_OPTS "$BENCH" "cd ${REPO_DIR} && source ~/.cargo/env && \
        ./target/release/trading-bench \
            --addr ${SERVER_VLAN}:9876 \
            --key bench.key \
            --json /tmp/bench-results.json \
            100000000 --clients 16 --window 256"

    cp /tmp/lan-bench-results.json "${RESULTS_DIR}/4-replication.json" 2>/dev/null || true
    scp $SSH_OPTS -q "${SSH_USER}@${BENCH_PUB}:/tmp/bench-results.json" "${RESULTS_DIR}/4-replication.json" 2>/dev/null || true

    # Stop both servers.
    ssh $SSH_OPTS "$SERVER" "pkill -INT -x trading-server 2>/dev/null; true"
    ssh $SSH_OPTS "$REPLICA" "pkill -INT -x trading-server 2>/dev/null; true"
    sleep 2
    echo "  Servers stopped."

    # Verify journal consistency between primary and replica.
    echo ""
    echo "  Verifying journal consistency..."
    "${SCRIPT_DIR}/journal-verify.sh" "$SERVER" "$JOURNAL_PATH" "$REPLICA" "$REPLICA_JOURNAL"
    echo ""
else
    echo ""
    echo "  (skipping replication benchmark — no replica server specified)"
    echo ""
fi

# ---------------------------------------------------------------------------
# 6. Generate plots
# ---------------------------------------------------------------------------
echo ""
echo "============================================================"
echo "  Generating plots"
echo "============================================================"
echo ""

if command -v cargo &>/dev/null && [[ -f "$(dirname "$0")/../crates/bench/src/plot.rs" ]]; then
    LOCAL_REPO="$(cd "$(dirname "$0")/.." && pwd)"
    PLOT_DIR="${LOCAL_REPO}/docs/plots"
    mkdir -p "${PLOT_DIR}"

    echo "  Building plot tool..."
    (cd "$LOCAL_REPO" && cargo build --release -p trading-bench --features plot --bin trading-plot 2>&1 | tail -1)
    PLOT_TOOL="${LOCAL_REPO}/target/release/trading-plot"

    echo "  Generating latency CDF..."
    CDF_FILES=(
        "${RESULTS_DIR}/1-fsync.json"
        "${RESULTS_DIR}/2-no-persist.json"
        "${RESULTS_DIR}/3-single-order.json"
    )
    if [[ -f "${RESULTS_DIR}/4-replication.json" ]]; then
        CDF_FILES+=("${RESULTS_DIR}/4-replication.json")
    fi
    "${PLOT_TOOL}" latency-cdf -o "${PLOT_DIR}/latency-cdf.svg" \
        "${CDF_FILES[@]}" 2>&1

    for sweep in window instruments; do
        dir="${RESULTS_DIR}/sweep-${sweep}"
        if [[ -d "$dir" ]] && ls "${dir}"/*.json &>/dev/null; then
            echo "  Generating saturation curve: ${sweep}..."
            "${PLOT_TOOL}" saturation -o "${PLOT_DIR}/saturation-${sweep}.svg" \
                "${dir}"/*.json 2>&1
        fi
    done

    echo ""
    echo "  Plots written to docs/plots/"
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "============================================================"
echo "  Suite complete. Results in ${RESULTS_DIR}/"
echo "============================================================"
echo ""
find "${RESULTS_DIR}" -type f | sort
