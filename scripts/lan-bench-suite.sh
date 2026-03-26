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
#   # Only run the replication benchmark:
#   RUN_FSYNC=0 RUN_NOPERSIST=0 RUN_SINGLE=0 RUN_SWEEPS=0 RUN_PLOTS=0 \
#     ./scripts/lan-bench-suite.sh ... root 84.32.176.144 10.0.0.3
#
# Environment variables (all default to 1 = enabled):
#   RUN_FSYNC=0|1        Peak throughput with full durability
#   RUN_NOPERSIST=0|1    Peak throughput without persistence
#   RUN_SINGLE=0|1       Single-order latency
#   RUN_SWEEPS=0|1       Parameter sweeps (window, clients)
#   RUN_SWEEP_INSTRUMENTS=0|1  Instrument count sweep (default: off)
#   RUN_SWEEP_ACCOUNTS=0|1    Account count sweep (default: off)
#   RUN_REPLICATION=0|1  Synchronous replication benchmark
#   RUN_PLOTS=0|1        Generate plots from results
#   RESULTS_DIR=<path>   Reuse existing results directory (e.g. for re-plotting)
#   BENCH_BRANCH=<ref>   Checkout a specific branch on all machines
#   BENCH_COMMIT=<hash>  Checkout a specific commit on all machines (mutually exclusive with BENCH_BRANCH)
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

# Toggle individual benchmarks (default: all enabled).
RUN_FSYNC="${RUN_FSYNC:-1}"
RUN_NOPERSIST="${RUN_NOPERSIST:-1}"
RUN_SINGLE="${RUN_SINGLE:-1}"
RUN_PIPELINE="${RUN_PIPELINE:-0}"
RUN_SWEEPS="${RUN_SWEEPS:-0}"
RUN_SWEEP_INSTRUMENTS="${RUN_SWEEP_INSTRUMENTS:-0}"
RUN_SWEEP_ACCOUNTS="${RUN_SWEEP_ACCOUNTS:-0}"
RUN_REPLICATION="${RUN_REPLICATION:-1}"
RUN_PLOTS="${RUN_PLOTS:-1}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LAN_BENCH="${SCRIPT_DIR}/lan-bench.sh"

SSH_OPTS="-A -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
SERVER="${SSH_USER}@${SERVER_PUB}"
BENCH="${SSH_USER}@${BENCH_PUB}"
REPO_DIR="~/workspace/trading"

RESULTS_DIR="${RESULTS_DIR:-/tmp/lan-bench-suite-$(date +%Y%m%d-%H%M%S)}"
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
# BENCH_BRANCH: checkout a specific branch on all machines.
# BENCH_COMMIT: checkout a specific commit hash on all machines.
# Only one may be specified.
if [[ -n "${BENCH_BRANCH:-}" && -n "${BENCH_COMMIT:-}" ]]; then
    echo "error: BENCH_BRANCH and BENCH_COMMIT are mutually exclusive" >&2
    exit 1
fi

GIT_CMD="git pull --ff-only"
if [[ -n "${BENCH_BRANCH:-}" ]]; then
    GIT_CMD="git fetch origin && git checkout ${BENCH_BRANCH} && git reset --hard origin/${BENCH_BRANCH}"
    echo "=== Using branch: ${BENCH_BRANCH} ==="
    echo ""
elif [[ -n "${BENCH_COMMIT:-}" ]]; then
    GIT_CMD="git fetch origin && git checkout ${BENCH_COMMIT}"
    echo "=== Using commit: ${BENCH_COMMIT} ==="
    echo ""
fi

echo "=== Building release binaries on all machines ==="
BUILD_HOSTS=("${SERVER}" "${BENCH}")
if [[ -n "$REPLICA" ]]; then
    BUILD_HOSTS+=("${REPLICA}")
fi
NOPERSIST_BUILD=""
if [[ "$RUN_NOPERSIST" == "1" ]]; then
    NOPERSIST_BUILD="&& cargo build --release --features no-persist"
fi
for HOST in "${BUILD_HOSTS[@]}"; do
    echo "  Building on ${HOST}..."
    ssh $SSH_OPTS "$HOST" "cd ${REPO_DIR} && ${GIT_CMD} && source ~/.cargo/env && \
        cargo build --release ${NOPERSIST_BUILD}" 2>&1 | tail -3
done
echo "  Builds complete."
echo ""

# ---------------------------------------------------------------------------
# Generate auth keys (shared setup — needed by all benchmarks)
# ---------------------------------------------------------------------------
echo "=== Setting up auth keys ==="
ssh $SSH_OPTS "$BENCH" "cd ${REPO_DIR} && \
    if [[ ! -f bench.key ]]; then \
        source ~/.cargo/env && \
        cargo run --release -p melin-admin --bin melin-keygen -- bench trader && \
        echo 'Generated bench.key'; \
    else \
        echo 'bench.key already exists'; \
    fi"
AUTH_LINE=$(ssh $SSH_OPTS "$BENCH" "cd ${REPO_DIR} && cat bench.pub | xargs -I{} echo 'trader {} bench'")
ssh $SSH_OPTS "$SERVER" "cd ${REPO_DIR} && echo '${AUTH_LINE}' > authorized_keys"
echo "  Auth keys configured."
echo ""

# Prevent lan-bench.sh from rebuilding (we already built).
export CARGO_BUILD_FLAGS="--release"

# ---------------------------------------------------------------------------
# 1. Peak throughput — full durability (fsync)
# ---------------------------------------------------------------------------
if [[ "$RUN_FSYNC" == "1" ]]; then
echo ""
echo "============================================================"
echo "  [1/3] Peak throughput — full durability"
echo "  100M pairs, 16 clients, window 256"
echo "============================================================"
echo ""

"${LAN_BENCH}" "$SERVER_PUB" "$BENCH_PUB" "$SERVER_VLAN" "$SSH_USER" \
    -- -- 100000000 --clients 16 --window 256

cp /tmp/lan-bench-results.json "${RESULTS_DIR}/1-fsync.json" 2>/dev/null || true
fi

# ---------------------------------------------------------------------------
# 2. Peak throughput — no persistence
# ---------------------------------------------------------------------------
if [[ "$RUN_NOPERSIST" == "1" ]]; then
echo ""
echo "============================================================"
echo "  [2/3] Peak throughput — no persistence"
echo "  250M pairs, 16 clients, window 256"
echo "============================================================"
echo ""

# For no-persist, we need to swap the server binary. The lan-bench.sh script
# always uses target/release/melin-server, so we swap it temporarily.
echo "  Swapping in no-persist server binary..."
ssh $SSH_OPTS "$SERVER" "cd ${REPO_DIR} && \
    cp target/release/melin-server target/release/melin-server.bak && \
    cp target/release/melin-server target/release/melin-server.persist && \
    find target/release/deps -name 'trading_server-*' -newer target/release/melin-server -executable 2>/dev/null | head -1 | xargs -I{} cp {} target/release/melin-server || true"

# The no-persist build produces the binary with the no-persist feature compiled in.
# We need to explicitly copy it. The feature flag is compiled into the binary at build time.
ssh $SSH_OPTS "$SERVER" "cd ${REPO_DIR} && source ~/.cargo/env && \
    cargo build --release --features no-persist 2>&1 | tail -1 && \
    cp target/release/melin-server target/release/melin-server.nopersist && \
    cp target/release/melin-server.nopersist target/release/melin-server"

"${LAN_BENCH}" "$SERVER_PUB" "$BENCH_PUB" "$SERVER_VLAN" "$SSH_USER" \
    -- -- 100000000 --clients 16 --window 256

cp /tmp/lan-bench-results.json "${RESULTS_DIR}/2-no-persist.json" 2>/dev/null || true

# Restore the normal (durable) binary.
echo "  Restoring durable server binary..."
ssh $SSH_OPTS "$SERVER" "cd ${REPO_DIR} && \
    cp target/release/melin-server.persist target/release/melin-server 2>/dev/null || true && \
    rm -f target/release/melin-server.bak target/release/melin-server.persist target/release/melin-server.nopersist"
fi

# ---------------------------------------------------------------------------
# 3. Single-order latency — full durability, 1 client, no pipelining
# ---------------------------------------------------------------------------
if [[ "$RUN_SINGLE" == "1" ]]; then
echo ""
echo "============================================================"
echo "  [3/3] Single-order latency — full durability"
echo "  500K pairs, 1 client, window 1"
echo "============================================================"
echo ""

"${LAN_BENCH}" "$SERVER_PUB" "$BENCH_PUB" "$SERVER_VLAN" "$SSH_USER" \
    -- -- 500000 --clients 1 --window 1

cp /tmp/lan-bench-results.json "${RESULTS_DIR}/3-single-order.json" 2>/dev/null || true
fi

# ---------------------------------------------------------------------------
# 4. Pipeline breakdown — engine-only and pipeline (no network)
# ---------------------------------------------------------------------------
if [[ "$RUN_PIPELINE" == "1" ]]; then

PIPELINE_PAIRS=100000000
PIPELINE_WINDOW=256

# 4a. Engine only — matching engine without journal or network.
# Runs on the server machine (same CPU as production benchmarks).
echo ""
echo "============================================================"
echo "  Engine only — matching engine, no journal, no network"
echo "  ${PIPELINE_PAIRS} pairs, window ${PIPELINE_WINDOW}"
echo "============================================================"
echo ""

ssh $SSH_OPTS "$SERVER" "cd ${REPO_DIR} && source ~/.cargo/env && \
    ./target/release/melin-bench \
        --mode engine \
        --json /tmp/bench-results.json \
        ${PIPELINE_PAIRS}"

scp $SSH_OPTS -q "${SSH_USER}@${SERVER_PUB}:/tmp/bench-results.json" "${RESULTS_DIR}/5-engine-only.json" 2>/dev/null || true

# 4b. Pipeline (no network) — journal + matching, no TCP.
# Uses the dedicated NVMe journal disk for realistic fsync costs.
echo ""
echo "============================================================"
echo "  Pipeline — journal + matching, no network"
echo "  ${PIPELINE_PAIRS} pairs, window ${PIPELINE_WINDOW}"
echo "============================================================"
echo ""

JOURNAL_PATH="/mnt/journal/bench.journal"
ssh $SSH_OPTS "$SERVER" "rm -f ${JOURNAL_PATH} ${JOURNAL_PATH}.* 2>/dev/null; true"

ssh $SSH_OPTS "$SERVER" "cd ${REPO_DIR} && source ~/.cargo/env && \
    ./target/release/melin-bench \
        --mode pipeline \
        --window ${PIPELINE_WINDOW} \
        --journal ${JOURNAL_PATH} \
        --json /tmp/bench-results.json \
        ${PIPELINE_PAIRS}"

scp $SSH_OPTS -q "${SSH_USER}@${SERVER_PUB}:/tmp/bench-results.json" "${RESULTS_DIR}/6-pipeline.json" 2>/dev/null || true

fi

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
if [[ "$RUN_SWEEPS" == "1" ]]; then

# 4a. Window sweep (fixed clients=16, accounts/instruments=server defaults)
run_sweep "window" \
    "w32:--clients 16 --window 32" \
    "w64:--clients 16 --window 64" \
    "w128:--clients 16 --window 128" \
    "w256:--clients 16 --window 256" \
    "w512:--clients 16 --window 512"

# 4b. Client sweep (constant total in-flight = 4096, fixed 10M orders per point)
# Window adjusted so clients × window = 4096, isolating client count effects.
run_sweep "clients" \
    "c64:--clients 64 --window 64" \
    "c128:--clients 128 --window 32" \
    "c256:--clients 256 --window 16" \
    "c512:--clients 512 --window 8" \
    "c1024:--clients 1024 --window 4"

# 4c. Instruments sweep (fixed clients=16, window=128)
if [[ "$RUN_SWEEP_INSTRUMENTS" == "1" ]]; then
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
fi

# 4d. Account sweep (fixed clients=16, window=128)
# Tests whether account count affects hot-path latency via cache pressure
# on the balance HashMap.
if [[ "$RUN_SWEEP_ACCOUNTS" == "1" ]]; then
ACCT_SWEEP_DIR="${RESULTS_DIR}/sweep-accounts"
mkdir -p "${ACCT_SWEEP_DIR}"
echo ""
echo "============================================================"
echo "  Sweep: accounts"
echo "  ${ORDERS_PER_SWEEP} orders per point"
echo "============================================================"
echo ""
for accts in 100000 1000000 10000000; do
    label="a${accts}"
    echo "--- accounts=${accts} ---"
    "${LAN_BENCH}" "$SERVER_PUB" "$BENCH_PUB" "$SERVER_VLAN" "$SSH_USER" \
        -- --accounts "${accts}" \
        -- ${ORDERS_PER_SWEEP} --clients 16 --window 128 --accounts "${accts}"
    cp /tmp/lan-bench-results.json "${ACCT_SWEEP_DIR}/${label}.json" 2>/dev/null || true
    echo ""
done
fi

fi

# ---------------------------------------------------------------------------
# 5. Replication benchmark (optional — requires replica server)
# ---------------------------------------------------------------------------
if [[ "$RUN_REPLICATION" == "1" && -n "$REPLICA_PUB" && -n "$REPLICA_VLAN" ]]; then
    echo ""
    echo "============================================================"
    echo "  [5] Peak throughput — full durability + sync replication"
    echo "  100M pairs, 16 clients, window 256"
    echo "============================================================"
    echo ""

    # Pin IRQs to core 0 on the replica (server + bench are pinned by lan-bench.sh).
    echo "  Pinning IRQs to core 0 on replica..."
    ssh $SSH_OPTS "$REPLICA" 'pinned=0; failed=0
for f in /proc/irq/*/smp_affinity; do
    if echo 1 > "$f" 2>/dev/null; then
        pinned=$((pinned + 1))
    else
        failed=$((failed + 1))
    fi
done
echo "    Pinned ${pinned} IRQs to core 0 (${failed} unchanged)"'

    # Clean journals on both primary and replica.
    JOURNAL_PATH="/mnt/journal/bench.journal"
    REPLICA_JOURNAL="/mnt/journal/replica.journal"
    ssh $SSH_OPTS "$SERVER" "rm -f ${JOURNAL_PATH} ${JOURNAL_PATH}.* 2>/dev/null; true"
    ssh $SSH_OPTS "$REPLICA" "rm -f ${REPLICA_JOURNAL} ${REPLICA_JOURNAL}.* 2>/dev/null; true"

    # Verify replica can reach primary on VLAN.
    if ! ssh $SSH_OPTS "$REPLICA" "nc -z -w 3 ${SERVER_VLAN} 22" 2>/dev/null; then
        echo "  WARNING: replica cannot reach ${SERVER_VLAN} on VLAN — replication may fail"
    fi

    # Start primary — it blocks on replica_ready before seeding, so the
    # "listening" log only appears after the replica connects AND seeding
    # completes. Start order: primary → wait for repl port → replica →
    # wait for "listening" (seeding done, accept loop running).
    echo "  Starting primary on ${SERVER} with --replication-bind..."
    ssh $SSH_OPTS "$SERVER" "pkill -x melin-server 2>/dev/null; true"
    sleep 1
    ssh $SSH_OPTS "$SERVER" "RUST_LOG=info nohup ${REPO_DIR}/target/release/melin-server \
            --bind ${SERVER_VLAN}:9876 \
            --health-bind ${SERVER_VLAN}:9878 \
            --journal ${JOURNAL_PATH} \
            --authorized-keys ${REPO_DIR}/authorized_keys \
            --replication-bind ${SERVER_VLAN}:${REPL_PORT} \
        >/tmp/melin-server.log 2>&1 </dev/null &" </dev/null

    # Wait for the replication listener to be ready before starting the replica.
    echo "  Waiting for replication listener..."
    for i in $(seq 1 30); do
        if ssh $SSH_OPTS "$SERVER" "grep -q 'replication sender listening' /tmp/melin-server.log 2>/dev/null"; then
            echo "  Replication listener ready (took ${i}s)."
            break
        fi
        if [[ $i -eq 30 ]]; then
            echo "  ERROR: Replication listener did not start. Check /tmp/melin-server.log"
            ssh $SSH_OPTS "$SERVER" "tail -20 /tmp/melin-server.log" 2>/dev/null || true
        fi
        sleep 1
    done

    # Start replica — connects to primary's replication port.
    echo "  Starting replica on ${REPLICA}..."
    ssh $SSH_OPTS "$REPLICA" "pkill -x melin-server 2>/dev/null; true"
    sleep 1
    ssh $SSH_OPTS "$REPLICA" "RUST_LOG=info nohup ${REPO_DIR}/target/release/melin-server \
            --replica-of ${SERVER_VLAN}:${REPL_PORT} \
            --journal ${REPLICA_JOURNAL} \
        >/tmp/trading-replica.log 2>&1 </dev/null &" </dev/null

    # Wait for "listening" — printed after replica connects, seeding completes,
    # and the accept loop starts. This is the true "ready for clients" signal.
    echo "  Waiting for primary to seed and start accepting clients..."
    for i in $(seq 1 120); do
        if ssh $SSH_OPTS "$SERVER" "grep -q 'listening' /tmp/melin-server.log 2>/dev/null"; then
            echo "  Primary is ready (took ${i}s)."
            break
        fi
        if [[ $i -eq 120 ]]; then
            echo "  ERROR: Primary did not become ready. Check /tmp/melin-server.log"
            ssh $SSH_OPTS "$SERVER" "tail -20 /tmp/melin-server.log" 2>/dev/null || true
        fi
        sleep 1
    done

    # Run the benchmark against the primary (same as fsync benchmark).
    echo "  Running benchmark..."
    ssh $SSH_OPTS "$BENCH" "cd ${REPO_DIR} && source ~/.cargo/env && \
        ./target/release/melin-bench \
            --addr ${SERVER_VLAN}:9876 \
            --health-addr ${SERVER_VLAN}:9878 \
            --key bench.key \
            --json /tmp/bench-results.json \
            --bench-cores 1 \
            100000000 --clients 16 --window 256"

    scp $SSH_OPTS -q "${SSH_USER}@${BENCH_PUB}:/tmp/bench-results.json" "${RESULTS_DIR}/4-replication.json" 2>/dev/null || true

    # Stop both servers.
    ssh $SSH_OPTS "$SERVER" "pkill -INT -x melin-server 2>/dev/null; true"
    ssh $SSH_OPTS "$REPLICA" "pkill -INT -x melin-server 2>/dev/null; true"
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
if [[ "$RUN_PLOTS" == "1" ]]; then
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
    (cd "$LOCAL_REPO" && cargo build --release -p melin-bench --features plot --bin melin-plot 2>&1 | tail -1)
    PLOT_TOOL="${LOCAL_REPO}/target/release/melin-plot"

    echo "  Generating latency CDF..."
    CDF_FILES=(
        "${RESULTS_DIR}/1-fsync.json"
        "${RESULTS_DIR}/2-no-persist.json"
    )
    if [[ -f "${RESULTS_DIR}/4-replication.json" ]]; then
        CDF_FILES+=("${RESULTS_DIR}/4-replication.json")
    fi
    "${PLOT_TOOL}" latency-cdf -o "${PLOT_DIR}/latency-cdf.svg" \
        "${CDF_FILES[@]}" 2>&1

    for sweep in window clients instruments accounts; do
        dir="${RESULTS_DIR}/sweep-${sweep}"
        if [[ -d "$dir" ]] && ls "${dir}"/*.json &>/dev/null; then
            echo "  Generating sweep plot: ${sweep}..."
            "${PLOT_TOOL}" sweep -o "${PLOT_DIR}/saturation-${sweep}.svg" \
                "${dir}"/*.json 2>&1
        fi
    done

    # Latency stability over time — one plot per mode.
    for f_label in "1-fsync:fsync" "2-no-persist:no-persist" "4-replication:replication"; do
        f="${f_label%%:*}"
        label="${f_label##*:}"
        if [[ -f "${RESULTS_DIR}/${f}.json" ]]; then
            echo "  Generating latency stability: ${label}..."
            "${PLOT_TOOL}" stability -o "${PLOT_DIR}/latency-stability-${label}.svg" \
                "${RESULTS_DIR}/${f}.json" 2>&1
        fi
    done

    # Server health metrics over time — one set of plots per mode.
    for f_label in "1-fsync:fsync" "2-no-persist:no-persist" "4-replication:replication"; do
        f="${f_label%%:*}"
        label="${f_label##*:}"
        if [[ -f "${RESULTS_DIR}/${f}.json" ]]; then
            echo "  Generating health plots: ${label}..."
            "${PLOT_TOOL}" health -o "${PLOT_DIR}/health-${label}" \
                "${RESULTS_DIR}/${f}.json" 2>&1
        fi
    done

    echo ""
    echo "  Plots written to docs/plots/"
fi
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
