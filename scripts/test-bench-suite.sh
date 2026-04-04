#!/usr/bin/env bash
# Smoke test the bench suite using Docker containers.
#
# Starts containers, copies local scripts, runs the suite with
# Docker-appropriate defaults, and stops containers on exit.
#
# Usage:
#   ./scripts/test-bench-suite.sh                                    # default: tcp-dual-repl throughput+single
#   ./scripts/test-bench-suite.sh TRANSPORTS=tcp,tcp-dual-repl       # override env vars
#   ./scripts/test-bench-suite.sh --no-cleanup                       # leave containers running
#   ./scripts/test-bench-suite.sh --branch feat/foo                  # checkout branch in containers
#
# Any KEY=VALUE positional args are forwarded as env vars to the bench
# suite. Docker-friendly defaults are applied for any vars not set.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CLEANUP=true
BRANCH_ARG=""
SUITE_ENV=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --no-cleanup) CLEANUP=false; shift ;;
        --branch) BRANCH_ARG="--branch $2"; shift 2 ;;
        *=*) SUITE_ENV+=("$1"); shift ;;
        *) echo "unknown arg: $1" >&2; exit 1 ;;
    esac
done

# ---------------------------------------------------------------------------
# Start containers
# ---------------------------------------------------------------------------
echo "=== Starting containers ==="
# shellcheck disable=SC2086
"${SCRIPT_DIR}/test-containers-start.sh" --dual-replica ${BRANCH_ARG}

SERVER_IP=$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' bench-server)
BENCH_IP=$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' bench-client)
REPLICA_IP=$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' bench-replica)
REPLICA2_IP=$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' bench-replica2)

# ---------------------------------------------------------------------------
# Cleanup on exit
# ---------------------------------------------------------------------------
cleanup() {
    if [[ "$CLEANUP" == "true" ]]; then
        echo ""
        echo "=== Stopping containers ==="
        "${SCRIPT_DIR}/test-containers-stop.sh"
    else
        echo ""
        echo "Containers left running (--no-cleanup). Re-run the suite with:"
        echo "  JOURNAL_PATH=/tmp/journal/bench.journal THROUGHPUT_ORDERS=1000 THROUGHPUT_CLIENTS=2 \\"
        echo "  THROUGHPUT_WINDOW=4 WARMUP_ORDERS=10 SINGLE_ORDERS=100 RUN_PLOTS=0 \\"
        echo "  TRANSPORTS=tcp-dual-repl WORKLOADS=throughput,single \\"
        echo "    ./scripts/lan-bench-suite.sh $SERVER_IP $BENCH_IP $SERVER_IP root \\"
        echo "      $REPLICA_IP $REPLICA_IP $REPLICA2_IP $REPLICA2_IP"
        echo ""
        echo "Stop containers with: ./scripts/test-containers-stop.sh"
    fi
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Copy local scripts into containers (test uncommitted changes)
# ---------------------------------------------------------------------------
echo ""
echo "=== Copying local scripts into containers ==="
for c in bench-server bench-client bench-replica bench-replica2; do
    for script in lan-bench-suite.sh journal-verify.sh lan-bench.sh dpdk-lan-bench.sh; do
        if [[ -f "${SCRIPT_DIR}/${script}" ]]; then
            docker cp "${SCRIPT_DIR}/${script}" "$c":/root/workspace/melin/scripts/"${script}"
        fi
    done
    docker exec "$c" mkdir -p /tmp/journal
done
echo "  Done."

# ---------------------------------------------------------------------------
# Docker-friendly defaults (overridable via KEY=VALUE args)
# ---------------------------------------------------------------------------
declare -A DEFAULTS=(
    [JOURNAL_PATH]=/tmp/journal/bench.journal
    [TRANSPORTS]=tcp-dual-repl
    [WORKLOADS]=throughput,single
    [THROUGHPUT_ORDERS]=1000
    [THROUGHPUT_CLIENTS]=2
    [THROUGHPUT_WINDOW]=4
    [WARMUP_ORDERS]=10
    [SINGLE_ORDERS]=100
    [ORDERS_PER_SWEEP]=1000
    [LOCAL_ORDERS]=1000
    [RUN_PLOTS]=0
)

# Build the env: defaults first, then user overrides.
ENV_ARGS=()
declare -A USER_KEYS
for kv in "${SUITE_ENV[@]+"${SUITE_ENV[@]}"}"; do
    key="${kv%%=*}"
    USER_KEYS[$key]=1
done
for key in "${!DEFAULTS[@]}"; do
    if [[ -z "${USER_KEYS[$key]:-}" ]]; then
        ENV_ARGS+=("${key}=${DEFAULTS[$key]}")
    fi
done
for kv in "${SUITE_ENV[@]+"${SUITE_ENV[@]}"}"; do
    ENV_ARGS+=("$kv")
done

# ---------------------------------------------------------------------------
# Run the bench suite
# ---------------------------------------------------------------------------
echo ""
echo "=== Running bench suite ==="
echo "  Env: ${ENV_ARGS[*]}"
echo ""

env "${ENV_ARGS[@]}" \
    "${SCRIPT_DIR}/lan-bench-suite.sh" \
    "$SERVER_IP" "$BENCH_IP" "$SERVER_IP" root \
    "$REPLICA_IP" "$REPLICA_IP" "$REPLICA2_IP" "$REPLICA2_IP"
