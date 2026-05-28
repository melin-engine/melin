#!/usr/bin/env bash
# Run the LAN benchmark suite against the local Docker test containers.
#
# Thin wrapper around lan-bench-suite.sh: it resolves the bench container
# IPs, applies container-friendly defaults (yield-idle so the shared host
# isn't pegged by busy-spin, standalone+local durability for the no-replica
# case, and the in-container /tmp/journal path), then hands off to the suite.
#
# Start the containers first with ./scripts/test-containers-start.sh.
#
# Usage:
#   ./scripts/docker-bench-suite.sh                       # tcp / throughput
#   TRANSPORTS=tcp WORKLOADS=throughput,single ./scripts/docker-bench-suite.sh
#   THROUGHPUT_DURATION=30s THROUGHPUT_WINDOW=256 ./scripts/docker-bench-suite.sh
#
# Every lan-bench-suite.sh environment variable is forwarded unchanged
# (TRANSPORTS, WORKLOADS, THROUGHPUT_DURATION, BENCH_BRANCH, PERF, ...).
# This wrapper only fills in defaults that suit the Docker setup; anything
# you set in the environment wins.
#
# Container-specific overrides:
#   SERVER_CONTAINER    (default: bench-server)
#   CLIENT_CONTAINER    (default: bench-client)
#   REPLICA_CONTAINER   (default: bench-replica)   — used if running
#   REPLICA2_CONTAINER  (default: bench-replica2)  — used if running
#   SSH_USER            (default: root)
#
# Defaults applied when unset:
#   TRANSPORTS=tcp  WORKLOADS=throughput
#   JOURNAL_PATH=/tmp/journal/bench.journal
#   SNAPSHOT_PATH=/tmp/journal/bench.snapshot
#   NO_PERSIST=1  (skip journal I/O — measures the transport floor without
#     fsync cost; set NO_PERSIST=0 to benchmark durable writes)
#   SERVER_EXTRA_ARGS="--yield-idle [--standalone --durability-mode local] <rate limits>"
#     (the standalone/local pair is omitted when a replicated transport is
#     selected, since replication needs a non-local durability mode)

set -euo pipefail

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    # Print the leading comment block as help: skip the shebang, strip the
    # "# " prefix from each comment line, and stop at the first non-comment
    # line (the blank line before `set -euo pipefail`). Driven by content,
    # so it survives edits to the header without a hardcoded line range.
    awk 'NR==1 {next} /^#/ {sub(/^# ?/, ""); print; next} {exit}' "$0"
    exit 0
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SUITE="${SCRIPT_DIR}/lan-bench-suite.sh"
if [[ ! -x "$SUITE" ]]; then
    echo "error: lan-bench-suite.sh not found next to this script ($SUITE)" >&2
    exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
    echo "error: docker not found on PATH" >&2
    exit 1
fi

SERVER_CONTAINER="${SERVER_CONTAINER:-bench-server}"
CLIENT_CONTAINER="${CLIENT_CONTAINER:-bench-client}"
REPLICA_CONTAINER="${REPLICA_CONTAINER:-bench-replica}"
REPLICA2_CONTAINER="${REPLICA2_CONTAINER:-bench-replica2}"
SSH_USER="${SSH_USER:-root}"

# Print the container's IP if it is running, or nothing otherwise (stopped
# or nonexistent). A single inspect resolves both. The `|| true` keeps a
# missing-container error (non-zero exit) from tripping `set -e` in the
# `VAR=$(container_ip ...)` assignments below.
container_ip() {
    docker inspect -f '{{if .State.Running}}{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}{{end}}' "$1" 2>/dev/null || true
}

SERVER_IP="$(container_ip "$SERVER_CONTAINER")"
CLIENT_IP="$(container_ip "$CLIENT_CONTAINER")"
REPLICA_IP="$(container_ip "$REPLICA_CONTAINER")"
REPLICA2_IP="$(container_ip "$REPLICA2_CONTAINER")"

if [[ -z "$SERVER_IP" || -z "$CLIENT_IP" ]]; then
    echo "error: bench containers not running (need '$SERVER_CONTAINER' and '$CLIENT_CONTAINER')." >&2
    echo "       Start them with: ./scripts/test-containers-start.sh" >&2
    exit 1
fi

# Container-friendly defaults (only when the caller hasn't set them).
TRANSPORTS="${TRANSPORTS:-tcp}"
WORKLOADS="${WORKLOADS:-throughput}"
JOURNAL_PATH="${JOURNAL_PATH:-/tmp/journal/bench.journal}"
SNAPSHOT_PATH="${SNAPSHOT_PATH:-/tmp/journal/bench.snapshot}"

# Default to no-persist: the containers' journal lives on the host's overlay
# filesystem, where fsync latency is unrepresentative and would dominate the
# measurement. Skipping journal I/O measures the transport floor instead.
# The default is surfaced in the config summary below (logged when applied).
NO_PERSIST_DEFAULTED=0
if [[ -z "${NO_PERSIST:-}" ]]; then
    NO_PERSIST=1
    NO_PERSIST_DEFAULTED=1
fi

# Default server args. Mirror the suite's high per-account rate limits so a
# power-law account distribution doesn't trip the ExceedsOrderRate guard,
# and enable yield-idle so the server doesn't busy-spin every pipeline core
# on a host shared with the bench client. For non-replicated transports add
# the standalone/local pair so the ack gate opens without a replica; for
# replicated transports leave durability at the server default (hybrid),
# which the connected replica satisfies.
RATE_ARGS="--max-orders-per-second 10000000 --max-orders-burst 50000000"
if [[ -z "${SERVER_EXTRA_ARGS:-}" ]]; then
    if [[ "$TRANSPORTS" == *repl* ]]; then
        SERVER_EXTRA_ARGS="--yield-idle ${RATE_ARGS}"
    else
        SERVER_EXTRA_ARGS="--yield-idle --standalone --durability-mode local ${RATE_ARGS}"
    fi
fi

# Ensure the journal directory exists in every running container the suite
# will use. Reuse the IPs already resolved above as the "is running" signal
# rather than inspecting each container a second time.
JOURNAL_DIR="$(dirname "$JOURNAL_PATH")"
for pair in "${SERVER_CONTAINER}=${SERVER_IP}" "${CLIENT_CONTAINER}=${CLIENT_IP}" \
            "${REPLICA_CONTAINER}=${REPLICA_IP}" "${REPLICA2_CONTAINER}=${REPLICA2_IP}"; do
    [[ -n "${pair#*=}" ]] || continue
    docker exec "${pair%%=*}" mkdir -p "$JOURNAL_DIR"
done

export TRANSPORTS WORKLOADS JOURNAL_PATH SNAPSHOT_PATH SERVER_EXTRA_ARGS NO_PERSIST

# A running replica2 without a replica can't be placed: the suite takes
# replica args positionally (replica first), so replica2 would land in the
# replica slot. Warn and ignore it rather than mis-wire the topology.
if [[ -z "$REPLICA_IP" && -n "$REPLICA2_IP" ]]; then
    echo "warning: ${REPLICA2_CONTAINER} is running but ${REPLICA_CONTAINER} is not; ignoring replica2." >&2
fi

# Build the positional argument list. pub-ip == vlan-ip inside the Docker
# network. Replica IPs are appended only when those containers are running.
SUITE_ARGS=("$SERVER_IP" "$CLIENT_IP" "$SERVER_IP" "$SSH_USER")
if [[ -n "$REPLICA_IP" ]]; then
    SUITE_ARGS+=("$REPLICA_IP" "$REPLICA_IP")
    if [[ -n "$REPLICA2_IP" ]]; then
        SUITE_ARGS+=("$REPLICA2_IP" "$REPLICA2_IP")
    fi
fi

echo "=== docker-bench-suite ==="
echo "  server:        ${SERVER_CONTAINER} (${SERVER_IP})"
echo "  client:        ${CLIENT_CONTAINER} (${CLIENT_IP})"
[[ -n "$REPLICA_IP" ]]  && echo "  replica:       ${REPLICA_CONTAINER} (${REPLICA_IP})"
[[ -n "$REPLICA2_IP" ]] && echo "  replica2:      ${REPLICA2_CONTAINER} (${REPLICA2_IP})"
echo "  transports:    ${TRANSPORTS}"
echo "  workloads:     ${WORKLOADS}"
echo "  journal:       ${JOURNAL_PATH}"
echo "  no-persist:    ${NO_PERSIST}"
[[ "$NO_PERSIST_DEFAULTED" == 1 ]] && \
    echo "                 (defaulted; set NO_PERSIST=0 to benchmark durable writes)"
echo "  server args:   ${SERVER_EXTRA_ARGS}"
echo ""

exec "$SUITE" "${SUITE_ARGS[@]}"
