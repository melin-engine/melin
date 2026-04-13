#!/usr/bin/env bash
# Start the melin Docker container, run the TUI, then stop the container on exit.
#
# Usage:
#   ./scripts/run-tui.sh [--rebuild]
#
# Options:
#   --rebuild   Rebuild the Docker image before starting (implies --ssh default)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
DATA_DIR="${DATA_DIR:-/tmp/melin-data}"
IMAGE="${IMAGE:-melin}"
CONTAINER_NAME="melin-dev"
TUI_LOG="$REPO_DIR/tui.log"

# Parse args
REBUILD=0
for arg in "$@"; do
    case "$arg" in
        --rebuild) REBUILD=1 ;;
    esac
done

if [ "$REBUILD" -eq 1 ]; then
    echo "Building Docker image..."
    docker build --ssh default -t "$IMAGE" "$(dirname "$0")/.."
fi

mkdir -p "$DATA_DIR"

# Ensure container is not already running
docker rm -f "$CONTAINER_NAME" 2>/dev/null || true

echo "Starting melin exchange stack..."
docker run --rm --privileged \
    --name "$CONTAINER_NAME" \
    -p 9000:9000 \
    -p 9001:9001 \
    -v "$DATA_DIR:/data" \
    -d \
    "$IMAGE"

# Wait for OE gateway to accept connections
echo "Waiting for gateways..."
for i in $(seq 1 50); do
    if nc -z localhost 9000 2>/dev/null && nc -z localhost 9001 2>/dev/null; then
        break
    fi
    sleep 0.2
done

echo "Stack ready. Starting TUI..."
echo ""

# Stop container on exit regardless of how TUI exits
cleanup() {
    echo ""
    echo "=== Container logs ==="
    docker logs "$CONTAINER_NAME" 2>/dev/null || true
    echo ""
    echo "Stopping container..."
    docker stop "$CONTAINER_NAME" 2>/dev/null || true
    if [ -s "$TUI_LOG" ]; then
        echo ""
        echo "=== TUI log ==="
        cat "$TUI_LOG"
    fi
}
trap cleanup EXIT INT TERM

cargo run -p melin-tui-fix-client -- \
    --oe-addr localhost:9000 \
    --md-addr localhost:9001 \
    --sender TRADER \
    --oe-target MELIN-OE \
    --md-target MELIN-MD
