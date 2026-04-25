#!/usr/bin/env bash
# Start the melin Docker container, run the TUI, then stop the container on exit.
#
# Usage:
#   ./scripts/run-tui.sh [--rebuild] [--no-bot]
#
# Options:
#   --rebuild   Rebuild the Docker image before starting (implies --ssh default)
#   --no-bot    Run the TUI without the synthetic order-flow bot.
#               Default is to pass --bot so the book has visible flow.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
DATA_DIR="${DATA_DIR:-/tmp/melin-data}"
IMAGE="${IMAGE:-melin}"
CONTAINER_NAME="melin-dev"
TUI_LOG="$REPO_DIR/tui.log"

# Parse args
REBUILD=0
BOT=1
for arg in "$@"; do
    case "$arg" in
        --rebuild) REBUILD=1 ;;
        --no-bot) BOT=0 ;;
        *)
            echo "error: unknown arg: $arg" >&2
            echo "usage: $0 [--rebuild] [--no-bot]" >&2
            exit 2
            ;;
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
# No --rm: keep the container around after exit so `docker logs` can still
# read its output during cleanup. Without this, an early crash inside the
# entrypoint disappears before the TUI quits and the logs are unrecoverable.
# Removed explicitly in cleanup() below.
docker run --privileged \
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

# Stop container on exit regardless of how TUI exits.
# Order matters: read logs first (works even if the container has already
# exited, as long as it hasn't been removed), THEN remove the container.
cleanup() {
    echo ""
    echo "=== Container logs ==="
    docker logs "$CONTAINER_NAME" 2>/dev/null || true
    echo ""
    echo "Removing container..."
    docker rm -f "$CONTAINER_NAME" 2>/dev/null || true
    if [ -s "$TUI_LOG" ]; then
        echo ""
        echo "=== TUI log ==="
        cat "$TUI_LOG"
    fi
}
trap cleanup EXIT INT TERM

TUI_ARGS=(
    --oe-addr localhost:9000
    --md-addr localhost:9001
    --sender TRADER
    --oe-target MELIN-OE
    --md-target MELIN-MD
)
if [ "$BOT" -eq 1 ]; then
    TUI_ARGS+=(--bot)
fi

cargo run -p melin-tui-fix-client -- "${TUI_ARGS[@]}"
