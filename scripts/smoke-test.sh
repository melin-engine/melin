#!/usr/bin/env bash
# Smoke test for the kernel TCP transport.
#
# Builds the server and bench, generates auth keys, starts the server
# on localhost, runs a short benchmark, and verifies orders were processed.
# No root required — runs entirely in userspace on loopback.
#
# Flow:
#   1. Build server + keygen + bench (default features)
#   2. Generate auth keys
#   3. Start server on localhost
#   4. Run a short benchmark
#   5. Verify orders were processed
#
# Usage:
#   ./scripts/smoke-test.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
TMPDIR=$(mktemp -d)

PORT=19876  # non-privileged port, unlikely to collide
ADDR="127.0.0.1:$PORT"

cleanup() {
    echo ""
    echo "=== Cleanup ==="

    # Kill server if running.
    if [[ -n "${SERVER_PID:-}" ]]; then
        kill "$SERVER_PID" 2>/dev/null && wait "$SERVER_PID" 2>/dev/null || true
        echo "  Server stopped (PID $SERVER_PID)"
    fi

    # Clean up temp dir.
    rm -rf "$TMPDIR"
    echo "  Temp dir cleaned: $TMPDIR"
}
trap cleanup EXIT

echo "============================================================"
echo "  Smoke Test (kernel TCP, localhost)"
echo "  Address:  $ADDR"
echo "  Temp dir: $TMPDIR"
echo "============================================================"
echo ""

# --- 1. Build ---
echo "=== Building ==="
cd "$PROJECT_DIR"
cargo build --release -p melin-server -p melin-bench -p melin-admin --quiet 2>&1
echo "  Build: OK"
echo ""

# --- 2. Auth keys ---
echo "=== Auth keys ==="
cd "$TMPDIR"
"$PROJECT_DIR/target/release/melin-keygen" bench trader
echo "trader $(cat bench.pub | tr -d '\n') bench" > authorized_keys
echo "  Generated bench.key + authorized_keys"
echo ""

# --- 3. Start server ---
echo "=== Starting server ==="
RUST_LOG=info,melin_server=debug \
"$PROJECT_DIR/target/release/melin-server" \
    --bind "$ADDR" \
    --journal "$TMPDIR/smoke.journal" \
    --authorized-keys "$TMPDIR/authorized_keys" \
    --standalone \
    --accounts 100 \
    --instruments 10 \
    > "$TMPDIR/server.log" 2>&1 &
SERVER_PID=$!
echo "  Server PID: $SERVER_PID"
echo "  Log: $TMPDIR/server.log"

# Wait for the server to start listening.
echo "  Waiting for server..."
WAIT=0
while ! timeout 1 bash -c "echo > /dev/tcp/127.0.0.1/$PORT" 2>/dev/null; do
    sleep 0.5
    WAIT=$((WAIT + 1))
    if [[ $WAIT -gt 20 ]]; then
        echo "  ERROR: Server not listening after 10s"
        echo "  --- Server log (last 30 lines) ---"
        tail -30 "$TMPDIR/server.log"
        exit 1
    fi
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "  ERROR: Server process died"
        echo "  --- Server log ---"
        cat "$TMPDIR/server.log"
        exit 1
    fi
done
echo "  Server ready"
echo ""

# --- 4. Run benchmark ---
echo "=== Running smoke benchmark ==="
echo "  1000 order pairs, 1 client, window 1 (single-order latency)"

"$PROJECT_DIR/target/release/melin-bench" \
    --addr "$ADDR" \
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
    echo "  SMOKE TEST: PASSED"
    echo "============================================================"
else
    echo "============================================================"
    echo "  SMOKE TEST: FAILED (bench exit code $BENCH_EXIT)"
    echo "============================================================"
    echo ""
    echo "  --- Server log (last 50 lines) ---"
    tail -50 "$TMPDIR/server.log"
    exit 1
fi
