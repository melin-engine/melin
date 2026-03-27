#!/usr/bin/env bash
# Quick-start script: builds everything, generates a key, starts the server,
# and launches the admin TUI. Cleans up on exit.
set -euo pipefail

TMPDIR=$(mktemp -d)
trap 'kill $SERVER_PID 2>/dev/null; rm -rf "$TMPDIR"' EXIT

echo "==> Building..."
cargo build --bin melin-server --bin melin-keygen --bin melin-admin --quiet

echo "==> Generating keypair..."
cd "$TMPDIR"
cargo run --manifest-path "$OLDPWD/Cargo.toml" --bin melin-keygen --quiet -- admin operator
# Extract the authorized_keys line from keygen output and write to file.
echo "operator $(cat admin.pub | tr -d '\n') admin" > authorized_keys

echo "==> Starting server..."
cargo run --manifest-path "$OLDPWD/Cargo.toml" --bin melin-server --quiet -- \
    --authorized-keys "$TMPDIR/authorized_keys" \
    --journal "$TMPDIR/demo.journal" &
SERVER_PID=$!
sleep 0.5

echo "==> Launching admin TUI (Esc to quit)..."
cargo run --manifest-path "$OLDPWD/Cargo.toml" --bin melin-admin --quiet -- \
    127.0.0.1:9876 "$TMPDIR/admin.key"
