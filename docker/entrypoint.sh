#!/usr/bin/env bash
# Entrypoint for the melin all-in-one Docker image.
# Starts melin-server, melin-oe-gateway, and melin-md-gateway.
#
# Ports exposed:
#   9000 — oe-gateway (FIX 4.4 order entry)
#   9001 — md-gateway (FIX 4.4 market data)
#
# Connect the TUI:
#   melin-tui-fix-client --oe-addr <host>:9000 --md-addr <host>:9001 \
#     --sender TRADER --oe-target MELIN-OE --md-target MELIN-MD

set -euo pipefail

DATA_DIR="${DATA_DIR:-/data}"
mkdir -p "$DATA_DIR"
cd "$DATA_DIR"

# --- Generate Ed25519 key pair if not present ---

if [ ! -f "$DATA_DIR/trader.key" ]; then
    echo "Generating Ed25519 key pair..."
    melin-keygen trader operator
    # keygen also writes trader.pub with base64 pubkey
    PUBKEY=$(cat trader.pub | tr -d '\n')
    echo "operator $PUBKEY trader" > authorized_keys
    # Also write a .pub.b64 for the md-gateway core (not yet signing)
    echo "$PUBKEY" > trader.pub.b64
    echo "  trader.key + authorized_keys created"
fi

# --- Write oe-gateway config if not present ---

if [ ! -f "$DATA_DIR/oe-gateway.toml" ]; then
    cat > "$DATA_DIR/oe-gateway.toml" <<TOML
server_addr = "127.0.0.1:9876"
listen_addr = "0.0.0.0:9000"
target_comp_id = "MELIN-OE"

[[session]]
sender_comp_id = "TRADER"
account_id = 1
key_path = "$DATA_DIR/trader.key"

[[symbol]]
fix_symbol = "BTC/USD"
melin_symbol = 0
tick_size_inverse = 100
lot_size_inverse = 1

[[symbol]]
fix_symbol = "ETH/USD"
melin_symbol = 1
tick_size_inverse = 100
lot_size_inverse = 1
TOML
    echo "  oe-gateway.toml created"
fi

# --- Write md-gateway config if not present ---

if [ ! -f "$DATA_DIR/md-gateway.toml" ]; then
    cat > "$DATA_DIR/md-gateway.toml" <<TOML
listen = "0.0.0.0:9001"
event_publisher = "127.0.0.1:9877"
authorized_keys = "$DATA_DIR/authorized_keys"
subscriber_key = "$DATA_DIR/trader.key"
sender_comp_id = "MELIN-MD"

[symbols."BTC/USD"]
id = 0
tick_inverse = 100
lot_inverse = 1
base_ccy = "BTC"
quote_ccy = "USD"

[symbols."ETH/USD"]
id = 1
tick_inverse = 100
lot_inverse = 1
base_ccy = "ETH"
quote_ccy = "USD"
TOML
    echo "  md-gateway.toml created"
fi

# --- Start processes ---

cleanup() {
    echo "Shutting down..."
    kill "$SERVER_PID" "$OE_PID" "$MD_PID" 2>/dev/null || true
    wait 2>/dev/null
}
trap cleanup EXIT INT TERM

echo "Starting melin-server..."
melin-server \
    --bind 127.0.0.1:9876 \
    --standalone \
    --authorized-keys "$DATA_DIR/authorized_keys" \
    --journal "$DATA_DIR/melin.journal" \
    --event-bind 127.0.0.1:9877 \
    --yield-idle \
    --cores 0,0,0,0,0,0,0,0 \
    --accounts 1000 \
    --instruments 2 \
    &
SERVER_PID=$!

# Wait for the server to accept connections.
for i in $(seq 1 50); do
    if nc -z 127.0.0.1 9876 2>/dev/null; then
        echo "  melin-server ready"
        break
    fi
    sleep 0.1
done

echo "Starting oe-gateway..."
melin-oe-gateway --config "$DATA_DIR/oe-gateway.toml" &
OE_PID=$!

echo "Starting md-gateway..."
melin-md-gateway --config "$DATA_DIR/md-gateway.toml" &
MD_PID=$!

sleep 0.5

echo ""
echo "=========================================="
echo "  Melin exchange stack running"
echo ""
echo "  OE gateway: 0.0.0.0:9000 (MELIN-OE)"
echo "  MD gateway: 0.0.0.0:9001 (MELIN-MD)"
echo ""
echo "  Connect TUI:"
echo "    melin-tui-fix-client \\"
echo "      --oe-addr localhost:9000 \\"
echo "      --md-addr localhost:9001 \\"
echo "      --sender TRADER \\"
echo "      --oe-target MELIN-OE \\"
echo "      --md-target MELIN-MD"
echo "=========================================="
echo ""

# Wait for any child to exit.
wait -n "$SERVER_PID" "$OE_PID" "$MD_PID" 2>/dev/null || true
