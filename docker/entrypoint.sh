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

# --- Generate Ed25519 key pairs if not present ---
# Two keys: one for the human trader (TRADER session, account 1) and one
# for the optional order-flow bot (BOT session, accounts ≥2). Distinct
# keys are required so the bot's per-key request-seq namespace on the
# engine doesn't collide with the human's.

if [ ! -f "$DATA_DIR/trader.key" ]; then
    echo "Generating trader Ed25519 key..."
    melin-keygen trader operator
    echo "  trader.key created"
fi

if [ ! -f "$DATA_DIR/bot.key" ]; then
    echo "Generating bot Ed25519 key..."
    melin-keygen bot operator
    echo "  bot.key created"
fi

# Always rewrite authorized_keys so both keys are trusted even if one was
# already on disk from a previous image version.
TRADER_PUB=$(tr -d '\n' < trader.pub)
BOT_PUB=$(tr -d '\n' < bot.pub)
cat > authorized_keys <<EOF
trader $TRADER_PUB trader
trader $BOT_PUB bot
EOF
echo "$TRADER_PUB" > trader.pub.b64
echo "  authorized_keys updated (trader + bot)"

# --- Write oe-gateway config (always overwritten) ---
# Regenerated every start so schema/session changes from the image apply
# without the user having to wipe $DATA_DIR.

cat > "$DATA_DIR/oe-gateway.toml" <<TOML
server_addr = "127.0.0.1:9876"
listen_addr = "0.0.0.0:9000"
target_comp_id = "MELIN-OE"

[[session]]
sender_comp_id = "TRADER"
account_id = 1
key_path = "$DATA_DIR/trader.key"

[[session]]
sender_comp_id = "BOT"
account_id = 2
key_path = "$DATA_DIR/bot.key"

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
echo "  oe-gateway.toml written (TRADER + BOT sessions)"

# --- Write md-gateway config (always overwritten) ---
# Same rationale as oe-gateway.toml above: regenerated every start so
# schema/symbol changes from the image apply without the user having to
# wipe $DATA_DIR.

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
echo "  md-gateway.toml written"

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

# Wait for the server to accept connections on BOTH ports:
#   9876 — RPC (oe-gateway client)
#   9877 — event publisher (md-gateway subscriber)
# Otherwise md-gateway races the server's event-publisher bind and logs a
# spurious "MarketDataCore disconnected, reconnecting in 1s" warning.
for i in $(seq 1 50); do
    if nc -z 127.0.0.1 9876 2>/dev/null && nc -z 127.0.0.1 9877 2>/dev/null; then
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
