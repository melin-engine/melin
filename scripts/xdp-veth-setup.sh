#!/usr/bin/env bash
# Create a veth pair for AF_XDP testing.
#
# Creates veth0 (bench client side) and veth1 (AF_XDP server side),
# assigns IP addresses, and brings them up. Run with sudo.
#
# Usage:
#   sudo ./scripts/xdp-veth-setup.sh       # create
#   sudo ./scripts/xdp-veth-setup.sh down   # tear down

set -euo pipefail

VETH0="xdp-bench"
VETH1="xdp-engine"
IP0="10.99.0.1/24"
IP1="10.99.0.2/24"

if [[ "${1:-}" == "down" ]]; then
    echo "Tearing down veth pair..."
    ip link del "$VETH0" 2>/dev/null || true
    echo "Done."
    exit 0
fi

echo "Creating veth pair: $VETH0 <-> $VETH1"
ip link add "$VETH0" type veth peer name "$VETH1"

echo "Assigning IPs: $VETH0=$IP0, $VETH1=$IP1"
ip addr add "$IP0" dev "$VETH0"
ip addr add "$IP1" dev "$VETH1"

echo "Bringing interfaces up..."
ip link set "$VETH0" up
ip link set "$VETH1" up

# Disable checksum offload — veth + XDP sometimes has issues with it.
ethtool -K "$VETH0" tx off rx off 2>/dev/null || true
ethtool -K "$VETH1" tx off rx off 2>/dev/null || true

# Verify XDP support on veth1 (the server side).
echo ""
echo "Interface status:"
ip -brief addr show "$VETH0"
ip -brief addr show "$VETH1"

echo ""
echo "Ready. Server binds AF_XDP on $VETH1 (10.99.0.2)."
echo "Bench client sends UDP to 10.99.0.2:9876."
echo ""
echo "  Server:  sudo target/release/melin-server --xdp-interface $VETH1 --bind 10.99.0.2:9876 --journal /tmp/xdp-test.journal --accounts 10 --instruments 2"
echo "  Bench:   target/release/melin-bench --addr 10.99.0.2:9876 --warmup-duration 2s --duration 10s --accounts 10 --instruments 2"
echo ""
echo "Tear down: sudo $0 down"
