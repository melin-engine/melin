#!/usr/bin/env bash
# Start the melin server with DPDK transport.
#
# Auto-detects DPDK IP from /etc/melin-dpdk.conf (written by
# dpdk-setup-sriov.sh) or derives it from the bond VLAN interface.
# Falls back to TAP mode if no SR-IOV config is found.
#
# Usage:
#   sudo ./scripts/dpdk-server.sh [extra server args...]
#
# Examples:
#   sudo ./scripts/dpdk-server.sh
#   sudo ./scripts/dpdk-server.sh --max-journal-mib 512
#   DPDK_IP=10.0.0.50 sudo ./scripts/dpdk-server.sh

set -euo pipefail

if [[ $EUID -ne 0 ]]; then
    echo "error: must run as root (hugepages + DPDK)" >&2
    echo "usage: sudo $0 [extra server args...]" >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Ensure cargo is available under sudo.
if [[ -n "${SUDO_USER:-}" ]]; then
    REAL_HOME=$(eval echo "~$SUDO_USER")
    export PATH="$REAL_HOME/.cargo/bin:$PATH"
    export RUSTUP_HOME="${RUSTUP_HOME:-$REAL_HOME/.rustup}"
    export CARGO_HOME="${CARGO_HOME:-$REAL_HOME/.cargo}"
fi

# ---------------------------------------------------------------------------
# Load config
# ---------------------------------------------------------------------------
CONF="/etc/melin-dpdk.conf"
HUGE_DIR="/mnt/huge_2m"
DPDK_PORT="${DPDK_PORT:-0}"
DPDK_CORE="${DPDK_CORE:-8}"
DPDK_PREFIX="${DPDK_PREFIX:-24}"
MTU="${MTU:-1500}"
JOURNAL="${JOURNAL:-/mnt/journal/bench.journal}"
AUTH_KEYS="${AUTH_KEYS:-$PROJECT_DIR/authorized_keys}"

if [[ -f "$CONF" ]]; then
    source "$CONF"
    echo "  Loaded config from $CONF"
fi

# Auto-detect DPDK IP if not set.
if [[ -z "${DPDK_IP:-}" ]]; then
    # Try bond VLAN interfaces.
    for iface in bond0.* eno*.* eth0.*; do
        BOND_IP=$(ip -4 addr show "$iface" 2>/dev/null | grep -oP 'inet \K[\d.]+' || true)
        if [[ -n "$BOND_IP" ]]; then
            BOND_PREFIX=$(ip -4 addr show "$iface" 2>/dev/null | grep -oP 'inet [\d.]+/\K\d+')
            IFS='.' read -r a b c d <<< "$BOND_IP"
            DPDK_LAST=$(( (d + 100) % 256 ))
            [[ "$DPDK_LAST" -eq "$d" ]] && DPDK_LAST=$(( (d + 101) % 256 ))
            DPDK_IP="${a}.${b}.${c}.${DPDK_LAST}"
            DPDK_PREFIX="${BOND_PREFIX:-24}"
            echo "  Auto-detected DPDK IP: $DPDK_IP/$DPDK_PREFIX (from $iface: $BOND_IP)"
            break
        fi
    done
fi

if [[ -z "${DPDK_IP:-}" ]]; then
    echo "error: could not detect DPDK IP" >&2
    echo "  Set DPDK_IP=... or run dpdk-setup-sriov.sh first" >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# Hugepages
# ---------------------------------------------------------------------------
HUGEPAGE_COUNT=$(cat /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages 2>/dev/null || echo 0)
if [[ "$HUGEPAGE_COUNT" -lt 256 ]]; then
    echo "  Allocating 256 x 2MB hugepages..."
    echo 256 > /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages
fi

if ! mount | grep -q "pagesize=2M"; then
    mkdir -p "$HUGE_DIR"
    mount -t hugetlbfs -o pagesize=2M nodev "$HUGE_DIR"
    echo "  Mounted 2MB hugetlbfs at $HUGE_DIR"
fi

# ---------------------------------------------------------------------------
# Detect SR-IOV vs TAP mode
# ---------------------------------------------------------------------------
EAL_ARGS="${DPDK_EAL_ARGS:-}"
DPDK_PORTS_ARG=""
DPDK_VLAN_ARG=""
DPDK_MTU_ARG=""

if ls /sys/bus/pci/drivers/vfio-pci/0000:* &>/dev/null; then
    # SR-IOV mode — VFs are bound to vfio-pci. Use both ports for LACP
    # bonds so traffic arriving on either bond member is received.
    MODE="SR-IOV"
    VF_COUNT=$(ls -d /sys/bus/pci/drivers/vfio-pci/0000:* 2>/dev/null | wc -l)
    EAL_ARGS="${EAL_ARGS:+$EAL_ARGS }--huge-dir=$HUGE_DIR"
    if [[ "$VF_COUNT" -ge 2 ]]; then
        DPDK_PORTS_ARG="--dpdk-ports 0,1"
    else
        DPDK_PORTS_ARG="--dpdk-ports $DPDK_PORT"
    fi
    if [[ -n "${VLAN_ID:-}" ]]; then
        DPDK_VLAN_ARG="--dpdk-vlan $VLAN_ID"
    fi
    if [[ "$MTU" != "1500" ]]; then
        DPDK_MTU_ARG="--dpdk-mtu $MTU"
    fi
else
    # TAP mode — no SR-IOV, use virtual TAP device.
    MODE="TAP"
    EAL_ARGS="--vdev=net_tap0 --no-pci --huge-dir=$HUGE_DIR"
fi

# ---------------------------------------------------------------------------
# Start
# ---------------------------------------------------------------------------
echo ""
echo "============================================================"
echo "  Melin DPDK Server ($MODE)"
echo "  DPDK IP:  $DPDK_IP/$DPDK_PREFIX"
echo "  Core:     $DPDK_CORE"
echo "  Journal:  $JOURNAL"
echo "  Auth:     $AUTH_KEYS"
echo "============================================================"
echo ""

# Clean old journal to avoid replaying stale state.
rm -f "$JOURNAL"*

cd "$PROJECT_DIR"
exec cargo run --release -p melin-server --features dpdk --no-default-features -- \
    --bind 0.0.0.0:9876 \
    --journal "$JOURNAL" \
    --authorized-keys "$AUTH_KEYS" \
    --standalone \
    --accounts 100 \
    --instruments 10 \
    --dpdk-eal-args="$EAL_ARGS" \
    --dpdk-ip "$DPDK_IP" \
    --dpdk-prefix-len "$DPDK_PREFIX" \
    --dpdk-core "$DPDK_CORE" \
    $DPDK_PORTS_ARG \
    $DPDK_VLAN_ARG \
    $DPDK_MTU_ARG \
    "$@"
