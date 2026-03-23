#!/usr/bin/env bash
# Set up DPDK with a dedicated NIC port (no SR-IOV).
#
# Removes one port from the LACP bond and binds it directly to DPDK
# via vfio-pci. This gives DPDK direct PF access — no SR-IOV VEB
# switching fabric overhead.
#
# The bond degrades to a single link (SSH/management still works, just
# half the bandwidth). Restore with dpdk-teardown-dedicated.sh or reboot.
#
# Prerequisites:
#   - LACP bond with 2+ member ports
#   - IOMMU enabled (iommu=pt in kernel cmdline)
#   - Root access
#
# Usage:
#   ./scripts/dpdk-setup-dedicated.sh [--vlan 1461] [--ip auto] [--port 1]
#
# After running this script, start the server with:
#   ./target/release/melin-server \
#       --dpdk-ports 0 \
#       --dpdk-ip <dpdk-ip> \
#       --dpdk-prefix-len 24 \
#       --journal /mnt/journal/bench.journal \
#       --authorized-keys authorized_keys \
#       --standalone

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Which bond member to remove and give to DPDK (0 = first, 1 = second).
# Default: 1 (keep the first port for SSH, give the second to DPDK).
BOND_PORT_INDEX="${BOND_PORT_INDEX:-1}"

# VLAN ID for trading traffic. Auto-detect from bond VLAN interface if not set.
VLAN_ID=""

# Dedicated IP for the DPDK interface.
DPDK_IP="${DPDK_IP:-auto}"

# Number of hugepages (2MB each). 1024 = 2GB, enough for DPDK mempool.
HUGEPAGES="${HUGEPAGES:-1024}"

# MTU for the DPDK interface. 1500 = standard (Cherry switches don't
# support jumbo). Use 9000 if the switch supports it.
MTU="${MTU:-1500}"

# Parse CLI overrides.
while [[ $# -gt 0 ]]; do
    case "$1" in
        --vlan) VLAN_ID="$2"; shift 2 ;;
        --ip) DPDK_IP="$2"; shift 2 ;;
        --port) BOND_PORT_INDEX="$2"; shift 2 ;;
        --mtu) MTU="$2"; shift 2 ;;
        *) echo "unknown option: $1" >&2; exit 1 ;;
    esac
done

# ---------------------------------------------------------------------------
# Preflight checks
# ---------------------------------------------------------------------------

if [[ $EUID -ne 0 ]]; then
    echo "error: must run as root" >&2
    exit 1
fi

# Cargo's built-in libgit2 can't authenticate private git SSH deps.
# The env var is more reliable than git config under sudo/root.
if ! grep -q "CARGO_NET_GIT_FETCH_WITH_CLI" /etc/environment 2>/dev/null; then
    echo 'CARGO_NET_GIT_FETCH_WITH_CLI=true' >> /etc/environment
    echo "  Added CARGO_NET_GIT_FETCH_WITH_CLI=true to /etc/environment"
fi
export CARGO_NET_GIT_FETCH_WITH_CLI=true

# ---------------------------------------------------------------------------
# Auto-detect bond members
# ---------------------------------------------------------------------------

BOND_SLAVES=$(cat /sys/class/net/bond0/bonding/slaves 2>/dev/null || true)
if [[ -z "$BOND_SLAVES" ]]; then
    echo "error: no bond0 found" >&2
    exit 1
fi

# Split slaves into array.
read -ra SLAVE_ARRAY <<< "$BOND_SLAVES"
if [[ ${#SLAVE_ARRAY[@]} -lt 2 ]]; then
    echo "error: bond0 has fewer than 2 slaves — can't remove one" >&2
    exit 1
fi

DPDK_IFACE="${SLAVE_ARRAY[$BOND_PORT_INDEX]}"
KEEP_IFACE="${SLAVE_ARRAY[$((1 - BOND_PORT_INDEX))]}"
DPDK_PCI=$(ethtool -i "$DPDK_IFACE" 2>/dev/null | grep bus-info | awk '{print $2}')

if [[ -z "$DPDK_PCI" ]]; then
    echo "error: could not determine PCI address for $DPDK_IFACE" >&2
    exit 1
fi

# Auto-detect VLAN ID.
if [[ -z "$VLAN_ID" ]]; then
    VLAN_IFACE=$(ip -o link show | grep "bond0\." | head -1 | awk -F'[ :@]+' '{print $2}')
    if [[ -n "$VLAN_IFACE" ]]; then
        VLAN_ID="${VLAN_IFACE##*.}"
    else
        echo "error: could not auto-detect VLAN ID" >&2
        exit 1
    fi
fi

# Auto-detect DPDK IP.
if [[ "$DPDK_IP" == "auto" ]]; then
    VLAN_IFACE="bond0.${VLAN_ID}"
    BOND_IP=$(ip -4 addr show "$VLAN_IFACE" 2>/dev/null | grep -oP 'inet \K[\d.]+')
    BOND_PREFIX=$(ip -4 addr show "$VLAN_IFACE" 2>/dev/null | grep -oP 'inet [\d.]+/\K\d+')

    if [[ -z "$BOND_IP" ]]; then
        echo "error: could not detect IP on $VLAN_IFACE — set DPDK_IP manually" >&2
        exit 1
    fi

    # Increment the last octet by 100 (wrap at 255).
    IFS='.' read -r a b c d <<< "$BOND_IP"
    DPDK_LAST=$(( (d + 100) % 256 ))
    if [[ "$DPDK_LAST" -eq "$d" ]]; then
        DPDK_LAST=$(( (d + 101) % 256 ))
    fi
    DPDK_IP="${a}.${b}.${c}.${DPDK_LAST}/${BOND_PREFIX}"

    echo "  Bond VLAN IP: ${BOND_IP}/${BOND_PREFIX} (${VLAN_IFACE})"
    echo "  DPDK IP:      ${DPDK_IP} (auto-derived)"
fi

echo "=== DPDK Dedicated NIC Setup ==="
echo "  Removing: ${DPDK_IFACE} (${DPDK_PCI}) from bond0"
echo "  Keeping:  ${KEEP_IFACE} in bond0 for SSH/management"
echo "  VLAN:     ${VLAN_ID}"
echo "  DPDK IP:  ${DPDK_IP}"
echo "  MTU:      ${MTU}"
echo ""

# ---------------------------------------------------------------------------
# 1. Configure hugepages
# ---------------------------------------------------------------------------

echo "--- Configuring hugepages (${HUGEPAGES} x 2MB) ---"

echo "$HUGEPAGES" > /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages

mkdir -p /mnt/huge_2m
if ! mount | grep -q "/mnt/huge_2m"; then
    mount -t hugetlbfs -o pagesize=2M nodev /mnt/huge_2m
    echo "  Mounted 2MB hugetlbfs at /mnt/huge_2m"
fi
if ! grep -q "/mnt/huge_2m" /etc/fstab 2>/dev/null; then
    echo "nodev /mnt/huge_2m hugetlbfs pagesize=2M 0 0" >> /etc/fstab
    echo "  Added /mnt/huge_2m to /etc/fstab"
fi

ACTUAL=$(cat /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages)
echo "  Hugepages allocated: ${ACTUAL}"

# ---------------------------------------------------------------------------
# 2. Load vfio-pci module
# ---------------------------------------------------------------------------

echo ""
echo "--- Loading vfio-pci module ---"

modprobe vfio-pci
echo "  vfio-pci loaded"

if [[ -f /sys/module/vfio/parameters/enable_unsafe_noiommu_mode ]]; then
    echo 1 > /sys/module/vfio/parameters/enable_unsafe_noiommu_mode 2>/dev/null || true
fi

# ---------------------------------------------------------------------------
# 3. Remove port from bond and bind to DPDK
# ---------------------------------------------------------------------------

echo ""
echo "--- Removing ${DPDK_IFACE} from bond0 ---"

# Set MTU on the port before removing (some drivers need this while up).
ip link set "$DPDK_IFACE" mtu "$MTU" 2>/dev/null || true

# Remove from bond.
echo "-${DPDK_IFACE}" > /sys/class/net/bond0/bonding/slaves 2>/dev/null || \
    ip link set "$DPDK_IFACE" nomaster 2>/dev/null || true
echo "  Removed ${DPDK_IFACE} from bond0"

# Brief wait for bond to stabilize.
sleep 1

# Verify bond still has a member.
REMAINING=$(cat /sys/class/net/bond0/bonding/slaves 2>/dev/null || true)
if [[ -z "$REMAINING" ]]; then
    echo "  ERROR: bond0 has no remaining slaves! SSH may be lost." >&2
    echo "  Attempting to restore ${DPDK_IFACE} to bond..." >&2
    ip link set "$DPDK_IFACE" master bond0 2>/dev/null || true
    exit 1
fi
echo "  Bond remaining slaves: ${REMAINING}"

echo ""
echo "--- Binding ${DPDK_IFACE} (${DPDK_PCI}) to vfio-pci ---"

# Bring the interface down.
ip link set "$DPDK_IFACE" down 2>/dev/null || true

# Unbind from kernel driver.
if [[ -e "/sys/bus/pci/devices/${DPDK_PCI}/driver" ]]; then
    echo "${DPDK_PCI}" > "/sys/bus/pci/devices/${DPDK_PCI}/driver/unbind" 2>/dev/null || true
    echo "  Unbound from kernel driver"
fi

# Bind to vfio-pci.
local_vendor=$(cat "/sys/bus/pci/devices/${DPDK_PCI}/vendor")
local_device=$(cat "/sys/bus/pci/devices/${DPDK_PCI}/device")
echo "${local_vendor} ${local_device}" > /sys/bus/pci/drivers/vfio-pci/new_id 2>/dev/null || true
echo "${DPDK_PCI}" > /sys/bus/pci/drivers/vfio-pci/bind 2>/dev/null || true
echo "  Bound ${DPDK_PCI} to vfio-pci"

# ---------------------------------------------------------------------------
# 4. Verify and save config
# ---------------------------------------------------------------------------

echo ""
echo "--- Verification ---"

echo "  Hugepages:"
grep -i huge /proc/meminfo | head -3

echo ""
echo "  DPDK-bound devices:"
ls -la /sys/bus/pci/drivers/vfio-pci/ 2>/dev/null | grep "0000:" || echo "  (none found)"

echo ""
echo "  Bond status:"
cat /sys/class/net/bond0/bonding/slaves 2>/dev/null || echo "  (no bond)"

# Save DPDK config.
DPDK_CONF="/etc/melin-dpdk.conf"
cat > "$DPDK_CONF" <<EOF
DPDK_IP=${DPDK_IP%%/*}
DPDK_PREFIX=${DPDK_IP##*/}
DPDK_PORT=0
DPDK_PCI=${DPDK_PCI}
DPDK_MODE=dedicated
HUGE_DIR=/mnt/huge_2m
MTU=${MTU}
REMOVED_IFACE=${DPDK_IFACE}
EOF
echo "  Config written to ${DPDK_CONF}"

# Save restore info for teardown.
echo "${DPDK_IFACE}" > /tmp/.dpdk-dedicated-iface

echo ""
echo "=== Setup complete ==="
echo ""
echo "  ${DPDK_IFACE} (${DPDK_PCI}) → vfio-pci (direct PF, no SR-IOV)"
echo "  ${KEEP_IFACE} → bond0 (SSH/management, single link)"
echo "  Hugepages: ${ACTUAL} x 2MB"
echo ""
echo "  Start the server with:"
echo "    ./target/release/melin-server \\"
echo "      --dpdk-eal-args='--huge-dir=/mnt/huge_2m' \\"
echo "      --dpdk-ports 0 \\"
echo "      --dpdk-ip ${DPDK_IP%%/*} \\"
echo "      --dpdk-prefix-len ${DPDK_IP##*/} \\"
echo "      --dpdk-mtu ${MTU} \\"
echo "      --journal /mnt/journal/bench.journal \\"
echo "      --authorized-keys authorized_keys \\"
echo "      --standalone --busy-spin"
echo ""
echo "  To restore the bond: reboot, or run:"
echo "    ip link set ${DPDK_IFACE} master bond0"
