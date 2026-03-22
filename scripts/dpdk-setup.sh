#!/usr/bin/env bash
# Set up DPDK with SR-IOV VFs on Intel E810 (bifurcated mode).
#
# Creates a VF on each bond member port, configures them for VLAN
# trading traffic, and binds them to vfio-pci for DPDK use. The bond
# and PFs are untouched — SSH/management traffic continues normally.
#
# Prerequisites:
#   - Intel E810 NICs with ice driver
#   - IOMMU enabled (intel_iommu=on iommu=pt in kernel cmdline)
#   - Root access
#
# Usage:
#   ./scripts/dpdk-setup.sh [--vlan 1461] [--ip 10.189.210.100/24]
#
# After running this script, start the server with:
#   ./target/release/melin-server \
#       --dpdk-port 0 \
#       --dpdk-ip <dpdk-ip>/24 \
#       --dpdk-gateway <gateway> \
#       --journal /mnt/journal/bench.journal \
#       --authorized-keys authorized_keys

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Bond member PCI addresses (from lspci). Update for your hardware.
PF0_PCI="${PF0_PCI:-0000:01:00.0}"
PF1_PCI="${PF1_PCI:-0000:01:00.1}"

# Bond member interface names. Update for your hardware.
PF0_IFACE="${PF0_IFACE:-enp1s0f0np0}"
PF1_IFACE="${PF1_IFACE:-enp1s0f1np1}"

# VLAN ID for trading traffic.
VLAN_ID="${VLAN_ID:-1461}"

# Dedicated IP for the DPDK interface. If not set, auto-derive from the
# bond VLAN IP by incrementing the last octet by 100. This avoids ARP
# conflicts between the kernel bond IP and the DPDK smoltcp IP.
DPDK_IP="${DPDK_IP:-auto}"

# Number of hugepages (2MB each). 1024 = 2GB, enough for DPDK mempool.
HUGEPAGES="${HUGEPAGES:-1024}"

# Parse CLI overrides.
while [[ $# -gt 0 ]]; do
    case "$1" in
        --vlan) VLAN_ID="$2"; shift 2 ;;
        --ip) DPDK_IP="$2"; shift 2 ;;
        *) echo "unknown option: $1" >&2; exit 1 ;;
    esac
done

# ---------------------------------------------------------------------------
# Auto-detect DPDK IP from bond VLAN interface
# ---------------------------------------------------------------------------
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

# ---------------------------------------------------------------------------
# Preflight checks
# ---------------------------------------------------------------------------

if [[ $EUID -ne 0 ]]; then
    echo "error: must run as root" >&2
    exit 1
fi

# Check IOMMU is enabled.
if ! dmesg | grep -qi "DMAR\|IOMMU"; then
    echo "warning: IOMMU may not be enabled. Add 'intel_iommu=on iommu=pt' to kernel cmdline."
fi

# Check ice driver is loaded.
if ! lsmod | grep -q "^ice "; then
    echo "error: ice driver not loaded (Intel E810 required)" >&2
    exit 1
fi

# Check SR-IOV support.
if [[ ! -f "/sys/bus/pci/devices/${PF0_PCI}/sriov_totalvfs" ]]; then
    echo "error: SR-IOV not available on ${PF0_PCI}." >&2
    echo "  The ice driver may lack CONFIG_PCI_IOV support." >&2
    echo "  Run cherry-setup.sh first — it installs Intel's out-of-tree ice driver." >&2
    exit 1
fi

echo "=== DPDK SR-IOV Setup for Intel E810 ==="
echo "  PF0: ${PF0_PCI} (${PF0_IFACE})"
echo "  PF1: ${PF1_PCI} (${PF1_IFACE})"
echo "  VLAN: ${VLAN_ID}"
echo "  DPDK IP: ${DPDK_IP}"
echo ""

# ---------------------------------------------------------------------------
# 1. Configure hugepages
# ---------------------------------------------------------------------------

echo "--- Configuring hugepages (${HUGEPAGES} x 2MB) ---"

echo "$HUGEPAGES" > /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages

# Mount 2MB hugetlbfs if not already mounted.
if ! mount | grep -q "pagesize=2M"; then
    mkdir -p /mnt/huge_2m
    mount -t hugetlbfs -o pagesize=2M nodev /mnt/huge_2m
    echo "  Mounted 2MB hugetlbfs at /mnt/huge_2m"
fi

ACTUAL=$(cat /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages)
echo "  Hugepages allocated: ${ACTUAL}"

if [[ "$ACTUAL" -lt "$HUGEPAGES" ]]; then
    echo "  warning: only ${ACTUAL}/${HUGEPAGES} hugepages allocated (memory fragmentation?)"
fi

# ---------------------------------------------------------------------------
# 2. Load vfio-pci module
# ---------------------------------------------------------------------------

echo ""
echo "--- Loading vfio-pci module ---"

modprobe vfio-pci
echo "  vfio-pci loaded"

# Enable unsafe IOMMU mode if IOMMU groups aren't properly isolated.
if [[ -f /sys/module/vfio/parameters/enable_unsafe_noiommu_mode ]]; then
    echo 1 > /sys/module/vfio/parameters/enable_unsafe_noiommu_mode 2>/dev/null || true
fi

# ---------------------------------------------------------------------------
# 3. Create VFs on both bond member ports
# ---------------------------------------------------------------------------

echo ""
echo "--- Creating SR-IOV VFs ---"

create_vf() {
    local pci="$1"
    local iface="$2"
    local label="$3"

    local current
    current=$(cat "/sys/bus/pci/devices/${pci}/sriov_numvfs" 2>/dev/null || echo 0)
    if [[ "$current" -gt 0 ]]; then
        echo "  ${label}: ${current} VFs already exist, skipping creation"
        return
    fi

    echo 1 > "/sys/bus/pci/devices/${pci}/sriov_numvfs"
    echo "  ${label}: created 1 VF"
    sleep 1

    # Trust VF 0 (required for DCF and flow rule offloading).
    ip link set "${iface}" vf 0 trust on
    echo "  ${label}: VF 0 trusted"

    # Assign VLAN to VF so it sees VLAN-tagged trading traffic.
    ip link set "${iface}" vf 0 vlan "${VLAN_ID}"
    echo "  ${label}: VF 0 assigned VLAN ${VLAN_ID}"

    # Disable spoofcheck (needed for smoltcp to use its own MAC).
    ip link set "${iface}" vf 0 spoofchk off
    echo "  ${label}: VF 0 spoofcheck disabled"
}

create_vf "${PF0_PCI}" "${PF0_IFACE}" "PF0"
create_vf "${PF1_PCI}" "${PF1_IFACE}" "PF1"

# ---------------------------------------------------------------------------
# 4. Bind VFs to vfio-pci
# ---------------------------------------------------------------------------

echo ""
echo "--- Binding VFs to vfio-pci ---"

bind_vf() {
    local pf_pci="$1"
    local label="$2"

    local vf_pci
    vf_pci=$(readlink -f "/sys/bus/pci/devices/${pf_pci}/virtfn0" | xargs basename)

    if [[ -z "$vf_pci" ]]; then
        echo "  ${label}: error: could not find VF PCI address" >&2
        return 1
    fi

    echo "  ${label}: VF at ${vf_pci}"

    # Unbind from current driver (iavf).
    if [[ -e "/sys/bus/pci/devices/${vf_pci}/driver" ]]; then
        echo "${vf_pci}" > "/sys/bus/pci/devices/${vf_pci}/driver/unbind" 2>/dev/null || true
        echo "  ${label}: unbound from kernel driver"
    fi

    local vendor device
    vendor=$(cat "/sys/bus/pci/devices/${vf_pci}/vendor")
    device=$(cat "/sys/bus/pci/devices/${vf_pci}/device")

    echo "${vendor} ${device}" > /sys/bus/pci/drivers/vfio-pci/new_id 2>/dev/null || true
    echo "${vf_pci}" > /sys/bus/pci/drivers/vfio-pci/bind 2>/dev/null || true
    echo "  ${label}: bound to vfio-pci"
}

bind_vf "${PF0_PCI}" "PF0-VF0"
bind_vf "${PF1_PCI}" "PF1-VF0"

# ---------------------------------------------------------------------------
# 5. Verify setup
# ---------------------------------------------------------------------------

echo ""
echo "--- Verification ---"

echo "  Hugepages:"
grep -i huge /proc/meminfo | head -3

echo ""
echo "  DPDK-bound devices:"
ls -la /sys/bus/pci/drivers/vfio-pci/ 2>/dev/null | grep "0000:" || echo "  (none found)"

VF0_PCI=$(readlink -f "/sys/bus/pci/devices/${PF0_PCI}/virtfn0" 2>/dev/null | xargs basename 2>/dev/null || echo "?")
VF1_PCI=$(readlink -f "/sys/bus/pci/devices/${PF1_PCI}/virtfn0" 2>/dev/null | xargs basename 2>/dev/null || echo "?")

# Save DPDK config for use by dpdk-lan-bench.sh.
DPDK_CONF="/etc/melin-dpdk.conf"
cat > "$DPDK_CONF" <<EOF
DPDK_IP=${DPDK_IP%%/*}
DPDK_PREFIX=${DPDK_IP##*/}
DPDK_PORT=0
HUGE_DIR=/mnt/huge_2m
EOF
echo "  Config written to ${DPDK_CONF}"

echo ""
echo "=== Setup complete ==="
echo ""
echo "  VF0 (on PF0): ${VF0_PCI} → vfio-pci"
echo "  VF1 (on PF1): ${VF1_PCI} → vfio-pci"
echo "  Bond: untouched (LACP active)"
echo "  Hugepages: ${ACTUAL} x 2MB"
echo ""
echo "  Start the server with:"
echo "    ./target/release/melin-server \\"
echo "      --dpdk-eal-args='--huge-dir=/mnt/huge_2m' \\"
echo "      --dpdk-port 0 \\"
echo "      --dpdk-ip ${DPDK_IP%%/*} \\"
echo "      --dpdk-prefix-len 24 \\"
echo "      --journal /mnt/journal/bench.journal \\"
echo "      --authorized-keys authorized_keys \\"
echo "      --standalone"
