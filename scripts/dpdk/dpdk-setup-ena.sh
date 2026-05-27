#!/usr/bin/env bash
# Set up DPDK on AWS EC2 instances using the ENA (Elastic Network Adapter).
#
# Binds a secondary ENI to vfio-pci for DPDK kernel bypass. The primary
# ENI (SSH/management) is left untouched.
#
# EC2 guests don't expose IOMMU, so vfio-pci runs in no-IOMMU mode.
# This is acceptable for single-tenant benchmark instances.
#
# Prerequisites:
#   - A secondary ENI attached to the instance (device index 1+)
#   - Root access
#   - DPDK packages installed (libdpdk-dev, server-setup.sh handles this)
#
# Usage:
#   sudo ./scripts/dpdk/dpdk-setup-ena.sh --ip <dpdk-private-ip> [--prefix <cidr>]
#
# The script writes /etc/melin-dpdk.conf so the bench suite can discover
# the DPDK configuration without any AWS-specific logic.

set -euo pipefail

DPDK_IP=""
DPDK_PREFIX="24"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --ip)     DPDK_IP="$2"; shift 2 ;;
        --prefix) DPDK_PREFIX="$2"; shift 2 ;;
        -h|--help)
            sed -n '2,/^$/p' "$0" | sed 's/^# \?//'
            exit 0 ;;
        *) echo "error: unknown option: $1" >&2; exit 1 ;;
    esac
done

if [[ -z "$DPDK_IP" ]]; then
    echo "error: --ip is required (private IP of the secondary ENI)" >&2
    exit 1
fi

if [[ $EUID -ne 0 ]]; then
    echo "error: must run as root" >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# 1. Detect primary and secondary ENA interfaces
# ---------------------------------------------------------------------------

PRIMARY_IFACE=$(ip route show default | awk '{print $5}' | head -1)
if [[ -z "$PRIMARY_IFACE" ]]; then
    echo "error: could not determine primary interface" >&2
    exit 1
fi

DPDK_IFACE=""
DPDK_PCI=""
for dev_path in /sys/class/net/*/device/driver; do
    iface=$(echo "$dev_path" | cut -d/ -f5)
    driver=$(basename "$(readlink "$dev_path")")
    if [[ "$driver" == "ena" && "$iface" != "$PRIMARY_IFACE" ]]; then
        DPDK_IFACE="$iface"
        DPDK_PCI=$(basename "$(readlink "/sys/class/net/$iface/device")")
        break
    fi
done

# If no kernel-managed secondary ENA found, check if one is already bound
# to vfio-pci (idempotent re-run).
if [[ -z "$DPDK_IFACE" ]]; then
    vfio_count=$(ls -d /sys/bus/pci/drivers/vfio-pci/0000:* 2>/dev/null | wc -l)
    if [[ "$vfio_count" -gt 0 ]]; then
        echo "=== DPDK ENA already set up (vfio-pci device present) ==="
        cat /etc/melin-dpdk.conf 2>/dev/null || true
        exit 0
    fi
    echo "error: no secondary ENA interface found" >&2
    echo "  Primary: $PRIMARY_IFACE" >&2
    echo "  Attach a second ENI to this instance and try again." >&2
    exit 1
fi

echo "=== DPDK ENA Setup ==="
echo "  Primary:   $PRIMARY_IFACE (kernel — SSH/management)"
echo "  Secondary: $DPDK_IFACE ($DPDK_PCI) → DPDK"
echo "  DPDK IP:   $DPDK_IP/$DPDK_PREFIX"
echo ""

# ---------------------------------------------------------------------------
# 2. Load vfio-pci with no-IOMMU mode
# ---------------------------------------------------------------------------

echo "--- Loading vfio-pci (no-IOMMU mode) ---"
modprobe vfio-pci
echo 1 > /sys/module/vfio/parameters/enable_unsafe_noiommu_mode
echo "  vfio-pci loaded, no-IOMMU mode enabled"

# ---------------------------------------------------------------------------
# 3. Unbind secondary ENI from ena kernel driver
# ---------------------------------------------------------------------------

echo ""
echo "--- Unbinding $DPDK_IFACE from ena driver ---"
ip link set "$DPDK_IFACE" down 2>/dev/null || true
echo "$DPDK_PCI" > "/sys/bus/pci/devices/$DPDK_PCI/driver/unbind"
echo "  Unbound $DPDK_PCI from ena"

# ---------------------------------------------------------------------------
# 4. Bind to vfio-pci
# ---------------------------------------------------------------------------

echo ""
echo "--- Binding $DPDK_PCI to vfio-pci ---"
VENDOR=$(cat "/sys/bus/pci/devices/$DPDK_PCI/vendor")
DEVICE=$(cat "/sys/bus/pci/devices/$DPDK_PCI/device")
echo "$VENDOR $DEVICE" > /sys/bus/pci/drivers/vfio-pci/new_id 2>/dev/null || true
echo "$DPDK_PCI" > /sys/bus/pci/drivers/vfio-pci/bind 2>/dev/null || true

if [[ -e "/sys/bus/pci/drivers/vfio-pci/$DPDK_PCI" ]]; then
    echo "  Bound $DPDK_PCI to vfio-pci"
else
    echo "error: failed to bind $DPDK_PCI to vfio-pci" >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# 5. Hugepages
# ---------------------------------------------------------------------------

echo ""
echo "--- Hugepages ---"
HUGEPAGES=1024
echo "$HUGEPAGES" > /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages
mkdir -p /mnt/huge_2m
if ! mount | grep -q "/mnt/huge_2m"; then
    mount -t hugetlbfs -o pagesize=2M nodev /mnt/huge_2m
    echo "  Mounted hugetlbfs at /mnt/huge_2m"
fi
ACTUAL=$(cat /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages)
echo "  Hugepages: ${ACTUAL} x 2 MiB"

# ---------------------------------------------------------------------------
# 6. Write DPDK config
# ---------------------------------------------------------------------------

echo ""
echo "--- Writing /etc/melin-dpdk.conf ---"
cat > /etc/melin-dpdk.conf <<EOF
DPDK_IP=${DPDK_IP}
DPDK_PREFIX=${DPDK_PREFIX}
DPDK_PORT=0
DPDK_MODE=ena
HUGE_DIR=/mnt/huge_2m
EOF
echo "  Config written"

echo ""
echo "=== DPDK ENA setup complete ==="
echo "  $DPDK_PCI → vfio-pci (no-IOMMU)"
echo "  DPDK IP: $DPDK_IP/$DPDK_PREFIX"
