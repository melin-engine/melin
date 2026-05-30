#!/usr/bin/env bash
# Configure DPDK on Intel or Mellanox NICs. Auto-detects topology:
#
#   sriov-bond  Intel E810/i40e/ixgbe behind an LACP bond. Creates one
#               SR-IOV VF on each bond member port and binds the VFs to
#               vfio-pci. The bond and PFs are untouched — SSH/management
#               traffic continues normally.
#
#   mlx5        Mellanox ConnectX-{5,6,7} on a raw (non-bonded) port. Uses
#               the bifurcated PMD: the kernel keeps the netdev and DPDK
#               attaches via verbs on the same PF. No SR-IOV, no vfio-pci.
#               The script refuses to use the port that carries the default
#               route so SSH stays alive.
#
# Supported NICs:
#   - Intel E810 (ice / iavf VF)        — sriov-bond path
#   - Intel X710 / XL710 (i40e / iavf)  — sriov-bond path
#   - Intel 82599 / X520 / X540 / X550  — sriov-bond path (ixgbe / ixgbevf)
#   - Mellanox ConnectX-{5,6,7}         — mlx5 bifurcated path
#
# Prerequisites:
#   - IOMMU enabled (intel_iommu=on iommu=pt or amd_iommu=on iommu=pt) —
#     sriov-bond path only; mlx5 bifurcated mode doesn't need IOMMU
#     because it never rebinds to vfio-pci.
#   - Root access.
#
# Env vars (mlx5 path):
#   DPDK_NIC=<iface>   netdev to hand to DPDK (e.g. enp193s0f1np1).
#                      Optional — defaults to the first mlx5 port that
#                      doesn't carry the default route.
#   DPDK_IP=<ip/cidr>  IP for the DPDK transport. Required: there is no
#                      bond VLAN to derive from in this mode.
#
# Usage (sriov-bond):
#   ./scripts/dpdk/dpdk-setup.sh [--vlan 1461] [--ip 10.189.210.100/24]
#
# Usage (mlx5):
#   DPDK_IP=10.0.0.10/24 ./scripts/dpdk/dpdk-setup.sh
#   DPDK_IP=10.0.0.10/24 DPDK_NIC=enp193s0f1np1 ./scripts/dpdk/dpdk-setup.sh
#
# After running this script, start the server with:
#   sudo ./scripts/dpdk/dpdk-server.sh

set -euo pipefail

# ---------------------------------------------------------------------------
# Shared defaults + CLI parsing
# ---------------------------------------------------------------------------

# Number of hugepages (2MB each). 1024 = 2GB, enough for the DPDK mempool.
HUGEPAGES="${HUGEPAGES:-1024}"

# MTU for trading interfaces. 1500 = standard Ethernet (default).
# Use 9000 for jumbo frames if the switch supports it (many VLAN
# switches do NOT — test before deploying).
MTU="${MTU:-1500}"

# Parse CLI overrides. --vlan is only meaningful in the sriov-bond path;
# --nic is only meaningful in the mlx5 path. Each path ignores the other's
# flag silently rather than rejecting it — keeps the wrapper interface
# uniform.
while [[ $# -gt 0 ]]; do
    case "$1" in
        --vlan) VLAN_ID="$2"; shift 2 ;;
        --ip)   DPDK_IP="$2"; shift 2 ;;
        --mtu)  MTU="$2"; shift 2 ;;
        --nic)  DPDK_NIC="$2"; shift 2 ;;
        *) echo "unknown option: $1" >&2; exit 1 ;;
    esac
done

if [[ $EUID -ne 0 ]]; then
    echo "error: must run as root" >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

# Allocate 2MB hugepages and mount the hugetlbfs at /mnt/huge_2m. Shared
# by both setup paths — DPDK's mempool wants 2MB pages regardless of how
# the PMD attaches.
setup_hugepages() {
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

    local actual
    actual=$(cat /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages)
    echo "  Hugepages allocated: ${actual}"
    if [[ "$actual" -lt "$HUGEPAGES" ]]; then
        echo "  warning: only ${actual}/${HUGEPAGES} hugepages allocated (memory fragmentation?)"
    fi
}

# Read the interface that currently carries the IPv4 default route. Used
# by the mlx5 path to avoid stealing the SSH/management port.
default_route_iface() {
    ip -4 route show default 2>/dev/null \
        | head -1 \
        | awk '{for (i = 1; i <= NF; i++) if ($i == "dev") print $(i+1)}'
}

# Detect topology: which setup path applies.
detect_mode() {
    # LACP bond present with members → SR-IOV path (cherry-fetch shape).
    if [[ -e /sys/class/net/bond0/bonding/slaves ]] \
       && [[ -s /sys/class/net/bond0/bonding/slaves ]]; then
        echo "sriov-bond"
        return
    fi
    # Mellanox driver present on any netdev → bifurcated PMD.
    for drv_link in /sys/class/net/*/device/driver; do
        local drv
        drv=$(basename "$(readlink "$drv_link" 2>/dev/null)" 2>/dev/null)
        if [[ "$drv" == "mlx5_core" ]]; then
            echo "mlx5"
            return
        fi
    done
    echo "unknown"
}

# ---------------------------------------------------------------------------
# mlx5 bifurcated path
# ---------------------------------------------------------------------------
run_mlx5_setup() {
    local def_iface
    def_iface=$(default_route_iface)

    # Auto-pick the first mlx5 netdev that isn't carrying the default
    # route, unless the operator overrode with DPDK_NIC. Cheapest way to
    # avoid stealing the SSH port.
    if [[ -z "${DPDK_NIC:-}" ]]; then
        for drv_link in /sys/class/net/*/device/driver; do
            local drv ifname
            drv=$(basename "$(readlink "$drv_link" 2>/dev/null)" 2>/dev/null)
            [[ "$drv" == "mlx5_core" ]] || continue
            ifname=$(basename "$(dirname "$(dirname "$drv_link")")")
            if [[ "$ifname" != "$def_iface" ]]; then
                DPDK_NIC="$ifname"
                break
            fi
        done
    fi
    if [[ -z "${DPDK_NIC:-}" ]]; then
        echo "error: no free mlx5 port found (all mlx5 ports carry the default route?)" >&2
        echo "  Override with DPDK_NIC=<iface>." >&2
        exit 1
    fi

    if [[ "$DPDK_NIC" == "$def_iface" ]]; then
        echo "error: $DPDK_NIC carries the default route; refusing to bind it to DPDK" >&2
        echo "  Handing it to DPDK would break management connectivity." >&2
        echo "  Pick a different port (DPDK_NIC=<other-iface>)." >&2
        exit 1
    fi

    if [[ -z "${DPDK_IP:-}" || "$DPDK_IP" == "auto" ]]; then
        echo "error: DPDK_IP=<addr/cidr> is required in mlx5 mode" >&2
        echo "  No bond VLAN to derive from — supply it explicitly." >&2
        echo "  example: DPDK_IP=10.0.0.10/24 sudo $0" >&2
        exit 1
    fi

    local pci mac
    pci=$(ethtool -i "$DPDK_NIC" 2>/dev/null | awk '/^bus-info:/{print $2}')
    if [[ -z "$pci" ]]; then
        echo "error: could not read PCI address for $DPDK_NIC" >&2
        exit 1
    fi
    # The kernel netdev keeps its MAC — bifurcated mode shares the
    # address. smoltcp on the DPDK side advertises the same MAC for ARP.
    mac=$(cat "/sys/class/net/${DPDK_NIC}/address")

    echo "=== DPDK Mellanox bifurcated mode ==="
    echo "  NIC:     ${DPDK_NIC} (default route on ${def_iface:-<none>}, untouched)"
    echo "  PCI:     ${pci}"
    echo "  DPDK IP: ${DPDK_IP}"
    echo "  MAC:     ${mac} (kernel netdev keeps ownership)"
    echo "  MTU:     ${MTU}"
    echo ""

    setup_hugepages

    # Bring the port up and set MTU. mlx5's bifurcated PMD reads link
    # state from the kernel; a down netdev means DPDK sees link-down.
    ip link set "${DPDK_NIC}" mtu "${MTU}"
    ip link set "${DPDK_NIC}" up

    # No vfio-pci rebind. The netdev stays bound to mlx5_core; the DPDK
    # PMD attaches via the ib_uverbs / rdma-core path and shares the
    # device with the kernel.

    # ---------------------------------------------------------------------
    # Write conf
    # ---------------------------------------------------------------------
    local conf="/etc/melin-dpdk.conf"
    # EAL allowlists the DPDK port by PCI address so the mlx5 PMD picks
    # up just that device and leaves any other Mellanox ports (e.g. the
    # SSH port on a dual-port card) untouched.
    # Multi-word values are double-quoted so `source` / `eval` consumers
    # (dpdk-server.sh, dpdk-test.sh, dpdk-lan-bench.sh) don't misparse
    # them as `VAR=word1 command word2…`. Single-word values are left
    # unquoted to match the existing SR-IOV / TAP conf shape.
    cat > "$conf" <<EOF
DPDK_IP=${DPDK_IP%%/*}
DPDK_PREFIX=${DPDK_IP##*/}
DPDK_PORT=0
DPDK_MODE=mlx5
DPDK_PCI=${pci}
DPDK_NIC=${DPDK_NIC}
HUGE_DIR=/mnt/huge_2m
MTU=${MTU}
DPDK_EAL_ARGS="-a ${pci} --huge-dir=/mnt/huge_2m"
EOF
    echo "  Config written to ${conf}"
    echo ""
    echo "=== Setup complete (mlx5 bifurcated) ==="
    echo "  Start the server with:"
    echo "    sudo ./scripts/dpdk/dpdk-server.sh"
}

# ---------------------------------------------------------------------------
# sriov-bond path (Intel E810/i40e/ixgbe behind an LACP bond)
# ---------------------------------------------------------------------------
run_sriov_bond_setup() {
    # ---- Configuration ------------------------------------------------
    # Auto-detect bond member PCI addresses and interface names if not set.
    # Finds the first two Intel Ethernet PCI devices that are bond slaves.
    if [[ -z "${PF0_PCI:-}" ]]; then
        BOND_SLAVES=$(cat /sys/class/net/bond0/bonding/slaves 2>/dev/null || true)
        if [[ -n "$BOND_SLAVES" ]]; then
            PF0_IFACE=$(echo "$BOND_SLAVES" | awk '{print $1}')
            PF1_IFACE=$(echo "$BOND_SLAVES" | awk '{print $2}')
            PF0_PCI=$(ethtool -i "$PF0_IFACE" 2>/dev/null | grep bus-info | awk '{print $2}')
            PF1_PCI=$(ethtool -i "$PF1_IFACE" 2>/dev/null | grep bus-info | awk '{print $2}')
        else
            echo "error: no bond0 found and PF0_PCI not set" >&2
            exit 1
        fi
    fi
    PF0_PCI="${PF0_PCI}"
    PF1_PCI="${PF1_PCI}"
    PF0_IFACE="${PF0_IFACE}"
    PF1_IFACE="${PF1_IFACE}"

    # Detect PF driver (ice or ixgbe).
    PF_DRIVER=$(ethtool -i "$PF0_IFACE" 2>/dev/null | awk '/^driver:/{print $2}')
    case "$PF_DRIVER" in
        ice)    NIC_NAME="E810"; VF_DRIVER="iavf" ;;
        i40e)   NIC_NAME="X710/XL710"; VF_DRIVER="iavf" ;;
        ixgbe)  NIC_NAME="82599/X520"; VF_DRIVER="ixgbevf" ;;
        *)
            echo "error: unsupported PF driver '$PF_DRIVER' on $PF0_IFACE" >&2
            echo "  Supported: ice (E810), i40e (X710/XL710), ixgbe (82599/X520/X540)" >&2
            exit 1
            ;;
    esac

    # VLAN ID for trading traffic. Auto-detect from bond VLAN interface if not set.
    if [[ -z "${VLAN_ID:-}" ]]; then
        VLAN_IFACE=$(ip -o link show | grep "bond0\." | head -1 | awk -F'[ :@]+' '{print $2}')
        if [[ -n "$VLAN_IFACE" ]]; then
            VLAN_ID="${VLAN_IFACE##*.}"
        else
            VLAN_ID="1461"  # fallback
        fi
    fi

    # Dedicated IP for the DPDK interface. If not set, auto-derive from the
    # bond VLAN IP by incrementing the last octet by 100. This avoids ARP
    # conflicts between the kernel bond IP and the DPDK smoltcp IP.
    DPDK_IP="${DPDK_IP:-auto}"

    # ---- Auto-detect DPDK IP from bond VLAN interface ----------------
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

    # ---- Derive a fixed VF MAC from the DPDK IP ----------------------
    # With LACP, the switch can deliver a frame to either physical port. Each
    # PF only forwards to its own VFs by destination MAC — so both VFs must
    # share the same MAC. Derived from the IP so it's deterministic per server.
    # Locally-administered (0x02 first octet), unicast.
    DPDK_IP_ONLY="${DPDK_IP%%/*}"
    IFS='.' read -r o1 o2 o3 o4 <<< "$DPDK_IP_ONLY"
    VF_MAC=$(printf "02:00:%02x:%02x:%02x:%02x" "$o1" "$o2" "$o3" "$o4")

    # ---- Preflight checks --------------------------------------------
    # Check IOMMU is enabled. Intel uses DMAR, AMD uses AMD-Vi.
    if ! dmesg | grep -qi "DMAR\|AMD-Vi\|iommu"; then
        if grep -qi "AuthenticAMD" /proc/cpuinfo 2>/dev/null; then
            echo "warning: IOMMU may not be enabled. Add 'iommu=pt' to kernel cmdline and reboot."
        else
            echo "warning: IOMMU may not be enabled. Add 'intel_iommu=on iommu=pt' to kernel cmdline and reboot."
        fi
    fi

    # Check SR-IOV support on the first PF.
    if [[ ! -f "/sys/bus/pci/devices/${PF0_PCI}/sriov_totalvfs" ]]; then
        echo "error: SR-IOV not available on ${PF0_PCI} ($(lspci -s ${PF0_PCI#*:} 2>/dev/null))" >&2
        echo "  Check kernel driver and IOMMU configuration." >&2
        exit 1
    fi

    TOTAL_VFS=$(cat "/sys/bus/pci/devices/${PF0_PCI}/sriov_totalvfs")
    if [[ "$TOTAL_VFS" -eq 0 ]]; then
        echo "error: sriov_totalvfs is 0 on ${PF0_PCI}" >&2
        echo "  IOMMU may not be enabled. Add 'intel_iommu=on iommu=pt' to kernel cmdline and reboot." >&2
        exit 1
    fi

    echo "=== DPDK SR-IOV Setup ($NIC_NAME / $PF_DRIVER) ==="
    echo "  PF0: ${PF0_PCI} (${PF0_IFACE})"
    echo "  PF1: ${PF1_PCI} (${PF1_IFACE})"
    echo "  Driver: ${PF_DRIVER} (VF: ${VF_DRIVER})"
    echo "  VLAN: ${VLAN_ID}"
    echo "  DPDK IP: ${DPDK_IP}"
    echo "  VF MAC: ${VF_MAC}"
    echo "  MTU: ${MTU}"
    echo ""

    # ---- Hugepages ---------------------------------------------------
    setup_hugepages

    # ---- Load vfio-pci module ----------------------------------------
    echo ""
    echo "--- Loading vfio-pci module ---"

    modprobe vfio-pci
    echo "  vfio-pci loaded"

    # Enable unsafe no-IOMMU mode if IOMMU groups aren't properly isolated.
    # ixgbe NICs commonly place PF+VFs in the same IOMMU group, which causes
    # vfio-pci to reject the bind. This workaround skips IOMMU isolation —
    # acceptable for single-tenant benchmark servers, not for shared hosts.
    if [[ -f /sys/module/vfio/parameters/enable_unsafe_noiommu_mode ]]; then
        echo 1 > /sys/module/vfio/parameters/enable_unsafe_noiommu_mode 2>/dev/null || true
        echo "  noiommu mode enabled (fallback for shared IOMMU groups)"
    fi

    # ---- Create VFs on both bond member ports ------------------------

    # Skip setup entirely if VFs are already bound to vfio-pci (idempotent).
    # After a previous DPDK run, VFs stay bound to vfio-pci and the ixgbe
    # VF driver reload would hang trying to unbind from vfio-pci.
    VF0_PCI_CHECK=$(readlink -f "/sys/bus/pci/devices/${PF0_PCI}/virtfn0" 2>/dev/null | xargs basename 2>/dev/null || true)
    if [[ -n "$VF0_PCI_CHECK" && -e "/sys/bus/pci/drivers/vfio-pci/${VF0_PCI_CHECK}" ]]; then
        echo ""
        echo "  VFs already bound to vfio-pci, skipping setup"
        echo ""
        echo "=== Setup complete ($NIC_NAME / $PF_DRIVER) ==="
        echo ""
        echo "  Bond: untouched (LACP active)"
        ACTUAL=$(cat /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages)
        echo "  Hugepages: ${ACTUAL} x 2MB"
        echo ""
        echo "  Start the server with:"
        echo "    sudo ./scripts/dpdk/dpdk-server.sh"
        return 0
    fi

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
        else
            echo 1 > "/sys/bus/pci/devices/${pci}/sriov_numvfs"
            echo "  ${label}: created 1 VF"
            sleep 1
        fi

        # Trust VF 0 (required for DCF on ice, and for MAC changes on ixgbe).
        ip link set "${iface}" vf 0 trust on
        echo "  ${label}: VF 0 trusted"

        # Assign VLAN to VF so it sees VLAN-tagged trading traffic.
        ip link set "${iface}" vf 0 vlan "${VLAN_ID}"
        echo "  ${label}: VF 0 assigned VLAN ${VLAN_ID}"

        # Set fixed MAC on VF. Both VFs share the same MAC so that LACP-hashed
        # frames are forwarded to the VF regardless of which PF receives them.
        ip link set "${iface}" vf 0 mac "${VF_MAC}"
        echo "  ${label}: VF 0 MAC set to ${VF_MAC}"

        # Disable spoofcheck (needed for smoltcp to use its own MAC).
        ip link set "${iface}" vf 0 spoofchk off
        echo "  ${label}: VF 0 spoofcheck disabled"

        # Set MTU on the PF so VFs can use large frames.
        ip link set "${iface}" mtu "${MTU}"
        echo "  ${label}: MTU set to ${MTU}"

        # On ixgbe, the VF driver must be reloaded for MAC changes to take
        # effect. Unbind and rebind the VF from its kernel driver.
        if [[ "$PF_DRIVER" == "ixgbe" ]]; then
            local vf_pci
            vf_pci=$(readlink -f "/sys/bus/pci/devices/${pci}/virtfn0" | xargs basename)
            if [[ -e "/sys/bus/pci/devices/${vf_pci}/driver" ]]; then
                local vf_drv
                vf_drv=$(basename "$(readlink -f "/sys/bus/pci/devices/${vf_pci}/driver")")
                echo "${vf_pci}" > "/sys/bus/pci/devices/${vf_pci}/driver/unbind" 2>/dev/null || true
                echo "${vf_pci}" > "/sys/bus/pci/drivers/${vf_drv}/bind" 2>/dev/null || true
                echo "  ${label}: reloaded VF driver (${vf_drv}) for MAC change"
                sleep 1
            fi
        fi
    }

    create_vf "${PF0_PCI}" "${PF0_IFACE}" "PF0"
    create_vf "${PF1_PCI}" "${PF1_IFACE}" "PF1"

    # ---- Bind VFs to vfio-pci ----------------------------------------

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

        # Unbind from current driver (iavf or ixgbevf).
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

    # ---- Verify + write conf -----------------------------------------

    echo ""
    echo "--- Verification ---"

    echo "  Hugepages:"
    grep -i huge /proc/meminfo | head -3

    echo ""
    echo "  DPDK-bound devices:"
    ls -la /sys/bus/pci/drivers/vfio-pci/ 2>/dev/null | grep "0000:" || echo "  (none found)"

    VF0_PCI=$(readlink -f "/sys/bus/pci/devices/${PF0_PCI}/virtfn0" 2>/dev/null | xargs basename 2>/dev/null || echo "?")
    VF1_PCI=$(readlink -f "/sys/bus/pci/devices/${PF1_PCI}/virtfn0" 2>/dev/null | xargs basename 2>/dev/null || echo "?")

    # Save DPDK config for use by dpdk-server.sh and dpdk-lan-bench.sh.
    DPDK_CONF="/etc/melin-dpdk.conf"
    cat > "$DPDK_CONF" <<EOF
DPDK_IP=${DPDK_IP%%/*}
DPDK_PREFIX=${DPDK_IP##*/}
DPDK_PORT=0
DPDK_MODE=sriov
HUGE_DIR=/mnt/huge_2m
MTU=${MTU}
VF_MAC=${VF_MAC}
VLAN_ID=${VLAN_ID}
EOF
    echo "  Config written to ${DPDK_CONF}"

    echo ""
    echo "=== Setup complete ($NIC_NAME / $PF_DRIVER) ==="
    echo ""
    echo "  VF0 (on PF0): ${VF0_PCI} → vfio-pci"
    echo "  VF1 (on PF1): ${VF1_PCI} → vfio-pci"
    echo "  Bond: untouched (LACP active)"
    ACTUAL=$(cat /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages)
    echo "  Hugepages: ${ACTUAL} x 2MB"
    echo ""
    echo "  Start the server with:"
    echo "    sudo ./scripts/dpdk/dpdk-server.sh"
}

# ---------------------------------------------------------------------------
# L3 bifurcated path (DPDK shares the public NIC with the kernel via
# rte_flow steering — used when the host has no dedicated L2 path to
# its peer and must talk over its public IP via the upstream router).
# ---------------------------------------------------------------------------
run_l3_setup() {
    if [[ -z "${DPDK_PEER_IP:-}" ]]; then
        echo "error: DPDK_PEER_IP=<peer-public-ip> is required in L3 mode" >&2
        echo "  L3 mode steers traffic from one specific source IP into DPDK;" >&2
        echo "  everything else stays with the kernel (SSH, etc.)." >&2
        echo "  Example: DPDK_MODE=l3 DPDK_PEER_IP=67.213.121.227 sudo $0" >&2
        exit 1
    fi

    local def_iface def_gw
    def_iface=$(default_route_iface)
    def_gw=$(ip -4 route show default 2>/dev/null | awk '{print $3; exit}')
    if [[ -z "$def_iface" || -z "$def_gw" ]]; then
        echo "error: could not detect default route — host has no L3 path" >&2
        exit 1
    fi

    # DPDK_NIC defaults to the default-route iface — that IS the point
    # of L3 mode (share the public NIC with the kernel).
    DPDK_NIC="${DPDK_NIC:-$def_iface}"

    # Auto-detect local IP / prefix from the kernel netdev unless overridden.
    if [[ -z "${DPDK_IP:-}" || "$DPDK_IP" == "auto" ]]; then
        local cidr
        cidr=$(ip -4 -o addr show "$DPDK_NIC" 2>/dev/null | awk '{print $4; exit}')
        if [[ -z "$cidr" ]]; then
            echo "error: could not read IPv4 address on $DPDK_NIC" >&2
            exit 1
        fi
        DPDK_IP="$cidr"
    fi

    # Resolve the gateway MAC from the kernel's ARP cache. The kernel
    # already maintains this for every packet to the gateway, so it's
    # almost always REACHABLE. Warm it with a ping if needed.
    local gw_mac
    gw_mac=$(ip neigh show "$def_gw" dev "$DPDK_NIC" 2>/dev/null \
        | awk '{for(i=1;i<NF;i++) if ($i=="lladdr") {print $(i+1); exit}}')
    if [[ -z "$gw_mac" ]]; then
        ping -c 1 -W 1 "$def_gw" >/dev/null 2>&1 || true
        gw_mac=$(ip neigh show "$def_gw" dev "$DPDK_NIC" 2>/dev/null \
            | awk '/lladdr/{print $5; exit}')
    fi
    if [[ -z "$gw_mac" ]]; then
        echo "error: could not resolve gateway $def_gw MAC via ARP" >&2
        exit 1
    fi

    local pci mac
    pci=$(ethtool -i "$DPDK_NIC" 2>/dev/null | awk '/^bus-info:/{print $2}')
    if [[ -z "$pci" ]]; then
        echo "error: could not read PCI address for $DPDK_NIC" >&2
        exit 1
    fi
    mac=$(cat "/sys/class/net/${DPDK_NIC}/address")

    echo "=== DPDK L3 bifurcated mode ==="
    echo "  NIC:          ${DPDK_NIC} (shares the public path with the kernel)"
    echo "  PCI:          ${pci}"
    echo "  Local IP:     ${DPDK_IP}"
    echo "  Local MAC:    ${mac}"
    echo "  Gateway IP:   ${def_gw}"
    echo "  Gateway MAC:  ${gw_mac}"
    echo "  Peer IP:      ${DPDK_PEER_IP}"
    echo "  MTU:          ${MTU}"
    echo ""

    setup_hugepages

    ip link set "${DPDK_NIC}" mtu "${MTU}"
    ip link set "${DPDK_NIC}" up

    local conf="/etc/melin-dpdk.conf"
    cat > "$conf" <<EOF
DPDK_IP=${DPDK_IP%%/*}
DPDK_PREFIX=${DPDK_IP##*/}
DPDK_PORT=0
DPDK_MODE=l3
DPDK_PCI=${pci}
DPDK_NIC=${DPDK_NIC}
DPDK_GATEWAY=${def_gw}
DPDK_GATEWAY_MAC=${gw_mac}
DPDK_PEER_IP=${DPDK_PEER_IP}
HUGE_DIR=/mnt/huge_2m
MTU=${MTU}
DPDK_EAL_ARGS="-a ${pci} --huge-dir=/mnt/huge_2m"
EOF
    echo "  Config written to ${conf}"
    echo ""
    echo "=== Setup complete (L3 bifurcated) ==="
}

# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------
# An explicit DPDK_MODE override always wins; otherwise auto-detect from
# the visible kernel topology. L3 must be explicit — we won't grab the
# public NIC without being told to.
MODE="${DPDK_MODE:-$(detect_mode)}"
case "$MODE" in
    sriov-bond) run_sriov_bond_setup ;;
    mlx5)       run_mlx5_setup ;;
    l3)         run_l3_setup ;;
    *)
        echo "error: could not detect a supported DPDK topology" >&2
        echo "  Supported:" >&2
        echo "    - LACP bond over Intel E810/i40e/ixgbe   → SR-IOV path" >&2
        echo "    - Mellanox CX-{5,6,7} on a raw port      → bifurcated PMD path" >&2
        echo "    - DPDK_MODE=l3 DPDK_PEER_IP=<peer>       → L3 bifurcated (public NIC)" >&2
        echo "  Observed: no bond0 and no mlx5_core netdev." >&2
        echo "  Override the bond auto-detect by setting PF0_PCI / PF1_PCI." >&2
        exit 1
        ;;
esac
