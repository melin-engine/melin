#!/usr/bin/env bash
# Configure DPDK on Intel, Broadcom or Mellanox NICs. Auto-detects topology:
#
#   sriov-bond  An SR-IOV NIC behind an LACP bond. Creates one SR-IOV VF
#               on each bond member port and binds the VFs to vfio-pci.
#               The bond and PFs are untouched — SSH/management traffic
#               continues normally.
#
#   mlx5        Mellanox ConnectX-{5,6,7} on a raw (non-bonded) port. Uses
#               the bifurcated PMD: the kernel keeps the netdev and DPDK
#               attaches via verbs on the same PF. No SR-IOV, no vfio-pci.
#               The script refuses to use the port that carries the default
#               route so SSH stays alive.
#
#   sriov       An SR-IOV NIC on a raw (non-bonded) port. Creates one VF on
#               the port and binds it to vfio-pci; the PF and any kernel
#               VLAN interface on it stay up beside the VF. The script
#               refuses to use the port that carries the default route.
#
# Supported NICs:
#   - Intel E810 (ice / iavf VF)              — sriov-bond and sriov paths
#   - Intel X710 / XL710 (i40e / iavf)        — sriov-bond and sriov paths
#   - Intel 82599 / X520 / X540 / X550        — sriov-bond and sriov paths
#     (ixgbe / ixgbevf)
#   - Broadcom NetXtreme-E (bnxt_en)          — sriov-bond and sriov paths
#   - Mellanox ConnectX-{5,6,7}               — mlx5 bifurcated path
#
# Prerequisites:
#   - IOMMU enabled (intel_iommu=on iommu=pt or amd_iommu=on iommu=pt) —
#     SR-IOV paths only; mlx5 bifurcated mode doesn't need IOMMU
#     because it never rebinds to vfio-pci.
#   - Root access.
#
# Env vars (mlx5 and sriov paths):
#   DPDK_NIC=<iface>   netdev to hand to DPDK (e.g. enp193s0f1np1).
#                      Optional — defaults to the first port of the
#                      path's NIC that doesn't carry the default route.
#   DPDK_IP=<ip/cidr>  IP for the DPDK transport. Required on the mlx5
#                      path. The sriov path derives it, when not given,
#                      from the kernel's address on the port's VLAN
#                      interface (<iface>.<vlan>), as sriov-bond does
#                      from the bond's.
#
# Usage (sriov-bond):
#   ./scripts/dpdk/dpdk-setup.sh [--vlan 1461] [--ip 10.189.210.100/24]
#
# Usage (sriov):
#   ./scripts/dpdk/dpdk-setup.sh [--vlan 2142] [--ip 10.8.0.101/24] [--nic eno2]
#
# Usage (mlx5):
#   DPDK_IP=10.0.0.10/24 ./scripts/dpdk/dpdk-setup.sh
#   DPDK_IP=10.0.0.10/24 DPDK_NIC=enp193s0f1np1 ./scripts/dpdk/dpdk-setup.sh
#   DPDK_IP=10.9.0.102/24 ./scripts/dpdk/dpdk-setup.sh --vlan 2170
#
# Pass --vlan whenever the fabric expects 802.1Q tags (Latitude private
# networks do, on bare metal). Untagged frames are dropped by the switch
# without an error on either side, so the symptom is a client that
# connects to nothing rather than a diagnosable failure. The SR-IOV paths
# take the VLAN from the kernel's VLAN interface when it is not given.
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

# Parse CLI overrides. --vlan applies to every path (the SR-IOV paths
# derive it from the kernel's VLAN interface when omitted; mlx5 has
# nothing to derive from, so it stays untagged unless given). --nic is
# only meaningful on the mlx5 and sriov paths, and is ignored silently
# elsewhere rather than rejected — keeps the wrapper interface uniform.
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
# by every setup path — DPDK's mempool wants 2MB pages regardless of how
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
# by the mlx5 and sriov paths to avoid stealing the SSH/management port.
default_route_iface() {
    ip -4 route show default 2>/dev/null \
        | head -1 \
        | awk '{for (i = 1; i <= NF; i++) if ($i == "dev") print $(i+1)}'
}

# Print "<iface> <driver>" for every physical port's netdev. A VF's
# netdev is left out: the SR-IOV paths hand DPDK a VF of a port, never a
# VF itself.
netdev_drivers() {
    local drv_link ifname
    for drv_link in /sys/class/net/*/device/driver; do
        [[ -e "$drv_link" ]] || continue
        ifname=${drv_link#/sys/class/net/}
        ifname=${ifname%%/*}
        [[ -e "/sys/class/net/${ifname}/device/physfn" ]] && continue
        echo "${ifname} $(basename "$(readlink "$drv_link")")"
    done
}

# The SR-IOV NIC families, by PF driver: sets NIC_NAME and VF_DRIVER (the
# kernel driver a VF comes up bound to), or fails for a driver the
# SR-IOV paths don't know.
sriov_driver_info() {
    case "$1" in
        ice)     NIC_NAME="E810"; VF_DRIVER="iavf" ;;
        i40e)    NIC_NAME="X710/XL710"; VF_DRIVER="iavf" ;;
        ixgbe)   NIC_NAME="82599/X520"; VF_DRIVER="ixgbevf" ;;
        bnxt_en) NIC_NAME="NetXtreme-E"; VF_DRIVER="bnxt_en" ;;
        *) return 1 ;;
    esac
}

# The port the sriov path hands a VF of to DPDK: the first, in the
# "<iface> <driver>" lines on stdin, whose driver is an SR-IOV one and
# which is not `def_iface` (the default route's port, which carries SSH).
# Fails when there is none.
pick_sriov_nic() {
    local def_iface="$1" ifname drv
    while read -r ifname drv; do
        [[ -n "$ifname" && "$ifname" != "$def_iface" ]] || continue
        if sriov_driver_info "$drv"; then
            echo "$ifname"
            return 0
        fi
    done
    return 1
}

# The VLAN of `nic`'s kernel VLAN interface, `<nic>.<id>`, among the
# interface names on stdin: empty when it has none, and a failure when it
# has several, since which one DPDK should join is then the operator's
# call (--vlan).
nic_vlan() {
    local nic="$1" name found=""
    while read -r name; do
        [[ "$name" =~ ^${nic//./\\.}\.([0-9]+)$ ]] || continue
        [[ -z "$found" ]] || return 1
        found="${BASH_REMATCH[1]}"
    done
    echo "$found"
}

# The first IPv4 address of `iface`, as <ip>/<prefix>, or nothing.
iface_ipv4() {
    ip -4 -o addr show dev "$1" 2>/dev/null | awk '{print $4; exit}'
}

# The DPDK transport's address beside a kernel address <ip> on the same
# network: the last octet plus 100, wrapping at 256. Its own address
# rather than the kernel's avoids ARP conflicts between the two stacks.
derive_dpdk_ip() {
    local ip="$1" prefix="$2" a b c d last
    IFS='.' read -r a b c d <<< "$ip"
    last=$(( (d + 100) % 256 ))
    echo "${a}.${b}.${c}.${last}/${prefix}"
}

# The fixed MAC a VF is given, derived from its DPDK IP so it is the same
# on every run and a peer can compute it (the transport's `02:00:<ip>`
# convention). Locally administered (0x02 first octet), unicast.
vf_mac_for_ip() {
    local o1 o2 o3 o4
    IFS='.' read -r o1 o2 o3 o4 <<< "$1"
    printf "02:00:%02x:%02x:%02x:%02x\n" "$o1" "$o2" "$o3" "$o4"
}

# Check that `pf_pci` can create a VF and that the host can hand one to
# vfio-pci, exiting when it cannot.
sriov_preflight() {
    local pf_pci="$1" total_vfs

    # vfio-pci needs the IOMMU's device groups; without them it binds only
    # in unsafe no-IOMMU mode (see load_vfio).
    if ! compgen -G '/sys/kernel/iommu_groups/*' >/dev/null; then
        if grep -qi "AuthenticAMD" /proc/cpuinfo 2>/dev/null; then
            echo "warning: IOMMU may not be enabled. Add 'amd_iommu=on iommu=pt' to kernel cmdline and reboot."
        else
            echo "warning: IOMMU may not be enabled. Add 'intel_iommu=on iommu=pt' to kernel cmdline and reboot."
        fi
    fi

    if [[ ! -f "/sys/bus/pci/devices/${pf_pci}/sriov_totalvfs" ]]; then
        echo "error: SR-IOV not available on ${pf_pci} ($(lspci -s "${pf_pci#*:}" 2>/dev/null))" >&2
        echo "  Check kernel driver and IOMMU configuration." >&2
        exit 1
    fi

    total_vfs=$(cat "/sys/bus/pci/devices/${pf_pci}/sriov_totalvfs")
    if [[ "$total_vfs" -eq 0 ]]; then
        echo "error: sriov_totalvfs is 0 on ${pf_pci}" >&2
        echo "  SR-IOV is off in the BIOS or the NIC's firmware, or the IOMMU is." >&2
        exit 1
    fi
}

# Load vfio-pci, the driver the SR-IOV paths bind VFs to.
load_vfio() {
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
}

# The PCI address of `pf_pci`'s first VF, or nothing when it has none.
first_vf_pci() {
    local link="/sys/bus/pci/devices/${1}/virtfn0"
    [[ -e "$link" ]] && basename "$(readlink -f "$link")"
    return 0
}

# Whether `pf_pci`'s first VF exists and is bound to vfio-pci, i.e. a
# previous run set it up. Re-running then would hang on ixgbe, whose VF
# driver reload waits on the unbind from vfio-pci.
vf_bound_to_vfio() {
    local vf_pci
    vf_pci=$(first_vf_pci "$1")
    [[ -n "$vf_pci" && -e "/sys/bus/pci/drivers/vfio-pci/${vf_pci}" ]]
}

# Create VF 0 on the port `iface` (PCI `pci`) and give it VF_MAC, the
# VLAN VLAN_ID (none when empty: the port is untagged) and MTU. PF_DRIVER
# names the port's driver.
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

    # Give the VF a port VLAN: the PF tags what it sends and strips what
    # it receives, so the VF, and the DPDK transport on it, sees untagged
    # frames and must not tag them itself.
    if [[ -n "${VLAN_ID:-}" ]]; then
        ip link set "${iface}" vf 0 vlan "${VLAN_ID}"
        echo "  ${label}: VF 0 assigned VLAN ${VLAN_ID}"
    else
        echo "  ${label}: VF 0 untagged (pass --vlan <id> if the fabric expects a tag)"
    fi

    # Set fixed MAC on VF. Behind a bond both VFs share the same MAC so
    # that LACP-hashed frames are forwarded to the VF regardless of which
    # PF receives them.
    ip link set "${iface}" vf 0 mac "${VF_MAC}"
    echo "  ${label}: VF 0 MAC set to ${VF_MAC}"

    # Disable spoofcheck (needed for smoltcp to use its own MAC).
    ip link set "${iface}" vf 0 spoofchk off
    echo "  ${label}: VF 0 spoofcheck disabled"

    # Raise the PF's MTU so VFs can use large frames. Never lower it: a
    # kernel VLAN interface on the port may have raised it for its tag, and
    # every change resets the port.
    local pf_mtu
    pf_mtu=$(cat "/sys/class/net/${iface}/mtu")
    if (( pf_mtu < MTU )); then
        ip link set "${iface}" mtu "${MTU}"
        echo "  ${label}: MTU raised to ${MTU}"
    else
        echo "  ${label}: MTU ${pf_mtu} kept (at least ${MTU})"
    fi

    # On ixgbe, the VF driver must be reloaded for MAC changes to take
    # effect. Unbind and rebind the VF from its kernel driver.
    if [[ "$PF_DRIVER" == "ixgbe" ]]; then
        local vf_pci
        vf_pci=$(first_vf_pci "$pci")
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

# Move `pf_pci`'s VF 0 from its kernel driver to vfio-pci.
bind_vf() {
    local pf_pci="$1"
    local label="$2"

    local vf_pci
    vf_pci=$(first_vf_pci "$pf_pci")

    if [[ -z "$vf_pci" ]]; then
        echo "  ${label}: error: could not find VF PCI address" >&2
        return 1
    fi

    echo "  ${label}: VF at ${vf_pci}"

    # Unbind from current driver (VF_DRIVER).
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

# Save the SR-IOV paths' DPDK config for the LAN suite and the run
# scripts.
#
# DPDK_MAC duplicates VF_MAC deliberately: every mode writes the
# address a peer must send to under the same key, so consumers read
# one name instead of branching on mode. VF_MAC stays for existing
# readers. Anything that derives the 02:00:<ip> form itself is
# working around this key being absent — the derivation belongs here
# and nowhere else.
#
# VLAN_ID is the VF's port VLAN, which the PF applies: a consumer must
# not hand it to the transport as `--dpdk-vlan`, which would tag the
# frames a second time. It is written only when the VF has one.
write_sriov_conf() {
    local conf="/etc/melin-dpdk.conf"
    cat > "$conf" <<EOF
DPDK_IP=${DPDK_IP%%/*}
DPDK_PREFIX=${DPDK_IP##*/}
DPDK_PORT=0
DPDK_MODE=sriov
HUGE_DIR=/mnt/huge_2m
MTU=${MTU}
VF_MAC=${VF_MAC}
DPDK_MAC=${VF_MAC}
EOF
    if [[ -n "${VLAN_ID:-}" ]]; then
        echo "VLAN_ID=${VLAN_ID}" >> "$conf"
    fi
    echo "  Config written to ${conf}"
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
    # A free port on an SR-IOV NIC → one VF on that port.
    local ports
    ports=$(netdev_drivers)
    if pick_sriov_nic "$(default_route_iface)" <<< "$ports" >/dev/null; then
        echo "sriov"
        return
    fi
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
    #
    # DPDK_MAC is the port's real hardware address. In bifurcated mode
    # the kernel netdev keeps it and the DPDK side shares it, so peers
    # cannot derive it the way they can for an SR-IOV VF (02:00:<ip>).
    # Consumers pass it to the client as `--dpdk-peer-mac`.
    #
    # VLAN_ID is written only when the operator supplied `--vlan`. On a
    # tagged fabric (Latitude private networks tag on bare metal) an
    # untagged frame is dropped by the switch with no error anywhere, so
    # omitting it fails silently rather than loudly.
    cat > "$conf" <<EOF
DPDK_IP=${DPDK_IP%%/*}
DPDK_PREFIX=${DPDK_IP##*/}
DPDK_PORT=0
DPDK_MODE=mlx5
DPDK_PCI=${pci}
DPDK_NIC=${DPDK_NIC}
DPDK_MAC=${mac}
HUGE_DIR=/mnt/huge_2m
MTU=${MTU}
DPDK_EAL_ARGS="-a ${pci} --huge-dir=/mnt/huge_2m"
EOF
    if [[ -n "${VLAN_ID:-}" ]]; then
        echo "VLAN_ID=${VLAN_ID}" >> "$conf"
        echo "  VLAN:    ${VLAN_ID} (802.1Q tag inserted/stripped in hardware)"
    else
        echo "  VLAN:    untagged — pass --vlan <id> if the fabric expects a tag"
    fi
    echo "  Config written to ${conf}"
    echo ""
    echo "=== Setup complete (mlx5 bifurcated) ==="
    echo "  Start the server with:"
    echo "    sudo ./scripts/dpdk/dpdk-server.sh"
}

# ---------------------------------------------------------------------------
# sriov-bond path (an SR-IOV NIC behind an LACP bond)
# ---------------------------------------------------------------------------
run_sriov_bond_setup() {
    # ---- Configuration ------------------------------------------------
    # Auto-detect bond member PCI addresses and interface names if not set.
    # Finds the first two Ethernet PCI devices that are bond slaves.
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

    PF_DRIVER=$(ethtool -i "$PF0_IFACE" 2>/dev/null | awk '/^driver:/{print $2}')
    if ! sriov_driver_info "$PF_DRIVER"; then
        echo "error: unsupported PF driver '$PF_DRIVER' on $PF0_IFACE" >&2
        echo "  Supported: ice (E810), i40e (X710/XL710), ixgbe (82599/X520/X540), bnxt_en (NetXtreme-E)" >&2
        exit 1
    fi

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
    # bond VLAN IP (derive_dpdk_ip).
    DPDK_IP="${DPDK_IP:-auto}"

    # ---- Auto-detect DPDK IP from bond VLAN interface ----------------
    if [[ "$DPDK_IP" == "auto" ]]; then
        VLAN_IFACE="bond0.${VLAN_ID}"
        local bond_cidr
        bond_cidr=$(iface_ipv4 "$VLAN_IFACE")

        if [[ -z "$bond_cidr" ]]; then
            echo "error: could not detect IP on $VLAN_IFACE — set DPDK_IP manually" >&2
            exit 1
        fi

        DPDK_IP=$(derive_dpdk_ip "${bond_cidr%/*}" "${bond_cidr#*/}")

        echo "  Bond VLAN IP: ${bond_cidr} (${VLAN_IFACE})"
        echo "  DPDK IP:      ${DPDK_IP} (auto-derived)"
    fi

    # ---- Derive a fixed VF MAC from the DPDK IP ----------------------
    # With LACP, the switch can deliver a frame to either physical port. Each
    # PF only forwards to its own VFs by destination MAC — so both VFs must
    # share the same MAC.
    VF_MAC=$(vf_mac_for_ip "${DPDK_IP%%/*}")

    # ---- Preflight checks --------------------------------------------
    sriov_preflight "$PF0_PCI"

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

    load_vfio

    # ---- Create VFs on both bond member ports ------------------------

    # Skip setup entirely if VFs are already bound to vfio-pci (idempotent).
    if vf_bound_to_vfio "$PF0_PCI"; then
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

    create_vf "${PF0_PCI}" "${PF0_IFACE}" "PF0"
    create_vf "${PF1_PCI}" "${PF1_IFACE}" "PF1"

    # ---- Bind VFs to vfio-pci ----------------------------------------

    echo ""
    echo "--- Binding VFs to vfio-pci ---"

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

    VF0_PCI=$(first_vf_pci "$PF0_PCI")
    VF1_PCI=$(first_vf_pci "$PF1_PCI")

    write_sriov_conf

    echo ""
    echo "=== Setup complete ($NIC_NAME / $PF_DRIVER) ==="
    echo ""
    echo "  VF0 (on PF0): ${VF0_PCI:-?} → vfio-pci"
    echo "  VF1 (on PF1): ${VF1_PCI:-?} → vfio-pci"
    echo "  Bond: untouched (LACP active)"
    ACTUAL=$(cat /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages)
    echo "  Hugepages: ${ACTUAL} x 2MB"
    echo ""
    echo "  Start the server with:"
    echo "    sudo ./scripts/dpdk/dpdk-server.sh"
}

# ---------------------------------------------------------------------------
# sriov path (an SR-IOV NIC on a raw, non-bonded port)
# ---------------------------------------------------------------------------
run_sriov_setup() {
    local def_iface ports names
    def_iface=$(default_route_iface)

    # Auto-pick the first SR-IOV port that isn't carrying the default
    # route, unless the operator overrode with DPDK_NIC.
    if [[ -z "${DPDK_NIC:-}" ]]; then
        ports=$(netdev_drivers)
        if ! DPDK_NIC=$(pick_sriov_nic "$def_iface" <<< "$ports"); then
            echo "error: no free SR-IOV port found (all of them carry the default route?)" >&2
            echo "  Override with DPDK_NIC=<iface>." >&2
            exit 1
        fi
    fi

    if [[ "$DPDK_NIC" == "$def_iface" ]]; then
        echo "error: $DPDK_NIC carries the default route; refusing to hand it to DPDK" >&2
        echo "  Its VF would share the management port's link and switch policy." >&2
        echo "  Pick a different port (DPDK_NIC=<other-iface>)." >&2
        exit 1
    fi

    PF_DRIVER=$(ethtool -i "$DPDK_NIC" 2>/dev/null | awk '/^driver:/{print $2}')
    if ! sriov_driver_info "$PF_DRIVER"; then
        echo "error: unsupported PF driver '$PF_DRIVER' on $DPDK_NIC" >&2
        echo "  Supported: ice (E810), i40e (X710/XL710), ixgbe (82599/X520/X540), bnxt_en (NetXtreme-E)" >&2
        exit 1
    fi
    local pf_pci
    pf_pci=$(ethtool -i "$DPDK_NIC" 2>/dev/null | awk '/^bus-info:/{print $2}')
    if [[ -z "$pf_pci" ]]; then
        echo "error: could not read PCI address for $DPDK_NIC" >&2
        exit 1
    fi

    # The VLAN: --vlan, or the one the kernel's VLAN interface on the port
    # carries, or none (an untagged port).
    if [[ -z "${VLAN_ID:-}" ]]; then
        names=$(ls /sys/class/net)
        if ! VLAN_ID=$(nic_vlan "$DPDK_NIC" <<< "$names"); then
            echo "error: $DPDK_NIC carries several VLAN interfaces; pass --vlan <id>" >&2
            exit 1
        fi
    fi

    # The DPDK address: DPDK_IP, or derived from the kernel's address on
    # the port's VLAN interface (or on the port, when untagged).
    if [[ -z "${DPDK_IP:-}" || "$DPDK_IP" == "auto" ]]; then
        local kernel_iface="$DPDK_NIC" kernel_cidr
        [[ -n "$VLAN_ID" ]] && kernel_iface="${DPDK_NIC}.${VLAN_ID}"
        kernel_cidr=$(iface_ipv4 "$kernel_iface")
        if [[ -z "$kernel_cidr" ]]; then
            echo "error: no IPv4 address on $kernel_iface to derive the DPDK address from" >&2
            echo "  Set DPDK_IP=<addr/cidr> (or --ip)." >&2
            exit 1
        fi
        DPDK_IP=$(derive_dpdk_ip "${kernel_cidr%/*}" "${kernel_cidr#*/}")
        echo "  Kernel IP: ${kernel_cidr} (${kernel_iface})"
    fi

    VF_MAC=$(vf_mac_for_ip "${DPDK_IP%%/*}")

    sriov_preflight "$pf_pci"

    echo "=== DPDK SR-IOV Setup, one port ($NIC_NAME / $PF_DRIVER) ==="
    echo "  NIC:     ${DPDK_NIC} (${pf_pci}; default route on ${def_iface:-<none>}, untouched)"
    echo "  Driver:  ${PF_DRIVER} (VF: ${VF_DRIVER})"
    echo "  VLAN:    ${VLAN_ID:-untagged}"
    echo "  DPDK IP: ${DPDK_IP}"
    echo "  VF MAC:  ${VF_MAC}"
    echo "  MTU:     ${MTU}"
    echo ""

    setup_hugepages

    load_vfio

    if vf_bound_to_vfio "$pf_pci"; then
        echo ""
        echo "  VF already bound to vfio-pci, skipping its setup"
    else
        echo ""
        echo "--- Creating the SR-IOV VF ---"
        create_vf "$pf_pci" "$DPDK_NIC" "PF"

        echo ""
        echo "--- Binding the VF to vfio-pci ---"
        bind_vf "$pf_pci" "PF-VF0"
    fi

    if ! vf_bound_to_vfio "$pf_pci"; then
        echo "error: the VF of ${DPDK_NIC} is not bound to vfio-pci" >&2
        exit 1
    fi

    write_sriov_conf

    echo ""
    echo "=== Setup complete (SR-IOV, one port) ==="
    echo "  VF: $(first_vf_pci "$pf_pci") → vfio-pci"
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
    sriov)      run_sriov_setup ;;
    mlx5)       run_mlx5_setup ;;
    l3)         run_l3_setup ;;
    *)
        echo "error: could not detect a supported DPDK topology" >&2
        echo "  Supported:" >&2
        echo "    - LACP bond over an SR-IOV NIC           → SR-IOV path, a VF per member" >&2
        echo "    - Mellanox CX-{5,6,7} on a raw port      → bifurcated PMD path" >&2
        echo "    - Intel or Broadcom NIC on a raw port    → SR-IOV path, one VF" >&2
        echo "    - DPDK_MODE=l3 DPDK_PEER_IP=<peer>       → L3 bifurcated (public NIC)" >&2
        echo "  Observed: no bond0, no mlx5_core netdev, and no free port on an" >&2
        echo "  ice/i40e/ixgbe/bnxt_en NIC." >&2
        echo "  Override the bond auto-detect by setting PF0_PCI / PF1_PCI." >&2
        exit 1
        ;;
esac
