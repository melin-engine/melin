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
#   DPDK_REPL_NIC=<iface>  optional: a second mlx5 netdev, for a node's
#                      replication on a NIC of its own (the node's port
#                      mode, --dpdk-replication-ports). Neither the
#                      default-route port nor DPDK_NIC. Needs DPDK_REPL_IP.
#   DPDK_REPL_IP=<ip[/cidr]>  the replication NIC's IP, in DPDK_IP's
#                      subnet (the node gives the replication NIC the
#                      client NIC's prefix length, gateway, MTU and VLAN),
#                      other than DPDK_IP. Needs DPDK_REPL_NIC.
#   With both, /etc/melin-dpdk.conf also gets DPDK_REPL_{NIC,PCI,IP,MAC,PORT},
#   DPDK_EAL_ARGS allows both devices, and DPDK_PORT is the client NIC's
#   port id among the two, which need not be 0 (see dpdk_pci_port_ids).
#   The SR-IOV and L3 paths refuse them.
#
# Usage (sriov-bond):
#   ./scripts/dpdk/dpdk-setup.sh [--vlan 1461] [--ip 10.189.210.100/24]
#
# Usage (mlx5):
#   DPDK_IP=10.0.0.10/24 ./scripts/dpdk/dpdk-setup.sh
#   DPDK_IP=10.0.0.10/24 DPDK_NIC=enp193s0f1np1 ./scripts/dpdk/dpdk-setup.sh
#   DPDK_IP=10.9.0.102/24 ./scripts/dpdk/dpdk-setup.sh --vlan 2170
#   DPDK_IP=10.0.0.10/24 DPDK_NIC=enp193s0f0np0 DPDK_REPL_NIC=enp193s0f1np1 \
#       DPDK_REPL_IP=10.0.0.20 ./scripts/dpdk/dpdk-setup.sh
#   (--repl-nic and --repl-ip are the flag spellings of the last two)
#
# Pass --vlan whenever the fabric expects 802.1Q tags (Latitude private
# networks do, on bare metal). Untagged frames are dropped by the switch
# without an error on either side, so the symptom is a client that
# connects to nothing rather than a diagnosable failure.
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

# Parse CLI overrides. --vlan applies to both the sriov-bond and mlx5
# paths (the sriov path derives it from the bond VLAN when omitted; mlx5
# has nothing to derive from, so it stays untagged unless given). --nic
# is only meaningful in the mlx5 path, and is ignored silently elsewhere
# rather than rejected — keeps the wrapper interface uniform. --repl-nic
# and --repl-ip are mlx5-only too, but refused elsewhere: a node set up
# without the NIC it was asked for would fail only once started in port
# mode.
while [[ $# -gt 0 ]]; do
    case "$1" in
        --vlan) VLAN_ID="$2"; shift 2 ;;
        --ip)   DPDK_IP="$2"; shift 2 ;;
        --mtu)  MTU="$2"; shift 2 ;;
        --nic)  DPDK_NIC="$2"; shift 2 ;;
        --repl-nic) DPDK_REPL_NIC="$2"; shift 2 ;;
        --repl-ip)  DPDK_REPL_IP="$2"; shift 2 ;;
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
# The replication NIC's pure checks and the mlx5 conf, host-free so that
# test-dpdk-setup.sh runs them (it extracts these functions, each a
# column-0 `name() {` .. `}`, and sources them).
# ---------------------------------------------------------------------------

# `refuse_repl_nic_outside_mlx5 <mode> <DPDK_REPL_NIC> <DPDK_REPL_IP>`:
# fails, saying why, when a replication NIC is asked for on the SR-IOV
# or L3 path. SR-IOV is no longer deployed, and L3 shares the public NIC.
# Any other mode passes: mlx5 takes one, and an undetected topology is
# left to the dispatch's own error, which says what was not found.
refuse_repl_nic_outside_mlx5() {
    local mode="$1" repl_nic="$2" repl_ip="$3"
    [[ ( "$mode" != sriov-bond && "$mode" != l3 ) || ( -z "$repl_nic" && -z "$repl_ip" ) ]] && return 0
    echo "error: a replication NIC (DPDK_REPL_NIC / DPDK_REPL_IP, --repl-nic / --repl-ip) is supported on the mlx5 path only, not ${mode}" >&2
    echo "  The SR-IOV and L3 paths set up one NIC; run without them there." >&2
    return 1
}

# `ipv4_to_int <a.b.c.d>`: the address as a 32-bit integer; fails on
# anything else, an octet with a leading zero included: the node parses
# its addresses as Rust's Ipv4Addr does, which refuses one, so an
# address accepted here and written to the config must be one it starts
# with. scripts/bench/lib.sh has a copy (ipv4_as_int), which
# test-dpdk-setup.sh checks agrees with this one.
ipv4_to_int() {
    local o='(0|[1-9][0-9]{0,2})'
    [[ "$1" =~ ^$o\.$o\.$o\.$o$ ]] || return 1
    local octet value=0
    for octet in "${BASH_REMATCH[@]:1}"; do
        (( octet <= 255 )) || return 1
        value=$(( (value << 8) | octet ))
    done
    echo "$value"
}

# `check_repl_nic_args <DPDK_NIC> <DPDK_IP> <default-route iface>
# <DPDK_REPL_NIC> <DPDK_REPL_IP>`: whether the replication NIC asked for
# is one the node can run (its flags' rules, checked before anything is
# touched): both given, neither the default-route port nor the client
# NIC, an address other than the client NIC's in its subnet, and no
# prefix length other than the client NIC's (the node gives the
# replication NIC the client NIC's). Says why on stderr and fails
# otherwise.
check_repl_nic_args() {
    local nic="$1" ip="$2" def_iface="$3" repl_nic="$4" repl_ip="$5"
    if [[ -z "$repl_nic" || -z "$repl_ip" ]]; then
        echo "error: DPDK_REPL_NIC and DPDK_REPL_IP go together: the replication NIC needs both" >&2
        return 1
    fi
    if [[ "$repl_nic" == "$def_iface" ]]; then
        echo "error: $repl_nic carries the default route; refusing to bind it to DPDK" >&2
        echo "  Handing it to DPDK would break management connectivity." >&2
        echo "  Pick a different port (DPDK_REPL_NIC=<other-iface>)." >&2
        return 1
    fi
    if [[ "$repl_nic" == "$nic" ]]; then
        echo "error: DPDK_REPL_NIC=$repl_nic is the client NIC (DPDK_NIC); the replication NIC must be another port" >&2
        return 1
    fi
    local addr="${ip%%/*}" prefix="${ip#*/}" repl_addr="${repl_ip%%/*}" a b mask
    if [[ "$ip" != */* ]] || ! [[ "$prefix" =~ ^[0-9]{1,2}$ ]] || (( 10#$prefix > 32 )) \
        || ! a=$(ipv4_to_int "$addr"); then
        echo "error: DPDK_IP=$ip is not an IPv4 address with a prefix length (a.b.c.d/len)" >&2
        return 1
    fi
    prefix=$(( 10#$prefix ))
    if ! b=$(ipv4_to_int "$repl_addr") \
        || { [[ "$repl_ip" == */* ]] && ! [[ "${repl_ip#*/}" =~ ^[0-9]{1,2}$ ]]; }; then
        echo "error: DPDK_REPL_IP=$repl_ip is not an IPv4 address (a.b.c.d, or a.b.c.d/len)" >&2
        return 1
    fi
    if [[ "$repl_ip" == */* ]] && (( 10#${repl_ip#*/} != prefix )); then
        echo "error: DPDK_REPL_IP=$repl_ip: the replication NIC takes the client NIC's prefix length (/$prefix, from DPDK_IP); give it /$prefix or none" >&2
        return 1
    fi
    if (( a == b )); then
        echo "error: DPDK_REPL_IP=$repl_addr is the client NIC's address (DPDK_IP); the replication NIC needs an address of its own" >&2
        return 1
    fi
    # A prefix of 0 is an empty mask: a shift by 32 is not one in bash.
    mask=$(( prefix == 0 ? 0 : (0xffffffff << (32 - prefix)) & 0xffffffff ))
    if (( (a & mask) != (b & mask) )); then
        echo "error: DPDK_REPL_IP=$repl_addr is outside the client NIC's subnet ($ip); the replication NIC takes its prefix length and gateway, so it must be in that subnet" >&2
        return 1
    fi
}

# `dpdk_pci_port_ids <client PCI> <replication PCI>`: the DPDK port ids
# the two devices get when DPDK_EAL_ARGS allows both, printed as
# `<client id> <replication id>`.
#
# EAL keeps the PCI devices it scans sorted by address (domain, bus,
# device, function), the PCI bus probes them in that order, and ethdev
# gives each port probed the lowest free id, from 0. `-a` filters what is
# probed but does not order it, so the lower address is port 0 whichever
# `-a` comes first. One port per device: mlx5 makes no representor or
# multiport ports without devargs saying so, and none are written here.
# Fails, saying why, on an address that is not a PCI one or on one device
# given twice.
dpdk_pci_port_ids() {
    local client="$1" repl="$2" pci key keys=()
    for pci in "$client" "$repl"; do
        if ! [[ "$pci" =~ ^([0-9a-fA-F]{4,8}):([0-9a-fA-F]{2}):([0-9a-fA-F]{2})\.([0-7])$ ]]; then
            echo "error: '$pci' is not a PCI address (domain:bus:device.function); cannot tell the DPDK port ids" >&2
            return 1
        fi
        # Each field numerically, in the order EAL compares them (a
        # domain can be wider than four digits).
        key=$(( ((16#${BASH_REMATCH[1]} * 256 + 16#${BASH_REMATCH[2]}) * 256 + 16#${BASH_REMATCH[3]}) * 8 + ${BASH_REMATCH[4]} ))
        keys+=("$key")
    done
    if (( keys[0] == keys[1] )); then
        echo "error: the client and replication NICs are one PCI device ($client); the replication NIC must be another port" >&2
        return 1
    fi
    if (( keys[0] < keys[1] )); then echo "0 1"; else echo "1 0"; fi
}

# `write_mlx5_conf <DPDK_IP> <PCI> <NIC> <MAC> <MTU> [<port> <repl NIC>
# <repl PCI> <repl IP> <repl MAC> <repl port>]`: /etc/melin-dpdk.conf's
# text on the mlx5 path, without VLAN_ID (appended only when given).
# Without a replication NIC, DPDK_PORT is 0 and the text is what it has
# always been; with one, DPDK_PORT is the client NIC's id and the
# DPDK_REPL_* keys follow.
#
# EAL allowlists the DPDK port by PCI address so the mlx5 PMD picks
# up just that device (both, with a replication NIC) and leaves any
# other Mellanox ports (e.g. the SSH port on a dual-port card) untouched.
# Multi-word values are double-quoted so `source` / `eval` consumers
# (dpdk-server.sh, dpdk-test.sh, dpdk-lan-bench.sh) don't misparse
# them as `VAR=word1 command word2…`. Single-word values are left
# unquoted to match the existing SR-IOV / TAP conf shape.
#
# DPDK_MAC is the port's real hardware address. In bifurcated mode
# the kernel netdev keeps it and the DPDK side shares it, so peers
# cannot derive it the way they can for an SR-IOV VF (02:00:<ip>).
# Consumers pass it to the client as `--dpdk-peer-mac`; DPDK_REPL_MAC,
# likewise, to a replica as `--dpdk-replication-peer-mac`.
write_mlx5_conf() {
    local ip="$1" pci="$2" nic="$3" mac="$4" mtu="$5" port="${6:-0}"
    local repl_nic="${7:-}" repl_pci="${8:-}" repl_ip="${9:-}" repl_mac="${10:-}" repl_port="${11:-}"
    local allowed="-a ${pci}"
    [[ -n "$repl_nic" ]] && allowed+=" -a ${repl_pci}"
    cat <<EOF
DPDK_IP=${ip%%/*}
DPDK_PREFIX=${ip##*/}
DPDK_PORT=${port}
DPDK_MODE=mlx5
DPDK_PCI=${pci}
DPDK_NIC=${nic}
DPDK_MAC=${mac}
HUGE_DIR=/mnt/huge_2m
MTU=${mtu}
DPDK_EAL_ARGS="${allowed} --huge-dir=/mnt/huge_2m"
EOF
    [[ -z "$repl_nic" ]] && return 0
    cat <<EOF
DPDK_REPL_NIC=${repl_nic}
DPDK_REPL_PCI=${repl_pci}
DPDK_REPL_IP=${repl_ip%%/*}
DPDK_REPL_MAC=${repl_mac}
DPDK_REPL_PORT=${repl_port}
EOF
}

# ---------------------------------------------------------------------------
# mlx5 bifurcated path
# ---------------------------------------------------------------------------
run_mlx5_setup() {
    local def_iface
    def_iface=$(default_route_iface)

    # Auto-pick the first mlx5 netdev that isn't carrying the default
    # route (nor asked for as the replication NIC), unless the operator
    # overrode with DPDK_NIC. Cheapest way to avoid stealing the SSH port.
    if [[ -z "${DPDK_NIC:-}" ]]; then
        for drv_link in /sys/class/net/*/device/driver; do
            local drv ifname
            drv=$(basename "$(readlink "$drv_link" 2>/dev/null)" 2>/dev/null)
            [[ "$drv" == "mlx5_core" ]] || continue
            ifname=$(basename "$(dirname "$(dirname "$drv_link")")")
            if [[ "$ifname" != "$def_iface" && "$ifname" != "${DPDK_REPL_NIC:-}" ]]; then
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

    # The replication NIC, when asked for: checked whole before anything
    # is touched, and its port id and the client NIC's derived from the
    # two PCI addresses.
    local repl_nic="${DPDK_REPL_NIC:-}" repl_pci="" repl_mac="" port=0 repl_port="" ids
    if [[ -n "$repl_nic" || -n "${DPDK_REPL_IP:-}" ]]; then
        check_repl_nic_args "$DPDK_NIC" "$DPDK_IP" "$def_iface" "$repl_nic" "${DPDK_REPL_IP:-}" || exit 1
        if [[ "$(basename "$(readlink "/sys/class/net/${repl_nic}/device/driver" 2>/dev/null)" 2>/dev/null)" != "mlx5_core" ]]; then
            echo "error: DPDK_REPL_NIC=$repl_nic is not an mlx5 netdev (no such interface, or another driver)" >&2
            exit 1
        fi
        repl_pci=$(ethtool -i "$repl_nic" 2>/dev/null | awk '/^bus-info:/{print $2}')
        if [[ -z "$repl_pci" ]]; then
            echo "error: could not read PCI address for $repl_nic" >&2
            exit 1
        fi
        repl_mac=$(cat "/sys/class/net/${repl_nic}/address")
        ids=$(dpdk_pci_port_ids "$pci" "$repl_pci") || exit 1
        read -r port repl_port <<< "$ids"
    fi

    echo "=== DPDK Mellanox bifurcated mode ==="
    echo "  NIC:     ${DPDK_NIC} (default route on ${def_iface:-<none>}, untouched)"
    echo "  PCI:     ${pci}"
    echo "  DPDK IP: ${DPDK_IP}"
    echo "  MAC:     ${mac} (kernel netdev keeps ownership)"
    echo "  MTU:     ${MTU}"
    if [[ -n "$repl_nic" ]]; then
        echo "  DPDK port: ${port} (of the two allowed devices, by PCI address)"
        echo "  Replication NIC: ${repl_nic} (DPDK port ${repl_port})"
        echo "  Replication PCI: ${repl_pci}"
        echo "  Replication IP:  ${DPDK_REPL_IP%%/*}"
        echo "  Replication MAC: ${repl_mac} (kernel netdev keeps ownership)"
    fi
    echo ""

    setup_hugepages

    # Bring the port up and set MTU. mlx5's bifurcated PMD reads link
    # state from the kernel; a down netdev means DPDK sees link-down.
    # The replication NIC takes the client NIC's MTU, as the node does.
    ip link set "${DPDK_NIC}" mtu "${MTU}"
    ip link set "${DPDK_NIC}" up
    if [[ -n "$repl_nic" ]]; then
        ip link set "${repl_nic}" mtu "${MTU}"
        ip link set "${repl_nic}" up
    fi

    # No vfio-pci rebind. The netdev stays bound to mlx5_core; the DPDK
    # PMD attaches via the ib_uverbs / rdma-core path and shares the
    # device with the kernel.

    # ---------------------------------------------------------------------
    # Write conf
    # ---------------------------------------------------------------------
    local conf="/etc/melin-dpdk.conf"
    # What it holds, and why, is write_mlx5_conf's to say.
    #
    # VLAN_ID is written only when the operator supplied `--vlan`. On a
    # tagged fabric (Latitude private networks tag on bare metal) an
    # untagged frame is dropped by the switch with no error anywhere, so
    # omitting it fails silently rather than loudly. The replication NIC
    # takes the client NIC's VLAN, as the node does.
    if [[ -n "$repl_nic" ]]; then
        write_mlx5_conf "$DPDK_IP" "$pci" "$DPDK_NIC" "$mac" "$MTU" \
            "$port" "$repl_nic" "$repl_pci" "$DPDK_REPL_IP" "$repl_mac" "$repl_port" > "$conf"
    else
        write_mlx5_conf "$DPDK_IP" "$pci" "$DPDK_NIC" "$mac" "$MTU" > "$conf"
    fi
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

    # Save DPDK config for use by dpdk-server.sh and the LAN suite.
    #
    # DPDK_MAC duplicates VF_MAC deliberately: every mode writes the
    # address a peer must send to under the same key, so consumers read
    # one name instead of branching on mode. VF_MAC stays for existing
    # readers. Anything that derives the 02:00:<ip> form itself is
    # working around this key being absent — the derivation belongs here
    # and nowhere else.
    DPDK_CONF="/etc/melin-dpdk.conf"
    cat > "$DPDK_CONF" <<EOF
DPDK_IP=${DPDK_IP%%/*}
DPDK_PREFIX=${DPDK_IP##*/}
DPDK_PORT=0
DPDK_MODE=sriov
HUGE_DIR=/mnt/huge_2m
MTU=${MTU}
VF_MAC=${VF_MAC}
DPDK_MAC=${VF_MAC}
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
refuse_repl_nic_outside_mlx5 "$MODE" "${DPDK_REPL_NIC:-}" "${DPDK_REPL_IP:-}" || exit 1
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
