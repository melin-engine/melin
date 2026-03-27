#!/usr/bin/env bash
# Test the DPDK environment independently of the melin server.
#
# Uses dpdk-testpmd (shipped with DPDK) to verify that ports, hugepages,
# VF binding, VLAN, and MAC configuration are working. This rules out
# NIC/driver/setup issues before debugging application-level problems.
#
# Modes:
#   info (default)  — show port info and exit
#   icmpecho        — respond to ICMP pings (test L2/L3 connectivity)
#   forward         — forward packets between ports (test LACP both-port RX)
#
# Usage:
#   sudo ./scripts/dpdk-test.sh [info|icmpecho|forward]
#
# Prerequisites:
#   - dpdk-testpmd installed (dnf install dpdk-tools / apt install dpdk)
#   - Hugepages configured
#   - VFs bound to vfio-pci (run dpdk-setup-sriov.sh first)
#
# Examples:
#   # Verify ports are detected and link is up:
#   sudo ./scripts/dpdk-test.sh
#
#   # Test connectivity from the bench machine (ping <DPDK_IP>):
#   sudo ./scripts/dpdk-test.sh icmpecho
#
#   # Test LACP — verify both bond members receive traffic:
#   sudo ./scripts/dpdk-test.sh forward

set -euo pipefail

if [[ $EUID -ne 0 ]]; then
    echo "error: must run as root" >&2
    echo "usage: sudo $0 [info|icmpecho|forward]" >&2
    exit 1
fi

if ! command -v dpdk-testpmd &>/dev/null; then
    echo "error: dpdk-testpmd not found" >&2
    echo "install: dnf install dpdk-tools  (or apt install dpdk)" >&2
    exit 1
fi

MODE="${1:-info}"

# Read config from dpdk-setup-sriov.sh if available.
CONF="/etc/melin-dpdk.conf"
if [[ -f "$CONF" ]]; then
    source "$CONF"
    echo "  Loaded config from $CONF"
    echo "    DPDK_IP=$DPDK_IP, MTU=${MTU:-1500}, VF_MAC=${VF_MAC:-auto}"
else
    echo "  No $CONF found — using defaults (run dpdk-setup-sriov.sh first for SR-IOV)"
fi

HUGE_DIR="${HUGE_DIR:-/mnt/huge_2m}"

# Ensure hugepages are mounted.
if ! mount | grep -q "pagesize=2M"; then
    mkdir -p "$HUGE_DIR"
    mount -t hugetlbfs -o pagesize=2M nodev "$HUGE_DIR"
    echo "  Mounted 2MB hugetlbfs at $HUGE_DIR"
fi

echo ""
echo "============================================================"
echo "  DPDK Environment Test (mode: $MODE)"
echo "============================================================"
echo ""

case "$MODE" in
    info)
        # Non-interactive: start testpmd, dump port info, exit.
        echo "=== Port Info ==="
        dpdk-testpmd --huge-dir="$HUGE_DIR" -- \
            --tx-first \
            --stats-period=1 \
            --nb-cores=1 \
            2>&1 <<< "show port info all
show port stats all
quit" | grep -E "^(  |Port |Link |MAC |Promiscuous|RX|TX|---)" || true
        echo ""
        echo "If ports show 'Link status: up' and the MAC matches your VF config, the DPDK environment is healthy."
        ;;

    icmpecho)
        # Interactive: respond to ICMP pings. Test from bench machine with:
        #   ping <DPDK_IP>
        echo "=== ICMP Echo Mode ==="
        echo "  testpmd will respond to ICMP pings on all DPDK ports."
        echo "  From the bench machine, run: ping ${DPDK_IP:-<DPDK_IP>}"
        echo "  Press Ctrl-C to stop."
        echo ""
        dpdk-testpmd --huge-dir="$HUGE_DIR" -- \
            -i \
            --forward-mode=icmpecho \
            --nb-cores=1
        ;;

    forward)
        # Interactive: forward packets between ports. Useful for testing
        # LACP — traffic arriving on either port should show up in stats.
        echo "=== Forward Mode ==="
        echo "  testpmd will forward packets between ports."
        echo "  Use 'show port stats all' to check RX/TX on each port."
        echo "  Type 'start' to begin, 'stop' to pause, 'quit' to exit."
        echo ""
        dpdk-testpmd --huge-dir="$HUGE_DIR" -- \
            -i \
            --nb-cores=1
        ;;

    *)
        echo "error: unknown mode '$MODE'" >&2
        echo "usage: sudo $0 [info|icmpecho|forward]" >&2
        exit 1
        ;;
esac
