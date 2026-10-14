#!/usr/bin/env bash
# Tests for dpdk-setup.sh's host-free parts: the checks on a replication
# NIC, the DPDK port ids of two allowed devices, and the mlx5 conf the
# script writes, which must be what it always was without a replication
# NIC. The rest of the script configures a host's NICs, as root, and is
# exercised by running it there.
#
# The function bodies are extracted from dpdk-setup.sh and sourced, so
# these tests exercise the shipping code rather than a copy of it.
#
# Usage: ./scripts/dpdk/test-dpdk-setup.sh

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SETUP_SCRIPT="${SCRIPT_DIR}/dpdk-setup.sh"

TMP_ROOT=$(mktemp -d)
trap 'rm -rf "$TMP_ROOT"' EXIT

# Sourcing the whole script is not an option: it is a root-only
# provisioner. A top-level function runs from its `name() {` line to the
# first `}` in column 0 after it.
FUNCTIONS=(refuse_repl_nic_outside_mlx5 ipv4_to_int check_repl_nic_args dpdk_pci_port_ids write_mlx5_conf)
FN_FILE="${TMP_ROOT}/fns.sh"
for fn in "${FUNCTIONS[@]}"; do
    awk "/^$fn\(\) \{/,/^\}/" "$SETUP_SCRIPT"
done > "$FN_FILE"
for fn in "${FUNCTIONS[@]}"; do
    if ! grep -q "^${fn}() {" "$FN_FILE"; then
        echo "error: failed to extract ${fn} from dpdk-setup.sh: renamed or reformatted?" >&2
        echo "  These tests are vacuous until the extraction is fixed." >&2
        exit 1
    fi
done
# shellcheck source=/dev/null
source "$FN_FILE"

PASSED=0
FAILED=0

check() {
    local name="$1" expected="$2" actual="$3"
    if [[ "$expected" == "$actual" ]]; then
        echo "  ok — ${name}"
        PASSED=$((PASSED + 1))
    else
        echo "  FAIL — ${name}"
        echo "    expected: ${expected}"
        echo "    actual:   ${actual}"
        FAILED=$((FAILED + 1))
    fi
}

# `status <command...>`: the command's exit status, then its stdout and
# stderr as one line.
status() {
    local out rc=0
    out=$("$@" 2>&1) || rc=$?
    echo "${rc} ${out//$'\n'/ | }"
}

echo "=== refuse_repl_nic_outside_mlx5 ==="
check "the mlx5 path takes a replication NIC" "0 " \
    "$(status refuse_repl_nic_outside_mlx5 mlx5 enp2 10.0.0.20)"
check "any path runs without one" "0 " "$(status refuse_repl_nic_outside_mlx5 sriov-bond "" "")"
check "the SR-IOV path refuses one" \
    "1 error: a replication NIC (DPDK_REPL_NIC / DPDK_REPL_IP, --repl-nic / --repl-ip) is supported on the mlx5 path only, not sriov-bond |   The SR-IOV and L3 paths set up one NIC; run without them there." \
    "$(status refuse_repl_nic_outside_mlx5 sriov-bond enp2 10.0.0.20)"
check "the L3 path refuses one, even given only an address" \
    "1 error: a replication NIC (DPDK_REPL_NIC / DPDK_REPL_IP, --repl-nic / --repl-ip) is supported on the mlx5 path only, not l3 |   The SR-IOV and L3 paths set up one NIC; run without them there." \
    "$(status refuse_repl_nic_outside_mlx5 l3 "" 10.0.0.20)"
check "an undetected topology is left to the dispatch's error" "0 " \
    "$(status refuse_repl_nic_outside_mlx5 unknown enp2 10.0.0.20)"

echo ""
echo "=== ipv4_to_int ==="
check "an address" "0 167772180" "$(status ipv4_to_int 10.0.0.20)"
check "the highest" "0 4294967295" "$(status ipv4_to_int 255.255.255.255)"
check "an octet of zero" "0 167772160" "$(status ipv4_to_int 10.0.0.0)"
check "a leading zero (the node refuses it)" "1 " "$(status ipv4_to_int 10.0.0.08)"
check "a leading zero on a valid octal" "1 " "$(status ipv4_to_int 010.0.0.1)"
check "an octet past 255" "1 " "$(status ipv4_to_int 10.0.0.256)"
check "three octets" "1 " "$(status ipv4_to_int 10.0.1)"
check "a prefix length is not an address" "1 " "$(status ipv4_to_int 10.0.0.1/24)"

echo ""
echo "=== check_repl_nic_args ==="
# The client NIC enp1 at 10.0.0.10/24, the default route on eno1.
repl() { status check_repl_nic_args enp1 10.0.0.10/24 eno1 "$@"; }
check "a second port in the client subnet" "0 " "$(repl enp2 10.0.0.20)"
check "with the client's prefix length" "0 " "$(repl enp2 10.0.0.20/24)"
check "the subnet's last host address" "0 " "$(repl enp2 10.0.0.254)"
check "a NIC without an address" \
    "1 error: DPDK_REPL_NIC and DPDK_REPL_IP go together: the replication NIC needs both" \
    "$(repl enp2 "")"
check "an address without a NIC" \
    "1 error: DPDK_REPL_NIC and DPDK_REPL_IP go together: the replication NIC needs both" \
    "$(repl "" 10.0.0.20)"
check "the default-route port" \
    "1 error: eno1 carries the default route; refusing to bind it to DPDK |   Handing it to DPDK would break management connectivity. |   Pick a different port (DPDK_REPL_NIC=<other-iface>)." \
    "$(repl eno1 10.0.0.20)"
check "the client NIC twice" \
    "1 error: DPDK_REPL_NIC=enp1 is the client NIC (DPDK_NIC); the replication NIC must be another port" \
    "$(repl enp1 10.0.0.20)"
check "an address outside the client subnet" \
    "1 error: DPDK_REPL_IP=10.0.1.20 is outside the client NIC's subnet (10.0.0.10/24); the replication NIC takes its prefix length and gateway, so it must be in that subnet" \
    "$(repl enp2 10.0.1.20)"
check "the client NIC's address" \
    "1 error: DPDK_REPL_IP=10.0.0.10 is the client NIC's address (DPDK_IP); the replication NIC needs an address of its own" \
    "$(repl enp2 10.0.0.10/24)"
check "another prefix length" \
    "1 error: DPDK_REPL_IP=10.0.0.20/16: the replication NIC takes the client NIC's prefix length (/24, from DPDK_IP); give it /24 or none" \
    "$(repl enp2 10.0.0.20/16)"
check "a malformed address" \
    "1 error: DPDK_REPL_IP=10.0.0 is not an IPv4 address (a.b.c.d, or a.b.c.d/len)" \
    "$(repl enp2 10.0.0)"
check "an octet with a leading zero, which the node refuses" \
    "1 error: DPDK_REPL_IP=10.0.0.020 is not an IPv4 address (a.b.c.d, or a.b.c.d/len)" \
    "$(repl enp2 10.0.0.020)"
check "a malformed prefix length" \
    "1 error: DPDK_REPL_IP=10.0.0.20/x is not an IPv4 address (a.b.c.d, or a.b.c.d/len)" \
    "$(repl enp2 10.0.0.20/x)"
check "a client address without a prefix length" \
    "1 error: DPDK_IP=10.0.0.10 is not an IPv4 address with a prefix length (a.b.c.d/len)" \
    "$(status check_repl_nic_args enp1 10.0.0.10 eno1 enp2 10.0.0.20)"
check "a client prefix length past 32" \
    "1 error: DPDK_IP=10.0.0.10/33 is not an IPv4 address with a prefix length (a.b.c.d/len)" \
    "$(status check_repl_nic_args enp1 10.0.0.10/33 eno1 enp2 10.0.0.20)"
# The subnet is the prefix's bits only, whatever the length.
check "a /16 takes the third octet in" "0 " \
    "$(status check_repl_nic_args enp1 10.0.0.10/16 eno1 enp2 10.0.255.254)"
check "a /32 has no room for a second address" \
    "1 error: DPDK_REPL_IP=10.0.0.11 is outside the client NIC's subnet (10.0.0.10/32); the replication NIC takes its prefix length and gateway, so it must be in that subnet" \
    "$(status check_repl_nic_args enp1 10.0.0.10/32 eno1 enp2 10.0.0.11)"
check "a /0 takes every address in" "0 " \
    "$(status check_repl_nic_args enp1 10.0.0.10/0 eno1 enp2 192.168.0.1)"
check "a /25 splits a /24" \
    "1 error: DPDK_REPL_IP=10.0.0.200 is outside the client NIC's subnet (10.0.0.10/25); the replication NIC takes its prefix length and gateway, so it must be in that subnet" \
    "$(status check_repl_nic_args enp1 10.0.0.10/25 eno1 enp2 10.0.0.200)"
check "no default route: any other port" "0 " \
    "$(status check_repl_nic_args enp1 10.0.0.10/24 "" enp2 10.0.0.20)"

echo ""
echo "=== dpdk_pci_port_ids ==="
check "the client NIC first on the bus is port 0" "0 0 1" \
    "$(status dpdk_pci_port_ids 0000:c1:00.0 0000:c1:00.1)"
check "the client NIC second on the bus is port 1" "0 1 0" \
    "$(status dpdk_pci_port_ids 0000:c1:00.1 0000:c1:00.0)"
check "the bus outranks the function" "0 1 0" \
    "$(status dpdk_pci_port_ids 0000:c1:00.0 0000:41:00.1)"
check "the device outranks the function" "0 0 1" \
    "$(status dpdk_pci_port_ids 0000:41:00.7 0000:41:01.0)"
check "the domain outranks the bus" "0 1 0" \
    "$(status dpdk_pci_port_ids 0001:00:00.0 0000:ff:1f.7)"
check "the order is numeric, case aside" "0 0 1" \
    "$(status dpdk_pci_port_ids 0000:0A:00.0 0000:a1:00.0)"
check "a domain past four digits" "0 1 0" \
    "$(status dpdk_pci_port_ids 10000:00:00.0 0000:c1:00.0)"
check "one device twice is refused" \
    "1 error: the client and replication NICs are one PCI device (0000:c1:00.0); the replication NIC must be another port" \
    "$(status dpdk_pci_port_ids 0000:c1:00.0 0000:C1:00.0)"
check "an address that is not a PCI one is refused" \
    "1 error: 'c1:00.0' is not a PCI address (domain:bus:device.function); cannot tell the DPDK port ids" \
    "$(status dpdk_pci_port_ids 0000:c1:00.0 c1:00.0)"
check "an empty address is refused" \
    "1 error: '' is not a PCI address (domain:bus:device.function); cannot tell the DPDK port ids" \
    "$(status dpdk_pci_port_ids "" 0000:c1:00.0)"

echo ""
echo "=== write_mlx5_conf ==="
# Without a replication NIC: the text the script wrote before it took
# one, byte for byte (its heredoc, as it was, with these values).
check "without a replication NIC, the conf is unchanged" \
    'DPDK_IP=10.0.0.10
DPDK_PREFIX=24
DPDK_PORT=0
DPDK_MODE=mlx5
DPDK_PCI=0000:c1:00.1
DPDK_NIC=enp193s0f1np1
DPDK_MAC=0c:42:a1:00:00:01
HUGE_DIR=/mnt/huge_2m
MTU=1500
DPDK_EAL_ARGS="-a 0000:c1:00.1 --huge-dir=/mnt/huge_2m"' \
    "$(write_mlx5_conf 10.0.0.10/24 0000:c1:00.1 enp193s0f1np1 0c:42:a1:00:00:01 1500)"
# Command substitution drops trailing newlines: compare the bytes.
check "and ends in one newline, as it did" "0a" \
    "$(write_mlx5_conf 10.0.0.10/24 0000:c1:00.1 enp193s0f1np1 0c:42:a1:00:00:01 1500 | tail -c 1 | od -An -tx1 | tr -d ' ')"
check "with a replication NIC, both devices allowed and the DPDK_REPL_* keys" \
    'DPDK_IP=10.0.0.10
DPDK_PREFIX=24
DPDK_PORT=1
DPDK_MODE=mlx5
DPDK_PCI=0000:c1:00.1
DPDK_NIC=enp193s0f1np1
DPDK_MAC=0c:42:a1:00:00:01
HUGE_DIR=/mnt/huge_2m
MTU=9000
DPDK_EAL_ARGS="-a 0000:c1:00.1 -a 0000:c1:00.0 --huge-dir=/mnt/huge_2m"
DPDK_REPL_NIC=enp193s0f0np0
DPDK_REPL_PCI=0000:c1:00.0
DPDK_REPL_IP=10.0.0.20
DPDK_REPL_MAC=0c:42:a1:00:00:00
DPDK_REPL_PORT=0' \
    "$(write_mlx5_conf 10.0.0.10/24 0000:c1:00.1 enp193s0f1np1 0c:42:a1:00:00:01 9000 \
        1 enp193s0f0np0 0000:c1:00.0 10.0.0.20/24 0c:42:a1:00:00:00 0)"
# The suite reads the conf through lib.sh's conf_value; what it reads
# back is what was written.
# shellcheck source=../bench/lib.sh
source "${SCRIPT_DIR}/../bench/lib.sh"
CONF=$(write_mlx5_conf 10.0.0.10/24 0000:c1:00.1 enp193s0f1np1 0c:42:a1:00:00:01 9000 \
    1 enp193s0f0np0 0000:c1:00.0 10.0.0.20 0c:42:a1:00:00:00 0)
check "the suite reads the replication NIC's keys back" "0|10.0.0.20|0c:42:a1:00:00:00|1" \
    "$(conf_value "$CONF" DPDK_REPL_PORT)|$(conf_value "$CONF" DPDK_REPL_IP)|$(conf_value "$CONF" DPDK_REPL_MAC)|$(conf_value "$CONF" DPDK_PORT)"
check "and finds the replication NIC allowed, so takes the host for port mode" "0 " \
    "$(status refuse_port_mode root@h1 "$CONF")"

echo ""
echo "=== ${PASSED} passed, ${FAILED} failed ==="
[[ "$FAILED" -eq 0 ]]
