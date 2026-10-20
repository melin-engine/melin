#!/usr/bin/env bash
# Tests for dpdk-setup.sh's host-free decisions on the SR-IOV paths: which
# port a VF is taken from, which VLAN it joins, and the address and MAC it
# is given. A wrong answer to any of them fails silently on the wire (a
# VF on the SSH port, frames on the wrong VLAN, a peer sending to a MAC
# nothing owns), so they are tested directly.
#
# The function bodies are extracted from dpdk-setup.sh and sourced, so
# these tests exercise the shipping code rather than a copy of it.
#
# Usage: ./scripts/dpdk/test-dpdk-setup.sh

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SETUP_SCRIPT="${SCRIPT_DIR}/dpdk-setup.sh"

if [[ ! -f "$SETUP_SCRIPT" ]]; then
    echo "error: cannot find dpdk-setup.sh at $SETUP_SCRIPT" >&2
    exit 1
fi

TMP_ROOT=$(mktemp -d)
trap 'rm -rf "$TMP_ROOT"' EXIT

# Pull the functions under test out of the setup script. Sourcing the
# whole script is not an option: it is a root-only provisioner that
# would start creating VFs. Anchoring on `^name() {` .. `^}` works
# because the closing brace of a top-level function is the only `}` in
# column 0 within its range.
FUNCTIONS=(sriov_driver_info pick_sriov_nic nic_vlan derive_dpdk_ip vf_mac_for_ip)
FN_FILE="${TMP_ROOT}/fns.sh"
for fn in "${FUNCTIONS[@]}"; do
    awk "/^${fn}\(\) \{/,/^\}/" "$SETUP_SCRIPT"
done > "$FN_FILE"

for fn in "${FUNCTIONS[@]}"; do
    if ! grep -q "^${fn}() {" "$FN_FILE"; then
        echo "error: failed to extract ${fn} from dpdk-setup.sh — has it been" >&2
        echo "  renamed or reformatted? These tests are now vacuous; fix the" >&2
        echo "  extraction before trusting a pass." >&2
        exit 1
    fi
done

# shellcheck source=/dev/null
source "$FN_FILE"

PASS=0
FAIL=0

ok() {
    PASS=$((PASS + 1))
    echo "  ok — $1"
}

bad() {
    FAIL=$((FAIL + 1))
    echo "  FAIL — $1" >&2
    [[ -n "${2:-}" ]] && echo "         $2" >&2
}

# Assert that `cmd` succeeds and prints `want`.
expect() {
    local name="$1" want="$2" got
    shift 2
    if got=$("$@"); then
        if [[ "$got" == "$want" ]]; then
            ok "$name"
        else
            bad "$name" "want '${want}', got '${got}'"
        fi
    else
        bad "$name" "failed (printed '${got}')"
    fi
}

# Assert that `cmd` fails.
expect_failure() {
    local name="$1" got
    shift
    if got=$("$@"); then
        bad "$name" "succeeded, printing '${got}'"
    else
        ok "$name"
    fi
}

# ---------------------------------------------------------------------
# The port: the first SR-IOV one that does not carry the default route.
# ---------------------------------------------------------------------
LATITUDE_PORTS=$'eno1 bnxt_en\neno2 bnxt_en'
expect "the default route's port is skipped for the next one" \
    eno2 pick_sriov_nic eno1 <<< "$LATITUDE_PORTS"
expect "with the default route elsewhere, the first port is taken" \
    eno1 pick_sriov_nic bond0 <<< "$LATITUDE_PORTS"
expect "a port of an unsupported driver is skipped" \
    enp2s0 pick_sriov_nic eno1 <<< $'eno1 ice\nenp1s0 r8169\nenp2s0 ice'
expect "every SR-IOV driver qualifies: i40e" \
    enp3s0 pick_sriov_nic "" <<< 'enp3s0 i40e'
expect "every SR-IOV driver qualifies: ixgbe" \
    enp3s0 pick_sriov_nic "" <<< 'enp3s0 ixgbe'
expect_failure "only the default route's port is SR-IOV: no port" \
    pick_sriov_nic eno1 <<< $'eno1 bnxt_en\neno2 r8169'
expect_failure "a Mellanox port is the mlx5 path's, not this one's" \
    pick_sriov_nic eno1 <<< $'eno1 mlx5_core\neno2 mlx5_core'
expect_failure "no ports: no port" \
    pick_sriov_nic eno1 <<< ''

# ---------------------------------------------------------------------
# The VLAN: the one of the port's kernel VLAN interface, if any.
# ---------------------------------------------------------------------
expect "the port's VLAN interface gives the VLAN" \
    2142 nic_vlan eno2 <<< $'lo\neno1\neno2\neno2.2142'
expect "no VLAN interface: untagged" \
    "" nic_vlan eno2 <<< $'lo\neno1\neno2'
expect "another port's VLAN interface is not this port's" \
    "" nic_vlan eno2 <<< $'eno1\neno1.2142\neno2'
expect "a port whose name extends this one's is another port" \
    "" nic_vlan eno2 <<< $'eno2\neno21.2142'
expect "the port name's dots are literal" \
    "" nic_vlan bond0.5 <<< $'bond0x5.7'
expect_failure "two VLAN interfaces: ambiguous" \
    nic_vlan eno2 <<< $'eno2\neno2.2142\neno2.2143'

# ---------------------------------------------------------------------
# The address beside the kernel's, and the MAC derived from it.
# ---------------------------------------------------------------------
expect "the last octet moves up by 100" \
    10.8.0.101/24 derive_dpdk_ip 10.8.0.1 24
expect "the last octet wraps at 256" \
    10.8.0.44/24 derive_dpdk_ip 10.8.0.200 24
expect "the prefix is kept" \
    10.189.210.150/22 derive_dpdk_ip 10.189.210.50 22
expect "the MAC carries the address, in hex" \
    02:00:0a:08:00:65 vf_mac_for_ip 10.8.0.101
expect "the MAC is locally administered and unicast at the extremes" \
    02:00:ff:00:00:01 vf_mac_for_ip 255.0.0.1

# ---------------------------------------------------------------------
# The driver table names a NIC for each supported driver.
# ---------------------------------------------------------------------
if sriov_driver_info bnxt_en && [[ "$VF_DRIVER" == bnxt_en ]]; then
    ok "a Broadcom PF's VFs come up on bnxt_en"
else
    bad "bnxt_en should be an SR-IOV driver whose VFs use bnxt_en"
fi
expect_failure "mlx5_core is not an SR-IOV path driver" sriov_driver_info mlx5_core

echo ""
echo "=== ${PASS} passed, ${FAIL} failed ==="
[[ "$FAIL" -eq 0 ]]
