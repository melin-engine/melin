#!/usr/bin/env bash
# Smoke-test the DPDK transport on a veth pair: no NIC, no hugepages, no
# root.
#
# Re-executes itself under `unshare -rnm` (new user, network and mount
# namespaces). Inside, it creates a veth pair, starts the echo server on
# DPDK on one end through the net_af_packet PMD, and points kernel-TCP
# echo clients at it from the other end. Two checks:
#
#   - a client with an authorized key completes round trips;
#   - a client with an unknown key is refused (AuthFailed).
#
# Nothing outside the namespaces is touched: the veth pair, the addresses
# and the tmpfs on /var/run all vanish with it. This validates the
# transport's logic, not its latency — af_packet is not a NIC. See
# docs/internal/dpdk-veth-testing.md for what it can and cannot cover.
#
# It builds nothing. Build the echo binaries with DPDK first:
#   cargo build --release -p melin-example-echo --features melin-server-runtime/dpdk
#
# Usage:
#   ./scripts/dpdk/dpdk-veth.sh [BIN_DIR]
#
# BIN_DIR holds echo-server and echo-client. Default:
# ${CARGO_TARGET_DIR:-target}/release.
#
# Prerequisites:
#   - libdpdk (the runtime libraries, including the af_packet PMD)
#   - util-linux `unshare`, and python3 (standard library only) for the
#     network setup (veth-setup.py): neither iproute2 nor ethtool is needed
#   - unprivileged user namespaces. Ubuntu 24.04 restricts them through
#     AppArmor; lift that with
#       sudo sysctl -w kernel.apparmor_restrict_unprivileged_userns=0
#
# Env vars:
#   RUST_LOG       server log filter (default: info, with the DPDK
#                  transport's per-connection debug lines)
#   STARTUP_SECS   how long the server may take to serve (default: 30)

set -euo pipefail

# Marks the copy of this script running inside the namespaces.
INNER_ENV=MELIN_DPDK_VETH_INNER

if [[ -z "${!INNER_ENV:-}" ]]; then
    REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
    BIN_DIR="${1:-${CARGO_TARGET_DIR:-$REPO_ROOT/target}/release}"
    BIN_DIR="$(cd "$BIN_DIR" 2>/dev/null && pwd)" || {
        echo "error: binary directory not found: ${1:-${CARGO_TARGET_DIR:-$REPO_ROOT/target}/release}" >&2
        exit 1
    }
    for bin in echo-server echo-client; do
        if [[ ! -x "$BIN_DIR/$bin" ]]; then
            echo "error: $BIN_DIR/$bin not found" >&2
            echo "build: cargo build --release -p melin-example-echo --features melin-server-runtime/dpdk" >&2
            exit 1
        fi
    done
    for tool in unshare python3; do
        if ! command -v "$tool" &>/dev/null; then
            echo "error: $tool not found" >&2
            exit 1
        fi
    done
    if ! unshare -rnm true 2>/dev/null; then
        echo "error: cannot create an unprivileged user + network + mount namespace" >&2
        echo "  (\`unshare -rnm true\` failed: $(unshare -rnm true 2>&1 || true))" >&2
        echo "  On Ubuntu 24.04 and later, AppArmor restricts them by default. Lift it with:" >&2
        echo "    sudo sysctl -w kernel.apparmor_restrict_unprivileged_userns=0" >&2
        echo "  Elsewhere, check kernel.unprivileged_userns_clone and user.max_user_namespaces." >&2
        exit 1
    fi
    export "$INNER_ENV=1" BIN_DIR
    exec unshare -rnm -- "$(realpath "${BASH_SOURCE[0]}")"
fi

# ---------------------------------------------------------------------------
# Inside the namespaces from here on.
# ---------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NODE_IP=10.99.0.2
CLIENT_IP=10.99.0.1
PREFIX_LEN=24
PORT=9876
STARTUP_SECS="${STARTUP_SECS:-30}"

# The tmpfs mounted on /var/run below would hide a work directory under
# it (TMPDIR=/run/user/$UID, say), so in that case it goes in /tmp.
case "$(realpath -m "${TMPDIR:-/tmp}")" in
    /run | /run/* | /var/run | /var/run/*) WORK="$(mktemp -d -p /tmp)" ;;
    *) WORK="$(mktemp -d)" ;;
esac
SERVER_PID=""
cleanup() {
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill -KILL "$SERVER_PID" 2>/dev/null || true
    fi
    rm -rf "$WORK"
}
trap cleanup EXIT

fail() {
    echo "FAIL: $*" >&2
    if [[ -f "$WORK/server.log" ]]; then
        echo "--- server log (last 40 lines) ---" >&2
        tail -n 40 "$WORK/server.log" >&2
    fi
    exit 1
}

# veth0 (DPDK, no address) <-> veth1 (kernel, $CLIENT_IP), both up, TX
# checksum offload off on both (see veth-setup.py for why).
python3 "$SCRIPT_DIR/veth-setup.py" veth0 veth1 "$CLIENT_IP/$PREFIX_LEN" || fail "network setup"

# EAL insists on creating /var/run/dpdk, and inside the user namespace it
# believes it is root. A private tmpfs, gone with the mount namespace.
mount -t tmpfs tmpfs /var/run || fail "mounting a tmpfs on /var/run"

# Fixed keys, so no key tool is needed. The authorized one is the PKCS#8
# PEM `openssl genpkey -algorithm ed25519` wrote for melin-client's own
# key tests, with the public key openssl derives for it. The unknown one
# is a raw 32-byte seed listed nowhere.
cat >"$WORK/good.pem" <<'EOF'
-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIDclw/zwdZEQraidYISn+CjytFLopT9cneV0G7+MvdtR
-----END PRIVATE KEY-----
EOF
printf 'veth-smoke-unknown-key-32-bytes!' >"$WORK/unknown.key"
echo "writer +tVsQuDHgy200knb+jTv5Zs6XAr4eV5crZS0j/578Ac= veth-smoke" >"$WORK/authorized_keys"

# The first CPU this process may run on, for EAL's main lcore: CPU 0 need
# not be in the set on a restricted host.
CPU="$(awk '/^Cpus_allowed_list/ { split($2, r, /[-,]/); print r[1] }' /proc/self/status)"

# `--in-memory` is not usable here: EAL refuses it with `--no-huge`, which
# implies legacy memory. The private /var/run makes it unnecessary.
EAL_ARGS="--no-huge -m 512 --no-pci --vdev=net_af_packet0,iface=veth0 -l $CPU"

echo "Starting echo-server on DPDK (net_af_packet on veth0, $NODE_IP:$PORT)"
RUST_LOG="${RUST_LOG:-info,melin_server_runtime::dpdk_transport=debug}" \
    "$BIN_DIR/echo-server" \
    --bind "$NODE_IP:$PORT" \
    --journal "$WORK/echo.journal" \
    --authorized-keys "$WORK/authorized_keys" \
    --standalone --ack-policy disk --no-mlock --cores none \
    --dpdk-eal-args="$EAL_ARGS" \
    --dpdk-ip "$NODE_IP" --dpdk-prefix-len "$PREFIX_LEN" \
    >"$WORK/server.log" 2>&1 &
SERVER_PID=$!

# Check 1: an authorized key completes round trips. Retried until the
# server serves: EAL init and journal creation take a moment, and before
# the port is up nothing answers ARP.
echo "Check 1: an authorized key completes round trips"
deadline=$((SECONDS + STARTUP_SECS))
until out="$("$BIN_DIR/echo-client" --server "$NODE_IP:$PORT" --key "$WORK/good.pem" --count 100 2>&1)"; do
    kill -0 "$SERVER_PID" 2>/dev/null || fail "echo-server exited during startup"
    ((SECONDS < deadline)) || fail "no round trip within ${STARTUP_SECS}s; last attempt: $out"
    sleep 0.5
done
echo "$out" | sed 's/^/    /'

# Check 2: an unknown key is refused, and refused for that reason rather
# than for anything else that can go wrong on the way.
echo "Check 2: an unknown key is refused"
if out="$("$BIN_DIR/echo-client" --server "$NODE_IP:$PORT" --key "$WORK/unknown.key" 2>&1)"; then
    fail "a client with an unknown key completed a round trip: $out"
fi
[[ "$out" == *"authentication failed"* ]] || fail "expected an authentication failure, got: $out"
echo "    $out"

# A clean shutdown is part of the check: a server that hangs on SIGTERM
# would hang an operator's restart too.
kill -TERM "$SERVER_PID"
for _ in $(seq 1 100); do
    kill -0 "$SERVER_PID" 2>/dev/null || break
    sleep 0.2
done
if kill -0 "$SERVER_PID" 2>/dev/null; then
    fail "echo-server did not shut down within 20s of SIGTERM"
fi
status=0
wait "$SERVER_PID" || status=$?
SERVER_PID=""
((status == 0)) || fail "echo-server exited with status $status"

echo "PASS: DPDK on veth — round trips served, unknown key refused, clean shutdown"
