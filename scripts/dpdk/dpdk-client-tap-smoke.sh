#!/usr/bin/env bash
# Smoke test for the DPDK client: a DPDK echo client against a kernel echo
# server, over a DPDK TAP virtual device, with no NIC and no namespace.
#
# The mirror image of the Exchange Core's TAP smoke test, which runs a DPDK
# node on a TAP device and talks to it from a kernel client. Here the
# client is the DPDK side: `echo-client --transport dpdk` brings EAL up on
# `--vdev=net_tap0 --no-pci`, which creates a kernel-visible TAP interface;
# the kernel side of that interface carries the echo server's address, and
# the client dials it through the whole of `melin-client`'s DPDK path: EAL,
# the NIC handle, the seeded peer MAC, the dial, the handshake, and a
# closed loop of echoes. A check outside the test rig and the test suite,
# for any host with libdpdk; the rig (`netns-runner.sh`) covers the same
# path in CI, on af_packet.
#
# REQUIRES `echo-client --transport dpdk`, which arrives with step 6 of
# docs/internal/dpdk-client-2026-10.md. Until then the script stops after
# the build, saying so: it was written against the flag rather than a
# throwaway client of its own, so that it checks the client an operator
# runs. The flags it passes are the shm-proxy's (`--dpdk-eal-args`,
# `--dpdk-ip`, `--dpdk-prefix-len`, `--dpdk-peer-mac`), which the plan
# gives the echo client.
#
# Flow:
#   1. build echo-server on kernel TCP and echo-client with `dpdk`, and
#      stop if the client has no `--transport` flag (before step 6);
#   2. hugepages: 2 MB pages allocated and a hugetlbfs mounted, if needed;
#   3. an Ed25519 key and an authorized_keys file;
#   4. start echo-server on kernel TCP, on every address;
#   5. start echo-client on DPDK, over the TAP device;
#   6. as soon as the TAP interface appears, give its kernel side a fixed
#      MAC and the server's address and bring it up (the client's SYNs
#      before that go unanswered, and its retransmissions cover the gap);
#   7. check the client's run finished and reported its round trips.
#
# The kernel side's MAC is fixed and passed to the client
# (`--dpdk-peer-mac`), and is deliberately not the `02:00:<ip>` convention
# the client falls back to without one: the run passes only if the peer
# MAC was passed through, as on any port that keeps its hardware address.
#
# Usage:
#   sudo ./scripts/dpdk/dpdk-client-tap-smoke.sh [COUNT]
#
# COUNT is the number of echoes (default 1000).
#
# Prerequisites:
#   - libdpdk with the net_tap PMD (dnf install dpdk-devel, apt install
#     libdpdk-dev), and libclang for the build
#   - root: hugepages, and the TAP interface's address
#   - iproute2 (`ip`) and openssl (the key)
#   - nothing else on the host using 192.168.201.0/24, the TAP interface
#     name below, or the server's port; a host firewall that filters a new
#     interface (firewalld's default zone) must let the port through

set -euo pipefail

# Ensure cargo/rustup work when running under sudo, which sets HOME to
# /root: point PATH, RUSTUP_HOME and CARGO_HOME at the invoking user's.
if [[ -n "${SUDO_USER:-}" ]]; then
    REAL_HOME=$(getent passwd "$SUDO_USER" | cut -d: -f6)
    export PATH="$REAL_HOME/.cargo/bin:$PATH"
    export RUSTUP_HOME="${RUSTUP_HOME:-$REAL_HOME/.rustup}"
    export CARGO_HOME="${CARGO_HOME:-$REAL_HOME/.cargo}"
fi

if [[ $EUID -ne 0 ]]; then
    echo "error: must run as root (hugepages + TAP interface)" >&2
    echo "usage: sudo $0 [COUNT]" >&2
    exit 1
fi

for tool in cargo ip openssl; do
    if ! command -v "$tool" &>/dev/null; then
        echo "error: $tool not found" >&2
        exit 1
    fi
done

COUNT="${1:-1000}"
if ! [[ "$COUNT" =~ ^[1-9][0-9]*$ ]]; then
    echo "error: COUNT must be a positive integer, not '$COUNT'" >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
WORK_DIR=$(mktemp -d)

# The TAP link. The kernel side carries the server's address, the DPDK
# side is the client's. A subnet and an interface name of their own, so
# the run can sit beside the Exchange Core's TAP smoke test
# (192.168.200.0/24 on dtap0).
TAP_IFACE=mdtap0
SERVER_IP=192.168.201.1
CLIENT_IP=192.168.201.2
PREFIX_LEN=24
# Locally administered, and not 02:00:c0:a8:c9:01, the convention's MAC
# for SERVER_IP.
SERVER_MAC=02:99:01:00:00:01
PORT="${PORT:-19876}"
# The client's own EAL runtime directory, so a DPDK process already on
# the host (a node, another client) cannot hold the lock it needs.
FILE_PREFIX=melin-tap-smoke

cleanup() {
    echo ""
    echo "=== Cleanup ==="
    if [[ -n "${CLIENT_PID:-}" ]] && kill -0 "$CLIENT_PID" 2>/dev/null; then
        kill "$CLIENT_PID" 2>/dev/null || true
        wait "$CLIENT_PID" 2>/dev/null || true
        echo "  Client stopped (PID $CLIENT_PID)"
    fi
    # The TAP interface goes with the client's DPDK port.
    if [[ -n "${SERVER_PID:-}" ]]; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
        echo "  Server stopped (PID $SERVER_PID)"
    fi
    if [[ "${MOUNTED_HUGE_2M:-}" == "1" ]]; then
        umount "$HUGE_2M_MOUNT" 2>/dev/null || true
    fi
    # The build ran as root: hand target/ back, or the user's next build
    # trips over root-owned files.
    if [[ -n "${SUDO_USER:-}" ]]; then
        chown -R "$SUDO_USER:" "$PROJECT_DIR/target" 2>/dev/null || true
    fi
    rm -rf "$WORK_DIR"
}
trap cleanup EXIT

echo "============================================================"
echo "  DPDK client smoke test (TAP virtual device)"
echo "  Server: $SERVER_IP:$PORT (kernel TCP, on $TAP_IFACE)"
echo "  Client: $CLIENT_IP (DPDK, net_tap0)"
echo "  Echoes: $COUNT"
echo "============================================================"
echo ""

# --- 1. Build ---
# Two builds of one package: the server without `dpdk` (with it, the
# server would run on DPDK too), the client with it. Each binary is copied
# out as soon as it is built, since both land in the same target path.
# Before the hugepages, so a client without the flag leaves the host as it
# found it.
echo "=== Building ==="
cd "$PROJECT_DIR"
cargo build --release -p melin-example-echo --bin echo-server --quiet
cp target/release/echo-server "$WORK_DIR/echo-server"
echo "  echo-server (kernel TCP): OK"
cargo build --release -p melin-example-echo --features dpdk --bin echo-client --quiet
cp target/release/echo-client "$WORK_DIR/echo-client"
echo "  echo-client (dpdk): OK"
# Captured first: with pipefail, `grep -q` quitting early could SIGPIPE
# the client and read as a missing flag.
CLIENT_HELP=$("$WORK_DIR/echo-client" --help)
if ! grep -q -- "--transport" <<<"$CLIENT_HELP"; then
    echo "  error: this echo-client has no --transport flag. The DPDK echo client arrives" >&2
    echo "  with step 6 of docs/internal/dpdk-client-2026-10.md; this script needs it." >&2
    exit 2
fi
echo ""

# --- 2. Hugepages ---
echo "=== Hugepages ==="
HUGEPAGE_COUNT=$(cat /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages 2>/dev/null || echo 0)
if [[ "$HUGEPAGE_COUNT" -lt 256 ]]; then
    echo "  Allocating 256 x 2MB hugepages..."
    echo 256 > /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages
    HUGEPAGE_COUNT=$(cat /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages)
fi
echo "  Hugepages available: $HUGEPAGE_COUNT x 2MB"
# Some systems (ARM, Asahi among them) default to larger pages: DPDK needs
# an explicit 2 MB mount.
HUGE_2M_MOUNT=/mnt/huge_2m
if ! mount | grep -q "pagesize=2M"; then
    mkdir -p "$HUGE_2M_MOUNT"
    mount -t hugetlbfs -o pagesize=2M nodev "$HUGE_2M_MOUNT"
    MOUNTED_HUGE_2M=1
    echo "  Mounted 2MB hugetlbfs at $HUGE_2M_MOUNT"
else
    HUGE_2M_MOUNT=$(mount | grep "pagesize=2M" | awk '{print $3}' | head -1)
    echo "  2MB hugetlbfs already mounted at $HUGE_2M_MOUNT"
fi
echo ""

# --- 3. Key ---
echo "=== Key ==="
cd "$WORK_DIR"
openssl genpkey -algorithm ed25519 -out client.pem 2>/dev/null
PUB=$(openssl pkey -in client.pem -pubout -outform DER | tail -c 32 | base64)
echo "writer $PUB tap-smoke" > authorized_keys
echo "  client.pem + authorized_keys"
echo ""

# --- 4. Kernel echo server ---
echo "=== Starting the kernel echo server ==="
RUST_LOG=info "$WORK_DIR/echo-server" \
    --bind "0.0.0.0:$PORT" \
    --standalone \
    --ack-policy disk \
    --cores none \
    --no-mlock \
    --health-bind 127.0.0.1:0 \
    --journal "$WORK_DIR/echo.journal" \
    --authorized-keys "$WORK_DIR/authorized_keys" \
    > "$WORK_DIR/server.log" 2>&1 &
SERVER_PID=$!
WAIT=0
until (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null; do
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "  error: the server exited" >&2
        cat "$WORK_DIR/server.log" >&2
        exit 1
    fi
    WAIT=$((WAIT + 1))
    if [[ $WAIT -gt 40 ]]; then
        echo "  error: the server is not listening on port $PORT after 10s" >&2
        tail -30 "$WORK_DIR/server.log" >&2
        exit 1
    fi
    sleep 0.25
done
echo "  Listening on port $PORT (PID $SERVER_PID)"
echo ""

# --- 5. DPDK echo client ---
echo "=== Starting the DPDK echo client ==="
RUST_LOG=info,melin_dpdk=debug "$WORK_DIR/echo-client" \
    --transport dpdk \
    --server "$SERVER_IP:$PORT" \
    --key "$WORK_DIR/client.pem" \
    --size 64 \
    --count "$COUNT" \
    --dpdk-eal-args="--vdev=net_tap0,iface=$TAP_IFACE --no-pci --huge-dir=$HUGE_2M_MOUNT --file-prefix=$FILE_PREFIX" \
    --dpdk-ip "$CLIENT_IP" \
    --dpdk-prefix-len "$PREFIX_LEN" \
    --dpdk-peer-mac "$SERVER_MAC" \
    > "$WORK_DIR/client.out" 2> "$WORK_DIR/client.log" &
CLIENT_PID=$!

# --- 6. The TAP interface's kernel side ---
WAIT=0
while ! ip link show "$TAP_IFACE" &>/dev/null; do
    if ! kill -0 "$CLIENT_PID" 2>/dev/null; then
        echo "  error: the client exited before creating $TAP_IFACE" >&2
        cat "$WORK_DIR/client.log" >&2
        exit 1
    fi
    WAIT=$((WAIT + 1))
    if [[ $WAIT -gt 40 ]]; then
        echo "  error: $TAP_IFACE not created after 10s" >&2
        tail -30 "$WORK_DIR/client.log" >&2
        exit 1
    fi
    sleep 0.25
done
# The net_tap PMD gives the netdevice a MAC of its own just after creating
# it; a set landing in between would be overwritten. Set, then read back
# until the address holds.
WAIT=0
while true; do
    ip link set dev "$TAP_IFACE" address "$SERVER_MAC"
    sleep 0.1
    CURRENT_MAC=$(cat "/sys/class/net/$TAP_IFACE/address")
    if [[ "${CURRENT_MAC,,}" == "${SERVER_MAC,,}" ]]; then
        break
    fi
    WAIT=$((WAIT + 1))
    if [[ $WAIT -gt 20 ]]; then
        echo "  error: $TAP_IFACE kept MAC $CURRENT_MAC, not $SERVER_MAC" >&2
        exit 1
    fi
done
ip addr add "$SERVER_IP/$PREFIX_LEN" dev "$TAP_IFACE"
ip link set dev "$TAP_IFACE" up
echo "  $TAP_IFACE: $SERVER_IP/$PREFIX_LEN, MAC $SERVER_MAC (up)"
echo ""

# --- 7. Result ---
echo "=== Running ==="
CLIENT_EXIT=0
wait "$CLIENT_PID" || CLIENT_EXIT=$?
CLIENT_PID=
cat "$WORK_DIR/client.out"
echo ""
# A run of one reports its round trip, a longer one the distribution.
if [[ $CLIENT_EXIT -eq 0 ]] && grep -qE "echoes to|bytes back in" "$WORK_DIR/client.out"; then
    echo "============================================================"
    echo "  DPDK CLIENT SMOKE TEST: PASSED"
    echo "============================================================"
else
    echo "============================================================"
    echo "  DPDK CLIENT SMOKE TEST: FAILED (client exit code $CLIENT_EXIT)"
    echo "============================================================"
    echo ""
    echo "  --- Client log (last 50 lines) ---"
    tail -50 "$WORK_DIR/client.log"
    echo "  --- Server log (last 30 lines) ---"
    tail -30 "$WORK_DIR/server.log"
    exit 1
fi
