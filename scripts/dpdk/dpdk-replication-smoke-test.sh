#!/usr/bin/env bash
# DPDK replication smoke test: primary + replica over veth + af_packet.
#
# Verifies that the DPDK replication transport works end-to-end:
#   1. Primary starts with DPDK, seeds instruments + accounts
#   2. Replica starts with DPDK, connects to primary
#   3. Primary transitions from "halted" to "trading"
#   4. Replica's journal is non-empty (received data)
#
# No bench client — the single af_packet queue is used entirely for
# replication. Client traffic is not tested here (see dpdk-e2e-smoke-test.sh).
#
# Usage:
#   sudo ./scripts/dpdk/dpdk-replication-smoke-test.sh
#
# Prerequisites:
#   - DPDK >= 22.11 installed
#   - Must run as root (hugepages + veth creation)

set -euo pipefail

# Ensure cargo/rustup work when running under sudo.
if [[ -n "${SUDO_USER:-}" ]]; then
    REAL_HOME=$(eval echo "~$SUDO_USER")
    export PATH="$REAL_HOME/.cargo/bin:$PATH"
    export RUSTUP_HOME="${RUSTUP_HOME:-$REAL_HOME/.rustup}"
    export CARGO_HOME="${CARGO_HOME:-$REAL_HOME/.cargo}"
fi

if [[ $EUID -ne 0 ]]; then
    echo "error: must run as root (hugepages + veth creation)" >&2
    echo "usage: sudo $0" >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
TMPDIR=$(mktemp -d)

# IP configuration: primary .1, replica .2, same /24 subnet.
PRIMARY_IP="192.168.201.1"
REPLICA_IP="192.168.201.2"
PREFIX=24
REPL_PORT=9877
HEALTH_PORT=9878

# Veth pair: replica side <-> primary side.
VETH_REPLICA="dpdk-repl-r"
VETH_PRIMARY="dpdk-repl-p"

# Capture core dumps to a temp file (bypassing apport) so we can pull a
# backtrace if either process crashes (e.g. a teardown segfault). Restored
# in cleanup.
ORIG_CORE_PATTERN=$(cat /proc/sys/kernel/core_pattern 2>/dev/null || echo "core")
ulimit -c unlimited
echo "$TMPDIR/core.%e.%p" > /proc/sys/kernel/core_pattern 2>/dev/null || true

# Stop a process gracefully (SIGTERM), escalating to SIGKILL only if it hangs.
# SIGTERM exercises the real teardown path (stop -> close the DPDK port -> EAL
# cleanup, journal fdatasync, drop the exchange) — what a production operator
# triggers. Both ends are DPDK here, so both exercise it. That path has wedged
# before in two DPDK-specific ways, both fixed:
#   1. mlockall(MCL_FUTURE) eagerly + synchronously populated every post-init
#      mmap (`__mm_populate`), an uninterruptible page-walk that outlasted even
#      SIGKILL — fixed by adding MCL_ONFAULT (lock-on-fault) in process.rs.
#   2. SCHED_FIFO busy-spin pipeline threads, pinned to cores EAL also uses,
#      starved co-located SCHED_OTHER DPDK control threads holding the glibc
#      malloc arena lock — fixed by gating SCHED_FIFO on real core isolation
#      (isolcpus) in affinity.rs.
# If a process survives SIGTERM past the grace window, the dumps below pinpoint
# the regression before we resort to SIGKILL.
stop_pid() {
    local pid="$1" name="$2" log="${3:-}"
    kill -TERM "$pid" 2>/dev/null || true
    local i=0
    while kill -0 "$pid" 2>/dev/null; do
        i=$((i + 1))
        if [[ $i -gt 150 ]]; then
            echo "  WARN: $name (pid $pid) ignored SIGTERM for 15s — kernel stacks:"
            grep '^State:' "/proc/$pid/status" 2>/dev/null || true
            for t in "/proc/$pid/task"/*; do
                echo "    -- thread $(cat "$t/comm" 2>/dev/null) ($(basename "$t")) --"
                cat "$t/stack" 2>/dev/null || true
            done
            # Kernel stacks only name syscalls. To name the Rust frames — which
            # mutex / join / alloc / spin is wedged — grab userspace backtraces
            # of every thread, persisted OUTSIDE the temp dir (cleanup wipes it)
            # and echoed. Prefer eu-stack; fall back to gdb under `timeout` so it
            # can never hang the harness. The process log carries a panicked
            # pipeline thread's backtrace if that's the cause.
            if [[ -n "$log" && -f "$log" ]]; then
                echo "  --- panic lines in $log ---"
                grep -n -A8 'panicked' "$log" 2>/dev/null || echo "    (no 'panicked' found)"
                echo "  --- tail of $log ---"
                tail -40 "$log" 2>/dev/null || true
            fi
            local diag="$PROJECT_DIR/dpdk-repl-wedge-$name-$pid.txt"
            echo "  --- userspace backtraces -> $diag (also printed below) ---"
            {
                if command -v eu-stack >/dev/null 2>&1; then
                    eu-stack -p "$pid" 2>&1
                elif command -v gdb >/dev/null 2>&1; then
                    timeout 60 gdb -batch -nx -p "$pid" \
                        -ex "set pagination off" \
                        -ex "set print thread-events off" \
                        -ex "thread apply all bt" 2>&1
                else
                    echo "(neither eu-stack nor gdb installed: apt-get install elfutils gdb)"
                fi
            } | tee "$diag"
            [[ -n "${SUDO_USER:-}" ]] && chown "$SUDO_USER:$SUDO_USER" "$diag" 2>/dev/null || true
            echo "  --- end userspace backtraces ---"
            echo "  Escalating to SIGKILL..."
            kill -9 "$pid" 2>/dev/null || true
            break
        fi
        sleep 0.1
    done
    wait "$pid" 2>/dev/null || true
    echo "  $name stopped"
}

cleanup() {
    echo ""
    echo "=== Cleanup ==="

    if [[ -n "${TCPDUMP_PID:-}" ]]; then
        kill "$TCPDUMP_PID" 2>/dev/null || true
    fi
    # Replica first (clean disconnect from the primary), then the primary.
    if [[ -n "${REPLICA_PID:-}" ]]; then
        stop_pid "$REPLICA_PID" "Replica" "$TMPDIR/replica.log"
    fi
    if [[ -n "${PRIMARY_PID:-}" ]]; then
        stop_pid "$PRIMARY_PID" "Primary" "$TMPDIR/primary.log"
    fi

    ip link del "$VETH_REPLICA" 2>/dev/null || true
    echo "  Veth pair removed"

    rm -rf /var/run/dpdk/primary /var/run/dpdk/replica

    # Restore the original core_pattern.
    echo "$ORIG_CORE_PATTERN" > /proc/sys/kernel/core_pattern 2>/dev/null || true

    if [[ "${MOUNTED_HUGE_2M:-}" == "1" ]]; then
        umount "$HUGE_2M_MOUNT" 2>/dev/null || true
    fi

    if [[ -n "${SUDO_USER:-}" ]]; then
        chown -R "$SUDO_USER:$SUDO_USER" "$PROJECT_DIR/target" 2>/dev/null || true
        echo "  Restored target/ ownership to $SUDO_USER"
    fi

    rm -rf "$TMPDIR"
    echo "  Temp dir cleaned: $TMPDIR"
}
trap cleanup EXIT

echo "============================================================"
echo "  DPDK Replication Smoke Test (primary + replica, af_packet)"
echo "  Primary: $PRIMARY_IP:$REPL_PORT (on $VETH_PRIMARY)"
echo "  Replica: $REPLICA_IP (on $VETH_REPLICA)"
echo "  Temp:    $TMPDIR"
echo "============================================================"
echo ""

# --- 0. Clean stale DPDK state ---
rm -rf /var/run/dpdk/primary /var/run/dpdk/replica

# --- 1. Hugepages ---
echo "=== Hugepages ==="
HUGEPAGE_COUNT=$(cat /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages 2>/dev/null || echo 0)
if [[ "$HUGEPAGE_COUNT" -lt 512 ]]; then
    echo "  Allocating 512 x 2MB hugepages..."
    echo 512 > /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages
    HUGEPAGE_COUNT=$(cat /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages)
fi
echo "  Hugepages available: $HUGEPAGE_COUNT x 2MB"

HUGE_2M_MOUNT="/mnt/huge_2m"
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

# --- 2. Build ---
echo "=== Building ==="
cd "$PROJECT_DIR"

echo "  Building server with DPDK..."
cargo build --release -p melin-server --features dpdk --no-default-features --quiet 2>&1
echo "  server: OK"

echo "  Building keygen..."
cargo build --release --bin melin-keygen --quiet 2>&1
echo "  keygen: OK"
echo ""

# --- 3. Auth keys ---
echo "=== Auth keys ==="
cd "$TMPDIR"
"$PROJECT_DIR/target/release/melin-keygen" repl_key trader
# The DPDK primary now authenticates connecting replicas: the key must carry
# Replication permission (the primary rejects Trader/Operator/etc.).
echo "replication $(cat repl_key.pub | tr -d '\n') repl" > authorized_keys
echo "  Generated repl_key.key + authorized_keys"
echo ""

# --- 4. Create veth pair ---
echo "=== Creating veth pair ==="
ip link add "$VETH_REPLICA" type veth peer name "$VETH_PRIMARY"
# The DPDK replica skips ARP and addresses the primary by the synthetic
# 02:00:<ip-octets> MAC (the SR-IOV VF convention from dpdk-setup.sh;
# replication/dpdk.rs seeds it into smoltcp's neighbor cache). The af_packet
# PMD reports the veth iface's MAC to smoltcp as its own hardware address, so
# each end must OWN its synthetic MAC — otherwise smoltcp drops the peer's
# frames (promiscuous mode delivers them to the PMD, but smoltcp rejects a
# frame not addressed to its hardware address). Set the MAC while down.
IFS=. read -r o1 o2 o3 o4 <<< "$PRIMARY_IP"
ip link set "$VETH_PRIMARY" address "$(printf '02:00:%02x:%02x:%02x:%02x' "$o1" "$o2" "$o3" "$o4")"
IFS=. read -r o1 o2 o3 o4 <<< "$REPLICA_IP"
ip link set "$VETH_REPLICA" address "$(printf '02:00:%02x:%02x:%02x:%02x' "$o1" "$o2" "$o3" "$o4")"
ip link set "$VETH_REPLICA" up
ip link set "$VETH_PRIMARY" up
ethtool -K "$VETH_REPLICA" tx off rx off 2>/dev/null || true
ethtool -K "$VETH_PRIMARY" tx off rx off 2>/dev/null || true
echo "  $VETH_REPLICA <-> $VETH_PRIMARY (up, synthetic 02:00:<ip> MACs)"

# Capture the wire so a failure shows whether ARP resolves and the replica's
# SYNs reach the primary with the right dst MAC (the synthetic 02:00:<ip>).
TCPDUMP_LOG="$TMPDIR/veth.tcpdump"
if command -v tcpdump >/dev/null 2>&1; then
    tcpdump -i "$VETH_PRIMARY" -e -nnvv -c 200 'arp or tcp port '"$REPL_PORT" \
        > "$TCPDUMP_LOG" 2>&1 &
    TCPDUMP_PID=$!
    echo "  tcpdump capturing $VETH_PRIMARY -> $TCPDUMP_LOG (PID $TCPDUMP_PID)"
fi
echo ""

# --- 5. Start primary ---
echo "=== Starting DPDK primary ==="
RUST_LOG=info RUST_BACKTRACE=1 \
"$PROJECT_DIR/target/release/melin-server" \
    --bind "0.0.0.0:9876" \
    --health-bind "0.0.0.0:$HEALTH_PORT" \
    --journal "$TMPDIR/primary.journal" \
    --authorized-keys "$TMPDIR/authorized_keys" \
    --accounts 100 \
    --instruments 10 \
    --replication-bind "0.0.0.0:$REPL_PORT" \
    --dpdk-eal-args="--vdev=net_af_packet0,iface=$VETH_PRIMARY --no-pci --log-level=6 --huge-dir=$HUGE_2M_MOUNT --file-prefix=primary" \
    --dpdk-ip "$PRIMARY_IP" \
    --dpdk-prefix-len "$PREFIX" \
    > "$TMPDIR/primary.log" 2>&1 &
PRIMARY_PID=$!
echo "  Primary PID: $PRIMARY_PID"

# Wait for primary to start listening.
echo "  Waiting for primary..."
WAIT=0
while ! grep -q "DPDK replication sender started" "$TMPDIR/primary.log" 2>/dev/null; do
    sleep 0.5
    WAIT=$((WAIT + 1))
    if [[ $WAIT -gt 30 ]]; then
        echo "  ERROR: Primary not ready after 15s"
        echo "  --- Primary log ---"
        cat "$TMPDIR/primary.log"
        exit 1
    fi
    if ! kill -0 "$PRIMARY_PID" 2>/dev/null; then
        echo "  ERROR: Primary died"
        echo "  --- Primary log ---"
        cat "$TMPDIR/primary.log"
        exit 1
    fi
done
echo "  Primary ready"

# Health endpoint may not be ready yet (main thread is blocked waiting
# for replica to connect before seeding). Skip the pre-replica check.
echo ""

# --- 6. Start replica ---
echo "=== Starting DPDK replica ==="
RUST_LOG=info RUST_BACKTRACE=1 \
"$PROJECT_DIR/target/release/melin-server" \
    --journal "$TMPDIR/replica.journal" \
    --snapshot-interval-ms 0 \
    --replica-of "$PRIMARY_IP:$REPL_PORT" \
    --replication-key "$TMPDIR/repl_key.key" \
    --dpdk-eal-args="--vdev=net_af_packet0,iface=$VETH_REPLICA --no-pci --log-level=6 --huge-dir=$HUGE_2M_MOUNT --file-prefix=replica" \
    --dpdk-ip "$REPLICA_IP" \
    --dpdk-prefix-len "$PREFIX" \
    > "$TMPDIR/replica.log" 2>&1 &
REPLICA_PID=$!
echo "  Replica PID: $REPLICA_PID"

# Wait for replica to connect and start streaming.
echo "  Waiting for replica to connect..."
WAIT=0
while ! grep -q "streaming started (DPDK)" "$TMPDIR/replica.log" 2>/dev/null; do
    sleep 0.5
    WAIT=$((WAIT + 1))
    if [[ $WAIT -gt 30 ]]; then
        echo "  ERROR: Replica not streaming after 15s"
        echo "  --- Replica log ---"
        cat "$TMPDIR/replica.log"
        echo "  --- Primary log (last 20 lines) ---"
        tail -20 "$TMPDIR/primary.log"
        exit 1
    fi
    if ! kill -0 "$REPLICA_PID" 2>/dev/null; then
        echo "  ERROR: Replica died"
        echo "  --- Replica log ---"
        cat "$TMPDIR/replica.log"
        exit 1
    fi
done
echo "  Replica connected and streaming"

# Give the replica a moment to receive seeded data.
sleep 2

# --- 7. Verify ---
echo ""
echo "=== Verification ==="

# Check primary health — should now be "trading".
HEALTH=$(echo "" | nc -q1 127.0.0.1 "$HEALTH_PORT" 2>/dev/null || true)
echo "  Primary health: $HEALTH"

PASSED=true

if echo "$HEALTH" | grep -q "trading"; then
    echo "  PASS: primary is trading (replica connected)"
else
    echo "  FAIL: expected 'trading' status"
    PASSED=false
fi

# Check replica journal exists and is non-empty.
if [[ -f "$TMPDIR/replica.journal" ]]; then
    REPLICA_SIZE=$(stat -c%s "$TMPDIR/replica.journal")
    echo "  Replica journal: $REPLICA_SIZE bytes"
    if [[ "$REPLICA_SIZE" -gt 100 ]]; then
        echo "  PASS: replica journal has data"
    else
        echo "  FAIL: replica journal too small ($REPLICA_SIZE bytes)"
        PASSED=false
    fi
else
    echo "  FAIL: replica journal not found"
    PASSED=false
fi

# Check primary is still alive.
if kill -0 "$PRIMARY_PID" 2>/dev/null; then
    echo "  PASS: primary still running"
else
    echo "  FAIL: primary died"
    PASSED=false
fi

# Check replica is still alive.
if kill -0 "$REPLICA_PID" 2>/dev/null; then
    echo "  PASS: replica still running"
else
    echo "  FAIL: replica died"
    PASSED=false
fi

echo ""
if [[ "$PASSED" == "true" ]]; then
    echo "============================================================"
    echo "  DPDK REPLICATION SMOKE TEST: PASSED"
    echo "============================================================"
else
    echo "============================================================"
    echo "  DPDK REPLICATION SMOKE TEST: FAILED"
    echo "============================================================"
    echo ""
    echo "  --- Primary log (last 30 lines) ---"
    tail -30 "$TMPDIR/primary.log"
    echo ""
    echo "  --- Replica log (last 30 lines) ---"
    tail -30 "$TMPDIR/replica.log"
    exit 1
fi
