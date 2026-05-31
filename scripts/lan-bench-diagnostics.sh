#!/usr/bin/env bash
###############################################################################
# ⚠  UNMAINTAINED — DOES NOT CURRENTLY RUN  ⚠
#
# This wrapper is NO LONGER MAINTAINED. It invokes lan-bench.sh, which has
# been removed, so it will fail if run as-is. The maintained benchmark path
# is lan-bench-suite.sh (or docker-bench-suite.sh for containers).
#
# It is kept ONLY in case the per-run diagnostic capture below is needed
# again later. If you revive it, repoint the invocation at lan-bench-suite.sh.
###############################################################################
#
# Run a LAN benchmark while capturing system diagnostics to help identify
# the source of tail latency spikes.
#
# Wraps lan-bench.sh with additional remote data collection on the
# target machine (server, bench, or both):
#   - Kernel boot params (isolcpus, nohz_full, rcu_nocbs)
#   - CPU governor, NMI watchdog, THP state
#   - IRQ affinity for monitored cores
#   - /proc/interrupts before and after (diff shows which IRQs fired)
#   - dmesg before and after (kernel events during bench)
#   - perf sched record on monitored cores (optional, PERF=1)
#   - SMI count via MSR 0x34 (Intel only)
#   - Workqueue and kthread placement on monitored cores
#
# Usage:
#   ./scripts/lan-bench-diagnostics.sh <server-pub-ip> <bench-pub-ip> <server-vlan-ip> [user] [-- server-args... -- bench-args...]
#
# Environment variables:
#   DIAG_TARGET=server|bench|both  Which machine to diagnose (default: server)
#   PERF=1              Enable perf sched recording on target (adds overhead)
#   PIPELINE_CORES=1-3  Server cores to monitor (default: 1-3)
#   BENCH_CORES=1-4     Bench cores to monitor (default: 1-4)
#   BENCH_BRANCH=<ref>  Checkout a specific branch on all machines
#
# Examples:
#   ./scripts/lan-bench-diagnostics.sh 1.2.3.4 5.6.7.8 10.0.0.1
#   DIAG_TARGET=bench ./scripts/lan-bench-diagnostics.sh 1.2.3.4 5.6.7.8 10.0.0.1
#   DIAG_TARGET=both PERF=1 ./scripts/lan-bench-diagnostics.sh 1.2.3.4 5.6.7.8 10.0.0.1
#
# Output:
#   Results and diagnostics saved to /tmp/lan-bench-latency-<timestamp>/

set -euo pipefail

# Loud runtime notice — this script is unmaintained (see header banner).
# Printed to stderr so it surfaces even if stdout is redirected/quiet.
cat >&2 <<'UNMAINTAINED'
###############################################################################
# WARNING: lan-bench-diagnostics.sh is UNMAINTAINED and likely broken — it
# wraps lan-bench.sh, which has been removed. Maintained path: lan-bench-suite.sh
# (or docker-bench-suite.sh for containers). Kept only for possible later revival.
###############################################################################
UNMAINTAINED

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DIAG_TARGET="${DIAG_TARGET:-server}"
PIPELINE_CORES="${PIPELINE_CORES:-1-3}"
BENCH_CORES="${BENCH_CORES:-1-4}"
PERF="${PERF:-0}"
RESULTS_DIR="/tmp/lan-bench-latency-$(date +%Y%m%d-%H%M%S)"
mkdir -p "${RESULTS_DIR}"

# Validate DIAG_TARGET.
case "$DIAG_TARGET" in
    server|bench|both) ;;
    *) echo "error: DIAG_TARGET must be server, bench, or both (got: ${DIAG_TARGET})" >&2; exit 1 ;;
esac

# ---------------------------------------------------------------------------
# Parse arguments (same format as lan-bench.sh)
# ---------------------------------------------------------------------------
POSITIONAL=()
PASS_THROUGH=()
found_separator=false

for arg in "$@"; do
    if $found_separator; then
        PASS_THROUGH+=("$arg")
    elif [[ "$arg" == "--" ]]; then
        found_separator=true
        PASS_THROUGH+=("$arg")
    else
        POSITIONAL+=("$arg")
    fi
done

if [[ ${#POSITIONAL[@]} -lt 3 ]]; then
    echo "usage: $0 <server-public-ip> <bench-public-ip> <server-vlan-ip> [user] [-- server-args... -- bench-args...]"
    echo ""
    echo "Wraps lan-bench.sh with system diagnostic capture."
    echo "Results saved to /tmp/lan-bench-latency-<timestamp>/"
    echo ""
    echo "Environment variables:"
    echo "  DIAG_TARGET=server|bench|both  Which machine to diagnose (default: server)"
    echo "  PERF=1              Enable perf sched recording on target"
    echo "  PIPELINE_CORES=1-3  Server cores to monitor (default: 1-3)"
    echo "  BENCH_CORES=1-4     Bench cores to monitor (default: 1-4)"
    echo "  BENCH_BRANCH=<ref>  Checkout a specific branch"
    exit 1
fi

SERVER_PUB="${POSITIONAL[0]}"
BENCH_PUB="${POSITIONAL[1]}"
SERVER_VLAN="${POSITIONAL[2]}"
SSH_USER="${POSITIONAL[3]:-root}"

SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
SERVER="${SSH_USER}@${SERVER_PUB}"
BENCH="${SSH_USER}@${BENCH_PUB}"

diag_server() { [[ "$DIAG_TARGET" == "server" || "$DIAG_TARGET" == "both" ]]; }
diag_bench()  { [[ "$DIAG_TARGET" == "bench"  || "$DIAG_TARGET" == "both" ]]; }

echo "============================================================"
echo "  Latency Diagnostic Benchmark"
echo "  Server:  ${SERVER_PUB} (VLAN: ${SERVER_VLAN})"
echo "  Bench:   ${BENCH_PUB}"
echo "  Target:  ${DIAG_TARGET}"
if diag_server; then echo "  Server cores: ${PIPELINE_CORES}"; fi
if diag_bench;  then echo "  Bench cores:  ${BENCH_CORES}"; fi
echo "  Perf:    ${PERF}"
echo "  Results: ${RESULTS_DIR}"
echo "============================================================"
echo ""

# ---------------------------------------------------------------------------
# Helper: capture system state on a remote host
# ---------------------------------------------------------------------------
capture_system_state() {
    local host="$1" label="$2" cores="$3" prefix="$4"

    echo "  [${label}] Capturing system state (cores ${cores})..."
    ssh $SSH_OPTS "$host" "bash -s" <<'REMOTE_DIAG' > "${RESULTS_DIR}/${prefix}-system-state.txt"
echo "=== Kernel boot params ==="
cat /proc/cmdline
echo ""

echo "=== CPU info ==="
lscpu | grep -E 'Model name|CPU\(s\)|Thread|Core|MHz|cache'
echo ""

echo "=== Isolated CPUs ==="
echo -n "  isolcpus: "; cat /sys/devices/system/cpu/isolated 2>/dev/null || echo "(not set)"
echo -n "  nohz_full: "; cat /sys/devices/system/cpu/nohz_full 2>/dev/null || echo "(not set)"
grep -o 'rcu_nocbs=[^ ]*' /proc/cmdline 2>/dev/null || echo "  rcu_nocbs: (not set)"
echo ""

echo "=== CPU governor ==="
for gov in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
    core="${gov#/sys/devices/system/cpu/cpu}"
    core="${core%/cpufreq/scaling_governor}"
    echo "  cpu${core}: $(cat "$gov" 2>/dev/null || echo 'N/A')"
done
echo ""

echo "=== NMI watchdog ==="
cat /proc/sys/kernel/nmi_watchdog 2>/dev/null || echo "N/A"
echo ""

echo "=== Transparent Huge Pages ==="
cat /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null || echo "N/A"
echo ""

echo "=== irqbalance ==="
systemctl is-active irqbalance 2>/dev/null || echo "inactive"
echo ""

echo "=== Workqueue cpumask (writeback) ==="
cat /sys/bus/workqueue/devices/writeback/cpumask 2>/dev/null || echo "N/A"
echo ""
REMOTE_DIAG

    ssh $SSH_OPTS "$host" "bash -s ${cores}" <<'REMOTE_IRQ' > "${RESULTS_DIR}/${prefix}-irq-affinity.txt"
CORES="$1"
echo "=== IRQ affinity (checking for IRQs on cores ${CORES}) ==="
echo ""
IFS='-' read -r lo hi <<< "$CORES"
hi="${hi:-$lo}"
echo "Monitoring cores: ${lo}-${hi}"
echo ""
echo "IRQs with affinity overlapping monitored cores:"
for f in /proc/irq/*/smp_affinity_list; do
    irq="${f#/proc/irq/}"
    irq="${irq%/smp_affinity_list}"
    list=$(cat "$f" 2>/dev/null) || continue
    for c in $(seq "$lo" "$hi"); do
        if echo "$list" | grep -qE "(^|,)${c}(-|,|$)"; then
            action=$(cat "/proc/irq/${irq}/actions" 2>/dev/null || echo "?")
            echo "  IRQ ${irq}: affinity=${list} action=${action}"
            break
        fi
    done
done
REMOTE_IRQ

    ssh $SSH_OPTS "$host" "cat /proc/interrupts" > "${RESULTS_DIR}/${prefix}-interrupts-before.txt"

    ssh $SSH_OPTS "$host" "bash -s ${cores}" <<'REMOTE_PS' > "${RESULTS_DIR}/${prefix}-processes-on-cores.txt"
IFS='-' read -r lo hi <<< "$1"
hi="${hi:-$lo}"
echo "=== Processes running on cores ${lo}-${hi} ==="
ps -eo pid,psr,comm | awk -v lo="$lo" -v hi="$hi" '$2 >= lo && $2 <= hi { print }'
REMOTE_PS
}

# ---------------------------------------------------------------------------
# Helper: capture post-benchmark interrupt diff
# ---------------------------------------------------------------------------
capture_post_state() {
    local host="$1" label="$2" cores="$3" prefix="$4"

    ssh $SSH_OPTS "$host" "cat /proc/interrupts" > "${RESULTS_DIR}/${prefix}-interrupts-after.txt"

    python3 - "${RESULTS_DIR}/${prefix}-interrupts-before.txt" "${RESULTS_DIR}/${prefix}-interrupts-after.txt" "$cores" > "${RESULTS_DIR}/${prefix}-interrupts-diff.txt" 2>/dev/null <<'PYDIFF' || true
import sys

def parse_interrupts(path):
    with open(path) as f:
        lines = f.readlines()
    rows = {}
    for line in lines[1:]:
        parts = line.split()
        if not parts:
            continue
        irq = parts[0].rstrip(':')
        counts = []
        for p in parts[1:]:
            if p.isdigit():
                counts.append(int(p))
            else:
                break
        action = ' '.join(parts[1+len(counts):])
        rows[irq] = (counts, action)
    return rows

lo, hi = sys.argv[3].split('-') if '-' in sys.argv[3] else (sys.argv[3], sys.argv[3])
lo, hi = int(lo), int(hi)

rows_b = parse_interrupts(sys.argv[1])
rows_a = parse_interrupts(sys.argv[2])

print(f"IRQ interrupts on cores {lo}-{hi} during benchmark:")
print(f"{'IRQ':<8} {'Count':>10}  {'Action'}")
print("-" * 60)

total = 0
results = []
for irq in rows_a:
    if irq not in rows_b:
        continue
    counts_b, action = rows_b[irq]
    counts_a, _ = rows_a[irq]
    delta = 0
    for c in range(lo, hi + 1):
        if c < len(counts_a) and c < len(counts_b):
            delta += counts_a[c] - counts_b[c]
    if delta > 0:
        results.append((delta, irq, action))
        total += delta

results.sort(reverse=True)
for delta, irq, action in results:
    print(f"{irq:<8} {delta:>10}  {action}")
print("-" * 60)
print(f"{'TOTAL':<8} {total:>10}")
PYDIFF

    echo "  [${label}] Interrupt diff (cores ${cores}):"
    cat "${RESULTS_DIR}/${prefix}-interrupts-diff.txt"
    echo ""
}

# ---------------------------------------------------------------------------
# Helper: capture dmesg diff
# ---------------------------------------------------------------------------
capture_dmesg_diff() {
    local host="$1" label="$2" prefix="$3" before_lines="$4"

    local after_lines
    after_lines=$(ssh $SSH_OPTS "$host" "dmesg | wc -l")
    local new_lines=$((after_lines - before_lines))
    if [[ $new_lines -gt 0 ]]; then
        ssh $SSH_OPTS "$host" "dmesg --time-format iso 2>/dev/null || dmesg" | tail -n "$new_lines" > "${RESULTS_DIR}/${prefix}-dmesg-diff.txt"
        echo "=== ${label} kernel messages (${new_lines} new lines) ==="
        cat "${RESULTS_DIR}/${prefix}-dmesg-diff.txt"
    else
        echo "=== ${label} kernel messages: (none) ==="
    fi
    echo ""
}

# ---------------------------------------------------------------------------
# Helper: collect perf results
# ---------------------------------------------------------------------------
collect_perf() {
    local host="$1" label="$2" pid="$3" prefix="$4"
    if [[ -z "$pid" ]]; then return; fi

    echo "  Stopping ${label} perf..."
    ssh $SSH_OPTS "$host" "kill -INT ${pid} 2>/dev/null; sleep 2"

    ssh $SSH_OPTS "$host" "perf sched latency -i /tmp/bench-perf-sched.data --sort max 2>/dev/null | head -40" \
        > "${RESULTS_DIR}/${prefix}-perf-sched-latency.txt" 2>/dev/null || true
    echo "  [${label}] perf sched latency:"
    cat "${RESULTS_DIR}/${prefix}-perf-sched-latency.txt"
    echo ""

    scp $SSH_OPTS -q "$host":/tmp/bench-perf-sched.data "${RESULTS_DIR}/${prefix}-perf-sched.data" 2>/dev/null || true
}

# ---------------------------------------------------------------------------
# 1. Capture pre-benchmark system state
# ---------------------------------------------------------------------------
echo "=== Capturing pre-benchmark system state ==="
if diag_server; then
    capture_system_state "$SERVER" "server" "$PIPELINE_CORES" "server"
    SERVER_DMESG_BEFORE=$(ssh $SSH_OPTS "$SERVER" "dmesg | wc -l")
fi
if diag_bench; then
    capture_system_state "$BENCH" "bench" "$BENCH_CORES" "bench"
    BENCH_DMESG_BEFORE=$(ssh $SSH_OPTS "$BENCH" "dmesg | wc -l")
fi

# SMI count (Intel MSR 0x34) — server only.
SMI_BEFORE=""
if diag_server; then
    SMI_BEFORE=$(ssh $SSH_OPTS "$SERVER" "modprobe msr 2>/dev/null; rdmsr -p 0 0x34 2>/dev/null || true")
    if [[ -n "$SMI_BEFORE" ]]; then
        echo "  Server SMI count before: ${SMI_BEFORE}"
    else
        echo "  Server SMI count: (MSR 0x34 not readable — AMD or no msr-tools)"
    fi
fi
echo ""

# ---------------------------------------------------------------------------
# 2. Start perf sched recording if requested
# ---------------------------------------------------------------------------
SERVER_PERF_PID=""
BENCH_PERF_PID=""
if [[ "$PERF" == "1" ]]; then
    echo "=== Starting perf sched record ==="
    echo "  WARNING: perf sampling adds overhead — results are diagnostic only"
    if diag_server; then
        ssh $SSH_OPTS "$SERVER" "nohup perf sched record -C ${PIPELINE_CORES} -o /tmp/bench-perf-sched.data -- sleep 300 >/dev/null 2>&1 </dev/null &
            echo \$!" > "${RESULTS_DIR}/server-perf-pid.txt"
        SERVER_PERF_PID=$(cat "${RESULTS_DIR}/server-perf-pid.txt")
        echo "  Server perf PID: ${SERVER_PERF_PID} (cores ${PIPELINE_CORES})"
    fi
    if diag_bench; then
        ssh $SSH_OPTS "$BENCH" "nohup perf sched record -C ${BENCH_CORES} -o /tmp/bench-perf-sched.data -- sleep 300 >/dev/null 2>&1 </dev/null &
            echo \$!" > "${RESULTS_DIR}/bench-perf-pid.txt"
        BENCH_PERF_PID=$(cat "${RESULTS_DIR}/bench-perf-pid.txt")
        echo "  Bench perf PID: ${BENCH_PERF_PID} (cores ${BENCH_CORES})"
    fi
    echo ""
fi

# ---------------------------------------------------------------------------
# 3. Run the actual benchmark via lan-bench.sh
# ---------------------------------------------------------------------------
echo "=== Running benchmark ==="
echo ""

"${SCRIPT_DIR}/lan-bench.sh" "${POSITIONAL[@]}" "${PASS_THROUGH[@]+"${PASS_THROUGH[@]}"}"

echo ""

# ---------------------------------------------------------------------------
# 4. Capture post-benchmark diagnostics
# ---------------------------------------------------------------------------
echo "=== Capturing post-benchmark diagnostics ==="
echo ""

if diag_server; then
    capture_post_state "$SERVER" "server" "$PIPELINE_CORES" "server"
    capture_dmesg_diff "$SERVER" "Server" "server" "$SERVER_DMESG_BEFORE"
fi
if diag_bench; then
    capture_post_state "$BENCH" "bench" "$BENCH_CORES" "bench"
    capture_dmesg_diff "$BENCH" "Bench" "bench" "$BENCH_DMESG_BEFORE"
fi

# SMI count after.
if [[ -n "$SMI_BEFORE" ]]; then
    SMI_AFTER=$(ssh $SSH_OPTS "$SERVER" "rdmsr -p 0 0x34 2>/dev/null || true")
    if [[ -n "$SMI_AFTER" ]]; then
        smi_before_dec=$((16#${SMI_BEFORE}))
        smi_after_dec=$((16#${SMI_AFTER}))
        smi_delta=$((smi_after_dec - smi_before_dec))
        echo "=== SMI report ==="
        if [[ $smi_delta -gt 0 ]]; then
            echo "  *** ${smi_delta} SMI(s) fired during benchmark ***"
            echo "  Each SMI pauses the CPU for ~50-200 µs."
        else
            echo "  No SMIs detected during benchmark."
        fi
        echo ""
    fi
fi

# ---------------------------------------------------------------------------
# 5. Stop perf and collect results
# ---------------------------------------------------------------------------
if [[ "$PERF" == "1" ]]; then
    echo "=== Collecting perf results ==="
    if diag_server; then collect_perf "$SERVER" "server" "$SERVER_PERF_PID" "server"; fi
    if diag_bench;  then collect_perf "$BENCH" "bench" "$BENCH_PERF_PID" "bench"; fi
fi

# ---------------------------------------------------------------------------
# 6. Copy benchmark results and generate stability plot
# ---------------------------------------------------------------------------
if [[ -f /tmp/lan-bench-results.json ]]; then
    cp /tmp/lan-bench-results.json "${RESULTS_DIR}/bench-results.json"
    echo "=== Generating latency stability plot ==="
    if cargo build --release -p melin-bench --features plot 2>/dev/null; then
        ./target/release/melin-plot stability \
            -o "${RESULTS_DIR}/latency-stability.svg" \
            "${RESULTS_DIR}/bench-results.json" 2>/dev/null \
            && echo "  Plot → ${RESULTS_DIR}/latency-stability.svg" \
            || echo "  (plot generation failed)"
    else
        echo "  (skipped — melin-plot build failed)"
    fi
    echo ""
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo "============================================================"
echo "  Diagnostics complete. Results in ${RESULTS_DIR}/"
echo "============================================================"
find "${RESULTS_DIR}" -type f | sort
