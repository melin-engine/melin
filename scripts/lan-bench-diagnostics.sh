#!/usr/bin/env bash
# Run a LAN benchmark while capturing system diagnostics to help identify
# the source of tail latency spikes (4ms+ max latency).
#
# Wraps lan-bench.sh with additional remote data collection on the server:
#   - Kernel boot params (isolcpus, nohz_full, rcu_nocbs)
#   - CPU governor, NMI watchdog, THP state
#   - IRQ affinity for pipeline cores
#   - /proc/interrupts before and after (diff shows which IRQs fired)
#   - dmesg before and after (kernel events during bench)
#   - perf sched record on pipeline cores (optional, PERF=1)
#   - SMI count via MSR 0x34 (Intel only)
#   - Workqueue and kthread placement on pipeline cores
#
# Usage:
#   ./scripts/lan-bench-latency.sh <server-public-ip> <bench-public-ip> <server-vlan-ip> [user] [-- server-args... -- bench-args...]
#
# Environment variables:
#   PERF=1              Enable perf sched recording on pipeline cores (adds overhead)
#   PIPELINE_CORES=1-3  Cores to monitor (default: 1-3)
#   BENCH_BRANCH=<ref>  Checkout a specific branch on all machines
#
# Examples:
#   ./scripts/lan-bench-latency.sh 84.32.70.218 84.32.70.221 10.184.12.27
#   PERF=1 ./scripts/lan-bench-latency.sh 84.32.70.218 84.32.70.221 10.184.12.27 root \
#       -- -- 10000000 --clients 16 --window 256
#
# Output:
#   Results and diagnostics saved to /tmp/lan-bench-latency-<timestamp>/

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PIPELINE_CORES="${PIPELINE_CORES:-1-3}"
PERF="${PERF:-0}"
RESULTS_DIR="/tmp/lan-bench-latency-$(date +%Y%m%d-%H%M%S)"
mkdir -p "${RESULTS_DIR}"

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
    echo "Wraps lan-bench.sh with system diagnostic capture on the server."
    echo "Results saved to /tmp/lan-bench-latency-<timestamp>/"
    echo ""
    echo "Environment variables:"
    echo "  PERF=1              Enable perf sched recording on pipeline cores"
    echo "  PIPELINE_CORES=1-3  Cores to monitor (default: 1-3)"
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

echo "============================================================"
echo "  Latency Diagnostic Benchmark"
echo "  Server:  ${SERVER_PUB} (VLAN: ${SERVER_VLAN})"
echo "  Bench:   ${BENCH_PUB}"
echo "  Cores:   ${PIPELINE_CORES}"
echo "  Perf:    ${PERF}"
echo "  Results: ${RESULTS_DIR}"
echo "============================================================"
echo ""

# ---------------------------------------------------------------------------
# 1. Capture system state BEFORE the benchmark
# ---------------------------------------------------------------------------
echo "=== Capturing pre-benchmark system state ==="

ssh $SSH_OPTS "$SERVER" "bash -s" <<'REMOTE_DIAG' > "${RESULTS_DIR}/system-state.txt"
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
echo "  System state → ${RESULTS_DIR}/system-state.txt"

# Capture IRQ affinity for pipeline cores.
ssh $SSH_OPTS "$SERVER" "bash -s ${PIPELINE_CORES}" <<'REMOTE_IRQ' > "${RESULTS_DIR}/irq-affinity.txt"
CORES="$1"
echo "=== IRQ affinity (checking for IRQs on cores ${CORES}) ==="
echo ""
# Show IRQs that have affinity including any pipeline core.
# Parse the core range into individual core numbers.
IFS='-' read -r lo hi <<< "$CORES"
hi="${hi:-$lo}"
echo "Monitoring cores: ${lo}-${hi}"
echo ""
echo "IRQs with affinity overlapping pipeline cores:"
for f in /proc/irq/*/smp_affinity_list; do
    irq="${f#/proc/irq/}"
    irq="${irq%/smp_affinity_list}"
    list=$(cat "$f" 2>/dev/null) || continue
    # Check if any pipeline core is in the affinity list.
    for c in $(seq "$lo" "$hi"); do
        if echo "$list" | grep -qE "(^|,)${c}(-|,|$)"; then
            action=$(cat "/proc/irq/${irq}/actions" 2>/dev/null || echo "?")
            echo "  IRQ ${irq}: affinity=${list} action=${action}"
            break
        fi
    done
done
REMOTE_IRQ
echo "  IRQ affinity → ${RESULTS_DIR}/irq-affinity.txt"

# Capture /proc/interrupts snapshot.
ssh $SSH_OPTS "$SERVER" "cat /proc/interrupts" > "${RESULTS_DIR}/interrupts-before.txt"
echo "  Interrupts snapshot → ${RESULTS_DIR}/interrupts-before.txt"

# Record dmesg line count so we can extract only new messages later.
DMESG_BEFORE_LINES=$(ssh $SSH_OPTS "$SERVER" "dmesg | wc -l")
echo "  dmesg baseline: ${DMESG_BEFORE_LINES} lines"

# Check for processes on pipeline cores.
ssh $SSH_OPTS "$SERVER" "bash -s ${PIPELINE_CORES}" <<'REMOTE_PS' > "${RESULTS_DIR}/processes-on-cores.txt"
IFS='-' read -r lo hi <<< "$1"
hi="${hi:-$lo}"
echo "=== Processes running on cores ${lo}-${hi} ==="
ps -eo pid,psr,comm | awk -v lo="$lo" -v hi="$hi" '$2 >= lo && $2 <= hi { print }'
REMOTE_PS
echo "  Processes on pipeline cores → ${RESULTS_DIR}/processes-on-cores.txt"

# SMI count (Intel MSR 0x34).
SMI_BEFORE=$(ssh $SSH_OPTS "$SERVER" "modprobe msr 2>/dev/null; rdmsr -p 0 0x34 2>/dev/null || true")
if [[ -n "$SMI_BEFORE" ]]; then
    echo "  SMI count before: ${SMI_BEFORE}"
else
    echo "  SMI count: (MSR 0x34 not readable — AMD or no msr-tools)"
fi

echo ""

# ---------------------------------------------------------------------------
# 2. Start perf sched recording if requested
# ---------------------------------------------------------------------------
PERF_PID=""
if [[ "$PERF" == "1" ]]; then
    echo "=== Starting perf sched record on server (cores ${PIPELINE_CORES}) ==="
    echo "  WARNING: perf sampling adds overhead — results are diagnostic only"
    ssh $SSH_OPTS "$SERVER" "nohup perf sched record -C ${PIPELINE_CORES} -o /tmp/bench-perf-sched.data -- sleep 300 >/dev/null 2>&1 </dev/null &
        echo \$!" > "${RESULTS_DIR}/perf-pid.txt"
    PERF_PID=$(cat "${RESULTS_DIR}/perf-pid.txt")
    echo "  perf PID: ${PERF_PID}"
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
# 4. Capture system state AFTER the benchmark
# ---------------------------------------------------------------------------
echo "=== Capturing post-benchmark diagnostics ==="

# /proc/interrupts after.
ssh $SSH_OPTS "$SERVER" "cat /proc/interrupts" > "${RESULTS_DIR}/interrupts-after.txt"

# Diff interrupts to show which IRQs fired during the benchmark.
# Parse the header to find pipeline core columns, then diff counts.
python3 - "${RESULTS_DIR}/interrupts-before.txt" "${RESULTS_DIR}/interrupts-after.txt" "$PIPELINE_CORES" > "${RESULTS_DIR}/interrupts-diff.txt" 2>/dev/null <<'PYDIFF' || true
import sys, re

def parse_interrupts(path):
    with open(path) as f:
        lines = f.readlines()
    header = lines[0].split()
    cpus = [h for h in header if h.startswith('CPU')]
    rows = {}
    for line in lines[1:]:
        parts = line.split()
        if not parts:
            continue
        irq = parts[0].rstrip(':')
        counts = []
        for i, p in enumerate(parts[1:]):
            if p.isdigit():
                counts.append(int(p))
            else:
                break
        action = ' '.join(parts[1+len(counts):])
        rows[irq] = (counts, action)
    return cpus, rows

lo, hi = sys.argv[3].split('-') if '-' in sys.argv[3] else (sys.argv[3], sys.argv[3])
lo, hi = int(lo), int(hi)

cpus_b, rows_b = parse_interrupts(sys.argv[1])
cpus_a, rows_a = parse_interrupts(sys.argv[2])

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
echo "  Interrupt diff → ${RESULTS_DIR}/interrupts-diff.txt"
cat "${RESULTS_DIR}/interrupts-diff.txt"
echo ""

# dmesg diff — extract only lines that appeared during the benchmark.
DMESG_AFTER_LINES=$(ssh $SSH_OPTS "$SERVER" "dmesg | wc -l")
dmesg_new=$((DMESG_AFTER_LINES - DMESG_BEFORE_LINES))
if [[ $dmesg_new -gt 0 ]]; then
    ssh $SSH_OPTS "$SERVER" "dmesg --time-format iso 2>/dev/null || dmesg" | tail -n "$dmesg_new" > "${RESULTS_DIR}/dmesg-diff.txt"
    echo "=== Kernel messages during benchmark (${dmesg_new} new lines) ==="
    cat "${RESULTS_DIR}/dmesg-diff.txt"
else
    echo "=== Kernel messages during benchmark ==="
    echo "  (none)"
fi
echo ""

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
if [[ -n "$PERF_PID" ]]; then
    echo "=== Stopping perf and collecting results ==="
    ssh $SSH_OPTS "$SERVER" "kill -INT ${PERF_PID} 2>/dev/null; sleep 2"

    # Get scheduling latency summary.
    ssh $SSH_OPTS "$SERVER" "perf sched latency -i /tmp/bench-perf-sched.data --sort max 2>/dev/null | head -40" \
        > "${RESULTS_DIR}/perf-sched-latency.txt" 2>/dev/null || true
    echo "  perf sched latency → ${RESULTS_DIR}/perf-sched-latency.txt"
    cat "${RESULTS_DIR}/perf-sched-latency.txt"
    echo ""

    # Copy raw perf data for offline analysis.
    scp $SSH_OPTS -q "$SERVER":/tmp/bench-perf-sched.data "${RESULTS_DIR}/perf-sched.data" 2>/dev/null || true
    echo "  Raw perf data → ${RESULTS_DIR}/perf-sched.data"
    echo ""
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
