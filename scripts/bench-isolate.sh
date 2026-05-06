#!/usr/bin/env bash
# Isolate CPU cores, tune kernel, run the benchmark, then restore everything.
#
# Usage:
#   sudo ./scripts/bench-isolate.sh [bench args]
#   BENCH_PERF=1 sudo ./scripts/bench-isolate.sh [bench args]   # with perf profiling
#
# Optimizations applied:
#   1. CPU governor → performance (lock max frequency, no scaling transitions)
#   2. NMI watchdog → disabled (eliminates periodic non-maskable interrupts)
#   3. IRQ affinity → pin all interrupts to core 0 (keeps hardware interrupts
#      off pipeline/reader/bench cores)
#   4. irqbalance → stopped (prevents daemon from redistributing IRQs)
#
# Core layout: 0=OS/IRQ, 1-3=pipeline (journal/matching/response),
# 4-5=reader threads (or DPDK poll threads), 6=repl-sender, 7=event-publisher, 8=shadow, 9-10=repl-handlers, 11+=bench threads.
# All pinned via sched_setaffinity.
# All settings are saved and restored on exit (including Ctrl-C / errors).
# Kernel dmesg is captured before/after to correlate spikes with kernel events.

set -euo pipefail

if [[ $EUID -ne 0 ]]; then
    echo "error: must run as root — use sudo" >&2
    exit 1
fi

# --- Save original state ---

ORIG_GOVERNOR=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo "schedutil")
ORIG_NMI=$(cat /proc/sys/kernel/nmi_watchdog 2>/dev/null || echo "1")
ORIG_THP=$(cat /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null | grep -o '\[.*\]' | tr -d '[]' || echo "always")
ORIG_WB_CPUMASK=$(cat /sys/bus/workqueue/devices/writeback/cpumask 2>/dev/null || true)
IRQBALANCE_WAS_RUNNING=false
if systemctl is-active --quiet irqbalance 2>/dev/null; then
    IRQBALANCE_WAS_RUNNING=true
fi

# Save original IRQ affinities so we can restore them.
# Format: one line per IRQ, "irq_number original_affinity_mask".
ORIG_IRQ_AFFINITIES=$(mktemp /tmp/bench-irq-affinity.XXXXXX)
for affinity_file in /proc/irq/*/smp_affinity; do
    irq_num="${affinity_file#/proc/irq/}"
    irq_num="${irq_num%/smp_affinity}"
    mask=$(cat "$affinity_file" 2>/dev/null) || continue
    echo "${irq_num} ${mask}" >> "$ORIG_IRQ_AFFINITIES"
done

restore() {
    echo ""
    echo "=== Restoring system state ==="

    # Restore CPU governor.
    for gov in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
        [ -f "$gov" ] && echo "$ORIG_GOVERNOR" > "$gov" 2>/dev/null || true
    done
    echo "  CPU governor → ${ORIG_GOVERNOR}"

    # Restore NMI watchdog.
    echo "$ORIG_NMI" > /proc/sys/kernel/nmi_watchdog 2>/dev/null || true
    echo "  NMI watchdog → ${ORIG_NMI}"

    # Restore IRQ affinities.
    local restored=0
    while read -r irq_num mask; do
        if echo "$mask" > "/proc/irq/${irq_num}/smp_affinity" 2>/dev/null; then
            restored=$((restored + 1))
        fi
    done < "$ORIG_IRQ_AFFINITIES"
    echo "  IRQ affinities → restored ${restored} IRQs"
    rm -f "$ORIG_IRQ_AFFINITIES"

    # Restore transparent huge pages.
    echo "$ORIG_THP" > /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null || true
    echo "  THP → ${ORIG_THP}"

    # Restore writeback workqueue cpumask.
    if [[ -n "$ORIG_WB_CPUMASK" ]]; then
        echo "$ORIG_WB_CPUMASK" > /sys/bus/workqueue/devices/writeback/cpumask 2>/dev/null || true
    fi

    # Restart irqbalance if it was running.
    if $IRQBALANCE_WAS_RUNNING; then
        systemctl start irqbalance 2>/dev/null || true
        echo "  irqbalance → restarted"
    fi

    echo "=== Done ==="
}

trap restore EXIT

# --- Apply optimizations ---

# Raise fd limit for high client-count benchmarks (512 clients × 2 fds each).
ulimit -n 65536

echo "=== Applying latency optimizations ==="

# 1. CPU governor → performance (eliminates frequency scaling stalls).
echo "  CPU governor → performance (was: ${ORIG_GOVERNOR})"
for gov in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
    [ -f "$gov" ] && echo performance > "$gov"
done

# 2. Disable NMI watchdog (eliminates periodic NMI interrupts).
echo "  NMI watchdog → 0 (was: ${ORIG_NMI})"
echo 0 > /proc/sys/kernel/nmi_watchdog 2>/dev/null || true

# 3. Stop irqbalance (prevents it from redistributing IRQs after we pin them).
if $IRQBALANCE_WAS_RUNNING; then
    systemctl stop irqbalance 2>/dev/null || true
    echo "  irqbalance → stopped (was: running)"
else
    echo "  irqbalance → already stopped"
fi

# 4. Pin all IRQs to core 0. This keeps hardware interrupts (NIC, NVMe,
#    USB, etc.) off pipeline cores 1-10 and bench cores 11+. Core 0 is
#    reserved for OS work and interrupt handling.
#    smp_affinity is a hex bitmask — "1" = core 0 only.
irq_pinned=0
irq_failed=0
for affinity_file in /proc/irq/*/smp_affinity; do
    if echo 1 > "$affinity_file" 2>/dev/null; then
        irq_pinned=$((irq_pinned + 1))
    else
        irq_failed=$((irq_failed + 1))
    fi
done
echo "  IRQ affinity → pinned ${irq_pinned} IRQs to core 0 (${irq_failed} unchanged)"

# 5. Disable transparent huge pages. khugepaged compaction runs in the
#    background and can stall any core for 1-4ms while collapsing pages
#    into 2 MiB huge pages. Disabling THP eliminates this jitter source.
echo "$ORIG_THP" > /dev/null  # save was done above
echo "never" > /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null || true
echo "  THP → never (was: ${ORIG_THP})"

# 6. Pin kernel workqueues to core 0. By default, workqueues (writeback,
#    mm_percpu_wq, nvme-wq) have cpumask=ffffffff and can schedule kworker
#    threads on isolated cores. Pin them to core 0 to keep kernel background
#    work off pipeline cores. Only pin unbound workqueues (bound per-CPU
#    workqueues like events_highpri cannot be reaffined).
wq_pinned=0
for cpumask_file in /sys/bus/workqueue/devices/*/cpumask; do
    if echo 1 > "$cpumask_file" 2>/dev/null; then
        wq_pinned=$((wq_pinned + 1))
    fi
done
echo "  workqueue affinity → pinned ${wq_pinned} workqueues to core 0"

# --- Report kernel boot tuning (read-only, set via GRUB) ---

echo ""
echo "=== Kernel boot tuning (see scripts/grub-bench.conf) ==="

isolated=$(cat /sys/devices/system/cpu/isolated 2>/dev/null || true)
if [[ -n "$isolated" ]]; then
    echo "  isolcpus: ${isolated}"
else
    echo "  isolcpus: (not set)"
fi

nohz=$(cat /sys/devices/system/cpu/nohz_full 2>/dev/null || true)
if [[ -n "$nohz" ]]; then
    echo "  nohz_full: ${nohz}"
else
    echo "  nohz_full: (not set)"
fi

if grep -q 'rcu_nocbs=' /proc/cmdline 2>/dev/null; then
    rcu_nocbs=$(grep -o 'rcu_nocbs=[^ ]*' /proc/cmdline)
    echo "  ${rcu_nocbs}"
else
    echo "  rcu_nocbs: (not set)"
fi

# --- Capture SMI count before benchmark ---
# SMIs (System Management Interrupts) are firmware-level interrupts that
# cannot be disabled from userspace. They pause the CPU for 50-200 µs,
# showing up as unexplained max latency spikes. The IA32_SMI_COUNT MSR
# (0x34) counts total SMIs since boot — we diff before/after to detect
# SMIs during the benchmark.
SMI_BEFORE=""
SMI_AFTER=""
echo ""
echo "=== SMI tracking ==="
if command -v rdmsr &>/dev/null; then
    modprobe msr 2>/dev/null || true
    # Read from CPU 0 explicitly — rdmsr defaults to all CPUs which can fail.
    SMI_BEFORE=$(rdmsr -p 0 0x34 2>/dev/null || true)
    if [[ -n "$SMI_BEFORE" ]]; then
        echo "  SMI count before: ${SMI_BEFORE} (IA32_SMI_COUNT MSR 0x34, CPU 0)"
    else
        echo "  (failed to read MSR 0x34 — check: ls /dev/cpu/0/msr)"
    fi
else
    echo "  (skipped — install msr-tools: sudo apt install msr-tools)"
fi

# --- Capture dmesg before benchmark ---

DMESG_BEFORE=$(mktemp /tmp/bench-dmesg-before.XXXXXX)
DMESG_AFTER=$(mktemp /tmp/bench-dmesg-after.XXXXXX)
dmesg --time-format iso > "$DMESG_BEFORE"

# --- Start perf profiling if requested ---
# Captures kernel-level activity on pipeline/reader/bench cores (1+) during
# the benchmark. Helps identify periodic kernel interrupts (khugepaged,
# vmstat, kworker) that cause tail latency spikes.
# WARNING: perf sampling itself introduces NMI-like interrupts that degrade
# latency (~20% throughput drop). Only enable for diagnosis, not for
# publishable benchmark numbers.
PERF_DATA=""
PERF_PID=""
if [[ "${BENCH_PERF:-0}" == "1" ]] && command -v perf &>/dev/null; then
    PERF_DATA=$(mktemp /tmp/bench-perf.XXXXXX.data)
    # Sample kernel + userspace stacks on cores 1+ at 997 Hz (prime to
    # avoid aliasing). --call-graph=dwarf walks unwind info, so it
    # produces correct Rust userspace stacks even under release LTO
    # (frame pointers aren't emitted there). Pairs with the
    # CARGO_PROFILE_RELEASE_DEBUG=line-tables-only build flag below to
    # get function + line resolution. dwarf stacks are larger; bump
    # --mmap-pages so the kernel ring buffer doesn't drop samples
    # under load.
    perf record -a -g --call-graph=dwarf,16384 --mmap-pages=512 -F 997 -C 1-15 -o "$PERF_DATA" &
    PERF_PID=$!
    echo "=== Perf profiling ==="
    echo "  Recording kernel activity on cores 1-15 (PID ${PERF_PID})"
    echo "  WARNING: perf sampling degrades latency — results are diagnostic only"
    echo ""
fi

echo "=== Running benchmark ==="
echo ""

# Run as the invoking user (SUDO_USER), not root.
# No taskset — all threads self-pin via sched_setaffinity:
# cores 1-3 pipeline, 4=DPDK poll (or 4-5 TCP readers), 6=repl-sender, 7=event-publisher, 8=shadow, 9+ bench threads.
#
# CARGO_PROFILE_RELEASE_DEBUG=line-tables-only emits .debug_line so
# perf can map sample addresses back to function + source line under
# release LTO. Cheap on binary size (~2 MB), no perf cost. Pairs with
# `perf record --call-graph=dwarf` above.
CARGO_BIN="$(sudo -u "${SUDO_USER}" bash -lc 'which cargo')"
sudo -u "${SUDO_USER}" \
    CARGO_PROFILE_RELEASE_DEBUG=line-tables-only \
    "$CARGO_BIN" run --release --bin melin-bench "$@"

# --- Stop perf and show summary ---
if [[ -n "$PERF_PID" ]]; then
    kill -INT "$PERF_PID" 2>/dev/null || true
    wait "$PERF_PID" 2>/dev/null || true
    # `perf record` writes the data file as root; chown to the
    # invoking user so follow-up `perf report` calls don't need sudo.
    chown "${SUDO_UID:-0}:${SUDO_GID:-0}" "$PERF_DATA" 2>/dev/null || true
    echo ""
    echo "=== Perf summary (cores 1-15, kernel + userspace) ==="
    # Show top functions by overhead. `--no-children` flattens to leaf
    # samples (better for spotting userspace hotspots than the
    # accumulated-children view). `--inline` resolves inlined frames
    # using DWARF — important under release LTO where most small
    # functions vanish into their callers without it.
    perf report -i "$PERF_DATA" --stdio --no-children --inline --percent-limit=0.5 2>/dev/null \
        | head -40 || echo "  (perf report failed)"
    echo ""
    echo "  Full report: perf report -i ${PERF_DATA}"
    echo "  (file preserved for manual inspection)"
fi

# --- Check SMI count after benchmark ---

if [[ -n "$SMI_BEFORE" ]]; then
    SMI_AFTER=$(rdmsr -p 0 0x34 2>/dev/null || true)
    if [[ -n "$SMI_AFTER" ]]; then
        # MSR values are hex — convert to decimal for diff.
        smi_before_dec=$((16#${SMI_BEFORE}))
        smi_after_dec=$((16#${SMI_AFTER}))
        smi_delta=$((smi_after_dec - smi_before_dec))
        echo ""
        echo "=== SMI report ==="
        echo "  SMI count after:  ${SMI_AFTER}"
        if [[ $smi_delta -gt 0 ]]; then
            echo "  *** ${smi_delta} SMI(s) fired during benchmark ***"
            echo "  Each SMI pauses the CPU for ~50-200 µs (firmware-level, cannot be disabled)."
            echo "  This likely explains max latency spikes."
        else
            echo "  No SMIs detected during benchmark."
        fi
    fi
fi

# --- Capture dmesg after benchmark and show diff ---

dmesg --time-format iso > "$DMESG_AFTER"

echo ""
echo "=== Kernel messages during benchmark ==="
# Show only new lines that appeared during the run.
if diff_output=$(diff "$DMESG_BEFORE" "$DMESG_AFTER" | grep '^>' | sed 's/^> //'); then
    if [[ -n "$diff_output" ]]; then
        echo "$diff_output"
    else
        echo "  (none)"
    fi
else
    echo "  (none)"
fi

rm -f "$DMESG_BEFORE" "$DMESG_AFTER"
