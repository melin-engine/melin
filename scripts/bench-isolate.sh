#!/usr/bin/env bash
# Isolate CPU cores, tune kernel, run the benchmark, then restore everything.
#
# Usage:
#   sudo ./scripts/bench-isolate.sh [bench args]
#
# Optimizations applied:
#   1. CPU governor → performance (lock max frequency, no scaling transitions)
#   2. NMI watchdog → disabled (eliminates periodic non-maskable interrupts)
#   3. IRQ affinity → pin all interrupts to core 0 (keeps hardware interrupts
#      off pipeline/reader/bench cores)
#   4. irqbalance → stopped (prevents daemon from redistributing IRQs)
#
# Core layout: 0=OS/IRQ, 1-3=pipeline (journal/matching/response),
# 4-5=reader threads, 6+=bench threads. All pinned via sched_setaffinity.
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

    # Restart irqbalance if it was running.
    if $IRQBALANCE_WAS_RUNNING; then
        systemctl start irqbalance 2>/dev/null || true
        echo "  irqbalance → restarted"
    fi

    echo "=== Done ==="
}

trap restore EXIT

# --- Apply optimizations ---

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
#    USB, etc.) off pipeline cores 1-5 and bench cores 6+. Core 0 is
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

# --- Capture dmesg before benchmark ---

DMESG_BEFORE=$(mktemp /tmp/bench-dmesg-before.XXXXXX)
DMESG_AFTER=$(mktemp /tmp/bench-dmesg-after.XXXXXX)
dmesg --time-format iso > "$DMESG_BEFORE"

echo "=== Running benchmark ==="
echo ""

# Run as the invoking user (SUDO_USER), not root.
# No taskset — all threads self-pin via sched_setaffinity:
# cores 1-3 pipeline, 4-5 readers, 6+ bench threads.
CARGO_BIN="$(sudo -u "${SUDO_USER}" bash -lc 'which cargo')"
sudo -u "${SUDO_USER}" \
    "$CARGO_BIN" run --release -p trading-bench "$@"

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
