#!/usr/bin/env bash
# Isolate CPU cores, tune kernel, run the benchmark, then restore everything.
#
# Usage:
#   sudo ./scripts/bench-isolate.sh [bench args]
#
# Optimizations applied:
#   1. CPU governor → performance (lock max frequency, no scaling transitions)
#   2. NMI watchdog → disabled (eliminates periodic non-maskable interrupts)
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
