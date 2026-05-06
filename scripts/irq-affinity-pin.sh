#!/usr/bin/env bash
# Pin all retargetable device IRQs to a housekeeping CPU mask.
#
# Boot-time defaults spread IRQs across cores in pairs (e.g. 4-5, 6-7,
# ...), so without intervention NIC/storage interrupts land on the cores
# we've isolated for the engine (journal=1, matching=2, response=3,
# DPDK poll=4 in the standard layout). `isolcpus` keeps the scheduler
# off those cores but does not steer IRQs — that's a separate setting.
#
# Usage:
#   scripts/irq-affinity-pin.sh [cpu_mask]
#
# `cpu_mask` is a Linux IRQ affinity bitmask in hex (default `1` = core 0).
# Some IRQs (per-CPU IPIs, IOMMU remapping, etc.) refuse affinity writes —
# those EIOs are expected and silently ignored.
set -euo pipefail
MASK="${1:-1}"
applied=0
skipped=0
for irq in /proc/irq/[0-9]*; do
    if printf '%s' "$MASK" > "$irq/smp_affinity" 2>/dev/null; then
        applied=$((applied + 1))
    else
        skipped=$((skipped + 1))
    fi
done
echo "irq-affinity-pin: mask=${MASK} applied=${applied} skipped=${skipped}"
