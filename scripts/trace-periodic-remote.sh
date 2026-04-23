#!/usr/bin/env bash
# Identify kernel events firing periodically on a remote host's CPU.
# Captures hrtimer callbacks, IRQ vectors, and softirq entries via perf
# tracepoints — then groups by callback / vector name and flags the
# ones with a tight periodic cadence (suitable for hunting 2s / 10s
# latency spikes whose source isn't visible in /proc/interrupts at
# coarse granularity).
#
# Usage:
#   ./scripts/trace-periodic-remote.sh <host> [seconds] [cpus]
#
# Arguments:
#   host     SSH target (e.g. root@10.0.0.1).
#   seconds  Trace duration. Default 60.
#   cpus     -C argument to perf (comma list / range). Default 0 (the
#            core that handles IRQs + kernel housekeeping on bench
#            setups). Use 0-4 to include bench/pipeline cores.
#
# Output:
#   Inline: event summary (top events by count) and periodicity analysis
#   for events that fired with low jitter (candidates for a clock-driven
#   spike source).
#   Raw perf.data left on the remote host at /tmp/trace-periodic-<ts>.data.
#
# Notes:
#   - perf tracepoints are cheap compared to profiling (no NMI sampling).
#     Still, don't quote latency numbers from a traced run.
#   - Kick off the bench in another terminal FIRST, wait for steady state,
#     then run this script — the spike cadence only appears under load.

set -euo pipefail

if [[ $# -lt 1 ]]; then
    cat <<'USAGE' >&2
usage: trace-periodic-remote.sh <host> [seconds] [cpus]

  host     SSH target (e.g. root@10.0.0.5)
  seconds  Trace duration (default: 60)
  cpus     perf -C argument (default: 0)
USAGE
    exit 1
fi

HOST="$1"
SECONDS_="${2:-60}"
CPUS="${3:-0}"

SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"

ssh $SSH_OPTS "$HOST" "SECONDS_=${SECONDS_} CPUS='${CPUS}' bash -s" <<'REMOTE'
set -euo pipefail

if ! command -v perf >/dev/null; then
    echo "error: perf not installed on $(hostname)" >&2
    echo "  install: apt install -y linux-perf  (or linux-tools-generic)" >&2
    exit 1
fi

OUT="/tmp/trace-periodic-$(date +%Y%m%d-%H%M%S).data"
EVENTS="timer:hrtimer_expire_entry,irq_vectors:local_timer_entry,irq_vectors:reschedule_entry,irq_vectors:irq_work_entry,irq_vectors:x86_platform_ipi_entry,irq:softirq_entry,workqueue:workqueue_execute_start"

echo "tracing ${EVENTS} on CPUs=${CPUS} for ${SECONDS_}s on $(hostname)..."
perf record -a \
    -C "$CPUS" \
    -e "$EVENTS" \
    -o "$OUT" \
    -- sleep "$SECONDS_"

echo ""
echo "=== event counts ==="
perf script -i "$OUT" --ns 2>/dev/null \
    | awk '{
        # The event name appears as "<category>:<name>:" in perf script
        # output. Extract it robustly by scanning for the first token
        # containing a colon-separated pair we know.
        for (i=1; i<=NF; i++) {
            if ($i ~ /:/ && ($i ~ /^timer:/ || $i ~ /^irq_vectors:/ || $i ~ /^irq:/ || $i ~ /^workqueue:/)) {
                name=$i; sub(/:$/, "", name)
                counts[name]++
                break
            }
        }
    }
    END {
        for (k in counts) printf "%10d  %s\n", counts[k], k
    }' | sort -rn | head -20

echo ""
echo "=== hrtimer callbacks seen (callback function by count) ==="
# perf script prints hrtimer_expire_entry with "function=<addr> <name>"
# somewhere in the trailing arguments. Extract the symbol.
perf script -i "$OUT" --ns 2>/dev/null \
    | awk '/timer:hrtimer_expire_entry/ {
        # Find the "function=" token and take the symbolic name if present
        for (i=1; i<=NF; i++) if ($i ~ /^function=/) {
            # Symbol usually follows in parentheses on the same line
            # but perf formats as "function=<hex>" when symbols unresolved.
            name=$i; sub(/function=/, "", name)
            print name
        }
    }' | sort | uniq -c | sort -rn | head -10

echo ""
echo "=== periodicity analysis (events with low-jitter interval) ==="
# For each hrtimer callback that fires 3+ times, compute mean interval
# and coefficient of variation. Low CV = clock-driven, high = load-driven.
perf script -i "$OUT" --ns 2>/dev/null \
    | awk '/timer:hrtimer_expire_entry/ {
        # perf script output has a variable leading comm column (PID may
        # or may not be appended with a dash). The [NNN] CPU bracket is
        # a stable anchor — the timestamp sits in the next field.
        ts = ""
        for (i=1; i<=NF; i++) {
            if ($i ~ /^\[[0-9]+\]$/) {
                if (i+1 <= NF) { ts = $(i+1); sub(/:$/, "", ts); ts += 0 }
                break
            }
        }
        if (ts == 0) next
        cb = ""
        for (i=1; i<=NF; i++) if ($i ~ /^function=/) { cb=$i; sub(/function=/, "", cb); break }
        if (cb == "") next
        if (!(cb in last)) { last[cb]=ts; count[cb]=1; next }
        gap = ts - last[cb]
        sum[cb] += gap
        sumsq[cb] += gap*gap
        count[cb]++
        last[cb] = ts
    }
    END {
        for (f in count) if (count[f] >= 3) {
            n = count[f] - 1
            mean = sum[f] / n
            var = sumsq[f] / n - mean*mean
            if (var < 0) var = 0
            sd = sqrt(var)
            cv = (mean > 0) ? sd/mean : 0
            printf "  %-24s  n=%-4d  mean=%.3fs  sd=%.3fs  cv=%.3f\n", f, n+1, mean, sd, cv
        }
    }' | sort -k4 -n | head -15

echo ""
echo "raw data: ${OUT}"
echo "deeper look: ssh ${HOST:-<host>} perf script -i ${OUT} | less"
REMOTE
