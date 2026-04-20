#!/usr/bin/env bash
# Profile a remote process with perf. Designed to run alongside an
# already-driving bench — kick off `lan-bench-suite.sh` in one terminal,
# run this in another once the bench is in steady state.
#
# Usage:
#   ./scripts/profile-remote.sh <host> [seconds] [pgrep-expr]
#
# Arguments:
#   host         SSH target (e.g. root@84.32.70.221 or a name from ~/.ssh/config).
#   seconds      Sampling duration. Default 20.
#   pgrep-expr   Process basename matched with `pgrep -x` (exact match on
#                comm, not the full command line). Default: `melin-server`.
#
# Examples:
#   ./scripts/profile-remote.sh root@10.0.0.5           # replica receiver, 20 s
#   ./scripts/profile-remote.sh root@10.0.0.1 30       # primary, 30 s
#   ./scripts/profile-remote.sh root@10.0.0.5 20 melin-bench
#
# Output:
#   Prints the top-60 hot stacks (perf report --stdio) inline, and leaves
#   the raw perf.data on the remote host at /tmp/profile-remote-<ts>.data
#   for deeper inspection (`ssh <host> perf report -i <that-path>`).
#
# Notes:
#   - Samples at 997 Hz (prime, avoids aliasing with 1 kHz timers).
#   - Frame-pointer call graphs. Rust release builds usually have these;
#     if the report looks flat, rebuild with CARGO_PROFILE_RELEASE_DEBUG=line-tables-only
#     or RUSTFLAGS="-C force-frame-pointers=yes".
#   - perf sampling itself adds NMI-like load and can skew a tight busy
#     loop by single-digit percent — fine for finding hot functions,
#     don't quote throughput from a profiled run.

set -euo pipefail

if [[ $# -lt 1 ]]; then
    cat <<'USAGE' >&2
usage: profile-remote.sh <host> [seconds] [pgrep-expr]

  host         SSH target (e.g. root@10.0.0.5)
  seconds      Sampling duration (default: 20)
  pgrep-expr   pgrep -x basename (default: melin-server)
USAGE
    exit 1
fi

HOST="$1"
SECONDS_="${2:-20}"
PGREP_EXPR="${3:-melin-server}"

SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"

# Single SSH session does everything — lets `set -e` abort the remote
# shell on a missing tool / no process / perf failure without leaving
# partial state behind.
ssh $SSH_OPTS "$HOST" "SECONDS_=${SECONDS_} PGREP_EXPR='${PGREP_EXPR}' bash -s" <<'REMOTE'
set -euo pipefail

if ! command -v perf >/dev/null; then
    echo "error: perf not installed on $(hostname)" >&2
    echo "  install: apt install -y linux-perf  (or linux-tools-generic)" >&2
    exit 1
fi

PID=$(pgrep -x "$PGREP_EXPR" | head -1 || true)
if [[ -z "$PID" ]]; then
    echo "error: no process matched 'pgrep -x ${PGREP_EXPR}' on $(hostname)" >&2
    echo "  tip: check 'ps -eo pid,comm | grep -i <name>' for the actual basename." >&2
    exit 1
fi
echo "profiling pid $PID ('$(ps -p "$PID" -o comm=)') for ${SECONDS_}s on $(hostname)..."

OUT="/tmp/profile-remote-$(date +%Y%m%d-%H%M%S).data"
# -g --call-graph=fp — frame-pointer stacks (cheap, accurate for Rust
#   release with default frame pointers).
# -F 997 — prime-number frequency avoids aliasing with kernel timers.
# --inherit — include threads spawned by the target.
perf record \
    -g --call-graph=fp \
    -F 997 \
    -p "$PID" --inherit \
    -o "$OUT" \
    -- sleep "$SECONDS_"

echo ""
echo "=== top stacks (>= 0.5%) ==="
# --no-children collapses nested-callee roll-ups so the hot leaf is the
# one that shows up — clearer for finding where cycles actually land.
perf report -i "$OUT" --stdio --no-children --percent-limit=0.5 | head -60
echo ""
echo "raw data: ${OUT}"
echo "deeper look: ssh ${HOSTNAME:-<host>} perf report -i ${OUT}"
REMOTE
