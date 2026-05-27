#!/usr/bin/env bash
# Run the README benchmarks on a LAN setup (two+ servers).
#
# Benchmarks are organized as a transport × workload matrix:
#
#   Transports (how the server runs):
#     tcp             Kernel TCP, standalone (no replication)
#     tcp-repl        Kernel TCP + 1 synchronous replica
#     tcp-dual-repl   Kernel TCP + 2 synchronous replicas
#     dpdk            DPDK kernel bypass, standalone
#     dpdk-repl       DPDK + 1 synchronous replica (e2e DPDK)
#     dpdk-dual-repl  DPDK + 2 synchronous replicas (e2e DPDK)
#
#   Workloads (what the bench runs):
#     throughput      Peak throughput — 100M pairs, 16 clients, window 128
#     single          Single-order latency — 500K pairs, 1 client, window 1
#     engine-only     Matching engine only — no journal, no network (local)
#     pipeline-only   Journal + matching — no network (local)
#     sweep-window    Window parameter sweep
#     sweep-clients   Client count sweep (constant in-flight)
#     sweep-instruments  Instrument count sweep
#     sweep-accounts  Account count sweep
#
# Usage:
#   ./scripts/lan-bench-suite.sh <server-pub-ip> <bench-pub-ip> <server-vlan-ip> [user] \
#       [replica-pub-ip] [replica-vlan-ip] [replica2-pub-ip] [replica2-vlan-ip]
#
# Examples:
#   # Dual replication throughput only (default):
#   ./scripts/lan-bench-suite.sh 84.32.176.142 84.32.176.143 10.0.0.1 root \
#       84.32.176.144 10.0.0.3 84.32.176.145 10.0.0.4
#
#   # All TCP workloads:
#   TRANSPORTS=tcp WORKLOADS=all ./scripts/lan-bench-suite.sh ...
#
#   # Specific combo:
#   TRANSPORTS=tcp,tcp-repl WORKLOADS=throughput,single ./scripts/lan-bench-suite.sh ...
#
# Environment variables:
#   TRANSPORTS=<list>   Comma-separated transports (default: tcp-dual-repl)
#   WORKLOADS=<list>    Comma-separated workloads (default: throughput)
#   RUN_PLOTS=0|1       Generate plots from results (default: 0)
#   THROUGHPUT_DURATION=T  Measured-phase duration for throughput workload
#                          (humantime, default: 60s)
#   THROUGHPUT_CLIENTS=N   Clients for throughput workload (default: 16)
#   THROUGHPUT_WINDOW=N    Window for throughput workload (default: 128)
#   BENCH_THREADS=N        Number of bench client io_uring threads (default: bench default)
#   SKIP_JOURNAL_VERIFY=1  Skip post-run journal consistency check (default: 0)
#   SINGLE_DURATION=T      Measured-phase duration for single-order workload
#                          (humantime, default: 30s)
#   WARMUP_DURATION=T      Warmup duration (humantime, default: bench default 5s)
#   COOLDOWN_DURATION=T    Cooldown duration excluded from the histogram
#                          (humantime, default: bench default 5s). Useful when
#                          the bench's final small batch flushes a non-amortised
#                          fdatasync that inflates run-max with a drain-tail
#                          artefact rather than steady-state behaviour.
#   SWEEP_DURATION=T       Measured-phase duration per sweep point (default: 30s)
#   LOCAL_DURATION=T       Measured-phase duration for local workloads (default: 60s)
#   RESULTS_DIR=<path>  Reuse existing results directory (default:
#                       <repo>/bench-results/lan-bench-suite-<timestamp>,
#                       git-ignored).
#   BENCH_BRANCH=<ref>  Checkout a specific branch on all machines
#   BENCH_COMMIT=<hash> Checkout a specific commit (mutually exclusive with BENCH_BRANCH)
#   CLEAN_BUILD=1       Run cargo clean before building (forces full recompile)
#   RUSTFLAGS=<flags>   Forwarded to every remote `cargo build` via ssh.
#                       Useful to enable debug assertions in release
#                       builds: RUSTFLAGS="-C debug-assertions=y"
#   NO_PERSIST=1        Build server + bench with the `no-persist` feature
#                       so journal I/O is skipped (unsafe for production;
#                       measures the transport floor without fsync cost).
#                       Composes with any transport × workload combination;
#                       result filenames get a `-no-persist` suffix so runs
#                       can coexist with durable-mode results.
#   MAIN_EXTRA_FEATURES=<list>
#                       Comma-separated cargo features appended to the
#                       kernel-TCP main build. Composes
#                       with NO_PERSIST. Use e.g. `no-o-direct` to bench
#                       the journal without `O_DIRECT` (consumer NVMe
#                       drives without Power Loss Protection).
#   SKIP_ORDER_EXEC=1   Build `melin-server` with the matching hot path
#                       bypassed (--no-default-features
#                       --features skip-order-exec) so the run isolates
#                       durable-transport cost from matching cost. The
#                       bench client + wire protocol are unchanged, so
#                       the server journals every request,
#                       `Exchange::execute` short-circuits to a trivial
#                       rejection, and the full disruptor + replication
#                       + shadow pipeline runs just like trading. The
#                       LOCAL workloads `engine-only` and `pipeline-only`
#                       are trading-only (they run a real Exchange
#                       in-process) and are skipped under
#                       SKIP_ORDER_EXEC=1.
#   PERF=1              Capture `perf record` on the server's ingress core
#                       (io_uring reader for kernel TCP, DPDK poll thread
#                       for DPDK — both default to core 4 via --cores reader field)
#                       during the first workload of the run. Report + raw
#                       perf.data are copied to ${RESULTS_DIR}. Defaults:
#                       core 4, settle 15s after server start, record 30s.
#                       Override with PERF_CORE, PERF_SETTLE, PERF_SECS.
#   PERF_TARGET=...     Which side(s) to capture: comma-separated list
#                       from `server` (default), `bench`, `replica`,
#                       `replica2`, or `both` (= server + bench).
#                       Bench-side capture targets the DPDK poll core
#                       (default ${BENCH_DPDK_CORE:-7}); override with
#                       PERF_BENCH_CORE. Replica captures the reader
#                       core (default 4); override with PERF_REPLICA_CORE
#                       / PERF_REPLICA2_CORE.
#   SKIP_REBOOT=1       Skip the post-DPDK reboot of all machines.
#                       Saves time when chaining DPDK runs back-to-back;
#                       remember to reboot manually before switching to
#                       a kernel transport.
#   DPDK_SERVER_EXTRA_FEATURES=<list>
#                       Comma-separated cargo features to append to the
#                       DPDK server build (e.g. `latency-trace`). Use
#                       this to enable diagnostic instrumentation on the
#                       DPDK transport without editing the script. Server
#                       prints histograms to stderr at shutdown
#                       (/tmp/melin-server.log on the remote).
#   DPDK_BENCH_EXTRA_FEATURES=<list>
#                       Comma-separated cargo features to append to the
#                       DPDK bench build (e.g. `latency-trace`). Mirrors
#                       DPDK_SERVER_EXTRA_FEATURES for the client side.
#                       Bench prints histograms to stderr at end of run
#                       (interleaved with the standard summary).
#
# Special values:
#   TRANSPORTS=all      All transports valid for the available infrastructure
#   WORKLOADS=all       All workloads valid for each transport
#
# Prerequisites:
#   - SSH access to all machines (as root by default)
#   - server-deploy.sh or server-setup.sh completed on all machines
#   - VLAN/private network between machines
#   - bench-isolate.sh run on all machines for stable numbers

set -euo pipefail

if [[ $# -lt 3 ]]; then
    echo "usage: $0 <server-pub-ip> <bench-pub-ip> <server-vlan-ip> [user] [replica-pub-ip] [replica-vlan-ip] [replica2-pub-ip] [replica2-vlan-ip]"
    exit 1
fi

# ---------------------------------------------------------------------------
# Parse positional arguments
# ---------------------------------------------------------------------------
SERVER_PUB="$1"
BENCH_PUB="$2"
SERVER_VLAN="$3"
SSH_USER="${4:-root}"
REPLICA_PUB="${5:-}"
REPLICA_VLAN="${6:-}"
REPLICA2_PUB="${7:-}"
REPLICA2_VLAN="${8:-}"

SSH_CONTROL_DIR="$(mktemp -d -t melin-bench-ssh.XXXXXX)"
SSH_OPTS="-A -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
# ControlMaster multiplexes every subsequent ssh over the first
# connection per host — amortizes the handshake from dozens of calls
# per workload down to one per host.
SSH_OPTS="${SSH_OPTS} -o ControlMaster=auto -o ControlPath=${SSH_CONTROL_DIR}/%r@%h:%p -o ControlPersist=5m"
SERVER="${SSH_USER}@${SERVER_PUB}"
BENCH="${SSH_USER}@${BENCH_PUB}"
REPLICA="${REPLICA_PUB:+${SSH_USER}@${REPLICA_PUB}}"
REPLICA2="${REPLICA2_PUB:+${SSH_USER}@${REPLICA2_PUB}}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOCAL_REPO="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_DIR="~/workspace/melin"
JOURNAL_PATH="${JOURNAL_PATH:-/mnt/journal/bench.journal}"
SNAPSHOT_PATH="${SNAPSHOT_PATH:-/mnt/journal/bench.snapshot}"
JOURNAL_DIR="$(dirname "$JOURNAL_PATH")"
REPLICA_JOURNAL="${JOURNAL_DIR}/replica.journal"
REPLICA2_JOURNAL="${JOURNAL_DIR}/replica2.journal"
REPL_PORT=9877
RUN_PLOTS="${RUN_PLOTS:-0}"

# Open-loop target rate in orders/sec for the throughput workload.
# `0` (default) keeps closed-loop window-filling. When set, the bench
# paces sends at the requested rate and feeds *scheduled* timestamps
# into the latency histogram (coordinated-omission fix). Requires a
# non-zero --window; the script's THROUGHPUT_WINDOW already provides
# one. Only wired into the throughput workload — single is a window=1
# unloaded-latency probe and pacing it would defeat the purpose.
TARGET_RATE="${TARGET_RATE:-0}"

# The server's per-account rate limiter (SEC-04) defaults to 1000 ops/s
# with 5000 burst — a sensible production ceiling, but far below what
# the bench's Zipf-distributed generator throws at head accounts (the
# top account routinely sees >>1M ops/s at peak throughput). Without
# raising these, ~46% of orders get rejected with `ExceedsOrderRate`
# at the gate and never reach the matcher, hiding real engine
# throughput. Bake high defaults in; operators can still override.
BENCH_DEFAULT_RATE_ARGS="--max-orders-per-second 10000000 --max-orders-burst 50000000"

# Primary server args.
SERVER_EXTRA_ARGS="${SERVER_EXTRA_ARGS:-${BENCH_DEFAULT_RATE_ARGS}}"

# Replica args. The legacy `--async-replica-ack` default was removed
# alongside the durability-policy refactor — it has no equivalent on
# the current code path until the ack-on-receive plumbing lands (see
# P1 in `docs/durability-policy-followups.md`). Bench numbers run
# under this script will be ~50-80µs higher per replication round-
# trip than figures previously published with `--async-replica-ack`
# enabled, until that follow-up ships.
REPLICA_EXTRA_ARGS="${REPLICA_EXTRA_ARGS:-${BENCH_DEFAULT_RATE_ARGS}}"

# RUST_LOG override for every remote server launch below (primary +
# replicas, TCP + DPDK). Leave at `info` for normal runs; bump to
# `debug` (or a scoped directive like
# `melin_server::replication=debug,info`) when diagnosing replication
# stalls. Debug logs include per-second TCP_INFO snapshots per replica
# socket, slow-SEND completions, and replica-side queue depths.
BENCH_RUST_LOG="${RUST_LOG:-info}"

# Order counts — override for quick smoke tests.
THROUGHPUT_DURATION="${THROUGHPUT_DURATION:-60s}"
THROUGHPUT_CLIENTS="${THROUGHPUT_CLIENTS:-16}"
THROUGHPUT_WINDOW="${THROUGHPUT_WINDOW:-128}"
SINGLE_DURATION="${SINGLE_DURATION:-30s}"
# Account / instrument cardinality for the throughput + single
# workloads. Higher account counts spread Zipf-distributed order flow
# across more per-account rate-limit buckets — at multi-M-orders/sec
# loads a small account pool saturates the buckets and inflates
# ExceedsOrderRate rejections.
BENCH_ACCOUNTS="${BENCH_ACCOUNTS:-100000}"
WARMUP_DURATION="${WARMUP_DURATION:-}"   # empty = bench default (5s)
COOLDOWN_DURATION="${COOLDOWN_DURATION:-}"  # empty = bench default (5s)
SWEEP_DURATION="${SWEEP_DURATION:-30s}"
LOCAL_DURATION="${LOCAL_DURATION:-60s}"

# Default results dir lives under the repo (git-ignored via
# `/bench-results/`) instead of /tmp so past runs survive reboots and
# are easy to find for side-by-side comparison. Override via
# `RESULTS_DIR=<path>` to write elsewhere.
RESULTS_DIR="${RESULTS_DIR:-${LOCAL_REPO}/bench-results/lan-bench-suite-$(date +%Y%m%d-%H%M%S)}"
mkdir -p "${RESULTS_DIR}"

# Track whether DPDK was used (need reboot at end).
DPDK_RAN=0

# ---------------------------------------------------------------------------
# Cleanup trap — kill servers on exit/interrupt
# ---------------------------------------------------------------------------
cleanup() {
    for host in "$SERVER" ${REPLICA:+"$REPLICA"} ${REPLICA2:+"$REPLICA2"}; do
        ssh $SSH_OPTS "$host" "pkill -INT -x melin-server 2>/dev/null; \
                               pkill -INT -f '[m]elin-server.dpdk' 2>/dev/null; true" 2>/dev/null || true
    done
    # Kill any orphaned bench client too — a hung run leaves the bench
    # binary executing on $BENCH and the next build trips "Text file
    # busy" on the cp into the .dpdk suffixed path.
    ssh $SSH_OPTS "$BENCH" "pkill -INT -x melin-bench 2>/dev/null; \
                            pkill -INT -f '[m]elin-bench.dpdk' 2>/dev/null; true" 2>/dev/null || true
    # Clean DPDK EAL lock files so the next run doesn't fail with
    # "Cannot create lock on '/var/run/dpdk/rte/config'".
    if [[ "${DPDK_RAN:-0}" == "1" ]]; then
        for host in "$SERVER" "$BENCH" ${REPLICA:+"$REPLICA"} ${REPLICA2:+"$REPLICA2"}; do
            ssh $SSH_OPTS "$host" "rm -rf /var/run/dpdk/rte 2>/dev/null; true" 2>/dev/null || true
        done
    fi
    # Close ssh master connections and remove their control sockets.
    for host in "$SERVER" "$BENCH" ${REPLICA:+"$REPLICA"} ${REPLICA2:+"$REPLICA2"}; do
        ssh -O exit $SSH_OPTS "$host" 2>/dev/null || true
    done
    rm -rf "$SSH_CONTROL_DIR" 2>/dev/null || true
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Resolve transport × workload matrix
# ---------------------------------------------------------------------------

# Valid combos. Each transport lists its supported workloads.
# "local" workloads (engine-only, pipeline-only) run independently of transport.
VALID_TCP="throughput single sweep-window sweep-clients sweep-instruments sweep-accounts"
VALID_TCP_REPL="throughput single"
VALID_TCP_DUAL_REPL="throughput single"
VALID_DPDK="throughput single"
VALID_DPDK_REPL="throughput single"
VALID_DPDK_DUAL_REPL="throughput single"
LOCAL_WORKLOADS="engine-only pipeline-only"
ALL_WORKLOADS="throughput single engine-only pipeline-only sweep-window sweep-clients sweep-instruments sweep-accounts"

# Defaults.
TRANSPORTS="${TRANSPORTS:-tcp-dual-repl}"
WORKLOADS="${WORKLOADS:-throughput}"

# Expand "all".
if [[ "$TRANSPORTS" == "all" ]]; then
    TRANSPORTS="tcp"
    if [[ -n "$REPLICA_PUB" ]]; then TRANSPORTS="${TRANSPORTS},tcp-repl"; fi
    if [[ -n "$REPLICA2_PUB" ]]; then TRANSPORTS="${TRANSPORTS},tcp-dual-repl"; fi
    TRANSPORTS="${TRANSPORTS},dpdk"
    if [[ -n "$REPLICA_PUB" ]]; then TRANSPORTS="${TRANSPORTS},dpdk-repl"; fi
    if [[ -n "$REPLICA2_PUB" ]]; then TRANSPORTS="${TRANSPORTS},dpdk-dual-repl"; fi
fi
if [[ "$WORKLOADS" == "all" ]]; then
    WORKLOADS="${ALL_WORKLOADS// /,}"
fi

# Convert to arrays.
IFS=',' read -ra TRANSPORT_LIST <<< "$TRANSPORTS"
IFS=',' read -ra WORKLOAD_LIST <<< "$WORKLOADS"

# Validate infrastructure requirements and build run matrix.
MATRIX=()
LOCAL_MATRIX=()

for workload in "${WORKLOAD_LIST[@]}"; do
    workload="$(echo "$workload" | xargs)" # trim whitespace
    if [[ " ${LOCAL_WORKLOADS} " == *" ${workload} "* ]]; then
        if [[ "${SKIP_ORDER_EXEC:-0}" == "1" ]]; then
            echo "  SKIP local:${workload} — SKIP_ORDER_EXEC=1 (runs Exchange in-process; trading-only)"
            continue
        fi
        LOCAL_MATRIX+=("$workload")
        continue
    fi

    for transport in "${TRANSPORT_LIST[@]}"; do
        transport="$(echo "$transport" | xargs)"

        # Check infrastructure.
        case "$transport" in
            tcp-repl|dpdk-repl)
                if [[ -z "$REPLICA_PUB" || -z "$REPLICA_VLAN" ]]; then
                    echo "  SKIP ${transport}:${workload} — no replica server specified"
                    continue
                fi ;;
            tcp-dual-repl|dpdk-dual-repl)
                if [[ -z "$REPLICA_PUB" || -z "$REPLICA2_PUB" ]]; then
                    echo "  SKIP ${transport}:${workload} — need two replica servers"
                    continue
                fi ;;
        esac

        # Check valid combo.
        valid_list=""
        case "$transport" in
            tcp)            valid_list="$VALID_TCP" ;;
            tcp-repl)       valid_list="$VALID_TCP_REPL" ;;
            tcp-dual-repl)  valid_list="$VALID_TCP_DUAL_REPL" ;;
            dpdk)           valid_list="$VALID_DPDK" ;;
            dpdk-repl)      valid_list="$VALID_DPDK_REPL" ;;
            dpdk-dual-repl) valid_list="$VALID_DPDK_DUAL_REPL" ;;
            *)
                echo "  SKIP unknown transport: ${transport}"
                continue ;;
        esac

        if [[ " ${valid_list} " != *" ${workload} "* ]]; then
            echo "  SKIP ${transport}:${workload} — not a valid combo"
            continue
        fi

        MATRIX+=("${transport}:${workload}")
    done
done

if [[ ${#MATRIX[@]} -eq 0 && ${#LOCAL_MATRIX[@]} -eq 0 ]]; then
    echo "error: no valid transport:workload combos to run" >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# Print plan
# ---------------------------------------------------------------------------
echo ""
echo "============================================================"
echo "  Benchmark Suite"
echo "  Server:  ${SERVER_PUB} (VLAN: ${SERVER_VLAN})"
echo "  Bench:   ${BENCH_PUB}"
if [[ -n "$REPLICA_PUB" ]]; then
    echo "  Replica: ${REPLICA_PUB} (VLAN: ${REPLICA_VLAN})"
fi
if [[ -n "$REPLICA2_PUB" ]]; then
    echo "  Replica2: ${REPLICA2_PUB} (VLAN: ${REPLICA2_VLAN})"
fi
if [[ "${SKIP_ORDER_EXEC:-0}" == "1" ]]; then
    echo "  Server feature: skip-order-exec (matching bypassed, trading wire)"
fi
if [[ "${NO_PERSIST:-0}" == "1" ]]; then
    echo "  Server feature: NO_PERSIST (journal I/O skipped — results tagged -no-persist)"
fi
echo "  Results: ${RESULTS_DIR}"
echo ""
echo "  Plan:"
for item in "${LOCAL_MATRIX[@]+"${LOCAL_MATRIX[@]}"}"; do
    echo "    local : ${item}"
done
for item in "${MATRIX[@]}"; do
    echo "    ${item%%:*} : ${item#*:}"
done
echo "============================================================"
echo ""

# ---------------------------------------------------------------------------
# Validate durability mode vs. replica count
# ---------------------------------------------------------------------------
# The server's default `--durability-mode` is `hybrid`, which gates
# client acks on `persisted>=1 && in_memory>=2` — i.e. needs at least
# one connected replica. Launching a non-replicated transport (tcp,
# dpdk) under the default mode wedges the ack gate and the bench
# appears to hang. Catch the mismatch up front so the operator sees a
# clear error instead of staring at a frozen run.

# Effective `--durability-mode` parsed from `SERVER_EXTRA_ARGS`. The
# server treats `--standalone` as forcing `local` (it rejects any
# other pairing), so we apply the same rule here. Falls back to the
# server's clap default (`hybrid`) when nothing is set.
_effective_durability_mode() {
    local args="${SERVER_EXTRA_ARGS:-}"
    if [[ " ${args} " == *" --standalone "* ]]; then
        echo "local"
        return
    fi
    # Matches `--durability-mode <X>` and `--durability-mode=<X>`.
    if [[ "${args}" =~ --durability-mode[[:space:]=]+([a-zA-Z-]+) ]]; then
        echo "${BASH_REMATCH[1]}"
        return
    fi
    echo "hybrid"
}

# Number of replicas a given transport label launches.
_transport_replica_count() {
    case "$1" in
        tcp|dpdk)                       echo 0 ;;
        tcp-repl|dpdk-repl)             echo 1 ;;
        tcp-dual-repl|dpdk-dual-repl)   echo 2 ;;
        *)                              echo 0 ;;
    esac
}

# Minimum connected replicas needed for the ack gate to advance under
# the given mode. `hybrid` needs >=1 replica for the in_memory>=2
# clause; `durably-replicated` needs >=1 replica for the persisted>=2
# clause. Unknown modes are not validated here — the server will
# reject them at startup.
_mode_min_replicas() {
    case "$1" in
        local)                          echo 0 ;;
        hybrid|durably-replicated)      echo 1 ;;
        *)                              echo 0 ;;
    esac
}

EFFECTIVE_DURABILITY_MODE="$(_effective_durability_mode)"
REQUIRED_REPLICAS="$(_mode_min_replicas "$EFFECTIVE_DURABILITY_MODE")"

# Walk the unique transports in MATRIX and flag any that can't satisfy
# the mode. LOCAL_MATRIX entries (engine-only, pipeline-only) bypass
# the response gate entirely and are not affected.
INVALID_DURABILITY=()
declare -A _VALIDATED
for item in "${MATRIX[@]+"${MATRIX[@]}"}"; do
    _t="${item%%:*}"
    if [[ -n "${_VALIDATED[$_t]:-}" ]]; then continue; fi
    _VALIDATED[$_t]=1
    _have="$(_transport_replica_count "$_t")"
    if (( _have < REQUIRED_REPLICAS )); then
        INVALID_DURABILITY+=("${_t} (launches ${_have} replica(s))")
    fi
done

if (( ${#INVALID_DURABILITY[@]} > 0 )); then
    echo "================================================================" >&2
    echo "  ERROR: durability mode '${EFFECTIVE_DURABILITY_MODE}' requires at least"        >&2
    echo "         ${REQUIRED_REPLICAS} connected replica(s), but the following transport(s)"  >&2
    echo "         in the plan launch with fewer:"                            >&2
    for combo in "${INVALID_DURABILITY[@]}"; do
        echo "             - ${combo}"                                       >&2
    done
    echo ""                                                                  >&2
    echo "  The ack gate would stall on the primary and the bench would"    >&2
    echo "  appear to hang indefinitely."                                    >&2
    echo ""                                                                  >&2
    echo "  Fix one of:"                                                     >&2
    echo "    * Run a replicated transport instead (TRANSPORTS=tcp-repl"     >&2
    echo "      or tcp-dual-repl) and pass the replica host args."          >&2
    echo "    * Declare the cluster standalone:"                             >&2
    echo "        SERVER_EXTRA_ARGS='--durability-mode local' ..."          >&2
    echo "      (add --standalone for an explicit single-node deployment)." >&2
    echo "================================================================" >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# Build binaries
# ---------------------------------------------------------------------------
if [[ -n "${BENCH_BRANCH:-}" && -n "${BENCH_COMMIT:-}" ]]; then
    echo "error: BENCH_BRANCH and BENCH_COMMIT are mutually exclusive" >&2
    exit 1
fi

GIT_CMD="git pull --ff-only"
if [[ -n "${BENCH_BRANCH:-}" ]]; then
    GIT_CMD="git fetch origin && git checkout ${BENCH_BRANCH} && git reset --hard origin/${BENCH_BRANCH} && find crates -name '*.rs' -exec touch {} +"
    echo "=== Using branch: ${BENCH_BRANCH} ==="
elif [[ -n "${BENCH_COMMIT:-}" ]]; then
    GIT_CMD="git fetch origin && git checkout ${BENCH_COMMIT} && find crates -name '*.rs' -exec touch {} +"
    echo "=== Using commit: ${BENCH_COMMIT} ==="
fi

# Determine what to build.
NEED_DPDK=0
for item in "${MATRIX[@]}"; do
    case "${item%%:*}" in dpdk|dpdk-repl|dpdk-dual-repl) NEED_DPDK=1 ;; esac
done

echo "=== Building release binaries ==="
BUILD_HOSTS=("$SERVER" "$BENCH")
if [[ -n "$REPLICA" ]]; then BUILD_HOSTS+=("$REPLICA"); fi
if [[ -n "$REPLICA2" ]]; then BUILD_HOSTS+=("$REPLICA2"); fi

# Cargo invocation for the server + bench release binaries. Feature
# selection composes SKIP_ORDER_EXEC (server only, bypasses the
# matching hot path) and NO_PERSIST (skips journal I/O on every crate
# that exposes the feature — unsafe for production but useful for
# benchmarking). The bench client remains default-featured — it talks
# the trading wire protocol regardless of the server mode.
# Internal feature-list variable for the skip-order-exec build;
# deliberately distinct from the user-facing `SERVER_FEATURES` env var
# below (the latter drives a separate diagnostic-rebuild step).
if [[ "${SKIP_ORDER_EXEC:-0}" == "1" ]]; then
    SKIP_ORDER_EXEC_FEATURES="skip-order-exec"
    if [[ "${NO_PERSIST:-0}" == "1" ]]; then
        SKIP_ORDER_EXEC_FEATURES="skip-order-exec,no-persist"
    fi
    if [[ -n "${MAIN_EXTRA_FEATURES:-}" ]]; then
        SKIP_ORDER_EXEC_FEATURES="${SKIP_ORDER_EXEC_FEATURES},${MAIN_EXTRA_FEATURES}"
    fi
    MAIN_BUILD="cargo build --release -p melin-bench && \
        cargo build --release -p melin-server --no-default-features --features ${SKIP_ORDER_EXEC_FEATURES}"
else
    MAIN_FEATURES=""
    if [[ "${NO_PERSIST:-0}" == "1" ]]; then
        MAIN_FEATURES="no-persist"
    fi
    if [[ -n "${MAIN_EXTRA_FEATURES:-}" ]]; then
        MAIN_FEATURES="${MAIN_FEATURES:+${MAIN_FEATURES},}${MAIN_EXTRA_FEATURES}"
    fi
    if [[ -n "${MAIN_FEATURES}" ]]; then
        MAIN_BUILD="cargo build --release --features ${MAIN_FEATURES}"
    else
        MAIN_BUILD="cargo build --release"
    fi
fi

CLEAN_CMD=""
if [[ "${CLEAN_BUILD:-0}" == "1" ]]; then
    CLEAN_CMD="cargo clean &&"
    echo "  (CLEAN_BUILD=1 — full recompile)"
fi

echo "  Building on ${#BUILD_HOSTS[@]} host(s) in parallel..."
build_pids=()
for HOST in "${BUILD_HOSTS[@]}"; do
    (
        ssh $SSH_OPTS "$HOST" "cd ${REPO_DIR} && ${GIT_CMD} && source ~/.cargo/env && \
            export RUSTFLAGS=\"${RUSTFLAGS:-}\" && \
            ${CLEAN_CMD} ${MAIN_BUILD}" 2>&1 \
            | tail -3 | sed "s/^/  [${HOST}] /"
    ) &
    build_pids+=($!)
done
build_failed=0
for pid in "${build_pids[@]}"; do
    wait "$pid" || build_failed=1
done
if [[ "$build_failed" == "1" ]]; then
    echo "  Build failed on at least one host."
    exit 1
fi

# Optional instrumented melin-server build on the primary only. Used to
# enable diagnostic features (pipeline-stats, latency-trace) for one-off
# investigations without polluting the default build on every host.
# Example: SERVER_FEATURES=pipeline-stats ./scripts/lan-bench-suite.sh ...
if [[ -n "${SERVER_FEATURES:-}" ]]; then
    echo "  Rebuilding melin-server on primary with --features ${SERVER_FEATURES}..."
    ssh $SSH_OPTS "$SERVER" "cd ${REPO_DIR} && source ~/.cargo/env && \
        export RUSTFLAGS=\"${RUSTFLAGS:-}\" && \
        cargo build --release -p melin-server --features ${SERVER_FEATURES}" 2>&1 | tail -3
fi

# DPDK build on server (and replica if dpdk-repl).
# In TAP mode, test-containers-start.sh already built the .dpdk binary.
# In SR-IOV mode we rebuild melin-server here with the dpdk feature plus
# the mode selector (skip-order-exec under SKIP_ORDER_EXEC=1, trading
# is the default and no longer needs a feature flag).
if [[ "$NEED_DPDK" == "1" ]]; then
    # Feature set for the DPDK server build. Mirrors MAIN_BUILD above.
    if [[ "${SKIP_ORDER_EXEC:-0}" == "1" ]]; then
        DPDK_SERVER_FEATURES="dpdk,skip-order-exec"
    else
        DPDK_SERVER_FEATURES="dpdk,hash-chain,release-tracing"
    fi
    if [[ "${NO_PERSIST:-0}" == "1" ]]; then
        DPDK_SERVER_FEATURES="${DPDK_SERVER_FEATURES},no-persist"
    fi
    # Append diagnostic features (e.g. latency-trace) without touching
    # the base feature set. Comma-separated list, no leading/trailing
    # comma. Bench runs that don't need extras leave it unset.
    if [[ -n "${DPDK_SERVER_EXTRA_FEATURES:-}" ]]; then
        DPDK_SERVER_FEATURES="${DPDK_SERVER_FEATURES},${DPDK_SERVER_EXTRA_FEATURES}"
    fi

    # Check if TAP mode .dpdk binary already exists (container setup).
    HAVE_DPDK_BIN=$(ssh $SSH_OPTS "$SERVER" "test -f ${REPO_DIR}/target/release/melin-server.dpdk && echo yes || echo no")
    if [[ "$HAVE_DPDK_BIN" == "yes" ]]; then
        echo "  DPDK binary already built (melin-server.dpdk)."
    else
        # Each DPDK build is independent — run them concurrently and
        # fail the suite if any one returns non-zero.
        echo "  Building DPDK server (--features ${DPDK_SERVER_FEATURES}), bench, (and replica if dpdk-repl) in parallel..."
        dpdk_pids=()
        (
            ssh $SSH_OPTS "$SERVER" "cd ${REPO_DIR} && source ~/.cargo/env && \
                export RUSTFLAGS=\"${RUSTFLAGS:-}\" && \
                cargo build --release -p melin-server --features ${DPDK_SERVER_FEATURES} --no-default-features" 2>&1 \
                | tail -3 | sed "s/^/  [${SERVER} dpdk-server] /"
        ) &
        dpdk_pids+=($!)
        # Bench feature set: append DPDK_BENCH_EXTRA_FEATURES if set so
        # diagnostic features (latency-trace) can be enabled per-run.
        DPDK_BENCH_FEATURES="dpdk"
        if [[ -n "${DPDK_BENCH_EXTRA_FEATURES:-}" ]]; then
            DPDK_BENCH_FEATURES="${DPDK_BENCH_FEATURES},${DPDK_BENCH_EXTRA_FEATURES}"
        fi
        (
            ssh $SSH_OPTS "$BENCH" "cd ${REPO_DIR} && source ~/.cargo/env && \
                export RUSTFLAGS=\"${RUSTFLAGS:-}\" && \
                cargo build --release -p melin-bench --features ${DPDK_BENCH_FEATURES}" 2>&1 \
                | tail -3 | sed "s/^/  [${BENCH} dpdk-bench] /"
        ) &
        dpdk_pids+=($!)
        # Build DPDK server on replicas when any dpdk-*repl variant is in
        # the matrix. dpdk-dual-repl also needs REPLICA2.
        _need_dpdk_repl=0
        _need_dpdk_dual_repl=0
        for item in "${MATRIX[@]}"; do
            case "${item%%:*}" in
                dpdk-repl)      _need_dpdk_repl=1 ;;
                dpdk-dual-repl) _need_dpdk_dual_repl=1 ;;
            esac
        done
        if (( _need_dpdk_repl || _need_dpdk_dual_repl )) && [[ -n "$REPLICA" ]]; then
            (
                ssh $SSH_OPTS "$REPLICA" "cd ${REPO_DIR} && source ~/.cargo/env && \
                    export RUSTFLAGS=\"${RUSTFLAGS:-}\" && \
                    cargo build --release -p melin-server --features ${DPDK_SERVER_FEATURES} --no-default-features" 2>&1 \
                    | tail -3 | sed "s/^/  [${REPLICA} dpdk-server] /"
            ) &
            dpdk_pids+=($!)
        fi
        if (( _need_dpdk_dual_repl )) && [[ -n "$REPLICA2" ]]; then
            (
                ssh $SSH_OPTS "$REPLICA2" "cd ${REPO_DIR} && source ~/.cargo/env && \
                    export RUSTFLAGS=\"${RUSTFLAGS:-}\" && \
                    cargo build --release -p melin-server --features ${DPDK_SERVER_FEATURES} --no-default-features" 2>&1 \
                    | tail -3 | sed "s/^/  [${REPLICA2} dpdk-server] /"
            ) &
            dpdk_pids+=($!)
        fi
        dpdk_failed=0
        for pid in "${dpdk_pids[@]}"; do
            wait "$pid" || dpdk_failed=1
        done
        if [[ "$dpdk_failed" == "1" ]]; then
            echo "  DPDK build failed on at least one host."
            exit 1
        fi
    fi
fi
echo "  Builds complete."
echo ""

# ---------------------------------------------------------------------------
# Generate auth keys (shared setup — needed by all benchmarks)
# ---------------------------------------------------------------------------
echo "=== Setting up auth keys ==="

# Generate trader key on bench machine.
ssh $SSH_OPTS "$BENCH" "cd ${REPO_DIR} && \
    if [[ ! -f bench.key ]]; then \
        source ~/.cargo/env && \
        cargo run --release -p melin-admin --bin melin-keygen -- bench trader && \
        echo 'Generated bench.key'; \
    else \
        echo 'bench.key already exists'; \
    fi"

# Derive per-client signing keys from bench.key and emit one
# authorized_keys line per derived child. The engine dedups by
# (key_hash, request_seq) with a single per-key HWM, so every bench
# connection needs its own key — sharing one collapses every
# connection into one HWM and the leader rejects everyone else as
# DuplicateRequest. 1024 entries covers the largest sweep-clients
# workload; extra entries cost nothing at server startup (HashMap
# insert) and runtime (no lookup hit on unused entries).
AUTH_KEYS_COUNT=1024
AUTH_LINE=$(ssh $SSH_OPTS "$BENCH" "cd ${REPO_DIR} && \
    ./target/release/melin-bench \
        --key bench.key \
        --clients ${AUTH_KEYS_COUNT} \
        --print-authorized-keys")

# Generate replication key on server if any replication transport is used.
REPL_AUTH_LINE=""
HAS_REPL=0
for item in "${MATRIX[@]}"; do
    case "${item%%:*}" in *repl*) HAS_REPL=1; break ;; esac
done

if [[ "$HAS_REPL" == "1" ]]; then
    ssh $SSH_OPTS "$SERVER" "cd ${REPO_DIR} && \
        if [[ ! -f repl.key ]]; then \
            source ~/.cargo/env && \
            cargo run --release -p melin-admin --bin melin-keygen -- repl replication && \
            echo 'Generated repl.key'; \
        else \
            echo 'repl.key already exists'; \
        fi"
    REPL_AUTH_LINE=$(ssh $SSH_OPTS "$SERVER" "cd ${REPO_DIR} && cat repl.pub | xargs -I{} echo 'replication {} repl'")

    # Copy replication key to replica(s).
    if [[ -n "$REPLICA" ]]; then
        scp $SSH_OPTS -q "${SSH_USER}@${SERVER_PUB}:${REPO_DIR}/repl.key" /tmp/repl.key
        scp $SSH_OPTS -q /tmp/repl.key "${REPLICA}:${REPO_DIR}/repl.key"
        echo "  Distributed replication key to replica"
    fi
    if [[ -n "$REPLICA2" ]]; then
        scp $SSH_OPTS -q /tmp/repl.key "${REPLICA2}:${REPO_DIR}/repl.key"
        echo "  Distributed replication key to replica2"
    fi
    rm -f /tmp/repl.key
fi

# Write authorized_keys on server (trader keys + replication).
# AUTH_LINE is multi-line (one per derived client key) so we pipe it
# through ssh stdin rather than embedding it in the remote command — a
# single-quoted command string would lose the line breaks during local
# expansion.
{
    printf '%s\n' "${AUTH_LINE}"
    if [[ -n "$REPL_AUTH_LINE" ]]; then
        printf '%s\n' "${REPL_AUTH_LINE}"
    fi
} | ssh $SSH_OPTS "$SERVER" "cat > ${REPO_DIR}/authorized_keys"

# Distribute authorized_keys to replicas so they don't use stale files.
if [[ -n "$REPLICA" ]]; then
    scp $SSH_OPTS -q "${SSH_USER}@${SERVER_PUB}:${REPO_DIR}/authorized_keys" /tmp/bench-authorized_keys
    scp $SSH_OPTS -q /tmp/bench-authorized_keys "${REPLICA}:${REPO_DIR}/authorized_keys"
    echo "  Distributed authorized_keys to replica"
fi
if [[ -n "$REPLICA2" ]]; then
    scp $SSH_OPTS -q /tmp/bench-authorized_keys "${REPLICA2}:${REPO_DIR}/authorized_keys"
    echo "  Distributed authorized_keys to replica2"
fi
rm -f /tmp/bench-authorized_keys

echo "  Auth keys configured."
echo ""

# Prevent sub-scripts from rebuilding.
export CARGO_BUILD_FLAGS="--release"

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

pin_irqs() {
    local host="$1" label="$2"
    echo "  Pinning IRQs on ${label}..."
    ssh $SSH_OPTS "$host" 'pinned=0; failed=0
for f in /proc/irq/*/smp_affinity; do
    if echo 1 > "$f" 2>/dev/null; then
        pinned=$((pinned + 1))
    else
        failed=$((failed + 1))
    fi
done
# Restrict kernel writeback threads to core 0 to prevent them from
# running on isolated pipeline cores during journal fsync.
echo 1 > /sys/bus/workqueue/devices/writeback/cpumask 2>/dev/null || true
echo "    Pinned ${pinned} IRQs to core 0 (${failed} unchanged)"'
}

clean_journal() {
    local host="$1" path="$2"
    ssh $SSH_OPTS "$host" "rm -f ${path} ${path}.* ${path%.journal}.snapshot ${path%.journal}.snapshot.* 2>/dev/null; true"
}

wait_for_log() {
    local host="$1" log_file="$2" pattern="$3" timeout="${4:-120}" label="${5:-server}"
    for i in $(seq 1 "$timeout"); do
        if ssh $SSH_OPTS "$host" "grep -q '${pattern}' ${log_file} 2>/dev/null"; then
            echo "  ${label} ready (took ${i}s)."
            return 0
        fi
        if [[ $i -eq "$timeout" ]]; then
            echo "  ERROR: ${label} did not become ready within ${timeout}s."
            ssh $SSH_OPTS "$host" "tail -20 ${log_file}" 2>/dev/null || true
            return 1
        fi
        sleep 1
    done
}

stop_servers() {
    for host in "$@"; do
        # `pkill -x` is exact-match; the dpdk binary has a suffix so
        # we list it explicitly.
        ssh $SSH_OPTS "$host" "pkill -INT -x melin-server 2>/dev/null; \
                               pkill -INT -f '[m]elin-server.dpdk' 2>/dev/null; true"
    done
    # Wait for processes to exit. DPDK EAL cleanup can take several
    # seconds; if we restart too early the VFIO groups are still held.
    # After 10s, SIGKILL any stragglers so VFIO devices are released.
    local waited=0
    while [ "$waited" -lt 10 ]; do
        local any_alive=0
        for host in "$@"; do
            # Treat SSH failures as "still alive" — a dropped control
            # socket shouldn't make us skip the wait and race VFIO.
            if ! ssh $SSH_OPTS "$host" "! pgrep -x melin-server >/dev/null 2>&1 && \
                                        ! pgrep -f '[m]elin-server.dpdk' >/dev/null 2>&1" 2>/dev/null; then
                any_alive=1
                break
            fi
        done
        if [ "$any_alive" -eq 0 ]; then break; fi
        sleep 1
        waited=$((waited + 1))
    done
    if [ "$waited" -ge 10 ]; then
        for host in "$@"; do
            ssh $SSH_OPTS "$host" "pkill -KILL -x melin-server 2>/dev/null; \
                                   pkill -KILL -f '[m]elin-server.dpdk' 2>/dev/null; true"
        done
        sleep 1
    fi
}

# Remove DPDK EAL lock files left by a previous run. Without this,
# restarting a DPDK process on the same host fails with
# "Cannot create lock on '/var/run/dpdk/rte/config'".
clean_eal_lockfiles() {
    for host in "$@"; do
        ssh $SSH_OPTS "$host" "rm -rf /var/run/dpdk/rte 2>/dev/null; true"
    done
}

# Run the bench client against an already-running server.
# Usage: run_bench <server_addr> <health_addr> <duration> <extra_bench_args...>
# `duration` is the measured-phase duration (humantime, e.g. `30s`).
run_bench() {
    local server_addr="$1" health_addr="$2" duration="$3"
    shift 3
    local warmup_arg=""
    if [[ -n "${WARMUP_DURATION}" ]]; then
        warmup_arg="--warmup-duration ${WARMUP_DURATION}"
    fi
    local cooldown_arg=""
    if [[ -n "${COOLDOWN_DURATION}" ]]; then
        cooldown_arg="--cooldown-duration ${COOLDOWN_DURATION}"
    fi
    local threads_arg=""
    if [[ -n "${BENCH_THREADS:-}" ]]; then
        threads_arg="--bench-threads ${BENCH_THREADS}"
    fi
    ssh $SSH_OPTS "$BENCH" "cd ${REPO_DIR} && source ~/.cargo/env && \
        ./target/release/melin-bench \
            --addr ${server_addr} \
            --health-addr ${health_addr} \
            --key bench.key \
            --json /tmp/bench-results.json \
            --bench-cores 1 \
            --duration ${duration} \
            ${warmup_arg} ${cooldown_arg} ${threads_arg} \
            $*"
}

collect_result() {
    local name="$1"
    # Tag NO_PERSIST runs so persist and no-persist JSONs can coexist
    # in the same directory and appear side-by-side in the CDF plot.
    if [[ "${NO_PERSIST:-0}" == "1" ]]; then
        name="${name}-no-persist"
    fi
    scp $SSH_OPTS -q "${SSH_USER}@${BENCH_PUB}:/tmp/bench-results.json" "${RESULTS_DIR}/${name}.json" 2>/dev/null || true
}

# ---------------------------------------------------------------------------
# Transport setup/teardown functions
# ---------------------------------------------------------------------------

# Each transport has:
#   transport_start_<t>   — clean journal, start server, wait for ready
#   transport_stop_<t>    — stop servers, optionally verify journals
# Global setup (IRQ pinning, DPDK sriov) is done once before the transport group.

CURRENT_BIND=""
CURRENT_HEALTH=""

transport_start_tcp() {
    clean_journal "$SERVER" "$JOURNAL_PATH"
    pin_irqs "$SERVER" "server"
    pin_irqs "$BENCH" "bench"

    ssh $SSH_OPTS "$SERVER" "pkill -x melin-server 2>/dev/null; true"
    sleep 1
    ssh $SSH_OPTS "$SERVER" "NO_COLOR=1 RUST_LOG=${BENCH_RUST_LOG} ${MELIN_EXTRA_ENV:-} nohup ${REPO_DIR}/target/release/melin-server \
            --bind ${SERVER_VLAN}:9876 \
            --health-bind ${SERVER_VLAN}:9878 \
            --journal ${JOURNAL_PATH} \
            --authorized-keys ${REPO_DIR}/authorized_keys \
            ${SERVER_EXTRA_ARGS:-} \
        >/tmp/melin-server.log 2>&1 </dev/null &" </dev/null

    wait_for_log "$SERVER" "/tmp/melin-server.log" "listening addr=${SERVER_VLAN}:9876" 120 "Server"
    CURRENT_BIND="${SERVER_VLAN}:9876"
    CURRENT_HEALTH="${SERVER_VLAN}:9878"

    perf_capture_start "tcp"
}

transport_stop_tcp() {
    perf_capture_stop
    stop_servers "$SERVER"
}

transport_start_tcp_repl() {
    local replica_journal="${REPLICA_JOURNAL}"
    clean_journal "$SERVER" "$JOURNAL_PATH"
    clean_journal "$REPLICA" "$replica_journal"
    pin_irqs "$SERVER" "server"
    pin_irqs "$BENCH" "bench"
    pin_irqs "$REPLICA" "replica"

    ssh $SSH_OPTS "$SERVER" "pkill -x melin-server 2>/dev/null; true"
    sleep 1
    ssh $SSH_OPTS "$SERVER" "NO_COLOR=1 RUST_LOG=${BENCH_RUST_LOG} ${MELIN_EXTRA_ENV:-} nohup ${REPO_DIR}/target/release/melin-server \
            --bind ${SERVER_VLAN}:9876 \
            --health-bind ${SERVER_VLAN}:9878 \
            --journal ${JOURNAL_PATH} \
            --authorized-keys ${REPO_DIR}/authorized_keys \
            --replication-bind ${SERVER_VLAN}:${REPL_PORT} \
            ${SERVER_EXTRA_ARGS:-} \
        >/tmp/melin-server.log 2>&1 </dev/null &" </dev/null

    wait_for_log "$SERVER" "/tmp/melin-server.log" "replication sender listening" 30 "Replication listener"

    ssh $SSH_OPTS "$REPLICA" "pkill -x melin-server 2>/dev/null; true"
    sleep 1
    ssh $SSH_OPTS "$REPLICA" "NO_COLOR=1 RUST_LOG=${BENCH_RUST_LOG} ${MELIN_EXTRA_ENV:-} nohup ${REPO_DIR}/target/release/melin-server \
            --replica-of ${SERVER_VLAN}:${REPL_PORT} \
            --replication-key ${REPO_DIR}/repl.key \
            --journal ${replica_journal} \
            --authorized-keys ${REPO_DIR}/authorized_keys \
            ${REPLICA_EXTRA_ARGS:-} \
        >/tmp/melin-server.log 2>&1 </dev/null &" </dev/null

    wait_for_log "$SERVER" "/tmp/melin-server.log" "listening addr=${SERVER_VLAN}:9876" 120 "Primary"
    CURRENT_BIND="${SERVER_VLAN}:9876"
    CURRENT_HEALTH="${SERVER_VLAN}:9878"

    perf_capture_start "tcp-repl"
}

transport_stop_tcp_repl() {
    perf_capture_stop
    stop_servers "$SERVER" "$REPLICA"
    if [[ "${SKIP_JOURNAL_VERIFY:-0}" == "1" ]]; then
        echo "  Skipping journal verification (SKIP_JOURNAL_VERIFY=1)"
        return
    fi
    echo "  Verifying journal consistency..."
    "${SCRIPT_DIR}/journal-verify.sh" "$SERVER" "$JOURNAL_PATH" "$REPLICA" "${REPLICA_JOURNAL}"
}

transport_start_tcp_dual_repl() {
    local replica_journal="${REPLICA_JOURNAL}"
    local replica2_journal="${REPLICA2_JOURNAL}"
    clean_journal "$SERVER" "$JOURNAL_PATH"
    clean_journal "$REPLICA" "$replica_journal"
    clean_journal "$REPLICA2" "$replica2_journal"
    pin_irqs "$SERVER" "server"
    pin_irqs "$BENCH" "bench"
    pin_irqs "$REPLICA" "replica1"
    pin_irqs "$REPLICA2" "replica2"

    ssh $SSH_OPTS "$SERVER" "pkill -x melin-server 2>/dev/null; true"
    sleep 1
    ssh $SSH_OPTS "$SERVER" "NO_COLOR=1 RUST_LOG=${BENCH_RUST_LOG} ${MELIN_EXTRA_ENV:-} nohup ${REPO_DIR}/target/release/melin-server \
            --bind ${SERVER_VLAN}:9876 \
            --health-bind ${SERVER_VLAN}:9878 \
            --journal ${JOURNAL_PATH} \
            --authorized-keys ${REPO_DIR}/authorized_keys \
            --replication-bind ${SERVER_VLAN}:${REPL_PORT} \
            ${SERVER_EXTRA_ARGS:-} \
        >/tmp/melin-server.log 2>&1 </dev/null &" </dev/null

    wait_for_log "$SERVER" "/tmp/melin-server.log" "replication sender listening" 30 "Replication listener"

    ssh $SSH_OPTS "$REPLICA" "pkill -x melin-server 2>/dev/null; true"
    sleep 1
    ssh $SSH_OPTS "$REPLICA" "NO_COLOR=1 RUST_LOG=${BENCH_RUST_LOG} ${MELIN_EXTRA_ENV:-} nohup ${REPO_DIR}/target/release/melin-server \
            --replica-of ${SERVER_VLAN}:${REPL_PORT} \
            --replication-key ${REPO_DIR}/repl.key \
            --journal ${replica_journal} \
            --authorized-keys ${REPO_DIR}/authorized_keys \
            ${REPLICA_EXTRA_ARGS:-} \
        >/tmp/melin-server.log 2>&1 </dev/null &" </dev/null

    ssh $SSH_OPTS "$REPLICA2" "pkill -x melin-server 2>/dev/null; true"
    sleep 1
    ssh $SSH_OPTS "$REPLICA2" "NO_COLOR=1 RUST_LOG=${BENCH_RUST_LOG} ${MELIN_EXTRA_ENV:-} nohup ${REPO_DIR}/target/release/melin-server \
            --replica-of ${SERVER_VLAN}:${REPL_PORT} \
            --replication-key ${REPO_DIR}/repl.key \
            --journal ${replica2_journal} \
            --authorized-keys ${REPO_DIR}/authorized_keys \
            ${REPLICA_EXTRA_ARGS:-} \
        >/tmp/melin-server.log 2>&1 </dev/null &" </dev/null

    wait_for_log "$SERVER" "/tmp/melin-server.log" "listening addr=${SERVER_VLAN}:9876" 120 "Primary"
    CURRENT_BIND="${SERVER_VLAN}:9876"
    CURRENT_HEALTH="${SERVER_VLAN}:9878"

    perf_capture_start "tcp-dual-repl"
}

transport_stop_tcp_dual_repl() {
    perf_capture_stop
    stop_servers "$SERVER" "$REPLICA" "$REPLICA2"
    if [[ "${SKIP_JOURNAL_VERIFY:-0}" == "1" ]]; then
        echo "  Skipping journal verification (SKIP_JOURNAL_VERIFY=1)"
        return
    fi
    echo "  Verifying journal consistency (replica1)..."
    "${SCRIPT_DIR}/journal-verify.sh" "$SERVER" "$JOURNAL_PATH" "$REPLICA" "${REPLICA_JOURNAL}"
    echo "  Verifying journal consistency (replica2)..."
    "${SCRIPT_DIR}/journal-verify.sh" "$SERVER" "$JOURNAL_PATH" "$REPLICA2" "${REPLICA2_JOURNAL}"
}

# --- DPDK transports ---

# Load DPDK config from /etc/melin-dpdk.conf on a host.
# Populates ${prefix}_DPDK_IP, _PORT, _PREFIX, _MODE, _EAL_ARGS.
load_dpdk_config() {
    local host="$1" prefix="$2"
    local conf
    conf=$(ssh $SSH_OPTS "$host" "cat /etc/melin-dpdk.conf 2>/dev/null" || true)
    if [[ -n "$conf" ]]; then
        local ip port dpdk_prefix mode eal_args
        ip=$(echo "$conf" | grep "^DPDK_IP=" | cut -d= -f2 || true)
        port=$(echo "$conf" | grep "^DPDK_PORT=" | cut -d= -f2 || true)
        dpdk_prefix=$(echo "$conf" | grep "^DPDK_PREFIX=" | cut -d= -f2 || true)
        mode=$(echo "$conf" | grep "^DPDK_MODE=" | cut -d= -f2 || true)
        eal_args=$(echo "$conf" | grep "^DPDK_EAL_ARGS=" | cut -d= -f2- || true)
        local vlan_id
        vlan_id=$(echo "$conf" | grep "^VLAN_ID=" | cut -d= -f2 || true)
        eval "${prefix}_DPDK_IP=${ip:-}"
        eval "${prefix}_DPDK_PORT=${port:-0}"
        eval "${prefix}_DPDK_PREFIX=${dpdk_prefix:-24}"
        eval "${prefix}_DPDK_MODE=${mode:-sriov}"
        eval "${prefix}_DPDK_EAL_ARGS='${eal_args:-}'"
        eval "${prefix}_DPDK_VLAN=${vlan_id:-}"
    fi
}

DPDK_SRIOV_DONE=0
# Set to "tap" when TAP mode detected — controls routing setup.
DPDK_MODE="sriov"

dpdk_sriov_setup() {
    if [[ "$DPDK_SRIOV_DONE" == "1" ]]; then return; fi

    load_dpdk_config "$SERVER" "SERVER"
    SERVER_DPDK_IP="${SERVER_DPDK_IP:-${SERVER_VLAN}}"
    SERVER_DPDK_PORT="${SERVER_DPDK_PORT:-0}"
    SERVER_DPDK_PREFIX="${SERVER_DPDK_PREFIX:-24}"
    DPDK_MODE="${SERVER_DPDK_MODE:-sriov}"

    if [[ "$DPDK_MODE" == "tap" ]]; then
        # TAP mode (Docker containers): skip SR-IOV, use TAP PMD.
        # The .dpdk binary is separate from the default binary so TCP
        # transports aren't broken by the DPDK feature flag.
        DPDK_SERVER_BIN="${REPO_DIR}/target/release/melin-server.dpdk"
        echo ""
        echo "=== DPDK TAP mode (no SR-IOV) ==="
        echo "  Server DPDK: IP=${SERVER_DPDK_IP}, port=${SERVER_DPDK_PORT}, mode=tap"
        echo ""
    else
        echo ""
        echo "=== Setting up DPDK SR-IOV ==="
        local hosts=("$SERVER" "$BENCH")
        local _need_repl=0 _need_repl2=0
        for item in "${MATRIX[@]}"; do
            case "${item%%:*}" in
                dpdk-repl)      _need_repl=1 ;;
                dpdk-dual-repl) _need_repl=1; _need_repl2=1 ;;
            esac
        done
        if (( _need_repl )) && [[ -n "$REPLICA" ]]; then hosts+=("$REPLICA"); fi
        if (( _need_repl2 )) && [[ -n "$REPLICA2" ]]; then hosts+=("$REPLICA2"); fi
        for HOST in "${hosts[@]}"; do
            echo "  Setting up DPDK on ${HOST}..."
            ssh $SSH_OPTS "$HOST" "cd ${REPO_DIR} && sudo ./scripts/dpdk/dpdk-setup-sriov.sh" 2>&1 | tail -5
        done
        # Re-read configs after setup wrote them (VLAN_ID, DPDK_MODE, etc.).
        load_dpdk_config "$SERVER" "SERVER"
        SERVER_DPDK_IP="${SERVER_DPDK_IP:-${SERVER_VLAN}}"
        SERVER_DPDK_PREFIX="${SERVER_DPDK_PREFIX:-24}"
        DPDK_MODE="${SERVER_DPDK_MODE:-sriov}"
        load_dpdk_config "$BENCH" "BENCH"
        DPDK_SERVER_BIN="${REPO_DIR}/target/release/melin-server"
        # Auto-detect VF count for LACP bonds: use both ports so traffic
        # arriving on either bond member's VF is received.
        local vf_count
        vf_count=$(ssh $SSH_OPTS "$SERVER" "ls -d /sys/bus/pci/drivers/vfio-pci/0000:* 2>/dev/null | wc -l" || echo 0)
        if [[ "$vf_count" -ge 2 ]]; then
            SERVER_DPDK_PORT="0,1"
        else
            SERVER_DPDK_PORT="${SERVER_DPDK_PORT:-0}"
        fi
        echo "  Server DPDK: IP=${SERVER_DPDK_IP}, port=${SERVER_DPDK_PORT}, mode=${DPDK_MODE}"
        echo ""
    fi

    # Auto-detect VF count on bench for LACP bonds.
    local bench_vf_count
    bench_vf_count=$(ssh $SSH_OPTS "$BENCH" "ls -d /sys/bus/pci/drivers/vfio-pci/0000:* 2>/dev/null | wc -l" || echo 0)
    if [[ "$bench_vf_count" -ge 2 ]]; then
        BENCH_DPDK_PORT="0,1"
    else
        BENCH_DPDK_PORT="${BENCH_DPDK_PORT:-0}"
    fi
    BENCH_DPDK_PREFIX="${BENCH_DPDK_PREFIX:-24}"
    DPDK_SRIOV_DONE=1
}

HUGE_DIR="${HUGE_DIR:-/mnt/huge_2m}"
BENCH_DPDK_CORE="${BENCH_DPDK_CORE:-7}"

# After starting a DPDK TAP server, set up kernel routing so external
# clients can reach smoltcp through the TAP device.
# TAP PMD creates a kernel interface (dtap0). The kernel forwards
# packets to it, DPDK reads from the TAP fd, smoltcp processes TCP.
setup_tap_routing() {
    local host="$1" dpdk_ip="$2"
    ssh $SSH_OPTS "$host" "
        ip link set dtap0 up 2>/dev/null
        echo 1 > /proc/sys/net/ipv4/ip_forward
        ip route replace ${dpdk_ip}/32 dev dtap0
        MAC=\$(ip link show dtap0 2>/dev/null | grep link/ether | awk '{print \$2}')
        ip neigh replace ${dpdk_ip} lladdr \$MAC dev dtap0 nud permanent
        echo \"  TAP routing: dtap0 up, route ${dpdk_ip} -> dtap0 (MAC=\$MAC)\"
    "
}

# Add a route on a remote host so it can reach the DPDK TAP IP via
# the server's kernel interface.
add_tap_route() {
    local host="$1" dpdk_ip="$2" via_ip="$3"
    ssh $SSH_OPTS "$host" "
        ip route replace ${dpdk_ip}/32 via ${via_ip} 2>/dev/null
    "
}

# Start a background perf record on a remote host's ingress core. Role is
# `server` or `bench`; each role tracks state in its own set of env vars so
# both can run in parallel. Returns immediately; data lands at
# /root/melin-perf-${role}-${label}.{data,report.txt} on the host after
# ${settle}+${secs} seconds. _perf_stop_on() waits, fetches both files to
# RESULTS_DIR, and clears the pending flag.
_perf_start_on() {
    local role="$1" host="$2" label="$3" core="$4"
    local secs="${PERF_SECS:-30}"
    local settle="${PERF_SETTLE:-15}"
    local data_path="/root/melin-perf-${role}-${label}.data"
    local report_path="/root/melin-perf-${role}-${label}.report.txt"

    # Store per-role state so the matching _perf_stop_on can find it.
    eval "PERF_${role^^}_LABEL='${label}'"
    eval "PERF_${role^^}_HOST='${host}'"
    eval "PERF_${role^^}_DATA='${data_path}'"
    eval "PERF_${role^^}_REPORT='${report_path}'"

    # core="all" → system-wide capture (perf record -a) instead of -C <n>.
    # Useful when the interesting thread is unpinned or we don't know
    # which core holds the hot path.
    local perf_scope
    if [[ "$core" == "all" ]]; then
        perf_scope="-a"
    else
        perf_scope="-C ${core}"
    fi

    echo "  perf(${role}): core=${core} settle=${settle}s record=${secs}s label=${label}"
    ssh $SSH_OPTS "$host" "rm -f ${data_path} ${report_path} /tmp/melin-perf-${role}.log; \
        nohup bash -c 'sleep ${settle} && \
            perf record ${perf_scope} -g -F 997 -o ${data_path} -- sleep ${secs} 2>>/tmp/melin-perf-${role}.log && \
            perf report -i ${data_path} --stdio --no-children -F overhead,sample,symbol 2>/dev/null \
                | head -200 > ${report_path}; \
            touch ${report_path}.done' \
        >/tmp/melin-perf-${role}.log 2>&1 </dev/null &" </dev/null
}

_perf_stop_on() {
    local role="$1"
    local label_var="PERF_${role^^}_LABEL"
    local host_var="PERF_${role^^}_HOST"
    local data_var="PERF_${role^^}_DATA"
    local report_var="PERF_${role^^}_REPORT"
    local label="${!label_var:-}"
    [[ -z "$label" ]] && return
    local host="${!host_var}"
    local data_path="${!data_var}"
    local report_path="${!report_var}"

    echo "  perf(${role}): waiting for capture to finish..."
    local max_wait=120 waited=0
    while (( waited < max_wait )); do
        if ssh $SSH_OPTS "$host" "test -f ${report_path}.done" 2>/dev/null; then
            break
        fi
        sleep 2
        waited=$((waited + 2))
    done
    if (( waited >= max_wait )); then
        echo "  perf(${role}): report not produced within ${max_wait}s — skipping fetch"
        ssh $SSH_OPTS "$host" "cat /tmp/melin-perf-${role}.log 2>/dev/null | tail -20" || true
        eval "PERF_${role^^}_LABEL=''"
        return
    fi

    echo "  perf(${role}): fetching data + report to ${RESULTS_DIR}"
    scp $SSH_OPTS "${host}:${data_path}" "${RESULTS_DIR}/perf-${role}-${label}.data" 2>/dev/null || true
    scp $SSH_OPTS "${host}:${report_path}" "${RESULTS_DIR}/perf-${role}-${label}.report.txt" 2>/dev/null || true
    eval "PERF_${role^^}_LABEL=''"
}

# Public entry points. PERF_TARGET selects which side(s) to capture
# — a comma-separated list chosen from:
#   server (default), bench, replica, replica2, both (= server + bench).
# `replica` / `replica2` require the corresponding host args to have
# been passed; otherwise they're silently skipped.
perf_capture_start() {
    [[ "${PERF:-0}" != "1" ]] && return
    local label="$1"
    local target="${PERF_TARGET:-server}"

    _want_role() {
        local role="$1"
        [[ "$target" == "$role" ]] && return 0
        [[ "$target" == "both" && ("$role" == "server" || "$role" == "bench") ]] && return 0
        [[ ",${target}," == *,"$role",* ]] && return 0
        return 1
    }

    if _want_role "server"; then
        _perf_start_on "server" "$SERVER" "$label" "${PERF_CORE:-4}"
    fi
    if _want_role "bench"; then
        _perf_start_on "bench" "$BENCH" "$label" "${PERF_BENCH_CORE:-${BENCH_DPDK_CORE:-7}}"
    fi
    if _want_role "replica" && [[ -n "${REPLICA:-}" ]]; then
        _perf_start_on "replica" "$REPLICA" "$label" "${PERF_REPLICA_CORE:-4}"
    fi
    if _want_role "replica2" && [[ -n "${REPLICA2:-}" ]]; then
        _perf_start_on "replica2" "$REPLICA2" "$label" "${PERF_REPLICA2_CORE:-4}"
    fi
}

perf_capture_stop() {
    [[ "${PERF:-0}" != "1" ]] && return
    _perf_stop_on "server"
    _perf_stop_on "bench"
    _perf_stop_on "replica"
    _perf_stop_on "replica2"
}

transport_start_dpdk() {
    dpdk_sriov_setup
    clean_journal "$SERVER" "$JOURNAL_PATH"
    pin_irqs "$SERVER" "server"
    pin_irqs "$BENCH" "bench"

    # Build EAL args: TAP mode uses config from /etc/melin-dpdk.conf,
    # SR-IOV mode uses hugepages.
    local server_eal
    if [[ "$DPDK_MODE" == "tap" ]]; then
        server_eal="${SERVER_DPDK_EAL_ARGS}"
    else
        server_eal="--huge-dir=${HUGE_DIR}"
    fi

    local vlan_arg=""
    if [[ -n "${SERVER_DPDK_VLAN:-}" ]]; then
        vlan_arg="--dpdk-vlan ${SERVER_DPDK_VLAN}"
    fi

    ssh $SSH_OPTS "$SERVER" "pkill -x melin-server 2>/dev/null; pkill -f '[m]elin-server.dpdk' 2>/dev/null; true"
    sleep 1
    ssh $SSH_OPTS "$SERVER" "NO_COLOR=1 RUST_LOG=${BENCH_RUST_LOG} nohup ${DPDK_SERVER_BIN} \
            --bind 0.0.0.0:9876 \
            --health-bind ${SERVER_VLAN}:9878 \
            --journal ${JOURNAL_PATH} \
            --authorized-keys ${REPO_DIR}/authorized_keys \
            --dpdk-eal-args='${server_eal}' \
            --dpdk-ip ${SERVER_DPDK_IP} \
            --dpdk-prefix-len ${SERVER_DPDK_PREFIX} \
            --dpdk-ports ${SERVER_DPDK_PORT} \
            ${vlan_arg} \
            ${SERVER_EXTRA_ARGS:-} \
        >/tmp/melin-server.log 2>&1 </dev/null &" </dev/null

    wait_for_log "$SERVER" "/tmp/melin-server.log" "listening" 120 "DPDK server"

    # TAP mode: set up kernel routing so the bench client can reach smoltcp.
    if [[ "$DPDK_MODE" == "tap" ]]; then
        setup_tap_routing "$SERVER" "${SERVER_DPDK_IP}"
        add_tap_route "$BENCH" "${SERVER_DPDK_IP}" "${SERVER_PUB}"
        # In TAP mode, the bench client uses kernel TCP (no DPDK on client).
        BENCH_DPDK_ARGS=""
    else
        local bench_eal="--huge-dir=${HUGE_DIR}"
        if [[ -n "${BENCH_DPDK_EAL_ARGS:-}" ]]; then
            bench_eal="${BENCH_DPDK_EAL_ARGS} ${bench_eal}"
        fi
        local bench_vlan_arg=""
        if [[ -n "${BENCH_DPDK_VLAN:-}" ]]; then
            bench_vlan_arg="--dpdk-vlan ${BENCH_DPDK_VLAN}"
        fi
        BENCH_DPDK_ARGS="--dpdk-eal-args='${bench_eal}' --dpdk-ports ${BENCH_DPDK_PORT} --dpdk-core ${BENCH_DPDK_CORE} ${bench_vlan_arg}"
        if [[ -n "${BENCH_DPDK_IP:-}" ]]; then
            BENCH_DPDK_ARGS="${BENCH_DPDK_ARGS} --dpdk-ip ${BENCH_DPDK_IP} --dpdk-prefix-len ${BENCH_DPDK_PREFIX}"
        fi
    fi

    CURRENT_BIND="${SERVER_DPDK_IP}:9876"
    # Health endpoint uses kernel TCP (separate from DPDK trading port),
    # so it's reachable from the bench host's kernel side. Required for
    # the bench's tick-to-trade /stats-dump fetch.
    CURRENT_HEALTH="${SERVER_VLAN}:9878"
    DPDK_RAN=1

    perf_capture_start "dpdk"
}

transport_stop_dpdk() {
    perf_capture_stop
    stop_servers "$SERVER"
    # TAP mode uses melin-server.dpdk — kill that too.
    ssh $SSH_OPTS "$SERVER" "pkill -INT -f '[m]elin-server.dpdk' 2>/dev/null; true"
    clean_eal_lockfiles "$SERVER" "$BENCH"
}

transport_start_dpdk_repl() {
    dpdk_sriov_setup
    local replica_journal="${REPLICA_JOURNAL}"
    clean_journal "$SERVER" "$JOURNAL_PATH"
    clean_journal "$REPLICA" "$replica_journal"
    pin_irqs "$SERVER" "server"
    pin_irqs "$BENCH" "bench"
    pin_irqs "$REPLICA" "replica"

    load_dpdk_config "$REPLICA" "REPLICA"
    REPLICA_DPDK_IP="${REPLICA_DPDK_IP:-${REPLICA_VLAN}}"
    REPLICA_DPDK_PREFIX="${REPLICA_DPDK_PREFIX:-24}"
    local replica_vf_count
    replica_vf_count=$(ssh $SSH_OPTS "$REPLICA" "ls -d /sys/bus/pci/drivers/vfio-pci/0000:* 2>/dev/null | wc -l" || echo 0)
    if [[ "$replica_vf_count" -ge 2 ]]; then
        REPLICA_DPDK_PORT="0,1"
    else
        REPLICA_DPDK_PORT="${REPLICA_DPDK_PORT:-0}"
    fi

    local server_eal replica_eal
    if [[ "$DPDK_MODE" == "tap" ]]; then
        server_eal="${SERVER_DPDK_EAL_ARGS}"
        replica_eal="${REPLICA_DPDK_EAL_ARGS:-${SERVER_DPDK_EAL_ARGS}}"
    else
        server_eal="--huge-dir=${HUGE_DIR}"
        replica_eal="--huge-dir=${HUGE_DIR}"
    fi

    ssh $SSH_OPTS "$SERVER" "pkill -x melin-server 2>/dev/null; pkill -f '[m]elin-server.dpdk' 2>/dev/null; true"
    sleep 1
    ssh $SSH_OPTS "$SERVER" "NO_COLOR=1 RUST_LOG=${BENCH_RUST_LOG} nohup ${DPDK_SERVER_BIN} \
            --bind 0.0.0.0:9876 \
            --journal ${JOURNAL_PATH} \
            --authorized-keys ${REPO_DIR}/authorized_keys \
            --replication-bind ${SERVER_DPDK_IP}:${REPL_PORT} \
            --dpdk-eal-args='${server_eal}' \
            --dpdk-ip ${SERVER_DPDK_IP} \
            --dpdk-prefix-len ${SERVER_DPDK_PREFIX} \
            --dpdk-ports ${SERVER_DPDK_PORT} \
            ${SERVER_EXTRA_ARGS:-} \
        >/tmp/melin-server.log 2>&1 </dev/null &" </dev/null

    wait_for_log "$SERVER" "/tmp/melin-server.log" "DPDK replication sender started" 30 "DPDK replication listener"

    ssh $SSH_OPTS "$REPLICA" "pkill -x melin-server 2>/dev/null; pkill -f '[m]elin-server.dpdk' 2>/dev/null; true"
    sleep 1
    ssh $SSH_OPTS "$REPLICA" "NO_COLOR=1 RUST_LOG=${BENCH_RUST_LOG} nohup ${DPDK_SERVER_BIN} \
            --replica-of ${SERVER_DPDK_IP}:${REPL_PORT} \
            --replication-key ${REPO_DIR}/repl.key \
            --journal ${replica_journal} \
            --authorized-keys ${REPO_DIR}/authorized_keys \
            --dpdk-eal-args='${replica_eal}' \
            --dpdk-ip ${REPLICA_DPDK_IP} \
            --dpdk-prefix-len ${REPLICA_DPDK_PREFIX} \
            --dpdk-ports ${REPLICA_DPDK_PORT} \
            ${REPLICA_EXTRA_ARGS:-} \
        >/tmp/melin-server.log 2>&1 </dev/null &" </dev/null

    wait_for_log "$SERVER" "/tmp/melin-server.log" "listening" 120 "DPDK primary"

    # TAP mode: routing for bench client.
    if [[ "$DPDK_MODE" == "tap" ]]; then
        setup_tap_routing "$SERVER" "${SERVER_DPDK_IP}"
        add_tap_route "$BENCH" "${SERVER_DPDK_IP}" "${SERVER_PUB}"
        BENCH_DPDK_ARGS=""
    else
        local bench_eal="--huge-dir=${HUGE_DIR}"
        if [[ -n "${BENCH_DPDK_EAL_ARGS:-}" ]]; then
            bench_eal="${BENCH_DPDK_EAL_ARGS} ${bench_eal}"
        fi
        BENCH_DPDK_ARGS="--dpdk-eal-args='${bench_eal}' --dpdk-ports ${BENCH_DPDK_PORT} --dpdk-core ${BENCH_DPDK_CORE}"
        if [[ -n "${BENCH_DPDK_IP:-}" ]]; then
            BENCH_DPDK_ARGS="${BENCH_DPDK_ARGS} --dpdk-ip ${BENCH_DPDK_IP} --dpdk-prefix-len ${BENCH_DPDK_PREFIX}"
        fi
    fi

    CURRENT_BIND="${SERVER_DPDK_IP}:9876"
    # Health endpoint stays on kernel TCP via SERVER_VLAN even in DPDK mode
    # — same as transport_start_dpdk. Empty would break --health-addr arg.
    CURRENT_HEALTH="${SERVER_VLAN}:9878"
    DPDK_RAN=1

    perf_capture_start "dpdk-repl"
}

transport_stop_dpdk_repl() {
    perf_capture_stop
    stop_servers "$SERVER" "$REPLICA"
    for host in "$SERVER" "$REPLICA"; do
        ssh $SSH_OPTS "$host" "pkill -INT -f '[m]elin-server.dpdk' 2>/dev/null; true"
    done
    clean_eal_lockfiles "$SERVER" "$BENCH" "$REPLICA"
    if [[ "${SKIP_JOURNAL_VERIFY:-0}" == "1" ]]; then
        echo "  Skipping journal verification (SKIP_JOURNAL_VERIFY=1)"
        return
    fi
    echo "  Verifying DPDK replication journal consistency..."
    "${SCRIPT_DIR}/journal-verify.sh" "$SERVER" "$JOURNAL_PATH" "$REPLICA" "${REPLICA_JOURNAL}"
}

transport_start_dpdk_dual_repl() {
    dpdk_sriov_setup
    local replica_journal="${REPLICA_JOURNAL}"
    local replica2_journal="${REPLICA2_JOURNAL}"
    clean_journal "$SERVER" "$JOURNAL_PATH"
    clean_journal "$REPLICA" "$replica_journal"
    clean_journal "$REPLICA2" "$replica2_journal"
    pin_irqs "$SERVER" "server"
    pin_irqs "$BENCH" "bench"
    pin_irqs "$REPLICA" "replica1"
    pin_irqs "$REPLICA2" "replica2"

    load_dpdk_config "$REPLICA" "REPLICA"
    REPLICA_DPDK_IP="${REPLICA_DPDK_IP:-${REPLICA_VLAN}}"
    REPLICA_DPDK_PREFIX="${REPLICA_DPDK_PREFIX:-24}"
    local replica_vf_count
    replica_vf_count=$(ssh $SSH_OPTS "$REPLICA" "ls -d /sys/bus/pci/drivers/vfio-pci/0000:* 2>/dev/null | wc -l" || echo 0)
    if [[ "$replica_vf_count" -ge 2 ]]; then
        REPLICA_DPDK_PORT="0,1"
    else
        REPLICA_DPDK_PORT="${REPLICA_DPDK_PORT:-0}"
    fi

    load_dpdk_config "$REPLICA2" "REPLICA2"
    REPLICA2_DPDK_IP="${REPLICA2_DPDK_IP:-${REPLICA2_VLAN}}"
    REPLICA2_DPDK_PREFIX="${REPLICA2_DPDK_PREFIX:-24}"
    local replica2_vf_count
    replica2_vf_count=$(ssh $SSH_OPTS "$REPLICA2" "ls -d /sys/bus/pci/drivers/vfio-pci/0000:* 2>/dev/null | wc -l" || echo 0)
    if [[ "$replica2_vf_count" -ge 2 ]]; then
        REPLICA2_DPDK_PORT="0,1"
    else
        REPLICA2_DPDK_PORT="${REPLICA2_DPDK_PORT:-0}"
    fi

    local server_eal replica_eal replica2_eal
    if [[ "$DPDK_MODE" == "tap" ]]; then
        server_eal="${SERVER_DPDK_EAL_ARGS}"
        replica_eal="${REPLICA_DPDK_EAL_ARGS:-${SERVER_DPDK_EAL_ARGS}}"
        replica2_eal="${REPLICA2_DPDK_EAL_ARGS:-${SERVER_DPDK_EAL_ARGS}}"
    else
        server_eal="--huge-dir=${HUGE_DIR}"
        replica_eal="--huge-dir=${HUGE_DIR}"
        replica2_eal="--huge-dir=${HUGE_DIR}"
    fi

    ssh $SSH_OPTS "$SERVER" "pkill -x melin-server 2>/dev/null; pkill -f '[m]elin-server.dpdk' 2>/dev/null; true"
    sleep 1
    ssh $SSH_OPTS "$SERVER" "NO_COLOR=1 RUST_LOG=${BENCH_RUST_LOG} nohup ${DPDK_SERVER_BIN} \
            --bind 0.0.0.0:9876 \
            --journal ${JOURNAL_PATH} \
            --authorized-keys ${REPO_DIR}/authorized_keys \
            --replication-bind ${SERVER_DPDK_IP}:${REPL_PORT} \
            --dpdk-eal-args='${server_eal}' \
            --dpdk-ip ${SERVER_DPDK_IP} \
            --dpdk-prefix-len ${SERVER_DPDK_PREFIX} \
            --dpdk-ports ${SERVER_DPDK_PORT} \
            ${SERVER_EXTRA_ARGS:-} \
        >/tmp/melin-server.log 2>&1 </dev/null &" </dev/null

    wait_for_log "$SERVER" "/tmp/melin-server.log" "DPDK replication sender started" 30 "DPDK replication listener"

    ssh $SSH_OPTS "$REPLICA" "pkill -x melin-server 2>/dev/null; pkill -f '[m]elin-server.dpdk' 2>/dev/null; true"
    sleep 1
    ssh $SSH_OPTS "$REPLICA" "NO_COLOR=1 RUST_LOG=${BENCH_RUST_LOG} nohup ${DPDK_SERVER_BIN} \
            --replica-of ${SERVER_DPDK_IP}:${REPL_PORT} \
            --replication-key ${REPO_DIR}/repl.key \
            --journal ${replica_journal} \
            --authorized-keys ${REPO_DIR}/authorized_keys \
            --dpdk-eal-args='${replica_eal}' \
            --dpdk-ip ${REPLICA_DPDK_IP} \
            --dpdk-prefix-len ${REPLICA_DPDK_PREFIX} \
            --dpdk-ports ${REPLICA_DPDK_PORT} \
            ${REPLICA_EXTRA_ARGS:-} \
        >/tmp/melin-server.log 2>&1 </dev/null &" </dev/null

    ssh $SSH_OPTS "$REPLICA2" "pkill -x melin-server 2>/dev/null; pkill -f '[m]elin-server.dpdk' 2>/dev/null; true"
    sleep 1
    ssh $SSH_OPTS "$REPLICA2" "NO_COLOR=1 RUST_LOG=${BENCH_RUST_LOG} nohup ${DPDK_SERVER_BIN} \
            --replica-of ${SERVER_DPDK_IP}:${REPL_PORT} \
            --replication-key ${REPO_DIR}/repl.key \
            --journal ${replica2_journal} \
            --authorized-keys ${REPO_DIR}/authorized_keys \
            --dpdk-eal-args='${replica2_eal}' \
            --dpdk-ip ${REPLICA2_DPDK_IP} \
            --dpdk-prefix-len ${REPLICA2_DPDK_PREFIX} \
            --dpdk-ports ${REPLICA2_DPDK_PORT} \
            ${REPLICA_EXTRA_ARGS:-} \
        >/tmp/melin-server.log 2>&1 </dev/null &" </dev/null

    wait_for_log "$SERVER" "/tmp/melin-server.log" "listening" 120 "DPDK primary"

    # TAP mode: routing for bench client.
    if [[ "$DPDK_MODE" == "tap" ]]; then
        setup_tap_routing "$SERVER" "${SERVER_DPDK_IP}"
        add_tap_route "$BENCH" "${SERVER_DPDK_IP}" "${SERVER_PUB}"
        BENCH_DPDK_ARGS=""
    else
        local bench_eal="--huge-dir=${HUGE_DIR}"
        if [[ -n "${BENCH_DPDK_EAL_ARGS:-}" ]]; then
            bench_eal="${BENCH_DPDK_EAL_ARGS} ${bench_eal}"
        fi
        BENCH_DPDK_ARGS="--dpdk-eal-args='${bench_eal}' --dpdk-ports ${BENCH_DPDK_PORT} --dpdk-core ${BENCH_DPDK_CORE}"
        if [[ -n "${BENCH_DPDK_IP:-}" ]]; then
            BENCH_DPDK_ARGS="${BENCH_DPDK_ARGS} --dpdk-ip ${BENCH_DPDK_IP} --dpdk-prefix-len ${BENCH_DPDK_PREFIX}"
        fi
    fi

    CURRENT_BIND="${SERVER_DPDK_IP}:9876"
    # Health endpoint stays on kernel TCP via SERVER_VLAN even in DPDK mode
    # — same as transport_start_dpdk. Empty would break --health-addr arg.
    CURRENT_HEALTH="${SERVER_VLAN}:9878"
    DPDK_RAN=1

    perf_capture_start "dpdk-dual-repl"
}

transport_stop_dpdk_dual_repl() {
    perf_capture_stop
    stop_servers "$SERVER" "$REPLICA" "$REPLICA2"
    for host in "$SERVER" "$REPLICA" "$REPLICA2"; do
        ssh $SSH_OPTS "$host" "pkill -INT -f '[m]elin-server.dpdk' 2>/dev/null; true"
    done
    clean_eal_lockfiles "$SERVER" "$BENCH" "$REPLICA" "$REPLICA2"
    if [[ "${SKIP_JOURNAL_VERIFY:-0}" == "1" ]]; then
        echo "  Skipping journal verification (SKIP_JOURNAL_VERIFY=1)"
        return
    fi
    echo "  Verifying DPDK replication journal consistency (replica1)..."
    "${SCRIPT_DIR}/journal-verify.sh" "$SERVER" "$JOURNAL_PATH" "$REPLICA" "${REPLICA_JOURNAL}"
    echo "  Verifying DPDK replication journal consistency (replica2)..."
    "${SCRIPT_DIR}/journal-verify.sh" "$SERVER" "$JOURNAL_PATH" "$REPLICA2" "${REPLICA2_JOURNAL}"
}

# ---------------------------------------------------------------------------
# Workload functions
# ---------------------------------------------------------------------------

workload_throughput() {
    local transport="$1"
    echo ""
    echo "============================================================"
    echo "  [${transport}] Peak throughput — full durability"
    echo "  ${THROUGHPUT_DURATION} measured, ${THROUGHPUT_CLIENTS} clients, window ${THROUGHPUT_WINDOW}"
    echo "============================================================"
    echo ""

    local warmup_arg=""
    if [[ -n "${WARMUP_DURATION}" ]]; then warmup_arg="--warmup-duration ${WARMUP_DURATION}"; fi
    local cooldown_arg=""
    if [[ -n "${COOLDOWN_DURATION}" ]]; then cooldown_arg="--cooldown-duration ${COOLDOWN_DURATION}"; fi
    local threads_arg=""
    if [[ -n "${BENCH_THREADS:-}" ]]; then threads_arg="--bench-threads ${BENCH_THREADS}"; fi
    local rate_arg=""
    if [[ "${TARGET_RATE}" != "0" ]]; then rate_arg="--target-rate ${TARGET_RATE}"; fi

    if [[ "$transport" == dpdk* ]]; then
        ssh $SSH_OPTS "$BENCH" "cd ${REPO_DIR} && source ~/.cargo/env && \
            ./target/release/melin-bench \
                --addr ${CURRENT_BIND} \
                --health-addr ${CURRENT_HEALTH} \
                --key bench.key \
                --json /tmp/bench-results.json \
                --duration ${THROUGHPUT_DURATION} \
                --accounts ${BENCH_ACCOUNTS} \
                ${BENCH_DPDK_ARGS} ${warmup_arg} ${cooldown_arg} ${threads_arg} ${rate_arg} \
                --clients ${THROUGHPUT_CLIENTS} --window ${THROUGHPUT_WINDOW}"
    elif [[ -n "${rate_arg}" ]]; then
        run_bench "$CURRENT_BIND" "$CURRENT_HEALTH" "${THROUGHPUT_DURATION}" --accounts "${BENCH_ACCOUNTS}" --clients "${THROUGHPUT_CLIENTS}" --window "${THROUGHPUT_WINDOW}" --target-rate "${TARGET_RATE}"
    else
        run_bench "$CURRENT_BIND" "$CURRENT_HEALTH" "${THROUGHPUT_DURATION}" --accounts "${BENCH_ACCOUNTS}" --clients "${THROUGHPUT_CLIENTS}" --window "${THROUGHPUT_WINDOW}"
    fi
    collect_result "${transport}-throughput"
}

workload_single() {
    local transport="$1"
    echo ""
    echo "============================================================"
    echo "  [${transport}] Single-order latency — full durability"
    echo "  ${SINGLE_DURATION} measured, 1 client, window 1"
    echo "============================================================"
    echo ""

    local warmup_arg=""
    if [[ -n "${WARMUP_DURATION}" ]]; then warmup_arg="--warmup-duration ${WARMUP_DURATION}"; fi
    local cooldown_arg=""
    if [[ -n "${COOLDOWN_DURATION}" ]]; then cooldown_arg="--cooldown-duration ${COOLDOWN_DURATION}"; fi

    if [[ "$transport" == dpdk* ]]; then
        ssh $SSH_OPTS "$BENCH" "cd ${REPO_DIR} && source ~/.cargo/env && \
            ./target/release/melin-bench \
                --addr ${CURRENT_BIND} \
                --health-addr ${CURRENT_HEALTH} \
                --key bench.key \
                --json /tmp/bench-results.json \
                --duration ${SINGLE_DURATION} \
                --accounts ${BENCH_ACCOUNTS} \
                ${BENCH_DPDK_ARGS} ${warmup_arg} ${cooldown_arg} \
                --clients 1 --window 1"
    else
        run_bench "$CURRENT_BIND" "$CURRENT_HEALTH" "${SINGLE_DURATION}" --accounts "${BENCH_ACCOUNTS}" --clients 1 --window 1
    fi
    collect_result "${transport}-single"
}

workload_engine_only() {
    echo ""
    echo "============================================================"
    echo "  [local] Engine only — matching engine, no journal, no network"
    echo "  ${LOCAL_DURATION} measured"
    echo "============================================================"
    echo ""

    ssh $SSH_OPTS "$SERVER" "cd ${REPO_DIR} && source ~/.cargo/env && \
        ./target/release/melin-bench \
            --mode engine \
            --json /tmp/bench-results.json \
            --duration ${LOCAL_DURATION}"

    scp $SSH_OPTS -q "${SSH_USER}@${SERVER_PUB}:/tmp/bench-results.json" \
        "${RESULTS_DIR}/local-engine-only.json" 2>/dev/null || true
}

workload_pipeline_only() {
    echo ""
    echo "============================================================"
    echo "  [local] Pipeline — journal + matching, no network"
    echo "  ${LOCAL_DURATION} measured, window 256"
    echo "============================================================"
    echo ""

    clean_journal "$SERVER" "$JOURNAL_PATH"

    ssh $SSH_OPTS "$SERVER" "cd ${REPO_DIR} && source ~/.cargo/env && \
        ./target/release/melin-bench \
            --mode pipeline \
            --window 256 \
            --journal ${JOURNAL_PATH} \
            --json /tmp/bench-results.json \
            --duration ${LOCAL_DURATION}"

    scp $SSH_OPTS -q "${SSH_USER}@${SERVER_PUB}:/tmp/bench-results.json" \
        "${RESULTS_DIR}/local-pipeline-only.json" 2>/dev/null || true
}

# --- Sweeps ---

# Run a sweep: for each config, restart the server and run the bench.
# Usage: run_sweep <sweep-name> <transport> <configs...>
#   Each config is "label:bench-args" or "label:server-args:bench-args"
run_sweep() {
    local sweep_name="$1" transport="$2"
    shift 2
    local sweep_dir="${RESULTS_DIR}/sweep-${sweep_name}"
    mkdir -p "${sweep_dir}"

    echo ""
    echo "============================================================"
    echo "  [${transport}] Sweep: ${sweep_name}"
    echo "  ${SWEEP_DURATION} measured per point"
    echo "============================================================"
    echo ""

    local start_fn="transport_start_${transport//-/_}"
    local stop_fn="transport_stop_${transport//-/_}"

    for config in "$@"; do
        local label="${config%%:*}"
        local rest="${config#*:}"
        local server_extra="" bench_args=""
        if [[ "$rest" == *:* ]]; then
            server_extra="${rest%%:*}"
            bench_args="${rest#*:}"
        else
            bench_args="$rest"
        fi

        echo "--- ${label} ---"

        # For sweeps with server args, we need to set SERVER_EXTRA_ARGS.
        SERVER_EXTRA_ARGS="${server_extra}"
        "${stop_fn}" 2>/dev/null || true
        "${start_fn}"
        run_bench "$CURRENT_BIND" "$CURRENT_HEALTH" "${SWEEP_DURATION}" ${bench_args}
        collect_result "_sweep_tmp"
        cp "${RESULTS_DIR}/_sweep_tmp.json" "${sweep_dir}/${label}.json" 2>/dev/null || true
        rm -f "${RESULTS_DIR}/_sweep_tmp.json"
        "${stop_fn}"
        SERVER_EXTRA_ARGS=""
        echo ""
    done
}

workload_sweep_window() {
    local transport="$1"
    run_sweep "window" "$transport" \
        "w32:--clients 16 --window 32" \
        "w64:--clients 16 --window 64" \
        "w128:--clients 16 --window 128" \
        "w256:--clients 16 --window 256" \
        "w512:--clients 16 --window 512"
}

workload_sweep_clients() {
    local transport="$1"
    run_sweep "clients" "$transport" \
        "c64:--clients 64 --window 64" \
        "c128:--clients 128 --window 32" \
        "c256:--clients 256 --window 16" \
        "c512:--clients 512 --window 8" \
        "c1024:--clients 1024 --window 4"
}

workload_sweep_instruments() {
    local transport="$1"
    run_sweep "instruments" "$transport" \
        "i10:--instruments 10:--clients 16 --window 128" \
        "i100:--instruments 100:--clients 16 --window 128" \
        "i1000:--instruments 1000:--clients 16 --window 128"
}

workload_sweep_accounts() {
    local transport="$1"
    run_sweep "accounts" "$transport" \
        "a100000:--accounts 100000:--clients 16 --window 128 --accounts 100000" \
        "a1000000:--accounts 1000000:--clients 16 --window 128 --accounts 1000000" \
        "a10000000:--accounts 10000000:--clients 16 --window 128 --accounts 10000000"
}

# ---------------------------------------------------------------------------
# Main execution loop
# ---------------------------------------------------------------------------

# Run local workloads first (no transport needed).
for workload in "${LOCAL_MATRIX[@]+"${LOCAL_MATRIX[@]}"}"; do
    fn="workload_${workload//-/_}"
    echo ""
    echo ">>> Running local workload: ${workload}"
    "$fn"
done

# Group by transport: start/stop server per workload, but do global setup
# (DPDK sriov) only once.
# Collect unique transports in order.
declare -A SEEN_TRANSPORTS
ORDERED_TRANSPORTS=()
for item in "${MATRIX[@]}"; do
    t="${item%%:*}"
    if [[ -z "${SEEN_TRANSPORTS[$t]:-}" ]]; then
        SEEN_TRANSPORTS[$t]=1
        ORDERED_TRANSPORTS+=("$t")
    fi
done

for transport in "${ORDERED_TRANSPORTS[@]}"; do
    start_fn="transport_start_${transport//-/_}"
    stop_fn="transport_stop_${transport//-/_}"

    # Collect workloads for this transport.
    TRANSPORT_WORKLOADS=()
    for item in "${MATRIX[@]}"; do
        if [[ "${item%%:*}" == "$transport" ]]; then
            TRANSPORT_WORKLOADS+=("${item#*:}")
        fi
    done

    # Separate sweep workloads (they handle their own start/stop) from
    # regular workloads (need start before, stop after).
    REGULAR_WORKLOADS=()
    SWEEP_WORKLOADS=()
    for w in "${TRANSPORT_WORKLOADS[@]}"; do
        case "$w" in
            sweep-*) SWEEP_WORKLOADS+=("$w") ;;
            *) REGULAR_WORKLOADS+=("$w") ;;
        esac
    done

    # Restart the server between each workload so per-key dedup HWM
    # state from one workload (e.g. throughput's 16-client ramp-up to
    # ~10M seq) does not reject every request from the next workload
    # (e.g. single-client which restarts seq at 1 and would be 100%
    # DuplicateRequest against the stale HWM).
    if [[ ${#REGULAR_WORKLOADS[@]} -gt 0 ]]; then
        echo ""
        echo ">>> Starting transport: ${transport}"

        first=1
        for workload in "${REGULAR_WORKLOADS[@]}"; do
            if [[ $first -eq 0 ]]; then
                echo ""
                echo ">>> Restarting servers between workloads (fresh dedup state)"
                "$stop_fn"
            fi
            "$start_fn"
            first=0

            fn="workload_${workload//-/_}"
            "$fn" "$transport"
        done

        "$stop_fn"
    fi

    # Run sweep workloads (each manages its own server lifecycle).
    for workload in "${SWEEP_WORKLOADS[@]}"; do
        fn="workload_${workload//-/_}"
        "$fn" "$transport"
    done
done

# ---------------------------------------------------------------------------
# DPDK cleanup: reboot if any DPDK transport ran
# ---------------------------------------------------------------------------
if [[ "$DPDK_RAN" == "1" && "$DPDK_MODE" != "tap" && "${SKIP_REBOOT:-0}" != "1" ]]; then
    echo ""
    echo "============================================================"
    echo "  Rebooting all machines to clean up DPDK state"
    echo "============================================================"
    echo ""
    REBOOT_HOSTS=("$SERVER" "$BENCH")
    if [[ -n "$REPLICA" ]]; then REBOOT_HOSTS+=("$REPLICA"); fi
    if [[ -n "$REPLICA2" ]]; then REBOOT_HOSTS+=("$REPLICA2"); fi
    for HOST in "${REBOOT_HOSTS[@]}"; do
        echo "  Rebooting ${HOST}..."
        ssh $SSH_OPTS "$HOST" "nohup bash -c 'sleep 1 && reboot' >/dev/null 2>&1 &" </dev/null || true
    done
    echo "  Reboot initiated."
fi

# ---------------------------------------------------------------------------
# Generate plots
# ---------------------------------------------------------------------------
if [[ "$RUN_PLOTS" == "1" ]]; then
    echo ""
    echo "============================================================"
    echo "  Generating plots"
    echo "============================================================"
    echo ""

    if command -v cargo &>/dev/null && [[ -f "${SCRIPT_DIR}/../crates/exchange/bench/src/plot.rs" ]]; then
        # Plots land alongside the run's JSON files so each results
        # directory is self-contained — two runs kept in /tmp can be
        # compared visually. The in-tree docs/plots/ copy is updated
        # manually when a run is good enough to publish.
        RUN_PLOT_DIR="${RESULTS_DIR}/plots"
        mkdir -p "${RUN_PLOT_DIR}"

        echo "  Building plot tool..."
        (cd "$LOCAL_REPO" && cargo build --release -p melin-bench --features plot --bin melin-plot 2>&1 | tail -1)
        PLOT_TOOL="${LOCAL_REPO}/target/release/melin-plot"

        # Latency CDF — throughput-style results (both durable and
        # `-no-persist` variants so the two can be overlaid).
        CDF_FILES=()
        for f in "${RESULTS_DIR}"/*-throughput.json "${RESULTS_DIR}"/*-throughput-no-persist.json; do
            [[ -f "$f" ]] && CDF_FILES+=("$f")
        done
        if [[ ${#CDF_FILES[@]} -gt 0 ]]; then
            echo "  Generating latency CDF..."
            "${PLOT_TOOL}" latency-cdf -o "${RUN_PLOT_DIR}/latency-cdf.svg" "${CDF_FILES[@]}" 2>&1
        fi

        # Sweep plots.
        for sweep in window clients instruments accounts; do
            dir="${RESULTS_DIR}/sweep-${sweep}"
            if [[ -d "$dir" ]] && ls "${dir}"/*.json &>/dev/null; then
                echo "  Generating sweep plot: ${sweep}..."
                "${PLOT_TOOL}" sweep -o "${RUN_PLOT_DIR}/saturation-${sweep}.svg" "${dir}"/*.json 2>&1
            fi
        done

        # Latency stability and health plots — all non-sweep JSON files.
        for f in "${RESULTS_DIR}"/*.json; do
            [[ -f "$f" ]] || continue
            label="$(basename "$f" .json)"
            echo "  Generating stability: ${label}..."
            "${PLOT_TOOL}" stability -o "${RUN_PLOT_DIR}/latency-stability-${label}.svg" "$f" 2>&1 || true
            echo "  Generating health: ${label}..."
            "${PLOT_TOOL}" health -o "${RUN_PLOT_DIR}/health-${label}" "$f" 2>&1 || true
        done

        echo ""
        echo "  Plots written to ${RUN_PLOT_DIR}/"
    fi
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "============================================================"
echo "  Suite complete. Results in ${RESULTS_DIR}/"
echo "============================================================"
echo ""
find "${RESULTS_DIR}" -type f | sort
