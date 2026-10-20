#!/usr/bin/env bash
# Benchmark a Melin application across a LAN: a primary, two synchronous
# replicas and a load-generating host, over kernel TCP and DPDK.
#
# The binaries are built here, on the machine running the suite, for the
# bench hosts' CPU (`-C target-cpu=<theirs>`), against a sysroot taken
# from a bench host so that glibc and libdpdk are exactly the ones the
# binaries will run with; then copied to every host. Nothing is built on
# the bench hosts, which need only `server-setup.sh`.
#
# Applications (first argument):
#   echo     echo-server, loaded by melin-bench (crates/tools/melin-bench)
#   notary   notary-server, loaded by melin-bench
#   ec       the Exchange Core's melin-ec-server and melin-ec-bench, built
#            from a sibling checkout (EC_REPO, default ../exchange-core)
#
# Transports (how the cluster runs, always a primary and two replicas):
#   kernel  kernel TCP, clients and replication
#   dpdk    DPDK, clients and replication; the load generator on DPDK too
#
# Workloads (what the load generator does):
#   throughput  THROUGHPUT_CLIENTS × THROUGHPUT_WINDOW in flight
#   single      one client, one request in flight: the latency floor
#
# Usage:
#   ./scripts/bench/lan-bench-suite.sh <app> <server-pub-ip> <bench-pub-ip> <server-vlan-ip> \
#       <replica-pub-ip> <replica2-pub-ip>
#
# Examples:
#   # echo, kernel TCP throughput (the default):
#   ./scripts/bench/lan-bench-suite.sh echo 84.32.176.142 84.32.176.143 10.0.0.1 \
#       84.32.176.144 84.32.176.145
#
#   # notary, both workloads on both transports:
#   TRANSPORTS=kernel,dpdk WORKLOADS=throughput,single ./scripts/bench/lan-bench-suite.sh notary ...
#
# Environment:
#   TRANSPORTS=<list>     comma-separated, or `all` (default: kernel)
#   WORKLOADS=<list>      comma-separated, or `all` (default: throughput)
#   SSH_USER              the user the hosts are reached as (default root;
#                         another needs passwordless sudo)
#   SSH_CONFIG            an ssh_config file the hosts are reached through,
#                         their addresses then being its Host aliases (the
#                         container rig, test-containers.sh, uses one)
#   THROUGHPUT_DURATION   measured phase of a throughput run (default 60s)
#   THROUGHPUT_CLIENTS    connections (default 4)
#   THROUGHPUT_WINDOW     requests in flight per connection (default 56)
#   SINGLE_DURATION       measured phase of a single run (default 30s)
#   WARMUP_DURATION       load before the measured phase (default 5s)
#   COOLDOWN_DURATION     load after it, untimed (default 5s)
#   SETTLE_SECS           seconds between both replicas live and the load
#                         starting (default 5; 0 starts it at once)
#   ACK_POLICY            the cluster's ack policy (default disk+ram)
#   BENCH_CORE            core the load generator's loop is pinned to
#                         (default 7)
#   SERVER_EXTRA_ARGS     appended to the primary's command line
#   REPLICA_EXTRA_ARGS    appended to each replica's; neither may carry a
#                         snapshot flag (refused), as the suite sets every
#                         node's --journal, --snapshot-path and
#                         --snapshot-interval-ms itself
#   BENCH_EXTRA_ARGS      appended to the load generator's
#   ECHO_SIZE             bytes per echo request (default 64)
#   EC_REPO               the Exchange Core checkout (default ../exchange-core)
#   EC_ACCOUNTS           funded accounts the Exchange Core seeds and the
#                         bench trades on (default 100000)
#   TARGET_CPU            the CPU to build for, instead of asking the hosts
#   SYSROOT               a sysroot to build against, instead of the cached
#                         one taken from the server host
#   PREBUILT_BIN_DIR      a local directory whose binaries replace the
#                         suite's builds of the same name (e.g.
#                         melin-ec-server, melin-ec-bench): an A/B against
#                         a build made elsewhere
#   JOURNAL_DIR           where the nodes journal (default /mnt/journal)
#   SNAPSHOT_INTERVAL_MS  the nodes' shadow snapshot interval (default
#                         10000); at least 1000 and at most the shortest
#                         measured phase, or 0 to turn snapshots off
#   SNAPSHOT_DIR          where the nodes write their snapshots, each as
#                         <node>.snapshot (default
#                         /var/lib/melin-bench/snapshots); on a disk other
#                         than JOURNAL_DIR's, which the suite checks
#   SNAPSHOT_SHARED_DISK_OK=1  warn rather than refuse when SNAPSHOT_DIR
#                         cannot be shown to be on another disk than
#                         JOURNAL_DIR: for rehearsals only (test-containers.sh)
#   RESULTS_DIR           default bench-results/lan-<app>-<timestamp>
#   RUST_LOG              the nodes' log filter (default info; with
#                         snapshots on, the shadow's info lines are kept
#                         whatever it says)
#   SKIP_REBOOT=1         skip the reboot that follows SR-IOV DPDK runs
#
# Local prerequisites: rustup (the hosts' target, x86_64-unknown-linux-gnu
# on bench hardware, is added if missing), clang, lld, python3, ssh. Remote: server-setup.sh on
# every host, root or passwordless sudo, and a private network between
# them (the VLAN addresses). The DPDK transport also needs
# /etc/melin-dpdk.conf on each host; a host without one gets
# scripts/dpdk/dpdk-setup.sh run on it, which configures SR-IOV on its
# own (behind a bond, or on a port with a kernel VLAN interface) and
# needs DPDK_IP for mlx5 (run it by hand there).
#
# Each run waits for both replicas to be live before the load starts,
# and samples the primary's health metrics throughout: a replica
# evicted, missing or catching up at any point marks the run unsteady.
#
# Snapshots are on by default, as in production: every node's shadow
# saves one every SNAPSHOT_INTERVAL_MS to SNAPSHOT_DIR, which must be on
# another disk than the journal (checked once, after deploy, failing
# closed). After each run every node's log is judged: a node with too few
# saves during the load for the measured phase, a failed save, or a save
# to another path fails the run. The writes to each node's snapshot and
# journal disks during the run go beside the verdict, in
# <transport>-<workload>.snapshots; every setting goes in suite.conf.
#
# The suite exits non-zero if any run was unsteady, could not start, or
# failed its snapshot verdict.

set -euo pipefail

if [[ $# -ne 6 ]]; then
    echo "usage: $0 <echo|notary|ec> <server-pub-ip> <bench-pub-ip> <server-vlan-ip> <replica-pub-ip> <replica2-pub-ip>" >&2
    exit 1
fi

APP="$1"
SERVER_PUB="$2"
BENCH_PUB="$3"
# The primary's address on the private network: where clients and
# replicas reach it on kernel TCP, and its health endpoint on both
# transports. The replicas need none of theirs: they dial out, and on
# DPDK their addresses come from their own /etc/melin-dpdk.conf.
SERVER_VLAN="$4"
REPLICA_PUB="$5"
REPLICA2_PUB="$6"
SSH_USER="${SSH_USER:-root}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MELIN_REPO="$(cd "${SCRIPT_DIR}/../.." && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"
EC_REPO="${EC_REPO:-${MELIN_REPO}/../exchange-core}"

# On every host, under the SSH user's home: the binaries, keys and
# authorized_keys this suite copies there. Relative for scp; through `~`
# in a remote shell's command.
REMOTE_REL=melin-bench
REMOTE_DIR="~/${REMOTE_REL}"
JOURNAL_DIR="${JOURNAL_DIR:-/mnt/journal}"
CLIENT_PORT=9876
REPL_PORT=9877
HEALTH_PORT=9878
NODE_LOG=/tmp/melin-bench-node.log
BENCH_JSON=/tmp/melin-bench-results.json

THROUGHPUT_DURATION="${THROUGHPUT_DURATION:-60s}"
THROUGHPUT_CLIENTS="${THROUGHPUT_CLIENTS:-4}"
THROUGHPUT_WINDOW="${THROUGHPUT_WINDOW:-56}"
SINGLE_DURATION="${SINGLE_DURATION:-30s}"
WARMUP_DURATION="${WARMUP_DURATION:-5s}"
COOLDOWN_DURATION="${COOLDOWN_DURATION:-5s}"
ACK_POLICY="${ACK_POLICY:-disk+ram}"
# Not 0: a replica that has only just caught up is still cold, and full
# load arriving within a second or two evicts it into a rejoin loop it
# cannot leave at that rate. A workaround, until the fix described in
# docs/internal/roadmap.md, "A replica that falls behind at full rate
# cannot rejoin, and a new history serves before its replicas are live",
# makes a node wait for warm replicas itself.
SETTLE_SECS="${SETTLE_SECS:-5}"
BENCH_CORE="${BENCH_CORE:-7}"
SERVER_EXTRA_ARGS="${SERVER_EXTRA_ARGS:-}"
REPLICA_EXTRA_ARGS="${REPLICA_EXTRA_ARGS:-}"
BENCH_EXTRA_ARGS="${BENCH_EXTRA_ARGS:-}"
ECHO_SIZE="${ECHO_SIZE:-64}"
EC_ACCOUNTS="${EC_ACCOUNTS:-100000}"
NODE_RUST_LOG="${RUST_LOG:-info}"
SNAPSHOT_INTERVAL_MS="${SNAPSHOT_INTERVAL_MS:-10000}"
# On the root filesystem: /tmp is tmpfs on current Debian, and a snapshot
# written there never reaches a disk.
SNAPSHOT_DIR="${SNAPSHOT_DIR:-/var/lib/melin-bench/snapshots}"
SNAPSHOT_SHARED_DISK_OK="${SNAPSHOT_SHARED_DISK_OK:-0}"

RESULTS_DIR="${RESULTS_DIR:-${MELIN_REPO}/bench-results/lan-${APP}-$(date +%Y%m%d-%H%M%S)}"
mkdir -p "$RESULTS_DIR"

# ---------------------------------------------------------------------------
# The application profile: which binaries, how they are built, and what
# a node and the load generator are told beyond what every node is.
# ---------------------------------------------------------------------------
case "$APP" in
    echo)
        SERVER_BIN=echo-server
        BENCH_BIN=melin-bench
        CLIENT_ROLE=writer
        APP_SERVER_ARGS=""
        ;;
    notary)
        SERVER_BIN=notary-server
        BENCH_BIN=melin-bench
        CLIENT_ROLE=submitter
        APP_SERVER_ARGS=""
        ;;
    ec)
        SERVER_BIN=melin-ec-server
        BENCH_BIN=melin-ec-bench
        CLIENT_ROLE=trader
        # The default rate limits are an exchange's, not a benchmark's.
        APP_SERVER_ARGS="--accounts ${EC_ACCOUNTS} --max-orders-per-second 10000000 --max-orders-burst 50000000"
        if [[ ! -f "${EC_REPO}/Cargo.toml" ]]; then
            echo "error: no Exchange Core checkout at ${EC_REPO} (set EC_REPO)" >&2
            exit 1
        fi
        ;;
    *)
        echo "error: unknown application '${APP}': echo, notary or ec" >&2
        exit 1
        ;;
esac

# ---------------------------------------------------------------------------
# SSH
# ---------------------------------------------------------------------------
SSH_CONTROL_DIR="$(mktemp -d -t melin-bench-ssh.XXXXXX)"
# ControlMaster multiplexes every later ssh to a host over its first
# connection: dozens of calls per run, one handshake per host.
SSH_OPTS=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR
    -o ControlMaster=auto -o "ControlPath=${SSH_CONTROL_DIR}/%r@%h:%p" -o ControlPersist=5m)
[[ -n "${SSH_CONFIG:-}" ]] && SSH_OPTS=(-F "$SSH_CONFIG" "${SSH_OPTS[@]}")
SERVER="${SSH_USER}@${SERVER_PUB}"
BENCH="${SSH_USER}@${BENCH_PUB}"
REPLICA="${SSH_USER}@${REPLICA_PUB}"
REPLICA2="${SSH_USER}@${REPLICA2_PUB}"
# Root-only remote work (journals, launching nodes, DPDK, IRQs) goes
# through this: nothing for root, `sudo -n` otherwise, which fails loudly
# rather than prompting when sudo needs a password.
if [[ "$SSH_USER" == "root" ]]; then SUDO=""; else SUDO="sudo -n"; fi

rssh() { local host="$1"; shift; ssh "${SSH_OPTS[@]}" "$host" "$@" </dev/null; }
rscp() { scp -q "${SSH_OPTS[@]}" "$@"; }

HOSTS=("$SERVER" "$BENCH" "$REPLICA" "$REPLICA2")

# Every process this suite starts runs a binary from REMOTE_DIR, so one
# pattern stops them all and nothing else. Bracketed so it does not match
# the remote shell running the pkill, whose command line holds it; a
# command that also names a binary's path must not carry it either.
BIN_PATTERN='[m]elin-bench/bin/'
DPDK_RAN=0
FAILED_RUNS=()

# `sample_metrics <file> <pattern>`: in the background, the primary's
# /metrics lines matching `pattern` (an extended regex), every second
# until stop_samplers, into `file`, each scrape after a `# sample` line.
# Scraped from the bench host over the private network: a shell loop
# forking curl on the primary would share core 0 with its network
# interrupts.
SAMPLERS=()
sample_metrics() {
    rssh "$BENCH" "while :; do echo '# sample'; curl -s --max-time 1 http://${SERVER_VLAN}:${HEALTH_PORT}/metrics | grep -E '$2'; sleep 1; done" > "$1" 2>/dev/null &
    SAMPLERS+=("$!")
}

# Stop every sampler sample_metrics started. Each is a subshell running
# rssh, whose ssh outlives it unless stopped first: left running, it would
# keep appending to its file for the rest of the suite. Defined ahead of
# `cleanup`, which calls it, since the suite can exit before reaching
# any run.
stop_samplers() {
    local sampler
    for sampler in "${SAMPLERS[@]}"; do
        pkill -P "$sampler" 2>/dev/null || true
        kill "$sampler" 2>/dev/null || true
        wait "$sampler" 2>/dev/null || true
    done
    SAMPLERS=()
}

cleanup() {
    local host
    # A run interrupted mid-way leaves its sampler running: its ssh, not
    # the subshell `jobs -p` lists below, is what keeps the remote loop.
    stop_samplers
    for host in "${HOSTS[@]}"; do
        # Best-effort: a host may be unreachable or already clean.
        rssh "$host" "${SUDO} pkill -INT -f '${BIN_PATTERN}'" 2>/dev/null || true
    done
    jobs -p | xargs -r kill 2>/dev/null || true
    for host in "${HOSTS[@]}"; do
        ssh "${SSH_OPTS[@]}" -O exit "$host" 2>/dev/null || true
    done
    rm -rf "$SSH_CONTROL_DIR"
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# The matrix
# ---------------------------------------------------------------------------
ALL_TRANSPORTS=(kernel dpdk)
ALL_WORKLOADS=(throughput single)

TRANSPORTS="${TRANSPORTS:-kernel}"
[[ "$TRANSPORTS" == all ]] && TRANSPORTS=$(IFS=,; echo "${ALL_TRANSPORTS[*]}")
WORKLOADS="${WORKLOADS:-throughput}"
[[ "$WORKLOADS" == all ]] && WORKLOADS=$(IFS=,; echo "${ALL_WORKLOADS[*]}")

IFS=, read -ra RUN_TRANSPORTS <<< "$TRANSPORTS"
IFS=, read -ra REQ_WORKLOADS <<< "$WORKLOADS"
NEED_KERNEL=0
NEED_DPDK=0
for t in "${RUN_TRANSPORTS[@]}"; do
    case "$t" in
        kernel) NEED_KERNEL=1 ;;
        dpdk) NEED_DPDK=1 ;;
        *)
            echo "error: unknown transport '$t': ${ALL_TRANSPORTS[*]}" >&2
            exit 1
            ;;
    esac
done
for w in "${REQ_WORKLOADS[@]}"; do
    if [[ ! " ${ALL_WORKLOADS[*]} " == *" $w "* ]]; then
        echo "error: unknown workload '$w': ${ALL_WORKLOADS[*]}" >&2
        exit 1
    fi
done

# The name of the variable holding `workload`'s measured phase.
workload_duration_var() {
    case "$1" in
        throughput) echo THROUGHPUT_DURATION ;;
        single) echo SINGLE_DURATION ;;
    esac
}

# ---------------------------------------------------------------------------
# Snapshot settings: refused here, before anything is built, rather than
# by a node that then never logs its ready line
# ---------------------------------------------------------------------------
for var in SERVER_EXTRA_ARGS REPLICA_EXTRA_ARGS; do
    if flag=$(extra_args_conflict "${!var}"); then
        case "$flag" in
            --snapshot-interval-ms) instead="set SNAPSHOT_INTERVAL_MS instead" ;;
            --snapshot-path) instead="set SNAPSHOT_DIR instead" ;;
            *) instead="the flag no longer exists; set SNAPSHOT_DIR to choose where snapshots go" ;;
        esac
        echo "error: ${var} carries ${flag}, which the suite sets itself: ${instead}" >&2
        exit 1
    fi
done
for var in JOURNAL_DIR SNAPSHOT_DIR; do
    if ! reason=$(remote_dir_check "$var" "${!var}"); then
        echo "error: ${reason}" >&2
        exit 1
    fi
done
# The measured phase of each workload run, in milliseconds, for the
# snapshot verdict: set only while snapshots are on, the one case a
# duration must be one the suite can read.
declare -A MEASURED_MS=()
if snapshots_off "$SNAPSHOT_INTERVAL_MS"; then
    SNAPSHOT_INTERVAL_MS=0
else
    shortest=""
    for w in "${REQ_WORKLOADS[@]}"; do
        var=$(workload_duration_var "$w")
        if ! ms=$(duration_ms "${!var}"); then
            echo "error: ${var}: ${ms}" >&2
            exit 1
        fi
        MEASURED_MS[$w]="$ms"
        if [[ -z "$shortest" ]] || (( ms < shortest )); then shortest="$ms"; fi
    done
    if ! reason=$(snapshot_settings_check "$SNAPSHOT_INTERVAL_MS" "$shortest"); then
        echo "error: ${reason}" >&2
        exit 1
    fi
    # Without its leading zeros, which bash arithmetic would read as octal.
    SNAPSHOT_INTERVAL_MS=$((10#$SNAPSHOT_INTERVAL_MS))
    # The lines each run is judged on, whatever RUST_LOG filters out.
    NODE_RUST_LOG+=",melin_transport_core::shadow=info"
fi
if [[ "$SNAPSHOT_SHARED_DISK_OK" != 0 && "$SNAPSHOT_SHARED_DISK_OK" != 1 ]]; then
    echo "error: SNAPSHOT_SHARED_DISK_OK='${SNAPSHOT_SHARED_DISK_OK}' is neither 0 nor 1" >&2
    exit 1
fi

# Every setting this suite runs with, one `KEY=value` per line: the
# console's header and `suite.conf`, from this one list. What is learnt
# later (the build's CPU, the disks the guard resolves) is added to both
# by record_setting.
suite_settings() {
    local rev ec_rev var
    # Best-effort: a tree that is not a git checkout records `unknown`
    # rather than stopping the suite.
    rev=$(git -C "$MELIN_REPO" describe --always --dirty 2>/dev/null) || rev=unknown
    printf '%s=%s\n' APP "$APP" MELIN_REVISION "$rev"
    if [[ "$APP" == ec ]]; then
        # Best-effort, as above.
        ec_rev=$(git -C "$EC_REPO" describe --always --dirty 2>/dev/null) || ec_rev=unknown
        printf '%s=%s\n' EC_REPO "$EC_REPO" EC_REVISION "$ec_rev" EC_ACCOUNTS "$EC_ACCOUNTS"
    fi
    [[ "$APP" == echo ]] && printf '%s=%s\n' ECHO_SIZE "$ECHO_SIZE"
    printf '%s=%s\n' SERVER "$SERVER_PUB" BENCH "$BENCH_PUB" SERVER_VLAN "$SERVER_VLAN" \
        REPLICA "$REPLICA_PUB" REPLICA2 "$REPLICA2_PUB" SSH_USER "$SSH_USER" \
        TRANSPORTS "$(IFS=,; echo "${RUN_TRANSPORTS[*]}")" WORKLOADS "$(IFS=,; echo "${REQ_WORKLOADS[*]}")"
    for var in THROUGHPUT_DURATION THROUGHPUT_CLIENTS THROUGHPUT_WINDOW SINGLE_DURATION \
        WARMUP_DURATION COOLDOWN_DURATION ACK_POLICY BENCH_CORE JOURNAL_DIR \
        SNAPSHOT_INTERVAL_MS SNAPSHOT_DIR SNAPSHOT_SHARED_DISK_OK \
        SERVER_EXTRA_ARGS REPLICA_EXTRA_ARGS BENCH_EXTRA_ARGS; do
        printf '%s=%s\n' "$var" "${!var}"
    done
    printf '%s=%s\n' SETTLE_SECS "$SETTLE_SECS" RUST_LOG "$NODE_RUST_LOG" \
        PREBUILT_BIN_DIR "${PREBUILT_BIN_DIR:-}" RESULTS_DIR "$RESULTS_DIR"
}

# `record_setting <key> <value>`: a setting learnt after the header, in
# suite.conf and on the console in the header's form.
record_setting() {
    printf '%s=%s\n' "$1" "$2" | tee -a "${RESULTS_DIR}/suite.conf" | sed 's/^/  /'
}

echo "=== Melin LAN suite: ${APP} ==="
suite_settings > "${RESULTS_DIR}/suite.conf"
sed 's/^/  /' "${RESULTS_DIR}/suite.conf"
echo ""

# ---------------------------------------------------------------------------
# Toolchain: the hosts' CPU, the sysroot, the local tools
# ---------------------------------------------------------------------------
for tool in clang ld.lld python3 rustup; do
    if ! command -v "$tool" >/dev/null; then
        echo "error: ${tool} not found on this machine" >&2
        exit 1
    fi
done

# What gcc on `host` takes `-march=native` to be: the host's CPU by the
# name LLVM also uses. x86 only: elsewhere gcc names an architecture
# level rather than a CPU LLVM knows, and the build targets `generic`
# unless TARGET_CPU says otherwise.
host_cpu() {
    case "$2" in
        x86_64) rssh "$1" "gcc -march=native -Q --help=target 2>/dev/null | awk '\$1 == \"-march=\" { print \$2; exit }'" ;;
        *) echo generic ;;
    esac
}

# The architecture, distribution and libdpdk the sysroot must match.
host_platform() {
    rssh "$1" '. /etc/os-release && printf "%s-%s-%s-dpdk%s\n" "$(uname -m)" "$ID" "$VERSION_ID" "$(dpkg-query -W -f="\${Version}" libdpdk-dev 2>/dev/null || echo none)"'
}

echo "=== Inspecting the hosts ==="
PLATFORM=""
DETECTED_CPU=""
ARCH=""
for host in "${HOSTS[@]}"; do
    arch=$(rssh "$host" uname -m)
    ARCH="${ARCH:-$arch}"
    platform=$(host_platform "$host")
    cpu=$(host_cpu "$host" "$arch")
    echo "  ${host}: ${platform}, cpu ${cpu:-unknown}"
    if [[ -n "$PLATFORM" && "$platform" != "$PLATFORM" ]]; then
        echo "error: the hosts run different architectures, distributions or libdpdk versions" >&2
        echo "       (${PLATFORM} and ${platform}); one build cannot serve both" >&2
        exit 1
    fi
    PLATFORM="$platform"
    if [[ -z "${TARGET_CPU:-}" ]]; then
        if [[ -z "$cpu" ]]; then
            echo "error: cannot ask gcc on ${host} for its CPU; set TARGET_CPU" >&2
            exit 1
        fi
        if [[ -n "$DETECTED_CPU" && "$cpu" != "$DETECTED_CPU" ]]; then
            echo "error: the hosts have different CPUs (${DETECTED_CPU} and ${cpu});" >&2
            echo "       set TARGET_CPU to one every host runs (e.g. the older)" >&2
            exit 1
        fi
        DETECTED_CPU="$cpu"
    fi
done
TARGET_CPU="${TARGET_CPU:-$DETECTED_CPU}"
# The hosts' Rust target, and its multiarch directory under /usr/lib.
TARGET="${ARCH}-unknown-linux-gnu"
MULTIARCH="${ARCH}-linux-gnu"
# The target in the spellings cargo's and the cc crate's variables take.
TARGET_ENV="${TARGET//-/_}"
TARGET_ENV_UPPER="${TARGET_ENV^^}"
if ! rustup target list --installed | grep -qx "$TARGET"; then
    echo "=== Adding the ${TARGET} Rust target ==="
    rustup target add "$TARGET"
fi
if ! rustc --print target-cpus --target "$TARGET" | awk '{print $1}' | grep -qx "$TARGET_CPU"; then
    echo "error: rustc does not know the CPU '${TARGET_CPU}'; set TARGET_CPU" >&2
    exit 1
fi
echo "  building for:"
record_setting PLATFORM "$PLATFORM"
record_setting TARGET "$TARGET"
record_setting TARGET_CPU "$TARGET_CPU"

# The sysroot: the server host's headers and libraries, taken once per
# architecture, distribution and libdpdk version and kept. `tar -h`
# follows symlinks, so a library reached through an absolute link (or
# /lib64, itself one) arrives as a file; the merged-/usr /lib link is
# recreated after. The dynamic loader comes too (/lib64's on x86_64,
# /usr/lib's elsewhere): glibc's linker script names it.
if [[ -z "${SYSROOT:-}" ]]; then
    SYSROOT="${XDG_CACHE_HOME:-${HOME}/.cache}/melin-bench/sysroot-${PLATFORM}"
    if [[ ! -f "${SYSROOT}/.complete" ]]; then
        echo "=== Taking a sysroot from ${SERVER} into ${SYSROOT} ==="
        rm -rf "$SYSROOT"
        mkdir -p "$SYSROOT"
        # `--ignore-failed-read`: a dangling link among the libraries is
        # not ours to fix and is never linked against.
        rssh "$SERVER" "cd / && tar -chf - --ignore-failed-read --warning=no-failed-read \
            --exclude='usr/lib/${MULTIARCH}/perl*' --exclude='usr/lib/${MULTIARCH}/dri' \
            --exclude='usr/lib/${MULTIARCH}/libLLVM*' --exclude='usr/lib/${MULTIARCH}/gconv' \
            --exclude='usr/lib/${MULTIARCH}/python3*' \
            usr/include usr/lib/${MULTIARCH} usr/lib/gcc/${MULTIARCH} usr/share/pkgconfig \
            \$(ls -d lib64 usr/lib/ld-linux* 2>/dev/null) 2>/dev/null" | tar -xf - -C "$SYSROOT"
        ln -s usr/lib "${SYSROOT}/lib"
        for needed in "usr/lib/${MULTIARCH}/libc.so.6" usr/include/stdio.h; do
            if [[ ! -e "${SYSROOT}/${needed}" ]]; then
                echo "error: the sysroot has no ${needed}; is build-essential installed on ${SERVER}?" >&2
                exit 1
            fi
        done
        touch "${SYSROOT}/.complete"
    fi
fi
if [[ "$NEED_DPDK" == 1 && ! -f "${SYSROOT}/usr/lib/${MULTIARCH}/pkgconfig/libdpdk.pc" ]]; then
    echo "error: the sysroot has no libdpdk; install libdpdk-dev on the hosts (server-setup.sh does)" >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------
# The binaries, copied out of cargo's target directory as each build
# finishes (the kernel and DPDK builds share names), under the names the
# hosts run them by: `<bin>` and `<bin>.dpdk`.
OUT_DIR="${RESULTS_DIR}/bin"
mkdir -p "$OUT_DIR"

# `cargo build` in `repo` for the bench hosts. The target directory is
# the suite's own, so these flags never invalidate a native build.
cross_build() {
    local repo="$1"; shift
    local ar=ar
    command -v llvm-ar >/dev/null && ar=llvm-ar
    (
        cd "$repo"
        export CARGO_TARGET_DIR="${repo}/target/lan-bench"
        # Replaces `.cargo/config.toml`'s flags for the target (a set
        # RUSTFLAGS wins over them): its `target-cpu=native` would name
        # this machine.
        export RUSTFLAGS="-C target-cpu=${TARGET_CPU} -C link-arg=--target=${TARGET} -C link-arg=--sysroot=${SYSROOT} -C link-arg=-fuse-ld=lld"
        export "CARGO_TARGET_${TARGET_ENV_UPPER}_LINKER=clang"
        export "CC_${TARGET_ENV}=clang"
        export "AR_${TARGET_ENV}=${ar}"
        # `-fuse-ld=lld` for build scripts that link C themselves, as
        # jemalloc's configure does: clang would otherwise link with this
        # machine's ld, which may not know the target. Unused on a
        # compile-only step, hence the warning silenced beside it.
        export "CFLAGS_${TARGET_ENV}=--sysroot=${SYSROOT} -fuse-ld=lld -Wno-unused-command-line-argument"
        export "BINDGEN_EXTRA_CLANG_ARGS_${TARGET_ENV}=--sysroot=${SYSROOT}"
        export PKG_CONFIG_ALLOW_CROSS=1
        export PKG_CONFIG_SYSROOT_DIR="$SYSROOT"
        export PKG_CONFIG_LIBDIR="${SYSROOT}/usr/lib/${MULTIARCH}/pkgconfig:${SYSROOT}/usr/share/pkgconfig"
        cargo build --release --target "$TARGET" "$@"
    )
}

# Copy `bin` out of `repo`'s build as `name`.
take() {
    local repo="$1" bin="$2" name="$3"
    cp "${repo}/target/lan-bench/${TARGET}/release/${bin}" "${OUT_DIR}/${name}"
}

echo ""
echo "=== Building for ${TARGET_CPU} ==="
case "$APP" in
    echo) APP_PKG=melin-example-echo ;;
    notary) APP_PKG=melin-example-notary ;;
    ec) APP_PKG="" ;;
esac
# The kernel melin-bench in every case: it prints the authorized_keys
# lines, and loads echo and notary over kernel TCP.
if [[ -n "$APP_PKG" && "$NEED_KERNEL" == 1 ]]; then
    cross_build "$MELIN_REPO" -p "$APP_PKG" --bin "$SERVER_BIN" -p melin-bench --bin melin-bench
    take "$MELIN_REPO" "$SERVER_BIN" "$SERVER_BIN"
else
    cross_build "$MELIN_REPO" -p melin-bench --bin melin-bench
fi
take "$MELIN_REPO" melin-bench melin-bench
if [[ "$APP" == ec ]]; then
    if [[ "$NEED_KERNEL" == 1 ]]; then
        cross_build "$EC_REPO" -p melin-ec-server --bin melin-ec-server --features synthetic-seed
        take "$EC_REPO" melin-ec-server melin-ec-server
    fi
    # In every case, as melin-bench is: it prints the clients' keys.
    cross_build "$EC_REPO" -p melin-ec-bench --bin melin-ec-bench
    take "$EC_REPO" melin-ec-bench melin-ec-bench
fi
if [[ "$NEED_DPDK" == 1 ]]; then
    if [[ -n "$APP_PKG" ]]; then
        cross_build "$MELIN_REPO" -p "$APP_PKG" --bin "$SERVER_BIN" -p melin-bench --bin melin-bench \
            --features "${APP_PKG}/dpdk,melin-bench/dpdk"
        take "$MELIN_REPO" "$SERVER_BIN" "${SERVER_BIN}.dpdk"
        take "$MELIN_REPO" melin-bench melin-bench.dpdk
    else
        cross_build "$EC_REPO" -p melin-ec-server --bin melin-ec-server --no-default-features \
            --features dpdk,hash-chain,release-tracing,synthetic-seed
        take "$EC_REPO" melin-ec-server melin-ec-server.dpdk
        cross_build "$EC_REPO" -p melin-ec-bench --bin melin-ec-bench --features dpdk
        take "$EC_REPO" melin-ec-bench melin-ec-bench.dpdk
    fi
fi

# A binary in PREBUILT_BIN_DIR replaces this suite's build of the same
# name. One that matches nothing is refused: a misspelt name would
# otherwise run the suite's own build while claiming the prebuilt one.
if [[ -n "${PREBUILT_BIN_DIR:-}" ]]; then
    for bin in "${PREBUILT_BIN_DIR}"/*; do
        [[ -f "$bin" ]] || continue
        name=$(basename "$bin")
        if [[ ! -e "${OUT_DIR}/${name}" ]]; then
            echo "error: ${bin} replaces nothing this run builds ($(ls "$OUT_DIR" | tr '\n' ' '))" >&2
            exit 1
        fi
        cp "$bin" "${OUT_DIR}/${name}"
        echo "  using the prebuilt ${name} from ${PREBUILT_BIN_DIR}"
    done
fi

# ---------------------------------------------------------------------------
# Deploy: binaries, keys, authorized_keys
# ---------------------------------------------------------------------------
echo ""
echo "=== Deploying ==="
KEY_DIR="${RESULTS_DIR}/keys"
mkdir -p "$KEY_DIR"
# Raw 32-byte Ed25519 seeds, which every binary here reads; fresh per
# suite run, like the journals.
head -c 32 /dev/urandom > "${KEY_DIR}/bench.key"
head -c 32 /dev/urandom > "${KEY_DIR}/repl.key"
chmod 600 "${KEY_DIR}"/*.key

for host in "${HOSTS[@]}"; do
    rssh "$host" "mkdir -p ${REMOTE_DIR}/bin && rm -f ${REMOTE_DIR}/bin/*"
    rscp "${OUT_DIR}"/* "${host}:${REMOTE_REL}/bin/"
    rscp "${KEY_DIR}"/*.key "${host}:${REMOTE_REL}/"
    rscp "${MELIN_REPO}/scripts/dpdk/dpdk-setup.sh" "${host}:${REMOTE_REL}/"
done

# The load generator's identities, as the application's bench spells
# them (the Exchange Core's derives one key per client from the
# master), and the replicas' key.
if [[ "$APP" == ec ]]; then
    CLIENT_LINES=$(rssh "$BENCH" "${REMOTE_DIR}/bin/melin-ec-bench --key ${REMOTE_DIR}/bench.key --clients 1024 --print-authorized-keys")
else
    CLIENT_LINES=$(rssh "$BENCH" "${REMOTE_DIR}/bin/melin-bench --key ${REMOTE_DIR}/bench.key --print-authorized-key ${CLIENT_ROLE}")
fi
REPL_LINE=$(rssh "$BENCH" "${REMOTE_DIR}/bin/melin-bench --key ${REMOTE_DIR}/repl.key --print-authorized-key replication")
printf '%s\n%s\n' "$CLIENT_LINES" "$REPL_LINE" > "${KEY_DIR}/authorized_keys"
for host in "${HOSTS[@]}"; do
    rscp "${KEY_DIR}/authorized_keys" "${host}:${REMOTE_REL}/"
done

# ---------------------------------------------------------------------------
# Node helpers
# ---------------------------------------------------------------------------
# The nodes, by the names their files and logs carry, and the host each
# runs on.
NODES=(primary replica1 replica2)
node_host() {
    case "$1" in
        primary) echo "$SERVER" ;;
        replica1) echo "$REPLICA" ;;
        replica2) echo "$REPLICA2" ;;
    esac
}

# A node's journal and snapshot: named after the node, so that each is
# its own even where two nodes share a directory, or REPLICA_EXTRA_ARGS
# (shared by both replicas) would have them share a path.
node_journal() { echo "${JOURNAL_DIR}/$1.journal"; }
node_snapshot() { echo "${SNAPSHOT_DIR}/$1.snapshot"; }

# The flags that put a node's journal and snapshots where the suite looks
# for them, on every transport.
node_store_flags() {
    echo "--journal $(node_journal "$1") --snapshot-path $(node_snapshot "$1") --snapshot-interval-ms ${SNAPSHOT_INTERVAL_MS}"
}

# Remove everything a previous run of `node` left on `host`, an aborted
# one's included, by exact name: its journal lineage (a resync's archive
# is a directory, hence `-r`), a snapshot beside the journal (where suite
# versions before --snapshot-path wrote it), and its snapshot, `.prev`,
# `.tmp` and resync archives in SNAPSHOT_DIR. Never a whole directory's
# contents: SNAPSHOT_DIR may be shared with other files. Creates both
# directories, which the node needs to exist.
clean_node_state() {
    local host="$1" node="$2" journal snapshot var reason
    # Checked at start-up too; again here, beside the `rm -rf` it guards.
    for var in JOURNAL_DIR SNAPSHOT_DIR; do
        if ! reason=$(remote_dir_check "$var" "${!var}"); then
            echo "  ${reason}" >&2
            return 1
        fi
    done
    journal=$(node_journal "$node")
    snapshot=$(node_snapshot "$node")
    rssh "$host" "${SUDO} mkdir -p ${JOURNAL_DIR} ${SNAPSHOT_DIR} && ${SUDO} rm -rf ${journal} ${journal}.* ${journal%.journal}.snapshot ${journal%.journal}.snapshot.* ${snapshot} ${snapshot}.*"
}

# ---------------------------------------------------------------------------
# The snapshot disk guard: snapshots on another disk than the journal
# ---------------------------------------------------------------------------
# Per node, the whole disks under its SNAPSHOT_DIR and JOURNAL_DIR as the
# guard resolved them (see disks_of), for the runs' disk writes; empty on
# a side it could not resolve, or with snapshots off.
declare -A SNAPSHOT_DISKS=() JOURNAL_DISKS=()

# `dir_devices <host> <dir>`: `lsblk -rnso NAME,TYPE` of the device
# `dir`'s filesystem is on, on `host`; or why there is none (a filesystem
# in memory, or none found), failing.
dir_devices() {
    local host="$1" dir="$2" mount device tree
    # A findmnt that fails prints nothing, which mount_source reports as
    # unresolved: its own error, on stderr, says why.
    mount=$(rssh "$host" "findmnt -nv -o SOURCE,FSTYPE -T $(printf %q "$dir")") || mount=""
    device=$(mount_source "$mount") || { echo "$device"; return 1; }
    # lsblk refuses a source that is no block device (an overlay's), which
    # shared_disks then reports as unresolved; its error would only repeat
    # that.
    tree=$(rssh "$host" "lsblk -rnso NAME,TYPE $(printf %q "$device")" 2>/dev/null) || tree=""
    echo "$tree"
}

# Refuse (or with SNAPSHOT_SHARED_DISK_OK=1, warn) unless every node's
# SNAPSHOT_DIR is shown to be on other disks than its JOURNAL_DIR: a run
# that cannot show the separation must not claim it. Records the disks.
check_snapshot_disks() {
    local node host snap_tree journal_tree snap_rc journal_rc problem refused=0
    echo ""
    echo "=== Checking the snapshot and journal disks ==="
    for node in "${NODES[@]}"; do
        host=$(node_host "$node")
        if ! rssh "$host" "${SUDO} mkdir -p ${JOURNAL_DIR} ${SNAPSHOT_DIR}"; then
            echo "error: cannot create ${JOURNAL_DIR} and ${SNAPSHOT_DIR} on ${node} (${host})" >&2
            exit 1
        fi
        snap_rc=0
        snap_tree=$(dir_devices "$host" "$SNAPSHOT_DIR") || snap_rc=$?
        journal_rc=0
        journal_tree=$(dir_devices "$host" "$JOURNAL_DIR") || journal_rc=$?
        problem=$(disk_problem "$snap_rc" "$snap_tree" "$journal_rc" "$journal_tree")
        # A directory that did not resolve holds its reason, not a tree:
        # it has no disks to record.
        if (( snap_rc != 0 )); then snap_tree=""; fi
        if (( journal_rc != 0 )); then journal_tree=""; fi
        SNAPSHOT_DISKS[$node]=$(disks_of "$snap_tree")
        JOURNAL_DISKS[$node]=$(disks_of "$journal_tree")
        record_setting "${node}.snapshot_disks" "${SNAPSHOT_DISKS[$node]:--}"
        record_setting "${node}.journal_disks" "${JOURNAL_DISKS[$node]:--}"
        if [[ -n "$problem" ]]; then
            if [[ "$SNAPSHOT_SHARED_DISK_OK" == 1 ]]; then
                echo "  warning: ${node} (${host}): ${problem}; going on, as SNAPSHOT_SHARED_DISK_OK=1 (rehearsals only)"
            else
                echo "error: ${node} (${host}): ${SNAPSHOT_DIR} cannot be shown to be on another disk than ${JOURNAL_DIR}: ${problem}" >&2
                refused=1
            fi
        fi
    done
    if (( refused )); then
        echo "       the snapshots must not share a disk with the journal: put SNAPSHOT_DIR and JOURNAL_DIR on block-device filesystems of distinct disks (is the journal drive mounted?)" >&2
        exit 1
    fi
}

# Every retargetable IRQ to core 0, and the writeback workqueue with them:
# the pinned threads' cores stay quiet. EIOs on IRQs that cannot move are
# expected, as is a read-only /proc in a container. Stderr is redirected
# first, so that it takes a failing redirect's own error.
pin_irqs() {
    rssh "$1" "${SUDO} bash -c 'for irq in /proc/irq/[0-9]*; do printf 1 2>/dev/null > \$irq/smp_affinity; done; echo 1 2>/dev/null > /sys/bus/workqueue/devices/writeback/cpumask; true'"
}

# Start a node on `host`: the binary, then its flags.
launch() {
    local host="$1"; shift
    stop_nodes "$host"
    rssh "$host" "${SUDO} env NO_COLOR=1 RUST_LOG=${NODE_RUST_LOG} nohup ${REMOTE_DIR}/bin/$* >${NODE_LOG} 2>&1 </dev/null &"
}

# Wait for `pattern` in `host`'s node log; if the node exits first or the
# wait times out, show the log's tail and fail.
wait_for_log() {
    local host="$1" pattern="$2" timeout="${3:-120}" rc=0
    rssh "$host" "for _ in \$(seq ${timeout}); do grep -q -- '${pattern}' ${NODE_LOG} && exit 0; pgrep -f '${BIN_PATTERN}' >/dev/null || exit 2; sleep 1; done; exit 1" || rc=$?
    if (( rc != 0 )); then
        if (( rc == 2 )); then
            # A node that logs no error of its own was killed: on a host
            # short of memory, by the OOM killer.
            echo "  the node on ${host} exited before '${pattern}' (with no error below, it was killed: out of memory?):" >&2
        else
            echo "  no '${pattern}' on ${host} within ${timeout}s:" >&2
        fi
        rssh "$host" "tail -20 ${NODE_LOG}" >&2 || true
        return 1
    fi
}

# The primary's health metrics, from the server host itself (the endpoint
# is on the private network).
metrics() {
    rssh "$SERVER" "curl -s --max-time 2 http://${SERVER_VLAN}:${HEALTH_PORT}/metrics"
}

# Wait until `expected` replicas are connected and none is catching up.
wait_for_replicas() {
    local expected="$1" timeout="${2:-120}" sample connected catching
    for _ in $(seq "$timeout"); do
        sample=$(metrics || true)
        connected=$(awk '$1 == "melin_replicas_connected" { print $2 }' <<< "$sample")
        catching=$(awk '$1 ~ /^melin_replica_catching_up\{/ && $2 != 0 { n++ } END { print n + 0 }' <<< "$sample")
        if [[ -n "$connected" ]] && (( connected >= expected && catching == 0 )); then
            return 0
        fi
        sleep 1
    done
    echo "  replicas not live within ${timeout}s (connected=${connected:-?}, catching up=${catching:-?})" >&2
    return 1
}

# Stop every node (and load generator) on `hosts`: SIGINT, up to 10 s for
# them to exit (a DPDK process must release its device before the next
# one starts), then SIGKILL.
stop_nodes() {
    local host
    for host in "$@"; do
        rssh "$host" "${SUDO} pkill -INT -f '${BIN_PATTERN}'; for _ in \$(seq 20); do pgrep -f '${BIN_PATTERN}' >/dev/null || exit 0; sleep 0.5; done; ${SUDO} pkill -KILL -f '${BIN_PATTERN}'; sleep 1" || true
        if [[ "$DPDK_RAN" == 1 ]]; then
            rssh "$host" "${SUDO} rm -rf /var/run/dpdk/rte /var/run/dpdk/melin-client" || true
        fi
    done
}

# ---------------------------------------------------------------------------
# DPDK configuration, per host, from /etc/melin-dpdk.conf
# ---------------------------------------------------------------------------
# Load `host`'s DPDK configuration into <PREFIX>_DPDK_{IP,PREFIX,PORTS,MAC,VLAN,EAL}.
load_dpdk_config() {
    local host="$1" prefix="$2" conf mode vfs ports eal vlan
    if ! rssh "$host" "test -f /etc/melin-dpdk.conf"; then
        echo "  ${host}: no /etc/melin-dpdk.conf, running dpdk-setup.sh"
        rssh "$host" "${SUDO} bash ${REMOTE_DIR}/dpdk-setup.sh"
    fi
    conf=$(rssh "$host" "cat /etc/melin-dpdk.conf")
    mode=$(conf_value "$conf" DPDK_MODE)
    ports=$(conf_value "$conf" DPDK_PORT)
    vlan=$(conf_value "$conf" VLAN_ID)
    if [[ "${mode:-sriov}" == sriov ]]; then
        vfs=$(rssh "$host" "ls -d /sys/bus/pci/drivers/vfio-pci/0000:* 2>/dev/null | wc -l")
        # A reboot (the one after every SR-IOV run) removes the VFs and
        # keeps the conf: set them up again, with the conf's own address
        # and VLAN.
        if (( vfs == 0 )); then
            echo "  ${host}: no VF bound to vfio-pci, rerunning dpdk-setup.sh"
            rssh "$host" "${SUDO} DPDK_IP=$(conf_value "$conf" DPDK_IP)/$(conf_value "$conf" DPDK_PREFIX) bash ${REMOTE_DIR}/dpdk-setup.sh ${vlan:+--vlan ${vlan}}"
            vfs=$(rssh "$host" "ls -d /sys/bus/pci/drivers/vfio-pci/0000:* 2>/dev/null | wc -l")
        fi
        # An SR-IOV host behind a bond has a VF on each member: both are
        # the node's.
        (( vfs >= 2 )) && ports="0,1"
        # The PF tags the VF's frames with its port VLAN; the transport
        # tagging them too would put a second tag on every frame.
        vlan=""
    fi
    eal=$(conf_value "$conf" DPDK_EAL_ARGS)
    [[ -z "$eal" ]] && eal="--huge-dir=$(conf_value "$conf" HUGE_DIR)"
    [[ "$eal" == "--huge-dir=" ]] && eal="--huge-dir=/mnt/huge_2m"
    printf -v "${prefix}_DPDK_MODE" '%s' "${mode:-sriov}"
    printf -v "${prefix}_DPDK_IP" '%s' "$(conf_value "$conf" DPDK_IP)"
    printf -v "${prefix}_DPDK_PREFIX" '%s' "$(conf_value "$conf" DPDK_PREFIX)"
    printf -v "${prefix}_DPDK_PORTS" '%s' "${ports:-0}"
    printf -v "${prefix}_DPDK_MAC" '%s' "$(conf_value "$conf" DPDK_MAC)"
    printf -v "${prefix}_DPDK_VLAN" '%s' "$vlan"
    printf -v "${prefix}_DPDK_EAL" '%s' "$eal"
    local ip_var="${prefix}_DPDK_IP" mac_var="${prefix}_DPDK_MAC"
    if [[ -z "${!ip_var}" || -z "${!mac_var}" ]]; then
        echo "error: ${host}'s /etc/melin-dpdk.conf has no DPDK_IP or DPDK_MAC; rerun scripts/dpdk/dpdk-setup.sh there" >&2
        exit 1
    fi
}

# The `--dpdk-*` flags a process on host <PREFIX> runs with.
dpdk_flags() {
    local prefix="$1" ip prefix_len ports vlan eal
    ip="${prefix}_DPDK_IP"; prefix_len="${prefix}_DPDK_PREFIX"; ports="${prefix}_DPDK_PORTS"
    vlan="${prefix}_DPDK_VLAN"; eal="${prefix}_DPDK_EAL"
    printf "%s" "--dpdk-eal-args='${!eal}' --dpdk-ip ${!ip} --dpdk-prefix-len ${!prefix_len:-24} --dpdk-ports ${!ports}"
    [[ -n "${!vlan}" ]] && printf " --dpdk-vlan %s" "${!vlan}"
    return 0
}

DPDK_LOADED=0
load_dpdk_hosts() {
    [[ "$DPDK_LOADED" == 1 ]] && return 0
    load_dpdk_config "$SERVER" SERVER
    load_dpdk_config "$BENCH" BENCH
    load_dpdk_config "$REPLICA" REPLICA
    load_dpdk_config "$REPLICA2" REPLICA2
    DPDK_LOADED=1
}

# ---------------------------------------------------------------------------
# A cluster: start one for a transport, and know where its client port is
# ---------------------------------------------------------------------------
# Set by start_cluster: where the load generator dials, and with what
# transport flags.
CLIENT_ADDR=""
BENCH_TRANSPORT_ARGS=""

start_cluster() {
    local transport="$1" bin primary_ready repl_ready primary_flags host

    local node
    for node in "${NODES[@]}"; do
        clean_node_state "$(node_host "$node")" "$node" || return 1
    done
    for host in "${HOSTS[@]}"; do pin_irqs "$host"; done

    if [[ "$transport" == dpdk ]]; then
        load_dpdk_hosts
        DPDK_RAN=1
        bin="${SERVER_BIN}.dpdk"
        primary_flags="--bind 0.0.0.0:${CLIENT_PORT} --replication-bind ${SERVER_DPDK_IP}:${REPL_PORT} $(dpdk_flags SERVER)"
        primary_ready="DPDK transport listening"
        repl_ready="DPDK replication sender started"
        CLIENT_ADDR="${SERVER_DPDK_IP}:${CLIENT_PORT}"
        BENCH_TRANSPORT_ARGS="$(dpdk_flags BENCH) --dpdk-peer-mac ${SERVER_DPDK_MAC}"
    else
        bin="$SERVER_BIN"
        primary_flags="--bind ${SERVER_VLAN}:${CLIENT_PORT} --replication-bind ${SERVER_VLAN}:${REPL_PORT}"
        primary_ready="listening addr=${SERVER_VLAN}:${CLIENT_PORT}"
        repl_ready="replication sender listening"
        CLIENT_ADDR="${SERVER_VLAN}:${CLIENT_PORT}"
        BENCH_TRANSPORT_ARGS=""
    fi

    launch "$SERVER" "${bin} ${primary_flags} $(node_store_flags primary) --authorized-keys ${REMOTE_DIR}/authorized_keys --health-bind ${SERVER_VLAN}:${HEALTH_PORT} --ack-policy ${ACK_POLICY} ${APP_SERVER_ARGS} ${SERVER_EXTRA_ARGS}"
    wait_for_log "$SERVER" "$repl_ready" 30 || return 1
    local i prefix replica_flags
    for i in 1 2; do
        host=$(node_host "replica${i}")
        # The host's DPDK configuration, as load_dpdk_hosts named it.
        prefix=REPLICA; (( i == 2 )) && prefix=REPLICA2
        if [[ "$transport" == dpdk ]]; then
            replica_flags="--replica-of ${SERVER_DPDK_IP}:${REPL_PORT} $(dpdk_flags "$prefix") --dpdk-peer-mac ${SERVER_DPDK_MAC}"
        else
            replica_flags="--replica-of ${SERVER_VLAN}:${REPL_PORT}"
        fi
        launch "$host" "${bin} ${replica_flags} --replication-key ${REMOTE_DIR}/repl.key $(node_store_flags "replica${i}") --authorized-keys ${REMOTE_DIR}/authorized_keys ${APP_SERVER_ARGS} ${REPLICA_EXTRA_ARGS}"
    done
    # On a fresh journal the primary serves clients only once a replica is
    # live, so this waits for the replicas too.
    wait_for_log "$SERVER" "$primary_ready" 120 || return 1
    wait_for_replicas 2 || return 1
    if (( SETTLE_SECS > 0 )); then
        echo "  settling ${SETTLE_SECS}s before the load"
        sleep "$SETTLE_SECS"
    fi
}

# ---------------------------------------------------------------------------
# A run: the load generator against the cluster, the primary's health
# sampled throughout, and the verdict
# ---------------------------------------------------------------------------
# The load generator's command line for `workload` on `transport`.
bench_command() {
    local transport="$1" clients="$2" window="$3" duration="$4" bin="$BENCH_BIN"
    [[ "$transport" == dpdk ]] && bin="${BENCH_BIN}.dpdk"
    local args="--addr ${CLIENT_ADDR} --key ${REMOTE_DIR}/bench.key --json ${BENCH_JSON} --clients ${clients} --window ${window} --duration ${duration} --warmup-duration ${WARMUP_DURATION} --cooldown-duration ${COOLDOWN_DURATION}"
    if [[ "$APP" == ec ]]; then
        args+=" --accounts ${EC_ACCOUNTS} --health-addr ${SERVER_VLAN}:${HEALTH_PORT}"
        if [[ "$transport" == dpdk ]]; then
            args+=" --dpdk-core ${BENCH_CORE} ${BENCH_TRANSPORT_ARGS}"
        else
            args+=" --bench-cores ${BENCH_CORE}"
        fi
    else
        args+=" --app ${APP} --core ${BENCH_CORE}"
        [[ "$APP" == echo ]] && args+=" --size ${ECHO_SIZE}"
        [[ "$transport" == dpdk ]] && args+=" --transport dpdk ${BENCH_TRANSPORT_ARGS}"
    fi
    echo "${REMOTE_DIR}/bin/${bin} ${args} ${BENCH_EXTRA_ARGS}"
}

# `node_diskstats <node>`: /proc/diskstats on `node`'s host, read only
# when the disk guard resolved one of its disks.
node_diskstats() {
    [[ -n "${SNAPSHOT_DISKS[$1]:-}${JOURNAL_DISKS[$1]:-}" ]] || return 0
    # Best-effort: an unreadable host gives an empty read, which
    # disk_writes reports per disk rather than this stopping the suite.
    rssh "$(node_host "$1")" "cat /proc/diskstats" || true
}

# Set by run_workload for judge_run: whether the load ran to the end, and
# each node's disk counters on either side of it.
RUN_LOADED=0
declare -A DISKSTATS_BEFORE=() DISKSTATS_AFTER=()

run_workload() {
    local transport="$1" workload="$2" clients window duration name node
    duration=$(workload_duration_var "$workload")
    duration="${!duration}"
    case "$workload" in
        throughput) clients="$THROUGHPUT_CLIENTS"; window="$THROUGHPUT_WINDOW" ;;
        single) clients=1; window=1 ;;
    esac
    name="${transport}-${workload}"
    RUN_LOADED=0
    echo ""
    echo "--- ${name}: ${clients} × ${window} for ${duration} ---"

    # The primary's health, sampled every second for the run's length;
    # its journal sequence bounds the snapshots the run is judged on.
    local samples="${RESULTS_DIR}/${name}.health"
    sample_metrics "$samples" '^melin_(replicas_connected|replica_catching_up|replica_evictions_total|journal_sequence)'

    if (( SNAPSHOT_INTERVAL_MS > 0 )); then
        for node in "${NODES[@]}"; do DISKSTATS_BEFORE[$node]=$(node_diskstats "$node"); done
    fi
    local rc=0
    rssh "$BENCH" "${SUDO} rm -f ${BENCH_JSON}"
    rssh "$BENCH" "${SUDO} $(bench_command "$transport" "$clients" "$window" "$duration")" \
        | tee "${RESULTS_DIR}/${name}.txt" || rc=$?
    if (( SNAPSHOT_INTERVAL_MS > 0 )); then
        for node in "${NODES[@]}"; do DISKSTATS_AFTER[$node]=$(node_diskstats "$node"); done
    fi
    stop_samplers

    if (( rc != 0 )) || ! rscp "${BENCH}:${BENCH_JSON}" "${RESULTS_DIR}/${name}.json" 2>/dev/null; then
        echo "  FAILED: the load generator exited ${rc}"
        FAILED_RUNS+=("${name}: the load generator failed")
        return
    fi
    RUN_LOADED=1
    local verdict
    if verdict=$(judge_health "$samples" 2); then
        echo "  cluster ${verdict}"
    else
        echo "  cluster ${verdict}"
        FAILED_RUNS+=("${name}: ${verdict}")
    fi
}

# Every node's log for run `name`, before the next run's launch truncates
# it: a replica's is the only record of why it fell behind. Best-effort:
# a host that cannot be reached loses its log, not the suite.
save_node_logs() {
    local name="$1" node
    for node in "${NODES[@]}"; do
        rssh "$(node_host "$node")" "cat ${NODE_LOG}" > "${RESULTS_DIR}/${name}.${node}.log" 2>/dev/null || true
    done
}

# The snapshot verdict on run `transport`-`workload`, from the logs
# save_node_logs took: each node must have saved enough snapshots to its
# own path during the load (judge_snapshots), the window being the
# journal sequences the primary's health samples bound (seq_window).
# Writes `<run>.snapshots`, each node's verdict then the disks' writes;
# a verdict that is not `ok` fails the run.
judge_run() {
    local name="$1-$2" workload="$2" out window first last node verdict
    out="${RESULTS_DIR}/${name}.snapshots"
    if ! window=$(seq_window "${RESULTS_DIR}/${name}.health"); then
        echo "  snapshots ${window}"
        echo "$window" > "$out"
        FAILED_RUNS+=("${name}: snapshots ${window}")
    else
        read -r first last <<< "$window"
        : > "$out"
        for node in "${NODES[@]}"; do
            if ! verdict=$(judge_snapshots "${RESULTS_DIR}/${name}.${node}.log" "$first" "$last" \
                "${MEASURED_MS[$workload]}" "$SNAPSHOT_INTERVAL_MS" "$(node_snapshot "$node")"); then
                FAILED_RUNS+=("${name}: ${node} snapshots ${verdict}")
            fi
            echo "  ${node} snapshots ${verdict}"
            echo "${node}: ${verdict}" >> "$out"
        done
    fi
    for node in "${NODES[@]}"; do
        disk_writes "$node" "${SNAPSHOT_DISKS[$node]:-}" "${JOURNAL_DISKS[$node]:-}" \
            "${DISKSTATS_BEFORE[$node]:-}" "${DISKSTATS_AFTER[$node]:-}" >> "$out"
    done
}

# ---------------------------------------------------------------------------
# The runs
# ---------------------------------------------------------------------------
# Once, before the first run. With snapshots off there is no separation
# to show, and a host with no journal drive of its own can still run the
# suite.
if (( SNAPSHOT_INTERVAL_MS > 0 )); then check_snapshot_disks; fi

for transport in "${RUN_TRANSPORTS[@]}"; do
    for workload in "${REQ_WORKLOADS[@]}"; do
        # A fresh cluster per run: each run starts from an empty journal.
        if ! start_cluster "$transport"; then
            FAILED_RUNS+=("${transport}-${workload}: the cluster did not start")
            stop_nodes "${HOSTS[@]}"
            save_node_logs "${transport}-${workload}"
            continue
        fi
        run_workload "$transport" "$workload"
        stop_nodes "${HOSTS[@]}"
        save_node_logs "${transport}-${workload}"
        # Not on a run whose load generator failed: it has already failed,
        # with no measured phase to expect saves from.
        if (( SNAPSHOT_INTERVAL_MS > 0 && RUN_LOADED )); then
            judge_run "$transport" "$workload"
        fi
    done
done

# SR-IOV leaves the VFs bound to vfio-pci and the hugepages taken; a
# kernel transport after it needs the hosts rebooted. mlx5 shares its
# port with the kernel and leaves nothing behind.
if [[ "$DPDK_RAN" == 1 && "${SERVER_DPDK_MODE:-}" == sriov && "${SKIP_REBOOT:-0}" != 1 ]]; then
    echo ""
    echo "=== Rebooting the hosts (SR-IOV DPDK ran; SKIP_REBOOT=1 to skip) ==="
    for host in "${HOSTS[@]}"; do
        rssh "$host" "${SUDO} nohup bash -c 'sleep 1 && reboot' >/dev/null 2>&1 &" || true
    done
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "=== Results (${RESULTS_DIR}) ==="
summary_table "$RESULTS_DIR"

if [[ ${#FAILED_RUNS[@]} -gt 0 ]]; then
    echo ""
    echo "Runs that did not pass:"
    for run in "${FAILED_RUNS[@]}"; do echo "  ${run}"; done
    exit 1
fi
