#!/usr/bin/env bash
# Run the LAN suite against four podman containers on this machine: a
# rehearsal of the whole suite (sysroot, cross build, deploy, cluster,
# health and snapshot verdicts, summary) without bench hosts. The figures
# it prints measure nothing; whether it gets to print them is the test.
#
# Snapshots are on, every 2 s so that each 5 s phase is judged on at
# least one save per node, in the suite's default SNAPSHOT_DIR, which the
# suite creates and cleans in the containers. Their filesystem is an
# overlay, which the suite's disk guard cannot resolve to a disk, so the
# rig sets SNAPSHOT_SHARED_DISK_OK=1: the guard warns instead of refusing,
# and the runs record their disks' writes as `-`.
#
# The containers run Debian, as the bench hosts do, on this machine's
# architecture (so the build is for that architecture, not x86_64), on
# one podman network that stands in for the private VLAN. They are
# reached over SSH on ports published on localhost, through an ssh_config
# written for the run (the suite's SSH_CONFIG). Kernel TCP only: DPDK
# needs a NIC.
#
# Safe for a laptop: the containers are unprivileged (the suite's IRQ
# pinning fails harmlessly inside them), though without a seccomp filter
# (podman's refuses io_uring), each is capped at CPUS cores, and
# the nodes run unpinned and yielding (`--cores none`) rather than
# spinning on the default layout's cores.
#
# Usage:
#   ./scripts/bench/test-containers.sh [echo|notary|ec]    # default echo
#
# Environment:
#   WORKLOADS   passed to the suite (default throughput,single)
#   CPUS        cores each container may use (default 2)
#   PLATFORM    a podman platform for the containers (e.g. linux/amd64,
#               which needs qemu-user-static, to rehearse the x86_64
#               build under emulation; slow, and not every kernel runs it)
#   KEEP=1      leave the containers running afterwards
# Any other suite setting (THROUGHPUT_CLIENTS, ...) passes through.

set -euo pipefail

APP="${1:-echo}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CPUS="${CPUS:-2}"
NETWORK=melin-bench-net
IMAGE=localhost/melin-bench-host
NAMES=(server client replica replica2)
PLATFORM_ARGS=()
[[ -n "${PLATFORM:-}" ]] && PLATFORM_ARGS=(--platform "$PLATFORM")

if ! command -v podman >/dev/null; then
    echo "error: podman not found" >&2
    exit 1
fi

WORK_DIR="$(mktemp -d -t melin-bench-containers.XXXXXX)"
cleanup() {
    if [[ "${KEEP:-0}" == 1 ]]; then
        echo "containers left running (KEEP=1); ssh config: ${WORK_DIR}/ssh_config"
        return
    fi
    local name
    for name in "${NAMES[@]}"; do
        podman rm -f "melin-bench-${name}" >/dev/null 2>&1 || true
    done
    podman network rm "$NETWORK" >/dev/null 2>&1 || true
    rm -rf "$WORK_DIR"
}
trap cleanup EXIT

# A key of the rig's own, so nothing of the operator's is installed in
# the containers.
ssh-keygen -q -t ed25519 -N '' -f "${WORK_DIR}/id_ed25519"

echo "=== Building the host image ==="
# What server-setup.sh installs that the suite uses, without the host
# tuning, which a container cannot do: build-essential for the sysroot,
# curl for the health samples, procps for pkill. The disk guard's findmnt
# and lsblk are util-linux's, in the base image.
podman build "${PLATFORM_ARGS[@]}" -q -t "$IMAGE" -f - "$WORK_DIR" <<'EOF' >/dev/null
FROM docker.io/library/debian:bookworm
RUN apt-get update -qq \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends \
      openssh-server build-essential curl procps ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && mkdir -p /run/sshd /root/.ssh \
 && chmod 700 /root/.ssh
CMD ["/usr/sbin/sshd", "-D", "-e"]
EOF

echo "=== Starting the hosts ==="
podman network exists "$NETWORK" || podman network create "$NETWORK" >/dev/null
SSH_CONFIG="${WORK_DIR}/ssh_config"
: > "$SSH_CONFIG"
for name in "${NAMES[@]}"; do
    container="melin-bench-${name}"
    podman rm -f "$container" >/dev/null 2>&1 || true
    # Seccomp unconfined: podman's default profile refuses io_uring, which
    # the kernel transport is built on (the node exits with ENOSYS). The
    # container stays unprivileged.
    podman run "${PLATFORM_ARGS[@]}" -d --name "$container" --hostname "$container" \
        --network "$NETWORK" -p 127.0.0.1::22 --cpus "$CPUS" \
        --security-opt seccomp=unconfined "$IMAGE" >/dev/null
    podman exec -i "$container" sh -c 'cat > /root/.ssh/authorized_keys && chmod 600 /root/.ssh/authorized_keys' \
        < "${WORK_DIR}/id_ed25519.pub"
    port=$(podman port "$container" 22 | head -1)
    port="${port##*:}"
    cat >> "$SSH_CONFIG" <<EOF
Host ${container}
    HostName 127.0.0.1
    Port ${port}
    User root
    IdentityFile ${WORK_DIR}/id_ed25519
    IdentitiesOnly yes
EOF
    echo "  ${container}: ssh on 127.0.0.1:${port}"
done
SERVER_IP=$(podman inspect -f "{{(index .NetworkSettings.Networks \"${NETWORK}\").IPAddress}}" melin-bench-server)
echo "  server on the network at ${SERVER_IP}"

for name in "${NAMES[@]}"; do
    for _ in $(seq 30); do
        ssh -F "$SSH_CONFIG" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
            -o LogLevel=ERROR -o BatchMode=yes "melin-bench-${name}" true 2>/dev/null && continue 2
        sleep 1
    done
    echo "error: melin-bench-${name} does not answer on SSH" >&2
    exit 1
done

echo ""
# Short phases: the rehearsal is of the suite, not of the figures. The
# nodes unpinned and yielding, without locking their memory (a container's
# memlock limit is small), on small journal segments. As many load
# threads as a container has whole cores (CPUS may be a fraction), and at
# least one: each spins on one.
NODE_ARGS="--cores none --no-mlock --max-journal-mib 64"
SSH_CONFIG="$SSH_CONFIG" \
TRANSPORTS=kernel \
WORKLOADS="${WORKLOADS:-throughput,single}" \
THROUGHPUT_DURATION="${THROUGHPUT_DURATION:-5s}" \
SINGLE_DURATION="${SINGLE_DURATION:-5s}" \
WARMUP_DURATION="${WARMUP_DURATION:-1s}" \
COOLDOWN_DURATION="${COOLDOWN_DURATION:-1s}" \
BENCH_CORE="${BENCH_CORE:-0}" \
BENCH_THREADS="${BENCH_THREADS:-$(( ${CPUS%.*} > 1 ? ${CPUS%.*} : 1 ))}" \
SNAPSHOT_INTERVAL_MS="${SNAPSHOT_INTERVAL_MS:-2000}" \
SNAPSHOT_SHARED_DISK_OK=1 \
SERVER_EXTRA_ARGS="${SERVER_EXTRA_ARGS:-$NODE_ARGS}" \
REPLICA_EXTRA_ARGS="${REPLICA_EXTRA_ARGS:-$NODE_ARGS}" \
    "${SCRIPT_DIR}/lan-bench-suite.sh" "$APP" \
    melin-bench-server melin-bench-client "$SERVER_IP" melin-bench-replica melin-bench-replica2
