#!/usr/bin/env bash
# Start Docker containers for testing the LAN bench suite and replication locally.
#
# Creates a "bench-net" network and two to four privileged Ubuntu
# containers with SSH access via your default SSH key.
#
# Usage:
#   ./scripts/test-containers-start.sh                              # server + client
#   ./scripts/test-containers-start.sh --replica                    # server + client + 1 replica
#   ./scripts/test-containers-start.sh --dual-replica               # server + client + 2 replicas
#   ./scripts/test-containers-start.sh --dual-replica --branch foo  # checkout branch "foo" in all containers
#   ./scripts/test-containers-start.sh --memif                      # additionally wire DPDK over memif (see below)
#
# DPDK transport modes:
#   Default: TAP mode (DPDK_MODE=tap). The DPDK server reads packets via a
#   kernel TAP fd — functional but slow (per-packet kernel crossing).
#   --memif: shared-memory mode (DPDK_MODE=memif). Both the server AND the
#   bench run DPDK and exchange packets over a memif shared-memory link via
#   a unix socket on a shared volume — near-native throughput, the closest
#   cheap proxy for real DPDK performance on a single host. Requires the
#   extra DPDK bench build, hence the flag. The kernel-TCP path is
#   unaffected either way, so TCP benchmarks keep working.
#
# After starting:
#   SERVER_IP=$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' bench-server)
#   BENCH_IP=$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' bench-client)
#   ./scripts/lan-bench-suite.sh "$SERVER_IP" "$BENCH_IP" "$SERVER_IP" root
#
#   # With replica:
#   REPLICA_IP=$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' bench-replica)
#   ./scripts/lan-bench-suite.sh "$SERVER_IP" "$BENCH_IP" "$SERVER_IP" root "$REPLICA_IP" "$REPLICA_IP"
#
#   # With dual replicas:
#   REPLICA2_IP=$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' bench-replica2)
#   ./scripts/lan-bench-suite.sh "$SERVER_IP" "$BENCH_IP" "$SERVER_IP" root "$REPLICA_IP" "$REPLICA_IP" "$REPLICA2_IP" "$REPLICA2_IP"

set -euo pipefail

NETWORK="bench-net"
SERVER="bench-server"
CLIENT="bench-client"
REPLICA="bench-replica"
REPLICA2="bench-replica2"
IMAGE="ubuntu:24.04"
REPO_DIR="/root/workspace/melin"

# Parse flags.
WITH_REPLICA=false
WITH_DUAL_REPLICA=false
WITH_MEMIF=false
BRANCH=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --replica) WITH_REPLICA=true; shift ;;
        --dual-replica) WITH_REPLICA=true; WITH_DUAL_REPLICA=true; shift ;;
        --memif) WITH_MEMIF=true; shift ;;
        --branch) BRANCH="$2"; shift 2 ;;
        *) echo "unknown flag: $1" >&2; exit 1 ;;
    esac
done

# memif rendezvous: a Docker volume shared between the server and client
# containers holds the memif control socket. The data rings are memfd-backed
# and handed over the socket via SCM_RIGHTS, so only the socket needs to live
# on a shared mount. Mounted at /memif in both endpoints.
MEMIF_VOLUME="bench-memif"
MEMIF_SOCKET="/memif/memif.sock"

# SSH key for logging into the containers.
SSH_PUB=""
for candidate in ~/.ssh/id_ed25519.pub ~/.ssh/id_rsa.pub ~/.ssh/id_ecdsa.pub; do
    if [[ -f "$candidate" ]]; then
        SSH_PUB="$candidate"
        break
    fi
done
if [[ -z "$SSH_PUB" ]]; then
    echo "error: no SSH public key found in ~/.ssh/" >&2
    exit 1
fi
echo "Using SSH key: $SSH_PUB"


# Create network (ignore if exists).
docker network create "$NETWORK" 2>/dev/null || true

# Recreate the memif socket volume fresh so a stale socket from a previous
# run can't confuse the handshake.
if [[ "$WITH_MEMIF" == "true" ]]; then
    docker volume rm "$MEMIF_VOLUME" 2>/dev/null || true
    docker volume create "$MEMIF_VOLUME" >/dev/null
fi

# Build container list.
CONTAINERS=("$SERVER" "$CLIENT")
if [[ "$WITH_REPLICA" == "true" ]]; then
    CONTAINERS+=("$REPLICA")
fi
if [[ "$WITH_DUAL_REPLICA" == "true" ]]; then
    CONTAINERS+=("$REPLICA2")
fi

# Start containers.
for name in "${CONTAINERS[@]}"; do
    # Remove old container if it exists.
    docker rm -f "$name" 2>/dev/null || true

    # Mount the shared memif socket volume only on the two memif endpoints
    # (server + client); replicas use TCP replication and don't need it.
    MOUNT_ARGS=()
    if [[ "$WITH_MEMIF" == "true" && ( "$name" == "$SERVER" || "$name" == "$CLIENT" ) ]]; then
        MOUNT_ARGS=(-v "${MEMIF_VOLUME}:/memif")
    fi

    # --init runs tini as PID 1 so it reaps orphaned children. Without it
    # the entrypoint (`sleep infinity`) is PID 1, never calls wait(), and
    # every benchmark's short-lived melin-server processes pile up as
    # unreaped zombies after their launching ssh/nohup shell exits.
    docker run -d \
        --name "$name" \
        --network "$NETWORK" \
        --privileged \
        --init \
        "${MOUNT_ARGS[@]+"${MOUNT_ARGS[@]}"}" \
        "$IMAGE" \
        sleep infinity

    # Install SSH server, Rust build deps, and nc (for connectivity check).
    docker exec "$name" bash -c "
        apt-get update -qq && \
        apt-get install -y --no-install-recommends \
            openssh-server build-essential pkg-config git curl ca-certificates netcat-openbsd sudo && \
        mkdir -p /run/sshd /root/.ssh && \
        chmod 700 /root/.ssh && \
        echo '$(cat "$SSH_PUB")' >> /root/.ssh/authorized_keys && \
        chmod 600 /root/.ssh/authorized_keys && \
        sed -i 's/#PermitRootLogin.*/PermitRootLogin yes/' /etc/ssh/sshd_config && \
        /usr/sbin/sshd
    "

    # Install Rust.
    echo "  Installing Rust in $name..."
    docker exec "$name" bash -c "
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
    " > /dev/null 2>&1

    # Clone the repo only — binaries are built by lan-bench-suite.sh on the
    # first run (single builder, so nothing is pre-baked and later goes
    # stale). The first suite run pays the full build; subsequent runs are
    # incremental.
    echo "  Cloning repo in $name..."
    CHECKOUT_CMD=""
    if [[ -n "$BRANCH" ]]; then
        CHECKOUT_CMD="git fetch origin $BRANCH && git checkout $BRANCH &&"
    fi
    docker exec "$name" bash -c "
        mkdir -p /root/workspace && \
        git clone https://github.com/melin-engine/melin.git $REPO_DIR && \
        cd $REPO_DIR && \
        $CHECKOUT_CMD true
    " 2>&1 | tail -3

    IP=$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$name")
    echo "  $name: $IP (ready)"
done

# ---------------------------------------------------------------------------
# Install DPDK build deps (the suite builds the DPDK binaries itself).
# ---------------------------------------------------------------------------
# The server container gets libdpdk-dev + libclang-dev so lan-bench-suite.sh
# can build the DPDK server on first run (bindgen needs libclang), plus
# iproute2 for TAP device routing. The suite detects the mode via
# DPDK_MODE=... in /etc/melin-dpdk.conf.
echo ""
echo "Installing DPDK build deps on server..."
docker exec "$SERVER" bash -c "
    apt-get install -y --no-install-recommends libdpdk-dev libclang-dev iproute2 2>&1 | tail -3
"

if [[ "$WITH_MEMIF" == "true" ]]; then
    # memif mode: both ends run DPDK over a shared-memory link, so the bench
    # (client) also needs the DPDK build toolchain — the suite builds the
    # DPDK bench there on first run. Then write a memif config the suite
    # consumes.
    echo "Installing DPDK build deps on client (the memif bench builds there)..."
    docker exec "$CLIENT" bash -c "
        apt-get install -y --no-install-recommends libdpdk-dev libclang-dev 2>&1 | tail -3
    "

    # Hugepages — memif throughput roughly doubled vs --no-huge (TLB
    # pressure). nr_hugepages is host-global (containers share the kernel),
    # so allocating it from a privileged container reserves host memory.
    # NOT released on container stop; reclaim with:
    #   docker exec ${SERVER} sh -c 'echo 0 > /proc/sys/vm/nr_hugepages'
    HUGEPAGES="${HUGEPAGES:-1024}"   # 2 MiB each => ~2 GiB
    echo "Allocating ${HUGEPAGES} hugepages (host-global) + mounting hugetlbfs..."
    docker exec "$SERVER" bash -c "echo ${HUGEPAGES} > /proc/sys/vm/nr_hugepages"
    for name in "$SERVER" "$CLIENT"; do
        docker exec "$name" bash -c "mkdir -p /dev/hugepages; mountpoint -q /dev/hugepages || mount -t hugetlbfs nodev /dev/hugepages"
    done
    echo "  HugePages_Total=$(docker exec "$SERVER" sh -c 'grep HugePages_Total /proc/meminfo | tr -s " " | cut -d" " -f2')"

    # memif is a direct point-to-point L2 link between the two DPDK stacks,
    # so both smoltcp endpoints sit on a private /24 with no kernel routing
    # or gateway (unlike TAP). Server = memif master (creates the socket),
    # bench = client. The config carries the complete EAL for BOTH ends so
    # the suite stays a transport-agnostic consumer (no role/MAC munging).
    # Cross-container, so each end has its own /var/run/dpdk — no file-prefix
    # needed. Each port's MAC is the deterministic 02:00:<ip> value the bench
    # seeds for its peer (see bench/src/dpdk.rs); without it smoltcp drops
    # every frame. Uses hugepages (allocated above).
    # TODO(mem): -m 256 is a guess for the memif rings + mbuf pool; tune.
    docker exec "$SERVER" bash -c "
        cat > /etc/melin-dpdk.conf << EOF
DPDK_MODE=memif
DPDK_IP=10.0.0.1
DPDK_PREFIX=24
DPDK_PORT=0
MEMIF_SOCKET=${MEMIF_SOCKET}
DPDK_EAL_ARGS=--no-pci -m 256 --vdev net_memif0,role=server,socket-abstract=no,socket=${MEMIF_SOCKET},id=0,mac=02:00:0a:00:00:01
MEMIF_CLIENT_IP=10.0.0.2
MEMIF_CLIENT_EAL=--no-pci -m 256 --vdev net_memif0,role=client,socket-abstract=no,socket=${MEMIF_SOCKET},id=0,mac=02:00:0a:00:00:02
EOF
"
    echo "  DPDK config written (memif mode, server IP=10.0.0.1, socket=${MEMIF_SOCKET})"
    # TODO(repl): memif here only covers the server<->bench trading link;
    #   replication still uses kernel TCP. memif for replica links is future.
else
    # Pick a DPDK IP that's on the Docker bridge subnet but not used by any
    # container. The bench suite routes traffic to this IP via the server.
    DPDK_TAP_IP="172.17.0.100"

    docker exec "$SERVER" bash -c "
        cat > /etc/melin-dpdk.conf << EOF
DPDK_MODE=tap
DPDK_IP=${DPDK_TAP_IP}
DPDK_PREFIX=16
DPDK_PORT=0
DPDK_EAL_ARGS=--no-huge --no-pci --vdev net_tap0 -m 256
EOF
"
    echo "  DPDK config written (TAP mode, IP=${DPDK_TAP_IP})"
fi

# Install iproute2 on all other containers (bench needs it for routing).
IPROUTE_HOSTS=("$CLIENT")
if [[ "$WITH_REPLICA" == "true" ]]; then IPROUTE_HOSTS+=("$REPLICA"); fi
if [[ "$WITH_DUAL_REPLICA" == "true" ]]; then IPROUTE_HOSTS+=("$REPLICA2"); fi
for name in "${IPROUTE_HOSTS[@]}"; do
    docker exec "$name" bash -c "apt-get install -y --no-install-recommends iproute2 2>&1 | tail -1"
done

# Create journal directory on all containers.
for name in "${CONTAINERS[@]}"; do
    docker exec "$name" mkdir -p /tmp/journal
done

SERVER_IP=$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$SERVER")
CLIENT_IP=$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$CLIENT")

# ---------------------------------------------------------------------------
# Generate keys and distribute to replicas.
# ---------------------------------------------------------------------------
if [[ "$WITH_REPLICA" == "true" ]]; then
    echo ""
    echo "Generating authentication keys..."
    REPLICA_IP=$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$REPLICA")

    # Generate trader key (for client connections) and replication key.
    docker exec "$SERVER" bash -c "
        cd /tmp && \
        rm -f trader.key trader.pub repl.key repl.pub authorized_keys 2>/dev/null; \
        $REPO_DIR/target/release/melin-keygen trader trader > /dev/null 2>&1 && \
        $REPO_DIR/target/release/melin-keygen repl replication > /dev/null 2>&1 && \
        echo \"trader \$(cat trader.pub | tr -d '\\n') trader\" > authorized_keys && \
        echo \"replication \$(cat repl.pub | tr -d '\\n') repl\" >> authorized_keys
    "

    # Copy replication key to replica(s).
    docker cp "$SERVER":/tmp/repl.key /tmp/repl.key
    docker cp /tmp/repl.key "$REPLICA":/tmp/repl.key
    echo "  Distributed replication key to $REPLICA"

    if [[ "$WITH_DUAL_REPLICA" == "true" ]]; then
        REPLICA2_IP=$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$REPLICA2")
        docker cp /tmp/repl.key "$REPLICA2":/tmp/repl.key
        echo "  Distributed replication key to $REPLICA2"
    fi
    rm -f /tmp/repl.key

    echo "  authorized_keys:"
    docker exec "$SERVER" cat /tmp/authorized_keys | sed 's/^/    /'
fi

# ---------------------------------------------------------------------------
# Print ready-to-use commands.
# ---------------------------------------------------------------------------
echo ""
echo "Containers ready. Run the benchmark with:"
echo "  ./scripts/lan-bench-suite.sh $SERVER_IP $CLIENT_IP $SERVER_IP root"

if [[ "$WITH_MEMIF" == "true" ]]; then
    echo ""
    echo "DPDK memif configured (DPDK_MODE=memif, socket=${MEMIF_SOCKET})."
    echo "  Drive it with TRANSPORTS=dpdk (the wrapper auto-detects memif):"
    echo "    TRANSPORTS=dpdk WORKLOADS=throughput THROUGHPUT_DURATION=15s \\"
    echo "      ./scripts/docker-bench-suite.sh"
fi

if [[ "$WITH_REPLICA" == "true" ]]; then
    echo ""
    echo "Replication benchmark:"
    echo "  RUN_FSYNC=0 RUN_NOPERSIST=0 RUN_SINGLE=0 RUN_SWEEPS=0 RUN_PLOTS=0 \\"
    echo "    ./scripts/lan-bench-suite.sh $SERVER_IP $CLIENT_IP $SERVER_IP root $REPLICA_IP $REPLICA_IP"

    echo ""
    echo "Manual smoke test (copy-paste):"
    echo ""
    echo "  # Start primary"
    echo "  docker exec -d $SERVER bash -c 'RUST_LOG=info $REPO_DIR/target/release/melin-server \\"
    echo "      --journal /tmp/bench.journal --replication-bind 0.0.0.0:9877 --health-bind 0.0.0.0:9878 \\"
    echo "      --bind 0.0.0.0:9876 --authorized-keys /tmp/authorized_keys --accounts 100 --instruments 5 \\"
    echo "      >/tmp/server.log 2>&1'"
    echo ""
    echo "  # Start replica"
    echo "  docker exec -d $REPLICA bash -c 'RUST_LOG=info $REPO_DIR/target/release/melin-server \\"
    echo "      --replica-of $SERVER_IP:9877 --replication-key /tmp/repl.key --journal /tmp/replica.journal \\"
    echo "      >/tmp/replica.log 2>&1'"

    if [[ "$WITH_DUAL_REPLICA" == "true" ]]; then
        echo ""
        echo "  # Start replica2"
        echo "  docker exec -d $REPLICA2 bash -c 'RUST_LOG=info $REPO_DIR/target/release/melin-server \\"
        echo "      --replica-of $SERVER_IP:9877 --replication-key /tmp/repl.key --journal /tmp/replica2.journal \\"
        echo "      >/tmp/replica2.log 2>&1'"

        echo ""
        echo "Dual replication benchmark:"
        echo "  RUN_FSYNC=0 RUN_NOPERSIST=0 RUN_SINGLE=0 RUN_SWEEPS=0 RUN_REPLICATION=0 RUN_PLOTS=0 \\"
        echo "    ./scripts/lan-bench-suite.sh $SERVER_IP $CLIENT_IP $SERVER_IP root $REPLICA_IP $REPLICA_IP $REPLICA2_IP $REPLICA2_IP"
    fi

    echo ""
    echo "  # Check health"
    echo "  docker exec $SERVER bash -c 'echo | nc -w1 127.0.0.1 9878'"
    echo ""
    echo "  # Check logs"
    echo "  docker exec $SERVER tail -5 /tmp/server.log"
    echo "  docker exec $REPLICA tail -5 /tmp/replica.log"
    if [[ "$WITH_DUAL_REPLICA" == "true" ]]; then
        echo "  docker exec $REPLICA2 tail -5 /tmp/replica2.log"
    fi
fi
