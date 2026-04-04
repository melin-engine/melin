#!/usr/bin/env bash
# Start Docker containers for testing lan-bench.sh and replication locally.
#
# Creates a "bench-net" network and two to four privileged Ubuntu
# containers with SSH access via your default SSH key.
#
# Usage:
#   ./scripts/test-containers-start.sh                              # server + client
#   ./scripts/test-containers-start.sh --replica                    # server + client + 1 replica
#   ./scripts/test-containers-start.sh --dual-replica               # server + client + 2 replicas
#   ./scripts/test-containers-start.sh --dual-replica --branch foo  # checkout branch "foo" in all containers
#
# After starting:
#   SERVER_IP=$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' bench-server)
#   BENCH_IP=$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' bench-client)
#   ./scripts/lan-bench.sh "$SERVER_IP" "$BENCH_IP" "$SERVER_IP" root
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
BRANCH=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --replica) WITH_REPLICA=true; shift ;;
        --dual-replica) WITH_REPLICA=true; WITH_DUAL_REPLICA=true; shift ;;
        --branch) BRANCH="$2"; shift 2 ;;
        *) echo "unknown flag: $1" >&2; exit 1 ;;
    esac
done

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

# GitHub deploy key for cloning the repo inside containers.
GITHUB_KEY="${GITHUB_DEPLOY_KEY:-}"
if [[ -z "$GITHUB_KEY" ]]; then
    for candidate in ~/.ssh/te-test2 ~/.ssh/te_test_ed; do
        if [[ -f "$candidate" ]]; then
            GITHUB_KEY="$candidate"
            break
        fi
    done
fi
if [[ -z "$GITHUB_KEY" || ! -f "$GITHUB_KEY" ]]; then
    echo "error: GitHub deploy key not found. Set GITHUB_DEPLOY_KEY=<path>" >&2
    exit 1
fi
echo "Using GitHub deploy key: $GITHUB_KEY"

# Create network (ignore if exists).
docker network create "$NETWORK" 2>/dev/null || true

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

    docker run -d \
        --name "$name" \
        --network "$NETWORK" \
        --privileged \
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

    # Copy GitHub deploy key so the container can clone the repo.
    docker cp "$GITHUB_KEY" "$name":/root/.ssh/deploy_key
    docker exec "$name" bash -c "
        chmod 600 /root/.ssh/deploy_key && \
        cat >> /root/.ssh/config << 'EOF'
Host github.com
    IdentityFile /root/.ssh/deploy_key
    StrictHostKeyChecking no
EOF
        chmod 600 /root/.ssh/config
    "

    # Install Rust.
    echo "  Installing Rust in $name..."
    docker exec "$name" bash -c "
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable && \
        mkdir -p /root/.cargo && \
        echo -e '[net]\ngit-fetch-with-cli = true' >> /root/.cargo/config.toml
    " > /dev/null 2>&1

    # Clone the repo and build.
    echo "  Cloning repo and building in $name (this takes a few minutes)..."
    CHECKOUT_CMD=""
    if [[ -n "$BRANCH" ]]; then
        CHECKOUT_CMD="git fetch origin $BRANCH && git checkout $BRANCH &&"
    fi
    docker exec "$name" bash -c "
        source /root/.cargo/env && \
        mkdir -p /root/workspace && \
        git clone git@github.com:pierre-l/melin.git $REPO_DIR && \
        cd $REPO_DIR && \
        $CHECKOUT_CMD
        cargo build --release
    " 2>&1 | tail -3

    IP=$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$name")
    echo "  $name: $IP (ready)"
done

# ---------------------------------------------------------------------------
# Install DPDK build deps and build the DPDK server binary.
# ---------------------------------------------------------------------------
# The server container gets libdpdk-dev + libclang-dev for the DPDK build,
# plus iproute2 for TAP device routing. The bench suite detects TAP mode
# via DPDK_MODE=tap in /etc/melin-dpdk.conf and skips SR-IOV setup.
echo ""
echo "Setting up DPDK (TAP mode) on server..."

docker exec "$SERVER" bash -c "
    apt-get install -y --no-install-recommends libdpdk-dev libclang-dev iproute2 2>&1 | tail -3 && \
    source /root/.cargo/env && \
    cd $REPO_DIR && \
    cargo build --release -p melin-server --features dpdk 2>&1 | tail -3 && \
    cp target/release/melin-server target/release/melin-server.dpdk && \
    echo 'Rebuilding default (non-DPDK) server binary...' && \
    cargo build --release -p melin-server 2>&1 | tail -3 && \
    ls -la target/release/melin-server target/release/melin-server.dpdk
"

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
echo "  ./scripts/lan-bench.sh $SERVER_IP $CLIENT_IP $SERVER_IP"

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
