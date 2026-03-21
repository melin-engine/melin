#!/usr/bin/env bash
# Start Docker containers for testing lan-bench.sh and replication locally.
#
# Creates a "bench-net" network and two (or three) privileged Ubuntu
# containers with SSH access via your default SSH key.
#
# Usage:
#   ./scripts/test-containers-start.sh            # server + client
#   ./scripts/test-containers-start.sh --replica   # server + client + replica
#
# After starting:
#   SERVER_IP=$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' bench-server)
#   BENCH_IP=$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' bench-client)
#   ./scripts/lan-bench.sh "$SERVER_IP" "$BENCH_IP" "$SERVER_IP" root
#
#   # With replica:
#   REPLICA_IP=$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' bench-replica)
#   ./scripts/lan-bench-suite.sh "$SERVER_IP" "$BENCH_IP" "$SERVER_IP" root "$REPLICA_IP" "$REPLICA_IP"

set -euo pipefail

NETWORK="bench-net"
SERVER="bench-server"
CLIENT="bench-client"
REPLICA="bench-replica"
IMAGE="ubuntu:24.04"

# Parse flags.
WITH_REPLICA=false
for arg in "$@"; do
    case "$arg" in
        --replica) WITH_REPLICA=true ;;
        *) echo "unknown flag: $arg" >&2; exit 1 ;;
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
GITHUB_KEY="/home/pierre/.ssh/te-test2"
if [[ ! -f "$GITHUB_KEY" ]]; then
    echo "error: GitHub deploy key not found: $GITHUB_KEY" >&2
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
    docker cp "$GITHUB_KEY" "$name":/root/.ssh/te-test2
    docker exec "$name" bash -c "
        chmod 600 /root/.ssh/te-test2 && \
        cat >> /root/.ssh/config << 'EOF'
Host github.com
    IdentityFile /root/.ssh/te-test2
    StrictHostKeyChecking no
EOF
        chmod 600 /root/.ssh/config
    "

    # Install Rust.
    echo "  Installing Rust in $name..."
    docker exec "$name" bash -c "
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
    " > /dev/null 2>&1

    # Clone the repo and build.
    echo "  Cloning repo and building in $name (this takes a few minutes)..."
    docker exec "$name" bash -c "
        source /root/.cargo/env && \
        mkdir -p /root/workspace && \
        git clone git@github.com:pierre-l/trading.git /root/workspace/trading && \
        cd /root/workspace/trading && \
        cargo build --release
    " 2>&1 | tail -3

    IP=$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$name")
    echo "  $name: $IP (ready)"
done

SERVER_IP=$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$SERVER")
CLIENT_IP=$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$CLIENT")

echo ""
echo "Containers ready. Run the benchmark with:"
echo "  ./scripts/lan-bench.sh $SERVER_IP $CLIENT_IP $SERVER_IP"

if [[ "$WITH_REPLICA" == "true" ]]; then
    REPLICA_IP=$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$REPLICA")
    echo ""
    echo "Replication benchmark:"
    echo "  RUN_FSYNC=0 RUN_NOPERSIST=0 RUN_SINGLE=0 RUN_SWEEPS=0 RUN_PLOTS=0 \\"
    echo "    ./scripts/lan-bench-suite.sh $SERVER_IP $CLIENT_IP $SERVER_IP root $REPLICA_IP $REPLICA_IP"
fi
