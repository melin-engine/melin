#!/usr/bin/env bash
# Stop and remove the Docker containers created by test-containers-start.sh.
#
# Usage:
#   ./scripts/test-containers-stop.sh

set -euo pipefail

for name in bench-server bench-client bench-replica; do
    if docker rm -f "$name" 2>/dev/null; then
        echo "Removed $name"
    fi
done

if docker network rm bench-net 2>/dev/null; then
    echo "Removed bench-net network"
fi

echo "Done."
