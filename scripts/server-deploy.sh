#!/usr/bin/env bash
# Deploy and setup a benchmark server.
#
# Copies SSH credentials and the setup script to the remote server,
# then runs setup via SSH. Expects root access on the remote.
#
# Usage:
#   ./scripts/server-deploy.sh <remote>
#
# Example:
#   ./scripts/server-deploy.sh root@84.32.176.142

set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo "usage: $0 <remote>"
    echo "  e.g. $0 root@84.32.176.142"
    exit 1
fi

REMOTE="$1"
SETUP_SCRIPT="$(dirname "$0")/server-setup.sh"

# Skip host key prompt on first connect — these are throwaway bench boxes.
SSH_OPTS=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR)
ssh()  { command ssh  "${SSH_OPTS[@]}" "$@"; }
scp()  { command scp  "${SSH_OPTS[@]}" "$@"; }

if [[ ! -f "$SETUP_SCRIPT" ]]; then
    echo "error: $SETUP_SCRIPT not found" >&2
    exit 1
fi

echo "=== Deploying to $REMOTE ==="

# 1. Copy and run the setup script.
echo "  Copying setup script..."
scp -q "$SETUP_SCRIPT" "$REMOTE:/tmp/server-setup.sh"
ssh "$REMOTE" "chmod +x /tmp/server-setup.sh"

echo "  Running setup (this may take a few minutes)..."
echo ""
# `sudo` here is a no-op when REMOTE is `root@host` and an
# elevation when REMOTE is e.g. `debian@host` / `ubuntu@host` on
# bare-metal providers that don't expose root via SSH (latitude.sh,
# Hetzner Robot, etc.). Relies on NOPASSWD sudo, which is the
# default on those images.
# Forward JOURNAL_DISK through `sudo env` rather than `sudo -E` —
# sudo's default `env_reset` drops arbitrary caller env vars even
# with -E, so we set them explicitly via the env(1) wrapper.
# Use bash explicitly to avoid TTY issues with ssh -t.
ssh "$REMOTE" "sudo -n env JOURNAL_DISK='${JOURNAL_DISK:-}' bash /tmp/server-setup.sh"

# 3. Reboot if kernel boot params were just configured.
NEEDS_REBOOT=$(ssh "$REMOTE" "test -f /tmp/.server-needs-reboot && echo yes || echo no")
if [[ "$NEEDS_REBOOT" == "yes" ]]; then
    echo "=== Rebooting for kernel boot params (isolcpus, nohz_full, rcu_nocbs) ==="
    ssh "$REMOTE" "sudo -n rm -f /tmp/.server-needs-reboot && sudo -n shutdown -r now" 2>/dev/null || true

    # Wait for SSH to go down.
    echo -n "  Waiting for shutdown..."
    sleep 5
    for _ in $(seq 1 30); do
        if ! ssh -o ConnectTimeout=2 -o BatchMode=yes "$REMOTE" true 2>/dev/null; then
            break
        fi
        sleep 1
    done
    echo " down."

    # Wait for SSH to come back up.
    echo -n "  Waiting for reboot..."
    for _ in $(seq 1 120); do
        if ssh -o ConnectTimeout=2 -o BatchMode=yes "$REMOTE" true 2>/dev/null; then
            echo " up."
            break
        fi
        sleep 2
    done

    # Verify boot params are active.
    echo ""
    echo "=== Verifying kernel boot params ==="
    ISOLATED=$(ssh "$REMOTE" "cat /sys/devices/system/cpu/isolated 2>/dev/null")
    NOHZ=$(ssh "$REMOTE" "cat /sys/devices/system/cpu/nohz_full 2>/dev/null")
    echo "  isolated cores: ${ISOLATED:-none}"
    echo "  nohz_full:      ${NOHZ:-none}"
    if [[ -z "$ISOLATED" ]]; then
        echo "  WARNING: isolcpus not active — check GRUB config"
    fi
fi

echo ""
echo "=== Deployment complete. Connecting... ==="
unset -f ssh scp
exec ssh "${SSH_OPTS[@]}" -t "$REMOTE" "cd ~/workspace/melin && exec bash --login"
