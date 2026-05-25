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
SSH_KEY="$HOME/.ssh/te_test_ed"
SSH_PUB="$HOME/.ssh/te_test_ed.pub"
SETUP_SCRIPT="$(dirname "$0")/server-setup.sh"

# Skip host key prompt on first connect — these are throwaway bench boxes.
SSH_OPTS=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR)
ssh()  { command ssh  "${SSH_OPTS[@]}" "$@"; }
scp()  { command scp  "${SSH_OPTS[@]}" "$@"; }

# Verify local files exist.
for f in "$SSH_KEY" "$SSH_PUB" "$SETUP_SCRIPT"; do
    if [[ ! -f "$f" ]]; then
        echo "error: $f not found" >&2
        exit 1
    fi
done

echo "=== Deploying to $REMOTE ==="

# 1. Copy SSH credentials for GitHub access.
echo "  Copying SSH keys..."
ssh "$REMOTE" "mkdir -p ~/.ssh && chmod 700 ~/.ssh"
scp -q "$SSH_KEY" "$REMOTE:~/.ssh/te_test_ed"
scp -q "$SSH_PUB" "$REMOTE:~/.ssh/te_test_ed.pub"
ssh "$REMOTE" "chmod 600 ~/.ssh/te_test_ed ~/.ssh/te_test_ed.pub"

# Configure SSH to use this key for GitHub.
ssh "$REMOTE" 'grep -q "github.com" ~/.ssh/config 2>/dev/null || cat >> ~/.ssh/config << EOF

Host github.com
    IdentityFile ~/.ssh/te_test_ed
    StrictHostKeyChecking no
EOF
chmod 600 ~/.ssh/config'

echo "  SSH keys deployed."

# 2. Copy and run the setup script.
echo "  Copying setup script..."
scp -q "$SETUP_SCRIPT" "$REMOTE:/tmp/server-setup.sh"
ssh "$REMOTE" "chmod +x /tmp/server-setup.sh"

echo "  Running setup (this may take a few minutes)..."
echo ""
# Run directly (not via sudo) — we're already root.
# Use bash explicitly to avoid TTY issues with ssh -t.
ssh "$REMOTE" "bash /tmp/server-setup.sh"

# 3. Reboot if kernel boot params were just configured.
NEEDS_REBOOT=$(ssh "$REMOTE" "test -f /tmp/.server-needs-reboot && echo yes || echo no")
if [[ "$NEEDS_REBOOT" == "yes" ]]; then
    echo "=== Rebooting for kernel boot params (isolcpus, nohz_full, rcu_nocbs) ==="
    ssh "$REMOTE" "rm -f /tmp/.server-needs-reboot && shutdown -r now" 2>/dev/null || true

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
