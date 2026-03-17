#!/usr/bin/env bash
# Deploy and setup a Cherry benchmark server.
#
# Copies SSH credentials and the setup script to the remote server,
# then runs setup via SSH. Expects root access on the remote.
#
# Usage:
#   ./scripts/cherry-deploy.sh <remote>
#
# Example:
#   ./scripts/cherry-deploy.sh root@84.32.176.142

set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo "usage: $0 <remote>"
    echo "  e.g. $0 root@84.32.176.142"
    exit 1
fi

REMOTE="$1"
SSH_KEY="$HOME/.ssh/te_test_ed"
SSH_PUB="$HOME/.ssh/te_test_ed.pub"
SETUP_SCRIPT="$(dirname "$0")/cherry-setup.sh"

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
scp -q "$SETUP_SCRIPT" "$REMOTE:/tmp/cherry-setup.sh"
ssh "$REMOTE" "chmod +x /tmp/cherry-setup.sh"

echo "  Running setup (this may take a few minutes)..."
echo ""
# Run directly (not via sudo) — we're already root.
# Use bash explicitly to avoid TTY issues with ssh -t.
ssh "$REMOTE" "bash /tmp/cherry-setup.sh"

echo ""
echo "=== Deployment complete. Connecting... ==="
exec ssh -t "$REMOTE" "cd ~/workspace/trading && exec bash --login"
