#!/usr/bin/env bash
# Setup script for Cherry benchmark servers.
#
# Installs system packages, Rust, clones the repo, builds, and runs tests.
# Designed to run as root (either directly or via sudo).
#
# Usage:
#   ./scripts/cherry-deploy.sh root@<server-ip>   # preferred (handles everything)
#   # or manually:
#   ssh root@<server-ip>
#   bash /tmp/cherry-setup.sh

set -euo pipefail

if [[ $EUID -ne 0 ]]; then
    echo "error: must run as root" >&2
    exit 1
fi

# When run via sudo, operate as the invoking user. When run as root
# directly (e.g., ssh root@host), operate as root.
USER_NAME="${SUDO_USER:-root}"
USER_HOME=$(eval echo "~$USER_NAME")

run_as_user() {
    if [[ "$USER_NAME" == "root" ]]; then
        bash -c "$1"
    else
        sudo -u "$USER_NAME" bash -c "$1"
    fi
}

echo "=== Cherry server setup ==="
echo "  User: $USER_NAME"
echo "  Home: $USER_HOME"
echo ""

# ---------------------------------------------------------------------------
# 1. System packages
# ---------------------------------------------------------------------------
echo "=== Installing system packages ==="
apt-get update -qq

apt-get install -y --no-install-recommends \
    build-essential \
    pkg-config \
    git \
    curl \
    ca-certificates

# AF_XDP / XDP dependencies
apt-get install -y --no-install-recommends \
    libxdp-dev \
    m4 \
    clang \
    llvm \
    libelf-dev \
    bpftool

# Benchmarking / diagnostics
apt-get install -y --no-install-recommends \
    ethtool \
    numactl \
    irqbalance \
    perf-tools-unstable \
    htop \
    || true  # some packages may not exist on all distros

echo ""

# ---------------------------------------------------------------------------
# 2. Rust toolchain
# ---------------------------------------------------------------------------
if run_as_user 'command -v rustup' &>/dev/null; then
    echo "=== Rust already installed, updating ==="
    run_as_user 'rustup update stable'
else
    echo "=== Installing Rust toolchain ==="
    run_as_user 'curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable'
fi

# Add cargo to system PATH so `sudo cargo` works without `source .cargo/env`.
CARGO_PROFILE="$USER_HOME/.cargo/env"
if [[ -f "$CARGO_PROFILE" ]]; then
    CARGO_BIN="$USER_HOME/.cargo/bin"
    if ! grep -q "$CARGO_BIN" /etc/environment 2>/dev/null; then
        # Append cargo bin to the system-wide PATH.
        if [[ -f /etc/environment ]]; then
            sed -i "s|^PATH=\"\(.*\)\"|PATH=\"\1:$CARGO_BIN\"|" /etc/environment
            # If PATH wasn't in /etc/environment, add it.
            grep -q "^PATH=" /etc/environment || echo "PATH=\"/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:$CARGO_BIN\"" >> /etc/environment
        else
            echo "PATH=\"/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:$CARGO_BIN\"" > /etc/environment
        fi
        echo "  Added $CARGO_BIN to /etc/environment"
    fi
    # Also create a symlink so cargo is immediately available in this session.
    if [[ ! -L /usr/local/bin/cargo ]]; then
        ln -sf "$CARGO_BIN/cargo" /usr/local/bin/cargo
        ln -sf "$CARGO_BIN/rustup" /usr/local/bin/rustup
        ln -sf "$CARGO_BIN/rustc" /usr/local/bin/rustc
        echo "  Symlinked cargo/rustup/rustc to /usr/local/bin"
    fi
fi

echo ""

# ---------------------------------------------------------------------------
# 3. Kernel boot parameters (isolcpus + nohz_full + rcu_nocbs)
# ---------------------------------------------------------------------------
GRUB_FILE="/etc/default/grub"
BENCH_PARAMS="isolcpus=nohz,domain,1-5 nohz_full=1-5 rcu_nocbs=1-5"

if [[ -f "$GRUB_FILE" ]]; then
    if grep -q "isolcpus" "$GRUB_FILE" 2>/dev/null; then
        echo "=== Kernel boot params already configured ==="
        grep "GRUB_CMDLINE_LINUX_DEFAULT" "$GRUB_FILE"
    else
        echo "=== Configuring kernel boot parameters ==="
        echo "  Adding: $BENCH_PARAMS"
        cp "$GRUB_FILE" "${GRUB_FILE}.bak"
        sed -i "s/^GRUB_CMDLINE_LINUX_DEFAULT=\"\(.*\)\"/GRUB_CMDLINE_LINUX_DEFAULT=\"\1 $BENCH_PARAMS\"/" "$GRUB_FILE"
        update-grub
        echo "  *** REBOOT REQUIRED for isolcpus to take effect ***"
    fi
else
    echo "=== No GRUB config found, skipping kernel boot params ==="
fi

echo ""

# ---------------------------------------------------------------------------
# 4. Journal disk (dedicated NVMe)
# ---------------------------------------------------------------------------
JOURNAL_MOUNT="/mnt/journal"
# Find the second NVMe disk (not the OS disk). The OS disk has partitions;
# the journal disk is a raw whole-disk device with no partitions.
JOURNAL_DISK=""
for dev in /dev/nvme*n1; do
    # Skip if it has partitions (OS disk).
    if ls "${dev}p"* &>/dev/null; then
        continue
    fi
    JOURNAL_DISK="$dev"
    break
done

if [[ -n "$JOURNAL_DISK" ]]; then
    echo "=== Journal disk: $JOURNAL_DISK → $JOURNAL_MOUNT ==="

    # Format if no filesystem yet.
    if ! blkid "$JOURNAL_DISK" | grep -q TYPE; then
        echo "  Formatting $JOURNAL_DISK as ext4..."
        mkfs.ext4 -q "$JOURNAL_DISK"
    fi

    mkdir -p "$JOURNAL_MOUNT"

    # Mount if not already mounted.
    if ! mountpoint -q "$JOURNAL_MOUNT"; then
        mount "$JOURNAL_DISK" "$JOURNAL_MOUNT"
        echo "  Mounted $JOURNAL_DISK at $JOURNAL_MOUNT"
    else
        echo "  Already mounted at $JOURNAL_MOUNT"
    fi

    # Add to fstab if not present.
    if ! grep -q "$JOURNAL_MOUNT" /etc/fstab; then
        UUID=$(blkid -s UUID -o value "$JOURNAL_DISK")
        echo "UUID=$UUID $JOURNAL_MOUNT ext4 defaults,noatime 0 2" >> /etc/fstab
        echo "  Added to /etc/fstab (UUID=$UUID)"
    fi
else
    echo "=== No dedicated journal disk found, skipping ==="
fi

echo ""

# ---------------------------------------------------------------------------
# 5. NIC diagnostics
# ---------------------------------------------------------------------------
echo "=== Network interfaces ==="
echo ""

# Wrap in a subshell with +e so failures don't abort the script.
(
    set +e
    for iface in $(ls /sys/class/net/ | grep -v lo); do
        driver=$(ethtool -i "$iface" 2>/dev/null | awk '/^driver:/{print $2}')
        driver="${driver:-unknown}"
        xdp_support="unknown"
        case "$driver" in
            ixgbe|ice|i40e|igb|igc|mlx5_core|mlx4_en|bnxt_en|nfp|virtio_net|veth)
                xdp_support="native"
                ;;
            unknown) ;;
            *) xdp_support="skb-only" ;;
        esac
        iface_ip=$(ip -4 addr show "$iface" 2>/dev/null | awk '/inet /{print $2}' | cut -d/ -f1)
        iface_ip="${iface_ip:-no-ip}"
        queues=$(ls -d /sys/class/net/"$iface"/queues/rx-* 2>/dev/null | wc -l || echo 0)
        echo "  $iface: driver=$driver, ip=$iface_ip, rx_queues=$queues, xdp=$xdp_support"

        # For bonded interfaces, show slave info.
        if [[ "$driver" == "bonding" ]] && ls /sys/class/net/"$iface"/lower_* &>/dev/null; then
            for slave in /sys/class/net/"$iface"/lower_*; do
                slave_name=$(basename "$slave" | sed 's/lower_//')
                slave_driver=$(ethtool -i "$slave_name" 2>/dev/null | awk '/^driver:/{print $2}')
                slave_driver="${slave_driver:-unknown}"
                echo "    slave $slave_name: driver=$slave_driver"
            done
        fi
    done
)

echo ""

# ---------------------------------------------------------------------------
# 5. Clone and build
# ---------------------------------------------------------------------------
echo "=== Cloning and building ==="

REPO_DIR="$USER_HOME/workspace/trading"

if [[ -d "$REPO_DIR/.git" ]]; then
    echo "  Repo already exists at $REPO_DIR, pulling latest..."
    run_as_user "cd $REPO_DIR && git checkout main && git pull"
else
    echo "  Cloning repo..."
    mkdir -p "$USER_HOME/workspace"
    chown "$USER_NAME:" "$USER_HOME/workspace"
    run_as_user "git clone git@github.com:pierre-l/trading.git $REPO_DIR"
    run_as_user "cd $REPO_DIR && git checkout main"
fi

echo "  Building default (TCP + io_uring)..."
run_as_user "source $USER_HOME/.cargo/env && cd $REPO_DIR && cargo build --release" 2>&1 | tail -3
echo "  Default build: OK"

echo "  Building with --features no-persist,pipeline-stats..."
run_as_user "source $USER_HOME/.cargo/env && cd $REPO_DIR && cargo build --release -p trading-server --features no-persist,pipeline-stats" 2>&1 | tail -3
echo "  Bench build: OK"

echo ""

# ---------------------------------------------------------------------------
# 6. Quick self-test
# ---------------------------------------------------------------------------
echo "=== Quick self-test ==="
run_as_user "source $USER_HOME/.cargo/env && cd $REPO_DIR && cargo test" 2>&1 | grep "test result:" | head -5
echo ""

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------
echo "=== Setup complete ==="
echo ""
echo "Next steps:"
echo "  1. Reboot if kernel boot params were added (isolcpus)"
echo "  2. Verify after reboot:"
echo "     cat /sys/devices/system/cpu/isolated   # should show: 1-5"
echo "     cat /sys/devices/system/cpu/nohz_full  # should show: 1-5"
echo ""
echo "  3. Run the LAN benchmark from your local machine:"
echo "     ./scripts/lan-bench.sh <server-pub-ip> <bench-pub-ip> <server-vlan-ip>"
echo ""
