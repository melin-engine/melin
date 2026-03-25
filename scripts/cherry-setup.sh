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
# 3. Kernel boot parameters
# ---------------------------------------------------------------------------
# Core isolation, tick suppression, and latency tuning — all persistent
# across reboots via GRUB.
#   isolcpus/nohz_full/rcu_nocbs: isolate cores 1-6 from scheduler/timers/RCU
#   nmi_watchdog=0: disable NMI watchdog (eliminates periodic NMI interrupts)
#   transparent_hugepage=never: disable THP (khugepaged compaction causes 1-4ms stalls)
#   cpufreq.default_governor=performance: lock max CPU frequency (no scaling transitions)
#   processor.max_cstate=1: prevent deep C-states (C2+ wakeup costs 10-100µs)
#   skew_tick=1: offset timer ticks across cores to reduce kernel lock contention
#   nosmt: disable hyperthreading — prevents HT siblings from polluting L1/L2 on pipeline cores
GRUB_FILE="/etc/default/grub"
BENCH_PARAMS="isolcpus=nohz,domain,1-6 nohz_full=1-6 rcu_nocbs=1-6 nmi_watchdog=0 transparent_hugepage=never cpufreq.default_governor=performance processor.max_cstate=1 skew_tick=1 nosmt"

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
        # Signal to cherry-deploy.sh that a reboot is needed.
        touch /tmp/.cherry-needs-reboot
        echo "  *** REBOOT REQUIRED for isolcpus to take effect ***"
    fi
else
    echo "=== No GRUB config found, skipping kernel boot params ==="
fi

echo ""

# ---------------------------------------------------------------------------
# 3b. Disable noisy background services
# ---------------------------------------------------------------------------
# These services wake up periodically on random cores, causing latency
# spikes via context switches, IPI interrupts, or disk I/O on pipeline
# cores. Disabling them is safe on single-purpose benchmark servers.
echo "=== Disabling background services ==="
for svc in irqbalance unattended-upgrades multipathd smartd cron; do
    if systemctl is-active --quiet "$svc" 2>/dev/null; then
        systemctl stop "$svc"
        systemctl disable "$svc"
        echo "  $svc → stopped and disabled"
    elif systemctl is-enabled --quiet "$svc" 2>/dev/null; then
        systemctl disable "$svc"
        echo "  $svc → disabled (was not running)"
    else
        echo "  $svc → not present"
    fi
done

echo ""

# ---------------------------------------------------------------------------
# 3c. Sysctl tuning (persistent via /etc/sysctl.d/)
# ---------------------------------------------------------------------------
echo "=== Configuring sysctl ==="
SYSCTL_FILE="/etc/sysctl.d/99-melin-bench.conf"
cat > "$SYSCTL_FILE" << 'EOF'
# Melin exchange engine — latency tuning.
# Never swap — a single page-in is ~1ms.
vm.swappiness = 0
# Disable automatic NUMA page migration. The balancing scanner wakes up
# periodically and can stall cores. Single-socket servers don't benefit.
kernel.numa_balancing = 0
EOF
# Raise the system-wide max file descriptor limit. The default (1024) is
# too low for client-sweep benchmarks: 512 clients × 2 fds (stream +
# clone) + server-side accept fds + journal/io_uring fds ≈ 1500+.
LIMITS_FILE="/etc/security/limits.d/99-melin-bench.conf"
cat > "$LIMITS_FILE" << 'EOF'
# Melin benchmark — raise fd limits for high client counts.
*    soft nofile 65536
*    hard nofile 65536
root soft nofile 65536
root hard nofile 65536
EOF
echo "  Written $LIMITS_FILE (nofile=65536)"
sysctl --system --quiet
echo "  Written $SYSCTL_FILE (vm.swappiness=0, kernel.numa_balancing=0)"

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

    # Mount options optimized for journal I/O on dedicated NVMe:
    #   noatime:    skip access-time metadata updates
    #   nobarrier:  skip disk cache flush barriers — redundant with
    #               NVMe FUA + RWF_DSYNC (per-write durability)
    #   data=writeback: don't order data writes before metadata journal
    #               commits — safe because RWF_DSYNC ensures data is on
    #               persistent storage before the syscall returns
    #   journal_async_commit: don't wait for ext4 journal commit completion
    #               before returning from metadata operations — reduces
    #               latency for extent conversions (unwritten → written)
    #   commit=300: delay ext4 journal commits to every 300s instead of 5s
    #               — our data path (RWF_DSYNC/FUA) doesn't rely on ext4
    #               journal commits for durability, so minimize their
    #               frequency to reduce jbd2 lock contention
    JOURNAL_MOUNT_OPTS="noatime,nobarrier,data=writeback,journal_async_commit,commit=300"

    # Mount if not already mounted.
    if ! mountpoint -q "$JOURNAL_MOUNT"; then
        mount -o "$JOURNAL_MOUNT_OPTS" "$JOURNAL_DISK" "$JOURNAL_MOUNT"
        echo "  Mounted $JOURNAL_DISK at $JOURNAL_MOUNT ($JOURNAL_MOUNT_OPTS)"
    else
        echo "  Already mounted at $JOURNAL_MOUNT"
        # data=writeback cannot be changed via remount — must unmount first.
        # Safe during setup: nothing is using the journal disk yet.
        echo "  Unmounting and remounting with optimized options..."
        umount "$JOURNAL_MOUNT"
        mount -o "$JOURNAL_MOUNT_OPTS" "$JOURNAL_DISK" "$JOURNAL_MOUNT"
        echo "  Remounted with $JOURNAL_MOUNT_OPTS"
    fi

    # Add to fstab if not present.
    if ! grep -q "$JOURNAL_MOUNT" /etc/fstab; then
        UUID=$(blkid -s UUID -o value "$JOURNAL_DISK")
        echo "UUID=$UUID $JOURNAL_MOUNT ext4 $JOURNAL_MOUNT_OPTS 0 2" >> /etc/fstab
        echo "  Added to /etc/fstab (UUID=$UUID)"
    fi

    # NVMe block device tuning — reduces jitter (p99.9/max) by eliminating
    # non-deterministic software overhead on the I/O path. These are sysfs
    # settings that don't survive reboots, so we also install a udev rule.
    #
    #   scheduler=none:   bypass mq-deadline's per-I/O sorting, timer-based
    #                     batching, and lock acquisition
    #   nr_requests=2:    minimal software queue depth — one inflight + one
    #                     queued (for future overlapped io_uring writes)
    #   nomerges=2:       skip merge scan entirely — FUA writes are issued
    #                     individually with nothing to merge; scan time varies
    #                     with queue depth (non-deterministic)
    #   wbt_lat_usec=0:   disable writeback throttling — can inject artificial
    #                     delays based on a moving average; not needed on a
    #                     dedicated single-writer device
    #   add_random=0:     don't feed I/O completion timing into the entropy
    #                     pool — avoids a spinlock per I/O completion
    JOURNAL_DEV=$(basename "$JOURNAL_DISK")
    BLOCK_SYSFS="/sys/block/$JOURNAL_DEV/queue"

    echo "  NVMe block tuning ($JOURNAL_DEV):"
    for param in "scheduler none" "nr_requests 2" "nomerges 2" "wbt_lat_usec 0" "add_random 0"; do
        key="${param% *}"
        val="${param#* }"
        target="$BLOCK_SYSFS/$key"
        if [[ -f "$target" ]]; then
            old=$(cat "$target" 2>/dev/null | sed 's/.*\[\(.*\)\].*/\1/' || true)
            echo "$val" > "$target" 2>/dev/null || true
            echo "    $key: $old → $val"
        else
            echo "    $key: (not available)"
        fi
    done

    # Persist via udev rule so settings survive reboots.
    UDEV_RULE="/etc/udev/rules.d/99-melin-journal-nvme.rules"
    cat > "$UDEV_RULE" << EOF
# Melin journal NVMe tuning — reduce block layer jitter.
# Applied to the journal disk identified during cherry-setup.sh.
ACTION=="add|change", KERNEL=="$JOURNAL_DEV", ATTR{queue/scheduler}="none", ATTR{queue/nr_requests}="2", ATTR{queue/nomerges}="2", ATTR{queue/wbt_lat_usec}="0", ATTR{queue/add_random}="0"
EOF
    echo "  Installed udev rule: $UDEV_RULE"
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
run_as_user "source $USER_HOME/.cargo/env && cd $REPO_DIR && CARGO_NET_GIT_FETCH_WITH_CLI=true cargo build --release" 2>&1 | tail -3
echo "  Default build: OK"

echo "  Building with --features no-persist,pipeline-stats..."
run_as_user "source $USER_HOME/.cargo/env && cd $REPO_DIR && CARGO_NET_GIT_FETCH_WITH_CLI=true cargo build --release -p melin-server --features no-persist,pipeline-stats" 2>&1 | tail -3
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
echo "     cat /sys/devices/system/cpu/isolated   # should show: 1-6"
echo "     cat /sys/devices/system/cpu/nohz_full  # should show: 1-6"
echo ""
echo "  3. Run the LAN benchmark from your local machine:"
echo "     ./scripts/lan-bench.sh <server-pub-ip> <bench-pub-ip> <server-vlan-ip>"
echo ""
