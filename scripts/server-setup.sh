#!/usr/bin/env bash
# Setup script for benchmark servers.
#
# Installs system packages, Rust, clones the repo, builds, and runs tests.
# Designed to run as root (either directly or via sudo).
#
# Usage:
#   ./scripts/server-deploy.sh root@<server-ip>   # preferred (handles everything)
#   # or manually:
#   ssh root@<server-ip>
#   bash /tmp/server-setup.sh

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

echo "=== Server setup ==="
echo "  User: $USER_NAME"
echo "  Home: $USER_HOME"
echo ""

# ---------------------------------------------------------------------------
# 1. System packages
# ---------------------------------------------------------------------------
echo "=== Installing system packages ==="
# Cloud images (AWS, GCP, Azure) may not have the universe repo enabled;
# clang/llvm/lld/nasm live there on Ubuntu. No-op if already enabled or
# not Ubuntu (Debian doesn't have universe).
if command -v add-apt-repository &>/dev/null; then
    add-apt-repository -y universe 2>/dev/null || true
fi

APT_INDEX_MAX_AGE=3600
if [[ -f /var/cache/apt/pkgcache.bin ]]; then
    INDEX_AGE=$(( $(date +%s) - $(stat -c %Y /var/cache/apt/pkgcache.bin) ))
    if [[ "$INDEX_AGE" -lt "$APT_INDEX_MAX_AGE" ]]; then
        echo "  apt index ${INDEX_AGE}s old (< ${APT_INDEX_MAX_AGE}s) — skipping refresh"
    else
        apt-get update -qq
    fi
else
    apt-get update -qq
fi

# Required toolchain: build + clang/llvm for bindgen (DPDK FFI). A single
# `apt-get install` lets apt batch dpkg triggers and avoid the global
# lock handoff between calls.
# nasm: required for blake3 to compile its AVX-512 compress_in_place assembly
# stubs (blake3_avx512_ffi); without it blake3 falls back to the SSE4.1 path
# even on AVX-512-capable CPUs (e.g., EPYC 9255 / Zen 4).
# lld: LLVM linker used via .cargo/config.toml; handles fat LTO natively,
# cutting release build times vs GNU ld's LTO plugin handoff.
apt-get install -y --no-install-recommends \
    build-essential \
    pkg-config \
    git \
    curl \
    ca-certificates \
    clang \
    llvm \
    lld \
    libelf-dev \
    nasm \
    xfsprogs

# Optional packages: DPDK kernel-bypass (only with --features dpdk) plus
# benchmarking/diagnostics tools. `|| true` because some of these may
# not be available on every distro/version — keep going with what is.
apt-get install -y --no-install-recommends \
    libdpdk-dev \
    dpdk-dev \
    ethtool \
    numactl \
    irqbalance \
    perf-tools-unstable \
    htop \
    || true

# SR-IOV check for Intel NICs (E810, X710, etc.).
sriov_check() {
    local found=0
    for pci in $(lspci -D | grep -i "Ethernet.*Intel" | awk '{print $1}'); do
        local name
        name=$(lspci -s "${pci#*:}" 2>/dev/null | sed 's/.*: //')
        if [[ -f "/sys/bus/pci/devices/${pci}/sriov_totalvfs" ]]; then
            local max_vfs
            max_vfs=$(cat "/sys/bus/pci/devices/${pci}/sriov_totalvfs")
            echo "  ${name}: SR-IOV available (max ${max_vfs} VFs)"
            found=1
        fi
    done
    if [[ "$found" -eq 0 ]]; then
        echo "  No Intel NIC with SR-IOV found (may need a different kernel or driver)"
    fi
}

sriov_check

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
#   isolcpus/nohz_full/rcu_nocbs: isolate cores 1..N-1 from scheduler/timers/RCU,
#     where N is the number of physical cores (detected at setup time so the
#     same script works on a 16-core 9950X and a 24-core EPYC 9255). Only
#     core 0 is left for the kernel/IRQ/housekeeping work; everything else
#     is reserved for explicitly-pinned Melin pipeline threads. This avoids
#     hardcoding a range that straddles CCD boundaries on parts with fewer
#     cores per CCD than expected.
#   nowatchdog: disable both the NMI (hard-lockup) and soft-lockup watchdogs.
#     The soft-lockup watchdog fires an hrtimer every `watchdog_thresh / 5`
#     seconds (2s at the default thresh=10) on every CPU in `watchdog_cpumask`.
#     Even when the cpumask excludes isolated cores, core 0's watchdog
#     cadence ripples into client-observed tail latency on the persist
#     path — measured as periodic ~600µs-1.2ms spikes every 2s / 10s in
#     single-order tests. Disabling both removes that entire cadence.
#   transparent_hugepage=never: disable THP (khugepaged compaction causes 1-4ms stalls)
#   cpufreq.default_governor=performance: lock max CPU frequency (no scaling transitions)
#   processor.max_cstate=1: prevent deep C-states (C2+ wakeup costs 10-100µs)
#   skew_tick=1: offset timer ticks across cores to reduce kernel lock contention
#   nosmt: disable hyperthreading — prevents HT siblings from polluting L1/L2 on pipeline cores
#   tsc=nowatchdog: disable the kernel's clocksource watchdog for TSC. Separate
#     from `nowatchdog` (which covers the lockup detectors only). The clocksource
#     watchdog periodically (~12s) reads HPET/ACPI PM to validate TSC drift; each
#     check is a ~100µs softirq that preempts the pinned engine thread, producing
#     a recurring p99.99999 cluster at ~106µs measured directly on cpu 9 of an
#     EPYC 7443P with all other tuning in place. AMD EPYC has invariant TSC, so
#     skipping watchdog validation is safe. Verified: after enabling, the 12s
#     cadence in `perf record -e timer:timer_start` disappears and the bench's
#     p99.99999 drops ~79% (102µs → 21µs).
# Use a grub.d drop-in so our params survive platform overrides.
# Cloud images (AWS, GCP) ship a 50-cloudimg-settings.cfg that overwrites
# GRUB_CMDLINE_LINUX_DEFAULT after /etc/default/grub is sourced. A
# 99-melin.cfg drop-in runs last and appends to whatever the platform set,
# so both the platform defaults and our tuning end up on the kernel cmdline.
GRUB_DIR="/etc/default/grub.d"
MELIN_GRUB_CFG="${GRUB_DIR}/99-melin.cfg"

# Count unique physical cores. `lscpu -p=CORE` lists one row per logical CPU
# with its physical core ID; sort -u collapses SMT siblings. nosmt is set
# below in BENCH_PARAMS, so this matches the post-reboot online CPU count.
PHYSICAL_CORES=$(lscpu -p=CORE 2>/dev/null | grep -v '^#' | sort -un | wc -l)
if [[ -z "$PHYSICAL_CORES" || "$PHYSICAL_CORES" -lt 2 ]]; then
    PHYSICAL_CORES=$(nproc 2>/dev/null || echo 16)
    echo "  warning: lscpu core detection failed, falling back to nproc=$PHYSICAL_CORES"
fi
LAST_ISOLATED=$((PHYSICAL_CORES - 1))
ISOLATED_RANGE="1-${LAST_ISOLATED}"
echo "  detected $PHYSICAL_CORES physical cores → isolating ${ISOLATED_RANGE}"

BENCH_PARAMS="isolcpus=nohz,domain,managed_irq,${ISOLATED_RANGE} nohz_full=${ISOLATED_RANGE} rcu_nocbs=${ISOLATED_RANGE} irqaffinity=0 nowatchdog transparent_hugepage=never cpufreq.default_governor=performance processor.max_cstate=1 skew_tick=1 nosmt tsc=nowatchdog"
# IOMMU for DPDK/vfio-pci. iommu=pt sets passthrough mode so DMA
# bypasses IOMMU translation for performance. intel_iommu=on is
# Intel-specific; on AMD (EPYC, Ryzen) the kernel uses AMD-Vi
# automatically when iommu=pt is set.
if grep -qi "AuthenticAMD" /proc/cpuinfo 2>/dev/null; then
    IOMMU_PARAMS="iommu=pt"
else
    IOMMU_PARAMS="intel_iommu=on iommu=pt"
fi

ALL_PARAMS="$BENCH_PARAMS $IOMMU_PARAMS"

if [[ -d "$GRUB_DIR" ]] || [[ -f /etc/default/grub ]]; then
    mkdir -p "$GRUB_DIR"
    NEEDS_UPDATE=0

    if [[ -f "$MELIN_GRUB_CFG" ]]; then
        # Check if the isolated range matches what this host needs.
        CURRENT_RANGE=$(grep -oE 'isolcpus=nohz,domain,[0-9-]+' "$MELIN_GRUB_CFG" 2>/dev/null | sed 's/^isolcpus=nohz,domain,//')
        if [[ -n "$CURRENT_RANGE" && "$CURRENT_RANGE" != "$ISOLATED_RANGE" ]]; then
            echo "=== Updating isolcpus range ==="
            echo "  Current: $CURRENT_RANGE → Desired: $ISOLATED_RANGE"
            NEEDS_UPDATE=1
        elif ! grep -q "tsc=nowatchdog" "$MELIN_GRUB_CFG" 2>/dev/null; then
            echo "  Backfilling tsc=nowatchdog"
            NEEDS_UPDATE=1
        elif ! grep -q "iommu=pt" "$MELIN_GRUB_CFG" 2>/dev/null; then
            echo "  Backfilling IOMMU params"
            NEEDS_UPDATE=1
        fi
    else
        echo "=== Adding kernel boot parameters ==="
        echo "  Adding: $ALL_PARAMS"
        NEEDS_UPDATE=1
    fi

    if [[ "$NEEDS_UPDATE" -eq 1 ]]; then
        cat > "$MELIN_GRUB_CFG" << EOF
# Melin benchmark — kernel tuning (appends to platform defaults).
GRUB_CMDLINE_LINUX_DEFAULT="\$GRUB_CMDLINE_LINUX_DEFAULT $ALL_PARAMS"
EOF
        update-grub
        touch /tmp/.server-needs-reboot
        echo "  *** REBOOT REQUIRED for new kernel params to take effect ***"
    else
        echo "=== Kernel boot params already configured ==="
        cat "$MELIN_GRUB_CFG"
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
# 3b'. Pin device IRQs to core 0
# ---------------------------------------------------------------------------
# `isolcpus` keeps the scheduler off the engine cores, but it does NOT
# steer hardware IRQs. Boot-time defaults spread NIC/NVMe/etc. interrupts
# across cores including the ones we use for journal/matching/response
# /DPDK-poll, so each interrupt delivery stalls a hot-path thread.
# We disabled `irqbalance` above, so a one-shot pin at boot is enough.
echo "=== Pinning device IRQs to core 0 ==="
cat > /usr/local/sbin/melin-irq-pin << 'EOF'
#!/usr/bin/env bash
# Pin all retargetable device IRQs to core 0. EIOs on un-retargetable
# IRQs (per-CPU IPIs, IOMMU remap) are expected and ignored.
set -u
applied=0
skipped=0
for irq in /proc/irq/[0-9]*; do
    if printf '1' > "$irq/smp_affinity" 2>/dev/null; then
        applied=$((applied + 1))
    else
        skipped=$((skipped + 1))
    fi
done
logger -t melin-irq-pin "applied=${applied} skipped=${skipped}"
EOF
chmod +x /usr/local/sbin/melin-irq-pin

cat > /etc/systemd/system/melin-irq-pin.service << 'EOF'
[Unit]
Description=Pin device IRQs to core 0 (Melin bench tuning)
After=multi-user.target
# irqbalance is disabled, but order this after it just in case it ever
# gets re-enabled — we want to be the last writer.
After=irqbalance.service

[Service]
Type=oneshot
ExecStart=/usr/local/sbin/melin-irq-pin
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload
systemctl enable melin-irq-pin.service
# Apply now too so we don't need to wait for a reboot.
/usr/local/sbin/melin-irq-pin
echo "  IRQ pin service installed and applied"

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
# Disable the lockup detectors (hard + soft). Matches `nowatchdog` on the
# kernel cmdline; applied here too so the setting takes effect on hosts
# that can't reboot right now. The soft-lockup watchdog's 2s hrtimer on
# core 0 ripples into persist-path tail latency.
kernel.watchdog = 0
# Cap dirty-page accumulation in absolute bytes so the values don't scale
# with RAM size. Defaults (10%/20% of RAM) on big-RAM boxes let many GiB
# of dirty pages build up before the kernel forces writeback, which can
# trigger 10-100ms `balance_dirty_pages` stalls when it does. The journal
# path itself bypasses this (RWF_DSYNC syncs immediately) but co-tenants
# — log rotation, snapshot writes, replica catch-up — share the same
# global accounting and can stall the whole machine.
vm.dirty_background_bytes = 33554432
vm.dirty_bytes = 67108864
# Network buffer tuning.
# rmem_max / wmem_max: system-wide cap on SO_RCVBUF / SO_SNDBUF for any
# socket. The kernel default (208 KiB) is on the low side for the
# bursty TCP replication writes; raising the ceiling lets `tcp_rmem` /
# `tcp_wmem` autotuning grow buffers without hitting an arbitrary cap.
net.core.rmem_max = 33554432
net.core.wmem_max = 33554432
# TCP receive/send buffer autotuning bounds. The max column is the ceiling
# the kernel can grow a TCP socket's buffer to under load. 16 MiB send max
# (vs the 4 MiB default) reduces write-stalls when the journal replication
# sender is bursting large batches to the replica over the VLAN.
net.ipv4.tcp_rmem = 4096 131072 16777216
net.ipv4.tcp_wmem = 4096 65536 16777216
# NIC → kernel packet queue depth. The default (1000) can back up and drop
# packets during bursts at high throughput. 10000 adds headroom with no
# latency cost in the common case.
net.core.netdev_max_backlog = 10000
EOF
# Raise the system-wide max file descriptor limit. The default (1024) is
# too low for client-sweep benchmarks: 512 clients × 2 fds (stream +
# clone) + server-side accept fds + journal/io_uring fds ≈ 1500+.
LIMITS_FILE="/etc/security/limits.d/99-melin-bench.conf"
cat > "$LIMITS_FILE" << 'EOF'
# Melin benchmark — raise fd and memlock limits.
*    soft nofile 65536
*    hard nofile 65536
root soft nofile 65536
root hard nofile 65536
*    soft memlock unlimited
*    hard memlock unlimited
root soft memlock unlimited
root hard memlock unlimited
EOF
echo "  Written $LIMITS_FILE (nofile=65536)"
sysctl --system --quiet
echo "  Written $SYSCTL_FILE (vm.swappiness=0, kernel.numa_balancing=0, kernel.watchdog=0, net.core.rmem_max=32MiB, net.core.netdev_max_backlog=10000)"

echo ""

# ---------------------------------------------------------------------------
# 3d. Hugepages for DPDK
# ---------------------------------------------------------------------------
# DPDK uses 2 MiB hugepages for packet buffers and memory pools. Allocate
# persistently via sysctl so they survive reboots. 1024 pages = 2 GiB.
HUGEPAGES_SYSCTL="/etc/sysctl.d/99-melin-hugepages.conf"
if [[ ! -f "$HUGEPAGES_SYSCTL" ]]; then
    echo "=== Configuring hugepages for DPDK ==="
    cat > "$HUGEPAGES_SYSCTL" << 'EOF'
# DPDK hugepage allocation — 1024 x 2 MiB = 2 GiB.
vm.nr_hugepages = 1024
EOF
    sysctl --system --quiet
    echo "  Allocated $(cat /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages) x 2 MiB hugepages"
else
    echo "=== Hugepages already configured ==="
    echo "  $(cat /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages) x 2 MiB hugepages"
fi

# ---------------------------------------------------------------------------
# 3e. vfio-pci module (DPDK NIC binding)
# ---------------------------------------------------------------------------
# Ensure vfio-pci is loaded at boot for DPDK to bind NICs via IOMMU.
if ! grep -q "vfio-pci" /etc/modules-load.d/*.conf 2>/dev/null; then
    echo "=== Configuring vfio-pci module autoload ==="
    echo "vfio-pci" > /etc/modules-load.d/vfio-pci.conf
    modprobe vfio-pci 2>/dev/null || true
    echo "  vfio-pci module configured for autoload"
else
    echo "=== vfio-pci already configured ==="
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
    # Skip unexpanded globs (no NVMe devices found).
    [[ -e "$dev" ]] || continue
    # Skip if it has partitions (OS disk).
    if ls "${dev}p"* &>/dev/null; then
        continue
    fi
    JOURNAL_DISK="$dev"
    break
done

if [[ -n "$JOURNAL_DISK" ]]; then
    echo "=== Journal disk: $JOURNAL_DISK → $JOURNAL_MOUNT ==="

    # Filesystem: xfs. We previously used ext4 with `data=writeback,
    # journal_async_commit, commit=300` and observed periodic 1–2 ms
    # `fdatasync` spikes at ~256 MiB write boundaries, correlated across
    # all replicas (deterministic event stream → identical byte layout →
    # ext4's jbd2 metadata batching fires at the same offset on every
    # node). The hybrid durability gate masks single-node hiccups, but
    # when all three nodes hit the spike simultaneously the bench sees a
    # ~10 s-cadence outlier in the tail. xfs doesn't exhibit this
    # behaviour on the same hardware: same throughput, p50/p99 unchanged,
    # max latency cut from ~2.6 ms to ~1.5 ms and the periodic cadence
    # vanishes entirely.
    if ! blkid "$JOURNAL_DISK" | grep -q TYPE; then
        echo "  Formatting $JOURNAL_DISK as xfs..."
        mkfs.xfs -f -q "$JOURNAL_DISK"
    elif ! blkid "$JOURNAL_DISK" | grep -q 'TYPE="xfs"'; then
        # Migrating from a previous ext4 layout: wipe and reformat. Safe
        # at setup time — nothing has opened the journal yet.
        echo "  Migrating $JOURNAL_DISK from $(blkid -s TYPE -o value "$JOURNAL_DISK") to xfs..."
        mountpoint -q "$JOURNAL_MOUNT" && umount "$JOURNAL_MOUNT"
        wipefs -a "$JOURNAL_DISK" >/dev/null
        mkfs.xfs -f -q "$JOURNAL_DISK"
        # Drop any stale ext4 fstab entry; we re-add the xfs one below.
        sed -i "\| $JOURNAL_MOUNT |d" /etc/fstab
    fi

    mkdir -p "$JOURNAL_MOUNT"

    # Mount options for journal I/O on dedicated NVMe + xfs:
    #   noatime:     skip access-time metadata updates
    #   logbsize=256k: larger in-memory log buffer reduces log-write
    #                 frequency, letting xfs batch metadata changes
    #                 (extent conversions, inode timestamp bumps) into
    #                 fewer, larger journal writes
    #   logbufs=8:   match xfs's max log buffers; trades a bit of memory
    #                for less log-buffer contention at this write rate
    JOURNAL_MOUNT_OPTS="noatime,logbsize=256k,logbufs=8"

    # Mount if not already mounted.
    if ! mountpoint -q "$JOURNAL_MOUNT"; then
        mount -o "$JOURNAL_MOUNT_OPTS" "$JOURNAL_DISK" "$JOURNAL_MOUNT"
        echo "  Mounted $JOURNAL_DISK at $JOURNAL_MOUNT ($JOURNAL_MOUNT_OPTS)"
    else
        echo "  Already mounted at $JOURNAL_MOUNT"
        # Remount with the standard options. xfs accepts remount for the
        # options we set; safe during setup since nothing is using the
        # journal disk yet.
        echo "  Remounting with standard options..."
        mount -o "remount,$JOURNAL_MOUNT_OPTS" "$JOURNAL_MOUNT"
        echo "  Remounted with $JOURNAL_MOUNT_OPTS"
    fi

    # Add to fstab if not present.
    if ! grep -q "$JOURNAL_MOUNT" /etc/fstab; then
        UUID=$(blkid -s UUID -o value "$JOURNAL_DISK")
        echo "UUID=$UUID $JOURNAL_MOUNT xfs $JOURNAL_MOUNT_OPTS 0 2" >> /etc/fstab
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
# Applied to the journal disk identified during server-setup.sh.
ACTION=="add|change", KERNEL=="$JOURNAL_DEV", ATTR{queue/scheduler}="none", ATTR{queue/nr_requests}="2", ATTR{queue/nomerges}="2", ATTR{queue/wbt_lat_usec}="0", ATTR{queue/add_random}="0"
EOF
    echo "  Installed udev rule: $UDEV_RULE"
else
    echo "=== No dedicated journal disk found, skipping ==="
    mkdir -p "$JOURNAL_MOUNT"
    chown "${USER_NAME}:${USER_NAME}" "$JOURNAL_MOUNT"
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
# 5. Clone the repo
# ---------------------------------------------------------------------------
echo "=== Cloning the repo ==="

REPO_DIR="$USER_HOME/workspace/melin"

if [[ -d "$REPO_DIR/.git" ]]; then
    echo "  Repo already exists at $REPO_DIR, pulling latest..."
    run_as_user "cd $REPO_DIR && git checkout main && git pull"
else
    echo "  Cloning repo..."
    mkdir -p "$USER_HOME/workspace"
    chown "$USER_NAME:" "$USER_HOME/workspace"
    run_as_user "git clone https://github.com/melin-engine/melin.git $REPO_DIR"
    run_as_user "cd $REPO_DIR && git checkout main"
fi

echo ""

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------
echo "=== Setup complete ==="
echo ""
echo "Next steps:"
echo "  1. Reboot if kernel boot params were added (isolcpus)"
echo "  2. Verify after reboot:"
echo "     cat /sys/devices/system/cpu/isolated   # should show: 1-10"
echo "     cat /sys/devices/system/cpu/nohz_full  # should show: 1-10"
echo ""
echo "  3. Run the LAN benchmark from your local machine:"
echo "     ./scripts/lan-bench.sh <server-pub-ip> <bench-pub-ip> <server-vlan-ip>"
echo ""
