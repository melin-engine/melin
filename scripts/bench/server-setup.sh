#!/usr/bin/env bash
# Setup script for benchmark servers.
#
# Installs the packages a bench host runs the binaries with (and the
# libraries the LAN suite links them against: see lan-bench-suite.sh's
# sysroot), tunes the kernel for latency, and prepares the journal disk
# and DPDK. Nothing is built here: the suite builds on the operator's
# machine and copies the binaries over. Designed to run as root (either
# directly or via sudo).
#
# Kept in step with the Exchange Core's copy, and writing the same files
# (the `99-melin-ec-bench` drop-ins among them), so a host set up by
# either converges on one configuration rather than two.
#
# Usage:
#   ./scripts/bench/server-deploy.sh root@<server-ip>   # preferred (handles everything)
#   # or manually:
#   ssh root@<server-ip>
#   bash /tmp/server-setup.sh

set -euo pipefail

if [[ $EUID -ne 0 ]]; then
    echo "error: must run as root" >&2
    exit 1
fi

echo "=== Server setup ==="
echo ""

# ---------------------------------------------------------------------------
# 1. System packages
# ---------------------------------------------------------------------------
echo "=== Installing system packages ==="
# Skip `apt-get update` if the index was refreshed within the last hour.
# `-qq` forces a refresh otherwise; on a freshly-provisioned box this
# dominates the setup boot time. Threshold is conservative: 1h is short
# enough that security updates land same-day in practice.
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

# Required. A single `apt-get install` lets apt batch dpkg triggers and
# avoid the global lock handoff between calls.
# build-essential: no compiler runs here, but the suite's sysroot is
#   taken from this host, and a link needs its C runtime objects and
#   libgcc (gcc's) and glibc's development files (libc6-dev's). gcc also
#   names the host's CPU for the suite's `-C target-cpu`.
# libdpdk-dev: the DPDK binaries' runtime libraries, and the headers and
#   pkg-config file the suite builds them against.
# curl: the suite reads the primary's health metrics from this host.
apt-get install -y --no-install-recommends \
    build-essential \
    libdpdk-dev \
    curl \
    ca-certificates \
    xfsprogs

# Optional packages: DPDK tools (dpdk-devbind, testpmd) plus
# benchmarking/diagnostics tools. `|| true` because some of these may
# not be available on every distro/version — keep going with what is.
apt-get install -y --no-install-recommends \
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

echo ""

# ---------------------------------------------------------------------------
# 3. Kernel boot parameters
# ---------------------------------------------------------------------------
# Core isolation, tick suppression, and latency tuning — all persistent
# across reboots via GRUB. Applied through a drop-in at /etc/default/grub.d
# and then verified against both the generated grub.cfg and /proc/cmdline;
# see the GRUB_DROPIN assignment below for why neither half is optional.
#
# What each parameter does:
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
GRUB_FILE="/etc/default/grub"
# Resolved rather than assumed to be /usr/sbin/grub-mkconfig: the guard
# below reads this script to confirm it sources our drop-in directory, and
# a path that happens to be wrong would fail that check — and abort the
# run — on a host where nothing is actually wrong.
GRUB_MKCONFIG="$(command -v grub-mkconfig || true)"
# Debian's grub-mkconfig sources /etc/default/grub, then every *.cfg in
# this directory. Owning a file here means never editing the vendor's.
#
# That ownership, and the verification pass further down, both exist because
# of the same incident. The previous version of this script `sed`-ed
# `GRUB_CMDLINE_LINUX_DEFAULT`, which some vendor images (latitude.sh
# Debian 13 among them) do not define at all. The substitution matched
# nothing, `update-grub` ran over an unchanged file, and the script still
# printed "REBOOT REQUIRED" — so hosts booted completely untuned while
# looking configured, and every re-run repeated the no-op because its guard
# was "does /etc/default/grub mention isolcpus". Writing a file we own
# removes the failure mode; checking the result afterwards is what catches
# the next one.
#
# This exact path is also named in docs/benchmarking.md, which tells operators
# to hand-write a *differently named* drop-in for manual tuning — because
# this one is regenerated wholesale on every run and would silently discard
# their edits.
GRUB_DROPIN_DIR="/etc/default/grub.d"
GRUB_DROPIN="${GRUB_DROPIN_DIR}/99-melin-ec-bench.cfg"
# What update-grub generates, and therefore what the next boot reads.
GRUB_CFG="/boot/grub/grub.cfg"
# Dropped on a reboot, which is exactly the lifetime we want: its presence
# means "staged but not yet live". Overridable for tests.
REBOOT_FLAG="${REBOOT_FLAG:-/tmp/.server-needs-reboot}"

# Decide how many cores to isolate, given lscpu's distinct-physical-core
# count and `nproc --all`. Kept free of the commands that produce those
# numbers so scripts/bench/test-server-setup.sh can drive every branch.
#
# The second argument must come from `nproc --all`, never plain `nproc`.
# isolcpus confines a process's default affinity to the housekeeping core,
# so on a host this script has already tuned, plain `nproc` reports 1 —
# measured on a 12-core box booted with isolcpus=1-11: `nproc` said 1,
# `nproc --all` said 24. Falling back to 1 would set LAST_ISOLATED=0 and
# emit `isolcpus=nohz,domain,1-0`, a reversed range the kernel cannot act
# on, which the verification below would then wave through: the malformed
# token does reach grub.cfg and /proc/cmdline verbatim.
resolve_physical_cores() {
    local lscpu_count="$1" nproc_all="$2" count="$1"
    # `wc -l` always prints a number, so a failed lscpu arrives as 0 rather
    # than an empty string — one numeric test covers both.
    if [[ "$count" -lt 2 ]]; then
        count="$nproc_all"
        echo "  warning: lscpu core detection failed, falling back to nproc --all=${count}" >&2
    fi
    # Refuse rather than emit a range that cannot mean anything. Every
    # range here is 1..N-1, so at N=1 that is "1-0" — not a narrower
    # isolation but a malformed one, and isolating nothing is the correct
    # reading of a single-core host anyway.
    if [[ "$count" -lt 2 ]]; then
        echo "error: detected only ${count} usable core(s) (lscpu: ${lscpu_count}," >&2
        echo "  nproc --all: ${nproc_all}). Core isolation needs at least two —" >&2
        echo "  core 0 for the OS and IRQs, and one or more for pinned threads." >&2
        echo "  Refusing to write a malformed isolcpus range." >&2
        return 1
    fi
    printf '%s' "$count"
}

# `lscpu -p=CORE` lists one row per logical CPU with its physical core ID;
# sort -u collapses SMT siblings. nosmt is set below in BENCH_PARAMS, and
# this count is stable across that reboot: lscpu lists only online CPUs, so
# it sees N cores whether SMT is on (2N rows, N distinct) or off (N rows,
# N distinct). Verified on a 12-core/24-thread host across a reboot.
LSCPU_CORES=$(lscpu -p=CORE 2>/dev/null | grep -v '^#' | sort -un | wc -l)
NPROC_ALL=$(nproc --all 2>/dev/null || echo 16)
if ! PHYSICAL_CORES=$(resolve_physical_cores "$LSCPU_CORES" "$NPROC_ALL"); then
    echo "  Setup stops here: kernel params, sysctl tuning, IRQ pinning, the CPU" >&2
    echo "  governor, hugepages, vfio-pci and the journal disk have NOT been" >&2
    echo "  configured." >&2
    exit 1
fi
LAST_ISOLATED=$((PHYSICAL_CORES - 1))
ISOLATED_RANGE="1-${LAST_ISOLATED}"
echo "  detected $PHYSICAL_CORES physical cores → isolating ${ISOLATED_RANGE}"

# Every kernel parameter this script manages, one array element each so
# the verification pass below can check them independently rather than
# trusting that a single string edit landed.
KERNEL_PARAMS=(
    "isolcpus=nohz,domain,${ISOLATED_RANGE}"
    "nohz_full=${ISOLATED_RANGE}"
    "rcu_nocbs=${ISOLATED_RANGE}"
    "nowatchdog"
    "transparent_hugepage=never"
    "cpufreq.default_governor=performance"
    "processor.max_cstate=1"
    "skew_tick=1"
    "nosmt"
    "tsc=nowatchdog"
)
# IOMMU for DPDK/vfio-pci. iommu=pt sets passthrough mode so DMA
# bypasses IOMMU translation for performance. intel_iommu=on is
# Intel-specific; on AMD (EPYC, Ryzen) the kernel uses AMD-Vi
# automatically when iommu=pt is set.
if grep -qi "AuthenticAMD" /proc/cpuinfo 2>/dev/null; then
    KERNEL_PARAMS+=("iommu=pt")
else
    KERNEL_PARAMS+=("intel_iommu=on" "iommu=pt")
fi
BENCH_PARAMS="${KERNEL_PARAMS[*]}"

# Is `param` present on the running kernel's command line? Exact token
# match against a split /proc/cmdline — a substring test would accept
# `nosmt` inside an unrelated value, and miss nothing we care about.
# `CMDLINE_SOURCE` is a seam for scripts/bench/test-server-setup.sh; in a real
# run it is always /proc/cmdline.
kernel_param_live() {
    local needle="$1" item
    local -a tokens
    # `read -ra` splits on IFS without the quoting hazards of an unquoted
    # command substitution. /proc/cmdline is a single line, so one read
    # takes the whole thing.
    read -ra tokens < "${CMDLINE_SOURCE:-/proc/cmdline}"
    for item in "${tokens[@]}"; do
        [[ "$item" == "$needle" ]] && return 0
    done
    return 1
}

# Is `param` on a kernel line of the generated grub config? Tokenises the
# `linux` lines and compares whole words, for the same reason the live
# check does: a substring test would let `nosmtfoo` satisfy `nosmt`, and
# report a host as tuned when it is not.
kernel_param_in_cfg() {
    awk -v want="$1" '
        /^[[:space:]]*linux(16|efi)?[[:space:]]/ {
            for (i = 2; i <= NF; i++) if ($i == want) found = 1
        }
        END { exit !found }
    ' "$GRUB_CFG" 2>/dev/null
}

# List the distinct values a `key=` parameter takes on the generated
# kernel lines. The awk `seen[]` map does the deduplicating; the trailing
# `sort` is only for stable ordering, since awk's `for (v in ...)` makes no
# promises about it and these values end up in an error message.
#
# More than one value means the same knob is set twice with
# different settings, and which one wins is not something to leave to
# chance. The case that produces it: a host provisioned by the older
# script, whose params were baked into /etc/default/grub directly, now
# also getting them from the drop-in — e.g. a stale `isolcpus=...,1-10`
# from a previous host size alongside this run's `1-5`.
kernel_param_values_in_cfg() {
    awk -v key="$1" '
        /^[[:space:]]*linux(16|efi)?[[:space:]]/ {
            for (i = 2; i <= NF; i++) {
                if (index($i, key) == 1) seen[$i] = 1
            }
        }
        END { for (v in seen) print v }
    ' "$GRUB_CFG" 2>/dev/null | sort
}

# Write the drop-in carrying every managed parameter, at `target`.
#
# Regenerated wholesale on each run rather than edited in place, which is
# what makes the step idempotent: there is nothing to detect, re-detect, or
# apply twice, and a changed core count simply rewrites the file.
#
# It appends to GRUB_CMDLINE_LINUX rather than GRUB_CMDLINE_LINUX_DEFAULT
# because only the former is guaranteed to exist — the latitude.sh Debian 13
# image ships GRUB_CMDLINE_LINUX alone, and the old `sed` targeting
# `_DEFAULT` silently matched nothing there. Debian's grub-mkconfig puts
# GRUB_CMDLINE_LINUX on both the normal and the recovery entry, so this
# also covers a recovery boot.
#
# The `\${GRUB_CMDLINE_LINUX}` escape is load-bearing and easy to lose: it
# must reach the file literally, so that grub-mkconfig expands it when it
# sources the drop-in and our params are *appended* to the vendor's. Expand
# it here instead and the generated line replaces the vendor's command line
# with ours, discarding whatever it held — on hosting images, typically the
# serial console settings that are the only way back into a remote box.
# scripts/bench/test-server-setup.sh covers this.
write_grub_dropin() {
    local target="$1"
    mkdir -p "$(dirname "$target")"
    cat > "$target" << EOF
# Melin benchmark tuning — generated by server-setup.sh.
# Do not edit: this file is overwritten on every setup run. Change the
# KERNEL_PARAMS array in that script instead. For tuning of your own, use
# a separate drop-in.
GRUB_CMDLINE_LINUX="\${GRUB_CMDLINE_LINUX} ${BENCH_PARAMS}"
EOF
}

# Report which managed params are missing from a generated grub.cfg and
# from the running kernel, then classify the outcome. This exists because
# the previous implementation could not tell "wrote the params" from
# "wrote nothing": it `sed`-ed a line that some vendor images don't have,
# ran update-grub over the unchanged file, and still announced success.
#
# The check is one-directional: it asks whether everything in KERNEL_PARAMS
# is present, never whether something present is unwanted. Drop a parameter
# from the array and the running kernel keeps it until the next reboot while
# this still reports all-clear — which is the honest answer, since the
# drop-in no longer sets it.
verify_kernel_params() {
    local missing_cfg=() missing_live=() p key values

    # A missing config gets its own message. Without this the per-param
    # checks below all read false and the run fails with "the parameters
    # did not reach $GRUB_CFG", sending the operator after a GRUB problem
    # that does not exist when the real answer is that this host writes
    # its generated config somewhere else.
    if [[ ! -f "$GRUB_CFG" ]]; then
        echo "error: $GRUB_CFG does not exist, so the parameters cannot be" >&2
        echo "  verified. update-grub on this host writes its generated config" >&2
        echo "  elsewhere (grub2/ on RHEL-family images, for one). Point GRUB_CFG" >&2
        echo "  at the right path before trusting any tuning on this box." >&2
        return 1
    fi

    # Conflicting duplicates first: a param that is present twice would
    # otherwise pass the "did it land" check below while leaving the
    # effective setting ambiguous.
    for p in "${KERNEL_PARAMS[@]}"; do
        [[ "$p" == *=* ]] || continue
        key="${p%%=*}="
        values=$(kernel_param_values_in_cfg "$key")
        if [[ $(wc -l <<< "$values") -gt 1 ]]; then
            echo "error: '${key%=}' is set more than once in $GRUB_CFG:" >&2
            # Unquoted on purpose: $values is newline-separated and kernel
            # parameters cannot contain whitespace, so this splits into one
            # argument per value.
            # shellcheck disable=SC2086
            printf '         %s\n' $values >&2
            echo "  This host was probably provisioned by an older version of this" >&2
            echo "  script, which wrote the parameters into $GRUB_FILE directly." >&2
            echo "  Remove them from that file's GRUB_CMDLINE_LINUX* lines and re-run;" >&2
            echo "  the drop-in at $GRUB_DROPIN is now the only place they belong." >&2
            return 1
        fi
    done

    for p in "${KERNEL_PARAMS[@]}"; do
        kernel_param_in_cfg "$p" || missing_cfg+=("$p")
        kernel_param_live "$p" || missing_live+=("$p")
    done

    if [[ "${#missing_cfg[@]}" -gt 0 ]]; then
        echo "error: these parameters did not reach $GRUB_CFG:" >&2
        printf '         %s\n' "${missing_cfg[@]}" >&2
        echo "  The drop-in at $GRUB_DROPIN was written and update-grub ran," >&2
        echo "  but the generated config does not carry the parameters. Do not" >&2
        echo "  benchmark this host: it will boot untuned while looking configured." >&2
        return 1
    fi

    if [[ "${#missing_live[@]}" -gt 0 ]]; then
        touch "$REBOOT_FLAG"
        echo "  In $GRUB_CFG but not yet on the running kernel:"
        printf '    %s\n' "${missing_live[@]}"
        echo "  *** REBOOT REQUIRED for these to take effect ***"
    else
        # Clear a flag left by an earlier run: server-deploy.sh only
        # removes it on the path where it does the reboot itself, so a
        # host rebooted by hand keeps it wherever /tmp is disk-backed,
        # and the closing summary would then contradict this line.
        rm -f "$REBOOT_FLAG"
        echo "  All ${#KERNEL_PARAMS[@]} parameters are active on the running kernel."
    fi
    return 0
}

if [[ ! -f "$GRUB_FILE" ]]; then
    echo "=== No GRUB config found, skipping kernel boot params ==="
elif [[ -z "$GRUB_MKCONFIG" ]] || ! grep -q "default/grub\.d" "$GRUB_MKCONFIG"; then
    # Bail loudly rather than fall back to editing the vendor file. The
    # fallback is what produced the silent no-op this rewrite fixes.
    if [[ -z "$GRUB_MKCONFIG" ]]; then
        echo "error: $GRUB_FILE exists but grub-mkconfig is not installed," >&2
        echo "  so there is no way to regenerate the boot config." >&2
    else
        echo "error: this host's grub-mkconfig ($GRUB_MKCONFIG) does not source" >&2
        echo "  /etc/default/grub.d, so the drop-in would be ignored." >&2
    fi
    echo "  Kernel tuning cannot be applied safely here — configure" >&2
    echo "  $GRUB_FILE by hand." >&2
    echo "  Setup stops here: sysctl tuning, IRQ pinning, the CPU governor," >&2
    echo "  hugepages, vfio-pci and the journal disk have NOT been configured." >&2
    exit 1
else
    echo "=== Configuring kernel boot parameters ==="
    write_grub_dropin "$GRUB_DROPIN"
    echo "  Wrote $GRUB_DROPIN"
    echo "  Params: $BENCH_PARAMS"

    update-grub

    # Explicit rather than leaning on `set -e`: a host whose params did
    # not land must stop the run, not continue into the rest of setup
    # looking healthy.
    if ! verify_kernel_params; then
        echo "  Setup stops here: sysctl tuning, IRQ pinning, the CPU governor," >&2
        echo "  hugepages, vfio-pci and the journal disk have NOT been configured." >&2
        exit 1
    fi
fi

echo ""

# ---------------------------------------------------------------------------
# 3a. Disable noisy background services
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
# 3b. Pin device IRQs to core 0
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
# 3c. Force the performance CPU governor
# ---------------------------------------------------------------------------
# `cpufreq.default_governor=performance` is on the kernel cmdline, but that
# only applies to policies created after the parameter is parsed — and it
# does nothing at all on a host that has not yet rebooted into the tuned
# cmdline. Set the governor directly as well, so a box is correct either
# way and the two mechanisms cover each other.
echo "=== Forcing the performance CPU governor ==="
cat > /usr/local/sbin/melin-cpu-governor << 'EOF'
#!/usr/bin/env bash
# Pin every CPU to the performance governor (Melin bench tuning).
set -u
applied=0
skipped=0
for gov in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
    if [ -w "$gov" ] && printf 'performance' > "$gov" 2>/dev/null; then
        applied=$((applied + 1))
    else
        skipped=$((skipped + 1))
    fi
done
# amd-pstate-epp and intel_pstate in active mode take a second hint beyond
# the governor: the energy/performance preference. Selecting the
# performance governor normally drives EPP to `performance` too, so this
# is belt-and-braces for a driver that decouples them. Best-effort by
# design — not every driver exposes or accepts a write here, and the
# governor above is the setting that actually decides frequency.
for epp in /sys/devices/system/cpu/cpu*/cpufreq/energy_performance_preference; do
    [ -w "$epp" ] && printf 'performance' > "$epp" 2>/dev/null
done
# Best-effort: logger exits non-zero when it cannot reach the syslog
# socket, and it is the last command here, so its status would become the
# script's — failing the systemd unit at every boot and, because
# server-setup.sh runs this directly under `set -e`, aborting a
# provisioning run that had already applied the governor successfully.
logger -t melin-cpu-governor "applied=${applied} skipped=${skipped}" || true
EOF
chmod +x /usr/local/sbin/melin-cpu-governor

cat > /etc/systemd/system/melin-cpu-governor.service << 'EOF'
[Unit]
Description=Force the performance CPU governor (Melin bench tuning)
After=multi-user.target
# Debian's ondemand.service sets a governor at boot on some releases.
# Order after it so we are the last writer if it is ever present.
After=ondemand.service

[Service]
Type=oneshot
ExecStart=/usr/local/sbin/melin-cpu-governor
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload
systemctl enable melin-cpu-governor.service
# Apply now too, so a reboot isn't needed — but through systemd rather than
# by running the binary directly. With RemainAfterExit=yes, `start` leaves
# the unit `active (exited)`, which is the same state it reaches on every
# subsequent boot. Invoking the script by hand sets the governor just as
# well but leaves the unit `inactive (dead)`, so an operator who checks
# `systemctl status melin-cpu-governor` straight after provisioning sees
# what looks like a unit that failed to run.
systemctl start melin-cpu-governor.service
GOVERNOR_NOW=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo "unavailable")
echo "  Governor service installed and applied (cpu0 now: ${GOVERNOR_NOW})"
if [[ "$GOVERNOR_NOW" != "performance" && "$GOVERNOR_NOW" != "unavailable" ]]; then
    echo "  warning: cpu0 governor is '${GOVERNOR_NOW}', not 'performance' — frequency"
    echo "           scaling transitions will show up in the latency tail."
fi

echo ""

# ---------------------------------------------------------------------------
# 3d. Sysctl tuning (persistent via /etc/sysctl.d/)
# ---------------------------------------------------------------------------
echo "=== Configuring sysctl ==="
SYSCTL_FILE="/etc/sysctl.d/99-melin-ec-bench.conf"
cat > "$SYSCTL_FILE" << 'EOF'
# Melin — latency tuning.
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
# Per-CPU vmstat counters are folded into the global counters by a deferred
# work item that fires every second by default. `nohz_full` suppresses the
# tick on isolated cores but does not stop this vmstat updater, so it stays
# a periodic jitter source on pipeline cores. 120s makes it effectively
# invisible; the cost is that /proc/vmstat and the page-allocator's
# watermark heuristics see staler per-CPU deltas, which matters for
# monitoring accuracy, not for correctness.
vm.stat_interval = 120
EOF
# Appended separately from the block above because the value is derived from
# this host's RAM, so the heredoc has to interpolate.
MEM_TOTAL_KB=$(awk '/^MemTotal:/ {print $2}' /proc/meminfo 2>/dev/null || echo 0)
MIN_FREE_KB=8388608
if (( MEM_TOTAL_KB == 0 )); then
    echo "  warning: could not read MemTotal from /proc/meminfo; applying the"
    echo "           unclamped ${MIN_FREE_KB} kB reserve. Check it against this"
    echo "           host's RAM before trusting the run."
elif (( MEM_TOTAL_KB / 8 < MIN_FREE_KB )); then
    MIN_FREE_KB=$(( MEM_TOTAL_KB / 8 ))
    echo "  note: clamping vm.min_free_kbytes to ${MIN_FREE_KB} kB (1/8 of $(( MEM_TOTAL_KB / 1024 )) MiB RAM)"
fi
cat >> "$SYSCTL_FILE" << EOF
# The watermark kswapd works to keep free. Set too low, an allocation on a
# hot thread can fall into *direct* reclaim, where the allocating thread
# itself synchronously scans and frees pages — a multi-millisecond stall
# landing wherever the allocation happened, which on a pipeline core is a
# p99.99 event. A large reserve keeps kswapd far enough ahead that the hot
# path never reaches that fallback.
#
# 8 GiB is the ceiling, clamped to 1/8 of RAM: on a small host an
# unconditional 8 GiB reserve is a large fraction of memory, and the
# allocator will OOM rather than dip below the watermark. Value below is for
# a host with $(( MEM_TOTAL_KB / 1024 )) MiB of RAM.
vm.min_free_kbytes = ${MIN_FREE_KB}
EOF
# Raise the system-wide max file descriptor limit. The default (1024) is
# too low for client-sweep benchmarks: 512 clients × 2 fds (stream +
# clone) + server-side accept fds + journal/io_uring fds ≈ 1500+.
LIMITS_FILE="/etc/security/limits.d/99-melin-ec-bench.conf"
cat > "$LIMITS_FILE" << 'EOF'
# Melin benchmark — raise fd limits for high client counts.
*    soft nofile 65536
*    hard nofile 65536
root soft nofile 65536
root hard nofile 65536
EOF
echo "  Written $LIMITS_FILE (nofile=65536)"
sysctl --system --quiet
echo "  Written $SYSCTL_FILE (vm.swappiness=0, kernel.numa_balancing=0, kernel.watchdog=0, net.core.rmem_max=32MiB, net.core.netdev_max_backlog=10000, vm.stat_interval=120, vm.min_free_kbytes=${MIN_FREE_KB})"

echo ""

# ---------------------------------------------------------------------------
# 3e. Hugepages for DPDK
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
# 3f. vfio-pci module (DPDK NIC binding)
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
# Pick the journal disk. Order of preference:
#   1. JOURNAL_DISK=/dev/nvmeXn1 in the environment — explicit operator pick,
#      used for benchmarking specific drives or any host where the auto-pick
#      doesn't match the operator's intent. Validated for block-device-ness
#      before we use it.
#   2. Largest unpartitioned NVMe whole-disk device. Production sizing intent
#      is "biggest data drive available" — more NAND = more endurance budget
#      and capacity headroom. "First unpartitioned" (the previous heuristic)
#      was enumeration-order dependent and silently grabbed whatever the BIOS
#      enumerated first, which on hosts with mixed M.2 + U.3 drives often
#      picked the smaller M.2 by accident.
# If nothing matches, JOURNAL_DISK is left empty and the conditional below
# skips journal setup with a clear log line — same behaviour as before.
if [[ -n "${JOURNAL_DISK:-}" ]]; then
    if [[ ! -b "${JOURNAL_DISK}" ]]; then
        echo "error: JOURNAL_DISK=${JOURNAL_DISK} is not a block device" >&2
        exit 1
    fi
    echo "  Journal disk: ${JOURNAL_DISK} (from JOURNAL_DISK env var)"
else
    # Walk the NVMe whole-disks and keep the largest unpartitioned one.
    # `blockdev --getsize64` returns bytes; we compare numerically. No
    # subshell pipeline — keeps state in this shell so we can also report
    # what we considered vs. picked if anyone adds debug logging later.
    # Initialise to empty so `set -u` is happy when no NVMe matches —
    # the existing "if [[ -n "$JOURNAL_DISK" ]]" below depends on it.
    JOURNAL_DISK=""
    BEST_SIZE=0
    for dev in /dev/nvme*n1; do
        [[ -e "$dev" ]] || continue
        # Skip if it has partitions (OS disk).
        ls "${dev}p"* &>/dev/null && continue
        size=$(blockdev --getsize64 "$dev" 2>/dev/null || echo 0)
        if (( size > BEST_SIZE )); then
            BEST_SIZE=$size
            JOURNAL_DISK=$dev
        fi
    done
fi

# Safety net for re-runs: if $JOURNAL_MOUNT is already mounted from a
# *different* block device than the one we just selected, refuse rather
# than silently formatting the new pick and leaving it unmounted while
# the old disk keeps serving the journal. The most common way to hit
# this is a host that was provisioned with the older "first
# unpartitioned NVMe" heuristic and is now being re-deployed — the new
# size-ranked picker chooses a different drive. Operators get an
# explicit choice: pin JOURNAL_DISK to the existing mount to keep it,
# or unmount + re-run to migrate.
if [[ -n "${JOURNAL_DISK:-}" ]] && mountpoint -q "$JOURNAL_MOUNT"; then
    current=$(findmnt -n -o SOURCE "$JOURNAL_MOUNT" || true)
    if [[ -n "$current" && "$current" != "$JOURNAL_DISK" ]]; then
        echo "error: $JOURNAL_MOUNT is mounted from $current but the setup picked $JOURNAL_DISK" >&2
        echo "  This typically means a previous setup chose a different disk." >&2
        echo "  Either:" >&2
        echo "    - Re-run with JOURNAL_DISK=$current to keep the existing mount, or" >&2
        echo "    - Stop the server, 'umount $JOURNAL_MOUNT', then re-run to migrate." >&2
        exit 1
    fi
fi

if [[ -n "$JOURNAL_DISK" ]]; then
    echo "=== Journal disk: $JOURNAL_DISK → $JOURNAL_MOUNT ==="

    # Filesystem: xfs. We previously used ext4 with `data=writeback,
    # journal_async_commit, commit=300` and observed periodic 1–2 ms
    # `fdatasync` spikes at ~256 MiB write boundaries, correlated across
    # all replicas (deterministic event stream → identical byte layout →
    # ext4's jbd2 metadata batching fires at the same offset on every
    # node). The `disk+ram` ack gate masks single-node hiccups, but
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
# Done
# ---------------------------------------------------------------------------
echo "=== Setup complete ==="
echo ""
echo "Next steps:"
if [[ -f "$REBOOT_FLAG" ]]; then
    echo "  1. REBOOT — kernel params are staged but not yet active (see above)"
else
    echo "  1. No reboot needed — every kernel param is already active"
fi
echo "  2. Verify after reboot:"
echo "     cat /sys/devices/system/cpu/isolated   # should show: ${ISOLATED_RANGE}"
echo "     cat /sys/devices/system/cpu/nohz_full  # should show: ${ISOLATED_RANGE}"
echo "     cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor  # performance"
echo "     Re-running this script is the fastest full check: it re-verifies"
echo "     every managed param against /proc/cmdline and fails loudly on a gap."
echo ""
echo "  3. Run the LAN suite from your local machine:"
echo "     ./scripts/bench/lan-bench-suite.sh <app> <server-pub-ip> <bench-pub-ip> <server-vlan-ip>"
echo ""
