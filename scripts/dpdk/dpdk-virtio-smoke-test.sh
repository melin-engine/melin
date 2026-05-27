#!/usr/bin/env bash
# Smoke test for DPDK with a real virtio-net PCI device inside a QEMU/KVM VM.
#
# Unlike the TAP smoke test (which uses a virtual device), this exercises
# real PCI device binding and the net_virtio PMD — the same code path
# used with physical NICs on bare-metal.
#
# Architecture:
#   Host                          Guest (Debian VM)
#   ────                          ─────
#   melin-bench ──TCP──> vmtap0 ──virtio-net-pci──> DPDK net_virtio PMD
#                        192.168.200.2              192.168.200.1
#                                                   melin-server (DPDK)
#
# The guest has two NICs:
#   - Management: QEMU user-mode (DHCP, SSH via port forward on 2222)
#   - Data plane: virtio-net-pci backed by host TAP, bound to DPDK
#
# Flow:
#   1. Download Debian 12 cloud image (cached for reuse)
#   2. Provision VM via cloud-init (Rust, DPDK packages)
#   3. Launch QEMU/KVM with two virtio NICs
#   4. SSH in: build server, bind NIC, start DPDK server
#   5. Host: run bench through TAP interface
#   6. Tear down
#
# Usage:
#   sudo --preserve-env=SSH_AUTH_SOCK ./scripts/dpdk/dpdk-virtio-smoke-test.sh
#
# Prerequisites:
#   - qemu-system-x86_64 with KVM support (/dev/kvm)
#   - cloud-localds (cloud-image-utils) or genisoimage
#   - Must run as root (TAP interface setup)
#   - Internet access on first run (downloads ~350MB image + packages)

set -euo pipefail

# Ensure cargo/rustup work when running under sudo.
if [[ -n "${SUDO_USER:-}" ]]; then
    REAL_HOME=$(eval echo "~$SUDO_USER")
    export PATH="$REAL_HOME/.cargo/bin:$PATH"
    export RUSTUP_HOME="${RUSTUP_HOME:-$REAL_HOME/.rustup}"
    export CARGO_HOME="${CARGO_HOME:-$REAL_HOME/.cargo}"
fi

# SSH agent forwarding is needed so cargo can fetch git deps inside the VM.
# sudo doesn't preserve SSH_AUTH_SOCK by default — check it's available.
if [[ -z "${SSH_AUTH_SOCK:-}" ]]; then
    echo "error: SSH_AUTH_SOCK not set. Run with: sudo --preserve-env=SSH_AUTH_SOCK $0" >&2
    echo "  (needed to forward your SSH agent into the VM for cargo git deps)" >&2
    exit 1
fi

if [[ $EUID -ne 0 ]]; then
    echo "error: must run as root (TAP interface + QEMU networking)" >&2
    echo "usage: sudo $0" >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
CACHE_DIR="$SCRIPT_DIR/.cache"
TMPDIR=$(mktemp -d)

# Networking.
DPDK_IP="192.168.200.1"
TAP_IP="192.168.200.2"
PREFIX=24
DPDK_PORT=9876
TAP_IFACE="vmtap0"
SSH_PORT=2222

# VM resources.
VM_RAM="8G"
VM_CPUS=4

# Debian 12 (bookworm) generic cloud image.
DEBIAN_IMAGE_URL="https://cloud.debian.org/images/cloud/bookworm/latest/debian-12-generic-amd64.qcow2"
DEBIAN_IMAGE="$CACHE_DIR/debian-12-generic-amd64.qcow2"

cleanup() {
    echo ""
    echo "=== Cleanup ==="

    # Kill QEMU if running.
    if [[ -n "${QEMU_PID:-}" ]]; then
        kill "$QEMU_PID" 2>/dev/null && wait "$QEMU_PID" 2>/dev/null || true
        echo "  VM stopped (PID $QEMU_PID)"
    fi

    # Remove TAP interface.
    ip link del "$TAP_IFACE" 2>/dev/null || true
    echo "  TAP interface removed"

    # Restore target/ ownership.
    if [[ -n "${SUDO_USER:-}" ]]; then
        chown -R "$SUDO_USER:$SUDO_USER" "$PROJECT_DIR/target" 2>/dev/null || true
        echo "  Restored target/ ownership to $SUDO_USER"
    fi

    rm -rf "$TMPDIR"
    echo "  Temp dir cleaned: $TMPDIR"
}
trap cleanup EXIT

# SSH helper — all SSH commands go through this.
# SSH helper — all SSH commands go through this.
# -A forwards the host SSH agent so cargo can fetch git deps from GitHub.
vm_ssh() {
    ssh -A -q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
        -o ConnectTimeout=5 -i "$TMPDIR/ssh_key" \
        -p "$SSH_PORT" melin@localhost "$@"
}

echo "============================================================"
echo "  DPDK Virtio Smoke Test (QEMU/KVM + net_virtio PMD)"
echo "  DPDK IP: $DPDK_IP:$DPDK_PORT (guest, virtio-net)"
echo "  TAP IP:  $TAP_IP (host)"
echo "  SSH:     localhost:$SSH_PORT"
echo "  Temp:    $TMPDIR"
echo "============================================================"
echo ""

# --- 0. Prerequisites ---
echo "=== Checking prerequisites ==="
for cmd in qemu-system-x86_64 qemu-img; do
    if ! command -v "$cmd" &>/dev/null; then
        echo "  ERROR: $cmd not found. Install qemu-system-x86 and qemu-utils." >&2
        exit 1
    fi
done
if [[ ! -e /dev/kvm ]]; then
    echo "  ERROR: /dev/kvm not found. Enable KVM in BIOS and load kvm module." >&2
    exit 1
fi
# We need a tool to create the cloud-init ISO (cidata volume).
if command -v cloud-localds &>/dev/null; then
    CLOUD_INIT_TOOL="cloud-localds"
elif command -v xorrisofs &>/dev/null; then
    CLOUD_INIT_TOOL="xorrisofs"
elif command -v genisoimage &>/dev/null; then
    CLOUD_INIT_TOOL="genisoimage"
else
    echo "  ERROR: cloud-localds, xorrisofs, or genisoimage required." >&2
    echo "  Install: apt install cloud-image-utils (or xorriso or genisoimage)" >&2
    exit 1
fi
echo "  qemu-system-x86_64: OK"
echo "  /dev/kvm: OK"
echo "  cloud-init tool: $CLOUD_INIT_TOOL"
echo ""

# --- 1. Download Debian cloud image ---
echo "=== Debian cloud image ==="
mkdir -p "$CACHE_DIR"
if [[ -f "$DEBIAN_IMAGE" ]]; then
    echo "  Cached: $DEBIAN_IMAGE"
else
    echo "  Downloading Debian 12 cloud image..."
    curl -fL -o "$DEBIAN_IMAGE.tmp" "$DEBIAN_IMAGE_URL"
    mv "$DEBIAN_IMAGE.tmp" "$DEBIAN_IMAGE"
    echo "  Downloaded: $DEBIAN_IMAGE"
fi
echo ""

# --- 2. Generate SSH key + cloud-init ---
echo "=== Cloud-init ==="
ssh-keygen -t ed25519 -f "$TMPDIR/ssh_key" -N "" -q
SSH_PUB=$(cat "$TMPDIR/ssh_key.pub")

cat > "$TMPDIR/user-data" << USERDATA
#cloud-config
hostname: melin-virtio
users:
  - name: melin
    sudo: ALL=(ALL) NOPASSWD:ALL
    shell: /bin/bash
    ssh_authorized_keys:
      - $SSH_PUB
package_update: true
packages:
  - dpdk
  - dpdk-dev
  - libdpdk-dev
  - build-essential
  - pkg-config
  - libclang-dev
  - clang
  - curl
  - rsync
runcmd:
  # Install Rust toolchain.
  - su - melin -c 'curl --proto =https --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable'
  # Signal that provisioning is done.
  - touch /tmp/cloud-init-done
USERDATA

cat > "$TMPDIR/meta-data" << METADATA
instance-id: melin-virtio-001
local-hostname: melin-virtio
METADATA

# Create cloud-init ISO.
CLOUD_INIT_ISO="$TMPDIR/cloud-init.iso"
case "$CLOUD_INIT_TOOL" in
    cloud-localds)
        cloud-localds "$CLOUD_INIT_ISO" "$TMPDIR/user-data" "$TMPDIR/meta-data"
        ;;
    xorrisofs)
        xorrisofs -output "$CLOUD_INIT_ISO" -volid cidata -joliet -rock \
            "$TMPDIR/user-data" "$TMPDIR/meta-data" 2>/dev/null
        ;;
    genisoimage)
        genisoimage -output "$CLOUD_INIT_ISO" -volid cidata -joliet -rock \
            "$TMPDIR/user-data" "$TMPDIR/meta-data" 2>/dev/null
        ;;
esac
echo "  SSH key + cloud-init ISO created"
echo ""

# --- 3. Create COW overlay ---
echo "=== VM disk ==="
VM_DISK="$TMPDIR/vm-disk.qcow2"
qemu-img create -f qcow2 -b "$DEBIAN_IMAGE" -F qcow2 "$VM_DISK" 20G >/dev/null
echo "  COW overlay: $VM_DISK (20G, backed by cached image)"
echo ""

# --- 4. Host TAP interface ---
echo "=== Host TAP interface ==="
ip tuntap add dev "$TAP_IFACE" mode tap
ip addr add "$TAP_IP/$PREFIX" dev "$TAP_IFACE"
ip link set "$TAP_IFACE" up
echo "  $TAP_IFACE: $TAP_IP/$PREFIX (up)"
echo ""

# --- 5. Launch QEMU ---
echo "=== Starting VM ==="
qemu-system-x86_64 \
    -enable-kvm \
    -cpu host \
    -m "$VM_RAM" \
    -smp "$VM_CPUS" \
    -drive file="$VM_DISK",format=qcow2,if=virtio \
    -drive file="$CLOUD_INIT_ISO",format=raw,if=virtio \
    -device virtio-net-pci,netdev=mgmt,mac=52:54:00:00:00:01 \
    -netdev user,id=mgmt,hostfwd=tcp::${SSH_PORT}-:22 \
    -device virtio-net-pci,netdev=data,mac=52:54:00:00:00:02 \
    -netdev tap,id=data,ifname="$TAP_IFACE",script=no,downscript=no \
    -nographic \
    -serial file:"$TMPDIR/vm-console.log" \
    &
QEMU_PID=$!
echo "  QEMU PID: $QEMU_PID"
echo "  Console log: $TMPDIR/vm-console.log"

# Wait for SSH.
echo "  Waiting for SSH..."
WAIT=0
while ! vm_ssh true 2>/dev/null; do
    sleep 2
    WAIT=$((WAIT + 1))
    if [[ $WAIT -gt 90 ]]; then
        echo "  ERROR: SSH not ready after 3 minutes"
        echo "  --- VM console (last 30 lines) ---"
        tail -30 "$TMPDIR/vm-console.log" 2>/dev/null || true
        exit 1
    fi
    if ! kill -0 "$QEMU_PID" 2>/dev/null; then
        echo "  ERROR: QEMU process died"
        echo "  --- VM console ---"
        cat "$TMPDIR/vm-console.log" 2>/dev/null || true
        exit 1
    fi
done
echo "  SSH ready"
echo ""

# --- 6. Wait for cloud-init provisioning ---
echo "=== Provisioning (cloud-init) ==="
echo "  Waiting for packages + Rust toolchain..."
WAIT=0
while ! vm_ssh "test -f /tmp/cloud-init-done" 2>/dev/null; do
    sleep 5
    WAIT=$((WAIT + 1))
    if [[ $WAIT -gt 120 ]]; then
        echo "  ERROR: cloud-init did not finish after 10 minutes"
        vm_ssh "cat /var/log/cloud-init-output.log" 2>/dev/null | tail -50 || true
        exit 1
    fi
done
echo "  Provisioning complete"
echo ""

# --- 7. Copy project source ---
echo "=== Syncing project source ==="
SSH_RSYNC="ssh -q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i $TMPDIR/ssh_key -p $SSH_PORT"

# Add GitHub to known_hosts so cargo git fetches don't prompt.
vm_ssh "mkdir -p ~/.ssh && ssh-keyscan -H github.com >> ~/.ssh/known_hosts 2>/dev/null"

# Rsync the project, excluding target/ and .git/ to keep it fast.
rsync -az --delete \
    -e "$SSH_RSYNC" \
    --exclude target/ --exclude .git/ --exclude '*.journal' \
    "$PROJECT_DIR/" melin@localhost:~/melin/
echo "  Project synced to guest:~/melin/"
echo ""

# --- 8. Build server inside VM ---
echo "=== Building server in VM ==="
echo "  cargo build --release -p melin-server --features dpdk --no-default-features"
echo "  (this may take a few minutes on first run)"
vm_ssh "cd ~/melin && source ~/.cargo/env && cargo build --release -p melin-server --features dpdk --no-default-features" 2>&1 | tail -5
echo "  Server build: OK"

echo "  Building keygen..."
vm_ssh "cd ~/melin && source ~/.cargo/env && cargo build --release --bin melin-keygen" 2>&1 | tail -3
echo "  Keygen build: OK"
echo ""

# --- 9. Set up DPDK inside VM ---
echo "=== Guest DPDK setup ==="

# Allocate hugepages.
vm_ssh "sudo bash -c 'echo 256 > /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages'"
vm_ssh "sudo mkdir -p /mnt/huge_2m && sudo mount -t hugetlbfs -o pagesize=2M nodev /mnt/huge_2m 2>/dev/null || true"
echo "  Hugepages: 256 x 2MB"

# Load uio_pci_generic — simpler than vfio-pci, no IOMMU needed.
vm_ssh "sudo modprobe uio_pci_generic"
echo "  uio_pci_generic: loaded"

# Find the data-plane NIC PCI address (second virtio-net, MAC 52:54:00:00:00:02).
# The sysfs device link resolves to e.g. ../../../virtio1; we need its parent
# PCI device (e.g. 0000:00:04.0) for dpdk-devbind.py.
DATA_NIC=$(vm_ssh "ip -o link | grep '52:54:00:00:00:02' | awk '{print \$2}' | tr -d ':'")
DATA_PCI=$(vm_ssh "basename \$(readlink -f /sys/class/net/$DATA_NIC/device/..)" | grep -oE '[0-9a-f]{4}:[0-9a-f]{2}:[0-9a-f]{2}\.[0-9a-f]')
echo "  Data NIC PCI address: $DATA_PCI"

# Unbind from kernel and bind to uio_pci_generic.
vm_ssh "sudo dpdk-devbind.py -b uio_pci_generic $DATA_PCI"
echo "  Bound $DATA_PCI to uio_pci_generic"
vm_ssh "dpdk-devbind.py --status-dev net" 2>/dev/null | head -10
echo ""

# --- 10. Generate auth keys + start server ---
echo "=== Starting DPDK server in VM ==="
vm_ssh "cd /tmp && ~/melin/target/release/melin-keygen bench trader"
vm_ssh "echo \"trader \$(cat /tmp/bench.pub | tr -d '\n') bench\" > /tmp/authorized_keys"
echo "  Auth keys generated"

vm_ssh "sudo RUST_LOG=info,melin_server=debug,melin_dpdk=debug \
    ~/melin/target/release/melin-server \
    --bind 0.0.0.0:$DPDK_PORT \
    --journal /tmp/smoke.journal \
    --authorized-keys /tmp/authorized_keys \
    --standalone \
    --accounts 100 \
    --instruments 10 \
    --yield-idle \
    --cores 0,0,0,0,0,0,0,0,0 \
    --dpdk-eal-args='--huge-dir=/mnt/huge_2m --log-level=6' \
    --dpdk-ip $DPDK_IP \
    --dpdk-prefix-len $PREFIX \
    > /tmp/server.log 2>&1 &"

# Wait for server to start.
echo "  Waiting for DPDK server..."
WAIT=0
while ! vm_ssh "grep -q 'DPDK transport listening' /tmp/server.log 2>/dev/null"; do
    sleep 1
    WAIT=$((WAIT + 1))
    if [[ $WAIT -gt 30 ]]; then
        echo "  ERROR: Server not ready after 30s"
        echo "  --- Server log ---"
        vm_ssh "cat /tmp/server.log" 2>/dev/null || true
        exit 1
    fi
    # Check if server process is still running.
    if ! vm_ssh "pgrep -f melin-server >/dev/null 2>&1"; then
        echo "  ERROR: Server process died"
        echo "  --- Server log ---"
        vm_ssh "cat /tmp/server.log" 2>/dev/null || true
        exit 1
    fi
done
echo "  DPDK server running (net_virtio PMD)"

# Show the DPDK port info from server log.
vm_ssh "grep -E '(DPDK|port|PMD|virtio)' /tmp/server.log" 2>/dev/null | head -10 || true
echo ""

# --- 11. Build + run bench on host ---
echo "=== Building host bench ==="
cd "$PROJECT_DIR"
cargo build --release --bin melin-bench --bin melin-keygen --quiet 2>&1
echo "  bench + keygen: OK"

# Generate matching auth keys on host.
# We need the same keypair — copy from VM.
vm_ssh "cat /tmp/bench.key" > "$TMPDIR/bench.key"
chmod 600 "$TMPDIR/bench.key"
echo ""

echo "=== Running smoke benchmark ==="
echo "  short timed run, 1 client, window 1 (single-order latency)"

"$PROJECT_DIR/target/release/melin-bench" \
    --addr "$DPDK_IP:$DPDK_PORT" \
    --key "$TMPDIR/bench.key" \
    --clients 1 \
    --window 1 \
    --warmup-duration 1s \
    --duration 3s \
    --cooldown-duration 0s \
    2>&1 | tee "$TMPDIR/bench.log"

BENCH_EXIT=$?

echo ""
if [[ $BENCH_EXIT -eq 0 ]]; then
    echo "============================================================"
    echo "  DPDK VIRTIO SMOKE TEST: PASSED"
    echo "============================================================"
else
    echo "============================================================"
    echo "  DPDK VIRTIO SMOKE TEST: FAILED (bench exit code $BENCH_EXIT)"
    echo "============================================================"
    echo ""
    echo "  --- Server log (last 50 lines) ---"
    vm_ssh "tail -50 /tmp/server.log" 2>/dev/null || true
    exit 1
fi
