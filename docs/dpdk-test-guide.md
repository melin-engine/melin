# DPDK Test Guide

Step-by-step procedure for testing the DPDK kernel-bypass transport.

## Local Testing (no servers needed)

### Smoke tests

```sh
# Server-side DPDK only (kernel TCP bench client, TAP device):
sudo ./scripts/dpdk-smoke-test.sh

# Both sides DPDK (veth pair + af_packet):
sudo ./scripts/dpdk-e2e-smoke-test.sh

# Kernel TCP baseline (no DPDK, no root):
./scripts/smoke-test.sh
```

### Environment verification

Use `dpdk-testpmd` to verify DPDK works independently of the application:

```sh
# Quick port check:
sudo ./scripts/dpdk-test.sh

# ICMP echo (test L2/L3 connectivity from bench machine):
sudo ./scripts/dpdk-test.sh icmpecho

# Interactive forward mode (check LACP both-port RX):
sudo ./scripts/dpdk-test.sh forward
```

## Remote Testing — Cherry Servers

Two Cherry Servers on the same VLAN. IPs change per rental — check the Cherry dashboard for public IPs and `ip addr show bond0.*` for VLAN IPs.

Supported NICs:
- Intel E810 (ice driver)
- Intel 82599 / X520 / X540 (ixgbe driver)

Throughout this guide:
- `SERVER` = server public IP
- `BENCH` = bench public IP

### 1. Setup (run once after reboot)

SR-IOV VF creation, driver binding, hugepages, and MTU are all runtime state — lost on reboot.

**On the server:**
```sh
ssh root@SERVER
cd ~/workspace/trading
sudo ./scripts/dpdk-setup-sriov.sh
```

**On the bench machine** (if using DPDK bench client):
```sh
ssh root@BENCH
cd ~/workspace/trading
sudo ./scripts/dpdk-setup-sriov.sh
```

Verify with:
```sh
cat /etc/melin-dpdk.conf               # auto-detected DPDK IP, port, MTU
ls /sys/bus/pci/drivers/vfio-pci/ | grep 0000   # VFs bound
grep -i huge /proc/meminfo              # hugepages allocated
```

### 2. Build

**On the server:**
```sh
cd ~/workspace/trading
git pull
cargo build --release -p melin-server --features dpdk --no-default-features
```

**On the bench machine:**
```sh
cd ~/workspace/trading
git pull
# Kernel TCP bench (no DPDK needed):
cargo build --release --bin melin-bench

# OR DPDK bench (requires dpdk-setup-sriov.sh):
cargo build --release -p melin-bench --features dpdk --no-default-features
```

### 3. Auth keys

Generate on the bench machine, copy to the server:
```sh
# On bench:
cd ~/workspace/trading
cargo run --release --bin melin-keygen -- bench trader
# Creates bench.key (private) and bench.pub (public)

# Copy public key to server's authorized_keys:
scp bench.pub root@SERVER:~/workspace/trading/
ssh root@SERVER "cd ~/workspace/trading && echo 'trader $(cat bench.pub) bench' > authorized_keys"
```

### 4. Start the server

```sh
ssh root@SERVER
cd ~/workspace/trading
rm -f /mnt/journal/bench.journal*
sudo ./scripts/dpdk-server.sh
```

The script auto-detects DPDK IP from `/etc/melin-dpdk.conf`, uses both VF ports for LACP, and falls back to TAP mode if no SR-IOV is available.

Look for these log lines to confirm startup:
```
DPDK transport initialized
DPDK port configured
DPDK port started
DPDK transport listening
```

### 5. Run the benchmark

**Kernel TCP bench client (simplest):**
```sh
ssh root@BENCH
cd ~/workspace/trading
DPDK_IP=$(ssh root@SERVER "grep DPDK_IP /etc/melin-dpdk.conf | cut -d= -f2")

cargo run --release --bin melin-bench -- \
    --addr $DPDK_IP:9876 \
    --key bench.key \
    --clients 1 \
    --window 256 \
    10000000
```

**DPDK bench client (both sides bypass kernel):**
```sh
DPDK_IP=$(ssh root@SERVER "grep DPDK_IP /etc/melin-dpdk.conf | cut -d= -f2")
source /etc/melin-dpdk.conf  # loads local DPDK_IP, HUGE_DIR, etc.

cargo run --release --bin melin-bench --features dpdk --no-default-features -- \
    --addr $DPDK_IP:9876 \
    --key bench.key \
    --clients 1 \
    --window 256 \
    --dpdk-eal-args="--huge-dir=$HUGE_DIR" \
    --dpdk-ports 0,1 \
    --dpdk-ip $DPDK_IP \
    --dpdk-prefix-len 24 \
    --dpdk-core 7 \
    10000000
```

### 6. Troubleshooting

#### Connection hangs or intermittent failures
- Use `--dpdk-ports 0,1` (not `--dpdk-ports 0`) — LACP bonds hash
  traffic across both PFs, single-port polling misses ~50%
- Check `authorized_keys` uses `trader` permission (not `admin` or `operator`)
- First connection after server restart may take a moment (ARP learning)

#### No packets received
- Verify VFs are bound: `ls /sys/bus/pci/drivers/vfio-pci/ | grep 0000`
- Check hugepages: `grep -i huge /proc/meminfo`
- Use `sudo ./scripts/dpdk-test.sh` to verify DPDK environment independently
- Check server log with `RUST_LOG=debug` for per-packet logging

#### Server crashes on startup
- Check IOMMU: `dmesg | grep -i iommu`
- Ensure `intel_iommu=on iommu=pt` in kernel cmdline (cherry-setup.sh adds this)
- Check VF exists: `lspci | grep -i virtual`
- Clean stale DPDK state: `rm -rf /var/run/dpdk/rte`

#### Server segfaults on exit
- This was a drop-order bug (EAL cleaned up before mempool). Fixed in commit `7b44832`.
- If using an older build, pull and rebuild.

#### Permission errors (0 orders processed)
- `trader` permission is required for trading orders
- `operator` can only do admin commands, not trade
- Check: `grep trader authorized_keys` on the server

### 7. What to compare

Run the same workload with kernel TCP to get a baseline:
```sh
# On server (no DPDK, default features):
cargo run --release --bin melin-server -- \
    --bind 0.0.0.0:9876 \
    --journal /mnt/journal/bench.journal \
    --authorized-keys authorized_keys \
    --standalone

# On bench:
cargo run --release --bin melin-bench -- \
    --addr SERVER_VLAN:9876 \
    --key bench.key \
    --clients 1 --window 256 \
    10000000
```
