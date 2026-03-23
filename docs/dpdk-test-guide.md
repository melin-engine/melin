# DPDK Test Guide — Cherry Servers

Step-by-step procedure for testing the DPDK kernel-bypass transport on Cherry Servers with Intel X710 SR-IOV.

## Machines

Two Cherry Servers on the same VLAN. IPs change per rental — check the Cherry dashboard for public IPs and `ip addr show bond0.*` for VLAN IPs.

Throughout this guide:
- `SERVER` = server public IP
- `BENCH` = bench public IP
- `SERVER_VLAN` = server VLAN IP (from `dpdk-setup-sriov.sh` output or `/etc/melin-dpdk.conf`)

## 1. Setup (run once after reboot)

SR-IOV VF creation, driver binding, hugepages, and MTU are all runtime state — lost on reboot.

**On the server:**
```sh
ssh root@SERVER
cd ~/workspace/trading
sudo ./scripts/dpdk-setup-sriov.sh          # creates VFs, binds to vfio-pci, sets MTU 9000
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

## 2. Build

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
cargo build --release -p melin-bench

# OR DPDK bench (requires dpdk-setup-sriov.sh):
cargo build --release -p melin-bench --features dpdk --no-default-features
```

## 3. Auth keys

Generate on the bench machine, copy to the server:
```sh
# On bench:
cd ~/workspace/trading
cargo run --release --bin melin-keygen -- bench admin
# Creates bench.key (private) and bench.pub (public)

# Copy public key to server's authorized_keys:
scp bench.pub root@SERVER:~/workspace/trading/
ssh root@SERVER "cd ~/workspace/trading && echo 'admin $(cat bench.pub) bench' > authorized_keys"
```

Or if keys already exist, just sync:
```sh
scp root@SERVER:~/workspace/trading/authorized_keys ./
scp root@SERVER:~/workspace/trading/bench.key ./
```

## 4. Start the server

```sh
ssh root@SERVER
cd ~/workspace/trading

# Clean old journal
rm -f /mnt/journal/bench.journal*

# Start with dual-port polling + jumbo frames
RUST_LOG=info ./target/release/melin-server \
    --bind 0.0.0.0:9876 \
    --journal /mnt/journal/bench.journal \
    --authorized-keys authorized_keys \
    --standalone \
    --dpdk-eal-args='--huge-dir=/mnt/huge_2m' \
    --dpdk-ports 0,1 \
    --dpdk-ip SERVER_VLAN \
    --dpdk-prefix-len 24 \
    --dpdk-mtu 9000
```

Look for these log lines to confirm startup:
```
DPDK transport initialized
DPDK port configured
DPDK port started
```

## 5. Run the benchmark

### Kernel TCP bench client (simplest)
```sh
ssh root@BENCH
cd ~/workspace/trading

./target/release/melin-bench \
    --addr SERVER_VLAN:9876 \
    --key bench.key \
    --clients 1 \
    --window 256 \
    10000000
```

### DPDK bench client (both sides bypass kernel)
```sh
./target/release/melin-bench \
    --addr SERVER_VLAN:9876 \
    --key bench.key \
    --clients 1 \
    --window 256 \
    --dpdk-eal-args='--huge-dir=/mnt/huge_2m' \
    --dpdk-ports 0,1 \
    --dpdk-ip BENCH_VLAN \
    --dpdk-prefix-len 24 \
    --dpdk-mtu 9000 \
    10000000
```

## 6. Troubleshooting

### "connection refused" or auth failure
- Check `authorized_keys` on server matches `bench.pub` on bench
- Check `bench.key` exists on bench machine

### Intermittent connection failures
- Use `--dpdk-ports 0,1` (not `--dpdk-ports 0`) — LACP bonds hash
  traffic across both PFs, single-port polling misses ~50%

### No packets received
- Verify VFs are bound: `ls /sys/bus/pci/drivers/vfio-pci/ | grep 0000`
- Check hugepages: `grep -i huge /proc/meminfo`
- Check server log: `RUST_LOG=debug` for per-packet logging

### Jumbo frame issues
- Both sides must use the same `--dpdk-mtu`
- PF MTU must be >= frame MTU: `ip link show` on bond members
- If switch doesn't support jumbo, use `--dpdk-mtu 1500` and
  `./scripts/dpdk-setup-sriov.sh --mtu 1500`

### Server crashes on startup
- Check IOMMU: `dmesg | grep -i iommu`
- Check VF exists: `lspci | grep -i virtual`
- Clean stale DPDK state: `rm -rf /var/run/dpdk/rte`

## 7. What to compare

Run the same workload with kernel TCP to get a baseline:
```sh
# On server (no DPDK):
./target/release/melin-server \
    --bind SERVER_VLAN:9876 \
    --journal /mnt/journal/bench.journal \
    --authorized-keys authorized_keys \
    --standalone

# On bench:
./target/release/melin-bench \
    --addr SERVER_VLAN:9876 \
    --key bench.key \
    --clients 1 --window 256 \
    10000000
```
