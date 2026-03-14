# LAN Benchmark Demo Plan

Demonstrate the matching engine's performance over a real datacenter network, not just loopback. Two rented servers on the same LAN (Hetzner), one running the engine, one running the benchmark client.

## Goals

1. **Prove real-network latency** — measure round-trip order latency across a physical network (expect ~50-100 us added vs loopback from NIC + switch + kernel stack on both sides).
2. **Show durability under load** — run with full `pwritev2 + RWF_DSYNC` persistence, not no-persist mode.
3. **Crash recovery demo** — `kill -9` the engine mid-benchmark, restart, replay journal, verify state is intact. This is the strongest proof of correctness.
4. **Produce publishable numbers** — latency histogram (p99, p99.9, max) and throughput at various client counts.

## What We Are NOT Testing

- Market data dissemination (not implemented, not relevant to latency demo)
- Multiple instruments (single-instrument stress test is more meaningful for raw performance)
- TLS / authentication (LAN-only, trusted environment)
- Gateway (direct engine connection, minimal hops)

## Hardware Requirements

Two Hetzner dedicated servers on the same datacenter VLAN:

**Engine server:**
- High single-thread performance CPU (AMD Ryzen or EPYC with high boost clock)
- NVMe SSD with FUA support (critical — check `hdparm -I /dev/nvmeXnY` for "Deterministic read after TRIM" and FUA bit)
- At least 8 cores (need 6 dedicated: 0=OS/IRQ, 1-3=pipeline, 4-5=readers)
- At least 32 GB RAM

**Bench server:**
- Similar CPU (bench client is CPU-bound in io_uring event loop)
- At least 8 cores (need cores for 16 client threads)
- NVMe not critical (no journaling on bench side)

Both servers should be in the same Hetzner datacenter and ideally on the same rack/switch for minimal network latency.

## Prerequisites (Code Changes Needed)

Before running the demo, these features must be implemented:

### 1. Configuration Management (hard blocker)

The engine's bind address is hardcoded to `127.0.0.1:9876`. The bench client starts an embedded server on `127.0.0.1:0`. For the LAN demo, we need:

**Engine server:** Accept `--bind=0.0.0.0:9876` (or a config file) so it listens on the network interface.

**Bench client:** Accept `--addr=<engine-ip>:9876` to connect to a remote engine instead of spawning an embedded one. This means a new bench mode (or modifying `roundtrip`) that skips the embedded server and connects to an external address.

Also needed: configurable core affinity (the core layout may differ between machines).

### 2. Graceful Shutdown

Handle SIGTERM/SIGINT: stop accepting connections, drain the disruptor, flush the journal, exit cleanly. Without this, stopping the engine risks partial journal writes (recoverable, but messy for a demo).

### 3. Heartbeats and Connection Timeouts

If a bench client crashes or the network blips, the engine should detect dead connections and clean up. Without this, a failed benchmark run can leave the engine in a bad state requiring restart.

### 4. Health Checks / Readiness Probes

The bench client needs to know when the engine is ready before sending orders. Options (simplest first):
- Log line on stdout: `listening on 0.0.0.0:9876` (already exists) — bench script polls with `nc -z`.
- TCP probe: successful connect + disconnect means ready.
- Dedicated health endpoint (overkill for now).

### 5. Metrics (polish existing)

The bench client already produces latency histograms and throughput numbers. `pipeline-stats` prints per-stage utilization on shutdown. What's missing:
- Structured output (JSON or CSV) for easy comparison across runs.
- Server-side latency breakdown (journal sync time, matching time) in the final report.

## Server Setup Procedure

### Engine Server

```sh
# 1. System setup
sudo apt update && sudo apt install -y build-essential linux-tools-common

# 2. Check NVMe FUA support
sudo hdparm -I /dev/nvme0n1 | grep -i fua

# 3. Kernel tuning (GRUB — requires reboot)
#    Edit /etc/default/grub, add to GRUB_CMDLINE_LINUX:
#    isolcpus=nohz,domain,1-5 nohz_full=1-5 rcu_nocbs=1-5
#    Then: sudo update-grub && sudo reboot

# 4. Verify isolation after reboot
cat /sys/devices/system/cpu/isolated    # should show 1-5
cat /sys/devices/system/cpu/nohz_full   # should show 1-5

# 5. Clone and build
git clone <repo> && cd trading
cargo build --release

# 6. Start engine (with isolation script)
sudo ./scripts/bench-isolate.sh  # needs adaptation for remote mode
# OR manually:
#   - Set CPU governor to performance
#   - Pin IRQs to core 0
#   - Stop irqbalance
#   - Run: ./target/release/trading-server --bind=0.0.0.0:9876
```

### Bench Server

```sh
# 1. System setup (same as engine)
sudo apt update && sudo apt install -y build-essential linux-tools-common

# 2. Kernel tuning (isolate bench cores from scheduler)
#    isolcpus for bench thread cores (e.g., 1-8 if using 8 client threads)

# 3. Clone and build
git clone <repo> && cd trading
cargo build --release

# 4. Network test — verify LAN latency baseline
ping -c 100 <engine-ip>  # expect <0.1ms on same-rack Hetzner
```

## Benchmark Runs

### Run 1: Baseline — No Persistence

Establishes the pipeline + network ceiling without journal I/O.

```sh
# Engine server:
sudo ./scripts/bench-isolate.sh --features no-persist -- --bind=0.0.0.0:9876

# Bench server:
./target/release/trading-bench --addr=<engine-ip>:9876 10000000 --clients=16 --window=64
```

Expected: throughput will be lower than loopback (3.61M) due to real network latency. The gap reveals the network cost.

### Run 2: Full Durability (fsync/FUA)

The production configuration.

```sh
# Engine server:
sudo ./scripts/bench-isolate.sh -- --bind=0.0.0.0:9876

# Bench server:
./target/release/trading-bench --addr=<engine-ip>:9876 10000000 --clients=16 --window=64
```

Expected: throughput similar to loopback (830K) since the bottleneck is NVMe FUA latency, not network. If network latency is < FUA latency (~50-100 us), throughput should be roughly the same.

### Run 3: Client Scaling

Vary client count to show how throughput scales.

```sh
# Bench server — run each:
./target/release/trading-bench --addr=<engine-ip>:9876 10000000 --clients=1 --window=64
./target/release/trading-bench --addr=<engine-ip>:9876 10000000 --clients=4 --window=64
./target/release/trading-bench --addr=<engine-ip>:9876 10000000 --clients=16 --window=64
./target/release/trading-bench --addr=<engine-ip>:9876 10000000 --clients=64 --window=64
```

### Run 4: Crash Recovery Demo

The most compelling demonstration.

```sh
# 1. Start engine with persistence
sudo ./scripts/bench-isolate.sh -- --bind=0.0.0.0:9876

# 2. Run benchmark for a few million orders, then Ctrl-C the bench client mid-run

# 3. Kill the engine hard (simulating a crash)
sudo kill -9 $(pidof trading-server)

# 4. Restart the engine — it replays the journal automatically
sudo ./scripts/bench-isolate.sh -- --bind=0.0.0.0:9876
# Observe: "recovered N events from journal" in logs

# 5. Verify: run a small benchmark or use the TUI to check that the order book
#    and account balances are identical to pre-crash state
```

### Run 5: Window Size Sensitivity

Show how pipelining depth affects throughput and tail latency.

```sh
./target/release/trading-bench --addr=<engine-ip>:9876 10000000 --clients=16 --window=1
./target/release/trading-bench --addr=<engine-ip>:9876 10000000 --clients=16 --window=8
./target/release/trading-bench --addr=<engine-ip>:9876 10000000 --clients=16 --window=64
./target/release/trading-bench --addr=<engine-ip>:9876 10000000 --clients=16 --window=256
```

## Expected Results

### Loopback vs LAN Comparison

| Metric | Loopback (current) | LAN (expected) |
|--------|-------------------|----------------|
| **No-persist throughput** | 3.61M orders/sec | 1-3M orders/sec (network-bound) |
| **No-persist p99** | 355 us | 400-800 us |
| **Fsync throughput** | 830K orders/sec | ~800K orders/sec (journal-bound, network hidden) |
| **Fsync p99** | 1.84 ms | 1.9-2.5 ms |

The key insight: with full persistence, **LAN latency should be nearly invisible** because it's overlapped with the much larger FUA sync latency (~50-100 us NVMe vs ~25-50 us LAN RTT). The no-persist run will show the network cost clearly.

### What Would Be Impressive

- Fsync throughput within 10% of loopback numbers (proves network is not the bottleneck)
- p99.9 < 5 ms with full durability over LAN
- Crash recovery in < 1 second for 10M events
- Clean scaling from 1 to 64 clients

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| NVMe doesn't support FUA | Check before renting. Hetzner NVMe drives generally support FUA. Fall back to fdatasync if needed (higher latency). |
| Network jitter from noisy neighbors | Run at off-peak hours. Use dedicated servers, not cloud VMs. |
| Different CPU microarchitecture | Re-tune core pinning for the server's topology. Check `lscpu` for NUMA nodes. |
| Kernel version differences | Target the same Ubuntu/kernel version on both machines. Our io_uring usage requires kernel >= 5.19. |
| `bench-isolate.sh` assumes local server | Adapt script to support remote engine mode (just CPU tuning + run bench, no embedded server). |

## Deliverables

After the demo, update:
- `README.md` Performance section with LAN numbers alongside loopback numbers
- Blog post or writeup with latency histograms, client scaling charts, and the crash recovery story
