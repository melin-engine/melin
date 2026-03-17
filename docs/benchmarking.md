# Benchmarking Guide

This document covers the benchmark suite in `crates/bench/`, including benchmark modes, order generation, CLI parameters, measurement methodology, hardware tuning, and how to reproduce the published performance numbers.

## Benchmark Modes

The suite offers three modes that progressively strip away layers of the stack, making it possible to isolate where time is spent.

### `--mode=roundtrip` (default)

Full end-to-end benchmark through the entire server. By default, an embedded server is spawned in-process and clients connect via TCP loopback. With `--addr=<ip:port>`, clients connect to a remote engine instead (LAN benchmark mode). With `--uds`, clients use Unix domain sockets.

What it measures: client-perceived round-trip latency including TCP/UDS transport, kernel network stack, the epoll/io_uring reader pool, CAS-based multi-producer publication to the disruptor, journal fsync, matching engine execution, response stage journal-cursor gating, and the return trip through the socket.

This mode uses either an io_uring-based single-threaded event loop (default, via the `io-uring` feature) or an epoll-based multi-threaded event loop (with `--no-default-features`). Each bench thread runs its own io_uring ring or epoll instance and manages a subset of connections.

### `--mode=pipeline`

Disruptor pipeline without network transport. Publishes `InputSlot` events directly to the `MultiProducer` ring buffer and drains `OutputSlot` responses from the SPSC consumer queue. The journal stage and matching stage run on their own threads, exactly as in the real server.

What it measures: journal I/O latency (pwritev2 + RWF_DSYNC) overlapped with matching engine execution, plus disruptor publication and SPSC drain overhead. Excludes all TCP/UDS syscall and kernel buffer costs.

Why numbers differ from roundtrip: the TCP/UDS network stack is the primary throughput limiter. Removing it reveals the raw pipeline throughput, which is substantially higher.

### `--mode=engine`

Matching engine only. Calls `Exchange::execute()` and `Exchange::cancel()` directly in a tight loop on the calling thread. No disruptor, no journal, no I/O, no threads.

What it measures: pure matching engine throughput and per-operation latency. This is the theoretical ceiling: the cost of order book operations (BTreeMap lookups, VecDeque manipulation, balance reservation math).

Why numbers differ from pipeline: there is no journal fsync, no ring buffer synchronization, no cross-thread cache coherence traffic. This mode shows how fast the business logic runs in isolation.

### Summary of what each mode includes

| Component | engine | pipeline | roundtrip |
|-----------|--------|----------|-----------|
| Matching engine | yes | yes | yes |
| Disruptor ring buffer | -- | yes | yes |
| Journal (fsync) | -- | yes | yes |
| Response stage | -- | -- | yes |
| TCP/UDS transport | -- | -- | yes |
| Reader pool (epoll) | -- | -- | yes |
| Ed25519 auth handshake | -- | -- | yes |

## Order Generation

All modes use the same realistic order flow generator (`crates/bench/src/generator.rs`), which produces synthetic order streams that mimic real exchange order flow patterns. Events are pre-generated into memory before the timed run begins, so RNG overhead and allocation do not pollute per-order timing.

### Flow composition

- **High cancel ratio**: 90% conditional cancel probability (configured via `cancel_ratio`). The realized unconditional ratio converges to approximately 47-52% because each cancel consumes a live order from the tracking pool, forcing new submits.
- **Order types**: ~5% market orders (`market_order_ratio`), ~5% limit IOC (`ioc_ratio`), ~2% limit FOK (`fok_ratio`), remainder limit GTC.
- **Aggressive orders**: 10% of limit submits cross the spread (`aggression_ratio`) -- buys placed above mid-price, sells below -- producing immediate fills.
- **Price placement**: power-law distribution around a mid-price (default 10,000 ticks). Exponent `price_alpha = 1.5` clusters orders near the inside of the book, with a long tail up to `max_price_offset = 200` ticks from mid.
- **Order sizes**: power-law distribution with exponent `size_alpha = 2.0`, range 1 to 1,000 lots.
- **Account selection**: Zipf-distributed across `num_accounts` accounts. Account 1 trades most frequently, account N least.
- **Self-trade prevention diversity**: 70% Allow, 10% CancelNewest, 10% CancelOldest, 10% CancelBoth.
- **Cancel targeting**: biased toward recent orders (U^2 distribution skews toward newest entries in the live order ring buffer), mimicking rapid quote updates.

### Live order tracking

The generator maintains a circular buffer of 100,000 recently submitted GTC limit order IDs. When the buffer wraps, evicted orders are automatically cancelled before generating new events, preventing orphaned resting orders from accumulating unboundedly.

### Pre-generation

- Engine mode: `generate_events(count)` returns a `Vec<GeneratedEvent>` of `Submit` and `Cancel` variants.
- Roundtrip mode: `generate_frames(count)` returns a `Vec<Vec<u8>>` of pre-encoded binary wire frames (without the 4-byte length prefix, which is prepended at send time).

Each client connection gets its own generator instance with a partitioned `start_order_id` range to avoid order ID collisions across connections.

## CLI Parameters

```
cargo run --release -p trading-bench [-- [OPTIONS] [PAIRS]]
```

### Positional arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `PAIRS` | 1,000,000 | Number of order pairs (each pair = 1 buy + 1 sell = 2 orders measured). Total measured orders = `PAIRS * 2`. |

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--mode` | `roundtrip` | Benchmark mode: `roundtrip`, `pipeline`, or `engine`. |
| `--addr <IP:PORT>` | (none) | Connect to a remote engine instead of spawning an embedded server. Enables LAN benchmarking. Mutually exclusive with `--uds`. Requires `--key`. |
| `--uds` | false | Use Unix domain sockets instead of TCP (roundtrip mode only, local embedded server). |
| `--clients` | 16 | Number of concurrent client connections (roundtrip mode). |
| `--window` | 64 | Pipeline depth: number of requests in flight per client before waiting for a response. |
| `--bench-threads` | 4 | Number of bench client threads. Each manages a subset of connections via epoll (ignored when compiled with the `io-uring` feature). Pinned to cores starting at core 6. |
| `--group-commit-us` | 0 | Group commit coalescing delay in microseconds. Adds an artificial delay before fsyncing to batch more events per sync. Beneficial for UDS transport; harmful for TCP (see CLAUDE.md dead ends). |
| `--warmup` | 100,000 | Warmup orders per client (not included in measurements). Primes caches, branch predictors, and allocator state. |
| `--journal <PATH>` | temp directory | Path for the journal file. Use a dedicated NVMe disk for realistic durability benchmarks. |
| `--accounts` | 1,000 | Number of trading accounts in the generator. |
| `--instruments` | 100 | Number of instruments. |
| `--json <PATH>` | (none) | Write results to a JSON file for machine-readable post-processing (saturation curve sweeps). |
| `--key <PATH>` | (none) | Path to a 32-byte raw Ed25519 private key file. Required for remote mode (`--addr`). Auto-generated for embedded mode. |

### Feature flags

| Feature | Effect |
|---------|--------|
| `io-uring` | Use io_uring RECV/SEND instead of epoll + blocking writes for the bench client. Single-threaded event loop per bench thread. |
| `chart` | After the benchmark, display a TUI with two views: (1) tail latency stability over time (p99/p99.9/p99.99 time series, sampled every 1,000 orders), and (2) latency distribution histogram. Press Tab to switch views, q to exit. |

## Measurement Methodology

### Timing

On x86_64, per-order latency is measured with `rdtscp` (Time Stamp Counter with serialization). Overhead is approximately 4 ns per read, compared to 15-25 ns for `Instant::now()` via the vDSO. The `rdtscp` instruction serializes: it waits for all prior instructions to retire before reading the counter, preventing the CPU from reordering the timestamp past the work being measured.

TSC ticks are converted to nanoseconds using a calibration factor computed at startup: a 10 ms `thread::sleep` is measured with both `rdtscp` and `Instant::now()`, and the ratio `ticks / ns` is stored. On non-x86_64 platforms, `Instant::now()` is used as a fallback.

### What is timed

- **Engine mode**: `rdtscp` bracketing each `Exchange::execute()` or `Exchange::cancel()` call. Measures pure function call latency.
- **Pipeline mode**: `Instant::now()` at publication to the disruptor, elapsed at `BatchEnd` consumption from the SPSC output queue. Measures end-to-end pipeline transit time.
- **Roundtrip mode**: `Instant::now()` at frame send, elapsed when the corresponding `BatchEnd` response is received and decoded. Measures full network round-trip including kernel buffers and transport overhead.

### Histogram

Latency samples are recorded into an HDR Histogram (`hdrhistogram` crate) with bounds of 1 ns to 10 seconds and 3 significant digits of precision. This provides sub-percent-accurate percentile reporting across the full dynamic range without fixed bucket boundaries.

Warmup orders (default 100,000 per client) are excluded from the histogram. Only the measured portion contributes to reported percentiles.

### Percentile depth

The number of reported percentiles adapts to the sample size. Each additional "9" requires 10x more samples for statistical significance:

- p99 requires at least 1,000 samples
- p99.9 requires at least 10,000 samples
- p99.99 requires at least 100,000 samples
- p99.999 requires at least 1,000,000 samples
- ...and so on

With 100M order pairs (200M measured orders), percentiles are reported through p99.99999.

### JSON output

With `--json <path>`, results are written as a single JSON object containing `label`, `measured_orders`, `warmup_orders`, `wall_ms`, `throughput_ops`, and a `latency` object with all computed percentiles in microseconds. This is designed for building saturation curves by sweeping `--clients` and `--window` across multiple runs.

## Pipelining

The `--window` flag controls how many requests each client keeps in flight simultaneously without waiting for responses. This is the key parameter for saturating the server pipeline.

### How it works

Each client maintains a FIFO of in-flight timestamps (`VecDeque<Instant>`). When a request is sent, its timestamp is pushed. When a `BatchEnd` response arrives, the oldest timestamp is popped and the round-trip latency is recorded. The client only sends new requests when the in-flight count is below `--window`.

### Why it increases throughput

Without pipelining (`--window=1`), each order must complete the full round trip (send, journal fsync, match, respond) before the next order is submitted. The pipeline sits idle between orders. With `--window=64` or higher, the journal stage processes a continuous stream of events, amortizing the fsync cost across many orders (batch sync amortization). The matching stage and journal stage overlap in parallel on different events from the same ring buffer.

### Choosing a window size

- `--window=1`: measures single-order latency with no amortization. This is the "how fast is one order" number (e.g., 70 us p50 with full durability).
- `--window=64` (default): reasonable balance between throughput and per-order latency.
- `--window=192-256`: saturates the pipeline for peak throughput measurements. Diminishing returns beyond this point; higher values mostly increase queueing delay without improving throughput.

## Hardware Setup

### CPU core pinning

All threads are pinned to specific CPU cores via `sched_setaffinity`. The layout is hardcoded (not CLI-configurable) for the benchmark:

| Cores | Threads |
|-------|---------|
| 0 | OS, IRQ handling, RCU callbacks |
| 1-3 | Pipeline (journal, matching, response) — set by server's `--cores` flag |
| 4-5 | Reader pool threads |
| 6+ | Bench client threads (`BENCH_CORE_START = 6`, hardcoded) |

Bench thread `i` is pinned to core `6 + i`. With 4 bench threads (default), cores 6-9 are used. The server's pipeline cores are configurable via `--cores`; the bench client's core start offset is not.

### IRQ affinity (`bench-isolate.sh`)

The `scripts/bench-isolate.sh` script applies runtime optimizations before running the benchmark (requires root):

```sh
sudo ./scripts/bench-isolate.sh [bench args]
```

What it does:

1. **CPU governor to performance** -- locks frequency at maximum, eliminates frequency scaling transitions that cause multi-microsecond stalls.
2. **NMI watchdog disabled** -- stops periodic non-maskable interrupts that can pause a core for several microseconds.
3. **irqbalance stopped** -- prevents the daemon from redistributing IRQs after they are pinned.
4. **All IRQs pinned to core 0** -- keeps NIC, NVMe, USB, and other hardware interrupts off pipeline cores 1-5 and bench cores 6+.
5. **SMI count tracking** -- reads MSR 0x34 (IA32_SMI_COUNT) before and after the benchmark to detect firmware-level System Management Interrupts (Intel CPUs only; not available on AMD).
6. **dmesg capture** -- diffs kernel messages before and after to correlate latency spikes with kernel events.
7. **All settings restored on exit** (including Ctrl-C).

Optional perf profiling:
```sh
BENCH_PERF=1 sudo ./scripts/bench-isolate.sh [bench args]
```
This samples kernel stacks on cores 1+ at 997 Hz. Warning: perf sampling itself introduces NMI-like interrupts that degrade latency (~20% throughput drop). Use for diagnosis only.

### Kernel boot parameters (`grub-bench.conf`)

For best results, the kernel should be booted with core isolation parameters. See `scripts/grub-bench.conf`:

```
isolcpus=nohz,domain,1-5 nohz_full=1-5 rcu_nocbs=1-5
```

- `isolcpus=nohz,domain,1-5` -- removes cores 1-5 from scheduler load balancing and timer tick distribution. Only explicitly pinned threads run on these cores.
- `nohz_full=1-5` -- stops the timer tick on cores 1-5 when only one task is running. Eliminates ~1-10 us jitter every 4 ms (HZ=250).
- `rcu_nocbs=1-5` -- moves RCU callback processing off cores 1-5. Without this, RCU grace periods can still interrupt isolated cores.

To apply: edit `/etc/default/grub`, append to `GRUB_CMDLINE_LINUX_DEFAULT`, run `sudo update-grub`, reboot.

To validate:
```sh
cat /sys/devices/system/cpu/isolated      # should print: 1-5
cat /sys/devices/system/cpu/nohz_full     # should print: 1-5
```

## Interpreting Results

### Throughput

Reported as orders/sec, computed as `(measured_orders + warmup_orders) / wall_time`. The wall time covers the entire run including warmup, since warmup orders still consume server resources. The throughput number represents the sustained rate the server processes under load, not just the measured portion.

### Latency percentiles

All latency values are in microseconds. The histogram reports:

- **min**: fastest single order (often dominated by cancels of non-existent orders or orders that don't match).
- **p50**: median latency. For roundtrip benchmarks, this includes network round-trip time.
- **p90**: 90th percentile.
- **p99, p99.9, ...**: tail latency. The depth of reported percentiles depends on sample size (see above).
- **max**: single worst-case order. Heavily influenced by SMIs, NMIs, kernel interrupts, and other non-deterministic system events.

### Max latency outliers

The max latency in engine-only mode (typically 20-120 us for 100M orders) is caused by System Management Interrupts (SMIs), Non-Maskable Interrupts (NMIs), and kernel interrupts that cannot be disabled from userspace. These events pause the CPU for 50-200 us. They occur roughly once per 20M orders and are not indicative of matching engine performance. The p99.99 is a more meaningful tail metric.

On AMD CPUs, SMI counts cannot be measured (MSR 0x34 is Intel-specific). The `bench-isolate.sh` script attempts to read it and reports results on Intel hardware.

## Reproducing Published Benchmarks

All published numbers use two Cherry AMD Ryzen 9950X servers (16C/32T, 192 GB RAM, 2x 1TB NVMe, 10 Gbps) with the engine on one server and the benchmark client on the other, connected via a private network.

### Peak-load with full durability (5.2M orders/sec)

Engine server:
```sh
./trading-server --bind 0.0.0.0:9876 --journal /mnt/journal/trading.journal
```

Bench client (separate machine):
```sh
./trading-bench 100000000 --addr <engine-ip>:9876 --window=256
```

Uses 16 clients (default), 256 pipelined requests per client, 100M order pairs (200M orders).

### Peak-load without persistence (11.2M orders/sec)

Engine server started without journal persistence. Bench client:
```sh
./trading-bench 100000000 --addr <engine-ip>:9876 --window=192 --clients=32
```

### Single-order latency (70 us p50)

```sh
./trading-bench 1000000 --addr <engine-ip>:9876 --window=1 --clients=1
```

No pipelining, no batching. Measures the true single-order round-trip time with full durability.

### Engine-only (17.3M orders/sec)

```sh
./trading-bench 100000000 --mode=engine
```

Runs on the engine server itself. No network, no journal, no pipeline.

### Build for benchmarking

The release profile is configured for maximum performance:
- `lto = "fat"` (link-time optimization across all crates)
- `codegen-units = 1` (better optimization at the cost of compile time)
- `panic = "abort"` (no unwinding overhead)
- `target-cpu=native` (use all available CPU instructions)
- jemalloc allocator (thread-local caches eliminate allocator lock contention)

Build with:
```sh
cargo build --release -p trading-bench
```

The binary is at `target/release/trading-bench`.

## Limitations and Caveats

1. **Pre-generated orders**: all orders are generated into memory before the timed run. This means there are zero allocations on the hot path during measurement, which is realistic for the matching engine (it uses pre-allocated data structures) but means the benchmark does not measure order generation or parsing overhead.

2. **Single instrument in pipeline mode**: the pipeline benchmark uses a single instrument with one funded account. The roundtrip and engine modes use configurable multi-instrument, multi-account workloads (defaults: 100 instruments, 1,000 accounts).

3. **No cross-symbol correlations**: the generator selects instruments uniformly at random. Real markets have correlated order flow across related instruments (e.g., an index ETF and its components). This does not affect matching engine performance but means the order book depth profile may differ from production.

4. **Generous balances**: all accounts are funded with `u64::MAX / 4` per currency. Balance reservation rejections are rare, which means the benchmark exercises the successful-reservation path almost exclusively.

5. **One order per request**: the benchmark sends one order per wire frame, matching real client behavior. It does not batch multiple orders into a single write, which would artificially inflate throughput numbers.

6. **Loopback vs. LAN**: local benchmarks (no `--addr`) use TCP loopback or UDS, which have lower and more predictable latency than real network links. Published numbers use LAN benchmarks with separate machines.

7. **No market data consumers**: the benchmark does not simulate market data subscribers reading order book updates. In production, market data dissemination adds load to the output path.

8. **Warmup sensitivity**: the default 100,000 warmup orders per client is sufficient for cache and branch predictor priming, but very short runs (e.g., 1,000 pairs) may not reach steady state. Use at least 100,000 pairs for meaningful results.

9. **TSC reliability**: the TSC-based timing assumes an invariant TSC (constant rate regardless of frequency scaling). This is true on modern x86_64 CPUs with `constant_tsc` and `nonstop_tsc` flags, but the calibration sleep may introduce a few percent of systematic error in the ticks-to-nanoseconds conversion.
