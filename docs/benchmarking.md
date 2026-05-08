# Benchmarking Guide

This document covers the benchmark suite in `crates/bench/`, including benchmark modes, order generation, CLI parameters, measurement methodology, hardware tuning, and how to reproduce the published performance numbers.

## Benchmark Modes

The suite offers three modes that progressively strip away layers of the stack, making it possible to isolate where time is spent.

### `--mode=roundtrip` (default)

Full end-to-end benchmark through the entire server. By default, an embedded server is spawned in-process and clients connect via TCP loopback. With `--addr=<ip:port>`, clients connect to a remote engine instead (LAN benchmark mode). With `--uds`, clients use Unix domain sockets.

What it measures: client-perceived round-trip latency including transport, reader thread, disruptor publication, journal fsync, matching engine execution, response stage, and the return trip through the socket.

Each bench thread runs its own io_uring ring (RECV/SEND) and manages a subset of connections.

### `--mode=pipeline`

Disruptor pipeline without network transport. Publishes events directly to the ring buffer and drains responses from the output queue. The journal stage and matching stage run on their own threads, exactly as in the real server.

What it measures: journal I/O latency overlapped with matching engine execution, plus disruptor publication and drain overhead. Excludes all transport syscall and kernel buffer costs.

Why numbers differ from roundtrip: the TCP/UDS network stack is the primary throughput limiter. Removing it reveals the raw pipeline throughput, which is substantially higher.

### `--mode=engine`

Matching engine only. Calls `Exchange::execute()` and `Exchange::cancel()` directly in a tight loop on the calling thread. No disruptor, no journal, no I/O, no threads.

What it measures: pure matching engine throughput and per-operation latency. This is the theoretical ceiling.

Why numbers differ from pipeline: there is no journal fsync, no ring buffer synchronization, no cross-thread cache coherence traffic. This mode shows how fast the business logic runs in isolation.

### Summary of what each mode includes

| Component | engine | pipeline | roundtrip |
|-----------|--------|----------|-----------|
| Matching engine | yes | yes | yes |
| Disruptor ring buffer | -- | yes | yes |
| Journal (fsync) | -- | yes | yes |
| Response stage | -- | -- | yes |
| TCP/UDS transport | -- | -- | yes |
| Reader thread (io_uring) | -- | -- | yes |
| Ed25519 auth handshake | -- | -- | yes |

## Order Generation

All modes use the same realistic order flow generator (`crates/bench/src/generator.rs`), which produces synthetic order streams that mimic real exchange order flow patterns. Events are pre-generated into memory before the timed run begins, so RNG overhead and allocation do not pollute per-order timing.

### Flow composition

- **High cancel+amend ratio**: 90% conditional probability when live orders exist — 60% pure cancels (`cancel_ratio`) + 30% cancel-replace amendments (`cancel_replace_ratio`). The realized unconditional ratio converges to approximately 47-52% because each cancel/amend consumes a live order from the tracking pool, forcing new submits.
- **Order types**: ~5% market orders (`market_order_ratio`), ~5% limit IOC (`ioc_ratio`), ~2% limit FOK (`fok_ratio`), ~3% stop/stop-limit (`stop_order_ratio`), remainder limit GTC. ~5% of non-aggressive GTC limits are post-only (`post_only_ratio`).
- **Aggressive orders**: 10% of limit submits cross the spread (`aggression_ratio`) -- buys placed above mid-price, sells below -- producing immediate fills.
- **Price placement**: power-law distribution around a mid-price (default 10,000 ticks). Exponent `price_alpha = 1.5` clusters orders near the inside of the book, with a long tail up to `max_price_offset = 200` ticks from mid.
- **Order sizes**: power-law distribution with exponent `size_alpha = 2.0`, range 1 to 1,000 lots.
- **Account selection**: Zipf-distributed across `num_accounts` accounts. Account 1 trades most frequently, account N least.
- **Self-trade prevention diversity**: 70% Allow, 10% CancelNewest, 10% CancelOldest, 10% CancelBoth.
- **Cancel targeting**: biased toward recent orders (U^2 distribution skews toward newest entries in the live order ring buffer), mimicking rapid quote updates.

### Live order tracking

The generator maintains a circular buffer of 100,000 recently submitted GTC limit order IDs. When the buffer wraps, evicted orders are automatically cancelled before generating new events, preventing orphaned resting orders from accumulating unboundedly.

### Pre-generation

- Engine mode: generates in-memory event structs (submit and cancel variants).
- Roundtrip mode: generates pre-encoded binary wire frames.

Each client connection gets its own generator instance with a partitioned order ID range to avoid collisions across connections.

## CLI Parameters

```
cargo run --release --bin melin-bench [-- [OPTIONS] [PAIRS]]
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
| `--bench-threads` | 4 | Number of bench client threads. Each runs its own io_uring ring and manages a subset of connections. |
| `--group-commit-us` | 0 | Group commit coalescing delay in microseconds. Adds an artificial delay before fsyncing to batch more events per sync. Beneficial for UDS transport; harmful for TCP (see [roadmap deferred section](roadmap.md#deferred)). |
| `--warmup` | 100,000 | Warmup orders per client (not included in measurements). Primes caches, branch predictors, and allocator state. |
| `--journal <PATH>` | temp directory | Path for the journal file. Use a dedicated NVMe disk for realistic durability benchmarks. |
| `--accounts` | 10,000 | Number of trading accounts in the generator. |
| `--instruments` | 100 | Number of instruments. |
| `--json <PATH>` | (none) | Write results to a JSON file for machine-readable post-processing (saturation curve sweeps). |
| `--key <PATH>` | (none) | Path to a 32-byte raw Ed25519 private key file. Required for remote mode (`--addr`). Auto-generated for embedded mode. |
| `--bench-cores <N>` | (unpinned) | First CPU core for bench thread pinning. Thread i pins to core N+i. Omit for unpinned (OS scheduler decides). Use 7 for local benchmarks (avoids server cores 1-6). Use 1 for remote benchmarks on a dedicated machine with `isolcpus`. |

### Feature flags

| Feature | Effect |
|---------|--------|
| `io-uring` | No-op (kept for backward compatibility). io_uring is now always used. |
| `chart` | After the benchmark, display a TUI with two views: (1) tail latency stability over time (p99/p99.9/p99.99 time series, sampled every 1,000 orders), and (2) latency distribution histogram. Press Tab to switch views, q to exit. |

## Measurement Methodology

### Timing

On x86_64, per-order latency is measured with `rdtscp` for minimal overhead. TSC ticks are converted to nanoseconds using a calibration factor computed at startup. On non-x86_64 platforms, `Instant::now()` is used as a fallback.

### What is timed

- **Engine mode**: timestamps bracketing each execute/cancel call. Measures pure function call latency.
- **Pipeline mode**: timestamp at publication to the disruptor, elapsed at response consumption. Measures end-to-end pipeline transit time.
- **Roundtrip mode**: timestamp at frame send, elapsed when the response is received and decoded. Measures full network round-trip.

### Histogram

Latency samples are recorded into an HDR Histogram with 3 significant digits of precision, providing sub-percent-accurate percentile reporting across the full dynamic range.

Warmup orders (default 100,000 per client) are excluded from the histogram. Only the measured portion contributes to reported percentiles.

### Percentile depth

The number of reported percentiles adapts to the sample size. Each additional "9" requires 10x more samples for statistical significance:

- p99 requires at least 1,000 samples
- p99.9 requires at least 10,000 samples
- p99.99 requires at least 100,000 samples
- p99.999 requires at least 1,000,000 samples
- ...and so on

With large enough sample sizes, percentiles are reported to p99.99999 and beyond.

### JSON output

With `--json <path>`, results are written as a JSON object with throughput, order counts, and all computed percentiles. Designed for building saturation curves by sweeping `--clients` and `--window` across multiple runs.

The JSON also includes `server_stages` populated by the tick-to-trade decomposition (next section). When the server is built without `--features latency-trace`, the field renders as `{"state": "disabled", "entries": []}` so downstream tooling can detect the unsupported configuration without a separate request.

## Tick-to-Trade Decomposition

The bench's full client-to-client round-trip percentiles answer "how long did one order take, end to end?" but not "where did that time go?". Exchange RFPs and serious diligence conversations typically ask for the latency profile decomposed into per-stage components: NIC ingress → parse → match → durability gate → encode → NIC egress.

Roundtrip mode collects this decomposition automatically when both ends are built with the `tick-to-trade` Cargo feature, by:

1. Server-side: each pipeline stage records nanosecond samples into a global `StatsRegistry` (one HDR histogram per stage). Production builds compile the entire path to ZSTs and inlined no-ops, so this is dev/bench only.
2. Bench-side: at end of run, the bench fetches `GET /stats-dump` from the server's health endpoint, parses the tab-separated body, and prints a `Server-side Per-Stage Latency` section under the per-order RTT histogram. The same data appears in the `--json` output as `server_stages.entries[]`.

### Two feature flags, two cost levels

The instrumentation is split across two opt-in flags, both off by default:

| Feature | Stages recorded | Per-event cost |
|---|---|---|
| `latency-trace` | 4 stages — journal/matching wakeup + execute, journal batch, plus reader publish, response SPSC wakeup, response dispatch, server e2e | ~3–5 mutex acquisitions (~100 ns / event at 4 M ops/s) |
| `tick-to-trade` (implies `latency-trace`) | adds 5 commercial-grade stages — reader: ingest, response: journal-wait, replica-wait, encode, egress | ~5 more mutex acquisitions on top (~200 ns / event total) |

`latency-trace` is the lighter mode for narrow stage-level debugging. `tick-to-trade` is the full decomposition for headline-quality bench artifacts, at roughly double the hot-path mutex traffic. Production builds (default features) carry zero overhead either way.

### Stages reported

| Stage | What it measures |
|---|---|
| `reader: ingest (recv_ts → publish complete)` | Full reader cost per frame: parse + auth/dedup + slot construction + ring publish |
| `reader: publish (decode → disruptor publish)` | Just the ring publish call, isolated as a sub-measurement of ingest |
| `journal: disruptor wakeup (publish → journal consume)` | Time between the reader publishing a slot and the journal stage consuming it |
| `journal: batch processing (write + sync)` | One sample per fsync batch — write encoding + io_uring submission + completion |
| `matching: disruptor wakeup (publish → matching consume)` | Reader-publish-to-matching-consume on the parallel matching consumer |
| `matching: execute (process_event)` | Matching engine cost for one event |
| `response: SPSC wakeup (matching publish → response consume)` | Matching-to-response output ring delivery time |
| `response: journal-wait (match_complete → journal cursor crossed)` | How long the journal cursor held up the response gate. Only sampled when the journal was actually on the critical path for that batch |
| `response: replica-wait (match_complete → replication cursor crossed)` | Same, for the replication cursor |
| `response: encode (per-kind wire encoding)` | One sample per outbound `ResponseKind` |
| `response: egress (flush_sends elapsed)` | One sample per io_uring SEND batch (TCP only — the DPDK egress lives in the poll thread and is a follow-up) |
| `response: dispatch (consume → socket write)` | Whole-batch dispatch time, kept as an overall sanity check |
| `server e2e (reader recv → response flush)` | Reader-to-egress server-side full path, kept as a sanity check |

Per-cursor wait samples (journal-wait / replica-wait) are recorded only when the cursor was strictly below `needed` at the gate loop's first iteration — so a cursor already past at entry doesn't get attributed wait time it didn't cause. See the `GateCrossTracker` doc-comment in `crates/server/src/response.rs` for the rationale and the per-slot caveat (cross timestamp is captured for the *batch's* `needed`, slightly overestimating wait for non-last slots).

### Measurement window

Server-side stage histograms accumulate from server start, including the seed-data drain (~10k bootstrap events) and the bench's warmup phase. The bench-side per-order RTT histogram, by contrast, only includes the measured period after warmup.

For long bench runs (millions of orders, multi-second measured period) the warmup phase is a negligible fraction of the totals and the percentiles are dominated by steady-state samples. For short runs (a few thousand measured orders, sub-second measured period) the warmup spike on stages like `matching: disruptor wakeup` can dominate the p99 and above — the seed-drain queues many events at once, producing wakeup samples in the hundreds of milliseconds.

Two ways to interpret the numbers:

- **For "where is the bottleneck?"** — read p50 of the lightweight stages (`reader: ingest`, `response: encode`, `response: egress`). These are dominated by per-event work, so warmup tail noise has minimal effect on the median.
- **For "p99/p99.9 of each stage in the measured period"** — use long bench runs. The warmup samples become a vanishing fraction of the percentile mass.

A future enhancement would expose a `/stats-reset` endpoint so the bench can clear histograms at the warmup→measured boundary; tracked separately.

### Software vs hardware NIC timestamps

`recv_ts` is captured in the reader thread *after* the kernel returns the bytes — a software approximation of NIC ingress, not a hardware timestamp. True NIC HW timestamping needs `SO_TIMESTAMPING` (kernel) or rte_mbuf timestamps with PHC support (DPDK), and is queued as a follow-up. For the typical use case (comparing transport options, identifying the bottleneck stage), the software approximation is within sub-µs of the true HW arrival time on loopback or LAN with a quiet NIC, and well below the metric resolution.

### Building and running

```sh
# Both server and bench built with `tick-to-trade` for the full
# decomposition. The flag implies `latency-trace`, so this also
# enables the lighter 4-stage histograms.
cargo build --release -p melin-server --features tick-to-trade
cargo build --release -p melin-bench  --features tick-to-trade

# Roundtrip benchmark — decomposition appears under the latency table.
# Use 1M+ orders so warmup phase doesn't dominate the percentiles
# (see "Measurement window" below).
./target/release/melin-bench --mode=roundtrip --clients=8 --window=64 1000000

# Or fetch the dump directly without running the bench.
curl http://127.0.0.1:9878/stats-dump
```

When the server is built without `tick-to-trade`, the bench prints a one-line note pointing at the feature flag rather than failing — `latency-trace`-only servers still return the lighter 4-stage dump but won't include the journal-wait / replica-wait / encode / egress / reader-ingest stages.

## Pipelining

The `--window` flag controls how many requests each client keeps in flight simultaneously without waiting for responses. This is the key parameter for saturating the server pipeline.

### How it works

Each client maintains a FIFO of in-flight timestamps. When a request is sent, its timestamp is pushed. When a response arrives, the oldest timestamp is popped and the round-trip latency is recorded. The client only sends new requests when the in-flight count is below `--window`.

### Why it increases throughput

Without pipelining (`--window=1`), each order must complete the full round trip (send, journal fsync, match, respond) before the next order is submitted. The pipeline sits idle between orders. With `--window=64` or higher, the journal stage processes a continuous stream of events, amortizing the fsync cost across many orders (batch sync amortization). The matching stage and journal stage overlap in parallel on different events from the same ring buffer.

### Choosing a window size

- `--window=1`: measures single-order latency with no amortization. This is the "how fast is one order" number.
- `--window=64` (default): reasonable balance between throughput and per-order latency.
- `--window=192-256`: saturates the pipeline for peak throughput measurements. Diminishing returns beyond this point; higher values mostly increase queueing delay without improving throughput.

## Hardware Setup

### CPU core pinning

All threads are pinned to specific CPU cores via `sched_setaffinity`. The layout is hardcoded (not CLI-configurable) for the benchmark:

| Cores | Threads |
|-------|---------|
| 0 | OS, IRQ handling, RCU callbacks |
| 1-3 | Pipeline (journal, matching, response) — set by server's `--cores` flag |
| 4 | Reader thread |
| 6 | Replication sender — set by server's `--cores` flag (4th value) |
| 7+ | Bench client threads (when `--bench-cores 7` is passed) |

Bench thread `i` is pinned to core `N + i` where N is the `--bench-cores` value. Without the flag, threads are unpinned. For local benchmarks, use `--bench-cores 7` to avoid server cores. For remote benchmarks on a dedicated machine, use `--bench-cores 1` with `isolcpus` for tighter measurements.

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
This samples kernel stacks on cores 1+ at 997 Hz. Warning: perf sampling itself introduces NMI-like interrupts that degrade latency. Use for diagnosis only.

### Kernel boot parameters (`grub-bench.conf`)

For best results, the kernel should be booted with core isolation parameters. See `scripts/grub-bench.conf`:

```
isolcpus=nohz,domain,1-5 nohz_full=1-5 rcu_nocbs=1-5
```

- `isolcpus=nohz,domain,1-5` -- removes cores 1-5 from scheduler load balancing and timer tick distribution. Only explicitly pinned threads run on these cores.
- `nohz_full=1-5` -- stops the timer tick on cores 1-5 when only one task is running.
- `rcu_nocbs=1-5` -- moves RCU callback processing off cores 1-5. Without this, RCU grace periods can still interrupt isolated cores.

To apply: edit `/etc/default/grub`, append to `GRUB_CMDLINE_LINUX_DEFAULT`, run `sudo update-grub`, reboot.

To validate:
```sh
cat /sys/devices/system/cpu/isolated      # should print: 1-5
cat /sys/devices/system/cpu/nohz_full     # should print: 1-5
```

### Kernel UDP buffers (rumcast benchmarks only — experimental)

The `--features rumcast` build uses UDP for the wire protocol. The Linux default `net.core.rmem_max` is 208 KB — too small for the bench's burst pattern; the kernel drops frames on arrival and the bench reports throughput an order of magnitude below the true server capacity, with multi-hundred-millisecond p99 latencies caused by NAK retransmits.

Apply before running rumcast benchmarks:

```sh
sudo sysctl -w \
    net.core.rmem_max=33554432 \
    net.core.wmem_max=33554432 \
    net.core.rmem_default=33554432 \
    net.core.wmem_default=33554432
```

See `docs/operations.md` for the persistent configuration and tail-latency signatures that confirm the cap was lifted.

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

The max latency in engine-only mode is caused by System Management Interrupts (SMIs), Non-Maskable Interrupts (NMIs), and kernel interrupts that cannot be disabled from userspace. These are not indicative of matching engine performance. The p99.99 is a more meaningful tail metric.

On AMD CPUs, SMI counts cannot be measured (MSR 0x34 is Intel-specific). The `bench-isolate.sh` script attempts to read it and reports results on Intel hardware.

### NVMe tail in standalone / single-replication modes

When the response stage waits on a local journal fsync for every order (standalone, or single-replication where the primary needs both local disk and the replica), the tail latency floor is set by the NVMe drive, not the engine. Enterprise NVMe drives occasionally pause command processing for ~1-2 ms to run internal garbage collection or wear-leveling. The bursts are short (tens to hundreds of ms) and rare — on a Micron 7450, roughly 1 in 10,000 commands takes >800 µs while the rest complete in ~25 µs. Pauses are triggered by the drive's internal free-list state under sustained writes, not by a wall-clock timer, so the observed cadence varies run to run.

Symptoms you will see at this floor:

- p99.9 is clean (<100 µs), p99.99 may creep above 1 ms.
- A small number of round-trips sit in the 0.5-2 ms range under persist mode but disappear entirely under `--features no-persist` (which skips journal I/O — unsafe for production, useful for confirming the hardware floor).

**Mitigations when a tighter tail matters:**

- **Run with dual replication and quorum durability** (default when 2 replicas are connected). The response stage then releases on any two of `{local fsync, replica 1 ack, replica 2 ack}` — the local NVMe is off the critical path whenever at least one replica has acked. This is the configuration the published peak-load numbers use. See [replication.md](replication.md).
- **Raise drive over-provisioning.** Create a smaller NVMe namespace that reserves more unallocated capacity (e.g., 28% instead of the default ~7%). Fewer valid pages per block means less GC copy-on-write and a shorter pause when GC does fire — typically cuts spike frequency 3-5×.
- **Use higher-endurance media.** Low-DWPD enterprise drives and pseudo-SLC-cache designs hold their tail better than general-purpose TLC parts.

## Reproducing Published Benchmarks

See the [README](../README.md#benchmarks) for the current hardware setup, benchmark parameters, and performance numbers. All LAN benchmarks are reproducible via `scripts/lan-bench-suite.sh`.

### Peak-load with full durability

Engine server:
```sh
./melin-server --bind 0.0.0.0:9876 --journal /mnt/journal/melin.journal
```

Bench client (separate machine):
```sh
./melin-bench 100000000 --addr <engine-ip>:9876 --window=256
```

### Single-order latency

```sh
./melin-bench 500000 --addr <engine-ip>:9876 --window=1 --clients=1
```

No pipelining, no batching. Measures the true single-order round-trip time with full durability.

### Engine-only

```sh
./melin-bench 100000000 --mode=engine
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
cargo build --release --bin melin-bench
```

The binary is at `target/release/melin-bench`.

## Limitations and Caveats

1. **Pre-generated orders**: all orders are generated into memory before the timed run. This means there are zero allocations on the hot path during measurement, which is realistic for the matching engine (it uses pre-allocated data structures) but means the benchmark does not measure order generation or parsing overhead.

2. **Single instrument in pipeline mode**: the pipeline benchmark uses a single instrument with one funded account. The roundtrip and engine modes use configurable multi-instrument, multi-account workloads (defaults: 100 instruments, 10,000 accounts).

3. **No cross-symbol correlations**: the generator selects instruments uniformly at random. Real markets have correlated order flow across related instruments (e.g., an index ETF and its components). This does not affect matching engine performance but means the order book depth profile may differ from production.

4. **Generous balances**: all accounts are generously funded. Balance reservation rejections are rare, which means the benchmark exercises the successful-reservation path almost exclusively.

5. **One order per request**: the benchmark sends one order per wire frame, matching real client behavior. It does not batch multiple orders into a single write, which would artificially inflate throughput numbers.

6. **Loopback vs. LAN**: local benchmarks (no `--addr`) use TCP loopback or UDS, which have lower and more predictable latency than real network links. Published numbers use LAN benchmarks with separate machines.

7. **No market data consumers**: the benchmark does not simulate market data subscribers reading order book updates. In production, market data dissemination adds load to the output path.

8. **Warmup sensitivity**: the default 100,000 warmup orders per client is sufficient for cache and branch predictor priming, but very short runs (e.g., 1,000 pairs) may not reach steady state. Use at least 100,000 pairs for meaningful results.

9. **TSC reliability**: the TSC-based timing assumes an invariant TSC (constant rate regardless of frequency scaling). This is true on modern x86_64 CPUs, but the calibration sleep may introduce a few percent of systematic error in the ticks-to-nanoseconds conversion.
