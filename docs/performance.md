# Performance

## Current numbers

LAN benchmark at `66fed71` (two Cherry AMD Ryzen 9950X servers, SMT disabled, dedicated NVMe journal disk, 10M order pairs):

| Mode | Throughput | p50 | p99.9 | max |
|------|-----------|-----|-------|-----|
| TCP + fsync (FUA) | 4.0M ord/s | 971 µs | 1083 µs | 1759 µs |
| TCP no-persist (window 512) | 10.0M ord/s | 762 µs | 1015 µs | 2767 µs |
| TCP + fsync + sync replication | 3.7M ord/s | 984 µs | 1332 µs | 2482 µs |
| Single-order (1 client, full durability) | — | 78 µs | — | — |
| Engine only | 12.9M ord/s | 50 ns | — | — |

Removing fsync unlocks 2.5x throughput (4.0M → 10.0M). The matching engine at 12.9M/s has ~3x headroom — it is not the bottleneck.

## Bottleneck stack

```
Engine only:     12.9M/s   ← matching engine (3x headroom)
TCP no-persist:  10.0M/s   ← TCP stack overhead
TCP + fsync:      4.0M/s   ← journal fsync gating halves it
TCP + repl:       3.7M/s   ← replica RTT costs another 8%
```

Core layout: 0=OS/IRQ, 1-3=pipeline (journal/matching/response), 4-5=readers, 6=repl-sender, 7+=bench.

## Engine profile

Profiled at `56e3f10` on Apple M1 (Asahi Linux), 20M orders, `perf record -D 3000`.

The bench loop spends ~58% of total time in ARM counter reads (`cntvct_el0`) — measurement overhead. On x86 Cherry servers with `rdtsc` (~4ns), this drops to ~10%. The table normalizes to engine-only time (excluding harness, generator, allocator).

| Engine % | Function | Notes |
|----------|----------|-------|
| **30.7%** | `process_reports` | FxHashMap lookups per fill, Vec::contains for double-free check |
| **12.3%** | `Exchange::execute` | Submit dispatch, validation, reserve, post-matching cleanup |
| **6.6%** | `hashbrown::insert` (2 sites) | FxHashMap insert into order_info + order_index per submit |
| **5.9%** | `OrderBook::cancel` | Index lookup + sorted Vec removal |
| **4.7%** | `OrderBook::execute` | Matching entry point |
| **3.9%** | `try_adjust_reservation` | Cancel-replace reservation adjustment |
| **3.1%** | `alloc` (grow + finish_grow) | VecDeque reallocation at price levels |
| **2.9%** | `cancel_replace` | Amend path |
| **2.8%** | `execute_limit` | Limit order processing |
| **2.4%** | `BookSide::add` | Binary search + insert into sorted Vec |
| **2.0%** | `match_against` | Price level iteration + fill loop |
| **1.2%** | `u128_div_rem` | Fee calc — software u128 div on ARM; native on x86 |

---

## Improvement roadmap

### Persistence / journal

The journal stage uses batch `pwritev2` + `RWF_DSYNC` (FUA) with 256 MiB pre-allocation, hardware CRC32C, and optional BLAKE3 hash chain. The response stage gates on `min(journal_cursor, replication_cursor)` — every microsecond of write latency directly adds to end-to-end latency.

| # | Optimization | Est. gain | Effort | Status |
|---|-------------|-----------|--------|--------|
| 1 | **Adaptive overlapped io_uring writes** | 30-50% throughput | Medium | Reverted — needs tail fix |
| 2 | **Auto-tune max batch size** | Up to 2-4x at sustained load | Low | Not started |
| 3 | **NVMe block device tuning** | Jitter reduction (p99.9/max) | Trivial | Done |
| 4 | **WRITE_FIXED for journal** | ~200ns/batch | Low | Not started |
| 5 | **Optane / persistent memory** | 3-10x journal latency | Hardware | Not started |
| 6 | **Vectored writes (iovec per event)** | ~1-2µs/batch | Low | Not started |

**1. Adaptive overlapped io_uring writes.** Double-buffer design: submit `WRITE` + `RWF_DSYNC` asynchronously, accumulate next batch in spare buffer while NVMe write is inflight. Already built and reverted — the problem is events accumulated during an inflight write have their cursor delayed by one extra NVMe write latency, increasing tail. Fix: only overlap when the batch is large enough (e.g., >16 events) that NVMe write time exceeds accumulation time. For small batches, write synchronously — the FUA is ~10µs anyway. This eliminates the tail penalty at low load (where tail matters most) while getting the throughput win under high load.

**2. Auto-tune max batch size.** Currently `MAX_JOURNAL_BATCH = 1024` is static. FUA cost is roughly constant regardless of payload size (up to ~128 KB = one NVMe command). Draining all available events from the ring (up to a higher cap, e.g., 4096) before writing means fewer FUA operations per second. At sustained high load, 4096 events in one FUA vs 4 × 1024-event FUA writes = 4x fewer syncs, directly increasing throughput. No latency penalty — events aren't delayed, they're already in the ring.

**3. NVMe block device tuning.** Implemented in `scripts/cherry-setup.sh` (sysfs writes + udev rule for persistence). Settings: `scheduler=none`, `nr_requests=2`, `nomerges=2`, `wbt_lat_usec=0`, `add_random=0`. Eliminates non-deterministic block layer overhead (scheduler sorting, merge scans, writeback throttling, entropy pool locks). Targets jitter (p99.9/max), not median throughput. Also worth checking NVMe volatile write cache: `nvme get-feature -f 0x06 /dev/nvmeXn1` — if disabled, FUA is already a no-op.

**4. WRITE_FIXED for journal.** Register the two batch buffers via `IORING_REGISTER_BUFFERS` and use `IORING_OP_WRITE_FIXED` instead of plain `WRITE`. Skips `get_user_pages()` per SQE (~100-200ns). This failed for *socket* I/O on kernel 6.8 (routes through VFS), but works reliably for file writes.

**5. Intel Optane / persistent memory.** Hardware change. Optane P5800X NVMe: ~5-7µs write latency with FUA vs ~10-20µs for typical TLC NVMe. Optane PMem (DCPMM) in App Direct mode: ~300ns write latency via DAX (`mount -o dax`) + `memcpy` + `clwb` + `sfence` — eliminates the syscall entirely.

**6. Vectored writes.** Encode each event into its own small stack buffer and pass all as an iovec array to `pwritev2`. Eliminates the memcpy-per-event into the batch buffer (~100 KB/batch at 1024 events). Small gain but essentially free.

### Engine / matching

| # | Optimization | Est. gain | Effort | Status |
|---|-------------|-----------|--------|--------|
| 1 | **Embed ReservationSlot in RestingOrder** | 5-10% matching | Moderate | Not started |
| 2 | **Monotonic sequence ID for order tracking** | 5-10% matching | Moderate | Not started |
| 3 | **Vectored response writes** | 5-10% throughput | Low | Not started |
| 4 | **`#[inline(always)]` on hot-path methods** | 2-5% throughput | Trivial | Not started |
| 5 | **Profile-Guided Optimization (PGO)** | 10-30% overall | Low | Not started |

**1. Embed ReservationSlot in RestingOrder.** Eliminates the global `order_info` FxHashMap. Every cancel/amend currently does 2 lookups + 1 remove on `order_info` (~15-30ns). With the slot stored in the resting order itself, that drops to zero. Needs to thread `ReservationSlot` through the OrderBook API.

**2. Monotonic sequence ID for order tracking.** Assign a global `u32` sequence number at submission. Index `order_info` and `order_index` as flat `Vec` instead of `FxHashMap<(AccountId, OrderId), ...>`. Eliminates remaining hash lookups per execute/cancel. Requires threading the ID through the wire protocol and orderbook.

**3. Vectored response writes.** Batch multiple responses to the same connection into one `writev` syscall. Reduces per-response syscall overhead.

**4. Inline hot-path exchange methods.** `cancel`, `cancel_replace`, `execute` are called millions of times. `#[inline(always)]` lets LLVM optimize across call boundaries.

**5. PGO.** Two-pass build with `rustc -Cprofile-generate` / `-Cprofile-use`. Branch-heavy matching loops benefit most. Ideally profile against real market data, not the synthetic generator.

### Network / transport

| # | Optimization | Est. gain | Effort | Status |
|---|-------------|-----------|--------|--------|
| 1 | **io_uring RECVSEND_FIXED_BUF (kernel 6.12+)** | 15-25% throughput | Low | Not started |
| 2 | **OpenOnload (Solarflare NIC)** | 2-4x throughput, 60-80% latency | Zero code | Not started |
| 3 | **DPDK + smoltcp** | See CLAUDE.md | High | In progress |
| 4 | **AF_XDP + smoltcp userspace TCP** | 20-40% latency | Very high | Not started |

Options 1-4 are mutually exclusive kernel bypass paths (pick one).

**1. io_uring registered buffer recv/send.** `IORING_RECVSEND_FIXED_BUF` returned EINVAL for RECV on kernel 6.8. Should work on 6.10+. Also test `IORING_RECVSEND_BUNDLE` for batched recv.

**2. OpenOnload.** Kernel-bypass TCP via `LD_PRELOAD`, zero code changes. BSD-licensed. Requires Solarflare/AMD Xilinx NIC (~$500-1000). This is what most production exchanges use.

**3. DPDK + smoltcp.** Already in progress on `feat/dpdk-transport`. See CLAUDE.md for current status and benchmark results. smoltcp's software TCP processing currently adds more overhead than the syscalls it eliminates — needs hardware checksum offload or direct PF binding.

**4. AF_XDP + smoltcp userspace TCP.** Full kernel bypass with Rust-native TCP stack over AF_XDP sockets. DaMoN '25 paper found AF_XDP disappoints vs DPDK for small-message request-response workloads due to remaining kernel overhead. Very high complexity (6+ months).

## Completed optimizations

- Release profile: `lto = "fat"`, `codegen-units = 1`, `panic = "abort"`, `target-cpu=native`
- jemalloc (`tikv-jemallocator`)
- CPU core pinning for all pipeline, reader, and bench threads
- IRQ affinity pinning (`bench-isolate.sh`)
- Kernel boot isolation (`isolcpus`, `nohz_full`, `rcu_nocbs`)
- Reservation slab — `Vec<Reservation>` + free list for O(1) indexed access on every fill/cancel
- Reusable match price buffer — avoids heap allocation per aggressive order
- Flat Vec instrument dispatch — `Vec<Option<Box<InstrumentState>>>` indexed by `Symbol.0`
- Flat Vec max_order_id — `Vec<u64>` indexed by `AccountId.0`
- FxHashMap for order_info, order_index, stop_index — ~4x faster than SipHash
- Cache-friendly price levels — sorted `Vec<(Price, VecDeque)>` with binary search (5-20 levels fit in 1-3 L1 cache lines)
- Right-sized HashMap pre-allocation — order_index 4K, order_info 32K (~5% matching utilization reduction)
- Batched matching stage consumption — `consume_batch(32)` amortizes atomic store (+3% throughput, -50% p99)
- Hardware CRC32C checksums (`crc32c` crate with SSE4.2/NEON intrinsics)
- `pwritev2` + `RWF_DSYNC` (FUA) — single syscall per batch, no separate fsync
- 256 MiB journal pre-allocation via `posix_fallocate` — eliminates metadata sync overhead
- Overlapped io_uring journal writes (built, reverted due to tail latency — preserved on branch)

## Dead ends

See CLAUDE.md "Dead Ends / Investigated & Rejected" for full details on:

- SMI count tracking via MSR 0x34 (AMD doesn't expose it)
- io_uring registered buffers for socket I/O (VFS layer routing, EINVAL on kernel 6.8)
- Group commit delay with TCP transport (only helps UDS)
- Overlapped io_uring journal writes (tail latency regression — fixable, see persistence roadmap above)
- Response stage per-slot journal cursor gating (synchronous flush too expensive)
