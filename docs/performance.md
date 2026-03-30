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

Performance optimization leads are tracked in [docs/roadmap.md](roadmap.md) (deferred section).

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
