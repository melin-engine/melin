# Latency improvement backlog

Engineering backlog for further floor- and tail-latency wins on the
DPDK transport, ranked by what we know after the latency-trace and
outlier investigations.

This is a working document; entries are added/removed/promoted as we
learn more from each bench run.

## Reference: where time goes today

DPDK + `no-persist` + `trading`, single client, window 1 (single-order
workload, EPYC 7443P):

| Stage | p50 | p99 | p99.9 | max |
|---|---|---|---|---|
| DPDK poll outer iteration (work-only) | 1.2 µs | 1.7 µs | 1.8 µs | 63 µs |
| Journal batch processing | 0.23 µs | 0.46 µs | 0.81 µs | 510 µs |
| Matching execute (process_event) | 0.05 µs | 0.71 µs | 1.18 µs | 12 ms (saturated; seed phase) |
| Response SPSC wakeup | 0.24 µs | 0.51 µs | 0.69 µs | 597 µs |
| Response dispatch | 0.46 µs | 0.67 µs | 0.86 µs | 11.5 µs |
| **Server e2e (recv → tx_rx push)** | **1.42 µs** | **2.09 µs** | **2.50 µs** | **23 µs** |
| **Bench-measured RTT** | **48 µs** | **78 µs** | **78 µs** | **183 µs** |

The 46 µs gap between server e2e and bench RTT lives **on the wire / NIC
path** — N2's investigation ruled out both the bench's poll loop and
smoltcp's TCP processing as sources of the variance. The 113 ms
matching-execute outliers we once chased turned out to be **seed-phase
only** (`connection_id=0`). The trading hot path has no >1 ms outliers
across 200M+ events.

## Done

| ID | Item | Result |
|---|---|---|
| T1 | Outlier logging on matching execute >1 ms | Identified 113 ms spikes as **seed-phase only**, not trading hot path |
| T5 | jemalloc `background_thread:true` + decay tuning | **+2.8 % throughput, −6 % p99, −17 % throughput max** |
| T7 | `mlockall(MCL_CURRENT \| MCL_FUTURE)` at startup | Bundled with T5 in the same merged branch |
| N2 | Investigate the 30 µs single-order RTT spread | **Characterized as wire/NIC-bounded, not software-actionable on this hardware.** Bench `iface.poll()` p99 = 0.17 µs, server e2e p99 = 2.1 µs — both ends are clean. The 30 µs lives in NIC silicon + PCIe DMA + switch hop + TCP framing, each contributing a few µs that sum to the observed spread. Meaningful cuts would require lower-latency NIC (Mellanox CX-6/7), cut-through switch, or UDP framing — none software-tunable. |
| N1 | Pre-size `AccountManager.balances` HashMap for bulk seed | **Eliminated the multi-hundred-ms seed spikes.** Before: 14 outliers >1 ms during seed, biggest 1146 ms near the end of 100K accounts; matching-execute histogram max 113 ms (saturated). After: 6 outliers, biggest 11 ms (a one-shot AddInstrument allocation, not the rehash phenomenon). Steady-state trading unchanged. Customer impact: failover RTO + replica catch-up no longer blocked by engine stalls during seed. |

## Still on the table — floor latency

| ID | Idea | Est. win | Effort | Notes |
|---|---|---|---|---|
| F3 | smoltcp TCP tuning: PSH every send + immediate ACK | a few µs of bench RTT | Low | Off-server (consumer side); helps client-perceived latency at window=1 |
| F4 | Strip per-iteration `tick_check_counter` work when `tick_interval_ms=0` | one branch / iter | Trivial | Confirmed not on critical path; almost cosmetic |
| F6 | Try mimalloc as alternative to jemalloc | unclear without testing | Low | Mostly mooted by T5 unless mimalloc has a meaningfully tighter purge model |

## Still on the table — tail latency (production hot path)

| ID | Idea | Est. win | Effort | Notes |
|---|---|---|---|---|
| T6 | Explicit hugepages for the engine heap (hugetlbfs / `madvise(MADV_HUGEPAGE)` despite global `transparent_hugepage=never`) | TLB miss + page-fault reduction | Medium | Could yield ms-level wins on rare paths |

## Newly identified

| ID | Idea | Win | Effort | Notes |
|---|---|---|---|---|
| N3 | Pre-size InstrumentState allocations | Eliminates the residual 5–11 ms instrument-creation spikes seen in N1's after-fix run | Low | Each AddInstrument allocates an order book + slabs + indices. Could be addressed similarly to N1: a `with_seed_capacity`-style constructor that uses pre-allocated Vec/HashMap sizing for the typical exchange shape. Lower priority — only matters at first-instrument creation, not at trading time. |

## Deprioritized after T1's findings

| ID | Idea | Why deprioritized |
|---|---|---|
| T2 | Pre-allocate `reports: Vec` to a fixed cap | Was aimed at the trading hot path. T1 confirmed the trading hot path has no >1 ms outliers — they were all seed-phase. Still good hygiene for memory bounds, just no longer urgent. |
| T3 | `prefault()` audit | T7's mlockall locks pages but doesn't pre-fault them. A page-walk audit would still help; lower priority since trading-phase tail looks clean. |
| T4 | Fixed-capacity HashMap for `live_order_ids` | Same logic as T2 — the rehash signature seen was on the *accounts* map during seed, not `live_order_ids` during trading. |
| T8 | Cap `drain_due_scheduled_tasks` per call | Hypothesis was tick-driven spikes. T1 confirmed all >1 ms execute outliers were `event_kind="app"`, not `tick`. Spikes are not tick-driven. |

## Considered and ruled out

| ID | Idea | Why ruled out |
|---|---|---|
| F1 | Fuse `dpdk_response` into the DPDK poll thread | Judged too risky vs the win; the poll loop is currently 1.2 µs/iter and adding encode work could widen its tail. |
| F2 | `idle=poll` on isolated cores in GRUB | Tested on a fresh-boot rig: **no measurable effect** on either single-order or throughput percentiles. The hot threads already busy-spin and we already gated `sched_yield` out (T7's busy_spin), so the kernel idle path almost never runs. Cost is real (100 % CPU on every isolated core, ~10 W per core extra power, hotter rig). Industry-standard for HFT/exchange deployments, but on this codebase it's a power/heat tax with no observable latency win. Reverted. |
| T10 | Disable Ethernet PAUSE frames on the PFs (`ethtool -A rx off tx off`) | Tested: **no measurable effect**. PAUSE doesn't fire at our load (single-order at 20 K/s or throughput at 5 M/s, both far below NIC saturation), so disabling it removes a class of event that wasn't happening anyway. **Non-trivial caveat:** with PAUSE off, NIC overflow events become packet drops + TCP retransmits (100s of ms tail) instead of brief stalls — strictly worse failure mode if overflow ever happens. Not worth the trade for zero observable benefit. Reverted. |

## Suggested ordering

1. **N1** — measurable customer-visible win for failover; concrete signal from T1 telling us where the work is.
2. **T6** — if N1 doesn't fully attack the page-fault axis.
3. **F3** — bench-RTT specifically. Note that N2 ruled out smoltcp processing as the source of the body of the bench-RTT spread, so F3's gain is now bounded to the consumer-side delay component (a few µs at most). Pursue only if it's cheap and we want to chase the bench number for marketing purposes.
