# Melin

Melin is a high-performance exchange core written in Rust on the [LMAX disruptor architecture](https://martinfowler.com/articles/lmax.html), built for venues that cannot compromise on correctness, durability, or performance.

Melin handles order matching, account balances, risk controls, circuit breakers, fee schedules, journaling, and replication — the critical path of an exchange, from order ingestion to durable execution.

It doesn't handle gateway concerns such as market data fan-out, client session management, per-account rate limiting, account identity and authorization, FIX/ITCH protocol translation, cold storage. These are out of scope and handled by upstream services that consume Melin's output event channel.

## Why Melin

Melin is:

**Correct** — strict price-time priority verified by property-based tests across thousands of random order sequences; cross-validated against independent matching engine implementations and real market data to surface edge cases that single-engine testing misses; deterministic replay guarantees identical state from the same journal; balance conservation enforced by proptest invariants; fuzz testing covers journal and wire protocol decoding. Hundres of test scenarios have been written to make sure Melin is correct.

**Durable** — every order is persisted (pwritev2 + RWF_DSYNC) and replicated before acknowledgement; crash recovery via journal replay with CRC32C integrity checks; BLAKE3 hash chain for tamper evidence. Melin supports dual-replication to survive and recover from major outage scenarios.

**Efficient** — single-threaded matching engine on a lock-free disruptor pipeline for maximum, with journal, matching and replication running in parallel. Melin can handle 8.1M orders/sec over LAN with local fsync, 5.8M/sec with synchronous replication, with a sub-100 µs p99.9 single-order latency.

## Architecture

```
                           ┌────────────────────────────────────────────────────────────┐
                           │                          PRIMARY                           │
                           │                                                            │
  Clients ─TCP────────────────────────► Accept Loop                                     │
                           │                │                                           │
                           │                ▼                                           │
                           │            Epoll/io_uring Reader Pool                      │
                           │            (edge-triggered, non-blocking)                  │
                           │                │                                           │
                           │                │  lock-free CAS                            │
                           │                ▼                                           │
                           │   ┌─────────────────────────────────┐                      │
                           │   │     Input Disruptor (ring buf)  │                      │
                           │   └──────────┬──────────────┬───────┘                      │
                           │              │              │                              │
                           │              ▼              ▼                              │
  ┌──────────────────┐     │   ┌──────────────┐  ┌──────────────┐                       │
  │     REPLICA      │     │   │   Journal    │  │   Matching   │  parallel consumers   │
  │                  │     │   │   Thread     │  │   Thread     │                       │
  │  replay + fsync  │◄────┼───│              │  │              │                       │
  │                  │repl │   │ pwritev2     │  │ Exchange     │                       │
  │  ack ─┐          │ring │   │ + RWF_DSYNC  │  │ .execute()   │                       │
  └───────┼──────────┘     │   └──────┬───────┘  └──────┬───────┘                       │
          │                │          │                 │                               │
          │ repl cursor    │          │ journal cursor  │ Output Disruptor Ring         │
          │                │          ▼                 ▼                               │
          │                │   ┌──────────────────────────────┐                         │
          └──────────────► │   │       Response Thread        │ consumer 0              │
                           │   │  gates on min(journal cursor,│                         │
                           │   │      repl cursor)            │                         │
                           │   └──────────────┬───────────────┘                         │
                           │                  │                                         │
                           │   ┌──────────────┴───────────────┐                         │
                           │   │    Event Publisher Thread     │ consumer 1 (optional)  │
                           │   │    (--event-bind, auth'd TCP) │                        │
                           │   └──────────────┬───────────────┘                         │
                           │                  │                                         │
                           └──────────────────┼─────────────────────────────────────────┘
                                              │
   Clients ◄─TCP──────────────────────────────┤
   Subscribers ◄─TCP──────────────────────────┘
```

- **[LMAX-style disruptor pipeline](docs/pipeline-architecture.md)** — 3 OS threads (journal, matching, response) on lock-free ring buffers; lock-free CAS-based multi-producer from reader pool; journal and matching run in parallel on the same events
- **Batch sync amortization** — under load, one sync covers many events; `pwritev2` with `RWF_DSYNC` (Force Unit Access) combines write + durability in a single syscall; `posix_fallocate` pre-allocates 256 MiB chunks so sync only flushes data pages, not extent metadata
- **Mechanical sympathy** — cache-line-padded sequences, fixed-point pricing (no floats), pre-allocated buffers with no per-order allocations on the hot path
- **Pre-allocated everything** — reservation slab (2M slots), order book indices, and balance maps are pre-sized and page-faulted at startup; jemalloc avoids glibc fragmentation

## LAN Benchmarks

All numbers are **full round-trip** (client sends order → server journals to NVMe with fsync → matching engine executes → response arrives at client). Every order is durably persisted before acknowledgement. [Realistic order flow](crates/bench/src/generator.rs). Reproducible via `scripts/lan-bench-suite.sh`. For production deployment and OS tuning, see [operations](docs/operations.md) and [benchmarking](docs/benchmarking.md).

### Peak throughput (16 clients, window 256)

Kernel TCP over 10 Gbps private VLAN. Two or three Cherry AMD Ryzen 9 9950X servers (16C, SMT off, dedicated NVMe journal). Commit [`ed9241d`](../../commit/ed9241d).

| Durability | Throughput | p50 | p99 | p99.9 | max |
|------------|-----------|-----|-----|-------|-----|
| **Local fsync** | **8.1M/s** | 439 µs | 569 µs | 636 µs | 1,017 µs |
| **Synchronous replication** (fsync + replica ack) | **5.8M/s** | 633 µs | 841 µs | 933 µs | 1,123 µs |

### Single-order latency (1 client, window 1)

The latency floor — one order at a time, no pipelining, no queuing.

| Transport | p50 | p90 | p99 | max | Hardware |
|-----------|-----|-----|-----|-----|----------|
| Kernel TCP | 72 µs | 87 µs | 90 µs | 207 µs | Ryzen 9 9950X, 10 GbE |
| **DPDK kernel bypass** | **37 µs** | **38 µs** | **101 µs** | 1,775 µs | EPYC 4564P, Intel 82599 10 GbE SR-IOV |

The DPDK result is an early experimental measurement with end-to-end kernel bypass (both client and server) on budget server-class hardware — not purpose-built low-latency infrastructure and with SR-IOV (dedicated or bifurcated would be better). 47% p50 reduction vs kernel TCP on the same machines.

**Latency CDF** — peak-load modes on the same axes:

![Latency CDF](docs/plots/latency-cdf.svg)

**Latency stability over time** (p99.99, replication mode):

![Latency stability — replication](docs/plots/latency-stability-replication.svg)

### Bottleneck and next steps

The TCP network stack is the primary throughput limiter. The journal pipeline hides fsync latency at high pipelining depths. DPDK kernel bypass (landed, experimental) halves single-order p50 latency; further transport tuning is the main remaining optimization vector.

## Features

### Order Types
- Market, Limit, Stop (stop-loss), Stop-Limit
- Time-in-force: GTC, IOC, FOK, Day, GTD (Good-Til-Date)
- Post-Only (maker-only, reject if would take)

### [Matching Engine](docs/matching-engine.md)
- Strict price-time priority (sorted Vec + binary search order book)
- Execution reports: Fill (with fees), Placed, Triggered, Cancelled, Rejected, Replaced, InstrumentStatusChanged
- Multi-instrument exchange with shared account balances
- Cancel-replace / order amendment (atomic price/qty modify; preserves queue priority when price unchanged, loses priority on price change)
- Circuit breakers (price bands, trading halts — per-instrument `CircuitBreakerConfig`)
- Instrument lifecycle management (disable/enable/remove — disable cancels all resting orders atomically, remove reclaims memory)

### [Fees](docs/fee-model.md)
- Maker/taker fee model (per-instrument `FeeSchedule` in basis points, configurable via admin API)
- Fee deduction on fill (fees in quote currency, deducted from buyer reservation and seller proceeds, reported in `ExecutionReport::Fill`)
- Collected fees credited to a dedicated fee account — operators can withdraw via admin API; balance conservation enforced across all accounts

### [Risk & Accounting](docs/risk-checks.md)
- Per-account, per-currency balance management (reserve on order, update on fill, release on cancel)
- Self-trade prevention (per-order modes: CancelNewest, CancelOldest, CancelBoth)
- Fat finger checks (max order size, max notional value — per-instrument configurable `RiskLimits`)
- Kill switch (cancel all resting orders and pending stops for an account across all instruments)
- Per-account OrderId high-water mark (prevents double-execution on crash-recovery retry)
- Price band checks (static lower/upper bounds, per-instrument — part of circuit breaker config)
- Withdraw event (debit funds, auto-evict zero-balance entries)

### [Event Sourcing & Durability](docs/journal.md)
- Write-ahead journal with CRC32C checksums and BLAKE3 hash chain (tamper evidence, replica consistency)
- Persist-before-ack: matching latency overlapped against journal writes, acknowledgement gated on confirmed durability
- Batch journal I/O via LMAX disruptor ring buffer pipeline (`pwritev2` + `RWF_DSYNC`)
- Pre-allocated storage (`posix_fallocate`) for reduced fsync latency
- Snapshot save/load for fast recovery; journal rotation (automatic snapshot + archive when size threshold exceeded)
- Deterministic replay from journal for crash recovery and audit
- Scheduled snapshots via shadow exchange — periodic snapshots on a dedicated thread without pausing the matching engine (`--snapshot-interval-secs`)

### [Replication & High Availability](docs/replication.md)
- Synchronous dual replication — live WAL streaming to 2 replicas via lock-free ring buffer; replicas fsync and ack before the primary sends responses to clients (zero acknowledged data loss)
- Journal catch-up — new replicas automatically catch up from the primary's journal files before switching to live streaming; enables replica replacement with zero downtime
- Snapshot transfer — when journal archives have been purged, the primary transfers its snapshot over the replication channel; the replica loads the snapshot and resumes from there
- Automatic trading halt when all replicas disconnect — trading continues with at least one replica; resumes instantly on reconnect
- Manual promotion — operator sends `PROMOTE` to the replica's trigger endpoint; in-process transition reuses the warm Exchange state with zero re-replay, sub-second switchover
- Multi-process failover tests — SIGKILL primary under load, promote replica, verify no data loss and clients can reconnect

### [Networking](docs/wire-protocol.md)
- Custom binary wire protocol (length-prefixed framing)
- TCP, Unix domain socket, and DPDK kernel bypass transports
- Epoll reader pool (edge-triggered, non-blocking) with dedicated I/O threads (zero tokio)
- io_uring transport (separate read/write rings, multishot RECV with provided buffer groups)
- Backpressure handling (explicit `ServerBusy` reject when the input pipeline is full — client should back off and retry)
- Output event channel — real-time broadcast of all execution events to authenticated TCP subscribers via `--event-bind`; per-frame monotonic sequence numbers for gap detection; slow subscriber disconnect

### [Authentication & Authorization](docs/admin-guide.md)
- Ed25519 challenge-response handshake
- Four permission roles: Operator (exchange configuration), Trader (order submission/cancellation), Custodian (deposit/withdraw), ReadOnly (heartbeats)
- Operator API (instrument management and lifecycle, circuit breakers, kill switch, risk limits, fee schedules, end-of-day, live stats dashboard)
- Per-key idempotency (sequence numbers with duplicate rejection — safe to retry on timeout without double-applying)

### [Operations](docs/operations.md)
- Structured logging (`tracing` crate, error-level for server malfunctions only)
- Health/liveness TCP endpoint (`--health-bind`) with Prometheus `/metrics` endpoint (active connections, events processed, journal sequence, replication lag, pipeline health, input queue depth, trading state)
- Admin TUI dashboard (live connection count, events processed, throughput, journal sequence)
- Sparse account storage to reduce memory usage, see [account lifecycle](docs/account-lifecycle.md)

### Testing
- Property-based tests (proptest): price-time priority, balance conservation, volume conservation, reservation consistency, no self-trades under STP, deterministic replay, overflow safety, cancel-replace invariants, fee accounting
- Cross-engine differential testing against independent matching engine implementations and a naive reference oracle (100K+ randomized events)
- Fuzz testing (bolero): journal codec, wire protocol codec
- Crash injection tests: truncation at every byte offset, during snapshot rotation, under realistic load, across multiple rotation cycles
- Multi-process failover tests: SIGKILL primary, promote replica, dual-replication failover, journal catch-up for replacement replicas, verify state consistency and no data loss
- Integration tests: snapshot round-trip, journal replay, shadow stage determinism

## License

Copyright (c) 2026 Pierre Larger. All Rights Reserved.

Commercial licensing available — contact [pierre.larger@gmail.com](mailto:pierre.larger@gmail.com).
