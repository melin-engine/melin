# Melin

Melin is a high-performance exchange core written in Rust on the [LMAX disruptor architecture](https://martinfowler.com/articles/lmax.html), built for venues that cannot compromise on correctness, durability, or performance.

Melin handles order matching, account balances, risk controls, circuit breakers, fee schedules, journaling, and replication — the critical path of an exchange, from order ingestion to durable execution. Gateway concerns (market data fan-out, session management, protocol translation) are out of scope and handled by upstream services that consume Melin's output event channel.

## Why Melin

**Correct** — every order matches exactly where it should, every time.
- Strict price-time priority verified by property-based tests across thousands of random order sequences
- Cross-validated against independent matching engine implementations and real market data
- Deterministic replay guarantees identical state from the same journal
- Verified by property-based, fuzz, crash-injection, cross-engine differential, and multi-process failover tests — hundreds of scenarios in total

**Durable** — every order is persisted and replicated before acknowledgement.
- Crash recovery via journal replay with CRC32C integrity checks
- BLAKE3 hash chain for tamper evidence
- Dual-replication to survive and recover from major outage scenarios

**Efficient** — 4.5M orders/sec with synchronous dual replication on regular datacenter hardware.
- Single-threaded matching engine on a lock-free disruptor pipeline
- Journal, matching, and replication run in parallel via io_uring
- Sub-100 µs p99.9 single-order latency with dual replication

## LAN Benchmarks

All numbers are **full round-trip** (client sends order → server journals to NVMe with fsync → matching engine executes → response arrives at client). Every order is durably persisted before acknowledgement. [Realistic order flow](crates/bench/src/generator.rs). Reproducible via `scripts/lan-bench-suite.sh`. For production deployment and OS tuning, see [operations](docs/operations.md) and [benchmarking](docs/benchmarking.md).

### Peak throughput (16 clients, window 256)

Kernel TCP over 10 Gbps private VLAN. Three AMD Ryzen 9 9950X servers (16C, SMT off, dedicated NVMe journal). Commit [`d7bab52`](../../commit/d7bab52).

| Durability | Throughput | p50 | p99 | p99.9 | max |
|------------|-----------|-----|-----|-------|-----|
| **Local fsync** | **6.7M/s** | 533 µs | 671 µs | 717 µs | 2,302 µs |
| **Synchronous replication** (1 replica) | **4.0M/s** | 896 µs | 1,133 µs | 1,371 µs | 3,463 µs |
| **Dual synchronous replication** (2 replicas) | **4.5M/s** | 867 µs | 1,040 µs | 1,150 µs | 2,204 µs |

Dual replication is the typical production setup for the strongest durability guarantees. It is faster than single replication because the primary only waits for the fastest replica's ack before responding to the client.

### Single-order latency (1 client, window 1)

The latency floor — one order at a time, no pipelining, no queuing.

| Durability | p50 | p90 | p99 | max |
|-----------|-----|-----|-----|-----|
| Kernel TCP (standalone) | 56 µs | 57 µs | 68 µs | 1,041 µs |
| **Synchronous replication** (1 replica) | 55 µs | 62 µs | 66 µs | 168 µs |
| **Dual synchronous replication** (2 replicas) | **55 µs** | **64 µs** | **71 µs** | 891 µs |

**Latency CDF** — peak-load modes on the same axes:

![Latency CDF](docs/plots/latency-cdf.png)

**Latency stability over time** (p99.99, dual replication throughput mode):

![Latency stability — dual replication](docs/plots/latency-stability-tcp-dual-repl-throughput.png)

### Going further

- **DPDK kernel bypass** for both client and replication transport is under active experimentation and should bring significant latency and throughput improvements by eliminating kernel TCP overhead entirely.
- **SPDK** and **dual-NVMe hedged writes** are being evaluated to reduce journal fsync tail latency.
- **Instrument-level sharding** of the matching engine across multiple cores would lift the single-threaded matching bottleneck for workloads spanning many independent order books.

## Features

### Order Types
- Market, Limit, Stop (stop-loss), Stop-Limit
- Time-in-force: GTC, IOC, FOK, Day, GTD (Good-Til-Date)
- Post-Only (maker-only, reject if would take)

### [Matching Engine](docs/matching-engine.md)
- Strict price-time priority
- Execution reports: Fill (with fees), Placed, Triggered, Cancelled, Rejected, Replaced, InstrumentStatusChanged
- Multi-instrument exchange with shared account balances
- Cancel-replace / order amendment (atomic price/qty modify; preserves queue priority when price unchanged, loses priority on price change)
- Circuit breakers (price bands, trading halts — configurable per instrument)
- Instrument lifecycle management (disable/enable/remove — disable cancels all resting orders atomically, remove reclaims memory)

### [Fees](docs/fee-model.md)
- Maker/taker fee model (per-instrument, in basis points, configurable via admin API)
- Fee deduction on fill (fees in quote currency, deducted from buyer reservation and seller proceeds)
- Collected fees credited to a dedicated fee account — operators can withdraw via admin API; balance conservation enforced across all accounts

### [Risk & Accounting](docs/risk-checks.md)
- Per-account, per-currency balance management (reserve on order, update on fill, release on cancel)
- Self-trade prevention (per-order modes: CancelNewest, CancelOldest, CancelBoth)
- Fat finger checks (max order size, max notional value — configurable per instrument)
- Kill switch (cancel all resting orders and pending stops for an account across all instruments)
- Per-account order ID high-water mark (prevents double-execution on crash-recovery retry)
- Price band checks (static lower/upper bounds, per-instrument)
- Withdraw (debit funds, auto-evict zero-balance entries)

### [Event Sourcing & Durability](docs/journal.md)
- Write-ahead journal with CRC32C checksums and BLAKE3 hash chain (tamper evidence, replica consistency)
- Persist-before-ack: matching overlapped against journal writes, acknowledgement gated on confirmed durability
- Batch journal I/O with pre-allocated storage for reduced fsync latency
- Snapshot save/load for fast recovery; journal rotation (automatic snapshot + archive when size threshold exceeded)
- Deterministic replay from journal for crash recovery and audit
- Scheduled snapshots on a dedicated thread without pausing the matching engine

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
- io_uring transport with dedicated I/O threads (multishot RECV, batched SEND)
- Backpressure handling (reject when the input pipeline is full — client backs off and retries)
- Output event channel — real-time broadcast of all execution events to authenticated subscribers; monotonic sequence numbers for gap detection

### [Authentication & Authorization](docs/admin-guide.md)
- Ed25519 challenge-response handshake
- Four permission roles: Operator (exchange configuration), Trader (order submission/cancellation), Custodian (deposit/withdraw), ReadOnly (heartbeats)
- Operator API (instrument management and lifecycle, circuit breakers, kill switch, risk limits, fee schedules, end-of-day, live stats dashboard)
- Per-key idempotency (sequence numbers with duplicate rejection — safe to retry on timeout without double-applying)

### [Operations](docs/operations.md)
- Structured logging (error-level for server malfunctions only)
- Health/liveness endpoint with Prometheus metrics (active connections, events processed, journal sequence, replication lag, pipeline health, input queue depth, trading state)
- Admin TUI dashboard (live connection count, events processed, throughput, journal sequence)
- Sparse account storage to reduce memory usage, see [account lifecycle](docs/account-lifecycle.md)

## License

Copyright (c) 2026 Pierre Larger. All Rights Reserved.

Commercial licensing available — contact [pierre.larger@gmail.com](mailto:pierre.larger@gmail.com).
