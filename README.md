# Matching Engine

A matching engine built on the [LMAX architecture](https://martinfowler.com/articles/lmax.html), written in Rust.

## Architecture

```
                      ┌─────────────────────────────────────────────────────────────┐
                      │                         SERVER                              │
                      │                                                             │
  Clients ─TCP───────────────────► Accept Loop                                      │
                      │                │                                            │
                      │                ▼                                            │
                      │            Epoll/io_uring Reader Pool                       │
                      │            (edge-triggered, non-blocking)                   │
                      │                │                                            │
                      │                │  lock-free CAS                             │
                      │                ▼                                            │
                      │   ┌─────────────────────────────────┐                       │
                      │   │     Input Disruptor (ring buf)  │                       │
                      │   └──────────┬──────────────┬───────┘                       │
                      │              │              │                               │
                      │              ▼              ▼                               │
                      │   ┌──────────────┐  ┌──────────────┐                        │
                      │   │   Journal    │  │   Matching   │   parallel consumers   │
                      │   │   Thread     │  │   Thread     │                        │
                      │   │              │  │              │                        │
                      │   │ pwritev2     │  │ Exchange     │                        │
                      │   │ + RWF_DSYNC  │  │ .execute()   │                        │
                      │   └──────┬───────┘  └──────┬───────┘                        │
                      │          │                 │                                │
                      │          │ cursor          │ output SPSC                    │
                      │          ▼                 ▼                                │
                      │   ┌──────────────────────────────┐                          │
                      │   │       Response Thread        │                          │
                      │   │                              │                          │
                      │   │  gates on journal cursor     │                          │
                      │   │  (persist-before-ack)        │                          │
                      │   └──────────────┬───────────────┘                          │
                      │                  │                                          │
                      └──────────────────┼──────────────────────────────────────────┘
                                         │
                                         │
  Clients ◄─TCP──────────────────────────┘
```

- **Single-threaded matching engine** — no locks on the hot path; one thread executes all matching logic
- **LMAX-style disruptor pipeline** — 3 OS threads (journal, matching, response) on lock-free ring buffers; lock-free CAS-based multi-producer from reader pool; journal and matching run in parallel on the same events
- **Persist-before-ack** — pipelined journal I/O with full durability guarantee; matching latency overlapped against journal writes, acknowledgement gated on confirmed durability, not optimistically sent
- **Batch sync amortization** — under load, one sync covers many events; `pwritev2` with `RWF_DSYNC` (Force Unit Access) combines write + durability in a single syscall; `posix_fallocate` pre-allocates 64 MiB chunks so sync only flushes data pages, not extent metadata
- **Event sourcing** — deterministic replay for crash recovery and audit; snapshots for fast restart
- **Mechanical sympathy** — cache-line-padded sequences, fixed-point pricing (no floats), pre-allocated buffers with no per-order allocations on the hot path

## Features

Checklist of features expected of a production trade execution engine. Items marked with **[x]** are implemented; **[ ]** are planned.

### Order Types
- [x] Market
- [x] Limit
- [x] Stop (stop-loss)
- [x] Stop-Limit
- [ ] Iceberg (hidden quantity)

### Time-in-Force
- [x] GTC (Good-Til-Cancelled)
- [x] IOC (Immediate-Or-Cancel)
- [x] FOK (Fill-Or-Kill)
- [ ] GTD (Good-Til-Date)
- [ ] Day

### Execution Qualifiers
- [ ] Post-Only (maker-only, reject if would take)

### Matching Engine
- [x] Strict price-time priority (BTreeMap + VecDeque order book)
- [x] Execution reports: Fill, Placed, Triggered, Cancelled, Rejected
- [x] Multi-instrument exchange with shared account balances
- [ ] Cancel-replace / order amendment (atomic modify without losing queue priority for unchanged price)
- [x] Circuit breakers (price bands, trading halts — per-instrument `CircuitBreakerConfig`)
- [ ] Auction mechanisms (opening/closing/volatility auctions)

### Fees
- [ ] Maker/taker fee model (configurable per instrument or tier)
- [ ] Fee deduction on fill (deduct from proceeds, include in ExecutionReport)
- [ ] Fee schedules (volume-based tiers, account-level overrides)

### Risk & Accounting
- [x] Per-account, per-currency balance management (reserve on order, update on fill, release on cancel)
- [x] Self-trade prevention (per-order modes: CancelNewest, CancelOldest, CancelBoth)
- [x] Fat finger checks (max order size, max notional value — per-instrument configurable `RiskLimits`)
- [x] Kill switch (cancel all resting orders and pending stops for an account across all instruments)
- [x] Client deduplication (per-account OrderId high-water mark — prevents double-execution on crash-recovery retry)
- [x] Price band checks (static lower/upper bounds, per-instrument — part of circuit breaker config)
- [ ] Position/exposure limits
- [ ] Order throttling (per-account rate limiting)

### Event Sourcing & Durability ([docs/journal.md](docs/journal.md))
- [x] Write-ahead journal with CRC32C checksums
- [x] Batch journal I/O via LMAX disruptor ring buffer pipeline
- [x] Pre-allocated storage (`posix_fallocate`) for reduced fsync latency
- [x] Snapshot save/load for fast recovery
- [x] Deterministic replay from journal
- [x] Pipelined io_uring async fsync with group commit
- [ ] Journal rotation
- [ ] Journal compaction (automatic snapshot trigger)
- [ ] Output event log (durable ExecutionReport stream for audit trail)

### Networking
- [x] Custom binary wire protocol (length-prefixed framing)
- [x] TCP transport with `TCP_NODELAY`
- [x] Unix domain socket transport
- [x] Epoll reader pool (edge-triggered, non-blocking) with dedicated I/O threads (zero tokio)
- [x] Lock-free CAS-based multi-producer disruptor (no mutex on input path)
- [x] io_uring transport (separate read/write rings, multishot RECV with provided buffer groups)
- [x] Typed client library
- [x] Terminal UI for interactive testing
- [x] Heartbeats and connection timeouts (bidirectional keepalive, configurable idle timeout detection)
- [ ] Backpressure handling (defined policy when disruptor is full)
- [ ] TLS (encrypted client connections)

### Gateway
- [x] TCP proxy between clients and engine (binary protocol)
- [ ] Scalable I/O model (epoll/io_uring multiplexing — current 2-threads-per-client caps at ~500 connections)
- [ ] Output event channel from matching stage (broadcast — prerequisite for market data)
- [ ] Market data dissemination (L2 snapshots, trade feed, BBO push updates)
- [ ] Subscription management (subscribe/unsubscribe per instrument)
- [ ] Reference data management (instrument lifecycle)
- [ ] Rate limiting and connection management (per-client throttling)

### Authentication & Authorization
- [x] Client authentication (Ed25519 challenge-response handshake)
- [ ] Per-account trading permissions
- [x] Admin API (instrument management, deposits, circuit breaker controls, kill switch, risk limits)

### Operations & Reliability
- [x] Structured logging (`tracing` crate, error-level for server malfunctions only)
- [x] Per-stage pipeline latency tracing (`latency-trace` feature gate)
- [x] Configuration management (CLI args for bind address, journal path, core affinity, reader threads)
- [x] Graceful shutdown (SIGINT/SIGTERM handler, ordered drain: readers → journal → matching → response)
- [x] Health checks / readiness probes (`ServerReady` wire handshake on connect)

### Metrics & Observability

Most analytics can run on a **replica** replaying the journal, keeping the primary's hot path free of instrumentation jitter.

#### Primary node (lightweight, operational health)
- [x] Pipeline stage utilization (`pipeline-stats` feature gate — busy/idle ratio per stage)
- [ ] Metrics transport (decide where/how to expose: stats file, output event channel, Prometheus endpoint, or admin socket — must not touch the hot path)
- [ ] Connection counts (active clients, connects/disconnects per second)
- [ ] Disruptor queue depth / backpressure monitoring (input ring fill level)
- [ ] Health/liveness endpoint (beyond current `ServerReady` handshake)

#### Replica or offline (journal-derived, zero primary impact)
- [ ] Order/fill/cancel throughput counters (events per second by type)
- [ ] Latency histograms (journal `timestamp_ns` → matching → response, per-event)
- [ ] Volume analytics (traded volume per instrument, per account)
- [ ] Book depth analytics (resting order counts, spread tracking)
- [ ] Audit trail queries (full event history for regulatory compliance)
- [ ] Fee/PnL accounting (when fees and position tracking exist)

### Testing
- [x] `proptest` invariant tests (price-time priority, volume conservation, balance conservation, book/reservation/order-sides consistency, overflow safety, STP enforcement — all order types including stops, all STP modes, circuit breaker toggling, cancel-all)
- [x] Verified `price × quantity` intermediate calculations don't overflow `u64` (use `u128` for computed values)
- [x] Bolero fuzz tests for journal and wire protocol codecs (decode crash discovery + encode/decode round-trip)
- [x] Security audit ([docs/security-audit.md](docs/security-audit.md))

### Redundancy & High Availability
- [ ] Journal replication (WAL streaming to replica; sync for zero data loss, async for lower latency)
- [ ] State machine replication (deterministic replay on replica)
- [ ] Failover detection and promotion (leader election, split-brain prevention)
- [ ] Client failover (reconnect to new primary, resume with sequence numbers)
- [ ] Network partition handling (fencing, quorum-based decisions)

## Priority Roadmap

Ordered by importance for commercial readiness (exchange operators and investors).

1. ~~**Circuit breakers**~~ ✅ — price bands, trading halts. Fully integrated with event sourcing.
2. **Cancel-replace / order amendment** — market makers won't evaluate without it. Most active order flow on any venue.
3. **Replication & HA** — journal streaming to a replica, deterministic replay, failover. No exchange runs a single node.
4. ~~**Fuzz testing**~~ ✅ — proptest coverage extended to all order types, STP modes, circuit breakers, stops. Found and fixed a reservation leak on price-improved fills.
5. **Journal rotation + compaction** — unbounded disk growth is a non-starter operationally.
6. ~~**Authentication**~~ ✅ — Ed25519 challenge-response. Admin API for instrument/deposit/risk/circuit-breaker management.
7. **TLS** — encrypted client connections. Required for non-VLAN deployments.
8. **Metrics & observability** — connection counts, queue depth, health endpoints. Operators need visibility.
9. **Auction mechanisms** — opening/closing/volatility auctions. Differentiator for regulated venues.
10. **Fee model** — maker/taker fees on fills. Shows the engine can generate revenue.
11. **Documentation** — architecture guide, API reference, operational runbook.
12. **Security hardening** — remaining [audit findings](docs/security-audit.md): per-account order limits (SEC-03), order throttling (SEC-04), disk exhaustion handling (SEC-05), snapshot validation (SEC-09).

Also needed: backpressure policy, gateway scalability (epoll/io_uring multiplexing), per-account permissions.

### Benchmarking & Measurements
- [x] Realistic order flow generator (power-law prices/sizes, cancels, fills, multiple accounts, STP diversity)
- [x] Multi-threaded io_uring benchmark client (`--bench-threads`)
- [x] JSON output for machine-readable results (`--json`)
- [x] TUI charts: tail latency stability and latency histogram (`--features chart`)
- [x] Dynamic percentile depth based on sample size
- [ ] Saturation curve — sweep `--clients` and `--window`, plot latency vs throughput from JSON output
- [ ] Multi-machine benchmark — run bench from multiple machines simultaneously (`--account-id`, `--order-id-offset`)
- [ ] Real-world data replay (NASDAQ ITCH 5.0, Databento, Lobster — legal review needed)

### Performance Tuning
- [x] Release profile: `lto = "fat"`, `codegen-units = 1`, `panic = "abort"`, `target-cpu=native`
- [x] jemalloc (`tikv-jemallocator`)
- [x] CPU core pinning for all pipeline, reader, and bench threads
- [x] IRQ affinity pinning (`bench-isolate.sh`)
- [x] Kernel boot isolation (`isolcpus`, `nohz_full`, `rcu_nocbs`)

## Project Structure

```
crates/
├── disruptor/     Lock-free ring buffers (generic, no trading-domain knowledge)
├── engine/        Matching engine, order books, event sourcing, journal pipeline
├── protocol/      Binary wire protocol, transport abstractions, blocking I/O
├── server/        Server, pipeline orchestration, dedicated I/O threads
├── admin/         CLI admin tool (instrument management, deposits, circuit breakers)
├── bench/         Benchmark suite (engine, pipeline, and full round-trip modes)
├── client/        Typed client library
└── tui/           Terminal UI for interactive testing
```

## Performance

LAN round-trip benchmarks. Two Cherry AMD Ryzen 9950X servers (16C/32T, 192 GB RAM, 2x 1TB NVMe, 10 Gbps). Engine on one server with journal on a dedicated NVMe disk, benchmark client on the other, TCP over private network. [Realistic order flow](crates/bench/).

**Peak-load throughput** — full durability, 100M order pairs, 16 clients, 256 pipelined:

| Metric | Value |
|--------|-------|
| **Throughput** | 5.2M orders/sec |
| **p90** | 788 µs |
| **p99** | 870 µs |
| **p99.9** | 939 µs |
| **p99.99** | 1.25 ms |
| **p99.999** | 1.42 ms |
| **p99.9999** | 1.46 ms |
| **max** | 1.47 ms |

```sh
./trading-server --bind 0.0.0.0:9876 --journal /mnt/journal/trading.journal  # engine server
./trading-bench 100000000 --addr <engine-ip>:9876 --window=256               # bench client
```

**Peak-load throughput** — no persistence, 100M order pairs, 32 clients, 192 pipelined:

| Metric | Value |
|--------|-------|
| **Throughput** | 11.2M orders/sec |
| **p90** | 593 µs |
| **p99** | 645 µs |
| **p99.9** | 747 µs |
| **p99.99** | 790 µs |
| **p99.999** | 821 µs |
| **p99.9999** | 877 µs |
| **p99.99999** | 913 µs |
| **max** | 915 µs |

```sh
./trading-bench 100000000 --addr <engine-ip>:9876 --window=192 --clients=32  # no-persist server
```

**Single-order latency** — full durability, 1 client, no pipelining, 1M order pairs:

| Metric | Value |
|--------|-------|
| **Throughput** | 14.1K orders/sec |
| **p50** | 70 µs |
| **p99** | 126 µs |
| **p99.99** | 134 µs |
| **max** | 689 µs |

```sh
./trading-bench 1000000 --addr <engine-ip>:9876 --window=1 --clients=1
```

**Engine-only** — no pipeline, no network, 100M order pairs on the Ryzen 9950X:

| Metric | Value |
|--------|-------|
| **Throughput** | 17.3M orders/sec |
| **p90** | 0.05 µs |
| **p99** | 0.06 µs |
| **p99.9** | 0.06 µs |
| **p99.99** | 0.12 µs |
| **p99.999** | 0.61 µs |
| **p99.9999** | 3.31 µs |
| **p99.99999** | 18.93 µs |
| **max** | 121.92 µs |

```sh
./trading-bench 100000000 --mode=engine
```

## License

Copyright (c) 2026 Pierre Larger. All Rights Reserved.
