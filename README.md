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
- [ ] Iceberg (hidden quantity)- [ ] Trailing Stop- [ ] OCO (One-Cancels-Other)- [ ] Bracket (entry + take-profit + stop-loss)
### Time-in-Force
- [x] GTC (Good-Til-Cancelled)
- [x] IOC (Immediate-Or-Cancel)
- [x] FOK (Fill-Or-Kill)
- [ ] GTD (Good-Til-Date)- [ ] Day
### Execution Qualifiers
- [ ] Post-Only (maker-only, reject if would take)- [ ] Reduce-Only (only decrease position size)
### Matching Engine
- [x] Strict price-time priority (BTreeMap + VecDeque order book)
- [x] Execution reports: Fill, Placed, Triggered, Cancelled, Rejected
- [x] Multi-instrument exchange with shared account balances
- [ ] Cancel-replace / order amendment- [ ] Circuit breakers (price bands, trading halts)- [ ] Auction mechanisms (opening/closing/volatility auctions)
### Fees
- [ ] Maker/taker fee model- [ ] Fee deduction on fill- [ ] Fee schedules (volume-based tiers)
### Risk & Accounting
- [x] Per-account, per-currency balance management (reserve on order, update on fill, release on cancel)
- [x] Self-trade prevention (per-order modes: CancelNewest, CancelOldest, CancelBoth)
- [x] Fat finger checks (max order size, max notional value — per-instrument configurable `RiskLimits`)
- [x] Kill switch (cancel all resting orders and pending stops for an account across all instruments)
- [x] Client deduplication (per-account OrderId high-water mark — prevents double-execution on crash-recovery retry)
- [ ] Price band checks (reject orders too far from reference price)- [ ] Order throttling (per-account rate limiting)- [ ] Position/exposure limits
### Event Sourcing & Durability ([docs/journal.md](docs/journal.md))
- [x] Write-ahead journal with CRC32C checksums
- [x] Batch journal I/O via LMAX disruptor ring buffer pipeline
- [x] Pre-allocated storage (`posix_fallocate`) for reduced fsync latency
- [x] Snapshot save/load for fast recovery
- [x] Deterministic replay from journal
- [x] Pipelined io_uring async fsync with group commit
- [ ] Journal rotation- [ ] Journal compaction (automatic snapshot trigger)- [ ] Output event log (durable ExecutionReport stream for audit trail)
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
- [ ] Backpressure handling (defined policy when disruptor is full)- [ ] TLS (encrypted client connections)- [ ] DDoS protection (connection rate limiting, per-IP limits, max connections cap)- [ ] QUIC transport- [ ] Kernel bypass (DPDK/ef_vi)
### Gateway
- [x] TCP proxy between clients and engine (binary protocol)
- [ ] Scalable I/O model (epoll/io_uring multiplexing)- [ ] Market data dissemination (L2 snapshots, trade feed, BBO push updates)- [ ] Subscription management (subscribe/unsubscribe per instrument)- [ ] Reference data management (instrument lifecycle)
### Authentication & Authorization
- [ ] Client authentication- [ ] Per-account trading permissions- [ ] Admin API (instrument management, circuit breaker controls, kill switch)
### Operations & Reliability
- [x] Structured logging (`tracing` crate)
- [x] Per-stage pipeline latency tracing (`latency-trace` feature gate)
- [x] Configuration management (CLI args for bind address, journal path, core affinity, reader threads)
- [x] Graceful shutdown (SIGINT/SIGTERM handler, ordered drain: readers → journal → matching → response)
- [x] Health checks / readiness probes (`ServerReady` wire handshake on connect)

### Metrics & Observability
- [x] Pipeline stage utilization (`pipeline-stats` feature gate — busy/idle ratio per stage)
- [ ] Metrics transport (decide where/how to expose: stats file, output event channel, admin socket)
- [ ] Connection counts, disruptor queue depth — *primary node only*
- [ ] Order/fill/cancel throughput, latency histograms, volume analytics — *replica or offline (journal-derived, zero primary impact)*

### Redundancy & High Availability
- [ ] Journal replication (WAL streaming to replica)- [ ] State machine replication (deterministic replay on replica)- [ ] Failover detection and promotion (leader election, split-brain prevention)- [ ] Client failover (reconnect to new primary, resume with sequence numbers)
### Horizontal Scaling
- [ ] Instrument sharding (partition instruments across engine instances, each single-threaded)- [ ] Cross-shard routing (gateway routes orders to the correct shard by symbol)- [ ] Cross-shard risk checks (portfolio-level margin requires message passing between shards)
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
