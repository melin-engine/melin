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
                      │          │ cursor           │ output SPSC                   │
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
- **LMAX disruptor pipeline** — 3 OS threads (journal, matching, response) on lock-free ring buffers; lock-free CAS-based multi-producer from reader pool; journal and matching run in parallel on the same events
- **Persist-before-ack** — pipelined journal I/O with full durability guarantee; matching latency overlapped against journal writes, acknowledgement gated on confirmed durability, not optimistically sent
- **Batch sync amortization** — under load, one sync covers many events; `pwritev2` with `RWF_DSYNC` (Force Unit Access) combines write + durability in a single syscall; `posix_fallocate` pre-allocates 64 MiB chunks so sync only flushes data pages, not extent metadata
- **Event sourcing** — deterministic replay for crash recovery and audit; snapshots for fast restart
- **Mechanical sympathy** — cache-line-padded sequences, fixed-point pricing (no floats), zero allocations on the hot path

## Features

Checklist of features expected of a production trade execution engine. Items marked with **[x]** are implemented; **[ ]** are planned; *deferred* items are not needed for the LAN benchmark demo.

### Order Types
- [x] Market
- [x] Limit
- [x] Stop (stop-loss)
- [x] Stop-Limit
- [ ] Iceberg (hidden quantity) — *deferred*
- [ ] Trailing Stop — *deferred*
- [ ] OCO (One-Cancels-Other) — *deferred*
- [ ] Bracket (entry + take-profit + stop-loss) — *deferred*

### Time-in-Force
- [x] GTC (Good-Til-Cancelled)
- [x] IOC (Immediate-Or-Cancel)
- [x] FOK (Fill-Or-Kill)
- [ ] GTD (Good-Til-Date) — *deferred*
- [ ] Day — *deferred*

### Execution Qualifiers
- [ ] Post-Only (maker-only, reject if would take) — *deferred*
- [ ] Reduce-Only (only decrease position size) — *deferred*

### Matching Engine
- [x] Strict price-time priority (BTreeMap + VecDeque order book)
- [x] Execution reports: Fill, Placed, Triggered, Cancelled, Rejected
- [x] Multi-instrument exchange with shared account balances
- [ ] Cancel-replace / order amendment — *deferred*
- [ ] Circuit breakers (price bands, trading halts) — *deferred*
- [ ] Auction mechanisms (opening/closing/volatility auctions) — *deferred*

### Fees
- [ ] Maker/taker fee model — *deferred*
- [ ] Fee deduction on fill — *deferred*
- [ ] Fee schedules (volume-based tiers) — *deferred*

### Risk & Accounting
- [x] Per-account, per-currency balance management (reserve on order, update on fill, release on cancel)
- [x] Self-trade prevention (per-order modes: CancelNewest, CancelOldest, CancelBoth)
- [x] Fat finger checks (max order size, max notional value — per-instrument configurable `RiskLimits`)
- [x] Kill switch (cancel all resting orders and pending stops for an account across all instruments)
- [x] Client deduplication (per-account OrderId high-water mark — prevents double-execution on crash-recovery retry)
- [ ] Price band checks (reject orders too far from reference price) — *deferred*
- [ ] Order throttling (per-account rate limiting) — *deferred*
- [ ] Position/exposure limits — *deferred*

### Event Sourcing & Durability ([docs/journal.md](docs/journal.md))
- [x] Write-ahead journal with CRC32C checksums
- [x] Batch journal I/O via LMAX disruptor ring buffer pipeline
- [x] Pre-allocated storage (`posix_fallocate`) for reduced fsync latency
- [x] Snapshot save/load for fast recovery
- [x] Deterministic replay from journal
- [x] Pipelined io_uring async fsync with group commit
- [ ] Journal rotation — *deferred*
- [ ] Journal compaction (automatic snapshot trigger) — *deferred*
- [ ] Output event log (durable ExecutionReport stream for audit trail) — *deferred*

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
- [ ] Backpressure handling (defined policy when disruptor is full) — *deferred*
- [ ] TLS (encrypted client connections) — *deferred*
- [ ] DDoS protection (connection rate limiting, per-IP limits, max connections cap) — *deferred*
- [ ] QUIC transport — *deferred*
- [ ] Kernel bypass (DPDK/ef_vi) — *deferred*

### Gateway
- [x] TCP proxy between clients and engine (binary protocol)
- [ ] Scalable I/O model (epoll/io_uring multiplexing) — *deferred*
- [ ] Market data dissemination (L2 snapshots, trade feed, BBO push updates) — *deferred*
- [ ] Subscription management (subscribe/unsubscribe per instrument) — *deferred*
- [ ] Reference data management (instrument lifecycle) — *deferred*

### Authentication & Authorization
- [ ] Client authentication — *deferred*
- [ ] Per-account trading permissions — *deferred*
- [ ] Admin API (instrument management, circuit breaker controls, kill switch) — *deferred*

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
- [ ] Order/fill/cancel throughput, latency histograms, volume analytics — *deffered: replica or offline (journal-derived, zero primary impact)*

### Redundancy & High Availability
- [ ] Journal replication (WAL streaming to replica) — *deferred*
- [ ] State machine replication (deterministic replay on replica) — *deferred*
- [ ] Failover detection and promotion (leader election, split-brain prevention) — *deferred*
- [ ] Client failover (reconnect to new primary, resume with sequence numbers) — *deferred*

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

The [benchmark suite](crates/bench/) supports three modes: bare matching engine, disruptor pipeline without network, and full TCP or UDS round-trip. AMD Ryzen 7 5800X3D (8C/16T), 64 GB DDR5, NVMe SSD, Linux 6.8. All threads pinned to dedicated cores, IRQs pinned to core 0, CPU governor locked to performance, kernel boot isolation (`isolcpus`, `nohz_full`, `rcu_nocbs`).

All benchmarks: 10M order pairs (20M measured), 16 clients, 64 pipelined orders per client.

| Metric | Engine-only | TCP (no persistence) | TCP (fsync/FUA) |
|--------|-------------|----------------------|-----------------|
| **Throughput** | 11.2M orders/sec | 3.01M orders/sec | 779K orders/sec |
| **p90** | 0.05 µs | 350 µs | 1.53 ms |
| **p99** | 0.05 µs | 495 µs | 1.85 ms |
| **p99.9** | 0.09 µs | 546 µs | 4.50 ms |
| **p99.99** | 0.12 µs | 588 µs | 7.32 ms |
| **max** | 95.23 µs | 722 µs | 7.58 ms |

```sh
sudo ./scripts/bench-isolate.sh -- 10000000 --clients=16 --window=64 --mode engine    # engine-only
sudo ./scripts/bench-isolate.sh --features no-persist -- 10000000 --clients=16 --window=64  # no persistence
sudo ./scripts/bench-isolate.sh -- 10000000 --clients=16 --window=64                   # with fsync/FUA
```

## License

Copyright (c) 2026 Pierre Larger. All Rights Reserved.
