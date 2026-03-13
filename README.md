# Trading Engine

A sub-millisecond, trading engine targeting **10M orders/sec**, built on the [LMAX architecture](https://martinfowler.com/articles/lmax.html) in Rust.

## Architecture

```
Clients ──TCP/UDS──> Accept Loop
                         │
                    Epoll Reader Pool (edge-triggered, non-blocking I/O)
                         │
                    lock-free MultiProducer ──> Input Disruptor (ring buffer)
                                                         │
                                          ┌──────────────┼──────────────────┐
                                          │                                 │
                                     Journal Thread                Matching Thread
                                     batch write + fsync           execute on Exchange
                                     (io_uring async fsync)        publish to output SPSC
                                          │                                 │
                                     advances cursor ────────┐              │
                                                             ▼              │
                                                      Response Thread  ◄───┘
                                                      gates on journal cursor
                                                      writes directly to sockets
                                                             │
                                                      ──TCP/UDS──> Clients
```

- **Single-threaded matching engine** — no locks on the hot path; one thread executes all matching logic
- **LMAX disruptor pipeline** — 3 OS threads (journal, matching, response) on lock-free ring buffers; lock-free CAS-based multi-producer from reader pool; journal and matching run in parallel on the same events
- **Persist-before-ack** — pipelined journal I/O with full durability guarantee; matching latency overlapped against journal writes, acknowledgement gated on confirmed durability, not optimistically sent
- **Batch fsync amortization** — under load, one fsync covers many events; optional io_uring async fsync overlaps I/O wait with encoding; `posix_fallocate` pre-allocates 64 MiB chunks so fsync only flushes data pages, not extent metadata
- **Event sourcing** — deterministic replay for crash recovery and audit; snapshots for fast restart
- **Mechanical sympathy** — cache-line-padded sequences, fixed-point pricing (no floats), zero allocations on the hot path

## Features

### Matching Engine
- Order types: Market, Limit, Stop, Stop-Limit
- Time-in-force: GTC, IOC, FOK
- Strict price-time priority (BTreeMap + VecDeque order book)
- Execution reports: Fill, Placed, Triggered, Cancelled, Rejected
- Multi-instrument exchange with shared account balances

### Event Sourcing
- Write-ahead journal with CRC32C checksums
- Batch journal I/O via disruptor ring buffer pipeline
- Pre-allocated storage (`posix_fallocate`) for reduced fsync latency
- Snapshot save/load for fast recovery
- Deterministic replay from journal

### Networking
- Custom binary wire protocol (length-prefixed framing)
- TCP transport with `TCP_NODELAY` and Unix domain socket transport
- Epoll reader pool (edge-triggered, non-blocking) with dedicated I/O threads (zero tokio)
- Transport abstraction (TCP/UDS now, io_uring/kernel bypass later)
- Typed client library
- Terminal UI for interactive testing

### Risk & Accounting
- Per-account, per-currency balance management
- Reserve on order, update on fill, release on cancel

## Build

```sh
cargo build          # compile
cargo run            # run server
cargo test           # run tests (126 tests across workspace)
cargo clippy         # lint
cargo fmt            # format
```

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

The [benchmark suite](crates/bench/) supports three modes: bare matching engine, disruptor pipeline without network, and full TCP/UDS round-trip. Results below are from the round-trip mode measuring full TCP loopback latency. AMD Ryzen 7 5800X3D (8C/16T), 64 GB DDR5, NVMe SSD, Linux 6.8.

All benchmarks: 16 clients, 64 pipelined orders per client, 1M order pairs.

**Without fsync** (isolates pipeline latency from disk I/O):

```
cargo run --release -p trading-bench --features io-uring,no-fsync -- 1000000 --clients=16 --window=64 --mode=roundtrip

Throughput:  1.62M orders/sec (0.62 µs/order)
Latency:     p99 = 378 µs, p99.9 = 506 µs, max = 1.07 ms
```

**With fsync** (full durability, io_uring async fdatasync):

```
cargo run --release -p trading-bench --features io-uring -- 1000000 --clients=16 --window=64 --mode=roundtrip

Throughput:  605K orders/sec (1.65 µs/order)
Latency:     p99 = 1.80 ms, p99.9 = 6.75 ms, max = 7.08 ms
```

## License

Copyright (c) 2026 Pierre Larger. All Rights Reserved.
