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
                                     batch write + sync            execute on Exchange
                                     (pwritev2 + RWF_DSYNC/FUA)   publish to output SPSC
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
- **Batch sync amortization** — under load, one sync covers many events; `pwritev2` with `RWF_DSYNC` (Force Unit Access) combines write + durability in a single syscall; `posix_fallocate` pre-allocates 64 MiB chunks so sync only flushes data pages, not extent metadata
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
- Self-trade prevention (per-order modes: CancelNewest, CancelOldest, CancelBoth)

## Build

```sh
cargo build          # compile
cargo run            # run server
cargo test           # run tests
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

The [benchmark suite](crates/bench/) supports three modes: bare matching engine, disruptor pipeline without network, and full TCP/UDS round-trip. Results below are from the round-trip mode measuring full TCP loopback latency. AMD Ryzen 7 5800X3D (8C/16T), 64 GB DDR5, NVMe SSD, Linux 6.8. All threads pinned to dedicated cores, IRQs pinned to core 0, CPU governor locked to performance.

All benchmarks: 10M order pairs, 16 clients, 64 pipelined orders per client.

**Without persistence** (pipeline + network ceiling):

```
sudo ./scripts/bench-isolate.sh --features io-uring,no-persist -- 10000000 --clients=16 --window=64

Throughput:  3.61M orders/sec (0.28 µs/order)
Latency:     p99 = 355 µs, p99.9 = 605 µs, max = 2.55 ms
```

**With fsync/FUA** (full durability, pwritev2 + RWF_DSYNC):

```
sudo ./scripts/bench-isolate.sh --features io-uring -- 10000000 --clients=16 --window=64

Throughput:  830K orders/sec (1.20 µs/order)
Latency:     p99 = 1.84 ms, p99.9 = 4.55 ms, max = 7.55 ms
```

## License

Copyright (c) 2026 Pierre Larger. All Rights Reserved.
