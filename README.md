# Melin

[![Crates.io](https://img.shields.io/crates/v/melin-app)](https://crates.io/crates/melin-app)
[![docs.rs](https://img.shields.io/docsrs/melin-app)](https://docs.rs/melin-app)
[![License: BSL-1.1](https://img.shields.io/badge/license-BSL--1.1-blue)](LICENSE)

Melin is a deterministic, replicated sequencer for latency-critical applications. It provides a multi-threaded, event-sourced processing pipeline with durable journaling, synchronous replication, and sub-millisecond tail latency under load. The infrastructure layer for systems where every event must be persisted, ordered, and replayed exactly.

Built in Rust on an [LMAX](https://martinfowler.com/articles/lmax.html)-inspired architecture: a lock-free disruptor pipeline, io_uring I/O, and mechanical sympathy throughout.

## Features

**Deterministic replay.** Given the same journal, the application produces identical output. This is the foundation of crash recovery, audit, and replica consistency. The sequencer enforces it; your application logic inherits it as long as it stays pure (no I/O, no non-deterministic state).

**Durable and replicated.** Every event is persisted to the journal and synchronously replicated via lock-free ring buffer before the client sees a response. CRC32C integrity checks and BLAKE3 hash chain for tamper evidence. Journal catch-up, snapshot transfer, and sub-second switchover upon promotion. Configurable durability modes let you trade latency for stronger guarantees:
- **Hybrid** (default): one node persisted, two nodes in-memory. Any single node's slow disk is masked by the others, and single-node failures cause no data-loss.
- **Durably replicated**: two on-disk copies on separate nodes before ack, for stricter compliance regimes.

**Fast.** p99 ~ 520 µs at 1M events/sec on kernel TCP and commodity datacenter hardware (AMD EPYC 9275F, 25 Gb/s NIC, PLP NVMe). Single-event latency floor: 27 µs p99.

## Architecture

Melin runs a fixed set of pinned threads connected by lock-free disruptor rings:

- **Reader**: single thread multiplexing all client connections. Sole producer into the input ring.
- **Journal**: consumes from the input ring, batch-encodes events, and writes them durably. Pushes encoded batches to replicas. Advances its cursor only after the write completes.
- **Application**: consumes from the same input ring *in parallel with the journal* (not chained). Executes your single-threaded business logic and publishes results to an output ring. Never waits on disk.
- **Replication senders**: stream journal batches to replicas over TCP.
- **Event publisher**: broadcasts application output to subscribers (market data, audit, analytics).
- **Response**: drains the output ring but gates on the journal and replication cursors before sending to clients, enforcing persist-before-ack without stalling the application.
- **Shadow**: third consumer on the input ring, gated on the journal cursor. Takes periodic snapshots without pausing the application.

## Building an application on Melin

Melin's core crates form a generic sequencer. Your application plugs in via four traits:

| Trait | Role |
|-------|------|
| `Application` | Your business logic: receives events, produces output. No I/O, no syscalls. Determinism is required for replay. |
| `AppFactory` | Constructs your application, deserializes snapshots, seeds initial state |
| `RequestDecoder` | Deserializes wire bytes into your domain request type |
| `ResponseEncoder` | Serializes your domain response type into wire bytes |

The runtime handles transport, journaling, replication, signal handling, memory locking, and CPU pinning. Your binary becomes pure composition:

```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ServerConfig::parse();
    let factory = MyAppFactory::new(/* ... */);
    server::run(config, factory, MyDecoder, MyEncoder, None)
}
```

## Benchmarks

All numbers are **full round-trip** (client sends → server persists + replicates → application executes → response arrives at client) against [the Melin Exchange Core](crates/exchange/README.md). Measured over LAN with four AMD EPYC 9275F servers (24C Zen 5, SMT off, 768 GB DDR5-6400, Micron 7450 PRO PLP NVMe, Intel E810-XXV 25 Gb/s NIC; 1 benchmark client, 1 primary, 2 replicas).

### Latency under load (1M/s)

| Durability | Throughput | p50 | p99 | p99.9 | p99.99 |
|------------|-----------|-----|-----|-------|--------|
| Hybrid (1 persisted + 2 in-memory) | 1M/s | 103 µs | 522 µs | 597 µs | 667 µs |

### Single-event latency (1 client, window 1)

| Durability | Throughput | p50 | p99 | p99.9 | p99.99 |
|-----------|-----------|-----|-----|-------|--------|
| Hybrid (1 persisted + 2 in-memory) | 45K/s | 22 µs | 27 µs | 30 µs | 36 µs |

See [replication](docs/replication.md) for the full durability-mode menu, [operations](docs/operations.md) and [benchmarking](docs/benchmarking.md) for tuning guidance.

DPDK integration is well under way and should bring these figures noticeably lower, especially for the tail under load.

## Melin Exchange Core

Melin ships with an exchange core built on the sequencer: order matching, account management, risk controls, circuit breakers, fee schedules, market data, and a FIX 4.4 gateway. See [the exchange core README](crates/exchange/README.md).

## Contributing

Bug fixes and correctness improvements are welcome. Feature PRs will likely be closed.

By submitting a pull request, you agree to the terms of our [Contributor License Agreement](CLA.md).

## License

Licensed under the [Business Source License 1.1](LICENSE). Production use requires a commercial license from P.L.S.C. Contact [contact@melin-engine.com](mailto:contact@melin-engine.com).

**Design partners wanted.** We are looking for one or two design partners willing to run Melin in a non-critical capacity (internal crossing, a new instrument, a parallel-run alongside an existing engine) in exchange for direct engineering support and influence over the roadmap. Get in touch: [contact@melin-engine.com](mailto:contact@melin-engine.com).

Each version of the Licensed Work converts to Apache License, Version 2.0 on the fourth anniversary of its first public distribution.
