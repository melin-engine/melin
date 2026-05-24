# Melin

Melin is a deterministic, replicated sequencer for latency-critical applications. It provides a single-threaded, event-sourced processing pipeline with durable journaling, synchronous replication, and sub-millisecond tail latency under load — the infrastructure layer for systems where every event must be persisted, ordered, and replayed exactly.

Built in Rust on an [LMAX](https://martinfowler.com/articles/lmax.html)-inspired architecture: a lock-free disruptor pipeline, io_uring I/O, and mechanical sympathy throughout.

## Why Melin

**Deterministic replay** — given the same journal, the application produces identical output. This is the foundation of crash recovery, audit, and replica consistency. The sequencer enforces it; your application logic inherits it for free.

**Durable before acknowledgement** — every event is persisted to the journal and replicated before the client sees a response. Configurable durability modes let you trade latency for stronger guarantees:
- **Hybrid** (default) — one node persisted, two nodes in-memory. Any single node's slow disk is masked by the others.
- **Durably replicated** — two on-disk copies on separate nodes before ack, for stricter compliance regimes.
- CRC32C integrity checks and BLAKE3 hash chain for tamper evidence.

**Fast** — p99 ~ 520 us at 1.06M events/sec on commodity datacenter hardware (AMD EPYC 9275F, 25 Gb/s NIC, PLP NVMe). Single-event latency floor: 22 us p50, 27 us p99.

**Replicated** — synchronous dual replication via lock-free ring buffer. Journal catch-up, snapshot transfer, and automatic trading halt on replica loss. Manual promotion with zero re-replay, sub-second switchover.

## Architecture

A single reader thread (io_uring or DPDK) ingests client requests into a lock-free disruptor ring. Two consumers process each event in parallel: the journal stage persists it to disk while the application stage executes business logic and publishes results to an output ring. The response stage drains the output ring but gates on the journal cursor — no client sees a response until the event is durable. This gives you persist-before-ack without stalling the application on fsync.

## Building an application on Melin

Melin's core crates (`melin-app`, `melin-disruptor`, `melin-journal`, `melin-server-runtime`, `melin-transport-core`, `melin-wire-protocol`) form a generic sequencer. Your application plugs in via three traits:

| Trait | Role |
|-------|------|
| `AppFactory` | Constructs your application, loads snapshots, seeds initial state |
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

All numbers are **full round-trip** (client sends → server persists + replicates → application executes → response arrives at client). Measured over LAN with four AMD EPYC 9275F servers (24C Zen 5, SMT off, 768 GB DDR5-6400, Micron 7450 PRO PLP NVMe, Intel E810-XXV 25 Gb/s NIC; 1 benchmark client, 1 primary, 2 replicas).

### Throughput under load (16 clients, window 11)

| Durability | Throughput | p50 | p99 | p99.9 | p99.99 |
|------------|-----------|-----|-----|-------|--------|
| **Hybrid** (1 persisted + 2 in-memory) | **1.06M/s** | 103 us | 522 us | 597 us | 667 us |

### Single-event latency (1 client, window 1)

| Durability | Throughput | p50 | p99 | p99.9 | p99.99 |
|-----------|-----------|-----|-----|-------|--------|
| **Hybrid** (1 persisted + 2 in-memory) | 45K/s | **22 us** | **27 us** | **30 us** | **36 us** |

See [replication](docs/replication.md) for the full durability-mode menu, [operations](docs/operations.md) and [benchmarking](docs/benchmarking.md) for tuning guidance.

## Core Features

- **Event sourcing** — write-ahead journal with CRC32C checksums, BLAKE3 hash chain, batch I/O, snapshot save/load, segment rotation
- **Replication** — synchronous dual replication, journal catch-up, snapshot transfer, automatic halt on replica loss, manual promotion
- **Transport** — TCP and Unix domain sockets via io_uring (multishot RECV, batched SEND); experimental DPDK kernel bypass
- **Authentication** — Ed25519 challenge-response handshake with role-based permissions
- **Backpressure** — reject-when-full input pipeline; clients back off and retry
- **Output event channel** — real-time broadcast of all application events to authenticated subscribers
- **Operations** — structured logging, Prometheus metrics, health endpoint, admin TUI

## Melin Exchange Core

Melin ships with an exchange core built on the sequencer: order matching, account management, risk controls, circuit breakers, fee schedules, market data, and a FIX 4.4 gateway. See [the exchange core README](crates/exchange/README.md).

## License

Copyright (c) 2026 P.L.S.C. All Rights Reserved.

Commercial licensing available — contact [contact@melin-engine.com](mailto:contact@melin-engine.com).
