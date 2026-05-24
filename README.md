# Melin

Melin is a deterministic, replicated sequencer for latency-critical applications. It provides a multi-threaded, event-sourced processing pipeline with durable journaling, synchronous replication, and sub-millisecond tail latency under load — the infrastructure layer for systems where every event must be persisted, ordered, and replayed exactly.

Built in Rust on an [LMAX](https://martinfowler.com/articles/lmax.html)-inspired architecture: a lock-free disruptor pipeline, io_uring I/O, and mechanical sympathy throughout.

## Properties

**Deterministic replay** — given the same journal, the application produces identical output. This is the foundation of crash recovery, audit, and replica consistency. The sequencer enforces it; your application logic inherits it as long as it stays pure (no I/O, no non-deterministic state).

**Durable before acknowledgement** — every event is persisted to the journal and replicated before the client sees a response. Configurable durability modes let you trade latency for stronger guarantees:
- **Hybrid** (default) — one node persisted, two nodes in-memory. Any single node's slow disk is masked by the others, and single-node failures cause no data-loss.
- **Durably replicated** — two on-disk copies on separate nodes before ack, for stricter compliance regimes.
- CRC32C integrity checks and BLAKE3 hash chain for tamper evidence.

**Fast** — p99 ~ 520 us at 1M events/sec on kernel TCP and commodity datacenter hardware (AMD EPYC 9275F, 25 Gb/s NIC, PLP NVMe). Single-event latency floor: 27 us p99.

**Replicated** — synchronous dual replication via lock-free ring buffer. Journal catch-up, snapshot transfer, and automatic trading halt on replica loss. Manual promotion with zero re-replay, sub-second switchover.

## Architecture

Melin runs a fixed set of pinned threads connected by lock-free disruptor rings:

- **Reader** — single thread multiplexing all client connections. Sole producer into the input ring.
- **Journal** — consumes from the input ring, batch-encodes events, and writes them durably. Pushes encoded batches to replicas. Advances its cursor only after the write completes.
- **Application** — consumes from the same input ring *in parallel with the journal* (not chained). Executes your single-threaded business logic and publishes results to an output ring. Never waits on disk.
- **Replication senders** — stream journal batches to replicas over TCP.
- **Event publisher** — broadcasts application output to subscribers (market data, audit, analytics).
- **Response** — drains the output ring but gates on the journal and replication cursors before sending to clients, enforcing persist-before-ack without stalling the application.
- **Shadow** — third consumer on the input ring, gated on the journal cursor. Takes periodic snapshots without pausing the application.

## Building an application on Melin

Melin's core crates (`melin-app`, `melin-disruptor`, `melin-journal`, `melin-server-runtime`, `melin-transport-core`, `melin-wire-protocol`) form a generic sequencer. Your application plugs in via four traits:

| Trait | Role |
|-------|------|
| `Application` | Your business logic — receives events, produces output. No I/O, no syscalls — determinism is required for replay. |
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

### Throughput under load (1M/s)

| Durability | Throughput | p50 | p99 | p99.9 | p99.99 |
|------------|-----------|-----|-----|-------|--------|
| Hybrid (1 persisted + 2 in-memory) | 1.06M/s | 103 us | 522 us | 597 us | 667 us |

### Single-event latency (1 client, window 1)

| Durability | Throughput | p50 | p99 | p99.9 | p99.99 |
|-----------|-----------|-----|-----|-------|--------|
| Hybrid (1 persisted + 2 in-memory) | 45K/s | 22 us | 27 us | 30 us | 36 us |

See [replication](docs/replication.md) for the full durability-mode menu, [operations](docs/operations.md) and [benchmarking](docs/benchmarking.md) for tuning guidance.

## Core Features

- **Event sourcing** — write-ahead journal with CRC32C checksums, BLAKE3 hash chain, batch I/O, snapshot save/load, segment rotation
- **Replication** — synchronous dual replication, journal catch-up, snapshot transfer, automatic halt on replica loss, manual promotion
- **Transport** — TCP and Unix domain sockets via io_uring (multishot RECV, batched SEND); experimental DPDK kernel bypass
- **Authentication** — Ed25519 challenge-response handshake with role-based permissions
- **Backpressure** — reject-when-full input pipeline; clients back off and retry
- **Output event channel** — real-time broadcast of all application events to authenticated subscribers
- **Operations** — structured logging, Prometheus metrics, health endpoint

## Melin Exchange Core

Melin ships with an exchange core built on the sequencer: order matching, account management, risk controls, circuit breakers, fee schedules, market data, and a FIX 4.4 gateway. See [the exchange core README](crates/exchange/README.md).

## Contributing

Bug fixes and correctness improvements are welcome. Feature PRs without prior discussion will likely be closed — open an issue first to discuss the problem and approach.

By submitting a pull request, you agree to the terms of our [Contributor License Agreement](CLA.md).

## License

Licensed under the [Business Source License 1.1](LICENSE). Production use requires a commercial license from P.L.S.C. — contact [contact@melin-engine.com](mailto:contact@melin-engine.com).

Each version of the Licensed Work converts to Apache License, Version 2.0 on the fourth anniversary of its first public distribution.
