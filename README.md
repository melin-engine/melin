# Melin

[![Crates.io](https://img.shields.io/crates/v/melin-app)](https://crates.io/crates/melin-app)
[![docs.rs](https://img.shields.io/docsrs/melin-app)](https://docs.rs/melin-app)
[![CI](https://img.shields.io/github/actions/workflow/status/melin-engine/melin/pre-merge.yml?label=CI)](https://github.com/melin-engine/melin/actions/workflows/pre-merge.yml)
[![License: BSL-1.1](https://img.shields.io/badge/license-BSL--1.1-blue)](LICENSE)

Melin is the runtime under a matching engine, a ledger, or any system whose business logic must process every event in a total order, survive a crash without losing one, and replay identically for audit — while keeping tail latency inside a budget measured in microseconds.

It is a deterministic, replicated sequencer: your single-threaded application logic plugs in, and Melin provides the event-sourced pipeline around it — durable journaling, synchronous replication, snapshots, transport, failover. Built in Rust on an [LMAX](https://martinfowler.com/articles/lmax.html)-inspired architecture: lock-free disruptor rings, io_uring I/O, and mechanical sympathy throughout.

**Design partners wanted.** We are looking for one or two design partners willing to run Melin in a non-critical capacity (internal crossing, a new instrument, a parallel run alongside an existing engine) in exchange for direct engineering support and influence over the roadmap. Get in touch: [contact@melin-engine.com](mailto:contact@melin-engine.com).

## Features

**Deterministic replay.** Given the same journal, the application produces identical output. This is the foundation of crash recovery, audit, and replica consistency. The sequencer enforces it; your application inherits it as long as its logic stays pure.

**Durable and replicated.** Every event is journaled and synchronously replicated before the client sees a response, with CRC32C integrity checks and a BLAKE3 hash chain for tamper evidence. By default an ack requires one node to have persisted and two to hold the event in memory, so a single slow disk or a single node failure costs neither latency nor data; a stricter two-disks-before-ack mode and a faster replication-only mode (no fsync on the ack path) are available. Journal catch-up, snapshot transfer, and automatic failover are built in. See [replication](docs/replication.md).

**Fast.** p99 of 404 µs at 1.00M events/sec, full round trip including persistence and replication, on kernel TCP and commodity datacenter hardware. Single-event floor: 66 µs p99. See [Benchmarks](#benchmarks).

**Tested for the failures that matter.** Every commit runs the full suite, including crash-recovery and three-node failover tests. Nightly, the suite runs again under ThreadSanitizer, the lock-free core runs under Miri across multiple scheduler seeds, and dependencies are checked against the RUSTSEC advisory database.

## Benchmarks

All numbers are **full round-trip** (client sends → server persists + replicates → application executes → response arrives at client) against [the Melin Exchange Core](https://github.com/melin-engine/exchange-core), an order-matching engine built on this sequencer. Measured over LAN with four AMD EPYC 9275F servers (24C Zen 5, SMT off, 768 GB DDR5-6400, Micron 7450 PRO PLP NVMe, Intel E810-XXV 25 Gb/s NIC; 1 benchmark client, 1 primary, 2 replicas). Default durability mode: one node persisted, two in memory.

### Latency under load (closed-loop)

Four connections, 56 requests in flight each.

| Throughput | p50 | p99 | p99.9 | p99.99 | p99.999 |
|-----------|-----|-----|-------|--------|---------|
| 1.00M/s | 207 µs | 404 µs | 511 µs | 595 µs | 691 µs |

### Single-event latency (1 client, window 1)

| Throughput | p50 | p99 | p99.9 | p99.99 |
|-----------|-----|-----|-------|--------|
| 20K/s | 49 µs | 66 µs | 113 µs | 138 µs |

The benchmark harness and tuning guidance ship with the Melin Exchange Core.

## Building an application on Melin

Melin's core crates form a generic sequencer. Your application plugs in via four traits:

| Trait | Role |
|-------|------|
| `Application` | Your business logic: receives events, produces output |
| `AppFactory` | Constructs your application, deserializes snapshots, seeds initial state |
| `RequestDecoder` | Deserializes wire bytes into your domain request type |
| `ResponseEncoder` | Serializes your domain response type into wire bytes |

The one rule: `Application` must be deterministic — no I/O, no clocks, no randomness. Everything else (transport, journaling, replication, signal handling, memory locking, CPU pinning) is handled by the runtime, and your binary becomes pure composition:

```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ServerConfig::parse();
    let factory = MyAppFactory::new(/* ... */);
    server::run(config, factory, MyDecoder, MyEncoder, None)
}
```

See [`crates/examples/counter`](crates/examples/counter) for a complete working example.

## Architecture

A node runs a fixed set of pinned threads connected by lock-free disruptor rings. No async runtime, no locks on the hot path.

```
 clients ──> Reader ──┬──> Journal ──┬──> Journal Disk ┄┄┄┄┐
 (TCP or DPDK)        │              └──> Replication ┄┄┄┄┄┤ durability
                      │                                    v cursors
                      ├──> Application ───────────┬──> Response ──> clients
                      │                           └──> Event publisher ──> subscribers
                      └──> Shadow ──> snapshots
```

- **Reader**: one thread multiplexing every client connection, over kernel TCP with io_uring or over DPDK in userspace. Sole producer into the input ring.
- **Journal**: sequences, encodes, and hash-chains events and feeds encoded batches to the replication senders — all in memory. A separate **Journal Disk** thread writes and syncs the batches and publishes the durability cursors. Because the two are split, a slow disk stalls neither ordering nor the replica feed.
- **Application**: consumes the input ring in parallel with the journal. Runs your single-threaded logic and publishes results to the output ring. Never waits on disk.
- **Response**: drains the output ring but gates each response on the journal and replication cursors before sending it, so persist-before-ack is enforced without stalling the application.
- **Event publisher**: broadcasts application output to subscribers (market data, audit, analytics).
- **Shadow**: a third consumer on the input ring, gated on the journal cursor, that takes periodic snapshots without pausing the application.

**Replicas** run the same pipeline, fed by the primary's journal batches over TCP: they journal, apply, and snapshot exactly as the primary does, with application state kept warm and outputs discarded.

**Recovery** on any node is snapshot plus journal replay: the newest snapshot is loaded and every journaled event after it is re-applied. Determinism guarantees the result is the state clients were told about.

**Control plane.** An optional Raft service handles leader election, fencing epochs, and automatic failover, and nothing else. It runs on its own thread, isolated from the data plane. Elections steer toward the most-caught-up replica, and an elected replica refuses to promote while it can still see a live primary.

## Melin Exchange Core

A production exchange core is built on this sequencer and distributed separately: order matching, account management, risk controls, circuit breakers, fee schedules, market data, and a FIX 4.4 gateway. See [melin-exchange-core](https://github.com/melin-engine/exchange-core).

## Contributing

Bug fixes and correctness improvements are welcome. Feature PRs will likely be closed: the roadmap is driven by the needs of the product and its design partners.

By submitting a pull request, you agree to the terms of our [Contributor License Agreement](CLA.md).

## License

Licensed under the [Business Source License 1.1](LICENSE). Production use requires a commercial license from P.L.S.C. Contact [contact@melin-engine.com](mailto:contact@melin-engine.com).

Each version of the Licensed Work converts to Apache License, Version 2.0 on the fourth anniversary of its first public distribution.
