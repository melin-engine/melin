# Melin

[![Crates.io](https://img.shields.io/crates/v/melin-app)](https://crates.io/crates/melin-app)
[![docs.rs](https://img.shields.io/docsrs/melin-app)](https://docs.rs/melin-app)
[![CI](https://img.shields.io/github/actions/workflow/status/melin-engine/melin/pre-merge.yml?label=CI)](https://github.com/melin-engine/melin/actions/workflows/pre-merge.yml)
[![MSRV](https://img.shields.io/crates/msrv/melin-app)](Cargo.toml)
[![License: BSL-1.1](https://img.shields.io/badge/license-BSL--1.1-blue)](LICENSE)

Melin is a replicated sequencer for latency-critical systems that cannot lose an event. Your business logic plugs in as a plain state machine, and Melin makes it durable and replicated: every input is assigned a total order, then journaled and replicated before the response goes out. Snapshots and automatic failover are built in.

It is the runtime under a matching engine, a ledger, or any system that must replay identically for audit while keeping tail latency inside a budget measured in microseconds. Built in Rust on an [LMAX](https://martinfowler.com/articles/lmax.html)-inspired architecture: lock-free disruptor rings, io_uring I/O with optional DPDK kernel bypass, and mechanical sympathy throughout.

**Design partners wanted.** We are looking for one or two design partners willing to run Melin in a non-critical capacity (internal crossing, a new instrument, a parallel run alongside an existing engine) in exchange for direct engineering support and influence over the roadmap. Get in touch: [contact@melin-engine.com](mailto:contact@melin-engine.com).

## Features

**Nothing is acknowledged before it is safe.** Every event is journaled and synchronously replicated before the client sees a response. The ack policy chooses what "safe" means. By default (`disk+ram`) it is one fsynced copy plus a second copy in another node's memory, so a slow disk or a dead node costs neither latency nor data. A stricter `two-disks` policy and a faster `ram` policy are available. See [replication](docs/replication.md).

**Fast.** p99 of 245 µs at 1M events/sec on kernel TCP, and 40 µs with DPDK kernel bypass, full round trip including persistence and replication on commodity datacenter hardware. See [Benchmarks](#benchmarks).

**Deterministic replay.** Given the same journal, the application reaches the same state and emits the same output, on every node and after every restart. Melin supplies the total order and the timestamps, so your logic never reads a clock. Keeping it pure is the only rule left to you.

**Tamper-evident history.** Per-entry CRC32C catches corruption. A BLAKE3 hash chain across segments catches rewrites, missing or foreign segments, and a node whose journal disagrees with the cluster's. See [journal](docs/journal.md).

**Failover built in.** A raft control plane, off the hot path, elects the primary and promotes a replica when it fails. Journal catch-up and snapshot transfer bring a rejoining node back to the head of the log.

**Tested for the failures that matter.** Every commit runs the full suite, including crash-recovery and three-node failover tests. Nightly, the suite runs again under ThreadSanitizer, the lock-free core runs under Miri across multiple scheduler seeds, and dependencies are checked against the RUSTSEC advisory database.

## Benchmarks

All numbers are **full round-trip** (client sends → server persists + replicates → application executes → response arrives at client) against [the Melin Exchange Core](https://github.com/melin-engine/exchange-core), an order-matching engine built on this sequencer. Measured over LAN with four bare-metal AMD EPYC 9275F servers (24C Zen 5, SMT off, Micron 7450 PRO PLP NVMe, Mellanox ConnectX-6 Dx 100 Gb/s NIC; 1 benchmark client, 1 primary, 2 replicas). Default ack policy (`disk+ram`): one fsynced copy, two in-memory copies on separate nodes.

### Kernel TCP

**Under load.** Four connections, 32 requests in flight each.

| Throughput | p50 | p99 | p99.9 | p99.99 | p99.999 |
|-----------|-----|-----|-------|--------|---------|
| 1M/s | 100 µs | 245 µs | 299 µs | 346 µs | 395 µs |

**Single event.** 1 client, window 1.

| Throughput | p50 | p99 | p99.9 | p99.99 |
|-----------|-----|-----|-------|--------|
| 25K/s | 38 µs | 62 µs | 73 µs | 104 µs |

### DPDK kernel bypass (experimental)

The same workload with the client and all three server nodes on DPDK kernel bypass.

**Under load.** Four connections, 8 requests in flight each.

| Throughput | p50 | p99 | p99.9 | p99.99 | p99.999 |
|-----------|-----|-----|-------|--------|---------|
| 1M/s | 28 µs | 40 µs | 61 µs | 76 µs | 87 µs |

**Single event.** 1 client, window 1.

| Throughput | p50 | p99 | p99.9 | p99.99 |
|-----------|-----|-----|-------|--------|
| 48K/s | 20 µs | 45 µs | 47 µs | 49 µs |

The benchmark harness and tuning guidance ship with the Melin Exchange Core.

## Building an application on Melin

Melin's core crates form a generic sequencer. Your application plugs in via five traits:

| Trait | Role |
|-------|------|
| `AppEvent` | Your journaled event type: its encoding, and the widest it can get, which the journal sizes itself from |
| `Application` | Your business logic: receives events, produces output |
| `AppFactory` | Constructs your application, deserializes snapshots, seeds initial state |
| `RequestDecoder` | Deserializes wire bytes into your domain request type |
| `ResponseEncoder` | Serializes your domain response type into wire bytes |

The one rule: `Application` must be deterministic: no I/O, no clocks, no randomness. Everything else (transport, journaling, replication, signal handling, memory locking, CPU pinning) is handled by the runtime, and your binary becomes pure composition:

```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ServerConfig::parse();
    let factory = MyAppFactory::new(/* ... */);
    server::run(config, factory, MyDecoder, MyEncoder, None)
}
```

Three examples, in order of size: [`crates/examples/echo`](crates/examples/echo) is the runtime with nothing on top — a state-free echo, and the sequencer's latency floor to measure any application against; [`crates/examples/counter`](crates/examples/counter) is the smallest application with state to keep; and [`crates/examples/notary`](crates/examples/notary) exercises the ordering and durability guarantees: a tamper-evident hash chain over client-submitted digests.

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
- **Journal**: sequences, encodes, and hash-chains events and feeds encoded batches to the replication senders. A separate **Journal Disk** thread writes and syncs the batches and publishes the durability cursors. Because the two are split, a slow disk stalls neither ordering nor the replica feed.
- **Application**: consumes the input ring in parallel with the journal. Runs your single-threaded logic and publishes results to the output ring. Never waits on disk.
- **Response**: drains the output ring but gates each response on the journal and replication cursors before sending it, so persist-before-ack is enforced without stalling the application.
- **Event publisher**: broadcasts application output to subscribers (market data, audit, analytics).
- **Shadow**: a third consumer on the input ring, gated on the journal cursor, that takes periodic snapshots without pausing the application.

**Replicas** run the same pipeline, fed by the primary's journal batches over TCP: they journal, apply, and snapshot exactly as the primary does, with application state kept warm and outputs discarded.

**Recovery** on any node is snapshot plus journal replay: the newest snapshot is loaded and every journaled event after it is re-applied. Determinism guarantees the result is the state clients were told about.

**Control plane.** An optional Raft service handles leader election, fencing epochs, and automatic failover, and nothing else. It runs on its own thread, isolated from the data plane. Elections steer toward the most-caught-up replica, and an elected replica refuses to promote while it can still see a live primary, or while any reachable peer holds more data than it does.

## Melin Exchange Core

A production exchange core is built on this sequencer and distributed separately: order matching, account management, risk controls, circuit breakers, fee schedules, market data, and a FIX 4.4 gateway. See [melin-exchange-core](https://github.com/melin-engine/exchange-core).

## Contributing

Bug fixes and correctness improvements are welcome. Feature PRs will likely be closed: the roadmap is driven by the needs of the product and its design partners.

By submitting a pull request, you agree to the terms of our [Contributor License Agreement](CLA.md).

## License

Licensed under the [Business Source License 1.1](LICENSE). Production use requires a commercial license from P.L.S.C. Contact [contact@melin-engine.com](mailto:contact@melin-engine.com).

Each version of the Licensed Work converts to Apache License, Version 2.0 on the fourth anniversary of its first public distribution.

The examples under [`crates/examples`](crates/examples) are licensed under the [Apache License, Version 2.0](crates/examples/echo/LICENSE) from the start: they exist to be copied into your own application, and the runtime's licence does not travel with the copied code.
