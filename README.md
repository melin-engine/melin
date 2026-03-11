# Trading Engine

A sub-millisecond, production-grade trading engine targeting **10M orders/sec**, built on the [LMAX architecture](https://martinfowler.com/articles/lmax.html) in Rust.

## Architecture

- **Single-threaded matching engine** — no locks on the hot path; I/O and journaling on separate threads via ring buffers
- **Event sourcing** — deterministic replay for crash recovery and audit
- **Mechanical sympathy** — cache-friendly data structures, zero allocations on the hot path, fixed-point pricing (no floats)

## Status

Early development. Core types are implemented:

- [x] Fixed-point price and quantity types (`NonZeroU64`-backed, niche-optimized)
- [x] Order model (Market, Limit)
- [x] Time-in-force (GTC, IOC, FOK)
- [x] Execution reports (Fill, Placed, Cancelled, Rejected)
- [ ] Order book
- [ ] Matching engine
- [ ] Event journal / recovery
- [ ] Risk checks
- [ ] Gateway / network layer

## Build

```sh
cargo build
cargo test
cargo clippy
```

## License

Copyright (c) 2026 Pierre Larger. All Rights Reserved.
