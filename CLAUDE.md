# CLAUDE.md

> **This file must be kept up to date** as the project evolves — update structure, dependencies, and conventions whenever they change.

## Project

Sub-millisecond, production-grade trading engine targeting **10M orders/sec**, built on the **LMAX architecture** (single-threaded business logic, event sourcing, mechanical sympathy). Rust (edition 2024). Early stage.

The engine must include all features required for production deployment.

## Build & Run

```sh
cargo build          # compile
cargo run            # run
cargo test           # run tests
cargo clippy         # lint
cargo fmt            # format
```

## Conventions

- Follow Rust best practices (idiomatic patterns, clippy clean, formatted with `cargo fmt`).
- Write unit tests for all non-trivial code. Skip only when genuinely unreasonable (e.g., trivial glue code).
- **Correctness is critical** — the matching engine is financial infrastructure. Correctness always comes first.
- **Reasonably optimized from the start** — don't prematurely optimize, but make performance-conscious choices by default: minimize allocations, avoid locks on the hot path, favor cache-friendly data structures. Profile before micro-optimizing.
- **No `.unwrap()` in production code** — use proper error handling. `.unwrap()` is fine in tests.
- **Comment data structure and type choices** — always add a comment justifying why a specific collection, data structure, or numeric type was chosen (e.g., why `BTreeMap` over `HashMap`, why `u64` over `u128`).

### Git
- **No co-authored commits** — do not add `Co-Authored-By` trailers.
- **Conventional Commits** — all commit messages must follow the [Conventional Commits](https://www.conventionalcommits.org/) spec (e.g., `feat:`, `fix:`, `refactor:`, `test:`, `docs:`, `chore:`).
- **Never push without explicit confirmation** — always ask for review before pushing. Do not push unless the user confirms.
- **Commit intermediary steps** — for large multi-step tasks, commit each logical step separately rather than batching everything into one giant commit. This keeps history clean and bisectable.

## Key Design Constraints

- **~100ns per order budget** — at 10M orders/sec, every allocation, cache miss, and branch misprediction counts
- **Single-threaded business logic** (LMAX core) — no locks on the hot path; I/O and journaling happen on separate threads via ring buffers
- **Deterministic replay** — given the same input events, output must be identical; this is the foundation of event sourcing and crash recovery
- **Strict price-time priority** — no order may jump the queue; correctness here is non-negotiable
- **Durable journaling** — every event is persisted before acknowledgement; snapshots prevent full replay from genesis on recovery
- **Full audit trail** — every order, fill, and cancellation must be recorded (regulatory requirement)
- **Hot-path scope** — risk checks, self-trade prevention, and order throttling all run on the critical path and must be zero/low-cost
- **Tail latency matters** — measure p99/p99.9, not averages
- **Extensive testing** — property-based and fuzz testing for edge cases (partial fills at price boundaries, cancel-replace races, empty book scenarios)

## Roadmap

### Order Types
- [x] Market
- [x] Limit
- [x] Stop (stop-loss)
- [x] Stop-Limit

### Time-in-Force
- [x] GTC (Good-Til-Cancelled)
- [x] IOC (Immediate-Or-Cancel)
- [x] FOK (Fill-Or-Kill)
- [ ] GTD (Good-Til-Date)
- [ ] Day

### Conditional / Advanced Orders
- [ ] Iceberg (hidden quantity)
- [ ] Trailing Stop
- [ ] OCO (One-Cancels-Other)
- [ ] Bracket (entry + take-profit + stop-loss)

### Execution Qualifiers
- [ ] Post-Only (maker-only)
- [ ] Reduce-Only

### Testing
- [ ] `proptest` invariant tests on order book (fill quantities, book consistency, volume conservation)
- [ ] `cargo-fuzz` crash discovery (arbitrary order sequences, overflow/saturation edge cases)
- [x] Verify `price × quantity` intermediate calculations don't overflow `u64` (use `u128` for computed values)

### Event Sourcing
- [x] Write-ahead journal (input commands, CRC32C checksums, crash recovery)
- [x] Snapshot save/load (version-boundary recovery, CRC32C integrity)
- [x] `JournaledExchange` wrapper (persist-before-ack, deterministic replay)
- [ ] Async journal I/O via ring buffer
- [ ] Journal rotation
- [ ] Output event log (ExecutionReports for audit)

### Risk Checks
- [x] Account balances (per-account, per-currency; reserve on order, update on fill, release on cancel)
- [ ] Self-trade prevention
- [ ] Order throttling
- [ ] Position/exposure limits

### Networking
- [x] Binary wire protocol (custom codec, length-prefixed framing)
- [x] Transport abstraction (TCP now, QUIC/kernel bypass later)
- [x] TCP transport with `TCP_NODELAY`
- [ ] Server (engine thread, session management, accept loop)
- [ ] Client library
- [ ] Admin API (instrument registration, deposits, withdrawals)
- [ ] TLS (rustls or native-tls for encrypted client connections)
- [ ] QUIC transport (investigate `quinn`)
- [ ] Kernel bypass (DPDK/ef_vi) for single-digit µs latency

### Logging & Observability
- [ ] Structured logging (connection events, order flow, errors)
- [ ] Output event log (ExecutionReports for audit trail)
- [ ] Metrics (latency histograms, throughput counters, connection counts)

## Next Steps

1. **Logging & observability** — structured logging across server, protocol, and engine; audit trail
2. **Remaining risk checks** — self-trade prevention, order throttling, position/exposure limits
3. **GTD / Day time-in-force** — requires a time source and expiry mechanism
4. **Benchmarks** — latency and throughput measurement (p99/p99.9) to validate the ~100ns budget
5. **Async journal I/O** — move journaling to a separate thread via ring buffer for ~100ns writes

## Structure

- `src/main.rs` — entry point
- `src/lib.rs` — crate root, module declarations
- `src/types.rs` — core types (OrderId, AccountId, CurrencyId, Price, Quantity, Order, ExecutionReport, InstrumentSpec, etc.)
- `src/account.rs` — account balance management (deposit, withdraw, reserve, fill, release)
- `src/orderbook.rs` — order book with price-time priority matching and stop trigger logic
- `src/exchange.rs` — multi-instrument dispatcher with integrated balance validation
- `src/journal/` — durable write-ahead log for event sourcing and crash recovery
  - `mod.rs` — module declarations, re-exports
  - `event.rs` — `JournalEvent` enum (input commands only)
  - `codec.rs` — binary encode/decode with CRC32C checksums
  - `writer.rs` — `JournalWriter` (append + fsync to disk)
  - `reader.rs` — `JournalReader` (sequential read + validate)
  - `engine.rs` — `JournaledExchange` wrapper (journal-before-execute + replay recovery)
  - `snapshot.rs` — snapshot save/load for Exchange state (version-boundary recovery)
  - `error.rs` — `JournalError` enum
- `Cargo.toml` — dependencies and project config
