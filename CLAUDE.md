# CLAUDE.md

> **This file must be kept up to date** as the project evolves — update structure, dependencies, and conventions whenever they change.
>
> **DONE**: Replaced `FxHashMap` with `astenn` (extendible hashing) for all engine HashMaps — grows one bucket at a time, no full-table rehash spikes. ~19% throughput regression vs flat Vec is the cost of sparse storage.

## Project

**Melin** — sub-millisecond, production-grade exchange core targeting **10M orders/sec**, built on the **LMAX architecture** (single-threaded business logic, event sourcing, mechanical sympathy). Rust (edition 2024). Handles order matching, account management, risk controls, circuit breakers, fee schedules, authentication, journaling, and replication.

**Commercial product** — the goal is to sell licenses to exchanges or sell the project to an acquirer. Every feature decision should be evaluated through the lens of "does this make the product more appealing to an exchange operator or investor?"

See [README.md](README.md#features) for implemented features and [docs/roadmap.md](docs/roadmap.md) for planned features.

## Conventions

- Follow Rust best practices (idiomatic patterns, clippy clean, formatted with `cargo fmt`).
- Write unit tests for all non-trivial code. Skip only when genuinely unreasonable (e.g., trivial glue code).
- **Correctness is critical** — the matching engine is financial infrastructure. Correctness always comes first.
- **Reasonably optimized from the start** — don't prematurely optimize, but make performance-conscious choices by default: minimize allocations, avoid locks on the hot path, favor cache-friendly data structures. Profile before micro-optimizing.
- **Always `cargo check` before committing** — run `cargo check` with the correct feature flags for all affected crates before committing. For DPDK code, check with `--features dpdk --no-default-features` on both server and bench.
- **No `.unwrap()` in production code** — use proper error handling. `.unwrap()` is fine in tests.
- **No `#[ignore]` on tests** — if a test fails, fix the bug. Never suppress a failing test with `#[ignore]`.
- **No silently ignored results** — do not use `let _ =` to discard `Result` values unless there is a clear reason (e.g., best-effort diagnostic writes). Handle errors explicitly.
- **Comment data structure and type choices** — always add a comment justifying why a specific collection, data structure, or numeric type was chosen (e.g., why `BTreeMap` over `HashMap`, why `u64` over `u128`).
- **Log levels** — `error!`: server malfunctions only (bugs, journal I/O failures) — must never fire due to bad client input or client network issues. `warn!`: degraded operation that isn't a bug but needs attention (e.g., CPU pinning failed, resource limits approaching, unexpected-but-handled conditions). `info!`: server lifecycle events (start, stop, recovery). `debug!`: client-caused events (connections, disconnects, malformed messages, write failures).

### Git
- **No co-authored commits** — do not add `Co-Authored-By` trailers.
- **Conventional Commits** — all commit messages must follow the [Conventional Commits](https://www.conventionalcommits.org/) spec (e.g., `feat:`, `fix:`, `refactor:`, `test:`, `docs:`, `chore:`).
- **Never commit without explicit request** — do NOT commit unless the user explicitly asks (e.g. "commit", "commit and push"). Completing a task does NOT imply permission to commit. Always wait for the user to request the commit.
- **Never push without explicit confirmation** — always ask for review before pushing. Do not push unless the user confirms.
- **Commit intermediary steps** — for large multi-step tasks, commit each logical step separately rather than batching everything into one giant commit. This keeps history clean and bisectable. Always ask for review after each commit before moving to the next.
- **Always check `Cargo.lock`** — when dependencies change, `Cargo.lock` must be staged and committed alongside `Cargo.toml` changes. The pre-commit hook enforces this.

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

See [README.md](README.md#features) for implemented features and [docs/roadmap.md](docs/roadmap.md) for planned features.

## DPDK Transport (`feat/dpdk-rss` branch, based on `feat/dpdk-zerocopy-rx`)

**Feature flag**: `--features dpdk` (compile-time, replaces io-uring/epoll transport)

DPDK kernel-bypass networking — bypasses the Linux kernel TCP stack entirely. Uses DPDK for NIC I/O (`rx_burst`/`tx_burst`) and a smoltcp fork ([fastcp](https://github.com/pierre-l/fastcp)) for userspace TCP. The engine pipeline is unaware of DPDK.

**Architecture**: N parallel poll threads, each with its own NIC queue pair and smoltcp stack. RSS (Receive Side Scaling) distributes TCP flows across queues in hardware. No shared mutable state between threads. `--readers` controls the thread count (same CLI arg as kernel TCP reader threads); `--reader-cores` controls where they pin. Queue count auto-clamps to NIC maximum (TAP devices get 1).

**Crate**: `crates/dpdk/` — gated behind `dpdk-sys` feature. Compiles as an empty shell without libdpdk, lives in the workspace unconditionally. Source files in `src/dpdk/` submodule. `DpdkShared` holds global resources (EAL, mempool, ports); `DpdkTransport` is per-thread (device, Interface, SocketSet).

**Scripts**: `dpdk-server.sh` (auto-detect config, start server), `dpdk-setup-sriov.sh` (VF creation for ice + ixgbe), `dpdk-test.sh` (testpmd environment check), `dpdk-smoke-test.sh` (TAP), `dpdk-e2e-smoke-test.sh` (veth + af_packet, both sides DPDK).

**Core layout**: 0=OS/IRQ, 1-3=pipeline (journal/matching/response), 4-5=readers or DPDK poll threads, 6=repl-sender, 7=event-publisher, 8+=bench.

**Tested hardware**: Intel 82599/X520 (ixgbe) SR-IOV on EPYC 4564P, LACP bond, Cherry Servers. Intel E810 (ice) supported but untested on current servers (IOMMU issues on some rentals).

**Benchmark results** (1 client, window 1, single-order round-trip, full journal durability):

| Transport | p50 | p90 | p99 | Hardware |
|-----------|-----|-----|-----|----------|
| Kernel TCP | 71 µs | 71 µs | 114 µs | EPYC 4564P, 82599 10GbE |
| DPDK (server only) | 59 µs | 61 µs | 113 µs | same |
| DPDK (e2e) | **37 µs** | **38 µs** | **101 µs** | same |

**Response routing**: thread_id is encoded in bits 56..63 of connection_id. The response stage extracts it with a shift to route TxFrames to the correct per-thread SPSC channel in O(1).

**Known remaining gaps**:
- Needs merge with main (shadow exchange, snapshot schedule, updated PipelineCores)

## Dead Ends / Investigated & Rejected
**How to apply:** The matching engine is not the bottleneck. The journal fsync stage gates pipeline throughput; TCP pipelining (window=256) effectively hides fsync latency. Further throughput gains require reducing transport overhead (UDS, kernel bypass) or journal I/O optimization (overlapped io_uring writes). See Performance Tuning leads in the README.

### Prioritized performance leads

| Priority | Optimization | Est. gain | Complexity |
|----------|-------------|-----------|------------|
| 1 | Embed ReservationSlot in RestingOrder | 5-10% matching | Moderate |
