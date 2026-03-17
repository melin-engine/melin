# CLAUDE.md

> **This file must be kept up to date** as the project evolves — update structure, dependencies, and conventions whenever they change.

## Project

Sub-millisecond, production-grade trading engine targeting **10M orders/sec**, built on the **LMAX architecture** (single-threaded business logic, event sourcing, mechanical sympathy). Rust (edition 2024).

**Commercial product** — the goal is to sell licenses to exchanges or sell the project to an acquirer. Every feature decision should be evaluated through the lens of "does this make the product more appealing to an exchange operator or investor?"

See [README.md](README.md#priority-roadmap) for the prioritized roadmap and full feature checklist.

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
- **No `#[ignore]` on tests** — if a test fails, fix the bug. Never suppress a failing test with `#[ignore]`.
- **No silently ignored results** — do not use `let _ =` to discard `Result` values unless there is a clear reason (e.g., best-effort diagnostic writes). Handle errors explicitly.
- **Comment data structure and type choices** — always add a comment justifying why a specific collection, data structure, or numeric type was chosen (e.g., why `BTreeMap` over `HashMap`, why `u64` over `u128`).
- **Log levels** — `error!`: server malfunctions only (bugs, journal I/O failures) — must never fire due to bad client input or client network issues. `info!`: server lifecycle events (start, stop, recovery). `debug!`: client-caused events (connections, disconnects, malformed messages, write failures).

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

See [README.md](README.md#features) for the full feature checklist and roadmap.

## Dead Ends / Investigated & Rejected

### SMI count tracking via MSR 0x34 (AMD Ryzen)

**Date**: 2026-03-14 | **CPU**: AMD Ryzen 7 5800X3D

Attempted to read MSR 0x34 (IA32_SMI_COUNT) to track SMI interrupts during benchmarks and explain the 20-112µs max latency spikes in engine-only mode. `modprobe msr` succeeded but `rdmsr -p 0 0x34` returns "cannot read MSR" — AMD doesn't expose this Intel-specific MSR.

**Conclusion**: Can't measure SMIs on this CPU. The max latency spikes (~1 in 20M orders) are likely SMIs/NMIs/kernel interrupts but not worth chasing — p99.99 is rock-solid at 0.11µs.

### io_uring registered buffers for socket I/O (kernel 6.8)

**Date**: 2026-03-13 | **Kernel**: 6.8.0-101-generic | **io_uring crate**: 0.7

Pre-registering buffers via `IORING_REGISTER_BUFFERS` to skip per-SQE `get_user_pages()` page table walks. Two approaches tested:

1. **`ReadFixed`/`WriteFixed` opcodes** — works but routes through VFS layer (`vfs_read` → `sock_read_iter`) instead of the direct socket path (`sock_recvmsg`). Benchmarked ~5% *slower* than plain `Recv`/`Send`.

2. **`IORING_RECVSEND_FIXED_BUF` flag (value=4) on `Recv`/`Send` SQEs** — stays on the direct socket path. Requires SQE patching (ioprio at offset 2, buf_index at offset 40) since the io_uring 0.7 crate doesn't expose these fields for Recv/Send. Returns `EINVAL` on kernel 6.8 for `IORING_OP_RECV`. `SEND` support landed in kernel 6.0; reliable `RECV` support came later.

**Conclusion**: Not viable on kernel 6.8. The per-SQE `get_user_pages()` cost (~100-200ns) is already optimized by the kernel's GUP fast path for recently-used pages. **Revisit on kernel ≥6.10** where `IORING_RECVSEND_FIXED_BUF` may work for both RECV and SEND.

### Group commit delay with TCP transport

**Date**: 2026-03-13

Tested `--group-commit-us` values 8, 16, 25, 64, 100, 128 µs on TCP loopback (16 clients, window 16). All values hurt throughput and p50 vs the zero-delay baseline (201K/s, p50 1114µs). Even 8µs dropped throughput to 194K/s. Reason: the delay holds the journal cursor longer, making the response stage block on the cursor spin-wait and accumulate larger TCP send buffers. The io_uring overlapped fsync already provides natural batching — while fsync A is in flight, events accumulate for batch B — so explicit delay adds no benefit.

Group commit *does* help UDS (270K/s at 100µs, +34% over baseline) because UDS transport is near-free (response stage 0.18% busy vs 25% on TCP).

**Conclusion**: Keep `group_commit_delay = 0` for TCP. Only use group commit with UDS or after making TCP response sends cheaper.

### Response stage per-slot journal cursor gating with mid-batch flush

**Date**: 2026-03-13

Moved journal cursor check from batch-level (wait for max input_seq) to per-slot, with mid-batch `flush_sends()` before spinning on journal cursor. Goal: send already-durable responses while waiting for fresher events.

Results: p99 improved 15% (1740→1476µs), journal utilization jumped from 0.38% to 19.86% (pipeline doing more useful work), but p50 regressed 13% (1115→1263µs) and response stage went from 23% to 31% busy. The synchronous `flush_sends()` (io_uring submit_and_wait) in the inner loop added per-iteration overhead.

Without the mid-batch flush (per-slot check only), results were identical to baseline — responses still sit in send_bufs until SPSC empties, so earlier encoding of durable slots doesn't help.

**Conclusion**: Mid-batch flush is the right direction (proved by 20x journal utilization increase) but synchronous sends are too expensive. Revisit with non-blocking send submission or when TCP response overhead is reduced.

## Performance Profile

Performance figures are in the [README](README.md#performance). Keep them up to date when making performance-related changes.

LAN benchmark (two Cherry AMD Ryzen 9950X servers, dedicated NVMe journal disk):
- **With fsync/FUA**: 5.2M orders/sec, p99.9 = 939 µs, max = 1.47 ms
- **Without persistence**: 11.2M orders/sec, p99.9 = 747 µs, max = 915 µs
- **Single-order latency**: 70 µs p50 (1 client, no pipelining, full durability)
- **Engine-only**: 17.3M orders/sec, p99 = 0.06 µs

### Current bottleneck: TCP network stack

The 2x gap between fsync (5.2M) and no-persist (11.2M) shows that journal I/O is no longer the dominant bottleneck on PLP NVMe hardware. The TCP stack (syscalls, kernel buffers, io_uring send/recv overhead) is now the primary throughput limiter. The engine itself runs at 17.3M/s — the pipeline and network consume the remaining headroom.

**How to apply:** Further throughput gains require reducing TCP overhead (e.g., batched io_uring multishot send). Journal optimization is no longer the priority.

Core layout: 0=OS/IRQ, 1-3=pipeline (journal/matching/response), 4-5=readers, 6+=bench.

**Benchmarking constraint**: do NOT optimize by batching multiple client requests into a single write — real clients send one order at a time. Batch submission is unrealistic and inflates throughput numbers artificially.

## Structure

### `crates/disruptor/` — generic lock-free ring buffers (no trading-domain knowledge)
- `src/padding.rs` — cache-line alignment (`CachePadded<T>`)
- `src/ring.rs` — multi-consumer disruptor (single-producer or CAS-based multi-producer, N gated consumers)
- `src/spsc.rs` — single-producer, single-consumer queue

### `crates/engine/` — matching engine and event sourcing
- `src/types.rs` — core types (OrderId, AccountId, CurrencyId, Price, Quantity, Order, ExecutionReport, InstrumentSpec, CircuitBreakerConfig, FeeSchedule, etc.)
- `src/account.rs` — account balance management (flat `Vec<Balance>` indexed by `account_id * stride + currency_id` for O(1) lookups; deposit, withdraw, reserve, fill, release)
- `src/orderbook.rs` — order book with price-time priority matching and stop trigger logic
- `src/exchange.rs` — multi-instrument dispatcher with integrated balance validation, fee computation, cancel-replace
- `src/journal/` — durable write-ahead log for event sourcing and crash recovery
  - `event.rs` — `JournalEvent` enum (input commands only)
  - `codec.rs` — binary encode/decode with CRC32C checksums
  - `writer.rs` — `JournalWriter` (append + fsync to disk, batch append API)
  - `reader.rs` — `JournalReader` (sequential read + validate)
  - `engine.rs` — `JournaledExchange` wrapper (journal-before-execute + replay recovery)
  - `pipeline.rs` — disruptor pipeline stages (`JournalStage`, `MatchingStage`, slot types)
  - `snapshot.rs` — snapshot save/load for Exchange state (version-boundary recovery)
  - `error.rs` — `JournalError` enum

### `crates/server/` — server and pipeline orchestration
- `src/server.rs` — builds disruptor pipeline, spawns 3 OS threads, accept loop
- `src/response.rs` — response stage thread (output SPSC → direct socket writes via `BlockingFrameWriter`)
- `src/reader.rs` — epoll-based multiplexed reader pool (edge-triggered, non-blocking I/O → lock-free `MultiProducer`)
- `src/affinity.rs` — CPU core pinning for pipeline and reader threads

### `crates/protocol/` — wire protocol (zero async, no tokio)
- `src/message.rs` — `Request`, `ResponseKind`, `ConnectionId`
- `src/codec.rs` — binary encode/decode for wire messages
- `src/transport.rs` — `BlockingTransportListener` trait (TCP, UDS, future io_uring)
- `src/blocking.rs` — `BlockingFrameReader`/`BlockingFrameWriter` for length-prefixed framing
- `src/tcp.rs` — `BlockingTcpListener` (std `TcpListener`, `TCP_NODELAY`)
- `src/uds.rs` — `BlockingUdsListener` (std `UnixListener`)

### `crates/gateway/` — TCP proxy between clients and engine
### `crates/admin/` — CLI admin tool (instrument management, deposits, circuit breakers, risk limits, fee schedules, cancel-replace, live stats dashboard, key generation)
### `crates/client/` — typed blocking client library (Ed25519 auth, std `TcpStream`)
### `crates/bench/` — pipelined end-to-end benchmark with latency histograms (TCP default, `--uds` flag)
### `crates/tui/` — terminal UI for interactive testing

### `scripts/`
- `bench-isolate.sh` — CPU governor tuning, NMI watchdog disable, IRQ affinity pinning, dmesg capture for latency benchmarking (requires root)
- `grub-bench.conf` — kernel boot parameters for `isolcpus` + `nohz_full` + `rcu_nocbs` core isolation


