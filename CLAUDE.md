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

## In Progress

### DPDK kernel-bypass transport (`feat/dpdk-transport` branch)

**Date**: 2026-03-22 | **Feature flag**: `--features dpdk` (compile-time, replaces io-uring/epoll transport)

DPDK kernel-bypass networking at the transport edge — bypasses the Linux kernel TCP stack entirely. Uses DPDK for NIC I/O (`rx_burst`/`tx_burst`) and smoltcp for userspace TCP processing. The engine pipeline is completely unaware of DPDK.

**What works:**
- Full transport layer: `crates/dpdk/` (EAL, mempool, port, smoltcp Device, C wrappers for inline DPDK functions)
- DPDK poll thread replaces epoll reader pool: NIC polling, smoltcp TCP, frame decode, disruptor publish
- Separate DPDK response stage: encodes to mpsc TX channel, DPDK poll thread drains into smoltcp sockets
- Non-blocking Ed25519 auth handshake state machine (Challenge → ChallengeResponse → ServerReady)
- Core pinning for DPDK poll thread (`--dpdk-core`, default 7)
- CLI args: `--dpdk-eal-args`, `--dpdk-port`, `--dpdk-ip`, `--dpdk-prefix-len`, `--dpdk-gateway`, `--dpdk-core`
- Smoke test passes end-to-end via TAP virtual device (p50=17µs)
- DPDK bench client (`crates/bench/src/dpdk.rs`): smoltcp outbound connections, non-blocking auth, pipelined send/recv
- 35 unit tests (frame parsing, request mapping, shared request module)
- SR-IOV setup script auto-detects bond slaves, PCI addresses, VLAN ID (`scripts/dpdk-setup.sh`)
- Shared `request.rs` module eliminates request processing duplication across all 3 transport backends
- **Validated on real hardware**: Intel X710 SR-IOV VF on Cherry Servers (Ubuntu 24.04, AMD EPYC). DPDK `net_iavf` driver probes the VF, smoltcp listens on the VLAN IP. Server starts and accepts connections.

**Server requirements for DPDK SR-IOV:**
- NIC must expose SR-IOV PCI capability. Check before renting:
  `lspci | grep -i ethernet; find /sys/bus/pci/devices/ -name sriov_totalvfs -exec sh -c 'echo "$(basename $(dirname {})): $(cat {})"' \;`
- If `sriov_totalvfs` shows a number > 0, the NIC works. The model matters less than whether SR-IOV is exposed.
- Ubuntu 24.04's HWE kernel (6.14) and GA kernel (6.8) both work with Intel X710 SR-IOV.
- Ubuntu 24.04's kernels do NOT have SR-IOV for Intel E810-XXV SFP (tested, confirmed missing).
- Debian Trixie's ice module lacks SR-IOV entirely. Don't use Debian for DPDK.
- Two servers on the same VLAN required for benchmarking — switches don't hairpin traffic to the same server.

**Benchmark results (Cherry Servers, AMD EPYC, Intel X710 10GbE, VLAN, Debian 6.12 kernel):**

Compared DPDK+smoltcp server vs kernel TCP server, both with kernel TCP bench client, 1 client, window 256, 10M order pairs, full journal durability:

| Transport | Throughput | p50 | p99 | p99.9 | max |
|-----------|-----------|-----|-----|-------|-----|
| Kernel TCP (io_uring) | 669K ord/s | 347 µs | 444 µs | 488 µs | 4.5 ms |
| DPDK + smoltcp | 446K ord/s | 471 µs | 707 µs | 10.2 ms | 43.1 ms |

**Result: kernel TCP is faster on this hardware.** smoltcp's software TCP processing (checksums, per-packet handling, no TSO/GRO) adds more overhead than the syscalls it eliminates. The DPDK path would benefit from:
- Hardware checksum offload (smoltcp computes in software)
- Direct PF binding instead of SR-IOV VF (eliminates PF switching fabric overhead)
- A NIC with Mellanox-style bifurcated driver (avoids SR-IOV entirely)
- Raw UDP transport (eliminates TCP overhead altogether)

**Known gaps (not yet implemented):**
- **X710 VF drops broadcast ARP**: VFs only receive unicast frames. Bench must pre-populate ARP (`ip neigh add`) or use unicast ARP. This blocks DPDK-to-DPDK testing. Mellanox ConnectX NICs support broadcast on VFs natively.
- **Server can't accept new connections after first client disconnects**: smoltcp listener doesn't recover — must restart server between runs.
- **No idle connection timeout**: authenticated connections have no timeout (epoll/uring path uses `--connection-timeout-secs`).
- **No `max_connections` enforcement**: the DPDK poll loop accepts all connections without checking the limit.
- **`active_connections` counter not wired**: counter exists but is never incremented/decremented.

## Dead Ends / Investigated & Rejected
**How to apply:** The matching engine is not the bottleneck. The journal fsync stage gates pipeline throughput; TCP pipelining (window=256) effectively hides fsync latency. Further throughput gains require reducing transport overhead (UDS, kernel bypass) or journal I/O optimization (overlapped io_uring writes). See Performance Tuning leads in the README.

Core layout: 0=OS/IRQ, 1-3=pipeline (journal/matching/response), 4-5=readers, 6=repl-sender, 7=event-publisher, 8+=bench.

### Prioritized performance leads

| Priority | Optimization | Est. gain | Complexity |
|----------|-------------|-----------|------------|
| 1 | Embed ReservationSlot in RestingOrder | 5-10% matching | Moderate |

| 4 | AF_XDP + smoltcp userspace TCP | 20-40% latency | Very high (6+ months) |
| 5 | DPDK + F-Stack | 2-3x throughput | Extreme, GPL concern |

Options 2-5 are mutually exclusive kernel bypass paths (pick one). See README Performance Tuning leads for details.
