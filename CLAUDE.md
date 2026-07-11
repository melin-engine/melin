# CLAUDE.md

> **This file must be kept up to date** as the project evolves — update structure, dependencies, and conventions whenever they change.

## Project

**Melin** — a deterministic, replicated sequencer for latency-critical applications, built on the **LMAX architecture** (single-threaded business logic, event sourcing, mechanical sympathy). Rust (edition 2024). Provides the event-sourced processing pipeline, durable journaling, synchronous replication, transport (kernel TCP and DPDK), and the application-agnostic server runtime. Applications (e.g. the Melin Exchange Core, maintained separately) plug in via the `melin-app` traits; `crates/examples/counter` is the reference application.

**Commercial product** — Every feature decision should be evaluated through the lens of "does this make the product more appealing to an operator of latency-critical, durability-critical systems?"

## Conventions

- Follow Rust best practices (idiomatic patterns, clippy clean, formatted with `cargo fmt`).
- Write unit tests for all non-trivial code. Skip only when genuinely unreasonable (e.g., trivial glue code).
- **Correctness is critical** — the sequencer carries financial infrastructure. Correctness always comes first.
- **Reasonably optimized from the start** — don't prematurely optimize, but make performance-conscious choices by default: minimize allocations, avoid locks on the hot path, favor cache-friendly data structures. Profile before micro-optimizing.
- **Always `cargo check` before committing** — run `cargo check` with the correct feature flags for all affected crates before committing. For DPDK code, check `melin-server-runtime` with `--features dpdk`.
- **No `.unwrap()` in production code** — use proper error handling, or an `.expect()` if really necessary. `.unwrap()` is fine in tests.
- **No `#[ignore]` on tests** — if a test fails, fix the bug. Never suppress a failing test with `#[ignore]`.
- **No silently ignored results** — do not discard `Result` errors via `let _ =`, `.unwrap_or(...)`, `.unwrap_or_default()`, `.ok()`, or similar swallowing patterns unless there is a clear reason (e.g., best-effort diagnostic writes). Handle errors explicitly. When discarding is genuinely the right call, leave a comment on the line above explaining *why* the error is being dropped.
- **Comment data structure and type choices** — always add a comment justifying why a specific collection, data structure, or numeric type was chosen (e.g., why `BTreeMap` over `HashMap`, why `u64` over `u128`).
- **Avoid `sed`** — do not use `sed` for inspecting or editing files. For editing, use the Edit/Write tools (exact, reviewable, no risk of a botched regex silently corrupting source). For reading a range of lines, use the Read tool. For searching, use `rg` (ripgrep). `sed` is acceptable only as a last resort in a throwaway shell pipeline where no dedicated tool fits — never to modify tracked files.
- **Log levels** — `error!`: server malfunctions only (bugs, journal I/O failures) — must never fire due to bad client input or client network issues. `warn!`: degraded operation that isn't a bug but needs attention (e.g., CPU pinning failed, resource limits approaching, unexpected-but-handled conditions). `info!`: server lifecycle events (start, stop, recovery). `debug!`: client-caused events (connections, disconnects, malformed messages, write failures).
- **Documentation audience** — files in `docs/` are written for operators and customers building on the sequencer, not contributors. Describe behavior, guarantees, and operational impact. Avoid implementation details (struct names, function names, borrow checker workarounds). Use `~~strikethrough~~` sparingly — prefer removing resolved items entirely rather than cluttering docs with changelog-style history. For contributors, use `docs/internal`

### Git
- **No co-authored commits** — do not add `Co-Authored-By` trailers.
- **Conventional Commits** — all commit messages must follow the [Conventional Commits](https://www.conventionalcommits.org/) spec (e.g., `feat:`, `fix:`, `refactor:`, `test:`, `docs:`, `chore:`).
- **Concise commit messages** — keep the subject line short and the body tight. Lead with what changed and why; skip exhaustive enumerations of every touched line. The diff is already in the commit — the message should add context, not duplicate it.
- **Never commit without explicit request** — do NOT commit unless the user explicitly asks (e.g. "commit", "commit and push"). Completing a task does NOT imply permission to commit. Always wait for the user to request the commit.
- **Never push without explicit confirmation** — always ask for review before pushing. Do not push unless the user confirms.
- **Commit intermediary steps** — for large multi-step tasks, commit each logical step separately rather than batching everything into one giant commit. This keeps history clean and bisectable. Always ask for review after each commit before moving to the next.
- **Always check `Cargo.lock`** — when dependencies change, `Cargo.lock` must be staged and committed alongside `Cargo.toml` changes. The pre-commit hook enforces this.
- **Never skip hooks** — do not use `--no-verify` to bypass the pre-commit hook. If the hook fails (clippy warnings, formatting), fix the issue first. The hook exists to catch problems before they enter history.

## Key Design Constraints

- **~100ns per event budget** — at 10M events/sec, every allocation, cache miss, and branch misprediction counts
- **Deterministic replay** — given the same input events, output must be identical; this is the foundation of event sourcing and crash recovery
- **Strict input ordering** — the sequencer assigns a total order to all events; no event may be processed, journaled, or replicated out of order
- **Durable journaling** — every event is persisted before acknowledgement; snapshots prevent full replay from genesis on recovery
- **Full audit trail** — every event must be journaled and reproducible (applications built on Melin face regulatory requirements)
- **Tail latency matters** — measure p99/p99.9, not averages
- **Extensive testing** — property-based and fuzz testing for edge cases (ring wrap-around, replica failover races, journal rotation boundaries, crash recovery)

## Working Style
- **Propose the best fix, not the simplest** — when there are multiple approaches, present the options with trade-offs and recommend the best one. Don't default to the quick hack.
- **Review before committing** — always review your own changes for correctness (including edge cases), test coverage, and documentation before attempting to commit. Don't rely on the user to catch issues.
- **One-liner commands** — when giving the user shell commands to run, always format them as a single line that can be copy-pasted directly. Do not use backslash continuations or multi-line formatting.


