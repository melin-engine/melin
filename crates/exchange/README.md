# Melin Exchange Core

An exchange core built on the [Melin sequencer](../../README.md). Handles order matching, account balances, risk controls, circuit breakers, fee schedules, and market data. The full critical path from order ingestion to durable execution.

**Design partners wanted.** We are looking for one or two design partners willing to run Melin in a non-critical capacity (internal crossing, a new instrument, a parallel-run alongside an existing engine) in exchange for direct engineering support and influence over the roadmap. Get in touch: [contact@melin-engine.com](mailto:contact@melin-engine.com).

## Performance

The single-threaded exchange core (order matching, risk checks, balance updates, fee calculation) with no network, journal, or replication. Measures the `Exchange::execute()` hot path on a single core under realistic order flow; 30 s measurement window after 10 s warmup. Single AMD EPYC 9255 (24C Zen 5, SMT off).

| Throughput | p50 | p99 | p99.9 | p99.99 | p99.999 | p99.9999 | p99.99999 |
|------------|-----|-----|-------|--------|---------|----------|-----------|
| 4.60M/s | 0.10 µs | 0.42 µs | 0.58 µs | 0.77 µs | 0.99 µs | 1.17 µs | 1.35 µs |

End-to-end numbers (including journal, replication, and network) are in the [sequencer README](../../README.md#benchmarks).

## Correctness

- Strict price-time priority verified by property-based tests across thousands of random order sequences
- Cross-validated against independent matching engine implementations and real market data
- Deterministic replay guarantees identical state from the same journal
- Property-based, fuzz, crash-injection, cross-engine differential, and multi-process failover tests: more than a thousand test scenarios in total

## Order Types

- Market, Limit, Stop (stop-loss), Stop-Limit
- Time-in-force: GTC, IOC, FOK, Day, GTD (Good-Til-Date)
- Post-Only (maker-only, reject if would take)

## [Matching Engine](../../docs/matching-engine.md)

- Strict price-time priority
- Execution reports: Fill (with fees), Placed, Triggered, Cancelled, Rejected, Replaced, InstrumentStatusChanged
- Multi-instrument exchange with shared account balances
- Cancel-replace / order amendment (atomic price/qty modify; preserves queue priority when price unchanged, loses priority on price change)
- Circuit breakers (price bands, trading halts, configurable per instrument)
- Instrument lifecycle management (disable/enable/remove; disable cancels all resting orders atomically, remove reclaims memory)

## [Fees](../../docs/fee-model.md)

- Maker/taker fee model (per-instrument, in basis points, configurable via admin API)
- Fee deduction on fill (fees in quote currency, deducted from buyer reservation and seller proceeds)
- Collected fees credited to a dedicated fee account. Operators can withdraw via admin API; balance conservation enforced across all accounts

## [Risk & Accounting](../../docs/risk-checks.md)

- Per-account, per-currency balance management (reserve on order, update on fill, release on cancel)
- Self-trade prevention (per-order modes: CancelNewest, CancelOldest, CancelBoth)
- Fat finger checks (max order size, max notional value, configurable per instrument)
- Kill switch (cancel all resting orders and pending stops for an account across all instruments)
- Per-account order ID high-water mark (prevents double-execution on crash-recovery retry)
- Price band checks (static lower/upper bounds, per-instrument)
- Withdraw (debit funds, auto-evict zero-balance entries)

## [FIX Gateway (OE)](../../docs/oe-gateway.md)

- Single-threaded io_uring event loop terminating many concurrent FIX 4.4 sessions
- Stateless session model: each connection starts at MsgSeqNum 1; cross-reconnect recovery is handled by the output event channel
- Standard FIX 4.4 gap recovery (ResendRequest, SequenceReset-GapFill) on both directions
- Bounded per-session outbound store with automatic GapFill for evicted ranges
- TargetCompID validation, heartbeat / TestRequest liveness, configurable per-session message rate limits

## [Authentication & Authorization](../../docs/admin-guide.md)

- Ed25519 challenge-response handshake
- Four permission roles: Operator (exchange configuration), Trader (order submission/cancellation), Custodian (deposit/withdraw), ReadOnly (heartbeats)
- Operator API (instrument management and lifecycle, circuit breakers, kill switch, risk limits, fee schedules, end-of-day, live stats dashboard)
- Per-key idempotency (sequence numbers with duplicate rejection; safe to retry on timeout without double-applying)

## [Operations](../../docs/operations.md)

- Structured logging with disciplined error levels (`error!` reserved for server malfunctions, never client-induced)
- Health/liveness endpoint with Prometheus metrics (active connections, events processed, journal sequence, replication lag, pipeline health, input queue depth, trading state)
- Admin TUI dashboard (live connection count, events processed, throughput, journal sequence)
- Sparse account storage to reduce memory usage, see [account lifecycle](../../docs/account-lifecycle.md)

## License

Licensed under the [Business Source License 1.1](../../LICENSE). Production use requires a commercial license from P.L.S.C. Contact [contact@melin-engine.com](mailto:contact@melin-engine.com).

Each version of the Licensed Work converts to Apache License, Version 2.0 on the fourth anniversary of its first public distribution.
