# Roadmap

Planned features not yet implemented. Ordered by category; see the [priority roadmap](#priority-roadmap) at the bottom for commercial-readiness ordering.

## Order Types
- [ ] Iceberg (hidden quantity)

## Time-in-Force
- [ ] GTD (Good-Til-Date)
- [ ] Day

## Matching Engine
- [ ] Auction mechanisms (opening/closing/volatility auctions)

## Fees
- [ ] Tiered fee schedules (volume-based tiers, account-level overrides)

## Risk & Accounting
- [ ] Position/exposure limits
- [ ] Order throttling (per-account rate limiting)
- [ ] Custodian permission role — 4th permission level: can only Deposit and Withdraw, cannot trade or perform admin ops (instrument management, circuit breakers, risk limits, kill switch). Separates fund management from trading and exchange administration. Enables the gateway deposit/withdraw lifecycle pattern (see [docs/account-lifecycle.md](account-lifecycle.md)) without granting trading or admin privileges. Current roles: `Admin` (full access), `Trader` (submit/cancel orders), `ReadOnly` (heartbeats, future market data).

## Event Sourcing & Durability
- [ ] Output event log (durable ExecutionReport stream for audit trail)

## Networking
- [ ] Batched io_uring SEND in response stage (reduce per-response syscall overhead)
- [ ] TCP_CORK / MSG_MORE response batching (coalesce small frames into single TCP segments)
- [ ] Backpressure handling (defined policy when disruptor is full)
- [ ] TLS (encrypted client connections)
- [ ] Investigate network protocol optims (do we need a length field?)

## Gateway
- [ ] Output event channel from matching stage (broadcast — prerequisite for market data)
- [ ] Market data dissemination (L2 snapshots, trade feed, BBO push updates)
- [ ] Subscription management (subscribe/unsubscribe per instrument)
- [ ] Reference data management (instrument lifecycle)

## Authentication & Authorization
- [ ] Per-account trading permissions

## Metrics & Observability

Most analytics can run on a **replica** replaying the journal, keeping the primary's hot path free of instrumentation jitter.

### Primary node (lightweight, operational health)
- [ ] Metrics transport (Prometheus endpoint or stats file — must not touch the hot path)
- [ ] Disruptor queue depth / backpressure monitoring (input ring fill level)
- [ ] Health/liveness endpoint (beyond current `ServerReady` handshake)

### Replica or offline (journal-derived, zero primary impact)
- [ ] Order/fill/cancel throughput counters (events per second by type)
- [ ] Latency histograms (journal `timestamp_ns` → matching → response, per-event)
- [ ] Volume analytics (traded volume per instrument, per account)
- [ ] Book depth analytics (resting order counts, spread tracking)
- [ ] Audit trail queries (full event history for regulatory compliance)
- [ ] Fee/PnL accounting (when fees and position tracking exist)

## Redundancy & High Availability
- [ ] Halt trading on replica disconnect (currently degrades silently to local-only, acking un-replicated orders)
- [ ] Catch-up from journal files (late-joining replica reads historical entries before live stream)
- [ ] Snapshot transfer (replica too far behind for journal catch-up)
- [ ] Manual promotion (operator command to promote replica to primary)
- [ ] Failover detection and promotion (leader election, split-brain prevention)
- [ ] Client failover (reconnect to new primary, resume with sequence numbers)
- [ ] Network partition handling (fencing, quorum-based decisions)

## Priority Roadmap

Ordered by importance for commercial readiness (exchange operators and investors).

1. **Metrics & observability** — connection counts, queue depth, health endpoints. Operators need visibility.
2. **Auction mechanisms** — opening/closing/volatility auctions. Differentiator for regulated venues.
3. **Security hardening** — remaining [audit findings](security-audit.md): per-account order limits (SEC-03), order throttling (SEC-04), disk exhaustion handling (SEC-05), snapshot validation (SEC-09).

Also needed: backpressure policy, gateway scalability (epoll/io_uring multiplexing), per-account permissions, crash injection tests (kill server at random points during load, verify recovery produces identical state — validates journal/snapshot/rotation crash safety end-to-end).
