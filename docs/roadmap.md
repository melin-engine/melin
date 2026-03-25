# Roadmap

Planned features sorted by value/complexity ratio for commercial readiness (exchange operators and investors).

| # | Feature | Commercial value | Complexity | Value/effort | Why |
|---|---------|:---:|:---:|:---:|-----|
| 1 | Backpressure handling | High | Low | ★★★★☆ | "What happens when the ring is full?" — needs a clear answer. Reject-with-error is simplest. |
| 2 | Day TIF | Medium | Very low | ★★★★☆ | Trivial — cancel at end-of-day. Most venues need it. |
| 3 | Manual promotion | High | Medium | ★★★☆☆ | "How do I failover?" is a deal-breaker question. Admin command to promote replica. |
| 4 | Output event channel | High | Medium | ★★★☆☆ | Prerequisite for market data, audit trail, and replica analytics. Unlocks many downstream features. |
| 5 | GTD TIF | Low | Very low | ★★★☆☆ | Easy add, nice checkbox. Less asked-for than Day. |
| 6 | Custodian role | Medium | Low | ★★★☆☆ | Separation of duties matters for regulated buyers. Small auth change. |
| 7 | Per-account trading permissions | Medium | Medium | ★★★☆☆ | Multi-tenant deployments need account-level access control. |
| 8 | Order throttling | Medium | Low | ★★★☆☆ | SEC-04 audit finding. Simple per-account counter on the hot path. |
| 9 | Snapshot schedule | Medium | Low | ★★★☆☆ | Operators don't want to trigger snapshots manually. Timer + existing save logic. |
| 10 | Output event log | High | Medium | ★★★☆☆ | Regulatory requirement, but depends on output event channel first. |
| 11 | Reference data management | Medium | Medium | ★★★☆☆ | Instrument disable/remove. Operators expect lifecycle management. |
| 12 | Catch-up from journal files | High | High | ★★☆☆☆ | Critical for production HA, but significant work (read historical segments, switch to live). |
| 13 | TLS | Medium | Medium | ★★☆☆☆ | Some buyers require it (compliance). Most exchange deployments use VLAN instead. |
| 14 | Tiered fee schedules | Medium | Medium | ★★☆☆☆ | Nice-to-have — most buyers customize fees anyway. |
| 15 | Position/exposure limits | Medium | Medium | ★★☆☆☆ | Important for derivatives, less so for spot. |
| 16 | Market data dissemination | High | High | ★★☆☆☆ | High value but large scope. Depends on output event channel. |
| 17 | Iceberg orders | Low | Medium | ★★☆☆☆ | Niche. Only matters for venues with institutional flow. |
| 18 | Auction mechanisms | High | Very high | ★☆☆☆☆ | Differentiator for regulated venues, but massive complexity (state machine, indicative pricing, uncrossing). |
| 19 | Snapshot transfer | Medium | High | ★☆☆☆☆ | Needed for full HA, but catch-up from journal comes first. |
| 20 | Client failover | Medium | High | ★☆☆☆☆ | Client-side reconnect + sequence resume. Significant protocol work. |
| 21 | Failover detection + promotion | High | Very high | ★☆☆☆☆ | Leader election, split-brain — distributed systems hard mode. |
| 22 | Network partition handling | High | Very high | ★☆☆☆☆ | Fencing, quorum. Same as above — extremely complex. |
| 23 | Subscription management | Low | Medium | ★☆☆☆☆ | Only needed with market data. Out of scope without it. |
| 24 | Replica analytics (6 items) | Low | Medium | ★☆☆☆☆ | Throughput counters, latency histograms, volume/book depth analytics, audit trail queries, fee/PnL accounting. Nice demos, but buyers build their own analytics. |
| 25 | Protocol optims investigation | Low | Unknown | ★☆☆☆☆ | Research, not a feature. No commercial value until proven. |
