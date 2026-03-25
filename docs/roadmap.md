# Roadmap

Planned features sorted by value/complexity ratio for commercial readiness (exchange operators and investors).

| # | Feature | Commercial value | Complexity | Value/effort | Why |
|---|---------|:---:|:---:|:---:|-----|
| 1 | Health/liveness endpoint | High | Very low | ★★★★★ | Every ops team expects this. Trivial HTTP or TCP check. |
| 2 | Halt trading on replica disconnect | High | Low | ★★★★★ | "What happens if the replica dies?" is a due diligence question. A flag + check in the accept path. |
| 3 | Metrics transport (Prometheus) | High | Low | ★★★★☆ | Operators won't run blind. Expose counters already tracked via a side-channel. |
| 4 | Disruptor queue depth monitoring | Medium | Very low | ★★★★☆ | Read the ring buffer cursors, expose as a metric. Near-free once metrics transport exists. |
| 5 | Backpressure handling | High | Low | ★★★★☆ | "What happens when the ring is full?" — needs a clear answer. Reject-with-error is simplest. |
| 6 | Day TIF | Medium | Very low | ★★★★☆ | Trivial — cancel at end-of-day. Most venues need it. |
| 7 | Manual promotion | High | Medium | ★★★☆☆ | "How do I failover?" is a deal-breaker question. Admin command to promote replica. |
| 8 | Output event channel | High | Medium | ★★★☆☆ | Prerequisite for market data, audit trail, and replica analytics. Unlocks many downstream features. |
| 9 | GTD TIF | Low | Very low | ★★★☆☆ | Easy add, nice checkbox. Less asked-for than Day. |
| 10 | Custodian role | Medium | Low | ★★★☆☆ | Separation of duties matters for regulated buyers. Small auth change. |
| 11 | Per-account trading permissions | Medium | Medium | ★★★☆☆ | Multi-tenant deployments need account-level access control. |
| 12 | Order throttling | Medium | Low | ★★★☆☆ | SEC-04 audit finding. Simple per-account counter on the hot path. |
| 13 | Snapshot schedule | Medium | Low | ★★★☆☆ | Operators don't want to trigger snapshots manually. Timer + existing save logic. |
| 14 | Output event log | High | Medium | ★★★☆☆ | Regulatory requirement, but depends on output event channel first. |
| 15 | Reference data management | Medium | Medium | ★★★☆☆ | Instrument disable/remove. Operators expect lifecycle management. |
| 16 | Catch-up from journal files | High | High | ★★☆☆☆ | Critical for production HA, but significant work (read historical segments, switch to live). |
| 17 | TLS | Medium | Medium | ★★☆☆☆ | Some buyers require it (compliance). Most exchange deployments use VLAN instead. |
| 18 | Tiered fee schedules | Medium | Medium | ★★☆☆☆ | Nice-to-have — most buyers customize fees anyway. |
| 19 | Position/exposure limits | Medium | Medium | ★★☆☆☆ | Important for derivatives, less so for spot. |
| 20 | Market data dissemination | High | High | ★★☆☆☆ | High value but large scope. Depends on output event channel. |
| 21 | Iceberg orders | Low | Medium | ★★☆☆☆ | Niche. Only matters for venues with institutional flow. |
| 22 | Auction mechanisms | High | Very high | ★☆☆☆☆ | Differentiator for regulated venues, but massive complexity (state machine, indicative pricing, uncrossing). |
| 23 | Snapshot transfer | Medium | High | ★☆☆☆☆ | Needed for full HA, but catch-up from journal comes first. |
| 24 | Client failover | Medium | High | ★☆☆☆☆ | Client-side reconnect + sequence resume. Significant protocol work. |
| 25 | Failover detection + promotion | High | Very high | ★☆☆☆☆ | Leader election, split-brain — distributed systems hard mode. |
| 26 | Network partition handling | High | Very high | ★☆☆☆☆ | Fencing, quorum. Same as above — extremely complex. |
| 27 | Subscription management | Low | Medium | ★☆☆☆☆ | Only needed with market data. Out of scope without it. |
| 28 | Replica analytics (6 items) | Low | Medium | ★☆☆☆☆ | Throughput counters, latency histograms, volume/book depth analytics, audit trail queries, fee/PnL accounting. Nice demos, but buyers build their own analytics. |
| 29 | Protocol optims investigation | Low | Unknown | ★☆☆☆☆ | Research, not a feature. No commercial value until proven. |
