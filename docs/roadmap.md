# Roadmap

Planned features sorted by value/complexity ratio for commercial readiness (exchange operators and investors).

## Active

| # | Feature | Commercial value | Complexity | Value/effort | Why |
|---|---------|:---:|:---:|:---:|-----|
| 1 | Catch-up from journal files | High | High | ★★☆☆☆ | Critical for production HA, but significant work (read historical segments, switch to live). |
| 2 | Snapshot transfer | Medium | High | ★☆☆☆☆ | Needed for full HA, but catch-up from journal comes first. |
| 3 | DPDK replication e2e testing | High | Low | ★★★★☆ | Test DPDK replication (smoltcp sender + receiver) on real multi-queue NICs with the bench suite. Virtual devices (TAP, af_packet) only support 1 queue so can't smoke-test locally. Branch: `feat/dpdk-bench-suite` has the implementation + bench suite integration. Needs SR-IOV hardware to validate. |
| 4 | Cross-engine differential testing | High | Medium | ★★★☆☆ | Build or use an independent matching engine implementation as a reference oracle. Feed identical order streams to both engines and compare execution reports. Catches correctness bugs that property tests miss — edge cases in price-time priority, partial fills at level boundaries, cancel-replace races, STP interactions. |
| 5 | Targeted edge-case tests | High | Low | ★★★★☆ | Cancel-replace crossing the spread, FOK+STP interaction, per-fill fee rounding conservation, stop trigger cascades, cancel-replace priority preservation, snapshot round-trip with fees, market order on empty book, GTD expiry during matching. Branch: `feat/targeted-edge-case-tests`. |
| 6 | Full doc review | High | Low | ★★★★☆ | Many docs are stale after recent features (permissions, backpressure, Day TIF, GTD TIF, promotion, health endpoint). Review and update all docs/ files, CLAUDE.md, and README. Do once all other MVP features are complete. |
| 7 | Brand setup (domain, GitHub org, email) | Medium | Low | ★★★☆☆ | Register melin.io/melin.com, set up contact@ email, create GitHub org, transfer repo, switch commit email going forward. Do not rewrite history. |

## DPDK Transport Optimization

| # | Optimization | Est. impact | Complexity | Description |
|---|-------------|------------|------------|-------------|
| 1 | Reduce tracing overhead | 5-10% throughput | Low | Build with `max_level_info` or `max_level_warn` for production. Even gated `debug!` macros hit the tracing filter check on every call site. |
| 2 | Bypass smoltcp on hot path | Significant latency | Very high | For connected+authenticated clients, parse TCP directly from raw Ethernet frames. Eliminates smoltcp's per-packet overhead (neighbor lookup, socket dispatch, congestion window, timer checks). Custom minimal TCP for steady-state data path only. |

## Deferred

Features targeting regulated venues, gateway responsibilities, or with limited near-term value. Will revisit when the core product is mature or a specific buyer requires them.

| Feature | Why deferred |
|---------|-------------|
| SPDK journal | Userspace NVMe driver for journal writes. Bypasses kernel block layer entirely. TCP pipelining already hides fsync latency (fsync and no-persist throughput converged at ~8M/s), so the kernel block layer is no longer a bottleneck. High complexity, minimal expected gain. |
| io_uring SQPOLL | `IORING_SETUP_SQPOLL` eliminates `io_uring_enter()` syscall (~1-2µs) per submission. Measured 15% p50 improvement on loopback but tail regresses on SMT-enabled machines due to SQPOLL kernel threads contending with pipeline threads. Needs Cherry server testing with SMT off and `setup_sqpoll_cpu()` pinning. Branch: `feat/uring-sqpoll`. |
| Response gate bottleneck counter | Expose a metric counting how often the response stage blocked on the journal cursor vs the replication cursor. Currently `min(journal, replication)` is opaque — no visibility into which is the tail latency contributor. Low complexity, expose via health/metrics endpoint. |
| Dual-NVMe journal hedging | Two journal threads on separate NVMe drives, response stage gates on the fastest. Cuts tail latency from P(slow) to P(slow)². Free durability redundancy. Low complexity but requires a second NVMe slot. Revisit when journal fsync is the dominant tail contributor. |
| AF_XDP transport | DaMoN '25 found AF_XDP disappoints vs DPDK for small-message request-response workloads. DPDK transport already in progress. Revisit if DPDK proves insufficient. |
| Per-account trading permissions | Gateway concern — each firm's gateway instance restricts which accounts that connection can trade. Multi-tenant access control. |
| Order throttling | Gateway concern — rate limit per-account before requests reach the engine. SEC-04 audit finding. |
| Client failover | Gateway concern — reconnect + sequence resume is session management, not engine logic. |
| Market data dissemination | Gateway concern — fan-out, L2 book building, BBO computation consumes the output event channel. Engine's job stops at emitting events. |
| Replica analytics (6 items) | External service — throughput counters, latency histograms, volume/book depth analytics, audit trail queries, fee/PnL. Consumes the journal stream, not engine code. |
| Output event log | Regulatory audit trail. Depends on output event channel. |
| Subscription management | Gateway concern — the engine broadcasts, the gateway filters per-subscriber. |
| Iceberg orders | Niche — only matters for venues with institutional flow. |
| Auction mechanisms | Regulated venues only. Massive complexity (state machine, indicative pricing, uncrossing). |
| Failover detection + promotion | Leader election, split-brain prevention. Distributed systems hard mode — manual promotion covers the MVP. |
| Network partition handling | Fencing, quorum-based decisions. Same as above — extremely complex. |
| Dual / chain replication | Replicate to 2+ replicas for stronger durability guarantees. Chain replication (primary → replica A → replica B) reduces primary fan-out. Current architecture supports one replica only. |
| Position/exposure limits | Important for derivatives, less so for spot. Defer until a derivatives buyer needs it. |
| Tiered fee schedules | Volume-based tiers and per-account overrides. Can be implemented outside Melin — a fee service looks up the account's tier and sets the rate via the existing per-instrument fee API. |
| TLS | Most exchange deployments use VLAN instead. Only needed for compliance-driven buyers. |
| Hybrid UDP multicast + TCP recovery for event channel | Current event channel is pure TCP. Multicast would reduce latency for co-located subscribers but adds complexity (gap detection, retransmit). Defer until a buyer needs sub-microsecond market data. |
