# Roadmap

Planned features sorted by value/complexity ratio for commercial readiness (exchange operators and investors).

## Active

| # | Feature | Commercial value | Complexity | Value/effort | Why |
|---|---------|:---:|:---:|:---:|-----|
| 1 | DPDK replication e2e testing | High | Low | ★★★★☆ | Test DPDK replication (smoltcp sender + receiver) on real multi-queue NICs with the bench suite. Virtual devices (TAP, af_packet) only support 1 queue so can't smoke-test locally. Branch: `feat/dpdk-bench-suite` has the implementation + bench suite integration. Needs SR-IOV hardware to validate. |
| 2 | Brand setup (domain, GitHub org, email) | Medium | Low | ★★★☆☆ | Register melin.io/melin.com, set up contact@ email, create GitHub org, transfer repo, switch commit email going forward. Do not rewrite history. |

## FIX Gateway Hardening

Follow-ups to take the FIX 4.4 gateway from minimum-viable to production-ready for a real exchange operator. The foundation (sessions, gap recovery, order entry, exec reports) is on `main`; these items make it deployable.

| # | Feature | Commercial value | Complexity | Value/effort | Why |
|---|---------|:---:|:---:|:---:|-----|
| 1 | Third-party FIX client soak test | High | Low | ★★★★★ | Current end-to-end tests use our own serializer on both sides — a closed loop that can't catch interop bugs. Run a sustained session against QuickFIX/J (or similar) to validate against an independent implementation. |
| 2 | IPv6 support | Medium | Low | ★★★☆☆ | `server_addr` and `listen_addr` are IPv4-only today (validation rejects IPv6). Many modern data centers require IPv6 dual-stack. |
| 3 | Market data (35=V/W/X) | Medium | High | ★★☆☆☆ | MarketDataRequest, snapshot/full refresh, incremental refresh. Requires a feed builder that consumes the engine's output event channel and maintains per-subscription book state. Larger surface than order entry. |

## DPDK Transport Optimization

| # | Optimization | Est. impact | Complexity | Description |
|---|-------------|------------|------------|-------------|
| 1 | Bypass smoltcp on hot path | Significant latency | Very high | For connected+authenticated clients, parse TCP directly from raw Ethernet frames. Eliminates smoltcp's per-packet overhead (neighbor lookup, socket dispatch, congestion window, timer checks). Custom minimal TCP for steady-state data path only. |

## Deferred

Features targeting regulated venues, gateway responsibilities, or with limited near-term value. Will revisit when the core product is mature or a specific buyer requires them.

| Feature | Why deferred |
|---------|-------------|
| SPDK journal | Userspace NVMe driver for journal writes. Bypasses kernel block layer entirely. TCP pipelining already hides fsync latency (fsync and no-persist throughput converged at ~8M/s), so the kernel block layer is no longer a bottleneck. High complexity, minimal expected gain. |
| Adaptive overlapped io_uring journal writes | The non-aggressive form is **already shipped** in `run_uring`: the journal stage submits `WRITE+RWF_DSYNC` async via io_uring and continues encoding the next batch while the NVMe write is in flight. CQEs are reaped non-blocking at two points per loop iteration; backpressure only kicks in when the next batch fills before the previous fsync completes. What is still deferred is the *aggressive* double-buffer variant (always keep two writes inflight, even for tiny batches). That was built and reverted because events caught in a small overlapped batch had their cursor delayed by one extra NVMe latency, hurting p99.9. The next attempt should only overlap when `pending > 16` events. Branch preserved. |
| WRITE_FIXED for journal | Register batch buffers via `IORING_REGISTER_BUFFERS`, use `IORING_OP_WRITE_FIXED`. Skips `get_user_pages()` per SQE (~100-200ns). Failed for socket I/O on kernel 6.8 but works for file writes. |
| Vectored journal writes | Encode each event into its own stack buffer, pass as iovec array to `pwritev2`. Eliminates memcpy-per-event into the batch buffer. Small gain (~1-2µs/batch). |
| io_uring RECVSEND_FIXED_BUF | `IORING_RECVSEND_FIXED_BUF` for registered buffer recv/send. Returned EINVAL on kernel 6.8; should work on 6.10+. Also test `IORING_RECVSEND_BUNDLE` for batched recv. Est. 15-25% throughput. |
| io_uring SQPOLL | `IORING_SETUP_SQPOLL` eliminates `io_uring_enter()` syscall (~1-2µs) per submission. Measured 15% p50 improvement on loopback but tail regresses on SMT-enabled machines due to SQPOLL kernel threads contending with pipeline threads. Needs Cherry server testing with SMT off and `setup_sqpoll_cpu()` pinning. Branch: `feat/uring-sqpoll`. |
| Dual-NVMe journal hedging | Two journal threads on separate NVMe drives, response stage gates on the fastest. Cuts tail latency from P(slow) to P(slow)². Free durability redundancy. Low complexity but requires a second NVMe slot. Revisit when journal fsync is the dominant tail contributor. |
| AF_XDP transport | DaMoN '25 found AF_XDP disappoints vs DPDK for small-message request-response workloads. DPDK transport already in progress. Revisit if DPDK proves insufficient. |
| Per-account trading permissions | Gateway concern — each firm's gateway instance restricts which accounts that connection can trade. Multi-tenant access control. |
| Replica analytics (6 items) | External service — throughput counters, latency histograms, volume/book depth analytics, audit trail queries, fee/PnL. Consumes the journal stream, not engine code. |
| Output event log | Regulatory audit trail. Depends on output event channel. |
| Subscription management | Gateway concern — the engine broadcasts, the gateway filters per-subscriber. |
| Iceberg orders | Niche — only matters for venues with institutional flow. |
| Auction mechanisms | Regulated venues only. Massive complexity (state machine, indicative pricing, uncrossing). |
| Replication chain hash verification | The wire protocol carries chain hashes in handshakes, heartbeats, and data batches, but the replica never verifies them against its local journal state. The primary unconditionally sends `StreamStart` without checking the replica's reported hash. Implementing verification would detect silent replica divergence (bit-rot, misrouted journal files) and trigger automatic snapshot re-sync. Deferred because validating a hash at an arbitrary historical sequence requires either a sequence→hash index or a journal re-scan. |
| Failover detection + promotion | Leader election, split-brain prevention. Distributed systems hard mode — manual promotion covers the MVP. |
| Network partition handling | Fencing, quorum-based decisions. Same as above — extremely complex. |
| Chain replication | Chain replication (primary → replica A → replica B) reduces primary fan-out. Dual parallel replication (up to 2 replicas) is implemented; chain topology is deferred. |
| Position/exposure limits | Important for derivatives, less so for spot. Defer until a derivatives buyer needs it. |
| Tiered fee schedules | Volume-based tiers and per-account overrides. Can be implemented outside Melin — a fee service looks up the account's tier and sets the rate via the existing per-instrument fee API. |
| TLS | Most exchange deployments use VLAN instead. Only needed for compliance-driven buyers. |
| Hybrid UDP multicast + TCP recovery for event channel | Current event channel is pure TCP. Multicast would reduce latency for co-located subscribers but adds complexity (gap detection, retransmit). Defer until a buyer needs sub-microsecond market data. |
