# Roadmap

Planned features sorted by value/complexity ratio for commercial readiness (exchange operators and investors).

## Active

| # | Feature | Commercial value | Complexity | Value/effort | Why |
|---|---------|:---:|:---:|:---:|-----|
| 1 | DPDK replication e2e testing | High | Low | ★★★★☆ | Test DPDK replication (smoltcp sender + receiver) on real multi-queue NICs with the bench suite. Virtual devices (TAP, af_packet) only support 1 queue so can't smoke-test locally. Branch: `feat/dpdk-bench-suite` has the implementation + bench suite integration. Needs SR-IOV hardware to validate. |
| 2 | Runtime journal rotation | High | Medium | ★★★★☆ | Rotation currently only fires at startup — a single long-running process grows the journal without bound. For a 24/5 or 24/7 exchange this is a production gap. Needs: size-threshold trigger, brief pipeline quiesce, snapshot + archive of old journal, atomic swap to a fresh file, coordination with the replica so both rotate at compatible sequence boundaries (or let each side rotate independently since their journals are byte-symmetric). Applies to both primary and replica. |
| 3 | Brand setup (domain, GitHub org, email) | Medium | Low | ★★★☆☆ | Register melin.io/melin.com, set up contact@ email, create GitHub org, transfer repo, switch commit email going forward. Do not rewrite history. |
| 4 | Safe-promotion handshake | Medium | Low | ★★★★☆ | The PROMOTE endpoint accepts the command unconditionally; the operator is responsible for verifying catch-up beforehand. The natural "wait for primary lag==0, then PROMOTE" recipe is unsafe — primary lag excludes disconnected slots from `min(slot0, slot1)`, so it can read 0 from one healthy replica alone before a freshly-spawned replacement has even connected. Make PROMOTE carry a target sequence (the last sequence the operator observed on the dying primary) and have the replica refuse promotion until its local `journal_seq` reaches it. Eliminates the silent data-loss footgun documented in `wait_for_replacement_catchup` in the failover tests. |
| 5 | Async primary ack (PLP path) | Medium | Low | ★★★★☆ | With `O_DIRECT + --no-fua`, journal flushes are plain `pwrite` calls — data lands in PLP-protected drive DRAM in ~1–5µs with no OS page cache involved. The same durability argument behind `--async-replica-ack` applies symmetrically to the primary: on any single-node failure the surviving node has the data, so the primary does not need to wait for `pwrite` to return before acking clients. Adding `--async-primary-ack` would queue the journal write and immediately ack, reducing end-to-end ack latency from `pwrite (~1–5µs) + network RTT` to network RTT only when combined with `--async-replica-ack`. Gate behind `--no-fua` — unsafe on non-PLP drives. |

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
| Verbatim byte-path journaling | Write the primary's encoded journal bytes directly on both primary and replica, skipping the decode/re-encode step on the replica side and an encode-from-struct pass on the primary. Saves CPU and reduces journal-stage work on the replay-critical path. Attempted once as a replica-only optimization (branch `replica-verbatim-journal`) but created an asymmetry that diverged fsync batching behavior between primary and replica — reverted in favor of a symmetric decode/re-encode on both sides. Revisit only with a plan to apply it on both sides simultaneously, preserving per-fsync size bounds. |
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
