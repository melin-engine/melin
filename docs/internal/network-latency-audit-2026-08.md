# Network latency audit — August 2026

Code-level read of every network-facing hot path — kernel-TCP client
ingress (io_uring reader), response egress, the replication wire path
(sender, receiver, acks, gate), and the DPDK/smoltcp transport — looking
specifically for costs that show up **under load** (p99/p99.9 at high
event rate), not at concurrency 1.

**Nothing here has been measured.** Same caveat as
`latency-audit-2026-07.md`: each item names a mechanism with a line
reference, not a quantified regression. Price each with the
`tick-to-trade` per-stage trace before landing it, and expect null
results on some.

Items landed are removed from this document as they ship; the numbering
below is the original audit's, so the gaps are done work, not omissions.
See "First pass" for what that was and what it bought.

## Framing — where loaded latency actually is

Measured 2026-08-12 (LAN suite, tcp-dual-repl, ~1.2 M orders/s, see
`io-uring-audit-2026-08.md`): single-order p50 ≈ 66 µs; loaded p50 ≈
130 µs, p99.9 ≈ 210 µs. So roughly half of loaded latency is queueing
and coalescing behaviour, and the ~24 µs replica ack round-trip is the
largest fixed component (`latency-audit-2026-07.md`, "Where the
latency actually is").

The July audit's cache-line and hashing items were measured as <1 % of
client latency at concurrency 1. Several of them are contention effects
that only exist under load with two replicas, so the ones with a real
mechanism reappear below, rated for the loaded case rather than
dismissed.

Everything the August io_uring audit fixed (`MSG_DONTWAIT` sends,
buf_ring recycling, WriteFixed, `SINGLE_ISSUER`, byte-threshold flush,
single egress writer) was verified as landed on `main`; nothing below
re-proposes it.

## Triage summary

Value = expected loaded-tail gain × confidence ÷ effort. Kernel-TCP path
unless marked DPDK. Ratings are judgement calls from code reading.

| # | Option | Gain (est.) | Risk | Effort | Value |
| --- | --- | --- | --- | --- | --- |
| 4 | Real NIC busy-poll on ack + ingress sockets (`SO_BUSY_POLL` is inert under io_uring) | several µs per hop × 3 hops, plausibly | Ops / kernel version | M | **High** (measure first) |
| 5 | Sub-batch replication publish every K encoded slots; publish regardless of group-commit | up to tens of µs on heads of large batches | Low | M | Med–High |
| 6 | Pad the gate's cursors — **tried, measured, reverted**; read the entry first | sub-µs per gate wake, and a measured throughput cost as implemented | Demonstrated | M | **Low** |
| 7 | `DEFER_TASKRUN` on the reader ring (residue) | 5–25 % reader-core cycles at ~1 M msg/s, less jitter | Med (CQ-read sites) | S | Med |
| 10 | Shard egress across K response consumers | divides per-flush xmit cost by K; only >32 dirty conns | Med | M–L | Low–Med |
| 11 | DPDK: fastcp TCP 4-tuple index caps at 64 and leaks on close | 10×+ per-packet ingest cost after churn | Low | S+S | **Very high** (DPDK) |
| 12 | DPDK: zero-copy RX cap of 32 segments/socket drops the excess | likely the DPDK throughput limiter; ≥1 RTT stalls | Med | M | **High** (DPDK) |
| 13 | DPDK: `poll()` is O(sockets) and runs O(N) times per iteration | dominant at N ≥ 64 | Low / Med | S / M | High at scale (DPDK) |
| 14 | DPDK: mempool sizing, TX-alloc `assert!`, neighbor cache 8, 528 B `TxFrame` copies | removes crash/drop cliffs, ~100–300 ns/frame | Low | S | Med (DPDK) |

### Suggested order

Kernel-TCP: instrument and decide 4 / 5 / 7 on numbers; 10 only with a
profile that has more than ~16 connections; 6 only after a microbenchmark
justifies a second attempt. DPDK: 11 → 14 (neighbor cache, mempool) →
12 → 13.

### First pass (landed, 2026-08-17)

Items 1, 3, 9 and 2 shipped in that order, one commit each, and are cut
from this document. What they did: the replica's pending-ack queue
merges instead of blocking the receiver; the response stage bounds
buffered-response *age* by slot count as well as bytes; the hygiene
bundle (FxHash connection table, dirty flag + list, single append per
slot against a pre-encoded `BatchEnd`, amortized idle clock,
`TCP_QUICKACK` re-armed only on a silent receive cycle); and the
receiver commits + records an ack target per frame with a bounded slot
count per call.

Two of them still need the regime that motivated them before their
headline claim can be believed:

- **Item 1** removes a *stall mode* — a replica fsync outlier freezing
  the receiver. A profile where that never fires cannot show it.
  `PendingAckQueue::merged()` on the replica is the tell: 0 for a run
  means the path never engaged. Exercise it with a slow journal (EBS) or
  a single replica, so there is no fastest-of-two smoothing to hide it.
- **Item 3** bounds buffered-response age across *many* connections; at
  a handful of closed-loop clients the idle-path flush already fires
  constantly. Needs > 32 connections, ideally open-loop.

The rest of the first pass is per-cycle CPU and syscall cost — the kind
that shows up on every path in absolute µs. Price it that way, not as a
percentage: the same saving is a large fraction of a short local path
and a small one of a loaded LAN path, so a percentage says more about
the profile than about the change.

### Second pass (2026-08-17)

Landed: item 8 in full — both response stages borrow the output batch in
place (`read_contiguous`) instead of memcpying up to 1024 slots before
touching the first — and the cheap half of 7, where the reader skips its
`io_uring_enter` when completions are already visible and the SQ is
empty. The residue of 7 is in its entry below.

**Reverted: item 6.** Its cache-line padding cost sustained replication
throughput, reproducibly, and a second attempt at the same idea did not
recover it. The entry below records what that rules out and what a retry
would have to do differently. It is the only item here with a measured
negative result, and the general lesson is worth more than the item: a
change justified by a *reader's* cache behaviour has to be validated on
a profile where that reader exists — the in-process replication bench
runs no durability gate at all.

Same caveat as the first pass for what did land: unmeasured per item.

---

## Kernel-TCP: replication path

### 4. `SO_BUSY_POLL` is set on every hot socket and inert under io_uring RECV

**Where:** `server.rs` (client accept: `set_busy_poll` with a comment
claiming the reader "already busy-spins"), `replication/tcp_sender.rs`
(ack socket), `replication/tcp_receiver.rs` (data socket); RECV via
`opcode::Recv` / `RecvMulti`; `reader.rs` blocks in `submit_and_wait(1)`
and `spawn_reader` takes no `busy_spin`.

io_uring's cqring wait does not consult per-socket `SO_BUSY_POLL`. The
recv is issued with `MSG_DONTWAIT`, so `sk_busy_loop(nonblock=1)` does at
most one NAPI pass at arm time, then the thread waits for the poll
wakeup. Every hop — client → reader, primary → replica, replica ack →
primary — is IRQ → softirq on the IRQ core → task wake → (possibly)
core exits idle. Typically 3–10 µs per hop depending on IRQ coalescing
and affinity, worst at burst tails when the CQ had drained. The only
NAPI busy-poll for io_uring is `IORING_REGISTER_NAPI` — **kernel ≥ 6.9;
production hosts run 6.8**. io-uring 0.7.14 exposes `register_napi`
(`Cargo.lock` pins 0.7.12).

**Fix.** Measure a variant. On ≥ 6.9: `register_napi` with
`busy_poll_to ≈ 50 µs`, `prefer_busy_poll`, gated on `busy_spin`, runtime
fallback on `EINVAL` (same hypervisor-filtering class already seen for
`PBUF_RING`). On 6.8: a bounded userspace spin of nonblocking `recv`
(each does one NAPI pass) or an `epoll_wait`-based wait with
`net.core.busy_poll`, cheapest to try on the sender's ack path (17-byte
frames). Fix the misleading comments either way.

Gain: needs measurement; plausibly several µs per hop, a visible slice of
the 24 µs RTT, plus removal of wake-from-idle jitter on ingress. Risk:
ops (burns cores already burned; NAPI-capable driver; kernel version).
Effort: M (S for the ack-side experiment). Confidence: high on "inert",
low–medium on gain size.

### 5. Replication publish waits for the whole journal batch to encode

**Where:** `crates/core/transport-core/src/pipeline.rs` — the journal
sequencer's `read_contiguous(MAX_JOURNAL_BATCH = 4096)`, per-slot
accumulation into `input_batch_buf`, `publish_input_batch_to_rings` only
in `submit_batch`; and `submit_batch` runs only when `should_sync`, so
`--group-commit-us > 0` delays the replication publish too.

With N ready slots, replication publish (and disk submit) happen after
all N are encoded — ~70 ns/event measured, so 4096 slots ≈ 280 µs before
the first reaches the wire. Under hybrid the local persisted gate waits
the same, but the replica leg is the longer one, so overlapping it with
the tail of the encode pays when N is large. Operators can already trim
`--max-journal-batch` as a blunt lever.

**Fix.** Publish sub-batches to the replication rings every K encoded
slots (64–256) or when the buffer crosses ~1 MSS, without changing the
disk batch; the protocol already tolerates multiple frames per fsync
batch. Publish the accumulated `InputBatch` at the end of each encode
span regardless of the sync decision. Must respect the mark-barrier
ordering (publish before the barrier's `submit_batch`).

Gain: needs measurement; up to tens of µs on the heads of large batches;
equal to the configured delay when group-commit is set. Risk: low.
Effort: M (S for the group-commit part). Confidence: medium.

### 6. The gate spins on lines written per ack and per SEND — TRIED AND REVERTED

**Where:** `crates/core/transport-core/src/replication/metrics.rs`
(`ReplicationMetrics`), `cursors.rs` (`ReplicaSlotCursors`,
`DurableWireSeqCursor`), `response.rs` (`with_cursor_view` in the gate
spin).

The mechanism is real: the gate's spin Acquire-loads both cursor arrays
every iteration, and `bytes_sent` (an RMW per completed SEND) shares
their line, so a write the gate does not care about invalidates the line
it is spinning on at SEND rate. Two replicas also false-share
`acked_sequence[0]` against `acked_sequence[1]`, 8 bytes apart.

**Read this before trying again.** The "cheap half" — `repr(C)` plus
padding to separate the writer-hot counters, and `#[repr(align(128))]`
on the two cursor allocations — was implemented, measured, and reverted.
It cost sustained throughput on the in-process replication bench,
consistently and reproducibly, and a follow-up that regrouped the fields
by *writing event* (the four fields `record_ack` touches together
compacted onto one line) recovered essentially none of it. What that
rules out is the field grouping being the mechanism; what is left is
that the same commit grew three allocations several-fold and forced
128-byte alignment on them, which moves every later allocation in the
arena. That is a layout effect, not a sharing effect, and no amount of
further field-shuffling addresses it.

Note also that the bench which showed the cost runs no durability gate
(it drains the output ring with a no-op), so it sees this item's cost
with none of its benefit. That does not make the cost less real — it is
a throughput ceiling the sender path pays in production too — but it
means the item was never actually disproven, only shown to be a bad
trade as implemented.

A retry needs, in order: a microbenchmark
(`crates/core/pipeline/examples/false_sharing.rs`) establishing the win
in isolation, a change that does not alter allocation sizes or
alignments of anything on the sender's path, and validation on the
in-process replication bench *before* the latency benches. Absent that,
leave it alone — this is the one item in this document with a measured
negative result.

Also open and independent of the above: the gate re-reads two separately
allocated `Arc<AtomicBool>` `replica_active` flags per iteration, so its
read set spans more lines than it needs to.

Gain: sub-µs per gate wake; will not show in an end-to-end bench. Risk:
demonstrated. Value: **Low** until the microbenchmark exists.

### 9 (residue). Replica receiver copies and legacy provided buffers

**Where:** `tcp_receiver.rs`, `receiver_transport.rs`.

Left over after the hygiene bundle landed: the replica receiver still
re-provisions buffers with legacy `ProvideBuffers` per RECV (the client
reader moved to `buf_ring`; low gain, worth doing for consistency), and
still pays two 128 B copies per slot — decode into `slot_buf`, then into
the ring — the same shape as July #9, ~10–30 ns/slot.

## Kernel-TCP: response egress

### 10. `flush_sends` does all TCP transmit work inline on the gate thread

**Where:** `response.rs` `flush_sends` — one SEND SQE per dirty
connection, `submit_and_wait(pending)`; with `MSG_DONTWAIT` each SEND
executes inline in the syscall (~1.5–5 µs each).

N dirty connections → N × that on the response thread, during which no
gate polling or encoding happens. It is the pre-gate flush, so it
usually overlaps the wait, but once N × cost exceeds the fsync/ack
interval the flush *is* the latency, and it grows linearly with
connection count.

Options: shard egress — the output ring supports multiple consumers; K
response consumers each owning `conn_id % K`, each spinning the same gate
cursors (M–L, K threads on the same cursors, `active_connections`
accounting split, slot reads duplicated). Completion-driven sends were
explicitly rejected in the August audit for buffer-ownership complexity.
Measure with a > 32-connection profile before investing. Related, cheap:
client fds are unregistered `types::Fd` on the response ring (fget/fput
per SEND) — a sparse fixed table updated on connect/teardown saves
~50–100 ns per SEND.

## Kernel-TCP: client ingress

The input disruptor is single-producer: no claim CAS, no lock, one
Release store per ≤ 16 frames. The "many connections contending on the
ring" class of problem does not exist; everything below is the reader
thread's own loop and its kernel interface.

### 7. Reader ring runs in default task-work mode

**Where:** `crates/core/server-runtime/src/reader.rs` — ring built with
`setup_single_issuer()` only; the comment defers to the journal ring's
measured rationale against `COOP_TASKRUN` / `DEFER_TASKRUN`.

Multishot RECV CQEs are not posted from softirq: data arrival → poll
wake → task_work queued to the reader task with `TWA_SIGNAL`. When the
reader is running in userspace parsing a drain (the loaded regime), the
kernel notifies the reader core, it traps in, runs the recv (copy into a
provided buffer), posts the CQE, returns — one kernel entry/exit per
arrival batch, preempting the parse loop at arbitrary points. The journal
ring's rationale (storage CQEs land from IRQ context, reaped with zero
syscalls) is category-specific and does not transfer to a socket ring.
With `DEFER_TASKRUN` (needs `SINGLE_ISSUER` — already set) that work is
deferred to the reader's next `io_uring_enter(GETEVENTS)`, which the loop
already performs every iteration; a CQE produced by preemption at time T
is still only processed in the next drain, so no latency is added.

**Fix.** `.setup_defer_taskrun()` behind a bench, and audit every CQ read
site: with `DEFER_TASKRUN`, `submit()` (no GETEVENTS) does not run local
task work, so the shutdown quiesce (submit then poll `completion()`)
would see nothing until its deadline — use `submit_and_wait(1)` there.
That failure mode is silent (a slower shutdown, nothing logged), which
is the main risk in the item.

Note the flag and the landed enter-skip below pull against each other:
the skip works precisely *because* default-mode task work posts CQEs
while the reader is in userspace. Under `DEFER_TASKRUN` the CQ would be
empty at that check and the skip would simply never fire. Bench them as
alternatives, not as a pair.

Gain: unknown, needs measurement; plausibly 5–25 % of reader-core cycles
at ~1 M msg/s and a jitter reduction. Risk: M (correctness of CQ-read
sites; behaviour otherwise identical). Effort: S. Confidence: medium.
Overlaps io_uring audit #5, which declined the flags without a bench —
this is the argument for running that bench.

**Landed from this item:** the enter-skip. The reader now enters the
kernel only when it has SQEs to hand over or an empty CQ, via
`ring_entry`. `register_ring_fd` is still open — it needs io-uring
0.7.14 and the workspace pins 0.7.12.

### Lower priority, ingress

- Head-of-line across connections inside a drain: CQEs are processed in
  CQ order and every complete frame in a CQE's payload is parsed before
  the next CQE; the kernel's inline multishot retry can produce up to
  32 × 4 KiB CQEs from one socket in one task-work run (~2.5 k small
  frames ≈ 200 µs of parse ahead of the next connection's single frame).
  Uniform load: nothing changes; one bulk sender next to latency-sensitive
  clients: real p99. A per-CQE frame cap with a "pending parse" list is
  the same missing mechanism as the accepted `PipelineFull` strand issue
  in the io_uring audit — one fix covers both. Effort M, fairness only.
- 4 KiB provided buffers → one recv + task-work iteration + CQE + parse
  call + commit per 4 KiB when a backlog exists; 16 KiB buffers quarter
  the per-byte overhead in the falling-behind regime.
  `IORING_RECVSEND_BUNDLE` needs 6.10. Effort S, needs measurement.
- Input ring: 1 M × 128 B = 128 MiB, allocated without `MADV_HUGEPAGE`
  (dTLB miss every 32 slots on the reader's write path); the comment in
  `pipeline.rs` still says "~72 bytes per slot … fits in L3". Depth also
  equals worst-case queueing under overload (~350 ms at 3 M/s before
  `ServerBusy`) — an operator knob with a documented latency bound would
  suit latency-critical deployments better than a fixed 1 M.
- 3–4 clock reads per drain iteration where one would do (~60–80 ns,
  visible only when drains are 1–2 CQEs).

## DPDK / smoltcp (fastcp)

`melin-dpdk` depends on the published `fastcp` crate; fastcp-side fixes
need a publish + bump. One poll thread (the EAL main lcore) does RX, TCP,
decode, ring publish, TX and both replication slots; the outer loop
visits every connection every iteration and calls `transport.poll()` at
the top, every 4 connections, and at the end, plus `repl_driver.tick()`
at the same cadence (which polls again whenever it queued data).

### 11. fastcp TCP 4-tuple index caps at 64 live entries and leaks on close

**Where:** `fastcp/src/iface/tcp_socket_index.rs` (`CAPACITY = 128`,
refuses insert at 50 % load), `fastcp/src/iface/interface/tcp.rs`
(miss → linear scan of all sockets with `accepts()`, then re-insert
which fails again). Removal only happens lazily on a stale hit;
`SocketSet::remove` never touches the index and melin's `close()` has no
way to.

Connection #65+ (concurrent) is never indexed; worse, closed connections
keep their entries, so after 64 connection *lifetimes* (a few bench runs
against one server process) every new connection is unindexed for its
whole life, and each of its segments costs O(live sockets) `accepts()`
checks (~10–20 ns each). ~200 ns/pkt at 16 sockets, tens of µs/pkt at
1000 — enough on its own to saturate the poll core, and a plausible
contributor to "DPDK throughput lower than TCP".

**Fix.** fastcp: raise `CAPACITY` to ≥ 2 × `MAX_CONNECTIONS` (~32 B/slot),
add a public `Interface::forget_tcp_socket(handle)` that melin's `close()`
calls, index at accept time rather than on the first data segment.

Gain: large at N > 64 or after churn; ~100–200 ns/pkt even at small N.
Risk: low. Effort: S (fastcp) + S (melin). Confidence: high.

### 12. Zero-copy RX holds ≤ 32 segments per socket; the excess in one burst is dropped

**Where:** `fastcp/build.rs` (`ZERO_COPY_RX_MAX_SEGMENTS = 32`, env-only),
`fastcp/src/socket/tcp.rs` (`Dropped`: no ACK, no retain), consumer drain
only in `recv_into_vec`, called once per connection per outer iteration
(and once per `poll_recv` on the replica).

`poll()` ingests up to 64 frames per port before any drain; any burst
> 32 data segments to one socket between drains drops the rest.
Replication is the worst case: the primary flushes up to 512 KiB (~350
MSS segments) in one `flush_tx`, the replica's `rx_burst(64)` delivers 64
for the same socket → 32 stored, 32 dropped, every poll. Recovery is via
dup-ACK fast retransmit or the 1 ms RTO, and smoltcp's retransmit rewinds
to `local_seq_no` and resends the whole outstanding window, re-triggering
the drop. Pipelined clients (64 KiB advertised window) can hit it too. For
bulk replication this caps effective throughput at ~46 KiB per drain plus
retransmit churn; for clients it is a sporadic ≥ 1 RTT stall — a p99.9
artefact. Strong candidate for the "smoltcp investigation" item in the
AWS bench notes.

**Fix.** (a) advertise a window bounded by free ZC slots × MSS so the
peer cannot exceed the cap; (b) raise the cap
(`SMOLTCP_ZERO_COPY_RX_MAX_SEGMENTS=256`, ~40 B/segment) and/or spill to
the copy-path `rx_buffer` when the array is full instead of dropping;
(c) drain touched sockets after every poll (item 13). Do (a)+(b) first;
a quick probe is the env var alone with replication throughput measured
before/after.

Gain: needs measurement; potentially the dominant DPDK replication
throughput limiter. Risk: medium (window logic). Effort: M (fastcp).
Confidence: high on mechanism, medium on magnitude.

### 13. `poll()` is O(all sockets), and runs O(N) times per outer iteration

**Where:** melin `dpdk_transport.rs` (poll at top, every 4 connections,
end; `flush_tx_queues` iterates every socket), `replication/dpdk.rs`
(extra polls per slot per tick); fastcp `iface/interface/mod.rs`
(`Interface::poll` egress loop repeats `socket_egress` until a pass sends
nothing, each pass visiting every socket at ~25 ns idle; default
`dispatch_burst_limit = 4`, never raised by melin, so a 350-segment
replication burst forces ~88 full passes).

Rough model: N=64 → ~4 µs/poll × 20–50 polls/iteration → 100–200 µs
iteration; N=256 → ~1 ms; N=1024 → ~15 ms. The iteration length is also
the visitation latency for a request that just missed its connection's
turn: ingest is scan-driven, not event-driven — data for an
already-visited socket sits in ZC segments (holding mbufs) until the next
lap.

**Fix.** melin: dirty-TX handle list so `flush_tx_queues` is O(pending)
(S); `set_dispatch_burst_limit(64..256)` on replication sockets (S);
adaptive mid-iteration polls (only when the last `rx_burst` was full)
(S). fastcp: return the handles that received payload from
`poll_ingress_batch_zero_copy` and drain exactly those in arrival order
right after each poll — melin then scans all connections only on the slow
tick (M); a "needs-egress" set so `socket_egress` is O(active) (M).

Gain: dominant at N ≥ 64; small at N ≤ 8. Risk: low for the melin parts,
medium for the fastcp parts. Confidence: high on mechanism.

### 14. Sizing cliffs and copies

- **Mempool** (8192 mbufs × ports × queues, `socket_id` hard-coded 0) is
  undersized for the zero-copy design: RX ring 1024 + up to 1024 TX in
  flight + 256 lcore cache leave ~5.9 K for retained ZC segments, while
  one outer iteration can retain 64 × polls (≈ 4 K at N=256). Exhaustion
  = RX refill fails (NIC drops → client RTO) and any TX alloc **panics
  the server** (`assert!` on null mbuf). Size ≥ RX_DESC + TX_DESC + cache
  + `MAX_CONNECTIONS` × ZC cap, allocate on `rte_eth_dev_socket_id(port)`,
  make TX alloc failure a retry-next-poll. Consider RX_DESC 4096 so a
  100–200 µs poll-thread stall does not drop.
- **Neighbor cache** is fastcp's default 8 entries (no
  `iface-neighbor-cache-count-N` feature set in `crates/core/dpdk/Cargo.toml`);
  eviction of oldest on full, a missing neighbor silences the socket for
  `DISCOVERY_SILENT_TIME`, MAC-learning reseed throttled to 30 s — a
  multi-second stall cliff when clients span > 8 hosts. Enable
  `iface-neighbor-cache-count-64`. Trivial.
- **`TxFrame`** is a fixed 528 B (`8+2+516`) struct copied whole out of
  the SPSC per frame (`try_consume` returns `T` by value; 9 cache lines
  on the poll thread, which is also ingress), two frames per request
  (Report + `BatchEnd`, two `queue_send` calls), then `TxQueue::push`,
  `send_slice`, mbuf — five copies of every response byte; the
  replication path copies four times (~> 1 GB/s of memcpy on the poll
  core at 3 M ev/s). Fold `BatchEnd` into the same `TxFrame` when
  `is_last_in_request` (S); a borrow-based `spsc::Consumer::peek` copying
  only `len` bytes (M); `queue_send` fast path straight to `send_slice`
  when the `TxQueue` is empty and `can_send()` (S). Per-slot `flush()`
  is July #8 (deliberate).
- **Replication egress bursts** stall the poll thread ~100–200 µs (up to
  512 KiB queued per tick, one `send_slice` memcpy, ~350 mbuf builds in
  one `iface.poll`) — a bimodal DPDK tail shape. Cap bytes per tick
  (64–128 KiB) with a raised `dispatch_burst_limit` so the cap costs no
  extra passes; the structural fix is the roadmap's "split replication
  off the DPDK poll thread".
- **Clock reads** scale with connection count: `Instant::now()` per
  active connection per iteration, per non-empty RX batch, and per
  replication tick per slot (~1.75 N + 3 vDSO reads per iteration).
  Stamp `last_activity` from a per-iteration cached `Instant`; gate the
  MAC-learning reseed and repl `last_send` checks on poll count.
- Small: `dpdk_response.rs` still uses std `HashMap` with a double lookup
  per slot; 5–6 non-inline FFI calls per RX mbuf plus per-mbuf free (a
  `(ptr,len)` accessor and `rte_pktmbuf_free_bulk` save ~10–20 ns/pkt);
  `SLOW_CHECK_INTERVAL` / `TICK_CHECK_INTERVAL` are iteration-based, so at
  100 µs iterations timeouts drift to 100 s / 400 ms — clock-gate them via
  the cached time; `SocketSet` is growable but `tx_queues` /
  `tx_queue_limits` / `connections` are fixed at `MAX_CONNECTIONS` and
  indexed by `handle.index()` (with 2 listeners + 2 replica sockets,
  1021+ clients index out of bounds); RSS config never sets
  `mq_mode = RTE_ETH_MQ_RX_RSS` (moot with `num_queues = 1`, but likely
  why RSS "was unworkable"); `set_initial_congestion_window` is a no-op
  under fastcp's `NoControl` (comment misleads).
- ENA/AWS operational checks (not verifiable from code): confirm the PMD
  log does not report LLQ falling back to host mode (needs WC BAR
  mapping; historically unavailable under vfio-pci → lower TX
  throughput); confirm EAL control threads (`eal-intr-thread`,
  `rte_mp_handle`, telemetry — pass `--no-telemetry`) do not share the
  reader core; confirm the clocksource is vDSO-capable (`tsc` /
  `kvm-clock`).

---

## What the audit confirmed is right

For the next auditor — load-bearing things already correct; don't
regress them:

- **Sender publishes before the disk write.** Wire-ready `InputBatch`
  frames go to the replication ring at `submit_batch` time, before the
  disk submit; the sender is a pass-through (memcpy chunk → one io_uring
  SEND, no re-encode) with no fill delay: it takes what is in the ring
  and sends immediately, one SEND in flight, CQE reaped in the same
  iteration.
- **No wake-up hop anywhere on the ack/gate path.** The primary sender
  spins on the mmap'd CQ; the response gate spins on atomics (per slot,
  with a pre-wait flush); the replica sends the in-memory ack as soon as
  it publishes, one ack in flight with send-latest coalescing — correct
  for cumulative cursors and adds no round-trip.
- **Dual-track ack semantics are conservative end-to-end**:
  `last_target = index + 1`, `pop_ready` gates on the disk-thread cursor
  that only advances on the fsync CQE, `record_ack` rejects impossible
  cursors.
- **The durability policy is "fastest replica"**: Hybrid =
  `min(journal, max(r1, r2))`, DurablyReplicated = second-largest
  persisted; no min-of-replicas anywhere on the gate; `effective_cursor`
  makes the non-atomic pair read safe.
- **Ingress**: single-producer ring (no CAS/lock), multishot RECV +
  ring-mapped provided buffers, `SINGLE_ISSUER`, slab index as
  `user_data`, pre-sized buffers, one wall-clock read per drain,
  `TCP_NODELAY`, reader pinned, ServerBusy off the reader thread.
- **Egress**: `MSG_DONTWAIT` sends (no HOL on a slow peer), paced retry
  only for blocked peers, one `io_uring_enter` per flush, no locks or
  allocations per response, heartbeat/policy timers only on the idle
  path, `MAX_MATCHING_BATCH = 16` bounds matching→response burst delay.
- **DPDK**: zero-copy RX with mbuf refcounting and a single copy into
  `parse_buf`; batched ingress with no per-poll allocation; ACKs
  coalesced per socket per poll; TX batched into one `tx_burst` per phase
  with unsent mbufs retained; HW checksum + VLAN offload; Nagle and
  delayed ACK off, 1 ms RTO floor; poll thread is the EAL main lcore (so
  the per-lcore mempool cache is in effect); pure busy-spin, no
  `poll_delay` sleeps on the primary loop.
- **Rejected without further work**: SQPOLL on the reader (adds a kernel
  thread for a ring whose SQ traffic is only re-arms), IOPOLL (storage
  only), zero-copy RX / RECV bundles / `RECVSEND_FIXED_BUF` (kernel ≥
  6.10, prod is 6.8), completion-driven client sends (see io_uring audit
  #1).

## Stale comments to fix regardless

- `crates/core/server-runtime/src/server.rs` — client accept path:
  "reader threads spin on io_uring CQEs" / `SO_BUSY_POLL` rationale
  (the reader blocks in `submit_and_wait`; the option is inert, item 4).
- `crates/core/server-runtime/src/replication/tcp_sender.rs` —
  `set_busy_poll` comment claims the thread "spins on recv" (same).
- `crates/core/transport-core/src/pipeline.rs` — input ring comment says
  "~72 bytes per slot … fits in L3"; `InputSlot` is `repr(align(64))` =
  128 B, 1 M slots = 128 MiB.
- `crates/core/dpdk/src/dpdk/transport.rs` — `tune_socket`'s
  `set_initial_congestion_window` comment (no-op under `NoControl`).
