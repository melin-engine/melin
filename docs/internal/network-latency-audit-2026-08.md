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

Items landed are removed from this document as they ship, and items
struck as inapplicable are reduced to a note saying why; the numbering
below is the original audit's, so the gaps are resolved work, not
omissions. The pass notes say what each round did.

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
| 12 (residue) | DPDK: small segments can still exhaust the zero-copy descriptor array | sporadic ≥1 RTT stalls | Med | M | Med (DPDK) |
| 14 (residue) | DPDK: per-mbuf FFI calls, `spsc` copy-out, iteration-based timers | ~10–30 ns/pkt, timer drift | Low | S–M | Low–Med (DPDK) |

Struck as not applicable to the deployment profile — see "Third pass":
10 (shard egress), 13. Item 11 landed, but for the leak rather than the
capacity ceiling the entry led with.

### Suggested order

Kernel-TCP: instrument and decide 4 / 5 / 7 on numbers; 6 only after a
microbenchmark justifies a second attempt. DPDK: measure what the third
pass landed before spending anything on the residue.

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

### Third pass — the DPDK items (2026-08-18)

**Read this before using any rating in this document.** The DPDK items
were rated per-mechanism, and that rating quietly assumed a scaling
regime Melin does not target. Melin runs a handful of sockets: a few
trading clients plus one or two replication sockets. Re-rated against
that profile the items split along an axis the original audit did not
draw:

- **Per-connection mechanisms** — poll cost O(sockets), egress passes
  over every socket, per-connection clock reads, index capacity, mempool
  retention sized for hundreds of retained segments. Worth approximately
  nothing at eight sockets.
- **Per-socket burst depth** — everything driven by how much data hits
  *one* socket between drains. Unaffected by connection count, and the
  replication socket has by far the deepest bursts in the system: up to
  512 KiB handed to a single `queue_send`.

Only the second kind survives. That also strikes item 10 on the
kernel-TCP side, which is explicitly gated on ">32 dirty connections".
Generalise the lesson: before costing an item, state which axis it
scales on and check that axis against the deployment, not against the
largest number the code permits.

**Landed.** Item 11 (index eviction on close, plus the fastcp API it
needed); the burst-depth half of 13 (replication sockets get an egress
burst limit covering a whole in-flight window; the trading port keeps
the fan-in default); and from 14 — the replication per-tick byte cap,
`BatchEnd` folded into the response's TX frame, both mbuf-exhaustion
panics removed, the FxHash connection table and its double lookup, the
mempool on the NIC's NUMA node, and the neighbour cache widened.

Item 12's mechanism was addressed in fastcp rather than melin: the
descriptor cap is a first-class config knob defaulting to a value that
covers several full RX bursts to one socket, and the advertised window
is now bounded by free descriptor slots so a bulk peer is flow-
controlled instead of silently overrunning the array.

**Right item, wrong reason.** Item 11's *capacity* was never the
constraint — 64 concurrent is far above the target. What bit was the
*leak*: entries were evicted only on a stale hit, so the cost scaled
with connection *lifetimes*, not concurrency, and degraded monotonically
with uptime. It landed for that, not for the per-packet number the entry
led with, and the capacity ceiling was left alone.

**Found while implementing, worth knowing.** fastcp's zero-copy ingest
checked descriptor capacity *after* adding the segment to the
reassembler, so an overflow drop left the reassembler holding bytes no
descriptor backed — once the preceding gap filled, the stream advanced
over data that was never stored and every later segment landed at the
wrong offset. Silent corruption, not a clean drop. Anything touching
that path should keep the capacity check ahead of every state mutation.

**Caveat, stronger here than for the earlier passes.** None of this is
exercised without a NIC. It is compile-verified and unit-tested where
the logic is transport-independent, and that is all. Price it on
hardware before believing any of it — and the descriptor cap is the one
to watch first, since it remains the most plausible explanation for the
DPDK-under-kernel-TCP throughput result in the AWS bench notes.

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

### 10 (struck). Sharding `flush_sends` across response consumers

Cut in the third pass. The mechanism is real — N dirty connections put N
inline sends on the response thread, and it grows linearly with
connection count — but it is a per-connection cost, and the entry's own
advice was to measure it only with a >32-connection profile. Melin does
not run one.

One cheap piece survives independent of connection count: client fds are
unregistered `types::Fd` on the response ring, costing an fget/fput per
SEND. A sparse fixed table updated on connect and teardown removes it,
~50–100 ns per SEND at any N.

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

`melin-dpdk` tracks a `fastcp` branch while the third pass's fixes are
unpublished; fastcp-side work still needs a publish + bump before melin
can be released. One poll thread (the EAL main lcore) does RX, TCP,
decode, ring publish, TX and both replication slots; the outer loop
visits every connection every iteration and calls `transport.poll()` at
the top, every 4 connections, and at the end, plus `repl_driver.tick()`
at the same cadence (which polls again whenever it queued data).

### 12 (residue). Small segments can still exhaust the descriptor array

The bulk half is fixed: the advertised window is now bounded by free
zero-copy descriptor slots, so a peer sending MSS-sized segments is
flow-controlled rather than overrunning the array and having the excess
dropped without an ACK. The cap itself is a config knob with a default
sized for several full RX bursts to one socket.

What no window can fix: TCP windows are denominated in bytes, so nothing
in the window bounds a segment *count*. A flow of small segments — which
is exactly what order entry looks like with Nagle off — consumes one
descriptor per packet while the byte window still reports plenty free.
Sizing the array is the only lever, and it is a bound, not a guarantee.

**Fix.** Spill to the copy-path `rx_buffer` when the descriptor array is
full instead of dropping. Non-trivial: the zero-copy path tracks its own
contiguous-byte count as a virtual `rx_buffer.len()`, so a mixed socket
needs the two to interleave in stream order on both the receive and the
window-accounting sides. Worth doing only if the cap is observed to bind
in practice; the counter to watch is whether the drop path logs at all.

Gain: removes a sporadic ≥1 RTT stall under small-segment bursts. Risk:
medium (stream ordering). Effort: M (fastcp). Confidence: high on
mechanism, unknown on frequency.

### 13 (struck). `poll()` is O(all sockets) and runs O(N) times per iteration

Cut in the third pass — the O(N) modelling starts at N=64 and Melin runs
an order of magnitude below that. The dirty-TX handle list, the
needs-egress set, the adaptive mid-iteration polls and the
touched-handle drain are all per-connection work with nothing to save at
this scale.

The burst-depth half of the item landed instead: the egress pass repeats
over every socket until one sends nothing, and the per-socket burst
limit defaulted to 4, so a replication window cost a pass per few
segments regardless of connection count. Replication sockets now carry a
limit sized to a whole in-flight window.

### 14 (residue). Copies and drift

Landed from this item: the mempool now allocates on the NIC's own NUMA
node; neither mbuf allocation site can panic the server (`Device`'s
token allocation reports the shortage and the stack retries); the
neighbour cache is widened past its 8-entry stall cliff; `BatchEnd`
rides in the same `TxFrame` as the response it terminates, with the
terminator pre-encoded once; and `dpdk_response.rs` uses an FxHash
connection table with a single lookup per slot.

Struck as per-connection: the mempool *resize* (its arithmetic was
driven by hundreds of retained segments at N=256); the per-connection
clock reads (~1.75 N + 3 vDSO reads per iteration, which at eight
sockets is noise); the `tx_queues` / `tx_queue_limits` / `connections`
out-of-bounds past 1021 clients; and the RSS `mq_mode` gap, moot at
`num_queues = 1`.

Still open, all independent of connection count:

- **Copies per response byte.** Folding the terminator removed one frame
  per request, but each frame is still copied by value out of the SPSC
  (`try_consume` returns `T`) on the poll thread, which is also ingress,
  then again through `TxQueue::push`, `send_slice` and into the mbuf. A
  borrow-based `spsc::Consumer::peek` copying only `len` bytes (M) and a
  `queue_send` fast path straight to `send_slice` when the `TxQueue` is
  empty and `can_send()` (S). Per-slot `flush()` is July #8 (deliberate).
- **Per-mbuf FFI**: 5–6 non-inline calls per RX mbuf plus a per-mbuf
  free; a `(ptr,len)` accessor and `rte_pktmbuf_free_bulk` save
  ~10–20 ns/pkt (S).
- **Iteration-based timers**: `SLOW_CHECK_INTERVAL` /
  `TICK_CHECK_INTERVAL` count iterations, so their real period moves
  with iteration length — clock-gate them off the cached time (S).
- **RX descriptors**: consider RX_DESC 4096 so a poll-thread stall does
  not drop. The per-tick replication cap shortens the worst stall, which
  reduces but does not remove the exposure.
- **Structural**: splitting replication off the DPDK poll thread (on the
  roadmap) subsumes the per-tick cap and the stall it bounds.
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
