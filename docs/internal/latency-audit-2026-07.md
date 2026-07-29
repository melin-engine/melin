# Latency audit — July 2026

Structural read of the hot paths (disruptor ring / SPSC, ingest, matching,
journal, response gate, replication metrics) looking for costs that are
avoidable rather than inherent.

**Nothing here has been measured.** These are code-level reads: each item
identifies a mechanism (a cache line that bounces, a copy that has no
consumer, a syscall on the wrong side of a branch), not a quantified
regression. Before acting on any of them, instrument first — the
`tick-to-trade` feature gate gives the per-stage decomposition (journal-wait
/ replica-wait / encode / egress, registered in
`crates/core/transport-core/src/trace.rs`), and
`crates/core/pipeline/examples/false_sharing.rs` is the ready-made
cache-line harness. Findings 1 and 4 are the ones worth measuring first.

Ordering is by expected impact, highest first.

**Read [Measured verdict](#measured-verdict) first.** The list was written
before any measurement. Bench data from 2026-07-12 since showed that none
of it is where this system's latency lives — the ratings below rank items
against each other, not against anything that matters.

---

## Triage summary

Ratings are None / Low / Med / High, and are judgement calls from code
reading — same caveat as above, nothing measured. "Regression risk" is the
potential for the fix itself to make performance *worse*, which is not
uniformly low and is the column most worth reading.

| # | Finding | Upside potential | Regression risk | Bench effort | Fix effort |
| --- | --- | --- | --- | --- | --- |
| 1 | `ReplicationMetrics` unpadded | **High** | Low | **Low** | Low–Med |
| 2 | Hoist `connected_persisted_min` | Low–Med | None | High\* | ~~Trivial~~ **done** |
| 3 | SipHash → FxHash (3 sites) | Med | Low | **Low** | **Trivial** |
| 4 | No flush while ring has work | **High** | **Med–High** | Low–Med | **partly done** |
| 5 | Journal 512 KiB copy | Med | Med | Med | **High** |
| 6 | `CachePadded` 64 → 128 | Med | **Med** | Low–Med | Trivial to try |
| 7 | `Arc<AtomicU64>` cursors unpadded | Low–Med | Low | Med | Low |
| 8 | DPDK per-slot `flush()` | Med | Med | Med | Low |
| 9 | Ingest double copy | Med | Low–Med | Med | Med |
| 10 | `spsc::flush` contended load | Low | None | Low | ~~Trivial~~ **done** |
| 11 | `ring::Producer` cursor loads | Low | Low | Low | Low–Med |

\* Finding 2 cannot be isolated from finding 1 — they touch the same cache
line. Fold it into finding 1's change and measure the pair once.

### Ratings that need a why

**4 is the only item that can plausibly make a benchmark look worse.**
Flushing more often means more `submit_and_wait` calls and fewer bytes per
send — a direct trade of throughput for tail latency. Expect the LAN
suite's throughput figure to dip while `server e2e` improves. That is the
change working as intended, but decide which number is being optimised
before picking the threshold. The gate-driven half that shipped has the
same exposure and has not been measured; see item 4's section for what it
does and does not cover.

**6 could genuinely net negative, which is why it stays an experiment.**
Editing `padding.rs` doubles padding at *every* `CachePadded` site — the
disruptor cursor, each consumer's `Arc<Sequence>`, both SPSC counters.
More footprint means more L1d and TLB pressure, which on a small-L3 part
could outweigh the prefetcher win. Genuinely two-sided.

**5's risk is the batch shape, not the copy.** `peek_batch` returns
contiguous slices, so a span crossing the ring's wrap point splits. If the
journal stage encodes per-span, write batches shrink near wraps, eating
into the NVMe write amortisation that `MAX_JOURNAL_BATCH` exists to get.
Combined with restructuring borrows around `sync_point` / `mark_split` in
correctness-critical code, this is the one where the fix cost may exceed
the win.

**1's low bench effort pairs with a measurement caveat.** The microbench is
the only place the effect will be visible; end-to-end it is likely masked
by the replica-side `recvmsg` + `fsync` ceiling. Budget for a null result
on the LAN suite that does *not* mean the fix is worthless.

### Suggested order

3 and 2 are near-free in both effort columns — worth doing regardless of
measurement. 1 has the best effort-to-information ratio on the list. 6 is
one line to try, so try it, but be willing to discard it. 4 is the
highest-value fix and needs a throughput-vs-latency decision rather than a
number. 5 is the only one where measuring first should be treated as
mandatory.

---

## 1. `ReplicationMetrics` is unpadded, and the durability gate spins on it

**Where:** `crates/core/transport-core/src/replication/metrics.rs:18`

The struct is ~104 bytes of bare atomics with no padding, so
`acked_sequence[2]`, `in_memory_sequence[2]`, `bytes_sent[2]` and
`ack_latency_us[2]` all fall within roughly one cache line.

Who touches that line:

| Thread | Access | Frequency |
| --- | --- | --- |
| Replication sender | `bytes_sent[slot].fetch_add(...)` — an RMW, takes the line **exclusive** (`server-runtime/src/replication/tcp_sender.rs:936`, `replication/dpdk.rs:953,977`) | every completed SEND |
| Replication sender | stores to `acked_sequence[slot]`, `in_memory_sequence[slot]`, `acks_received[slot]` (`transport-core/src/replication/cursors.rs:204`) | every ack |
| Response stage | Acquire-loads `acked_sequence[0..2]` and `in_memory_sequence[0..2]` (`server-runtime/src/response.rs:591`) | **every iteration of the gate spin loop** |

So the tightest wait loop in the system re-reads a line that a different
thread invalidates at the replication send rate. On a multi-CCD part that
is a cross-CCD miss per spin iteration where it should be an L1 hit. With
two replicas the two senders also false-share each other's slots.

The rest of the codebase is careful about exactly this — `CachePadded`,
`#[repr(align(64))]` on `InputSlot` — this struct just missed it.

**Proposed fix.** Split read-hot from write-hot and pad per slot:

- A `#[repr(align(64))]` per-slot struct holding the two cursors the gate
  reads (`acked_sequence`, `in_memory_sequence`), one cache line per
  replica slot.
- Move the pure telemetry counters (`bytes_sent`, `ack_latency_us`,
  `acks_received`) off the gate's line entirely — nothing on the hot path
  reads them, only the health endpoint does.

This changes `ReplicationMetrics`'s field layout, which the health
endpoint and its tests read directly; the accessor surface should absorb
that rather than leaking the padding into callers.

---

## 2. The gate spin loop recomputes `connected_persisted_min` for nothing

**Where:** `crates/core/server-runtime/src/response.rs:594`

`repl_min` has exactly two consumers: the `#[cfg(feature =
"tick-to-trade")]` cross-tracker, and the attribution branch at
`response.rs:630` that runs **once**, on the iteration where the gate
opens. On a build without `tick-to-trade` it is otherwise dead.

It is nonetheless computed on every spin iteration — four Acquire loads,
on the line from finding 1.

**Fixed.** Each consumer now samples `connected_persisted_min` where it is
actually used: the attribution branch computes it inside the
`cached_durable_pos >= needed` arm (once per gate open), and the traced
build computes it inline at the `gate_tracker.observe` call, which
genuinely needs a per-iteration value. The spin body no longer touches it
at all on a build without `tick-to-trade`.

Both response stages had the identical loop — `response.rs` and
`dpdk_response.rs:398` — so both were changed.

Behaviour is preserved. The attribution counters read the same
`journal_pos` from the same iteration, so the comparison stays a
like-for-like snapshot; with no replication configured
`connected_persisted_min` returns `u64::MAX` and the branch still
attributes to the journal, as before.

Still worth folding into finding 1's measurement rather than measuring on
its own — the loads it removes are on finding 1's cache line.

---

## 3. SipHash on the kernel-transport response path

**Where:** `crates/core/server-runtime/src/response.rs:176,268`

```rust
let mut connections: HashMap<u64, ConnectionEntry> = HashMap::with_capacity(256);
let mut dirty_connections: HashSet<u64> = HashSet::new();
```

Both use the std default hasher. Per response slot that is a
`connections.get_mut(&connection_id)` plus a
`dirty_connections.insert(connection_id)` **per frame** — and each request
emits two frames (payload + `BatchEnd`). Three SipHash-1-3 hashes of a
`u64` per request, on the egress hot path, for keys that are
internally-generated connection IDs with no HashDoS surface.

The DPDK transport already made this call and left the reasoning inline
(`server-runtime/src/dpdk_transport.rs:162`):

> FxHash instead of SipHash — u64 keys, no HashDoS surface internally.

Three sites never got the same treatment:

- `server-runtime/src/response.rs:176,268` — per response slot / per frame
- `server-runtime/src/dpdk_response.rs:130` — per response slot
- `server-runtime/src/reader.rs:331` (`fd_to_slab`) — per CQE

**Proposed fix.** Switch all three to `FxHashMap` / `FxHashSet`.
`rustc-hash` is already a `server-runtime` dependency, so this is
mechanical. While there, give `dirty_connections` a `with_capacity` — it
currently reallocates during warmup.

---

## 4. Response data never flushes while the output ring has work

**Where:** `crates/core/server-runtime/src/response.rs:447`

`flush_sends` was reachable from exactly three places: the idle path
(`count == 0`), the heartbeat scan, and shutdown. There was **no flush on
the path where slots were consumed**.

Under sustained load — or, more sharply, immediately after a durability
gate stall, during which the matching stage has been filling the output
ring the whole time — the stage runs iteration after iteration with
`count > 0`, appending into each connection's `send_buf` and never
flushing. Responses sit in userspace until traffic happens to pause.

The degenerate case is not just latency. `append_frame`
(`response.rs:1040`) drops the connection outright once `send_buf` would
exceed `MAX_SEND_BUF` (64 KiB), so a client that keeps the pipeline busy
enough gets disconnected rather than served.

**Proposed fix.** Add a flush trigger on the consumed path. Either a byte
threshold per connection (flush once a `send_buf` passes roughly one MSS)
or a slot-count trigger, whichever profiles better. The point is to bound
the interval between "matched" and "on the wire" by something other than
"the next lull".

**Partly fixed** — a fourth `flush_sends` site now fires immediately
before the stage blocks on a durability wait, so a response that is
already durable no longer sits in `send_buf` for the length of an
unrelated event's fsync and replica round-trip. That trigger was chosen
over a byte threshold because it needs no tuning constant and cannot cost
latency: it only fires when the stage was about to spin anyway. The same
change made the gate per-slot rather than per-batch, which is what makes
the trigger fire at durability boundaries instead of once per batch.

**Still open, and it is the half with teeth.** The new trigger is
gate-driven, so it does nothing in the regime where the gate never
closes — `local` mode, or any deployment whose durability frontier stays
ahead of the response stage. There, a saturated output ring still means
no wait, no lull, and no flush, and the 64 KiB disconnect above is
reached exactly as before. Closing that needs the size- or count-based
trigger this section originally proposed. Note that a byte threshold is
the only one of the two that bounds the disconnect directly.

**Unmeasured.** The throughput-vs-latency trade flagged in "Ratings that
need a why" has not been run on the LAN suite, and should be before this
is treated as settled. Pick the run deliberately: this audit records a max
output-ring depth of 1 in the latency run, so the stage reaches its idle
path and flushes constantly there. Extra `submit_and_wait` calls will show
up in a saturating throughput run or not at all.

**Adjacent, same area.** `flush_sends` submits with
`submit_and_wait(pending)` and `retry_send` (`response.rs:1136`) loops
synchronously on `submit_and_wait(1)`. A single client with a full TCP
receive window therefore head-of-line-blocks the response stage for every
other connection. Worth separating from the flush-trigger change, but it
belongs on the same list — and its priority went up with the partial fix
above, which moved a `flush_sends` call onto the gate path. That exposure
used to be confined to lulls and shutdown; it now sits in a path that runs
under load.

---

## 5. The journal stage copies every event into a 512 KiB stack buffer

**Where:** `crates/core/transport-core/src/pipeline.rs:716` (sync) and
`pipeline.rs:1918` (io_uring)

```rust
let mut batch = [InputSlot::default(); MAX_JOURNAL_BATCH];  // 4096 x 128 B
```

`read_batch` then memcpys each ready slot out of the ring into it: a
128-byte copy per event, into a working set far larger than L2, evicting
the writer's own encode buffers on the way through.

The matching stage already solved this. `Consumer::peek_batch`
(`pipeline/src/ring.rs:601`) borrows ready slots in place as up to two
contiguous slices, and its own doc comment gives the motivation:

> Use this instead of `consume_batch` / `read_batch` when the caller would
> otherwise copy the batch into a stack array just to iterate it — the
> matching stage does this on every disruptor batch and the copy is pure
> overhead.

The journal stage does the identical thing and was never converted.

**Proposed fix.** Move the journal stage to `peek_batch` +
`commit_consumed`. This is the fiddliest item on the list: the encode loop
interleaves `self.sync_point(...)`, `apply_stream_marks`, and
`mark_split` calls with iteration, and `peek_batch` holds a borrow on
`self.consumer` across the loop body. It likely needs the mark-split span
loop restructured so the borrow ends before each barrier. Worth doing, but
not a mechanical change — and correctness here is load-bearing (the
deferred-commit-until-fsync contract).

**Same shape, cheaper fix:** `response.rs:178` copies up to 1024
`OutputSlot`s out of the SPSC per iteration, because `spsc::Consumer` has
no `peek_batch` counterpart to the disruptor's. Adding one to
`pipeline/src/spsc.rs` is straightforward and removes the copy from both
response stages.

---

## 6. `CachePadded` is 64 bytes; on Zen the interference unit is 128

**Where:** `crates/core/pipeline/src/padding.rs:16`

`#[repr(align(64))]` puts two adjacent `CachePadded` fields exactly 64
bytes apart — the same 128-byte sector. AMD's adjacent-line and L2 spatial
prefetchers pull the pair together, reintroducing the interference the
padding exists to prevent. This is why `crossbeam-utils` uses 128 bytes on
x86-64.

The clean instance is `spsc::Shared` (`pipeline/src/spsc.rs:29`):

```rust
head: CachePadded<AtomicU64>,   // producer writes
tail: CachePadded<AtomicU64>,   // consumer writes
```

Adjacent fields, 64 bytes apart, opposite writers.

**Proposed fix.** Try `align(128)` and measure — this is the one finding
with a ready-made harness. `crates/core/pipeline/examples/false_sharing.rs`
already interleaves samples to control for thermal drift; extend it to
compare 64- vs 128-byte `CachePadded` on the SPSC head/tail pair. Given
the target hardware is EPYC, this is a cheap experiment with a plausible
payoff, but it is strictly an experiment — the doubled padding costs
footprint, so it should not land on reasoning alone.

---

## Lower priority

Numbered to match the triage table above.

- **7 — `Arc<AtomicU64>` cursors are unpadded.** `DurableWireSeqCursor`
  (`transport-core/src/cursors.rs:123`) and `replica_quorum_cursor`
  (`cursors.rs:175`) are plain `Arc<AtomicU64>`, allocated back-to-back in
  `PipelineCursors::new`. Two 24-byte allocations from the same size class
  will very likely share a line — and they have different writer threads
  (journal stage vs. replication sender) with the response stage reading
  both. Same class as finding 1, at a much lower write rate.

- **8 — Per-slot flush on the DPDK response path.**
  `server-runtime/src/dpdk_response.rs:542` calls
  `tx_producers[tid].flush()` inside the per-slot loop — one release store
  per slot. The matching stage goes to real trouble to amortise its output
  cursor store over a whole batch (`pipeline.rs:2569`, `out_batch`); this
  gives that back one slot at a time. Hoisting the flush to the end of the
  batch trades a small visibility delay for the amortisation.

- **9 — Double copy on ingest.** `reader.rs:605` unconditionally
  `extend_from_slice`s the recv payload from the io_uring provided-buffer
  pool into the connection's `parse_buf`, then `process_client_frames`
  compacts it with a `copy_within` (`client_frames.rs:155`). In the common
  case — `parse_buf` empty, recv contains whole frames — both copies are
  avoidable by parsing directly out of the pool buffer and only spilling a
  trailing partial frame into `parse_buf`.

- **10 — `SpscProducer::flush` loads a contended line to decide whether
  to store.** **Fixed.** `flush` compared `local_head` against a Relaxed
  load of `shared.head` — a line the consumer reads continuously, putting
  a shared-line load in the dependency chain ahead of the Release store on
  every call. `Producer` now carries a `committed_head` mirror, which is
  exact rather than a hint: `flush` is the only writer of `shared.head`,
  so nothing can move it underneath the producer. A no-op flush now
  touches the shared line not at all. This matters more than the rating
  suggests while item 8 stands, since the DPDK response path calls `flush`
  once per slot.

- **11 — `ring::Producer` reloads its own cursor.** `try_publish`
  (`pipeline/src/ring.rs:154`), `publish_with` (`:207`), `try_claim`
  (`:240`) and `batch()` (`:309`) each Relaxed-load `shared.cursor` to
  find the next sequence, rather than mirroring it locally the way
  `spsc::Producer` now mirrors `head` (item 10). The producer is the sole
  writer, so a mirror would be exact; the consumers read that line
  continuously through `DependencyKind::load`.

  **Scope is narrower than it first looks, and the rating reflects that.**
  Every hot-path caller already avoids the per-event load: `Batch`
  computes each sequence as `start_seq + count` from local state, and
  `Batch::commit` stores without loading. Ingest
  (`server-runtime/src/client_frames.rs:75,146`), the replica receiver
  (`replication/receiver_transport.rs:281,459`) and the matching stage's
  output (`pipeline.rs:2569`) all go through `Batch`. The residual is one
  load per `batch()` construction — on ingest that is once per recv plus
  once per `COMMIT_EVERY` (16) frames — plus one `try_claim` per fsync
  batch on the replication ring (`journal/src/replication.rs:116`;
  `record_slot_for_replication` only accumulates into a buffer per slot,
  it does not publish). The remaining `publish` / `try_publish` callers
  are cold: startup seeding, shutdown sentinels, and the 250 ms tick.

  **Why the risk is Low rather than None**, unlike item 10. `Batch` rolls
  back on drop by *not* storing, so a producer-side mirror would have to
  be updated at three separate commit sites inside `Batch` (`commit`, and
  the mid-spin auto-commits in `push_with` and `push_with_or_abort`) while
  staying untouched on the rollback path. That is a real invariant where
  today there is none — worth pinning with tests if it is ever done.

---

## Measured verdict

Added after the fact, against the LAN bench suite of 2026-07-12
(`exchange-core/bench-results/lan-bench-suite-20260712-*`): four runs,
TCP with dual replication, one latency profile and three throughput
profiles. This section supersedes the expectations set above.

**The short version: everything on this list together is worth well under
1% of client-observed latency. None of it should be scheduled as
performance work.**

### What the runs show

The latency profile (`tcp-dual-repl-single`) is the one that matters —
one request outstanding, so no queueing and no Little's Law distortion:

| | |
| --- | --- |
| Roundtrip p50 | 48.64 µs (min 44.86, p99 67.45) |
| Throughput | 20,133 ops/s at concurrency 1.0 |
| Replica ack round-trip | median 24 µs — roughly half the total |
| Ack quantum | 1.1 sequences per ack (no coalescing delay) |

The throughput profiles run ~1000 requests in flight, where
`throughput x p50` lands exactly on the offered concurrency. Their
"latency" is `N / throughput`, a derived quantity — it moves only if
throughput moves.

Nothing on the primary is the constraint. Real utilisation, computed from
the stage histograms rather than the busy/idle counters:

- matching: 142.5 M events x 160 ns p50 execute = **~38% of one core**
- journal: 880 k batches x 11.3 µs p50 = **~17%**
- input ring depth: median 120 of 1,048,576

Matching would not saturate until roughly 5 M ops/s, which is already past
the replica-bound ceiling. The primary has more headroom than the cluster
can consume, so the throughput-headroom argument for these items is weak
too.

### Why the items are noise against that

Costed generously against a 48,640 ns roundtrip: finding 3 ~60 ns,
finding 9 ~20–40 ns, finding 5 ~15 ns, and the cache-line items (1, 6, 7,
10, 11) are contention effects with little concurrent traffic to contend
with at concurrency 1. Finding 4 does not fire at all — queue depth median
0, max 1 in the latency run, so the response stage reaches its idle path
and flushes constantly.

The cheap ones remain worth taking as code hygiene. Findings 2 and 10 are
done; finding 3 is a mechanical type swap. Nothing here justifies the fix
cost of finding 5.

### Where the latency actually is

1. **The replica ack round-trip — ~24 µs of 48.64 µs.** The primary
   persists locally faster than the network turnaround, so the gate is
   waiting to *learn* the replica's position, not waiting on remote disk.
2. **The remaining ~24 µs floor** — two network hops, wire encode/decode,
   primary fsync, egress. Unmeasured.

### Two caveats for whoever picks this up

**Do not assume the gate waits on replica fsync.** The default `Hybrid`
policy is `persisted>=1 && in_memory>=2`: the primary supplies the
persisted copy, the replica only needs the event in RAM. Measured replica
`in_memory - persisted` is median 0 in all four runs anyway, so there is
no persisted-vs-in-memory gap to reclaim.

**The gate attribution counters were soft evidence, not hard — since
fixed.** `connected_persisted_min` read `acked_sequence` (replica
*persisted*) regardless of the policy in force, and took the *minimum*
across replicas regardless of how many the policy required. Under
`local` (`persisted>=1`) that reported replication as the blocker in a
mode where replicas cannot bind the gate at all; under `hybrid` it read
the wrong level (those cursors sat ~474 vs ~96 events behind the primary
in the throughput runs), and one slow-but-connected replica could drag
the verdict to replication while the gate was being satisfied by the
other one.

Attribution now goes through `Policy::attribute_blocker`, which finds
the binding clause and reports whether the node at that clause's
threshold rank was the primary or a replica — correct for any policy
shape. Counters recorded before that change are biased toward
replication and should be treated with suspicion.

The concurrency-1 figure quoted above survives, though. In that run the
replica's in-memory and persisted cursors coincide and both sit one
event behind the primary, so the old comparison and the policy-correct
one return the same verdict. The throughput-run percentages are the
ones to discard.

One reading note that survives the fix: under `durably-replicated` a
replication verdict is the *expected* steady state, not a finding. The
primary persists before it ships, so the second-largest persisted cursor
is normally a replica's.

**Stage histograms are mostly missing from these runs.** Three of four
recorded none; the fourth got 4 of 8 (both journal, both matching). No
`server e2e`, no response-stage or reader stages, and no `tick-to-trade`
breakdown anywhere — see the dormant-recorder caveat on
`StatsRegistry::snapshot_all`. Until that is fixed the suite cannot answer
"where did the 48 µs go", which is the only question worth asking here.
