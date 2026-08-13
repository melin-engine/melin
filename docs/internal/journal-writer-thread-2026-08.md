# Journal writer thread — moving the write off the encode path (scope)

Status: **proposed** (2026-08). Successor to
[journal-async-flush-2026-08.md](journal-async-flush-2026-08.md), which
moved `fdatasync` off the journal thread. Not yet implemented.

## Why

The async flush left `pwrite` on the journal thread and `fdatasync` on
the executor, which means two threads issue I/O against the same inode
concurrently. Under load that produced ~8 ms stalls *inside a 156-byte
`pwrite`*, on both primary and replica, tracked to the journal thread —
and reproduced only in a concurrent setup, which rules out the competing
explanation (the preparer's staging log forces, which serialise
filesystem-wide; see `preparer.rs`).

So the async flush relocated the stall rather than removing it: the
journal thread no longer blocks in `fdatasync`, it blocks in `pwrite`
instead, and since it still feeds the replication rings and the cursors,
the stall reaches clients exactly as before.

The fix is to give **one thread sole ownership of the file** — write,
sync, and rotation — and leave the journal thread doing only what must
be deterministic and single-threaded: sequence allocation, encoding,
hashing, and publishing to the replication rings.

## What this deletes

The payoff is not only the stall. The async-flush design's hardest
machinery exists purely because two threads share one file, and all of
it goes away:

- the **rotation drain protocol** — quiesce the executor before every
  segment swap;
- the **`RawFd` travelling through a `Copy` seqlock cell**, whose
  validity rests entirely on that drain;
- the **"every rotation site must drain"** invariant, its four call
  sites and the `debug_assert` at the swap that guards it;
- the **mid-batch mark barrier** on replicas (`sync_point` at
  `read_start + stop`, then `apply_stream_marks(true)`), which exists
  because rotation must be synchronous with the writer — it becomes an
  ordered message in the queue instead;
- the `Publication::Inline` / `Publication::Executor` split, since
  publication has a single owner again.

That is most of the review surface of the previous branch, and the
source of three of its confirmed defects.

## Shape

```
journal thread                          writer thread
──────────────                          ─────────────
read_batch                              dequeue command
  allocate sequence                       Write(buf)  → pwrite
  encode + hash                           Rotate(..)  → archive + swap
  publish InputBatch to repl rings      fdatasync (covers all drained)
  enqueue Write(buf)                    publish cursors
```

**The journal thread never touches the file.** The writer thread never
touches sequence allocation, the hash chain, or the replication rings.

### The queue carries commands, not just bytes

An ordered command stream, single producer, single consumer:

| Command | Payload | Notes |
| --- | --- | --- |
| `Write` | encoded batch buffer + watermark fields | the steady-state case |
| `Rotate` | boundary + anchor hash | replica adoption and local rotation both |

Rotation being a *queue message* rather than a synchronous barrier is
what removes the drain: ordering against the entry stream is what
rotation needs, and a FIFO already provides it. A replica's
primary-announced boundary becomes "enqueue `Rotate` after the batch
ending at `boundary_seq`" — no quiesce flag, no mid-batch sync point, no
`quiesced: bool` parameter threading through `apply_stream_marks`.

### Queue depth is load-bearing

Not a tuning knob — the design's masking property depends on it.

Today a stalled `fdatasync` does not stop writes: the journal thread
keeps encoding, keeps `pwrite`-ing into the page cache, and keeps
publishing replication frames. Only the sync lags. Move the write and a
stalled sync blocks writes too, so the encoder runs ahead only as far as
it has buffers. **A double buffer would stall the encoder after one
batch**, freezing the replication feed during exactly the stall this
work exists to survive.

Sizing: absorb a sync's duration, not an arbitrary stall. At ~4 ms
typical and ~8 ms observed outliers against the measured write rate,
that is on the order of a few hundred KB — so **4–8 buffers of
`BATCH_BUF_CAPACITY` (512 KiB)**, 2–4 MiB total. Beyond that the disk is
genuinely broken and backpressure is the correct answer, not more
buffering.

Two benefits fall out of depth > 1: the writer can drain several queued
batches and cover them all with **one** `fdatasync` (it is cumulative),
and the writes themselves coalesce, cutting syscall count.

### Replication publishes from the encode path

Decided: the `InputBatch` frame goes out when the batch is **encoded**,
not when it is written.

The previous branch deliberately tied the publish to the write, on the
argument that keying it to the read batch would collapse frame sizes
(review finding 1 there). That argument was correct while the write was
inline. It stops being correct once the write moves: with the publish
tied to the write, a full queue backpressures the encoder *and* stops
the replication feed, so `in_memory>=2` freezes on local disk trouble —
the exact coupling this work removes.

Publishing at encode keeps `hybrid` masking alive even when the write
queue is full. Frame size stays governed by how much the encoder
accumulates per read batch, which the input ring's arrival rate sets;
watch it in acceptance (below) rather than assuming.

## Ownership after the move

| Concern | Owner | Notes |
| --- | --- | --- |
| sequence allocation, encode, hash chain | journal | unchanged; deterministic core |
| `InputBatch` publish to replication rings | journal | moves earlier, to encode |
| `batch_buf` | shared | ownership passes with the queue message and returns for reuse |
| `File`, `write_pos` / `valid_end` | writer | |
| `pwrite`, `fdatasync` | writer | one thread, one inode |
| rotation (local + adopted) | writer | via `Rotate` command |
| `SegmentPreparer` | writer | it stages the file the writer adopts |
| cursor publication (`CursorPublisher`) | writer | single owner; the `Publication` enum goes away |

### Things this ownership split makes awkward, and the answer

**`valid_end` drives the rotation size trigger** (`pipeline.rs:1805`),
and it now lives on the writer. Publish it as an atomic the journal
thread reads, same shape as the flush-lag gauge. The trigger is a
threshold comparison against a value that only grows — a slightly stale
read costs at most one batch of lateness.

**The replication slice points into `batch_buf`**
(`last_user_entry_replication_slice`, `buffered_writer.rs:474`) and the
buffer is about to be handed away. Since the publish moves to encode
time, the frame is assembled and published *before* the handoff, so the
slice is read while the journal thread still owns the buffer. No copy
needed — but the ordering is now load-bearing and should be asserted.

**Recovery and startup open the file.** The writer thread must own it
from the first write, so either it opens the segment, or the opened
writer is moved into it at spawn. The latter is smaller and keeps
recovery where it is.

**Thread creation.** The writer is spawned from the journal thread,
which is pinned and `SCHED_FIFO` — so it needs
`affinity::configure_spawned_thread`, from the parent, for the reason
that function documents. This is the same trap the previous branch fell
into twice.

## Acceptance

1. **The 8 ms spikes are gone.** Same workload and tracing that found
   them: no journal-thread syscall over ~1 ms, and the client-side
   2.016 s periodicity absent.
2. **No regression** against 0.13: p99.9/p99.99/max at or below, fast
   rotations still 100 %, journal verification MATCH on both replicas.
3. **Frame size did not collapse.** `InputBatch` frames/sec and mean
   events per frame at or near 0.13's, now that the publish is keyed to
   encode rather than to the write.
4. **Masking survives a full write queue.** Fault-inject a slow sync
   until the queue fills; the replication feed must keep flowing and
   `hybrid` acks must continue via the replica leg.
5. **Rotation ordering.** A replica adopts a primary-announced boundary
   at exactly `boundary_seq` with the barrier expressed as a queue
   message — covered by the existing adoption tests, which should pass
   unchanged.
6. **Coverage for what the previous branch got wrong.** Every property
   that broke there gets a test that fails without the fix: parent-side
   thread configuration, errno survival to the poison path, cursor
   advance while running (not only after shutdown).

## Sequencing (as built)

1. `BufferedWriter` learns to give up its `SegmentFile`
   (`detach`/`attach`, `take_batch`, the rotation halves).
2. The write queue and writer loop, with rotation as a queued command.
3. The pipeline wiring: `Durability::{Inline, Writer}` replaces the
   `Publication` split, and the flush executor is deleted.

Rotation did **not** stay on the journal thread behind a drain, as an
earlier draft of this plan had it: rotation needs the file, and the file
is the thing that moved. Putting it in the queue is what deletes the
drain protocol rather than preserving it.

## Resolved while building

- **Queue depth** is the constant `WRITE_QUEUE_DEPTH = 8`, not a knob —
  the sizing argument above, plus the `--group-commit-us` drift as
  precedent.
- **Rotation is synchronous from the journal thread's side.** It queues
  the command and blocks for the reply. It must know whether the
  rotation succeeded before re-anchoring its chain, and a failure has to
  leave the encoder writing to the *old* segment. An asynchronous
  rotation would allow an encoder whose chain has moved onto a segment
  that does not exist. The wait is bounded by the queue ahead of it,
  which is the same stall the inline path always paid — minus the
  separate drain.
- **A poisoned writer still answers rotations** (with an error).
  Silence would wedge a blocked journal thread rather than fail it, on
  exactly the path where operators need the error.
- **`--cores` entry eleven is renamed** `journal-flush` →
  `journal-write`. Position and default (core 11) unchanged, so only the
  documented name moves.

## Open questions

- **Does the writer thread want `SCHED_FIFO`?** It does blocking I/O
  rather than busy-spinning between batches, so probably not — unlike
  the flush executor it replaces. It currently inherits whatever
  `configure_spawned_thread` grants for its core. Decide against
  measurement.
- **Is `valid_end` lagging by the queue depth acceptable for the
  rotation size trigger?** It now reports what the writer has written,
  so it trails what the encoder has handed over by at most
  `WRITE_QUEUE_DEPTH` batches (~4 MiB against segments of hundreds of
  MiB). Believed negligible; unmeasured.

## Not yet verified

The acceptance benches still need the 9275F fleet: no-regression,
batching-did-not-collapse, fault-injected stall masking, and the
deep-rejoin drill. Nothing here has been run against real hardware — the
stall this branch exists to remove has not been observed to be gone.
