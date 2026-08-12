# Journal fsync beat — August 2026

A 10.24-second periodic ~2 ms freeze of the entire pipeline, diagnosed on
the LAN bench fleet down to a kernel stack, with a fix plan for the
buffered writer. Unlike the July latency audit, **everything here is
measured** — each claim cites the probe that produced it.

## Symptom

Under load (`tcp-dual-repl`, ~1.1–1.5 M orders/s), the client-side
latency time series shows a full-path freeze of ~1.8–2.4 ms every
10.24 s: in the affected windows p99 ≈ p99.99 — *every* in-flight order
across all client connections stalls together, then the backlog flushes.
The period is wall-clock-locked (identical at 1.06 M and 1.54 M orders/s,
window 40 and 56), fires only under load, and appears on the published
0.12.0 crates and on `fix/io-uring-audit` alike. It shapes only the
p99.99+ region (~0.02 % of wall time); throughput and p99 comparisons are
unaffected.

## Root cause

`fdatasync` on the journal segment periodically has to commit filesystem
*metadata*, not just data. The journal thread's beat-time off-CPU stacks
(perf, `sched:sched_switch` with call graphs, 4 M samples):

```
xfs_file_fsync
  → xfs_log_force_seq
    → xlog_cil_force_seq
      → xlog_cil_push_now
        → __flush_workqueue
          → wait_for_completion        ~0.6 ms, ×4 back-to-back
```

An XFS CIL (Committed Item List) log force inside `fdatasync`: queue a
workqueue item, wait for the CIL push worker to be scheduled, wait for
the log buffers to reach disk — four consecutive batches eat it, ~2.4 ms
total. The journal cursor stalls, the durability gate holds every ack,
everything downstream freezes.

The metadata being committed is **unwritten-extent conversion**. Segments
are preallocated with `posix_fallocate` (extent allocation only — see the
comment in `buffered_writer.rs`), so every append converts unwritten
extents to written, and each conversion is a logged XFS transaction.
Most `fdatasync` calls ride the data-only fast path (~30 µs,
`folio_wait_writeback`); every 10.24 s one lands while conversion
metadata is still in the unpushed CIL context and pays the force. The
exact XFS timer behind the 10.24 s cadence was never identified — it
stopped mattering once the dependency was named, because the fix removes
the dependency, not the timer.

### Evidence chain (all 2026-08-12, LAN bench fleet)

| Probe | Result |
| --- | --- |
| Client time series (1 ms windows) | Freeze clusters every 10.24 s, p99 ≈ p99.99 inside them |
| FIFO+DRAM-strided gap detectors, all 4 hosts, during bench | Silent — no whole-machine stall (no SMI, no DRAM retraining) |
| 1 kHz ICMP over the bench VLAN, during bench | Clean while 6 beats fired — network path exonerated |
| `sched_switch` ftrace, pipeline cores | Journal thread sleep bursts align beat-for-beat with client freezes (constant offset, 4/4); matching/response never block |
| `block_rq_issue/complete` on the journal NVMe | 886 k requests, p50 7 µs, **max 53 µs whole run**; during a beat the device served 556 requests ≤29 µs while the journal thread slept 2.38 ms — device exonerated |
| perf off-CPU stacks of the journal thread | The CIL-force stack above, 13/15 long sleeps, clusters spaced 10.24 s |
| `lazytime` remount on all journal mounts | Beat unchanged — timestamp updates acquitted, extent conversion convicted |

Also ruled out along the way: khugepaged/THP (off), irqbalance (absent),
dirty-writeback timers (defaults), root-disk jbd2 (idle at beats), the
melin/exchange-core codebases (no 10 s timers on the loaded path).

Fleet: 4× AMD 9950X, DDR5, dedicated journal NVMe (Micron 7450 PRO,
PLP, `write_cache=write through` — fdatasync sends no FLUSH), XFS
`rw,noatime,logbufs=8,logbsize=256k`, Debian 6.12. Note the drives are
enterprise PLP — this is not a cheap-hardware artifact.

## Fix plan (buffered writer)

**Pre-zero segments with real writes, staged in the preparer, and let the
buffered writer adopt prepared segments.**

1. **Write real zeros at prepare time.** Extend the segment preparer to
   bulk-`pwrite` zero buffers over the whole segment (then `sync_all`)
   instead of relying on allocation-only preparation. Appends into
   already-written extents generate **no** metadata transactions, so
   `fdatasync` permanently stays on the data-only fast path and never
   touches the CIL — the beat's dependency is severed regardless of what
   ticks.

   ⚠ Trap: `zero_range_extents` (`FALLOC_FL_ZERO_RANGE`) is **not**
   sufficient — it marks extents zeroed but leaves them *unwritten*, so
   conversions (and the beat) survive. Only actual data writes convert
   extents. The existing preparer sequence
   (`preallocate + zero_range + prefault + sync_all`) therefore does not
   fix this as-is.

2. **Wire the prepared-segment path into the buffered writer.** Today
   `rotate_segment_with_prepared` is sector-only and the buffered writer
   rotates via plain `rotate_segment()` on the journal thread (see the
   `set_rotation` comment in `transport-core/src/pipeline.rs`) — which is
   why `melin_journal_rotations_total{path="sync_fallback"}` is 100 % of
   buffered-mode rotations on every bench run. Adopting prepared segments
   fixes that too: rotations leave the hot path, and the `fast` counter
   finally moves.

3. **Trade-off to document:** pre-zeroing writes every segment twice
   (zeros, then data) — ~2× sequential write volume on the journal
   device, sequential and off the hot path. At bench rates (~110 MB/s
   journal) that is comfortably inside the 7450's 1.4 GB/s; endurance
   cost is the honest caveat for the ops docs. Recycling old segments
   instead is not available — rotated segments are retained for the audit
   trail.

4. **Secondary knob (ops, optional):** larger `logbsize` shortens any
   residual log force; irrelevant once (1) lands but cheap insurance for
   operators on other filesystems.

### Acceptance test

`scripts/lan-bench-suite.sh` (exchange-core) throughput workload,
`tcp-dual-repl`: the 10.24 s clusters disappear from the latency time
series (no window with p99 ≈ p99.99 above ~1 ms on the beat grid), and
health shows `rotations_total{path="fast"} > 0` with `sync_fallback`
dropping to ~0 in steady state. Expect p99.99 to tighten toward the
~300 µs the beat-free windows already show; p99.999+ should lose the
~1.8 ms shelf.

### Follow-up: staging-time log-force collisions (2026-08-13)

The first acceptance runs killed the beat (fast rotations 21/21,
`sync_fallback` 0) but surfaced a new stall class: ~9.4 ms
rotation-adjacent pipeline stalls at ~7/21 rotations, visible in
replica ack-latency gauges as well as the primary. Two staging-pacing
iterations (memcpy-clocked, then device-clocked double-window) changed
nothing — the stall was never data bandwidth.

Mechanism: `sync_file_range` flushes data pages but never logs
filesystem metadata, so the paced zero-fill accumulated the *entire
segment's* extent-allocation log items in the XFS CIL and detonated
them in the terminal `sync_all` — one segment-sized log force per
staging cycle. Log forces serialize filesystem-wide; any hot-path
fdatasync (primary's or a replica's — replicas run their own preparers,
and a delayed replica ack stalls the durability gate end-to-end)
landing in that window queues behind it. The fix that removed metadata
debt from steady state had batched the same debt into one lump.

Fix: the paced fill issues an incremental `sync_data` every 64 MiB on
the preparer thread, so each log force covers only the allocations made
since the previous one and a colliding fdatasync waits sub-millisecond.
The terminal `sync_all` remains the durability point but is cheap by
construction. (The `FALLOC_FL_WRITE_ZEROES` fast path is unaffected —
its allocation is a handful of extents, one small log item.)

That killed the ~9.4 ms class (run max 9.4 → 6.6 ms) but left a ~2.5 ms
stall at *every* rotation and p99.9 at ~1.5 ms vs the ~197 µs baseline.
An off-CPU trace of the primary's journal thread (sched_switch with
kernel stacks, 45 s / ~13 rotations) attributed it conclusively: every
block ≥ 1 ms — 1082 of them — was the hot-path fdatasync sleeping in
`folio_wait_writeback`, i.e. queued behind in-flight staging writeback
on the device; zero were in `xfs_log_force`, and the rotation-path
syscalls themselves (renames, dir fsync, archive truncate, header
sync) all completed in under 1 ms. The slow flushes formed a ~250 ms
burst after each rotation — the staging window — where every batch
flush took 1-2 ms instead of ~0.33 ms: the double-window pacing kept
two 2 MiB chunks (≈ 4 MiB ≈ 1.2-1.4 ms of device time) in flight ahead
of any colliding flush, and ~7% of samples at 1-2 ms is exactly a
~1.5 ms p99.9.

Final pacing shape: single-window (write one chunk, wait for *its*
writeback, sleep 3×) with 256 KiB chunks — at most ~90 µs of staging
IO can ever sit ahead of a hot-path flush, below the flush's own
~300 µs floor. Duty and staging duration are unchanged; only the
in-flight window shrank.

Accepted 2026-08-13 (throughput workload, tcp-dual-repl, 2 replicas,
1.23 M orders/s): p99.9 242 µs, p99.99 273 µs, **max 387 µs** — vs the
fix/io-uring-audit baseline's p99.9 197 µs / max 1912 µs (beat) and the
double-window run's p99.9 1520 µs / max 9.8 ms. 21/21 rotations on the
fast path, zero sync fallbacks, zero windows with p99.99 above 500 µs
in 74 k windows, no replica ack-latency spikes. The beat and both
staging-induced stall classes are gone.

## Implication: the sector writer

Both structural advantages the sector writer holds over the buffered
writer — no page-cache/CIL entanglement, prepared-segment rotation —
transfer to the buffered writer with this change. Combined with its
standing caveats (experimental, unresolved tail spike on some firmware,
silent data loss without PLP), that materially strengthens the case for
retiring it rather than paying the two-writer duplication tax the
roadmap already flags. If retirement is chosen, this fix touches only
the buffered writer and preparer, and the roadmap's "extract the shared
core first" rule no longer binds.

## Loose ends

- The identity of the 10.24 s XFS-internal cadence (1024 × 10 ms — some
  tick-counter-derived interval; none of the `fs.xfs.*` sysctls, which
  are all defaults) was never pinned down. Not needed for the fix.
- The bench fleet's journal mounts currently carry `lazytime` from the
  discrimination experiment (harmless; revert with
  `mount -o remount,nolazytime /mnt/journal` for pristine state).
- One primary (happy-mammal) rebooted spontaneously mid-investigation —
  provider ticket material, unrelated to the beat.
