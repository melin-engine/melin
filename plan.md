# Unified primary/replica pipeline — plan

Working branch: `feat/unified-pipeline` (from `main`).

## Goal

Eliminate the asymmetry where the replica runs a *different* receive path
from the primary. After this, primary and replica share **the same** ingest
path: `wire → frame decode → input ring → matching → journal → response/drain`.
The replica differs only in (a) its single input source (TCP from primary
instead of multiple client connections), (b) sequence-gap detection, and
(c) what its response stage drains to (no client emits).

This removes the receive path that's currently capping replication
throughput at ~1.5M ops/s, since the replica's pipeline becomes literally
the same one that runs at 2.36M+ on the primary.

## Diagnosis context (why we're doing this)

What we measured before deciding to change the architecture:

- `tcp-repl` throughput plateaus at ~1.18-1.5M ops/s
- Every primary stage idle (journal 0.1%, matching 3%, response 0.1%)
- Replication ring depth ~0, ack latency ~60 µs, no evictions, replica
  fully caught up
- Adding bench window doesn't scale (queues; latency rises proportionally)
- Adding clients only partially scales (16→32 = +28%, not +100%)
- Standalone (no repl) hits 2.36M; the gap is the replication round-trip

What turned out to be the architectural insight: **the replica is already
doing input replication**. Look at `replication/mod.rs::submit_batch_to_pipeline`:
it decodes journal-byte frames back into `InputSlot`s and pushes them through
the local pipeline. The journal stage on the replica re-encodes its own
journal bytes (relying on engine determinism) — confirmed by the comment
near `encode_data_batch`:

> Per-batch chain hashes are not transmitted: with input replication each
> replica re-encodes its own journal, so the primary's per-batch hash would
> not match the replica's.

So the runtime model is correct; the implementation just routes InputSlots
through the journal codec on send and back through it on receive, when it
could just send InputSlots directly. The structural asymmetry is **purely
in the wire format and the receive code path**, not in the pipeline.

## Auth note

No new role needed. The existing `replication` permission already encodes
"trust this peer to push state into our pipeline." The wire format change
doesn't widen that trust. Keep the permission, change what it carries.

(If we ever need to verify "the peer is *the* current primary, not just
something with the replication key" for HA / split-brain safety, that's a
separate leader-lease / token discussion.)

## Six-phase plan

### Phase 1 — `feat(replication): InputBatch wire format alongside DataBatch`

**Status: ✅ committed as `f0b8e54` on `feat/unified-pipeline`.**

What's in it:

- `protocol.rs`: new `MSG_INPUT_BATCH = 0x21`, `SLOT_TAG_*` constants,
  `encode_input_batch(&[InputSlot], &mut Vec<u8>)`,
  `try_decode_input_batch(&[u8]) -> io::Result<Vec<InputSlot>>`
- Wire layout:
  ```
  [type:0x21] [count:u16]
  for each slot:
    [event_size:u16]
    [sequence:u64] [timestamp_ns:u64]
    [key_hash:u64] [request_seq:u64]
    [event_tag:u8] [event_payload]
  ```
- Stripped vs journal codec: no per-entry magic, no length envelope, no
  CRC32C (TCP handles framing/integrity)
- `connection_id`, `publish_ts`, `recv_ts` not on the wire (primary-internal)
- 5 unit tests: transport variants roundtrip, empty batch, wrong type tag,
  truncated header, truncated slot payload — all passing
- New symbols are `#[allow(dead_code)]` so cargo clippy stays clean while
  the runtime still uses `DataBatch`

### Phase 2 — `feat(replication): TCP path uses InputBatch wire format`

**Status: ✅ committed as `209840b` on `feat/unified-pipeline`.**

What's in it:

- `tcp_sender.rs`, `catchup.rs`: read journal bytes from the replication
  ring (live) or from journal files (catch-up), decode them via the new
  `decode_journal_to_input_slots` helper, then encode as `InputBatch`
- `tcp_receiver.rs`: decode `InputBatch` via `try_decode_input_batch`,
  publish slots into the local input ring directly (no more
  `journal_accum` / `submit_batch_to_pipeline`), one `pending_acks`
  entry per RECV CQE covering all slots in that buffer
- Function signatures shrink: `replica_stream_uring` drops its
  `journal_accum: &mut Vec<u8>` parameter
- DPDK path unchanged; `submit_batch_to_pipeline`, `try_decode_data_batch`,
  `encode_data_batch` are `#[allow(dead_code)]` under non-DPDK builds
  (still used by DPDK; phase 4 migrates them and removes them entirely)

Note: the sender does an extra journal-codec decode pass per batch (to
turn ring bytes back into InputSlots before re-encoding as InputBatch).
Phase 3 removes this round-trip.

163 server tests pass.

### Phase 3 — `feat(replication): replication ring carries InputSlots directly`

**Status: not started.**

Change what the journal stage publishes to the replication ring: instead
of journal-bytes (encoded for disk), push the `InputSlot` records it just
durably journaled. The replication ring still sits *after* fsync, so
durability semantics are preserved (we send only what's on disk).

Approach:
1. The replication ring stays a byte-buffer ring (`ReplicationProducer` +
   `SharedBuffers`), but the bytes stored are now `InputBatch` wire bytes
   instead of journal-codec bytes
2. `JournalStage` encodes each event into the writer's journal-codec buffer
   AND into a parallel `InputBatch` buffer, then publishes the InputBatch
   buffer to the replication ring at sync time
3. Sender (TCP and later DPDK) reads bytes from the ring and forwards
   directly — no more `decode_journal_to_input_slots`

Tradeoff: extra encode pass on the journal hot path. Should be cheap
(InputBatch encode skips CRC compute) and amortized over the fsync that
already happens. Keeps the ring infrastructure unchanged.

Alternative (more invasive): change ring storage from `[u8; CHUNK_SIZE]`
to `[InputSlot; SLOTS]`, eliminating both encodes. Punt for now.

Removes `decode_journal_to_input_slots` from the live + catch-up paths.

### Phase 4 — `refactor(transport-core): build_pipeline accepts a Role enum`

Collapse `build_primary_pipeline` and `build_replica_pipeline` into one
`build_pipeline(role: PipelineRole)`. Mostly cosmetic; deferred — the
two builders share most of their body, but they have meaningfully
different signatures and their callers are well-isolated, so the
collapse can happen any time.

### Phase 5a — `feat(replication): pin replica receiver to its own thread`

**Status: ✅ committed as `a8dcf2c` on `feat/unified-pipeline`.**

The replica's streaming-receive loop used to run on the orchestrator
(main) thread alongside connect/auth/handshake/snapshot logic and the
reconnect outer loop — unpinned, contending with whatever else main
was doing. The primary's reader (reader.rs) runs on a dedicated pinned
thread (reader_cores, default core 4); this commit gives the replica
the same treatment via `std::thread::scope` + `pin_replica_thread`,
plumbing `config.reader_cores` through `run_receiver`.

### Phase 5b — `feat(replication): replica pipeline lives across reconnects`

**Status: ✅ committed as `08feffd` (transport-core prep) and `862078a`
(server-side refactor) on `feat/unified-pipeline`.**

The replica pipeline (input ring + journal/matching/drain/shadow
stages) is now built once and persists across `Disconnected`
reconnects, mirroring how the primary's pipeline outlives individual
client connections. Only `Snapshot` transfer (rare), `Promote`,
`Shutdown`, and `Fatal` exits tear it down.

Structural pieces:
- `JournalStage` gained an `Option<Arc<AtomicU64>>` last-seq publisher
  (set on replicas, ignored on primaries) updated post-fsync alongside
  the existing `chain_hash` SeqLock
- `ReplicaPipelineHandles` bundles input_producer, journal_cursor, the
  two atomics, shutdown flag, and the four thread handles
- `build_replica_pipeline_with_threads` / `teardown_replica_pipeline`
  factor the pipeline-build + thread-spawn / join-and-recover code
  that used to live inline
- `run_receiver` hoists `pipeline: Option<...>` above the outer loop;
  top of each iteration refreshes last_sequence / chain_hash from the
  atomics (when Some); `NeedSnapshot` path tears down before wiping
  the journal; pipeline build fires on `pipeline.is_none()` only

This phase eliminates the per-reconnect cost of journal-recovery +
thread-spawn + warm-up. Health endpoint integration (the original
phase 5 motivation) becomes trivial from here — `HealthState` can
read directly from the long-lived `ReplicaPipelineHandles` atomics
without needing the receiver to surrender state.

### Phase 6 — `feat(replication): DPDK uses InputBatch; remove DataBatch`

Today's catch-up replays journal-bytes from disk. Under the new model:

- Replica reconnects, sends `last_sequence` in handshake
- Primary streams `InputBatch` from `last_sequence + 1` — read from journal,
  decode the InputSlots that were stored there, send them as InputBatch
  frames (one-shot batch loop, off the live streaming path)
- If primary's journal doesn't go back that far, send `NeedSnapshot`

Strictly simpler than the current `catch_up_from_journal_dpdk` path, which
has its own framing + transport-level integration.

Final cleanup: remove `MSG_DATA_BATCH`, `encode_data_batch`,
`try_decode_data_batch`, `submit_batch_to_pipeline`, and the
`#[allow(dead_code)]` annotations on the new symbols.

## Testing

- Existing failover suite (17 tests) is the regression bar — promotion,
  snapshot transfer, reconnect under load all need to pass
- Add: a determinism test that runs the same sequence of orders through
  two engines and asserts journal bytes are identical (we have this
  implicitly; make it explicit)
- Re-run `tcp-repl` and `tcp-dual-repl` benches after phase 5; expectation
  is throughput closes the gap with standalone (or comes within a small
  constant)

## Risk / rollback

- Each phase is a separate commit. If a phase causes a regression we can't
  fix quickly, revert to the previous (which still ships old `DataBatch`
  paths). Phases 3 and 4 must land together to switch protocols, but they
  can land in one PR even if implemented as two commits.
- Determinism violation (the load-bearing risk): if some part of matching
  produces non-deterministic output, journals diverge silently. Catch via
  the explicit determinism test above, plus the existing journal-verify
  post-bench check.

## Out of scope for this branch

- Multi-master / write-replicas
- Quorum changes (keep `min(both_acked, max(journal, fastest_acked))`)
- Snapshot wire format changes
- DPDK replication path — the same architectural change applies, but the
  TCP path is the priority; once the TCP refactor is solid, mirror it on
  the DPDK path

## Where we left off

- On `feat/unified-pipeline` branched from `main`
- Phases 1, 2, 5a, 5b committed (`f0b8e54`, `209840b`, `a8dcf2c`,
  `08feffd`, `862078a`)
- 163 server tests passing, clippy clean
- Outstanding phases: 3 (ring carries InputSlots directly — eliminates
  the sender-side journal-codec round-trip), 4 (collapse build_pipeline
  /build_replica_pipeline), 6 (DPDK migrates to InputBatch + cleanup)

End-to-end bench validation can run now — phases 5a and 5b should both
move tcp-repl numbers (5a removes contention with orchestrator on main;
5b removes per-reconnect cost). Phase 2's sender-side journal round-trip
will be eliminated by phase 3.

## Related branches (not part of this work but referenced)

- `feat/replica-health-endpoint`: standalone work to expose the replica's
  `/healthz`. Becomes redundant in phase 5; can be dropped.
- `perf/parallel-replica-acks`: +9% throughput via parallel ack SQEs on
  the receiver. Patches the old protocol; becomes irrelevant once the
  receive loop is rewritten in phase 4.
- `perf/replication-pipeline`: bumped `replication_batch_size` and
  `PendingAckQueue::CAP`. Null result on tcp-repl (knobs weren't binding).
  Becomes irrelevant under the new protocol.
- `feat/journal-prealloc-watermark`: +32% on tcp-repl, -19% on tcp-dual-repl.
  Orthogonal to this work — can compose. Re-evaluate after phase 5 lands.
