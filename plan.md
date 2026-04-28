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

### Phase 3 — `feat(replication): replication ring carries InputBatch bytes`

**Status: ✅ in progress on `feat/unified-pipeline` (rolled phase 6 in —
DPDK migrated alongside since both senders share the ring).**

What's in it:

- New `crates/transport-core/src/replication_wire.rs` module owns the
  `InputBatch` wire format: constants (`MSG_INPUT_BATCH = 0x21`,
  `SLOT_TAG_*`), one-shot `encode_input_batch` / `try_decode_input_batch`
  (used by catch-up paths), and a streaming triplet
  `init_input_batch` / `append_input_slot` / `finalize_input_batch`
  (used by the journal stage hot path)
- `JournalStage` (in `transport-core/src/pipeline.rs`):
  - `ReplicationState` gained `input_batch_buf: Vec<u8>` and
    `input_batch_count: u16`, lazily initialized on the first slot of
    each fsync batch
  - `record_slot_for_replication` appends each just-encoded slot to the
    buffer alongside `writer.encode_event` (no-op when no producers)
  - `publish_input_batch_to_rings` finalizes the buffer (back-fills
    length / type / count) and publishes to the replication rings at
    the existing pre-fsync boundary, then resets count + clears bytes
- `tcp_sender.rs`: live ring → wire is now a passthrough — ring chunk
  bytes are wire-ready frames; the live path no longer calls
  `decode_journal_to_input_slots` + `encode_input_batch`. Catch-up
  overlap drain forwards as-is too.
- DPDK migrated in the same commit (rolling phase 6's DPDK work in,
  since the ring format is shared):
  - DPDK live sender: `slot.send_buf.extend_from_slice(data)` instead
    of wrapping in `encode_data_batch`
  - DPDK catch-up overlap drain: same passthrough
  - DPDK catch-up file path: decode journal bytes via
    `decode_journal_to_input_slots`, re-encode via
    `encode_input_batch` (same as TCP catch-up — journal *files* still
    contain journal-codec bytes; only the live ring switched)
  - DPDK receiver: `try_decode_input_batch` + per-slot
    `input_producer.publish()` (mirrors `tcp_receiver.rs`); removed
    `journal_accum` accumulator and the `submit_batch_to_pipeline`
    call site
- `replication/protocol.rs`: removed `MSG_DATA_BATCH`,
  `encode_data_batch`, `try_decode_data_batch`, the
  `PrimaryMessage::DataBatch` variant, and the `MSG_DATA_BATCH` arm in
  `decode_primary_message`. `decode_journal_to_input_slots` stays —
  catch-up paths still need it. `MSG_INPUT_BATCH` constants and the
  encode/decode helpers re-export from transport-core.
- `replication/mod.rs`: removed `submit_batch_to_pipeline`. Three
  unit tests that exercised end-to-end `DataBatch` round-trips were
  rewritten to use a small `encode_input_batch_with_seq` helper.
- 7 wire-format unit tests live in `replication_wire.rs` (transport-
  core), using a minimal `TestEvent: AppEvent`. The previous five in
  `protocol.rs` are gone — the implementation moved with them.

Validation:
- Default build, `--features dpdk`, and `--features "dpdk noop"
  --no-default-features` all clippy-clean
- 154 server tests pass (down from 163: 9 obsolete `DataBatch` tests
  deleted; the 7 wire-format tests now run in transport-core instead)
- Two known-flaky failover tests passed on retry, same as pre-phase-3

Removed entirely: `MSG_DATA_BATCH`, `encode_data_batch`,
`try_decode_data_batch`, `submit_batch_to_pipeline`,
`PrimaryMessage::DataBatch`. Phase 6's cleanup landed here.

### Phase 4 — `refactor(transport-core): factor shared input-disruptor + chain-hash setup`

**Status: ✅ committed on `feat/unified-pipeline`.**

The literal "collapse into one `build_pipeline(role: PipelineRole)`"
forces the unified return type into a soup of `Option`s (drain_consumer
for replica only, output_consumers for primary only, last_seq for
replica only, replication_consumers for primary only, …) — every caller
then unwraps the role-specific fields. Trades duplication for
awkwardness; not obviously a win, which is probably why the plan tagged
it "cosmetic; deferred".

What landed instead: a focused DRY refactor that extracts the
genuinely-shared bits without changing the public API.

- `build_input_disruptor<E>(enable_shadow) -> InputDisruptorParts<E>`:
  one place owns the input ring topology (journal + matching gated on
  producer in parallel + optional shadow chained after journal),
  consumer-pop order, and progress-cursor extraction
- `setup_chain_hash_publisher(&mut journal_stage, enable_shadow)`:
  one place owns the BLAKE3 chain-hash SeqLock allocation + wiring
- `build_pipeline_with_replication` and `build_replica_pipeline` keep
  their existing public signatures and return types; their bodies just
  destructure the helper outputs and add the role-specific bits

Net diff: ~+25 lines. Each builder now reads as the role-specific delta
(replication rings + replicas_connected halt + response/event-publisher
output ring on the primary; drain consumer + last_seq publisher on the
replica) on top of a shared skeleton.

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

### Phase 5b — DPDK port

**Status: ✅ in progress on `feat/unified-pipeline`.**

Mirrors the TCP work above onto `run_receiver_dpdk`:

- `ReplicaPipelineHandles`, `build_replica_pipeline_with_threads`,
  `teardown_replica_pipeline` moved from `tcp_receiver.rs` to
  `mod.rs` so both transports share them
- `run_receiver_dpdk` gains `cores: PipelineCores` and `busy_spin:
  bool` parameters; pipeline children pin to the configured cores
  instead of running unpinned (matches TCP's behaviour)
- Hoisted `pipeline: Option<ReplicaPipelineHandles>` above the outer
  reconnect loop; top of each iteration refreshes `last_sequence`
  and `chain_hash` from the atomics when `Some`
- `NeedSnapshot` path tears down the live pipeline before wiping the
  journal/snapshot files (necessary — the journal stage holds the
  file open and the App lives on the matching thread)
- `clear_affinity` + `pin_replica_thread("receiver", ...)` wrap the
  build block so children spawn on un-pinned context, then the
  receiver re-pins (same dance as TCP)
- Inner streaming loop now `break`s with a `SessionExit` enum
  (`Disconnected` / `Shutdown` / `Promote` / `Fatal(err)`) instead of
  inline `return`; the post-loop match drives teardown

Phase 5a-equivalent ("split the receiver onto its own pinned thread")
doesn't apply on DPDK — smoltcp is single-threaded and DPDK's
transport poll loop has to run continuously, so all
orchestration + streaming live on the same pinned thread by design.
The existing `pin_replica_thread("receiver", receiver_core)` call
already covers the architectural intent.

### Phase 6 — `feat(replication): DPDK uses InputBatch; remove DataBatch`

**Status: ✅ landed inside the phase 3 commit.** Phase 3 forced the DPDK
migration since both senders share the replication ring; rolling them
together avoided the dual-format detour that "TCP only" would have
required. See phase 3 above for the exact changes.

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

- On `feat/unified-pipeline`, rebased onto `main` (which picked up
  `91b6cbb perf(server): pin replica receive loop` — superseded by our
  phase 5a `thread::scope` approach; the conflict resolved cleanly to
  ours)
- Phases 1, 2, 3 (incl. phase 6), 4 (DRY refactor), 5a, 5b (TCP + DPDK)
  all committed
- 906 workspace tests pass; clippy clean on default, `--features dpdk`,
  and `--features "dpdk noop" --no-default-features`
- No outstanding work items on the plan

End-to-end bench validation is now meaningful for phase 3 — the live
TCP path lost an entire decode + re-encode pass per ring chunk. Combined
with 5a (pinned receiver thread) and 5b (pipeline lives across
reconnects), expect tcp-repl to close most of the gap with the
standalone topology.

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
