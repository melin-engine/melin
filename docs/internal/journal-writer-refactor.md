# JournalWriter abstraction — audit & refactor plan

Internal design note. Audience: contributors. Not for `docs/` operator-facing
material.

## Current shape

`melin_journal::JournalWriter<E>` is a two-variant enum:

```rust
pub enum JournalWriter<E: AppEvent> {
    Sector(SectorWriter<E>),    // O_DIRECT, requires PLP, io_uring path
    Buffered(BufferedWriter<E>), // pwrite + fdatasync, default
}
```

Surface area:

- **~25 pass-through methods**, each a 2-arm `match` (`append`, `batch_append`,
  `batch_append_with_ts`, `allocate_sequence`, `encode_event`,
  `flush_batch_sync`, `discard_batch_buf`, `sync`, `next_sequence`,
  `set_next_sequence`, `write_pos`, `valid_end`, `path`, `chain_hash`,
  `events_since_checkpoint`, `pending_batch_bytes`,
  `last_user_entry_replication_slice`, `rotate_segment`, `read_genesis_entry`,
  `mode`, …).
- **5 constructors**: `create`, `create_default`, `create_continuing`,
  `open_append`, `create_fresh_replica`.
- **4 sector-only escape hatches**: `as_sector`, `as_sector_mut`,
  `unwrap_sector`, `unwrap_sector_mut`.

The variant is chosen once at startup from `--journal-writer` and never
changes for the lifetime of the value. Pipeline branches once on `mode()`
at the top of `JournalStage::run()` to pick `run_uring` (sector only) vs
`run_sync` (both variants).

## What's wrong

### 1. The enum is doing static dispatch's job, badly

`JournalStage` already commits to a writer at construction time and then
either runs `run_uring` or `run_sync`. The `unwrap_sector_mut()` calls
inside `run_uring` are the abstraction admitting it: *"I know this is a
Sector, but the type system doesn't."*

`run_uring` makes ~9 separate calls to `unwrap_sector*` per loop iteration
(`take_batch_for_async_write`, `confirm_async_write`, `fd`,
`io_uring_rw_flags`). Each is a runtime check whose only purpose is to
re-prove an invariant that was already established at construction time.

### 2. Redundant API on the writers themselves

`append`, `batch_append`, `batch_append_with_ts` are three shapes of the
same operation. The pipeline never calls any of them — it goes
`allocate_sequence` → `encode_event` → `flush_batch_sync` directly. Only
tests and benches use the `*append*` family.

### 3. Bloated constructor set

- `create_default` exists purely so tests don't have to pass a mode.
- `create_fresh_replica` is replica-bootstrap glue that wraps `open_append`
  with codec/header writes — fine as a helper, but unrelated to the variant
  dispatch.

### 4. `mode()` is a self-referential accessor

Used in two places: pipeline's run-dispatch (to pick `run_uring` vs
`run_sync`) and a metrics log line. Both go away under static dispatch.

## Recommendation: trait + generic `JournalStage`

Two-step refactor:

### Step 1 — define `trait JournalWrite`

Move the shared API onto a trait. Both `SectorWriter` and `BufferedWriter`
implement it directly.

```rust
pub trait JournalWrite<E: AppEvent> {
    fn allocate_sequence(&mut self) -> u64;
    fn encode_event(
        &mut self, seq: u64, ts: u64, event: &JournalEvent<E>,
        key_hash: u64, request_seq: u64,
    ) -> Result<(), JournalError>;
    fn flush_batch_sync(&mut self) -> Result<(), JournalError>;
    fn discard_batch_buf(&mut self);
    fn sync(&mut self) -> Result<(), JournalError>;

    fn next_sequence(&self) -> u64;
    fn set_next_sequence(&mut self, seq: u64);
    fn write_pos(&self) -> u64;
    fn valid_end(&self) -> u64;
    fn path(&self) -> &Path;
    fn chain_hash(&self) -> Option<[u8; 32]>;
    fn events_since_checkpoint(&self) -> u64;
    fn pending_batch_bytes(&self) -> &[u8];
    fn last_user_entry_replication_slice(&self) -> &[u8];
    fn rotate_segment(&mut self) -> Result<PathBuf, JournalError>;
    fn read_genesis_entry(&self) -> Result<Vec<u8>, JournalError>;
}
```

### Step 2 — make `JournalStage` generic over `W: JournalWrite`

```rust
pub struct JournalStage<E: AppEvent, W: JournalWrite<E>> { writer: W, ... }

impl<E: AppEvent, W: JournalWrite<E>> JournalStage<E, W> {
    pub fn run_sync(self, shutdown: &AtomicBool) -> Result<W, JournalError> { ... }
}

// Sector-only methods (io_uring) live in a specialized impl.
impl<E: AppEvent> JournalStage<E, SectorWriter<E>> {
    pub fn run_uring(self, shutdown: &AtomicBool) -> Result<SectorWriter<E>, JournalError> { ... }
}
```

Server boot:

```rust
match config.journal_writer {
    JournalWriterMode::Sector => {
        let writer = SectorWriter::create(...)?;
        JournalStage::new(writer, ...).run_uring(shutdown)
    }
    JournalWriterMode::Buffered => {
        let writer = BufferedWriter::create(...)?;
        JournalStage::new(writer, ...).run_sync(shutdown)
    }
}
```

### What disappears

- `JournalWriter` enum and its ~25 match arms.
- `as_sector`, `as_sector_mut`, `unwrap_sector`, `unwrap_sector_mut`.
- The runtime `mode()` dispatch at the top of `run()`.
- The compile-time impossibility of `run_uring` being entered with a
  buffered writer is proven by the type system, not asserted by `expect`.

### Costs

- 2× compiled pipeline code (one specialization per writer). Only one
  specialization runs per process — instruction-cache impact bounded.
- Trait dyn-compatibility is irrelevant here (we never need
  `Box<dyn JournalWrite>`); the trait can use generics freely.
- Test fixtures that previously took `JournalWriter<TestEvent>` need to
  either become generic or pick a concrete writer. Most can pick
  `BufferedWriter` directly.

### Conservative alternative (not recommended)

Keep the enum, prune the obvious dead weight:

- Drop `append`, `batch_append`, `batch_append_with_ts` from the enum
  (keep on concrete writers for test/bench use).
- Drop `create_default` (tests pass `JournalWriterMode::default()`).
- Drop `mode()` after the static-dispatch refactor obviates it.

~50 lines of churn, zero hot-path benefit, escape hatches remain. Useful
only as a cleanup pass; doesn't fix the real smell.

## Implementation plan

1. Define `trait JournalWrite<E>` in `melin-journal`, implement for both
   writers. Verify both compile and tests pass.
2. Move pipeline-internal helpers that touch `JournalWriter` to use the
   trait bound; leave `JournalWriter` enum in place for now.
3. Make `JournalStage` generic. `run_uring` moves to a sector-only `impl`.
   Branching on `mode()` moves from runtime to construction site.
4. Update every construction site (`server.rs`, `rumcast_transport.rs`,
   `replication/{tcp,dpdk,rumcast}_receiver.rs`, `replication-bench.rs`,
   `transport-core/journaled_app.rs`, `engine/journal/engine.rs`, benches,
   tests) to pick the concrete writer at startup.
5. Delete the `JournalWriter` enum and `create_fresh_replica` helper —
   the latter becomes a free function (or a `trait` extension method)
   parameterised by the chosen writer type.
6. Run full bench (`tcp-dual-repl throughput`, `standalone`) to confirm
   no regression — the change should be neutral-to-positive on the
   buffered hot path and remove one branch per syscall on the sector
   path.

## Acceptance criteria

- `JournalWriter` enum deleted.
- No `unwrap_sector*` or `as_sector*` calls anywhere.
- `cargo check` passes for all feature combinations: default, `dpdk,trading`,
  `dpdk,noop`, `rumcast,trading`, `rumcast,noop`, `melin-bench --features dpdk`.
- `tcp-dual-repl throughput` ≥ 3.6M ord/s on Cherry rig (current baseline:
  3.68M).
- Standalone buffered ≥ 3.8M ord/s (current baseline: 3.85M).

## Status — what landed

- ✅ `JournalWriter` enum deleted.
- ✅ `JournalStage`, `Pipeline`, `ReplicaPipeline`, `JournaledApp`,
  `JournaledExchange`, and the `build_*` factories are generic over
  `W: JournalWrite<E>`.
- ✅ `run_uring`, `enable_preparer`, and the fast-path
  `maybe_rotate_with_prepared` live in a `SectorWriter`-specialised impl;
  `run_sync` is on the generic impl. Each writer specialisation also
  exposes a single-argument `run` so call sites that work generically
  over the writer can stay terse.
- ✅ Trait-level constructors (`W::create`, `W::create_continuing`,
  `W::open_append`) let generic code build a writer of any concrete
  type without knowing which one.
- ✅ `create_fresh_replica` is now a free helper in
  `melin_journal::fresh_replica`, parameterised by the chosen writer.
- ✅ `JournalStageRun` trait lets generic boot code drive the stage
  through `stage.run(&shutdown)` regardless of which writer was
  selected; both specialisations implement it.
- ✅ Server boot path now dispatches on `--journal-writer` at the
  three public entry points (`run_with_shutdown`, `run_dpdk`,
  `run_rumcast`). `init_engine`, `run_as_primary`, the rumcast
  primary/replica helpers, and all three replica receivers
  (`run_receiver`, `run_receiver_dpdk`, `run_receiver_rumcast`) are
  generic over `W`. The `pub type JournalWriter = BufferedWriter;`
  shims in both `melin-engine` and `melin-server` are gone.
- ⚠️ `melin-bench` and `replication-bench` remain monomorphised on
  `BufferedWriter` by design — both are standalone harnesses; their
  `--journal-writer` flag is recorded for provenance only.
