# Continuous snapshots in the LAN benchmark, on the OS disk (plan)

Status: **planned** (2026-10). Step 0 addresses the snapshot-location
half of determinism-audit findings 21 and 22; the rest of both stays open
(see "Not part of this item").

The one-line goal: **every LAN benchmark run takes shadow snapshots
throughout its measured phase, on every node, written to the OS disk and
never to the journal disk, and the suite proves it did.** The figures then
describe the configuration an operator runs in production (the shadow is
on by default) without snapshot I/O sharing a device, a filesystem or a
filesystem log with the journal's writes and `fdatasync`s. Leftover
snapshots are removed before every run, an aborted one's included.

This is not an A/B mode. There is no switch that puts snapshots back on
the journal disk; the suite refuses a snapshot directory that shares a
disk with the journal (see "The disk guard").

## What the code shows

- **Snapshots are effectively off in today's benchmark.** The suite
  passes no snapshot flag, so the shadow runs at the runtime default,
  `--snapshot-interval-ms 3000000` (50 min). A run lasts about 70 s, so
  no snapshot is ever written. Had one been written, it would have landed
  on the journal disk: `ServerConfig::shadow_snapshot_path`
  (`server-runtime/src/server.rs`) defaults to `<journal>.snapshot`.
- **`--snapshot-path` moves every snapshot write.** The shadow writes
  `<path>.tmp`, fsyncs it, renames the previous snapshot to `<path>.prev`,
  renames the tmp file over `<path>`, and fsyncs the parent directory
  (`snapshot::save_with_limit`, `transport-core/src/snapshot.rs`). All
  three files are siblings of `<path>`, so both renames stay on one
  filesystem. Replicas take the same path: `run_receiver` is handed
  `config.shadow_snapshot_path()`, and the replica's shadow
  (`replication/mod.rs`) runs the same `shadow::run`. A replica that is
  re-seeded receives its snapshot into
  `<path>.with_extension("snapshot.tmp")` next to `<path>`, which is also
  on the OS disk (but is not the shadow's `<path>.tmp` when `<path>` does
  not end in `.snapshot`; step 0 unifies the two).
- **Nothing else that a snapshot produces writes to the journal disk,
  except the resync archive.** The segment seed (`<journal>.seed.tmp`) is
  journal data and belongs there. Rotation archives segments and never
  touches snapshots (`docs/journal-rotation.md`). Catch-up serving only
  *reads* a snapshot. The resync archive moves the snapshot into the
  journal's directory; step 0 keeps it on the snapshot's own filesystem.
- **`--snapshot-path` is ignored by primary boot and snapshot
  transfer** (the location half of determinism-audit findings 21 and
  22, fixed by step 0). Primary
  boot (`init_engine`, which reads `--snapshot` or `<journal>.snapshot`)
  and snapshot transfer (`catchup::preflight_snapshot_transfer` and
  `catchup::snapshot_transfer_with`, both hard-coded to
  `<journal>.snapshot`) never look at `--snapshot-path`. Replica recovery
  and the shadow do. The resync archive (`archive_local_lineage`) renames
  the snapshot into the journal directory, which fails with EXDEV when
  the two are on different filesystems. None of this affects a benchmark
  run: every run starts fresh clusters on empty journals, with no restart
  and no resync. It does mean the layout the benchmark measures is not
  one an operator can run in production until step 0 lands.
- **A deferred snapshot waits a whole interval.** `shadow::run` calls
  `try_save_snapshot` once per timer tick. If the shadow's ring cursor is
  not at the journal's last fsync boundary (`FsyncState.input_ring_seq`),
  the call returns without saving and without logging anything, and
  `AmortizedTimer::tick` has already moved its deadline on. Under
  sustained load the journal publishes a new boundary every few
  microseconds, so the attempt can miss again on every tick. A primary
  could then go several intervals with no snapshot and leave no record
  that it did. Run continuously, the benchmark would make this visible,
  and "a run that silently wrote no snapshot" is exactly what it should
  catch.
- **The save's cost lands on the shadow's thread, and the shadow can
  apply backpressure.** The shadow is a terminal consumer of the input
  ring. While a save serializes, writes and fsyncs, it consumes nothing,
  and the producer gates slot reuse on it. A save that takes longer than
  the ring's headroom at the offered rate therefore stalls ingress,
  whichever disk it is on. A replica's shadow does the same to the
  replica's ingest, which shows up as lag, catch-up or eviction. Nothing
  records how long a save takes today: the "shadow snapshot saved" line
  carries only `journal_seq` and `path`.
- **Global dirty-page accounting still couples the two disks.** The
  journal writer (`BufferedWriter`) writes into the page cache with
  `pwrite` and then calls `fdatasync`, so its writes pass through
  `balance_dirty_pages`. `server-setup.sh` caps dirty memory at
  `vm.dirty_background_bytes = 32 MiB` and `vm.dirty_bytes = 64 MiB`.
  The snapshot is written with one `write_all` of the whole buffer, then
  `sync_all`. A snapshot larger than the freerun ceiling (about 48 MiB at
  these settings; the Exchange Core's state at `EC_ACCOUNTS=100000` may
  reach it) pushes the global dirty count above it while the write is in
  progress. Every writer, the journal's disk thread included, is then
  rate-limited in `balance_dirty_pages`, even though the pages it dirties
  are on another device. Separate disks remove the shared filesystem log
  and the shared device queue. They do not remove this coupling.
- **Interrupts and cores do not couple the two disks.** NVMe completion
  vectors are per-queue and managed, and a completion arrives on the
  submitting CPU's queue. The shadow's `sync_all` is submitted from the
  shadow's core and the journal's `fdatasync` from the journal-disk
  thread's core. Background writeback runs on core 0, where `pin_irqs`
  already puts the writeback workqueue. The two devices therefore share
  no completion vector, and nothing in this plan changes IRQ placement.
- **The health endpoint has no snapshot metric.** The node log is the
  only record that a save happened (the "shadow snapshot saved" and
  "shadow snapshot failed" lines). The suite already copies every node's
  log into the results after each run (`save_node_logs`).

## Step 0: one snapshot location for every role

The layout the benchmark measures (journal on its own disk, snapshots on
the OS disk) must be one an operator can run. Step 0 makes it safe
before the suite measures it. It fixes *where* every role finds the
snapshot and *where* a resync archives it, which is the location half of
determinism-audit findings 21 and 22. Validation of a served snapshot,
crash-atomicity of the archive and orphan snapshots stay open (see "Not
part of this item"). Neither half below needs those to be done cleanly.
Two consequences are worth stating. A primary now serves the file at
`--snapshot-path`, still checked by header only, which is the same exposure
the derived file has today, not a new one. And an archive split across
two directories is no more crash-atomic than one in a single directory.

### 0a. One flag, one resolver (finding 22, location half)

**`--snapshot` is removed, and `--snapshot-path` is the node's only
snapshot location.** Both flags name "this node's snapshot file". They
differ only in which roles honour them. `--snapshot-path` is the
one the shadow and replica recovery already use, and the one the
benchmark will pass (step 4). Today
`--snapshot` means "restore from this file at boot", and only on a
primary. The alternatives:

- **Alias `--snapshot` to `--snapshot-path`.** Rejected. It would silently
  turn an existing `--snapshot X` command line from "read X once" into
  "X is where this node writes". The shadow's first save would rotate X
  to `X.prev` and overwrite the previous `X.prev`, so a backup an operator
  restored from would be rotated away without a word.
- **Keep both with a precedence rule.** Rejected, because two flags for
  one file is how finding 22 arose.
- **Remove `--snapshot` (recommended).** A command line that still
  carries it fails at parse time ("unexpected argument"), which forces the
  operator to decide. To restore from a backup, copy it to the snapshot
  path, as the standard upgrade already does with the derived path.
  Giving both flags is then impossible, not a rule to remember.

**An explicit path that does not exist, with no journal** (finding 22's
second consequence): **not closed by step 0.** Step 0 removes the
read-once flag, so the case no longer arises from two flags naming one
file: with one location, a missing snapshot and no journal is treated as
a first boot whether the path is derived or explicit, and the shadow then
creates the file. But a mistyped `--snapshot-path` on the upgrade's
snapshot-only layout still boots Fresh from genesis, the same hazard as a
mistyped `--journal`. Closing it needs an explicit "this boot continues a
history" intent, which is the follow-up's, and the audit keeps this
sub-point open. Step 0 only adds start-up refusals that are cheap and
unambiguous (three planned, two more found while building it):

- the snapshot path names no file (empty, or ending in `..`): nothing
  could be written at it, and its `.tmp`/`.prev` siblings and its
  archive directory are named after its file name;
- the snapshot path names an existing directory, or a link to one (e.g.
  `--snapshot-path /var/lib/snaps/`): boot would fail later with EISDIR
  reading it, or the shadow would log `error!` every interval; refused
  up front with a message naming the flag;
- the snapshot's parent directory is missing or not a directory (today
  the shadow would log `error!` every interval and never save). The
  resolver maps both a `None` parent and an empty one to `.`: the default
  bare `melin.snapshot` has `Some("")` as its parent, not `None`, and
  `Path::new("").is_dir()` is false, so mapping only `None` (as
  `archive.rs` did) would refuse the default layout. That rule was
  already written out twice in `melin_journal::segment` and once in
  `snapshot.rs`; it is hoisted to `melin_journal::segment::parent_dir`
  (with `fsync_dir`, the sync of a directory itself), and every copy uses
  it;
- the snapshot path equals the journal path, compared after
  canonicalising both directories (a `..` or a symlink does not hide it;
  a journal whose directory does not exist yet is in another directory);
- `--snapshot-path` is given, nothing is at it, the node has no journal
  lineage, and the derived `<journal>.snapshot` exists. This is an
  operator adding `--snapshot-path` for the first time to a node on the
  snapshot-only upgrade layout: today that command line recovers from the
  derived file, and after step 0 it would boot Fresh from genesis beside
  it. The refusal names both paths and says to move the derived file to
  the new location. It needs no "continues a history" intent, so it
  belongs here rather than in the follow-up.

**The type.** A new `NodeStore` in
`crates/core/transport-core/src/node_store.rs`, with private fields and
one constructor:

```rust
pub struct NodeStore { journal: PathBuf, snapshot: PathBuf }
impl NodeStore {
    pub fn resolve(journal: PathBuf, snapshot_path: Option<PathBuf>)
        -> Result<Self, NodeStoreError>;
    pub fn journal(&self) -> &Path;
    pub fn snapshot(&self) -> &Path;
    pub fn snapshot_tmp(&self) -> PathBuf;  // <snapshot>.tmp
    pub fn snapshot_prev(&self) -> PathBuf; // <snapshot>.prev
    pub fn journal_lineage_exists(&self) -> io::Result<bool>;
    pub(crate) fn snapshot_shares_journal_dir(&self) -> io::Result<bool>;
}
```

As built, `journal_lineage_exists` (a live or archived segment on disk)
is the check the third refusal makes and replica recovery's "fresh
replica" test, one copy for both, and `snapshot_shares_journal_dir` is
the archive's canonicalised layout test. `NodeStoreError` has a variant
per refusal and an `Io` one carrying the path that could not be
inspected; it is re-exported at the crate root with `NodeStore`.

Every function that reads, writes, serves or archives the snapshot takes
`&NodeStore` instead of a journal path or a bare snapshot `&Path`, with
two exceptions whose caller takes the path from the store: the file-level
primitives `snapshot::save` and `load` (below), and `shadow::run`, the
writer, which lives in `transport-core` beside them and is handed
`store.snapshot()` by its spawn. The
derived-path rule (`with_extension("snapshot")`) then exists in one
place. The siblings reuse `snapshot.rs`'s `with_suffix`, `TMP_SUFFIX` and
`PREV_SUFFIX` (made `pub(crate)`), so the shadow's temp file and a
re-seeded replica's become the same `<snapshot>.tmp` and cannot drift.
The journal and the snapshot are paired, rather than wrapping the snapshot
alone in a newtype, because catch-up and archival take both. A pair means
no caller can combine the right snapshot with another node's journal,
which several tests in `tcp_receiver.rs` do by hand today. The pair is
built once per boot by `ServerConfig::store()`, right after
`validate_primary_config` in each transport's entry chain (`run_impl`
and `run_dpdk_impl`), so a refused layout leaves the disk untouched, and
passed down: to `init_engine`, `run_as_primary`/`run_as_primary_dpdk`
(and through them the shadow spawn and the senders), and the receivers.

`ServerConfig::shadow_snapshot_path` is removed.

What the type cannot stop is a caller that calls `NodeStore::resolve(journal,
None)` itself. Review checks that `resolve` has one non-test caller
(`rg 'NodeStore::resolve'`), and that no non-test
`with_extension("snapshot")` remains outside `node_store.rs`.

`snapshot::save` and `load` keep taking `&Path`. They are file-level
primitives, and their callers obtain the path from the store.

### 0b. Archive the snapshot on its own filesystem (finding 21, location half)

`archive_local_lineage(store: &NodeStore, reason)`:

- when the journal's and the snapshot's directories are the same after
  canonicalisation, as with the default derived path, keeps today's single
  `<journal dir>/<journal-name>.<reason>.<n>/` holding everything, so an
  operator who does not set `--snapshot-path` sees no layout change;
- otherwise picks the lowest `n` for which
  `<journal-name>.<reason>.<n>` is free in the journal's directory **and**
  `<snapshot-name>.<reason>.<n>` is free in the snapshot's, then moves the
  archives and the live segment into the first and the snapshot and its
  `.prev` into the second. The snapshot half is named by the snapshot's
  own file name, not the journal's, so nodes sharing one snapshot
  directory (the bench's `SNAPSHOT_DIR/<node>.snapshot`) never share a
  name series even if their journal file names coincide, and the
  shared-free-`n` rule never skips indices on the journal side because of
  another node. `.prev` is
  old-lineage material too, and today it is left beside the new lineage's
  snapshot. `.tmp` is never history and is not moved. `n` is chosen free
  in both directories even when only one side has material, so a
  snapshot-only archive `.0` and a later journal-only `.0` can never pose
  as one resync;
- fsyncs, best effort as today, both new archive directories and both
  source parents: the journal's directory and the snapshot's. Today's
  single `fsync_parent_dir(journal_path)` covers only the first parent;
  without the snapshot's, a power loss can bring the old-lineage
  snapshot back at `--snapshot-path` beside the re-seeded journal, the
  state finding 21 describes. A comment beside the calls names the set
  (the fsyncs cannot be observed from a test without a power cut);
- returns `Option<ArchivedLineage { journal_dir, snapshot_dir }>` (each
  an `Option<PathBuf>`, `Some` when that side had material to move) and
  logs both directories. In the default layout both fields that are
  `Some` name the same directory, so a caller looking for the archived
  snapshot reads `snapshot_dir` whatever the layout.

Every rename now stays inside one directory, and so on one filesystem,
so EXDEV cannot occur. The shared `n` pairs the two halves for
reconciliation, and the log line and `ArchivedLineage` name both: an
operator, or later tooling, finds the archive of resync `n` as
`<journal-name>.<reason>.<n>` beside the journal and
`<snapshot-name>.<reason>.<n>` beside the snapshot. Recovery never reads an
archive, and it reads the snapshot only through `NodeStore`, so an
archived file cannot be picked up. The order of the moves is unchanged.
On two filesystems the existing non-atomic window gains one directory.
The durable intent marker (follow-up) closes it.

One more gap of the same kind was closed while building this: a
re-seeded replica renamed the received snapshot into place and relied on
the seed's later `fsync_parent_dir(journal)` for its durability, which
covers the snapshot only when the two share a directory. It now syncs the
snapshot's own directory after that rename.

### 0c. Files and functions

- `transport-core/src/node_store.rs` (new): `NodeStore`,
  `NodeStoreError`, and the resolver's unit tests.
- `transport-core/src/test_fs.rs` (new, `test` or `test-utils` only):
  `second_filesystem_dir(first)`, registered in `lib.rs` beside `test_ports`.
- `transport-core/src/snapshot.rs`: `with_suffix`, `TMP_SUFFIX` and
  `PREV_SUFFIX` become `pub(crate)`.
- `transport-core/src/replication/archive.rs`: `archive_local_lineage`
  as above, with a small pure helper for the shared free index.
- `transport-core/src/replication/catchup.rs`:
  `preflight_snapshot_transfer` and `snapshot_transfer_with` take
  `&NodeStore` and read `store.snapshot()`. The journal-only functions
  are unchanged.
- `server-runtime/src/server.rs`:
  - remove the `ServerConfig::snapshot` field (`--snapshot`, also set
    in `Default`) and `ServerConfig::shadow_snapshot_path`;
  - add `ServerConfig::store()`;
  - rewrite the `--snapshot-path` doc comment (its `--help` text): it is
    the node's one snapshot location, read at boot, served and written,
    defaulting to `<journal>.snapshot`; "the startup snapshot path" goes;
  - in `init_engine`, replace the path selection under "Check for a
    snapshot" (`config.snapshot` / `derived_snap`) with `store.snapshot()`;
  - pass the store to the shadow spawn and to both `run_receiver` call
    sites;
  - switch the genesis tests that save to
    `cfg.journal.with_extension("snapshot")` to the store.
- `server-runtime/src/replication/mod.rs`: replica recovery (the
  `snapshot_path.exists()` branch), `receive_resync_transfer` (the temp
  path and the rename), the archive call, and the failed-transfer
  `remove_file`.
- `server-runtime/src/replication/{tcp_sender,join_worker,dpdk,tcp_receiver}.rs`:
  - `JoinContext::journal_path` becomes `store`;
  - the TCP and DPDK senders and `run_receiver_dpdk` carry the store;
  - the `tcp_receiver.rs` tests stop pairing
    `primary_journal.with_extension("snapshot")` by hand.
- `server-runtime/src/server.rs`, beyond the above: `init_engine`,
  `run_as_primary`, `run_as_primary_dpdk` and `spawn_shadow_stage` take
  `&NodeStore`; `init_engine` reads the journal from the store too.
- `journal/src/segment.rs`: `parent_dir` and `fsync_dir` (see 0a), used
  by `list_archives`, `fsync_parent_dir`, `snapshot::save_with_limit`,
  the store and the archive.
- `.config/nextest.toml`: the new `snapshot_location` binary joins the
  `cluster-serial` group.
- `scripts/miri.sh`: add `--skip node_store::` to `SKIPS` (its tests
  touch the filesystem). `test_fs` carries no tests of its own, so it
  needs no skip; `replication::archive` and `replication::catchup` are
  already skipped.
- `docs/internal/determinism-audit-2026-09.md`: in the triage table and
  in sections 21 and 22, mark both findings partially fixed and name what
  remains (for 22, the missing-explicit-path-boots-Fresh sub-point stays
  open).

### 0d. Tests

**Unit tests (transport-core, ungated):**

- `NodeStore::resolve`:
  - derived from `x.journal`;
  - derived from a journal with no extension;
  - the default bare `melin.journal` (empty parent, taken as `.`, not
    refused as missing);
  - an explicit path used as given;
  - a missing parent directory is refused;
  - a parent that is a file is refused;
  - a snapshot path equal to the journal is refused;
  - an explicit path with nothing at it, no journal, and a derived
    `<journal>.snapshot` present is refused, naming both paths; the same
    with no derived file, or with a journal present, is accepted;
  - `snapshot_tmp` and `snapshot_prev` equal the names `snapshot::save`
    actually writes. The test saves twice through `save` and lists the
    directory, then makes a save fail before its publish rename, which
    leaves the temp file under the store's name;
  - (added) a path naming no file is refused; the same file spelled
    through `..` or a symlink is still the journal; the same file name in
    another directory, or with the journal's directory not yet created,
    is accepted.
- `archive_local_lineage`:
  - default layout: one directory holding the archives, the live
    segment, the snapshot and `.prev` (the existing test, extended with
    `.prev`);
  - snapshot in another directory on the same filesystem: the snapshot
    and `.prev` land in `<snapdir>/<snapshot-name>.<reason>.0/`, and
    nothing of the snapshot is in the journal's archive;
  - `n` free in one directory and taken in the other: both use the next
    index free in both;
  - two nodes whose journals share a file name in different directories,
    with one shared snapshot directory: each node's snapshot archive is
    under its own snapshot name, and neither journal side skips an index;
  - only a snapshot, or only a journal: one directory is returned and
    the other side is `None`, and `n` still skips an index taken on the
    empty side (a snapshot-only `.0` then a journal-only archive gives
    `.1`);
  - across two filesystems: the journal in a `tempfile::tempdir()` (as
    the existing `snapshot.rs` tests; `CARGO_TARGET_TMPDIR` is set only
    for integration tests), and the snapshot under `/dev/shm` (tmpfs).
    A tmpfs `/tmp`, as on Fedora, is still a different device from
    `/dev/shm`, so the case stays meaningful there. This is the
    reproduction, since it fails with EXDEV on today's code. A helper,
    `second_filesystem_dir(first: &Path)`, provides the directory and
    does the `st_dev` comparison itself: a candidate on the same device
    as `first` (say, `/dev/shm` for a developer building under tmpfs)
    counts as "no second filesystem", never as a bare assertion failure.
    It lives in a new
    `transport-core/src/test_fs.rs` under
    `#[cfg(any(test, feature = "test-utils"))]`, beside `test_ports`, so
    the unit test here and server-runtime's integration test (which
    already dev-depends on transport-core with `test-utils`) share one
    copy:
    - it honours `MELIN_TEST_SECOND_FS` (another mount) before
      `/dev/shm`, and takes the first candidate whose `st_dev` differs
      from `first`'s;
    - when no candidate qualifies, it **fails under `CI`**, so CI
      cannot silently lose the case;
    - elsewhere it prints why and returns. This is the only test allowed
      to do that, and the same-filesystem case above still pins where
      the snapshot goes.

  GitHub's runners have `/dev/shm` on tmpfs and `/tmp` and the target
  directory on ext4, so the test runs in every CI job.
- `catchup`, with the snapshot only at an explicit path in another
  directory:
  - the preflight passes and the transfer serves that snapshot;
  - with a stale file also at `<journal>.snapshot`, the explicit one is
    served (compare the header's sequence).
- `ServerConfig` parsing, through `ServerConfig::try_parse_from`:
  `--snapshot` is rejected as unknown, and `--snapshot-path` reaches
  `store()`.

**Integration (server-runtime, kernel TCP through `melin-test-node`, new
`tests/snapshot_location.rs`):**

- A primary with `--snapshot-path` in another directory, beside a stale
  `<journal>.snapshot` holding other state, recovers from its own
  snapshot on restart. The test checks the recovered state, which differs
  from the stale file's, so no log capture is needed. As built: the stale
  file is placed after the primary's first run (before it, the node has
  no journal and the third refusal applies), and the primary is
  restarted twice, with its journal and then with the snapshot alone.
- A divergent replica resyncs from a primary that serves its snapshot
  from its own `--snapshot-path`. The replica's `--snapshot-path` is on
  the second filesystem (`melin_transport_core::test_fs`, so the same CI
  rule). It ends acking the primary's whole history, and its old snapshot
  (checked by header) sits in `<second fs>/…/<snapshot-name>.divergent.0/`
  with its old journal in `<journal-name>.divergent.0/`. As built: each
  node first writes its history standalone and is then restarted in its
  role, because a primary that begins a history waits for a replica
  before it serves, and the replica must find a snapshot to be served.
- Added while building: a snapshot directory that does not exist, and a
  first `--snapshot-path` on the upgrade layout, are refused at start,
  naming the paths, with no journal created.

The file runs on DPDK too, under the netns runner with the `dpdk`
feature, like the crate's other cluster tests, and joins the nextest
`cluster-serial` group.

**DPDK (netns runner):** extend `large_joins_complete_a_tick_at_a_time`
so that the primary and the divergent replica both take `--snapshot-path`
in a directory of their own. The join then serves the snapshot from
there (and no `<journal>.snapshot` is written), and the divergent
replica's archive lands there. As built, the divergent replica is given
an old snapshot of its one-event history (`JournaledApp::save_snapshot`)
so there is a snapshot to archive.

**Coverage:** run `cargo llvm-cov` on `transport-core` and
`server-runtime`, and cover every changed branch it reports as
unexercised.

### 0e. CHANGELOG

- **Added:** `NodeStore`, the one resolver of a node's journal and
  snapshot paths, and `ServerConfig::store()`. As built, also
  `melin_journal::segment::parent_dir` and `fsync_dir` (0a), and under
  **Fixed** the received snapshot's directory sync (0b).
- **Removed (breaking):**
  - `--snapshot`. A command line with `--snapshot X` is refused.
    - If `--snapshot-path` is already set, drop `--snapshot` and leave
      the file at `--snapshot-path` alone: it is the freshest snapshot,
      and a `<journal>.snapshot`, if any, is a stale leftover. (With
      `--snapshot X --snapshot-path Y`, X was a one-off restore.)
    - With `--snapshot X` alone, the shadow kept writing the derived
      `<journal>.snapshot`, so that file, not X, is the freshest
      snapshot. If `--snapshot` was a one-off restore, drop it: the
      derived path is already the live location. To keep a custom
      location, move the current `<journal>.snapshot` there first, then
      set `--snapshot-path`.
    - To restore from a backup, copy it to the snapshot path.
  - `ServerConfig::snapshot` and `ServerConfig::shadow_snapshot_path`;
    use `ServerConfig::store()`.
- **Changed (breaking):**
  - `--snapshot-path` is the node's one snapshot location: read at boot
    by a primary and a replica, served to replicas, written by the
    shadow.
  - The library: `archive_local_lineage`, `preflight_snapshot_transfer`
    and `snapshot_transfer_with` take a `NodeStore`.
    `archive_local_lineage` returns both archive directories.
  - A resync now archives `<snapshot>.prev` with the snapshot. Before,
    the old lineage's `.prev` stayed beside the new lineage's snapshot,
    where `mv <snapshot>.prev <snapshot>` would have restored a snapshot
    of another history; it is now in the resync archive.
  - A node refuses to start when its snapshot's directory does not exist,
    when the snapshot path names a directory, when the snapshot path is
    the journal path, or when `--snapshot-path`
    names nothing while a derived `<journal>.snapshot` exists and there is
    no journal.
- **Fixed:**
  - A primary with `--snapshot-path` now recovers from, and serves, its
    own snapshot. Before, it ignored that snapshot and could serve a stale
    `<journal>.snapshot`.
  - A replica whose `--snapshot-path` is on another filesystem than its
    journal no longer fails every resync with EXDEV. The snapshot is
    archived beside itself.
  - A re-seeded replica's temporary snapshot is now `<snapshot>.tmp`, the
    shadow's own name.

### 0f. Docs

- `docs/journal-rotation.md`:
  - replace the `--snapshot` row of the flags table with
    `--snapshot-path`;
  - in "Operational Notes", replace the non-existent copy fallback: the
    snapshot may be on another filesystem than the journal, and its
    `.tmp`, its `.prev` and its resync archive stay on the snapshot's own
    filesystem, so every rename is atomic.
- `docs/journal.md`:
  - in "When to Snapshot", recommend a snapshot disk separate from the
    journal's for latency-critical deployments (see "Docs" below for the
    wording);
  - in "Standard Upgrade", the snapshot is the one at `--snapshot-path`,
    and restoring from a backup means copying it there;
  - in "Limitations", state what step 0 leaves of determinism-audit
    findings 21 and 22, as the roadmap asks of open findings: a snapshot
    served to a replica is checked only by its header, a resync's archive
    is not crash-atomic, and a mistyped `--snapshot-path` on a node with
    no journal and nothing at either path is not caught.
- `docs/replication.md`, the archive paragraph: with `--snapshot-path` in
  another directory, the snapshot is archived beside it in a directory
  named after the snapshot file with the same reason and index as the
  journal's archive; in every layout its `.prev` goes with it, so the
  `.prev` rollback applies within a lineage only.

## Design decisions

### 1. Snapshots on by default in the suite, with one interval knob

New environment variable `SNAPSHOT_INTERVAL_MS`, default `10000`. A
value of `0` turns the shadow off (the server's own meaning of
`--snapshot-interval-ms 0`), as a way to isolate a regression. It is not
a second mode with its own reporting.

**Recommendation: on by default.** An operator who benchmarks the
sequencer wants figures for what they will deploy, and production runs
the shadow. Opt-in would leave the suite's headline figures describing a
configuration with no snapshots, which no operator runs.

**Why 10 s.** At that interval every measured phase at the default
durations (30 s single, 60 s throughput) contains several saves, landing
at different points of the load. The suite refuses intervals below
1000 ms: back-to-back saves of a large state would measure a
configuration nobody runs. It also refuses an interval longer than the
shortest measured phase requested, since no save would then be judged.
The interval is far denser than the 50-minute production default. That
is deliberate: a save costs the same at either interval, and density only
raises the chance that one lands inside the measured window. The bench
therefore gives an upper bound on how much snapshots interfere.

### 2. Snapshots go to a directory on the OS disk, named per node

New environment variable `SNAPSHOT_DIR`, default
`/var/lib/melin-bench/snapshots`. The default is on the root filesystem
on every bench host. `/tmp` is ruled out because it is tmpfs on current
Debian, and a snapshot written there never reaches a disk.

Each node writes `${SNAPSHOT_DIR}/<node>.snapshot`, where `<node>` is
`primary`, `replica1` or `replica2`, the names its journal already has.
The suite passes the path itself, so the path is unique per node even
though `REPLICA_EXTRA_ARGS` is shared by both replicas, and even on a rig
where two nodes share a filesystem. The flags come from one helper (see
"Files and functions"), not from two spelled-out launch lines.

`SERVER_EXTRA_ARGS` and `REPLICA_EXTRA_ARGS` must not contain
`--snapshot-interval-ms`, `--snapshot-path` or `--snapshot`. The suite
refuses them at start-up and names the environment variable to use
instead. `--snapshot` no longer exists after step 0; it stays on the
list so that a stale setting fails with a message that names it.
Otherwise clap would reject the duplicate flag on the host, and the
failure would only show as a node that never logged its ready line.

### 3. The disk guard: fail closed

Before the first run, the suite creates `SNAPSHOT_DIR` on each node host
and resolves both directories to their disks. For each of `SNAPSHOT_DIR`
and `JOURNAL_DIR` it runs `findmnt -nv -o SOURCE,FSTYPE -T <dir>`, then
`lsblk -rnso NAME,TYPE <source>`, which lists the device and its
ancestors down to the whole disks, through partitions, LVM and md.
`-v` (`--nofsroot`) drops a btrfs subvolume suffix (`/dev/sda2[/@]`),
which `lsblk` would reject and the guard would then refuse a sound host
over. `-r` prints raw lines rather than a tree, so the fixtures, taken
from real output, carry no box-drawing prefixes for `shared_disks` to
strip. The run is refused if:

- the snapshot filesystem is `tmpfs` or `ramfs`;
- the two directories' sets of `disk` ancestors intersect, for example
  a partition and the disk it lives on, or two LVs on one PV;
- either directory cannot be resolved to a disk, for example on an
  overlay filesystem in a container. Fail closed: a run that cannot
  prove the separation must not claim it.

`SNAPSHOT_SHARED_DISK_OK=1` turns the refusal into a printed warning.
It exists for the container rehearsal (`test-containers.sh` sets it),
and is documented as for rehearsals only. A host without a dedicated
journal drive (`server-setup.sh` found none, so `/mnt/journal` is on the
root disk) is refused by the guard. That is correct, because on such a
host the separation this plan is about cannot exist.

With `SNAPSHOT_INTERVAL_MS=0` the guard does not run, and neither does
the judge (step 5) or the `RUST_LOG` addition. With snapshots off there
is no separation to prove, so a host without a dedicated journal drive
can still run the suite.

### 4. Cleanup before every run, by exact name

`clean_journal` becomes `clean_node_state <host> <node>`. It removes:

- the journal lineage, as today: `<node>.journal`, `<node>.journal.*`.
  `rm -rf` replaces `rm -f`, because a resync archive
  (`<node>.journal.divergent.<n>/`) is a directory that `rm -f` refuses,
  silently today, since the call's status is lost inside `if !
  start_cluster`;
- the derived snapshot next to the journal, `<node>.snapshot` and
  `<node>.snapshot.*`. After step 0 no node reads it while
  `--snapshot-path` is given. It is removed anyway, because suite versions
  before this plan wrote it there;
- the node's files in `SNAPSHOT_DIR`: `<node>.snapshot` and
  `<node>.snapshot.*`, which covers `.prev` and `.tmp` (after step 0 the
  shadow's temp file and a re-seeded replica's are both
  `<node>.snapshot.tmp`), and the snapshot half of a resync archive
  (step 0b), `<node>.snapshot.<reason>.<n>/`, which needs `rm -rf`.

It runs at the start of every `start_cluster`, before any node is
launched. Because of that, leftovers from an aborted earlier run (a
`Ctrl-C`, a host reboot, a crashed suite) are removed before they can be
read, with no separate end-of-suite sweep. The last run's files are left
in place for post-mortem, as journals are today. Only exact names are
removed, never `SNAPSHOT_DIR/*`, so an operator who points the directory
at a shared location loses nothing else. A failed cleanup now fails the
run's start (`|| return 1`). Before any `rm`, the suite checks that
`SNAPSHOT_DIR` and `JOURNAL_DIR` are absolute and non-empty, so a broken
expansion cannot turn into `rm -rf /<node>...`.

Files from an aborted run that used a different `SNAPSHOT_DIR` are left
behind. They are harmless, because no node reads a path it was not
given, but they take space. The docs say so.

### 5. Proving each run snapshotted, and where

A run passes only if every node saved snapshots during the load. This
needs no new metric:

- **The window.** The health sampler's regex gains
  `melin_journal_sequence`. The first and last sampled values bound the
  journal sequences journaled while the load ran. Both primary and replica
  saves log the wire sequence, so one window serves all three nodes.
- **The count.** After `save_node_logs`, each node's log is judged. The
  judge counts "shadow snapshot saved" lines whose `journal_seq` lies in
  the window, `first < journal_seq <= last`, and whose `path` is the
  node's expected path. The lower bound is exclusive: journaled ticks
  cross the ring before the load starts, so an idle save taken before
  the load can carry exactly the first sampled sequence, and it must not
  count. It expects at least `max(1, floor(measured_ms / interval) - 1)`.
  Using the measured duration, not the whole load window, absorbs phase.
  Any "shadow snapshot failed" line, or a save to any other path, fails
  the run. A failure is recorded in `DEGRADED_RUNS` alongside the health
  verdict, so the suite exits non-zero.
- **The log filter.** When snapshots are on, the suite appends
  `,melin_transport_core::shadow=info` to the nodes' `RUST_LOG`, so that
  a `RUST_LOG=warn` run cannot hide the lines it is judged on.
- **When the judge does not run.** It is skipped when snapshots are off.
  It is also skipped when `run_workload` returned early because the load
  generator failed. That run has already failed, and with no measured
  duration there is no expected count to judge against.
- **The device.** `/proc/diskstats` is read on each node host before
  and after each run. The delta in sectors written to the snapshot disk
  and to the journal disk goes into the run's results. This confirms
  directly that the snapshot bytes reached the OS disk. When the guard
  (section 3) could not resolve either disk on a host, which is always
  the case in the container rehearsal (overlay, with
  `SNAPSHOT_SHARED_DISK_OK=1`), `diskstat_written` is not called for that
  host and the delta is recorded as `-`. The call is guarded explicitly,
  so the suite's `set -e` cannot abort a run on it, and the rehearsal
  covers that branch.
- **The cost.** The runtime reports each save's size and duration (step
  1). The judge reports the largest per node, which is the figure to
  compare against the input ring's headroom.

### 6. The runtime makes a save prompt, visible and bounded

Three small runtime changes make the benchmark mean what it claims:

- **A deferred save is retried, not dropped.** `shadow::run` keeps a
  `due` flag. A timer tick sets it. While it is set, every consumed batch
  (and every idle pass) attempts the save, and the flag clears when the
  save succeeds. When a save *fails* (I/O error), the flag also clears,
  and the next attempt is a full interval later, so a persistent `EIO`
  does not become a hot loop of `error!` lines. The decision lives in a
  small pure type (`SnapshotSchedule`) that can be unit-tested
  without threads or clocks. A tick that finds the previous save still
  due logs a `warn!` ("shadow snapshot deferred for a whole interval",
  with its attempts so far), at most once an interval, so a node that
  cannot align is visible before the judge reads its log. The check made
  on every iteration while a save is due is the `FsyncState` seqlock read
  (lock-free; an L1 hit until the journal publishes again) and one
  comparison; the save itself is a `#[cold]` out-of-line function.

  The retry assumes an aligned moment occurs under sustained load.
  Alignment needs the fsync boundary (`FsyncState.input_ring_seq`) to
  equal the shadow's next read. The shadow is gated behind the journal
  stage's consumed position, which runs ahead of the fsync boundary
  while requests are in flight, so the two coincide only when the ring
  has drained and the last batch's fsync is published. Under the
  closed-loop load generator that happens at the ack cadence, so the
  design is expected to hold. If a workload rarely aligns, the judge
  (section 5) fails the run: that is a product finding the bench exists
  to surface (an operator under that load would get no snapshots), not
  a reason to loosen the judge's minimum. The fix would then be a
  follow-up that captures the snapshot cursor at the fsync boundary
  itself.
- **The saved line says what it cost.** `snapshot::save` returns the
  bytes it wrote. The log line becomes "shadow snapshot saved
  journal_seq= bytes= elapsed_ms= attempts= path=", where `attempts`
  counts every attempt at the save, the one that landed included (1 when
  the first attempt saved), and `elapsed_ms` is fractional (a small
  state saves in well under a millisecond). The "shadow snapshot failed"
  line carries `attempts` too. The shadow calls `snapshot::save`
  directly, so the size flows from there to `try_save_snapshot` and no
  further; `JournaledApp::save_snapshot`, whose callers are all tests,
  is left alone.
- **The snapshot's dirty footprint is bounded.** `save_with_limit`
  writes the buffer in fixed chunks (4 MiB, a constant with a comment
  justifying it). After each chunk it calls `sync_file_range` with
  `SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WRITE |
  SYNC_FILE_RANGE_WAIT_AFTER` on the previous chunk, so that at most two
  chunks are dirty at once. The final `sync_all` still provides
  durability. `sync_file_range` alone does not flush metadata or the
  device cache, so it does not replace that call. The snapshot can then
  no longer push global dirty memory past the freerun ceiling under
  `server-setup.sh`'s caps, or under an operator's. `libc` is already a
  dependency of `transport-core`.

Alternatives considered for the dirty-page coupling:

- **A per-BDI limit on the OS disk** (`strict_limit` plus `max_bytes`
  under `/sys/class/bdi/`, kernel 6.2 and later) in `server-setup.sh`.
  It would also cover other OS-disk writers, but it protects only hosts
  set up by our script. An operator would get figures that their own
  hosts do not reproduce. Not adopted. It remains a possible host-level
  defense in depth later.
- **`O_DIRECT` for the snapshot file.** This needs an aligned buffer
  and a padded tail, and changes the on-disk write pattern for every
  user. It is more invasive than chunked writeback for the same bound.
- **Raising `vm.dirty_bytes`.** This moves the threshold rather than
  removing the coupling, and brings back the large writeback storms the
  cap exists to prevent.

### 7. What a run records

- `${RESULTS_DIR}/suite.conf`: every setting the suite ran with, one
  `KEY=value` per line. That includes `SNAPSHOT_INTERVAL_MS`,
  `SNAPSHOT_DIR`, `SNAPSHOT_SHARED_DISK_OK`, `ACK_POLICY`, the durations,
  the extra arguments, the git revision, and per host the journal and
  snapshot disks the guard resolved. One function prints the settings
  both to the console header and to this file, so the two cannot drift.
- `<transport>-<workload>.snapshots`: the judge's verdict per node
  (saves in the window, largest `elapsed_ms` and `bytes`), followed by
  the diskstats deltas.
- The summary table gains a column with the fewest in-window saves on
  any node and the largest save time. A run with snapshots off shows `-`.

## Order of work

Each step is one commit, reviewed before the next.

Steps 0, 1 and 2 change the runtime, step 0 breaks the CLI and the
library, and step 3 adds `test-lan-bench-suite.sh` to `pre-merge.yml`.
These go beyond the bench suite the request named. The user confirms
them before step 0 begins, rather than the implementation proceeding on
an assumed go-ahead.

0. **Runtime: one snapshot location for every role** (`fix(snapshot)!`).
   This covers `NodeStore`, the removal of `--snapshot`, the archive on
   the snapshot's own filesystem, their tests, the docs listed in "0f",
   and the audit-doc status. CHANGELOG entries as in "0e". This step is
   its own commit, reviewed before step 1 starts. Steps 1 to 4 do not
   depend on its code, but the benchmark's claim to measure a production
   layout does.
1. **Runtime: prompt, visible saves** (`fix(shadow)`). Covers
   `SnapshotSchedule`, the retry, the new fields on the saved line, and
   `save` returning its size. CHANGELOG entry under **Fixed** ("a
   snapshot deferred under load is retried, not skipped for an
   interval"), and one under **Changed** for `snapshot::save`'s new
   return type (it is public API).
2. **Runtime: bounded snapshot writeback** (`perf(snapshot)`).
   CHANGELOG entry under **Changed**.
3. **Suite: host-free logic and its tests** (`feat(bench)`). The new
   `lib.sh` helpers and their cases in `test-lan-bench-suite.sh`.
4. **Suite: wiring, rehearsal and docs** (`feat(bench)`). Changes to
   `lan-bench-suite.sh`, `test-containers.sh`, `server-setup.sh`'s
   comment, and `docs/benchmarking.md`.

## Files and functions

Step 0's files are listed in "0c". `shadow::run`'s signature is
untouched by step 0; only its caller changes, passing
`store.snapshot()` instead of `shadow_snapshot_path()`.

**`crates/core/transport-core/src/shadow.rs`**

- `run`: replace the single-shot tick with `SnapshotSchedule`
  (`on_tick`, which returns the attempts so far when the previous save
  is still due, `should_attempt`, `on_deferred`, `on_saved`,
  `on_failed`), and count attempts. The tick and the due-save attempt
  are one block after the (possibly empty) batch, shared by busy and
  idle passes, rather than one copy in each branch.
- `try_save_snapshot`: return an outcome enum (`Saved { journal_seq,
  bytes, elapsed }`, `Deferred`, `Failed { journal_seq, error }`). Use an
  exhaustive `match` at the call site (`attempt_save`), so that a fourth
  outcome cannot fall through.

**`crates/core/transport-core/src/snapshot.rs`**

- `save` and `save_with_limit`: return the byte count, and write in
  chunks with `sync_file_range` on the previous chunk.
- Make the chunk size a parameter of `save_with_limit`, as the size cap
  already is, so that tests can exercise it with small payloads.

**`scripts/bench/lib.sh`** (host-free, tested)

- `duration_ms <d>`: accepts `<n>ms`, `<n>s`, `<n>m`, the forms the
  suite's defaults use, and fails on anything else. The suite refuses a
  duration it cannot parse while snapshots are on.
- `snapshot_settings_check <interval_ms> <min measured ms>`: the 0,
  floor and ceiling rules from decision 1.
- `extra_args_conflict <args>`: names the first snapshot flag found.
- `shared_disks <lsblk A> <lsblk B>`: prints the intersecting `disk`
  names, or fails with "unresolved" when either side has none.
- `seq_window <health samples>`: the first and last
  `melin_journal_sequence`.
- `judge_snapshots <log> <first> <last> <expected> <path>`: prints the
  verdict line and fails unless it is `ok`, in the style of
  `judge_health`.
- `diskstat_written <before> <after> <dev>`: bytes written between the
  two reads.

**`scripts/bench/lan-bench-suite.sh`**

- Header: document `SNAPSHOT_INTERVAL_MS`, `SNAPSHOT_DIR` and
  `SNAPSHOT_SHARED_DISK_OK`, and add a paragraph to the closing
  description: snapshots are on by default and judged per run.
- Settings: validation of the new variables and the extra-args
  conflict; the `RUST_LOG` addition; `suite_settings` for the header
  and `suite.conf`.
- `node_journal <node>`, `node_snapshot <node>`, and `node_store_flags
  <node>` (which prints `--journal ... --snapshot-path ...
  --snapshot-interval-ms ...`). These replace the two hand-spelled
  journal paths in `start_cluster` and the separate `clean_journal`
  arguments.
- `clean_journal` becomes `clean_node_state` (decision 4).
- `check_snapshot_disks`: once per suite, after deploy (decision 3).
- `run_workload`: add `melin_journal_sequence` to the sampler regex and
  read diskstats around the load. The snapshot verdict moves to a
  `judge_run` called after `save_node_logs` in the main loop, because
  the replicas' logs exist only then.
- Summary: the new column.

**`scripts/bench/test-containers.sh`**

- Set `SNAPSHOT_SHARED_DISK_OK=1`. Use a short interval (`2000`, below
  the rig's 5 s phases) so that the rehearsal judges at least one save
  per node. Leave `SNAPSHOT_DIR` at its default, which exercises the
  `mkdir` and the cleanup in the containers.

**`scripts/bench/server-setup.sh`**

- The dirty-page comment (the "co-tenants — log rotation, snapshot
  writes" paragraph): state that snapshot writeback is now bounded by the
  runtime, and that the snapshot directory lives on the OS disk. Also
  rewrite its premise: "The journal path itself bypasses this (RWF_DSYNC
  syncs immediately)" is wrong. The journal writes through the page cache
  and then calls `fdatasync`, so it is exposed to `balance_dirty_pages`
  like any other writer (see "Global dirty-page accounting" above). No tuning
  change.

## Tests

**`test-lan-bench-suite.sh`** (unit, runs anywhere), one section per
helper:

- `judge_snapshots`:
  - enough in-window saves passes;
  - none fails with "no snapshot";
  - fewer than expected fails;
  - saves outside the window are not counted (before load: idle-period
    saves driven by ticks; after: cooldown);
  - the bounds: a save at exactly `first` is not counted, one at exactly
    `last` is;
  - one "failed" line fails even with enough saves;
  - a save to another path fails;
  - log lines with fields in another order, or with ANSI-free
    timestamps, still parse;
  - an empty log fails;
  - interval `0` yields `off` and passes.
- `seq_window`: normal samples; samples where the endpoint did not
  answer; no sequence at all, which is unjudged.
- `shared_disks`:
  - two disks pass;
  - a partition against its own disk is shared;
  - an LV whose PVs span both disks is shared;
  - an empty (overlay) side is unresolved.
- `duration_ms`: each accepted form; refused forms (`1h30m`, `60`,
  empty).
- `snapshot_settings_check`: `0`; below the floor; above the shortest
  phase; the boundary values themselves.
- `extra_args_conflict`: each of the three flags; a flag that merely
  shares a prefix (`--snapshot-interval-msx`) is not matched;
  `--snapshot=x` forms.
- `diskstat_written`: a device present in both reads; a device missing
  from one, which fails rather than reporting 0.

**Runtime unit tests:**

- `SnapshotSchedule`, exhaustively:
  - tick then a deferred attempt keeps the save due;
  - a deferred attempt followed by an aligned one saves;
  - a failed save clears the due flag until the next tick;
  - no attempt is made before the first tick;
  - `attempts` resets after a save;
  - a tick that finds the save still due reports it, and the count runs
    on across the tick.
- A threaded `shadow.rs` test, its log captured: `FsyncState` is
  misaligned at the first tick and aligned shortly after. With a 4 s
  interval, the snapshot must exist before the second tick could fire,
  and its saved line must count more than one attempt. The count is what
  keeps scheduling noise from passing the test by accident: a first tick
  late enough to land after the alignment would save at once, with one
  attempt.
- Two more threaded tests with captured logs: a save misaligned for
  more than an interval warns at most once a tick and still lands; a
  save to a missing directory fails at most once a tick, each with one
  attempt.
- `snapshot.rs`, independent of chunking: the size `save` returns equals
  the file's length (on a first save and on a rotating one), and a save
  of exactly the size cap is written while one byte under it is refused.
- `snapshot.rs`, with the chunk size made small:
  - a payload of exactly one chunk;
  - a payload of one chunk plus one byte;
  - a payload of several chunks;
  - an empty application payload.
  Each round-trips through `load`, checks the returned size equals the
  file's length, and keeps the `.prev` rotation.

**Container rehearsal** (`test-containers.sh`): runs the whole path
end to end on kernel TCP. It covers the new flags reaching the nodes,
the directory being created, the cleanup, the sampler window, and the
judge against real node logs. The guard runs too, but there it can only
take the opt-out branch. Run it for `echo` and `notary`, and for `ec`
when a checkout is at hand.

Step 0's tests are listed in "0d".

**Coverage.** Run `cargo llvm-cov` on `transport-core` for steps 1
and 2 (step 0: see "0d"), and cover every changed branch it reports
unexercised. The shell helpers are covered case by case above.

`test-lan-bench-suite.sh` is not run in CI. Add it to `pre-merge.yml`
(it needs bash and awk only) in step 3, so that the judge's tests
cannot rot.

## Docs

- **`docs/benchmarking.md`** (operator-facing). Under "Running it":
  snapshots are on by default every `SNAPSHOT_INTERVAL_MS` and written to
  `SNAPSHOT_DIR` on the OS disk, the suite refuses a directory on the
  journal's disk, and why (the figures reflect a production
  configuration whose snapshot I/O is kept off the journal device).
  Under "Hosts": the root filesystem must be on a disk other than the
  journal drive. Under "Results": the `.snapshots` file, `suite.conf`,
  and the rule that a run with too few snapshots is not a passing run.
  Under "Rehearsing": the rig opts out of the disk guard. Say that the
  suite's layout is the one `docs/journal.md` recommends for production.
- **`docs/journal.md`** (operator-facing). Under "When to Snapshot":
  - the shadow retries a save deferred under load until it lands;
  - the snapshot's writeback is bounded, so that it cannot drive the
    host's dirty-page limits (steps 1 and 2);
  - with step 0, recommend `--snapshot-path` on a disk other than the
    journal's for latency-critical deployments. Every role reads,
    serves, writes and archives the snapshot there, and every rename
    stays on that filesystem. The recommendation is scoped to what step
    0 fixes. Under "Limitations", state that a served snapshot is
    checked only by its header, that a resync archive is not
    crash-atomic, and that a node with no journal and nothing at its
    snapshot path boots a new history, so a mistyped path is not caught
    (the open halves of findings 22 and 21). All three hold
    wherever the snapshot lives, so none is a reason to keep the
    snapshot beside the journal.
- **`docs/journal-rotation.md`** (operator-facing): the flags-table row
  and the corrected "Operational Notes" listed in step 0 ("0f"). The
  claim of a copy fallback goes; there never was one.
- **`docs/internal/roadmap.md`**: under the audit item's "Done so far",
  add "the snapshot location in every role (the location halves of
  findings 21 and 22)", and note that the rest of both is the follow-up
  under "Not part of this item".
- **`CHANGELOG.md`**: step 0's entries ("0e"), the two runtime entries
  above, and a bench-suite line under **Changed** (snapshots on by
  default, on the OS disk).

## Left untested, and why

- **The DPDK transport with snapshots.** The shadow is
  transport-agnostic and the flags are the same, but the rehearsal is
  kernel-only (no NIC). It is covered only by running the suite on the
  bench hosts with `TRANSPORTS=dpdk`.
- **The disk guard's refusing and passing branches against real
  hardware.** Containers resolve to overlay, so only the opt-out branch
  runs there. The decision logic is unit-tested on `lsblk` fixtures; the
  `findmnt`/`lsblk` invocation itself first runs on the bench hosts.
- **That chunked writeback keeps the journal thread out of
  `balance_dirty_pages`.** This is a kernel behaviour that no unit test
  can observe. The unit tests prove the chunking is correct. The evidence
  that it works is the bench itself: the Exchange Core at its default
  account count, with saves logged as tens of MiB, showing no p99.9 or
  max excursion attributable to a save. Confirming that from a run means
  matching the save timestamps in the node logs against the load
  generator's output; this plan does not automate that correlation.
- **Step 0 in the benchmark itself.** Runs never restart or resync, so
  the suite never takes the boot, serving or archive paths that step 0
  fixes. Those are covered by step 0's unit, integration and netns tests
  ("0d"), not by the bench.
- **The cross-filesystem archive on a host with no second filesystem.**
  Off CI, the test returns early with a printed reason. Under `CI` it
  fails instead, and the same-filesystem case still pins where the
  snapshot goes.
- **A crash between the journal's and the snapshot's renames.** This is
  the open half of finding 21, and its test belongs to the follow-up's
  intent marker.

## Not part of this item

- A snapshot metric on `/metrics` (`melin_snapshots_saved_total`, last
  saved sequence, last save duration). Operators would want it for
  alerting on a stale snapshot, and it would let the suite judge from
  the sampler instead of logs. It is a separate product change; log
  lines suffice for the benchmark.
- **The rest of determinism-audit findings 21 and 22.** This is a
  follow-up on its own branch, built on step 0's `NodeStore`:
  - validation of a served snapshot as strict as `snapshot::load` (CRC,
    `app_version`) before the verdict frame is sent;
  - a durable intent marker that makes the resync archive crash-atomic,
    completed at boot;
  - archiving a lone (orphan) snapshot at replica recovery;
  - an explicit "this boot continues a history" intent, so that a
    mistyped `--snapshot-path` on the snapshot-only upgrade layout is
    refused instead of booting a new history (see "0a").
- Any comparison mode between snapshot locations.
