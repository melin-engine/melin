# Continuous snapshots in the LAN benchmark, on the OS disk (plan)

Status: **planned** (2026-10).

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
  on the OS disk.
- **Nothing else that a snapshot produces writes to the journal disk.**
  The segment seed (`<journal>.seed.tmp`) is journal data and belongs
  there. Rotation archives segments and never touches snapshots
  (`docs/journal-rotation.md`). Catch-up serving only *reads* a snapshot.
- **`--snapshot-path` is honoured by the writer alone.** Determinism-audit
  findings 21 and 22 are still open. Primary boot (`init_engine`, which
  reads `--snapshot` or `<journal>.snapshot`) and snapshot transfer
  (`catchup::preflight_snapshot_transfer` and
  `catchup::snapshot_transfer_with`, both hard-coded to
  `<journal>.snapshot`) never look at `--snapshot-path`. The resync
  archive (`archive_local_lineage`) renames the snapshot into the journal
  directory, which fails with EXDEV when the two are on different
  filesystems. None of this affects a benchmark run: every run starts
  fresh clusters on empty journals, with no restart and no resync.
  It does mean the docs must not yet tell operators to put snapshots on
  another disk (see "Docs").
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
instead. Otherwise clap would reject the duplicate flag on the host, and
the failure would only show as a node that never logged its ready line.

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
  `<node>.snapshot.*`. Primary boot and catch-up read this file (findings
  21 and 22), so a stale one would bootstrap the next run's primary from
  the old state;
- the node's files in `SNAPSHOT_DIR`: `<node>.snapshot` and
  `<node>.snapshot.*`, which covers `.prev` and `.tmp` (the shadow's
  temp file and a re-seeded replica's share the name
  `<node>.snapshot.tmp`).

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
  small pure type (for example `SnapshotSchedule`) that can be unit-tested
  without threads or clocks.

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
  counts the deferrals before the save. The shadow calls `snapshot::save`
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

Steps 1 and 2 change the runtime, and step 3 adds
`test-lan-bench-suite.sh` to `pre-merge.yml`. These go beyond the bench
suite the request named; the user confirms them before step 1 begins,
rather than the implementation proceeding on an assumed go-ahead.

1. **Runtime: prompt, visible saves** (`fix(shadow)`). Covers
   `SnapshotSchedule`, the retry, the new fields on the saved line, and
   `save` returning its size. CHANGELOG entry under **Fixed** ("a
   snapshot deferred under load is retried, not skipped for an
   interval").
2. **Runtime: bounded snapshot writeback** (`perf(snapshot)`).
   CHANGELOG entry under **Changed**.
3. **Suite: host-free logic and its tests** (`feat(bench)`). The new
   `lib.sh` helpers and their cases in `test-lan-bench-suite.sh`.
4. **Suite: wiring, rehearsal and docs** (`feat(bench)`). Changes to
   `lan-bench-suite.sh`, `test-containers.sh`, `server-setup.sh`'s
   comment, and `docs/benchmarking.md`.

## Files and functions

**`crates/core/transport-core/src/shadow.rs`**

- `run`: replace the single-shot tick with `SnapshotSchedule`
  (`on_tick`, `should_attempt`, `on_saved`, `on_failed`), and count
  attempts.
- `try_save_snapshot`: return an outcome enum (`Saved { bytes, elapsed }`,
  `Deferred`, `Failed`). Use an exhaustive `match` at the call site, so
  that a fourth outcome cannot fall through.

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
  - `attempts` resets after a save.
- A threaded `shadow.rs` test: `FsyncState` is misaligned at the first
  tick and aligned shortly after. With a 5 s interval and a deadline
  well under it, the snapshot exists before the second interval. Under
  the old behaviour it could not appear before the second tick, so
  scheduling noise cannot make the test pass by accident.
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

**Coverage.** Run `cargo llvm-cov` on `transport-core` for steps 1
and 2, and cover every changed branch it reports unexercised. The shell
helpers are covered case by case above.

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
  Under "Rehearsing": the rig opts out of the disk guard.
- **`docs/journal.md`** (operator-facing). Under "When to Snapshot":
  the shadow retries a save deferred under load until it lands, and the
  snapshot's writeback is bounded so that it cannot drive the host's
  dirty-page limits. Do **not** yet advise putting `--snapshot-path` on
  another disk in production: until findings 21 and 22 are fixed, a
  primary neither recovers from nor serves a snapshot at that path, and a
  replica's resync fails across filesystems. `--snapshot-path` is not
  documented under `docs/` today. Add it to the flags table in
  `docs/journal-rotation.md` ("Configuration"), and state the limitation
  in `docs/journal.md` under "Limitations".
- **`docs/journal-rotation.md`** (operator-facing). Its "Operational
  Notes" claim that a cross-filesystem snapshot path "falls back to a
  copy that is not crash-safe". No such fallback exists: the rename
  fails with EXDEV (see "`--snapshot-path` is honoured by the writer
  alone" above).
  Correct it to say that the path must be on the journal's filesystem
  until findings 21 and 22 are fixed.
- **`docs/internal/roadmap.md`**: under the audit item, note that
  findings 21 and 22 block recommending a separate snapshot disk to
  operators. Once they are fixed, the benchmark's layout is the one to
  document as recommended.
- **`CHANGELOG.md`**: the two runtime entries above, and a bench-suite
  line under **Changed** (snapshots on by default, on the OS disk).

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
- **Findings 21 and 22** (recovery and serving from `--snapshot-path`,
  and EXDEV on resync). These are out of scope, and runs never take
  those paths because clusters are fresh and never restart. They gate
  operator guidance, not the benchmark.

## Not part of this item

- A snapshot metric on `/metrics` (`melin_snapshots_saved_total`, last
  saved sequence, last save duration). Operators would want it for
  alerting on a stale snapshot, and it would let the suite judge from
  the sampler instead of logs. It is a separate product change; log
  lines suffice for the benchmark.
- Fixing determinism-audit findings 21 and 22.
- Any comparison mode between snapshot locations.
