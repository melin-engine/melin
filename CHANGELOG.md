# Changelog

All notable changes to Melin are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
the project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Every published crate in the workspace shares a single version number, so an
entry here covers all of them.

While the project is at `0.x`, a minor release may contain breaking changes.
Anything source-breaking is called out under **Removed** or **Changed**.

## [Unreleased]

### Added

- **`--dpdk-client-rx-buf-kib`, `--dpdk-client-tx-buf-kib`,
  `--dpdk-client-tx-queue-kib`** — the per-connection buffering of the
  sockets accepted on the DPDK trading port, previously fixed at 64 KiB,
  16 KiB and 64 KiB. The send buffer is the in-flight window, so it caps a
  single connection's response bandwidth at that much per round trip; the
  queue is the slack behind it before the connection is dropped as fallen
  behind. The defaults are unchanged and suit fan-in; a single connection
  driven at the stack's full rate — a load generator at a million
  messages a second — reaches the window and is dropped on the first
  round-trip hiccup, and wants both raised. Source-breaking for direct
  users of `melin_dpdk::DpdkConfig`, which gains `client_buffers:
  SocketBuffers`.

- **`--journal-sync <batch|writeback>`** — whether the disk thread
  `fdatasync`s each batch. `batch` is the default and the previous
  behaviour. `writeback` writes and never syncs, leaving the device to the
  kernel's writeback; accepted only under `--ack-policy ram`, whose gate
  needs no persisted copy, and refused at startup with every other policy.
  For network-attached volumes, where the per-batch sync is a round trip
  that no ack waits on under `ram` but every batch pays, backing the
  journal ring up behind the device; the durability contract becomes that
  of a replicated log that never syncs. Applies on replicas too; rotation
  still syncs. Under `writeback` the staging mode defaults to `allocate`:
  `zero-fill` exists to keep the per-batch sync metadata-free, and without
  that sync its pre-writes would only spend a segment of bandwidth per
  rotation. Source-breaking for direct users of the runtime:
  `melin_transport_core::journal_disk::SyncMode` and a `set_sync_mode` on
  `JournalStage` and `JournalDisk`, `ServerConfig` gained `journal_sync` and
  its `journal_staging_mode` became an `Option` resolved by
  `ServerConfig::staging_mode`, and
  `melin_server_runtime::replication::run_receiver` / `run_receiver_dpdk`
  take the mode.
- **`--journal-staging-mode <zero-fill|allocate>`** — how the background
  preparer stages the next journal segment. `zero-fill` (the default under
  `--journal-sync batch`, and the previous behaviour) pre-writes it so
  appends never carry extent-conversion
  metadata; `allocate` only reserves it, trading that back for staging that
  costs no device bandwidth. Aimed at network-attached volumes such as EBS,
  where the pre-write draws from the same metered bandwidth as the hot path
  and the preparer keeps up only while the journal rate stays under a quarter
  of it, a ratio the segment size does not change. Which mode wins on a given
  volume is a property of that volume; measure both. Source-breaking for
  direct users of the runtime: `melin_journal::preparer::SegmentPreparer::
  spawn_zero_fill` is now `spawn` with a `StagingMode` argument,
  `PreparedSegment` gained a `written` field, `ServerConfig` gained
  `journal_staging_mode`, and `melin_server_runtime::replication::run_receiver`
  / `run_receiver_dpdk` take the mode.
- **A warning when a pre-written segment outgrows its staged region.** Appends
  past it silently regained the periodic filesystem-metadata commit until the
  next rotation. Logged once per affected segment; segments that were never
  pre-written (rotation disabled, the first segment after start, `allocate`
  staging) extend silently, as before.

### Changed

- **Journal replay readers hint sequential access** to the kernel
  (`POSIX_FADV_SEQUENTIAL`), so readahead runs further ahead of the recovery,
  catch-up and chain-rebuild scans. Invisible on local NVMe; on
  network-attached storage, where a device round trip is closer to a
  millisecond, it shortens restart and failover.
- **A replica serves its health endpoint without control-plane raft.**
  `--health-bind` (default `127.0.0.1:9878`) now starts the endpoint on a
  replica whether or not election is enabled; previously a non-raft
  replica stayed headless, which hid its liveness and, under
  `latency-trace`, its `/stats-dump` — the only place the replica's half
  of the replication round trip is visible. The election gauges are
  absent without raft, as before. A process that runs a primary and a
  replica on one host now needs distinct binds for the two.
- **`--dpdk-eal-args` requires the joined form** (`--dpdk-eal-args="-l 0-7"`).
  The space-separated form used to accept a value, but a forgotten value made
  the parser silently take the next flag as the EAL string; now either mistake
  is a startup error that names the fix. Launch scripts using the space form
  must add the `=`.

## [0.15.0] - 2026-08-27

### Added

- **`--dpdk-peer-mac <mac>`** — the Ethernet address of the primary a replica
  dials over DPDK. ARP cannot supply it on a DPDK port: an SR-IOV VF receives
  no broadcast, and a port shared with the kernel steers only IPv4 by source
  address. The replica previously assumed the address convention
  `dpdk-setup.sh` assigns to VFs, which is wrong on any port that keeps its
  real hardware address — the replica's connection attempts then went to an
  address nothing answered for, with no error to show for it, retrying with
  backoff forever. The derived fallback is unchanged, and the startup log
  names which source supplied the address. Source-breaking for anything
  constructing `melin_dpdk::DpdkConfig` directly: it gained a `peer_mac`
  field.
- **Two more examples.** `echo` is a state-free application whose client
  measures closed-loop round trips: the sequencer's latency floor.
  `notary` exercises the ordering guarantee with a hash chain over
  client-submitted digests, self-verifying receipts, a command-line client
  and an offline journal auditor.
- **The bounds an application must fit are public**, so a codec can assert
  against them at compile time: `melin_server_runtime::MAX_FRAME_SIZE` and
  `MAX_RESPONSE_BUF` for the wire; `melin_journal::codec::ENTRY_FRAMING_SIZE`,
  `TRANSPORT_PAYLOAD_SIZE`, `melin_journal::encoder::entry_size::<E>()` and
  `melin_transport_core::pipeline::max_journal_batch::<E>()` for the
  journal.

### Changed

- **The tamper-evident journal hash chain is on by default.** Every crate
  shipped with it off, so a stock build had no tamper evidence and — the
  loss that needs no attacker — no cross-node divergence detection. The
  replica handshake, every rotation boundary and the periodic chain checks
  compare BLAKE3 chain values, and without the chain those comparisons
  compile out: an ex-primary rejoining after failover with events it
  journaled but never replicated was streamed to on top of its forked
  history instead of being resynced. The cost is one incremental hash update
  per entry on the journal stage, off the matching thread. A build without
  the chain now says so at startup, at `warn`. Not source-breaking: the
  `hash-chain` feature keeps its name on every crate and a manifest that
  already enabled it is unchanged. One thing to check downstream:
  `default-features = false` on `melin-journal`, `melin-transport-core` or
  `melin-server-runtime` — previously a no-op on the latter two, which had
  no default features — now turns the chain off, and only the runtime's
  `hash-chain` feature switches all three together. **Upgrade-breaking for
  a node that ran 0.14 without the chain** (the old default): such a build
  wrote an all-zero anchor into every rotated segment and an all-zero chain
  value into every snapshot, and a build with the chain rejects both at
  recovery (`SegmentChainBreak`, `SnapshotChainMismatch`). Upgrade it the
  way a format bump is upgraded (see `docs/journal.md`): snapshot, deploy,
  start on a fresh journal directory, and give replicas a clean directory
  to re-bootstrap from. Restarting in place over a journal that has rotated,
  or over a snapshot plus its journal, is refused at boot. Mixed-version
  clusters interoperate during the rollout: a peer without the chain is
  skipped by the handshake and rotation checks, not judged divergent.
- **An application declares how wide its events can get**, via
  `AppEvent::MAX_ENCODED_SIZE`, and the journal sizes itself from that.
  Previously every entry got a fixed 144-byte reservation — 102 bytes of
  payload — which was invisible to application authors until an oversized
  event failed at run time, and which could not be raised without making
  every application pay for the widest event any of them might have. The
  relationship now inverts: ring slots stay a fixed size and the batch
  *length* adapts, so an application with narrow events still batches 4,096
  while one with 288-byte payloads batches about 1,588. Both write a
  comparable number of bytes per sync, which is what the device cost
  amortises over, and memory is unchanged for everyone. The ceiling on an
  application event rises from 102 to 1,047 bytes, enough for the widest
  event a 1 KiB client frame can induce. Source-breaking: the constant is
  required and has no default — the right value is a property of the
  implementor's wire format, and a default would hand a wrong bound to the
  application that most needed to think about it. Declaring more than the
  journal can carry fails to build (`cargo build` or `cargo test` — the
  check runs when the journal is instantiated for the type, which `cargo
  check` does not do); an event that outgrows its own
  declared bound is refused at encode time rather than corrupting a
  reservation several layers away. `melin_journal::encoder::MAX_ENTRY_SIZE`
  is now the ceiling across every application (1,088 bytes, up from 144),
  not the per-application reservation, which is `entry_size::<E>()`.
- **"Durability mode" is now the "ack policy"**, and its values name the
  copies that must exist before a response is released: `disk` (one fsynced
  copy), `ram` (two in-memory copies), `disk+ram` (one fsynced copy plus a
  second in memory — the default), `two-disks` (two fsynced copies). The old
  names implied that the journal fsyncs less in some modes (it never did) and
  that the fsynced copy is the primary's (it is whichever node confirms
  first). Source-breaking: `--durability-mode` is `--ack-policy`, the admin
  command `DURABILITY` is `ACK-POLICY`, `DurabilityMode` is `AckPolicy`, and
  the `melin_durability_policy_degraded*` metrics are
  `melin_ack_policy_degraded*`. The `durability_policy` module is
  `ack_policy` in both `melin-transport-core` and `melin-server-runtime`,
  `ServerConfig::durability_mode` is `ack_policy`, `ACKING_MODE_UNKNOWN` is
  `ACK_POLICY_UNKNOWN`, and the `durability_mode` fields on `StreamStart`,
  `Heartbeat` and `ReplicaControlPlane` (`primary_acking_mode`) follow. The
  startup and admin log lines say "ack policy" where they said "durability
  mode". The byte advertised on the replication stream is unchanged, so
  mixed-version clusters keep interoperating. For this one
  release the old admin verb still works (old value names included, logged at
  `warn`) and the old metric names are still exported alongside the new ones,
  so alerts and runbooks have a release to migrate; both go away in the next
  minor.
- **Failover guidance now covers every policy.** The "never restart a crashed
  primary in place" rule was documented for `replicated` only; because a
  policy counts copies rather than nodes, a replica's fsync can be the copy
  that satisfied `disk` or `disk+ram`, so a primary lost to power failure can
  come back short of acked events under any policy — bounded by the batches
  in flight under the disk-gated ones, unbounded under `ram`. Behaviour is
  unchanged; the docs now say so.
- **Replication hands its bytes to the DPDK wire in bounded slices.** The poll
  thread that receives client packets is also the one that serialises
  replication traffic, so a full queue was a client-ingress stall of that
  length. What one tick hands over is now capped, and the replication listener
  sends a whole in-flight window per egress pass instead of a few segments at
  a time; the trading port keeps its fan-in behaviour, so one client's burst
  still cannot delay its peers. Catch-up and snapshot transfer still run on
  that thread and are unaffected.
- **Lower per-request cost on the DPDK path.** A response and its batch
  terminator ride in one frame rather than two, and the connection table is
  cheaper to look up.
- **The DPDK packet buffer pool is allocated on the NIC's NUMA node** instead
  of node 0. On a two-socket host with the NIC on the far node, every received
  and transmitted frame previously crossed the interconnect. Ports on
  different nodes warn and follow the first.
- **The replication accept thread is no longer pinned**, and is named
  `repl-accept` for what it does — it accepts connections and sleeps, while
  the per-replica handler threads do the streaming. Pinning it spent a
  reserved core to idle. Operators can set the fifth `--cores` entry to `0` to
  hand that core back; every other position keeps its meaning.
- **The userspace TCP stack behind the DPDK transport moves to fastcp
  0.13.1**, picking up duplicate-ACK counting in the batch ingress path and a
  set of zero-copy receive fixes. Its neighbour cache is widened from 8 to
  64 entries: past 8 peers an evicted entry silenced a socket for the
  discovery timeout.
- **The examples are Apache-2.0**, `counter` included (it shipped under
  BUSL-1.1). They exist to be copied into an application, and the
  runtime's licence should not travel with the copied code. `counter` now
  enables `hash-chain` by default like the runtime it forwards to.

### Removed

- **`melin_journal::codec::MAX_PAYLOAD_SIZE`.** It described the `u16` length
  field, not a bound any application could rely on, and its documentation
  said codecs could assume it — 65 KiB against a real ceiling of 1,047
  bytes. The bound that applies is `AppEvent::MAX_ENCODED_SIZE`.
- **`PipelineCores::repl_sender`.** Source-breaking for anything constructing
  the struct directly — drop the initializer. No `--cores` value needs to
  change: the fifth entry is still validated and then ignored, deliberately,
  because the likeliest cause of a bad value there is a list shifted by one.

### Fixed

- **A replica handshake over DPDK could hang forever.** The per-handshake
  validation thread inherited the poll thread's pinning and real-time
  scheduling and so was never scheduled at all; the replica waited for a
  stream that was never started. Validation now runs on workers created
  before the poll thread pins itself.
- **A transient packet buffer shortage no longer aborts the server.** Both
  allocation sites asserted, so a shortage took down a sequencer carrying
  live orders. The transport now leaves the data queued for the next poll.
- **Off-subnet destinations no longer become unreachable 60 seconds after
  startup on DPDK.** The gateway's address was resolved once at startup and
  nothing refreshed it, so its entry expired and traffic through it fell back
  to an ARP the port cannot deliver. It is now refreshed well inside the
  entry's lifetime.
- **A closed DPDK connection now frees its demultiplexing slot.** Slots were
  held for the life of the process and new ones are refused past half
  capacity, so a long-running server degraded with connection churn rather
  than with concurrency.
- **A malformed DPDK address or MAC on the command line reports a usage error
  naming the flag**, instead of aborting the process.

## [0.14.0] - 2026-08-21

### Added

- **`--durability-mode replicated`** — RAM-quorum acking. A second node holds
  the event in memory before the client is acked, and disk writes trail
  asynchronously off the ack path; the journal still fsyncs every batch, it
  just no longer gates responses. Survives any single node failure via
  failover, and loses only the un-fsynced tail if the whole cluster loses
  power at once. Intended for deployments where fsync is slow — cloud block
  storage in particular — and where that bounded RPO buys the lowest available
  ack latency. Fails closed when no replica is connected.
- **The journal stage now runs on two threads.** Encoding stays on the
  pipeline thread; disk I/O moves to a dedicated journal disk thread fed by a
  hand-off ring. The new thread inherits its scheduling context from the
  parent, can be pinned like the others, and reports its lag as a gauge so a
  disk falling behind the pipeline is visible before it becomes a stall.
- **A declared minimum supported Rust version** (1.91), enforced in CI rather
  than documented and left to rot.

### Changed

- Crate versions are inherited from the workspace, so the whole set moves
  together and a dependent can never resolve against a sibling version that
  was never published.

### Removed

- **The `O_DIRECT` sector writer, and the `SectorSizeMismatch` error it
  raised.** Durability is `fdatasync`-based; the sector-aligned path it
  replaced is gone. Source-breaking for anything matching on that error.

### Fixed

- **Failover no longer livelocks.** A tip-behind replica could win an election
  it was not fit to serve and be deposed immediately, repeatedly — observed
  pushing failover past a 60-second deadline in roughly 5% of runs. Three
  changes close it: the per-node heartbeat offset is derived from the node id
  so nodes stop campaigning on an aligned grid, a node stands down when a
  reachable peer holds a fresher journal tip, and auto-promotion is refused
  behind a reachable peer's tip.
- Auto-promotion is refused on a blank genesis node, and requires a sustained
  primary outage rather than a momentary one.
- io_uring rings are proven quiescent before teardown on the replication
  sender, the replication receiver, and the reader, instead of teardown
  waiting out a timeout and hoping.
- A replica that sees a gap in the replication stream reconnects instead of
  exiting.
- **`hybrid` mode no longer puts the replica's disk on the client's ack
  path.** Once enough batches were awaiting the replica's fsync, its receiver
  stalled — and with it the in-memory acknowledgement the mode gates on, so a
  slow replica disk delayed client responses in the one mode designed not to
  wait for it. Pending acks now coalesce instead of blocking; an ack can only
  arrive later than before, never earlier.

## [0.13.0] - 2026-08-13

### Added

- **A raft control plane for leader election and automatic failover.** It runs
  on its own dedicated thread and never touches the hot path — the one place
  in the system where async and serialisation are permitted.
- **Pre-zeroed prepared segments.** The journal stages the next segment ahead
  of rotation and paces the zero-fill against the device rather than the page
  cache, so rotation no longer surfaces as a latency spike.

### Changed

- Durability is gated per slot rather than per batch, so a response is
  released as soon as its own event is safe instead of waiting for the batch.
- Journal batch buffers are pinned once and written with `WriteFixed`, and
  survive segment rotation instead of being re-registered.
- The reader recycles ring-mapped `buf_ring` entries, falling back
  automatically on kernels that do not support it.

### Fixed

- A response flush can no longer block on a slow client's socket, and
  heartbeats respect the send-buffer limit and skip peers that are blocked.
  One slow consumer no longer affects the others.
- Replication binds its listener at boot, before the pipeline starts, and
  fails startup outright if the bind fails rather than coming up unreplicated.
- Failed replica authentication backs off before retrying, and reconnect
  backoff resets once the primary has spoken during a session.
- Archived segments are compacted to their valid data when sealed.
- A rotation that committed is no longer reported as failed when the directory
  fsync errors afterwards.

[Unreleased]: https://github.com/melin-engine/melin/compare/v0.15.0...HEAD
[0.15.0]: https://github.com/melin-engine/melin/releases/tag/v0.15.0
[0.14.0]: https://github.com/melin-engine/melin/releases/tag/v0.14.0
[0.13.0]: https://github.com/melin-engine/melin/releases/tag/v0.13.0
