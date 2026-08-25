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

- **`--dpdk-peer-mac <mac>`** — the Ethernet address of the primary a replica
  dials over DPDK. ARP cannot supply it on a DPDK port: an SR-IOV VF receives
  no broadcast, and a port shared with the kernel steers only IPv4 by source
  address. The replica previously assumed the address convention
  `dpdk-setup.sh` assigns to VFs, which is wrong on any port that keeps its
  real hardware address — the replica's connection attempts then went to an
  address nothing answered for, with no error to show for it, retrying with
  backoff forever. The derived fallback is unchanged, and the startup log
  names which source supplied the address.

### Changed

- **"Durability mode" is now the "ack policy"**, and its values name the
  copies that must exist before a response is released: `disk` (one fsynced
  copy), `ram` (two in-memory copies), `disk+ram` (one fsynced copy plus a
  second in memory — the default), `two-disks` (two fsynced copies). The old
  names implied that the journal fsyncs less in some modes (it never did) and
  that the fsynced copy is the primary's (it is whichever node confirms
  first). Source-breaking: `--durability-mode` is `--ack-policy`, the admin
  command `DURABILITY` is `ACK-POLICY`, `DurabilityMode` is `AckPolicy`, and
  the `melin_durability_policy_degraded*` metrics are
  `melin_ack_policy_degraded*`. The byte advertised on the replication stream
  is unchanged, so mixed-version clusters keep interoperating. For this one
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
  set of zero-copy receive fixes.

### Removed

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

[Unreleased]: https://github.com/melin-engine/melin/compare/v0.14.0...HEAD
[0.14.0]: https://github.com/melin-engine/melin/releases/tag/v0.14.0
[0.13.0]: https://github.com/melin-engine/melin/releases/tag/v0.13.0
