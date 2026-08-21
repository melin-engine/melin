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

[Unreleased]: https://github.com/melin-engine/melin/compare/v0.13.0...HEAD
[0.13.0]: https://github.com/melin-engine/melin/releases/tag/v0.13.0
