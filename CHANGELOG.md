# Changelog

All notable changes to Melin are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
the project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Every published crate in the workspace shares a single version number, so an
entry here covers all of them.

While the project is at `0.x`, a minor release may contain breaking changes.
Anything source-breaking is called out under **Removed** or **Changed**.

## [Unreleased]

### Changed

- **Dependencies refreshed to their latest compatible releases** (tokio,
  serde, thiserror, zerocopy and zeroize among them). Only the committed
  lockfile moves: no crate's version requirement changes, so a consumer's
  own resolution is unaffected, and a build from the repository with
  `--locked` picks up the new set. The resolved tree shrank, dropping
  transitive crates (rkyv, the wit-bindgen and wasm-tools stack, uuid,
  bitvec) that nothing needed any more. The minimum supported Rust version
  is unchanged.
- **`--max-connections` now sizes the kernel-TCP transport's io_uring
  rings, and must be between 1 and 8192.** The reader's and the response
  stage's rings, and the reader's receive-buffer pool, used to be fixed
  for about a thousand connections; they are now derived from the cap at
  startup. At the default cap (1024) the sizes are the ones they always
  were. A smaller cap takes less locked memory: the kernel charges ring
  memory against `RLIMIT_MEMLOCK`, so a node sized for the clients it
  actually serves fits a tighter limit, and many such nodes fit on one
  host. `0`, which meant unlimited, is refused at startup with an error
  naming the largest supported value, and so is anything above 8192; a
  deployment passing `--max-connections 0` must pick a cap. The cap now
  gates the DPDK transport's accepts unconditionally too.
- **The reader and the response stage take their ring sizes.**
  `reader::spawn_reader` takes a `connection_limit::RingSizing` and
  returns an `io::Result`, and `response::Response` has two new fields,
  `ring_sizing` and `ready`: a caller that builds either directly has to
  supply them. `response::run` returns a `bool` (whether the stage
  started) instead of `()`, so a `JoinHandle<()>` binding for its thread
  no longer compiles.

### Fixed

- **A node that cannot create its io_uring rings refuses to start, and
  says why.** Ring creation failed on the stage's own thread, which
  panicked; the node went down later, on noticing the dead thread, with
  an error that did not name the cause. Each
  stage now reports whether it holds its ring before startup continues,
  and a failure stops the node with an error. When the cause is the
  locked-memory limit, the usual one, the error names `RLIMIT_MEMLOCK`
  and the systemd `LimitMEMLOCK=` setting that raises it.
- **A `--max-connections` above about a thousand, or `0`, could panic the
  reader or the response stage.** The rings were fixed in size, so
  enough connections flushing or re-arming at once overran the
  submission queue. The rings are now sized from the cap, and a cap they
  cannot be sized for is refused at startup.
- **The reader's legacy receive-buffer fallback could overrun its
  submission queue.** On hosts that refuse the ring-mapped buffer pool,
  each recycled buffer takes a submission entry on top of those the
  ring is sized for; a full queue is now flushed to the kernel instead
  of panicking the reader.

## [0.19.0] - 2026-10-10

### Added

- **Application-defined client roles.** An application declares its
  roles as a type implementing `melin_app::auth::Role`, a table pairing
  each role with the token that names it in `authorized_keys`; the
  runtime keeps only the two roles it acts on, `operator` and
  `replication`. `validate_roles` checks a table (tokens lowercase ASCII
  letters, digits, `-` and `_`, starting with a letter; none of the
  runtime's; no token or role listed twice), and every keys-file parse
  runs it, so a node refuses a bad table before it serves. `NoRoles` is
  the role type of an application that admits operator keys only.
  `ClientRole<R>` is what a decoder receives: `Operator`, or `App(R)`.
  `KeyRole` is what the keys file grants a key, `Replication` or
  `Client(ClientRole<RoleId>)`, with `KeyRole::client` giving a key's
  client role and `None` for a replication key, so a listener of an
  application's own that admits the same keys applies the client
  listener's rule through it. `AuthorizedKeys::token` names a role for a
  log line. `RoleId` is an application role as the runtime carries it,
  an index into the role table, and `ErasedDecoder` the decoder as the
  runtime holds it, implemented for every `RequestDecoder`.
- **`melin_server_runtime::client_auth::verify_client`**, the client
  listener's check on a challenge response (key listed, not a
  replication key, signature over the nonce valid), with its
  `ClientAuthError`. A listener of an application's own that admits the
  node's client keys, an event publisher's subscribers say, calls it and
  applies exactly the client listener's rule, instead of keeping a copy
  of it in step.
- **`melin_dpdk::DpdkTransport::tx_drained`**: whether everything queued
  on a connection has been acknowledged by the peer. `close` discards
  what is unsent, so a caller with a last frame to deliver waits on it
  first.
- **A process-wide DPDK EAL**, for a process hosting several DPDK nodes,
  at once or one after another: `melin_dpdk::Eal::init_process_wide`
  initialises it once and never cleans it up, `Eal::process_wide` reads
  it, and `Eal::attach_vdev` / `detach_vdev` give each node a virtual
  device and port of its own. A node in such a process shares it and
  takes no EAL arguments of its own. A process that never calls it is
  unchanged: the node owns its EAL, as before.
- **`melin_server_runtime::server::run_with_shutdown`**: `run` on the
  build's own transport, stopped through a caller's flag instead of the
  process-wide signal handler `run` installs. For a host that owns its
  process's signals, or runs several nodes in one process. Unlike
  `run_with_listener`, the client listener stays the transport's own, so
  it works on DPDK.
- **DPDK testing without a NIC.** The DPDK transport runs on veth links
  through the `net_af_packet` driver, unprivileged and without hugepages:
  `scripts/dpdk/netns-runner.sh`, set as cargo's target runner, runs the
  integration suites on DPDK when the `dpdk` feature is enabled (the
  examples gain a `dpdk` feature for it), with the `dpdk` nextest
  profile. For contributors; nothing ships with it.
- **Peer liveness for long-lived DPDK links.** `melin_dpdk::PeerLiveness`
  arms keep-alive probes and a timeout on a connection
  (`DpdkTransport::set_peer_liveness`), and `DpdkTransport::reset` closes
  a connection and sends its RST rather than vanishing.
- **`DpdkTransport::from_shared_unlistening`**: a transport with no
  listener, for one that only dials out, to which listeners can be added
  later with `add_listener`.
- **I/O-free framing, for a program that runs its own I/O loop**
  (io_uring, DPDK). `melin_wire_protocol::framing` splits length-prefixed
  frames out of bytes the caller already holds: `split_frame` and
  `split_frame_limited` over a caller-owned buffer, and `FrameDecoder`,
  which buffers partial frames across reads. A prefix over the limit is
  `FrameTooLarge`, and poisons the decoder, since no frame boundary is
  left to resume from. `BlockingFrameReader` reports an oversized prefix
  as before, an `InvalidData` I/O error, now carrying the typed
  `FrameTooLarge` as its inner error, so a caller can downcast it. The
  same module frames requests in place: the
  caller encodes a body into the region `request_body` hands out, behind
  `REQUEST_HEADER_LEN` bytes of header, at any offset of a larger send
  buffer, and `seal_request` writes the length prefix and the tag in
  front of it; `frame_request` does both around a closure. The region
  is capped at `MAX_REQUEST_BODY` bytes, a longer body is refused, and
  on any error no header is written. A sender that writes the header
  apart from the body (a vectored write) checks the limit and gets the
  prefix value from `request_payload_len`. Failures are `RequestFrameError`,
  which converts into `ProtocolError`, so a codec crate that needs only
  framing depends on `melin-wire-protocol` alone and frames with `?`.
  `melin-client` re-exports the module as `melin_client::framing` and
  the request framing at its root, so a client needs no wire-protocol
  dependency to frame, and adds `next_reply`, the next classified
  `Reply` out of a `FrameDecoder`. None of it does I/O, so read
  timeouts, and the rule that a heartbeat does not extend one, stay with
  the caller.
- **Exit status 74 for a journal write failure.** When a sync of the
  journal fails (the device did not take data the kernel had accepted),
  the node stops and must not be restarted in place: the operating
  system can keep that data in memory, marked as written, and present it
  to the next process on the same host. `melin_server_runtime::exit::exit_code`
  turns what `server::run` returns into the process's exit status: 74
  (`EXIT_JOURNAL_WRITE_FAILED`, `EX_IOERR`) after a journal write
  failure, 1 for any other error. It reads the error's `source` chain
  and also a process-wide latch, `melin_journal::write_failure_latched`,
  so the status holds even when a layer above the journal flattened the
  error into a message; `exit::is_journal_write_failure` tests an
  error's chain alone. The latch also holds a failed sync the node
  survived (a rolled-back rotation), so a later stop on any error exits
  74; `exit_code` then prints a second line saying a journal sync failed
  earlier. An application's `main` must propagate the status
  (see The binary in `docs/building-an-application.md`), and a supervisor
  must not restart on it (systemd: `RestartPreventExitStatus=74`); see
  "When a journal write fails" in `docs/journal.md` for what to do
  instead. `JournalError::WriteFailed` is the error behind it, built
  through `JournalError::write_failed`, which sets the latch.
- **`melin_journal::test_utils::fail_next_sync`** and
  **`reset_write_failure_latch`**, under the `test-utils` feature: the
  next sync of a given live segment fails with `EIO`, entering where the
  kernel's error would; and the write-failure latch is cleared, for tests
  that share a process.

### Removed

- **`melin_app::auth::Permission`**, the runtime's fixed set of client
  roles, replaced by `ClientRole<R>` and `KeyRole` (see Changed). With
  it go the exchange's roles and their helpers, `Permission::Trader`,
  `Custodian`, `ReadOnly`, `can_trade` and `can_manage_funds`: they were
  one application's separation of duties, in the runtime every
  application builds on, and an application that used them declares them
  as its own `Role` type. `Permission::is_replication` and
  `Permission::may_connect_as_client` (the latter added in 0.18) go too:
  a replication key is a `KeyRole` now, not a permission, so
  `KeyRole::is_replication` replaces the one, and `KeyRole::client`,
  which is `None` exactly where `may_connect_as_client` was false, the
  other.
- **`set_next_sequence`, from `JournalWrite`, `BufferedWriter` and
  `JournalEncoder`.** It moved a writer's sequence counter anywhere,
  backwards included. `JournalEncoder::adopt_sequence` replaces it for a
  replica taking its primary's numbering, and accepts only the next
  sequence.
- **`melin_dpdk::DpdkTransport::from_shared_with_port`.** Build the
  transport with `from_shared_unlistening` and add the port with
  `add_listener`.

### Changed

- **`melin_client::Error` gains `FrameTooLarge { declared, max }` and
  `BufferTooSmall { needed, available }`.** `FrameTooLarge` is a length
  prefix from the node over the frame limit, from `next_reply` and from
  `Connection` alike (where it was an I/O error before), so a caller
  tells it apart from a frame no reply carries (`Protocol`) without
  asking the decoder; either way the connection is to be dropped.
  `BufferTooSmall` is a request framed in place that does not fit its
  buffer, converted from `RequestFrameError`. Source-breaking for an
  exhaustive `match` on `Error`: add the arms.
- **`JournalError` gains `WriteFailed`**, for a failed sync of journal
  data: the live segment's sync after an append, the sync of a new
  segment's header, and the sync that seals a reopened segment before
  appends resume. These used to be `Io`. Source-breaking for an
  exhaustive `match` on `JournalError`. A refused write (`pwrite`), a
  refused `fallocate` and every read error remain `Io`: a write the
  kernel refused leaves nothing behind that a restart could misread.
- **`server::run` reports a journal failure as itself.** A node whose
  journal stage failed returned the bare message `pipeline failure`
  (primary) or a formatted string (replica); it now returns an error
  naming the journal's failure and carrying it as its `source`. A
  replica that is shut down, whether streaming or waiting to reconnect,
  reports a journal stage that failed or panicked on the way down
  (exit status 1, or 74 for a write failure) instead of returning as
  from a clean shutdown. A replica whose journal stage failed when a
  resync from the primary begins now stops instead of resyncing over
  it. A replica whose own storage fails while it receives a resync
  (creating, writing, syncing or installing the snapshot or segment
  seed) now stops instead of retrying the transfer indefinitely: with
  status 74 for a failed sync, and 1 otherwise, a full filesystem
  included (safe to restart once space is freed).
- **A decoder receives the application's own roles: `Permission` is
  replaced by `ClientRole<R>`.** `RequestDecoder` gains `type Role`, and
  `decode` takes `role: ClientRole<Self::Role>` in place of
  `permission: Permission`. A decoder never sees a replication key, and
  its type now says so: there is no replication variant. Source-breaking
  for every decoder. To migrate: declare the roles your keys files use
  (`trader`, `readonly`, …) as your own `Role` type, so existing files
  load unchanged; set `type Role` to it; and match `ClientRole::Operator`
  and `ClientRole::App(role)` where you matched `Permission`.
  `AuthorizedKeys::parse` and `load` take the role type
  (`parse::<MyRole>`), and `lookup` returns a `KeyRole`. Errors and logs
  now name a role by its token in the keys file (`trader`, not
  `Trader`), and a keys file naming an unknown role is refused with
  `unknown role`, listing every valid one, where it said
  `unknown permission`. In `melin-server-runtime`,
  `reader::ReaderRegistration`'s `permission` field is now
  `role: ClientRole<RoleId>`, and `reader::RequestDecoderArc`, the
  decoder `spawn_reader` and `run_dpdk_poll` take, is now
  `Arc<dyn ErasedDecoder<E>>`.
- **The examples declare roles of their own.** The counter and echo
  examples admit `writer` and `reader` keys, the notary `submitter` and
  `auditor`, in place of the exchange's `trader` and `readonly`: a keys
  file written for an example needs its role tokens renamed.
- **`JournalError` gains `ReplicaSequenceMismatch`, `SequenceRegression`
  and `UnrecoverableTail`.** The first two are the sequence refusals
  under Fixed; the third is a segment whose entries stop early with data
  after them that no crash can explain (see the recovery rule below).
  Source-breaking for code that matches on `JournalError` exhaustively.
- **Genesis is journaled as the journal is created, not through the
  pipeline** (see the genesis fixes under Fixed). Visible effects: the
  first replica of a new cluster copies the genesis by catch-up, as it
  copies the rest of the history, rather than live — a new primary still
  holds its clients until that replica attaches, but the genesis is
  durable before it waits; every genesis event carries the same
  timestamp, the moment the journal was created; the genesis reaches the
  application by replay, so a new primary's `Application::prefault` runs
  before the genesis and again before it serves, as on a recovering
  primary; and the genesis's reports are not published on the event
  feed. In `melin-transport-core`, `journaled_app::write_genesis_journal`
  creates a journal this way, `journaled_app::genesis_staging_path`
  names its temporary file and `journaled_app::discard_genesis_staging`
  removes one an interrupted boot left behind.
- **The journal records the history's genesis length: journal format
  16, snapshot framing 3, replication protocol 7.** The journal header
  gains `genesis_entries`, how many entries the genesis occupies, written
  when the journal is created and carried unchanged by every later
  segment, every snapshot and every replica's journal; a replica learns
  it from its primary's `StreamStart`. Recovery refuses a segment whose
  recorded length differs from the segment before it
  (`JournaledAppError::GenesisLengthMismatch`, a new variant): within one
  history they always agree. Format-15 journals and
  version-1/2 snapshots are still read, with the length unknown, and a
  history begun under format 15 keeps writing format-15 headers (and
  version-2 snapshots), so its segments stay identical across nodes: no
  migration is needed. The replication protocol is not backward
  compatible: nodes on protocol 6 and 7 refuse each other at the
  handshake, so stop the cluster, upgrade every node, and restart on the
  existing files. Source-breaking: `JournalWrite::create_continuing`,
  `BufferedWriter::create_continuing` and `SegmentFile::create_continuing`
  take the genesis length (`Option<u64>`, `None` for unknown), as do
  `codec::encode_file_header`, `snapshot::save`, `shadow::run` and
  `replication::protocol::encode_stream_start`; `FileHeaderInfo`,
  `SnapshotHeader` and `PrimaryMessage::StreamStart` gain a
  `genesis_entries` field; `catchup::lineage_origin` returns the oldest
  segment's whole `FileHeaderInfo`. New: `codec::FORMAT_VERSION_V15`,
  `JournalReader::genesis_entries`, `JournaledApp::genesis_entries`,
  `snapshot::load_with_header`, `snapshot::MAX_HEADER_SIZE`, and
  `melin_journal::fresh_anchor` is public.
- **A DPDK primary reads a joining replica's catch-up and snapshot off
  its poll thread.** The poll thread also carries client traffic and the
  other replica's stream, and the join's disk reads stalled them all
  while they ran: seconds for a large snapshot on a cold disk. A worker
  per replica slot now does the reading, and the poll thread sends what
  it has read a little at a time between its other work.
- **A DPDK replica refuses client connections until it is promoted.** It
  listened on its client port without serving, so a connect completed
  and then saw nothing. It now has no client listener until promotion,
  and a connect is refused at once. A kernel-TCP replica is unchanged: a
  connect waits in the kernel's backlog. Clients retry either way.
- **One rule decides where a journal segment's data ends** (see the
  recovery fixes under Fixed). `JournalReader` applies it: `open` reads
  a live segment, `open_archived` an archive, `open_segment` either
  (`SegmentKind`), and `torn_tail` reports the torn write a live segment
  ended in (`TornTail`); a segment whose entries stop early with data
  after them that no crash can explain is `JournalError::UnrecoverableTail`
  (see above). `segment::LineageReport::live_tail_gap` is replaced by
  `live_torn_tail`: a sequence gap is now always an error.
  `write_ring::MAX_UNSYNCED_BYTES` names the bound the rule uses.
  `codec::decode` refuses an entry longer than its event type allows,
  so an application must never lower `AppEvent::MAX_ENCODED_SIZE` below
  the width of events in a journal it replays. Recovery now reads the
  live segment to its end, pre-allocated space included, on every start.
- **`melin_transport_core::tick::TickSchedule`** holds the tick deadline,
  the monotonic clamp and the stall catch-up rule that the io_uring
  reader and the DPDK poll thread each kept a copy of; both now drive the
  one schedule. Public for the crate boundary, not as a stable interface.

### Fixed

- **The segment preparer could stage a segment whose zeros never
  reached the device.** The zero-fill waited for each window's
  write-back and ignored the result; the wait consumes a write-back
  error, so the final sync then succeeded and the segment could be
  adopted at the next rotation. A write-back error there (`EIO`,
  `ENOSPC`, `EDQUOT`) now fails the prepare, which is logged and retried
  later, and the rotation allocates its segment synchronously in the
  meantime. A filesystem that does not support the wait at all keeps
  the old behaviour (logged once, the fill paces less accurately).
- **Recovery could delete acknowledged journal entries, and could
  refuse to start after an ordinary crash.** A sequence gap in the live
  segment, a range of zeros, or one flipped bit in the last entry's
  length each made recovery stop early and truncate everything after,
  replicated entries included; the newest archive, when the live segment
  was missing, could be cut short the same way. Yet a process killed
  mid-write could leave a final entry cut between its two magic bytes or
  inside its checksum, which recovery refused, keeping the node down
  until someone edited the file. Recovery now discards only what a crash
  can have left: a malformed final write within one unsynced write
  (40 MiB) of the last whole entry, followed by nothing but zeros, in
  the live segment only. Anything else — a whole entry with the wrong
  sequence, an over-long entry length, data beyond that reach, anything
  after an archive's last entry but zeros — stops the node with an error
  and leaves the journal untouched (see Crash Recovery in
  `docs/journal.md`).

- **A reconnecting replica could take entries it already held a second
  time.** A replica reconnects from the position its journal has made
  durable, but a session publishes ahead of durability, so when the link
  dropped its pipeline could still hold entries not yet written. If they
  were still unwritten when it reconnected (a disk stall longer than the
  reconnect backoff), the primary resent them and the replica journaled
  and applied them twice. A replica that reconnected without having
  written anything since its pipeline was built (a quiet primary, or a
  link that dropped straight after the first handshake) did worse: it
  reported an empty journal, and the primary either resent history the
  replica held or sent it a snapshot it did not need. The replica now
  waits for its journal to catch up with everything it received before
  reconnecting, and reports the journal it was built over from the start.
- **A journal could be written out of order in a release build.** The
  checks that refuse a repeated or backward sequence ran only in debug
  builds, so a release build journaled such an entry and the journal then
  refused to recover. Every build now refuses one before writing it. On
  a replica, any sequence but the next one stops the node with an error,
  its journal intact and recoverable, rather than duplicating history or
  leaving a hole.
- **The shutdown drain skipped an entry it failed to journal** and
  journaled the next one after it, leaving a hole behind an entry the
  application had already applied. It now fails as the steady-state path
  does, so the teardown reports a failed journal and a promotion refuses
  to proceed on that state.
- **A replica whose journal had failed while it was disconnected
  connected to its primary anyway**, taking one of the primary's replica
  slots for a session that ended at once. It now acts on the failure
  before connecting: a divergent journal is resynced in-process as
  before, and any other failure stops the node. A journal stage that
  panicked (only possible in a build that unwinds on panic; release
  builds abort) is now treated as failed too, instead of leaving the
  replica waiting for it indefinitely.
- **A replicated entry damaged in transit was applied, journaled and
  acknowledged.** Replicated entries carried no checksum of their own:
  the replica decoded each one and journaled it under a fresh CRC, so an
  entry corrupted past TCP's checksum — or in a NIC, a switch buffer or
  memory — that still decoded was taken as a different, valid event,
  acknowledged in memory and on disk, and counted toward the primary's
  ack policy. Each replicated entry now carries the CRC32C its primary
  journaled it with, and the replica checks the entry it is about to
  journal against it before applying or acknowledging anything. A
  damaged batch, whether in an entry or in the batch's own entry count
  and lengths, is refused whole and the replica reconnects to fetch it
  again, with a warning; an entry that arrives intact but that the
  application's codec does not reproduce byte for byte, or cannot
  decode, stops the replica with an error, since it could never hold its
  primary's history.
  Catch-up now ships entries exactly as they are on the primary's disk
  rather than re-encoding them. **Breaking on the wire:** the replication
  protocol changes, and a node on this release refuses a peer on 0.18 in
  either direction, so upgrade primaries and replicas together; the
  version it ships is the one under Changed (the genesis-length entry).
  In `melin-transport-core`, `try_decode_input_batch` and
  `try_decode_input_batch_into` return the new `InputBatchError`,
  `encode_input_batch` and `append_input_slot` return a `Result`, and
  `decode_journal_to_input_slots` is replaced by
  `encode_input_batch_from_journal`; in `melin-journal`, `CRC_SIZE`,
  `ENTRY_MAGIC` and the new `ENTRY_MAGIC_SIZE` are public and
  `last_user_entry_replication_slice` keeps the CRC trailer.
- **DPDK verified no TCP or IPv4 checksum on receive.** With receive
  checksum offload enabled the userspace TCP stack skips verification,
  but a NIC flags a bad checksum rather than dropping the frame, and
  nothing read the flag: a corrupted segment reached replication and
  client ingress unverified. Frames the NIC flags bad are now dropped
  before the stack sees them, and frames it leaves unchecked are verified
  in software. The policy is `melin_dpdk::rx_checksum`, built without
  `dpdk-sys` too; `DpdkDevice::rx_checksum_drops` counts the drops.
- **Starting from a snapshot alone applied the genesis twice.** A node
  with a snapshot and no journal segment — the layout the standard
  upgrade (snapshot, deploy, fresh journal) produces — restored the
  snapshot, whose state already held the genesis, and then journaled the
  genesis again; replicas followed. Only a node with nothing on disk now
  journals genesis.
- **A first boot that failed after creating the journal lost the
  genesis for good.** The journal was created before the boot checked
  its configuration, bound its listeners and journaled the genesis, and
  the next boot took the journal's existence to mean the genesis was in
  it. A refused `--standalone` (under any ack policy but `disk`), a port
  in use, a shutdown while a fresh primary waited for its first replica,
  or a crash in the middle of the genesis left a node serving without
  its genesis, or with part of it, with no error, and replicas — and a
  replica promoted later — followed it. The journal is now created with
  the genesis already in it: written under a temporary name and renamed
  into place once it is on disk, so it exists complete or not at all,
  and a boot that fails before the rename is retried as a first boot; a
  shutdown while a new primary waits for its first replica now finds the
  genesis already durable.
  Configuration a primary cannot run under is refused before the journal
  directory is touched, on replicas too, which serve under it once
  promoted. A replica whose primary was lost before it had copied the
  whole genesis now refuses promotion, and exits with an error, instead
  of serving a genesis prefix, and a primary booting from a history
  shorter than its genesis — a replica's partial copy started on a
  primary's flags — refuses to start. Both checks compare against the
  genesis length the history records (see the format change under
  Changed), never the node's own genesis configuration: nodes need not
  agree on it, and changing it after the first boot changes nothing. A
  replica whose journal records a different genesis length than its
  primary's refuses to follow it and exits. Histories begun by an
  earlier release record no length, and boot and promote unchecked, as
  they did. A journal with no entry at all, which an earlier release
  left after such a failure (or a replica's copy of it restarted on a
  primary's flags), gets its genesis on the next boot, with a warning;
  a replica holding an empty copy of it takes the recorded length from
  its primary on the next connection. An empty journal begun by this
  release keeps the length it records: it is never given a genesis
  later. A temporary genesis file left by an interrupted first boot is
  removed on the next boot, whichever way it starts.
- **A failed client authentication left a DPDK connection open.** A bad
  signature, an undecodable response or an oversized auth frame was
  answered with `AuthFailed`, but the connection stayed in the
  handshake: the client could retry signatures against the same nonce
  until the auth timeout, and an oversized frame was answered again on
  every receive. As on kernel TCP, one failed attempt now ends the
  connection, closed once the client has the `AuthFailed`.
- **`melin_replica_ack_latency_us` read 0 on DPDK.** Only the kernel-TCP
  sender recorded it. The DPDK sender now records the same measure: the
  time from the latest send to the latest ack (not a per-message round
  trip, on either transport, as its help text now says).
- **`melin_replica_evictions_total` over-counted on kernel TCP.** An
  eviction was counted, and warned about, again on every supervisor pass
  until the evicted handler exited. It is counted once.
- **Replication session teardown.** A kernel-TCP replica waiting to
  retry a failed resync held its socket, and with it a slot on its
  primary, through the backoff. A DPDK replica that failed to decode its
  primary's handshake reply left its pipeline running. Both now tear
  down as every other failed session does.
- **A new DPDK client connection was heartbeated early.** It was stamped
  with the time of the last heartbeat scan, stale under load, so the
  next scan could heartbeat a connection that had only just been made.
  It is stamped when the connection is registered.
- **On DPDK, neither end of a replication link noticed that its peer had
  gone.** A stopped DPDK node told its peer nothing, and neither end had
  a deadline on a silent peer. A primary kept counting a stopped replica
  (`melin_replicas_connected`) and never halted, so it did not refuse
  writes as a primary whose last replica has left must; a replica kept a
  stopped primary's link up, so auto-promotion refused to depose it and
  the cluster never failed over. A replication link is now reset once
  its peer has answered nothing for five seconds, a peer's FIN ends it,
  and a node that drops a link or stops tells its peer at once. A peer
  that is alive but has stopped reading, a replica installing a snapshot
  say, still answers, and stays connected: this relies on fastcp 0.13.2,
  now required, which probes such a peer at least once a second. The
  DPDK stack's clock is also monotonic now: on the wall clock, a step
  could have fired or held its timers.
- **A promoted DPDK replica could not serve.** Promotion fell back to the
  kernel-TCP primary, which binds a kernel socket on the client address,
  an address only the DPDK port holds: the bind failed and the node
  exited instead of taking over. Where the kernel did hold the address,
  the node served on kernel TCP, giving up kernel bypass without a word.
  A promoted DPDK replica now becomes a DPDK primary, through the same
  steps as a kernel-TCP promotion, on the transport it ran on.
- **A DPDK replica that stopped reading as it finished catching up froze
  its primary's clients.** The last step of a replica's join, the
  switch from catch-up to the live stream, ran on the poll thread that
  also carries client traffic, and waited there for room in the
  replica's socket. A replica that stopped reading at that moment, or
  died then, held every client for as long as its socket stayed open:
  indefinitely, for one that stayed connected. The switch now advances a
  little at a time between the poll thread's other work, reading the
  disk on the slot's worker, exactly as the catch-up before it does, and
  sends the same entries in the same order. A joining replica that has
  started acknowledging entries and then takes nothing the primary has
  for it for five seconds (the replication liveness timeout) is dropped
  and reconnects, rather than keeping its slot for as long as it stays
  silent. Before its first acknowledgement it is never dropped for
  reading nothing, so a replica installing a large snapshot may take as
  long as its state needs to load; one that stops reading once its ring
  is active is still evicted when the ring fills, as before. Kernel-TCP
  primaries are unchanged: each replica's join runs on its own thread.
  The stepped handoff is `melin_transport_core::replication::handoff`
  (`LiveHandoff`, `HandoffStep`, `PassProgress`, `HandoffIo`), and
  `melin_journal` `ReplicationConsumer::pending` re-reads the batch a
  consumer holds uncommitted across steps. Both are public for the crate
  boundary, not as a stable interface.
- **The io_uring reader fell back to legacy buffer recycling on kernels
  with pages larger than 4 KiB.** The provided-buffer ring was aligned to
  4 KiB, and the kernel refuses one not aligned to its own page size
  (16 KiB and 64 KiB are common on aarch64), so registration failed
  whenever the allocation missed that boundary by chance, with a warning,
  and the reader paid one extra submission and completion per received
  chunk. The ring is now aligned to the running kernel's page size.

## [0.18.0] - 2026-09-27

### Added

- **`Application::tick` and `Application::query` have defaults.** `tick`
  does nothing, and `query` answers nothing, so an application with no
  time-driven work or no queries leaves them out. The default `query`
  panics in debug builds when handed an event whose `is_query` is true:
  an application that has queries and forgot to implement it would
  otherwise answer each with an empty batch.
- **`melin_app::NoQuery`**, a type with no values, for the
  `QueryResponse` of an application that answers no queries. Its
  encoder's `encode_query` becomes `match *query {}`, which the compiler
  proves unreachable. The echo example uses it.
- **`melin_app::key_hash`**, the function every transport derives
  `ApplyCtx::key_hash` and `QueryCtx::key_hash` from, so a test harness
  or tool can compute which value a given public key arrives under.
- **`Permission::may_connect_as_client`**, false for the `replication`
  role only.

### Removed

- **`--max-orders-per-account`, `--max-orders-per-second` and
  `--max-orders-burst`, from `ServerConfig`.** Limits of one application,
  the exchange, that the runtime never read: every other application's
  node accepted them and did nothing with them. An application that needs
  such limits declares its own flags and journals their values as
  `StartupEvents`. A node given one of these flags now refuses to start,
  as for any unknown flag.
- **`melin_app::EncodeReport`.** Nothing implemented or required it; a
  response is encoded by the application's `ResponseEncoder`.

- **The per-key duplicate-request gate: `Application::check_request_seq` and
  `RejectReason::DuplicateRequest`.** Whether a repeated request is refused
  was already the application's policy; the runtime's part was to ask
  before `apply` and reject on its behalf. Every event now reaches `apply`
  — live, on replay and in the shadow stage, through one shared path — and
  an application that refuses repeats does so there: it decodes the
  sequence into its own event, checks it keyed on `ApplyCtx::key_hash`, and
  builds its own rejection report. Source-breaking for every `Application`
  implementation: delete `check_request_seq`, move its check to the top of
  `apply` (skipping queries, as the runtime did), and stop matching on
  `DuplicateRequest`.
- **The request sequence, from the wire, the journal and replication.** With
  the gate gone the runtime never read it, yet every request, journal entry
  and replication slot carried its eight bytes for every application. A
  client frame is now `[tag][body]`, the handshake's challenge response
  included (see Changed for what the tag now is); `melin-client`'s
  `send`, `request` and `request_one` lose their sequence argument;
  `Decoded::Permitted` is a tuple variant carrying only the event;
  `InputSlot` and `JournalEntry` lose `request_seq`, and
  `melin-journal`'s `JournalWrite::encode_event`, `batch_append_with_ts`,
  `codec::encode` and `codec::decode` the matching argument or tuple
  field. An application that sequences requests puts the sequence in its
  own request body and event. Breaking on every surface it touched:
  - **Journal format 15.** Entries are eight bytes shorter, and a node
    refuses a format-14 journal with `UnsupportedVersion`. Upgrade through a
    snapshot, as for any format change: snapshot on the old version, deploy,
    start on a fresh journal.
  - **Replication protocol 5.** A replica and primary on different versions
    refuse each other at the handshake; upgrade the cluster together.
  - **Client protocol.** A client built against an earlier `melin-client`
    fails the handshake against this node, and one built against this
    version fails it against an earlier node; either way the client sees
    its key refused. Upgrade clients with the nodes.

### Changed

- **The frame's tag is the protocol's alone; an application's bytes are an
  opaque body behind it.** Every frame is `[length][tag][body]`, and an
  application's request or response travels as an application frame, under
  the one tag `TAG_APP` (`0x09`). The protocol no longer shares its tag
  byte with the application by range, so an application no longer numbers
  its messages from `0x10`, and nothing it sends can be read as a protocol
  frame: its body may start with any byte, or be empty. The runtime reads
  and writes the framing. A `RequestDecoder` is called as
  `decode(body, permission)` and returns `Decoded::Permitted(event)`; the
  runtime drops an empty frame and any frame that is not an application
  frame before a decoder sees it. A `ResponseEncoder` writes the body and
  returns its length; the runtime writes the length prefix and `TAG_APP`.
  `melin-client`'s `send`, `request` and `request_one` take the body alone,
  and `Frame::Response` / `Reply::Response` carry the body with the tag
  stripped.
  - **Client protocol.** Breaking on the wire: an application's first byte
    moves behind `TAG_APP`. It ships with the request sequence's removal,
    whose break an earlier client already meets at the handshake (see
    Removed), so there is no further upgrade step.
  - **Source.** For application codecs: keep a message discriminator, if
    the application needs one, as the first byte of its own body, and
    parse and write it there. `Encoded` is removed.
    `melin_server_runtime::MAX_RESPONSE_BUF` is replaced by
    `MAX_RESPONSE_BODY`, a bound on the body alone, and `MAX_REQUEST_BODY`
    gives the matching request bound; `MAX_FRAME_SIZE` remains for
    programs that read client frames themselves. `melin-wire-protocol`
    gains `TAG_APP` and `TAG_LEN`.
- **Queries have a method of their own, which cannot change state.**
  `Application::query(&self, event, &QueryCtx) -> Option<QueryResponse>`
  answers queries; `Application::apply` now takes only journaled events and
  returns nothing. A query was never journaled, so one that changed state
  changed it on one node and nowhere else — now the compiler refuses it. A
  query no longer advances the scheduler clock on the matching stage or the
  shadow stage, whatever its slot's timestamp. The node-local counters
  (`journal_sequence`, `active_connections`, `events_processed`) move from
  `ApplyCtx` to `QueryCtx`: replay passed zeros for them, so an `apply` that
  read them diverged from the live run. `ApplyCtx` keeps `now_ns` and
  `key_hash`, both journaled with the event. To migrate: move
  query arms out of `apply` into `query` (returning `None` for any other
  event), leave a no-op arm for query variants in `apply`, and read the
  counters from `QueryCtx`.
- **Log lines, metric descriptions and messages no longer describe a
  trading system.** Wording only — no metric, flag or health-endpoint field
  is renamed — but an alert that matches on message text needs updating:
  - `all replicas disconnected — trading halted` (warn) is now
    `all replicas disconnected — halted, refusing client writes`.
  - `raft core stopped — control plane down, trading unaffected` (error)
    now ends `sequencing unaffected`.
  - The snapshot-transfer error now says the *shadow stage* writes
    snapshots, and the auto-promotion refusal speaks of acked *events*.
  - The `# HELP` text of `melin_trading_active`, `melin_events_processed`
    and `melin_raft_driver_running` is reworded; the metric names and the
    health endpoint's `trading` / `halted` flag are unchanged.
  - `--help` describes the binary as a node of the Melin replicated
    sequencer.
- **A replication key can no longer open a client connection.** The
  `replication` role authorizes node-to-node streaming only; the client
  listener, over TCP and DPDK, now refuses it during the handshake, so no
  request of its reaches an application's decoder. A client that
  connected with a replication key needs a key of its own, under a
  client role.
- **An `authorized_keys` file that lists a key twice no longer loads.**
  The last line used to win silently; which role was meant is not the
  loader's to guess. A node given such a file refuses to start and names
  the line. Remove the duplicate before upgrading.
- **A snapshot whose `restore` leaves bytes unread is refused.** Unread
  bytes mean `snapshot` and `restore` disagree about the layout, usually
  a layout changed without an `APP_VERSION` bump, and the restored state
  is not the saved one. A node refuses to start from such a snapshot
  (`SnapshotError::UnreadPayload`), and `Application::clone_via_snapshot`,
  which builds the shadow stage's copy, fails the same way. An
  application whose `restore` deliberately skipped trailing bytes must
  read them. Source-breaking for code that matches `SnapshotError`
  exhaustively.
- **An `AppEvent::encode` that returns a length other than
  `encoded_size` stops the node.** Release builds used to frame the
  entry from the returned length and journal, acknowledge and replicate
  a truncated event that recovery could not decode. The journal now
  refuses it before it is written, and the node stops. It is an
  application bug: a retry of the same event stops the next primary too.
- **`key_hash` is computed by Melin's own code rather than through
  rustc-hash**, so an application's dependency updates or toolchain can
  no longer change it. Every key keeps the value it had; journals and
  state kept under it are unaffected.
- **The counter example refuses an increment that would overflow**, with
  a new response (`KIND_RESP_OVERFLOW`, `0x33`) carrying the unchanged
  value, where it used to wrap. It also decodes exactly, refusing
  trailing bytes in requests and journal entries, and refuses increments
  from `readonly` keys.

## [0.17.0] - 2026-09-22

### Added

- **`melin-pipeline`: `spsc::Consumer::refresh` and
  `try_consume_visible`**, to consume only what was published before a
  chosen point, and **`ring::Batch::next_sequence`**, the sequence the next
  entry in a batch takes.
- **`melin_writes_refused_total` on `/metrics`**, a counter of client
  writes turned away while the node was halted. A refused write is never
  journaled, so until now it left no trace at all.

### Changed

- **`MatchingStage::new` no longer takes the replica count, and
  `OutputSlot` loses `durability_bypass`.** The matching stage applies
  every event it is given; halting is the readers' job (see Fixed).
  `spawn_reader`, `run_dpdk_poll`, `dpdk_response::run` and
  `response::Response` take the new halt gate and refusal queue from
  `melin_server_runtime::halt`. Application traits are unchanged, but
  `Application::build_reject` now runs on the request-reading thread for a
  halt rejection, rather than on the matching thread.
- **A superseded node closes client connections instead of answering.** A
  node fenced by a newer primary is stopping; a connection that sends
  anything while it winds down is closed at once, and the rest close when
  the process exits. Clients reconnect to the new primary, as after a
  crash. Previously the node queued a `Superseded` rejection that, in
  practice, never went out before the stage stopped.
- **Operator configuration reaches the application as journaled events.**
  `server::run` and `server::run_with_listener` take `StartupEvents { genesis,
  on_primary }` where they took an `AppFactory`. `genesis` is journaled once,
  when a node creates the journal as primary; `on_primary` every time a node
  becomes primary — at boot, and on promotion right after the epoch bump. Both
  are applied before the first client is served. The values in force are the
  primary's: replicas apply them from the stream, and replay, from genesis or
  from a snapshot, reproduces the decisions made under them. Operationally, a
  node's own limits take effect only while it is primary; a replica started
  with different values follows the primary's until it is promoted.
- **`Application` requires `Default`**, the state before the first event on
  every node. It must not depend on anything local to the node; capacity may
  still be pre-allocated there.
- **Capacity comes from the node, through `Application::Sizing`.** The trait
  gains an associated `Sizing` type — what the operator tells the application
  to reserve for, `()` when there is nothing — and `prefault` takes it:
  `fn prefault(&mut self, sizing: &Self::Sizing)`. `server::run` and
  `server::run_with_listener` take the node's sizing after its startup
  events: `server::run::<MyApp>(config, startup, sizing, decoder, encoder,
  None)`; `replication::run_receiver` and `run_receiver_dpdk` take a
  reference to it. Sizing is local to the node and never journaled, so it
  must not influence what `apply` decides; `Default` stays the small,
  parameterless genesis, and production capacity is reserved in `prefault`.
  A genesis instance is sized before a journal is replayed into it, and
  every instance is sized again before it serves — a restored snapshot only
  then, since it has no genesis instance. An implementation must therefore
  change capacity only — every entry survives and `apply` decides the same
  afterwards, though a collection may be rebuilt at the larger size — and
  be a no-op when called again.
- **Replicas size their application too.** `prefault` now runs on a
  replica before its pipeline starts, and again on one rebuilt after a
  resync. Until now only a primary at boot, and a replica at promotion, ran
  it: a replica applied the whole stream on cold, unsized collections, and
  under `disk+ram` its growth and page faults sat on the primary's ack
  path.

### Removed

- **`RejectReason::Superseded`.** Nothing produces it any more (see
  Changed). Applications that mapped it to a wire code or a display string
  drop that arm.
- **`melin_app::app_factory::AppFactory`.** To migrate: implement `Default`
  for the application from what `empty` built; return `seed_events` as
  `StartupEvents::genesis`; turn what `apply_operator_policy` set into events
  the application applies, passed as `StartupEvents::on_primary`, and keep
  those values in the snapshot; move what `AppFactory::prefault` sized from
  into `Application::Sizing`, and do the reserving in `Application::prefault`
  (see Changed). The runtime entry points now name the application type —
  `server::run::<MyApp>(config, startup, sizing, decoder, encoder, None)` —
  and `replication::run_receiver` / `run_receiver_dpdk` no longer take a
  factory. An application with nothing to journal at startup passes
  `StartupEvents::none()`, and one with nothing to reserve passes `()`.
- **`--accounts` and `--instruments`**, and the `ServerConfig` fields behind
  them. They were the counts the runtime seeded an exchange from, and nothing
  read them any more. An application that sizes its genesis from the command
  line defines those flags itself and builds `StartupEvents::genesis` from
  them, and its `Sizing` too if they are also what it reserves memory for.

### Fixed

- **A write refused while halted was replayed.** A primary with no replica
  attached, or superseded by a newer one, rejected client writes in the
  matching stage — after the journal stage, running beside it, had already
  recorded them. The client was told the write failed, yet the next replay,
  a replica catching up, or a promoted node applied it. The node now
  refuses writes before publishing them, so a refused write is never
  journaled. The rejection is unchanged on the wire and still skips the ack
  policy, but now waits for the replies to the client's earlier requests,
  so replies stay in request order.
- **A node held on its ack policy could not be stopped.** With every
  replica gone under a policy that needs one, the response stage waited in
  the durability gate for a replica to return and never observed shutdown,
  so an operator restart, or a fence, hung the process. The gate wait now
  exits on shutdown. The reply it was holding is dropped, as it would be
  by a crash: the policy never confirmed it, and the client reconciles on
  reconnect.
- **A request refused as a duplicate could reach a snapshot.** A duplicate is
  still journaled, and while the live engine refused it, the stage that keeps
  the copy snapshots are written from applied it anyway. Recovering from such
  a snapshot — on restart, or on a replica bootstrapped by snapshot transfer —
  therefore held the effect of a request whose client was told it was
  rejected. Replaying the journal refused the duplicate but still advanced the
  application's clock for it, firing time-driven tasks at a point the primary
  never did. Both now refuse a duplicate exactly as the live engine does. Only
  an application that enforces request sequences is affected.

  Upgrading does not repair a snapshot an earlier version already wrote, and
  nothing in the file shows whether it holds a duplicate. Recovery restores
  such a snapshot as-is, and the journal's hash chain cannot catch it: the
  chain covers the journal, not application state. If your application
  enforces request sequences, stop the node, move its snapshot and the
  `.prev` beside it aside, and restart: with the journal intact from
  sequence 1, recovery rebuilds state by replaying it, now refusing
  duplicates. A node whose journal no longer reaches sequence 1 — a replica
  bootstrapped by snapshot transfer, or one whose old segments were removed —
  refuses to start without its snapshot rather than rebuild partial state;
  re-bootstrap it from a node that has recovered this way.
- **An event applied during shutdown lost its client identity.** Events
  still queued when a node stopped were applied with no client key, where
  the live engine and replay pass the submitting key. An application that
  reads the key when applying a write could reach different state for those
  events than a replay of the same journal. The live engine, replay and the
  snapshot stage now hand every event to the application through one shared
  path, so they cannot disagree on it.
- **Recovery replayed the journal under the application's default limits.**
  A primary restarting from its journal, with or without a snapshot, applied
  operator policy only after replaying, so orders the policy had rejected were
  accepted on replay and the node's state diverged from its replicas'. A
  replica's restart did the same, and a replica bootstrapped by snapshot
  transfer, and every shadow copy of an application that keeps the default
  `clone_via_snapshot`, never received the policy at all. Superseded by the
  change above, which removes out-of-journal policy altogether.

## [0.16.0] - 2026-09-14

### Added

- **`--journal-staging-mode <zero-fill|allocate>`** — how the background
  preparer stages the next journal segment. `zero-fill` (the default, and the
  previous behaviour) pre-writes it so appends never carry extent-conversion
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
- **`melin-client`** — the client side of the wire protocol as a crate:
  connect and authenticate with an Ed25519 key (PEM or raw seed), send
  requests, read reply batches with heartbeats skipped, and a node's silence
  reported as an error that says what it usually means. Blocking, `std::net`
  only, and generic over the application's tags — a client of an application
  is its own codec and nothing else. The handshake is also a function of its
  own over any `Read + Write` stream, with unbuffered reads, for a program
  that owns its socket: a Unix socket, or a descriptor its own I/O loop
  takes over once the node is ready. Under that is a state machine with no
  I/O in it, fed the node's frames and handing back what to send, for a
  program whose frames arrive through an I/O loop of its own: a gateway
  session, a load generator on a user-space TCP stack. The same program
  tells a node's frames apart with `classify`, the decision `next_frame`
  makes on each frame it reads — response, heartbeat, batch end, busy,
  engine error, or a protocol error for an empty frame or a reserved tag —
  as a function of the payload alone. Every example client
  and test harness now uses the crate instead of a private copy of the
  framing and handshake.
  Apache-2.0, like the examples: it is the code a customer links into their
  own client binaries.
- `melin-wire-protocol`: `encode_challenge_response` and
  `CHALLENGE_RESPONSE_LEN` beside the decoder, so the handshake frame has one
  home; `BlockingFrameReader::frame` returns the last frame read;
  `BlockingFrameWriter::write_frame_parts` writes a frame held in pieces
  without a staging copy, which is how the client sends a request; the
  writer refuses a frame over the 1 KiB cap before any of it is written,
  and the cap is public (`MAX_FRAME_SIZE`, also re-exported by
  `melin-client`, whose `send` reports a request over it as
  `RequestTooLarge` without sending it).
- **A wait policy per pipeline thread, in `--cores`.** Each thread's core
  takes a suffix: `matching=7` (or `matching=7s`) busy-spins and needs
  core 7 to itself, `matching=7y` spins briefly then yields and may share
  it. One node can therefore spin its hot stages on isolated cores and
  pack the auxiliary threads onto a shared core that yields — previously
  the choice was one policy for the whole process. `0` (unpinned) always
  yields, so `0s` is refused; `journal-prep` blocks in I/O rather than
  polling and takes no suffix. The node refuses to
  start when two threads share a core and either busy-spins, naming both
  threads and the core: a spinner on a shared core holds the CPU for a full
  scheduler slice while the thread it is waiting for sits queued behind it,
  which is how co-scheduled pipeline threads starved each other. The boot
  log prints the resolved layout in `--cores` syntax. Every wait in the
  pipeline — consumers polling an empty ring, producers blocked on a full
  one, the journal stage waiting on its disk thread, the response stage's
  durability gate, the startup drains — goes through the thread's policy;
  the producer-side waits and the gate previously spun unconditionally,
  whatever `--yield-idle` said. Source-breaking for direct users of the
  runtime: `PipelineCores` fields are `Placement { core, wait }`
  instead of bare core numbers; `EventPublisherFn` takes a
  `melin_pipeline::wait::WaitStrategy` where it took a `bool`; the pipeline
  builders take a `StageWaits`; the replication `Sender` takes `handlers:
  [Placement; 2]`; `run_receiver` / `run_receiver_dpdk` no longer take a
  wait flag; and `melin_pipeline`'s `DisruptorBuilder::build`,
  `spsc::channel` and `melin_journal::replication::build_replication_ring`
  take the producer's `WaitStrategy`.
- **A boot warning when a mandatory pipeline thread has no core.**
  journal-seq, matching, response, reader and journal-disk carry every
  request, so one of them left to the scheduler shares its core with
  whatever else runs there and pays for it on every acknowledgement. The
  warning names the threads. The auxiliary threads draw none: leaving them
  unpinned is a documented layout. `--cores none` gets its own line, as the
  development layout whose latency figures say nothing about the node.

### Changed

- **`--cores` names its threads.** A layout is now written
  `journal-seq=1,matching=2,response=3,reader=4,journal-disk=5,…`, in any
  order, instead of as a list read by position. Every thread must be
  named, `journal-prep` and `journal-disk` included, although a nine- or
  ten-entry list used to leave them unpinned: running a thread unpinned is
  now a stated choice (`journal-disk=0`) rather than an omission. A core
  of `0` still unpins any thread, and `none` unpins every thread. The
  positional list could not shed its retired fifth entry (the replication
  accept thread, unpinned since 0.15) without reading every later core as
  its neighbour's, in most cases with no error, and each new thread could
  only be appended as another optional position. A positional value is
  now refused at startup with a message naming the threads, as are
  missing, unknown and repeated names, and the boot log prints the layout
  in the named form. The default layout is unchanged. To migrate, name the
  positions and drop the fifth: `1,2,3,4,0,6,7,8,9,10,11` is
  `journal-seq=1,matching=2,response=3,reader=4,event-publisher=6,shadow=7,repl-handler-0=8,repl-handler-1=9,journal-prep=10,journal-disk=11`;
  a nine- or ten-entry list adds `journal-prep=0` and `journal-disk=0` for
  the threads it left out; and an all-`0` list is `none`.
  `PipelineCores::unpinned()` builds the latter in code.
- **The journal's sequencing thread is named `journal-seq`**, where it was
  `journal`: in `--cores`, in the thread name `top -H`, `ps` and `perf`
  show, and in log lines. The journal stage runs three threads —
  `journal-seq` orders, encodes and hash-chains events, `journal-disk`
  writes and syncs them, `journal-prep` stages the next segment — and the
  bare name did not say which one it was. Stage-level labels (utilization
  and latency statistics) still say `journal`. Source-breaking for direct
  users of the runtime: `PipelineCores::journal` is `journal_seq`.
- **`journal-disk` and `journal-prep` are started by the thread that
  starts `journal-seq`, instead of by `journal-seq` itself.** A thread
  inherits its creator's placement, and on a host with isolated cores
  `journal-seq` busy-spins on one at real-time priority: an unpinned
  helper it started could stay on that core and barely run. Started
  beside the other pipeline threads, an unpinned helper now runs on the
  non-isolated cores like every other unpinned thread. Source-breaking
  for direct users of the runtime: `JournalStage::run` is replaced by
  `JournalStage::start`, called on the thread that spawns the sequencing
  thread, and `Sequencer::run`, called on the sequencing thread;
  `JournalStage::run_sync` is gone.
- **A `--cores` layout that puts two threads on one core no longer starts
  the node.** Previously such threads busy-spun against each other; the
  optional threads were even documented as able to share an auxiliary
  core. Now the node refuses at boot unless both entries carry a `y`
  suffix, and it refuses for every thread, including threads no flag
  enables. To migrate, suffix the sharing entries:
  `event-publisher=6,shadow=6` becomes `event-publisher=6y,shadow=6y`. On
  DPDK the `reader` entry cannot take `y` — it pins the NIC poll thread,
  which never yields — so give it a core of its own.
- **A replica's shadow stage now busy-spins by default**, like the
  primary's, instead of always yielding: it is pinned to the `shadow` core
  the same way. A replica whose `--cores` puts the shadow on a shared core
  now has a spinner there where it had a yielder, and the startup check
  above says so; suffix the entry with `y`. The compact layout the embedded
  bench uses leaves `journal-disk` unpinned, so it now yields rather than
  spinning wherever the scheduler places it.
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

### Removed

- **`--yield-idle`.** It was shorthand for a `y` on every `--cores` entry,
  and a second spelling of the same layout needed rules of its own: it won
  over explicit suffixes, and on DPDK it had to treat the reader
  differently from what its entry said. `--cores` is now the only place a
  wait policy is stated. To migrate, suffix every pinned entry:
  `--yield-idle` with the default layout becomes
  `--cores journal-seq=1y,matching=2y,response=3y,reader=4y,event-publisher=6y,shadow=7y,repl-handler-0=8y,repl-handler-1=9y,journal-prep=10,journal-disk=11y`;
  on DPDK leave the `reader` entry bare, since the NIC poll thread cannot
  yield. `ServerConfig` loses the `yield_idle` field; code that built a
  shared-machine configuration sets `cores` to
  `PipelineCores::all_yielding()` of its layout instead.

### Fixed

- **An unpinned thread could run on a pinned thread's isolated core.**
  Threads the runtime starts unpinned from a pinned thread were handed
  every CPU, so on a host with isolated cores they began on their
  creator's isolated core and the scheduler never moved them off — beside
  a thread that may busy-spin there at real-time priority. The case that
  remained was a DPDK replica rebuilding its pipeline after a snapshot
  transfer, which starts every pipeline thread from its receiver thread.
  Unpinned threads now get the CPU set the process was started with,
  which excludes isolated cores and respects `taskset`, systemd's
  `CPUAffinity=` and container cpusets. For direct users of `melin-app`:
  `affinity::pin_thread` with core `0` now applies that set, and resets a
  real-time policy, where it used to leave the thread untouched;
  `affinity::capture_home_mask` records the set and belongs at the top of
  `main`, before anything pins a thread or initialises DPDK (the server
  runtime's entry points call it).
- **A node could drop a client's first reply.** The response stage learns
  of a new connection and of that connection's replies through two
  separate channels, and read them in the wrong order: a reply that
  arrived while the connection's registration was still queued was
  discarded, and the client waited out its read timeout while the node's
  heartbeats kept the socket alive. It took the stage's thread being
  descheduled across a narrow gap, so it showed on a fresh node under a
  loaded host — a shared or non-isolated response core — as an
  occasional lost first request, on both the kernel TCP and the DPDK
  transports. The stage now reads its replies before it applies
  registrations, which makes the order safe by construction, and a reply
  that still finds no connection is logged at debug level.

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

[Unreleased]: https://github.com/melin-engine/melin/compare/v0.19.0...HEAD
[0.19.0]: https://github.com/melin-engine/melin/releases/tag/v0.19.0
[0.18.0]: https://github.com/melin-engine/melin/releases/tag/v0.18.0
[0.17.0]: https://github.com/melin-engine/melin/releases/tag/v0.17.0
[0.16.0]: https://github.com/melin-engine/melin/releases/tag/v0.16.0
[0.15.0]: https://github.com/melin-engine/melin/releases/tag/v0.15.0
[0.14.0]: https://github.com/melin-engine/melin/releases/tag/v0.14.0
[0.13.0]: https://github.com/melin-engine/melin/releases/tag/v0.13.0
