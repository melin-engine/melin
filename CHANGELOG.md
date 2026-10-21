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

- **`melin_transport_core::NodeStore`**, the one resolver of a node's
  journal and snapshot paths, and **`ServerConfig::store()`**, which
  builds it from `--journal` and `--snapshot-path`. Every role that
  reads, writes, serves or archives the snapshot takes its path from it.
- **`melin_journal::segment::parent_dir` and `fsync_dir`**: the directory
  a file lives in (a bare name's is `.`), and a sync of a directory itself.
- **`melin_journal::segment::start_writeback` and `writeback_and_wait`**:
  `sync_file_range` over a range of a file, starting its writeback or
  waiting for it to finish, with the error returned and the length
  non-zero by type (`sync_file_range` reads a zero length as "to the end
  of the file").
- **`RequestId`**, in `melin-wire-protocol`'s `control` module (and
  re-exported from `melin_transport_core::pipeline`): a request's
  identifier, chosen by the client, which the ring slots now carry (see
  Changed). In `melin-transport-core`, `InputSlot::client` and
  `ClientOrigin` build a client request's slot whole, the way both of
  the node's readers now publish one.
- **Client protocol 1's pieces, in `melin-wire-protocol`** (see Changed):
  `control_codec::CLIENT_PROTOCOL_VERSION`; the `Refused` frame
  (`TransportResponse::Refused`, `TAG_REFUSED`, `decode_refused`) and
  `control::RefusalKind`, with `control_codec::Refused` (a decoded
  refusal), `BadRefusal` and `MAX_REFUSAL_REASON`; `REQUEST_ID_LEN`;
  `TAG_REQUEST`, `framing::split_request` (the one parser of a request:
  its ID and its body, or `NotARequest`), `framing::request_tag_and_id`
  and `REQUEST_TAG_AND_ID_LEN`; `control_codec::terminal_request_id`
  (the ID a final frame names); `control_codec::decode_challenge` (the
  one parser of the node's challenge, or `BadChallenge`), with
  `CHALLENGE_LEN`, `CHALLENGE_V0_LEN` and `CHALLENGE_NONCE_LEN`;
  `RequestId` is ordered (`PartialOrd`, `Ord`); `BATCH_END_FRAME_LEN` and
  `write_terminal_id`, for a stage that pre-encodes its batch
  terminators; `MAX_CONTROL_FRAME_LEN`; and `RETIRED_TAGS`, tags no frame
  may reuse. `melin-client` re-exports `RequestId`, `RefusalKind` and
  `CLIENT_PROTOCOL_VERSION`.
- **`melin_client::InFlight<T>`**: the requests in flight on one
  connection, each with a value of the caller's, found again by the ID a
  final frame names. The lookup the connections pair answers with, for a
  caller that keeps per-request state of its own (`melin-bench` keeps
  each request's send time and sequence in one).
- **`melin_app::affinity::first_allowed_cpu`**: the lowest CPU the
  calling thread may run on, a core that pinning to cannot fail on a
  restricted host.
- **Counters on `/metrics` for the requests a node answers without
  applying them, and for parked connections**, beside
  `melin_writes_refused_total`: `melin_requests_shed_total` (answered
  `ServerBusy` on a full input ring, the requests buffered behind each
  included), `melin_requests_refused_total` labelled
  `kind="permission_denied"` or `kind="malformed"` (refused by the
  application's decoder), `melin_requests_filtered_total` (filtered by
  it), the gauge `melin_connections_parked` (connections the node has
  stopped reading because it had no room to queue an answer for them)
  and `melin_connection_parks_total`. None of these requests is
  journaled, so the counters are their only trace; each is exported at
  zero on a node that has had none, and on a replica. In
  `melin-transport-core`'s `health` module, `IngressMetrics` holds them
  and `IngressTally` is one receive's count of them.
- **`melin-bench` reports shed requests**: a request the node answers
  `ServerBusy` is counted as shed (`shed: N requests` in the summary,
  `shed_requests` in the JSON results) instead of failing the run.
- **`shm-proxy --trace` pairs each request with the final frame that
  names it**, a busy or a refusal included, and reports requests it did
  not track and answers it could not pair beside the requests never
  answered.

### Changed

- **`--snapshot-path` is the node's one snapshot location**: read at
  boot by a primary and a replica, served to a replica that needs a
  resync, and written by the shadow stage. It defaults, as before, to the
  journal path with a `.snapshot` extension.
- **A node refuses to start on a snapshot location it cannot use**: when
  the snapshot path names no file or names a directory, when the
  snapshot's directory does not exist (or is not a directory), when the
  snapshot path is the journal path, or when `--snapshot-path` names
  nothing while a snapshot exists at the derived `<journal>.snapshot` and
  the node has no journal (move that snapshot to `--snapshot-path`).
- **A resync archives `<snapshot>.prev` with the snapshot.** The old
  history's `.prev` used to stay beside the new history's snapshot, where
  rolling the snapshot back to it would have restored a snapshot of
  another history. A snapshot in another directory than the journal is
  archived beside itself, in `<snapshot-name>.<reason>.<n>/`, with the
  same `<n>` as the journal's archive.
- **The library takes a `NodeStore`** (source-breaking):
  `archive_local_lineage`, `preflight_snapshot_transfer` and
  `snapshot_transfer_with` take the store instead of paths, and
  `archive_local_lineage` returns both archive directories
  (`ArchivedLineage`). In `melin_server_runtime::replication`,
  `run_receiver`, `run_receiver_dpdk` and `DpdkReplicationDriver::new`
  take the store in place of their journal and snapshot paths, and
  `Sender`'s `journal_path` field is now `store`.
- **`melin_transport_core::snapshot::save` returns the size it wrote**
  (`Result<u64, SnapshotError>`; it returned `Result<(), _>`).
  Source-breaking where the result was used as a `Result<(), _>` (matched
  on `Ok(())`, returned as a function's tail, or passed on as one);
  callers that apply `?` or discard the value are unaffected.
- **A snapshot save leaves at most 8 MiB of unwritten data in the page
  cache.** It writes in 4 MiB pieces, each sent to disk as soon as it is
  written, and waits for one piece to reach the disk before writing the
  one after next. A large snapshot used to sit in the page cache whole
  until its final `fsync`, which could carry the host past its
  dirty-page limit and make the kernel throttle every writer, the
  journal's thread included, for the length of the save. The final
  `fsync` remains the durability point.
- **The LAN benchmark suite takes snapshots in every run, on the OS
  disk.** Every node saves one every `SNAPSHOT_INTERVAL_MS` (10 s by
  default; `0` turns them off) to `SNAPSHOT_DIR`
  (`/var/lib/melin-bench/snapshots` by default), which the suite refuses
  to run with when it cannot show it is on another disk than the
  journal. A run passes only if every node saved enough snapshots during
  the load, all to its own path; each run records the verdict and the
  writes to both disks in `<transport>-<workload>.snapshots`, and the
  suite records its settings in `suite.conf`. Before, the nodes ran at
  the 50-minute default interval and no run ever took a snapshot. Node
  arguments carrying a snapshot flag are now refused, and the files of
  earlier runs are removed by exact name before each run, resync archive
  directories included. See [docs/benchmarking.md](docs/benchmarking.md).
- **A snapshot may be up to 1 TiB; the limit was 256 MiB.** A larger
  state could never be snapshotted: every save failed, logged at `ERROR`
  each interval, and recovery fell back to replaying the journal. The
  Exchange Core's state at 100,000 accounts is about 480 MB. A load still
  reads the snapshot whole, so the limit no longer bounds the memory a
  load takes.
- **The LAN bench suite waits 5 s between both replicas going live and
  the load** (`SETTLE_SECS`, which defaulted to 0). Full load on a
  replica that has only just caught up evicted it into a rejoin loop for
  the rest of the run, as on the Exchange Core; `SETTLE_SECS=0` restores
  the old behaviour.
- **The LAN bench suite calls a run whose replicas were evicted, missing
  or catching up `UNSTEADY`**, where it said `DEGRADED`, which read as
  degraded acks, a different thing.
- **`InputSlot` and `OutputSlot` carry a `request_id: RequestId`**,
  beside `connection_id`, in `melin-transport-core`. The matching stage
  copies an input slot's ID into every output slot of its event, so an
  event publisher sees it as it sees the connection. It is never
  journaled or replicated, and is `RequestId(0)` on node events and
  replicas; on a client's request it is the ID the request frame carries
  (see client protocol 1 below). Source-breaking for code that builds either slot with a
  struct literal: add the field, or build node events with
  `InputSlot::node_event` and client requests with `InputSlot::client`.
  A slot whose fixed fields grow by eight bytes can cross a cache-line
  boundary: an `InputSlot` (padded to whole lines) grows by a line when
  the application's event comes within eight bytes of one.
- **Client protocol 1: every request carries an ID, and is answered once,
  naming it.** Wire-breaking: upgrade clients with the nodes, admin tools
  and the shm-proxy's client included. The node's challenge carries the
  client protocol version ahead of its nonce; a client refuses a node of
  another protocol before it signs anything (`Error::ProtocolVersion`,
  which `Connection::connect_by` reports at once rather than retrying),
  and a client of an earlier release refuses the new challenge. A request
  is `[len][TAG_REQUEST][id: u64 LE][body]`; anything else after the
  handshake (an empty frame, another tag, a request too short for its
  ID) closes the connection, where it used to be dropped silently. Every
  frame that ends a request's reply names the request: `BatchEnd` and
  `BatchEndDegraded` carry its ID, and so does `ServerBusy`. A request the
  application's decoder refuses is answered with a new `Refused` frame
  carrying the refusal's kind (`PermissionDenied`, `Malformed`) and the
  decoder's reason, cut to 255 bytes (the text is for people, may change
  with any release, and reaches the authenticated client whose request
  it was); one it filters is answered with an empty batch. A request
  that finds the input ring full is answered `ServerBusy`, and the
  connection is kept (on DPDK too, which used to close it). The
  ordering contract: replies to requests the node accepted arrive in the
  order the requests were sent; a request it did not accept (shed,
  refused by the decoder, filtered, refused while halted) may be
  answered before requests sent ahead of it, but never in the middle of
  another request's reply (see below). A busy request was not applied,
  but requests sent after it may have been, so a client whose requests
  depend on each other's order stops on a busy, waits for the replies it
  is owed, and resends from the busy request on. The sizes:
  `REQUEST_HEADER_LEN` is 13, and
  `MAX_REQUEST_BODY` 1015 (the ID comes out of the body; the frame limit
  stays 1024), so an application that does not check its widest request
  at build time fails at run time for a body between 1016 and 1023 bytes.
  Source-breaking in `melin-wire-protocol`: `seal_request` and
  `frame_request` take the request's ID; `TransportResponse::BatchEnd`,
  `BatchEndDegraded` and `ServerBusy` carry a `RequestId`, and
  `TransportResponse` gains `Refused` (exhaustive, so a new arm in every
  match). In `melin-client`: `send` and `send_with` return the
  request's `RequestId` on every surface (`Connection`,
  `PolledConnection`, `StreamConnection` and the `Session` trait), each
  connection numbering its requests from 1; `Frame` and `Reply` are
  `Response`, `BatchEnd { id, ack }`, `ServerBusy { id }` and
  `Refused { id, kind, reason }`; `Error::Refused` and
  `Error::ProtocolVersion` are new variants of an exhaustive `Error`.
  The connections check every final frame against the requests
  outstanding: an answer for none, or a busy or refusal inside another
  request's reply, is `Error::Protocol`. The convenience calls
  (`request`, `request_batch`, `request_one`) refuse an answer for
  another request as `Error::Protocol`, and report a refusal and a busy
  as `Error::Refused` and `Error::ServerBusy`, which now leave the
  connection usable (`ServerBusy` means resend, no longer reconnect).
  `Error::NoReply` now means the node or the network stopped answering:
  a node no longer drops a request silently.
- **The shm-proxy's contract with the Aeron benchmark harness changes
  with the protocol**: the harness's Java rig frames each request with
  `TAG_REQUEST` and an ID, and reads the new final frames. The proxy's
  flags and its `connected to` and `error:` lines on stderr are
  unchanged; its `--trace` report gains two lines (see Added).
- **A request the node answers without applying it no longer waits for
  the replies to earlier requests, a request shed on a full input ring
  takes the requests buffered behind it with it, and a reader that
  cannot queue an answer parks the connection.** Every answer a reader
  decides on without publishing the request (a write refused while
  halted, a request the decoder refuses or filters, one shed on a full
  input ring) goes to the response stage through one queue and out at
  the first point between two requests' replies, even while the ack
  policy holds a write the same client sent before it: its final frame
  names it, so the client pairs it by ID. A refusal therefore no longer
  waits behind a held write. Replies to requests the node accepted still
  arrive in request order, and nothing falls inside another request's
  reply. Every complete request already received behind a shed one is
  answered `ServerBusy` too, read only as far as its ID, so nothing a
  client pipelined behind a shed request in the same burst is applied
  ahead of its resend; a request on a later receive may be. When the
  queue is full (the response stage has stopped taking answers),
  neither transport closes the connection, nor answers `ServerBusy` out
  of band as a full refusal queue did (`melin_writes_refused_total`'s
  help text no longer says so): it stops reading the connection,
  leaving the request and everything behind it unread, and retries
  until the queue has room (the kernel transport every millisecond, the
  DPDK transport on every poll), while the client's sends back up under
  TCP flow control. A parked connection is not dropped as idle, and on
  DPDK not for the bytes it holds either: one receive can take a
  connection past the 64 KiB a DPDK node otherwise lets it buffer before
  it parks, and those bytes are the node's backlog. A connection parked
  for over a second is degraded operation, reported at `warn` at most
  once a second while any stays parked. On a host that filters
  io_uring's cancel operation, a parked connection would go on being
  read by the kernel transport, so after a second it is closed instead,
  with a `warn` naming the cause. Shed, refused and filtered requests
  and parked connections are counted on `/metrics` (see Added).
- **`melin-server-runtime`'s refusal queue is the early-reply queue,
  and its readers count what they answer early.** Source-breaking. The
  queue moves out of the `halt` module into a new `early_reply` module:
  `halt::Refusal` is `early_reply::EarlyReply` (a connection, the
  request's ID and an `EarlyKind`: `Rejected`, `Filtered`, `Refused`,
  `Busy`), with no input sequence; `RefusalSender` and `RefusalQueue`
  are `EarlyReplySender` and `EarlyReplyQueue`;
  `refusal_channel(matching_progress)` is
  `early_reply_channel(capacity)`, taking the queue's capacity rather
  than the matching stage's progress (`EARLY_REPLY_QUEUE_CAPACITY`, the
  capacity a node gives it, is public); `RefusalQueue::idle_bound`,
  `sync` and the bounded `release` are gone, as the response stages read
  the queue through the crate's own types, which only send it between
  two requests' replies. `response::Response`'s `refusals` field is
  `early_replies`. `ControlEvent::PipelineBusy` is removed from the
  crate-root `ControlEvent`: a busy no longer rides the control channel.
  `reader::spawn_reader` and `dpdk_transport::run_dpdk_poll` take an
  `Arc<IngressMetrics>` after the early-reply sender, the counters they
  keep for the health endpoint. In `melin-transport-core`,
  `health::HealthState` gains an `ingress: Arc<IngressMetrics>` field,
  source-breaking for code that builds one with a struct literal.
- **`melin_transport_core::pipeline::StreamMarkQueue` is
  `Arc<StreamMarks>`**, no longer `Arc<Mutex<VecDeque<StreamMark>>>`.
  Source-breaking: build one with `StreamMarks::shared()`, queue a mark
  with `push`, and read what is queued with `queued()`. The queue keeps
  a lock-free count of its marks, so the journal stage can look for one
  on every idle iteration (see Fixed).

### Removed

- **`--snapshot`** (breaking). A command line that still carries it is
  refused at parse time. It was read once, at a primary's boot, while the
  shadow stage went on writing elsewhere. To migrate:
  - with `--snapshot-path` already set, drop `--snapshot` and leave the
    file at `--snapshot-path` alone: it is the freshest snapshot, and a
    `<journal>.snapshot`, if any, is a stale leftover;
  - with `--snapshot X` alone, the shadow stage kept writing the derived
    `<journal>.snapshot`, so that file, not X, is the freshest snapshot.
    If `--snapshot` was a one-off restore, drop it: the derived path is
    already the live location. To keep a custom location, move the
    current `<journal>.snapshot` there first, then set `--snapshot-path`;
  - to restore from a backup, copy it to the snapshot path.
- **`ServerConfig::snapshot` and `ServerConfig::shadow_snapshot_path`**.
  Use `ServerConfig::store()`.
- **`EngineError`**: `TransportResponse::EngineError`,
  `control_codec::TAG_ENGINE_ERROR` (its value, `0x03`, is retired, in
  `RETIRED_TAGS`), `melin_transport_core`'s `OutputPayload::EngineError`,
  and `melin-client`'s `Frame::EngineError`, `Reply::EngineError` and
  `Error::EngineError`. No node ever sent the frame, and the two client
  surfaces disagreed on whether it ended a request.

### Fixed

- **A journal write interrupted by a signal is retried** (part of
  finding 38 of the determinism audit). It used to fail with `EINTR`,
  as a journal I/O error.
- **A snapshot deferred under load is retried, not skipped for an
  interval.** The shadow stage saves only at a journal fsync boundary,
  and tried once per `--snapshot-interval-ms`: under sustained load it
  could miss the boundary at every tick and go several intervals without
  a snapshot, logging nothing. A due save is now attempted until it
  lands; a save still waiting at the next interval logs a warning, and a
  failed save is retried at the next interval rather than continuously.
  The `shadow snapshot saved` line now carries `bytes`, `elapsed_ms` and
  `attempts`, and `shadow snapshot failed` carries `attempts`.
- **A primary with `--snapshot-path` recovers from, and serves, its own
  snapshot** (the location half of findings 21 and 22 of the
  determinism audit). It used to ignore that snapshot at boot and when a
  replica needed a resync, and could serve a stale `<journal>.snapshot`.
- **A replica whose `--snapshot-path` is on another filesystem than its
  journal no longer fails every resync** with EXDEV: its snapshot is
  archived beside itself, on its own filesystem.
- **A re-seeded replica's temporary snapshot is `<snapshot>.tmp`**, the
  shadow stage's own temporary name, and its directory is synced once
  the received snapshot is in place.
- **`scripts/bench/server-setup.sh` tunes the journal disk after a
  reboot.** Its udev rule matched the disk by kernel name, which NVMe
  assigns in probe order: on a host with two identical drives the names
  swapped across the setup's own reboot, and the rule tuned the OS disk
  instead. It now matches the journal's filesystem UUID, applies the
  settings through the rule and reads them back, failing the setup when
  they did not land. `nr_requests` asks for 4, the kernel's minimum,
  rather than a 2 it silently raised.
- **The LAN bench suite runs under a locale with a decimal comma.** Its
  snapshot verdicts read and printed save times in the caller's number
  format: a save of 12.25 ms counted as 12, printed as `12,000`, and the
  closing summary failed to parse it, failing the suite. It now runs in
  the C locale throughout.
- **A request the application's decoder refuses or filters is answered**
  (S1 of the application API review). It used to be dropped without a
  reply, so on a pipelining connection every later reply was paired with
  the request before it.
- **A `ServerBusy` no longer overtakes replies to earlier requests, nor
  lands inside another request's reply** (finding 18 of the determinism
  audit). It went out of band, on the response stage's control channel,
  ahead of replies still in the output ring; it is now an early reply
  like a halt refusal, naming its request, sent only between two
  requests' replies.
- **A refusal no longer waits behind a write the ack policy holds**
  (finding 18 of the determinism audit). A halt refusal, and with request
  IDs every other answer a node gives without applying a request, waited
  for the replies to everything the client had sent before it; with
  degraded acks off, behind a write held for a replica that had gone, it
  waited until a replica came back or the operator swapped the policy.
- **A DPDK node keeps a connection whose request finds the input ring
  full**, answering it `ServerBusy` as the kernel transport does. It used
  to close the connection, throwing away the replies to writes already
  published.
- **Requests pipelined behind one shed on a full input ring are
  answered.** They waited unread until the client sent more bytes, which
  a client waiting for its replies never does, so they ended in its
  no-reply timeout. They are now shed with it (see Changed), on both
  transports.
- **A replica adopts the primary's last rotation promptly when the
  stream then goes quiet.** The rotation arrives after the entries it
  closes, so with nothing behind it only the journal stage's idle loop
  could apply it, and that looked once every 1024 idle iterations: on a
  busy core, where each idle iteration yields, that took tens of
  seconds. Nothing journaled was affected (an entry past the boundary
  forces the adoption first), and a replica restarted in that window
  adopts the rotation when it reconnects. A replica promoted in it,
  though, became a primary without the boundary, and every replica that
  had adopted it diverged at its first entry past it (chain values are
  scoped to their segment), each answered with an archive and a full
  re-seed. The idle loop now looks on every iteration, at the cost of
  one atomic load.

## [0.20.0] - 2026-10-19

### Added

- **`melin_ack_policy_journaled` and `melin_ack_policy_in_force`**, two
  `/metrics` gauges on a primary: the policy the newest journaled change
  names, and the policy replies are held to, each as the policy's wire
  value (`0` `disk`, `1` `disk+ram`, `2` `two-disks`, `3` `ram`). They
  differ while a change waits for the nodes the old policy relied on.
- **The pieces of journaled ack-policy changes, public for embedders and
  tools.** In `melin-journal`: `AckPolicyChange` (with `AckPolicyByte`
  and `JournalEvent::ack_policy_change`) and `codec::decode_event`, one
  decoder for every entry tag. In `melin-transport-core`: the `lineage`
  and `replica_presence` modules, `InputSlot::node_event` and
  `MatchingStage::forced_policy`. In `melin-server-runtime`: the
  `policy_control` module.
- **`--dpdk-client-rx-buf-kib`, `--dpdk-client-tx-buf-kib`,
  `--dpdk-client-tx-queue-kib`**: the per-connection buffering of the
  client connections a DPDK node accepts, previously fixed at 64 KiB of
  receive buffer, 16 KiB of send buffer and 64 KiB of queue. The send
  buffer is the in-flight window, so it caps one connection's response
  bandwidth at that much per round trip; the queue is the slack behind it
  before the connection is dropped as fallen behind. The old sizes dropped a
  single client at a million messages a second on the first round-trip
  hiccup, with nothing but "the server closed the connection" on the
  client's side, so the defaults rise: the send buffer to 64 KiB, which
  carries such a client with headroom, and the queue to 8 MiB, several
  milliseconds of its hiccups. The receive buffer stays at 64 KiB. A
  connection driven at the stack's full rate wants a larger send buffer
  still (256 KiB). Each client connection allocates its send and receive
  buffers up front, so they multiply by the number of clients; the queue
  is allocated only while a client is behind, up to the limit.

- **`melin-shm-proxy`** (`crates/tools/melin-shm-proxy`, the `shm-proxy`
  binary) — a sidecar that holds one connection to a node, over kernel TCP
  or, with `--features dpdk`, kernel bypass, and offers it to a client in
  another process through two shared-memory byte rings and a state word in
  a mapped file (`--shm`). For clients that cannot host DPDK themselves —
  a JVM, the Aeron benchmark harness's Java rig first among them — and
  arranged the way Aeron's own DPDK driver is: the client in one process,
  the NIC in another. It knows no application: nothing is framed or timed
  in between, the client keeps its own handshake and clock, and the one
  thing it reads of the bytes is the protocol's tags, for `--trace`, which
  times the loop and the round trip from a request reaching the stack to
  the batch-end that closes its reply. Whatever the client has written
  when the loop turns goes out as one segment, so the packet rate follows
  the backlog rather than the message rate. The layout is documented in
  `crates/tools/melin-shm-proxy/src/shm.rs`. Over DPDK it dials through
  `melin-dpdk`'s client NIC handle (below), with the node's `--dpdk-*`
  flags (and its own `--dpdk-port`, which `--dpdk-ports` replaces for a
  bond) and an EAL file prefix of its own by default, and a NIC it cannot
  bring up is reported with the node's own errors, which name the EAL file
  prefix, the port or the pool, and a dial that fails with the handle's: a
  timeout names the MAC the SYN went to and where it came from, the first
  thing to check. Its flags, the `connected to` line and the
  `error:` prefix on stderr, and the `--trace` report are what the Aeron
  harness scripts against, and are kept stable. On the wire, its
  gratuitous ARP request now zeroes the target hardware address, as the
  node's always has, where it sent the broadcast address; receivers ignore
  the field.
- **A polled connection in `melin-client`**, the blocking `Connection`'s
  protocol over a non-blocking byte stream, with no blocking call in it.
  - `ByteStream`, the seam to the transport: a non-blocking `send`, a
    `service` step at the caller's monotonic nanosecond time, a `recv` for
    which nothing yet is zero and a closed connection an error, and a
    `send_in_place` that lends the stream's transmit buffer so a request
    is encoded where it is sent from. The in-place step defaults to
    lending nothing, so a stream with no transmit buffer of its own
    implements the other three.
  - `PolledConnection`, the protocol state alone (the handshake, the
    receive buffer, a scratch buffer, the read deadline) with no stream
    and no clock: `poll` takes the stream and the caller's time and
    returns the next frame or nothing yet, `feed` takes bytes an I/O loop
    received itself (an `io_uring` completion's, one copy, as a
    `FrameDecoder` push is), and `send` and `send_with` frame a request in
    the room the stream lends, or in the scratch buffer when that room is
    short of it (a transmit ring about to wrap). The handshake runs inside
    `poll`, and a request sent before the node is ready is held until it
    is. The read deadline runs only while a reply is owed, a heartbeat
    does not push it back, and its expiry is `Error::NoReply`. For a
    caller with several connections on one NIC, or a clock of its own.
  - `StreamConnection`, one stream and a cached clock around it, with the
    blocking connection's `send`, `next_frame`, `request` and
    `request_one` (spinning on the stream where those block) and their
    errors, plus a non-blocking `poll`. `StreamConnection::connect` dials
    a kernel socket.
  - `Session`, the calls a request loop makes (`send`, `next_frame`,
    `request`, `request_one`), implemented by `Connection` and
    `StreamConnection`, so a loop is written once over `&mut dyn Session`
    and the transport picked at runtime. `Connection` is otherwise
    unchanged.
  - Two streams: `NonBlockingTcp`, a kernel socket in non-blocking mode
    with Nagle off, and `BufferStream`, whose received bytes the caller
    pushes in and whose sent bytes it takes out, with an optional transmit
    capacity, for an I/O loop that owns the socket itself.
- **`melin_dpdk::NicHandle`, a client's DPDK NIC** (`dpdk-sys` feature):
  the node's DPDK transport listening on no port, dialing TCP streams to a
  node and servicing all of them in one device poll, for clients that run
  several connections on the one DPDK port a host has. The public API is
  std types only: no smoltcp dependency is needed to use it.
  - `NicHandle::open` takes a `NicConfig`, the node's whole NIC surface
    (LACP bond, VLAN, bifurcated peer IP, the L3 mode's gateway MAC, peer
    MAC, MTU), the same type a node's `DpdkConfig` holds. It initialises
    EAL with a file prefix of its own (`melin-client`) unless the
    arguments name one, so a client beside a node takes no runtime lock
    the node holds, and an EAL failure names the prefix; or it shares the
    process-wide EAL, as a node does.
  - `dial` takes a deadline and the caller's clock, gives every socket a
    fresh local port, and tells a refusal (`ConnectionRefused`) from a
    timeout (`TimedOut`, naming the MAC the SYN went to as the first
    thing to check). Dials are sequential: a DPDK node takes only a few
    handshakes at once on a port and refuses a SYN beyond them, so `dial`
    returns only once its connection is established or has failed.
  - `stream(&id)` lends the stream a `StreamId` names as a `NicStream`
    view, for one call: a non-blocking `send` with a `can_send` room
    query, an in-place `send_in_place` that lends the transmit buffer's
    contiguous free region, a `recv` for which a closed connection is an
    error, never a zero, and the handle's `service` step. `service`
    advances every stream at once, at the caller's monotonic nanosecond
    time; `NicClock` (`clock()`, with its `origin`) is that time for a
    caller without a clock of its own. A closed stream, and every stream of
    a dropped handle, is reset so the node frees its slot at once.
- **A `dpdk` feature in `melin-client`**, off by default: the polled
  connection over kernel bypass, on the client NIC handle. Turning it on
  links `melin-dpdk`, which is BUSL-1.1, into the binary (`melin-client`
  itself stays Apache-2.0, and without the feature nothing of
  `melin-dpdk` is compiled), and needs libdpdk and libclang on the build
  host. A NIC that cannot be brought up is the new `Error::Nic` (see
  Changed). `docs/building-an-application.md` covers what a DPDK client
  needs of its host.
  - `StreamConnection::connect_dpdk` brings a NIC up from a `NicConfig`
    and dials and authenticates, with a socket buffer size and a timeout
    that bounds the dial, the handshake and every wait after;
    `StreamConnection::connect_dpdk_on` does the same on a `NicHandle`
    already up, owned or lent as `&mut`, so a client reconnects without
    touching EAL and a process that holds the process-wide EAL shares it.
    One clock drives the handle's stack: the dial runs on the handle's own
    clock, and the connection's cached clock counts from the same origin.
    A failed dial is `Error::Connect`, its source the handle's error
    unchanged, so a timeout and a refusal stay apart.
  - `DpdkStream`, one stream of a NIC handle owned with the handle (or a
    `&mut` to it): the stream a `StreamConnection` over DPDK owns. Dropping
    it closes the stream, so the node frees the connection's slot at once.
  - `ByteStream` for `DpdkStream` and for the handle's `NicStream` view,
    whose in-place send is the socket's own, so a request is framed
    straight into the stack's transmit buffer. A program with several
    connections on one NIC keeps the handle and drives one
    `PolledConnection` per stream with the view, per call, on its own
    clock, calling the handle's service step once per loop iteration.
  - Re-exported from `melin-dpdk`, so a client needs no direct dependency
    on it: `NicConfig`, the node's DPDK configuration (one field per
    `--dpdk-*` flag; a plain struct, which the flags, `DpdkArgs` below,
    map onto, and the fields' documentation says why a peer MAC is required on
    a fabric that assigns MACs, an AWS ENI, and why a client beside a node
    needs an EAL file prefix of its own), `NicHandle`, `NicStream`,
    `StreamId`, `NicClock`, `PeerMacSource`, `try_parse_mac` and
    `MacParseError`.
- **`melin_dpdk::fmt_mac`, `NextHop`, `NicHandle::next_hop` and
  `NicHandle::connect_line`**: a MAC as `aa:bb:cc:dd:ee:ff`; where a
  dial's first frame goes (the server's MAC or the gateway's, with where
  it came from) as one value with a `Display` and a `warning` for a MAC
  that was derived rather than given; and the connect line a binary
  prints before dialing, composed by the handle. The shm-proxy and the
  echo client print it, unchanged in text, and a client of the handle can
  do the same rather than keep a copy. Behind a gateway, a server on the
  NIC's own network is dialed directly, as the stack routes it, so the
  handle seeds the server's MAC on every dial whatever the mode and the
  next hop names the MAC the dial really goes to. EAL arguments ending in
  a `--file-prefix` with no prefix are refused rather than defaulted
  past. Re-exported by `melin-client`'s `dpdk` feature.
- **`melin_dpdk::DpdkArgs`, the `--dpdk-*` flags declared once**, behind
  a new `clap` feature of `melin-dpdk` (off by default; like `NicConfig`
  it needs no libdpdk and no `dpdk-sys`). A `clap::Args` struct a binary
  flattens into its command line, with the node's flag names, help and
  defaults (`--dpdk-eal-args` in the joined form only, `--dpdk-ports 0`,
  `--dpdk-prefix-len 24`, `--dpdk-mtu 1500`, the rest unset; the
  addresses typed as IPv4), and the one mapping onto a `NicConfig`:
  `nic_config` where `--dpdk-ip` is required, `nic_config_or` where the
  binary has an address to assume, both validating with one wording
  (`invalid --dpdk-peer-mac: ...` naming the flag and the value,
  `--dpdk-ip is required`) as a `DpdkArgsError`. `--dpdk-gateway-mac`
  requires `--dpdk-gateway`. `first_given` names the first flag set away
  from its default, for a binary that refuses the flags on a run that
  would ignore them. Its `Default` is an empty command line's. What an
  operator has to know about each flag stays on `NicConfig`'s fields.
- **`echo-client --transport dpdk` and `--core`**: the echo example's
  client measures the sequencer's latency floor over kernel bypass, in a
  build with the example's `dpdk` feature, with the node's `--dpdk-*`
  flags (`--dpdk-eal-args`, `--dpdk-ports`, `--dpdk-ip`,
  `--dpdk-prefix-len`, `--dpdk-gateway`, `--dpdk-peer-ip`,
  `--dpdk-gateway-mac`, `--dpdk-peer-mac`, `--dpdk-mtu`, `--dpdk-vlan`).
  Both transports run one measurement loop and print the same result
  lines; the kernel transport, still the default, behaves and prints as
  before. A DPDK run prints the MAC it dials the server by on stderr, and
  warns when that MAC was derived rather than given. Without the feature,
  `--transport dpdk` is refused with a message naming it. `--core` pins
  the measuring thread to a core once the connection is up, for either
  transport: a DPDK client spins at full load, so its figures want a core
  of its own.
- **DPDK clients in the DPDK test rig.** `scripts/dpdk/netns-runner.sh`
  builds a veth pair for a DPDK client beside the node slots and publishes
  it in `MELIN_NETNS_DPDK_CLIENT_IFACE`, `_IP` and `_MAC` (the node slots
  and the kernel client's variables are unchanged); `melin-test-node`'s
  `open_dpdk_client` opens a `NicHandle` on it, on the process-wide EAL;
  the echo and counter round trips gain a case with the client on DPDK;
  the CI `dpdk` job builds, tests and documents `melin-client/dpdk`.
  `scripts/dpdk/dpdk-client-tap-smoke.sh` runs a DPDK echo client over a
  TAP device against a kernel echo server. For contributors; nothing ships
  with it. The `shm-proxy` binary's DPDK path is tested there too, end to
  end, with `melin-test-node`'s `dpdk_client_process_eal_args` putting it
  on the client pair in a process of its own.
- **A LAN benchmark suite and a load generator.**
  `scripts/bench/lan-bench-suite.sh` runs a primary, two replicas and a
  load-generating host through a transport × workload matrix (kernel TCP
  or DPDK for clients and replication alike; throughput or a single
  request in flight), for the echo and notary examples or the Exchange
  Core. It builds every binary on the machine running it, for the bench
  hosts' CPU and against a sysroot taken from them, and copies them
  over; each run starts from fresh journals, keeps every node's log, and
  a replica evicted, missing or catching up during it fails the suite.
  `scripts/bench/server-setup.sh` and `server-deploy.sh` prepare the
  hosts; `scripts/bench/test-containers.sh` rehearses the whole suite
  against podman containers. The load generator is the new `melin-bench` tool
  (`crates/tools/melin-bench`, unpublished): clients × window in flight
  from one pinned loop, over kernel TCP or, with `--features dpdk`, one
  DPDK NIC, writing its throughput and latency percentiles as JSON. See
  [docs/benchmarking.md](docs/benchmarking.md).
- **`melin_dpdk::dial_addr`** (with `dpdk-sys`; re-exported by
  `melin-client`'s `dpdk` feature): the address check a DPDK dial needs before a NIC comes up,
  IPv4 and not loopback, with the reason to print. The echo client, the
  `shm-proxy` and `melin-bench` share it.
- **`ByteStream` is implemented for `&mut S`** where `S: ByteStream`, so
  a caller holding its streams in a collection lends one per call.
- **The primary warns when a replica accepts entries but persists none.**
  A replica whose journal stops advancing while it keeps taking entries
  still satisfies an ack policy that counts in-memory confirmations, and
  used to go unnoticed until its input ring filled and it was evicted for
  ring backpressure. After 5 s of entries pending with nothing persisted,
  the primary now logs a warning naming the replica's slot and both
  positions, and logs again when it persists once more.
- **`melin_app::affinity::spawn_placed` and `with_child_context`**: the
  one way to spawn a thread that has a placement (a core, or `0` for
  unpinned). The thread is created already in that placement's
  scheduling context, its spawner is put back where it was, CPU
  included, and `spawn_placed` returns once the thread has pinned itself,
  or a `SpawnPlacedError` naming the thread and its core when it has not
  within 10 s. `with_child_context` runs the same handover around a spawn
  of the caller's own, such as a scoped thread.
- **`melin_transport_core::replication::catchup::handoff_pass_from_journal`**:
  one journal pass of a handoff into live streaming, which both senders
  run. It streams as `catch_up_from_journal_with` does, and reports a
  start point lost to a prune as an error rather than a snapshot verdict.
- **Degraded acks on a halted primary.** A primary that has lost its
  last replica no longer goes silent on the replies it holds. Once the
  halt has lasted `--degraded-ack-grace-ms` (2 seconds by default), each
  held reply — to a write accepted before the halt, or to a query — is
  sent as soon as the primary's own journal has the event fsynced,
  ending in the new `BatchEndDegraded` frame: backed by the primary's
  disk alone, weaker than the ack policy, and marked so. The rejections
  and queries queued behind them are answered too, in order; a
  rejection always ends in a plain `BatchEnd`. A replica that returns
  within the grace period confirms everything in full; under `disk`
  nothing is ever degraded. The policy itself never changes, and
  `melin_ack_policy_degraded` stays `1`; a journaled policy change
  released this way is applied only once the policy before it confirms
  it, as any change is. `melin_degraded_acks_total` counts degraded
  acks, and a `warn` marks the start and end of each stretch.
  `--no-degraded-acks` keeps the previous behaviour: every held reply,
  and everything behind it, waits for a replica or a policy swap. See
  "Degraded acks" in `docs/replication.md`, and the source-breaking side
  under Changed.

### Changed

- **The ack policy is journaled: replication protocol 8, snapshot
  framing 4.** A change of policy is a journal entry (tag `0x05`,
  `[policy: u8][flags: u8]`), sequenced and replicated in order with the
  events it governs and never delivered to the application, like an
  epoch bump. The response gate switches at the change's place in the
  order, once the change's own entry is durable under the policy before
  it, so a weakening waits for the nodes the old policy relied on, and
  every reply after the change, the rest of an in-flight batch included,
  is held to the new policy; two pending changes are applied one by
  one, never merged.
  Snapshots record the policy in force at their sequence (framing
  version 4 adds, after `genesis_entries`, a flags byte, the policy, the
  policy before the change that put it in force and that change's
  sequence, so a primary starting from the snapshot can apply the change
  again; versions 1 to 3 are still read, and a history that records neither
  the genesis length nor a policy still writes version 2). Replicas
  judge auto-promotion by the newest change their journal holds, falling
  back to their own requested policy only while the history holds none.
  `StreamStart` and `Heartbeat` no longer carry the policy. Not backward
  compatible: nodes on protocol 7 and 8 refuse each other at the
  handshake, so stop the cluster, upgrade every node, and restart on the
  existing files; an older release refuses to start on a journal holding
  a policy change or on a version-4 snapshot, rather than discard
  anything. See "The policy is part of the journal" and "Upgrade and
  rollback notes" in `docs/replication.md`.
- **Operator-visible: how an ack-policy swap behaves.**
  - `ACK-POLICY` on a serving primary replies `OK` once the change is
    journaled, which can be before it is in force. It replies
    `ERR ACK-POLICY not journaled: ...` if the change cannot be journaled,
    naming why: the input ring stayed full for 2 s, the node stopped
    serving as primary, or another `ACK-POLICY` request is still
    waiting. On a replica,
    or a primary still starting, it replies `OK staged`, and the policy
    is journaled when the node serves as primary, unless its history
    already holds it. Every command a serving primary takes is
    journaled, repeats of the policy in force included.
  - On a node without `--replication-bind` (a `--standalone` node, or a
    replica that would be promoted without one), `ACK-POLICY` naming a
    policy that needs a replica (`ram`, `disk+ram`, `two-disks`) is
    refused in every role with `ERR ACK-POLICY not journaled: ...`
    naming the cause, logged at `warn!`, and nothing is journaled or
    staged. It used to be journaled and take effect, holding every reply
    from then on, since no replica could ever attach to meet it.
  - `ACK-POLICY disk` sent while no replica is connected is
    *forced*: journaled as unwitnessed, in force once its own entry is on
    the primary's disk (releasing replies held under the old policy),
    lifting the write halt until a replica streaming live acknowledges
    an entry or a policy that needs one is journaled, and logged at
    `warn!`. A replica that connects and leaves before then (one
    refusing the primary's history, retrying, or one leaving during its
    catch-up, or a caught-up one that joins and leaves while no write
    reaches it) leaves the halt lifted: set a policy that needs a
    replica to end the override once replicas are back. The lifted halt
    does not survive a restart: re-send `ACK-POLICY disk` once the node
    serves. A forced swap is not witnessed by a replica that was
    offline; `docs/replication.md` states the limitation and the
    operational rule, and how to force a swap past a connected replica
    that never acknowledges.
  - A replica counts toward the write halt, the forced-swap rule and
    `melin_replicas_connected` once the primary accepts its handshake,
    no longer as soon as it authenticates: one the primary refuses after
    authentication (a handshake it cannot validate, or one showing the
    primary superseded) never lifts the halt.
  - A policy staged on a replica before `PROMOTE` is journaled by the
    promotion but never forced: it no longer resumes replies on a
    promoted node with no replica. Send `ACK-POLICY disk` again once the
    node serves.
  - `--ack-policy` is the policy a primary *requests*. A history that
    records another policy keeps serving under it until the change to
    the requested one, journaled at startup or promotion, takes effect.
    A node without `--replication-bind` serves under the requested
    policy at once, and journals it unwitnessed when the history already
    records it through a change that may still have been waiting for a
    replica (any change after the first, journaled witnessed), so the
    journal marks that it took effect without one; once, not on every
    restart.
  - A replicated primary's restart, and a promotion, confirm the
    history's newest change again before acknowledging anything under
    it: the node serves under the policy before that change until the
    change's entry is held by the copies that policy demands (a forced
    change: until it is on the node's disk), since whether it had taken
    effect before the stop is not recorded. A `disk+ram` → `disk` swap
    the replica never received therefore keeps a primary restarted
    under `disk` on `disk+ram` until the replica holds it.
    `melin_ack_policy_in_force` reports the policy before the change
    until then. A repeat of the policy in force is not the change
    confirmed; a forced repeat marks it forced.
  - The info-level "ack policy changed by operator" log is replaced by
    "ack policy change requested by operator" (or "staged"), "ack policy
    change journaled", and "ack policy switched" (with `prev`, `next`
    and the cause) when it takes effect; a change repeating the policy
    in force switches nothing and logs no switch.
- **Source-breaking API changes for journaled ack-policy changes.**
  - `melin_journal::JournalEvent` has a new variant,
    `AckPolicyChange(AckPolicyChange)`, and
    `melin_transport_core::pipeline::OutputPayload` one of the same name,
    emitted at the change's place in the output ring. Add an arm to an
    exhaustive `match` on either: an event publisher sees the new output
    slot and should skip it (it carries no reply).
  - `melin_transport_core::snapshot::load` returns `(A, SnapshotHeader)`
    (it returned a tuple of state, sequence, chain hash and epoch), and
    `load_with_header` is gone; `SnapshotHeader::epoch` became
    `lineage: Lineage`; `snapshot::save` and `shadow::run` take a
    `Lineage` where they took an epoch, and `shadow::run` the wire
    sequence of the first entry it will consume; `JournaledApp::from_parts`
    takes a `Lineage`, and `recovered_epoch` became `recovered_lineage`.
    `Lineage::ack_policy` is a `LineagePolicy` (the policy, and the
    change that put it in force, at a `WireSeq`, with the policy before
    it).
  - `replication::protocol::PrimaryMessage::StreamStart` and `Heartbeat`
    lost their `ack_policy` field, and `encode_stream_start`,
    `encode_heartbeat` and `replication::catchup::snapshot_transfer_with`
    their `ack_policy` argument.
  - `melin_server_runtime::replication::ReplicaControlPlane` lost
    `primary_ack_policy` and gained crate-private fields, so it can no
    longer be built with a struct literal (use `ReplicaControlPlane::new()`
    or `Default`); `replication::Sender` lost `ack_policy`.
  - `melin_server_runtime::response::Response` and
    `dpdk_response::run` take `policy: GatePolicy` (a `GateStart`, the
    policy in force and the lineage's change still to apply, which
    outside the runtime can only be built settled with
    `GateStart::settled`, and the forced change reader) in place of the
    `ack_policy` atomic, and
    `admin::spawn` takes the `policy_control::AckPolicyControl` mailbox
    in place of the atomic. `reader::spawn_reader` takes a
    `policy_control::BootedClaim` (a `PublisherClaim` whose boot entry is
    journaled: `PublisherClaim::journal_boot` consumes the claim and
    returns it, and only it attaches a producer), and
    `dpdk_transport::run_dpdk_poll` an `AckPolicyPublisher`: the input
    ring's producer journals the `ACK-POLICY` requests. The mailbox, the
    claims and the publisher are built only by the server's start-up
    (`AckPolicyControl::new`, `claim` and `journal_boot` are
    crate-private), so outside the crate `admin::spawn`
    takes `None` and these two entry points are driven by the server
    alone; there is no outside migration path.
  - The replica count is a `melin_transport_core::replica_presence::ReplicaPresence`
    (count and halt override in one type) where it was an
    `Arc<AtomicU32>`: `Pipeline::replicas_connected`,
    `health::HealthState::replicas_connected`, `halt::HaltGate::new` and
    `replication::run_sender` change type. `ReplicaPresence::lift`
    returns a `FirstAck`, through which a sender reports the replica's
    acks (`FirstAck::acked`): the first one ends the halt override.
    `ReplicaPresence::lower` returns a `Lowered`: whether the replica
    was the last, and if so whether the node halts or a forced `disk`
    keeps it taking writes, which its warning states. The override
    changes only on evidence from `ReplicaPresence::observe`:
    `latch_override` takes its `NoneSince`, and `clear_override` the
    `Observed` itself (neither is `Copy` nor `Clone`).
    `Observed::Connected` carries a `Seen` token that only `observe`
    makes, as `NoneConnected` carries `NoneSince`; match it as
    `Observed::Connected(_)`.
  - `pipeline::StageUtilization` has two new public fields,
    `ack_policy_in_force` and `ack_policy_journaled`.
- **`melin_journal::replication::ReplicationConsumer` reads by
  `peek`.** `try_read`, `pending` and `commit` are replaced by `peek`,
  which lends the next batch as a `ReplicationBatch` without consuming
  it, and `ReplicationBatch::commit`, which consumes it. A batch dropped
  uncommitted is peeked again, and no second batch can be peeked while
  one is held: the borrow checker refuses it, where `try_read` read past
  an uncommitted batch in release builds. Source-breaking for every
  reader of the ring:
  `let Some(batch) = consumer.peek()`, then `batch.meta()`,
  `batch.data()` and `batch.commit()`.

- **`melin_dpdk::DpdkConfig` has a new field, `client_buffers:
  SocketBuffers`**, the sizes behind the `--dpdk-client-*-kib` flags.
  Source-breaking for a `DpdkConfig` built field by field: add it, or end
  the literal with `..DpdkConfig::default()`. A promoted replica's client
  listener now takes them too, through `DpdkTransport::add_client_listener`.
- **`melin_client::Error` has a new variant, `Error::Nic(io::Error)`**,
  for a DPDK NIC that cannot be brought up (EAL initialisation, a missing
  port, the mbuf pool, the port's configuration). Only the `dpdk`
  feature's constructors return it, but the variant exists in every build,
  since features are unified across a build graph and a variant that came
  and went with the feature would break a match in one crate when another
  turned it on. Source-breaking for an exhaustive `match` on `Error`: add
  an arm for it.
- **`DpdkTransport::connect_to` and `connect_to_with_buffers` return a
  `Result`** instead of panicking when the stack refuses to open the
  socket (an endpoint it cannot address). A replica whose dial is refused
  that way now logs it and retries with its usual backoff, where it used
  to go down. Source-breaking for a caller of either: handle the error.
- **`melin_dpdk::DpdkConfig`'s NIC fields moved into `melin_dpdk::NicConfig`**,
  the configuration a node and a client's NIC handle share. The EAL
  arguments, ports, address and prefix, gateway, bifurcated peer IP,
  gateway and peer MACs, MTU and VLAN id are now fields of
  `DpdkConfig::nic`; `DpdkConfig` keeps `listen_port` and `num_queues`.
  Names, types and defaults are unchanged, and so is what the node does
  with them. Source-breaking for anything that constructs a `DpdkConfig`
  or reads its NIC fields: `config.peer_mac` becomes
  `config.nic.peer_mac`. Both types are now available without the
  `dpdk-sys` feature, so a binary can map its flags onto them in any
  build. The ARP frames the transport sends and injects are built by the
  new `melin_dpdk::arp` module, unchanged on the wire.
- **`ServerConfig`'s DPDK fields moved under `ServerConfig::dpdk`**, a
  `melin_dpdk::DpdkArgs` (re-exported as
  `melin_server_runtime::server::DpdkArgs`), flattened into the node's
  command line. Source-breaking for code that builds or reads a
  `ServerConfig` by its DPDK fields: `dpdk_peer_mac` becomes
  `dpdk.peer_mac`, likewise `dpdk.eal_args`, `dpdk.ports`, `dpdk.ip`,
  `dpdk.prefix_len`, `dpdk.gateway`, `dpdk.peer_ip`, `dpdk.gateway_mac`,
  `dpdk.mtu` and `dpdk.vlan`; `dpdk.ip`, `dpdk.gateway` and
  `dpdk.peer_ip` are now `Ipv4Addr`s rather than strings, and `dpdk.ip`
  an `Option` (the node assumes 10.0.0.1 without one, as before). A
  struct literal ending in `..ServerConfig::default()` that sets none of
  them, and a command line flattening `ServerConfig`, are unaffected. The
  node's command line is unchanged in flags and defaults (its
  `--dpdk-peer-mac` help is shorter, the full explanation now living on
  `NicConfig::peer_mac`); a malformed
  address is now refused by the parser rather than at startup, and
  `--dpdk-gateway-mac` without `--dpdk-gateway`, which nothing used, is
  refused. `melin-server-runtime` now depends on `melin-dpdk` in every
  build, not only with `dpdk` (without `dpdk-sys` that crate needs no
  libdpdk).

  The shm-proxy and the echo client take the same flags from it, and so
  the node's validation and error wording: a malformed MAC is reported as
  `invalid --dpdk-peer-mac: ...` (was `--dpdk-peer-mac: ...`), and a
  missing address as `--dpdk-ip is required` (was `... with --transport
  dpdk`); the `error:` prefix and the exit status are unchanged. Their
  `--dpdk-*` help is now the node's text. The proxy keeps its flag names
  and defaults (its EAL file prefix, `--dpdk-port 0`, and `--dpdk-ports`
  in conflict with it), its `link at` and `connected to` lines, and its
  `--trace` report. The echo client keeps refusing a `--dpdk-*` flag on a
  kernel run and a loopback `--server` on a DPDK one.
- **A DPDK node's NIC setup errors name their port and pool.** A port
  that fails to configure, start or take its steering rule is reported as
  `port <id> configure: ...`, `port <id> start: ...` or `port <id>
  steering rule for <peer>: ...`, and a pool that cannot be created as
  `mempool <name>: ...`, where they used to give the DPDK call alone: on
  an LACP bond, which member failed was not said.
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
- **`melin-dpdk`'s C wrapper is compiled for the target, not the build
  machine.** The x86 ISA baseline applies only when building for x86_64,
  and the CPU is the one rustc builds for (`-C target-cpu`), so a cross
  build compiles the wrapper for the hosts that will run it rather than
  dropping the build machine's CPU name.
- **The `shm-proxy` refuses a loopback `--server` on DPDK** before
  bringing the NIC up, as the echo client does; a NIC never reaches
  loopback, and the dial used to wait out its timeout instead.
- **`scripts/dpdk/dpdk-setup.sh` writes `DPDK_MAC`** in every mode (the
  address a peer sends to), and `VLAN_ID` on the mlx5 path when `--vlan`
  is given.
- **`melin_app::affinity::capture_home_mask` leaves out isolated CPUs and
  moves its caller.** The home mask, the CPU set an unpinned thread runs
  on, is the startup mask without the CPUs the kernel reports isolated,
  unless those are all the process was given, and `capture_home_mask`
  now also narrows the calling thread to it, so the threads it spawns
  start off the isolated CPUs too. It only ever narrows, never widens.
- **Degraded acks change the response stages' and the config's shape**
  (see Added). `ServerConfig` gains `no_degraded_acks` and
  `degraded_ack_grace_ms`, `StageUtilization` gains `degraded_acks`,
  and `response::Response` gains `degraded_release`, a
  `halt::DegradedRelease` that `dpdk_response::run` takes too (`None`
  keeps the previous stall). Source-breaking for code that builds a
  `Response`, or a `ServerConfig` or `StageUtilization` without
  `..Default::default()`. `ReplicaPresence` records when its count last
  dropped to zero, and `ReplicaPresence::last_loss` reads it:
  `Some(LastLoss)` while none streams (the instant, or `Unknown` when
  the stamp was cleared), `None` otherwise.
- **A reply batch says what backs it: a new `BatchEndDegraded` frame,
  and `melin_client`'s `Frame::BatchEnd(Ack)` / `Reply::BatchEnd(Ack)`.**
  The wire protocol gains `TAG_BATCH_END_DEGRADED` (`0x0A`) and
  `TransportResponse::BatchEndDegraded`: the end of a reply backed by the
  primary's own disk alone, weaker than the ack policy, which a primary
  halted for want of a replica sends (see Added). In `melin-client`,
  `Frame::BatchEnd` and `Reply::BatchEnd` carry an `Ack`, `Policy` or
  `PrimaryOnly`, so every caller that matches a batch end decides what a
  degraded one means to it; `request_batch`, new on `Connection`,
  `StreamConnection` and `Session`, returns a reply's frames with their
  `Ack`; and `request` and `request_one` take only a full reply,
  reporting a degraded one as the new `Error::Degraded`, which carries
  its frames. Source-breaking for every `match` on `Frame`, `Reply`,
  `Error` or `TransportResponse`. Wire-breaking by design: a
  client built against an older `melin-client` gets `Error::Protocol` on
  the first degraded reply rather than reading it as a full ack, so
  upgrade clients before nodes.

### Removed

- **`melin_server_runtime::ack_policy::ACK_POLICY_UNKNOWN`** and
  **`melin_transport_core::replication_wire`'s `SLOT_TAG_TICK`,
  `SLOT_TAG_EPOCH_BUMP` and `SLOT_TAG_APP`**. Replicas no longer hold
  a policy byte that can be unknown, and a replicated slot is decoded by
  the journal's own codec (`melin_journal::codec::decode_event`), so the
  tags are defined once.
- **`melin_app::affinity::take_context`, `prepare_child_context`,
  `restore_context` and `SchedContext`** are private. They were the
  steps of the handover that `spawn_placed` and `with_child_context` now
  run; done by hand they were done partly, which is how pipeline threads
  came to start on a core a real-time thread held. Spawn through either
  function instead.
- **`melin_dpdk::DpdkTransport::add_listener`**. It listened with the
  client port's fixed sizes, which is how a promoted replica's client port
  came to ignore the `--dpdk-client-*-kib` flags. Use
  `add_client_listener` for the client port, and
  `add_listener_with_buffers` for any other.

### Fixed

- **A runtime ack-policy swap now reaches replicas while data flows**
  (finding 10 of the determinism audit). Replicas learned the primary's
  policy only from `StreamStart` and idle heartbeats, and journaled ticks
  kept the link busy, so after the documented `ACK-POLICY disk` lever
  they kept believing the old policy: if the primary then died,
  auto-promotion skipped its `disk` refusal and lost events acknowledged
  on the primary's disk alone; after a swap back to `disk+ram`,
  auto-failover refused indefinitely. A replica disconnected during a
  swap could never be told at all. See Changed (the policy is
  journaled); the audit's reproductions are regression tests, on kernel
  TCP and DPDK.
- **Tightening the ack policy no longer lags the in-flight batch**
  (finding 29). The rest of a response batch was released against a
  durable position cached under the old, weaker policy. See Changed.
- **`ACK-POLICY disk` lifts the write halt, as the manual-promotion
  playbook says.** The swap the playbook prescribed released replies
  held at the gate but never lifted the halt, so a promoted node with no
  replica had no way to resume writes. See Changed (the forced swap).
- **A DPDK primary no longer skips a batch when a replica's socket is
  full.** A streaming replica whose socket had no room for the next batch
  left it read but unsent, and the next tick read past it: a release
  build never sent that batch (a skipped entry batch cost the replica a
  reconnect, a skipped rotation left its journal misframed, a skipped
  chain check went unchecked), and a debug build panicked the primary's
  poll thread. The batch is now offered again until it is sent, and the
  ring's reader makes reading past an unconsumed batch impossible (see
  Changed).
- **A DPDK node no longer refuses a client that connects right behind
  another.** Each port had one listening socket, replaced only once its
  connection was established, and the stack answers a SYN that finds no
  socket listening with a reset: a SYN that arrived during another
  connection's handshake, or in the same poll as its last ACK, was
  refused, so clients dialing in turn were refused intermittently and
  clients reconnecting together after a failover more often. Each port
  now keeps a backlog of listening sockets and replaces each one as it
  is accepted, so it takes several handshakes at once, as a kernel's
  listen backlog does; only a burst beyond it is refused. A handshake
  whose peer vanished after its SYN now times out and frees its socket
  for the next client, rather than holding it until a restart. A full socket
  set now shrinks the backlog instead of accepting a connection on a
  slot beyond the node's per-connection tables, which could panic its
  poll thread, and a connection its peer closed or reset in the poll
  that completed its handshake no longer leaves its port without a
  listener for good.
- **A DPDK node frees a client's slot when the client closes.** The
  client's FIN left the node's socket half-closed, which its stack still
  counts as active, so the connection and its slot were held until the
  next heartbeat was answered with a reset, until the idle timeout with
  heartbeats off, or for good with both off; a node at `--max-connections`
  turned new clients away meanwhile. The node now takes a drained,
  half-closed socket as the end of the connection, as kernel TCP takes
  EOF. The divergence entry is closed and its test flipped.
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
- **Pipeline threads could start on an isolated core held by a real-time
  thread, and never run.** On a host with `isolcpus`, real-time
  throttling off and the node running with `CAP_SYS_NICE`, a replica's
  sequencing, matching, drain and shadow threads could be created on the
  journal disk thread's core and wait there forever behind it. The
  replica kept acknowledging until its input ring filled, then stalled
  and was evicted, without logging anything. Every thread with a core
  assignment is now created on that core, and the thread that spawns it
  is put back where it was, CPU included, and a pipeline thread that
  still does not start within 10 s fails the node's startup with an
  error naming the thread and its core. Threads with no core of their
  own now stay off isolated CPUs even when the process was started on
  them (`numactl --cpunodebind`, `taskset`), unless isolated CPUs are
  all it was given. On such a node DPDK, given no core list of its own
  (`-l`), now sees only those housekeeping cores, as under the earlier
  `numactl --physcpubind` workaround, and no longer starts idle workers
  on the isolated ones.
- **A primary that failed to start could leave its pipeline threads
  running.** When a stage that starts later (the replication sender, the
  shadow or the event publisher, the health endpoint, the reader) could
  not, startup returned with the stages already started still spinning.
  A process that exits on the error ends them with it; one that hosts
  several nodes through `run_with_shutdown` kept them past the node's
  failure. Every early return now stops and joins the threads started.
- **A DPDK primary could hold a write forever after a replica joined.**
  It read and dropped the replica's acks while the catch-up was still
  being sent, counting on a later ack to cover them. A replica acks only
  when it holds something new, so when its last ack of the join landed in
  that window, which a primary thread descheduled for a moment opens, the
  primary went on believing the replica held nothing past its handshake:
  a write accepted during the join and waiting on that replica under
  `ram` or `disk+ram` was released only by the next journaled event, a
  tick with ticks on (up to the tick interval late), and never on a quiet
  primary with ticks off. The newest ack is now kept through the whole
  join and counted once the replica streams, as on kernel TCP, whose
  catch-up leaves acks in the socket until then. The end of a replica's
  join is now logged on both transports (`replica caught up, streaming
  live`), and the handoff's journal passes, which were logged as further
  catch-ups, are logged at debug as what they are.
- **The event publisher no longer broadcasts reports before they are
  durable.** It read each report as soon as the application produced it,
  before the journal's fsync and before any replica held the event, so a
  crash or a failover in that window could take back a trade its
  subscribers had already seen. It now reads behind the response stage
  and sees an event only once the event holds the copies the ack policy
  demands, as the client's reply does: the feed trails the replies, and
  stalls with them while the policy cannot be met. The publisher's
  signature is unchanged. Publishers should stamp their frames with the
  slot's wire sequence (`OutputSlot::wire_seq`), the event's journal
  sequence, which survives restarts and failovers, rather than their
  position in the output ring, which restarts at every boot. Finding 9 of
  the September determinism audit.

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

[Unreleased]: https://github.com/melin-engine/melin/compare/v0.20.0...HEAD
[0.20.0]: https://github.com/melin-engine/melin/releases/tag/v0.20.0
[0.19.0]: https://github.com/melin-engine/melin/releases/tag/v0.19.0
[0.18.0]: https://github.com/melin-engine/melin/releases/tag/v0.18.0
[0.17.0]: https://github.com/melin-engine/melin/releases/tag/v0.17.0
[0.16.0]: https://github.com/melin-engine/melin/releases/tag/v0.16.0
[0.15.0]: https://github.com/melin-engine/melin/releases/tag/v0.15.0
[0.14.0]: https://github.com/melin-engine/melin/releases/tag/v0.14.0
[0.13.0]: https://github.com/melin-engine/melin/releases/tag/v0.13.0
