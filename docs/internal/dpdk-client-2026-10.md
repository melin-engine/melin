# DPDK support in `melin-client` (plan)

Status: **proposed, not started** (2026-10). Covers the roadmap item
"DPDK support in `melin-client`" ([roadmap.md](roadmap.md)).

The one-line goal: **a client links `melin-client`, turns on one feature,
and talks to a node over kernel bypass with the same protocol code the
kernel path uses.** Today a Rust client that wants DPDK either re-rolls
the dialer, the handshake loop and the reply loop on top of the client's
I/O-free pieces, or goes through the shared-memory proxy, a second process
built for clients that cannot host DPDK at all. Neither is what an
operator measuring the sequencer's floor on kernel bypass should have to
do, and neither lets this repository measure that floor itself.

Not a goal: replacing the proxy in the Aeron benchmark harness. That
harness retired an earlier Rust load generator on purpose, so that its one
client, the Java rig, measures every system the same way, with a separate
process owning the NIC as Aeron's own DPDK driver does. The proxy stays its
client; this work gives it a better dialer.

## What the code shows

- **The client already has the I/O-free half.** `Handshake` is the
  handshake as a state machine fed the node's frames; `framing::split_frame`
  and `FrameDecoder` find frames in received bytes; `classify` and
  `next_reply` tell replies apart, heartbeats included; `frame_request`
  and `seal_request` frame a request in place, at any offset of a send
  buffer. The crate documentation presents them for "a load generator on
  a user-space TCP stack". What is missing is a connection type that drives
  them over a polled transport, and a DPDK byte stream to drive it on.
- **The blocking `Connection` is the behaviour to match.** It runs the
  handshake, reads into its reader's buffer and borrows each frame from
  there, classifies replies, skips heartbeats without pushing the read
  deadline back, and copies a request body once, into its writer's buffer.
  It is blocking by design, one thread per connection on `std::net`, and
  its tests run it against a fake node in `lib.rs`.
- **The DPDK byte stream exists, in the wrong place.** The shm-proxy's
  `dpdk.rs` is a single-socket dialer over `DpdkDevice` and smoltcp: it
  initialises the EAL, builds the pool, port, device and interface, seeds
  the server's MAC from `--dpdk-peer-mac` or the SR-IOV convention
  (`resolve_peer_mac`), and dials one TCP connection with a deadline. Four
  things keep it from serving a client library:
  - it lives in an unpublished tool;
  - it owns its EAL outright (`Eal::init`), so the first connection that
    closes ends DPDK for the process;
  - its deadline and its stack clock come from the proxy's calibrated TSC
    (`tsc.rs`), which no library should carry;
  - its ARP frames (`arp.rs`) are a third copy of what
    `DpdkTransport::send_gratuitous_arp` and `seed_neighbor` build inline
    in `transport.rs`. The copies have already drifted: the proxy's
    gratuitous request carries a broadcast target hardware address, the
    transport's a zeroed one. Receivers ignore that field in a gratuitous
    request (RFC 5227), so neither is wrong, but the hoisted builder has to
    pick one, and the node's (zeroed, as the RFC writes it) is the one with
    production behind it.
- **The exchange's benchmark client is a third dialer, and the largest.**
  The Exchange Core's `melin-ec-bench` has a DPDK module of its own
  (`crates/exchange/bench/src/dpdk.rs` in that repository) that initialises
  its EAL, builds pool, ports, device and interface, seeds the neighbour
  cache with a fourth copy of the ARP frames (a gratuitous request and two
  synthetic replies, one for the server and one for the gateway), drives
  the stack clock from the wall clock on every poll, and runs the handshake
  and the reply loop over the client's I/O-free pieces by hand. It is the
  client behind the README's headline figures, and it is the one with the
  demanding shape: several connections on the one DPDK port the bench host
  has, in one socket set on one interface, polled by one pinned thread,
  each connection dialed and authenticated in turn. It takes
  the node's whole configuration surface, not the proxy's subset: a list of
  ports for an LACP bond, a VLAN id, a bifurcated peer IP with its steering
  rule, and a gateway MAC for the L3 mode where the gateway is seeded in
  place of the server and no gratuitous ARP goes out. And it depends on the
  smoltcp fork directly to do all of this. The proxy and the bench also
  repeat the same socket tuning (Nagle off, no delayed ACK, the stack's
  1 ms retransmit floor, a 64 KiB initial window) with different socket
  buffer sizes.
- **The Exchange Core has two more clients built on the I/O-free pieces.**
  The bench's kernel path, on io_uring, and the order-entry gateway's
  session, driven from an io_uring completion loop, both run `Handshake`,
  `FrameDecoder` and `classify` by hand. Neither is polled: bytes arrive
  in a buffer the completion names. A polled connection that owns its
  stream cannot serve them; one that is fed its bytes can.
- **The node transport is already a dialer.** `DpdkTransport` has every
  piece the handle below needs: the shared EAL (`DpdkShared::init`), a
  constructor that listens on no port (`from_shared_unlistening`), an
  outbound connect (`connect_to`) on a tuned socket, neighbour seeding in
  both modes, and the poll. The replica dials the primary through exactly
  these calls, so the dialing sequence exists a fourth time, inside the
  runtime, next to the proxy's and the bench's. The socket tuning the
  proxy and the bench re-roll is its `tune_socket`. What it lacks for a
  client is small: a dial with a deadline and a fresh local port, a send
  that frames into the socket's own transmit buffer instead of the
  per-connection queue the node's response thread hands off through, and
  a receive that does not go through the zero-copy callbacks the node
  registers. A new stack driver beside it would be the fifth copy of the
  thing this plan exists to deduplicate.
- **A DPDK node refuses a second SYN during a handshake.** The transport
  keeps one listening socket per port and replaces it only once the
  accepted one is established (`check_listener`), and the stack answers a
  SYN that matches no listener with an RST. A second SYN in flight during
  the first handshake is therefore refused outright, where the kernel
  would queue it in the backlog. The exchange's bench dials sequentially
  for this reason, although its commit blamed its own auth loop, which at
  the time expected connections in the order it dialed them. Every DPDK
  client inherits the constraint until the node keeps a backlog of
  listeners; it is a kernel-versus-DPDK divergence missing from
  `transport-divergences-2026-10.md`.
- **The proxy's transport seam is already the right shape.** Its
  `Transport` trait is non-blocking send, a service step, and receive into
  a caller's buffer, generic rather than dynamic so there is no indirection
  in the loop. The client's byte-stream trait below starts from those
  three steps and adds a fourth, an in-place send, so a request can be
  framed where the stack sends it from.
- **EAL initialises once per process and never again after cleanup**
  (`eal.rs`). So "connect" must not own the EAL. A client that reconnects,
  or a test process that hosts nodes and a client, needs a long-lived NIC
  handle separate from the connection. The test launcher already works this
  way for nodes: one process-wide EAL (`Eal::init_process_wide`), shared by
  every node it starts (`DpdkShared::init`), each on a virtual device of
  its own attached by hotplug (`Eal::attach_vdev`). A node on the shared
  EAL refuses EAL arguments of its own and names its mbuf pool after its
  port (`eal_sharing.rs`), since pool names are unique per process; the
  dialer needs both rules too, and they are already plain, ungated code in
  the same crate.
- **The node's stack clock is monotonic, and coarse on purpose.**
  `DpdkTransport::poll` refreshes smoltcp's timestamp once every
  `TIMESTAMP_REFRESH_INTERVAL` polls, from `StackClock`, which counts
  milliseconds forward from a `std::time::Instant` because a wall clock
  stepped forward would fire every timer at once. The proxy's TSC clock is
  monotonic in practice; a library clock must be by construction.
- **Licensing.** `melin-client` is Apache-2.0 and published, so that a
  customer can link it into their own binaries and copy from it without the
  runtime's licence travelling along. `melin-dpdk` is BUSL-1.1 and
  published. An optional dependency of one on the other passes `deny.toml`
  unchanged, because the BSL exception is granted per crate name and
  `melin-dpdk` already has one. But a customer who enables the feature
  links BSL code. Off by default keeps the copy-freely story intact for
  everyone who does not ask for kernel bypass.
- **The test network has no DPDK seat for a client.** The netns runner
  builds one veth pair per node slot, the node's end addressless for DPDK,
  and gives the client only the kernel address on the bridge
  (`MELIN_NETNS_CLIENT_IP`, `MELIN_NETNS_CLIENT_MAC`, the bridge's own). A
  client on DPDK needs a veth pair of its own with no address on its DPDK
  end, built the way a node slot is.
- **The CI `dpdk` job** (`.github/workflows/pre-merge.yml`) checks, tests
  and documents a fixed list of packages and features. Nothing in
  `melin-client` is on it today, because nothing in it needs libdpdk.

## Options

**(a) Copy the proxy's dialer into the client behind a feature.** The
quickest: the code works and is already shaped for one socket. But it
leaves two copies of the dialer (three of the ARP frames), which drift as
the ARP builders already have, and the client inherits the EAL ownership
problem without anyone deciding it.

**(b) Make the blocking `Connection` generic over a transport trait.** One
connection type for both. But it reaches into the kernel path, which is
blocking by design and well tested, and a blocking API over a polled stack
is a spin loop wearing a blocking signature; the generic parameter would
also leak into every consumer that names the type today.

**(c) A new single-socket dialer in `melin-dpdk`, beside the transport.**
The proxy's code moved into the library and made reusable. But the
transport already dials (the replica's connection to the primary goes
through it), already tunes its sockets and seeds its neighbours, so this
leaves two stack drivers in one crate, and the runtime's is the one with
production behind it. The copy this plan removes from the proxy would come
back inside the library.

**(d) Build the client's NIC handle on the transport, listen-free, and add
a polled connection to the client, generic over a byte stream.**
Recommended. The handle is `DpdkTransport` constructed on no port, plus
the few things a client needs that the node does not; the dialing sequence
then exists once, used by the replica, the proxy, the client and the
exchange's bench. The protocol logic of the polled connection is tested on
every host with no libdpdk, against the same fake node as the blocking
connection, and only the thin binding to the handle needs the DPDK test
rig. The blocking connection is untouched.

## Design

### 1. The NIC handle in `melin-dpdk`, on the transport

Two pieces, split along the EAL's lifetime.

**The NIC handle** is the node transport constructed on no port
(`from_shared_unlistening`), over an EAL it initialises from arguments or
shares with the process (refusing arguments of its own, as a node does),
with pool, ports, device and interface built as the node builds them. It
outlives any one connection. With an owned EAL it is the last thing torn
down; on the shared EAL it leaves the EAL alone, as a node does. The
transport's neighbour seeding serves unchanged, in both its modes: in L3
mode the gateway is seeded from the supplied MAC and nothing is announced,
otherwise the client announces itself with a gratuitous ARP and the server
is seeded from a supplied value or the SR-IOV convention
(`resolve_peer_mac`). The handle reports which it used: a wrong MAC is
silent, only a connect that times out, so the error for a timed-out dial
names the peer MAC as the first thing to check, as the proxy's does.

Its configuration is the node's, shared rather than mirrored. The NIC
fields of `DpdkConfig` (EAL arguments, the list of ports, where an LACP
bond is two ports driven as one, the first for transmit and all of them
polled; the address and prefix; the gateway; the bifurcated peer IP; the
gateway MAC; the peer MAC; the MTU; the VLAN id) move into a struct of
their own that the node's config composes with its listen port and queue
count, and the client re-exports that struct. A copy with the same field
names would be a fifth configuration to keep in step, and drift between
copies is the whole motivation here. The exchange's bench runs in every
mode the node does (SR-IOV with a VLAN, an mlx5 port shared with the
kernel, L3 through a gateway), and takes every one of these fields today.

A client's EAL arguments default to a file prefix of the client's own. A
DPDK process without one shares the runtime directory with every other on
the host, so a client beside a node, the usual local arrangement, fails
to take the runtime lock, and the exchange's suite clears that directory
between runs for exactly this reason. The error for a failed EAL
initialisation names the prefix.

**The NIC owns the socket set, and a stream is one socket in it.** The
bench host has one DPDK NIC: one VF, one ENI, one port shared with the
kernel. A port is configured and started by one owner in a process, so a
second handle on it is not possible, a VF per connection is a MAC and an
IP per connection that nothing provisions, and splitting a port by
hardware queue does not help either, since the return traffic is hashed
across queues by four-tuple and lands wherever the hash says, not on the
interface that owns the socket. So several connections on one NIC
necessarily share its interface, its socket set and its poll thread,
which is how the bench works today. The handle therefore holds the socket
set, dials any number of streams on it, and services all of them in one
device poll. A single-stream client is the degenerate case and pays
nothing for the generality: one socket in the set, one poll.

**The stream** is one TCP connection dialed from the NIC handle with a
deadline, on a fresh local port, through the transport's own connect, so
the socket carries the transport's tuning (`tune_socket`: Nagle off, no
delayed ACK, the stack's retransmit floor and initial window), which the
proxy and the bench each set by hand today, and a socket buffer size the
caller chooses, as the transport's replication connections already do.
What the dial adds to `connect_to` is the deadline, the local port, and
the wait for the socket to be established, with a refusal and a timeout
told apart. The dial loop refreshes the stack's clock on every iteration:
the connect phase runs on retransmit timers, and the bench found that a
stale timestamp there stalls the handshake. Dials on one NIC are
sequential, and the reason is the node's, not the client's: a DPDK node
keeps one listener per port and answers a SYN during another's handshake
with an RST (see "What the code shows"), so a second dial in flight would
be refused, not queued. A stream is established and handed back before
the next is dialed. The constraint lifts when the node keeps a backlog of
listeners; until then the handle enforces it, and its documentation says
why.

A stream is reset when it is dropped or closed, whatever its state, so the
node frees the connection's slot promptly. Aborting a smoltcp socket only
queues the RST; it goes out on a later poll and flush, and only while the
socket is still in the socket set. So the hand-back follows the node's own
reset (`abort_announced`, then a transmit flush): it aborts the socket,
runs one egress pass and flushes the device, and only then removes the
socket. One pass, best effort, as on the node: a frame the device cannot
take at that moment is not retried, and the node's heartbeat covers that
case. Removing the socket first would drop the RST on the floor every time
and leave the node holding the slot until its next heartbeat, which is the
very divergence the next paragraph cites.

Each dial takes a fresh local port, per socket, not per process. The proxy
picks one from its process ID, once per process, which is enough for one
connection per process; the bench adds the connection's index. A client
that reconnects on one NIC would otherwise reuse the four-tuple of a
connection the node may still hold (a DPDK node does not see a client's
close until its next heartbeat; see `transport-divergences-2026-10.md`).

Ownership follows from the shared socket set. A stream cannot hold the
NIC mutably across calls while its siblings do, so the stream type is a
short-lived view the handle lends out by stream id, borrowing the handle
for the duration of one call, and the handle's service step advances every
stream at once. The polled connection in decision 2 is shaped to take a
stream per call rather than own one, so that this is the natural fit and
not a workaround.

The stream offers:

- a non-blocking send that takes what the socket's transmit buffer has
  room for, possibly nothing;
- receive into a caller's buffer, nothing yet being a zero and a closed
  connection an error, never a zero;
- a service step that polls the device, runs the stack over every socket
  in the set, and flushes the transmit queue, taking the caller's
  timestamp. It is the NIC's step offered through the stream, so a
  single-stream caller never names the NIC in its loop, and a caller with
  several streams calls it once per iteration, or between streams as the
  bench does to keep the NIC busy, not once per stream;
- an in-place send that hands the caller the contiguous free region of the
  socket's transmit buffer to frame a request into, so a request is encoded
  where the stack sends it from.

The timestamp is a monotonic nanosecond count whose origin is fixed for
the life of the NIC handle, since the interface and its timers outlive one
connection. The stream clamps a reading that goes backwards rather than
trusting it. The client derives the count from a `std::time::Instant`
origin held with the NIC handle; the proxy keeps its TSC clock and passes
its own reading. No TSC code moves into the library.

Everything the dialer exposes is a std type (addresses, byte slices,
`io::Error`, the MAC as six bytes), so no consumer needs a direct smoltcp
dependency: the proxy's goes away, and so can the exchange bench's
dependency on the fork.

The ARP frame builders move here from the proxy, as plain functions, and
replace the two inline copies in `transport.rs`. Their tests run ungated,
like the crate's other pure logic (`mac`, `rx_checksum`, `eal_sharing`).

### 2. A polled connection in `melin-client`, ungated

A small byte-stream trait of four steps: the three of the proxy's
`Transport` (non-blocking send of a byte slice, service, receive), and an
in-place send that hands the caller the contiguous free region of the
stream's transmit buffer and then commits however many bytes the caller
wrote into it. The in-place step has a default implementation that frames
into a scratch buffer of the connection's and goes through the plain send,
so a stream with no transmit buffer of its own to lend (the non-blocking
kernel socket, the buffer-fed stream) implements only the three; the
dialer's stream overrides it with the real thing. A connection generic over
the trait does what the blocking `Connection` does: runs the handshake,
keeps a receive buffer and splits frames in place with `split_frame`,
classifies replies, skips heartbeats without extending the deadline, and
frames requests with `frame_request` through the in-place step. Generic,
not a trait object, so the hot loop pays no indirection.

The connection is protocol state, not stream ownership. It holds the
handshake, the receive buffer, the scratch buffer and the deadline, and
every call takes the stream and the caller's timestamp as arguments. Two
consumers need it this way. The exchange's bench runs many connections on
one NIC whose socket set the handle owns, so no connection can own its
stream, and it keeps its own TSC for its samples, so it wants to pass the
time it already read rather than have the library read another. The
gateway's session is fed bytes by an io_uring completion loop, and for it
the connection accepts bytes directly, as the frame decoder it runs today
does: one copy, from the completion's buffer into the connection's, the
same count as now. Routing those bytes through a stream the connection
then receives from would be a second copy for nothing.

The state-only form offers only what returns at once: feed bytes, poll for
the next frame or nothing yet, send or frame a request into the stream. It
cannot spin, since the stream view it is handed borrows the NIC for one
call, and a form that spun on it would starve the NIC's other streams.
The spinning calls of the blocking connection (next frame until a frame
or the deadline, request) belong to **the single-stream wrapper**, which
owns one stream and a cached clock and gives the single-stream caller (the
echo client, a customer's hand-written loop) the ergonomic form. The
session trait is implemented by the wrapper and the blocking connection,
not by the state-only form. The wrapper is what the tests below exercise
through the fake node.

No staging copy in either direction on DPDK, and one copy per direction on
the kernel path, as the blocking connection has: a reply is split where it
was received, a request is encoded where it is sent from. The one exception
is a request that does not fit the contiguous room at the end of the
stack's transmit ring; it is framed in the scratch buffer and copied
through the plain send, rather than split across the wrap. The encoder
never sees two regions.

The wrapper offers the blocking connection's calls (send, next frame,
request) with the same errors, the next-frame call spinning on the stream
until a frame or the deadline, and a non-blocking poll for a caller that
runs its own loop and wants to service other work between frames.

The handshake is not pipelined with the first request: the connection
waits for the node's ready before it sends anything else. A DPDK node can
stall a request that arrives in the same segment as the challenge response
(see "Scope limits"), so this is the only safe order, and it is the order
the blocking connection already keeps.

The wrapper's clock is a `std::time::Instant` refreshed every so many
polls, as the node transport refreshes its stack's
(`TIMESTAMP_REFRESH_INTERVAL`), not on every iteration. The read deadline
and the stream's service timestamp both read that cached value. No TSC in
the library: a caller that wants nanosecond timestamps for its samples
takes its own, as the echo client already does, and a caller that has its
own clock passes its reading to the state-only form.

Tests run against the existing fake node in `lib.rs`, through a
non-blocking kernel socket implementing the byte-stream trait: every
blocking-connection behaviour the fake node already exercises (a reply
batch, heartbeats skipped, silence reported as no reply and not deferred by
heartbeats, a refused key, an oversized frame, a closed connection, a
pipelined burst) has a polled twin. A buffer-fed stream, a public type
designed as one and not a test double promoted, covers what a real socket
rarely produces on demand: a frame split across every possible receive
boundary, a send buffer that takes nothing and then a few bytes, and, by
overriding the in-place step with a ring of its own, a request that meets
the transmit ring's wrap and falls back to the scratch buffer. The
non-blocking kernel stream is public too: a polled kernel client in its
own right, the proxy's kernel transport in library form. The gateway needs
neither; it feeds the state-only connection its bytes directly and polls
it for frames, instead of running the handshake and the decoder by hand
as it does today.

A small session trait, implemented by the single-stream wrapper and the
blocking connection, carries the calls a request loop needs (send, next
frame, request), so a caller writes its loop once and picks the transport
at runtime.

### 3. The `dpdk` feature of `melin-client`

Off by default. It binds the polled connection to the dialer's stream and
adds:

- a configuration with one field per DPDK flag of the node
  (`--dpdk-eal-args`, `--dpdk-ports`, `--dpdk-ip`, `--dpdk-prefix-len`,
  `--dpdk-gateway`, `--dpdk-peer-ip`, `--dpdk-gateway-mac`,
  `--dpdk-peer-mac`, `--dpdk-mtu`, `--dpdk-vlan`), the NIC handle's
  configuration re-exported. A plain struct, not a clap one: the client
  does not depend on clap. Each binary declares its flags and maps them
  onto it, and the explanation that matters (why the peer MAC is not
  optional on a fabric that assigns MACs, an AWS ENI among them; why a
  client on a host with a node needs a file prefix of its own) lives on the
  field, so the binaries' copies stay thin;
- a constructor that brings a NIC up from that configuration, and one that
  takes an existing NIC handle, so a client reconnects without touching the
  EAL and a process that already holds the process-wide EAL (a test hosting
  nodes) shares it.

A failed dial maps onto the existing errors: a timeout to
`Error::Connect` with a timed-out source and the peer-MAC hint, a refusal
to `Error::Connect` with a refused one. A NIC that cannot be brought up (EAL
initialisation, a missing port, the pool, port configuration) needs a new
`Error` variant. Under the no-`#[non_exhaustive]` rule that is
source-breaking for every exhaustive match, so it goes in `CHANGELOG.md`
under **Changed**. The variant is unconditional, present with or without
the feature and constructed only with it: features are unified across a
build graph, so a variant that exists only under `dpdk` would break a
crate's exhaustive match the moment some other crate in the same build
turned the feature on.

The client's manifest gains a comment beside the optional dependency
explaining why a BSL dependency is acceptable in an Apache-2.0 crate: it is
off by default, nothing of `melin-dpdk` is compiled without it, and a
customer who turns it on has chosen to link the runtime's licence for that
binary.

### 4. Test rig

- **The runner** adds a client veth pair, its DPDK end up without an
  address and its peer on the bridge, built by `veth-setup.py` exactly as a
  node slot is, and publishes its interface, IP and MAC in variables of
  their own. A separate set rather than another node slot, so no cluster
  test can start a node on it by counting slots. The kernel client address
  on the bridge stays, for every test that does not ask for DPDK.
- **The test launcher** gains a helper that attaches an af_packet device
  for the client's interface on the process-wide EAL, under a device name
  no node slot uses, and returns the NIC handle. The pool name follows the
  shared-EAL rule, after the port.
- **The echo and counter round trips** gain a case with the client on
  DPDK, against a node on DPDK, under the runner. The peer MAC comes from
  the runner's layout (`Layout::mac_of`); the runner's MACs are
  deliberately not the SR-IOV convention, so the test passes only if the
  MAC was passed through, as for a replica.
- **CI.** The `dpdk` job adds `melin-client/dpdk` to its check, test and
  doc steps. The `dpdk` nextest profile already runs one test at a time;
  a client on DPDK adds one more busy-polling thread to a test, beside its
  nodes', on a CI runner whose cores are few.
- **A TAP smoke script**, under `scripts/`, for a check outside the rig
  and the test suite. The exchange's smoke test runs a DPDK node on a TAP
  virtual device (`--vdev=net_tap0 --no-pci`) and talks to it from a
  kernel client on the TAP's other end, with no NIC and no namespace. The
  mirror image, a DPDK echo client over TAP against a kernel echo-server,
  checks the dialer's whole path, EAL to handshake, in a minute on any
  host with libdpdk. Hugepages and root, as the exchange's script
  arranges.

### 5. Consumers

- **`echo-client`** gains `--transport dpdk` with the proxy's DPDK flags,
  and a `--core` pin, so the sequencer's latency floor is measurable on
  kernel bypass from this repository. The echo example's `dpdk` feature
  forwards to `melin-client/dpdk`. Its loop goes through the session trait,
  so the kernel and DPDK runs share one measurement.
- **The shm-proxy** drops `dpdk.rs` and `arp.rs` for the hoisted dialer,
  keeps its own `Transport` trait (it knows no protocol and does not
  depend on `melin-client`) with the dialer's stream behind it, and keeps
  its TSC clock through the service step's timestamp. Its flags and their
  defaults do not change, and neither does what it prints: the Aeron
  harness's remote runner greps its stderr for the connected line and the
  error line to sequence the rig's start, and the `--trace` report is
  quoted throughout that harness's AWS results. Both are a contract, byte
  for byte. The new node-side flags (`--dpdk-ports`, `--dpdk-peer-ip`,
  `--dpdk-gateway-mac`, `--dpdk-vlan`) come to it for free and are added.
- **The exchange's bench**, in its own repository and on the next
  sequencer release, moves `bench/src/dpdk.rs` onto the NIC handle and its
  streams, keeping its loop (the window, the pacer, the TSC samples, the
  outcome tally) and dropping its EAL setup, its ARP frames, its socket
  tuning and its dependency on the fork. Its handshakes go through the
  state-only connection, one per stream, fed the stream and the bench's
  own time. The migration is the acceptance test of the multi-socket
  handle: the suite's throughput workload on DPDK measures the same before
  and after.
- **The exchange's gateway** is a later consumer, not work here: its
  session would feed the bytes each completion delivers to the state-only
  connection and poll it for frames, at the one copy it pays today. A
  gateway whose node side is on DPDK is the product feature behind that,
  and it is the same split that makes it possible.

### 6. Docs

- The client section of `docs/building-an-application.md`: the polled
  connection and the session trait, the `dpdk` feature, what it requires of
  the host, that turning it on links BSL code, and that a DPDK client
  thread spins at full load and should be pinned to a core of its own.
- `CHANGELOG.md`: the polled connection and the feature under **Added**;
  the new `Error` variant under **Changed**.
- The CLAUDE.md project description: the client's DPDK feature, and the
  dialer in `melin-dpdk`.
- `docs/internal/dpdk-testing.md`: the client's slot in the runner and the
  launcher's helper.
- `docs/internal/transport-divergences-2026-10.md`: the entry for the
  single listener, a SYN during another's handshake refused rather than
  queued. Written with this plan, since the plan is what found it.

## Order of work

Each step is a commit, reviewed before the next.

1. **The factoring, behaviour-preserving.** The NIC fields of `DpdkConfig`
   into their own struct, composed by the node's config; the ARP builders
   as plain tested functions replacing the transport's two inline copies
   and the proxy's; `tune_socket` and the seeding reachable by the handle.
   No new behaviour, the node's DPDK suites unchanged. Its own commit, so
   the handle's review is about the handle.
2. **The handle, with the proxy on it.** The dial with its deadline and
   local port, the stream as a view by handle, the in-place send and the
   plain receive on a transport constructed listen-free, and the proxy
   moved onto it with its smoltcp dependency dropped. The proxy's
   behaviour is the acceptance test: its tests still pass, it still builds
   with its `dpdk` feature in the CI job, a DPDK run against a node
   behaves as before, and its stderr reads the same.
3. **The polled connection, with its tests.** Ungated: the state-only
   connection and its single-stream wrapper, against the fake node and the
   buffer-fed stream, plus the session trait implemented by the wrapper
   and the blocking connection.
4. **The `dpdk` feature.** The configuration re-exported, the
   constructors, the new `Error` variant, the manifest note, and the
   changelog entry for the variant.
5. **The test rig, the TAP smoke script and CI.** The runner's client
   pair, the launcher's helper, the DPDK client cases in the echo and
   counter round trips, the smoke script, and `melin-client/dpdk` on the
   CI job.
6. **The echo client's transport flag.**
7. **Docs**, the divergences entry included.
8. **The exchange's bench onto the handle**, in that repository, once the
   release carrying steps 1 to 4 is out and its sequencer pins move.

## Scope limits and open questions

- **Several sockets per NIC are in scope from the start.** An earlier
  draft left one connection per NIC, with the split chosen so more could
  come later. The exchange's bench settles it: it needs them from the
  first day it can move, and a handle that one stream holds exclusively
  would be rebuilt, not extended, to take it. The single-stream caller
  sees none of it.
- **The exchange's loop stays in the exchange.** The window, the open-loop
  pacer, the per-connection keys and the outcome tally are the bench's;
  only the dialer and the handshake move under it.
- **IPv4 only**, as the transport is.
- **Node-side divergences stay out of scope**
  (`transport-divergences-2026-10.md`): a DPDK node closes a client
  connection without a FIN or an RST, and can stall a first request that
  arrives pipelined with the challenge response. They affect a DPDK client
  as they affect a kernel one. The first means the polled connection, like
  the blocking one, learns of a node-side close from its read deadline or
  its next send, not from an EOF; the second is avoided by the handshake
  order in decision 2.
- **A DPDK client thread spins at full load.** The library does not pin
  it; the docs say to, and the echo client's `--core` does.
- **Open: the proxy's trait and the client's.** The client's is the
  proxy's three steps plus the in-place send, which the proxy has no use
  for: it forwards bytes it did not frame. Folding them into one would mean
  the proxy depending on `melin-client`, which it has avoided so as to know
  no protocol. Left separate unless a third consumer appears.
