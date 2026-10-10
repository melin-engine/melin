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

**(c) Hoist the dialer into `melin-dpdk`, and add a polled connection to
the client, generic over a byte stream.** Recommended. The dialer lives
once, next to the device and transport it is built from, and the proxy and
the client both use it. The protocol logic of the polled connection is
tested on every host with no libdpdk, against the same fake node as the
blocking connection, and only the thin binding to the dialer needs the
DPDK test rig. The blocking connection is untouched.

## Design

### 1. A single-socket dialer in `melin-dpdk`

Two pieces, split along the EAL's lifetime.

**The NIC handle** initialises the EAL from arguments, or shares the
process-wide one (refusing arguments of its own, as a node does), and
builds the pool, port, device and interface. It outlives any one
connection. With an owned EAL it is the last thing torn down; on the shared
EAL it leaves the EAL alone, as a node does.

**The stream** is one TCP connection dialed from the NIC handle with a
deadline. Before the first frame the dialer announces the client with a
gratuitous ARP and seeds the server's MAC into the interface's neighbour
cache, from a supplied value or the SR-IOV convention (`resolve_peer_mac`),
and reports which it used: a wrong MAC is silent, only a connect that times
out, so the error for a timed-out dial names the peer MAC as the first
thing to check, as the proxy's does. One connection per NIC: the stream
holds the NIC handle exclusively and gives it back when it is dropped or
closed, so a reconnect dials again on the same NIC. Whatever the stream's
state on hand-back, its socket is reset rather than forgotten, so the node
frees the connection's slot promptly. Aborting a smoltcp socket only queues
the RST; it goes out on a later poll and flush, and only while the socket
is still in the interface's socket set. So the hand-back follows the
node's own reset (`abort_announced`, then a transmit flush): it aborts the
socket, runs one egress pass and flushes the device, and only then removes
the socket. One pass, best effort, as on the node: a frame the device
cannot take at that moment is not retried, and the node's heartbeat covers
that case. Removing the socket first would drop the RST on the floor every
time and leave the node holding the slot until its next heartbeat, which is
the very divergence the next paragraph cites.

Each dial takes a fresh local port. The proxy picks one from its process
ID, once per process, which is enough for one connection per process; a
client that reconnects on one NIC would otherwise reuse the four-tuple of a
connection the node may still hold (a DPDK node does not see a client's
close until its next heartbeat; see `transport-divergences-2026-10.md`).

The stream offers:

- a non-blocking send that takes what the socket's transmit buffer has
  room for, possibly nothing;
- receive into a caller's buffer, nothing yet being a zero and a closed
  connection an error, never a zero;
- a service step that polls the device, runs the stack, and flushes the
  transmit queue, taking the caller's timestamp;
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
dependency, and the proxy's goes away.

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
kernel socket, the in-memory test stream) implements only the three; the
dialer's stream overrides it with the real thing. A connection generic over
the trait does what the blocking `Connection` does: runs the handshake,
keeps a receive buffer and splits frames in place with `split_frame`,
classifies replies, skips heartbeats without extending the deadline, and
frames requests with `frame_request` through the in-place step. Generic,
not a trait object, so the hot loop pays no indirection.

No staging copy in either direction on DPDK, and one copy per direction on
the kernel path, as the blocking connection has: a reply is split where it
was received, a request is encoded where it is sent from. The one exception
is a request that does not fit the contiguous room at the end of the
stack's transmit ring; it is framed in the scratch buffer and copied
through the plain send, rather than split across the wrap. The encoder
never sees two regions.

It offers the blocking connection's calls (send, next frame, request) with
the same errors, the next-frame call spinning on the stream until a frame
or the deadline, and a non-blocking poll for a caller that runs its own
loop and wants to service other work between frames.

The handshake is not pipelined with the first request: the connection
waits for the node's ready before it sends anything else. A DPDK node can
stall a request that arrives in the same segment as the challenge response
(see "Scope limits"), so this is the only safe order, and it is the order
the blocking connection already keeps.

The clock is a `std::time::Instant` refreshed every so many polls, as the
node transport refreshes its stack's (`TIMESTAMP_REFRESH_INTERVAL`), not
on every iteration. The read deadline and the stream's service timestamp
both read that cached value. No TSC in the library: a caller that wants
nanosecond timestamps for its samples takes its own, as the echo client
already does.

Tests run against the existing fake node in `lib.rs`, through a
non-blocking kernel socket implementing the byte-stream trait: every
blocking-connection behaviour the fake node already exercises (a reply
batch, heartbeats skipped, silence reported as no reply and not deferred by
heartbeats, a refused key, an oversized frame, a closed connection, a
pipelined burst) has a polled twin. An in-memory stream covers what a real
socket rarely produces on demand: a frame split across every possible
receive boundary, a send buffer that takes nothing and then a few bytes,
and, by overriding the in-place step with a ring of its own, a request that
meets the transmit ring's wrap and falls back to the scratch buffer. The
non-blocking kernel stream is public: it is also a polled kernel client in
its own right, the proxy's kernel transport in library form.

A small session trait, implemented by both connections, carries the calls
a request loop needs (send, next frame, request), so a caller writes its
loop once and picks the transport at runtime.

### 3. The `dpdk` feature of `melin-client`

Off by default. It binds the polled connection to the dialer's stream and
adds:

- a configuration with one field per DPDK flag of the proxy
  (`--dpdk-eal-args`, `--dpdk-port`, `--dpdk-ip`, `--dpdk-prefix-len`,
  `--dpdk-gateway`, `--dpdk-peer-mac`, `--dpdk-mtu`). A plain struct, not a
  clap one: the client does not depend on clap. Each binary declares its
  flags and maps them onto it, and the explanation that matters (why the
  peer MAC is not optional on a fabric that assigns MACs, an AWS ENI among
  them) lives on the field, so the binaries' copies stay thin;
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
  defaults do not change.

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

## Order of work

Each step is a commit, reviewed before the next.

1. **The dialer hoist, with the proxy on it.** The NIC handle and the
   stream in `melin-dpdk`, the ARP builders as plain tested functions
   replacing the transport's two inline copies, and the proxy moved onto
   the dialer with its smoltcp dependency dropped. The proxy's behaviour is
   the acceptance test: its tests still pass, it still builds with its
   `dpdk` feature in the CI job, and a DPDK run against a node behaves as
   before.
2. **The polled connection, with its tests.** Ungated, against the fake
   node and the in-memory stream, plus the session trait implemented by
   both connections.
3. **The `dpdk` feature.** The configuration, the constructors, the new
   `Error` variant, the manifest note, and the changelog entry for the
   variant.
4. **The test rig and CI.** The runner's client pair, the launcher's
   helper, the DPDK client cases in the echo and counter round trips, and
   `melin-client/dpdk` on the CI job.
5. **The echo client's transport flag.**
6. **Docs.**

## Scope limits and open questions

- **One connection per NIC** in the first version, as in the proxy. The
  NIC/stream split is chosen so several sockets on one stack can be added
  later without changing the connection API. If the exchange's benchmark
  client needs many connections on one port from the start, that moves
  into scope, and the NIC handle grows a poll step of its own that services
  every stream on it once per iteration.
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
