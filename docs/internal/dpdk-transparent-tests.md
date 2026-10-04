# Plan: the kernel-TCP test suite, on DPDK

Goal: an integration test written against kernel TCP runs against the DPDK
transport when the `dpdk` feature is enabled, unchanged. The suite we already
trust then becomes the DPDK suite. Any test that fails there is either a
divergence to fix or a test of kernel-TCP internals.

This builds on the veth harness (`dpdk-veth-testing.md`), which showed a DPDK
node can run on a veth pair through `net_af_packet`, unprivileged, with no
hugepages and no NIC.

Status: steps 1 and 2 are done and pass on a developer host; their CI
steps are written but not yet proven on a hosted runner. Step 3 has
lifted every gate step 2 left; the divergences that remain have no test
compiled out on them (see step 3). Where a
step settled something the design left open, or turned out differently,
its section says so.

To run the suites on DPDK, from the repository root (one test at a time,
as every node busy-polls a core). The example suites:

```sh
CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER="$PWD/scripts/dpdk/netns-runner.sh" cargo nextest run -p melin-example-echo -p melin-example-counter -p melin-example-notary --features melin-example-echo/dpdk,melin-example-counter/dpdk,melin-example-notary/dpdk -j 1
```

For one example, `-p melin-example-echo --features dpdk` is enough. The
runtime's integration suites, `dpdk_veth` included:

```sh
CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER="$PWD/scripts/dpdk/netns-runner.sh" cargo nextest run -p melin-server-runtime --features dpdk -E 'kind(test)' -j 1
```

The host needs unprivileged user namespaces and the veth and bridge
drivers loaded (see the runner's header).

## Why it does not work today

- **Transport is chosen by `run_with_listener`.** `server::run` already
  dispatches to the DPDK transport under the feature. But the integration
  tests call `server::run_with_listener` with a kernel listener bound to
  `127.0.0.1:0`, which is kernel TCP whatever the feature says.
- **EAL is per process.** It initialises once per process and cannot be
  initialised again after its cleanup. The runtime initialises it per node.
  The cluster tests run two or three nodes as threads of one process, and
  some tests restart a node in the same process.
- **Addresses.** A DPDK node owns an IP on a link, not an ephemeral port on
  loopback. Nodes that replicate to each other need a shared L2 segment.
- **Namespaces.** Each test process must run inside its own user, network and
  mount namespaces, with the link already built.

## Design

### Network: a test runner, not code in the tests

A script, `scripts/dpdk/netns-runner.sh`, is set as cargo's target runner
(`CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER`, which nextest honours). For
each test process it:

1. enters `unshare -rnm`;
2. builds the link: veth pairs, then in step 2 a bridge;
3. turns off TX checksum offload on every veth;
4. mounts a private tmpfs on `/var/run`;
5. publishes the layout in environment variables (interfaces, node IPs,
   client IP);
6. `exec`s the test binary.

nextest runs each test in its own process, so each test gets a fresh network.
Tests need no namespace code. A run without the runner, under the feature,
fails loudly, saying how to run it.

As built (step 1): the link is built by `scripts/dpdk/veth-setup.py`
(rtnetlink and the ethtool ioctl from Python's standard library), which the
smoke script now shares instead of carrying its own copy. One pair:
`veth0` for DPDK, `veth1` with the client address `10.99.0.1/24`, the node
on `10.99.0.2`. The layout goes out as space-separated lists, one entry per
node slot, so the bridge of step 2 adds entries rather than variables:
`MELIN_NETNS_DPDK_IFACES`, `MELIN_NETNS_NODE_IPS`, `MELIN_NETNS_PREFIX_LEN`,
`MELIN_NETNS_CLIENT_IP`. A `TMPDIR` under `/run`, which the tmpfs would
hide, is moved to `/tmp`. The runner does not `exec` into `unshare`, so that
when a run fails it can check whether the namespaces were what failed and
say how to allow them; it does not probe up front, which would add a
namespace to every test.

The runner also runs nextest's `--list` invocations, which build a link
for nothing; that costs a fraction of a second per test binary.

As built (step 2): a bridge, `br0`, carries the client address
(`10.99.0.1/24`) and has one veth pair per node slot, three slots (the
largest cluster a test runs): `dpdkN` for DPDK, its peer `dpdkN-br` on
the bridge, node `N` on `10.99.0.(N+2)`. The setup script gained a
`--bridge` mode; the smoke script keeps the single pair. Every MAC is set
(`02:99:00:00:00:NN`) rather than left random, and published
(`MELIN_NETNS_NODE_MACS`, `MELIN_NETNS_CLIENT_MAC`): a DPDK replica must
be given its primary's MAC (`--dpdk-peer-mac`), as on any port that keeps
a hardware address, and the launcher fills it in from the layout. The
MACs are deliberately not the `02:00:<ip>` convention a replica falls back
to without one, so a test passes only if the peer MAC was passed through.
TX checksum offload is off on both ends of every veth, which makes the
kernel finish the checksum of anything the bridge forwards to a node; the
bridge itself needs nothing. A user namespace cannot load a module, so
the veth and bridge drivers must already be loaded on the host (Docker
loads both; the CI job loads them).

### Node launcher: one entry point for both transports

A shared test-support helper starts a node from a `ServerConfig` and returns
its client address and a handle to stop it.

- **Kernel TCP.** Exactly what the tests do now: bind `127.0.0.1:0`, call
  `run_with_listener`.
- **`dpdk` feature.** Take the next node IP from the runner's layout, fill in
  the DPDK fields (EAL args, IP, prefix, port) and call `server::run`.

Every integration test that starts a node goes through it. Where it lives is
an implementation choice: a `test-utils` feature of `melin-server-runtime`, or
a small dev-only crate. It must not add a dependency to the runtime's
production build.

"Enabling the feature" means a crate-level `dpdk` feature that switches the
launcher to DPDK (see "The feature" below for what was built).

As built (step 1):

- **Where.** A dev-only crate, `crates/core/test-node` (`melin-test-node`,
  `publish = false`), rather than a `test-utils` feature of the runtime. The
  runtime's own integration tests can then use it in step 2 through a
  path-only dev-dependency, the cycle the counter example already forms; a
  feature would have needed the runtime to enable a feature of itself for
  its own tests. Its only dependencies are workspace crates and `libc`,
  the latter only under its `dpdk` feature.
- **API.** `melin_test_node::start::<A>(config, startup, sizing, decoder,
  encoder)` returns a `Node` with `addr()` and `stop()`. It overwrites
  `config.bind` (and on DPDK the `dpdk_*` fields) and leaves the rest of
  the config to the test. `STARTUP_LIMIT` is how long a client should give
  the node to serve: unchanged on kernel TCP, longer on DPDK for EAL and
  port start. Step 2 added the cluster side, below.
- **Stopping a DPDK node.** Step 1 stopped one with SIGTERM to the
  process, as `server::run` owns its shutdown flag, which left a window
  where a node already failing took the signal as a second one and exited
  the process. Step 2 replaced it: see below.
- **The feature.** Each example has a `dpdk` feature forwarding to
  `melin-server-runtime/dpdk` and `melin-test-node/dpdk`; the launcher
  takes the transport from its own feature. Enabling only
  `melin-server-runtime/dpdk` builds DPDK into an example's binaries but
  leaves its tests on kernel TCP (`run_with_listener` ignores the
  feature), so the example feature is the switch. (For the runtime's own
  tests, step 2 made the runtime's `dpdk` the switch; see below.)
- **EAL arguments** are the veth harness's: `--no-huge -m 512 --no-pci`,
  main lcore on the first CPU the process may use. (Step 1 put the slot's
  af_packet device in them too; step 2 attaches devices per node.) The
  client port is fixed: a node owns its IP outright.

As built (step 2):

- **Cluster addresses.** `melin_test_node::addrs(slot, free_addr)` gives
  node `slot`'s client and replication addresses before any node starts,
  so a test wires its cluster (`replication_bind`, `replica_of`) the same
  way on both transports, and `start_at::<A>(&addrs, config, ...)` starts
  the node there. On kernel TCP they are two ports from the test's own
  allocator (`|| free_addr(PORT_BASE)`), `slot` playing no part (what the
  tests did before); on DPDK, the slot's IP with fixed client and
  replication ports. The allocator is passed in rather than called by the
  launcher: `free_addr` sits behind `melin-transport-core`'s `test-utils`,
  which as a normal dependency of the launcher would be switched on in
  every workspace-wide build. A DPDK replica's
  `dpdk_peer_mac` comes from the layout, from its `replica_of`, and the
  launcher refuses a `replication_bind` off the slot's IP or a
  `replica_of` off the runner's network, saying how to take them from
  `addrs` (the mistake an unported test would make).
- **Slots.** On DPDK a node holds its slot from start until it is stopped
  or joined; starting a second node on a held slot panics. `start`
  without addresses takes the lowest free slot.
- **The test's own peers.** `local_ip()` is where a test binds a socket a
  node must reach (a scripted primary): loopback on kernel TCP, the
  bridge's address on DPDK.
- **Waiting on a node.** `Node::join` waits for a node to return on its own
  (one expected to fail, or to fence itself) and returns its result;
  `is_finished` and `shutdown_requested` (whether its shutdown flag is
  set, which a fenced node does itself) serve the tests that watch for
  that.
- **Stopping a DPDK node** sets its shutdown flag, as on kernel TCP: the
  launcher runs DPDK nodes with `server::run_with_shutdown` (below), so a
  node can be stopped while others run, and step 1's signal window is
  gone.
- **One process, several nodes.** Under plain `cargo test` (under the
  runner), tests of one binary now start their nodes in one process one
  after another, as long as they hold no more slots at once than there
  are. Running them in parallel threads is still nextest's job, one test
  per process.

### EAL: process-wide (step 2)

One EAL per process, cleaned up only at process exit. Each node takes a
port of its own. The runtime already shares one EAL between a node's client
and replication ports (one `DpdkShared`); this makes the sharing
process-wide.

This is the one change to production code. The production path, with one
node per process, must keep its behaviour exactly, including teardown order
(see the note on vdev PMDs in `crates/core/dpdk/src/dpdk/port.rs`).

As built:

- **Opt-in, by whoever hosts several nodes.** `Eal::init_process_wide(args)`
  (`melin-dpdk`) initialises EAL once and keeps it in a static that is
  never dropped, so never cleaned up; `Eal::process_wide()` returns it. A
  process exit releases its memory (no hugepages in the tests, and the
  runtime directory is the runner's private tmpfs). Only the test launcher
  calls it, with the arguments above and no device.
- **`DpdkShared::init` checks for it first.** None, as in every deployment:
  exactly the old path, EAL initialised from the node's arguments, owned,
  and cleaned up last when the node's resources drop (ports stopped and
  closed, then the pool, then EAL). Some: the node borrows it. Its own EAL
  arguments must be empty, an error otherwise (they could only be
  ignored); its mbuf pool is named after its first port,
  `pktmbuf_pool_<port>`, as pool names are process-wide and nodes on one
  EAL run at once (a node owning its EAL keeps `pktmbuf_pool`); its ports
  stop and close at teardown as always; EAL is left running. The choice
  is an enum held where the EAL used to be, so the drop order is the old
  one. The cost to a deployed node is one `OnceLock` read at start, and
  nothing at all on the poll loop. The two decisions with no libdpdk in
  them (the arguments check, the pool name) live in an ungated module and
  are unit-tested on every host.
- **A device per node, by hotplug.** EAL starts with no device. The
  launcher attaches a node's af_packet device as the node starts
  (`Eal::attach_vdev`, over `rte_eal_hotplug_add`) and passes the port it
  gets as `dpdk_ports`. The node's teardown closes the port, which
  releases it but leaves the device on the virtual bus, so once the node
  has returned the launcher detaches it (`Eal::detach_vdev`) and the next
  node on the slot attaches it afresh. Stop, close, then cleanup still
  holds: on the process-wide EAL there is no cleanup.
- **Stopping one node of several.** `server::run_with_shutdown` is `run`
  with a caller-owned shutdown flag and no signal handler. `run`'s handler
  is process-wide, so with several nodes a signal would stop only the last
  one to install it. `run` keeps its exact order (CPU mask, keys, handler,
  memory lock, transport) and now ends in the same private function.
- **EAL init on a thread of its own.** EAL pins the thread that
  initialises it to its main lcore, and the threads it spawns inherit
  that. The launcher initialises it on a short-lived thread, so the test's
  threads and the nodes keep every CPU. So no node's poll thread is an EAL
  lcore in the tests, and mbuf allocation skips the pool's per-lcore cache:
  fine for logic tests, and never the case in a deployment, where the
  node's own main thread initialises EAL and goes on to poll.

Alternatives considered:

- **An EAL per node, with `--file-prefix`** (the idea in
  `dpdk-veth-testing.md`, step 4). EAL is a singleton per process whatever
  the prefix: the prefix separates processes sharing a host, not nodes in
  one. Not possible.
- **The first node initialises EAL for the process.** No new call, but it
  changes who owns EAL in every deployed node, for a test-only need: the
  explicit opt-in leaves the deployed path as it was.
- **Count references, clean up after the last node.** A restart after the
  last node stopped would need EAL again, which cannot be initialised a
  second time.
- **Keep ports open on the shared EAL and reconfigure them on restart.** No
  hotplug, but a stopped port's receive ring can hold mbufs from the pool
  its node just freed (a NIC driver fills the ring from it), and
  reconfiguring frees them into a freed pool. It would be correct for
  af_packet only, by accident.
- **Every device at EAL init (`--vdev` per slot).** Serves a cluster, but a
  restart still has to re-probe a closed device: hotplug for one case
  makes hotplug for all of them the simpler rule.
- **A process per node.** The runtime would be untouched, but the tests
  read their nodes' state in-process (a sizing probe, a fencing flag) and
  every cluster test would have to be rewritten around processes.

## What stays kernel-only

- **Tests of kernel-TCP internals.** The `response_flush_*` tests drive the
  io_uring response stage directly. So do some `server.rs` unit tests on real
  sockets. They test that implementation, not node behaviour.
- **Raft, health, admin and event endpoints.** These are kernel TCP by
  design, on the namespace's loopback, and need no change.

## Tests that hit a divergence

On DPDK they fail. That is the point: they become the work list for
`transport-divergences-2026-10.md`.

Until the divergence is fixed, such a test carries a narrow
`#[cfg_attr(feature = ..., ...)]` or `cfg` gate naming its divergence entry.
No `#[ignore]`. The gate goes when the divergence does.

## Steps

### 1. Runner, launcher, single-node tests

- The runner script, with one veth pair.
- The node launcher.
- The single-node example tests ported to the launcher: `echo`, `counter`,
  `notary` round trips.

Done when all three pass both ways:

- with kernel TCP, unchanged under the default build;
- on DPDK, under the runner with the feature.

A test that restarts a node, or runs two, in one process waits for step 2,
gated with a note saying so.

Also:

- the existing `dpdk_veth` harness moves onto the runner if that simplifies
  it;
- the CI `dpdk` job gains the example suites on DPDK.

As built: every single-node round trip in the three examples passes on
DPDK. None hit a divergence, so none carries a divergence gate and no new
entry was found. Three tests are compiled out under the feature
(`#[cfg(not(feature = "dpdk"))]`, with a comment pointing here), as they
start a second node in the process:

- echo: `the_node_recovers_from_a_snapshot_and_the_journal_tail` (restart);
- notary: `the_chain_survives_a_restart` (restart) and
  `a_promoted_replica_reports_the_head_the_primary_receipted` (two nodes;
  ported to the launcher all the same, so step 2 only removes the gate).

The `dpdk_veth` harness stays as it is. Moving it onto the runner would
drop its re-exec and its namespace code, but it would need the launcher as
a runtime dev-dependency (step 2 adds that), and it would no longer run
from a plain
`cargo nextest run --features dpdk`. Worth revisiting once step 2 has
landed. (It moved in step 2.)

The CI `dpdk` job checks the examples and the launcher with their `dpdk`
features, and runs the three example suites through the runner with
`-j 1`.

### 2. Process-wide EAL and a bridge

- The EAL change above.
- A bridge with one veth per node slot in the runner.
- The cluster and restart tests ported to the launcher: `replicated_failover`,
  `halt_refusal`, `genesis_promotion`, `sizing`, `startup_events`, the raft
  tests.

As built:

- **The switch.** The runtime's `dpdk` feature now also enables
  `melin-test-node/dpdk`, a dev-dependency's feature, so it reaches only
  the runtime's own test builds (a dependent never builds the runtime's
  dev-dependencies). `--features dpdk` on the runtime is therefore the
  switch for its integration tests, as each example's `dpdk` is for its
  own; the second command at the top runs them.
- **Ported.** Every test of those seven binaries starts its nodes through
  the launcher. On kernel TCP each behaves as before: the same `free_addr`
  port scheme, nodes on threads through `run_with_listener`, stopped by
  their flag. The `response_flush_*` binaries start no node and stay as
  they are, kernel-only by nature; they run harmlessly in the DPDK run.
- **Passing on DPDK:** `startup_events` (restarts in one process),
  `raft_smoke`, `sizing` (a primary and a replica replicating over DPDK,
  then both restarted), `genesis_promotion`'s refusal case (a DPDK replica
  dialling the test's scripted primary on the kernel side), and in the
  examples the two restart tests step 1 gated. Replication between DPDK
  nodes works over the bridge, the replica seeded with its primary's MAC.
- **Gated on a divergence**, both new and added to
  `transport-divergences-2026-10.md`:
  - "DPDK does not notice a replication peer that has gone": a node that
    stopped left its peer's link up, as nothing on a DPDK link had a
    deadline. A primary kept counting the replica that left and never
    halted (`halt_refusal`); replicas kept following a stopped primary
    and refused to depose it (`replicated_failover`, `raft_failover`).
    Fixed in step 3 (see "Replication peer liveness on DPDK" below);
    `halt_refusal` runs on DPDK.
  - "A promoted DPDK replica serves on kernel TCP": promotion fell back
    to the kernel-TCP primary on the DPDK port's address and failed to
    bind (`genesis_promotion`'s
    `a_replica_configured_with_a_larger_genesis_is_promoted`, the notary
    example's `a_promoted_replica_reports_the_head_the_primary_receipted`;
    and the two failover tests behind the entry above). Fixed in step 3
    (see "A promoted replica on DPDK" below); every one of these runs on
    DPDK.

  A single-test binary is gated whole (`#![cfg(not(feature = "dpdk"))]`),
  a test in a binary of several with `#[cfg(not(feature = "dpdk"))]`, and
  each gate's comment names its entries.
- **`dpdk_veth`** runs under the runner and starts its node through the
  launcher: its re-exec, namespace code, done marker and copy of the EAL
  arguments are gone (`dpdk-veth-testing.md` says what that changed). It
  is DPDK-only by nature and built only with the feature.
- **CI.** The `dpdk` job runs the runtime's integration suites on DPDK
  through the runner, `-j 1`, in place of the veth tests alone, then the
  example suites as before. It also loads the veth and bridge drivers,
  which the runner's bridge needs and a user namespace cannot load. Both
  DPDK runs are serial and take minutes; pre-merge still holds them.

### 3. Divergences

Gate whatever fails on its divergence entry, then fix the divergences one at a
time. Each fix removes a gate.

The work list step 2 left, in the order that unblocks most: noticing a
replication peer that has gone (three gates, the two failover tests also
needing the next), then a DPDK primary for a promoted replica (two
gates more).

Done: noticing a replication peer that has gone, below. `halt_refusal`
runs on DPDK. Also done: the limitation that deadline introduced, a
replica's join stalling the primary's poll thread ("A replica's join off
the primary's poll thread", below). And a DPDK primary for a promoted
replica ("A promoted replica on DPDK", below), which lifted the last
gates step 2 left: no test is compiled out on a divergence any more.

#### Replication peer liveness on DPDK

The problem: a kernel-TCP node's peer learns that it stopped from the
node's kernel, which closes its sockets (EOF, or a reset) even when the
process crashed. A DPDK node is its own TCP stack, so once it stops
nothing speaks for it, and neither end of a DPDK replication link had a
deadline on a silent peer. The kernel-TCP path has no application-level
deadline while streaming either: its sender and receiver both end the
session on EOF or a reset (its receiver's 5 s read timeout covers only
the auth and handshake reads).

What was built, all in the replication layer and the transport calls it
uses (`melin_dpdk::peer_liveness`, `replication/dpdk.rs`):

- **A deadline on a silent peer, in the TCP stack.** Every replication
  socket, at both ends, is armed once established with smoltcp's
  keep-alive (a probe a second on an idle link) and its timeout (the
  connection is reset once the peer has sent nothing for 5 s, the
  kernel-TCP receiver's quiet-primary figure). A peer whose stack
  answers is alive however quiet its application is; one that answers
  nothing is gone, and every link check then ends the session: the
  sender's slot goes `Idle` (the halt gate lowered, the replica
  uncounted), the receiver drops its primary link (auto-promotion may
  proceed). A link sending into a dead peer can take up to twice the
  timeout (smoltcp restarts the count when data is queued on an idle
  socket), never more.
- **Telling the peer.** Every replication close goes through
  `DpdkTransport::reset`, which sends the socket's RST before removing
  it: a slot dropped by the primary, a session the replica ends, and
  both ends' links when a node stops (the primary's poll loop resets its
  replicas' links as it exits; every receiver session exit, a stop or a
  promotion included, resets the link). Before the reset each waits up to
  100 ms for what it has queued to be acknowledged (the replica's final
  ack, the primary's last stream frames), the part of a kernel's flush
  before its FIN that matters here. An orderly stop is seen at once, as
  on kernel TCP; the deadline covers a crash, a cut link, and a lost RST.
- **Answering while busy.** The stack answers only when its thread polls
  it, and a replica's receiver thread also waits on local work: a push
  into a full input ring behind a slow journal, and the resync's
  teardown, snapshot load and seed install. Unanswered, the primary's
  deadline would drop a replica that is only slow — and halt, if it was
  the last — where a kernel-TCP replica stays connected behind a zero
  window. So the ring wait polls the stack on every pass
  (`ReceiverTransport::keep_link_serviced`), and the resync steps run on
  a helper thread while the receiver polls (`ControlFrameSource::serviced`,
  `run_serviced`); neither reads, so the window closes as the socket
  buffer fills, as a kernel's would. Kernel TCP runs both inline. A
  replica whose disk stalls long enough still fills the primary's
  replication ring and is evicted, on either transport.
- **Seeing the peer's FIN.** The link checks on both ends now ask
  whether both halves are open (`is_connected`), not whether the socket
  is active, so a peer's FIN (a kernel-TCP primary's close, for a DPDK
  replica) ends the session once the bytes before it are read, as EOF
  does on kernel TCP.
- **A monotonic stack clock.** The transport fed smoltcp the wall clock;
  a step forward would now reset every replication link at once, and a
  step back would hold a dead peer for as long as the step. The stack's
  clock is the wall clock's reading at start plus a monotonic count.
- **Promotion during a dial.** The receiver's connect loop also stops on a
  promotion request: a dial to a stopped DPDK primary is answered by
  nothing, and waiting out the connect timeout delayed the failover by
  as much.

Not serviced on the replica: once the stream starts, opening the
continuing journal writer and building the replica
pipeline (ring allocation, memory locking, thread spawn) run inline on
the receiver thread while the primary streams, so only a build that
stalled past the deadline would cost the link. That build is
ordinarily far quicker than the deadline, so it is left as is.

Nothing on the hot path: the probes and the timeout are timers the stack
already runs on its egress passes, the link check replaces the one each
tick already made, and the clock is read where it was.

Alternatives considered:

- **An application-level deadline on silence.** The primary heartbeats an
  idle link, so a replica could time its primary out on the heartbeat
  interval. The replica does not speak when idle (it acks data only), so
  the primary could not do the same without a new replica heartbeat in
  the wire protocol, and the replica's deadline would hang on an
  operator setting (`--replication-heartbeat-secs`) that was never meant
  as one. The TCP-level deadline works at both ends, needs no protocol
  change, and keeps the kernel-TCP path untouched.
- **Announcing the close only (FIN or RST on stop).** Covers an orderly
  stop, not a crash, which is the case failover exists for.
- **A graceful FIN instead of the RST.** Delivers what is queued and is
  what kernel TCP mostly sends, but the socket must stay in the set until
  the FIN is acknowledged, where every close here removes it at once. The
  RST is one segment; the final ack a replica owes on its way out is
  waited for (bounded) before it.
- **Changing `DpdkTransport::close` itself.** It is shared with client
  connections, whose silent close is a separate divergence ("DPDK closes
  a client connection without telling the peer"), left as it is: the
  new call is used by replication alone.

The decision itself is unit-tested without libdpdk: two smoltcp stacks
over an in-memory wire and a virtual clock (`peer_liveness` tests), for
a live idle peer kept, a stopped one declared gone within the bound
idle and while sending, a cut link seen at both ends, the announced
reset seen at once where a plain removal is not, and a peer's FIN. The
busy-replica servicing is unit-tested as well: a full-ring push that
completes only if the wait services the link, and `run_serviced`
servicing until its work is done. End to end, `halt_refusal` covers an
orderly stop on DPDK, and `dpdk_veth`'s
`a_replica_cut_off_is_dropped_and_its_primary_halts` the deadline: the
replica's veth is taken down under it (`melin_test_node::Node::cut_off`),
and its primary must drop it within the bound and refuse writes. Only
the primary's end is checked there; the replica's (its
`primary_link_up`) has no gauge to read, and is covered by the failover
tests instead, which run on DPDK since a promoted replica serves there
(below): `raft_failover` stops its primary, whose links are reset as it
goes, and `replicated_failover` cuts its primary off before stopping it,
so that no RST arrives. Auto-promotion refuses while a replica's
primary link is up, so the second failover completes only once a
replica has dropped the link on its own deadline.

#### A replica's join off the primary's poll thread

The deadline made a stall on the DPDK primary's poll thread cost links:
the thread is the sender, the client loop and the only thing running
the stack, and a replica's join did its file I/O on it — the catch-up
probe, the snapshot pre-flight, the snapshot's and the segment seed's
whole-file reads, and every journal read of the catch-up. A stall past
the 5 s deadline (a large snapshot on a cold or slow disk) made every
other DPDK replica reset its link to a live primary, and the primary
halt if that left it none; short of that, it froze client traffic and
the other replica's stream for as long as the disk took. Kernel TCP has
a thread per replica and a kernel answering for the process.

What was built (`replication/join_worker.rs`, the `Joining` slot state
in `replication/dpdk.rs`): a parked join worker per slot, spawned with
the driver (as the validation worker is, for the same reason: a thread
created from the pinned, real-time poll thread never runs), does the
join's disk side — the same steps, in the same order, with the
pre-flight still ahead of the resync verdict — and encodes its frames
into a bounded channel (a few frames deep, buffers recycled). Once the
handshake is validated the slot goes `Joining` and each tick moves what
the worker has ready into its socket, as TX space allows and at most a
tick's byte budget, then returns: the poll thread goes on serving
clients, the other slot's stream and every link's probes and ACKs while
the disk takes what it takes. A frame the socket refuses is held and
sent first next tick; one larger than the socket's queue can ever take
fails the join rather than wedging it. When the worker reports the end,
every frame is already queued in order, and the slot runs the bridge
into the live ring as before. A slot that drops mid-join cancels it (the
worker stops at its next frame); the slot drops its stream before its
worker, so a worker blocked on a full channel is released before it is
joined.

Alternatives considered:

- **`run_serviced` around the inline steps**, as the receiver's resync
  does. It keeps the stack answered, but holds up client traffic and
  the other replica's stream for the whole step (that replica is then
  evicted when its ring fills, and clients see the stall), and it
  spawns its helper from the calling thread, which on the primary is
  the pinned, real-time poll thread whose children never run.
- **Reading the snapshot in chunks between polls.** Bounds the join's
  memory to a chunk, but a single read on a cold or slow disk still
  blocks the poll thread, and the probe, the pre-flight, the seed and
  the catch-up reads stay inline. The worker covers every step at once.

Open follow-up: the snapshot's and the segment seed's whole-file reads
remain, now on the worker. That objection applies to chunks read between
polls, not to chunks read on the worker, where chunking is still worth
doing on its merits: it would bound the sender's peak memory to a chunk
instead of a whole body, on both transports (the transfer code is
shared), and shorten the one wait on the disk a stop still has — a
slot's worker joined mid-read waits out that read. It is a change to
the shared transfer code (`snapshot_transfer_with` and the seed prefix),
not to the poll thread, and is left for its own change.

Audit of what else the poll thread does that could block for long:

- The chain validation: on its worker already.
- The bridge into the live ring (`bridge_catchup_to_live`) runs inline,
  because it owns the slot's ring consumer and active flag. It re-reads
  only what was journaled since the worker reached the end of the
  journal (a few frames of pumping ago while the joiner drains at wire
  speed; a backpressured joiner stretches that to its drain time, which
  the residual pass then re-reads and publishes inline — still shorter
  than the whole catch-up, and recent, so in the page cache), polls the
  stack after every frame, and its wait on the disk is bounded (30 ms).
  The links stay answered throughout, but the client loop sharing the
  thread is held while the bridge runs, and the bridge waits for room in
  the joiner's send queue. A joiner that dies mid-bridge holds client
  ingress until its liveness deadline resets the link — up to about twice
  the liveness timeout, since data is being sent into it. A joiner that
  is alive but stops reading keeps acknowledging with a zero window, so
  its deadline never fires: ingress is held until it drains, with no
  bound but the joiner. Pre-existing, and DPDK only (the kernel-TCP
  sender bridges on the replica's own thread, off the client path); a
  deadline on the bridge's sends is an open follow-up.
- Streaming, acks, heartbeats, auth (one signature check), accepting
  (one nonce), dropping a link: no I/O, no waits.
- Closing every link on stop waits at most 100 ms, once, on the way out.
- The client loop sharing the thread: every hand-off (the input ring,
  the refusal queue, the tick, the response queue, the control channel)
  is a non-blocking push or an unbounded send; nothing reads a file.
- Logging: a log line on this thread is written by the application's
  subscriber, which the embedding binary chooses; lines here are
  lifecycle and per-connection events, not per-message.

The hot path is unchanged: the streaming arm is untouched, and a slot
pays for the join's channel only while it is `Joining`.

The join worker's mechanics are unit-tested without libdpdk
(`join_worker` tests: frames in order, a pump that never waits on a
stalled job, a refused frame resent first, the tick budget, an
abandoned join freeing the worker, a slot dropped mid-join not hanging,
the join's steps against a real journal). End to end, `dpdk_veth`'s
`a_stalled_join_costs_the_other_replica_nothing` stalls a divergent
replica's join on the primary's snapshot (a FIFO the test holds open
and never writes into) for twice the liveness timeout and more, while
a request is answered promptly every few hundred milliseconds and both
replicas stay counted; with the join's wait put back on the poll thread
it fails on the first request. `large_joins_complete_a_tick_at_a_time`
brings up a fresh replica (journal catch-up) and a divergent one
(snapshot, a seed of several MiB, catch-up), each many times a socket's
queue, and checks both ack the primary's whole history.

#### A promoted replica on DPDK

The problem: a promoted DPDK replica ran the kernel-TCP primary, which
binds kernel listeners on the client address and `--replication-bind`.
Only the DPDK port holds those, so the bind failed and the node exited
instead of serving; where the kernel did hold the address, the node
served on kernel TCP, giving up kernel bypass without a word. A
promoted kernel-TCP replica serves on the transport it ran on.

What was built (`server.rs`):

- **One DPDK primary function for both entries.** The part of
  `run_dpdk_impl` that serves as a primary is now `run_as_primary_dpdk`,
  the twin of the kernel-TCP `run_as_primary`, with the same parameters
  bar the bring-up gate's: the pipeline, the response stage and its ack
  gate, the halt gate, the replication driver (its liveness deadline and
  join worker included), shadow snapshots, health, ticks and the poll
  loop. The normal startup calls it after `init_engine` and the raft
  driver, with no promotion; the promotion arm calls it with the
  receiver's application and journal writer and the promotion request's
  epoch floor. The epoch bump, which was inline in `run_as_primary`, is
  a function both call (`journal_promotion_epoch_bump`), so the two
  transports mint and order it the same way: after the pipeline is up,
  before `on_primary`, before the first client or replica is served.
- **The replica's transport, reused.** The receiver borrows the queue-0
  transport rather than consuming it, and resets every link it opened
  before it returns. The replica builds that transport with no listener
  (`DpdkTransport::from_shared_unlistening`), where it used to listen on
  a stray port nobody read; at promotion it gains the client listener
  `from_shared` gives a booting primary, and `run_as_primary_dpdk` adds
  the replication listener as it does at boot. EAL, ports and pool are
  never touched again (EAL cannot be initialised twice), and the
  receiver's stack, with its neighbour cache, carries on as the poll
  thread's. A gratuitous ARP goes out again, as at boot.
- **Everything else as on kernel TCP.** The promotion arm runs the same
  steps in the same order as the kernel-TCP one: the genesis check
  (`check_promotable`), the replica health endpoint released for the
  primary's, sizing, the latched `ROTATE` cleared; the fence state, the
  advertised tip, the raft driver and the admin endpoint (with its
  `PROMOTE` flag) carry over. The receiver's pipeline is torn down
  before the primary's is built, by the shared
  `take_pipeline_for_promotion`, so the history continues in the same
  journal from the writer's next sequence: nothing lost, repeated or
  reordered. One DPDK-only step: the main thread ran the receiver,
  pinned to the reader's core once it streamed, and is unpinned before
  it spawns the primary's threads (a child of a pinned real-time thread
  never runs to pin itself), then pinned again as the poll thread.
- **Clients before the promotion.** A replica serves no client on either
  transport. On kernel TCP its listener is bound from boot, so a connect
  completes in the kernel's backlog and is served if the node is
  promoted while it waits; on DPDK there is no listener until the
  promotion, so a connect is refused. Clients retry either way, and the
  tests' "serves clients" probe reads both as not serving.

The normal DPDK primary startup behaves as before, with two changes of
order, neither visible to a client or replica: the admin endpoint is
spawned before the pipeline is built rather than just after (the order
the kernel-TCP primary has always used; a failed admin bind now refuses
the boot before any pipeline thread exists), and the one-queue check
returns an error before anything is spawned instead of asserting after.
The `on_primary` drain, the poll loop and the hot path are untouched;
promotion adds nothing to them.

Alternatives considered:

- **A second, promotion-only DPDK primary function.** Simplest to
  write, but two copies of the primary's assembly drift apart, and a
  promoted node would be a different primary from a booted one.
- **A fresh transport at promotion** (`from_shared` on queue 0 after
  dropping the replica's). It would also reuse EAL, ports and pool, but
  it rebuilds the stack under a queue another stack had been polling,
  and throws away its neighbour cache and clock for nothing; reusing the
  one transport keeps a single owner of the queue throughout.
- **Listening on the client port from boot**, as the kernel-TCP replica
  holds its listener. On DPDK nothing else can take the port, so there
  is nothing to hold, and established connections nobody accepts would
  pile up in the replica's socket set without the bound a kernel backlog
  has.

Coverage: `genesis_promotion`'s
`a_replica_configured_with_a_larger_genesis_is_promoted` and the notary
example's `a_promoted_replica_reports_the_head_the_primary_receipted`
(operator `PROMOTE`, then clients served by the promoted DPDK node, its
state and chain continuing the primary's), `raft_failover` (an
auto-promotion after an orderly stop, the loser still following, the
revived ex-primary fenced), and `replicated_failover` (an auto-promotion
after the primary is cut off, every event acknowledged under `ram`
present on the new primary). All four run on DPDK.

## Limits

- **Logic, not hardware.** af_packet is not a NIC: this checks logic, not
  latency or hardware offloads (see `dpdk-veth-testing.md`).
- **CPU.** Every DPDK node busy-polls a core. The DPDK run is serial, and if
  it grows too slow for pre-merge CI it moves to nightly.
- **Host.** The host must allow unprivileged user namespaces, as for the veth
  harness, and have the veth and bridge drivers loaded.
- **Slots.** The runner builds three node slots, the largest cluster a test
  runs. A test that needs more needs the runner to build more.
