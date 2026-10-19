# Testing the DPDK transport without a NIC

The integration suites written for kernel TCP also run on the DPDK
transport, over veth links in unprivileged namespaces, with no NIC, no
hugepages and no root. A test that starts its nodes through
`melin-test-node` runs on DPDK when its crate's `dpdk` feature is enabled,
unchanged. The suite we already trust is then the DPDK suite: a test that
fails there is either a divergence between the transports
(`transport-divergences-2026-10.md`) or a test of kernel-TCP internals.

## Running it

From the repository root, with libdpdk and libclang installed:

```sh
CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER="$PWD/scripts/dpdk/netns-runner.sh" cargo nextest run --profile dpdk -p melin-server-runtime --features dpdk -E 'kind(test) | (kind(lib) & test(dpdk))'
```

The runner variable names the target triple: on an aarch64 host it is
`CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_RUNNER`, with the same value. The
filter takes the integration suites and, of the library's unit tests,
those whose path names DPDK: they compile only with the feature, and
some run a DPDK transport (see "A node's NIC without a node" below).

The examples run the same way with their own feature
(`-p melin-example-echo --features melin-example-echo/dpdk`). The `dpdk`
nextest profile runs one test at a time and leaves out the tests that fail
on a known divergence (below). The CI `dpdk` job runs both, on every push,
and `melin-client`'s own suite with its `dpdk` feature
(`-p melin-client --features melin-client/dpdk`), the build a DPDK client
links.

Without nextest, `cargo test` takes the same runner, with two caveats
(see "One process per test" under Limits): pass `-- --test-threads=1`,
since the tests of one binary then share a process and its slots, and
run the runtime's `dpdk_veth` suite one test per process (`--exact`, test
by test, from its `--list`), since a test that leaves a slot held fails
the ones after it. Nor does `cargo test` read the profile's filter, so a
test the profile leaves out on a divergence runs, and fails, there.

## What makes DPDK run on veth

Inside an unprivileged user, network and mount namespace (`unshare -rnm`):

- **Link.** veth pairs are created over rtnetlink, so no `ip` binary is
  needed. A node's end is left up without an address, for DPDK; the
  kernel side carries the client's address.
- **Driver.** The node runs DPDK on its end through the `net_af_packet`
  PMD. EAL arguments: `--no-huge -m <MiB> --no-pci -l <core>`, the core
  being the first CPU the process may run on, not CPU 0, which a
  restricted host need not allow. `--in-memory` cannot be used: EAL refuses
  it with `--no-huge`, which implies legacy memory.
- **Runtime directory.** EAL insists on creating `/var/run/dpdk`, and inside
  the user namespace it believes it is root. A private tmpfs mounted on
  `/var/run` satisfies it.
- **Checksums.** TX checksum offload must be off on both ends of every veth
  (the `ETHTOOL_STXCSUM` ioctl). Otherwise the kernel hands frames to
  af_packet with partial TCP checksums, the userspace stack drops them, and
  connects time out.

## The runner

`scripts/dpdk/netns-runner.sh` is set as cargo's target runner
(`CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER`, or the aarch64 one, which
nextest honours). For every process it starts (every test, under nextest)
it:

1. re-executes itself under `unshare -rnm`;
2. builds a bridge, `br0`, carrying the client's address (`10.99.0.1/24`),
   with one veth pair per node slot: `dpdkN` for the node, its peer
   `dpdkN-br` on the bridge, node `N` on `10.99.0.(N+2)`. Three slots, the
   largest cluster a test runs. One more pair, built exactly as a slot's,
   is the DPDK client's: `dpdkc`, its peer `dpdkc-br`, the client on
   `10.99.0.100`. Every MAC is fixed (`02:99:00:00:00:NN`, the last octet
   the IP's) and TX checksum offload is off on every veth
   (`scripts/dpdk/veth-setup.py`, Python's standard library only);
3. mounts the private tmpfs on `/var/run`;
4. publishes the layout in the environment: `MELIN_NETNS_DPDK_IFACES`,
   `MELIN_NETNS_NODE_IPS`, `MELIN_NETNS_NODE_MACS`, `MELIN_NETNS_PREFIX_LEN`,
   `MELIN_NETNS_CLIENT_IP`, `MELIN_NETNS_CLIENT_MAC`, as space-separated
   lists with one entry per slot, and the DPDK client's pair apart from
   them: `MELIN_NETNS_DPDK_CLIENT_IFACE`, `MELIN_NETNS_DPDK_CLIENT_IP`,
   `MELIN_NETNS_DPDK_CLIENT_MAC`, one value each;
5. runs the test binary.

nextest runs each test in a process of its own, so each test gets a fresh
network and the tests need no namespace code. The runner does not `exec`
into `unshare`, so that when a run fails it can check whether the
namespaces were what failed and say how to allow them. A `TMPDIR` under
`/run`, which the tmpfs would hide, is moved to `/tmp`.

The MACs are published because a DPDK replica must be given its primary's
(`--dpdk-peer-mac`), and a DPDK client its node's (`NicConfig::peer_mac`),
as on any port that keeps a hardware address. They are deliberately not
the `02:00:<ip>` convention a dialer falls back to without one, so a test
passes only if the peer MAC was passed through. A unit test of the
launcher's layout pins that for every address published.

The DPDK client's pair is a set of its own rather than a fourth node slot,
so nothing that counts the slots (the launcher's search for a free one)
can start a node on it, and the slots keep their names and numbers. The
kernel client's address on the bridge stays, for every test whose client
is not on DPDK; the cluster tests' own peers bind there too. A runner that
publishes no client pair (one older than it) still runs every node test:
the launcher reads the three variables together or not at all, and only a
test that asks for a DPDK client fails without them, saying why.

## The launcher

`crates/core/test-node` (`melin-test-node`, dev-only, `publish = false`)
starts a test's nodes from a `ServerConfig`:

- **Kernel TCP**, by default: it binds the client listener on `127.0.0.1:0`
  and runs `server::run_with_listener`, as the tests always did.
- **DPDK**, under its `dpdk` feature: it fills in the DPDK fields (EAL
  arguments, IP, prefix, port, peer MAC) from the runner's layout and runs
  `server::run_with_shutdown`. Without the runner it fails, saying how to
  run the test.

It is a crate rather than a feature of the runtime so that the runtime's
own integration tests can use it, through a path-only dev-dependency (the
cycle the counter example already forms), without a test-only switch on a
published crate. Each example has a `dpdk` feature forwarding to
`melin-server-runtime/dpdk` and `melin-test-node/dpdk`, and the runtime's
own `dpdk` feature switches its tests over the same way.

- **Addresses.** `start::<A>(config, ...)` starts a node and returns a
  `Node` with `addr()` and `stop()`. A cluster first takes each node's
  addresses with `addrs(slot, free_addr)`, so that it can wire
  `replication_bind` and `replica_of` the same way on both transports, and
  starts each node with `start_at`. On kernel TCP the addresses are ports
  from the test's own allocator; on DPDK, the slot's IP with fixed ports.
  The launcher refuses addresses off the runner's network, saying how to
  take them from `addrs`. `local_ip()` is where a test binds a socket a
  node must reach.
- **Slots.** On DPDK a node holds its slot until it is stopped or joined.
  Starting a second node on a held slot panics.
- **Stopping.** A node is stopped by setting its shutdown flag, on both
  transports. `Node::join` waits for a node that returns on its own (one
  expected to fail, or to fence itself).
- **Cutting a node off.** `Node::cut_off` brings the bridge side of its
  link down, as a pulled cable would: nothing it sends arrives, and no peer
  is told.
- **Startup.** `STARTUP_LIMIT` is how long a client should give a node to
  serve: longer on DPDK, for EAL and port start.
- **A client on DPDK.** `open_dpdk_client(config)` (DPDK only) opens a
  `melin_dpdk::NicHandle` on the runner's client pair: an af_packet device
  attached for `dpdkc` on the process-wide EAL the nodes share, under a
  name no slot's device takes (`net_af_packet_client`, where a slot's is
  `net_af_packet<N>`). Like a node's, its EAL arguments are empty (the
  handle refuses any on the shared EAL), its port and address are the
  launcher's, and its mbuf pool is named after its port. The rest of the
  `NicConfig` is the test's, the peer MAC first: `mac_of(node.addr())`
  gives the node's, and `dpdk_client()` the client's own IP, MAC and
  prefix length. A plain handle, so a test dials on it with
  `StreamConnection::connect_dpdk_on(&mut nic, ...)`, or drives several
  streams on it as the exchange's bench does. One per process: the device
  stays attached as the EAL does, and a test reconnects on the handle it
  has. A test whose DPDK client is a child process takes
  `dpdk_client_process_eal_args(file_prefix)` for it instead, and then
  opens no handle of its own: the launcher panics on either call after the
  other.
- **A node's NIC without a node.** `dpdk_node_nic()` (DPDK only) takes
  the first free slot, attaches its device, and hands back the slot's
  `NicConfig` and address, for a test that builds a node's DPDK
  transport and runs its pieces itself. The runtime's unit tests of the
  poll loop's parking do (`dpdk_transport.rs`): they hold the DPDK
  response stage at a `cfg(test)` seam, which exists only in the
  runtime's own test build, not in the copy of the runtime a launched
  node runs. The slot is freed when the value drops, after the transport.

## Several nodes in one process

EAL initialises once per process and cannot be initialised again after its
cleanup, while the cluster tests run two or three nodes as threads of one
process, and some tests restart a node. So the launcher initialises a
process-wide EAL (`Eal::init_process_wide`, in `melin-dpdk`) once, on a
short-lived thread so that the test's threads keep every CPU, and never
cleans it up. Each node gets a virtual device and port of its own, attached
by hotplug as it starts (`Eal::attach_vdev`) and detached once it has
returned, so the next node on the slot can attach it afresh. A node on the
shared EAL takes no EAL arguments of its own, and names its mbuf pool after
its port.

This is opt-in and test-only. A deployed node never calls it, so
`DpdkShared::init` takes exactly the old path: the node initialises EAL
from its own arguments, owns it, and cleans it up last, after its ports and
pool. The cost to a deployed node is one check at start.

`server::run_with_shutdown` is what lets one node of several be stopped:
`run` installs a process-wide signal handler, which would stop only the
last node to install it.

Alternatives considered:

- **An EAL per node with `--file-prefix`.** EAL is a singleton per process
  whatever the prefix, which separates processes, not nodes in one.
- **The first node initialises EAL for the process.** It would change who
  owns EAL in every deployed node, for a test-only need.
- **Reference counting, with cleanup after the last node.** A restart after
  the last node stopped would need EAL again, which cannot be initialised a
  second time.
- **Keeping ports open and reconfiguring them on restart.** A stopped
  port's receive ring can hold mbufs from the pool its node just freed, and
  reconfiguring frees them into a freed pool. Correct for af_packet only,
  by accident.
- **A process per node.** The runtime would be untouched, but the tests
  read their nodes' state in-process and every cluster test would have to
  be rewritten around processes.

## The DPDK-only tests

`crates/core/server-runtime/tests/dpdk_veth/` holds the tests that pin what
the DPDK transport does where it differs from kernel TCP, so they exist only
on DPDK (`required-features = ["dpdk"]`: a plain `cargo test` never builds
them). Some assert behaviour that is a known divergence, a server-side close
that tells the client nothing for example, and are written to fail once the
divergence is fixed, saying how to flip them.

## A client on DPDK

Every other test talks to its DPDK nodes from the kernel side of the
bridge. The echo and counter round trips each have one case more, compiled
only with the example's `dpdk` feature (which turns on `melin-client/dpdk`
too): `a_dpdk_client_echoes_through_a_dpdk_node` and
`a_dpdk_client_round_trips_through_a_dpdk_node`, the client on DPDK
through `melin-client`'s polled connection on the launcher's client NIC,
against a node on DPDK. Each sends its requests, drops the connection and
dials again on the same handle, which the next connection of a real client
would do. The node's MAC is passed through from the layout; the case
asserts that it is not the fallback convention, so a dial that ignored it
would time out rather than pass. The echo case goes on to every way a dial
can fail, on the same handle: a port of the node nothing listens on (a
DPDK node's stack answers the SYN with an RST, so the dial is refused, not
timed out), an address on the runner's network nothing owns (timed out,
the error naming the MAC the SYN went to), and a key the node refuses;
after each, the handle dials and echoes again. Then it drives several
streams on the handle at once, the shape of the exchange's bench, which
otherwise runs only on a real rig: the streams dialed in turn, one
polled connection each, and one loop that services the handle once per
iteration and polls every stream, the handshakes inside the polls. Every
stream keeps several requests in flight together, each stream's payloads
its own, and every reply must come back on the stream that sent it, in
that stream's order; the journal holds each stream's echoes in its
order, the order across streams being the node's. The streams are closed
with the handle's own close, and the handle dials once more. One case
rather than several because the launcher opens the client NIC once per
process.

Each close is also checked to free the node's slot at once, which is
what the close's reset ordering (abort, egress pass, flush, then remove)
is for and which the in-memory wire alone tested before. The node runs
with a health endpoint on the namespace's loopback, and the case reads
its count of authenticated connections: it falls by one within a bound
of a couple of seconds after each of the streams is closed, and after a
connection that owns its stream is dropped. The node runs with no
heartbeat and no idle timeout, so nothing but the client's own close can
free a slot, and the case checks that the bound is well under the
defaults of both, which would free it eventually.

The launcher returns before a node serves, and the polled connection has
no twin of `Connection::connect_by` that retries until a deadline. The
cases wait for the node with the kernel client, which has, and then dial
on DPDK once, at a node already serving.

The shm-proxy's DPDK path has a test of its own,
`crates/tools/melin-shm-proxy/tests/dpdk_bridge.rs`, built only with the
proxy's `dpdk` feature. It starts an echo node on DPDK through the
launcher and runs the `shm-proxy` binary as a child, with
`--transport dpdk` on the runner's client pair: the binary owns that
pair's device in a process of its own, with an EAL of its own, so the
test opens no client NIC itself. `dpdk_client_process_eal_args` gives
the child's EAL arguments: the launcher's, the client's af_packet device
as the child's one port, and a file prefix apart from the test process's
in the shared private `/var/run` (the proxy's default names a hugepage
mount the runner has none of). The test then maps the link file from its
documented layout and is the client on the rings, as the Aeron harness's
Java rig is: the handshake, requests framed and replies split with
`melin-client`'s I/O-free pieces (a dev-dependency only; the binary
depends on no client crate), echoes of every size through rings small
enough to wrap, then the close asked through the header, a clean exit,
and the `connected to <addr> over DPDK` line the harness waits for.

`scripts/dpdk/dpdk-client-tap-smoke.sh` checks the same path outside the
rig: a DPDK echo client on a TAP virtual device (`--vdev=net_tap0
--no-pci`) against a kernel echo server on the TAP's other end, with no
NIC and no namespace, on any host with libdpdk and `/dev/net/tun`, as
root for hugepages and the TAP's address. The mirror image of the
Exchange Core's TAP smoke test, which runs a DPDK node and a kernel
client. The client is `echo-client --transport dpdk`, the one an
operator runs, built with the echo example's `dpdk` feature.

## The `dpdk` profile and known divergences

A test that fails on DPDK because of a documented transport divergence is
left out of the `dpdk` nextest profile (`.config/nextest.toml`), listed
under the divergence entry that explains it. The profile is therefore the
list of what does not work on DPDK yet. The fix for an entry removes its
tests from the profile's filter in the same change, which shows exactly
what the fix enabled. No test is gated in its own code, and none is
`#[ignore]`d.

## What stays kernel-only

- **Tests of kernel-TCP internals.** The `response_flush_*` tests drive the
  io_uring response stage directly, and so do some `server.rs` unit tests
  on real sockets. They run under the DPDK profile too, on kernel TCP.
- **Raft, health, admin and event endpoints.** These are kernel TCP by
  design, on the namespace's loopback.

## Limits

- **Logic, not hardware.** af_packet is not a NIC. These tests check the
  transport's logic, not its latency, and say nothing about offloads,
  multi-queue and RSS, `rte_flow` isolation (`--dpdk-peer-ip`), VLAN, LACP
  bonds or mlx5 bifurcation, which still need hardware.
- **Host.** The host must allow unprivileged user namespaces (Ubuntu 24.04
  restricts them through AppArmor, `kernel.apparmor_restrict_unprivileged_userns`)
  and have the veth and bridge drivers loaded, which a user namespace
  cannot do. Docker loads both; the CI job loads them and lifts the
  restriction.
- **CPU.** Every DPDK node busy-polls a core, so the profile runs one test
  at a time. A client on DPDK spins too, one more busy thread beside its
  test's nodes, on a CI runner whose cores are few. If the run grows too
  slow for pre-merge CI it moves to nightly.
- **Slots.** The runner builds three node slots and one DPDK client pair.
  A test that needs more needs the runner to build more.
- **One process per test.** Under plain `cargo test` the tests of one
  binary start their nodes in one process one after another, which works
  as long as no test leaves a slot held; a failing test can. nextest, one
  test per process, is the supported way to run them.
