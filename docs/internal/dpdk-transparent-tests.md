# Plan: the kernel-TCP test suite, on DPDK

Goal: an integration test written against kernel TCP runs against the DPDK
transport when the `dpdk` feature is enabled, unchanged. The suite we already
trust then becomes the DPDK suite. Any test that fails there is either a
divergence to fix or a test of kernel-TCP internals.

This builds on the veth harness (`dpdk-veth-testing.md`), which showed a DPDK
node can run on a veth pair through `net_af_packet`, unprivileged, with no
hugepages and no NIC.

Status: step 1 is done and passes on a developer host; its CI step is
written but not yet proven on a hosted runner. Steps 2 and 3 are not
started. Where step 1 settled something the design left open, or turned
out differently, its section says so.

To run the example suites on DPDK, from the repository root (one test at a
time, as every node busy-polls a core):

```sh
CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER="$PWD/scripts/dpdk/netns-runner.sh" cargo nextest run -p melin-example-echo -p melin-example-counter -p melin-example-notary --features melin-example-echo/dpdk,melin-example-counter/dpdk,melin-example-notary/dpdk -j 1
```

For one example, `-p melin-example-echo --features dpdk` is enough.

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
  port start.
- **Stopping a DPDK node.** `server::run` owns its shutdown flag, so the
  launcher stops it as an operator would, with SIGTERM to the process. It
  first waits for `run` to have installed its handler (it reads SIGTERM's
  disposition), since a SIGTERM before then would kill the test process.
  No production change was needed. One window is left: a node already
  tearing itself down after an internal failure, but not yet returned,
  takes the SIGTERM as a second signal and exits the process with status 1,
  so a test that is failing anyway loses the node's error message. Closing
  it needs a way to stop `run` without a signal.
- **One node per process.** Under the feature the launcher refuses a second
  node in one process, saying why, instead of letting EAL fail. Under plain
  `cargo test`, which runs every test of a binary in one process, all but
  the first node-starting test fail that way: the DPDK run is nextest only.
- **The feature.** Each example has a `dpdk` feature forwarding to
  `melin-server-runtime/dpdk` and `melin-test-node/dpdk`; the launcher
  takes the transport from its own feature. Enabling only
  `melin-server-runtime/dpdk` builds DPDK into the binaries but leaves the
  tests on kernel TCP (`run_with_listener` ignores the feature), so the
  example feature is the switch.
- **EAL arguments** are the veth harness's: af_packet on the slot's
  interface, `--no-huge -m 512 --no-pci`, main lcore on the first CPU the
  process may use. The client port is fixed: a node owns its IP outright.

### EAL: process-wide (step 2)

One EAL per process, initialised by the first node with every port the runner
built, and cleaned up only at process exit. Each node takes a port of its own.
The runtime already shares one EAL between a node's client and replication
ports (`from_shared_with_port`); this makes the sharing process-wide.

This is the one change to production code. The production path, with one
node per process, must keep its behaviour exactly, including teardown order
(see the note on vdev PMDs in `crates/core/dpdk/src/dpdk/port.rs`).

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
landed.

The CI `dpdk` job checks the examples and the launcher with their `dpdk`
features, and runs the three example suites through the runner with
`-j 1`.

### 2. Process-wide EAL and a bridge

- The EAL change above.
- A bridge with one veth per node slot in the runner.
- The cluster and restart tests ported to the launcher: `replicated_failover`,
  `halt_refusal`, `genesis_promotion`, `sizing`, `startup_events`, the raft
  tests.

### 3. Divergences

Gate whatever fails on its divergence entry, then fix the divergences one at a
time. Each fix removes a gate.

## Limits

- **Logic, not hardware.** af_packet is not a NIC: this checks logic, not
  latency or hardware offloads (see `dpdk-veth-testing.md`).
- **CPU.** Every DPDK node busy-polls a core. The DPDK run is serial, and if
  it grows too slow for pre-merge CI it moves to nightly.
- **Host.** The host must allow unprivileged user namespaces, as for the veth
  harness.
