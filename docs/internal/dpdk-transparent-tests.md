# Plan: the kernel-TCP test suite, on DPDK

Goal: an integration test written against kernel TCP runs against the DPDK
transport when the `dpdk` feature is enabled, unchanged. The suite we already
trust then becomes the DPDK suite. Any test that fails there is either a
divergence to fix or a test of kernel-TCP internals.

This builds on the veth harness (`dpdk-veth-testing.md`), which showed a DPDK
node can run on a veth pair through `net_af_packet`, unprivileged, with no
hugepages and no NIC.

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

"Enabling the feature" means `--features melin-server-runtime/dpdk` on the
test command (or an example-level `dpdk` feature forwarding to it).

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
