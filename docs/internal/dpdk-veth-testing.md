# Plan: testing the DPDK transport on veth

The DPDK paths have almost no automated coverage. The unit tests cover the
pure pieces extracted from them, and everything else (the poll loop, the
response stage, the replication transport) has only been exercised by hand
on hardware. Several of the open entries in
`transport-divergences-2026-10.md` stay open for that reason.

A hand-run probe showed the whole DPDK path can run on an ordinary Linux
host without root, hugepages, or a NIC. This plan turns that into a smoke
script, a test harness, and a CI job.

## What the probe established

Inside an unprivileged user + network + mount namespace (`unshare -rnm`):

- **Link.** A veth pair is created over rtnetlink, with no `ip` binary
  needed. veth0 is left up without an address for DPDK; veth1 carries the
  kernel-side address for the client.
- **Server.** The server runs DPDK on veth0 through the `net_af_packet` PMD.
  EAL arguments: `--no-huge -m <MiB> --no-pci --vdev=net_af_packet0,iface=veth0
  -l <core>`. `--in-memory` cannot be used: EAL refuses it with `--no-huge`,
  which implies legacy memory. The core is the first CPU the process may
  run on, not CPU 0, which a restricted host need not allow.
- **Runtime directory.** EAL insists on creating `/var/run/dpdk`, and inside
  the user namespace it believes it is root. A private tmpfs mounted on
  `/var/run` satisfies it.
- **Checksums.** TX checksum offload must be off on both veth ends (the
  `ETHTOOL_STXCSUM` ioctl). Otherwise the kernel hands frames to af_packet
  with partial TCP checksums, the userspace stack drops them, and connects
  time out.
- **Result.** A kernel-TCP client on veth1 (`melin-client`, through the echo
  example) completes authenticated round trips. A client with an unknown key
  gets `AuthFailed`, and the server closes the connection once that frame is
  acknowledged.

## Limits

- **Logic, not hardware.** af_packet is not a NIC. These tests validate
  logic, not latency, and say nothing about the following, which still need
  hardware:
  - offloads, multi-queue / RSS;
  - `rte_flow` isolation (`--dpdk-peer-ip`);
  - VLAN, LACP bonds, mlx5 bifurcation.
- **Namespaces.** The host must allow unprivileged user namespaces. Ubuntu
  24.04 restricts them through AppArmor by default
  (`kernel.apparmor_restrict_unprivileged_userns`). A hosted runner has sudo
  and can lift the restriction for the job.
- **Cost.** Each server costs an EAL init and a busy-polling core, so the
  tests run serially and stay out of the default `cargo test`.

## Steps

### 1. Smoke script

`scripts/dpdk/dpdk-veth.sh`: builds nothing, takes the echo binaries built
with `melin-server-runtime/dpdk`, and re-executes itself under
`unshare -rnm`. Inside the namespace it sets up the veth pair and runs an echo
server on DPDK. It then checks two things:

- a client with a good key completes round trips;
- a client with an unknown key is refused.

It exits non-zero on any failure and fails loudly if the namespace cannot be
created. Network setup must not need iproute2 or ethtool. Python's standard
library (rtnetlink over `AF_NETLINK`, the ethtool ioctl) is acceptable for a
script.

As built, the keys are fixed in the script (the authorized one is the
openssl-generated PEM from `melin-client`'s key tests), so no key tool is
needed either. The unknown key must fail with "authentication failed", not
with any error, and the server must then exit cleanly on SIGTERM.

### 2. Test harness

A test target with `required-features = ["dpdk"]`, so plain `cargo test` never
builds it.

- **Re-exec under namespaces.** EAL initialises once per process. The test
  runner is multithreaded, and `unshare(CLONE_NEWUSER)` needs a
  single-threaded caller. So each test re-executes its own binary under
  `unshare -rnm`, filtered to itself and with a marker environment variable.
  The parent asserts the child's exit status; the child does the real work.
- **Network setup in Rust.** The child builds the veth pair, address and
  checksum setting with a small rtnetlink + ioctl helper over `libc`, so no
  new dependency and no Python.
- **Did the child run?** A filter that matches nothing makes the child exit
  0 having run no test. So the child also writes a marker file once the
  test body has passed, and the parent requires both.
- **Server in-process.** The server runs inside the child, through the
  same entry point as the binaries (`server::run`), and stops on SIGTERM
  sent to the child itself, because that entry point owns its shutdown
  flag. The first draft meant to read slot counts off the health
  endpoint, but it counts authenticated connections only, so a refused
  connection's slot never appears there. The node runs with
  `max_connections = 1` instead, and a slot is free when the next
  authorised client gets in. That check must finish inside
  the 5-second auth timeout, which frees an unclosed slot on its own.
- **Clients.** `melin-client` for well-behaved clients; raw sockets for
  misbehaving ones.
- **No silent skip.** A missing namespace capability fails the test with a
  message saying what the host lacks. Per CLAUDE.md, no `#[ignore]`.
- **Serial.** A nextest test group with `max-threads = 1` (`dpdk-serial`,
  next to `cluster-serial` in `.config/nextest.toml`) keeps the tests
  serial. Under a plain `cargo test --features dpdk`, a lock in the parent
  does the same.
- **Bounded.** The parent kills a child that runs past a limit set under
  nextest's slow-timeout, so a hang is reported by the test, with the
  child's output.

The harness is `crates/core/server-runtime/tests/dpdk_veth/`. First tests,
chosen because each pins a known behaviour:

- **Bad key.** A client with an unknown key gets `AuthFailed`, and the server
  closes its connection and frees the slot.
- **Retry after failure.** A client that sends a second ChallengeResponse after
  a failed one gets no second `AuthFailed`. The connection closes. Two shapes:
  the retry pipelined behind the failed attempt, and sent after the verdict.
  The retry is signed by an authorised key, so a node that read it would
  answer `ServerReady`.
- **Close is silent.** After a server-side close the client sees neither FIN
  nor RST, and learns of the close only from the RST that answers its next
  send. This pins the documented divergence, and the test flips when that
  is fixed.

Writing these turned up a fourth, now pinned too:

- **Client close unseen.** The node does not notice an authorised client's
  FIN. Its slot comes back only when the next heartbeat is answered with an
  RST. Until then the node turns new connections away at `max_connections`,
  so every test's connect helper retries rather than expecting the slot at
  once. The test flips when the transport handles the close.

### 3. CI

The existing `dpdk` job in `.github/workflows/pre-merge.yml` already
installs the SDK. It gains three things:

- a step lifting the AppArmor user-namespace restriction;
- a step running the DPDK test target serially;
- an update to the comment saying a hosted runner cannot run DPDK.

If that pushes the job too long, the tests move to `nightly.yml` instead.

### 4. Later: two-node replication

Two DPDK nodes on the two ends of one veth pair, in one namespace, each with
its own EAL `--file-prefix`. This would let the tests cover:

- the replication handshake;
- the sender's missing `Handshaking` deadline;
- `queue_send` failures.

Unverified so far.

## Divergences this makes testable

From `transport-divergences-2026-10.md`:

- the pipelined first request after auth;
- `PipelineFull` closing instead of `ServerBusy`;
- the silent close (now pinned by `a_server_side_close_is_silent`);
- the unseen client close (now pinned by
  `a_client_close_is_seen_only_at_the_next_heartbeat`);
- the heartbeat drop when the SPSC ring is full (needs a small ring);
- with step 4, the DPDK replication entries.

Each of those fixes would land with a test on this harness.
