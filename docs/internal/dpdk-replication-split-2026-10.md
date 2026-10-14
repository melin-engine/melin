# Replication off the DPDK client poll thread (plan)

Status: **planned** (2026-10), on branch `feat/dpdk-replication-split`,
stacked on `feat/lan-bench-suite` so that the suite can compare the
variants on one rig. Implements the roadmap item "Split replication off
the DPDK client poll thread".

The one-line goal: **on DPDK, a primary's replication runs on a core, a
queue and a TCP stack of its own, chosen by a flag, so that one ephemeral
rig measures it against today's shared thread.** Implement first, then
measure. Today's shared thread stays the default until the measurement
says otherwise.

Not a goal: more than one client poll thread (RSS across client queues).
The input ring has a single producer, and the clock tick is generated on
the client poll thread. Both stay as they are.

## What the code shows

- **The driver is already transport-agnostic.** `DpdkReplicationDriver`
  owns both slots' state machines and the cursors, but takes the
  `DpdkTransport` as an argument to `accept_connection`, `tick` and
  `close_all_links`. Moving it to a thread of its own is wiring, not a
  rewrite.
- **It is called from three places in `run_dpdk_poll`**: accepts are
  dispatched by listen port, `tick` runs after the mid-loop poll every
  `POLL_EVERY_N_CONNS` connections, and again at the end of each
  iteration. The mid-loop call exists only because, under client load,
  one tick per iteration let the ring fill and replicas were evicted.
  That is the coupling this work removes.
- **The split existed until `d9742a79` (2026-04-28).** Two queue pairs
  and two threads, steered by the NIC. It was merged into one thread
  because IAVF (SR-IOV) steering stalled sustained streams. SR-IOV is no
  longer deployed: the targets are mlx5 in bifurcated or dedicated mode
  (Latitude.sh) and ENA (AWS), where more ENIs can be added. Remnants of
  that design remain: `DpdkShared` with `from_shared(queue_id)`, and a
  `--cores` test that refuses the retired `repl-sender` entry.
- **The ack path needs no new hop.** Acks update `ReplicaSlotCursors`
  atomics, which the response stage's durability gate reads. The gate is
  already on another thread, so moving the ack reader to a third thread
  adds no cross-core transfer that the shared design does not already
  pay.
- **A replica runs its receiver on queue 0's transport, unlistening,
  and is promoted on that same transport** (`server.rs`: listeners are
  added at promotion). A split replica must bring up both transports at
  boot, because ports and queues are configured once, in `DpdkShared::init`.
- **`DpdkShared` is one EAL, one mempool and one MAC for all its ports.**
  `port_ids` is a list of LACP bond members, not independent NICs. A
  second ENI has its own MAC and IP, and EAL can only be initialized
  once per process.
- **Queue mode cannot be tested under the netns runner.** The runner's
  `net_af_packet` vdevs have no `rte_flow` and one queue. The port mode
  can: a second veth per node slot is a second port.
- **Non-EAL threads get no mempool cache.** A new `std::thread` has
  `LCORE_ID_ANY`, so every mbuf alloc and free goes to the pool's ring.

## Design

### Three modes, one per flag

| Mode | Flag (node only) | Where replication runs | For |
|---|---|---|---|
| `shared` | none (default) | the client queue and thread, as today | baseline |
| `queue` | `--dpdk-replication-queue` | queue 1 of the client's ports, steered by `rte_flow` | mlx5 |
| `port` | `--dpdk-replication-ports`, `--dpdk-replication-ip`, `--dpdk-replication-peer-mac` | its own port, MAC and IP | ENA (second ENI), any NIC, the netns runner |

These are node flags in the server's config, not `DpdkArgs`, which clients
flatten too. In `port` mode the replication NIC inherits prefix length,
gateway, gateway MAC, MTU and VLAN from the client NIC. The first cut
requires the replication ENI to be in the same subnet. The modes are
mutually exclusive, and the mapping refuses a combination, as `DpdkArgs`
refuses bad flags: before EAL init and before any thread starts.

`NicConfig` gains nothing. The node's `DpdkConfig` gains a
`replication: ReplicationNic` enum (`Shared`, `Queue`, `Port(NicConfig)`),
plain data outside the `dpdk-sys` gate like the rest of the config.

### The replication poll thread

- **Placement:** a new `--cores` entry `repl-poll`, core 5 by default
  (the core the retired `repl-sender` had; the default layout leaves it
  free). It always busy-spins, as the reader does on DPDK, and is used only
  on DPDK in `queue` or `port` mode. It is a new name, so the
  retired-name test still holds.
- **Ownership:** the thread builds its own `DpdkTransport` from the
  shared resources (queue 1, or the replication port's), calls
  `rte_thread_register` so that it gets an lcore id and a mempool cache,
  listens on the replication port, and loops: poll, accept, `tick`. On
  shutdown it calls `close_all_links`, as the client poll thread does
  today.
- **The client poll thread** is passed `repl_driver: None` in these
  modes, and its replication code paths become dead there. In `shared`
  mode nothing changes.
- **Observability:** a `latency-trace` stage for the replication loop's
  work iterations, beside the client loop's. Per-queue RX and TX packet
  counts from `rte_eth_stats` go on `/metrics`. Misrouted steering is not
  silent: a replication segment that lands on the client stack gets a
  RST, and the link drops. The counters show which queue saw what.

### Steering in `queue` mode (mlx5)

Two queues, and no RSS: the client queue must take everything that
is not replication.
- **Non-isolated (dedicated port):** RSS mode (ethdev 22.11 refuses a
  RETA update without it), and all RSS RETA entries pointed at queue 0,
  after the port starts (mlx5 sizes its table then, and restarts the
  port to apply an update) and before any rule. The port requests no
  hash field, but that is not relied on: a PMD may read an empty set as
  its default (mlx5 is believed to hash IP then), so until the pin a
  packet can reach queue 1; once pinned, the hash no longer matters. On
  the rig, check the per-queue counters right after start.
- **Isolated (bifurcated):** the existing source-IP rule, below the
  replication rules, so that replication from the peer's host still
  reaches queue 1.

The replication rules:
- TCP destination port = the node's own replication port (from
  `--replication-bind`) → queue 1 (its replicas, as primary or once
  promoted);
- TCP source port = the primary's replication port (from `--replica-of`)
  → queue 1 (a replica's link to its primary). It need not be the
  replica's own replication port, so a replica may need both rules.

In isolated mode both also match the node's own destination IPv4, so
that replication from every replica host is captured, whatever the peer
IP. On a replica, a rule matching the client port, destination → queue 0
(isolated: from the peer only, capturing nothing new), ranks above the
source-port rule, so a client whose ephemeral port happens to equal the
primary's replication port still lands on queue 0. The mirror case, the
replica dialing its primary from the client port, looks the same to the
NIC: **step 6's replica must not dial from the client port** (today's
dial loop starts at 40000 and wraps there, so a client port in that range
can collide).

Priorities are numbered densely from 0 and only as many are used as the
rules need: two in queue mode, three on an isolated replica. mlx5's root
table offers few user levels (as few as two, depending on device and flow
engine): check the isolated replica's third level on the rig. Queue mode
with no replication port at all (a standalone node), or with the node's
replication port equal to its client port, is refused before EAL init.

Which rules a mode installs is a pure function of (mode, client port,
replication ports, peer IP, own IP), outside the gate and unit-tested
(`melin_dpdk::steering`). The FFI only installs what it returns.
`Port::configure` takes the queue plan rather than a bare `num_queues`.

### `port` mode: one EAL, several NICs

`DpdkShared` is split into the EAL, acquired once, and a NIC group: ports,
a mempool on the group's NUMA node, and the MAC. A node in `port` mode
brings up two groups on one EAL. The EAL arguments must allowlist both
devices (ENA: both PCI addresses; the runner: both vdevs). `shared` and
`queue` modes bring up one group, as today.

### Replica and promotion

In `queue` and `port` modes, a replica brings up both transports at boot:
- **The replication transport** carries its receiver. The replica dials
  the primary's replication address from its own replication NIC or
  queue, with `--replica-of` naming that address.
- **The client transport** stays unlistening and unpolled until promotion.
  Clients do not connect to replicas. What arrives meanwhile is dropped
  at the RX ring.

At promotion, the client transport gains the client listener and becomes
the client poll thread's. The replication transport gains the replication
listener and moves to the `repl-poll` thread with a new driver. Each
transport keeps its role in both directions, so promotion swaps no
queues.

How a surviving replica finds the new primary is unchanged and outside
this work. In `port` mode, the address it is given must be the new
primary's replication address.

## Order of work

One commit each, with its tests. The default (`shared`) behaviour is
unchanged after every step.

1. **Config and flags.** `ReplicationNic`, the three flag sets and their
   mapping and validation, with unit tests for each mode and each
   refused combination. Fix the stale `dpdk_num_queues` / "at most 2"
   comment in `dpdk_transport.rs`.
2. **`--cores` `repl-poll`.** The layout entry, its default, its validation
   (busy-spins; refused with `y`, as the reader is on DPDK), and its tests.
3. **One EAL, several NIC groups** in `melin-dpdk`. A refactor with no
   behaviour change: the existing DPDK suites under the netns runner are
   its test, plus a unit test on group/EAL ownership where it is
   reachable without libdpdk.
4. **Steering for `queue` mode.** The pure rule-plan function with its
   unit tests, the queue plan in `Port::configure` (RETA to queue 0, no
   RSS), and the rules' installation. Hardware-only past the pure
   function: say so in the commit, and check it first on the rig.
5. **The replication poll thread on the primary.** Spawned in `queue` and
   `port` modes, with `rte_thread_register`, the trace stage, and the
   per-queue counters on `/metrics`, and the boot warning the DPDK reader
   gets when unpinned (it busy-polls wherever the scheduler puts it)
   extended to an unpinned `repl-poll`, sharing the reader's wording
   rather than copying it. Netns runner: a second veth per node
   slot (`veth-setup.py`, the test-node layout), and the existing
   replication cases run in `port` mode as well as `shared`.
6. **Replica and promotion.** Both transports at boot, the receiver on the
   replication one, and promotion handing each transport to its thread.
   Netns runner: the failover cases in `port` mode.
7. **Bench suite.** `DPDK_REPLICATION=shared,queue[,port]` runs each value
   as its own pass of the DPDK transport, from one build. Results are
   named `dpdk-shared`, `dpdk-queue`, and so on. `port` mode reads
   `DPDK_REPL_PORTS`, `DPDK_REPL_IP` and `DPDK_REPL_MAC` from
   `/etc/melin-dpdk.conf`, and is skipped on a host without them.
8. **Docs.** The modes, flags and the `repl-poll` core in the operator
   docs (`deployment.md`, `replication.md`); `dpdk-replication.md`; the
   roadmap item (removed or rewritten once measured); `CHANGELOG.md`
   (Added); `CLAUDE.md` if the crate descriptions change. Step 2 described
   `repl-poll` as reserved for a thread "no configuration runs yet", in its
   `CHANGELOG.md` entry under Changed and in `pipeline-architecture.md`
   (the `--cores` row and the mixed-layout example): reword those once
   step 5 runs the thread, so the release notes do not contradict
   themselves.

## The measurement

One Latitude.sh rig with mlx5: four hosts, one build. The suite runs
`TRANSPORTS=dpdk DPDK_REPLICATION=shared,queue` with throughput and
single-client workloads. It compares p50, p99, p99.9 and throughput,
and the health verdict (a degraded run means a replica was evicted or
caught up). Then a second pass with a `latency-trace` build, for the
client loop's iteration time in both modes. The first thing to check on
the rig is the per-queue counters: replication on queue 1 only, client
traffic on queue 0 only. `port` mode is measured when an AWS rig with a
second ENI is up.

Expectation, to be confirmed or refuted: no change at the single-client
median, and lower p99/p99.9 and no evictions under throughput. Client
responses no longer queue in the NIC's TX ring behind a replication
burst, and the client loop no longer pays for replication ticks. The
cost is one more spinning core per node.

## Risks and open questions

- **mlx5 precedence** between `rte_flow` rules and the default RSS
  in non-isolated mode: the plan pins the RETA to queue 0, so precedence
  does not matter. Verify it on the rig with the counters.
- **Bifurcated mode captures one source IP** (`--dpdk-peer-ip`), so a
  bifurcated primary sees one peer host. That is an existing gap, not
  this work's, but a split primary in bifurcated mode needs every
  peer's address captured.
- **ENA Express is a per-ENI setting**: enable it on the replication ENI
  too. The instance's bandwidth and packet-rate allowances are shared
  across ENIs, so the split separates queues and cores, not that budget.
- **Driver and transport `Send`.** The driver ran on its own thread
  before April. The transport is built on the thread that polls it, so it
  never crosses a thread boundary.
