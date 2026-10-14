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
  counts go on `/metrics`, counted by each queue's poll thread at the
  burst (`melin_dpdk::QueueCounters`) rather than read from
  `rte_eth_stats`, as first planned: `rte_eth_stats_get` is not
  guaranteed thread-safe on every PMD against the queue's poll thread or a
  second reader of the port (queue mode puts two poll threads on each
  port), covers the first `RTE_ETHDEV_QUEUE_STAT_CNTRS` queues only, and
  some PMDs leave the per-queue fields unset. They count what the poll
  thread took off and put on the queue, not the NIC's drops before it.
  Misrouted steering is not silent: a replication segment that lands on
  the client stack gets a RST, and the link drops. The counters show which
  queue saw what.

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

The replication rules (and, on a replica, an ARP rule: see below):
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
NIC: **a replica must not dial from the client port** (the dial loop
starts at 40000 and wraps there, so a client port in that range could
collide; step 6 made it skip every port the plan steers by destination).

On a replica whose port is not isolated, one more rule, held until
promotion: **every ARP frame (EtherType 0x0806) → queue 1**, whose
transport has the node's address and MAC and answers for it while queue 0
goes unpolled (see "ARP on a queue-mode replica" below). The node keeps
its handle, one per port, and removes it at promotion. Isolated, the
kernel keeps ARP and owns the address: no rule. A primary at boot polls
queue 0 from the start: no rule.

Priorities are numbered densely from 0 and only as many are used as the
rules need: two in queue mode, three on an isolated replica. The ARP rule
takes none of its own: it shares level 0 with the replica's client-port
rule, which every replica has, and since an IPv4 rule's pattern
(ETH / IPV4 ...) never matches an ARP frame and the ARP rule's (ETH type
0x0806 / END) never matches an IPv4 one, the two at one level cannot
overlap, which `rte_flow` leaves undefined only for overlapping rules.
mlx5's root table offers few user levels (as few as two, depending on
device and flow engine): check the isolated replica's third level on the
rig. Queue mode with no replication port at all (a standalone node), or
with the node's replication port equal to its client port, is refused
before EAL init.

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
  queue, with `--replica-of` naming that address, and the primary's MAC
  seeded from `--dpdk-replication-peer-mac` in port mode (the NIC it dials
  from), `--dpdk-peer-mac` in queue mode.
- **The client transport** stays unlistening and unpolled until promotion.
  Clients do not connect to replicas. What arrives meanwhile is dropped
  at the RX ring, but ARP, which in queue mode a rule sends to queue 1
  until promotion.

At promotion, a queue-mode replica's ARP rule is removed, and the client
transport gains the client listener and becomes the client poll
thread's. The replication transport is dropped on the receiver's thread,
and the `repl-poll` thread builds a transport of its own on the same
queue, with the replication listener and a new driver, exactly as at a
primary's boot (step 6 corrected "moves to the `repl-poll` thread": a
transport never crosses threads). Each queue keeps its role in both
directions, so promotion swaps no queues.

How a surviving replica finds the new primary is unchanged and outside
this work. In `port` mode, the address it is given must be the new
primary's replication address.

What step 6 found, and settled:

- **An unpolled client queue costs nothing a polled one does not hold.**
  Its RX ring keeps the descriptors it was set up with, polled or not; once
  full, the NIC drops what else arrives for it (on mlx5 that is believed
  to be a per-queue drop, `rx_out_of_buffer`, not a pause of the port:
  check on the rig that queue 1 keeps flowing on a replica whose queue 0
  is flooded). No mbuf leaves the pool for it beyond the ring's. At
  promotion the client poll thread's first polls take whatever stale
  frames the ring held: old SYNs, answered and then reset by their
  long-gone clients (not ARP, which a queue-mode replica's NIC sends to
  queue 1 until promotion; see below). They are taken
  at the stack's time as of the promotion, not the transport's build: a
  transport's first poll reads the clock (it used to wait for the
  periodic refresh, so a neighbour learned from those frames was seeded
  already expired, and its re-seeding held off by the device's cooldown).
- **Nothing the replica needs arrives only on the unpolled queue**, on the
  same subnet. Its dial needs no ARP reply (the primary's MAC is seeded,
  as it always was: ARP is not relied on, for SR-IOV's sake), and a DPDK
  primary learns the replica's MAC from its frames (the device's MAC
  learning, not smoltcp, which only refreshes entries it already has),
  re-learned for as long as the link carries heartbeats. A **kernel-TCP
  primary** does not: Linux makes no neighbour entry from an incoming
  SYN, and with the default `arp_accept=0` ignores the replica's
  gratuitous ARP, so it ARPs for the replica. In port mode and shared
  mode the replica's polled transport answers. In queue mode, where queue
  0 is the port's default queue, step 6 left the request on the unpolled
  queue 0, so the link never came up, and a **router between the replica
  and its primary** could not resolve it either: closed since by an ARP
  rule (see "ARP on a queue-mode replica" below). In both split modes a
  **client connecting to a replica sees no answer** rather than the
  refusal a shared-mode DPDK replica sends (its stack is what refuses, and
  it is not polled), as a kernel-TCP replica leaves it waiting; the
  operator docs (step 8) must state it. Not fixed: a replica could poll
  its client queue from the receiver's thread (on every step of the
  receiver's loops, the replica's ack path), which is larger than the gap.
- **A dropped transport gave back too little.** A zero-copy socket keeps
  the mbufs of received data not yet read until it is read, reset or
  aborted, and dropping it did none of these; nor did the device free the
  frames still queued for a TX burst. Harmless when the process exits
  next, a leak once a transport is rebuilt on its queue. `DpdkTransport`
  now aborts its sockets on drop, and `DpdkDevice` frees what it holds.
  The receiver already resets every link it opened before it returns, so
  the leak needed a receiver that forgot one; the test
  (`melin-test-node`'s `transport_rebuild`) drops a transport with data
  unread and finds the pool whole. That covers the socket abort only: the
  device's two freeing loops (frames left in its TX batch, received
  frames not yet handed to the stack) are untested, as both are empty
  whenever a poll loop has finished, which is when a transport is
  dropped, and af_packet offers no deterministic way to stage a frame
  there; they are guards for a drop mid-poll.
- **The replica must not dial from a port the NIC steers by destination**
  (the step 4 correction). The dial loop's local ports (40000 up, wrapping
  back to 40000) now skip every TCP destination port of the steering
  plan of the NIC replication runs on (`SteeringPlan::for_replication`,
  `destination_ports`): in queue mode the client port, and the node's own
  replication port, which steers to the right queue but is not the dial's
  to take. Shared and port modes steer by no port, and dial as before.
- **A promoted queue-mode replica keeps its replica's TCP rules.** They
  are installed once, at boot, so the client-port rule and the rule on the
  old primary's replication port stay (the ARP rule, added after step 6,
  is removed at promotion: see below). Both are harmless to a primary: the
  client-port rule sends to the queue the default RSS would, and the
  source-port rule outranks no client (the client-port rule is above it)
  and matches nothing else a primary is sent but replication, which it
  sends to the replication queue. The driver and the `repl-poll` placement
  are the boot-time primary's: the same function builds them, from the
  same configuration.
- **Mixed clusters work both ways.** A split-mode replica dials the
  primary's replication address whatever mode the primary runs: a
  shared-mode primary's is on its client NIC, and is reached from the
  replica's replication queue or NIC as any address is. In port mode the
  replica's replication NIC dials it with
  `--dpdk-replication-peer-mac` naming that primary's client NIC.

### ARP on a queue-mode replica

Closed after step 6, as its own change. On a replica in queue mode whose
port is not isolated, the plan adds a rule sending every ARP frame
(EtherType 0x0806) to queue 1 (`SteeringPlan::arp_until_promotion`); the
node keeps the rule's handle, one per port, and removes it at promotion
(`DpdkShared::end_replica_steering`), before the client queue is polled
as a primary's. What decided it:

- **ARP requests before promotion.** Queue 1's transport has the node's
  address and MAC (one interface per queue, on the group's one MAC), and
  smoltcp answers a request whose target is its address. So a kernel-TCP
  primary (`arp_accept=0`) and a router resolve the replica, and the link
  comes up with no static neighbour entry. The replica's queue 0 no longer
  collects ARP while unpolled.
- **ARP replies before promotion.** Queue 0's transport sends nothing
  until promotion but its boot-time gratuitous ARP, an announcement that
  expects no reply, so no reply is owed to it. Queue 1's transport ARPs
  only when a neighbour it needs is missing: the primary's MAC is seeded
  at every dial and re-seeded from the primary's frames (the device's MAC
  learning, at most every 30 s while frames arrive), and the gateway's is
  seeded and re-seeded every 20 s, so it ARPs when the primary's entry
  expires (60 s with nothing heard from it, which the liveness deadline
  ends sooner) or is evicted (the stack's neighbour cache holds 64
  entries, and every request for the node's address fills one). Before the
  rule its reply landed on the unpolled queue 0 and was lost; it now comes
  back to queue 1, the transport that asked.
- **Replies to requests a transport did not send.** smoltcp takes any ARP
  frame whose target address is its own: a request is answered, and a
  request or a reply fills the neighbour cache with the sender, nothing
  more. A reply queue 1 did not ask for (none is expected before
  promotion) costs a cache entry, the oldest to expire evicted when full,
  as on every polled DPDK transport today.
- **After promotion the rule must go.** Queue 0's transport then ARPs
  for every on-link client the stack has forgotten (an entry expired, or
  evicted once a primary has more on-link peers than its 64-entry cache),
  and must read the replies. Left in place, the rule would send them to
  queue 1, whose transport would fill its own cache from them and answer
  nothing, and the client would stall for the stack's discovery timeout
  each time. Removed, ARP goes to queue 0 through the pinned RSS table,
  exactly as on a primary booted in queue mode, which has no ARP rule:
  for ARP, a promoted node's steering is the boot-time primary's. Its TCP
  rules still differ, harmlessly (see above). The removal runs once per
  process, on the one path both promotions take (the operator's `PROMOTE`
  and raft's both end the receiver, which returns to `run_dpdk_impl`). A
  port that refuses it leaves the rule in place and the node serving,
  with a warning: it still answers ARP (on queue 1) and still learns
  every peer that sends it a packet, and what it loses is a stall for a
  client it must resolve again, not its clients.
- **Why not match only what is safe.** Steering only requests would make
  the removal unnecessary, but `rte_flow` matches the ARP opcode only
  through `ARP_ETH_IPV4`, which mlx5 does not support, and matching the
  broadcast destination instead misses the unicast requests Linux sends to
  confirm a stale neighbour entry (`ucast_probes`), so a kernel-TCP
  primary's entry for the replica would fail once it went stale.
- **The level.** Level 0, shared with the replica's client-port rule,
  which every replica has: no new level, so the plans stay at two levels
  (three on an isolated replica, which gets no ARP rule). The ARP rule's
  pattern (ETH type 0x0806 / END) and every IPv4 rule's (ETH / IPV4 ...,
  which carries EtherType 0x0800) match disjoint frames, and `rte_flow`
  leaves only overlapping rules at one level undefined. mlx5 folds a
  level into sub-priorities by the deepest layer matched, an L2-only rule
  ranking lowest within it, which disjoint patterns make irrelevant.
- **Isolated (bifurcated) mode:** no rule. The kernel owns the address
  and answers ARP, as it always has; the plan's rules capture IPv4 only.
  **Shared and port modes:** unchanged, no rule (shared polls queue 0 from
  boot; port mode's replication NIC answers for its own address). **A
  queue-mode primary at boot:** no rule, as queue 0 is polled from the
  start.

What runs where: which modes and roles get the rule, its level and its
disjointness from the TCP rules, and that a promoted replica's ARP
steering is the boot-time primary's are unit-tested on every host
(`melin_dpdk::steering`). Under the netns runner the promotion's removal
runs on every DPDK promotion (port and shared modes, the operator's and
raft's), finding nothing to remove, and `transport_rebuild` calls it on
both NIC groups of a port-mode node, twice. Hardware-only: the rule's
installation, its handle and its removal (af_packet has no `rte_flow`,
and queue mode does not come up there), checked on the rig.

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
   replication one, and promotion handing each queue to its thread (the
   client transport to the client poll thread; the replication one
   dropped, and rebuilt by `repl-poll`). The dial-port exclusion, a
   transport's drop giving back its mbufs, and the replica refusal of
   step 1 removed. Netns runner: the failover cases in `port` mode (raft
   auto-promotion and the operator's `PROMOTE`, the primary in `port` and
   in `shared` mode, the other replica rejoining the promoted node), a
   `queue`-mode replica refused on one queue pair, and a transport rebuilt
   on its queue. Hardware-only: a `queue`-mode replica end to end (the
   rules, queue 1's receiver, the unpolled queue 0). Followed by the
   replica's ARP rule, removed at promotion (see "ARP on a queue-mode
   replica"), its own change.
7. **Bench suite, shared, queue and port modes.** `DPDK_REPLICATION=shared,queue,port`
   (or `all`; `shared` by default, so an invocation without it runs as
   before) runs each value as its own pass of the DPDK transport, from one
   build, validated up front with the other settings. Runs are named
   `dpdk-<mode>-<workload>` (kernel ones `kernel-<workload>`, as before).
   In queue mode the primary (keeping `--replication-bind`, the port the
   NIC steers) and the replicas (keeping `--replica-of`) add
   `--dpdk-replication-queue`, and a node host whose
   `/etc/melin-dpdk.conf` is SR-IOV's, or missing (the suite's unattended
   setup can only configure SR-IOV; mlx5 needs `DPDK_IP`), is refused
   before the build, by name. The readiness wait
   needed no change: both of a DPDK primary's replication lines (shared
   mode's, from the client poll thread, and the split modes', logged once
   `repl-poll` listens) begin with "DPDK replication sender started",
   which the suite's test checks in `server.rs`. On DPDK the primary's
   per-queue counters are sampled beside the health samples (`.queues`),
   and in queue mode judged (`judge_steering` in `lib.sh`): between the
   first and last complete samples, queue 1 and queue 0 must each have
   moved at least 100 packets per sampled second in each direction,
   summed over ports, or the run is degraded. The counters cannot
   attribute a packet to a flow, so a flow misrouted beside correctly
   steered traffic is left to its consequences (a reset link: the health
   verdict, or the load generator failing). `port` mode followed as its
   own commit. `dpdk-setup.sh` (mlx5 path only; SR-IOV and L3 refuse it)
   takes a second port, `DPDK_REPL_NIC` and `DPDK_REPL_IP`, refused on
   the default-route port, on the client port, and outside the client
   port's subnet, and writes `DPDK_REPL_{NIC,PCI,IP,MAC,PORT}` with
   `DPDK_EAL_ARGS` allowing both devices. The port ids follow EAL's
   probe order, PCI address order for PCI devices, not the `-a` order,
   so `DPDK_PORT` is the client port's id among the two and may be 1;
   without a second port the conf is byte for byte what it was
   (`test-dpdk-setup.sh`, which extracts the script's pure functions, as
   `test-server-setup.sh` does). The suite refuses, by name and before
   the build, a node host whose conf lacks the `DPDK_REPL_*` keys or
   whose EAL arguments do not allow the second device, rather than skip
   it: `all` is every mode, so a rig without a second port lists its
   modes. In port mode the primary binds replication on its replication
   NIC's address and the replicas dial it there (`--replica-of`), each
   node adding `--dpdk-replication-ports` and `--dpdk-replication-ip`,
   and each replica `--dpdk-replication-peer-mac` with the primary's
   replication MAC; the client NIC's flags are unchanged. The readiness
   waits needed no change (the split modes' replication line, then the
   client listener's and the health endpoint's, are the same in port
   mode). `judge_steering` tells the sides apart by port in port mode
   (the replication NIC's ports against the client NIC's, every queue of
   each) with queue mode's threshold, and degrades a primary with no
   replication port. The counters judge volume only, so ports recorded
   the wrong way round pass the verdict: that shows as a cluster that
   does not start, each port then holding the other's address, so that
   no replica connects. `TRACE=1` followed as its own change: the servers
   are built with `latency-trace` (the echo and notary examples gained the
   feature, a forward to the runtime's; the Exchange Core's server had
   it), each in a cargo invocation of its own and into
   `target/lan-bench-trace`, so that a traced and an untraced invocation
   never share a binary and melin-bench, which links the examples, is
   built exactly as untraced (the traced server, built alone, lacks in
   turn what melin-bench's features unify into their shared
   dependencies, such as melin-client's `dpdk` for notary on DPDK:
   features which only add code); a server without the feature is refused
   before any host is touched (`refuse_untraced_server`, on `cargo
   metadata`). The load generators stay untraced: melin-bench has no such
   feature, and the Exchange Core's bench would time its own DPDK loop,
   perturbing the client's figures. After each run, before the nodes
   stop, the primary's `/stats-dump` (its health bind, tab-separated
   stage lines) is saved as `<run>.stats` and judged (`judge_trace`: on
   DPDK the client poll loop's stage, and in queue and port modes the
   replication poll loop's, present with samples); the replicas' are not
   taken, as they run no health endpoint in the suite and the loops
   compared are the primary's. The summary ends with both loops'
   work-iteration percentiles per run (`trace_summary`); the results
   directory takes a `-trace` suffix and the summary's header says
   `TRACED`. Untested until the rig: a traced DPDK pass; the podman
   rehearsal (`TRACE=1 test-containers.sh`) covers the kernel path.
   The suite does not provision AWS ENIs; port mode there is for hosts
   with a second mlx5 port. Untested until the rig: the DPDK passes
   themselves (the podman rehearsal runs kernel TCP only) and
   `dpdk-setup.sh`'s hardware path, including the port ids it derives.
8. **Docs.** The modes, flags and the `repl-poll` core in the operator
   docs (`deployment.md`, `replication.md`); `dpdk-replication.md`; the
   roadmap item (removed or rewritten once measured); `CHANGELOG.md`
   (Added); `CLAUDE.md` if the crate descriptions change. Step 2 described
   `repl-poll` as reserved for a thread "no configuration runs yet", in its
   `CHANGELOG.md` entry under Changed and in `pipeline-architecture.md`
   (the `--cores` row and the mixed-layout example); step 5 reworded those
   when the thread started running, and added the `CHANGELOG.md` entry for
   a primary, so step 8 extends them for the replica rather than writes
   them. The operator docs must also say that `queue` and `port` modes
   lock out DPDK secondary-process tools (`dpdk-proc-info`,
   `dpdk-dumpcap`, `dpdk-pdump`): `repl-poll` registers with EAL as an
   lcore, and DPDK turns multi-process off for a primary that registers
   one. The node's per-queue counters on `/metrics` take the place of
   `dpdk-proc-info` for checking steering. And they must say what a
   replica in `queue` or `port` mode does differently (step 6): a client
   connecting to it is left unanswered rather than refused (the sentence
   in `replication.md` step 6 qualified). In `queue` mode it answers ARP
   from its replication queue until promoted, so a kernel-TCP primary or
   a router resolves it as any host; nothing to say there beyond the
   mode's mlx5 requirement.

## The measurement

One Latitude.sh rig with mlx5: four hosts, one build. The suite runs
`TRANSPORTS=dpdk DPDK_REPLICATION=shared,queue` with throughput and
single-client workloads. It compares p50, p99, p99.9 and throughput,
and the health verdict (a degraded run means a replica was evicted or
caught up). Then a second pass with a `latency-trace` build, for the
client loop's iteration time in both modes: the same invocation with
`TRACE=1`, whose summary lists the client poll loop's work-iteration
percentiles per run and, in queue mode, the replication poll loop's
beside them (its figures are perturbed by the tracing, so they are read
for the breakdown only, against each other, never against the first
pass's). The first thing to check on
the rig is the per-queue counters: replication on queue 1 only, client
traffic on queue 0 only. `port` mode is measured on a rig whose hosts
have a second mlx5 port (`DPDK_REPLICATION=port`), or when an AWS rig
with a second ENI is up.

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
- **A `queue`-mode replica's ARP rule** (see "ARP on a queue-mode
  replica"), hardware-only. On the rig: that mlx5 accepts the
  EtherType-only rule at level 0 beside the client-port rule (a refusal
  fails the replica's boot naming the rule); that a kernel-TCP primary
  with an empty ARP cache (`ip neigh flush`) resolves a queue-mode
  replica and its link comes up, the replica's queue 1 counting the ARP
  frames; the same with the replica run with `--dpdk-vlan`, where ARP
  arrives 802.1Q-tagged and the rule (ETH type 0x0806, no VLAN item)
  matches only if mlx5 compares the EtherType after the tag, as
  `rte_flow` documents the field as "EtherType or TPID" and leaves it
  open (if queue 1 counts nothing there, the shim needs an
  ETH / VLAN(inner type 0x0806) variant of the rule for VLAN
  deployments); and that at promotion the node's log names the rule
  removed, and an `arping` of the promoted node is answered with
  queue 0, not queue 1, counting it. That log line is the only check on
  the promotion's removal call: the netns promotions run it with nothing
  to remove, so no test fails if it is dropped.
- **mlx5 with an unpolled queue**: confirm on the rig that a full queue 0
  drops per queue (`rx_out_of_buffer`) and never pauses the port, so a
  replica's queue 1 keeps flowing under a client flood to its address.
