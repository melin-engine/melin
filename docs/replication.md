# Replication

Synchronous journal replication from a primary server to one or two
replicas. The primary streams journaled events to each replica over a
dedicated connection; the replica persists them locally, acknowledges,
and replays them through its own matching engine so its state stays
warm for failover.

Every client response is gated on a configurable **ack policy** that
says which *copies* of an event must exist before the primary tells the
client it's done. Policies range from a single fsynced copy (dev/test)
through two in-memory copies (lowest latency, bounded RPO) to two
fsynced copies (compliance).

## Ack policies

The operator picks one of four named policies via
`--ack-policy <policy>` on the primary. Each names the copies that must
exist before the client gets a reply:

| Policy | Copies required at client ack | Vulnerable to | When to use |
|---|---|---|---|
| `disk` | One fsynced copy on PLP-backed NVMe. | Hardware failure of the disk holding that copy. | Dev, staging, single-node deployments. |
| `ram` | Two copies in memory, on two nodes. Disk writes trail asynchronously — every journal still syncs every batch, just off the ack path. | Simultaneous failure of every node holding the event within the fsync window (typically milliseconds) — the un-synced tail is lost. Any single node failure is fully covered by failover. | Storage where fsync is slow (cloud block volumes) or latency-critical applications that accept a small, bounded RPO. Lowest ack latency of the four policies. |
| `disk+ram` *(default)* | One fsynced copy on PLP-backed NVMe **plus** a second copy in another node's memory. | Failure of the disk holding the fsynced copy within ~80 µs of the ack — the window before the other node completes its own fsync. PLP-protected power loss is fully handled. | Typical live-trading deployments. Saves ~50–80 µs per fill vs `two-disks`. |
| `two-disks` | Two fsynced copies on PLP-backed NVMe, on two nodes. | Simultaneous disk failure on two nodes. | Compliance-driven venues that require two durable copies before client ack. |

The PLP (Power-Loss-Protection) capacitor on the NVMe device is what
makes a fsynced copy a meaningful guarantee without an explicit fsync
round-trip on every event — the device commits the write to flash
across a power loss.

### Policies count copies, not nodes

No policy names a node. `disk` and `disk+ram` require *one* fsynced
copy, and it is satisfied by whichever node's disk confirms first. That
is usually the primary, but not always: when the primary's device hits a
p99.9 hiccup, the replica's fsync of the same event wins and the ack
goes out without waiting for the primary's disk. Likewise the in-memory
copy of `ram` and `disk+ram` is "a second node has it", not "a specific
replica has it". The names say what must hold the event at ack time and
nothing about where.

### A replica's disk is not on the `disk+ram` critical path

`disk+ram` asks the second node to confirm *receipt in RAM*, not a
second fsync, so a replica whose own device stalls — a p99.9 NVMe
hiccup, a network-attached volume, a rotation pause — must not show up
in client latency. It doesn't: the replica keeps receiving and keeps
confirming receipt while its disk catches up. Only its *persisted*
confirmation falls behind, and that is the one `two-disks` waits for.

Under `two-disks` a second disk is on the critical path by definition —
that is the guarantee the policy buys. With one replica that is its
disk; with two, the faster replica's. A node running behind its own
device is visible on that node's `melin_journal_disk_lag_batches`.

### No disk on the `ram` critical path

`ram` takes the `disk+ram` idea one step further: *no* disk gates the
ack, not even the primary's. A response goes out once a second node
has confirmed receipt in RAM. Every node's journal keeps writing and
syncing every batch exactly as under the other policies — durability is
not disabled, it is moved off the ack path — so the on-disk copies
trail the acked frontier by roughly one disk-sync interval.

The resulting contract:

- **Any single node failure loses nothing — provided you fail over.**
  Every acked event is held by at least two nodes, so a surviving
  replica always has it. See the warning below: under this policy
  recovering a crashed primary by restarting it *in place* can lose an
  unbounded number of acked events, not just the batches in flight.
- **Losing every node that held an event loses it.** The gate requires
  two holders, not all three, so an event can be acked while only the
  primary and one replica have it — losing that pair loses the event
  even with a third node still running. In the common case that means
  a simultaneous outage across the cluster, but it does not take a
  full cluster outage.
- **What is lost is the un-synced tail** — the acked events no journal
  had synced yet, typically the final few milliseconds. That window is
  the policy's recovery point objective (RPO), and it is the price of
  taking fsync out of the ack path.

Pick `ram` when that trade is right: deployments whose storage makes
fsync expensive (cloud block volumes, network-attached disks) — where
the other policies would put milliseconds of disk latency inside every
ack — or applications that value the lowest possible ack latency over a
guarantee against simultaneous multi-node loss.

#### Failover is mandatory — never restart a crashed primary in place

Because policies count copies rather than nodes, a connected replica
can be the node that satisfies the ack. Under `disk` and `disk+ram`
that happens whenever the replica's fsync lands before the primary's —
normal during a primary disk hiccup — and under `two-disks` the
primary's own fsync is never waited for when two replicas are faster.
So under *every* policy, a primary that dies while a replica is
connected can come back with a journal **short of events its clients
were told were durable**. What differs is how short:

- Under `disk`, `disk+ram` and `two-disks` the gap is bounded by the
  batches that were in flight to the primary's disk — the journal syncs
  every batch, so it cannot run further behind than what the device was
  writing at the moment of the crash. A process crash loses nothing
  (the kernel still writes the pages out); a power loss or kernel panic
  loses that in-flight tail.
- Under `ram` the primary acks events it has not yet written and keeps
  acking ahead of its disk indefinitely, so the gap is unbounded and a
  plain process crash is enough to open it.

Restarting that primary in place discards the gap. The surviving
replica does hold those events, and on reconnect it advertises a
sequence beyond the restarted primary's tip — which the primary
correctly reads as divergent history and resolves by rebasing the
replica onto its own shorter journal (the replica archives its lineage
first, so the events are recoverable from the archive, but they leave
the live cluster and any client that read them is now ahead of the
system of record).

So after a primary crash, under any policy:

- **Promote a surviving replica.** Raft-driven failover already does
  this automatically and elects the most caught-up node.
- **Do not restart the old primary as primary.** Bring it back as a
  replica of the newly promoted node; it will catch up from the
  authoritative journal.

Only a node that crashed with no replica connected — `--standalone`,
or a cluster whose replicas were all down, which under every policy
but `disk` means the gate was closed and nothing was being acked —
can safely be restarted in place.

#### Version requirement for automatic failover

The ack policy is advertised to replicas on the replication stream,
and a node refuses to auto-promote on a policy it does not recognise —
the correct fail-closed behaviour, but it means a replica running a
build that predates `ram` will decline to take over from a primary
using it. Upgrade every replica before switching a primary to `ram`,
or automatic failover is silently unavailable until you do. Manual
`PROMOTE` is unaffected.

### Strict fail-closed semantics

Every policy is **strict**. If the required copies can't exist in the
current cluster shape (e.g. `disk+ram` configured but no replica is
connected), the response gate stalls and clients see no reply rather
than the system silently weakening the contract. The
`melin_ack_policy_degraded` gauge on `/healthz` flips to `1` and a
warn-level log line is emitted on transition and every 5 seconds while
degraded.

This is deliberate: silently down-grading the ack contract under load
is exactly the kind of failure mode regulators and exchange operators
write off in post-mortems. Operators who want the system to keep
trading under a weaker policy during a partial outage use the runtime
policy swap below.

### Trading halts when all replicas disconnect

Independent of the ack gate, the matching engine halts when **every**
configured replica disconnects. New client orders are rejected with a
`ReplicaDisconnected` reason code immediately — clients see the halt
reason rather than a TCP read timeout. The rejection bypasses the ack
gate because no engine state changed: replicas will deterministically
produce the same rejection when they replay the same input on
reconnect.

Standalone deployments (no replication configured) skip this halt
entirely and run under `disk`.

### Runtime policy swap

The operator can change the active ack policy without restarting the
node via a signed admin command:

```
ACK-POLICY disk
ACK-POLICY ram
ACK-POLICY disk+ram
ACK-POLICY two-disks
```

Sent over the same admin connection as `PROMOTE` / `ROTATE`, authenticated
with an operator key (Ed25519 challenge-response). Every swap is
INFO-logged with the `prev → next` transition for the audit trail.

Until the next minor release the pre-0.15 spelling is accepted as a
deprecated alias — `DURABILITY local|replicated|hybrid|durably-replicated`
maps onto `disk|ram|disk+ram|two-disks` and logs a warning. Update
runbooks before it is removed.

The intended workflow is failover:

1. Primary dies, replica is promoted (`PROMOTE`).
2. The promoted node is now standalone — under `disk+ram` its gate is
   structurally unsatisfiable (no second node to hold the in-memory
   copy) and trading would stall.
3. Operator sends `ACK-POLICY disk` → the gate re-evaluates under
   `disk` and trading resumes in seconds, no restart, no dropped
   client connections.
4. New replicas are spun up and connect.
5. Operator sends `ACK-POLICY disk+ram` → the gate is satisfied by the
   new cluster shape and trading continues at the full contract.

The replica's admin listener also accepts `ACK-POLICY` — operators can
**pre-stage** the post-promotion policy by sending `ACK-POLICY disk`
*before* `PROMOTE`; the value persists across the in-process
transition.

## Replica configuration

A node started with `--replica-of <primary_addr>` runs as a replica:

- Authenticates with the primary via Ed25519 challenge-response
  (`--replication-key`). The corresponding public key must be in the
  primary's `authorized_keys` file with the `replication` permission.
- Receives a stream of input events with pre-assigned sequences and
  timestamps from the primary. The replica's pipeline produces a
  journal that is a **bitwise mirror** of the primary's — same
  sequences, same events, same segment boundaries (see "Journal
  mirroring and divergence detection") — and runs the same matching
  engine over it so its state stays warm for promotion.
- Acknowledges each batch on a **dual track**: an `in_memory_sequence`
  that advances as soon as the batch is received, and an
  `acked_sequence` that advances once the local journal write is
  durable. Both fields are populated on every ack so the primary's
  gate can evaluate any policy without separate ack streams.
- Does not accept client connections.

If the primary disconnects or evicts the replica, the receiver
reconnects with exponential backoff (1 s → 30 s cap), recovers its own
state on its own journal, and resumes from its last durable sequence.
Periodic snapshots are taken on a dedicated thread so a crash doesn't
require replaying from genesis.

### Fault isolation between replica slots

Each replica slot has an independent ring buffer (configurable via
`--replication-ring-size`, default 256 slots × 512 KiB = 128 MiB per
ring, 256 MiB total for a dual-replica deployment). If a slot's ring
fills up — i.e. the replica isn't draining fast enough for the
primary's next journal batch to fit — the primary evicts that replica
immediately, on the same batch, and frees the ring. There is no grace
period: a skipped batch would create a sequence gap in the replica's
journal that can only be repaired by reconnection + catch-up, so the
primary refuses to publish past the gap. The surviving replica and
client trading are unaffected.

## Manual promotion

The admin endpoint accepts `PROMOTE` on a replica to switch it to
primary mode in-process: the warm matching state is reused directly,
no journal re-replay, no snapshot reload. Sub-second switchover.

After promotion the new primary will halt new orders if it has no
replicas connected (see above) — the operator's playbook is to either
spin up new replicas immediately or send `ACK-POLICY disk` to resume
trading under the single-copy policy.

The old primary should still be stopped promptly, but epoch fencing
(below) now closes the split-brain window if it isn't: the moment the
stale primary hears from any node that observed the promotion, it
stops accepting and acknowledging orders and shuts itself down.

## Fencing epochs

Every promotion advances a cluster-wide **fencing epoch**, recorded in
the journal as the first entry of the new primary's tenure and
replicated to every node like any other event. The epoch survives
restarts and snapshots, and establishes which primary tenure any given
order belongs to.

The epoch is exchanged on every replication connection, in both
directions, and enforces two rules:

- **A superseded primary self-demotes.** If a connecting replica
  advertises a higher epoch than the primary's own, a promotion
  happened that this primary missed — it is stale. It immediately
  stops accepting orders, stops acknowledging in-flight ones (those
  clients see a connection reset and should reconcile on reconnect),
  reports `halted` on the health endpoint, logs an error, and shuts
  down. Restart it with `--replica-of` pointing at the new primary to
  rejoin the cluster.
- **A replica refuses a stale primary.** If a primary advertises a
  lower epoch than the replica has already observed, the replica
  refuses to follow it (its lineage would overwrite newer state),
  logs a warning, and retries with backoff — check the `--replica-of`
  target if this fires persistently.

No operator action is needed to *enable* fencing; it is always on
when replication is configured. Without the control plane (below), one
gap remains: two promotions issued independently during the same outage
land on the same epoch and fencing cannot distinguish them — promote
exactly one replica per failover. With the control plane enabled,
election-driven promotions stamp their (unique, strictly increasing)
election term as the new epoch, closing that gap.

## Control plane: coordinated election and automatic failover

Nodes can optionally run a **control-plane consensus service** (Raft)
that carries leader election, cluster membership, and fencing epochs —
and nothing else. Order flow stays on the replication path above, and
the ack policies are unchanged. The control plane is fully isolated
from trading: losing control-plane quorum never halts or slows the
data plane; failover simply degrades to the manual `PROMOTE` playbook
until quorum returns.

Enable it per node with:

| Flag | Purpose |
|---|---|
| `--raft-bind <addr>` | Control-plane listener (kernel TCP, also on DPDK nodes). Enables the control plane. |
| `--raft-node-id <id>` | This node's id (non-zero, unique per cluster). |
| `--raft-peer <id@host:port#base64-pubkey>` | One cluster member (repeatable). Give **every node the same list, including an entry for the node itself**. The pubkey pins the peer's identity: a connection authenticated with a different key cannot speak for that id. |
| `--raft-dir <path>` | Durable election state. Defaults to the journal path with a `.raft` extension. |
| `--raft-auto-promote` | Act on election wins (below). Off by default. |

Peer links authenticate with the same Ed25519 replication keys as the
data plane (`replication` permission in `authorized_keys`), so
`--replication-key` is required on every raft-enabled node, primaries
included. Election state is observable on every node's `--health-bind`
endpoint via the `melin_raft_*` gauges (node id, term, leader, role,
whether the driver is running) — raft-enabled replicas serve a minimal
`/metrics` for exactly this purpose.

With `--raft-bind` alone the election is **observational**: leadership
is elected and exported, but promotion stays operator-driven. Elections
steer toward the most-caught-up node — candidates advertise their
journal position and peers holding more data decline to vote for them —
so the elected leader is the right node to `PROMOTE`.

### Automatic failover (`--raft-auto-promote`)

With `--raft-auto-promote`, a replica that wins an election promotes
itself: the election term is journaled as the new fencing epoch, so any
two election-driven promotions always mint distinct epochs and the
newer fences the older. Expect failover within several seconds: the
election timeout (1–2 s), a short grace period confirming the primary is
really gone (see below), and the promotion itself (sub-second).

Auto-promotion is deliberately conservative. The elected replica
**refuses** to promote — logging the reason — when:

- its journal recovery hasn't finished (its advertised position isn't
  trustworthy yet);
- it has been fenced (a newer primary exists);
- its replication link to the primary is still up (a live primary must
  never be deposed by control-plane noise);
- its link to the primary dropped only moments ago — a replica only acts
  once the link has been *continuously* down for a few seconds. The link
  state is one-sided (it reflects only the winner's own socket), so a
  transient network blip reads the same as a real failure; requiring a
  sustained outage keeps a brief hiccup from deposing a healthy primary,
  at the cost of a few seconds of extra failover latency. The grace
  period cannot vouch for a primary whose *startup* outlasts it — see
  the bring-up rule under "Deployment rules";
- it has never observed a primary since it booted **and** its own
  journal is empty — a blank node winning an election is a cluster
  bring-up race, not a failover, and must not depose a primary that is
  merely slow to start;
- the primary was acking under the `disk` policy — acks never waited
  for a second copy on another node, so no election can prove the
  winner holds every acked order; failover stays a manual, eyes-on
  decision under `disk`;
- **a reachable peer holds more data than it does.** Election steering
  is best-effort, so a behind replica can end up holding leadership —
  and under `disk+ram`/`ram` an ack only requires the *fastest*
  replica, so at the moment of a crash the slower replica legitimately
  lacks the newest acked events. Every raft message carries the
  sender's journal position; the winner refuses to promote while any
  peer it can still hear advertises a position ahead of its own. The
  caught-up peer recognizes the standoff (it sees the leader's
  position on every heartbeat) and takes over leadership itself, so
  promotion lands on the node that holds every acked event. A peer
  that has genuinely stopped responding cannot hold this up: after a
  short grace it is treated as dead — nothing better is reachable —
  and promotion proceeds, with a loud log if that peer's last known
  position was ahead (its journal is then the reconciliation source
  when it returns);
- manual promotions have outrun election terms (the term must be
  strictly above the epoch in force; the alignment heals as terms
  advance — promote manually in the interim).

Manual `PROMOTE` remains available at all times, including during a
control-plane outage; whichever request is filed first wins, and a
later duplicate cannot retarget an in-flight promotion.

Under auto-promotion the raft peer mesh also becomes an additional
fencing channel: a serving primary whose peers advertise a higher
fencing epoch self-fences and shuts down immediately, without waiting
for a data-plane connection to cross.

### Deployment rules

- **Bring the cluster up primary-first when auto-promotion is armed.**
  The grace period tells a link blip from an outage; it cannot tell a
  dead primary from one whose startup (journal recovery, memory
  prefault) simply outlasts it. A blank replica refuses to promote (see
  the refusal list above), but a replica that already carries journal
  data will — one election timeout plus the grace after its link went
  down — depose a primary that is still recovering, fencing it when it
  finally comes up. On a full-cluster (re)start, start the primary and
  wait until it serves before starting the replicas. A crashed primary
  that restarts slower than the grace while its replicas stayed up *is*
  deposed — that is automatic failover doing its job, not a
  misconfiguration.
- **Three voters minimum for auto-promotion.** A two-node cluster
  cannot elect a leader after losing either node, so automation would
  never fire when needed; the server refuses to start with
  `--raft-auto-promote` and fewer than three configured voters.
  Two-node deployments keep today's manual `PROMOTE` playbook (the
  observational election still works).
- **`--raft-dir` is durable state.** Keep it on the same durability
  class as the journal. Never restart a node with a wiped raft dir
  while its peers are live — a forgotten vote can be granted twice in
  the same term, undermining the epoch guarantee. If the dir is lost,
  restart that node last, after a leader is established, or with the
  raft flags off.
- **Membership is static on this release.** The `--raft-peer` list is
  fixed at first boot (later flag changes are ignored in favor of the
  stored membership, with a warning). Surviving replicas do **not**
  automatically re-point `--replica-of` at a newly promoted primary —
  reconnecting them is still operator work after a failover. Give
  every candidate `--replication-bind` (see the flags section) so the
  winner's listener is already up when you re-point them.
- **Same peer list everywhere.** Identical `--raft-peer` lists
  (including each node's own entry) keep the first-boot membership
  consistent across the cluster.
- **Arm `--raft-auto-promote` uniformly — every node or none.** The
  flag arms two things on the node that carries it: acting on election
  wins, and the raft-mesh fencing channel described above. A cluster
  with the flag on only some nodes still fails over (any flagged
  replica can win and promote) but fences asymmetrically: an unflagged
  ex-primary ignores the mesh and keeps serving until a data-plane
  connection carries the new epoch to it, lengthening the
  two-primaries exposure window that mesh fencing exists to close.

## Journal mirroring and divergence detection

A healthy replica's journal is **byte-for-byte identical** to the
primary's — not just the same events, but the same segment files with
the same boundaries. Two mechanisms maintain and enforce this:

**Primary-driven rotation.** Replicas never rotate journal segments on
their own. The primary announces each rotation in the replication
stream at its exact sequence boundary, and replicas rotate at the same
entry. (`--max-journal-mib` and the admin `ROTATE` command therefore
act on primaries only; a replica's segmentation always follows its
primary's. `ROTATE` on an empty live segment is a no-op — the boundary
already exists.) Because segments match file-for-file, a replica's
journal can be verified against the primary's offline with a plain
byte hash — no Melin tooling required for backup or audit
verification.

**Cross-node chain validation.** Every announced rotation carries the
primary's tamper-evident chain hash at the boundary, and the replica
verifies its own chain matches before adopting it. The same check runs
when a replica connects (the primary recomputes its chain at the
replica's reported position and compares) and periodically during live
streaming. A mismatch anywhere means the replica's journal holds
**divergent history** — most commonly an ex-primary rejoining after a
failover with orders it journaled but never replicated.

Chain validation requires the tamper-evident hash chain on **both**
nodes (the `hash-chain` build feature, on by default). A pair where one
node was built without it still replicates, but with reduced
verification: boundary and connect-time checks fall back to
sequence-only validation, so a fork on such a pair goes undetected
until both nodes run hash-chain builds. Run matching builds across a
deployment.

**Divergence repair is automatic.** A divergent replica is re-seeded
from the primary through the snapshot path on the same connection.
The primary confirms a snapshot is actually available *before*
instructing the replica to archive anything: a primary that cannot
serve one (snapshots disabled, or the snapshot's boundary segment
pruned) refuses the resync with an explicit error and the replica's
journal is left untouched. A transfer that fails midway (network drop,
primary restart) is retried with backoff on a fresh connection; the
pre-resync journal stays archived for reconciliation either way.
Divergence detected *mid-stream* (a rotation announce or periodic
chain check failing against the local journal) repairs the same way
without a process restart: the replica tears its pipeline down,
re-derives its position from disk, reconnects, and takes the same
re-seed path — no supervisor required. Every divergence verdict —
mid-stream or at handshake — increments the primary's
`melin_replica_divergence_total` counter; alert on any growth, and
treat growth outside an expected failover rejoin as a possible
corruption event requiring immediate investigation. The in-process
repair runs **once** per process lifetime: a second mid-stream
divergence in the same process is systematic, and the replica exits
instead of looping (each repair cycle archives a full journal copy,
and recurrence at that rate means something upstream is seriously
wrong). Either way, the replica's
old journal and snapshot are **archived, never deleted** — moved to a
sibling directory named `<journal>.divergent.<n>`. Under the `disk`
policy that journal may hold acked orders that did not survive the
failover, which is exactly what an operator or regulator needs for
reconciliation. Routine (non-divergent) resyncs archive to
`<journal>.resync.<n>` for the same conservative reason — and note
that when a replica's position predates the primary's oldest retained
segment, divergence there *cannot be checked*: a `.resync.<n>` archive
from a node that was ever a primary may contain a fork and deserves
the same care as a `.divergent.<n>` one. These directories are never
cleaned up automatically — reclaim the space once reconciled.

## Snapshot transfer

When a replica is too far behind the primary's live journal and the
intervening archive segments have been purged — or its journal was
judged divergent — the primary streams a snapshot of its application
state to the replica before resuming normal replication. The transfer
is checksummed end-to-end (CRC32C) and verified incrementally on
receipt, so no large in-memory buffer is needed.

The snapshot is followed by a **segment seed**: the byte prefix of the
primary's journal segment containing the snapshot position. Written
verbatim as the replica's live segment, it makes the new replica's
journal a byte-copy of the primary's from the first moment — chain
validation holds immediately, with no alignment grace period. The seed
spans from the containing segment's start through the snapshot
position, so its size is bounded by the segment size (the primary
buffers it in memory for the transfer — released snapshot first, so
peak memory is one body at a time — with `--max-journal-mib 0` the
live segment, and therefore a worst-case seed, is unbounded; keep
size-driven rotation on when serving replicas). Sending `ROTATE` to the primary shortly before
attaching a fresh replica keeps the seed near the 4 KiB minimum. The
primary must retain journal segments at least as far back as its
serving snapshot, or transfers fail with an explicit error.

This lets a fresh replica bootstrap from a running primary without
requiring the full journal history.

## CLI flags

| Flag | Required | Default | Purpose |
|---|---|---|---|
| `--replication-bind <addr>` | No | — | Address to listen for replica connections. Bound at startup on any node that sets it — including a replica, which holds the port from boot and starts serving on it at promotion. |
| `--standalone` | No | `false` | Explicitly disable replication. Requires `--ack-policy disk`. |
| `--replica-of <addr>` | No | — | Run as a replica connected to the given primary. |
| `--replication-key <path>` | Replica | — | Ed25519 private key for replication auth. Required when `--replica-of` is set. The corresponding public key must be in the primary's `authorized_keys` with `replication` permission. |
| `--admin-bind <addr>` | Any | — | Address for the operator admin endpoint. Accepts `PROMOTE`, `ROTATE`, and `ACK-POLICY <policy>`. Bound at startup; the server fails to start if the address cannot be bound, so a node never runs with its admin commands silently unavailable. |
| `--ack-policy <policy>` | Primary | `disk+ram` | Active ack policy at startup: which copies of an event must exist before its response is released. `disk`, `ram`, `disk+ram`, or `two-disks`. Can be swapped at runtime via admin `ACK-POLICY`. |
| `--dpdk-peer-mac <mac>` | Replica on DPDK | derived | Ethernet address of the primary named by `--replica-of`. Only consulted when replicating over DPDK. See below. |

### Addressing the primary over DPDK

A replica dials out, so it must address its first frame to the primary
before any address resolution can happen — and on a DPDK port, ARP
cannot supply the answer. An SR-IOV VF does not receive broadcast, and
a port shared with the kernel steers only IPv4 by source address, so
ARP is never delivered to the userspace stack at all.

Without `--dpdk-peer-mac`, the replica assumes the address convention
that `dpdk-setup.sh` assigns to SR-IOV VFs. That assumption holds only
on that path. A port that keeps its real hardware address — anything
sharing the NIC with the kernel netdev — needs the flag, or the
replica's connection attempts go to an address nothing on the segment
answers for. There is no error in that case: the replica simply retries
with backoff and never connects.

Read the value from `/sys/class/net/<iface>/address` on the primary.
The startup log line reporting the seeded address names its source, so
check there first when a replica will not connect.

`--standalone` is mutually exclusive with both `--replication-bind`
and `--replica-of`. `--replica-of` **combines** with
`--replication-bind`: the replica binds the port at startup and holds
it unused until a promotion starts serving on it. Give every failover
candidate the same `--replication-bind` it would need as a primary, so
a promoted winner is immediately ready to accept re-pointed replicas —
no other process can have taken the port in the meantime. If none of
these flags are specified, the server runs in standalone mode.

## Wire protocol

Length-prefixed frames, little-endian. Runs over a dedicated TCP
connection separate from the client protocol.

### Replica → Primary

| Message | Layout | Purpose |
|---|---|---|
| Handshake | `[len:u32][type=0x01][last_sequence:u64][chain_hash:[u8;32]][epoch:u64][protocol_version:u16]` | Initial connection — replica reports its last durable sequence, the chain hash at that point, its fencing epoch, and the replication protocol version it speaks. A version mismatch is rejected with an explicit log line naming both versions. |
| Ack | `[len:u32][type=0x02][acked_sequence:u64][in_memory_sequence:u64]` | Replica confirms persisted writes up to `acked_sequence` and pre-journal receipt up to `in_memory_sequence`. Both fields are populated on every ack so the primary's gate can evaluate any policy without separate ack streams. |

### Primary → Replica

| Message | Layout | Purpose |
|---|---|---|
| StreamStart | `[len:u32][type=0x10][start_sequence:u64][segment_start_sequence:u64][anchor_hash:[u8;32]][epoch:u64]` | Confirms the handshake; carries the primary's fencing epoch and the journal-segment identity (starting sequence + chain anchor) a fresh replica creates its local journal with. Segment boundaries stay aligned from then on — rotation is primary-driven (see "Journal mirroring"). |
| NeedSnapshot | `[len:u32][type=0x11]` | Replica is too far behind the live journal and archives have been purged — triggers snapshot transfer. |
| HashMismatch | `[len:u32][type=0x12]` | The replica's journal is divergent at its reported position. The replica archives its local journal, then receives the snapshot transfer that follows on the same connection. |
| SnapshotBegin | `[len:u32][type=0x13][snapshot_len:u64][snap_sequence:u64][snap_chain_hash:[u8;32]]` | Start of snapshot transfer with metadata. |
| SnapshotChunk | `[len:u32][type=0x14][data...]` | Chunk of snapshot or segment-seed data (up to 64 KiB). |
| SnapshotEnd | `[len:u32][type=0x15][crc32c:u32]` | End of a snapshot or segment-seed transfer; CRC32C of the full payload for integrity. |
| Rotate | `[len:u32][type=0x16][boundary_seq:u64][tail_hash:[u8;32]]` | Primary-driven rotation: the replica rotates its journal at exactly `boundary_seq`, after verifying its own chain at the boundary equals `tail_hash`. |
| ChainCheck | `[len:u32][type=0x17][sequence:u64][chain_hash:[u8;32]]` | Periodic live-stream validation: the primary's chain value at `sequence`; the replica compares its own and treats a mismatch as divergence. |
| SegmentSeedBegin | `[len:u32][type=0x18][seed_len:u64]` | Start of the post-snapshot segment seed (see "Snapshot transfer"); the body rides SnapshotChunk frames and ends with a SnapshotEnd. |
| InputBatch | `[len:u32][type=0x21][count:u16][slot...]` | Batch of input events (sequence + timestamp + key/request hash + the event itself). |
| Heartbeat | `[len:u32][type=0x30][sequence:u64]` | Periodic idle keepalive (5 s interval) advertising the primary's last published sequence. |

## Cluster recovery

Most failures resolve without operator action:

- **Primary crashes, one or both replicas alive** — promote the most
  caught-up surviving replica. With a single replica, the ack policy
  guarantees it holds every acked event (`ram`, `disk+ram`, and
  `two-disks` all required its confirmation before each ack). With
  two replicas, an ack only ever required the
  *faster* one, so the two may differ by the final instants of
  traffic: a raft-driven failover handles this — it steers the
  election toward the most caught-up node, and a winner refuses to
  promote while a reachable peer holds more (see "Automatic
  failover"). For a manual `PROMOTE`, compare `journal_sequence` on
  each replica's `/healthz` first and promote the higher one. Under
  `ram`, additionally **do not restart the old primary as
  primary**: its journal may be short of events it already acked, and
  bringing it back in that role discards them (see "Failover is
  mandatory" above). Bring it back as a replica instead. Send
  `ACK-POLICY disk` after promotion if the new primary is standalone;
  restore the target policy once new replicas attach.
- **One replica crashes, primary and other replica alive** — the
  cluster continues under the configured policy. Under `disk+ram` the
  gate is satisfied by whichever node fsyncs first plus the surviving
  replica's in-memory ack. Under `two-disks` it's satisfied by both
  nodes persisting. The crashed replica reconnects and catches up
  automatically.

### Cluster-wide outage

When all nodes restart with their own journals they may differ in
length. Under every policy except `ram` the contract is that every
event the client was told about is on at least one PLP-backed disk, so
the node with the longest journal holds the acked frontier (and
possibly some events past it that were locally durable but never
confirmed to a client). Under `ram` the longest journal holds the
acked frontier *minus the un-synced tail* — a whole-cluster outage may
lose the final few milliseconds of acked events, which is that
policy's documented RPO ("No disk on the `ram` critical path" above).
The recovery procedure is the same under every policy:

1. Stop all nodes if not already stopped.
2. Determine each node's journal end sequence. Today this means
   starting each node briefly in `--standalone` mode and reading
   `journal_sequence` from `/healthz`. (A one-shot offline inspector
   is on the wishlist; see Limitations.)
3. Start the node with the longest journal as primary. If two nodes
   tie they have the same entries; either is valid.
4. Connect the others as replicas. Replicas that are behind catch up
   from the primary's journal. A replica holding entries past the
   primary's tail is detected as divergent at handshake and re-seeded
   automatically, with its old journal archived (see "Journal
   mirroring and divergence detection") — the new primary's journal is
   authoritative, and the archived entries remain available for
   reconciliation.

Under `two-disks`, the second-longest journal is also
guaranteed to hold the acked frontier (by contract two nodes had each
acked event on disk), so the top two journals being tied is the
normal-case post-recovery state.

## Upgrade and rollback notes

- **Upgrade primaries and replicas together.** The replication
  protocol carries a version number and frame layouts change between
  releases; a mixed-version pair refuses to connect, logging which
  side is behind. Replication (and trading, under the
  replica-requiring ack policies) is down until the versions
  match, so upgrade the whole cluster in one maintenance window.
- **Snapshots are forward-compatible.** This release reads snapshots
  written by pre-fencing releases (their epoch is taken as 0, which
  is exact — they predate any promotion). No action needed before
  upgrading.
- **Rolling back across a promotion needs care.** Once a promotion
  has been journaled, binaries older than this release cannot replay
  that journal — they stop at the promotion marker and report the
  entry as unreadable. The journal is healthy; the old binary simply
  predates the entry type. To roll back anyway, restore the node from
  a snapshot taken by the older release, or re-sync it as a fresh
  replica of a node running the older version.

## Observability

- The health endpoint's `trading`/`halted` flag (and the
  `melin_trading_active` gauge) reports `halted` on a fenced node even
  while replicas remain connected — point load-balancer probes and
  failover alerting at it.
- `melin_ack_policy_degraded` (Prometheus gauge on the health
  endpoint) — `1` while the active policy can't be satisfied by the
  current cluster shape, `0` otherwise. Alert on sustained `1`.
- `melin_ack_policy_degraded_seconds_total` (Prometheus counter)
  — cumulative seconds spent in the degraded state. Advances on each
  policy evaluation (per response batch under load, sub-second while the
  ack gate is stalled, and roughly once a second while idle), so a
  degradation shorter than that interval on a quiet venue may not be
  resolved. Use `rate(melin_ack_policy_degraded_seconds_total[5m])`
  for the fraction of the last 5 minutes spent degraded, without scraping
  the gauge at high frequency to reconstruct intervals. The accumulator
  resets to zero on process restart (standard Prometheus counter
  semantics — `rate()`/`increase()` handle resets); cumulative degraded
  time across a restart is not retained.
- Both are also exported under their pre-0.15 names,
  `melin_durability_policy_degraded` and
  `melin_durability_policy_degraded_seconds_total`, until the next minor
  release so existing alerts keep firing; re-point them before then.
- A warn-level log fires on transition into the degraded state and
  every 5 seconds while it persists; an info-level log fires on
  return to target.
- Every admin `ACK-POLICY` swap emits an info-level audit log with
  the `prev → next` transition.
- On raft-enabled nodes, the `melin_raft_node_id` / `melin_raft_term` /
  `melin_raft_leader_id` / `melin_raft_role` / `melin_raft_is_leader` /
  `melin_raft_driver_running` gauges expose control-plane election
  state on every node, replicas included. Alert on
  `melin_raft_driver_running` dropping to 0 (the control plane died —
  trading continues, but automatic failover is offline) and on a
  sustained absence of any node reporting `melin_raft_is_leader 1`
  (control-plane quorum lost).

## Limitations

### Concurrent manual promotions on non-raft deployments

With the control plane enabled, election-driven promotions mint unique
epochs and this limitation does not apply. Without it, epoch fencing
demotes a stale primary as soon as any higher-epoch node contacts it,
but two replicas promoted *manually and independently* during the same
outage land on the *same* epoch and neither fences the other — promote
exactly one replica per failover. On every deployment, a stale primary
that never hears from a higher-epoch node (e.g. fully partitioned with
its own replica set) keeps trading until the partition heals — fencing
triggers on contact, not on a timer.

### Static control-plane membership

The voter set is fixed at first boot (see "Deployment rules"). Runtime
voter add/remove/replace — and with it automatic re-pointing of
surviving replicas at a newly promoted primary — is roadmap work.
Replacing a dead voter today means standing up the whole cluster's
control plane again with a fresh peer list (fresh `--raft-dir`s), while
the data plane keeps running unaffected.

### No offline journal inspector

Determining a node's journal end sequence without starting the server
process is not yet supported — recovery playbooks have to spin each
node up in `--standalone` mode briefly to read `/healthz`. A
read-only journal-inspection tool that reads the journal files
directly is on the wishlist for the failover ergonomics
workstream.
