# Replication

Synchronous journal replication from a primary server to one or two
replicas. The primary streams journaled events to each replica over a
dedicated connection; the replica persists them locally, acknowledges,
and replays them through its own matching engine so its state stays
warm for failover.

Every client response is gated on a configurable **durability mode**
that decides what "the cluster has acknowledged this event" means
before the primary tells the client it's done. Modes range from
single-node durability (dev/test) to two-disk durability (compliance).

## Durability modes

The operator picks one of three named modes via
`--durability-mode <mode>` on the primary. Each carries a different
guarantee about what's confirmed before the client gets a reply:

| Mode | Guarantee at client ack | Vulnerable to | When to use |
|---|---|---|---|
| `local` | One node has the event on PLP-backed NVMe (the primary's own disk). | Primary disk hardware failure. | Dev, staging, single-node deployments. |
| `hybrid` *(default)* | One node has the event on PLP-backed NVMe **and** a second node has confirmed receipt in RAM. | Primary disk hardware failure within ~80 µs of the ack — the window before the secondary completes its own fsync. PLP-protected power loss is fully handled. | Typical live-trading deployments. Saves ~50–80 µs per fill vs `durably-replicated`. |
| `durably-replicated` | Two nodes have the event on PLP-backed NVMe. | Simultaneous disk failure on two nodes. | Compliance-driven venues that require two durable copies before client ack. |

The PLP (Power-Loss-Protection) capacitor on the NVMe device is what
makes `persisted` a meaningful guarantee without an explicit fsync
round-trip on every event — the device commits the write to flash
across a power loss.

### Strict fail-closed semantics

Every mode is **strict**. If the configured guarantee can't be met by
the current cluster shape (e.g. `hybrid` configured but no replica is
connected), the response gate stalls and clients see no reply rather
than the system silently weakening the contract. The
`melin_durability_policy_degraded` gauge on `/healthz` flips to `1` and
a warn-level log line is emitted on transition and every 5 seconds
while degraded.

This is deliberate: silently down-grading the durability contract under
load is exactly the kind of failure mode regulators and exchange
operators write off in post-mortems. Operators who want the system to
keep trading at reduced durability during a partial outage use the
runtime mode swap below.

### Trading halts when all replicas disconnect

Independent of the durability gate, the matching engine halts when
**every** configured replica disconnects. New client orders are
rejected with a `ReplicaDisconnected` reason code immediately —
clients see the halt reason rather than a TCP read timeout. The
rejection bypasses the durability gate because no engine state
changed: replicas will deterministically produce the same rejection
when they replay the same input on reconnect.

Standalone deployments (no replication configured) skip this halt
entirely and run under `local`.

### Runtime mode swap

The operator can change the active durability mode without restarting
the node via a signed admin command:

```
DURABILITY local
DURABILITY hybrid
DURABILITY durably-replicated
```

Sent over the same admin connection as `PROMOTE` / `ROTATE`, authenticated
with an operator key (Ed25519 challenge-response). Every swap is
INFO-logged with the `prev → next` transition for the audit trail.

The intended workflow is failover:

1. Primary dies, replica is promoted (`PROMOTE`).
2. The promoted node is now standalone — under `hybrid` its gate is
   structurally unsatisfiable (no second node to ack in memory) and
   trading would stall.
3. Operator sends `DURABILITY local` → the gate re-evaluates under
   `local` and trading resumes in seconds, no restart, no dropped
   client connections.
4. New replicas are spun up and connect.
5. Operator sends `DURABILITY hybrid` → the gate is satisfied by the
   new cluster shape and trading continues at the full contract.

The replica's admin listener also accepts `DURABILITY` — operators can
**pre-stage** the post-promotion mode by sending `DURABILITY local`
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
  gate can evaluate any mode without separate ack streams.
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
spin up new replicas immediately or send `DURABILITY local` to resume
trading at reduced durability.

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
when replication is configured. The remaining gap is two promotions
issued independently during the same outage: both new primaries land
on the same epoch and fencing cannot distinguish them. Promote exactly
one replica per failover — coordinated election lands with the
automatic-failover roadmap item.

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
sibling directory named `<journal>.divergent.<n>`. Under `local`
durability that journal may hold acked orders that did not survive the
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
| `--replication-bind <addr>` | No | — | Address to listen for replica connections. |
| `--standalone` | No | `false` | Explicitly disable replication. Requires `--durability-mode local`. |
| `--replica-of <addr>` | No | — | Run as a replica connected to the given primary. |
| `--replication-key <path>` | Replica | — | Ed25519 private key for replication auth. Required when `--replica-of` is set. The corresponding public key must be in the primary's `authorized_keys` with `replication` permission. |
| `--admin-bind <addr>` | Any | — | Address for the operator admin endpoint. Accepts `PROMOTE`, `ROTATE`, and `DURABILITY <mode>`. |
| `--durability-mode <mode>` | Primary | `hybrid` | Active durability mode at startup. `local`, `hybrid`, or `durably-replicated`. Can be swapped at runtime via admin `DURABILITY`. |

`--replication-bind` and `--standalone` are mutually exclusive.
`--replica-of` is mutually exclusive with both. If none are specified,
the server runs in standalone mode.

## Wire protocol

Length-prefixed frames, little-endian. Runs over a dedicated TCP
connection separate from the client protocol.

### Replica → Primary

| Message | Layout | Purpose |
|---|---|---|
| Handshake | `[len:u32][type=0x01][last_sequence:u64][chain_hash:[u8;32]][epoch:u64][protocol_version:u16]` | Initial connection — replica reports its last durable sequence, the chain hash at that point, its fencing epoch, and the replication protocol version it speaks. A version mismatch is rejected with an explicit log line naming both versions. |
| Ack | `[len:u32][type=0x02][acked_sequence:u64][in_memory_sequence:u64]` | Replica confirms persisted writes up to `acked_sequence` and pre-journal receipt up to `in_memory_sequence`. Both fields are populated on every ack so the primary's gate can evaluate any mode without separate ack streams. |

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

- **Primary crashes, one or both replicas alive** — promote any
  surviving replica via `PROMOTE`. Under `hybrid` or
  `durably-replicated`, all surviving replicas hold the same set of
  acked events (the contract guaranteed that before the client was
  told). Send `DURABILITY local` after promotion if the new primary
  is standalone; restore the target mode once new replicas attach.
- **One replica crashes, primary and other replica alive** — the
  cluster continues at the configured mode. Under `hybrid` the gate
  is satisfied by the primary plus the surviving replica's in-memory
  ack. Under `durably-replicated` it's satisfied by both nodes
  persisting. The crashed replica reconnects and catches up
  automatically.

### Cluster-wide outage

When all nodes restart with their own journals they may differ in
length. Under every mode the contract is that every event the client
was told about is on at least one PLP-backed disk, so the node with
the longest journal holds the acked frontier (and possibly some
events past it that were locally durable but never confirmed to a
client). The recovery procedure:

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

Under `durably-replicated`, the second-longest journal is also
guaranteed to hold the acked frontier (by contract two nodes had each
acked event on disk), so the top two journals being tied is the
normal-case post-recovery state.

## Upgrade and rollback notes

- **Upgrade primaries and replicas together.** The replication
  protocol carries a version number and frame layouts change between
  releases; a mixed-version pair refuses to connect, logging which
  side is behind. Replication (and trading, under the
  replicas-required durability modes) is down until the versions
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
- `melin_durability_policy_degraded` (Prometheus gauge on the health
  endpoint) — `1` while the active mode can't be satisfied by the
  current cluster shape, `0` otherwise. Alert on sustained `1`.
- `melin_durability_policy_degraded_seconds_total` (Prometheus counter)
  — cumulative seconds spent in the degraded state. Advances on each
  policy evaluation (per response batch under load, sub-second while the
  durability gate is stalled, and roughly once a second while idle), so a
  degradation shorter than that interval on a quiet venue may not be
  resolved. Use `rate(melin_durability_policy_degraded_seconds_total[5m])`
  for the fraction of the last 5 minutes spent degraded, without scraping
  the gauge at high frequency to reconstruct intervals. The accumulator
  resets to zero on process restart (standard Prometheus counter
  semantics — `rate()`/`increase()` handle resets); cumulative degraded
  time across a restart is not retained.
- A warn-level log fires on transition into the degraded state and
  every 5 seconds while it persists; an info-level log fires on
  return to target.
- Every admin `DURABILITY` swap emits an info-level audit log with
  the `prev → next` transition.

## Limitations

### Fencing cannot distinguish concurrent promotions

Epoch fencing (see above) demotes a stale primary as soon as any
higher-epoch node contacts it, but two replicas promoted independently
during the same outage land on the *same* epoch and neither fences the
other. Until coordinated election lands (next item), the operator
playbook is: promote exactly one replica per failover. A stale primary
that never hears from a higher-epoch node (e.g. fully partitioned with
its own replica set) also keeps trading until the partition heals —
fencing triggers on contact, not on a timer.

### No automatic failover (election shipped, promotion still manual)

Promotion is operator-driven via the `--admin-bind` endpoint. The
first phase of the control-plane Raft integration has landed: nodes
configured with `--raft-bind`, `--raft-node-id`, and `--raft-peer`
run leader election among themselves and expose the outcome through
the metrics endpoint (`melin_raft_term`, `melin_raft_leader_id`,
`melin_raft_role`, `melin_raft_is_leader`), so monitoring can already
observe which node the cluster would elect. Raft carries election,
membership, and (in a later phase) fencing-epoch allocation only —
order flow stays on the existing replication path and the durability
modes are unchanged. The control plane is deliberately unhurried:
~200 ms heartbeats, 1–2 s election timeouts, and vote requests from
nodes whose journal is behind the voter's are refused, so the
most-caught-up node wins.

In this phase election is **observational**: it does not trigger
promotion, and the manual `PROMOTE` playbook (including the
"promote exactly one replica" rule above) remains authoritative.
Configuration propagation and automatic promotion build on it next.

Peer links authenticate with the cluster's replication keys, so every
node's key must carry `replication` permission in every other node's
`authorized_keys` file, and each raft node needs `--replication-key`.
Durable election state (term, vote) lives in `--raft-dir` (default:
`<journal>.raft/`); treat it like the journal — never wipe or share
it on a live cluster, or a node can vote twice in one term.

### No offline journal inspector

Determining a node's journal end sequence without starting the server
process is not yet supported — recovery playbooks have to spin each
node up in `--standalone` mode briefly to read `/healthz`. A
read-only `melin-admin journal-info` subcommand that inspects the
journal files directly is on the wishlist for the failover
ergonomics workstream.
