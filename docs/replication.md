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
  journal that is logically identical to the primary's — same
  sequences, same events — and runs the same matching engine over
  it so its state stays warm for promotion.
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
`--replication-ring-size`, default 64 slots × 512 KiB = 32 MiB per
slot). If a slot is full for longer than 500 ms (replica not keeping
up), the primary disconnects that replica and frees the ring — the
surviving replica and client trading are unaffected.

## Manual promotion

The admin endpoint accepts `PROMOTE` on a replica to switch it to
primary mode in-process: the warm matching state is reused directly,
no journal re-replay, no snapshot reload. Sub-second switchover.

After promotion the new primary will halt new orders if it has no
replicas connected (see above) — the operator's playbook is to either
spin up new replicas immediately or send `DURABILITY local` to resume
trading at reduced durability.

The old primary must be stopped to prevent split-brain. Automatic
fencing is not yet implemented; see Limitations.

## Snapshot transfer

When a replica is too far behind the primary's live journal and the
intervening archive segments have been purged, the primary streams a
snapshot of its application state to the replica before resuming
normal replication. The transfer is checksummed end-to-end (CRC32C)
and verified incrementally on receipt, so no large in-memory buffer
is needed.

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
| Handshake | `[len:u32][type=0x01][last_sequence:u64][chain_hash:[u8;32]]` | Initial connection — replica reports its last durable sequence and the chain hash at that point. |
| Ack | `[len:u32][type=0x02][acked_sequence:u64][in_memory_sequence:u64]` | Replica confirms persisted writes up to `acked_sequence` and pre-journal receipt up to `in_memory_sequence`. Both fields are populated on every ack so the primary's gate can evaluate any mode without separate ack streams. |

### Primary → Replica

| Message | Layout | Purpose |
|---|---|---|
| StreamStart | `[len:u32][type=0x10][start_sequence:u64][genesis_len:u32][genesis_entry_bytes...]` | Confirms the handshake; includes the raw genesis entry so a fresh replica's hash chain starts byte-identical to the primary's. |
| NeedSnapshot | `[len:u32][type=0x11]` | Replica is too far behind the live journal and archives have been purged — triggers snapshot transfer. |
| SnapshotBegin | `[len:u32][type=0x13][snapshot_len:u64][snap_sequence:u64][snap_chain_hash:[u8;32]]` | Start of snapshot transfer with metadata. |
| SnapshotChunk | `[len:u32][type=0x14][data...]` | Chunk of snapshot data (up to 64 KiB). |
| SnapshotEnd | `[len:u32][type=0x15][crc32c:u32]` | End of snapshot transfer; CRC32C of the full payload for integrity. |
| HashMismatch | `[len:u32][type=0x12]` | Chain hash mismatch at the replica's reported sequence (reserved — see Limitations). |
| InputBatch | `[len:u32][type=0x21][count:u16][slot...]` | Batch of input events (sequence + timestamp + key/request hash + the event itself). Divergence is verified at Checkpoint markers inside the slot stream. |
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
   from the primary's journal. Replicas that have extra entries past
   the primary's tail are reset during catch-up — the new primary's
   journal is authoritative after promotion.

Under `durably-replicated`, the second-longest journal is also
guaranteed to hold the acked frontier (by contract two nodes had each
acked event on disk), so the top two journals being tied is the
normal-case post-recovery state.

## Observability

- `melin_durability_policy_degraded` (Prometheus gauge on the health
  endpoint) — `1` while the active mode can't be satisfied by the
  current cluster shape, `0` otherwise. Alert on sustained `1`.
- A warn-level log fires on transition into the degraded state and
  every 5 seconds while it persists; an info-level log fires on
  return to target.
- Every admin `DURABILITY` swap emits an info-level audit log with
  the `prev → next` transition.

## Limitations

### No chain-hash validation at handshake

The replica reports its `chain_hash` at `last_sequence` in the
`Handshake` frame and the `HashMismatch` response type is reserved in
the wire protocol, but neither side compares the two against the
primary's own journal at the same sequence. A replica with divergent
history (e.g. previously connected to a different primary, or with a
corrupted journal) is accepted without warning. After failover the
promoted node would hold a journal that doesn't match the events
clients were told about. Tracked as a roadmap item — two
implementation shapes are scoped (tip-only check, ~half a day; full
arbitrary-N check, 1–2 days against the journal-crate hashing
arithmetic).

### No automatic split-brain fencing

After manual promotion, the old primary must be stopped manually. If
it stays up, two primaries will accept writes. Automatic fencing
(epoch-based or STONITH-style) is on the roadmap and gated on the
Raft integration.

### No automatic failover

Promotion is operator-driven via the `--admin-bind` endpoint. Leader
election and automatic promotion are on the roadmap and gated on the
Raft integration.

### No offline journal inspector

Determining a node's journal end sequence without starting the server
process is not yet supported — recovery playbooks have to spin each
node up in `--standalone` mode briefly to read `/healthz`. A
read-only `melin-admin journal-info` subcommand that inspects the
journal files directly is on the wishlist for the failover
ergonomics workstream.
