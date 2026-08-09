# Control-Plane Raft (contributor notes)

The `melin-raft` crate and its wiring in `melin-server-runtime` implement
the control plane described operationally in `docs/replication.md`: leader
election, static membership, and fencing-epoch allocation. This document
covers the design arguments a contributor needs before touching it.

## Scope and non-goals

The control plane carries **election, membership, and fencing epochs —
nothing else**. Order flow stays on the synchronous replication data
plane with its durability modes untouched. Non-goals on this iteration:
runtime voter changes (static `--raft-peer` lists only), client
redirects / follow-the-leader, witness (data-plane-less) voters.

## Architecture

One dedicated `raft-driver` thread per node owns:

- an [openraft](https://docs.rs/openraft/0.9) `Raft` instance (0.9.x),
- a **current-thread tokio runtime** — openraft requires an async
  runtime; confining it to this one thread keeps the rest of the
  codebase synchronous and the hot path untouched,
- the control-plane TCP listener (kernel TCP always, even on DPDK
  nodes — the control plane must not consume a DPDK queue),
- a metrics bridge copying `RaftMetrics` into the lock-free
  `RaftStatus` atomics the health endpoint and the auto-promotion
  thread read.

Everything that *acts* on elections lives outside the async world:
`raft_promotion.rs` is a plain `std` thread polling `RaftStatus` every
100 ms and filing a `PromotionRequest`. This keeps promotion policy —
whose inputs (acking mode, primary link state, fence state) are all
data-plane concepts — synchronous and unit-testable without tokio.

The data plane never calls into `melin-raft`. The only raft →
data-plane edges are the `PromotionRequest` (unfileable without an
election quorum) and fencing (which keys off journaled epochs, not raft
terms). **Raft quorum loss therefore cannot halt trading**; failover
degrades to the manual `PROMOTE` playbook.

## Term = fencing epoch

An auto-promotion journals `EpochBump { epoch: term }` — the election
term becomes the tenure's fencing epoch, so two promotions from
different elections always mint distinct epochs and the newer fences
the older. This closes the documented "fencing cannot distinguish
concurrent promotions" gap.

**This requires openraft's `single-term-leader` cargo feature.** The
default openraft leader id is the lexicographic pair `(term, node_id)`,
which deliberately allows more than one leader to be elected within one
term. `single-term-leader` restores the standard-Raft rule — at most
one leader per term, terms strictly increasing per leader tenure —
which is what makes the term usable as an epoch. The raft storage
format version byte covers the `Vote` layout, so a binary built with
the feature flipped refuses an existing `--raft-dir` instead of
misreading it.

Epoch allocation on promotion is `max(fence_epoch + 1, requested)`
(`server.rs`): a manual `PROMOTE` files `MANUAL` (= 1) and folds to the
classic `epoch + 1`; an auto-promotion files its term. Manual and
automatic promotions coexist — first request wins the one-shot, and a
manual promotion that outruns raft terms simply makes the policy refuse
(`term <= fence_epoch`) until terms catch up.

## Journal-tip vote recency filter (`recency.rs`)

Melin replicates order data out-of-band from raft, so raft's own
log-recency vote rule says nothing about journal recency. Every RPC
envelope carries the sender's journal tip `(fencing epoch, advertised
sequence)`; a voter **drops** vote requests from candidates behind its
own tip, before they reach `Raft::vote`. Dropping is indistinguishable
from packet loss to raft, so safety is untouched — the same shape as
MongoDB's election over its oplog, PacificA, and Viewstamped
Replication. Epoch dominates sequence in the tip order: a long suffix
on an old epoch is a divergent lineage, not recency.

Tip sources (`AdvertisedJournalTip` in transport-core):

- **replica** — the receive loop advances it to its in-memory
  *accepted* position (a promotion drains the ring into the journal, so
  accepted events survive; advertising only the fsynced position would
  understate the tip),
- **primary** — the journal stage publishes its durable cursor per
  fsync batch,
- the one legitimate regression is a snapshot resync discarding a
  known-divergent suffix (explicit `reset`).

**Liveness escape.** The tip order and raft's log order are independent
orderings applied conjunctively, and they can veto each other into
leaderlessness. After `LIVENESS_ESCAPE_DROPS` (8) consecutively dropped
vote requests with no leader observed, the filter opens until a leader
is next seen. The driver re-arms it whenever its metrics show *any*
leader — a node that is itself the leader receives no appends and would
otherwise never re-arm. Two openraft-specific consequences:

- openraft has no pre-vote, so a filtered candidate still inflates its
  own term and can briefly churn an established leader; the leader
  lease and the re-arm bound the disruption.
- steering is therefore **best-effort**: under sustained churn the
  escape can legitimately let a behind node win. The authoritative
  safety checks live at promotion time (`auto_promotion_decision`),
  never at the ballot box. Tests that assert "behind node never wins"
  must give the caught-up side a quorum that does not need the behind
  node's cooperation (see `tests/election.rs`).

## Auto-promotion policy (`raft_promotion.rs`)

`auto_promotion_decision` is a pure function; the refusal strings are
operator-facing. An election win is the data-safety proof (a quorum of
voters held no more data than this node); the rules cover what an
election cannot prove — see the rustdoc on the function for each rule's
argument. The `local`-durability refusal is the load-bearing one: acks
in `local` mode never waited for any replica, so no election can prove
data completeness, and C3's acking-mode propagation exists precisely so
the replica judges the mode the *dead primary* acked under rather than
its own configuration.

Under `--raft-auto-promote` the raft mesh doubles as a fencing channel
(`SupersessionPolicy` in `rpc_server.rs`): a serving node — a primary,
or a replica whose promotion is already in flight — that reads a peer
envelope advertising a strictly higher fencing epoch self-fences and
shuts down, exactly like the data-plane handshake path but without
waiting for a data-plane connection to cross.

## RPC transport

Length-prefixed postcard frames (`wire.rs`) carrying the tip envelope
plus openraft's serde-enabled RPC types. Postcard over bincode: compact
varints and no encoder/decoder configuration knobs to diverge between
peers. Peer links authenticate with the replication Ed25519
challenge-response (initiator proves; `Permission::Replication`;
codecs shared with the data plane so the two paths cannot diverge), and
the server pins each connection's authenticated key to its configured
node id — a request claiming another id closes the connection.
`--raft-peer` lists are identical on every node and include the node
itself (the self entry supplies the dialable address written into the
first-boot membership).

## Storage (`storage.rs`)

File-backed `RaftLogStorage`/`RaftStateMachine` under `--raft-dir`:

| file | write discipline | damage policy |
|---|---|---|
| `vote` | atomic tmp + fsync + rename | refuse to open (a forgotten vote can double-vote) |
| `log` | append + fsync before the `LogFlushed` callback | torn tail truncated (never acked); header/purge-marker damage refuses (only ever written atomically — recovery there would be a log reversion) |
| `sm`, `snapshot` | atomic replace | refuse to open |

Every file carries a magic + format-version byte. Log truncate/purge
rewrite the file wholesale — the election-only log is dozens of
entries. Blocking fsync inline on the driver runtime is deliberate:
appends happen only at leader establishment and membership changes,
never per-heartbeat, and a ~ms fsync is harmless against the 200 ms
heartbeat / 1 s election floor. The primary storage test is openraft's
own `Suite::test_all` compliance suite plus a torn-tail byte-sweep.

Losing `--raft-dir` while peers are live can double-grant a vote in a
term (standard Raft hazard) and with it the term-uniqueness the epoch
design rests on — hence the operator rule in `docs/replication.md`.

## Election tuning

200 ms heartbeats, 1–2 s randomized election timeout — deliberately
slow. Control-plane latency only affects failover reaction time (an
election plus a 100 ms poll), never order flow; stability wins.

## What the in-process tests cannot cover

True power-loss fsync semantics of the raft files, asymmetric network
partitions (tests kill threads and links, not selective packet loss),
inter-node clock skew, and whole-process `kill -9` mid-append (covered
indirectly by the storage crash tests). The venue for those is a manual
multi-VM failover drill with the AWS bench scripts.
