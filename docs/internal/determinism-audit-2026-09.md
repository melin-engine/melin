# Determinism and durability audit: September 2026

A hunt for every way Melin can break the promises it is sold on: that
replaying the journal reproduces the live state, that every node holds
the same history, and that nothing a client was told about is lost.
Twelve read-only reviews ran in parallel, one per subsystem: journal
write side, journal recovery, journal stage and matching mirror, shadow
and snapshots, primary-side replication, replica-side replication, resync
and divergence repair, promotion and fencing, ingress and ticks, pipeline
rings and seqlock, response gate and ack policy, and the shipped
applications. The most serious findings were then re-read and, where
practical, reproduced with failing tests.

Status: **open**. Findings 1, 2 and 24, and the sequence checks of
finding 37, are fixed (see
[replica-reconnect-fix-2026-09.md](replica-reconnect-fix-2026-09.md));
each says so in its section. The rest is open. Every reference is to
commit `0fa03c79` (2026-09-25). Line numbers drift, so re-locate each site
before working on it.

## Evidence levels

- **Reproduced**: a failing test demonstrates the defect. The tests are
  described under [Reproductions](#reproductions) so they can be rebuilt
  as regression tests alongside each fix.
- **Confirmed**: the mechanism was re-read in the code and holds. The
  trigger timing was not exercised.
- **Reported**: found by the reviews and not independently re-verified.
  Treat it as a lead.

A subsystem with no finding here was reviewed, not proven correct. DPDK
paths were read, not run.

## The promises under test

1. Replaying the journal, from genesis or from a snapshot, reproduces the
   state the live application had.
2. The matching stage, the shadow stage and replay hand the application
   the same events with the same context.
3. A replica's journal is byte-identical to its primary's, and its state
   matches.
4. No client, and no other observer, sees an event before it holds the
   copies the ack policy demands.
5. Recovery never silently discards durable entries: corruption is an
   error, and only a torn, never-acknowledged tail is dropped.
6. Genesis is journaled exactly once per lineage.
7. Failover promotes a node holding every acknowledged event, and fencing
   stops a superseded primary.

## Triage summary

Severity is a judgement from code reading.

| # | Finding | Severity | Evidence |
| --- | --- | --- | --- |
| 1 | A replica reconnecting before its first durable batch re-applies the primary's history | **Critical** | Reproduced; fixed |
| 2 | A replica reconnecting while its journal lags re-applies the unjournaled tail | High | Confirmed; fixed |
| 3 | A snapshot-only boot journals genesis a second time | High | Reproduced |
| 4 | A first boot that fails after creating the journal loses genesis for good | High | Reproduced |
| 5 | A sequence gap in the live segment truncates durable entries | High | Reproduced |
| 6 | A zeroed range in the live segment reads as end of data | Medium | Reproduced |
| 7 | A flipped bit in an entry's length field bypasses the CRC | Medium | Reproduced |
| 8 | Replicated entries have no end-to-end integrity; DPDK verifies no checksum on receive | High | Confirmed |
| 9 | The event publisher broadcasts reports before they are durable | High | Confirmed |
| 10 | A runtime ack-policy change never reaches replicas while data flows | High | Confirmed |
| 11 | The DPDK replication sender skips a batch that does not fit its transmit queue | High | Reproduced |
| 12 | A primary disk stall makes a truthful replica look divergent | Medium | Confirmed |
| 13 | Raft-mesh fencing ignores reply envelopes | Medium | Confirmed |
| 14 | A replica adopts its primary's epoch before it holds that epoch's entries | Medium | Confirmed |
| 15 | Promotion can drop a rotation boundary | Medium | Confirmed |
| 16 | A torn, never-acknowledged final entry can block startup | Medium | Reproduced |
| 17 | The tick contract is untested and has no working example | Medium | Confirmed |
| 18 | A ServerBusy reply can overtake replies to earlier requests | Medium | Confirmed |
| 19 | Recovery trusts the page cache after a failed fsync | Medium | Reported |
| 20 | A failed directory fsync after rotation does not hold back acks | Medium | Confirmed |
| 21 | The resync archive is not atomic, and a lone snapshot is ignored | Medium | Confirmed |
| 22 | Snapshot paths are resolved three ways, and a served snapshot is barely checked | Medium | Confirmed |
| 23 | Without the hash chain, an ahead-of-tip replica is credited by the ack gate | Medium | Confirmed |
| 24 | The shutdown drain skips a failed encode after allocating its sequence | Low | Confirmed; fixed |
| 25 | A crash inside a snapshot save leaves no snapshot under its name | Low | Confirmed |
| 26 | The application codec contract is unenforced | Low | Confirmed |
| 27 | The DPDK response stage drops a connection without closing it | Low | Reported |
| 28 | A half-open replica slot can count one replica twice | Low | Reported |
| 29 | Tightening the ack policy lags the in-flight batch | Low | Reported |
| 30 | `no-persist` fakes durability without a signal | Low | Reported |
| 31 | Fence suppression is partial | Low | Reported |
| 32 | Epoch uniqueness has holes | Low | Confirmed in part |
| 33 | Divergence repair can hand a forked journal to a pending promotion | Low | Reported |
| 34 | Two primaries can ack before the new epoch is visible | Low | Reported |
| 35 | Shadow snapshots can slip by an interval under load | Low | Reported |
| 36 | The mark barrier aliases released ring slots, and `RingBuffer` is too loosely `Sync` | Low | Reported |
| 37 | Journal invariants are checked only in debug builds | Low | Confirmed in part; sequence checks fixed |
| 38 | Filesystem edge cases in the journal | Low | Reported |
| 39 | Archive bookkeeping edge cases | Low | Reported |
| 40 | Races that cost a reconnect or a failed resync | Low | Reported |
| 41 | The notary accepts trailing bytes after its head query | Low | Reported |

Availability defects found on the way, and documentation that no longer
matches the code, follow the findings.

## Replica reconnect

### 1. A replica reconnecting before its first durable batch re-applies the primary's history

**Critical. Reproduced.** Breaks promises 1, 3 and 7.

**Status: fixed.** The seqlock is seeded from the writer when the
pipeline is built, and the reproduction is a regression test
(`tests/replica_reconnect.rs` in server-runtime, and the seed and
refusal tests in transport-core's `pipeline_tests.rs`).

- `transport-core/src/pipeline.rs:3098-3109`: `setup_chain_hash_publisher`
  creates the `FsyncState` seqlock as `FsyncState::default()`, which is
  sequence 0 with a zero chain hash. `build_replica_pipeline` calls it
  unconditionally (`:3395`) and seeds only the durable wire-seq cursor from
  the writer (`:3408-3413`). Only the journal's disk thread ever stores to
  the seqlock, after a durable batch.
- `server-runtime/src/replication/tcp_receiver.rs:615-631` reads the
  reconnect handshake pair from that seqlock whenever a pipeline exists;
  `replication/dpdk.rs:1204-1212` does the same. The branch that would
  read the correctly seeded cursor is dead on replicas.
- That pair becomes the session anchor (`tcp_receiver.rs:779`) and seeds
  the contiguity gate (`receiver_transport.rs:738`).
- On the primary, `replication/validate.rs:163-165` accepts sequence 0 as
  a fresh replica, and catch-up streams from sequence 1 whenever the
  oldest retained segment starts there. That is the default, since
  nothing prunes archives.
- The replica's journal stage takes each slot's sequence verbatim and
  calls `set_next_sequence(seq + 1)` (`pipeline.rs:1265-1267`). Only
  `debug_assert!`s catch a backward move (`journal/src/encoder.rs:256-265`,
  `:326-333`). The matching stage never looks at the sequence.

**Trigger.** Any session that ends before the rebuilt pipeline's first
durable batch: a primary restart or crash, a network blip, an eviction, a
`StreamGap` in the first catch-up frames, an io_uring setup failure. A
replica rebuilds its pipeline on every boot and after every snapshot
resync. With the primary's ticks on, the window is at most one tick
interval after each rebuild. With ticks off on an idle primary it has no
bound.

**Consequence.** The replica applies the whole history again on top of
the state it already holds. In release builds its journal gets sequence 1
onwards appended after its tail, and recovery then fails on
`SequenceDuplicate`, so the node cannot restart until someone repairs the
journal by hand. Detection waits for the next rotation announce or chain
check. A promotion before then serves the double-applied state. When the
primary has pruned its history, the same bug instead archives a healthy
journal and forces a full resync.

**Fix direction.** Seed the seqlock at build time with the writer's
`(next_sequence - 1, chain hash)` and ring position 0; the shadow's
`has_events` guard and alignment check keep that safe. Make a backward or
non-contiguous stamped sequence a hard error in release (finding 37).

### 2. A replica reconnecting while its journal lags re-applies the unjournaled tail

**High. Confirmed.** Breaks promises 1, 3 and 7. Same family as finding 1.

**Status: fixed**, by the second fix direction below, not the first:
the reconnect waits until the journal covers every slot the previous
session published, then handshakes from that one `FsyncState` load.
Anchoring the gate higher would let the re-delivery rule hide a fork
after a failover. The plan records why.

The handshake sends `FsyncState.journal_seq`, the durable position, and
the session anchor equals it. The input ring can still hold entries the
previous session accepted but the journal has not written yet. Several
session exits skip `drain_pending_acks`:

- a `poll_recv` error (`receiver_transport.rs:854-857`), which is how the
  kernel-TCP transport reports every disconnect, clean close included;
- a `send_ack` error (`:843-850`);
- a `StreamGap` (`:908-909`).

The draining branch (`:861-872`) is effectively unreachable on kernel TCP.
Only a backoff sleep of at least a second separates the exit from the next
handshake. `receiver_transport.rs:743-747` already notes that "a reconnect
whose handshake read the journal before the ring settled" is possible, but
guards only the advertised raft tip.

**Scenario.**
1. The replica's disk runs slower than the stream or stalls. The docs
   tolerate this explicitly, and an adopted rotation that fails backs off
   before retrying. The ring holds accepted entries D+1 to A.
2. The primary evicts the replica when its replication ring fills, or the
   link drops.
3. After the backoff the replica handshakes at D, and the primary streams
   from D+1.
4. The gate accepts D+1 to A again, behind the originals. The rest
   matches finding 1.

**Fix direction.** Carry the previous session's accepted position across
the reconnect and anchor the gate at the larger of it and the handshake
position. Alternatively, wait (abortable on journal failure) until the
journal's ring progress reaches the producer cursor before reading the
handshake pair. Progress alone is not enough, because the disk thread
stores progress before the seqlock.

## Genesis

### 3. A snapshot-only boot journals genesis a second time

**High. Reproduced.** Breaks promise 6.

`server-runtime/src/server.rs:3166-3168` sets
`needs_seeding = !journal_exists && !archives_exist` and ignores the
snapshot. The `SnapshotOnly` arm (`:3136-3150`) restores a snapshot whose
state already includes genesis, then `run_as_primary` journals
`startup.genesis` (`:1833-1846`); the DPDK boot path does the same. The
contract in `startup.rs:15-20` says genesis is "Journaled once, by the
node that creates the journal".

**Trigger.** The Standard Upgrade in `docs/journal.md` (take a snapshot,
deploy, start on a fresh journal) produces exactly this layout, and so
does losing every journal segment while the snapshot survives. Replicas
re-bootstrap from the result, so every node agrees on the wrong state and
the audit trail records the duplicated genesis.

**Exposure.** The shipped examples declare no genesis. Any application
that seeds reference data or balances through genesis is affected.

**Fix direction.** Seed only for `BootstrapSource::Fresh`.

### 4. A first boot that fails after creating the journal loses genesis for good

**High. Reproduced** for the configuration trigger. Breaks promise 6.

`init_engine` creates the journal file, header included
(`server.rs:1082-1083`), before `run_as_primary` validates the
configuration (`:1339-1352`), binds its listeners and journals genesis.
Nothing records that genesis completed; the next boot infers it from the
file existing (`:3168`), takes the `JournalOnly` path and seeds nothing.

**Triggers.**
- `--standalone` with any ack policy but `disk`. The default policy is
  `disk+ram`, so this always reproduces.
- A port already in use, or a failure in `clone_via_snapshot` or the
  journal stage start.
- SIGTERM while a fresh primary waits for its first replica before
  seeding: the stages exit, and genesis is published into a ring nobody
  reads (reported).
- A crash in the middle of a large genesis, which leaves a prefix.
- Promotion: a replica that followed a fresh primary which died before or
  during genesis promotes with `needs_seeding = false`, so the cluster
  never gets the rest (reported).

**Consequence.** The node serves from `Default`, or from a genesis prefix,
with no error and no log line, and replicas follow it.

**Fix direction.** Validate the configuration and bind every port before
creating the journal. Mark genesis completion durably, for example with a
runtime entry after the last genesis event, and refuse to serve or promote
a non-empty history that lacks the mark.

## Journal recovery

### 5. A sequence gap in the live segment truncates durable entries

**High. Reproduced.** Breaks promise 5.

- `transport-core/src/journaled_app.rs:366-378` replays the live segment
  with `allow_partial_tail = true`, and `:640-650` treats `SequenceGap` as
  a torn tail: a warning, then replay stops.
- `JournalReader` also reports a first entry that disagrees with the
  header's `starting_sequence` as `SequenceGap` (`journal/src/reader.rs:318-326`),
  so that case is tolerated too.
- `W::open_append` (`journaled_app.rs:401`) then scrubs the file from the
  stopping point: `SegmentFile::open_append` truncates and syncs, keeping
  no copy of what it removed.

**Why the tolerance is wrong.** The buffered writer issues one ordered
`pwritev` per drain into a zero-filled, preallocated file, so no crash can
leave a CRC-valid entry with the wrong sequence at the tail. A gap is a
writer bug (findings 24 and 26 each produce one), a misdirected write or
tampering. `docs/journal.md` promises that "a CRC mismatch or sequence
gap mid-stream is treated as real corruption and returns an error".

**Consequence.** Acknowledged, replicated entries disappear. On a primary
the next events reuse their sequence numbers, and replicas holding the
originals are judged divergent and resynced from the truncated history.

**Fix direction.** Refuse a gap in the live segment unless everything
after it is preallocation zeros within the bound of one unsynced drain.
Move any discarded tail aside instead of truncating it.
`segment.rs::verify_lineage_reports_gap_at_live_tail` codifies today's
tolerance and changes with it.

### 6. A zeroed range in the live segment reads as end of data

**Medium. Reproduced.** Breaks promise 5.

`reader.rs:160-165` and `:179-183` return end of data on two zero magic
bytes, with no look at what follows. The zero-CRC path (`:230-302`) does
check the remainder, and its own comment calls a hole with data after it
"data loss".

**Triggers.** A lost, misdirected or trimmed write, or storage returning
zeros for a failed block (thin provisioning, a lost unwritten-extent
conversion), that starts on an entry boundary; also finding 19. In the
worst case the first data page reads as zeros: entries start at offset
4096, so the whole live segment is discarded and recovery reports
success.

**Archive variant (reported).** When the live segment is missing, the last
archive has no successor header to cross-check, so an early stop there is
accepted too. Archives are compacted to their exact length, so any
non-zero byte after the stop is corruption and could be refused.

**Fix direction.** Give the zero-magic path the zero-CRC path's remainder
scan, bounded as in finding 5, and preserve the tail.

### 7. A flipped bit in an entry's length field bypasses the CRC

**Medium. Reproduced.** Breaks promise 5.

`journal/src/codec.rs:511-529` locates an entry's CRC from its own
`length`, and `reader.rs:264-302` computes the zero-tail check from that
same, possibly corrupted, value. One flipped bit in the last entry's
length moves the claimed CRC slot into preallocation zeros. The stored CRC
reads 0, the rest of the file is zero, and the reader returns end of data
with a warning. `open_append` then deletes the entry. Larger flips can
swallow entries up to about 64 KiB before the tail.

**Fix direction.** Reject a `length` above `ENTRY_META_SIZE` plus the
widest payload (the application's `MAX_ENCODED_SIZE`, or eight bytes for a
tick or epoch bump) before checking the CRC. The reader already knows the
event type.

### 16. A torn, never-acknowledged final entry can block startup

**Medium, availability. Reproduced.**

The reader accepts a live tail as end of data only for zero magic, for an
entry running past EOF (impossible in a preallocated file), or for a zero
CRC followed by zeros (`reader.rs:160-302`). A process killed mid-`pwritev`
leaves a prefix of the drain in the page cache, cut at a page boundary. A
cut between the two magic bytes gives "bad entry magic"; a cut inside the
CRC gives a checksum mismatch with a non-zero stored value. Power loss
during `fdatasync` can persist sectors out of order with the same effect.

**Consequence.** Boot fails, and the node stays down until someone
truncates the file by hand, although the torn entry was never
acknowledged. `docs/journal.md` lists a crash mid-write as handled.

**Fix direction.** Treat any malformed entry as a torn tail when
everything after it, within the bound of one unsynced drain, is zeros, and
preserve what is discarded. The existing sweep
`crash_at_every_byte_offset_recovers` truncates the file instead of
leaving preallocation zeros, which is why it passes.

## Integrity and observers

### 8. Replicated entries have no end-to-end integrity, and DPDK verifies no checksum on receive

**High. Confirmed.** Breaks promises 3 and 4.

- A replication slot is the journal entry minus its magic and CRC
  (`journal/src/encoder.rs:368-375`). `transport-core/src/replication_wire.rs:22`
  states the assumption: "TCP/DPDK handle framing and integrity".
- The replica decodes each slot and re-encodes it with a fresh CRC
  (`pipeline.rs:1276-1289`), so its CRC certifies whatever arrived.
- On DPDK, `dpdk/src/dpdk/port.rs:111-166` enables receive checksum offload
  when the NIC supports it, and `device.rs:376-393` then tells smoltcp not
  to verify TCP or IPv4 checksums. Nothing reads the mbuf's receive flags,
  and DPDK flags a bad checksum rather than dropping the packet, so no TCP
  checksum is checked anywhere on receive. Client ingress on DPDK has the
  same gap.

**Scenario.** A segment corrupted past the link FCS (a switch buffer, the
NIC, PCIe or host memory) decodes to a different, valid event. The
replica applies it, acknowledges it in memory, journals it with a valid
CRC and acknowledges it as persisted, and the primary's gate counts that
copy. Detection waits for the next periodic chain check or rotation
announce. If the primary is lost first, the promoted replica serves the
corrupted event as acknowledged history. Kernel TCP still has its 16-bit
checksum, which is known to miss a fraction of errors.

**Fix direction.** Ship the primary's entry CRC in each slot, and have the
replica compare it with its own re-encoded CRC before publishing to its
ring. That also catches an application codec that does not round-trip
(finding 26). On DPDK, drop mbufs flagged bad and verify in software when
the NIC reports the status as unknown. This keeps the symmetric re-encode
the deferred "Verbatim byte-path journaling" item settled on.

### 9. The event publisher broadcasts reports before they are durable

**High for any application with a feed. Confirmed.** Breaks promise 4.

The publisher's output-ring consumer is gated only on the producer
(`pipeline.rs:3187-3195`). `EventPublisherFn` (`server.rs:76-82`) receives
the raw consumer, a bind address, keys, the shutdown flag and a wait
strategy: no durable cursor, no ack policy and no replica cursors. An
application's publisher cannot gate even if it wants to.

**Scenario.** Matching produces a fill at wire sequence N, and the
publisher sends it to market-data and audit subscribers. The primary loses
power before N has its copies, and failover promotes a node without N.
Subscribers saw a trade that does not exist in the system of record. The
Exchange Core's publisher, maintained separately, broadcasts on consume
and stamps frames with the output-ring position, which restarts at every
boot (reported).

**Docs.** `docs/pipeline-architecture.md` says "no client will see those
results until the journal confirms durability", and suggests the
publisher for "an audit stream".

**Fix direction.** Give the publisher the response stage's gate, so the
runtime releases only gated slots to it, or at least pass it the gate
inputs and document the choice. Stamp publisher frames with the wire
sequence so subscribers can reconcile after a failover.

### 11. The DPDK replication sender skips a batch that does not fit its transmit queue

**High for DPDK replication. Reproduced** for the consumer semantics,
**confirmed** for the sender. Breaks promise 3.

`ReplicationConsumer::try_read` advances the read position
(`journal/src/replication.rs:266-288`). Its docs call it a peek, and only
a `debug_assert!` stops a second read before `commit()`.
`server-runtime/src/replication/dpdk.rs:1009-1031` reads a batch, and when
it does not fit even after a flush and a poll, breaks without committing,
meaning to retry it on the next tick. The next tick reads the following
batch instead, and the next commit releases both.

**Consequence.** In release builds the replica never receives the skipped
batch:
- a skipped data batch becomes a `StreamGap` and a reconnect, which opens
  the windows of findings 1 and 2;
- a skipped `Rotate` frame leaves the replica misframed, later judged
  divergent;
- a skipped chain check is lost silently.

In debug builds the assertion panics the DPDK poll thread, which also
serves client ingress. The per-socket transmit queue is the size of one
ring chunk, so a large batch fits only when the queue is nearly empty,
which happens under normal load. `go_idle` also leaves a pending read in
place (reported).

**Fix direction.** Make `try_read` return the pending batch while one is
outstanding, so it really is a peek until `commit()`, and clear the
pending state in `go_idle`.

## Failover and fencing

### 10. A runtime ack-policy change never reaches replicas while data flows

**High. Confirmed.** Breaks promise 7.

A replica learns its primary's policy from `StreamStart` and from
heartbeats only (`receiver_transport.rs:892-896`). The kernel-TCP sender
resets its heartbeat timer on every data send and sends a heartbeat only
when nothing was coalesced (`replication/tcp_sender.rs:912-973`); the DPDK
sender also needs an idle tick (reported). Journaled clock ticks are data,
so with the default tick cadence a heartbeat never fires, even on a
primary with no client traffic.

**Consequence.** After the documented partial-outage lever, `ACK-POLICY
disk` on the primary, connected replicas keep the old policy. The primary
then acks on its own fsync alone. If it dies, auto-promotion skips the
refusal that exists for exactly this policy (`raft_promotion.rs:258-272`,
which reads the replica's observed policy at `:322-331`), and acknowledged
events the winner never received are lost. The reverse also holds: after
the operator restores `disk+ram`, replicas keep believing `disk` and
auto-failover refuses indefinitely.

**Fix direction.** Send the policy whenever it changes, for example as a
dedicated frame on each swap or a policy byte in every `InputBatch`
header, not only on idle heartbeats.

### 12. A primary disk stall makes a truthful replica look divergent

**Medium. Confirmed.** Breaks promise 7.

The primary publishes each batch to the replication rings before handing
it to its disk thread (`pipeline.rs:1767-1771`).
`validate_replica_handshake_settled` (`validate.rs:125-141`) retries for
well under a second before a `BeyondTip` verdict becomes
`Divergent(AheadOfTip)`. Its comment names this exact transient and
assumes it "clears within a flush".

**Scenario.**
1. The primary's device stalls for longer than the replica's reconnect
   backoff plus that retry budget.
2. The replica has fsynced batches the primary has not written. As
   designed, its fsync satisfied the disk clause of `disk` or `disk+ram`
   acks.
3. The replica reconnects for any reason, is judged divergent, archives
   its journal and re-seeds from a snapshot below the acknowledged
   frontier. `melin_replica_divergence_total` increments.
4. If the primary then crashes, even as a plain process crash since the
   batches were never written, the acknowledged events survive only in
   the `.divergent` archive.

**Fix direction.** Compare the claim with the primary's sequenced tip
rather than its file tip. When the claim is at or below the sequenced
tip, wait for the disk to reach it, bounded by shutdown and disconnect.

### 13. Raft-mesh fencing ignores reply envelopes

**Medium. Confirmed.** Breaks promise 7.

`observe_peer_epoch` runs only on inbound frames at the RPC server
(`raft/src/rpc_server.rs:165-182`, called at `:300`). The client side
records each reply's tip in `PeerTips` and never checks its epoch
(`raft/src/network.rs:156-167`). A raft leader sends requests and
receives replies, and followers send it nothing between elections.

**Scenario.** An operator runs `PROMOTE` on a replica while the primary is
alive and leads raft, which is common: with primary-first bring-up the
primary wins the first election. The promoted node's replies carry the
new epoch on every heartbeat, and the old primary ignores them. With a
second replica still streaming from it, both primaries accept and
acknowledge writes indefinitely. `docs/replication.md` says the stale
primary stops "the moment [it] hears from any node that observed the
promotion", and that the raft mesh is an additional fencing channel.

**Fix direction.** Feed reply envelopes' epochs to the same supersession
policy.

### 14. A replica adopts its primary's epoch before it holds that epoch's entries

**Medium. Confirmed.** Breaks promise 7.

At `StreamStart` the replica calls `observe_epoch` with the primary's
current epoch (`tcp_receiver.rs:776`; the DPDK and resync paths do the
same, reported). Its raft tip is `(fence epoch, sequence)`, ordered epoch
first (`raft/src/recency.rs:56-73`, `:89-95`). The replica's shadow seeds
its snapshot epoch from the fence after that adoption
(`replication/mod.rs:474`).

**Scenario A, cascading failure.**
1. Primary A acks up to P through a fast replica B. A slower replica C
   holds Q, below P.
2. A dies. B wins term T and journals `EpochBump(T)`.
3. C is re-pointed at B, and its tip becomes (T, Q).
4. B dies during C's catch-up, while A, restarted as a replica, holds
   every acknowledged event at the older epoch.
5. C's vote filter and peer-tip veto rank A behind, so C promotes without
   Q+1 to P.

**Scenario B, snapshot epochs.** A snapshot taken before the bump is
journaled records the new epoch. A restart then recovers an epoch its
journal does not contain, so two nodes with identical journals can
recover different epochs.

**Fix direction.** Advertise, and stamp into snapshots, the highest epoch
actually journaled, as replayed `EpochBump`s give it, and keep the
observed epoch for fencing only. The roadmap's "Promoted-node journal
catch-up before serving" would also close scenario A.

### 15. Promotion can drop a rotation boundary

**Medium. Confirmed.** Breaks promise 3.

The promotion drain skips every frame that is not an `InputBatch`
(`receiver_transport.rs:547-551`, `:577`). A `Rotate` frame in the drained
bytes is dropped, and the entries after it are journaled into the
pre-boundary segment. Separately, teardown sets the stage flag right
after the sentinel, which can route the journal stage through
`drain_remaining`, and that drain ignores stream marks by design
(`pipeline.rs:1457-1467`). Its comment accepts the misframing because "the
reconnect handshake detects the framing mismatch", which does not hold
for a node being promoted.

**Consequence.** Chains are scoped to their segment, so every surviving
replica that adopted the boundary fails its handshake against the new
primary. It archives a correct journal as `.divergent.<n>` and resyncs in
full, and writes stall meanwhile under replica-requiring policies. The
old and new primaries' journals are no longer byte-identical.

**Fix direction.** Apply `Rotate` marks in the promotion drain, which runs
on a quiesced stream, and honour pending marks in the flag-path drain
when the node is being promoted.

## Application contract and clients

### 17. The tick contract is untested and has no working example

**Medium. Confirmed.** Breaks promise 1 for applications that get it
wrong. Overlaps the roadmap items "Monotonic sequencer time, derived from
the journal" (the runtime fix) and "Determinism test in the counter
example (snapshot vs replay)".

The dispatch watermark starts at zero in the matching stage
(`pipeline.rs:2487-2493`, `:2530`), in the shadow (`shadow.rs:70`) and in
replay (`journaled_app.rs:256`). Every journaled `Tick` calls
`Application::tick` twice (`dispatch.rs:57-70`). An application stays
deterministic only if a tick for a time already passed changes nothing,
which in practice means keeping its own clock in its snapshot.

**Coverage.**
- `TestApp::tick` counts every call (`test_support.rs:133-135`), so the
  runtime's own test application breaks the rule.
- The snapshot-recovery tests compare only `total`.
  `recover_from_snapshot_applies_post_snapshot_delta` builds a journal
  whose timestamps restart mid-stream, so full replay and snapshot
  recovery disagree on `ticks`, and the test does not look.
- `TestApp::apply` ignores `ApplyCtx::now_ns`, and every example's
  end-to-end test runs with snapshots and ticks off, so no test checks the
  time an application receives on the shadow path.
- None of echo, counter or notary implements `tick`.

**Guide.** `docs/building-an-application.md` does not name the routine
causes of repeated ticks: the double call, and a restart or restore inside
a batch that shares one timestamp. It does not say to keep the last
acted-on time in snapshotted state, and gives no example. The natural
"fire every due timer" pattern diverges. Suppose a node's watermark
stands at 100 when an event stamped 90, after a clock step or a failover,
schedules a timer at 95, and a snapshot is taken. The next event is
stamped 96. The running node does not tick for it, so the timer fires
later. A node restored from the snapshot starts from a zero watermark,
calls `tick(96)` and fires the timer before applying the event.

**Fix direction.** Land the monotonic-time plan. Meanwhile make `TestApp`
idempotent against a stored clock, restore `ticks` to the recovery
assertions, and add a tick-driven example.

### 18. A ServerBusy reply can overtake replies to earlier requests

**Medium. Confirmed.** Kernel TCP only.

When the input ring or the refusal queue is full, the reader drops the
frame and sends `PipelineBusy` on the control channel (`reader.rs`,
reported). The response stage appends the ServerBusy frame to the
connection's send buffer during its control drain
(`response.rs:689-701`), before its slot loop has produced replies to
requests published earlier on that connection. Frames behind the dropped
one stay in the parse buffer and are published on the next receive. Halt
refusals solved the same ordering problem (`halt.rs`), but ServerBusy was
left out. DPDK closes the connection instead.

**Scenario.** A pipelining client sends W1 then W2. W1 is published and
W2 dropped, and ServerBusy arrives before W1's reply. `melin-client`
documents ServerBusy as "nothing further will come for the request", so
the client pairs it with W1, drops the connection and retries W1, which is
then applied twice unless the application deduplicates.

**Fix direction.** Order ServerBusy through the same sequence-stamped
queue as halt refusals, or close the connection as DPDK does. The
roadmap's "Document the in-flight order contract on pipeline halt" should
cover the client side.

## Durability edges

### 19. Recovery trusts the page cache after a failed fsync

**Medium. Reported;** the kernel behaviour is well documented.

After a writeback error, Linux marks the failed pages clean, keeps their
contents and reports the error once. The disk thread poisons correctly
and the process exits. A supervisor restarts it without a reboot, and
recovery reads the never-persisted batch through the page cache as valid,
replays it and rebuilds the chain over it. `open_append`'s `sync_all` on a
new descriptor succeeds, and later acknowledged batches land after a
region that is not on disk. After page eviction or power loss that region
reads as zeros, and finding 6 truncates everything after it.

The segment preparer has the same shape: it drops the result of
`sync_file_range(WAIT_AFTER)` (`journal/src/preparer.rs:762-776`), which
consumes a writeback error, so the final `sync_all` succeeds on a segment
that is not durable.

**Fix direction.** At recovery, `sync_all` each segment, failing on an
I/O error, and drop its cached pages before reading, or read with
`O_DIRECT`. Alternatively refuse to start after a recorded journal poison
until the host reboots. Treat a `sync_file_range` error as a failed
prepare.

### 20. A failed directory fsync after rotation does not hold back acks

**Medium, filesystem-dependent. Confirmed** mechanism.

`SegmentFile::rotate` treats the post-rotation directory fsync as best
effort and retries it from the flush path (`segment_file.rs:287-302`),
while the disk thread publishes durability for the new segment
regardless. At startup `cleanup_staging_orphan` deletes
`<live>.next-staging` unconditionally (`preparer.rs:852-870`).

**Scenario** on ext4 with the default staging mode:
1. The directory fsync fails, for example on EMFILE opening the directory.
2. Appends continue. Each `fdatasync` covers data only, and acks go out.
3. Power fails before the filesystem's journal commit. The directory shows
   the pre-rotation layout, with the adopted segment, header and
   acknowledged entries included, still under the staging name.
4. Recovery resumes the old live segment, deletes the staging file and
   reissues the acknowledged sequence numbers.

**Fix direction.** Keep the directory descriptor open, retry the fsync
synchronously and poison on failure. Never delete a staging file that
carries a valid header.

### 21. The resync archive is not atomic, and a lone snapshot is ignored

**Medium. Confirmed.**

`archive_local_lineage` (`transport-core/src/replication/archive.rs:47-95`)
renames the archives, then the live segment, then the snapshot, one at a
time. An error aborts mid-move with no rollback, and only the journal's
parent directory is fsynced, as best effort. Replica recovery decides it
is fresh from journal files alone, so a snapshot left behind is never
archived and sits next to the new journal.

**Scenarios.** A crash between the live-segment rename and the snapshot
rename leaves an old-lineage snapshot beside a new journal. So does a
`--snapshot-path` on another filesystem, where the rename fails with EXDEV
every time; `docs/journal-rotation.md` describes a copy fallback that does
not exist (reported). With the hash chain on, recovery then fails closed
and the node is stuck until an operator removes the file. Without it,
recovery silently restores the old snapshot and replays the new lineage
on top. A crash in the middle of the list splits the old lineage across
two archive directories.

**Fix direction.** Write a durable intent marker that boot completes,
archive the snapshot on its own filesystem, and treat a lone snapshot as
an orphan to archive.

### 22. Snapshot paths are resolved three ways, and a served snapshot is barely checked

**Medium. Confirmed.**

- The shadow writes `--snapshot-path`, or `<journal>.snapshot`
  (`server.rs:572-576`).
- Primary boot reads `--snapshot`, or the derived path
  (`server.rs:3107-3116`).
- Snapshot transfer and its preflight hard-code the derived path
  (`replication/catchup.rs:375`, `:431`).

The primary serves a snapshot after parsing only its header, with no CRC
or `app_version` check (`catchup.rs:372-453`), and by then the verdict
frame has already made the replica archive its lineage.

**Consequences.**
- With `--snapshot-path` set, the primary never recovers from or serves
  its own snapshots, and may serve a stale derived file.
- An explicit `--snapshot` that does not exist, with no journal present,
  boots as `Fresh` from genesis.
- A corrupt or version-mismatched snapshot on the primary sends a
  divergent replica into an endless retry with its lineage already moved
  aside.

**Fix direction.** One resolver for every role, and validation as strict
as `snapshot::load` before the verdict is sent.

### 23. Without the hash chain, an ahead-of-tip replica is credited by the ack gate

**Medium, in `--no-default-features` builds. Confirmed** for validation,
**reported** for cursor seeding.

With `hash-chain` off, `validate_replica_handshake` returns `Ok` for every
handshake (`validate.rs:183-187`), although its own comment says
`AheadOfTip` is still enforced because "it is a sequence property, not a
chain one". The sender then seeds the slot's cursors at the replica's
claim and discards ring chunks at or below it.

**Scenario.** An ex-primary whose journal runs to 150, divergent from
101, rejoins a new primary whose tip is 100. It is credited with 101 to
150 of the new primary's history, which it never receives, so `ram`,
`disk+ram` and `two-disks` acks for those events are released with fewer
real copies than the policy demands. `docs/replication.md` says only that
a fork goes undetected in such builds.

**Fix direction.** Enforce the ahead-of-tip check from sequences alone in
every build.

## Lower-severity findings

24. **The shutdown drain skips a failed encode after allocating its
    sequence.** `pipeline.rs:1488-1508` logs and continues, so later
    entries are journaled after a hole, and finding 5 truncates them on
    restart. The matching stage has already applied the skipped event. The
    steady-state path fails closed on the same error. Confirmed.
    **Fixed:** the drain now fails closed on any failure, as the
    steady-state path does.
25. **A crash inside a snapshot save leaves no snapshot under its name.**
    `snapshot.rs:304-331` renames the old snapshot to `.prev` before
    publishing the new one, and boot never looks at `.prev`. Recovery
    falls back to a full replay, or to `MissingHistoryPrefix` when archives
    were pruned or on a snapshot-seeded replica. `docs/journal.md` says the
    previous snapshot "remains intact". Confirmed.
26. **The application codec contract is unenforced.** Confirmed.
    - Nothing rejects a journaled entry that decodes to a query: neither
      the journal decoder (`codec.rs:568-573`) nor the wire decoder
      (`replication_wire.rs:289-292`), only a `debug_assert!` in dispatch.
      A replica's journal stage skips such an entry as a query, leaving a
      gap that finding 5 then truncates at.
    - Replicas and replay depend on decode then re-encode being
      byte-identical, but only documentation says so. Overlaps the roadmap
      item "Record the application's event-encoding version in the
      journal".
    - `TestEvent::decode`, and the journal crate's test event, accept
      trailing bytes, which would hide a framing bug. The wire decoder
      accepts oversized `Tick` and `EpochBump` payloads.
27. **The DPDK response stage drops a connection without closing it.**
    When a heartbeat cannot be queued, `dpdk_response.rs` removes the
    connection from its reply map but leaves the socket open, so requests
    keep being applied and every reply is discarded. The client reads an
    applied write as refused. Reported.
28. **A half-open replica slot can count one replica twice.** A dead
    connection keeps its slot active with frozen acks while the same host
    reconnects on the other slot, so `two-disks` can be satisfied by one
    disk. Reported.
29. **Tightening the ack policy lags the in-flight batch.** The rest of a
    response batch keeps releasing against a position cached under the
    old, weaker policy. Reported.
30. **`no-persist` fakes durability without a signal.** The disk thread
    publishes durable cursors without writing, nothing warns at startup,
    and a `no-persist` replica's persisted acks are fake. No workspace
    crate enables it. Reported.
31. **Fence suppression is partial.** Only the final flush is skipped:
    mid-batch flushes keep shipping already-gated replies after the fence,
    and DPDK suppresses nothing. `docs/replication.md` says the node
    "immediately" stops acknowledging. Reported.
32. **Epoch uniqueness has holes.** Manual `PROMOTE` mints `epoch + 1`
    (`server.rs:1772`), so two manual promotions under observational raft
    collide, while `docs/replication.md` says that with the control plane
    enabled the limitation "does not apply" (confirmed). Auto-promotion can
    read a stale role with a new term, and a minted epoch can equal a later
    election's term (both plausible, narrow).
33. **Divergence repair can hand a forked journal to a pending
    promotion.** The in-process repair recovers the known-forked journal
    into the receiver's locals, a promotion requested meanwhile hands them
    to `run_as_primary`, and the advertised tip is not reset on that path.
    Reported.
34. **Two primaries can ack before the new epoch is visible.** A promoted
    node advertises its epoch only after the `EpochBump` is applied, which
    follows a full `clone_via_snapshot` and pipeline spawn; until then the
    old primary can keep acknowledging writes the new lineage never holds.
    Partly documented. Reported.
35. **Shadow snapshots can slip by an interval under load.** A misaligned
    save attempt is dropped silently and the amortized timer resets
    (`shadow.rs:139-158`), so under sustained load a snapshot can wait a
    whole interval. The snapshot is never inconsistent, only stale.
    Reported.
36. **The mark barrier aliases released ring slots, and `RingBuffer` is
    too loosely `Sync`.** The mid-batch barrier keeps a live slice over
    slots it has already released to the producer (`pipeline.rs:1321-1351`);
    under Stacked Borrows that is undefined behaviour once they are
    overwritten, although no released byte is read and nothing
    miscompiles. Miri skips these tests. `RingBuffer<T>` is `Sync` for any
    `T: Send`, while two consumers can hold `&T` to one slot; it should
    require `T: Sync`. Reported.
37. **Journal invariants are checked only in debug builds.** Backward or
    duplicate sequences (findings 1 and 2) and the writer/reader chain
    equality at recovery (`journaled_app.rs:411-415`) are confirmed. Also
    reported: `from_halves` with a non-empty batch, `resume` and
    `begin_segment` sequence agreement, and a format-15 header whose
    `sector_size` is not 4096. Each is cheap to enforce in release.
    **Sequence checks fixed:** a replica adopts only the next sequence,
    and the encoder refuses one at or below the last it wrote, in every
    build. The other checks listed here are unchanged.
38. **Filesystem edge cases in the journal.** A short `read` mid-segment on
    NFS or FUSE reads as end of data. No `flock` stops two processes
    recovering one journal. A crash inside `create_continuing` can leave a
    headerless live file that blocks boot, a case `install_prepared` avoids
    and `install_fresh`, snapshot-only boot and missing-live synthesis do
    not. Segment writes do not retry `EINTR`. Reported.
39. **Archive bookkeeping edge cases.** Archive numbering restarts at
    `.000001` after local archives are trimmed, and diverges from the
    primary's after a resync, although the docs promise matching numbering.
    Paths derived with `with_extension` let journals named `node.a` and
    `node.b` share `node.snapshot` and `node.raft`. `<snapshot>.prev` is not
    archived with its lineage. Reported.
40. **Races that cost a reconnect or a failed resync.** A rotation between
    reading the seed segment's header and its prefix, segment discovery
    that is not atomic against a rotation, and handshake validation that
    fails open when archive discovery errors. No data is lost. Reported.
41. **The notary accepts trailing bytes after its head query.** Its decoder
    maps `[0x11, anything]` to the head query. It is never journaled, but a
    malformed request is answered rather than refused. Reported.

## Availability defects found on the way

Not determinism, but each one defeats failover or stalls a node.

- **DPDK with an application publisher wedges the pipeline.** With a
  publisher supplied and `--event-bind` set, the DPDK path builds a second
  output consumer that nothing runs (`server.rs:2514-2520`, `:2679`,
  `:2941`). Ring consumers have no `Drop`, so it gates the output ring
  forever, and the matching stage stalls once the ring laps. Confirmed.
- **Nothing notices a primary that vanished silently.** The replica link
  has no keepalive, no `TCP_USER_TIMEOUT` and no heartbeat-silence check,
  so a primary that dies without FIN or RST (power loss, partition) leaves
  `primary_link_up` set, and auto-promotion refuses indefinitely.
  Reported, consistent with reading.
- **A read error after the handshake exits the replica.** The `?` on
  `read_frame` at `tcp_receiver.rs:742` ends the process instead of
  retrying, for example when the primary drops the connection after a
  failed snapshot preflight. Confirmed.
- **The DPDK receiver can hang after a disconnect** that leaves a partial
  frame buffered, and a connection in CloseWait counts as active.
  Reported.
- **Silent thread death and unbounded boot waits.** The accept loop does
  not watch the reader thread, so a panic in an application's decoder
  leaves a node that authenticates clients but never reads them while
  health stays green. The epoch-bump wait, the startup-event drain and the
  first-replica wait exit only on shutdown. Reported.
- **An adopted rotation that keeps failing can wedge promotion and
  shutdown** under a persistent ENOSPC or read-only filesystem. Reported.
- **A divergent replica with large segments may never get a verdict**
  within its handshake timeout, because the primary rescans the containing
  segment on each retry. Reported.
- **DPDK allows several authentication attempts on one connection**, where
  kernel TCP drops the connection on the first failure. Reported.
- **A stopped node's admin listener outlives it.** `admin::spawn` hands
  the bound listener to a thread the server never joins (`_admin_handle`
  in `server.rs`), and that thread polls the shutdown flag every 100 ms.
  A node restarted on the same admin address within the same process
  (tests, or an embedder calling `run_with_listener`) can fail to bind
  with "address in use" while the old thread lingers. A process restart
  is unaffected: exit closes the socket. Joining needs its own stop
  signal, since error exits do not set the shutdown flag. Found while
  fixing findings 1 and 2, where it made the reconnect test flaky under
  load. Confirmed.

## Documentation that no longer matches the code

- `docs/pipeline-architecture.md` describes a CAS-based multi-producer
  input ring with per-slot generation flags; the input ring is
  single-producer. It calls the output queue an SPSC; it is the
  multi-consumer disruptor. Its durability sentence contradicts finding 9.
- `docs/replication.md` says "a process crash loses nothing" under `disk`,
  `disk+ram` and `two-disks`, which ignores batches still in the journal's
  write ring. It says the ack policy "is advertised to replicas on the
  replication stream" (finding 10). Findings 13 and 31 contradict its
  fencing claims, and "queries keep answering" during a halt is the
  roadmap's "Answer queries on a halted node".
- `docs/journal.md` promises that a gap is an error (finding 5), that the
  previous snapshot remains intact (finding 25) and that a crash mid-write
  is handled (finding 16).
- `docs/journal-rotation.md` says a headerless live file is recreated
  (finding 38), describes an EXDEV copy fallback (finding 21) and promises
  matching archive numbering (finding 39).
- `docs/building-an-application.md` lacks the tick pattern (finding 17), a
  canonical and query-free decode requirement (finding 26), a warning that
  interior mutability defeats `query(&self)`, the debug-versus-release
  overflow split, and who receives reports produced by `tick`: the client
  whose event moved the clock forward.
- Code comments: `tick.rs` claims the journaled stream is strictly
  monotonic, but only ticks are clamped, against the previous tick, from
  zero at every boot. `dispatch.rs` and `app/src/lib.rs` still describe a
  producer race. `build_replica_pipeline`'s doc says replica journals are
  not byte-identical. `snapshot.rs` points at a `docs/operations.md` this
  repository does not have.

## Checked and found sound

Recorded so a later audit can start past them.

- **Dispatch.** Matching, shadow and replay call the same `dispatch` with
  the journaled timestamp and key. Queries are classified by one
  predicate everywhere and never change state. `ApplyCtx` carries only
  journaled fields. The shutdown sentinel carries timestamp 0 and never
  moves a clock.
- **Ingress.** The input ring has exactly one producer at every moment,
  and every producer writes every slot field. `key_hash` is derived one
  way on both transports and fixed per connection.
- **Sequencing.** The matching stage's wire-seq mirror matches the journal
  allocator's skip rule. Durable cursors are published only after
  `fdatasync`. Each `FsyncState` triple comes from one encoder state, and
  the mid-batch barrier publishes the prefix position.
- **Concurrency.** The ring, SPSC and seqlock orderings are sound, torn
  seqlock reads cannot pass the check, and the producer is gated so that it
  never overwrites a slot the journal is still reading.
- **Replica streaming.** Within a session, contiguity is enforced before
  publishing. The in-memory ack covers only committed slots, and stream
  marks are queued before any slot past them.
- **Recovery.** Snapshot load checks the CRC, `app_version`, unread payload
  and size cap. Chain cross-checks run at the snapshot anchor and at
  segment boundaries, and `MissingHistoryPrefix` and
  `SnapshotAnchorMissing` fire before anything is replayed or truncated.
  Archive discovery excludes dotted names.
- **Writing and rotation.** Rotation drains first, announces only on
  success and restores the live segment on failure. Short writes are
  resumed, and an fsync failure poisons with nothing published.
- **Resync.** The seed ends exactly at the snapshot sequence, the snapshot
  and seed come from a consistent point, and the old pipeline's threads are
  joined before anything is archived.
- **Epochs.** Epochs merge by maximum, the `EpochBump` is the first entry
  of a tenure, and a stale primary is refused before any data.
- **Examples.** Echo, counter and notary apply deterministically, snapshot
  completely, avoid hash-ordered iteration, clocks, randomness and floats,
  and the counter and echo codecs are strict.

## Reproductions

The tests live on branch `scratch/determinism-repro`, behind the opt-in
`determinism-repro` feature. Each description is enough to rebuild the
test as a regression test beside its fix. Each asserts the documented
behaviour, so it fails until the defect is fixed; the results were
observed on 2026-09-25 at `0fa03c79`. The replica pipeline tests and the
replica end-to-end scenario are now regression tests beside the fix for
findings 1 and 2.

**Journal recovery** (`transport-core` unit tests over `JournaledApp` and
`BufferedWriter`, `TestApp` events `Add(seq)`):

| Setup | Documented | Observed |
| --- | --- | --- |
| Live segment with entries 1, 2, 3, 5, 6 | recovery error | recovery succeeds; the file keeps 1 to 3 |
| Entries 1 to 10, entries 4 to 6 zeroed in place | recovery error | recovery succeeds; the file keeps 1 to 3 |
| Entries 1 to 5, bit 6 of entry 5's length flipped | recovery error | recovery succeeds without entry 5, which is deleted |
| Entries 1 to 5 plus each prefix of entry 6 (42 bytes) | every cut recovers 1 to 5 | refused at cuts 1, 39, 40 and 41 |

**Replica pipeline** (`transport-core` unit tests over
`build_replica_pipeline`, on a writer recovered at sequence 3):

| Setup | Documented | Observed |
| --- | --- | --- |
| Read the handshake pair from `chain_hash_lock` | (3, recovered chain) | (0, zero hash); the durable cursor correctly reads 3 |
| Publish sequences 1 to 3 again, as a re-anchored session would | refused | applied twice; release journal holds 1, 2, 3, 1, 2, 3 and fails on `SequenceDuplicate`; debug panics on the assertion |

**Replication ring** (`journal` unit test): publish two batches, call
`try_read`, skip `commit`, call `try_read` again. Documented: the same
batch. Observed: the second batch in release, a panic in debug.

**End to end** (`server-runtime` integration tests, counter application,
real sockets, ticks and snapshots as noted):

| Scenario | Documented | Observed |
| --- | --- | --- |
| Primary under `two-disks` plus one replica, increments 1, 2 and 4; restart the replica; restart the idle primary; `PROMOTE` and `ACK-POLICY disk` on the replica; read the counter | 7 | 14 in release, and the replica's journal fails on `SequenceDuplicate`; in debug the replica's journal thread panics on the assertion and the promoted node never answers |
| Standalone node with genesis increment 1,000,000 and snapshots every 100 ms; increment 5; wait for a snapshot covering it; stop; move the journal aside; boot | 1,000,005 | 2,000,005 |
| `--standalone` with the default ack policy (refused, journal file left behind); boot again with `disk` and the same genesis | 1,000,000 | 0 |

## Suggested order

1. Findings 1, 2 and 37 together: seed the replica's handshake state,
   anchor the gate at the accepted position, and make backward sequences a
   release-mode error. Small, and the highest risk. **Done**, with the
   reconnect waiting for the journal instead of anchoring the gate (see
   finding 2), and finding 24 fixed along the way.
2. Findings 3 and 4: genesis. Small.
3. Findings 5, 6, 7, 16 and 19: one recovery policy. Tolerate only a
   bounded, all-zero tail, preserve whatever is discarded, and cap entry
   length. (Finding 24, the shutdown drain, is already fixed.)
4. Findings 8 and 11: wire integrity and the DPDK sender.
5. Findings 10, 12, 13, 14 and 15: failover correctness.
6. Finding 9: an API decision, gating the publisher or documenting it.
7. The rest, then the documentation.

Each fix lands with its reproduction as a regression test.
