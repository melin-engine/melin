# Journal & Event Sourcing

This document describes the write-ahead journal, snapshot system, crash recovery, and version migration procedures.

## Design Principles

1. **Input-only journaling** — only input events are persisted: the application's events, and the few the runtime journals on its own behalf (clock ticks, fencing epochs). The reports an application produces are *not* journaled. The application is deterministic: replaying the same inputs always produces identical outputs. This keeps the journal small and avoids coupling its format to the application's reports.

2. **Persist-before-ack** — no response is sent to a client until the corresponding journal entry is durable on disk. The LMAX disruptor pipeline enforces this: the response stage gates on the journal cursor, which advances only after the batch's `fdatasync` completes.

3. **Manual binary codec** — no serde, no protobuf. Every field of the entry framing is encoded by hand in little-endian with known offsets. This gives predictable layout, zero allocations, and immunity to serialization library version changes. An application's event bytes inside an entry are its own encoding.

4. **CRC32C integrity** — every journal entry and every snapshot file is checksummed with CRC32C (hardware-accelerated on x86). Corruption is detected on read, never silently replayed.

5. **BLAKE3 hash chain, anchored per segment** — every journal segment carries a 32-byte chain **anchor** in its file header (random salt for a fresh journal, the previous segment's tail hash after rotation). The chain value after any entry is a pure function of the anchor and the raw on-disk bytes: `chain(S) = BLAKE3(entry bytes through S ‖ anchor)`. No chain metadata lives in the entry stream — sequence numbers are dense over real events, and a sealed segment can be verified with nothing but its own bytes and its successor's anchor.

## Journal File Format

### File Header (60 meaningful bytes, written once, padded to 4096)

```
Offset  Size  Field              Value
0       4     file_magic         0x4A4F5552 ("JOUR")
4       2     format_version     16
6       2     sector_size        4096
8       8     starting_sequence  sequence carried by this segment's first entry
16      32    anchor_hash        chain anchor (random salt or previous segment's tail hash)
48      8     genesis_entries    how many entries the history's genesis occupies (0: none)
56      4     header_crc         CRC32C of the preceding 56 bytes
```

The header is written when the segment is created and never modified. Its CRC protects the anchor — the root of all chain verification — and the genesis length against storage corruption.

`genesis_entries` belongs to the history, like the anchor: it is fixed when the journal is created with its genesis (see [Creating a Journal](#creating-a-journal)) and copied unchanged into every later segment, every replica's journal and every snapshot. It is what lets a node tell, from its own disk alone, whether its history holds the whole genesis. Recovery refuses a segment whose genesis length differs from the segment before it, like a segment whose anchor does not continue the chain: the segments come from different journals, or a header was edited.

`format_version` is checked on open. Version 16 is current; version 15 is also read — its header is the same without `genesis_entries`, and its entries are identical. A history begun by a release that wrote version 15 has no recorded genesis length, and keeps writing version-15 headers as it rotates and as replicas copy it, so its segments stay byte-for-byte identical across nodes. Any other version is refused (see Migration below).

### Entry Layout (repeats after the 4096-byte header reservation)

```
Offset  Size  Field           Description
0       2     entry_magic     0x4A45 — misalignment / corruption detection
2       2     length          byte count of (key_hash + event_tag + payload)
4       8     sequence        monotonically increasing, starts at 1, no gaps
12      8     timestamp_ns    wall-clock nanoseconds since Unix epoch (see Timestamps)
20      8     key_hash        hash of the client's signing key (0 for server-internal events)
28      1     event_tag       which kind of event (see below)
29      var   payload         the event's fields (see below)
29+len  4     crc32c          CRC32C of all preceding bytes in this entry
```

Total entry size: `20 + length + 4` bytes. The first entry's `sequence` must equal the header's `starting_sequence` — a segment renamed into the wrong place in the lineage is rejected at the first read.

### Event Payloads

| `event_tag` | Event | Payload |
|-------------|-------|---------|
| `0x03` | Tick | `now_ns` (u64) — the clock reading the runtime journals so the application's time-driven work replays identically |
| `0x04` | Epoch bump | `epoch` (u64) — the fencing epoch a newly promoted primary starts under (see [replication.md](replication.md)); never delivered to the application |
| `0x80` | Application event | the application's own encoding of one event, exactly as it wrote it |

The runtime never reads inside an application event's payload: its layout, and how it changes between versions of the application, are the application's (see [Changing the Application's Encoding](#changing-the-applications-encoding)). Tags `0x01` and `0x02` are retired and never written.

## Durability

### Write Path

Each batch is written with `pwrite` plus `fdatasync`, on the journal's disk thread — the sequencing thread hands the batch over and moves on, so encoding and the replica feed keep flowing while the device works. This is honest durability on any drive: `fdatasync` flushes the page cache to the drive and waits for the drive to acknowledge a flush of its own write cache. When it returns, every byte in the batch is in non-volatile storage regardless of whether the drive has power-loss protection — the kernel always issues a flush command (`REQ_OP_FLUSH`) to the device, and the device must acknowledge it before the syscall returns. On a drive with a volatile write cache the flush physically flushes the cache to media; on a PLP drive with the volatile write cache disabled (`VWC=0`) the flush is a near-no-op, because the device acknowledges writes only once the capacitor protects them.

Its cost is the device flush: short on a drive with power-loss protection, which acknowledges once its capacitors cover the write, and much longer on a consumer drive, which must flush its volatile cache first.

### Pre-allocation

On creation and when space runs low, the writer calls `posix_fallocate` to extend the file by 256 MiB. This pre-allocates disk extents (blocks) without writing zeros. Subsequent syncs only flush data pages — no extent metadata updates are needed, which would otherwise require a more expensive metadata sync.

### Batch Amortization

In the pipeline architecture, the journal stage reads a batch of events from the disruptor, encodes them all into a contiguous buffer, and hands it to the disk thread, which issues a single `pwrite` for the batch. Under load, one write covers many events. The disruptor naturally accumulates events while the previous write is in flight, providing implicit batching without any artificial delay.

Batches that queue up behind a slow device coalesce further: the disk thread writes every batch waiting for it and then issues **one** `fdatasync` covering all of them. A backlog built up during a stall therefore costs a single sync to clear, not one per batch.

An explicit group commit delay (`group_commit_delay`) can be configured but is set to zero for TCP. Testing showed that any delay hurts TCP throughput because it holds the journal cursor longer, stalling the response stage. It only helps with UDS transport where response sends are near-free.

## Crash Recovery

### What Can Go Wrong

1. **Clean shutdown** — all entries are complete and synced. No recovery needed.
2. **Crash mid-write** — the last entry may be partially written (truncated). The entry magic, length, or CRC will be invalid.
3. **Crash after write** — pre-allocated zero-filled space beyond the last write is always present; PLP ensures the written data itself is durable.
4. **Bit rot / storage corruption** — a previously valid entry has flipped bits. CRC32C detects this.

### Recovery Algorithm

```
recover(journal_path):
  1. For each archived segment in monotonic order, then the live segment:
     a. Validate the file header (magic + version + header CRC).
     b. Verify lineage: the header's anchor must equal the previous
        segment's tail chain hash, and the header's starting_sequence
        must continue the sequence space exactly. Checked BEFORE any
        replay — a foreign or tampered segment never reaches the application.
     c. Read entries sequentially:
        - Validate entry_magic (0x4A45), CRC32C, sequence continuity.
        - Absorb the entry's raw bytes into the segment hash chain.
        - Apply it to the application.
        - If entry_magic is 0x0000 → end of data (pre-allocated space). Stop.
        - If entry is truncated at EOF (live segment) → partial write
          from crash. Stop.
        - If CRC mismatch or sequence gap mid-archive → return error.
  2. Truncate the live file to valid_file_end (remove trailing garbage).
  3. Re-allocate space from valid_file_end forward.
  4. Reopen writer for appending. The writer rebuilds its chain state
     self-containedly: anchor from the header, hasher re-absorbed from
     the raw byte range — no chain state is handed over from the replay.
```

**Key behaviors:**

- A truncated final entry is treated as harmless (crash during write) and silently discarded. The events it contained were never acknowledged to the client (persist-before-ack), so no client believes they succeeded.
- Zero-filled bytes (from `posix_fallocate`) are treated as end-of-data, not corruption.
- A CRC mismatch or sequence gap mid-stream is treated as real corruption and returns an error. The operator must investigate — this should never happen under normal operation.

### Recovery with Snapshots

```
recover_from_snapshot(snapshot_path, journal_path):
  1. Load snapshot → (application state, snapshot_sequence, snapshot_chain_hash).
  2. Walk segments as above. Skip events with sequence <= snapshot_sequence
     (still validated and absorbed into the chain).
  3. At the snapshot's anchor sequence, verify the journal's chain hash at
     that point matches the snapshot's recorded chain hash. Mismatch aborts
     recovery before any post-snapshot events are replayed.
  4. Replay only entries after the snapshot.
  5. Truncate and reopen writer as above.
```

This avoids replaying the entire journal from genesis. Recovery time is proportional to the journal tail length (events since last snapshot), not total history.

The chain-hash cross-check at the anchor sequence ensures the snapshot and the journal share the same history: it rejects a snapshot paired with another cluster's journal, a divergent history, or a journal whose entries up to the anchor were tampered with. A snapshot anchored exactly at a rotation boundary is verified against the successor segment's header anchor (which *is* the chain value at that boundary) — so the check holds even when the segment holding the anchor entry has been moved to cold storage.

### Creating a Journal

On its first boot, a primary creates the journal with the application's genesis events (its initial reference data) already in it, and records in the journal header how many entries the genesis occupies. The journal is built under a temporary name next to the journal path (`<journal>.genesis-staging`), synced to disk, and only then renamed into place, so a journal at the configured path always begins with the complete genesis:

- A first boot that fails or crashes before the rename leaves no journal. The next boot is still a first boot: it discards the temporary file and journals the genesis whole.
- A first boot that fails after the rename (a port already in use, say) leaves a journal that already holds its genesis. The next boot recovers it and journals nothing more.

Genesis is journaled only when a node starts a new history: no journal, archive or snapshot on disk. A node that recovers a journal or restores a snapshot already has the genesis in its state and never journals it again, whatever genesis it is configured with. The one exception is a journal written by an earlier release that holds no entry at all (see [Journals written by earlier releases](#journals-written-by-earlier-releases)). A journal begun by this release records its genesis length from the start, so even with no entry it is never given a genesis afterwards: one begun with no genesis stays that way whatever genesis is configured later, and a replica's journal that received none of its primary's genesis is refused if started with a primary's flags, like any other partial copy (below).

Whether a history holds its whole genesis is judged against the genesis length recorded in the journal header when the history began — never against the genesis a node is configured with. A node's genesis configuration only matters on the boot that begins a history; changing it afterwards, on any node, changes nothing, and nodes of one cluster need not agree on it.

A primary booting from a history that holds fewer entries than its recorded genesis refuses to start, with an error naming both counts. A journal a primary creates always holds its whole genesis, so a shorter history is a replica's journal copied before it had the whole genesis and then started with a primary's flags; serving it would serve a state the application never configured. Start again from a new history — remove the journal, its archives and its snapshots, since a snapshot taken from the partial history holds the partial genesis too — or restart the node as a replica of a primary holding the whole history. A leftover `<journal>.genesis-staging` file is removed on every boot.

Configuration a primary cannot run under, such as `--standalone` with an ack policy other than `disk`, is refused before the journal directory is touched. A replica's configuration is checked the same way at boot, since it is also the configuration it serves under once promoted.

A replica learns the genesis length from its primary when it creates its journal, and records it in its own journal header, so it keeps it across restarts and still has it if the primary is lost. It copies the genesis itself like the rest of the history, entry by entry, so for a while after it first connects it holds only part of it. If the primary is lost before the replica has the whole genesis, the replica refuses promotion (`PROMOTE` or a raft election win) and exits with an error rather than serve a state the application never configured. Recover by restarting the original primary, whose journal holds the whole genesis; if it is gone, no node holds a complete history, and the cluster starts again from a new journal.

A replica whose journal records one genesis length refuses to follow a primary whose journal records another, and exits with an error naming both. Within one history the two always agree, so a difference means the journals do not share one history — a damaged or hand-edited header, or a bug. The replica never overwrites its own value: inspect both journals, then resync the replica from scratch (move its journal, archives and snapshots aside) or restore the primary.

#### Journals written by earlier releases

A journal written before the genesis length was recorded (format version 15) has no recorded length, and neither does any snapshot taken from it. Such a history boots, replicates and promotes exactly as it did under the release that wrote it, without the genesis checks above, and keeps format-version-15 headers as it rotates; no upgrade step is needed. The checks apply to histories begun by this release or later. The one exception is a version-15 journal holding no entry at all, which an earlier release could leave behind after a refused first boot (or a replica's copy of one, restarted with a primary's flags): nothing was ever served from it, so the next boot journals the node's genesis into it, keeping its chain anchor, records the length, and logs a warning. A replica that had copied such an empty journal from its primary takes the recorded length from the primary when it next connects, so the promotion check guards it too.

## Snapshots

### File Format

```
Offset  Size  Field              Value
0       4     file_magic         0x534E4150 ("SNAP")
4       2     transport_version  3 — this framing's version
6       2     app_version        the application's snapshot version at save time
8       8     sequence           journal sequence number at snapshot time
16      32    chain_hash         BLAKE3 hash chain state at that sequence
48      8     epoch              fencing epoch at snapshot time
56      8     genesis_entries    the history's genesis length, from the journal header
64      var   app_payload        the application's state, in its own encoding
EOF-4   4     crc32c             CRC32C of everything from offset 0 through EOF-4
```

`genesis_entries` lets a node that starts from a snapshot alone, with no journal segment left, record the genesis length in the journal it starts. A snapshot of a history with no recorded genesis length (see [Journals written by earlier releases](#journals-written-by-earlier-releases)) is written in the version-2 framing, which ends at `epoch`.

Maximum file size enforced on load: 256 MiB (prevents OOM from corrupt/malicious files).

### Snapshot Contents

The runtime owns the framing; the payload is whatever the application writes to capture its state, and the application alone reads it back. A snapshot restores to exactly the state the application had at `sequence`: nothing about the payload's layout is visible to, or checked by, the runtime beyond the CRC and `app_version`.

### Atomic Writes

Snapshots are written atomically:

1. Serialize entire state into an in-memory buffer.
2. Write to a temporary file.
3. `fdatasync` the temporary file.
4. `rename` the temporary file to the final path (atomic on POSIX).

A crash during snapshot creation leaves only a temporary file, which is harmless. The previous snapshot (if any) remains intact.

### When to Snapshot

Snapshots are written exclusively by the shadow stage on a configurable interval (`--snapshot-interval-ms`, default 50 minutes). The shadow runs as a dedicated consumer on the input ring, applies events to its own copy of the application, and writes a snapshot every interval — entirely off the thread that serves clients.

Journal segment rotation is independent of snapshots. When the live journal exceeds `--max-journal-mib` (default 256 MiB), the segment is archived and a fresh live file opens; no snapshot is written at rotation. Recovery walks the archive chain forward from the latest shadow snapshot.

- **On interval** — the shadow stage writes snapshots every `--snapshot-interval-ms`.
- **Before deploying a new application version** — version boundary (see Migration below).

## BLAKE3 Hash Chain

Every journal segment maintains a BLAKE3 hash chain for tamper evidence. The chain is **schedule-free**: its value at any point depends only on the segment's header anchor and the raw bytes written so far — never on how writes were batched or when intermediate values were computed.

### How It Works

1. **Anchor** — each segment's file header carries a 32-byte anchor: random salt for a fresh journal (so two independent journal lineages can never share a chain value), or the previous segment's tail hash after rotation.

2. **Chain definition** — `chain(S) = BLAKE3(raw bytes of entries 1..=S ‖ anchor)`, where "raw bytes" are the entries exactly as written on disk, CRC trailers included. An empty segment's chain value is its anchor. Because the definition is over the byte stream, the chain over a sealed segment can be recomputed by any tool that can hash a byte range — no journal-aware decoding required.

3. **Cost** — entries are absorbed into an incremental hasher, in memory only, on the journal stage, which runs parallel to matching. The 32-byte value is finalized on demand — at fsync boundaries (for snapshot coordination), at snapshot saves, and at rotation — never per entry.

4. **Rotation continuity** — the new segment's header anchor is the outgoing segment's tail chain hash. Recovery verifies this link *before* replaying each segment, so a tampered, missing, or foreign archive is rejected before any of its events reach the application.

5. **Snapshot integration** — snapshots store the chain hash at their anchor sequence; recovery cross-checks it against the journal (see Recovery with Snapshots above).

### What It Detects

- **Tampered entries** — even a CRC-consistent rewrite (payload altered, CRC recomputed) changes the segment's tail hash and breaks the link to the next segment's anchor or the snapshot cross-check. (Plain bit-flips are caught earlier, by per-entry CRC32C.)
- **Reordered, inserted, or removed entries** — any change to the byte stream changes the chain.
- **Snapshot/journal mismatch** — a snapshot paired with the wrong cluster's journal, a divergent history, or pre-anchor tampering is rejected before any state is restored.
- **Lineage breaks** — a missing archive between two surviving segments, or an archive from another deployment spliced into the directory.

### What It Does NOT Detect

- **Tamper in the live segment after the last snapshot anchor** — nothing has committed to those bytes yet. (An attacker with that level of access could equally truncate the tail, which is likewise undetectable in any design; sealing the segment — rotation — or the next snapshot closes the window.)
- **Truncation attacks** — removing entries from the end of the live segment produces a valid (shorter) chain. Sequence numbers detect this if the expected sequence is known externally.

### Turning It Off

The chain is on by default. A build with `--no-default-features` omits it and saves the per-entry hashing cost, giving up more than tamper evidence: the rotation-continuity check at recovery, the snapshot/journal cross-check, and the cross-node divergence detection built on top of them all go with it. That last one is the consequential loss in ordinary operation, with no attacker involved — it is what routes a rejoining ex-primary through resync instead of letting it be streamed to on top of a forked history (see [replication.md](replication.md)). Leave the chain on for any deployment that is replicated or carries an audit requirement.

## Journal Rotation

When the live segment exceeds the configured size threshold (`--max-journal-mib`, default 256 MiB), or an operator issues `ROTATE`, the journal stage rotates at the next fsync boundary — while the node keeps serving. (On replicas neither trigger applies: rotation is primary-driven, so a replica rotates exactly where its primary did — see [replication.md](replication.md).) The steps:

1. **Archive the live segment** by renaming it to the next monotonic slot (`melin.journal` → `melin.journal.000042`).
2. **Create a new live segment** at the original path. Its header records the continuing sequence number and an anchor equal to the old segment's tail chain hash. No snapshot is taken and **no sequence number is consumed** — the next event gets exactly the sequence it would have without the rotation.

Recovery walks archives in order, then the live segment. Old segments are kept for audit. See [Journal Rotation & Recovery](journal-rotation.md) for crash windows and operational guidance.

## Pipeline Architecture

The journal participates in a 3-stage LMAX disruptor pipeline:

```
            Input Disruptor (1M slots, lock-free ring buffer)
                    │
        ┌───────────┴───────────┐
        │                       │
   Journal Stage           Matching Stage
   (encode + sync)         (apply to the application)
   advances cursor ──┐     publishes to output ring
                     │           │
                     ▼           │
               Output Disruptor Ring (multi-consumer)
                     │
            ┌────────┴────────┐
            │                 │
      Response Stage    Event Publisher
      gates on cursor   (optional, --event-bind)
      sends to clients  broadcasts to subscribers
```

- **Journal and Matching run in parallel** on the same events. The matching stage — the thread that applies events to the application, `matching` in `--cores` — does not wait for the journal: it applies each event immediately. This overlaps the application's latency with journal I/O latency.
- **Response Stage gates on the journal cursor** — it will not send a response to the client until the journal stage has committed (synced) that event's sequence number. This enforces persist-before-ack without blocking the application.
- **Input ring capacity**: 1,048,576 slots. Each slot holds the application's widest event inline, so the ring's memory footprint follows how wide the application declares its events to be. At 10M events/sec, the ring provides ~100 ms of buffering.
- **Max journal batch**: 4,096 events per batch handed to the disk thread. Limits encoding time before hand-off, bounding worst-case latency. One `fdatasync` may cover several such batches when the device is behind.

### Feature Gates

| Feature | Effect |
|---------|--------|
| `no-persist` | Journal stage skips all I/O. Cursor advances immediately. For benchmarking the pipeline + network ceiling. |
| `pipeline-stats` | Prints per-stage busy/idle utilization percentages on shutdown. |
| `latency-trace` | Records per-event, per-stage latency in histograms (adds timestamp fields to slots). |

## Format Versioning

Both the journal and snapshot have independent `format_version` fields. Current journal version: **16**. Current snapshot framing version: **3**.

### Journal Version History

| Version | Change |
|---------|--------|
| 1-11 | Formats from before the runtime was separated from its first application, when the journal encoded that application's events itself |
| 12 | Application events carried as an opaque payload under their own tag, encoded by the application |
| 13 | Entry offset fixed at 4096 regardless of device sector size. Journals stay interchangeable across devices, and across the writer change that followed — the since-retired O_DIRECT writer produced this same layout |
| 14 | Chain metadata moved out of the entry stream: file header gained `starting_sequence`, `anchor_hash`, and a header CRC; `GenesisHash` and `Checkpoint` entry tags retired. The chain is anchored per segment and schedule-free; sequence numbers are dense over real events |
| 15 | Per-entry `request_seq` removed; an application that sequences requests carries the sequence in its own event payload |
| 16 | File header gained `genesis_entries`, the history's genesis length. Entries unchanged; version 15 is still read |

### Snapshot Framing History

The framing's `transport_version` changes only with the runtime; the application's payload is versioned separately, by `app_version`.

| Version | Change |
|---------|--------|
| 1 | Initial framing |
| 2 | Added the fencing `epoch` after `chain_hash` |
| 3 | Added `genesis_entries` after `epoch` |

### Compatibility Rules

- **Pre-production policy:** the journal reader accepts the current format version and version 15, whose entries are identical; a version-15 journal needs no migration (see [Journals written by earlier releases](#journals-written-by-earlier-releases)). Older versions are rejected with `UnsupportedVersion`; migrate via the snapshot-boundary procedure below.
- The snapshot reader accepts every framing version: a version-1 snapshot predates any promotion, so it loads with epoch 0; a version-1 or version-2 snapshot loads with no recorded genesis length.
- A snapshot loads only into the application version that wrote it: an `app_version` other than the running application's is refused before the application sees the payload.
- The journal records no application version. Entries written by one version of an application are decoded by whichever version replays them — see [Changing the Application's Encoding](#changing-the-applications-encoding).

## Migration Procedure

When changing the journal format (bumping `format_version`) or the application's snapshot layout (bumping its `app_version`):

### Standard Upgrade

1. **Take a snapshot** with the current (old) version. This captures the full application state at a known journal sequence.
2. **Deploy the new version.**
3. **Start fresh**: the new version creates a new journal file (new format) and loads the snapshot.
   - The application's genesis is not journaled again: the snapshot's state already includes it, and the new journal continues from the snapshot with no entry of its own. Its header takes the genesis length the snapshot recorded.
   - The restarted node continues a history rather than beginning one, so it serves clients immediately, as after any restart. With replication enabled and an acknowledgement policy that requires a replica, it refuses writes until a replica reconnects.
   - The snapshot must be one the new version accepts. If the application's snapshot layout changed, the new version refuses the old `app_version`, and a one-time migration tool must convert the snapshot.
   - When *only* the snapshot layout changed — the journal format and the application's event encoding did not — and the journal is retained from sequence 1, this procedure is not needed: move the old snapshot aside and start the new version on the existing journal, which it replays from the start to rebuild its state.
4. **Archive the old journal** for audit purposes. It can only be replayed by the old version.

### Upgrading a Replicated Deployment

The replication protocol does not negotiate versions: a format bump changes
both the on-disk journal and the replication frames, so mixed-version pairs
fail fast at the connection (a reconnect loop on the replica — not data
corruption). Upgrade the primary and all replicas together:

1. Follow the standard upgrade on the primary (snapshot → deploy → fresh
   journal from the snapshot).
2. Deploy the new version on every replica and start each with a clean
   journal directory, archiving its old files for audit alongside the
   primary's. Replicas re-bootstrap from the new primary automatically —
   via catch-up or snapshot transfer — and their new journals carry the
   primary's lineage identity.

Do not leave a replica on the old version expecting it to resume after the
primary upgrades: its journal cannot continue under the new format, and the
stream will not decode.

The move from journal version 15 to 16 is the exception: version 16 reads
version-15 journals and snapshots, so no snapshot boundary and no clean
journal directory are needed. Stop every node, deploy the new version
everywhere, and restart on the existing files. The replication protocol
version changed with it, so nodes on different versions still refuse to
replicate — see [replication.md](replication.md).

### Why This Works

- The snapshot captures the application's state completely — no journal replay needed for entries before the snapshot.
- The new journal starts from the snapshot sequence + 1, in the new format.
- Deterministic replay is preserved within each format version. Cross-version replay is not supported and not needed.

### What NOT to Do

- **Do not replay old-format journals with new-format code.** The reader will reject the version mismatch. Even if you bypassed the check, field layout differences would produce corrupt state.
- **Do not mix format versions in a single journal file.** Each file has one format version in its header.

### Changing the Application's Encoding

The journal's `format_version` covers the runtime's framing only. An application event's bytes are decoded by whichever version of the application replays them — on restart, on a replica catching up, after a failover — and nothing in the journal says which version wrote them. So:

- **Adding a new kind of event** is safe for replay: no existing entry carries it. An older version that meets one — a replica not yet upgraded, for instance — fails to decode the entry with `CorruptEntry` rather than guessing. Upgrade every node before a new event kind is written.
- **Changing an existing event's layout** — adding, removing, resizing or reordering a field — changes what entries already in the journal mean. Do it only across a snapshot boundary with the standard upgrade above, so that the new version never replays an entry the old one wrote. A decode that happens to succeed on the old bytes is the dangerous case: it replays a different history without an error.
- **Changing the snapshot layout** requires bumping the application's `app_version`, which the runtime checks on load.

## Operational Notes

### Watching the disk keep up

`melin_journal_disk_lag_batches` on `/metrics` is the number of batches handed to the journal's disk thread that are not yet durable.

- **0** in steady state: the device finishes each batch before the next one arrives.
- **Briefly non-zero** under load bursts or a slow flush: normal, and precisely what the split is for — the sequencer kept ordering, encoding, and feeding replicas through it.
- **Sustained, and climbing toward 64** (the hand-off depth): the device is not keeping up with the write rate. At the depth the sequencer stalls at its next batch, the input ring fills, and clients feel backpressure. Alert here, not on the metric merely being non-zero.

A sustained non-zero lag with a healthy device usually means the journal is sharing a device with something else, or `journal-disk` is unpinned and competing for its core.

Two related signals: `melin_journal_rotations_total{path="sync_fallback"}` climbing means segment staging is falling behind (see [Journal Rotation & Recovery](journal-rotation.md)), and under the `disk` ack policy with no replica attached (standalone), a stalled device shows up directly as client latency, because no other node's fsync can supply the copy the gate needs.

### Journal File Growth

The journal grows monotonically. Pre-allocation extends it in 256 MiB chunks. An entry takes 33 bytes of framing plus the event's own encoded bytes, so an application event of `n` bytes costs `33 + n` on disk, and the journal's write rate is that times the event rate. Queries are never journaled.

Journal rotation triggers live, at the fsync boundary after the segment exceeds `--max-journal-mib` (default 256 MiB), archiving the old segment — see [Journal Rotation & Recovery](journal-rotation.md).

### Sequence Numbers

Sequences are `u64`, starting at 1, monotonically increasing, with no gaps. At 10M events/sec, the sequence space lasts ~58,000 years before wrapping. Wrapping is not handled (nor needed).

### Timestamps

The `timestamp_ns` field is wall-clock time from `clock_gettime(CLOCK_REALTIME)`, read on the primary when the event enters the pipeline. The journal never uses it for ordering — sequence numbers do that — but it is the time the application is handed with the event, live and on every replay, so replay reproduces any decision the application based on it. If the system clock jumps (an NTP step), or a failover hands the primary role to a node whose clock is behind, consecutive timestamps may go backwards, and so may the time the application's time-driven work is advanced to. An application must not assume either only increases.

### Error Handling

| Error | Cause | Action |
|-------|-------|--------|
| `InvalidFile` | Bad magic bytes | Wrong file, not a journal |
| `UnsupportedVersion` | Format version mismatch | Need the version that wrote the journal (see Migration) |
| `CorruptEntry` | Unknown tag, or an event the application cannot decode | Storage corruption, or a journal written by an application version that encodes events differently (see Changing the Application's Encoding) — investigate before restarting |
| `ChecksumMismatch` | CRC32C validation failed (entry or file header) | Bit rot or partial write — investigate storage |
| `SequenceGap` | Non-contiguous sequence numbers, or a segment's first entry disagreeing with its header | Corruption, file truncation, or a misplaced segment — investigate |
| `SequenceDuplicate` | A sequence number repeated | Writer bug or storage anomaly — investigate |
| `TruncatedEntry` | Incomplete entry at EOF | Normal crash recovery — entry is discarded |
| `SegmentChainBreak` | A segment's header anchor does not equal the previous segment's tail chain hash | Tampered archive, missing segment, or foreign segment spliced in — investigate before trusting the history |
| `MissingHistoryPrefix` | The oldest surviving segment starts after the history start recovery requires (sequence 1, or the snapshot's anchor + 1) | Archives trimmed without a covering snapshot — restore the trimmed segments or a snapshot that covers them; recovery refuses to build partial state |
| `Io` | Underlying I/O error | Disk failure, permissions, full disk |

### Limitations

- **No output event log** — the application's reports are not persisted. Audit trail requires replaying the journal.
- **Single journal file** — no striping or parallel writes. The journal is single-threaded by design (LMAX architecture).
- **No encryption** — journal and snapshot files are plaintext binary. Whatever the application's events and state contain is visible to anyone with file access.
- **No verification without the chain** — a build with `--no-default-features` omits the hash chain, and with it every tamper and divergence check described above. See "Turning It Off".
