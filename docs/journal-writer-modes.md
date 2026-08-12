# Journal Writer Modes

The server's `--journal-writer` flag selects how the journal stage writes batches to disk. Two modes are supported:

- **`buffered`** *(default, recommended for production)* — page-cache writes plus `fdatasync` per batch. Honest durability on any drive.
- **`sector`** *(experimental)* — `O_DIRECT` sector-aligned writes, no `fdatasync`. Lower latency on enterprise NVMe with capacitor-backed power-loss protection (PLP). **Not durable on drives with a volatile write cache.**

This document explains how to choose between them, how to verify your drives support `sector`, and how to migrate from one to the other.

---

## Status

**`buffered` is the production-ready writer.** Default, recommended, and what every Melin deployment should run unless one of the two conditions below applies.

**`sector` is experimental** and is shipped for two purposes:

1. Benchmarking the latency floor on PLP-equipped hardware (the lowest-latency path the server can currently take, given the durability assumption holds).
2. Investigating an open issue: under sustained load on some NVMe firmware, `sector` mode exhibits ~1 Hz tail-latency spikes that do not appear under `buffered`. Root cause is not yet identified; suspects include drive-internal housekeeping, jbd2 commits triggered by unrelated metadata, and userspace agents holding the I/O scheduler. Until the spike source is understood and either eliminated or characterised, `sector` should not be relied upon in production even on drives where its durability assumption holds.

Operators evaluating Melin for production should treat the `--journal-writer` flag as effectively single-valued (`buffered`) and ignore `sector`. The flag exists so the experimental work can continue without forking the binary.

---

## Quick decision

Use `buffered`.

The only reasons to pick `sector` are:

- You're running a controlled latency benchmark on PLP-equipped enterprise NVMe and have separately verified that the spike issue above does not manifest on your hardware/firmware combination.
- You're contributing to the investigation of the open issue.

The default is `buffered` both because incorrect use of `sector` silently corrupts the journal on power loss (an unrecoverable failure an operator may not detect until the next outage) **and** because `sector`'s tail-latency behaviour is not yet trustworthy. Buffered mode pays a small overhead in exchange for failure-mode safety and predictable latency.

---

## How each mode achieves durability

### Buffered mode

For each fsync batch:

1. `pwrite` the batch bytes to the journal file (writes hit the kernel page cache).
2. `fdatasync` the file (kernel flushes dirty pages to the drive, drive flushes its own write cache to media, kernel returns).
3. Acknowledge the batch to the response stage.

The `fdatasync` syscall is the durability boundary. When it returns, every byte in the batch is in non-volatile storage regardless of whether the drive has PLP — the kernel always issues a flush command (`REQ_OP_FLUSH`) to the device as part of `fdatasync`, and the device must acknowledge it before the syscall returns. On a drive with a volatile write cache, the flush command physically flushes the cache to media. On a PLP drive with the volatile write cache disabled (`VWC=0`), the flush is a near-no-op because the device acknowledges writes only after they're protected by the capacitor.

### Sector mode

For each fsync batch:

1. `pwrite` the sector-aligned batch bytes via `O_DIRECT` (writes bypass the page cache, go straight to the drive).
2. The drive accepts the bytes into its write cache and returns success.
3. Acknowledge the batch to the response stage.

There is no `fdatasync`. Durability depends entirely on the drive: a PLP drive's capacitors protect the write cache on power loss; a non-PLP drive loses the cache and the acknowledged batch with it. Sector mode does *not* issue a flush command — that's the source of its latency win on PLP drives, and the source of its data-loss risk on non-PLP drives.

---

## When `sector` is safe

`sector` mode is safe if and only if both conditions hold for **every drive in the journal path** (including drives behind any RAID/LVM layer):

1. **The drive has capacitor-backed power-loss protection.** This is sometimes called "enhanced power loss protection" or "PLP" in vendor materials. It means the drive has on-board capacitors sized to flush the entire write cache to NAND on a sudden power loss.
2. **The drive's volatile write cache is disabled** (`VWC=0`), or equivalently, the drive advertises that acknowledged writes are persistent.

Both conditions are needed. A drive with PLP capacitors but `VWC=1` is reporting writes as durable before the capacitor protection kicks in, which is not what `sector` mode assumes. (In practice, drives that ship with PLP tend to ship with `VWC=0`, but this is not universal.)

### Verifying PLP on Linux

```
nvme id-ctrl /dev/nvme0 | grep vwc
```

- `vwc      : 0` — volatile write cache is disabled. The drive only acknowledges writes when they're durable. Pairs with PLP-equipped drives. **`sector` is safe.**
- `vwc      : 1` — volatile write cache is enabled. The drive acknowledges writes as soon as they hit the cache, before they reach media. **`sector` is NOT safe.** Even if the drive has PLP capacitors, the volatile-write-cache bit means it's not committed to durability-before-ack semantics.

Some additional commands worth running:

```
# Check the controller's advertised power-loss capabilities. PLP drives
# often advertise an "Enterprise" or "PLP" feature tag. The exact field
# depends on vendor; check the vendor data sheet.
nvme id-ctrl /dev/nvme0 | grep -iE "vwc|power|plp"

# Drive model — cross-reference with the vendor's PLP-equipped SKU list.
nvme list
```

### Drives that typically advertise PLP

- Micron 7450 PRO / MAX
- Micron 7500 PRO / MAX
- Solidigm D5-P5430, D7-P5520, D7-P5620
- Samsung PM893, PM9A3, PM9A1a (enterprise SKUs only — consumer 980 / 990 PRO do **not**)
- Kioxia KCD6, CD8

Consumer NVMe (Samsung 980/990, WD Black SN850, Crucial P5 Plus, etc.) typically do *not* have PLP. Always verify with `vwc` and the vendor data sheet — model-name pattern-matching is not authoritative.

---

## Performance characteristics

Numbers are illustrative; measure on your hardware before sizing.

### Throughput

Both writers can sustain >2M orders/sec on the embedded pipeline bench on a modern enterprise NVMe. The difference is in the 99.9p latency tail, not steady-state throughput.

### Per-batch latency

On a PLP drive (`VWC=0`):
- **`sector`**: ~5-15 µs per batch, dominated by drive write-cache acknowledgement.
- **`buffered`**: ~10-30 µs per batch. The `fdatasync` adds a syscall round-trip plus a flush command that the device acknowledges almost immediately (capacitors are already protecting the cache).

On a non-PLP drive (`VWC=1`):
- **`sector`**: ~5-15 µs per batch (same as PLP). **But acknowledged batches can be lost on power failure.** Do not use.
- **`buffered`**: ~50-200 µs per batch. The `fdatasync` triggers a real device flush, which on consumer NVMe is the dominant cost.

### Tail latency

Sector mode under sustained load can show ~1 Hz spikes on some NVMe firmware (driven by drive-internal housekeeping, not the writer). Buffered mode tends to be more uniform because the page cache absorbs short bursts of write pressure.

### Segment rotation cost

`sector` mode pre-stages the next segment off the hot path using a background preparer thread whenever rotation recurs — size-driven rotation, or a replica following the primary's announced boundaries. Those rotations cost ~microseconds in the journal stage's critical path. Manual-only configurations (`--max-journal-mib 0`, rotating exclusively via `ROTATE`) skip pre-staging and pay a synchronous allocate of tens of milliseconds per rotation; raise `--max-journal-mib` if fast manual rotation matters. `melin_journal_rotations_total{path=...}` on `/metrics` shows which path rotations are taking.

`buffered` mode pre-stages recurring rotations the same way, with one addition: the staged segment is pre-written with real zeros rather than merely space-allocated. This serves two purposes. First, rotation stays off the critical path, same as `sector` mode. Second — and this matters for tail latency between rotations — appending into merely-allocated space forces a filesystem metadata commit inside `fdatasync` at unpredictable intervals on the filesystems that track unwritten regions (XFS, ext4); under sustained load this manifests as a periodic multi-millisecond stall of the whole order pipeline (measured at ~2 ms every ~10 s on XFS). Appending into pre-written space never touches filesystem metadata, so the per-batch `fdatasync` stays on its fastest kernel path. The trade-off: every journal segment is written twice (zeros at staging time, data at append time), roughly doubling the journal device's sequential write volume — size the device's endurance budget accordingly. Manual-only configurations (`--max-journal-mib 0`) skip pre-staging as in `sector` mode: each manual rotation pays a synchronous allocate, and the new segment loses the pre-written property until recurring rotation is re-enabled.

---

## Migration

### `buffered` → `sector`

Conditions: every drive in the journal path is PLP-equipped with `VWC=0` (see verification above).

Procedure:

1. Verify PLP on every drive.
2. Stop the server cleanly (so the journal has no pending writes).
3. Restart with `--journal-writer sector`.

**Caveat — 4Kn drives:** if your drives have a 4096-byte native sector size (`nvme id-ns /dev/nvme0n1 | grep LBAF` — look for `lbads:12` meaning 2^12 = 4096), a journal originally created with `--journal-writer buffered` may fail to open under `sector` mode. The buffered writer records a 512-byte sector size in the file header; the sector writer rejects journals whose header sector size is smaller than the device's native sector size.

Workaround:

1. Rotate the journal under buffered mode first (sends a `ROTATE` admin command or restarts the server with the rotation threshold lowered). This archives the existing journal and would normally create a new one — but instead, immediately stop the server.
2. Delete or move the empty/new live segment.
3. Start the server with `--journal-writer sector`. It creates a fresh live segment with the correct (device-native) sector size in the header.
4. The archived journal still replays correctly on subsequent recoveries; only the live segment's header had to match the new mode.

### `sector` → `buffered`

Always safe; no migration steps needed. Stop the server, restart with `--journal-writer buffered`.

This is a reasonable response if you've discovered that one or more drives in the journal path don't actually have PLP. Switch to `buffered`, run the post-incident replay to verify no acknowledged batches were lost, and only then consider whether to retire the affected drives.

---

## Diagnostic indicators

These show up in the server's startup log:

```
INFO journal writer_mode=buffered "creating new journal"
INFO journal writer_mode=sector "recovering from journal"
```

The journal stage's runtime path is determined by the variant:

- `buffered` mode runs the synchronous write loop (pwrite + fdatasync).
- `sector` mode runs the io_uring overlapped loop (async pwrite via `io_uring`, no fdatasync).

If you're auditing whether durability is being enforced correctly, grep for `flush_batch_sync` in `crates/core/journal/src/buffered_writer.rs` (buffered) and `confirm_async_write` in `crates/core/journal/src/sector_writer.rs` (sector).
