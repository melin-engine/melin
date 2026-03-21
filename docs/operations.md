# Operations Runbook

Production operations guide for the trading engine. Written for the person running the server at 3 AM.

---

## Table of Contents

1. [Server Startup](#server-startup)
2. [Recovery on Startup](#recovery-on-startup)
3. [Journal Management](#journal-management)
4. [Log Levels](#log-levels)
5. [CPU Tuning](#cpu-tuning)
6. [Monitoring](#monitoring)
7. [Emergency Procedures](#emergency-procedures)
8. [Crash Recovery Scenarios](#crash-recovery-scenarios)
9. [Disk Failure](#disk-failure)
10. [Capacity Planning](#capacity-planning)

---

## Server Startup

### Binary

```sh
cargo build --release
./target/release/melin-server [OPTIONS]
```

The server uses jemalloc by default (thread-local caches eliminate allocator lock contention).

### CLI Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--bind` | `127.0.0.1:9876` | TCP address to bind. Use `0.0.0.0:9876` for LAN access. |
| `--journal` | `melin.journal` | Path to the journal file. Use a dedicated NVMe for best latency. |
| `--snapshot` | (derived) | Path to the snapshot file. If omitted, defaults to `<journal>.snapshot` (e.g., `trading.snapshot`). |
| `--authorized-keys` | `authorized_keys` | Path to the Ed25519 authorized keys file. Every connection must authenticate before trading. |
| `--cores` | `1,2,3,6` | Pipeline core IDs: `journal,matching,response,repl-sender` (comma-separated). Core 0 should be reserved for OS/IRQ. |
| `--readers` | `2` | Number of epoll reader threads. Each multiplexes connections via epoll. |
| `--reader-cores` | `4` | First CPU core for reader threads. Reader thread `i` is pinned to core `reader_cores + i`. |
| `--max-journal-mib` | `256` | Maximum journal size in MiB before automatic rotation at startup. Set to `0` to disable. |
| `--group-commit-us` | `0` | Group commit coalescing delay in microseconds. Keep at `0` for TCP transport. Only useful with UDS (see CLAUDE.md). |
| `--accounts` | `2` | Number of test accounts to seed on first startup (fresh journal only). |
| `--instruments` | `2` | Number of test instruments to seed on first startup (fresh journal only). |
| `--heartbeat-interval-secs` | `10` | Seconds between heartbeats to idle connections. `0` to disable. |
| `--connection-timeout-secs` | `30` | Seconds before disconnecting silent clients. `0` to disable. |
| `--max-connections` | `1024` | Maximum concurrent authenticated connections. `0` for unlimited. Rejects new connections at the limit. |

### Startup Sequence

1. Load authorized keys from `--authorized-keys`.
2. Initialize or recover the exchange (see [Recovery on Startup](#recovery-on-startup)).
3. Pre-fault all exchange hash map pages (avoids page faults on the hot path).
4. Build the disruptor pipeline (input ring buffer + output SPSC queue).
5. Spawn reader thread pool (epoll-based, one thread per `--readers`).
6. Spawn 3-4 pipeline OS threads: journal, matching, response, and optionally repl-sender -- each pinned to its `--cores` value.
7. Set listener to non-blocking mode.
8. Enter accept loop, authenticating connections via Ed25519 challenge-response.

### Minimal Production Launch

```sh
./target/release/melin-server \
    --bind 0.0.0.0:9876 \
    --journal /mnt/nvme/melin.journal \
    --authorized-keys /etc/trading/authorized_keys \
    --cores 1,2,3,6 \
    --readers 2 \
    --reader-cores 4 \
    --max-journal-mib 512
```

---

## Recovery on Startup

The server automatically detects and handles all recovery scenarios. No manual intervention is needed for normal restarts.

### Decision Tree

The `init_engine` function checks the following conditions in order:

1. **Snapshot exists AND journal exists**: Recover from snapshot, then replay only journal entries after the snapshot's sequence number. This is the fast path -- avoids replaying the full history from genesis.

2. **Snapshot exists AND journal is missing**: This indicates a crash between journal archive (rename) and new journal creation during rotation. Loads the snapshot and creates a fresh journal continuing from the snapshot's sequence number. Logs: `recovering from snapshot only (journal missing, post-rotation crash?)`.

3. **Journal exists (no snapshot)**: Full replay from genesis. Every event in the journal is replayed to reconstruct exchange state.

4. **Neither exists**: Fresh start. Creates a new journal and seeds test data based on `--accounts` and `--instruments`.

### Post-Recovery Rotation Check

After recovery, if `--max-journal-mib` is set (default 256) and the journal exceeds that threshold, the server automatically:

1. Saves a snapshot at the current sequence boundary.
2. Archives the old journal (renames to `.1`, bumping existing archives).
3. Creates a fresh journal continuing the sequence numbering.

This prevents unbounded journal growth across restarts.

### Recovery Time

Recovery time is proportional to the number of journal entries replayed. With snapshots enabled (default), only entries since the last snapshot are replayed. At ~80 bytes per event:

- 256 MiB journal = ~3.2M events to replay
- With snapshot: only events since last rotation (typically seconds of traffic)

---

## Journal Management

### How Rotation Works

Rotation is triggered at startup when the journal exceeds `--max-journal-mib` (default: 256 MiB). The process:

1. **Save snapshot**: Writes the full exchange state (accounts, order books, instruments, circuit breakers, risk limits) to the snapshot file. Written atomically via `.tmp` + rename.
2. **Archive old journal**: Renames the current journal using a numeric suffix scheme:
   - `melin.journal` becomes `melin.journal.1`
   - Existing `.1` becomes `.2`, `.2` becomes `.3`, etc.
   - Renames happen in reverse order to avoid overwriting.
3. **Create new journal**: Opens a fresh journal file continuing the sequence numbering and BLAKE3 hash chain from where the old journal left off.

### Archive Naming

```
melin.journal      <-- current (active)
melin.journal.1    <-- previous rotation
melin.journal.2    <-- two rotations ago
melin.journal.3    <-- three rotations ago
...
```

The snapshot file is always overwritten on each rotation -- only the latest snapshot is kept. Archived journals are preserved indefinitely for audit purposes.

### Disk Space Planning

**Journal growth rate**: ~80 bytes per event (entry header + payload + CRC32C).

| Throughput | Per Hour | Per Day | Per Week |
|-----------|----------|---------|----------|
| 100K orders/sec | ~28 GiB | ~672 GiB | ~4.6 TiB |
| 1M orders/sec | ~280 GiB | ~6.7 TiB | ~47 TiB |
| 5M orders/sec | ~1.4 TiB | ~33 TiB | ~235 TiB |

The journal writer pre-allocates in 64 MiB chunks (`posix_fallocate`) to avoid filesystem metadata overhead during writes. The on-disk file size will be larger than the valid data by up to one chunk.

**Action items**:

- Set `--max-journal-mib` to trigger rotation before disk fills. The default of 256 MiB is conservative.
- Periodically archive or delete old `.journal.N` files. They are only needed for audit replay with the matching engine version that produced them.
- Monitor disk free space. If the journal disk fills, writes will fail and the server will log errors but continue running (see [Disk Failure](#disk-failure)).

---

## Log Levels

Log output uses `tracing` with the `RUST_LOG` environment variable. The conventions are strict:

### `error` -- Server bugs and I/O failures only

Must never fire due to bad client input or client network issues. If you see an `error` log, something is wrong with the server itself.

Examples:
- `journal encode error` -- failed to encode a journal entry
- `journal flush_batch_sync error` -- fsync failed (disk problem)
- `accept error` -- listener socket error

**Action**: Investigate immediately. These indicate hardware failure, bugs, or resource exhaustion.

### `info` -- Server lifecycle events

Normal operational events. Safe to monitor in production.

Examples:
- `loaded authorized keys` -- startup
- `recovering from snapshot + journal` -- recovery path taken
- `journal exceeds threshold, rotating` -- automatic rotation
- `listening` -- ready to accept connections
- `pinned to core` -- thread affinity applied
- `shutdown signal received` / `shutdown complete` -- orderly shutdown
- `seeded test data` -- first startup

### `debug` -- Client-caused events

High-volume in production. Enable only for debugging specific issues.

Examples:
- `new connection` -- client connected
- `authenticated` -- client passed auth
- `connection rejected: max_connections reached` -- at limit
- `auth failed, dropping` -- bad credentials
- `failed to set auth timeout` -- socket option issue

### Configuration

```sh
# Production: info level (default)
RUST_LOG=info ./target/release/melin-server ...

# Debugging client issues:
RUST_LOG=debug ./target/release/melin-server ...

# Debugging specific crate:
RUST_LOG=trading_server=debug,trading_engine=info ./target/release/melin-server ...
```

---

## CPU Tuning

### Core Layout

The recommended core assignment for a production server:

| Core(s) | Assignment | Flag |
|---------|-----------|------|
| 0 | OS, IRQs, RCU callbacks | (reserved, never assign pipeline work) |
| 1 | Journal stage | `--cores 1,...` |
| 2 | Matching stage | `--cores ...,2,...` |
| 3 | Response stage | `--cores ...,...,3,...` |
| 4-5 | Reader threads | `--readers 2 --reader-cores 4` |
| 6 | Replication sender | `--cores ...,...,...,6` |
| 7+ | Available for other work (benchmarks, monitoring) | -- |

### Core Pinning (`--cores`, `--readers`, `--reader-cores`)

Each pipeline thread calls `sched_setaffinity` to pin itself to the specified core. If pinning fails, a warning is logged but the server continues.

- `--cores 1,2,3,6` pins journal to core 1, matching to core 2, response to core 3, repl-sender to core 6.
- `--readers 2 --reader-cores 4` pins reader 0 to core 4, reader 1 to core 5.

### Kernel Boot Parameters (GRUB)

For lowest latency, configure kernel boot parameters. Edit `/etc/default/grub` and append to `GRUB_CMDLINE_LINUX_DEFAULT`:

```
isolcpus=nohz,domain,1-5 nohz_full=1-5 rcu_nocbs=1-5
```

Then apply:

```sh
sudo update-grub
sudo reboot
```

What each parameter does:

- **`isolcpus=nohz,domain,1-5`**: Removes cores 1-5 from the scheduler's load balancing and timer tick distribution. Only explicitly pinned threads run on these cores.
- **`nohz_full=1-5`**: Stops the timer tick on cores 1-5 when only one task is running. Eliminates ~1-10us jitter every 4ms (HZ=250).
- **`rcu_nocbs=1-5`**: Moves RCU callback processing off cores 1-5. Without this, RCU grace periods can still interrupt isolated cores.

Verify after reboot:

```sh
cat /sys/devices/system/cpu/isolated      # should print: 1-5
cat /sys/devices/system/cpu/nohz_full     # should print: 1-5
grep rcu_nocbs /proc/cmdline              # should show rcu_nocbs=1-5
```

To revert:

```sh
sudo cp /etc/default/grub.bak /etc/default/grub && sudo update-grub && sudo reboot
```

### Runtime Tuning (bench-isolate.sh)

The `scripts/bench-isolate.sh` script applies runtime tuning that does not require a reboot. It must run as root and automatically restores settings on exit:

1. **CPU governor**: Sets all cores to `performance` (locks max frequency, no scaling transitions).
2. **NMI watchdog**: Disables it (eliminates periodic non-maskable interrupts).
3. **IRQ affinity**: Pins all hardware interrupts to core 0.
4. **irqbalance**: Stops the daemon to prevent it from redistributing IRQs.

```sh
sudo ./scripts/bench-isolate.sh [bench args]
```

For production, apply these settings permanently:

```sh
# CPU governor (add to /etc/rc.local or systemd unit)
for gov in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
    echo performance > "$gov"
done

# NMI watchdog
echo 0 > /proc/sys/kernel/nmi_watchdog

# IRQ affinity (pin all IRQs to core 0)
for f in /proc/irq/*/smp_affinity; do
    echo 1 > "$f" 2>/dev/null
done

# Disable irqbalance
systemctl disable --now irqbalance
```

---

## Monitoring

### Admin Dashboard (QueryStats)

The admin TUI (`melin-admin`) connects to a running server and can send a `QueryStats` request. This returns a live snapshot of server state:

- **Active connections**: current authenticated client count
- **Events processed**: total events handled by the matching engine
- **Journal sequence**: current durable journal position

QueryStats is not journaled (no state change) and does not affect the hot path. It reads counters via relaxed atomics.

```sh
melin-admin <server-addr> <admin-key-file>
```

### Pipeline Stats Feature

Compile with the `pipeline-stats` feature to enable per-stage busy/idle counters:

```sh
cargo build --release --features pipeline-stats
```

This adds counters tracking how many iterations each stage spent busy vs. idle. Useful for identifying bottlenecks (e.g., response stage at 25% busy indicates TCP overhead).

### Latency Trace Feature

Compile with the `latency-trace` feature for per-stage HDR histograms:

```sh
cargo build --release --features latency-trace
```

This records timestamps at each pipeline stage transition and builds histograms for:
- **Wakeup latency**: time from publish to stage pickup
- **Batch encode time**: journal encoding duration
- **Execute time**: matching engine execution duration
- **End-to-end server latency**: wire-receive to wire-send

Histograms are reported on shutdown. The bench crate passes these features through:

```sh
cargo run --release -p melin-bench --features latency-trace,pipeline-stats
```

**Warning**: Latency trace adds overhead (~tens of nanoseconds per event for `rdtsc` calls). Do not enable in production unless actively diagnosing a latency issue.

---

## Emergency Procedures

### Kill Switch: Cancel All Orders for an Account

Use the admin tool to send `CancelAll` for a specific account. This cancels all resting orders across all instruments for that account. The command is journaled before execution.

```
melin-admin <server-addr> <admin-key-file>
# Select "Cancel All" from the menu
# Enter account ID
```

### Trading Halt: Circuit Breaker

Use the admin tool to set a circuit breaker with `halted=true` on a specific instrument. All new orders for that instrument will be rejected with `TradingHalted`. Existing resting orders remain on the book but will not match.

```
melin-admin <server-addr> <admin-key-file>
# Select "Set Circuit Breaker" from the menu
# Enter symbol, set halted = true
```

The halt persists across restarts (it is journaled and included in snapshots).

To resume trading, send another `SetCircuitBreaker` with `halted=false`.

### Halt All Instruments

There is no single "halt everything" command. You must send `SetCircuitBreaker` with `halted=true` for each instrument individually.

### Graceful Shutdown (SIGINT / SIGTERM)

Send `SIGINT` (Ctrl-C) or `SIGTERM` to the server process. The shutdown sequence:

1. Accept loop exits (non-blocking check on shutdown flag).
2. Reader threads stop -- no new events enter the disruptor.
3. Pipeline shutdown signal is set.
4. Journal stage drains remaining events from the ring buffer and flushes to disk.
5. Matching stage drains remaining events and publishes responses.
6. Response stage exits.
7. Server logs `shutdown complete` and exits with status 0.

**Second signal**: If you send SIGINT/SIGTERM again while shutdown is in progress, the server calls `_exit(1)` immediately (hard exit, no cleanup). Use this only if the graceful shutdown appears stuck.

**All events that entered the disruptor before shutdown will be journaled and responded to.** The ordered shutdown ensures no data loss.

---

## Crash Recovery Scenarios

### 1. Clean Shutdown

No action needed. The journal is fully synced. On next startup, the server recovers from the journal (or snapshot + journal) and resumes from where it left off.

### 2. Crash Mid-Write (Partial Entry)

The journal uses CRC32C checksums on every entry. If the server crashes during a write:

- The partially written entry will fail CRC validation on recovery.
- The `JournalReader` detects the truncated/corrupt entry and stops replaying at the last valid entry.
- The `JournalWriter` reopens the file for appending at the valid data boundary, effectively truncating the garbage.
- **One event may be lost** (the one being written at crash time). All prior events are intact.

This is handled automatically. No manual intervention required.

### 3. Crash During Rotation

Rotation has three steps: (1) save snapshot, (2) rename journal to `.1`, (3) create new journal. A crash between steps:

- **Crash after step 1 but before step 2**: Snapshot exists, original journal still in place. Normal recovery from snapshot + journal. The snapshot is redundant but harmless.

- **Crash after step 2 but before step 3**: Snapshot exists, old journal is archived as `.1`, but the new (active) journal does not exist. The server detects this case on startup: snapshot exists but journal is missing. It loads the snapshot and creates a fresh journal continuing from the snapshot's sequence number. Logs: `recovering from snapshot only (journal missing, post-rotation crash?)`.

- **Crash during snapshot write**: Snapshots are written atomically via `.tmp` file + rename. If the crash happens during the `.tmp` write, the rename never occurs and the old snapshot (if any) remains valid. If there is no prior snapshot, the server falls back to full journal replay.

### 4. Snapshot-Only Recovery (No Journal)

If only a snapshot file exists (journal deleted or on a different disk that failed), the server loads the snapshot and creates a fresh journal. State is restored to the point of the last snapshot. Events between the snapshot and the crash are lost.

### 5. Complete Data Loss

If both the journal and snapshot are gone, the server starts fresh with empty state and seeds test data per `--accounts`/`--instruments`.

---

## Disk Failure

### What Happens When Journal Writes Fail

The journal stage logs an `error` on write/sync failure:

```
journal encode error: ...
journal flush_batch_sync error: ...
```

The pipeline does **not** crash on journal I/O errors. The journal stage logs the error and continues processing. However:

- Events that failed to persist will NOT have their responses gated by the journal cursor (the cursor does not advance past them).
- Depending on the failure mode, the response stage may stall waiting for the journal cursor to advance, causing client timeouts.

**This is a critical situation.** The persist-before-ack guarantee is broken if journal writes fail silently.

### Detection

1. **Monitor for `error` level log messages.** Any `error` log in production indicates a server-level problem. Journal I/O failures will appear as `journal flush_batch_sync error` or `journal encode error`.
2. **Monitor journal file growth.** If the journal stops growing while the server is receiving traffic, writes are failing.
3. **Monitor disk health.** Use `smartctl`, NVMe health counters, and filesystem error counts.

### When to Intervene

- **Single transient error** (e.g., momentary disk stall): The server self-recovers on the next successful write. Monitor closely.
- **Repeated errors**: The journal disk is failing. **Stop the server immediately** (SIGINT). Investigate the disk. Replace if necessary. Restore the journal and snapshot to a healthy disk and restart.
- **Disk full**: Clear space (delete old `.journal.N` archives) or increase `--max-journal-mib` to trigger more frequent rotation. Restart the server.

### NVMe-Specific Considerations

For best journal performance, use an NVMe drive with:

- **Power Loss Protection (PLP)**: Ensures FUA writes are truly durable. Without PLP, the drive's write cache may lie about durability.
- **Dedicated journal disk**: Avoids contention with OS I/O. The journal writer uses `pwritev2` with `RWF_DSYNC` (FUA) which bypasses the page cache, but sharing the disk with other workloads increases p99 latency.

---

## Capacity Planning

### Journal Size

Each journal entry is approximately **80 bytes** (20-byte header + variable payload + 4-byte CRC32C). The exact size depends on the event type:

- Limit order submit: ~65-80 bytes
- Cancel: ~30-40 bytes
- Deposit: ~30-40 bytes
- Hash chain checkpoints: ~77 bytes, emitted every 100K events

The journal writer pre-allocates in **64 MiB chunks**. The on-disk file size jumps in 64 MiB increments.

### Snapshot Size

Snapshot size depends on the number of accounts, instruments, and resting orders:

- Base: ~50 bytes (header + sequence + chain hash + CRC)
- Per account: ~16 bytes per currency balance
- Per instrument: ~100 bytes (spec + circuit breaker + risk limits + fee schedule)
- Per resting order: ~40 bytes

A server with 10K accounts, 100 instruments, and 50K resting orders uses approximately:
- 10K accounts * 200 currencies * 16 bytes = ~32 MiB
- 50K orders * 40 bytes = ~2 MiB
- Total: ~34 MiB

### Ring Buffer Memory

The input and output ring buffers are allocated at startup:

| Buffer | Capacity | Slot Size | Memory |
|--------|----------|-----------|--------|
| Input disruptor | 2^20 = 1,048,576 slots | ~72 bytes | ~72 MiB |
| Output SPSC | 2^20 = 1,048,576 slots | ~varies | ~72 MiB |

Total ring buffer memory: approximately **144 MiB**. This is fixed regardless of throughput.

### Total Memory Budget

| Component | Estimate |
|-----------|----------|
| Ring buffers | ~144 MiB |
| Exchange state (order books, accounts) | 10-500 MiB (depends on active orders) |
| Journal pre-allocation | 64 MiB chunk |
| Connection state | ~4 KiB per connection |
| jemalloc overhead | ~10-50 MiB |
| **Total (typical)** | **300-800 MiB** |

### Throughput vs. Disk Bandwidth

At 5M orders/sec with ~80 bytes/event, the journal writes **~400 MB/s** sustained. Ensure the journal disk can sustain this write rate with FUA/dsync. Modern NVMe drives typically support 1-3 GB/s sequential write with FUA.

At 10M orders/sec (engine-only rate), you would need ~800 MB/s sustained write bandwidth. In practice, the TCP network stack is the bottleneck before journal bandwidth becomes limiting.
