# Pipeline Architecture

This document describes the LMAX-style disruptor pipeline that forms the core of the trading engine's I/O and execution model.

## Overview

The server uses a 3-stage pipeline plus a reader pool, modeled after the LMAX Disruptor architecture:

```
                        +-------------------+
  Reader Pool --------->| Input Disruptor   |-----+----> Journal Stage
  (N epoll threads,     | (1M-slot ring,    |     |
   MultiProducer CAS)   |  lock-free CAS)   |     +----> Matching Stage
                        +-------------------+              |
                                                           | SPSC
                                                           v
                                              +----> Response Stage ----> Clients
```

1. **Reader pool** -- N epoll-based threads (default 2) multiplex all client connections and publish decoded requests into the input disruptor via lock-free CAS (`MultiProducer`).
2. **Journal stage** -- batch-encodes events and writes them durably to disk via `pwritev2` + `RWF_DSYNC` (FUA). Advances its cursor only after the durable write completes.
3. **Matching stage** -- executes commands against the `Exchange` engine and publishes execution reports to an output SPSC queue. Runs in parallel with the journal stage (does not wait for fsync).
4. **Response stage** -- consumes the output SPSC but gates on the journal cursor before sending responses to clients, enforcing the persist-before-ack invariant.

**Why this design**: Single-threaded business logic (the matching stage) eliminates locks on the hot path. Parallelizing journal I/O with matching hides fsync latency. The persist-before-ack boundary is enforced at the response stage, not in the matching stage, so the engine never stalls waiting for disk.

## Input Disruptor

The input disruptor is a multi-producer, multi-consumer ring buffer defined in `crates/disruptor/src/ring.rs`.

**Capacity**: `INPUT_RING_CAPACITY = 1 << 20` (1,048,576 slots). At approximately 72 bytes per `InputSlot`, this is roughly 72 MiB -- sized to fit in L3 cache on modern server CPUs. Provides approximately 100 ms of buffering at 10M orders/sec, enough headroom for fsync stalls without backpressure reaching the readers.

**Publishing**: Reader threads publish via `MultiProducer`, which uses CAS-based slot claiming (the LMAX multi-producer pattern):

1. `fetch_add` on the cursor atomically claims a unique sequence number.
2. The value is written to the claimed slot.
3. A per-slot generation flag (stored in a parallel `AtomicI32` array) is set so consumers know the slot is ready.

The `MultiProducer` is `Clone + Send + Sync` -- each reader thread holds its own clone. No mutex is needed. The per-slot generation array costs approximately 4 MiB (1M slots x 4 bytes).

**Consumers**: Two consumers are registered in parallel, both gated on the producer only (not on each other):

- **Consumer 0**: Journal stage
- **Consumer 1**: Matching stage

Because both consumers are gated only on the producer, they can process events concurrently. The journal stage does not block the matching stage, and vice versa. Backpressure is applied by the producer checking the minimum progress of all terminal consumers (both journal and matching) before claiming new slots.

**`InputSlot` layout** (~72 bytes):

| Field | Description |
|-------|-------------|
| `connection_id: u64` | Originating client connection |
| `event: JournalEvent` | The command (order submit, cancel, deposit, etc.) |
| `publish_ts: TraceTimestamp` | Disruptor publish timestamp (zero-sized when `latency-trace` disabled) |
| `recv_ts: TraceTimestamp` | Wire receive timestamp (zero-sized when `latency-trace` disabled) |

## Journal Stage

Defined in `crates/engine/src/journal/pipeline.rs` as `JournalStage`.

The journal stage runs on a dedicated OS thread and is responsible for making every event durable before it can be acknowledged to clients. It uses `read_batch` + `commit` (not `consume_batch`) to decouple reading from cursor advancement -- the cursor is advanced only **after** the durable write completes.

### Processing loop

1. Call `consumer.read_batch()` to copy up to `MAX_JOURNAL_BATCH` (1,024) events from the ring buffer into a local array. This does not advance the consumer's progress cursor.
2. Batch-encode all events into the `JournalWriter`'s internal buffer. `QueryStats` events are skipped (they cause no state change and are not journaled).
3. When a sync trigger fires, call `flush_batch_sync()` which issues a single `pwritev2` with `RWF_DSYNC` (Force Unit Access) -- combining write + sync into one syscall. On NVMe with power-loss protection, this achieves approximately 10-100 us sync latency instead of the approximately 1-7 ms of full cache flushes via `fdatasync`.
4. Call `consumer.commit(pending)` to advance the progress cursor, making the journal's position visible to the response stage.

### Sync triggers

A batch is synced when any of:
- The batch reaches `MAX_JOURNAL_BATCH` (1,024 events)
- The group commit delay has elapsed since the first unsynced write
- Group commit delay is zero (sync immediately after each read batch)

### Group commit delay

The `group_commit_delay` parameter (configurable via `--group-commit-us`) allows the journal to wait up to a specified duration for more events to accumulate before issuing the durable write. Under high load, the batch fills naturally and the delay rarely fires.

**Important**: Group commit helps throughput only with UDS transport (+34% at 100 us). With TCP transport, it hurts throughput because the delay holds the journal cursor longer, making the response stage block and accumulate larger TCP send buffers. **Keep at 0 for TCP** (the default).

### Idle behavior

When no events are available, the journal stage uses adaptive spinning: 1,000 `spin_loop()` iterations (approximately 1 us), then falls back to `thread::yield_now()` to avoid aggressive OS preemption.

### Shutdown

On shutdown, the journal stage flushes any pending data, then drains all remaining entries from the ring buffer (encoding and syncing each batch), ensuring no event is lost.

## Matching Stage

Defined in `crates/engine/src/journal/pipeline.rs` as `MatchingStage`.

The matching stage runs on a dedicated OS thread and is the only thread that mutates the `Exchange` state. This single-writer design eliminates all locks on the hot path.

### Processing loop

1. Call `consumer.try_consume()` to read one event at a time (single-entry consumption, not batched).
2. Execute the event against the `Exchange`, producing execution reports into a pre-allocated `Vec<ExecutionReport>` (capacity 256, reused across commands).
3. Publish each execution report as an `OutputSlot` to the output SPSC, followed by a `BatchEnd` marker signaling the end of reports for this request.

### QueryStats handling

`QueryStats` is handled inline in the matching stage without touching the `Exchange`. It reads:
- A thread-local events counter (plain `u64`, flushed to a shared `Arc<AtomicU64>` only on `QueryStats` or shutdown)
- The journal cursor (via a shared `Arc<Sequence>`)
- The active connection count (via a shared `Arc<AtomicU64>`)

This avoids adding any cross-thread synchronization cost on the trading hot path.

### Parallelism with journal

The matching stage does **not** wait for the journal stage. Both consumers are gated only on the producer, so matching proceeds as soon as events are published. The persist-before-ack check is deferred to the response stage. This means the matching stage may process events that are not yet durable -- but no client will see those results until the journal confirms durability.

### Idle behavior

Same adaptive spinning as the journal stage: 1,000 spin loops, then `yield_now()`.

## Response Stage

Defined in `crates/server/src/response.rs`.

The response stage runs on a dedicated OS thread and is the final stage in the pipeline. It consumes from the output SPSC and writes encoded responses to client sockets.

### Journal cursor gating (persist-before-ack)

Before sending any response, the response stage verifies that the corresponding event is durable:

1. For each batch consumed from the SPSC, find the maximum `input_seq` across all slots.
2. If the cached journal cursor position is less than `max_seq + 1`, spin-wait on the journal cursor (`Ordering::Acquire`) until it advances past that value.
3. Once confirmed, encode and send all responses in the batch.

The journal cursor value is cached across batches to avoid redundant atomic loads when the journal is ahead of the response stage.

### Per-connection send buffers

Connections are managed via a `HashMap<u64, ConnectionState>` containing:
- A `BlockingFrameWriter<Box<dyn Write + Send>>` for length-prefixed framing
- A `last_send: Instant` timestamp for heartbeat scheduling

Connections are registered and unregistered via a `std::sync::mpsc` control channel (not the disruptor), since connect/disconnect is rare and not on the hot path.

### Adaptive flushing

The response stage uses an adaptive flush strategy:

- Under high load, it processes many SPSC batches before the queue empties, accumulating writes in `BlockingFrameWriter` buffers. Flushes happen only when the SPSC is empty, amortizing syscall overhead across thousands of entries.
- Under low load, the SPSC empties quickly, and the flush happens promptly.
- A `dirty_connections: HashSet<u64>` tracks which connections have buffered writes pending flush.

### Heartbeats

When the SPSC is empty (idle period), the response stage scans connections for heartbeat timeouts. The scan runs at most once per second (coarse-gated by `Instant` comparison). Connections that have been idle longer than `heartbeat_interval` receive a pre-encoded heartbeat frame. Write errors during heartbeat cause the connection to be dropped.

### Connection lifecycle

- **Connect**: The accept loop sends a `ControlEvent::Connected` with the `BlockingFrameWriter` via the mpsc channel.
- **Disconnect**: Reader threads send `ControlEvent::Disconnected`. The response stage also drops connections on write errors. In both cases, the `active_connections` counter is decremented.

## Output SPSC

The output SPSC queue connects the matching stage to the response stage. Defined in `crates/disruptor/src/spsc.rs`.

**Capacity**: `OUTPUT_RING_CAPACITY = 1 << 20` (1,048,576 slots). Matches the input ring size because one input event can produce multiple output messages (e.g., a market order sweeping many price levels produces one `Fill` per level plus a `BatchEnd`).

**`OutputSlot` layout**:

| Field | Description |
|-------|-------------|
| `connection_id: u64` | Target client connection |
| `input_seq: u64` | Input disruptor sequence (for journal cursor gating) |
| `payload: OutputPayload` | `Report(ExecutionReport)`, `BatchEnd`, `EngineError`, or `StatsHeader` |
| `match_complete_ts` | Matching completion timestamp (zero-sized when `latency-trace` disabled) |
| `recv_ts` | Wire receive timestamp (zero-sized when `latency-trace` disabled) |

The `BatchEnd` marker is critical: it tells the response stage that all execution reports for a given request have been published. The client uses this to know when a request is fully processed.

The SPSC uses two cache-line-padded atomic counters (`head` and `tail`) for coordination, with cached values to reduce atomic reads on the fast path.

## Threading Model

The server spawns 3 dedicated OS threads for the pipeline plus N reader threads:

| Thread | Default Core | Role |
|--------|-------------|------|
| Journal | 1 | Durable write-ahead log |
| Matching | 2 | Order execution (single-writer) |
| Response | 3 | Client socket writes |
| Reader 0 | 4 | Epoll-based connection multiplexing |
| Reader 1 | 5 | Epoll-based connection multiplexing |

Core 0 is reserved for OS/IRQ handling.

### CPU core pinning

Each pipeline thread calls `sched_setaffinity` (via `crate::affinity::pin_to_core`) immediately after spawning, before entering its main loop. Pinning eliminates involuntary context switches and keeps hot data in L1/L2 cache, reducing p99/p99.9 latency jitter from approximately 5-20 us per core migration to near zero.

Reader threads are pinned to cores starting at `--reader-cores` (default 4). Reader thread `i` is pinned to core `reader_cores + i`.

### Why not async

The server is fully synchronous -- no async runtime. Eliminating tokio removes async scheduling jitter from the response path and simplifies reasoning about thread ownership. The reader threads use epoll directly (via libc) for connection multiplexing, and the pipeline threads use spin-wait loops with adaptive yielding.

## Persist-Before-Ack Invariant

The persist-before-ack invariant guarantees that **no client ever receives a response for an event that is not yet durable on disk**. This is the foundation of the event sourcing model: on crash recovery, the journal contains every event that any client was told succeeded.

### Why it matters

Without this invariant, a crash between matching and journal sync could cause:
- A client believes an order was placed, but the journal never recorded it.
- On recovery, the exchange state diverges from what clients observed.
- Regulatory audit trail is broken.

### How it is enforced

1. The journal stage reads events from the input disruptor and writes them to disk. Its consumer progress cursor advances **only after** `flush_batch_sync()` completes (durable write confirmed by the kernel/NVMe controller).
2. The matching stage publishes each `OutputSlot` with the `input_seq` it originated from.
3. The response stage, before sending a batch, computes `max_seq = max(batch[..count].input_seq)` and spin-waits until `journal_cursor >= max_seq + 1`.
4. Only then are the responses encoded and written to client sockets.

Because the journal and matching consumers run in parallel (not chained), the matching stage often finishes before the journal. The response stage absorbs this difference by waiting on the journal cursor, achieving maximum pipeline parallelism while preserving the durability guarantee.

## Configuration

| Flag | Default | Description |
|------|---------|-------------|
| `--cores` | `1,2,3,6` | Pipeline core IDs: journal, matching, response, repl-sender (comma-separated) |
| `--readers` | `2` | Number of epoll reader threads |
| `--reader-cores` | `4` | First CPU core for reader thread pinning (reader i -> core reader_cores + i) |
| `--group-commit-us` | `0` | Group commit coalescing delay in microseconds. Keep at 0 for TCP. |
| `--heartbeat-interval-secs` | `10` | Heartbeat interval for idle connections (0 to disable) |
| `--connection-timeout-secs` | `30` | Disconnect clients silent for this long (0 to disable) |
| `--max-connections` | `1024` | Maximum concurrent authenticated connections (0 for unlimited) |

## Feature Gates

| Feature | Effect |
|---------|--------|
| `no-persist` | Disables journal writes entirely. Events are still sequenced through the disruptor but not written to disk. Used for benchmarking engine throughput without I/O overhead. |
| `no-fsync` | Disables `RWF_DSYNC` / `fdatasync`. Events are written to the journal file but not synced. The journal cursor advances immediately after encoding (no sync trigger logic). Useful for development. |
| `pipeline-stats` | Enables busy/idle utilization counters on each stage. Printed on shutdown showing percentage busy, total busy iterations, and total idle iterations. |
| `latency-trace` | Enables per-event timestamps at each pipeline boundary. Tracks: disruptor wakeup latency (publish to consume), batch processing time, SPSC wakeup latency, dispatch latency, and server-side end-to-end (reader recv to response flush). Histograms are printed on shutdown. The `TraceTimestamp` type is `()` (zero-sized) when disabled, so there is no overhead in production builds. |
| `io-uring` | Replaces the epoll reader and blocking response writer with io_uring-based implementations. |

## Key Constants

| Constant | Value | Location |
|----------|-------|----------|
| `INPUT_RING_CAPACITY` | `1 << 20` (1,048,576) | `crates/engine/src/journal/pipeline.rs` |
| `OUTPUT_RING_CAPACITY` | `1 << 20` (1,048,576) | `crates/engine/src/journal/pipeline.rs` |
| `MAX_JOURNAL_BATCH` | `1024` | `crates/engine/src/journal/pipeline.rs` |
| `MAX_BATCH` (response) | `1024` | `crates/server/src/response.rs` |
| `MAX_RESPONSE_BUF` | `128` bytes | `crates/server/src/response.rs` |
| `MAX_EPOLL_EVENTS` | `64` | `crates/server/src/reader.rs` |
