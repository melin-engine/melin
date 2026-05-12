# Pipeline Architecture

This document describes the LMAX-style disruptor pipeline that forms the core of the trading engine's I/O and execution model.

## Overview

The server uses a 3-stage pipeline plus a single reader thread, modeled after the LMAX Disruptor architecture:

```
  Reader     -----+    +-------------------+
  (1 io_uring     +--->| Input Disruptor   |--+--> Journal Stage
   thread, also   |    | (1M-slot ring)    |  |
   emits Ticks    |    +-------------------+  +--> Matching Stage
   via io_uring   |                           |         |
   timeout)       |                           |         | Output Disruptor
                  |                           |         | (multi-consumer)
  Seeding --------+                           |         v
  (startup only)                              |  +--> Response Stage --> Clients
                                              |  |
                                              |  +--> Event Publisher --> Subscribers
                                              |       (optional, --event-bind)
                                              +----> Shadow Stage --> Snapshots
                                                     (optional, gated on journal)
```

1. **Reader** -- a single thread multiplexes every TCP client connection and publishes decoded requests into the input disruptor. The same thread also generates the engine's scheduler ticks at the configured cadence (**default 250 ms**). With this, the input ring is single-producer in steady state on both transports.
   - **io_uring**: the reader arms an `IORING_OP_TIMEOUT` SQE at the cadence; the deadline wakes `submit_and_wait` even when no client traffic is flowing, and the loop emits a `JournalEvent::Tick { now_ns }` when the deadline passes.
   - **DPDK**: the poll thread compares the wall clock to the deadline once every ~4096 poll iterations (negligible cost on a 100% busy spin loop) and emits the tick the same way.
2. **Tick semantics** -- the matching stage advances its scheduler clock from `slot.timestamp_ns` on every event, so under load each order/cancel implicitly fires due tasks at microsecond precision. The 250 ms tick is the safety net that keeps time moving forward during quiet periods (no client traffic).
3. **Journal stage** -- batch-encodes events and writes them durably to disk via `pwrite` + `O_DIRECT` (PLP drives required). Advances its cursor only after the write completes.
4. **Matching stage** -- executes commands against the `Exchange` engine and publishes execution reports to an output disruptor ring. Runs in parallel with the journal stage (does not wait for fsync).
5. **Response stage** -- consumes from the output ring but gates on the journal cursor before sending responses to clients, enforcing the persist-before-ack invariant.
6. **Event publisher** (optional) -- second consumer on the output ring, enabled by `--event-bind`. Broadcasts all execution events to TCP subscribers for market data gateways, analytics, and audit loggers. Ed25519 auth required.
7. **Shadow stage** (optional) -- third consumer on the input ring, gated on the journal cursor. Periodically saves an exchange snapshot on a dedicated thread without pausing the matching engine.

**Why this design**: Single-threaded business logic (the matching stage) eliminates locks on the hot path. Parallelizing journal I/O with matching hides fsync latency. The persist-before-ack boundary is enforced at the response stage, not in the matching stage, so the engine never stalls waiting for disk.

## Full data flow (primary + replica)

The simplified diagram above shows the primary-side request path. The picture below is the full topology — including the tick generator, the shadow stage, and the replication transport to replicas — as it exists today.

```
+===============================================================+
|                           PRIMARY                              |
+===============================================================+

 CLIENT TCP   +   WALL CLOCK            SEED LOOP
 (N conns)    |   (io_uring timeout)    (startup only)
      |       |        ^                     |
      v       |        | TIMEOUT_TOKEN       |
 +----------------+    | CQE                 |
 |    READER      |----+                     |
 | (1 io_uring    |  arms tick timeout       |
 |  thread; also  |  per cadence; emits      |
 |  publishes     |  Tick{now_ns} when       |
 |  Ticks)        |  the deadline passes     |
 +-------+--------+                          |
         |                                   |
         | client requests + Tick{now_ns}    |  AddInstrument /
         |                                   |  ProvisionAccount
         v                                   v
 +---------------------------------------------+
 |  INPUT RING -- disruptor, 1M InputSlot      |
 +---+-------------+----------------+----------+
     |             |                |
     v             v                v (gated on journal)
 +-------+    +---------+     +-----------+
 |JOURNAL|    |MATCHING |     |  SHADOW   | (optional)
 | STAGE |    |  STAGE  |     |  STAGE    |
 +-+--+--+    +---+-----+     +-----+-----+
   |  |           |                 |
   |  |           v                 v
   |  |     +-------------+    periodic
   |  |     | OUTPUT RING |    .snapshot
   |  |     | disruptor   |
   |  |     +--+-------+--+
   |  |        |       |
   |  |        v       v
   |  |   +--------+ +-----------+
   |  |   |RESPONSE| |EVENT PUB  | (opt, --event-bind)
   |  |   | STAGE  | |           |
   |  |   +---+----+ +----+------+
   |  |       |           |
   |  |       v           v
   |  |   CLIENT TCP   MARKET-DATA
   |  |   (reports)    SUBSCRIBERS
   |  |
   |  |  pwritev2 (RWF_DSYNC) -> JOURNAL FILE
   |  |  journal bytes carry (sequence, timestamp, event, key_hash, request_seq)
   |  |
   |  |  post-fsync: push encoded batch bytes
   |  v
   |  +------------------+
   |  | REPLICATION RING |  per-replica (up to 2)
   |  |  slot 0 | slot 1 |  ring-of-batches (not per-event)
   |  +----+---------+---+
   |       |         |
   |       v         v
   |   +--------+ +--------+
   |   | REPL   | | REPL   |
   |   | SENDER | | SENDER |
   |   |   0    | |   1    |
   |   +----+---+ +---+----+
   |        |         |
   |        | TCP     | TCP
   |        v         v
   v      replica 0  replica 1
 (local journal on disk; used on primary's own recovery path)


+===============================================================+
|                          REPLICA                               |
+===============================================================+

     TCP from primary
          |
          | journal-batch bytes (pre-sequenced, pre-hashed)
          v
 +----------------+
 | REPL RECEIVER  |   parses batches, emits one InputSlot per
 |     THREAD     |   journal entry with the primary's sequence
 +-------+--------+   and timestamp embedded verbatim
         |
         v
 +---------------------------------------------+
 |  INPUT RING -- disruptor, 1M InputSlot      |
 +---+--------------+---------------+----------+
     |              |               |
     v              v               v (gated on journal)
 +-------+    +---------+     +-----------+
 |JOURNAL|    |MATCHING |     |  SHADOW   | (optional)
 | STAGE |    |  STAGE  |     |  STAGE    |
 +---+---+    +----+----+     +-----+-----+
     |             |                |
     v             v                v
   LOCAL     +-------------+   .snapshot
  JOURNAL    | OUTPUT RING |
   FILE      | disruptor   |
             +------+------+
                    |
                    v
              +---------+
              |  DRAIN  |  reports are discarded — replica
              | (only   |  does not serve clients until
              | consumer|  promotion. See docs/replication.md.
              +---------+
```

### Responsibilities, at a glance

| Thread / stage              | Ingress                                 | Egress                                                |
|----------------------------|-----------------------------------------|-------------------------------------------------------|
| Reader (primary)           | Client TCP/DPDK + cadence wakeup (wall-clock-cadenced, monotonic-clamped) | Client requests AND `JournalEvent::Tick` into the same input ring |
| Seed loop (primary, boot)  | Config (`--accounts`, `--instruments`)  | `AddInstrument` / `ProvisionAccount` into input ring  |
| Journal stage              | Input ring                              | Journal file; batch bytes into each replication ring  |
| Matching stage             | Input ring                              | Execution reports into output ring                    |
| Shadow stage (opt)         | Input ring (gated on journal)           | Periodic `.snapshot` files                            |
| Response stage (primary)   | Output ring (gated on journal cursor)   | Client TCP                                            |
| Event publisher (opt)      | Output ring                             | Subscriber TCP (market data feed)                     |
| Replication sender         | Replication ring                        | Replica TCP                                           |
| Replication receiver (rep) | Primary TCP                             | `InputSlot` into replica input ring (sequence stamped from primary's bytes) |

### Authoritative state, and how it flows

- **Event payload**: produced at the ingress edge (client requests, the ingress thread's tick generator, seed loop). Flows unchanged through every stage and across the TCP boundary to replicas.
- **Sequence number**: on the primary, allocated by the journal stage at encode time, in disruptor ring-cursor order. Producers publish `InputSlot { sequence: 0, … }` and never coordinate across an external counter — eliminating the prior "claim then publish" leak window. On replicas the replication receiver decodes the primary's sequence from the wire bytes and stamps it onto `InputSlot.sequence` before publishing; the journal stage uses that value verbatim. Either way the on-disk journal sequence and the disruptor cursor advance in lock-step.
- **Wall-clock timestamp**: stamped at ingress by each producer (e.g. `wall_clock_nanos()` in the reader). Embedded into the journal entry and shipped to replicas.
- **Hash chain**: computed by the primary's journal writer over each batch's bytes (sequence + payload + checkpoint metadata). Replicas recompute the chain over the received bytes and should arrive at the same hash.
- **Replica journals** are *logically* identical to the primary's for the event stream (same sequences, same events). They are not byte-identical: each node stamps its own batch-header wall clock and may emit checkpoints at different rotation boundaries.

### Scheduler clock

The matching stage maintains a per-instance `last_drain_ns` watermark. At the head of every event it processes, if `slot.timestamp_ns > last_drain_ns` it drains all due scheduled tasks up to `slot.timestamp_ns` and updates the watermark. Under load this means each order/cancel event implicitly fires due tasks (GTD expiry, etc.) at microsecond precision, with no extra latency hop for a separate `Tick` event. The tick generator's role narrows to "make sure the clock advances during quiet periods" — at the default 250 ms cadence it costs ~4 events/sec of journal traffic.

`replay_event` and the shadow stage's `dispatch_event` mirror the same drain at the same point so live, replay, and shadow exchanges stay byte-identical.

## Input Disruptor

The input disruptor is a multi-producer, multi-consumer ring buffer defined in `crates/disruptor/src/ring.rs`.

**Capacity**: `INPUT_RING_CAPACITY = 1 << 20` (1,048,576 slots). At approximately 72 bytes per `InputSlot`, this is roughly 72 MiB -- sized to fit in L3 cache on modern server CPUs. Provides approximately 100 ms of buffering at 10M orders/sec, enough headroom for fsync stalls without backpressure reaching the readers.

**Publishing**: Reader threads publish via `MultiProducer`, which uses CAS-based slot claiming (the LMAX multi-producer pattern):

1. `fetch_add` on the cursor atomically claims a unique sequence number.
2. The value is written to the claimed slot.
3. A per-slot generation flag (stored in a parallel `AtomicI32` array) is set so consumers know the slot is ready.

The `MultiProducer` is `Clone + Send + Sync` -- each reader thread holds its own clone. No mutex is needed. The per-slot generation array costs approximately 4 MiB (1M slots x 4 bytes).

**Consumers**: Two or three consumers are registered in parallel, both gated on the producer only (not on each other):

- **Consumer 0**: Journal stage
- **Consumer 1**: Matching stage
- **Consumer 2** (optional): Shadow exchange stage (when `--snapshot-interval-secs > 0`). Gated on the journal cursor — it only processes events after they are durable. Takes periodic snapshots on a dedicated thread without pausing the matching engine.

Because the journal and matching consumers are gated only on the producer, they can process events concurrently. The journal stage does not block the matching stage, and vice versa. Backpressure is applied by the producer checking the minimum progress of all terminal consumers before claiming new slots.

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
2. Batch-encode all events into the `SectorWriter`'s internal buffer. `QueryStats` events are skipped (they cause no state change and are not journaled).
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

Defined in `crates/server/src/response.rs` (io_uring-based SEND).

The response stage runs on a dedicated OS thread and is the final stage in the pipeline. It consumes from the output SPSC and writes encoded responses to client sockets.

### Durability gating (persist-before-ack)

Before sending any response, the response stage verifies that the corresponding event is durable:

1. For each batch consumed from the SPSC, find the maximum `input_seq` across all slots.
2. Determine the durable position:
   - **Quorum mode** (default, 2 replicas connected): `replication_cursor` — both replicas have acked, NVMe fsync is off the critical path.
   - **Degraded/standalone mode** (0-1 replicas): `min(journal_cursor, replication_cursor)` — local fsync is required.
3. If the durable position is less than `max_seq + 1`, spin-wait until it advances.
4. Once confirmed, encode and send all responses in the batch.

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

**Multi-consumer**: When `--event-bind` is set, the output ring has two consumers: (1) the response stage (per-client, gated on durability cursors), and (2) the event publisher (TCP broadcast to subscribers). Both consumers run in parallel. The producer is gated on the slowest consumer.

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

The server spawns 3-6 dedicated OS threads for the pipeline plus one reader thread:

| Thread | Default Core | Role | Optional? |
|--------|-------------|------|-----------|
| Journal | 1 | Durable write-ahead log | No |
| Matching | 2 | Order execution (single-writer) | No |
| Response | 3 | Client socket writes | No |
| Reader | 4 | io_uring-based connection multiplexing + tick generation | No |
| Repl Sender | 6 | Stream journal batches to replicas | Yes (`--replication-bind`) |
| Event Publisher | 7 | Broadcast execution events to subscribers | Yes (`--event-bind`) |
| Shadow Exchange | 8 | Periodic snapshots without pausing matching | Yes (`--snapshot-interval-secs`) |

Core 0 is reserved for OS/IRQ handling.

### CPU core pinning

Each pipeline thread calls `sched_setaffinity` (via `crate::affinity::pin_to_core`) immediately after spawning, before entering its main loop. Pinning eliminates involuntary context switches and keeps hot data in L1/L2 cache, reducing p99/p99.9 latency jitter from approximately 5-20 us per core migration to near zero.

In **kernel TCP mode**, the reader thread is pinned to `--reader-cores` (default 4). io_uring with multishot RECV multiplexes every client connection on this single thread.

In **DPDK mode**, a single poll thread handles all client connections (one NIC queue, no RSS). It is also pinned to the `--reader-cores` core.

### Why not async

The server is fully synchronous -- no async runtime. Eliminating tokio removes async scheduling jitter from the response path and simplifies reasoning about thread ownership. The reader thread uses io_uring with multishot RECV for connection multiplexing, and the pipeline threads use spin-wait loops with adaptive yielding.

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
| `--cores` | `1,2,3,6,7,8` | Pipeline core IDs: journal, matching, response, repl-sender, event-publisher, shadow (comma-separated) |
| `--reader-cores` | `4` | CPU core for the reader thread (TCP) or first poll thread (DPDK). |
| `--group-commit-us` | `0` | Group commit coalescing delay in microseconds. Keep at 0 for TCP. |
| `--heartbeat-interval-secs` | `10` | Heartbeat interval for idle connections (0 to disable) |
| `--connection-timeout-secs` | `30` | Disconnect clients silent for this long (0 to disable) |
| `--max-connections` | `1024` | Maximum concurrent authenticated connections (0 for unlimited) |

## Feature Gates

| Feature | Effect |
|---------|--------|
| `no-persist` | Disables journal writes entirely. Events are still sequenced through the disruptor but not written to disk. Used for benchmarking engine throughput without I/O overhead. |
| `pipeline-stats` | Enables busy/idle utilization counters on each stage. Printed on shutdown showing percentage busy, total busy iterations, and total idle iterations. |
| `latency-trace` | Enables per-event timestamps at each pipeline boundary. Tracks: disruptor wakeup latency (publish to consume), batch processing time, SPSC wakeup latency, dispatch latency, and server-side end-to-end (reader recv to response flush). Histograms are printed on shutdown. The `TraceTimestamp` type is `()` (zero-sized) when disabled, so there is no overhead in production builds. |
| `io-uring` | No-op (kept for backward compatibility). io_uring is now always used for readers, response writes, and replication I/O. |

## Key Constants

| Constant | Value | Location |
|----------|-------|----------|
| `INPUT_RING_CAPACITY` | `1 << 20` (1,048,576) | `crates/engine/src/journal/pipeline.rs` |
| `OUTPUT_RING_CAPACITY` | `1 << 20` (1,048,576) | `crates/engine/src/journal/pipeline.rs` |
| `MAX_JOURNAL_BATCH` | `1024` | `crates/engine/src/journal/pipeline.rs` |
| `MAX_BATCH` (response) | `1024` | `crates/server/src/response.rs` |
| `MAX_RESPONSE_BUF` | `128` bytes | `crates/server/src/response.rs` |
| `NUM_BUFFERS` | `2048` | `crates/server/src/reader.rs` (io_uring provided buffer pool) |
