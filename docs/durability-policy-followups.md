# Durability policy & ack-on-receive — open follow-ups

Action-oriented list of work remaining on the durability policy
framework (roadmap item #4, on `feat/durability-policy`) and the
ack-on-receive plumbing (roadmap item #5, on `feat/ack-on-receive`).
Items are things to implement, improve, or fix — completed work is
not tracked here.

---

## Bugs to fix on `feat/ack-on-receive` before merge

### Backpressure / keepalive paths leak the coalescing tracker

The dual-track flush block in each receiver maintains
`last_sent_acked_seq` and `last_sent_in_memory_seq` to coalesce acks.
Backpressure-drain and keepalive sites send acks without updating
these — directly after a backpressure event the regular flush block
sees its tracker as stale and either fires a duplicate ack or, in
pathological cases, sends an `acked_sequence` on the wire that's
lower than the value the backpressure path already published
(monotonicity violation on the primary's view of the cursor).

Sites:

- `crates/server/src/replication/tcp_receiver.rs:441-451` — backpressure-drain ack
- `crates/server/src/replication/dpdk.rs:1343, :1472, :1482` — close-time ack, backpressure, disconnect-drain
- `crates/server/src/replication/rumcast_receiver.rs:1054, :1072` — backpressure-drain and idle-keepalive

Fix: assign both tracking variables wherever an ack goes out. Pair
with a `debug_assert!(acked_now >= last_sent_acked_seq)` in the
flush block to catch the namespace-translation class of bug (a prior
implementation attempt sent `journal_cursor.load()` on the wire,
mixing local-ring-position and primary-sequence spaces).

### Coalescing is per-iteration, not per-time-window

On a `busy_spin` loop, iterations are sub-microsecond. Each cursor
advance triggers an ack — potentially millions of acks/sec instead
of the ~20k/sec roadmap #5 projected. The "coalescing falls out
naturally while SEND is in flight" claim holds only when SEND is
genuinely pending; on a fast loop with no in-flight SEND, every
delta fires.

Fix path (decide after bench):

- Bench the actual ack rate under realistic load.
- If excessive, add a 50–100 µs minimum-interval throttle on the
  flush block (matches the design call in roadmap #5).
- Otherwise document the per-iteration behaviour as acceptable and
  correct the overstated comment at `tcp_receiver.rs:262`.

### Stale comments and docs after the ack-on-receive landing

- `crates/server/src/server.rs:636` and `crates/server/src/rumcast_transport.rs:2153` reference "pending ack-on-receive plumbing" — stale.
- `docs/replication.md:160` reads "Sends `Ack` frames after the journal stage confirms durable write" — now misleading; acks also fire on receive via the in-memory cursor track.
- `docs/roadmap.md:16` (item #5) phrased as future work. Receiver-side has landed on `feat/ack-on-receive`; CLI-flag swap (next section) remains.

---

## Tests to add on `feat/ack-on-receive`

- **Regression for the namespace bug**: set `--durability-policy in_memory>=2`, drive traffic through a 1+2 cluster, assert the primary's `metrics.in_memory_sequence[slot]` advances *before* `metrics.acked_sequence[slot]`. A prior implementation attempt mixed local-ring-position and primary-sequence spaces on the wire; this test would catch any re-introduction.
- **Backpressure-drain → flush duplicate-ack sequence**: simulate a queue-full event, drive a follow-up batch, assert the next ack does not regress `acked_sequence` on the primary.
- **Unit test for the dual-track coalescing rule**: a focused test (not full integration) on the flush block's `acked_now > last_sent || in_mem_now > last_sent` logic.

---

## Refactor: extract `try_flush_dual_track` helper

Three near-identical inlined flush blocks live in `tcp_receiver`,
`dpdk`, and `rumcast_receiver`. Drift between them is exactly the
shape of bug the first implementation attempt produced (namespace
mismatch in one receiver but not another). Extract a shared helper
so future changes apply uniformly:

```rust
fn try_flush_dual_track(
    pending_acks: &mut PendingAckQueue,
    journal_cursor: &Sequence,
    accum_end_sequence: u64,
    last_sent_acked: &mut u64,
    last_sent_in_memory: &mut u64,
    async_ack: bool,
) -> Option<Ack> { /* ... */ }
```

Each receiver still owns its send-side I/O (io_uring SEND, DPDK
queue, rumcast publish) but the cursor-advance + coalescing logic
becomes one tested function. Carry the load-bearing comment about
namespace translation (currently at `tcp_receiver.rs:262`) on the
helper, not on each call site.

---

## Next interface step: 3-variant `DurabilityMode` enum

After ack-on-receive validates on the bench, swap the operator-
facing surface from the DSL (`--durability-policy <STRING>`) to a
single `--durability-mode <local|hybrid|durably-replicated>` flag.

Target enum:

```rust
pub enum DurabilityMode {
    /// `persisted>=1`. Single-node durability — the primary's
    /// fsync is the only confirmation needed. Standalone / dev.
    Local,

    /// `persisted>=1 && in_memory>=2`. One durable copy on disk
    /// plus an in-memory ack from another node. Single-failure-
    /// safe; ~80 µs RAM-only window for the secondary copy. The
    /// new default — typical exchange deployments on PLP-backed
    /// NVMe. Saves ~50–80 µs per fill vs `DurablyReplicated`.
    Hybrid,

    /// `persisted>=2`. Two durable copies before client ack. Zero
    /// RAM-only window; gate stalls if a replica is unreachable.
    /// Compliance-driven venues.
    DurablyReplicated,
}
```

### What gets dropped

- `--durability-policy <STRING>` flag.
- The DSL parser (~150 LOC + ~30 unit tests).
- The `best_effort` modifier syntax.
- The floor pattern (`persisted>=3 best_effort && persisted>=2`) —
  largely redundant with the matching-stage halt at
  `replicas_connected==0` for new orders.

### What stays

- `Policy` / `Clause` / `Level` types as internal construction
  helpers (each mode builds its clause list in code).
- Wire protocol unchanged: `Ack { acked_sequence, in_memory_sequence }`
  carries forward identically.
- All cursor plumbing, observability (`policy_degraded` gauge,
  periodic warn, `DegradationLogger`), and tests.

### Why retire `async_ack` here too

`async_ack` is hardcoded `false` at every call site in production
since `--async-replica-ack` was removed. The receiver code still
threads the boolean through the streaming loop and the
`pop_all_async` branch in the flush block remains live. When the
enum swap lands, drop the parameter from receiver signatures and
remove the `pop_all_async` branch — the dual-track flush already
delivers the latency the legacy flag was trying to enable.

Net code reduction across both pieces: ~250 LOC plus the tests.
Operator-facing surface shrinks to one flag with three values.

---

## Commercial polish (buyer-driven)

These are real features but only worth building when a specific
buyer asks:

- **Degraded-duration counter on `/healthz`** — turn `melin_durability_policy_degraded` from a 0/1 gauge into a paired counter (`melin_durability_policy_degraded_seconds_total`) so SLO dashboards can compute time-in-degraded over arbitrary windows.
- **Multi-region awareness** — operators with replicas across availability zones want "≥1 ack from each zone" (Cassandra `EACH_QUORUM`). Needs node-tagging at handshake plus a richer policy clause shape. Would justify a 4th `DurabilityMode` variant.
- **Per-request policy override** — let the client specify a stronger consistency level per high-stakes order (Cassandra `w=` / MongoDB pattern). The wire protocol already carries a per-request envelope that could be extended. Composes cleanly with the enum: operator's `--durability-mode` becomes a default, per-request overrides scoped to the same named-mode set.
