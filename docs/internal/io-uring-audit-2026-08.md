# io_uring / disruptor integration audit — August 2026

Code-level audit of every io_uring ↔ disruptor boundary (reader ingress,
response egress, journal writes, replication I/O) against the integration
invariants listed below. Unlike the July latency audit, the top finding
here is a **correctness/availability defect**, not a performance read:
one slow client can stall the acknowledgement path for every client.

Findings 1–2 are defects; 3–5 are optimization gaps. Fixes are being made
on the `fix/io-uring-audit` branch.

---

## The invariants

These are the rules a disruptor-based pipeline must hold when its stages
do I/O through io_uring. Each finding below is a violation of one of them.

1. **One ring per thread; the ring owner is the sole producer or consumer
   at that boundary.** io_uring instances are not cheaply shareable, and
   the disruptor's single-writer principle wants exactly one thread at
   each end anyway. Melin holds this everywhere.
2. **Data crosses the boundary by copy into owned buffers — SQEs never
   reference disruptor slots.** A slot can be reused on ring wrap-around
   while the kernel still holds a pointer into it. Melin holds this
   everywhere (reader copies out of the provided-buffer pool; the journal
   encodes into owned double buffers whose ownership transfers to the
   in-flight batch until the CQE confirms).
3. **Durability CQEs drive gating-sequence advancement.** The journal
   consumer's cursor only advances when the write's CQE arrives, so
   downstream consumers that gate on it (the response stage's durability
   gate) inherit "acked implies durable" for free. Melin holds this
   (`confirm_async_write` → `consumer.set_progress`).
4. **Completions must never pace a fan-out stage.** Egress completions
   exist to recycle buffers, not to sequence work. Blocking a stage on a
   send completion couples every connection's latency to the slowest
   peer. **Violated — findings 1 and 2.**
5. **Backpressure flows CQ → producer stall → TCP window, or is shed
   explicitly — never absorbed into unbounded userspace queues or
   blocking calls.** Melin sheds load explicitly (ServerBusy on pipeline
   full, `MAX_SEND_BUF` drop on laggy consumers), which is the right
   design for bounded latency — but the shedding write itself violates
   invariant 4 (finding 2).
6. **Stay on the inline submission path.** An io_uring op that cannot
   complete inline is punted to an io-wq kernel worker — tens of
   microseconds and a p99.9 killer. O_DIRECT with aligned writes,
   prefaulted pages, registered files/buffers keep ops inline. Melin
   mostly holds this (prefaulting, O_DIRECT, registered files, io-wq
   affinity pinning on the journal ring); findings 3–5 are the residue.

## Triage summary

Severity is a judgement call from code reading. Findings 3–5 are unmeasured
performance reads — same caveat as the July audit: instrument before
optimizing. Findings 1–2 need no measurement; they are behavioural defects
with deterministic reproductions.

| # | Finding | Kind | Severity | Fix effort |
| --- | --- | --- | --- | --- |
| 1 | Response stage blocks on slow-client SENDs | Defect (availability) | **High** | Med |
| 2 | ServerBusy write can block the reader thread | Defect (availability) | Med | Trivial |
| 3 | Legacy `ProvideBuffers` instead of a buf_ring | Perf (hot path) | Med | Med |
| 4 | Journal uses `Write`, not `WriteFixed` | Perf (per-batch fixed cost) | Low–Med | Low–Med |
| 5 | `SINGLE_ISSUER` missing on reader/response rings | Perf (free flag) | Low | Trivial |

---

## 1. One stalled client wedges the response stage for all clients

`flush_sends` (`crates/core/server-runtime/src/response.rs`) submits one
SEND per dirty connection, then `submit_and_wait(pending)` — it does not
proceed until **every** send has completed. The client sockets are never
set non-blocking (only the listener is), and the SENDs carry no
`MSG_DONTWAIT`. Neither would matter on its own: io_uring never surfaces
`EAGAIN` from a plain SEND — when the socket buffer is full it arms an
internal poll and withholds the CQE until the peer reads.

So a client that stops reading (full TCP receive window — a laggy
consumer, a wedged process, or a deliberate zero-window attack) makes its
SEND's CQE arrive only when *it* decides to read. Until then
`submit_and_wait(pending)` blocks the response thread: every other
client's acknowledgements stall behind it, unboundedly. `retry_send` has
the same shape (a `submit_and_wait(1)` loop until the buffer drains).

The existing guards don't reach this:

- `MAX_SEND_BUF` (64 KiB drop) operates on the userspace buffer at append
  time; the stall is at the io_uring level during flush.
- The heartbeat-timeout scan runs on the same thread that is blocked.
- The DPDK replication path handles its equivalent explicitly (TX
  overflow → disconnect, `replication/dpdk.rs`); the kernel-TCP response
  path has no equivalent.

Trigger: any client, no special privileges — connect, send orders, stop
reading responses. This is both a tail-latency bug and a trivially
reachable denial of service on the kernel-TCP transport.

**Fix direction:** put `MSG_DONTWAIT` in every SEND's msg_flags — with it
io_uring completes the op immediately with `-EAGAIN` instead of parking
it (the flag sets `REQ_F_NOWAIT`), so `submit_and_wait` degenerates into
"submit + drain" and can no longer stall. Undelivered bytes stay in the
connection's `send_buf` and the connection stays dirty for a paced retry;
`MAX_SEND_BUF` remains the growth cap, plus a blocked-duration cap so a
client that stops reading during a quiet period (nothing appending, so
`MAX_SEND_BUF` never trips) is still dropped. `retry_send`'s blocking
loop is deleted.

A fully completion-driven alternative (fire SENDs, never wait, reap CQEs
opportunistically, arm `POLL_ADD(POLLOUT)` for blocked peers) was
considered and rejected: it needs per-connection in-flight buffer
ownership, cross-flush CQE accounting, and cancel-on-disconnect handling
— real complexity on the correctness-critical ack path, buying
efficiency only in the rare blocked-client case that the paced retry
already bounds.

**Adversarial-review follow-ups (2026-08-07)**, applied on top of the
initial fix:

- `submit_and_wait` retries `EINTR` instead of returning early. The
  interrupt lands after the submit phase consumed the SQEs, so the
  inline completions are already posted; returning without reaping left
  stale CQEs that the *next* flush would apply against updated buffer
  contents — a delivered prefix re-sent and `drain(..sent)` applied
  twice. Non-`EINTR` submit errors now fall through and apply whatever
  completed rather than leaving those CQEs stale.
- The blocked-drop clock measures time since last *forward progress*,
  not time since first refusal: a partial send restarts it. A
  slow-but-steadily-draining client is `MAX_SEND_BUF`'s problem;
  only a peer accepting zero bytes for the full timeout is dropped.
- ~~Accepted, not fixed here~~ **since closed (2026-08-07)**: the busy
  path (SPSC never empty, gate open) originally flushed only inside the
  gate-wait branch, starving both delivery and the blocked-peer
  bookkeeping during a sustained busy stretch. Resolved as finding 4 of
  `latency-audit-2026-07.md`: a byte-threshold flush on the consumed
  path (see that doc for the design and the outstanding bench run).
  Still accepted: under `latency-trace`,
  e2e samples close on the first flush *attempt* even when the SEND
  returned `EAGAIN`, slightly understating egress for blocked peers
  (trace-only, and blocked peers are the pathology being measured
  around, not the signal).

**Second review round (2026-08-07)**, on the follow-up commits:

- A non-`EINTR` `submit_and_wait` error means the SQEs were *not*
  consumed; "drain and continue" would leave them queued for the next
  flush's submit, against buffers mutated in between — the double-drain
  class resurrected on the error path. The stage now fails loudly
  instead (panic → the accept loop's pipeline-death detection): the
  errno class is kernel resource exhaustion, and on the ack path of a
  financial sequencer, dying beats sending corrupt bytes.
- A CQE result of 0 (unreachable for `SOCK_STREAM` sends of nonzero
  length) is treated as zero progress, not as a partial send, so it
  cannot restart the blocked clock.

## Review finding F1: response-initiated drops leaked the connection

Found while auditing the drop path (pre-existing, not io_uring-specific,
widened by the new `BLOCKED_SEND_TIMEOUT` drop): removing a connection
from the response stage's map only dropped *its* dup of the socket. The
reader holds another dup, no `shutdown(2)` was issued, and no
reader-ward disconnect signal exists — so the socket stayed fully open.
A dropped client that kept sending kept its ingress slot indefinitely
(the reader's idle timeout never fires while traffic flows) with zero
acks ever delivered. Worse, `active_connections` was never decremented
anywhere on the kernel-TCP path: every disconnect permanently consumed a
`max_connections` permit — reconnect churn alone could walk a server to
"connection rejected" with near-zero live clients.

Fixed: a response-initiated drop now calls `shutdown(2)` `SHUT_RDWR`
(reaches every dup — the reader's multishot RECV completes with 0, it
tears down its half and emits `Disconnected`, which finds the entry
already gone), and the permit is decremented exactly when an entry
actually leaves the response stage's connection map, whichever side
initiated the death.

## Review finding F3: two threads wrote the same client socket

The reader's direct ServerBusy send and the response stage's SENDs
targeted the same socket. After the finding-1 fix, a *partially-flushed*
response frame with the remainder held for paced retry is a designed
steady state — a ServerBusy landing between the two halves permanently
desyncs the client's length-prefix framing, and the trigger conditions
(pipeline full, slow consumer) are correlated. Fixed by making the
response stage the sole egress writer per socket: the reader emits
`ControlEvent::PipelineBusy` and the busy frame goes out as an ordinary
send-buffer append. Invariant worth preserving in future transports:
**one thread writes a given client socket, ever.**

## 2. The ServerBusy frame is written with a blocking syscall on the reader thread

When the input disruptor is full, the reader sheds load by writing a
pre-encoded ServerBusy frame with plain `write(2)` on the client's fd
(`reader.rs`, `process_frames`). Client fds are in blocking mode, so if
that client's socket send buffer is also full, `write` blocks the reader
thread — ingress for **all** connections stops.

This fires exactly when it is most likely to hurt: the pipeline-full path
is the overload path, and the client most likely to have triggered it is
the one most likely to have a congested socket. The comment already
declares the write best-effort; it just isn't. (Review correction: the
accept loop sets `SO_SNDTIMEO` on the socket, so each blocked write was
bounded at ~5 s, not indefinite — repeated 5 s ingress stalls per
overload event rather than a permanent wedge. Still a defect.)

**Fix, as landed (two steps):** first `send(2)` with `MSG_DONTWAIT`,
dropping the frame on `EAGAIN` — the request was already shed and client
timeout was the documented fallback. The adversarial review then flagged
that *any* reader-side send races the response stage's writes on the
same socket (finding F3 below), so the final form goes further: the
reader emits `ControlEvent::PipelineBusy` and the response stage — the
sole egress writer per socket — appends the pre-encoded frame to the
connection's send buffer. The reader no longer touches client sockets at
all.

## 3. Receive-buffer recycling costs one SQE + one CQE per received chunk

The reader hands buffers back to the kernel with legacy `ProvideBuffers`
SQEs (`re_provide_buffer`, one per consumed receive). Each recycle is an
SQE on submit and a CQE to skip on drain. At the multi-million-requests/s
rates the reader comments cite, that roughly doubles SQ traffic and adds
a branch per CQE.

The ring-mapped provided-buffer ring (`IORING_REGISTER_PBUF_RING`, kernel
≥ 5.19; production hosts run 6.8) replenishes with a userspace write and
an atomic tail bump — no SQE, no CQE, no syscall. Same pool, same
multishot RECV, same CQE `buffer_id` plumbing.

**Fixed, as landed** (`crates/core/server-runtime/src/buf_ring.rs`):

- The `BufRing` type documents and encodes the kernel contract (release
  store of the tail pairing with the kernel's acquire read) and the
  overrun discipline (a bid is only ever pushed after the kernel handed
  exactly that bid back in a CQE, so kernel-claimable + kernel-held +
  userspace-held always sums to the pool size). Publication is per push,
  not batched: a release store is an ordinary store on x86, and the
  tighter window reduces `ENOBUFS` terminations.
- **Runtime fallback, not a hard floor**: registration is attempted at
  startup and failure degrades to the legacy `ProvideBuffers` path with
  a warn. This is not theoretical — a dev host reporting a 6.8 uname
  rejects `PBUF_RING` with `EINVAL` while accepting older register
  opcodes (a hypervisor/seccomp layer filtering newer `io_uring_register`
  opcodes). Deployments behind such filtering silently run the legacy
  path; the warn is the operator's signal.
- **Latent defect found and fixed during the migration**: on pool
  exhaustion the kernel completes the multishot RECV with `-ENOBUFS`
  (no data consumed), and the reader's CQE handler treated any
  non-positive result as a client error — a burst that exhausted the
  pool *disconnected* innocent clients. The handler now re-arms on
  `ENOBUFS`; the fix applies to both recycle modes.
- Kernel-referenced memory (buffer pool, buf_ring, eventfd buffer) is
  now declared before the `IoUring` so it outlives the ring fd on every
  exit path including panic unwind — previously the pool could be freed
  while armed multishot RECVs still referenced it during unwind.
- Tests: entry-layout and u16-tail-wrap unit tests; kernel-level
  integration tests (delivery through selected buffers, a recycle soak
  crossing the tail wrap under the kernel with sequence-stamped payload
  verification, exhaustion → `ENOBUFS` → re-arm recovery), runtime-
  skipped with a stderr notice on hosts that reject `PBUF_RING`; and an
  end-to-end reader-loop soak (4 connections × 10 000 single-write
  frames) asserting exactly-once, in-order delivery with no spurious
  disconnects in whichever recycle mode the host engages.

**Third review round (2026-08-07)**, on the buf_ring commit. The core
protocol (tail publication, wrap arithmetic, overrun accounting,
registration ordering) survived adversarial refutation. Findings fixed:

- **Slab-index reuse injection (pre-existing, the round's real
  defect)**: tearing down a connection (malformed frame, idle timeout)
  never cancelled its armed multishot RECV, and the slab's LIFO free
  list handed the index straight to the next registration. The armed op
  holds a kernel file reference past the fd close, so the repudiated
  peer's socket stayed live and its bytes were parsed under the *new*
  connection's identity, key hash, and permissions. Teardown now severs
  the peer (`shutdown(2)`), marks the entry dying, cancels the armed op
  (`AsyncCancel`), and frees the index only at the terminal CQE — dying
  entries' CQEs recycle their buffers and are otherwise discarded.
  Regression-tested end to end (repudiated peer keeps writing while a
  new client registers; nothing crosses streams).
- Buffers are now extracted and recycled on *every* CQE arm, including
  EOF/error CQEs (6.8 recycles those internally; the 5.19 floor could
  not be shown to) and defensive paths — a leaked buffer is permanent.
- Multishot bookkeeping (`F_MORE`) is recorded before any branch can
  `continue`, so no defensive skip can wedge a connection; the
  `ENOBUFS` re-arm guards on `F_MORE` against future kernels retrying
  the multishot themselves.
- Shutdown now quiesces (CANCEL_ANY + bounded CQ drain) before the
  kernel-referenced allocations leave scope — closing the ring fd alone
  triggers *asynchronous* teardown that could outlive the frees. Panic
  unwind accepts the residual window (process failing; ring fd still
  closes before the frees).
- The recycle bid check is a hard assert (per-CQE path, off the
  per-frame budget): an out-of-pool bid handed to the kernel would aim
  a future DMA at arbitrary heap.

## 4. Journal writes re-pin their buffer pages on every submit

The journal ring registers the file (`types::Fixed(0)`) and pins io-wq
workers off the busy-spinning core — clearly tuned — but submits with
`opcode::Write`, so every O_DIRECT write runs `get_user_pages` on the
batch buffer at submit time. The sector writer already double-buffers
through two stable, long-lived allocations: registering both once
(`register_buffers`, two slots) and submitting `WriteFixed` removes that
fixed per-batch cost from the durability-critical path.

Caveat to verify during implementation: registration freezes the buffer
addresses, so the buffers must never reallocate (grow) after
registration. If the encode path can grow a batch buffer, they must be
pre-sized to their maximum before registering.

## 5. `SINGLE_ISSUER` is set on two of the four rings

The journal ring and both replication rings set `setup_single_issuer`
(kernel skips SQ locking); the reader and response rings use plain
`IoUring::new`. Both are created and used by exactly one thread, so the
flag is free. `COOP_TASKRUN` / `DEFER_TASKRUN` are deliberately **not**
proposed: the journal ring documents a measured rationale against them
(CQEs posted from interrupt context are reaped from the mmap'd CQ with
zero syscalls; deferring task-work would add an `io_uring_enter` per reap
point), and the same logic plausibly applies to the reader. Do not cargo-
cult those flags in without a bench.

---

## What the audit confirmed is right

For the next person auditing this integration, the load-bearing things
that are already correct — don't regress them:

- **Durability gating** — the journal stage's consumer cursor advances
  only on CQE confirmation, and the response stage's durability gate
  reads that cursor. "Acked implies durable" is structural.
- **Buffer ownership transfer** — an in-flight journal batch owns its
  buffer until `confirm_async_write`; encode continues into the spare.
  No SQE ever references memory that can be concurrently mutated.
- **Copy at the ingress boundary** — recv bytes are copied out of the
  provided-buffer pool into connection parse buffers and then into
  fixed-size disruptor slots; kernel buffers recycle immediately and
  slot layout stays cache-friendly.
- **Inline-submission discipline on the journal** — O_DIRECT,
  sector-aligned writes, page prefaulting (so no io-wq punt on a page
  cache miss), registered file with `register_files_update` on segment
  rotation, io-wq affinity pinned off the busy-spin cores.
- **Tick generation via `IORING_OP_TIMEOUT`** — time advances during
  quiet periods with no extra thread or timer fd, and the timeout SQE's
  `Timespec` is kept alive across the submit (see the comment in
  `reader_loop` — the kernel reads it at submit time, not push time).
