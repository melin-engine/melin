# Raft integration — remaining work plan

> Contributor-facing plan for finishing the control-plane Raft work on
> `feat/raft-integration`. Covers every task that remains, the design decisions
> already made, the sharp edges discovered while shipping the earlier slices,
> and a test plan per task. Update this document as tasks land; delete sections
> once shipped (the roadmap carries the operator-facing summary).

## Where the branch stands

Shipped and pushed (roadmap items 15–17, all verified end-to-end):

- **Election + epochs** — `ControlNode` (`crates/core/raft/src/node.rs`) wraps
  `RawNode<FileStorage>` with the persistence contract handled in
  `drain_ready`; journal-tip recency vote filtering; term-derived fencing
  epochs; auto-promotion behind `--raft-auto-promote` with the refusal rules in
  `auto_promotion_decision` (`crates/core/server-runtime/src/raft_driver.rs`).
- **Membership registry** — `MemberRecord`/`Registry`
  (`crates/core/raft/src/registry.rs`, RECORD_VERSION = 2) as the replicated
  directory: raft addr, replication addr, order-entry addr, pinned public key.
  Announce loop re-proposes until applied; `sync_registry` folds applied
  records into dial targets, identity pins, and the `ClusterDirectory`.
  The registry is the **directory only**; voter authority stays in raft's
  `ConfState` (task 1 below adds the operator path to change it).
- **Follow-the-leader** — kernel-TCP replicas re-target the new leader's
  replication address via `LeaderFollow` (alternating leader-hint/static
  target). DPDK replicas keep their static target (task 5).
- **Client redirect** — replicas answer authenticated clients with
  `TransportResponse::Redirect{addr}` resolved from the leader's announced
  order-entry address (`crates/core/server-runtime/src/redirect.rs`); the
  native client follows up to `MAX_REDIRECT_HOPS = 3` with one overall
  deadline; the FIX gateway follows redirects too (generation-guarded io_uring
  lifecycle). `ServerBusy` while leaderless.
- **Gateway multi-seed upstream (task 1)** — the FIX gateway rotates
  through `server_addrs` and prefers a learned redirect target, so a dead
  seed no longer strands reconnects. Hands-off FIX failover with every
  cluster node listed as a seed.
- **Runtime voter-set changes (task 2)** — `RAFT-ADD-VOTER` /
  `RAFT-REMOVE-VOTER` grow, shrink, or re-identity the cluster live: joiner
  boots `--raft-join` with an empty `ConfState`, is admitted once its
  record and the `AddNode` commit, and catches up via log or a forced
  snapshot. Safety rails refuse the live leader, the last voter, and
  in-place re-key; an interrupted add is reclaimable via remove;
  follower-targeted commands forward to the leader. Durable cluster-wide
  before the command returns.
- **Serving-primary registry claim (task 3)** — records carry a
  `serving_epoch` claim (record v3); redirects and replica reconnect both
  resolve the highest live claim (fencing order, tie-broken by node id)
  rather than the raft leader, so a client dialing a replica — or a replica
  choosing whom to follow — lands on the node actually serving the journal,
  not a leader-replica that would only answer `ServerBusy`.
- **Deadline-IO unification (task 4)** — the per-syscall deadline helpers
  (`remaining_budget`, `DeadlineSocket`, `read_exact_deadline` /
  `write_all_deadline` / `read_frame_deadline`) live once in
  `melin-wire-protocol::blocking`; the redirect acceptor uses them (dropping
  its raw-fd `setsockopt` plumbing) and the client shares `remaining_budget`
  (the timeval-zero hazard is now defined in one place). Deferred: upgrading
  the *client's* buffered read path from per-frame to per-syscall re-arming —
  it needs a careful `BufReader` rework, not a mechanical swap.

Remaining, in recommended order (rationale at the end):

| # | Task | Size |
|---|------|------|
| 5 | DPDK follow-the-leader (+ DPDK promotion) | L (parked) |

Tasks 1–4 are shipped on this branch; their design records below are kept
for context and marked ✅.

---

## Task 1 — Gateway multi-seed upstream addresses ✅ shipped

### Problem

The FIX gateway (`crates/exchange/oe-gateway`) learns a new primary only via a
`Redirect` from a **live** node. Its config carries exactly one upstream
(`GatewayConfig::server_addr`). If that seed is the node that died, every
reconnect loops connect-refused against a corpse: session creation fails, the
FIX client retries, the gateway dials the same dead seed. An operator has to
edit the config and restart the gateway — the one manual step left in the
failover story for FIX clients.

### Design

- **Config**: add `server_addrs: Vec<SocketAddr>` (TOML list). Keep
  `server_addr` accepted as a single-entry alias for compatibility with
  existing configs; setting **both** is a validation error (ambiguous intent),
  and at least one entry is required. Extend the existing IPv4-only validation
  (`config.rs:86`) to every entry. Normalize to one `Vec<SocketAddr>` field on
  the parsed struct so the event loop never sees the alias.
- **Rotation state**: the event loop already keeps a gateway-global
  `melin_target: Option<SocketAddr>` (last redirect wins, documented). Add a
  gateway-global seed cursor (`usize` into the seed list — a plain index, not
  a per-session field: upstream reachability is a gateway-wide fact, and
  per-session cursors would make N sessions probe the same dead seed N times).
- **Connect-target selection** (one helper, unit-testable):
  1. `melin_target` if set (the learned primary — best information);
  2. otherwise `server_addrs[cursor]`.
- **On connect failure** (both the synchronous `connect()` error path and the
  failed-connect CQE in `handle_melin_connected`): if the failed address was
  the learned target, clear `melin_target` (it's dead — a redirect will
  re-teach it); if it was a seed, advance the cursor to the next seed. Either
  way the *next* session attempt tries the next candidate. Log at `debug!`
  (upstream-down is an expected condition the retry loop handles; the existing
  `warn!` on connect failure stays for the first observation).
- **Redirect interaction**: unchanged — a redirect from any live seed
  overwrites `melin_target`, which then takes precedence. With all cluster
  nodes listed as seeds, any survivor teaches the gateway where the primary
  went; that is the point of the task.
- **Docs**: `docs/oe-gateway.md` gains an operator paragraph: list every
  cluster node's order-entry address; the gateway finds the primary through
  any live one.

### Tests

Unit (config):
- `server_addrs` list parses; single `server_addr` still parses (alias);
  both set → error; empty list → error; IPv6 entry anywhere → error.

Unit (event loop, existing `gateway_with_session` harness):
- Target selection: learned target preferred over seeds; cursor starts at 0.
- Connect failure on a seed advances the cursor; failure on the learned
  target clears it and falls back to the seed cursor.
- Cursor wraps past the end of the list.

Integration (existing `test_stub.rs` infra, `StubMode::{Serve, RedirectTo}`):
- Two seeds: seed 0 not listening, seed 1 = stub in `RedirectTo(primary)`;
  a FIX session ends up connected to the primary stub (the full
  dead-seed → live-seed → redirect → primary chain).
- Primary dies mid-session, seed 0 still dead: reconnect walks to seed 1 and
  follows its redirect (failover mid-run, not just at boot).

---

## Task 2 — Runtime voter-set changes (`ConfChange` add/remove) ✅ shipped

### Problem

The voter set is fixed at boot: `server.rs` builds
`RaftDriverConfig::voters = [self ∪ peers]` from `--raft-peer` flags
(`crates/core/server-runtime/src/server.rs:3298`), and
`ControlNode::open` bootstraps `ConfState` from it on first boot. Growing
3→5, shrinking, or replacing a node under a **new identity** requires a
coordinated full-cluster restart. The apply path for committed
`EntryConfChange` entries already exists (`node.rs::apply_committed`) — what
is missing is the propose path, the operator command, the join flow for a
fresh node, and safety rails.

### What already works without this task (document, don't rebuild)

Replacing a dead node **with the same node id and replication key at a new
address** is already shipped: boot the replacement with the old key, its
announce re-proposes its `MemberRecord` with the new addresses, and
`sync_registry` re-dials it everywhere. No voter change involved. The admin
guide should state this recipe explicitly — task 2 is only needed when the
*identity* (id or key) changes or the voter count changes.

### Pre-implementation verifications (raft-rs semantics)

The design below assumes specific raft-rs behaviors. **Verify each against the
vendored checkout** (`~/.cargo/git/checkouts/raft-rs-8d904e7e48a482b3/`,
rev `ad13f3d`) before coding, and encode each answer as a sim test:

1. `RawNode::new` on storage with an **empty** `ConfState` (the joiner
   pattern): accepted, node stays a passive follower until the leader's
   append/snapshot delivers membership.
2. `propose_conf_change` on a **follower** is forwarded to the leader like any
   `MsgPropose` (the registry announce path already relies on forwarding for
   normal entries; confirm conf-change entries forward too).
3. Only one conf change may be in flight: a proposal while another is pending
   (appended, not yet applied) is silently replaced by an empty entry on the
   leader. Consequence: a dropped change surfaces as an observation timeout,
   never as two racing changes.
4. `apply_conf_change` error cases: duplicate `AddNode` for an existing voter,
   `RemoveNode` for an absent id, removing the **last** voter. Whichever of
   these error rather than no-op must be prevented by validation (rails below)
   and absorbed by the apply-path hardening.
5. Behavior of a removed-but-still-running node: with `pre_vote` +
   `check_quorum` it cannot disrupt the cluster (its pre-votes never reach
   quorum). Confirm, then document "stop the process after removing it" as an
   operational note, not a safety requirement.

### Design

**Apply-path hardening first (its own commit).** Today an
`apply_conf_change` error in `apply_committed` maps to `io::Error`, which
`drain_node` treats as a storage failure — the driver **halts the control
plane**. Because every node applies the same committed entry, one bad conf
change would brick raft **cluster-wide, deterministically**. Change the two
conf-change arms to: on `apply_conf_change` error, `error!` (it means our
validation failed — a bug), stage the entry as applied, do **not** touch
`ConfState`, and continue. Skipping converges (every node skips identically);
halting the whole control plane does not. This is a pre-existing hazard worth
fixing even before the propose path exists — any future raft-rs version bump
or bit flip in a committed entry hits it.

**`ControlNode` API (melin-raft):**

```rust
/// Current voter set, ascending. Read from the persisted ConfState.
pub fn voters(&self) -> Vec<u64>;
/// Propose adding/removing a voter. Same contract as propose_member:
/// `true` = accepted for append/forwarding, never "committed" —
/// callers observe `voters()` until it reflects the change.
pub fn propose_add_voter(&mut self, node_id: u64) -> bool;
pub fn propose_remove_voter(&mut self, node_id: u64) -> bool;
```

Both build an `eraftpb::ConfChange` (`AddNode`/`RemoveNode`) and call
`raw.propose_conf_change(vec![], cc)`; map the error to `debug!` + `false`
exactly like `propose_member` (leaderless is expected, the caller retries).

**Join flow.** A fresh node must boot with an **empty** voter bootstrap — a
node bootstrapped with a guessed voter set has a divergent `ConfState` that
raft never reconciles. Add a server flag `--raft-join`: keep `--raft-node-id`,
`--raft-bind`, `--raft-peer` (dial targets + identity pins for the existing
members) but pass `voters = []` into `RaftDriverConfig`.
`initialize_with_conf_state(vec![])` leaves `FileStorage::initialized()`
false (empty ConfState is the default), so re-running it on every boot is a
no-op — verify and pin with a storage test.

The bootstrap chicken-and-egg: the joiner dials the members, but their
inbound auth rejects a key that maps to no id (`pubkey_to_id`), and nobody
dials the joiner. The fix is that the **admin command carries the seed
identity** and the driver proposes a seed `MemberRecord` (raft addr + key,
`replication_addr`/`order_entry_addr` = `None`) *before* the `ConfChange`.
Once the seed record applies: members' `sync_registry` pins the key, creates
a `PeerLink`, and dials the joiner; the joiner's own dials get accepted; the
leader can then replicate to it. After the `ConfChange` applies, the joiner
receives appends/snapshot (snapshot data already carries the registry), and
its own announce loop upgrades the seed record with its full addresses.

Sequencing inside the driver (a two-stage pending state machine):

```
AddVoter: propose seed record → registry.get(id).is_some()
        → propose ConfChange AddNode → voters().contains(id) → reply OK
RemoveVoter: propose ConfChange RemoveNode → !voters().contains(id) → reply OK
```

Each stage re-proposes on `ANNOUNCE_RETRY_INTERVAL` (2 s) until observed
(proposals are ack-less and lost to leader churn — same rule as the announce
loop), with one overall deadline (10 s) after which the driver replies
`ERR not committed within 10s — check cluster health and retry`.

**Operational prerequisite (document loudly):** `AuthorizedKeys` is loaded at
boot and immutable per process (established during the 2c fingerprint work).
The joiner's replication key must already be present in every member's
`authorized_keys` file — provision spare cluster keys at deploy time, or a
member restart is needed before ADD. The seed record solves id mapping, not
key authorization.

**Admin plumbing.** New request type in `raft_driver.rs`:

```rust
pub enum VoterChange {
    Add { node_id: u64, raft_addr: SocketAddr, public_key: [u8; 32] },
    Remove { node_id: u64 },
}
pub struct VoterChangeRequest {
    pub change: VoterChange,
    /// One reply per request: resulting voter set, or the refusal.
    pub reply: mpsc::Sender<Result<Vec<u64>, String>>,
}
```

`server.rs` creates the `mpsc::channel` when raft is configured; the
`Receiver` rides `RaftDriverContext`, the `Sender` goes to `admin::spawn` as a
new `Option<...>` capability (same pattern as `promote`/`rotate`/
`durability_mode`; `None` on raft-less nodes → structured
`ERR ... not available on this node`). The driver drains the channel once per
loop iteration; at most **one** pending change at a time — a second request
while one is in flight gets an immediate
`ERR another voter change is in flight` (raft only admits one pending conf
change anyway; see verification 3).

Admin text protocol (parsed in `handle_connection`, same line discipline as
`DURABILITY`):

```
RAFT-ADD-VOTER <node_id> <raft_addr> <pubkey_b64>
RAFT-REMOVE-VOTER <node_id>
→ OK voters=1,2,3,4
→ ERR <reason>
```

The admin handler blocks on `reply.recv_timeout(15s)` (driver deadline is
10 s, so the driver always answers first; verify the `melin-admin` client's
own read timeout exceeds 15 s and raise it if not). `melin-admin` gains
`raft-add-voter`/`raft-remove-voter` text commands + menu entries mirroring
the `operator-policy` shape.

**Safety rails** (validated in the driver on request receipt, before any
proposal — the apply-path hardening is the backstop, not the primary defense):

- `node_id == 0` → ERR (raft's invalid-id sentinel).
- Add: id already a voter **and** registry has its record → idempotent
  `OK voters=...` (scripting-friendly).
- Add: pubkey already pinned to a *different* id → ERR (one key, one
  identity — the registry map is keyed by pubkey).
- Remove: id not a voter → idempotent OK.
- Remove: id is the **current leader** and reachable → ERR
  `node <id> currently leads — stop it and let the cluster elect first`.
  This only bites for a *live* leader: the dominant use case (removing a dead
  ex-leader) is fine because leadership has already moved by then.
- Remove: last remaining voter → ERR (would brick consensus; also protects
  against verification-4 apply errors).
- Removing *this* node or the serving primary is allowed (control-plane-only
  effect); document that a removed node keeps serving the data plane and that
  auto-promotion can no longer elect it.

**Non-goals (parked, note in the doc):** registry tombstones (a removed
node's directory record lingers; members keep re-dialing it at 1 s backoff —
bounded `debug!` noise, acceptable); joint consensus / multi-node changes
(single-node changes only, which raft serializes anyway); a `RAFT-STATUS`
admin query (the health endpoint's gauges cover observability for now — add
voters to `/metrics` if the E2E wants it, it's a 5-line change).

### Tests

Sim tests (`node.rs::sim` — the deterministic in-process cluster; this is
where the correctness weight goes):
- `add_voter_grows_the_cluster`: 3 nodes elect; open node 4 with
  `voters = &[]`; leader proposes 4's seed record then `AddNode`; step until
  all four agree on `voters() == [1,2,3,4]` and node 4's registry matches the
  leader's; then partition-kill the old leader and assert a quorum of the
  *new* set elects (the added voter actually votes).
- `joiner_catches_up_via_snapshot`: same as above but push
  `LOG_RETENTION + N` records through before wiring node 4 in, so it can only
  join via snapshot (pins that `ConfState` + registry ride the snapshot).
- `remove_voter_shrinks_the_cluster`: 3 → 2; assert the removed node stops
  receiving appends and the remaining 2 still elect after killing one…
  (2-voter quorum = 2 — assert instead that the pair keeps a stable leader).
- `replace_a_dead_node`: kill node 3, add node 4 (new key), remove node 3;
  survivors + node 4 elect and converge. The "replace" headline scenario.
- `conf_change_survives_restart`: apply an add, crash-reopen a follower from
  its dir, assert `voters()` reflects the change (pins the
  `stage_applied`-before-`set_conf_state` atomicity contract).
- `duplicate_add_is_harmless` / `remove_absent_is_harmless`: encode whatever
  verification 4 finds — either no-op applies or validation-refused.
- Re-propose loop: drop the first proposal (partition the leader briefly),
  assert the pending machinery retries and converges.

Unit (raft_driver):
- The pending-change state machine, factored so it is drivable without
  sockets: two-stage add ordering (ConfChange never proposed before the seed
  record is observed), deadline → ERR reply, second request while pending →
  immediate ERR, every rail above (leader, last voter, id 0, key conflict,
  idempotent paths).

Unit (admin.rs, existing listener harness):
- Parse/dispatch both commands (valid, malformed id, malformed pubkey,
  missing args); `ERR ... not available` when the capability is `None`;
  reply-timeout path (a test-side receiver that never replies).

Unit (storage.rs):
- `initialize_with_conf_state(vec![])` leaves `initialized()` false and is
  idempotent across reopen.

E2E (`failover.rs`, real processes — plumbing proof, not correctness depth):
- `voter_replacement_without_cluster_restart`: 3-node cluster (P + 2
  replicas, auto-promote); SIGKILL replica 3; boot node 4 with `--raft-join`
  (its key pre-provisioned in the shared `authorized_keys`);
  `RAFT-ADD-VOTER 4 …` → `OK voters=1,2,4` (after the remove:
  `RAFT-REMOVE-VOTER 3` → `OK voters=1,2,4`); then SIGKILL the primary and
  assert the surviving pair elects + auto-promotes + serves a fill — the
  replaced cluster is actually functional, not just reconfigured.
- Negative: `RAFT-ADD-VOTER` against a leaderless cluster (kill 2 of 3
  first) → `ERR not committed within …`.

---

## Task 3 — Serving-primary registry claim (redirect gap) ✅ shipped

### Problem

Redirects resolve the raft **leader's** order-entry address
(`redirect.rs:220` → `LeaderFollow::leader_order_entry_addr`). Under the
primary-link-up auto-promotion veto, a replica can hold raft leadership while
the old primary keeps serving (leadership landed on a connected replica
because a *different* node's raft died, or plain election randomness at
boot). In that healthy topology a client that dials a replica is bounced to
the leader-replica, whose own acceptor answers `ServerBusy` — while the real
primary would have served it. Documented and bounded, but wrong: the cluster
knows who serves; the directory just doesn't carry it.

### Design

Extend `MemberRecord` with a **serving claim** (RECORD_VERSION 2 → 3):

```rust
/// The fencing epoch under which this node acts as serving primary;
/// `None` while it is a replica. Self-reported, leader-serialized.
/// Resolution picks the LIVE claim with the highest epoch — fencing
/// order, not announcement order, decides, so a superseded primary's
/// stale claim is outranked the moment the new primary announces.
pub serving_epoch: Option<u64>,
```

Why an epoch and not a bool: fencing already totally orders primaries.
A deposed primary that never hears about its supersession keeps its old claim
in the directory — but the promoted node announces a strictly higher epoch,
so max-by-epoch resolution is self-healing with **no tombstones and no
revocation path**. A fenced node halts anyway (fence = shutdown), and its
lingering record loses every comparison. Dual *manual* promotions can collide
on the same epoch (pre-existing documented hazard); tie-break
deterministically on lower node id and leave the real fix to the epoch
allocation story.

Implementation points:

- **Record v3**: bump `RECORD_VERSION` to 3; encode `serving_epoch` as
  8 bytes + presence byte (or 0-length/8-length prefix, matching the codec's
  conventions) **after** `order_entry_addr`, before `public_key`. v1/v2 (and
  the two interim-v1 shapes) decode with `serving_epoch = None`. This time
  the version byte is bumped in the same commit as the layout change — the
  interim-v1 mess came from shipping a layout change without the bump.
- **Announce**: `self_record` is currently built once before the driver loop
  (`raft_driver.rs::run`). The serving claim changes at runtime (promotion),
  so compute it per iteration: serving iff this node acts as primary —
  `context.replica.is_none()` (genesis primary) **or**
  `replica.promote.is_requested()` (promotion in flight/complete) — with
  `epoch = context.fence_state.epoch()`. The announce condition
  (`registry.get(id) != Some(&self_record)`) already re-announces on any
  field change, so promotion triggers the upgrade automatically within one
  `ANNOUNCE_RETRY_INTERVAL`. Note the epoch read must be the post-bump value:
  `run_as_primary` journals `EpochBump` before the replica signals promotion
  completion — verify the ordering and, if the bump lands late, announce on
  the next iteration (the loop re-evaluates every 10 ms; eventual is fine).
  A genesis primary announces its claim from the first iteration (epoch
  seeded by journal recovery before the driver spawns).
- **Resolution**: `ClusterDirectory::serving_primary(&self) ->
  Option<(u64, SocketAddr)>` — max `serving_epoch` over records that carry an
  order-entry address, tie-broken by lower node id. `LeaderFollow` gains
  `serving_primary_order_entry_addr()` with the same self-exclusion rule as
  `leader_field` (if the max claim is *this* node, return `None` — a
  redirecting replica must never point clients at itself).
- **Redirect acceptor** (`redirect.rs:220`): resolve the serving primary
  first, fall back to `leader_order_entry_addr()` (post-failover the two
  agree; the fallback covers a v2-only directory mid-upgrade), then
  `ServerBusy` as today.
- **Replica reconnect** (optional refinement, separate commit):
  `tcp_receiver.rs:529` currently alternates leader-hint/static. Prefer the
  serving-primary hint over the leader hint in the alternation — it is the
  strictly better signal for "whom do I replicate from". Keep the
  alternation; a bad hint must still cost one dial, never a wedged loop.
- **Docs**: `docs/replication.md` "Clients follow the leader too" section —
  reword to "clients follow the serving primary"; drop the documented
  pre-failover busy-bounce caveat.

### Tests

Unit (registry.rs):
- v3 round-trip with/without claim; v2 decodes with `serving_epoch = None`
  (bytes hand-built, as the existing v1 tests do); v1 + both interim-v1
  shapes still decode (regression — the remainder-length disambiguation must
  survive the new tail field); newer-version skip still works.

Unit (raft_driver):
- `ClusterDirectory::serving_primary`: empty, single claim, competing claims
  (higher epoch wins regardless of node-id order), tie → lower id, claimant
  without order-entry addr skipped, self-exclusion in the `LeaderFollow`
  accessor.
- Announce upgrade: drive the self-record construction (factor it into a
  function of `(config, promote_state, fence_epoch)`) — replica → no claim,
  promotion requested → claim at the current epoch.

Unit (redirect.rs, existing `UnixStream::pair` harness):
- Serving claim beats leader id: directory where node A leads but node B
  holds the claim → redirect answers B's address.
- No claim anywhere → falls back to leader; neither → `ServerBusy` (existing
  tests keep passing).

E2E (failover.rs):
- Extend the existing auto-promote E2E with a **pre-failover** redirect
  step: before killing anything, a client dials a replica and must land on
  the genesis primary with `Placed` — deterministically, regardless of which
  node happens to hold raft leadership at that moment. (Today this step
  would be flaky-or-busy; with the claim it is exact.) The post-failover
  step already asserts the new primary's claim supersedes.

---

## Task 4 — Deadline-IO unification ✅ shipped (client per-syscall upgrade deferred)

### Problem

Three hand-rolled deadline-bounded blocking-IO implementations agree on
semantics only because review forced them to:

1. `redirect.rs:253–340` — `remaining_budget` (1 ms floor, `TimedOut` when
   spent), `arm_timeout` (raw-fd `setsockopt` via
   `server.rs::set_socket_timeout`), `read_exact_deadline` /
   `write_all_deadline` (re-arm per syscall; `WouldBlock`/`TimedOut` →
   re-check budget; `Interrupted` → continue).
2. `client/src/tcp.rs:164–200` — `remaining` / `arm_read_deadline`
   (per-frame re-arm via `TcpStream::set_read_timeout`), plus
   `send_request_with_deadline`.
3. `replication/auth.rs` — the DPDK tick-stepper (`AuthChallenge.deadline`
   checked across poll ticks). **Out of scope**: it is genuinely
   non-blocking (shared poll thread, no socket timeouts) — forcing it into a
   blocking helper would be a regression, not a unification.

The timeval-zero hazard ("0 remaining = block forever") is documented twice,
and the next handshake surface (market data, drop-copy) will copy whichever
variant its author finds first.

### Design

New items in `melin-wire-protocol::blocking` (beside
`BlockingFrameReader`/`Writer`, which is where every blocking-transport
consumer already looks):

```rust
/// Remaining budget until `deadline`, floored at 1 ms — a zero timeval
/// means "no timeout" to setsockopt, so a sub-ms remainder must round
/// UP to a real timeout, and a spent budget is TimedOut, never zero.
pub fn remaining_budget(deadline: Instant) -> io::Result<Duration>;

/// Socket-timeout arming, so the deadline helpers are generic over
/// TcpStream / UnixStream (tests) without raw-fd plumbing.
pub trait DeadlineSocket: io::Read + io::Write {
    fn arm_read_deadline(&mut self, dur: Duration) -> io::Result<()>;
    fn arm_write_deadline(&mut self, dur: Duration) -> io::Result<()>;
}
// impls for TcpStream and UnixStream delegate to set_read_timeout /
// set_write_timeout(Some(dur)).

pub fn read_exact_deadline(s: &mut impl DeadlineSocket, buf: &mut [u8], deadline: Instant) -> io::Result<()>;
pub fn write_all_deadline(s: &mut impl DeadlineSocket, buf: &[u8], deadline: Instant) -> io::Result<()>;
/// Length-prefixed frame read with a cap, re-armed per syscall.
pub fn read_frame_deadline(s: &mut impl DeadlineSocket, max_len: usize, deadline: Instant) -> io::Result<Vec<u8>>;
```

Then:

- `redirect.rs` drops its four private helpers and the raw-fd
  `set_socket_timeout` dependency; behavior byte-identical (per-syscall
  re-arm).
- `client/tcp.rs` drops `remaining`/`arm_read_deadline` and reads frames via
  `read_frame_deadline`; this *upgrades* the client from per-frame to
  per-syscall re-arming — strictly tighter, and the within-frame-granularity
  doc caveat on `connect_following_redirects` gets deleted.
- `server.rs::set_socket_timeout` stays only if the reader hot path still
  needs the raw-fd form (it takes an fd, not a stream); if the redirect
  acceptor was its last non-hot-path user, shrink its visibility.
- Kernel-TCP auth handshakes (`authenticate_with_primary`,
  `authenticate_replica_identified`) currently use whole-connection
  `set_read_timeout` — converting them is optional; do it only if it is a
  pure mechanical swap.

No behavior change intended anywhere except the client granularity upgrade —
state that in the commit message.

### Tests

- Move the redirect deadline tests (9, `UnixStream::pair`-based) into
  `blocking.rs` against the new helpers, keeping thin call-site tests in
  redirect.rs (one happy path, one timeout) to prove the wiring.
- New: spent-budget → `TimedOut` without a syscall; sub-ms remainder arms
  1 ms (never zero); budget shrinks across consecutive reads (a peer that
  trickles one byte per almost-timeout cannot hold the connection past the
  deadline — the exact attack the per-syscall re-arm exists for);
  `Interrupted` retries without budget loss beyond elapsed time;
  `read_frame_deadline` rejects frames over `max_len` and short frames.
- Client: existing `overall_deadline_bounds_busy_retries` /
  `redirect_loop_is_bounded` / heartbeat-stream tests keep passing unchanged
  (they pin the observable contract).
- Full feature-matrix compile (the client is used by admin/TUI/bench; the
  helpers must not drag server-only deps into wire-protocol).

---

## Task 5 — DPDK follow-the-leader (parked)

Parked by commercial strategy: kernel-TCP is what real buyers run; revisit
when a diligence process or demo asks. Recorded here so the design intent
isn't lost.

- **Pairs with DPDK promotion** — re-targeting is pointless while a DPDK
  replica cannot take over as primary anyway. Do promotion first.
- Current state: `run_receiver_dpdk` takes the `LeaderFollow` param
  (signature parity, `dpdk.rs:1100`) but warns and keeps the static
  `--replica-of` target.
- Re-target sketch: on leader change, tear down the smoltcp TCP socket;
  if the new target is on a different next-hop/IP, the smoltcp interface
  needs neighbor (ARP) resolution for it — issue the resolution via the
  interface's poll loop and wait for the neighbor cache to fill before the
  SYN (smoltcp returns `Unaddressable` on a cold cache); then rebuild the
  session: TCP handshake → replication auth → `StreamStart` catch-up, which
  is the existing reconnect path above the transport.
- The alternation rule (leader-hint / static, one dial per attempt) must
  survive the port: a bad hint costs one dial cycle, never a wedge.
- Test approach: `--features dpdk --no-default-features` compile matrix
  (both `melin-server` variants + `melin-bench`); unit-test the target
  selection logic shared with the kernel receiver; actual traffic
  validation needs the DPDK lab hosts — schedule a lan-bench-suite failover
  session (the suite runs from the local machine over SSH).

---

## Recommended order and commit slicing

Tasks 1 and 2 shipped in that order — the gateway multi-seed completed the
FIX failover story from the redirect slice, and voter-set changes closed the
last functional gap in roadmap item 15. **3 → 4 → 5** remain: the serving
claim is a correctness fix that builds on the registry versioning mechanics
task 2 introduced; the deadline refactor is hygiene that got cheaper once
tasks 1–2 stopped touching the same files; DPDK stays parked.

Commit slicing (each verified — `cargo fmt`, clippy-clean, full nextest, the
DPDK/skip-order-exec feature matrix, affected E2Es — before moving on):

1. `feat(oe-gateway): rotate through multi-seed upstream addresses`
2. `fix(raft): survive a failed conf-change apply` (hardening, standalone)
3. `feat(raft): propose voter conf changes + joiner bootstrap` (ControlNode
   API, `--raft-join`, sim tests)
4. `feat(server-runtime): RAFT-ADD-VOTER / RAFT-REMOVE-VOTER admin commands`
   (driver state machine, admin plumbing, melin-admin, E2E)
5. `feat(raft): serving-primary claim in the membership registry` (record
   v3, announce, resolution, redirect + E2E)
6. `refactor(wire-protocol): shared deadline-IO helpers` (+ the two
   call-site migrations; client granularity change called out)

Conventions that repeatedly mattered on this branch: no `.unwrap()` outside
tests; comment on any discarded `Result`; justify collection/type choices;
`debug!` for peer/client-caused conditions, `warn!` for degraded-but-handled,
`error!` only for genuine bugs; registry/wire layout changes bump the version
byte **in the same commit**; E2E green ≠ no crash — grep the spawned servers'
logs for `panicked` before trusting a failover run.
