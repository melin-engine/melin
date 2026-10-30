# Control-plane raft: decided follow-up work (contributor notes)

Work on the raft control plane that has been designed and decided but
not built. It was recorded while the leader-lease gate on auto-promotion
was implemented (branch `fix/raft-promotion-quorum-lease`; the gate
itself is described in `raft-control-plane.md`, "The leader-lease gate",
under "Auto-promotion policy"). Each item gives the problem, why it
matters, the decided design and what was rejected. Function names are the
anchors; line numbers are left out because they go stale.

The first three items are one follow-up branch, **in this order**: the
raft protocol version (done), then mutual authentication in the same
handshake change (done), then the stale-peer-tip stamp, which is the
first envelope change the version protects (done). The one-sided
partition gap after them turned out, by test, to need no tick inside the
failover grace, and is closed by configuration for a uniformly configured
cluster (item 4); its envelope fix has only been sketched, for mixed or
slow-ticking deployments.

## 1. A raft protocol version (done)

Built as decided here, and described in `raft-control-plane.md`
("RPC transport", "The link carries a protocol version"):
`RAFT_PROTOCOL_VERSION` in
`crates/core/raft/src/wire.rs`, exchanged both ways by
`exchange_version` after authentication, in a fixed three-byte frame
that releases from before it refuse as an undecodable `RpcFrame`
(pinned against a frozen copy of their decoder in
`crates/core/raft/src/tests/pre_version_wire.rs`).

What it meant for item 3: it would have bumped `RAFT_PROTOCOL_VERSION`
(it changes the envelope) had a release shipped version 1; none had, so
version 1 absorbed it as it absorbed item 2 (a version only has to change
once a release has shipped it).
The version frame's own layout never changes. It needs no
frozen-decoder test of its own: from version 1 on, a peer reads the
version before anything else.

## 2. Mutual raft authentication (done)

Built as decided here, and described in `raft-control-plane.md` ("RPC
transport"). The problem it closed: authentication was one-way, so
anything answering at a peer's raft address could send a reply with a
huge `tip_epoch` and make a serving primary fence and shut down (the
reply path feeds every reply's epoch to `SupersessionPolicy::observe`),
or feed `PeerTips` a tip of its choosing. Out of scope, as before: an
active attacker on the path, who could relay both challenges and rewrite
the frames after them, since raft frames carry no MAC binding them to the
session; the control-plane network is assumed private.

- After the version exchange the dialer challenges the responder
  (`challenge_responder` in `crates/core/raft/src/auth.rs`), through the
  same verification as the responder's challenge of the dialer
  (`verify_challenge_response_identified`, now returning a typed
  `KeyRefusal`), and accepts only the key the `--raft-peer` table maps
  to the id it dialled. That table is the server's own key → id map,
  one `Arc` shared with `RaftClientFactory`; the stored membership's
  node is an address only, so a stored node that no `--raft-peer` entry
  names is not dialled at all.
- No version bump: `RAFT_PROTOCOL_VERSION` 1 came in on the same
  unreleased branch (no release tag carries it), so version 1 is
  defined as including mutual authentication.
- A refusal is logged at `warn` on the dialer and maps to `Unreachable`;
  it happens before any request is sent, so no reply is applied. The one
  exception is a dialled node no `--raft-peer` entry gives a key to: it
  is logged at `debug` per attempt, since the driver already warns once
  at startup that the stored membership differs from the configured list.
- Tests: `auth.rs` (the dialer's challenge: the right key, another
  configured peer's key, a signature over another nonce, a malformed
  response, a stray replication key, no configured key) and
  `src/tests/rpc_loopback.rs` (a real server proving an unknown key, an
  operator key, the other peer's key or a stray replication key is
  refused as unreachable, with no fence and no peer tip recorded; a node
  with no configured key is never connected to; the accepted replies of
  the reply-path tests record the tip).

## 3. Stale peer tips (done)

Built as decided here, and described in `raft-control-plane.md` ("Peer
tips are ordered by a sender stamp"): `SenderStamp` on every `RpcFrame`,
taken with the tip by `TipSource::stamp`; `PeerTips::record` drops a frame
not newer than the one recorded, tip and last-heard time alike; the boot
identity is `BootStamp` in `melin-transport-core`, read once by
`spawn_raft_driver`, whose failure refuses startup. The stamp gates the
peer-tip record only, not the epoch fed to `SupersessionPolicy::observe`
nor the vote filter's candidate tip (the design document says why). No
version bump: version 1 was still unreleased, so it absorbed the envelope
change as it absorbed item 2. The design as decided follows.

**Problem.** `PeerTips::record_at` (`crates/core/raft/src/recency.rs`)
overwrites unconditionally and stamps the *arrival* time. An old frame
arriving after a newer one on another connection to the same peer (TCP
retransmission after a real partition; openraft opens a fresh client per
voter for leadership confirmation, so one peer can have several
connections) regresses the stored tip and makes it look fresh. If that
makes the faster replica look level with the slower one while the slower
one decides, the peer-tip veto in `auto_promotion_decision` passes and
**acknowledged writes can be lost**. The reply path is bounded by the RPC
timeout (the client drops its stream on it); the request path is not.
Unlikely, but the outcome is the worst class, which is why it gets a
design of its own rather than a heuristic.

**Design (decided).**

- The sender stamps every envelope, request and reply, with a **boot
  identifier** and a **per-frame counter**. The counter is taken in the
  same critical section as the tip read, so counter order is tip-read
  order. (Today `TipSource::local_tip` reads the fence epoch and the
  advertised sequence as two independent loads, and `RaftClient::request`
  and the RPC server's reply path (`handle_connection`) each call it; the
  stamp needs one place that
  reads the tip and takes the next counter value together.)
- The receiver keeps, per peer, the newest `(boot identifier, counter)`
  it has accepted, and **drops** any frame older than it. A dropped frame
  does not refresh the peer's last-heard time either, or a stale frame
  would keep a dead peer `Fresh`.
- The boot identifier is a **kernel boot stamp, not persisted**: the
  host's `/proc/sys/kernel/random/boot_id` plus the process start time
  read from `CLOCK_BOOTTIME`. Within one host boot, process starts are
  strictly ordered (`CLOCK_BOOTTIME` is monotonic and is not stepped by
  NTP), so a stale frame from an earlier process of the same node is
  older and dropped. Across host boots, a different `boot_id` replaces the
  stored one: the old kernel is gone, so no frame from the old boot can
  still be in flight.
- The helper lives in `melin-transport-core`, which already depends on
  `libc`. An unreadable `boot_id` **refuses startup** rather than falling
  back to something weaker.
- A VM snapshot restore rewinds clocks, memory and disk together. That is
  a hazard the node shares with its journal; it belongs in the operator
  docs, not in this design.

**Rejected, and why.**

- *A persisted counter.* Losing or rewinding that state (a restored disk,
  a deleted file) makes every peer drop every frame from the node: it
  looks `Silent`, the peer-tip veto skips `Silent` peers, and the bug this
  fixes returns, silently.
- *A random per-process nonce.* Nonces are unordered: after a restart a
  stale frame from the previous process regresses the tip for a
  heartbeat, which is the bug again. A "retire the old nonce" rule can
  instead drop a live process for good.
- *Heuristics without a stamp*, considered before the decision: ignoring
  a sample whose tip is behind the stored one within the same epoch (a
  peer's tip regresses legitimately only on a re-seed, which changes its
  lineage), or stamping tips with the sender's raft term and dropping
  older terms. Neither orders frames within a term or an epoch, which is
  exactly where the stale frame sits.

**Tests.** A frame older than the newest accepted from a peer is dropped
and does not refresh its last-heard time; a newer one is accepted; a new
`boot_id` replaces the stored identifier; a later process start on the
same boot wins over an earlier one; the counter and the tip are taken
together (a unit test over the stamping helper); startup refuses when
`boot_id` cannot be read (through a seam over the path).

## 4. The one-sided partition with an idle primary (closed by configuration: auto-promotion refuses ticks outside the failover grace)

**Gap.** The replica raft leader `L` loses only its link to the primary,
while the other replica `O` still reaches both. `L` keeps a fresh lease
(`O` acks it), so the leader-lease filter and the mint-time confirmation
both pass. Whether `L` then deposes the primary depends on whether the
primary journals anything during `PRIMARY_DOWN_GRACE`:

- if it does, `O`'s tip moves ahead of `L`'s and the peer-tip veto
  refuses for as long as `O` stays ahead. `O` does not campaign, because
  the challenger nudge needs `O`'s own primary link down
  (`challenger_should_campaign`), so nobody promotes and the primary keeps
  serving. A primary is never idle in this sense by default: it journals a
  clock tick every `--tick-interval-ms` (default 250 ms), well inside the
  grace. The chain: the ingress thread of a primary (the reader under
  `run_as_primary`, the poll thread under `run_as_primary_dpdk`; a replica
  runs neither) publishes
  `JournalEvent::Tick` onto the input ring (`tick::publish_tick`); the
  journal stage sequences and journals it like any entry, and replication
  streams it in an `InputBatch`; `O`'s receiver advances its
  `AdvertisedJournalTip` to the *accepted* position as it publishes the
  batch into its ring, before the entry is persisted
  (`receiver_transport.rs`); `TipSource::stamp` reads that tip into every
  envelope, so `O`'s next heartbeat reply to `L` carries it, while `L`'s
  tip, its replication link cut, stays where it was. Pinned by
  `replica_leader_cut_from_the_primary_alone_while_it_is_idle` in
  `crates/core/server-runtime/tests/raft_failover.rs`: no write, the
  primary at the slowest cadence plus group-commit window auto-promotion
  accepts, run once with all of it in the cadence and once with all but
  a few milliseconds of it in the window, so that the window outlasts
  the cadence and the sum, not the larger of the two, is what is tested;
  with `RUST_LOG` set, `L`'s refusal once the grace
  ends is the peer-tip veto's. It had a twin with writes flowing and no
  tick inside the test, which pinned that writes alone suffice; it went
  with the cadence bound below, which forbids that configuration on an
  auto-promoting node, leaving the twin nothing the idle test does not
  pin;
- if it does not, that is with ticks disabled (`--tick-interval-ms 0`) or
  too slow for one to land inside the grace, and no client write, the tips
  stay equal, nothing vetoes, and `L` promotes and deposes a primary the
  rest of the cluster can see. Confirmed by
  running the idle-primary test once with the primary's ticks disabled (a
  scratch change, not kept): `L` auto-promoted as the grace ran out and
  minted its epoch. Not kept as a test, since it would pin the defect.

This is the documented one-sided-link limitation of `PRIMARY_DOWN_GRACE`
(the link flag is this node's socket only). The same partition also
reaches the one case the mint-time argument leaves open (see
`raft-control-plane.md`): `L` steps down after its confirmation and
before the mint, and the newer leader is the primary, which never mints.
With ticks inside the grace, the peer-tip veto keeps `L` from reaching
its confirmation through this partition at all, so reaching that case this
way needs ticks disabled or too slow as well.

**Closed by configuration (decided by the maintainer).** Ticks disabled is
a benchmark option, and a cadence too slow for a tick to land inside the
grace is as exposed, so auto-promotion supports neither: a node refuses to
start with `--raft-auto-promote` and a `--tick-interval-ms` of 0 or above
`MAX_AUTO_PROMOTE_TICK_DELAY`, half of `PRIMARY_DOWN_GRACE`, or whose
`--group-commit-us` window, which every tick waits out before it
replicates, takes the sum above it (`build_raft_config` in
`crates/core/server-runtime/src/raft.rs`, beside the three-voter refusal,
pinned by `auto_promote_refuses_ticks_outside_the_failover_grace`; the
bound's derivation and its compile-time assertion are beside the constant
in `raft_promotion.rs`, summarised in `raft-control-plane.md`). The
refusal is per node, on every node that sets the flag, though the tick
that protects is the primary's and the promotion that harms is a
replica's: any node may become the primary, and no node can see another's
flags. So it closes the gap for a cluster whose nodes share one
configuration, the normal deployment, and `docs/replication.md` states
the rule for the rest ("Run every node of a raft cluster with clock ticks,
plus any group-commit window, within 1500 ms", under "Deployment
rules"). What it does not close is a mixed cluster: a primary ticking
outside the bound (its group-commit window counted) and without the
flag, among replicas that carry it.

**Sketched fix**, the way to support mixed deployments if it is ever
needed. Carry each node's primary-link state in the raft envelope, beside
the tip, and have a replica refuse to auto-promote while a fresh peer
reports the primary up. It is an envelope change, so it
comes after item 1. Open points for its design: how fresh "fresh" is
(`PEER_TIP_GRACE` is the natural bound, and the item 3 stamp makes the
report as trustworthy as the tip), how a primary reports itself (it has
no primary link), and whether the refusal should yield to a peer that
reports the primary up but is itself behind.

## Other open items

- **openraft's `error!` during leadership confirmation: left as it is
  (decided by the maintainer).** openraft 0.9.25's
  `RaftCore::handle_check_is_leader_request` logs "timeout while confirming
  leadership for read request" at `error` for each voter whose confirming
  heartbeat fails or is refused, so every genuine failover whose dead
  primary is a voter logs one. The subscriber belongs to the application's
  binary, so the runtime has no filter to cap; `docs/replication.md` quotes
  the line and says it is expected then. Do not "fix" it in the runtime.
- **The "Runtime suites on DPDK" CI run (x86_64).** The partition tests of
  `raft_failover.rs` (raft proxies on 127.0.0.1, `raft_ip(i)` =
  127.0.0.(i+2)) passed under `scripts/dpdk/netns-runner.sh` on an aarch64
  host only. CI's x86_64 run is the confirmation. If only the partition
  tests fail there, compare with the raft proxies bound on
  `melin_test_node::local_ip()` before gating them to kernel TCP, and
  record why in the file header. One unexplained failure of
  `a_serving_leader_fences_on_a_followers_higher_epoch` seen in a review
  run did not recur and is consistent with a stale build: a modified copy
  of the tree sharing `CARGO_TARGET_DIR` writes `melin-raft` artifacts
  under the same metadata hash with a newer fingerprint, so the next build
  of a dependent links the modified crate. After such a run use
  `cargo clean -p melin-raft`, or give the copy its own target directory.
- **Re-measure the healthy quorum-ack age under suite load.**
  `a_healthy_leaders_quorum_ack_age_stays_under_the_gate` was measured
  under `cargo test` (the peak was about the leader's tick period, far
  under `QUORUM_ACK_MAX_AGE`), not under `cargo nextest` with the whole
  suite running. If the peak ever approaches the threshold, revisit the
  threshold with the maintainer rather than loosening it silently: a
  filter that refuses a healthy leader delays every failover.
- **A promotion crash loop, if it is ever seen.** The filter reads acks on
  raft's long-lived connections, the confirmation opens a fresh connection
  and Ed25519 handshake per voter. Anything that breaks new connections
  but not established ones makes the node promote, abandon, exit, restart
  and possibly win again. If the genuine-failover tests ever show a
  spurious `NoQuorum` or `TimedOut`, the direction is a confirmation over
  connections that already exist, not a longer `LEADER_CONFIRM_TIMEOUT`,
  which the mint-time safety assertion bounds.
- **Committing the promotion as a raft log entry.** The strongest form of
  the mint-time check (the epoch committed through raft, not confirmed
  beside it), rejected for the lease branch as a control-plane redesign.
  Worth weighing with "Hold the ack policy in the raft control plane" in
  `roadmap.md`, which also puts state into the raft state machine.
- **Deferred fallbacks for the replicas-first bring-up.** The partition
  tests put raft leadership on a replica by starting the replicas first
  (`Cluster::start_replicas_first`), which asserts that the late primary
  does not force a re-election. It has always held. Should it ever fail,
  capture the raft logs (`RUST_LOG=openraft=info`) first; the fallbacks
  (a delayed or skipped `initialize` for a late joiner, which is a new
  knob on the published `RaftConfig` and needs the maintainer, or
  restarting the primary until a replica leads, which is flaky) were
  deliberately not built.
