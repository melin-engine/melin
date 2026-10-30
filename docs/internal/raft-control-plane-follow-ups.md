# Control-plane raft: decided follow-up work (contributor notes)

Work on the raft control plane that has been designed and decided but
not built. It was recorded while the leader-lease gate on auto-promotion
was implemented (branch `fix/raft-promotion-quorum-lease`; the gate
itself is described in `raft-control-plane.md`, "The leader-lease gate",
under "Auto-promotion policy"). Each item gives the problem, why it
matters, the decided design and what was rejected. Function names are the
anchors; line numbers are left out because they go stale.

The first three items are one follow-up branch, **in this order**: the
raft protocol version, then mutual authentication in the same handshake
change, then the stale-peer-tip stamp, which is the first envelope change
the version protects. The one-sided partition gap after them is a
separate design that has only been sketched.

## 1. A raft protocol version (prerequisite of 2 and 3)

**Problem.** Neither the RPC envelope (`RpcFrame` in
`crates/core/raft/src/wire.rs`: a `u32` LE length prefix and a postcard
payload) nor the raft handshake (`crates/core/raft/src/auth.rs`, which
reuses only the replication challenge/response frames from
`melin_transport_core::replication::protocol`) carries a version. A raft
envelope change between releases is therefore undiagnosable during a
rolling upgrade, and because postcard is not self-describing, a layout
change could be *misdecoded* by the other release rather than refused.
Items 2 and 3 both change what crosses the raft link, so this comes
first (decided).

**Design (decided).**

- A `RAFT_PROTOCOL_VERSION` constant in `melin-raft`, exchanged right
  after authentication succeeds and before the first `RpcFrame`, in both
  directions. A mismatch closes the connection with an error naming both
  versions, as the replication handshake does with
  `REPL_PROTOCOL_VERSION` (`crates/core/transport-core/src/replication/protocol.rs`)
  and the client protocol with `CLIENT_PROTOCOL_VERSION`.
- The version frame has a **fixed byte layout** (a tag and a little-endian
  version, encoded by hand like the replication control frames), not
  postcard, so every future release can read an older or newer peer's
  version frame whatever else changes.
- The release that introduces it must be **guaranteed undecodable by
  today's `RpcFrame` decoder**. Today's nodes expect an `RpcFrame` straight
  after authentication, so a new dialer's version frame lands in an old
  responder's `read_frame` (and a new responder's in an old dialer's
  reply read). It must fail to decode there, never decode as a valid
  frame: choose the frame's bytes so postcard rejects them as an
  `RpcFrame` (for example a payload too short to hold the envelope, or one
  whose `RpcBody` variant index is out of range). Pin it with a test that
  feeds the version frame to a **frozen copy** of today's `RpcFrame`
  decoder (a copy of the types and `read_frame` as they are now, kept in
  the test with a comment saying why it is a copy) and asserts an error.
  Otherwise the first mixed-version cluster could act on a misread vote
  or tip.

**Tests.** Matching versions connect; a mismatch is refused in each
direction with both versions in the message; the frozen-decoder test
above; the frame's encode/decode round trip and its refusal of a short or
wrong-tag frame.

## 2. Mutual raft authentication

**Problem.** Raft authentication is one-way: the dialer proves its key
(`authenticate_outbound` signs the responder's nonce), and the responder
pins the dialer's key to its configured node id, but the dialer accepts
the responder's verdict without checking who the responder is. Since the
reply path fences (`RaftClient::request` feeds every reply's `tip_epoch`
to `SupersessionPolicy::observe`), anything answering at a peer's
configured raft address (a process that took the port after the peer
died, or something on the network path) can send a reply with a huge
`tip_epoch` and make a serving primary fence and shut down. The failure
is fail-safe (an outage, never a split brain), and openraft already
trusts unauthenticated replies for votes and the replies feed `PeerTips`,
so the trust model is not new; but this path now leads directly to a
primary outage. Accepted for the lease branch by the maintainer and
added to this follow-up (decided).

**Design (decided).** In the same handshake change as item 1: the dialer
sends a nonce of its own, and the responder signs it with its key; the
dialer verifies the signature and checks that the key is the one
configured for **the peer id it dialled** (the `--raft-peer` entry, or
the stored membership's node), exactly as the server already pins the
dialer's key to the id a request claims. The responder's key must hold
the `replication` role, as the dialer's must. Share the verification with
`verify_challenge_response_identified` rather than writing a second copy.

**Tests.** A responder with the right key passes; one with an unknown
key, a key of another role, or the key of a *different* configured peer
is refused by the dialer; a dialer refusing never applies a reply (no
fence, no peer tip recorded). The reply-path fence tests in
`crates/core/raft/src/tests/rpc_loopback.rs` keep passing.

## 3. Stale peer tips

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

## 4. The one-sided partition with an idle primary (sketched, not decided)

**Gap.** The replica raft leader `L` loses only its link to the primary,
while the other replica `O` still reaches both. `L` keeps a fresh lease
(`O` acks it), so the leader-lease filter and the mint-time confirmation
both pass. Whether `L` then deposes the primary depends on traffic:

- if the primary takes writes during `PRIMARY_DOWN_GRACE`, `O`'s tip moves
  ahead of `L`'s and the peer-tip veto refuses for as long as `O` stays
  ahead. `O` does not campaign, because the challenger nudge needs `O`'s
  own primary link down (`challenger_should_campaign`), so nobody promotes
  and the primary keeps serving. Pinned by
  `a_replica_leader_cut_from_the_primary_alone_does_not_promote_while_writes_flow`
  in `crates/core/server-runtime/tests/raft_failover.rs`;
- if the primary is **idle**, the tips stay equal, nothing vetoes, and `L`
  promotes and deposes a primary the rest of the cluster can see. Untested,
  since the outcome is the known gap.

This is the documented one-sided-link limitation of `PRIMARY_DOWN_GRACE`
(the link flag is this node's socket only). The same partition also
reaches the one case the mint-time argument leaves open (see
`raft-control-plane.md`): `L` steps down after its confirmation and
before the mint, and the newer leader is the primary, which never mints.

**Sketched fix.** Carry each node's primary-link state in the raft
envelope, beside the tip, and have a replica refuse to auto-promote while
a fresh peer reports the primary up. It is an envelope change, so it
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
