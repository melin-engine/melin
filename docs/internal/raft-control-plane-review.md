# Control-plane raft — branch review findings

Review of the `feat/control-plane-raft` branch (13 commits) after rebase
onto `main` at 0.12.0. Status at review time: clippy clean, full test
suite green (including the E2E failover test), no correctness-critical
bugs found. The items below are the open findings, ranked; delete each
one as it is resolved. (Resolved so far: finding 1, the overstated
"still-starting primary" grace-period claim — blank-genesis refusal,
corrected rustdoc, primary-first bring-up rule; finding 2, the
error-path driver/health leak — `RaftDriverGuard` and
`ReplicaHealthGuard` now tear down on every exit path via Drop;
finding 3, the hardcoded replica `pipeline_healthy` — now a live
mirror of the replica journal stage's failure latch. Fixing 3 also
surfaced and fixed a branch regression: the `hash-chain`-gated
divergence-resync tests had been left uncompiled against the new
receiver/protocol signatures — remember to run the `hash-chain`
feature in verification, not just the default set.)

## 4. Test gaps (low)

- **The 3-voter auto-promote refusal is untested.**
  `build_raft_config`'s rejection of `--raft-auto-promote` with fewer
  than three voters has no unit test; every other config refusal has
  one.
- **The tip-readiness vote gate is untested.**
  `RpcServerConfig::admit_vote`'s "drop all votes while
  `tip.is_ready()` is false" branch has no coverage at any level —
  `rpc_loopback.rs` always constructs ready tips. One loopback test
  with `ready: false` asserting the connection closes would pin it.

The behind-candidate drop itself is covered end-to-end
(`election.rs::behind_node_never_wins_an_election`), and supersession
has a real-socket test.

## 5. Nits

- `tcp_receiver.rs` (StreamStart handling): the old "Adopt the
  primary's epoch immediately…" comment was left in place when the
  extended version was added — it appears twice back to back.
- `rpc_server.rs`: the `WRITE_TIMEOUT` comment says "30 such peers
  would exhaust the accept cap"; `MAX_INBOUND` is 32.
- `build_raft_config` runs twice on the primary paths (once for the
  `None`/`Some` gate, once inside `spawn_raft_driver`), re-parsing the
  peer list. Harmless at startup; a `Some(cfg)` handoff would be
  cleaner.
- The `REPL_PROTOCOL_VERSION` 3→4 bump makes mixed-version replication
  refuse cleanly (correct), but the operator docs never state that a
  rolling upgrade must take the whole replica set together.
- `SupersessionPolicy` is armed per node by that node's own
  `--raft-auto-promote` flag; a cluster with the flag on only some
  nodes gets asymmetric fencing (an unflagged serving node will not
  self-fence via the raft mesh). The docs never state the flag should
  be uniform across the cluster.
