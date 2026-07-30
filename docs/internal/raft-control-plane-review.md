# Control-plane raft — branch review findings

Review of the `feat/control-plane-raft` branch (13 commits) after rebase
onto `main` at 0.12.0. Status at review time: clippy clean, full test
suite green (including the E2E failover test), no correctness-critical
bugs found. The items below are the open findings, ranked; strike or
delete each one as it is resolved.

## 1. Auto-promotion's "primary still starting up" protection is weaker than documented (medium)

`PRIMARY_DOWN_GRACE` (3 s, `raft_promotion.rs`) measures how long *this
process* has seen the primary link down, with the clock starting when
the promotion thread boots. The rustdoc and `docs/replication.md` claim
the grace distinguishes "a primary that is simply still starting up"
from a real failure — but it only does so if the primary comes up
within ~3 s of the replicas:

- **Cluster cold start / rolling restart with auto-promote armed.**
  Replicas boot, form quorum (1–2 s election), and 3 s later one
  auto-promotes even though it has *never observed a primary at all*
  (`primary_link_up` starts false; no refusal covers "never saw a
  primary this boot"). A primary whose journal recovery + prefault
  takes longer than ~5 s comes up already superseded and is fenced at
  birth.
- **Restart of a crashed primary.** By restart time `down_for` is long
  past the grace, so the primary's recovery duration is irrelevant —
  failover has already happened. Arguably intended (that is what
  auto-failover is for), but the docs imply more protection than
  exists.

No data loss in either case (epochs stay distinct, fencing holds), but
a spurious deposition at cluster bring-up is an operational footgun.
Options, in order of preference:

1. Document the bring-up ordering rule (start the primary and let
   replicas connect before relying on auto-promote, or arm it only
   after the cluster has formed).
2. Add a refusal: "no primary observed this boot **and** the local
   journal is empty" — kills the genesis-race case specifically.
3. Make the grace configurable with a more conservative default.

A plain "must have observed the primary once since boot" rule would be
wrong: it deadlocks the legitimate case of a replica that restarts
during a primary outage and wins the election.

## 2. Error paths leak the raft driver thread and its port (low-medium)

In `server.rs`, `stop_raft_driver` / `stop_replica_health` run on the
success and clean-shutdown paths, but not when a `?` fires between
driver spawn and those calls — e.g. `spawn_replica_health(...)?`
failing on a taken health port, or `run_receiver(...)?` returning
`Err`. The driver thread, its `--raft-bind` port, and the promotion
thread outlive the function. Irrelevant when `main` exits, but
`run_with_listener` is a library entry point (the integration tests
call it in-process and re-bind ports); a leaked driver keeps its port
and keeps voting. The `stop_raft_driver` doc comment advertises
coverage of error-return paths, but nothing routes those paths through
it. A scope guard — or an inner function whose result passes through a
single cleanup point — closes this.

## 3. Replica health endpoint hardcodes `pipeline_healthy = true` (low)

`spawn_replica_health` (`raft.rs`) passes a fresh
`AtomicBool::new(true)` into `HealthState::for_replica`, and nothing
ever writes it. A replica whose journal stage has failed
(`journal_failed` latched, receive loop resyncing in a loop) still
exports a healthy pipeline gauge. Either plumb the real flag (it lives
inside `run_receiver`, so a shared handle would need to be created
alongside `ReplicaControlPlane`) or suppress the gauge on the replica
endpoint so it cannot mislead.

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
