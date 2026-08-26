# Control-plane raft — branch review findings

Review of the `feat/control-plane-raft` branch (13 commits) after rebase
onto `main` at 0.12.0. Status at review time: clippy clean, full test
suite green (including the E2E failover test), no correctness-critical
bugs found.

**All findings are resolved.** For the record:

1. The overstated "still-starting primary" grace-period claim —
   blank-genesis refusal added, rustdoc corrected, primary-first
   bring-up rule documented.
2. The error-path driver/health leak — `RaftDriverGuard` and
   `ReplicaHealthGuard` now tear down on every exit path via Drop.
3. The hardcoded replica `pipeline_healthy` — now a live mirror of the
   replica journal stage's failure latch. Fixing this also surfaced and
   fixed a branch regression: the `hash-chain`-gated divergence-resync
   tests had been left uncompiled against the new receiver/protocol
   signatures. The chain was off by default then, so the default
   verification set never built them; it is on by default now and the
   ordinary suite runs them, with the chain-less build guarded by its own
   check instead.
4. Test gaps — the 3-voter auto-promote refusal and the tip-readiness
   vote gate are now covered.
5. Nits — duplicated epoch-adoption comment removed, `WRITE_TIMEOUT`
   comment count corrected, `spawn_raft_driver` now takes the built
   `RaftConfig` (peer list parsed once per boot), and the
   `--raft-auto-promote`-uniformity deployment rule documented. The
   suspected missing rolling-upgrade rule already existed ("Upgrade
   primaries and replicas together", added with the protocol-version
   bump) — no change needed.
