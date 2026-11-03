# Iteration time (2026-11)

Where the edit–build–test loop spent its time, what was changed about it,
and what is left. Covers the test suite, compilation, the pre-commit hook,
and agent-driven workflows. Open items are rated for effort (S/M/H/XL),
risk, and value.

## Measurements

All taken 2026-11-02 on an AMD Ryzen 7 5800X3D (8 cores, 16 threads),
inside a container, with a warm target directory. "Confined to 4 CPUs"
means the same host restricted to four CPUs, standing in for a hosted CI
runner. They describe that machine on that day; re-measure before relying
on them.

### Before

| Step | Time |
|---|---|
| Full `cargo nextest run` | 4m26s wall |
| CPU time during the suite | about 32 min (12 min user, 20 min system) |
| `melin-example-echo::round_trip an_echo_returns_the_bytes_it_was_sent`, alone | 0.77s |
| The same test inside the full suite | about 9s; every example round trip behaved alike |
| The cluster test group, then run one test at a time | about 225s, most of the suite's wall time |
| Rest of the pre-commit hook after touching one file (fmt, clippy, rustdoc, feature checks, test build) | about 25s combined |

### What each change did

| Change | Suite wall time | Suite CPU time |
|---|---|---|
| Idle unpinned threads park instead of yielding | unchanged, 4m26s | about 32 min down to about 5 min |
| Test journals zero-fill 8 MiB segments instead of 256 MiB, and the cluster group runs four tests at a time | about 75s on 16 threads, about 95s confined to 4 CPUs | — |

The pre-commit hook, which runs the full suite, takes about 1.5 min after
both.

## What was learned

**Idle CPU was not what made the suite slow.** Each test node ran its
pipeline threads under spin-then-yield, which never sleeps, so a parallel
suite kept every core busy and system time exceeded user time. That looked
like the cause of the slowdown. Parking idle threads removed nearly all of
that CPU and left wall time exactly where it was: the tests were waiting on
something else.

**The real costs were disk and serialization.**

- *Journal zero-fill.* Every test node staged its journal segments at the
  production rotation threshold, zero-filling 256 MiB per segment with
  synchronous I/O. Many nodes doing that at once queued on the disk:
  their journal-prep threads sat blocked in `submit_bio_wait`, and a test
  that needs under a second alone took about 9s. Since journal
  preallocation follows the rotation threshold, test nodes (which rotate
  at `melin_test_node::MAX_JOURNAL_MIB`) zero-fill small segments, and the
  per-test opt-outs to allocate-only staging are gone.
- *The serialized cluster group.* Its tests mostly wait out real raft and
  replication timeouts, and run one at a time they were the suite's
  critical path. With the disk no longer saturated, four at a time is
  stable, both on all 16 threads and confined to 4 CPUs, checked by
  repeated full runs.

**The flaky raft test was a test setup race, not a product bug.**
`raft_failover::a_serving_leader_fences_on_a_followers_higher_epoch`
muted the replicas after `wait_formed`, which returned on *any* leader.
When a replica happened to lead and the primary had not yet received its
first entry, every node the primary could reach refused its vote on the
log check, and nothing could be elected. The test now starts replica-led
on purpose and hands leadership to the primary, by stalling the leader's
raft traffic at the primary's proxy alone, before muting the replicas.

The race made this test flaky. The related product behaviour, openraft
0.9's lack of pre-vote letting a returning candidate briefly churn an
established leader, is already documented and bounded in
[raft-control-plane.md](raft-control-plane.md) (the leader lease and the
re-arm limit the disruption) and tracked as the openraft 0.10 migration in
[roadmap.md](roadmap.md).

**A broken environment is the worst case.** In a container whose seccomp
filter blocked `io_uring_setup` (ENOSYS), every node-starting test failed,
and the server logged only `startup failed: stopping the stages already
running`, without the cause. Each such test waited out the full startup
timeout, and each cluster test ran out the nextest timeout: the suite would
have taken hours.

## Done

- **A parking wait strategy** (`WaitStrategy::SpinYieldPark`): spin, yield,
  then sleep with a backoff. What unpinned threads use (`--cores none`, any
  `0` entry), and a `p` suffix in `--cores` for pinned ones; `0y` keeps an
  unpinned thread yielding. Never the default on a pinned core. Its effect
  on the suite is CPU, not wall time (above).
- **Test nodes unpinned in one place**: `melin_test_node::config()`, rather
  than in each test, with a unit test pinning every field it changes from
  production.
- **Small test journals**: the rotation-driven preallocation, and the
  per-test overrides that worked around zero-fill removed.
- **The cluster group bounded, not serialized**: `cluster` in
  `.config/nextest.toml`, four at a time.
- **Coverage of the old timing kept after merge**:
  `MELIN_TEST_NODE_WAIT=yield` (`melin_test_node::WAIT_ENV`) makes test
  nodes yield rather than park, which interleaves stages more finely and
  gives narrow races more chances to show. The post-merge workflow runs the
  suite that way, with the cluster tests one at a time.
- **The raft setup race fixed** (above).

## Open items

Ordered by expected value for the effort.

**1. Shorter raft and replication timings in tests.** With the disk and the
serial group out of the way, the slowest tests are the ones that wait out
real election and heartbeat periods: `raft_failover`, the `melin-raft`
election tests, `degraded_acks` and `ack_policy_changes`. Tests proving
that something *never* happens mostly wait out timeouts. A test
configuration with those periods scaled down shortens them. Effort M, risk
medium (shorter timeouts flake under load; the post-merge yielding run is
the place to watch), value high: these now set the suite's wall time.

**2. Scope what review loops run.** A multi-agent implement/review loop
where the implementer, self-reviewer and reviewer each run the suite and
`cargo llvm-cov` every round repeats the full suite, plus an instrumented
rebuild and rerun, several times per round. Have reviewers run the
affected crates' tests and `cargo llvm-cov nextest -p <changed crates>`,
with one full-suite gate at the end. A script mapping changed files to
crates and their reverse dependencies (`cargo metadata`) makes the scoping
mechanical. Effort S (prompt edits, a script), risk low, value high.

**3. Fail fast, with the cause, when a node does not start.**
`melin-test-node` keeps retrying `connect` after the node's thread has
already exited with an error, and the server's `startup failed` line omits
the error. Effort S, risk very low, value high in the worst case above. The
logging half is an operator-facing fix in its own right.

**4. Check the environment before tests run.** A one-time check (in
`melin-test-node` or a `scripts/` preflight, possibly run from an agent's
session-start hook) that io_uring works and the locked-memory limit is
high enough, with a clear message when not. Effort S, no risk, value
medium; mostly helps agents and new machines.

**5. One integration-test binary, and less debug info.**
`melin-server-runtime` builds one test binary per file, each large, and
linking dominates incremental builds. One binary (`tests/it/main.rs` with
modules) moves the `cluster` group's filter in `.config/nextest.toml` from
`binary(...)` to test-name filters. Separately, `[profile.dev] debug =
"line-tables-only"`, none for dependencies, speeds links and shrinks the
target directory; backtraces keep file and line. Effort S–M, risk low,
value medium–high.

**6. Split the DPDK suites in pre-merge.** The DPDK job runs its suites one
test at a time under the netns runner, which makes it the longest pre-merge
job. Splitting it across parallel jobs (by suite, or with `nextest
--partition`) shortens the gate without dropping any coverage. Effort S,
risk low, value medium on CI wall time; no local benefit.

**7. Deterministic simulation testing** of the cluster layer: simulated
clock and network, in the style of TigerBeetle's VOPR. Effort XL, risk high
(refactoring the I/O boundaries), value very high in the long run. Cluster
tests go from seconds of real waiting to milliseconds and replay
deterministically, and simulation finds failover races, like the one above,
that wall-clock tests hit only by chance. A strategic project, not a quick
win.

Lower value, kept for reference: splitting `melin-server-runtime` into
smaller crates (profile with `cargo build --timings` first); sccache, worth
it only with a target directory per agent; a faster linker (already on
lld); nightly-only compiler options (the workspace is on stable with an
enforced MSRV).
