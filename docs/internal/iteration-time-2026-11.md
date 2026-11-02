# Iteration time (2026-11)

A survey of where the edit–build–test loop spends its time, and the options
for shortening it: tests, compilation, the pre-commit hook, and agent-driven
workflows. Each option is rated for effort (S/M/H/XL), risk, and value.

## Measurements

Taken 2026-11-02 on an AMD Ryzen 7 5800X3D (8 cores, 16 threads, 60 GB),
inside a container, with a warm target directory, at commit `cc260632`.
They describe that machine on that day; re-measure before relying on them.

| Step | Time |
|---|---|
| Full `cargo nextest run` | 4m26s wall, 1870 tests, all pass |
| Sum of the tests' own run times | 856s (about 54s of ideal wall time on 16 threads) |
| CPU time during the suite | 12m user, 20m system |
| `melin-example-echo::round_trip an_echo_returns_the_bytes_it_was_sent`, alone | 0.77s |
| The same test inside the full suite | about 9s; every example round-trip test behaves alike |
| `cluster-serial` test group, back to back | about 235s, most of the suite's wall time |
| Rest of the pre-commit hook after touching one file (fmt, clippy, rustdoc, feature checks, test build) | about 25s combined |

Conclusions:

- **Tests are about 90% of the loop.** Compilation is a secondary target.
- **Tests mostly slow each other down.** Each node runs several pipeline
  threads under `WaitStrategy::SpinThenYield`, which spins and then calls
  `sched_yield` on every idle iteration, forever: it never sleeps. With up to
  16 tests in parallel, each starting nodes, spinning threads far outnumber
  cores, hence system time above user time and a 12× slowdown of a test that
  needs under a second on its own. The `cluster-serial` tests run under the
  same contention, so their uncontended time is probably much lower; it was
  not measured.
- **A broken environment is the worst case.** In a container whose seccomp
  filter blocked `io_uring_setup` (ENOSYS), every node-starting test failed,
  and the server logged only `startup failed: stopping the stages already
  running`, without the cause. Each such test waited 10s for the node to
  serve, and each cluster test ran out the 120s nextest timeout, one at a
  time: the suite would have taken hours.

## A. Tests

**1. A parking wait strategy for shared cores.** Spin, then yield, then park
(a backed-off sleep or a futex) after a number of yields. Default for
`--cores none` and `melin-test-node`; `BusySpin` stays for isolated cores.
Effort M, risk low, value very high. Targets the measured cause; should bring
the suite toward its ideal and reduce timing flakes. Tests already run a
different wait path from production (`SpinThenYield`), so nothing is lost
there; the new strategy must never become the default on pinned cores, and
needs unit tests like the existing strategies have.

**2. Fail fast, with the cause, when a node does not start.** `melin-test-node`
keeps retrying `connect` for 10s after the node's thread has already exited
with an error, and the server's `startup failed` line omits the error. Effort
S, risk very low, value high. A broken environment or a startup regression
then costs milliseconds and says why. The logging half is an operator-facing
fix in its own right.

**3. Check the environment before tests run.** A one-time check (in
`melin-test-node` or a `scripts/` preflight) that io_uring works and the
locked-memory limit is high enough, with a clear message when not. Effort S,
no risk, value medium; mostly helps agents and new machines.

**4. Shorter raft and replication timings in tests.** `raft_failover` is about
120s across 8 tests, the longest 36s. Tests proving that something *never*
happens (`..._never_deposes_...`, `..._never_auto_promotes`) mostly wait out
real timeouts. A test configuration with election and heartbeat periods
scaled down 5–10× shortens them. Effort M, risk medium (shorter timeouts
flake under load, so do item 1 first), value high on the serialized critical
path.

**5. Loosen `cluster-serial`.** With item 1 in place, `max-threads = 1` may be
able to rise to 3–4. Re-measure with nextest's slow-test report. Effort S,
risk medium (the resource reasons in `.config/nextest.toml`), value high: the
group sets the suite's wall time.

**6. A `quick` nextest profile for the inner loop.** Leaves out the
`cluster-serial` binaries and the example round trips; the hook and CI still
run everything. Effort S, risk low, value high for edit–test cycles.

**7. Run only the affected crates.** A script maps changed files to crates
plus their reverse dependencies (via `cargo metadata`) and passes `-p` flags
to nextest. A change in `wire-protocol` still runs most of the workspace; one
in `melin-bench` runs one crate. Effort S–M, risk low, value medium–high,
especially for agents (section D).

**8. Deterministic simulation testing** of the cluster layer: simulated clock
and network, in the style of TigerBeetle's VOPR. Effort XL, risk high
(refactoring the I/O boundaries), value very high in the long run. Cluster
tests go from seconds of real waiting to milliseconds and replay
deterministically, and simulation finds failover races that wall-clock tests
cannot. A strategic project, not a quick win.

**9. Cheaper test journals.** Creating a journal took about 150ms in the traced
test, most likely its preallocation. Smaller limits in test configurations,
or a tmpfs `TMPDIR`. Effort S, risk low–medium (tmpfs does not behave like a
disk for O_DIRECT or fsync), value low–medium.

**10. Shard CI with `nextest --partition`** across matrix jobs on the 4-vCPU
runners. Effort S, risk low, value medium on CI wall time; no local benefit.

## B. Compilation

**11. One integration-test binary for `melin-server-runtime`** (`tests/it/main.rs`
with modules) instead of one per file. Test binaries are about 125 MB each,
and linking dominates incremental builds. Effort S–M, risk low: the
`cluster-serial` filter in `.config/nextest.toml` moves from `binary(...)` to a
test-name filter such as `test(/^raft_failover::/)`. Value medium–high.

**12. Less debug info.** `[profile.dev] debug = "line-tables-only"`, none for
dependencies, `split-debuginfo = "unpacked"`. Effort S, risk low, value
medium: faster links and a much smaller target directory. Debuggers see less;
backtraces keep file and line.

**13. Split `melin-server-runtime`**, by far the largest crate. Effort H, risk
medium (API boundaries, `pub` creep), value medium: better build parallelism
and smaller incremental rebuilds. Profile with `cargo build --timings` first;
worth it only if that crate is clearly on the critical path.

**14. mold or wild instead of lld.** Effort S, risk low, value low: already on
lld.

**15. sccache.** Effort S, risk low, value low for incremental builds, medium
with per-agent target directories (item 18), whose cold builds it makes cheap.

**16. Nightly-only options** (Cranelift backend, `-Zthreads`). Value medium,
risk medium; the workspace is on stable with an enforced MSRV, so skipped.

## C. Pre-commit hook

**17. Keep the full suite in the hook once item 1 lands; otherwise run the
`quick` profile there.** Effort S, value high. At about a minute the full gate
is affordable and suits correctness-first; moving it to CI only lets a broken
commit into history between pushes.

## D. Agent-driven loops

**18. Shared target directory and CPU.** Every agent and worktree uses the same
`CARGO_TARGET_DIR`. Parallel agents queue on cargo's build lock and run full
suites at once, worsening the spin contention and risking spurious timeouts
that agents then try to "fix". Options: at most one full suite at a time,
enforced by `flock` in a wrapper (effort S), or a target directory per agent
backed by sccache (effort S, costs disk). Value high when agents run in
parallel.

**19. Scope what review loops run.** A multi-agent implement/review loop where
the implementer, self-reviewer and reviewer each build, test and run
`cargo llvm-cov` every round repeats the full suite, plus an instrumented
rebuild and rerun, several times per round. Have reviewers run affected-crate
tests (item 7) and `cargo llvm-cov nextest -p <changed crates>`, with one
full-suite gate at the end. Effort S (prompt edits), risk low, value high.

**20. Document the loop in `CLAUDE.md`:** which command to run at each stage
(`check`, targeted nextest, `quick`, full). Effort S, value medium.

**21. Fewer permission prompts.** Allowlist the read-only and build commands an
agent runs routinely (`cargo check`, `cargo test`, `cargo build`, `rg`); each
prompt stalls an unattended loop. Effort S, value medium.

**22. A faster model for read-only subagents** (search, exploration), keeping
the strongest models for implementing and reviewing. Effort S, value
low–medium.

**23. A session-start hook running the environment check of item 3**, so an
agent in a broken container learns it at the start rather than after an hour
of failures. Effort S, value medium.

## Recommended order

1. Items 2 and 3: an hour or two each, and they remove the worst case.
2. Item 1, then re-measure. Expected to cut the suite by more than half;
   a prediction until measured.
3. Items 4 and 5: shrink the serialized cluster tests once contention is gone.
4. Items 19, 6 and 7: scope agent and inner-loop test runs.
5. Items 11 and 12 for compile and link time.
6. Item 8 as a strategic project.

Not yet measured: the cluster tests' uncontended time, and the cost of a real
code edit (rather than a touched file) in `melin-server-runtime`. Both are
worth measuring before committing to items 5 and 13.
