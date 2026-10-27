# No stale bytes in response bodies, and a debug check that encoders are pure (plan)

Status: **done** (2026-10), steps 1 to 3. Where the build departs from
the text below:

- Step 1's end-to-end leak test ran through a running node
  (`crates/core/server-runtime/tests/encoder_purity.rs`) rather than the
  response stage alone. CI runs no test with `--release`, so step 3
  replaced it there with the debug test that the check fires through a
  running node, on kernel TCP and on DPDK.
- The check-free inner function (`frame_zeroed`) is private, not
  `pub(crate)`: only `frame_app_response` and its own tests call it.
- Step 2's measurement, 2026-10-27, on an Apple M-series laptop (8 cores,
  Asahi Linux), kernel TCP over loopback, echo with 64-byte requests, one
  client, window 1, `disk` acks, five interleaved 15-second runs of each
  build: p50 13.17 to 13.46 µs before, 13.13 to 13.50 µs after; p99 17.2
  to 17.4 µs before, 16.9 to 17.7 µs after; p99.9 87 to 105 µs before, 85
  to 136 µs after. Within run-to-run noise, so no high-water mark. Not
  measured over DPDK: no bench host with a NIC was available.

It was to be implemented on its own branch off `main`. It is independent
of the divergence-detection plan (not yet written), which is to build on
it: that plan's `encode_report_body` is to wrap `frame_app_response` and
so inherit both changes.

## The problem

The response stages encode each application response body into a reused
stack buffer: `EncodeBuf` (`crates/core/server-runtime/src/response_frame.rs`),
the slot path's in `response_loop::run`, and the early replies' own
`EarlyReplyBuf` beside it. `ResponseEncoder::encode_report` and
`encode_query` (`crates/core/app/src/encoder.rs`) write a body and return
its length. Nothing requires them to write every byte below that length.

So an encoder that skips a byte, such as a reserved or padding field,
sends whatever an earlier encode left there. That earlier encode may have
been another client's response. This is a potential cross-client data
leak today. Under the planned output digest it would also be a false
divergence: the leftover bytes differ from copy to copy, so a healthy
application would stop a node.

Every application encode goes through one function, `frame_app_response`
(`response_frame.rs`), called from `encode_slot_payload` and
`encode_early_reply`. Both fixes go there, so they cover every path and
no caller can forget them.

## Two changes, kept apart

- **(b) The release guarantee: zero the body region before every
  encode.** A skipped byte is then always zero, on every copy and to every
  client. It is unconditional, and it fixes the leak whatever the
  application's encoder does.
- **(d) Detection in debug builds: check that encoders are pure.** Encode
  twice, over two different poison patterns, and panic if the results
  differ. That catches skipped bytes, and also an encoder whose output
  depends on something other than its input (internal state), which (b)
  cannot catch. It costs nothing in release builds.

(b) prevents the failure in production. (d) finds the application bug
behind it in tests. Neither replaces the other: (d) alone leaves
production unprotected for any report variant the tests never encode.

### Why the whole region, not a high-water mark

Zeroing only up to the longest length returned so far is not sound. An
encoder may write past the length it returns, for example using the rest
of the buffer as scratch. Those bytes lie beyond the recorded mark, are
never zeroed, and resurface when a later encode skips a byte there. The
runtime cannot know which bytes an encoder touched, so only zeroing the
whole body region (`MAX_RESPONSE_BODY`) gives the guarantee without a
contract the encoder must keep.

The buffer is reused for every slot, so it stays in L1 cache and the
zeroing should be cheap. It does sit on the reply's latency, though, so
step 2 measures it. A high-water mark is the fallback only if that
measurement shows a cost. It would be sound only together with a contract
that encoders never write past their returned length, enforced by (d) in
debug builds alone.

## Conventions

CLAUDE.md applies throughout:

- Conventional Commits, one commit per step.
- No new dependency, so `Cargo.lock` is unchanged.
- No `.unwrap()` in production code.
- Comments justifying data-structure and type choices.
- Never `--no-verify`.

## Step 1: repro tests

These fail before step 2 and pass after it. The pre-commit hook runs the
suite and must not be bypassed, so fold this step into step 2's commit,
with the tests first in the diff, rather than committing a failing test.

1. **Unit test** in `response_frame.rs`'s test module, beside its existing
   test encoders:
   - Define a `SkippingEncoder`. Its `encode_query` writes every byte of a
     16-byte body as `0xFF`. Its `encode_report` writes byte 0 and bytes
     2..16 of a 16-byte body, **leaves byte 1 untouched**, and returns 16.
   - Through one `EncodeBuf`, frame a query response, then a report.
   - Assert the report body's byte 1 is `0`. Before the fix it is `0xFF`,
     the query's byte.
   - Name it for the behaviour, e.g.
     `a_body_byte_the_encoder_skips_is_zero_not_the_previous_bodys`.
2. **The same for early replies:** an early rejection framed through
   `encode_early_reply`, after a report in the same `EarlyReplyBuf`,
   carries no byte of the earlier frame.
3. **An end-to-end leak test** in `crates/core/server-runtime/src/tests/`,
   modelled on `response_flush_within_batch.rs`, which shows an encoder in
   a test:
   - two clients on one node with the `SkippingEncoder`;
   - client A queries, then client B receives a report;
   - assert that B's body byte 1 is `0`, carrying nothing of A's response.

   This test interacts with step 3; see there.

Run them before the fix and check that they fail for the stated reason.

## Step 2: (b) zero the body region before every encode

1. In `frame_app_response`, before calling `encode`, zero the body region
   (`buf[HEADER_LEN..].fill(0)`). The comment says why:
   - encoders are not required to write every byte below their returned
     length, and may use bytes past it as scratch;
   - so only the whole region guarantees that no byte of an earlier body
     survives: not the previous length, and not a high-water mark of
     returned lengths;
   - the buffer is reused and stays in L1 cache.
2. Do not add zeroing at the call sites. Both go through
   `frame_app_response`, which is the point.
3. Step 1's tests now pass.
4. **Measure the cost.**
   - Run `melin-bench` closed-loop against the echo example, before and
     after, over kernel TCP, and over DPDK if the bench host has it (see
     `scripts/bench/`). Compare p50, p99 and p99.9.
   - If the difference is within run-to-run noise, done.
   - If it shows, report the numbers before optimizing. The fallback is a
     high-water mark held inside a newtype `EncodeBuf`, sound only with
     step 3's check extended to assert that an encoder never writes past
     its returned length. Do not implement it unless asked.
5. `CHANGELOG.md`, **Fixed**: a response body byte the application's
   encoder does not write is now zero. It could previously carry bytes of
   an earlier response, possibly one sent to another client.
6. Commit: `fix(server-runtime): zero the response body region before each encode`.

## Step 3: (d) the debug-build purity check

1. In `frame_app_response`, under `#[cfg(debug_assertions)]`, before the
   real encode:
   - fill the body region with pattern A (e.g. `0xA5`), encode, and copy
     out the result;
   - fill it with pattern B (e.g. `0x5A`), encode again;
   - panic unless the two results match: the same `Ok` length with the
     same bytes, or the same `Err` reason.

   The panic message names the first offset that differs, and says that
   an encoder must write every byte below the length it returns and
   depend only on its input. Then zero (step 2) and do the real encode, so
   debug and release builds send the same bytes.

   - Put the check in one private function that `frame_app_response`
     calls, not a copy per caller.
   - `encode` is an `FnOnce` closure today. Change it to `Fn` (or pass the
     encoder and payload instead), so the check can call it three times,
     and update both callers.
   - Comment that the check triples encoder calls in debug builds only.
     Also say why it panics: an impure encoder is an application bug that
     the output digest would otherwise turn into a stopped node in
     production.
2. **Tests:**
   - The `SkippingEncoder` makes the check panic. Use
     `#[should_panic(expected = …)]` on a unit test that calls
     `frame_app_response` directly, gated on `#[cfg(debug_assertions)]`.
   - A `CountingEncoder`, whose body carries a counter it increments on
     every call, also makes it panic.
   - Every existing test encoder in the workspace passes unchanged. If one
     fails, it was impure: fix the test encoder, not the check.
3. **Step 1's tests now panic in debug builds,** because their encoder is
   impure by design. Keep step 2's guarantee covered on its own:
   - Split the zero-then-encode part of `frame_app_response` into a
     `pub(crate)` inner function that skips the purity check, and point
     step 1's unit tests at it. They then keep testing (b) in every build.
   - For the end-to-end test, check whether CI runs any tests with
     `--release` (`.github/workflows/`).
     - If it does, gate the end-to-end leak test on
       `#[cfg(not(debug_assertions))]`, with a comment saying why.
     - If it does not, replace it with an end-to-end debug test showing
       that the check fires through a running node. First find how a
       response-thread panic surfaces in the existing harness (see the
       panicked-stage tests in `melin-transport-core`), then assert on
       that.
     - Say in the commit message which of the two was done.
4. **The encoder contract**, in the same commit:
   - `ResponseEncoder`'s rustdoc (`crates/core/app/src/encoder.rs`): the
     body must be a pure function of its input, every byte below the
     returned length must be written, and debug builds check both.
   - `docs/building-an-application.md`: the same rule for application
     authors, in "The determinism rules" or beside the encoder section.
   - `CHANGELOG.md`, **Changed**: debug builds now panic on an encoder
     that skips bytes or depends on state.
5. Commit: `feat(server-runtime): check encoder purity in debug builds`.

## Done when

- The full suite passes, including the DPDK cases under the netns runner
  if any test touches the DPDK egress.
- Clippy is clean.
- Step 2's measurement is reported.
- What is left untested is listed, with the reason. For example, a
  release-build run of the end-to-end test, if CI has none.
