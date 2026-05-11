# Code Smell Cleanup — Hot Path

Survey scope: `crates/engine/`, `crates/journal/`, replication code.
Branch: `chore/code-smell-cleanup`.

Tackle items one by one. Mark `[x]` when done and reference the commit.

## High

- [x] **`engine/src/journal/snapshot.rs:553–723` — split `decode_exchange_state()`**
  Done: 12 per-section helpers + `decode_opt_nz_u64` + `read_section_len`;
  `decode_exchange_state` is now a ~75-line orchestrator. 364 engine tests pass, clippy clean.

- [ ] **`engine/src/journal/snapshot.rs:1309–1380` — split `restore_state()`**
  68 lines mixing symbol-indexed instrument assembly (1324–1346) with
  reservation slot injection. Extract to named helpers.

- [ ] **`engine/src/orderbook.rs:1686, 1704, 1770` — three `.expect("front existed")` in `match_against()` hot loop**
  Safe under protocol invariants, but a corruption/index bug would panic
  with no breadcrumb. Either (a) add a comment above each justifying the
  invariant, or (b) convert to logged + recoverable error path. Pick (a)
  unless the panic context is genuinely insufficient.

- [ ] **`engine/src/exchange.rs:1059, 1259` — `.expect("instrument verified to exist above")` lacks pointer to the check**
  Add a one-line comment referencing the prior risk-check site so future
  readers can verify the invariant without re-reading the whole fn.

## Medium

- [ ] **`engine/src/application_impl.rs:240` — `Vec::new()` in `restore()` snapshot deserialization**
  Pre-allocate with `Vec::with_capacity()` from payload hint / max snapshot size to avoid realloc during `read_to_end()`.

- [ ] **`engine/src/orderbook.rs:979–982, 1083–1086` — `Vec::new()` for hot-path scratch buffers in `new()`**
  `trigger_price_buf`, `triggered_buf`, `match_price_buf`, `consumed_slots`
  get `with_capacity` in `with_capacity()` but not in `new()`. Add a comment
  explaining why capacity hints matter (cleared and reused per order).

- [ ] **`engine/src/account.rs:397` — `.unwrap_or_default()` on balance lookup**
  Per CLAUDE.md, swallowed results need a justifying comment. Add: "Missing
  account/currency returns zero Balance; replay-safe since deposit initializes accounts."

- [ ] **`engine/src/journal/snapshot.rs:1330` — `Vec::resize_with(max_sym + 1, || None)` sparse symbol table**
  CLAUDE.md requires comments justifying data-structure choices. Add one
  noting sparse Vec over HashMap for cache locality + O(1) access via `symbol.0` as index.

- [ ] **`engine/src/journal/snapshot.rs:327` — split `encode_exchange_state()` (~120 lines)**
  Dual of `decode_exchange_state`. After the decode split, encode is the
  only side still written as a single linear function. Splitting it into
  per-section encoders makes the wire format auditable from both directions
  in one glance. Lower urgency than decode (the recovery path is the higher
  blast-radius side).

## Low

- [ ] **`crates/journal/src/trace.rs:435` — `writer.join().unwrap()`**
  Dev/bench only, but replace with proper error: `.map_err(|_| "thread panicked during trace flush")`.

- [ ] **`engine/src/scheduler.rs:148, 150, 154` — `.unwrap()` on `pop_due()` in unit tests**
  Replace with `assert_eq!(heap.pop_due(150), Some(expected))` for clarity.

- [ ] **`engine/src/application_impl.rs:308, 311` — `NonZeroU64::new(p).unwrap()` in test helpers**
  Test-only; consider const helpers / `const fn` to signal compile-time guarantee.

- [ ] **`journal/src/writer.rs:244, 1327` — `let _ = ...` discards**
  Add a one-line comment above each explaining why the result is safe to drop.

- [ ] **`engine/src/exchange.rs:898` — `let _ = reports` in a test**
  Replace with comment if intentional, or assert on it.

---

Note: no `panic!`/`todo!`/`unimplemented!` on the hot path, no locks on matching, allocations mostly pre-sized. This list is the long tail.
